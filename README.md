# SP500 자동매매 트레이딩 봇

한국투자증권(KIS) Open API를 이용한 **SP500(미국)** 중심 퀀트 자동매매 봇입니다.  
국내 시장(KOSPI / KOSDAQ / KONEX) 경로도 코드에 남아 있어 `MARKET` 환경 변수로 전환할 수 있습니다.

> **⚠️ 면책 조항 — 본 코드를 사용하기 전에 반드시 읽으세요**
>
> * 본 저장소는 **알고리즘 트레이딩 학습·연구 목적**의 예제 코드이며, **투자 조언·수익 보장이 아닙니다.**
> * 실제 매매에 따른 **손익·세금·법적 책임은 전적으로 사용자**에게 있습니다.
> * API 장애, 버그, 슬리피지, 환율·시차, 급변하는 시장 등으로 **예상치 못한 손실**이 발생할 수 있습니다.
> * 실전 계좌(`prod`) 투입 전 **`vps`(모의투자)로 충분히 검증**할 것을 권장합니다.
> * 상세 문구는 [9. 면책 조항](#9-면책-조항-disclaimer)을 참고하세요.

---

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [주요 기능](#2-주요-기능)
3. [시스템 아키텍처](#3-시스템-아키텍처-및-데이터-흐름)
4. [모듈 설명](#4-모듈-설명)
5. [기술 스택](#5-기술-스택)
6. [파이프라인 사전 준비](#6-파이프라인-사전-준비)
7. [설치 및 실행](#7-설치-및-실행)
8. [프로젝트 구조](#8-프로젝트-구조)
9. [구현 상태 및 알려진 제한](#9-구현-상태-및-알려진-제한)
10. [면책 조항](#10-면책-조항-disclaimer)

---

## 1. 프로젝트 개요

정해진 스케줄(**KST 기준**, 미국 장 시간에 맞춤)에 따라 다음을 자동 수행합니다.

| 단계 | 설명 (SP500) |
|------|------------------|
| 스크리닝 | KIS `frgn_code.mst`(S&P500) + NAS/NYS/AMS 마스터 → 유동성·재무·기술·**SPY 레짐**·섹터 트렌드 |
| 뉴스 수집 | Google News RSS (KR 시장은 네이버 검색 API) |
| 분석 | OpenAI GPT(US 프롬프트·USD 표기) 또는 휴리스틱 (`OPENAI_API_KEY` 없을 때) |
| 매매 | KIS Open API — `MARKET=SP500` 시 **해외 잔고·주문** (`TTTS3012R`, `TTTT1002U` 등) |
| 리스크 | 장중 별도 프로세스에서 손절·익절·전략 매도 |
| 사후 처리 | SQLite 기록, 주문 정합성, 월간 성과 리뷰·산출물 정리 |

- **기본 시장:** `MARKET=SP500` (`integrated_manager`, `screener`, `gpt_analyzer` 기본값)
- **실행 환경:** Docker Compose (`integrated_manager` + `background_risk_manager`)
- **설정:** `config/config.json`(전략·스케줄) + `config/.env`(비밀값, Git 제외)
- **모듈 연동:** `output/` 아래 JSON·DB 파일 파이프라인
- **알림:** Discord 웹훅(선택)

---

## 2. 주요 기능

- **SP500 유니버스** — `frgn_code.mst`(S&P500=1) ∩ NAS/NYS/AMS 해외 마스터 (~500종, 티커별 거래소)
- **US 스크리닝** — 5일 평균 거래대금(`min_trading_value_5d_avg_us`), 최소 점수·모멘텀·변동성 필터, 섹터 다양화
- **티커 정규화** — `utils.normalize_ticker_6()` / `norm_ticker` (`MARKET` 기준: US 심볼·KR 6자리). `trader`는 `self._t()` 헬퍼 사용
- **KIS 해외 시세·일봉** — `overseas_price`(실시간), `overseas_daily_price`(일봉), `overseas_price_detail`(PER/PBR) (`api/overseas_stock/`)
- **US 시세 라우팅** — `KIS.get_realtime_price_with_quotes()` / `inquire_price()`가 `MARKET=SP500`이면 해외 TR(`HHDFS00000300`)로 자동 분기 (`trader`·`risk_manager` 공통)
- **RSI·손절/목표·ATR** — `kis_market_data.py`가 KIS 일봉 우선 (`get_historical_prices` → US는 pykrx/fdr 미사용)
- **GPT / 휴리스틱 분석** — `MARKET=SP500` 시 US 전용 프롬프트(초기필터·전술·리밸런싱), 가격·예산 **USD** (`fmt_money`), `gpt_params.budget_guard` / `initial_filter` 연동
- **스케줄 오케스트레이션** — `integrated_manager.py`가 평일 잡·스크리너·파이프라인·잔액·체결확인·리컨실·요약 담당
- **해외 잔고·주문** — `account.py` → `kis_overseas_account` (USD), `KIS.order_cash()` → `overseas_order` (NASD)
- **장중 리스크** — `background_risk_manager` (US: `sell_time_windows`·`direct_execute`·NYSE 거래일; 잔고 `prpr=0` 시 KIS 실시간 시세 보정)
- **파이프라인 AM/PM 세션** — KST 시각·US ET 거래일 기준 `session`(`am`/`pm`)·`trade_date`를 산출물 파일명·환경변수로 고정 (자정 넘김 시 스크리너↔GPT 짝 유지)
- **주문 정합성** — `order_reconciler.py` (DB `pending`/`partial` → KIS 미체결·일자별 주문 조회로 `executed` 갱신; `order_id` 없는 orphan 방지·`--backfill-only`)
- **영속 손절/목표(positions)** — `recorder.py`의 `positions` 테이블에 `stop_price/target_price`를 저장하고, `trader.run_sell_logic()`에서 **positions 레벨을 우선 적용**
- **회전 정책 모듈화** — `rotation_policy.py`에서 최소 보유일·Δscore·예산/경제성·페어 상한(`max_pairs_per_run`)을 공통 정책으로 적용
- **비밀값 분리** — API 키·계좌·웹훅은 `config/.env`만 사용

---

## 3. 시스템 아키텍처 및 데이터 흐름

모듈 간 통신은 **`output/` JSON·SQLite**와 **`config/`** 를 중심으로 합니다.

### 3.1 배포 구조 (Docker Compose)

| 서비스 | 진입점 | 역할 |
|--------|--------|------|
| `integrated_manager` | `run_integrated_manager.py` | 평일 스케줄·스크리너·매매 파이프라인·잔액/요약·체결확인·리컨실 |
| `background_risk_manager` | `run_background_risk_manager.py` | 장중 약 5분 주기 `risk_manager._run_cycle()` (`config/.env.risk` 추가 로드) |

공통: `env_file: ./config/.env`, 볼륨 `./src`, `./config`, `./output`

```
┌─────────────────────────────────────────────────────────────────────────┐
│  config/config.json + config/.env (Git 제외)                             │
│  output/  ← screener_*_{date}_{am|pm}_SP500.json, gpt_trades_*, trading_data.db │
└─────────────────────────────────────────────────────────────────────────┘
         ▲                              ▲
         │                              │
┌────────┴─────────────┐      ┌─────────┴──────────────────┐
│ integrated_manager    │      │ background_risk_manager   │
│ schedule · subprocess │      │ RiskManager · KIS · 매도   │
└──────────────────────┘      └──────────────────────────┘
```

### 3.2 평일 스케줄 (KST, SP500 기준)

`config/config.json`의 `daily_summary`, `schedule_times`, `batch_execution_check`, `order_reconcile`로 설정합니다. **현재 기본값(미국 정규장 대략 23:30~06:00 KST):**

| 시각 | 작업 | 실행 |
|------|------|------|
| 23:25 | 장 시작 직전 잔액 | `account.py` |
| **22:30** | 스크리너 | `screener.py --market SP500` |
| **23:40** | 매매 파이프라인 | `health_check` → `news_collector` → `gpt_analyzer` → `trader` |
| **06:05** | 일괄 체결 확인 | `trader.py --batch-check-only` (`batch_execution_check.check_time`) |
| **06:10** | 주문 정합성 | `order_reconciler.py` (`order_reconcile.reconcile_time`) |
| **06:00** | 장 마감 후 잔액 | `account.py` |
| **06:15** | 일일 요약 | Discord (`close` 당일 + `open` 전일·US 세션 귀속) |

> 서머타임(DST) 적용 시 미국 장 개장·마감이 KST에서 약 1시간 밀리므로 `schedule_times`·`buy_time_windows`를 수동 조정하세요.

- 휴장일: 스크리너·파이프라인 스킵 (`is_market_open_day` — US: NYSE/XNYS, KR: 주말만)
- **월간 유지보수:** `monthly_maintenance.day`(기본 1일) 1회 — `reviewer.py` → `cleanup_output.py`

### 3.3 파이프라인 AM/PM 세션 · 거래일

`integrated_manager`가 실행 시 `utils.resolve_pipeline_context()`로 다음을 계산하고, 하위 스크립트에 `--date`·`--session` 및 환경변수 `PIPELINE_TRADE_DATE`·`PIPELINE_SESSION`으로 전달합니다.

| 구분 | US (`MARKET=SP500`) | 국내 (`KOSPI` 등) |
|------|---------------------|-------------------|
| **pm** | KST **22:00~06:30** (22:30 스크리너·23:40 파이프라인·자정 이후 동일 사이클) | KST 12:00 이후 |
| **am** | KST **06:30~22:00** (장후·주간 유지보수) | KST 12:00 이전 |
| **trade_date** | **NYSE(ET) 거래일** — 자정 넘어도 동일 US 세션은 같은 `trade_date` | KST 달력일(휴장 시 직전 거래일) |

`config.json` → `pipeline_sessions`로 경계 조정 가능:

```json
"pipeline_sessions": { "pm_start": "22:00", "am_end": "06:30" }
```

수동 오버라이드: `PIPELINE_SESSION=pm PIPELINE_TRADE_DATE=20260602`

### 3.4 스크리너 vs 매매 파이프라인

```
[22:30 screener, session=pm]              [23:40 pipeline, 동일 trade_date·session]
screener.py --market SP500              health_check.py (AAPL @ NAS)
  --date {trade_date} --session pm        → news_collector.py (--file 스크리너 JSON 고정)
  → screener_candidates_{date}_pm_SP500     → gpt_analyzer.py (--date --session)
  → screener_scores_{date}_pm_SP500.json  → trader.py (PIPELINE_* env로 gpt_trades 매칭)
  → market_state_{date}_pm_SP500.json     → recorder → trading_data.db
         └──────────────────────────────────────────┘
                              (장중, 별도 컨테이너) risk_manager.py
                              direct_execute 매도 → pending INSERT → order_reconciler → executed
```

**`PIPELINE_SCRIPTS` (의존성 순):**

1. `health_check.py`
2. `news_collector.py` ← 당일 `pm` 스크리너 JSON (`--file` 명시)
3. `gpt_analyzer.py` ← `collected_news_{date}_{session}_SP500.json` (`summary_*.json` 있으면 USD Budget Guard)
4. `trader.py` ← `gpt_trades_{date}_{session}_SP500.json`

스케줄상 `account.py`는 23:25·06:00에 별도 실행되며, 로컬 GPT 테스트 시에는 파이프라인 직전에 한 번 실행하는 것을 권장합니다.

레거시 파일(`_{date}_SP500.json`, 세션 접미사 없음)도 `find_latest_file()`이 호환합니다.

### 3.5 주요 산출물 (`output/`)

| 패턴 | 모듈 |
|------|------|
| `screener_candidates_{date}_{am\|pm}_SP500.json` | `screener.py` |
| `screener_candidates_full_*`, `screener_scores_*`, `screener_holdings_*` | `screener.py` |
| `market_state_{date}_{am\|pm}_SP500.json` | `screener.py` |
| `collected_news_{date}_{am\|pm}_SP500.json` | `news_collector.py` |
| `gpt_trades_{date}_{am\|pm}_SP500.json` | `gpt_analyzer.py` (`plans[]`, `session` 메타 포함) |
| `balance_*`, `summary_*` | `account.py` (US: `currency: "USD"`) |
| `daily_balances/balance_{open\|close}_*.json` | `integrated_manager` 일일 요약용 |
| `trading_data.db` | `recorder.py` (`trade_records`, `positions`) |
| `cache/` (`kis_token.json`, `*.mst`, `*.pkl`) | KIS·스크리너 |

Git에는 `output/.gitkeep`만 추적합니다.

### 3.6 DB 기록 · 주문 정합성

| 단계 | 동작 |
|------|------|
| `risk_manager` `direct_execute` | 주문 성공 + `order_id` → `record_trade` **`pending`**, `executed_qty=0` |
| `order_reconciler` (06:10 등) | DB open 주문 ↔ KIS `inquire_orders` → 없으면 `inquire_daily_order`로 체결 확인 → **`executed`**·`executed_qty` 갱신 |
| orphan 방지 | `pending`이면서 `order_id` 없으면 INSERT 생략 |

리컨실 시 **체결가·`profit_loss`는 갱신하지 않음**(주문 시점 호가·추정 손익 유지). 정밀 손익은 KIS 체결가 기준 별도 검증 권장.

```bash
# 수동 리컨실 (Docker)
docker compose exec integrated_manager python /app/src/order_reconciler.py --since-hours 36

# DB 행 확인
docker compose exec integrated_manager sqlite3 /app/output/trading_data.db \
  "SELECT order_id, ticker, action, order_status, executed_qty, price, profit_loss FROM trade_records WHERE order_id='...';"
```

디버그: `DB_RECORD_DEBUG=1` (`config/.env`)

### 3.7 KIS API 계층

```
api/kis_auth.KIS (DomesticStock + OverseasStock)
  ├── DomesticStock   — 국내 시세·잔고·주문
  ├── OverseasStock   — 해외 시세·일별시세·PER/PBR·잔고·주문 TR
  ├── order_cash()              — MARKET → 국내 order_cash / 해외 overseas_order
  ├── get_realtime_price_with_quotes()  — US: overseas_price / KR: 국내 inquire-price
  └── inquire_price()           — US: 해외 시세를 stck_prpr 형식 DataFrame으로 호환 반환
```

**해외 시세·일봉 TR (예: NASDAQ `EXCD=NAS`, `SYMB=NVDA`):**

| TR | 용도 |
|----|------|
| `HHDFS00000300` | 해외 현재가·호가 (`overseas_price`) |
| `HHDFS76240000` | 해외 기간별 일봉 (`overseas_daily_price` → `kis_market_data`) |
| `HHDFS76200200` | 현재가 상세·PER/PBR (`overseas_price_detail`) |

**해외 계좌·주문 TR (미국 NASDAQ, `OVRS_EXCG_CD=NASD`):**

| TR | 용도 |
|----|------|
| `TTTS3012R` / `VTTS3012R` | 해외주식 잔고 (`inquire_overseas_balance`) |
| `CTRP6504R` / `VTRP6504R` | 체결기준 현재잔고 (`inquire_overseas_present_balance`) |
| `TTTT1002U` / `TTTT1006U` | 미국 매수·매도 (`overseas_order`, 모의는 `V` 접두) |

정규화: `kis_overseas_account.py` → `balance_*.json` / `summary_*.json`  
- **예수금·주문가능:** `available_cash`(USD), `available_cash_krw` / `tot_evlu_amt_krw`(원화환산) 분리 저장  
- Discord·로그: 예수금 USD, 주문가능/총평가 원화환산 표기

**과거 OHLCV (`kis_market_data.py`):**

- `get_historical_prices_kis()` — US: `overseas_daily_price` BYMD 페이지네이션 → `Open/High/Low/Close/Volume`
- `screener_core.get_historical_prices()` — **KIS 우선**, US 실패 시 fdr/pykrx **미사용**, KR만 레거시 백업
- 사용처: `risk_manager` RSI·손절/목표·MA, `_compute_levels` ATR·스윙, `MarketAnalyzer` SPY 레짐

- **마스터:** `kis_master.load_kis_master("SP500")` — `frgn_code.mst`(S&P500) ∩ (nasmst+nysmst+amsmst)
- **레이트 리밋:** `config.json` → `kis_limits.max_rps=2`, `max_concurrency=1`

---

## 4. 모듈 설명

### 오케스트레이션

| 파일 | 역할 |
|------|------|
| `integrated_manager.py` | 스케줄, AM/PM·`trade_date` 컨텍스트, subprocess 파이프라인, 일일 잔액·요약 |
| `run_integrated_manager.py` | Docker / 로컬 진입점 |
| `risk_manager.py` | 장중 리스크 사이클·`check_sell_condition`·옵션 `direct_execute` 즉시매도 |
| `run_background_risk_manager.py` | 리스크 전용 컨테이너 (`config/.env.risk`) |
| `kis_market_data.py` | KIS 일봉 OHLCV·RSI/ATR 입력용 정규화 |
| `rotation_policy.py` | 회전(리밸런싱) 공통 정책 (`trader`·`rotation_manager`) |

### 파이프라인

| 파일 | 역할 |
|------|------|
| `screener.py` | `--market SP500` (기본), `--debug` 퍼널 로그 |
| `screener_core.py` | 지표·점수·`MarketState` |
| `kis_master.py` | 국내/해외 `.mst`·`.cod` 다운로드·캐시 |
| `health_check.py` | US: `AAPL` @ `NAS`, KR: `005930` |
| `news_collector.py` | US: Google News RSS / KR: Naver API |
| `gpt_analyzer.py` | GPT·휴리스틱; US/KR 프롬프트·시스템 메시지 분기, USD Budget Guard |
| `account.py` | 잔고·요약 JSON (`MARKET`에 따라 국내/해외 분기) |
| `kis_overseas_account.py` | 해외 잔고 TR → 국내 JSON 호환 정규화 (USD) |
| `trader.py` | 매수/매도·`positions` 손절/목표 우선·회전 (`KIS.order_cash` 해외 자동 라우팅) |
| `recorder.py` | `trading_data.db`·`positions`·`order_id` UPSERT·정합성 API |

### 공통·API

| 파일 | 역할 |
|------|------|
| `utils.py` | `resolve_pipeline_context`, `format_pipeline_artifact`, `is_us_market`, `norm_ticker`, `find_latest_file`(세션·거래일 필터) |
| `order_reconciler.py` | `pending`/`partial` ↔ KIS 체결 리컨실·orphan `order_id` backfill |
| `api/overseas_stock/overseas_stock_functions.py` | 해외 시세·잔고·주문 TR 래퍼 |
| `settings.py` / `env_loader.py` | 설정·`.env` 로드 |

---

## 5. 기술 스택

| 구분 | 내용 |
|------|------|
| 언어 | Python 3.11 (`Dockerfile`) |
| 스케줄 | `schedule` |
| 데이터 | `pandas`, `numpy` — **US 일봉·RSI: KIS** (`kis_market_data`); KR 백업: `FinanceDataReader`, `pykrx` |
| HTTP | `requests`, `httpx` |
| AI | `openai` (선택, HTTP 폴백 지원) |
| 설정 | `python-dotenv`, `PyYAML` |
| DB | SQLite (`output/trading_data.db`) |
| 배포 | Docker Compose |

**외부 API:** KIS Open API, Google News RSS(US 뉴스), Naver Search API(KR 뉴스), OpenAI(선택), Discord(선택)

---

## 6. 파이프라인 사전 준비

### 6.1 공통 환경

| 항목 | 필수 | 설명 |
|------|------|------|
| Docker & Compose | ✅ | 두 서비스 실행 |
| `config/.env` | ✅ | `cp config/.env.example config/.env` |
| `config/config.json` | ✅ | `trading_environment`: `vps` 또는 `prod` |
| `output/` | 자동 | 런타임 전용 |

### 6.2 API별 설정 (`config/.env`)

#### KIS Open API — 필수

| 변수 | 설명 |
|------|------|
| `KIS_MY_APP`, `KIS_MY_SEC` | 실전 App Key / Secret |
| `KIS_MY_ACCT_STOCK`, `KIS_MY_PROD` | 실전 계좌·상품코드 |
| `KIS_PAPER_*` | 모의투자 키·계좌 |

발급: [KIS Developers](https://apiportal.koreainvestment.com/) — **해외주식 거래 권한·계좌** 필요

**SP500 실매매 시:** 해외증권 계좌에 **USD 예수금**(또는 원화 → 외화 환전)이 있어야 합니다.  
`account.py`는 `MARKET=SP500`일 때 해외 잔고 TR을 조회하며, `summary_*.json`에 `currency: "USD"`와 함께 **USD 예수금**(`available_cash`)·**원화환산 주문가능/총평가**(`available_cash_krw`, `tot_evlu_amt_krw`)를 구분 저장합니다. `gpt_analyzer` Budget Guard·`trader` 가용 현금은 `extract_cash_from_summary()`로 USD 우선 해석합니다.

로컬 해외 잔고 확인:

```bash
cd src
CONFIG_PATH=../config/config.json OUTPUT_DIR=../output CACHE_DIR=../output/cache \
  KIS_TOKEN_FILE=../output/cache/kis_token.json MARKET=SP500 \
  python account.py
```

#### 실행 파라미터 — 권장

| 변수 | 기본 | 설명 |
|------|------|------|
| `MARKET` | `SP500` | `SP500` / `KOSPI` / `KOSDAQ` / `KONEX` |
| `SLOTS` | `3` | GPT 최대 매수 계획 수 |
| `PIPELINE_SESSION` | (자동) | `am` / `pm` — 수동 오버라이드 |
| `PIPELINE_TRADE_DATE` | (자동) | 산출물 `YYYYMMDD` — 수동 오버라이드 |

로컬 실행 시 (Docker 외):

```bash
CONFIG_PATH=./config/config.json
OUTPUT_DIR=./output
CACHE_DIR=./output/cache
KIS_TOKEN_FILE=./output/cache/kis_token.json
```

#### Naver Search API — KR 뉴스만

`MARKET`이 국내 시장일 때 `news_collector`에 필요합니다. SP500만 쓸 경우 **선택**.

#### OpenAI — 선택

| 변수 | 설명 |
|------|------|
| `OPENAI_API_KEY` | 없으면 `gpt_analyzer` 휴리스틱 모드 |

#### Discord — 선택

| 변수 | 설명 |
|------|------|
| `DISCORD_WEBHOOK_URL` | 통합 매니저 |
| `DISCORD_WEBHOOK_URL_RISK` | 리스크 (`config/.env.risk`, 미설정 시 위 URL) |

### 6.3 스크리너 주요 설정 (`config.json` → `screener_params`)

| 키 | US 기본값 | 설명 |
|----|-----------|------|
| `min_trading_value_5d_avg_us` | `1000000000` | 5일 평균 거래대금 하한(USD) — 조정 가능 |
| `min_market_cap_us` | `5000000000` | 시총 하한(마스터 Marcap 미제공 시 1차 스킵) |
| `min_score_threshold` | `0.52` | 최종 점수 컷 |
| `require_positive_momentum` | `true` | 20일 모멘텀 > 0 |
| `volatility_threshold` | `0.75` | 연율화 변동성 상한 |
| `top_n` | `8` | 최종 후보 수 (`max_positions`와 min) |

### 6.4 GPT 분석 (`config.json` → `gpt_params`)

| 키 | 기본 | 설명 |
|----|------|------|
| `openai_model` | `gpt-4o-mini` | OpenAI 모델 (HTTP 폴백 시 프롬프트 JSON 유도) |
| `budget_guard` | `true` | `summary_*.json`의 USD 가용금액으로 매수 적정성 검사 |
| `max_entry_price_ratio` | `0.2` | 종목당 최대 진입 비율 (슬롯·현금 대비) |
| `initial_filter.min_score_pass` | `0.52` | 1차 GPT/휴리스틱 점수 컷 (0–1 스케일) |
| `analysis_expansion.max_total_analysis` | `15` | GPT 상세 분석 최대 종목 수 |

US 프롬프트는 점수 **0.0–1.0**, 손절/목표·MA를 **$X.XX USD**로 안내합니다. `account.py`로 `summary_YYYYMMDD.json`이 없으면 Budget Guard가 비활성화됩니다.

### 6.5 장중 리스크 (`risk_params` · `trading_params`)

| 키 | 기본 | 설명 |
|----|------|------|
| `risk_params.auto_sell.direct_execute` | `true` | SELL 판단 시 `risk_manager`가 즉시 `order_cash` 매도 |
| `risk_params.auto_sell.cooldown_sec_per_ticker` | `20` | 종목별 즉시매도 쿨다운 |
| `trading_params.sell_time_windows` | `["23:15-06:00"]` | 매도·즉시매도 허용 KST 구간 (판단·실행 공통) |
| `trading_params.min_holding_hours` | `72` | 매수 후 N시간 미만이면 매도 보류 |
| `trading_params.buy_enabled` | `false` | `true`일 때만 `trader` 매수 실행 |

손절/목표·RSI는 `kis_market_data` + `compute_realtime_levels()`로 계산하며, DB `positions`에 저장된 레벨이 있으면 `trader` 매도 시 우선합니다.

**US 일일 요약:** `23:25` open 스냅샷은 다음 거래일 `session_close_date` 키로도 저장되고, `06:15` 요약은 `balance_close_당일` + `balance_open_전일`(또는 동일 세션 키)을 짝지어 비교합니다.

---

## 7. 설치 및 실행

### 7.1 클론 및 설정

```bash
git clone https://github.com/tingcho330/nasdaqstock.git
cd nasdaqstock   # 또는 로컬 폴더명

cp config/.env.example config/.env
# KIS, (선택) OpenAI, Discord 편집

# 선택
cp config/kis_devlp.yaml.example config/kis_devlp.yaml
cp config/.env.risk.example config/.env.risk
```

`config/config.json`에서 `trading_environment` 확인. 처음에는 **`vps`** 권장.

### 7.2 Docker 실행 (권장, NAS Synology 등)

```bash
docker compose up --build -d
docker compose logs -f integrated_manager
docker compose logs -f background_risk_manager
```

**일일 요약·잔액 (컨테이너 안에서 실행):**

```bash
docker compose exec integrated_manager python /app/run_integrated_manager.py --capture-close
docker compose exec integrated_manager python /app/run_integrated_manager.py --send-summary
ls -la output/daily_balances/
```

호스트에서 `python -c`로 `integrated_manager` 모듈을 직접 import하면 `schedule` 등 의존성·경로가 어긋날 수 있으므로 **위처럼 `run_integrated_manager.py`를 컨테이너에서 호출**하세요.

### 7.3 로컬 — E2E 테스트 (SP500)

프로젝트 루트에서 공통 환경 변수를 설정한 뒤 `src/`에서 실행합니다.

```bash
cd trading_bot_260530_NASDAQ

export CONFIG_PATH="$(pwd)/config/config.json"
export OUTPUT_DIR="$(pwd)/output"
export CACHE_DIR="$(pwd)/output/cache"
export KIS_TOKEN_FILE="$(pwd)/output/cache/kis_token.json"
export MARKET=SP500
export SLOTS=3

# trade_date·session: output/screener_candidates_{DATE}_{SESSION}_SP500.json 과 일치
DATE=20260529
SESSION=pm   # 저녁 스크리너/파이프라인이면 pm (integrated_manager와 동일)

cd src
```

#### A. 스크리너 (선택, ~3–4분)

```bash
python3 -u screener.py --market SP500 --date ${DATE} --session ${SESSION} --workers 1 --debug
# → ../output/screener_candidates_${DATE}_${SESSION}_SP500.json
```

#### B. 매매 파이프라인 — GPT까지 (`integrated_manager`와 동일 순서)

```bash
export PIPELINE_TRADE_DATE=${DATE}
export PIPELINE_SESSION=${SESSION}

# 1) API 헬스체크 (US: AAPL @ NAS)
python3 health_check.py

# 2) 뉴스 (Google RSS)
python3 news_collector.py --file ../output/screener_candidates_${DATE}_${SESSION}_SP500.json

# 3) 해외 잔고 스냅샷 (권장 — GPT Budget Guard용 USD)
python3 account.py
# → ../output/summary_${DATE}.json (currency: USD)

# 4) GPT 분석
python3 gpt_analyzer.py --date ${DATE} --session ${SESSION} --market SP500 --slots ${SLOTS:-3}
# → ../output/gpt_trades_${DATE}_${SESSION}_SP500.json
```

#### C. 트레이더 (실주문 주의)

`trading_environment`가 `prod`이면 실계좌 주문이 나갈 수 있습니다. 처음에는 `config.json`에서 **`vps`** 로 두고 검증하세요.

또한 기본 설정에서는 안전을 위해 `config/config.json`의 `trading_params.buy_enabled=false`로 둘 것을 권장합니다(매도/리스크만 검증).

```bash
python3 trader.py --date ${DATE}
# trader는 PIPELINE_TRADE_DATE·PIPELINE_SESSION env로 gpt_trades 최신 파일 매칭
```

**검증된 로컬 결과 (예시, `DATE=20260529`):** health_check ✅ → news 1종목 ✅ → gpt_analyzer 매수 계획 1건(AMZN, USD 손절/목표 표기) ✅.  
후보 수는 `min_trading_value_5d_avg_us`에 따라 달라집니다(기본 10억 USD면 유동 상위만 통과).

`PYTHONPATH=src` 대신 `cd src` 후 실행해도 동일합니다. Docker는 `/app/src` 기준으로 동일 스크립트를 호출합니다.

### 7.4 환경 변수 참고

| 변수 | 용도 |
|------|------|
| `LOG_LEVEL` | `DEBUG` / `INFO` |
| `SCREENER_TIMEOUT_SEC` | 스크리너 subprocess 타임아웃 |
| `HEALTH_CHECK_TICKER_NAS` / `_NYS` | US 헬스체크 (기본 `AAPL`, `JPM`) |
| `KIS_TRACE` | `1` 시 해외 잔고·시세 파싱 디버그 로그 |
| `DB_RECORD_DEBUG` | `1` 시 DB·리컨실 단계별 `[DB_DEBUG]` 로그 |

---

## 8. 프로젝트 구조

```
trading_bot_260530_NASDAQ/
├── config/
│   ├── config.json              # 전략·스케줄 (Git OK)
│   ├── .env.example
│   ├── .env.risk.example
│   ├── kis_devlp.yaml.example
│   └── .env                     # 비밀값 (Git 제외)
├── output/                      # 런타임 (Git 제외)
├── src/
│   ├── api/
│   │   ├── kis_auth.py          # DomesticStock + OverseasStock
│   │   ├── domestic_stock/
│   │   └── overseas_stock/
│   │       └── overseas_stock_functions.py
│   ├── kis_master.py            # KOSPI/KOSDAQ + SP500 마스터
│   ├── screener.py / screener_core.py
│   ├── news_collector.py / gpt_analyzer.py
│   ├── kis_overseas_account.py  # 해외 잔고 → balance/summary JSON (USD)
│   ├── trader.py / risk_manager.py / account.py
│   ├── kis_market_data.py       # KIS 일봉 OHLCV (RSI·손절/목표·ATR)
│   ├── order_reconciler.py
│   ├── rotation_policy.py / rotation_manager.py
│   ├── integrated_manager.py
│   ├── db_debug.py              # DB_RECORD_DEBUG 헬퍼
│   └── utils.py                 # pipeline session, norm_ticker, fmt_money, …
├── run_integrated_manager.py
├── run_background_risk_manager.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 9. 구현 상태 및 알려진 제한

| 영역 | 상태 |
|------|------|
| SP500 마스터·1·2차 스크리닝 | ✅ 동작 확인 |
| 해외 시세·Amount5D·PER/PBR | ✅ KIS TR |
| US 실시간 시세·호가 (`trader`·`risk_manager`) | ✅ `overseas_price` (국내 `inquire-price` 미사용) |
| US 일봉·RSI·손절/ATR (`kis_market_data`) | ✅ `HHDFS76240000`; US fdr/pykrx 백업 없음 |
| Google News RSS (US) | ✅ |
| GPT 분석·`gpt_trades_*.json` | ✅ (US 프롬프트·USD 표기) |
| 티커 정규화 (`normalize_ticker_6`) | ✅ trader·GPT·recorder·리컨실 등 |
| `Ticker` US 심볼 저장 | ✅ `AMZN` 형식 (zfill 미사용) |
| 해외 실주문 (`KIS.order_cash` → `overseas_order`) | ✅ 코드 연동 (`vps`/`prod`에서 검증 필요) |
| 해외 잔고 (`account.py` + `kis_overseas_account`) | ✅ USD·KRW 환산 필드 분리 |
| USD 예수금 (`extract_cash_from_summary`) | ✅ `frcr_dncl_amt_2` 등 CTRP6504R 필드 매핑 |
| `positions` 손절/목표·매도 우선 | ✅ `recorder` + `trader.run_sell_logic` |
| 회전 정책 (`rotation_policy`) | ✅ `trader` 리밸런스·`max_pairs_per_run` |
| 파이프라인 AM/PM·`trade_date` 연동 | ✅ 산출물 `_{date}_{am\|pm}_SP500`·env 전달 |
| 파이프라인 E2E (health → news → GPT) | ✅ 로컬 검증 (§7.3) |
| 장중 리스크·즉시매도 (`direct_execute`) | ✅ `pending` DB 기록 → 리컨실 `executed` |
| `order_reconciler`·`--backfill-only` | ✅ 미체결 0건·일자별 주문으로 체결 해소 |
| US 일일 요약 open/close 짝 | ✅ `session_close_date`·`--capture-close` |
| 해외 평단 (`pchs_avg_pric`) | ✅ 수량으로 재나누지 않음 (주당 평단) |
| 휴장일 판단 | ✅ US: NYSE(XNYS) / KR: 주말 (`is_market_open_day`) |
| `Marcap` (US) | 마스터 미제공 → 0, 시총 필터 스킵 |
| `investor_flow` (US) | 0 (국내 수급 API 경로) |
| `dynamic_cash_management` | ⚠️ 보유 0·현금 100% 시 가용금 축소 가능 → 설정 확인 |

실전 미국 매매 전: **`vps` → health_check → account → 스크리너 → 뉴스 → GPT → (선택) trader** 순으로 검증하세요.  
`trading_params.buy_enabled=false`로 매도·리스크만 먼저 검증하는 것을 권장합니다.

---

## 10. 면책 조항 (Disclaimer)

본 프로젝트는 **알고리즘 트레이딩 학습 및 연구 목적**으로 개발되었습니다.

* 제공되는 소스 코드, 설정 예시, 문서는 **투자 권유·투자 자문·수익률 보장이 아닙니다.**
* 본 코드를 다운로드·실행·수정·배포하여 발생하는 **모든 투자 손익, 세금, 법적 분쟁의 책임은 사용자 본인**에게 있습니다.
* 자동매매 시스템은 **소프트웨어 버그**, **증권사·외부 API 장애·지연**, **네트워크 오류**, **시장 급변·유동성 부족·슬리피지·환율** 등으로 인해 의도와 다른 주문·손실이 발생할 수 있습니다.
* GPT·뉴스·기술적 지표 기반 판단은 **오류·편향·지연**을 포함할 수 있으며, 과거 성과가 미래 수익을 보장하지 않습니다.
* 실전 계좌에 연결하기 전 **`vps`(모의투자) 환경에서 충분히 테스트**하고, 본인의 투자 성향·자금·리스크 허용 범위를 스스로 판단하시기 바랍니다.
* 제3자 API(KIS, Naver, OpenAI, Discord) 이용 시 각 서비스의 **이용약관·요금·호출 한도**를 준수해야 합니다.

**본 코드를 사용함으로써, 위 내용을 이해하고 이에 동의한 것으로 간주합니다.**
