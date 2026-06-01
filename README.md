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
- **KIS 해외 시세** — `overseas_daily_price`, `overseas_price_detail` 등 (`api/overseas_stock/`)
- **GPT / 휴리스틱 분석** — `MARKET=SP500` 시 US 전용 프롬프트(초기필터·전술·리밸런싱), 가격·예산 **USD** (`fmt_money`), `gpt_params.budget_guard` / `initial_filter` 연동
- **스케줄 오케스트레이션** — `integrated_manager.py`가 평일 잡·스크리너·파이프라인·잔액·체결확인·리컨실·요약 담당
- **해외 잔고·주문** — `account.py` → `kis_overseas_account` (USD), `KIS.order_cash()` → `overseas_order` (NASD)
- **장중 리스크** — `background_risk_manager` 컨테이너 (국내 장 시간 로직 잔존, US 전환 작업 중)
- **주문 정합성** — `order_reconciler.py` (`norm_ticker`로 US/KR 티커 통일)
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
│  output/  ← screener_*_SP500.json, gpt_trades_*, trading_log.db     │
└─────────────────────────────────────────────────────────────────────────┘
         ▲                              ▲
         │                              │
┌────────┴─────────────┐      ┌─────────┴──────────────────┐
│ integrated_manager    │      │ background_risk_manager   │
│ schedule · subprocess │      │ RiskManager · KIS · 매도   │
└──────────────────────┘      └──────────────────────────┘
```

### 3.2 평일 스케줄 (KST, SP500 기준)

`config/config.json`의 `daily_summary`, `schedule_times`, `batch_execution_check`로 설정합니다. **현재 기본값(미국 정규장 대략 23:30~06:00 KST):**

| 시각 | 작업 | 실행 |
|------|------|------|
| 23:25 | 장 시작 직전 잔액 | `account.py` |
| **22:30** | 스크리너 | `screener.py --market SP500` |
| **23:40** | 매매 파이프라인 | `health_check` → `news_collector` → `gpt_analyzer` → `trader` |
| **06:05** | 일괄 체결 확인 | `trader.py --batch-check-only` |
| 15:22 | 주문 정합성 | `order_reconciler.py` (KR 스케줄 잔존, 필요 시 조정) |
| **06:00** | 장 마감 후 잔액 | `account.py` |
| **06:15** | 일일 요약 | Discord |

> 서머타임(DST) 적용 시 미국 장 개장·마감이 KST에서 약 1시간 밀리므로 `schedule_times`·`buy_time_windows`를 수동 조정하세요.

- 휴장일: 스크리너·파이프라인 스킵 (`is_market_open_day` — 한국 거래소 캘린더 기준, US 전용 캘린더는 미구현)
- **월간 유지보수:** `monthly_maintenance.day`(기본 1일) 1회 — `reviewer.py` → `cleanup_output.py`

### 3.3 스크리너 vs 매매 파이프라인

```
[22:30 screener]                         [23:40 pipeline]
screener.py --market SP500           health_check.py (AAPL @ NAS)
  → screener_candidates_*_SP500.json   → news_collector.py (Google RSS)
  → screener_scores_*_SP500.json       → gpt_analyzer.py
  → market_state_*_SP500.json          → trader.py → recorder → trading_log.db
         └──────────────────────────────────────────┘
                              (장중, 별도 컨테이너) risk_manager.py
```

**`PIPELINE_SCRIPTS` (의존성 순):**

1. `health_check.py`
2. `news_collector.py` ← 스크리너 JSON
3. `gpt_analyzer.py` ← 뉴스 JSON (`summary_*.json` 있으면 USD Budget Guard)
4. `trader.py` ← `gpt_trades_*.json`

스케줄상 `account.py`는 23:25·06:00에 별도 실행되며, 로컬 GPT 테스트 시에는 파이프라인 직전에 한 번 실행하는 것을 권장합니다.

### 3.4 주요 산출물 (`output/`)

| 패턴 | 모듈 |
|------|------|
| `screener_candidates_{date}_SP500.json` | `screener.py` |
| `screener_candidates_full_*`, `screener_scores_*` | `screener.py` |
| `market_state_{date}_SP500.json` | `screener.py` |
| `collected_news_{date}_SP500.json` | `news_collector.py` |
| `gpt_trades_{date}_SP500.json` | `gpt_analyzer.py` (`plans[]`, 결정·전략·USD 분석) |
| `balance_*`, `summary_*` | `account.py` (US: `currency: "USD"`) |
| `trading_log.db` | `recorder.py` |
| `cache/` (`kis_token.json`, `*.mst`, `*.pkl`) | KIS·스크리너 |

Git에는 `output/.gitkeep`만 추적합니다.

### 3.5 KIS API 계층

```
api/kis_auth.KIS (DomesticStock + OverseasStock)
  ├── DomesticStock   — 국내 시세·잔고·주문
  ├── OverseasStock   — 해외 시세·일별시세·PER/PBR·잔고·주문 TR
  └── order_cash()    — MARKET에 따라 국내 order_cash / 해외 overseas_order 자동 라우팅
```

**해외 계좌·주문 TR (미국 NASDAQ, `OVRS_EXCG_CD=NASD`):**

| TR | 용도 |
|----|------|
| `TTTS3012R` / `VTTS3012R` | 해외주식 잔고 (`inquire_overseas_balance`) |
| `CTRP6504R` / `VTRP6504R` | 체결기준 현재잔고 (`inquire_overseas_present_balance`) |
| `TTTT1002U` / `TTTT1006U` | 미국 매수·매도 (`overseas_order`, 모의는 `V` 접두) |

정규화: `kis_overseas_account.py` → `balance_*.json` / `summary_*.json` (USD, `currency` 필드)

- **마스터:** `kis_master.load_kis_master("SP500")` — `frgn_code.mst`(S&P500) ∩ (nasmst+nysmst+amsmst)
- **레이트 리밋:** `config.json` → `kis_limits.max_rps=2`, `max_concurrency=1`

---

## 4. 모듈 설명

### 오케스트레이션

| 파일 | 역할 |
|------|------|
| `integrated_manager.py` | 스케줄, subprocess 파이프라인, `MARKET` 기본 `SP500` |
| `run_integrated_manager.py` | Docker / 로컬 진입점 |
| `risk_manager.py` | 장중 리스크 사이클 |
| `run_background_risk_manager.py` | 리스크 전용 컨테이너 |

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
| `trader.py` | 매수/매도 (`KIS.order_cash` 해외 자동 라우팅) |

### 공통·API

| 파일 | 역할 |
|------|------|
| `utils.py` | `is_us_market`, `norm_ticker`, `normalize_ticker_6`, `fmt_money`, `extract_cash_from_summary` |
| `order_reconciler.py` | 당일 주문 DB 정합성 (`normalize_ticker_6`) |
| `api/overseas_stock/overseas_stock_functions.py` | 해외 시세·잔고·주문 TR 래퍼 |
| `settings.py` / `env_loader.py` | 설정·`.env` 로드 |

---

## 5. 기술 스택

| 구분 | 내용 |
|------|------|
| 언어 | Python 3.11 (`Dockerfile`) |
| 스케줄 | `schedule` |
| 데이터 | `pandas`, `numpy`, `FinanceDataReader` (차트), `pykrx` (KR 레거시) |
| HTTP | `requests`, `httpx` |
| AI | `openai` (선택, HTTP 폴백 지원) |
| 설정 | `python-dotenv`, `PyYAML` |
| DB | SQLite (`output/trading_log.db`) |
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
`account.py`는 `MARKET=SP500`일 때 해외 잔고 TR을 조회하며, `summary_*.json`의 금액은 **USD**이고 `currency: "USD"`가 포함됩니다. `gpt_analyzer` Budget Guard·`trader` 주문 가능 금액도 이 값을 사용합니다.

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

---

## 7. 설치 및 실행

### 7.1 클론 및 설정

```bash
git clone <your-repo-url>
cd trading_bot_260530_NASDAQ

cp config/.env.example config/.env
# KIS, (선택) OpenAI, Discord 편집

# 선택
cp config/kis_devlp.yaml.example config/kis_devlp.yaml
cp config/.env.risk.example config/.env.risk
```

`config/config.json`에서 `trading_environment` 확인. 처음에는 **`vps`** 권장.

### 7.2 Docker 실행 (권장)

```bash
docker compose up --build -d
docker compose logs -f integrated_manager
```

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

# 날짜: output/screener_candidates_YYYYMMDD_SP500.json 의 YYYYMMDD
DATE=20260529   # 실제 파일명에 맞게 변경

cd src
```

#### A. 스크리너 (선택, ~3–4분)

```bash
python3 -u screener.py --market SP500 --workers 1 --debug
# → ../output/screener_candidates_${DATE}_SP500.json
```

#### B. 매매 파이프라인 — GPT까지 (`integrated_manager`와 동일 순서)

```bash
# 1) API 헬스체크 (US: AAPL @ NAS)
python3 health_check.py

# 2) 뉴스 (Google RSS)
python3 news_collector.py --file ../output/screener_candidates_${DATE}_SP500.json

# 3) 해외 잔고 스냅샷 (권장 — GPT Budget Guard용 USD)
python3 account.py
# → ../output/summary_${DATE}.json (currency: USD)

# 4) GPT 분석
python3 gpt_analyzer.py --date ${DATE} --market SP500 --slots ${SLOTS:-3}
# → ../output/gpt_trades_${DATE}_SP500.json
```

#### C. 트레이더 (실주문 주의)

`trading_environment`가 `prod`이면 실계좌 주문이 나갈 수 있습니다. 처음에는 `config.json`에서 **`vps`** 로 두고 검증하세요.

```bash
python3 trader.py --date ${DATE}
```

**검증된 로컬 결과 (예시, `DATE=20260529`):** health_check ✅ → news 1종목 ✅ → gpt_analyzer 매수 계획 1건(AMZN, USD 손절/목표 표기) ✅.  
후보 수는 `min_trading_value_5d_avg_us`에 따라 달라집니다(기본 10억 USD면 유동 상위만 통과).

`PYTHONPATH=src` 대신 `cd src` 후 실행해도 동일합니다. Docker는 `/app/src` 기준으로 동일 스크립트를 호출합니다.

### 7.4 환경 변수 참고

| 변수 | 용도 |
|------|------|
| `LOG_LEVEL` | `DEBUG` / `INFO` |
| `SCREENER_TIMEOUT_SEC` | 스크리너 subprocess 타임아웃 |
| `HEALTH_CHECK_TICKER` | US 헬스체크 심볼 (기본 `AAPL`) |
| `DB_RECORD_DEBUG` | `1` 시 DB 디버그 로그 |

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
│   ├── order_reconciler.py
│   ├── integrated_manager.py
│   └── utils.py                 # norm_ticker, normalize_ticker_6, fmt_money
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
| Google News RSS (US) | ✅ |
| GPT 분석·`gpt_trades_*.json` | ✅ (US 프롬프트·USD 표기) |
| 티커 정규화 (`normalize_ticker_6`) | ✅ trader·GPT·recorder·리컨실 등 |
| `Ticker` US 심볼 저장 | ✅ `AMZN` 형식 (zfill 미사용) |
| 해외 실주문 (`KIS.order_cash` → `overseas_order`) | ✅ 코드 연동 (`vps`/`prod`에서 검증 필요) |
| 해외 잔고 (`account.py` + `kis_overseas_account`) | ✅ USD `summary_*.json` |
| USD 예수금 (`extract_cash_from_summary`) | ✅ `ord_psbl_frcr_amt` 우선 |
| 파이프라인 E2E (health → news → GPT) | ✅ 로컬 검증 (§7.3) |
| 장중 리스크 US 장 시간 | ⏳ `risk_manager` KR 09:00–15:30 잔존 |
| `order_reconciler` 스케줄 | ⏳ 15:22 KST (US 장에 맞게 조정 권장) |
| 휴장일 판단 | ⏳ 한국 거래소 캘린더 (`is_market_open_day`) |
| `Marcap` (US) | 마스터 미제공 → 0, 시총 필터 스킵 |
| `investor_flow` (US) | 0 (국내 수급 API 경로) |

실전 미국 매매 전: **`vps` → 스크리너 → 뉴스 → account → GPT → (선택) trader** 순으로 로컬·모의 검증을 권장합니다.

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
