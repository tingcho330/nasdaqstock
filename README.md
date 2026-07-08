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
| 스크리닝 | KIS `frgn_code.mst`(S&P500) + NAS/NYS/AMS 마스터 → 유동성·재무·기술·**SPX 레짐**·섹터 트렌드 |
| 뉴스 수집 | Google News RSS (KR 시장은 네이버 검색 API) |
| 분석 | OpenAI GPT(US 프롬프트·USD 표기) 또는 휴리스틱 (`OPENAI_API_KEY` 없을 때) |
| 매매 | KIS Open API — `MARKET=SP500` 시 **해외 잔고·주문** (`TTTS3012R`, `TTTT1002U` 등) |
| 리스크 | 장중 별도 프로세스에서 손절·익절·전략 매도 |
| 사후 처리 | SQLite 기록, 주문 정합성, **KIS 엔드포인트 기반 performance review**, 산출물 정리 |

- **기본 시장:** `MARKET=SP500` (`integrated_manager`, `screener`, `gpt_analyzer` 기본값)
- **실행 환경:** Docker Compose (`integrated_manager` + `background_risk_manager`)
- **설정:** `config/config.json`(전략·스케줄, `//` 한글 주석 지원) + `config/.env`(비밀값, Git 제외)
- **모듈 연동:** `output/` 아래 JSON·DB 파일 파이프라인
- **알림:** Discord 웹훅(선택)

---

## 2. 주요 기능

- **SP500 유니버스** — `frgn_code.mst`(S&P500=1) ∩ NAS/NYS/AMS 해외 마스터 (~500종, 티커별 거래소)
- **US 스크리닝** — 5일 평균 거래대금(`min_trading_value_5d_avg_us`), 최소 점수·모멘텀·변동성 필터, 섹터 다양화
- **티커 정규화** — `utils.normalize_ticker_6()` / `norm_ticker` (`MARKET` 기준: US 심볼·KR 6자리). `trader`는 `self._t()` 헬퍼 사용
- **KIS 해외 시세·일봉** — `overseas_price`(실시간), `overseas_daily_price`(일봉), `overseas_price_detail`(PER/PBR) (`api/overseas_stock/`)
- **US 시세 라우팅** — `KIS.get_realtime_price_with_quotes()` / `inquire_price()`가 `MARKET=SP500`이면 해외 TR(`HHDFS00000300`)로 자동 분기 (`trader`·`risk_manager` 공통)
- **RSI·손절/목표·ATR** — `kis_market_data.py`가 KIS 일봉 우선 (`get_historical_prices` → US는 pykrx/fdr **미로드·미사용**)
- **Google News RSS (US)** — RSS 수집 + publisher **본문 scrape** → `collected_news` (`text`는 GPT 입력용)
- **GPT / 휴리스틱 분석** — US 프롬프트·1차 필터 `min_score_pass` config 연동·USD Budget Guard → `gpt_trades_*.json`
- **스케줄 오케스트레이션** — `integrated_manager.py`가 평일 잡·스크리너·파이프라인·잔액·체결확인·리컨실·요약 담당
- **일일 요약(US)** — `balance_open`/`balance_close` + `summary_*.json` KIS 필드 → Discord: **일일수익률(USD Primary)**, **보유평가(시장가)**, **예수금 세분화**, **원화 총평가(참고)**, **환율·원화 분해**
- **해외 잔고·주문** — `account.py` → `kis_overseas_account` (USD), `KIS.order_cash()` → `overseas_order` (NASD)
- **US 티커↔거래소 매핑** — `load_us_ticker_exchange_maps()`로 NAS/NYS/AMS 마스터 로드 → `resolve_us_excd()`·`resolve_us_ovrs_excg()`가 시세(`EXCD`: NAS/NYS/AMS)와 주문(`OVRS_EXCG_CD`: NASD/NYSE/AMEX) 분리. NYSE 종목(DELL 등)을 NAS로 잘못 조회하는 문제 방지
- **US 해외 매수 주문** — `TTTT1002U`는 `ORD_DVSN=00`(지정가) + `OVRS_ORD_UNPR>0`. `utils.resolve_us_buy_order_params()`로 `trader` 매수 경로 통일 (normal 30bps·urgent 100bps 상향). `01`(시장가)+`unpr=0`은 KIS 거절·`output` 누락(`API 응답 없음`) 빈발
- **US 해외 매도 주문** — `TTTT1006U`는 `ORD_DVSN=00`(지정가) + `OVRS_ORD_UNPR>0`만 유효. `utils.resolve_us_sell_order_params()`로 `risk_manager` DIRECT_SELL·`trader` 매도 경로 통일 (긴급/손절: 슬리피지 100bps). `ord_unpr=0` 매도는 API·`_order_cash_safe`에서 차단
- **매수 주문가능금액 동기화** — `trader`가 KIS `ord_psbl_frcr_amt`로 `remaining_cash` 동기화·수량 상한(`_cap_buy_to_orderable`) → 연쇄 매수 시 `APBK0952`(주문가능금액 초과) 선제 방지. KIS 거절 시 `rt_cd`/`msg1` 로그 노출
- **장중 리스크** — `background_risk_manager` (US: `market_hours.SP500.risk_poll_windows`·`sell_time_windows`·`direct_execute`·`direct_execute_partial`·NYSE 거래일; 장외는 다음 세션까지 대기·스레드 자동 재시작; 잔고 `prpr=0` 시 KIS HHDFS00000300/76200200·`trader._resolve_execution_price`와 동일 계열 시세 보정; 손절/목표 기준가는 **평단(`pchs_avg_pric`)** 우선)
- **EmergencyDrop·최소보유** — `emergency_drop_pct` **-13%** 급락 시 `min_holding_hours`(336h=**14일**) **예외** 즉시매도. 그 외 **전량·손절·RSI 등**은 매수일 기준 14일(`min_holding_hours`)·`rotation.min_holding_days`(14일) 적용. **부분익절(`PartialProfit`)은 14일 면제** — 장중 `direct_execute_partial` 또는 `trader` 파이프라인. 1주×50%는 **전량 treat**
- **파이프라인 AM/PM 세션** — KST 시각·US ET 거래일 기준 `session`(`am`/`pm`)·`trade_date`를 산출물 파일명·환경변수로 고정 (자정 넘김 시 스크리너↔GPT 짝 유지)
- **주문 정합성** — `order_reconciler.py` (DB `pending`/`partial` → KIS 조회: US `inquire-nccs`/`inquire-ccnl`, KR `inquire-orders`/`inquire-daily-ccld`로 `executed` 갱신; orphan `order_id` backfill·`--backfill-only`; 실행 후 **`order_reconcile_*.json` evidence 저장**)
- **KIS endpoint evidence JSON** — `trader`/`kis_overseas_account`가 매매 직전 `account_snapshot_{market}_{trade_date}[_{session}].json` + `account_snapshot_latest_{market}.json` 저장. `trade_date`·`session`은 **`resolve_pipeline_context()`** 기준(stale `PIPELINE_TRADE_DATE` 무시). `endpoint_evidence`(balance/present/nccs)·USD/KRW 분리 필드·`present_balance.call_count`(USD 1회)·`krw_aux_call_count`·`exchange_coverage`/`holding_exchange_coverage`·`sellable_qty_by_ticker` 포함. raw 응답·계좌번호·토큰 **미저장**. `_clamp_sell_qty()` 결과는 `trade_records.structured_context`에 `sellable_qty_checked`·`account_snapshot_file`·`snapshot_ts_kst` 등으로 기록
- **영속 손절/목표(positions)** — `recorder.py`의 `positions` 테이블에 `stop_price/target_price`를 저장하고, `trader.run_sell_logic()`에서 **positions 레벨을 우선 적용**
- **회전 정책 모듈화** — `rotation_policy.py`에서 최소 보유일·Δscore·예산/경제성·페어 상한(`max_pairs_per_run`)을 공통 정책으로 적용
- **비밀값 분리** — API 키·계좌·웹훅은 `config/.env`만 사용
- **KIS 엔드포인트 performance review** — `performance_review.py`가 **KIS API를 직접 호출하지 않고** `trading_data.db`·`balance_*/summary_*`·`account_snapshot_*`·`order_reconcile_*`·로그만 사후 분석. evidence 우선순위(dated → latest 일치 → fallback+WARN), **증거 부족 vs 실제 운영 오류** 구분, finding `category`·`evidence_source_file`·`evidence_trade_date`·`evidence_generated_at`, strict/일반 severity 분리, weekly/monthly **ccnl 기간 coverage** 검증, Markdown/JSON 보고서
- **reviewer.py** — `performance_review.py` wrapper (기본: `--period monthly`). 레거시 GPT config 튜닝(`run_review()`) 함수는 모듈 내 유지

---

## 3. 시스템 아키텍처 및 데이터 흐름

모듈 간 통신은 **`output/` JSON·SQLite**와 **`config/`** 를 중심으로 합니다.

### 3.1 배포 구조 (Docker Compose)

| 서비스 | 진입점 | 역할 |
|--------|--------|------|
| `integrated_manager` | `run_integrated_manager.py` | 평일 스케줄·스크리너·매매 파이프라인·잔액/요약·체결확인·리컨실 |
| `background_risk_manager` | `run_background_risk_manager.py` | `risk_poll_windows` 구간만 약 5분 주기 `_run_cycle()`; 장외는 `next_session_open_kst`까지 대기; 스레드 중단 시 60초마다 재시작 (`DISCORD_WEBHOOK_URL_RISK`) |

공통: **`env_file: ./config/.env`만** (두 서비스 동일), 볼륨 `./src`, `./config`, `./output`  
`config/.env.risk`는 **사용하지 않습니다.**

**Discord 웹훅 분리**

| 변수 | 사용처 |
|------|--------|
| `DISCORD_WEBHOOK_URL` | `integrated_manager`·파이프라인·스크리너 (`notifier`) |
| `DISCORD_WEBHOOK_URL_RISK` | `background_risk_manager` — `run_background_risk_manager.py`가 기동 시 이 값을 `DISCORD_WEBHOOK_URL`로 주입 후 `risk_manager` 로드 |

리스크 채널을 분리하려면 `config/.env`에 두 URL을 모두 넣으세요. `DISCORD_WEBHOOK_URL_RISK`가 비어 있으면 `DISCORD_WEBHOOK_URL`로 폴백합니다.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  config/config.json + config/.env (Git 제외, DISCORD_* 포함)            │
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

`config/config.json`의 `daily_summary`, `schedule_times`, `batch_execution_check`, `order_reconcile`로 설정합니다. **저장소 기본값(미국 정규장 대략 22:50~06:15 KST, `config.json` 기준):**

| 시각 | 작업 | 실행 |
|------|------|------|
| **23:55** | 장 시작 직전 잔액 스냅샷 | `capture_balance_snapshot("open")` → `balance_open_*.json` |
| **22:50** | 스크리너 | `screener.py --market SP500` |
| **23:30** | 매매 파이프라인 | `health_check` → `news_collector` → `gpt_analyzer` → `trader` |
| **06:05** | 일괄 체결 확인 | `trader.py --batch-check-only` (`batch_execution_check.check_time`) |
| **06:10** | 주문 정합성 | `order_reconciler.py` (`order_reconcile.reconcile_time`) |
| **06:00** | 장 마감 후 잔액 스냅샷 | `capture_balance_snapshot("close")` → `balance_close_*.json` |
| **06:15** | 일일 요약 | Discord — open/close KIS summary 필드 비교 (US: 원화 총평가·USD 예수금·평가손익 합계) |

> 서머타임(DST) 적용 시 미국 장 개장·마감이 KST에서 약 1시간 밀리므로 `schedule_times`·`buy_time_windows`를 수동 조정하세요.

- 휴장일: 스크리너·파이프라인 스킵 (`is_market_open_day` — US: NYSE/XNYS, KR: 주말만)
- **월간 유지보수:** `monthly_maintenance.day`(기본 1일) 1회 — `performance_review.py --period monthly` → `cleanup_output.py` (매매 파이프라인과 **분리**된 사후 분석)
- **주간 performance review (선택):** `performance_review.weekly_enabled=true` 시 월요일 09:00 KST — `performance_review.py --period weekly`

### 3.2.1 일일 요약 · 잔액 비교 (US / KR)

`send_daily_trading_summary()`가 `output/daily_balances/`의 open/close 스냅샷을 짝지어 Discord로 보냅니다.

**US (`MARKET=SP500`)** — `account.py`가 저장한 `summary_YYYYMMDD.json`의 KIS 필드를 사용합니다 (`compare_balances` → `integrated_manager`).

| Discord 필드 | summary / 스냅샷 `kis_summary` 키 | 표기 통화 | 변화량 |
|--------------|-----------------------------------|-----------|--------|
| **일일 수익률 (USD)** ← Primary | `ord_psbl_frcr_amt` + `holdings_value` 합산 | **USD** | (close−open) / open |
| USD 총자산 (예수금+보유) | 위 합산 | **USD** | close − open |
| 보유평가 (USD, 시장가) | `balance_*.json` `holdings_detail[].evlu_amt` 합 | **USD** | close − open |
| 예수금 (USD) | `ord_psbl_frcr_amt`, `usd_cash_total`, `usd_withdrawable`, `usd_sell_reuse`, `usd_buy_margin`, `krw_cash` | **USD/KRW** | close − open (+ close 세분화) |
| 총평가 (원화환산, 참고) | `tot_evlu_amt_krw` (CTRP6504R output3) | **원화** | close − open |
| 환율 · 원화 분해 | `bass_exrt`, `tot_evlu_amt_krw` | **원화** | open→close 환율, 매매기여·환율영향 |
| 실현 손익 (KIS) | `ovrs_rlzt_pfls_amt` (TTTS3012R output2) | **USD** | close − open (누적 delta) |

- 스냅샷 캡처 시 `kis_summary`를 `balance_{open\|close}_*.json`에 함께 저장합니다.
- `CTRP6504R`은 `WCRC_FRCR_DVSN_CD=02`(외화) + `01`(원화, `krw_cash`) 이중 조회합니다.
- 과거 스냅샷은 `summary_file` 경로 또는 `summary_{file_date}.json`에서 동일 필드를 로드합니다.
- **일일 수익률 Primary는 USD**입니다. 원화 총평가·환율 분해는 참고용입니다. US 요약에서는 혼합 단위 추정 **추정 수수료** 필드를 생략합니다.

**KR** — 스냅샷 `total_balance`·`cash`·`holdings_value`(예수금+보유평가 합산) 및 `holdings_detail` 기반 보조 지표를 사용합니다.

| 공통 | 설명 |
|------|------|
| **open 짝** | US: `balance_close_{당일}` + `balance_open_{전일}` (또는 `session_close_date` 키). `23:55` open은 다음 NYSE 거래일 키로도 저장 |
| **재전송** | open/close JSON만 있으면 `--send-summary`로 Discord 재전송 가능 |

`daily_summary` (`config.json`): `balance_open_time`, `balance_close_time`, `summary_send_time`

```bash
# 요약만 재전송 (스냅샷 재캡처 없이, summary/ kis_summary 기준 재계산)
docker compose exec integrated_manager python /app/run_integrated_manager.py --send-summary
```

### 3.3 파이프라인 AM/PM 세션 · 거래일

`integrated_manager`가 실행 시 `utils.resolve_pipeline_context()`로 다음을 계산하고, 하위 스크립트에 `--date`·`--session` 및 환경변수 `PIPELINE_TRADE_DATE`·`PIPELINE_SESSION`으로 전달합니다.

| 구분 | US (`MARKET=SP500`) | 국내 (`KOSPI` 등) |
|------|---------------------|-------------------|
| **pm** | KST **22:00~06:30** (22:50 스크리너·23:30 파이프라인·자정 이후 동일 사이클) | KST 12:00 이후 |
| **am** | KST **06:30~22:00** (장후·주간 유지보수) | KST 12:00 이전 |
| **trade_date** | **NYSE(ET) 거래일** — 자정 넘어도 동일 US 세션은 같은 `trade_date` | KST 달력일(휴장 시 직전 거래일) |

`config.json` → `pipeline_sessions`로 경계 조정 가능:

```json
"pipeline_sessions": { "pm_start": "22:00", "am_end": "06:30" }
```

수동 오버라이드: `PIPELINE_SESSION=pm PIPELINE_TRADE_DATE=20260602`

### 3.3.1 파이프라인 PM vs 리스크 폴링 세션 (US)

| 구분 | KST 시간창 | 설정 키 | 비고 |
|------|------------|---------|------|
| **파이프라인 PM** | 22:00~06:30 | `pipeline_sessions.pm_start` / `am_end` | 22:50 스크리너·23:30 파이프라인 등 **스케줄 잡** |
| **리스크 폴링** | 22:30~05:00 | `market_hours.SP500.risk_poll_windows` (없으면 `trading_params.sell_time_windows`) | `background_risk_manager`만 사용 · NYSE 거래일 가드 · **미국 정규장(ET 9:30~16:00)에 맞춤** |

컨테이너가 **22:00에 기동**해도 `risk_poll_windows` 시작(22:30) 이전에는 리스크 체크를 하지 않습니다(정상). 세션 시작 시 `리스크 세션 시작 — 모니터링 재개` 로그 후 5분 주기로 `_run_cycle()`이 돌아갑니다. **05:00 KST 이후(미국 장 마감 후)**에는 폴링·매도 주문을 시도하지 않습니다. DST 전환 시 `risk_poll_windows`·`sell_time_windows`를 수동 조정하세요.

```bash
docker compose logs -f background_risk_manager | grep -E '장외 대기|리스크 세션|리스크 체크'
```

### 3.4 스크리너 vs 매매 파이프라인

```
[22:50 screener, session=pm]              [23:30 pipeline, 동일 trade_date·session]
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

스케줄상 잔액 스냅샷은 `23:55`(open)·`06:00`(close)에 `account.py`를 호출해 저장하며, 로컬 GPT 테스트 시에는 파이프라인 직전에 `account.py`를 한 번 실행하는 것을 권장합니다.

레거시 파일(`_{date}_SP500.json`, 세션 접미사 없음)도 `find_latest_file()`이 호환합니다.

### 3.5 주요 산출물 (`output/`)

| 패턴 | 모듈 |
|------|------|
| `screener_candidates_{date}_{am\|pm}_SP500.json` | `screener.py` |
| `screener_candidates_full_*`, `screener_scores_*`, `screener_holdings_*` | `screener.py` |
| `market_state_{date}_{am\|pm}_SP500.json` | `screener.py` |
| `collected_news_{date}_{am\|pm}_SP500.json` | `news_collector.py` — `{status, text, articles[], meta}` (US: RSS+본문 scrape) |
| `gpt_trades_{date}_{am\|pm}_SP500.json` | `gpt_analyzer.py` (`plans[]`, `session` 메타 포함) |
| `balance_*`, `summary_*` | `account.py` (US: `currency: "USD"`) |
| `daily_balances/balance_{open\|close}_*.json` | `integrated_manager` 일일 요약 — `kis_summary`(US)·`total_balance`·`cash`·`holdings_value`·`holdings_detail[]`·`summary_file` |
| `trading_data.db` | `recorder.py` (`trade_records`, `positions`) |
| `account_snapshot_{market}_{trade_date}[_{am\|pm}].json` | `trader` KIS 스냅샷 evidence (`schema_version` 1.0, `endpoint_evidence`, USD/KRW 분리, `pipeline_context_source`) |
| `account_snapshot_latest_{market}.json` | 최신 account snapshot evidence (리뷰 date 불일치 시 fallback+WARN) |
| `order_reconcile_{market}_{trade_date}.json` | `order_reconciler` nccs/ccnl·`query_start_date`/`query_end_date`·db_reconcile evidence |
| `order_reconcile_latest_{market}.json` | 최신 order reconcile evidence |
| `performance_reviews/performance_review_{market}_{period}_{date}.{json,md}` | `performance_review.py` — KIS endpoint review·finding(category·evidence 메타) |
| `performance_reviews/latest_{market}_{period}.{json,md}` | 최신 performance review |
| `cache/` (`kis_token.json`, `*.mst`, `*.pkl`) | KIS·스크리너 |

Git에는 `output/.gitkeep`만 추적합니다. `cleanup_output.py`는 `performance_reviews/`·`account_snapshot_*`·`order_reconcile_*`(latest 포함)·`logs/` 최근 N일(기본 14일)·evidence JSON 최근 **20거래일**을 **삭제하지 않습니다**.

### 3.6 DB 기록 · 주문 정합성

| 단계 | 동작 |
|------|------|
| `risk_manager` `direct_execute` | 주문 성공 + `order_id` → `record_trade` **`pending`**, `executed_qty=0` |
| `order_reconciler` (06:10 등) | DB open 주문 ↔ KIS 조회 — **US:** `inquire-nccs` → `inquire-ccnl` / **KR:** `inquire-orders` → `inquire-daily-ccld` → **`executed`**·`executed_qty` 갱신 |
| `trader` 매수 직후 | `inquire-ccnl`·잔고 delta로 체결 확인, `pending` 시 `add_pending_order` + DB 기록 |
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

**US 시세 `EXCD` vs 주문 `OVRS_EXCG_CD` (혼동 주의):**

| 용도 | 코드 예 | KIS 필드 |
|------|---------|----------|
| 시세·일봉 조회 | NAS / NYS / AMS | `EXCD` (`HHDFS00000300`, `HHDFS76240000`) |
| 매수·매도 주문 | NASD / NYSE / AMEX | `OVRS_EXCG_CD` (`TTTT1002U`, `TTTT1006U`) |

`overseas_price()`는 `resolve_us_excd_candidates()`로 EXCD 후보(NYS→NAS 등) 순회. 마스터 힌트(`ovrs_excg_hint`)가 있으면 해당 EXCD 우선.

**해외 시세·일봉 TR (예: NYSE `EXCD=NYS`, `SYMB=DELL`):**

| TR | 용도 |
|----|------|
| `HHDFS00000300` | 해외 현재가·호가 (`overseas_price`; `last` 비면 `base` 파싱) |
| `HHDFS76240000` | 해외 기간별 일봉 (`overseas_daily_price` → `kis_market_data`) |
| `FHKST03030100` | 해외지수·환율 기간별 일봉 (`overseas_daily_chart_price` → US 레짐 SPX) |
| `HHDFS76200200` | 현재가 상세·PER/PBR·**시세 폴백** (`overseas_price_detail`; `00000300` 실패 시) |

**해외 계좌·주문 TR (미국, `OVRS_EXCG_CD=NASD`/`NYSE`/`AMEX`):**

| TR | 용도 |
|----|------|
| `TTTS3012R` / `VTTS3012R` | 해외주식 잔고 (`inquire_overseas_balance`) |
| `CTRP6504R` / `VTRP6504R` | 체결기준 현재잔고 (`inquire_overseas_present_balance`) |
| `TTTS3018R` / `VTTS3018R` | 해외주식 **미체결** (`inquire-nccs`) |
| `TTTS3035R` / `VTTS3035R` | 해외주식 **주문체결내역** (`inquire-ccnl`, US ET 날짜) |
| `TTTT1002U` / `TTTT1006U` | 미국 매수·매도 (`overseas_order`, 모의는 `V` 접두) |

**미국 매수(`TTTT1002U`)·매도(`TTTT1006U`) ORD_DVSN:** 본 저장소는 **`00`(지정가)+`OVRS_ORD_UNPR>0`** 만 사용. 매수: `resolve_us_buy_order_params()`(현재가+슬리피지 상향). 매도: `resolve_us_sell_order_params()`(현재가−슬리피지 하향). `01`(시장가)+`unpr=0`은 거절·`output` 누락 빈발 — `overseas_order()`가 `rt_cd`/`msg1`만 담은 DataFrame 반환. 매도 TR의 `31`~`34`(MOO/LOO/MOC/LOC)는 미사용.

정규화: `kis_overseas_account.py` → `balance_*.json` / `summary_*.json`  
- **예수금·주문가능:** `available_cash`(USD), `usd_cash_total`·`usd_withdrawable`·`usd_sell_reuse`·`usd_buy_margin`, `krw_cash` / `tot_evlu_amt_krw`(원화환산) 분리 저장  
- **환율·손익:** `bass_exrt`, `ovrs_rlzt_pfls_amt`  
- Discord·로그: **USD 일일수익률 Primary**, 예수금 세분화, 원화 총평가(참고), 환율 분해

**과거 OHLCV (`kis_market_data.py`):**

- `get_historical_prices_kis()` — US 개별종목: `overseas_daily_price` (HHDFS76240000) BYMD 페이지네이션
- `get_us_regime_ohlcv()` — US 레짐: `overseas_daily_chart_price` (FHKST03030100) **SPX 지수**, 실패 시 SPY@AMS (HHDFS76240000)
- `config.json` → `us_market_regime` (`index_symbol`, `etf_fallback`)
- `screener_core.get_historical_prices()` — **KIS 우선**, US는 pykrx/fdr **미로드**, KR만 레거시 백업
- 사용처: `risk_manager` RSI·손절/목표·MA, `_compute_levels` ATR·스윙 / 레짐·`MarketAnalyzer` → `get_us_regime_ohlcv`

- **마스터:** `kis_master.load_kis_master("SP500")` — `frgn_code.mst`(S&P500) ∩ (nasmst+nysmst+amsmst)
- **레이트 리밋:** `config.json` → `kis_limits.max_rps=2`, `max_concurrency=2`

---

## 4. 모듈 설명

### 오케스트레이션

| 파일 | 역할 |
|------|------|
| `integrated_manager.py` | 스케줄, AM/PM·`trade_date` 컨텍스트, subprocess 파이프라인, open/close 잔액 스냅샷·**US/KR 일일 요약**(KIS summary 필드) |
| `run_integrated_manager.py` | Docker / 로컬 진입점 |
| `risk_manager.py` | 장중 리스크·`check_sell_condition`·`EmergencyDrop`·`direct_execute`(전량)·**`direct_execute_partial`(부분익절)**·`partial_sell_state` 연동 |
| `partial_sell_state.py` | 부분익절 수량·`partial_sell_flags.json`·open SELL·매수 cooldown 공통 |
| `run_background_risk_manager.py` | 리스크 전용 컨테이너·백그라운드 스레드 생존 감시·재시작 |
| `integrated_manager.BackgroundRiskManager` | `risk_poll_windows`·5분 주기·장외 `next_session_open_kst` 대기 |
| `kis_market_data.py` | KIS 일봉 OHLCV·RSI/ATR 입력용 정규화 |
| `rotation_policy.py` | 회전(리밸런싱) 공통 정책 (`trader`·`rotation_manager`) |

### 파이프라인

| 파일 | 역할 |
|------|------|
| `screener.py` | `--market SP500` (기본), `--debug` 퍼널 로그, `--force` 휴장일 테스트 |
| `screener_core.py` | 지표·점수·`MarketState` (US: pykrx/fdr 지연 로드 스킵) |
| `kis_master.py` | 국내/해외 `.mst`·`.cod` 다운로드·캐시 |
| `health_check.py` | US: `AAPL` @ `NAS`, KR: `005930` |
| `news_collector.py` | US: Google RSS → publisher URL resolve → **본문 scrape**; KR: Naver API + 본문 scrape |
| `gpt_analyzer.py` | GPT·휴리스틱; 1차 필터 `min_score_pass` **config 연동**; US/KR 프롬프트·USD Budget Guard |
| `account.py` | 잔고·요약 JSON (`MARKET`에 따라 국내/해외 분기) |
| `kis_overseas_account.py` | 해외 잔고 TR → 국내 JSON 호환 정규화 (USD) |
| `trader.py` | 매수/매도·KIS `AccountSnapshot` 로드·evidence 저장·`_clamp_sell_qty` sellable evidence·`positions` 손절/목표 우선·부분익절 dedup·회전·`--batch-check-only` |
| `recorder.py` | `trading_data.db`·`positions`·`order_id` UPSERT·`structured_context` JSON 저장 |

### 공통·API

| 파일 | 역할 |
|------|------|
| `utils.py` | `resolve_pipeline_context`, `load_us_ticker_exchange_maps`, `resolve_us_excd`/`resolve_us_ovrs_excg`, `resolve_us_buy_order_params`, `resolve_us_sell_order_params`, `risk_session_windows`, `find_latest_file`(세션·거래일 필터) |
| `account_snapshot.py` | KIS endpoint `AccountSnapshot`·evidence JSON 빌드/저장·`compute_sellable_qty`·매매 유효성 검증 |
| `order_reconciler.py` | `pending`/`partial` ↔ KIS 체결 리컨실·orphan `order_id` backfill·**order_reconcile evidence JSON** |
| `performance_review.py` | **사후** KIS 5 TR evidence 리뷰·finding category·strict/일반 severity·보고서 (API 호출 **금지**) |
| `reviewer.py` | `performance_review` CLI wrapper + 레거시 `run_review()` (GPT config 튜닝) |
| `cleanup_output.py` | 오래된 산출물 정리 (`performance_reviews/` 등 보호) |
| `api/overseas_stock/overseas_stock_functions.py` | 해외 시세·잔고·주문 TR 래퍼 |
| `settings.py` / `env_loader.py` | 설정·`.env` 로드 |

### 3.8 KIS 엔드포인트 performance review

`performance_review.py`는 **저장된 artifact만** 읽습니다. 매매·주문·계좌 조회 API는 실행하지 않습니다.

**원칙**

| 구분 | 설명 |
|------|------|
| API 호출 | **금지** — `trader`·`kis_overseas_account`·`order_reconciler`만 KIS 호출 |
| account_snapshot 우선순위 | `account_snapshot_{market}_{review_date}_{session}.json` → `_{review_date}.json` → latest(trade_date 일치) → latest fallback + `ACCOUNT_SNAPSHOT_DATE_MISMATCH` |
| order_reconcile (daily) | `order_reconcile_{market}_{date}.json` → latest(trade_date 일치 시) |
| order_reconcile (weekly/monthly) | 기간 내 `order_reconcile_{market}_{date}.json` **전부** 수집 · ccnl `query_start_date`~`query_end_date`로 DB 주문 coverage 판정 |
| balance coverage | `status_by_exchange` 기준 (OK/EMPTY 정상, `row_count=0`은 실패 아님) — 보유종목 거래소와 무관 |
| present_balance | USD primary `call_count=1` 정상 · KRW 보조는 `krw_aux_call_count` · `total_asset_usd`는 USD만 |
| 민감정보 | raw KIS 응답·계좌번호·token·appkey는 evidence/보고서에 **저장하지 않음** |

| KIS TR | endpoint | 리뷰 목적 |
|--------|----------|-----------|
| `TTTS3012R` | inquire-balance | 보유수량·평균단가·`status_by_exchange`(NASD/NYSE/AMEX) |
| `CTRP6504R` | inquire-present-balance | USD 현금·총자산·`call_count`/`krw_aux_call_count`(거래소 loop 아님) |
| `TTTS3018R` | inquire-nccs | 미체결·pending sell·`sellable_qty` |
| `TTTS3035R` | inquire-ccnl | ODNO 체결·DB vs KIS 상태·기간 coverage |
| `TTTT1002U`/`TTTT1006U` | order | rt_cd/ODNO 품질·거절·가능수량/주문가능금액 초과 |

**finding category:** `DATA_QUALITY` · `OPERATIONS` · `TRADE_EXECUTION` · `RISK`

**strict vs 일반 모드:** evidence 부재(`KIS_NCCS_MISSING`, `KIS_CCNL_MISSING`, `KIS_PRESENT_BALANCE_EVIDENCE_MISSING`, `KIS_EXECUTED_FILL_UNVERIFIED` 등)는 strict=WARN, 일반=INFO. 실제 운영 오류(`KIS_PRESENT_BALANCE_CURRENCY_MIXED`, `KIS_SELL_SENT_WITH_ZERO_SELLABLE_QTY`, ccnl evidence 있는 `KIS_EXECUTED_WITHOUT_FILL` 등)는 strict 여부와 무관하게 ERROR/CRITICAL.

**대표 finding**

| finding | 의미 |
|---------|------|
| `KIS_BALANCE_PARTIAL_EXCHANGE_COVERAGE` | `status_by_exchange`에 FAILED/MISSING 거래소만 — NASD만 보유해도 NYSE/AMEX EMPTY면 미발생 |
| `KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP` | `call_count≥2` + 총자산 2배 이상 — CRITICAL (`call_count=1`이면 미발생) |
| `KIS_PRESENT_BALANCE_CURRENCY_MIXED` | `total_asset_usd`가 `available_cash_usd+holdings_value_usd` 대비 10배 이상 — ERROR |
| `KIS_PRESENT_BALANCE_EVIDENCE_MISSING` | `call_count` evidence 없음 — WARN/INFO |
| `KIS_CCNL_PERIOD_COVERAGE_INCOMPLETE` | weekly/monthly에서 ccnl query range가 DB 주문 기간 미커버 — coverage 부족 주문은 mismatch 아님 |
| `KIS_EXECUTED_FILL_UNVERIFIED` | ccnl evidence 없이 executed — WARN/INFO |
| `KIS_EXECUTED_WITHOUT_FILL` | ccnl evidence 있고 exec_qty=0 — ERROR |
| `ACCOUNT_SNAPSHOT_DATE_MISMATCH` | account_snapshot `trade_date`·`snapshot_ts_kst`/`generated_at_kst` 날짜 불일치(US는 KST 세션 보정 적용) |
| `KIS_SELL_WITHOUT_SELLABLE_CHECK` | `structured_context.sellable_qty_checked`·snapshot evidence 없을 때만 |

```bash
# Docker (사후 분석 — 파이프라인과 독립)
docker compose exec integrated_manager python -m performance_review \
  --market SP500 --date 20260630 --no-discord

docker compose exec integrated_manager python -m performance_review \
  --market SP500 --date 20260630 --strict-kis-endpoints --include-logs --no-discord

# 로컬
PYTHONPATH=src OUTPUT_DIR=./output CONFIG_PATH=config/config.json \
  python -m performance_review --market SP500 --date 20260630 \
  --strict-kis-endpoints --no-discord --include-logs

PYTHONPATH=src OUTPUT_DIR=./output CONFIG_PATH=config/config.json \
  python -m performance_review --market SP500 --period monthly --no-discord

# 단위 테스트 (로컬 venv)
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt pytest
PYTHONPATH=src OUTPUT_DIR=./output_test .venv/bin/python -m pytest \
  tests/test_performance_review_kis_endpoints.py -v
```

`config.json` → `performance_review`: `strict_kis_endpoints`, `weekly_enabled`, `monthly_enabled`, `max_findings` 등.

**US(예: SP500) account_snapshot 날짜 판정(오탐 방지)**

- **정상 케이스**: US trade_date=D의 `account_snapshot`이 KST로 **D 당일 밤~D+1 오전**에 생성되는 것은 정상입니다.
- **KST grace window(기본값)**: \(D 18:00 KST ~ D+1 09:00 KST\)
- **설정 키** (`config.json` → `performance_review`)
  - `us_trade_date_kst_grace_start_hour` (기본 18)
  - `us_trade_date_kst_grace_end_hour` (기본 9)
  - `us_trade_date_kst_grace_hours` (설정 시 end_hour override)
- **`ACCOUNT_SNAPSHOT_DATE_MISMATCH` 발생 조건**
  - `snapshot.trade_date != review_date` → **ERROR** (단, latest fallback 사용으로만 불일치면 **WARN**)
  - `snapshot_ts_kst`가 `trade_date` 대비 **2일 이상 차이** → **ERROR**
  - `generated_at_kst`가 review scope와 **명백히 무관(2일 이상 차이)** → **ERROR**

**테스트:** `tests/test_performance_review_kis_endpoints.py` (18 케이스 — present call_count·USD/KRW 혼합·balance coverage·ccnl 기간 coverage·sellable evidence 등)

---

## 5. 기술 스택

| 구분 | 내용 |
|------|------|
| 언어 | Python 3.11 (`Dockerfile`) |
| 스케줄 | `schedule` |
| 데이터 | `pandas`, `numpy` — **US 일봉·RSI: KIS** (`kis_market_data`); KR 백업: `FinanceDataReader`, `pykrx` (**SP500에서는 미로드**) |
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
| `config/config.json` | ✅ | `trading_environment`: `vps` 또는 `prod` — `//` 라인 주석(한글 설명) 지원 |
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
| `DISCORD_WEBHOOK_URL` | 통합 매니저·스크리너·파이프라인 |
| `DISCORD_WEBHOOK_URL_RISK` | `background_risk_manager` 전용 — **`config/.env`에만 설정** (`config/.env.risk` 미사용). 비어 있으면 `DISCORD_WEBHOOK_URL` 폴백 |

예시 (`config/.env`):

```env
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/.../integrated
DISCORD_WEBHOOK_URL_RISK=https://discord.com/api/webhooks/.../risk
```

### 6.3 스크리너 주요 설정 (`config.json` → `screener_params`)

| 키 | US 기본값 | 설명 |
|----|-----------|------|
| `min_trading_value_5d_avg_us` | `5000000000` | 5일 평균 거래대금 하한(USD) — 조정 가능 |
| `min_market_cap_us` | `5000000000` | 시총 하한(마스터 Marcap 미제공 시 1차 스킵) |
| `min_score_threshold` | `0.48` | 최종 점수 컷 (스크리너·리밸런스·GPT `initial_filter` 동기화) |
| `require_positive_momentum` | `false` | `true`면 20일 모멘텀 < 0 종목 제외 |
| `exclude_high_volatility` | `false` | `true`면 `volatility_threshold` 초과 종목 제외 |
| `volatility_threshold` | `0.90` | 연율화 변동성 상한 (`exclude_high_volatility=true`일 때) |
| `top_n` | `8` | 최종 후보 수 (`max_positions`와 min) |

### 6.4 뉴스 수집 (`config.json` → `news_params`)

| 키 | 기본 | 설명 |
|----|------|------|
| `days` | `14` | 최근 N일 RSS 필터 (스윙용; legacy 90) |
| `articles_per_stock` | `5` | 종목당 기사 수 |
| `scrape_body_us` | `true` | US: Google News 링크 follow 후 publisher 본문 scrape |
| `max_body_chars_per_article` | `800` | 기사별 본문 상한 |
| `max_gpt_text_chars` | `1500` | GPT `text` 필드 상한 (`gpt_analyzer`와 동일) |
| `dedupe_title_threshold` | `0.85` | US RSS 제목 중복 제거 |
| `us_query_mode` | `ticker_and_name` | `"Apple" AAPL stock` 검색 (`ticker_only` 가능) |
| `scrape_rps` | `2` | US 본문 scrape 초당 요청 상한 |
| `include_links_in_gpt_text` | `false` | `false`: URL은 `articles[].url`, GPT `text`는 제목·요약·본문만 |

**`collected_news_*.json` 종목별 구조:** `status` (`OK` / `PARTIAL` / `NO_NEWS` / `ERROR`), `text` (GPT 입력), `articles[]` (메타·URL), `meta` (수집 통계). GPT는 **`text` 문자열만** 분석하며 URL을 fetch하지 않습니다.

### 6.5 GPT 분석 (`config.json` → `gpt_params`)

| 키 | 기본 | 설명 |
|----|------|------|
| `openai_model` | `gpt-4o-mini` | OpenAI 모델 (HTTP 폴백 시 프롬프트 JSON 유도) |
| `budget_guard` | `true` | `summary_*.json`의 USD 가용금액으로 매수 적정성 검사 |
| `max_entry_price_ratio` | `0.95` | 종목당 최대 진입 비율 — `trading_params.per_ticker_max_weight`와 **동일 값 유지** |
| `initial_filter.min_score_pass` | `0.48` | 1차 GPT 프롬프트·하드컷 (config 런타임 로드; 구 0.55 하드코딩 제거) |
| `analysis_expansion.max_total_analysis` | `15` | GPT 상세 분석 최대 종목 수 |

US 프롬프트는 점수 **0.0–1.0**, 손절/목표·MA를 **$X.XX USD**로 안내합니다. `account.py`로 `summary_YYYYMMDD.json`이 없으면 Budget Guard가 비활성화됩니다. GPT `max_entry_price_ratio`와 trader `per_ticker_max_weight`를 **같은 값(기본 0.95)** 으로 맞추면, GPT가 “매수 가능”으로 내린 종목이 trader 예산 배분에서도 동일 상한(`usable_cash × 0.95`)을 받습니다. 복수 매수 시에는 `remaining_cash ÷ 남은 슬롯` 균등 분배가 추가로 적용됩니다.

### 6.6 장중 리스크 (`market_hours` · `risk_params` · `trading_params`)

| 키 | 기본 | 설명 |
|----|------|------|
| `market_hours.SP500.risk_poll_windows` | `["22:30-05:00"]` | `background_risk_manager` 활성 KST 구간 (`utils.is_regular_session`) — 미국 정규장(EDT ≈ 9:30~16:00 ET) |
| `risk_params.auto_sell.direct_execute` | `true` | **전량** SELL 판단 시 `risk_manager`가 즉시 TTTT1006U (urgent 슬리피지) |
| `risk_params.auto_sell.direct_execute_partial` | `true` | **PARTIAL_SELL** 판단 시 장중 즉시 부분 매도 (normal 30bps). 기본값 `true` |
| `risk_params.auto_sell.partial_execute_once` | `true` | 종목당 부분익절 1회 (`output/partial_sell_flags.json`) |
| `risk_params.auto_sell.partial_full_if_rounding_zero` | `true` | 1주×50% → 0이면 **1주 전량** treat |
| `risk_params.auto_sell.cooldown_sec_per_ticker` | `20` | 종목별 즉시매도 쿨다운 |
| `trading_params.sell_time_windows` | `["22:30-05:00"]` | 매도·즉시매도 허용 KST 구간 (판단·실행 공통) |
| `trading_params.buy_time_windows` | `["22:35-04:45"]` | `trader` 매수·보유 0 시 리스크 자동 `trader` 트리거 허용 구간 |
| `trading_params.min_holding_hours` | `336` | 매수일 기준 **14일** 미만이면 **전량·손절·RSI 등** 매도 보류. DB 없음 → **strict 차단**. **`PartialProfit` 부분익절 제외** |
| `rotation.min_holding_days` | `14` | 손절·RSI·회전 — `min_holding_hours`와 동일 14일 (`days×24==hours` 검증) |
| `trading_params.post_partial_sell_buy_cooldown_days` | `5` | 부분익절 후 재매수 cooldown (`output/cooldown.json`) |
| `risk_params.auto_sell.emergency_drop_pct` | `0.13` | 손익률 ≤ **-13%** 급락 시 즉시매도 (`min_holding_hours` 우회) |
| `trading_params.buy_enabled` | `true` | `false`면 `trader` 매수 스킵(매도·리스크만 검증 시 권장) |
| `trading_params.per_ticker_max_weight` | `0.95` | 종목당 주문 예산 상한 — `gpt_params.max_entry_price_ratio`와 **동일 값 유지** |

손절/목표·RSI는 `kis_market_data` + `compute_realtime_levels()`로 계산하며, DB `positions`에 저장된 레벨이 있으면 `trader` 매도 시 우선합니다.

**코드·설정 반영 후 재배포** (볼륨 마운트 `./src`·`./config`):

```bash
docker compose restart background_risk_manager integrated_manager
```

**US 일일 요약:** `balance_open_time`(기본 `22:45`) open 스냅샷은 다음 거래일 `session_close_date` 키로도 저장됩니다. `06:15` 요약은 `balance_close_당일` + `balance_open_전일`을 짝지어 Discord에 **USD 일일수익률(Primary)**·**보유평가(시장가)**·**예수금 세분화**·**원화 총평가(참고)**·**환율 분해**를 표시합니다. `available_cash_krw`를 USD 총평가로 쓰지 않습니다.

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
# 리스크 Discord: config/.env 에 DISCORD_WEBHOOK_URL_RISK 추가 (별도 .env.risk 파일 불필요)
```

`config/config.json`에서 `trading_environment` 확인. 처음에는 **`vps`** 권장.

### 7.2 Docker 실행 (권장, NAS Synology 등)

```bash
docker compose up --build -d
docker compose logs -f integrated_manager
docker compose logs -f background_risk_manager
```

**스크리너 로그 (integrated_manager 컨테이너에서 subprocess 실행):**

```bash
# 스크리너 관련만 필터
docker compose logs -f integrated_manager | grep -i screener

# 수동 실행 (디버그·주말/휴장일 테스트)
docker compose exec integrated_manager python /app/src/screener.py \
  --market SP500 --date YYYYMMDD --session pm --workers 2 --debug --force
```

> **주말·휴장일 수동 실행:** 기본적으로 스크리너는 **실행 당일(ET) 거래일**만 검사합니다. 토요일·NYSE 휴장일에는 `휴장일이므로 screener를 건너뜁니다`로 종료됩니다. 로컬·Docker 테스트 시 **`--force`** 로 휴장 검사를 건너뛸 수 있습니다. SP500 실행 시 pykrx(KRX)는 **로드하지 않습니다** (KRX 로그인 경고 없음).

**일일 요약·잔액 (컨테이너 안에서 실행):**

```bash
# close 스냅샷이 없을 때만 (06:00 스케줄 누락 시)
docker compose exec integrated_manager python /app/run_integrated_manager.py --capture-close
# open/close JSON이 있으면 재캡처 없이 summary/kis_summary 기준 재계산 후 Discord 전송
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
python3 -u screener.py --market SP500 --date ${DATE} --session ${SESSION} --workers 2 --debug
# 주말·휴장일: --force 추가
# → ../output/screener_candidates_${DATE}_${SESSION}_SP500.json
```

#### B. 매매 파이프라인 — GPT까지 (`integrated_manager`와 동일 순서)

```bash
export PIPELINE_TRADE_DATE=${DATE}
export PIPELINE_SESSION=${SESSION}

# 1) API 헬스체크 (US: AAPL @ NAS)
python3 health_check.py

# 2) 뉴스 (Google RSS + US 본문 scrape, config news_params)
python3 news_collector.py --file ../output/screener_candidates_${DATE}_${SESSION}_SP500.json
# → collected_news: status/text/articles/meta (--days·--articles는 config 기본값)

# 3) 해외 잔고 스냅샷 (권장 — GPT Budget Guard용 USD)
python3 account.py
# → ../output/summary_${DATE}.json (currency: USD)

# 4) GPT 분석
python3 gpt_analyzer.py --date ${DATE} --session ${SESSION} --market SP500 --slots ${SLOTS:-3}
# → ../output/gpt_trades_${DATE}_${SESSION}_SP500.json
```

#### C. 트레이더 (실주문 주의)

`trading_environment`가 `prod`이면 실계좌 주문이 나갈 수 있습니다. 처음에는 `config.json`에서 **`vps`** 로 두고 검증하세요.

실주문 검증 전에는 `config/config.json`에서 `trading_params.buy_enabled=false`로 두고 매도·리스크만 먼저 확인하는 것을 권장합니다(저장소 기본값은 `true`).

```bash
python3 trader.py --date ${DATE}
# trader는 PIPELINE_TRADE_DATE·PIPELINE_SESSION env로 gpt_trades 최신 파일 매칭
```

**검증된 로컬 결과 (예시):** health_check ✅ → news 8종목(RSS+본문) ✅ → gpt_analyzer ✅.  
스크리너 후보 수는 `min_trading_value_5d_avg_us`·`min_score_threshold`·모멘텀/변동성 필터에 따라 달라집니다(현재 기본: 점수≥0.48, 모멘텀/변동성 필터 OFF → 최대 ~8종).

`PYTHONPATH=src` 대신 `cd src` 후 실행해도 동일합니다. Docker는 `/app/src` 기준으로 동일 스크립트를 호출합니다.

### 7.4 환경 변수 참고

| 변수 | 용도 |
|------|------|
| `LOG_LEVEL` | `DEBUG` / `INFO` |
| `SCREENER_TIMEOUT_SEC` | 스크리너 subprocess 타임아웃 |
| `HEALTH_CHECK_TICKER_NAS` / `_NYS` | US 헬스체크 (기본 `AAPL`, `JPM`) |
| `KIS_TRACE` | `1` 시 해외 잔고·시세 파싱 디버그 로그 |
| `DB_RECORD_DEBUG` | `1` 시 DB·리컨실 단계별 `[DB_DEBUG]` 로그 |
| `PERF_REVIEW_PERIOD` | `integrated_manager` 월간/주간 유지보수 시 `monthly` / `weekly` |
| `EVIDENCE_RETAIN_TRADING_DAYS` | `cleanup_output` evidence JSON 보존 거래일 (기본 20) |

---

## 8. 프로젝트 구조

```
trading_bot_260530_NASDAQ/
├── config/
│   ├── config.json              # 전략·스케줄 (Git OK, // 한글 주석)
│   ├── .env.example
│   ├── .env.risk.example        # (참고용, 미사용 — 웹훅은 .env 에 설정)
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
│   ├── account_snapshot.py      # KIS AccountSnapshot (매매 primary)
│   ├── order_reconciler.py
│   ├── performance_review.py    # KIS endpoint 사후 리뷰 (API 호출 금지)
│   ├── reviewer.py              # performance_review wrapper
│   ├── cleanup_output.py
│   ├── rotation_policy.py / rotation_manager.py
│   ├── integrated_manager.py
│   ├── db_debug.py              # DB_RECORD_DEBUG 헬퍼
│   └── utils.py                 # pipeline session, norm_ticker, fmt_money, …
├── tests/
│   └── test_performance_review_kis_endpoints.py
├── run_integrated_manager.py
├── run_background_risk_manager.py
├── docker-compose.yml
├── Dockerfile                   # PYTHONPATH=/app/src (python -m performance_review)
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
| US 일봉·RSI·손절/ATR (`kis_market_data`) | ✅ `HHDFS76240000`; US pykrx/fdr **미로드** |
| SP500 pykrx/KRX 초기화 | ✅ `MARKET=SP500` 시 pykrx/fdr 지연 로드 스킵 |
| 스크리너 `--force` (휴장일 테스트) | ✅ 주말·NYSE 휴장일 수동 실행 |
| Google News RSS + US 본문 scrape | ✅ RSS dedupe·publisher URL resolve·`PARTIAL` status |
| GPT 1차 필터 config 연동 | ✅ `initial_filter.min_score_pass` 런타임 반영 |
| GPT 분석·`gpt_trades_*.json` | ✅ (US 프롬프트·USD 표기·Budget Guard) |
| 티커 정규화 (`normalize_ticker_6`) | ✅ trader·GPT·recorder·리컨실 등 |
| `Ticker` US 심볼 저장 | ✅ `AMZN` 형식 (zfill 미사용) |
| 해외 실주문 (`KIS.order_cash` → `overseas_order`) | ✅ 코드 연동 (`vps`/`prod`에서 검증 필요) |
| US 티커↔EXCD/OVRS_EXCG (`load_us_ticker_exchange_maps`) | ✅ NYSE/NAS/AMS 마스터·시세 EXCD 후보·주문 OVRS_EXCG 분리 |
| US 해외 매수 ORD_DVSN (`resolve_us_buy_order_params`) | ✅ `00`+슬리피지 지정가 · `APBK0952` 선제 캡·KIS `msg1` 노출 |
| US 해외 매도 ORD_DVSN (`resolve_us_sell_order_params`) | ✅ `00`+현재가 지정가 · DIRECT_SELL·trader·`ord_unpr=0` 차단 |
| EmergencyDrop·min_holding (14일) | ✅ -13% 급락 예외 · 매수일 기준 336h/14d · **PartialProfit 면제** |
| 장중 DIRECT_PARTIAL | ✅ `direct_execute_partial` · `partial_sell_flags` dedup |
| 해외 시세 EXCD 폴백·`HHDFS76200200` | ✅ `overseas_price` 후보 순회·상세 TR 폴백 (`risk_manager`·`trader`) |
| 해외 평가손익 (`evlu_pfls_*`) | ✅ KIS 0 반환 시 `pchs_avg_pric`·`prpr`로 재계산 (`kis_overseas_account`) |
| 해외 잔고 (`account.py` + `kis_overseas_account`) | ✅ USD·KRW 환산 필드 분리 |
| USD 예수금 (`extract_cash_from_summary`) | ✅ `frcr_dncl_amt_2` 등 CTRP6504R 필드 매핑 |
| `positions` 손절/목표·매도 우선 | ✅ `recorder` + `trader.run_sell_logic` |
| 회전 정책 (`rotation_policy`) | ✅ `trader` 리밸런스·`max_pairs_per_run` |
| 파이프라인 AM/PM·`trade_date` 연동 | ✅ 산출물 `_{date}_{am\|pm}_SP500`·env 전달 |
| 파이프라인 E2E (health → news → GPT) | ✅ 로컬 검증 (§7.3) |
| 장중 리스크·즉시매도 (`direct_execute`) | ✅ `pending` DB 기록 → 리컨실 `executed` |
| 리스크 세션 대기·세션 시작 재개 (`BackgroundRiskManager`) | ✅ `next_session_open_kst`·스레드 watchdog 재시작 · `22:30-05:00` KST |
| 리스크 Discord (`DISCORD_WEBHOOK_URL_RISK`) | ✅ `config/.env` 단일 로드 (`docker-compose`에 `.env.risk` 없음) |
| `order_reconciler`·`--backfill-only` | ✅ US: `inquire-ccnl`·`inquire-nccs` / KR: 일자별 주문으로 체결 해소 |
| US 일일 요약 open/close 짝 | ✅ `session_close_date`·`--capture-close` |
| US 일일 요약 Discord 표기 | ✅ USD 수익률 Primary·원화 참고·환율 분해·예수금 세분화·`ovrs_rlzt_pfls_amt` |
| `trader` 매도 체결 수량 (`_get_qty`) | ✅ 인스턴스 메서드·티커 정규화 (긴급 손절 등 매도 후 체결 확인) |
| `trader` 매도 사유 코드 | ✅ `EMERGENCY_DROP` 등 `risk_manager` 구조화 사유 매핑 |
| 해외 평단 (`pchs_avg_pric`) | ✅ 수량으로 재나누지 않음 (주당 평단) |
| 휴장일 판단 | ✅ US: NYSE(XNYS) / KR: 주말 — 기본은 **실행일** 기준; `--force`로 테스트 우회 |
| `Marcap` (US) | 마스터 미제공 → 0, 시총 필터 스킵 |
| `investor_flow` (US) | 0 (국내 수급 API 경로) |
| `dynamic_cash_management` | ⚠️ 보유 0·현금 100% 시 가용금 축소 가능 → 설정 확인 |
| KIS endpoint performance review | ✅ artifact/DB/log 기반 · evidence JSON · USD/KRW 분리 · ccnl 기간 coverage · API 직접 호출 없음 |
| `resolve_pipeline_context` account_snapshot trade_date | ✅ stale `PIPELINE_TRADE_DATE` 대신 resolved trade_date 저장 |
| `tests/test_performance_review_kis_endpoints.py` | ✅ 18 케이스 (present call_count·balance coverage·ccnl period·sellable evidence 등) |

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
