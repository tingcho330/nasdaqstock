# src/gpt_analyzer.py
import os
import re
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import contextlib

from dotenv import load_dotenv, find_dotenv
import httpx  # HTTP 폴백을 위한 경량 의존성 (이미 requirements에 있음)

# ────────────── 공통 유틸 ──────────────
from utils import (
    setup_logging,
    OUTPUT_DIR,
    load_config,
    find_latest_file,
    get_account_snapshot_cached,
    extract_cash_from_summary,
    convert_screener_data_to_trader_format,
    is_us_market,
    norm_ticker,
    normalize_ticker_6,
    fmt_money,
)

# ────────────── notifier 연동 ──────────────
from notifier import (
    DiscordLogHandler,
    WEBHOOK_URL,
    is_valid_webhook,
    send_discord_message,
)

# ────────────── 로깅 ──────────────
setup_logging()
logger = logging.getLogger("gpt_analyzer")

# 루트 로거에 디스코드 에러 핸들러 장착(중복 방지)
_root = logging.getLogger()
if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
    if not any(isinstance(h, DiscordLogHandler) for h in _root.handlers):
        _root.addHandler(DiscordLogHandler(WEBHOOK_URL))
        logger.info("DiscordLogHandler attached to root logger.")
else:
    logger.warning("유효한 DISCORD_WEBHOOK_URL이 없어 에러 로그의 디스코드 전송을 비활성화합니다.")

# ── 간단 쿨다운(스팸 방지) ──
_last_sent: Dict[str, float] = {}
def _notify(content: str, key: str, cooldown_sec: int = 120):
    """
    경량 텍스트 알림(최소화). 실패는 무시한다.
    """
    try:
        now = time.time()
        if key not in _last_sent or now - _last_sent[key] >= cooldown_sec:
            _last_sent[key] = now
            if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
                send_discord_message(content=content)
    except Exception:
        pass

def _notify_embed(title: str, description: str, fields: Optional[List[Dict[str, Any]]] = None):
    """
    최종 한 번만 쓰는 임베드 알림(스케줄러 요약과 중복 최소화).
    """
    if not (WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL)):
        return
    try:
        embed = {
            "title": title,
            "description": description,
            "type": "rich",
        }
        if fields:
            embed["fields"] = fields
        send_discord_message(content="", embeds=[embed])
    except Exception as e:
        logger.warning("알림(임베드) 전송 실패: %s", e)

# ────────────── .env 로딩 ──────────────
def _load_env() -> None:
    candidates = [
        Path("/app/config/.env"),
        Path(__file__).resolve().parents[1] / "config" / ".env",
        Path.cwd() / "config" / ".env",
        Path.cwd() / ".env",
    ]
    loaded = ""
    for p in candidates:
        try:
            if p.is_file():
                load_dotenv(dotenv_path=p, override=False)
                loaded = str(p)
                break
        except Exception:
            continue
    if not loaded:
        try:
            found = find_dotenv(usecwd=True)
            if found:
                load_dotenv(found, override=False)
                loaded = found
        except Exception:
            pass
    logger.info(f".env loaded from: {loaded if loaded else 'None'}")

_load_env()

# ────────────── 외부 의존(리포 내부) ──────────────
try:
    from src.screener import get_market_trend
except Exception:
    def get_market_trend(date_str: str) -> str:
        return "Sideways"

# ────────────── Config 및 OpenAI ──────────────
_config: Dict[str, Any] = load_config() or {}
_gpt_params = _config.get("gpt_params", {}) or {}
_trading_params = _config.get("trading_params", {}) or {}

OPENAI_MODEL = _gpt_params.get("openai_model", "gpt-5-mini")
_strategy_weights = _gpt_params.get("strategy_weights", {})

# ▼ 예산 가드 설정
_BUDGET_GUARD_ENABLED: bool = bool(_gpt_params.get("budget_guard", False))
_MAX_ENTRY_PRICE_RATIO: float = float(_gpt_params.get("max_entry_price_ratio", 0.95))
_CASH_BUFFER_RATIO: float = float(_trading_params.get("cash_buffer_ratio", 0.0))

_INITIAL_FILTER_CFG = _gpt_params.get("initial_filter", {}) or {}
_IF_MIN_SCORE_PASS = float(_INITIAL_FILTER_CFG.get("min_score_pass", 0.52))
_IF_MIN_SCORE_NO_NEWS = float(_INITIAL_FILTER_CFG.get("min_score_no_news", 0.52))
_IF_HEURISTIC_MIN_SCORE = float(_INITIAL_FILTER_CFG.get("heuristic_min_score", 0.52))
_IF_MIN_NEWS_CHARS = int(_INITIAL_FILTER_CFG.get("min_news_chars", 200))

# ────────────── GPT 백엔드 계층 ──────────────
class _GPTBackendBase:
    name = "base"
    def create_json(self, model: str, system_prompt: str, user_prompt: str, timeout: int = 30) -> Optional[dict]:
        raise NotImplementedError

class _GPTBackendOpenAIv1(_GPTBackendBase):
    name = "openai_v1"
    def __init__(self, api_key: str):
        from openai import OpenAI  # pydantic_core 불일치 시 여기서 터짐
        self.client = OpenAI(api_key=api_key)
    def create_json(self, model: str, system_prompt: str, user_prompt: str, timeout: int = 30) -> Optional[dict]:
        resp = self.client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
            timeout=timeout,
        )
        txt = (resp.choices[0].message.content or "").strip()
        return json.loads(_strip_to_json(txt)) if txt else None

class _GPTBackendOpenAILegacy(_GPTBackendBase):
    name = "openai_legacy"
    def __init__(self, api_key: str):
        import openai  # 레거시 0.x 계열
        openai.api_key = api_key
        self.openai = openai
    def create_json(self, model: str, system_prompt: str, user_prompt: str, timeout: int = 30) -> Optional[dict]:
        # 레거시는 response_format 미지원 → 프롬프트 유도 + JSON 파싱
        resp = self.openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt + " 반드시 단일 JSON만 출력."},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            request_timeout=timeout,
        )
        txt = (resp["choices"][0]["message"]["content"] or "").strip()
        return json.loads(_strip_to_json(txt)) if txt else None

class _GPTBackendHTTP(_GPTBackendBase):
    name = "http"
    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
    def create_json(self, model: str, system_prompt: str, user_prompt: str, timeout: int = 30) -> Optional[dict]:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
        }
        # v1 포맷 호환: response_format이 있으면 사용 (없어도 문제 없음)
        payload["response_format"] = {"type": "json_object"}
        with httpx.Client(timeout=timeout) as cli:
            r = cli.post(url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()
            txt = (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
            return json.loads(_strip_to_json(txt)) if txt else None

def _init_gpt_backend() -> tuple[Optional[_GPTBackendBase], bool]:
    api_key = os.getenv("OPENAI_API_KEY", "")
    force_http = os.getenv("FORCE_GPT_HTTP", "0") == "1"
    if not api_key:
        logger.warning("OPENAI_API_KEY 미설정. 휴리스틱 모드로 동작합니다.")
        _notify("ℹ️ OPENAI_API_KEY 미설정 → 휴리스틱 분석으로 진행", key="gpt_analyzer_no_api", cooldown_sec=600)
        return None, False
    # 강제 HTTP 백엔드
    if force_http:
        try:
            be = _GPTBackendHTTP(api_key)
            logger.info("GPT 백엔드: HTTP 직접 호출 사용(FORCE_GPT_HTTP=1).")
            return be, True
        except Exception as e:
            logger.warning(f"HTTP 백엔드 초기화 실패: {e}")
            return None, False
    # 1) 신규 SDK
    with contextlib.suppress(Exception):
        be = _GPTBackendOpenAIv1(api_key)
        logger.info("GPT 백엔드: OpenAI SDK v1 사용.")
        return be, True
    # 2) 레거시 SDK
    with contextlib.suppress(Exception):
        be = _GPTBackendOpenAILegacy(api_key)
        logger.info("GPT 백엔드: OpenAI 레거시 SDK 사용.")
        return be, True
    # 3) HTTP 폴백
    with contextlib.suppress(Exception):
        be = _GPTBackendHTTP(api_key)
        logger.info("GPT 백엔드: HTTP 직접 호출 사용(폴백).")
        return be, True
    # 모두 실패
    _notify("ℹ️ OpenAI 초기화 실패 → 휴리스틱 분석으로 진행", key="gpt_analyzer_openai_fail", cooldown_sec=600)
    return None, False

_gpt_backend, _OPENAI_AVAILABLE = _init_gpt_backend()

# ────────────── 프롬프트 템플릿 ──────────────
INITIAL_FILTER_PROMPT_TMPL = (
    "You are an expert Korean stock market analyst specializing in short-term swing trading. "
    "Your task is to perform rapid initial screening based on quantitative scores and news analysis. "
    "Be decisive and focus on actionable insights. Your response must be a single, valid JSON object.\n\n"
    "**Input Data:**\n"
    "- 종목명: {name}\n"
    "- 정량적 점수: {score:.3f}\n"
    "- 최근 뉴스 요약 (최대 1500자):\n"
    "{news_text}\n\n"
    "**판단 기준:**\n"
    "1. 뉴스가 명백히 부정적(스캔들, 소송, 실적 악화)이면 '보류' 권장\n"
    "2. 정량적 점수가 매우 낮으면(60 미만) 뉴스에 강력한 촉매가 없으면 회의적 접근\n"
    "3. 좋은 점수와 긍정적 뉴스의 조합을 찾아라\n"
    "4. 한국 시장 특성(기관 매매, 외국인 자금 흐름) 고려\n\n"
    "**--- 필수 JSON 출력 (한국어) ---**\n"
    "{{\"decision\": \"<매수 고려 or 보류>\", \"reason\": \"<결정의 핵심 이유를 한 문장으로 간결하게 설명>\"}}"
)

INITIAL_FILTER_PROMPT_US_TMPL = (
    "You are an expert US equity analyst for S&P 500 swing trading (regular session, USD). "
    "Perform rapid screening from quantitative scores (0.0–1.0 scale, NOT 0–100) and news. "
    "Respond with a single valid JSON object only.\n\n"
    "**Input:**\n"
    "- Symbol / Name: {name}\n"
    "- Quantitative score: {score:.3f} (higher is better; typical buy threshold ≥ 0.52)\n"
    "- Recent news summary (max 1500 chars):\n{news_text}\n\n"
    "**US market rules:**\n"
    "1. Clearly negative news (fraud, major lawsuit, guidance cut, SEC issues) → recommend hold (보류)\n"
    "2. Score < 0.55 without a strong positive catalyst → skeptical / hold\n"
    "3. Prefer combination of solid score and constructive news (earnings beat, product catalyst, analyst upgrade)\n"
    "4. Consider US context: mega-cap liquidity, after-hours headline risk, sector rotation (tech-heavy index)\n"
    "5. Do NOT cite Korean-market flows (foreigner/institution net buy on KRX)\n\n"
    '**JSON (Korean keys, reason may be Korean or English):** '
    '{{"decision": "<매수 고려 or 보류>", "reason": "<one concise sentence>"}}'
)

TACTICAL_PLAN_PROMPT_TMPL = (
    "당신은 한국 주식시장 전문 투자 전략가입니다. 제공된 모든 데이터를 면밀히 분석하여 실행 가능한 최종 매매 계획을 수립하세요. "
    "응답은 반드시 단일 유효한 JSON 객체여야 하며, 다른 텍스트나 설명은 포함하지 마세요. "
    "점수 분석에 특히 주의를 기울이세요; 약한 시장에서 높은 섹터 점수는 시장 선도 테마를 나타낼 수 있습니다. "
    "패턴 분석을 활용하여 신뢰도를 높이세요.\n\n"
    "**시장 및 종목 데이터:**\n"
    "- 전체 시장 추세: {market_trend}\n"
    "- 종목: {name} ({ticker})\n"
    "- 섹터: {stock_sector}\n"
    "- 현재가: {price:,.0f}원\n\n"
    "**점수 프로필:**\n"
    "- 전체 점수: {score:.4f}\n"
    "- 점수 세부: 재무={fin_score:.4f}, 기술={tech_score:.4f}, 시장={mkt_score:.4f}, 섹터={sector_score:.4f}\n"
    "- 패턴 점수: {pattern_score:.4f}\n\n"
    "- 재무 지표: PER={per}, PBR={pbr}\n"
    "- 기술 지표:\n"
    "  • RSI: {rsi:.2f}\n"
    "  • 50일 이동평균: {ma50:,.0f}원\n"
    "  • 200일 이동평균: {ma200:,.0f}원\n\n"
    "**차트 및 거래량 패턴 분석:**\n"
    "- 20일 이동평균 상승?: {is_ma20_rising}\n"
    "- 누적 거래량 감지?: {is_accumulation_volume}\n"
    "- 상승 저점 패턴?: {has_higher_lows}\n"
    "- 급등 후 정체?: {is_consolidating}\n"
    "- 양음양 캔들 패턴?: {has_yey_pattern}\n\n"
    "- 최근 뉴스:\n"
    "{news_text}\n"
    "{budget_guard_block}\n"
    "**사용 가능한 고급 전략 (하나 선택):**\n"
    "1. `RsiReversalStrategy` - RSI 역전 신호 기반\n"
    "2. `TrendFollowingStrategy` - 추세 추종 전략\n"
    "3. `AdvancedTechnicalStrategy` - 고급 기술 분석\n"
    "4. `DynamicAtrStrategy` - 동적 ATR 기반\n"
    "5. `BaseStrategy` - 기본 전략\n\n"
    "**--- 필수 JSON 객체 구조 ---**\n"
    "{{\n"
    "  \"결정\": \"<매수 or 보류>\",\n"
    "  \"분석\": \"<**[중요]** '매수' 또는 '보류' 결정에 대한 핵심적인 종합 분석. **특히 점수 세부(특히 섹터 점수)와 패턴 분석 결과를 반드시 분석에 반영할 것.** 결정을 '보류'할 경우, 그 이유를 명확히 서술할 것.>\",\n"
    "  \"전략_클래스\": \"<위 5가지 전략 중 가장 적합한 것 하나를 선택>\",\n"
    "  \"매매전술\": \"<'집중 투자 전략' 또는 '분할 매수 전략' 중 선택>\",\n"
    "  \"parameters\": {{\"installments\": []}}\n"
    "}}"
)

TACTICAL_PLAN_PROMPT_US_TMPL = (
    "You are a US equity strategist for S&P 500 swing trades. "
    "All share prices, moving averages, stop/target levels, and budget figures are in **USD**. "
    "Output a single JSON object only (Korean field names below).\n\n"
    "**Market regime:** {market_trend} (Bull / Bear / Sideways — US large-cap tech context)\n"
    "**Stock:** {name} ({ticker}) | GICS/Sector: {stock_sector}\n"
    "**Last price:** {price_str}\n\n"
    "**Score profile (0–1):**\n"
    "- Total: {score:.4f}\n"
    "- Breakdown: financial={fin_score:.4f}, technical={tech_score:.4f}, "
    "market={mkt_score:.4f}, sector={sector_score:.4f}, pattern={pattern_score:.4f}\n\n"
    "**Fundamentals & technicals:**\n"
    "- PER={per}, PBR={pbr} (US GAAP; null if unavailable)\n"
    "- RSI(14): {rsi:.2f}\n"
    "- MA50: {ma50_str} | MA200: {ma200_str}\n\n"
    "**Pattern flags:**\n"
    "- MA20 rising: {is_ma20_rising}\n"
    "- Accumulation volume: {is_accumulation_volume}\n"
    "- Higher lows: {has_higher_lows}\n"
    "- Post-rally consolidation: {is_consolidating}\n"
    "- Y-E-Y candle pattern: {has_yey_pattern}\n\n"
    "**News (US sources):**\n{news_text}\n"
    "{budget_guard_block}\n"
    "**Pick one strategy class:**\n"
    "1. `RsiReversalStrategy` — oversold bounce\n"
    "2. `TrendFollowingStrategy` — trend continuation\n"
    "3. `AdvancedTechnicalStrategy` — multi-factor technical\n"
    "4. `DynamicAtrStrategy` — ATR-based stops/sizing\n"
    "5. `BaseStrategy` — conservative default\n\n"
    "**Tactics:** `집중 투자 전략` (high conviction) or `분할 매수 전략` (scale in).\n\n"
    "**Required JSON (한국어 필드명):**\n"
    "{{\n"
    '  "결정": "<매수 or 보류>",\n'
    '  "분석": "<Summarize in Korean or English; cite prices as $X.XX USD when mentioning levels>",\n'
    '  "전략_클래스": "<one of the five classes>",\n'
    '  "매매전술": "<집중 투자 전략 or 분할 매수 전략>",\n'
    '  "parameters": {{"installments": []}}\n'
    "}}\n"
    "If budget guard says LOW_FUNDS, set 결정 to 보류 and explain insufficient USD buying power."
)

REBALANCE_PROMPT_KR_TMPL = """
당신은 한국 주식시장 전문 포트폴리오 매니저입니다.
현재 보유 종목과 새로운 후보 종목들을 비교 분석하여 리밸런싱 결정을 내려야 합니다.

**현재 보유 종목:**
{holdings_json}

**새로운 후보 종목 (Screener 상위):**
{candidates_json}

**시장 상황:**
- 시장 추세: {market_trend}
- 분석 일자: {analysis_date}

**분석 기준:**
1. 종목별 점수 (Score, FinScore, TechScore, MktScore, SectorScore)
2. 기술적 지표 (RSI, MA20, MA50, MA200)
3. 패턴 분석 (MA20Up, AccumVol, HigherLows, Consolidation, YEY)
4. 포트폴리오 다양성 (섹터 분산)
5. 리스크 관리 (보유일수, 현재 수익률)

**응답 형식 (JSON):**
{{
  "rebalance_decisions": [
    {{
      "action": "KEEP" | "SELL" | "BUY",
      "ticker": "종목코드",
      "name": "종목명",
      "reason": "판단 이유",
      "confidence": 0.0-1.0,
      "priority": 1-10
    }}
  ],
  "portfolio_analysis": {{
    "sector_diversification": "섹터 분산도 분석",
    "risk_assessment": "리스크 평가",
    "recommendations": "전체 포트폴리오 개선 제안"
  }}
}}

**중요 지침:**
- 보유 종목 중 점수가 낮고 기술적 지표가 약한 종목은 SELL 고려
- 새로운 후보 중 점수가 높고 기술적 패턴이 강한 종목은 BUY 고려
- 섹터 집중도를 고려하여 포트폴리오 다양성 유지
- 최대 3-5개의 리밸런싱 결정만 제안
- 각 결정에 대한 명확한 이유와 신뢰도 제공
"""

REBALANCE_PROMPT_US_TMPL = """
You are a US equity portfolio manager rebalancing an S&P 500-focused account.
All quoted prices and position values are in **USD**. Ticker symbols are US (e.g. AAPL, MSFT).

**Current holdings:**
{holdings_json}

**New screener candidates (top ranks):**
{candidates_json}

**Market context:**
- Regime: {market_trend}
- Analysis date: {analysis_date} (YYYYMMDD, US session reference in KST pipeline)

**Evaluation criteria:**
1. Scores (0–1): Score, FinScore, TechScore, MktScore, SectorScore
2. Technicals: RSI, MA20Up, MA50/MA200 levels (USD)
3. Patterns: AccumVol, HigherLows, Consolidation, YEY
4. Sector diversification (avoid excessive tech concentration unless justified)
5. Risk: holding_days, unrealized P&L if provided

**Response (JSON only):**
{{
  "rebalance_decisions": [
    {{
      "action": "KEEP" | "SELL" | "BUY",
      "ticker": "<US symbol>",
      "name": "<company name>",
      "reason": "<brief reason; use $ amounts in USD when citing prices>",
      "confidence": 0.0-1.0,
      "priority": 1-10
    }}
  ],
  "portfolio_analysis": {{
    "sector_diversification": "<sector exposure comment>",
    "risk_assessment": "<risk comment>",
    "recommendations": "<portfolio-level suggestions in Korean or English>"
  }}
}}

**Guidelines:**
- SELL weak holdings (low score, broken technicals, negative news)
- BUY only strong candidates with clear edge vs existing holdings
- Limit to 3–5 decisions; prefer quality over churn
- Respect US liquidity and regular-session execution assumptions
"""


def _market_env(market: Optional[str] = None) -> str:
    return market or os.getenv("MARKET", "SP500")


def _gpt_system_initial(market: Optional[str] = None) -> str:
    if is_us_market(_market_env(market)):
        return (
            "You are a fast S&P 500 equity analyst. "
            "Quantitative scores use a 0.0–1.0 scale (not 0–100). "
            "Reply with a single JSON object only."
        )
    return "You are a fast investment analyst. Always reply with a single JSON object only."


def _gpt_system_tactical(market: Optional[str] = None) -> str:
    if is_us_market(_market_env(market)):
        return (
            "You are Chief Investment Strategist for US equities (S&P 500). "
            "Express all monetary values in USD ($). Output must be a single JSON object ONLY."
        )
    return "You are a Chief Investment Strategist. Output must be a single JSON object ONLY."


def _gpt_system_rebalance(market: Optional[str] = None) -> str:
    if is_us_market(_market_env(market)):
        return (
            "You are a professional US equity portfolio manager (S&P 500). "
            "All prices and notionals are USD. Output a single JSON object only."
        )
    return "당신은 전문 포트폴리오 매니저입니다. 한국 주식시장의 리밸런싱 결정을 내려야 합니다."


def _format_rebalance_rows(rows: List[Dict], market: Optional[str] = None) -> List[Dict]:
    """리밸런싱 프롬프트용 — US면 price 필드를 USD 문자열로 표기."""
    mkt = _market_env(market)
    if not is_us_market(mkt):
        return rows
    formatted: List[Dict] = []
    for row in rows:
        item = dict(row)
        raw = item.get("price")
        if raw is not None and raw != "":
            try:
                item["price"] = fmt_money(float(raw), mkt)
            except (TypeError, ValueError):
                pass
        formatted.append(item)
    return formatted


# ────────────── 헬퍼들 ──────────────
def _read_json(p: Path) -> Any:
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def _detect_files(fixed_date: str, market: str):
    preferred = OUTPUT_DIR / f"screener_candidates_{fixed_date}_{market}.json"
    if preferred.exists():
        return preferred, OUTPUT_DIR / f"collected_news_{fixed_date}_{market}.json"

    fallbacks = [
        OUTPUT_DIR / f"screener_candidates_full_{fixed_date}_{market}.json",
        OUTPUT_DIR / f"screener_rank_{fixed_date}_{market}.json",
        OUTPUT_DIR / f"screener_rank_full_{fixed_date}_{market}.json",
    ]
    for fb in fallbacks:
        if fb.exists():
            return fb, OUTPUT_DIR / f"collected_news_{fixed_date}_{market}.json"

    legacy = OUTPUT_DIR / f"screener_results_{fixed_date}_{market}.json"
    return legacy, OUTPUT_DIR / f"collected_news_{fixed_date}_{market}.json"

def _detect_latest_screener_file() -> Optional[Path]:
    patterns = [
        "screener_candidates_*.json",
        "screener_candidates_full_*.json",
        "screener_rank_*.json",
        "screener_rank_full_*.json",
        "screener_results_*_*.json",
    ]
    for pat in patterns:
        p = find_latest_file(pat)
        if p:
            return p
    return None

def _to_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _normalize_candidates(cands: List[Dict]) -> List[Dict]:
    out = []
    for c in cands:
        item = dict(c)
        if not item.get("Ticker"):
            if item.get("Code"):
                item["Ticker"] = norm_ticker(item["Code"], os.getenv("MARKET", "SP500"))
        if not item.get("Name"):
            for k in ["Name", "종목명", "name"]:
                if c.get(k):
                    item["Name"] = str(c[k])
                    break

        def _f(key, default=0.0):
            try:
                return float(item.get(key, default))
            except Exception:
                return float(default)

        item["Score"]        = _f("Score")
        item["FinScore"]     = _f("FinScore")
        item["TechScore"]    = _f("TechScore")
        item["MktScore"]     = _f("MktScore")
        item["SectorScore"]  = _f("SectorScore")
        item["PatternScore"] = _f("PatternScore")
        item["Price"]        = _f("Price", 0.0)
        item["RSI"]          = _f("RSI", 50.0)
        item["ATR"]          = _to_float(item.get("ATR"), None)
        item["MA50"]         = _f("MA50", 0.0)
        item["MA200"]        = _f("MA200", 0.0)

        item["PER"]          = _to_float(item.get("PER"), None) if item.get("PER") is not None else None
        item["PBR"]          = _to_float(item.get("PBR"), None) if item.get("PBR") is not None else None

        for b in ["MA20Up","AccumVol","HigherLows","Consolidation","YEY"]:
            item[b] = bool(item.get(b, False))

        if item.get("Sector") is None and c.get("Industry"):
            item["Sector"] = c.get("Industry")
        item["Sector"] = item.get("Sector", "N/A")
        item["SectorSource"] = item.get("SectorSource", None)

        stop_price = item.get("stop_price", item.get("손절가"))
        target_price = item.get("target_price", item.get("목표가"))
        levels_source = item.get("levels_source", item.get("source"))
        item["stop_price"] = int(round(float(stop_price))) if stop_price is not None else None
        item["target_price"] = int(round(float(target_price))) if target_price is not None else None
        item["levels_source"] = levels_source if levels_source is not None else None

        if "daily_chart" in c:
            item["daily_chart"] = c["daily_chart"]
        if "investor_flow" in c:
            item["investor_flow"] = c["investor_flow"]

        out.append(item)
    return out

def _strip_to_json(text: str) -> str:
    text = text.strip()
    if text.startswith("{") and text.endswith("}"):
        return text
    candidates = re.findall(r"\{.*\}", text, flags=re.DOTALL)
    if candidates:
        return max(candidates, key=len)
    return text

def _safe_json_loads(text: str) -> Optional[dict]:
    try:
        return json.loads(text)
    except Exception:
        return None

def _call_openai_json(system_prompt: str, user_prompt: str, max_retries: int = 3) -> Optional[dict]:
    if not _OPENAI_AVAILABLE or _gpt_backend is None:
        logger.debug("OpenAI 백엔드 없음 → 휴리스틱 모드로 전환")
        return None
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            result = _gpt_backend.create_json(OPENAI_MODEL, system_prompt, user_prompt, timeout=30)
            if result:
                logger.debug(f"OpenAI 호출 성공({_gpt_backend.name}) {attempt}/{max_retries}")
                return result
            logger.warning(f"OpenAI 응답 파싱 실패({_gpt_backend.name}) {attempt}/{max_retries}")
        except Exception as e:
            last_err = e
            logger.warning(f"OpenAI 호출 실패({_gpt_backend.name}) {attempt}/{max_retries}: {e}")
            time.sleep(1)
    _notify(f"⚠️ OpenAI 호출 최종 실패({_gpt_backend.name}): {str(last_err)[:400]}", key="gpt_analyzer_call_fail", cooldown_sec=300)
    return None

def _pretty_print_plans(plans: List[Dict], market: Optional[str] = None) -> None:
    mkt = _market_env(market)
    if not plans:
        print("\n--- 매수 계획 없음 ---")
        return
    print("\n=== ✨ 생성된 매수 계획 ===")
    for i, plan in enumerate(plans, 1):
        stock = plan.get("stock_info", {})
        name = stock.get("Name", "N/A")
        ticker = norm_ticker(stock.get("Ticker", "N/A"), mkt)
        strategy = plan.get("전략_클래스", "N/A")
        tactic = plan.get("매매전술", "N/A")
        decision = plan.get("결정", "N/A")
        reason = (plan.get("분석", "") or "")[:300]
        stop_px = stock.get("stop_price")
        tgt_px = stock.get("target_price")
        source = stock.get("levels_source")
        print(f"\n[{i}] {name} ({ticker}) - {decision}")
        print(f" - 전략: {strategy}")
        print(f" - 전술: {tactic}")
        if stop_px and tgt_px and decision == "매수":
            try:
                sl = fmt_money(float(stop_px), mkt)
                tp = fmt_money(float(tgt_px), mkt)
                print(f" - 손절/목표: {sl} / {tp}  (source={source})")
            except Exception:
                print(f" - 손절/목표: {stop_px} / {tgt_px}  (source={source})")
        print(f" - 근거: {reason}...")

def _apply_strategy_weights(selected: str, c: Dict, market_trend: str, weights: Dict[str, float]) -> str:
    rsi = float(c.get("RSI", 50.0))
    ma20 = bool(c.get("MA20Up", False))
    hl = bool(c.get("HigherLows", False))
    cons = bool(c.get("Consolidation", False))
    psc = float(c.get("PatternScore", 0.0))
    tech = float(c.get("TechScore", 0.5))

    tf = 0.5 + (0.2 if ma20 else 0.0) + (0.2 if hl else 0.0) + (0.1 if market_trend in ("Bull","Sideways") else 0.0)
    rr = 0.5 + (0.25 if rsi < 40 else 0.0) + (0.15 if not ma20 else 0.0) + (0.1 if not cons else 0.0)
    at = 0.4 + min(0.6, psc)
    da = 0.4 + min(0.5, tech)
    bs = 0.5

    ctx = {
        "TrendFollowingStrategy": tf,
        "RsiReversalStrategy": rr,
        "AdvancedTechnicalStrategy": at,
        "DynamicAtrStrategy": da,
        "BaseStrategy": bs,
    }
    scored = {k: ctx.get(k, 0.0) * float(weights.get(k, 0.5)) for k in ctx.keys()}
    return max(scored.items(), key=lambda kv: (kv[1], 1.0 if kv[0] == selected else 0.0))[0]

def _initial_filter_gpt(
    name: str, score: float, news_text: str, market: Optional[str] = None
) -> Optional[dict]:
    mkt = _market_env(market)
    sys = _gpt_system_initial(mkt)
    tmpl = INITIAL_FILTER_PROMPT_US_TMPL if is_us_market(mkt) else INITIAL_FILTER_PROMPT_TMPL
    user = tmpl.format(name=name, score=score, news_text=news_text[:1500])
    return _call_openai_json(sys, user)

def _build_budget_guard_block(
    enabled: bool,
    usable_cash: Optional[int],
    ratio: float,
    buffer_ratio: float,
    price: float,
    score: float,
) -> str:
    """
    예산 가드 프롬프트 블록을 생성한다(조건부).
    """
    if not enabled or usable_cash is None:
        return ""
    max_entry = int(usable_cash * ratio)
    mkt = os.getenv("MARKET", "SP500")
    if is_us_market(mkt):
        return (
            "\n**Budget Guard (USD — overseas account buying power):**\n"
            f"- usable_cash = {fmt_money(float(usable_cash), mkt)}\n"
            f"- max_allowed_entry_price (per slot) = {fmt_money(float(max_entry), mkt)}\n"
            f"- candidate_last_price = {fmt_money(price, mkt)} | score = {score:.4f}\n"
            "- Recommend '매수' only if one share at candidate price fits within usable_cash.\n"
            "- If insufficient USD, set 결정 to '보류' and mention LOW_FUNDS in 분석.\n"
        )
    return (
        "\n**Budget Guard (예산 가드):**\n"
        f"- usable_cash = {usable_cash:,} KRW\n"
        f"- max_allowed_entry_price = {max_entry:,} KRW\n"
        f"- candidate_price = {int(price):,} KRW | candidate_score = {score:.4f}\n"
    )

def _tactical_plan_gpt(
    market_trend: str,
    c: Dict,
    news_text: str,
    budget_ctx: Optional[Dict[str, Any]] = None,
    market: Optional[str] = None,
) -> Optional[dict]:
    mkt = market or os.getenv("MARKET", "SP500")
    name = c.get("Name", "N/A")
    ticker = norm_ticker(c.get("Ticker", "N/A"), mkt)
    sector = c.get("Sector", "N/A")
    price = float(c.get("Price", 0.0))
    ma50 = float(c.get("MA50", 0.0))
    ma200 = float(c.get("MA200", 0.0))

    budget_block = _build_budget_guard_block(
        enabled=bool(budget_ctx and budget_ctx.get("enabled")),
        usable_cash=budget_ctx.get("usable_cash") if budget_ctx else None,
        ratio=float(budget_ctx.get("max_entry_price_ratio", 0.95)) if budget_ctx else 0.95,
        buffer_ratio=float(budget_ctx.get("cash_buffer_ratio", 0.0)) if budget_ctx else 0.0,
        price=price,
        score=float(c.get("Score", 0.0)),
    )

    sys = _gpt_system_tactical(mkt)
    if is_us_market(mkt):
        user = TACTICAL_PLAN_PROMPT_US_TMPL.format(
            market_trend=market_trend,
            name=name, ticker=ticker, stock_sector=sector,
            price_str=fmt_money(price, mkt),
            score=float(c.get("Score", 0.0)),
            fin_score=float(c.get("FinScore", 0.0)),
            tech_score=float(c.get("TechScore", 0.0)),
            mkt_score=float(c.get("MktScore", 0.0)),
            sector_score=float(c.get("SectorScore", 0.0)),
            pattern_score=float(c.get("PatternScore", 0.0)),
            per=("null" if c.get("PER") is None else c.get("PER")),
            pbr=("null" if c.get("PBR") is None else c.get("PBR")),
            rsi=float(c.get("RSI", 0.0)),
            ma50_str=fmt_money(ma50, mkt),
            ma200_str=fmt_money(ma200, mkt),
            is_ma20_rising=bool(c.get("MA20Up", False)),
            is_accumulation_volume=bool(c.get("AccumVol", False)),
            has_higher_lows=bool(c.get("HigherLows", False)),
            is_consolidating=bool(c.get("Consolidation", False)),
            has_yey_pattern=bool(c.get("YEY", False)),
            news_text=(news_text or "")[:1500],
            budget_guard_block=budget_block,
        )
    else:
        user = TACTICAL_PLAN_PROMPT_TMPL.format(
            market_trend=market_trend,
            name=name, ticker=ticker, stock_sector=sector, price=price,
            score=float(c.get("Score", 0.0)),
            fin_score=float(c.get("FinScore", 0.0)),
            tech_score=float(c.get("TechScore", 0.0)),
            mkt_score=float(c.get("MktScore", 0.0)),
            sector_score=float(c.get("SectorScore", 0.0)),
            pattern_score=float(c.get("PatternScore", 0.0)),
            per=("null" if c.get("PER") is None else c.get("PER")),
            pbr=("null" if c.get("PBR") is None else c.get("PBR")),
            rsi=float(c.get("RSI", 0.0)),
            ma50=ma50, ma200=ma200,
            is_ma20_rising=bool(c.get("MA20Up", False)),
            is_accumulation_volume=bool(c.get("AccumVol", False)),
            has_higher_lows=bool(c.get("HigherLows", False)),
            is_consolidating=bool(c.get("Consolidation", False)),
            has_yey_pattern=bool(c.get("YEY", False)),
            news_text=(news_text or "")[:1500],
            budget_guard_block=budget_block,
        )
    return _call_openai_json(sys, user)

def _heuristic_plan(
    c: Dict,
    news_text: str,
    market_trend: str,
    market: Optional[str] = None,
) -> Dict:
    """
    고도화된 휴리스틱 분석 로직
    패턴 분석, 기술적 지표, 시장 상황을 종합적으로 고려한 지능적 분석
    """
    # 기본 지표 추출
    score = float(c.get("Score", 0.0))
    rsi = float(c.get("RSI", 50.0))
    pattern_score = float(c.get("PatternScore", 0.0))
    tech_score = float(c.get("TechScore", 0.5))
    sector_score = float(c.get("SectorScore", 0.5))
    fin_score = float(c.get("FinScore", 0.5))
    mkt_score = float(c.get("MktScore", 0.5))
    
    # 가격 및 이동평균
    price = float(c.get("Price", 0.0))
    ma50 = float(c.get("MA50", 0.0))
    ma200 = float(c.get("MA200", 0.0))
    
    # 패턴 분석 결과
    ma20_up = bool(c.get("MA20Up", False))
    accum_vol = bool(c.get("AccumVol", False))
    higher_lows = bool(c.get("HigherLows", False))
    consolidation = bool(c.get("Consolidation", False))
    yey_pattern = bool(c.get("YEY", False))
    
    # 고급 분석 지표
    per = c.get("PER")
    pbr = c.get("PBR")
    atr = c.get("ATR", 0.0)
    
    # 1. 종합 점수 계산 (가중치 적용)
    pattern_bonus = 0.0
    risk_penalty = 0.0
    
    # 패턴 보너스 (강한 신호일수록 높은 보너스)
    if ma20_up:
        pattern_bonus += 0.08  # 강한 추세 신호
    if accum_vol:
        pattern_bonus += 0.06  # 거래량 확인
    if higher_lows:
        pattern_bonus += 0.07  # 상승 패턴
    if consolidation:
        pattern_bonus += 0.03  # 정체 후 돌파 가능성
    if yey_pattern:
        pattern_bonus += 0.10  # 강한 반전 신호
    
    # 리스크 페널티
    if rsi > 80:
        risk_penalty += 0.05  # 과매수
    elif rsi < 20:
        risk_penalty += 0.03  # 과매도 (하지만 기회일 수도)
    
    if per is not None and per > 50:
        risk_penalty += 0.03  # 고평가
    if pbr is not None and pbr > 3:
        risk_penalty += 0.02  # 고PBR
    
    # 시장 상황 고려
    market_bonus = 0.0
    if market_trend == "Bull":
        market_bonus = 0.05
    elif market_trend == "Bear":
        market_bonus = -0.05
    
    # 최종 조정 점수
    adjusted_score = min(1.0, max(0.0, score + pattern_bonus - risk_penalty + market_bonus))
    
    # 2. 매수 결정 로직 (다단계 검증)
    decision = "보류"
    confidence = 0.0
    
    # 강한 매수 신호
    if adjusted_score >= 0.80 and (ma20_up or accum_vol) and rsi < 70:
        decision = "매수"
        confidence = 0.9
    # 중간 매수 신호
    elif adjusted_score >= 0.70 and (ma20_up or higher_lows) and rsi < 75:
        decision = "매수"
        confidence = 0.7
    # 약한 매수 신호 (조건부)
    elif adjusted_score >= 0.65 and (ma20_up and accum_vol) and rsi < 80:
        decision = "매수"
        confidence = 0.6
    # 특별한 경우 (강한 패턴)
    elif adjusted_score >= 0.60 and (yey_pattern or (ma20_up and higher_lows and accum_vol)):
        decision = "매수"
        confidence = 0.8
    
    # 3. 전략 선택 로직 (상황별 최적화)
    if decision == "매수":
        # RSI 역전 전략
        if rsi < 35 and not ma20_up and pattern_score < 0.3:
            base_strategy = "RsiReversalStrategy"
        # 추세 추종 전략
        elif ma20_up and higher_lows and market_trend in ["Bull", "Sideways"]:
            base_strategy = "TrendFollowingStrategy"
        # 고급 기술 분석 전략
        elif pattern_score > 0.6 and tech_score > 0.7:
            base_strategy = "AdvancedTechnicalStrategy"
        # 동적 ATR 전략
        elif tech_score > 0.8 and atr > 0:
            base_strategy = "DynamicAtrStrategy"
        # 기본 전략
        else:
            base_strategy = "BaseStrategy"
    else:
        base_strategy = "BaseStrategy"
    
    # 4. 매매 전술 결정 (리스크 관리)
    if decision == "매수":
        if confidence >= 0.8 and adjusted_score >= 0.8:
            tactic = "집중 투자 전략"
        elif confidence >= 0.7:
            tactic = "분할 매수 전략"
        else:
            tactic = "소액 분할 매수 전략"
    else:
        tactic = "관망 전략"
    
    # 5. 상세 분석 이유 구성
    analysis_parts = []
    
    mkt = _market_env(market)
    price_note = fmt_money(price, mkt) if price > 0 else "N/A"

    # 점수 분석
    score_change = adjusted_score - score
    analysis_parts.append(f"Score={score:.3f}→{adjusted_score:.3f}")
    analysis_parts.append(f"Px={price_note}")
    if score_change > 0:
        analysis_parts.append(f"(+{score_change:.3f})")
    elif score_change < 0:
        analysis_parts.append(f"({score_change:.3f})")
    
    # 시장 상황
    analysis_parts.append(f"시장={market_trend}")
    
    # 기술적 지표
    analysis_parts.append(f"RSI={rsi:.1f}")
    
    # 패턴 분석
    pattern_info = []
    if ma20_up:
        pattern_info.append("MA20상승")
    if accum_vol:
        pattern_info.append("누적거래량")
    if higher_lows:
        pattern_info.append("상승저점")
    if consolidation:
        pattern_info.append("정체구간")
    if yey_pattern:
        pattern_info.append("양음양패턴")
    
    if pattern_info:
        analysis_parts.append(f"패턴:{','.join(pattern_info)}")
    else:
        analysis_parts.append("패턴:없음")
    
    # 신뢰도
    analysis_parts.append(f"신뢰도={confidence:.1f}")
    
    # 뉴스 정보
    analysis_parts.append(f"뉴스={len(news_text)}자")
    
    # PER/PBR 정보
    if per is not None:
        analysis_parts.append(f"PER={per}")
    if pbr is not None:
        analysis_parts.append(f"PBR={pbr}")
    
    reason = f"고도화분석: {', '.join(analysis_parts)}"
    
    return {
        "결정": decision, 
        "분석": reason, 
        "전략_클래스": base_strategy, 
        "매매전술": tactic, 
        "parameters": {"installments": []}
    }

def _compose_reason_suffix(c: Dict, market: Optional[str] = None) -> str:
    mkt = _market_env(market)
    sec = c.get("Sector", "N/A")
    sec_src = c.get("SectorSource", "N/A")
    rsi = c.get("RSI", None)
    psc = c.get("PatternScore", None)
    m20 = "▲" if c.get("MA20Up") else "─"
    lv_src = c.get("levels_source", "N/A")
    sp = c.get("stop_price", None)
    tp = c.get("target_price", None)
    if isinstance(sp, (int, float)) and isinstance(tp, (int, float)):
        level_bit = f" [SL:{fmt_money(float(sp), mkt)}/TP:{fmt_money(float(tp), mkt)}]"
    else:
        level_bit = ""
    parts = [
        f"섹터={sec}({sec_src})",
        f"RSI={rsi:.2f}" if isinstance(rsi, (int, float)) else "RSI=N/A",
        f"PatternScore={psc:.2f}" if isinstance(psc, (int, float)) else "PatternScore=N/A",
        f"MA20={m20}",
        f"레벨={lv_src}{level_bit}",
    ]
    return " | " + " / ".join(parts)

def _get_usable_cash() -> Optional[int]:
    """
    최신 요약/밸런스 캐시에서 가용 현금을 읽어온다.
    """
    try:
        summary_dict, *_ = get_account_snapshot_cached(
            summary_pattern="summary_*.json",
            balance_pattern="balance_*.json",
            ttl_sec=None,
        )
        cash_map = extract_cash_from_summary(
            summary_dict,
            market=os.getenv("MARKET", "SP500"),
        ) if summary_dict else {}
        usable = cash_map.get("available_cash")
        if isinstance(usable, (int, float)):
            return int(usable)
        return None
    except Exception as e:
        logger.debug(f"usable_cash 로드 실패: {e}")
        return None

def analyze_candidates_and_create_plans(
    candidates: List[Dict],
    news_cache: Dict[str, Any],
    market_trend: str,
    available_slots: int = 3,
    budget_ctx: Optional[Dict[str, Any]] = None,
    market: Optional[str] = None,
) -> List[Dict]:
    """
    후보 종목들을 분석하여 매매 계획을 생성합니다.
    성능 최적화를 위해 배치 처리와 조기 필터링을 적용합니다.
    """
    # 점수 기준으로 정렬 (높은 점수부터)
    cand_sorted = sorted(candidates, key=lambda x: float(x.get("Score", 0.0)), reverse=True)
    results: List[Dict] = []
    
    # 조기 필터링: 상위 N개만 처리 (성능 최적화)
    # 설정에서 최대 분석 종목 수 가져오기
    from settings import settings
    gpt_config = settings._config.get("gpt_params", {}).get("analysis_expansion", {})
    max_total_analysis = gpt_config.get("max_total_analysis", 10)
    max_primary_candidates = gpt_config.get("max_primary_candidates", 5)
    
    # 기본 분석 범위 (슬롯의 3배)
    base_max_candidates = min(len(cand_sorted), available_slots * 3)
    
    # 설정에 따른 확장된 분석 범위
    if gpt_config.get("enabled", True):
        max_candidates = min(len(cand_sorted), max_total_analysis)
        logger.info(f"통합 분석 모드: 최대 {max_candidates}개 종목 분석 (설정: max_total_analysis={max_total_analysis})")
    else:
        max_candidates = base_max_candidates
        logger.info(f"기본 분석 모드: {max_candidates}개 종목 분석")
    
    candidates_to_process = cand_sorted[:max_candidates]
    mkt = market or os.getenv("MARKET", "SP500")

    logger.info(f"분석 대상: {len(candidates_to_process)}개 종목 (전체 {len(cand_sorted)}개 중)")

    # 결과 제한 설정
    if gpt_config.get("enabled", True):
        # 통합 분석 모드: 더 많은 결과 허용 (리밸런싱 고려)
        max_results = min(max_primary_candidates, len(candidates_to_process))
    else:
        # 기본 모드: 슬롯 수만큼만
        max_results = max(1, int(available_slots))
    
    for i, c in enumerate(candidates_to_process):
        if len(results) >= max_results:
            break

        name = c.get("Name", "N/A")
        ticker = norm_ticker(c.get("Ticker", "N/A"), mkt)
        score = float(c.get("Score", 0.0))

        # 뉴스 데이터 처리
        raw_news = news_cache.get(ticker, "")
        news_status = None
        news_text = ""
        if isinstance(raw_news, dict):
            news_status = raw_news.get("status")
            news_text = (raw_news.get("text") or "")[:1500]
        else:
            news_text = (raw_news or "")[:1500]

        # 1차 필터링: 기본 조건 체크
        passed = True
        if news_status == "NO_NEWS":
            if score < _IF_MIN_SCORE_NO_NEWS:
                passed = False
            logger.debug(f"[Initial] {name}({ticker}) → NO_NEWS 플래그 감지(Score={score:.3f})")

        # GPT 초기 필터링 (가능한 경우)
        if _OPENAI_AVAILABLE and passed:
            try:
                js = _initial_filter_gpt(name=name, score=score, news_text=news_text, market=mkt)
                if js and isinstance(js, dict) and js.get("decision") and "보류" in js["decision"]:
                    passed = False
                logger.debug(f"[Initial] {name}({ticker}) → {js.get('decision') if js else '실패'} (passed={passed})")
            except Exception as e:
                logger.warning(f"GPT 초기 필터링 실패 {name}({ticker}): {e}")
                # GPT 실패 시 휴리스틱으로 폴백
                if score < _IF_HEURISTIC_MIN_SCORE and len(news_text) < _IF_MIN_NEWS_CHARS:
                    passed = False
        else:
            # 휴리스틱 필터링
            if score < _IF_HEURISTIC_MIN_SCORE and len(news_text) < _IF_MIN_NEWS_CHARS:
                passed = False

        if not passed:
            continue

        # 전술적 계획 생성
        try:
            plan_js = (
                _tactical_plan_gpt(
                    market_trend=market_trend,
                    c=c,
                    news_text=news_text,
                    budget_ctx=budget_ctx if _BUDGET_GUARD_ENABLED else None,
                    market=mkt,
                )
                if _OPENAI_AVAILABLE else
                _heuristic_plan(c, news_text, market_trend, market=mkt)
            )
            
            # 계획 생성 실패 시 휴리스틱으로 폴백
            if not (plan_js and isinstance(plan_js, dict) and plan_js.get("결정")):
                plan_js = _heuristic_plan(c, news_text, market_trend, market=mkt)
                logger.debug(f"휴리스틱 폴백 적용: {name}({ticker})")
        except Exception as e:
            logger.warning(f"계획 생성 실패 {name}({ticker}): {e}")
            plan_js = _heuristic_plan(c, news_text, market_trend, market=mkt)

        # 전략 가중치 적용
        sel = plan_js.get("전략_클래스", "BaseStrategy")
        best = _apply_strategy_weights(selected=sel, c=c, market_trend=market_trend, weights=_strategy_weights)
        if best != sel:
            plan_js["전략_클래스"] = best

        # 분석 텍스트 구성
        stock_info = {k: v for k, v in c.items()}
        src = stock_info.get("source") or stock_info.get("levels_source")
        sector = stock_info.get("Sector")
        rsi = stock_info.get("RSI"); ma50 = stock_info.get("MA50"); ma200 = stock_info.get("MA200")
        news_tag = "(NO_NEWS)" if news_status == "NO_NEWS" else "(NEWS_OK)"
        if is_us_market(mkt):
            ma50_s = fmt_money(float(ma50 or 0), mkt)
            ma200_s = fmt_money(float(ma200 or 0), mkt)
            extra = f" [{news_tag} | levels_source={src} | sector={sector} | RSI={rsi}, MA50={ma50_s}, MA200={ma200_s}]"
        else:
            extra = f" [{news_tag} | levels_source={src} | sector={sector} | RSI={rsi}, MA50={ma50}, MA200={ma200}]"

        if "분석" in plan_js and isinstance(plan_js["분석"], str):
            plan_js["분석"] = (plan_js["분석"] or "") + extra
        else:
            plan_js["분석"] = extra
        plan_js["분석"] += _compose_reason_suffix(c, market=mkt)

        # 최종 결과 구성
        stock_info_min = {
            "Ticker": c.get("Ticker"),
            "Name": c.get("Name"),
            "Price": c.get("Price"),
            "ATR": c.get("ATR"),
            "RSI": c.get("RSI"),
            "MA50": c.get("MA50"),
            "MA200": c.get("MA200"),
            "Score": c.get("Score"),
            "FinScore": c.get("FinScore"),
            "TechScore": c.get("TechScore"),
            "MktScore": c.get("MktScore"),
            "SectorScore": c.get("SectorScore"),
            "PatternScore": c.get("PatternScore"),
            "MA20Up": c.get("MA20Up"),
            "AccumVol": c.get("AccumVol"),
            "HigherLows": c.get("HigherLows"),
            "Consolidation": c.get("Consolidation"),
            "YEY": c.get("YEY"),
            "PER": c.get("PER"),
            "PBR": c.get("PBR"),
            "Sector": c.get("Sector"),
            "SectorSource": c.get("SectorSource"),
            "stop_price": c.get("stop_price"),
            "target_price": c.get("target_price"),
            "levels_source": c.get("levels_source"),
            "daily_chart": c.get("daily_chart"),
            "investor_flow": c.get("investor_flow"),
        }

        merged = {"rank": len(results) + 1, "stock_info": stock_info_min, **plan_js}
        results.append(merged)
        
        logger.info(f"[{i+1}/{len(candidates_to_process)}] {name}({ticker}) → {plan_js.get('결정', 'N/A')}")

    logger.info(f"분석 완료: {len(results)}개 계획 생성")
    
    # 통합 분석 모드에서 상세 로깅
    if gpt_config.get("enabled", True):
        logger.info("=== GPT 분석 결과 요약 ===")
        for i, result in enumerate(results):
            stock_info = result.get("stock_info", {})
            decision = result.get("결정", "N/A")
            name = stock_info.get("Name", "N/A")
            ticker = norm_ticker(stock_info.get("Ticker", ""), mkt)
            score = stock_info.get("Score", 0.0)
            logger.info(f"  [{i+1}] {name}({ticker}): {decision} (점수: {score:.3f})")
        logger.info("=== GPT 분석 결과 요약 완료 ===")
    
    return results

def run_pipeline(
    fixed_date: Optional[str] = None,
    market: str = "SP500",
    available_slots: int = 3
) -> Optional[Path]:
    start_msg = f"▶ GPT 분석 시작 (date={fixed_date or 'auto'}, market={market}, slots={available_slots})"
    logger.info(start_msg)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not fixed_date:
        latest = _detect_latest_screener_file()
        if not latest:
            msg = "스크리너 결과 파일을 찾지 못했습니다. (candidates/rank 패턴 모두 실패)"
            logger.error(msg)
            _notify(f"❌ {msg}", key="gpt_analyzer_missing_screener", cooldown_sec=120)
            return None
        parts = latest.stem.split("_")
        fixed_date, market = (parts[-2], parts[-1]) if len(parts) >= 4 else (None, market)
        if not fixed_date:
            msg = f"파일명에서 날짜/시장 추출 실패: {latest.name}"
            logger.error(msg)
            _notify(f"❌ {msg}", key="gpt_analyzer_parse_fail", cooldown_sec=120)
            return None

    screener_file, news_file = _detect_files(fixed_date, market)
    if not screener_file.exists():
        msg = f"스크리너 결과 없음: {screener_file.name}"
        logger.error(msg)
        _notify(f"❌ {msg}", key="gpt_analyzer_no_screener", cooldown_sec=120)
        return None
    if not news_file.exists():
        msg = f"뉴스 결과 없음: {news_file.name} (먼저 news_collector 실행 필요)"
        logger.error(msg)
        _notify(f"❌ {msg}", key="gpt_analyzer_no_news", cooldown_sec=120)
        return None

    logger.info(f"로드: {screener_file.name}, {news_file.name}")
    candidates_raw: List[Dict] = _read_json(screener_file)
    candidates: List[Dict] = _normalize_candidates(candidates_raw)
    news_cache: Dict[str, Any] = _read_json(news_file)
    market_trend = get_market_trend(fixed_date)
    logger.info(f"시장 추세: {market_trend}")

    # ▼ 예산 가드용 컨텍스트 구성
    usable_cash = _get_usable_cash()
    if _BUDGET_GUARD_ENABLED and usable_cash is not None:
        logger.info(
            f"[BudgetGuard] enabled=True | usable_cash={usable_cash:,}, "
            f"max_entry_price_ratio={_MAX_ENTRY_PRICE_RATIO:.2f}, cash_buffer_ratio={_CASH_BUFFER_RATIO:.2f}"
        )
    budget_ctx = {
        "enabled": _BUDGET_GUARD_ENABLED,
        "usable_cash": usable_cash,
        "max_entry_price_ratio": _MAX_ENTRY_PRICE_RATIO,
        "cash_buffer_ratio": _CASH_BUFFER_RATIO,
    }

    plans = analyze_candidates_and_create_plans(
        candidates,
        news_cache,
        market_trend,
        available_slots,
        budget_ctx=budget_ctx,
        market=market,
    )
    _pretty_print_plans(plans, market=market)

    # 메타데이터 포함 출력
    out_path = OUTPUT_DIR / f"gpt_trades_{fixed_date}_{market}.json"
    from datetime import datetime
    from utils import KST
    wrapped = {
        "schema_version": "1.0",
        "generated_at": datetime.now(KST).isoformat(),
        "market": market,
        "date": fixed_date,
        "plans": plans or []
    }
    # 경량 스키마 검증: 필수 키 확인
    try:
        assert isinstance(wrapped.get("plans"), list)
        for p in wrapped["plans"]:
            assert isinstance(p, dict)
            # 최소 필수 필드 점검(유연하게)
            s = p.get("stock_info", {})
            _ = s.get("Ticker")
    except Exception:
        logger.warning("[Schema] gpt_trades 래핑 구조 확인 경고(필드 누락 가능)")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, ensure_ascii=False, indent=2)
    logger.info(f"저장 완료 → {out_path}")

    # ── Phase 1: 회전(리밸런스) 샌드박스 제안 생성 및 로깅 (집행 없음) ──
    try:
        cfg = load_config() or {}
        ia = (cfg or {}).get("integrated_analysis", {})
        if ia.get("log_gpt_rotation_suggestions", True):
            # 계좌 스냅샷에서 보유/총평가액 추출
            summary_dict, holdings, _sp, _bp = get_account_snapshot_cached()
            if not isinstance(holdings, list):
                holdings = []
            total_value = float((summary_dict or {}).get("tot_evlu_amt", 0) or 0)
            if total_value <= 0:
                logger.info("[RotationSandbox] 총 평가액이 0이어서 제안 생성을 건너뜁니다.")
            else:
                tp = (cfg or {}).get("trading_params", {})
                per_ticker_max = float(tp.get("per_ticker_max_weight", 0.15))
                min_conf = float(ia.get("min_confidence_for_rotation", 0.7))

                suggestions = []
                mkt_rot = os.getenv("MARKET", "SP500")
                for h in holdings:
                    t = norm_ticker(h.get("pdno", ""), mkt_rot)
                    n = h.get("prdt_name", "N/A")
                    qty = int(h.get("hldg_qty", 0) or 0)
                    px = float(h.get("prpr", 0) or 0)
                    if qty <= 0 or px <= 0:
                        continue
                    cur_val = qty * px
                    cur_w = cur_val / total_value if total_value > 0 else 0.0
                    if cur_w > per_ticker_max:
                        target_w = per_ticker_max
                        overflow = min(0.5, max(0.0, cur_w - per_ticker_max))
                        confidence = max(min_conf, min(1.0, 0.6 + overflow * 0.8))
                        suggestions.append({
                            "ticker": t,
                            "name": n,
                            "current_weight": round(cur_w, 6),
                            "target_weight": round(target_w, 6),
                            "decision": "SELL",
                            "confidence": round(confidence, 3),
                            "reasons": [
                                "overweight_breach",
                                f"current_weight={cur_w:.4f}",
                                f"max_weight={per_ticker_max:.4f}"
                            ],
                            "priority": round(cur_w - per_ticker_max, 6)
                        })

                rot_path = OUTPUT_DIR / f"gpt_rotations_{fixed_date}_{market}.json"
                json_payload = {
                    "schema_version": "1.0",
                    "generated_at": datetime.now(KST).isoformat(),
                    "date": fixed_date,
                    "market": market,
                    "decision_source": "gpt_rotation_sandbox",
                    "suggestions": suggestions,
                }
                # 경량 스키마 검증
                try:
                    assert isinstance(json_payload.get("suggestions"), list)
                    for s in json_payload["suggestions"]:
                        assert isinstance(s.get("ticker"), str)
                        assert s.get("decision") in ("SELL", "KEEP")
                except Exception:
                    logger.warning("[Schema] gpt_rotations 구조 확인 경고(필드 누락/타입)")
                with open(rot_path, "w", encoding="utf-8") as rf:
                    json.dump(json_payload, rf, ensure_ascii=False, indent=2)
                logger.info(f"[RotationSandbox] 제안 {len(suggestions)}건 저장 → {rot_path}")
                if suggestions:
                    top = sorted(suggestions, key=lambda s: s.get("priority", 0), reverse=True)[:3]
                    for s in top:
                        logger.info(
                            f"[RotationSandbox] SELL 제안: {s['name']}({s['ticker']}) w={s['current_weight']:.3f}→{s['target_weight']:.3f} conf={s['confidence']:.2f}"
                        )
    except Exception as e:
        logger.warning(f"[RotationSandbox] 제안 생성 중 오류: {e}")

    try:
        if plans:
            top = plans[:min(3, len(plans))]
            fields = []
            for i, p in enumerate(top, 1):
                s = p.get('stock_info', {})
                sym = normalize_ticker_6(s.get('Ticker', 'N/A'), market)
                sl = s.get('stop_price')
                tp = s.get('target_price')
                try:
                    sl_s = fmt_money(float(sl), market) if sl is not None else "N/A"
                    tp_s = fmt_money(float(tp), market) if tp is not None else "N/A"
                except (TypeError, ValueError):
                    sl_s, tp_s = sl, tp
                fields.append({
                    "name": f"[{i}] {s.get('Name','N/A')} ({sym})",
                    "value": f"{p.get('결정')} / {p.get('전략_클래스')} / SL:{sl_s} TP:{tp_s} ({s.get('levels_source')})",
                    "inline": False
                })
            _notify_embed(
                title=f"✅ GPT 분석 완료: {len(plans)}건 생성",
                description=f"date={fixed_date}, market={market}",
                fields=fields
            )
        else:
            _notify(f"ℹ️ GPT 분석 완료: 생성된 계획 없음 (date={fixed_date}, {market})", key="gpt_analyzer_done_empty", cooldown_sec=60)
    except Exception as e:
        logger.warning("최종 요약 알림 실패: %s", e)

    return out_path

# ────────────── CLI ──────────────
# ── GPT 기반 리밸런싱 함수들 ──────────────────────────────────────────
def get_top_screener_candidates(all_stock_data: Dict, holdings: List[Dict], settings: Dict) -> List[Dict]:
    """Screener 데이터에서 상위 후보 선정 (확장된 데이터 구조 사용)"""
    logger.debug(f"[DEBUG] Screener 상위 후보 선정 시작")
    
    # 보유 종목 제외
    held_tickers = {normalize_ticker_6(h.get("pdno", ""), os.getenv("MARKET", "SP500")) for h in holdings}
    
    # 설정에서 최소 점수 임계치 가져오기
    min_score_threshold = settings.get("rebalance_params", {}).get("min_score_threshold", 0.6)
    max_candidates = settings.get("rebalance_params", {}).get("screener_top_n", 20)
    
    candidates = []
    for ticker, data in all_stock_data.items():
        if ticker not in held_tickers:
            score = float(data.get("Score", 0.0))  # 이미 변환된 데이터 사용
            if score >= min_score_threshold:
                # all_stock_data는 이미 변환된 형태이므로 그대로 사용
                candidate = dict(data)  # 복사본 생성
                candidates.append(candidate)
    
    # 점수 기준 정렬 후 상위 N개 반환
    candidates.sort(key=lambda x: float(x.get("Score", 0.0)), reverse=True)
    result = candidates[:min(max_candidates, len(candidates))]
    
    logger.debug(f"[DEBUG] Screener 상위 후보 선정 완료: {len(result)}개 (임계치: {min_score_threshold})")
    return result

def analyze_rebalance_with_gpt(
    current_holdings: List[Dict],
    new_candidates: List[Dict],
    market_trend: str,
    analysis_date: str,
    market: Optional[str] = None,
) -> Optional[Dict]:
    """GPT를 활용한 리밸런싱 분석"""
    logger.debug(f"[DEBUG] GPT 리밸런싱 분석 시작")
    
    try:
        # 현재 보유 종목 정보 구성
        holdings_info = []
        for h in current_holdings:
            holdings_info.append({
                "ticker": normalize_ticker_6(h.get("pdno", ""), os.getenv("MARKET", "SP500")),
                "name": h.get("prdt_name", "N/A"),
                "qty": h.get("hldg_qty", 0),
                "price": h.get("prpr", 0),
                "score": h.get("current_score", 0.0),
                "sector": h.get("screener_sector", ""),
                "rsi": h.get("rsi", 0.0),
                "holding_days": h.get("holding_days", 0)
            })
        
        # 새로운 후보 정보 구성
        candidates_info = []
        for c in new_candidates:
            candidates_info.append({
                "ticker": normalize_ticker_6(c.get("Ticker", ""), os.getenv("MARKET", "SP500")),
                "name": c.get("Name", "N/A"),
                "price": c.get("Price", 0),
                "score": c.get("Score", 0.0),
                "fin_score": c.get("FinScore", 0.0),
                "tech_score": c.get("TechScore", 0.0),
                "mkt_score": c.get("MktScore", 0.0),
                "sector_score": c.get("SectorScore", 0.0),
                "sector": c.get("Sector", ""),
                "rsi": c.get("RSI", 0.0),
                "ma20_up": c.get("MA20Up", False),
                "accum_vol": c.get("AccumVol", False),
                "higher_lows": c.get("HigherLows", False),
                "consolidation": c.get("Consolidation", False),
                "yey_pattern": c.get("YEY", False)
            })
        
        mkt = _market_env(market)
        prompt = build_rebalance_prompt(
            holdings_info, candidates_info, market_trend, analysis_date, market=mkt
        )
        response = _call_openai_json(
            system_prompt=_gpt_system_rebalance(mkt),
            user_prompt=prompt,
        )
        
        logger.debug(f"[DEBUG] GPT 리밸런싱 분석 완료")
        return response
        
    except Exception as e:
        logger.error(f"GPT 리밸런싱 분석 실패: {e}")
        return None

def build_rebalance_prompt(
    holdings_info: List[Dict],
    candidates_info: List[Dict],
    market_trend: str,
    analysis_date: str,
    market: Optional[str] = None,
) -> str:
    """리밸런싱 분석을 위한 GPT 프롬프트 구성 (KR / US 분기)."""
    mkt = _market_env(market)
    holdings_json = json.dumps(
        _format_rebalance_rows(holdings_info, mkt), ensure_ascii=False, indent=2
    )
    candidates_json = json.dumps(
        _format_rebalance_rows(candidates_info, mkt), ensure_ascii=False, indent=2
    )
    tmpl = REBALANCE_PROMPT_US_TMPL if is_us_market(mkt) else REBALANCE_PROMPT_KR_TMPL
    return tmpl.format(
        holdings_json=holdings_json,
        candidates_json=candidates_json,
        market_trend=market_trend,
        analysis_date=analysis_date,
    )

def parse_gpt_rebalance_decisions(
    gpt_analysis: Dict, 
    holdings: List[Dict], 
    candidates: List[Dict]
) -> Tuple[List[Dict], List[Dict]]:
    """GPT 분석 결과를 매도/매수 계획으로 변환"""
    logger.debug(f"[DEBUG] GPT 리밸런싱 결과 파싱 시작")
    
    to_sell_list = []
    to_buy_plans = []
    
    decisions = gpt_analysis.get("rebalance_decisions", [])
    
    for decision in decisions:
        action = decision.get("action")
        ticker = normalize_ticker_6(decision.get("ticker", ""), os.getenv("MARKET", "SP500"))
        name = decision.get("name", "N/A")
        reason = decision.get("reason", "")
        confidence = decision.get("confidence", 0.0)
        priority = decision.get("priority", 5)
        
        if action == "SELL":
            # 보유 종목에서 매도 대상 찾기
            for holding in holdings:
                if normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "SP500")) == ticker:
                    to_sell_list.append({
                        "ticker": ticker,
                        "name": name,
                        "qty": holding.get("hldg_qty", 0),
                        "reason": reason,
                        "confidence": confidence,
                        "priority": priority
                    })
                    logger.debug(f"[DEBUG] 매도 대상 추가: {name}({ticker}) - {reason}")
                    break
        
        elif action == "BUY":
            # 후보에서 매수 대상 찾기
            for candidate in candidates:
                if normalize_ticker_6(candidate.get("Ticker", ""), os.getenv("MARKET", "SP500")) == ticker:
                    to_buy_plans.append({
                        "stock_info": candidate,
                        "reason": reason,
                        "confidence": confidence,
                        "priority": priority,
                        "gpt_analysis": decision
                    })
                    logger.debug(f"[DEBUG] 매수 대상 추가: {name}({ticker}) - {reason}")
                    break
    
    # 우선순위 기준 정렬
    to_sell_list.sort(key=lambda x: x.get("priority", 5), reverse=True)
    to_buy_plans.sort(key=lambda x: x.get("priority", 5), reverse=True)
    
    logger.debug(f"[DEBUG] GPT 리밸런싱 결과 파싱 완료 - 매도: {len(to_sell_list)}건, 매수: {len(to_buy_plans)}건")
    return to_sell_list, to_buy_plans

def get_gpt_enhanced_rebalance_candidates(
    holdings: List[Dict], 
    all_stock_data: Dict, 
    settings: Dict
) -> Tuple[List[Dict], List[Dict]]:
    """GPT 기반 리밸런싱 후보 선정 (Screener 우선)"""
    logger.debug(f"[DEBUG] GPT 기반 리밸런싱 시작 - 보유종목: {len(holdings)}개")
    
    # 1. Screener 데이터에서 상위 후보 선정
    screener_candidates = get_top_screener_candidates(all_stock_data, holdings, settings)
    logger.debug(f"[DEBUG] Screener 상위 후보: {len(screener_candidates)}개")
    
    if not screener_candidates:
        logger.warning("[DEBUG] Screener 후보가 없어 리밸런싱 불가")
        return [], []
    
    # 2. GPT 리밸런싱 분석 실행
    market_trend = get_market_trend("")  # 현재 날짜 사용
    from datetime import datetime
    from utils import KST
    analysis_date = datetime.now(KST).strftime("%Y%m%d")
    
    mkt = os.getenv("MARKET", "SP500")
    gpt_analysis = analyze_rebalance_with_gpt(
        holdings,
        screener_candidates,
        market_trend,
        analysis_date,
        market=mkt,
    )
    
    if not gpt_analysis:
        logger.warning("[DEBUG] GPT 분석 실패 - 기존 로직으로 폴백")
        return fallback_rebalance_logic(holdings, all_stock_data)
    
    # 3. GPT 분석 결과 파싱 및 적용
    to_sell_list, to_buy_plans = parse_gpt_rebalance_decisions(
        gpt_analysis, 
        holdings, 
        screener_candidates
    )
    
    logger.debug(f"[DEBUG] GPT 리밸런싱 완료 - 매도: {len(to_sell_list)}건, 매수: {len(to_buy_plans)}건")
    
    return to_sell_list, to_buy_plans

def fallback_rebalance_logic(holdings: List[Dict], all_stock_data: Dict) -> Tuple[List[Dict], List[Dict]]:
    """GPT 실패 시 기존 로직으로 폴백"""
    logger.info("[FALLBACK] GPT 분석 실패 - 기존 점수 기반 리밸런싱 사용")
    
    # trader.py의 _determine_rebalance_swaps 로직을 직접 구현
    # (순환 import 방지를 위해 여기서 직접 구현)
    try:
        # 간단한 점수 기반 리밸런싱 로직
        # 보유 종목에서 점수가 낮은 종목을 매도 대상으로 선정
        to_sell_list = []
        to_buy_plans = []
        
        # 보유 종목 점수 기준 정렬 (낮은 점수부터)
        holdings_with_scores = []
        for h in holdings:
            score = float(h.get("current_score", 0.0))
            if score > 0:
                holdings_with_scores.append((h, score))
        
        holdings_with_scores.sort(key=lambda x: x[1])
        
        # 상위 후보 선정 (점수 기준)
        candidates = []
        for ticker, data in all_stock_data.items():
            score = float(data.get("Score", 0.0))
            if score > 0.6:  # 최소 점수 임계치
                candidates.append(data)
        
        candidates.sort(key=lambda x: float(x.get("Score", 0.0)), reverse=True)
        
        # 매칭 로직 (간단한 구현)
        max_pairs = min(len(holdings_with_scores), len(candidates), 3)
        for i in range(max_pairs):
            if i < len(holdings_with_scores) and i < len(candidates):
                holding, old_score = holdings_with_scores[i]
                candidate = candidates[i]
                new_score = float(candidate.get("Score", 0.0))
                
                # 점수 차이가 충분한 경우에만 매칭
                if new_score - old_score > 0.1:
                    to_sell_list.append({
                        "ticker": normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "SP500")),
                        "name": holding.get("prdt_name", "N/A"),
                        "qty": holding.get("hldg_qty", 0),
                        "reason": f"점수 하락 ({old_score:.3f} → {new_score:.3f})"
                    })
                    to_buy_plans.append({
                        "stock_info": candidate,
                        "reason": f"점수 상승 ({new_score:.3f})"
                    })
        
        logger.debug(f"[FALLBACK] 폴백 리밸런싱 완료 - 매도: {len(to_sell_list)}건, 매수: {len(to_buy_plans)}건")
        return to_sell_list, to_buy_plans
        
    except Exception as e:
        logger.error(f"폴백 리밸런싱 실패: {e}")
        return [], []

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Screener + News + GPT Analyzer Pipeline")
    parser.add_argument("--date", help="YYYYMMDD (미지정 시 최신 파일 자동 탐색)")
    parser.add_argument(
        "--market",
        default=os.getenv("MARKET", "SP500"),
        choices=["KOSPI", "KONEX", "KOSDAQ", "SP500"],
    )
    parser.add_argument("--slots", type=int, default=3, help="생성할 최대 매수 계획 개수")
    args = parser.parse_args()

    run_pipeline(fixed_date=args.date, market=args.market, available_slots=args.slots)
