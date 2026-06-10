# src/utils.py
import os
import json
import time as pytime   # ← 모듈 time 충돌 방지
import logging
import math
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any, Iterable, Union
from datetime import datetime, time as dt_time, date, timedelta  # ← datetime.time 별칭
from zoneinfo import ZoneInfo
import holidays
import threading
import re
import pandas as pd


# ────────────────────────────────
# 공개 심볼 (다른 모듈에서 무엇을 가져갈지 명시)
# ────────────────────────────────
__all__ = [
    "KST",
    "OUTPUT_DIR",
    "CACHE_DIR",
    "CONFIG_PATH",
    "setup_logging",
    "load_config",
    "get_cfg",
    "compute_52w_position",
    "compute_kki_metrics",
    "count_consecutive_up",
    "is_newly_listed",
    "in_time_windows",
    "is_market_open_day",
    "risk_session_windows",
    "is_regular_session",
    "next_session_open_kst",
    "resolve_pipeline_context",
    "format_pipeline_artifact",
    "pipeline_artifact_path",
    "parse_pipeline_artifact_stem",
    "find_latest_file",
    "cache_save",
    "cache_load",
    "load_account_files_with_retry",
    "extract_cash_from_summary",
    "_to_int_krw",  # ← 공개 심볼 추가
    "convert_screener_data_to_trader_format",  # ← 공통 변환 함수 추가
    # 추가: 계좌 스냅샷 캐시 프로바이더 & 호가 유틸
    "get_account_snapshot_cached",
    "get_tick_size",
    "round_to_tick",
    # 공용 유틸리티
    "tail_file",
    # Phase 1: 공통 유틸리티 함수
    "check_min_holding_period",
    "check_min_holding_hours",
    "validate_config_consistency",
    "extract_broker_order_id",
    # US/KR dual market
    "is_us_market",
    "norm_ticker",
    "normalize_ticker_6",
    "norm_ticker_series",
    "us_excd",
    "us_ovrs_excg_cd",
    "resolve_us_excd",
    "resolve_us_ovrs_excg",
    "resolve_us_sell_order_params",
    "resolve_us_buy_order_params",
    "set_us_ticker_excd_map",
    "set_us_ticker_ovrs_excg_map",
    "load_us_ticker_exchange_maps",
    "ovrs_excg_to_excd",
    "resolve_us_excd_candidates",
    "us_regime_benchmark",
    "get_us_regime_config",
    "min_trading_value_5d_avg",
    "fmt_money",
    "fmt_money_signed",
]

# ────────────────────────────────
# KST 타임존 정의
# ────────────────────────────────
KST = ZoneInfo("Asia/Seoul")
_ET = ZoneInfo("America/New_York")
_XNYS_HOLIDAY_YEARS: Dict[int, set] = {}

# ────────────────────────────────
# 공통 경로
#   - 환경변수로 오버라이드 가능
# ────────────────────────────────
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/app/output")).resolve()
CACHE_DIR = Path(os.getenv("CACHE_DIR", str(OUTPUT_DIR / "cache"))).resolve()
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "/app/config/config.json")).resolve()

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────
# 로깅 설정 (KST)
# ────────────────────────────────
def setup_logging(level: int = logging.INFO) -> logging.Logger:
    root = logging.getLogger()
    root.setLevel(level)

    logging.Formatter.converter = lambda *args: datetime.now(KST).timetuple()
    fmt = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if not root.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        root.addHandler(ch)
    else:
        for h in root.handlers:
            h.setFormatter(fmt)
            h.setLevel(level)

    # noisy 네트워크 로깅 차단
    for noisy in ("httpx", "httpcore", "urllib3"):
        lg = logging.getLogger(noisy)
        lg.setLevel(logging.WARNING)
        lg.propagate = False

    root.debug(
        "logging initialized (KST). OUTPUT_DIR=%s, CACHE_DIR=%s, CONFIG_PATH=%s",
        str(OUTPUT_DIR),
        str(CACHE_DIR),
        str(CONFIG_PATH),
    )
    return root

# ────────────────────────────────
# 설정 파일 및 데이터 분석 유틸리티
# ────────────────────────────────
def strip_jsonc_comments(text: str) -> str:
    """config.json 등 // 라인 주석을 제거 (표준 json.load 호환용)."""
    out: List[str] = []
    for line in text.splitlines():
        in_str = False
        esc = False
        cut = len(line)
        i = 0
        while i < len(line):
            ch = line[i]
            if esc:
                esc = False
            elif ch == "\\" and in_str:
                esc = True
            elif ch == '"':
                in_str = not in_str
            elif not in_str and ch == "/" and i + 1 < len(line) and line[i + 1] == "/":
                cut = i
                break
            i += 1
        out.append(line[:cut].rstrip())
    return "\n".join(out)


def load_json_config(path: Path) -> Optional[Dict[str, Any]]:
    """JSON/JSONC 설정 파일을 dict로 로드."""
    logger = logging.getLogger(__name__)
    try:
        raw = path.read_text(encoding="utf-8")
        cleaned = strip_jsonc_comments(raw)
        cfg = json.loads(cleaned)
        if not isinstance(cfg, dict):
            logger.error("설정 파일의 최상위 구조가 dict가 아닙니다: %s", path)
            return None
        return cfg
    except (json.JSONDecodeError, OSError) as e:
        logger.error("설정 파일 읽기 실패(%s): %s", path, e)
        return None


def load_config(path: Path = CONFIG_PATH) -> Optional[Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    try:
        if not path.exists():
            logger.error("설정 파일을 찾을 수 없습니다: %s", path)
            return None
        cfg = load_json_config(path)
        if cfg is not None:
            logger.info("설정 로드 완료: %s", path)
        return cfg
    except Exception as e:
        logger.error("설정 파일 읽기 실패(%s): %s", path, e)
        return None

def get_cfg(path: Path = CONFIG_PATH) -> dict:
    """설정 로드 + 기본값 보정 + 유효성 검사"""
    logger = logging.getLogger(__name__)
    cfg = load_config(path)
    if cfg is None:
        logger.warning("get_cfg: 설정 로드에 실패하여 기본값으로 진행합니다.")
        cfg = {}  # Fallback to an empty dict
        
    s = cfg.get("screener_params", {})
    s.setdefault("max_market_cap", int(1e13))
    s.setdefault("vol_kki_weight", 0.10)
    s.setdefault("pos_52w_weight", 0.05)
    s.setdefault("exclude_newly_listed_days", 60)
    s.setdefault("exclude_consecutive_up_days", 3)
    cfg.setdefault("trading_guards", {"min_cash_to_trade": 120000, "auto_shrink_slots": True})
    cfg.setdefault("prompting", {"core_questions": True})
    cfg.setdefault("rotation", {"enabled": True, "delta_score_min": 0.10})
    return cfg

def compute_52w_position(series: pd.Series) -> float:
    """52주 범위에서 현재 위치(0~1). 데이터 결측/분모 0은 0 처리"""
    if series is None or series.empty:
        return 0.0
    high_52 = series[-252:].max()
    low_52 = series[-252:].min()
    last = series.iloc[-1]
    rng = max(1e-9, (high_52 - low_52))
    pos = (last - low_52) / rng
    return float(max(0.0, min(1.0, pos)))

def compute_kki_metrics(df: pd.DataFrame) -> float:
    """
    '끼' 점수(0~1): 60D 수익률 표준편차 정규화 + 1Y 상한가 빈도
    df: 반드시 'close','open','high','low' 포함, 일자 오름차순
    """
    if df is None or df.empty:
        return 0.0
    # ← 컬럼 케이스 보정(호출부 무관하게 동작)
    df = df.rename(columns=str.lower)
    if any(c not in df.columns for c in ["close", "open", "high", "low"]):
        return 0.0

    rets = df["close"].pct_change()
    vol = rets.rolling(60).std().iloc[-1]
    # z-score를 간단히 0~3 범위에 맵핑(로버스트 클립)
    if pd.isna(vol):
        vol_norm = 0.0
    else:
        vol_norm = min(3.0, max(0.0, (vol / (rets.std() + 1e-9)))) / 3.0
    # 1Y 상한가 빈도(보수적 근사: 1.29배)
    year = df.tail(252)
    prev_close = year["close"].shift(1)
    limit_hits = ((year["high"] >= prev_close * 1.29).fillna(False)).sum()
    limit_freq = min(1.0, limit_hits / 252.0)
    return float(max(0.0, min(1.0, 0.7 * vol_norm + 0.3 * limit_freq)))

def count_consecutive_up(df: pd.DataFrame, window: int = 3) -> int:
    """연속 양봉 수(마지막 날 기준)"""
    if df is None or df.empty:
        return 0
    up = (df["close"] > df["open"]).astype(int)
    cnt = 0
    for v in reversed(up.tolist()):
        if v == 1:
            cnt += 1
        else:
            break
    return cnt

def is_newly_listed(listing_date: datetime, today: datetime, limit_days: int) -> bool:
    if listing_date is None:
        return False
    return (today.date() - listing_date.date()).days < limit_days

#
# ────────────────────────────────
# 시간창 포함 여부 체크 (형식 검증 + 자정 교차 구간 지원)
# ────────────────────────────────
_WINDOW_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$")

def _parse_hhmm(hh: str, mm: str) -> dt_time:
    """'HH','MM' 숫자 문자열을 datetime.time으로 안전 변환(범위 보정 포함)."""
    h = max(0, min(23, int(hh)))
    m = max(0, min(59, int(mm)))
    return dt_time(h, m)

def in_time_windows(
    now: datetime,
    windows: Optional[List[str]] = None,
    tz: Optional[ZoneInfo] = None,
) -> bool:
    """
    주어진 시각(now)이 'HH:MM-HH:MM' 형태의 구간 리스트 중 하나라도 포함되면 True
    - 잘못된 포맷은 무시하고 경고 로그를 남깁니다
    - 시작 > 종료 인 구간은 '자정 교차(cross-midnight)' 구간으로 해석합니다
      예) 23:50-00:10 → 23:50~24:00 또는 00:00~00:10
    - windows가 비어있거나 None이면 '제한 없음'으로 간주하여 True를 반환합니다
    """
    logger = logging.getLogger(__name__)

    # 타임존 보정: naive → 지정 tz로 로컬라이즈, aware → tz로 변환
    if tz is not None:
        if now.tzinfo is None:
            now = now.replace(tzinfo=tz)
        else:
            now = now.astimezone(tz)

    # 제한 구간이 없으면 통과 (기본 허용)
    if not windows:
        return True

    hm = now.time()
    for raw in windows:
        if not isinstance(raw, str):
            logger.warning("in_time_windows: 잘못된 항목(문자열 아님) 무시: %r", raw)
            continue
        m = _WINDOW_RE.match(raw)
        if not m:
            logger.warning("in_time_windows: 포맷 불일치 'HH:MM-HH:MM' 무시: %s", raw)
            continue
        s = _parse_hhmm(m.group(1), m.group(2))
        e = _parse_hhmm(m.group(3), m.group(4))

        if s <= e:
            # 일반 구간: s <= hm <= e
            if s <= hm <= e:
                return True
        else:
            # 자정 교차 구간: s..24:00 또는 00:00..e
            if hm >= s or hm <= e:
                return True
    return False

# ────────────────────────────────
# 장 개장일 체크 (KR: 주말 제외 / US: NYSE XNYS 휴장 캘린더)
# ────────────────────────────────
def _xnys_holiday_dates(year: int) -> set:
    cached = _XNYS_HOLIDAY_YEARS.get(year)
    if cached is not None:
        return cached
    cal = holidays.financial_holidays("XNYS", years=year)
    cached = set(cal.keys())
    _XNYS_HOLIDAY_YEARS[year] = cached
    return cached


def _is_us_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    return d not in _xnys_holiday_dates(d.year)


def is_market_open_day(check_date: Optional[date] = None, market: Optional[str] = None) -> bool:
    """
    거래일 여부. MARKET 미지정 시 환경변수 MARKET 사용.
    US(SP500 등): America/New_York 기준 달력일 + NYSE(XNYS) 휴장.
    KR: KST 기준 달력일, 주말 제외(공휴일 캘린더는 미연동).
    """
    m = (market or os.getenv("MARKET", "KOSPI")).upper().strip()
    if is_us_market(m):
        if check_date is None:
            check_date = datetime.now(_ET).date()
        return _is_us_trading_day(check_date)
    if check_date is None:
        check_date = datetime.now(KST).date()
    return check_date.weekday() < 5


def previous_trading_day(
    check_date: Optional[date] = None,
    market: Optional[str] = None,
    max_lookback: int = 15,
) -> date:
    """check_date 직전(포함 시 개장일이면 check_date) 거래일."""
    m = (market or os.getenv("MARKET", "KOSPI")).upper().strip()
    if check_date is None:
        check_date = datetime.now(_ET).date() if is_us_market(m) else datetime.now(KST).date()
    if is_market_open_day(check_date, m):
        return check_date
    d = check_date
    for _ in range(max_lookback):
        d -= timedelta(days=1)
        if is_market_open_day(d, m):
            return d
    return check_date

# ────────────────────────────────
# 장중 세션 (리스크 폴링·트리거 가드) — KST 시간창 + 거래일
# ────────────────────────────────
_KR_RISK_SESSION_WINDOWS = ["09:00-15:30"]
_US_RISK_SESSION_DEFAULT = ["23:15-06:00"]


def risk_session_windows(market: Optional[str] = None, config: Optional[dict] = None) -> List[str]:
    """리스크 모니터링 활성 KST 시간창. US: trading_params.sell_time_windows 또는 market_hours."""
    m = (market or os.getenv("MARKET", "KOSPI")).upper().strip()
    cfg = config if config is not None else (load_config() or {})
    if is_us_market(m):
        mh = cfg.get("market_hours") if isinstance(cfg.get("market_hours"), dict) else {}
        entry = (mh or {}).get(m) or (mh or {}).get("SP500") or {}
        if isinstance(entry, dict) and entry.get("risk_poll_windows"):
            return [str(w) for w in entry["risk_poll_windows"]]
        tp = cfg.get("trading_params") or {}
        sw = tp.get("sell_time_windows")
        if sw:
            return [str(w) for w in sw]
        return list(_US_RISK_SESSION_DEFAULT)
    return list(_KR_RISK_SESSION_WINDOWS)


def is_regular_session(
    now: Optional[datetime] = None,
    market: Optional[str] = None,
    session_windows: Optional[List[str]] = None,
    config: Optional[dict] = None,
) -> bool:
    """거래일이며 세션 시간창(KST) 안이면 True."""
    m = (market or os.getenv("MARKET", "KOSPI")).upper().strip()
    if now is None:
        now = datetime.now(KST)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    else:
        now = now.astimezone(KST)

    if is_us_market(m):
        if not is_market_open_day(now.astimezone(_ET).date(), m):
            return False
    elif not is_market_open_day(now.date(), m):
        return False

    wins = session_windows if session_windows is not None else risk_session_windows(m, config)
    return in_time_windows(now, wins, tz=KST)


def next_session_open_kst(
    now: Optional[datetime] = None,
    market: Optional[str] = None,
    session_windows: Optional[List[str]] = None,
    config: Optional[dict] = None,
) -> datetime:
    """다음 세션 시작 시각(KST). 이미 장중이면 now 반환."""
    if now is None:
        now = datetime.now(KST)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    else:
        now = now.astimezone(KST)

    if is_regular_session(now, market, session_windows, config):
        return now

    candidate = now
    for _ in range(7 * 24 * 4):
        candidate += timedelta(minutes=15)
        if is_regular_session(candidate, market, session_windows, config):
            return candidate
    return now + timedelta(hours=1)

# ────────────────────────────────
# 파이프라인 AM/PM 세션 · 거래일 (KST 스케줄 ↔ US ET 세션 정렬)
# ────────────────────────────────
_PIPELINE_SESSION_RE = re.compile(r"_(?P<session>am|pm)_", re.IGNORECASE)


def _resolve_pipeline_session(
    now_kst: datetime,
    market: str,
    config: Optional[dict] = None,
) -> str:
    """KST 시각 기준 파이프라인 슬롯: am(장전·새벽·장후) / pm(저녁 준비)."""
    forced = os.getenv("PIPELINE_SESSION", "").lower().strip()
    if forced in ("am", "pm"):
        return forced

    cfg = config or {}
    slots = cfg.get("pipeline_sessions") if isinstance(cfg.get("pipeline_sessions"), dict) else {}
    pm_from = _parse_schedule_hhmm(slots.get("pm_start", "22:00"))
    am_until = _parse_schedule_hhmm(slots.get("am_end", "06:30"))

    t = now_kst.time()
    if is_us_market(market):
        # 22:00~06:30 KST = 동일 US 야간 사이클(스크리너 22:30 · 파이프라인 23:40·자정 이후 포함)
        if t >= pm_from or t < am_until:
            return "pm"
        return "am"

    if t < dt_time(12, 0):
        return "am"
    return "pm"


def _parse_schedule_hhmm(raw: str) -> dt_time:
    m = re.match(r"^(\d{1,2}):(\d{2})$", str(raw or "").strip())
    if not m:
        return dt_time(0, 0)
    return _parse_hhmm(m.group(1), m.group(2))


def _resolve_pipeline_trade_date(now_kst: datetime, market: str) -> str:
    """산출물·GPT·트레이더가 공유할 거래일(YYYYMMDD). US는 ET 세션 기준."""
    forced = os.getenv("PIPELINE_TRADE_DATE", "").strip()
    if re.fullmatch(r"\d{8}", forced):
        return forced

    if is_us_market(market):
        et_now = now_kst.astimezone(_ET)
        et_d = et_now.date()
        t_kst = now_kst.time()
        if t_kst >= dt_time(22, 0) or t_kst < dt_time(6, 30):
            if is_market_open_day(et_d, market):
                return et_d.strftime("%Y%m%d")
            return previous_trading_day(et_d, market).strftime("%Y%m%d")
        return previous_trading_day(et_d, market).strftime("%Y%m%d")

    d = now_kst.date()
    if is_market_open_day(d, market):
        return d.strftime("%Y%m%d")
    return previous_trading_day(d, market).strftime("%Y%m%d")


def resolve_pipeline_context(
    now: Optional[datetime] = None,
    market: Optional[str] = None,
    config: Optional[dict] = None,
) -> Dict[str, str]:
    """
    파이프라인/스크리너 실행 컨텍스트.
    - session: am | pm
    - trade_date: 산출물 접미사용 거래일
    - kst_date: KST 달력일
    """
    m = (market or os.getenv("MARKET", "KOSPI")).upper().strip()
    if now is None:
        now = datetime.now(KST)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    else:
        now = now.astimezone(KST)

    session = _resolve_pipeline_session(now, m, config)
    trade_date = _resolve_pipeline_trade_date(now, m)
    return {
        "session": session,
        "trade_date": trade_date,
        "kst_date": now.strftime("%Y%m%d"),
    }


def format_pipeline_artifact(
    prefix: str,
    trade_date: str,
    market: str,
    session: Optional[str] = None,
    suffix: str = ".json",
) -> str:
    """예: screener_candidates_20260602_pm_SP500.json"""
    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()
    td = str(trade_date).strip()
    sess = (session or os.getenv("PIPELINE_SESSION") or "").lower().strip()
    if sess in ("am", "pm"):
        return f"{prefix}_{td}_{sess}_{mkt}{suffix}"
    return f"{prefix}_{td}_{mkt}{suffix}"


def pipeline_artifact_path(
    prefix: str,
    trade_date: str,
    market: str,
    session: Optional[str] = None,
) -> Path:
    return OUTPUT_DIR / format_pipeline_artifact(prefix, trade_date, market, session)


def parse_pipeline_artifact_stem(stem: str) -> Dict[str, Optional[str]]:
    """스크리너/뉴스/GPT 산출물 stem에서 date·session·market 추출."""
    meta: Dict[str, Optional[str]] = {"date": None, "session": None, "market": None}
    mm = _market_pattern.search(stem)
    if mm:
        meta["market"] = mm.group(0).upper()
    dm = re.search(r"(\d{8})", stem)
    if dm:
        meta["date"] = dm.group(1)
    sm = _PIPELINE_SESSION_RE.search(stem)
    if sm:
        meta["session"] = sm.group("session").lower()
    return meta

# ────────────────────────────────
# 내부: 파일명에서 날짜/시장/런ID 추출
# ────────────────────────────────
_date_patterns = [
    re.compile(r"(?P<date>\d{8})[._-]?(?P<hms>\d{6})?"),  # 20250904 or 20250904-134000
    re.compile(r"(?P<date>\d{8})"),                       # 20250904
]
_market_pattern = re.compile(
    r"(KOSPI|KOSDAQ|KONEX|SP500|SPX500|NASDAQ100|NYSE|NASDAQ|AMEX|SPX|NIKKEI|HKEX)",
    re.IGNORECASE,
)

_US_MARKETS = frozenset({
    "SP500", "SPX500", "SPX",
    "NASDAQ100", "NDX100",
    "NASDAQ", "NASD", "NYSE", "AMEX", "US",
})
_US_EXCD_MAP = {
    "SP500": "NAS",
    "SPX500": "NAS",
    "SPX": "NAS",
    "NASDAQ100": "NAS",
    "NDX100": "NAS",
    "NASDAQ": "NAS",
    "NASD": "NAS",
    "NYSE": "NYS",
    "AMEX": "AMS",
    "US": "NAS",
}
_US_OVRS_EXCG_MAP = {
    "SP500": "NASD",
    "SPX500": "NASD",
    "SPX": "NASD",
    "NASDAQ100": "NASD",
    "NDX100": "NASD",
    "NASDAQ": "NASD",
    "NASD": "NASD",
    "NYSE": "NYSE",
    "AMEX": "AMEX",
    "US": "NASD",
}
_EXCD_TO_OVRS = {"NAS": "NASD", "NYS": "NYSE", "AMS": "AMEX"}
_OVRS_TO_EXCD = {"NASD": "NAS", "NYSE": "NYS", "AMEX": "AMS", "NAS": "NAS", "NYS": "NYS", "AMS": "AMS"}
_US_EXCD_TRY_ORDER = ("NYS", "NAS", "AMS")
_US_TICKER_EXCD: Dict[str, str] = {}
_US_TICKER_OVRS: Dict[str, str] = {}


def set_us_ticker_excd_map(mapping: Dict[str, str]) -> None:
    global _US_TICKER_EXCD
    _US_TICKER_EXCD = {
        str(k).strip().upper(): str(v).strip().upper()
        for k, v in (mapping or {}).items()
        if k and v
    }


def set_us_ticker_ovrs_excg_map(mapping: Dict[str, str]) -> None:
    global _US_TICKER_OVRS
    _US_TICKER_OVRS = {
        str(k).strip().upper(): str(v).strip().upper()
        for k, v in (mapping or {}).items()
        if k and v
    }


def load_us_ticker_exchange_maps(market: Optional[str] = None) -> int:
    """SP500 마스터 → 티커별 EXCD/OvrsExcg 맵 로드. 반환: 티커 수."""
    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()
    if not is_us_market(mkt):
        return 0
    try:
        from kis_master import load_kis_master

        mst = load_kis_master(mkt, cache_key=datetime.now(KST).strftime("%Y%m%d"))
        if mst is None or mst.empty:
            return 0
        if "EXCD" in mst.columns:
            set_us_ticker_excd_map(dict(zip(mst.index.astype(str), mst["EXCD"].astype(str))))
        if "OvrsExcg" in mst.columns:
            set_us_ticker_ovrs_excg_map(dict(zip(mst.index.astype(str), mst["OvrsExcg"].astype(str))))
        return len(mst)
    except Exception as e:
        logging.getLogger(__name__).warning("US 거래소 맵 로드 실패: %s", e)
        return 0


def ovrs_excg_to_excd(ovrs_excg_cd: Optional[str]) -> Optional[str]:
    """TTTS3012R ovrs_excg_cd(NASD/NYSE/AMEX) → HHDFS EXCD(NAS/NYS/AMS)."""
    s = str(ovrs_excg_cd or "").strip().upper()
    if not s:
        return None
    if s in _OVRS_TO_EXCD:
        mapped = _OVRS_TO_EXCD[s]
        return mapped if mapped in ("NAS", "NYS", "AMS") else None
    return None


_DEFAULT_US_MARKET_REGIME: Dict[str, Any] = {
    "source": "index",
    "index_market_code": "N",
    "index_symbol": "SPX",
    "lookback_calendar_days": 500,
    "min_bars": 200,
    "etf_fallback": {
        "enabled": True,
        "symbol": "SPY",
        "excd": "AMS",
    },
}


def get_us_regime_config(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """config.json us_market_regime 병합 (기본: SPX 지수 + SPY@AMS 폴백)."""
    merged = dict(_DEFAULT_US_MARKET_REGIME)
    raw = (cfg if cfg is not None else get_cfg()).get("us_market_regime")
    if isinstance(raw, dict):
        merged.update({k: v for k, v in raw.items() if k != "etf_fallback"})
        fb = dict(merged.get("etf_fallback") or {})
        if isinstance(raw.get("etf_fallback"), dict):
            fb.update(raw["etf_fallback"])
        merged["etf_fallback"] = fb
    return merged


def us_regime_benchmark(market: Optional[str] = None) -> str:
    """US 시장 레짐·추세 벤치마크 심볼 (config: SPX 지수 기본)."""
    if not is_us_market(market):
        return ""
    rc = get_us_regime_config()
    if str(rc.get("source") or "index").strip().lower() == "index":
        return str(rc.get("index_symbol") or "SPX").strip().upper()
    fb = rc.get("etf_fallback") if isinstance(rc.get("etf_fallback"), dict) else {}
    return str(fb.get("symbol") or "SPY").strip().upper()


def resolve_us_excd_candidates(
    ticker: str,
    market: Optional[str] = None,
    ovrs_excg_hint: Optional[str] = None,
) -> List[str]:
    """HHDFS 시세 TR용 EXCD 후보 (우선순위 순, 중복 제거)."""
    t = norm_ticker(ticker, market)
    out: List[str] = []

    def _add(excd: Optional[str]) -> None:
        x = str(excd or "").strip().upper()
        if x in ("NAS", "NYS", "AMS") and x not in out:
            out.append(x)

    _add(ovrs_excg_to_excd(ovrs_excg_hint))
    if t and t in _US_TICKER_EXCD:
        _add(_US_TICKER_EXCD[t])
    m = (market or os.getenv("MARKET", "SP500")).upper().strip()
    _add(_US_EXCD_MAP.get(m, "NAS"))
    for fb in _US_EXCD_TRY_ORDER:
        _add(fb)
    return out or ["NAS"]


def resolve_us_excd(
    ticker: str,
    market: Optional[str] = None,
    ovrs_excg_hint: Optional[str] = None,
) -> str:
    """티커별 KIS EXCD (NAS/NYS/AMS). 잔고 hint → 마스터 → MARKET 폴백."""
    return resolve_us_excd_candidates(ticker, market, ovrs_excg_hint)[0]


def resolve_us_ovrs_excg(
    ticker: str,
    market: Optional[str] = None,
    ovrs_excg_hint: Optional[str] = None,
) -> str:
    """TTTT1006U 등 주문 TR용 OVRS_EXCG_CD (NASD/NYSE/AMEX)."""
    if ovrs_excg_hint:
        s = str(ovrs_excg_hint).strip().upper()
        if s in ("NASD", "NYSE", "AMEX"):
            return s
        excd = ovrs_excg_to_excd(s)
        if excd:
            return _EXCD_TO_OVRS.get(excd, "NASD")
    t = norm_ticker(ticker, market)
    if t and t in _US_TICKER_OVRS:
        return _US_TICKER_OVRS[t]
    excd = resolve_us_excd(t, market)
    return _EXCD_TO_OVRS.get(excd, _US_OVRS_EXCG_MAP.get(
        (market or os.getenv("MARKET", "SP500")).upper().strip(), "NASD"
    ))


def is_us_market(market: Optional[str] = None) -> bool:
    m = (market or os.getenv("MARKET", "")).upper().strip()
    return m in _US_MARKETS


def norm_ticker(ticker: str, market: Optional[str] = None) -> str:
    """KR: 6자리 숫자 코드 / US: 심볼 보존 (AAPL)."""
    s = str(ticker or "").strip().upper()
    if not s:
        return s
    if is_us_market(market):
        return re.sub(r"[^A-Z0-9.^-]", "", s)
    try:
        return format(int(re.sub(r"\D", "", s) or "0"), "06d")
    except (ValueError, TypeError):
        return s.zfill(6) if len(s) <= 6 else s


def norm_ticker_series(series: pd.Series, market: Optional[str] = None) -> pd.Index:
    return pd.Index([norm_ticker(x, market) for x in series.astype(str)], name="Code")


def us_excd(market: Optional[str] = None) -> str:
    m = (market or os.getenv("MARKET", "SP500")).upper().strip()
    return _US_EXCD_MAP.get(m, "NAS")


def us_ovrs_excg_cd(market: Optional[str] = None) -> str:
    m = (market or os.getenv("MARKET", "SP500")).upper().strip()
    return _US_OVRS_EXCG_MAP.get(m, "NASD")


def min_trading_value_5d_avg(cfg: Dict[str, Any], market: Optional[str] = None) -> float:
    sp = cfg or {}
    if is_us_market(market):
        return float(sp.get("min_trading_value_5d_avg_us", 30_000_000))
    return float(sp.get("min_trading_value_5d_avg", 0))


def fmt_money(amount: float, market: Optional[str] = None) -> str:
    try:
        val = float(amount)
    except (TypeError, ValueError):
        val = 0.0
    if is_us_market(market):
        return f"${val:,.2f} USD"
    return f"{val:,.0f}원"


def fmt_money_signed(amount: float, market: Optional[str] = None) -> str:
    """Signed PnL / balance change (e.g. +$100.00 USD, -1,000원)."""
    try:
        val = float(amount)
    except (TypeError, ValueError):
        val = 0.0
    if is_us_market(market):
        if val < 0:
            return f"-${abs(val):,.2f} USD"
        if val > 0:
            return f"+${val:,.2f} USD"
        return "$0.00 USD"
    return f"{val:+,.0f}원"


def _extract_meta_from_name(name: str) -> Dict[str, Any]:
    meta: Dict[str, Any] = {"date": None, "hms": None, "market": None, "session": None}
    for pat in _date_patterns:
        m = pat.search(name)
        if m:
            meta["date"] = m.group("date")
            if "hms" in m.groupdict():
                meta["hms"] = m.group("hms")
            break
    mm = _market_pattern.search(name)
    if mm:
        meta["market"] = mm.group(0).upper()
    sm = _PIPELINE_SESSION_RE.search(name)
    if sm:
        meta["session"] = sm.group("session").lower()
    return meta


def _session_rank(meta: Dict[str, Any], preferred_session: Optional[str]) -> int:
    """정렬 우선순위: 세션 일치(2) > 레거시(1) > 불일치(0)."""
    if not preferred_session:
        return 1
    fs = meta.get("session")
    if fs == preferred_session:
        return 2
    if not fs:
        return 1
    return 0


def _score_file(
    p: Path,
    prefer_date: bool = True,
    preferred_session: Optional[str] = None,
) -> Tuple[int, int, int, float]:
    """
    스코어: (date_int, hms_int, mtime)
    - 날짜가 없으면 0 취급
    - prefer_date=True면 날짜/시간 우선, 동률이면 mtime
    """
    name = p.name
    meta = _extract_meta_from_name(name)
    try:
        date_int = int(meta["date"]) if meta["date"] else 0
    except Exception:
        date_int = 0
    try:
        hms_int = int(meta["hms"]) if meta["hms"] else 0
    except Exception:
        hms_int = 0
    try:
        mtime = p.stat().st_mtime
    except Exception:
        mtime = 0.0

    sess_rank = _session_rank(meta, preferred_session)
    return (
        date_int if prefer_date else 0,
        sess_rank,
        hms_int if prefer_date else 0,
        mtime,
    )

def _iter_globs(patterns: Union[str, Iterable[str]]) -> List[Path]:
    pats: List[str] = []
    if isinstance(patterns, str):
        pats = [patterns]
    else:
        pats = [str(x) for x in patterns]
    seen: Dict[str, Path] = {}
    for pat in pats:
        for p in OUTPUT_DIR.glob(pat):
            seen[str(p)] = p
    return list(seen.values())

# ────────────────────────────────
# 최신 파일 찾기 (다중 패턴/마켓 필터/날짜 우선)
# ────────────────────────────────
def find_latest_file(
    patterns: Union[str, Iterable[str]],
    *,
    market: Optional[str] = None,
    prefer_date_over_mtime: bool = True,
    trade_date: Optional[str] = None,
    session: Optional[str] = None,
) -> Optional[Path]:
    """
    OUTPUT_DIR에서 patterns에 매칭되는 파일 중 '최신' 하나를 반환합니다.
    - patterns: 문자열 패턴 또는 패턴 리스트/튜플
    - market: "KOSPI" 등 필터(대소문자 무시). 파일명에 시장명이 포함된 경우에만 필터 적용.
    - prefer_date_over_mtime: 파일명 날짜 우선, 동일/부재시 mtime 기준.
    - trade_date / session: PIPELINE_TRADE_DATE·PIPELINE_SESSION 환경변수와 동일 규칙.
    """
    logger = logging.getLogger(__name__)
    candidates = _iter_globs(patterns)
    if not candidates:
        return None

    mkt = (market or os.getenv("MARKET", "")).upper().strip()
    td = (trade_date or os.getenv("PIPELINE_TRADE_DATE", "")).strip()
    sess = (session or os.getenv("PIPELINE_SESSION", "")).lower().strip()
    if sess not in ("am", "pm"):
        sess = None

    def _collect(*, require_trade_date: bool) -> List[Tuple[Tuple[int, int, int, float], Path]]:
        out: List[Tuple[Tuple[int, int, int, float], Path]] = []
        for p in candidates:
            meta = _extract_meta_from_name(p.name)
            if mkt and meta.get("market") and meta["market"] != mkt:
                continue
            if require_trade_date and td and meta.get("date") and meta["date"] != td:
                continue
            score = _score_file(
                p,
                prefer_date=prefer_date_over_mtime,
                preferred_session=sess,
            )
            out.append((score, p))
        return out

    filtered = _collect(require_trade_date=bool(td))
    if not filtered and td:
        logger.debug(
            "find_latest_file: trade_date=%s 매칭 없음 → 날짜 필터 완화",
            td,
        )
        filtered = _collect(require_trade_date=False)

    if not filtered:
        logger.debug(
            "find_latest_file: 후보 없음 → fallback(mtime). market_filter=%s, patterns=%s",
            mkt or "NONE", patterns,
        )
        try:
            return max(candidates, key=lambda x: x.stat().st_mtime)
        except Exception:
            return None

    latest = max(filtered, key=lambda t: t[0])[1]
    return latest

# ────────────────────────────────
# 캐시 유틸리티 (pickle)
# ────────────────────────────────
def cache_save(prefix: str, key: str, data: Any) -> None:
    import pickle
    p = CACHE_DIR / f"{prefix}_{key}.pkl"
    try:
        with open(p, "wb") as f:
            pickle.dump(data, f)
    except Exception as e:
        logging.getLogger(__name__).warning("캐시 저장 실패(%s): %s", p, e)

def cache_load(prefix: str, key: str) -> Any:
    import pickle
    p = CACHE_DIR / f"{prefix}_{key}.pkl"
    if not p.exists():
        return None
    try:
        with open(p, "rb") as f:
            return pickle.load(f)
    except Exception as e:
        logging.getLogger(__name__).warning("캐시 로드 실패(%s): %s", p, e)
        return None

# ────────────────────────────────
# JSON 파일 로드 & 계좌/잔고 파싱
# ────────────────────────────────
def _read_json(p: Path) -> Optional[dict]:
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.getLogger(__name__).error("JSON 읽기 실패: %s (%s)", p.name, e)
        return None

def _parse_summary_payload(obj: dict) -> Dict[str, str]:
    """
    account.py 저장 포맷 예시:
    {"comments": {...}, "data": [ { "0": { ... } } ]}  또는  {"data": [ { ... } ]}
    """
    if not obj:
        return {}
    data = obj.get("data", [])
    if not data or not isinstance(data, list):
        return {}
    first = data[0]
    if isinstance(first, dict) and "0" in first and isinstance(first["0"], dict):
        return dict(first["0"])
    if isinstance(first, dict):
        return dict(first)
    return {}

def _parse_balance_payload(obj: dict) -> List[Dict]:
    if not obj:
        return []
    data = obj.get("data", [])
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    return []

def _to_int(v, default: int = 0) -> int:
    """문자열/숫자를 안전하게 int 변환 (쉼표 제거 포함). 실패 시 default."""
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str):
            s = v.replace(",", "").strip()
            if s.startswith("-") and s[1:].isdigit():
                return int(s)
            return int(s) if s.isdigit() else default
        return default
    except Exception:
        return default

def _to_float(v, default: float = 0.0) -> float:
    """문자열/숫자를 안전하게 float 변환 (쉼표 제거 포함). 실패 시 default."""
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.replace(",", "").strip()
            return float(s) if s else default
        return default
    except Exception:
        return default

def _to_int_krw(v) -> int:
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        s = v.replace(",", "").strip()
        if s.startswith("-") and s[1:].isdigit():
            return int(s)
        return int(s) if s.isdigit() else 0
    return 0

def load_account_files_with_retry(
    summary_pattern: str = "summary_*.json",
    balance_pattern: str = "balance_*.json",
    max_wait_sec: int = 5,
) -> Tuple[Dict[str, str], List[Dict], Optional[Path], Optional[Path]]:
    """
    최신 summary/balance 파일을 읽고 (summary_dict, balance_list, summary_path, balance_path) 반환.
    파일 생성 직후일 수 있어 최대 max_wait_sec 동안 재시도.
    """
    logger = logging.getLogger(__name__)
    deadline = pytime.time() + max_wait_sec
    summary_path: Optional[Path] = None
    balance_path: Optional[Path] = None
    parsed_summary: Dict[str, str] = {}
    parsed_balance: List[Dict] = []

    while pytime.time() < deadline:
        if summary_path is None:
            summary_path = find_latest_file(summary_pattern)
        if balance_path is None:
            balance_path = find_latest_file(balance_pattern)

        ok = True
        if summary_path and summary_path.exists():
            js = _read_json(summary_path)
            parsed_summary = _parse_summary_payload(js)
        else:
            ok = False

        if balance_path and balance_path.exists():
            jb = _read_json(balance_path)
            parsed_balance = _parse_balance_payload(jb)

        if ok:
            return parsed_summary, parsed_balance, summary_path, balance_path
        pytime.sleep(0.5)

    # 마지막 시도
    if summary_path and summary_path.exists() and not parsed_summary:
        js = _read_json(summary_path)
        parsed_summary = _parse_summary_payload(js)
    if balance_path and balance_path.exists() and not parsed_balance:
        jb = _read_json(balance_path)
        parsed_balance = _parse_balance_payload(jb)

    if not parsed_summary:
        logger.warning("요약 파일을 찾지 못했거나 파싱 실패 (pattern=%s)", summary_pattern)
    return parsed_summary, parsed_balance, summary_path, balance_path

def extract_cash_from_summary(
    summary_dict: Dict[str, str],
    market: Optional[str] = None,
) -> Dict[str, int]:
    """
    summary.json에서 현금 관련 키를 추출하고 주문 가능 금액을 계산합니다.
    KR: prvs_rcdl_excc_amt(D+2) > nxdy_excc_amt > dnca_tot_amt
    US: ord_psbl_frcr_amt / available_cash (USD)
    """
    if not summary_dict:
        return {"available_cash": 0}

    mkt = market or os.getenv("MARKET", "SP500")
    us_mode = is_us_market(mkt) or str(summary_dict.get("currency", "")).upper() == "USD"

    def _amt_keys(k: str) -> bool:
        if not isinstance(k, str):
            return False
        return ("amt" in k) or k.endswith("_cash") or "ord_psbl" in k

    cash_map = {k: _to_int_krw(v) for k, v in summary_dict.items() if _amt_keys(k)}

    if us_mode:
        for key in (
            "available_cash",
            "ord_psbl_frcr_amt",
            "prvs_rcdl_excc_amt",
            "frcr_buy_amt",
            "dnca_tot_amt",
        ):
            val = cash_map.get(key) or _to_int_krw(summary_dict.get(key, 0))
            if val > 0:
                cash_map["available_cash"] = val
                return cash_map
        cash_map["available_cash"] = 0
        return cash_map

    if cash_map.get("prvs_rcdl_excc_amt", 0) > 0:
        available = cash_map["prvs_rcdl_excc_amt"]
    elif cash_map.get("nxdy_excc_amt", 0) > 0:
        available = cash_map["nxdy_excc_amt"]
    else:
        available = cash_map.get("dnca_tot_amt", 0)

    cash_map["available_cash"] = available
    return cash_map

# ────────────────────────────────
# Account Snapshot Provider (파일 mtime/락 기반 캐시)
# ────────────────────────────────
_SNAPSHOT_CACHE_LOCK = threading.Lock()
_SNAPSHOT_CACHE: Dict[str, Any] = {
    "ts": 0.0,                  # 캐시 생성 시각 (epoch)
    "summary_path": None,       # 마지막 사용 summary 파일 경로
    "balance_path": None,       # 마지막 사용 balance 파일 경로
    "summary_mtime": 0.0,       # summary 파일 mtime
    "balance_mtime": 0.0,       # balance 파일 mtime
    "summary": {},              # 파싱된 summary
    "balance": [],              # 파싱된 balance
}
_SNAPSHOT_LOCKFILE = Path(os.getenv("ACCOUNT_SNAPSHOT_LOCK", "/tmp/account_snapshot.lock"))
_SNAPSHOT_TTL_SEC = int(os.getenv("ACCOUNT_SNAPSHOT_TTL_SEC", "90"))  # 기본 90초
_SNAPSHOT_WAIT_ON_LOCK_SEC = int(os.getenv("ACCOUNT_SNAPSHOT_WAIT_SEC", "5"))  # 락이 있으면 최대 대기

def _files_unchanged(summary_path: Optional[Path], balance_path: Optional[Path],
                     cached_summary_mtime: float, cached_balance_mtime: float) -> bool:
    try:
        sm_ok = (summary_path is None) or (summary_path.exists() and abs(summary_path.stat().st_mtime - cached_summary_mtime) < 1e-6)
        bl_ok = (balance_path is None) or (balance_path.exists() and abs(balance_path.stat().st_mtime - cached_balance_mtime) < 1e-6)
        return sm_ok and bl_ok
    except Exception:
        return False

def _touch_lockfile() -> None:
    try:
        _SNAPSHOT_LOCKFILE.write_text(str(pytime.time()))
    except Exception:
        pass

def _lock_is_recent(max_age_sec: int = 10) -> bool:
    try:
        if not _SNAPSHOT_LOCKFILE.exists():
            return False
        age = pytime.time() - _SNAPSHOT_LOCKFILE.stat().st_mtime
        return age <= max_age_sec
    except Exception:
        return False

def get_account_snapshot_cached(
    summary_pattern: str = "summary_*.json",
    balance_pattern: str = "balance_*.json",
    ttl_sec: Optional[int] = None,
) -> Tuple[Dict[str, int], List[Dict], Optional[Path], Optional[Path]]:
    """
    요약/잔고 파일을 읽어 캐시로 제공.
    - 캐시 TTL(기본 90초) 내에서는 메모리 캐시 반환
    - 캐시가 있어도 파일 mtime 변경 시 즉시 재로딩
    - 다른 프로세스가 동시에 갱신 중이면 lockfile 존재 시 잠깐 대기 후 캐시 재확인
    반환: (summary_dict, balance_list, summary_path, balance_path)
    """
    logger = logging.getLogger(__name__)
    ttl = int(ttl_sec if ttl_sec is not None else _SNAPSHOT_TTL_SEC)

    # 1) 락 파일이 최신이라면 잠깐 대기(중복 IO 억제)
    wait_deadline = pytime.time() + _SNAPSHOT_WAIT_ON_LOCK_SEC
    while _lock_is_recent() and pytime.time() < wait_deadline:
        pytime.sleep(0.2)

    with _SNAPSHOT_CACHE_LOCK:
        now = pytime.time()
        # 캐시가 유효하면 그대로 반환
        if (now - _SNAPSHOT_CACHE["ts"]) <= ttl:
            # 파일 변경 없는지 확인
            sp = _SNAPSHOT_CACHE["summary_path"]
            bp = _SNAPSHOT_CACHE["balance_path"]
            if _files_unchanged(sp, bp, _SNAPSHOT_CACHE["summary_mtime"], _SNAPSHOT_CACHE["balance_mtime"]):
                return (
                    dict(_SNAPSHOT_CACHE["summary"]),
                    list(_SNAPSHOT_CACHE["balance"]),
                    sp,
                    bp,
                )

        # 2) 재로딩 (락 생성 후 로드)
        _touch_lockfile()
        summary_dict, balance_list, summary_path, balance_path = load_account_files_with_retry(
            summary_pattern=summary_pattern,
            balance_pattern=balance_pattern,
            max_wait_sec=5,
        )

        # 3) 캐시에 저장
        try:
            sm_mtime = summary_path.stat().st_mtime if summary_path and summary_path.exists() else 0.0
            bl_mtime = balance_path.stat().st_mtime if balance_path and balance_path.exists() else 0.0
        except Exception:
            sm_mtime = bl_mtime = 0.0

        _SNAPSHOT_CACHE.update({
            "ts": now,
            "summary_path": summary_path,
            "balance_path": balance_path,
            "summary_mtime": sm_mtime,
            "balance_mtime": bl_mtime,
            "summary": summary_dict,
            "balance": balance_list,
        })

        # 4) 반환
        return summary_dict, balance_list, summary_path, balance_path

# ────────────────────────────────
# 호가 단위 유틸 (표준화)
# ────────────────────────────────
def get_tick_size(price: float) -> int:
    """
    KRX 일반 호가단위 (원 기준, 단순화 버전)
    """
    try:
        p = float(price)
    except Exception:
        p = 0.0
    if p < 2000: return 1
    elif p < 5000: return 5
    elif p < 20000: return 10
    elif p < 50000: return 50
    elif p < 200000: return 100
    elif p < 500000: return 500
    else: return 1000

def round_to_tick(price: float, mode: str = "nearest") -> int:
    """
    호가단위에 맞춰 반올림.
      - mode='nearest' (기본): 가장 가까운 호가
      - mode='down'         : 아래 호가
      - mode='up'           : 위 호가
    """
    try:
        p = float(price)
    except Exception:
        return 0
    tick = get_tick_size(p)
    if tick <= 0:
        return int(round(p))
    if mode == "down":
        return int((p // tick) * tick)
    if mode == "up":
        return int(((p + tick - 1) // tick) * tick)
    # nearest
    return int(round(p / tick) * tick)


def resolve_us_sell_order_params(
    current_price: int,
    *,
    urgency: str = "normal",
    slippage_bps: Optional[int] = None,
) -> Tuple[str, int]:
    """
    KIS TTTT1006U(미국 매도) 유효 조합: ORD_DVSN=00(지정가) + OVRS_ORD_UNPR>0.
    urgent(EmergencyDrop/StopLoss): 슬리피지 100bps, normal: 30bps.
    """
    px = int(current_price or 0)
    if px <= 0:
        raise ValueError(f"US sell requires current_price > 0, got {current_price!r}")
    if slippage_bps is None:
        slippage_bps = 100 if str(urgency).lower() == "urgent" else 30
    slippage_factor = 1.0 - (float(slippage_bps) / 10000.0)
    target_price = px * slippage_factor
    sell_price = round_to_tick(target_price, mode="down")
    if sell_price <= 0:
        sell_price = max(1, px)
    return "00", sell_price


def resolve_us_buy_order_params(
    current_price: int,
    *,
    urgency: str = "normal",
    slippage_bps: Optional[int] = None,
) -> Tuple[str, int]:
    """
    KIS TTTT1002U(미국 매수): ORD_DVSN=00(지정가) + OVRS_ORD_UNPR>0.
    US 매수 시장가(01+unpr=0)는 output 없이 거절되는 경우가 많아 지정가+슬리피지 사용.
    urgent: 100bps, normal: 30bps (매수는 현재가 대비 상향).
    """
    px = int(current_price or 0)
    if px <= 0:
        raise ValueError(f"US buy requires current_price > 0, got {current_price!r}")
    if slippage_bps is None:
        slippage_bps = 100 if str(urgency).lower() == "urgent" else 30
    slippage_factor = 1.0 + (float(slippage_bps) / 10000.0)
    target_price = px * slippage_factor
    buy_price = round_to_tick(target_price, mode="up")
    if buy_price <= 0:
        buy_price = max(1, px)
    return "00", buy_price


# ────────────────────────────────
def convert_screener_data_to_trader_format(screener_data: Dict) -> Dict:
    """
    Screener 데이터를 trader.py 형식으로 변환하는 공통 함수
    
    Args:
        screener_data: screener_scores_*.json의 개별 종목 데이터
        
    Returns:
        trader.py가 기대하는 all_stock_data 형식의 딕셔너리
    """
    ticker = normalize_ticker_6(screener_data.get('ticker', ''))
    if not ticker or ticker == "000000":
        return {}
    
    # 한국어 키 폴백 매핑
    ko_stop = screener_data.get('손절가')
    ko_target = screener_data.get('목표가')
    ko_source = screener_data.get('levels_source') or screener_data.get('source')

    out = {
        'Ticker': ticker,
        'Name': screener_data.get('name', f'종목_{ticker}'),
        'Price': int(screener_data.get('price', 0)),
        'Score': float(screener_data.get('score_total', 0.0)),
        'FinScore': float(screener_data.get('fin_score', 0.0)),
        'TechScore': float(screener_data.get('tech_score', 0.0)),
        'MktScore': float(screener_data.get('mkt_score', 0.0)),
        'SectorScore': float(screener_data.get('sector_score', 0.0)),
        'PatternScore': float(screener_data.get('pattern_score', 0.0)),
        'Sector': screener_data.get('sector', ''),
        'RSI': float(screener_data.get('rsi', 50.0)),
        'ATR': float(screener_data.get('atr', 0.0)) if screener_data.get('atr') is not None else None,
        'MA50': float(screener_data.get('ma50', 0.0)),
        'MA200': float(screener_data.get('ma200', 0.0)),
        'MA20Up': bool(screener_data.get('ma20_up', False)),
        'AccumVol': bool(screener_data.get('accum_vol', False)),
        'HigherLows': bool(screener_data.get('higher_lows', False)),
        'Consolidation': bool(screener_data.get('consolidation', False)),
        'YEY': bool(screener_data.get('yey', False)),
        'PER': float(screener_data.get('per', 0.0)) if screener_data.get('per') is not None else None,
        'PBR': float(screener_data.get('pbr', 0.0)) if screener_data.get('pbr') is not None else None,
        'VolKki': float(screener_data.get('vol_kki', 0.0)),
        'Pos52w': float(screener_data.get('pos_52w', 0.0)),
        # 우선 순위: 영문 키 → 한국어 키
        'stop_price': int(screener_data.get('stop_price', 0)) if screener_data.get('stop_price') is not None else (int(ko_stop) if (ko_stop is not None and str(ko_stop).strip() != '') else None),
        'target_price': int(screener_data.get('target_price', 0)) if screener_data.get('target_price') is not None else (int(ko_target) if (ko_target is not None and str(ko_target).strip() != '') else None),
        'levels_source': ko_source,
        'daily_chart': screener_data.get('daily_chart', None),
        'investor_flow': screener_data.get('investor_flow', None),
        'SectorSource': screener_data.get('sector_source', None),
        'exclude_reasons': screener_data.get('exclude_reasons', []),
        'updated_at': screener_data.get('updated_at', '')
    }

    return out

# ────────────────────────────────
# 공용 유틸리티 함수들
# ────────────────────────────────

def tail_file(file_path: str, lines: int = 10) -> List[str]:
    """파일의 마지막 N줄을 읽어서 반환"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
            return [line.rstrip('\n') for line in all_lines[-lines:]]
    except Exception as e:
        logging.getLogger(__name__).warning(f"파일 tail 실패 {file_path}: {e}")
        return []

# ────────────────────────────────
# Phase 1: 공통 유틸리티 함수
# ────────────────────────────────

def extract_broker_order_id(data: Any, primary_key: str = "ODNO") -> str:
    """
    KIS order_cash 응답(DataFrame, 표준화 dict, nested output)에서 주문번호(ODNO) 추출.
    """
    if data is None:
        return ""

    def _from_mapping(m: Dict[str, Any]) -> str:
        if not isinstance(m, dict):
            return ""
        keys = (primary_key, "odno", "ODNO", "order_id", "order_no", "ORD_NO")
        for k in keys:
            v = m.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        for nest_key in ("output", "output1", "output2"):
            nest = m.get(nest_key)
            if isinstance(nest, dict):
                got = _from_mapping(nest)
                if got:
                    return got
            elif isinstance(nest, list):
                for item in nest:
                    if isinstance(item, dict):
                        got = _from_mapping(item)
                        if got:
                            return got
        raw = m.get("raw")
        if isinstance(raw, dict) and raw is not m:
            return _from_mapping(raw)
        return ""

    if isinstance(data, dict):
        return _from_mapping(data)

    if hasattr(data, "empty"):
        try:
            if data is not None and not data.empty:
                return _from_mapping(data.to_dict("records")[0])
        except Exception:
            pass
    return ""


def normalize_ticker_6(ticker: str, market: Optional[str] = None) -> str:
    """
    종목코드를 6자리 표준 형식으로 통일 (당일 매도 방지 등 DB 조회 시 형식 불일치 방지).
    US 시장이면 norm_ticker()로 심볼을 보존한다.
    """
    if is_us_market(market):
        return norm_ticker(ticker, market)
    s = str(ticker).strip()
    try:
        return format(int(s), "06d")
    except (ValueError, TypeError):
        return s.zfill(6) if len(s) <= 6 else s

def check_min_holding_period(ticker: str, min_days: int, current_time: Optional[datetime] = None) -> Tuple[bool, int]:
    """
    최소 보유기간 체크 (통합 함수)
    Returns: (is_eligible, holding_days)
    - is_eligible: 최소 보유일수 충족 여부
    - holding_days: 현재 보유일수
    """
    logger = logging.getLogger(__name__)
    
    if current_time is None:
        current_time = datetime.now(KST)
    
    if min_days <= 0:
        return True, 0
    
    ticker = normalize_ticker_6(ticker)
    
    try:
        from recorder import fetch_trades_by_tickers
        
        trades = fetch_trades_by_tickers([ticker])
        if not trades or ticker not in trades or not trades[ticker]:
            return True, 0  # 기록 없으면 통과
        
        buy_trades = [t for t in trades[ticker] if t.get('action', '').upper() == 'BUY']
        if not buy_trades:
            return True, 0
        
        latest_buy = max(buy_trades, key=lambda x: x.get('timestamp', datetime.min))
        buy_time = latest_buy.get('timestamp')
        
        if isinstance(buy_time, str):
            buy_time = datetime.fromisoformat(buy_time.replace('Z', '+00:00'))
        elif buy_time.tzinfo is None:
            buy_time = buy_time.replace(tzinfo=KST)
        
        holding_days = (current_time - buy_time).days
        is_eligible = holding_days >= min_days
        
        return is_eligible, holding_days
    except Exception as e:
        logger.warning(f"[{ticker}] 최소 보유기간 체크 실패: {e}")
        return True, 0  # 오류 시 안전하게 통과


def check_min_holding_hours(ticker: str, min_hours: int, current_time: Optional[datetime] = None) -> Tuple[bool, float]:
    """
    최소 보유시간 체크 (당일 매도 방지)
    Returns: (is_eligible, holding_hours)
    """
    logger = logging.getLogger(__name__)
    
    if current_time is None:
        current_time = datetime.now(KST)
    
    if min_hours <= 0:
        return True, 0.0
    
    # DB 조회 시 '0280360' vs '280360' 형식 불일치 방지 (당일 매도 방지 우회 버그 수정)
    ticker = normalize_ticker_6(ticker)
    
    try:
        from recorder import fetch_trades_by_tickers
        
        trades = fetch_trades_by_tickers([ticker])
        if not trades or ticker not in trades or not trades[ticker]:
            return True, 0.0
        
        # 취소된 주문 제외, executed·pending만 사용 (당일 매도 방지는 주문 접수 시각부터 적용)
        def _valid_buy(t):
            if t.get('action', '').upper() != 'BUY':
                return False
            st = (t.get('order_status') or 'executed').lower()
            return st in ('executed', 'pending', 'submitted', 'partial')
        
        buy_trades = [t for t in trades[ticker] if _valid_buy(t)]
        if not buy_trades:
            return True, 0.0
        
        # 당일 매도 방지: 당일 최초 매수(또는 주문 접수) 시각 기준으로 보유시간 계산.
        # 11시 pending → 15시 20분 체결 시 두 건 있으면, 가장 이른 시각(11시)을 써서 당일 매도 차단.
        today = current_time.date()
        today_buys = []
        for t in buy_trades:
            ts = t.get('timestamp')
            if ts is None:
                continue
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=KST)
            if ts.date() == today:
                today_buys.append(ts)
        
        if not today_buys:
            return True, 0.0
        
        # 당일 매수 중 가장 이른 시각 기준
        buy_time = min(today_buys)
        holding_hours = (current_time - buy_time).total_seconds() / 3600.0
        is_eligible = holding_hours >= min_hours
        return is_eligible, holding_hours
    except Exception as e:
        # 보유시간 체크는 "당일 매도 방지" 안전장치이므로,
        # 실패 시 보수적으로 '매도 불가'로 처리한다.
        logger.warning(f"[{ticker}] 최소 보유시간 체크 실패(보수적 차단): {e}")
        return False, 0.0


def validate_config_consistency(config: Dict) -> List[str]:
    """설정값 일관성 검증"""
    errors = []
    
    # 손절매 일관성 검증
    strategy_sl = config.get("strategy_params", {}).get("stop_loss_pct")
    risk_sl = config.get("risk_params", {}).get("stop_pct")
    auto_sl = config.get("risk_params", {}).get("auto_sell", {}).get("stop_loss_pct")
    
    if strategy_sl and risk_sl and abs(strategy_sl - risk_sl) > 0.001:
        errors.append(f"손절매 불일치: strategy_params={strategy_sl}, risk_params={risk_sl}")
    
    if strategy_sl and auto_sl and abs(strategy_sl - auto_sl) > 0.001:
        errors.append(f"손절매 불일치: strategy_params={strategy_sl}, auto_sell={auto_sl}")
    
    # 익절 일관성 검증
    strategy_tp = config.get("strategy_params", {}).get("take_profit_pct")
    auto_tp = config.get("risk_params", {}).get("auto_sell", {}).get("target_pct")
    
    if strategy_tp and auto_tp and abs(strategy_tp - auto_tp) > 0.001:
        errors.append(f"익절 불일치: strategy_params={strategy_tp}, auto_sell={auto_tp}")
    
    return errors

# 모듈 로드 확인용 디버그 로그
# ────────────────────────────────
logging.getLogger(__name__).debug(
    "utils loaded. exports: %s",
    ", ".join(__all__),
)
