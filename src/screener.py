# src/screener.py
import os
import json
import logging
import argparse
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, List, Any, Tuple
from pathlib import Path
import threading
from collections import defaultdict
import random

import numpy as np
import pandas as pd

from kis_master import load_kis_master

# ───────────────── utils (모듈 임포트; load_config는 hasattr로 접근) ─────────────────
import utils  # 모듈 전체 임포트

from utils import (
    setup_logging,
    OUTPUT_DIR,
    CACHE_DIR,
    cache_load,
    cache_save,
    find_latest_file,
    is_market_open_day,
    KST,  # ← generated_at용
    get_cfg,
    compute_52w_position,
    compute_kki_metrics,
    count_consecutive_up,
    is_newly_listed,
    is_us_market,
    norm_ticker,
    norm_ticker_series,
    us_excd,
    min_trading_value_5d_avg,
    resolve_us_excd,
    set_us_ticker_excd_map,
    set_us_ticker_ovrs_excg_map,
    format_pipeline_artifact,
    resolve_pipeline_context,
    us_regime_benchmark,
    _to_float,
)

# 시장 분석 모듈 (screener_core에서 통합)
from screener_core import MarketAnalyzer, MarketRegime, MarketState, get_historical_prices

# ─────── 스키마 메타 ───────
SCHEMA_VERSION = "1.2"  # Output schema pinned

# ─────── 캐시 버전 키 ───────
# 버그 수정으로 기존 캐시를 강제 무효화해야 할 때 버전을 올린다(파일명에 포함됨).
# v2: 상장일 오인식/섹터 트렌드 404 버그 수정으로 기존 캐시 폐기
CACHE_PREFIX_LISTING = "kis_listing_v2"
CACHE_PREFIX_SECTOR_MAP = "kis_sector_map_v2"
CACHE_PREFIX_SECTOR_TRENDS = "sector_trends_v2"

# 전역 워커 상한 (폭주 방지)
MAX_WORKERS_HARD_CAP = int(os.getenv("WORKERS_HARD_CAP", "8"))

# 전역 시장 상태 (MarketAnalyzer 결과 저장)
_CURRENT_MARKET_STATE = None

# ---- load_config 폴백 (get_cfg가 주력이지만 호환성을 위해 유지) ----
def _load_config_fallback() -> dict:
    """utils.load_config가 없거나 실패할 때 쓰는 폴백 로더"""
    cfg_path = getattr(utils, "CONFIG_PATH", Path("/app/config/config.json"))
    try:
        p = Path(cfg_path)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        logging.getLogger(__name__).error("설정 파일을 찾을 수 없습니다: %s", p)
        return {}
    except Exception as e:
        logging.getLogger(__name__).error("설정 파일 읽기 실패: %s", e)
        return {}

# ───────────────── notifier ─────────────────
from notifier import (
    DiscordLogHandler,
    WEBHOOK_URL,
    is_valid_webhook,
    send_discord_message,
)

# ───────────────── 계산 코어 (부작용 없음) ─────────────────
from screener_core import (
    calculate_rsi,           # RSI 계산
    calculate_atr,           # ATR 계산
)

# ───────────────── 기본 설정/로깅 ─────────────────
setup_logging()
logger = logging.getLogger("screener")
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# 루트 로거에 디스코드 에러 핸들러 부착(중복 방지)
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
    now = time.time()
    if key not in _last_sent or now - _last_sent[key] >= cooldown_sec:
        _last_sent[key] = now
        try:
            send_discord_message(content=content)
        except Exception:
            pass

@contextmanager
def stage(name: str, notify_key: Optional[str] = None):
    t0 = time.perf_counter()
    logger.info("▶ %s 시작", name)
    if notify_key:
        try:
            _notify(f"▶ {name} 시작", key=f"{notify_key}_start", cooldown_sec=60)
        except Exception as e:
            logger.debug("stage 시작 알림 실패(무시): %s", e)
    try:
        yield
    finally:
        secs = time.perf_counter() - t0
        logger.info("⏱ %s 완료 (%.2fs)", name, secs)
        if notify_key:
            try:
                _notify(f"⏱ {name} 완료 ({secs:.1f}s)", key=f"{notify_key}_done", cooldown_sec=60)
            except Exception as e:
                logger.debug("stage 완료 알림 실패(무시): %s", e)

# ───────────────── 유틸 함수 (로컬) ─────────────────
def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _describe_series(name: str, s: pd.Series):
    s_num = pd.to_numeric(s, errors="coerce").dropna()
    if s_num.empty:
        logger.info("[%s] 값 없음", name)
        return
    qs = s_num.quantile([0.5, 0.75, 0.9, 0.95]).to_dict()
    logger.info(
        "[%s] 중앙값=%s, P75=%s, P90=%s, P95=%s, 최대=%s",
        name,
        f"{int(qs.get(0.5, 0)):,}",
        f"{int(qs.get(0.75, 0)):,}",
        f"{int(qs.get(0.9, 0)):,}",
        f"{int(qs.get(0.95, 0)):,}",
        f"{int(s_num.max()):,}",
    )

# ───────────────── 단계별 디버깅 로깅 헬퍼 ─────────────────
def _log_funnel(title: str, stages: List[Tuple[str, int]]) -> None:
    """스크리닝 단계별 생존 종목 수를 퍼널 형태로 로깅한다.

    stages: [(단계명, 생존 종목 수), ...] (입력 순서대로 출력)
    """
    if not stages:
        return
    start = stages[0][1] or 1
    logger.info("┌─ %s 퍼널 ──────────────────", title)
    prev: Optional[int] = None
    for label, cnt in stages:
        if prev is None:
            logger.info("│ %-26s %6d", label, cnt)
        else:
            drop = prev - cnt
            pct = (cnt / start * 100.0) if start else 0.0
            logger.info("│ %-26s %6d  (−%d, 전체대비 %.1f%%)", label, cnt, drop, pct)
        prev = cnt
    logger.info("└──────────────────────────────────────")


def _log_dropped(stage_label: str, before_idx, after_idx, limit: int = 30) -> None:
    """단계 통과 전/후 인덱스를 비교해 제외된 티커 목록을 DEBUG 로그로 남긴다."""
    try:
        after_set = set(str(t) for t in after_idx)
        dropped = [str(t) for t in before_idx if str(t) not in after_set]
        if not dropped:
            return
        shown = dropped[:limit]
        suffix = f" 외 {len(dropped) - limit}건" if len(dropped) > limit else ""
        logger.debug("[%s] 제외 %d건: %s%s", stage_label, len(dropped), ", ".join(shown), suffix)
    except Exception as e:
        logger.debug("[%s] 제외 목록 로깅 실패(무시): %s", stage_label, e)

# ─────────── 상장일(KIS) 캐시 ───────────
from api.kis_auth import KIS
_KIS_INSTANCE: Optional[KIS] = None

# 메모리 캐시
_LISTING_DATES_CACHE: Dict[str, Optional[datetime]] = {}
_LISTING_PREFETCHED = False
_LISTING_LOCK = threading.Lock()

# ─────────── KIS 레이트 리미터 ───────────
class RateLimiter:
    def __init__(self, rps: float):
        # rps가 0이면 비활성
        self.min_interval = 1.0 / max(0.1, float(rps))
        self._last = 0.0
        self._lock = threading.Lock()
    def wait(self):
        with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

_KIS_RATE_LIMITER: Optional[RateLimiter] = None
_KIS_MAX_CONCURRENCY: int = 2

def _is_kis_ratelimit_error(e: Exception) -> bool:
    msg = str(e)
    return ("EGW00201" in msg) or ("초당 거래건수" in msg)

def _parse_listing_date_value(v: Any) -> Optional[datetime]:
    """KIS 응답의 다양한 상장일 필드를 datetime으로 변환"""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"):
        try:
            digits = "".join(ch for ch in s if ch.isdigit())
            if fmt in ("%Y%m%d", "%Y-%m-%d") and len(digits) >= 8:
                return datetime.strptime(digits[:8], "%Y%m%d")
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        try:
            return datetime.strptime(digits[:8], "%Y%m%d")
        except Exception:
            pass
    return None

def _is_plausible_listing_date(dt: Optional[datetime]) -> bool:
    """상장일로서 타당한 날짜인지 검증.
    - 미래 날짜(오늘 이후)는 무효
    - 1956년(KRX 개장) 이전은 무효
    잘못된 숫자 필드가 날짜로 오인되는 것을 방지한다.
    """
    if dt is None:
        return False
    try:
        today = datetime.now()
        if dt.date() > today.date():
            return False
        if dt.year < 1956:
            return False
        return True
    except Exception:
        return False


def _extract_listing_date_from_kis_df(df: pd.DataFrame) -> Optional[datetime]:
    """inquire_price 응답에서 상장일 후보 컬럼만 골라 추출한다.

    주의: 과거에는 후보 컬럼이 없으면 '모든 컬럼'을 스캔해 8자리 숫자를 날짜로
    채택했는데, w52_hgpr_date(52주 최고가 일자) 등 최근 일자 필드가 상장일로
    오인되어 정상 종목이 '신규상장'으로 대량 누락되는 버그가 있었다.
    → 무차별 컬럼 스캔을 제거하고, 후보 컬럼 + 타당성 검증만 사용한다.
    """
    if df is None or df.empty:
        return None
    # 진짜 상장일 의미의 컬럼만 후보로 사용
    candidates = [
        "lstg_dt", "lstg_de", "lstg_st_dt", "scts_lstg_dt", "kospi_lstg_dt",
        "list_dt", "list_dd", "list_dttm", "list_dtm", "ipo_dt", "ipo_de",
        "stck_lstg_dt", "lstn_dt",
    ]
    # 상장일이 아닌 날짜성 컬럼(오인 방지용 블랙리스트)
    blacklist = {
        "w52_hgpr_date", "w52_lwpr_date", "stck_dryy_hgpr_date", "stck_dryy_lwpr_date",
        "dryy_hgpr_date", "dryy_lwpr_date", "stck_bsop_date", "bstp_nmix_prdy_date",
    }
    cols_map = {str(c).strip().lower(): c for c in df.columns}
    for key in candidates:
        low = key.lower()
        if low in blacklist:
            continue
        if low in cols_map:
            val = df[cols_map[low]].iloc[0]
            dt = _parse_listing_date_value(val)
            if dt and _is_plausible_listing_date(dt):
                return dt
    # 후보 컬럼에서 타당한 상장일을 찾지 못하면 '데이터 없음'으로 처리(스킵 방지)
    return None


def _screener_ticker_key(ticker: str, market: Optional[str] = None) -> str:
    """스크리너 캐시/API 공통 티커 키 (US: AAPL, KR: 6자리)."""
    return norm_ticker(ticker, market or os.getenv("MARKET", "SP500"))


def _kis_quote_for_screener_meta(
    kis: KIS,
    ticker: str,
    market: str,
    *,
    retries: int = 4,
) -> Optional[pd.DataFrame]:
    """섹터/상장일 메타: US=해외 price-detail, KR=국내 inquire_price."""
    code = _screener_ticker_key(ticker, market)
    if not code:
        return None
    if is_us_market(market):
        for attempt in range(max(1, retries)):
            try:
                if _KIS_RATE_LIMITER:
                    _KIS_RATE_LIMITER.wait()
                df = kis.overseas_price_detail(resolve_us_excd(code, market), code)
                return df if df is not None and not df.empty else None
            except Exception as e:
                if _is_kis_ratelimit_error(e) and attempt < retries - 1:
                    backoff = min(1.0 * (attempt + 1), 3.0) + random.uniform(0, 0.25)
                    time.sleep(backoff)
                    continue
                logger.debug("KIS overseas price-detail 실패(%s): %s", code, str(e))
                return None
        return None
    return _kis_inquire_price_safe(kis, code, retries=retries)


def _kis_inquire_price_safe(kis: KIS, code: str, retries: int = 4) -> Optional[pd.DataFrame]:
    """KIS API 호출(상장일/섹터) - 레이트 리미터 + 지수 백오프"""
    code = str(code).zfill(6)
    for attempt in range(max(1, retries)):
        try:
            if _KIS_RATE_LIMITER:
                _KIS_RATE_LIMITER.wait()
            return kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd=code)
        except Exception as e:
            if _is_kis_ratelimit_error(e) and attempt < retries - 1:
                backoff = min(1.0 * (attempt + 1), 3.0) + random.uniform(0, 0.25)
                time.sleep(backoff)
                continue
            logger.debug("KIS inquire_price 실패(%s): %s", code, str(e))
            return None


def _kis_overseas_daily_price_safe(
    kis: KIS,
    symb: str,
    date_str: str,
    market: str,
    *,
    excd: Optional[str] = None,
    retries: int = 4,
) -> pd.DataFrame:
    """해외주식 기간별시세(HHDFS76240000) — Amount5D용."""
    symb = norm_ticker(symb, market)
    excd = excd or resolve_us_excd(symb, market)
    for attempt in range(max(1, retries)):
        try:
            if _KIS_RATE_LIMITER:
                _KIS_RATE_LIMITER.wait()
            return kis.overseas_daily_price(excd, symb, bymd=date_str, gubn="0", modp="0")
        except Exception as e:
            if _is_kis_ratelimit_error(e) and attempt < retries - 1:
                backoff = min(1.5 * (2 ** attempt), 8.0) + random.uniform(0, 0.3)
                time.sleep(backoff)
                continue
            logger.debug("KIS overseas dailyprice 실패(%s): %s", symb, str(e))
            return pd.DataFrame()
    return pd.DataFrame()


def _kis_period_price_safe(
    kis: KIS,
    code: str,
    start_date: str,
    end_date: str,
    *,
    market_div: str = "J",
    market: Optional[str] = None,
    retries: int = 3,
) -> pd.DataFrame:
    """KIS 기간별시세(FHKST03010100) 안전 래퍼 (레이트리밋/백오프)."""
    if is_us_market(market):
        return _kis_overseas_daily_price_safe(kis, code, end_date, market or "SP500", retries=retries)
    code = norm_ticker(code, market)
    for attempt in range(max(1, retries)):
        try:
            if _KIS_RATE_LIMITER:
                _KIS_RATE_LIMITER.wait()
            return kis.inquire_period_price(
                fid_cond_mrkt_div_code=market_div,
                fid_input_iscd=code,
                fid_input_date_1=start_date,
                fid_input_date_2=end_date,
                fid_period_div_code="D",
                fid_org_adj_prc="0",
            )
        except Exception as e:
            if _is_kis_ratelimit_error(e) and attempt < retries - 1:
                backoff = min(1.0 * (attempt + 1), 3.0) + random.uniform(0, 0.25)
                time.sleep(backoff)
                continue
            logger.debug("KIS 기간별시세 실패(%s): %s", code, str(e))
            return pd.DataFrame()
    return pd.DataFrame()


def _kis_investor_trend_safe(
    kis: KIS,
    code: str,
    start_date: str,
    end_date: str,
    *,
    market_div: str = "J",
    retries: int = 3,
) -> pd.DataFrame:
    """KIS 투자자별추이(FHKST01010900) 안전 래퍼."""
    code = str(code).zfill(6)
    for attempt in range(max(1, retries)):
        try:
            if _KIS_RATE_LIMITER:
                _KIS_RATE_LIMITER.wait()
            return kis.inquire_investor_trend(
                fid_cond_mrkt_div_code=market_div,
                fid_input_iscd=code,
                fid_input_date_1=start_date,
                fid_input_date_2=end_date,
            )
        except Exception as e:
            if _is_kis_ratelimit_error(e) and attempt < retries - 1:
                backoff = min(1.0 * (attempt + 1), 3.0) + random.uniform(0, 0.25)
                time.sleep(backoff)
                continue
            logger.debug("KIS 투자자별추이 실패(%s): %s", code, str(e))
            return pd.DataFrame()
    return pd.DataFrame()


def _kis_industry_price_safe(
    kis: KIS,
    industry_code: str,
    start_date: str,
    end_date: str,
    *,
    retries: int = 3,
) -> pd.DataFrame:
    """KIS 업종 일자별(FHKUP03500100) 안전 래퍼."""
    industry_code = str(industry_code).strip()
    for attempt in range(max(1, retries)):
        try:
            if _KIS_RATE_LIMITER:
                _KIS_RATE_LIMITER.wait()
            return kis.inquire_industry_period_price(
                fid_input_iscd=industry_code,
                fid_input_date_1=start_date,
                fid_input_date_2=end_date,
                fid_period_div_code="D",
            )
        except Exception as e:
            if _is_kis_ratelimit_error(e) and attempt < retries - 1:
                backoff = min(1.0 * (attempt + 1), 3.0) + random.uniform(0, 0.25)
                time.sleep(backoff)
                continue
            logger.debug("KIS 업종일자별 실패(%s): %s", industry_code, str(e))
            return pd.DataFrame()
    return pd.DataFrame()

# === KIS 1회 호출로 섹터+상장일 동시 조회 (호출 수 절반) ===
def kis_fetch_sector_and_listing_batch(
    kis: KIS,
    codes: List[str],
    date_str: str,
    workers: int = 4,
    market: Optional[str] = None,
) -> None:
    """
    inquire_price(국내) 또는 overseas price-detail(US) 1회당 섹터·상장일 추출.
    kis_sector_map 캐시와 _LISTING_DATES_CACHE/kis_listing 캐시를 채운다.
    """
    if not codes:
        return
    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()
    uniq = [_screener_ticker_key(c, mkt) for c in pd.unique(pd.Series(codes))]
    uniq = [c for c in uniq if c]

    # 이미 두 캐시가 모두 찼으면 스킵 (파일 캐시 기준)
    sector_cached = cache_load(CACHE_PREFIX_SECTOR_MAP, date_str)
    listing_cached = cache_load(CACHE_PREFIX_LISTING, date_str)
    targets = uniq
    if isinstance(listing_cached, dict) and listing_cached:
        with _LISTING_LOCK:
            for k, v in listing_cached.items():
                key = _screener_ticker_key(k, mkt)
                if key not in _LISTING_DATES_CACHE or _LISTING_DATES_CACHE[key] is None:
                    if isinstance(v, str):
                        try:
                            _LISTING_DATES_CACHE[key] = datetime.strptime(v, "%Y-%m-%d")
                        except Exception:
                            _LISTING_DATES_CACHE[key] = _parse_listing_date_value(v)
                    else:
                        _LISTING_DATES_CACHE[key] = v
    if isinstance(sector_cached, dict) and sector_cached and isinstance(listing_cached, dict) and listing_cached:
        missing = [c for c in uniq if c not in sector_cached or c not in listing_cached]
        if not missing:
            logger.info("KIS 통합 조회 스킵(섹터+상장일 캐시 모두 존재).")
            return
        targets = missing

    sector_map: Dict[str, str] = dict(sector_cached) if isinstance(sector_cached, dict) else {}

    def _fetch_one(code: str) -> Tuple[str, Optional[str], Optional[datetime]]:
        df = _kis_quote_for_screener_meta(kis, code, mkt)
        if df is None or df.empty:
            return code, None, None
        sec = _extract_sector_from_kis_df(df)
        dt = _extract_listing_date_from_kis_df(df) if not is_us_market(mkt) else None
        return code, _normalize_sector_name(sec) if sec else "N/A", dt

    actual_workers = max(1, min(workers, _KIS_MAX_CONCURRENCY))
    total = len(targets)
    api_label = "overseas price-detail" if is_us_market(mkt) else "inquire_price"
    logger.info(
        "KIS 통합 조회(섹터+상장일) 시작 (대상 %d종목, %s, market=%s)",
        total, api_label, mkt,
    )
    with ThreadPoolExecutor(max_workers=actual_workers) as ex:
        futs = {ex.submit(_fetch_one, c): c for c in targets}
        for i, fut in enumerate(as_completed(futs), start=1):
            try:
                code, sec, dt = fut.result()
            except Exception as e:
                logger.warning("KIS 통합 조회 실패(%s): %s", futs.get(fut), e)
                continue
            sector_map[code] = sec or "N/A"
            with _LISTING_LOCK:
                _LISTING_DATES_CACHE[code] = dt
            if i % 20 == 0 or i == total:
                logger.info("  >> KIS 통합 조회 진행: %d/%d (%.1f%%)", i, total, i * 100.0 / total)

    if sector_map:
        cache_save(CACHE_PREFIX_SECTOR_MAP, date_str, sector_map)
    with _LISTING_LOCK:
        serializable = {
            k: (v.strftime("%Y-%m-%d") if isinstance(v, datetime) else None)
            for k, v in _LISTING_DATES_CACHE.items()
        }
    cache_save(CACHE_PREFIX_LISTING, date_str, serializable)
    logger.info("KIS 통합 조회 완료: 섹터 %d건, 상장일 캐시 갱신", len(sector_map))

# === 신규 추가: 공개 API ===
def get_listing_date_kis_prefetch(
    kis: KIS,
    codes: List[str],
    date_str: str,
    workers: int = 4,
    market: Optional[str] = None,
) -> None:
    """
    요청한 날짜 키(date_str) 기준으로 KIS 상장일을 일괄 프리패치해
    - 메모리 캐시(_LISTING_DATES_CACHE)
    - 파일 캐시(cache_save("kis_listing", date_str, ...))
    에 저장한다.
    """
    if not codes:
        return
    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()
    uniq = [_screener_ticker_key(c, mkt) for c in pd.unique(pd.Series(codes))]
    uniq = [c for c in uniq if c]

    # 파일 캐시가 있으면 먼저 로딩
    cached = cache_load(CACHE_PREFIX_LISTING, date_str)
    if isinstance(cached, dict) and cached:
        logger.info("상장일(KIS) 캐시 로드: %s_%s.pkl", CACHE_PREFIX_LISTING, date_str)
        with _LISTING_LOCK:
            for k, v in cached.items():
                if isinstance(v, str):
                    try:
                        cached_dt = datetime.strptime(v, "%Y-%m-%d")
                    except Exception:
                        cached_dt = _parse_listing_date_value(v)
                else:
                    cached_dt = v
                _LISTING_DATES_CACHE[_screener_ticker_key(k, mkt)] = cached_dt

    # 아직 없는 코드만 병렬 조회
    targets = []
    with _LISTING_LOCK:
        for c in uniq:
            if c not in _LISTING_DATES_CACHE or _LISTING_DATES_CACHE[c] is None:
                targets.append(c)
    if not targets:
        logger.info("상장일(KIS) 일괄 조회 스킵(모든 대상이 캐시에 있음).")
        return

    logger.info("상장일(KIS) 일괄 조회 시작 (대상 %d종목)", len(targets))

    def _fetch(code: str) -> Tuple[str, Optional[datetime]]:
        if is_us_market(mkt):
            return code, None
        df = _kis_quote_for_screener_meta(kis, code, mkt)
        dt = _extract_listing_date_from_kis_df(df) if df is not None else None
        return code, dt

    actual_workers = max(1, min(workers, _KIS_MAX_CONCURRENCY))
    done = 0
    with ThreadPoolExecutor(max_workers=actual_workers) as ex:
        futs = {ex.submit(_fetch, c): c for c in targets}
        total = len(futs)
        for i, fut in enumerate(as_completed(futs), start=1):
            try:
                code, dt = fut.result()
            except Exception as e:
                logger.warning("상장일 조회 실패(%s): %s", futs.get(fut), e)
                continue
            with _LISTING_LOCK:
                _LISTING_DATES_CACHE[code] = dt
            done += 1
            logger.info("  >> 상장일(KIS) 조회 진행: %d/%d (%.1f%%)", i, total, i * 100.0 / total)

    # 파일 캐시에 전체 저장(이미 있던 값 포함)
    with _LISTING_LOCK:
        serializable = {k: (v.strftime("%Y-%m-%d") if isinstance(v, datetime) else None) for k, v in _LISTING_DATES_CACHE.items()}
    cache_save(CACHE_PREFIX_LISTING, date_str, serializable)
    logger.info("상장일(KIS) 일괄 조회 완료: %d건 캐시", done)

# 유지: 내부 사용(기존 이름과 호환)
def prefetch_listing_dates_kis(codes: List[str], kis: KIS, workers: int = 4):
    # date_str은 비즈니스 날짜 키로 묶어 저장
    date_key = datetime.now().strftime("%Y%m%d")
    return get_listing_date_kis_prefetch(kis, codes, date_key, workers)

def get_listing_date(ticker: str, market: Optional[str] = None) -> Optional[datetime]:
    """상장일을 캐시에서 반환. 없으면 KIS 단건 조회(조용히)."""
    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()
    code = _screener_ticker_key(ticker, mkt)
    with _LISTING_LOCK:
        if code in _LISTING_DATES_CACHE:
            return _LISTING_DATES_CACHE[code]
    kis = _KIS_INSTANCE
    if kis is None:
        return None
    if is_us_market(mkt):
        return None
    df = _kis_quote_for_screener_meta(kis, code, mkt)
    dt = _extract_listing_date_from_kis_df(df) if df is not None else None
    with _LISTING_LOCK:
        _LISTING_DATES_CACHE[code] = dt
    return dt

# ─────────── 스코어링 실패/스킵 집계 ───────────
_fail_stats = defaultdict(int)
_fail_rows: List[Dict[str, Any]] = []
_fail_lock = threading.Lock()

def standardize_ohlcv(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    다양한 컬럼명(영문 소문자/한글/조정종가/변형명)을 표준 OHLCV로 매핑.
    반환: (표준화 DF or None, 실패사유 or None)
    """
    if df is None or df.empty:
        return None, "empty_price"

    d = df.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]

    cand = {
        "open":   ["open", "시가"],
        "high":   ["high", "고가"],
        "low":    ["low", "저가"],
        "close":  ["close", "종가", "adj close", "adj_close", "adjclose", "adjusted_close", "close*"],
        "volume": ["volume", "거래량", "vol"],
    }

    def _find(names: List[str]) -> Optional[str]:
        for n in names:
            if n.endswith("*"):
                base = n[:-1]
                cand_cols = [c for c in d.columns if c.startswith(base)]
                if cand_cols:
                    return cand_cols[0]
            elif n in d.columns:
                return n
        return None

    out = {}
    for key, names in cand.items():
        found = _find(names)
        if found is None:
            if key == "volume":
                out["Volume"] = pd.Series(0, index=d.index)  # volume 없으면 0
            else:
                return None, f"missing_{key}"
        else:
            out[key.capitalize()] = d[found]

    std = pd.DataFrame(out, index=d.index)
    try:
        std = std.sort_index()
    except Exception:
        pass
    return std, None

def get_stock_listing(market: str = "KOSPI") -> pd.DataFrame:
    """
    종목 마스터 조회 (KIS 종목정보파일 .mst 기반)
    - 기본 뼈대: Name, Sector, Close(기준가), ListedShares, Marcap(기준가*상장주식수)
    """
    try:
        fixed_market = (market or "KOSPI").upper()
        # date 키는 캐시 단위를 하루로 묶어 충분(장 시작 전 다운로드를 가정)
        date_key = datetime.now().strftime("%Y%m%d")
        df = load_kis_master(fixed_market, cache_key=date_key, force_refresh=False)
        if df is None or df.empty:
            logger.error("KIS 마스터(.mst) 로드 실패/빈 DF: market=%s", fixed_market)
            return pd.DataFrame()
        # screener 파이프라인 호환: index=Code
        out = df.copy()
        if out.index.name != "Code":
            out.index.name = "Code"
        # 최소 컬럼 보장
        if "Name" not in out.columns:
            out["Name"] = out.index.astype(str)
        if "Marcap" not in out.columns:
            close = pd.to_numeric(out.get("Close", 0), errors="coerce").fillna(0)
            shares = pd.to_numeric(out.get("ListedShares", 0), errors="coerce").fillna(0)
            out["Marcap"] = close * shares
        if is_us_market(market):
            if "EXCD" in out.columns:
                set_us_ticker_excd_map(dict(zip(out.index.astype(str), out["EXCD"].astype(str))))
            if "OvrsExcg" in out.columns:
                set_us_ticker_ovrs_excg_map(dict(zip(out.index.astype(str), out["OvrsExcg"].astype(str))))
            logger.info(
                "US 티커 거래소 맵 로드: %d종 (EXCD 분포 %s)",
                len(out),
                out["EXCD"].value_counts().to_dict() if "EXCD" in out.columns else {},
            )
        return out
    except Exception as e:
        logger.error("KIS 마스터(.mst) 조회 실패: %s", str(e))
        return pd.DataFrame()


def get_fundamentals(
    date_str: str,
    market: str = "KOSPI",
    tickers: Optional[List[str]] = None,
    kis: Optional[KIS] = None,
) -> pd.DataFrame:
    """
    펀더멘털(주로 PER/PBR) 조회.
    - 전체 종목을 긁지 않고, tickers(관심종목)만 KIS inquire_price로 조회해 병합.
    - 반환은 인덱스=Code(6자리), 컬럼 PER/PBR (없으면 NaN)
    """
    try:
        if not tickers:
            return pd.DataFrame()
        kis = kis or _KIS_INSTANCE
        if kis is None:
            return pd.DataFrame()

        uniq = [norm_ticker(t, market) for t in pd.unique(pd.Series(tickers)) if t]
        rows = []
        for code in uniq:
            if is_us_market(market):
                if _KIS_RATE_LIMITER:
                    _KIS_RATE_LIMITER.wait()
                df = kis.overseas_price_detail(resolve_us_excd(code, market), code)
            else:
                df = _kis_inquire_price_safe(kis, code, retries=3)
            if df is None or df.empty:
                continue
            row = df.iloc[0].to_dict()
            per = row.get("per") or row.get("PER") or row.get("perx") or row.get("stck_per")
            pbr = row.get("pbr") or row.get("PBR") or row.get("pbrx") or row.get("stck_pbr")
            rows.append({"Code": code, "PER": per, "PBR": pbr})
        if not rows:
            return pd.DataFrame()
        out = pd.DataFrame(rows).set_index("Code")
        out.index.name = "Code"
        out["PER"] = pd.to_numeric(out.get("PER"), errors="coerce")
        out["PBR"] = pd.to_numeric(out.get("PBR"), errors="coerce")
        return out
    except Exception as e:
        logger.debug("KIS 펀더멘털 조회 실패: %s", e)
        return pd.DataFrame()


def _get_us_benchmark_close(date_str: str, min_bars: int = 60) -> Optional[pd.Series]:
    """US 레짐/추세: SPY 일봉 종가 (FinanceDataReader)."""
    try:
        end_dt = datetime.strptime(date_str, "%Y%m%d")
        start_dt = (end_dt - timedelta(days=max(400, int(min_bars * 1.8)))).strftime("%Y%m%d")
        sym = us_regime_benchmark("SP500") or "SPY"
        df = get_historical_prices(sym, start_dt, date_str, market="SP500", kis=_KIS_INSTANCE)
        if df is None or df.empty or "Close" not in df.columns:
            return None
        close = pd.to_numeric(df["Close"], errors="coerce").dropna()
        return close if len(close) >= min_bars else None
    except Exception:
        return None


def get_market_trend(date_str: str, market: str = "KOSPI") -> str:
    """
    시장 추세(단기): MA5 vs MA20
    - KR: KIS 업종 일자별 / US: SPY (fdr)
    """
    try:
        if is_us_market(market):
            close = _get_us_benchmark_close(date_str, min_bars=20)
            if close is None or len(close) < 20:
                return "Sideways"
            ma5 = close.rolling(5).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            if pd.isna(ma5) or pd.isna(ma20):
                return "Sideways"
            return "Bull" if ma5 > ma20 else "Bear"

        kis = _KIS_INSTANCE
        if kis is None:
            return "Sideways"
        idx_code = "0001" if (market or "").upper() == "KOSPI" else "1001"
        end_dt = datetime.strptime(date_str, "%Y%m%d")
        start_dt = (end_dt - timedelta(days=60)).strftime("%Y%m%d")
        df_idx = _kis_industry_price_safe(kis, idx_code, start_dt, date_str)
        if df_idx is None or df_idx.empty:
            return "Sideways"
        # 종가 키 후보
        close_col = None
        for c in ["stck_clpr", "clspr", "stck_prpr", "close", "Close", "종가"]:
            if c in df_idx.columns:
                close_col = c
                break
        if close_col is None:
            return "Sideways"
        close = pd.to_numeric(df_idx[close_col], errors="coerce").dropna()
        if len(close) < 20:
            return "Sideways"
        ma5 = close.rolling(5).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        if pd.isna(ma5) or pd.isna(ma20):
            return "Sideways"
        return "Bull" if ma5 > ma20 else "Bear"
    except Exception:
        return "Sideways"

def _norm_code_index(obj: pd.DataFrame) -> pd.DataFrame:
    if obj is None or obj.empty:
        return obj
    try:
        idx = obj.index.astype(str).str.replace(r"[^0-9]", "", regex=True).str.zfill(6)
        obj = obj.copy()
        obj.index = idx
    except Exception:
        pass
    return obj

def analyze_ma20_trend(df: pd.DataFrame) -> bool:
    if len(df) < 21:
        return False
    ma20 = df["Close"].rolling(window=20).mean()
    if pd.isna(ma20.iloc[-1]) or pd.isna(ma20.iloc[-2]):
        return False
    return ma20.iloc[-1] > ma20.iloc[-2]

def analyze_accumulation_volume(df: pd.DataFrame, period: int = 20) -> bool:
    if len(df) < period:
        return False
    recent_df = df.tail(period)
    up_days = recent_df[recent_df["Close"] > recent_df["Open"]]
    down_days = recent_df[recent_df["Close"] <= recent_df["Open"]]
    if len(up_days) < 3 or len(down_days) < 3:
        return False
    avg_vol_up = up_days["Volume"].mean()
    avg_vol_down = down_days["Volume"].mean()
    return avg_vol_up > avg_vol_down * 1.5

def detect_higher_lows(df: pd.DataFrame, period: int = 10) -> bool:
    if len(df) < period:
        return False
    recent_lows = df["Low"].tail(period)
    x = np.arange(len(recent_lows))
    slope, _ = np.polyfit(x, recent_lows, 1)
    return slope > 0

def detect_consolidation(df: pd.DataFrame, prior_trend_period: int = 60, consolidation_period: int = 15) -> bool:
    if len(df) < prior_trend_period + consolidation_period:
        return False
    start_price = df["Close"].iloc[-(prior_trend_period + consolidation_period)]
    peak_price_before_consolidation = df["Close"].iloc[-consolidation_period]
    if (peak_price_before_consolidation - start_price) / start_price < 0.3:
        return False
    cons_df = df.tail(consolidation_period)
    max_high = cons_df["High"].max()
    min_low = cons_df["Low"].min()
    return (max_high - min_low) / min_low < 0.15

def detect_yey_pattern(df: pd.DataFrame) -> bool:
    if len(df) < 3:
        return False
    d2, d1, d0 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    is_yang2 = d2["Close"] > d2["Open"]
    is_eum1 = d1["Close"] < d1["Open"]
    is_yang0 = d0["Close"] > d0["Open"]
    is_reversal = d0["Close"] > d2["Close"]
    return is_yang2 and is_eum1 and is_yang0 and is_reversal

def _normalize_sector_name(x: Optional[str]) -> str:
    if not x or str(x).strip().upper() in {"", "NAN", "NA", "N/A"}:
        return "N/A"
    s = str(x).strip()
    mapping = {
        "보험": "금융", "증권": "금융", "은행": "금융",
        "IT 서비스": "IT서비스", "정보기술": "IT서비스",
        "반도체": "전기전자", "전자": "전기전자",
        "건설": "건설", "조선": "제조", "기계": "제조", "화학": "화학",
        "유통": "유통", "통신": "통신", "의료정밀": "의료정밀", "의약품": "의약품",
    }
    if s in mapping:
        return mapping[s]
    for k, v in mapping.items():
        if k in s:
            return v
    return s

def _extract_sector_from_kis_df(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    for col in ["sect_kr_nm", "bstp_kor_isnm", "bstp_kor_isnm_nm", "induty_kor_isnm"]:
        if col in df.columns:
            val = str(df[col].iloc[0]).strip()
            if val and val.upper() not in {"N/A", "NONE"}:
                return val
    code_cols = ["bstp_cls_code", "std_idst_clsf_cd"]
    for col in code_cols:
        if col in df.columns:
            code = str(df[col].iloc[0]).strip()
            code_map = {"01": "제조", "10": "금융", "15": "IT서비스"}
            if code in code_map:
                return code_map[code]
    return None

# ─────────── KIS 호출 & 섹터 보강 ───────────
def _get_kis_sector_map(
    codes: List[str],
    kis: KIS,
    cache_key: Optional[str] = None,
    workers: int = 4,
    market: Optional[str] = None,
) -> Dict[str, str]:
    if cache_key:
        cached = cache_load(CACHE_PREFIX_SECTOR_MAP, cache_key)
        if isinstance(cached, dict) and cached:
            logger.info("kis 섹터맵 캐시 사용: %s_%s.pkl", CACHE_PREFIX_SECTOR_MAP, cache_key)
            return cached

    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()

    def _fetch_one(code: str) -> Tuple[str, str]:
        key = _screener_ticker_key(code, mkt)
        try:
            df = _kis_quote_for_screener_meta(kis, code, mkt)
            if df is not None and not df.empty:
                sec = _extract_sector_from_kis_df(df)
                return (key, _normalize_sector_name(sec) if sec else "N/A")
            return (key, "N/A")
        except Exception as e:
            logger.debug("KIS 섹터 조회 실패(%s): %s", key, str(e))
            return (key, "N/A")

    sectors: Dict[str, str] = {}
    actual_workers = max(1, min(workers, _KIS_MAX_CONCURRENCY))
    with ThreadPoolExecutor(max_workers=actual_workers) as ex:
        futs = {ex.submit(_fetch_one, c): c for c in codes}
        total = len(codes)
        for i, fut in enumerate(as_completed(futs), start=1):
            k, v = fut.result()
            sectors[k] = v
            if i % 20 == 0 or i == total:
                logger.info("  >> KIS(inquire_price) 섹터 조회 진행: %d/%d (%.1f%%)", i, total, i * 100.0 / total)
    if cache_key:
        cache_save(CACHE_PREFIX_SECTOR_MAP, cache_key, sectors)
    return sectors

def _enrich_sector_with_kis_api(
    df_base: pd.DataFrame,
    kis: KIS,
    workers: int,
    cache_key: Optional[str] = None,
    market: Optional[str] = None,
) -> pd.DataFrame:
    if df_base is None or df_base.empty:
        out = df_base.copy()
        out["Sector"] = out.get("Sector", "N/A")
        return out
    out = df_base.copy()
    if "Sector" not in out.columns:
        out["Sector"] = np.nan
    out["Sector"] = out["Sector"].astype("object")
    target_idx = out.index[out["Sector"].isna() | out["Sector"].eq("N/A")]
    if len(target_idx) == 0:
        logger.info("KIS 보강 대상 없음.")
        return out
    
    # 중복 인덱스 검증 및 제거
    if out.index.duplicated().any():
        dup_count = out.index.duplicated().sum()
        logger.warning(f"입력 데이터에 중복 인덱스 {dup_count}개 발견, 첫 번째 값 유지")
        out = out[~out.index.duplicated(keep='first')]
        target_idx = out.index[out["Sector"].isna() | out["Sector"].eq("N/A")]
    
    logger.info("KIS(inquire_price) 섹터 보강 시작 (대상 %d종목)", len(target_idx))
    ck = cache_key or datetime.now().strftime("%Y%m%d")
    
    try:
        kis_map = _get_kis_sector_map(
            target_idx.tolist(), kis, ck, workers, market=market
        )
        
        # KIS 맵 결과 검증
        if kis_map and len(kis_map) > 0:
            # 중복 키 검증
            if len(kis_map) != len(set(kis_map.keys())):
                logger.warning(f"KIS 맵에 중복 키 {len(kis_map) - len(set(kis_map.keys()))}개 발견")
                # 중복 제거 (첫 번째 값 유지)
                unique_kis_map = {}
                for k, v in kis_map.items():
                    if k not in unique_kis_map:
                        unique_kis_map[k] = v
                kis_map = unique_kis_map
            
            # 안전한 매핑
            mapped_values = out.loc[target_idx].index.to_series().map(kis_map)
            out.loc[target_idx, "Sector"] = mapped_values.values
            logger.info(f"KIS 섹터 매핑 완료: {mapped_values.notna().sum()}/{len(target_idx)} 성공")
        else:
            logger.warning("KIS 섹터 맵이 비어있음")
            
    except Exception as e:
        logger.error(f"KIS 섹터 보강 중 오류 발생: {e}")
        # 오류 발생 시 기본값 유지
        pass
    
    out["Sector"] = out["Sector"].map(_normalize_sector_name).fillna("N/A").astype("object")
    logger.info("✅ KIS(inquire_price) 섹터 정보 보강 완료.")
    return out

def _enrich_sector_with_fdr_krx(df_base: pd.DataFrame, market: str = "KOSPI") -> pd.DataFrame:
    """(Deprecated) FDR 제거로 인해 입력을 그대로 반환."""
    out = df_base.copy()
    if "Sector" not in out.columns:
        out["Sector"] = "N/A"
    out["Sector"] = out["Sector"].map(_normalize_sector_name).fillna("N/A").astype("object")
    return out

def _log_sector_summary(df: pd.DataFrame, label: str):
    if "Sector" not in df.columns:
        logger.info("섹터 요약(%s): Sector 컬럼 없음", label)
        return
    sec = df["Sector"].fillna("N/A")
    vc = sec.value_counts()
    na = int(vc.get("N/A", 0))
    tot = int(len(df))
    ratio = (na / tot * 100) if tot > 0 else 0.0
    logger.info(
        "섹터 요약(%s): 고유=%d, N/A=%d (%.1f%%), TOP5=%s",
        label, len(vc), na, ratio, vc.head(5).to_dict(),
    )

def _get_pykrx_ticker_sector_map(date_str: str) -> Dict[str, str]:
    """(Deprecated) pykrx 제거로 인해 빈 맵 반환."""
    return {}


def _enrich_sector_with_pykrx_partial(missing_codes: List[str], date_str: str) -> Dict[str, str]:
    """(Deprecated) pykrx 제거로 인해 빈 맵 반환."""
    return {}

def _sector_code_candidates(k: str) -> List[str]:
    """KIS 마스터의 섹터 키(형식: IDX_big-mid-small)에서 업종지수 조회용
    후보 코드들을 생성한다. 업종지수 코드 규격이 불확실하므로 여러 변형을 시도한다.
    예: 'IDX_27-13-0' → ['0027', '27', '2713', '0013', '13', ...]
    """
    cands: List[str] = []
    try:
        k2 = k.split("IDX_", 1)[1] if "IDX_" in k else k
        parts = [p.strip() for p in k2.split("-") if p.strip() != ""]
        # 0이 아닌 의미있는 세그먼트들
        seg = [p for p in parts if p and p != "0"]
        for p in seg:
            cands.append(p.zfill(4))   # 4자리 zero-pad (예: 0027)
            cands.append("00" + p.zfill(2))  # 00 접두 (예: 0027)
            cands.append(p)            # 원본
        # 대분류+중분류 결합 (예: 2713)
        if len(seg) >= 2:
            cands.append((seg[0] + seg[1]).zfill(4))
    except Exception:
        pass
    # 중복 제거(순서 유지)
    seen = set()
    out: List[str] = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _calculate_sector_trends(date_str: str) -> Dict[str, float]:
    """
    업종(지수)별 MA5 > MA20 여부로 0/1 점수를 계산해 섹터 트렌드 맵을 만든다.
    - 캐시 키: CACHE_PREFIX_SECTOR_TRENDS, date_str
    - 반환: {"IDX_27-13-0": 1.0, ...} (스코어링의 Sector 키와 동일 포맷)
    주의: 커버리지가 0이면(전 업종 조회 실패) 캐시에 저장하지 않아 다음 실행에서 재시도한다.
    """
    cached = cache_load(CACHE_PREFIX_SECTOR_TRENDS, date_str)
    if isinstance(cached, dict) and cached:
        logger.info("섹터 트렌드 캐시 사용: %s_%s.pkl (%d개)", CACHE_PREFIX_SECTOR_TRENDS, date_str, len(cached))
        return cached

    logger.info("섹터 트렌드(KIS) 분석 시작...")
    sector_trends: Dict[str, float] = {}
    total_keys = 0
    try:
        kis = _KIS_INSTANCE
        if kis is None:
            return {}

        # 마스터에서 업종(소분류) 코드를 최대한 수집(형식: IDX_big-mid-small)
        m = load_kis_master("KOSPI", cache_key=date_str, force_refresh=False)
        if m is None or m.empty or "Sector" not in m.columns:
            return {}
        sector_keys = m["Sector"].dropna().astype(str).unique().tolist()
        sector_keys = [s for s in sector_keys if s and s.upper() not in {"N/A", "NA", "NAN"}]
        # 너무 많은 호출 방지: 설정으로 상한
        cap = int(get_cfg().get("screener_params", {}).get("sector_trend_max_sectors", 80))
        sector_keys = sector_keys[:cap]
        total_keys = len(sector_keys)

        end_date = datetime.strptime(date_str, "%Y%m%d")
        start_date = (end_date - timedelta(days=60)).strftime("%Y%m%d")

        def _fetch_sector_close(code: str) -> Optional[pd.Series]:
            df_idx = _kis_industry_price_safe(kis, code, start_date, date_str, retries=2)
            if df_idx is None or df_idx.empty or len(df_idx) < 20:
                return None
            close = _index_close_from_df(df_idx)
            return close if (close is not None and len(close) >= 20) else None

        for sk in sector_keys:
            close = None
            used_code = None
            for cand in _sector_code_candidates(sk):
                close = _fetch_sector_close(cand)
                if close is not None:
                    used_code = cand
                    break
            if close is None:
                logger.debug("[섹터트렌드] %s: 유효 업종지수 코드 없음(스킵)", sk)
                continue
            ma5 = close.rolling(5).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            score = 1.0 if (pd.notna(ma5) and pd.notna(ma20) and ma5 > ma20) else 0.0
            sector_trends[str(sk)] = float(score)
            logger.debug("[섹터트렌드] %s(code=%s): MA5=%.1f MA20=%.1f → %.0f", sk, used_code, ma5, ma20, score)

        cover = (len(sector_trends) / total_keys * 100.0) if total_keys else 0.0
        up_cnt = sum(1 for v in sector_trends.values() if v >= 0.5)
        logger.info(
            "✅ 섹터 트렌드(KIS) 완료: %d/%d개 (커버리지 %.1f%%, 상승=%d/하락=%d)",
            len(sector_trends), total_keys, cover, up_cnt, len(sector_trends) - up_cnt,
        )
    except Exception as e:
        logger.error("섹터 트렌드(KIS) 오류: %s", str(e))
        sector_trends = {}

    # 커버리지 0이면 캐시하지 않음(빈 결과 고착 방지 → 다음 실행 재시도)
    if sector_trends:
        cache_save(CACHE_PREFIX_SECTOR_TRENDS, date_str, sector_trends)
    else:
        logger.warning("섹터 트렌드 커버리지 0%% → 캐시 저장 생략(다음 실행 재시도). SectorScore는 중립(0.5)으로 처리됨.")
    return sector_trends


def _validate_dataframe_integrity(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """데이터프레임 무결성 검증 및 중복 제거"""
    if df is None or df.empty:
        logger.warning(f"{name}: 빈 데이터프레임")
        return df
    
    original_len = len(df)
    
    # 중복 인덱스 검증
    if df.index.duplicated().any():
        dup_count = df.index.duplicated().sum()
        logger.warning(f"{name}: 중복 인덱스 {dup_count}개 발견, 첫 번째 값 유지")
        df = df[~df.index.duplicated(keep='first')]
        logger.info(f"{name}: 중복 제거 후 {original_len} → {len(df)} 행")
    
    # NaN 인덱스 검증
    if df.index.isna().any():
        nan_count = df.index.isna().sum()
        logger.warning(f"{name}: NaN 인덱스 {nan_count}개 발견, 제거")
        df = df[df.index.notna()]
        logger.info(f"{name}: NaN 인덱스 제거 후 {len(df)} 행")
    
    return df

def _apply_sector_source_order(
    df_base: pd.DataFrame,
    order: List[str],
    kis: KIS,
    workers: int,
    date_str: str,
    market: str,
) -> pd.DataFrame:
    # 입력 데이터 무결성 검증
    df = _validate_dataframe_integrity(df_base.copy(), "섹터 보강 입력")
    if "Sector" not in df.columns:
        df["Sector"] = np.nan
    df["Sector"] = df["Sector"].astype("object")
    if "SectorSource" not in df.columns:
        df["SectorSource"] = pd.Series(index=df.index, dtype="object")

    # 1) mst(마스터) 기반 섹터는 get_stock_listing 단계에서 이미 채워짐
    # 그래도 결측이 있으면 마스터를 다시 조인하여 보강
    try:
        master = load_kis_master((market or "KOSPI").upper(), cache_key=date_str, force_refresh=False)
        if master is not None and not master.empty and "Sector" in master.columns:
            missing_idx = df.index[df["Sector"].isna() | df["Sector"].eq("N/A")]
            if len(missing_idx) > 0:
                sec = master.loc[missing_idx, "Sector"] if all(i in master.index for i in missing_idx) else master["Sector"]
                df.loc[missing_idx, "Sector"] = df.loc[missing_idx].index.to_series().map(master["Sector"])
                df.loc[missing_idx, "SectorSource"] = np.where(df.loc[missing_idx, "Sector"].notna(), "mst", df.loc[missing_idx, "SectorSource"])
    except Exception:
        pass

    # 2) 여전히 결측이면 KIS 실시간(inquire_price)로 최소 보강 (대상만)
    missing_idx = df.index[df["Sector"].isna() | df["Sector"].eq("N/A")]
    if len(missing_idx) > 0 and kis is not None:
        logger.info("섹터 보강(KIS) 대상: %d 종목", len(missing_idx))
        kis_df = _enrich_sector_with_kis_api(
            df.loc[missing_idx].copy(), kis, workers, cache_key=date_str, market=market
        )
        if kis_df is not None and not kis_df.empty and "Sector" in kis_df.columns:
            common_idx = missing_idx.intersection(kis_df.index)
            if len(common_idx) > 0:
                df.loc[common_idx, "Sector"] = kis_df.loc[common_idx, "Sector"]
                df.loc[common_idx, "SectorSource"] = np.where(kis_df.loc[common_idx, "Sector"].notna(), "kis", df.loc[common_idx, "SectorSource"])

    df["Sector"] = df["Sector"].map(_normalize_sector_name).fillna("N/A").astype("object")
    _log_sector_summary(df, "섹터 최종(mst/kis)")
    return df

def _resolve_business_date(date_str: str, market: str) -> str:
    """
    기준일 보정: pykrx 의존 제거.
    - 휴장일이면 is_market_open_day 기준으로 직전 거래일로 보정
    - 데이터 유효성(펀더멘털/시총/지수) 체크는 KIS 호출을 늘리므로 여기서는 하지 않는다.
    """
    try:
        dt = datetime.strptime(date_str, "%Y%m%d").date()
    except Exception:
        return date_str

    if is_market_open_day(dt, market):
        return date_str

    for i in range(1, 15):
        prev = dt - timedelta(days=i)
        if is_market_open_day(prev, market):
            d = prev.strftime("%Y%m%d")
            logger.info("휴장일 감지 → 기준일 보정: %s → %s", date_str, d)
            return d
    return date_str

def _safe_concat_mean(series_list: List[pd.Series]) -> pd.Series:
    """중복 인덱스/형식 불일치에 강한 평균 집계기."""
    if not series_list:
        return pd.Series(dtype="float64")
    cleaned = []
    for s in series_list:
        s = pd.to_numeric(s, errors="coerce")
        # 중복 인덱스는 평균으로 축약
        if not s.index.is_unique:
            s = s.groupby(level=0).mean()
        cleaned.append(s)
    # 가능한 한 빠르게 outer align
    try:
        df = pd.concat(cleaned, axis=1, join="outer", sort=False, copy=False)
    except ValueError:
        # 마지막 방어: 인덱스 합집합으로 수동 정렬 후 concat
        idx = cleaned[0].index
        for s in cleaned[1:]:
            idx = idx.union(s.index)
        aligned = [s.reindex(idx) for s in cleaned]
        df = pd.concat(aligned, axis=1, join="outer", sort=False, copy=False)
    return df.mean(axis=1)

def _get_trading_value_5d_avg(
    date_str: str,
    market: str,
    *,
    tickers: Optional[List[str]] = None,
    kis: Optional[KIS] = None,
) -> pd.Series:
    """
    거래대금 5일 평균(Amount5D)을 KIS 기간별 시세로 계산.
    - 전체 종목을 긁지 않고, tickers(관심종목)에 대해서만 호출.
    - 기준일(date_str, 당일)은 장중 누적 거래대금이라 제외하고 직전 5거래일만 사용.
    - 반환: index=Code (KR 6자리 / US 심볼), name="Amount5D"
    """
    if not tickers:
        return pd.Series(dtype="float64", name="Amount5D")
    kis = kis or _KIS_INSTANCE
    if kis is None:
        return pd.Series(dtype="float64", name="Amount5D")

    try:
        datetime.strptime(date_str, "%Y%m%d")
    except Exception:
        return pd.Series(dtype="float64", name="Amount5D")

    us_mode = is_us_market(market)
    start_dt = (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=30)).strftime("%Y%m%d")
    uniq = [norm_ticker(t, market) for t in pd.unique(pd.Series(tickers)) if t]

    debug_limit = int(os.getenv("SCREENER_AMOUNT5D_DEBUG_LIMIT", "30"))
    debug_entries: List[Dict[str, Any]] = []
    debug_lock = threading.Lock()

    def _avg_from_df(code: str, df: pd.DataFrame) -> float:
        if df is None or df.empty:
            return 0.0
        date_candidates = ["xymd", "stck_bsop_date", "bsop_date", "date", "Date"]
        tv_candidates = ["tamt", "acml_tr_pbmn", "acml_tr_pbmn_amt", "stck_tr_pbmn", "trade_value", "거래대금"]
        date_col = next((c for c in date_candidates if c in df.columns), None)
        tv_col = next((c for c in tv_candidates if c in df.columns), None)
        if tv_col is None:
            with debug_lock:
                if len(debug_entries) < debug_limit:
                    debug_entries.append({"ticker": code, "stage": "tv_col_not_found", "cols": list(df.columns)})
            return 0.0
        if date_col:
            try:
                tmp = df[[date_col, tv_col]].copy()
                tmp[date_col] = tmp[date_col].astype(str).str.replace(r"[^0-9]", "", regex=True)
                tmp[tv_col] = pd.to_numeric(tmp[tv_col], errors="coerce")
                tmp = tmp.dropna(subset=[tv_col]).sort_values(date_col)
                tmp = tmp[tmp[date_col] != str(date_str)]
                s2 = tmp[tv_col].tail(5)
                return float(s2.mean()) if len(s2) else 0.0
            except Exception:
                pass
        s = pd.to_numeric(df[tv_col], errors="coerce").dropna()
        return float(s.tail(5).mean()) if len(s) else 0.0

    def _one(code: str) -> Tuple[str, float]:
        if us_mode:
            df = _kis_overseas_daily_price_safe(kis, code, date_str, market, retries=4)
        else:
            df = _kis_period_price_safe(
                kis, code, start_dt, date_str, market_div="J", market=market, retries=3
            )
        if df is None or df.empty:
            with debug_lock:
                if len(debug_entries) < debug_limit:
                    debug_entries.append({"ticker": code, "stage": "period_price_empty_df", "cols": []})
            return code, 0.0
        return code, _avg_from_df(code, df)

    results: Dict[str, float] = {}
    default_workers = "1" if us_mode else "4"
    workers = max(1, min(int(os.getenv("KIS_SCREEN_WORKERS", default_workers)), _KIS_MAX_CONCURRENCY))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_one, c): c for c in uniq}
        for fut in as_completed(futs):
            code, val = fut.result()
            results[norm_ticker(code, market)] = val

    out = pd.Series(results, name="Amount5D", dtype="float64")
    if us_mode:
        out.index = norm_ticker_series(out.index, market)
    else:
        out.index = out.index.astype(str).str.replace(r"[^0-9]", "", regex=True).str.zfill(6)

    # 0이 너무 많이 나오는지 요약 로그 + 디버그 샘플 저장
    try:
        zeros = int((out == 0.0).sum())
        nonzeros = len(out) - zeros
        logger.info("[Amount5D] zeros=%d nonzeros=%d total=%d", zeros, nonzeros, len(out))
        if debug_entries:
            debug_dir = OUTPUT_DIR / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            dbg_path = debug_dir / f"amount5d_debug_{market}_{date_str}.json"
            with open(dbg_path, "w", encoding="utf-8") as f:
                json.dump(debug_entries, f, ensure_ascii=False, indent=2)
            logger.info("[Amount5D] debug 샘플 저장: %s", str(dbg_path))
    except Exception:
        pass
    return out

def _index_close_from_df(df: pd.DataFrame) -> Optional[pd.Series]:
    """KIS 업종/지수 일자별 응답에서 종가 시계열을 '오름차순(과거→현재)'으로 반환.
    KIS는 보통 최신일자가 먼저 오는 내림차순으로 응답하므로, 이동평균의 마지막 값이
    최신이 되도록 날짜 기준 정렬한다.
    """
    if df is None or df.empty:
        return None
    close_col = None
    for c in ["bstp_nmix_prpr", "stck_clpr", "clspr", "close", "Close", "종가"]:
        if c in df.columns:
            close_col = c
            break
    if close_col is None:
        return None
    d = df.copy()
    for dc in ["stck_bsop_date", "bsop_date", "date", "Date"]:
        if dc in d.columns:
            try:
                d = d.sort_values(dc)
            except Exception:
                pass
            break
    close = pd.to_numeric(d[close_col], errors="coerce").dropna()
    return close if len(close) else None


# KIS 업종지수 일봉 누적 종가 캐시: (idx_code, end_date) -> 오름차순 종가 Series
_INDUSTRY_CLOSE_CACHE: Dict[Tuple[str, str], Optional[pd.Series]] = {}


def _kis_industry_close_history(
    kis: KIS,
    idx_code: str,
    end_date: str,
    *,
    min_bars: int = 260,
    max_pages: int = 6,
    retries: int = 3,
) -> Optional[pd.Series]:
    """KIS 업종지수 일봉 종가를 여러 페이지로 누적해 '오름차순(과거→현재)' Series로 반환.

    KIS 기간별시세(FHKUP03500100)는 1회 응답이 ~100봉으로 제한된다. MA200을 산출하려면
    200봉 이상이 필요하므로, 조회 종료일을 과거로 밀며 여러 번 호출해 min_bars 이상을 모은다.
    (인덱스는 YYYYMMDD 문자열 → 사전식 정렬이 곧 시간순 정렬)
    """
    idx_code = str(idx_code).strip()
    end_date = str(end_date)
    cache_key = (idx_code, end_date)
    if cache_key in _INDUSTRY_CLOSE_CACHE:
        return _INDUSTRY_CLOSE_CACHE[cache_key]

    merged: Dict[str, float] = {}
    cur_end = end_date
    for _ in range(max(1, max_pages)):
        start = (datetime.strptime(cur_end, "%Y%m%d") - timedelta(days=200)).strftime("%Y%m%d")
        df = _kis_industry_price_safe(kis, idx_code, start, cur_end, retries=retries)
        if df is None or df.empty:
            break
        date_col = next((c for c in ["stck_bsop_date", "bsop_date", "date", "Date"] if c in df.columns), None)
        close_col = next((c for c in ["bstp_nmix_prpr", "stck_clpr", "clspr", "close", "Close", "종가"] if c in df.columns), None)
        if close_col is None:
            break
        if date_col is None:
            # 날짜가 없으면 페이지네이션 불가 → 단일 페이지 종가로 대체
            single = _index_close_from_df(df)
            _INDUSTRY_CLOSE_CACHE[cache_key] = single
            return single

        prev_n = len(merged)
        dates = df[date_col].astype(str)
        vals = pd.to_numeric(df[close_col], errors="coerce")
        for d, c in zip(dates, vals):
            if d and pd.notna(c):
                merged[d] = float(c)

        if len(merged) >= min_bars:
            break
        if len(merged) <= prev_n:
            break  # 새 데이터 없음(중복만 수신) → 종료
        earliest = min(merged.keys())
        try:
            next_end = (datetime.strptime(earliest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
        except Exception:
            break
        if next_end >= cur_end:
            break
        cur_end = next_end

    result = pd.Series(merged).sort_index() if merged else None
    if result is not None and not len(result):
        result = None
    _INDUSTRY_CLOSE_CACHE[cache_key] = result
    if result is not None:
        logger.debug("[레짐] KIS 업종지수 %s 누적 종가 %d봉 확보", idx_code, len(result))
    return result


def _get_market_regime_score(date_str: str, market: str) -> float:
    try:
        if is_us_market(market):
            close = _get_us_benchmark_close(date_str, min_bars=200)
            if close is None or len(close) < 60:
                return 0.5
            ma50 = close.rolling(50).mean().iloc[-1]
            ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
            rsi_val = calculate_rsi(close)
            rsi = rsi_val.iloc[-1] if isinstance(rsi_val, pd.Series) and len(rsi_val) else float(rsi_val) if rsi_val is not None else np.nan
            if pd.isna(ma50) or pd.isna(rsi):
                return 0.5
            above_ma50 = 1 if close.iloc[-1] > ma50 else 0
            ma_term = 0.5 if pd.isna(ma200) else (1 if ma50 > ma200 else 0)
            rsi_term = max(0.0, 1 - abs(rsi - 50) / 50)
            return float((above_ma50 + ma_term + rsi_term) / 3.0)

        kis = _KIS_INSTANCE
        if kis is None:
            return 0.5
        idx_code = "0001" if (market or "").upper() == "KOSPI" else "1001"
        # 200봉+를 페이지네이션으로 누적해 MA200(골든크로스)까지 산출한다.
        close = _kis_industry_close_history(kis, idx_code, date_str, min_bars=260)
        if close is None or len(close) < 60:
            return 0.5
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
        rsi_val = calculate_rsi(close)
        rsi = rsi_val.iloc[-1] if isinstance(rsi_val, pd.Series) and len(rsi_val) else float(rsi_val) if rsi_val is not None else np.nan
        if pd.isna(ma50) or pd.isna(rsi):
            return 0.5
        above_ma50 = 1 if close.iloc[-1] > ma50 else 0
        ma_term = 0.5 if pd.isna(ma200) else (1 if ma50 > ma200 else 0)
        rsi_term = max(0.0, 1 - abs(rsi - 50) / 50)
        score = (above_ma50 + ma_term + rsi_term) / 3.0
        return float(score)
    except Exception:
        return 0.5

def _get_market_regime_components(date_str: str, market: str) -> Dict[str, float]:
    try:
        if is_us_market(market):
            close = _get_us_benchmark_close(date_str, min_bars=200)
            if close is None or len(close) < 60:
                return {"above_ma50": 0.5, "ma50_gt_ma200": 0.5, "rsi_term": 0.5}
            ma50 = close.rolling(50).mean().iloc[-1]
            ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
            rsi_val = calculate_rsi(close)
            rsi = rsi_val.iloc[-1] if isinstance(rsi_val, pd.Series) and len(rsi_val) else float(rsi_val) if rsi_val is not None else np.nan
            return {
                "above_ma50": 1.0 if (not pd.isna(ma50) and close.iloc[-1] > ma50) else 0.0,
                "ma50_gt_ma200": 0.5 if pd.isna(ma200) else (1.0 if ma50 > ma200 else 0.0),
                "rsi_term": max(0.0, 1 - abs(rsi - 50) / 50) if not pd.isna(rsi) else 0.5,
                "benchmark": us_regime_benchmark(market) or "SPY",
            }

        kis = _KIS_INSTANCE
        if kis is None:
            return {"above_ma50": 0.5, "ma50_gt_ma200": 0.5, "rsi_term": 0.5}
        idx_code = "0001" if (market or "").upper() == "KOSPI" else "1001"
        # 점수 함수와 동일하게 200봉+를 페이지네이션으로 누적(캐시 공유).
        close = _kis_industry_close_history(kis, idx_code, date_str, min_bars=260)
        if close is None or len(close) < 60:
            return {"above_ma50": 0.5, "ma50_gt_ma200": 0.5, "rsi_term": 0.5}
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
        rsi_val = calculate_rsi(close)
        rsi = rsi_val.iloc[-1] if isinstance(rsi_val, pd.Series) and len(rsi_val) else float(rsi_val) if rsi_val is not None else np.nan
        return {
            "above_ma50": 1.0 if (not pd.isna(ma50) and close.iloc[-1] > ma50) else 0.0,
            "ma50_gt_ma200": 0.5 if pd.isna(ma200) else (1.0 if ma50 > ma200 else 0.0),
            "rsi_term": max(0.0, 1 - abs(rsi - 50) / 50) if not pd.isna(rsi) else 0.5,
        }
    except Exception:
        return {"above_ma50": 0.5, "ma50_gt_ma200": 0.5, "rsi_term": 0.5}

# ─────────── 보유 종목 스코어 업데이트 ───────────
def get_holdings_from_balance() -> List[Dict[str, Any]]:
    """balance 파일에서 보유 종목 정보를 읽어옵니다."""
    try:
        balance_file = find_latest_file("balance_*.json")
        if not balance_file:
            logger.warning("balance 파일을 찾을 수 없습니다.")
            return []
        
        with open(balance_file, "r", encoding="utf-8") as f:
            balance_data = json.load(f)
        
        holdings = []
        if "data" in balance_data:
            for item in balance_data["data"]:
                hldg_qty = int(item.get("hldg_qty", 0))
                if hldg_qty > 0:  # 보유 수량이 있는 것만
                    holdings.append({
                        "pdno": item.get("pdno", ""),
                        "prdt_name": item.get("prdt_name", ""),
                        "hldg_qty": str(hldg_qty),
                        "prpr": item.get("prpr", "0"),
                        "pchs_avg_pric": item.get("pchs_avg_pric", "0"),
                        "evlu_amt": item.get("evlu_amt", "0"),
                        "evlu_pfls_amt": item.get("evlu_pfls_amt", "0")
                    })
        
        logger.info("보유 종목 %d개 로드 완료 (balance 파일)", len(holdings))
        return holdings
    except Exception as e:
        logger.error("보유 종목 로드 실패: %s", str(e))
        return []

def update_holdings_scores(holdings: List[Dict[str, Any]], date_str: str, market: str, 
                          screener_params: Dict[str, Any], market_score: float, 
                          sector_trends: Dict[str, float], risk_params: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """보유 종목들의 최신 스코어를 계산합니다."""
    if not holdings:
        return {}
    
    logger.info("보유 종목 %d개 스코어 업데이트 시작", len(holdings))
    
    # 보유 종목 정보를 DataFrame으로 변환
    holdings_data = []
    for holding in holdings:
        ticker = norm_ticker(holding.get("pdno", ""), market)
        name = holding.get("prdt_name", "")
        price = _to_float(holding.get("prpr", 0))
        
        holdings_data.append({
            "Ticker": ticker,
            "Name": name,
            "Price": price,
            "Sector": "N/A",  # 기본값, 나중에 업데이트
            "SectorSource": "unknown"
        })
    
    if not holdings_data:
        return {}
    
    df_holdings = pd.DataFrame(holdings_data).set_index("Ticker")
    
    # 섹터 정보 보강 (간단한 버전)
    try:
        # KIS API를 사용한 섹터 보강
        kis = _KIS_INSTANCE
        if kis:
            logger.info("보유 종목 섹터 정보 보강 중...")
            df_holdings = _enrich_sector_with_kis_api(
                df_holdings, kis, workers=2, cache_key=date_str, market=market
            )
    except Exception as e:
        logger.debug("보유 종목 섹터 보강 실패: %s", str(e))
    
    # 각 보유 종목의 스코어 계산 (보유 종목용 - 신규상장 제외 규칙 비활성화)
    holdings_scores = {}
    for ticker, row in df_holdings.iterrows():
        try:
            # 보유 종목용 스코어 계산 (신규상장 제외 규칙 비활성화)
            score_data = _calculate_scores_for_holdings_ticker(
                ticker,
                date_str,
                row,
                screener_params,
                market_score,
                sector_trends,
                risk_params
            )
            
            # 계산 실패 시에도 기본 정보는 저장
            if not score_data:
                logger.warning(f"보유 종목 {ticker} 스코어 계산 실패, 기본값으로 저장")
                holdings_scores[ticker] = {
                    "ticker": ticker,
                    "name": row.get("Name", ""),
                    "sector": row.get("Sector", "N/A"),
                    "price": row.get("Price", 0),
                    "score": 0.0,
                    "fin_score": 0.0,
                    "tech_score": 0.0,
                    "mkt_score": 0.0,
                    "sector_score": 0.0,
                    "vol_kki": 0.0,
                    "pos_52w": 0.0,
                    "per": 20.0,
                    "pbr": 1.5,
                    "rsi": 50.0,
                    "atr": row.get("Price", 0) * 0.02 if row.get("Price", 0) > 0 else 100.0,
                    "ma50": row.get("Price", 0),
                    "ma200": row.get("Price", 0),
                    "updated_at": date_str
                }
                continue
            
            # 성공적으로 계산된 경우
            holdings_scores[ticker] = {
                "ticker": ticker,
                "name": score_data.get("Name", ""),
                "sector": score_data.get("Sector", "N/A"),
                "price": score_data.get("Price", 0),
                "score": score_data.get("Score", 0.0),
                "fin_score": score_data.get("FinScore", 0.0),
                "tech_score": score_data.get("TechScore", 0.0),
                "mkt_score": score_data.get("MktScore", 0.0),
                "sector_score": score_data.get("SectorScore", 0.0),
                "vol_kki": score_data.get("VolKki", 0.0),
                "pos_52w": score_data.get("Pos52w", 0.0),
                "per": score_data.get("PER"),
                "pbr": score_data.get("PBR"),
                "rsi": score_data.get("RSI"),
                "atr": score_data.get("ATR"),
                "ma50": score_data.get("MA50"),
                "ma200": score_data.get("MA200"),
                "updated_at": date_str
            }
        except Exception as e:
            logger.error("보유 종목 스코어 계산 실패 (%s): %s", ticker, str(e))
            holdings_scores[ticker] = {
                "ticker": ticker,
                "name": row.get("Name", ""),
                "sector": row.get("Sector", "N/A"),
                "price": row.get("Price", 0),
                "score": 0.0,
                "fin_score": 0.0,
                "tech_score": 0.0,
                "mkt_score": 0.0,
                "sector_score": 0.0,
                "vol_kki": 0.0,
                "pos_52w": 0.0,
                "per": None,
                "pbr": None,
                "rsi": None,
                "atr": None,
                "ma50": None,
                "ma200": None,
                "updated_at": date_str
            }
    
    logger.info("보유 종목 스코어 업데이트 완료: %d개", len(holdings_scores))
    return holdings_scores

def get_holdings_scores_from_file(date_str: str, market: str) -> Dict[str, Dict[str, Any]]:
    """보유 종목 스코어 파일에서 데이터를 읽어옵니다."""
    try:
        holdings_file = OUTPUT_DIR / f"screener_holdings_{date_str}_{market}.json"
        if not holdings_file.exists():
            logger.debug("보유 종목 스코어 파일이 없습니다: %s", holdings_file)
            return {}
        
        with open(holdings_file, "r", encoding="utf-8") as f:
            holdings_list = json.load(f)
        
        # 리스트를 딕셔너리로 변환 (ticker를 키로)
        holdings_scores = {}
        for holding in holdings_list:
            ticker = holding.get("ticker", "")
            if ticker:
                holdings_scores[ticker] = holding
        
        logger.info("보유 종목 스코어 로드 완료: %d개", len(holdings_scores))
        return holdings_scores
    except Exception as e:
        logger.error("보유 종목 스코어 로드 실패: %s", str(e))
        return {}

# ─────────── 투자자별 수급 데이터 조회 ───────────
def get_investor_flow(ticker: str, date_str: str, days_lookback: int = 10) -> Optional[pd.DataFrame]:
    """지정된 기간 동안의 투자자별 거래대금(기관, 외국인 등)을 조회합니다."""
    try:
        kis = _KIS_INSTANCE
        if kis is None:
            return None
        end_date = datetime.strptime(date_str, "%Y%m%d")
        start_date = (end_date - timedelta(days=days_lookback * 3)).strftime("%Y%m%d")  # 주말 포함 여유
        df_flow = _kis_investor_trend_safe(kis, str(ticker).zfill(6), start_date, date_str, market_div="J", retries=3)
        if df_flow is None or df_flow.empty:
            return None

        # 컬럼 후보(문서/응답 차이 대비)
        # 금액/수량 모두 가능하지만, 기존은 "대금" 기반이므로 금액 우선
        inst_cols = ["inst_tot_amt", "orgn_tot_amt", "기관합계대금", "기관합계", "inst_amt", "기관"]
        frgn_cols = ["frgn_tot_amt", "frgn_tot_amt", "외국인합계대금", "외국인합계", "frgn_amt", "외국인"]

        def _pick(cols: List[str]) -> Optional[str]:
            for c in cols:
                if c in df_flow.columns:
                    return c
            return None

        c_inst = _pick(inst_cols)
        c_frgn = _pick(frgn_cols)
        if not c_inst or not c_frgn:
            return None

        out = df_flow.rename(columns={c_inst: "기관합계", c_frgn: "외국인합계"})
        out = out[["기관합계", "외국인합계"]].copy()
        out["기관합계"] = pd.to_numeric(out["기관합계"], errors="coerce").fillna(0)
        out["외국인합계"] = pd.to_numeric(out["외국인합계"], errors="coerce").fillna(0)
        return out.tail(days_lookback)
    except Exception as e:
        logger.debug("[%s] 투자자별 수급 조회 실패: %s", ticker, str(e))
    return None

# ─────────── FDR Marcap 비정상 시 PYKRX 시총 폴백 ───────────
def _get_marcap_series_from_pykrx(date_str: str, market: str) -> pd.Series:
    """(Deprecated) pykrx 제거로 인해 빈 Series 반환."""
    return pd.Series(dtype="float64", name="Marcap")

def _filter_initial_stocks(
    date_str: str,
    cfg: Dict[str, Any],
    market: str,
    risk: Dict[str, Any],
    debug: bool,
) -> Tuple[pd.DataFrame, str]:
    logger.info("1차 필터링 시작...")
    fixed_date = _resolve_business_date(date_str, market)

    # 종목 기본 목록(KIS master)
    df_all = get_stock_listing(market)
    if df_all is None or df_all.empty:
        logger.error("종목 마스터가 비어 있어 1차 필터링을 중단합니다.")
        return pd.DataFrame(), fixed_date

    # Name/Marcap 보정
    if "Name" not in df_all.columns:
        df_all = df_all.copy()
        df_all["Name"] = df_all.index.astype(str)
    if "Marcap" not in df_all.columns:
        df_all = df_all.copy()
        close = pd.to_numeric(df_all.get("Close", 0), errors="coerce").fillna(0)
        shares = pd.to_numeric(df_all.get("ListedShares", 0), errors="coerce").fillna(0)
        df_all["Marcap"] = close * shares

    # 1) 마켓캡 기반 1차 필터(빠름)
    df_pre = df_all[
        [c for c in ["Name", "Marcap", "Sector", "SectorSource", "EXCD", "OvrsExcg"] if c in df_all.columns]
    ].copy()
    df_pre["Marcap"] = pd.to_numeric(df_pre["Marcap"], errors="coerce").fillna(0)

    if debug:
        (OUTPUT_DIR / "debug").mkdir(exist_ok=True, parents=True)
        df_pre.to_csv(OUTPUT_DIR / f"debug/debug_joined_{market}_{fixed_date}.csv")

    _describe_series("Marcap", df_pre["Marcap"])

    # 필터링
    if is_us_market(market):
        min_mc = float(cfg.get("min_market_cap_us", 0))
        max_mc = float(cfg.get("max_market_cap_us", cfg.get("max_market_cap", 1e15)))
    else:
        min_mc = float(cfg.get("min_market_cap", 0))
        max_mc = float(cfg.get("max_market_cap", 1e13))
    min_amt = min_trading_value_5d_avg(cfg, market)
    if is_us_market(market) and float(df_pre["Marcap"].max() or 0) <= 0:
        mask_mc = pd.Series(True, index=df_pre.index)
        logger.info("US Marcap 미제공(마스터) → 1차 시총 필터 스킵")
    else:
        mask_mc = (df_pre["Marcap"] >= min_mc) & (df_pre["Marcap"] <= max_mc)
    n0 = len(df_pre)
    n1 = int(mask_mc.sum())
    logger.info("단계별 생존 수: 시작=%d → Marcap(≥%s, ≤%s)=%d", n0, f"{int(min_mc):,}", f"{int(max_mc):,}", n1)
    # 디버깅: Marcap 필터로 제외된 종목 (하한/상한 사유 분리)
    logger.debug(
        "[1차:Marcap] 하한미달=%d건, 상한초과=%d건",
        int((df_pre["Marcap"] < min_mc).sum()),
        int((df_pre["Marcap"] > max_mc).sum()),
    )
    df_mc = df_pre[mask_mc].copy()
    _log_dropped("1차:Marcap", df_pre.index, df_mc.index)
    if df_mc.empty:
        logger.warning("Marcap 필터 후 종목이 없습니다.")
        return pd.DataFrame(), fixed_date

    # 2) 거래대금(5D avg): 관심종목(마켓캡 통과)만 KIS 기간별 시세로 계산
    amt5 = _get_trading_value_5d_avg(fixed_date, market, tickers=df_mc.index.tolist(), kis=_KIS_INSTANCE)
    df_mc = df_mc.join(amt5, how="left")
    amt_num = pd.to_numeric(df_mc.get("Amount5D", pd.Series(index=df_mc.index, dtype="float64")), errors="coerce").fillna(0)
    mask_amt = amt_num >= min_amt
    n2 = int(mask_amt.sum())
    logger.info("단계별 생존 수: +Amount5D(≥%s)=%d (대상=%d)", f"{int(min_amt):,}", n2, len(df_mc))
    # 디버깅: Amount5D 분포 + 제외 종목
    _describe_series("Amount5D", amt_num)
    logger.debug("[1차:Amount5D] 거래대금 미달=%d건", int((amt_num < min_amt).sum()))
    df_filtered = df_mc[mask_amt].copy()
    _log_dropped("1차:Amount5D", df_mc.index, df_filtered.index)
    if df_filtered.empty:
        logger.warning("거래대금(5D) 필터 후 종목이 없습니다.")
        return pd.DataFrame(), fixed_date

    # 3) 펀더멘털(PER/PBR): 최종 관심종목만 inquire_price로 보강
    fundamentals = get_fundamentals(fixed_date, market, tickers=df_filtered.index.tolist(), kis=_KIS_INSTANCE)
    if fundamentals is not None and not fundamentals.empty:
        df_filtered = df_filtered.join(fundamentals[["PER", "PBR"]], how="left")
    else:
        df_filtered["PER"] = np.nan
        df_filtered["PBR"] = np.nan

    n_fund = len(df_filtered)

    # 화이트/블랙리스트
    bl = {norm_ticker(x, market) for x in risk.get("blacklist_tickers", []) if x}
    wl = {norm_ticker(x, market) for x in risk.get("whitelist_tickers", []) if x}
    if wl:
        before = len(df_filtered)
        _before_idx = df_filtered.index
        df_filtered = df_filtered[df_filtered.index.isin(wl)]
        logger.info("화이트리스트 적용: %d → %d", before, len(df_filtered))
        _log_dropped("1차:화이트리스트", _before_idx, df_filtered.index)
    if bl:
        before = len(df_filtered)
        _before_idx = df_filtered.index
        df_filtered = df_filtered[~df_filtered.index.isin(bl)]
        logger.info("블랙리스트 적용: %d → %d", before, len(df_filtered))
        _log_dropped("1차:블랙리스트", _before_idx, df_filtered.index)
    n_list = len(df_filtered)

    # 빈/무효 티커 제거 (KR: 4자 이상 / US: 1자 이상)
    _min_len = 1 if is_us_market(market) else 4
    valid_idx = df_filtered.index.notna() & (df_filtered.index.astype(str).str.strip().str.len() >= _min_len)
    if not valid_idx.all():
        dropped = int((~valid_idx).sum())
        _before_idx = df_filtered.index
        df_filtered = df_filtered[valid_idx]
        logger.info("1차 필터링 후 무효 티커 제외: %d건 → %d 종목", dropped, len(df_filtered))
        _log_dropped("1차:무효티커", _before_idx, df_filtered.index)

    # 단계별 생존 종목 수 퍼널 요약
    _log_funnel(
        "1차 필터링",
        [
            ("전체 종목", n0),
            (f"Marcap(≥{int(min_mc):,})", n1),
            (f"Amount5D(≥{int(min_amt):,})", n2),
            ("펀더멘털 보강", n_fund),
            ("화이트/블랙리스트", n_list),
            ("유효 티커", len(df_filtered)),
        ],
    )
    logger.info(
        "✅ 1차 필터링 완료: %d → %d 종목 (시장=%s, 기준일=%s, min_mc=%s, min_amt5D=%s)",
        len(df_pre), len(df_filtered), market, fixed_date, f"{int(min_mc):,}", f"{int(min_amt):,}",
    )
    return df_filtered, fixed_date

def _calculate_scores_for_holdings_ticker(
    code: str,
    date_str: str,
    fin_info: pd.Series,
    cfg: Dict[str, Any],
    market_score: float,
    sector_trends: Dict[str, float],
    risk_params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """보유 종목용 스코어 계산 (신규상장 제외 규칙 비활성화)"""
    try:
        # 조기 반환 조건들
        if not code or pd.isna(code):
            return None
            
        lookback_days = int(cfg.get("history_lookback_days", 730))
        start_dt_str = (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")
        
        # 가격 데이터 조회
        df_price_raw = get_historical_prices(code, start_dt_str, date_str)
        if df_price_raw is None or df_price_raw.empty:
            return None

        # 표준화
        df_price, std_err = standardize_ohlcv(df_price_raw)
        if std_err is not None:
            return None

        # 계산 창 슬라이싱
        calc_window_days = int(cfg.get("calc_window_days", 365))
        if calc_window_days > 0 and len(df_price) > calc_window_days:
            df_price = df_price.tail(calc_window_days)

        # 신규상장 제외 규칙 비활성화 (보유 종목이므로)
        # listing_dt = _LISTING_DATES_CACHE.get(str(code).zfill(6)) or get_listing_date(code)
        # newly_days = int(cfg.get("exclude_newly_listed_days", 60))
        # if listing_dt is not None and newly_days > 0 and ...

        # ▶ 최소 봉수 체크 (완화된 기준)
        min_history_bars = int(cfg.get("min_history_bars", 50))  # 100 → 50으로 완화
        if df_price is None or len(df_price) < min_history_bars:
            return None

        # 지표 계산
        try:
            from screener_core import (
                calculate_rsi, calculate_macd, calculate_bollinger_bands, calculate_atr,
                calculate_technical_score, _compute_levels
            )
            
            # 기술적 지표 개별 계산
            prices = df_price["Close"].tolist()
            volumes = df_price["Volume"].tolist()
            
            rsi = calculate_rsi(prices)
            atr_val = calculate_atr(df_price)
            ma50 = df_price["Close"].rolling(50).mean().iloc[-1] if len(df_price) >= 50 else df_price["Close"].iloc[-1]
            ma200 = df_price["Close"].rolling(200).mean().iloc[-1] if len(df_price) >= 200 else df_price["Close"].iloc[-1]
            
            # 간단한 패턴 분석
            ma20 = df_price["Close"].rolling(20).mean().iloc[-1] if len(df_price) >= 20 else df_price["Close"].iloc[-1]
            ma20_up = df_price["Close"].iloc[-1] > ma20
            accum_vol = False  # 간단히 False로 설정
            higher_lows = False  # 간단히 False로 설정
            consolidation = False  # 간단히 False로 설정
            yey_pattern = False  # 간단히 False로 설정
            
            # 펀더멘털 점수 (간단한 계산)
            per_val = fin_info.get("PER", 20.0)
            pbr_val = fin_info.get("PBR", 1.5)
            fin_score = 0.5  # 기본값
            
            # 기술 점수
            tech_score = calculate_technical_score(code, prices, volumes)
            
            # 시장 점수 (간단한 계산)
            sector_name = fin_info.get("Sector", "N/A")
            sector_trend = sector_trends.get(sector_name, 0.0)
            mkt_score = (market_score + sector_trend) / 2
            
            # 섹터 점수 (간단한 계산)
            sector_score = max(0.0, min(1.0, sector_trend))
            
            # 패턴 점수 (간단한 계산)
            pattern_score = 0.5  # 기본값
            
            # 거래량/위치 점수 (간단한 계산)
            vol_kki = 1.0  # 기본값
            pos_52w = 0.5  # 기본값
            
            # 종합 점수 계산
            total_score = (
                fin_score * 0.3 +
                tech_score * 0.3 +
                mkt_score * 0.2 +
                sector_score * 0.1 +
                pattern_score * 0.1
            )
            
            # 시장 분석 기반 스코어 조정
            from screener_core import calculate_market_adjusted_score
            if _CURRENT_MARKET_STATE is not None:
                total_score = calculate_market_adjusted_score(total_score, _CURRENT_MARKET_STATE)
            
            # 손절/목표가 계산 (간단한 버전)
            current_price = df_price["Close"].iloc[-1]
            swing_high = df_price["High"].max()
            swing_low = df_price["Low"].min()
            stop_price = current_price * 0.95  # 5% 손절
            target_price = current_price * 1.15  # 15% 목표
            
            # 일봉 차트 데이터 (최근 30일)
            daily_chart_data = []
            for i, (_, row) in enumerate(df_price.tail(30).iterrows()):
                daily_chart_data.append({
                    "index": int(row.name.timestamp() * 1000),
                    "Open": int(row["Open"]),
                    "High": int(row["High"]),
                    "Low": int(row["Low"]),
                    "Close": int(row["Close"]),
                    "Volume": int(row["Volume"])
                })
            
            # 투자자별 매매동향 (최근 10일) - 일시적으로 비활성화
            df_investor_flow = None
            # try:
            #     from pykrx import stock
            #     df_investor_flow = stock.get_market_net_purchases_of_equities(
            #         start_dt_str, date_str, code
            #     )
            #     if df_investor_flow is not None and not df_investor_flow.empty:
            #         df_investor_flow = df_investor_flow.tail(10)
            # except Exception:
            #     pass
            
            return {
                "Ticker": code,
                "Name": str(fin_info.get("Name", "")),
                "Sector": sector_name,
                "SectorSource": str(fin_info.get("SectorSource", "unknown")),
                "Price": int(round(float(current_price))),
                "Score": round(float(total_score), 4),
                "FinScore": round(float(fin_score), 4),
                "TechScore": round(float(tech_score), 4),
                "MktScore": round(float(mkt_score), 4),
                "SectorScore": round(float(sector_score), 4),
                "VolKki": round(float(vol_kki), 4),
                "Pos52w": round(float(pos_52w), 4),
                "PatternScore": round(float(pattern_score), 4),
                "PER": round(float(per_val), 2) if pd.notna(per_val) else 20.0,
                "PBR": round(float(pbr_val), 2) if pd.notna(pbr_val) else 1.5,
                "RSI": round(float(rsi), 2) if pd.notna(rsi) else 50.0,
                "ATR": round(float(atr_val), 2) if atr_val is not None and pd.notna(atr_val) else (round(float(current_price) * 0.02, 2) if pd.notna(current_price) else 100.0),
                "MA50": round(float(ma50), 2) if pd.notna(ma50) else (round(float(current_price), 2) if pd.notna(current_price) else 0.0),
                "MA200": round(float(ma200), 2) if pd.notna(ma200) else (round(float(current_price), 2) if pd.notna(current_price) else 0.0),
                "MA20Up": bool(ma20_up),
                "AccumVol": bool(accum_vol),
                "HigherLows": bool(higher_lows),
                "Consolidation": bool(consolidation),
                "YEY": bool(yey_pattern),
                "exclude_reasons": [],
                "daily_chart": daily_chart_data,
                "investor_flow": df_investor_flow.reset_index().to_dict('records') if df_investor_flow is not None else None,
                "stop_price": int(stop_price),
                "target_price": int(target_price),
                "levels_source": "atr_based"
            }
            
        except Exception as e:
            logger.error(f"보유 종목 {code} 지표 계산 실패: {e}")
            return None
            
    except Exception as ex:
        logger.error(f"보유 종목 {code} 스코어 계산 예외: {ex}")
        return None

def _calculate_scores_for_ticker(
    code: str,
    date_str: str,
    fin_info: pd.Series,
    cfg: Dict[str, Any],
    market_score: float,
    sector_trends: Dict[str, float],
    risk_params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    try:
        # 조기 반환 조건들
        if not code or pd.isna(code):
            with _fail_lock:
                _fail_stats["invalid_code"] += 1
                _fail_rows.append({"Ticker": code, "reason": "invalid_code"})
            return None
            
        lookback_days = int(cfg.get("history_lookback_days", 730))
        start_dt_str = (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")
        
        # 가격 데이터 조회
        df_price_raw = get_historical_prices(code, start_dt_str, date_str)
        if df_price_raw is None or df_price_raw.empty:
            with _fail_lock:
                _fail_stats["no_price_data"] += 1
                _fail_rows.append({"Ticker": code, "reason": "no_price_data"})
            return None

        # 표준화
        df_price, std_err = standardize_ohlcv(df_price_raw)
        if std_err is not None:
            with _fail_lock:
                _fail_stats[std_err] += 1
                _fail_rows.append({"Ticker": code, "reason": std_err})
            return None

        # 계산 창 슬라이싱
        calc_window_days = int(cfg.get("calc_window_days", 365))
        if calc_window_days > 0 and len(df_price) > calc_window_days:
            df_price = df_price.tail(calc_window_days)

        # --- 신규상장 우선 스킵 ---
        listing_dt = _LISTING_DATES_CACHE.get(
            _screener_ticker_key(code, market)
        ) or get_listing_date(code, market)
        newly_days = int(cfg.get("exclude_newly_listed_days", 60))
        if listing_dt is not None and newly_days > 0 and is_newly_listed(listing_dt, datetime.now(), newly_days):
            with _fail_lock:
                _fail_stats["newly_listed_skip"] += 1
                _fail_rows.append({"Ticker": code, "reason": "NEWLY_LISTED"})
            return None

        # ▶ 최소 봉수 체크 (완화된 기준)
        min_history_bars = int(cfg.get("min_history_bars", 50))  # 100 → 50으로 완화
        if df_price is None or len(df_price) < min_history_bars:
            with _fail_lock:
                _fail_stats["skipped_short_history"] += 1
                _fail_rows.append({
                    "Ticker": code, "reason": "INSUFFICIENT_HISTORY",
                    "len": float(len(df_price) if df_price is not None else 0),
                })
            return None

        # 지표 계산
        try:
            close_series = df_price["Close"]
            close = close_series.iloc[-1]
            ma50 = close_series.rolling(50).mean().iloc[-1]
            ma200 = close_series.rolling(200).mean().iloc[-1]

            # RSI 계산 (더 유연한 방식)
            try:
                rsi_series = calculate_rsi(close_series.dropna())
                rsi = rsi_series.iloc[-1] if isinstance(rsi_series, pd.Series) and len(rsi_series) else (float(rsi_series) if rsi_series is not None else np.nan)
            except Exception as e:
                logger.debug(f"[{code}] RSI 계산 실패: {e}")
                rsi = np.nan

            # ATR 계산 (더 유연한 방식)
            try:
                atr_period = int((risk_params or {}).get("atr_period", 14))
                atr_val = calculate_atr(df_price, period=atr_period)
            except Exception as e:
                logger.debug(f"[{code}] ATR 계산 실패: {e}")
                atr_val = np.nan

            # MA50 체크 (완화된 기준 - 데이터가 부족하면 현재가로 대체)
            if pd.isna(ma50):
                # 데이터 길이에 따라 다른 이동평균 사용
                data_length = len(close_series)
                if data_length >= 20:
                    # 20일 이상이면 MA20 사용
                    ma50 = close_series.rolling(20).mean().iloc[-1]
                    logger.debug(f"[{code}] MA50 없음 → MA20 사용: {ma50}")
                elif data_length >= 10:
                    # 10일 이상이면 MA10 사용
                    ma50 = close_series.rolling(10).mean().iloc[-1]
                    logger.debug(f"[{code}] MA50 없음 → MA10 사용: {ma50}")
                else:
                    # 10일 미만이면 현재가로 설정
                    ma50 = close
                    logger.debug(f"[{code}] MA50 없음 → 현재가로 대체: {ma50}")
                
                # 여전히 NaN이면 현재가로 설정
                if pd.isna(ma50):
                    ma50 = close
                    logger.debug(f"[{code}] MA50 최종 대체: {ma50}")
            
            # MA200이 없을 때 대체 로직
            if pd.isna(ma200):
                # 사용 가능한 데이터 길이에 따라 다른 이동평균 사용
                data_length = len(close_series)
                if data_length >= 100:
                    # 100일 이상이면 MA100 사용
                    ma200 = close_series.rolling(100).mean().iloc[-1]
                    logger.debug(f"[{code}] MA200 없음 → MA100 사용: {ma200}")
                elif data_length >= 50:
                    # 50일 이상이면 MA50과 동일하게 설정 (단기 추세만 고려)
                    ma200 = ma50
                    logger.debug(f"[{code}] MA200 없음 → MA50으로 대체: {ma200}")
                else:
                    # 50일 미만이면 현재가로 설정 (중립)
                    ma200 = close
                    logger.debug(f"[{code}] MA200 없음 → 현재가로 대체: {ma200}")
                
            if pd.isna(rsi):
                rsi = 50.0  # RSI 기본값 설정
                
            if pd.isna(atr_val) or atr_val <= 0:
                atr_val = close * 0.02  # ATR 기본값을 현재가의 2%로 설정
                
        except Exception as e:
            with _fail_lock:
                _fail_stats["indicator_calc_error"] += 1
                _fail_rows.append({"Ticker": code, "reason": "indicator_calc_error", "msg": f"{type(e).__name__}:{str(e)[:160]}"})
            return None

        # 연속 양봉 제외
        exclude_reasons = []
        try:
            df_price_lower = df_price.rename(str.lower, axis=1)
            if count_consecutive_up(df_price_lower.tail(10)) >= int(cfg.get("exclude_consecutive_up_days", 5)):
                exclude_reasons.append("UP_STREAK")
        except Exception as e:
            with _fail_lock:
                _fail_stats["up_streak_calc"] += 1
                _fail_rows.append({"Ticker": code, "reason": "up_streak_calc", "msg": f"{type(e).__name__}:{str(e)[:160]}"})

        # 투자자별 수급 데이터 조회 (안전한 방식)
        df_investor_flow = None
        try:
            df_investor_flow = get_investor_flow(code, date_str)
            if df_investor_flow is not None and not df_investor_flow.empty:
                # 데이터 정규화
                df_investor_flow = df_investor_flow.fillna(0)
                # 컬럼명 정리
                df_investor_flow.columns = [col.strip() for col in df_investor_flow.columns]
            else:
                # 기본 투자자 흐름 데이터 생성 (실제 데이터가 없을 때)
                end_date = datetime.strptime(date_str, "%Y%m%d")
                dates = [end_date - timedelta(days=i) for i in range(10, 0, -1)]
                df_investor_flow = pd.DataFrame({
                    'Date': dates,
                    '기관합계': [0] * 10,
                    '외국인합계': [0] * 10
                })
        except Exception as e:
            logger.debug(f"[{code}] 투자자 흐름 데이터 조회 실패: {e}")
            # 기본 데이터 생성
            end_date = datetime.strptime(date_str, "%Y%m%d")
            dates = [end_date - timedelta(days=i) for i in range(10, 0, -1)]
            df_investor_flow = pd.DataFrame({
                'Date': dates,
                '기관합계': [0] * 10,
                '외국인합계': [0] * 10
            })
        
        # --- 차트 데이터 준비 ---
        close_series = df_price["Close"] if "Close" in df_price.columns else None
        
        # daily_chart 데이터 준비 (최근 60일 OHLCV 데이터)
        daily_chart_data = None
        if df_price is not None and not df_price.empty and len(df_price) >= 20:
            try:
                # 최근 60일 데이터만 사용 (메모리 절약)
                recent_data = df_price.tail(60).copy()
                # 인덱스를 날짜로 변환
                if hasattr(recent_data.index, 'to_pydatetime'):
                    recent_data.index = recent_data.index.to_pydatetime()
                daily_chart_data = recent_data.reset_index().to_dict('records')
            except Exception as e:
                logger.debug(f"[{code}] daily_chart 데이터 준비 실패: {e}")
                daily_chart_data = None
        
        # --- 컴포넌트 스코어 계산 ---
        # MA200이 없을 때를 고려한 기술적 스코어 계산
        ma50_above_ma200_score = 1 if ma50 > ma200 else 0
        close_above_ma50_score = 1 if close > ma50 else 0
        rsi_score = max(0, 1 - abs(rsi - 50) / 50)
        
        # MA200이 대체값인 경우 가중치 조정
        data_length = len(close_series)
        if data_length < 200:  # MA200이 실제가 아닌 경우
            if data_length >= 100:  # MA100 사용
                tech_score = (close_above_ma50_score * 0.4 + ma50_above_ma200_score * 0.3 + rsi_score * 0.3)
            elif data_length >= 50:  # MA50 사용
                tech_score = (close_above_ma50_score * 0.5 + rsi_score * 0.5)
            else:  # 현재가 사용
                tech_score = (close_above_ma50_score * 0.6 + rsi_score * 0.4)
        else:  # 정상적인 MA200 사용
            tech_score = (close_above_ma50_score + ma50_above_ma200_score + rsi_score) / 3
        per_val = pd.to_numeric(fin_info.get("PER"), errors="coerce")
        pbr_val = pd.to_numeric(fin_info.get("PBR"), errors="coerce")

        # KIS는 결측치를 NaN이 아닌 0.0으로 반환하는 경우가 많다 → 0도 결측으로 취급.
        # (과거: 0/음수를 '매우 저평가'로 보아 만점을 줘서 결측주·적자기업이 상위에 오던 버그)
        per_missing = pd.isna(per_val) or (per_val == 0)
        pbr_missing = pd.isna(pbr_val) or (pbr_val == 0)
        marcap = fin_info.get("Marcap", 0)

        # PER 결측 시 시가총액 기반 추정값으로 대체
        if per_missing:
            if marcap > 1e12:      # 1조 이상(대형주)
                per_val = 15.0
            elif marcap > 1e11:    # 1000억 이상(중형주)
                per_val = 20.0
            elif marcap > 0:       # 소형주
                per_val = 25.0
            else:
                per_val = 20.0     # 기본값
            logger.debug(f"[{code}] PER 결측(0/NaN) → 추정값 {per_val} 사용")

        # PBR 결측 시 시가총액 기반 추정값으로 대체
        if pbr_missing:
            if marcap > 1e12:
                pbr_val = 1.2
            elif marcap > 1e11:
                pbr_val = 1.5
            elif marcap > 0:
                pbr_val = 2.0
            else:
                pbr_val = 1.5
            logger.debug(f"[{code}] PBR 결측(0/NaN) → 추정값 {pbr_val} 사용")

        # PER 점수: 음수(적자)는 페널티(B안), 양수는 저평가일수록 고점
        if per_val < 0:
            per_term = 0.1  # 적자 기업 페널티(저평가가 아니라 수익성 부재)
            logger.debug(f"[{code}] 음수 PER({per_val}) → 적자 페널티 적용(per_term=0.1)")
        else:
            per_term = max(0.0, min(1.0, (50 - per_val) / 50))

        # PBR 점수: 음수(자본잠식)는 페널티, 양수는 저평가일수록 고점
        if pbr_val < 0:
            pbr_term = 0.1  # 자본잠식 페널티
            logger.debug(f"[{code}] 음수 PBR({pbr_val}) → 자본잠식 페널티 적용(pbr_term=0.1)")
        else:
            pbr_term = max(0.0, min(1.0, (5 - pbr_val) / 5))

        fin_score = 0.5 * (per_term + pbr_term)
        sector_name = str(fin_info.get("Sector", "N/A")) if "Sector" in fin_info else "N/A"
        sector_score = float(sector_trends.get(sector_name, 0.5))
        
        # 신규 스코어
        df_price_lower_for_kki = df_price.rename(str.lower, axis=1)
        vol_kki = compute_kki_metrics(df_price_lower_for_kki)
        pos_52w = compute_52w_position(close_series)
        
        # 패턴 분석 추가
        try:
            ma20_up = analyze_ma20_trend(df_price)
            accum_vol = analyze_accumulation_volume(df_price)
            higher_lows = detect_higher_lows(df_price)
            consolidation = detect_consolidation(df_price)
            yey_pattern = detect_yey_pattern(df_price)
            
            # 패턴 스코어 계산
            pattern_score = 0.0
            if ma20_up:
                pattern_score += 0.2
            if accum_vol:
                pattern_score += 0.2
            if higher_lows:
                pattern_score += 0.2
            if consolidation:
                pattern_score += 0.2
            if yey_pattern:
                pattern_score += 0.2
                
        except Exception as e:
            logger.warning(f"[{code}] 패턴 분석 실패: {e}")
            ma20_up = False
            accum_vol = False
            higher_lows = False
            consolidation = False
            yey_pattern = False
            pattern_score = 0.0

        # --- 가중치 및 최종 스코어 ---
        fin_w = float(cfg.get("fin_weight", 0.25))
        tech_w = float(cfg.get("tech_weight", 0.30))
        mkt_w = float(cfg.get("mkt_weight", 0.15))
        sector_w = float(cfg.get("sector_weight", 0.15))
        vol_kki_w = float(cfg.get("vol_kki_weight", 0.10))
        pos_52w_w = float(cfg.get("pos_52w_weight", 0.05))

        total_score = (
            fin_score * fin_w
            + tech_score * tech_w
            + market_score * mkt_w
            + sector_score * sector_w
            + vol_kki * vol_kki_w
            + pos_52w * pos_52w_w
        )
        total_score = float(np.clip(total_score, 0.0, 1.0))
        
        # 시장 분석 기반 스코어 조정
        from screener_core import calculate_market_adjusted_score
        if _CURRENT_MARKET_STATE is not None:
            _score_before_mkt_adj = total_score
            total_score = calculate_market_adjusted_score(total_score, _CURRENT_MARKET_STATE)
            if abs(total_score - _score_before_mkt_adj) > 1e-9:
                logger.debug(
                    "[%s] 시장보정: %.4f → %.4f (regime=%s)",
                    code, _score_before_mkt_adj, total_score,
                    getattr(getattr(_CURRENT_MARKET_STATE, "regime", None), "value", "?"),
                )
        name_val = fin_info.get("Name", "")
        sector_src = fin_info.get("SectorSource", "unknown")

        # 디버깅: 종목별 컴포넌트 스코어 분해 로그
        logger.debug(
            "[%s] 스코어=%.4f | Fin=%.3f(w%.2f) Tech=%.3f(w%.2f) Mkt=%.3f(w%.2f) "
            "Sector=%.3f(w%.2f) VolKki=%.3f(w%.2f) Pos52w=%.3f(w%.2f) | "
            "RSI=%.1f MA50=%.0f MA200=%.0f PER=%.1f PBR=%.2f%s",
            code, float(total_score),
            float(fin_score), fin_w, float(tech_score), tech_w, float(market_score), mkt_w,
            float(sector_score), sector_w, float(vol_kki), vol_kki_w, float(pos_52w), pos_52w_w,
            float(rsi), float(ma50), float(ma200), float(per_val), float(pbr_val),
            (" | 제외사유:" + ",".join(exclude_reasons)) if exclude_reasons else "",
        )

        return {
            "Ticker": code,
            "Name": str(name_val) if pd.notna(name_val) else "",
            "Sector": sector_name,
            "SectorSource": str(sector_src) if pd.notna(sector_src) else "unknown",
            "Price": int(round(float(close))),
            "Score": round(float(total_score), 4),

            "FinScore": round(float(fin_score), 4),
            "TechScore": round(float(tech_score), 4),
            "MktScore": round(float(market_score), 4),
            "SectorScore": round(float(sector_score), 4),
            "VolKki": round(float(vol_kki), 4),
            "Pos52w": round(float(pos_52w), 4),
            "PatternScore": round(float(pattern_score), 4),

            "PER": round(float(per_val), 2) if pd.notna(per_val) else 20.0,
            "PBR": round(float(pbr_val), 2) if pd.notna(pbr_val) else 1.5,
            "RSI": round(float(rsi), 2) if pd.notna(rsi) else 50.0,
            "ATR": round(float(atr_val), 2) if atr_val is not None and pd.notna(atr_val) else (round(float(close) * 0.02, 2) if pd.notna(close) else 100.0),
            "MA50": round(float(ma50), 2) if pd.notna(ma50) else (round(float(close), 2) if pd.notna(close) else 0.0),
            "MA200": round(float(ma200), 2) if pd.notna(ma200) else (round(float(close), 2) if pd.notna(close) else 0.0),

            # 패턴 분석 결과
            "MA20Up": bool(ma20_up),
            "AccumVol": bool(accum_vol),
            "HigherLows": bool(higher_lows),
            "Consolidation": bool(consolidation),
            "YEY": bool(yey_pattern),

            "exclude_reasons": exclude_reasons,
            "daily_chart": daily_chart_data,
            "investor_flow": df_investor_flow.reset_index().to_dict('records') if df_investor_flow is not None else None,
        }

    except Exception as ex:
        error_msg = f"{type(ex).__name__}:{str(ex)[:160]}"
        logger.debug("[%s] 스코어 계산 예외(step=main): %s", code, ex, exc_info=True)
        with _fail_lock:
            _fail_stats["exception"] += 1
            _fail_rows.append({"Ticker": code, "reason": "exception", "msg": f"main:{error_msg}"})
        return None

def diversify_by_sector(df_sorted: pd.DataFrame, top_n: int, sector_cap: float) -> pd.DataFrame:
    if top_n <= 0 or df_sorted.empty:
        return df_sorted.iloc[0:0]
    if sector_cap <= 0:
        return df_sorted.head(top_n)
    
    df_clean = df_sorted[df_sorted["exclude_reasons"].apply(len) == 0]
    df_excluded = df_sorted[df_sorted["exclude_reasons"].apply(len) > 0]
    
    max_per_sector = max(1, int(np.ceil(top_n * float(sector_cap))))
    sector_series = (
        df_clean["Sector"]
        if "Sector" in df_clean.columns
        else pd.Series(["N/A"] * len(df_clean), index=df_clean.index)
    )
    counts: Dict[str, int] = {}
    selected_idx: List[Any] = []
    
    for idx, sec in zip(df_clean.index, sector_series):
        c = counts.get(sec, 0)
        if c < max_per_sector:
            selected_idx.append(idx)
            counts[sec] = c + 1
        if len(selected_idx) >= top_n:
            break
            
    if len(selected_idx) < top_n and not df_excluded.empty:
        need = top_n - len(selected_idx)
        selected_idx.extend(df_excluded.index[:need].tolist())

    final_df = df_sorted.loc[selected_idx]
    return final_df.head(top_n)

# ─────────── 메인 실행 ───────────
def run_screener(
    date_str: str,
    market: str,
    config_path: Optional[str],
    workers: int,
    debug: bool,
    pipeline_session: Optional[str] = None,
):
    global _KIS_INSTANCE, _KIS_RATE_LIMITER, _KIS_MAX_CONCURRENCY, _CURRENT_MARKET_STATE
    sess = (pipeline_session or os.getenv("PIPELINE_SESSION", "")).lower().strip()
    if sess not in ("am", "pm"):
        sess = resolve_pipeline_context(market=market).get("session", "pm")
    os.environ["PIPELINE_SESSION"] = sess
    start_msg = (
        f"▶ 스크리너 시작 (date={date_str}, session={sess}, market={market}, "
        f"workers={workers}, debug={debug})"
    )
    logger.info(start_msg)
    _notify(start_msg, key="screener_start", cooldown_sec=60)

    if debug:
        # 단계별 DEBUG 로그가 실제로 출력되도록 로거+핸들러 레벨을 함께 낮춘다.
        logger.setLevel(logging.DEBUG)
        _root_logger = logging.getLogger()
        _root_logger.setLevel(logging.DEBUG)
        for _h in _root_logger.handlers:
            # 디스코드 핸들러는 에러 전용으로 유지(스팸 방지), 콘솔/파일 핸들러만 DEBUG로 낮춘다.
            if isinstance(_h, DiscordLogHandler):
                continue
            try:
                _h.setLevel(logging.DEBUG)
            except Exception:
                pass
        logger.debug("DEBUG 로깅 활성화: 단계별 상세 로그를 출력합니다.")

    ensure_output_dir()

    if not is_market_open_day(market=market):
        msg = f"휴장일이므로 screener를 건너뜁니다. (market={market})"
        logger.info(msg)
        _notify(f"ℹ️ {msg}", key="screener_holiday", cooldown_sec=600)
        return

    # 오늘 개장일 여부(로그용)
    try:
        mkt_label = "US(NYSE)" if is_us_market(market) else "국내"
        open_day = is_market_open_day(market=market)
        logger.info("오늘 %s 개장일 여부: %s", mkt_label, "개장" if open_day else "휴장")
    except Exception:
        pass

    # config 로드 (utils.get_cfg 사용)
    settings = get_cfg()

    if config_path and Path(config_path).expanduser().is_file():
        try:
            with open(Path(config_path).expanduser(), "r", encoding="utf-8") as f:
                cli_cfg = json.load(f)
            settings.update(cli_cfg or {})
            logger.info("CLI config 병합 완료: %s", str(Path(config_path).expanduser()))
        except Exception as e:
            logger.warning("CLI config 병합 실패(%s): %s", config_path, str(e))

    if not settings:
        msg = "설정 로딩 실패로 종료합니다."
        logger.error(msg)
        _notify(f"❌ {msg}", key="screener_config_err", cooldown_sec=60)
        return

    # KIS 인스턴스
    broker_config = settings.get("kis_broker", {})
    trading_env = settings.get("trading_environment", "mock")
    kis = KIS(broker_config, env=trading_env)
    if not getattr(kis, "auth_token", None):
        msg = "KIS API 인증 실패로 종료합니다."
        logger.error(msg)
        _notify(f"❌ {msg}", key="screener_kis_auth_fail", cooldown_sec=60)
        return
    logger.info("'%s' 모드로 KIS API 인증 완료.", trading_env)
    _KIS_INSTANCE = kis

    # KIS 레이트 리밋/동시성 설정(설정값/환경변수/기본값)
    kis_limits = settings.get("kis_limits", {})
    kis_rps = float(kis_limits.get("max_rps", os.getenv("KIS_MAX_RPS", 3)))
    max_conc = int(kis_limits.get("max_concurrency", os.getenv("KIS_MAX_CONCURRENCY", 2)))
    _KIS_RATE_LIMITER = RateLimiter(kis_rps) if kis_rps and kis_rps > 0 else None
    _KIS_MAX_CONCURRENCY = max(1, min(max_conc, 4))  # 하드 안전상한 4

    screener_params = settings.get("screener_params", {})
    risk_params = settings.get("risk_params", {})

    with stage("1차 필터링", notify_key="screener_stage1"):
        # 시장 상태를 고려한 스크리닝 파라미터 조정
        from screener_core import get_market_aware_screening_params
        if _CURRENT_MARKET_STATE is not None:
            adjusted_params = get_market_aware_screening_params(_CURRENT_MARKET_STATE, screener_params)
            logger.info(f"시장 인식 스크리닝 파라미터 적용: {_CURRENT_MARKET_STATE.regime.value}")
        else:
            adjusted_params = screener_params
        
        df_filtered, fixed_date = _filter_initial_stocks(date_str, adjusted_params, market, risk_params, debug)
        if df_filtered.empty:
            msg = "❌ 1차 필터링 결과, 대상 종목이 없습니다."
            logger.warning(msg)
            _notify(msg, key="screener_no_candidates_stage1", cooldown_sec=60)
            return

    # KIS 1회 호출로 섹터+상장일 동시 조회 → 이후 섹터 보강/상장일 프리패치는 캐시만 사용
    with stage("KIS 통합 조회(섹터+상장일)", notify_key=None):
        kis_fetch_sector_and_listing_batch(
            kis, list(df_filtered.index), fixed_date, workers, market=market
        )

    with stage("섹터 보강", notify_key="screener_sector"):
        # 기본 우선순위: pykrx 우선, FDR/캐시 다음, 실시간은 KIS 우선
        order = screener_params.get("sector_source_priority", ["pykrx", "fdr", "kis"])
        df_filtered = _apply_sector_source_order(df_filtered, order, kis, workers, fixed_date, market)

    with stage("시장 레짐 계산", notify_key="screener_regime"):
        # 기존 시장 레짐 계산
        regime = _get_market_regime_score(fixed_date, market)
        market_score = 0.7 * regime + 0.3 * 0.5
        comps = _get_market_regime_components(fixed_date, market)
        market_trend = get_market_trend(fixed_date)
        logger.info("시장 레짐 스코어 (가중치 적용): %.3f", market_score)
        logger.info(
            "레짐 구성요소: above_ma50=%.2f, ma50>ma200=%.2f, rsi_term=%.2f",
            comps["above_ma50"], comps["ma50_gt_ma200"], comps["rsi_term"],
        )
        logger.info("시장 단기 추세(60D MA5/MA20): %s", market_trend)
        
        # MarketAnalyzer를 통한 고급 시장 분석
        market_analyzer = MarketAnalyzer(settings, kis=kis, market=market, date_str=fixed_date)
        market_state = market_analyzer.analyze_market_state()
        logger.info("고급 시장 분석: %s", market_analyzer.get_market_summary(market_state))

        # market_state sidecar 저장 (trader의 dynamic_cash_management가 재사용)
        try:
            out = {
                "generated_at": datetime.now(KST).isoformat(),
                "date": fixed_date,
                "market": market,
                "regime": getattr(getattr(market_state, "regime", None), "value", None),
                "volatility_level": getattr(market_state, "volatility_level", None),
                "trend_direction": getattr(market_state, "trend_direction", None),
                "confidence": float(getattr(market_state, "confidence", 0.0) or 0.0),
                "regime_components": comps,
                "regime_score": float(regime) if regime is not None else None,
                "market_score": float(market_score) if market_score is not None else None,
                "market_trend": market_trend,
            }
            p = OUTPUT_DIR / format_pipeline_artifact(
                "market_state", fixed_date, market, sess
            )
            with open(p, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            logger.info("시장 상태 저장: %s", p)
        except Exception as e:
            logger.warning("시장 상태 저장 실패: %s", e)
        
        # 시장 상태를 전역 변수로 저장 (후속 단계에서 사용)
        _CURRENT_MARKET_STATE = market_state

    with stage("섹터 트렌드 계산", notify_key="screener_sector_trend"):
        sector_trends = _calculate_sector_trends(fixed_date)

    # ✅ 보유 종목 스코어 업데이트
    holdings_scores = {}
    with stage("보유 종목 스코어 업데이트", notify_key="screener_holdings"):
        holdings = get_holdings_from_balance()
        if holdings:
            holdings_scores = update_holdings_scores(
                holdings, fixed_date, market, screener_params, 
                market_score, sector_trends, risk_params
            )
        else:
            logger.info("보유 종목이 없어 스코어 업데이트를 건너뜁니다.")

    # ✅ KIS 상장일 사전 캐싱 (로그 1회, 스코어링 전)
    with stage("상장일(KIS) 프리패치", notify_key=None):
        get_listing_date_kis_prefetch(
            kis, list(df_filtered.index), fixed_date, workers, market=market
        )

    with stage("상세 분석(스코어링)", notify_key="screener_scoring"):
        # 빈/무효 티커 제거 (All attempts failed for "" 방지)
        _min_len = 1 if is_us_market(market) else 4
        _valid_idx = df_filtered.index.notna() & (df_filtered.index.astype(str).str.strip().str.len() >= _min_len)
        if not _valid_idx.all():
            dropped = (~_valid_idx).sum()
            logger.warning("무효 티커(빈/짧은 인덱스) 제외: %d건 → 스코어링 대상 %d건", int(dropped), int(_valid_idx.sum()))
            df_filtered = df_filtered[_valid_idx]
        if df_filtered.empty:
            logger.warning("스코어링 대상 종목이 없어 상세 분석을 건너뜁니다.")
            return
        # 스코어링 실패 통계 초기화
        global _fail_stats, _fail_rows
        _fail_stats.clear()
        _fail_rows.clear()
        
        results = []
        total = len(df_filtered)
        actual_workers = max(1, min(workers, MAX_WORKERS_HARD_CAP))
        
        logger.info("스코어링 시작: %d 종목, %d 워커", total, actual_workers)
        
        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            futures = {
                executor.submit(
                    _calculate_scores_for_ticker,
                    code,
                    fixed_date,
                    row,
                    screener_params,
                    market_score,
                    sector_trends,
                    risk_params,
                ): code
                for code, row in df_filtered.iterrows()
            }
            
            completed = 0
            for fut in as_completed(futures):
                completed += 1
                if completed % 25 == 0 or completed == total:
                    progress_pct = completed * 100.0 / total
                    logger.info("  >> 상세 분석 진행률: %d/%d (%.1f%%)", completed, total, progress_pct)
                
                try:
                    res = fut.result(timeout=30)  # 30초 타임아웃 추가
                    if res:
                        results.append(res)
                except Exception as e:
                    code = futures[fut]
                    logger.warning("[%s] 스코어링 타임아웃/에러: %s", code, str(e))
                    with _fail_lock:
                        _fail_stats["timeout_error"] += 1
                        _fail_rows.append({"Ticker": code, "reason": "timeout_error", "msg": str(e)[:160]})

        # 디버깅: 스코어링 성공/스킵/실패 집계 및 점수 분포
        skip_total = sum(_fail_stats.values())
        logger.info(
            "스코어링 결과: 성공=%d, 스킵/실패=%d (대상=%d, 성공률=%.1f%%)",
            len(results), skip_total, total,
            (len(results) * 100.0 / total) if total else 0.0,
        )
        if results:
            try:
                _scores = pd.Series([r.get("Score", 0.0) for r in results], dtype="float64")
                logger.info(
                    "스코어 분포: 평균=%.3f, 중앙=%.3f, 최소=%.3f, 최대=%.3f, P75=%.3f, P90=%.3f",
                    _scores.mean(), _scores.median(), _scores.min(), _scores.max(),
                    _scores.quantile(0.75), _scores.quantile(0.90),
                )
                _thr = float(screener_params.get("min_score_threshold", 0.0) or 0.0)
                if _thr > 0:
                    logger.info(
                        "스코어 임계값(%.2f) 이상: %d/%d 종목",
                        _thr, int((_scores >= _thr).sum()), len(_scores),
                    )
            except Exception as _e:
                logger.debug("스코어 분포 로깅 실패(무시): %s", _e)

        # 스코어링 실패/스킵 요약 및 CSV 덤프
        try:
            if _fail_stats:
                fail_sum = ", ".join(f"{k}={v}" for k, v in _fail_stats.items())
                only_skips = set(_fail_stats.keys()).issubset({"skipped_short_history", "newly_listed_skip"})
                if only_skips:
                    logger.info("스코어링 스킵 요약: %s", fail_sum)
                else:
                    logger.warning("스코어링 실패 요약: %s", fail_sum)
                dbg_dir = OUTPUT_DIR / "debug"
                dbg_dir.mkdir(parents=True, exist_ok=True)
                fail_csv = dbg_dir / f"scoring_fail_{fixed_date}_{market}.csv"
                pd.DataFrame(_fail_rows).to_csv(fail_csv, index=False, encoding="utf-8-sig")
                logger.warning("스코어링 실패 상세 CSV 저장: %s", fail_csv)
        except Exception as _e:
            logger.debug("실패 요약/CSV 저장 중 오류: %s", _e)

        if not results:
            try:
                dbg_dir = OUTPUT_DIR / "debug"
                dbg_dir.mkdir(parents=True, exist_ok=True)
                dbg_meta = {
                    "date": fixed_date,
                    "market": market,
                    "filtered_tickers": [str(x) for x in df_filtered.index],
                    "fail_stats": dict(_fail_stats),
                }
                with open(dbg_dir / f"scoring_ctx_{fixed_date}_{market}.json", "w", encoding="utf-8") as f:
                    json.dump(dbg_meta, f, ensure_ascii=False, indent=2)
                logger.info("스코어링 컨텍스트 저장: %s", dbg_dir / f"scoring_ctx_{fixed_date}_{market}.json")
            except Exception as _e:
                logger.debug("컨텍스트 저장 실패: %s", _e)

            # 실패 원인 분석
            fail_analysis = []
            if _fail_stats.get("newly_listed_skip", 0) > 0:
                fail_analysis.append(f"신규상장({_fail_stats['newly_listed_skip']}개)")
            if _fail_stats.get("exception", 0) > 0:
                fail_analysis.append(f"계산오류({_fail_stats['exception']}개)")
            if _fail_stats.get("skipped_short_history", 0) > 0:
                fail_analysis.append(f"데이터부족({_fail_stats['skipped_short_history']}개)")
            if _fail_stats.get("insufficient_data", 0) > 0:
                fail_analysis.append(f"지표계산실패({_fail_stats['insufficient_data']}개)")
            
            analysis_str = ", ".join(fail_analysis) if fail_analysis else "원인불명"
            msg = f"❌ 2차 스크리닝 결과, 최종 후보가 없습니다. (실패원인: {analysis_str})"
            logger.warning(msg)
            _notify(msg, key="screener_no_candidates_stage2", cooldown_sec=60)
            return

    with stage("정렬/다양화/손절·목표가 계산/저장", notify_key="screener_finalize"):
        df_scores = pd.DataFrame(results).set_index("Ticker")
        left = df_filtered.copy()
        right = df_scores.copy()
        overlapping = set(left.columns).intersection(set(right.columns))
        if overlapping:
            logger.debug("join 전 중복 컬럼 제거: %s", sorted(overlapping))
            left = left.drop(columns=list(overlapping), errors="ignore")

        df_final = (
            left.join(right, how="inner")
            .reset_index()
            .rename(columns={"index": "Ticker"})
        )
        # Ticker: KR 6자리 / US 심볼 (Code 컬럼이 있으면 통일 후 제거)
        if "Code" in df_final.columns:
            df_final["Ticker"] = norm_ticker_series(df_final["Code"], market)
            df_final = df_final.drop(columns=["Code"], errors="ignore")
        elif "Ticker" in df_final.columns:
            df_final["Ticker"] = norm_ticker_series(df_final["Ticker"], market)

        # 정렬: 제외사유 없는 것 우선, 그 다음 점수 높은 순
        df_final["exclude_reasons_len"] = df_final["exclude_reasons"].apply(len)
        df_sorted = df_final.sort_values(by=["exclude_reasons_len", "Score"], ascending=[True, False]).drop(columns=["exclude_reasons_len"])

        # Phase 3: 스크리너 필터링 강화
        n_scored_total = len(df_sorted)
        n_after_score = n_scored_total
        n_after_momentum = n_scored_total
        n_after_vol = n_scored_total

        # 1) 최소 점수 임계값 필터
        min_score_threshold = screener_params.get("min_score_threshold", 0.0)
        if min_score_threshold > 0:
            before = len(df_sorted)
            _before_tickers = df_sorted["Ticker"] if "Ticker" in df_sorted.columns else df_sorted.index
            df_sorted = df_sorted[df_sorted["Score"] >= min_score_threshold]
            n_after_score = len(df_sorted)
            logger.info(f"최소 점수 필터 ({min_score_threshold:.2f}): {before} → {len(df_sorted)} 종목")
            _after_tickers = df_sorted["Ticker"] if "Ticker" in df_sorted.columns else df_sorted.index
            _log_dropped(f"최종:최소점수<{min_score_threshold:.2f}", _before_tickers, _after_tickers)
        
        # 2) 모멘텀 필터 (양의 모멘텀 필수)
        if screener_params.get("require_positive_momentum", False):
            before = len(df_sorted)
            # 20일 수익률 계산 (간단한 모멘텀 지표)
            momentum_mask = pd.Series(True, index=df_sorted.index)
            for idx, row in df_sorted.iterrows():
                ticker = row.get("Ticker", "")
                try:
                    # 가격 데이터 조회하여 모멘텀 계산
                    price_data = get_historical_prices(ticker, 
                        (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=30)).strftime("%Y%m%d"),
                        date_str)
                    if price_data is not None and len(price_data) >= 20:
                        close_col = None
                        for col in ["Close", "close", "종가"]:
                            if col in price_data.columns:
                                close_col = col
                                break
                        if close_col:
                            prices = price_data[close_col].tolist()
                            if len(prices) >= 20:
                                momentum_20d = (prices[-1] - prices[-20]) / prices[-20] if prices[-20] > 0 else 0
                                momentum_mask.loc[idx] = momentum_20d > 0
                except Exception as e:
                    logger.debug(f"[{ticker}] 모멘텀 계산 실패: {e}")
                    momentum_mask.loc[idx] = True  # 오류 시 통과
            
            _before_tickers = df_sorted["Ticker"] if "Ticker" in df_sorted.columns else df_sorted.index
            df_sorted = df_sorted[momentum_mask]
            n_after_momentum = len(df_sorted)
            logger.info(f"양의 모멘텀 필터: {before} → {len(df_sorted)} 종목")
            _after_tickers = df_sorted["Ticker"] if "Ticker" in df_sorted.columns else df_sorted.index
            _log_dropped("최종:음의모멘텀", _before_tickers, _after_tickers)
        
        # 3) 변동성 필터 (고변동성 종목 제외)
        if screener_params.get("exclude_high_volatility", False):
            volatility_threshold = screener_params.get("volatility_threshold", 0.30)
            before = len(df_sorted)
            volatility_mask = pd.Series(True, index=df_sorted.index)
            for idx, row in df_sorted.iterrows():
                ticker = row.get("Ticker", "")
                try:
                    # 가격 데이터 조회하여 변동성 계산
                    price_data = get_historical_prices(ticker,
                        (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d"),
                        date_str)
                    if price_data is not None and len(price_data) >= 20:
                        close_col = None
                        for col in ["Close", "close", "종가"]:
                            if col in price_data.columns:
                                close_col = col
                                break
                        if close_col:
                            prices = price_data[close_col].tolist()
                            if len(prices) >= 20:
                                returns = pd.Series(prices).pct_change().dropna()
                                volatility = returns.std() * (252 ** 0.5)  # 연율화 변동성
                                volatility_mask.loc[idx] = volatility <= volatility_threshold
                except Exception as e:
                    logger.debug(f"[{ticker}] 변동성 계산 실패: {e}")
                    volatility_mask.loc[idx] = True  # 오류 시 통과
            
            _before_tickers = df_sorted["Ticker"] if "Ticker" in df_sorted.columns else df_sorted.index
            df_sorted = df_sorted[volatility_mask]
            n_after_vol = len(df_sorted)
            logger.info(f"변동성 필터 (≤{volatility_threshold:.0%}): {before} → {len(df_sorted)} 종목")
            _after_tickers = df_sorted["Ticker"] if "Ticker" in df_sorted.columns else df_sorted.index
            _log_dropped(f"최종:고변동성>{volatility_threshold:.0%}", _before_tickers, _after_tickers)

        top_n = min(int(screener_params.get("top_n", 10)), int(risk_params.get("max_positions", 10)))
        sector_cap = float(screener_params.get("sector_cap", 0.3))
        
        # 다양화 (Ticker 컬럼 없으면 인덱스를 Ticker로 사용)
        if "Ticker" not in df_sorted.columns and len(df_sorted.columns) > 0:
            df_sorted = df_sorted.copy()
            df_sorted.insert(0, "Ticker", df_sorted.index)
        final_candidates_base = diversify_by_sector(df_sorted.set_index("Ticker"), top_n, sector_cap).reset_index()

        # 최종 단계 퍼널 요약
        _log_funnel(
            "최종 선정",
            [
                ("스코어링 통과", n_scored_total),
                (f"최소점수≥{min_score_threshold:.2f}", n_after_score),
                ("양의 모멘텀", n_after_momentum),
                ("변동성 필터", n_after_vol),
                (f"섹터다양화(top{top_n})", len(final_candidates_base)),
            ],
        )
        if "Sector" in final_candidates_base.columns and not final_candidates_base.empty:
            _sec_dist = final_candidates_base["Sector"].value_counts().to_dict()
            logger.info("최종 후보 섹터 분포: %s", _sec_dist)

        # ── 레벨 계산 ──
        # Phase 1: config에서 읽기
        strategy_params = settings.get("strategy_params", {})
        stop_loss_pct = strategy_params.get("stop_loss_pct", 0.03)
        take_profit_pct = strategy_params.get("take_profit_pct", 0.08)
        
        levels_data = []
        for _, row in final_candidates_base.iterrows():
            levels = {
                "stop_loss": row["Price"] * (1 - stop_loss_pct),  # Phase 1: 하드코딩 제거
                "take_profit": row["Price"] * (1 + take_profit_pct),  # Phase 1: 하드코딩 제거
                "atr_stop": row["Price"] * (1 - stop_loss_pct * 0.5),  # ATR 기반 손절 (임시)
                "atr_profit": row["Price"] * (1 + take_profit_pct * 0.5)   # ATR 기반 목표가 (임시)
            }
            levels_data.append(levels)
        df_levels = pd.DataFrame(levels_data, index=final_candidates_base.index)
        final_candidates = pd.concat([final_candidates_base, df_levels], axis=1)

        # 필수 컬럼 보장 - stop_loss/take_profit은 후보 0건이면 df_levels가 비어 있어 없을 수 있음
        if "손절가" not in final_candidates.columns:
            if "stop_loss" in final_candidates.columns:
                final_candidates["손절가"] = final_candidates["stop_loss"]
            elif "Price" in final_candidates.columns:
                final_candidates["손절가"] = final_candidates["Price"] * (1 - stop_loss_pct)
            else:
                final_candidates["손절가"] = np.nan
        if "목표가" not in final_candidates.columns:
            if "take_profit" in final_candidates.columns:
                final_candidates["목표가"] = final_candidates["take_profit"]
            elif "Price" in final_candidates.columns:
                final_candidates["목표가"] = final_candidates["Price"] * (1 + take_profit_pct)
            else:
                final_candidates["목표가"] = np.nan
        if "source" not in final_candidates.columns:
            final_candidates["source"] = "atr_based"
        if "stop_price" not in final_candidates.columns:
            final_candidates["stop_price"] = final_candidates["손절가"]
        if "target_price" not in final_candidates.columns:
            final_candidates["target_price"] = final_candidates["목표가"]
        if "levels_source" not in final_candidates.columns:
            final_candidates["levels_source"] = final_candidates["source"]
        if "SectorSource" not in final_candidates.columns:
            final_candidates["SectorSource"] = "unknown"
        if "Sector" not in final_candidates.columns:
            final_candidates["Sector"] = "N/A"
        if "Score" not in final_candidates.columns:
            final_candidates["Score"] = 0.0

        # 컬럼 순서
        cols = [
            "Ticker", "Name", "Sector", "SectorSource", "EXCD", "OvrsExcg", "Price",
            "손절가", "목표가", "source", "stop_price", "target_price", "levels_source",
            "MA50", "MA200", "Score",
            "FinScore", "TechScore", "MktScore", "SectorScore", "VolKki", "Pos52w",
            "PER", "PBR", "RSI", "ATR", "Marcap", "Amount5D", "exclude_reasons",
        ]
        keep = [c for c in cols if c in final_candidates.columns]
        final_candidates = final_candidates[keep + [c for c in final_candidates.columns if c not in keep]]

        # Ticker 정규화: KR 6자리 / US 심볼 (하류 trader/GPT 호환)
        if "Ticker" in final_candidates.columns:
            final_candidates["Ticker"] = norm_ticker_series(final_candidates["Ticker"], market)
            _min_ticker_len = 1 if is_us_market(market) else 4
            empty_ticker = (final_candidates["Ticker"] == "") | (
                final_candidates["Ticker"].str.len() < _min_ticker_len
            )
            if empty_ticker.any():
                logger.warning("Ticker 비정상 %d건 제외", empty_ticker.sum())
                final_candidates = final_candidates[~empty_ticker]

        # daily_chart와 investor_flow는 유지 (GPT 분석기에서 필요)
        generated_at = datetime.now(KST).isoformat()
        
        # 후보 데이터 생성 (실제 사용되는 것만)
        final_candidates_full = final_candidates.copy()
        final_candidates_full["schema_version"] = SCHEMA_VERSION
        final_candidates_full["generated_at"] = generated_at
        final_candidates_slim = final_candidates.copy()  # 모든 컬럼 유지
        final_candidates_slim["schema_version"] = SCHEMA_VERSION
        final_candidates_slim["generated_at"] = generated_at

        # ▶ 스크리너 단계에서는 '요청 플래그만' 기록 (Trader에서 실제 필터링)
        aff_req = bool(settings.get("screener_params", {}).get("affordability_filter", False))
        for df_ in (final_candidates_full, final_candidates_slim):
            df_["affordability_filter_requested"] = aff_req

        # 파일 경로 (실제 사용되는 파일만)
        cands_full_json  = OUTPUT_DIR / format_pipeline_artifact(
            "screener_candidates_full", fixed_date, market, sess
        )
        cands_slim_json  = OUTPUT_DIR / format_pipeline_artifact(
            "screener_candidates", fixed_date, market, sess
        )
        scores_json      = OUTPUT_DIR / format_pipeline_artifact(
            "screener_scores", fixed_date, market, sess
        )
        holdings_json    = OUTPUT_DIR / format_pipeline_artifact(
            "screener_holdings", fixed_date, market, sess
        )

        # 저장 (실제 사용되는 파일만)
        final_candidates_full.to_json(cands_full_json, orient="records", indent=2, force_ascii=False)
        final_candidates_slim.to_json(cands_slim_json, orient="records", indent=2, force_ascii=False)
        
        # 보유 종목 점수 캐시 (트레이더 교체 판단용) - 확장된 데이터 구조 (존재하는 컬럼만 사용)
        _score_cols = [
            "Ticker", "Name", "Sector", "Price", "Score",
            "FinScore", "TechScore", "MktScore", "SectorScore", "PatternScore",
            "VolKki", "Pos52w", "PER", "PBR", "RSI", "ATR", "MA50", "MA200",
            "MA20Up", "AccumVol", "HigherLows", "Consolidation", "YEY",
            "affordability_filter_requested",
        ]
        _score_cols_present = [c for c in _score_cols if c in final_candidates_slim.columns]
        if not _score_cols_present:
            logger.warning("스코어 저장용 컬럼이 없어 scores_json 생략")
        else:
            scores_to_save = final_candidates_slim[_score_cols_present].copy()
            _rename_map = {
                "Ticker": "ticker",
                "Score": "score_total",
                "Name": "name",
                "Sector": "sector",
                "Price": "price",
                "FinScore": "fin_score",
                "TechScore": "tech_score",
                "MktScore": "mkt_score",
                "SectorScore": "sector_score",
                "PatternScore": "pattern_score",
                "VolKki": "vol_kki",
                "Pos52w": "pos_52w",
                "PER": "per",
                "PBR": "pbr",
                "RSI": "rsi",
                "ATR": "atr",
                "MA50": "ma50",
                "MA200": "ma200",
                "MA20Up": "ma20_up",
                "AccumVol": "accum_vol",
                "HigherLows": "higher_lows",
                "Consolidation": "consolidation",
                "YEY": "yey",
            }
            scores_to_save = scores_to_save.rename(columns={k: v for k, v in _rename_map.items() if k in scores_to_save.columns})
            scores_to_save["updated_at"] = fixed_date
            scores_to_save.to_json(scores_json, orient="records", indent=2, force_ascii=False)

        # 보유 종목 스코어 저장 (trader.py가 사용)
        if holdings_scores:
            holdings_list = list(holdings_scores.values())
            with open(holdings_json, "w", encoding="utf-8") as f:
                json.dump(holdings_list, f, ensure_ascii=False, indent=2)
            logger.info("보유 종목 스코어 저장: %s", holdings_json)

        # 로그 (실제 사용되는 파일만)
        logger.info("최종 후보(풀) 저장: %s", cands_full_json)
        logger.info("✅ 스크리닝 완료. 후보(슬림) 저장: %s", cands_slim_json)
        logger.info("스코어 캐시 저장: %s", scores_json)
        if holdings_scores:
            logger.info("보유 종목 스코어 저장: %s (%d개)", holdings_json, len(holdings_scores))

        try:
            _top_cols = ["Ticker", "Name", "Sector", "Price", "목표가", "손절가", "Score"]
            _top_cols_ok = [c for c in _top_cols if c in final_candidates_slim.columns]
            if not _top_cols_ok or final_candidates_slim.empty:
                _notify("✅ 스크리너 완료 (후보 없음 또는 요약 컬럼 부족)", key="screener_done", cooldown_sec=60)
            else:
                top5 = final_candidates_slim.head(5)[_top_cols_ok]
                lines = ["Top5:"]
                for _, r in top5.iterrows():
                    px = int(r["Price"]) if pd.notna(r["Price"]) else 0
                    tp = int(r["목표가"]) if pd.notna(r["목표가"]) else 0
                    sl = int(r["손절가"]) if pd.notna(r["손절가"]) else 0
                    lines.append(
                        f"- {r.get('Name','')}({norm_ticker(r['Ticker'], market)}), "
                        f"Sec:{r.get('Sector','N/A')}, Px:{px:,}, "
                        f"TP:{tp:,}, SL:{sl:,}, S:{float(r['Score']):.3f}"
                    )
                _notify("✅ 스크리너 완료\n" + "\n".join(lines), key="screener_done", cooldown_sec=60)
        except Exception:
            _notify("✅ 스크리너 완료 (요약 구성 실패)", key="screener_done", cooldown_sec=60)

# ─────────── CLI ───────────
def parse_args():
    parser = argparse.ArgumentParser(description="KOSPI/KOSDAQ/KONEX 스크리너")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--session", choices=["am", "pm"], help="파이프라인 세션 (미지정 시 KST·MARKET 기준 자동)")
    parser.add_argument(
        "--market",
        default=os.getenv("MARKET", "SP500"),
        choices=["KOSPI", "KOSDAQ", "KONEX", "SP500"],
    )
    parser.add_argument("--config", help="추가/오버레이할 config.json 파일 경로")
    parser.add_argument("--workers", type=int, default=int(os.getenv("WORKERS", "4")))
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()

if __name__ == "__main__":
    import sys

    args = parse_args()
    try:
        run_screener(
            args.date,
            args.market,
            args.config,
            max(1, min(args.workers, MAX_WORKERS_HARD_CAP)),
            args.debug,
            pipeline_session=args.session,
        )
    except Exception as e:
        logger.critical("스크리너 치명적 오류: %s", e, exc_info=True)
        sys.exit(1)
