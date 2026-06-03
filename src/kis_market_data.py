# src/kis_market_data.py
"""KIS Open API 기반 OHLCV 조회 (RSI·손절/목표·ATR 등)."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils import get_us_regime_config, is_us_market, norm_ticker, resolve_us_excd

logger = logging.getLogger(__name__)

_kis_singleton: Any = None
_LAST_US_REGIME_META: Dict[str, Any] = {}


def get_last_us_regime_meta() -> Dict[str, Any]:
    """직전 get_us_regime_ohlcv() fetch 메타 (market_state sidecar용)."""
    return dict(_LAST_US_REGIME_META)

# KIS raw column → 표준 OHLCV
_OHLCV_MAP: Dict[str, List[str]] = {
    "date": [
        "xymd",
        "stck_bsop_date",
        "bsop_date",
        "date",
    ],
    "open": ["open", "stck_oprc", "oprc", "ovrs_nmix_oprc"],
    "high": ["high", "stck_hgpr", "hgpr", "ovrs_nmix_hgpr"],
    "low": ["low", "stck_lwpr", "lwpr", "ovrs_nmix_lwpr"],
    "close": ["clos", "close", "stck_clpr", "clpr", "ovrs_nmix_prpr", "종가"],
    "volume": ["tvol", "acml_vol", "volume", "거래량"],
}


def _pick_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    cols = {str(c).strip().lower(): c for c in columns}
    for name in candidates:
        key = name.lower()
        if key in cols:
            return cols[key]
    return None


def normalize_kis_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """KIS 국내/해외 일봉 응답 → Open, High, Low, Close, Volume (오름차순)."""
    if df is None or df.empty:
        return pd.DataFrame()

    date_col = _pick_col(list(df.columns), _OHLCV_MAP["date"])
    open_col = _pick_col(list(df.columns), _OHLCV_MAP["open"])
    high_col = _pick_col(list(df.columns), _OHLCV_MAP["high"])
    low_col = _pick_col(list(df.columns), _OHLCV_MAP["low"])
    close_col = _pick_col(list(df.columns), _OHLCV_MAP["close"])
    vol_col = _pick_col(list(df.columns), _OHLCV_MAP["volume"])

    if not close_col:
        logger.debug("KIS OHLCV: 종가 컬럼 없음 cols=%s", list(df.columns))
        return pd.DataFrame()

    out = pd.DataFrame()
    if date_col:
        out["Date"] = (
            df[date_col].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
        )
    else:
        out["Date"] = pd.RangeIndex(len(df)).astype(str)

    def _num(col: Optional[str], default: float = 0.0) -> pd.Series:
        if not col:
            return pd.Series([default] * len(df))
        return pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        ).fillna(default)

    out["Open"] = _num(open_col)
    out["High"] = _num(high_col)
    out["Low"] = _num(low_col)
    out["Close"] = _num(close_col)
    out["Volume"] = _num(vol_col)

    out = out[out["Close"] > 0]
    out = out.drop_duplicates(subset=["Date"], keep="last")
    out = out.sort_values("Date").reset_index(drop=True)
    return out


def get_kis_client(kis: Any = None) -> Any:
    """공유 KIS 인스턴스 (없으면 env/config로 1회 생성)."""
    global _kis_singleton
    if kis is not None:
        return kis
    if _kis_singleton is not None:
        return _kis_singleton
    try:
        from api.kis_auth import KIS

        env = os.getenv("KIS_ENV", "prod")
        cfg: dict = {}
        try:
            from settings import Settings

            s = Settings()
            cfg = getattr(s, "_config", {}) or {}
            env = cfg.get("trading_environment", env)
        except Exception:
            pass
        broker_cfg = cfg.get("kis_broker", {}) if cfg else {}
        _kis_singleton = KIS(config=broker_cfg, env=env)
        return _kis_singleton
    except Exception as e:
        logger.debug("KIS 클라이언트 생성 실패: %s", e)
        return None


def _fetch_overseas_daily_pages(
    kis: Any,
    symb: str,
    excd: str,
    start_date: str,
    end_date: str,
    *,
    max_pages: int = 12,
) -> pd.DataFrame:
    """해외 일봉(HHDFS76240000) — BYMD 페이지네이션."""
    chunks: List[pd.DataFrame] = []
    bymd = end_date
    prev_oldest: Optional[str] = None

    for _ in range(max(1, max_pages)):
        raw = kis.overseas_daily_price(excd, symb, bymd=bymd, gubn="0", modp="0")
        if raw is None or raw.empty:
            break
        chunks.append(raw)

        if "xymd" not in raw.columns:
            break
        oldest = str(raw["xymd"].astype(str).min())
        if oldest <= start_date or oldest == prev_oldest:
            break
        prev_oldest = oldest
        try:
            bymd = (datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
        except Exception:
            break
        time.sleep(0.05)

    if not chunks:
        return pd.DataFrame()
    merged = pd.concat(chunks, ignore_index=True)
    norm = normalize_kis_ohlcv(merged)
    if norm.empty:
        return norm
    mask = (norm["Date"] >= start_date) & (norm["Date"] <= end_date)
    return norm.loc[mask].reset_index(drop=True)


def _iter_date_chunks(
    start_date: str,
    end_date: str,
    *,
    chunk_days: int = 120,
) -> List[Tuple[str, str]]:
    """YYYYMMDD 구간을 chunk_days 단위로 분할 (과거→현재)."""
    try:
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
    except ValueError:
        return [(start_date, end_date)]
    if start > end:
        start, end = end, start
    out: List[Tuple[str, str]] = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        out.append((cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        cur = chunk_end + timedelta(days=1)
    return out


def _fetch_overseas_index_daily(
    kis: Any,
    market_code: str,
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    chunk_days: int = 120,
) -> pd.DataFrame:
    """해외지수 일봉(FHKST03030100) — 기간 청크 병합."""
    chunks: List[pd.DataFrame] = []
    sym = str(symbol or "").strip().upper()
    mc = str(market_code or "N").strip().upper()

    for d1, d2 in _iter_date_chunks(start_date, end_date, chunk_days=chunk_days):
        raw = kis.overseas_daily_chart_price(mc, sym, d1, d2, period="D")
        if raw is not None and not raw.empty:
            chunks.append(raw)
        time.sleep(0.05)

    if not chunks:
        return pd.DataFrame()
    merged = pd.concat(chunks, ignore_index=True)
    norm = normalize_kis_ohlcv(merged)
    if norm.empty:
        return norm
    mask = (norm["Date"] >= start_date) & (norm["Date"] <= end_date)
    return norm.loc[mask].reset_index(drop=True)


def get_us_regime_ohlcv(
    start_date: str,
    end_date: str,
    *,
    kis: Any = None,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    US 시장 레짐용 OHLCV (KIS 해외지수 일봉 우선, 실패 시 ETF dailyprice 폴백).

    Returns:
        (DataFrame with Open/High/Low/Close/Volume or None, fetch metadata)
    """
    rc = dict(config or get_us_regime_config())
    min_bars = int(rc.get("min_bars") or 200)
    meta: Dict[str, Any] = {
        "benchmark_source": None,
        "benchmark_symbol": None,
        "benchmark_market_code": None,
        "benchmark_bars": 0,
    }

    client = get_kis_client(kis)
    if client is None:
        return None, meta

    source = str(rc.get("source") or "index").strip().lower()
    df = pd.DataFrame()

    if source == "index":
        mc = str(rc.get("index_market_code") or "N").strip().upper()
        sym = str(rc.get("index_symbol") or "SPX").strip().upper()
        meta["benchmark_market_code"] = mc
        meta["benchmark_symbol"] = sym
        try:
            df = _fetch_overseas_index_daily(client, mc, sym, start_date, end_date)
            if df is not None and not df.empty:
                meta["benchmark_source"] = "kis_index"
                meta["benchmark_bars"] = len(df)
        except Exception as e:
            logger.debug("US regime index OHLCV 실패 %s@%s: %s", sym, mc, e)

    fb_cfg = rc.get("etf_fallback") if isinstance(rc.get("etf_fallback"), dict) else {}
    need_fallback = (
        bool(fb_cfg.get("enabled"))
        and (df is None or df.empty or len(df) < min_bars)
    )
    if need_fallback:
        sym_fb = str(fb_cfg.get("symbol") or "SPY").strip().upper()
        excd_fb = str(fb_cfg.get("excd") or "AMS").strip().upper()
        try:
            df_fb = _fetch_overseas_daily_pages(
                client, sym_fb, excd_fb, start_date, end_date
            )
            if df_fb is not None and not df_fb.empty and len(df_fb) >= len(df):
                df = df_fb
                meta["benchmark_source"] = "kis_etf"
                meta["benchmark_symbol"] = sym_fb
                meta["benchmark_market_code"] = excd_fb
                meta["benchmark_bars"] = len(df)
                logger.info(
                    "US regime ETF 폴백 %s@%s rows=%d",
                    sym_fb,
                    excd_fb,
                    len(df),
                )
        except Exception as e:
            logger.debug("US regime ETF 폴백 실패 %s@%s: %s", sym_fb, excd_fb, e)

    if df is not None and not df.empty and meta.get("benchmark_source") == "kis_index":
        logger.info(
            "US regime index OHLCV %s@%s rows=%d",
            meta.get("benchmark_symbol"),
            meta.get("benchmark_market_code"),
            len(df),
        )

    global _LAST_US_REGIME_META
    _LAST_US_REGIME_META = dict(meta)
    if df is None or df.empty:
        logger.warning(
            "US regime OHLCV 없음 (%s~%s, index=%s, fallback=%s)",
            start_date,
            end_date,
            meta.get("benchmark_symbol"),
            bool(fb_cfg.get("enabled")),
        )
        return None, meta
    return df, meta


def _fetch_domestic_period(
    kis: Any,
    code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """국내 기간별 일봉(FHKST03010100)."""
    raw = kis.inquire_period_price(
        fid_cond_mrkt_div_code="J",
        fid_input_iscd=code,
        fid_input_date_1=start_date,
        fid_input_date_2=end_date,
        fid_period_div_code="D",
        fid_org_adj_prc="0",
    )
    return normalize_kis_ohlcv(raw)


def get_historical_prices_kis(
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    market: Optional[str] = None,
    kis: Any = None,
    retries: int = 3,
) -> Optional[pd.DataFrame]:
    """
    KIS API로 OHLCV 조회.
    - US: overseas_daily_price (HHDFS76240000)
    - KR: inquire_period_price (FHKST03010100)
    """
    sym = str(symbol or "").strip()
    if not sym:
        return None

    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()
    client = get_kis_client(kis)
    if client is None:
        return None

    code = norm_ticker(sym, mkt)
    if not is_us_market(mkt):
        code = str(code).zfill(6)

    last_err: Optional[Exception] = None
    for attempt in range(max(1, retries)):
        try:
            if is_us_market(mkt):
                excd = resolve_us_excd(code, mkt)
                df = _fetch_overseas_daily_pages(
                    client, code, excd, start_date, end_date
                )
            else:
                df = _fetch_domestic_period(client, code, start_date, end_date)
            if df is not None and not df.empty:
                logger.debug(
                    "KIS OHLCV %s %s~%s rows=%d",
                    code,
                    start_date,
                    end_date,
                    len(df),
                )
                return df
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(0.3 * (attempt + 1))
    if last_err:
        logger.debug("KIS OHLCV 실패 %s: %s", code, last_err)
    return None
