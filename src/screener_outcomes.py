"""Candidate outcome settlement for screener quality analysis.

Close-to-close forward returns on US trading days. Never mutates Production
DECISION / diagnostics / GPT artifacts. Trader does not read this module.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("screener_outcomes")

OUTCOME_PRICE_SOURCE = "close_to_close"
HORIZONS = (1, 3, 5, 10)

DEFAULT_QUALITY_POLICY = {
    "structural_min_days": 20,
    "outcome_min_1d": 30,
    "outcome_min_5d": 30,
    "outcome_min_10d": 20,
    "policy_review_min_5d": 40,
}

# Process-local OHLCV cache keyed by ticker (wide window); correctness-preserving.
_OHLCV_BY_TICKER: Dict[str, Any] = {}
_OHLCV_META: Dict[str, Tuple[str, str]] = {}  # ticker → (start, end) of cached window


def clear_ohlcv_cache() -> None:
    _OHLCV_BY_TICKER.clear()
    _OHLCV_META.clear()


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def normalize_trade_date(value: Any) -> Optional[str]:
    """Canonical YYYYMMDD from str/date/datetime/pandas timestamps."""
    if value is None:
        return None
    if isinstance(value, datetime):
        # Use wall-calendar date in the datetime's timezone (do not shift via UTC)
        return value.strftime("%Y%m%d")
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    # pandas Timestamp
    if hasattr(value, "to_pydatetime"):
        try:
            return normalize_trade_date(value.to_pydatetime())
        except Exception:
            pass
    if hasattr(value, "strftime") and not isinstance(value, str):
        try:
            return value.strftime("%Y%m%d")
        except Exception:
            pass
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    # ISO with time
    if "T" in s or " " in s:
        try:
            iso = s.replace("Z", "+00:00")
            return normalize_trade_date(datetime.fromisoformat(iso))
        except Exception:
            pass
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def ohlcv_to_closes_by_date(df: Any) -> Dict[str, Dict[str, Optional[float]]]:
    """Extract {YYYYMMDD: {close, high, low}} from KIS-normalized or indexed OHLCV.

    KIS path returns RangeIndex + Date column — must NOT treat the integer index
    as a calendar date.
    """
    out: Dict[str, Dict[str, Optional[float]]] = {}
    if df is None or getattr(df, "empty", True):
        return out

    cols = {str(c).strip().lower(): c for c in getattr(df, "columns", [])}
    date_col = None
    for cand in ("date", "xymd", "stck_bsop_date", "bsop_date"):
        if cand in cols:
            date_col = cols[cand]
            break
    close_col = cols.get("close")
    high_col = cols.get("high")
    low_col = cols.get("low")

    if date_col is not None and close_col is not None:
        for _, row in df.iterrows():
            d = normalize_trade_date(row.get(date_col) if hasattr(row, "get") else row[date_col])
            if not d:
                continue
            c = _safe_float(row.get(close_col) if hasattr(row, "get") else row[close_col])
            if c is None:
                continue
            out[d] = {
                "close": c,
                "high": _safe_float(row.get(high_col) if high_col and hasattr(row, "get") else (row[high_col] if high_col else None)),
                "low": _safe_float(row.get(low_col) if low_col and hasattr(row, "get") else (row[low_col] if low_col else None)),
            }
        return out

    # Fallback: DatetimeIndex / date-like index (pykrx/fdr style)
    if close_col is None:
        return out
    for idx, row in df.iterrows():
        d = normalize_trade_date(idx)
        if not d:
            continue
        c = _safe_float(row.get(close_col) if hasattr(row, "get") else row[close_col])
        if c is None:
            continue
        out[d] = {
            "close": c,
            "high": _safe_float(row.get(high_col) if high_col and hasattr(row, "get") else None),
            "low": _safe_float(row.get(low_col) if low_col and hasattr(row, "get") else None),
        }
    return out


def get_forward_trading_dates(
    trade_date: Any,
    n: int,
    *,
    price_index: Optional[Sequence[Any]] = None,
) -> List[str]:
    """Return the next *n* trading dates after trade_date from sorted OHLCV dates."""
    if n <= 0:
        return []
    td = normalize_trade_date(trade_date)
    if not td:
        return []
    if price_index is not None and len(price_index) > 0:
        dates: List[str] = []
        for raw in price_index:
            d = normalize_trade_date(raw)
            if not d:
                continue
            if d > td:
                dates.append(d)
            if len(dates) >= n:
                break
        return dates

    # Weekend-only fallback (tests / no price feed)
    out: List[str] = []
    dt = datetime.strptime(td, "%Y%m%d")
    while len(out) < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            out.append(dt.strftime("%Y%m%d"))
    return out


def calculate_forward_return(
    reference_price: float,
    future_price: Optional[float],
) -> Optional[float]:
    """(future / reference - 1) * 100."""
    ref = _safe_float(reference_price)
    fut = _safe_float(future_price)
    if ref is None or fut is None or ref == 0:
        return None
    return round((fut / ref - 1.0) * 100.0, 6)


def calculate_forward_max_drawdown(
    reference_price: float,
    forward_closes: Sequence[float],
    *,
    forward_lows: Optional[Sequence[Optional[float]]] = None,
) -> Optional[float]:
    """Max drawdown from reference across forward path, in percent (≤ 0)."""
    ref = _safe_float(reference_price)
    if ref is None or ref <= 0:
        return None
    closes = [c for c in (_safe_float(x) for x in forward_closes) if c is not None]
    if not closes:
        return None
    peak = ref
    mdd = 0.0
    for i, c in enumerate(closes):
        peak = max(peak, c)
        trough = c
        if forward_lows is not None and i < len(forward_lows):
            lo = _safe_float(forward_lows[i])
            if lo is not None:
                trough = lo
        if peak > 0:
            mdd = min(mdd, (trough - peak) / peak * 100.0)
    return round(mdd, 6)


def calculate_forward_max_runup(
    reference_price: float,
    forward_closes: Sequence[float],
    *,
    forward_highs: Optional[Sequence[Optional[float]]] = None,
) -> Optional[float]:
    ref = _safe_float(reference_price)
    if ref is None or ref <= 0:
        return None
    best = 0.0
    n = len(forward_closes)
    if n == 0 and not forward_highs:
        return None
    for i in range(max(n, len(forward_highs or []))):
        hi = None
        if forward_highs and i < len(forward_highs):
            hi = _safe_float(forward_highs[i])
        if hi is None and i < n:
            hi = _safe_float(forward_closes[i])
        if hi is None:
            continue
        best = max(best, (hi / ref - 1.0) * 100.0)
    return round(best, 6)


def _load_ohlcv_frame(
    ticker: str,
    trade_date: str,
    *,
    lookforward_calendar_days: int = 45,
    lookback_calendar_days: int = 10,
    price_loader: Optional[Callable[..., Any]] = None,
    use_cache: bool = True,
    fetch_start: Optional[str] = None,
    fetch_end: Optional[str] = None,
) -> Optional[Any]:
    td = normalize_trade_date(trade_date)
    if not td and not (fetch_start and fetch_end):
        return None
    tkey = str(ticker).upper()
    if fetch_start and fetch_end:
        start_s = normalize_trade_date(fetch_start) or fetch_start
        end_s = normalize_trade_date(fetch_end) or fetch_end
    else:
        assert td is not None
        start = datetime.strptime(td, "%Y%m%d") - timedelta(days=lookback_calendar_days)
        end = datetime.strptime(td, "%Y%m%d") + timedelta(days=lookforward_calendar_days)
        today = datetime.utcnow().date()
        end_d = min(end.date(), today + timedelta(days=1))
        start_s = start.strftime("%Y%m%d")
        end_s = end_d.strftime("%Y%m%d")

    if use_cache and tkey in _OHLCV_BY_TICKER:
        return _OHLCV_BY_TICKER[tkey]

    loader = price_loader
    if loader is None:
        try:
            from screener_core import get_historical_prices

            loader = get_historical_prices
        except Exception as e:
            logger.debug("price loader unavailable: %s", e)
            return None
    try:
        df = loader(ticker, start_s, end_s)
        if df is None or getattr(df, "empty", True):
            if use_cache:
                _OHLCV_BY_TICKER[tkey] = None
                _OHLCV_META[tkey] = (start_s, end_s)
            return None
        if use_cache:
            # Merge with existing cache if present
            prev = _OHLCV_BY_TICKER.get(tkey)
            if prev is not None and not getattr(prev, "empty", True):
                try:
                    import pandas as pd

                    df = pd.concat([prev, df], ignore_index=True)
                    if "Date" in df.columns:
                        df = df.drop_duplicates(subset=["Date"], keep="last")
                        df = df.sort_values("Date").reset_index(drop=True)
                except Exception:
                    pass
            _OHLCV_BY_TICKER[tkey] = df
            prev_meta = _OHLCV_META.get(tkey)
            if prev_meta:
                _OHLCV_META[tkey] = (min(prev_meta[0], start_s), max(prev_meta[1], end_s))
            else:
                _OHLCV_META[tkey] = (start_s, end_s)
        return df
    except Exception as e:
        logger.debug("OHLCV load failed %s %s: %s", ticker, td, e)
        return None


def prefetch_ohlcv_for_observations(
    observations: Sequence[Dict[str, Any]],
    *,
    price_loader: Optional[Callable[..., Any]] = None,
    as_of_trade_date: Optional[str] = None,
) -> None:
    """One (or few) KIS fetch(es) per ticker covering all observation dates."""
    by_ticker: Dict[str, List[str]] = {}
    for o in observations:
        t = str(o.get("ticker") or "").upper()
        td = normalize_trade_date(o.get("trade_date"))
        if t and td:
            by_ticker.setdefault(t, []).append(td)
    as_of = normalize_trade_date(as_of_trade_date)
    today = datetime.utcnow().strftime("%Y%m%d")
    end_cap = as_of or today
    for t, dates in by_ticker.items():
        d0, d1 = min(dates), max(dates)
        start = (datetime.strptime(d0, "%Y%m%d") - timedelta(days=10)).strftime("%Y%m%d")
        # Enough calendar span for 10 trading days past the latest needed end
        end = (
            datetime.strptime(max(d1, end_cap), "%Y%m%d") + timedelta(days=25)
        ).strftime("%Y%m%d")
        _load_ohlcv_frame(
            t,
            d0,
            price_loader=price_loader,
            use_cache=True,
            fetch_start=start,
            fetch_end=end,
        )


def _load_close_series(
    ticker: str,
    trade_date: str,
    *,
    lookforward_calendar_days: int = 45,
    price_loader: Optional[Callable[..., Any]] = None,
) -> Optional[Any]:
    """Back-compat alias used by tests."""
    return _load_ohlcv_frame(
        ticker,
        trade_date,
        lookforward_calendar_days=lookforward_calendar_days,
        price_loader=price_loader,
    )


def _outcome_status_from_maturity(maturity: Dict[str, bool]) -> str:
    vals = [bool(maturity.get(k)) for k in ("1d", "3d", "5d", "10d")]
    if all(vals):
        return "FULLY_MATURED"
    if any(vals):
        return "PARTIALLY_MATURED"
    return "PENDING"


def settle_observation_outcome(
    obs: Dict[str, Any],
    *,
    as_of_trade_date: Optional[str] = None,
    price_loader: Optional[Callable[..., Any]] = None,
    close_series: Optional[Any] = None,
    debug: bool = False,
) -> Dict[str, Any]:
    """Settle forward returns for one observation (idempotent field merge)."""
    out = dict(obs)
    ticker = str(out.get("ticker") or "").upper()
    trade_date = normalize_trade_date(out.get("trade_date"))
    as_of = normalize_trade_date(as_of_trade_date) if as_of_trade_date else None
    debug_info: Dict[str, Any] = {}

    if not ticker or not trade_date:
        out["outcome_status"] = "DATA_UNAVAILABLE"
        out["maturity"] = {"1d": False, "3d": False, "5d": False, "10d": False}
        return out

    out["trade_date"] = trade_date
    ref = _safe_float(out.get("reference_price"))
    if ref is None:
        ref = _safe_float(out.get("decision_price"))
    ref_source = "OBSERVATION" if ref is not None else None

    df = close_series
    if df is None:
        df = _load_ohlcv_frame(ticker, trade_date, price_loader=price_loader)

    bars = ohlcv_to_closes_by_date(df) if df is not None else {}
    sorted_dates = sorted(bars.keys())
    debug_info.update(
        {
            "ticker": ticker,
            "trade_date": trade_date,
            "ohlcv_rows": len(sorted_dates),
            "ohlcv_first_date": sorted_dates[0] if sorted_dates else None,
            "ohlcv_last_date": sorted_dates[-1] if sorted_dates else None,
            "candidate_date_present": trade_date in bars,
        }
    )

    if not bars:
        out.setdefault("outcome_status", "PENDING")
        out["maturity"] = {"1d": False, "3d": False, "5d": False, "10d": False}
        out["settlement_note"] = "OHLCV_EMPTY_OR_UNPARSED"
        if debug:
            out["_settlement_debug"] = debug_info
        return out

    if ref is None:
        if trade_date in bars:
            ref = bars[trade_date]["close"]
            ref_source = "OHLCV_CLOSE_BACKFILL"
            out["reference_price_date"] = trade_date
        else:
            prior = [d for d in sorted_dates if d <= trade_date]
            if prior:
                ref = bars[prior[-1]]["close"]
                ref_source = "OHLCV_CLOSE_BACKFILL"
                out["reference_price_date"] = prior[-1]
    if ref is None:
        out["outcome_status"] = "DATA_UNAVAILABLE"
        out["maturity"] = {"1d": False, "3d": False, "5d": False, "10d": False}
        out["settlement_note"] = "REFERENCE_PRICE_UNAVAILABLE"
        if debug:
            out["_settlement_debug"] = debug_info
        return out

    out["reference_price"] = ref
    out.setdefault("reference_price_date", trade_date)
    out["reference_price_source"] = ref_source or "OBSERVATION"
    out["outcome_price_source"] = OUTCOME_PRICE_SOURCE

    # Reference index in sorted trading calendar
    if trade_date in bars:
        ref_idx = sorted_dates.index(trade_date)
    else:
        prior = [d for d in sorted_dates if d <= trade_date]
        ref_idx = sorted_dates.index(prior[-1]) if prior else -1

    forward_dates = get_forward_trading_dates(trade_date, 10, price_index=sorted_dates)
    last_available = sorted_dates[-1]
    if as_of:
        last_available = min(last_available, as_of)

    debug_info.update(
        {
            "reference_index": ref_idx,
            "reference_close": ref,
            "1D_target_date": forward_dates[0] if len(forward_dates) >= 1 else None,
            "3D_target_date": forward_dates[2] if len(forward_dates) >= 3 else None,
            "5D_target_date": forward_dates[4] if len(forward_dates) >= 5 else None,
            "10D_target_date": forward_dates[9] if len(forward_dates) >= 10 else None,
            "last_available": last_available,
        }
    )

    maturity = {"1d": False, "3d": False, "5d": False, "10d": False}
    forward_closes: List[float] = []
    forward_highs: List[Optional[float]] = []
    forward_lows: List[Optional[float]] = []

    for h in HORIZONS:
        key_r = f"return_{h}d_pct"
        if len(forward_dates) < h:
            out[key_r] = None
            continue
        fd = forward_dates[h - 1]
        if fd > last_available:
            out[key_r] = None
            continue
        bar = bars.get(fd)
        fp = bar["close"] if bar else None
        out[key_r] = calculate_forward_return(ref, fp)
        if out[key_r] is not None:
            maturity[f"{h}d"] = True
            if h == 1:
                debug_info["return_1d"] = out[key_r]
            elif h == 3:
                debug_info["return_3d"] = out[key_r]
            elif h == 5:
                debug_info["return_5d"] = out[key_r]

    for fd in forward_dates:
        if fd > last_available:
            break
        bar = bars.get(fd)
        if not bar or bar.get("close") is None:
            continue
        forward_closes.append(float(bar["close"]))
        forward_highs.append(bar.get("high"))
        forward_lows.append(bar.get("low"))

    for h in (5, 10):
        if len(forward_closes) >= h:
            path_c = forward_closes[:h]
            path_h = forward_highs[:h]
            path_l = forward_lows[:h]
            # Returns always; MDD/runup need path — use closes if HL missing
            has_hl = any(v is not None for v in path_h) or any(v is not None for v in path_l)
            out[f"max_drawdown_{h}d_pct"] = calculate_forward_max_drawdown(
                ref, path_c, forward_lows=path_l if has_hl else None
            )
            out[f"max_runup_{h}d_pct"] = calculate_forward_max_runup(
                ref, path_c, forward_highs=path_h if has_hl else None
            )
        else:
            out.setdefault(f"max_drawdown_{h}d_pct", None)
            out.setdefault(f"max_runup_{h}d_pct", None)

    out["maturity"] = maturity
    out["outcome_status"] = _outcome_status_from_maturity(maturity)
    debug_info["maturity"] = dict(maturity)
    debug_info["outcome_status"] = out["outcome_status"]
    if debug:
        out["_settlement_debug"] = debug_info
        logger.info(
            "[SETTLEMENT_DEBUG] %s",
            {k: debug_info[k] for k in debug_info if not k.startswith("_")},
        )
    return out


def settle_candidate_outcomes(
    observations: Sequence[Dict[str, Any]],
    *,
    as_of_trade_date: Optional[str] = None,
    price_loader: Optional[Callable[..., Any]] = None,
    use_cache: bool = True,
) -> List[Dict[str, Any]]:
    if use_cache:
        prefetch_ohlcv_for_observations(
            observations,
            price_loader=price_loader,
            as_of_trade_date=as_of_trade_date,
        )

    return [
        settle_observation_outcome(
            o, as_of_trade_date=as_of_trade_date, price_loader=price_loader
        )
        for o in observations
    ]


def backfill_candidate_outcomes(
    ledger_rows: Sequence[Dict[str, Any]],
    *,
    as_of_trade_date: Optional[str] = None,
    price_loader: Optional[Callable[..., Any]] = None,
    only_trusted: bool = True,
    use_cache: bool = True,
) -> List[Dict[str, Any]]:
    """Settle outcomes for ledger rows; skip untrusted when only_trusted."""
    to_settle: List[Dict[str, Any]] = []
    skipped: List[Tuple[int, Dict[str, Any]]] = []
    for i, row in enumerate(ledger_rows):
        if only_trusted and row.get("trusted_for_analysis") is False:
            skipped.append((i, dict(row)))
            continue
        to_settle.append(dict(row))

    settled_list = settle_candidate_outcomes(
        to_settle,
        as_of_trade_date=as_of_trade_date,
        price_loader=price_loader,
        use_cache=use_cache,
    )

    # Reconstruct original order
    out: List[Dict[str, Any]] = []
    sit = iter(settled_list)
    skip_map = {i: r for i, r in skipped}
    settle_i = 0
    for i in range(len(ledger_rows)):
        if i in skip_map:
            out.append(skip_map[i])
        else:
            out.append(next(sit))
            settle_i += 1
    return out


def debug_settle_one(
    obs: Dict[str, Any],
    *,
    as_of_trade_date: Optional[str] = None,
    price_loader: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Settle a single observation with verbose debug payload."""
    return settle_observation_outcome(
        obs,
        as_of_trade_date=as_of_trade_date,
        price_loader=price_loader,
        debug=True,
    )


def classify_sample_statuses(
    *,
    trading_days: int,
    matured_1d: int,
    matured_5d: int,
    matured_10d: int,
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    p = {**DEFAULT_QUALITY_POLICY, **(policy or {})}
    if trading_days <= 0:
        structural = "NO_DATA"
    elif trading_days < int(p["structural_min_days"]):
        structural = "INSUFFICIENT_SAMPLE"
    else:
        structural = "ADEQUATE_SAMPLE"

    if matured_1d <= 0 and matured_5d <= 0:
        outcome = "NO_SETTLED_OUTCOMES"
    elif matured_5d >= int(p["outcome_min_5d"]):
        if matured_10d >= int(p["outcome_min_10d"]):
            outcome = "ADEQUATE_10D_SAMPLE"
        else:
            outcome = "ADEQUATE_5D_SAMPLE"
    elif matured_1d >= int(p["outcome_min_1d"]):
        outcome = "ADEQUATE_1D_SAMPLE"
    else:
        outcome = "INSUFFICIENT_OUTCOMES"

    if structural == "NO_DATA":
        policy_status = "DO_NOT_CHANGE"
    elif outcome in ("NO_SETTLED_OUTCOMES", "INSUFFICIENT_OUTCOMES", "ADEQUATE_1D_SAMPLE"):
        policy_status = "DO_NOT_CHANGE" if outcome == "NO_SETTLED_OUTCOMES" else "CONTINUE_OBSERVATION"
    elif matured_5d >= int(p["policy_review_min_5d"]):
        policy_status = "READY_FOR_REVIEW"
    else:
        policy_status = "CONTINUE_OBSERVATION"

    return {
        "structural_sample_status": structural,
        "outcome_sample_status": outcome,
        "policy_change_status": policy_status,
    }


def spearman_corr(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    n = len(xs)

    def _ranks(vals: Sequence[float]) -> List[float]:
        order = sorted(range(n), key=lambda i: vals[i])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and vals[order[j + 1]] == vals[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                ranks[order[k]] = avg
            i = j + 1
        return ranks

    try:
        rx, ry = _ranks(xs), _ranks(ys)
        mx = sum(rx) / n
        my = sum(ry) / n
        num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
        denx = sum((a - mx) ** 2 for a in rx) ** 0.5
        deny = sum((b - my) ** 2 for b in ry) ** 0.5
        if denx == 0 or deny == 0:
            return None
        return float(num / (denx * deny))
    except Exception:
        return None


def summarize_outcome_group(
    rows: Sequence[Dict[str, Any]],
    *,
    horizon: int = 5,
) -> Dict[str, Any]:
    key = f"return_{horizon}d_pct"
    mdd_key = f"max_drawdown_{horizon}d_pct"
    runup_key = f"max_runup_{horizon}d_pct"
    vals = [_safe_float(r.get(key)) for r in rows]
    matured = [v for v in vals if v is not None]
    tickers = {str(r.get("ticker") or "").upper() for r in rows if r.get("ticker")}
    n = len(rows)
    nm = len(matured)
    if nm == 0:
        return {
            "observations": n,
            "matured_observations": 0,
            "unique_tickers": len(tickers),
            "mean_return": None,
            "median_return": None,
            "win_rate": None,
            "p25": None,
            "p75": None,
            "mean_max_drawdown": None,
            "mean_max_runup": None,
            "ticker_equal_weight_mean_return": None,
            "positive_tail": None,
            "negative_tail": None,
        }
    matured_sorted = sorted(matured)
    wins = sum(1 for v in matured if v > 0)
    mdds = [
        _safe_float(r.get(mdd_key))
        for r in rows
        if _safe_float(r.get(key)) is not None
    ]
    mdds_ok = [v for v in mdds if v is not None]
    runups = [
        _safe_float(r.get(runup_key))
        for r in rows
        if _safe_float(r.get(key)) is not None
    ]
    runups_ok = [v for v in runups if v is not None]
    by_ticker: Dict[str, List[float]] = {}
    for r in rows:
        t = str(r.get("ticker") or "").upper()
        v = _safe_float(r.get(key))
        if t and v is not None:
            by_ticker.setdefault(t, []).append(v)
    tmeans = [sum(vs) / len(vs) for vs in by_ticker.values()] if by_ticker else []
    return {
        "observations": n,
        "matured_observations": nm,
        "unique_tickers": len(tickers),
        "mean_return": round(sum(matured) / nm, 6),
        "median_return": matured_sorted[nm // 2],
        "win_rate": round(wins / nm, 6),
        "p25": matured_sorted[max(0, nm // 4)],
        "p75": matured_sorted[min(nm - 1, (3 * nm) // 4)],
        "mean_max_drawdown": (
            round(sum(mdds_ok) / len(mdds_ok), 6) if mdds_ok else None
        ),
        "mean_max_runup": (
            round(sum(runups_ok) / len(runups_ok), 6) if runups_ok else None
        ),
        "ticker_equal_weight_mean_return": (
            round(sum(tmeans) / len(tmeans), 6) if tmeans else None
        ),
        "positive_tail": matured_sorted[-1],
        "negative_tail": matured_sorted[0],
    }


def score_calibration_buckets(
    rows: Sequence[Dict[str, Any]],
    *,
    horizon: int = 5,
    edges: Optional[Sequence[float]] = None,
) -> List[Dict[str, Any]]:
    bounds = list(edges or (0.42, 0.46, 0.48, 0.52, 0.56, 0.60, 2.0))
    buckets: List[Dict[str, Any]] = []
    for i in range(len(bounds) - 1):
        lo, hi = bounds[i], bounds[i + 1]
        label = f"{lo:.2f}~{hi:.2f}" if hi < 1.5 else f"{lo:.2f}+"
        group = []
        for r in rows:
            s = _safe_float(r.get("decision_score") or r.get("score"))
            if s is None:
                continue
            if (s >= lo) and (s < hi or (hi >= 1.5 and s >= lo)):
                group.append(r)
        stats = summarize_outcome_group(group, horizon=horizon)
        stats["bucket"] = label
        stats["n"] = stats["matured_observations"]
        buckets.append(stats)
    return buckets
