"""Candidate outcome settlement for screener quality analysis.

Close-to-close forward returns on US trading days. Never mutates Production
DECISION / diagnostics / GPT artifacts. Trader does not read this module.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
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


def get_forward_trading_dates(
    trade_date: str,
    n: int,
    *,
    price_index: Optional[Sequence[Any]] = None,
) -> List[str]:
    """Return the next *n* trading dates after trade_date.

    Prefer the actual price series index (holiday-aware). Fallback walks calendar
    days excluding weekends only when no index is provided.
    """
    if n <= 0:
        return []
    td = str(trade_date).strip()
    if price_index is not None and len(price_index) > 0:
        dates: List[str] = []
        for raw in price_index:
            try:
                if hasattr(raw, "strftime"):
                    d = raw.strftime("%Y%m%d")
                else:
                    d = str(raw).replace("-", "")[:8]
            except Exception:
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
) -> Optional[float]:
    """Max drawdown from reference across forward closes, in percent (≤ 0)."""
    ref = _safe_float(reference_price)
    if ref is None or ref <= 0:
        return None
    closes = [c for c in (_safe_float(x) for x in forward_closes) if c is not None]
    if not closes:
        return None
    peak = ref
    mdd = 0.0
    for p in closes:
        peak = max(peak, p)
        if peak > 0:
            mdd = min(mdd, (p - peak) / peak * 100.0)
    return round(mdd, 6)


def calculate_forward_max_runup(
    reference_price: float,
    forward_closes: Sequence[float],
) -> Optional[float]:
    ref = _safe_float(reference_price)
    if ref is None or ref <= 0:
        return None
    closes = [c for c in (_safe_float(x) for x in forward_closes) if c is not None]
    if not closes:
        return None
    best = 0.0
    for p in closes:
        best = max(best, (p / ref - 1.0) * 100.0)
    return round(best, 6)


def _load_close_series(
    ticker: str,
    trade_date: str,
    *,
    lookforward_calendar_days: int = 40,
    price_loader: Optional[Callable[..., Any]] = None,
) -> Optional[Any]:
    loader = price_loader
    if loader is None:
        try:
            from screener_core import get_historical_prices

            loader = get_historical_prices
        except Exception as e:
            logger.debug("price loader unavailable: %s", e)
            return None
    try:
        start = datetime.strptime(trade_date, "%Y%m%d")
        end = start + timedelta(days=lookforward_calendar_days)
        # Include trade_date itself for reference close
        start_s = (start - timedelta(days=5)).strftime("%Y%m%d")
        end_s = end.strftime("%Y%m%d")
        df = loader(ticker, start_s, end_s)
        if df is None or getattr(df, "empty", True):
            return None
        if "Close" not in df.columns:
            # try lowercase
            cols = {c.lower(): c for c in df.columns}
            if "close" not in cols:
                return None
            df = df.rename(columns={cols["close"]: "Close"})
        return df
    except Exception as e:
        logger.debug("close series load failed %s %s: %s", ticker, trade_date, e)
        return None


def settle_observation_outcome(
    obs: Dict[str, Any],
    *,
    as_of_trade_date: Optional[str] = None,
    price_loader: Optional[Callable[..., Any]] = None,
    close_series: Optional[Any] = None,
) -> Dict[str, Any]:
    """Settle forward returns for one observation (idempotent field merge)."""
    out = dict(obs)
    ticker = str(out.get("ticker") or "").upper()
    trade_date = str(out.get("trade_date") or "")
    if not ticker or not trade_date:
        out["outcome_status"] = "DATA_UNAVAILABLE"
        return out

    ref = _safe_float(out.get("reference_price") or out.get("decision_price"))
    df = close_series
    if df is None:
        df = _load_close_series(ticker, trade_date, price_loader=price_loader)
    if df is None or getattr(df, "empty", True):
        out.setdefault("outcome_status", "PENDING")
        out.setdefault(
            "maturity",
            {"1d": False, "3d": False, "5d": False, "10d": False},
        )
        return out

    # Normalize index to YYYYMMDD strings
    closes_by_date: Dict[str, float] = {}
    for idx, row in df.iterrows():
        try:
            if hasattr(idx, "strftime"):
                d = idx.strftime("%Y%m%d")
            else:
                d = str(idx).replace("-", "")[:8]
            c = _safe_float(row.get("Close") if hasattr(row, "get") else row["Close"])
            if c is not None:
                closes_by_date[d] = c
        except Exception:
            continue

    sorted_dates = sorted(closes_by_date.keys())
    if ref is None:
        # Use trade_date close as reference when available
        if trade_date in closes_by_date:
            ref = closes_by_date[trade_date]
        else:
            # nearest on-or-before
            prior = [d for d in sorted_dates if d <= trade_date]
            if prior:
                ref = closes_by_date[prior[-1]]
                out["reference_price_date"] = prior[-1]
    if ref is None:
        out["outcome_status"] = "DATA_UNAVAILABLE"
        return out

    out["reference_price"] = ref
    out.setdefault("reference_price_date", trade_date)
    out["outcome_price_source"] = OUTCOME_PRICE_SOURCE

    forward_dates = get_forward_trading_dates(trade_date, 10, price_index=sorted_dates)
    # Cap by as_of if provided (no future leakage beyond known last bar)
    last_available = sorted_dates[-1] if sorted_dates else trade_date
    if as_of_trade_date:
        last_available = min(last_available, str(as_of_trade_date))

    maturity = {"1d": False, "3d": False, "5d": False, "10d": False}
    forward_closes_accum: List[float] = []

    for h in HORIZONS:
        key_r = f"return_{h}d_pct"
        if len(forward_dates) < h:
            out[key_r] = None
            continue
        fd = forward_dates[h - 1]
        if fd > last_available:
            out[key_r] = None
            continue
        fp = closes_by_date.get(fd)
        out[key_r] = calculate_forward_return(ref, fp)
        if out[key_r] is not None:
            maturity[f"{h}d"] = True

    # Build path closes for MDD/runup up to available horizon
    for fd in forward_dates:
        if fd > last_available:
            break
        c = closes_by_date.get(fd)
        if c is not None:
            forward_closes_accum.append(c)

    for h in (5, 10):
        path = forward_closes_accum[:h] if len(forward_closes_accum) >= h else []
        if len(path) >= h:
            out[f"max_drawdown_{h}d_pct"] = calculate_forward_max_drawdown(ref, path)
            out[f"max_runup_{h}d_pct"] = calculate_forward_max_runup(ref, path)
        else:
            out.setdefault(f"max_drawdown_{h}d_pct", None)
            out.setdefault(f"max_runup_{h}d_pct", None)

    out["maturity"] = maturity
    if maturity["10d"]:
        out["outcome_status"] = "PARTIALLY_MATURED" if not all(maturity.values()) else "PARTIALLY_MATURED"
        # All primary horizons matured
        if maturity["1d"] and maturity["3d"] and maturity["5d"] and maturity["10d"]:
            out["outcome_status"] = "MATURED"
        else:
            out["outcome_status"] = "PARTIALLY_MATURED"
    elif maturity["1d"] or maturity["3d"] or maturity["5d"]:
        out["outcome_status"] = "PARTIALLY_MATURED"
    else:
        out["outcome_status"] = "PENDING"
    return out


def settle_candidate_outcomes(
    observations: Sequence[Dict[str, Any]],
    *,
    as_of_trade_date: Optional[str] = None,
    price_loader: Optional[Callable[..., Any]] = None,
) -> List[Dict[str, Any]]:
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
) -> List[Dict[str, Any]]:
    """Settle outcomes for ledger rows; skip untrusted when only_trusted."""
    out: List[Dict[str, Any]] = []
    for row in ledger_rows:
        if only_trusted and row.get("trusted_for_analysis") is False:
            out.append(dict(row))
            continue
        out.append(
            settle_observation_outcome(
                row, as_of_trade_date=as_of_trade_date, price_loader=price_loader
            )
        )
    return out


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
    try:
        import pandas as pd

        return float(pd.Series(xs).corr(pd.Series(ys), method="spearman"))
    except Exception:
        return None


def summarize_outcome_group(
    rows: Sequence[Dict[str, Any]],
    *,
    horizon: int = 5,
) -> Dict[str, Any]:
    key = f"return_{horizon}d_pct"
    mdd_key = f"max_drawdown_{horizon}d_pct"
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
