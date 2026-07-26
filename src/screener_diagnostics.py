"""Observability-only diagnostics and shadow helpers for the screener.

Nothing in this module changes Production score formulas, thresholds, or
trader inputs. All outputs are diagnostic_only / used_by_trader=false.
"""
from __future__ import annotations

import logging
import math
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

from screener_ops import dedupe_by_issuer_group

logger = logging.getLogger("screener_diagnostics")

DIAGNOSTIC_META = {
    "diagnostic_only": True,
    "used_in_production_score": False,
    "used_by_trader": False,
}


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def _pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Return (a-b)/b * 100 when both valid and b != 0."""
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return (a - b) / b * 100.0


def compute_price_diagnostics(
    closes: Sequence[float],
    *,
    opens: Optional[Sequence[float]] = None,
    ma50: Optional[float] = None,
    ma200: Optional[float] = None,
    atr_14: Optional[float] = None,
) -> Dict[str, Optional[float]]:
    """Compute short-horizon return / drawdown / gap diagnostics from closes."""
    out: Dict[str, Optional[float]] = {
        "return_1d_pct": None,
        "return_3d_pct": None,
        "return_5d_pct": None,
        "price_vs_ma50_pct": None,
        "price_vs_ma200_pct": None,
        "ma50_vs_ma200_pct": None,
        "max_drawdown_5d_pct": None,
        "max_drawdown_20d_pct": None,
        "atr_14": _safe_float(atr_14),
        "atr_14_pct": None,
        "gap_pct": None,
        "recent_drop_1d": None,
        "recent_drop_3d": None,
        "recent_drop_5d": None,
    }
    closes_f = [c for c in (_safe_float(c) for c in closes) if c is not None]
    if not closes_f:
        return out
    last = closes_f[-1]
    if len(closes_f) >= 2 and closes_f[-2]:
        out["return_1d_pct"] = _pct_change(last, closes_f[-2])
    if len(closes_f) >= 4 and closes_f[-4]:
        out["return_3d_pct"] = _pct_change(last, closes_f[-4])
    if len(closes_f) >= 6 and closes_f[-6]:
        out["return_5d_pct"] = _pct_change(last, closes_f[-6])

    out["price_vs_ma50_pct"] = _pct_change(last, _safe_float(ma50))
    out["price_vs_ma200_pct"] = _pct_change(last, _safe_float(ma200))
    out["ma50_vs_ma200_pct"] = _pct_change(_safe_float(ma50), _safe_float(ma200))

    atr = _safe_float(atr_14)
    if atr is not None and last:
        out["atr_14_pct"] = atr / last * 100.0
        out["atr_14"] = atr

    if opens and len(opens) >= 1 and len(closes_f) >= 2:
        out["gap_pct"] = _pct_change(_safe_float(opens[-1]), closes_f[-2])

    def _mdd(window: int) -> Optional[float]:
        if len(closes_f) < 2:
            return None
        slice_ = closes_f[-window:] if len(closes_f) >= window else closes_f
        if len(slice_) < 2:
            return None
        peak = slice_[0]
        mdd = 0.0
        for p in slice_:
            peak = max(peak, p)
            if peak > 0:
                mdd = min(mdd, (p - peak) / peak * 100.0)
        return mdd

    out["max_drawdown_5d_pct"] = _mdd(5)
    out["max_drawdown_20d_pct"] = _mdd(20)

    r1, r3, r5 = out["return_1d_pct"], out["return_3d_pct"], out["return_5d_pct"]
    out["recent_drop_1d"] = r1 if r1 is not None and r1 < 0 else None
    out["recent_drop_3d"] = r3 if r3 is not None and r3 < 0 else None
    out["recent_drop_5d"] = r5 if r5 is not None and r5 < 0 else None
    return out


def evaluate_diagnostic_flags(
    *,
    financial_score: Optional[float],
    technical_score: Optional[float],
    return_1d_pct: Optional[float],
    return_3d_pct: Optional[float],
    return_5d_pct: Optional[float],
    price_vs_ma50_pct: Optional[float],
    atr_14_pct: Optional[float],
    gap_pct: Optional[float],
    rsi: Optional[float],
    policy: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Return diagnostic-only flags. Never used as Production exclusion reasons."""
    p = policy or {}
    flags: List[str] = []
    fin_max = float(p.get("high_tech_low_fin_financial_max", 0.10))
    tech_min = float(p.get("high_tech_low_fin_technical_min", 0.85))
    drop_1d = float(p.get("short_term_drop_1d_pct", -3.0))
    drop_3d = float(p.get("short_term_drop_3d_pct", -5.0))
    drop_5d = float(p.get("short_term_drop_5d_pct", -8.0))
    extended_ma50 = float(p.get("extended_above_ma50_pct", 8.0))
    high_atr = float(p.get("high_atr_pct", 5.0))
    gap_up = float(p.get("gap_up_pct", 2.0))
    gap_down = float(p.get("gap_down_pct", -2.0))
    rsi_hot = float(p.get("rsi_overheated_threshold", 70.0))
    rsi_cold = float(p.get("rsi_oversold_threshold", 30.0))

    # Distinguish actual zero from missing: only flag when both scores are present.
    if financial_score is not None and technical_score is not None:
        if financial_score <= fin_max and technical_score >= tech_min:
            flags.append("HIGH_TECH_LOW_FIN")

    if (
        (return_1d_pct is not None and return_1d_pct <= drop_1d)
        or (return_3d_pct is not None and return_3d_pct <= drop_3d)
        or (return_5d_pct is not None and return_5d_pct <= drop_5d)
    ):
        flags.append("SHORT_TERM_DROP")

    if price_vs_ma50_pct is not None and price_vs_ma50_pct >= extended_ma50:
        flags.append("EXTENDED_ABOVE_MA50")

    if atr_14_pct is not None and atr_14_pct >= high_atr:
        flags.append("HIGH_ATR")

    if gap_pct is not None and gap_pct >= gap_up:
        flags.append("GAP_UP")
    if gap_pct is not None and gap_pct <= gap_down:
        flags.append("GAP_DOWN")

    if rsi is not None and not (isinstance(rsi, float) and math.isnan(rsi)):
        if float(rsi) >= rsi_hot:
            flags.append("RSI_OVERHEATED")
        if float(rsi) <= rsi_cold:
            flags.append("RSI_OVERSOLD")

    return flags


def build_score_component_diagnostics(row: Dict[str, Any] | pd.Series) -> Dict[str, Any]:
    """Map score component fields for export (diagnostic aliases)."""
    get = row.get if hasattr(row, "get") else lambda k, d=None: row[k] if k in row.index else d
    return {
        "financial_score": _safe_float(get("FinScore", get("fin_score"))),
        "technical_score": _safe_float(get("TechScore", get("tech_score"))),
        "market_score": _safe_float(get("MktScore", get("market_score"))),
        "sector_score": _safe_float(get("SectorScore", get("sector_score"))),
        "volatility_score": _safe_float(get("VolKki", get("vol_kki"))),
        "position_52w_score": _safe_float(get("Pos52w", get("pos_52w"))),
    }


def smooth_above_ma50_score(
    distance_to_ma50_pct: Optional[float],
    *,
    transition_band_pct: float = 2.0,
) -> Optional[float]:
    """Soft above-MA50 score in [0, 1]. Production binary remains unchanged."""
    if distance_to_ma50_pct is None:
        return None
    band = max(1e-9, float(transition_band_pct))
    raw = 0.5 + float(distance_to_ma50_pct) / band / 2.0
    return float(max(0.0, min(1.0, raw)))


def compute_market_regime_shadow(
    *,
    index_close: Optional[float],
    index_ma50: Optional[float],
    production_above_ma50_binary: Optional[float],
    ma50_gt_ma200: Optional[float],
    rsi_term: Optional[float],
    production_weighted_regime_score: Optional[float],
    scoring_market_component: Optional[float],
    advanced_market_confidence: Optional[float] = None,
    transition_band_pct: float = 2.0,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Shadow smooth regime diagnostics. Never feeds Production score."""
    dist = _pct_change(index_close, index_ma50)
    smooth = smooth_above_ma50_score(dist, transition_band_pct=transition_band_pct) if enabled else None
    smooth_weighted = None
    if smooth is not None and ma50_gt_ma200 is not None and rsi_term is not None:
        smooth_weighted = float((smooth + float(ma50_gt_ma200) + float(rsi_term)) / 3.0)

    payload = {
        "enabled": bool(enabled),
        "index_close": _safe_float(index_close),
        "index_ma50": _safe_float(index_ma50),
        "distance_to_ma50_pct": None if dist is None else round(dist, 6),
        "production_above_ma50_binary": _safe_float(production_above_ma50_binary),
        "smooth_above_ma50_score": None if smooth is None else round(smooth, 6),
        "production_weighted_regime_score": _safe_float(production_weighted_regime_score),
        "smooth_weighted_regime_score": None if smooth_weighted is None else round(smooth_weighted, 6),
        "scoring_market_component": _safe_float(scoring_market_component),
        "advanced_market_confidence": _safe_float(advanced_market_confidence),
        "transition_band_pct": float(transition_band_pct),
        **DIAGNOSTIC_META,
    }
    return payload


def compute_exclusion_summary(
    scored_df: pd.DataFrame,
    *,
    issuer_dup_tickers: Optional[Set[str]] = None,
    sector_cap_tickers: Optional[Set[str]] = None,
) -> Dict[str, int]:
    """Count exclusion reason occurrences (multi-reason tickers counted in each)."""
    keys = [
        "ALREADY_HELD",
        "RSI_OVERHEATED",
        "UP_STREAK",
        "ISSUER_DUPLICATE",
        "SECTOR_CAP",
        "NEWLY_LISTED",
        "NEGATIVE_MOMENTUM",
        "HIGH_VOLATILITY",
    ]
    summary = {k: 0 for k in keys}
    if scored_df is None or scored_df.empty:
        return summary

    for _, row in scored_df.iterrows():
        reasons = row.get("exclusion_reasons") or row.get("exclude_reasons") or []
        if isinstance(reasons, str):
            reasons = [reasons] if reasons else []
        elif not isinstance(reasons, list):
            try:
                reasons = list(reasons)
            except Exception:
                reasons = []
        for r in reasons:
            key = str(r)
            if key in summary:
                summary[key] += 1
            else:
                summary[key] = summary.get(key, 0) + 1

    for t in issuer_dup_tickers or set():
        summary["ISSUER_DUPLICATE"] = summary.get("ISSUER_DUPLICATE", 0) + 1
    for t in sector_cap_tickers or set():
        summary["SECTOR_CAP"] = summary.get("SECTOR_CAP", 0) + 1
    return summary


def compute_stage_drop_summary(funnel_stages: Sequence[Any]) -> Dict[str, int]:
    """Unique ticker drops per stage (from StageResult.dropped_count)."""
    out: Dict[str, int] = {}
    for st in funnel_stages or []:
        if hasattr(st, "stage"):
            name = st.stage
            dropped = int(getattr(st, "dropped_count", 0) or 0)
        elif isinstance(st, dict):
            name = str(st.get("stage") or "")
            dropped = int(st.get("dropped_count") or 0)
        else:
            continue
        if name:
            out[name] = dropped
    return out


def classify_empty_result_v2(
    *,
    candidate_count: int,
    scored_count: int,
    universe_count: int,
    amount5d_pass: int,
    scoring_failures_all: bool,
    data_quality_codes: Sequence[str],
    funnel_stages: Sequence[Any],
    threshold_pass_rows: Optional[pd.DataFrame] = None,
) -> Tuple[str, str, Optional[str], Optional[Dict[str, Any]]]:
    """Return (status, result_status, empty_reason, empty_reason_detail)."""
    if candidate_count > 0:
        return "SUCCESS", "HAS_CANDIDATES", None, None

    by_stage: Dict[str, Any] = {}
    for st in funnel_stages or []:
        if hasattr(st, "stage"):
            by_stage[st.stage] = st
        elif isinstance(st, dict) and st.get("stage"):
            by_stage[str(st["stage"])] = st

    def _out(name: str) -> Optional[int]:
        st = by_stage.get(name)
        if st is None:
            return None
        if hasattr(st, "output_count"):
            return int(st.output_count)
        return int(st.get("output_count") or 0)

    def _status(name: str) -> Optional[str]:
        st = by_stage.get(name)
        if st is None:
            return None
        if hasattr(st, "status"):
            return str(st.status)
        return str(st.get("status") or "")

    hard_dq = {
        "PRICE_DATA_UNAVAILABLE",
        "AMOUNT5D_DATA_UNAVAILABLE",
        "SCORING_FAILURE_ALL",
        "UNIVERSE_EMPTY",
        "EMPTY_DATA_QUALITY",
    }
    dq_hit = [c for c in data_quality_codes if c in hard_dq]
    if universe_count <= 0:
        return "SUCCESS", "EMPTY_DATA_QUALITY", "EMPTY_DATA_QUALITY", {"code": "UNIVERSE_EMPTY"}
    if amount5d_pass <= 0 and "AMOUNT5D_DATA_UNAVAILABLE" in data_quality_codes:
        return "SUCCESS", "EMPTY_DATA_QUALITY", "EMPTY_DATA_QUALITY", {"code": "AMOUNT5D_DATA_UNAVAILABLE"}
    if scoring_failures_all or scored_count <= 0:
        return "SUCCESS", "EMPTY_DATA_QUALITY", "SCORING_FAILED", {"scored_count": scored_count}

    min_score_out = _out("MIN_SCORE")
    if min_score_out == 0:
        return "SUCCESS", "EMPTY_VALID", "MIN_SCORE_THRESHOLD_NOT_MET", {
            "threshold_pass_count": 0,
        }

    elig_out = _out("ELIGIBILITY")
    detail: Dict[str, Any] = {
        "threshold_pass_count": int(min_score_out or 0),
        "eligibility_output": elig_out,
    }

    if threshold_pass_rows is not None and not threshold_pass_rows.empty:
        reasons_lists = []
        held_count = 0
        for _, row in threshold_pass_rows.iterrows():
            reasons = row.get("exclusion_reasons") or row.get("exclude_reasons") or []
            if isinstance(reasons, str):
                reasons = [reasons] if reasons else []
            elif not isinstance(reasons, list):
                try:
                    reasons = list(reasons)
                except Exception:
                    reasons = []
            reasons_lists.append(reasons)
            if bool(row.get("held")) or "ALREADY_HELD" in reasons:
                held_count += 1
        other_ineligible = 0
        for reasons in reasons_lists:
            if reasons and "ALREADY_HELD" not in reasons:
                other_ineligible += 1
            elif reasons and "ALREADY_HELD" in reasons and len([r for r in reasons if r != "ALREADY_HELD"]) > 0:
                # still primarily held; counted in held
                pass
        detail["already_held_count"] = held_count
        detail["other_ineligible_count"] = sum(
            1
            for reasons in reasons_lists
            if reasons and not (len(reasons) == 1 and reasons[0] == "ALREADY_HELD") and "ALREADY_HELD" not in reasons
        )
        # Prefer counting pure / all held passers
        all_held = held_count == len(threshold_pass_rows) and held_count > 0
        if elig_out == 0 and all_held:
            return (
                "SUCCESS",
                "EMPTY_VALID",
                "ALL_THRESHOLD_PASSERS_ALREADY_HELD",
                detail,
            )
        if elig_out == 0:
            return (
                "SUCCESS",
                "EMPTY_VALID",
                "ALL_THRESHOLD_PASSERS_INELIGIBLE",
                detail,
            )

    if elig_out == 0:
        return "SUCCESS", "EMPTY_VALID", "ALL_THRESHOLD_PASSERS_INELIGIBLE", detail

    issuer_out = _out("ISSUER_DEDUP")
    issuer_status = _status("ISSUER_DEDUP")
    if issuer_status == "APPLIED" and issuer_out == 0:
        return "SUCCESS", "EMPTY_VALID", "ALL_ELIGIBLE_REMOVED_BY_ISSUER_DEDUP", detail

    sector_out = _out("SECTOR_DIVERSIFICATION")
    sector_status = _status("SECTOR_DIVERSIFICATION")
    if sector_status == "APPLIED" and sector_out == 0:
        return "SUCCESS", "EMPTY_VALID", "ALL_ELIGIBLE_REMOVED_BY_SECTOR_CAP", detail

    if elig_out is not None and elig_out > 0 and (candidate_count or 0) == 0:
        return "SUCCESS", "EMPTY_VALID", "NO_ELIGIBLE_NEW_BUY_CANDIDATES", detail

    if dq_hit:
        return "SUCCESS", "EMPTY_DATA_QUALITY", "EMPTY_DATA_QUALITY", {"codes": list(dq_hit)}

    return "SUCCESS", "EMPTY_VALID", "UNKNOWN_EMPTY_REASON", detail


def compute_eligible_shadow(
    scored_df: pd.DataFrame,
    *,
    policy: Dict[str, Any],
    production_tickers: Optional[Set[str]] = None,
    diversify_fn: Optional[Callable] = None,
    top_n: int = 8,
    sector_cap: float = 0.35,
    apply_sector_cap: bool = True,
    apply_issuer_dedupe: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Eligible-only near-miss shadow. Never used by trader."""
    empty = scored_df.iloc[0:0] if scored_df is not None else pd.DataFrame()
    meta: Dict[str, Any] = {
        "enabled": bool(policy.get("enabled", True)),
        "type": "ELIGIBLE_ONLY",
        "population_count": 0,
        "population_p90": None,
        "floor": float(policy.get("floor", 0.42) or 0.42),
        "percentile": float(policy.get("percentile", 0.90) or 0.90),
        "threshold": None,
        "candidate_count": 0,
        "production_candidates_excluded": bool(policy.get("exclude_production_candidates", True)),
        "min_population": int(policy.get("min_population", 5) or 5),
        "threshold_mode": "floor_fallback",
        "used_by_trader": False,
        **DIAGNOSTIC_META,
    }
    if not policy.get("enabled", True) or scored_df is None or scored_df.empty:
        meta["enabled"] = bool(policy.get("enabled", True))
        return empty, meta

    work = scored_df.copy()
    if "eligibility_status" in work.columns:
        work = work[work["eligibility_status"] == "ELIGIBLE"].copy()
    elif "exclude_reasons" in work.columns:
        work = work[
            work["exclude_reasons"].apply(
                lambda v: not v or (isinstance(v, float) and math.isnan(v)) or len(v) == 0
            )
        ].copy()

    if apply_issuer_dedupe and not work.empty:
        work = dedupe_by_issuer_group(work)

    if apply_sector_cap and diversify_fn is not None and not work.empty:
        indexed = work.set_index("Ticker") if "Ticker" in work.columns else work
        max_n = int(policy.get("max_candidates", top_n) or top_n)
        # Use a large top_n for population construction then threshold later
        diversified = diversify_fn(indexed, max(max_n, len(work)), sector_cap)
        if diversified is not None and not diversified.empty:
            work = (
                diversified.reset_index()
                if "Ticker" not in getattr(diversified, "columns", [])
                else diversified
            )

    meta["population_count"] = int(len(work))
    if work.empty:
        meta["threshold"] = meta["floor"]
        return empty, meta

    scores = [float(x) for x in work["Score"].tolist()]
    floor = float(policy.get("floor", 0.42) or 0.42)
    pct = float(policy.get("percentile", 0.90) or 0.90)
    min_pop = int(policy.get("min_population", 5) or 5)
    p90 = float(pd.Series(scores).quantile(pct)) if scores else None
    meta["population_p90"] = None if p90 is None else round(p90, 6)

    mode = str(policy.get("mode", "hybrid_percentile")).lower()
    if len(scores) < min_pop:
        thr = floor
        meta["threshold_mode"] = "floor_only_insufficient_population"
    elif mode == "floor":
        thr = floor
        meta["threshold_mode"] = "floor"
    elif mode == "percentile":
        thr = float(p90) if p90 is not None else floor
        meta["threshold_mode"] = "percentile"
    else:
        thr = max(floor, float(p90) if p90 is not None else floor)
        meta["threshold_mode"] = "hybrid_percentile"

    meta["threshold"] = round(float(thr), 6)
    cands = work[work["Score"] >= float(thr)].copy()

    prod = {str(t).upper() for t in (production_tickers or set())}
    if policy.get("exclude_production_candidates", True) and prod and not cands.empty:
        cands = cands[~cands["Ticker"].astype(str).str.upper().isin(prod)].copy()

    max_c = int(policy.get("max_candidates", 5) or 5)
    if not cands.empty:
        cands = cands.sort_values(by=["Score"], ascending=False).head(max_c)
    meta["candidate_count"] = int(len(cands))
    return cands, meta


def compute_liquidity_shadow_universe(
    amount_by_ticker: Dict[str, float],
    *,
    policy: Dict[str, Any],
    production_threshold: float,
) -> Tuple[List[str], Dict[str, Any]]:
    """Build Amount5D P90 (or configured) universe for liquidity shadow."""
    meta: Dict[str, Any] = {
        "enabled": bool(policy.get("enabled", True)),
        "type": "LIQUIDITY",
        "production_liquidity_threshold": float(production_threshold),
        "shadow_liquidity_threshold": None,
        "universe_count": 0,
        "production_liquidity_pass_count": 0,
        "max_universe": int(policy.get("max_universe", 60) or 60),
        "used_by_trader": False,
        **DIAGNOSTIC_META,
    }
    if not policy.get("enabled", True) or not amount_by_ticker:
        return [], meta

    s = pd.to_numeric(pd.Series(amount_by_ticker), errors="coerce").dropna()
    if s.empty:
        return [], meta

    mode = str(policy.get("threshold_mode", "percentile")).lower()
    pct = float(policy.get("percentile", 0.90) or 0.90)
    if mode == "static":
        thr = float(policy.get("static_threshold", s.quantile(pct)))
    else:
        thr = float(s.quantile(pct))
    meta["shadow_liquidity_threshold"] = thr
    meta["production_liquidity_pass_count"] = int((s >= float(production_threshold)).sum())

    ranked = s.sort_values(ascending=False)
    selected = ranked[ranked >= thr]
    max_u = int(policy.get("max_universe", 60) or 60)
    selected = selected.head(max_u)
    tickers = [str(t).upper() for t in selected.index.tolist()]
    meta["universe_count"] = len(tickers)
    return tickers, meta


def build_liquidity_shadow_rows(
    *,
    scored_rows: List[Dict[str, Any]],
    amount_by_ticker: Dict[str, float],
    production_threshold: float,
    shadow_threshold: float,
    production_tickers: Optional[Set[str]] = None,
    eligible_shadow_tickers: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Attach liquidity pass flags to scored shadow rows."""
    prod = {str(t).upper() for t in (production_tickers or set())}
    elig = {str(t).upper() for t in (eligible_shadow_tickers or set())}
    out: List[Dict[str, Any]] = []
    for row in scored_rows:
        t = str(row.get("ticker") or row.get("Ticker") or "").upper()
        amt = _safe_float(amount_by_ticker.get(t, row.get("amount5d") or row.get("Amount5D")))
        rec = dict(row)
        rec["ticker"] = t
        rec["amount5d"] = amt
        rec["production_liquidity_threshold"] = float(production_threshold)
        rec["shadow_liquidity_threshold"] = float(shadow_threshold)
        rec["production_liquidity_pass"] = bool(amt is not None and amt >= float(production_threshold))
        rec["shadow_liquidity_pass"] = bool(amt is not None and amt >= float(shadow_threshold))
        rec["production_candidate"] = t in prod
        rec["eligible_shadow_candidate"] = t in elig
        rec["used_by_trader"] = False
        rec["diagnostic_only"] = True
        out.append(rec)
    return out


def select_liquidity_shadow_candidates(
    rows: List[Dict[str, Any]],
    *,
    max_candidates: int = 10,
    min_score: Optional[float] = None,
) -> List[Dict[str, Any]]:
    scored = [r for r in rows if _safe_float(r.get("score") or r.get("Score")) is not None]
    if min_score is not None:
        scored = [r for r in scored if float(r.get("score") or r.get("Score") or 0) >= float(min_score)]
    scored.sort(key=lambda r: float(r.get("score") or r.get("Score") or 0), reverse=True)
    return scored[: int(max_candidates)]


def summarize_diagnostics(score_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate diagnostic flags for run meta / review."""
    flag_counts: Dict[str, int] = {}
    by_flag: Dict[str, List[str]] = {}
    near_miss: List[Dict[str, Any]] = []
    for rec in score_records or []:
        flags = rec.get("diagnostic_flags") or []
        ticker = str(rec.get("ticker") or "")
        for f in flags:
            flag_counts[f] = flag_counts.get(f, 0) + 1
            by_flag.setdefault(f, []).append(ticker)
        score = _safe_float(rec.get("score"))
        thr = _safe_float(rec.get("production_threshold")) or _safe_float(
            rec.get("configured_threshold")
        )
        if (
            score is not None
            and thr is not None
            and not rec.get("production_candidate")
            and bool(rec.get("eligibility_pass"))
            and 0 < (thr - score) <= 0.05
        ):
            near_miss.append(
                {
                    "ticker": ticker,
                    "score": score,
                    "threshold_delta": round(thr - score, 6),
                }
            )
    return {
        "flag_counts": flag_counts,
        "tickers_by_flag": {k: v[:20] for k, v in by_flag.items()},
        "near_miss": near_miss[:20],
        **DIAGNOSTIC_META,
    }


def annotate_stage_outcomes(
    scored_df: pd.DataFrame,
    *,
    production_tickers: Set[str],
    eligibility_pass_tickers: Set[str],
    issuer_pass_tickers: Optional[Set[str]],
    sector_pass_tickers: Optional[Set[str]],
    high_conviction_shadow_tickers: Optional[Set[str]] = None,
    eligible_shadow_tickers: Optional[Set[str]] = None,
    liquidity_shadow_tickers: Optional[Set[str]] = None,
    issuer_drop_reasons: Optional[Dict[str, str]] = None,
    sector_drop_reasons: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Annotate scored rows with stage pass flags (null = not evaluated)."""
    if scored_df is None or scored_df.empty:
        return scored_df
    out = scored_df.copy()
    prod = {str(t).upper() for t in production_tickers}
    elig = {str(t).upper() for t in eligibility_pass_tickers}
    issuer_pass = None if issuer_pass_tickers is None else {str(t).upper() for t in issuer_pass_tickers}
    sector_pass = None if sector_pass_tickers is None else {str(t).upper() for t in sector_pass_tickers}
    hc = {str(t).upper() for t in (high_conviction_shadow_tickers or set())}
    es = {str(t).upper() for t in (eligible_shadow_tickers or set())}
    ls = {str(t).upper() for t in (liquidity_shadow_tickers or set())}
    issuer_reasons = issuer_drop_reasons or {}
    sector_reasons = sector_drop_reasons or {}

    elig_pass, issuer_p, issuer_r, sector_p, sector_r = [], [], [], [], []
    prod_flags, hc_flags, es_flags, ls_flags = [], [], [], []

    for _, row in out.iterrows():
        t = str(row.get("Ticker") or "").upper()
        threshold_ok = bool(row.get("threshold_pass", False))
        # eligibility only evaluated for threshold passers in production pipeline
        if not threshold_ok:
            elig_pass.append(None)
            issuer_p.append(None)
            issuer_r.append(None)
            sector_p.append(None)
            sector_r.append(None)
        else:
            e_ok = t in elig
            elig_pass.append(e_ok)
            if not e_ok:
                issuer_p.append(None)
                issuer_r.append(None)
                sector_p.append(None)
                sector_r.append(None)
            elif issuer_pass is None:
                issuer_p.append(None)
                issuer_r.append("NOT_EVALUATED")
                sector_p.append(None)
                sector_r.append("NOT_EVALUATED")
            else:
                i_ok = t in issuer_pass
                issuer_p.append(i_ok)
                issuer_r.append(None if i_ok else issuer_reasons.get(t, "ISSUER_DUPLICATE"))
                if not i_ok or sector_pass is None:
                    sector_p.append(None)
                    sector_r.append(None if i_ok else "NOT_EVALUATED")
                else:
                    s_ok = t in sector_pass
                    sector_p.append(s_ok)
                    sector_r.append(None if s_ok else sector_reasons.get(t, "SECTOR_CAP"))

        prod_flags.append(t in prod)
        hc_flags.append(t in hc)
        es_flags.append(t in es)
        ls_flags.append(t in ls)

    out["eligibility_pass"] = elig_pass
    out["issuer_dedup_pass"] = issuer_p
    out["issuer_dedup_reason"] = issuer_r
    out["sector_diversification_pass"] = sector_p
    out["sector_diversification_reason"] = sector_r
    out["production_candidate"] = prod_flags
    out["high_conviction_shadow_candidate"] = hc_flags
    out["eligible_shadow_candidate"] = es_flags
    out["liquidity_shadow_candidate"] = ls_flags
    return out
