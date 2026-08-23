"""Read-only offline Production score factor decomposition.

Never mutates Production thresholds, score weights, trader, GPT, DECISION
artifacts, diagnostics, or DB. Liquidity Shadow and GPT are excluded from
this analysis — only Production Screener score components are studied.

Factor names come from the Production weighted formula in ``screener.py``
and the ``screener_scores.json`` / candidates artifact schema — not guesses.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger("screener_factor_analysis")

HORIZONS = (1, 3, 5, 10)
RETURN_KEYS = {h: f"return_{h}d_pct" for h in HORIZONS}

# Production weighted components (screener.py total_score formula).
# pattern_score is computed in artifacts but NOT in the weighted sum — excluded.
# Aliases cover screener_scores snake_case, PascalCase candidates, and diagnostics.
PRODUCTION_FACTOR_SPECS: Tuple[Dict[str, Any], ...] = (
    {
        "name": "fin_score",
        "aliases": ("fin_score", "FinScore", "financial_score"),
        "weight_key": "fin_weight",
        "default_weight": 0.25,
        "artifact_sources": ("screener_scores", "screener_candidates"),
    },
    {
        "name": "tech_score",
        "aliases": ("tech_score", "TechScore", "technical_score"),
        "weight_key": "tech_weight",
        "default_weight": 0.30,
        "artifact_sources": ("screener_scores", "screener_candidates"),
    },
    {
        "name": "market_score",
        "aliases": ("market_score", "MktScore", "market_component"),
        "weight_key": "mkt_weight",
        "default_weight": 0.15,
        "artifact_sources": ("screener_scores", "screener_candidates"),
    },
    {
        "name": "sector_score",
        "aliases": ("sector_score", "SectorScore"),
        "weight_key": "sector_weight",
        "default_weight": 0.15,
        "artifact_sources": ("screener_scores", "screener_candidates"),
    },
    {
        "name": "vol_kki",
        "aliases": ("vol_kki", "VolKki", "volatility_score"),
        "weight_key": "vol_kki_weight",
        "default_weight": 0.10,
        "artifact_sources": ("screener_scores", "screener_candidates"),
    },
    {
        "name": "pos_52w",
        "aliases": ("pos_52w", "Pos52w", "position_52w_score"),
        "weight_key": "pos_52w_weight",
        "default_weight": 0.05,
        "artifact_sources": ("screener_scores", "screener_candidates"),
    },
)

# Explicitly not Production total_score factors for this calibration.
EXCLUDED_FROM_FACTOR_ANALYSIS = (
    "pattern_score",
    "PatternScore",
    "gpt_score",
    "liquidity_shadow",
)

FINDING_NEG = "NEGATIVE_FACTOR_CANDIDATE"
FINDING_POS = "POSITIVE_FACTOR_CANDIDATE"
FINDING_LOW = "LOW_SIGNAL_FACTOR"
FINDING_DC = "POSSIBLE_DOUBLE_COUNTING"
FINDING_REGIME = "REGIME_DEPENDENT_FACTOR"

CORR_HIGH = 0.8
SPEARMAN_SIGNAL = 0.15
SPEARMAN_LOW = 0.10


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


def _load_json(path: Path) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("JSON load failed %s: %s", path, e)
        return None


def _ticker(row: Dict[str, Any]) -> str:
    return str(row.get("Ticker") or row.get("ticker") or "").upper()


def production_factor_weights(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    """Read Production weights from config (read-only). Never writes config."""
    params: Dict[str, Any] = {}
    if isinstance(cfg, dict):
        params = dict(cfg.get("screener_params") or {})
    out: Dict[str, float] = {}
    for spec in PRODUCTION_FACTOR_SPECS:
        name = str(spec["name"])
        key = str(spec["weight_key"])
        default = float(spec["default_weight"])
        out[name] = float(params.get(key, default))
    return out


def extract_factor_value(row: Dict[str, Any], factor_name: str) -> Optional[float]:
    """Resolve a Production factor from artifact row via known aliases."""
    spec = next((s for s in PRODUCTION_FACTOR_SPECS if s["name"] == factor_name), None)
    if spec is None:
        return None
    for alias in spec["aliases"]:
        if alias in row and row.get(alias) is not None:
            v = _safe_float(row.get(alias))
            if v is not None:
                return v
    return None


def inspect_artifact_factor_schema(
    sample_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Inspect actual keys present in screener_scores / candidates samples.

    Does not invent factor names; intersects Production formula with observed keys.
    """
    key_counts: Counter = Counter()
    for row in sample_rows:
        if not isinstance(row, dict):
            continue
        for k in row.keys():
            key_counts[str(k)] += 1

    observed_keys = sorted(key_counts.keys())
    resolved: List[Dict[str, Any]] = []
    missing: List[str] = []
    for spec in PRODUCTION_FACTOR_SPECS:
        present_aliases = [a for a in spec["aliases"] if a in key_counts]
        if present_aliases:
            resolved.append(
                {
                    "factor": spec["name"],
                    "weight_key": spec["weight_key"],
                    "resolved_aliases": present_aliases,
                    "primary_field": present_aliases[0],
                    "observed_count": sum(key_counts[a] for a in present_aliases),
                }
            )
        else:
            missing.append(str(spec["name"]))

    excluded_present = [k for k in EXCLUDED_FROM_FACTOR_ANALYSIS if k in key_counts]
    return {
        "observed_keys": observed_keys,
        "production_factors_resolved": resolved,
        "production_factors_missing": missing,
        "excluded_fields_present_but_not_in_weighted_score": excluded_present,
        "source": "screener.py weighted total_score ∩ artifact schema",
        "note": (
            "pattern_score may appear in artifacts but is not part of the "
            "Production weighted total_score formula."
        ),
    }


def load_score_rows_from_run(run_dir: Path) -> List[Dict[str, Any]]:
    """Prefer screener_scores.json; fall back to candidates for factor fields."""
    scores = _load_json(Path(run_dir) / "screener_scores.json")
    rows: List[Dict[str, Any]] = []
    if isinstance(scores, list):
        rows.extend([r for r in scores if isinstance(r, dict)])
    if rows:
        return rows
    for name in ("screener_candidates_full.json", "screener_candidates.json"):
        cands = _load_json(Path(run_dir) / name)
        if isinstance(cands, list):
            rows.extend([r for r in cands if isinstance(r, dict)])
            if rows:
                return rows
    return rows


def score_index_by_ticker(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        t = _ticker(r)
        if t:
            out[t] = r
    return out


def extract_market_regime(meta: Dict[str, Any]) -> str:
    """Best-effort regime label from run meta (read-only)."""
    for key in ("regime",):
        v = meta.get(key)
        if v:
            return str(v)
    ms = meta.get("market_state")
    if isinstance(ms, dict):
        for key in ("regime", "market_regime"):
            v = ms.get(key)
            if v:
                return str(v)
    adv = meta.get("advanced_market_state")
    if isinstance(adv, dict) and adv.get("regime"):
        return str(adv.get("regime"))
    return "UNKNOWN"


def is_settled_observation(row: Dict[str, Any], *, require_5d: bool = True) -> bool:
    """Settled = forward return available (no PENDING-only stubs)."""
    status = str(row.get("outcome_status") or "")
    if status in ("PENDING",):
        # Still allow if returns already populated (ledger merge edge)
        pass
    if require_5d:
        return _safe_float(row.get("return_5d_pct")) is not None
    for h in HORIZONS:
        if _safe_float(row.get(RETURN_KEYS[h])) is not None:
            return True
    mat = row.get("maturity") or {}
    if isinstance(mat, dict) and any(mat.get(f"{h}d") for h in HORIZONS):
        return True
    return status not in ("", "PENDING") and any(
        _safe_float(row.get(RETURN_KEYS[h])) is not None for h in HORIZONS
    )


def filter_production_analysis_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    start_trade_date: str,
    end_trade_date: str,
    market: str,
    session: str,
) -> List[Dict[str, Any]]:
    """PRODUCTION + trusted + report-window + settled only."""
    from screener_quality import (
        filter_report_scoped_observations,
        is_trusted_for_analysis,
    )

    scoped = filter_report_scoped_observations(
        rows,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
        market=market,
        session=session,
    )
    out: List[Dict[str, Any]] = []
    for r in scoped:
        if str(r.get("candidate_type") or "") != "PRODUCTION":
            continue
        if not is_trusted_for_analysis(r):
            continue
        if not is_settled_observation(r, require_5d=True):
            continue
        out.append(r)
    return out


def first_signal_only(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Per ticker keep earliest trade_date (then decision_run_id)."""
    best: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        t = str(r.get("ticker") or "").upper()
        if not t:
            continue
        prev = best.get(t)
        if prev is None:
            best[t] = r
            continue
        td = str(r.get("trade_date") or "")
        ptd = str(prev.get("trade_date") or "")
        if td < ptd or (
            td == ptd
            and str(r.get("decision_run_id") or "") < str(prev.get("decision_run_id") or "")
        ):
            best[t] = r
    return list(best.values())


def ticker_equal_weight_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse repeated tickers to one synthetic row (mean numerics)."""
    by_t: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        t = str(r.get("ticker") or "").upper()
        if t:
            by_t[t].append(r)
    out: List[Dict[str, Any]] = []
    numeric_keys = [
        "total_score",
        "decision_score",
        "return_1d_pct",
        "return_3d_pct",
        "return_5d_pct",
        "return_10d_pct",
        "max_drawdown_5d_pct",
        "max_runup_5d_pct",
        "contribution_sum",
    ]
    for t, group in by_t.items():
        syn = dict(group[0])
        syn["ticker"] = t
        syn["observation_weight_mode"] = "ticker_equal"
        syn["collapsed_observations"] = len(group)
        for k in numeric_keys:
            vals = [_safe_float(g.get(k)) for g in group]
            ok = [v for v in vals if v is not None]
            syn[k] = (sum(ok) / len(ok)) if ok else None
        # Factors / contributions: mean across observations
        factors = set()
        for g in group:
            factors.update((g.get("factors") or {}).keys())
        f_out: Dict[str, Optional[float]] = {}
        c_out: Dict[str, Optional[float]] = {}
        for fname in factors:
            fvals = [_safe_float((g.get("factors") or {}).get(fname)) for g in group]
            fok = [v for v in fvals if v is not None]
            f_out[fname] = (sum(fok) / len(fok)) if fok else None
            cvals = [
                _safe_float((g.get("contributions") or {}).get(fname)) for g in group
            ]
            cok = [v for v in cvals if v is not None]
            c_out[fname] = (sum(cok) / len(cok)) if cok else None
        syn["factors"] = f_out
        syn["contributions"] = c_out
        out.append(syn)
    return out


def assign_quintiles(
    rows: Sequence[Dict[str, Any]],
    *,
    value_getter,
) -> List[Optional[int]]:
    """Assign Q1..Q5 (1=lowest). Ties use average-rank then map to quintile."""
    n = len(rows)
    if n == 0:
        return []
    vals = [value_getter(r) for r in rows]
    indexed = [(i, v) for i, v in enumerate(vals) if v is not None]
    labels: List[Optional[int]] = [None] * n
    if len(indexed) < 5:
        # Degenerate: put all available into middle quintile or skip
        for i, _v in indexed:
            labels[i] = 3
        return labels
    indexed.sort(key=lambda x: x[1])
    m = len(indexed)
    for rank0, (i, _v) in enumerate(indexed):
        # rank0 in [0, m-1] → quintile 1..5
        q = int(rank0 * 5 // m) + 1
        if q > 5:
            q = 5
        labels[i] = q
    return labels


def spearman_factor_vs_returns(
    rows: Sequence[Dict[str, Any]],
    *,
    factor_name: Optional[str] = None,
    use_total_score: bool = False,
) -> Dict[str, Optional[float]]:
    from screener_outcomes import spearman_corr

    out: Dict[str, Optional[float]] = {}
    for h in HORIZONS:
        xs: List[float] = []
        ys: List[float] = []
        rkey = RETURN_KEYS[h]
        for r in rows:
            if use_total_score:
                x = _safe_float(r.get("total_score") if r.get("total_score") is not None else r.get("decision_score"))
            else:
                x = _safe_float((r.get("factors") or {}).get(factor_name or ""))
            y = _safe_float(r.get(rkey))
            if x is not None and y is not None:
                xs.append(x)
                ys.append(y)
        out[f"spearman_{h}d"] = spearman_corr(xs, ys)
        out[f"n_{h}d"] = len(xs)
    return out


def quintile_bucket_stats(
    rows: Sequence[Dict[str, Any]],
    *,
    factor_name: str,
    horizon: int = 5,
) -> List[Dict[str, Any]]:
    labels = assign_quintiles(
        rows, value_getter=lambda r: _safe_float((r.get("factors") or {}).get(factor_name))
    )
    rkey = RETURN_KEYS[horizon]
    mdd_key = f"max_drawdown_{horizon}d_pct"
    buckets: List[Dict[str, Any]] = []
    for q in range(1, 6):
        group = [r for r, lab in zip(rows, labels) if lab == q]
        rets = [_safe_float(r.get(rkey)) for r in group]
        ok = [v for v in rets if v is not None]
        tickers = {str(r.get("ticker") or "").upper() for r in group if r.get("ticker")}
        mdds = [_safe_float(r.get(mdd_key)) for r in group]
        mdds_ok = [v for v in mdds if v is not None]
        n = len(ok)
        buckets.append(
            {
                "factor": factor_name,
                "quintile": f"Q{q}",
                "n": n,
                "unique_tickers": len(tickers),
                "mean_return": round(sum(ok) / n, 6) if n else None,
                "median_return": (sorted(ok)[n // 2] if n else None),
                "win_rate": round(sum(1 for v in ok if v > 0) / n, 6) if n else None,
                "mean_mdd": round(sum(mdds_ok) / len(mdds_ok), 6) if mdds_ok else None,
                "horizon_d": horizon,
            }
        )
    return buckets


def factor_correlation_matrix(
    rows: Sequence[Dict[str, Any]],
    factor_names: Sequence[str],
) -> Dict[str, Dict[str, Optional[float]]]:
    from screener_outcomes import spearman_corr

    matrix: Dict[str, Dict[str, Optional[float]]] = {}
    for a in factor_names:
        matrix[a] = {}
        for b in factor_names:
            if a == b:
                matrix[a][b] = 1.0
                continue
            xs: List[float] = []
            ys: List[float] = []
            for r in rows:
                fa = _safe_float((r.get("factors") or {}).get(a))
                fb = _safe_float((r.get("factors") or {}).get(b))
                if fa is not None and fb is not None:
                    xs.append(fa)
                    ys.append(fb)
            matrix[a][b] = spearman_corr(xs, ys)
    return matrix


def contribution_decomposition_summary(
    rows: Sequence[Dict[str, Any]],
    factor_names: Sequence[str],
) -> Dict[str, Any]:
    """Mean absolute / signed contribution share of each factor."""
    sums = {f: 0.0 for f in factor_names}
    abs_sums = {f: 0.0 for f in factor_names}
    n_used = 0
    score_gaps: List[float] = []
    for r in rows:
        contribs = r.get("contributions") or {}
        present = {
            f: _safe_float(contribs.get(f))
            for f in factor_names
            if _safe_float(contribs.get(f)) is not None
        }
        if not present:
            continue
        n_used += 1
        for f, v in present.items():
            assert v is not None
            sums[f] += v
            abs_sums[f] += abs(v)
        csum = _safe_float(r.get("contribution_sum"))
        score = _safe_float(r.get("total_score") if r.get("total_score") is not None else r.get("decision_score"))
        if csum is not None and score is not None:
            score_gaps.append(score - csum)
    mean_contrib = {
        f: (round(sums[f] / n_used, 6) if n_used else None) for f in factor_names
    }
    mean_abs = {
        f: (round(abs_sums[f] / n_used, 6) if n_used else None) for f in factor_names
    }
    abs_total = sum(v for v in mean_abs.values() if v is not None) or 0.0
    share = {
        f: (round((mean_abs[f] or 0.0) / abs_total, 6) if abs_total > 0 else None)
        for f in factor_names
    }
    return {
        "observations_used": n_used,
        "mean_contribution": mean_contrib,
        "mean_abs_contribution": mean_abs,
        "mean_abs_share": share,
        "mean_score_minus_contribution_sum": (
            round(sum(score_gaps) / len(score_gaps), 6) if score_gaps else None
        ),
        "note": (
            "contribution = factor_value * production_weight; "
            "decision Score may also include market regime adjustment after the weighted sum."
        ),
    }


def cohort_factor_profile(
    rows: Sequence[Dict[str, Any]],
    factor_names: Sequence[str],
) -> Dict[str, Any]:
    profile: Dict[str, Any] = {
        "n": len(rows),
        "unique_tickers": len({str(r.get("ticker") or "").upper() for r in rows if r.get("ticker")}),
        "mean_total_score": None,
        "mean_return_5d": None,
        "mean_factors": {},
        "mean_contributions": {},
    }
    scores = [
        _safe_float(r.get("total_score") if r.get("total_score") is not None else r.get("decision_score"))
        for r in rows
    ]
    sok = [v for v in scores if v is not None]
    rets = [_safe_float(r.get("return_5d_pct")) for r in rows]
    rok = [v for v in rets if v is not None]
    if sok:
        profile["mean_total_score"] = round(sum(sok) / len(sok), 6)
    if rok:
        profile["mean_return_5d"] = round(sum(rok) / len(rok), 6)
    for f in factor_names:
        fvals = [_safe_float((r.get("factors") or {}).get(f)) for r in rows]
        fok = [v for v in fvals if v is not None]
        profile["mean_factors"][f] = round(sum(fok) / len(fok), 6) if fok else None
        cvals = [_safe_float((r.get("contributions") or {}).get(f)) for r in rows]
        cok = [v for v in cvals if v is not None]
        profile["mean_contributions"][f] = round(sum(cok) / len(cok), 6) if cok else None
    return profile


def compare_cohorts(
    high_loss: Dict[str, Any],
    mid_win: Dict[str, Any],
    factor_names: Sequence[str],
) -> List[Dict[str, Any]]:
    """Highlight factors higher in 0.60+ losers vs 0.48–0.52 winners."""
    diffs: List[Dict[str, Any]] = []
    for f in factor_names:
        a = _safe_float((high_loss.get("mean_factors") or {}).get(f))
        b = _safe_float((mid_win.get("mean_factors") or {}).get(f))
        if a is None or b is None:
            continue
        diffs.append(
            {
                "factor": f,
                "high_score_loss_mean": a,
                "mid_score_win_mean": b,
                "delta_high_minus_mid": round(a - b, 6),
            }
        )
    diffs.sort(key=lambda d: abs(d["delta_high_minus_mid"]), reverse=True)
    return diffs


def build_findings(
    *,
    factor_spearman: Dict[str, Dict[str, Any]],
    correlation: Dict[str, Dict[str, Optional[float]]],
    regime_spearman: Dict[str, Dict[str, Dict[str, Any]]],
    factor_names: Sequence[str],
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    ranked = []
    for f in factor_names:
        sp = factor_spearman.get(f) or {}
        s5 = _safe_float(sp.get("spearman_5d"))
        n5 = int(sp.get("n_5d") or 0)
        ranked.append((f, s5, n5))

    for f, s5, n5 in ranked:
        if s5 is None or n5 < 3:
            findings.append(
                {
                    "type": FINDING_LOW,
                    "factor": f,
                    "spearman_5d": s5,
                    "n_5d": n5,
                    "detail": "insufficient paired samples or undefined Spearman",
                }
            )
            continue
        if s5 <= -SPEARMAN_SIGNAL:
            findings.append(
                {
                    "type": FINDING_NEG,
                    "factor": f,
                    "spearman_5d": s5,
                    "n_5d": n5,
                    "detail": "higher factor values associated with worse 5D returns",
                }
            )
        elif s5 >= SPEARMAN_SIGNAL:
            findings.append(
                {
                    "type": FINDING_POS,
                    "factor": f,
                    "spearman_5d": s5,
                    "n_5d": n5,
                    "detail": "higher factor values associated with better 5D returns",
                }
            )
        elif abs(s5) < SPEARMAN_LOW:
            findings.append(
                {
                    "type": FINDING_LOW,
                    "factor": f,
                    "spearman_5d": s5,
                    "n_5d": n5,
                    "detail": "near-zero rank correlation vs 5D return",
                }
            )

    seen_pairs = set()
    for a in factor_names:
        for b in factor_names:
            if a >= b:
                continue
            c = _safe_float((correlation.get(a) or {}).get(b))
            if c is not None and c >= CORR_HIGH:
                pair = (a, b)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                findings.append(
                    {
                        "type": FINDING_DC,
                        "factors": [a, b],
                        "spearman": c,
                        "detail": f"factor Spearman correlation >= {CORR_HIGH}",
                    }
                )

    for f in factor_names:
        by_reg = regime_spearman.get(f) or {}
        signs = []
        for reg, sp in by_reg.items():
            s5 = _safe_float((sp or {}).get("spearman_5d"))
            n5 = int((sp or {}).get("n_5d") or 0)
            if s5 is not None and n5 >= 5:
                signs.append((reg, s5))
        if len(signs) >= 2:
            vals = [s for _, s in signs]
            if min(vals) < -SPEARMAN_LOW and max(vals) > SPEARMAN_LOW:
                findings.append(
                    {
                        "type": FINDING_REGIME,
                        "factor": f,
                        "by_regime": {reg: s for reg, s in signs},
                        "detail": "Spearman 5D sign differs across market regimes",
                    }
                )

    # Stable ordering by severity
    order = {FINDING_NEG: 0, FINDING_POS: 1, FINDING_DC: 2, FINDING_REGIME: 3, FINDING_LOW: 4}
    findings.sort(key=lambda x: (order.get(str(x.get("type")), 9), str(x.get("factor") or x.get("factors"))))
    return findings


@dataclass
class FactorAnalysisResult:
    observations: List[Dict[str, Any]] = field(default_factory=list)
    schema_inspection: Dict[str, Any] = field(default_factory=dict)
    weights: Dict[str, float] = field(default_factory=dict)
    factor_names: List[str] = field(default_factory=list)
    report: Dict[str, Any] = field(default_factory=dict)


def enrich_observation_with_factors(
    obs: Dict[str, Any],
    score_row: Optional[Dict[str, Any]],
    *,
    weights: Dict[str, float],
    market_regime: str,
) -> Dict[str, Any]:
    """Attach factor values / contributions. Missing factors → None (graceful)."""
    row = dict(obs)
    src = score_row or {}
    factors: Dict[str, Optional[float]] = {}
    contribs: Dict[str, Optional[float]] = {}
    for spec in PRODUCTION_FACTOR_SPECS:
        name = str(spec["name"])
        val = extract_factor_value(src, name)
        if val is None:
            # Try observation itself (if already joined)
            val = extract_factor_value(obs, name)
        factors[name] = val
        w = float(weights.get(name, spec["default_weight"]))
        contribs[name] = (round(val * w, 8) if val is not None else None)

    csum_vals = [v for v in contribs.values() if v is not None]
    row["factors"] = factors
    row["contributions"] = contribs
    row["contribution_sum"] = round(sum(csum_vals), 8) if csum_vals else None
    row["factor_weights"] = dict(weights)
    row["total_score"] = _safe_float(obs.get("decision_score") or obs.get("score") or src.get("score") or src.get("Score"))
    row["market_regime"] = market_regime or str(obs.get("market_regime") or "UNKNOWN")
    # Ensure return / risk fields present
    for h in HORIZONS:
        row.setdefault(RETURN_KEYS[h], obs.get(RETURN_KEYS[h]))
    row.setdefault("max_drawdown_5d_pct", obs.get("max_drawdown_5d_pct"))
    row.setdefault("max_runup_5d_pct", obs.get("max_runup_5d_pct"))
    return row


def build_factor_observations(
    settled_rows: Sequence[Dict[str, Any]],
    *,
    run_dirs: Sequence[Path],
    merged_by_run: Optional[Dict[str, Dict[str, Any]]] = None,
    weights: Optional[Dict[str, float]] = None,
    start_trade_date: str,
    end_trade_date: str,
    market: str,
    session: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, float]]:
    """Filter + join Production factors from DECISION score artifacts."""
    from screener_quality import _merged_of

    w = weights or production_factor_weights()
    regime_by_run: Dict[str, str] = {}
    score_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
    trade_date_by_run: Dict[str, str] = {}
    schema_samples: List[Dict[str, Any]] = []

    for rd in run_dirs:
        meta = (merged_by_run or {}).get(str(rd)) or _merged_of(Path(rd))
        rid = str(meta.get("run_id") or Path(rd).name)
        regime_by_run[rid] = extract_market_regime(meta)
        trade_date_by_run[rid] = str(meta.get("trade_date") or "")
        scores = load_score_rows_from_run(Path(rd))
        score_cache[rid] = score_index_by_ticker(scores)
        schema_samples.extend(scores[:20])

    schema = inspect_artifact_factor_schema(schema_samples)
    # Also inspect candidates if scores empty
    if not schema.get("production_factors_resolved"):
        for rd in run_dirs:
            for name in ("screener_candidates.json", "screener_candidates_full.json"):
                data = _load_json(Path(rd) / name)
                if isinstance(data, list):
                    schema_samples.extend([r for r in data if isinstance(r, dict)][:20])
        schema = inspect_artifact_factor_schema(schema_samples)

    filtered = filter_production_analysis_rows(
        settled_rows,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
        market=market,
        session=session,
    )

    enriched: List[Dict[str, Any]] = []
    for obs in filtered:
        rid = str(obs.get("decision_run_id") or obs.get("source_run_id") or "")
        t = str(obs.get("ticker") or "").upper()
        obs_td = str(obs.get("trade_date") or "")
        score_row = (score_cache.get(rid) or {}).get(t)
        # Fallback only within the same trade_date (no cross-day / future leakage)
        if score_row is None and obs_td:
            for _rid, idx in score_cache.items():
                if trade_date_by_run.get(_rid) != obs_td:
                    continue
                if t in idx:
                    score_row = idx[t]
                    break
        regime = regime_by_run.get(rid) or str(obs.get("market_regime") or "UNKNOWN")
        enriched.append(
            enrich_observation_with_factors(
                obs, score_row, weights=w, market_regime=regime
            )
        )
    return enriched, schema, w


def analyze_factor_dataset(
    observations: Sequence[Dict[str, Any]],
    *,
    schema_inspection: Dict[str, Any],
    weights: Dict[str, float],
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
) -> Dict[str, Any]:
    factor_names = [str(s["name"]) for s in PRODUCTION_FACTOR_SPECS]
    # Drop factors entirely absent across dataset (graceful)
    present = [
        f
        for f in factor_names
        if any(_safe_float((r.get("factors") or {}).get(f)) is not None for r in observations)
    ]
    missing = [f for f in factor_names if f not in present]

    def _run_block(rows: Sequence[Dict[str, Any]], mode: str) -> Dict[str, Any]:
        factor_sp = {f: spearman_factor_vs_returns(rows, factor_name=f) for f in present}
        total_sp = spearman_factor_vs_returns(rows, use_total_score=True)
        buckets: List[Dict[str, Any]] = []
        for f in present:
            buckets.extend(quintile_bucket_stats(rows, factor_name=f, horizon=5))
        corr = factor_correlation_matrix(rows, present)
        decomp = contribution_decomposition_summary(rows, present)

        high_loss = [
            r
            for r in rows
            if (_safe_float(r.get("total_score") if r.get("total_score") is not None else r.get("decision_score")) or -1)
            >= 0.60
            and (_safe_float(r.get("return_5d_pct")) or 0) < 0
        ]
        mid_win = [
            r
            for r in rows
            if 0.48
            <= (
                _safe_float(r.get("total_score") if r.get("total_score") is not None else r.get("decision_score"))
                or -1
            )
            < 0.52
            and (_safe_float(r.get("return_5d_pct")) or -1) > 0
        ]
        high_prof = cohort_factor_profile(high_loss, present)
        mid_prof = cohort_factor_profile(mid_win, present)
        cohort_cmp = compare_cohorts(high_prof, mid_prof, present)

        by_regime: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            by_regime[str(r.get("market_regime") or "UNKNOWN")].append(r)
        regime_factor_sp: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for f in present:
            regime_factor_sp[f] = {
                reg: spearman_factor_vs_returns(grp, factor_name=f)
                for reg, grp in by_regime.items()
            }
        regime_total = {
            reg: spearman_factor_vs_returns(grp, use_total_score=True)
            for reg, grp in by_regime.items()
        }

        findings = build_findings(
            factor_spearman=factor_sp,
            correlation=corr,
            regime_spearman=regime_factor_sp,
            factor_names=present,
        )

        # Rank factors by 5D Spearman
        ranked_5d = []
        for f in present:
            s5 = _safe_float((factor_sp[f] or {}).get("spearman_5d"))
            ranked_5d.append({"factor": f, "spearman_5d": s5, "n_5d": (factor_sp[f] or {}).get("n_5d")})
        ranked_5d_sorted = sorted(
            [x for x in ranked_5d if x["spearman_5d"] is not None],
            key=lambda x: x["spearman_5d"],
        )

        high_corr_pairs = []
        for a in present:
            for b in present:
                if a >= b:
                    continue
                c = _safe_float((corr.get(a) or {}).get(b))
                if c is not None and c > CORR_HIGH:
                    high_corr_pairs.append({"factor_a": a, "factor_b": b, "spearman": c})

        return {
            "weighting_mode": mode,
            "observations": len(rows),
            "unique_tickers": len({str(r.get("ticker") or "").upper() for r in rows if r.get("ticker")}),
            "total_score_spearman": total_sp,
            "factor_spearman": factor_sp,
            "factor_quintiles_5d": buckets,
            "factor_correlation": corr,
            "high_correlation_pairs": high_corr_pairs,
            "contribution_decomposition": decomp,
            "cohort_score_ge_060_5d_loss": high_prof,
            "cohort_score_048_052_5d_win": mid_prof,
            "cohort_factor_deltas": cohort_cmp,
            "regime_total_score_spearman": regime_total,
            "regime_factor_spearman": regime_factor_sp,
            "top5_negative_5d": ranked_5d_sorted[:5],
            "top5_positive_5d": list(reversed(ranked_5d_sorted[-5:])) if ranked_5d_sorted else [],
            "findings": findings,
        }

    obs_block = _run_block(list(observations), "observation_weighted")
    first_rows = first_signal_only(observations)
    first_block = _run_block(first_rows, "first_signal_only")
    te_rows = ticker_equal_weight_rows(observations)
    te_block = _run_block(te_rows, "ticker_equal_weight")

    report: Dict[str, Any] = {
        "analysis": "screener_production_factor_decomposition",
        "read_only": True,
        "production_policy_unchanged": True,
        "excluded": ["LIQUIDITY_SHADOW", "GPT", "pattern_score_not_in_weighted_formula"],
        "market": market,
        "session": session,
        "start_trade_date": start_trade_date,
        "end_trade_date": end_trade_date,
        "candidate_type": "PRODUCTION",
        "trusted_for_analysis": True,
        "settled_only": True,
        "factor_schema": schema_inspection,
        "production_factor_names": present,
        "missing_factors": missing,
        "production_weights_read_only": weights,
        "observation_weighted": obs_block,
        "first_signal_only": first_block,
        "ticker_equal_weight": te_block,
        "mode_comparison": {
            "observation_weighted_total_spearman_5d": (obs_block.get("total_score_spearman") or {}).get(
                "spearman_5d"
            ),
            "first_signal_only_total_spearman_5d": (first_block.get("total_score_spearman") or {}).get(
                "spearman_5d"
            ),
            "ticker_equal_weight_total_spearman_5d": (te_block.get("total_score_spearman") or {}).get(
                "spearman_5d"
            ),
            "observation_n": obs_block.get("observations"),
            "first_signal_n": first_block.get("observations"),
            "ticker_equal_n": te_block.get("observations"),
        },
        "primary_findings": obs_block.get("findings") or [],
        "disclaimer": (
            "Offline diagnostic only. Does not recommend or apply weight / threshold changes."
        ),
    }
    return report


def flatten_observation_csv_rows(observations: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for r in observations:
        base = {
            "ticker": r.get("ticker"),
            "trade_date": r.get("trade_date"),
            "decision_run_id": r.get("decision_run_id"),
            "total_score": r.get("total_score"),
            "market_regime": r.get("market_regime"),
            "return_1d_pct": r.get("return_1d_pct"),
            "return_3d_pct": r.get("return_3d_pct"),
            "return_5d_pct": r.get("return_5d_pct"),
            "return_10d_pct": r.get("return_10d_pct"),
            "max_drawdown_5d_pct": r.get("max_drawdown_5d_pct"),
            "max_runup_5d_pct": r.get("max_runup_5d_pct"),
            "contribution_sum": r.get("contribution_sum"),
        }
        for f, v in (r.get("factors") or {}).items():
            base[f"factor_{f}"] = v
        for f, v in (r.get("contributions") or {}).items():
            base[f"contrib_{f}"] = v
        rows.append(base)
    return rows


def flatten_spearman_csv(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    block = report.get("observation_weighted") or {}
    total = block.get("total_score_spearman") or {}
    rows.append(
        {
            "scope": "observation_weighted",
            "factor": "total_score",
            "spearman_1d": total.get("spearman_1d"),
            "spearman_3d": total.get("spearman_3d"),
            "spearman_5d": total.get("spearman_5d"),
            "spearman_10d": total.get("spearman_10d"),
            "n_5d": total.get("n_5d"),
        }
    )
    for f, sp in (block.get("factor_spearman") or {}).items():
        rows.append(
            {
                "scope": "observation_weighted",
                "factor": f,
                "spearman_1d": sp.get("spearman_1d"),
                "spearman_3d": sp.get("spearman_3d"),
                "spearman_5d": sp.get("spearman_5d"),
                "spearman_10d": sp.get("spearman_10d"),
                "n_5d": sp.get("n_5d"),
            }
        )
    return rows


def flatten_correlation_csv(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    corr = (report.get("observation_weighted") or {}).get("factor_correlation") or {}
    rows: List[Dict[str, Any]] = []
    names = list(corr.keys())
    for a in names:
        for b in names:
            rows.append({"factor_a": a, "factor_b": b, "spearman": (corr.get(a) or {}).get(b)})
    return rows


def render_factor_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Screener Production Factor Analysis")
    lines.append("")
    lines.append(
        f"- Window: `{report.get('start_trade_date')}`–`{report.get('end_trade_date')}` "
        f"`{report.get('market')}` `{report.get('session')}`"
    )
    lines.append("- Scope: PRODUCTION · trusted · settled · report-window only")
    lines.append("- Read-only: Production weights/thresholds/artifacts **unchanged**")
    lines.append("")
    lines.append("## Production factors (schema ∩ formula)")
    lines.append("")
    for f in report.get("production_factor_names") or []:
        w = (report.get("production_weights_read_only") or {}).get(f)
        lines.append(f"- `{f}` weight={w}")
    missing = report.get("missing_factors") or []
    if missing:
        lines.append("")
        lines.append(f"Missing in dataset (graceful): `{', '.join(missing)}`")
    lines.append("")
    lines.append("## Primary findings")
    lines.append("")
    findings = report.get("primary_findings") or []
    if not findings:
        lines.append("_No automated findings._")
    else:
        for fd in findings:
            typ = fd.get("type")
            if typ == FINDING_DC:
                lines.append(
                    f"- **{typ}**: `{fd.get('factors')}` spearman={fd.get('spearman')} — {fd.get('detail')}"
                )
            else:
                lines.append(
                    f"- **{typ}**: `{fd.get('factor')}` spearman_5d={fd.get('spearman_5d')} — {fd.get('detail')}"
                )
    ow = report.get("observation_weighted") or {}
    lines.append("")
    lines.append("## Total score Spearman")
    lines.append("")
    tsp = ow.get("total_score_spearman") or {}
    lines.append(
        f"- 1D={tsp.get('spearman_1d')} · 3D={tsp.get('spearman_3d')} · "
        f"5D={tsp.get('spearman_5d')} · 10D={tsp.get('spearman_10d')}"
    )
    lines.append("")
    lines.append("## Factor Spearman (5D focus)")
    lines.append("")
    lines.append("| Factor | 1D | 3D | 5D | 10D | n_5d |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for f, sp in (ow.get("factor_spearman") or {}).items():
        lines.append(
            f"| `{f}` | {sp.get('spearman_1d')} | {sp.get('spearman_3d')} | "
            f"{sp.get('spearman_5d')} | {sp.get('spearman_10d')} | {sp.get('n_5d')} |"
        )
    lines.append("")
    lines.append("### Top negative (5D)")
    for row in ow.get("top5_negative_5d") or []:
        lines.append(f"- `{row.get('factor')}`: {row.get('spearman_5d')}")
    lines.append("")
    lines.append("### Top positive (5D)")
    for row in ow.get("top5_positive_5d") or []:
        lines.append(f"- `{row.get('factor')}`: {row.get('spearman_5d')}")
    lines.append("")
    lines.append("## Weighting mode comparison")
    lines.append("")
    mc = report.get("mode_comparison") or {}
    lines.append(
        f"- observation_weighted 5D Spearman: `{mc.get('observation_weighted_total_spearman_5d')}` (n={mc.get('observation_n')})"
    )
    lines.append(
        f"- first_signal_only 5D Spearman: `{mc.get('first_signal_only_total_spearman_5d')}` (n={mc.get('first_signal_n')})"
    )
    lines.append(
        f"- ticker_equal_weight 5D Spearman: `{mc.get('ticker_equal_weight_total_spearman_5d')}` (n={mc.get('ticker_equal_n')})"
    )
    lines.append("")
    lines.append("## Cohorts")
    lines.append("")
    hi = ow.get("cohort_score_ge_060_5d_loss") or {}
    mid = ow.get("cohort_score_048_052_5d_win") or {}
    lines.append(
        f"- score≥0.60 & 5D loss: n={hi.get('n')} mean_score={hi.get('mean_total_score')} "
        f"mean_5d={hi.get('mean_return_5d')} factors={hi.get('mean_factors')}"
    )
    lines.append(
        f"- score 0.48–0.52 & 5D win: n={mid.get('n')} mean_score={mid.get('mean_total_score')} "
        f"mean_5d={mid.get('mean_return_5d')} factors={mid.get('mean_factors')}"
    )
    lines.append("")
    lines.append("## Disclaimer")
    lines.append("")
    lines.append(str(report.get("disclaimer") or ""))
    lines.append("")
    return "\n".join(lines)


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    # Union of keys preserving first-row order then extras
    keys: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                keys.append(k)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in keys})


def write_factor_analysis_outputs(
    report: Dict[str, Any],
    observations: Sequence[Dict[str, Any]],
    output_dir: Path,
) -> Dict[str, Path]:
    out_dir = Path(output_dir) / "quality" / "factor_analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    start = report.get("start_trade_date") or "UNKNOWN"
    end = report.get("end_trade_date") or "UNKNOWN"
    market = report.get("market") or "UNKNOWN"
    stem = f"screener_factor_analysis_{start}_{end}_{market}"
    json_path = out_dir / f"{stem}.json"
    md_path = out_dir / f"{stem}.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    md_path.write_text(render_factor_markdown(report), encoding="utf-8")

    obs_csv = out_dir / "factor_observations.csv"
    sp_csv = out_dir / "factor_spearman.csv"
    buck_csv = out_dir / "factor_buckets.csv"
    corr_csv = out_dir / "factor_correlation.csv"
    _write_csv(obs_csv, flatten_observation_csv_rows(observations))
    _write_csv(sp_csv, flatten_spearman_csv(report))
    _write_csv(
        buck_csv,
        list((report.get("observation_weighted") or {}).get("factor_quintiles_5d") or []),
    )
    _write_csv(corr_csv, flatten_correlation_csv(report))
    return {
        "json": json_path,
        "md": md_path,
        "factor_observations": obs_csv,
        "factor_spearman": sp_csv,
        "factor_buckets": buck_csv,
        "factor_correlation": corr_csv,
    }


def load_observation_ledger(ledger_path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not ledger_path.exists():
        return rows
    with open(ledger_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def discover_runs_in_window(
    output_dir: Path,
    *,
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
) -> Any:
    from screener_quality import discover_decision_runs, _merged_of

    discovered = discover_decision_runs(
        Path(output_dir),
        market=market,
        session=session,
        days=0,  # all dates; filter below
        decision_only=True,
    )
    kept_dirs: List[Path] = []
    kept_meta: Dict[str, Dict[str, Any]] = {}
    for rd in discovered.run_dirs:
        meta = discovered.merged_by_run.get(str(rd)) or _merged_of(Path(rd))
        td = str(meta.get("trade_date") or "")
        if td < str(start_trade_date) or td > str(end_trade_date):
            continue
        kept_dirs.append(Path(rd))
        kept_meta[str(rd)] = meta
    discovered.run_dirs = kept_dirs
    discovered.merged_by_run = kept_meta
    discovered.discovery = dict(discovered.discovery or {})
    discovered.discovery["factor_analysis_window"] = {
        "from": start_trade_date,
        "to": end_trade_date,
        "included_run_count": len(kept_dirs),
    }
    return discovered


def run_factor_analysis(
    *,
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
    output_dir: Path,
    config_path: Optional[Path] = None,
    settle: bool = False,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Path]]:
    """End-to-end read-only analysis. Optionally settle ledger (outcomes only)."""
    from screener_quality import load_runtime_config, build_observation_rows_from_run

    cfg = load_runtime_config(config_path)
    weights = production_factor_weights(cfg)
    output_dir = Path(output_dir)
    discovered = discover_runs_in_window(
        output_dir,
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
    )

    ledger = output_dir / "quality" / "screener_candidate_observations.jsonl"
    rows = load_observation_ledger(ledger)

    if settle and discovered.run_dirs:
        # Rebuild stubs + settle — does not touch DECISION artifacts / weights.
        from screener_quality import upsert_observation_ledger
        from screener_outcomes import backfill_candidate_outcomes, clear_ohlcv_cache

        stubs: List[Dict[str, Any]] = []
        for rd in discovered.run_dirs:
            stubs.extend(build_observation_rows_from_run(rd, output_dir=output_dir))
        upsert_observation_ledger(ledger, stubs)
        clear_ohlcv_cache()
        existing = load_observation_ledger(ledger)
        settled = backfill_candidate_outcomes(
            existing, as_of_trade_date=end_trade_date, only_trusted=False
        )
        upsert_observation_ledger(ledger, settled)
        rows = settled

    observations, schema, w = build_factor_observations(
        rows,
        run_dirs=discovered.run_dirs,
        merged_by_run=discovered.merged_by_run,
        weights=weights,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
        market=market,
        session=session,
    )
    report = analyze_factor_dataset(
        observations,
        schema_inspection=schema,
        weights=w,
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
    )
    report["discovery"] = discovered.discovery
    paths = write_factor_analysis_outputs(report, observations, output_dir)
    return report, observations, paths


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Production score factor decomposition (offline)"
    )
    parser.add_argument("--market", default=os.getenv("MARKET", "SP500"))
    parser.add_argument("--session", default="pm")
    parser.add_argument("--from", dest="date_from", required=True, help="YYYYMMDD")
    parser.add_argument("--to", dest="date_to", required=True, help="YYYYMMDD")
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", str(Path(__file__).resolve().parents[1] / "output")),
    )
    parser.add_argument("--config", default=None)
    parser.add_argument(
        "--settle",
        action="store_true",
        default=False,
        help="Optionally backfill outcomes into observation ledger (still read-only for Production score)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    report, observations, paths = run_factor_analysis(
        market=str(args.market).upper(),
        session=str(args.session).lower(),
        start_trade_date=str(args.date_from),
        end_trade_date=str(args.date_to),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config) if args.config else None,
        settle=bool(args.settle),
    )
    payload = {
        "json": str(paths["json"]),
        "md": str(paths["md"]),
        "observations": len(observations),
        "factors": report.get("production_factor_names"),
        "total_spearman_5d": (
            (report.get("observation_weighted") or {}).get("total_score_spearman") or {}
        ).get("spearman_5d"),
        "top5_negative_5d": (report.get("observation_weighted") or {}).get("top5_negative_5d"),
        "top5_positive_5d": (report.get("observation_weighted") or {}).get("top5_positive_5d"),
        "findings": [
            {"type": f.get("type"), "factor": f.get("factor") or f.get("factors")}
            for f in (report.get("primary_findings") or [])[:12]
        ],
        "production_policy_unchanged": True,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0 if observations else 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(main())
