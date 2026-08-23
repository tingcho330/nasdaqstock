"""Read-only Offline Weight Simulator for Production Screener factors.

Compares exploratory weight scenarios against settled Production observations.
Never mutates Production config, screener.py, thresholds, trader, GPT,
DECISION artifacts, diagnostics, or DB.

Score semantics mirror screener.py:
  total = fin*w_fin + tech*w_tech + mkt*w_mkt + sector*w_sector
        + vol*w_vol + pos*w_pos
  total = clip(total, 0, 1)   # no normalize-to-1.0
  Score = round(total, 4)

Production config weights currently sum to 0.90 (not 1.0); that is intentional.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("screener_weight_simulation")

# ── Constants (exploratory offline only; not Production writes) ─────

FACTOR_NAMES: Tuple[str, ...] = (
    "fin_score",
    "tech_score",
    "market_score",
    "sector_score",
    "vol_kki",
    "pos_52w",
)

PRODUCTION_THRESHOLD = 0.48
BASELINE_SCORE_TOLERANCE = 1e-4  # Score is round(..., 4)
WEIGHT_SUM_EXPECTED = 0.90
WEIGHT_SUM_TOLERANCE = 1e-9

DEFAULT_TRAIN_END = "20260807"
DEFAULT_VALIDATION_START = "20260810"

STATUS_OK = "OK"
STATUS_BASELINE_FAILED = "BASELINE_RECONSTRUCTION_FAILED"
SCOPE_NOTE = "CANDIDATE_SET_COUNTERFACTUAL_ONLY"

FINDING_IMPROVES_RANKING = "IMPROVES_RANKING"
FINDING_IMPROVES_VALIDATION = "IMPROVES_VALIDATION"
FINDING_DEGRADES_VALIDATION = "DEGRADES_VALIDATION"
FINDING_OUTLIER_DEPENDENT = "OUTLIER_DEPENDENT"
FINDING_OVERFIT_RISK = "OVERFIT_RISK"
FINDING_LOW_SAMPLE = "LOW_SAMPLE"
FINDING_COUNTERFACTUAL = "CANDIDATE_SET_COUNTERFACTUAL_ONLY"

HORIZONS = (1, 3, 5, 10)
RETURN_KEYS = {h: f"return_{h}d_pct" for h in HORIZONS}

SCENARIOS: Dict[str, Dict[str, float]] = {
    "A_BASELINE": {
        "fin_score": 0.25,
        "tech_score": 0.35,
        "market_score": 0.10,
        "sector_score": 0.10,
        "vol_kki": 0.05,
        "pos_52w": 0.05,
    },
    "B_TECH_DOWN": {
        "fin_score": 0.30,
        "tech_score": 0.25,
        "market_score": 0.10,
        "sector_score": 0.10,
        "vol_kki": 0.10,
        "pos_52w": 0.05,
    },
    "C_POS52W_ZERO": {
        "fin_score": 0.25,
        "tech_score": 0.35,
        "market_score": 0.10,
        "sector_score": 0.10,
        "vol_kki": 0.10,
        "pos_52w": 0.00,
    },
    "D_TECH_POS_DOWN": {
        "fin_score": 0.30,
        "tech_score": 0.20,
        "market_score": 0.10,
        "sector_score": 0.10,
        "vol_kki": 0.20,
        "pos_52w": 0.00,
    },
    "E_CONSERVATIVE": {
        "fin_score": 0.35,
        "tech_score": 0.15,
        "market_score": 0.10,
        "sector_score": 0.10,
        "vol_kki": 0.20,
        "pos_52w": 0.00,
    },
}


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
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


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _clip01(x: float) -> float:
    return float(max(0.0, min(1.0, x)))


def weight_sum(weights: Dict[str, float]) -> float:
    return round(float(sum(float(weights.get(f, 0.0)) for f in FACTOR_NAMES)), 10)


def assert_weight_semantics(weights: Dict[str, float], *, label: str = "") -> None:
    """Do not normalize; Production keeps sum≈0.90 as-is."""
    s = weight_sum(weights)
    if abs(s - WEIGHT_SUM_EXPECTED) > 1e-6:
        logger.warning(
            "Weight sum for %s is %.6f (expected ~%.2f); using as-is without normalize",
            label or "scenario",
            s,
            WEIGHT_SUM_EXPECTED,
        )


def production_baseline_weights(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    """Read Production weights (read-only). Prefer config; fall back to A_BASELINE."""
    from screener_factor_analysis import production_factor_weights

    if cfg is None:
        try:
            from screener_quality import load_runtime_config

            cfg = load_runtime_config()
        except Exception:
            cfg = {}
    w = production_factor_weights(cfg if isinstance(cfg, dict) else {})
    # Prefer explicit Production config values (sum 0.90). If defaults from
    # PRODUCTION_FACTOR_SPECS differ, overlay A_BASELINE when config empty.
    params = (cfg or {}).get("screener_params") if isinstance(cfg, dict) else {}
    if not isinstance(params, dict) or not params:
        return dict(SCENARIOS["A_BASELINE"])
    out = dict(SCENARIOS["A_BASELINE"])
    out.update({k: float(v) for k, v in w.items() if k in FACTOR_NAMES})
    return out


def compute_scenario_score(
    factors: Dict[str, Optional[float]],
    weights: Dict[str, float],
    *,
    missing_as_zero: bool = True,
) -> Optional[float]:
    """Mirror screener.py weighted sum + clip + round(4). No normalize-to-1."""
    present = 0
    raw = 0.0
    for name in FACTOR_NAMES:
        v = _safe_float(factors.get(name))
        w = float(weights.get(name, 0.0))
        if v is None:
            if not missing_as_zero:
                return None
            v = 0.0
        else:
            present += 1
        raw += v * w
    if present == 0:
        return None
    return round(_clip01(raw), 4)


def reconstruct_baseline_scores(
    observations: Sequence[Dict[str, Any]],
    baseline_weights: Dict[str, float],
    *,
    tolerance: float = BASELINE_SCORE_TOLERANCE,
) -> Dict[str, Any]:
    """Recompute Production score from factors; abort simulation on mismatch."""
    errors: List[Dict[str, Any]] = []
    max_err = 0.0
    n_checked = 0
    n_skipped_missing = 0
    for obs in observations:
        factors = obs.get("factors") or {}
        if not isinstance(factors, dict):
            factors = {}
        # Prefer nested factors; also accept flat factor_* already hydrated
        recon = compute_scenario_score(factors, baseline_weights)
        prod = _safe_float(
            obs.get("total_score")
            if obs.get("total_score") is not None
            else obs.get("decision_score")
        )
        if recon is None or prod is None:
            n_skipped_missing += 1
            continue
        # Market regime adjustment may sit on Production Score after the weighted
        # sum. Prefer matching the pure weighted formula when contribution_sum
        # is available and closer; otherwise require Score match.
        contrib = _safe_float(obs.get("contribution_sum"))
        target = prod
        if contrib is not None:
            contrib_r = round(_clip01(contrib), 4)
            # If Score diverges from contribution (market adj), validate against
            # contribution_sum — that is the weight formula under test.
            if abs(prod - contrib_r) > tolerance and abs(recon - contrib_r) <= tolerance:
                target = contrib_r
        err = abs(recon - target)
        n_checked += 1
        if err > max_err:
            max_err = err
        if err > tolerance:
            errors.append(
                {
                    "ticker": obs.get("ticker"),
                    "trade_date": obs.get("trade_date"),
                    "production_score": prod,
                    "reconstructed_score": recon,
                    "contribution_sum": contrib,
                    "abs_error": err,
                }
            )
    ok = n_checked > 0 and len(errors) == 0
    return {
        "ok": ok,
        "status": STATUS_OK if ok else STATUS_BASELINE_FAILED,
        "n_checked": n_checked,
        "n_skipped_missing_factors": n_skipped_missing,
        "max_abs_error": max_err,
        "tolerance": tolerance,
        "failures": errors[:20],
        "failure_count": len(errors),
        "weight_sum": weight_sum(baseline_weights),
        "note": (
            "Reconstruction uses screener.py semantics: sum(factor*weight), "
            "clip[0,1], round 4dp; no weight normalize. If Production Score "
            "includes post-sum market adjustment, contribution_sum is the "
            "weight-formula target."
        ),
    }


def hydrate_observation_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize CSV / JSON observation into simulator shape."""
    out = dict(row)
    factors: Dict[str, Optional[float]] = {}
    if isinstance(row.get("factors"), dict):
        for name in FACTOR_NAMES:
            factors[name] = _safe_float(row["factors"].get(name))
    else:
        for name in FACTOR_NAMES:
            v = row.get(f"factor_{name}")
            if v is None:
                v = row.get(name)
            factors[name] = _safe_float(v)
    out["factors"] = factors
    if out.get("total_score") is None:
        out["total_score"] = _safe_float(row.get("decision_score") or row.get("score"))
    else:
        out["total_score"] = _safe_float(out.get("total_score"))
    out["ticker"] = str(row.get("ticker") or "").upper()
    out["trade_date"] = str(row.get("trade_date") or "")
    for h in HORIZONS:
        key = RETURN_KEYS[h]
        out[key] = _safe_float(row.get(key))
    out["max_drawdown_5d_pct"] = _safe_float(row.get("max_drawdown_5d_pct"))
    out["max_runup_5d_pct"] = _safe_float(row.get("max_runup_5d_pct"))
    out["contribution_sum"] = _safe_float(row.get("contribution_sum"))
    # Preserve analysis filters if present
    if "candidate_type" not in out:
        out["candidate_type"] = row.get("candidate_type") or "PRODUCTION"
    if "trusted_for_analysis" not in out:
        # factor_observations.csv is already PRODUCTION trusted settled
        out["trusted_for_analysis"] = row.get("trusted_for_analysis", True)
    if "market" not in out:
        out["market"] = row.get("market")
    if "session" not in out:
        out["session"] = row.get("session")
    return out


def load_factor_observations_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            rows.append(hydrate_observation_row(dict(raw)))
    return rows


def load_simulation_observations(
    *,
    output_dir: Path,
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Prefer factor_observations.csv; else rebuild read-only from ledger+runs."""
    from screener_factor_analysis import (
        build_factor_observations,
        discover_runs_in_window,
        filter_production_analysis_rows,
        load_observation_ledger,
        production_factor_weights,
    )

    output_dir = Path(output_dir)
    meta: Dict[str, Any] = {"source": None}
    csv_path = output_dir / "quality" / "factor_analysis" / "factor_observations.csv"
    rows = load_factor_observations_csv(csv_path)
    if rows:
        meta["source"] = str(csv_path)
        # CSV may lack market/session/candidate_type — assume already scoped,
        # but still enforce window + settled + production filters when fields exist.
        filtered: List[Dict[str, Any]] = []
        for r in rows:
            td = str(r.get("trade_date") or "")
            if td < start_trade_date or td > end_trade_date:
                continue
            mkt = str(r.get("market") or market).upper()
            sess = str(r.get("session") or session).lower()
            if mkt and mkt != str(market).upper():
                continue
            if sess and sess != str(session).lower():
                continue
            ct = str(r.get("candidate_type") or "PRODUCTION")
            if ct and ct != "PRODUCTION":
                continue
            trusted = r.get("trusted_for_analysis")
            if trusted in (False, "False", "false", "0", 0):
                continue
            if _safe_float(r.get("return_5d_pct")) is None:
                continue
            if r.get("market") is None:
                r["market"] = market
            if r.get("session") is None:
                r["session"] = session
            filtered.append(r)
        meta["n_loaded"] = len(rows)
        meta["n_after_filter"] = len(filtered)
        return filtered, meta

    # Fallback: read-only join from ledger + DECISION score artifacts
    ledger = output_dir / "quality" / "screener_candidate_observations.jsonl"
    ledger_rows = load_observation_ledger(ledger)
    discovered = discover_runs_in_window(
        output_dir,
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
    )
    weights = production_factor_weights(cfg or {})
    enriched, schema, _w = build_factor_observations(
        ledger_rows,
        run_dirs=discovered.run_dirs,
        merged_by_run=discovered.merged_by_run,
        weights=weights,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
        market=market,
        session=session,
    )
    meta["source"] = "ledger+decision_artifacts"
    meta["schema"] = schema
    meta["n_after_filter"] = len(enriched)
    return enriched, meta


def split_train_validation(
    observations: Sequence[Dict[str, Any]],
    *,
    train_end: str = DEFAULT_TRAIN_END,
    validation_start: str = DEFAULT_VALIDATION_START,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    train = [r for r in observations if str(r.get("trade_date") or "") <= train_end]
    valid = [
        r for r in observations if str(r.get("trade_date") or "") >= validation_start
    ]
    return train, valid


def _percentile(sorted_vals: Sequence[float], q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    n = len(sorted_vals)
    if n == 1:
        return float(sorted_vals[0])
    idx = int(round(q * (n - 1)))
    idx = max(0, min(n - 1, idx))
    return float(sorted_vals[idx])


def outcome_metrics(
    rows: Sequence[Dict[str, Any]],
    *,
    score_key: str = "scenario_score",
    horizon: int = 5,
) -> Dict[str, Any]:
    """Observation-weighted outcome stats for a row set."""
    from screener_factor_analysis import first_signal_only
    from screener_outcomes import spearman_corr

    rkey = RETURN_KEYS[horizon]
    rets = [_safe_float(r.get(rkey)) for r in rows]
    ok_pairs = [
        (r, v)
        for r, v in zip(rows, rets)
        if v is not None
    ]
    matured = [v for _, v in ok_pairs]
    n = len(rows)
    nm = len(matured)
    tickers = {str(r.get("ticker") or "").upper() for r in rows if r.get("ticker")}
    if nm == 0:
        return {
            "observations": n,
            "mature_5d_count": 0,
            "unique_tickers": len(tickers),
            "mean_5d": None,
            "median_5d": None,
            "win_rate": None,
            "p25": None,
            "p75": None,
            "mean_mdd_5d": None,
            "mean_runup_5d": None,
            "ticker_equal_weight_mean_5d": None,
            "first_signal_mean_5d": None,
            "first_signal_median_5d": None,
            "first_signal_win_rate": None,
            "first_signal_n": 0,
            "spearman_1d": None,
            "spearman_3d": None,
            "spearman_5d": None,
            "spearman_10d": None,
        }

    matured_sorted = sorted(matured)
    wins = sum(1 for v in matured if v > 0)
    mdds = [
        _safe_float(r.get("max_drawdown_5d_pct"))
        for r, v in ok_pairs
    ]
    mdds_ok = [v for v in mdds if v is not None]
    runups = [
        _safe_float(r.get("max_runup_5d_pct"))
        for r, v in ok_pairs
    ]
    runups_ok = [v for v in runups if v is not None]

    by_ticker: Dict[str, List[float]] = defaultdict(list)
    for r, v in ok_pairs:
        t = str(r.get("ticker") or "").upper()
        if t:
            by_ticker[t].append(v)
    tmeans = [sum(vs) / len(vs) for vs in by_ticker.values()] if by_ticker else []

    first = first_signal_only(list(rows))
    first_rets = [_safe_float(r.get(rkey)) for r in first]
    first_ok = [v for v in first_rets if v is not None]
    first_sorted = sorted(first_ok)

    spearman: Dict[str, Optional[float]] = {}
    for h in HORIZONS:
        xs: List[float] = []
        ys: List[float] = []
        hk = RETURN_KEYS[h]
        for r in rows:
            x = _safe_float(r.get(score_key))
            y = _safe_float(r.get(hk))
            if x is not None and y is not None:
                xs.append(x)
                ys.append(y)
        spearman[f"spearman_{h}d"] = spearman_corr(xs, ys)

    return {
        "observations": n,
        "mature_5d_count": nm,
        "unique_tickers": len(tickers),
        "mean_5d": round(sum(matured) / nm, 6),
        "median_5d": matured_sorted[nm // 2],
        "win_rate": round(wins / nm, 6),
        "p25": _percentile(matured_sorted, 0.25),
        "p75": _percentile(matured_sorted, 0.75),
        "mean_mdd_5d": (
            round(sum(mdds_ok) / len(mdds_ok), 6) if mdds_ok else None
        ),
        "mean_runup_5d": (
            round(sum(runups_ok) / len(runups_ok), 6) if runups_ok else None
        ),
        "ticker_equal_weight_mean_5d": (
            round(sum(tmeans) / len(tmeans), 6) if tmeans else None
        ),
        "first_signal_mean_5d": (
            round(sum(first_ok) / len(first_ok), 6) if first_ok else None
        ),
        "first_signal_median_5d": (
            first_sorted[len(first_sorted) // 2] if first_sorted else None
        ),
        "first_signal_win_rate": (
            round(sum(1 for v in first_ok if v > 0) / len(first_ok), 6)
            if first_ok
            else None
        ),
        "first_signal_n": len(first_ok),
        **spearman,
    }


def apply_scenario_scores(
    observations: Sequence[Dict[str, Any]],
    weights: Dict[str, float],
    *,
    baseline_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for obs in observations:
        row = dict(obs)
        factors = obs.get("factors") or {}
        row["baseline_score"] = compute_scenario_score(factors, baseline_weights)
        row["scenario_score"] = compute_scenario_score(factors, weights)
        # Contributions under this scenario
        contribs: Dict[str, Optional[float]] = {}
        for name in FACTOR_NAMES:
            v = _safe_float(factors.get(name))
            w = float(weights.get(name, 0.0))
            contribs[name] = (round(v * w, 8) if v is not None else None)
        row["scenario_contributions"] = contribs
        out.append(row)
    return out


def threshold_pass_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    score_key: str = "scenario_score",
    threshold: float = PRODUCTION_THRESHOLD,
) -> List[Dict[str, Any]]:
    return [
        r
        for r in rows
        if (_safe_float(r.get(score_key)) is not None)
        and (_safe_float(r.get(score_key)) or 0.0) >= threshold
    ]


def top_n_rows(
    rows: Sequence[Dict[str, Any]],
    n: int,
    *,
    score_key: str = "scenario_score",
) -> List[Dict[str, Any]]:
    scored = [
        r for r in rows if _safe_float(r.get(score_key)) is not None
    ]
    scored.sort(
        key=lambda r: (
            -float(_safe_float(r.get(score_key)) or 0.0),
            str(r.get("trade_date") or ""),
            str(r.get("ticker") or ""),
        )
    )
    return scored[:n]


def mean_factor_contributions(rows: Sequence[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    sums = {f: 0.0 for f in FACTOR_NAMES}
    counts = {f: 0 for f in FACTOR_NAMES}
    for r in rows:
        contribs = r.get("scenario_contributions") or {}
        for f in FACTOR_NAMES:
            v = _safe_float(contribs.get(f))
            if v is not None:
                sums[f] += v
                counts[f] += 1
    return {
        f: (round(sums[f] / counts[f], 6) if counts[f] else None) for f in FACTOR_NAMES
    }


def candidate_migration(
    rows: Sequence[Dict[str, Any]],
    *,
    threshold: float = PRODUCTION_THRESHOLD,
) -> Dict[str, Any]:
    both: List[Dict[str, Any]] = []
    base_only: List[Dict[str, Any]] = []
    scen_only: List[Dict[str, Any]] = []
    for r in rows:
        b = _safe_float(r.get("baseline_score"))
        s = _safe_float(r.get("scenario_score"))
        if b is None or s is None:
            continue
        bp = b >= threshold
        sp = s >= threshold
        rec = {
            "ticker": r.get("ticker"),
            "trade_date": r.get("trade_date"),
            "baseline_score": b,
            "scenario_score": s,
            "return_5d_pct": _safe_float(r.get("return_5d_pct")),
        }
        if bp and sp:
            both.append(rec)
        elif bp and not sp:
            base_only.append(rec)
        elif (not bp) and sp:
            scen_only.append(rec)
    return {
        "baseline_pass_and_scenario_pass": both,
        "baseline_pass_scenario_fail": base_only,
        "baseline_fail_scenario_pass": scen_only,
        "counts": {
            "baseline_pass_and_scenario_pass": len(both),
            "baseline_pass_scenario_fail": len(base_only),
            "baseline_fail_scenario_pass": len(scen_only),
        },
        "limitation": SCOPE_NOTE,
        "note": (
            "Migration is within the existing Production candidate observation "
            "set only; names that never passed Production threshold may be absent."
        ),
    }


def high_score_loss_cohort(
    rows: Sequence[Dict[str, Any]],
    *,
    score_floor: float = 0.60,
) -> List[Dict[str, Any]]:
    """Auto-discover score>=floor & 5D loss; do not hardcode n."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        # Cohort defined on Production / baseline score
        score = _safe_float(r.get("baseline_score"))
        if score is None:
            score = _safe_float(r.get("total_score") or r.get("decision_score"))
        ret = _safe_float(r.get("return_5d_pct"))
        if score is not None and score >= score_floor and ret is not None and ret < 0:
            out.append(r)
    return out


def cohort_movement(
    cohort: Sequence[Dict[str, Any]],
    all_scored: Sequence[Dict[str, Any]],
    *,
    threshold: float = PRODUCTION_THRESHOLD,
) -> Dict[str, Any]:
    """How high-score losers move under the scenario."""
    if not cohort:
        return {
            "n": 0,
            "mean_baseline_score": None,
            "mean_scenario_score": None,
            "mean_score_delta": None,
            "dropped_below_threshold": 0,
            "mean_rank_delta": None,
        }

    # Ranks within full set by baseline / scenario score (1 = best)
    def _ranks(score_key: str) -> Dict[Tuple[str, str], float]:
        items = [
            (str(r.get("ticker") or ""), str(r.get("trade_date") or ""), _safe_float(r.get(score_key)))
            for r in all_scored
        ]
        items = [(t, d, s) for t, d, s in items if s is not None]
        items.sort(key=lambda x: (-x[2], x[1], x[0]))
        ranks: Dict[Tuple[str, str], float] = {}
        for i, (t, d, _s) in enumerate(items):
            ranks[(t, d)] = float(i + 1)
        return ranks

    br = _ranks("baseline_score")
    sr = _ranks("scenario_score")
    b_scores = [_safe_float(r.get("baseline_score")) for r in cohort]
    s_scores = [_safe_float(r.get("scenario_score")) for r in cohort]
    bok = [v for v in b_scores if v is not None]
    sok = [v for v in s_scores if v is not None]
    dropped = sum(
        1
        for r in cohort
        if (_safe_float(r.get("baseline_score")) or 0) >= threshold
        and (_safe_float(r.get("scenario_score")) is not None)
        and (_safe_float(r.get("scenario_score")) or 0) < threshold
    )
    rank_deltas: List[float] = []
    for r in cohort:
        key = (str(r.get("ticker") or ""), str(r.get("trade_date") or ""))
        if key in br and key in sr:
            rank_deltas.append(sr[key] - br[key])  # positive = worse rank
    return {
        "n": len(cohort),
        "mean_baseline_score": round(sum(bok) / len(bok), 6) if bok else None,
        "mean_scenario_score": round(sum(sok) / len(sok), 6) if sok else None,
        "mean_score_delta": (
            round((sum(sok) / len(sok)) - (sum(bok) / len(bok)), 6)
            if bok and sok
            else None
        ),
        "dropped_below_threshold": dropped,
        "mean_rank_delta": (
            round(sum(rank_deltas) / len(rank_deltas), 6) if rank_deltas else None
        ),
    }


def _delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return round(a - b, 6)


def deltas_vs_baseline(
    scenario_metrics: Dict[str, Any],
    baseline_metrics: Dict[str, Any],
    *,
    pass_count: Optional[int] = None,
    baseline_pass_count: Optional[int] = None,
) -> Dict[str, Any]:
    keys = [
        "spearman_1d",
        "spearman_3d",
        "spearman_5d",
        "spearman_10d",
        "mean_5d",
        "median_5d",
        "win_rate",
        "mean_mdd_5d",
        "ticker_equal_weight_mean_5d",
        "first_signal_mean_5d",
        "first_signal_median_5d",
        "first_signal_win_rate",
    ]
    out = {k: _delta(scenario_metrics.get(k), baseline_metrics.get(k)) for k in keys}
    if pass_count is not None and baseline_pass_count is not None:
        out["candidate_count"] = pass_count - baseline_pass_count
    return out


def detect_outlier_dependent(
    metrics: Dict[str, Any],
    *,
    mean_median_gap: float = 2.0,
) -> bool:
    """Warn when mean is buoyed by outliers vs median / ticker-EW."""
    mean_v = _safe_float(metrics.get("mean_5d"))
    med_v = _safe_float(metrics.get("median_5d"))
    tew = _safe_float(metrics.get("ticker_equal_weight_mean_5d"))
    if mean_v is None or med_v is None:
        return False
    if mean_v - med_v >= mean_median_gap:
        return True
    if tew is not None and mean_v - tew >= mean_median_gap:
        return True
    if med_v < 0 and mean_v > 0:
        return True
    if tew is not None and tew < 0 and mean_v > 0:
        return True
    return False


def evaluate_scenario_window(
    observations: Sequence[Dict[str, Any]],
    weights: Dict[str, float],
    *,
    baseline_weights: Dict[str, float],
    threshold: float = PRODUCTION_THRESHOLD,
    top_ns: Sequence[int] = (3, 5, 10),
) -> Dict[str, Any]:
    scored = apply_scenario_scores(
        observations, weights, baseline_weights=baseline_weights
    )
    # Ranking quality on full observation set (not just passers)
    ranking = outcome_metrics(scored, score_key="scenario_score")

    passed = threshold_pass_rows(scored, threshold=threshold)
    counterfactual = outcome_metrics(passed, score_key="scenario_score")
    counterfactual["pass_count"] = len(passed)

    topn_block: Dict[str, Any] = {}
    for n in top_ns:
        top = top_n_rows(scored, n)
        m = outcome_metrics(top, score_key="scenario_score")
        topn_block[f"top_{n}"] = m

    cohort = high_score_loss_cohort(scored)
    migration = candidate_migration(scored, threshold=threshold)

    return {
        "scored_observations": scored,
        "ranking_quality": ranking,
        "threshold_counterfactual": counterfactual,
        "top_n": topn_block,
        "mean_factor_contributions": mean_factor_contributions(scored),
        "high_score_loss_cohort": cohort_movement(cohort, scored, threshold=threshold),
        "candidate_migration": migration,
        "scope": SCOPE_NOTE,
    }


def rank_scenarios(
    scenario_results: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Multi-criteria ranking; never mean-only BEST."""

    def _key(name: str) -> Tuple:
        val = scenario_results[name]
        vm = (val.get("validation") or {}).get("threshold_counterfactual") or {}
        vr = (val.get("validation") or {}).get("ranking_quality") or {}
        # Higher better except MDD (less negative / higher is better if MDD is negative)
        med = _safe_float(vm.get("median_5d"))
        tew = _safe_float(vm.get("ticker_equal_weight_mean_5d"))
        wr = _safe_float(vm.get("win_rate"))
        mdd = _safe_float(vm.get("mean_mdd_5d"))
        sp = _safe_float(vr.get("spearman_5d"))
        fs = _safe_float(vm.get("first_signal_median_5d"))
        # MDD: prefer higher (closer to 0 / less drawdown if stored negative)
        return (
            med if med is not None else -1e9,
            tew if tew is not None else -1e9,
            wr if wr is not None else -1e9,
            mdd if mdd is not None else -1e9,
            sp if sp is not None else -1e9,
            fs if fs is not None else -1e9,
            name,
        )

    names = list(scenario_results.keys())
    names.sort(key=_key, reverse=True)
    ranked = []
    for i, name in enumerate(names):
        ranked.append({"rank": i + 1, "scenario": name, "criteria": "validation_priority"})
    return ranked


def build_findings(
    scenario_results: Dict[str, Dict[str, Any]],
    *,
    baseline_name: str = "A_BASELINE",
    low_sample_n: int = 8,
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = [
        {
            "type": FINDING_COUNTERFACTUAL,
            "detail": (
                "All threshold / migration results are counterfactual within the "
                "existing Production candidate observation set only."
            ),
        }
    ]
    base = scenario_results.get(baseline_name) or {}
    base_val = (base.get("validation") or {}).get("threshold_counterfactual") or {}
    base_rank_val = (base.get("validation") or {}).get("ranking_quality") or {}
    base_rank_full = (base.get("full") or {}).get("ranking_quality") or {}

    for name, block in scenario_results.items():
        if name == baseline_name:
            continue
        full = block.get("full") or {}
        train = block.get("train") or {}
        valid = block.get("validation") or {}
        fr = full.get("ranking_quality") or {}
        vr = valid.get("ranking_quality") or {}
        vt = valid.get("threshold_counterfactual") or {}
        tt = train.get("threshold_counterfactual") or {}

        n_val = int(vt.get("mature_5d_count") or vt.get("observations") or 0)
        if n_val < low_sample_n:
            findings.append(
                {
                    "type": FINDING_LOW_SAMPLE,
                    "scenario": name,
                    "detail": f"validation mature n={n_val} < {low_sample_n}",
                }
            )

        sp_full = _safe_float(fr.get("spearman_5d"))
        sp_base_full = _safe_float(base_rank_full.get("spearman_5d"))
        if (
            sp_full is not None
            and sp_base_full is not None
            and sp_full > sp_base_full + 0.02
        ):
            findings.append(
                {
                    "type": FINDING_IMPROVES_RANKING,
                    "scenario": name,
                    "detail": (
                        f"full 5D Spearman {sp_full} vs baseline {sp_base_full}"
                    ),
                }
            )

        med = _safe_float(vt.get("median_5d"))
        med_b = _safe_float(base_val.get("median_5d"))
        tew = _safe_float(vt.get("ticker_equal_weight_mean_5d"))
        tew_b = _safe_float(base_val.get("ticker_equal_weight_mean_5d"))
        wr = _safe_float(vt.get("win_rate"))
        wr_b = _safe_float(base_val.get("win_rate"))

        improves = 0
        degrades = 0
        for a, b in ((med, med_b), (tew, tew_b), (wr, wr_b)):
            if a is None or b is None:
                continue
            if a > b:
                improves += 1
            elif a < b:
                degrades += 1

        if improves >= 2 and degrades == 0:
            findings.append(
                {
                    "type": FINDING_IMPROVES_VALIDATION,
                    "scenario": name,
                    "detail": "validation median/TEW/win_rate mostly above baseline",
                }
            )
        if degrades >= 2 and improves == 0:
            findings.append(
                {
                    "type": FINDING_DEGRADES_VALIDATION,
                    "scenario": name,
                    "detail": "validation median/TEW/win_rate mostly below baseline",
                }
            )

        # Overfit: train improves, validation degrades on median
        t_med = _safe_float(tt.get("median_5d"))
        t_med_b = _safe_float(
            ((base.get("train") or {}).get("threshold_counterfactual") or {}).get(
                "median_5d"
            )
        )
        if (
            t_med is not None
            and t_med_b is not None
            and med is not None
            and med_b is not None
            and t_med > t_med_b
            and med < med_b
        ):
            findings.append(
                {
                    "type": FINDING_OVERFIT_RISK,
                    "scenario": name,
                    "detail": "train median up vs baseline but validation median down",
                }
            )

        if detect_outlier_dependent(vt) or detect_outlier_dependent(
            full.get("threshold_counterfactual") or {}
        ):
            findings.append(
                {
                    "type": FINDING_OUTLIER_DEPENDENT,
                    "scenario": name,
                    "detail": "mean elevated vs median and/or ticker-EW",
                }
            )

        # Direction consistency train/validation on median delta
        if (
            t_med is not None
            and t_med_b is not None
            and med is not None
            and med_b is not None
        ):
            if (t_med - t_med_b) * (med - med_b) < 0:
                # already covered by overfit when train up / val down; also flag reverse
                if not any(
                    f.get("type") == FINDING_OVERFIT_RISK and f.get("scenario") == name
                    for f in findings
                ):
                    findings.append(
                        {
                            "type": FINDING_OVERFIT_RISK,
                            "scenario": name,
                            "detail": "train/validation median deltas disagree in sign",
                        }
                    )

        sp_val = _safe_float(vr.get("spearman_5d"))
        sp_base_val = _safe_float(base_rank_val.get("spearman_5d"))
        if (
            sp_val is not None
            and sp_base_val is not None
            and sp_val < sp_base_val - 0.05
        ):
            findings.append(
                {
                    "type": FINDING_DEGRADES_VALIDATION,
                    "scenario": name,
                    "detail": (
                        f"validation 5D Spearman {sp_val} vs baseline {sp_base_val}"
                    ),
                }
            )

    return findings


def analyze_weight_scenarios(
    observations: Sequence[Dict[str, Any]],
    *,
    baseline_weights: Dict[str, float],
    scenarios: Optional[Dict[str, Dict[str, float]]] = None,
    threshold: float = PRODUCTION_THRESHOLD,
    train_end: str = DEFAULT_TRAIN_END,
    validation_start: str = DEFAULT_VALIDATION_START,
    market: str = "SP500",
    session: str = "pm",
    start_trade_date: str = "20260727",
    end_trade_date: str = "20260821",
) -> Dict[str, Any]:
    scenarios = scenarios or SCENARIOS
    assert_weight_semantics(baseline_weights, label="baseline")

    recon = reconstruct_baseline_scores(observations, baseline_weights)
    if not recon["ok"]:
        return {
            "status": STATUS_BASELINE_FAILED,
            "baseline_reconstruction": recon,
            "market": market,
            "session": session,
            "start_trade_date": start_trade_date,
            "end_trade_date": end_trade_date,
            "threshold": threshold,
            "observations": len(observations),
            "production_policy_unchanged": True,
            "recommendation": "NONE — offline diagnostic only; no Production change",
            "findings": [
                {
                    "type": STATUS_BASELINE_FAILED,
                    "detail": "Baseline score reconstruction failed; simulation aborted",
                }
            ],
        }

    train_rows, valid_rows = split_train_validation(
        observations, train_end=train_end, validation_start=validation_start
    )

    scenario_results: Dict[str, Dict[str, Any]] = {}
    for name, weights in scenarios.items():
        assert_weight_semantics(weights, label=name)
        full_block = evaluate_scenario_window(
            observations, weights, baseline_weights=baseline_weights, threshold=threshold
        )
        train_block = evaluate_scenario_window(
            train_rows, weights, baseline_weights=baseline_weights, threshold=threshold
        )
        valid_block = evaluate_scenario_window(
            valid_rows, weights, baseline_weights=baseline_weights, threshold=threshold
        )
        # Drop bulky scored lists from stored blocks (keep in migration CSV path)
        def _slim(block: Dict[str, Any]) -> Dict[str, Any]:
            slim = {
                k: v
                for k, v in block.items()
                if k != "scored_observations"
            }
            # Shrink migration detail lists in JSON (keep counts + sample)
            mig = slim.get("candidate_migration") or {}
            slim["candidate_migration"] = {
                "counts": mig.get("counts"),
                "limitation": mig.get("limitation"),
                "note": mig.get("note"),
                "baseline_pass_scenario_fail_sample": (
                    mig.get("baseline_pass_scenario_fail") or []
                )[:10],
                "baseline_fail_scenario_pass_sample": (
                    mig.get("baseline_fail_scenario_pass") or []
                )[:10],
            }
            return slim

        scenario_results[name] = {
            "weights": dict(weights),
            "weight_sum": weight_sum(weights),
            "full": _slim(full_block),
            "train": _slim(train_block),
            "validation": _slim(valid_block),
            # keep full migration for CSV export
            "_migration_full": full_block.get("candidate_migration"),
            "_scored_full": full_block.get("scored_observations"),
        }

    baseline_full = scenario_results["A_BASELINE"]["full"]
    baseline_val = scenario_results["A_BASELINE"]["validation"]
    for name, block in scenario_results.items():
        block["delta_vs_baseline_full"] = deltas_vs_baseline(
            (block["full"].get("threshold_counterfactual") or {}),
            (baseline_full.get("threshold_counterfactual") or {}),
            pass_count=(block["full"].get("threshold_counterfactual") or {}).get(
                "pass_count"
            ),
            baseline_pass_count=(baseline_full.get("threshold_counterfactual") or {}).get(
                "pass_count"
            ),
        )
        # Also ranking spearman deltas
        for spk in ("spearman_1d", "spearman_3d", "spearman_5d", "spearman_10d"):
            block["delta_vs_baseline_full"][spk] = _delta(
                (block["full"].get("ranking_quality") or {}).get(spk),
                (baseline_full.get("ranking_quality") or {}).get(spk),
            )
        block["delta_vs_baseline_validation"] = deltas_vs_baseline(
            (block["validation"].get("threshold_counterfactual") or {}),
            (baseline_val.get("threshold_counterfactual") or {}),
            pass_count=(block["validation"].get("threshold_counterfactual") or {}).get(
                "pass_count"
            ),
            baseline_pass_count=(
                baseline_val.get("threshold_counterfactual") or {}
            ).get("pass_count"),
        )
        for spk in ("spearman_1d", "spearman_3d", "spearman_5d", "spearman_10d"):
            block["delta_vs_baseline_validation"][spk] = _delta(
                (block["validation"].get("ranking_quality") or {}).get(spk),
                (baseline_val.get("ranking_quality") or {}).get(spk),
            )

    findings = build_findings(scenario_results)
    ranking = rank_scenarios(scenario_results)

    # Strip private keys before return
    public_scenarios: Dict[str, Any] = {}
    migration_rows: List[Dict[str, Any]] = []
    topn_rows_out: List[Dict[str, Any]] = []
    for name, block in scenario_results.items():
        mig = block.pop("_migration_full", None) or {}
        scored = block.pop("_scored_full", None) or []
        public_scenarios[name] = block
        for group in (
            "baseline_pass_and_scenario_pass",
            "baseline_pass_scenario_fail",
            "baseline_fail_scenario_pass",
        ):
            for rec in mig.get(group) or []:
                migration_rows.append(
                    {
                        "scenario": name,
                        "migration_group": group,
                        **rec,
                    }
                )
        for n_key, top_block in (block.get("full") or {}).get("top_n", {}).items():
            topn_rows_out.append(
                {
                    "scenario": name,
                    "window": "full",
                    "top_n": n_key,
                    **{k: top_block.get(k) for k in top_block},
                }
            )
        for n_key, top_block in (block.get("validation") or {}).get("top_n", {}).items():
            topn_rows_out.append(
                {
                    "scenario": name,
                    "window": "validation",
                    "top_n": n_key,
                    **{k: top_block.get(k) for k in top_block},
                }
            )

    return {
        "status": STATUS_OK,
        "baseline_reconstruction": recon,
        "market": market,
        "session": session,
        "start_trade_date": start_trade_date,
        "end_trade_date": end_trade_date,
        "train_end": train_end,
        "validation_start": validation_start,
        "threshold": threshold,
        "threshold_fixed": True,
        "observations": len(observations),
        "train_observations": len(train_rows),
        "validation_observations": len(valid_rows),
        "baseline_weights": dict(baseline_weights),
        "baseline_weight_sum": weight_sum(baseline_weights),
        "scenarios": public_scenarios,
        "scenario_ranking": ranking,
        "findings": findings,
        "scope": SCOPE_NOTE,
        "production_policy_unchanged": True,
        "recommendation": "NONE — offline diagnostic only; do not change Production",
        "_export_migration": migration_rows,
        "_export_topn": topn_rows_out,
    }


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
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


def flatten_scenario_summary(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name, block in (report.get("scenarios") or {}).items():
        for window in ("full", "train", "validation"):
            wb = block.get(window) or {}
            rq = wb.get("ranking_quality") or {}
            cf = wb.get("threshold_counterfactual") or {}
            delta_key = (
                "delta_vs_baseline_full"
                if window == "full"
                else (
                    "delta_vs_baseline_validation"
                    if window == "validation"
                    else None
                )
            )
            delta = block.get(delta_key) or {} if delta_key else {}
            rows.append(
                {
                    "scenario": name,
                    "window": window,
                    "weight_sum": block.get("weight_sum"),
                    "pass_count": cf.get("pass_count"),
                    "mature_5d_count": cf.get("mature_5d_count"),
                    "unique_tickers": cf.get("unique_tickers"),
                    "mean_5d": cf.get("mean_5d"),
                    "median_5d": cf.get("median_5d"),
                    "win_rate": cf.get("win_rate"),
                    "p25": cf.get("p25"),
                    "p75": cf.get("p75"),
                    "mean_mdd_5d": cf.get("mean_mdd_5d"),
                    "mean_runup_5d": cf.get("mean_runup_5d"),
                    "ticker_equal_weight_mean_5d": cf.get(
                        "ticker_equal_weight_mean_5d"
                    ),
                    "first_signal_mean_5d": cf.get("first_signal_mean_5d"),
                    "first_signal_median_5d": cf.get("first_signal_median_5d"),
                    "first_signal_win_rate": cf.get("first_signal_win_rate"),
                    "spearman_1d": rq.get("spearman_1d"),
                    "spearman_3d": rq.get("spearman_3d"),
                    "spearman_5d": rq.get("spearman_5d"),
                    "spearman_10d": rq.get("spearman_10d"),
                    "delta_spearman_5d": delta.get("spearman_5d"),
                    "delta_mean_5d": delta.get("mean_5d"),
                    "delta_median_5d": delta.get("median_5d"),
                    "delta_win_rate": delta.get("win_rate"),
                    "delta_mean_mdd_5d": delta.get("mean_mdd_5d"),
                    "delta_ticker_ew": delta.get("ticker_equal_weight_mean_5d"),
                    "delta_candidate_count": delta.get("candidate_count"),
                }
            )
    return rows


def flatten_validation_csv(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [r for r in flatten_scenario_summary(report) if r.get("window") == "validation"]


def render_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Screener Offline Weight Simulation")
    lines.append("")
    lines.append(
        f"- Window: `{report.get('start_trade_date')}`–`{report.get('end_trade_date')}` "
        f"`{report.get('market')}` `{report.get('session')}`"
    )
    lines.append(
        f"- Status: `{report.get('status')}` · threshold=`{report.get('threshold')}` (fixed)"
    )
    lines.append(
        f"- Observations: {report.get('observations')} "
        f"(train={report.get('train_observations')}, "
        f"validation={report.get('validation_observations')})"
    )
    lines.append(f"- Scope: `{report.get('scope')}`")
    lines.append("- Production policy: **unchanged** (offline diagnostic only)")
    lines.append("")

    recon = report.get("baseline_reconstruction") or {}
    lines.append("## Baseline reconstruction")
    lines.append(
        f"- max_abs_error=`{recon.get('max_abs_error')}` "
        f"tolerance=`{recon.get('tolerance')}` "
        f"n_checked=`{recon.get('n_checked')}` "
        f"weight_sum=`{recon.get('weight_sum')}`"
    )
    lines.append("")

    if report.get("status") == STATUS_BASELINE_FAILED:
        lines.append("Simulation aborted: `BASELINE_RECONSTRUCTION_FAILED`")
        lines.append("")
        lines.append("## Findings")
        for f in report.get("findings") or []:
            lines.append(f"- **{f.get('type')}**: {f.get('detail')}")
        return "\n".join(lines) + "\n"

    lines.append("## Scenario weights")
    for name, block in (report.get("scenarios") or {}).items():
        w = block.get("weights") or {}
        lines.append(
            f"- **{name}**: fin={w.get('fin_score')} tech={w.get('tech_score')} "
            f"mkt={w.get('market_score')} sector={w.get('sector_score')} "
            f"vol={w.get('vol_kki')} pos={w.get('pos_52w')} "
            f"TOTAL={block.get('weight_sum')}"
        )
    lines.append("")

    for window, title in (
        ("full", "Full"),
        ("train", "Train"),
        ("validation", "Validation"),
    ):
        lines.append(f"## {title}")
        lines.append("")
        lines.append(
            "| Scenario | pass | mean5 | med5 | win | MDD | TEW | sp5 | "
            "Δmed | Δsp5 |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for name, block in (report.get("scenarios") or {}).items():
            wb = block.get(window) or {}
            cf = wb.get("threshold_counterfactual") or {}
            rq = wb.get("ranking_quality") or {}
            if window == "full":
                d = block.get("delta_vs_baseline_full") or {}
            elif window == "validation":
                d = block.get("delta_vs_baseline_validation") or {}
            else:
                d = {}
            lines.append(
                f"| {name} | {cf.get('pass_count')} | {cf.get('mean_5d')} | "
                f"{cf.get('median_5d')} | {cf.get('win_rate')} | "
                f"{cf.get('mean_mdd_5d')} | {cf.get('ticker_equal_weight_mean_5d')} | "
                f"{rq.get('spearman_5d')} | {d.get('median_5d')} | "
                f"{d.get('spearman_5d')} |"
            )
        lines.append("")

    lines.append("## Findings")
    for f in report.get("findings") or []:
        scen = f.get("scenario")
        prefix = f"`{scen}` — " if scen else ""
        lines.append(f"- **{f.get('type')}**: {prefix}{f.get('detail')}")
    lines.append("")
    lines.append("## Recommendation")
    lines.append(str(report.get("recommendation")))
    lines.append("")
    return "\n".join(lines)


def write_simulation_outputs(
    report: Dict[str, Any],
    output_dir: Path,
) -> Dict[str, Path]:
    out_dir = Path(output_dir) / "quality" / "weight_simulation"
    out_dir.mkdir(parents=True, exist_ok=True)
    start = report.get("start_trade_date") or "UNKNOWN"
    end = report.get("end_trade_date") or "UNKNOWN"
    market = report.get("market") or "UNKNOWN"
    stem = f"screener_weight_simulation_{start}_{end}_{market}"

    # Public JSON without private export helpers
    public = {
        k: v
        for k, v in report.items()
        if not str(k).startswith("_")
    }
    json_path = out_dir / f"{stem}.json"
    md_path = out_dir / f"{stem}.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(public, f, ensure_ascii=False, indent=2)
    md_path.write_text(render_markdown(report), encoding="utf-8")

    summary_csv = out_dir / "scenario_summary.csv"
    validation_csv = out_dir / "scenario_validation.csv"
    migration_csv = out_dir / "scenario_candidate_migration.csv"
    topn_csv = out_dir / "scenario_topn.csv"

    _write_csv(summary_csv, flatten_scenario_summary(report))
    _write_csv(validation_csv, flatten_validation_csv(report))
    _write_csv(migration_csv, list(report.get("_export_migration") or []))
    _write_csv(topn_csv, list(report.get("_export_topn") or []))

    return {
        "json": json_path,
        "md": md_path,
        "scenario_summary": summary_csv,
        "scenario_validation": validation_csv,
        "scenario_candidate_migration": migration_csv,
        "scenario_topn": topn_csv,
        "dir": out_dir,
    }


def run_weight_simulation(
    *,
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
    output_dir: Path,
    config_path: Optional[Path] = None,
    train_end: str = DEFAULT_TRAIN_END,
    validation_start: str = DEFAULT_VALIDATION_START,
    threshold: float = PRODUCTION_THRESHOLD,
) -> Tuple[Dict[str, Any], Dict[str, Path]]:
    """End-to-end read-only simulation. Never writes Production artifacts."""
    from screener_quality import load_runtime_config

    output_dir = Path(output_dir)
    cfg = load_runtime_config(config_path)
    baseline_w = production_baseline_weights(cfg)

    # Fingerprint Production inputs we must not mutate
    config_file = Path(config_path) if config_path else None
    if config_file is None:
        try:
            from utils import CONFIG_PATH

            config_file = Path(CONFIG_PATH)
        except Exception:
            config_file = Path("config/config.json")
    pre_hashes: Dict[str, str] = {}
    if config_file.exists():
        pre_hashes[str(config_file)] = _sha256_file(config_file)

    observations, load_meta = load_simulation_observations(
        output_dir=output_dir,
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
        cfg=cfg,
    )

    report = analyze_weight_scenarios(
        observations,
        baseline_weights=baseline_w,
        scenarios=SCENARIOS,
        threshold=threshold,
        train_end=train_end,
        validation_start=validation_start,
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
    )
    report["data_source"] = load_meta
    report["input_fingerprints_before"] = pre_hashes

    paths = write_simulation_outputs(report, output_dir)

    # Verify Production config untouched
    post_hashes: Dict[str, str] = {}
    for p, h in pre_hashes.items():
        if Path(p).exists():
            post_hashes[p] = _sha256_file(Path(p))
    report["input_fingerprints_after"] = post_hashes
    report["production_inputs_unchanged"] = pre_hashes == post_hashes

    # Rewrite JSON with fingerprint verification
    public = {k: v for k, v in report.items() if not str(k).startswith("_")}
    with open(paths["json"], "w", encoding="utf-8") as f:
        json.dump(public, f, ensure_ascii=False, indent=2)

    return report, paths


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Offline Weight Simulator (exploratory scenarios)"
    )
    parser.add_argument("--market", default=os.getenv("MARKET", "SP500"))
    parser.add_argument("--session", default="pm")
    parser.add_argument("--from", dest="date_from", required=True, help="YYYYMMDD")
    parser.add_argument("--to", dest="date_to", required=True, help="YYYYMMDD")
    parser.add_argument(
        "--output-dir",
        default=os.getenv(
            "OUTPUT_DIR", str(Path(__file__).resolve().parents[1] / "output")
        ),
    )
    parser.add_argument("--config", default=None)
    parser.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    parser.add_argument("--validation-start", default=DEFAULT_VALIDATION_START)
    args = parser.parse_args(list(argv) if argv is not None else None)

    report, paths = run_weight_simulation(
        market=str(args.market).upper(),
        session=str(args.session).lower(),
        start_trade_date=str(args.date_from),
        end_trade_date=str(args.date_to),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config) if args.config else None,
        train_end=str(args.train_end),
        validation_start=str(args.validation_start),
    )

    summary: Dict[str, Any] = {
        "status": report.get("status"),
        "json": str(paths["json"]),
        "md": str(paths["md"]),
        "observations": report.get("observations"),
        "baseline_max_abs_error": (report.get("baseline_reconstruction") or {}).get(
            "max_abs_error"
        ),
        "findings": [
            {"type": f.get("type"), "scenario": f.get("scenario")}
            for f in (report.get("findings") or [])[:20]
        ],
        "production_policy_unchanged": True,
        "recommendation": report.get("recommendation"),
    }
    # Per-scenario validation snapshot
    scen_snap = []
    for name, block in (report.get("scenarios") or {}).items():
        vt = (block.get("validation") or {}).get("threshold_counterfactual") or {}
        vr = (block.get("validation") or {}).get("ranking_quality") or {}
        scen_snap.append(
            {
                "scenario": name,
                "weights": block.get("weights"),
                "val_spearman_5d": vr.get("spearman_5d"),
                "val_mean": vt.get("mean_5d"),
                "val_median": vt.get("median_5d"),
                "val_win_rate": vt.get("win_rate"),
                "val_mdd": vt.get("mean_mdd_5d"),
                "val_tew": vt.get("ticker_equal_weight_mean_5d"),
                "delta_val": block.get("delta_vs_baseline_validation"),
            }
        )
    summary["scenarios"] = scen_snap
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    if report.get("status") == STATUS_BASELINE_FAILED:
        return 3
    return 0 if report.get("observations") else 2


if __name__ == "__main__":
    sys.exit(main())
