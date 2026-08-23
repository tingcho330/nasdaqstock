"""Read-only Full-Universe Weight Replay Analyzer.

Replays Production and shadow weight scenarios on each trade day's full scored
universe (screener_scores.json), running the same downstream candidate pipeline
as screener.py. Never mutates Production config, screener.py, DECISION artifacts,
diagnostics, trading DB, or observation ledger.

Unlike the candidate-only Offline Weight Simulator (CANDIDATE_SET_COUNTERFACTUAL_ONLY),
this analyzer can surface NEW candidates that never appeared in Production observations.
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
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

logger = logging.getLogger("screener_full_universe_replay")

# Reuse weight/score semantics from offline weight simulator
from screener_weight_simulation import (  # noqa: E402
    DEFAULT_TRAIN_END,
    DEFAULT_VALIDATION_START,
    FACTOR_NAMES,
    HORIZONS,
    PRODUCTION_THRESHOLD,
    RETURN_KEYS,
    SCENARIOS,
    STATUS_BASELINE_FAILED,
    STATUS_OK,
    WEIGHT_SUM_EXPECTED,
    _clip01,
    _safe_float,
    _sha256_file,
    outcome_metrics,
    production_baseline_weights,
    split_train_validation,
    threshold_pass_rows,
    top_n_rows,
    weight_sum,
)

SCOPE_NOTE = "FULL_UNIVERSE_REPLAY"
STATUS_NO_DATA = "NO_DATA"

TRUSTED = "TRUSTED"
TRUSTED_WITH_WARNING = "TRUSTED_WITH_WARNING"
LEGACY_UNTRUSTED = "LEGACY_UNTRUSTED"
LEGACY_META_SELF_HASH_MISMATCH = "LEGACY_META_SELF_HASH_MISMATCH"

PRIMARY_SCORES_ARTIFACT = "screener_scores.json"

# Core Production factors (aliases resolved via extract_factor_value)
REQUIRED_SCORE_FACTORS = FACTOR_NAMES
# Fields the replay pipeline reads (presence OR documented default)
REQUIRED_SCORE_IDENTITY = ("ticker", "score")
REQUIRED_PIPELINE_FIELDS = (
    "momentum_pass",
    "volatility_pass",
    "issuer_group",
    "sector",
    "production_candidate",
)
# Eligibility: at least one of these must be present
ELIGIBILITY_FIELD_ALTS = (
    "eligibility_status",
    "eligibility_pass",
    "exclusion_reasons",
    "exclude_reasons",
)

WARNING_BASELINE_REPLAY_MISMATCH = "BASELINE_REPLAY_MISMATCH"
WARNING_LOW_SAMPLE = "LOW_SAMPLE"
WARNING_OUTLIER_DEPENDENT = "OUTLIER_DEPENDENT"
WARNING_REGIME_CONCENTRATED = "REGIME_CONCENTRATED"
WARNING_CANDIDATE_COUNT_COLLAPSE = "CANDIDATE_COUNT_COLLAPSE"
WARNING_INSUFFICIENT_VALIDATION = "INSUFFICIENT_VALIDATION"

SHADOW_CANDIDATE = "SHADOW_CANDIDATE"
SHADOW_REJECTED = "SHADOW_REJECTED"

# Final historical research only — no finer grid, no new weight scenarios.
D_RESEARCH_THRESHOLDS = (0.38, 0.40, 0.42)
D_RESEARCH_SCENARIO = "D_TECH_POS_DOWN"
# Backward-compatible alias (tests may still reference name)
THRESHOLD_GRID_DE = D_RESEARCH_THRESHOLDS
TOP_N_LEVELS = (5, 10, 20)

MIGRATION_KEEP = "KEEP"
MIGRATION_DROP = "DROP"
MIGRATION_NEW = "NEW"
MIGRATION_NEITHER = "NEITHER"

STATUS_RESEARCH_ONLY = "RESEARCH_ONLY"
STATUS_HISTORICAL_WEIGHT_SEARCH_CLOSED = "HISTORICAL_WEIGHT_SEARCH_CLOSED"
LOW_SAMPLE_REGIME_N = 5
OUTLIER_ABS_EXCLUDE_N = 1

# Production final Score tolerance after market adjustment (factor export is round(4))
REPLAY_SCORE_TOLERANCE = 0.0001 + 1e-12

MISSING_HISTORICAL_MARKET_STATE = "MISSING_HISTORICAL_MARKET_STATE"
HISTORICAL_MARKET_STATE_CONFLICT = "HISTORICAL_MARKET_STATE_CONFLICT"

REGIME_WEIGHTS: Dict[Any, float] = {}
VOLATILITY_WEIGHTS: Dict[str, float] = {
    "low": 1.05,
    "medium": 1.0,
    "high": 0.95,
}


def _init_regime_weights() -> None:
    from reviewer import MarketRegime

    global REGIME_WEIGHTS
    if not REGIME_WEIGHTS:
        REGIME_WEIGHTS.update(
            {
                MarketRegime.BULL: 1.1,
                MarketRegime.BEAR: 0.9,
                MarketRegime.SIDEWAYS: 1.0,
                MarketRegime.VOLATILE: 0.95,
            }
        )


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def assess_run_trust(run_dir: Path) -> Dict[str, Any]:
    """Delegate to canonical Quality decision-run trust semantics.

    Never invents a stricter Replay-only policy for historical meta self-hash.
    """
    from screener_quality import assess_decision_run_trust

    trust = assess_decision_run_trust(Path(run_dir))
    return {
        "status": trust.get("trust_status") or trust.get("status") or LEGACY_UNTRUSTED,
        "trust_status": trust.get("trust_status") or LEGACY_UNTRUSTED,
        "trust_reason": trust.get("trust_reason") or "UNKNOWN",
        "manifest_ok": bool(trust.get("manifest_ok")),
        "issues": list(trust.get("issues") or []),
        "reasons": list(trust.get("reasons") or []),
        "source_artifact": PRIMARY_SCORES_ARTIFACT,
    }


def _regime_vol_from_payload(payload: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(payload, dict):
        return None, None
    regime = payload.get("regime") or payload.get("trend")
    vol = payload.get("volatility") or payload.get("volatility_level")
    regime_s = str(regime).strip().lower() if regime is not None else None
    vol_s = str(vol).strip().lower() if vol is not None else None
    return regime_s, vol_s


def _parse_market_regime(regime_str: str) -> Any:
    from reviewer import MarketRegime

    mapping = {
        "bull": MarketRegime.BULL,
        "bear": MarketRegime.BEAR,
        "sideways": MarketRegime.SIDEWAYS,
        "volatile": MarketRegime.VOLATILE,
    }
    key = str(regime_str or "").strip().lower()
    if key not in mapping:
        raise ValueError(f"unknown market regime: {regime_str!r}")
    return mapping[key]


def build_market_state_from_payload(payload: Dict[str, Any]) -> Any:
    """Restore MarketState for calculate_market_adjusted_score from run artifact."""
    from datetime import datetime

    from reviewer import MarketState
    from utils import KST

    regime_str, vol_str = _regime_vol_from_payload(payload)
    if not regime_str or not vol_str:
        raise ValueError("market_state payload missing regime or volatility")
    trend = str(payload.get("trend") or regime_str).strip().lower()
    conf = _safe_float(
        payload.get("confidence")
        if payload.get("confidence") is not None
        else payload.get("advanced_market_confidence")
    )
    if conf is None:
        conf = 0.5
    ts_raw = payload.get("as_of_kst") or payload.get("timestamp")
    ts = datetime.now(KST)
    if ts_raw:
        try:
            ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=KST)
        except Exception:
            ts = datetime.now(KST)
    return MarketState(
        regime=_parse_market_regime(regime_str),
        volatility_level=vol_str,
        trend_direction=trend,
        confidence=float(conf),
        timestamp=ts,
    )


def combined_market_multiplier(market_state: Any) -> float:
    """Diagnostics-only combined regime * volatility weight (mirrors screener_core)."""
    _init_regime_weights()
    rw = REGIME_WEIGHTS.get(getattr(market_state, "regime", None), 1.0)
    vw = VOLATILITY_WEIGHTS.get(str(getattr(market_state, "volatility_level", "") or ""), 1.0)
    return round(float(rw) * float(vw), 6)


def load_historical_market_state(
    run_dir: Path,
    merged_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Load point-in-time MarketState from DECISION artifacts (never live market data).

    Primary: screener_run_meta.json["market_state"]
    Fallback: inputs/market_regime_inputs.json (only when primary absent)
    """
    run_dir = Path(run_dir)
    merged_meta = merged_meta if isinstance(merged_meta, dict) else {}
    run_meta = _load_json(run_dir / "screener_run_meta.json") or {}
    if not isinstance(run_meta, dict):
        run_meta = {}

    primary_payload = merged_meta.get("market_state")
    if not isinstance(primary_payload, dict):
        primary_payload = run_meta.get("market_state")
    if not isinstance(primary_payload, dict):
        primary_payload = None

    inputs_path = run_dir / "inputs" / "market_regime_inputs.json"
    fallback_payload = _load_json(inputs_path) if inputs_path.exists() else None
    if not isinstance(fallback_payload, dict):
        fallback_payload = None

    primary_regime, primary_vol = _regime_vol_from_payload(primary_payload)
    fallback_regime, fallback_vol = _regime_vol_from_payload(fallback_payload)

    if primary_regime and primary_vol and fallback_regime and fallback_vol:
        if primary_regime != fallback_regime or primary_vol != fallback_vol:
            return {
                "ok": False,
                "status": HISTORICAL_MARKET_STATE_CONFLICT,
                "source": "screener_run_meta.market_state+inputs/market_regime_inputs.json",
                "primary_regime": primary_regime,
                "primary_volatility": primary_vol,
                "fallback_regime": fallback_regime,
                "fallback_volatility": fallback_vol,
                "market_state": None,
            }

    payload: Optional[Dict[str, Any]] = None
    source: Optional[str] = None
    if primary_regime and primary_vol:
        payload = primary_payload
        source = "screener_run_meta.market_state"
    elif fallback_regime and fallback_vol:
        payload = fallback_payload
        source = "inputs/market_regime_inputs.json"
    else:
        return {
            "ok": False,
            "status": MISSING_HISTORICAL_MARKET_STATE,
            "source": None,
            "market_state": None,
        }

    try:
        ms = build_market_state_from_payload(payload or {})
    except Exception as exc:
        return {
            "ok": False,
            "status": MISSING_HISTORICAL_MARKET_STATE,
            "source": source,
            "error": str(exc),
            "market_state": None,
        }

    regime_str, vol_str = _regime_vol_from_payload(payload)
    return {
        "ok": True,
        "status": "OK",
        "source": source,
        "market_state": ms,
        "regime": regime_str,
        "volatility": vol_str,
        "expected_multiplier": combined_market_multiplier(ms),
        "payload": payload,
    }


def compute_weighted_base(
    factors: Dict[str, Optional[float]],
    weights: Dict[str, float],
    *,
    missing_as_zero: bool = True,
) -> Optional[float]:
    """Weighted factor sum + clip[0,1] before market adjustment (Production order)."""
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
    return _clip01(raw)


def replay_score(
    factors: Dict[str, Optional[float]],
    weights: Dict[str, float],
    historical_market_state: Optional[Any],
    *,
    missing_as_zero: bool = True,
) -> Optional[float]:
    """Mirror screener.py: clip weighted sum → market adjust → round(4)."""
    from screener_core import calculate_market_adjusted_score

    base = compute_weighted_base(factors, weights, missing_as_zero=missing_as_zero)
    if base is None:
        return None
    adjusted = base
    if historical_market_state is not None:
        adjusted = calculate_market_adjusted_score(float(base), historical_market_state)
    return round(float(adjusted), 4)


def apply_replay_scenario_scores(
    observations: Sequence[Dict[str, Any]],
    weights: Dict[str, float],
    *,
    baseline_weights: Dict[str, float],
    historical_market_state: Optional[Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for obs in observations:
        row = dict(obs)
        factors = obs.get("factors") or {}
        row["baseline_score"] = replay_score(
            factors, baseline_weights, historical_market_state
        )
        row["scenario_score"] = replay_score(factors, weights, historical_market_state)
        row["weighted_base"] = compute_weighted_base(factors, weights)
        contribs: Dict[str, Optional[float]] = {}
        for name in FACTOR_NAMES:
            v = _safe_float(factors.get(name))
            w = float(weights.get(name, 0.0))
            contribs[name] = round(v * w, 8) if v is not None else None
        row["scenario_contributions"] = contribs
        out.append(row)
    return out


def reconstruct_baseline_scores_replay(
    observations: Sequence[Dict[str, Any]],
    baseline_weights: Dict[str, float],
    *,
    market_state_by_date: Dict[str, Any],
    tolerance: float = REPLAY_SCORE_TOLERANCE,
) -> Dict[str, Any]:
    """Recompute Production final Score with historical market adjustment per trade_date."""
    from screener_core import calculate_market_adjusted_score

    _init_regime_weights()
    errors: List[Dict[str, Any]] = []
    diagnostics: List[Dict[str, Any]] = []
    by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    max_err = 0.0
    n_checked = 0
    n_skipped_missing = 0
    n_skipped_no_market_state = 0

    for obs in observations:
        factors = obs.get("factors") or {}
        if not isinstance(factors, dict):
            factors = {}
        td = str(obs.get("trade_date") or "")
        ms = market_state_by_date.get(td)
        if ms is None:
            n_skipped_no_market_state += 1
            continue

        base = compute_weighted_base(factors, baseline_weights)
        prod = _safe_float(
            obs.get("total_score")
            if obs.get("total_score") is not None
            else obs.get("score")
        )
        if base is None or prod is None:
            n_skipped_missing += 1
            continue

        adjusted = calculate_market_adjusted_score(float(base), ms)
        recon = round(float(adjusted), 4)
        err = abs(recon - prod)
        n_checked += 1
        if err > max_err:
            max_err = err

        rw = REGIME_WEIGHTS.get(getattr(ms, "regime", None), 1.0)
        vw = VOLATILITY_WEIGHTS.get(str(getattr(ms, "volatility_level", "") or ""), 1.0)
        diag = {
            "trade_date": td,
            "ticker": obs.get("ticker"),
            "factor_weighted_sum": round(float(base), 6),
            "market_regime": getattr(getattr(ms, "regime", None), "value", None),
            "volatility_level": getattr(ms, "volatility_level", None),
            "regime_weight": rw,
            "volatility_weight": vw,
            "combined_market_multiplier": round(float(rw) * float(vw), 6),
            "reconstructed_adjusted_score": recon,
            "production_score": prod,
            "abs_error": err,
        }
        diagnostics.append(diag)
        by_date[td].append(diag)

        if err > tolerance:
            errors.append(diag)

    date_summary: List[Dict[str, Any]] = []
    for td in sorted(by_date.keys()):
        rows = by_date[td]
        errs = [r["abs_error"] for r in rows]
        ms = market_state_by_date.get(td)
        date_summary.append(
            {
                "trade_date": td,
                "regime": getattr(getattr(ms, "regime", None), "value", None),
                "volatility": getattr(ms, "volatility_level", None),
                "expected_multiplier": combined_market_multiplier(ms) if ms else None,
                "n_checked": len(rows),
                "max_abs_error": max(errs) if errs else None,
                "median_abs_error": sorted(errs)[len(errs) // 2] if errs else None,
                "failure_count": sum(1 for e in errs if e > tolerance),
            }
        )

    ok = n_checked > 0 and len(errors) == 0
    return {
        "ok": ok,
        "status": STATUS_OK if ok else STATUS_BASELINE_FAILED,
        "n_checked": n_checked,
        "n_skipped_missing_factors": n_skipped_missing,
        "n_skipped_no_market_state": n_skipped_no_market_state,
        "max_abs_error": max_err,
        "tolerance": tolerance,
        "failures": errors[:20],
        "failure_count": len(errors),
        "weight_sum": weight_sum(baseline_weights),
        "uses_calculate_market_adjusted_score": True,
        "historical_market_state_source": "screener_run_meta.market_state",
        "diagnostics_sample": diagnostics[:50],
        "date_summary": date_summary,
        "note": (
            "Reconstruction mirrors screener.py: sum(factor*weight), clip[0,1], "
            "calculate_market_adjusted_score(historical MarketState from run_meta), "
            "round(4). Tolerance allows factor export round(4) boundary effects."
        ),
    }


def _row_has_any_key(row: Dict[str, Any], keys: Sequence[str]) -> bool:
    return any(k in row and row.get(k) is not None for k in keys)


def _resolve_ticker_field(row: Dict[str, Any]) -> Optional[str]:
    t = _ticker_of(row)
    return t or None


def _resolve_score_field(row: Dict[str, Any]) -> Optional[float]:
    return _safe_float(row.get("score") if row.get("score") is not None else row.get("Score"))


def verify_screener_scores_integrity(
    run_dir: Path,
    *,
    expected_run_id: Optional[str] = None,
    expected_market: Optional[str] = None,
    expected_session: Optional[str] = None,
    expected_trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Independent integrity check for primary Replay artifact screener_scores.json."""
    from screener_artifacts import sha256_file
    from screener_factor_analysis import extract_factor_value

    run_dir = Path(run_dir)
    result: Dict[str, Any] = {
        "status": "FAIL",
        "artifact": PRIMARY_SCORES_ARTIFACT,
        "file_exists": False,
        "sha_ok": False,
        "row_count_ok": False,
        "json_ok": False,
        "schema_ok": False,
        "source_run_id_ok": True,
        "path_consistency_ok": True,
        "row_count": 0,
        "reasons": [],
        "rows": [],
    }
    scores_path = run_dir / PRIMARY_SCORES_ARTIFACT
    if not scores_path.exists():
        result["reasons"].append("SCORES_MISSING")
        return result
    result["file_exists"] = True

    man = _load_json(run_dir / "manifest.json") or {}
    arts = man.get("artifacts") if isinstance(man, dict) else {}
    art_info = (arts or {}).get(PRIMARY_SCORES_ARTIFACT) if isinstance(arts, dict) else {}
    if not isinstance(art_info, dict):
        art_info = {}

    expected_sha = art_info.get("sha256")
    try:
        actual_sha = sha256_file(scores_path)
    except Exception:
        result["reasons"].append("SCORES_SHA_UNREADABLE")
        return result
    if expected_sha:
        result["sha_ok"] = actual_sha == expected_sha
        if not result["sha_ok"]:
            result["reasons"].append("SCORES_SHA_MISMATCH")
    else:
        # Manifest entry without sha — cannot claim PASS
        result["sha_ok"] = False
        result["reasons"].append("SCORES_SHA_MISSING_IN_MANIFEST")

    data = _load_json(scores_path)
    if not isinstance(data, list):
        result["reasons"].append("SCORES_NOT_LIST")
        return result
    result["json_ok"] = True
    result["row_count"] = len(data)
    result["rows"] = [r for r in data if isinstance(r, dict)]

    exp_rc = art_info.get("row_count")
    if exp_rc is not None:
        result["row_count_ok"] = int(exp_rc) == int(len(data))
        if not result["row_count_ok"]:
            result["reasons"].append("SCORES_ROW_COUNT_MISMATCH")
    else:
        result["row_count_ok"] = True  # no declared count to contradict

    if not result["rows"]:
        result["reasons"].append("SCORES_EMPTY")
        return result

    # Schema: required factors + identity + pipeline fields used by replay
    schema_ok = True
    for row in result["rows"][: min(20, len(result["rows"]))]:
        if not _resolve_ticker_field(row):
            schema_ok = False
            result["reasons"].append("SCHEMA_MISSING_TICKER")
            break
        if _resolve_score_field(row) is None:
            schema_ok = False
            result["reasons"].append("SCHEMA_MISSING_SCORE")
            break
        for fac in REQUIRED_SCORE_FACTORS:
            if extract_factor_value(row, fac) is None:
                schema_ok = False
                result["reasons"].append(f"SCHEMA_MISSING_FACTOR:{fac}")
                break
        if not schema_ok:
            break
        if not any(_row_has_any_key(row, [a]) for a in ELIGIBILITY_FIELD_ALTS):
            # eligibility_status may be derived later; require at least exclusion list or status
            if "held" not in row and "exclusion_reasons" not in row and "exclude_reasons" not in row:
                schema_ok = False
                result["reasons"].append("SCHEMA_MISSING_ELIGIBILITY")
                break
        for fld in ("momentum_pass", "volatility_pass"):
            if fld not in row:
                schema_ok = False
                result["reasons"].append(f"SCHEMA_MISSING:{fld}")
                break
        if not schema_ok:
            break
        if not _row_has_any_key(row, ("issuer_group", "IssuerGroup")):
            # issuer defaults to ticker in normalize — warn but allow if ticker present
            pass
        if not _row_has_any_key(row, ("sector", "Sector")):
            schema_ok = False
            result["reasons"].append("SCHEMA_MISSING_SECTOR")
            break
        if "production_candidate" not in row:
            schema_ok = False
            result["reasons"].append("SCHEMA_MISSING_PRODUCTION_CANDIDATE")
            break
    result["schema_ok"] = schema_ok

    # source_run_id consistency when present on rows
    if expected_run_id:
        mismatched = [
            r
            for r in result["rows"]
            if r.get("source_run_id") is not None
            and str(r.get("source_run_id")) != str(expected_run_id)
        ]
        if mismatched:
            result["source_run_id_ok"] = False
            result["reasons"].append("SOURCE_RUN_ID_MISMATCH")

    # Path vs declared identity
    parts = {p.name for p in run_dir.parents}
    parts.add(run_dir.name)
    if expected_market and expected_market.upper() not in {p.upper() for p in parts}:
        # path layout: .../decision/{MARKET}/{date}/{session}/{run_id}
        try:
            sess_dir = run_dir.parent
            date_dir = sess_dir.parent
            mkt_dir = date_dir.parent
            if str(mkt_dir.name).upper() != str(expected_market).upper():
                result["path_consistency_ok"] = False
                result["reasons"].append("PATH_MARKET_MISMATCH")
            if expected_trade_date and str(date_dir.name) != str(expected_trade_date):
                result["path_consistency_ok"] = False
                result["reasons"].append("PATH_TRADE_DATE_MISMATCH")
            if expected_session and str(sess_dir.name).lower() != str(expected_session).lower():
                result["path_consistency_ok"] = False
                result["reasons"].append("PATH_SESSION_MISMATCH")
        except Exception:
            pass

    if (
        result["sha_ok"]
        and result["row_count_ok"]
        and result["json_ok"]
        and result["schema_ok"]
        and result["source_run_id_ok"]
        and result["path_consistency_ok"]
        and not result["reasons"]
    ):
        result["status"] = "PASS"
    elif (
        result["sha_ok"]
        and result["row_count_ok"]
        and result["json_ok"]
        and result["schema_ok"]
        and result["source_run_id_ok"]
        and result["path_consistency_ok"]
    ):
        # reasons may still list soft notes — treat as PASS only when empty
        result["status"] = "PASS" if not result["reasons"] else "FAIL"
    else:
        result["status"] = "FAIL"

    # Drop bulky rows from return for callers that only need status
    rows = result.pop("rows")
    result["rows"] = rows  # keep for load_replay_days
    return result


def replay_inclusion_decision(
    trust: Dict[str, Any],
    scores_integrity: Dict[str, Any],
) -> Dict[str, Any]:
    """Include TRUSTED or LEGACY_META_SELF_HASH warning runs with PASS scores integrity."""
    status = trust.get("trust_status") or trust.get("status")
    reason = trust.get("trust_reason") or ""
    scores_pass = scores_integrity.get("status") == "PASS"

    included = False
    exclusion_reason: Optional[str] = None
    if status == TRUSTED and scores_pass:
        included = True
    elif (
        status == TRUSTED_WITH_WARNING
        and reason == LEGACY_META_SELF_HASH_MISMATCH
        and scores_pass
    ):
        included = True
    elif status == LEGACY_UNTRUSTED:
        exclusion_reason = reason or LEGACY_UNTRUSTED
    elif status == TRUSTED_WITH_WARNING and reason != LEGACY_META_SELF_HASH_MISMATCH:
        exclusion_reason = f"UNACCEPTED_WARNING:{reason}"
    elif not scores_pass:
        exclusion_reason = "SCORES_INTEGRITY_FAIL:" + ",".join(
            scores_integrity.get("reasons") or ["UNKNOWN"]
        )
    else:
        exclusion_reason = f"UNACCEPTED_TRUST:{status}:{reason}"

    return {
        "included": included,
        "exclusion_reason": exclusion_reason,
        "trust_status": status,
        "trust_reason": reason,
        "artifact_integrity_status": scores_integrity.get("status"),
    }


def resolve_full_scored_universe(
    run_dir: Path,
) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    """Load the most upstream full scored universe for a DECISION run.

    Priority:
      1. screener_scores.json
      2. screener_candidates_full.json (post-pipeline but broader than candidates)
      3. empty (caller skips)
    """
    run_dir = Path(run_dir)
    meta: Dict[str, Any] = {"run_dir": str(run_dir)}
    scores_path = run_dir / "screener_scores.json"
    if scores_path.exists():
        data = _load_json(scores_path)
        if isinstance(data, list) and data:
            meta["source"] = "screener_scores.json"
            meta["row_count"] = len(data)
            return data, "screener_scores.json", meta
    full_path = run_dir / "screener_candidates_full.json"
    if full_path.exists():
        data = _load_json(full_path)
        if isinstance(data, list) and data:
            meta["source"] = "screener_candidates_full.json"
            meta["source_note"] = "fallback; not pre-filter scored universe"
            meta["row_count"] = len(data)
            return data, "screener_candidates_full.json", meta
    meta["source"] = None
    meta["row_count"] = 0
    return [], "", meta


def _ticker_of(row: Dict[str, Any]) -> str:
    return str(row.get("ticker") or row.get("Ticker") or "").upper()


def extract_factors_from_score_row(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    from screener_factor_analysis import extract_factor_value

    return {name: extract_factor_value(row, name) for name in FACTOR_NAMES}


def normalize_score_record(
    row: Dict[str, Any],
    *,
    trade_date: str,
    market: str,
    session: str,
    source_run_id: str,
) -> Dict[str, Any]:
    """Hydrate a screener_scores row into replay shape."""
    factors = extract_factors_from_score_row(row)
    out: Dict[str, Any] = {
        "ticker": _ticker_of(row),
        "trade_date": trade_date,
        "market": market,
        "session": session,
        "source_run_id": source_run_id,
        "factors": factors,
        "total_score": _safe_float(row.get("score") if row.get("score") is not None else row.get("Score")),
        "sector": str(row.get("sector") or row.get("Sector") or ""),
        "issuer_group": str(row.get("issuer_group") or row.get("IssuerGroup") or _ticker_of(row)),
        "held": bool(row.get("held", False)),
        "exclusion_reasons": list(row.get("exclusion_reasons") or row.get("exclude_reasons") or []),
        "eligibility_status": str(row.get("eligibility_status") or "UNKNOWN"),
        "momentum_pass": bool(row.get("momentum_pass", True)),
        "volatility_pass": bool(row.get("volatility_pass", True)),
        "threshold_pass": bool(row.get("threshold_pass", False)),
        "production_candidate": bool(row.get("production_candidate", False)),
        "amount5d": _safe_float(row.get("amount5d") or row.get("Amount5D")),
        "market_cap": _safe_float(row.get("marcap") or row.get("Marcap") or row.get("market_cap")),
        "rsi": _safe_float(row.get("rsi") or row.get("RSI")),
        "price": _safe_float(row.get("price") or row.get("Price")),
    }
    for h in HORIZONS:
        key = RETURN_KEYS[h]
        if row.get(key) is not None:
            out[key] = _safe_float(row.get(key))
    for extra in ("max_drawdown_5d_pct", "max_runup_5d_pct"):
        if row.get(extra) is not None:
            out[extra] = _safe_float(row.get(extra))
    return out


def load_actual_production_candidates(run_dir: Path) -> List[Dict[str, Any]]:
    data = _load_json(run_dir / "screener_candidates.json")
    if not isinstance(data, list):
        return []
    return data


def load_market_regime(run_dir: Path, merged_meta: Dict[str, Any]) -> str:
    ms = _load_json(run_dir / "market_state.json")
    if isinstance(ms, dict):
        for key in ("regime", "trend", "market_regime"):
            v = ms.get(key)
            if v:
                return str(v).upper()
    mstate = merged_meta.get("market_state") if isinstance(merged_meta.get("market_state"), dict) else {}
    for key in ("regime", "trend"):
        v = mstate.get(key)
        if v:
            return str(v).upper()
    r = merged_meta.get("regime")
    return str(r).upper() if r else "UNKNOWN"


def pipeline_params_from_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    sp = cfg.get("screener_params") or {}
    return {
        "threshold": float(sp.get("min_score_threshold", PRODUCTION_THRESHOLD) or PRODUCTION_THRESHOLD),
        "require_positive_momentum": bool(sp.get("require_positive_momentum", False)),
        "exclude_high_volatility": bool(sp.get("exclude_high_volatility", False)),
        "top_n": int(sp.get("top_n", 8) or 8),
        "sector_cap": float(sp.get("sector_cap", 0.35) or 0.35),
        "apply_issuer_dedupe": bool(sp.get("issuer_dedupe_enabled", True)),
        "rsi_overheated_threshold": float(sp.get("rsi_overheated_threshold", 70.0) or 70.0),
        "exclude_held_from_candidates": bool(sp.get("exclude_held_from_candidates", True)),
    }


def annotate_eligibility_for_scenario(
    row: Dict[str, Any],
    *,
    scenario_score: float,
    threshold: float,
    rsi_overheated_threshold: float,
    exclude_held: bool,
) -> Tuple[str, List[str]]:
    from screener_ops import eligibility_for_row

    base_reasons = list(row.get("exclusion_reasons") or [])
    # Strip score-derived reasons; recompute under scenario threshold
    base_reasons = [
        r for r in base_reasons if r not in ("BELOW_MIN_SCORE", "NEGATIVE_MOMENTUM", "HIGH_VOLATILITY")
    ]
    threshold_pass = scenario_score >= threshold
    return eligibility_for_row(
        held=bool(row.get("held")),
        exclude_reasons=base_reasons,
        rsi=row.get("rsi"),
        rsi_overheated_threshold=rsi_overheated_threshold,
        threshold_pass=threshold_pass,
        momentum_pass=bool(row.get("momentum_pass", True)),
        volatility_pass=bool(row.get("volatility_pass", True)),
        exclude_held=exclude_held,
    )


def build_replay_dataframe(
    universe_rows: Sequence[Dict[str, Any]],
    *,
    weights: Dict[str, float],
    threshold: float,
    pipeline_params: Dict[str, Any],
    historical_market_state: Optional[Any] = None,
) -> pd.DataFrame:
    """Build scored DataFrame for select_candidates_pipeline."""
    records: List[Dict[str, Any]] = []
    rsi_thr = float(pipeline_params.get("rsi_overheated_threshold", 70.0))
    exclude_held = bool(pipeline_params.get("exclude_held_from_candidates", True))
    for row in universe_rows:
        factors = row.get("factors") or {}
        score = replay_score(factors, weights, historical_market_state)
        if score is None:
            continue
        elig_status, excl = annotate_eligibility_for_scenario(
            row,
            scenario_score=float(score),
            threshold=threshold,
            rsi_overheated_threshold=rsi_thr,
            exclude_held=exclude_held,
        )
        records.append(
            {
                "Ticker": row.get("ticker"),
                "Score": float(score),
                "Sector": row.get("sector") or "N/A",
                "issuer_group": row.get("issuer_group") or row.get("ticker"),
                "held": bool(row.get("held")),
                "exclude_reasons": excl,
                "eligibility_status": elig_status,
                "momentum_pass": bool(row.get("momentum_pass", True)),
                "volatility_pass": bool(row.get("volatility_pass", True)),
                "threshold_pass": float(score) >= threshold,
                "RSI": row.get("rsi"),
            }
        )
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df = df.sort_values(by=["Score"], ascending=False).reset_index(drop=True)
    return df


def run_candidate_pipeline(
    scored_df: pd.DataFrame,
    *,
    threshold: float,
    pipeline_params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[Any]]:
    from screener import diversify_by_sector
    from screener_ops import select_candidates_pipeline

    if scored_df is None or scored_df.empty:
        return scored_df.iloc[0:0] if scored_df is not None else pd.DataFrame(), []
    out, stages = select_candidates_pipeline(
        scored_df,
        threshold=threshold,
        require_positive_momentum=bool(pipeline_params.get("require_positive_momentum")),
        exclude_high_volatility=bool(pipeline_params.get("exclude_high_volatility")),
        top_n=int(pipeline_params.get("top_n", 8)),
        sector_cap=float(pipeline_params.get("sector_cap", 0.35)),
        diversify_fn=diversify_by_sector,
        apply_issuer_dedupe=bool(pipeline_params.get("apply_issuer_dedupe", True)),
        require_eligible=True,
    )
    return out, stages


def candidate_ticker_list(candidates_df: pd.DataFrame) -> List[str]:
    if candidates_df is None or candidates_df.empty:
        return []
    col = "Ticker" if "Ticker" in candidates_df.columns else "ticker"
    return [str(t).upper() for t in candidates_df[col].tolist()]


def compare_baseline_replay_to_production(
    replay_tickers: Sequence[str],
    actual_tickers: Sequence[str],
    *,
    replay_scores: Optional[Dict[str, float]] = None,
    actual_scores: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    replay = [str(t).upper() for t in replay_tickers]
    actual = [str(t).upper() for t in actual_tickers]
    replay_set = set(replay)
    actual_set = set(actual)
    missing = sorted(actual_set - replay_set)
    extra = sorted(replay_set - actual_set)
    both = sorted(replay_set & actual_set)
    exact_order = replay == actual
    score_diffs: List[Dict[str, Any]] = []
    if replay_scores and actual_scores:
        for t in both:
            rs = _safe_float(replay_scores.get(t))
            ac = _safe_float(actual_scores.get(t))
            if rs is not None and ac is not None and abs(rs - ac) > REPLAY_SCORE_TOLERANCE:
                score_diffs.append({"ticker": t, "replay_score": rs, "actual_score": ac, "abs_diff": abs(rs - ac)})
    match_count = len(both)
    total = max(len(actual_set), 1)
    return {
        "replay_count": len(replay),
        "actual_count": len(actual),
        "exact_match_count": match_count,
        "exact_order_match": exact_order,
        "match_pct": round(match_count / total * 100.0, 4) if actual_set else (100.0 if not replay_set else 0.0),
        "missing_from_replay": missing,
        "extra_in_replay": extra,
        "both": both,
        "score_differences": score_diffs[:20],
        "exact_set_match": replay_set == actual_set and not missing and not extra,
    }


def eligible_universe_top_k(
    universe_rows: Sequence[Dict[str, Any]],
    *,
    weights: Dict[str, float],
    threshold: float,
    pipeline_params: Dict[str, Any],
    k: int,
    historical_market_state: Optional[Any] = None,
) -> List[str]:
    """Top-K by scenario score among ELIGIBILITY-passing universe rows."""
    scored_df = build_replay_dataframe(
        universe_rows,
        weights=weights,
        threshold=threshold,
        pipeline_params=pipeline_params,
        historical_market_state=historical_market_state,
    )
    if scored_df.empty or k <= 0:
        return []
    eligible = scored_df[scored_df["eligibility_status"] == "ELIGIBLE"].copy()
    if eligible.empty:
        return []
    eligible = eligible.sort_values(by=["Score"], ascending=False)
    return candidate_ticker_list(eligible.head(k))


def classify_migration(
    ticker: str,
    *,
    baseline_candidates: Set[str],
    scenario_candidates: Set[str],
) -> str:
    bp = ticker in baseline_candidates
    sp = ticker in scenario_candidates
    if bp and sp:
        return MIGRATION_KEEP
    if bp and not sp:
        return MIGRATION_DROP
    if (not bp) and sp:
        return MIGRATION_NEW
    return MIGRATION_NEITHER


def sector_concentration(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    counts: Counter = Counter()
    for r in rows:
        sec = str(r.get("sector") or "UNKNOWN")
        counts[sec] += 1
    return dict(counts)


def detect_outlier_dependent(metrics: Dict[str, Any]) -> bool:
    mean_v = _safe_float(metrics.get("mean_5d"))
    med_v = _safe_float(metrics.get("median_5d"))
    tew = _safe_float(metrics.get("ticker_equal_weight_mean_5d"))
    if mean_v is None or med_v is None:
        return False
    if mean_v - med_v >= 2.0:
        return True
    if tew is not None and mean_v - tew >= 2.0:
        return True
    return False


def _avg_daily_candidate_count(rows: Sequence[Dict[str, Any]]) -> Optional[float]:
    by_date: Counter = Counter()
    for r in rows:
        td = str(r.get("trade_date") or "")
        if td:
            by_date[td] += 1
    if not by_date:
        return None
    return round(sum(by_date.values()) / len(by_date), 4)


def window_outcome_block(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Outcome metrics plus avg daily candidate count for a candidate row set."""
    m = outcome_metrics(rows, score_key="scenario_score")
    m["avg_daily_candidate_count"] = _avg_daily_candidate_count(rows)
    return m


def collect_candidate_obs_from_day_results(
    day_results: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    obs_rows: List[Dict[str, Any]] = []
    for dr in day_results:
        for t in dr.get("replay_tickers") or []:
            src = next(
                (
                    r
                    for r in (dr.get("scored_observations") or [])
                    if str(r.get("ticker") or "").upper() == t
                ),
                {"ticker": t},
            )
            obs_rows.append(
                {
                    **src,
                    "scenario_candidate": True,
                    "trade_date": dr.get("trade_date"),
                    "ticker": str(src.get("ticker") or t).upper(),
                }
            )
    return obs_rows


def collect_topk_obs_from_day_results(
    day_results: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for dr in day_results:
        for t in dr.get("daily_top_k_matched") or []:
            src = next(
                (
                    r
                    for r in (dr.get("scored_observations") or [])
                    if str(r.get("ticker") or "").upper() == t
                ),
                {"ticker": t, "trade_date": dr.get("trade_date")},
            )
            rows.append({**src, "trade_date": dr.get("trade_date"), "ticker": t})
    return rows


def migration_group_metrics(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """KEEP / DROP / NEW summary with robust stats (no production recommendation)."""
    out: Dict[str, Any] = {}
    for group in (MIGRATION_KEEP, MIGRATION_DROP, MIGRATION_NEW):
        subset = [r for r in records if r.get("migration_group") == group]
        m = outcome_metrics(subset, score_key="scenario_score")
        out[group] = {
            "n": len(subset),
            "unique_tickers": m.get("unique_tickers"),
            "mean_5d": m.get("mean_5d"),
            "median_5d": m.get("median_5d"),
            "win_rate": m.get("win_rate"),
            "p25": m.get("p25"),
            "p75": m.get("p75"),
            "mature_5d_count": m.get("mature_5d_count"),
        }
    return out


def metrics_excluding_largest_abs_return(
    rows: Sequence[Dict[str, Any]],
    *,
    n_exclude: int = OUTLIER_ABS_EXCLUDE_N,
) -> Dict[str, Any]:
    """Diagnostic-only: drop the largest |return_5d| observation(s). Never cherry-picks sign."""
    keyed = RETURN_KEYS[5]
    with_ret = []
    without_ret = []
    for r in rows:
        v = _safe_float(r.get(keyed))
        if v is None:
            without_ret.append(r)
        else:
            with_ret.append((abs(v), r))
    if not with_ret or n_exclude <= 0:
        return {
            "excluded_n": 0,
            "note": "insufficient matured returns for abs-exclude diagnostic",
            "metrics": window_outcome_block(list(rows)),
        }
    with_ret_sorted = sorted(with_ret, key=lambda x: x[0], reverse=True)
    excluded = [r for _, r in with_ret_sorted[:n_exclude]]
    kept = [r for _, r in with_ret_sorted[n_exclude:]] + without_ret
    excl_info = [
        {
            "ticker": e.get("ticker"),
            "trade_date": e.get("trade_date"),
            "return_5d_pct": _safe_float(e.get(keyed)),
        }
        for e in excluded
    ]
    return {
        "excluded_n": len(excluded),
        "excluded": excl_info,
        "note": (
            "diagnostic_only: exclude largest absolute 5d return(s); "
            "not used for Production decisions"
        ),
        "metrics": window_outcome_block(kept),
    }


def summarize_d_threshold_windows(
    day_results: Sequence[Dict[str, Any]],
    *,
    train_end: str,
    validation_start: str,
) -> Dict[str, Any]:
    obs = collect_candidate_obs_from_day_results(day_results)
    train, valid = split_train_validation(
        obs, train_end=train_end, validation_start=validation_start
    )
    return {
        "full": window_outcome_block(obs),
        "train": window_outcome_block(train),
        "validation": window_outcome_block(valid),
    }


def _metric_improved(
    shadow: Optional[float],
    baseline: Optional[float],
    *,
    higher_is_better: bool = True,
    slack: float = 0.0,
) -> bool:
    if shadow is None or baseline is None:
        return False
    if higher_is_better:
        return shadow > baseline + slack
    return shadow < baseline - slack


def evaluate_d_robust_improvement(
    *,
    baseline_validation: Dict[str, Any],
    d_validation: Dict[str, Any],
    baseline_topk: Dict[str, Any],
    d_topk: Dict[str, Any],
    migration_validation: Dict[str, Any],
    outlier_flag: bool,
) -> Dict[str, Any]:
    """Research gate only — never recommends Production change."""
    b_med = _safe_float(baseline_validation.get("median_5d"))
    d_med = _safe_float(d_validation.get("median_5d"))
    b_tew = _safe_float(baseline_validation.get("ticker_equal_weight_mean_5d"))
    d_tew = _safe_float(d_validation.get("ticker_equal_weight_mean_5d"))
    b_fs = _safe_float(baseline_validation.get("first_signal_median_5d"))
    d_fs = _safe_float(d_validation.get("first_signal_median_5d"))
    b_wr = _safe_float(baseline_validation.get("win_rate"))
    d_wr = _safe_float(d_validation.get("win_rate"))
    b_topk_med = _safe_float(baseline_topk.get("median_5d"))
    d_topk_med = _safe_float(d_topk.get("median_5d"))
    new_block = migration_validation.get(MIGRATION_NEW) or {}
    new_med = _safe_float(new_block.get("median_5d"))

    checks = {
        "validation_median_improved": _metric_improved(d_med, b_med),
        "validation_tew_improved": _metric_improved(d_tew, b_tew),
        "validation_win_rate_ok": (
            d_wr is not None and b_wr is not None and d_wr >= b_wr - 0.05
        ),
        "first_signal_ok": d_fs is None or b_fs is None or d_fs >= b_fs - 2.0,
        "matched_topk_median_improved": _metric_improved(d_topk_med, b_topk_med),
        "new_candidates_ok": new_med is None or new_med >= -1.0,
        "not_outlier_dependent": not outlier_flag,
    }
    return {
        "robust_improvement": all(checks.values()),
        "checks": checks,
    }


def decide_historical_weight_search(
    d_threshold_results: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Max status is RESEARCH_ONLY. Close search when no robust D improvement."""
    any_robust = any(bool(r.get("robust_improvement")) for r in d_threshold_results)
    if any_robust:
        return {
            "status": STATUS_RESEARCH_ONLY,
            "historical_weight_search": "OPEN_RESEARCH_ONLY",
            "note": (
                "At least one D research threshold shows research-grade signals; "
                "Production remains A_BASELINE — no change recommended"
            ),
            "production_change_recommended": False,
        }
    return {
        "status": STATUS_RESEARCH_ONLY,
        "historical_weight_search": STATUS_HISTORICAL_WEIGHT_SEARCH_CLOSED,
        "note": (
            "D thresholds 0.38/0.40/0.42 show no robust improvement vs A_BASELINE; "
            "HISTORICAL_WEIGHT_SEARCH_CLOSED — Production unchanged"
        ),
        "production_change_recommended": False,
        "HISTORICAL_WEIGHT_SEARCH_CLOSED": True,
    }


def evaluate_shadow_candidate(
    scenario_name: str,
    *,
    baseline_replay_ok: bool,
    validation_metrics: Dict[str, Any],
    baseline_validation_metrics: Dict[str, Any],
    new_candidate_metrics: Dict[str, Any],
    warnings: Sequence[str],
    validation_obs_count: int,
    min_validation_n: int = 8,
) -> Dict[str, Any]:
    """Shadow policy gate — never emits PRODUCTION_CHANGE_RECOMMENDED."""
    if not baseline_replay_ok:
        return {"status": SHADOW_REJECTED, "reason": "baseline_replay_failed"}
    if validation_obs_count < min_validation_n:
        return {"status": SHADOW_REJECTED, "reason": "insufficient_validation_sample"}
    if WARNING_BASELINE_REPLAY_MISMATCH in warnings:
        return {"status": SHADOW_REJECTED, "reason": "baseline_replay_mismatch"}
    if WARNING_CANDIDATE_COUNT_COLLAPSE in warnings:
        return {"status": SHADOW_REJECTED, "reason": "candidate_count_collapse"}

    b_med = _safe_float(baseline_validation_metrics.get("median_5d"))
    s_med = _safe_float(validation_metrics.get("median_5d"))
    b_tew = _safe_float(baseline_validation_metrics.get("ticker_equal_weight_mean_5d"))
    s_tew = _safe_float(validation_metrics.get("ticker_equal_weight_mean_5d"))
    b_wr = _safe_float(baseline_validation_metrics.get("win_rate"))
    s_wr = _safe_float(validation_metrics.get("win_rate"))
    b_mdd = _safe_float(baseline_validation_metrics.get("mean_mdd_5d"))
    s_mdd = _safe_float(validation_metrics.get("mean_mdd_5d"))
    b_fs = _safe_float(baseline_validation_metrics.get("first_signal_median_5d"))
    s_fs = _safe_float(validation_metrics.get("first_signal_median_5d"))
    new_med = _safe_float(new_candidate_metrics.get("median_5d"))
    new_wr = _safe_float(new_candidate_metrics.get("win_rate"))

    checks = {
        "validation_median_improved": s_med is not None and b_med is not None and s_med > b_med,
        "validation_tew_improved": s_tew is not None and b_tew is not None and s_tew > b_tew,
        "validation_win_rate_ok": s_wr is not None and b_wr is not None and s_wr >= b_wr - 0.05,
        "mdd_not_worse": s_mdd is not None and b_mdd is not None and s_mdd >= b_mdd - 1.0,
        "first_signal_ok": s_fs is None or b_fs is None or s_fs >= b_fs - 2.0,
        "new_candidates_ok": new_med is None or new_med >= -1.0,
        "new_win_rate_ok": new_wr is None or new_wr >= 0.35,
        "not_outlier_dependent": WARNING_OUTLIER_DEPENDENT not in warnings,
    }
    if all(checks.values()):
        return {"status": SHADOW_CANDIDATE, "checks": checks, "scenario": scenario_name}
    return {"status": SHADOW_REJECTED, "checks": checks, "scenario": scenario_name}


def lookup_outcomes(
    rows: Sequence[Dict[str, Any]],
    *,
    output_dir: Path,
    as_of_trade_date: str,
    settle_missing: bool = True,
) -> List[Dict[str, Any]]:
    """Join outcomes from ledger; optionally settle missing into row copies only."""
    from screener_factor_analysis import load_observation_ledger
    from screener_outcomes import settle_observation_outcome

    ledger_path = Path(output_dir) / "quality" / "screener_candidate_observations.jsonl"
    ledger = load_observation_ledger(ledger_path)
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for obs in ledger:
        t = _ticker_of(obs)
        td = str(obs.get("trade_date") or "")
        if t and td:
            by_key[(t, td)] = obs

    out: List[Dict[str, Any]] = []
    for row in rows:
        r = dict(row)
        key = (str(r.get("ticker") or "").upper(), str(r.get("trade_date") or ""))
        obs = by_key.get(key)
        if obs:
            for h in HORIZONS:
                hk = RETURN_KEYS[h]
                if obs.get(hk) is not None:
                    r[hk] = _safe_float(obs.get(hk))
            for ek in ("max_drawdown_5d_pct", "max_runup_5d_pct"):
                if obs.get(ek) is not None:
                    r[ek] = _safe_float(obs.get(ek))
        elif settle_missing and r.get("return_5d_pct") is None:
            stub = {
                "ticker": r.get("ticker"),
                "trade_date": r.get("trade_date"),
                "reference_price": r.get("price"),
                "decision_price": r.get("price"),
            }
            settled = settle_observation_outcome(stub, as_of_trade_date=as_of_trade_date)
            for h in HORIZONS:
                hk = RETURN_KEYS[h]
                if settled.get(hk) is not None:
                    r[hk] = _safe_float(settled.get(hk))
            for ek in ("max_drawdown_5d_pct", "max_runup_5d_pct"):
                if settled.get(ek) is not None:
                    r[ek] = _safe_float(settled.get(ek))
        out.append(r)
    return out


def load_replay_days(
    *,
    output_dir: Path,
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load includable DECISION runs with verified full scored universe."""
    from screener_factor_analysis import discover_runs_in_window, inspect_artifact_factor_schema

    discovered = discover_runs_in_window(
        Path(output_dir),
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
    )
    days: List[Dict[str, Any]] = []
    source_counts: Counter = Counter()
    trust_counts_raw: Counter = Counter()
    accepted_trust_counts: Counter = Counter()
    excluded_trust_counts: Counter = Counter()
    artifact_integrity_counts: Counter = Counter()
    market_state_exclusion_counts: Counter = Counter()
    run_decisions: List[Dict[str, Any]] = []
    universe_sizes: List[int] = []
    sample_rows: List[Dict[str, Any]] = []

    for run_dir in discovered.run_dirs:
        run_dir = Path(run_dir)
        merged = discovered.merged_by_run.get(str(run_dir)) or {}
        trade_date = str(merged.get("trade_date") or "")
        market_v = str(merged.get("market") or market).upper()
        session_v = str(merged.get("session") or session).lower()
        run_id = str(merged.get("run_id") or run_dir.name)

        trust = assess_run_trust(run_dir)
        trust_counts_raw[trust["trust_status"]] += 1

        scores_integrity = verify_screener_scores_integrity(
            run_dir,
            expected_run_id=run_id,
            expected_market=market_v,
            expected_session=session_v,
            expected_trade_date=trade_date,
        )
        # Avoid carrying full row payloads in decisions
        integrity_summary = {
            k: v for k, v in scores_integrity.items() if k != "rows"
        }
        artifact_integrity_counts[scores_integrity.get("status") or "FAIL"] += 1

        decision = replay_inclusion_decision(trust, scores_integrity)
        decision_rec = {
            "trade_date": trade_date,
            "run_id": run_id,
            "trust_status": decision["trust_status"],
            "trust_reason": decision["trust_reason"],
            "artifact": PRIMARY_SCORES_ARTIFACT,
            "artifact_sha_ok": bool(scores_integrity.get("sha_ok")),
            "artifact_row_count_ok": bool(scores_integrity.get("row_count_ok")),
            "schema_ok": bool(scores_integrity.get("schema_ok")),
            "artifact_integrity_status": decision["artifact_integrity_status"],
            "included": decision["included"],
            "exclusion_reason": decision["exclusion_reason"],
            "screener_scores_integrity": scores_integrity.get("status"),
        }
        run_decisions.append(decision_rec)

        if not decision["included"]:
            excluded_trust_counts[trust["trust_status"]] += 1
            continue

        hist_ms = load_historical_market_state(run_dir, merged)
        if not hist_ms.get("ok"):
            decision_rec["included"] = False
            decision_rec["exclusion_reason"] = hist_ms.get("status")
            decision_rec["historical_market_state_status"] = hist_ms.get("status")
            market_state_exclusion_counts[hist_ms.get("status") or "UNKNOWN"] += 1
            excluded_trust_counts[trust["trust_status"]] += 1
            run_decisions[-1] = decision_rec
            continue

        accepted_trust_counts[trust["trust_status"]] += 1
        universe = list(scores_integrity.get("rows") or [])
        if not universe:
            # Fallback resolve (should not happen after PASS)
            universe, source_name, src_meta = resolve_full_scored_universe(run_dir)
        else:
            source_name = PRIMARY_SCORES_ARTIFACT
            src_meta = {"source": source_name, "row_count": len(universe)}

        if not universe:
            excluded_trust_counts[trust["trust_status"]] += 1
            decision_rec["included"] = False
            decision_rec["exclusion_reason"] = "SCORES_EMPTY_AFTER_PASS"
            continue

        source_counts[source_name] += 1
        universe_sizes.append(len(universe))
        if len(sample_rows) < 50:
            sample_rows.extend(universe[: max(0, 50 - len(sample_rows))])

        regime = load_market_regime(run_dir, merged)
        hydrated = [
            normalize_score_record(
                r,
                trade_date=trade_date,
                market=market_v,
                session=session_v,
                source_run_id=run_id,
            )
            for r in universe
        ]
        actual_cands = load_actual_production_candidates(run_dir)
        actual_tickers = [_ticker_of(c) for c in actual_cands if _ticker_of(c)]
        actual_scores = {
            _ticker_of(c): _safe_float(
                c.get("Score") if c.get("Score") is not None else c.get("score")
            )
            for c in actual_cands
            if _ticker_of(c)
        }

        days.append(
            {
                "trade_date": trade_date,
                "market": market_v,
                "session": session_v,
                "source_run_id": run_id,
                "run_dir": str(run_dir),
                "regime": regime,
                "trust_status": trust["trust_status"],
                "trust_reason": trust["trust_reason"],
                "trust_issues": trust.get("issues") or [],
                "artifact_integrity_status": scores_integrity.get("status"),
                "screener_scores_integrity": integrity_summary,
                "included_for_replay": True,
                "historical_market_state": hist_ms.get("market_state"),
                "historical_market_state_source": hist_ms.get("source"),
                "historical_market_state_regime": hist_ms.get("regime"),
                "historical_market_state_volatility": hist_ms.get("volatility"),
                "expected_market_multiplier": hist_ms.get("expected_multiplier"),
                "universe_source": source_name,
                "universe_source_meta": src_meta,
                "universe_rows": hydrated,
                "actual_production_tickers": actual_tickers,
                "actual_production_scores": actual_scores,
                "actual_production_count": len(actual_tickers),
            }
        )

    schema = inspect_artifact_factor_schema(sample_rows) if sample_rows else {}
    meta = {
        "included_days": len(days),
        "trust_counts": dict(accepted_trust_counts),  # backward-compatible = accepted
        "trust_counts_raw": dict(trust_counts_raw),
        "accepted_trust_counts": dict(accepted_trust_counts),
        "excluded_trust_counts": dict(excluded_trust_counts),
        "artifact_integrity_counts": dict(artifact_integrity_counts),
        "run_decisions": run_decisions,
        "universe_source_counts": dict(source_counts),
        "avg_universe_size": (
            round(sum(universe_sizes) / len(universe_sizes), 2) if universe_sizes else 0
        ),
        "universe_size_by_date": {
            d["trade_date"]: len(d["universe_rows"]) for d in days
        },
        "factor_schema": schema,
        "primary_universe_artifact": PRIMARY_SCORES_ARTIFACT,
        "scope": SCOPE_NOTE,
        "missing_market_state_policy": "exclude_run_from_baseline_valid_universe",
        "historical_market_state_exclusion_counts": dict(market_state_exclusion_counts),
        "market_state_by_date": {
            d["trade_date"]: {
                "regime": d.get("historical_market_state_regime"),
                "volatility": d.get("historical_market_state_volatility"),
                "expected_multiplier": d.get("expected_market_multiplier"),
                "source": d.get("historical_market_state_source"),
            }
            for d in days
        },
    }
    return days, meta


def replay_day_scenario(
    day: Dict[str, Any],
    *,
    scenario_name: str,
    weights: Dict[str, float],
    threshold: float,
    pipeline_params: Dict[str, Any],
    baseline_weights: Dict[str, float],
) -> Dict[str, Any]:
    universe = day.get("universe_rows") or []
    historical_market_state = day.get("historical_market_state")
    scored_obs = apply_replay_scenario_scores(
        universe,
        weights,
        baseline_weights=baseline_weights,
        historical_market_state=historical_market_state,
    )
    scored_df = build_replay_dataframe(
        scored_obs,
        weights=weights,
        threshold=threshold,
        pipeline_params=pipeline_params,
        historical_market_state=historical_market_state,
    )
    candidates_df, stages = run_candidate_pipeline(
        scored_df, threshold=threshold, pipeline_params=pipeline_params
    )
    replay_tickers = candidate_ticker_list(candidates_df)
    replay_scores = {
        str(r.get("Ticker") or ""): float(r.get("Score") or 0)
        for _, r in candidates_df.iterrows()
    } if not candidates_df.empty else {}

    baseline_compare = None
    if scenario_name == "A_BASELINE":
        baseline_compare = compare_baseline_replay_to_production(
            replay_tickers,
            day.get("actual_production_tickers") or [],
            replay_scores=replay_scores,
            actual_scores=day.get("actual_production_scores") or {},
        )

    # Full-universe ranking top-N (threshold independent)
    topn: Dict[str, Any] = {}
    for n in TOP_N_LEVELS:
        top = top_n_rows(scored_obs, n, score_key="scenario_score")
        topn[f"top_{n}"] = outcome_metrics(top, score_key="scenario_score")

    # Daily top-K matched to actual production count
    k = int(day.get("actual_production_count") or 0)
    top_k_tickers = eligible_universe_top_k(
        scored_obs,
        weights=weights,
        threshold=threshold,
        pipeline_params=pipeline_params,
        k=k,
        historical_market_state=historical_market_state,
    )

    return {
        "trade_date": day.get("trade_date"),
        "scenario": scenario_name,
        "threshold": threshold,
        "replay_tickers": replay_tickers,
        "replay_scores": replay_scores,
        "candidate_count": len(replay_tickers),
        "pipeline_stages": [s.to_dict() for s in stages],
        "baseline_compare": baseline_compare,
        "scored_observations": scored_obs,
        "topn": topn,
        "daily_top_k_matched": top_k_tickers,
        "regime": day.get("regime"),
    }


def build_migration_records(
    baseline_day: Dict[str, Any],
    scenario_day: Dict[str, Any],
    *,
    scenario_name: str,
    threshold: float,
) -> List[Dict[str, Any]]:
    base_set = set(baseline_day.get("replay_tickers") or [])
    scen_set = set(scenario_day.get("replay_tickers") or [])
    all_tickers = set()
    by_ticker_base: Dict[str, Dict[str, Any]] = {}
    by_ticker_scen: Dict[str, Dict[str, Any]] = {}
    for r in baseline_day.get("scored_observations") or []:
        t = str(r.get("ticker") or "").upper()
        if t:
            all_tickers.add(t)
            by_ticker_base[t] = r
    for r in scenario_day.get("scored_observations") or []:
        t = str(r.get("ticker") or "").upper()
        if t:
            all_tickers.add(t)
            by_ticker_scen[t] = r

    records: List[Dict[str, Any]] = []
    for t in sorted(all_tickers):
        group = classify_migration(t, baseline_candidates=base_set, scenario_candidates=scen_set)
        br = by_ticker_base.get(t) or by_ticker_scen.get(t) or {}
        sr = by_ticker_scen.get(t) or br
        records.append(
            {
                "trade_date": baseline_day.get("trade_date"),
                "scenario": scenario_name,
                "threshold": threshold,
                "ticker": t,
                "migration_group": group,
                "baseline_score": _safe_float(br.get("baseline_score")),
                "scenario_score": _safe_float(sr.get("scenario_score")),
                "baseline_candidate": t in base_set,
                "scenario_candidate": t in scen_set,
                "return_5d_pct": _safe_float(br.get("return_5d_pct") or sr.get("return_5d_pct")),
                "factors": br.get("factors") or sr.get("factors") or {},
            }
        )
    return records


def flatten_daily_candidates(
    day_results: Sequence[Dict[str, Any]],
    *,
    baseline_day: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    base_set = set((baseline_day or {}).get("replay_tickers") or [])
    for dr in day_results:
        scen_set = set(dr.get("replay_tickers") or [])
        rank_map = {t: i + 1 for i, t in enumerate(dr.get("replay_tickers") or [])}
        for r in dr.get("scored_observations") or []:
            t = str(r.get("ticker") or "").upper()
            if t not in scen_set and t not in base_set:
                continue
            rows.append(
                {
                    "trade_date": dr.get("trade_date"),
                    "scenario": dr.get("scenario"),
                    "threshold": dr.get("threshold"),
                    "ticker": t,
                    "scenario_score": _safe_float(r.get("scenario_score")),
                    "rank": rank_map.get(t),
                    "sector": r.get("sector"),
                    "issuer": r.get("issuer_group"),
                    "baseline_score": _safe_float(r.get("baseline_score")),
                    "baseline_candidate": t in base_set,
                    "scenario_candidate": t in scen_set,
                    "return_1d_pct": _safe_float(r.get("return_1d_pct")),
                    "return_3d_pct": _safe_float(r.get("return_3d_pct")),
                    "return_5d_pct": _safe_float(r.get("return_5d_pct")),
                    "return_10d_pct": _safe_float(r.get("return_10d_pct")),
                    "MDD": _safe_float(r.get("max_drawdown_5d_pct")),
                    "runup": _safe_float(r.get("max_runup_5d_pct")),
                    "regime": dr.get("regime"),
                }
            )
    return rows


def a_vs_c_comparison(
    baseline_candidates: Sequence[Dict[str, Any]],
    c_candidates: Sequence[Dict[str, Any]],
    migration: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    def _metrics(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        return outcome_metrics(rows, score_key="scenario_score")

    a_only = [r for r in migration if r.get("migration_group") == MIGRATION_DROP]
    c_only = [r for r in migration if r.get("migration_group") == MIGRATION_NEW]
    both = [r for r in migration if r.get("migration_group") == MIGRATION_KEEP]
    keep_rows = [r for r in baseline_candidates if r.get("scenario_candidate") and r.get("baseline_candidate")]
    return {
        "A_candidate_count": len({r.get("ticker") for r in baseline_candidates if r.get("scenario_candidate")}),
        "C_candidate_count": len({r.get("ticker") for r in c_candidates if r.get("scenario_candidate")}),
        "A_only_count": len(a_only),
        "C_only_count": len(c_only),
        "both_count": len(both),
        "A_metrics": _metrics(baseline_candidates),
        "C_metrics": _metrics(c_candidates),
        "C_NEW": {
            "n": len(c_only),
            **_metrics([{"return_5d_pct": r.get("return_5d_pct")} for r in c_only]),
        },
        "C_DROP": {
            "n": len(a_only),
            **_metrics([{"return_5d_pct": r.get("return_5d_pct")} for r in a_only]),
        },
        "C_KEEP": {
            "n": len(both),
            **_metrics([{"return_5d_pct": r.get("return_5d_pct")} for r in both]),
        },
    }


def threshold_grid_metrics(
    candidate_rows: Sequence[Dict[str, Any]],
    *,
    scenario: str,
    threshold: float,
) -> Dict[str, Any]:
    passed = [r for r in candidate_rows if r.get("scenario_candidate")]
    m = outcome_metrics(passed, score_key="scenario_score")
    by_date: Counter = Counter()
    for r in passed:
        by_date[str(r.get("trade_date") or "")] += 1
    m.update(
        {
            "scenario": scenario,
            "threshold": threshold,
            "daily_candidate_counts": dict(by_date),
            "avg_daily_candidate_count": round(sum(by_date.values()) / max(len(by_date), 1), 4),
        }
    )
    return m


def find_count_equivalent_threshold(
    grid_rows: Sequence[Dict[str, Any]],
    *,
    baseline_count: int,
    scenario: str,
) -> Optional[float]:
    scen_rows = [r for r in grid_rows if r.get("scenario") == scenario]
    if not scen_rows:
        return None
    best = min(
        scen_rows,
        key=lambda r: abs(int(r.get("pass_count") or r.get("observations") or 0) - baseline_count),
    )
    return _safe_float(best.get("threshold"))


def analyze_full_universe_replay(
    days: Sequence[Dict[str, Any]],
    *,
    baseline_weights: Dict[str, float],
    pipeline_params: Dict[str, Any],
    scenarios: Optional[Dict[str, Dict[str, float]]] = None,
    market: str = "SP500",
    session: str = "pm",
    start_trade_date: str = "20260727",
    end_trade_date: str = "20260821",
    train_end: str = DEFAULT_TRAIN_END,
    validation_start: str = DEFAULT_VALIDATION_START,
    load_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    scenarios = scenarios or SCENARIOS
    fixed_threshold_scenarios = {"A_BASELINE", "C_POS52W_ZERO"}

    # Baseline score reconstruction on full universe (all rows, all days)
    all_universe_rows: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    for d in days:
        all_universe_rows.extend(d.get("universe_rows") or [])

    if not days or not all_universe_rows:
        warnings.append({"type": "NO_TRUSTED_RUNS", "detail": "no trusted DECISION runs with scored universe in window"})
        return {
            "status": STATUS_NO_DATA,
            "scope": SCOPE_NOTE,
            "market": market,
            "session": session,
            "start_trade_date": start_trade_date,
            "end_trade_date": end_trade_date,
            "train_end": train_end,
            "validation_start": validation_start,
            "baseline_weights": dict(baseline_weights),
            "baseline_weight_sum": weight_sum(baseline_weights),
            "baseline_reconstruction": {"ok": False, "n_checked": 0, "status": "NO_DATA"},
            "baseline_replay": {"ok": False, "avg_match_pct": 0.0, "daily": []},
            "universe_meta": load_meta,
            "warnings": warnings,
            "production_policy_unchanged": True,
            "recommendation": "NONE — no trusted full-universe data in window",
            "shadow_verdicts": {
                n: {"status": SHADOW_REJECTED, "reason": "no_data"}
                for n in ("C_POS52W_ZERO", "D_TECH_POS_DOWN", "E_CONSERVATIVE")
            },
            "_export_daily_candidates": [],
            "_export_migration": [],
            "_export_topn": [],
            "_export_scenario_summary": [],
        }

    recon = reconstruct_baseline_scores_replay(
        all_universe_rows,
        baseline_weights,
        market_state_by_date={
            str(d.get("trade_date") or ""): d.get("historical_market_state")
            for d in days
            if d.get("historical_market_state") is not None
        },
    )

    baseline_replay_matches: List[Dict[str, Any]] = []
    baseline_replay_ok = True
    day_results_by_scenario: Dict[str, List[Dict[str, Any]]] = {name: [] for name in scenarios}
    migration_all: List[Dict[str, Any]] = []
    threshold_grid: List[Dict[str, Any]] = []
    daily_candidate_rows: List[Dict[str, Any]] = []
    topn_rows: List[Dict[str, Any]] = []
    d_threshold_day_results: Dict[float, List[Dict[str, Any]]] = {
        float(t): [] for t in D_RESEARCH_THRESHOLDS
    }
    d_threshold_migrations: Dict[float, List[Dict[str, Any]]] = {
        float(t): [] for t in D_RESEARCH_THRESHOLDS
    }

    output_dir = Path((load_meta or {}).get("output_dir") or ".")

    if not recon.get("ok"):
        baseline_replay_ok = False
        warnings.append({"type": STATUS_BASELINE_FAILED, "detail": "factor score reconstruction failed"})
        return _abort_report(
            recon=recon,
            market=market,
            session=session,
            start_trade_date=start_trade_date,
            end_trade_date=end_trade_date,
            load_meta=load_meta,
            warnings=warnings,
        )

    # Outcome join on universe rows before replay metrics (read-only)
    for day in days:
        day["universe_rows"] = lookup_outcomes(
            day.get("universe_rows") or [],
            output_dir=output_dir,
            as_of_trade_date=end_trade_date,
            settle_missing=False,
        )

    for day in days:
        baseline_day_result = replay_day_scenario(
            day,
            scenario_name="A_BASELINE",
            weights=baseline_weights,
            threshold=PRODUCTION_THRESHOLD,
            pipeline_params=pipeline_params,
            baseline_weights=baseline_weights,
        )
        day_results_by_scenario["A_BASELINE"].append(baseline_day_result)
        bc = baseline_day_result.get("baseline_compare") or {}
        baseline_replay_matches.append({"trade_date": day.get("trade_date"), **bc})
        if not bc.get("exact_set_match", False):
            baseline_replay_ok = False

        for name, weights in scenarios.items():
            if name == "A_BASELINE":
                continue
            threshold = PRODUCTION_THRESHOLD if name in fixed_threshold_scenarios else PRODUCTION_THRESHOLD
            dr = replay_day_scenario(
                day,
                scenario_name=name,
                weights=weights,
                threshold=threshold,
                pipeline_params=pipeline_params,
                baseline_weights=baseline_weights,
            )
            day_results_by_scenario[name].append(dr)
            mig = build_migration_records(
                baseline_day_result, dr, scenario_name=name, threshold=threshold
            )
            migration_all.extend(mig)

        # D research thresholds only (0.38 / 0.40 / 0.42) — no E grid, no finer grid
        for thr in D_RESEARCH_THRESHOLDS:
            dr = replay_day_scenario(
                day,
                scenario_name=D_RESEARCH_SCENARIO,
                weights=scenarios[D_RESEARCH_SCENARIO],
                threshold=float(thr),
                pipeline_params=pipeline_params,
                baseline_weights=baseline_weights,
            )
            d_threshold_day_results[float(thr)].append(dr)
            mig = build_migration_records(
                baseline_day_result,
                dr,
                scenario_name=D_RESEARCH_SCENARIO,
                threshold=float(thr),
            )
            d_threshold_migrations[float(thr)].extend(mig)
            cand_rows = collect_candidate_obs_from_day_results([dr])
            gm = threshold_grid_metrics(
                cand_rows, scenario=D_RESEARCH_SCENARIO, threshold=float(thr)
            )
            gm["trade_date"] = day.get("trade_date")
            threshold_grid.append(gm)

    if not baseline_replay_ok:
        warnings.append(
            {
                "type": WARNING_BASELINE_REPLAY_MISMATCH,
                "detail": "A_BASELINE replay candidate set does not exactly match Production on all days",
            }
        )

    baseline_daily = day_results_by_scenario["A_BASELINE"]
    for name, results in day_results_by_scenario.items():
        for dr in results:
            for n_key, block in (dr.get("topn") or {}).items():
                topn_rows.append(
                    {
                        "scenario": name,
                        "trade_date": dr.get("trade_date"),
                        "top_n": n_key,
                        **{k: block.get(k) for k in block},
                    }
                )

    daily_candidate_rows = []
    for name, results in day_results_by_scenario.items():
        for i, dr in enumerate(results):
            base_dr = baseline_daily[i] if i < len(baseline_daily) else None
            daily_candidate_rows.extend(flatten_daily_candidates([dr], baseline_day=base_dr))

    # Scenario summaries (full / train / validation)
    scenario_summaries: Dict[str, Any] = {}
    all_candidate_obs: List[Dict[str, Any]] = []
    for name, results in day_results_by_scenario.items():
        obs_rows: List[Dict[str, Any]] = []
        for dr in results:
            for t in dr.get("replay_tickers") or []:
                src = next(
                    (r for r in (dr.get("scored_observations") or []) if str(r.get("ticker")).upper() == t),
                    {},
                )
                obs_rows.append({**src, "scenario_candidate": True, "trade_date": dr.get("trade_date")})
        all_candidate_obs.extend(obs_rows)
        train, valid = split_train_validation(obs_rows)
        thr = PRODUCTION_THRESHOLD
        full_m = window_outcome_block(obs_rows)
        train_m = window_outcome_block(train)
        valid_m = window_outcome_block(valid)
        scenario_summaries[name] = {
            "weights": dict(scenarios[name]),
            "weight_sum": weight_sum(scenarios[name]),
            "threshold": thr,
            "full": full_m,
            "train": train_m,
            "validation": valid_m,
            "sector_concentration": sector_concentration(obs_rows),
            "daily_candidate_counts": Counter(str(r.get("trade_date")) for r in obs_rows),
        }

    # A vs C core comparison (validation)
    c_mig = [m for m in migration_all if m.get("scenario") == "C_POS52W_ZERO"]
    a_c = a_vs_c_comparison(
        [r for r in daily_candidate_rows if r.get("scenario") == "A_BASELINE" and r.get("scenario_candidate")],
        [r for r in daily_candidate_rows if r.get("scenario") == "C_POS52W_ZERO" and r.get("scenario_candidate")],
        c_mig,
    )

    # Regime splits
    regime_results: Dict[str, Any] = {}
    for name, results in day_results_by_scenario.items():
        by_regime: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for dr in results:
            reg = str(dr.get("regime") or "UNKNOWN")
            for t in dr.get("replay_tickers") or []:
                src = next(
                    (r for r in (dr.get("scored_observations") or []) if str(r.get("ticker")).upper() == t),
                    {},
                )
                by_regime[reg].append(src)
        regime_results[name] = {
            reg: {
                "candidate_count": len(rows),
                "metrics": outcome_metrics(rows),
            }
            for reg, rows in by_regime.items()
        }

    # Daily top-K matched summary
    topk_matched: Dict[str, Any] = {}
    for name in ("A_BASELINE", "C_POS52W_ZERO", "D_TECH_POS_DOWN", "E_CONSERVATIVE"):
        if name not in day_results_by_scenario:
            continue
        rows: List[Dict[str, Any]] = []
        for dr in day_results_by_scenario[name]:
            for t in dr.get("daily_top_k_matched") or []:
                src = next(
                    (r for r in (dr.get("scored_observations") or []) if str(r.get("ticker")).upper() == t),
                    {"ticker": t, "trade_date": dr.get("trade_date")},
                )
                rows.append(src)
        topk_matched[name] = outcome_metrics(rows)

    # Aggregate threshold grid by scenario+threshold (compat export)
    grid_agg: Dict[Tuple[str, float], Dict[str, Any]] = {}
    for row in threshold_grid:
        key = (str(row.get("scenario")), float(row.get("threshold") or 0))
        bucket = grid_agg.setdefault(
            key,
            {
                "scenario": key[0],
                "threshold": key[1],
                "observations": 0,
                "mature_5d_count": 0,
                "daily_counts": [],
            },
        )
        bucket["observations"] += int(row.get("observations") or 0)
        bucket["mature_5d_count"] += int(row.get("mature_5d_count") or 0)
        bucket["daily_counts"].append(int(row.get("observations") or 0))
        for mk in ("mean_5d", "median_5d", "win_rate", "ticker_equal_weight_mean_5d", "mean_mdd_5d"):
            if row.get(mk) is not None:
                bucket.setdefault(mk + "_samples", []).append(row.get(mk))
    grid_summary: List[Dict[str, Any]] = []
    baseline_pass = int(scenario_summaries.get("A_BASELINE", {}).get("full", {}).get("observations") or 0)
    for (scen, thr), bucket in sorted(grid_agg.items()):
        rec = {
            "scenario": scen,
            "threshold": thr,
            "total_observations": bucket["observations"],
            "mature_5d_count": bucket["mature_5d_count"],
            "avg_daily_candidate_count": round(
                sum(bucket["daily_counts"]) / max(len(bucket["daily_counts"]), 1), 4
            ),
        }
        for mk in ("mean_5d", "median_5d", "win_rate", "ticker_equal_weight_mean_5d", "mean_mdd_5d"):
            samples = bucket.get(mk + "_samples") or []
            rec[mk] = round(sum(samples) / len(samples), 6) if samples else None
        rec["delta_vs_baseline_count"] = bucket["observations"] - baseline_pass
        grid_summary.append(rec)

    count_equiv: Dict[str, Optional[float]] = {}
    count_equiv[D_RESEARCH_SCENARIO] = find_count_equivalent_threshold(
        [
            {
                "scenario": r["scenario"],
                "threshold": r["threshold"],
                "pass_count": r["total_observations"],
            }
            for r in grid_summary
        ],
        baseline_count=baseline_pass,
        scenario=D_RESEARCH_SCENARIO,
    )

    # --- D historical research diagnostics (0.38 / 0.40 / 0.42) ---
    baseline_obs = collect_candidate_obs_from_day_results(baseline_daily)
    baseline_train, baseline_valid = split_train_validation(
        baseline_obs, train_end=train_end, validation_start=validation_start
    )
    baseline_windows = {
        "full": window_outcome_block(baseline_obs),
        "train": window_outcome_block(baseline_train),
        "validation": window_outcome_block(baseline_valid),
    }
    baseline_topk_obs = collect_topk_obs_from_day_results(baseline_daily)
    baseline_topk_metrics = window_outcome_block(baseline_topk_obs)

    d_threshold_diagnostics: List[Dict[str, Any]] = []
    for thr in D_RESEARCH_THRESHOLDS:
        thr_f = float(thr)
        drs = d_threshold_day_results.get(thr_f) or []
        windows = summarize_d_threshold_windows(
            drs, train_end=train_end, validation_start=validation_start
        )
        mig = d_threshold_migrations.get(thr_f) or []
        mig_full = migration_group_metrics(mig)
        mig_valid_recs = [
            m
            for m in mig
            if str(m.get("trade_date") or "") >= validation_start
            and m.get("migration_group") in (MIGRATION_KEEP, MIGRATION_DROP, MIGRATION_NEW)
        ]
        mig_valid = migration_group_metrics(mig_valid_recs)
        topk_obs = collect_topk_obs_from_day_results(drs)
        topk_m = window_outcome_block(topk_obs)
        cand_obs = collect_candidate_obs_from_day_results(drs)
        _, cand_valid = split_train_validation(
            cand_obs, train_end=train_end, validation_start=validation_start
        )
        outlier_full = metrics_excluding_largest_abs_return(cand_obs)
        outlier_valid = metrics_excluding_largest_abs_return(cand_valid)
        outlier_flag = detect_outlier_dependent(windows.get("validation") or {})
        robust = evaluate_d_robust_improvement(
            baseline_validation=baseline_windows["validation"],
            d_validation=windows["validation"],
            baseline_topk=baseline_topk_metrics,
            d_topk=topk_m,
            migration_validation=mig_valid,
            outlier_flag=outlier_flag,
        )
        d_threshold_diagnostics.append(
            {
                "scenario": D_RESEARCH_SCENARIO,
                "threshold": thr_f,
                "windows": windows,
                "migration": {"full": mig_full, "validation": mig_valid},
                "daily_topk_matched": {
                    "A_BASELINE": baseline_topk_metrics,
                    D_RESEARCH_SCENARIO: topk_m,
                    "note": (
                        "K = Production A candidate count per day; "
                        "separates threshold count effects from ranking quality"
                    ),
                },
                "outlier_sensitivity": {
                    "full": outlier_full,
                    "validation": outlier_valid,
                    "outlier_dependent_flag": outlier_flag,
                },
                "robust_improvement": robust.get("robust_improvement"),
                "robust_checks": robust.get("checks"),
            }
        )

    # Regime split A vs D (at research shadow threshold 0.40)
    regime_avd: Dict[str, Any] = {}
    d40_results = d_threshold_day_results.get(0.40) or []
    a_by_regime: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    d_by_regime: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for dr in baseline_daily:
        reg = str(dr.get("regime") or "UNKNOWN").lower()
        a_by_regime[reg].extend(collect_candidate_obs_from_day_results([dr]))
    for dr in d40_results:
        reg = str(dr.get("regime") or "UNKNOWN").lower()
        d_by_regime[reg].extend(collect_candidate_obs_from_day_results([dr]))
    all_regimes = sorted(set(a_by_regime) | set(d_by_regime))
    for reg in all_regimes:
        a_rows = a_by_regime.get(reg) or []
        d_rows = d_by_regime.get(reg) or []
        a_m = window_outcome_block(a_rows)
        d_m = window_outcome_block(d_rows)
        low_sample = (
            int(a_m.get("observations") or 0) < LOW_SAMPLE_REGIME_N
            or int(d_m.get("observations") or 0) < LOW_SAMPLE_REGIME_N
            or int(a_m.get("mature_5d_count") or 0) < LOW_SAMPLE_REGIME_N
            or int(d_m.get("mature_5d_count") or 0) < LOW_SAMPLE_REGIME_N
        )
        regime_avd[reg] = {
            "A_BASELINE": a_m,
            D_RESEARCH_SCENARIO: d_m,
            "LOW_SAMPLE": low_sample,
            "sample_flag": WARNING_LOW_SAMPLE if low_sample else None,
        }

    historical_decision = decide_historical_weight_search(d_threshold_diagnostics)

    # Warnings
    val_n = int(scenario_summaries.get("A_BASELINE", {}).get("validation", {}).get("observations") or 0)
    if val_n < 8:
        warnings.append({"type": WARNING_INSUFFICIENT_VALIDATION, "detail": f"validation n={val_n}"})
    if val_n < 8:
        warnings.append({"type": WARNING_LOW_SAMPLE, "detail": f"validation mature n={val_n}"})
    for name, summ in scenario_summaries.items():
        if name == "A_BASELINE":
            continue
        full_m = summ.get("full") or {}
        base_full = scenario_summaries["A_BASELINE"]["full"]
        if int(full_m.get("observations") or 0) < int(base_full.get("observations") or 1) * 0.5:
            warnings.append(
                {"type": WARNING_CANDIDATE_COUNT_COLLAPSE, "scenario": name, "detail": "candidate count collapsed vs baseline"}
            )
        if detect_outlier_dependent(summ.get("validation") or {}):
            warnings.append({"type": WARNING_OUTLIER_DEPENDENT, "scenario": name, "detail": "validation mean vs median gap"})

    # Shadow evaluation (only if baseline replay ok) — Production unchanged
    shadow_verdicts: Dict[str, Any] = {}
    if baseline_replay_ok:
        base_val = scenario_summaries["A_BASELINE"]["validation"]
        for name in ("C_POS52W_ZERO", "D_TECH_POS_DOWN", "E_CONSERVATIVE"):
            new_m = [m for m in migration_all if m.get("scenario") == name and m.get("migration_group") == MIGRATION_NEW]
            shadow_verdicts[name] = evaluate_shadow_candidate(
                name,
                baseline_replay_ok=True,
                validation_metrics=scenario_summaries[name]["validation"],
                baseline_validation_metrics=base_val,
                new_candidate_metrics=outcome_metrics(new_m),
                warnings=[w.get("type", "") for w in warnings],
                validation_obs_count=val_n,
            )
    else:
        for name in ("C_POS52W_ZERO", "D_TECH_POS_DOWN", "E_CONSERVATIVE"):
            shadow_verdicts[name] = {"status": SHADOW_REJECTED, "reason": "baseline_replay_mismatch"}

    match_pcts = [m.get("match_pct") for m in baseline_replay_matches if m.get("match_pct") is not None]
    avg_match = round(sum(match_pcts) / len(match_pcts), 4) if match_pcts else 0.0

    market_state_summary = {
        "primary_source": "screener_run_meta.market_state",
        "fallback_source": "inputs/market_regime_inputs.json",
        "uses_calculate_market_adjusted_score": True,
        "missing_market_state_policy": (load_meta or {}).get("missing_market_state_policy"),
        "historical_market_state_exclusion_counts": (load_meta or {}).get(
            "historical_market_state_exclusion_counts"
        ),
        "by_date": (load_meta or {}).get("market_state_by_date") or {},
        "reconstruction_date_summary": recon.get("date_summary") or [],
    }

    return {
        "status": STATUS_OK if baseline_replay_ok else STATUS_BASELINE_FAILED,
        "scope": SCOPE_NOTE,
        "market": market,
        "session": session,
        "start_trade_date": start_trade_date,
        "end_trade_date": end_trade_date,
        "train_end": train_end,
        "validation_start": validation_start,
        "baseline_weights": dict(baseline_weights),
        "baseline_weight_sum": weight_sum(baseline_weights),
        "baseline_reconstruction": recon,
        "market_state_summary": market_state_summary,
        "baseline_replay": {
            "ok": baseline_replay_ok,
            "avg_match_pct": avg_match,
            "daily": baseline_replay_matches,
        },
        "universe_meta": load_meta,
        "scenarios": scenario_summaries,
        "a_vs_c": a_c,
        "regime_results": regime_results,
        "regime_split_a_vs_d": regime_avd,
        "daily_top_k_matched": topk_matched,
        "threshold_grid": grid_summary,
        "count_equivalent_threshold": count_equiv,
        "d_threshold_diagnostics": d_threshold_diagnostics,
        "baseline_windows_for_d_compare": baseline_windows,
        "historical_decision": historical_decision,
        "research_status": STATUS_RESEARCH_ONLY,
        "shadow_verdicts": shadow_verdicts,
        "warnings": warnings,
        "production_policy_unchanged": True,
        "recommendation": (
            f"{STATUS_RESEARCH_ONLY} — read-only full-universe replay; "
            "no Production change"
        ),
        "_export_daily_candidates": daily_candidate_rows,
        "_export_migration": migration_all
        + [m for mig_list in d_threshold_migrations.values() for m in mig_list],
        "_export_topn": topn_rows,
        "_export_scenario_summary": _flatten_scenario_summary(scenario_summaries),
    }


def _abort_report(
    *,
    recon: Dict[str, Any],
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
    load_meta: Optional[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "status": STATUS_BASELINE_FAILED,
        "scope": SCOPE_NOTE,
        "market": market,
        "session": session,
        "start_trade_date": start_trade_date,
        "end_trade_date": end_trade_date,
        "baseline_reconstruction": recon,
        "baseline_replay": {"ok": False, "avg_match_pct": 0.0, "daily": []},
        "universe_meta": load_meta,
        "warnings": warnings,
        "production_policy_unchanged": True,
        "recommendation": "NONE — baseline failed; shadow results excluded from policy use",
        "shadow_verdicts": {
            n: {"status": SHADOW_REJECTED, "reason": "baseline_failed"}
            for n in ("C_POS52W_ZERO", "D_TECH_POS_DOWN", "E_CONSERVATIVE")
        },
    }


def _flatten_scenario_summary(scenarios: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name, block in scenarios.items():
        for window in ("full", "train", "validation"):
            m = block.get(window) or {}
            rows.append(
                {
                    "scenario": name,
                    "window": window,
                    "threshold": block.get("threshold"),
                    "weight_sum": block.get("weight_sum"),
                    "observations": m.get("observations"),
                    "mature_5d_count": m.get("mature_5d_count"),
                    "unique_tickers": m.get("unique_tickers"),
                    "mean_5d": m.get("mean_5d"),
                    "median_5d": m.get("median_5d"),
                    "win_rate": m.get("win_rate"),
                    "p25": m.get("p25"),
                    "p75": m.get("p75"),
                    "mean_mdd_5d": m.get("mean_mdd_5d"),
                    "mean_runup_5d": m.get("mean_runup_5d"),
                    "ticker_equal_weight_mean_5d": m.get("ticker_equal_weight_mean_5d"),
                    "first_signal_mean_5d": m.get("first_signal_mean_5d"),
                    "first_signal_median_5d": m.get("first_signal_median_5d"),
                    "first_signal_win_rate": m.get("first_signal_win_rate"),
                    "avg_daily_candidate_count": m.get("avg_daily_candidate_count"),
                    "spearman_5d": m.get("spearman_5d"),
                }
            )
    return rows


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: List[str] = []
    seen: Set[str] = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                keys.append(k)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row = dict(r)
            if "factors" in row and isinstance(row["factors"], dict):
                for fn, fv in row["factors"].items():
                    row[f"factor_{fn}"] = fv
                del row["factors"]
            w.writerow(row)


def render_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Screener Full-Universe Weight Replay")
    lines.append("")
    um = report.get("universe_meta") or {}
    lines.append(
        f"- Window: `{report.get('start_trade_date')}`–`{report.get('end_trade_date')}` "
        f"`{report.get('market')}` `{report.get('session')}`"
    )
    lines.append(f"- Status: `{report.get('status')}` · scope=`{report.get('scope')}`")
    lines.append(f"- Research status: `{report.get('research_status') or STATUS_RESEARCH_ONLY}`")
    hd = report.get("historical_decision") or {}
    lines.append(
        f"- Historical decision: `{hd.get('historical_weight_search')}` "
        f"(production_change_recommended={hd.get('production_change_recommended', False)})"
    )
    lines.append(f"- Primary universe artifact: `{um.get('primary_universe_artifact')}`")
    lines.append(f"- Included days: {um.get('included_days')} · avg universe size: {um.get('avg_universe_size')}")
    lines.append(f"- trust_counts_raw: `{um.get('trust_counts_raw')}`")
    lines.append(f"- accepted_trust_counts: `{um.get('accepted_trust_counts')}`")
    lines.append(f"- excluded_trust_counts: `{um.get('excluded_trust_counts')}`")
    lines.append(f"- artifact_integrity_counts: `{um.get('artifact_integrity_counts')}`")
    br = report.get("baseline_replay") or {}
    lines.append(f"- Baseline replay match avg: {br.get('avg_match_pct')}% · ok={br.get('ok')}")
    recon = report.get("baseline_reconstruction") or {}
    lines.append(
        f"- Baseline reconstruction: ok={recon.get('ok')} "
        f"n_checked={recon.get('n_checked')} failure_count={recon.get('failure_count')} "
        f"max_abs_error={recon.get('max_abs_error')}"
    )
    ms = report.get("market_state_summary") or {}
    lines.append(
        f"- Historical MarketState: primary=`{ms.get('primary_source')}` "
        f"fallback=`{ms.get('fallback_source')}` "
        f"uses_calculate_market_adjusted_score={ms.get('uses_calculate_market_adjusted_score')}"
    )
    lines.append("- Production policy: **unchanged**")
    lines.append("")
    if report.get("status") == STATUS_BASELINE_FAILED:
        lines.append("## Baseline failure — shadow policy use blocked")
        for w in report.get("warnings") or []:
            lines.append(f"- **{w.get('type')}**: {w.get('detail')}")
        return "\n".join(lines) + "\n"

    lines.append("## A vs C (threshold 0.48)")
    ac = report.get("a_vs_c") or {}
    lines.append(f"- A candidates: {ac.get('A_candidate_count')} · C candidates: {ac.get('C_candidate_count')}")
    lines.append(f"- C NEW n={((ac.get('C_NEW') or {}).get('n'))} · C DROP n={((ac.get('C_DROP') or {}).get('n'))}")
    lines.append("")

    lines.append("## D research thresholds (0.38 / 0.40 / 0.42)")
    for block in report.get("d_threshold_diagnostics") or []:
        thr = block.get("threshold")
        lines.append(f"### D_TECH_POS_DOWN @ {thr}")
        for window in ("full", "train", "validation"):
            m = (block.get("windows") or {}).get(window) or {}
            lines.append(
                f"- **{window}**: n={m.get('observations')} tickers={m.get('unique_tickers')} "
                f"mean5d={m.get('mean_5d')} median5d={m.get('median_5d')} wr={m.get('win_rate')} "
                f"tew={m.get('ticker_equal_weight_mean_5d')} "
                f"fs_med={m.get('first_signal_median_5d')} "
                f"avg_daily={m.get('avg_daily_candidate_count')}"
            )
        lines.append(f"- robust_improvement={block.get('robust_improvement')}")
        mig_v = ((block.get("migration") or {}).get("validation") or {})
        lines.append(
            f"- validation migration KEEP n={((mig_v.get(MIGRATION_KEEP) or {}).get('n'))} "
            f"DROP n={((mig_v.get(MIGRATION_DROP) or {}).get('n'))} "
            f"NEW n={((mig_v.get(MIGRATION_NEW) or {}).get('n'))}"
        )
        lines.append("")

    lines.append("## Regime split A vs D@0.40")
    for reg, block in (report.get("regime_split_a_vs_d") or {}).items():
        flag = " LOW_SAMPLE" if block.get("LOW_SAMPLE") else ""
        a_m = block.get("A_BASELINE") or {}
        d_m = block.get(D_RESEARCH_SCENARIO) or {}
        lines.append(
            f"- **{reg}**{flag}: A med={a_m.get('median_5d')} (n={a_m.get('observations')}) · "
            f"D med={d_m.get('median_5d')} (n={d_m.get('observations')})"
        )
    lines.append("")

    lines.append("## Shadow verdicts")
    for name, v in (report.get("shadow_verdicts") or {}).items():
        lines.append(f"- **{name}**: `{v.get('status')}` {v.get('reason') or ''}")
    lines.append("")
    lines.append("## Warnings")
    for w in report.get("warnings") or []:
        lines.append(f"- **{w.get('type')}**: {w.get('detail')}")
    lines.append("")
    if hd.get("HISTORICAL_WEIGHT_SEARCH_CLOSED") or hd.get("historical_weight_search") == STATUS_HISTORICAL_WEIGHT_SEARCH_CLOSED:
        lines.append(f"**{STATUS_HISTORICAL_WEIGHT_SEARCH_CLOSED}**")
        lines.append("")
    lines.append(str(report.get("recommendation")))
    return "\n".join(lines) + "\n"


def write_replay_outputs(report: Dict[str, Any], output_dir: Path) -> Dict[str, Path]:
    out_dir = Path(output_dir) / "quality" / "full_universe_replay"
    out_dir.mkdir(parents=True, exist_ok=True)
    start = report.get("start_trade_date") or "UNKNOWN"
    end = report.get("end_trade_date") or "UNKNOWN"
    market = report.get("market") or "UNKNOWN"
    stem = f"screener_full_universe_replay_{start}_{end}_{market}"

    public = {k: v for k, v in report.items() if not str(k).startswith("_")}
    json_path = out_dir / f"{stem}.json"
    md_path = out_dir / f"{stem}.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(public, f, ensure_ascii=False, indent=2)
    md_path.write_text(render_markdown(report), encoding="utf-8")

    paths = {
        "json": json_path,
        "md": md_path,
        "replay_daily_candidates": out_dir / "replay_daily_candidates.csv",
        "replay_scenario_summary": out_dir / "replay_scenario_summary.csv",
        "replay_candidate_migration": out_dir / "replay_candidate_migration.csv",
        "replay_threshold_grid": out_dir / "replay_threshold_grid.csv",
        "replay_topn": out_dir / "replay_topn.csv",
        "dir": out_dir,
    }
    _write_csv(paths["replay_daily_candidates"], report.get("_export_daily_candidates") or [])
    _write_csv(paths["replay_scenario_summary"], report.get("_export_scenario_summary") or [])
    _write_csv(paths["replay_candidate_migration"], report.get("_export_migration") or [])
    _write_csv(paths["replay_threshold_grid"], report.get("threshold_grid") or [])
    _write_csv(paths["replay_topn"], report.get("_export_topn") or [])
    return paths


def run_full_universe_replay(
    *,
    market: str,
    session: str,
    start_trade_date: str,
    end_trade_date: str,
    output_dir: Path,
    config_path: Optional[Path] = None,
    train_end: str = DEFAULT_TRAIN_END,
    validation_start: str = DEFAULT_VALIDATION_START,
) -> Tuple[Dict[str, Any], Dict[str, Path]]:
    from screener_quality import load_runtime_config

    output_dir = Path(output_dir)
    cfg = load_runtime_config(config_path)
    baseline_w = production_baseline_weights(cfg)
    pipeline_params = pipeline_params_from_config(cfg)

    config_file = Path(config_path) if config_path else Path(__file__).resolve().parents[1] / "config" / "config.json"
    screener_py = Path(__file__).resolve().parent / "screener.py"
    pre_hashes: Dict[str, str] = {}
    if config_file.exists():
        pre_hashes[str(config_file)] = _sha256_file(config_file)
    if screener_py.exists():
        pre_hashes[str(screener_py)] = _sha256_file(screener_py)

    days, load_meta = load_replay_days(
        output_dir=output_dir,
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
    )
    load_meta["output_dir"] = str(output_dir)

    report = analyze_full_universe_replay(
        days,
        baseline_weights=baseline_w,
        pipeline_params=pipeline_params,
        scenarios=SCENARIOS,
        market=market,
        session=session,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
        train_end=train_end,
        validation_start=validation_start,
        load_meta=load_meta,
    )
    report["input_fingerprints_before"] = pre_hashes

    paths = write_replay_outputs(report, output_dir)

    post_hashes: Dict[str, str] = {}
    for p in pre_hashes:
        if Path(p).exists():
            post_hashes[p] = _sha256_file(Path(p))
    report["input_fingerprints_after"] = post_hashes
    report["production_inputs_unchanged"] = pre_hashes == post_hashes

    public = {k: v for k, v in report.items() if not str(k).startswith("_")}
    with open(paths["json"], "w", encoding="utf-8") as f:
        json.dump(public, f, ensure_ascii=False, indent=2)

    return report, paths


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Full-Universe Weight Replay Analyzer"
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
    parser.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    parser.add_argument("--validation-start", default=DEFAULT_VALIDATION_START)
    args = parser.parse_args(list(argv) if argv is not None else None)

    report, paths = run_full_universe_replay(
        market=str(args.market).upper(),
        session=str(args.session).lower(),
        start_trade_date=str(args.date_from),
        end_trade_date=str(args.date_to),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config) if args.config else None,
        train_end=str(args.train_end),
        validation_start=str(args.validation_start),
    )

    summary = {
        "status": report.get("status"),
        "json": str(paths["json"]),
        "md": str(paths["md"]),
        "included_days": (report.get("universe_meta") or {}).get("included_days"),
        "trust_counts_raw": (report.get("universe_meta") or {}).get("trust_counts_raw"),
        "accepted_trust_counts": (report.get("universe_meta") or {}).get("accepted_trust_counts"),
        "excluded_trust_counts": (report.get("universe_meta") or {}).get("excluded_trust_counts"),
        "artifact_integrity_counts": (report.get("universe_meta") or {}).get(
            "artifact_integrity_counts"
        ),
        "historical_market_state": report.get("market_state_summary"),
        "baseline_reconstruction": {
            "ok": (report.get("baseline_reconstruction") or {}).get("ok"),
            "n_checked": (report.get("baseline_reconstruction") or {}).get("n_checked"),
            "failure_count": (report.get("baseline_reconstruction") or {}).get("failure_count"),
            "max_abs_error": (report.get("baseline_reconstruction") or {}).get("max_abs_error"),
            "date_summary": (report.get("baseline_reconstruction") or {}).get("date_summary"),
        },
        "baseline_replay_ok": (report.get("baseline_replay") or {}).get("ok"),
        "baseline_avg_match_pct": (report.get("baseline_replay") or {}).get("avg_match_pct"),
        "production_inputs_unchanged": report.get("production_inputs_unchanged"),
        "a_vs_c": {
            "A_count": (report.get("a_vs_c") or {}).get("A_candidate_count"),
            "C_count": (report.get("a_vs_c") or {}).get("C_candidate_count"),
            "C_NEW_n": ((report.get("a_vs_c") or {}).get("C_NEW") or {}).get("n"),
        },
        "shadow_verdicts": report.get("shadow_verdicts"),
        "historical_decision": report.get("historical_decision"),
        "research_status": report.get("research_status"),
        "d_thresholds": [
            {
                "threshold": b.get("threshold"),
                "robust_improvement": b.get("robust_improvement"),
                "validation_median_5d": ((b.get("windows") or {}).get("validation") or {}).get("median_5d"),
                "validation_avg_daily": ((b.get("windows") or {}).get("validation") or {}).get(
                    "avg_daily_candidate_count"
                ),
            }
            for b in (report.get("d_threshold_diagnostics") or [])
        ],
        "warnings": report.get("warnings"),
        "recommendation": report.get("recommendation"),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    status = report.get("status")
    if status == STATUS_BASELINE_FAILED:
        return 3
    if status == STATUS_NO_DATA or not (report.get("universe_meta") or {}).get("included_days"):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
