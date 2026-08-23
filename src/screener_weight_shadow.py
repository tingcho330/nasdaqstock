"""Read-only prospective WEIGHT_SHADOW diagnostics.

Computes D_TECH_POS_DOWN @ research_threshold after Production DECISION is
finalized. Never mutates Production candidates, DECISION artifacts, trader,
GPT, orders, position sizing, or trading DB.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

logger = logging.getLogger("screener_weight_shadow")

from screener_weight_simulation import (  # noqa: E402
    FACTOR_NAMES,
    HORIZONS,
    PRODUCTION_THRESHOLD,
    RETURN_KEYS,
    SCENARIOS,
    _safe_float,
    outcome_metrics,
    production_baseline_weights,
    weight_sum,
)

DIAGNOSTIC_TYPE = "WEIGHT_SHADOW"
SHADOW_SCENARIO = "D_TECH_POS_DOWN"
RESEARCH_THRESHOLD_DEFAULT = 0.40
PROSPECTIVE_START_DEFAULT = "20260824"
PROSPECTIVE_MIN_DAYS_BEFORE_RECOMMENDATION = 20
STATUS_RESEARCH_ONLY = "RESEARCH_ONLY"

MIGRATION_KEEP = "KEEP"
MIGRATION_DROP = "DROP"
MIGRATION_NEW = "NEW"
MIGRATION_NEITHER = "NEITHER"


def weight_shadow_policy_defaults() -> Dict[str, Any]:
    return {
        "enabled": True,
        "markets": ["SP500"],
        "sessions": ["pm"],
        "scenario": SHADOW_SCENARIO,
        "research_threshold": RESEARCH_THRESHOLD_DEFAULT,
        "prospective_start_trade_date": PROSPECTIVE_START_DEFAULT,
        "used_by_trader": False,
        "diagnostic_only": True,
        "failure_policy": "WARN_AND_CONTINUE",
        "run_after_production_artifacts_saved": True,
        "min_days_before_recommendation": PROSPECTIVE_MIN_DAYS_BEFORE_RECOMMENDATION,
    }


def should_run_weight_shadow(
    policy: Optional[Dict[str, Any]],
    *,
    market: str,
    session: str,
    trade_date: str,
    run_mode: Optional[str] = None,
) -> bool:
    pol = {**weight_shadow_policy_defaults(), **(policy or {})}
    if not bool(pol.get("enabled", True)):
        return False
    if bool(pol.get("used_by_trader")):
        # Hard isolation: never allow trader consumption
        logger.error("weight_shadow_policy.used_by_trader must be false — forcing skip")
        return False
    markets = {str(m).upper() for m in (pol.get("markets") or ["SP500"])}
    sessions = {str(s).lower() for s in (pol.get("sessions") or ["pm"])}
    if str(market).upper() not in markets:
        return False
    if str(session).lower() not in sessions:
        return False
    # DECISION-only for prospective shadow (REPLAY excluded)
    if run_mode and str(run_mode).upper() not in ("DECISION", ""):
        return False
    start = str(pol.get("prospective_start_trade_date") or PROSPECTIVE_START_DEFAULT)
    if str(trade_date) < start:
        logger.info(
            "WEIGHT_SHADOW skip: trade_date=%s < prospective_start=%s (historical excluded)",
            trade_date,
            start,
        )
        return False
    return True


def _extract_factors(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    from screener_factor_analysis import extract_factor_value

    factors: Dict[str, Optional[float]] = {}
    src = row.get("factors") if isinstance(row.get("factors"), dict) else row
    for name in FACTOR_NAMES:
        factors[name] = extract_factor_value(src, name)
        if factors[name] is None:
            factors[name] = extract_factor_value(row, name)
    return factors


def _normalize_score_row(
    row: Dict[str, Any],
    *,
    trade_date: str,
    market: str,
    session: str,
    source_run_id: str,
) -> Dict[str, Any]:
    ticker = str(row.get("ticker") or row.get("Ticker") or "").upper()
    factors = _extract_factors(row)
    prod_score = _safe_float(row.get("score") if row.get("score") is not None else row.get("Score"))
    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "market": market,
        "session": session,
        "source_run_id": source_run_id,
        "factors": factors,
        "score": prod_score,
        "total_score": prod_score,
        "sector": row.get("sector") or row.get("Sector") or "N/A",
        "issuer_group": row.get("issuer_group") or row.get("IssuerGroup") or ticker,
        "held": bool(row.get("held")),
        "momentum_pass": bool(row.get("momentum_pass", True)),
        "volatility_pass": bool(row.get("volatility_pass", True)),
        "rsi": row.get("rsi") or row.get("RSI"),
        "exclusion_reasons": list(row.get("exclusion_reasons") or row.get("exclude_reasons") or []),
        "eligibility_status": row.get("eligibility_status"),
        "production_candidate": bool(row.get("production_candidate", False)),
        "price": _safe_float(row.get("price") or row.get("Price")),
    }


def classify_migration_group(
    ticker: str,
    *,
    production_candidates: Set[str],
    shadow_candidates: Set[str],
) -> str:
    bp = ticker in production_candidates
    sp = ticker in shadow_candidates
    if bp and sp:
        return MIGRATION_KEEP
    if bp and not sp:
        return MIGRATION_DROP
    if (not bp) and sp:
        return MIGRATION_NEW
    return MIGRATION_NEITHER


def build_weight_shadow_payload(
    *,
    scores_records: Sequence[Dict[str, Any]],
    production_tickers: Set[str],
    trade_date: str,
    market: str,
    session: str,
    source_run_id: str,
    market_state: Optional[Any],
    market_state_payload: Optional[Dict[str, Any]],
    baseline_weights: Dict[str, float],
    shadow_weights: Dict[str, float],
    production_threshold: float,
    research_threshold: float,
    pipeline_params: Dict[str, Any],
    prospective_start_trade_date: str,
) -> Dict[str, Any]:
    """Build weight_shadow.json body (diagnostic only)."""
    from screener_full_universe_replay import (
        build_replay_dataframe,
        classify_migration,
        replay_score,
        run_candidate_pipeline,
    )

    prod_set = {str(t).upper() for t in production_tickers if t}
    universe = [
        _normalize_score_row(
            r,
            trade_date=trade_date,
            market=market,
            session=session,
            source_run_id=source_run_id,
        )
        for r in scores_records
        if str(r.get("ticker") or r.get("Ticker") or "").strip()
    ]

    # Production scores from artifact; shadow scores recomputed
    scored_rows: List[Dict[str, Any]] = []
    for row in universe:
        factors = row.get("factors") or {}
        prod = _safe_float(row.get("score"))
        if prod is None:
            prod = replay_score(factors, baseline_weights, market_state)
        shadow = replay_score(factors, shadow_weights, market_state)
        scored_rows.append(
            {
                **row,
                "production_score": prod,
                "shadow_score": shadow,
                "baseline_score": prod,
                "scenario_score": shadow,
            }
        )

    shadow_df = build_replay_dataframe(
        scored_rows,
        weights=shadow_weights,
        threshold=float(research_threshold),
        pipeline_params=pipeline_params,
        historical_market_state=market_state,
    )
    shadow_cands_df, _stages = run_candidate_pipeline(
        shadow_df, threshold=float(research_threshold), pipeline_params=pipeline_params
    )
    shadow_tickers: Set[str] = set()
    if not shadow_cands_df.empty:
        shadow_tickers = {
            str(t).upper()
            for t in shadow_cands_df["Ticker"].astype(str).tolist()
            if t
        }

    rows_out: List[Dict[str, Any]] = []
    for row in scored_rows:
        t = str(row.get("ticker") or "").upper()
        if not t:
            continue
        prod_cand = t in prod_set
        shadow_cand = t in shadow_tickers
        # Keep all production + shadow candidates + near-threshold for diagnostics
        if not (prod_cand or shadow_cand):
            continue
        group = classify_migration(
            t, baseline_candidates=prod_set, scenario_candidates=shadow_tickers
        )
        factors = row.get("factors") or {}
        rows_out.append(
            {
                "ticker": t,
                "factor_values": {k: factors.get(k) for k in FACTOR_NAMES},
                "production_score": row.get("production_score"),
                "shadow_score": row.get("shadow_score"),
                "production_candidate": prod_cand,
                "shadow_candidate": shadow_cand,
                "migration_group": group,
            }
        )

    rows_out.sort(
        key=lambda r: (
            0 if r.get("shadow_candidate") else 1,
            -(float(r.get("shadow_score") or 0)),
            str(r.get("ticker") or ""),
        )
    )

    return {
        "schema_version": 1,
        "diagnostic_type": DIAGNOSTIC_TYPE,
        "diagnostic_only": True,
        "used_by_trader": False,
        "trade_date": trade_date,
        "market": str(market).upper(),
        "session": str(session).lower(),
        "source_run_id": source_run_id,
        "prospective_start_trade_date": prospective_start_trade_date,
        "historical_runs_excluded": True,
        "production_policy": {
            "scenario": "A_BASELINE",
            "threshold": float(production_threshold),
            "weights": dict(baseline_weights),
            "weight_sum": weight_sum(baseline_weights),
        },
        "shadow_policy": {
            "scenario": SHADOW_SCENARIO,
            "threshold": float(research_threshold),
            "weights": dict(shadow_weights),
            "weight_sum": weight_sum(shadow_weights),
        },
        "market_state": market_state_payload,
        "counts": {
            "universe_rows": len(universe),
            "production_candidates": len(prod_set),
            "shadow_candidates": len(shadow_tickers),
            "rows_exported": len(rows_out),
            "KEEP": sum(1 for r in rows_out if r.get("migration_group") == MIGRATION_KEEP),
            "DROP": sum(1 for r in rows_out if r.get("migration_group") == MIGRATION_DROP),
            "NEW": sum(1 for r in rows_out if r.get("migration_group") == MIGRATION_NEW),
        },
        "rows": rows_out,
        "status": "OK",
        "research_status": STATUS_RESEARCH_ONLY,
        "production_change_recommended": False,
    }


def publish_weight_shadow_only(
    *,
    output_dir: Path,
    market: str,
    trade_date: str,
    session: str,
    source_run_id: str,
    source_decision_manifest_sha256: Optional[str],
    payload: Dict[str, Any],
) -> Path:
    """Publish diagnostics dir containing only weight_shadow.json."""
    from screener_artifacts import PostRunDiagnosticsWriter
    from datetime import timezone

    try:
        from utils import KST
    except Exception:
        KST = timezone.utc  # type: ignore

    started = datetime.now(KST).isoformat()
    writer = PostRunDiagnosticsWriter(
        output_dir=Path(output_dir),
        market=str(market).upper(),
        trade_date=str(trade_date),
        session=str(session).lower(),
        source_run_id=source_run_id,
        source_decision_manifest_sha256=source_decision_manifest_sha256,
        started_at_kst=started,
    )
    writer.write_json("weight_shadow.json", payload)
    completed = datetime.now(KST).isoformat()
    man = writer.build_manifest(
        status=str(payload.get("status") or "OK"),
        completed_at_kst=completed,
        diagnostic_types=[DIAGNOSTIC_TYPE],
    )
    return writer.publish(man)


def write_weight_shadow_into_diagnostics(
    diag_writer: Any,
    payload: Dict[str, Any],
) -> None:
    """Write weight_shadow.json into an open (unpublished) PostRunDiagnosticsWriter."""
    diag_writer.write_json("weight_shadow.json", payload)


def run_weight_shadow_post_diagnostics(
    *,
    writer: Any,
    policy: Dict[str, Any],
    scores_records: Sequence[Dict[str, Any]],
    production_tickers: Set[str],
    trade_date: str,
    market: str,
    session: str,
    screener_params: Dict[str, Any],
    market_state: Optional[Any],
    market_state_payload: Optional[Dict[str, Any]],
    baseline_weights: Optional[Dict[str, float]] = None,
    existing_diag_writer: Any = None,
) -> Dict[str, Any]:
    """Compute and publish WEIGHT_SHADOW. Never touches DECISION dir."""
    from screener_artifacts import sha256_file
    from screener_full_universe_replay import pipeline_params_from_config

    pol = {**weight_shadow_policy_defaults(), **(policy or {})}
    meta: Dict[str, Any] = {
        "enabled": True,
        "diagnostic_type": DIAGNOSTIC_TYPE,
        "used_by_trader": False,
        "diagnostic_only": True,
        "status": "PENDING",
    }
    if writer is None or not getattr(writer, "published", False):
        meta["status"] = "SKIPPED"
        meta["errors"] = ["requires published DECISION run"]
        return meta

    try:
        cfg = {"screener_params": screener_params or {}}
        baseline_w = baseline_weights or production_baseline_weights(cfg)
        shadow_w = dict(SCENARIOS[SHADOW_SCENARIO])
        research_thr = float(pol.get("research_threshold") or RESEARCH_THRESHOLD_DEFAULT)
        prod_thr = float(
            (screener_params or {}).get("min_score_threshold") or PRODUCTION_THRESHOLD
        )
        pipeline_params = pipeline_params_from_config(cfg)
        prospective_start = str(
            pol.get("prospective_start_trade_date") or PROSPECTIVE_START_DEFAULT
        )

        payload = build_weight_shadow_payload(
            scores_records=scores_records,
            production_tickers=production_tickers,
            trade_date=trade_date,
            market=market,
            session=session,
            source_run_id=writer.run_id,
            market_state=market_state,
            market_state_payload=market_state_payload,
            baseline_weights=baseline_w,
            shadow_weights=shadow_w,
            production_threshold=prod_thr,
            research_threshold=research_thr,
            pipeline_params=pipeline_params,
            prospective_start_trade_date=prospective_start,
        )

        source_manifest_sha = None
        man_path = Path(writer.final_dir) / "manifest.json"
        if man_path.exists():
            source_manifest_sha = sha256_file(man_path)

        if existing_diag_writer is not None and not getattr(
            existing_diag_writer, "published", True
        ):
            write_weight_shadow_into_diagnostics(existing_diag_writer, payload)
            meta["status"] = "OK"
            meta["published_with"] = "shared_diagnostics_writer"
        else:
            # Prefer appending into existing liquidity diagnostics dir if present
            from screener_artifacts import post_run_diagnostics_root

            diag_dir = (
                post_run_diagnostics_root(writer.output_dir)
                / str(market).upper()
                / str(trade_date)
                / str(session).lower()
                / writer.run_id
            )
            if diag_dir.is_dir() and not (diag_dir / "weight_shadow.json").exists():
                # Diagnostics already published (immutable). Write via atomic add +
                # manifest patch only for weight_shadow.json (DECISION untouched).
                _append_weight_shadow_to_published_diag(diag_dir, payload)
                meta["status"] = "OK"
                meta["diagnostics_directory"] = str(diag_dir)
            elif diag_dir.is_dir() and (diag_dir / "weight_shadow.json").exists():
                meta["status"] = "ALREADY_EXISTS"
                meta["diagnostics_directory"] = str(diag_dir)
            else:
                published = publish_weight_shadow_only(
                    output_dir=writer.output_dir,
                    market=market,
                    trade_date=trade_date,
                    session=session,
                    source_run_id=writer.run_id,
                    source_decision_manifest_sha256=source_manifest_sha,
                    payload=payload,
                )
                meta["status"] = "OK"
                meta["diagnostics_directory"] = str(published)

        meta["counts"] = payload.get("counts")
        meta["shadow_policy"] = payload.get("shadow_policy")
        meta["production_policy"] = payload.get("production_policy")
        meta["research_status"] = STATUS_RESEARCH_ONLY
        meta["production_change_recommended"] = False
        return meta
    except Exception as e:
        logger.warning("WEIGHT_SHADOW failed (Production unchanged): %s", e)
        meta["status"] = "FAILED"
        meta["errors"] = [str(e)]
        meta["used_by_trader"] = False
        return meta


def _append_weight_shadow_to_published_diag(diag_dir: Path, payload: Dict[str, Any]) -> None:
    """Atomically add weight_shadow.json to an existing post_run_diagnostics dir.

    Does not touch DECISION. Updates diagnostics_manifest artifact map when present.
    """
    from screener_artifacts import atomic_write_json, sha256_file

    target = Path(diag_dir) / "weight_shadow.json"
    if target.exists():
        return
    atomic_write_json(target, payload)
    man_path = Path(diag_dir) / "diagnostics_manifest.json"
    if man_path.exists():
        try:
            man = json.loads(man_path.read_text(encoding="utf-8"))
            arts = dict(man.get("artifacts") or {})
            arts["weight_shadow.json"] = {
                "sha256": sha256_file(target),
                "row_count": len(payload.get("rows") or []),
            }
            man["artifacts"] = arts
            types = list(man.get("diagnostic_types") or [])
            if DIAGNOSTIC_TYPE not in types:
                types.append(DIAGNOSTIC_TYPE)
            man["diagnostic_types"] = types
            man["used_by_trader"] = False
            atomic_write_json(man_path, man)
        except Exception as e:
            logger.warning("WEIGHT_SHADOW manifest patch failed (artifact written): %s", e)


# ─── Prospective quality aggregation ─────────────────────────────────────────


def discover_weight_shadow_artifacts(
    output_dir: Path,
    *,
    market: str = "SP500",
    session: str = "pm",
    prospective_start_trade_date: str = PROSPECTIVE_START_DEFAULT,
) -> List[Dict[str, Any]]:
    from screener_artifacts import post_run_diagnostics_root

    root = post_run_diagnostics_root(output_dir) / str(market).upper()
    if not root.is_dir():
        return []
    found: List[Dict[str, Any]] = []
    for date_dir in sorted(root.iterdir()):
        if not date_dir.is_dir():
            continue
        trade_date = date_dir.name
        if trade_date < prospective_start_trade_date:
            continue
        sess_dir = date_dir / str(session).lower()
        if not sess_dir.is_dir():
            continue
        for run_dir in sorted(sess_dir.iterdir()):
            shadow_path = run_dir / "weight_shadow.json"
            if not shadow_path.exists():
                continue
            try:
                payload = json.loads(shadow_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            found.append(
                {
                    "trade_date": trade_date,
                    "path": str(shadow_path),
                    "payload": payload,
                    "source_run_id": payload.get("source_run_id") or run_dir.name,
                }
            )
    return found


def _settle_shadow_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    trade_date: str,
    as_of_trade_date: str,
) -> List[Dict[str, Any]]:
    from screener_outcomes import settle_observation_outcome

    out: List[Dict[str, Any]] = []
    for r in rows:
        stub = {
            "ticker": r.get("ticker"),
            "trade_date": trade_date,
            "reference_price": None,
            "decision_price": None,
        }
        settled = settle_observation_outcome(stub, as_of_trade_date=as_of_trade_date)
        merged = dict(r)
        for h in HORIZONS:
            hk = RETURN_KEYS[h]
            if settled.get(hk) is not None:
                merged[hk] = _safe_float(settled.get(hk))
        for ek in ("max_drawdown_5d_pct", "max_runup_5d_pct"):
            if settled.get(ek) is not None:
                merged[ek] = _safe_float(settled.get(ek))
        out.append(merged)
    return out


def _migration_metrics(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for group in (MIGRATION_KEEP, MIGRATION_DROP, MIGRATION_NEW):
        subset = [r for r in rows if r.get("migration_group") == group]
        m = outcome_metrics(subset, score_key="shadow_score")
        out[group] = {
            "n": len(subset),
            "unique_tickers": m.get("unique_tickers"),
            "mean_5d": m.get("mean_5d"),
            "median_5d": m.get("median_5d"),
            "win_rate": m.get("win_rate"),
            "p25": m.get("p25"),
            "p75": m.get("p75"),
        }
    return out


def build_prospective_weight_shadow_quality(
    *,
    output_dir: Path,
    market: str = "SP500",
    session: str = "pm",
    prospective_start_trade_date: str = PROSPECTIVE_START_DEFAULT,
    as_of_trade_date: Optional[str] = None,
    min_days_before_recommendation: int = PROSPECTIVE_MIN_DAYS_BEFORE_RECOMMENDATION,
) -> Dict[str, Any]:
    """Aggregate prospective WEIGHT_SHADOW outcomes. No Production recommendation <20 days."""
    as_of = as_of_trade_date or datetime.now().strftime("%Y%m%d")
    artifacts = discover_weight_shadow_artifacts(
        output_dir,
        market=market,
        session=session,
        prospective_start_trade_date=prospective_start_trade_date,
    )
    prod_rows: List[Dict[str, Any]] = []
    shadow_rows: List[Dict[str, Any]] = []
    migration_rows: List[Dict[str, Any]] = []
    by_regime: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(
        lambda: {"A": [], "D": []}
    )
    days_observed = 0

    for art in artifacts:
        payload = art.get("payload") or {}
        td = str(art.get("trade_date") or payload.get("trade_date") or "")
        if td < prospective_start_trade_date:
            continue
        days_observed += 1
        rows = _settle_shadow_rows(
            payload.get("rows") or [],
            trade_date=td,
            as_of_trade_date=as_of,
        )
        regime = None
        ms = payload.get("market_state") or {}
        if isinstance(ms, dict):
            regime = str(ms.get("regime") or ms.get("trend") or "UNKNOWN").lower()
        else:
            regime = "UNKNOWN"
        for r in rows:
            r = {**r, "trade_date": td, "regime": regime}
            if r.get("production_candidate"):
                prod_rows.append({**r, "scenario_score": r.get("production_score")})
                by_regime[regime]["A"].append(r)
            if r.get("shadow_candidate"):
                shadow_rows.append({**r, "scenario_score": r.get("shadow_score")})
                by_regime[regime]["D"].append(r)
            if r.get("migration_group") in (MIGRATION_KEEP, MIGRATION_DROP, MIGRATION_NEW):
                migration_rows.append(r)

    def _mature_counts(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for h in HORIZONS:
            hk = RETURN_KEYS[h]
            out[f"mature_{h}d"] = sum(1 for r in rows if r.get(hk) is not None)
        return out

    a_metrics = outcome_metrics(prod_rows, score_key="production_score")
    d_metrics = outcome_metrics(shadow_rows, score_key="shadow_score")
    regime_split: Dict[str, Any] = {}
    for reg, sides in by_regime.items():
        a_m = outcome_metrics(sides["A"], score_key="production_score")
        d_m = outcome_metrics(sides["D"], score_key="shadow_score")
        low = int(a_m.get("observations") or 0) < 5 or int(d_m.get("observations") or 0) < 5
        regime_split[reg] = {
            "A_BASELINE": a_m,
            "D_TECH_POS_DOWN": d_m,
            "LOW_SAMPLE": low,
        }

    outlier_warnings: List[str] = []
    if a_metrics.get("mean_5d") is not None and a_metrics.get("median_5d") is not None:
        if float(a_metrics["mean_5d"]) - float(a_metrics["median_5d"]) >= 2.0:
            outlier_warnings.append("A_MEAN_MEDIAN_GAP")
    if d_metrics.get("mean_5d") is not None and d_metrics.get("median_5d") is not None:
        if float(d_metrics["mean_5d"]) - float(d_metrics["median_5d"]) >= 2.0:
            outlier_warnings.append("D_MEAN_MEDIAN_GAP")

    recommend_allowed = days_observed >= int(min_days_before_recommendation)

    report = {
        "schema_version": 1,
        "diagnostic_type": DIAGNOSTIC_TYPE,
        "research_status": STATUS_RESEARCH_ONLY,
        "market": str(market).upper(),
        "session": str(session).lower(),
        "prospective_start_trade_date": prospective_start_trade_date,
        "as_of_trade_date": as_of,
        "days_observed": days_observed,
        "min_days_before_recommendation": int(min_days_before_recommendation),
        "recommendation_gate_open": recommend_allowed,
        "production_change_recommended": False,
        "note": (
            "Prospective WEIGHT_SHADOW only — historical runs excluded. "
            "Production remains A_BASELINE. No Production change before "
            f"{min_days_before_recommendation} prospective trading days."
            if not recommend_allowed
            else "Prospective sample meets day gate; still RESEARCH_ONLY — no auto Production change."
        ),
        "mature_counts": {
            "A": _mature_counts(prod_rows),
            "D": _mature_counts(shadow_rows),
        },
        "A_vs_D": {
            "A_BASELINE": a_metrics,
            "D_TECH_POS_DOWN": d_metrics,
        },
        "migration": _migration_metrics(migration_rows),
        "regime_split": regime_split,
        "ticker_equal_weight": {
            "A": a_metrics.get("ticker_equal_weight_mean_5d"),
            "D": d_metrics.get("ticker_equal_weight_mean_5d"),
        },
        "first_signal": {
            "A": {
                "mean_5d": a_metrics.get("first_signal_mean_5d"),
                "median_5d": a_metrics.get("first_signal_median_5d"),
                "win_rate": a_metrics.get("first_signal_win_rate"),
            },
            "D": {
                "mean_5d": d_metrics.get("first_signal_mean_5d"),
                "median_5d": d_metrics.get("first_signal_median_5d"),
                "win_rate": d_metrics.get("first_signal_win_rate"),
            },
        },
        "outlier_warnings": outlier_warnings,
        "used_by_trader": False,
        "diagnostic_only": True,
    }
    return report


def render_prospective_quality_md(report: Dict[str, Any]) -> str:
    lines = [
        "# Prospective WEIGHT_SHADOW Quality",
        "",
        f"- Market/session: `{report.get('market')}` `{report.get('session')}`",
        f"- Prospective start: `{report.get('prospective_start_trade_date')}`",
        f"- Days observed: {report.get('days_observed')}",
        f"- Research status: `{report.get('research_status')}`",
        f"- Production change recommended: `{report.get('production_change_recommended')}`",
        f"- Recommendation gate open (≥{report.get('min_days_before_recommendation')}d): "
        f"`{report.get('recommendation_gate_open')}`",
        "",
        "## A vs D",
    ]
    avd = report.get("A_vs_D") or {}
    for name in ("A_BASELINE", "D_TECH_POS_DOWN"):
        m = avd.get(name) or {}
        lines.append(
            f"- **{name}**: n={m.get('observations')} med5d={m.get('median_5d')} "
            f"tew={m.get('ticker_equal_weight_mean_5d')} wr={m.get('win_rate')}"
        )
    lines.append("")
    lines.append("## Migration")
    for g, m in (report.get("migration") or {}).items():
        lines.append(
            f"- **{g}**: n={m.get('n')} med5d={m.get('median_5d')} wr={m.get('win_rate')}"
        )
    lines.append("")
    lines.append("## Regime split")
    for reg, block in (report.get("regime_split") or {}).items():
        flag = " LOW_SAMPLE" if block.get("LOW_SAMPLE") else ""
        lines.append(f"- **{reg}**{flag}")
    lines.append("")
    if report.get("outlier_warnings"):
        lines.append("## Outlier warnings")
        for w in report["outlier_warnings"]:
            lines.append(f"- {w}")
        lines.append("")
    lines.append(str(report.get("note") or ""))
    return "\n".join(lines) + "\n"


def write_prospective_quality_report(
    report: Dict[str, Any],
    output_dir: Path,
) -> Dict[str, Path]:
    out = Path(output_dir) / "quality" / "weight_shadow"
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "prospective_weight_shadow_quality.json"
    md_path = out / "prospective_weight_shadow_quality.md"
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_prospective_quality_md(report), encoding="utf-8")
    return {"json": json_path, "md": md_path, "dir": out}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Prospective WEIGHT_SHADOW quality")
    parser.add_argument("--market", default="SP500")
    parser.add_argument("--session", default="pm")
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", str(Path(__file__).resolve().parents[1] / "output")),
    )
    parser.add_argument(
        "--prospective-start",
        default=PROSPECTIVE_START_DEFAULT,
    )
    parser.add_argument("--as-of", default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    report = build_prospective_weight_shadow_quality(
        output_dir=Path(args.output_dir),
        market=str(args.market).upper(),
        session=str(args.session).lower(),
        prospective_start_trade_date=str(args.prospective_start),
        as_of_trade_date=args.as_of,
    )
    paths = write_prospective_quality_report(report, Path(args.output_dir))
    print(
        json.dumps(
            {
                "days_observed": report.get("days_observed"),
                "research_status": report.get("research_status"),
                "production_change_recommended": report.get("production_change_recommended"),
                "json": str(paths["json"]),
                "md": str(paths["md"]),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
