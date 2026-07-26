"""Tests for screener observability: funnel split, shadows, diagnostics, quality."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_diagnostics import (  # noqa: E402
    annotate_stage_outcomes,
    classify_empty_result_v2,
    compute_eligible_shadow,
    compute_exclusion_summary,
    compute_liquidity_shadow_universe,
    compute_market_regime_shadow,
    compute_price_diagnostics,
    compute_stage_drop_summary,
    evaluate_diagnostic_flags,
    smooth_above_ma50_score,
)
from screener_ops import (  # noqa: E402
    FunnelRecorder,
    StageResult,
    classify_empty_result,
    enrich_scored_dataframe,
    extract_pipeline_pass_sets,
    scores_records_for_export,
    select_candidates_pipeline,
)
from screener_quality import (  # noqa: E402
    INSUFFICIENT_SAMPLE,
    aggregate_quality_report,
    observation_key,
    upsert_observation_ledger,
)
from screener_artifacts import (  # noqa: E402
    get_git_commit,
    resolve_build_identity,
    validate_decision_artifacts_for_trader,
)


def _diversify_passthrough(df, top_n, sector_cap):
    return df.head(top_n)


def _row(
    ticker: str,
    score: float,
    *,
    held: bool = False,
    rsi: float = 50.0,
    reasons: List[str] | None = None,
    sector: str = "Tech",
    issuer: str | None = None,
    fin: float = 0.5,
    tech: float = 0.5,
) -> Dict[str, Any]:
    return {
        "Ticker": ticker,
        "Name": ticker,
        "Sector": sector,
        "Score": score,
        "FinScore": fin,
        "TechScore": tech,
        "MktScore": 0.5,
        "SectorScore": 0.5,
        "PatternScore": 0.2,
        "VolKki": 0.1,
        "Pos52w": 0.7,
        "RSI": rsi,
        "ATR": 1.0,
        "MA50": 100.0,
        "MA200": 90.0,
        "PER": 20,
        "PBR": 2,
        "Price": 100.0,
        "exclude_reasons": list(reasons or []),
        "issuer_group": issuer or ticker,
        "momentum_pass": True,
        "volatility_pass": True,
    }


def _enrich(rows: List[Dict[str, Any]], held: set | None = None, thr: float = 0.48):
    df = pd.DataFrame(rows)
    return enrich_scored_dataframe(
        df,
        held_tickers=held or set(),
        issuer_map={"GOOG": "ALPHABET", "GOOGL": "ALPHABET", "AAPL": "APPLE"},
        production_threshold=thr,
    )


# ── A. Funnel ───────────────────────────────────────────────────────

def test_funnel_eligibility_issuer_sector_chain():
    scored = _enrich(
        [
            _row("MU", 0.50),
            _row("AMD", 0.49),
            _row("GOOGL", 0.485),
            _row("GOOG", 0.484),
            _row("LOW", 0.40),
        ]
    )
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    by = {s.stage: s for s in stages}
    assert list(by) == [
        "MIN_SCORE",
        "MOMENTUM",
        "VOLATILITY",
        "ELIGIBILITY",
        "ISSUER_DEDUP",
        "SECTOR_DIVERSIFICATION",
    ]
    assert by["MIN_SCORE"].output_count == 4
    assert by["ELIGIBILITY"].output_count == 4
    assert by["ISSUER_DEDUP"].output_count == 3  # GOOG dropped
    assert by["MIN_SCORE"].input_count == by["MIN_SCORE"].output_count + by["MIN_SCORE"].dropped_count
    for i in range(1, len(stages)):
        assert stages[i].input_count == stages[i - 1].output_count
    assert "GOOG" not in set(out["Ticker"])


def test_already_held_drops_in_eligibility_not_sector():
    scored = _enrich(
        [_row("AAPL", 0.51, rsi=88.0), _row("MU", 0.50)],
        held={"AAPL"},
    )
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    by = {s.stage: s for s in stages}
    assert by["MIN_SCORE"].output_count == 2
    assert by["ELIGIBILITY"].output_count == 1
    assert by["ELIGIBILITY"].dropped_count == 1
    assert by["SECTOR_DIVERSIFICATION"].dropped_count == 0
    assert list(out["Ticker"]) == ["MU"]


def test_issuer_duplicate_in_issuer_dedup_stage():
    scored = _enrich(
        [_row("GOOGL", 0.50), _row("GOOG", 0.49), _row("MU", 0.485)],
    )
    _, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    by = {s.stage: s for s in stages}
    assert by["ISSUER_DEDUP"].dropped_count == 1
    assert by["ISSUER_DEDUP"].extra.get("dropped_tickers") == ["GOOG"]


def test_sector_cap_drop_in_sector_stage():
    def diversify_cap(df, top_n, sector_cap):
        # Keep only first ticker → force sector/capacity drop of rest
        return df.head(1)

    scored = _enrich(
        [_row("MU", 0.50, sector="Tech"), _row("AMD", 0.49, sector="Tech"), _row("JPM", 0.485, sector="Fin")],
    )
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=diversify_cap,
    )
    by = {s.stage: s for s in stages}
    assert by["ELIGIBILITY"].output_count == 3
    assert by["ISSUER_DEDUP"].output_count == 3
    assert by["SECTOR_DIVERSIFICATION"].output_count == 1
    assert by["SECTOR_DIVERSIFICATION"].dropped_count == 2
    assert list(out["Ticker"]) == ["MU"]


def test_zero_input_downstream_not_run():
    scored = _enrich([_row(f"T{i}", 0.40) for i in range(5)])
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    assert len(out) == 0
    by = {s.stage: s for s in stages}
    assert by["MIN_SCORE"].output_count == 0
    for name in ("MOMENTUM", "VOLATILITY", "ELIGIBILITY", "ISSUER_DEDUP", "SECTOR_DIVERSIFICATION"):
        assert by[name].status == "NOT_RUN"
        assert by[name].input_count == 0
        assert by[name].output_count == 0
        assert by[name].dropped_count >= 0


def test_exclusion_vs_stage_drop_summary_distinct():
    scored = _enrich(
        [
            _row("AAPL", 0.51, rsi=88.0, reasons=["UP_STREAK"]),
            _row("AMD", 0.50),
            _row("MU", 0.40),
        ],
        held={"AAPL"},
    )
    _, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    ex = compute_exclusion_summary(scored)
    drops = compute_stage_drop_summary(stages)
    assert ex["ALREADY_HELD"] >= 1
    assert ex["RSI_OVERHEATED"] >= 1
    assert drops["MIN_SCORE"] == 1
    assert drops["ELIGIBILITY"] == 1
    # Multi-reason ticker counted in each exclusion bucket, but only once in stage drop
    assert drops["ELIGIBILITY"] != ex["ALREADY_HELD"] + ex["RSI_OVERHEATED"]


# ── B. Empty reason ─────────────────────────────────────────────────

def test_empty_min_score_threshold_not_met():
    stages = [
        StageResult("MIN_SCORE", "APPLIED", 13, 0, threshold=0.48),
        StageResult("MOMENTUM", "NOT_RUN", 0, 0, reason="NO_INPUT"),
        StageResult("VOLATILITY", "NOT_RUN", 0, 0, reason="NO_INPUT"),
        StageResult("ELIGIBILITY", "NOT_RUN", 0, 0, reason="NO_INPUT"),
        StageResult("ISSUER_DEDUP", "NOT_RUN", 0, 0, reason="NO_INPUT"),
        StageResult("SECTOR_DIVERSIFICATION", "NOT_RUN", 0, 0, reason="NO_INPUT"),
    ]
    status, rs, reason, detail = classify_empty_result_v2(
        candidate_count=0,
        scored_count=13,
        universe_count=501,
        amount5d_pass=13,
        scoring_failures_all=False,
        data_quality_codes=[],
        funnel_stages=stages,
    )
    assert status == "SUCCESS"
    assert rs == "EMPTY_VALID"
    assert reason == "MIN_SCORE_THRESHOLD_NOT_MET"


def test_empty_all_threshold_passers_already_held():
    scored = _enrich(
        [_row("AAPL", 0.51), _row("AMD", 0.50)],
        held={"AAPL", "AMD"},
    )
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    assert len(out) == 0
    thr = scored[scored["Score"] >= 0.48]
    status, rs, reason, detail = classify_empty_result_v2(
        candidate_count=0,
        scored_count=2,
        universe_count=501,
        amount5d_pass=2,
        scoring_failures_all=False,
        data_quality_codes=[],
        funnel_stages=stages,
        threshold_pass_rows=thr,
    )
    assert rs == "EMPTY_VALID"
    assert reason == "ALL_THRESHOLD_PASSERS_ALREADY_HELD"
    assert detail["already_held_count"] == 2


def test_empty_all_threshold_passers_ineligible():
    scored = _enrich([_row("ZZZ", 0.50, rsi=90.0)])
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    thr = scored[scored["Score"] >= 0.48]
    _, rs, reason, _ = classify_empty_result_v2(
        candidate_count=0,
        scored_count=1,
        universe_count=10,
        amount5d_pass=1,
        scoring_failures_all=False,
        data_quality_codes=[],
        funnel_stages=stages,
        threshold_pass_rows=thr,
    )
    assert rs == "EMPTY_VALID"
    assert reason == "ALL_THRESHOLD_PASSERS_INELIGIBLE"


def test_empty_issuer_dedup_and_sector_cap():
    # Issuer: two Alphabet only → after eligibility both pass, issuer leaves 1? 
    # For ALL removed by issuer: need two same issuer and diversify empty somehow.
    # Use two tickers same issuer; after issuer dedupe 1 remains — not 0.
    # For issuer-all-removed we need eligibility passers that all get removed —
    # impossible with keep-first unless eligibility empty.
    # Simulate stages directly.
    stages = [
        StageResult("MIN_SCORE", "APPLIED", 2, 2, threshold=0.48),
        StageResult("MOMENTUM", "SKIPPED", 2, 2, reason="DISABLED_IN_CONFIG"),
        StageResult("VOLATILITY", "SKIPPED", 2, 2, reason="DISABLED_IN_CONFIG"),
        StageResult("ELIGIBILITY", "APPLIED", 2, 2),
        StageResult("ISSUER_DEDUP", "APPLIED", 2, 0),
        StageResult("SECTOR_DIVERSIFICATION", "NOT_RUN", 0, 0, reason="NO_INPUT"),
    ]
    _, rs, reason, _ = classify_empty_result_v2(
        candidate_count=0,
        scored_count=2,
        universe_count=10,
        amount5d_pass=2,
        scoring_failures_all=False,
        data_quality_codes=[],
        funnel_stages=stages,
    )
    assert reason == "ALL_ELIGIBLE_REMOVED_BY_ISSUER_DEDUP"

    stages2 = [
        StageResult("MIN_SCORE", "APPLIED", 2, 2, threshold=0.48),
        StageResult("MOMENTUM", "SKIPPED", 2, 2, reason="DISABLED_IN_CONFIG"),
        StageResult("VOLATILITY", "SKIPPED", 2, 2, reason="DISABLED_IN_CONFIG"),
        StageResult("ELIGIBILITY", "APPLIED", 2, 2),
        StageResult("ISSUER_DEDUP", "APPLIED", 2, 2),
        StageResult("SECTOR_DIVERSIFICATION", "APPLIED", 2, 0),
    ]
    _, _, reason2, _ = classify_empty_result_v2(
        candidate_count=0,
        scored_count=2,
        universe_count=10,
        amount5d_pass=2,
        scoring_failures_all=False,
        data_quality_codes=[],
        funnel_stages=stages2,
    )
    assert reason2 == "ALL_ELIGIBLE_REMOVED_BY_SECTOR_CAP"


def test_empty_data_quality_vs_valid():
    _, rs, reason, _ = classify_empty_result_v2(
        candidate_count=0,
        scored_count=0,
        universe_count=501,
        amount5d_pass=16,
        scoring_failures_all=True,
        data_quality_codes=["SCORING_FAILURE_ALL"],
        funnel_stages=[],
    )
    assert rs == "EMPTY_DATA_QUALITY"
    assert reason == "SCORING_FAILED"


# ── C. Eligible Shadow ──────────────────────────────────────────────

def test_eligible_shadow_excludes_held_and_includes_mu_fixture():
    scored = _enrich(
        [
            _row("AAPL", 0.5097, rsi=75.0),
            _row("AMD", 0.4920),
            _row("MU", 0.4574),
            _row("T1", 0.41),
            _row("T2", 0.40),
            _row("T3", 0.39),
            _row("T4", 0.38),
            _row("T5", 0.37),
            _row("T6", 0.36),
            _row("T7", 0.35),
            _row("T8", 0.34),
            _row("T9", 0.33),
            _row("T10", 0.32),
        ],
        held={"AAPL", "AMD"},
    )
    # Production candidates empty (threshold 0.48 all held)
    cands, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    assert len(cands) == 0
    by = {s.stage: s for s in stages}
    assert by["MIN_SCORE"].output_count == 2
    assert by["ELIGIBILITY"].output_count == 0
    assert by["ISSUER_DEDUP"].status == "NOT_RUN"
    assert by["SECTOR_DIVERSIFICATION"].status == "NOT_RUN"

    shadow_df, meta = compute_eligible_shadow(
        scored,
        policy={
            "enabled": True,
            "mode": "hybrid_percentile",
            "floor": 0.42,
            "percentile": 0.90,
            "max_candidates": 5,
            "min_population": 5,
            "exclude_production_candidates": True,
            "used_by_trader": False,
        },
        production_tickers=set(),
        diversify_fn=_diversify_passthrough,
    )
    assert meta["used_by_trader"] is False
    assert "AAPL" not in set(shadow_df["Ticker"])
    assert "AMD" not in set(shadow_df["Ticker"])
    assert "MU" in set(shadow_df["Ticker"])
    assert meta["threshold"] >= 0.42


def test_eligible_shadow_floor_fallback_small_population():
    scored = _enrich([_row("MU", 0.45), _row("X", 0.43), _row("Y", 0.41)])
    _, meta = compute_eligible_shadow(
        scored,
        policy={
            "enabled": True,
            "floor": 0.42,
            "percentile": 0.90,
            "min_population": 5,
            "max_candidates": 5,
        },
        diversify_fn=_diversify_passthrough,
    )
    assert meta["threshold_mode"] == "floor_only_insufficient_population"
    assert meta["threshold"] == 0.42


# ── D. Liquidity Shadow ─────────────────────────────────────────────

def test_liquidity_shadow_universe_p90_and_max():
    amounts = {f"T{i}": float(i + 1) * 1e8 for i in range(100)}
    # Add production-pass names
    amounts["MU"] = 6e9
    tickers, meta = compute_liquidity_shadow_universe(
        amounts,
        policy={"enabled": True, "threshold_mode": "percentile", "percentile": 0.90, "max_universe": 20},
        production_threshold=5e9,
    )
    assert meta["production_liquidity_threshold"] == 5e9
    assert meta["universe_count"] <= 20
    assert meta["used_by_trader"] is False
    assert len(tickers) == meta["universe_count"]


def test_trader_rejects_new_shadow_names(tmp_path):
    cands = tmp_path / "screener_eligible_shadow_candidates_20260724_pm_SP500.json"
    cands.write_text("[]", encoding="utf-8")
    ok, msg, _ = validate_decision_artifacts_for_trader(
        trade_date="20260724",
        market="SP500",
        session="pm",
        candidates_path=cands,
        output_dir=tmp_path,
    )
    assert ok is False
    assert "not trader" in msg.lower() or "shadow" in msg.lower()


# ── E. Diagnostics ──────────────────────────────────────────────────

def test_high_tech_low_fin_and_missing_vs_zero():
    flags = evaluate_diagnostic_flags(
        financial_score=0.0,
        technical_score=0.95,
        return_1d_pct=None,
        return_3d_pct=None,
        return_5d_pct=None,
        price_vs_ma50_pct=None,
        atr_14_pct=None,
        gap_pct=None,
        rsi=50,
    )
    assert "HIGH_TECH_LOW_FIN" in flags
    flags_missing = evaluate_diagnostic_flags(
        financial_score=None,
        technical_score=0.95,
        return_1d_pct=None,
        return_3d_pct=None,
        return_5d_pct=None,
        price_vs_ma50_pct=None,
        atr_14_pct=None,
        gap_pct=None,
        rsi=50,
    )
    assert "HIGH_TECH_LOW_FIN" not in flags_missing


def test_price_diagnostics_returns_and_nulls():
    d = compute_price_diagnostics([100, 102, 101, 99, 98, 97], opens=[100, 102, 101, 99, 98, 96], ma50=100, ma200=90, atr_14=2.0)
    assert d["return_1d_pct"] is not None
    assert d["return_5d_pct"] is not None
    assert d["atr_14_pct"] is not None
    assert d["gap_pct"] is not None
    empty = compute_price_diagnostics([])
    assert empty["return_1d_pct"] is None


# ── F. Regime Shadow ────────────────────────────────────────────────

def test_smooth_regime_shadow_bounds_and_production_unchanged():
    assert 0.0 <= smooth_above_ma50_score(0.0) <= 1.0
    assert smooth_above_ma50_score(10.0, transition_band_pct=2.0) == 1.0
    assert smooth_above_ma50_score(-10.0, transition_band_pct=2.0) == 0.0
    payload = compute_market_regime_shadow(
        index_close=101.0,
        index_ma50=100.0,
        production_above_ma50_binary=1.0,
        ma50_gt_ma200=1.0,
        rsi_term=0.8,
        production_weighted_regime_score=0.9,
        scoring_market_component=0.78,
        transition_band_pct=2.0,
    )
    assert payload["used_in_production_score"] is False
    assert payload["production_above_ma50_binary"] == 1.0
    assert 0.0 <= payload["smooth_above_ma50_score"] <= 1.0


# ── G. Quality Report ───────────────────────────────────────────────

def test_quality_report_decision_only_and_insufficient_sample(tmp_path):
    # Real writer uses lowercase "decision"
    base = tmp_path / "runs" / "decision" / "SP500" / "20260724" / "pm" / "run1"
    base.mkdir(parents=True)
    (base / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_mode": "DECISION",
                "decision_artifact": True,
                "market": "SP500",
                "session": "pm",
                "trade_date": "20260724",
                "status": "SUCCESS",
            }
        ),
        encoding="utf-8",
    )
    (base / "screener_run_meta.json").write_text(
        json.dumps(
            {
                "run_mode": "DECISION",
                "trade_date": "20260724",
                "result_status": "EMPTY_VALID",
                "empty_reason": "ALL_THRESHOLD_PASSERS_ALREADY_HELD",
                "production_candidate_count": 0,
                "score_distribution": {"count": 13, "mean": 0.38, "p90": 0.49},
            }
        ),
        encoding="utf-8",
    )
    (base / "screener_candidates.json").write_text("[]", encoding="utf-8")
    (base / "screener_scores.json").write_text("[]", encoding="utf-8")
    (base / "screener_shadow_candidates.json").write_text("[]", encoding="utf-8")
    (base / "screener_eligible_shadow_candidates.json").write_text(
        json.dumps([{"Ticker": "MU", "Score": 0.4574}]), encoding="utf-8"
    )
    replay = tmp_path / "runs" / "replay" / "SP500" / "20260724" / "pm" / "runR"
    replay.mkdir(parents=True)
    (replay / "manifest.json").write_text(
        json.dumps(
            {
                "run_mode": "REPLAY",
                "market": "SP500",
                "session": "pm",
                "trade_date": "20260724",
                "status": "SUCCESS",
            }
        ),
        encoding="utf-8",
    )
    (replay / "screener_run_meta.json").write_text(
        json.dumps({"run_mode": "REPLAY", "trade_date": "20260724", "production_candidate_count": 9}),
        encoding="utf-8",
    )
    (replay / "screener_candidates.json").write_text(
        json.dumps([{"Ticker": "SHOULD_NOT_COUNT"}]), encoding="utf-8"
    )

    from screener_quality import discover_decision_runs

    discovered = discover_decision_runs(tmp_path, market="SP500", days=20, decision_only=True)
    assert len(discovered.run_dirs) == 1
    assert all("replay" not in str(r).lower() or "decision" in str(r).lower() for r in discovered.run_dirs)
    assert all(Path(r).parts[-5].lower() == "decision" for r in discovered.run_dirs)
    report = aggregate_quality_report(
        discovered.run_dirs,
        market="SP500",
        discovery=discovered.discovery,
        merged_by_run=discovered.merged_by_run,
    )
    assert report["sample_status"] == INSUFFICIENT_SAMPLE
    assert "SHOULD_NOT_COUNT" not in (report.get("repeated_production_candidates") or {})
    assert report["eligible_shadow_frequency"].get("MU") == 1
    assert report["discovery"]["included_run_count"] == 1
    assert report["discovery"]["skip_reasons"].get("REPLAY_EXCLUDED", 0) >= 1


def test_observation_ledger_idempotent(tmp_path):
    path = tmp_path / "screener_candidate_observations.jsonl"
    rows = [
        {
            "decision_run_id": "r1",
            "ticker": "MU",
            "candidate_type": "ELIGIBLE_SHADOW",
            "decision_score": 0.4574,
            "return_1d_pct": None,
            "outcome_status": "PENDING",
        }
    ]
    n1 = upsert_observation_ledger(path, rows)
    rows2 = [
        {
            "decision_run_id": "r1",
            "ticker": "MU",
            "candidate_type": "ELIGIBLE_SHADOW",
            "decision_score": 0.4574,
            "return_1d_pct": 1.2,
            "outcome_status": "OBSERVED",
        }
    ]
    n2 = upsert_observation_ledger(path, rows2)
    assert n1 == 1 and n2 == 1
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["return_1d_pct"] == 1.2
    assert observation_key("r1", "MU", "ELIGIBLE_SHADOW") in {
        observation_key(rec["decision_run_id"], rec["ticker"], rec["candidate_type"])
    }


# ── H. Build / config ───────────────────────────────────────────────

def test_build_identity_env_priority(monkeypatch):
    monkeypatch.setenv("APP_GIT_COMMIT", "abc123env")
    monkeypatch.setenv("APP_IMAGE_TAG", "img:1")
    bi = resolve_build_identity()
    assert bi["git_commit"] == "abc123env"
    assert bi["source"] == "environment"
    assert bi["image_tag"] == "img:1"


def test_get_git_commit_safe_without_git(monkeypatch, tmp_path):
    monkeypatch.delenv("APP_GIT_COMMIT", raising=False)
    # Point at empty dir without .git
    sha, err = get_git_commit(tmp_path)
    assert sha is None
    assert err is not None


def test_score_export_stage_nulls_not_false():
    scored = _enrich([_row("AAPL", 0.51), _row("MU", 0.45)], held={"AAPL"})
    scored = annotate_stage_outcomes(
        scored,
        production_tickers=set(),
        eligibility_pass_tickers=set(),
        issuer_pass_tickers=None,
        sector_pass_tickers=None,
    )
    recs = scores_records_for_export(scored, trade_date="20260724")
    aapl = next(r for r in recs if r["ticker"] == "AAPL")
    # threshold pass but eligibility failed → issuer/sector not evaluated
    assert aapl["threshold_pass"] is True
    assert aapl["eligibility_pass"] is False
    assert aapl["issuer_dedup_pass"] is None
    assert aapl["sector_diversification_pass"] is None


# ── I. Acceptance 20260722–24 ───────────────────────────────────────

def test_acceptance_20260722_fixture():
    scored = _enrich(
        [
            _row("AAPL", 0.51, rsi=88.0),
            _row("MU", 0.50),
            _row("AMD", 0.49),
            _row("X", 0.40),
        ],
        held={"AAPL"},
    )
    # ensure RSI overheated on AAPL
    assert "RSI_OVERHEATED" in scored.loc[scored["Ticker"] == "AAPL", "exclusion_reasons"].iloc[0]
    assert "ALREADY_HELD" in scored.loc[scored["Ticker"] == "AAPL", "exclusion_reasons"].iloc[0]
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    by = {s.stage: s for s in stages}
    assert by["MIN_SCORE"].output_count == 3
    assert by["ELIGIBILITY"].output_count == 2
    assert set(out["Ticker"]) == {"MU", "AMD"}


def test_acceptance_20260723_fixture():
    scored = _enrich(
        [
            _row("AAPL", 0.52),
            _row("NVDA", 0.51),
            _row("AMD", 0.50),
            _row("MU", 0.49),
            _row("Z", 0.40),
        ],
        held={"AAPL", "NVDA", "AMD"},
    )
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    by = {s.stage: s for s in stages}
    assert by["MIN_SCORE"].output_count == 4
    assert by["ELIGIBILITY"].output_count == 1
    assert list(out["Ticker"]) == ["MU"]


def test_acceptance_20260724_fixture():
    scored = _enrich(
        [
            _row("AAPL", 0.51),
            _row("AMD", 0.50),
            _row("MU", 0.4574),
            *[_row(f"T{i}", 0.30 + i * 0.005) for i in range(10)],
        ],
        held={"AAPL", "AMD"},
    )
    out, stages = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    by = {s.stage: s for s in stages}
    assert by["MIN_SCORE"].output_count == 2
    assert by["ELIGIBILITY"].output_count == 0
    assert by["ISSUER_DEDUP"].status == "NOT_RUN"
    assert by["ISSUER_DEDUP"].input_count == 0
    assert by["SECTOR_DIVERSIFICATION"].status == "NOT_RUN"
    assert len(out) == 0
    thr = scored[scored["Score"] >= 0.48]
    status, rs, reason, detail = classify_empty_result_v2(
        candidate_count=0,
        scored_count=len(scored),
        universe_count=501,
        amount5d_pass=13,
        scoring_failures_all=False,
        data_quality_codes=[],
        funnel_stages=stages,
        threshold_pass_rows=thr,
    )
    assert status == "SUCCESS"
    assert rs == "EMPTY_VALID"
    assert reason == "ALL_THRESHOLD_PASSERS_ALREADY_HELD"
    shadow_df, meta = compute_eligible_shadow(
        scored,
        policy={
            "enabled": True,
            "floor": 0.42,
            "percentile": 0.90,
            "min_population": 5,
            "max_candidates": 5,
            "exclude_production_candidates": True,
            "used_by_trader": False,
        },
        production_tickers=set(),
        diversify_fn=_diversify_passthrough,
    )
    assert "MU" in set(shadow_df["Ticker"])
    assert meta["used_by_trader"] is False


def test_production_thresholds_unchanged_in_config():
    from utils import strip_jsonc_comments

    raw = (ROOT / "config" / "config.json").read_text(encoding="utf-8")
    cfg = json.loads(strip_jsonc_comments(raw))
    sp = cfg["screener_params"]
    assert sp["min_score_threshold"] == 0.48
    assert sp["score_threshold_policy"]["static_threshold"] == 0.48
    assert sp["score_threshold_policy"]["mode"] == "static"
    assert sp["min_trading_value_5d_avg_us"] == 5_000_000_000
    assert sp["amount5d_policy"]["static_threshold"] == 5_000_000_000


def test_grep_trader_does_not_reference_new_shadow_files():
    trader = (SRC / "trader.py").read_text(encoding="utf-8")
    gpt = (SRC / "gpt_analyzer.py").read_text(encoding="utf-8")
    for needle in (
        "screener_eligible_shadow_candidates",
        "screener_liquidity_shadow_scores",
        "screener_liquidity_shadow_candidates",
        "screener_candidate_observations",
    ):
        assert needle not in trader
        assert needle not in gpt
    # Quality report filename must not be a trader input prefix
    assert 'prefix in (\n                    "screener_candidates"' in trader or '"screener_candidates"' in trader
    assert "screener_quality" not in gpt
    # Trader may mention quality/shadow only inside exclusion guards
    for line in trader.splitlines():
        if "screener_quality" in line:
            assert "continue" in line or "not trader" in line.lower() or "skip" in line.lower() or "in p.name" in line


# ── J. Discovery path case-insensitivity / legacy schema ─────────────

from screener_quality import (  # noqa: E402
    NOT_AVAILABLE,
    discover_decision_runs,
    main as quality_main,
    merge_manifest_and_meta,
    quality_report_stem,
    write_quality_report,
)


def _write_decision_run(
    root: Path,
    *,
    mode_dir: str,
    trade_date: str,
    run_id: str,
    result_status: str,
    empty_reason: str | None,
    prod_count: int,
    prod_tickers: List[str] | None = None,
    schema_version: int = 1,
    session: str = "pm",
    market: str = "SP500",
    status: str = "SUCCESS",
    completed_at: str | None = None,
    include_v3: bool = False,
) -> Path:
    run_dir = root / "runs" / mode_dir / market / trade_date / session / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    man = {
        "schema_version": schema_version,
        "run_mode": "DECISION",
        "decision_artifact": True,
        "market": market,
        "session": session,
        "trade_date": trade_date,
        "status": status,
        "run_id": run_id,
        "completed_at_kst": completed_at or f"2026-07-26T12:00:00+09:00",
    }
    meta: Dict[str, Any] = {
        "schema_version": str(schema_version) if schema_version >= 3 else "1.0",
        "run_mode": "DECISION",
        "trade_date": trade_date,
        "session": session,
        "market": market,
        "status": status,
        "result_status": result_status,
        "empty_reason": empty_reason,
        "production_candidate_count": prod_count,
        "candidate_count": prod_count,
        "score_distribution": {"count": 13, "mean": 0.4, "p90": 0.5},
        "run_id": run_id,
        "finished_at_kst": man["completed_at_kst"],
    }
    if include_v3:
        meta.update(
            {
                "stage_drop_summary": {"MIN_SCORE": 10},
                "exclusion_summary": {"ALREADY_HELD": 2},
                "candidate_availability": {"threshold_pass_count": 2},
                "eligible_shadow": {"candidate_count": 1},
                "liquidity_shadow": {"candidate_count": 0},
                "diagnostics": {"flag_counts": {}},
                "market_regime_shadow": {"enabled": True},
                "build_identity": {"source": "test"},
            }
        )
    (run_dir / "manifest.json").write_text(json.dumps(man), encoding="utf-8")
    (run_dir / "screener_run_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    cands = [{"Ticker": t, "Score": 0.5, "Sector": "Tech"} for t in (prod_tickers or [])]
    (run_dir / "screener_candidates.json").write_text(json.dumps(cands), encoding="utf-8")
    (run_dir / "screener_scores.json").write_text("[]", encoding="utf-8")
    (run_dir / "screener_shadow_candidates.json").write_text("[]", encoding="utf-8")
    (run_dir / "screener_eligible_shadow_candidates.json").write_text("[]", encoding="utf-8")
    return run_dir


def _acceptance_three_days(root: Path, mode_dir: str = "decision") -> None:
    _write_decision_run(
        root,
        mode_dir=mode_dir,
        trade_date="20260722",
        run_id="run-a",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=2,
        prod_tickers=["MU", "AMD"],
        completed_at="2026-07-22T23:00:00+09:00",
    )
    _write_decision_run(
        root,
        mode_dir=mode_dir,
        trade_date="20260723",
        run_id="run-b",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["MU"],
        completed_at="2026-07-23T23:00:00+09:00",
    )
    _write_decision_run(
        root,
        mode_dir=mode_dir,
        trade_date="20260724",
        run_id="run-c",
        result_status="EMPTY_VALID",
        empty_reason="ALL_THRESHOLD_PASSERS_ALREADY_HELD",
        prod_count=0,
        prod_tickers=[],
        completed_at="2026-07-24T23:00:00+09:00",
    )


def test_discover_lowercase_decision_path(tmp_path):
    _acceptance_three_days(tmp_path, "decision")
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", days=20, decision_only=True)
    assert d.discovery["included_run_count"] == 3
    assert d.discovery["manifest_count"] == 3
    report = aggregate_quality_report(
        d.run_dirs, market="SP500", session="pm", discovery=d.discovery, merged_by_run=d.merged_by_run
    )
    assert report["trading_days"] == 3
    assert report["start_trade_date"] == "20260722"
    assert report["end_trade_date"] == "20260724"


def test_discover_uppercase_decision_path(tmp_path):
    _acceptance_three_days(tmp_path, "DECISION")
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", days=20, decision_only=True)
    assert d.discovery["included_run_count"] == 3
    report = aggregate_quality_report(
        d.run_dirs, market="SP500", session="pm", discovery=d.discovery, merged_by_run=d.merged_by_run
    )
    assert report["trading_days"] == 3


def test_discover_mixed_case_decision_path(tmp_path):
    _acceptance_three_days(tmp_path, "Decision")
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", days=20, decision_only=True)
    assert d.discovery["included_run_count"] == 3


def test_manifest_run_mode_case_insensitive(tmp_path):
    run = _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="run-a",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["MU"],
    )
    man = json.loads((run / "manifest.json").read_text())
    man["run_mode"] = "decision"
    (run / "manifest.json").write_text(json.dumps(man), encoding="utf-8")
    meta = json.loads((run / "screener_run_meta.json").read_text())
    meta["run_mode"] = "Decision"
    (run / "screener_run_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    assert d.discovery["included_run_count"] == 1


def test_schema_v1_and_v3_both_included(tmp_path):
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="run-a",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["MU"],
        schema_version=1,
        include_v3=False,
    )
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260723",
        run_id="run-b",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["AMD"],
        schema_version=3,
        include_v3=True,
    )
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    report = aggregate_quality_report(
        d.run_dirs, market="SP500", discovery=d.discovery, merged_by_run=d.merged_by_run
    )
    assert report["trading_days"] == 2
    legacy = next(x for x in report["days"] if x["trade_date"] == "20260722")
    v3 = next(x for x in report["days"] if x["trade_date"] == "20260723")
    assert legacy["eligible_shadow"] == NOT_AVAILABLE
    assert legacy["stage_drop_summary"] == NOT_AVAILABLE
    assert isinstance(v3["stage_drop_summary"], dict)


def test_decision_only_excludes_replay_include_replay_keeps(tmp_path):
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="d1",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["MU"],
    )
    rdir = tmp_path / "runs" / "replay" / "SP500" / "20260723" / "pm" / "r1"
    rdir.mkdir(parents=True)
    (rdir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_mode": "REPLAY",
                "market": "SP500",
                "session": "pm",
                "trade_date": "20260723",
                "status": "SUCCESS",
                "run_id": "r1",
                "completed_at_kst": "2026-07-23T12:00:00+09:00",
            }
        ),
        encoding="utf-8",
    )
    (rdir / "screener_run_meta.json").write_text(
        json.dumps(
            {
                "run_mode": "REPLAY",
                "trade_date": "20260723",
                "session": "pm",
                "result_status": "HAS_CANDIDATES",
                "production_candidate_count": 9,
                "finished_at_kst": "2026-07-23T12:00:00+09:00",
            }
        ),
        encoding="utf-8",
    )
    (rdir / "screener_candidates.json").write_text("[]", encoding="utf-8")
    only = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    assert only.discovery["included_run_count"] == 1
    assert only.discovery["skip_reasons"].get("REPLAY_EXCLUDED", 0) >= 1
    both = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=False)
    assert both.discovery["included_run_count"] == 2


def test_market_and_session_mismatch_skipped(tmp_path):
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="ok",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["MU"],
        market="SP500",
        session="pm",
    )
    other = _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="am",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["AMD"],
        market="SP500",
        session="am",
    )
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    assert d.discovery["included_run_count"] == 1
    assert d.discovery["skip_reasons"].get("SESSION_MISMATCH", 0) >= 1
    d2 = discover_decision_runs(tmp_path, market="KOSPI", session="pm", decision_only=True)
    assert d2.discovery["included_run_count"] == 0


def test_failed_status_excluded(tmp_path):
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="fail",
        result_status="EMPTY_DATA_QUALITY",
        empty_reason="SCORING_FAILED",
        prod_count=0,
        status="FAILED",
    )
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    assert d.discovery["included_run_count"] == 0
    assert d.discovery["skip_reasons"].get("STATUS_NOT_SUCCESS", 0) >= 1


def test_duplicate_trade_date_keeps_latest(tmp_path):
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="older",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["OLD"],
        completed_at="2026-07-22T10:00:00+09:00",
    )
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="newer",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        prod_count=1,
        prod_tickers=["NEW"],
        completed_at="2026-07-22T23:00:00+09:00",
    )
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    assert d.discovery["included_run_count"] == 1
    assert d.discovery["skip_reasons"].get("DUPLICATE_TRADE_DATE_OLDER_RUN", 0) >= 1
    assert d.run_dirs[0].name == "newer"


def test_acceptance_20260722_24_lowercase_and_filenames(tmp_path):
    _acceptance_three_days(tmp_path, "decision")
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", days=20, decision_only=True)
    report = aggregate_quality_report(
        d.run_dirs, market="SP500", session="pm", discovery=d.discovery, merged_by_run=d.merged_by_run
    )
    assert report["trading_days"] == 3
    assert report["start_trade_date"] == "20260722"
    assert report["end_trade_date"] == "20260724"
    assert report["production_candidate_days"] == 2
    assert report["empty_valid_days"] == 1
    assert report["empty_reason_distribution"].get("ALL_THRESHOLD_PASSERS_ALREADY_HELD") == 1
    assert report["sample_status"] == INSUFFICIENT_SAMPLE
    stem = quality_report_stem(report, "SP500")
    assert stem == "screener_quality_20260722_20260724_SP500"
    assert "UNKNOWN" not in stem
    json_path, md_path = write_quality_report(report, tmp_path, market="SP500")
    assert json_path.name == "screener_quality_20260722_20260724_SP500.json"
    assert md_path.name == "screener_quality_20260722_20260724_SP500.md"
    assert "UNKNOWN" not in json_path.name


def test_no_data_filename_not_unknown(tmp_path):
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    report = aggregate_quality_report(
        d.run_dirs, market="SP500", discovery=d.discovery, merged_by_run=d.merged_by_run
    )
    assert report["trading_days"] == 0
    assert report["sample_status"] == "NO_DATA"
    stem = quality_report_stem(report, "SP500")
    assert stem == "screener_quality_NO_DATA_SP500"
    assert "UNKNOWN" not in stem
    json_path, _ = write_quality_report(report, tmp_path, market="SP500")
    assert "UNKNOWN" not in json_path.name


def test_merge_manifest_meta_priority():
    man = {
        "run_id": "from-man",
        "run_mode": "DECISION",
        "trade_date": "20260724",
        "status": "SUCCESS",
        "result_status": "SHOULD_NOT_WIN",
    }
    met = {
        "run_id": "from-meta",
        "result_status": "EMPTY_VALID",
        "empty_reason": "ALL_THRESHOLD_PASSERS_ALREADY_HELD",
        "production_candidate_count": 0,
    }
    m = merge_manifest_and_meta(man, met)
    assert m["run_id"] == "from-man"
    assert m["result_status"] == "EMPTY_VALID"
    assert m["empty_reason"] == "ALL_THRESHOLD_PASSERS_ALREADY_HELD"


def test_quality_cli_does_not_touch_fixed_candidates(tmp_path):
    _acceptance_three_days(tmp_path, "decision")
    fixed = tmp_path / "screener_candidates_20260724_pm_SP500.json"
    fixed.write_text(json.dumps([{"Ticker": "FIXED"}]), encoding="utf-8")
    before = fixed.read_bytes()
    rc = quality_main(
        [
            "--market",
            "SP500",
            "--session",
            "pm",
            "--days",
            "20",
            "--decision-only",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert rc == 0
    assert fixed.read_bytes() == before
    out = list((tmp_path / "quality").glob("screener_quality_20260722_20260724_SP500.json"))
    assert len(out) == 1


def test_manifests_found_but_none_included_warns(tmp_path):
    _write_decision_run(
        tmp_path,
        mode_dir="decision",
        trade_date="20260722",
        run_id="fail",
        result_status="EMPTY_DATA_QUALITY",
        empty_reason="X",
        prod_count=0,
        status="FAILED",
    )
    d = discover_decision_runs(tmp_path, market="SP500", session="pm", decision_only=True)
    assert d.discovery["manifest_count"] >= 1
    assert d.discovery["included_run_count"] == 0
    assert d.discovery.get("warning") == "MANIFESTS_FOUND_BUT_NONE_INCLUDED"
