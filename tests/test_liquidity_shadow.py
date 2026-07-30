"""Liquidity Shadow scoring, candidate semantics, and post_run_diagnostics immutability."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_artifacts import (  # noqa: E402
    ArtifactError,
    PostRunDiagnosticsWriter,
    ScreenerRunWriter,
    RunModeDecision,
    assert_run_directory_immutable,
    directory_sha_snapshot,
    is_finalized_run,
    reject_post_finalize_write,
    resolve_data_clock,
    sha256_file,
    verify_manifest_integrity,
)
from screener_diagnostics import (  # noqa: E402
    build_liquidity_shadow_rows,
    compute_liquidity_shadow_candidates,
    compute_liquidity_shadow_universe,
    summarize_liquidity_shadow_meta,
)
from screener_quality import (  # noqa: E402
    FAILED_STATUS,
    NOT_AVAILABLE,
    TRUSTED,
    evaluate_liquidity_shadow_trust,
)


def _policy(**overrides: Any) -> Dict[str, Any]:
    base = {
        "enabled": True,
        "threshold_mode": "percentile",
        "percentile": 0.90,
        "liquidity_percentile": 0.90,
        "max_universe": 60,
        "score_threshold_mode": "production_threshold",
        "score_floor": 0.42,
        "score_percentile": 0.90,
        "max_candidates": 10,
        "exclude_production_candidates": True,
        "require_eligibility": True,
        "apply_issuer_dedup": True,
        "apply_sector_diversification": True,
        "used_by_trader": False,
        "time_budget_sec": 90,
    }
    base.update(overrides)
    return base


def _writer(tmp_path: Path, run_id: str = "20260729-225000-liq001") -> ScreenerRunWriter:
    clock = {
        "as_of_kst": "2026-07-29T22:50:00+09:00",
        "as_of_utc": "2026-07-29T13:50:00+00:00",
        "data_cutoff_at_kst": "2026-07-29T22:50:00+09:00",
        "market_session_state": "OPEN",
        "daily_bar_status": "INTRADAY_PARTIAL",
    }
    policy = RunModeDecision(
        run_mode="DECISION",
        replay_type=None,
        decision_artifact=True,
        allow_fixed_update=True,
        invoked_by="test",
        reason="test",
    )
    return ScreenerRunWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260729",
        session="pm",
        run_mode="DECISION",
        run_id=run_id,
        policy=policy,
        clock=clock,
        started_at_kst=clock["as_of_kst"],
    )


def _publish_minimal_decision(tmp_path: Path, candidates: Optional[List[Dict]] = None) -> Path:
    writer = _writer(tmp_path)
    cands = candidates if candidates is not None else [{"Ticker": "AMD", "Score": 0.50}]
    writer.write_json("screener_candidates.json", cands)
    writer.write_json("screener_candidates_full.json", cands)
    writer.write_json("screener_scores.json", [{"ticker": "AMD", "score": 0.50}])
    writer.write_json("screener_shadow_candidates.json", [])
    writer.write_json("screener_eligible_shadow_candidates.json", [])
    writer.write_json("screener_holdings.json", [])
    writer.write_json("market_state.json", {})
    writer.write_json(
        "screener_run_meta.json",
        {
            "status": "SUCCESS",
            "run_mode": "DECISION",
            "liquidity_shadow": {"status": "PENDING", "used_by_trader": False},
        },
    )
    writer.write_text("screener_review.md", "# review\n")
    man = writer.build_manifest(
        status="SUCCESS",
        result_status="HAS_CANDIDATES",
        completed_at_kst="2026-07-29T22:55:00+09:00",
        production_threshold=0.48,
        production_candidate_count=len(cands),
        shadow_threshold=None,
        shadow_candidate_count=0,
        score_count=1,
        config_sha256="cfg",
        issuer_groups_sha256="iss",
        git_commit="abc",
    )
    assert "screener_liquidity_shadow_scores.json" not in man["artifacts"]
    assert man["immutable"] is True
    assert man["post_run_diagnostics_status"] == "PENDING"
    return writer.publish(man)


# ── A. Immutability ──────────────────────────────────────────────────

def test_post_finalize_write_rejected(tmp_path):
    run_dir = _publish_minimal_decision(tmp_path)
    assert is_finalized_run(run_dir)
    assert_run_directory_immutable(run_dir)
    with pytest.raises(ArtifactError):
        reject_post_finalize_write(run_dir, target=run_dir / "screener_run_meta.json")


def test_decision_sha_unchanged_after_diagnostics(tmp_path):
    run_dir = _publish_minimal_decision(tmp_path)
    before = directory_sha_snapshot(run_dir)
    ok, issues = verify_manifest_integrity(run_dir)
    assert ok, issues

    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260729",
        session="pm",
        source_run_id=run_dir.name,
        source_decision_manifest_sha256=sha256_file(run_dir / "manifest.json"),
        started_at_kst="2026-07-29T22:56:00+09:00",
    )
    diag.write_json("screener_liquidity_shadow_scores.json", [{"ticker": "PLTR", "score": 0.51}])
    diag.write_json(
        "screener_liquidity_shadow_candidates.json",
        [{"ticker": "PLTR", "liquidity_shadow_candidate": True, "used_by_trader": False}],
    )
    diag.write_json("liquidity_shadow_meta.json", {"status": "SUCCESS", "universe_count": 1})
    diag.write_text("liquidity_shadow_review.md", "# liq\n")
    man = diag.build_manifest(status="SUCCESS", completed_at_kst="2026-07-29T22:57:00+09:00")
    diag.publish(man)

    after = directory_sha_snapshot(run_dir)
    assert before == after
    assert (tmp_path / "post_run_diagnostics" / "SP500" / "20260729" / "pm" / run_dir.name).exists()


def test_diagnostics_immutable_after_publish(tmp_path):
    run_dir = _publish_minimal_decision(tmp_path)
    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260729",
        session="pm",
        source_run_id=run_dir.name,
        source_decision_manifest_sha256="x",
        started_at_kst="t0",
    )
    diag.write_json("screener_liquidity_shadow_scores.json", [])
    diag.write_json("screener_liquidity_shadow_candidates.json", [])
    diag.write_json("liquidity_shadow_meta.json", {"status": "SUCCESS"})
    published = diag.publish(diag.build_manifest(status="SUCCESS", completed_at_kst="t1"))
    with pytest.raises(ArtifactError):
        diag.write_json("liquidity_shadow_meta.json", {"status": "TAMPERED"})
    assert is_finalized_run(published)


# ── B. Universe + scoring reuse ──────────────────────────────────────

def test_liquidity_universe_includes_below_production_5b():
    amounts = {f"T{i}": float(i + 1) * 1e8 for i in range(100)}
    amounts["PROD1"] = 6e9
    amounts["SHADOW1"] = 2e9  # below 5B but potentially in P90 head
    tickers, meta = compute_liquidity_shadow_universe(
        amounts,
        policy=_policy(max_universe=20),
        production_threshold=5e9,
    )
    assert meta["production_liquidity_threshold"] == 5e9
    assert meta["universe_count"] <= 20
    assert meta["used_by_trader"] is False
    # At least one production-pass name present when ranked high enough
    assert any(amounts[t] >= 5e9 for t in tickers) or meta["production_liquidity_pass_count"] >= 1


def test_shadow_only_rows_get_source_universe_and_status():
    rows = build_liquidity_shadow_rows(
        scored_rows=[
            {"ticker": "AMD", "score": 0.50, "score_status": "SUCCESS"},
            {
                "ticker": "PLTR",
                "score": None,
                "score_status": "DATA_UNAVAILABLE",
                "failure_reason": "NO_PRICE_DATA",
            },
        ],
        amount_by_ticker={"AMD": 6e9, "PLTR": 3e9},
        production_threshold=5e9,
        shadow_threshold=1.7e9,
        production_tickers={"AMD"},
    )
    by = {r["ticker"]: r for r in rows}
    assert by["AMD"]["source_universe"] == "PRODUCTION_AND_LIQUIDITY_SHADOW"
    assert by["PLTR"]["source_universe"] == "LIQUIDITY_SHADOW_ONLY"
    assert by["PLTR"]["score_status"] == "DATA_UNAVAILABLE"
    assert by["PLTR"]["failure_reason"]


def test_universe_invariant_in_meta():
    universe = [f"T{i}" for i in range(5)]
    score_rows = [
        {"ticker": "T0", "score": 0.5, "score_status": "SUCCESS", "source_universe": "PRODUCTION_AND_LIQUIDITY_SHADOW"},
        {"ticker": "T1", "score": 0.4, "score_status": "SUCCESS", "source_universe": "LIQUIDITY_SHADOW_ONLY"},
        {"ticker": "T2", "score": None, "score_status": "API_FAILED", "source_universe": "LIQUIDITY_SHADOW_ONLY"},
        {"ticker": "T3", "score": None, "score_status": "NOT_RUN", "source_universe": "LIQUIDITY_SHADOW_ONLY"},
        {"ticker": "T4", "score": None, "score_status": "TIME_BUDGET_EXCEEDED", "source_universe": "LIQUIDITY_SHADOW_ONLY"},
    ]
    meta = summarize_liquidity_shadow_meta(
        base_meta={"shadow_liquidity_threshold": 1.0},
        universe=universe,
        score_rows=score_rows,
        candidates=[],
        production_reused=1,
        shadow_only_requested=4,
        duration_sec=1.2,
        time_budget_exceeded=True,
    )
    assert meta["universe_count"] == meta["scored_count"] + meta["failed_count"] + meta["unscored_count"]
    assert meta["shadow_only_scored_count"] <= meta["shadow_only_requested_count"]
    assert meta["status"] == "PARTIAL_SHADOW"


# ── C. Candidate semantics ───────────────────────────────────────────

def test_candidates_only_true_flags_and_exclude_production():
    score_rows = []
    # Production candidate AMD
    score_rows.append(
        {
            "ticker": "AMD",
            "score": 0.55,
            "Score": 0.55,
            "name": "AMD",
            "sector": "Tech",
            "amount5d": 6e9,
            "production_liquidity_pass": True,
            "source_universe": "PRODUCTION_AND_LIQUIDITY_SHADOW",
            "exclusion_reasons": [],
            "RSI": 50,
        }
    )
    # Shadow-only eligible
    score_rows.append(
        {
            "ticker": "PLTR",
            "score": 0.52,
            "Score": 0.52,
            "name": "PLTR",
            "sector": "Tech",
            "amount5d": 3e9,
            "production_liquidity_pass": False,
            "source_universe": "LIQUIDITY_SHADOW_ONLY",
            "exclusion_reasons": [],
            "RSI": 45,
        }
    )
    # Ineligible held name
    score_rows.append(
        {
            "ticker": "AAPL",
            "score": 0.60,
            "Score": 0.60,
            "name": "AAPL",
            "sector": "Tech",
            "amount5d": 8e9,
            "production_liquidity_pass": True,
            "source_universe": "PRODUCTION_AND_LIQUIDITY_SHADOW",
            "exclusion_reasons": ["ALREADY_HELD"],
            "RSI": 55,
        }
    )
    cands, meta = compute_liquidity_shadow_candidates(
        score_rows,
        policy=_policy(max_candidates=10),
        production_threshold=0.48,
        production_tickers={"AMD"},
        held_tickers={"AAPL"},
        diversify_fn=lambda df, n, cap: df.head(n),
    )
    assert meta["candidate_count"] == len(cands)
    assert all(c.get("liquidity_shadow_candidate") is True for c in cands)
    assert all(c.get("used_by_trader") is False for c in cands)
    tickers = {c["ticker"] for c in cands}
    assert "AMD" not in tickers  # exclude_production_candidates
    assert "AAPL" not in tickers  # eligibility
    assert "PLTR" in tickers
    assert any(c.get("candidate_origin") == "SHADOW_ONLY" for c in cands)


def test_empty_candidates_are_empty_array():
    cands, meta = compute_liquidity_shadow_candidates(
        [{"ticker": "X", "score": 0.10, "Score": 0.10, "sector": "A", "exclusion_reasons": []}],
        policy=_policy(),
        production_threshold=0.48,
        production_tickers=set(),
        diversify_fn=lambda df, n, cap: df.head(n),
    )
    assert cands == []
    assert meta["candidate_count"] == 0


# ── E. Diagnostics layout ────────────────────────────────────────────

def test_diagnostics_manifest_links_source_run(tmp_path):
    run_dir = _publish_minimal_decision(tmp_path)
    src_sha = sha256_file(run_dir / "manifest.json")
    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260729",
        session="pm",
        source_run_id=run_dir.name,
        source_decision_manifest_sha256=src_sha,
        started_at_kst="t0",
    )
    scores = [{"ticker": "PLTR", "score": 0.5, "liquidity_shadow_candidate": False}]
    cands = [
        {
            "ticker": "PLTR",
            "score": 0.5,
            "liquidity_shadow_candidate": True,
            "used_by_trader": False,
            "candidate_origin": "SHADOW_ONLY",
        }
    ]
    diag.write_json("screener_liquidity_shadow_scores.json", scores)
    diag.write_json("screener_liquidity_shadow_candidates.json", cands)
    diag.write_json(
        "liquidity_shadow_meta.json",
        {"status": "SUCCESS", "universe_count": 1, "candidate_count": 1},
    )
    published = diag.publish(diag.build_manifest(status="SUCCESS", completed_at_kst="t1"))
    man = json.loads((published / "diagnostics_manifest.json").read_text(encoding="utf-8"))
    assert man["source_decision_run_id"] == run_dir.name
    assert man["source_decision_manifest_sha256"] == src_sha
    assert man["used_by_trader"] is False
    assert man["artifacts"]["screener_liquidity_shadow_candidates.json"]["row_count"] == 1
    assert man["artifacts"]["screener_liquidity_shadow_candidates.json"]["sha256"] == sha256_file(
        published / "screener_liquidity_shadow_candidates.json"
    )


# ── F. Quality trust ─────────────────────────────────────────────────

def test_quality_trusts_diagnostics_not_legacy_mutation(tmp_path):
    run_dir = _publish_minimal_decision(tmp_path)
    # Trusted diagnostics
    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260729",
        session="pm",
        source_run_id=run_dir.name,
        source_decision_manifest_sha256=sha256_file(run_dir / "manifest.json"),
        started_at_kst="t0",
    )
    diag.write_json("screener_liquidity_shadow_scores.json", [{"ticker": "PLTR", "score": 0.5}])
    diag.write_json(
        "screener_liquidity_shadow_candidates.json",
        [{"ticker": "PLTR", "liquidity_shadow_candidate": True, "used_by_trader": False}],
    )
    diag.write_json("liquidity_shadow_meta.json", {"status": "SUCCESS", "candidate_count": 1})
    diag.publish(diag.build_manifest(status="SUCCESS", completed_at_kst="t1"))

    trust = evaluate_liquidity_shadow_trust(run_dir, output_dir=tmp_path)
    assert trust["trust_status"] == TRUSTED
    assert trust["candidates"][0]["ticker"] == "PLTR"


def test_quality_missing_diagnostics_not_available(tmp_path):
    run_dir = _publish_minimal_decision(tmp_path)
    trust = evaluate_liquidity_shadow_trust(run_dir, output_dir=tmp_path)
    assert trust["trust_status"] == NOT_AVAILABLE


def test_legacy_post_finalize_mutation_untrusted(tmp_path):
    run_dir = _publish_minimal_decision(tmp_path)
    # Simulate legacy mutation: empty digest in an old-style manifest entry vs non-empty file
    man = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    empty_sha = hashlib.sha256(b"[]").hexdigest()
    # Bypass immutability for fixture construction of legacy bad state
    path = run_dir / "screener_liquidity_shadow_candidates.json"
    path.write_text(json.dumps([{"ticker": "FAKE"}]), encoding="utf-8")
    man.setdefault("artifacts", {})["screener_liquidity_shadow_candidates.json"] = {
        "sha256": empty_sha,
        "row_count": 0,
    }
    (run_dir / "manifest.json").write_text(json.dumps(man, indent=2), encoding="utf-8")
    trust = evaluate_liquidity_shadow_trust(run_dir, output_dir=tmp_path)
    assert trust["trust_status"] == "LIQUIDITY_SHADOW_UNTRUSTED"
    assert "LEGACY_POST_FINALIZE_MUTATION_DETECTED" in trust["reasons"]


# ── §18 Acceptance 20260729 ──────────────────────────────────────────

def test_acceptance_20260729_liquidity_shadow_counts(tmp_path):
    """Synthetic 501/13/51/38 acceptance — no hardcoded ticker scores."""
    # Build Amount5D map: 13 production-pass (>=5B), 38 shadow-only in top-51 P90 band
    amounts: Dict[str, float] = {}
    # Fill lower ranks
    for i in range(501):
        amounts[f"U{i:03d}"] = float(i + 1) * 1e6  # very small
    # 51 liquid names: 13 above 5B, 38 between ~1.7B–4.9B
    for i in range(13):
        amounts[f"P{i:02d}"] = 5e9 + i * 1e8
    for i in range(38):
        amounts[f"S{i:02d}"] = 2e9 + i * 5e7

    tickers, meta = compute_liquidity_shadow_universe(
        amounts,
        policy=_policy(max_universe=51, percentile=0.90),
        production_threshold=5e9,
    )
    # With 501 names, P90 is near top ~50; max_universe=51 caps it
    assert meta["universe_count"] == 51
    assert meta["universe_count"] <= 51
    prod_pass_in_u = [t for t in tickers if amounts[t] >= 5e9]
    shadow_only = [t for t in tickers if amounts[t] < 5e9]
    assert len(prod_pass_in_u) == 13
    assert len(shadow_only) == 38

    # Fake production scores for the 13; mock scorer results for shadow-only
    scores_records = [
        {
            "ticker": t,
            "score": 0.50,
            "Score": 0.50,
            "sector": "Tech",
            "exclusion_reasons": [],
            "RSI": 50,
            "amount5d": amounts[t],
        }
        for t in prod_pass_in_u
    ]
    shadow_scored = [
        {
            "ticker": t,
            "score": 0.50 + (i % 5) * 0.01,
            "Score": 0.50 + (i % 5) * 0.01,
            "name": t,
            "sector": "Tech" if i % 2 == 0 else "Health",
            "exclusion_reasons": [],
            "RSI": 48,
            "amount5d": amounts[t],
            "production_liquidity_pass": False,
            "source_universe": "LIQUIDITY_SHADOW_ONLY",
            "score_status": "SUCCESS",
        }
        for i, t in enumerate(shadow_only)
    ]
    raw = scores_records + shadow_scored
    annotated = build_liquidity_shadow_rows(
        scored_rows=raw,
        amount_by_ticker=amounts,
        production_threshold=5e9,
        shadow_threshold=float(meta["shadow_liquidity_threshold"]),
        production_tickers=set(),  # no production candidates this day
    )
    assert len(annotated) == 51
    scored_n = sum(1 for r in annotated if r.get("score") is not None)
    assert scored_n > 13
    assert scored_n == 51

    cands, cand_meta = compute_liquidity_shadow_candidates(
        annotated,
        policy=_policy(max_candidates=10),
        production_threshold=0.48,
        production_tickers=set(),
        diversify_fn=lambda df, n, cap: df.head(n),
    )
    assert cand_meta["candidate_count"] == len(cands)
    assert all(c.get("liquidity_shadow_candidate") is True for c in cands)
    assert all(c.get("used_by_trader") is False for c in cands)
    if cands:
        assert any(c.get("candidate_origin") == "SHADOW_ONLY" for c in cands)

    summary = summarize_liquidity_shadow_meta(
        base_meta=meta,
        universe=tickers,
        score_rows=annotated,
        candidates=cands,
        candidate_meta=cand_meta,
        production_reused=13,
        shadow_only_requested=38,
        duration_sec=2.0,
    )
    assert summary["universe_count"] == 51
    assert summary["production_score_reused_count"] == 13
    assert summary["shadow_only_requested_count"] == 38
    assert summary["shadow_only_scored_count"] > 0
    assert summary["scored_count"] > 13
    assert summary["unscored_count"] == 0
    assert summary["candidate_count"] == len(cands)

    # Diagnostics publish leaves DECISION untouched
    run_dir = _publish_minimal_decision(tmp_path, candidates=[])
    before = directory_sha_snapshot(run_dir)
    prod_sha = sha256_file(run_dir / "screener_candidates.json")
    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260729",
        session="pm",
        source_run_id=run_dir.name,
        source_decision_manifest_sha256=sha256_file(run_dir / "manifest.json"),
        started_at_kst="t0",
    )
    diag.write_json("screener_liquidity_shadow_scores.json", annotated)
    diag.write_json("screener_liquidity_shadow_candidates.json", cands)
    diag.write_json("liquidity_shadow_meta.json", summary)
    diag.publish(diag.build_manifest(status=summary["status"], completed_at_kst="t1"))
    assert directory_sha_snapshot(run_dir) == before
    assert sha256_file(run_dir / "screener_candidates.json") == prod_sha
