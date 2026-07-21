"""Tests for DECISION/REPLAY artifact isolation, manifests, and trader guards."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_artifacts import (  # noqa: E402
    ArtifactError,
    FrozenReplayNotImplemented,
    ScreenerRunWriter,
    clarify_regime_fields,
    generate_run_id,
    get_git_commit,
    latest_decision_pointer_path,
    promote_fixed_artifacts,
    resolve_data_clock,
    resolve_run_mode_policy,
    sha256_file,
    validate_decision_artifacts_for_trader,
    validate_screener_step_for_pipeline,
    write_latest_decision_pointer,
)
from screener_ops import build_run_meta, write_review_markdown  # noqa: E402

KST = ZoneInfo("Asia/Seoul")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_minimal_run(
    tmp_path: Path,
    *,
    mode: str,
    trade_date: str,
    session: str,
    market: str,
    run_id: str,
    candidates: List[Dict[str, Any]],
    scores: List[Dict[str, Any]],
    shadow: List[Dict[str, Any]],
    weighted_regime_score: float,
    scoring_market_component: float,
    as_of_kst: str,
    market_session_state: str,
    daily_bar_status: str,
    allow_fixed: bool = True,
) -> Path:
    from screener_artifacts import RunModeDecision

    clock = {
        "as_of_kst": as_of_kst,
        "as_of_utc": as_of_kst,
        "data_cutoff_at_kst": as_of_kst,
        "market_session_state": market_session_state,
        "daily_bar_status": daily_bar_status,
    }
    policy = RunModeDecision(
        run_mode=mode,
        replay_type=None if mode == "DECISION" else "CURRENT_DATA_RECALCULATION",
        decision_artifact=(mode == "DECISION"),
        allow_fixed_update=allow_fixed and mode == "DECISION",
        invoked_by="test",
        reason="test",
    )
    writer = ScreenerRunWriter(
        output_dir=tmp_path,
        market=market,
        trade_date=trade_date,
        session=session,
        run_mode=mode,
        run_id=run_id,
        policy=policy,
        clock=clock,
        started_at_kst=as_of_kst,
    )
    writer.write_json("screener_candidates.json", candidates)
    writer.write_json("screener_candidates_full.json", candidates)
    writer.write_json("screener_scores.json", scores)
    writer.write_json("screener_shadow_candidates.json", shadow)
    writer.write_json("screener_holdings.json", [])
    writer.write_json(
        "market_state.json",
        clarify_regime_fields(
            components_avg=weighted_regime_score,
            scoring_market_component=scoring_market_component,
            advanced_market_confidence=0.63,
        ),
    )
    meta = build_run_meta(
        market=market,
        trade_date=trade_date,
        session=session,
        status="SUCCESS",
        result_status="HAS_CANDIDATES" if candidates else "EMPTY_VALID",
        empty_reason=None,
        started_at_kst=as_of_kst,
        finished_at_kst=as_of_kst,
        duration_sec=1.0,
        market_state=clarify_regime_fields(
            components_avg=weighted_regime_score,
            scoring_market_component=scoring_market_component,
            advanced_market_confidence=0.63,
        ),
        funnel=[],
        score_distribution_data={"count": len(scores), "mean": 0.4, "p90": 0.5},
        configured_threshold=0.48,
        effective_threshold=0.48,
        candidate_count=len(candidates),
        data_quality_findings=[],
        stage_durations_sec={},
        run_id=writer.run_id,
        run_mode=mode,
        replay_type=policy.replay_type,
        decision_artifact=(mode == "DECISION"),
        invoked_by="test",
        source_run_id=writer.run_id,
        run_directory=str(writer.final_dir),
        as_of_kst=as_of_kst,
        as_of_utc=as_of_kst,
        data_cutoff_at_kst=as_of_kst,
        market_session_state=market_session_state,
        daily_bar_status=daily_bar_status,
        shadow={"threshold": 0.52, "candidate_count": len(shadow)},
    )
    writer.write_json("screener_run_meta.json", meta)
    review = writer.path("screener_review.md")
    write_review_markdown(
        review,
        meta,
        top_scores=scores[:10],
        production_candidates=candidates,
        shadow_candidates=shadow,
    )
    writer._files["screener_review.md"] = review
    manifest = writer.build_manifest(
        status="SUCCESS",
        result_status=meta["result_status"],
        completed_at_kst=as_of_kst,
        production_threshold=0.48,
        production_candidate_count=len(candidates),
        shadow_threshold=0.52,
        shadow_candidate_count=len(shadow),
        score_count=len(scores),
        config_sha256="abc",
        issuer_groups_sha256="def",
        git_commit="deadbeef",
        extra={
            "weighted_regime_score": weighted_regime_score,
            "scoring_market_component": scoring_market_component,
        },
    )
    writer.publish(manifest)
    if mode == "DECISION" and allow_fixed:
        promote_fixed_artifacts(writer)
    return writer.final_dir


# ── A. Run mode ──────────────────────────────────────────────────────

def test_cli_default_is_replay(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKET", "SP500")
    d = resolve_run_mode_policy(
        explicit_run_mode=None,
        force=False,
        trade_date="20260720",
        market="SP500",
        invoked_by="cli",
        fixed_artifact_exists=False,
        now_kst=datetime(2026, 7, 21, 9, 15, tzinfo=KST),
    )
    assert d.run_mode == "REPLAY"
    assert d.replay_type == "CURRENT_DATA_RECALCULATION"
    assert d.allow_fixed_update is False


def test_force_defaults_to_replay():
    d = resolve_run_mode_policy(
        explicit_run_mode=None,
        force=True,
        trade_date="20260720",
        market="SP500",
        invoked_by="cli",
        now_kst=datetime(2026, 7, 21, 9, 15, tzinfo=KST),
    )
    assert d.run_mode == "REPLAY"
    assert d.allow_fixed_update is False


def test_integrated_manager_explicit_decision():
    d = resolve_run_mode_policy(
        explicit_run_mode="decision",
        force=False,
        trade_date="20260720",
        market="SP500",
        invoked_by="integrated_manager",
        fixed_artifact_exists=False,
        now_kst=datetime(2026, 7, 20, 22, 50, tzinfo=KST),
    )
    assert d.run_mode == "DECISION"
    assert d.decision_artifact is True
    assert d.allow_fixed_update is True


def test_past_date_decision_without_allow_converts_to_replay():
    d = resolve_run_mode_policy(
        explicit_run_mode="decision",
        allow_decision_overwrite=False,
        trade_date="20260720",
        market="SP500",
        invoked_by="cli",
        now_kst=datetime(2026, 7, 21, 9, 15, tzinfo=KST),
    )
    assert d.run_mode == "REPLAY"
    assert d.allow_fixed_update is False


def test_past_date_decision_with_allow_updates_fixed():
    d = resolve_run_mode_policy(
        explicit_run_mode="decision",
        allow_decision_overwrite=True,
        trade_date="20260720",
        market="SP500",
        invoked_by="cli",
        fixed_artifact_exists=True,
        now_kst=datetime(2026, 7, 21, 9, 15, tzinfo=KST),
    )
    assert d.run_mode == "DECISION"
    assert d.allow_fixed_update is True


def test_existing_fixed_without_allow_blocks_promote_only():
    d = resolve_run_mode_policy(
        explicit_run_mode="decision",
        allow_decision_overwrite=False,
        trade_date="20260720",
        market="SP500",
        invoked_by="integrated_manager",
        fixed_artifact_exists=True,
        now_kst=datetime(2026, 7, 20, 22, 50, tzinfo=KST),
    )
    assert d.run_mode == "DECISION"
    assert d.allow_fixed_update is False


# ── B/C. Immutable storage + fixed files ─────────────────────────────

def test_decision_and_replay_dirs_isolated(tmp_path):
    decision_dir = _write_minimal_run(
        tmp_path,
        mode="DECISION",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260720-225000-a91f2c",
        candidates=[{"Ticker": "AMD"}, {"Ticker": "GOOGL"}, {"Ticker": "MU"}],
        scores=[{"ticker": f"T{i}", "score": 0.4} for i in range(16)],
        shadow=[{"Ticker": "AMD"}],
        weighted_regime_score=0.820,
        scoring_market_component=0.724,
        as_of_kst="2026-07-20T22:50:00+09:00",
        market_session_state="OPEN",
        daily_bar_status="INTRADAY_PARTIAL",
    )
    fixed_cands = tmp_path / "screener_candidates_20260720_pm_SP500.json"
    before = fixed_cands.read_bytes()
    before_sha = hashlib.sha256(before).hexdigest()

    replay_dir = _write_minimal_run(
        tmp_path,
        mode="REPLAY",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260721-091458-b73d10",
        candidates=[{"Ticker": "AMD"}],
        scores=[{"ticker": f"T{i}", "score": 0.38} for i in range(16)],
        shadow=[{"Ticker": "AMD"}],
        weighted_regime_score=0.615,
        scoring_market_component=0.5805,
        as_of_kst="2026-07-21T09:15:00+09:00",
        market_session_state="CLOSED",
        daily_bar_status="FINAL",
        allow_fixed=False,
    )
    assert decision_dir.exists()
    assert replay_dir.exists()
    assert "decision" in str(decision_dir)
    assert "replay" in str(replay_dir)
    assert hashlib.sha256(fixed_cands.read_bytes()).hexdigest() == before_sha
    pointer = json.loads(
        latest_decision_pointer_path("SP500", "pm", tmp_path).read_text(encoding="utf-8")
    )
    assert pointer["run_id"] == "20260720-225000-a91f2c"
    trader_rows = json.loads(fixed_cands.read_text(encoding="utf-8"))
    assert [r["Ticker"] for r in trader_rows] == ["AMD", "GOOGL", "MU"]


def test_same_run_id_overwrite_rejected(tmp_path):
    _write_minimal_run(
        tmp_path,
        mode="REPLAY",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260720-225000-dup001",
        candidates=[],
        scores=[],
        shadow=[],
        weighted_regime_score=0.5,
        scoring_market_component=0.5,
        as_of_kst="2026-07-20T22:50:00+09:00",
        market_session_state="OPEN",
        daily_bar_status="INTRADAY_PARTIAL",
        allow_fixed=False,
    )
    from screener_artifacts import RunModeDecision

    policy = RunModeDecision(
        run_mode="REPLAY",
        replay_type="CURRENT_DATA_RECALCULATION",
        decision_artifact=False,
        allow_fixed_update=False,
        invoked_by="test",
        reason="test",
    )
    # ensure_unique_run_id should allocate a different id rather than overwrite
    writer = ScreenerRunWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260720",
        session="pm",
        run_mode="REPLAY",
        run_id="20260720-225000-dup001",
        policy=policy,
        clock=resolve_data_clock(market="SP500", trade_date="20260720"),
        started_at_kst="2026-07-20T22:50:00+09:00",
    )
    assert writer.run_id != "20260720-225000-dup001"
    writer.abandon_staging()


def test_failed_run_does_not_promote(tmp_path):
    from screener_artifacts import RunModeDecision

    # Seed fixed file
    fixed = tmp_path / "screener_candidates_20260720_pm_SP500.json"
    fixed.write_text(json.dumps([{"Ticker": "KEEP"}]), encoding="utf-8")
    before = _sha(fixed)

    clock = resolve_data_clock(
        market="SP500",
        trade_date="20260720",
        as_of_kst=datetime(2026, 7, 20, 22, 50, tzinfo=KST),
    )
    policy = RunModeDecision(
        run_mode="DECISION",
        replay_type=None,
        decision_artifact=True,
        allow_fixed_update=True,
        invoked_by="test",
        reason="test",
    )
    writer = ScreenerRunWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260720",
        session="pm",
        run_mode="DECISION",
        run_id="20260720-225000-fail01",
        policy=policy,
        clock=clock,
        started_at_kst=clock["as_of_kst"],
    )
    writer.write_json("screener_candidates.json", [{"Ticker": "BAD"}])
    writer.write_json("screener_scores.json", [])
    writer.write_json("screener_run_meta.json", {"status": "FAILED"})
    man = writer.build_manifest(
        status="FAILED",
        result_status="FAILED",
        completed_at_kst=clock["as_of_kst"],
        production_threshold=0.48,
        production_candidate_count=0,
        shadow_threshold=None,
        shadow_candidate_count=0,
        score_count=0,
        config_sha256=None,
        issuer_groups_sha256=None,
        git_commit=None,
    )
    writer.publish(man)
    with pytest.raises(ArtifactError):
        promote_fixed_artifacts(writer)
    assert _sha(fixed) == before


# ── D. Manifest ──────────────────────────────────────────────────────

def test_manifest_sha_and_row_counts(tmp_path):
    run_dir = _write_minimal_run(
        tmp_path,
        mode="DECISION",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260720-225000-man001",
        candidates=[{"Ticker": "AMD"}, {"Ticker": "GOOGL"}, {"Ticker": "MU"}],
        scores=[{"ticker": f"T{i}"} for i in range(16)],
        shadow=[{"Ticker": "AMD"}],
        weighted_regime_score=0.82,
        scoring_market_component=0.72,
        as_of_kst="2026-07-20T22:50:00+09:00",
        market_session_state="OPEN",
        daily_bar_status="INTRADAY_PARTIAL",
    )
    man = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert man["run_id"]
    assert man["run_mode"] == "DECISION"
    assert man["status"] == "SUCCESS"
    assert man["config_sha256"] == "abc"
    art = man["artifacts"]["screener_candidates.json"]
    assert art["row_count"] == 3
    assert art["sha256"] == sha256_file(run_dir / "screener_candidates.json")
    assert man["artifacts"]["screener_scores.json"]["row_count"] == 16


def test_git_commit_failure_is_safe():
    sha, err = get_git_commit(repo_root=Path("/tmp/does-not-exist-git-repo-xyz"))
    # Either null with error, or somehow succeeds — must not raise
    assert sha is None or isinstance(sha, str)


# ── E. Market clock ──────────────────────────────────────────────────

def test_open_intraday_partial():
    clock = resolve_data_clock(
        market="SP500",
        trade_date="20260720",
        as_of_kst=datetime(2026, 7, 20, 22, 50, tzinfo=KST),
    )
    assert clock["market_session_state"] == "OPEN"
    assert clock["daily_bar_status"] == "INTRADAY_PARTIAL"


def test_closed_final():
    clock = resolve_data_clock(
        market="SP500",
        trade_date="20260720",
        as_of_kst=datetime(2026, 7, 21, 9, 15, tzinfo=KST),
    )
    assert clock["market_session_state"] == "CLOSED"
    assert clock["daily_bar_status"] == "FINAL"


def test_unknown_on_bad_date():
    clock = resolve_data_clock(market="SP500", trade_date="notadate")
    assert clock["market_session_state"] == "UNKNOWN"
    assert clock["daily_bar_status"] == "UNKNOWN"


# ── F. Regime fields ─────────────────────────────────────────────────

def test_regime_fields_separated():
    clarified = clarify_regime_fields(
        components_avg=0.957,
        scoring_market_component=0.820,
        advanced_market_confidence=0.63,
    )
    assert clarified["weighted_regime_score"] == 0.957
    assert clarified["scoring_market_component"] == 0.820
    assert clarified["advanced_market_confidence"] == 0.63
    assert clarified["regime_score"] == 0.957
    assert clarified["regime_score_deprecated_alias_of"] == "weighted_regime_score"


def test_review_labels_distinguish_regime(tmp_path):
    meta = build_run_meta(
        market="SP500",
        trade_date="20260720",
        session="pm",
        status="SUCCESS",
        result_status="HAS_CANDIDATES",
        empty_reason=None,
        started_at_kst="t0",
        finished_at_kst="t1",
        duration_sec=1,
        market_state=clarify_regime_fields(
            components_avg=0.957,
            scoring_market_component=0.820,
            advanced_market_confidence=0.63,
        ),
        funnel=[],
        score_distribution_data={},
        configured_threshold=0.48,
        effective_threshold=0.48,
        candidate_count=3,
        data_quality_findings=[],
        stage_durations_sec={},
        run_mode="REPLAY",
        replay_type="CURRENT_DATA_RECALCULATION",
        decision_artifact=False,
        as_of_kst="2026-07-21T09:15:00+09:00",
        market_session_state="CLOSED",
        daily_bar_status="FINAL",
    )
    path = tmp_path / "review.md"
    write_review_markdown(path, meta, top_scores=[], production_candidates=[], shadow_candidates=[])
    text = path.read_text(encoding="utf-8")
    assert "REPLAY — NOT USED BY TRADER" in text
    assert "weighted_regime_score" in text
    assert "scoring_market_component" in text
    assert "CURRENT_DATA_RECALCULATION" in text or "recalculated using data available" in text


# ── G. Trader safety ─────────────────────────────────────────────────

def test_trader_rejects_replay_meta(tmp_path):
    run_dir = _write_minimal_run(
        tmp_path,
        mode="REPLAY",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260721-091458-rep001",
        candidates=[{"Ticker": "AMD"}],
        scores=[],
        shadow=[],
        weighted_regime_score=0.6,
        scoring_market_component=0.5,
        as_of_kst="2026-07-21T09:15:00+09:00",
        market_session_state="CLOSED",
        daily_bar_status="FINAL",
        allow_fixed=False,
    )
    # Manually plant a fake fixed meta pointing at REPLAY (should never happen, but guard)
    meta = json.loads((run_dir / "screener_run_meta.json").read_text(encoding="utf-8"))
    (tmp_path / "screener_run_meta_20260720_pm_SP500.json").write_text(
        json.dumps(meta), encoding="utf-8"
    )
    (tmp_path / "screener_candidates_20260720_pm_SP500.json").write_text(
        json.dumps([{"Ticker": "AMD"}]), encoding="utf-8"
    )
    ok, msg, _ = validate_decision_artifacts_for_trader(
        trade_date="20260720", market="SP500", session="pm", output_dir=tmp_path
    )
    assert ok is False
    assert "DECISION" in msg or "replay" in msg.lower()


def test_shadow_not_trader_input(tmp_path):
    ok, msg, _ = validate_decision_artifacts_for_trader(
        trade_date="20260720",
        market="SP500",
        session="pm",
        output_dir=tmp_path,
        candidates_path=tmp_path / "screener_shadow_candidates_20260720_pm_SP500.json",
    )
    assert ok is False


def test_hash_mismatch_blocks_trader(tmp_path):
    run_dir = _write_minimal_run(
        tmp_path,
        mode="DECISION",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260720-225000-hash01",
        candidates=[{"Ticker": "AMD"}, {"Ticker": "GOOGL"}, {"Ticker": "MU"}],
        scores=[{"ticker": "AMD"}],
        shadow=[],
        weighted_regime_score=0.82,
        scoring_market_component=0.72,
        as_of_kst="2026-07-20T22:50:00+09:00",
        market_session_state="OPEN",
        daily_bar_status="INTRADAY_PARTIAL",
    )
    # Tamper fixed candidates after promote
    cands = tmp_path / "screener_candidates_20260720_pm_SP500.json"
    cands.write_text(json.dumps([{"Ticker": "TAMPERED"}]), encoding="utf-8")
    ok, msg, _ = validate_decision_artifacts_for_trader(
        trade_date="20260720", market="SP500", session="pm", output_dir=tmp_path
    )
    assert ok is False
    assert "sha256" in msg or "mismatch" in msg


def test_pipeline_gate_ok_for_decision(tmp_path):
    _write_minimal_run(
        tmp_path,
        mode="DECISION",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260720-225000-gate01",
        candidates=[{"Ticker": "AMD"}, {"Ticker": "GOOGL"}, {"Ticker": "MU"}],
        scores=[{"ticker": "AMD"}],
        shadow=[],
        weighted_regime_score=0.82,
        scoring_market_component=0.72,
        as_of_kst="2026-07-20T22:50:00+09:00",
        market_session_state="OPEN",
        daily_bar_status="INTRADAY_PARTIAL",
    )
    ok, msg, meta = validate_screener_step_for_pipeline(
        trade_date="20260720", market="SP500", session="pm", output_dir=tmp_path
    )
    assert ok is True, msg
    assert meta["run_mode"] == "DECISION"


# ── H/Acceptance 20260720 ────────────────────────────────────────────

def test_acceptance_20260720_decision_vs_replay(tmp_path):
    """Fixture acceptance: REPLAY must not alter DECISION Production bytes."""
    decision_dir = _write_minimal_run(
        tmp_path,
        mode="DECISION",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260720-225000-a91f2c",
        candidates=[
            {"Ticker": "AMD", "Score": 0.50},
            {"Ticker": "GOOGL", "Score": 0.49},
            {"Ticker": "MU", "Score": 0.48},
        ],
        scores=[{"ticker": f"S{i}", "score": 0.434} for i in range(16)],
        shadow=[{"Ticker": "AMD", "Score": 0.53}],
        weighted_regime_score=0.820,
        scoring_market_component=0.724,
        as_of_kst="2026-07-20T22:50:00+09:00",
        market_session_state="OPEN",
        daily_bar_status="INTRADAY_PARTIAL",
    )
    fixed_files = [
        tmp_path / "screener_candidates_20260720_pm_SP500.json",
        tmp_path / "screener_scores_20260720_pm_SP500.json",
        tmp_path / "screener_run_meta_20260720_pm_SP500.json",
    ]
    shas_before = {p.name: _sha(p) for p in fixed_files}

    replay_dir = _write_minimal_run(
        tmp_path,
        mode="REPLAY",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260721-091458-b73d10",
        candidates=[{"Ticker": "AMD", "Score": 0.40}],
        scores=[{"ticker": f"S{i}", "score": 0.380} for i in range(16)],
        shadow=[{"Ticker": "AMD"}],
        weighted_regime_score=0.615,
        scoring_market_component=0.5805,
        as_of_kst="2026-07-21T09:15:00+09:00",
        market_session_state="CLOSED",
        daily_bar_status="FINAL",
        allow_fixed=False,
    )

    assert decision_dir.is_dir() and replay_dir.is_dir()
    for p in fixed_files:
        assert _sha(p) == shas_before[p.name]

    pointer = json.loads(
        latest_decision_pointer_path("SP500", "pm", tmp_path).read_text(encoding="utf-8")
    )
    assert pointer["run_id"] == "20260720-225000-a91f2c"

    prod = json.loads(fixed_files[0].read_text(encoding="utf-8"))
    assert [r["Ticker"] for r in prod] == ["AMD", "GOOGL", "MU"]

    d_man = json.loads((decision_dir / "manifest.json").read_text(encoding="utf-8"))
    r_man = json.loads((replay_dir / "manifest.json").read_text(encoding="utf-8"))
    assert d_man["as_of_kst"] != r_man["as_of_kst"]
    assert d_man["market_session_state"] == "OPEN"
    assert r_man["market_session_state"] == "CLOSED"
    assert d_man["weighted_regime_score"] == 0.820
    assert r_man["weighted_regime_score"] == 0.615
    assert r_man["replay_type"] == "CURRENT_DATA_RECALCULATION"

    ok, _, _ = validate_decision_artifacts_for_trader(
        trade_date="20260720", market="SP500", session="pm", output_dir=tmp_path
    )
    assert ok is True

    # Threshold unchanged in meta
    meta = json.loads(fixed_files[2].read_text(encoding="utf-8"))
    assert meta["production_threshold"] == 0.48


def test_generate_run_id_format():
    rid = generate_run_id(datetime(2026, 7, 20, 22, 50, 0, tzinfo=KST))
    assert rid.startswith("20260720-225000-")
    assert len(rid.split("-")[-1]) == 6


def test_frozen_replay_not_implemented():
    with pytest.raises(FrozenReplayNotImplemented):
        resolve_run_mode_policy(
            explicit_run_mode="replay",
            explicit_replay_type="frozen_input_replay",
            trade_date="20260720",
            market="SP500",
            now_kst=datetime(2026, 7, 21, 9, 15, tzinfo=KST),
        )


def test_legacy_paths_untouched(tmp_path):
    legacy_replay = tmp_path / "replay" / "20260720_20260721-091458"
    legacy_decision = tmp_path / "decision_runs" / "keep_me"
    legacy_replay.mkdir(parents=True)
    legacy_decision.mkdir(parents=True)
    (legacy_replay / "note.txt").write_text("legacy", encoding="utf-8")
    (legacy_decision / "note.txt").write_text("legacy", encoding="utf-8")
    _write_minimal_run(
        tmp_path,
        mode="REPLAY",
        trade_date="20260720",
        session="pm",
        market="SP500",
        run_id="20260721-091458-new001",
        candidates=[],
        scores=[],
        shadow=[],
        weighted_regime_score=0.5,
        scoring_market_component=0.5,
        as_of_kst="2026-07-21T09:15:00+09:00",
        market_session_state="CLOSED",
        daily_bar_status="FINAL",
        allow_fixed=False,
    )
    assert (legacy_replay / "note.txt").read_text(encoding="utf-8") == "legacy"
    assert (legacy_decision / "note.txt").read_text(encoding="utf-8") == "legacy"
