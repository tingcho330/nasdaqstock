"""Quality verification fixes: fundamental parity, meta SHA, trust, outcomes, GPT."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_artifacts import (  # noqa: E402
    ScreenerRunWriter,
    RunModeDecision,
    sha256_file,
    verify_manifest_integrity,
)
from screener_diagnostics import (  # noqa: E402
    annotate_fundamental_parity_fields,
    compute_fundamental_parity_diagnostics,
)
from screener_quality import (  # noqa: E402
    TRUSTED,
    TRUSTED_WITH_WARNING,
    UNTRUSTED,
    NOT_AVAILABLE,
    detect_legacy_meta_self_hash_only,
    evaluate_liquidity_shadow_trust,
    meta_contains_self_hash,
    upsert_observation_ledger,
    build_observation_rows_from_run,
)
from screener_outcomes import (  # noqa: E402
    calculate_forward_return,
    calculate_forward_max_drawdown,
    classify_sample_statuses,
    get_forward_trading_dates,
    settle_observation_outcome,
    backfill_candidate_outcomes,
)
from gpt_quality import (  # noqa: E402
    normalize_gpt_decision,
    parse_gpt_trades_payload,
    evaluate_gpt_incremental_value,
    join_gpt_with_outcomes,
)
from performance_review import (  # noqa: E402
    _screener_candidates_empty_list_semantics,
    _stale_artifact_findings_for_path,
)


def _writer(tmp_path: Path, run_id: str = "20260801-120000-q001") -> ScreenerRunWriter:
    clock = {
        "as_of_kst": "2026-08-01T12:00:00+09:00",
        "as_of_utc": "2026-08-01T03:00:00+00:00",
        "data_cutoff_at_kst": "2026-08-01T12:00:00+09:00",
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
        trade_date="20260801",
        session="pm",
        run_mode="DECISION",
        run_id=run_id,
        policy=policy,
        clock=clock,
        started_at_kst=clock["as_of_kst"],
    )


def _publish(tmp_path: Path, candidates: List[Dict] | None = None) -> Path:
    w = _writer(tmp_path)
    cands = candidates if candidates is not None else [{"Ticker": "AMD", "Score": 0.50}]
    w.write_json("screener_candidates.json", cands)
    w.write_json("screener_scores.json", [{"Ticker": "AMD", "Score": 0.50, "FinScore": 0.4}])
    meta = {
        "run_id": w.run_id,
        "market": "SP500",
        "trade_date": "20260801",
        "session": "pm",
        "result_status": "HAS_CANDIDATES" if cands else "EMPTY_VALID",
        "run_mode": "DECISION",
        "artifact_integrity": {
            "screener_candidates.json": {"sha256": "x"},
            "screener_scores.json": {"sha256": "y"},
        },
    }
    w.write_json("screener_run_meta.json", meta)
    man = w.build_manifest(
        status="SUCCESS",
        result_status=meta["result_status"],
        completed_at_kst=w.started_at_kst,
        production_threshold=0.48,
        production_candidate_count=len(cands),
        shadow_threshold=0.42,
        shadow_candidate_count=0,
        score_count=1,
        config_sha256=None,
        issuer_groups_sha256=None,
        git_commit=None,
    )
    return w.publish(man)


# ── A. Fundamental parity ────────────────────────────────────────────

def test_fundamental_parity_annotation_marks_defaults():
    fin = pd.Series({"PER": float("nan"), "PBR": float("nan")})
    row = annotate_fundamental_parity_fields(
        {"ticker": "XYZ", "fin_score": 0.65, "per": 20.0, "pbr": 1.5, "tech_score": 0.5},
        fin_info=fin,
        score_reused=False,
    )
    assert row["fundamental_default_used"] is True
    assert row["feature_quality"]["fundamentals"] == "UNAVAILABLE"
    assert row["score_feature_source"] == "SHADOW_SCORED_VIA_PRODUCTION_PIPELINE"


def test_fundamental_parity_diagnostics_detects_constant_shadow():
    prod = [
        {"score_reused": True, "fin_score": 0.4, "per": 12, "pbr": 1.1, "fundamental_default_used": False},
        {"score_reused": True, "fin_score": 0.55, "per": 18, "pbr": 2.0, "fundamental_default_used": False},
    ]
    shadow = [
        {
            "score_reused": False,
            "source_universe": "LIQUIDITY_SHADOW_ONLY",
            "fin_score": 0.65,
            "per": 20.0,
            "pbr": 1.5,
            "fundamental_default_used": True,
        }
        for _ in range(20)
    ]
    d = compute_fundamental_parity_diagnostics(prod + shadow)
    assert d["suspicious_constant_feature_detected"] is True
    assert d["shadow_only_unique_fin_scores"] == 1
    assert d["production_unique_fin_scores"] == 2


def test_load_scoring_fin_info_merges_fundamentals(monkeypatch):
    import screener as sc

    listing = pd.DataFrame(
        {"Name": ["AAA"], "Sector": ["Tech"], "Marcap": [0.0]},
        index=["AAA"],
    )

    def fake_fund(date, market, tickers=None, kis=None):
        return pd.DataFrame({"PER": [15.5], "PBR": [2.2]}, index=["AAA"])

    monkeypatch.setattr(sc, "get_fundamentals", fake_fund)
    fin = sc.load_scoring_fin_info(
        ["AAA"], trade_date="20260801", market="SP500", listing_df=listing
    )
    assert float(fin["AAA"]["PER"]) == 15.5
    assert float(fin["AAA"]["PBR"]) == 2.2


# ── B. Meta self-hash ────────────────────────────────────────────────

def test_new_meta_excludes_self_from_artifact_integrity(tmp_path):
    run_dir = _publish(tmp_path)
    meta = json.loads((run_dir / "screener_run_meta.json").read_text(encoding="utf-8"))
    integrity = meta.get("artifact_integrity") or {}
    assert "screener_run_meta.json" not in integrity
    assert "manifest.json" not in integrity
    ok, issues = verify_manifest_integrity(run_dir)
    assert ok, issues


def test_meta_contains_self_hash_detector():
    assert meta_contains_self_hash({"artifact_integrity": {"screener_run_meta.json": {"sha256": "a"}}})
    assert not meta_contains_self_hash({"artifact_integrity": {"screener_scores.json": {"sha256": "a"}}})


def test_legacy_meta_self_hash_only_detection(tmp_path):
    run_dir = _publish(tmp_path)
    meta_path = run_dir / "screener_run_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    # Simulate legacy: embed self hash then mutate file so manifest mismatches only meta
    meta["artifact_integrity"] = dict(meta.get("artifact_integrity") or {})
    meta["artifact_integrity"]["screener_run_meta.json"] = {
        "sha256": sha256_file(meta_path)
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    # Change content again so SHA differs from what was recorded in self-hash (and manifest)
    meta["legacy_marker"] = True
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    ok, issues = verify_manifest_integrity(run_dir)
    assert not ok
    assert detect_legacy_meta_self_hash_only(run_dir, issues=issues, meta=meta)


# ── C. Diagnostics trust ─────────────────────────────────────────────

def test_legacy_self_hash_yields_trusted_with_warning(tmp_path):
    from screener_artifacts import PostRunDiagnosticsWriter

    run_dir = _publish(tmp_path)
    # Inject legacy self-hash mismatch on meta only
    meta_path = run_dir / "screener_run_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["artifact_integrity"] = dict(meta.get("artifact_integrity") or {})
    meta["artifact_integrity"]["screener_run_meta.json"] = {"sha256": "deadbeef"}
    meta_path.write_text(json.dumps(meta) + "\n", encoding="utf-8")

    man_sha = sha256_file(run_dir / "manifest.json")
    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260801",
        session="pm",
        source_run_id=run_dir.name,
        source_decision_manifest_sha256=man_sha,
        started_at_kst="t0",
    )
    diag.write_json("screener_liquidity_shadow_scores.json", [{"ticker": "PLTR", "score": 0.5}])
    diag.write_json(
        "screener_liquidity_shadow_candidates.json",
        [{"ticker": "PLTR", "used_by_trader": False}],
    )
    diag.write_json("liquidity_shadow_meta.json", {"status": "SUCCESS"})
    diag.publish(diag.build_manifest(status="SUCCESS", completed_at_kst="t1"))

    trust = evaluate_liquidity_shadow_trust(run_dir, output_dir=tmp_path)
    assert trust["trust_status"] == TRUSTED_WITH_WARNING
    assert trust["liquidity_shadow_trust_reason"] == "LEGACY_META_SELF_HASH_MISMATCH"


def test_source_manifest_mismatch_untrusted(tmp_path):
    from screener_artifacts import PostRunDiagnosticsWriter

    run_dir = _publish(tmp_path)
    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260801",
        session="pm",
        source_run_id=run_dir.name,
        source_decision_manifest_sha256="0" * 64,
        started_at_kst="t0",
    )
    diag.write_json("screener_liquidity_shadow_scores.json", [])
    diag.write_json("screener_liquidity_shadow_candidates.json", [])
    diag.write_json("liquidity_shadow_meta.json", {"status": "SUCCESS"})
    diag.publish(diag.build_manifest(status="SUCCESS", completed_at_kst="t1"))
    trust = evaluate_liquidity_shadow_trust(run_dir, output_dir=tmp_path)
    assert trust["trust_status"] == UNTRUSTED
    assert "SOURCE_DECISION_MANIFEST_SHA_MISMATCH" in trust["reasons"]


# ── D/E. Ledger + outcomes ───────────────────────────────────────────

def test_outcome_trading_day_skip_weekend():
    # Friday 20260807 → next trading day Monday 20260810
    dates = get_forward_trading_dates("20260807", 1)
    assert dates[0] == "20260810"


def test_forward_return_and_mdd():
    assert calculate_forward_return(100.0, 110.0) == 10.0
    mdd = calculate_forward_max_drawdown(100.0, [105.0, 90.0, 95.0])
    assert mdd is not None and mdd < 0


def test_settle_observation_with_synthetic_series():
    idx = pd.to_datetime(
        ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07", "2026-08-10"]
    )
    df = pd.DataFrame({"Close": [100.0, 101.0, 102.0, 99.0, 103.0, 104.0]}, index=idx)
    obs = {
        "ticker": "AAA",
        "trade_date": "20260803",
        "reference_price": 100.0,
        "outcome_status": "PENDING",
    }
    settled = settle_observation_outcome(obs, close_series=df, as_of_trade_date="20260810")
    assert settled["maturity"]["1d"] is True
    assert settled["return_1d_pct"] == 1.0
    assert settled["outcome_status"] != "PENDING"


def test_ledger_upsert_idempotent(tmp_path):
    ledger = tmp_path / "obs.jsonl"
    rows = [
        {
            "decision_run_id": "r1",
            "ticker": "AAA",
            "candidate_type": "PRODUCTION",
            "trade_date": "20260801",
            "trusted_for_analysis": True,
            "outcome_status": "PENDING",
        }
    ]
    n1 = upsert_observation_ledger(ledger, rows)
    n2 = upsert_observation_ledger(ledger, rows)
    assert n1 == n2 == 1


def test_sample_status_split_no_policy_on_structural_only():
    bits = classify_sample_statuses(
        trading_days=20, matured_1d=0, matured_5d=0, matured_10d=0
    )
    assert bits["structural_sample_status"] == "ADEQUATE_SAMPLE"
    assert bits["outcome_sample_status"] == "NO_SETTLED_OUTCOMES"
    assert bits["policy_change_status"] == "DO_NOT_CHANGE"


# ── G. Performance Review EMPTY_VALID ────────────────────────────────

def test_empty_valid_empty_list_is_valid(tmp_path):
    cands = tmp_path / "screener_candidates_20260801_pm_SP500.json"
    meta = tmp_path / "screener_run_meta_20260801_pm_SP500.json"
    cands.write_text("[]", encoding="utf-8")
    meta.write_text(json.dumps({"result_status": "EMPTY_VALID"}), encoding="utf-8")
    assert _screener_candidates_empty_list_semantics(cands) == "VALID_EMPTY_RESULT"
    findings = _stale_artifact_findings_for_path(cands, stale_sev="WARN")
    titles = {f.title for f in findings}
    assert "ARTIFACT_EMPTY_LIST" not in titles
    assert "VALID_EMPTY_RESULT" in titles


def test_has_candidates_empty_list_is_error(tmp_path):
    cands = tmp_path / "screener_candidates_20260801_pm_SP500.json"
    meta = tmp_path / "screener_run_meta_20260801_pm_SP500.json"
    cands.write_text("[]", encoding="utf-8")
    meta.write_text(json.dumps({"result_status": "HAS_CANDIDATES"}), encoding="utf-8")
    assert _screener_candidates_empty_list_semantics(cands) == "HAS_CANDIDATES_EMPTY_LIST"
    findings = _stale_artifact_findings_for_path(cands, stale_sev="WARN")
    assert any(f.title == "ARTIFACT_EMPTY_LIST" for f in findings)


def test_empty_valid_nonempty_is_error(tmp_path):
    cands = tmp_path / "screener_candidates_20260801_pm_SP500.json"
    meta = tmp_path / "screener_run_meta_20260801_pm_SP500.json"
    cands.write_text(json.dumps([{"Ticker": "AMD"}]), encoding="utf-8")
    meta.write_text(json.dumps({"result_status": "EMPTY_VALID"}), encoding="utf-8")
    assert _screener_candidates_empty_list_semantics(cands) == "EMPTY_VALID_NONEMPTY"
    findings = _stale_artifact_findings_for_path(cands, stale_sev="WARN")
    assert any(f.title == "EMPTY_VALID_NONEMPTY_CANDIDATES" for f in findings)


# ── H. GPT quality ───────────────────────────────────────────────────

def test_gpt_parse_and_incremental():
    payload = [
        {"rank": 1, "결정": "매수", "stock_info": {"Ticker": "AAA", "Score": 0.5}},
        {"rank": 2, "결정": "보류", "stock_info": {"Ticker": "BBB", "Score": 0.5}},
    ]
    rows = parse_gpt_trades_payload(payload)
    assert normalize_gpt_decision("매수") == "BUY"
    assert rows[0]["gpt_decision"] == "BUY"
    joined = join_gpt_with_outcomes(
        [
            {**rows[0], "trade_date": "20260801", "return_5d_pct": 2.0},
            {**rows[1], "trade_date": "20260801", "return_5d_pct": -1.0},
        ],
        [],
    )
    # join without outcomes still keeps rows; inject returns directly
    for j, r in zip(joined, [2.0, -1.0]):
        j["return_5d_pct"] = r
    report = evaluate_gpt_incremental_value(joined)
    assert report["gpt_buy_count"] == 1
    assert report["gpt_hold_count"] == 1
    assert report["production_unchanged"] is True


def test_production_config_threshold_unchanged():
    cfg = (ROOT / "config" / "config.json").read_text(encoding="utf-8")
    # Strip comments roughly for presence checks
    assert '"min_score_threshold": 0.48' in cfg
    assert '"min_trading_value_5d_avg_us": 5000000000' in cfg
    assert '"quality_policy"' in cfg
