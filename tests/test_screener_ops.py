"""Unit tests for screener operational improvements (funnel, cache, shadow, scores)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_ops import (  # noqa: E402
    FunnelRecorder,
    StageResult,
    amount5d_cache_path,
    atomic_write_json,
    classify_empty_result,
    compute_shadow_score_threshold,
    dedupe_by_issuer_group,
    enrich_scored_dataframe,
    extract_screener_summary_lines,
    load_amount5d_cache,
    marcap_filter_decision,
    merge_amount5d_cache_entries,
    resolve_issuer_group,
    save_amount5d_cache,
    save_subprocess_log,
    score_distribution,
    scores_records_for_export,
    select_candidates_pipeline,
)


def _scored_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _diversify_passthrough(df, top_n, sector_cap):
    return df.head(top_n)


# ── 1. Marcap all-zero → SKIPPED ─────────────────────────────────────

def test_marcap_all_zero_skipped_not_applied():
    s = pd.Series([0.0] * 501)
    status, reason, mask = marcap_filter_decision(
        s, min_mc=5e9, max_mc=5e12, is_us=True, min_valid_ratio=0.8
    )
    assert status == "SKIPPED"
    assert reason == "MARKET_CAP_DATA_UNAVAILABLE"
    assert int(mask.sum()) == 501  # pass-through but not "passed filter"


def test_marcap_funnel_skipped_status_in_recorder():
    funnel = FunnelRecorder()
    funnel.record_applied("UNIVERSE", 501, 501)
    funnel.record_skipped("MARKET_CAP", 501, reason="MARKET_CAP_DATA_UNAVAILABLE")
    d = funnel.stages[1].to_dict()
    assert d["status"] == "SKIPPED"
    assert d["reason"] == "MARKET_CAP_DATA_UNAVAILABLE"
    assert d["input_count"] == 501
    assert d["output_count"] == 501
    # Must not look like a successful filter pass label-wise
    assert d["status"] != "APPLIED"


# ── 2. Funnel 16→0 then NOT_RUN, no negative drops ───────────────────

def test_funnel_min_score_zero_downstream_not_run():
    df = _scored_df(
        [
            {"Ticker": f"T{i}", "Score": 0.40 + i * 0.001, "exclude_reasons": [], "eligibility_status": "ELIGIBLE", "momentum_pass": True, "volatility_pass": True, "issuer_group": f"T{i}"}
            for i in range(16)
        ]
    )
    out, stages = select_candidates_pipeline(
        df,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    assert len(out) == 0
    by = {s.stage: s for s in stages}
    assert by["MIN_SCORE"].input_count == 16
    assert by["MIN_SCORE"].output_count == 0
    assert by["MIN_SCORE"].dropped_count == 16
    assert by["MOMENTUM"].status == "NOT_RUN"
    assert by["MOMENTUM"].input_count == 0
    assert by["VOLATILITY"].status == "NOT_RUN"
    assert by["SECTOR_DIVERSIFICATION"].status == "NOT_RUN"
    for s in stages:
        assert s.dropped_count >= 0


def test_funnel_invariant_chain():
    funnel = FunnelRecorder()
    funnel.record_applied("UNIVERSE", 501, 501)
    funnel.record_skipped("MARKET_CAP", 501, reason="MARKET_CAP_DATA_UNAVAILABLE")
    funnel.record_applied("AMOUNT5D", 501, 16, threshold=5e9)
    funnel.record_applied("SCORING", 16, 16)
    funnel.record_applied("MIN_SCORE", 16, 0, threshold=0.48)
    funnel.record_not_run("MOMENTUM", reason="NO_INPUT")
    funnel.record_not_run("VOLATILITY", reason="NO_INPUT")
    funnel.record_not_run("SECTOR_DIVERSIFICATION", reason="NO_INPUT")
    errors = funnel.validate()
    assert errors == []


# ── 3/4/5. Scores keep all; candidates empty; EMPTY_VALID ────────────

def test_scores_export_keeps_all_and_exclusion_reasons():
    df = enrich_scored_dataframe(
        _scored_df(
            [
                {
                    "Ticker": "AAPL",
                    "Name": "Apple",
                    "Sector": "Tech",
                    "Score": 0.4524,
                    "FinScore": 0.5,
                    "TechScore": 0.4,
                    "MktScore": 0.5,
                    "SectorScore": 0.5,
                    "PatternScore": 0.2,
                    "VolKki": 0.1,
                    "Pos52w": 0.8,
                    "RSI": 88.6,
                    "ATR": 1.0,
                    "MA50": 100,
                    "MA200": 90,
                    "PER": 30,
                    "PBR": 40,
                    "Price": 200,
                    "exclude_reasons": ["UP_STREAK"],
                },
                {
                    "Ticker": "MU",
                    "Name": "Micron",
                    "Sector": "Tech",
                    "Score": 0.4079,
                    "FinScore": 0.4,
                    "TechScore": 0.4,
                    "MktScore": 0.5,
                    "SectorScore": 0.5,
                    "PatternScore": 0.2,
                    "VolKki": 0.1,
                    "Pos52w": 0.7,
                    "RSI": 55,
                    "ATR": 1.0,
                    "MA50": 100,
                    "MA200": 90,
                    "PER": 20,
                    "PBR": 2,
                    "Price": 100,
                    "exclude_reasons": [],
                },
            ]
        ),
        held_tickers={"AAPL"},
        issuer_map={"GOOG": "ALPHABET", "GOOGL": "ALPHABET"},
        production_threshold=0.48,
        rsi_overheated_threshold=70.0,
    )
    records = scores_records_for_export(df, trade_date="20260717")
    assert len(records) == 2
    aapl = next(r for r in records if r["ticker"] == "AAPL")
    assert aapl["held"] is True
    assert "UP_STREAK" in aapl["exclusion_reasons"]
    assert "ALREADY_HELD" in aapl["exclusion_reasons"]
    assert "RSI_OVERHEATED" in aapl["exclusion_reasons"]
    assert aapl["eligibility_status"] == "EXCLUDED"


def test_empty_valid_classification():
    status, result_status, empty_reason = classify_empty_result(
        candidate_count=0,
        scored_count=16,
        universe_count=501,
        amount5d_pass=16,
        scoring_failures_all=False,
        data_quality_codes=["MARKET_CAP_DATA_UNAVAILABLE"],
        min_score_pass=0,
        empty_after_min_score=True,
    )
    assert status == "SUCCESS"
    assert result_status == "EMPTY_VALID"
    assert empty_reason == "MIN_SCORE_THRESHOLD_NOT_MET"


# ── 6. AAPL held excluded from candidates, kept in scores ────────────

def test_held_upstreak_excluded_from_candidates_kept_in_scores():
    rows = [
        {
            "Ticker": "AAPL",
            "Score": 0.50,
            "RSI": 88.6,
            "exclude_reasons": ["UP_STREAK"],
            "Name": "Apple",
            "Sector": "Tech",
            "FinScore": 0.5,
            "TechScore": 0.5,
            "MktScore": 0.5,
            "SectorScore": 0.5,
            "PatternScore": 0.2,
            "VolKki": 0.1,
            "Pos52w": 0.8,
            "ATR": 1,
            "MA50": 1,
            "MA200": 1,
            "PER": 1,
            "PBR": 1,
            "Price": 1,
        },
        {
            "Ticker": "MU",
            "Score": 0.49,
            "RSI": 50,
            "exclude_reasons": [],
            "Name": "Micron",
            "Sector": "Tech",
            "FinScore": 0.5,
            "TechScore": 0.5,
            "MktScore": 0.5,
            "SectorScore": 0.5,
            "PatternScore": 0.2,
            "VolKki": 0.1,
            "Pos52w": 0.7,
            "ATR": 1,
            "MA50": 1,
            "MA200": 1,
            "PER": 1,
            "PBR": 1,
            "Price": 1,
        },
    ]
    scored = enrich_scored_dataframe(
        _scored_df(rows),
        held_tickers={"AAPL"},
        issuer_map={},
        production_threshold=0.48,
    )
    cands, _ = select_candidates_pipeline(
        scored,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
        require_eligible=True,
    )
    assert "AAPL" not in set(cands["Ticker"].tolist()) if not cands.empty else True
    assert len(scores_records_for_export(scored, trade_date="20260717")) == 2
    if not cands.empty:
        assert list(cands["Ticker"]) == ["MU"]


# ── 7. GOOG/GOOGL issuer dedupe ──────────────────────────────────────

def test_goog_googl_issuer_dedupe():
    assert resolve_issuer_group("GOOG", {"GOOG": "ALPHABET", "GOOGL": "ALPHABET"}) == "ALPHABET"
    df = _scored_df(
        [
            {"Ticker": "GOOGL", "Score": 0.438, "issuer_group": "ALPHABET"},
            {"Ticker": "GOOG", "Score": 0.4366, "issuer_group": "ALPHABET"},
            {"Ticker": "MU", "Score": 0.40, "issuer_group": "MU"},
        ]
    )
    out = dedupe_by_issuer_group(df)
    assert len(out) == 2
    assert "GOOGL" in set(out["Ticker"])
    assert "GOOG" not in set(out["Ticker"])


# ── 8/9. Static 0.48 keeps 0; shadow hybrid ──────────────────────────

def test_static_threshold_keeps_zero_candidates_20260717():
    scores = [0.4524, 0.4380, 0.4366, 0.4079, 0.4057, 0.4026] + [0.30] * 10
    rows = []
    for i in range(16):
        rows.append(
            {
                "Ticker": f"T{i}",
                "Score": scores[i],
                "exclude_reasons": [],
                "eligibility_status": "ELIGIBLE",
                "momentum_pass": True,
                "volatility_pass": True,
                "issuer_group": f"T{i}",
                "RSI": 50,
            }
        )
    rows[0].update(
        {
            "Ticker": "AAPL",
            "eligibility_status": "EXCLUDED",
            "exclude_reasons": ["ALREADY_HELD", "UP_STREAK", "RSI_OVERHEATED"],
        }
    )
    rows[1].update({"Ticker": "GOOGL", "issuer_group": "ALPHABET"})
    rows[2].update({"Ticker": "GOOG", "issuer_group": "ALPHABET"})
    df = _scored_df(rows)

    prod, stages = select_candidates_pipeline(
        df,
        threshold=0.48,
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
    )
    assert len(prod) == 0
    assert stages[0].threshold == 0.48

    shadow_thr = compute_shadow_score_threshold(
        scores,
        {
            "shadow_enabled": True,
            "shadow_mode": "hybrid",
            "shadow_floor": 0.42,
            "shadow_percentile": 0.90,
        },
    )
    assert shadow_thr is not None
    assert abs(shadow_thr - max(0.42, float(pd.Series(scores).quantile(0.90)))) < 1e-9
    # Shadow uses lower threshold but still excludes ineligible / issuer dupes
    shadow_df = enrich_scored_dataframe(
        df.drop(columns=["eligibility_status", "issuer_group", "momentum_pass", "volatility_pass"], errors="ignore"),
        held_tickers={"AAPL"},
        issuer_map={"GOOG": "ALPHABET", "GOOGL": "ALPHABET"},
        production_threshold=0.48,
    )
    # Re-apply scores from original
    shadow_df["Score"] = df["Score"].values
    shadow_cands, _ = select_candidates_pipeline(
        shadow_df,
        threshold=float(shadow_thr),
        require_positive_momentum=False,
        exclude_high_volatility=False,
        top_n=8,
        sector_cap=0.35,
        diversify_fn=_diversify_passthrough,
        require_eligible=True,
    )
    tickers = set(shadow_cands["Ticker"].tolist()) if not shadow_cands.empty else set()
    assert "AAPL" not in tickers
    assert not ({"GOOG", "GOOGL"} <= tickers)  # at most one Alphabet


# ── 10/11. Amount5D cache hit/miss + corrupt schema ──────────────────

def test_amount5d_cache_hit_miss_and_schema_mismatch(tmp_path):
    path = amount5d_cache_path(tmp_path, "SP500", "20260717")
    payload = merge_amount5d_cache_entries(
        None,
        market="SP500",
        trade_date="20260717",
        lookback_days=5,
        data_source="kis",
        new_entries={"AAPL": {"value": 1e10, "status": "ok", "exchange": "NAS"}},
    )
    save_amount5d_cache(path, payload)
    loaded = load_amount5d_cache(
        path, market="SP500", trade_date="20260717", lookback_days=5, data_source="kis"
    )
    assert loaded is not None
    assert loaded["entries"]["AAPL"]["value"] == 1e10

    # schema mismatch → ignore
    bad = dict(payload)
    bad["schema_version"] = "1"
    path.write_text(json.dumps(bad), encoding="utf-8")
    assert (
        load_amount5d_cache(
            path, market="SP500", trade_date="20260717", lookback_days=5, data_source="kis"
        )
        is None
    )

    # corrupt JSON → ignore
    path.write_text("{not-json", encoding="utf-8")
    assert (
        load_amount5d_cache(
            path, market="SP500", trade_date="20260717", lookback_days=5, data_source="kis"
        )
        is None
    )


# ── 12/13. Subprocess log + summary extraction ───────────────────────

def test_subprocess_log_and_summary(tmp_path):
    stdout = "\n".join(
        [
            "noise",
            "┌─ 1차 필터링 퍼널 ──────────────────",
            "스코어 분포: 평균=0.363",
            "최소점수 통과 수=0 / 최종 후보 수=0",
            "EMPTY reason=MIN_SCORE_THRESHOLD_NOT_MET",
            "⏱ 완료",
        ]
    )
    path = save_subprocess_log(
        tmp_path / "logs",
        script_stem="screener",
        trade_date="20260717",
        session="pm",
        market="SP500",
        run_id="test-run",
        stdout=stdout,
        stderr="err",
    )
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    assert "1차 필터링" in text
    assert "===== STDERR =====" in text
    summary = extract_screener_summary_lines(stdout)
    assert any("EMPTY" in x for x in summary)
    assert any("스코어 분포" in x for x in summary)


# ── 14. Atomic meta write ────────────────────────────────────────────

def test_atomic_write_json(tmp_path):
    p = tmp_path / "screener_run_meta_20260717_pm_SP500.json"
    atomic_write_json(p, {"status": "SUCCESS", "candidate_count": 0})
    assert p.exists()
    assert json.loads(p.read_text(encoding="utf-8"))["status"] == "SUCCESS"
    # no leftover temp
    assert list(tmp_path.glob(".*.tmp")) == []


# ── 15. Funnel invariant violation detection ──────────────────────────

def test_funnel_invariant_violation_repaired():
    funnel = FunnelRecorder()
    funnel.stages = [
        StageResult("MIN_SCORE", "APPLIED", 16, 0),
        StageResult("MOMENTUM", "APPLIED", 16, 16),  # broken: input != prev out
    ]
    # Force bad dropped
    funnel.stages[1].dropped_count = -16
    data, repaired = funnel.sanitize_for_storage()
    assert repaired is True
    assert any(f["code"] == "SCREENER_FUNNEL_INVARIANT_VIOLATION" for f in funnel.findings)
    assert data[1]["input_count"] == data[0]["output_count"]
    assert data[1]["dropped_count"] >= 0


def test_score_distribution_helper():
    d = score_distribution([0.45, 0.43, 0.40, 0.30])
    assert d["count"] == 4
    assert d["max"] == 0.45


# ── integrated_manager helpers ───────────────────────────────────────

def test_integrated_manager_summary_helpers_via_ops(tmp_path):
    """Cover the same helpers integrated_manager uses without importing it."""
    stdout = "hello\n스코어 분포: 평균=0.3\n최종 후보 수=0\nEMPTY reason=X\n"
    summary_lines = extract_screener_summary_lines(stdout)
    assert any("스코어 분포" in x for x in summary_lines)
    path = save_subprocess_log(
        tmp_path / "logs",
        script_stem="screener",
        trade_date="20260717",
        session="pm",
        market="SP500",
        run_id="rid",
        stdout="stdout-body",
        stderr="stderr-body",
    )
    assert path.exists()
    assert "stdout-body" in path.read_text(encoding="utf-8")
