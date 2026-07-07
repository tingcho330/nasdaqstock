"""performance_review artifact temporal metadata / stale checks."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import os  # noqa: E402

os.environ.setdefault("OUTPUT_DIR", str(ROOT / "output_test"))

from performance_review import (  # noqa: E402
    ReviewArtifacts,
    _check_stale_artifacts,
    build_kis_endpoint_review,
    collect_artifacts,
    extract_artifact_temporal_metadata,
    run_performance_review,
)


def _artifacts(tmp_path: Path, review_date: str = "20260707") -> ReviewArtifacts:
    return ReviewArtifacts(
        market="SP500",
        start_date=review_date,
        review_date=review_date,
        period="daily",
        session="pm",
        output_dir=tmp_path,
        db_path=tmp_path / "trading_data.db",
        account_snapshot_paths=[],
        order_reconcile_paths=[],
        trade_rows=[],
        logs_text="",
    )


def test_list_screener_no_exception(tmp_path):
    path = tmp_path / "screener_candidates_20260707_pm_SP500.json"
    path.write_text(
        json.dumps([{"ticker": "AAPL", "updated_at": "2026-07-07T10:00:00+09:00"}]),
        encoding="utf-8",
    )
    art = _artifacts(tmp_path)
    findings = _check_stale_artifacts(art, strict=True)
    assert isinstance(findings, list)


def test_filename_metadata_screener_candidates(tmp_path):
    path = tmp_path / "screener_candidates_20260701_pm_SP500.json"
    meta = extract_artifact_temporal_metadata(path, [{"ticker": "AAPL"}])
    assert meta["artifact_date"] == "20260701"
    assert meta["artifact_session"] == "pm"
    assert meta["artifact_market"] == "SP500"
    assert meta["root_type"] == "list"
    assert meta["item_count"] == 1


def test_list_item_updated_at_as_generated_at(tmp_path):
    path = tmp_path / "screener_scores_20260701_pm_SP500.json"
    payload = [
        {"ticker": "AAPL", "updated_at": "2026-07-01T08:00:00+09:00"},
        {"ticker": "MSFT", "updated_at": "2026-07-01T12:00:00+09:00"},
    ]
    meta = extract_artifact_temporal_metadata(path, payload)
    assert "2026-07-01T12:00:00+09:00" in meta["generated_at"]


def test_empty_list_finding(tmp_path):
    path = tmp_path / "screener_holdings_20260701_pm_SP500.json"
    path.write_text("[]", encoding="utf-8")
    art = _artifacts(tmp_path)
    findings = _check_stale_artifacts(art, strict=True)
    titles = {f.title for f in findings}
    assert "ARTIFACT_EMPTY_LIST" in titles


def test_unsupported_root_finding(tmp_path):
    path = tmp_path / "screener_candidates_20260701_pm_SP500.json"
    path.write_text('"not-a-dict-or-list"', encoding="utf-8")
    art = _artifacts(tmp_path)
    findings = _check_stale_artifacts(art, strict=True)
    titles = {f.title for f in findings}
    assert "ARTIFACT_UNSUPPORTED_JSON_ROOT" in titles


def test_dict_root_metadata(tmp_path):
    path = tmp_path / "market_state_20260707_pm_SP500.json"
    payload = {
        "trade_date": "20260707",
        "session": "pm",
        "market": "SP500",
        "generated_at_kst": "2026-07-07T10:00:00+09:00",
    }
    meta = extract_artifact_temporal_metadata(path, payload)
    assert meta["root_type"] == "dict"
    assert meta["artifact_date"] == "20260707"
    assert meta["generated_at_kst"] == "2026-07-07T10:00:00+09:00"


def test_list_screener_same_review_date_not_stale(tmp_path):
    for name in (
        "screener_candidates_20260707_pm_SP500.json",
        "screener_candidates_full_20260707_pm_SP500.json",
        "screener_holdings_20260707_pm_SP500.json",
        "screener_scores_20260707_pm_SP500.json",
    ):
        p = tmp_path / name
        p.write_text(json.dumps([{"ticker": "AAPL", "score": 0.8}]), encoding="utf-8")
    art = _artifacts(tmp_path, review_date="20260707")
    findings = _check_stale_artifacts(art, strict=True)
    assert "ARTIFACT_DATE_STALE" not in {f.title for f in findings}


def test_build_kis_review_with_list_screener_no_crash(tmp_path):
    (tmp_path / "screener_candidates_20260701_pm_SP500.json").write_text(
        json.dumps([{"ticker": "AAPL"}]), encoding="utf-8",
    )
    art = collect_artifacts("SP500", "20260707", "20260707", "pm", tmp_path, include_logs=False)
    ker = build_kis_endpoint_review(art, strict=True, cfg={}, include_logs=False)
    assert ker.endpoint_health_score >= 0


def test_run_performance_review_no_analysis_failed(tmp_path, monkeypatch):
    review_out = tmp_path / "performance_reviews"
    review_out.mkdir(parents=True, exist_ok=True)
    for name in (
        "screener_candidates_20260701_pm_SP500.json",
        "screener_candidates_full_20260701_pm_SP500.json",
    ):
        (tmp_path / name).write_text(json.dumps([{"ticker": "AAPL"}]), encoding="utf-8")
    (tmp_path / "market_state_20260701_pm_SP500.json").write_text(
        json.dumps({"date": "20260701", "session": "pm", "market": "SP500"}),
        encoding="utf-8",
    )

    result = run_performance_review(
        "SP500",
        "20260707",
        "20260707",
        period="daily",
        session="pm",
        strict_kis=True,
        include_logs=True,
        send_discord=False,
        json_only=True,
        output_dir=tmp_path,
        review_cfg={"output_dir": "performance_reviews", "send_discord": False},
    )
    assert result.context.get("review_error") is None
    report = review_out / "performance_review_SP500_daily_20260707.json"
    assert report.exists()
