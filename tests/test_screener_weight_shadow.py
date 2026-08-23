"""Tests for read-only prospective WEIGHT_SHADOW diagnostics."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("OUTPUT_DIR", str(ROOT / "output"))
os.environ.setdefault("CACHE_DIR", str(ROOT / "output" / "cache"))
os.environ.setdefault("CONFIG_PATH", str(ROOT / "config" / "config.json"))

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_artifacts import PostRunDiagnosticsWriter, sha256_file  # noqa: E402
from screener_weight_simulation import PRODUCTION_THRESHOLD, SCENARIOS  # noqa: E402
from screener_weight_shadow import (  # noqa: E402
    PROSPECTIVE_START_DEFAULT,
    STATUS_RESEARCH_ONLY,
    build_prospective_weight_shadow_quality,
    build_weight_shadow_payload,
    should_run_weight_shadow,
    write_prospective_quality_report,
)
from screener_full_universe_replay import (  # noqa: E402
    pipeline_params_from_config,
)


def _factors(**kwargs) -> Dict[str, float]:
    base = {
        "fin_score": 0.5,
        "tech_score": 0.5,
        "market_score": 0.5,
        "sector_score": 0.5,
        "vol_kki": 0.5,
        "pos_52w": 0.5,
    }
    base.update(kwargs)
    return base


def _score_row(ticker: str, *, score: float, production_candidate: bool = False, **fac) -> Dict[str, Any]:
    return {
        "ticker": ticker,
        "score": score,
        "Score": score,
        "sector": "Tech",
        "issuer_group": ticker,
        "held": False,
        "momentum_pass": True,
        "volatility_pass": True,
        "production_candidate": production_candidate,
        "factors": _factors(**fac),
        "fin_score": fac.get("fin_score", 0.5),
        "tech_score": fac.get("tech_score", 0.5),
        "market_score": fac.get("market_score", 0.5),
        "sector_score": fac.get("sector_score", 0.5),
        "vol_kki": fac.get("vol_kki", 0.5),
        "pos_52w": fac.get("pos_52w", 0.5),
    }


def test_should_run_weight_shadow_gates():
    pol = {
        "enabled": True,
        "markets": ["SP500"],
        "sessions": ["pm"],
        "prospective_start_trade_date": "20260824",
        "used_by_trader": False,
    }
    assert should_run_weight_shadow(
        pol, market="SP500", session="pm", trade_date="20260824", run_mode="DECISION"
    )
    assert not should_run_weight_shadow(
        pol, market="SP500", session="pm", trade_date="20260821", run_mode="DECISION"
    )
    assert not should_run_weight_shadow(
        pol, market="KOSPI", session="pm", trade_date="20260824", run_mode="DECISION"
    )
    assert not should_run_weight_shadow(
        pol, market="SP500", session="am", trade_date="20260824", run_mode="DECISION"
    )
    assert not should_run_weight_shadow(
        {**pol, "used_by_trader": True},
        market="SP500",
        session="pm",
        trade_date="20260824",
        run_mode="DECISION",
    )


def test_build_weight_shadow_payload_structure():
    scores = [
        _score_row("AAA", score=0.55, production_candidate=True, fin_score=0.8, tech_score=0.7),
        _score_row("BBB", score=0.50, production_candidate=True, fin_score=0.7, tech_score=0.6),
        _score_row("CCC", score=0.30, production_candidate=False, fin_score=0.9, vol_kki=0.9, tech_score=0.1),
    ]
    payload = build_weight_shadow_payload(
        scores_records=scores,
        production_tickers={"AAA", "BBB"},
        trade_date="20260824",
        market="SP500",
        session="pm",
        source_run_id="run-test",
        market_state=None,
        market_state_payload={"regime": "sideways", "volatility": "medium"},
        baseline_weights=dict(SCENARIOS["A_BASELINE"]),
        shadow_weights=dict(SCENARIOS["D_TECH_POS_DOWN"]),
        production_threshold=PRODUCTION_THRESHOLD,
        research_threshold=0.40,
        pipeline_params=pipeline_params_from_config({"screener_params": {}}),
        prospective_start_trade_date=PROSPECTIVE_START_DEFAULT,
    )
    assert payload["diagnostic_only"] is True
    assert payload["used_by_trader"] is False
    assert payload["production_policy"]["scenario"] == "A_BASELINE"
    assert payload["production_policy"]["threshold"] == 0.48
    assert payload["shadow_policy"]["scenario"] == "D_TECH_POS_DOWN"
    assert payload["shadow_policy"]["threshold"] == 0.40
    assert payload["research_status"] == STATUS_RESEARCH_ONLY
    assert payload["production_change_recommended"] is False
    assert payload["historical_runs_excluded"] is True
    for row in payload["rows"]:
        assert "ticker" in row
        assert "factor_values" in row
        assert "production_score" in row
        assert "shadow_score" in row
        assert "migration_group" in row


def test_weight_shadow_publish_does_not_touch_decision(tmp_path: Path):
    decision = tmp_path / "runs" / "decision" / "SP500" / "20260824" / "pm" / "run1"
    decision.mkdir(parents=True)
    cand = [{"Ticker": "AAA", "Score": 0.55}]
    (decision / "screener_candidates.json").write_text(json.dumps(cand), encoding="utf-8")
    (decision / "manifest.json").write_text(json.dumps({"immutable": True}), encoding="utf-8")
    before = sha256_file(decision / "screener_candidates.json")

    class _W:
        published = True
        output_dir = tmp_path
        market = "SP500"
        trade_date = "20260824"
        session = "pm"
        run_id = "run1"
        final_dir = decision

    payload = build_weight_shadow_payload(
        scores_records=[
            _score_row("AAA", score=0.55, production_candidate=True),
            _score_row("BBB", score=0.45, production_candidate=False, vol_kki=0.95, fin_score=0.9),
        ],
        production_tickers={"AAA"},
        trade_date="20260824",
        market="SP500",
        session="pm",
        source_run_id="run1",
        market_state=None,
        market_state_payload={"regime": "bull"},
        baseline_weights=dict(SCENARIOS["A_BASELINE"]),
        shadow_weights=dict(SCENARIOS["D_TECH_POS_DOWN"]),
        production_threshold=0.48,
        research_threshold=0.40,
        pipeline_params=pipeline_params_from_config({"screener_params": {}}),
        prospective_start_trade_date="20260824",
    )

    diag = PostRunDiagnosticsWriter(
        output_dir=tmp_path,
        market="SP500",
        trade_date="20260824",
        session="pm",
        source_run_id="run1",
        source_decision_manifest_sha256=None,
        started_at_kst="2026-08-24T00:00:00+09:00",
    )
    diag.write_json("weight_shadow.json", payload)
    published = diag.publish(
        diag.build_manifest(
            status="OK",
            completed_at_kst="2026-08-24T00:01:00+09:00",
            diagnostic_types=["WEIGHT_SHADOW"],
        )
    )
    assert (published / "weight_shadow.json").exists()
    assert sha256_file(decision / "screener_candidates.json") == before
    man = json.loads((published / "diagnostics_manifest.json").read_text(encoding="utf-8"))
    assert man.get("used_by_trader") is False
    assert "WEIGHT_SHADOW" in (man.get("diagnostic_types") or [])


def test_prospective_quality_excludes_historical_and_no_early_recommend(tmp_path: Path):
    # Historical date must be excluded
    hist = (
        tmp_path
        / "post_run_diagnostics"
        / "SP500"
        / "20260821"
        / "pm"
        / "hist-run"
    )
    hist.mkdir(parents=True)
    (hist / "weight_shadow.json").write_text(
        json.dumps(
            {
                "trade_date": "20260821",
                "rows": [{"ticker": "AAA", "production_candidate": True, "shadow_candidate": True, "migration_group": "KEEP"}],
                "market_state": {"regime": "sideways"},
            }
        ),
        encoding="utf-8",
    )
    # Prospective day
    prosp = (
        tmp_path
        / "post_run_diagnostics"
        / "SP500"
        / "20260824"
        / "pm"
        / "pros-run"
    )
    prosp.mkdir(parents=True)
    (prosp / "weight_shadow.json").write_text(
        json.dumps(
            {
                "trade_date": "20260824",
                "source_run_id": "pros-run",
                "rows": [
                    {
                        "ticker": "AAA",
                        "production_candidate": True,
                        "shadow_candidate": True,
                        "migration_group": "KEEP",
                        "production_score": 0.5,
                        "shadow_score": 0.45,
                    }
                ],
                "market_state": {"regime": "bull"},
            }
        ),
        encoding="utf-8",
    )
    report = build_prospective_weight_shadow_quality(
        output_dir=tmp_path,
        market="SP500",
        session="pm",
        prospective_start_trade_date="20260824",
        as_of_trade_date="20260824",
    )
    assert report["days_observed"] == 1
    assert report["production_change_recommended"] is False
    assert report["recommendation_gate_open"] is False
    assert report["research_status"] == STATUS_RESEARCH_ONLY
    paths = write_prospective_quality_report(report, tmp_path)
    assert paths["json"].exists()
    assert paths["md"].exists()


def test_production_config_unchanged_for_weight_shadow():
    from screener_quality import load_runtime_config

    runtime = load_runtime_config(ROOT / "config" / "config.json")
    sp = runtime.get("screener_params") or {}
    assert float(sp.get("min_score_threshold")) == 0.48
    amt = (sp.get("amount5d_policy") or {}).get("static_threshold")
    assert float(amt) == 5_000_000_000
    ws = sp.get("weight_shadow_policy") or {}
    assert ws.get("used_by_trader") is False
    assert float(ws.get("research_threshold")) == 0.40
    assert ws.get("prospective_start_trade_date") == "20260824"
    assert float(sp.get("min_score_threshold")) == PRODUCTION_THRESHOLD
