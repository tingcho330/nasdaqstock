"""Quality Report scope vs full-ledger settlement + per-type calibration."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_quality import (  # noqa: E402
    apply_outcome_stats_to_report,
    build_score_calibration_by_candidate_type,
    filter_report_scoped_observations,
    is_eligible_for_score_calibration,
    pick_debug_settle_observation,
    upsert_observation_ledger,
)
from screener_outcomes import backfill_candidate_outcomes, clear_ohlcv_cache  # noqa: E402
from gpt_quality import discover_gpt_trades_files, load_gpt_observations  # noqa: E402


START = "20260727"
END = "20260821"
MARKET = "SP500"
SESSION = "pm"


def _obs(
    *,
    trade_date: str,
    ticker: str = "AAA",
    candidate_type: str = "PRODUCTION",
    market: str = MARKET,
    session: str = SESSION,
    trusted: bool = True,
    score: float = 0.50,
    ret5: float | None = 1.0,
    fundamental_parity_status: str | None = None,
    decision_run_id: str | None = None,
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "decision_run_id": decision_run_id or f"{trade_date}-{ticker}-{candidate_type}",
        "ticker": ticker,
        "candidate_type": candidate_type,
        "trade_date": trade_date,
        "market": market,
        "session": session,
        "trusted_for_analysis": trusted,
        "decision_score": score,
        "return_5d_pct": ret5,
        "return_1d_pct": ret5,
        "return_3d_pct": ret5,
        "return_10d_pct": ret5,
        "outcome_status": "FULLY_MATURED" if ret5 is not None else "PENDING",
        "maturity": {
            "1d": ret5 is not None,
            "3d": ret5 is not None,
            "5d": ret5 is not None,
            "10d": ret5 is not None,
        },
    }
    if fundamental_parity_status is not None:
        row["fundamental_parity_status"] = fundamental_parity_status
    return row


def _base_report(**extra: Any) -> Dict[str, Any]:
    r: Dict[str, Any] = {
        "market": MARKET,
        "session": SESSION,
        "start_trade_date": START,
        "end_trade_date": END,
        "trading_days": 20,
    }
    r.update(extra)
    return r


def test_report_excludes_trade_date_before_scope():
    rows = [
        _obs(trade_date="20260722", ticker="MU", ret5=9.0),
        _obs(trade_date="20260728", ticker="AAA", ret5=1.0),
    ]
    report = apply_outcome_stats_to_report(_base_report(), rows)
    assert report["outcome_counts"]["ledger_total_rows"] == 2
    assert report["outcome_counts"]["report_scoped_rows"] == 1
    assert report["outcome_counts"]["report_trusted_rows"] == 1
    assert report["candidate_performance"]["PRODUCTION"][5]["observations"] == 1
    assert report["candidate_performance"]["PRODUCTION"][5]["mean_return"] == 1.0


def test_report_excludes_trade_date_after_scope():
    rows = [
        _obs(trade_date="20260822", ticker="BBB", ret5=5.0),
        _obs(trade_date="20260820", ticker="AAA", ret5=2.0),
    ]
    report = apply_outcome_stats_to_report(_base_report(), rows)
    assert report["outcome_counts"]["report_scoped_rows"] == 1
    assert report["candidate_performance"]["PRODUCTION"][5]["mean_return"] == 2.0


def test_report_excludes_different_market():
    rows = [
        _obs(trade_date="20260801", market="NASDAQ", ret5=8.0),
        _obs(trade_date="20260801", market="SP500", ret5=1.5),
    ]
    report = apply_outcome_stats_to_report(_base_report(), rows)
    assert report["outcome_counts"]["report_scoped_rows"] == 1
    assert report["candidate_performance"]["PRODUCTION"][5]["mean_return"] == 1.5


def test_report_excludes_different_session():
    rows = [
        _obs(trade_date="20260801", session="am", ret5=7.0),
        _obs(trade_date="20260801", session="pm", ret5=1.2),
    ]
    report = apply_outcome_stats_to_report(_base_report(), rows)
    assert report["outcome_counts"]["report_scoped_rows"] == 1
    assert report["candidate_performance"]["PRODUCTION"][5]["mean_return"] == 1.2


def test_settlement_can_update_out_of_scope_rows(tmp_path):
    """Settlement may backfill rows outside the Quality Report window."""
    import pandas as pd

    dates = [
        "20260722",
        "20260723",
        "20260724",
        "20260727",
        "20260728",
        "20260729",
        "20260730",
        "20260731",
    ]
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": [100 + i for i in range(len(dates))],
            "High": [101 + i for i in range(len(dates))],
            "Low": [99 + i for i in range(len(dates))],
            "Close": [100 + i for i in range(len(dates))],
            "Volume": [1_000_000] * len(dates),
        }
    )

    def loader(ticker, start, end):
        return df

    out_of_scope = {
        "decision_run_id": "old",
        "ticker": "MU",
        "candidate_type": "PRODUCTION",
        "trade_date": "20260722",
        "market": MARKET,
        "session": SESSION,
        "reference_price": 100.0,
        "trusted_for_analysis": True,
        "outcome_status": "PENDING",
    }
    clear_ohlcv_cache()
    settled = backfill_candidate_outcomes(
        [out_of_scope],
        as_of_trade_date="20260731",
        price_loader=loader,
        only_trusted=False,
    )
    assert settled[0]["trade_date"] == "20260722"
    assert (settled[0].get("maturity") or {}).get("1d") is True
    assert settled[0].get("return_1d_pct") is not None

    ledger = tmp_path / "obs.jsonl"
    upsert_observation_ledger(ledger, settled)
    report = apply_outcome_stats_to_report(_base_report(), settled)
    # Settled outside window must not enter report stats
    assert report["outcome_counts"]["ledger_total_rows"] == 1
    assert report["outcome_counts"]["report_scoped_rows"] == 0
    assert report["outcome_counts"]["report_trusted_rows"] == 0


def test_report_aggregation_uses_scope_only():
    rows = [
        _obs(trade_date="20260722", ret5=99.0),
        _obs(trade_date="20260801", ret5=1.0),
        _obs(trade_date="20260815", ret5=3.0),
        _obs(trade_date="20260830", ret5=50.0),
    ]
    scoped = filter_report_scoped_observations(
        rows,
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    assert {r["trade_date"] for r in scoped} == {"20260801", "20260815"}
    report = apply_outcome_stats_to_report(_base_report(), rows)
    assert report["outcome_counts"]["report_trusted_rows"] == 2
    assert report["candidate_performance"]["PRODUCTION"][5]["mean_return"] == 2.0


def test_debug_settle_sample_inside_report_scope():
    rows = [
        _obs(trade_date="20260722", ticker="MU"),
        _obs(trade_date="20260728", ticker="OLD"),
        _obs(trade_date="20260805", ticker="NEW"),
        _obs(trade_date="20260801", ticker="X", candidate_type="ELIGIBLE_SHADOW"),
    ]
    pick = pick_debug_settle_observation(
        rows,
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    assert pick is not None
    assert pick["trade_date"] >= START
    assert pick["trade_date"] <= END
    assert pick["candidate_type"] == "PRODUCTION"
    assert pick["ticker"] == "OLD"  # oldest PRODUCTION inside scope


def test_production_spearman_uses_production_only():
    rows = [
        _obs(trade_date="20260801", ticker="P1", score=0.42, ret5=1.0),
        _obs(trade_date="20260802", ticker="P2", score=0.48, ret5=2.0),
        _obs(trade_date="20260803", ticker="P3", score=0.52, ret5=3.0),
        _obs(trade_date="20260804", ticker="P4", score=0.56, ret5=4.0),
        _obs(
            trade_date="20260801",
            ticker="H1",
            candidate_type="HIGH_CONVICTION_SHADOW",
            score=0.90,
            ret5=-10.0,
        ),
        _obs(
            trade_date="20260802",
            ticker="H2",
            candidate_type="HIGH_CONVICTION_SHADOW",
            score=0.91,
            ret5=-20.0,
        ),
        _obs(
            trade_date="20260803",
            ticker="H3",
            candidate_type="HIGH_CONVICTION_SHADOW",
            score=0.92,
            ret5=-30.0,
        ),
        _obs(
            trade_date="20260804",
            ticker="H4",
            candidate_type="HIGH_CONVICTION_SHADOW",
            score=0.93,
            ret5=-40.0,
        ),
    ]
    report = apply_outcome_stats_to_report(_base_report(), rows)
    prod_sp = report["score_calibration"]["PRODUCTION"]["spearman_5d"]
    hc_sp = report["score_calibration"]["HIGH_CONVICTION_SHADOW"]["spearman_5d"]
    assert prod_sp is not None
    assert hc_sp is not None
    # Opposite monotonicity → must not be a single mixed Spearman
    assert prod_sp > 0
    assert hc_sp < 0
    assert report["candidate_performance"]["PRODUCTION"]["spearman_score_5d"] == prod_sp


def test_candidate_type_spearman_isolation():
    calib = build_score_calibration_by_candidate_type(
        [
            _obs(trade_date="20260801", ticker="A", score=0.4, ret5=1.0),
            _obs(trade_date="20260802", ticker="B", score=0.5, ret5=2.0),
            _obs(trade_date="20260803", ticker="C", score=0.6, ret5=3.0),
            _obs(
                trade_date="20260801",
                ticker="E1",
                candidate_type="ELIGIBLE_SHADOW",
                score=0.4,
                ret5=3.0,
            ),
            _obs(
                trade_date="20260802",
                ticker="E2",
                candidate_type="ELIGIBLE_SHADOW",
                score=0.5,
                ret5=2.0,
            ),
            _obs(
                trade_date="20260803",
                ticker="E3",
                candidate_type="ELIGIBLE_SHADOW",
                score=0.6,
                ret5=1.0,
            ),
        ]
    )
    assert "PRODUCTION" in calib
    assert "ELIGIBLE_SHADOW" in calib
    assert calib["PRODUCTION"]["spearman_5d"] != calib["ELIGIBLE_SHADOW"]["spearman_5d"]


def test_legacy_liquidity_excluded_from_calibration():
    rows = [
        _obs(
            trade_date="20260801",
            ticker="L1",
            candidate_type="LIQUIDITY_SHADOW",
            score=0.55,
            ret5=5.0,
            fundamental_parity_status="LEGACY_UNCORRECTED",
        ),
        _obs(
            trade_date="20260802",
            ticker="L2",
            candidate_type="LIQUIDITY_SHADOW",
            score=0.60,
            ret5=6.0,
            fundamental_parity_status="CHECK_REQUIRED",
        ),
        _obs(trade_date="20260801", ticker="P1", score=0.48, ret5=1.0),
        _obs(trade_date="20260802", ticker="P2", score=0.50, ret5=2.0),
        _obs(trade_date="20260803", ticker="P3", score=0.52, ret5=3.0),
    ]
    for r in rows:
        if r["candidate_type"] == "LIQUIDITY_SHADOW":
            assert not is_eligible_for_score_calibration(r)

    report = apply_outcome_stats_to_report(_base_report(), rows)
    # Outcome stats still count trusted liquidity rows
    assert report["candidate_performance"]["LIQUIDITY_SHADOW"][5]["observations"] == 2
    liq_cal = report["score_calibration"]["LIQUIDITY_SHADOW"]
    assert liq_cal["status"] == "EXCLUDED_LEGACY_FUNDAMENTAL_PARITY"
    assert liq_cal["spearman_5d"] is None
    assert report["candidate_performance"]["LIQUIDITY_SHADOW"][
        "spearman_calibration_status"
    ] == "EXCLUDED_LEGACY_FUNDAMENTAL_PARITY"


def test_verified_liquidity_included_in_calibration():
    rows = [
        _obs(
            trade_date="20260801",
            ticker="L1",
            candidate_type="LIQUIDITY_SHADOW",
            score=0.42,
            ret5=1.0,
            fundamental_parity_status="VERIFIED",
        ),
        _obs(
            trade_date="20260802",
            ticker="L2",
            candidate_type="LIQUIDITY_SHADOW",
            score=0.50,
            ret5=2.0,
            fundamental_parity_status="VERIFIED",
        ),
        _obs(
            trade_date="20260803",
            ticker="L3",
            candidate_type="LIQUIDITY_SHADOW",
            score=0.58,
            ret5=3.0,
            fundamental_parity_status="VERIFIED",
        ),
    ]
    calib = build_score_calibration_by_candidate_type(rows)
    assert calib["LIQUIDITY_SHADOW"]["status"] == "OK"
    assert calib["LIQUIDITY_SHADOW"]["spearman_5d"] is not None


def test_gpt_date_from_to_isolation(tmp_path):
    out = tmp_path / "output"
    out.mkdir()
    # Outside range
    (out / "gpt_trades_20260720_pm_SP500.json").write_text(
        json.dumps(
            [{"rank": 1, "결정": "매수", "stock_info": {"Ticker": "OLD", "Score": 0.5}}]
        ),
        encoding="utf-8",
    )
    # Inside range
    (out / "gpt_trades_20260801_pm_SP500.json").write_text(
        json.dumps(
            [
                {"rank": 1, "결정": "매수", "stock_info": {"Ticker": "AAA", "Score": 0.5}},
                {"rank": 2, "결정": "보류", "stock_info": {"Ticker": "BBB", "Score": 0.5}},
            ]
        ),
        encoding="utf-8",
    )
    (out / "gpt_trades_20260810_pm_SP500.json").write_text(
        json.dumps(
            [{"rank": 1, "결정": "보류", "stock_info": {"Ticker": "CCC", "Score": 0.5}}]
        ),
        encoding="utf-8",
    )
    # After range
    (out / "gpt_trades_20260825_pm_SP500.json").write_text(
        json.dumps(
            [{"rank": 1, "결정": "매수", "stock_info": {"Ticker": "NEW", "Score": 0.5}}]
        ),
        encoding="utf-8",
    )

    found = discover_gpt_trades_files(
        out, market="SP500", session="pm", date_from=START, date_to=END
    )
    dates = sorted(p.name.split("_")[2] for p in found)
    assert dates == ["20260801", "20260810"]

    obs = load_gpt_observations(
        out, market="SP500", session="pm", date_from=START, date_to=END
    )
    assert len(obs) == 3
    assert {o["trade_date"] for o in obs} == {"20260801", "20260810"}
    assert sum(1 for o in obs if o["gpt_decision"] == "BUY") == 1
    assert sum(1 for o in obs if o["gpt_decision"] == "HOLD") == 2


def test_production_policy_unchanged():
    from utils import strip_jsonc_comments

    raw = (ROOT / "config" / "config.json").read_text(encoding="utf-8")
    cfg = json.loads(strip_jsonc_comments(raw))
    sp = cfg["screener_params"]
    assert sp["min_score_threshold"] == 0.48
    assert sp["score_threshold_policy"]["static_threshold"] == 0.48
    assert sp["min_trading_value_5d_avg_us"] == 5_000_000_000
    assert sp["amount5d_policy"]["static_threshold"] == 5_000_000_000


def test_untrusted_excluded_from_report_trusted_rows():
    rows = [
        _obs(trade_date="20260801", trusted=False, ret5=9.0),
        _obs(trade_date="20260801", ticker="OK", ret5=1.0),
    ]
    report = apply_outcome_stats_to_report(_base_report(), rows)
    assert report["outcome_counts"]["report_scoped_rows"] == 2
    assert report["outcome_counts"]["report_trusted_rows"] == 1
