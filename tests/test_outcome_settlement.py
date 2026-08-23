"""Focused tests for candidate outcome settlement / backfill."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_outcomes import (  # noqa: E402
    backfill_candidate_outcomes,
    calculate_forward_return,
    classify_sample_statuses,
    clear_ohlcv_cache,
    get_forward_trading_dates,
    normalize_trade_date,
    ohlcv_to_closes_by_date,
    settle_observation_outcome,
    spearman_corr,
)
from screener_quality import upsert_observation_ledger  # noqa: E402
from gpt_quality import evaluate_gpt_incremental_value, join_gpt_with_outcomes  # noqa: E402


def _kis_style_ohlcv(dates: List[str], closes: List[float]) -> pd.DataFrame:
    """Simulate KIS normalize_kis_ohlcv output: RangeIndex + Date column."""
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": closes,
            "High": [c * 1.01 for c in closes],
            "Low": [c * 0.99 for c in closes],
            "Close": closes,
            "Volume": [1_000_000] * len(closes),
        }
    )


# ── Date normalization ───────────────────────────────────────────────

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("20260730", "20260730"),
        ("2026-07-30", "20260730"),
        (date(2026, 7, 30), "20260730"),
        (datetime(2026, 7, 30, 15, 30, 0), "20260730"),
        (datetime(2026, 7, 30, 15, 30, tzinfo=timezone(timedelta(hours=9))), "20260730"),
        ("2026-07-30T21:00:00+09:00", "20260730"),
    ],
)
def test_normalize_trade_date(raw, expected):
    assert normalize_trade_date(raw) == expected


# ── OHLCV Date column (root-cause regression) ────────────────────────

def test_ohlcv_date_column_not_range_index():
    df = _kis_style_ohlcv(
        ["20260730", "20260731", "20260803", "20260804", "20260805"],
        [100.0, 101.0, 102.0, 103.0, 104.0],
    )
    bars = ohlcv_to_closes_by_date(df)
    assert "0" not in bars
    assert "20260730" in bars
    assert bars["20260730"]["close"] == 100.0


def test_settlement_with_kis_style_dataframe():
    """Root cause: RangeIndex must not be treated as YYYYMMDD."""
    dates = [
        "20260730",
        "20260731",
        "20260803",
        "20260804",
        "20260805",
        "20260806",
        "20260807",
        "20260810",
        "20260811",
        "20260812",
        "20260813",
    ]
    closes = [100 + i for i in range(len(dates))]
    df = _kis_style_ohlcv(dates, closes)
    obs = {
        "ticker": "AAA",
        "trade_date": "20260730",
        "reference_price": 100.0,
        "trusted_for_analysis": True,
        "candidate_type": "PRODUCTION",
    }
    settled = settle_observation_outcome(
        obs, close_series=df, as_of_trade_date="20260813"
    )
    assert settled["maturity"]["1d"] is True
    assert settled["maturity"]["3d"] is True
    assert settled["maturity"]["5d"] is True
    assert settled["maturity"]["10d"] is True
    assert settled["return_1d_pct"] == 1.0  # 101/100 - 1
    assert settled["outcome_status"] == "FULLY_MATURED"


# ── Forward trading days ─────────────────────────────────────────────

def test_weekend_crossing_friday_to_monday():
    dates = get_forward_trading_dates("20260807", 1)  # Friday
    assert dates[0] == "20260810"


def test_forward_from_price_index_skips_gap():
    idx = ["20260807", "20260810", "20260811", "20260812"]
    fwd = get_forward_trading_dates("20260807", 3, price_index=idx)
    assert fwd == ["20260810", "20260811", "20260812"]


def test_partial_maturity_when_future_short():
    dates = ["20260820", "20260821", "20260822"]
    closes = [100.0, 101.0, 102.0]
    df = _kis_style_ohlcv(dates, closes)
    obs = {
        "ticker": "BBB",
        "trade_date": "20260820",
        "reference_price": 100.0,
        "trusted_for_analysis": True,
    }
    settled = settle_observation_outcome(
        obs, close_series=df, as_of_trade_date="20260822"
    )
    assert settled["maturity"]["1d"] is True
    assert settled["maturity"]["3d"] is False
    assert settled["maturity"]["5d"] is False
    assert settled["outcome_status"] == "PARTIALLY_MATURED"
    assert settled["return_1d_pct"] == 1.0


def test_horizons_1_3_5_10():
    # 1 + 10 forward trading days
    base = datetime(2026, 7, 30)
    dates = []
    d = base
    while len(dates) < 15:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    closes = [100.0 + i for i in range(len(dates))]
    df = _kis_style_ohlcv(dates, closes)
    obs = {
        "ticker": "CCC",
        "trade_date": dates[0],
        "reference_price": 100.0,
    }
    settled = settle_observation_outcome(
        obs, close_series=df, as_of_trade_date=dates[-1]
    )
    assert settled["maturity"]["1d"]
    assert settled["maturity"]["3d"]
    assert settled["maturity"]["5d"]
    assert settled["maturity"]["10d"]
    assert calculate_forward_return(100.0, closes[1]) == settled["return_1d_pct"]


# ── Reference price fallback ─────────────────────────────────────────

def test_missing_reference_uses_ohlcv_close():
    df = _kis_style_ohlcv(
        ["20260730", "20260731", "20260803"],
        [50.0, 55.0, 60.0],
    )
    obs = {"ticker": "DDD", "trade_date": "20260730"}  # no reference_price
    settled = settle_observation_outcome(
        obs, close_series=df, as_of_trade_date="20260803"
    )
    assert settled["reference_price"] == 50.0
    assert settled["reference_price_source"] == "OHLCV_CLOSE_BACKFILL"
    assert settled["maturity"]["1d"] is True


def test_return_without_high_low():
    df = pd.DataFrame({"Date": ["20260730", "20260731"], "Close": [10.0, 11.0]})
    obs = {"ticker": "EEE", "trade_date": "20260730", "reference_price": 10.0}
    settled = settle_observation_outcome(
        obs, close_series=df, as_of_trade_date="20260731"
    )
    assert settled["return_1d_pct"] == 10.0
    assert settled["maturity"]["1d"] is True


# ── Persistence / idempotency ────────────────────────────────────────

def test_ledger_settlement_persist_and_idempotent(tmp_path):
    ledger = tmp_path / "obs.jsonl"
    dates = [
        "20260730",
        "20260731",
        "20260803",
        "20260804",
        "20260805",
        "20260806",
        "20260807",
    ]
    closes = [100 + i for i in range(len(dates))]
    df = _kis_style_ohlcv(dates, closes)

    def loader(ticker, start, end):
        return df

    rows = [
        {
            "decision_run_id": "r1",
            "ticker": "AAA",
            "candidate_type": "PRODUCTION",
            "trade_date": "20260730",
            "reference_price": 100.0,
            "decision_score": 0.5,
            "trusted_for_analysis": True,
            "outcome_status": "PENDING",
        }
    ]
    upsert_observation_ledger(ledger, rows)
    clear_ohlcv_cache()
    settled = backfill_candidate_outcomes(
        rows, as_of_trade_date="20260807", price_loader=loader, only_trusted=False
    )
    upsert_observation_ledger(ledger, settled)
    loaded = [json.loads(l) for l in ledger.read_text().splitlines() if l.strip()]
    assert len(loaded) == 1
    assert loaded[0]["maturity"]["1d"] is True
    assert loaded[0]["return_1d_pct"] is not None

    # Rerun must not regress to PENDING or duplicate
    clear_ohlcv_cache()
    settled2 = backfill_candidate_outcomes(
        loaded, as_of_trade_date="20260807", price_loader=loader, only_trusted=False
    )
    upsert_observation_ledger(ledger, settled2)
    loaded2 = [json.loads(l) for l in ledger.read_text().splitlines() if l.strip()]
    assert len(loaded2) == 1
    assert loaded2[0]["outcome_status"] != "PENDING"
    assert loaded2[0]["return_1d_pct"] == loaded[0]["return_1d_pct"]


def test_stub_rebuild_does_not_regress_matured(tmp_path):
    ledger = tmp_path / "obs.jsonl"
    matured = {
        "decision_run_id": "r1",
        "ticker": "AAA",
        "candidate_type": "PRODUCTION",
        "trade_date": "20260730",
        "outcome_status": "FULLY_MATURED",
        "return_1d_pct": 1.5,
        "maturity": {"1d": True, "3d": True, "5d": True, "10d": True},
        "trusted_for_analysis": True,
    }
    upsert_observation_ledger(ledger, [matured])
    stub = {
        "decision_run_id": "r1",
        "ticker": "AAA",
        "candidate_type": "PRODUCTION",
        "trade_date": "20260730",
        "outcome_status": "PENDING",
        "return_1d_pct": None,
        "maturity": {"1d": False, "3d": False, "5d": False, "10d": False},
        "trusted_for_analysis": True,
    }
    upsert_observation_ledger(ledger, [stub])
    loaded = [json.loads(l) for l in ledger.read_text().splitlines() if l.strip()][0]
    assert loaded["outcome_status"] == "FULLY_MATURED"
    assert loaded["return_1d_pct"] == 1.5


# ── KIS cache ────────────────────────────────────────────────────────

def test_repeated_ticker_cache_single_fetch():
    clear_ohlcv_cache()
    calls = {"n": 0}
    dates = ["20260730", "20260731", "20260803", "20260804", "20260805"]
    df = _kis_style_ohlcv(dates, [100, 101, 102, 103, 104])

    def loader(ticker, start, end):
        calls["n"] += 1
        return df

    rows = [
        {
            "ticker": "JPM",
            "trade_date": d,
            "reference_price": 100.0,
            "trusted_for_analysis": True,
            "candidate_type": "PRODUCTION",
            "decision_run_id": f"r-{d}",
        }
        for d in ("20260730", "20260731", "20260803")
    ]
    settled = backfill_candidate_outcomes(
        rows, as_of_trade_date="20260805", price_loader=loader, only_trusted=False
    )
    assert calls["n"] == 1  # prefetch once per ticker
    assert any((r.get("maturity") or {}).get("1d") for r in settled)


# ── Sample status / Spearman / GPT ───────────────────────────────────

def test_outcome_sample_status_changes_when_matured():
    bits0 = classify_sample_statuses(
        trading_days=20, matured_1d=0, matured_5d=0, matured_10d=0
    )
    assert bits0["outcome_sample_status"] == "NO_SETTLED_OUTCOMES"
    bits1 = classify_sample_statuses(
        trading_days=20, matured_1d=5, matured_5d=5, matured_10d=0
    )
    assert bits1["outcome_sample_status"] != "NO_SETTLED_OUTCOMES"


def test_spearman_non_null_with_pairs():
    xs = [0.42, 0.48, 0.52, 0.56, 0.60]
    ys = [1.0, 2.0, 1.5, 3.0, 2.5]
    assert spearman_corr(xs, ys) is not None


def test_gpt_alpha_non_null_with_matured_5d():
    gpt = [
        {"ticker": "A", "trade_date": "20260801", "gpt_decision": "BUY"},
        {"ticker": "B", "trade_date": "20260801", "gpt_decision": "HOLD"},
        {"ticker": "C", "trade_date": "20260801", "gpt_decision": "BUY"},
        {"ticker": "D", "trade_date": "20260801", "gpt_decision": "HOLD"},
    ]
    for i, g in enumerate(gpt):
        g["return_5d_pct"] = [2.0, -1.0, 3.0, 0.0][i]
    # pad to avoid INSUFFICIENT — alpha still computed when means exist
    joined = join_gpt_with_outcomes(gpt, [])
    for j, r in zip(joined, [2.0, -1.0, 3.0, 0.0]):
        j["return_5d_pct"] = r
    report = evaluate_gpt_incremental_value(joined)
    assert report["gpt_incremental_alpha_5d"] is not None
    assert report["gpt_buy_count"] == 2
    assert report["gpt_hold_count"] == 2


def test_twenty_day_fixture_old_observation_matures():
    """Synthetic 20-day calendar: early observation must mature 1/3/5D."""
    base = datetime(2026, 7, 27)
    dates = []
    d = base
    while len(dates) < 25:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    closes = [100.0 + 0.5 * i for i in range(len(dates))]
    df = _kis_style_ohlcv(dates, closes)
    obs = {
        "ticker": "FIX",
        "trade_date": dates[0],
        "reference_price": closes[0],
        "trusted_for_analysis": True,
        "candidate_type": "PRODUCTION",
    }
    settled = settle_observation_outcome(
        obs, close_series=df, as_of_trade_date=dates[-1]
    )
    assert settled["maturity"]["1d"] and settled["maturity"]["3d"] and settled["maturity"]["5d"]
    assert settled["outcome_status"] in ("PARTIALLY_MATURED", "FULLY_MATURED")
