"""Tests for read-only Production score factor decomposition."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_factor_analysis import (  # noqa: E402
    CORR_HIGH,
    FINDING_DC,
    FINDING_NEG,
    PRODUCTION_FACTOR_SPECS,
    analyze_factor_dataset,
    assign_quintiles,
    build_factor_observations,
    enrich_observation_with_factors,
    extract_factor_value,
    filter_production_analysis_rows,
    first_signal_only,
    factor_correlation_matrix,
    inspect_artifact_factor_schema,
    production_factor_weights,
    spearman_factor_vs_returns,
    ticker_equal_weight_rows,
    write_factor_analysis_outputs,
)
from screener_outcomes import spearman_corr  # noqa: E402


START = "20260727"
END = "20260821"
MARKET = "SP500"
SESSION = "pm"


def _score_row(
    ticker: str,
    *,
    score: float = 0.50,
    fin: float = 0.4,
    tech: float = 0.5,
    mkt: float = 0.5,
    sector: float = 0.5,
    vol: float = 0.2,
    pos: float = 0.6,
) -> Dict[str, Any]:
    return {
        "ticker": ticker,
        "score": score,
        "fin_score": fin,
        "tech_score": tech,
        "market_score": mkt,
        "sector_score": sector,
        "vol_kki": vol,
        "pos_52w": pos,
        "pattern_score": 0.9,  # present but NOT a weighted Production factor
        "schema_version": "1.4",
    }


def _obs(
    *,
    trade_date: str,
    ticker: str = "AAA",
    score: float = 0.50,
    ret5: Optional[float] = 1.0,
    candidate_type: str = "PRODUCTION",
    trusted: bool = True,
    run_id: str = "run-1",
    market: str = MARKET,
    session: str = SESSION,
    ret1: Optional[float] = None,
    ret3: Optional[float] = None,
    ret10: Optional[float] = None,
) -> Dict[str, Any]:
    r5 = ret5
    return {
        "decision_run_id": run_id,
        "source_run_id": run_id,
        "ticker": ticker,
        "candidate_type": candidate_type,
        "trade_date": trade_date,
        "market": market,
        "session": session,
        "trusted_for_analysis": trusted,
        "decision_score": score,
        "return_1d_pct": ret1 if ret1 is not None else r5,
        "return_3d_pct": ret3 if ret3 is not None else r5,
        "return_5d_pct": r5,
        "return_10d_pct": ret10 if ret10 is not None else r5,
        "max_drawdown_5d_pct": -1.0,
        "max_runup_5d_pct": 2.0,
        "outcome_status": "FULLY_MATURED" if r5 is not None else "PENDING",
        "maturity": {
            "1d": r5 is not None,
            "3d": r5 is not None,
            "5d": r5 is not None,
            "10d": r5 is not None,
        },
    }


def _write_run(
    root: Path,
    *,
    trade_date: str,
    run_id: str,
    scores: List[Dict[str, Any]],
    candidates: Optional[List[Dict[str, Any]]] = None,
    regime: str = "SIDEWAYS",
) -> Path:
    run_dir = (
        root
        / "runs"
        / "decision"
        / MARKET
        / trade_date
        / SESSION
        / run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "run_id": run_id,
        "run_mode": "DECISION",
        "market": MARKET,
        "session": SESSION,
        "trade_date": trade_date,
        "status": "SUCCESS",
        "decision_artifact": True,
        "regime": regime,
        "market_state": {"regime": regime},
        "schema_version": "3",
    }
    (run_dir / "screener_run_meta.json").write_text(
        json.dumps(meta), encoding="utf-8"
    )
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "run_mode": "DECISION",
                "market": MARKET,
                "session": SESSION,
                "trade_date": trade_date,
                "status": "SUCCESS",
                "decision_artifact": True,
                "artifacts": {
                    "screener_scores.json": "x",
                    "screener_candidates.json": "x",
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "screener_scores.json").write_text(
        json.dumps(scores), encoding="utf-8"
    )
    cands = candidates or [
        {
            "Ticker": s["ticker"],
            "Score": s["score"],
            "FinScore": s["fin_score"],
            "TechScore": s["tech_score"],
            "MktScore": s["market_score"],
            "SectorScore": s["sector_score"],
            "VolKki": s["vol_kki"],
            "Pos52w": s["pos_52w"],
        }
        for s in scores
    ]
    (run_dir / "screener_candidates.json").write_text(
        json.dumps(cands), encoding="utf-8"
    )
    return run_dir


# ── Schema / Production formula ─────────────────────────────────────


def test_production_factor_names_match_screener_formula():
    names = [s["name"] for s in PRODUCTION_FACTOR_SPECS]
    assert names == [
        "fin_score",
        "tech_score",
        "market_score",
        "sector_score",
        "vol_kki",
        "pos_52w",
    ]
    assert "pattern_score" not in names


def test_inspect_schema_uses_actual_artifact_fields_not_guesses():
    rows = [_score_row("AAA"), _score_row("BBB", fin=0.7)]
    schema = inspect_artifact_factor_schema(rows)
    resolved = {r["factor"]: r["primary_field"] for r in schema["production_factors_resolved"]}
    assert resolved["fin_score"] == "fin_score"
    assert resolved["tech_score"] == "tech_score"
    assert resolved["market_score"] == "market_score"
    assert "pattern_score" in schema["excluded_fields_present_but_not_in_weighted_score"]
    assert "MomentumScore" not in schema["observed_keys"]


def test_extract_factor_pascalcase_candidates():
    row = {
        "FinScore": 0.55,
        "TechScore": 0.44,
        "MktScore": 0.33,
        "SectorScore": 0.22,
        "VolKki": 0.11,
        "Pos52w": 0.66,
    }
    assert extract_factor_value(row, "fin_score") == 0.55
    assert extract_factor_value(row, "market_score") == 0.33
    assert extract_factor_value(row, "vol_kki") == 0.11


def test_missing_factor_graceful():
    row = {"ticker": "X", "fin_score": 0.5}
    assert extract_factor_value(row, "tech_score") is None
    enriched = enrich_observation_with_factors(
        _obs(trade_date="20260728", ticker="X"),
        row,
        weights=production_factor_weights(),
        market_regime="BULL",
    )
    assert enriched["factors"]["fin_score"] == 0.5
    assert enriched["factors"]["tech_score"] is None
    assert enriched["contributions"]["tech_score"] is None


def test_weights_read_from_config_without_mutation(tmp_path: Path):
    cfg = {
        "screener_params": {
            "fin_weight": 0.25,
            "tech_weight": 0.35,
            "mkt_weight": 0.1,
            "sector_weight": 0.1,
            "vol_kki_weight": 0.05,
            "pos_52w_weight": 0.05,
        }
    }
    before = json.dumps(cfg, sort_keys=True)
    w = production_factor_weights(cfg)
    assert w["tech_score"] == 0.35
    assert w["fin_score"] == 0.25
    assert json.dumps(cfg, sort_keys=True) == before


# ── Scope filters ───────────────────────────────────────────────────


def test_report_window_isolation():
    rows = [
        _obs(trade_date="20260722", ticker="OLD", ret5=9.0),
        _obs(trade_date="20260728", ticker="IN", ret5=1.0),
        _obs(trade_date="20260825", ticker="NEW", ret5=2.0),
    ]
    out = filter_production_analysis_rows(
        rows,
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    assert [r["ticker"] for r in out] == ["IN"]


def test_production_only_excludes_shadow_and_liquidity():
    rows = [
        _obs(trade_date="20260728", ticker="P", candidate_type="PRODUCTION"),
        _obs(trade_date="20260728", ticker="E", candidate_type="ELIGIBLE_SHADOW"),
        _obs(trade_date="20260728", ticker="L", candidate_type="LIQUIDITY_SHADOW"),
        _obs(trade_date="20260728", ticker="H", candidate_type="HIGH_CONVICTION_SHADOW"),
    ]
    out = filter_production_analysis_rows(
        rows,
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    assert [r["ticker"] for r in out] == ["P"]


def test_trusted_only():
    rows = [
        _obs(trade_date="20260728", ticker="T", trusted=True),
        _obs(trade_date="20260728", ticker="U", trusted=False),
    ]
    out = filter_production_analysis_rows(
        rows,
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    assert [r["ticker"] for r in out] == ["T"]


def test_settled_only_excludes_pending():
    rows = [
        _obs(trade_date="20260728", ticker="OK", ret5=1.0),
        _obs(trade_date="20260728", ticker="PEND", ret5=None),
    ]
    out = filter_production_analysis_rows(
        rows,
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    assert [r["ticker"] for r in out] == ["OK"]


def test_no_future_leakage_in_returns_join(tmp_path: Path):
    """Factor values come from same-run score artifact; returns are forward labels only."""
    run = _write_run(
        tmp_path,
        trade_date="20260728",
        run_id="run-a",
        scores=[_score_row("AAA", score=0.55, fin=0.8)],
    )
    obs = [
        _obs(trade_date="20260728", ticker="AAA", score=0.55, ret5=-2.0, run_id="run-a"),
        _obs(trade_date="20260801", ticker="AAA", score=0.55, ret5=3.0, run_id="run-future"),
    ]
    # Future run not in run_dirs → should not pollute 20260728 factor join
    enriched, schema, _w = build_factor_observations(
        obs,
        run_dirs=[run],
        merged_by_run={},
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    by_date = {r["trade_date"]: r for r in enriched}
    assert by_date["20260728"]["factors"]["fin_score"] == 0.8
    assert by_date["20260728"]["return_5d_pct"] == -2.0
    # 20260801 kept as observation if settled+scoped, but factors may be missing
    assert "20260801" in by_date
    assert schema["production_factors_resolved"]


# ── Stats ───────────────────────────────────────────────────────────


def test_spearman_calculation():
    # Perfect negative rank correlation
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    ys = [5.0, 4.0, 3.0, 2.0, 1.0]
    assert spearman_corr(xs, ys) == pytest.approx(-1.0)
    rows = []
    for i, (x, y) in enumerate(zip(xs, ys)):
        r = _obs(trade_date="20260728", ticker=f"T{i}", score=x, ret5=y)
        r = enrich_observation_with_factors(
            r,
            _score_row(f"T{i}", score=x, fin=x),
            weights=production_factor_weights(),
            market_regime="SIDEWAYS",
        )
        rows.append(r)
    sp = spearman_factor_vs_returns(rows, factor_name="fin_score")
    assert sp["spearman_5d"] == pytest.approx(-1.0)
    tsp = spearman_factor_vs_returns(rows, use_total_score=True)
    assert tsp["spearman_5d"] == pytest.approx(-1.0)


def test_quintile_assignment():
    rows = [{"v": float(i)} for i in range(10)]
    labels = assign_quintiles(rows, value_getter=lambda r: r["v"])
    assert None not in labels
    assert set(labels) == {1, 2, 3, 4, 5}
    assert labels[0] == 1
    assert labels[-1] == 5


def test_first_signal_only_repeated_ticker():
    rows = [
        _obs(trade_date="20260805", ticker="MU", score=0.6, ret5=-1),
        _obs(trade_date="20260728", ticker="MU", score=0.5, ret5=2),
        _obs(trade_date="20260801", ticker="AMD", score=0.55, ret5=1),
    ]
    out = first_signal_only(rows)
    by_t = {r["ticker"]: r for r in out}
    assert by_t["MU"]["trade_date"] == "20260728"
    assert by_t["MU"]["decision_score"] == 0.5
    assert len(out) == 2


def test_ticker_equal_weighting():
    rows = [
        enrich_observation_with_factors(
            _obs(trade_date="20260728", ticker="MU", score=0.5, ret5=2.0),
            _score_row("MU", fin=0.4),
            weights=production_factor_weights(),
            market_regime="BULL",
        ),
        enrich_observation_with_factors(
            _obs(trade_date="20260801", ticker="MU", score=0.6, ret5=4.0),
            _score_row("MU", fin=0.8),
            weights=production_factor_weights(),
            market_regime="BULL",
        ),
        enrich_observation_with_factors(
            _obs(trade_date="20260728", ticker="AMD", score=0.5, ret5=1.0),
            _score_row("AMD", fin=0.3),
            weights=production_factor_weights(),
            market_regime="BULL",
        ),
    ]
    te = ticker_equal_weight_rows(rows)
    assert len(te) == 2
    mu = next(r for r in te if r["ticker"] == "MU")
    assert mu["return_5d_pct"] == pytest.approx(3.0)
    assert mu["factors"]["fin_score"] == pytest.approx(0.6)


def test_regime_separation_in_analysis():
    rows = []
    # BULL: positive factor↔return
    for i in range(6):
        r = enrich_observation_with_factors(
            _obs(trade_date="20260728", ticker=f"B{i}", score=0.4 + i * 0.02, ret5=float(i)),
            _score_row(f"B{i}", fin=0.2 + i * 0.1),
            weights=production_factor_weights(),
            market_regime="BULL",
        )
        rows.append(r)
    # BEAR: negative factor↔return
    for i in range(6):
        r = enrich_observation_with_factors(
            _obs(trade_date="20260801", ticker=f"R{i}", score=0.4 + i * 0.02, ret5=float(5 - i)),
            _score_row(f"R{i}", fin=0.2 + i * 0.1),
            weights=production_factor_weights(),
            market_regime="BEAR",
        )
        rows.append(r)
    report = analyze_factor_dataset(
        rows,
        schema_inspection=inspect_artifact_factor_schema([_score_row("X")]),
        weights=production_factor_weights(),
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    reg = (report["observation_weighted"]["regime_factor_spearman"]["fin_score"])
    assert reg["BULL"]["spearman_5d"] is not None
    assert reg["BEAR"]["spearman_5d"] is not None
    assert reg["BULL"]["spearman_5d"] > 0
    assert reg["BEAR"]["spearman_5d"] < 0


def test_factor_correlation_and_double_counting_finding():
    rows = []
    for i in range(8):
        fin = 0.1 * i
        # tech almost identical to fin → high correlation
        r = enrich_observation_with_factors(
            _obs(trade_date="20260728", ticker=f"C{i}", score=0.5, ret5=float(i % 3)),
            _score_row(f"C{i}", fin=fin, tech=fin + 0.01),
            weights=production_factor_weights(),
            market_regime="SIDEWAYS",
        )
        rows.append(r)
    corr = factor_correlation_matrix(rows, ["fin_score", "tech_score", "vol_kki"])
    assert corr["fin_score"]["tech_score"] is not None
    assert corr["fin_score"]["tech_score"] > CORR_HIGH
    report = analyze_factor_dataset(
        rows,
        schema_inspection=inspect_artifact_factor_schema([_score_row("X")]),
        weights=production_factor_weights(),
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    types = {f["type"] for f in report["primary_findings"]}
    assert FINDING_DC in types


def test_negative_factor_finding_when_spearman_negative():
    rows = []
    for i in range(8):
        r = enrich_observation_with_factors(
            _obs(trade_date="20260728", ticker=f"N{i}", score=0.4 + 0.02 * i, ret5=float(7 - i)),
            _score_row(f"N{i}", fin=0.1 * i, tech=0.5, mkt=0.5, sector=0.5, vol=0.2, pos=0.5),
            weights=production_factor_weights(),
            market_regime="SIDEWAYS",
        )
        rows.append(r)
    report = analyze_factor_dataset(
        rows,
        schema_inspection=inspect_artifact_factor_schema([_score_row("X")]),
        weights=production_factor_weights(),
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    neg = [f for f in report["primary_findings"] if f["type"] == FINDING_NEG]
    assert any(f.get("factor") == "fin_score" for f in neg)


def test_outputs_written(tmp_path: Path):
    rows = []
    for i in range(10):
        rows.append(
            enrich_observation_with_factors(
                _obs(trade_date="20260728", ticker=f"T{i}", score=0.45 + 0.01 * i, ret5=float(i - 5)),
                _score_row(f"T{i}", fin=0.1 * i),
                weights=production_factor_weights(),
                market_regime="SIDEWAYS",
            )
        )
    report = analyze_factor_dataset(
        rows,
        schema_inspection=inspect_artifact_factor_schema([_score_row("X")]),
        weights=production_factor_weights(),
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    paths = write_factor_analysis_outputs(report, rows, tmp_path)
    assert paths["json"].exists()
    assert paths["md"].exists()
    assert paths["factor_observations"].exists()
    assert paths["factor_spearman"].exists()
    assert paths["factor_buckets"].exists()
    assert paths["factor_correlation"].exists()
    md = paths["md"].read_text(encoding="utf-8")
    assert "NEGATIVE_FACTOR_CANDIDATE" in md or "LOW_SIGNAL_FACTOR" in md or "POSITIVE_FACTOR_CANDIDATE" in md
    assert "unchanged" in md.lower() or "Read-only" in md


def test_end_to_end_build_from_artifacts(tmp_path: Path):
    run = _write_run(
        tmp_path,
        trade_date="20260728",
        run_id="run-e2e",
        scores=[
            _score_row("AAA", score=0.62, fin=0.9, tech=0.8),
            _score_row("BBB", score=0.50, fin=0.4, tech=0.5),
        ],
        regime="BULL",
    )
    ledger_rows = [
        _obs(trade_date="20260728", ticker="AAA", score=0.62, ret5=-3.0, run_id="run-e2e"),
        _obs(trade_date="20260728", ticker="BBB", score=0.50, ret5=2.0, run_id="run-e2e"),
        _obs(
            trade_date="20260728",
            ticker="CCC",
            score=0.55,
            ret5=1.0,
            run_id="run-e2e",
            candidate_type="LIQUIDITY_SHADOW",
        ),
    ]
    enriched, schema, w = build_factor_observations(
        ledger_rows,
        run_dirs=[run],
        start_trade_date=START,
        end_trade_date=END,
        market=MARKET,
        session=SESSION,
    )
    assert len(enriched) == 2
    assert {r["ticker"] for r in enriched} == {"AAA", "BBB"}
    aaa = next(r for r in enriched if r["ticker"] == "AAA")
    assert aaa["factors"]["fin_score"] == 0.9
    assert aaa["market_regime"] == "BULL"
    assert aaa["contributions"]["fin_score"] == pytest.approx(0.9 * w["fin_score"])
    assert any(r["factor"] == "fin_score" for r in schema["production_factors_resolved"])
