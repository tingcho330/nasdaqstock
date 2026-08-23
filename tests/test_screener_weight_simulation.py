"""Tests for read-only Offline Weight Simulator."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_weight_simulation import (  # noqa: E402
    BASELINE_SCORE_TOLERANCE,
    FACTOR_NAMES,
    FINDING_COUNTERFACTUAL,
    PRODUCTION_THRESHOLD,
    SCENARIOS,
    STATUS_BASELINE_FAILED,
    STATUS_OK,
    WEIGHT_SUM_EXPECTED,
    analyze_weight_scenarios,
    apply_scenario_scores,
    candidate_migration,
    compute_scenario_score,
    high_score_loss_cohort,
    hydrate_observation_row,
    load_factor_observations_csv,
    outcome_metrics,
    production_baseline_weights,
    reconstruct_baseline_scores,
    run_weight_simulation,
    split_train_validation,
    threshold_pass_rows,
    top_n_rows,
    weight_sum,
    write_simulation_outputs,
)
from screener_outcomes import spearman_corr  # noqa: E402


START = "20260727"
END = "20260821"
MARKET = "SP500"
SESSION = "pm"
BASE_W = dict(SCENARIOS["A_BASELINE"])


def _factors(
    fin: float = 0.5,
    tech: float = 0.5,
    mkt: float = 0.5,
    sector: float = 0.5,
    vol: float = 0.5,
    pos: float = 0.5,
) -> Dict[str, float]:
    return {
        "fin_score": fin,
        "tech_score": tech,
        "market_score": mkt,
        "sector_score": sector,
        "vol_kki": vol,
        "pos_52w": pos,
    }


def _obs(
    *,
    trade_date: str,
    ticker: str = "AAA",
    factors: Optional[Dict[str, float]] = None,
    ret5: Optional[float] = 1.0,
    ret1: Optional[float] = None,
    ret3: Optional[float] = None,
    ret10: Optional[float] = None,
    mdd: float = -1.0,
    runup: float = 2.0,
    candidate_type: str = "PRODUCTION",
    trusted: bool = True,
    market: str = MARKET,
    session: str = SESSION,
    weights: Optional[Dict[str, float]] = None,
    total_score_override: Optional[float] = None,
) -> Dict[str, Any]:
    f = factors or _factors()
    w = weights or BASE_W
    score = compute_scenario_score(f, w)
    if total_score_override is not None:
        score = total_score_override
    r5 = ret5
    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "market": market,
        "session": session,
        "candidate_type": candidate_type,
        "trusted_for_analysis": trusted,
        "factors": f,
        "total_score": score,
        "decision_score": score,
        "contribution_sum": compute_scenario_score(f, w),
        "return_1d_pct": ret1 if ret1 is not None else r5,
        "return_3d_pct": ret3 if ret3 is not None else r5,
        "return_5d_pct": r5,
        "return_10d_pct": ret10 if ret10 is not None else r5,
        "max_drawdown_5d_pct": mdd,
        "max_runup_5d_pct": runup,
    }


def _dataset() -> List[Dict[str, Any]]:
    """Diverse reconstructable Production-like observations across train/val."""
    rows: List[Dict[str, Any]] = []
    # Train window
    specs = [
        ("20260728", "AAA", _factors(0.8, 0.9, 0.6, 0.5, 0.4, 0.7), 2.0),
        ("20260728", "BBB", _factors(0.4, 0.3, 0.5, 0.5, 0.8, 0.2), -1.0),
        ("20260729", "CCC", _factors(0.9, 0.7, 0.5, 0.6, 0.3, 0.8), -2.5),
        ("20260730", "DDD", _factors(0.5, 0.5, 0.5, 0.5, 0.5, 0.5), 1.0),
        ("20260803", "EEE", _factors(0.7, 0.8, 0.4, 0.4, 0.6, 0.9), 0.5),
        ("20260805", "AAA", _factors(0.75, 0.85, 0.55, 0.5, 0.45, 0.65), -0.5),
        ("20260806", "FFF", _factors(0.3, 0.2, 0.5, 0.5, 0.9, 0.1), 3.0),
        ("20260807", "GGG", _factors(0.6, 0.6, 0.6, 0.6, 0.6, 0.6), 1.5),
        # Validation window
        ("20260810", "HHH", _factors(0.85, 0.75, 0.5, 0.5, 0.35, 0.7), 1.2),
        ("20260811", "III", _factors(0.45, 0.35, 0.5, 0.5, 0.85, 0.15), -0.8),
        ("20260812", "JJJ", _factors(0.95, 0.9, 0.55, 0.55, 0.25, 0.9), -3.0),
        ("20260813", "KKK", _factors(0.55, 0.45, 0.5, 0.5, 0.7, 0.3), 2.0),
        ("20260814", "LLL", _factors(0.65, 0.55, 0.5, 0.5, 0.55, 0.4), 0.8),
        ("20260817", "MMM", _factors(0.4, 0.25, 0.5, 0.5, 0.95, 0.05), 1.8),
        ("20260818", "NNN", _factors(0.7, 0.65, 0.5, 0.5, 0.5, 0.5), -1.2),
        ("20260819", "OOO", _factors(0.5, 0.4, 0.5, 0.5, 0.75, 0.2), 0.3),
        ("20260820", "PPP", _factors(0.8, 0.7, 0.5, 0.5, 0.4, 0.6), 2.5),
        ("20260821", "QQQ", _factors(0.35, 0.3, 0.5, 0.5, 0.9, 0.1), -0.4),
    ]
    for td, tkr, fac, ret in specs:
        rows.append(_obs(trade_date=td, ticker=tkr, factors=fac, ret5=ret))
    return rows


# ── Production untouched ────────────────────────────────────────────


def test_production_config_not_modified(tmp_path: Path):
    cfg_path = ROOT / "config" / "config.json"
    before = hashlib.sha256(cfg_path.read_bytes()).hexdigest()
    text_before = cfg_path.read_text(encoding="utf-8")

    out = tmp_path / "output"
    (out / "quality" / "factor_analysis").mkdir(parents=True)
    # Write minimal CSV so loader works
    from screener_factor_analysis import flatten_observation_csv_rows, _write_csv

    rows = _dataset()
    _write_csv(
        out / "quality" / "factor_analysis" / "factor_observations.csv",
        flatten_observation_csv_rows(rows),
    )
    run_weight_simulation(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
        config_path=cfg_path,
    )
    after = hashlib.sha256(cfg_path.read_bytes()).hexdigest()
    assert before == after
    assert cfg_path.read_text(encoding="utf-8") == text_before
    assert '"tech_weight": 0.35' in text_before
    assert '"min_score_threshold": 0.48' in text_before


def test_production_artifact_not_modified(tmp_path: Path):
    out = tmp_path / "output"
    fa = out / "quality" / "factor_analysis"
    fa.mkdir(parents=True)
    from screener_factor_analysis import flatten_observation_csv_rows, _write_csv

    csv_path = fa / "factor_observations.csv"
    _write_csv(csv_path, flatten_observation_csv_rows(_dataset()))
    # Fake a DECISION artifact that must stay untouched
    run_dir = (
        out / "runs" / "decision" / MARKET / "20260728" / SESSION / "run-x"
    )
    run_dir.mkdir(parents=True)
    scores_path = run_dir / "screener_scores.json"
    scores_path.write_text(json.dumps([{"ticker": "AAA", "score": 0.5}]), encoding="utf-8")
    sha_before = hashlib.sha256(scores_path.read_bytes()).hexdigest()
    csv_before = hashlib.sha256(csv_path.read_bytes()).hexdigest()

    run_weight_simulation(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    assert hashlib.sha256(scores_path.read_bytes()).hexdigest() == sha_before
    assert hashlib.sha256(csv_path.read_bytes()).hexdigest() == csv_before
    # Simulation writes only under weight_simulation/
    assert (out / "quality" / "weight_simulation").is_dir()


# ── Baseline reconstruction / weight semantics ──────────────────────


def test_baseline_score_reconstruction():
    rows = _dataset()
    recon = reconstruct_baseline_scores(rows, BASE_W)
    assert recon["ok"] is True
    assert recon["status"] == STATUS_OK
    assert recon["max_abs_error"] <= BASELINE_SCORE_TOLERANCE
    assert abs(recon["weight_sum"] - WEIGHT_SUM_EXPECTED) < 1e-9


def test_baseline_weight_total_090_semantics():
    assert abs(weight_sum(BASE_W) - 0.90) < 1e-12
    for name, w in SCENARIOS.items():
        assert abs(weight_sum(w) - 0.90) < 1e-12, name
    # Must NOT normalize: score uses raw 0.90 sum
    f = _factors(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    s = compute_scenario_score(f, BASE_W)
    assert s == pytest.approx(0.90, abs=1e-9)


def test_baseline_reconstruction_failure_aborts():
    row = _obs(trade_date="20260728", ticker="X")
    # Corrupt factors after sealing score so neither Score nor contribution_sum match
    row["factors"] = _factors(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    row["contribution_sum"] = 0.99
    report = analyze_weight_scenarios(
        [row],
        baseline_weights=BASE_W,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert report["status"] == STATUS_BASELINE_FAILED
    assert "scenarios" not in report or not report.get("scenarios")


def test_scenario_score_calculation():
    f = _factors(0.4, 0.6, 0.5, 0.5, 0.2, 0.8)
    w = SCENARIOS["B_TECH_DOWN"]
    expected = round(
        min(
            1.0,
            max(
                0.0,
                0.4 * 0.30
                + 0.6 * 0.25
                + 0.5 * 0.10
                + 0.5 * 0.10
                + 0.2 * 0.10
                + 0.8 * 0.05,
            ),
        ),
        4,
    )
    assert compute_scenario_score(f, w) == pytest.approx(expected)
    scored = apply_scenario_scores(
        [_obs(trade_date="20260728", factors=f)],
        w,
        baseline_weights=BASE_W,
    )
    assert scored[0]["scenario_score"] == pytest.approx(expected)
    assert scored[0]["baseline_score"] is not None


def test_threshold_048_fixed():
    assert PRODUCTION_THRESHOLD == 0.48
    rows = apply_scenario_scores(_dataset(), BASE_W, baseline_weights=BASE_W)
    passed = threshold_pass_rows(rows, threshold=PRODUCTION_THRESHOLD)
    for r in passed:
        assert r["scenario_score"] >= 0.48
    report = analyze_weight_scenarios(
        _dataset(),
        baseline_weights=BASE_W,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert report["threshold"] == 0.48
    assert report["threshold_fixed"] is True


# ── Scope filters ───────────────────────────────────────────────────


def test_report_window_isolation(tmp_path: Path):
    out = tmp_path / "output"
    fa = out / "quality" / "factor_analysis"
    fa.mkdir(parents=True)
    from screener_factor_analysis import flatten_observation_csv_rows, _write_csv

    rows = _dataset() + [
        _obs(trade_date="20260720", ticker="OLD", ret5=9.0),
        _obs(trade_date="20260825", ticker="NEW", ret5=9.0),
    ]
    _write_csv(fa / "factor_observations.csv", flatten_observation_csv_rows(rows))
    report, _paths = run_weight_simulation(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    assert report["status"] == STATUS_OK
    # Outside-window tickers must not appear in observation count basis
    assert report["observations"] == len(_dataset())


def test_production_only_excludes_shadow():
    rows = [
        _obs(trade_date="20260728", ticker="P", candidate_type="PRODUCTION"),
        _obs(trade_date="20260728", ticker="L", candidate_type="LIQUIDITY_SHADOW"),
    ]
    # analyze uses provided rows; filtering is in loader — test hydrate + filter path
    from screener_weight_simulation import load_simulation_observations
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "output"
        fa = out / "quality" / "factor_analysis"
        fa.mkdir(parents=True)
        from screener_factor_analysis import flatten_observation_csv_rows, _write_csv

        # CSV from factor analysis is already PRODUCTION-only; simulate mixed by
        # writing then filtering via load path with candidate_type column.
        flat = flatten_observation_csv_rows(rows)
        for i, r in enumerate(rows):
            flat[i]["candidate_type"] = r["candidate_type"]
            flat[i]["market"] = MARKET
            flat[i]["session"] = SESSION
            flat[i]["trusted_for_analysis"] = True
        _write_csv(fa / "factor_observations.csv", flat)
        loaded, _meta = load_simulation_observations(
            output_dir=out,
            market=MARKET,
            session=SESSION,
            start_trade_date=START,
            end_trade_date=END,
        )
        assert [r["ticker"] for r in loaded] == ["P"]


def test_trusted_only(tmp_path: Path):
    out = tmp_path / "output"
    fa = out / "quality" / "factor_analysis"
    fa.mkdir(parents=True)
    from screener_factor_analysis import flatten_observation_csv_rows, _write_csv

    rows = [
        _obs(trade_date="20260728", ticker="T", trusted=True),
        _obs(trade_date="20260728", ticker="U", trusted=False),
    ]
    flat = flatten_observation_csv_rows(rows)
    for i, r in enumerate(rows):
        flat[i]["candidate_type"] = "PRODUCTION"
        flat[i]["market"] = MARKET
        flat[i]["session"] = SESSION
        flat[i]["trusted_for_analysis"] = r["trusted_for_analysis"]
    _write_csv(fa / "factor_observations.csv", flat)
    from screener_weight_simulation import load_simulation_observations

    loaded, _ = load_simulation_observations(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert [r["ticker"] for r in loaded] == ["T"]


def test_settled_only(tmp_path: Path):
    out = tmp_path / "output"
    fa = out / "quality" / "factor_analysis"
    fa.mkdir(parents=True)
    from screener_factor_analysis import flatten_observation_csv_rows, _write_csv

    rows = [
        _obs(trade_date="20260728", ticker="OK", ret5=1.0),
        _obs(trade_date="20260728", ticker="PEND", ret5=None),
    ]
    flat = flatten_observation_csv_rows(rows)
    for i, r in enumerate(rows):
        flat[i]["candidate_type"] = "PRODUCTION"
        flat[i]["market"] = MARKET
        flat[i]["session"] = SESSION
        flat[i]["trusted_for_analysis"] = True
        flat[i]["return_5d_pct"] = r["return_5d_pct"]
    _write_csv(fa / "factor_observations.csv", flat)
    from screener_weight_simulation import load_simulation_observations

    loaded, _ = load_simulation_observations(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert [r["ticker"] for r in loaded] == ["OK"]


def test_no_future_leakage_in_scores():
    """Scenario score uses only same-row factors; returns are labels only."""
    f = _factors(0.2, 0.2, 0.2, 0.2, 0.2, 0.2)
    row = _obs(trade_date="20260728", ticker="A", factors=f, ret5=99.0)
    scored = apply_scenario_scores([row], BASE_W, baseline_weights=BASE_W)[0]
    # Score independent of return
    assert scored["scenario_score"] == compute_scenario_score(f, BASE_W)
    assert scored["return_5d_pct"] == 99.0


# ── Stats ───────────────────────────────────────────────────────────


def test_spearman():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    ys = [5.0, 4.0, 3.0, 2.0, 1.0]
    assert spearman_corr(xs, ys) == pytest.approx(-1.0)
    rows = []
    for i, (x, y) in enumerate(zip(xs, ys)):
        # Build factors so scenario score ranks with x under baseline weights
        fac = _factors(fin=x / 5.0, tech=x / 5.0)
        r = _obs(trade_date="20260728", ticker=f"T{i}", factors=fac, ret5=y)
        rows.append(r)
    scored = apply_scenario_scores(rows, BASE_W, baseline_weights=BASE_W)
    m = outcome_metrics(scored, score_key="scenario_score")
    assert m["spearman_5d"] == pytest.approx(-1.0)


def test_median_win_rate():
    rows = apply_scenario_scores(
        [
            _obs(trade_date="20260728", ticker="A", ret5=1.0),
            _obs(trade_date="20260728", ticker="B", ret5=-1.0),
            _obs(trade_date="20260728", ticker="C", ret5=3.0),
        ],
        BASE_W,
        baseline_weights=BASE_W,
    )
    m = outcome_metrics(rows)
    assert m["win_rate"] == pytest.approx(2 / 3)
    assert m["median_5d"] == 1.0


def test_first_signal_and_ticker_equal_weight():
    rows = apply_scenario_scores(
        [
            _obs(trade_date="20260805", ticker="MU", ret5=-1.0),
            _obs(trade_date="20260728", ticker="MU", ret5=2.0),
            _obs(trade_date="20260728", ticker="AMD", ret5=1.0),
        ],
        BASE_W,
        baseline_weights=BASE_W,
    )
    m = outcome_metrics(rows)
    assert m["first_signal_n"] == 2
    # first MU is 20260728 → +2; AMD +1 → mean 1.5
    assert m["first_signal_mean_5d"] == pytest.approx(1.5)
    # ticker EW: MU mean (2+-1)/2=0.5, AMD=1 → mean 0.75
    assert m["ticker_equal_weight_mean_5d"] == pytest.approx(0.75)


def test_top_n():
    rows = []
    for i in range(12):
        fac = _factors(fin=0.1 * i, tech=0.1 * i)
        rows.append(
            _obs(
                trade_date="20260728",
                ticker=f"T{i}",
                factors=fac,
                ret5=float(i),
            )
        )
    scored = apply_scenario_scores(rows, BASE_W, baseline_weights=BASE_W)
    top5 = top_n_rows(scored, 5)
    assert len(top5) == 5
    scores = [r["scenario_score"] for r in top5]
    assert scores == sorted(scores, reverse=True)
    m = outcome_metrics(top5)
    assert m["observations"] == 5


def test_train_validation_date_split():
    train, valid = split_train_validation(_dataset())
    assert all(r["trade_date"] <= "20260807" for r in train)
    assert all(r["trade_date"] >= "20260810" for r in valid)
    assert len(train) + len(valid) == len(_dataset())


def test_baseline_delta_present():
    report = analyze_weight_scenarios(
        _dataset(),
        baseline_weights=BASE_W,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    for name, block in report["scenarios"].items():
        d = block["delta_vs_baseline_validation"]
        assert "spearman_5d" in d
        assert "mean_5d" in d
        assert "median_5d" in d
        assert "win_rate" in d
        assert "candidate_count" in d
        if name == "A_BASELINE":
            assert d["mean_5d"] == 0.0 or d["mean_5d"] == pytest.approx(0.0)


def test_candidate_migration():
    # Mid factors: baseline (tech-heavy) may pass; E_CONSERVATIVE (tech-down) may fail
    mid = _factors(0.5, 0.95, 0.5, 0.5, 0.2, 0.9)
    # High vol / low tech: baseline may fail; E may pass
    vol_heavy = _factors(0.7, 0.2, 0.5, 0.5, 1.0, 0.0)
    rows = [
        _obs(trade_date="20260728", ticker="MID", factors=mid, ret5=1.0),
        _obs(trade_date="20260728", ticker="VOL", factors=vol_heavy, ret5=-1.0),
    ]
    scored = apply_scenario_scores(
        rows, SCENARIOS["E_CONSERVATIVE"], baseline_weights=BASE_W
    )
    mig = candidate_migration(scored)
    assert "counts" in mig
    assert mig["limitation"] == "CANDIDATE_SET_COUNTERFACTUAL_ONLY"
    assert mig["counts"]["baseline_pass_and_scenario_pass"] >= 0
    # At least one migration direction should be non-empty for these crafted rows
    assert (
        mig["counts"]["baseline_pass_scenario_fail"]
        + mig["counts"]["baseline_fail_scenario_pass"]
        + mig["counts"]["baseline_pass_and_scenario_pass"]
    ) >= 1
    # Verify score keys present on any recorded migration row
    for group in (
        "baseline_pass_and_scenario_pass",
        "baseline_pass_scenario_fail",
        "baseline_fail_scenario_pass",
    ):
        for rec in mig[group]:
            assert "baseline_score" in rec and "scenario_score" in rec
            assert "return_5d_pct" in rec


def test_high_score_loss_cohort_movement():
    # Auto-discover: do not hardcode n=13
    rows = []
    for i in range(5):
        # High baseline score via high factors, negative 5D
        fac = _factors(0.95, 0.95, 0.9, 0.9, 0.9, 0.9)
        rows.append(
            _obs(
                trade_date="20260728",
                ticker=f"L{i}",
                factors=fac,
                ret5=-1.0 - i * 0.1,
            )
        )
    for i in range(3):
        fac = _factors(0.4, 0.4, 0.4, 0.4, 0.4, 0.4)
        rows.append(
            _obs(trade_date="20260728", ticker=f"W{i}", factors=fac, ret5=1.0)
        )
    scored = apply_scenario_scores(
        rows, SCENARIOS["E_CONSERVATIVE"], baseline_weights=BASE_W
    )
    cohort = high_score_loss_cohort(scored)
    assert len(cohort) == 5  # discovered, not hardcoded constant elsewhere
    report = analyze_weight_scenarios(
        rows,
        baseline_weights=BASE_W,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    hs = report["scenarios"]["E_CONSERVATIVE"]["full"]["high_score_loss_cohort"]
    assert hs["n"] == 5
    assert hs["mean_score_delta"] is not None


def test_missing_factor_graceful():
    row = hydrate_observation_row(
        {
            "ticker": "X",
            "trade_date": "20260728",
            "factor_fin_score": 0.5,
            "total_score": 0.125,  # only fin*0.25 if others zeroed
            "return_5d_pct": 1.0,
            "candidate_type": "PRODUCTION",
            "trusted_for_analysis": True,
            "market": MARKET,
            "session": SESSION,
        }
    )
    assert row["factors"]["fin_score"] == 0.5
    assert row["factors"]["tech_score"] is None
    score = compute_scenario_score(row["factors"], BASE_W, missing_as_zero=True)
    assert score == pytest.approx(round(0.5 * 0.25, 4))


def test_rerun_idempotency(tmp_path: Path):
    out = tmp_path / "output"
    fa = out / "quality" / "factor_analysis"
    fa.mkdir(parents=True)
    from screener_factor_analysis import flatten_observation_csv_rows, _write_csv

    _write_csv(fa / "factor_observations.csv", flatten_observation_csv_rows(_dataset()))
    r1, p1 = run_weight_simulation(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    j1 = p1["json"].read_text(encoding="utf-8")
    r2, p2 = run_weight_simulation(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    j2 = p2["json"].read_text(encoding="utf-8")
    assert r1["status"] == r2["status"] == STATUS_OK
    # Drop fingerprint timestamps-equivalent: compare scenario metrics
    assert r1["scenarios"]["A_BASELINE"]["full"]["ranking_quality"] == r2[
        "scenarios"
    ]["A_BASELINE"]["full"]["ranking_quality"]
    d1 = json.loads(j1)
    d2 = json.loads(j2)
    d1.pop("input_fingerprints_before", None)
    d1.pop("input_fingerprints_after", None)
    d2.pop("input_fingerprints_before", None)
    d2.pop("input_fingerprints_after", None)
    assert d1["scenarios"] == d2["scenarios"]


def test_outputs_and_findings(tmp_path: Path):
    report = analyze_weight_scenarios(
        _dataset(),
        baseline_weights=BASE_W,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    paths = write_simulation_outputs(report, tmp_path)
    assert paths["json"].exists()
    assert paths["md"].exists()
    assert paths["scenario_summary"].exists()
    assert paths["scenario_validation"].exists()
    assert paths["scenario_candidate_migration"].exists()
    assert paths["scenario_topn"].exists()
    md = paths["md"].read_text(encoding="utf-8")
    assert "A_BASELINE" in md
    assert FINDING_COUNTERFACTUAL in md or "CANDIDATE_SET_COUNTERFACTUAL_ONLY" in md
    assert "unchanged" in md.lower() or "NONE" in md
    assert report["recommendation"].startswith("NONE")


def test_production_baseline_weights_from_config():
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
    w = production_baseline_weights(cfg)
    assert w["tech_score"] == 0.35
    assert abs(weight_sum(w) - 0.90) < 1e-12
    assert json.dumps(cfg, sort_keys=True) == before


def test_factor_names_match_production():
    assert list(FACTOR_NAMES) == [
        "fin_score",
        "tech_score",
        "market_score",
        "sector_score",
        "vol_kki",
        "pos_52w",
    ]
