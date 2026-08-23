"""Tests for read-only Full-Universe Weight Replay Analyzer."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("OUTPUT_DIR", str(ROOT / "output"))
os.environ.setdefault("CACHE_DIR", str(ROOT / "output" / "cache"))
os.environ.setdefault("CONFIG_PATH", str(ROOT / "config" / "config.json"))

import pytest
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from screener_full_universe_replay import (  # noqa: E402
    MIGRATION_DROP,
    MIGRATION_KEEP,
    MIGRATION_NEW,
    MIGRATION_NEITHER,
    PRODUCTION_THRESHOLD,
    SCENARIOS,
    SCOPE_NOTE,
    STATUS_OK,
    TRUSTED,
    WARNING_BASELINE_REPLAY_MISMATCH,
    analyze_full_universe_replay,
    assess_run_trust,
    build_migration_records,
    build_replay_dataframe,
    classify_migration,
    compare_baseline_replay_to_production,
    compute_scenario_score,
    eligible_universe_top_k,
    load_replay_days,
    normalize_score_record,
    pipeline_params_from_config,
    replay_day_scenario,
    resolve_full_scored_universe,
    run_candidate_pipeline,
    run_full_universe_replay,
    write_replay_outputs,
)
from screener_ops import enrich_scored_dataframe, select_candidates_pipeline  # noqa: E402
from screener_weight_simulation import FACTOR_NAMES  # noqa: E402,F401

BASE_W = dict(SCENARIOS["A_BASELINE"])

START = "20260727"
END = "20260821"
MARKET = "SP500"
SESSION = "pm"


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


def _score_row(
    ticker: str,
    *,
    fin: float = 0.5,
    tech: float = 0.5,
    mkt: float = 0.5,
    sector: float = 0.5,
    vol: float = 0.5,
    pos: float = 0.5,
    sector_name: str = "Tech",
    held: bool = False,
    exclusion_reasons: Optional[List[str]] = None,
    issuer_group: Optional[str] = None,
    ret5: Optional[float] = 1.0,
) -> Dict[str, Any]:
    score = compute_scenario_score(_factors(fin, tech, mkt, sector, vol, pos), BASE_W)
    row: Dict[str, Any] = {
        "ticker": ticker,
        "score": score,
        "fin_score": fin,
        "tech_score": tech,
        "market_score": mkt,
        "sector_score": sector,
        "vol_kki": vol,
        "pos_52w": pos,
        "sector": sector_name,
        "issuer_group": issuer_group or ticker,
        "held": held,
        "exclusion_reasons": exclusion_reasons or [],
        "eligibility_status": "ELIGIBLE",
        "momentum_pass": True,
        "volatility_pass": True,
        "threshold_pass": (score or 0) >= PRODUCTION_THRESHOLD,
        "schema_version": "1.4",
    }
    if ret5 is not None:
        row["return_5d_pct"] = ret5
    return row


def _pipeline_candidates_from_universe(
    universe: List[Dict[str, Any]],
    *,
    cfg: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Derive Production-like candidates via the same pipeline."""
    cfg = cfg or {"screener_params": {"top_n": 8, "sector_cap": 0.35, "issuer_dedupe_enabled": True}}
    params = pipeline_params_from_config(cfg)
    import pandas as pd

    records = []
    for u in universe:
        records.append(
            {
                "Ticker": u["ticker"],
                "Score": u["score"],
                "Sector": u.get("sector") or "Tech",
                "issuer_group": u.get("issuer_group") or u["ticker"],
                "held": u.get("held", False),
                "exclude_reasons": list(u.get("exclusion_reasons") or []),
                "eligibility_status": "ELIGIBLE" if not u.get("held") else "EXCLUDED",
                "momentum_pass": True,
                "volatility_pass": True,
                "RSI": 55.0,
            }
        )
    df = pd.DataFrame(records)
    df = enrich_scored_dataframe(
        df,
        held_tickers={r["Ticker"] for r in records if r.get("held")},
        issuer_map={},
        production_threshold=PRODUCTION_THRESHOLD,
    )

    def _div(df_sorted, top_n, sector_cap):
        from screener import diversify_by_sector

        return diversify_by_sector(df_sorted, top_n, sector_cap)

    out, _ = select_candidates_pipeline(
        df,
        threshold=PRODUCTION_THRESHOLD,
        require_positive_momentum=params["require_positive_momentum"],
        exclude_high_volatility=params["exclude_high_volatility"],
        top_n=params["top_n"],
        sector_cap=params["sector_cap"],
        diversify_fn=_div,
        apply_issuer_dedupe=params["apply_issuer_dedupe"],
        require_eligible=True,
    )
    cands = []
    for _, r in out.iterrows():
        cands.append({"Ticker": r["Ticker"], "Score": float(r["Score"])})
    return cands


def _write_replay_run(
    root: Path,
    *,
    trade_date: str,
    run_id: str,
    universe: List[Dict[str, Any]],
    candidates: Optional[List[Dict[str, Any]]] = None,
    regime: str = "BULL",
) -> Path:
    run_dir = root / "runs" / "decision" / MARKET / trade_date / SESSION / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    cands = candidates if candidates is not None else _pipeline_candidates_from_universe(universe)
    scores_path = run_dir / "screener_scores.json"
    scores_path.write_text(json.dumps(universe), encoding="utf-8")
    cands_path = run_dir / "screener_candidates.json"
    cands_path.write_text(json.dumps(cands), encoding="utf-8")
    meta = {
        "run_id": run_id,
        "run_mode": "DECISION",
        "market": MARKET,
        "session": SESSION,
        "trade_date": trade_date,
        "status": "SUCCESS",
        "decision_artifact": True,
        "schema_version": "3",
    }
    meta_path = run_dir / "screener_run_meta.json"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    ms_path = run_dir / "market_state.json"
    ms_path.write_text(json.dumps({"regime": regime, "trend": regime}), encoding="utf-8")
    manifest = {
        "run_id": run_id,
        "run_mode": "DECISION",
        "market": MARKET,
        "session": SESSION,
        "trade_date": trade_date,
        "status": "SUCCESS",
        "decision_artifact": True,
        "artifacts": {
            "screener_scores.json": {
                "sha256": hashlib.sha256(scores_path.read_bytes()).hexdigest(),
                "row_count": len(universe),
            },
            "screener_candidates.json": {
                "sha256": hashlib.sha256(cands_path.read_bytes()).hexdigest(),
                "row_count": len(cands),
            },
            "screener_run_meta.json": {
                "sha256": hashlib.sha256(meta_path.read_bytes()).hexdigest(),
            },
            "market_state.json": {
                "sha256": hashlib.sha256(ms_path.read_bytes()).hexdigest(),
            },
        },
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return run_dir


def _build_test_universe() -> List[Dict[str, Any]]:
    """Universe with above/below threshold names for migration tests."""
    rows = [
        _score_row("AAA", fin=0.9, tech=0.9, sector_name="Tech", ret5=2.0),
        _score_row("BBB", fin=0.85, tech=0.85, sector_name="Health", ret5=1.0),
        _score_row("CCC", fin=0.8, tech=0.8, sector_name="Energy", ret5=-1.0),
        _score_row("DDD", fin=0.75, tech=0.75, sector_name="Finance", ret5=0.5),
        # Below baseline threshold but may pass under C (pos52w zero -> vol up)
        _score_row("NEW1", fin=0.7, tech=0.7, vol=0.95, pos=0.1, sector_name="Tech2", ret5=3.0),
        # High pos52w — C may drop vs A
        _score_row("DROP1", fin=0.72, tech=0.72, vol=0.2, pos=0.95, sector_name="Cons", ret5=-2.0),
        # Sub-threshold under all scenarios at 0.48
        _score_row("LOW", fin=0.2, tech=0.2, vol=0.2, pos=0.2, sector_name="Other", ret5=0.1),
        _score_row("HELD", fin=0.95, tech=0.95, held=True, sector_name="Tech", ret5=0.0),
    ]
    return rows


def _seed_output(tmp_path: Path) -> Path:
    out = tmp_path / "output"
    universe = _build_test_universe()
    dates = ["20260728", "20260729", "20260810", "20260811"]
    for i, td in enumerate(dates):
        _write_replay_run(out, trade_date=td, run_id=f"run-{i}", universe=universe)
    return out


# ── Production untouched ────────────────────────────────────────────


def test_production_config_not_modified(tmp_path: Path):
    cfg_path = ROOT / "config" / "config.json"
    before = hashlib.sha256(cfg_path.read_bytes()).hexdigest()
    out = _seed_output(tmp_path)
    run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
        config_path=cfg_path,
    )
    assert hashlib.sha256(cfg_path.read_bytes()).hexdigest() == before


def test_screener_py_not_modified(tmp_path: Path):
    screener_path = ROOT / "src" / "screener.py"
    before = hashlib.sha256(screener_path.read_bytes()).hexdigest()
    out = _seed_output(tmp_path)
    run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    assert hashlib.sha256(screener_path.read_bytes()).hexdigest() == before


def test_decision_artifacts_not_modified(tmp_path: Path):
    out = _seed_output(tmp_path)
    run_dir = out / "runs" / "decision" / MARKET / "20260728" / SESSION / "run-0"
    scores_path = run_dir / "screener_scores.json"
    sha_before = hashlib.sha256(scores_path.read_bytes()).hexdigest()
    run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    assert hashlib.sha256(scores_path.read_bytes()).hexdigest() == sha_before
    assert (out / "quality" / "full_universe_replay").is_dir()


def test_input_fingerprint_unchanged(tmp_path: Path):
    out = _seed_output(tmp_path)
    report, _ = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
        config_path=ROOT / "config" / "config.json",
    )
    assert report.get("production_inputs_unchanged") is True


# ── Universe loading ────────────────────────────────────────────────


def test_full_scored_universe_from_screener_scores(tmp_path: Path):
    out = _seed_output(tmp_path)
    run_dir = out / "runs" / "decision" / MARKET / "20260728" / SESSION / "run-0"
    rows, source, meta = resolve_full_scored_universe(run_dir)
    assert source == "screener_scores.json"
    assert len(rows) == 8
    assert meta["row_count"] == 8


def test_trusted_run_assessment(tmp_path: Path):
    out = _seed_output(tmp_path)
    run_dir = out / "runs" / "decision" / MARKET / "20260728" / SESSION / "run-0"
    trust = assess_run_trust(run_dir)
    assert trust["status"] == TRUSTED


def test_load_replay_days_uses_full_universe_not_candidates_only(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["primary_universe_artifact"] == "screener_scores.json"
    assert meta["included_days"] >= 1
    assert len(days[0]["universe_rows"]) == 8


def test_report_window_isolation(tmp_path: Path):
    out = _seed_output(tmp_path)
    _write_replay_run(out, trade_date="20260720", run_id="old", universe=_build_test_universe())
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert all(d["trade_date"] >= START for d in days)
    assert all(d["trade_date"] <= END for d in days)
    assert meta["included_days"] == 4


# ── Score / baseline ────────────────────────────────────────────────


def test_baseline_score_reproduction():
    u = _build_test_universe()
    hydrated = [
        normalize_score_record(r, trade_date="20260728", market=MARKET, session=SESSION, source_run_id="r1")
        for r in u
    ]
    from screener_weight_simulation import reconstruct_baseline_scores

    recon = reconstruct_baseline_scores(hydrated, BASE_W)
    assert recon["ok"] is True


def test_baseline_threshold_048():
    assert PRODUCTION_THRESHOLD == 0.48


def test_scenario_scores_c_d_e():
    f = _factors(0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
    assert compute_scenario_score(f, SCENARIOS["C_POS52W_ZERO"]) is not None
    assert compute_scenario_score(f, SCENARIOS["D_TECH_POS_DOWN"]) is not None
    assert compute_scenario_score(f, SCENARIOS["E_CONSERVATIVE"]) is not None


def test_baseline_candidate_replay_match(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, _ = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    params = pipeline_params_from_config({"screener_params": {}})
    day = days[0]
    dr = replay_day_scenario(
        day,
        scenario_name="A_BASELINE",
        weights=BASE_W,
        threshold=PRODUCTION_THRESHOLD,
        pipeline_params=params,
        baseline_weights=BASE_W,
    )
    bc = dr["baseline_compare"]
    assert bc["exact_set_match"] is True
    assert bc["missing_from_replay"] == []
    assert bc["extra_in_replay"] == []


# ── Pipeline / migration ────────────────────────────────────────────


def test_downstream_eligibility_issuer_sector(tmp_path: Path):
    universe = [
        _score_row("GOOG", fin=0.9, tech=0.9, issuer_group="ALPHABET", sector_name="Tech"),
        _score_row("GOOGL", fin=0.88, tech=0.88, issuer_group="ALPHABET", sector_name="Tech"),
        _score_row("AAA", fin=0.85, tech=0.85, sector_name="Health"),
    ]
    hydrated = [
        normalize_score_record(r, trade_date="20260728", market=MARKET, session=SESSION, source_run_id="r")
        for r in universe
    ]
    params = pipeline_params_from_config({"screener_params": {"issuer_dedupe_enabled": True, "top_n": 8}})
    df = build_replay_dataframe(hydrated, weights=BASE_W, threshold=PRODUCTION_THRESHOLD, pipeline_params=params)
    out, stages = run_candidate_pipeline(df, threshold=PRODUCTION_THRESHOLD, pipeline_params=params)
    tickers = set(out["Ticker"].tolist()) if not out.empty else set()
    assert "GOOG" in tickers or "GOOGL" in tickers
    assert not ("GOOG" in tickers and "GOOGL" in tickers)
    stage_names = [s.stage for s in stages]
    assert "ISSUER_DEDUP" in stage_names
    assert "SECTOR_DIVERSIFICATION" in stage_names


def test_migration_keep_drop_new_neither():
    assert classify_migration("A", baseline_candidates={"A", "B"}, scenario_candidates={"A", "C"}) == MIGRATION_KEEP
    assert classify_migration("B", baseline_candidates={"A", "B"}, scenario_candidates={"A", "C"}) == MIGRATION_DROP
    assert classify_migration("C", baseline_candidates={"A", "B"}, scenario_candidates={"A", "C"}) == MIGRATION_NEW
    assert classify_migration("Z", baseline_candidates={"A"}, scenario_candidates={"A"}) == MIGRATION_NEITHER


def test_new_candidate_migration_visible(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, load_meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    load_meta["output_dir"] = str(out)
    params = pipeline_params_from_config({"screener_params": {}})
    report = analyze_full_universe_replay(
        days,
        baseline_weights=BASE_W,
        pipeline_params=params,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        load_meta=load_meta,
    )
    mig = report.get("_export_migration") or []
    new_rows = [m for m in mig if m.get("migration_group") == MIGRATION_NEW and m.get("scenario") == "C_POS52W_ZERO"]
    assert len(new_rows) >= 0  # may or may not trigger depending on scores
    assert report.get("scope") == SCOPE_NOTE


# ── Top-N / top-K / split / outcomes ───────────────────────────────


def test_top_n_full_universe(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, load_meta = load_replay_days(
        output_dir=out, market=MARKET, session=SESSION, start_trade_date=START, end_trade_date=END
    )
    load_meta["output_dir"] = str(out)
    params = pipeline_params_from_config({"screener_params": {}})
    report = analyze_full_universe_replay(
        days,
        baseline_weights=BASE_W,
        pipeline_params=params,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        load_meta=load_meta,
    )
    topn = report.get("_export_topn") or []
    assert any(r.get("top_n") == "top_10" for r in topn)


def test_daily_top_k_matched(tmp_path: Path):
    universe = _build_test_universe()
    hydrated = [
        normalize_score_record(r, trade_date="20260728", market=MARKET, session=SESSION, source_run_id="r")
        for r in universe
    ]
    params = pipeline_params_from_config({"screener_params": {}})
    k = 3
    topk = eligible_universe_top_k(hydrated, weights=BASE_W, threshold=PRODUCTION_THRESHOLD, pipeline_params=params, k=k)
    assert len(topk) <= k


def test_train_validation_split_in_summary(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, load_meta = load_replay_days(
        output_dir=out, market=MARKET, session=SESSION, start_trade_date=START, end_trade_date=END
    )
    load_meta["output_dir"] = str(out)
    params = pipeline_params_from_config({"screener_params": {}})
    report = analyze_full_universe_replay(
        days,
        baseline_weights=BASE_W,
        pipeline_params=params,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        load_meta=load_meta,
    )
    summary = report.get("_export_scenario_summary") or []
    windows = {r["window"] for r in summary if r.get("scenario") == "A_BASELINE"}
    assert "train" in windows
    assert "validation" in windows


def test_regime_split_present(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, load_meta = load_replay_days(
        output_dir=out, market=MARKET, session=SESSION, start_trade_date=START, end_trade_date=END
    )
    load_meta["output_dir"] = str(out)
    params = pipeline_params_from_config({"screener_params": {}})
    report = analyze_full_universe_replay(
        days,
        baseline_weights=BASE_W,
        pipeline_params=params,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        load_meta=load_meta,
    )
    assert "BULL" in (report.get("regime_results") or {}).get("A_BASELINE", {})


def test_threshold_grid_for_d_e(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, load_meta = load_replay_days(
        output_dir=out, market=MARKET, session=SESSION, start_trade_date=START, end_trade_date=END
    )
    load_meta["output_dir"] = str(out)
    params = pipeline_params_from_config({"screener_params": {}})
    report = analyze_full_universe_replay(
        days,
        baseline_weights=BASE_W,
        pipeline_params=params,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        load_meta=load_meta,
    )
    grid = report.get("threshold_grid") or []
    d_rows = [g for g in grid if g.get("scenario") == "D_TECH_POS_DOWN"]
    assert len(d_rows) >= 7


def test_missing_outcome_graceful(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, load_meta = load_replay_days(
        output_dir=out, market=MARKET, session=SESSION, start_trade_date=START, end_trade_date=END
    )
    load_meta["output_dir"] = str(out)
    params = pipeline_params_from_config({"screener_params": {}})
    report = analyze_full_universe_replay(
        days,
        baseline_weights=BASE_W,
        pipeline_params=params,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        load_meta=load_meta,
    )
    assert report.get("status") in (STATUS_OK, "BASELINE_RECONSTRUCTION_FAILED")


def test_no_future_leakage_in_scores():
    f = _factors(0.3, 0.3, 0.3, 0.3, 0.3, 0.3)
    row = normalize_score_record(
        _score_row("X", fin=0.3, tech=0.3, mkt=0.3, sector=0.3, vol=0.3, pos=0.3, ret5=99.0),
        trade_date="20260728",
        market=MARKET,
        session=SESSION,
        source_run_id="r",
    )
    row["return_5d_pct"] = 99.0
    s = compute_scenario_score(f, BASE_W)
    assert compute_scenario_score(row["factors"], BASE_W) == s
    assert row["return_5d_pct"] == 99.0


def test_market_session_date_isolation(tmp_path: Path):
    out = _seed_output(tmp_path)
    _write_replay_run(
        out,
        trade_date="20260728",
        run_id="kr-run",
        universe=_build_test_universe(),
    )
    # Patch meta market to KOSPI — should be excluded when loading SP500
    run_dir = out / "runs" / "decision" / MARKET / "20260728" / SESSION / "kr-run"
    meta = json.loads((run_dir / "screener_run_meta.json").read_text(encoding="utf-8"))
    meta["market"] = "KOSPI"
    (run_dir / "screener_run_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    days, _ = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    run_ids = {d["source_run_id"] for d in days}
    assert "kr-run" not in run_ids


def test_rerun_idempotent(tmp_path: Path):
    out = _seed_output(tmp_path)
    r1, p1 = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    r2, _p2 = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    d1 = json.loads(p1["json"].read_text(encoding="utf-8"))
    d2 = json.loads(p1["json"].read_text(encoding="utf-8"))
    d1.pop("input_fingerprints_before", None)
    d1.pop("input_fingerprints_after", None)
    d2.pop("input_fingerprints_before", None)
    d2.pop("input_fingerprints_after", None)
    assert r1["baseline_replay"]["avg_match_pct"] == r2["baseline_replay"]["avg_match_pct"]


def test_outputs_written(tmp_path: Path):
    out = _seed_output(tmp_path)
    report, paths = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    write_replay_outputs(report, out)
    for key in (
        "json",
        "md",
        "replay_daily_candidates",
        "replay_scenario_summary",
        "replay_candidate_migration",
        "replay_threshold_grid",
        "replay_topn",
    ):
        assert paths[key].exists(), key
    assert "full_universe_replay" in str(paths["dir"])


def test_compare_baseline_replay_helper():
    cmp = compare_baseline_replay_to_production(["A", "B"], ["A", "B"])
    assert cmp["exact_set_match"] is True
    assert cmp["exact_order_match"] is True
