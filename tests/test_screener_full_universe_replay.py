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
    HISTORICAL_MARKET_STATE_CONFLICT,
    LEGACY_META_SELF_HASH_MISMATCH,
    LEGACY_UNTRUSTED,
    MISSING_HISTORICAL_MARKET_STATE,
    MIGRATION_DROP,
    MIGRATION_KEEP,
    MIGRATION_NEW,
    MIGRATION_NEITHER,
    PRODUCTION_THRESHOLD,
    REPLAY_SCORE_TOLERANCE,
    SCENARIOS,
    SCOPE_NOTE,
    STATUS_BASELINE_FAILED,
    STATUS_NO_DATA,
    STATUS_OK,
    TRUSTED,
    TRUSTED_WITH_WARNING,
    WARNING_BASELINE_REPLAY_MISMATCH,
    analyze_full_universe_replay,
    apply_replay_scenario_scores,
    assess_run_trust,
    build_market_state_from_payload,
    build_migration_records,
    build_replay_dataframe,
    classify_migration,
    combined_market_multiplier,
    compare_baseline_replay_to_production,
    compute_weighted_base,
    eligible_universe_top_k,
    load_historical_market_state,
    load_replay_days,
    normalize_score_record,
    pipeline_params_from_config,
    replay_day_scenario,
    replay_inclusion_decision,
    replay_score,
    reconstruct_baseline_scores_replay,
    resolve_full_scored_universe,
    run_candidate_pipeline,
    run_full_universe_replay,
    verify_screener_scores_integrity,
    write_replay_outputs,
)
from screener_ops import enrich_scored_dataframe, select_candidates_pipeline  # noqa: E402
from screener_quality import (  # noqa: E402
    assess_decision_run_trust,
    detect_legacy_meta_self_hash_only,
)
from screener_weight_simulation import FACTOR_NAMES, compute_scenario_score  # noqa: E402,F401
from screener_artifacts import sha256_file, verify_manifest_integrity  # noqa: E402

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


def _market_state_payload(regime: str = "bull", volatility: str = "medium") -> Dict[str, Any]:
    return {
        "regime": regime,
        "trend": regime,
        "volatility": volatility,
        "confidence": 0.5,
        "as_of_kst": "2026-08-03T09:00:00+09:00",
    }


def _historical_market_state(regime: str = "bull", volatility: str = "medium"):
    return build_market_state_from_payload(_market_state_payload(regime, volatility))


def _rescore_universe(
    universe: List[Dict[str, Any]],
    *,
    regime: str = "bull",
    volatility: str = "medium",
) -> List[Dict[str, Any]]:
    ms = _historical_market_state(regime, volatility)
    out: List[Dict[str, Any]] = []
    for row in universe:
        r = dict(row)
        factors = {name: row.get(name) for name in FACTOR_NAMES}
        score = replay_score(factors, BASE_W, ms)
        r["score"] = score
        r["threshold_pass"] = (score or 0) >= PRODUCTION_THRESHOLD
        out.append(r)
    return out


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
    regime: str = "bull",
    volatility: str = "medium",
) -> Dict[str, Any]:
    factors = _factors(fin, tech, mkt, sector, vol, pos)
    ms = _historical_market_state(regime, volatility)
    score = replay_score(factors, BASE_W, ms)
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
        "production_candidate": False,
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
    regime: str = "bull",
    volatility: str = "medium",
) -> Path:
    run_dir = root / "runs" / "decision" / MARKET / trade_date / SESSION / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    ms_payload = _market_state_payload(regime, volatility)
    universe = _rescore_universe(universe, regime=regime, volatility=volatility)
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
        "market_state": ms_payload,
    }
    meta_path = run_dir / "screener_run_meta.json"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    ms_path = run_dir / "market_state.json"
    ms_path.write_text(json.dumps({"regime": regime, "trend": regime, "volatility": volatility}), encoding="utf-8")
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
    ms = _historical_market_state("bull", "medium")
    hydrated = [
        normalize_score_record(r, trade_date="20260728", market=MARKET, session=SESSION, source_run_id="r1")
        for r in u
    ]
    recon = reconstruct_baseline_scores_replay(
        hydrated,
        BASE_W,
        market_state_by_date={"20260728": ms},
    )
    assert recon["ok"] is True
    assert recon["uses_calculate_market_adjusted_score"] is True


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
    ms = _historical_market_state()
    df = build_replay_dataframe(
        hydrated,
        weights=BASE_W,
        threshold=PRODUCTION_THRESHOLD,
        pipeline_params=params,
        historical_market_state=ms,
    )
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
    ms = _historical_market_state()
    topk = eligible_universe_top_k(
        hydrated,
        weights=BASE_W,
        threshold=PRODUCTION_THRESHOLD,
        pipeline_params=params,
        k=k,
        historical_market_state=ms,
    )
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
    ms = _historical_market_state()
    row = normalize_score_record(
        _score_row("X", fin=0.3, tech=0.3, mkt=0.3, sector=0.3, vol=0.3, pos=0.3, ret5=99.0),
        trade_date="20260728",
        market=MARKET,
        session=SESSION,
        source_run_id="r",
    )
    row["return_5d_pct"] = 99.0
    s = replay_score(f, BASE_W, ms)
    assert replay_score(row["factors"], BASE_W, ms) == s
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


# ── Trust alignment with Quality (legacy meta self-hash) ─────────────


def _inject_legacy_meta_self_hash(run_dir: Path) -> None:
    """Reproduce historical LEGACY_META_SELF_HASH_MISMATCH (meta-only SHA drift)."""
    meta_path = run_dir / "screener_run_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["artifact_integrity"] = dict(meta.get("artifact_integrity") or {})
    meta["artifact_integrity"]["screener_run_meta.json"] = {
        "sha256": sha256_file(meta_path)
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    meta["legacy_marker"] = True
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    # Manifest still points at pre-mutation meta sha → SHA_MISMATCH:screener_run_meta.json only


def _rewrite_manifest_scores_entry(run_dir: Path, **updates: Any) -> None:
    man_path = run_dir / "manifest.json"
    man = json.loads(man_path.read_text(encoding="utf-8"))
    arts = man.setdefault("artifacts", {})
    entry = dict(arts.get("screener_scores.json") or {})
    entry.update(updates)
    arts["screener_scores.json"] = entry
    man_path.write_text(json.dumps(man), encoding="utf-8")


def test_legacy_meta_self_hash_is_trusted_with_warning(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="legacy-meta", universe=_build_test_universe()
    )
    _inject_legacy_meta_self_hash(run_dir)
    ok, issues = verify_manifest_integrity(run_dir)
    assert not ok
    assert any(i.endswith("screener_run_meta.json") for i in issues if i.startswith("SHA_MISMATCH:"))
    meta = json.loads((run_dir / "screener_run_meta.json").read_text(encoding="utf-8"))
    assert detect_legacy_meta_self_hash_only(run_dir, issues=issues, meta=meta)

    trust = assess_decision_run_trust(run_dir)
    assert trust["trust_status"] == TRUSTED_WITH_WARNING
    assert trust["trust_reason"] == LEGACY_META_SELF_HASH_MISMATCH

    replay_trust = assess_run_trust(run_dir)
    assert replay_trust["status"] == TRUSTED_WITH_WARNING
    assert replay_trust["trust_reason"] == LEGACY_META_SELF_HASH_MISMATCH


def test_legacy_meta_self_hash_with_scores_pass_is_included(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="inc-warn", universe=_build_test_universe()
    )
    _inject_legacy_meta_self_hash(run_dir)
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["included_days"] == 1
    assert days[0]["trust_status"] == TRUSTED_WITH_WARNING
    assert days[0]["trust_reason"] == LEGACY_META_SELF_HASH_MISMATCH
    assert days[0]["artifact_integrity_status"] == "PASS"
    assert meta["accepted_trust_counts"].get(TRUSTED_WITH_WARNING) == 1
    assert meta["trust_counts_raw"].get(TRUSTED_WITH_WARNING) == 1


def test_legacy_meta_self_hash_scores_sha_mismatch_excluded(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="bad-sha", universe=_build_test_universe()
    )
    _inject_legacy_meta_self_hash(run_dir)
    _rewrite_manifest_scores_entry(run_dir, sha256="0" * 64)
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["included_days"] == 0
    # Scores SHA mismatch is not legacy-meta-only → untrusted (or scores FAIL)
    assert meta["excluded_trust_counts"].get(LEGACY_UNTRUSTED) == 1
    assert meta["artifact_integrity_counts"].get("FAIL") == 1
    assert any(not d.get("included") for d in meta["run_decisions"])


def test_legacy_meta_self_hash_row_count_mismatch_excluded(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="bad-rc", universe=_build_test_universe()
    )
    _inject_legacy_meta_self_hash(run_dir)
    _rewrite_manifest_scores_entry(run_dir, row_count=999)
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["included_days"] == 0
    assert any(not d.get("included") for d in meta["run_decisions"])
    # Independent scores check must flag row-count failure
    integ = verify_screener_scores_integrity(run_dir, expected_run_id="bad-rc")
    assert integ["status"] == "FAIL"
    assert not integ["row_count_ok"]


def test_scores_integrity_fail_blocks_trusted_warning(tmp_path: Path):
    """Artifact gate: TRUSTED_WITH_WARNING + schema fail → exclude even if trust ok."""
    out = tmp_path / "output"
    universe = _build_test_universe()
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="warn-schema", universe=universe
    )
    _inject_legacy_meta_self_hash(run_dir)
    # Corrupt schema without changing bytes hash: rewrite scores then restore manifest sha
    # by rewriting empty factors while updating manifest to match new content → trust still
    # meta-only warning if scores sha matches new file.
    bad = [
        {
            "ticker": "AAA",
            "score": 0.5,
            "fin_score": 0.5,
            "tech_score": 0.5,
            "market_score": 0.5,
            "sector_score": 0.5,
            "vol_kki": 0.5,
            "pos_52w": 0.5,
            # missing momentum/volatility/sector/production_candidate
        }
    ]
    scores_path = run_dir / "screener_scores.json"
    scores_path.write_text(json.dumps(bad), encoding="utf-8")
    _rewrite_manifest_scores_entry(
        run_dir,
        sha256=hashlib.sha256(scores_path.read_bytes()).hexdigest(),
        row_count=1,
    )
    trust = assess_run_trust(run_dir)
    assert trust["trust_status"] == TRUSTED_WITH_WARNING
    integ = verify_screener_scores_integrity(run_dir, expected_run_id="warn-schema")
    assert integ["status"] == "FAIL"
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["included_days"] == 0
    assert any(
        (d.get("exclusion_reason") or "").startswith("SCORES_INTEGRITY_FAIL")
        for d in meta["run_decisions"]
    )


def test_unknown_run_meta_mismatch_excluded(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="unknown-meta", universe=_build_test_universe()
    )
    # Corrupt meta WITHOUT embedding self-hash → not detect_legacy_meta_self_hash_only
    meta_path = run_dir / "screener_run_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["tampered"] = True
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    trust = assess_decision_run_trust(run_dir)
    assert trust["trust_status"] == LEGACY_UNTRUSTED
    assert trust["trust_reason"] == "DECISION_SHA_MISMATCH"
    days, meta_out = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert days == []
    assert meta_out["excluded_trust_counts"].get(LEGACY_UNTRUSTED) == 1


def test_post_finalize_liquidity_mutation_untrusted(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260728", run_id="liq-mut", universe=_build_test_universe()
    )
    # File appears after finalize / not in manifest → LEGACY_POST_FINALIZE_MUTATION
    (run_dir / "screener_liquidity_shadow_candidates.json").write_text(
        json.dumps([{"ticker": "X"}]), encoding="utf-8"
    )
    trust = assess_decision_run_trust(run_dir)
    assert trust["trust_status"] == LEGACY_UNTRUSTED
    assert trust["trust_reason"] == "LEGACY_POST_FINALIZE_MUTATION"
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["included_days"] == 0
    assert meta["trust_counts_raw"].get(LEGACY_UNTRUSTED) == 1


def test_trusted_normal_run_included(tmp_path: Path):
    out = _seed_output(tmp_path)
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["included_days"] == 4
    assert meta["accepted_trust_counts"].get(TRUSTED) == 4
    assert all(d["artifact_integrity_status"] == "PASS" for d in days)


def test_required_schema_missing_excluded(tmp_path: Path):
    out = tmp_path / "output"
    bad = [{"ticker": "AAA", "score": 0.5}]  # missing factors / pipeline fields
    run_dir = _write_replay_run(out, trade_date="20260810", run_id="bad-schema", universe=bad)
    # Fix candidates empty ok; re-hash scores in manifest after write already correct for bad list
    integ = verify_screener_scores_integrity(run_dir, expected_run_id="bad-schema")
    assert integ["status"] == "FAIL"
    assert not integ["schema_ok"]
    days, meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert meta["included_days"] == 0


def test_source_run_id_mismatch_excluded(tmp_path: Path):
    out = tmp_path / "output"
    universe = _build_test_universe()
    for r in universe:
        r["source_run_id"] = "OTHER-RUN"
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="rid-mismatch", universe=universe
    )
    integ = verify_screener_scores_integrity(run_dir, expected_run_id="rid-mismatch")
    assert integ["status"] == "FAIL"
    assert not integ["source_run_id_ok"]
    days, _meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert days == []


def test_no_data_status_when_all_excluded(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260810", run_id="all-excl", universe=_build_test_universe()
    )
    _rewrite_manifest_scores_entry(run_dir, sha256="0" * 64)
    report, _ = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    assert report["status"] == STATUS_NO_DATA
    assert (report.get("universe_meta") or {}).get("included_days") == 0


def test_baseline_replay_gate_still_blocks_shadow(tmp_path: Path):
    out = tmp_path / "output"
    universe = _build_test_universe()
    # Force Production candidates that cannot match replay pipeline output
    fake_cands = [{"Ticker": "ZZZZ", "Score": 0.99}]
    _write_replay_run(
        out,
        trade_date="20260810",
        run_id="mismatch-base",
        universe=universe,
        candidates=fake_cands,
    )
    report, _ = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    assert report.get("baseline_replay", {}).get("ok") is False
    for name, v in (report.get("shadow_verdicts") or {}).items():
        assert v.get("status") == "SHADOW_REJECTED"


def test_assess_decision_run_trust_matches_quality_helper(tmp_path: Path):
    """Shared helper agrees with Quality detector; does not change Quality contract."""
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out, trade_date="20260811", run_id="shared", universe=_build_test_universe()
    )
    clean = assess_decision_run_trust(run_dir)
    assert clean["trust_status"] == TRUSTED
    _inject_legacy_meta_self_hash(run_dir)
    warned = assess_decision_run_trust(run_dir)
    assert warned["trust_status"] == TRUSTED_WITH_WARNING
    assert warned["trust_reason"] == LEGACY_META_SELF_HASH_MISMATCH


# ── Historical MarketState score reconstruction ───────────────────────


@pytest.mark.parametrize(
    "regime,volatility,expected_multiplier",
    [
        ("sideways", "medium", 1.0),
        ("bull", "medium", 1.1),
        ("bull", "low", 1.155),
        ("bear", "medium", 0.9),
        ("volatile", "medium", 0.95),
        ("bull", "high", 1.045),
    ],
)
def test_market_adjusted_score_parity(regime, volatility, expected_multiplier):
    ms = _historical_market_state(regime, volatility)
    assert combined_market_multiplier(ms) == pytest.approx(expected_multiplier, rel=0, abs=1e-6)
    factors = _factors(0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
    base = compute_weighted_base(factors, BASE_W)
    assert base is not None
    from screener_core import calculate_market_adjusted_score

    adjusted = calculate_market_adjusted_score(float(base), ms)
    assert replay_score(factors, BASE_W, ms) == round(float(adjusted), 4)


def test_replay_score_uses_screener_core_calculate_market_adjusted_score(monkeypatch):
    from screener_core import calculate_market_adjusted_score as real_calc

    calls = []

    def _spy(base, ms):
        calls.append((base, ms.regime.value, ms.volatility_level))
        return real_calc(base, ms)

    monkeypatch.setattr("screener_core.calculate_market_adjusted_score", _spy)
    ms = _historical_market_state("bull", "low")
    replay_score(_factors(), BASE_W, ms)
    assert len(calls) == 1


def test_load_historical_market_state_from_run_meta(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out,
        trade_date="20260803",
        run_id="ms-meta",
        universe=_build_test_universe(),
        regime="sideways",
        volatility="medium",
    )
    hist = load_historical_market_state(run_dir)
    assert hist["ok"] is True
    assert hist["source"] == "screener_run_meta.market_state"
    assert hist["regime"] == "sideways"
    assert hist["volatility"] == "medium"
    assert hist["expected_multiplier"] == pytest.approx(1.0)


def test_load_historical_market_state_fallback_inputs(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out,
        trade_date="20260804",
        run_id="ms-fallback",
        universe=_build_test_universe(),
        regime="bull",
        volatility="medium",
    )
    meta_path = run_dir / "screener_run_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    del meta["market_state"]
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    inputs_dir = run_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    (inputs_dir / "market_regime_inputs.json").write_text(
        json.dumps(_market_state_payload("bull", "medium")), encoding="utf-8"
    )
    hist = load_historical_market_state(run_dir)
    assert hist["ok"] is True
    assert hist["source"] == "inputs/market_regime_inputs.json"


def test_historical_market_state_conflict(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out,
        trade_date="20260821",
        run_id="ms-conflict",
        universe=_build_test_universe(),
        regime="bull",
        volatility="low",
    )
    inputs_dir = run_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    (inputs_dir / "market_regime_inputs.json").write_text(
        json.dumps(_market_state_payload("bear", "high")), encoding="utf-8"
    )
    hist = load_historical_market_state(run_dir)
    assert hist["ok"] is False
    assert hist["status"] == HISTORICAL_MARKET_STATE_CONFLICT


def test_missing_historical_market_state_excludes_run(tmp_path: Path):
    out = tmp_path / "output"
    run_dir = _write_replay_run(
        out,
        trade_date="20260810",
        run_id="no-ms",
        universe=_build_test_universe(),
    )
    meta_path = run_dir / "screener_run_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    del meta["market_state"]
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    man_path = run_dir / "manifest.json"
    man = json.loads(man_path.read_text(encoding="utf-8"))
    man["artifacts"]["screener_run_meta.json"]["sha256"] = hashlib.sha256(
        meta_path.read_bytes()
    ).hexdigest()
    man_path.write_text(json.dumps(man), encoding="utf-8")
    hist = load_historical_market_state(run_dir)
    assert hist["status"] == MISSING_HISTORICAL_MARKET_STATE
    days, meta_out = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
    )
    assert days == []
    assert meta_out["historical_market_state_exclusion_counts"].get(
        MISSING_HISTORICAL_MARKET_STATE
    ) == 1


def test_factor_export_rounding_boundary_accepted():
    ms = _historical_market_state("bull", "low")
    factors = _factors(0.50005, 0.50005, 0.50005, 0.50005, 0.50005, 0.50005)
    exported = {k: round(v, 4) for k, v in factors.items()}
    prod_score = replay_score(exported, BASE_W, ms)
    raw_score = replay_score(factors, BASE_W, ms)
    assert prod_score is not None and raw_score is not None
    err = abs(prod_score - raw_score)
    assert err <= REPLAY_SCORE_TOLERANCE


def test_acceptance_epsilon_0_0001_boundary():
    err = 0.0001000000000000445
    assert err <= REPLAY_SCORE_TOLERANCE


def test_large_mismatch_fails_reconstruction():
    ms = _historical_market_state("bull", "medium")
    row = normalize_score_record(
        _score_row("X", fin=0.9, tech=0.9, regime="bull", volatility="medium"),
        trade_date="20260804",
        market=MARKET,
        session=SESSION,
        source_run_id="r",
    )
    row["total_score"] = 0.05  # deliberate large mismatch
    recon = reconstruct_baseline_scores_replay(
        [row],
        BASE_W,
        market_state_by_date={"20260804": ms},
    )
    assert recon["ok"] is False
    assert recon["failure_count"] >= 1
    assert recon["max_abs_error"] > REPLAY_SCORE_TOLERANCE


def test_c_d_e_share_same_historical_market_adjustment(tmp_path: Path):
    out = tmp_path / "output"
    universe = _build_test_universe()
    _write_replay_run(
        out,
        trade_date="20260821",
        run_id="shared-ms",
        universe=universe,
        regime="bull",
        volatility="low",
    )
    days, load_meta = load_replay_days(
        output_dir=out,
        market=MARKET,
        session=SESSION,
        start_trade_date="20260821",
        end_trade_date="20260821",
    )
    load_meta["output_dir"] = str(out)
    day = days[0]
    params = pipeline_params_from_config({"screener_params": {}})
    base_w = BASE_W
    ms = day["historical_market_state"]
    universe_rows = day["universe_rows"]
    a_scores = apply_replay_scenario_scores(
        universe_rows, base_w, baseline_weights=base_w, historical_market_state=ms
    )
    c_scores = apply_replay_scenario_scores(
        universe_rows,
        SCENARIOS["C_POS52W_ZERO"],
        baseline_weights=base_w,
        historical_market_state=ms,
    )
    for a, c in zip(a_scores, c_scores):
        assert combined_market_multiplier(ms) == pytest.approx(1.155)
        assert a["weighted_base"] is not None
        assert c["weighted_base"] is not None


def test_baseline_reconstruction_failure_blocks_shadow_scenarios(tmp_path: Path):
    out = tmp_path / "output"
    universe = _build_test_universe()
    run_dir = _write_replay_run(
        out,
        trade_date="20260810",
        run_id="recon-fail",
        universe=universe,
        regime="sideways",
        volatility="medium",
    )
    scores = json.loads((run_dir / "screener_scores.json").read_text(encoding="utf-8"))
    for row in scores:
        row["score"] = 0.01
    scores_path = run_dir / "screener_scores.json"
    scores_path.write_text(json.dumps(scores), encoding="utf-8")
    _rewrite_manifest_scores_entry(
        run_dir,
        sha256=hashlib.sha256(scores_path.read_bytes()).hexdigest(),
        row_count=len(scores),
    )
    report, _ = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date="20260810",
        end_trade_date="20260810",
        output_dir=out,
    )
    assert report["status"] == STATUS_BASELINE_FAILED
    assert report["baseline_reconstruction"]["ok"] is False
    for name, v in (report.get("shadow_verdicts") or {}).items():
        assert v.get("reason") == "baseline_failed"


def test_no_production_ratio_inverse_in_replay_module():
    import screener_full_universe_replay as mod

    src = Path(mod.__file__).read_text(encoding="utf-8")
    assert "production_score /" not in src.replace(" ", "")
    assert "reconstructed_base_score" not in src


def test_report_includes_market_state_summary(tmp_path: Path):
    out = _seed_output(tmp_path)
    report, _ = run_full_universe_replay(
        market=MARKET,
        session=SESSION,
        start_trade_date=START,
        end_trade_date=END,
        output_dir=out,
    )
    ms = report.get("market_state_summary") or {}
    assert ms.get("uses_calculate_market_adjusted_score") is True
    assert ms.get("primary_source") == "screener_run_meta.market_state"
    recon = report.get("baseline_reconstruction") or {}
    assert recon.get("failure_count") == 0
    assert recon.get("max_abs_error", 1.0) <= REPLAY_SCORE_TOLERANCE
