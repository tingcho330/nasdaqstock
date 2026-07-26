"""Multi-day screener quality reports and candidate observation ledger.

Decision-run only by default. Never feeds trader / GPT order inputs.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger("screener_quality")

INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE_FOR_POLICY_CHANGE"
MIN_SAMPLE_FOR_POLICY = 15


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def _load_json(path: Path) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("JSON load failed %s: %s", path, e)
        return None


def load_runtime_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Reuse the screener's JSONC config loader (comments, defaults)."""
    try:
        from utils import CONFIG_PATH, get_cfg, load_config

        path = Path(config_path) if config_path else CONFIG_PATH
        cfg = get_cfg(path)
        return cfg if isinstance(cfg, dict) else {}
    except Exception as e:
        logger.warning("runtime config load failed: %s", e)
        return {}


def discover_decision_runs(
    output_dir: Path,
    *,
    market: str,
    session: Optional[str] = None,
    days: int = 20,
    decision_only: bool = True,
) -> List[Path]:
    """Find immutable run directories under output/runs/DECISION/..."""
    runs_root = Path(output_dir) / "runs"
    modes = ["DECISION"] if decision_only else ["DECISION", "REPLAY"]
    found: List[Tuple[str, Path]] = []
    for mode in modes:
        base = runs_root / mode / str(market).upper()
        if not base.exists():
            continue
        for date_dir in sorted(base.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.isdigit():
                continue
            for sess_dir in sorted(date_dir.iterdir()):
                if not sess_dir.is_dir():
                    continue
                if session and sess_dir.name != session:
                    continue
                for run_dir in sorted(sess_dir.iterdir()):
                    if not run_dir.is_dir():
                        continue
                    meta_path = run_dir / "screener_run_meta.json"
                    if not meta_path.exists():
                        continue
                    found.append((date_dir.name, run_dir))

    # Keep latest run per trade_date+session
    by_key: Dict[Tuple[str, str], Path] = {}
    for trade_date, run_dir in found:
        sess = run_dir.parent.name
        key = (trade_date, sess)
        # lexicographic run_id / mtime preference: last wins in sorted order
        by_key[key] = run_dir

    ordered = sorted(by_key.items(), key=lambda kv: kv[0][0])
    if days > 0:
        ordered = ordered[-int(days) :]
    return [p for _, p in ordered]


def _meta_of(run_dir: Path) -> Dict[str, Any]:
    data = _load_json(run_dir / "screener_run_meta.json") or {}
    return data if isinstance(data, dict) else {}


def _candidates_of(run_dir: Path, name: str = "screener_candidates.json") -> List[Dict[str, Any]]:
    data = _load_json(run_dir / name)
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


def _scores_of(run_dir: Path) -> List[Dict[str, Any]]:
    data = _load_json(run_dir / "screener_scores.json")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


def _ticker(row: Dict[str, Any]) -> str:
    return str(row.get("Ticker") or row.get("ticker") or "").upper()


def aggregate_quality_report(
    run_dirs: Sequence[Path],
    *,
    market: str,
    session: Optional[str] = None,
    min_sample_for_policy: int = MIN_SAMPLE_FOR_POLICY,
) -> Dict[str, Any]:
    """Aggregate Decision-run observability into a quality report payload."""
    days_meta: List[Dict[str, Any]] = []
    empty_reasons: Counter = Counter()
    prod_freq: Counter = Counter()
    hc_freq: Counter = Counter()
    elig_freq: Counter = Counter()
    liq_freq: Counter = Counter()
    sector_freq: Counter = Counter()
    issuer_freq: Counter = Counter()
    held_excl = 0
    thr_pass_total = 0
    high_tech_low_fin = 0
    scored_total = 0
    amount5d_pass_rates: List[float] = []
    cache_hits = 0
    cache_miss = 0
    cache_failed = 0
    prev_prod: Optional[Set[str]] = None
    turnover_rates: List[float] = []
    score_series: List[Dict[str, Any]] = []

    for run_dir in run_dirs:
        meta = _meta_of(run_dir)
        if str(meta.get("run_mode") or "").upper() == "REPLAY":
            # Extra guard even if discover filtered
            continue
        trade_date = str(meta.get("trade_date") or run_dir.parent.parent.name)
        prod = _candidates_of(run_dir, "screener_candidates.json")
        hc = _candidates_of(run_dir, "screener_shadow_candidates.json")
        elig = _candidates_of(run_dir, "screener_eligible_shadow_candidates.json")
        liq = _candidates_of(run_dir, "screener_liquidity_shadow_candidates.json")
        scores = _scores_of(run_dir)

        prod_set = {_ticker(r) for r in prod if _ticker(r)}
        for t in prod_set:
            prod_freq[t] += 1
        for r in hc:
            t = _ticker(r)
            if t:
                hc_freq[t] += 1
        for r in elig:
            t = _ticker(r)
            if t:
                elig_freq[t] += 1
        for r in liq:
            t = _ticker(r)
            if t:
                liq_freq[t] += 1

        for r in prod:
            sec = str(r.get("Sector") or r.get("sector") or "UNKNOWN")
            sector_freq[sec] += 1
            iss = str(r.get("issuer_group") or _ticker(r))
            issuer_freq[iss] += 1

        for r in scores:
            scored_total += 1
            reasons = r.get("exclusion_reasons") or []
            if "ALREADY_HELD" in reasons:
                held_excl += 1
            flags = r.get("diagnostic_flags") or []
            if "HIGH_TECH_LOW_FIN" in flags:
                high_tech_low_fin += 1
            if r.get("threshold_pass"):
                thr_pass_total += 1

        if prev_prod is not None:
            union = prod_set | prev_prod
            if union:
                churn = len(prod_set.symmetric_difference(prev_prod)) / len(union)
                turnover_rates.append(churn)
        prev_prod = prod_set

        er = meta.get("empty_reason")
        if er:
            empty_reasons[str(er)] += 1
        sd = meta.get("score_distribution") or {}
        score_series.append(
            {
                "trade_date": trade_date,
                "count": sd.get("count"),
                "mean": sd.get("mean"),
                "median": sd.get("median"),
                "p90": sd.get("p90"),
                "production_candidate_count": meta.get("production_candidate_count"),
                "empty_reason": er,
                "result_status": meta.get("result_status"),
            }
        )

        amt = meta.get("amount5d_distribution") or {}
        if amt.get("pass_rate") is not None:
            amount5d_pass_rates.append(float(amt["pass_rate"]))
        elif amt.get("count") and amt.get("pass_count") is not None:
            amount5d_pass_rates.append(float(amt["pass_count"]) / max(1, float(amt["count"])))

        cache = meta.get("amount5d_cache") or {}
        cache_hits += int(cache.get("hit") or cache.get("hits") or 0)
        cache_miss += int(cache.get("miss") or cache.get("misses") or 0)
        cache_failed += int(cache.get("failed") or cache.get("failures") or 0)

        avail = meta.get("candidate_availability") or {}
        days_meta.append(
            {
                "trade_date": trade_date,
                "run_id": meta.get("run_id"),
                "run_directory": str(run_dir),
                "result_status": meta.get("result_status"),
                "empty_reason": er,
                "production_candidate_count": meta.get("production_candidate_count", len(prod)),
                "threshold_pass_count": avail.get("threshold_pass_count"),
                "eligible_new_buy_count": avail.get("eligible_new_buy_count"),
                "high_conviction_shadow_count": (meta.get("shadow") or {}).get("candidate_count", len(hc)),
                "eligible_shadow_count": (meta.get("eligible_shadow") or {}).get("candidate_count", len(elig)),
                "liquidity_shadow_count": (meta.get("liquidity_shadow") or {}).get("candidate_count", len(liq)),
                "stage_drop_summary": meta.get("stage_drop_summary") or {},
                "exclusion_summary": meta.get("exclusion_summary") or {},
                "stage_durations_sec": meta.get("stage_durations_sec") or {},
                "data_quality_findings": meta.get("data_quality_findings") or [],
            }
        )

    n_days = len(days_meta)
    prod_days = sum(1 for d in days_meta if int(d.get("production_candidate_count") or 0) > 0)
    empty_valid_days = sum(1 for d in days_meta if d.get("result_status") == "EMPTY_VALID")
    sample_status = (
        INSUFFICIENT_SAMPLE if n_days < int(min_sample_for_policy) else "ADEQUATE_SAMPLE"
    )

    start = days_meta[0]["trade_date"] if days_meta else None
    end = days_meta[-1]["trade_date"] if days_meta else None

    return {
        "schema_version": 1,
        "market": market,
        "session": session,
        "start_trade_date": start,
        "end_trade_date": end,
        "trading_days": n_days,
        "production_candidate_days": prod_days,
        "empty_valid_days": empty_valid_days,
        "empty_reason_distribution": dict(empty_reasons),
        "score_series": score_series,
        "score_count_mean": (
            round(
                sum(float(s.get("count") or 0) for s in score_series) / n_days,
                4,
            )
            if n_days
            else None
        ),
        "production_threshold_pass_total": thr_pass_total,
        "already_held_exclusion_ratio": (
            round(held_excl / scored_total, 6) if scored_total else None
        ),
        "high_tech_low_fin_ratio": (
            round(high_tech_low_fin / scored_total, 6) if scored_total else None
        ),
        "amount5d_production_pass_rate_mean": (
            round(sum(amount5d_pass_rates) / len(amount5d_pass_rates), 6)
            if amount5d_pass_rates
            else None
        ),
        "cache": {"hit": cache_hits, "miss": cache_miss, "failed": cache_failed},
        "repeated_production_candidates": dict(prod_freq.most_common()),
        "unique_production_candidates": len(prod_freq),
        "daily_candidate_turnover_mean": (
            round(sum(turnover_rates) / len(turnover_rates), 6) if turnover_rates else None
        ),
        "sector_concentration": dict(sector_freq.most_common(20)),
        "issuer_concentration": dict(issuer_freq.most_common(20)),
        "high_conviction_shadow_frequency": dict(hc_freq.most_common()),
        "eligible_shadow_frequency": dict(elig_freq.most_common()),
        "liquidity_shadow_frequency": dict(liq_freq.most_common()),
        "days": days_meta,
        "sample_status": sample_status,
        "min_sample_for_policy_change": int(min_sample_for_policy),
        "policy_change_recommendation": sample_status,
        "used_by_trader": False,
        "decision_only": True,
        "candidate_performance": {
            "note": "Returns are null when future price evidence is insufficient; never coerced to 0.",
            "horizons": ["next_decision", "1d", "3d", "5d"],
        },
    }


def render_quality_markdown(report: Dict[str, Any]) -> str:
    lines = [
        f"# Screener Quality Report — {report.get('market')} "
        f"{report.get('start_trade_date')}→{report.get('end_trade_date')}",
        "",
        f"- trading_days: {report.get('trading_days')}",
        f"- production_candidate_days: {report.get('production_candidate_days')}",
        f"- empty_valid_days: {report.get('empty_valid_days')}",
        f"- sample_status: `{report.get('sample_status')}`",
        f"- used_by_trader: {report.get('used_by_trader', False)}",
        "",
        "## Empty Reason Distribution",
    ]
    for k, v in (report.get("empty_reason_distribution") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Repeated Production Candidates")
    for k, v in (report.get("repeated_production_candidates") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Eligible-only Shadow Frequency")
    for k, v in (report.get("eligible_shadow_frequency") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Score Series")
    for row in report.get("score_series") or []:
        lines.append(
            f"- {row.get('trade_date')}: count={row.get('count')} mean={row.get('mean')} "
            f"p90={row.get('p90')} prod={row.get('production_candidate_count')} "
            f"empty={row.get('empty_reason')}"
        )
    lines.append("")
    if report.get("sample_status") == INSUFFICIENT_SAMPLE:
        lines.append(f"**{INSUFFICIENT_SAMPLE}** — do not change Production thresholds yet.")
        lines.append("")
    return "\n".join(lines) + "\n"


def observation_key(decision_run_id: str, ticker: str, candidate_type: str) -> str:
    return f"{decision_run_id}|{str(ticker).upper()}|{candidate_type}"


def upsert_observation_ledger(
    ledger_path: Path,
    rows: Sequence[Dict[str, Any]],
) -> int:
    """Idempotent upsert by decision_run_id + ticker + candidate_type."""
    ledger_path = Path(ledger_path)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    existing: Dict[str, Dict[str, Any]] = {}
    if ledger_path.exists():
        with open(ledger_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                key = observation_key(
                    str(rec.get("decision_run_id") or ""),
                    str(rec.get("ticker") or ""),
                    str(rec.get("candidate_type") or ""),
                )
                existing[key] = rec

    for row in rows:
        key = observation_key(
            str(row.get("decision_run_id") or ""),
            str(row.get("ticker") or ""),
            str(row.get("candidate_type") or ""),
        )
        prev = existing.get(key, {})
        merged = dict(prev)
        merged.update(row)
        # Preserve already-filled returns; never overwrite with null unless missing
        for field in (
            "return_1d_pct",
            "return_3d_pct",
            "return_5d_pct",
            "max_drawdown_5d_pct",
            "decision_price",
        ):
            if merged.get(field) is None and prev.get(field) is not None:
                merged[field] = prev.get(field)
        if merged.get("outcome_status") is None:
            merged["outcome_status"] = "PENDING"
        existing[key] = merged

    with open(ledger_path, "w", encoding="utf-8") as f:
        for rec in existing.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(existing)


def build_observation_rows_from_run(run_dir: Path) -> List[Dict[str, Any]]:
    meta = _meta_of(run_dir)
    run_id = str(meta.get("run_id") or run_dir.name)
    trade_date = str(meta.get("trade_date") or "")
    as_of = meta.get("as_of_kst")
    rows: List[Dict[str, Any]] = []

    def _add(items: List[Dict[str, Any]], ctype: str) -> None:
        for r in items:
            t = _ticker(r)
            if not t:
                continue
            score = _safe_float(r.get("Score") if "Score" in r else r.get("score"))
            price = _safe_float(r.get("Price") if "Price" in r else r.get("price")) or 0
            rows.append(
                {
                    "decision_run_id": run_id,
                    "trade_date": trade_date,
                    "ticker": t,
                    "candidate_type": ctype,
                    "decision_score": score,
                    "decision_price": price,
                    "decision_price_source": "screener_artifact",
                    "decision_as_of_kst": as_of,
                    "return_1d_pct": None,
                    "return_3d_pct": None,
                    "return_5d_pct": None,
                    "max_drawdown_5d_pct": None,
                    "outcome_status": "PENDING",
                    "used_by_trader": False,
                }
            )

    _add(_candidates_of(run_dir, "screener_candidates.json"), "PRODUCTION")
    _add(_candidates_of(run_dir, "screener_shadow_candidates.json"), "HIGH_CONVICTION_SHADOW")
    _add(_candidates_of(run_dir, "screener_eligible_shadow_candidates.json"), "ELIGIBLE_SHADOW")
    _add(_candidates_of(run_dir, "screener_liquidity_shadow_candidates.json"), "LIQUIDITY_SHADOW")
    return rows


def write_quality_report(
    report: Dict[str, Any],
    output_dir: Path,
    *,
    market: str,
) -> Tuple[Path, Path]:
    out = Path(output_dir) / "quality"
    out.mkdir(parents=True, exist_ok=True)
    start = report.get("start_trade_date") or "UNKNOWN"
    end = report.get("end_trade_date") or "UNKNOWN"
    stem = f"screener_quality_{start}_{end}_{market}"
    json_path = out / f"{stem}.json"
    md_path = out / f"{stem}.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    md_path.write_text(render_quality_markdown(report), encoding="utf-8")
    return json_path, md_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Screener multi-day quality report")
    parser.add_argument("--market", default=os.getenv("MARKET", "SP500"))
    parser.add_argument("--session", default=None)
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--decision-only", action="store_true", default=True)
    parser.add_argument("--include-replay", action="store_true", default=False)
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", str(Path(__file__).resolve().parents[1] / "output")),
    )
    parser.add_argument("--config", default=None, help="Optional config path (runtime loader)")
    parser.add_argument("--update-ledger", action="store_true", default=True)
    args = parser.parse_args(list(argv) if argv is not None else None)

    # Prefer immutable Decision config snapshots when present; still load runtime cfg
    _ = load_runtime_config(Path(args.config) if args.config else None)

    decision_only = not bool(args.include_replay)
    run_dirs = discover_decision_runs(
        Path(args.output_dir),
        market=args.market,
        session=args.session,
        days=args.days,
        decision_only=decision_only,
    )
    report = aggregate_quality_report(
        run_dirs,
        market=args.market,
        session=args.session,
    )
    json_path, md_path = write_quality_report(report, Path(args.output_dir), market=args.market)
    logger.info("quality report: %s / %s (days=%d)", json_path, md_path, report.get("trading_days"))

    if args.update_ledger:
        ledger = Path(args.output_dir) / "quality" / "screener_candidate_observations.jsonl"
        rows: List[Dict[str, Any]] = []
        for rd in run_dirs:
            rows.extend(build_observation_rows_from_run(rd))
        n = upsert_observation_ledger(ledger, rows)
        logger.info("observation ledger upserted entries=%d path=%s", n, ledger)

    print(json.dumps({"json": str(json_path), "md": str(md_path), "days": report.get("trading_days")}, indent=2))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(main())
