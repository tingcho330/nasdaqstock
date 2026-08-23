"""Monthly quality review package CLI (observability only).

Does not change Production thresholds, Amount5D, scoring, trader, or GPT weights.
Never mutates finalized DECISION / diagnostics / gpt_trades artifacts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger("monthly_quality_review")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_tree_filtered(src: Path, dest: Path, *, max_files: int = 500) -> int:
    if not src.exists():
        return 0
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    if src.is_file():
        shutil.copy2(src, dest / src.name)
        return 1
    for p in sorted(src.rglob("*")):
        if not p.is_file():
            continue
        if n >= max_files:
            break
        rel = p.relative_to(src)
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, target)
        n += 1
    return n


def build_monthly_package(
    *,
    output_dir: Path,
    market: str,
    session: str,
    date_from: str,
    date_to: str,
    include_gpt_logs: bool = False,
) -> Path:
    from screener_quality import (
        aggregate_quality_report,
        build_observation_rows_from_run,
        discover_decision_runs,
        upsert_observation_ledger,
        write_quality_report,
    )
    from screener_outcomes import backfill_candidate_outcomes
    from gpt_quality import build_gpt_quality_report, write_gpt_quality_report

    out = Path(output_dir)
    pkg_name = f"monthly_quality_{date_from}_{date_to}_{market}"
    pkg = out / "review_packages" / pkg_name
    pkg.mkdir(parents=True, exist_ok=True)

    discovered = discover_decision_runs(
        out,
        market=market,
        session=session,
        days=60,
        decision_only=True,
    )

    # Filter by date range
    run_dirs = []
    for rd in discovered.run_dirs:
        # trade_date is typically .../MARKET/YYYYMMDD/session/run_id
        parts = Path(rd).parts
        td = None
        for p in parts:
            if len(p) == 8 and p.isdigit():
                td = p
        if td and (td < date_from or td > date_to):
            continue
        if td is None:
            continue
        run_dirs.append(Path(rd))

    report = aggregate_quality_report(
        run_dirs,
        market=market,
        session=session,
        discovery=getattr(discovered, "discovery", None),
        merged_by_run=getattr(discovered, "merged_by_run", None),
        decision_only=True,
    )
    # Force reported window
    report["start_trade_date"] = date_from
    report["end_trade_date"] = date_to

    jq, mq = write_quality_report(report, pkg, market=market)
    # Also canonical names inside package
    shutil.copy2(jq, pkg / "screener_quality.json")
    shutil.copy2(mq, pkg / "screener_quality.md")

    ledger = out / "quality" / "screener_candidate_observations.jsonl"
    rows: List[Dict[str, Any]] = []
    for rd in run_dirs:
        rows.extend(build_observation_rows_from_run(rd, output_dir=out))
    upsert_observation_ledger(ledger, rows)
    existing: List[Dict[str, Any]] = []
    if ledger.exists():
        with open(ledger, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    existing.append(json.loads(line))
                except Exception:
                    continue
    settled = backfill_candidate_outcomes(
        existing, as_of_trade_date=date_to, only_trusted=False
    )
    upsert_observation_ledger(ledger, settled)
    shutil.copy2(ledger, pkg / "screener_candidate_observations.jsonl")

    gpt_report = build_gpt_quality_report(
        out,
        market=market,
        session=session,
        date_from=date_from,
        date_to=date_to,
        outcome_ledger_path=ledger,
    )
    write_gpt_quality_report(gpt_report, pkg, stem="gpt_quality")

    # Bundle references (copy, never mutate sources)
    dec_dest = pkg / "decision_runs"
    diag_dest = pkg / "post_run_diagnostics"
    gpt_dest = pkg / "gpt_trades"
    fixed_dest = pkg / "production_fixed_candidates"
    dec_dest.mkdir(exist_ok=True)
    diag_dest.mkdir(exist_ok=True)
    gpt_dest.mkdir(exist_ok=True)
    fixed_dest.mkdir(exist_ok=True)

    integrity: Dict[str, Any] = {"files": {}}
    for rd in run_dirs:
        target = dec_dest / Path(rd).name
        _copy_tree_filtered(Path(rd), target, max_files=80)
        man = Path(rd) / "manifest.json"
        if man.exists():
            integrity["files"][f"decision_runs/{Path(rd).name}/manifest.json"] = {
                "sha256": _sha256_file(man)
            }

    diag_root = out / "post_run_diagnostics" / market.upper()
    if diag_root.exists():
        for dpath in sorted(diag_root.rglob("diagnostics_manifest.json")):
            parent = dpath.parent
            # date folder filter
            parts = parent.parts
            dates = [p for p in parts if len(p) == 8 and p.isdigit()]
            if dates and (dates[-1] < date_from or dates[-1] > date_to):
                continue
            rel = parent.relative_to(diag_root)
            _copy_tree_filtered(parent, diag_dest / rel, max_files=40)

    for gp in sorted(out.glob(f"gpt_trades_*_{session}_{market}.json")):
        # date in name
        name = gp.name
        bits = name.split("_")
        d = bits[2] if len(bits) > 2 else ""
        if d.isdigit() and len(d) == 8 and (d < date_from or d > date_to):
            continue
        shutil.copy2(gp, gpt_dest / gp.name)
        if include_gpt_logs:
            log = out / "logs" / gp.name.replace(".json", ".log")
            if log.exists():
                (pkg / "gpt_logs").mkdir(exist_ok=True)
                shutil.copy2(log, pkg / "gpt_logs" / log.name)

    for fp in sorted(out.glob(f"screener_candidates_*_{session}_{market}.json")):
        d = fp.name.split("_")[2] if len(fp.name.split("_")) > 2 else ""
        if d.isdigit() and (d < date_from or d > date_to):
            continue
        shutil.copy2(fp, fixed_dest / fp.name)

    # Optional performance review copy if present
    pr_dir = out / "performance_reviews"
    if pr_dir.exists():
        for p in sorted(pr_dir.glob("*.json"))[-3:]:
            shutil.copy2(p, pkg / p.name)

    package_index = {
        "package": pkg_name,
        "market": market,
        "session": session,
        "from": date_from,
        "to": date_to,
        "decision_run_count": len(run_dirs),
        "screener_quality": "screener_quality.json",
        "gpt_quality": "gpt_quality.json",
        "observation_ledger": "screener_candidate_observations.jsonl",
        "production_policy_unchanged": True,
        "note": "Quality package only — Production parameters not modified.",
    }
    with open(pkg / "package_index.json", "w", encoding="utf-8") as f:
        json.dump(package_index, f, ensure_ascii=False, indent=2)
    with open(pkg / "integrity_manifest.json", "w", encoding="utf-8") as f:
        json.dump(integrity, f, ensure_ascii=False, indent=2)

    logger.info("monthly package written: %s", pkg)
    return pkg


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build monthly screener/GPT quality package")
    parser.add_argument("--market", default=os.getenv("MARKET", "SP500"))
    parser.add_argument("--session", default="pm")
    parser.add_argument("--from", dest="date_from", required=True)
    parser.add_argument("--to", dest="date_to", required=True)
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", str(Path(__file__).resolve().parents[1] / "output")),
    )
    parser.add_argument("--include-gpt-logs", action="store_true", default=False)
    args = parser.parse_args(list(argv) if argv is not None else None)

    pkg = build_monthly_package(
        output_dir=Path(args.output_dir),
        market=args.market,
        session=args.session,
        date_from=args.date_from,
        date_to=args.date_to,
        include_gpt_logs=bool(args.include_gpt_logs),
    )
    print(json.dumps({"package": str(pkg)}, indent=2))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(main())
