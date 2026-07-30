"""Multi-day screener quality reports and candidate observation ledger.

Decision-run only by default. Never feeds trader / GPT order inputs.

Run directories are discovered case-insensitively under output/runs/{decision|DECISION}/…
because ScreenerRunWriter stores mode segments in lowercase.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger("screener_quality")

INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE_FOR_POLICY_CHANGE"
MIN_SAMPLE_FOR_POLICY = 15
NOT_AVAILABLE = "NOT_AVAILABLE"
TRUSTED = "TRUSTED"
UNTRUSTED = "UNTRUSTED"
FAILED_STATUS = "FAILED"


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
        from utils import CONFIG_PATH, get_cfg

        path = Path(config_path) if config_path else CONFIG_PATH
        cfg = get_cfg(path)
        return cfg if isinstance(cfg, dict) else {}
    except Exception as e:
        logger.warning("runtime config load failed: %s", e)
        return {}


def _market_dirs_match(dir_name: str, market: str) -> bool:
    return str(dir_name).upper() == str(market).upper()


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        # Normalize to naive UTC-ish comparable values (strip tzinfo for ordering)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _run_recency_key(merged: Dict[str, Any], run_dir: Path) -> Tuple[Any, ...]:
    """Prefer completed_at_kst / started_at_kst, then mtime."""
    completed = _parse_ts(merged.get("completed_at_kst") or merged.get("finished_at_kst"))
    started = _parse_ts(merged.get("started_at_kst"))
    try:
        mtime = run_dir.stat().st_mtime
    except Exception:
        mtime = 0.0
    return (
        completed or datetime.min,
        started or datetime.min,
        float(mtime),
        str(run_dir.name),
    )


def merge_manifest_and_meta(
    manifest: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Identity from manifest; operational fields from meta; cross-fallback."""
    man = dict(manifest or {})
    met = dict(meta or {})
    merged = dict(met)
    merged.update(man)  # manifest wins for overlapping identity keys first

    # Identity: manifest preferred (already applied), then meta fallback
    for key in (
        "run_id",
        "run_mode",
        "market",
        "session",
        "trade_date",
        "status",
        "decision_artifact",
        "schema_version",
        "completed_at_kst",
        "started_at_kst",
        "git_commit",
        "build_identity",
    ):
        if merged.get(key) is None and met.get(key) is not None:
            merged[key] = met.get(key)
        if key in ("run_id", "run_mode", "market", "session", "trade_date", "status", "decision_artifact"):
            if man.get(key) is not None:
                merged[key] = man.get(key)

    # Operational: meta preferred
    for key in (
        "result_status",
        "empty_reason",
        "empty_reason_detail",
        "score_distribution",
        "production_candidate_count",
        "candidate_count",
        "shadow",
        "eligible_shadow",
        "liquidity_shadow",
        "funnel",
        "stage_drop_summary",
        "exclusion_summary",
        "candidate_availability",
        "diagnostics",
        "market_regime_shadow",
        "amount5d_distribution",
        "amount5d_cache",
        "stage_durations_sec",
        "data_quality_findings",
        "production_threshold",
        "configured_threshold",
    ):
        if met.get(key) is not None:
            merged[key] = met.get(key)
        elif man.get(key) is not None and merged.get(key) is None:
            merged[key] = man.get(key)

    return merged


@dataclass
class DiscoveryResult:
    run_dirs: List[Path] = field(default_factory=list)
    merged_by_run: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    discovery: Dict[str, Any] = field(default_factory=dict)

    def __iter__(self):
        return iter(self.run_dirs)

    def __len__(self) -> int:
        return len(self.run_dirs)


def discover_decision_runs(
    output_dir: Path,
    *,
    market: str,
    session: Optional[str] = None,
    days: int = 20,
    decision_only: bool = True,
) -> DiscoveryResult:
    """Find immutable run dirs under output/runs/{decision|DECISION|…}/…

    Directory names are matched case-insensitively. Manifest ``run_mode`` is the
    source of truth for DECISION vs REPLAY inclusion.
    """
    runs_root = Path(output_dir) / "runs"
    allowed = {"DECISION"} if decision_only else {"DECISION", "REPLAY"}
    market_u = str(market).upper()
    session_l = str(session).lower() if session else None

    manifest_count = 0
    skip_reasons: Counter = Counter()
    # key=(trade_date, session) -> (recency_key, run_dir, merged)
    best: Dict[Tuple[str, str], Tuple[Tuple[Any, ...], Path, Dict[str, Any]]] = {}
    # Track older duplicates for skip accounting after selection
    candidates: List[Tuple[Tuple[str, str], Tuple[Any, ...], Path, Dict[str, Any]]] = []

    if not runs_root.exists():
        disc = {
            "manifest_count": 0,
            "included_run_count": 0,
            "excluded_run_count": 0,
            "skip_reasons": {},
            "runs_root": str(runs_root),
            "decision_only": decision_only,
            "warning": "RUNS_ROOT_MISSING",
        }
        logger.warning("quality discovery: runs root missing: %s", runs_root)
        return DiscoveryResult(discovery=disc)

    for mode_dir in sorted(runs_root.iterdir()):
        if not mode_dir.is_dir():
            continue
        normalized_mode = mode_dir.name.upper()
        if normalized_mode not in ("DECISION", "REPLAY"):
            continue
        # Still walk both modes so we can count REPLAY_EXCLUDED when decision_only
        for mkt_dir in sorted(mode_dir.iterdir()):
            if not mkt_dir.is_dir():
                continue
            if not _market_dirs_match(mkt_dir.name, market_u):
                # Only count skips under allowed mode dirs that have manifests
                continue
            for date_dir in sorted(mkt_dir.iterdir()):
                if not date_dir.is_dir() or not date_dir.name.isdigit():
                    continue
                for sess_dir in sorted(date_dir.iterdir()):
                    if not sess_dir.is_dir():
                        continue
                    if session_l and sess_dir.name.lower() != session_l:
                        # Defer SESSION_MISMATCH until we see a manifest
                        pass
                    for run_dir in sorted(sess_dir.iterdir()):
                        if not run_dir.is_dir():
                            continue
                        man_path = run_dir / "manifest.json"
                        meta_path = run_dir / "screener_run_meta.json"
                        if not man_path.exists() and not meta_path.exists():
                            continue
                        if man_path.exists():
                            manifest_count += 1
                        manifest = _load_json(man_path) if man_path.exists() else None
                        if man_path.exists() and manifest is None:
                            skip_reasons["MANIFEST_READ_FAILED"] += 1
                            logger.debug("skip %s: MANIFEST_READ_FAILED", run_dir)
                            continue
                        if manifest is not None and not isinstance(manifest, dict):
                            skip_reasons["MANIFEST_READ_FAILED"] += 1
                            logger.debug("skip %s: MANIFEST_READ_FAILED (not dict)", run_dir)
                            continue
                        meta = _load_json(meta_path) if meta_path.exists() else None
                        if meta is not None and not isinstance(meta, dict):
                            meta = None
                        if not meta_path.exists() or meta is None:
                            skip_reasons["META_MISSING"] += 1
                            logger.debug("skip %s: META_MISSING", run_dir)
                            continue

                        merged = merge_manifest_and_meta(
                            manifest if isinstance(manifest, dict) else {},
                            meta,
                        )

                        # Path vs manifest mode conflict
                        man_mode = str(merged.get("run_mode") or "").upper()
                        if man_mode and man_mode != normalized_mode:
                            skip_reasons["RUN_MODE_MISMATCH"] += 1
                            logger.debug(
                                "skip %s: RUN_MODE_MISMATCH path=%s manifest=%s",
                                run_dir,
                                normalized_mode,
                                man_mode,
                            )
                            continue
                        if not man_mode:
                            # Fall back to path mode only as hint, still require validation
                            man_mode = normalized_mode
                            merged["run_mode"] = man_mode

                        if decision_only and man_mode != "DECISION":
                            skip_reasons["REPLAY_EXCLUDED"] += 1
                            logger.debug("skip %s: REPLAY_EXCLUDED", run_dir)
                            continue
                        if man_mode not in allowed:
                            skip_reasons["REPLAY_EXCLUDED"] += 1
                            logger.debug("skip %s: mode %s not allowed", run_dir, man_mode)
                            continue

                        mkt_val = str(merged.get("market") or mkt_dir.name).upper()
                        if mkt_val != market_u:
                            skip_reasons["MARKET_MISMATCH"] += 1
                            logger.debug("skip %s: MARKET_MISMATCH %s", run_dir, mkt_val)
                            continue

                        sess_val = str(merged.get("session") or sess_dir.name).lower()
                        if session_l and sess_val != session_l:
                            skip_reasons["SESSION_MISMATCH"] += 1
                            logger.debug("skip %s: SESSION_MISMATCH %s", run_dir, sess_val)
                            continue

                        status = str(merged.get("status") or "")
                        if status and not status.startswith("SUCCESS"):
                            skip_reasons["STATUS_NOT_SUCCESS"] += 1
                            logger.debug("skip %s: STATUS_NOT_SUCCESS %s", run_dir, status)
                            continue

                        trade_date = str(merged.get("trade_date") or date_dir.name)
                        if not (trade_date.isdigit() and len(trade_date) == 8):
                            skip_reasons["TRADE_DATE_INVALID"] += 1
                            logger.debug("skip %s: TRADE_DATE_INVALID %s", run_dir, trade_date)
                            continue

                        # decision_artifact soft check: prefer true, but don't exclude
                        # legacy schema_version=1 that may omit it when run_mode=DECISION
                        if (
                            decision_only
                            and merged.get("decision_artifact") is False
                            and man_mode == "DECISION"
                        ):
                            skip_reasons["DECISION_ARTIFACT_FALSE"] += 1
                            logger.debug("skip %s: DECISION_ARTIFACT_FALSE", run_dir)
                            continue

                        key = (trade_date, sess_val)
                        recency = _run_recency_key(merged, run_dir)
                        candidates.append((key, recency, run_dir, merged))

    for key, recency, run_dir, merged in candidates:
        prev = best.get(key)
        if prev is None:
            best[key] = (recency, run_dir, merged)
            continue
        prev_recency, prev_dir, _prev_merged = prev
        if recency > prev_recency:
            skip_reasons["DUPLICATE_TRADE_DATE_OLDER_RUN"] += 1
            logger.debug(
                "skip older duplicate %s (kept %s)",
                prev_dir,
                run_dir,
            )
            best[key] = (recency, run_dir, merged)
        else:
            skip_reasons["DUPLICATE_TRADE_DATE_OLDER_RUN"] += 1
            logger.debug(
                "skip older duplicate %s (kept %s)",
                run_dir,
                prev_dir,
            )

    ordered_keys = sorted(best.keys(), key=lambda k: k[0])
    if days > 0:
        # Unique trade dates (session aware): keep last N trade_date groups
        # Group by trade_date preserving order
        by_date: Dict[str, List[Tuple[str, str]]] = {}
        for k in ordered_keys:
            by_date.setdefault(k[0], []).append(k)
        date_keys = sorted(by_date.keys())
        keep_dates = set(date_keys[-int(days) :])
        ordered_keys = [k for k in ordered_keys if k[0] in keep_dates]

    run_dirs: List[Path] = []
    merged_by_run: Dict[str, Dict[str, Any]] = {}
    for k in ordered_keys:
        _rec, run_dir, merged = best[k]
        run_dirs.append(run_dir)
        merged_by_run[str(run_dir)] = merged

    included = len(run_dirs)
    excluded = int(sum(skip_reasons.values()))
    disc = {
        "manifest_count": manifest_count,
        "included_run_count": included,
        "excluded_run_count": excluded,
        "skip_reasons": dict(skip_reasons),
        "runs_root": str(runs_root),
        "decision_only": decision_only,
        "market": market_u,
        "session": session_l,
    }
    if manifest_count > 0 and included == 0:
        disc["warning"] = "MANIFESTS_FOUND_BUT_NONE_INCLUDED"
        logger.warning(
            "quality discovery: found %d manifest(s) but included_run_count=0 skip_reasons=%s",
            manifest_count,
            dict(skip_reasons),
        )
    elif included == 0:
        disc["warning"] = "NO_DECISION_RUNS_FOUND"
        logger.warning("quality discovery: no decision runs under %s", runs_root)

    logger.info(
        "quality discovery: manifests=%d included=%d excluded=%d skip=%s",
        manifest_count,
        included,
        excluded,
        dict(skip_reasons),
    )
    return DiscoveryResult(run_dirs=run_dirs, merged_by_run=merged_by_run, discovery=disc)


# Back-compat: older tests may expect a list of Paths
def discover_decision_run_paths(
    output_dir: Path,
    *,
    market: str,
    session: Optional[str] = None,
    days: int = 20,
    decision_only: bool = True,
) -> List[Path]:
    return discover_decision_runs(
        output_dir,
        market=market,
        session=session,
        days=days,
        decision_only=decision_only,
    ).run_dirs


NOT_AVAILABLE = "NOT_AVAILABLE"
TRUSTED = "TRUSTED"
UNTRUSTED = "UNTRUSTED"
FAILED_STATUS = "FAILED"


class RunArtifactCache:
    """Load DECISION / diagnostics artifacts once per run for quality + ledger."""

    def __init__(self) -> None:
        self._json: Dict[str, Any] = {}
        self._optional: Dict[str, Any] = {}

    def load_json(self, path: Path) -> Optional[Any]:
        key = str(path)
        if key in self._json:
            return self._json[key]
        data = _load_json(path)
        self._json[key] = data
        return data

    def load_optional_list(
        self,
        path: Path,
        *,
        schema_version: Any = None,
        manifest_listed: bool = False,
        label: str = "",
    ) -> Tuple[Any, Optional[str]]:
        """Return (rows_or_sentinel, trust_note).

        Missing optional file on legacy schema → NOT_AVAILABLE (DEBUG/INFO, not WARNING).
        """
        key = f"opt:{path}:{schema_version}:{manifest_listed}"
        if key in self._optional:
            return self._optional[key]
        if not path.exists():
            # schema v1 / feature unsupported: missing is normal
            try:
                sv = int(schema_version) if schema_version is not None else 1
            except Exception:
                sv = 1
            if not manifest_listed and sv <= 1:
                logger.debug("optional artifact missing (NOT_AVAILABLE): %s", path)
                result = (NOT_AVAILABLE, None)
            elif manifest_listed:
                logger.warning("manifest lists artifact but file missing: %s", path)
                result = ([], "MANIFEST_LISTED_BUT_MISSING")
            else:
                logger.info("optional artifact missing: %s", path)
                result = (NOT_AVAILABLE, None)
            self._optional[key] = result
            return result
        data = self.load_json(path)
        if data is None:
            logger.warning("JSON parse failed for optional artifact: %s", path)
            result = ([], "JSON_PARSE_FAILED")
            self._optional[key] = result
            return result
        if isinstance(data, list):
            rows = [r for r in data if isinstance(r, dict)]
        else:
            rows = []
        result = (rows, None)
        self._optional[key] = result
        return result


def discover_post_run_diagnostics(
    output_dir: Path,
    *,
    source_run_id: str,
    market: str,
    trade_date: str,
    session: str,
) -> Optional[Path]:
    """Locate post_run_diagnostics/{MARKET}/{date}/{session}/{source_run_id}."""
    root = (
        Path(output_dir)
        / "post_run_diagnostics"
        / str(market).upper()
        / str(trade_date)
        / str(session).lower()
        / str(source_run_id)
    )
    if root.is_dir() and (root / "diagnostics_manifest.json").exists():
        return root
    return None


def evaluate_liquidity_shadow_trust(
    run_dir: Path,
    *,
    output_dir: Optional[Path] = None,
    cache: Optional[RunArtifactCache] = None,
) -> Dict[str, Any]:
    """Assess whether Liquidity Shadow results are trusted for quality aggregates."""
    from screener_artifacts import (
        LEGACY_LIQUIDITY_SHADOW_ARTIFACTS,
        sha256_file,
        verify_manifest_integrity,
    )

    cache = cache or RunArtifactCache()
    run_dir = Path(run_dir)
    man = cache.load_json(run_dir / "manifest.json") or {}
    meta = cache.load_json(run_dir / "screener_run_meta.json") or {}
    merged = merge_manifest_and_meta(
        man if isinstance(man, dict) else {},
        meta if isinstance(meta, dict) else {},
    )
    market = str(merged.get("market") or "")
    trade_date = str(merged.get("trade_date") or "")
    session = str(merged.get("session") or "pm")
    run_id = str(merged.get("run_id") or run_dir.name)
    out_dir = Path(output_dir or run_dir.parents[4] if len(run_dir.parents) >= 5 else run_dir)

    # Prefer guessing output root: .../output/runs/decision/MKT/date/sess/run_id
    try:
        # run_dir = output/runs/decision/SP500/date/pm/run_id → parents[4]=output
        guessed = run_dir.parents[4]
        if (guessed / "runs").exists():
            out_dir = guessed
    except Exception:
        pass
    if output_dir is not None:
        out_dir = Path(output_dir)

    result: Dict[str, Any] = {
        "trust_status": NOT_AVAILABLE,
        "candidates": [],
        "meta": NOT_AVAILABLE,
        "diagnostics_dir": None,
        "reasons": [],
    }

    # Legacy mutation detection inside DECISION run
    ok, issues = verify_manifest_integrity(run_dir)
    legacy_mut = [i for i in issues if i.startswith("LEGACY_POST_FINALIZE_MUTATION")]
    sha_mismatches = [i for i in issues if i.startswith("SHA_MISMATCH")]
    if legacy_mut or any(
        (run_dir / n).exists()
        and ((man.get("artifacts") or {}).get(n) or {}).get("row_count") == 0
        for n in LEGACY_LIQUIDITY_SHADOW_ARTIFACTS
    ):
        # Extra check: empty digest in manifest but non-empty file
        for n in LEGACY_LIQUIDITY_SHADOW_ARTIFACTS:
            p = run_dir / n
            if not p.exists():
                continue
            art = (man.get("artifacts") or {}).get(n) or {}
            expected = art.get("sha256")
            try:
                actual = sha256_file(p)
            except Exception:
                actual = None
            if expected and actual and expected != actual:
                result["trust_status"] = "LIQUIDITY_SHADOW_UNTRUSTED"
                result["reasons"].append("LEGACY_POST_FINALIZE_MUTATION_DETECTED")
                result["reasons"].append("MANIFEST_SHA_MISMATCH")
                # Still try to read but mark untrusted — exclude from performance
                data = cache.load_json(p)
                if isinstance(data, list):
                    result["candidates"] = [r for r in data if isinstance(r, dict)]
                return result

    diag_dir = discover_post_run_diagnostics(
        out_dir,
        source_run_id=run_id,
        market=market,
        trade_date=trade_date,
        session=session,
    )
    if diag_dir is None:
        # No diagnostics — normal for legacy runs
        result["trust_status"] = NOT_AVAILABLE
        result["reasons"].append("POST_RUN_DIAGNOSTICS_MISSING")
        return result

    result["diagnostics_dir"] = str(diag_dir)
    dman = cache.load_json(diag_dir / "diagnostics_manifest.json")
    if not isinstance(dman, dict):
        result["trust_status"] = UNTRUSTED
        result["reasons"].append("DIAGNOSTICS_MANIFEST_INVALID")
        return result

    # Verify diagnostics artifact digests
    arts = dman.get("artifacts") or {}
    for name, info in arts.items():
        if not isinstance(info, dict):
            continue
        p = diag_dir / name
        if not p.exists():
            result["trust_status"] = UNTRUSTED
            result["reasons"].append(f"DIAGNOSTICS_MISSING:{name}")
            return result
        expected = info.get("sha256")
        if expected:
            actual = sha256_file(p)
            if actual != expected:
                result["trust_status"] = UNTRUSTED
                result["reasons"].append(f"DIAGNOSTICS_SHA_MISMATCH:{name}")
                return result

    status = str(dman.get("status") or "")
    liq_meta = cache.load_json(diag_dir / "liquidity_shadow_meta.json")
    if isinstance(liq_meta, dict):
        result["meta"] = liq_meta
        status = str(liq_meta.get("status") or status)

    cands_path = diag_dir / "screener_liquidity_shadow_candidates.json"
    cands_data = cache.load_json(cands_path)
    cands = [r for r in cands_data if isinstance(r, dict)] if isinstance(cands_data, list) else []

    if status == "FAILED" or status.endswith("FAILED"):
        result["trust_status"] = FAILED_STATUS
        result["candidates"] = cands
        result["reasons"].append("DIAGNOSTICS_FAILED")
        return result

    result["trust_status"] = TRUSTED
    result["candidates"] = cands
    result["status"] = status
    return result


def _meta_of(run_dir: Path) -> Dict[str, Any]:
    data = _load_json(run_dir / "screener_run_meta.json") or {}
    return data if isinstance(data, dict) else {}


def _manifest_of(run_dir: Path) -> Dict[str, Any]:
    data = _load_json(run_dir / "manifest.json") or {}
    return data if isinstance(data, dict) else {}


def _merged_of(run_dir: Path, cache: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    if cache and str(run_dir) in cache:
        return cache[str(run_dir)]
    return merge_manifest_and_meta(_manifest_of(run_dir), _meta_of(run_dir))


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


def _optional_section(meta: Dict[str, Any], key: str) -> Any:
    """Legacy schema may omit v3 fields — return NOT_AVAILABLE sentinel, never invent 0."""
    if key not in meta:
        return NOT_AVAILABLE
    val = meta.get(key)
    if val is None:
        return None
    return val


def aggregate_quality_report(
    run_dirs: Sequence[Path],
    *,
    market: str,
    session: Optional[str] = None,
    min_sample_for_policy: int = MIN_SAMPLE_FOR_POLICY,
    discovery: Optional[Dict[str, Any]] = None,
    merged_by_run: Optional[Dict[str, Dict[str, Any]]] = None,
    decision_only: bool = True,
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
    artifact_cache = RunArtifactCache()

    for run_dir in run_dirs:
        meta = _merged_of(Path(run_dir), merged_by_run)
        run_mode = str(meta.get("run_mode") or "").upper()
        if decision_only and run_mode == "REPLAY":
            continue
        trade_date = str(meta.get("trade_date") or Path(run_dir).parent.parent.name)
        prod = _candidates_of(Path(run_dir), "screener_candidates.json")
        hc = _candidates_of(Path(run_dir), "screener_shadow_candidates.json")
        schema_v = meta.get("schema_version")
        man = _manifest_of(Path(run_dir))
        man_arts = man.get("artifacts") or {}
        elig, _elig_note = artifact_cache.load_optional_list(
            Path(run_dir) / "screener_eligible_shadow_candidates.json",
            schema_version=schema_v,
            manifest_listed="screener_eligible_shadow_candidates.json" in man_arts,
            label="eligible_shadow",
        )
        if elig is NOT_AVAILABLE:
            elig = []
        liq_trust = evaluate_liquidity_shadow_trust(
            Path(run_dir),
            cache=artifact_cache,
        )
        liq = liq_trust.get("candidates") or []
        if liq_trust.get("trust_status") in (
            UNTRUSTED,
            "LIQUIDITY_SHADOW_UNTRUSTED",
            NOT_AVAILABLE,
            FAILED_STATUS,
        ):
            # Exclude untrusted / missing / failed from frequency aggregates
            if liq_trust.get("trust_status") != TRUSTED:
                liq = []
        scores = _scores_of(Path(run_dir))

        prod_set = {_ticker(r) for r in prod if _ticker(r)}
        for t in prod_set:
            prod_freq[t] += 1
        for r in hc:
            t = _ticker(r)
            if t:
                hc_freq[t] += 1
        for r in elig if isinstance(elig, list) else []:
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
        prod_count = meta.get("production_candidate_count")
        if prod_count is None:
            prod_count = meta.get("candidate_count")
        if prod_count is None:
            prod_count = len(prod)

        score_series.append(
            {
                "trade_date": trade_date,
                "count": sd.get("count") if sd else None,
                "mean": sd.get("mean") if sd else None,
                "median": sd.get("median") if sd else None,
                "p90": sd.get("p90") if sd else None,
                "production_candidate_count": prod_count,
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

        avail = _optional_section(meta, "candidate_availability")
        shadow = _optional_section(meta, "shadow")
        elig_sh = _optional_section(meta, "eligible_shadow")
        # Prefer trusted diagnostics meta over DECISION PENDING stub
        if liq_trust.get("trust_status") == TRUSTED and isinstance(liq_trust.get("meta"), dict):
            liq_sh = liq_trust["meta"]
        elif liq_trust.get("trust_status") == NOT_AVAILABLE:
            liq_sh = NOT_AVAILABLE
        elif liq_trust.get("trust_status") == FAILED_STATUS:
            liq_sh = {"status": FAILED_STATUS, "trust_status": FAILED_STATUS}
        elif liq_trust.get("trust_status") in (UNTRUSTED, "LIQUIDITY_SHADOW_UNTRUSTED"):
            liq_sh = {
                "status": "LIQUIDITY_SHADOW_UNTRUSTED",
                "trust_status": "LIQUIDITY_SHADOW_UNTRUSTED",
                "reasons": liq_trust.get("reasons"),
            }
        else:
            liq_sh = _optional_section(meta, "liquidity_shadow")

        def _shadow_count(section: Any, fallback_len: int) -> Any:
            if section is NOT_AVAILABLE:
                return NOT_AVAILABLE if fallback_len == 0 else fallback_len
            if isinstance(section, dict):
                if "candidate_count" in section:
                    return section.get("candidate_count")
                return fallback_len
            return fallback_len

        days_meta.append(
            {
                "trade_date": trade_date,
                "run_id": meta.get("run_id"),
                "run_directory": str(run_dir),
                "schema_version": meta.get("schema_version"),
                "result_status": meta.get("result_status"),
                "empty_reason": er,
                "production_candidate_count": prod_count,
                "threshold_pass_count": (
                    avail.get("threshold_pass_count")
                    if isinstance(avail, dict)
                    else (None if avail is NOT_AVAILABLE else None)
                ),
                "eligible_new_buy_count": (
                    avail.get("eligible_new_buy_count") if isinstance(avail, dict) else None
                ),
                "high_conviction_shadow_count": _shadow_count(shadow, len(hc)),
                "eligible_shadow_count": _shadow_count(
                    elig_sh, len(elig) if isinstance(elig, list) else 0
                ),
                "liquidity_shadow_count": _shadow_count(liq_sh, len(liq)),
                "liquidity_shadow_trust": liq_trust.get("trust_status"),
                "liquidity_shadow_reasons": liq_trust.get("reasons") or [],
                "stage_drop_summary": _optional_section(meta, "stage_drop_summary"),
                "exclusion_summary": _optional_section(meta, "exclusion_summary"),
                "candidate_availability": avail,
                "eligible_shadow": elig_sh,
                "liquidity_shadow": liq_sh,
                "diagnostics": _optional_section(meta, "diagnostics"),
                "market_regime_shadow": _optional_section(meta, "market_regime_shadow"),
                "build_identity": _optional_section(meta, "build_identity"),
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
    if n_days == 0:
        sample_status = "NO_DATA"

    start = days_meta[0]["trade_date"] if days_meta else None
    end = days_meta[-1]["trade_date"] if days_meta else None

    report = {
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
        "decision_only": decision_only,
        "discovery": discovery
        or {
            "manifest_count": 0,
            "included_run_count": n_days,
            "excluded_run_count": 0,
            "skip_reasons": {},
        },
        "candidate_performance": {
            "note": "Returns are null when future price evidence is insufficient; never coerced to 0.",
            "horizons": ["next_decision", "1d", "3d", "5d"],
        },
        "legacy_field_policy": {
            "missing_v3_fields": NOT_AVAILABLE,
            "note": "Absent schema v3 fields are NOT_AVAILABLE/null; never coerced to 0.",
        },
    }
    return report


def render_quality_markdown(report: Dict[str, Any]) -> str:
    lines = [
        f"# Screener Quality Report — {report.get('market')} "
        f"{report.get('start_trade_date') or 'NO_DATA'}→{report.get('end_trade_date') or 'NO_DATA'}",
        "",
        f"- trading_days: {report.get('trading_days')}",
        f"- production_candidate_days: {report.get('production_candidate_days')}",
        f"- empty_valid_days: {report.get('empty_valid_days')}",
        f"- sample_status: `{report.get('sample_status')}`",
        f"- used_by_trader: {report.get('used_by_trader', False)}",
        "",
        "## Discovery",
    ]
    disc = report.get("discovery") or {}
    lines.append(f"- manifest_count: {disc.get('manifest_count')}")
    lines.append(f"- included_run_count: {disc.get('included_run_count')}")
    lines.append(f"- excluded_run_count: {disc.get('excluded_run_count')}")
    lines.append(f"- skip_reasons: `{disc.get('skip_reasons')}`")
    if disc.get("warning"):
        lines.append(f"- warning: `{disc.get('warning')}`")
    lines.append("")
    lines.append("## Empty Reason Distribution")
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
    if report.get("sample_status") == "NO_DATA":
        lines.append("**NO_DATA** — no qualifying Decision runs were included.")
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
        for field_name in (
            "return_1d_pct",
            "return_3d_pct",
            "return_5d_pct",
            "max_drawdown_5d_pct",
            "decision_price",
        ):
            if merged.get(field_name) is None and prev.get(field_name) is not None:
                merged[field_name] = prev.get(field_name)
        if merged.get("outcome_status") is None:
            merged["outcome_status"] = "PENDING"
        existing[key] = merged

    with open(ledger_path, "w", encoding="utf-8") as f:
        for rec in existing.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(existing)


def build_observation_rows_from_run(
    run_dir: Path,
    *,
    cache: Optional[RunArtifactCache] = None,
) -> List[Dict[str, Any]]:
    cache = cache or RunArtifactCache()
    meta = _merged_of(Path(run_dir))
    run_id = str(meta.get("run_id") or Path(run_dir).name)
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

    rd = Path(run_dir)
    _add(_candidates_of(rd, "screener_candidates.json"), "PRODUCTION")
    _add(_candidates_of(rd, "screener_shadow_candidates.json"), "HIGH_CONVICTION_SHADOW")
    elig, _ = cache.load_optional_list(
        rd / "screener_eligible_shadow_candidates.json",
        schema_version=meta.get("schema_version"),
        manifest_listed="screener_eligible_shadow_candidates.json"
        in ((_manifest_of(rd).get("artifacts") or {})),
    )
    if isinstance(elig, list):
        _add(elig, "ELIGIBLE_SHADOW")
    liq_trust = evaluate_liquidity_shadow_trust(rd, cache=cache)
    if liq_trust.get("trust_status") == TRUSTED:
        _add(list(liq_trust.get("candidates") or []), "LIQUIDITY_SHADOW")
    return rows


def quality_report_stem(report: Dict[str, Any], market: str) -> str:
    """Build filename stem — never UNKNOWN_UNKNOWN."""
    start = report.get("start_trade_date")
    end = report.get("end_trade_date")
    n = int(report.get("trading_days") or 0)
    if n > 0 and start and end:
        return f"screener_quality_{start}_{end}_{market}"
    return f"screener_quality_NO_DATA_{market}"


def write_quality_report(
    report: Dict[str, Any],
    output_dir: Path,
    *,
    market: str,
) -> Tuple[Path, Path]:
    out = Path(output_dir) / "quality"
    out.mkdir(parents=True, exist_ok=True)
    stem = quality_report_stem(report, market)
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

    _ = load_runtime_config(Path(args.config) if args.config else None)

    decision_only = not bool(args.include_replay)
    discovered = discover_decision_runs(
        Path(args.output_dir),
        market=args.market,
        session=args.session,
        days=args.days,
        decision_only=decision_only,
    )
    report = aggregate_quality_report(
        discovered.run_dirs,
        market=args.market,
        session=args.session,
        discovery=discovered.discovery,
        merged_by_run=discovered.merged_by_run,
        decision_only=decision_only,
    )
    json_path, md_path = write_quality_report(report, Path(args.output_dir), market=args.market)
    logger.info(
        "quality report: %s / %s (days=%s)",
        json_path,
        md_path,
        report.get("trading_days"),
    )

    if args.update_ledger and discovered.run_dirs:
        ledger = Path(args.output_dir) / "quality" / "screener_candidate_observations.jsonl"
        rows: List[Dict[str, Any]] = []
        for rd in discovered.run_dirs:
            rows.extend(build_observation_rows_from_run(rd))
        n = upsert_observation_ledger(ledger, rows)
        logger.info("observation ledger upserted entries=%d path=%s", n, ledger)

    payload = {
        "json": str(json_path),
        "md": str(md_path),
        "days": report.get("trading_days"),
        "start_trade_date": report.get("start_trade_date"),
        "end_trade_date": report.get("end_trade_date"),
        "discovery": report.get("discovery"),
        "sample_status": report.get("sample_status"),
    }
    print(json.dumps(payload, indent=2))

    # NO_DATA is a warning report, not a Production pipeline failure.
    if int(report.get("trading_days") or 0) == 0:
        return 2
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(main())
