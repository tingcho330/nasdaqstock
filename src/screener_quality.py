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
TRUSTED_WITH_WARNING = "TRUSTED_WITH_WARNING"
UNTRUSTED = "UNTRUSTED"
FAILED_STATUS = "FAILED"
LEGACY_UNTRUSTED = "LEGACY_UNTRUSTED"


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
    base = Path(output_dir) / "post_run_diagnostics"
    if not base.is_dir():
        return None
    for mdir in base.iterdir():
        if not mdir.is_dir():
            continue
        if mdir.name.upper() != str(market).upper():
            continue
        cand = mdir / str(trade_date) / str(session).lower() / str(source_run_id)
        if cand.is_dir() and (cand / "diagnostics_manifest.json").exists():
            return cand
    return None


def _guess_output_dir(run_dir: Path, output_dir: Optional[Path] = None) -> Path:
    if output_dir is not None:
        return Path(output_dir)
    run_dir = Path(run_dir)
    for parent in [run_dir, *run_dir.parents]:
        if (parent / "post_run_diagnostics").is_dir() or (parent / "runs").is_dir():
            return parent
    try:
        if len(run_dir.parents) >= 5:
            return run_dir.parents[4]
    except Exception:
        pass
    return run_dir


def meta_contains_self_hash(meta: Dict[str, Any]) -> bool:
    integrity = meta.get("artifact_integrity")
    if not isinstance(integrity, dict):
        return False
    return "screener_run_meta.json" in integrity


def detect_legacy_meta_self_hash_only(
    run_dir: Path,
    *,
    issues: Sequence[str],
    meta: Optional[Dict[str, Any]] = None,
) -> bool:
    sha_issues = [i for i in issues if i.startswith("SHA_MISMATCH:")]
    if not sha_issues:
        return False
    if any(not i.endswith("screener_run_meta.json") for i in sha_issues):
        return False
    other = [
        i
        for i in issues
        if not i.startswith("SHA_MISMATCH:")
        and not i.startswith("LEGACY_POST_FINALIZE_MUTATION")
    ]
    if other:
        return False
    meta = meta if isinstance(meta, dict) else (_load_json(Path(run_dir) / "screener_run_meta.json") or {})
    if not isinstance(meta, dict):
        return False
    return meta_contains_self_hash(meta)


def evaluate_liquidity_shadow_trust(
    run_dir: Path,
    *,
    output_dir: Optional[Path] = None,
    cache: Optional["RunArtifactCache"] = None,
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
    out_dir = _guess_output_dir(run_dir, output_dir)

    result: Dict[str, Any] = {
        "trust_status": NOT_AVAILABLE,
        "liquidity_shadow_trust": NOT_AVAILABLE,
        "liquidity_shadow_trust_reason": "INIT",
        "candidates": [],
        "scores": [],
        "meta": NOT_AVAILABLE,
        "diagnostics_dir": None,
        "reasons": [],
        "trade_date": trade_date,
        "source_decision_run_id": run_id,
    }

    _ok, issues = verify_manifest_integrity(run_dir)
    legacy_meta_self_hash = detect_legacy_meta_self_hash_only(
        run_dir, issues=issues, meta=meta if isinstance(meta, dict) else {}
    )
    legacy_mut = [i for i in issues if i.startswith("LEGACY_POST_FINALIZE_MUTATION")]
    if legacy_mut or any(
        (run_dir / n).exists()
        and ((man.get("artifacts") or {}).get(n) or {}).get("row_count") == 0
        for n in LEGACY_LIQUIDITY_SHADOW_ARTIFACTS
    ):
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
                result["liquidity_shadow_trust"] = LEGACY_UNTRUSTED
                result["liquidity_shadow_trust_reason"] = "LEGACY_POST_FINALIZE_MUTATION"
                result["reasons"].append("LEGACY_POST_FINALIZE_MUTATION_DETECTED")
                result["reasons"].append("MANIFEST_SHA_MISMATCH")
                data = cache.load_json(p)
                if isinstance(data, list):
                    result["candidates"] = [r for r in data if isinstance(r, dict)]
                return result

    sha_mismatches = [i for i in issues if i.startswith("SHA_MISMATCH:")]
    if sha_mismatches and not legacy_meta_self_hash:
        result["reasons"].append("DECISION_SHA_MISMATCH")
        result["reasons"].extend(sha_mismatches)

    diag_dir = discover_post_run_diagnostics(
        out_dir,
        source_run_id=run_id,
        market=market,
        trade_date=trade_date,
        session=session,
    )
    if diag_dir is None:
        if legacy_mut or any((run_dir / n).exists() for n in LEGACY_LIQUIDITY_SHADOW_ARTIFACTS):
            result["trust_status"] = LEGACY_UNTRUSTED
            result["liquidity_shadow_trust"] = LEGACY_UNTRUSTED
            result["liquidity_shadow_trust_reason"] = "LEGACY_LIQUIDITY_ARTIFACT_UNTRUSTED"
            result["reasons"].append("LEGACY_LIQUIDITY_ARTIFACT_UNTRUSTED")
            return result
        result["trust_status"] = NOT_AVAILABLE
        result["liquidity_shadow_trust"] = NOT_AVAILABLE
        result["liquidity_shadow_trust_reason"] = "POST_RUN_DIAGNOSTICS_MISSING"
        result["reasons"].append("POST_RUN_DIAGNOSTICS_MISSING")
        return result

    result["diagnostics_dir"] = str(diag_dir)
    dman = cache.load_json(diag_dir / "diagnostics_manifest.json")
    if not isinstance(dman, dict):
        result["trust_status"] = UNTRUSTED
        result["liquidity_shadow_trust"] = UNTRUSTED
        result["liquidity_shadow_trust_reason"] = "DIAGNOSTICS_MANIFEST_INVALID"
        result["reasons"].append("DIAGNOSTICS_MANIFEST_INVALID")
        return result

    expected_src = dman.get("source_decision_manifest_sha256")
    man_path = run_dir / "manifest.json"
    if expected_src and man_path.exists():
        actual_src = sha256_file(man_path)
        if actual_src != expected_src:
            result["trust_status"] = UNTRUSTED
            result["liquidity_shadow_trust"] = UNTRUSTED
            result["liquidity_shadow_trust_reason"] = "SOURCE_DECISION_MANIFEST_SHA_MISMATCH"
            result["reasons"].append("SOURCE_DECISION_MANIFEST_SHA_MISMATCH")
            return result

    arts = dman.get("artifacts") or {}
    for name, info in arts.items():
        if not isinstance(info, dict):
            continue
        p = diag_dir / name
        if not p.exists():
            result["trust_status"] = UNTRUSTED
            result["liquidity_shadow_trust"] = UNTRUSTED
            result["liquidity_shadow_trust_reason"] = f"DIAGNOSTICS_MISSING:{name}"
            result["reasons"].append(f"DIAGNOSTICS_MISSING:{name}")
            return result
        expected = info.get("sha256")
        if expected:
            actual = sha256_file(p)
            if actual != expected:
                result["trust_status"] = UNTRUSTED
                result["liquidity_shadow_trust"] = UNTRUSTED
                result["liquidity_shadow_trust_reason"] = f"DIAGNOSTICS_SHA_MISMATCH:{name}"
                result["reasons"].append(f"DIAGNOSTICS_SHA_MISMATCH:{name}")
                return result

    status = str(dman.get("status") or "")
    liq_meta = cache.load_json(diag_dir / "liquidity_shadow_meta.json")
    if isinstance(liq_meta, dict):
        result["meta"] = liq_meta
        status = str(liq_meta.get("status") or status)

    cands_data = cache.load_json(diag_dir / "screener_liquidity_shadow_candidates.json")
    cands = [r for r in cands_data if isinstance(r, dict)] if isinstance(cands_data, list) else []
    scores_data = cache.load_json(diag_dir / "screener_liquidity_shadow_scores.json")
    scores = [r for r in scores_data if isinstance(r, dict)] if isinstance(scores_data, list) else []
    result["scores"] = scores

    if any(c.get("used_by_trader") for c in cands):
        result["trust_status"] = UNTRUSTED
        result["liquidity_shadow_trust"] = UNTRUSTED
        result["liquidity_shadow_trust_reason"] = "USED_BY_TRADER_TRUE"
        result["reasons"].append("USED_BY_TRADER_TRUE")
        result["candidates"] = cands
        return result

    if status == "FAILED" or status.endswith("FAILED"):
        result["trust_status"] = FAILED_STATUS
        result["liquidity_shadow_trust"] = FAILED_STATUS
        result["liquidity_shadow_trust_reason"] = "DIAGNOSTICS_FAILED"
        result["candidates"] = cands
        result["reasons"].append("DIAGNOSTICS_FAILED")
        return result

    if legacy_meta_self_hash:
        result["trust_status"] = TRUSTED_WITH_WARNING
        result["liquidity_shadow_trust"] = TRUSTED_WITH_WARNING
        result["liquidity_shadow_trust_reason"] = "LEGACY_META_SELF_HASH_MISMATCH"
        result["reasons"].append("LEGACY_META_SELF_HASH_MISMATCH")
        result["candidates"] = cands
        result["status"] = status
        return result

    if sha_mismatches:
        result["trust_status"] = UNTRUSTED
        result["liquidity_shadow_trust"] = UNTRUSTED
        result["liquidity_shadow_trust_reason"] = "DECISION_SHA_MISMATCH"
        result["candidates"] = cands
        return result

    result["trust_status"] = TRUSTED
    result["liquidity_shadow_trust"] = TRUSTED
    result["liquidity_shadow_trust_reason"] = "ALL_INTEGRITY_CHECKS_PASSED"
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
        _trusted_liq = liq_trust.get("trust_status") in (TRUSTED, TRUSTED_WITH_WARNING)
        if not _trusted_liq:
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
        if liq_trust.get("trust_status") in (TRUSTED, TRUSTED_WITH_WARNING) and isinstance(
            liq_trust.get("meta"), dict
        ):
            liq_sh = dict(liq_trust["meta"])
            liq_sh["trust_status"] = liq_trust.get("trust_status")
            liq_sh["trust_reason"] = liq_trust.get("liquidity_shadow_trust_reason")
        elif liq_trust.get("trust_status") == NOT_AVAILABLE:
            liq_sh = NOT_AVAILABLE
        elif liq_trust.get("trust_status") == FAILED_STATUS:
            liq_sh = {"status": FAILED_STATUS, "trust_status": FAILED_STATUS}
        elif liq_trust.get("trust_status") in (
            UNTRUSTED,
            "LIQUIDITY_SHADOW_UNTRUSTED",
            LEGACY_UNTRUSTED,
        ):
            liq_sh = {
                "status": liq_trust.get("trust_status"),
                "trust_status": liq_trust.get("trust_status"),
                "trust_reason": liq_trust.get("liquidity_shadow_trust_reason"),
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
                "liquidity_shadow_trust": liq_trust.get("liquidity_shadow_trust")
                or liq_trust.get("trust_status"),
                "liquidity_shadow_trust_reason": liq_trust.get(
                    "liquidity_shadow_trust_reason"
                ),
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

    from screener_outcomes import (
        DEFAULT_QUALITY_POLICY,
        classify_sample_statuses,
    )

    # Structural vs outcome vs policy — never treat ADEQUATE_SAMPLE as policy green light
    sample_bits = classify_sample_statuses(
        trading_days=n_days,
        matured_1d=0,
        matured_5d=0,
        matured_10d=0,
        policy={"structural_min_days": int(min_sample_for_policy)},
    )
    # Prefer structural from days count; outcome filled after ledger settle in CLI
    sample_bits["structural_sample_status"] = (
        "NO_DATA"
        if n_days == 0
        else (
            "INSUFFICIENT_SAMPLE"
            if n_days < int(min_sample_for_policy)
            else "ADEQUATE_SAMPLE"
        )
    )
    sample_bits["policy_change_status"] = "DO_NOT_CHANGE"
    if sample_bits["structural_sample_status"] == "ADEQUATE_SAMPLE":
        sample_bits["policy_change_status"] = "CONTINUE_OBSERVATION"

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
        "sample_status": sample_status,  # legacy alias = structural only
        "structural_sample_status": sample_bits["structural_sample_status"],
        "outcome_sample_status": sample_bits["outcome_sample_status"],
        "policy_change_status": sample_bits["policy_change_status"],
        "min_sample_for_policy_change": int(min_sample_for_policy),
        "policy_change_recommendation": sample_bits["policy_change_status"],
        "quality_policy": dict(DEFAULT_QUALITY_POLICY),
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


def observation_in_report_scope(
    row: Dict[str, Any],
    *,
    start_trade_date: Optional[str],
    end_trade_date: Optional[str],
    market: Optional[str],
    session: Optional[str],
) -> bool:
    """True when observation belongs to the current Quality Report window."""
    td = str(row.get("trade_date") or "").strip()
    if not td:
        return False
    if start_trade_date and td < str(start_trade_date):
        return False
    if end_trade_date and td > str(end_trade_date):
        return False
    if market:
        if str(row.get("market") or "").upper() != str(market).upper():
            return False
    if session:
        if str(row.get("session") or "").lower() != str(session).lower():
            return False
    return True


def filter_report_scoped_observations(
    rows: Sequence[Dict[str, Any]],
    *,
    start_trade_date: Optional[str],
    end_trade_date: Optional[str],
    market: Optional[str],
    session: Optional[str],
) -> List[Dict[str, Any]]:
    return [
        r
        for r in rows
        if observation_in_report_scope(
            r,
            start_trade_date=start_trade_date,
            end_trade_date=end_trade_date,
            market=market,
            session=session,
        )
    ]


def is_trusted_for_analysis(row: Dict[str, Any]) -> bool:
    return row.get("trusted_for_analysis") is not False


def derive_fundamental_parity_status(
    liq_trust_or_meta: Optional[Dict[str, Any]],
) -> str:
    """Map liquidity-shadow meta → fundamental_parity_status for observations."""
    if not isinstance(liq_trust_or_meta, dict):
        return "CHECK_REQUIRED"
    meta = liq_trust_or_meta
    if "fundamental_parity" not in meta and isinstance(meta.get("meta"), dict):
        meta = meta["meta"]
    explicit = meta.get("fundamental_parity_status")
    if explicit:
        return str(explicit)
    fp = meta.get("fundamental_parity")
    if not isinstance(fp, dict):
        return "CHECK_REQUIRED"
    if fp.get("status") == "VERIFIED" or fp.get("verified") is True:
        return "VERIFIED"
    if fp.get("suspicious_constant_feature_detected"):
        return "LEGACY_UNCORRECTED"
    return "CHECK_REQUIRED"


def is_eligible_for_score_calibration(row: Dict[str, Any]) -> bool:
    """LIQUIDITY_SHADOW requires VERIFIED fundamental parity for calibration/Spearman."""
    if str(row.get("candidate_type") or "") == "LIQUIDITY_SHADOW":
        return str(row.get("fundamental_parity_status") or "") == "VERIFIED"
    return True


def pick_debug_settle_observation(
    rows: Sequence[Dict[str, Any]],
    *,
    start_trade_date: Optional[str],
    end_trade_date: Optional[str],
    market: Optional[str],
    session: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Prefer scoped → trusted → PRODUCTION → oldest trade_date."""
    scoped = filter_report_scoped_observations(
        rows,
        start_trade_date=start_trade_date,
        end_trade_date=end_trade_date,
        market=market,
        session=session,
    )
    trusted = [r for r in scoped if is_trusted_for_analysis(r)]
    production = [
        r
        for r in trusted
        if str(r.get("candidate_type") or "") == "PRODUCTION"
        and r.get("trade_date")
        and r.get("ticker")
    ]
    pool = production or [
        r for r in trusted if r.get("trade_date") and r.get("ticker")
    ] or [r for r in scoped if r.get("trade_date") and r.get("ticker")]
    if not pool:
        return None
    pool = sorted(pool, key=lambda r: str(r.get("trade_date")))
    return pool[0]


def _spearman_score_vs_return(
    rows: Sequence[Dict[str, Any]],
    *,
    horizon: int = 5,
) -> Optional[float]:
    from screener_outcomes import spearman_corr

    xs: List[float] = []
    ys: List[float] = []
    key = f"return_{horizon}d_pct"
    for r in rows:
        s = _safe_float(r.get("decision_score") or r.get("score"))
        y = _safe_float(r.get(key))
        if s is not None and y is not None:
            xs.append(s)
            ys.append(y)
    return spearman_corr(xs, ys)


def build_score_calibration_by_candidate_type(
    trusted_rows: Sequence[Dict[str, Any]],
    *,
    horizon: int = 5,
) -> Dict[str, Any]:
    """Per-candidate-type score calibration; never mixes types into one Spearman."""
    from screener_outcomes import score_calibration_buckets

    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for r in trusted_rows:
        by_type.setdefault(str(r.get("candidate_type") or "UNKNOWN"), []).append(r)

    out: Dict[str, Any] = {}
    for ctype in (
        "PRODUCTION",
        "ELIGIBLE_SHADOW",
        "HIGH_CONVICTION_SHADOW",
        "LIQUIDITY_SHADOW",
    ):
        group = by_type.get(ctype) or []
        if not group:
            continue
        if ctype == "LIQUIDITY_SHADOW":
            eligible = [r for r in group if is_eligible_for_score_calibration(r)]
            excluded_n = len(group) - len(eligible)
            if not eligible:
                out[ctype] = {
                    "status": "EXCLUDED_LEGACY_FUNDAMENTAL_PARITY",
                    "observations": len(group),
                    "excluded_from_calibration": excluded_n or len(group),
                    f"spearman_{horizon}d": None,
                    "buckets": [],
                }
                continue
            out[ctype] = {
                "status": "OK" if excluded_n == 0 else "PARTIAL_LEGACY_EXCLUDED",
                "observations": len(group),
                "calibration_rows": len(eligible),
                "excluded_from_calibration": excluded_n,
                f"spearman_{horizon}d": _spearman_score_vs_return(
                    eligible, horizon=horizon
                ),
                "buckets": score_calibration_buckets(eligible, horizon=horizon),
            }
            continue

        out[ctype] = {
            "status": "OK",
            "observations": len(group),
            "calibration_rows": len(group),
            "excluded_from_calibration": 0,
            f"spearman_{horizon}d": _spearman_score_vs_return(group, horizon=horizon),
            "buckets": score_calibration_buckets(group, horizon=horizon),
        }

    # Preserve any unexpected types (scoped + trusted) with same rules
    for ctype, group in by_type.items():
        if ctype in out:
            continue
        eligible = [r for r in group if is_eligible_for_score_calibration(r)]
        out[ctype] = {
            "status": "OK",
            "observations": len(group),
            "calibration_rows": len(eligible),
            "excluded_from_calibration": len(group) - len(eligible),
            f"spearman_{horizon}d": _spearman_score_vs_return(
                eligible, horizon=horizon
            ),
            "buckets": score_calibration_buckets(eligible, horizon=horizon),
        }
    return out


def apply_outcome_stats_to_report(
    report: Dict[str, Any],
    settled_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Attach outcome aggregates using report-scoped trusted rows only.

    Settlement may cover the full ledger; Quality statistics must not.
    """
    from screener_outcomes import (
        classify_sample_statuses,
        summarize_outcome_group,
    )

    start = report.get("start_trade_date")
    end = report.get("end_trade_date")
    market = report.get("market")
    session = report.get("session")

    ledger_total = len(settled_rows)
    scoped = filter_report_scoped_observations(
        settled_rows,
        start_trade_date=start,
        end_trade_date=end,
        market=market,
        session=session,
    )
    trusted = [r for r in scoped if is_trusted_for_analysis(r)]

    m1 = sum(1 for r in trusted if (r.get("maturity") or {}).get("1d"))
    m3 = sum(1 for r in trusted if (r.get("maturity") or {}).get("3d"))
    m5 = sum(1 for r in trusted if (r.get("maturity") or {}).get("5d"))
    m10 = sum(1 for r in trusted if (r.get("maturity") or {}).get("10d"))
    bits = classify_sample_statuses(
        trading_days=int(report.get("trading_days") or 0),
        matured_1d=m1,
        matured_5d=m5,
        matured_10d=m10,
    )
    report["structural_sample_status"] = bits["structural_sample_status"]
    report["outcome_sample_status"] = bits["outcome_sample_status"]
    report["policy_change_status"] = bits["policy_change_status"]
    report["policy_change_recommendation"] = bits["policy_change_status"]
    report["outcome_counts"] = {
        "ledger_total_rows": ledger_total,
        "report_scoped_rows": len(scoped),
        "report_trusted_rows": len(trusted),
        "trusted_rows": len(trusted),
        "matured_1d": m1,
        "matured_3d": m3,
        "matured_5d": m5,
        "matured_10d": m10,
        "pending": sum(1 for r in trusted if r.get("outcome_status") == "PENDING"),
        "partially_matured": sum(
            1 for r in trusted if r.get("outcome_status") == "PARTIALLY_MATURED"
        ),
        "fully_matured": sum(
            1 for r in trusted if r.get("outcome_status") == "FULLY_MATURED"
        ),
        "policy_review_sample_count": m5,
    }

    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for r in trusted:
        by_type.setdefault(str(r.get("candidate_type") or "UNKNOWN"), []).append(r)

    cand_perf: Dict[str, Any] = {
        "note": (
            "Returns use report-scoped trusted observations only; "
            "Spearman/calibration are per candidate_type and never mixed."
        ),
        "horizons": ["1d", "3d", "5d", "10d"],
    }
    for ctype, group in by_type.items():
        entry: Dict[str, Any] = {
            h: summarize_outcome_group(group, horizon=h) for h in (1, 3, 5, 10)
        }
        calib_rows = [r for r in group if is_eligible_for_score_calibration(r)]
        if ctype == "LIQUIDITY_SHADOW" and not calib_rows:
            entry["spearman_score_5d"] = None
            entry["spearman_calibration_status"] = "EXCLUDED_LEGACY_FUNDAMENTAL_PARITY"
        else:
            entry["spearman_score_5d"] = _spearman_score_vs_return(
                calib_rows if ctype == "LIQUIDITY_SHADOW" else group, horizon=5
            )
        cand_perf[ctype] = entry

    report["candidate_performance"] = cand_perf
    report["score_calibration"] = build_score_calibration_by_candidate_type(
        trusted, horizon=5
    )
    return report


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
            "return_10d_pct",
            "max_drawdown_5d_pct",
            "max_drawdown_10d_pct",
            "max_runup_5d_pct",
            "max_runup_10d_pct",
            "decision_price",
            "reference_price",
            "maturity",
        ):
            if merged.get(field_name) is None and prev.get(field_name) is not None:
                merged[field_name] = prev.get(field_name)
        # Do not regress settled outcomes when rebuilding observation stubs
        prev_status = prev.get("outcome_status")
        new_status = merged.get("outcome_status")
        if prev_status and prev_status not in (None, "PENDING") and new_status in (
            None,
            "PENDING",
        ):
            merged["outcome_status"] = prev_status
            if prev.get("maturity") is not None:
                merged["maturity"] = prev.get("maturity")
        if merged.get("trusted_for_analysis") is None and prev.get("trusted_for_analysis") is not None:
            merged["trusted_for_analysis"] = prev.get("trusted_for_analysis")
        if merged.get("exclusion_reason") is None and prev.get("exclusion_reason") is not None:
            merged["exclusion_reason"] = prev.get("exclusion_reason")
        if (
            merged.get("source_integrity_status") is None
            and prev.get("source_integrity_status") is not None
        ):
            merged["source_integrity_status"] = prev.get("source_integrity_status")
        if (
            merged.get("fundamental_parity_status") is None
            and prev.get("fundamental_parity_status") is not None
        ):
            merged["fundamental_parity_status"] = prev.get("fundamental_parity_status")
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
    output_dir: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    cache = cache or RunArtifactCache()
    meta = _merged_of(Path(run_dir))
    run_id = str(meta.get("run_id") or Path(run_dir).name)
    trade_date = str(meta.get("trade_date") or "")
    session = str(meta.get("session") or "pm")
    market = str(meta.get("market") or "")
    as_of = meta.get("as_of_kst")
    rows: List[Dict[str, Any]] = []

    def _add(
        items: List[Dict[str, Any]],
        ctype: str,
        *,
        source_type: str,
        source_integrity_status: str,
        trusted_for_analysis: bool,
        exclusion_reason: Optional[str] = None,
        source_diagnostics_run_id: Optional[str] = None,
        fundamental_parity_status: Optional[str] = None,
    ) -> None:
        for r in items:
            t = _ticker(r)
            if not t:
                continue
            score = _safe_float(r.get("Score") if "Score" in r else r.get("score"))
            price = _safe_float(r.get("Price") if "Price" in r else r.get("price"))
            rec: Dict[str, Any] = {
                "decision_run_id": run_id,
                "source_run_id": run_id,
                "source_diagnostics_run_id": source_diagnostics_run_id,
                "trade_date": trade_date,
                "session": session,
                "market": market,
                "ticker": t,
                "candidate_type": ctype,
                "source_type": source_type,
                "source_integrity_status": source_integrity_status,
                "trusted_for_analysis": bool(trusted_for_analysis),
                "exclusion_reason": exclusion_reason,
                "decision_score": score,
                "decision_price": price,
                "reference_price": price,
                "reference_price_date": trade_date,
                "decision_price_source": "screener_artifact",
                "outcome_price_source": "close_to_close",
                "decision_as_of_kst": as_of,
                "return_1d_pct": None,
                "return_3d_pct": None,
                "return_5d_pct": None,
                "return_10d_pct": None,
                "max_drawdown_5d_pct": None,
                "max_drawdown_10d_pct": None,
                "outcome_status": "PENDING",
                "maturity": {"1d": False, "3d": False, "5d": False, "10d": False},
                "used_by_trader": False,
            }
            if fundamental_parity_status is not None:
                rec["fundamental_parity_status"] = fundamental_parity_status
            rows.append(rec)

    rd = Path(run_dir)
    _add(
        _candidates_of(rd, "screener_candidates.json"),
        "PRODUCTION",
        source_type="DECISION_CANDIDATES",
        source_integrity_status="TRUSTED",
        trusted_for_analysis=True,
    )
    _add(
        _candidates_of(rd, "screener_shadow_candidates.json"),
        "HIGH_CONVICTION_SHADOW",
        source_type="DECISION_SHADOW",
        source_integrity_status="TRUSTED",
        trusted_for_analysis=True,
    )
    elig, _ = cache.load_optional_list(
        rd / "screener_eligible_shadow_candidates.json",
        schema_version=meta.get("schema_version"),
        manifest_listed="screener_eligible_shadow_candidates.json"
        in ((_manifest_of(rd).get("artifacts") or {})),
    )
    if isinstance(elig, list):
        _add(
            elig,
            "ELIGIBLE_SHADOW",
            source_type="DECISION_ELIGIBLE_SHADOW",
            source_integrity_status="TRUSTED",
            trusted_for_analysis=True,
        )
    liq_trust = evaluate_liquidity_shadow_trust(rd, cache=cache, output_dir=output_dir)
    trust_st = liq_trust.get("trust_status")
    diag_id = None
    if liq_trust.get("diagnostics_dir"):
        diag_id = Path(str(liq_trust["diagnostics_dir"])).name
    fp_status = derive_fundamental_parity_status(liq_trust)
    if trust_st in (TRUSTED, TRUSTED_WITH_WARNING):
        _add(
            list(liq_trust.get("candidates") or []),
            "LIQUIDITY_SHADOW",
            source_type="POST_RUN_DIAGNOSTICS",
            source_integrity_status=str(trust_st),
            trusted_for_analysis=True,
            exclusion_reason=(
                None
                if trust_st == TRUSTED
                else liq_trust.get("liquidity_shadow_trust_reason")
            ),
            source_diagnostics_run_id=diag_id,
            fundamental_parity_status=fp_status,
        )
    elif trust_st in (LEGACY_UNTRUSTED, "LIQUIDITY_SHADOW_UNTRUSTED", UNTRUSTED):
        # Preserve rows but exclude from analysis aggregates
        _add(
            list(liq_trust.get("candidates") or []),
            "LIQUIDITY_SHADOW",
            source_type="LEGACY_OR_UNTRUSTED",
            source_integrity_status=str(trust_st),
            trusted_for_analysis=False,
            exclusion_reason=str(
                liq_trust.get("liquidity_shadow_trust_reason")
                or "LEGACY_LIQUIDITY_ARTIFACT_UNTRUSTED"
            ),
            source_diagnostics_run_id=diag_id,
            fundamental_parity_status=fp_status
            if fp_status != "CHECK_REQUIRED"
            else "LEGACY_UNCORRECTED",
        )
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
    parser.add_argument(
        "--skip-ledger",
        action="store_true",
        default=False,
        help="Skip observation ledger upsert / outcome settlement",
    )
    parser.add_argument(
        "--debug-settle",
        action="store_true",
        default=False,
        help="Pick oldest trusted PRODUCTION observation inside report scope and print settlement debug",
    )
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

    for day in report.get("days") or []:
        liq = day.get("liquidity_shadow")
        if isinstance(liq, dict) and day.get("liquidity_shadow_trust") in (
            TRUSTED,
            TRUSTED_WITH_WARNING,
        ):
            fp = liq.get("fundamental_parity")
            if not isinstance(fp, dict) or fp.get("suspicious_constant_feature_detected"):
                day["fundamental_parity_status"] = "LEGACY_UNCORRECTED"
            else:
                day["fundamental_parity_status"] = day.get(
                    "fundamental_parity_status", "CHECK_REQUIRED"
                )

    update_ledger = bool(args.update_ledger) and not bool(args.skip_ledger)
    ledger = Path(args.output_dir) / "quality" / "screener_candidate_observations.jsonl"

    if update_ledger and discovered.run_dirs:
        rows: List[Dict[str, Any]] = []
        for rd in discovered.run_dirs:
            rows.extend(
                build_observation_rows_from_run(rd, output_dir=Path(args.output_dir))
            )
        n = upsert_observation_ledger(ledger, rows)
        logger.info("observation ledger upserted stubs entries=%d path=%s", n, ledger)
        try:
            from screener_outcomes import (
                backfill_candidate_outcomes,
                clear_ohlcv_cache,
                debug_settle_one,
            )

            clear_ohlcv_cache()
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
            as_of = report.get("end_trade_date")
            logger.info(
                "outcome settlement start rows=%d as_of=%s",
                len(existing),
                as_of,
            )
            # Settle full ledger (including out-of-report-scope backfill).
            settled = backfill_candidate_outcomes(
                existing, as_of_trade_date=as_of, only_trusted=False
            )
            n2 = upsert_observation_ledger(ledger, settled)
            logger.info(
                "outcome settlement done persisted=%d matured_1d=%d matured_5d=%d",
                n2,
                sum(1 for r in settled if (r.get("maturity") or {}).get("1d")),
                sum(1 for r in settled if (r.get("maturity") or {}).get("5d")),
            )

            if args.debug_settle:
                pick = pick_debug_settle_observation(
                    settled,
                    start_trade_date=report.get("start_trade_date"),
                    end_trade_date=report.get("end_trade_date"),
                    market=report.get("market") or args.market,
                    session=report.get("session") or args.session,
                )
                if pick:
                    dbg = debug_settle_one(pick, as_of_trade_date=as_of)
                    print(
                        json.dumps(
                            {
                                "debug_settle": dbg.get("_settlement_debug")
                                or {
                                    "ticker": dbg.get("ticker"),
                                    "trade_date": dbg.get("trade_date"),
                                    "reference_price": dbg.get("reference_price"),
                                    "return_1d_pct": dbg.get("return_1d_pct"),
                                    "return_3d_pct": dbg.get("return_3d_pct"),
                                    "return_5d_pct": dbg.get("return_5d_pct"),
                                    "maturity": dbg.get("maturity"),
                                    "outcome_status": dbg.get("outcome_status"),
                                }
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                    )

            # Quality aggregates: report scope only (never full ledger).
            apply_outcome_stats_to_report(report, settled)
        except Exception as e:
            logger.exception("outcome backfill failed: %s", e)

    json_path, md_path = write_quality_report(report, Path(args.output_dir), market=args.market)
    logger.info(
        "quality report: %s / %s (days=%s outcome=%s)",
        json_path,
        md_path,
        report.get("trading_days"),
        report.get("outcome_sample_status"),
    )

    payload = {
        "json": str(json_path),
        "md": str(md_path),
        "days": report.get("trading_days"),
        "start_trade_date": report.get("start_trade_date"),
        "end_trade_date": report.get("end_trade_date"),
        "discovery": report.get("discovery"),
        "sample_status": report.get("sample_status"),
        "structural_sample_status": report.get("structural_sample_status"),
        "outcome_sample_status": report.get("outcome_sample_status"),
        "policy_change_status": report.get("policy_change_status"),
        "outcome_counts": report.get("outcome_counts"),
        "score_calibration": {
            k: {
                "status": (v or {}).get("status") if isinstance(v, dict) else None,
                "observations": (v or {}).get("observations") if isinstance(v, dict) else None,
                "spearman_5d": (v or {}).get("spearman_5d") if isinstance(v, dict) else None,
            }
            for k, v in (report.get("score_calibration") or {}).items()
            if k
            in (
                "PRODUCTION",
                "ELIGIBLE_SHADOW",
                "HIGH_CONVICTION_SHADOW",
                "LIQUIDITY_SHADOW",
            )
        }
        if isinstance(report.get("score_calibration"), dict)
        else report.get("score_calibration"),
    }
    print(json.dumps(payload, indent=2))

    if int(report.get("trading_days") or 0) == 0:
        return 2
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(main())
