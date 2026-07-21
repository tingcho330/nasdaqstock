"""Immutable screener run artifacts, DECISION/REPLAY isolation, and trader guards.

Does not change scoring formulas or production thresholds — only how results are
stored, labeled, and selected for downstream trader/GPT steps.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from utils import (
    KST,
    OUTPUT_DIR,
    _ET,
    format_pipeline_artifact,
    is_market_open_day,
    is_us_market,
    resolve_market_trade_date,
)

logger = logging.getLogger("screener_artifacts")

MANIFEST_SCHEMA_VERSION = 1
RUN_MODE_DECISION = "DECISION"
RUN_MODE_REPLAY = "REPLAY"

REPLAY_TYPE_CURRENT = "CURRENT_DATA_RECALCULATION"
REPLAY_TYPE_FROZEN = "FROZEN_INPUT_REPLAY"

ARTIFACT_NAMES = (
    "manifest.json",
    "screener_run_meta.json",
    "screener_review.md",
    "screener_scores.json",
    "screener_candidates_full.json",
    "screener_candidates.json",
    "screener_shadow_candidates.json",
    "screener_holdings.json",
    "market_state.json",
    "screener.log",
)

FIXED_PREFIXES = (
    "screener_run_meta",
    "screener_scores",
    "screener_candidates",
    "screener_candidates_full",
    "screener_shadow_candidates",
    "screener_holdings",
    "market_state",
)


class RunMode(str, Enum):
    DECISION = RUN_MODE_DECISION
    REPLAY = RUN_MODE_REPLAY


class ReplayType(str, Enum):
    CURRENT_DATA_RECALCULATION = REPLAY_TYPE_CURRENT
    FROZEN_INPUT_REPLAY = REPLAY_TYPE_FROZEN


class MarketSessionState(str, Enum):
    PREOPEN = "PREOPEN"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    HOLIDAY = "HOLIDAY"
    UNKNOWN = "UNKNOWN"


class DailyBarStatus(str, Enum):
    INTRADAY_PARTIAL = "INTRADAY_PARTIAL"
    FINAL = "FINAL"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNKNOWN = "UNKNOWN"


class ArtifactError(Exception):
    """Raised when artifact policy forbids an action."""


class FrozenReplayNotImplemented(ArtifactError):
    """FROZEN_INPUT_REPLAY execution is not implemented yet."""


def normalize_run_mode(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip().upper()
    if not v:
        return None
    if v in ("DECISION", "DEC", "PROD", "PRODUCTION"):
        return RUN_MODE_DECISION
    if v in ("REPLAY", "REP", "RECALC", "RECALCULATION"):
        return RUN_MODE_REPLAY
    raise ArtifactError(f"Invalid run_mode: {value!r} (expected decision|replay)")


def normalize_replay_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip().upper()
    if not v or v in ("NULL", "NONE"):
        return None
    if v in ("CURRENT_DATA_RECALCULATION", "CURRENT", "RECALC"):
        return REPLAY_TYPE_CURRENT
    if v in ("FROZEN_INPUT_REPLAY", "FROZEN", "SNAPSHOT"):
        return REPLAY_TYPE_FROZEN
    raise ArtifactError(f"Invalid replay_type: {value!r}")


def generate_run_id(now_kst: Optional[datetime] = None) -> str:
    """YYYYMMDD-HHMMSS-<6 hex> in KST; path-safe."""
    now = now_kst or datetime.now(KST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    else:
        now = now.astimezone(KST)
    stamp = now.strftime("%Y%m%d-%H%M%S")
    suffix = secrets.token_hex(3)
    return f"{stamp}-{suffix}"


def ensure_unique_run_id(run_dir_parent: Path, preferred: Optional[str] = None) -> str:
    """Return a run_id whose destination directory does not yet exist."""
    candidate = preferred or generate_run_id()
    for _ in range(8):
        dest = run_dir_parent / candidate
        if not dest.exists():
            return candidate
        candidate = generate_run_id()
    raise ArtifactError(f"Unable to allocate unique run_id under {run_dir_parent}")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_json_payload(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    return sha256_bytes(raw.encode("utf-8"))


def file_row_count(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    if path.suffix.lower() != ".json":
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return len(data)
        if isinstance(data, dict) and isinstance(data.get("rows"), list):
            return len(data["rows"])
        return None
    except Exception:
        return None


def get_git_commit(repo_root: Optional[Path] = None) -> Tuple[Optional[str], Optional[str]]:
    """Return (commit_sha, error_reason). Never raises."""
    root = repo_root or Path(__file__).resolve().parents[1]
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if proc.returncode == 0:
            sha = (proc.stdout or "").strip()
            return (sha or None), None
        err = (proc.stderr or proc.stdout or f"exit={proc.returncode}").strip()
        return None, err[:200] or "git_rev_parse_failed"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def resolve_data_clock(
    *,
    market: str,
    trade_date: str,
    as_of_kst: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Record exchange session / daily-bar status using calendar + RTH, not wall-clock guesses."""
    now = as_of_kst or datetime.now(KST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    else:
        now = now.astimezone(KST)
    as_of_utc = now.astimezone(timezone.utc)

    session_state = MarketSessionState.UNKNOWN.value
    bar_status = DailyBarStatus.UNKNOWN.value
    reason = "ok"

    try:
        td = datetime.strptime(str(trade_date), "%Y%m%d").date()
    except ValueError:
        return {
            "as_of_kst": now.isoformat(),
            "as_of_utc": as_of_utc.isoformat(),
            "data_cutoff_at_kst": now.isoformat(),
            "market_session_state": MarketSessionState.UNKNOWN.value,
            "daily_bar_status": DailyBarStatus.UNKNOWN.value,
            "clock_reason": "invalid_trade_date",
        }

    try:
        open_day = is_market_open_day(td, market)
    except Exception as e:
        return {
            "as_of_kst": now.isoformat(),
            "as_of_utc": as_of_utc.isoformat(),
            "data_cutoff_at_kst": now.isoformat(),
            "market_session_state": MarketSessionState.UNKNOWN.value,
            "daily_bar_status": DailyBarStatus.UNKNOWN.value,
            "clock_reason": f"calendar_error:{type(e).__name__}",
        }

    if not open_day:
        session_state = MarketSessionState.HOLIDAY.value
        bar_status = DailyBarStatus.NOT_APPLICABLE.value
        reason = "holiday_or_weekend"
    elif is_us_market(market):
        et = now.astimezone(_ET)
        open_t, close_t = time(9, 30), time(16, 0)
        if et.date() > td:
            session_state = MarketSessionState.CLOSED.value
            bar_status = DailyBarStatus.FINAL.value
            reason = "after_trade_date_et"
        elif et.date() < td:
            session_state = MarketSessionState.PREOPEN.value
            bar_status = DailyBarStatus.NOT_APPLICABLE.value
            reason = "before_trade_date_et"
        elif et.time() < open_t:
            session_state = MarketSessionState.PREOPEN.value
            bar_status = DailyBarStatus.NOT_APPLICABLE.value
            reason = "before_rth_open"
        elif et.time() >= close_t:
            session_state = MarketSessionState.CLOSED.value
            bar_status = DailyBarStatus.FINAL.value
            reason = "after_rth_close"
        else:
            session_state = MarketSessionState.OPEN.value
            bar_status = DailyBarStatus.INTRADAY_PARTIAL.value
            reason = "during_rth"
    else:
        # KR RTH approx 09:00–15:30 KST
        open_t, close_t = time(9, 0), time(15, 30)
        if now.date() > td:
            session_state = MarketSessionState.CLOSED.value
            bar_status = DailyBarStatus.FINAL.value
            reason = "after_trade_date_kst"
        elif now.date() < td:
            session_state = MarketSessionState.PREOPEN.value
            bar_status = DailyBarStatus.NOT_APPLICABLE.value
            reason = "before_trade_date_kst"
        elif now.time() < open_t:
            session_state = MarketSessionState.PREOPEN.value
            bar_status = DailyBarStatus.NOT_APPLICABLE.value
            reason = "before_krx_open"
        elif now.time() >= close_t:
            session_state = MarketSessionState.CLOSED.value
            bar_status = DailyBarStatus.FINAL.value
            reason = "after_krx_close"
        else:
            session_state = MarketSessionState.OPEN.value
            bar_status = DailyBarStatus.INTRADAY_PARTIAL.value
            reason = "during_krx_rth"

    return {
        "as_of_kst": now.isoformat(),
        "as_of_utc": as_of_utc.isoformat(),
        "data_cutoff_at_kst": now.isoformat(),
        "market_session_state": session_state,
        "daily_bar_status": bar_status,
        "clock_reason": reason,
    }


@dataclass
class RunModeDecision:
    run_mode: str
    replay_type: Optional[str]
    decision_artifact: bool
    allow_fixed_update: bool
    invoked_by: str
    reason: str
    warnings: List[str] = field(default_factory=list)


def is_past_trade_date(market: str, trade_date: str, now_kst: Optional[datetime] = None) -> bool:
    now = now_kst or datetime.now(KST)
    live = resolve_market_trade_date(market, now, mode="live")
    live_td = str(live.get("resolved_trade_date") or "")
    if live_td and str(trade_date) < live_td:
        return True
    # Same trade_date after RTH close (e.g. next-morning --force recalc) is also
    # a post-decision recalculation and must not silently overwrite Production.
    clock = resolve_data_clock(market=market, trade_date=trade_date, as_of_kst=now)
    if clock.get("daily_bar_status") == DailyBarStatus.FINAL.value:
        return True
    if clock.get("market_session_state") == MarketSessionState.CLOSED.value:
        return True
    return False


def resolve_run_mode_policy(
    *,
    explicit_run_mode: Optional[str],
    force: bool = False,
    trade_date: str,
    market: str,
    invoked_by: Optional[str] = None,
    allow_decision_overwrite: bool = False,
    explicit_replay_type: Optional[str] = None,
    fixed_artifact_exists: bool = False,
    now_kst: Optional[datetime] = None,
) -> RunModeDecision:
    """Explicit CLI mode wins; human CLI defaults to REPLAY."""
    warnings: List[str] = []
    inv = (invoked_by or os.getenv("SCREENER_INVOKED_BY") or "cli").strip() or "cli"
    mode = normalize_run_mode(explicit_run_mode)
    replay_type = normalize_replay_type(explicit_replay_type)
    past = is_past_trade_date(market, trade_date, now_kst=now_kst)

    if mode is None:
        # Default: REPLAY for direct/manual runs (including --force / past dates)
        mode = RUN_MODE_REPLAY
        reason = "default_cli_replay"
        if force:
            reason = "force_defaults_to_replay"
        elif past:
            reason = "past_trade_date_defaults_to_replay"
    else:
        reason = "explicit_run_mode"

    if mode == RUN_MODE_REPLAY:
        if replay_type is None:
            replay_type = REPLAY_TYPE_CURRENT
        if replay_type == REPLAY_TYPE_FROZEN:
            raise FrozenReplayNotImplemented(
                "FROZEN_INPUT_REPLAY is not implemented; "
                "snapshots are saved for future use. Use CURRENT_DATA_RECALCULATION."
            )
        return RunModeDecision(
            run_mode=RUN_MODE_REPLAY,
            replay_type=replay_type,
            decision_artifact=False,
            allow_fixed_update=False,
            invoked_by=inv,
            reason=reason,
            warnings=warnings,
        )

    # DECISION
    allow_fixed = True
    if past and not allow_decision_overwrite:
        # Explicit decision on past/post-close date without allow → convert to REPLAY
        warnings.append(
            "past_or_postclose_decision_without_allow→REPLAY "
            "(pass --allow-decision-overwrite with --run-mode decision to update Production)"
        )
        return RunModeDecision(
            run_mode=RUN_MODE_REPLAY,
            replay_type=REPLAY_TYPE_CURRENT,
            decision_artifact=False,
            allow_fixed_update=False,
            invoked_by=inv,
            reason="past_decision_denied_converted_to_replay",
            warnings=warnings,
        )

    if fixed_artifact_exists and not allow_decision_overwrite:
        allow_fixed = False
        warnings.append(
            "existing_decision_fixed_artifacts_present; "
            "immutable run dir will be written but Production fixed files will NOT be updated "
            "without --allow-decision-overwrite"
        )

    if past and allow_decision_overwrite:
        warnings.append("past_trade_date_decision_overwrite_allowed")

    return RunModeDecision(
        run_mode=RUN_MODE_DECISION,
        replay_type=None,
        decision_artifact=True,
        allow_fixed_update=allow_fixed,
        invoked_by=inv,
        reason=reason,
        warnings=warnings,
    )


def runs_root(output_dir: Optional[Path] = None) -> Path:
    return Path(output_dir or OUTPUT_DIR) / "runs"


def run_directory(
    *,
    market: str,
    trade_date: str,
    session: str,
    run_mode: str,
    run_id: str,
    output_dir: Optional[Path] = None,
) -> Path:
    mode = str(run_mode).strip().lower()
    return (
        runs_root(output_dir)
        / mode
        / str(market).upper()
        / str(trade_date)
        / str(session).lower()
        / str(run_id)
    )


def latest_decision_pointer_path(
    market: str,
    session: str,
    output_dir: Optional[Path] = None,
) -> Path:
    return Path(output_dir or OUTPUT_DIR) / "latest" / f"screener_decision_{market}_{session}.json"


def fixed_artifact_path(
    prefix: str,
    trade_date: str,
    market: str,
    session: str,
    output_dir: Optional[Path] = None,
    suffix: str = ".json",
) -> Path:
    return Path(output_dir or OUTPUT_DIR) / format_pipeline_artifact(
        prefix, trade_date, market, session, suffix=suffix
    )


def production_fixed_exists(
    trade_date: str,
    market: str,
    session: str,
    output_dir: Optional[Path] = None,
) -> bool:
    meta = fixed_artifact_path("screener_run_meta", trade_date, market, session, output_dir)
    cands = fixed_artifact_path("screener_candidates", trade_date, market, session, output_dir)
    return meta.exists() or cands.exists()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass
        raise


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, text.encode(encoding))


def atomic_write_json(path: Path, payload: Any, *, indent: int = 2) -> None:
    data = json.dumps(payload, ensure_ascii=False, indent=indent, default=str)
    atomic_write_text(path, data)


def atomic_copy_file(src: Path, dest: Path) -> None:
    src = Path(src)
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".tmp", dir=str(dest.parent))
    try:
        os.close(fd)
        shutil.copyfile(src, tmp_name)
        os.replace(tmp_name, dest)
    except Exception:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass
        raise


@dataclass
class ScreenerRunWriter:
    """Stage artifacts under a temp dir, then atomically publish the immutable run directory."""

    output_dir: Path
    market: str
    trade_date: str
    session: str
    run_mode: str
    run_id: str
    policy: RunModeDecision
    clock: Dict[str, Any]
    started_at_kst: str
    staging_dir: Path = field(init=False)
    final_dir: Path = field(init=False)
    published: bool = False
    status: str = "RUNNING"
    _files: Dict[str, Path] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir)
        mode_parent = (
            runs_root(self.output_dir)
            / str(self.run_mode).lower()
            / self.market
            / self.trade_date
            / self.session
        )
        mode_parent.mkdir(parents=True, exist_ok=True)
        self.run_id = ensure_unique_run_id(mode_parent, self.run_id)
        self.final_dir = mode_parent / self.run_id
        if self.final_dir.exists():
            raise ArtifactError(f"run directory already exists: {self.final_dir}")
        self.staging_dir = mode_parent / f".staging-{self.run_id}-{secrets.token_hex(2)}"
        self.staging_dir.mkdir(parents=True, exist_ok=False)
        (self.staging_dir / "inputs").mkdir(parents=True, exist_ok=True)

    def path(self, name: str) -> Path:
        return self.staging_dir / name

    def write_json(self, name: str, payload: Any) -> Path:
        p = self.path(name)
        p.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(p, payload)
        self._files[name] = p
        return p

    def write_text(self, name: str, text: str) -> Path:
        p = self.path(name)
        p.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(p, text)
        self._files[name] = p
        return p

    def write_inputs_json(self, name: str, payload: Any) -> Path:
        return self.write_json(f"inputs/{name}", payload)

    def artifact_digest_map(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for name, path in sorted(self._files.items()):
            if name == "manifest.json":
                continue
            if not path.exists():
                continue
            rel = name
            info: Dict[str, Any] = {"sha256": sha256_file(path)}
            rc = file_row_count(path)
            if rc is not None:
                info["row_count"] = rc
            out[rel] = info
        return out

    def build_manifest(
        self,
        *,
        status: str,
        result_status: str,
        completed_at_kst: str,
        production_threshold: float,
        production_candidate_count: int,
        shadow_threshold: Optional[float],
        shadow_candidate_count: int,
        score_count: int,
        config_sha256: Optional[str],
        issuer_groups_sha256: Optional[str],
        git_commit: Optional[str],
        git_commit_error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        artifacts = self.artifact_digest_map()
        # include review/meta if written under alternate names
        for fname in (
            "screener_run_meta.json",
            "screener_scores.json",
            "screener_candidates.json",
            "screener_candidates_full.json",
            "screener_shadow_candidates.json",
            "screener_holdings.json",
            "market_state.json",
            "screener_review.md",
            "screener.log",
        ):
            p = self.path(fname)
            if p.exists() and fname not in artifacts:
                info: Dict[str, Any] = {"sha256": sha256_file(p)}
                rc = file_row_count(p)
                if rc is not None:
                    info["row_count"] = rc
                artifacts[fname] = info

        manifest: Dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "run_id": self.run_id,
            "run_mode": self.run_mode,
            "decision_artifact": bool(self.policy.decision_artifact and status.startswith("SUCCESS")),
            "replay": self.run_mode == RUN_MODE_REPLAY,
            "replay_type": self.policy.replay_type,
            "invoked_by": self.policy.invoked_by,
            "market": self.market,
            "trade_date": self.trade_date,
            "session": self.session,
            "started_at_kst": self.started_at_kst,
            "completed_at_kst": completed_at_kst,
            "as_of_kst": self.clock.get("as_of_kst"),
            "as_of_utc": self.clock.get("as_of_utc"),
            "market_session_state": self.clock.get("market_session_state"),
            "daily_bar_status": self.clock.get("daily_bar_status"),
            "data_cutoff_at_kst": self.clock.get("data_cutoff_at_kst"),
            "status": status,
            "result_status": result_status,
            "production_threshold": production_threshold,
            "production_candidate_count": production_candidate_count,
            "shadow_threshold": shadow_threshold,
            "shadow_candidate_count": shadow_candidate_count,
            "score_count": score_count,
            "git_commit": git_commit,
            "git_commit_error": git_commit_error,
            "config_sha256": config_sha256,
            "issuer_groups_sha256": issuer_groups_sha256,
            "run_directory": str(self.final_dir),
            "compat_log_hint": str(
                self.output_dir
                / "logs"
                / f"screener_{self.trade_date}_{self.session}_{self.market}_{self.run_id}.log"
            ),
            "artifacts": artifacts,
            "policy_reason": self.policy.reason,
            "policy_warnings": list(self.policy.warnings),
        }
        if extra:
            manifest.update(extra)
        return manifest

    def publish(self, manifest: Dict[str, Any]) -> Path:
        """Write manifest, then atomically rename staging → final run dir."""
        if self.published:
            raise ArtifactError("run already published")
        if self.final_dir.exists():
            raise ArtifactError(f"refusing to overwrite existing run dir: {self.final_dir}")
        self.write_json("manifest.json", manifest)
        # Final atomic publish
        os.replace(str(self.staging_dir), str(self.final_dir))
        self.published = True
        self.status = str(manifest.get("status") or self.status)
        self.staging_dir = self.final_dir  # subsequent paths resolve to final
        return self.final_dir

    def abandon_staging(self) -> None:
        if self.published:
            return
        try:
            if self.staging_dir.exists():
                shutil.rmtree(self.staging_dir, ignore_errors=True)
        except Exception:
            pass


def promote_fixed_artifacts(
    writer: ScreenerRunWriter,
    *,
    review_relative: str = "screener_review.md",
) -> Dict[str, str]:
    """Copy successful DECISION run artifacts to Production fixed filenames."""
    if writer.run_mode != RUN_MODE_DECISION:
        raise ArtifactError("REPLAY must not promote fixed Production artifacts")
    if not writer.policy.allow_fixed_update:
        raise ArtifactError(
            "fixed Production update denied (existing decision without --allow-decision-overwrite)"
        )
    if not writer.published:
        raise ArtifactError("run must be published before promoting fixed artifacts")

    run_dir = writer.final_dir
    mapping = {
        "screener_run_meta.json": fixed_artifact_path(
            "screener_run_meta", writer.trade_date, writer.market, writer.session, writer.output_dir
        ),
        "screener_scores.json": fixed_artifact_path(
            "screener_scores", writer.trade_date, writer.market, writer.session, writer.output_dir
        ),
        "screener_candidates.json": fixed_artifact_path(
            "screener_candidates", writer.trade_date, writer.market, writer.session, writer.output_dir
        ),
        "screener_candidates_full.json": fixed_artifact_path(
            "screener_candidates_full", writer.trade_date, writer.market, writer.session, writer.output_dir
        ),
        "screener_shadow_candidates.json": fixed_artifact_path(
            "screener_shadow_candidates", writer.trade_date, writer.market, writer.session, writer.output_dir
        ),
        "screener_holdings.json": fixed_artifact_path(
            "screener_holdings", writer.trade_date, writer.market, writer.session, writer.output_dir
        ),
        "market_state.json": fixed_artifact_path(
            "market_state", writer.trade_date, writer.market, writer.session, writer.output_dir
        ),
    }
    review_src = run_dir / review_relative
    review_dest = (
        writer.output_dir
        / "reviews"
        / format_pipeline_artifact(
            "screener_review", writer.trade_date, writer.market, writer.session, suffix=".md"
        )
    )

    # Verify run SUCCESS first
    manifest_path = run_dir / "manifest.json"
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    status = str(manifest.get("status") or "")
    if not status.startswith("SUCCESS"):
        raise ArtifactError(f"refusing to promote FAILED/non-success run: status={status}")
    if manifest.get("run_mode") != RUN_MODE_DECISION:
        raise ArtifactError("manifest run_mode is not DECISION")

    shas: Dict[str, str] = {}
    # Write all fixed files (atomic) — order: data first, meta last
    for src_name, dest in mapping.items():
        src = run_dir / src_name
        if not src.exists():
            if src_name in ("screener_holdings.json", "market_state.json", "screener_shadow_candidates.json"):
                continue
            raise ArtifactError(f"missing run artifact for promotion: {src_name}")
        atomic_copy_file(src, dest)
        shas[dest.name] = sha256_file(dest)

    if review_src.exists():
        atomic_copy_file(review_src, review_dest)
        shas[review_dest.name] = sha256_file(review_dest)

    # Sidecar pointer on fixed meta already includes source_run_id; also write latest pointer
    write_latest_decision_pointer(
        output_dir=writer.output_dir,
        market=writer.market,
        session=writer.session,
        trade_date=writer.trade_date,
        run_id=writer.run_id,
        run_directory=run_dir,
        manifest_path=manifest_path,
    )
    return shas


def write_latest_decision_pointer(
    *,
    output_dir: Path,
    market: str,
    session: str,
    trade_date: str,
    run_id: str,
    run_directory: Path,
    manifest_path: Path,
) -> Path:
    pointer = {
        "run_id": run_id,
        "trade_date": trade_date,
        "session": session,
        "market": market,
        "run_directory": str(run_directory),
        "manifest_sha256": sha256_file(manifest_path) if manifest_path.exists() else None,
        "updated_at_kst": datetime.now(KST).isoformat(),
    }
    path = latest_decision_pointer_path(market, session, output_dir)
    atomic_write_json(path, pointer)
    return path


def load_manifest(run_dir: Path) -> Dict[str, Any]:
    with open(Path(run_dir) / "manifest.json", "r", encoding="utf-8") as f:
        return json.load(f)


def validate_decision_artifacts_for_trader(
    *,
    trade_date: str,
    market: str,
    session: str,
    output_dir: Optional[Path] = None,
    candidates_path: Optional[Path] = None,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Ensure Production fixed artifacts come from a successful DECISION run."""
    out = Path(output_dir or OUTPUT_DIR)
    meta_path = fixed_artifact_path("screener_run_meta", trade_date, market, session, out)
    cands_path = candidates_path or fixed_artifact_path(
        "screener_candidates", trade_date, market, session, out
    )
    if not meta_path.exists():
        return False, f"missing run_meta: {meta_path}", None
    if not cands_path.exists():
        return False, f"missing candidates: {cands_path}", None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as e:
        return False, f"run_meta unreadable: {e}", None

    run_mode = str(meta.get("run_mode") or "").upper()
    if run_mode and run_mode != RUN_MODE_DECISION:
        return False, f"run_mode={run_mode} is not DECISION (trader blocked)", meta
    if meta.get("decision_artifact") is False:
        return False, "decision_artifact=false (trader blocked)", meta
    if meta.get("replay") is True:
        return False, "replay=true (trader blocked)", meta
    status = str(meta.get("status") or "")
    if not status.startswith("SUCCESS"):
        return False, f"status={status} (trader blocked)", meta
    if str(meta.get("trade_date")) != str(trade_date):
        return False, "trade_date mismatch", meta
    if str(meta.get("session") or "").lower() != str(session).lower():
        return False, "session mismatch", meta
    if str(meta.get("market") or "").upper() != str(market).upper():
        return False, "market mismatch", meta

    # Hash check against immutable run manifest when source_run_id present
    source_run_id = meta.get("source_run_id") or meta.get("run_id")
    run_dir_hint = meta.get("run_directory")
    if source_run_id and run_dir_hint:
        man_path = Path(run_dir_hint) / "manifest.json"
        if man_path.exists():
            try:
                man = load_manifest(Path(run_dir_hint))
                if str(man.get("run_mode")) != RUN_MODE_DECISION:
                    return False, "source manifest run_mode != DECISION", meta
                art = (man.get("artifacts") or {}).get("screener_candidates.json") or {}
                expected = art.get("sha256")
                if expected:
                    actual = sha256_file(cands_path)
                    if actual != expected:
                        return (
                            False,
                            f"candidates sha256 mismatch expected={expected} actual={actual}",
                            meta,
                        )
                exp_count = art.get("row_count")
                if exp_count is not None:
                    with open(cands_path, "r", encoding="utf-8") as f:
                        rows = json.load(f)
                    if isinstance(rows, list) and len(rows) != int(exp_count):
                        return False, "candidate_count mismatch vs manifest", meta
            except Exception as e:
                return False, f"manifest validation error: {e}", meta

    # Never treat shadow file as trader input
    if "shadow" in cands_path.name.lower():
        return False, "shadow candidates are not trader inputs", meta

    return True, "ok", meta


def validate_screener_step_for_pipeline(
    *,
    trade_date: str,
    market: str,
    session: str,
    run_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Post-screener gate used by integrated_manager before news/gpt/trader."""
    out = Path(output_dir or OUTPUT_DIR)
    meta_path = fixed_artifact_path("screener_run_meta", trade_date, market, session, out)
    if not meta_path.exists():
        return False, f"Production run_meta missing after screener: {meta_path}", None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as e:
        return False, f"run_meta unreadable: {e}", None

    if str(meta.get("run_mode") or "").upper() != RUN_MODE_DECISION:
        return False, "fixed artifact is not DECISION", meta
    if meta.get("replay") is True:
        return False, "fixed artifact marked replay", meta
    if not str(meta.get("status") or "").startswith("SUCCESS"):
        return False, f"screener status={meta.get('status')}", meta
    if run_id and meta.get("source_run_id") and meta.get("source_run_id") != run_id:
        # integrated_manager may pass pipeline run_id; screener has its own run_id.
        # Prefer screener source_run_id match when SCREENER_RUN_ID env set.
        screener_run_id = os.getenv("SCREENER_RUN_ID") or os.getenv("PIPELINE_SCREENER_RUN_ID")
        if screener_run_id and meta.get("source_run_id") != screener_run_id:
            return False, "source_run_id does not match SCREENER_RUN_ID", meta

    ok, msg, _ = validate_decision_artifacts_for_trader(
        trade_date=trade_date, market=market, session=session, output_dir=out
    )
    if not ok:
        return False, msg, meta

    # candidate count consistency
    cands = fixed_artifact_path("screener_candidates", trade_date, market, session, out)
    try:
        with open(cands, "r", encoding="utf-8") as f:
            rows = json.load(f)
        n = len(rows) if isinstance(rows, list) else None
    except Exception as e:
        return False, f"candidates unreadable: {e}", meta
    meta_count = meta.get("production_candidate_count", meta.get("candidate_count"))
    if n is not None and meta_count is not None and int(n) != int(meta_count):
        return False, f"candidate_count mismatch file={n} meta={meta_count}", meta

    return True, "ok", meta


def build_score_features_snapshot(
    scores_records: Sequence[Dict[str, Any]],
    *,
    as_of: str,
    weighted_regime_score: Optional[float],
    scoring_market_component: Optional[float],
) -> List[Dict[str, Any]]:
    """Compact feature vectors for DECISION input provenance (not full OHLCV)."""
    out: List[Dict[str, Any]] = []
    for row in scores_records or []:
        out.append(
            {
                "ticker": row.get("ticker") or row.get("Ticker"),
                "as_of": as_of,
                "last_bar_date": row.get("updated_at") or row.get("last_bar_date"),
                "financial_score": row.get("fin_score"),
                "technical_score": row.get("tech_score"),
                "market_component": row.get("market_score"),
                "scoring_market_component": scoring_market_component,
                "weighted_regime_score": weighted_regime_score,
                "sector_score": row.get("sector_score"),
                "volatility_score": row.get("vol_kki"),
                "position_52w_score": row.get("pos_52w"),
                "final_score": row.get("score"),
                "rsi": row.get("rsi"),
                "ma50": row.get("ma50"),
                "ma200": row.get("ma200"),
                "per": row.get("per"),
                "pbr": row.get("pbr"),
                "held": row.get("held"),
                "issuer_group": row.get("issuer_group"),
                "exclusion_reasons": row.get("exclusion_reasons") or [],
                "eligibility_status": row.get("eligibility_status"),
                "threshold_pass": row.get("threshold_pass"),
                "data_source": row.get("excd") or row.get("ovrs_excg") or "kis",
            }
        )
    return out


def clarify_regime_fields(
    *,
    components_avg: Optional[float],
    scoring_market_component: Optional[float],
    advanced_market_confidence: Optional[float] = None,
    components: Optional[Dict[str, Any]] = None,
    trend: Optional[str] = None,
    volatility: Optional[str] = None,
    regime_label: Optional[str] = None,
) -> Dict[str, Any]:
    """Single source of truth for regime naming in meta/review/market_state.

    - weighted_regime_score: equal-weight average of regime components
      (``_get_market_regime_score`` result).
    - scoring_market_component: value injected into stock scores
      (``0.7 * weighted + 0.3 * 0.5``).
    - advanced_market_confidence: MarketAnalyzer confidence.

    Deprecated alias: ``regime_score`` → weighted_regime_score only (never the
    scoring blend), so Review/Meta cannot mix the two values under one name.
    """
    return {
        "weighted_regime_score": components_avg,
        "scoring_market_component": scoring_market_component,
        "advanced_market_confidence": advanced_market_confidence,
        "regime_components": components or {},
        "trend": trend,
        "volatility": volatility,
        "regime": regime_label,
        # deprecated alias — documented as weighted_regime_score only
        "regime_score": components_avg,
        "regime_score_deprecated_alias_of": "weighted_regime_score",
    }
