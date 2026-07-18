"""Screener operational helpers: funnel, meta, cache, shadow, issuer groups.

Production candidate policy is unchanged unless config explicitly enables
non-static threshold modes. Shadow outputs never feed trader inputs.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("screener_ops")

RUN_META_SCHEMA = "1.0"
AMOUNT5D_CACHE_SCHEMA = "2"
SCORE_EXPORT_SCHEMA = "1.3"

# Lines from child screener stdout that should surface at INFO in the parent.
_SCREENER_SUMMARY_PATTERNS = [
    re.compile(r"1차 필터링"),
    re.compile(r"최종 선정"),
    re.compile(r"스코어 분포"),
    re.compile(r"최소점수"),
    re.compile(r"최종 후보"),
    re.compile(r"EMPTY"),
    re.compile(r"empty_reason", re.I),
    re.compile(r"data.?quality", re.I),
    re.compile(r"MARKET_CAP"),
    re.compile(r"AMOUNT5D"),
    re.compile(r"\[AMOUNT5D_CACHE\]"),
    re.compile(r"퍼널"),
    re.compile(r"실행시간|duration|⏱", re.I),
    re.compile(r"result_status", re.I),
    re.compile(r"후보\(슬림\)|스크리닝 완료"),
    re.compile(r"SKIPPED|NOT_RUN|SUCCESS_WITH_WARNINGS"),
]


@dataclass
class StageResult:
    stage: str
    status: str  # APPLIED | SKIPPED | NOT_RUN
    input_count: int
    output_count: int
    dropped_count: int = 0
    threshold: Optional[float] = None
    reason: Optional[str] = None
    duration_sec: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        in_c = max(0, int(self.input_count or 0))
        out_c = max(0, int(self.output_count or 0))
        if out_c > in_c:
            out_c = in_c
        self.input_count = in_c
        self.output_count = out_c
        self.dropped_count = in_c - out_c

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "stage": self.stage,
            "status": self.status,
            "input_count": self.input_count,
            "output_count": self.output_count,
            "dropped_count": self.dropped_count,
        }
        if self.threshold is not None:
            d["threshold"] = self.threshold
        if self.reason:
            d["reason"] = self.reason
        if self.duration_sec is not None:
            d["duration_sec"] = round(float(self.duration_sec), 3)
        if self.extra:
            d.update(self.extra)
        return d


class FunnelRecorder:
    """Ordered funnel stages with invariant checks."""

    def __init__(self) -> None:
        self.stages: List[StageResult] = []
        self.findings: List[Dict[str, str]] = []

    def record(self, result: StageResult) -> StageResult:
        self.stages.append(result)
        return result

    def record_applied(
        self,
        stage: str,
        input_count: int,
        output_count: int,
        *,
        threshold: Optional[float] = None,
        duration_sec: Optional[float] = None,
        reason: Optional[str] = None,
        **extra: Any,
    ) -> StageResult:
        return self.record(
            StageResult(
                stage=stage,
                status="APPLIED",
                input_count=input_count,
                output_count=output_count,
                threshold=threshold,
                reason=reason,
                duration_sec=duration_sec,
                extra=extra,
            )
        )

    def record_skipped(
        self,
        stage: str,
        input_count: int,
        *,
        reason: str,
        duration_sec: Optional[float] = None,
        threshold: Optional[float] = None,
        **extra: Any,
    ) -> StageResult:
        return self.record(
            StageResult(
                stage=stage,
                status="SKIPPED",
                input_count=input_count,
                output_count=input_count,
                reason=reason,
                duration_sec=duration_sec,
                threshold=threshold,
                extra=extra,
            )
        )

    def record_not_run(
        self,
        stage: str,
        *,
        reason: str = "NO_INPUT",
        input_count: int = 0,
        **extra: Any,
    ) -> StageResult:
        return self.record(
            StageResult(
                stage=stage,
                status="NOT_RUN",
                input_count=input_count,
                output_count=0 if reason == "NO_INPUT" else input_count,
                reason=reason,
                extra=extra,
            )
        )

    def add_finding(self, code: str, severity: str = "WARNING") -> None:
        self.findings.append({"code": code, "severity": severity})

    def validate(self) -> List[str]:
        """Return list of invariant violation messages."""
        errors: List[str] = []
        for i, st in enumerate(self.stages):
            if st.dropped_count < 0:
                errors.append(f"{st.stage}: negative dropped_count={st.dropped_count}")
            if st.output_count < 0 or st.input_count < 0:
                errors.append(f"{st.stage}: negative counts")
            if st.output_count > st.input_count:
                errors.append(
                    f"{st.stage}: output_count({st.output_count}) > input_count({st.input_count})"
                )
            if st.dropped_count != st.input_count - st.output_count:
                errors.append(
                    f"{st.stage}: dropped_count mismatch "
                    f"{st.dropped_count} != {st.input_count - st.output_count}"
                )
            if i > 0:
                prev = self.stages[i - 1]
                # NOT_RUN / SKIPPED chains still must chain input from previous output
                # except when previous was NOT_RUN with reason NO_INPUT (both 0).
                if st.input_count != prev.output_count:
                    # Allow NOT_RUN after empty previous when both are 0
                    if not (
                        prev.output_count == 0
                        and st.status == "NOT_RUN"
                        and st.input_count == 0
                    ):
                        errors.append(
                            f"{st.stage}: input_count({st.input_count}) != "
                            f"previous {prev.stage}.output_count({prev.output_count})"
                        )
        return errors

    def sanitize_for_storage(self) -> Tuple[List[Dict[str, Any]], bool]:
        """Validate; on violation rebuild a safe chain and flag warning."""
        errors = self.validate()
        if not errors:
            return [s.to_dict() for s in self.stages], False

        for err in errors:
            logger.warning("[SCREENER_FUNNEL_INVARIANT_VIOLATION] %s", err)
        self.add_finding("SCREENER_FUNNEL_INVARIANT_VIOLATION", "WARNING")

        # Rebuild a monotonic safe chain from recorded stages without inventing
        # pass-through counts that imply filters ran successfully.
        safe: List[StageResult] = []
        prev_out = None
        for st in self.stages:
            inp = st.input_count if prev_out is None else prev_out
            if st.status == "NOT_RUN" and inp == 0:
                out = 0
            elif st.status == "SKIPPED":
                out = inp
            else:
                out = min(max(0, st.output_count), inp)
            fixed = StageResult(
                stage=st.stage,
                status=st.status,
                input_count=inp,
                output_count=out,
                threshold=st.threshold,
                reason=st.reason or ("INVARIANT_REPAIRED" if errors else None),
                duration_sec=st.duration_sec,
                extra=dict(st.extra or {}),
            )
            if errors and fixed.reason != st.reason:
                fixed.extra.setdefault("original_output_count", st.output_count)
                fixed.extra.setdefault("original_input_count", st.input_count)
            safe.append(fixed)
            prev_out = fixed.output_count
        self.stages = safe
        return [s.to_dict() for s in self.stages], True

    def log(self, title: str = "스크리너") -> None:
        if not self.stages:
            return
        start = self.stages[0].input_count or 1
        logger.info("┌─ %s 퍼널 ──────────────────", title)
        for st in self.stages:
            tag = st.status
            extra = f" [{tag}]"
            if st.reason:
                extra += f" reason={st.reason}"
            if st.threshold is not None:
                extra += f" thr={st.threshold}"
            pct = (st.output_count / start * 100.0) if start else 0.0
            logger.info(
                "│ %-28s in=%4d out=%4d drop=%4d (%.1f%%)%s",
                st.stage,
                st.input_count,
                st.output_count,
                st.dropped_count,
                pct,
                extra,
            )
        logger.info("└──────────────────────────────────────")


def atomic_write_json(path: Path, payload: Any, *, indent: int = 2) -> None:
    """Write JSON via temp file + atomic rename."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False, indent=indent, default=_json_default)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
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


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)!r} is not JSON serializable")


def score_distribution(scores: Sequence[float]) -> Dict[str, Any]:
    if not scores:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "p75": None,
            "p90": None,
        }
    s = pd.Series(list(scores), dtype="float64")
    return {
        "count": int(len(s)),
        "mean": round(float(s.mean()), 4),
        "median": round(float(s.median()), 4),
        "min": round(float(s.min()), 4),
        "max": round(float(s.max()), 4),
        "p75": round(float(s.quantile(0.75)), 4),
        "p90": round(float(s.quantile(0.90)), 4),
    }


def amount5d_distribution(values: Sequence[float], threshold: float) -> Dict[str, Any]:
    s = pd.to_numeric(pd.Series(list(values)), errors="coerce").dropna()
    if s.empty:
        return {
            "count": 0,
            "min": None,
            "median": None,
            "p75": None,
            "p90": None,
            "p95": None,
            "max": None,
            "configured_threshold": threshold,
            "pass_count": 0,
            "pass_ratio": 0.0,
        }
    pass_count = int((s >= threshold).sum())
    return {
        "count": int(len(s)),
        "min": float(s.min()),
        "median": float(s.median()),
        "p75": float(s.quantile(0.75)),
        "p90": float(s.quantile(0.90)),
        "p95": float(s.quantile(0.95)),
        "max": float(s.max()),
        "configured_threshold": float(threshold),
        "pass_count": pass_count,
        "pass_ratio": round(pass_count / len(s), 4) if len(s) else 0.0,
    }


def marcap_filter_decision(
    marcap: pd.Series,
    *,
    min_mc: float,
    max_mc: float,
    is_us: bool,
    min_valid_ratio: float = 0.8,
) -> Tuple[str, str, pd.Series]:
    """Decide whether to apply marcap filter.

    Returns (status, reason, mask). status is APPLIED or SKIPPED.
    When SKIPPED, mask is all-True (no filtering) but status must not be
    presented as a successful pass of all names.
    """
    mc = pd.to_numeric(marcap, errors="coerce")
    n = len(mc)
    if n == 0:
        return "SKIPPED", "MARKET_CAP_DATA_UNAVAILABLE", pd.Series(dtype=bool)

    valid = mc.notna() & (mc > 0)
    valid_ratio = float(valid.sum()) / float(n) if n else 0.0

    if is_us and valid_ratio < min_valid_ratio:
        return (
            "SKIPPED",
            "MARKET_CAP_DATA_UNAVAILABLE",
            pd.Series(True, index=marcap.index),
        )

    if not is_us and valid_ratio < min_valid_ratio:
        return (
            "SKIPPED",
            "MARKET_CAP_DATA_UNAVAILABLE",
            pd.Series(True, index=marcap.index),
        )

    mask = (mc >= min_mc) & (mc <= max_mc)
    mask = mask.fillna(False)
    return "APPLIED", "", mask


def compute_shadow_score_threshold(
    scores: Sequence[float],
    policy: Dict[str, Any],
) -> Optional[float]:
    """Shadow hybrid: max(floor, P{percentile}). Returns None if disabled/empty."""
    if not policy or not policy.get("shadow_enabled", False):
        return None
    if not scores:
        return None
    mode = str(policy.get("shadow_mode", "hybrid")).lower()
    floor = float(policy.get("shadow_floor", 0.42) or 0.42)
    pct = float(policy.get("shadow_percentile", 0.90) or 0.90)
    s = pd.Series(list(scores), dtype="float64")
    p_val = float(s.quantile(pct))
    if mode == "percentile":
        return p_val
    if mode == "floor":
        return floor
    # hybrid (default)
    return max(floor, p_val)


def compute_shadow_liquidity_threshold(
    amounts: Sequence[float],
    policy: Dict[str, Any],
) -> Optional[float]:
    if not policy or not policy.get("shadow_enabled", False):
        return None
    if not amounts:
        return None
    mode = str(policy.get("shadow_mode", "percentile")).lower()
    pct = float(policy.get("shadow_percentile", 0.90) or 0.90)
    min_thr = float(policy.get("shadow_min_threshold", 0) or 0)
    s = pd.to_numeric(pd.Series(list(amounts)), errors="coerce").dropna()
    if s.empty:
        return None
    p_val = float(s.quantile(pct))
    if mode == "static":
        return float(policy.get("static_threshold", p_val))
    return max(min_thr, p_val)


def resolve_issuer_group(ticker: str, mapping: Dict[str, str]) -> str:
    t = str(ticker or "").strip().upper()
    if not t:
        return ""
    return str(mapping.get(t, t)).strip().upper() or t


def load_issuer_group_map(cfg: Dict[str, Any], config_dir: Optional[Path] = None) -> Dict[str, str]:
    """Load issuer groups from screener_params and optional mapping file."""
    sp = cfg.get("screener_params") or cfg
    mapping: Dict[str, str] = {}
    inline = sp.get("issuer_groups") or {}
    if isinstance(inline, dict):
        mapping.update({str(k).upper(): str(v).upper() for k, v in inline.items()})

    map_file = sp.get("issuer_groups_file")
    if map_file:
        path = Path(map_file)
        if not path.is_absolute() and config_dir is not None:
            path = config_dir / path
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = f.read()
                # allow jsonc
                try:
                    from utils import strip_jsonc_comments

                    raw = strip_jsonc_comments(raw)
                except Exception:
                    pass
                data = json.loads(raw)
                if isinstance(data, dict):
                    groups = data.get("issuer_groups", data)
                    if isinstance(groups, dict):
                        mapping.update(
                            {str(k).upper(): str(v).upper() for k, v in groups.items()}
                        )
            except Exception as e:
                logger.warning("issuer_groups 파일 로드 실패(%s): %s", path, e)

    # Built-in Alphabet defaults if not overridden
    mapping.setdefault("GOOG", "ALPHABET")
    mapping.setdefault("GOOGL", "ALPHABET")
    return mapping


def dedupe_by_issuer_group(
    df: pd.DataFrame,
    *,
    ticker_col: str = "Ticker",
    score_col: str = "Score",
    issuer_col: str = "issuer_group",
) -> pd.DataFrame:
    """Keep highest-score row per issuer_group."""
    if df is None or df.empty:
        return df
    out = df.copy()
    if issuer_col not in out.columns:
        return out
    out = out.sort_values(by=[score_col], ascending=False)
    return out.drop_duplicates(subset=[issuer_col], keep="first")


def classify_empty_result(
    *,
    candidate_count: int,
    scored_count: int,
    universe_count: int,
    amount5d_pass: int,
    scoring_failures_all: bool,
    data_quality_codes: Sequence[str],
    min_score_pass: int,
    empty_after_min_score: bool,
) -> Tuple[str, str, Optional[str]]:
    """Return (status, result_status, empty_reason)."""
    if candidate_count > 0:
        return "SUCCESS", "HAS_CANDIDATES", None

    hard_dq = {
        "PRICE_DATA_UNAVAILABLE",
        "AMOUNT5D_DATA_UNAVAILABLE",
        "SCORING_FAILURE_ALL",
        "UNIVERSE_EMPTY",
    }
    dq_hit = [c for c in data_quality_codes if c in hard_dq]
    if universe_count <= 0:
        return "SUCCESS", "EMPTY_DATA_QUALITY", "UNIVERSE_EMPTY"
    if amount5d_pass <= 0 and "AMOUNT5D_DATA_UNAVAILABLE" in data_quality_codes:
        return "SUCCESS", "EMPTY_DATA_QUALITY", "AMOUNT5D_DATA_UNAVAILABLE"
    if scoring_failures_all or scored_count <= 0:
        return "SUCCESS", "EMPTY_DATA_QUALITY", "SCORING_FAILURE_ALL"
    if dq_hit and scored_count <= 0:
        return "SUCCESS", "EMPTY_DATA_QUALITY", dq_hit[0]
    if empty_after_min_score or min_score_pass == 0:
        return "SUCCESS", "EMPTY_VALID", "MIN_SCORE_THRESHOLD_NOT_MET"
    return "SUCCESS", "EMPTY_VALID", "NO_CANDIDATES_AFTER_FILTERS"


def eligibility_for_row(
    *,
    held: bool,
    exclude_reasons: List[str],
    rsi: Optional[float],
    rsi_overheated_threshold: float,
    threshold_pass: bool,
    momentum_pass: bool,
    volatility_pass: bool,
    exclude_held: bool = True,
) -> Tuple[str, List[str]]:
    """Return (eligibility_status, exclusion_reasons)."""
    reasons = list(exclude_reasons or [])
    if held and exclude_held and "ALREADY_HELD" not in reasons:
        reasons.append("ALREADY_HELD")
    if rsi is not None and not (isinstance(rsi, float) and math.isnan(rsi)):
        if float(rsi) >= float(rsi_overheated_threshold) and "RSI_OVERHEATED" not in reasons:
            reasons.append("RSI_OVERHEATED")
    if not threshold_pass and "BELOW_MIN_SCORE" not in reasons:
        # Only attach when used as candidate exclusion annotation
        pass
    if not momentum_pass and "NEGATIVE_MOMENTUM" not in reasons:
        pass
    if not volatility_pass and "HIGH_VOLATILITY" not in reasons:
        pass

    status = "ELIGIBLE" if not reasons else "EXCLUDED"
    # held alone with exclude_held makes EXCLUDED
    if held and exclude_held:
        status = "EXCLUDED"
    if any(r in reasons for r in ("UP_STREAK", "RSI_OVERHEATED", "ALREADY_HELD", "NEWLY_LISTED")):
        status = "EXCLUDED"
    return status, reasons


# ───────────────── Amount5D cache ─────────────────

def amount5d_cache_path(output_dir: Path, market: str, trade_date: str) -> Path:
    return Path(output_dir) / "cache" / f"amount5d_{market}_{trade_date}_v{AMOUNT5D_CACHE_SCHEMA}.json"


def load_amount5d_cache(
    path: Path,
    *,
    market: str,
    trade_date: str,
    lookback_days: int = 5,
    data_source: str = "kis",
) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("[AMOUNT5D_CACHE] corrupt/unreadable %s: %s", path, e)
        return None

    if str(data.get("schema_version")) != AMOUNT5D_CACHE_SCHEMA:
        logger.info("[AMOUNT5D_CACHE] schema mismatch → ignore")
        return None
    if data.get("market") != market or str(data.get("trade_date")) != str(trade_date):
        return None
    if int(data.get("lookback_days", 5)) != int(lookback_days):
        return None
    if data.get("data_source") != data_source:
        return None
    if not isinstance(data.get("entries"), dict):
        return None
    return data


def save_amount5d_cache(path: Path, payload: Dict[str, Any]) -> None:
    atomic_write_json(path, payload)


def merge_amount5d_cache_entries(
    existing: Optional[Dict[str, Any]],
    *,
    market: str,
    trade_date: str,
    lookback_days: int,
    data_source: str,
    new_entries: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    entries = {}
    if existing and isinstance(existing.get("entries"), dict):
        entries.update(existing["entries"])
    entries.update(new_entries)
    return {
        "schema_version": AMOUNT5D_CACHE_SCHEMA,
        "market": market,
        "trade_date": trade_date,
        "lookback_days": lookback_days,
        "data_source": data_source,
        "cached_at_kst": datetime.now().isoformat(),
        "entries": entries,
    }


# ───────────────── Score export / selection ─────────────────

def enrich_scored_dataframe(
    df: pd.DataFrame,
    *,
    held_tickers: set,
    issuer_map: Dict[str, str],
    production_threshold: float,
    rsi_overheated_threshold: float = 70.0,
    exclude_held_from_candidates: bool = True,
    momentum_pass_map: Optional[Dict[str, bool]] = None,
    volatility_pass_map: Optional[Dict[str, bool]] = None,
) -> pd.DataFrame:
    """Annotate all scored rows with eligibility / rank / pass flags."""
    if df is None or df.empty:
        return df
    out = df.copy()
    if "Ticker" not in out.columns:
        out = out.reset_index().rename(columns={"index": "Ticker"})
    out["Ticker"] = out["Ticker"].astype(str).str.upper()
    out = out.sort_values(by=["Score"], ascending=False).reset_index(drop=True)
    out["score_rank"] = np.arange(1, len(out) + 1)
    out["issuer_group"] = out["Ticker"].map(lambda t: resolve_issuer_group(t, issuer_map))
    out["held"] = out["Ticker"].map(lambda t: t in held_tickers)
    out["threshold_pass"] = out["Score"] >= float(production_threshold)

    mom = momentum_pass_map or {}
    vol = volatility_pass_map or {}
    out["momentum_pass"] = out["Ticker"].map(lambda t: bool(mom.get(t, True)))
    out["volatility_pass"] = out["Ticker"].map(lambda t: bool(vol.get(t, True)))

    statuses = []
    reasons_list = []
    for _, row in out.iterrows():
        base_reasons = row.get("exclude_reasons") or []
        if isinstance(base_reasons, str):
            base_reasons = [base_reasons] if base_reasons else []
        elif not isinstance(base_reasons, list):
            try:
                base_reasons = list(base_reasons)
            except Exception:
                base_reasons = []
        status, reasons = eligibility_for_row(
            held=bool(row.get("held")),
            exclude_reasons=base_reasons,
            rsi=row.get("RSI"),
            rsi_overheated_threshold=rsi_overheated_threshold,
            threshold_pass=bool(row.get("threshold_pass")),
            momentum_pass=bool(row.get("momentum_pass")),
            volatility_pass=bool(row.get("volatility_pass")),
            exclude_held=exclude_held_from_candidates,
        )
        statuses.append(status)
        reasons_list.append(reasons)
    out["eligibility_status"] = statuses
    out["exclusion_reasons"] = reasons_list
    # Keep legacy key in sync for diversify_by_sector
    out["exclude_reasons"] = reasons_list
    return out


def scores_records_for_export(df: pd.DataFrame, *, trade_date: str) -> List[Dict[str, Any]]:
    """Build screener_scores JSON rows (all successfully scored tickers)."""
    if df is None or df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        rec = {
            "ticker": str(row.get("Ticker", "")),
            "name": str(row.get("Name", "") or ""),
            "sector": str(row.get("Sector", "") or ""),
            "score": _num(row.get("Score")),
            "fin_score": _num(row.get("FinScore")),
            "tech_score": _num(row.get("TechScore")),
            "market_score": _num(row.get("MktScore")),
            "sector_score": _num(row.get("SectorScore")),
            "pattern_score": _num(row.get("PatternScore")),
            "vol_kki": _num(row.get("VolKki")),
            "pos_52w": _num(row.get("Pos52w")),
            "rsi": _num(row.get("RSI")),
            "atr": _num(row.get("ATR")),
            "ma50": _num(row.get("MA50")),
            "ma200": _num(row.get("MA200")),
            "per": _num(row.get("PER")),
            "pbr": _num(row.get("PBR")),
            "price": _num(row.get("Price")),
            "exclusion_reasons": list(row.get("exclusion_reasons") or row.get("exclude_reasons") or []),
            "held": bool(row.get("held", False)),
            "issuer_group": str(row.get("issuer_group") or row.get("Ticker") or ""),
            "score_rank": int(row.get("score_rank") or 0),
            "threshold_pass": bool(row.get("threshold_pass", False)),
            "momentum_pass": bool(row.get("momentum_pass", True)),
            "volatility_pass": bool(row.get("volatility_pass", True)),
            "eligibility_status": str(row.get("eligibility_status") or "UNKNOWN"),
            "updated_at": trade_date,
            "schema_version": SCORE_EXPORT_SCHEMA,
        }
        # Optional extras
        for src, dst in (
            ("EXCD", "excd"),
            ("OvrsExcg", "ovrs_excg"),
            ("Amount5D", "amount5d"),
            ("Marcap", "marcap"),
        ):
            if src in row.index and pd.notna(row.get(src)):
                rec[dst] = _num(row.get(src)) if src in ("Amount5D", "Marcap") else row.get(src)
        records.append(rec)
    return records


def _num(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def _is_clean_exclude_reasons(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, str):
        return len(val.strip()) == 0
    try:
        return len(val) == 0
    except Exception:
        return True


def select_candidates_pipeline(
    scored_df: pd.DataFrame,
    *,
    threshold: float,
    require_positive_momentum: bool,
    exclude_high_volatility: bool,
    top_n: int,
    sector_cap: float,
    diversify_fn,
    apply_issuer_dedupe: bool = True,
    require_eligible: bool = True,
    max_candidates: Optional[int] = None,
) -> Tuple[pd.DataFrame, List[StageResult]]:
    """Apply min-score → momentum/vol → issuer dedupe → sector diversify.

    Returns (candidates_df, ordered StageResult list for MIN_SCORE..SECTOR).
    When a stage receives 0 input, subsequent stages are NOT_RUN/NO_INPUT.
    Disabled momentum/vol with input>0 → SKIPPED (pass-through).
    """
    stages: List[StageResult] = []
    empty = scored_df.iloc[0:0] if scored_df is not None else pd.DataFrame()
    cur = scored_df.copy() if scored_df is not None and not scored_df.empty else empty

    def _tail_not_run_from(start_name: str) -> None:
        order = ["MIN_SCORE", "MOMENTUM", "VOLATILITY", "SECTOR_DIVERSIFICATION"]
        # issuer is folded into sector diversification stage for meta schema compatibility
        started = False
        for name in order:
            if name == start_name:
                started = True
            if started:
                stages.append(StageResult(name, "NOT_RUN", 0, 0, reason="NO_INPUT"))

    n_in = len(cur)
    if n_in == 0:
        _tail_not_run_from("MIN_SCORE")
        return empty, stages

    # MIN_SCORE: score threshold only (eligibility applied later in SECTOR stage)
    passed = cur[cur["Score"] >= float(threshold)].copy()
    stages.append(
        StageResult("MIN_SCORE", "APPLIED", n_in, len(passed), threshold=float(threshold))
    )
    cur = passed

    # MOMENTUM
    n_in = len(cur)
    if n_in == 0:
        stages.append(StageResult("MOMENTUM", "NOT_RUN", 0, 0, reason="NO_INPUT"))
        stages.append(StageResult("VOLATILITY", "NOT_RUN", 0, 0, reason="NO_INPUT"))
        stages.append(StageResult("SECTOR_DIVERSIFICATION", "NOT_RUN", 0, 0, reason="NO_INPUT"))
        return empty, stages
    if require_positive_momentum:
        cur = cur[cur["momentum_pass"] == True].copy()  # noqa: E712
        stages.append(StageResult("MOMENTUM", "APPLIED", n_in, len(cur)))
    else:
        stages.append(StageResult("MOMENTUM", "SKIPPED", n_in, n_in, reason="DISABLED_IN_CONFIG"))

    # VOLATILITY
    n_in = len(cur)
    if n_in == 0:
        stages.append(StageResult("VOLATILITY", "NOT_RUN", 0, 0, reason="NO_INPUT"))
        stages.append(StageResult("SECTOR_DIVERSIFICATION", "NOT_RUN", 0, 0, reason="NO_INPUT"))
        return empty, stages
    if exclude_high_volatility:
        cur = cur[cur["volatility_pass"] == True].copy()  # noqa: E712
        stages.append(StageResult("VOLATILITY", "APPLIED", n_in, len(cur)))
    else:
        stages.append(StageResult("VOLATILITY", "SKIPPED", n_in, n_in, reason="DISABLED_IN_CONFIG"))

    # SECTOR_DIVERSIFICATION: eligibility + issuer dedupe + sector cap
    n_in = len(cur)
    if n_in == 0:
        stages.append(StageResult("SECTOR_DIVERSIFICATION", "NOT_RUN", 0, 0, reason="NO_INPUT"))
        return empty, stages

    work = cur
    if require_eligible:
        if "eligibility_status" in work.columns:
            work = work[work["eligibility_status"] == "ELIGIBLE"].copy()
        elif "exclude_reasons" in work.columns:
            work = work[work["exclude_reasons"].apply(_is_clean_exclude_reasons)].copy()
    if apply_issuer_dedupe and not work.empty:
        work = dedupe_by_issuer_group(work)

    limit_n = int(max_candidates) if max_candidates is not None else int(top_n)
    if work.empty:
        stages.append(StageResult("SECTOR_DIVERSIFICATION", "APPLIED", n_in, 0))
        return empty, stages

    indexed = work.set_index("Ticker") if "Ticker" in work.columns else work
    diversified = diversify_fn(indexed, limit_n, sector_cap)
    if diversified is None or diversified.empty:
        stages.append(StageResult("SECTOR_DIVERSIFICATION", "APPLIED", n_in, 0))
        return empty, stages
    out = diversified.reset_index() if "Ticker" not in getattr(diversified, "columns", []) else diversified
    if "Ticker" not in out.columns and out.index.name == "Ticker":
        out = out.reset_index()
    # Do not reintroduce excluded filler names
    if require_eligible and "exclude_reasons" in out.columns:
        out = out[out["exclude_reasons"].apply(_is_clean_exclude_reasons)].copy()
    stages.append(StageResult("SECTOR_DIVERSIFICATION", "APPLIED", n_in, len(out)))
    return out, stages


def build_run_meta(
    *,
    market: str,
    trade_date: str,
    session: str,
    status: str,
    result_status: str,
    empty_reason: Optional[str],
    started_at_kst: str,
    finished_at_kst: str,
    duration_sec: float,
    market_state: Dict[str, Any],
    funnel: List[Dict[str, Any]],
    score_distribution_data: Dict[str, Any],
    configured_threshold: float,
    effective_threshold: float,
    candidate_count: int,
    data_quality_findings: List[Dict[str, str]],
    stage_durations_sec: Dict[str, float],
    amount5d_stats: Optional[Dict[str, Any]] = None,
    amount5d_cache_stats: Optional[Dict[str, Any]] = None,
    shadow: Optional[Dict[str, Any]] = None,
    production_shadow_difference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "schema_version": RUN_META_SCHEMA,
        "market": market,
        "trade_date": trade_date,
        "session": session,
        "status": status,
        "result_status": result_status,
        "empty_reason": empty_reason,
        "started_at_kst": started_at_kst,
        "finished_at_kst": finished_at_kst,
        "duration_sec": round(float(duration_sec), 3),
        "market_state": market_state or {},
        "funnel": funnel,
        "score_distribution": score_distribution_data,
        "configured_threshold": configured_threshold,
        "effective_threshold": effective_threshold,
        "production_threshold": configured_threshold,
        "production_candidate_count": candidate_count,
        "candidate_count": candidate_count,
        "data_quality_findings": data_quality_findings or [],
        "stage_durations_sec": stage_durations_sec or {},
    }
    if amount5d_stats is not None:
        meta["amount5d_distribution"] = amount5d_stats
    if amount5d_cache_stats is not None:
        meta["amount5d_cache"] = amount5d_cache_stats
    if shadow is not None:
        meta["shadow"] = shadow
        meta["shadow_threshold"] = shadow.get("threshold")
        meta["shadow_candidate_count"] = shadow.get("candidate_count", 0)
    if production_shadow_difference is not None:
        meta["production_shadow_difference"] = production_shadow_difference
    return meta


def write_review_markdown(
    path: Path,
    meta: Dict[str, Any],
    *,
    top_scores: List[Dict[str, Any]],
    production_candidates: List[Dict[str, Any]],
    shadow_candidates: List[Dict[str, Any]],
    previous_meta: Optional[Dict[str, Any]] = None,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    lines.append(f"# Screener Review — {meta.get('market')} {meta.get('trade_date')} {meta.get('session')}")
    lines.append("")
    lines.append("## Status")
    lines.append(f"- status: `{meta.get('status')}`")
    lines.append(f"- result_status: `{meta.get('result_status')}`")
    lines.append(f"- empty_reason: `{meta.get('empty_reason')}`")
    lines.append(f"- duration_sec: {meta.get('duration_sec')}")
    lines.append("")
    ms = meta.get("market_state") or {}
    lines.append("## Market Regime")
    lines.append(f"- regime_score: {ms.get('regime_score')}")
    lines.append(f"- trend: {ms.get('trend')}")
    lines.append(f"- volatility: {ms.get('volatility')}")
    lines.append("")
    lines.append("## Funnel")
    lines.append("| stage | status | in | out | drop | reason |")
    lines.append("|---|---|---:|---:|---:|---|")
    for st in meta.get("funnel") or []:
        lines.append(
            f"| {st.get('stage')} | {st.get('status')} | {st.get('input_count')} | "
            f"{st.get('output_count')} | {st.get('dropped_count')} | {st.get('reason', '')} |"
        )
    lines.append("")
    sd = meta.get("score_distribution") or {}
    lines.append("## Score Distribution")
    lines.append(
        f"- count={sd.get('count')} mean={sd.get('mean')} median={sd.get('median')} "
        f"min={sd.get('min')} max={sd.get('max')} p75={sd.get('p75')} p90={sd.get('p90')}"
    )
    lines.append(f"- production_threshold: {meta.get('production_threshold')}")
    lines.append(f"- production_candidate_count: {meta.get('production_candidate_count')}")
    shadow = meta.get("shadow") or {}
    lines.append(f"- shadow_threshold: {shadow.get('threshold')}")
    lines.append(f"- shadow_candidate_count: {shadow.get('candidate_count')}")
    lines.append("")
    lines.append("## Top 10 Scores")
    for i, row in enumerate(top_scores[:10], 1):
        lines.append(
            f"{i}. {row.get('ticker')} score={row.get('score')} "
            f"held={row.get('held')} eligibility={row.get('eligibility_status')} "
            f"reasons={row.get('exclusion_reasons')}"
        )
    lines.append("")
    lines.append("## Production Candidates")
    if not production_candidates:
        lines.append("(none)")
    else:
        for c in production_candidates:
            lines.append(f"- {c.get('Ticker') or c.get('ticker')}: {c.get('Score') or c.get('score')}")
    lines.append("")
    lines.append("## Shadow Candidates (not used by trader)")
    if not shadow_candidates:
        lines.append("(none)")
    else:
        for c in shadow_candidates:
            lines.append(f"- {c.get('Ticker') or c.get('ticker')}: {c.get('Score') or c.get('score')}")
    lines.append("")
    lines.append("## Data Quality")
    findings = meta.get("data_quality_findings") or []
    if not findings:
        lines.append("(none)")
    else:
        for f in findings:
            lines.append(f"- [{f.get('severity')}] {f.get('code')}")
    lines.append("")
    lines.append("## Stage Durations (sec)")
    for k, v in (meta.get("stage_durations_sec") or {}).items():
        lines.append(f"- {k}: {v}")
    if previous_meta:
        lines.append("")
        lines.append("## vs Previous Run")
        lines.append(
            f"- prev candidates: {previous_meta.get('candidate_count')} → "
            f"{meta.get('candidate_count')}"
        )
        lines.append(
            f"- prev max score: {(previous_meta.get('score_distribution') or {}).get('max')} → "
            f"{(meta.get('score_distribution') or {}).get('max')}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def extract_screener_summary_lines(stdout: str, *, max_lines: int = 80) -> List[str]:
    if not stdout:
        return []
    picked: List[str] = []
    for line in stdout.splitlines():
        if any(p.search(line) for p in _SCREENER_SUMMARY_PATTERNS):
            picked.append(line)
        if len(picked) >= max_lines:
            break
    return picked


def save_subprocess_log(
    logs_dir: Path,
    *,
    script_stem: str,
    trade_date: str,
    session: str,
    market: str,
    run_id: str,
    stdout: str,
    stderr: str = "",
) -> Path:
    logs_dir = Path(logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / f"{script_stem}_{trade_date}_{session}_{market}_{run_id}.log"
    parts = [
        f"# {script_stem} subprocess log",
        f"# trade_date={trade_date} session={session} market={market} run_id={run_id}",
        "",
        "===== STDOUT =====",
        stdout or "",
        "",
        "===== STDERR =====",
        stderr or "",
        "",
    ]
    path.write_text("\n".join(parts), encoding="utf-8")
    return path
