#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Performance Review — KIS 해외주식 엔드포인트 기반 사후 분석.

원칙:
- KIS API 직접 호출 금지
- 매매/주문/계좌 조회 실행 금지
- trading_data.db, output JSON, account_snapshot, order_reconcile, logs 만 읽음
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from utils import KST, OUTPUT_DIR, load_config, norm_ticker, setup_logging

try:
    from notifier import send_discord_message, WEBHOOK_URL, is_valid_webhook
except ImportError:
    send_discord_message = None  # type: ignore
    WEBHOOK_URL = ""
    is_valid_webhook = lambda _url: False  # type: ignore

logger = logging.getLogger("performance_review")

US_EXCHANGES = ("NASD", "NYSE", "AMEX")

KIS_ENDPOINT_META = {
    "inquire-balance": {
        "path": "/uapi/overseas-stock/v1/trading/inquire-balance",
        "tr_ids": ("TTTS3012R", "VTTS3012R"),
    },
    "inquire-present-balance": {
        "path": "/uapi/overseas-stock/v1/trading/inquire-present-balance",
        "tr_ids": ("CTRP6504R", "VTRP6504R"),
    },
    "inquire-nccs": {
        "path": "/uapi/overseas-stock/v1/trading/inquire-nccs",
        "tr_ids": ("TTTS3018R", "VTTS3018R"),
    },
    "inquire-ccnl": {
        "path": "/uapi/overseas-stock/v1/trading/inquire-ccnl",
        "tr_ids": ("TTTS3035R", "VTTS3035R"),
    },
    "order": {
        "path": "/uapi/overseas-stock/v1/trading/order",
        "buy_tr_ids": ("TTTT1002U", "VTTT1002U"),
        "sell_tr_ids": ("TTTT1006U", "VTTT1006U"),
    },
}

STANDARD_FINDINGS = {
    "KIS_BALANCE_MISSING",
    "KIS_BALANCE_PARTIAL_EXCHANGE_COVERAGE",
    "KIS_BALANCE_DUPLICATE_TICKER",
    "KIS_PRESENT_BALANCE_MISSING",
    "KIS_PRESENT_BALANCE_CURRENCY_MIXED",
    "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP",
    "KIS_PRESENT_BALANCE_EVIDENCE_MISSING",
    "KIS_NCCS_MISSING",
    "KIS_NCCS_ALL_EXCHANGES_FAILED",
    "KIS_NCCS_STALE_PENDING_ORDER",
    "KIS_SELLABLE_QTY_NEGATIVE",
    "KIS_CCNL_MISSING",
    "KIS_CCNL_ALL_EXCHANGES_FAILED",
    "KIS_DB_CCNL_STATUS_MISMATCH",
    "KIS_ORDER_MISSING_ODNO",
    "KIS_ORDER_REJECTED",
    "KIS_SELL_QTY_EXCEEDED",
    "KIS_CASH_EXCEEDED",
    "KIS_EXECUTED_WITHOUT_FILL",
    "KIS_EXECUTED_FILL_UNVERIFIED",
    "KIS_FAILED_MARKED_AS_EXECUTED",
    "KIS_SELL_WITHOUT_SELLABLE_CHECK",
    "KIS_SELL_SENT_WITH_ZERO_SELLABLE_QTY",
    "ACCOUNT_SNAPSHOT_KIS_MISSING",
    "ACCOUNT_SYNC_MISMATCH",
    "ACCOUNT_SNAPSHOT_INVALID",
    "LOG_UNAVAILABLE",
}

SENSITIVE_PATTERNS = (
    re.compile(r"(appkey|app_secret|secret|authorization|access_token|token)", re.I),
    re.compile(r"(cano|acnt|account_no|account_number|계좌)", re.I),
    re.compile(r"\b\d{8,12}\b"),
)

TICKER_FIELDS = ("pdno", "ovrs_pdno", "ticker", "symbol", "code")
QTY_FIELDS = ("hldg_qty", "ovrs_cblc_qty", "qty", "quantity")
AVG_PRICE_FIELDS = ("pchs_avg_pric", "avg_price", "purchase_avg_price", "avg_unpr3")
VALUATION_FIELDS = ("ovrs_stck_evlu_amt", "evlu_amt", "valuation_amount", "holdings_value_usd", "frcr_evlu_amt2")
ORDER_ID_FIELDS = ("ODNO", "odno", "order_id", "orgn_odno")
NCCS_QTY_FIELDS = ("nccs_qty", "unfilled_qty", "remaining_qty", "ord_unpr", "ord_qty", "ft_ord_qty")
EXEC_QTY_FIELDS = ("ccld_qty", "executed_qty", "filled_qty", "tot_ccld_qty")
ORD_QTY_FIELDS = ("ord_qty", "requested_qty", "qty", "quantity")
SIDE_FIELDS = ("sll_buy_dvsn_cd", "side", "action", "ord_dvsn_name")
EXCHANGE_FIELDS = ("ovrs_excg_cd", "exchange", "excg_cd")
TR_ID_FIELDS = ("tr_id", "TR_ID", "observed_tr_id", "kis_tr_id")
STATUS_FIELDS = ("rt_cd", "msg_cd", "msg1", "error", "error_message")


def normalize_finding_title(title: str) -> str:
    if title == "KIS_CCLN_MISSING":
        return "KIS_CCNL_MISSING"
    return title


def normalize_finding_category(title: str, category: Optional[str] = None) -> str:
    if category:
        return category
    t = normalize_finding_title(title)
    if t.startswith("KIS_BALANCE_") or t.startswith("KIS_PRESENT_BALANCE_") or t == "LOG_UNAVAILABLE":
        return "DATA_QUALITY"
    if t.startswith("KIS_NCCS_") or t.startswith("KIS_CCNL_") or t.startswith("KIS_ORDER_"):
        return "OPERATIONS"
    if t.startswith("KIS_EXECUTED_") or t == "KIS_FAILED_MARKED_AS_EXECUTED":
        return "TRADE_EXECUTION"
    if t.startswith("KIS_SELL_") or t == "KIS_CASH_EXCEEDED":
        return "RISK"
    if t.startswith("ACCOUNT_SNAPSHOT_") or t == "ACCOUNT_SYNC_MISMATCH":
        return "OPERATIONS"
    return "OPERATIONS"


def make_finding(
    title: str,
    severity: str,
    endpoint: str = "",
    evidence: str = "",
    impact: str = "",
    recommendation: str = "",
    category: Optional[str] = None,
    evidence_source_file: str = "",
) -> "ReviewFinding":
    return ReviewFinding(
        title=title,
        severity=severity,
        endpoint=endpoint,
        evidence=evidence,
        impact=impact,
        recommendation=recommendation,
        category=normalize_finding_category(title, category),
        evidence_source_file=evidence_source_file,
    )


def finalize_findings(findings: List["ReviewFinding"]) -> List["ReviewFinding"]:
    for f in findings:
        if not f.category:
            f.category = normalize_finding_category(f.title)
    return findings


def _missing_evidence_severity(strict: bool, *, normal: str = "INFO") -> str:
    return "WARN" if strict else normal


def _first_present(record: Dict[str, Any], keys: Sequence[str]) -> Any:
    for k in keys:
        if k in record and record[k] not in (None, ""):
            return record[k]
    return None


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        if val is None or val == "":
            return default
        return int(round(_safe_float(val, default)))
    except (TypeError, ValueError):
        return default


def _norm_ticker(record: Dict[str, Any], market: str) -> str:
    raw = _first_present(record, TICKER_FIELDS)
    return norm_ticker(str(raw or ""), market)


def _norm_side(record: Dict[str, Any]) -> str:
    raw = str(_first_present(record, SIDE_FIELDS) or "").strip().lower()
    if raw in ("01", "1", "sell", "s"):
        return "sell"
    if raw in ("02", "2", "buy", "b"):
        return "buy"
    if raw == "sell":
        return "sell"
    if raw == "buy":
        return "buy"
    return raw


def _records_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        out: List[Dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                if "0" in item and isinstance(item["0"], dict):
                    out.append(item["0"])
                elif 0 in item and isinstance(item[0], dict):
                    out.append(item[0])
                else:
                    out.append(item)
        return out
    if isinstance(data, dict):
        return [data]
    holdings = payload.get("holdings")
    if isinstance(holdings, list):
        return [h for h in holdings if isinstance(h, dict)]
    return []


def _redact_sensitive(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if any(p.search(str(k)) for p in SENSITIVE_PATTERNS):
                out[k] = "***REDACTED***"
            else:
                out[k] = _redact_sensitive(v)
        return out
    if isinstance(obj, list):
        return [_redact_sensitive(x) for x in obj]
    if isinstance(obj, str):
        s = obj
        for p in SENSITIVE_PATTERNS:
            s = p.sub("***", s)
        return s
    return obj


@dataclass
class KisEndpointEvidence:
    endpoint_name: str = ""
    endpoint_path: str = ""
    tr_id_real: str = ""
    tr_id_paper: str = ""
    observed_tr_id: str = ""
    observed_exchange_codes: List[str] = field(default_factory=list)
    source_file: str = ""
    source_log: str = ""
    source_db_table: str = ""
    observed_at: str = ""
    status: str = "MISSING"  # OK/MISSING/FAILED/STALE/PARTIAL
    rt_cd: str = ""
    msg_cd: str = ""
    msg1: str = ""
    row_count: int = 0
    key_fields_present: List[str] = field(default_factory=list)
    missing_fields: List[str] = field(default_factory=list)
    notes: str = ""


@dataclass
class ReviewFinding:
    title: str
    severity: str
    endpoint: str = ""
    evidence: str = ""
    impact: str = ""
    recommendation: str = ""
    category: str = ""
    evidence_source_file: str = ""

    def to_dict(self) -> Dict[str, Any]:
        cat = self.category or normalize_finding_category(self.title)
        return {
            "title": normalize_finding_title(self.title),
            "severity": self.severity,
            "category": cat,
            "endpoint": self.endpoint,
            "evidence": self.evidence,
            "impact": self.impact,
            "recommendation": self.recommendation,
            "evidence_source_file": self.evidence_source_file or None,
        }


@dataclass
class KisBalanceReview:
    endpoint_name: str = "inquire-balance"
    expected_path: str = KIS_ENDPOINT_META["inquire-balance"]["path"]
    expected_tr_ids: List[str] = field(default_factory=lambda: list(KIS_ENDPOINT_META["inquire-balance"]["tr_ids"]))
    exchange_coverage: List[str] = field(default_factory=list)
    holdings_count: int = 0
    tickers: List[str] = field(default_factory=list)
    duplicate_tickers: List[str] = field(default_factory=list)
    missing_exchange_codes: List[str] = field(default_factory=list)
    total_holding_qty_by_ticker: Dict[str, int] = field(default_factory=dict)
    avg_price_by_ticker: Dict[str, float] = field(default_factory=dict)
    holdings_value_usd: float = 0.0
    balance_source_status: KisEndpointEvidence = field(default_factory=KisEndpointEvidence)
    findings: List[ReviewFinding] = field(default_factory=list)


@dataclass
class KisPresentBalanceReview:
    endpoint_name: str = "inquire-present-balance"
    expected_path: str = KIS_ENDPOINT_META["inquire-present-balance"]["path"]
    expected_tr_ids: List[str] = field(default_factory=lambda: list(KIS_ENDPOINT_META["inquire-present-balance"]["tr_ids"]))
    available_cash_usd: float = 0.0
    total_asset_usd: float = 0.0
    holdings_value_usd: float = 0.0
    cash_map: Dict[str, Any] = field(default_factory=dict)
    currency_consistency_status: str = "unknown"
    present_balance_source_status: KisEndpointEvidence = field(default_factory=KisEndpointEvidence)
    findings: List[ReviewFinding] = field(default_factory=list)


@dataclass
class KisNccsReview:
    endpoint_name: str = "inquire-nccs"
    expected_path: str = KIS_ENDPOINT_META["inquire-nccs"]["path"]
    expected_tr_ids: List[str] = field(default_factory=lambda: list(KIS_ENDPOINT_META["inquire-nccs"]["tr_ids"]))
    exchange_coverage: List[str] = field(default_factory=list)
    open_orders_count: int = 0
    open_sell_orders_count: int = 0
    pending_sell_qty_by_ticker: Dict[str, int] = field(default_factory=dict)
    stale_pending_orders: List[Dict[str, Any]] = field(default_factory=list)
    next_day_pending_orders: List[Dict[str, Any]] = field(default_factory=list)
    nccs_source_status: KisEndpointEvidence = field(default_factory=KisEndpointEvidence)
    findings: List[ReviewFinding] = field(default_factory=list)


@dataclass
class KisCcnlReview:
    endpoint_name: str = "inquire-ccnl"
    expected_path: str = KIS_ENDPOINT_META["inquire-ccnl"]["path"]
    expected_tr_ids: List[str] = field(default_factory=lambda: list(KIS_ENDPOINT_META["inquire-ccnl"]["tr_ids"]))
    exchange_coverage: List[str] = field(default_factory=list)
    ccnl_order_count: int = 0
    filled_order_count: int = 0
    partial_order_count: int = 0
    canceled_order_count: int = 0
    failed_order_count: int = 0
    order_id_coverage_rate: float = 0.0
    db_vs_ccnl_mismatch_count: int = 0
    ccnl_source_status: KisEndpointEvidence = field(default_factory=KisEndpointEvidence)
    findings: List[ReviewFinding] = field(default_factory=list)


@dataclass
class KisOrderReview:
    endpoint_name: str = "order"
    expected_path: str = KIS_ENDPOINT_META["order"]["path"]
    expected_buy_tr_ids: List[str] = field(default_factory=lambda: list(KIS_ENDPOINT_META["order"]["buy_tr_ids"]))
    expected_sell_tr_ids: List[str] = field(default_factory=lambda: list(KIS_ENDPOINT_META["order"]["sell_tr_ids"]))
    submitted_order_count: int = 0
    successful_order_count: int = 0
    rejected_order_count: int = 0
    missing_odno_count: int = 0
    sell_qty_exceeded_count: int = 0
    cash_exceeded_count: int = 0
    order_status_quality: str = "unknown"
    findings: List[ReviewFinding] = field(default_factory=list)


@dataclass
class KisEndpointReview:
    balance_review: KisBalanceReview = field(default_factory=KisBalanceReview)
    present_balance_review: KisPresentBalanceReview = field(default_factory=KisPresentBalanceReview)
    nccs_review: KisNccsReview = field(default_factory=KisNccsReview)
    ccnl_review: KisCcnlReview = field(default_factory=KisCcnlReview)
    order_review: KisOrderReview = field(default_factory=KisOrderReview)
    endpoint_findings: List[ReviewFinding] = field(default_factory=list)
    endpoint_health_score: float = 100.0


@dataclass
class PerformanceReviewResult:
    context: Dict[str, Any] = field(default_factory=dict)
    artifact_group: Dict[str, Any] = field(default_factory=dict)
    trade_performance: Dict[str, Any] = field(default_factory=dict)
    strategy_quality: Dict[str, Any] = field(default_factory=dict)
    risk_quality: Dict[str, Any] = field(default_factory=dict)
    kis_endpoint_review: KisEndpointReview = field(default_factory=KisEndpointReview)
    findings: List[ReviewFinding] = field(default_factory=list)
    summary_text: str = ""
    action_items: List[str] = field(default_factory=list)
    config_suggestions: List[str] = field(default_factory=list)


@dataclass
class ReviewArtifacts:
    market: str
    review_date: str
    period: str
    session: Optional[str]
    output_dir: Path
    db_path: Path
    balance_path: Optional[Path] = None
    summary_path: Optional[Path] = None
    daily_balance_paths: List[Path] = field(default_factory=list)
    account_snapshot_paths: List[Path] = field(default_factory=list)
    order_reconcile_paths: List[Path] = field(default_factory=list)
    log_paths: List[Path] = field(default_factory=list)
    pipeline_state_path: Optional[Path] = None
    trade_rows: List[Dict[str, Any]] = field(default_factory=list)
    logs_text: str = ""


def _default_review_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {
        "enabled": True,
        "daily_enabled": False,
        "weekly_enabled": True,
        "monthly_enabled": True,
        "strict_kis_endpoints": True,
        "send_discord": True,
        "lookback_trading_days": 20,
        "output_dir": "performance_reviews",
        "max_findings": 20,
        "critical_on_account_sync_mismatch": True,
        "critical_on_snapshot_invalid": True,
        "critical_on_sell_sent_with_zero_sellable_qty": True,
        "critical_on_failed_marked_as_executed": True,
        "warn_on_missing_kis_endpoint_evidence": True,
        "warn_on_partial_exchange_coverage": True,
    }
    user = cfg.get("performance_review") or {}
    return {**defaults, **user}


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d").replace(tzinfo=KST)


def resolve_review_dates(args: argparse.Namespace) -> Tuple[str, str, str]:
    """Return (period, start_date, end_date) as YYYYMMDD."""
    today = datetime.now(KST)
    if args.date:
        d = args.date
        return "daily", d, d
    if args.date_from and args.date_to:
        return "daily", args.date_from, args.date_to
    period = args.period or "daily"
    if period == "daily":
        d = today.strftime("%Y%m%d")
        return period, d, d
    if period == "weekly":
        end = today
        start = end - timedelta(days=6)
        return period, start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    if period == "monthly":
        start = today.replace(day=1)
        end = today
        return period, start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    d = today.strftime("%Y%m%d")
    return "daily", d, d


def _glob_latest(pattern: str, output_dir: Path) -> Optional[Path]:
    files = sorted(output_dir.glob(pattern), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return files[0] if files else None


def _glob_for_date(prefix: str, date: str, output_dir: Path) -> Optional[Path]:
    p = output_dir / f"{prefix}_{date}.json"
    return p if p.exists() else None


def load_trade_rows(db_path: Path, start: str, end: str) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    start_dt = _parse_date(start).strftime("%Y-%m-%d")
    end_dt = (_parse_date(end) + timedelta(days=1)).strftime("%Y-%m-%d")
    rows: List[Dict[str, Any]] = []
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("PRAGMA table_info(trade_records)")
            cols = {r[1] for r in cur.fetchall()}
            select_cols = [
                c for c in (
                    "timestamp", "ticker", "action", "quantity", "price", "order_id",
                    "order_status", "requested_qty", "executed_qty", "profit_loss",
                    "sell_reason", "reason_code", "structured_context",
                )
                if c in cols
            ]
            if not select_cols:
                return []
            q = (
                f"SELECT {', '.join(select_cols)} FROM trade_records "
                "WHERE timestamp >= ? AND timestamp < ? ORDER BY timestamp"
            )
            for row in conn.execute(q, (start_dt, end_dt)):
                rows.append(dict(row))
    except Exception as e:
        logger.warning("DB trade_records 로드 실패: %s", e)
    return rows


def _load_json(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.debug("JSON 로드 실패 %s: %s", path, e)
        return None


def _pick_evidence_file(
    paths: List[Path],
    *,
    exact_name: str,
    latest_name: str,
    prefix: str,
    market: str,
    trade_date: str,
) -> Optional[Path]:
    for p in paths:
        if p.name == exact_name:
            return p
    for p in paths:
        if p.name == latest_name:
            return p
    dated_matches = [
        p for p in paths
        if p.name.startswith(f"{prefix}_{market}_") and trade_date in p.name
    ]
    if dated_matches:
        return sorted(dated_matches, key=lambda x: x.stat().st_mtime, reverse=True)[0]
    return None


def load_order_reconcile_evidence(
    artifacts: ReviewArtifacts,
) -> Tuple[Optional[Dict[str, Any]], Optional[Path]]:
    market = artifacts.market
    td = artifacts.review_date
    path = _pick_evidence_file(
        artifacts.order_reconcile_paths,
        exact_name=f"order_reconcile_{market}_{td}.json",
        latest_name=f"order_reconcile_latest_{market}.json",
        prefix="order_reconcile",
        market=market,
        trade_date=td,
    )
    return _load_json(path), path


def load_account_snapshot_evidence(
    artifacts: ReviewArtifacts,
    session: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Path]]:
    market = artifacts.market
    td = artifacts.review_date
    exact = f"account_snapshot_{market}_{td}_{session}.json" if session else f"account_snapshot_{market}_{td}.json"
    path = _pick_evidence_file(
        artifacts.account_snapshot_paths,
        exact_name=exact,
        latest_name=f"account_snapshot_latest_{market}.json",
        prefix="account_snapshot",
        market=market,
        trade_date=td,
    )
    if not path:
        path = _pick_evidence_file(
            artifacts.account_snapshot_paths,
            exact_name=f"account_snapshot_{market}_{td}.json",
            latest_name=f"account_snapshot_latest_{market}.json",
            prefix="account_snapshot",
            market=market,
            trade_date=td,
        )
    return _load_json(path), path


def _endpoint_block_has_evidence(block: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(block, dict) or not block:
        return False
    status = str(block.get("status") or "").upper()
    if status in ("OK", "EMPTY", "PARTIAL"):
        return True
    if block.get("status_by_exchange"):
        return True
    if block.get("call_count") is not None:
        return True
    return False


def _ccnl_evidence_available(
    reconcile: Optional[Dict[str, Any]],
    reconcile_path: Optional[Path],
    artifacts: ReviewArtifacts,
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[Path]]:
    if reconcile and _endpoint_block_has_evidence(reconcile.get("ccnl")):
        ccnl = reconcile.get("ccnl") or {}
        if not ccnl.get("all_exchanges_failed"):
            return True, ccnl, reconcile_path
    if "ccnl" in artifacts.logs_text.lower() and "inquire_ccnl" in artifacts.logs_text.lower():
        return True, None, None
    return False, None, reconcile_path


def _nccs_evidence_available(
    reconcile: Optional[Dict[str, Any]],
    reconcile_path: Optional[Path],
    snap: Optional[Dict[str, Any]],
    snap_path: Optional[Path],
    artifacts: ReviewArtifacts,
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[Path]]:
    if reconcile and _endpoint_block_has_evidence(reconcile.get("nccs")):
        nccs = reconcile.get("nccs") or {}
        if not nccs.get("all_exchanges_failed"):
            return True, nccs, reconcile_path
    if snap:
        ep = (snap.get("endpoints") or {}).get("nccs") or snap.get("nccs")
        if _endpoint_block_has_evidence(ep):
            return True, ep, snap_path
    if "[ACCOUNT_SNAPSHOT_KIS]" in artifacts.logs_text or "inquire_nccs" in artifacts.logs_text.lower():
        return True, None, None
    return False, None, reconcile_path or snap_path


def collect_artifacts(
    market: str,
    start_date: str,
    end_date: str,
    session: Optional[str],
    output_dir: Path,
    *,
    include_logs: bool = False,
) -> ReviewArtifacts:
    db_path = output_dir / "trading_data.db"
    balance_path = _glob_for_date("balance", end_date, output_dir) or _glob_latest("balance_*.json", output_dir)
    summary_path = _glob_for_date("summary", end_date, output_dir) or _glob_latest("summary_*.json", output_dir)

    daily_balance_paths = sorted(output_dir.glob("daily_balances/balance_*_*.json"))
    account_snapshot_paths = sorted(output_dir.glob("account_snapshot_*.json"))
    order_reconcile_paths = sorted(output_dir.glob("order_reconcile_*.json"))
    pipeline_state_path = output_dir / "pipeline_state.json"

    log_paths: List[Path] = []
    logs_text = ""
    if include_logs:
        log_paths.extend(sorted(output_dir.glob("*.log")))
        logs_dir = output_dir / "logs"
        if logs_dir.is_dir():
            log_paths.extend(sorted(logs_dir.glob("*.log")))
        if log_paths:
            chunks = []
            for lp in log_paths[:20]:
                try:
                    chunks.append(lp.read_text(encoding="utf-8", errors="ignore")[-50000:])
                except Exception:
                    pass
            logs_text = "\n".join(chunks)

    trade_rows = load_trade_rows(db_path, start_date, end_date)

    return ReviewArtifacts(
        market=market,
        review_date=end_date,
        period="daily",
        session=session,
        output_dir=output_dir,
        db_path=db_path,
        balance_path=balance_path,
        summary_path=summary_path,
        daily_balance_paths=daily_balance_paths,
        account_snapshot_paths=account_snapshot_paths,
        order_reconcile_paths=order_reconcile_paths,
        log_paths=log_paths,
        pipeline_state_path=pipeline_state_path if pipeline_state_path.exists() else None,
        trade_rows=trade_rows,
        logs_text=logs_text,
    )


def _extract_tr_id(payload: Dict[str, Any]) -> str:
    for k in TR_ID_FIELDS + ("balance_endpoint", "present_balance_endpoint", "nccs_endpoint", "ccnl_endpoint"):
        v = payload.get(k)
        if v:
            return str(v)
    endpoints = payload.get("endpoints") or {}
    if isinstance(endpoints, dict):
        for v in endpoints.values():
            if v:
                return str(v)
    return ""


def _build_evidence(
    endpoint_name: str,
    payload: Optional[Dict[str, Any]],
    source_file: str = "",
    source_log: str = "",
    records: Optional[List[Dict[str, Any]]] = None,
) -> KisEndpointEvidence:
    meta = KIS_ENDPOINT_META.get(endpoint_name, {})
    tr_ids = meta.get("tr_ids") or meta.get("buy_tr_ids", ()) + meta.get("sell_tr_ids", ())
    real_id = tr_ids[0] if len(tr_ids) > 0 else ""
    paper_id = tr_ids[1] if len(tr_ids) > 1 else ""
    ev = KisEndpointEvidence(
        endpoint_name=endpoint_name,
        endpoint_path=meta.get("path", ""),
        tr_id_real=real_id,
        tr_id_paper=paper_id,
        source_file=source_file,
        source_log=source_log,
    )
    if not payload and not records:
        ev.status = "MISSING"
        return ev

    src = payload or {}
    ev.observed_tr_id = _extract_tr_id(src)
    ev.rt_cd = str(_first_present(src, STATUS_FIELDS) or src.get("status", ""))
    ev.msg_cd = str(src.get("msg_cd") or "")
    ev.msg1 = str(src.get("msg1") or src.get("status_reason") or "")
    recs = records or _records_from_payload(src)
    ev.row_count = len(recs)
    exchanges: Set[str] = set()
    for r in recs:
        exc = _first_present(r, EXCHANGE_FIELDS)
        if exc:
            exchanges.add(str(exc).upper())
    if not exchanges and isinstance(src.get("exchange_codes"), list):
        exchanges.update(str(x).upper() for x in src["exchange_codes"])
    ev.observed_exchange_codes = sorted(exchanges)
    ev.observed_at = str(src.get("snapshot_ts") or src.get("timestamp") or src.get("observed_at") or "")

    if src.get("status") == "degraded" or ev.rt_cd not in ("", "0", "ok"):
        if ev.rt_cd and ev.rt_cd not in ("0", "ok"):
            ev.status = "FAILED"
        elif src.get("status") == "degraded":
            ev.status = "FAILED"
        else:
            ev.status = "PARTIAL"
    elif ev.row_count > 0 or src.get("source") == "kis_endpoint":
        ev.status = "OK"
    elif src:
        ev.status = "PARTIAL"
    else:
        ev.status = "MISSING"
    return ev


def review_balance(artifacts: ReviewArtifacts, strict: bool) -> KisBalanceReview:
    review = KisBalanceReview()
    market = artifacts.market
    payload = _load_json(artifacts.balance_path)
    snap_payload, snap_path = load_account_snapshot_evidence(artifacts, session=artifacts.session)

    records = _records_from_payload(payload or {})
    if snap_payload:
        records = records or _records_from_payload(snap_payload)
        if snap_payload.get("holdings_summary"):
            for h in snap_payload["holdings_summary"]:
                records.append({
                    "pdno": h.get("ticker"),
                    "hldg_qty": h.get("qty"),
                    "evlu_amt": h.get("valuation_usd"),
                    "pchs_avg_pric": h.get("avg_price"),
                })

    review.balance_source_status = _build_evidence(
        "inquire-balance",
        {**(payload or {}), **(snap_payload or {})},
        source_file=str(snap_path or artifacts.balance_path or ""),
        records=records,
    )
    if snap_path:
        review.balance_source_status.source_file = str(snap_path)

    by_ticker_exc: Dict[str, Dict[str, int]] = {}
    qty_by_ticker: Dict[str, int] = {}
    avg_by_ticker: Dict[str, float] = {}
    holdings_value = 0.0
    exchanges: Set[str] = set()

    for rec in records:
        ticker = _norm_ticker(rec, market)
        if not ticker:
            continue
        qty = _safe_int(_first_present(rec, QTY_FIELDS))
        if qty <= 0:
            continue
        exc = str(_first_present(rec, EXCHANGE_FIELDS) or "UNKNOWN").upper()
        exchanges.add(exc)
        by_ticker_exc.setdefault(ticker, {})
        by_ticker_exc[ticker][exc] = by_ticker_exc[ticker].get(exc, 0) + qty
        qty_by_ticker[ticker] = qty_by_ticker.get(ticker, 0) + qty
        avg = _safe_float(_first_present(rec, AVG_PRICE_FIELDS))
        if avg > 0:
            avg_by_ticker[ticker] = avg
        val = _safe_float(_first_present(rec, VALUATION_FIELDS))
        if val <= 0 and avg > 0:
            val = avg * qty
        prpr = _safe_float(_first_present(rec, ("now_pric2", "ovrs_now_pric1", "prpr", "current_price")))
        if val <= 0 and prpr > 0:
            val = prpr * qty
        holdings_value += val

    review.exchange_coverage = sorted(exchanges - {"UNKNOWN"})
    review.missing_exchange_codes = [e for e in US_EXCHANGES if e not in review.exchange_coverage]
    review.tickers = sorted(qty_by_ticker.keys())
    review.holdings_count = len(review.tickers)
    review.total_holding_qty_by_ticker = qty_by_ticker
    review.avg_price_by_ticker = avg_by_ticker
    review.holdings_value_usd = round(holdings_value, 2)

    for ticker, exc_map in by_ticker_exc.items():
        active = {e: q for e, q in exc_map.items() if q > 0 and e != "UNKNOWN"}
        if len(active) > 1:
            if ticker not in review.duplicate_tickers:
                review.duplicate_tickers.append(ticker)
                review.balance_source_status.notes += f"; {ticker} multi-exchange (ADR possible)"
            review.findings.append(make_finding(
                "KIS_BALANCE_DUPLICATE_TICKER", "WARN", "inquire-balance",
                f"{ticker} on exchanges {list(active.keys())}",
                "다른 거래소 중복 보유", "ADR/특수케이스 또는 병합 로직 확인",
            ))
        for exc, q in active.items():
            if q > 0 and sum(1 for e2, q2 in active.items() if e2 == exc) > 1:
                if ticker not in review.duplicate_tickers:
                    review.duplicate_tickers.append(ticker)
                review.findings.append(make_finding(
                    "KIS_BALANCE_DUPLICATE_TICKER", "ERROR", "inquire-balance",
                    f"{ticker} duplicated on {exc}",
                    "동일 거래소 중복", "balance merge dedup 확인",
                ))

    if not payload and not snap_payload:
        review.findings.append(make_finding(
            "KIS_BALANCE_MISSING", "ERROR" if strict else "WARN", "inquire-balance",
            "balance/account_snapshot artifact 없음",
            "보유수량 검증 불가", "account.py 또는 trader snapshot 저장 확인",
        ))
    elif review.missing_exchange_codes and review.exchange_coverage:
        review.findings.append(make_finding(
            "KIS_BALANCE_PARTIAL_EXCHANGE_COVERAGE", "WARN", "inquire-balance",
            f"coverage={review.exchange_coverage}, missing={review.missing_exchange_codes}",
            "일부 거래소 잔고 미반영 가능", "NASD/NYSE/AMEX 전체 조회 로그 확인",
            evidence_source_file=str(snap_path or ""),
        ))
    if review.holdings_count > 0 and review.holdings_value_usd == 0:
        review.findings.append(make_finding(
            "ACCOUNT_SNAPSHOT_INVALID", "ERROR", "inquire-balance",
            f"holdings_count={review.holdings_count}, holdings_value_usd=0",
            "평가금액 검증 실패", "balance JSON evlu 필드 및 KIS 응답 확인",
        ))
    if not snap_payload:
        review.findings.append(make_finding(
            "ACCOUNT_SNAPSHOT_KIS_MISSING",
            _missing_evidence_severity(strict, normal="INFO"),
            "account_snapshot",
            "account_snapshot_*.json 없음", "KIS primary source 추적 제한", "trader snapshot 저장 활성화",
        ))
    return review


def review_present_balance(artifacts: ReviewArtifacts, balance: KisBalanceReview, strict: bool) -> KisPresentBalanceReview:
    review = KisPresentBalanceReview()
    payload = _load_json(artifacts.summary_path)
    snap_payload, snap_path = load_account_snapshot_evidence(artifacts, session=artifacts.session)

    cash_map: Dict[str, Any] = {}
    if payload:
        recs = _records_from_payload(payload)
        if recs:
            cash_map.update(recs[0])
        cash_map.update({k: v for k, v in payload.items() if k not in ("data", "comments")})
    if snap_payload:
        if isinstance(snap_payload.get("cash_map"), dict):
            cm = snap_payload["cash_map"]
            if isinstance(cm.get("USD"), dict):
                cash_map.update(cm["USD"])
            cash_map.update(cm)
        if snap_payload.get("available_cash_usd") is not None:
            cash_map.setdefault("available_cash", snap_payload["available_cash_usd"])
        if snap_payload.get("total_asset_usd") is not None:
            cash_map.setdefault("tot_evlu_amt_usd", snap_payload["total_asset_usd"])
        if snap_payload.get("holdings_value_usd") is not None:
            cash_map.setdefault("holdings_value_usd", snap_payload["holdings_value_usd"])

    review.cash_map = {k: cash_map[k] for k in cash_map if not any(p.search(str(k)) for p in SENSITIVE_PATTERNS)}
    review.available_cash_usd = _safe_float(
        cash_map.get("available_cash") or cash_map.get("ord_psbl_frcr_amt") or cash_map.get("prvs_rcdl_excc_amt")
        or (snap_payload or {}).get("available_cash_usd")
    )
    review.total_asset_usd = _safe_float(
        cash_map.get("tot_evlu_amt_usd") or cash_map.get("tot_evlu_amt")
        or (snap_payload or {}).get("total_asset_usd")
    )
    review.holdings_value_usd = balance.holdings_value_usd or _safe_float(
        cash_map.get("holdings_value_usd") or (snap_payload or {}).get("holdings_value_usd")
    )

    review.present_balance_source_status = _build_evidence(
        "inquire-present-balance",
        {**(payload or {}), **(snap_payload or {})},
        source_file=str(snap_path or artifacts.summary_path or ""),
    )
    if snap_path:
        review.present_balance_source_status.source_file = str(snap_path)

    has_krw = any(k.endswith("_krw") or "krw" in k.lower() for k in cash_map)
    has_usd = review.available_cash_usd > 0 or "USD" in str(cash_map.get("currency", ""))
    if has_krw and has_usd and review.total_asset_usd > 0 and _safe_float(cash_map.get("tot_evlu_amt_krw")) > 0:
        usd_total = review.total_asset_usd
        krw_total = _safe_float(cash_map.get("tot_evlu_amt_krw"))
        if usd_total > 0 and krw_total > 0 and abs(usd_total - krw_total) < 1:
            review.currency_consistency_status = "mixed_suspicious"
            review.findings.append(make_finding(
                "KIS_PRESENT_BALANCE_CURRENCY_MIXED", "WARN", "inquire-present-balance",
                f"USD total={usd_total}, KRW total={krw_total}",
                "통화 혼합 의심", "USD/KRW 필드 분리 저장 확인",
            ))
        else:
            review.currency_consistency_status = "mixed_ok"
    elif has_usd:
        review.currency_consistency_status = "usd"
    else:
        review.currency_consistency_status = "unknown"

    if not payload and not snap_payload:
        review.findings.append(make_finding(
            "KIS_PRESENT_BALANCE_MISSING", "ERROR" if strict else "WARN", "inquire-present-balance",
            "summary/account_snapshot 없음", "현금/총자산 검증 불가", "summary_YYYYMMDD.json 확인",
        ))

    present_ep = {}
    call_count = None
    if snap_payload:
        present_ep = (snap_payload.get("endpoints") or {}).get("present_balance") or {}
        call_count = present_ep.get("call_count")

    bal_val = review.holdings_value_usd or balance.holdings_value_usd
    total = review.total_asset_usd
    ratio = (total / bal_val) if bal_val > 0 and total > 0 else 0.0
    src = str(snap_path or "")

    if call_count == 1:
        pass
    elif call_count is not None and call_count >= 2 and ratio >= 2.0:
        sev = "CRITICAL" if ratio >= 2.5 else "ERROR"
        review.findings.append(make_finding(
            "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP", sev, "inquire-present-balance",
            f"call_count={call_count}, total_asset_usd={total}, balance_holdings_value={bal_val}, ratio={ratio:.2f}",
            "거래소 루프 중복 합산 의심", "present_balance 단일 조회 vs 거래소별 합산 확인",
            evidence_source_file=src,
        ))
    elif not present_ep and call_count is None:
        if ratio >= 2.5:
            review.findings.append(make_finding(
                "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP", "CRITICAL", "inquire-present-balance",
                f"total_asset_usd={total}, balance_holdings_value={bal_val}, ratio={ratio:.2f}",
                "총자산 과대 (evidence 부족)", "CTRP6504R call_count evidence 저장 확인",
                evidence_source_file=src,
            ))
        elif ratio >= 2.0:
            review.findings.append(make_finding(
                "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP", "ERROR", "inquire-present-balance",
                f"total/balance ratio={ratio:.2f}", "총자산 과대 가능", "present_balance evidence 확인",
                evidence_source_file=src,
            ))
        else:
            review.findings.append(make_finding(
                "KIS_PRESENT_BALANCE_EVIDENCE_MISSING",
                _missing_evidence_severity(strict),
                "inquire-present-balance",
                "present_balance endpoint evidence 없음",
                "중복 합산 판정 제한", "account_snapshot evidence 저장 확인",
                evidence_source_file=src,
            ))
    elif ratio >= 2.5:
        review.findings.append(make_finding(
            "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP", "CRITICAL", "inquire-present-balance",
            f"total_asset_usd={total}, balance_holdings_value={bal_val}, ratio={ratio:.2f}",
            "총자산 과대", "present_balance 조회 로직 확인",
            evidence_source_file=src,
        ))
    elif ratio >= 2.0:
        review.findings.append(make_finding(
            "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP", "WARN", "inquire-present-balance",
            f"total/balance ratio={ratio:.2f}", "총자산 과대 가능", "CTRP6504R 단일 호출 결과 사용 확인",
            evidence_source_file=src,
        ))

    return review


def _parse_nccs_from_sources(artifacts: ReviewArtifacts, market: str) -> Tuple[List[Dict], Dict[str, int], KisEndpointEvidence, bool]:
    orders: List[Dict[str, Any]] = []
    pending_sell: Dict[str, int] = {}
    evidence = KisEndpointEvidence(endpoint_name="inquire-nccs", endpoint_path=KIS_ENDPOINT_META["inquire-nccs"]["path"])
    has_evidence = False

    reconcile, reconcile_path = load_order_reconcile_evidence(artifacts)
    snap_payload, snap_path = load_account_snapshot_evidence(artifacts, session=artifacts.session)
    nccs_ok, nccs_block, ev_path = _nccs_evidence_available(
        reconcile, reconcile_path, snap_payload, snap_path, artifacts,
    )
    has_evidence = nccs_ok

    if reconcile and isinstance(reconcile.get("nccs"), dict):
        nb = reconcile["nccs"]
        evidence = _build_evidence("inquire-nccs", reconcile, source_file=str(reconcile_path or ""))
        evidence.observed_tr_id = ",".join(nb.get("observed_tr_ids") or [])
        evidence.observed_exchange_codes = list(nb.get("exchange_coverage") or [])
        evidence.status = str(nb.get("status") or evidence.status).upper()
        if nb.get("all_exchanges_failed"):
            evidence.status = "FAILED"
        pending_map = nb.get("pending_sell_qty_by_ticker") or {}
        if isinstance(pending_map, dict):
            for k, v in pending_map.items():
                pending_sell[str(k)] = pending_sell.get(str(k), 0) + _safe_int(v)

    for p in artifacts.account_snapshot_paths:
        payload = _load_json(p)
        if not payload:
            continue
        oo = payload.get("open_orders") or []
        if isinstance(oo, list) and oo:
            orders.extend(oo)
        pmap = payload.get("sell_pending_qty_by_ticker") or {}
        if isinstance(pmap, dict):
            for k, v in pmap.items():
                pending_sell[str(k)] = pending_sell.get(str(k), 0) + _safe_int(v)
        if not evidence.observed_tr_id:
            ep = (payload.get("endpoints") or {}).get("nccs") or {}
            evidence = _build_evidence("inquire-nccs", payload, source_file=str(p), records=oo)
            if ep.get("observed_tr_ids"):
                evidence.observed_tr_id = ",".join(ep.get("observed_tr_ids") or [])
        break

    if not orders and not pending_sell:
        for rec in artifacts.trade_rows:
            if str(rec.get("order_status", "")).lower() == "pending":
                side = str(rec.get("action", "")).lower()
                if side == "sell":
                    t = norm_ticker(rec.get("ticker", ""), market)
                    pending_sell[t] = pending_sell.get(t, 0) + _safe_int(rec.get("quantity"))

    if not pending_sell and orders:
        for o in orders:
            if _norm_side(o) == "sell":
                t = _norm_ticker(o, market)
                q = _safe_int(_first_present(o, NCCS_QTY_FIELDS))
                pending_sell[t] = pending_sell.get(t, 0) + q

    if nccs_block and nccs_block.get("open_orders_count") is not None:
        review_count = _safe_int(nccs_block.get("open_orders_count"))
        if review_count >= 0 and not orders:
            pass  # evidence says 0 open orders — valid empty state

    if ev_path and not evidence.source_file:
        evidence.source_file = str(ev_path)

    return orders, pending_sell, evidence, has_evidence


def review_nccs(artifacts: ReviewArtifacts, balance: KisBalanceReview, strict: bool) -> KisNccsReview:
    review = KisNccsReview()
    market = artifacts.market
    reconcile, reconcile_path = load_order_reconcile_evidence(artifacts)
    orders, pending_sell, evidence, has_evidence = _parse_nccs_from_sources(artifacts, market)
    review.nccs_source_status = evidence
    review.pending_sell_qty_by_ticker = pending_sell
    review.open_orders_count = len(orders)
    if reconcile and isinstance(reconcile.get("nccs"), dict):
        nb = reconcile["nccs"]
        if nb.get("open_orders_count") is not None:
            review.open_orders_count = _safe_int(nb.get("open_orders_count"))
        review.open_sell_orders_count = _safe_int(nb.get("open_sell_orders_count"))
        review.exchange_coverage = list(nb.get("exchange_coverage") or [])
    else:
        review.open_sell_orders_count = sum(1 for o in orders if _norm_side(o) == "sell")
        exchanges: Set[str] = set()
        for o in orders:
            exc = _first_present(o, EXCHANGE_FIELDS)
            if exc:
                exchanges.add(str(exc).upper())
        review.exchange_coverage = sorted(exchanges)

    nccs_block = (reconcile or {}).get("nccs") or {}
    if nccs_block.get("all_exchanges_failed"):
        review.findings.append(make_finding(
            "KIS_NCCS_ALL_EXCHANGES_FAILED", "ERROR", "inquire-nccs",
            "nccs all_exchanges_failed=true", "미체결 조회 실패", "TTTS3018R 거래소별 조회 확인",
            evidence_source_file=str(reconcile_path or evidence.source_file or ""),
        ))
    elif "모든 거래소" in artifacts.logs_text and "nccs" in artifacts.logs_text.lower():
        review.findings.append(make_finding(
            "KIS_NCCS_ALL_EXCHANGES_FAILED", "ERROR", "inquire-nccs",
            "로그에 nccs 전 거래소 실패 흔적", "미체결 조회 불가", "TTTS3018R 거래소별 조회 확인",
        ))

    nccs_status = str(nccs_block.get("status") or "").upper()
    if not has_evidence:
        review.findings.append(make_finding(
            "KIS_NCCS_MISSING",
            _missing_evidence_severity(strict),
            "inquire-nccs",
            "nccs evidence 없음", "pending 검증 제한", "order_reconcile 또는 account_snapshot 저장",
            evidence_source_file=str(reconcile_path or evidence.source_file or ""),
        ))

    for ticker, hold_qty in balance.total_holding_qty_by_ticker.items():
        pending = pending_sell.get(ticker, 0)
        if hold_qty - pending < 0:
            review.findings.append(make_finding(
                "KIS_SELLABLE_QTY_NEGATIVE", "ERROR", "inquire-nccs",
                f"{ticker}: holding={hold_qty}, pending_sell={pending}",
                "매도 가능수량 음수", "nccs/balance 동기화 확인",
            ))

    for o in orders:
        if _norm_side(o) != "sell":
            continue
        od = str(_first_present(o, ORDER_ID_FIELDS) or "")
        ts = str(o.get("order_time") or o.get("timestamp") or "")
        if ts and ts[:8] < artifacts.review_date:
            review.stale_pending_orders.append({"order_id": od, "ticker": _norm_ticker(o, market), "ts": ts})
            review.findings.append(make_finding(
                "KIS_NCCS_STALE_PENDING_ORDER", "WARN", "inquire-nccs",
                f"order_id={od}, ts={ts}", "장기 pending", "order_reconciler 재실행",
            ))

    return review


def _parse_ccnl_from_sources(
    artifacts: ReviewArtifacts,
) -> Tuple[List[Dict[str, Any]], KisEndpointEvidence, bool, Optional[Path]]:
    rows: List[Dict[str, Any]] = []
    evidence = KisEndpointEvidence(endpoint_name="inquire-ccnl", endpoint_path=KIS_ENDPOINT_META["inquire-ccnl"]["path"])
    reconcile, reconcile_path = load_order_reconcile_evidence(artifacts)
    has_evidence, ccnl_block, ev_path = _ccnl_evidence_available(reconcile, reconcile_path, artifacts)

    if reconcile and isinstance(reconcile.get("ccnl"), dict):
        cb = reconcile["ccnl"]
        evidence = _build_evidence("inquire-ccnl", reconcile, source_file=str(reconcile_path or ""))
        evidence.observed_tr_id = ",".join(cb.get("observed_tr_ids") or [])
        evidence.observed_exchange_codes = list(cb.get("exchange_coverage") or [])
        evidence.status = str(cb.get("status") or evidence.status).upper()
        if cb.get("all_exchanges_failed"):
            evidence.status = "FAILED"
        evidence.row_count = _safe_int(cb.get("order_count"))

    for p in artifacts.order_reconcile_paths:
        payload = _load_json(p)
        if not payload:
            continue
        part = payload.get("ccnl_orders") or payload.get("fills") or []
        if isinstance(part, list):
            rows.extend(part)
        if not evidence.observed_tr_id and isinstance(payload.get("ccnl"), dict):
            cb = payload["ccnl"]
            evidence.observed_tr_id = ",".join(cb.get("observed_tr_ids") or [])
            evidence.source_file = str(p)
        break

    if "ccnl" in artifacts.logs_text.lower() and "모든 거래소" in artifacts.logs_text:
        evidence.status = "FAILED"
        evidence.notes = "all exchanges failed (log)"
    if ev_path and not evidence.source_file:
        evidence.source_file = str(ev_path)
    return rows, evidence, has_evidence, reconcile_path


def review_ccnl(artifacts: ReviewArtifacts, strict: bool) -> KisCcnlReview:
    review = KisCcnlReview()
    reconcile, reconcile_path = load_order_reconcile_evidence(artifacts)
    ccnl_rows, evidence, has_evidence, ev_path = _parse_ccnl_from_sources(artifacts)
    review.ccnl_source_status = evidence
    ccnl_block = (reconcile or {}).get("ccnl") or {}

    if ccnl_block.get("order_count") is not None:
        review.ccnl_order_count = _safe_int(ccnl_block.get("order_count"))
        review.filled_order_count = _safe_int(ccnl_block.get("filled_order_count"))
        review.partial_order_count = _safe_int(ccnl_block.get("partial_order_count"))
        review.canceled_order_count = _safe_int(ccnl_block.get("canceled_order_count"))
        review.failed_order_count = _safe_int(ccnl_block.get("failed_order_count"))
        review.exchange_coverage = list(ccnl_block.get("exchange_coverage") or [])
    else:
        review.ccnl_order_count = len(ccnl_rows)
        for row in ccnl_rows:
            exec_q = _safe_int(_first_present(row, EXEC_QTY_FIELDS))
            status = str(row.get("status") or row.get("order_status") or "").lower()
            cancelled = str(row.get("cncl_yn") or row.get("cancelled") or "").upper() == "Y"
            if cancelled or status == "cancelled":
                review.canceled_order_count += 1
            elif exec_q <= 0 and status in ("failed", "rejected"):
                review.failed_order_count += 1
            elif exec_q > 0 and status == "partial":
                review.partial_order_count += 1
            elif exec_q > 0:
                review.filled_order_count += 1

    ccnl_by_id: Dict[str, Dict[str, Any]] = {}
    for row in ccnl_rows:
        oid = str(_first_present(row, ORDER_ID_FIELDS) or "")
        if oid:
            ccnl_by_id[oid] = row

    db_with_id = [r for r in artifacts.trade_rows if str(r.get("order_id") or "").strip()]
    if db_with_id:
        matched = sum(1 for r in db_with_id if str(r.get("order_id")) in ccnl_by_id)
        review.order_id_coverage_rate = round(matched / len(db_with_id), 4)
    elif reconcile and isinstance(reconcile.get("db_reconcile"), dict):
        review.order_id_coverage_rate = _safe_float(reconcile["db_reconcile"].get("order_id_coverage_rate"), 1.0)
    elif artifacts.trade_rows:
        review.order_id_coverage_rate = 0.0
    else:
        review.order_id_coverage_rate = 1.0 if not ccnl_rows else 0.0

    ccnl_status = str(ccnl_block.get("status") or "").upper()
    src = str(reconcile_path or evidence.source_file or "")

    for tr in artifacts.trade_rows:
        oid = str(tr.get("order_id") or "").strip()
        db_status = str(tr.get("order_status") or "").lower()
        db_exec = _safe_int(tr.get("executed_qty"))

        if db_status in ("executed", "filled") or db_exec > 0:
            if not has_evidence:
                review.findings.append(make_finding(
                    "KIS_EXECUTED_FILL_UNVERIFIED",
                    _missing_evidence_severity(strict),
                    "inquire-ccnl",
                    f"order_id={oid or 'N/A'}, db_status={db_status}, ccnl evidence 없음",
                    "체결 증거 미확인", "order_reconcile ccnl evidence 저장 확인",
                    evidence_source_file=src,
                ))
                continue

        if not oid:
            if db_status in ("executed", "filled") and has_evidence:
                review.findings.append(make_finding(
                    "KIS_ORDER_MISSING_ODNO", "ERROR", "inquire-ccnl",
                    f"ticker={tr.get('ticker')}, db_status={db_status}, order_id empty",
                    "체결 기록에 주문번호 없음", "주문 응답 ODNO 저장 확인",
                    evidence_source_file=src,
                ))
            continue

        if oid not in ccnl_by_id:
            if has_evidence and db_status in ("executed", "filled"):
                review.findings.append(make_finding(
                    "KIS_DB_CCNL_STATUS_MISMATCH", "WARN", "inquire-ccnl",
                    f"order_id={oid}, db={db_status}, ccnl에 없음",
                    "DB vs ccnl 불일치", "order_reconciler 재실행",
                    evidence_source_file=src,
                ))
            continue

        cc = ccnl_by_id[oid]
        exec_q = _safe_int(_first_present(cc, EXEC_QTY_FIELDS))
        cc_status = str(cc.get("status") or cc.get("order_status") or "").lower()
        if db_status == "pending" and (exec_q > 0 or cc_status in ("executed", "filled", "cancelled", "failed")):
            review.db_vs_ccnl_mismatch_count += 1
            review.findings.append(make_finding(
                "KIS_DB_CCNL_STATUS_MISMATCH", "WARN", "inquire-ccnl",
                f"order_id={oid}: db={db_status}, ccnl={cc_status}, exec={exec_q}",
                "DB pending vs KIS 체결 불일치", "order_reconciler 재분류 확인",
                evidence_source_file=src,
            ))
        if db_status in ("executed", "filled") and exec_q == 0 and has_evidence:
            review.findings.append(make_finding(
                "KIS_EXECUTED_WITHOUT_FILL", "ERROR", "inquire-ccnl",
                f"order_id={oid}, db executed but ccnl exec=0",
                "체결수량 0", "TTTS3035R 조회 및 DB 업데이트",
                evidence_source_file=src,
            ))

    if ccnl_block.get("all_exchanges_failed"):
        review.findings.append(make_finding(
            "KIS_CCNL_ALL_EXCHANGES_FAILED", "ERROR", "inquire-ccnl",
            "ccnl all_exchanges_failed=true", "체결내역 조회 실패", "거래소별 TTTS3035R 확인",
            evidence_source_file=src,
        ))
    elif evidence.status == "FAILED" or (
        "KIS_CCNL" in artifacts.logs_text and "모든 거래소" in artifacts.logs_text
    ):
        review.findings.append(make_finding(
            "KIS_CCNL_ALL_EXCHANGES_FAILED", "ERROR", "inquire-ccnl",
            "ccnl 전 거래소 실패 로그", "체결내역 조회 실패", "거래소별 TTTS3035R 확인",
            evidence_source_file=src,
        ))
    elif not has_evidence:
        review.findings.append(make_finding(
            "KIS_CCNL_MISSING",
            _missing_evidence_severity(strict),
            "inquire-ccnl",
            "ccnl evidence 없음", "체결 검증 제한", "order_reconcile 결과 저장",
            evidence_source_file=src,
        ))
    elif ccnl_status in ("OK", "EMPTY") and review.ccnl_order_count == 0:
        pass  # valid empty ccnl

    return review


def _parse_structured_context(row: Dict[str, Any]) -> Dict[str, Any]:
    ctx_raw = row.get("structured_context") or ""
    if isinstance(ctx_raw, str) and ctx_raw.strip():
        try:
            parsed = json.loads(ctx_raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    if isinstance(ctx_raw, dict):
        return ctx_raw
    return {}


def _parse_order_response_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    ctx = _parse_structured_context(row)
    for k in ("kis_response", "order_response", "raw_response", "raw"):
        if isinstance(ctx.get(k), dict):
            return ctx[k]
    if "rt_cd" in ctx:
        return ctx
    return {}


def review_orders(
    artifacts: ReviewArtifacts,
    nccs: KisNccsReview,
    balance: KisBalanceReview,
    strict: bool,
    cfg: Dict[str, Any],
) -> KisOrderReview:
    review = KisOrderReview()
    market = artifacts.market
    snap_payload, snap_path = load_account_snapshot_evidence(artifacts, session=artifacts.session)
    snap_sellable = (snap_payload or {}).get("sellable_qty_by_ticker") or {}
    sellable_checks: Set[str] = set()
    if "SELLABLE_QTY_CHECK" in artifacts.logs_text:
        for m in re.finditer(r"\[SELLABLE_QTY_CHECK\]\s+ticker=(\S+)", artifacts.logs_text):
            sellable_checks.add(m.group(1).upper())

    order_rows = [
        r for r in artifacts.trade_rows
        if str(r.get("action", "")).upper() in ("BUY", "SELL")
    ]
    review.submitted_order_count = len(order_rows)

    for row in order_rows:
        resp = _parse_order_response_from_row(row)
        row_ctx = _parse_structured_context(row)
        rt_cd = str(_first_present(resp, STATUS_FIELDS) or resp.get("rt_cd") or row_ctx.get("rt_cd") or "")
        odno = _first_present(resp, ORDER_ID_FIELDS) or row.get("order_id") or row_ctx.get("ODNO") or row_ctx.get("odno")
        msg1 = str(resp.get("msg1") or row_ctx.get("msg1") or "")
        msg_cd = str(resp.get("msg_cd") or row_ctx.get("msg_cd") or "")
        action = str(row.get("action", "")).upper()
        ticker = norm_ticker(row.get("ticker", ""), market)

        if rt_cd == "0" or (not rt_cd and odno):
            if odno and str(odno).strip():
                review.successful_order_count += 1
            else:
                review.missing_odno_count += 1
                review.findings.append(make_finding(
                    "KIS_ORDER_MISSING_ODNO", "ERROR", "order",
                    f"ticker={ticker}, rt_cd=0, odno empty",
                    "주문번호 누락", "주문 응답 ODNO 저장 확인",
                ))
        elif rt_cd and rt_cd != "0":
            review.rejected_order_count += 1
            review.findings.append(make_finding(
                "KIS_ORDER_REJECTED", "WARN", "order",
                f"ticker={ticker}, rt_cd={rt_cd}, msg1={msg1[:80]}",
                "주문 거절", "거절 사유 및 잔고 확인",
            ))

        combined = f"{msg1} {msg_cd}"
        if "주문수량이 가능수량보다" in msg1 or "가능수량" in msg1:
            review.sell_qty_exceeded_count += 1
            review.findings.append(make_finding(
                "KIS_SELL_QTY_EXCEEDED", "WARN", "order",
                combined[:120], "매도 가능수량 초과", "sellable_qty clamp 확인",
            ))
        if "주문가능금액" in msg1 or "cash exceeded" in combined.lower():
            review.cash_exceeded_count += 1
            review.findings.append(make_finding(
                "KIS_CASH_EXCEEDED", "WARN", "order",
                combined[:120], "주문가능금액 초과", "available_cash 확인",
            ))

        db_status = str(row.get("order_status") or "").lower()
        if rt_cd and rt_cd != "0" and db_status in ("executed", "filled"):
            sev = "CRITICAL" if cfg.get("critical_on_failed_marked_as_executed", True) else "ERROR"
            review.findings.append(make_finding(
                "KIS_FAILED_MARKED_AS_EXECUTED", sev, "order",
                f"order_id={odno}, rt_cd={rt_cd}, db_status={db_status}",
                "실패 주문이 체결로 기록", "DB order_status 정정",
            ))

        if action == "SELL" and rt_cd in ("0", "") and odno:
            hold = balance.total_holding_qty_by_ticker.get(ticker, 0)
            pending = nccs.pending_sell_qty_by_ticker.get(ticker, 0)
            sellable = max(0, hold - pending)
            ctx_sellable = _safe_int(row_ctx.get("sellable_qty"))
            ctx_checked = bool(row_ctx.get("sellable_qty_checked"))
            if row_ctx.get("clamp_action") == "skipped_zero_sellable" or (
                ctx_checked and ctx_sellable == 0 and _safe_int(row.get("quantity")) > 0
            ):
                sev = "CRITICAL" if cfg.get("critical_on_sell_sent_with_zero_sellable_qty", True) else "ERROR"
                review.findings.append(make_finding(
                    "KIS_SELL_SENT_WITH_ZERO_SELLABLE_QTY", sev, "order",
                    f"ticker={ticker}, sellable=0, sell submitted",
                    "0 sellable 매도 전송", "SELL_SKIP_NO_SELLABLE_QTY 가드 확인",
                    evidence_source_file=str(snap_path or ""),
                ))
            elif sellable == 0 and hold > 0 and pending >= hold:
                sev = "CRITICAL" if cfg.get("critical_on_sell_sent_with_zero_sellable_qty", True) else "ERROR"
                review.findings.append(make_finding(
                    "KIS_SELL_SENT_WITH_ZERO_SELLABLE_QTY", sev, "order",
                    f"ticker={ticker}, sellable=0, sell submitted",
                    "0 sellable 매도 전송", "SELL_SKIP_NO_SELLABLE_QTY 가드 확인",
                    evidence_source_file=str(snap_path or ""),
                ))
            elif (
                ticker.upper() not in sellable_checks
                and not ctx_checked
                and ticker not in snap_sellable
            ):
                review.findings.append(make_finding(
                    "KIS_SELL_WITHOUT_SELLABLE_CHECK",
                    _missing_evidence_severity(strict, normal="WARN"),
                    "order",
                    f"ticker={ticker}, no SELLABLE_QTY_CHECK log or structured_context",
                    "매도 전 sellable 검증 evidence 부족", "trader _clamp_sell_qty evidence 저장 확인",
                    evidence_source_file=str(snap_path or ""),
                ))

    if "executed_sells=True" in artifacts.logs_text and review.rejected_order_count > 0:
        review.findings.append(make_finding(
            "KIS_FAILED_MARKED_AS_EXECUTED", "CRITICAL", "order",
            "log executed_sells=True with rejected orders",
            "실패인데 executed_sells 처리", "trader run_sell_logic 반환값 확인",
        ))

    if review.submitted_order_count == 0:
        review.order_status_quality = "no_orders"
    elif review.missing_odno_count == 0 and review.rejected_order_count == 0:
        review.order_status_quality = "good"
    elif review.missing_odno_count > 0:
        review.order_status_quality = "missing_odno"
    else:
        review.order_status_quality = "mixed"
    return review


def _compute_health_score(findings: List[ReviewFinding]) -> float:
    score = 100.0
    penalties = {"CRITICAL": 25, "ERROR": 15, "WARN": 5, "INFO": 1}
    for f in findings:
        score -= penalties.get(f.severity, 3)
    return max(0.0, min(100.0, score))


def build_kis_endpoint_review(
    artifacts: ReviewArtifacts,
    strict: bool,
    cfg: Dict[str, Any],
    *,
    include_logs: bool = False,
) -> KisEndpointReview:
    ker = KisEndpointReview()
    ker.balance_review = review_balance(artifacts, strict)
    ker.present_balance_review = review_present_balance(artifacts, ker.balance_review, strict)
    ker.nccs_review = review_nccs(artifacts, ker.balance_review, strict)
    ker.ccnl_review = review_ccnl(artifacts, strict)
    ker.order_review = review_orders(artifacts, ker.nccs_review, ker.balance_review, strict, cfg)

    all_findings: List[ReviewFinding] = []
    for part in (
        ker.balance_review,
        ker.present_balance_review,
        ker.nccs_review,
        ker.ccnl_review,
        ker.order_review,
    ):
        all_findings.extend(getattr(part, "findings", []))

    if include_logs and not artifacts.log_paths:
        all_findings.append(make_finding(
            "LOG_UNAVAILABLE", "INFO", "",
            "output logs 없음", "로그 기반 교차검증 제한", "pipeline 로그 저장 확인",
        ))

    if strict:
        for name, rev in (
            ("inquire-balance", ker.balance_review.balance_source_status),
            ("inquire-present-balance", ker.present_balance_review.present_balance_source_status),
            ("inquire-nccs", ker.nccs_review.nccs_source_status),
            ("inquire-ccnl", ker.ccnl_review.ccnl_source_status),
        ):
            if rev.status in ("MISSING", "FAILED"):
                title = f"KIS_{name.upper().replace('-', '_')}_MISSING".replace("INQUIRE_", "")
                if name == "inquire-balance":
                    title = "KIS_BALANCE_MISSING"
                elif name == "inquire-present-balance":
                    title = "KIS_PRESENT_BALANCE_MISSING"
                elif name == "inquire-nccs":
                    title = "KIS_NCCS_MISSING"
                elif name == "inquire-ccnl":
                    title = "KIS_CCNL_MISSING"
                if not any(f.title == title for f in all_findings):
                    all_findings.append(make_finding(
                        title, "WARN" if cfg.get("warn_on_missing_kis_endpoint_evidence") else "ERROR",
                        name, f"status={rev.status}", "endpoint evidence 불완전", "KIS artifact/metadata 저장",
                        evidence_source_file=rev.source_file,
                    ))

    ker.endpoint_findings = finalize_findings(all_findings)
    ker.endpoint_health_score = _compute_health_score(all_findings)
    return ker


def _summarize_trades(trade_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    sells = [r for r in trade_rows if str(r.get("action", "")).upper() == "SELL"]
    wins = [r for r in sells if _safe_float(r.get("profit_loss")) > 0]
    return {
        "trade_count": len(trade_rows),
        "sell_count": len(sells),
        "win_rate": round(len(wins) / len(sells), 4) if sells else 0.0,
        "net_pnl": round(sum(_safe_float(r.get("profit_loss")) for r in sells), 2),
    }


def run_performance_review(
    market: str,
    start_date: str,
    end_date: str,
    *,
    period: str = "daily",
    session: Optional[str] = None,
    strict_kis: bool = False,
    include_logs: bool = False,
    send_discord: bool = True,
    json_only: bool = False,
    output_dir: Optional[Path] = None,
    review_cfg: Optional[Dict[str, Any]] = None,
) -> PerformanceReviewResult:
    setup_logging()
    cfg_root = load_config()
    cfg = review_cfg or _default_review_config(cfg_root)
    out_base = output_dir or OUTPUT_DIR
    review_out = out_base / cfg.get("output_dir", "performance_reviews")
    review_out.mkdir(parents=True, exist_ok=True)

    review_error: Optional[str] = None
    artifacts: Optional[ReviewArtifacts] = None
    kis_review = KisEndpointReview()
    trade_perf: Dict[str, Any] = {"trade_count": 0, "sell_count": 0, "win_rate": 0.0, "net_pnl": 0.0}
    all_findings: List[ReviewFinding] = []

    try:
        artifacts = collect_artifacts(
            market, start_date, end_date, session, out_base, include_logs=include_logs,
        )
        artifacts.period = period
        kis_review = build_kis_endpoint_review(
            artifacts, strict_kis, cfg, include_logs=include_logs,
        )
        max_findings = int(cfg.get("max_findings", 20))
        all_findings = kis_review.endpoint_findings[:max_findings]
        trade_perf = _summarize_trades(artifacts.trade_rows)
    except Exception as e:
        review_error = str(e)
        logger.exception("Performance review analysis failed: %s", e)
        all_findings.append(make_finding(
            "ACCOUNT_SNAPSHOT_INVALID",
            "ERROR",
            "",
            f"review pipeline error: {review_error[:200]}",
            "리뷰 분석 중단",
            "artifact/DB 상태 확인 후 재실행",
        ))
        kis_review.endpoint_findings = all_findings
        kis_review.endpoint_health_score = _compute_health_score(all_findings)

    result = PerformanceReviewResult(
        context={
            "market": market,
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "session": session,
            "strict_kis_endpoints": strict_kis,
            "include_logs": include_logs,
            "generated_at": datetime.now(KST).isoformat(),
            "review_error": review_error,
        },
        artifact_group={
            "balance_file": str(artifacts.balance_path) if artifacts and artifacts.balance_path else None,
            "summary_file": str(artifacts.summary_path) if artifacts and artifacts.summary_path else None,
            "account_snapshots": [str(p) for p in artifacts.account_snapshot_paths] if artifacts else [],
            "order_reconciles": [str(p) for p in artifacts.order_reconcile_paths] if artifacts else [],
            "log_count": len(artifacts.log_paths) if artifacts else 0,
            "db_path": str(artifacts.db_path) if artifacts else str(out_base / "trading_data.db"),
            "include_logs": include_logs,
        },
        trade_performance=trade_perf,
        strategy_quality={"note": "artifact-based post-hoc review"},
        risk_quality={"note": "artifact-based post-hoc review"},
        kis_endpoint_review=kis_review,
        findings=all_findings,
    )

    critical = [f for f in all_findings if f.severity == "CRITICAL"]
    errors = [f for f in all_findings if f.severity == "ERROR"]
    result.summary_text = (
        f"KIS endpoint health={kis_review.endpoint_health_score:.0f}/100; "
        f"findings={len(all_findings)} (CRITICAL={len(critical)}, ERROR={len(errors)})"
    )
    if review_error:
        result.summary_text += f"; error={review_error[:120]}"
    result.action_items = [f"[{f.severity}] {f.title}: {f.recommendation}" for f in critical + errors][:10]

    try:
        write_reports(result, review_out, market, period, end_date, json_only=json_only)
    except Exception as e:
        logger.exception("Performance review report write failed: %s", e)

    _send_discord_summary(result, cfg, send_discord=send_discord)
    return result


def kis_review_to_json_summary(ker: KisEndpointReview) -> Dict[str, Any]:
    br = ker.balance_review
    pr = ker.present_balance_review
    nr = ker.nccs_review
    cr = ker.ccnl_review
    orr = ker.order_review
    return _redact_sensitive({
        "endpoint_health_score": ker.endpoint_health_score,
        "balance": {
            "status": br.balance_source_status.status,
            "observed_tr_ids": [br.balance_source_status.observed_tr_id] if br.balance_source_status.observed_tr_id else [],
            "exchange_coverage": br.exchange_coverage,
            "holdings_count": br.holdings_count,
            "tickers": br.tickers,
            "duplicate_tickers": br.duplicate_tickers,
            "holdings_value_usd": br.holdings_value_usd,
            "evidence_source_file": br.balance_source_status.source_file or None,
        },
        "present_balance": {
            "status": pr.present_balance_source_status.status,
            "observed_tr_ids": [pr.present_balance_source_status.observed_tr_id] if pr.present_balance_source_status.observed_tr_id else [],
            "available_cash_usd": pr.available_cash_usd,
            "total_asset_usd": pr.total_asset_usd,
            "holdings_value_usd": pr.holdings_value_usd,
            "currency_consistency_status": pr.currency_consistency_status,
            "evidence_source_file": pr.present_balance_source_status.source_file or None,
        },
        "nccs": {
            "status": nr.nccs_source_status.status,
            "observed_tr_ids": [nr.nccs_source_status.observed_tr_id] if nr.nccs_source_status.observed_tr_id else [],
            "exchange_coverage": nr.exchange_coverage,
            "open_orders_count": nr.open_orders_count,
            "pending_sell_qty_by_ticker": nr.pending_sell_qty_by_ticker,
            "stale_pending_orders_count": len(nr.stale_pending_orders),
            "evidence_source_file": nr.nccs_source_status.source_file or None,
        },
        "ccnl": {
            "status": cr.ccnl_source_status.status,
            "observed_tr_ids": [cr.ccnl_source_status.observed_tr_id] if cr.ccnl_source_status.observed_tr_id else [],
            "exchange_coverage": cr.exchange_coverage,
            "order_id_coverage_rate": cr.order_id_coverage_rate,
            "db_vs_ccnl_mismatch_count": cr.db_vs_ccnl_mismatch_count,
            "evidence_source_file": cr.ccnl_source_status.source_file or None,
        },
        "order": {
            "submitted_order_count": orr.submitted_order_count,
            "successful_order_count": orr.successful_order_count,
            "rejected_order_count": orr.rejected_order_count,
            "missing_odno_count": orr.missing_odno_count,
            "sell_qty_exceeded_count": orr.sell_qty_exceeded_count,
            "cash_exceeded_count": orr.cash_exceeded_count,
        },
        "findings": [f.to_dict() for f in ker.endpoint_findings],
    })


def result_to_dict(result: PerformanceReviewResult) -> Dict[str, Any]:
    return _redact_sensitive({
        "context": result.context,
        "artifact_group": result.artifact_group,
        "trade_performance": result.trade_performance,
        "strategy_quality": result.strategy_quality,
        "risk_quality": result.risk_quality,
        "kis_endpoint_review": kis_review_to_json_summary(result.kis_endpoint_review),
        "findings": [f.to_dict() for f in result.findings],
        "summary_text": result.summary_text,
        "action_items": result.action_items,
        "config_suggestions": result.config_suggestions,
    })


def render_markdown(result: PerformanceReviewResult) -> str:
    ker = result.kis_endpoint_review
    br, pr, nr, cr, orr = ker.balance_review, ker.present_balance_review, ker.nccs_review, ker.ccnl_review, ker.order_review
    lines = [
        f"# Performance Review — {result.context.get('market')} ({result.context.get('period')})",
        "",
        f"**Date range:** {result.context.get('start_date')} — {result.context.get('end_date')}",
        "",
        f"**Summary:** {result.summary_text}",
        "",
        "## Trade Performance",
        f"- Trades: {result.trade_performance.get('trade_count', 0)}",
        f"- Sells: {result.trade_performance.get('sell_count', 0)}",
        f"- Win rate: {result.trade_performance.get('win_rate', 0)}",
        f"- Net PnL: {result.trade_performance.get('net_pnl', 0)}",
        "",
        "## KIS Endpoint Review",
        "",
        "### Balance - inquire-balance",
        f"- status: {br.balance_source_status.status}",
        f"- TR_ID 관측값: {br.balance_source_status.observed_tr_id or 'N/A'}",
        f"- 거래소 coverage: {', '.join(br.exchange_coverage) or 'none'}",
        f"- evidence_source_file: {br.balance_source_status.source_file or 'N/A'}",
        f"- 보유종목 수: {br.holdings_count}",
        f"- 중복 ticker: {', '.join(br.duplicate_tickers) or 'none'}",
        f"- 평가금액 상태: ${br.holdings_value_usd:,.2f} ({br.balance_source_status.status})",
        "",
        "### Present Balance - inquire-present-balance",
        f"- status: {pr.present_balance_source_status.status}",
        f"- USD 현금: ${pr.available_cash_usd:,.2f}",
        f"- USD 총자산: ${pr.total_asset_usd:,.2f}",
        f"- 통화 일관성: {pr.currency_consistency_status}",
        f"- evidence_source_file: {pr.present_balance_source_status.source_file or 'N/A'}",
        f"- 거래소 루프 중복 합산 의심: "
        + ("yes" if any(f.title == "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP" for f in pr.findings) else "no"),
        "",
        "### Open Orders - inquire-nccs",
        f"- status: {nr.nccs_source_status.status}",
        f"- 미체결 주문 수: {nr.open_orders_count}",
        f"- 미체결 매도수량: {nr.pending_sell_qty_by_ticker}",
        f"- evidence_source_file: {nr.nccs_source_status.source_file or 'N/A'}",
        f"- sellable_qty 검증: see findings",
        f"- 장기 pending 주문: {len(nr.stale_pending_orders)}",
        "",
        "### Order Fills - inquire-ccnl",
        f"- status: {cr.ccnl_source_status.status}",
        f"- 주문번호 coverage: {cr.order_id_coverage_rate:.1%}",
        f"- evidence_source_file: {cr.ccnl_source_status.source_file or 'N/A'}",
        f"- DB와 KIS 체결상태 비교 mismatch: {cr.db_vs_ccnl_mismatch_count}",
        f"- filled/partial/canceled/failed: {cr.filled_order_count}/{cr.partial_order_count}/"
        f"{cr.canceled_order_count}/{cr.failed_order_count}",
        "",
        "### Order Submit - order",
        f"- status: {orr.order_status_quality}",
        f"- 주문 성공률: {orr.successful_order_count}/{orr.submitted_order_count}",
        f"- rt_cd/ODNO 품질: {orr.order_status_quality}",
        f"- missing ODNO: {orr.missing_odno_count}",
        f"- 가능수량 초과: {orr.sell_qty_exceeded_count}",
        f"- 주문가능금액 초과: {orr.cash_exceeded_count}",
        "",
        "## Findings",
        "",
        "| severity | category | title | evidence | impact | recommendation | evidence_source |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for f in result.findings:
        cat = f.category or normalize_finding_category(f.title)
        src = f.evidence_source_file or ""
        lines.append(
            f"| {f.severity} | {cat} | {normalize_finding_title(f.title)} | "
            f"{f.evidence[:80]} | {f.impact[:60]} | {f.recommendation[:60]} | {src[:60]} |"
        )
    lines.extend(["", "### KIS Endpoint Findings", ""])
    for f in result.findings:
        cat = f.category or normalize_finding_category(f.title)
        lines.append(
            f"- **{f.severity}** [{cat}] {normalize_finding_title(f.title)}: {f.evidence[:120]}"
        )
    if result.action_items:
        lines.extend(["", "## Action Items", ""])
        for item in result.action_items:
            lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def write_reports(
    result: PerformanceReviewResult,
    review_out: Path,
    market: str,
    period: str,
    date_tag: str,
    json_only: bool = False,
) -> Tuple[Path, Optional[Path]]:
    base = f"performance_review_{market}_{period}_{date_tag}"
    json_path = review_out / f"{base}.json"
    md_path = review_out / f"{base}.md"
    latest_json = review_out / f"latest_{market}_{period}.json"
    latest_md = review_out / f"latest_{market}_{period}.md"

    payload = result_to_dict(result)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    if not json_only:
        md_text = render_markdown(result)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_text)
        with open(latest_md, "w", encoding="utf-8") as f:
            f.write(md_text)
    logger.info("Performance review saved: %s", json_path)
    return json_path, md_path if not json_only else None


def _send_discord_summary(
    result: PerformanceReviewResult,
    cfg: Dict[str, Any],
    *,
    send_discord: bool = True,
) -> None:
    """CRITICAL/ERROR finding이 있을 때만 10줄 이내 요약 전송 (Markdown 전문 금지)."""
    if not send_discord or not cfg.get("send_discord", True):
        return
    if send_discord_message is None:
        return
    try:
        if not (WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL)):
            return
        urgent = [
            f for f in result.findings
            if f.severity in ("CRITICAL", "ERROR")
        ]
        if not urgent:
            return
        lines = [
            f"Performance Review {result.context.get('market')} "
            f"({result.context.get('start_date')}–{result.context.get('end_date')})",
            result.summary_text,
        ]
        for f in urgent:
            if len(lines) >= 10:
                break
            lines.append(
                f"[{f.severity}] {normalize_finding_title(f.title)} @ {f.endpoint or 'kis'}: "
                f"{f.evidence[:80]}"
            )
        description = "\n".join(lines[:10])
        color = 0xE74C3C if any(f.severity == "CRITICAL" for f in urgent) else 0xE67E22
        embed = {
            "type": "rich",
            "title": f"⚠️ KIS Performance Review — {result.context.get('market')}",
            "description": description[:1900],
            "color": color,
            "timestamp": datetime.now(KST).isoformat(),
        }
        send_discord_message(embeds=[embed])
    except Exception as e:
        logger.debug("Discord 전송 실패: %s", e)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="KIS endpoint 기반 performance review (사후 분석)")
    p.add_argument("--market", default=os.getenv("MARKET", "SP500"), help="Market code (default: MARKET env or SP500)")
    p.add_argument("--date", dest="date", metavar="YYYYMMDD", help="Single review date")
    p.add_argument(
        "--from", "--date-from",
        dest="date_from",
        metavar="YYYYMMDD",
        help="Date range start",
    )
    p.add_argument(
        "--to", "--date-to",
        dest="date_to",
        metavar="YYYYMMDD",
        help="Date range end",
    )
    p.add_argument(
        "--period",
        choices=("daily", "weekly", "monthly"),
        help="Review period bucket (default: daily or inferred from --date)",
    )
    p.add_argument("--session", choices=("am", "pm"), help="Pipeline session filter (optional)")
    p.add_argument(
        "--strict-kis-endpoints",
        dest="strict_kis_endpoints",
        action="store_true",
        help="WARN/ERROR when KIS endpoint evidence missing or incomplete",
    )
    p.add_argument("--no-discord", action="store_true", help="Skip Discord notification")
    p.add_argument("--json-only", action="store_true", help="Write JSON only (skip Markdown)")
    p.add_argument(
        "--include-logs",
        action="store_true",
        help="Include output/logs/*.log and output/*.log in review",
    )
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint: python -m performance_review …"""
    exit_code = 0
    try:
        args = build_arg_parser().parse_args(argv)
        period, start, end = resolve_review_dates(args)
        if args.period:
            period = args.period

        cfg_root = load_config()
        cfg = _default_review_config(cfg_root)
        if args.strict_kis_endpoints:
            cfg["strict_kis_endpoints"] = True
        strict = bool(cfg.get("strict_kis_endpoints", False))

        run_performance_review(
            market=args.market,
            start_date=start,
            end_date=end,
            period=period,
            session=args.session,
            strict_kis=strict,
            include_logs=args.include_logs,
            send_discord=not args.no_discord,
            json_only=args.json_only,
            review_cfg=cfg,
        )
    except Exception as e:
        logger.exception("performance_review main failed: %s", e)
        exit_code = 1
        try:
            market = os.getenv("MARKET", "SP500")
            today = datetime.now(KST).strftime("%Y%m%d")
            run_performance_review(
                market=market,
                start_date=today,
                end_date=today,
                period="daily",
                send_discord=False,
                json_only=False,
                strict_kis=False,
            )
        except Exception as fallback_err:
            logger.error("Fallback report generation failed: %s", fallback_err)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
