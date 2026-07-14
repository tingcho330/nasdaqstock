# -*- coding: utf-8 -*-
"""Common durable broker order journal + trade_records persist path.

Order of operations (callers must follow):
1. correlation_id + journal(created)
2. KIS order submit
3. journal broker_accepted (rt_cd / order_id)
4. idempotent trade_records upsert
5. journal db_persisted (or persist_failed / reconcile_required)
6. Only then strategy flags (partial_sell / cooldown)
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from utils import KST, OUTPUT_DIR, norm_ticker

logger = logging.getLogger(__name__)

ORDER_JOURNAL_DIR = OUTPUT_DIR / "order_journal"
PERSIST_LOCK_DIR = OUTPUT_DIR / "order_persist_locks"

JOURNAL_CREATED = "created"
JOURNAL_BROKER_ACCEPTED = "broker_accepted"
JOURNAL_DB_PERSISTED = "db_persisted"
JOURNAL_RECONCILE_REQUIRED = "reconcile_required"
JOURNAL_RECONCILED = "reconciled"
JOURNAL_PERSIST_FAILED = "persist_failed"


def new_correlation_id() -> str:
    return datetime.now(KST).strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:12]


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.stem + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def journal_path(correlation_id: str) -> Path:
    return ORDER_JOURNAL_DIR / f"order_{correlation_id}.json"


def lock_path(ticker: str, side: str, order_id: str = "") -> Path:
    safe_t = norm_ticker(ticker, os.getenv("MARKET", "SP500")) or "UNKNOWN"
    safe_s = str(side or "").upper() or "UNK"
    oid = str(order_id or "NOOID").strip() or "NOOID"
    return PERSIST_LOCK_DIR / f"{safe_t}_{safe_s}_{oid}.lock"


def write_order_journal(
    correlation_id: str,
    *,
    status: str,
    market: str,
    ticker: str,
    side: str,
    requested_qty: int,
    requested_price: float,
    order_type: str = "",
    strategy_type: str = "",
    reason_code: str = "",
    structured_context: Optional[Dict[str, Any]] = None,
    broker_order_id: str = "",
    broker_response: Optional[Dict[str, Any]] = None,
    db_persisted: bool = False,
    recovery_required: bool = False,
    extra: Optional[Dict[str, Any]] = None,
) -> Path:
    """Create or overwrite journal for correlation_id (atomic)."""
    path = journal_path(correlation_id)
    existing: Dict[str, Any] = {}
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f) or {}
        except Exception:
            existing = {}

    payload = {
        **existing,
        "correlation_id": correlation_id,
        "created_at_kst": existing.get("created_at_kst") or datetime.now(KST).isoformat(),
        "updated_at_kst": datetime.now(KST).isoformat(),
        "status": status,
        "market": market,
        "ticker": norm_ticker(ticker, market),
        "side": str(side or "").upper(),
        "requested_qty": int(requested_qty or 0),
        "requested_price": requested_price,
        "order_type": order_type,
        "strategy_type": strategy_type,
        "reason_code": reason_code,
        "structured_context": structured_context or existing.get("structured_context") or {},
        "broker_order_id": broker_order_id or existing.get("broker_order_id") or "",
        "broker_response": broker_response if broker_response is not None else existing.get("broker_response"),
        "db_persisted": bool(db_persisted),
        "recovery_required": bool(recovery_required),
    }
    if extra:
        payload.update(extra)
    _atomic_write_json(path, payload)
    return path


def update_order_journal(correlation_id: str, **fields: Any) -> Path:
    path = journal_path(correlation_id)
    existing: Dict[str, Any] = {}
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f) or {}
        except Exception:
            existing = {}
    existing.update({k: v for k, v in fields.items() if v is not None})
    existing["correlation_id"] = correlation_id
    existing["updated_at_kst"] = datetime.now(KST).isoformat()
    if "created_at_kst" not in existing:
        existing["created_at_kst"] = existing["updated_at_kst"]
    _atomic_write_json(path, existing)
    return path


def create_persist_failure_lock(
    *,
    ticker: str,
    side: str,
    order_id: str,
    correlation_id: str,
    reason: str,
) -> Path:
    path = lock_path(ticker, side, order_id)
    payload = {
        "ticker": norm_ticker(ticker),
        "side": str(side or "").upper(),
        "order_id": str(order_id or "").strip(),
        "correlation_id": correlation_id,
        "reason": reason,
        "created_at_kst": datetime.now(KST).isoformat(),
        "blocks_additional_orders": True,
    }
    _atomic_write_json(path, payload)
    logger.critical(
        "[%s] ticker=%s side=%s order_id=%s correlation_id=%s lock=%s",
        reason,
        ticker,
        side,
        order_id,
        correlation_id,
        path,
    )
    return path


def has_persist_failure_lock(ticker: str, side: str = "", order_id: str = "") -> bool:
    """True if any matching operational lock blocks further orders."""
    PERSIST_LOCK_DIR.mkdir(parents=True, exist_ok=True)
    t = norm_ticker(ticker)
    s = str(side or "").upper()
    oid = str(order_id or "").strip()
    for p in PERSIST_LOCK_DIR.glob("*.lock"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            continue
        if norm_ticker(data.get("ticker", "")) != t:
            continue
        if s and str(data.get("side", "")).upper() != s:
            continue
        if oid and str(data.get("order_id", "")).strip() != oid:
            continue
        if data.get("blocks_additional_orders", True):
            return True
    return False


def clear_persist_failure_lock(ticker: str, side: str = "", order_id: str = "") -> int:
    cleared = 0
    if not PERSIST_LOCK_DIR.exists():
        return 0
    t = norm_ticker(ticker)
    s = str(side or "").upper()
    oid = str(order_id or "").strip()
    for p in list(PERSIST_LOCK_DIR.glob("*.lock")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            continue
        if norm_ticker(data.get("ticker", "")) != t:
            continue
        if s and str(data.get("side", "")).upper() != s:
            continue
        if oid and str(data.get("order_id", "")).strip() != oid:
            continue
        try:
            p.unlink()
            cleared += 1
        except OSError:
            pass
    return cleared


def persist_broker_order_to_db(
    trade_payload: Dict[str, Any],
    *,
    correlation_id: str,
    market: str = "",
    strategy_type: str = "",
    reason_code: str = "",
) -> Tuple[bool, str]:
    """
    Idempotent upsert into trade_records after broker accept.
    Returns (ok, detail). On failure writes persist_failed journal + lock.
    Does NOT swallow exceptions from caller viewpoint — returns False.
    """
    from recorder import record_trade

    ticker = str(trade_payload.get("ticker") or "")
    side = str(trade_payload.get("side") or "").upper()
    order_id = str(
        trade_payload.get("order_id")
        or trade_payload.get("odno")
        or ""
    ).strip()
    mkt = market or os.getenv("MARKET", "SP500")

    try:
        ok = bool(record_trade(trade_payload))
    except Exception as e:
        ok = False
        detail = f"record_trade_exception:{e}"
        logger.exception(
            "DB persist exception correlation_id=%s ticker=%s order_id=%s: %s",
            correlation_id,
            ticker,
            order_id,
            e,
        )
    else:
        detail = "ok" if ok else "record_trade_returned_false"

    if ok:
        update_order_journal(
            correlation_id,
            status=JOURNAL_DB_PERSISTED,
            db_persisted=True,
            recovery_required=False,
            broker_order_id=order_id,
        )
        if order_id:
            clear_persist_failure_lock(ticker, side, order_id)
        return True, detail

    finding = "SELL_DB_PERSIST_FAILED" if side == "SELL" else "BUY_DB_PERSIST_FAILED"
    update_order_journal(
        correlation_id,
        status=JOURNAL_PERSIST_FAILED,
        db_persisted=False,
        recovery_required=True,
        broker_order_id=order_id,
        extra={
            "finding": finding,
            "persist_error": detail,
            "reconcile_status": JOURNAL_RECONCILE_REQUIRED,
        },
    )
    # Also mark reconcile_required via status preference after persist_failed
    update_order_journal(
        correlation_id,
        status=JOURNAL_RECONCILE_REQUIRED,
        recovery_required=True,
    )
    create_persist_failure_lock(
        ticker=ticker,
        side=side,
        order_id=order_id or "NOOID",
        correlation_id=correlation_id,
        reason=finding,
    )
    logger.critical(
        "[%s] CRITICAL correlation_id=%s ticker=%s side=%s order_id=%s detail=%s",
        finding,
        correlation_id,
        ticker,
        side,
        order_id,
        detail,
    )
    return False, detail


def begin_broker_order(
    *,
    market: str,
    ticker: str,
    side: str,
    requested_qty: int,
    requested_price: float,
    order_type: str = "",
    strategy_type: str = "",
    reason_code: str = "",
    structured_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Allocate correlation_id and write created journal. Raises if persist lock blocks."""
    if has_persist_failure_lock(ticker, side):
        raise RuntimeError(
            f"ORDER_JOURNAL_RECOVERY_REQUIRED: persist lock blocks {side} {ticker}"
        )
    cid = new_correlation_id()
    write_order_journal(
        cid,
        status=JOURNAL_CREATED,
        market=market,
        ticker=ticker,
        side=side,
        requested_qty=requested_qty,
        requested_price=requested_price,
        order_type=order_type,
        strategy_type=strategy_type,
        reason_code=reason_code,
        structured_context=structured_context or {},
    )
    return cid


def mark_broker_accepted(
    correlation_id: str,
    *,
    broker_order_id: str,
    broker_response: Optional[Dict[str, Any]] = None,
) -> None:
    update_order_journal(
        correlation_id,
        status=JOURNAL_BROKER_ACCEPTED,
        broker_order_id=broker_order_id,
        broker_response=broker_response,
    )
