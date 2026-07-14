# -*- coding: utf-8 -*-
"""
KIS CCNL ledger repair — UPDATE-only, transactional, dry-run by default.

Does not auto-run on import or normal reconciler. Explicit CLI only:
  python order_reconciler.py --repair-ledger-from-ccnl --ticker X --from Y --to Z [--apply-repair]
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils import KST, OUTPUT_DIR, norm_ticker

logger = logging.getLogger("LedgerRepair")


def default_trading_db_path() -> Path:
    return Path(os.environ.get("OUTPUT_DIR", str(OUTPUT_DIR) or "/app/output")) / "trading_data.db"


def file_checksum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def file_meta(path: Path) -> Dict[str, Any]:
    st = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size": st.st_size,
        "mtime": st.st_mtime,
        "sha256": file_checksum(path),
    }


def _dec(val: Any) -> Optional[Decimal]:
    if val is None:
        return None
    if isinstance(val, Decimal):
        return val
    s = str(val).strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _dec_str(val: Any) -> Optional[str]:
    d = _dec(val)
    if d is None:
        return None
    return format(d, "f")


def _parse_hhmmss(t: Any) -> Optional[Tuple[int, int, int]]:
    digits = "".join(ch for ch in str(t or "") if ch.isdigit())
    if len(digits) < 6:
        return None
    try:
        return int(digits[0:2]), int(digits[2:4]), int(digits[4:6])
    except ValueError:
        return None


def _combine_kst(date_ymd: str, time_raw: Any) -> Optional[datetime]:
    d = str(date_ymd or "").strip()
    if not re.fullmatch(r"\d{8}", d):
        return None
    hm = _parse_hhmmss(time_raw)
    if hm is None:
        return None
    hh, mm, ss = hm
    try:
        return datetime(int(d[0:4]), int(d[4:6]), int(d[6:8]), hh, mm, ss, tzinfo=KST)
    except ValueError:
        return None


def effective_trade_timestamp_from_ccnl(kis_o: Dict[str, Any]) -> Tuple[Optional[datetime], Optional[str]]:
    """
    KST effective timestamp priority:
      1) dmst_ord_dt + thco_ord_tmd
      2) dmst_ord_dt + ord_tmd
      3) ord_dt + thco_ord_tmd
      4) ord_dt + ord_tmd
    Returns (datetime|None, incompleteness_reason|None).
    """
    dmst = str(
        kis_o.get("dmst_ord_dt")
        or kis_o.get("domestic_order_date")
        or ""
    ).strip()
    ord_dt = str(kis_o.get("ord_dt") or kis_o.get("order_date") or "").strip()
    thco = str(kis_o.get("thco_ord_tmd") or "").strip()
    ord_tmd = str(kis_o.get("ord_tmd") or "").strip()
    # legacy fallback if only order_time populated
    if not ord_tmd and not thco:
        legacy = str(kis_o.get("order_time") or "").strip()
        ord_tmd = legacy

    candidates = [
        (dmst, thco),
        (dmst, ord_tmd),
        (ord_dt, thco),
        (ord_dt, ord_tmd),
    ]
    for date_part, time_part in candidates:
        if date_part and time_part:
            dt = _combine_kst(date_part, time_part)
            if dt is not None:
                return dt, None
    return None, "TIMESTAMP_EVIDENCE_INCOMPLETE"


@dataclass
class FifoLot:
    buy_order_id: str
    effective_trade_timestamp: str
    remaining_qty: int
    executed_price: Decimal


@dataclass
class SellMatchResult:
    gross_pnl: Optional[Decimal]
    gross_pnl_complete: bool
    incomplete_reason: Optional[str] = None
    matched_buy_lots: List[Dict[str, Any]] = field(default_factory=list)
    fifo_lot_remaining_after: List[Dict[str, Any]] = field(default_factory=list)


def fifo_match_sell(
    lots: List[FifoLot],
    *,
    sell_executed_price: Decimal,
    sell_qty: int,
) -> SellMatchResult:
    """Consume FIFO lots for one SELL. Mutates lots in place."""
    if sell_qty <= 0:
        return SellMatchResult(
            gross_pnl=None,
            gross_pnl_complete=False,
            incomplete_reason="invalid_sell_qty",
        )
    remaining = sell_qty
    matched: List[Dict[str, Any]] = []
    gross = Decimal("0")
    while remaining > 0 and lots:
        lot = lots[0]
        if lot.remaining_qty <= 0:
            lots.pop(0)
            continue
        take = min(lot.remaining_qty, remaining)
        matched.append({
            "buy_order_id": lot.buy_order_id,
            "effective_trade_timestamp": lot.effective_trade_timestamp,
            "matched_qty": take,
            "executed_price": _dec_str(lot.executed_price),
        })
        gross += (sell_executed_price - lot.executed_price) * Decimal(take)
        lot.remaining_qty -= take
        remaining -= take
        if lot.remaining_qty <= 0:
            lots.pop(0)

    snapshot = [
        {
            "buy_order_id": L.buy_order_id,
            "effective_trade_timestamp": L.effective_trade_timestamp,
            "remaining_qty": L.remaining_qty,
            "executed_price": _dec_str(L.executed_price),
        }
        for L in lots
        if L.remaining_qty > 0
    ]

    if remaining > 0:
        return SellMatchResult(
            gross_pnl=None,
            gross_pnl_complete=False,
            incomplete_reason="buy_cost_basis_unavailable",
            matched_buy_lots=matched,
            fifo_lot_remaining_after=snapshot,
        )
    return SellMatchResult(
        gross_pnl=gross,
        gross_pnl_complete=True,
        matched_buy_lots=matched,
        fifo_lot_remaining_after=snapshot,
    )


def _parse_ctx(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def _load_db_rows_by_order_id(
    conn: sqlite3.Connection,
    *,
    ticker: str,
) -> Dict[str, Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT * FROM trade_records WHERE UPPER(ticker)=UPPER(?) "
        "AND order_id IS NOT NULL AND TRIM(order_id) != '' "
        "ORDER BY id ASC",
        (ticker,),
    )
    out: Dict[str, Dict[str, Any]] = {}
    for row in cur.fetchall():
        d = dict(row)
        oid = str(d.get("order_id") or "").strip()
        if oid and oid not in out:
            out[oid] = d
    return out


def _row_count(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM trade_records").fetchone()[0])


def _sort_key_for_fifo(kis_o: Dict[str, Any], eff: datetime) -> Tuple:
    side = str(kis_o.get("side") or "").lower()
    # BUY before SELL at identical timestamp
    side_rank = 0 if side == "buy" else 1
    return (eff.isoformat(), side_rank, str(kis_o.get("order_id") or ""))


def _build_after_fields(
    *,
    db_row: Dict[str, Any],
    kis_o: Dict[str, Any],
    eff: datetime,
    sell_match: Optional[SellMatchResult],
    corrected_at: str,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """Return (column_updates, new_context, changed_field_names)."""
    prev_ctx = _parse_ctx(db_row.get("structured_context"))
    exe_px = _dec(kis_o.get("executed_price"))
    assert exe_px is not None
    exe_qty = int(kis_o.get("executed_qty") or 0)
    req_qty = int(
        kis_o.get("requested_qty")
        or kis_o.get("quantity")
        or exe_qty
        or 0
    )
    order_px = _dec(kis_o.get("order_price"))
    exe_amt = _dec(kis_o.get("executed_amount"))
    amount = float(exe_amt) if exe_amt is not None else float(exe_px) * exe_qty
    side = str(kis_o.get("side") or "").lower()

    previous_db_values = {
        "timestamp": db_row.get("timestamp"),
        "quantity": db_row.get("quantity"),
        "requested_qty": db_row.get("requested_qty"),
        "executed_qty": db_row.get("executed_qty"),
        "price": db_row.get("price"),
        "profit_loss": db_row.get("profit_loss"),
        "amount": db_row.get("amount"),
    }

    new_ctx = dict(prev_ctx)
    new_ctx.update({
        "price_column_semantics": "executed_price",
        "order_price": _dec_str(order_px),
        "executed_price": _dec_str(exe_px),
        "effective_trade_timestamp": eff.isoformat(),
        "ord_dt": str(kis_o.get("ord_dt") or kis_o.get("order_date") or "") or prev_ctx.get("ord_dt"),
        "dmst_ord_dt": str(kis_o.get("dmst_ord_dt") or kis_o.get("domestic_order_date") or "")
        or prev_ctx.get("dmst_ord_dt"),
        "ord_tmd": str(kis_o.get("ord_tmd") or "") or prev_ctx.get("ord_tmd"),
        "thco_ord_tmd": str(kis_o.get("thco_ord_tmd") or "") or prev_ctx.get("thco_ord_tmd"),
        "order_date": str(kis_o.get("order_date") or kis_o.get("ord_dt") or "") or prev_ctx.get("order_date"),
        "order_time": str(kis_o.get("ord_tmd") or kis_o.get("order_time") or "") or prev_ctx.get("order_time"),
        "exchange": kis_o.get("exchange") or prev_ctx.get("exchange"),
        "currency": kis_o.get("currency") or prev_ctx.get("currency") or "USD",
        "media": kis_o.get("media") or prev_ctx.get("media"),
        "previous_db_values": previous_db_values,
        "corrected_from_ccnl": True,
        "corrected_at_kst": corrected_at,
        "net_pnl_complete": False,
        "commission_known": bool(prev_ctx.get("commission_known")) if float(db_row.get("commission") or 0) else False,
        "tax_known": bool(prev_ctx.get("tax_known")) if float(db_row.get("tax") or 0) else False,
    })

    profit_loss = float(db_row.get("profit_loss") or 0)
    if side == "sell" and sell_match is not None:
        new_ctx["gross_pnl_basis"] = True
        new_ctx["gross_pnl_complete"] = sell_match.gross_pnl_complete
        new_ctx["matched_buy_lots"] = sell_match.matched_buy_lots
        new_ctx["fifo_lot_remaining_after"] = sell_match.fifo_lot_remaining_after
        if sell_match.gross_pnl_complete and sell_match.gross_pnl is not None:
            new_ctx["gross_pnl"] = _dec_str(sell_match.gross_pnl)
            new_ctx.pop("incomplete_reason", None)
            profit_loss = float(sell_match.gross_pnl)
            if len(sell_match.matched_buy_lots) == 1:
                lot0 = sell_match.matched_buy_lots[0]
                new_ctx["matched_buy_order_id"] = lot0.get("buy_order_id")
                new_ctx["matched_buy_executed_price"] = lot0.get("executed_price")
                new_ctx["matched_buy_qty"] = lot0.get("matched_qty")
            else:
                new_ctx["matched_buy_order_id"] = None
                new_ctx["matched_buy_executed_price"] = None
                new_ctx["matched_buy_qty"] = sum(
                    int(m.get("matched_qty") or 0) for m in sell_match.matched_buy_lots
                )
        else:
            new_ctx["gross_pnl"] = None
            new_ctx["incomplete_reason"] = sell_match.incomplete_reason or "buy_cost_basis_unavailable"
            # Do not treat incomplete gross as realized 0
            profit_loss = float(db_row.get("profit_loss") or 0)
            # Keep previous until apply decides — caller zeros on incomplete if policy wants
            # Spec: profit_loss only updated when gross complete
            profit_loss = float(db_row.get("profit_loss") or 0)

    cols = {
        "timestamp": eff.isoformat(),
        "quantity": exe_qty,
        "requested_qty": req_qty,
        "executed_qty": exe_qty,
        "price": float(exe_px),
        "amount": amount,
        "total_cost": amount,
        "net_amount": amount,
        "order_status": "executed",
        "profit_loss": (
            profit_loss
            if (side != "sell" or (sell_match and sell_match.gross_pnl_complete))
            else float(db_row.get("profit_loss") or 0)
        ),
        "structured_context": json.dumps(new_ctx, ensure_ascii=False),
    }
    if side == "sell" and sell_match and sell_match.gross_pnl_complete and sell_match.gross_pnl is not None:
        cols["profit_loss"] = float(sell_match.gross_pnl)
    elif side == "sell" and sell_match and not sell_match.gross_pnl_complete:
        # leave profit_loss unchanged in columns relative to "need update" detection,
        # but do not interpret as known 0; document in context only
        cols["profit_loss"] = float(db_row.get("profit_loss") or 0)

    changed: List[str] = []
    for k, v in cols.items():
        if k == "structured_context":
            old_ctx = _parse_ctx(db_row.get("structured_context"))
            # Compare key economic fields rather than entire JSON blob noise
            interesting = (
                "price_column_semantics", "order_price", "executed_price",
                "effective_trade_timestamp", "gross_pnl", "gross_pnl_complete",
                "matched_buy_lots",
            )
            if any(old_ctx.get(i) != new_ctx.get(i) for i in interesting):
                changed.append(k)
            continue
        old = db_row.get(k)
        if k in ("price", "amount", "total_cost", "net_amount", "profit_loss"):
            try:
                if abs(float(old or 0) - float(v)) > 1e-9:
                    changed.append(k)
            except Exception:
                changed.append(k)
        elif k == "timestamp":
            old_s = str(old or "")
            if not old_s.startswith(str(v)[:19]):
                changed.append(k)
        else:
            if str(old) != str(v):
                changed.append(k)

    # Detect numeric corrections for counters
    try:
        if abs(float(db_row.get("price") or 0) - float(exe_px)) > 1e-9:
            if "price" not in changed:
                changed.append("price")
    except Exception:
        pass

    return cols, new_ctx, changed


def repair_ledger_from_ccnl(
    *,
    ticker: str,
    date_from: str,
    date_to: str,
    apply: bool = False,
    db_path: Optional[Path] = None,
    evidence_path: Optional[Path] = None,
    market: str = "SP500",
    kis_orders: Optional[Dict[str, Dict[str, Any]]] = None,
    fetch_ccnl: Optional[Callable[..., Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]]] = None,
    output_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    UPDATE-only CCNL ledger repair for one ticker + date window.
    apply=False => dry-run (no DB writes, checksum unchanged).
    Never creates a new DB file. Never INSERT new trade_records.
    """
    mode = "apply" if apply else "dry_run"
    tkr = norm_ticker(ticker, market)
    path = Path(db_path) if db_path else default_trading_db_path()
    out_dir = Path(output_dir) if output_dir else Path(os.environ.get("OUTPUT_DIR", str(OUTPUT_DIR) or "/app/output"))
    generated_at = datetime.now(KST).isoformat()

    result: Dict[str, Any] = {
        "generated_at_kst": generated_at,
        "market": market,
        "ticker": tkr,
        "date_from": date_from,
        "date_to": date_to,
        "mode": mode,
        "db_path": str(path),
        "db_exists": path.exists(),
        "backup_path": None,
        "kis_ccnl_raw_count": 0,
        "kis_ccnl_unique_order_count": 0,
        "db_matched_order_count": 0,
        "rows_needing_update": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
        "duplicate_order_ids": [],
        "gross_pnl_total": None,
        "gross_complete": True,
        "net_complete": False,
        "changes": [],
        "proposed_changes": [],
        "broker_only_detected_count": 0,
        "broker_only_orders": [],
        "ccnl_corrected_order_count": 0,
        "executed_price_corrected_count": 0,
        "quantity_corrected_count": 0,
        "sell_pnl_corrected_count": 0,
        "error": None,
    }

    logger.info("[LEDGER_REPAIR] mode=%s ticker=%s from=%s to=%s db=%s", mode, tkr, date_from, date_to, path)

    if not path.exists():
        result["error"] = f"DB file not found: {path} (refusing to create)"
        logger.error(result["error"])
        _save_evidence(result, out_dir, evidence_path, market, tkr, date_from, date_to, mode)
        return result

    meta_before = file_meta(path)

    # Fetch / accept CCNL
    ccnl_evidence: Dict[str, Any] = {}
    if kis_orders is not None:
        by_id = dict(kis_orders)
        raw_count = len(by_id)
    else:
        fetch = fetch_ccnl
        if fetch is None:
            from order_reconciler import _fetch_overseas_daily_orders
            from api.kis_auth import KIS
            from settings import settings

            env = settings._config.get("trading_environment", "vps")
            kis = KIS(env=env)

            def fetch(start_ymd: str, end_ymd: str):
                return _fetch_overseas_daily_orders(
                    kis, start_ymd=start_ymd, end_ymd=end_ymd, env=env,
                )

        by_id, ccnl_evidence = fetch(date_from, date_to)
        raw_count = int(ccnl_evidence.get("raw_row_count") or len(by_id))

    # Filter ticker + executed only
    filtered: Dict[str, Dict[str, Any]] = {}
    for oid, o in by_id.items():
        if norm_ticker(str(o.get("ticker") or ""), market) != tkr:
            continue
        if str(o.get("status") or "").lower() != "executed":
            continue
        if int(o.get("executed_qty") or 0) <= 0:
            continue
        if _dec(o.get("executed_price")) is None or _dec(o.get("executed_price")) <= 0:
            continue
        filtered[str(oid)] = o

    result["kis_ccnl_raw_count"] = raw_count
    result["kis_ccnl_unique_order_count"] = len(filtered)
    result["ccnl_evidence"] = ccnl_evidence

    # Detect duplicates in input list (already unique by_id)
    result["duplicate_order_ids"] = list(ccnl_evidence.get("duplicate_order_ids") or [])

    conn = sqlite3.connect(str(path))
    try:
        db_rows = _load_db_rows_by_order_id(conn, ticker=tkr)
        row_count_before = _row_count(conn)

        # Broker-only relative to this ticker window (detect only — no insert)
        broker_only = [
            {
                "order_id": oid,
                "ticker": tkr,
                "side": o.get("side"),
                "executed_qty": o.get("executed_qty"),
                "executed_price": _dec_str(o.get("executed_price")),
            }
            for oid, o in filtered.items()
            if oid not in db_rows
        ]
        result["broker_only_detected_count"] = len(broker_only)
        result["broker_only_orders"] = broker_only

        # Stamp timestamps / sort for FIFO among matched + use all filtered executed for lot build
        timed: List[Tuple[datetime, Dict[str, Any]]] = []
        incomplete_ts: List[str] = []
        for oid, o in filtered.items():
            eff, reason = effective_trade_timestamp_from_ccnl(o)
            if eff is None:
                incomplete_ts.append(oid)
                if oid in db_rows:
                    result["rows_skipped"] += 1
                    result["changes"].append({
                        "order_id": oid,
                        "side": o.get("side"),
                        "before": {},
                        "after": {},
                        "changed_fields": [],
                        "effective_trade_timestamp": None,
                        "gross_pnl": None,
                        "gross_pnl_complete": False,
                        "incomplete_reason": reason or "TIMESTAMP_EVIDENCE_INCOMPLETE",
                    })
                continue
            o = {**o, "_eff": eff}
            timed.append((eff, o))

        timed.sort(key=lambda x: _sort_key_for_fifo(x[1], x[0]))

        # FIFO across all executed (including broker-only fills for cost basis),
        # but only UPDATE rows that exist in DB.
        lots: List[FifoLot] = []
        sell_matches: Dict[str, SellMatchResult] = {}
        for eff, o in timed:
            side = str(o.get("side") or "").lower()
            exe_px = _dec(o.get("executed_price"))
            exe_qty = int(o.get("executed_qty") or 0)
            oid = str(o.get("order_id") or "")
            if side == "buy" and exe_px is not None:
                lots.append(FifoLot(
                    buy_order_id=oid,
                    effective_trade_timestamp=eff.isoformat(),
                    remaining_qty=exe_qty,
                    executed_price=exe_px,
                ))
            elif side == "sell" and exe_px is not None:
                sell_matches[oid] = fifo_match_sell(
                    lots, sell_executed_price=exe_px, sell_qty=exe_qty,
                )

        proposed: List[Dict[str, Any]] = []
        updates: List[Tuple[int, str, Dict[str, Any], Dict[str, Any], List[str]]] = []

        for eff, o in timed:
            oid = str(o.get("order_id") or "")
            if oid not in db_rows:
                continue  # broker-only → no INSERT in ledger repair
            db_row = db_rows[oid]
            result["db_matched_order_count"] += 1
            sm = sell_matches.get(oid) if str(o.get("side")).lower() == "sell" else None
            cols, new_ctx, changed = _build_after_fields(
                db_row=db_row,
                kis_o=o,
                eff=eff,
                sell_match=sm,
                corrected_at=generated_at,
            )
            if not changed:
                result["rows_skipped"] += 1
                continue

            before = {
                "timestamp": db_row.get("timestamp"),
                "quantity": db_row.get("quantity"),
                "requested_qty": db_row.get("requested_qty"),
                "executed_qty": db_row.get("executed_qty"),
                "price": db_row.get("price"),
                "profit_loss": db_row.get("profit_loss"),
            }
            after = {
                "timestamp": cols["timestamp"],
                "quantity": cols["quantity"],
                "requested_qty": cols["requested_qty"],
                "executed_qty": cols["executed_qty"],
                "price": cols["price"],
                "profit_loss": cols["profit_loss"],
            }
            change = {
                "order_id": oid,
                "side": str(o.get("side") or "").upper(),
                "before": before,
                "after": after,
                "changed_fields": changed,
                "effective_trade_timestamp": eff.isoformat(),
                "gross_pnl": new_ctx.get("gross_pnl"),
                "gross_pnl_complete": new_ctx.get("gross_pnl_complete"),
                "incomplete_reason": new_ctx.get("incomplete_reason"),
            }
            proposed.append(change)
            updates.append((int(db_row["id"]), oid, cols, new_ctx, changed))

            if "price" in changed:
                result["executed_price_corrected_count"] += 1
            if "quantity" in changed or "executed_qty" in changed:
                result["quantity_corrected_count"] += 1
            if "profit_loss" in changed or (
                new_ctx.get("gross_pnl_complete") and "structured_context" in changed
            ):
                if str(o.get("side")).lower() == "sell":
                    result["sell_pnl_corrected_count"] += 1

        result["rows_needing_update"] = len(proposed)
        result["proposed_changes"] = proposed
        result["changes"] = proposed  # dry-run documents proposed; apply overwrites below

        # Gross totals from FIFO results for matched sells
        gross_total = Decimal("0")
        any_sell = False
        gross_ok = True
        for oid, sm in sell_matches.items():
            if oid not in db_rows:
                continue
            any_sell = True
            if sm.gross_pnl_complete and sm.gross_pnl is not None:
                gross_total += sm.gross_pnl
            else:
                gross_ok = False
        result["gross_complete"] = (not any_sell) or gross_ok
        result["gross_pnl_total"] = _dec_str(gross_total) if any_sell and gross_ok else (
            _dec_str(gross_total) if any_sell and gross_ok else (None if not gross_ok else "0")
        )
        if any_sell and gross_ok:
            result["gross_pnl_total"] = _dec_str(gross_total)
        elif any_sell:
            # still report known partial? Spec wants total when complete; else null-ish
            known = Decimal("0")
            for oid, sm in sell_matches.items():
                if oid in db_rows and sm.gross_pnl_complete and sm.gross_pnl is not None:
                    known += sm.gross_pnl
            result["gross_pnl_total"] = _dec_str(known) if known != 0 or gross_ok else None
            if not gross_ok:
                # keep known sum of complete sells for visibility
                complete_sum = sum(
                    (sm.gross_pnl for oid, sm in sell_matches.items()
                     if oid in db_rows and sm.gross_pnl_complete and sm.gross_pnl is not None),
                    Decimal("0"),
                )
                result["gross_pnl_total"] = _dec_str(complete_sum)

        result["net_complete"] = False

        if mode == "dry_run":
            result["rows_updated"] = 0
            result["ccnl_corrected_order_count"] = 0
            conn.close()
            meta_after = file_meta(path)
            result["db_meta_before"] = meta_before
            result["db_meta_after"] = meta_after
            result["db_unchanged"] = (
                meta_before["sha256"] == meta_after["sha256"]
                and meta_before["size"] == meta_after["size"]
                and meta_before["mtime"] == meta_after["mtime"]
            )
            _save_evidence(result, out_dir, evidence_path, market, tkr, date_from, date_to, mode)
            logger.info(
                "[LEDGER_REPAIR] dry_run done needing_update=%s broker_only=%s",
                result["rows_needing_update"],
                result["broker_only_detected_count"],
            )
            return result

        # APPLY: backup then transactional UPDATE (no INSERT)
        bak = path.with_name(
            f"{path.stem}.bak.{datetime.now(KST).strftime('%Y%m%d%H%M%S')}{path.suffix}"
        )
        shutil.copy2(path, bak)
        result["backup_path"] = str(bak)
        logger.info("[LEDGER_REPAIR] backup_path=%s", bak)

        try:
            conn.execute("BEGIN IMMEDIATE")
            for row_id, oid, cols, new_ctx, changed in updates:
                conn.execute(
                    """
                    UPDATE trade_records SET
                        timestamp = ?,
                        quantity = ?,
                        requested_qty = ?,
                        executed_qty = ?,
                        price = ?,
                        amount = ?,
                        total_cost = ?,
                        net_amount = ?,
                        profit_loss = ?,
                        order_status = ?,
                        structured_context = ?,
                        last_status_update_ts = ?
                    WHERE id = ? AND order_id = ?
                    """,
                    (
                        cols["timestamp"],
                        cols["quantity"],
                        cols["requested_qty"],
                        cols["executed_qty"],
                        cols["price"],
                        cols["amount"],
                        cols["total_cost"],
                        cols["net_amount"],
                        cols["profit_loss"],
                        cols["order_status"],
                        cols["structured_context"],
                        generated_at,
                        row_id,
                        oid,
                    ),
                )
            # Ensure no accidental inserts — assert row count unchanged
            row_count_after = _row_count(conn)
            if row_count_after != row_count_before:
                raise RuntimeError(
                    f"row count changed during repair: {row_count_before} -> {row_count_after}"
                )
            conn.commit()
            result["rows_updated"] = len(updates)
            result["ccnl_corrected_order_count"] = len(updates)
            result["changes"] = proposed
        except Exception as e:
            conn.rollback()
            result["error"] = f"transaction rolled back: {e}"
            result["rows_updated"] = 0
            logger.exception("[LEDGER_REPAIR] apply failed: %s", e)
        finally:
            conn.close()

        meta_after = file_meta(path)
        result["db_meta_before"] = meta_before
        result["db_meta_after"] = meta_after
        _save_evidence(result, out_dir, evidence_path, market, tkr, date_from, date_to, mode)
        logger.info(
            "[LEDGER_REPAIR] apply done updated=%s backup=%s",
            result["rows_updated"],
            result["backup_path"],
        )
        return result
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        result["error"] = str(e)
        logger.exception("[LEDGER_REPAIR] failed: %s", e)
        _save_evidence(result, out_dir, evidence_path, market, tkr, date_from, date_to, mode)
        return result


def _save_evidence(
    result: Dict[str, Any],
    out_dir: Path,
    evidence_path: Optional[Path],
    market: str,
    ticker: str,
    date_from: str,
    date_to: str,
    mode: str,
) -> Optional[Path]:
    try:
        if evidence_path:
            path = Path(evidence_path)
        else:
            out_dir.mkdir(parents=True, exist_ok=True)
            fname = f"ledger_repair_{market}_{ticker}_{date_from}_{date_to}_{mode}.json"
            path = out_dir / fname
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = deepcopy(result)
        # JSON-safe
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        result["evidence_path"] = str(path)
        return path
    except Exception as e:
        logger.warning("failed to save ledger repair evidence: %s", e)
        return None
