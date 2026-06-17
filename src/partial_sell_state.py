# src/partial_sell_state.py
"""부분 익절 공통 상태 — trader / risk_manager 공유."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from utils import KST, OUTPUT_DIR, normalize_ticker_6

logger = logging.getLogger(__name__)

PARTIAL_SELL_FLAGS_FILE = OUTPUT_DIR / "partial_sell_flags.json"
COOLDOWN_FILE = OUTPUT_DIR / "cooldown.json"


def compute_partial_qty(
    total_qty: int,
    ratio: float,
    *,
    full_if_rounding_zero: bool = True,
) -> int:
    """부분 익절 수량. int(qty*ratio)==0 이고 1주 이상이면 전량 treat."""
    total = int(total_qty or 0)
    if total <= 0:
        return 0
    partial = int(total * float(ratio or 0))
    if partial <= 0 and full_if_rounding_zero and total >= 1:
        return total
    return max(partial, 0)


def _load_flags() -> Dict:
    if not PARTIAL_SELL_FLAGS_FILE.exists():
        return {}
    try:
        with open(PARTIAL_SELL_FLAGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (IOError, json.JSONDecodeError):
        return {}


def _save_flags(data: Dict) -> None:
    PARTIAL_SELL_FLAGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PARTIAL_SELL_FLAGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def mark_partial_sell(ticker: str) -> None:
    t = normalize_ticker_6(ticker)
    flags = _load_flags()
    flags[t] = datetime.now(KST).isoformat()
    _save_flags(flags)


def clear_partial_sell_flag(ticker: str) -> None:
    t = normalize_ticker_6(ticker)
    flags = _load_flags()
    if t in flags:
        del flags[t]
        _save_flags(flags)


def had_partial_sell(ticker: str, ttl_days: int = 7) -> bool:
    t = normalize_ticker_6(ticker)
    raw = _load_flags().get(t)
    if not isinstance(raw, str) or not raw:
        return False
    try:
        ts = datetime.fromisoformat(raw)
    except Exception:
        flags = _load_flags()
        flags.pop(t, None)
        _save_flags(flags)
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=KST)
    ttl = max(1, int(ttl_days or 7))
    if datetime.now(KST) - ts <= timedelta(days=ttl):
        return True
    clear_partial_sell_flag(t)
    return False


def has_open_sell_order(ticker: str) -> bool:
    t = normalize_ticker_6(ticker)
    try:
        from recorder import get_recorder

        for o in get_recorder().get_open_orders(statuses=("pending", "partial")):
            if str(o.get("action", "")).upper() == "SELL":
                if normalize_ticker_6(str(o.get("ticker", ""))) == t:
                    return True
    except Exception as e:
        logger.debug("open SELL 조회 실패 (%s): %s", t, e)
    return False


def has_buy_record(ticker: str) -> bool:
    t = normalize_ticker_6(ticker)
    try:
        from recorder import fetch_trades_by_tickers

        trades = fetch_trades_by_tickers([t])
        if not trades or t not in trades:
            return False
        for row in trades[t]:
            if str(row.get("action", "")).upper() != "BUY":
                continue
            st = (row.get("order_status") or "executed").lower()
            if st in ("executed", "pending", "submitted", "partial"):
                return True
    except Exception as e:
        logger.debug("BUY 기록 조회 실패 (%s): %s", t, e)
    return False


def add_post_partial_buy_cooldown(ticker: str, days: int, reason: str) -> None:
    t = normalize_ticker_6(ticker)
    try:
        days = int(days)
    except (TypeError, ValueError):
        return
    if days <= 0:
        return

    cooldown: Dict = {}
    if COOLDOWN_FILE.exists():
        try:
            with open(COOLDOWN_FILE, "r", encoding="utf-8") as f:
                cooldown = json.load(f)
            if not isinstance(cooldown, dict):
                cooldown = {}
        except (IOError, json.JSONDecodeError):
            cooldown = {}

    until_dt = datetime.now(KST) + timedelta(days=days)
    prev = cooldown.get(t)
    if isinstance(prev, str):
        try:
            prev_dt = datetime.fromisoformat(prev)
            if prev_dt.tzinfo is None:
                prev_dt = prev_dt.replace(tzinfo=KST)
            if prev_dt > until_dt:
                until_dt = prev_dt
        except Exception:
            pass

    cooldown[t] = until_dt.isoformat()
    COOLDOWN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COOLDOWN_FILE, "w", encoding="utf-8") as f:
        json.dump(cooldown, f, indent=2, ensure_ascii=False)
    logger.info("[%s] %s → cooldown until %s", t, reason, until_dt.isoformat()[:19])
