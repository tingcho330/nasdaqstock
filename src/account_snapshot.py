# src/account_snapshot.py
"""KIS endpoint 기반 계좌 스냅샷 (매매 판단 primary source)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

US_BALANCE_EXCHANGES: tuple = ("NASD", "NYSE", "AMEX")


@dataclass
class AccountSnapshot:
    """KIS 해외주식 엔드포인트 응답으로 구성된 계좌 스냅샷."""

    trade_date: str
    source: str = "kis_endpoint"
    balance_endpoint: str = ""
    present_balance_endpoint: str = ""
    nccs_endpoint: str = ""
    ccnl_endpoint: Optional[str] = None
    holdings: List[Dict[str, Any]] = field(default_factory=list)
    cash_map: Dict[str, Any] = field(default_factory=dict)
    open_orders: List[Dict[str, Any]] = field(default_factory=list)
    sell_pending_qty_by_ticker: Dict[str, int] = field(default_factory=dict)
    sellable_qty_by_ticker: Dict[str, int] = field(default_factory=dict)
    holding_count: int = 0
    tickers: List[str] = field(default_factory=list)
    snapshot_ts: str = ""
    exchange_codes: List[str] = field(default_factory=list)
    valid: bool = True
    invalid_reason: str = ""
    error: str = ""

    @property
    def available_cash(self) -> int:
        try:
            return int(self.cash_map.get("available_cash", 0) or 0)
        except (TypeError, ValueError):
            return 0

    @property
    def total_value(self) -> float:
        hold_val = sum(
            _safe_float(h.get("evlu_amt"))
            for h in self.holdings
            if _safe_int(h.get("hldg_qty")) > 0
        )
        return hold_val + float(self.available_cash)

    def open_sell_order_count(self) -> int:
        return sum(
            1
            for o in self.open_orders
            if str(o.get("side", "")).lower() == "sell"
        )

    def is_invalid_for_trading(self) -> bool:
        if not self.valid:
            return True
        if self.holding_count > 0 and self.total_value <= 0:
            return True
        return False


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        if val is None or val == "":
            return default
        return int(float(str(val).replace(",", "").strip()))
    except (TypeError, ValueError):
        return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def compute_sellable_qty(holding_qty: int, pending_sell_qty: int) -> int:
    return max(0, int(holding_qty) - int(pending_sell_qty))


def build_sellable_qty_map(
    holdings: List[Dict[str, Any]],
    sell_pending_qty_by_ticker: Dict[str, int],
    *,
    norm_ticker_fn,
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for h in holdings:
        sym = norm_ticker_fn(h.get("pdno", ""))
        if not sym:
            continue
        qty = _safe_int(h.get("hldg_qty"))
        if qty <= 0:
            continue
        pending = sell_pending_qty_by_ticker.get(sym, 0)
        out[sym] = compute_sellable_qty(qty, pending)
    return out


def validate_snapshot_for_trading(snapshot: AccountSnapshot) -> Optional[str]:
    """ACCOUNT_SNAPSHOT_INVALID 조건 반환 (없으면 None)."""
    if snapshot.error:
        return snapshot.error
    if not snapshot.valid:
        return snapshot.invalid_reason or "snapshot invalid"
    if snapshot.holding_count > 0 and snapshot.total_value <= 0:
        return "holdings>0 but total_value=0"
    return None


def log_account_snapshot_kis(snapshot: AccountSnapshot) -> None:
    open_sell = snapshot.open_sell_order_count()
    logger.info(
        "[ACCOUNT_SNAPSHOT_KIS] source=%s holding_count=%s tickers=%s "
        "available_cash=%s total_value=%.2f open_sell_orders=%s exchanges=%s",
        snapshot.source,
        snapshot.holding_count,
        snapshot.tickers,
        snapshot.available_cash,
        snapshot.total_value,
        open_sell,
        snapshot.exchange_codes,
    )


def is_sell_order_success(
    result: Dict[str, Any],
    *,
    executed_qty: int = 0,
) -> bool:
    """KIS 주문 성공 또는 실제 체결 기준."""
    if executed_qty > 0:
        return True
    if not result:
        return False
    rt_cd = str(result.get("rt_cd") or "")
    odno = (
        result.get("ODNO")
        or result.get("odno")
        or result.get("order_id")
        or (result.get("raw") or {}).get("ODNO")
        or (result.get("raw") or {}).get("odno")
    )
    if rt_cd == "0" and odno and str(odno).strip():
        return True
    return False


def is_sell_qty_exceeded_error(result: Dict[str, Any]) -> bool:
    msg1 = str(result.get("msg1") or "")
    msg_cd = str(result.get("msg_cd") or "")
    combined = f"{msg1} {msg_cd}".lower()
    return (
        "가능수량" in msg1
        or "주문수량이" in msg1
        or "sellable" in combined
        or msg_cd.upper() in ("APBK", "APBK0999")
    )
