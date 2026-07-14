# src/account_snapshot.py
"""KIS endpoint 기반 계좌 스냅샷 (매매 판단 primary source)."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils import KST, OUTPUT_DIR, norm_ticker

logger = logging.getLogger(__name__)

US_BALANCE_EXCHANGES: tuple = ("NASD", "NYSE", "AMEX")

SENSITIVE_EVIDENCE_KEYS = re.compile(
    r"(appkey|app_secret|secret|authorization|access_token|token|cano|acnt|account_no|account_number|계좌)",
    re.I,
)

KIS_EVIDENCE_SCHEMA_VERSION = "1.0"


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
    endpoint_evidence: Dict[str, Any] = field(default_factory=dict)

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


def _sanitize_evidence_value(val: Any) -> Any:
    if isinstance(val, dict):
        return {k: _sanitize_evidence_value(v) for k, v in val.items() if not SENSITIVE_EVIDENCE_KEYS.search(str(k))}
    if isinstance(val, list):
        return [_sanitize_evidence_value(x) for x in val]
    if isinstance(val, str) and SENSITIVE_EVIDENCE_KEYS.search(val):
        return "***"
    return val


def _holdings_summary(holdings: List[Dict[str, Any]], market: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for h in holdings or []:
        sym = norm_ticker(h.get("pdno", ""), market)
        qty = _safe_int(h.get("hldg_qty"))
        if not sym or qty <= 0:
            continue
        avg = _safe_float(h.get("pchs_avg_pric") or h.get("avg_unpr3") or h.get("avg_price"))
        val = _safe_float(h.get("evlu_amt"))
        if val <= 0 and avg > 0:
            val = avg * qty
        out.append({
            "ticker": sym,
            "qty": qty,
            "avg_price": round(avg, 4) if avg else 0.0,
            "valuation_usd": round(val, 2),
        })
    return out


def build_account_snapshot_evidence_payload(
    snapshot: AccountSnapshot,
    *,
    market: str,
    session: Optional[str] = None,
    pipeline_context_source: Optional[str] = None,
    generated_at_kst: Optional[str] = None,
) -> Dict[str, Any]:
    """KIS endpoint 요약 evidence (raw 응답·민감정보 제외)."""
    ep = snapshot.endpoint_evidence or {}
    cash = snapshot.cash_map or {}
    available = _safe_float(
        cash.get("available_cash_usd")
        or cash.get("available_cash")
        or cash.get("ord_psbl_frcr_amt")
    )
    holdings_val = round(
        sum(
            _safe_float(h.get("evlu_amt"))
            for h in snapshot.holdings or []
            if _safe_int(h.get("hldg_qty")) > 0
        ),
        2,
    )
    if holdings_val <= 0:
        holdings_val = _safe_float(cash.get("holdings_value_usd"))
    computed_total = round(available + holdings_val, 2)
    total_asset = _safe_float(cash.get("total_asset_usd") or cash.get("tot_evlu_amt_usd"))
    if total_asset <= 0:
        total_asset = computed_total
    available_krw = _safe_float(cash.get("available_cash_krw"))
    total_asset_krw = _safe_float(cash.get("total_asset_krw") or cash.get("tot_evlu_amt_krw"))
    holdings_val_krw = _safe_float(cash.get("holdings_value_krw"))
    krw_cash = _safe_float(cash.get("krw_cash"))
    open_sell = snapshot.open_sell_order_count()
    open_buy = max(0, len(snapshot.open_orders or []) - open_sell)
    snapshot_ts = snapshot.snapshot_ts or datetime.now(KST).isoformat()
    gen_at = generated_at_kst or snapshot_ts
    bal_ep = ep.get("balance") or {}
    exchange_coverage = list(bal_ep.get("exchange_coverage") or snapshot.exchange_codes or [])
    holding_exchange_coverage = sorted({
        str(h.get("ovrs_excg_cd") or "").upper()
        for h in snapshot.holdings or []
        if str(h.get("ovrs_excg_cd") or "").strip()
    })
    return _sanitize_evidence_value({
        "schema_version": KIS_EVIDENCE_SCHEMA_VERSION,
        "source": "kis_endpoint",
        "market": market,
        "trade_date": snapshot.trade_date,
        "session": session,
        "snapshot_ts_kst": snapshot_ts,
        "pipeline_context_source": pipeline_context_source,
        "generated_at_kst": gen_at,
        "valid": snapshot.valid,
        "error": snapshot.error or snapshot.invalid_reason or None,
        "endpoint_evidence": ep,
        "endpoints": ep,
        "holdings_count": snapshot.holding_count,
        "tickers": list(snapshot.tickers or []),
        "exchange_coverage": exchange_coverage,
        "holding_exchange_coverage": holding_exchange_coverage,
        "holdings_value_usd": holdings_val,
        "available_cash_usd": round(available, 2),
        "total_asset_usd": round(total_asset, 2),
        "available_cash_krw": round(available_krw, 2),
        "total_asset_krw": round(total_asset_krw, 2),
        "holdings_value_krw": round(holdings_val_krw, 2),
        "krw_cash": round(krw_cash, 2),
        "cash_map": {
            "USD": {
                "available_cash": round(available, 2),
                "total_asset": round(total_asset, 2),
                "holdings_value": holdings_val,
                "currency": "USD",
            },
            "KRW": {
                "available_cash": round(available_krw, 2),
                "total_asset": round(total_asset_krw, 2),
                "holdings_value": round(holdings_val_krw, 2),
                "krw_cash": round(krw_cash, 2),
                "currency": "KRW",
            },
        },
        "sell_pending_qty_by_ticker": dict(snapshot.sell_pending_qty_by_ticker or {}),
        "sellable_qty_by_ticker": dict(snapshot.sellable_qty_by_ticker or {}),
        "open_orders_summary": {
            "count": len(snapshot.open_orders or []),
            "sell_count": open_sell,
            "buy_count": open_buy,
        },
    })


def account_snapshot_evidence_paths(
    market: str,
    trade_date: str,
    session: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> tuple[Path, Path]:
    base = output_dir or OUTPUT_DIR
    base.mkdir(parents=True, exist_ok=True)
    if session:
        dated = base / f"account_snapshot_{market}_{trade_date}_{session}.json"
    else:
        dated = base / f"account_snapshot_{market}_{trade_date}.json"
    latest = base / f"account_snapshot_latest_{market}.json"
    return dated, latest


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    import os
    import tempfile

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


def validate_snapshot_target_dates(
    *,
    snapshot_trade_date: str,
    resolved_live_trade_date: str,
    target_filename_date: str,
) -> Optional[str]:
    """Return error code if dated/live/filename dates disagree; else None."""
    a = str(snapshot_trade_date or "").strip()
    b = str(resolved_live_trade_date or "").strip()
    c = str(target_filename_date or "").strip()
    if a and b and c and a == b == c:
        return None
    return "ACCOUNT_SNAPSHOT_TARGET_DATE_MISMATCH"


def save_account_snapshot_evidence(
    snapshot: AccountSnapshot,
    *,
    market: str,
    session: Optional[str] = None,
    output_dir: Optional[Path] = None,
    pipeline_context_source: Optional[str] = None,
    generated_at_kst: Optional[str] = None,
    resolved_live_trade_date: Optional[str] = None,
    allow_date_mismatch: bool = False,
) -> Optional[Path]:
    """AccountSnapshot KIS endpoint evidence JSON 저장 (원자적).

    live 저장 시 snapshot.trade_date == resolved_live_trade_date == filename date
    검증에 실패하면 dated/latest 모두 갱신하지 않는다.
    """
    try:
        trade_date = str(snapshot.trade_date or "").strip()
        live_td = str(resolved_live_trade_date or trade_date).strip()
        dated_path, latest_path = account_snapshot_evidence_paths(
            market, trade_date, session, output_dir,
        )
        # filename date is the trade_date segment used to build dated_path
        target_filename_date = trade_date
        if not allow_date_mismatch:
            mismatch = validate_snapshot_target_dates(
                snapshot_trade_date=trade_date,
                resolved_live_trade_date=live_td,
                target_filename_date=target_filename_date,
            )
            if mismatch:
                logger.error(
                    "[%s] snapshot.trade_date=%s resolved_live=%s target_file_date=%s "
                    "— dated/latest write blocked",
                    mismatch,
                    trade_date,
                    live_td,
                    target_filename_date,
                )
                return None

        payload = build_account_snapshot_evidence_payload(
            snapshot,
            market=market,
            session=session,
            pipeline_context_source=pipeline_context_source,
            generated_at_kst=generated_at_kst,
        )
        payload["resolved_live_trade_date"] = live_td
        _atomic_write_json(dated_path, payload)
        _atomic_write_json(latest_path, payload)
        logger.info("[ACCOUNT_SNAPSHOT_SAVED] path=%s", dated_path)
        return dated_path
    except Exception as e:
        logger.error("[ACCOUNT_SNAPSHOT_SAVE_FAILED] reason=%s", str(e)[:300])
        return None


def extract_account_file_date(
    balance_path: Optional[Path],
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Resolve account_file_date from metadata or filename; include mtime."""
    meta: Dict[str, Any] = {
        "account_file": str(balance_path) if balance_path else None,
        "account_file_date": None,
        "account_file_mtime": None,
        "trade_date": None,
        "source": None,
        "valid": None,
    }
    if balance_path is not None:
        try:
            if balance_path.exists():
                meta["account_file_mtime"] = balance_path.stat().st_mtime
        except OSError:
            pass
        m = re.search(r"(\d{8})", balance_path.name)
        if m:
            meta["account_file_date"] = m.group(1)

    data = payload
    if data is None and balance_path is not None and balance_path.exists():
        try:
            with open(balance_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None

    if isinstance(data, dict):
        td = (
            data.get("trade_date")
            or (data.get("metadata") or {}).get("trade_date")
            or data.get("date")
        )
        if td and re.fullmatch(r"\d{8}", str(td).strip()):
            meta["trade_date"] = str(td).strip()
            meta["account_file_date"] = meta["trade_date"]
        meta["source"] = data.get("source") or (data.get("metadata") or {}).get("source")
        if "valid" in data:
            meta["valid"] = data.get("valid")
        elif isinstance(data.get("metadata"), dict) and "valid" in data["metadata"]:
            meta["valid"] = data["metadata"].get("valid")
        # Filename wins only when metadata missing
        if not meta["account_file_date"] and balance_path is not None:
            m = re.search(r"(\d{8})", balance_path.name)
            if m:
                meta["account_file_date"] = m.group(1)

    return meta
