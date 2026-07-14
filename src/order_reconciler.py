#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Order reconciler

- DB(trade_records)의 pending/partial 레코드를 KIS 당일 주문 조회로 재검증하여
  executed/partial/pending/cancelled 상태를 최신화한다.
- B안: 기존 구조 유지 + 정기 리컨실로 pending 누적 방지
"""

from __future__ import annotations

import argparse
import json
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set

from settings import settings
import os

from utils import setup_logging, KST, norm_ticker, is_us_market, kst_window_to_us_order_dates, OUTPUT_DIR

try:
    from account_snapshot import compute_sellable_qty, is_sell_order_success
    from kis_overseas_account import US_BALANCE_EXCHANGES
except ImportError:
    compute_sellable_qty = None
    is_sell_order_success = None
    US_BALANCE_EXCHANGES = ("NASD", "NYSE", "AMEX")
from api.kis_auth import KIS
from recorder import get_recorder

try:
    from db_debug import log as _db_dbg_log, log_skip as _db_dbg_skip, is_enabled as _db_dbg_enabled
except ImportError:
    def _db_dbg_log(*args, **kwargs):
        pass
    def _db_dbg_skip(*args, **kwargs):
        pass
    def _db_dbg_enabled():
        return False


logger = logging.getLogger("OrderReconciler")

KIS_EVIDENCE_SCHEMA_VERSION = "1.0"
NCCS_ENDPOINT = "/uapi/overseas-stock/v1/trading/inquire-nccs"
CCNL_ENDPOINT = "/uapi/overseas-stock/v1/trading/inquire-ccnl"


def _kis_tr_id_nccs(env: str) -> str:
    return "VTTS3018R" if env == "vps" else "TTTS3018R"


def _kis_tr_id_ccnl(env: str) -> str:
    return "VTTS3035R" if env == "vps" else "TTTS3035R"


def _exchange_status_from_results(
    exchanges: List[str],
    status_by_exchange: Dict[str, str],
) -> str:
    if not status_by_exchange:
        return "FAILED"
    if all(status_by_exchange.get(e) == "FAILED" for e in exchanges):
        return "FAILED"
    if any(status_by_exchange.get(e) == "OK" for e in exchanges):
        return "OK"
    return "EMPTY"

def _mask_account(s: Optional[str]) -> str:
    """계좌/식별자 로그 마스킹(끝 2~3자리만 노출)."""
    if not s:
        return "N/A"
    t = str(s).strip()
    if len(t) <= 3:
        return "***"
    return ("*" * (len(t) - 3)) + t[-3:]


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _safe_decimal(x: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    if x is None or x == "":
        return default
    try:
        return Decimal(str(x).replace(",", "").strip())
    except (InvalidOperation, ValueError, TypeError):
        return default


def _decimal_to_json(val: Optional[Decimal]) -> Any:
    if val is None:
        return None
    # Keep as string for exactness in evidence; float for DB callers via float()
    return str(val)


def _normalize_domestic_order_row(row: Any) -> Optional[Dict[str, Any]]:
    """KIS 국내주식 주문 row(Series/dict)를 내부 표준 dict로 정규화."""
    try:
        order_id = str(row.get("odno", "") or "").strip()
        if not order_id:
            return None
        qty = _safe_int(row.get("ord_qty", 0))
        executed_qty = _safe_int(row.get("tot_ccld_qty", 0))
        ticker = norm_ticker(str(row.get("pdno", "") or ""), os.getenv("MARKET", "SP500"))
        side = "buy" if str(row.get("sll_buy_dvsn_cd", "")) == "02" else "sell"
        order_time = str(row.get("ord_tmd", "") or "")
        cancelled = str(row.get("cncl_yn", "") or "").strip().upper() == "Y"

        if executed_qty <= 0 and cancelled:
            status = "cancelled"
        elif executed_qty <= 0:
            status = "pending"
        elif qty > 0 and executed_qty < qty:
            status = "partial"
        elif qty > 0 and executed_qty >= qty:
            status = "executed"
        else:
            status = "partial"

        return {
            "order_id": order_id,
            "ticker": ticker,
            "side": side,
            "quantity": qty,
            "executed_qty": executed_qty,
            "status": status,
            "cancelled": cancelled,
            "order_time": order_time,
        }
    except Exception:
        return None


def _normalize_overseas_order_row(row: Any) -> Optional[Dict[str, Any]]:
    """KIS 해외주식 주문 row — Decimal 정밀도 유지, CCNL 필드 전체 보존."""
    try:
        order_id = str(row.get("odno", "") or "").strip()
        if not order_id:
            return None

        qty_dec = _safe_decimal(row.get("ft_ord_qty", row.get("ord_qty", 0)), Decimal(0)) or Decimal(0)
        exe_dec = _safe_decimal(row.get("ft_ccld_qty", row.get("tot_ccld_qty", 0)), Decimal(0)) or Decimal(0)
        qty = int(qty_dec)
        executed_qty = int(exe_dec)

        ticker = norm_ticker(str(row.get("pdno", "") or ""), os.getenv("MARKET", "SP500"))
        side_cd = str(row.get("sll_buy_dvsn_cd", row.get("sll_buy_dvsn", "")) or "")
        side_name = str(row.get("sll_buy_dvsn_cd_name", "") or "")
        side = "buy" if side_cd == "02" else "sell"

        order_date = str(row.get("ord_dt", "") or "").strip()
        domestic_order_date = str(row.get("dmst_ord_dt", "") or "").strip()
        order_time = str(
            row.get("ord_tmd") or row.get("thco_ord_tmd") or ""
        ).strip()

        order_price = _safe_decimal(row.get("ft_ord_unpr3", row.get("ord_unpr", 0)))
        executed_price = _safe_decimal(row.get("ft_ccld_unpr3", row.get("avg_unpr", 0)))
        executed_amount = _safe_decimal(row.get("ft_ccld_amt3", 0))
        unfilled_qty_dec = _safe_decimal(row.get("nccs_qty", 0), Decimal(0)) or Decimal(0)

        rvse = str(row.get("rvse_cncl_dvsn", row.get("rvse_cncl_dvsn_cd", "")) or "").strip()
        prcs = str(row.get("prcs_stat_name", "") or "")
        cancelled = rvse in ("02", "2") or ("취소" in prcs and executed_qty <= 0)

        if executed_qty <= 0 and cancelled:
            status = "cancelled"
        elif executed_qty <= 0:
            status = "pending"
        elif qty > 0 and executed_qty < qty:
            status = "partial"
        elif qty > 0 and executed_qty >= qty:
            status = "executed"
        else:
            status = "partial"

        return {
            "order_id": order_id,
            "original_order_id": str(row.get("orgn_odno", "") or "").strip(),
            "order_date": order_date,
            "domestic_order_date": domestic_order_date,
            "order_time": order_time,
            "ticker": ticker,
            "product_name": str(row.get("prdt_name", "") or ""),
            "side": side,
            "side_code": side_cd,
            "side_name": side_name,
            "quantity": qty,
            "order_price": order_price,
            "order_price_str": _decimal_to_json(order_price),
            "executed_qty": executed_qty,
            "executed_price": executed_price,
            "executed_price_str": _decimal_to_json(executed_price),
            "executed_amount": executed_amount,
            "executed_amount_str": _decimal_to_json(executed_amount),
            "unfilled_qty": int(unfilled_qty_dec),
            "status": status,
            "status_name": prcs,
            "rejection_reason": str(row.get("rjct_rson_name", "") or ""),
            "cancelled": cancelled,
            "exchange": str(row.get("ovrs_excg_cd", "") or "").strip().upper(),
            "currency": str(row.get("tr_crcy_cd", "") or "").strip().upper(),
            "media": str(row.get("mdia_dvsn_name", "") or ""),
            "market_name": str(row.get("tr_mket_name", "") or ""),
        }
    except Exception:
        return None


def _normalize_order_row(row: Any, *, overseas: bool = False) -> Optional[Dict[str, Any]]:
    if overseas:
        return _normalize_overseas_order_row(row)
    return _normalize_domestic_order_row(row)


def _fetch_overseas_open_orders(
    kis: KIS, *, ovrs_excg_cd: str = "NASD", env: str = "vps"
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """KIS inquire_nccs(해외 미체결) → order_id dict + endpoint evidence."""
    orders: List[Dict[str, Any]] = []
    exchanges = list(US_BALANCE_EXCHANGES) if is_us_market() else [ovrs_excg_cd]
    failures: List[str] = []
    status_by_exchange: Dict[str, str] = {}
    for exc in exchanges:
        try:
            if hasattr(kis, "inquire_nccs"):
                df = kis.inquire_nccs(ovrs_excg_cd=exc)
                if df is not None and not df.empty:
                    status_by_exchange[exc] = "OK"
                    for _, row in df.iterrows():
                        o = _normalize_order_row(row, overseas=True)
                        if o:
                            o["ovrs_excg_cd"] = exc
                            orders.append(o)
                else:
                    status_by_exchange[exc] = "EMPTY"
        except Exception as e:
            msg = f"{exc}: {e}"
            failures.append(msg)
            status_by_exchange[exc] = "FAILED"
            logger.warning("inquire_nccs(%s) 조회 실패: %s", exc, e)

    if failures and not orders:
        logger.error(
            "KIS_ENDPOINT_UNAVAILABLE: inquire_nccs all exchanges failed (%s)",
            "; ".join(failures)[:400],
        )

    by_id: Dict[str, Dict[str, Any]] = {}
    pending_sell: Dict[str, int] = {}
    for o in orders:
        by_id[o["order_id"]] = o
        if o.get("side") == "sell":
            t = o.get("ticker") or ""
            pending_sell[t] = pending_sell.get(t, 0) + _safe_int(o.get("quantity", 0))

    tr_id = _kis_tr_id_nccs(env)
    all_failed = bool(exchanges) and all(status_by_exchange.get(e) == "FAILED" for e in exchanges)
    evidence = {
        "endpoint": NCCS_ENDPOINT,
        "expected_tr_ids": ["TTTS3018R", "VTTS3018R"],
        "observed_tr_ids": [tr_id],
        "exchange_coverage": [e for e in exchanges if status_by_exchange.get(e) in ("OK", "EMPTY")],
        "status_by_exchange": status_by_exchange,
        "open_orders_count": len(orders),
        "open_sell_orders_count": sum(1 for o in orders if o.get("side") == "sell"),
        "pending_sell_qty_by_ticker": pending_sell,
        "all_exchanges_failed": all_failed,
        "status": _exchange_status_from_results(exchanges, status_by_exchange),
    }
    return by_id, evidence


def _classify_kis_order_status(
    kis_o: Dict[str, Any],
    *,
    in_open_orders: bool,
) -> str:
    """inquire-nccs / inquire-ccnl 기준 pending/submitted/filled/failed/canceled."""
    executed_qty = _safe_int(kis_o.get("executed_qty", 0))
    qty = _safe_int(kis_o.get("quantity", 0))
    cancelled = bool(kis_o.get("cancelled"))

    if cancelled and executed_qty <= 0:
        return "canceled"
    if executed_qty <= 0 and in_open_orders:
        return "pending"
    if executed_qty <= 0:
        return "submitted"
    if qty > 0 and executed_qty < qty:
        return "partial"
    if executed_qty > 0:
        return "filled"
    return "failed"


def log_failed_sell_diagnostic(
    *,
    ticker: str,
    requested_qty: int,
    sellable_qty: int = 0,
    pending_sell_qty: int = 0,
    msg_cd: Optional[str] = None,
    msg1: Optional[str] = None,
) -> None:
    """failed SELL 진단 (order_id 없는 failed SELL은 DB INSERT하지 않는 정책과 별도 로그)."""
    logger.error(
        "[SELL_REJECT_DIAG] reconciler ticker=%s requested_qty=%s sellable_qty=%s "
        "pending_sell_qty=%s msg_cd=%s msg1=%s",
        ticker,
        requested_qty,
        sellable_qty,
        pending_sell_qty,
        msg_cd,
        msg1,
    )


def _fetch_overseas_daily_orders(
    kis: KIS, *, start_ymd: str, end_ymd: str, ovrs_excg_cd: str = "NASD", env: str = "vps"
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """KIS inquire_ccnl(해외 주문체결내역) → order_id dict + endpoint evidence."""
    orders: List[Dict[str, Any]] = []
    exchanges = list(US_BALANCE_EXCHANGES) if is_us_market() else [ovrs_excg_cd]
    failures: List[str] = []
    status_by_exchange: Dict[str, str] = {}
    filled = partial = canceled = failed = 0
    for exc in exchanges:
        try:
            if hasattr(kis, "inquire_ccnl"):
                df = kis.inquire_ccnl(
                    ord_strt_dt=start_ymd,
                    ord_end_dt=end_ymd,
                    ovrs_excg_cd=exc,
                )
                if df is not None and not df.empty:
                    status_by_exchange[exc] = "OK"
                    for _, row in df.iterrows():
                        o = _normalize_order_row(row, overseas=True)
                        if o:
                            o["ovrs_excg_cd"] = exc
                            orders.append(o)
                            exe = _safe_int(o.get("executed_qty", 0))
                            qty = _safe_int(o.get("quantity", 0))
                            if o.get("cancelled") and exe <= 0:
                                canceled += 1
                            elif exe <= 0:
                                failed += 1
                            elif qty > 0 and exe < qty:
                                partial += 1
                            elif exe > 0:
                                filled += 1
                else:
                    status_by_exchange[exc] = "EMPTY"
        except Exception as e:
            msg = f"{exc}: {e}"
            failures.append(msg)
            status_by_exchange[exc] = "FAILED"
            logger.warning("inquire_ccnl(%s) 조회 실패: %s", exc, e)

    if failures and not orders:
        logger.error(
            "KIS_ENDPOINT_UNAVAILABLE: inquire_ccnl all exchanges failed (%s~%s) %s",
            start_ymd,
            end_ymd,
            "; ".join(failures)[:400],
        )

    by_id: Dict[str, Dict[str, Any]] = {}
    raw_row_count = len(orders)
    for o in orders:
        oid = o["order_id"]
        # Prefer row with higher executed_qty when duplicates
        prev = by_id.get(oid)
        if prev is None or _safe_int(o.get("executed_qty", 0)) >= _safe_int(prev.get("executed_qty", 0)):
            by_id[oid] = o

    unique_order_count = len(by_id)
    duplicate_order_id_count = max(0, raw_row_count - unique_order_count)
    filled_unique = sum(
        1 for o in by_id.values()
        if _safe_int(o.get("executed_qty", 0)) > 0 and not o.get("cancelled")
    )

    tr_id = _kis_tr_id_ccnl(env)
    all_failed = bool(exchanges) and all(status_by_exchange.get(e) == "FAILED" for e in exchanges)
    evidence = {
        "endpoint": CCNL_ENDPOINT,
        "expected_tr_ids": ["TTTS3035R", "VTTS3035R"],
        "observed_tr_ids": [tr_id],
        "exchange_coverage": [e for e in exchanges if status_by_exchange.get(e) in ("OK", "EMPTY")],
        "status_by_exchange": status_by_exchange,
        # order_count kept for backward compat = raw rows
        "order_count": raw_row_count,
        "raw_row_count": raw_row_count,
        "unique_order_count": unique_order_count,
        "duplicate_order_id_count": duplicate_order_id_count,
        "filled_unique_order_count": filled_unique,
        "filled_order_count": filled,
        "partial_order_count": partial,
        "canceled_order_count": canceled,
        "failed_order_count": failed,
        "all_exchanges_failed": all_failed,
        "status": _exchange_status_from_results(exchanges, status_by_exchange),
        "query_start_date": start_ymd,
        "query_end_date": end_ymd,
    }
    return by_id, evidence


def _fetch_open_orders_inquire_orders(
    kis: KIS, *, start_ymd: Optional[str] = None, end_ymd: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    KIS inquire_orders(미체결/부분체결) 조회 결과를 order_id 기준으로 정규화해서 반환.
    반환: {order_id: {order_id,ticker,side,quantity,executed_qty,status,order_time}}

    - start_ymd/end_ymd로 조회 창을 지정한다. 미지정 시 오늘 하루로 폴백한다.
      (전일 이월 pending 주문을 잡으려면 reconcile 대상 행 범위와 동일하게 넘겨야 한다)
    """
    orders: List[Dict[str, Any]] = []
    today = datetime.now(KST).strftime("%Y%m%d")
    strt = start_ymd or today
    end = end_ymd or today

    # 1) inquire_orders() 기반(open orders) 우선
    # - 이 API는 "미체결/부분체결" 중심이라 executed/cancelled 확정은 보완이 필요할 수 있다.
    try:
        df = None
        if hasattr(kis, "inquire_orders"):
            df = kis.inquire_orders(
                inqr_dvsn="00",
                inqr_strt_ymd=strt,
                inqr_end_ymd=end,
                sll_buy_dvsn_cd="00",
                inqr_dvsn_cd="00",
            )
        elif hasattr(kis, "get_pending_orders"):
            # 하위 호환
            df = kis.get_pending_orders()

        if df is not None and not df.empty:
            for _, row in df.iterrows():
                o = _normalize_order_row(row)
                if o:
                    orders.append(o)
    except Exception as e:
        logger.debug(f"inquire_orders() 조회 실패: {e}")

    # 마지막 값으로 덮어쓰기(시간 역순/중복 가능)
    by_id: Dict[str, Dict[str, Any]] = {}
    for o in orders:
        by_id[o["order_id"]] = o
    _db_dbg_log(
        "reconciler.fetch_today_orders.OK",
        inqr_strt_ymd=strt,
        inqr_end_ymd=end,
        raw_rows=len(orders),
        unique_order_ids=len(by_id),
        sample_ids=list(by_id.keys())[:8],
    )
    return by_id


def _fetch_open_orders(
    kis: KIS, *, start_ymd: Optional[str] = None, end_ymd: Optional[str] = None, env: str = "vps"
) -> Tuple[Dict[str, Dict[str, Any]], Optional[Dict[str, Any]]]:
    """MARKET에 따라 KIS 미체결 주문 조회."""
    if is_us_market():
        by_id, evidence = _fetch_overseas_open_orders(kis, env=env)
        _db_dbg_log(
            "reconciler.fetch_overseas_nccs.OK",
            api="inquire-nccs",
            raw_rows=len(by_id),
            unique_order_ids=len(by_id),
            sample_ids=list(by_id.keys())[:8],
        )
        return by_id, evidence
    return _fetch_open_orders_inquire_orders(kis, start_ymd=start_ymd, end_ymd=end_ymd), None


def _fetch_daily_orders(
    kis: KIS, *, start_ymd: str, end_ymd: Optional[str] = None,
    since_dt: Optional[datetime] = None, until_dt: Optional[datetime] = None,
    env: str = "vps",
) -> Tuple[Dict[str, Dict[str, Any]], Optional[Dict[str, Any]]]:
    """MARKET에 따라 KIS 일자별 주문 조회."""
    end = end_ymd or start_ymd
    if is_us_market():
        if since_dt is not None and until_dt is not None:
            start_ymd, end = kst_window_to_us_order_dates(since_dt, until_dt)
        return _fetch_overseas_daily_orders(kis, start_ymd=start_ymd, end_ymd=end, env=env)
    return _fetch_daily_orders_domestic(kis, start_ymd=start_ymd, end_ymd=end), None


def _fetch_daily_orders_domestic(
    kis: KIS, *, start_ymd: str, end_ymd: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    KIS inquire_daily_order(일자별 주문 전체) 결과를 order_id 기준으로 정규화해서 반환.
    - executed/cancelled 확정 보완용(누락 order_id가 있을 때만 호출 권장)
    - start_ymd~end_ymd 범위로 조회한다(전일 이월 주문 포함).
    """
    orders: List[Dict[str, Any]] = []
    end = end_ymd or start_ymd
    try:
        df = kis.inquire_daily_order(
            cano=kis.cano,
            acnt_prdt_cd=kis.acnt_prdt_cd,
            inqr_strt_dt=start_ymd,
            inqr_end_dt=end,
            sll_buy_dvsn_cd="00",
            inqr_dvsn="00",
            sort_ord="2",
            ord_gnno_yn="N",
            odno="",
            inqr_dvsn_3="00",
            inqr_dvsn_1="",
            tot_ccld_qty_smtl_yn="N",
        )
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                o = _normalize_order_row(row)
                if o:
                    orders.append(o)
    except Exception as e:
        logger.warning(f"inquire_daily_order() 조회 실패: {e}")

    by_id: Dict[str, Dict[str, Any]] = {}
    for o in orders:
        by_id[o["order_id"]] = o
    return by_id


def _db_action_to_kis_side(action: str) -> str:
    """trade_records.action → KIS 기준 side ('buy'/'sell')."""
    a = str(action or "").strip().upper()
    if a in ("BUY", "B"):
        return "buy"
    if a in ("SELL", "S"):
        return "sell"
    return ""


def _row_target_qty(row: Dict[str, Any]) -> int:
    rq = _safe_int(row.get("requested_qty", 0))
    if rq > 0:
        return rq
    return _safe_int(row.get("quantity", 0))


def _match_kis_candidates(
    row: Dict[str, Any],
    daily_orders: Dict[str, Dict[str, Any]],
    *,
    used_order_ids: set,
) -> List[Dict[str, Any]]:
    """DB orphan 행에 대응하는 KIS daily 주문 후보(0~N)."""
    ticker = norm_ticker(str(row.get("ticker") or ""), os.getenv("MARKET", "SP500"))
    side = _db_action_to_kis_side(row.get("action") or "")
    target_qty = _row_target_qty(row)
    if not ticker or not side or target_qty <= 0:
        return []

    candidates: List[Dict[str, Any]] = []
    for oid, kis_o in daily_orders.items():
        if oid in used_order_ids:
            continue
        if norm_ticker(str(kis_o.get("ticker") or ""), os.getenv("MARKET", "SP500")) != ticker:
            continue
        if str(kis_o.get("side") or "") != side:
            continue
        kis_qty = _safe_int(kis_o.get("quantity", 0))
        if kis_qty != target_qty:
            continue
        kis_exe = _safe_int(kis_o.get("executed_qty", 0))
        db_status = str(row.get("order_status") or "").lower()
        if db_status in ("executed", "completed") and kis_exe <= 0:
            continue
        candidates.append(kis_o)
    return candidates


def backfill_orphan_order_ids(*, since_hours: int = 24, limit: int = 200) -> Dict[str, int]:
    """
    order_id가 비어 있는 DB 행을 KIS 일자별 주문과 매칭해 backfill.
    유일 매칭(1건)일 때만 UPDATE.
    """
    setup_logging()
    env = settings._config.get("trading_environment", "vps")
    kis = KIS(env=env)
    recorder = get_recorder()

    now_kst = datetime.now(KST)
    since_dt = now_kst - timedelta(hours=since_hours)
    since_ts = since_dt.isoformat()
    start_ymd = since_dt.strftime("%Y%m%d")
    end_ymd = now_kst.strftime("%Y%m%d")

    orphans = recorder.get_orphan_trade_records(since_ts=since_ts, limit=limit)
    logger.info(f"orphan backfill 대상 {len(orphans)}건 (since={since_ts})")

    daily_orders, _ = _fetch_daily_orders(
        kis, start_ymd=start_ymd, end_ymd=end_ymd, since_dt=since_dt, until_dt=now_kst, env=env,
    )
    logger.info(f"KIS 일자별 주문(daily) backfill 조회: {len(daily_orders)}건 ({start_ymd}~{end_ymd})")

    updated = 0
    skipped_ambiguous = 0
    skipped_no_match = 0
    used_order_ids = set()

    for row in orphans:
        row_id = row.get("id")
        candidates = _match_kis_candidates(row, daily_orders, used_order_ids=used_order_ids)
        if len(candidates) == 0:
            skipped_no_match += 1
            continue
        if len(candidates) > 1:
            skipped_ambiguous += 1
            continue

        kis_o = candidates[0]
        order_id = str(kis_o.get("order_id") or "").strip()
        kis_exe = _safe_int(kis_o.get("executed_qty", 0))
        kis_status = str(kis_o.get("status") or "executed")
        if kis_status == "executed" and kis_exe <= 0:
            kis_exe = _row_target_qty(row)

        n = recorder.backfill_order_id(
            row_id=int(row_id),
            order_id=order_id,
            executed_qty=kis_exe if kis_exe > 0 else None,
            order_status=kis_status,
        )
        if n:
            updated += n
            used_order_ids.add(order_id)

    summary = {
        "backfill_orphans": len(orphans),
        "backfill_updated": updated,
        "backfill_skipped_ambiguous": skipped_ambiguous,
        "backfill_skipped_no_match": skipped_no_match,
        "kis_daily_orders": len(daily_orders),
    }
    logger.info(f"orphan backfill 결과: {summary}")
    return summary


def order_reconcile_evidence_paths(
    market: str,
    trade_date: str,
    output_dir: Optional[Path] = None,
) -> Tuple[Path, Path]:
    base = output_dir or OUTPUT_DIR
    base.mkdir(parents=True, exist_ok=True)
    dated = base / f"order_reconcile_{market}_{trade_date}.json"
    latest = base / f"order_reconcile_latest_{market}.json"
    return dated, latest


def get_db_order_ids(recorder=None) -> Set[str]:
    """All non-empty order_id values currently in trade_records."""
    rec = recorder or get_recorder()
    ids: Set[str] = set()
    try:
        if hasattr(rec, "get_known_order_ids"):
            return set(rec.get_known_order_ids())
        import sqlite3
        with sqlite3.connect(rec.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT order_id FROM trade_records "
                "WHERE order_id IS NOT NULL AND TRIM(order_id) != ''"
            )
            for (oid,) in cur.fetchall():
                s = str(oid or "").strip()
                if s:
                    ids.add(s)
    except Exception as e:
        logger.warning("get_db_order_ids failed: %s", e)
    return ids


def detect_broker_only_orders(
    kis_orders: Dict[str, Dict[str, Any]],
    db_order_ids: Optional[Set[str]] = None,
    *,
    recorder=None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    KIS CCNL unique order_id − DB trade_records order_id = broker_only.
    Returns (broker_only_orders, findings). Does NOT mutate DB.
    """
    known = db_order_ids if db_order_ids is not None else get_db_order_ids(recorder)
    broker_only: List[Dict[str, Any]] = []
    findings: List[Dict[str, Any]] = []

    for oid, kis_o in (kis_orders or {}).items():
        if oid in known:
            continue
        broker_only.append(kis_o)
        findings.append({
            "title": "BROKER_TRADE_MISSING_IN_DB",
            "severity": "ERROR",
            "category": "DATA_INTEGRITY",
            "details": {
                "order_id": oid,
                "order_date": kis_o.get("order_date"),
                "order_time": kis_o.get("order_time"),
                "ticker": kis_o.get("ticker"),
                "product_name": kis_o.get("product_name"),
                "side": kis_o.get("side"),
                "quantity": kis_o.get("quantity"),
                "executed_qty": kis_o.get("executed_qty"),
                "order_price": _decimal_to_json(kis_o.get("order_price"))
                if isinstance(kis_o.get("order_price"), Decimal)
                else kis_o.get("order_price_str") or kis_o.get("order_price"),
                "executed_price": _decimal_to_json(kis_o.get("executed_price"))
                if isinstance(kis_o.get("executed_price"), Decimal)
                else kis_o.get("executed_price_str") or kis_o.get("executed_price"),
                "executed_amount": _decimal_to_json(kis_o.get("executed_amount"))
                if isinstance(kis_o.get("executed_amount"), Decimal)
                else kis_o.get("executed_amount_str") or kis_o.get("executed_amount"),
                "exchange": kis_o.get("exchange") or kis_o.get("ovrs_excg_cd"),
                "currency": kis_o.get("currency"),
                "media": kis_o.get("media"),
                "db_lookup_result": "missing",
            },
        })
    return broker_only, findings


def _backfill_eligibility(kis_o: Dict[str, Any]) -> Tuple[bool, str]:
    """All conditions required for safe --backfill-broker-only upsert."""
    if str(kis_o.get("status") or "").lower() != "executed":
        return False, "status_not_executed"
    if not str(kis_o.get("order_id") or "").strip():
        return False, "missing_order_id"
    if not str(kis_o.get("ticker") or "").strip():
        return False, "missing_ticker"
    if not str(kis_o.get("side") or "").strip():
        return False, "missing_side"
    exe_qty = _safe_int(kis_o.get("executed_qty", 0))
    if exe_qty <= 0:
        return False, "executed_qty_le_0"
    exe_px = kis_o.get("executed_price")
    if isinstance(exe_px, Decimal):
        ok_px = exe_px > 0
        px_val = exe_px
    else:
        px_val = _safe_decimal(exe_px, Decimal(0)) or Decimal(0)
        ok_px = px_val > 0
    if not ok_px:
        return False, "executed_price_le_0"
    if not str(kis_o.get("order_date") or "").strip():
        return False, "missing_order_date"
    return True, ""


def _gross_pnl_for_backfill(
    recorder,
    *,
    ticker: str,
    side: str,
    qty: int,
    executed_price: Decimal,
) -> Optional[Decimal]:
    """Gross P&L only (no commission/tax estimate). SELL vs last BUY."""
    if side != "sell":
        return None
    try:
        trades = recorder.get_trade_records(ticker=ticker)
        buys = [t for t in trades if str(t.action).upper() == "BUY" and (t.quantity or 0) > 0]
        if not buys:
            return None
        last_buy = buys[-1]
        buy_px = Decimal(str(last_buy.price or 0))
        if buy_px <= 0:
            return None
        return (executed_price - buy_px) * Decimal(qty)
    except Exception:
        return None


def backfill_broker_only_orders(
    broker_only: List[Dict[str, Any]],
    *,
    recorder=None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Idempotent upsert for eligible broker-only executed orders.
    Incomplete rows → BROKER_TRADE_BACKFILL_INCOMPLETE, no DB change.
    """
    rec = recorder or get_recorder()
    now_kst = datetime.now(KST).isoformat()
    inserted = 0
    updated = 0
    skipped_incomplete = 0
    incomplete_findings: List[Dict[str, Any]] = []
    backfilled_ids: List[str] = []

    for kis_o in broker_only:
        ok, reason = _backfill_eligibility(kis_o)
        if not ok:
            skipped_incomplete += 1
            incomplete_findings.append({
                "title": "BROKER_TRADE_BACKFILL_INCOMPLETE",
                "severity": "ERROR",
                "category": "DATA_INTEGRITY",
                "details": {
                    "order_id": kis_o.get("order_id"),
                    "ticker": kis_o.get("ticker"),
                    "reason": reason,
                    "status": kis_o.get("status"),
                    "executed_qty": kis_o.get("executed_qty"),
                    "executed_price": kis_o.get("executed_price_str") or str(kis_o.get("executed_price")),
                    "order_date": kis_o.get("order_date"),
                },
            })
            logger.warning(
                "[BROKER_TRADE_BACKFILL_INCOMPLETE] order_id=%s reason=%s — DB unchanged",
                kis_o.get("order_id"),
                reason,
            )
            continue

        exe_px = kis_o.get("executed_price")
        if not isinstance(exe_px, Decimal):
            exe_px = _safe_decimal(exe_px, Decimal(0)) or Decimal(0)
        exe_amt = kis_o.get("executed_amount")
        if not isinstance(exe_amt, Decimal):
            exe_amt = _safe_decimal(exe_amt)
        qty = _safe_int(kis_o.get("executed_qty") or kis_o.get("quantity") or 0)
        side = str(kis_o.get("side") or "").lower()
        ticker = str(kis_o.get("ticker") or "")
        order_id = str(kis_o.get("order_id") or "").strip()

        gross = _gross_pnl_for_backfill(
            rec, ticker=ticker, side=side, qty=qty, executed_price=exe_px,
        )
        structured = {
            "source": "order_reconciler",
            "backfilled_from": "kis_ccnl",
            "broker_only": True,
            "media": kis_o.get("media") or "OpenAPI",
            "recovered_at_kst": now_kst,
            "gross_pnl_basis": True,
            "net_pnl_complete": False,
            "order_date": kis_o.get("order_date"),
            "order_time": kis_o.get("order_time"),
            "exchange": kis_o.get("exchange"),
            "currency": kis_o.get("currency") or "USD",
            "executed_amount": _decimal_to_json(exe_amt) if exe_amt is not None else None,
            "gross_pnl": _decimal_to_json(gross) if gross is not None else None,
            "commission_known": False,
            "tax_known": False,
            # Do NOT invent strategy type (e.g. PartialProfit) when unknown
        }

        # profit_loss: store gross only with explicit flag; never fake net
        pnl_value = float(gross) if gross is not None else 0.0

        payload = {
            "side": side,
            "ticker": ticker,
            "name": kis_o.get("product_name") or ticker,
            "qty": qty,
            "price": float(exe_px),
            "trade_status": "executed",
            "order_id": order_id,
            "requested_qty": _safe_int(kis_o.get("quantity") or qty),
            "executed_qty": qty,
            "pnl_amount": pnl_value if side == "sell" else None,
            "structured_context": structured,
            "strategy_details": structured,
            "reason_code": "BROKER_ONLY_BACKFILL",
            "_debug_context": "order_reconciler.backfill_broker_only",
        }

        if dry_run:
            logger.info(
                "[BACKFILL_DRY_RUN] would upsert order_id=%s ticker=%s side=%s qty=%s px=%s gross=%s",
                order_id,
                ticker,
                side,
                qty,
                exe_px,
                gross,
            )
            backfilled_ids.append(order_id)
            continue

        # Detect insert vs update against the target recorder
        known_before = order_id in get_db_order_ids(rec)

        from recorder import TradeRecord
        import json as _json

        # Build TradeRecord and upsert on the SAME recorder instance
        try:
            structured_json = _json.dumps(structured, ensure_ascii=False)
        except Exception:
            structured_json = str(structured)

        tr = TradeRecord(
            timestamp=datetime.now(KST),
            ticker=ticker,
            action=side.upper(),
            quantity=qty,
            price=float(exe_px),
            amount=float(exe_amt) if exe_amt is not None else float(exe_px) * qty,
            commission=0.0,
            tax=0.0,
            total_cost=float(exe_amt) if exe_amt is not None else float(exe_px) * qty,
            net_amount=float(exe_amt) if exe_amt is not None else float(exe_px) * qty,
            profit_loss=pnl_value if side == "sell" else 0.0,
            holding_period_days=0,
            order_status="executed",
            order_id=order_id,
            requested_qty=_safe_int(kis_o.get("quantity") or qty),
            executed_qty=qty,
            last_status_update_ts=now_kst,
            reason_code="BROKER_ONLY_BACKFILL",
            structured_context=structured_json,
        )
        db_ok = bool(rec.upsert_trade_record_by_order_id(tr))
        if not db_ok:
            incomplete_findings.append({
                "title": "BROKER_TRADE_BACKFILL_INCOMPLETE",
                "severity": "ERROR",
                "category": "DATA_INTEGRITY",
                "details": {"order_id": order_id, "reason": "upsert_failed"},
            })
            continue
        if known_before:
            updated += 1
        else:
            inserted += 1
        backfilled_ids.append(order_id)
        try:
            from broker_order_persist import clear_persist_failure_lock
            clear_persist_failure_lock(ticker, side.upper(), order_id)
        except Exception:
            pass

    return {
        "backfill_inserted": inserted,
        "backfill_updated": updated,
        "backfill_skipped_incomplete": skipped_incomplete,
        "backfill_order_ids": backfilled_ids,
        "incomplete_findings": incomplete_findings,
        "dry_run": dry_run,
    }


def detect_and_optionally_backfill_broker_only(
    daily_orders: Dict[str, Dict[str, Any]],
    *,
    backfill: bool = False,
    dry_run: bool = False,
    recorder=None,
) -> Dict[str, Any]:
    rec = recorder or get_recorder()
    broker_only, findings = detect_broker_only_orders(daily_orders, recorder=rec)
    result: Dict[str, Any] = {
        "broker_only_count": len(broker_only),
        "broker_only_order_ids": [o.get("order_id") for o in broker_only],
        "findings": findings,
    }
    if backfill and broker_only:
        bf = backfill_broker_only_orders(broker_only, recorder=rec, dry_run=dry_run)
        result.update(bf)
        result["findings"] = findings + list(bf.get("incomplete_findings") or [])
        # Refresh detect after backfill
        if not dry_run:
            remaining, remaining_findings = detect_broker_only_orders(
                daily_orders, recorder=rec,
            )
            result["broker_only_remaining"] = len(remaining)
            result["findings_after_backfill"] = remaining_findings
    return result


def save_order_reconcile_evidence(

    payload: Dict[str, Any],
    *,
    market: str,
    trade_date: str,
    output_dir: Optional[Path] = None,
) -> Optional[Path]:
    try:
        dated_path, latest_path = order_reconcile_evidence_paths(market, trade_date, output_dir)
        for path in (dated_path, latest_path):
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info("[ORDER_RECONCILE_EVIDENCE_SAVED] path=%s", dated_path)
        return dated_path
    except Exception as e:
        logger.error("[ORDER_RECONCILE_EVIDENCE_SAVE_FAILED] reason=%s", str(e)[:300])
        return None


def reconcile_open_orders(
    *,
    since_hours: int = 24,
    limit: int = 500,
    backfill_broker_only: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    DB open(pending/partial) 주문을 KIS 조회 결과로 리컨실.
    또한 KIS CCNL − DB order_id = broker_only 검출 (기본: DB 미수정).
    --backfill-broker-only 시 자격 충족 executed 주문만 idempotent upsert.
    """
    setup_logging()
    logger.setLevel(logging.INFO)

    env = settings._config.get("trading_environment", "vps")
    kis = KIS(env=env)
    recorder = get_recorder()

    now_kst = datetime.now(KST)
    since_dt = now_kst - timedelta(hours=since_hours)
    since_ts = since_dt.isoformat()
    # KIS 조회 창: reconcile 대상(DB) 행 범위와 동일하게 since_hours 시작일 ~ 오늘.
    # (오늘 하루로 고정하면 전일 이월 pending 주문이 영원히 해소되지 않음)
    start_ymd = since_dt.strftime("%Y%m%d")
    end_ymd = now_kst.strftime("%Y%m%d")
    _db_dbg_log(
        "reconciler.start",
        since_hours=since_hours,
        since_ts=since_ts,
        kis_inqr_strt_ymd=start_ymd,
        kis_inqr_end_ymd=end_ymd,
        limit=limit,
        env=env,
        kis_api_family="overseas" if is_us_market() else "domestic",
        kis_cano=_mask_account(getattr(kis, "cano", None)),
        kis_acnt_prdt_cd=str(getattr(kis, "acnt_prdt_cd", "") or ""),
        kis_url_base=str(getattr(kis, "url_base", "") or "")[:80],
    )
    if _db_dbg_enabled():
        try:
            import sqlite3
            with sqlite3.connect(recorder.db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT COUNT(*) FROM trade_records
                    WHERE timestamp >= ? AND lower(order_status) IN ('pending','partial')
                      AND (order_id IS NULL OR order_id = '')
                    """,
                    (since_ts,),
                )
                orphan = int(cur.fetchone()[0] or 0)
            _db_dbg_log(
                "reconciler.orphan_pending_without_order_id",
                count=orphan,
                note="these rows are invisible to get_open_orders / reconciler loop",
            )
        except Exception as e:
            _db_dbg_log("reconciler.orphan_count.FAIL", error=str(e))
    open_rows = recorder.get_open_orders(statuses=("pending", "partial"), since_ts=since_ts, limit=limit)
    logger.info(f"리컨실 대상(open) {len(open_rows)}건 (since={since_ts})")

    today = end_ymd
    open_orders, nccs_evidence = _fetch_open_orders(
        kis, start_ymd=start_ymd, end_ymd=end_ymd, env=env,
    )
    open_api = "inquire_nccs" if is_us_market() else "inquire_orders"
    logger.info(
        f"KIS 미체결({open_api}) 조회: {len(open_orders)}건 "
        f"({start_ymd}~{end_ymd})"
    )
    daily_orders: Optional[Dict[str, Dict[str, Any]]] = None
    ccnl_evidence: Optional[Dict[str, Any]] = None

    updated = 0
    to_executed = 0
    to_partial = 0
    to_pending = 0
    to_cancelled = 0
    skipped_no_order_id = 0
    skipped_kis_miss = 0
    resolved_by_daily = 0
    still_missing_after_daily = 0

    for r in open_rows:
        order_id = str(r.get("order_id") or "").strip()
        if not order_id:
            skipped_no_order_id += 1
            _db_dbg_skip(
                "reconciler.loop.SKIP_NO_ORDER_ID",
                reason="get_open_orders only returns rows with order_id; this should not happen",
                row_id=r.get("id"),
                ticker=r.get("ticker"),
            )
            continue

        # 1) inquire_orders 결과로 먼저 매칭
        kis_o = open_orders.get(order_id)
        # 2) 누락이면 daily를 1회만 로드해서 보완
        if not kis_o:
            if daily_orders is None:
                daily_orders, ccnl_evidence = _fetch_daily_orders(
                    kis,
                    start_ymd=start_ymd,
                    end_ymd=end_ymd,
                    since_dt=since_dt,
                    until_dt=now_kst,
                    env=env,
                )
                daily_api = "inquire_ccnl" if is_us_market() else "inquire_daily_order"
                logger.info(
                    f"KIS 일자별 주문({daily_api}) 보완 조회: {len(daily_orders)}건 "
                    f"({start_ymd}~{end_ymd})"
                )
            kis_o = (daily_orders or {}).get(order_id)

        if not kis_o:
            skipped_kis_miss += 1
            if daily_orders is None:
                _db_dbg_skip(
                    "reconciler.loop.SKIP_KIS_MISS_OPEN_ONLY",
                    reason="order_id not in inquire_orders (daily not fetched because no misses trigger?)",
                    order_id=order_id,
                    ticker=r.get("ticker"),
                    db_status=r.get("order_status"),
                )
            else:
                still_missing_after_daily += 1
                _db_dbg_skip(
                    "reconciler.loop.SKIP_KIS_MISS_AFTER_DAILY",
                    reason="order_id not in open_orders nor daily_orders",
                    order_id=order_id,
                    ticker=r.get("ticker"),
                    db_status=r.get("order_status"),
                )
                _db_dbg_log(
                    "reconciler.miss_after_daily.DIAG",
                    hint="common causes: (1) wrong env(prod/vps) (2) different account/product (3) US ET vs KST date boundary (4) order_id not saved correctly (5) domestic API used for overseas market",
                    env=env,
                    today=today,
                    kis_cano=_mask_account(getattr(kis, "cano", None)),
                    kis_acnt_prdt_cd=str(getattr(kis, "acnt_prdt_cd", "") or ""),
                    order_id=order_id,
                    db_row_id=r.get("id"),
                    db_ts=r.get("timestamp"),
                    db_ticker=r.get("ticker"),
                    db_side=r.get("side") or r.get("action"),
                    db_status=r.get("order_status"),
                    db_requested_qty=r.get("requested_qty"),
                    db_executed_qty=r.get("executed_qty"),
                )
            continue
        else:
            # daily에서 해결된 케이스 카운트(=open에 없고 daily에만 있었던 것)
            if daily_orders is not None and order_id not in open_orders:
                resolved_by_daily += 1

        requested_qty = _safe_int(r.get("requested_qty", 0))
        kis_qty = _safe_int(kis_o.get("quantity", 0))
        kis_exe = _safe_int(kis_o.get("executed_qty", 0))
        # requested_qty가 0이면 KIS ord_qty로 보정
        if requested_qty <= 0 and kis_qty > 0:
            requested_qty = kis_qty

        if kis_exe <= 0 and kis_o.get("cancelled"):
            new_status = "cancelled"
        elif kis_exe <= 0:
            new_status = "pending"
        elif requested_qty > 0 and kis_exe < requested_qty:
            new_status = "partial"
        else:
            new_status = "executed"

        kis_class = _classify_kis_order_status(
            kis_o,
            in_open_orders=order_id in open_orders,
        )
        if kis_class == "canceled":
            new_status = "cancelled"
        elif kis_class == "filled":
            new_status = "executed"
        elif kis_class == "partial":
            new_status = "partial"
        elif kis_class in ("pending", "submitted"):
            new_status = "pending"

        db_side = _db_action_to_kis_side(r.get("action") or r.get("side"))
        if (
            db_side == "sell"
            and new_status in ("pending", "cancelled", "executed")
            and kis_exe <= 0
            and str(r.get("order_status") or "").lower() == "failed"
        ):
            log_failed_sell_diagnostic(
                ticker=str(r.get("ticker") or ""),
                requested_qty=requested_qty,
                msg1="reconciler: failed sell with zero fill",
            )

        _db_dbg_log(
            "reconciler.loop.MATCH",
            order_id=order_id,
            ticker=r.get("ticker"),
            db_status=r.get("order_status"),
            new_status=new_status,
            requested_qty=requested_qty,
            kis_exe=kis_exe,
        )
        n = recorder.update_order_status(
            order_id=order_id,
            order_status=new_status,
            executed_qty=kis_exe,
        )
        if n:
            updated += n
            if new_status == "executed":
                to_executed += n
            elif new_status == "partial":
                to_partial += n
            elif new_status == "cancelled":
                to_cancelled += n
            else:
                to_pending += n

    summary = {
        "updated": updated,
        "to_executed": to_executed,
        "to_partial": to_partial,
        "to_pending": to_pending,
        "to_cancelled": to_cancelled,
        "skipped_no_order_id": skipped_no_order_id,
        "skipped_kis_miss": skipped_kis_miss,
        "open_rows_with_order_id": len(open_rows),
        "kis_open_orders_inquire_orders": len(open_orders),
        "kis_daily_orders_fetched": 0 if daily_orders is None else len(daily_orders),
        "resolved_by_daily": resolved_by_daily,
        "still_missing_after_daily": still_missing_after_daily,
    }
    logger.info(f"리컨실 결과: {summary}")
    _db_dbg_log("reconciler.done", **summary)
    if still_missing_after_daily > 0:
        logger.warning(
            "리컨실 미해결 주문이 있습니다(still_missing_after_daily=%s). "
            "env/계좌/날짜경계/DB order_id 저장 여부를 점검하세요.",
            still_missing_after_daily,
        )
    if _db_dbg_enabled() and skipped_kis_miss == 0 and updated == 0 and open_rows:
        _db_dbg_skip(
            "reconciler.HINT_ORPHAN_PENDING",
            reason="open_rows>0 but updated=0; check recorder.get_open_orders orphan_pending_no_order_id count",
        )
    backfill_summary = backfill_orphan_order_ids(since_hours=since_hours, limit=min(limit, 200))
    summary.update(backfill_summary)
    logger.info(f"리컨실+backfill 통합 결과: {summary}")
    _db_dbg_log("reconciler.done_with_backfill", **summary)

    if is_us_market():
        if ccnl_evidence is None or daily_orders is None:
            daily_orders, ccnl_evidence = _fetch_daily_orders(
                kis,
                start_ymd=start_ymd,
                end_ymd=end_ymd,
                since_dt=since_dt,
                until_dt=now_kst,
                env=env,
            )
        market = os.getenv("MARKET", "SP500")
        db_with_id = [
            r for r in open_rows if str(r.get("order_id") or "").strip()
        ]
        matched = 0
        if daily_orders and db_with_id:
            matched = sum(
                1 for r in db_with_id if str(r.get("order_id")) in daily_orders
            )
        coverage = round(matched / len(db_with_id), 4) if db_with_id else 1.0

        broker_result = detect_and_optionally_backfill_broker_only(
            daily_orders or {},
            backfill=backfill_broker_only,
            dry_run=dry_run,
            recorder=recorder,
        )
        summary["broker_only_count"] = broker_result.get("broker_only_count", 0)
        summary["broker_only_order_ids"] = broker_result.get("broker_only_order_ids", [])
        if backfill_broker_only:
            summary["backfill_broker_inserted"] = broker_result.get("backfill_inserted", 0)
            summary["backfill_broker_updated"] = broker_result.get("backfill_updated", 0)
            summary["backfill_broker_skipped_incomplete"] = broker_result.get(
                "backfill_skipped_incomplete", 0
            )
            summary["broker_only_remaining"] = broker_result.get(
                "broker_only_remaining",
                broker_result.get("broker_only_count", 0),
            )

        evidence_payload = {
            "schema_version": KIS_EVIDENCE_SCHEMA_VERSION,
            "source": "kis_endpoint",
            "market": market,
            "trade_date": end_ymd,
            "evidence_trade_date": end_ymd,
            "generated_at_kst": now_kst.isoformat(),
            "query_start_date": start_ymd,
            "query_end_date": end_ymd,
            "nccs": nccs_evidence or {},
            "ccnl": ccnl_evidence or {},
            "db_reconcile": {
                "checked_trade_records": len(open_rows),
                "updated_trade_records": updated,
                "db_vs_ccnl_mismatch_count": still_missing_after_daily,
                "order_id_coverage_rate": coverage,
                "broker_only_count": broker_result.get("broker_only_count", 0),
                "broker_only_order_ids": broker_result.get("broker_only_order_ids", []),
            },
            "findings": broker_result.get("findings") or [],
        }
        save_order_reconcile_evidence(
            evidence_payload,
            market=market,
            trade_date=end_ymd,
        )
        logger.info(
            "broker-only detection: count=%s backfill=%s dry_run=%s",
            broker_result.get("broker_only_count"),
            backfill_broker_only,
            dry_run,
        )

    return summary


def main():
    parser = argparse.ArgumentParser(description="DB pending/partial 주문 리컨실")
    parser.add_argument("--since-hours", type=int, default=24, help="리컨실 대상 조회 범위(시간)")
    parser.add_argument("--limit", type=int, default=500, help="최대 조회 건수")
    parser.add_argument("--backfill-only", action="store_true", help="orphan order_id backfill만 실행")
    parser.add_argument(
        "--backfill-broker-only",
        action="store_true",
        help="KIS에만 있는 executed 주문을 trade_records에 idempotent backfill",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="backfill 대상만 로그 (DB 변경 없음)",
    )
    args = parser.parse_args()

    if args.backfill_only and not args.backfill_broker_only:
        backfill_orphan_order_ids(since_hours=args.since_hours, limit=min(args.limit, 200))
    else:
        reconcile_open_orders(
            since_hours=args.since_hours,
            limit=args.limit,
            backfill_broker_only=args.backfill_broker_only,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()

