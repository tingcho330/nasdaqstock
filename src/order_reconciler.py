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
import re
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
        ord_tmd = str(row.get("ord_tmd", "") or "").strip()
        thco_ord_tmd = str(row.get("thco_ord_tmd", "") or "").strip()
        order_time = ord_tmd or thco_ord_tmd


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
            "ord_dt": order_date,
            "domestic_order_date": domestic_order_date,
            "dmst_ord_dt": domestic_order_date,
            "order_time": order_time,
            "ord_tmd": ord_tmd,
            "thco_ord_tmd": thco_ord_tmd,
            "ticker": ticker,
            "product_name": str(row.get("prdt_name", "") or ""),
            "side": side,
            "side_code": side_cd,
            "side_name": side_name,
            "quantity": qty,
            "requested_qty": qty,
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
    else:
        px_val = _safe_decimal(exe_px, Decimal(0)) or Decimal(0)
        ok_px = px_val > 0
    if not ok_px:
        return False, "executed_price_le_0"
    try:
        from ledger_repair import effective_trade_timestamp_from_ccnl
        dt, reason = effective_trade_timestamp_from_ccnl(kis_o)
        if dt is None:
            return False, reason or "TIMESTAMP_EVIDENCE_INCOMPLETE"
    except Exception:
        if not str(kis_o.get("order_date") or "").strip():
            return False, "missing_order_date"
        if not str(kis_o.get("order_time") or "").strip():
            return False, "missing_order_time"
    return True, ""


def broker_trade_timestamp_kst(
    order_date: Any,
    order_time: Any,
) -> Optional[datetime]:
    """Build KST datetime from KIS ord_dt (YYYYMMDD) + ord_tmd (HHMMSS or HHMMSSSSS)."""
    d = str(order_date or "").strip()
    t = str(order_time or "").strip()
    if not re.fullmatch(r"\d{8}", d):
        return None
    digits = "".join(ch for ch in t if ch.isdigit())
    if len(digits) < 6:
        return None
    hh, mm, ss = digits[0:2], digits[2:4], digits[4:6]
    try:
        return datetime(
            int(d[0:4]), int(d[4:6]), int(d[6:8]),
            int(hh), int(mm), int(ss),
            tzinfo=KST,
        )
    except ValueError:
        return None


def _as_decimal_price(val: Any) -> Optional[Decimal]:
    if isinstance(val, Decimal):
        return val if val > 0 else None
    d = _safe_decimal(val)
    if d is None or d <= 0:
        return None
    return d


def _find_matching_buy_fill_from_ccnl(
    kis_orders: Dict[str, Dict[str, Any]],
    *,
    ticker: str,
    sell_order_date: str,
) -> Optional[Dict[str, Any]]:
    """Prefer most recent executed BUY fill for ticker with order_date <= sell date."""
    candidates: List[Dict[str, Any]] = []
    t = norm_ticker(ticker, os.getenv("MARKET", "SP500"))
    for o in (kis_orders or {}).values():
        if str(o.get("side") or "").lower() != "buy":
            continue
        if norm_ticker(str(o.get("ticker") or ""), os.getenv("MARKET", "SP500")) != t:
            continue
        if str(o.get("status") or "").lower() != "executed":
            continue
        if _safe_int(o.get("executed_qty", 0)) <= 0:
            continue
        if _as_decimal_price(o.get("executed_price")) is None:
            continue
        od = str(o.get("order_date") or "").strip()
        if od and sell_order_date and od > sell_order_date:
            continue
        candidates.append(o)
    if not candidates:
        return None
    candidates.sort(
        key=lambda x: (
            str(x.get("order_date") or ""),
            str(x.get("order_time") or ""),
            str(x.get("order_id") or ""),
        )
    )
    return candidates[-1]


def _gross_pnl_fill_to_fill(
    *,
    sell_executed_price: Decimal,
    buy_executed_price: Optional[Decimal],
    qty: int,
) -> Tuple[Optional[Decimal], bool]:
    """Return (gross_pnl, gross_pnl_complete). Never invent buy fill."""
    if buy_executed_price is None or buy_executed_price <= 0 or qty <= 0:
        return None, False
    return (sell_executed_price - buy_executed_price) * Decimal(qty), True


def _price_semantics_context(
    *,
    order_price: Any,
    executed_price: Any,
) -> Dict[str, Any]:
    return {
        "price_column_semantics": "executed_price",
        "order_price": _decimal_to_json(order_price)
        if isinstance(order_price, Decimal)
        else (str(order_price) if order_price is not None else None),
        "executed_price": _decimal_to_json(executed_price)
        if isinstance(executed_price, Decimal)
        else (str(executed_price) if executed_price is not None else None),
    }


def backfill_broker_only_orders(
    broker_only: List[Dict[str, Any]],
    *,
    recorder=None,
    dry_run: bool = False,
    all_kis_orders: Optional[Dict[str, Dict[str, Any]]] = None,
    recovered_at_kst: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Idempotent upsert for eligible broker-only executed orders.
    timestamp = order_date + order_time (not recovered_at).
    price = executed_price; order_price kept in structured_context.
    Gross P&L uses KIS CCNL buy fill when available.
    """
    rec = recorder or get_recorder()
    recovered_at = recovered_at_kst or datetime.now(KST).isoformat()
    kis_pool = all_kis_orders or {}
    inserted = 0
    updated = 0
    skipped_incomplete = 0
    incomplete_findings: List[Dict[str, Any]] = []
    backfilled_ids: List[str] = []
    corrected_buy_ids: List[str] = []

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
                    "order_time": kis_o.get("order_time"),
                },
            })
            logger.warning(
                "[BROKER_TRADE_BACKFILL_INCOMPLETE] order_id=%s reason=%s — DB unchanged",
                kis_o.get("order_id"),
                reason,
            )
            continue

        trade_ts = broker_trade_timestamp_kst(kis_o.get("order_date"), kis_o.get("order_time"))
        try:
            from ledger_repair import effective_trade_timestamp_from_ccnl
            eff, _reason = effective_trade_timestamp_from_ccnl(kis_o)
            if eff is not None:
                trade_ts = eff
        except Exception:
            pass
        if trade_ts is None:
            skipped_incomplete += 1
            incomplete_findings.append({
                "title": "BROKER_TRADE_BACKFILL_INCOMPLETE",
                "severity": "ERROR",
                "category": "DATA_INTEGRITY",
                "details": {
                    "order_id": kis_o.get("order_id"),
                    "reason": "invalid_order_date_time",
                    "order_date": kis_o.get("order_date"),
                    "order_time": kis_o.get("order_time"),
                },
            })
            continue

        exe_px = _as_decimal_price(kis_o.get("executed_price"))
        assert exe_px is not None  # eligibility already checked
        order_px = _as_decimal_price(kis_o.get("order_price"))
        exe_amt = _safe_decimal(kis_o.get("executed_amount"))
        qty = _safe_int(kis_o.get("executed_qty") or kis_o.get("quantity") or 0)
        side = str(kis_o.get("side") or "").lower()
        ticker = str(kis_o.get("ticker") or "")
        order_id = str(kis_o.get("order_id") or "").strip()
        order_date = str(kis_o.get("order_date") or "").strip()

        matched_buy = None
        buy_fill_px: Optional[Decimal] = None
        gross: Optional[Decimal] = None
        gross_complete = False
        if side == "sell":
            matched_buy = _find_matching_buy_fill_from_ccnl(
                kis_pool, ticker=ticker, sell_order_date=order_date,
            )
            if matched_buy is not None:
                buy_fill_px = _as_decimal_price(matched_buy.get("executed_price"))
            gross, gross_complete = _gross_pnl_fill_to_fill(
                sell_executed_price=exe_px,
                buy_executed_price=buy_fill_px,
                qty=qty,
            )

        price_meta = _price_semantics_context(order_price=order_px, executed_price=exe_px)
        structured = {
            "source": "order_reconciler",
            "backfilled_from": "kis_ccnl",
            "broker_only": True,
            "media": kis_o.get("media") or "OpenAPI",
            "recovered_at_kst": recovered_at,
            "effective_trade_timestamp": trade_ts.isoformat(),
            "order_date": order_date,
            "order_time": str(kis_o.get("order_time") or "").strip(),
            "exchange": kis_o.get("exchange"),
            "currency": kis_o.get("currency") or "USD",
            "executed_amount": _decimal_to_json(exe_amt) if exe_amt is not None else None,
            "gross_pnl": _decimal_to_json(gross) if gross is not None else None,
            "gross_pnl_basis": True,
            "gross_pnl_complete": gross_complete,
            "net_pnl_complete": False,
            "commission_known": False,
            "tax_known": False,
            "matched_buy_order_id": (matched_buy or {}).get("order_id") if matched_buy else None,
            "matched_buy_executed_price": _decimal_to_json(buy_fill_px) if buy_fill_px else None,
            **price_meta,
        }

        # profit_loss stores gross only when complete; else 0 with incomplete flag
        pnl_value = float(gross) if (side == "sell" and gross_complete and gross is not None) else 0.0

        if dry_run:
            logger.info(
                "[BACKFILL_DRY_RUN] would upsert order_id=%s ticker=%s side=%s qty=%s "
                "px=%s ts=%s gross=%s complete=%s",
                order_id,
                ticker,
                side,
                qty,
                exe_px,
                trade_ts.isoformat(),
                gross,
                gross_complete,
            )
            backfilled_ids.append(order_id)
            continue

        known_before = order_id in get_db_order_ids(rec)
        from recorder import TradeRecord
        import json as _json

        # Preserve prior recovered_at if correcting an existing backfill row
        if known_before:
            try:
                existing = [
                    t for t in rec.get_trade_records(ticker=ticker)
                    if str(getattr(t, "order_id", "") or "") == order_id
                ]
                if existing:
                    prev_ctx = {}
                    raw = getattr(existing[0], "structured_context", "") or ""
                    if isinstance(raw, str) and raw.strip().startswith("{"):
                        prev_ctx = _json.loads(raw)
                    if prev_ctx.get("recovered_at_kst"):
                        structured["recovered_at_kst"] = prev_ctx["recovered_at_kst"]
                    structured["corrected_at_kst"] = recovered_at
            except Exception:
                pass

        try:
            structured_json = _json.dumps(structured, ensure_ascii=False)
        except Exception:
            structured_json = str(structured)

        amount = float(exe_amt) if exe_amt is not None else float(exe_px) * qty
        tr = TradeRecord(
            timestamp=trade_ts,
            ticker=ticker,
            action=side.upper(),
            quantity=qty,
            price=float(exe_px),  # executed fill price
            amount=amount,
            commission=0.0,
            tax=0.0,
            total_cost=amount,
            net_amount=amount,
            profit_loss=pnl_value if side == "sell" else 0.0,
            holding_period_days=0,
            order_status="executed",
            order_id=order_id,
            requested_qty=_safe_int(kis_o.get("quantity") or qty),
            executed_qty=qty,
            last_status_update_ts=recovered_at,
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

        # Align matched BUY DB price to KIS executed fill (idempotent)
        if matched_buy and buy_fill_px is not None:
            buy_oid = str(matched_buy.get("order_id") or "").strip()
            if buy_oid and correct_executed_price_from_ccnl(
                recorder=rec,
                kis_o=matched_buy,
                dry_run=False,
            ):
                corrected_buy_ids.append(buy_oid)

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
        "corrected_buy_order_ids": corrected_buy_ids,
        "incomplete_findings": incomplete_findings,
        "dry_run": dry_run,
    }


def correct_executed_price_from_ccnl(
    *,
    recorder,
    kis_o: Dict[str, Any],
    dry_run: bool = False,
) -> bool:
    """Idempotent: set DB price to KIS executed_price for an existing executed row."""
    import json as _json
    from recorder import TradeRecord

    oid = str(kis_o.get("order_id") or "").strip()
    exe_px = _as_decimal_price(kis_o.get("executed_price"))
    if not oid or exe_px is None:
        return False
    ticker = str(kis_o.get("ticker") or "")
    existing = []
    try:
        if ticker:
            existing = [
                t for t in recorder.get_trade_records(ticker=ticker)
                if str(getattr(t, "order_id", "") or "") == oid
            ]
        if not existing:
            existing = [
                t for t in recorder.get_trade_records()
                if str(getattr(t, "order_id", "") or "") == oid
            ]
    except Exception:
        existing = []
    if not existing:
        return False
    row = existing[0]
    if abs(float(row.price or 0) - float(exe_px)) < 1e-9 and str(row.order_status).lower() in (
        "executed", "completed",
    ):
        # still refresh structured context semantics
        pass

    prev_ctx: Dict[str, Any] = {}
    raw = getattr(row, "structured_context", "") or ""
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            prev_ctx = _json.loads(raw)
        except Exception:
            prev_ctx = {}
    order_px = _as_decimal_price(kis_o.get("order_price"))
    if order_px is None and prev_ctx.get("order_price") is not None:
        order_px = _as_decimal_price(prev_ctx.get("order_price"))
    # If previous DB price looked like order price, keep it as order_price evidence
    if order_px is None and abs(float(row.price or 0) - float(exe_px)) >= 1e-6:
        order_px = _safe_decimal(row.price)

    trade_ts = broker_trade_timestamp_kst(kis_o.get("order_date"), kis_o.get("order_time"))
    if trade_ts is None and getattr(row, "timestamp", None):
        trade_ts = row.timestamp if getattr(row.timestamp, "tzinfo", None) else (
            row.timestamp.replace(tzinfo=KST) if isinstance(row.timestamp, datetime) else datetime.now(KST)
        )

    ctx = {
        **prev_ctx,
        **_price_semantics_context(order_price=order_px, executed_price=exe_px),
        "kis_ccnl_executed_price": _decimal_to_json(exe_px),
        "price_corrected_from_ccnl": True,
    }
    if kis_o.get("order_date"):
        ctx.setdefault("order_date", str(kis_o.get("order_date")))
    if kis_o.get("order_time"):
        ctx.setdefault("order_time", str(kis_o.get("order_time")))
    if trade_ts is not None:
        ctx["effective_trade_timestamp"] = trade_ts.isoformat() if hasattr(trade_ts, "isoformat") else str(trade_ts)

    qty = int(getattr(row, "executed_qty", 0) or getattr(row, "quantity", 0) or 0)
    amount = float(exe_px) * qty if qty > 0 else float(exe_px)
    if dry_run:
        return True
    tr = TradeRecord(
        timestamp=trade_ts if isinstance(trade_ts, datetime) else datetime.now(KST),
        ticker=row.ticker,
        action=row.action,
        quantity=int(row.quantity or qty),
        price=float(exe_px),
        amount=amount,
        commission=float(getattr(row, "commission", 0) or 0),
        tax=float(getattr(row, "tax", 0) or 0),
        total_cost=amount,
        net_amount=amount,
        profit_loss=float(getattr(row, "profit_loss", 0) or 0),
        holding_period_days=int(getattr(row, "holding_period_days", 0) or 0),
        sector=getattr(row, "sector", "") or "",
        market_regime=getattr(row, "market_regime", "") or "",
        order_status="executed",
        order_id=oid,
        requested_qty=int(getattr(row, "requested_qty", 0) or qty),
        executed_qty=qty if qty > 0 else int(row.quantity or 0),
        last_status_update_ts=datetime.now(KST).isoformat(),
        sell_reason=getattr(row, "sell_reason", "") or "",
        reason_code=getattr(row, "reason_code", "") or "",
        structured_context=_json.dumps(ctx, ensure_ascii=False),
    )
    return bool(recorder.upsert_trade_record_by_order_id(tr))


# Known production backfill that used recovery wall-clock timestamp / order-price BUY.
SNDK_SELL_ORDER_ID = "0031276871"
SNDK_BUY_ORDER_ID = "0030975669"
SNDK_FIX_EVIDENCE = {
    "sell_order_id": SNDK_SELL_ORDER_ID,
    "buy_order_id": SNDK_BUY_ORDER_ID,
    "order_date": "20260709",
    "order_time": "233156",
    "sell_executed_price": "1833.9032",
    "buy_executed_price": "1692.9724",
    "buy_order_price": "1695.0",
    "gross_pnl": "140.9308",
    "effective_trade_timestamp": "2026-07-09T23:31:56+09:00",
    "recovered_at_kst": "2026-07-14T13:24:52+09:00",
}


def correct_sell_fill_pnl_from_ccnl(
    *,
    recorder,
    sell_kis: Dict[str, Any],
    all_kis_orders: Dict[str, Dict[str, Any]],
    dry_run: bool = False,
) -> bool:
    """
    Idempotent correction for an existing SELL row:
    - timestamp / effective_trade_timestamp from order_date+order_time
    - price = executed_price
    - gross_pnl from CCNL BUY fill (never invent)
    Preserves recovered_at_kst and broker_only flags; does not invent new broker_only rows.
    """
    import json as _json
    from recorder import TradeRecord

    oid = str(sell_kis.get("order_id") or "").strip()
    if not oid:
        return False
    existing = [
        t for t in recorder.get_trade_records()
        if str(getattr(t, "order_id", "") or "") == oid
    ]
    if not existing:
        return False
    row = existing[0]
    if str(getattr(row, "action", "") or "").upper() != "SELL":
        return False

    trade_ts = broker_trade_timestamp_kst(sell_kis.get("order_date"), sell_kis.get("order_time"))
    if trade_ts is None:
        logger.warning(
            "[REPAIR_SKIP] order_id=%s missing/invalid order_date+time", oid,
        )
        return False

    exe_px = _as_decimal_price(sell_kis.get("executed_price"))
    if exe_px is None:
        return False
    order_px = _as_decimal_price(sell_kis.get("order_price"))
    qty = _safe_int(
        sell_kis.get("executed_qty")
        or getattr(row, "executed_qty", 0)
        or getattr(row, "quantity", 0)
        or 0
    )
    order_date = str(sell_kis.get("order_date") or "").strip()
    matched_buy = _find_matching_buy_fill_from_ccnl(
        all_kis_orders, ticker=str(row.ticker or sell_kis.get("ticker") or ""),
        sell_order_date=order_date,
    )
    buy_fill_px = _as_decimal_price((matched_buy or {}).get("executed_price"))
    gross, gross_complete = _gross_pnl_fill_to_fill(
        sell_executed_price=exe_px, buy_executed_price=buy_fill_px, qty=qty,
    )

    prev_ctx: Dict[str, Any] = {}
    raw = getattr(row, "structured_context", "") or ""
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            prev_ctx = _json.loads(raw)
        except Exception:
            prev_ctx = {}

    recovered_at = prev_ctx.get("recovered_at_kst")
    ctx = {
        **prev_ctx,
        **_price_semantics_context(order_price=order_px, executed_price=exe_px),
        "effective_trade_timestamp": trade_ts.isoformat(),
        "order_date": order_date,
        "order_time": str(sell_kis.get("order_time") or "").strip(),
        "gross_pnl": _decimal_to_json(gross) if gross is not None else None,
        "gross_pnl_basis": True,
        "gross_pnl_complete": gross_complete,
        "net_pnl_complete": False if not prev_ctx.get("commission_known") else prev_ctx.get("net_pnl_complete"),
        "matched_buy_order_id": (matched_buy or {}).get("order_id") if matched_buy else prev_ctx.get("matched_buy_order_id"),
        "matched_buy_executed_price": (
            _decimal_to_json(buy_fill_px) if buy_fill_px else prev_ctx.get("matched_buy_executed_price")
        ),
        "price_corrected_from_ccnl": True,
        "corrected_at_kst": datetime.now(KST).isoformat(),
    }
    if recovered_at:
        ctx["recovered_at_kst"] = recovered_at

    if dry_run:
        return True

    amount = float(exe_px) * qty if qty > 0 else float(exe_px)
    pnl_value = float(gross) if (gross_complete and gross is not None) else 0.0

    tr = TradeRecord(
        timestamp=trade_ts,
        ticker=row.ticker,
        action="SELL",
        quantity=int(row.quantity or qty),
        price=float(exe_px),
        amount=amount,
        commission=float(getattr(row, "commission", 0) or 0),
        tax=float(getattr(row, "tax", 0) or 0),
        total_cost=amount,
        net_amount=amount,
        profit_loss=pnl_value,
        holding_period_days=int(getattr(row, "holding_period_days", 0) or 0),
        sector=getattr(row, "sector", "") or "",
        market_regime=getattr(row, "market_regime", "") or "",
        order_status="executed",
        order_id=oid,
        requested_qty=int(getattr(row, "requested_qty", 0) or qty),
        executed_qty=qty if qty > 0 else int(row.quantity or 0),
        last_status_update_ts=datetime.now(KST).isoformat(),
        sell_reason=getattr(row, "sell_reason", "") or "",
        reason_code=getattr(row, "reason_code", "") or "",
        structured_context=_json.dumps(ctx, ensure_ascii=False),
    )
    ok = bool(recorder.upsert_trade_record_by_order_id(tr))
    if ok and matched_buy and buy_fill_px is not None:
        correct_executed_price_from_ccnl(recorder=recorder, kis_o=matched_buy, dry_run=False)
    return ok


def repair_backfilled_trades_from_ccnl(
    kis_orders: Dict[str, Dict[str, Any]],
    *,
    recorder=None,
    order_ids: Optional[List[str]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Idempotent correction for already-persisted rows using CCNL fills.
    Does not insert new broker_only rows and does not force broker_only=True on live sells.
    Prefer rows that already exist in DB and (optionally) known SNDK fix targets.
    """
    rec = recorder or get_recorder()
    known = get_db_order_ids(rec)
    targets = set(order_ids or []) or set(kis_orders.keys())
    # Always include known SNDK fixture ids when present in CCNL/DB
    targets |= {SNDK_SELL_ORDER_ID, SNDK_BUY_ORDER_ID}
    repaired: List[str] = []
    details: List[Dict[str, Any]] = []

    for oid in sorted(targets):
        kis_o = kis_orders.get(oid)
        if not kis_o:
            continue
        if oid not in known:
            continue
        side = str(kis_o.get("side") or "").lower()
        if side == "buy":
            if correct_executed_price_from_ccnl(recorder=rec, kis_o=kis_o, dry_run=dry_run):
                repaired.append(oid)
        elif side == "sell":
            if correct_sell_fill_pnl_from_ccnl(
                recorder=rec,
                sell_kis=kis_o,
                all_kis_orders=kis_orders,
                dry_run=dry_run,
            ):
                repaired.append(oid)

    # Explicit SNDK migration when CCNL window missed the fixture (hardcoded evidence)
    if SNDK_SELL_ORDER_ID in known:
        mig = migrate_sndk_backfill_row(recorder=rec, kis_orders=kis_orders, dry_run=dry_run)
        details.append(mig)
        repaired.extend(mig.get("repaired_order_ids") or [])

    return {
        "repaired_order_ids": sorted(set(repaired)),
        "details": details,
        "dry_run": dry_run,
    }


def migrate_sndk_backfill_row(
    *,
    recorder=None,
    kis_orders: Optional[Dict[str, Dict[str, Any]]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Idempotent migration for SNDK SELL 0031276871 / BUY 0030975669.
    Uses live CCNL when available; otherwise SNDK_FIX_EVIDENCE constants.
    Never creates duplicate order_id rows.
    """
    import json as _json
    from recorder import TradeRecord

    rec = recorder or get_recorder()
    kis = dict(kis_orders or {})
    ev = SNDK_FIX_EVIDENCE
    repaired: List[str] = []

    # Prefer live CCNL normalization when present
    sell_kis = kis.get(SNDK_SELL_ORDER_ID)
    buy_kis = kis.get(SNDK_BUY_ORDER_ID)
    if sell_kis is None:
        sell_kis = {
            "order_id": SNDK_SELL_ORDER_ID,
            "order_date": ev["order_date"],
            "order_time": ev["order_time"],
            "ticker": "SNDK",
            "side": "sell",
            "status": "executed",
            "quantity": 1,
            "executed_qty": 1,
            "executed_price": Decimal(ev["sell_executed_price"]),
            "order_price": Decimal("1827"),
        }
    if buy_kis is None:
        buy_kis = {
            "order_id": SNDK_BUY_ORDER_ID,
            "order_date": "20260708",
            "order_time": "230000",
            "ticker": "SNDK",
            "side": "buy",
            "status": "executed",
            "quantity": 1,
            "executed_qty": 1,
            "executed_price": Decimal(ev["buy_executed_price"]),
            "order_price": Decimal(ev["buy_order_price"]),
        }
    pool = {**kis, SNDK_SELL_ORDER_ID: sell_kis, SNDK_BUY_ORDER_ID: buy_kis}

    sell_rows = [
        t for t in rec.get_trade_records(ticker="SNDK")
        if str(getattr(t, "order_id", "") or "") == SNDK_SELL_ORDER_ID
    ]
    if not sell_rows:
        return {"repaired_order_ids": [], "skipped": "sell_missing", "dry_run": dry_run}

    sell = sell_rows[0]
    prev_ctx: Dict[str, Any] = {}
    raw = getattr(sell, "structured_context", "") or ""
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            prev_ctx = _json.loads(raw)
        except Exception:
            prev_ctx = {}

    trade_ts = broker_trade_timestamp_kst(ev["order_date"], ev["order_time"])
    assert trade_ts is not None
    sell_px = Decimal(ev["sell_executed_price"])
    buy_px = Decimal(ev["buy_executed_price"])
    gross = Decimal(ev["gross_pnl"])
    recovered = prev_ctx.get("recovered_at_kst") or ev["recovered_at_kst"]

    ts_ok = False
    try:
        ts = getattr(sell, "timestamp", None)
        if isinstance(ts, datetime):
            ts_ok = ts.astimezone(KST).strftime("%Y-%m-%dT%H:%M:%S") == "2026-07-09T23:31:56"
        else:
            ts_ok = str(ts or "").startswith("2026-07-09T23:31:56")
    except Exception:
        ts_ok = False
    buy_ev_ok = abs(float(prev_ctx.get("matched_buy_executed_price") or 0) - float(buy_px)) < 1e-6
    already = (
        abs(float(sell.price or 0) - float(sell_px)) < 1e-6
        and abs(float(sell.profit_loss or 0) - float(gross)) < 1e-4
        and ts_ok
        and buy_ev_ok
        and str(prev_ctx.get("recovered_at_kst") or "") == recovered
    )
    if already:
        if correct_executed_price_from_ccnl(recorder=rec, kis_o=buy_kis, dry_run=dry_run):
            repaired.append(SNDK_BUY_ORDER_ID)
        return {"repaired_order_ids": sorted(set(repaired)), "already_correct": True, "dry_run": dry_run}

    if dry_run:
        return {
            "repaired_order_ids": [SNDK_SELL_ORDER_ID, SNDK_BUY_ORDER_ID],
            "dry_run": True,
        }

    ctx = {
        **prev_ctx,
        "source": prev_ctx.get("source") or "order_reconciler",
        "backfilled_from": prev_ctx.get("backfilled_from") or "kis_ccnl",
        "broker_only": True if prev_ctx.get("broker_only", True) else False,
        "recovered_at_kst": recovered,
        "effective_trade_timestamp": ev["effective_trade_timestamp"],
        "order_date": ev["order_date"],
        "order_time": ev["order_time"],
        "gross_pnl": ev["gross_pnl"],
        "gross_pnl_basis": True,
        "gross_pnl_complete": True,
        "net_pnl_complete": False,
        "commission_known": False,
        "tax_known": False,
        "matched_buy_order_id": SNDK_BUY_ORDER_ID,
        "matched_buy_executed_price": ev["buy_executed_price"],
        "migration": "sndk_0031276871_fill_to_fill",
        "corrected_at_kst": datetime.now(KST).isoformat(),
        **_price_semantics_context(
            order_price=_as_decimal_price(sell_kis.get("order_price")) or Decimal("1827"),
            executed_price=sell_px,
        ),
    }

    qty = int(getattr(sell, "executed_qty", 0) or getattr(sell, "quantity", 0) or 1)
    amount = float(sell_px) * qty
    tr = TradeRecord(
        timestamp=trade_ts,
        ticker="SNDK",
        action="SELL",
        quantity=int(sell.quantity or qty),
        price=float(sell_px),
        amount=amount,
        commission=float(getattr(sell, "commission", 0) or 0),
        tax=float(getattr(sell, "tax", 0) or 0),
        total_cost=amount,
        net_amount=amount,
        profit_loss=float(gross),
        holding_period_days=int(getattr(sell, "holding_period_days", 0) or 0),
        sector=getattr(sell, "sector", "") or "",
        market_regime=getattr(sell, "market_regime", "") or "",
        order_status="executed",
        order_id=SNDK_SELL_ORDER_ID,
        requested_qty=int(getattr(sell, "requested_qty", 0) or qty),
        executed_qty=qty,
        last_status_update_ts=datetime.now(KST).isoformat(),
        sell_reason=getattr(sell, "sell_reason", "") or "",
        reason_code=getattr(sell, "reason_code", "") or "BROKER_ONLY_BACKFILL",
        structured_context=_json.dumps(ctx, ensure_ascii=False),
    )
    if rec.upsert_trade_record_by_order_id(tr):
        repaired.append(SNDK_SELL_ORDER_ID)

    # BUY executed price + evidence
    buy_rows = [
        t for t in rec.get_trade_records(ticker="SNDK")
        if str(getattr(t, "order_id", "") or "") == SNDK_BUY_ORDER_ID
    ]
    if buy_rows:
        if correct_executed_price_from_ccnl(recorder=rec, kis_o=buy_kis, dry_run=False):
            repaired.append(SNDK_BUY_ORDER_ID)
        # Ensure order-price evidence retained
        buy2 = [
            t for t in rec.get_trade_records(ticker="SNDK")
            if str(getattr(t, "order_id", "") or "") == SNDK_BUY_ORDER_ID
        ]
        if buy2:
            bctx: Dict[str, Any] = {}
            braw = getattr(buy2[0], "structured_context", "") or ""
            if isinstance(braw, str) and braw.strip().startswith("{"):
                try:
                    bctx = _json.loads(braw)
                except Exception:
                    bctx = {}
            bctx.update(_price_semantics_context(
                order_price=Decimal(ev["buy_order_price"]),
                executed_price=buy_px,
            ))
            bctx["kis_ccnl_executed_price"] = ev["buy_executed_price"]
            b = buy2[0]
            btr = TradeRecord(
                timestamp=b.timestamp if isinstance(b.timestamp, datetime) else trade_ts,
                ticker="SNDK",
                action="BUY",
                quantity=int(b.quantity or 1),
                price=float(buy_px),
                amount=float(buy_px) * int(b.quantity or 1),
                commission=float(getattr(b, "commission", 0) or 0),
                tax=float(getattr(b, "tax", 0) or 0),
                total_cost=float(buy_px) * int(b.quantity or 1),
                net_amount=float(buy_px) * int(b.quantity or 1),
                profit_loss=0.0,
                holding_period_days=int(getattr(b, "holding_period_days", 0) or 0),
                order_status="executed",
                order_id=SNDK_BUY_ORDER_ID,
                requested_qty=int(getattr(b, "requested_qty", 0) or 1),
                executed_qty=int(getattr(b, "executed_qty", 0) or 1),
                last_status_update_ts=datetime.now(KST).isoformat(),
                reason_code=getattr(b, "reason_code", "") or "",
                structured_context=_json.dumps(bctx, ensure_ascii=False),
            )
            rec.upsert_trade_record_by_order_id(btr)

    # Ensure no duplicate order_ids for sell
    all_sells = [
        t for t in rec.get_trade_records(ticker="SNDK")
        if str(getattr(t, "order_id", "") or "") == SNDK_SELL_ORDER_ID
    ]
    return {
        "repaired_order_ids": sorted(set(repaired)),
        "sell_row_count": len(all_sells),
        "gross_pnl": ev["gross_pnl"],
        "effective_trade_timestamp": ev["effective_trade_timestamp"],
        "recovered_at_kst": recovered,
        "dry_run": dry_run,
        "pool_order_ids": list(pool.keys()),
    }


def detect_and_optionally_backfill_broker_only(
    daily_orders: Dict[str, Dict[str, Any]],
    *,
    backfill: bool = False,
    dry_run: bool = False,
    recorder=None,
    repair_existing: bool = False,
) -> Dict[str, Any]:
    """
    Detect broker-only orders. Optionally backfill missing rows.
    Does NOT auto-repair existing DB rows from CCNL — use --repair-ledger-from-ccnl.
    """
    rec = recorder or get_recorder()
    broker_only, findings = detect_broker_only_orders(daily_orders, recorder=rec)
    result: Dict[str, Any] = {
        "broker_only_detected_count": len(broker_only),
        "broker_only_order_count": len(broker_only),
        "broker_only_count": len(broker_only),  # backward compat
        "broker_only_orders": [
            {
                "order_id": o.get("order_id"),
                "order_date": o.get("order_date"),
                "order_time": o.get("order_time"),
                "ticker": o.get("ticker"),
                "side": o.get("side"),
                "executed_qty": o.get("executed_qty"),
                "executed_price": o.get("executed_price_str") or _decimal_to_json(o.get("executed_price")),
            }
            for o in broker_only
        ],
        "broker_only_order_ids": [o.get("order_id") for o in broker_only],
        "broker_only_backfilled_count": 0,
        "broker_only_incomplete_count": 0,
        "ccnl_corrected_order_count": 0,
        "executed_price_corrected_count": 0,
        "quantity_corrected_count": 0,
        "sell_pnl_corrected_count": 0,
        "findings": findings,
    }

    # Explicit opt-in only (deprecated path; prefer --repair-ledger-from-ccnl)
    if repair_existing and daily_orders:
        repair = repair_backfilled_trades_from_ccnl(
            daily_orders or {}, recorder=rec, dry_run=dry_run,
        )
        result["repair"] = repair
        result["ccnl_corrected_order_count"] = len(repair.get("repaired_order_ids") or [])

    if backfill and broker_only:
        bf = backfill_broker_only_orders(
            broker_only,
            recorder=rec,
            dry_run=dry_run,
            all_kis_orders=daily_orders,
        )
        result.update(bf)
        result["broker_only_backfilled_count"] = int(
            (bf.get("backfill_inserted") or 0) + (bf.get("backfill_updated") or 0)
        )
        result["broker_only_incomplete_count"] = int(bf.get("backfill_skipped_incomplete") or 0)
        result["findings"] = findings + list(bf.get("incomplete_findings") or [])
        if not dry_run:
            remaining, remaining_findings = detect_broker_only_orders(
                daily_orders, recorder=rec,
            )
            result["broker_only_remaining"] = len(remaining)
            result["findings_after_backfill"] = remaining_findings
            result["broker_only_detected_count"] = len(remaining)
            result["broker_only_order_count"] = len(remaining)
            result["broker_only_orders"] = [
                {
                    "order_id": o.get("order_id"),
                    "order_date": o.get("order_date"),
                    "order_time": o.get("order_time"),
                    "ticker": o.get("ticker"),
                    "side": o.get("side"),
                }
                for o in remaining
            ]
            result["broker_only_order_ids"] = [o.get("order_id") for o in remaining]
    elif not backfill:
        result["broker_only_backfilled_count"] = 0
        result["broker_only_incomplete_count"] = 0

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
            repair_existing=False,  # never auto-migrate; use --repair-ledger-from-ccnl
        )
        summary["broker_only_count"] = broker_result.get("broker_only_order_count", 0)
        summary["broker_only_order_ids"] = broker_result.get("broker_only_order_ids", [])
        if backfill_broker_only:
            summary["backfill_broker_inserted"] = broker_result.get("backfill_inserted", 0)
            summary["backfill_broker_updated"] = broker_result.get("backfill_updated", 0)
            summary["backfill_broker_skipped_incomplete"] = broker_result.get(
                "backfill_skipped_incomplete", 0
            )
            summary["broker_only_remaining"] = broker_result.get(
                "broker_only_remaining",
                broker_result.get("broker_only_order_count", 0),
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
                # Stable schema even when count=0
                "broker_only_detected_count": int(
                    broker_result.get("broker_only_detected_count")
                    or broker_result.get("broker_only_order_count")
                    or 0
                ),
                "broker_only_order_count": int(
                    broker_result.get("broker_only_order_count") or 0
                ),
                "broker_only_orders": list(broker_result.get("broker_only_orders") or []),
                "broker_only_backfilled_count": int(
                    broker_result.get("broker_only_backfilled_count") or 0
                ),
                "broker_only_incomplete_count": int(
                    broker_result.get("broker_only_incomplete_count") or 0
                ),
                "ccnl_corrected_order_count": int(
                    broker_result.get("ccnl_corrected_order_count") or 0
                ),
                "executed_price_corrected_count": int(
                    broker_result.get("executed_price_corrected_count") or 0
                ),
                "quantity_corrected_count": int(
                    broker_result.get("quantity_corrected_count") or 0
                ),
                "sell_pnl_corrected_count": int(
                    broker_result.get("sell_pnl_corrected_count") or 0
                ),
                # backward-compat aliases
                "broker_only_count": int(broker_result.get("broker_only_order_count") or 0),
                "broker_only_order_ids": list(broker_result.get("broker_only_order_ids") or []),
            },
            "broker_only_detected_count": int(
                broker_result.get("broker_only_detected_count")
                or broker_result.get("broker_only_order_count")
                or 0
            ),
            "broker_only_order_count": int(
                broker_result.get("broker_only_order_count") or 0
            ),
            "broker_only_orders": list(broker_result.get("broker_only_orders") or []),
            "broker_only_backfilled_count": int(
                broker_result.get("broker_only_backfilled_count") or 0
            ),
            "broker_only_incomplete_count": int(
                broker_result.get("broker_only_incomplete_count") or 0
            ),
            "ccnl_corrected_order_count": int(
                broker_result.get("ccnl_corrected_order_count") or 0
            ),
            "executed_price_corrected_count": int(
                broker_result.get("executed_price_corrected_count") or 0
            ),
            "quantity_corrected_count": int(
                broker_result.get("quantity_corrected_count") or 0
            ),
            "sell_pnl_corrected_count": int(
                broker_result.get("sell_pnl_corrected_count") or 0
            ),
            "findings": broker_result.get("findings") or [],
        }
        save_order_reconcile_evidence(
            evidence_payload,
            market=market,
            trade_date=end_ymd,
        )
        logger.info(
            "broker-only detection: count=%s backfill=%s dry_run=%s",
            broker_result.get("broker_only_order_count"),
            backfill_broker_only,
            dry_run,
        )

    return summary


def main():
    parser = argparse.ArgumentParser(description="DB pending/partial 주문 리컨실 / CCNL ledger repair")
    parser.add_argument("--since-hours", type=int, default=24, help="리컨실 대상 조회 범위(시간)")
    parser.add_argument("--limit", type=int, default=500, help="최대 조회 건수")
    parser.add_argument("--backfill-only", action="store_true", help="orphan order_id backfill만 실행")
    parser.add_argument(
        "--backfill-broker-only",
        action="store_true",
        help="KIS에만 있는 executed 주문을 trade_records에 idempotent backfill",
    )
    parser.add_argument(
        "--repair-ledger-from-ccnl",
        action="store_true",
        help="기존 DB 행을 KIS CCNL 체결가로 UPDATE-only repair (기본 dry-run)",
    )
    parser.add_argument("--ticker", type=str, help="--repair-ledger-from-ccnl 대상 티커")
    parser.add_argument("--from", dest="date_from", metavar="YYYYMMDD", help="repair 시작일")
    parser.add_argument("--to", dest="date_to", metavar="YYYYMMDD", help="repair 종료일")
    parser.add_argument(
        "--apply-repair",
        action="store_true",
        help="ledger repair 실제 DB 적용 (없으면 dry-run)",
    )
    parser.add_argument("--db-path", type=str, help="trading_data.db 경로")
    parser.add_argument("--evidence-path", type=str, help="ledger repair evidence JSON 경로")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB 변경 없음 (repair 기본값; backfill과 함께 사용 가능)",
    )
    args = parser.parse_args()

    if args.repair_ledger_from_ccnl:
        if args.dry_run and args.apply_repair:
            raise SystemExit("ERROR: --dry-run and --apply-repair are mutually exclusive")
        if not args.ticker or not args.date_from or not args.date_to:
            raise SystemExit(
                "ERROR: --repair-ledger-from-ccnl requires --ticker, --from, --to"
            )
        from ledger_repair import repair_ledger_from_ccnl, default_trading_db_path

        apply = bool(args.apply_repair)
        mode = "apply" if apply else "dry_run"
        db_path = Path(args.db_path) if args.db_path else default_trading_db_path()
        print(f"[LEDGER_REPAIR] mode={mode} ticker={args.ticker} "
              f"from={args.date_from} to={args.date_to} db={db_path}")
        result = repair_ledger_from_ccnl(
            ticker=args.ticker,
            date_from=args.date_from,
            date_to=args.date_to,
            apply=apply,
            db_path=db_path,
            evidence_path=Path(args.evidence_path) if args.evidence_path else None,
            market=os.getenv("MARKET", "SP500"),
        )
        if result.get("error"):
            print(f"[LEDGER_REPAIR] ERROR: {result['error']}")
            raise SystemExit(1)
        print(
            f"[LEDGER_REPAIR] done mode={result['mode']} "
            f"needing={result.get('rows_needing_update')} "
            f"updated={result.get('rows_updated')} "
            f"evidence={result.get('evidence_path')}"
        )
        return

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

