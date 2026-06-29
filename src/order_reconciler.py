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
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from settings import settings
import os

from utils import setup_logging, KST, norm_ticker, is_us_market, kst_window_to_us_order_dates

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
    """KIS 해외주식 주문 row(Series/dict)를 내부 표준 dict로 정규화."""
    try:
        order_id = str(row.get("odno", "") or "").strip()
        if not order_id:
            return None
        qty = _safe_int(row.get("ft_ord_qty", row.get("ord_qty", 0)))
        executed_qty = _safe_int(row.get("ft_ccld_qty", row.get("tot_ccld_qty", 0)))
        ticker = norm_ticker(str(row.get("pdno", "") or ""), os.getenv("MARKET", "SP500"))
        side_cd = str(row.get("sll_buy_dvsn_cd", row.get("sll_buy_dvsn", "")) or "")
        side = "buy" if side_cd == "02" else "sell"
        order_time = str(row.get("ord_tmd", "") or "")
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


def _normalize_order_row(row: Any, *, overseas: bool = False) -> Optional[Dict[str, Any]]:
    if overseas:
        return _normalize_overseas_order_row(row)
    return _normalize_domestic_order_row(row)


def _fetch_overseas_open_orders(
    kis: KIS, *, ovrs_excg_cd: str = "NASD"
) -> Dict[str, Dict[str, Any]]:
    """KIS inquire_nccs(해외 미체결) → order_id dict. NASD/NYSE/AMEX 순회."""
    orders: List[Dict[str, Any]] = []
    exchanges = list(US_BALANCE_EXCHANGES) if is_us_market() else [ovrs_excg_cd]
    failures: List[str] = []
    for exc in exchanges:
        try:
            if hasattr(kis, "inquire_nccs"):
                df = kis.inquire_nccs(ovrs_excg_cd=exc)
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        o = _normalize_order_row(row, overseas=True)
                        if o:
                            o["ovrs_excg_cd"] = exc
                            orders.append(o)
        except Exception as e:
            msg = f"{exc}: {e}"
            failures.append(msg)
            logger.warning("inquire_nccs(%s) 조회 실패: %s", exc, e)

    if failures and not orders:
        logger.error(
            "KIS_ENDPOINT_UNAVAILABLE: inquire_nccs all exchanges failed (%s)",
            "; ".join(failures)[:400],
        )

    by_id: Dict[str, Dict[str, Any]] = {}
    for o in orders:
        by_id[o["order_id"]] = o
    return by_id


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
    kis: KIS, *, start_ymd: str, end_ymd: str, ovrs_excg_cd: str = "NASD"
) -> Dict[str, Dict[str, Any]]:
    """KIS inquire_ccnl(해외 주문체결내역) → order_id dict. NASD/NYSE/AMEX 순회."""
    orders: List[Dict[str, Any]] = []
    exchanges = list(US_BALANCE_EXCHANGES) if is_us_market() else [ovrs_excg_cd]
    failures: List[str] = []
    for exc in exchanges:
        try:
            if hasattr(kis, "inquire_ccnl"):
                df = kis.inquire_ccnl(
                    ord_strt_dt=start_ymd,
                    ord_end_dt=end_ymd,
                    ovrs_excg_cd=exc,
                )
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        o = _normalize_order_row(row, overseas=True)
                        if o:
                            o["ovrs_excg_cd"] = exc
                            orders.append(o)
        except Exception as e:
            msg = f"{exc}: {e}"
            failures.append(msg)
            logger.warning("inquire_ccnl(%s) 조회 실패: %s", exc, e)

    if failures and not orders:
        logger.error(
            "KIS_ENDPOINT_UNAVAILABLE: inquire_ccnl all exchanges failed (%s~%s) %s",
            start_ymd,
            end_ymd,
            "; ".join(failures)[:400],
        )

    by_id: Dict[str, Dict[str, Any]] = {}
    for o in orders:
        by_id[o["order_id"]] = o
    return by_id


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
    kis: KIS, *, start_ymd: Optional[str] = None, end_ymd: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """MARKET에 따라 KIS 미체결 주문 조회."""
    if is_us_market():
        by_id = _fetch_overseas_open_orders(kis)
        _db_dbg_log(
            "reconciler.fetch_overseas_nccs.OK",
            api="inquire-nccs",
            raw_rows=len(by_id),
            unique_order_ids=len(by_id),
            sample_ids=list(by_id.keys())[:8],
        )
        return by_id
    return _fetch_open_orders_inquire_orders(kis, start_ymd=start_ymd, end_ymd=end_ymd)


def _fetch_daily_orders(
    kis: KIS, *, start_ymd: str, end_ymd: Optional[str] = None,
    since_dt: Optional[datetime] = None, until_dt: Optional[datetime] = None,
) -> Dict[str, Dict[str, Any]]:
    """MARKET에 따라 KIS 일자별 주문 조회."""
    end = end_ymd or start_ymd
    if is_us_market():
        if since_dt is not None and until_dt is not None:
            start_ymd, end = kst_window_to_us_order_dates(since_dt, until_dt)
        return _fetch_overseas_daily_orders(kis, start_ymd=start_ymd, end_ymd=end)
    return _fetch_daily_orders_domestic(kis, start_ymd=start_ymd, end_ymd=end)


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

    daily_orders = _fetch_daily_orders(kis, start_ymd=start_ymd, end_ymd=end_ymd, since_dt=since_dt, until_dt=now_kst)
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


def reconcile_open_orders(*, since_hours: int = 24, limit: int = 500) -> Dict[str, int]:
    """
    DB open(pending/partial) 주문을 KIS 조회 결과로 리컨실.
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
    open_orders = _fetch_open_orders(kis, start_ymd=start_ymd, end_ymd=end_ymd)
    open_api = "inquire_nccs" if is_us_market() else "inquire_orders"
    logger.info(
        f"KIS 미체결({open_api}) 조회: {len(open_orders)}건 "
        f"({start_ymd}~{end_ymd})"
    )
    daily_orders: Optional[Dict[str, Dict[str, Any]]] = None

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
                daily_orders = _fetch_daily_orders(
                    kis,
                    start_ymd=start_ymd,
                    end_ymd=end_ymd,
                    since_dt=since_dt,
                    until_dt=now_kst,
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
    return summary


def main():
    parser = argparse.ArgumentParser(description="DB pending/partial 주문 리컨실")
    parser.add_argument("--since-hours", type=int, default=24, help="리컨실 대상 조회 범위(시간)")
    parser.add_argument("--limit", type=int, default=500, help="최대 조회 건수")
    parser.add_argument("--backfill-only", action="store_true", help="orphan order_id backfill만 실행")
    args = parser.parse_args()

    if args.backfill_only:
        backfill_orphan_order_ids(since_hours=args.since_hours, limit=min(args.limit, 200))
    else:
        reconcile_open_orders(since_hours=args.since_hours, limit=args.limit)


if __name__ == "__main__":
    main()

