# src/kis_overseas_account.py
"""KIS 해외주식 잔고 조회 → 국내 balance/summary JSON 호환 형식으로 정규화."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from datetime import datetime

from account_snapshot import (
    AccountSnapshot,
    US_BALANCE_EXCHANGES,
    build_sellable_qty_map,
    compute_sellable_qty,
)
from utils import KST, norm_ticker, us_ovrs_excg_cd

logger = logging.getLogger(__name__)


def _f(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _i(val: Any, default: int = 0) -> int:
    try:
        return int(round(_f(val, default)))
    except (TypeError, ValueError):
        return default


def _first_row(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {}
    return df.iloc[0].to_dict()


def _rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def _present_usd_row(df_present_out2: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """CTRP6504R output2: 통화별 행 — USD 행 우선."""
    if df_present_out2 is None or df_present_out2.empty:
        return {}
    for rec in _rows(df_present_out2):
        cd = str(rec.get("crcy_cd") or "").strip().upper()
        name = str(rec.get("crcy_cd_name") or "").strip().upper()
        if cd == "USD" or "USD" in name or "달러" in name:
            return rec
    return df_present_out2.iloc[0].to_dict()


def _resolve_overseas_avg_price(rec: Dict[str, Any], qty: float) -> float:
    """
    해외 잔고 output1 평균매입단가.
    - pchs_avg_pric / avg_unpr3 등: 이미 주당 단가(USD) → qty로 나누지 않음
    - frcr_pchs_amt1 등: 총 매입금액(USD) → qty로 나눠 주당 단가 산출
    """
    qty_f = max(_f(qty), 1.0)
    per_share_keys = (
        "pchs_avg_pric",
        "avg_unpr3",
        "ovrs_avg_unpr",
        "pchs_avg_pric1",
        "avg_unpr",
    )
    for key in per_share_keys:
        v = _f(rec.get(key))
        if v > 0:
            return v

    total_keys = ("frcr_pchs_amt1", "frcr_pchs_amt", "pchs_amt", "ovrs_pchs_amt")
    for key in total_keys:
        total = _f(rec.get(key))
        if total > 0:
            return total / qty_f
    return 0.0


def _pick_positive(*sources: Dict[str, Any], keys: Tuple[str, ...]) -> float:
    """여러 dict에서 첫 번째 양수 금액 필드."""
    for src in sources:
        if not src:
            continue
        for k in keys:
            v = _f(src.get(k))
            if v > 0:
                return v
    return 0.0


def _pick_bass_exrt(
    df_present_out1: Optional[pd.DataFrame],
    pres2: Dict[str, Any],
    bal2: Dict[str, Any],
) -> float:
    """CTRP6504R/TTTS3012R 적용환율(bass_exrt) — 종목 output1 우선."""
    for rec in _rows(df_present_out1):
        v = _f(rec.get("bass_exrt"))
        if v > 0:
            return v
    for src in (pres2, bal2):
        v = _f(src.get("bass_exrt"))
        if v > 0:
            return v
    return 0.0


def normalize_overseas_holdings(df_hold: pd.DataFrame, market: str) -> pd.DataFrame:
    """해외 잔고 output1 → trader/recorder 호환 컬럼."""
    rows: List[Dict[str, Any]] = []
    for rec in _rows(df_hold):
        sym = norm_ticker(
            rec.get("ovrs_pdno") or rec.get("pdno") or rec.get("symb") or "",
            market,
        )
        if not sym:
            continue
        qty = _f(rec.get("ovrs_cblc_qty") or rec.get("hldg_qty") or rec.get("cblc_qty"))
        if qty <= 0:
            continue
        prpr = _f(rec.get("now_pric2") or rec.get("prpr") or rec.get("ovrs_now_pric1"))
        avg_px = _resolve_overseas_avg_price(rec, qty)
        evlu = _f(rec.get("frcr_evlu_amt2") or rec.get("evlu_amt") or rec.get("ovrs_stck_evlu_amt"))
        if evlu <= 0 and prpr > 0:
            evlu = prpr * qty
        pfls_amt = _f(rec.get("frcr_evlu_pfls_amt2") or rec.get("evlu_pfls_amt"))
        if pfls_amt == 0 and prpr > 0 and avg_px > 0 and qty > 0:
            pfls_amt = (prpr - avg_px) * qty
        pfls_rt = _f(rec.get("evlu_pfls_rt") or rec.get("frcr_evlu_pfls_rt"))
        if pfls_rt == 0 and avg_px > 0 and prpr > 0:
            pfls_rt = ((prpr - avg_px) / avg_px) * 100.0
        if os.getenv("KIS_TRACE", "").strip() in ("1", "true", "yes") and avg_px > 0:
            logger.info(
                "[KIS_TRACE] %s avg_px=%s qty=%s raw_pchs_avg=%s raw_frcr_pchs=%s prpr=%s",
                sym,
                avg_px,
                qty,
                rec.get("pchs_avg_pric"),
                rec.get("frcr_pchs_amt1"),
                prpr,
            )
        rows.append(
            {
                "pdno": sym,
                "prdt_name": str(
                    rec.get("ovrs_item_name")
                    or rec.get("prdt_name")
                    or rec.get("item_name")
                    or sym
                ),
                "hldg_qty": str(int(qty)),
                "pchs_avg_pric": str(avg_px),
                "prpr": str(prpr),
                "evlu_amt": str(evlu),
                "evlu_pfls_amt": str(pfls_amt),
                "evlu_pfls_rt": str(pfls_rt),
                "ovrs_excg_cd": str(rec.get("ovrs_excg_cd") or ""),
                "tr_crcy_cd": str(rec.get("tr_crcy_cd") or "USD"),
            }
        )
    return pd.DataFrame(rows)


def build_overseas_summary(
    df_bal_out2: pd.DataFrame,
    df_present_out2: Optional[pd.DataFrame] = None,
    df_present_out3: Optional[pd.DataFrame] = None,
    df_present_out1: Optional[pd.DataFrame] = None,
    df_present_out3_krw: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    해외 잔고/체결기준현재잔고 summary → extract_cash_from_summary 호환 dict.
    금액 단위: USD (정수 달러, 소수 반올림).
    """
    bal2 = _first_row(df_bal_out2)
    pres2 = _present_usd_row(df_present_out2)
    pres3 = _first_row(df_present_out3) if df_present_out3 is not None else {}
    pres3_krw = _first_row(df_present_out3_krw) if df_present_out3_krw is not None else {}

    # TTTS3012R output2 + CTRP6504R output2/3 (KIS 공식 필드명)
    #
    # ⚠️ 주의: CTRP6504R의 output3 일부 금액은 "원화 환산"으로 내려오는 경우가 있어
    #         USD(외화)와 KRW(원화환산)를 분리해 저장한다.
    usd_cash = _pick_positive(
        pres2,
        bal2,
        keys=(
            "frcr_dncl_amt_2",   # CTRP6504R output2: 외화예수금(USD)
            "frcr_dncl_amt1",
            "frcr_buy_amt",
            "frcr_buy_amt_smtl1",
            "frcr_buy_amt_smtl",
        ),
    )
    usd_order_psbl = _pick_positive(
        pres2,
        bal2,
        keys=(
            "frcr_drwg_psbl_amt_1",  # CTRP6504R output2: 외화출금가능금액(USD)
            "nxdy_frcr_drwg_psbl_amt",
            "ord_psbl_frcr_amt",
            "frcr_ord_psbl_amt",
            "frcr_gnrl_ord_psbl_amt",
        ),
    )
    krw_order_psbl = _pick_positive(
        pres3,
        keys=(
            "frcr_use_psbl_amt",  # CTRP6504R output3: (원화환산) 외화사용가능금액
            "wdrw_psbl_tot_amt",
            "tot_dncl_amt",
        ),
    )
    krw_tot_evlu = _pick_positive(
        pres3,
        keys=(
            "tot_asst_amt",
            "frcr_evlu_tota",
            "evlu_amt_smtl_amt",
            "evlu_amt_smtl",
        ),
    )
    usd_tot_evlu = _pick_positive(
        pres2,
        bal2,
        keys=("frcr_evlu_amt2", "tot_evlu_amt"),
    )
    pchs_smtl = _pick_positive(bal2, pres3, keys=("frcr_pchs_amt1", "pchs_amt_smtl", "pchs_amt_smtl_amt"))
    pfls_smtl = _pick_positive(
        bal2,
        pres3,
        keys=("frcr_evlu_pfls_amt2", "evlu_pfls_amt_smtl", "tot_evlu_pfls_amt"),
    )
    rlzt_pfls = _pick_positive(
        bal2,
        keys=("ovrs_rlzt_pfls_amt", "ovrs_rlzt_pfls_amt2", "rlzt_pfls_amt"),
    )
    bass_exrt = _pick_bass_exrt(df_present_out1, pres2, bal2)
    usd_withdrawable = _pick_positive(
        pres2,
        keys=("frcr_drwg_psbl_amt_1", "nxdy_frcr_drwg_psbl_amt"),
    )
    usd_sell_reuse = _pick_positive(
        pres2,
        pres3,
        keys=(
            "ruse_psbl_amt",
            "thdt_sll_ccld_frcr_amt",
            "thdt_sll_ccld_frcr_amt1",
            "frcr_sll_ruse_amt",
        ),
    )
    usd_buy_margin = _pick_positive(
        pres2,
        keys=("frcr_buy_mgn_amt", "buy_mgn_amt", "frcr_etc_mgna"),
    )
    krw_cash = _pick_positive(
        pres3_krw,
        pres3,
        keys=(
            "wcrc_dncl_amt",
            "dncl_amt",
            "krw_dncl_amt",
            "tot_dncl_amt",
        ),
    )

    available_usd = int(round(usd_order_psbl if usd_order_psbl > 0 else usd_cash))
    available_krw = int(round(krw_order_psbl))

    if available_usd <= 0 and os.getenv("KIS_TRACE", "").strip() == "1":
        try:
            logger.info(
                "[KIS_TRACE] overseas_summary candidates usd_order_psbl=%s usd_cash=%s usd_tot_evlu=%s krw_order_psbl=%s krw_tot_evlu=%s keys_bal2=%s keys_pres2=%s keys_pres3=%s",
                usd_order_psbl,
                usd_cash,
                usd_tot_evlu,
                krw_order_psbl,
                krw_tot_evlu,
                list(bal2.keys())[:50],
                list(pres2.keys())[:50],
                list(pres3.keys())[:50],
            )
        except Exception as e:
            logger.info("[KIS_TRACE] overseas_summary trace error: %s", e)

    order_psbl_val = usd_order_psbl if usd_order_psbl > 0 else usd_cash
    return {
        "currency": "USD",
        # USD (외화)
        "dnca_tot_amt": str(_i(usd_cash)),
        "prvs_rcdl_excc_amt": str(available_usd),
        "nxdy_excc_amt": str(available_usd),
        "ord_psbl_frcr_amt": str(_i(order_psbl_val)),
        "frcr_buy_amt": str(_i(usd_cash)),
        "tot_evlu_amt_usd": str(_i(usd_tot_evlu)),
        "available_cash": str(available_usd),
        "usd_cash_total": str(round(usd_cash, 2)),
        "usd_withdrawable": str(round(usd_withdrawable if usd_withdrawable > 0 else order_psbl_val, 2)),
        "usd_sell_reuse": str(round(usd_sell_reuse, 2)),
        "usd_buy_margin": str(round(usd_buy_margin, 2)),
        "ovrs_rlzt_pfls_amt": str(round(rlzt_pfls, 2)),
        "bass_exrt": str(round(bass_exrt, 4)),
        # KRW 환산(표시용)
        "available_cash_krw": str(_i(available_krw)),
        "tot_evlu_amt_krw": str(_i(krw_tot_evlu)),
        "krw_cash": str(_i(krw_cash)),
        "pchs_amt_smtl_amt": str(_i(pchs_smtl)),
        "evlu_pfls_smtl_amt": str(_i(pfls_smtl)),
        "status": "ok",
    }


def _kis_tr_id_balance(is_vps: bool) -> str:
    return "VTTS3012R" if is_vps else "TTTS3012R"


def _kis_tr_id_present(is_vps: bool) -> str:
    return "VTRP6504R" if is_vps else "CTRP6504R"


def _kis_tr_id_nccs(is_vps: bool) -> str:
    return "VTTS3018R" if is_vps else "TTTS3018R"


def _kis_tr_id_ccnl(is_vps: bool) -> str:
    return "VTTS3035R" if is_vps else "TTTS3035R"


def _norm_sym(val: Any, market: str) -> str:
    return norm_ticker(val, market)


def parse_nccs_sell_pending(df_nccs: pd.DataFrame, market: str) -> Dict[str, int]:
    """inquire-nccs 미체결 매도수량 → ticker별 합계."""
    pending: Dict[str, int] = {}
    if df_nccs is None or df_nccs.empty:
        return pending
    for rec in _rows(df_nccs):
        side = str(rec.get("sll_buy_dvsn_cd") or rec.get("sll_buy_dvsn") or "").strip()
        if side not in ("01", "1", "sell", "S"):
            continue
        sym = _norm_sym(rec.get("pdno") or rec.get("ovrs_pdno") or "", market)
        if not sym:
            continue
        qty = _i(rec.get("nccs_qty") or rec.get("ord_qty") or rec.get("ft_ord_qty"))
        if qty <= 0:
            continue
        pending[sym] = pending.get(sym, 0) + qty
    return pending


def parse_nccs_open_orders(df_nccs: pd.DataFrame, market: str) -> List[Dict[str, Any]]:
    """inquire-nccs → open_orders list."""
    orders: List[Dict[str, Any]] = []
    if df_nccs is None or df_nccs.empty:
        return orders
    for rec in _rows(df_nccs):
        sym = _norm_sym(rec.get("pdno") or rec.get("ovrs_pdno") or "", market)
        side_cd = str(rec.get("sll_buy_dvsn_cd") or rec.get("sll_buy_dvsn") or "")
        side = "sell" if side_cd in ("01", "1") else "buy"
        qty = _i(rec.get("nccs_qty") or rec.get("ord_qty") or rec.get("ft_ord_qty"))
        orders.append(
            {
                "order_id": str(rec.get("odno") or ""),
                "ticker": sym,
                "side": side,
                "quantity": qty,
                "ovrs_excg_cd": str(rec.get("ovrs_excg_cd") or ""),
            }
        )
    return orders


def _merge_balance_holdings(frames: List[pd.DataFrame], market: str) -> pd.DataFrame:
    """거래소별 balance output1 병합 (동일 ticker는 수량 합산)."""
    by_sym: Dict[str, Dict[str, Any]] = {}
    for df in frames:
        if df is None or df.empty:
            continue
        for rec in _rows(df):
            sym = _norm_sym(
                rec.get("ovrs_pdno") or rec.get("pdno") or rec.get("symb") or "",
                market,
            )
            if not sym:
                continue
            qty = _f(rec.get("ovrs_cblc_qty") or rec.get("hldg_qty") or rec.get("cblc_qty"))
            if qty <= 0:
                continue
            if sym not in by_sym:
                by_sym[sym] = dict(rec)
            else:
                prev_qty = _f(by_sym[sym].get("ovrs_cblc_qty") or by_sym[sym].get("hldg_qty"))
                new_qty = prev_qty + qty
                by_sym[sym]["ovrs_cblc_qty"] = str(new_qty)
                by_sym[sym]["hldg_qty"] = str(int(new_qty))
    if not by_sym:
        return pd.DataFrame()
    return pd.DataFrame(list(by_sym.values()))


def load_kis_account_snapshot(
    kis,
    market: str = "SP500",
    *,
    trade_date: Optional[str] = None,
    exchanges: Optional[tuple] = None,
    include_ccnl: bool = False,
    ccnl_start_ymd: Optional[str] = None,
    ccnl_end_ymd: Optional[str] = None,
) -> AccountSnapshot:
    """
    KIS 해외주식 balance/present/nccs(/ccnl) 엔드포인트로 AccountSnapshot 생성.
    NASD/NYSE/AMEX 거래소를 순회해 보유종목을 병합한다.
    """
    is_vps = getattr(kis, "env", "prod") == "vps"
    td = trade_date or datetime.now(KST).strftime("%Y%m%d")
    exc_list = list(exchanges or US_BALANCE_EXCHANGES)
    ts = datetime.now(KST).isoformat()

    snap = AccountSnapshot(
        trade_date=td,
        source="kis_endpoint",
        balance_endpoint=_kis_tr_id_balance(is_vps),
        present_balance_endpoint=_kis_tr_id_present(is_vps),
        nccs_endpoint=_kis_tr_id_nccs(is_vps),
        ccnl_endpoint=_kis_tr_id_ccnl(is_vps) if include_ccnl else None,
        snapshot_ts=ts,
        exchange_codes=exc_list,
    )
    tr_balance = _kis_tr_id_balance(is_vps)
    tr_present = _kis_tr_id_present(is_vps)
    tr_nccs = _kis_tr_id_nccs(is_vps)
    balance_status: Dict[str, str] = {}
    balance_row_count: Dict[str, int] = {}
    nccs_status: Dict[str, str] = {}
    nccs_row_count: Dict[str, int] = {}
    present_call_count = 0
    present_failed = False

    try:
        hold_frames: List[pd.DataFrame] = []
        bal2_frames: List[pd.DataFrame] = []
        queried_exchanges: List[str] = []

        for exc in exc_list:
            try:
                df_hold, df_bal2 = kis.inquire_overseas_balance(
                    ovrs_excg_cd=exc,
                    tr_crcy_cd="USD",
                )
                queried_exchanges.append(exc)
                rows = len(df_hold) if df_hold is not None and not df_hold.empty else 0
                balance_row_count[exc] = rows
                if rows > 0 or (df_bal2 is not None and not df_bal2.empty):
                    balance_status[exc] = "OK"
                else:
                    balance_status[exc] = "EMPTY"
                if df_hold is not None and not df_hold.empty:
                    hold_frames.append(df_hold)
                if df_bal2 is not None and not df_bal2.empty:
                    bal2_frames.append(df_bal2)
            except Exception as exc_err:
                balance_status[exc] = "FAILED"
                balance_row_count[exc] = 0
                logger.warning("inquire_overseas_balance(%s) 실패: %s", exc, exc_err)

        if not queried_exchanges:
            snap.valid = False
            snap.error = "KIS balance 조회 실패 (모든 거래소)"
            snap.endpoint_evidence = {
                "balance": {
                    "endpoint": "/uapi/overseas-stock/v1/trading/inquire-balance",
                    "expected_tr_ids": ["TTTS3012R", "VTTS3012R"],
                    "observed_tr_ids": [tr_balance],
                    "exchange_coverage": [],
                    "status_by_exchange": balance_status,
                    "row_count_by_exchange": balance_row_count,
                }
            }
            return snap

        merged_hold_raw = _merge_balance_holdings(hold_frames, market)
        holdings_df = normalize_overseas_holdings(merged_hold_raw, market)

        df_bal2 = pd.concat(bal2_frames, ignore_index=True) if bal2_frames else pd.DataFrame()

        df_pres1, df_pres2, df_pres3 = kis.inquire_overseas_present_balance(
            wcrc_frcr_dvsn_cd="02",
            natn_cd="840",
            tr_mket_cd="00",
            inqr_dvsn_cd="00",
        )
        present_call_count += 1
        df_pres3_krw = pd.DataFrame()
        try:
            _, _, df_pres3_krw = kis.inquire_overseas_present_balance(
                wcrc_frcr_dvsn_cd="01",
                natn_cd="840",
                tr_mket_cd="00",
            )
        except Exception as krw_err:
            logger.debug("present_balance KRW(01) 스킵: %s", krw_err)

        summary = build_overseas_summary(
            df_bal2,
            df_pres2,
            df_pres3,
            df_present_out1=df_pres1,
            df_present_out3_krw=df_pres3_krw,
        )
        summary["currency"] = "USD"

        nccs_frames: List[pd.DataFrame] = []
        for exc in exc_list:
            try:
                df_n = kis.inquire_nccs(ovrs_excg_cd=exc)
                if df_n is not None and not df_n.empty:
                    nccs_frames.append(df_n)
                    nccs_status[exc] = "OK"
                    nccs_row_count[exc] = len(df_n)
                else:
                    nccs_status[exc] = "EMPTY"
                    nccs_row_count[exc] = 0
            except Exception as nccs_err:
                nccs_status[exc] = "FAILED"
                nccs_row_count[exc] = 0
                logger.warning("inquire_nccs(%s) 실패: %s", exc, nccs_err)
        if not nccs_frames and hasattr(kis, "inquire_nccs"):
            try:
                df_n = kis.inquire_nccs(ovrs_excg_cd="NASD")
                if df_n is not None and not df_n.empty:
                    nccs_frames.append(df_n)
            except Exception:
                pass

        df_nccs = pd.concat(nccs_frames, ignore_index=True) if nccs_frames else pd.DataFrame()
        sell_pending = parse_nccs_sell_pending(df_nccs, market)
        open_orders = parse_nccs_open_orders(df_nccs, market)

        holdings = holdings_df.to_dict("records") if not holdings_df.empty else []
        holdings = [h for h in holdings if _i(h.get("hldg_qty")) > 0]
        tickers = sorted({_norm_sym(h.get("pdno"), market) for h in holdings if h.get("pdno")})

        def _nt(x):
            return _norm_sym(x, market)

        sellable = build_sellable_qty_map(holdings, sell_pending, norm_ticker_fn=_nt)

        cash_map: Dict[str, Any] = {
            "currency": "USD",
            "available_cash": _i(summary.get("available_cash")),
            "dnca_tot_amt": _i(summary.get("dnca_tot_amt")),
            "ord_psbl_frcr_amt": _i(summary.get("ord_psbl_frcr_amt")),
            "prvs_rcdl_excc_amt": _i(summary.get("prvs_rcdl_excc_amt")),
            "tot_evlu_amt_usd": _i(summary.get("tot_evlu_amt_usd")),
            "available_cash_krw": _i(summary.get("available_cash_krw")),
            "tot_evlu_amt_krw": _i(summary.get("tot_evlu_amt_krw")),
        }
        tot_evlu = sum(_f(h.get("evlu_amt")) for h in holdings) + float(cash_map["available_cash"])
        cash_map["tot_evlu_amt"] = int(round(tot_evlu))

        snap.holdings = holdings
        snap.cash_map = cash_map
        snap.open_orders = open_orders
        snap.sell_pending_qty_by_ticker = sell_pending
        snap.sellable_qty_by_ticker = sellable
        snap.holding_count = len(holdings)
        snap.tickers = tickers
        snap.exchange_codes = queried_exchanges

        if include_ccnl and ccnl_start_ymd and ccnl_end_ymd and hasattr(kis, "inquire_ccnl"):
            try:
                kis.inquire_ccnl(
                    ord_strt_dt=ccnl_start_ymd,
                    ord_end_dt=ccnl_end_ymd,
                    ovrs_excg_cd=exc_list[0],
                )
            except Exception as ccnl_err:
                logger.debug("inquire_ccnl optional 조회: %s", ccnl_err)

        if snap.holding_count > 0 and snap.total_value <= 0:
            snap.valid = False
            snap.invalid_reason = "ACCOUNT_SNAPSHOT_INVALID: holdings>0 total_value=0"

        present_status = "OK"
        if present_failed:
            present_status = "FAILED"
        elif present_call_count <= 0:
            present_status = "FAILED"
        elif not summary:
            present_status = "EMPTY"

        snap.endpoint_evidence = {
            "balance": {
                "endpoint": "/uapi/overseas-stock/v1/trading/inquire-balance",
                "expected_tr_ids": ["TTTS3012R", "VTTS3012R"],
                "observed_tr_ids": [tr_balance],
                "exchange_coverage": list(queried_exchanges),
                "status_by_exchange": balance_status,
                "row_count_by_exchange": balance_row_count,
            },
            "present_balance": {
                "endpoint": "/uapi/overseas-stock/v1/trading/inquire-present-balance",
                "expected_tr_ids": ["CTRP6504R", "VTRP6504R"],
                "observed_tr_ids": [tr_present],
                "call_count": present_call_count,
                "status": present_status,
            },
            "nccs": {
                "endpoint": "/uapi/overseas-stock/v1/trading/inquire-nccs",
                "expected_tr_ids": ["TTTS3018R", "VTTS3018R"],
                "observed_tr_ids": [tr_nccs],
                "exchange_coverage": [e for e, s in nccs_status.items() if s in ("OK", "EMPTY")],
                "status_by_exchange": nccs_status,
                "open_orders_count": len(open_orders),
            },
        }

        return snap

    except Exception as e:
        err = str(e)[:400]
        logger.error("KIS endpoint snapshot 생성 실패: %s", err, exc_info=True)
        snap.valid = False
        snap.error = err
        snap.endpoint_evidence = {
            "balance": {
                "endpoint": "/uapi/overseas-stock/v1/trading/inquire-balance",
                "expected_tr_ids": ["TTTS3012R", "VTTS3012R"],
                "observed_tr_ids": [tr_balance],
                "exchange_coverage": [],
                "status_by_exchange": balance_status,
                "row_count_by_exchange": balance_row_count,
            }
        }
        return snap


def inquire_overseas_account(
    kis,
    market: str = "SP500",
    *,
    tr_crcy_cd: str = "USD",
) -> Tuple[pd.DataFrame, Dict[str, Any], bool, str]:
    """
    해외 잔고 + 체결기준현재잔고 조회 후 정규화.
    NASD/NYSE/AMEX 거래소별 balance를 병합한다.
    반환: (holdings_df, summary_dict, degraded, error_msg)
    """
    last_err = ""

    try:
        hold_frames: List[pd.DataFrame] = []
        bal2_frames: List[pd.DataFrame] = []
        for exc in US_BALANCE_EXCHANGES:
            try:
                df_hold, df_bal2 = kis.inquire_overseas_balance(
                    ovrs_excg_cd=exc,
                    tr_crcy_cd=tr_crcy_cd,
                )
                if df_hold is not None and not df_hold.empty:
                    hold_frames.append(df_hold)
                if df_bal2 is not None and not df_bal2.empty:
                    bal2_frames.append(df_bal2)
            except Exception as exc_err:
                logger.debug("inquire_overseas_balance(%s) 스킵: %s", exc, exc_err)

        merged_hold_raw = _merge_balance_holdings(hold_frames, market)
        df_bal2 = pd.concat(bal2_frames, ignore_index=True) if bal2_frames else pd.DataFrame()

        df_pres1, df_pres2, df_pres3 = kis.inquire_overseas_present_balance(
            wcrc_frcr_dvsn_cd="02",
            natn_cd="840",
            tr_mket_cd="00",
        )
        df_pres3_krw = pd.DataFrame()
        try:
            _, _, df_pres3_krw = kis.inquire_overseas_present_balance(
                wcrc_frcr_dvsn_cd="01",
                natn_cd="840",
                tr_mket_cd="00",
            )
        except Exception as krw_err:
            logger.debug("CTRP6504R 원화(01) 조회 스킵: %s", krw_err)
        holdings = normalize_overseas_holdings(merged_hold_raw, market)
        summary = build_overseas_summary(
            df_bal2,
            df_pres2,
            df_pres3,
            df_present_out1=df_pres1,
            df_present_out3_krw=df_pres3_krw,
        )
        if not summary.get("available_cash") and summary.get("dnca_tot_amt") == "0":
            last_err = "해외 summary 금액 필드 비어있음(USD 예수금 0 또는 파싱 실패)"
            logger.warning(last_err)
        return holdings, summary, False, ""
    except Exception as e:
        last_err = str(e)[:400]
        logger.warning("해외 계좌 조회 실패: %s", last_err, exc_info=True)
        return pd.DataFrame(), {}, True, last_err
