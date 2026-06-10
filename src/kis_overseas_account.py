# src/kis_overseas_account.py
"""KIS 해외주식 잔고 조회 → 국내 balance/summary JSON 호환 형식으로 정규화."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils import us_ovrs_excg_cd, norm_ticker

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
) -> Dict[str, Any]:
    """
    해외 잔고/체결기준현재잔고 summary → extract_cash_from_summary 호환 dict.
    금액 단위: USD (정수 달러, 소수 반올림).
    """
    bal2 = _first_row(df_bal_out2)
    pres2 = _present_usd_row(df_present_out2)
    pres3 = _first_row(df_present_out3) if df_present_out3 is not None else {}

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

    return {
        "currency": "USD",
        # USD (외화)
        "dnca_tot_amt": str(_i(usd_cash)),
        "prvs_rcdl_excc_amt": str(available_usd),
        "nxdy_excc_amt": str(available_usd),
        "ord_psbl_frcr_amt": str(_i(usd_order_psbl if usd_order_psbl > 0 else usd_cash)),
        "frcr_buy_amt": str(_i(usd_cash)),
        "tot_evlu_amt_usd": str(_i(usd_tot_evlu)),
        "available_cash": str(available_usd),
        # KRW 환산(표시용)
        "available_cash_krw": str(_i(available_krw)),
        "tot_evlu_amt_krw": str(_i(krw_tot_evlu)),
        "pchs_amt_smtl_amt": str(_i(pchs_smtl)),
        "evlu_pfls_smtl_amt": str(_i(pfls_smtl)),
        "status": "ok",
    }


def inquire_overseas_account(
    kis,
    market: str = "SP500",
    *,
    tr_crcy_cd: str = "USD",
) -> Tuple[pd.DataFrame, Dict[str, Any], bool, str]:
    """
    해외 잔고 + 체결기준현재잔고 조회 후 정규화.
    반환: (holdings_df, summary_dict, degraded, error_msg)
    """
    ovrs_excg = us_ovrs_excg_cd(market)
    last_err = ""

    try:
        df_hold, df_bal2 = kis.inquire_overseas_balance(
            ovrs_excg_cd=ovrs_excg,
            tr_crcy_cd=tr_crcy_cd,
        )
        df_pres1, df_pres2, df_pres3 = kis.inquire_overseas_present_balance(
            wcrc_frcr_dvsn_cd="02",
            natn_cd="840",
            tr_mket_cd="00",
        )
        if df_hold is None:
            df_hold = pd.DataFrame()
        holdings = normalize_overseas_holdings(df_hold, market)
        summary = build_overseas_summary(df_bal2, df_pres2, df_pres3)
        if not summary.get("available_cash") and summary.get("dnca_tot_amt") == "0":
            last_err = "해외 summary 금액 필드 비어있음(USD 예수금 0 또는 파싱 실패)"
            logger.warning(last_err)
        return holdings, summary, False, ""
    except Exception as e:
        last_err = str(e)[:400]
        logger.warning("해외 계좌 조회 실패: %s", last_err, exc_info=True)
        return pd.DataFrame(), {}, True, last_err
