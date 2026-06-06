# api/overseas_stock/overseas_stock_functions.py
"""KIS 해외주식 Open API 래퍼 ([해외주식] 기본시세 / 주문·계좌)."""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# 미국 거래소 코드 (주문 API)
_US_EXCH_ORDER = frozenset({"NASD", "NYSE", "AMEX", "NAS", "NYS", "AMS"})


class OverseasStock:
    """DomesticStock 과 동일하게 KIS 인스턴스에서 url_base/headers/request_get 을 상속."""

    def _quote_headers(self, tr_id: str) -> Dict[str, str]:
        return {"tr_id": tr_id, "custtype": "P"}

    def overseas_price(self, excd: str, symb: str) -> pd.DataFrame:
        """해외주식 현재체결가 — HHDFS00000300"""
        url = f"{self.url_base}/uapi/overseas-price/v1/quotations/price"
        params = {"AUTH": "", "EXCD": excd, "SYMB": symb}
        res = self.request_get(url, headers=self._quote_headers("HHDFS00000300"), params=params)
        if res.status_code != 200:
            logger.warning(
                "해외 현재가 실패 status=%s symb=%s: %s",
                res.status_code, symb, (res.text or "")[:200],
            )
            return pd.DataFrame()
        body = res.json()
        out = body.get("output") or body.get("output1")
        if isinstance(out, dict):
            return pd.DataFrame([out])
        return pd.DataFrame()

    @staticmethod
    def _parse_overseas_quote_row(row: Any) -> Optional[Dict[str, Any]]:
        """해외 현재가 API output → domestic get_realtime_price_with_quotes 호환 dict."""

        def _pf(val: Any) -> float:
            try:
                if val is None or val == "":
                    return 0.0
                return float(str(val).replace(",", "").strip())
            except (TypeError, ValueError):
                return 0.0

        px_f = _pf(
            row.get("last")
            or row.get("last_pr")
            or row.get("stck_prpr")
            or row.get("prpr")
        )
        if px_f <= 0:
            return None
        px = int(round(px_f))
        bid_f = _pf(row.get("bidp") or row.get("bid") or row.get("basp") or row.get("bidp1"))
        ask_f = _pf(row.get("askp") or row.get("ask") or row.get("bastp") or row.get("askp1"))
        bid = int(round(bid_f)) if bid_f > 0 else px
        ask = int(round(ask_f)) if ask_f > 0 else px
        chg_f = _pf(row.get("rate") or row.get("prdy_ctrt") or row.get("prdy_vrss_ctrt"))
        chg_amt_f = _pf(row.get("diff") or row.get("prdy_vrss") or row.get("change"))
        return {
            "current_price": px,
            "bid_price": bid,
            "ask_price": ask,
            "volume": int(_pf(row.get("acml_vol") or row.get("tvol") or row.get("vol"))),
            "change_rate": chg_f,
            "change_amount": int(round(chg_amt_f)) if chg_amt_f else 0,
            "high_price": int(round(_pf(row.get("high") or row.get("stck_hgpr")))) or px,
            "low_price": int(round(_pf(row.get("low") or row.get("stck_lwpr")))) or px,
            "open_price": int(round(_pf(row.get("open") or row.get("stck_oprc")))) or px,
            "prev_close": int(round(_pf(row.get("base") or row.get("stck_sdpr") or row.get("pvol")))) or px,
        }

    def get_realtime_price_with_quotes(self, ticker: str, market: Optional[str] = None):
        """해외주식 실시간 현재가·호가 (HHDFS00000300)."""
        import os
        from utils import is_us_market, norm_ticker, resolve_us_excd

        mkt = market or os.getenv("MARKET", "SP500")
        if not is_us_market(mkt):
            return None
        symb = norm_ticker(ticker, mkt)
        if not symb:
            return None
        excd = resolve_us_excd(symb, mkt)
        df = self.overseas_price(excd, symb)
        if df is None or df.empty:
            logger.warning("해외 실시간 시세 빈 응답: %s@%s", symb, excd)
            return None
        row = df.iloc[0]
        info = self._parse_overseas_quote_row(row.to_dict() if hasattr(row, "to_dict") else dict(row))
        if not info:
            logger.warning(
                "해외 실시간 시세 파싱 실패: %s@%s keys=%s",
                symb,
                excd,
                list(row.index) if hasattr(row, "index") else [],
            )
            return None
        if os.getenv("KIS_TRACE", "").strip() in ("1", "true", "yes"):
            logger.info(
                "KIS_TRACE overseas quote %s@%s px=%s bid=%s ask=%s",
                symb,
                excd,
                info.get("current_price"),
                info.get("bid_price"),
                info.get("ask_price"),
            )
        return info

    def overseas_price_detail(self, excd: str, symb: str) -> pd.DataFrame:
        """해외주식 현재가상세 — HHDFS76200200 (PER/PBR 등)"""
        url = f"{self.url_base}/uapi/overseas-price/v1/quotations/price-detail"
        params = {"AUTH": "", "EXCD": excd, "SYMB": symb}
        res = self.request_get(url, headers=self._quote_headers("HHDFS76200200"), params=params)
        if res.status_code != 200:
            logger.warning(
                "해외 price-detail 실패 status=%s symb=%s: %s",
                res.status_code, symb, (res.text or "")[:200],
            )
            return pd.DataFrame()
        body = res.json()
        out = body.get("output") or body.get("output1")
        if isinstance(out, dict):
            return pd.DataFrame([out])
        return pd.DataFrame()

    def overseas_daily_price(
        self,
        excd: str,
        symb: str,
        *,
        bymd: str = "",
        gubn: str = "0",
        modp: str = "0",
    ) -> pd.DataFrame:
        """해외주식 기간별시세 — HHDFS76240000. output2 일봉 리스트."""
        url = f"{self.url_base}/uapi/overseas-price/v1/quotations/dailyprice"
        params = {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": symb,
            "GUBN": gubn,
            "BYMD": bymd or "",
            "MODP": modp,
        }
        res = self.request_get(url, headers=self._quote_headers("HHDFS76240000"), params=params)
        if res.status_code != 200:
            logger.warning(
                "해외 dailyprice 실패 status=%s symb=%s: %s",
                res.status_code, symb, (res.text or "")[:200],
            )
            return pd.DataFrame()
        body = res.json()
        rows = body.get("output2") or []
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def overseas_daily_chart_price(
        self,
        market_code: str,
        symbol: str,
        date_from: str,
        date_to: str,
        *,
        period: str = "D",
    ) -> pd.DataFrame:
        """해외주식 종목/지수/환율 기간별시세 — FHKST03030100 (output2 일봉).

        미국 지수: FID_COND_MRKT_DIV_CODE=N, FID_INPUT_ISCD=SPX 등.
        """
        url = f"{self.url_base}/uapi/overseas-price/v1/quotations/inquire-daily-chartprice"
        params = {
            "FID_COND_MRKT_DIV_CODE": str(market_code or "N").strip(),
            "FID_INPUT_ISCD": str(symbol or "").strip().upper(),
            "FID_INPUT_DATE_1": str(date_from or "").strip(),
            "FID_INPUT_DATE_2": str(date_to or "").strip(),
            "FID_PERIOD_DIV_CODE": str(period or "D").strip().upper()[:1],
        }
        res = self.request_get(url, headers=self._quote_headers("FHKST03030100"), params=params)
        if res.status_code != 200:
            logger.warning(
                "해외 지수/차트 일봉 실패 status=%s %s %s~%s: %s",
                res.status_code,
                symbol,
                date_from,
                date_to,
                (res.text or "")[:200],
            )
            return pd.DataFrame()
        body = res.json()
        if str(body.get("rt_cd", "0")) not in ("0", ""):
            logger.warning(
                "해외 지수/차트 일봉 rt_cd=%s %s: %s",
                body.get("rt_cd"),
                symbol,
                str(body.get("msg1") or "")[:120],
            )
            return pd.DataFrame()
        rows = body.get("output2") or []
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    # ── 주문/계좌 ─────────────────────────────────────────────

    def _trading_headers(self, tr_id: str) -> Dict[str, str]:
        h = dict(getattr(self, "headers", {}) or {})
        h["tr_id"] = tr_id
        h["custtype"] = "P"
        return h

    def _parse_order_tr_id(self, ord_dv: str, ovrs_excg_cd: str) -> str:
        """ord_dv: buy/sell 또는 국내식 02(매수)/01(매도)."""
        side = str(ord_dv or "").strip().lower()
        if side in ("02", "2", "buy", "b"):
            is_buy = True
        elif side in ("01", "1", "sell", "s"):
            is_buy = False
        else:
            raise ValueError(f"알 수 없는 ord_dv: {ord_dv}")

        excd = str(ovrs_excg_cd or "NASD").upper()
        is_vps = getattr(self, "env", "prod") == "vps"

        if excd in _US_EXCH_ORDER or excd in ("NAS", "NYS", "AMS"):
            tr = "TTTT1002U" if is_buy else "TTTT1006U"
        elif excd == "SEHK":
            tr = "TTTS1002U" if is_buy else "TTTS1001U"
        elif excd == "SHAA":
            tr = "TTTS0202U" if is_buy else "TTTS1005U"
        elif excd == "SZAA":
            tr = "TTTS0305U" if is_buy else "TTTS0304U"
        elif excd == "TKSE":
            tr = "TTTS0308U" if is_buy else "TTTS0307U"
        elif excd in ("HASE", "VNSE"):
            tr = "TTTS0311U" if is_buy else "TTTS0310U"
        else:
            tr = "TTTT1002U" if is_buy else "TTTT1006U"

        if is_vps:
            tr = "V" + tr[1:]
        return tr

    def inquire_overseas_balance(
        self,
        ovrs_excg_cd: str = "NASD",
        tr_crcy_cd: str = "USD",
        ctx_area_fk200: str = "",
        ctx_area_nk200: str = "",
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        해외주식 잔고 — TTTS3012R / VTTS3012R
        반환: (output1 종목별, output2 요약)
        """
        url = f"{self.url_base}/uapi/overseas-stock/v1/trading/inquire-balance"
        is_vps = getattr(self, "env", "prod") == "vps"
        tr_id = "VTTS3012R" if is_vps else "TTTS3012R"
        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "OVRS_EXCG_CD": ovrs_excg_cd,
            "TR_CRCY_CD": tr_crcy_cd,
            "CTX_AREA_FK200": ctx_area_fk200 or "",
            "CTX_AREA_NK200": ctx_area_nk200 or "",
        }
        res = self.request_get(url, headers=self._trading_headers(tr_id), params=params)
        if res.status_code != 200:
            logger.warning(
                "해외 잔고 조회 실패 status=%s: %s",
                res.status_code,
                (res.text or "")[:200],
            )
            return pd.DataFrame(), pd.DataFrame()
        body = res.json()
        if os.getenv("KIS_TRACE", "").strip() == "1":
            try:
                o2 = body.get("output2") or []
                if isinstance(o2, dict):
                    o2 = [o2]
                k2 = list((o2[0] or {}).keys())[:50] if o2 else []
                logger.info(
                    "[KIS_TRACE] inquire_overseas_balance tr_id=%s rt_cd=%s msg_cd=%s msg1=%s out2_keys=%s",
                    tr_id,
                    body.get("rt_cd"),
                    body.get("msg_cd"),
                    str(body.get("msg1") or "")[:120],
                    k2,
                )
            except Exception as e:
                logger.info("[KIS_TRACE] inquire_overseas_balance parse error: %s", e)
        o1 = body.get("output1") or []
        o2 = body.get("output2") or []
        if isinstance(o1, dict):
            o1 = [o1]
        if isinstance(o2, dict):
            o2 = [o2]
        return pd.DataFrame(o1), pd.DataFrame(o2)

    def inquire_overseas_present_balance(
        self,
        wcrc_frcr_dvsn_cd: str = "02",
        natn_cd: str = "840",
        tr_mket_cd: str = "00",
        inqr_dvsn_cd: str = "00",
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        해외주식 체결기준현재잔고 — CTRP6504R / VTRP6504R
        wcrc_frcr_dvsn_cd: 01=원화, 02=외화
        """
        url = f"{self.url_base}/uapi/overseas-stock/v1/trading/inquire-present-balance"
        is_vps = getattr(self, "env", "prod") == "vps"
        tr_id = "VTRP6504R" if is_vps else "CTRP6504R"
        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "WCRC_FRCR_DVSN_CD": wcrc_frcr_dvsn_cd,
            "NATN_CD": natn_cd,
            "TR_MKET_CD": tr_mket_cd,
            "INQR_DVSN_CD": inqr_dvsn_cd,
        }
        res = self.request_get(url, headers=self._trading_headers(tr_id), params=params)
        if res.status_code != 200:
            logger.warning(
                "해외 체결기준잔고 실패 status=%s: %s",
                res.status_code,
                (res.text or "")[:200],
            )
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        body = res.json()
        if os.getenv("KIS_TRACE", "").strip() == "1":
            try:
                o2 = body.get("output2") or []
                o3 = body.get("output3") or []
                if isinstance(o2, dict):
                    o2 = [o2]
                if isinstance(o3, dict):
                    o3 = [o3]
                k2 = list((o2[0] or {}).keys())[:50] if o2 else []
                k3 = list((o3[0] or {}).keys())[:50] if o3 else []
                logger.info(
                    "[KIS_TRACE] inquire_overseas_present_balance tr_id=%s rt_cd=%s msg_cd=%s msg1=%s out2_keys=%s out3_keys=%s",
                    tr_id,
                    body.get("rt_cd"),
                    body.get("msg_cd"),
                    str(body.get("msg1") or "")[:120],
                    k2,
                    k3,
                )
            except Exception as e:
                logger.info("[KIS_TRACE] inquire_overseas_present_balance parse error: %s", e)

        def _df(key: str) -> pd.DataFrame:
            raw = body.get(key) or []
            if isinstance(raw, dict):
                raw = [raw]
            return pd.DataFrame(raw) if raw else pd.DataFrame()

        return _df("output1"), _df("output2"), _df("output3")

    def overseas_order(
        self,
        ord_dv: str,
        pdno: str,
        ord_dvsn: str,
        ord_qty: int,
        ord_unpr: int,
        *,
        ovrs_excg_cd: str = "NASD",
        ctac_tlno: str = "",
        mgco_aptm_odno: str = "",
        ord_svr_dvsn_cd: str = "0",
    ) -> pd.DataFrame:
        """
        해외주식 주문 — TTTT1002U(미국매수) / TTTT1006U(미국매도) 등
        ord_dv: buy/sell 또는 02(매수)/01(매도)
        ord_dvsn: 00=지정가, 시장가는 거래소별 상이(미국 매수는 00 권장)
        """
        url = f"{self.url_base}/uapi/overseas-stock/v1/trading/order"
        symb = str(pdno or "").strip().upper()
        qty = int(ord_qty)
        price = int(ord_unpr)
        excd = str(ovrs_excg_cd or "NASD").upper()

        side = str(ord_dv or "").strip().lower()
        is_sell = side in ("01", "1", "sell", "s")
        tr_id = self._parse_order_tr_id(ord_dv, excd)

        if is_sell and price <= 0:
            logger.error(
                "해외 매도 거부: OVRS_ORD_UNPR=0 (sym=%s excd=%s ord_dvsn=%s)",
                symb,
                excd,
                ord_dvsn,
            )
            return pd.DataFrame()

        if is_sell:
            unpr_str = str(price)
            ord_dvsn = str(ord_dvsn or "00")
            if ord_dvsn not in ("00", "0", "31", "32", "33", "34"):
                logger.warning(
                    "해외 매도 ord_dvsn=%s → 00(지정가)로 정규화 (sym=%s)",
                    ord_dvsn,
                    symb,
                )
                ord_dvsn = "00"
        else:
            # 매수: 단가 0 허용(시장가 등)
            unpr_str = "0" if price <= 0 else str(price)
            if price <= 0 and str(ord_dvsn) not in ("00", "0"):
                ord_dvsn = "00"

        data = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "OVRS_EXCG_CD": excd,
            "PDNO": symb,
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": unpr_str,
            "CTAC_TLNO": ctac_tlno or "",
            "MGCO_APTM_ODNO": mgco_aptm_odno or "",
            "SLL_TYPE": "00" if is_sell else "",
            "ORD_SVR_DVSN_CD": ord_svr_dvsn_cd or "0",
            "ORD_DVSN": str(ord_dvsn or "00"),
        }

        res = self.request_post(
            url,
            headers=self._trading_headers(tr_id),
            data=json.dumps(data),
        )
        if res.status_code != 200:
            logger.warning(
                "해외 주문 실패 status=%s %s %s: %s",
                res.status_code,
                excd,
                symb,
                (res.text or "")[:300],
            )
            return pd.DataFrame()
        body = res.json()
        out = body.get("output") or body.get("output1")
        if isinstance(out, dict):
            row = dict(out)
            row.setdefault("rt_cd", body.get("rt_cd", ""))
            row.setdefault("msg_cd", body.get("msg_cd", ""))
            row.setdefault("msg1", body.get("msg1", ""))
            return pd.DataFrame([row])
        logger.warning(
            "해외 주문 응답 output 없음 %s %s rt_cd=%s msg_cd=%s msg1=%s",
            excd,
            symb,
            body.get("rt_cd"),
            body.get("msg_cd"),
            body.get("msg1"),
        )
        return pd.DataFrame()
