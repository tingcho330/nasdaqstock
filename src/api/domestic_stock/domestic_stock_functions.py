import os
from utils import normalize_ticker_6
import requests
import json
import pandas as pd
import logging
import time
from datetime import datetime, timedelta

# 로깅 설정
logger = logging.getLogger(__name__)

class DomesticStock:
    def __init__(self):
        # This will be initialized by the KIS class
        self.url_base = getattr(self, 'url_base', '')
        self.headers = getattr(self, 'headers', {})
        self.cano = getattr(self, 'cano', '')
        self.acnt_prdt_cd = getattr(self, 'acnt_prdt_cd', '')

    def inquire_price(self, fid_cond_mrkt_div_code: str, fid_input_iscd: str):
        """
        주식현재가 시세
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"
        
        params = {
            "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
            "FID_INPUT_ISCD": fid_input_iscd,
        }
        
        res = self.request_get(url, headers={"tr_id": tr_id}, params=params)
        
        # Check if the request was successful
        if res.status_code == 200:
            return pd.DataFrame([res.json()['output']])
        else:
            logger.warning(f"현재가 조회 실패 (status={res.status_code}, ticker={fid_input_iscd}): {res.text[:200]}")
            return pd.DataFrame()

    def inquire_balance(self, inqr_dvsn: str, afhr_flpr_yn: str, ofl_yn: str, 
                        unpr_dvsn: str, fund_sttl_icld_yn: str, fncg_amt_auto_rdpt_yn: str, 
                        prcs_dvsn: str, ctx_area_fk100: str = "", ctx_area_nk100: str = ""):
        """
        주식 잔고 조회
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/trading/inquire-balance"
        is_vps = getattr(self, 'env', 'prod') == 'vps'
        tr_id = "VTTC8434R" if is_vps else "TTTC8434R"
        
        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "AFHR_FLPR_YN": afhr_flpr_yn,
            "OFL_YN": ofl_yn,
            "INQR_DVSN": inqr_dvsn,
            "UNPR_DVSN": unpr_dvsn,
            "FUND_STTL_ICLD_YN": fund_sttl_icld_yn,
            "FNCG_AMT_AUTO_RDPT_YN": fncg_amt_auto_rdpt_yn,
            "PRCS_DVSN": prcs_dvsn,
            "CTX_AREA_FK100": ctx_area_fk100,
            "CTX_AREA_NK100": ctx_area_nk100
        }
        
        res = self.request_get(url, headers={"tr_id": tr_id}, params=params)
        
        if res.status_code == 200:
            df_balance = pd.DataFrame(res.json()['output1'])
            df_summary = pd.DataFrame([res.json()['output2']])
            return df_balance, df_summary
        else:
            logger.warning(f"잔고 조회 실패 (status={res.status_code}): {res.text[:200]}")
            return pd.DataFrame(), pd.DataFrame()

    def order_cash(self, ord_dv: str, pdno: str, ord_dvsn: str, ord_qty: int, ord_unpr: int):
        """
        현금 주문
        ord_dv: "01": 매도, "02": 매수
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "TTTC0802U" if ord_dv == "02" else "TTTC0801U"
        
        data = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "PDNO": pdno,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(ord_qty),
            "ORD_UNPR": str(ord_unpr),
        }
        
        res = self.request_post(url, headers={"tr_id": tr_id}, data=json.dumps(data))
        
        if res.status_code == 200:
            return pd.DataFrame([res.json()['output']])
        else:
            logger.warning(f"현금 주문 실패 (status={res.status_code}, ticker={pdno}, ord_dv={ord_dv}): {res.text[:200]}")
            return pd.DataFrame()

    def inquire_orders(self, inqr_dvsn: str = "00", inqr_strt_ymd: str = "", 
                      inqr_end_ymd: str = "", sll_buy_dvsn_cd: str = "00", 
                      inqr_dvsn_cd: str = "00", pdno: str = "", 
                      ccano: str = "", ord_gno_brno: str = "", 
                      odno: str = "", inqr_dvsn_3: str = "00", 
                      inqr_dvsn_3_odno: str = "", cts_evlu_pnl_amt: str = "0", 
                      cts_evlu_pnl_rt: str = "0", evlu_pnl_amt: str = "0", 
                      evlu_pnl_rt: str = "0", tot_evlu_pnl_amt: str = "0", 
                      tot_evlu_pnl_rt: str = "0", sll_buy_dvsn_cd_2: str = "00", 
                      inqr_dvsn_cd_2: str = "00", ctx_area_fk100: str = "", 
                      ctx_area_nk100: str = ""):
        """
        주식 주문 조회 (미체결 주문 포함)
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/trading/inquire-orders"
        is_vps = getattr(self, 'env', 'prod') == 'vps'
        tr_id = "VTTC8001R" if is_vps else "TTTC8001R"
        
        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "INQR_DVSN": inqr_dvsn,
            "INQR_STRT_YMD": inqr_strt_ymd,
            "INQR_END_YMD": inqr_end_ymd,
            "SLL_BUY_DVSN_CD": sll_buy_dvsn_cd,
            "INQR_DVSN_CD": inqr_dvsn_cd,
            "PDNO": pdno,
            "CCANO": ccano,
            "ORD_GNO_BRNO": ord_gno_brno,
            "ODNO": odno,
            "INQR_DVSN_3": inqr_dvsn_3,
            "INQR_DVSN_3_ODNO": inqr_dvsn_3_odno,
            "CTS_EVLU_PNL_AMT": cts_evlu_pnl_amt,
            "CTS_EVLU_PNL_RT": cts_evlu_pnl_rt,
            "EVLU_PNL_AMT": evlu_pnl_amt,
            "EVLU_PNL_RT": evlu_pnl_rt,
            "TOT_EVLU_PNL_AMT": tot_evlu_pnl_amt,
            "TOT_EVLU_PNL_RT": tot_evlu_pnl_rt,
            "SLL_BUY_DVSN_CD_2": sll_buy_dvsn_cd_2,
            "INQR_DVSN_CD_2": inqr_dvsn_cd_2,
            "CTX_AREA_FK100": ctx_area_fk100,
            "CTX_AREA_NK100": ctx_area_nk100
        }
        
        res = self.request_get(url, headers={"tr_id": tr_id}, params=params)
        
        if res.status_code == 200:
            data = res.json()
            if 'output1' in data and data['output1']:
                return pd.DataFrame(data['output1'])
            else:
                # 미체결 주문이 없는 경우 (정상 케이스)
                logger.debug(f"미체결 주문 없음 (inquire_orders: pdno={pdno or '전체'})")
                return pd.DataFrame()
        elif res.status_code == 404:
            # 404는 미체결 주문이 없는 정상적인 경우
            logger.debug(f"미체결 주문 없음 (404 응답, inquire_orders: pdno={pdno or '전체'})")
            return pd.DataFrame()
        else:
            # 기타 에러는 로깅
            error_msg = f"주문 조회 실패 (status={res.status_code}, pdno={pdno or '전체'}): {res.text[:200]}"
            logger.warning(error_msg)
            return pd.DataFrame()

    def inquire_daily_order(self, cano: str, acnt_prdt_cd: str, inqr_strt_dt: str,
                           inqr_end_dt: str, sll_buy_dvsn_cd: str = "00",
                           inqr_dvsn: str = "00", ccld_dvsn: str = "00",
                           pdno: str = "", odno: str = "",
                           inqr_dvsn_3: str = "00", inqr_dvsn_1: str = "",
                           inqr_dvsn_2: str = "", max_pages: int = 20,
                           # ── 하위 호환(구 시그니처) 무시용 인자 ──
                           sort_ord: str = "2", ord_gnno_yn: str = "N",
                           tot_ccld_qty_smtl_yn: str = "N"):
        """
        주식일별주문체결조회 (inquire-daily-ccld).

        - 체결/미체결 주문을 모두 조회(CCLD_DVSN='00').
        - 응답은 output1(주문 목록)에 담긴다. output2는 요약.
        - 연속조회(tr_cont / CTX_AREA_FK100·NK100)를 자동 처리한다.
        - TR_ID: 실전 TTTC8001R / 모의 VTTC8001R (3개월 이내).
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        is_vps = getattr(self, 'env', 'prod') == 'vps'
        tr_id = "VTTC8001R" if is_vps else "TTTC8001R"

        frames = []
        ctx_fk100 = ""
        ctx_nk100 = ""
        tr_cont = ""  # 최초 조회는 공란
        for _page in range(max(1, max_pages)):
            params = {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "INQR_STRT_DT": inqr_strt_dt,
                "INQR_END_DT": inqr_end_dt,
                "SLL_BUY_DVSN_CD": sll_buy_dvsn_cd,
                "INQR_DVSN": inqr_dvsn,
                "PDNO": pdno,
                "CCLD_DVSN": ccld_dvsn,
                "ORD_GNO_BRNO": "",
                "ODNO": odno,
                "INQR_DVSN_3": inqr_dvsn_3,
                "INQR_DVSN_1": inqr_dvsn_1,
                "INQR_DVSN_2": inqr_dvsn_2,
                "CTX_AREA_FK100": ctx_fk100,
                "CTX_AREA_NK100": ctx_nk100,
            }
            headers = {"tr_id": tr_id}
            if tr_cont:
                headers["tr_cont"] = tr_cont

            res = self.request_get(url, headers=headers, params=params)

            if res.status_code != 200:
                if res.status_code == 404:
                    logger.debug(
                        f"일자별 주문체결 조회 결과 없음 (404, start={inqr_strt_dt}, end={inqr_end_dt})"
                    )
                else:
                    logger.warning(
                        f"일자별 주문체결 조회 실패 (status={res.status_code}, "
                        f"start={inqr_strt_dt}, end={inqr_end_dt}): {res.text[:200]}"
                    )
                break

            data = res.json()
            rt_cd = str(data.get("rt_cd", ""))
            if rt_cd and rt_cd != "0":
                logger.warning(
                    f"일자별 주문체결 조회 응답 오류 (rt_cd={rt_cd}, "
                    f"msg_cd={data.get('msg_cd')}, msg={str(data.get('msg1',''))[:120]})"
                )
                break

            rows = data.get("output1") or []
            if rows:
                frames.append(pd.DataFrame(rows))

            # 연속조회 판단: 응답 헤더 tr_cont가 F/M이면 다음 페이지 존재
            tr_cont_resp = str(res.headers.get("tr_cont", "")).strip().upper()
            ctx_fk100 = str(data.get("ctx_area_fk100", "") or "").strip()
            ctx_nk100 = str(data.get("ctx_area_nk100", "") or "").strip()
            if tr_cont_resp in ("F", "M") and ctx_nk100:
                tr_cont = "N"  # 다음 페이지 요청
                continue
            break

        if not frames:
            logger.debug(
                f"일자별 주문체결 조회 결과 0건 (start={inqr_strt_dt}, end={inqr_end_dt})"
            )
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        # odno 공란(요약/빈 행) 제거
        if "odno" in df.columns:
            df = df[df["odno"].astype(str).str.strip() != ""].reset_index(drop=True)
        return df

    def inquire_stock_list(self, fid_cond_mrkt_div_code: str = "J", 
                          fid_input_iscd: str = "0000", 
                          fid_input_iscd_2: str = "9999"):
        """
        주식 종목 리스트 조회
        fid_cond_mrkt_div_code: J=KOSPI, Q=KOSDAQ, N=KONEX
        fid_input_iscd: 시작 종목코드 (0000부터)
        fid_input_iscd_2: 종료 종목코드 (9999까지)
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/quotations/inquire-stock-list"
        tr_id = "FHKST01010100"
        
        params = {
            "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
            "FID_INPUT_ISCD": fid_input_iscd,
            "FID_INPUT_ISCD_2": fid_input_iscd_2,
        }
        
        res = self.request_get(url, headers={"tr_id": tr_id}, params=params)
        
        if res.status_code == 200:
            data = res.json()
            if 'output' in data and data['output']:
                return pd.DataFrame(data['output'])
            else:
                logger.debug(f"종목 리스트 조회 결과 없음 (market={fid_cond_mrkt_div_code})")
                return pd.DataFrame()
        else:
            logger.warning(f"종목 리스트 조회 실패 (status={res.status_code}, market={fid_cond_mrkt_div_code}): {res.text[:200]}")
            return pd.DataFrame()

    def cancel_order(self, cano: str, acnt_prdt_cd: str, pdno: str, 
                    ord_dvsn: str, ord_qty: str, ord_unpr: str, 
                    ord_gno_brno: str, odno: str, sll_buy_dvsn_cd: str = "00"):
        """
        주식 주문 취소
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/trading/order-cash"
        is_vps = getattr(self, 'env', 'prod') == 'vps'
        tr_id = "VTTC0803U" if is_vps else "TTTC0803U"
        
        data = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "PDNO": pdno,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": ord_qty,
            "ORD_UNPR": ord_unpr,
            "ORD_GNO_BRNO": ord_gno_brno,
            "ODNO": odno,
            "SLL_BUY_DVSN_CD": sll_buy_dvsn_cd
        }
        
        res = self.request_post(url, headers={"tr_id": tr_id}, data=json.dumps(data))
        
        if res.status_code == 200:
            return res.json()
        else:
            logger.warning(f"주문 취소 실패 (status={res.status_code}, ticker={pdno}, odno={odno}): {res.text[:200]}")
            return None

    def get_pending_orders(self):
        """
        미체결 주문 조회 (간편 버전)
        """
        # 오늘 날짜로 조회
        today = datetime.now().strftime("%Y%m%d")
        return self.inquire_orders(
            inqr_dvsn="00",  # 전체
            inqr_strt_ymd=today,
            inqr_end_ymd=today,
            sll_buy_dvsn_cd="00",  # 전체
            inqr_dvsn_cd="00"  # 전체
        )

    def cancel_all_pending_orders(self, ticker: str = None):
        """
        모든 미체결 주문 취소 (특정 종목 지정 가능)
        """
        pending_orders = self.get_pending_orders()
        
        if pending_orders.empty:
            logger.debug("[ORDER] 취소할 미체결 주문이 없습니다.")
            return []
        
        cancelled_orders = []
        ticker_filter = f" (종목: {ticker})" if ticker else ""
        logger.info(f"[ORDER] 미체결 주문 {len(pending_orders)}건 발견{ticker_filter}, 취소 시작")
        
        for _, order in pending_orders.iterrows():
            # 특정 종목 필터링
            if ticker and normalize_ticker_6(order.get('pdno', ''), os.getenv('MARKET', 'KOSPI')) != normalize_ticker_6(ticker, os.getenv('MARKET', 'KOSPI')):
                continue
                
            try:
                result = self.cancel_order(
                    cano=self.cano,
                    acnt_prdt_cd=self.acnt_prdt_cd,
                    pdno=order.get('pdno', ''),
                    ord_dvsn=order.get('ord_dvsn', ''),
                    ord_qty=str(order.get('ord_qty', '')),
                    ord_unpr=str(order.get('ord_unpr', '')),
                    ord_gno_brno=order.get('ord_gno_brno', ''),
                    odno=order.get('odno', ''),
                    sll_buy_dvsn_cd=order.get('sll_buy_dvsn_cd', '00')
                )
                
                if result and result.get('rt_cd') == '0':
                    cancelled_orders.append({
                        'ticker': order.get('pdno', ''),
                        'name': order.get('prdt_name', ''),
                        'side': '매수' if order.get('sll_buy_dvsn_cd', '') == '02' else '매도',
                        'qty': order.get('ord_qty', ''),
                        'price': order.get('ord_unpr', ''),
                        'status': '취소완료'
                    })
                    logger.info(f"[ORDER] 주문 취소 완료: {order.get('prdt_name', '')}({order.get('pdno', '')})")
                else:
                    logger.warning(f"[ORDER] 주문 취소 실패: {order.get('prdt_name', '')}({order.get('pdno', '')}) - {result}")
                    
            except Exception as e:
                logger.error(f"[ORDER] 주문 취소 중 오류: {order.get('prdt_name', '')}({order.get('pdno', '')}) - {e}", exc_info=True)
        
        if cancelled_orders:
            logger.info(f"[ORDER] 총 {len(cancelled_orders)}건 주문 취소 완료")
        
        return cancelled_orders

    def get_realtime_price_with_quotes(self, ticker: str):
        """
        실시간 현재가 및 호가 정보 조회
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # 주식
            "FID_INPUT_ISCD": ticker,
        }
        
        res = self.request_get(url, headers={"tr_id": tr_id}, params=params)
        
        if res.status_code == 200:
            data = res.json()
            if 'output' in data:
                output = data['output']
                return {
                    'current_price': int(output.get('stck_prpr', 0)),
                    'bid_price': int(output.get('bidp', 0)),
                    'ask_price': int(output.get('askp', 0)),
                    'volume': int(output.get('acml_vol', 0)),
                    'change_rate': float(output.get('prdy_vrss_ctrt', 0)),
                    'change_amount': int(output.get('prdy_vrss', 0)),
                    'high_price': int(output.get('stck_hgpr', 0)),
                    'low_price': int(output.get('stck_lwpr', 0)),
                    'open_price': int(output.get('stck_oprc', 0)),
                    'prev_close': int(output.get('stck_sdpr', 0))
                }
        else:
            logger.warning(f"실시간 가격 조회 실패 (status={res.status_code}, ticker={ticker}): {res.text[:200]}")
            return None

    def get_multiple_realtime_prices(self, tickers: list):
        """
        여러 종목의 실시간 가격 정보를 한 번에 조회
        """
        results = {}
        for ticker in tickers:
            try:
                price_info = self.get_realtime_price_with_quotes(ticker)
                if price_info:
                    results[ticker] = price_info
                time.sleep(0.1)  # API 호출 간격 조절
            except Exception as e:
                logger.warning(f"가격 조회 중 오류 (ticker={ticker}): {e}")
                results[ticker] = None
        return results

    # ────────────────────────────────────────────────────────────────
    # Screener용: 기간별 시세 / 투자자별 추이 / 업종 일자별
    # ────────────────────────────────────────────────────────────────
    def inquire_period_price(
        self,
        *,
        fid_cond_mrkt_div_code: str,
        fid_input_iscd: str,
        fid_input_date_1: str,
        fid_input_date_2: str,
        fid_period_div_code: str = "D",
        fid_org_adj_prc: str = "0",
    ) -> pd.DataFrame:
        """
        국내주식 기간별 시세 (일/주/월)
        - TR: FHKST03010100
        - URL(일반적으로 사용): /uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        tr_id = "FHKST03010100"
        params = {
            "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
            "FID_INPUT_ISCD": fid_input_iscd,
            "FID_INPUT_DATE_1": fid_input_date_1,
            "FID_INPUT_DATE_2": fid_input_date_2,
            "FID_PERIOD_DIV_CODE": fid_period_div_code,
            "FID_ORG_ADJ_PRC": fid_org_adj_prc,
        }
        try:
            res = self.request_get(url, headers={"tr_id": tr_id}, params=params, timeout=10)
            if res.status_code != 200:
                logger.debug("기간별시세 실패(status=%s, ticker=%s): %s", res.status_code, fid_input_iscd, res.text[:200])
                return pd.DataFrame()
            data = res.json()
            # KIS는 보통 output1(요약), output2(시계열)
            out = data.get("output2") or data.get("output") or []
            if not out:
                return pd.DataFrame()
            return pd.DataFrame(out)
        except Exception as e:
            logger.debug("기간별시세 예외(ticker=%s): %s", fid_input_iscd, e)
            return pd.DataFrame()

    def inquire_investor_trend(
        self,
        *,
        fid_cond_mrkt_div_code: str,
        fid_input_iscd: str,
        fid_input_date_1: str,
        fid_input_date_2: str,
    ) -> pd.DataFrame:
        """
        주식 투자자별 추이 (외국인/기관 등)
        - TR: FHKST01010900
        - URL(문서 기준): /uapi/domestic-stock/v1/quotations/foreign-institution-total
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/quotations/foreign-institution-total"
        tr_id = "FHKST01010900"
        params = {
            "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
            "FID_INPUT_ISCD": fid_input_iscd,
            "FID_INPUT_DATE_1": fid_input_date_1,
            "FID_INPUT_DATE_2": fid_input_date_2,
        }
        try:
            res = self.request_get(url, headers={"tr_id": tr_id}, params=params, timeout=10)
            if res.status_code != 200:
                logger.debug("투자자추이 실패(status=%s, ticker=%s): %s", res.status_code, fid_input_iscd, res.text[:200])
                return pd.DataFrame()
            data = res.json()
            out = data.get("output") or data.get("output2") or []
            if not out:
                return pd.DataFrame()
            return pd.DataFrame(out)
        except Exception as e:
            logger.debug("투자자추이 예외(ticker=%s): %s", fid_input_iscd, e)
            return pd.DataFrame()

    def inquire_industry_period_price(
        self,
        *,
        fid_input_iscd: str,
        fid_input_date_1: str,
        fid_input_date_2: str,
        fid_period_div_code: str = "D",
        fid_cond_mrkt_div_code: str = "U",
    ) -> pd.DataFrame:
        """
        국내주식업종 기간별시세(일/주/월/년)
        - TR: FHKUP03500100
        - URL: /uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice
          (과거 'inquire-daily-industrychartprice'는 존재하지 않는 경로로 404 발생)
        - 업종/지수 조회에는 FID_COND_MRKT_DIV_CODE="U"(업종)가 필수.
        """
        url = f"{self.url_base}/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
        tr_id = "FHKUP03500100"
        params = {
            "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
            "FID_INPUT_ISCD": fid_input_iscd,
            "FID_INPUT_DATE_1": fid_input_date_1,
            "FID_INPUT_DATE_2": fid_input_date_2,
            "FID_PERIOD_DIV_CODE": fid_period_div_code,
        }
        try:
            res = self.request_get(url, headers={"tr_id": tr_id}, params=params, timeout=10)
            if res.status_code != 200:
                # 404 등 원인 추적을 위해 호출 URL/파라미터와 전체 응답 본문을 남긴다.
                logger.debug(
                    "업종일자별 실패(status=%s, code=%s)\n  url=%s\n  params=%s\n  resp=%s",
                    res.status_code, fid_input_iscd, url, params, res.text,
                )
                return pd.DataFrame()
            data = res.json()
            out = data.get("output2") or data.get("output") or []
            if not out:
                return pd.DataFrame()
            return pd.DataFrame(out)
        except Exception as e:
            logger.debug("업종일자별 예외(code=%s): %s", fid_input_iscd, e)
            return pd.DataFrame()