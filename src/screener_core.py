#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Screener Core Module - 최적화된 스크리너 핵심 기능

주요 기능:
1. 기술적 지표 계산
2. 스크리닝 로직
3. 점수 계산
4. 시장 분석 (기본)
5. 거래 비용 계산
6. 리스크 관리 (기본)
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock as pykrx

# ───────────────── pykrx 커스텀 헤더 패치 적용 ─────────────────
# 패치는 모듈 레벨에서 한 번만 적용되면 모든 곳에서 적용되므로,
# screener.py에서 이미 적용되었더라도 중복 적용해도 안전합니다.
CUSTOM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
}

def _apply_pykrx_patch():
    """pykrx 라이브러리에 커스텀 헤더 패치 적용 (내부 함수)"""
    try:
        from pykrx.website.comm import webio
        import requests
        
        def _patched_get_read(self, **params):
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd"
            }
            return requests.get(self.url, headers=headers, params=params)
        
        def _patched_post_read(self, **params):
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd"
            }
            return requests.post(self.url, headers=headers, data=params)
        
        # 기존 메서드를 새 메서드로 대체
        webio.Get.read = _patched_get_read
        webio.Post.read = _patched_post_read
        
        return True
    except Exception:
        # 패치 실패 시 무시 (이미 다른 곳에서 적용되었을 수 있음)
        return False

# 패치 적용 (모듈 로드 시 자동 적용)
_apply_pykrx_patch()

# reviewer.py와 recorder.py에서 import
from reviewer import MarketRegime, MarketState
from recorder import DataRecorder

logger = logging.getLogger(__name__)

def _compute_levels(
    ticker: str,
    current_price: float,
    date_str: str,
    risk_params: Dict[str, Any],
    strategy_params: Optional[Dict[str, Any]] = None,
    *,
    market: Optional[str] = None,
    kis: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    손절/목표가 계산 (통합 함수)
    우선순위: strategy_params → risk_params → 기본값
    
    Args:
        ticker: 종목 코드
        current_price: 현재 가격
        date_str: 날짜 문자열 (YYYYMMDD)
        risk_params: 위험 관리 파라미터
        strategy_params: 전략 파라미터 (선택적)
    
    Returns:
        Dict with 손절가, 목표가, source
    """
    try:
        # Phase 1: 설정값 우선순위 적용 (strategy_params → risk_params → 기본값)
        if strategy_params:
            stop_loss_pct = (
                strategy_params.get("stop_loss_pct") or 
                risk_params.get("stop_pct") or 
                risk_params.get("auto_sell", {}).get("stop_loss_pct") or 
                0.03
            )
            target_pct = (
                strategy_params.get("take_profit_pct") or 
                risk_params.get("auto_sell", {}).get("target_pct") or 
                0.08
            )
            atr_k_stop = strategy_params.get("atr_k_stop", 2.0)
            atr_k_profit = strategy_params.get("atr_k_profit", 4.0)
        else:
            stop_loss_pct = (
                risk_params.get("stop_pct") or 
                risk_params.get("auto_sell", {}).get("stop_loss_pct") or 
                0.03
            )
            target_pct = (
                risk_params.get("auto_sell", {}).get("target_pct") or 
                0.08
            )
            atr_k_stop = risk_params.get("atr_k_stop", 2.0)
            atr_k_profit = risk_params.get("atr_k_profit", 4.0)  # 수정: risk_params에서도 읽기
        
        # 현재 가격을 float로 변환
        price = float(current_price)
        
        # 기본 손절/목표가 계산
        stop_loss = price * (1 - stop_loss_pct)
        target = price * (1 + target_pct)
        
        # 과거 데이터를 통한 고급 계산 시도
        try:
            end_date = date_str
            start_dt = datetime.strptime(date_str, "%Y%m%d") - timedelta(days=60)
            start_date = start_dt.strftime("%Y%m%d")
            
            df = get_historical_prices(
                ticker, start_date, end_date, market=market, kis=kis
            )
            if df is not None and len(df) > 20:
                # ATR 계산
                atr = calculate_atr(df, period=14)
                
                # 스윙 고저점 계산 (한국어 우선)
                close_col = None
                high_col = None
                low_col = None
                
                if '종가' in df.columns:
                    close_col = '종가'
                elif 'close' in df.columns:
                    close_col = 'close'
                elif 'Close' in df.columns:
                    close_col = 'Close'
                    
                if '고가' in df.columns:
                    high_col = '고가'
                elif 'high' in df.columns:
                    high_col = 'high'
                elif 'High' in df.columns:
                    high_col = 'High'
                    
                if '저가' in df.columns:
                    low_col = '저가'
                elif 'low' in df.columns:
                    low_col = 'low'
                elif 'Low' in df.columns:
                    low_col = 'Low'
                
                if not all([close_col, high_col, low_col]):
                    logger.debug(f"필요한 컬럼을 찾을 수 없음: {df.columns.tolist()}")
                    raise ValueError("Required columns not found")
                
                recent_high = df[high_col].tail(20).max()
                recent_low = df[low_col].tail(20).min()
                
                if atr > 0 and recent_high > 0 and recent_low > 0:
                    # Phase 1: ATR 기반 계산 (하드코딩 제거, config 값 사용)
                    atr_stop_loss = price - (atr * atr_k_stop)
                    atr_target = price + (atr * atr_k_profit)
                    
                    # 스윙 기반 계산
                    swing_stop_loss = recent_low * 0.95
                    swing_target = recent_high * 1.15
                    
                    # 가장 보수적인 값 선택
                    stop_loss = max(atr_stop_loss, swing_stop_loss, stop_loss)
                    target = min(atr_target, swing_target, target)
                    
                    return {
                        "손절가": stop_loss,
                        "목표가": target,
                        "source": "atr_swing"
                    }
                    
        except Exception as e:
            logger.debug(f"고급 손절/목표가 계산 실패 ({ticker}): {e}")
        
        return {
            "손절가": stop_loss,
            "목표가": target,
            "source": "percent_backup"
        }
        
    except Exception as e:
        logger.error(f"손절/목표가 계산 실패 ({ticker}): {e}")
        # Phase 1: 최종 백업 (하드코딩 제거, config 값 사용)
        try:
            price = float(current_price)
            return {
                "손절가": price * (1 - stop_loss_pct),
                "목표가": price * (1 + target_pct),
                "source": "fallback"
            }
        except:
            return {
                "손절가": 0,
                "목표가": 0,
                "source": "error"
            }

def get_historical_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    retries: int = 3,
    *,
    market: Optional[str] = None,
    kis: Optional[Any] = None,
) -> Optional[pd.DataFrame]:
    """
    과거 시세 조회 — KIS 우선, 실패 시 pykrx/fdr (US는 KIS만).
    
    Args:
        symbol: 종목 코드
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
        retries: 재시도 횟수
        market: MARKET (미지정 시 환경변수)
        kis: KIS 인스턴스 (risk_manager 등에서 주입)
    
    Returns:
        DataFrame with OHLCV data or None if failed
    """
    import os
    import time
    from utils import is_us_market

    if not symbol or not str(symbol).strip():
        logger.debug("get_historical_prices: empty symbol, skipping")
        return None

    mkt = (market or os.getenv("MARKET", "SP500")).upper().strip()

    try:
        from kis_market_data import get_historical_prices_kis

        df_kis = get_historical_prices_kis(
            symbol,
            start_date,
            end_date,
            market=mkt,
            kis=kis,
            retries=retries,
        )
        if df_kis is not None and not df_kis.empty:
            return df_kis
    except Exception as e:
        logger.debug("KIS get_historical_prices 실패(%s): %s", symbol, e)

    if is_us_market(mkt):
        logger.warning(
            "US 일봉 KIS 조회 실패 — pykrx/fdr 미사용: %s (%s~%s)",
            symbol,
            start_date,
            end_date,
        )
        return None

    for attempt in range(retries):
        try:
            # 1단계: pykrx 시도
            try:
                df = pykrx.get_market_ohlcv(start_date, end_date, symbol)
                if df is not None and not df.empty:
                    logger.debug(f"pykrx success for {symbol}: {len(df)} rows")
                    return df
            except Exception as e:
                logger.debug(f"pykrx attempt {attempt + 1} failed for {symbol}: {e}")
            
            # 2단계: fdr 시도
            try:
                start_dt = datetime.strptime(start_date, '%Y%m%d')
                end_dt = datetime.strptime(end_date, '%Y%m%d')
                
                df = fdr.DataReader(symbol, start=start_dt, end=end_dt)
                if df is not None and not df.empty:
                    logger.debug(f"fdr success for {symbol}: {len(df)} rows")
                    return df
            except Exception as e:
                logger.debug(f"fdr attempt {attempt + 1} failed for {symbol}: {e}")
            
            # 3단계: 날짜 범위 확장 시도 (마지막 시도에서만)
            if attempt == retries - 1:
                try:
                    # 시작일을 더 앞으로 확장
                    start_dt = datetime.strptime(start_date, '%Y%m%d') - timedelta(days=30)
                    extended_start = start_dt.strftime('%Y%m%d')
                    
                    df = pykrx.get_market_ohlcv(extended_start, end_date, symbol)
                    if df is not None and not df.empty:
                        # 원하는 시작일 이후 데이터만 필터링
                        df = df[df.index >= start_date]
                        if not df.empty:
                            logger.debug(f"pykrx with extended range success for {symbol}: {len(df)} rows")
                            return df
                except Exception as e:
                    logger.debug(f"extended range pykrx failed for {symbol}: {e}")
            
            # 재시도 전 대기
            if attempt < retries - 1:
                time.sleep(0.5 * (attempt + 1))
                
        except Exception as e:
            logger.warning(f"Error in attempt {attempt + 1} for {symbol}: {e}")
            if attempt < retries - 1:
                time.sleep(0.5 * (attempt + 1))
    
    if symbol and str(symbol).strip():
        logger.warning(f"All attempts failed for {symbol} ({start_date} to {end_date})")
    return None

class MarketAnalyzer:
    """시장 분석기 (기본 기능)"""
    
    def __init__(
        self,
        settings: Dict[str, Any],
        *,
        kis: Optional[Any] = None,
        market: str = "KOSPI",
        date_str: Optional[str] = None,
    ):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.kis = kis
        self.market = (market or "KOSPI").upper()
        self.date_str = date_str

    def _paginated_close(self, fetch_fn, end_dt: str, *, min_bars: int = 260, max_pages: int = 6) -> Optional[pd.Series]:
        """fetch_fn(start, end)->df 를 종료일을 과거로 밀며 여러 번 호출해 종가를 누적,
        '오름차순(과거→현재)' Series로 반환한다.

        KIS 기간별시세는 1회 응답이 ~100봉으로 제한되므로, MA200 산출을 위해
        end_dt를 과거로 이동시키며 min_bars 이상을 모은다(YYYYMMDD 문자열 인덱스).
        """
        merged: Dict[str, float] = {}
        cur_end = str(end_dt)
        for _ in range(max(1, max_pages)):
            start = (datetime.strptime(cur_end, "%Y%m%d") - timedelta(days=200)).strftime("%Y%m%d")
            try:
                df = fetch_fn(start, cur_end)
            except Exception as e:
                self.logger.debug("지수 종가 페이지 조회 실패(end=%s): %s", cur_end, e)
                break
            if df is None or getattr(df, "empty", True):
                break
            date_col = next((c for c in ["stck_bsop_date", "bsop_date", "date", "Date"] if c in df.columns), None)
            close_col = next((c for c in ["bstp_nmix_prpr", "stck_clpr", "clspr", "close", "Close", "종가"] if c in df.columns), None)
            if close_col is None:
                break
            if date_col is None:
                # 날짜가 없으면 더 거슬러 올라갈 수 없음 → 단일 페이지 종가만 사용
                vals = pd.to_numeric(df[close_col], errors="coerce").dropna()
                return vals.reset_index(drop=True) if len(vals) else None
            prev_n = len(merged)
            for d, c in zip(df[date_col].astype(str), pd.to_numeric(df[close_col], errors="coerce")):
                if d and pd.notna(c):
                    merged[d] = float(c)
            if len(merged) >= min_bars:
                break
            if len(merged) <= prev_n:
                break  # 새 데이터 없음(중복만 수신)
            earliest = min(merged.keys())
            try:
                next_end = (datetime.strptime(earliest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
            except Exception:
                break
            if next_end >= cur_end:
                break
            cur_end = next_end
        if not merged:
            return None
        return pd.Series(merged).sort_index()

    def analyze_market_state(self) -> MarketState:
        """시장 상태 분석"""
        try:
            current_time = datetime.now()

            from utils import is_us_market, us_regime_benchmark

            # US: SPY 일봉 (fdr) — S&P500 레짐
            if is_us_market(self.market):
                end_dt = self.date_str or current_time.strftime("%Y%m%d")
                sym = us_regime_benchmark(self.market) or "SPY"
                start_dt = (
                    datetime.strptime(end_dt, "%Y%m%d") - timedelta(days=500)
                ).strftime("%Y%m%d")
                df = get_historical_prices(sym, start_dt, end_dt, market=self.market, kis=self.kis)
                if df is not None and not df.empty and "Close" in df.columns:
                    close = pd.to_numeric(df["Close"], errors="coerce").dropna()
                    if close is not None and len(close) >= 60:
                        ma20 = close.rolling(20).mean()
                        ma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else close.rolling(20).mean().iloc[-1]
                        ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else float("nan")
                        rsi_val = calculate_rsi(close)
                        rsi = (
                            float(rsi_val.iloc[-1])
                            if isinstance(rsi_val, pd.Series) and len(rsi_val)
                            else float(rsi_val) if rsi_val is not None else 50.0
                        )
                        returns = close.pct_change().dropna()
                        vol_ann = float(returns.std() * (252 ** 0.5)) if len(returns) else 0.0
                        if vol_ann < 0.15:
                            volatility_level = "low"
                        elif vol_ann < 0.25:
                            volatility_level = "medium"
                        else:
                            volatility_level = "high"
                        trend_direction = "sideways"
                        try:
                            ma20_last = float(ma20.iloc[-1])
                            ma20_prev = float(ma20.iloc[-6])
                            delta = (ma20_last - ma20_prev) / ma20_prev if ma20_prev else 0.0
                            if delta > 0.003:
                                trend_direction = "up"
                            elif delta < -0.003:
                                trend_direction = "down"
                        except Exception:
                            pass
                        last_close = float(close.iloc[-1])
                        ma50_gt_ma200 = (ma50 > ma200) if not pd.isna(ma200) else None
                        is_bull = (last_close > ma50) and ((ma50_gt_ma200 is None) or ma50_gt_ma200) and (rsi >= 55)
                        is_bear = (last_close < ma50) and ((ma50_gt_ma200 is None) or (not ma50_gt_ma200)) and (rsi <= 45)
                        if vol_ann >= 0.35:
                            regime = MarketRegime.VOLATILE
                        elif is_bull:
                            regime = MarketRegime.BULL
                        elif is_bear:
                            regime = MarketRegime.BEAR
                        else:
                            regime = MarketRegime.SIDEWAYS
                        rsi_term = max(0.0, 1 - abs(rsi - 50) / 50)
                        ma_term = 0.5 if (ma50_gt_ma200 is None) else (1.0 if ma50_gt_ma200 else 0.0)
                        score = ((1 if last_close > ma50 else 0) + ma_term + rsi_term) / 3.0
                        confidence = 0.50 + min(0.40, abs(score - 0.5) * 0.8)
                        if regime == MarketRegime.VOLATILE:
                            confidence = max(0.50, confidence - 0.10)
                        self.logger.info("US market state from %s (%d bars)", sym, len(close))
                        return MarketState(
                            regime=regime,
                            volatility_level=volatility_level,
                            trend_direction=trend_direction,
                            confidence=confidence,
                            timestamp=current_time,
                        )

            # KIS 업종지수 일자별(일봉) 기반 결정적 시장판단
            if self.kis is not None:
                end_dt = self.date_str or current_time.strftime("%Y%m%d")
                idx_code = "0001" if self.market == "KOSPI" else "1001"

                # 업종지수 일봉을 페이지네이션으로 200봉+ 누적(MA200 산출 목적).
                close = self._paginated_close(
                    lambda s, e: self.kis.inquire_industry_period_price(
                        fid_input_iscd=idx_code,
                        fid_input_date_1=s,
                        fid_input_date_2=e,
                        fid_period_div_code="D",
                    ),
                    end_dt,
                    min_bars=260,
                )
                # 업종지수가 부실하면 지수추종 ETF(KODEX200/KODEX코스닥150)로 폴백.
                if close is None or len(close) < 60:
                    proxy = "069500" if self.market == "KOSPI" else "229200"
                    close = self._paginated_close(
                        lambda s, e: self.kis.inquire_period_price(
                            fid_cond_mrkt_div_code="J",
                            fid_input_iscd=proxy,
                            fid_input_date_1=s,
                            fid_input_date_2=e,
                            fid_period_div_code="D",
                            fid_org_adj_prc="0",
                        ),
                        end_dt,
                        min_bars=260,
                    )
                if close is None or len(close) < 60:
                    self.logger.warning(
                        "KIS index history insufficient (industry_code=%s, end=%s) → sideways fallback",
                        idx_code,
                        end_dt,
                    )
                    return MarketState(
                        regime=MarketRegime.SIDEWAYS,
                        volatility_level="medium",
                        trend_direction="sideways",
                        confidence=0.5,
                        timestamp=current_time,
                    )

                ma20 = close.rolling(20).mean()
                ma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else close.rolling(20).mean().iloc[-1]
                ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else float("nan")

                rsi_val = calculate_rsi(close)
                rsi = (
                    float(rsi_val.iloc[-1])
                    if isinstance(rsi_val, pd.Series) and len(rsi_val)
                    else float(rsi_val) if rsi_val is not None else 50.0
                )

                # 변동성(연율화) + 레벨
                returns = close.pct_change().dropna()
                vol_ann = float(returns.std() * (252 ** 0.5)) if len(returns) else 0.0
                if vol_ann < 0.15:
                    volatility_level = "low"
                elif vol_ann < 0.25:
                    volatility_level = "medium"
                else:
                    volatility_level = "high"

                # 추세 방향: MA20 5영업일 변화
                trend_direction = "sideways"
                try:
                    ma20_last = float(ma20.iloc[-1])
                    ma20_prev = float(ma20.iloc[-6])
                    delta = (ma20_last - ma20_prev) / ma20_prev if ma20_prev else 0.0
                    if delta > 0.003:
                        trend_direction = "up"
                    elif delta < -0.003:
                        trend_direction = "down"
                except Exception:
                    pass

                # 레짐 결정 (ma200이 없으면 중립 처리)
                last_close = float(close.iloc[-1])
                ma50_gt_ma200 = (ma50 > ma200) if not pd.isna(ma200) else None
                is_bull = (last_close > ma50) and ((ma50_gt_ma200 is None) or ma50_gt_ma200) and (rsi >= 55)
                is_bear = (last_close < ma50) and ((ma50_gt_ma200 is None) or (not ma50_gt_ma200)) and (rsi <= 45)
                if vol_ann >= 0.35:
                    regime = MarketRegime.VOLATILE
                elif is_bull:
                    regime = MarketRegime.BULL
                elif is_bear:
                    regime = MarketRegime.BEAR
                else:
                    regime = MarketRegime.SIDEWAYS

                # 신뢰도: (MA/RSI 일치 정도 + 변동성 페널티)
                rsi_term = max(0.0, 1 - abs(rsi - 50) / 50)
                ma_term = 0.5 if (ma50_gt_ma200 is None) else (1.0 if ma50_gt_ma200 else 0.0)
                score = ((1 if last_close > ma50 else 0) + ma_term + rsi_term) / 3.0
                confidence = 0.50 + min(0.40, abs(score - 0.5) * 0.8)
                if regime == MarketRegime.VOLATILE:
                    confidence = max(0.50, confidence - 0.10)

                return MarketState(
                    regime=regime,
                    volatility_level=volatility_level,
                    trend_direction=trend_direction,
                    confidence=float(confidence),
                    timestamp=current_time,
                )

            # KIS가 없으면 보수적으로 sideways로 폴백(결정적)
            return MarketState(
                regime=MarketRegime.SIDEWAYS,
                volatility_level="medium",
                trend_direction="sideways",
                confidence=0.5,
                timestamp=current_time,
            )
            
        except Exception as e:
            self.logger.error(f"시장 상태 분석 실패: {e}")
            return MarketState(
                regime=MarketRegime.SIDEWAYS,
                volatility_level="medium",
                trend_direction="sideways",
                confidence=0.5,
                timestamp=datetime.now()
            )
    
    def calculate_dynamic_threshold(self, base_threshold: float, market_state: MarketState) -> float:
        """동적 임계값 계산"""
        try:
            regime_multiplier = {
                MarketRegime.BULL: 0.8,
                MarketRegime.BEAR: 1.5,
                MarketRegime.SIDEWAYS: 1.0,
                MarketRegime.VOLATILE: 2.0
            }
            
            volatility_multiplier = {
                "low": 0.8,
                "medium": 1.0,
                "high": 1.5
            }
            
            regime_factor = regime_multiplier.get(market_state.regime, 1.0)
            volatility_factor = volatility_multiplier.get(market_state.volatility_level, 1.0)
            
            adjusted_threshold = base_threshold * regime_factor * volatility_factor
            return max(0.01, min(0.5, adjusted_threshold))
            
        except Exception as e:
            self.logger.error(f"동적 임계값 계산 실패: {e}")
            return base_threshold
    
    def get_market_summary(self, market_state: MarketState) -> str:
        """시장 요약"""
        return f"시장상황: {market_state.regime.value} | 변동성: {market_state.volatility_level} | 추세: {market_state.trend_direction} | 신뢰도: {market_state.confidence:.2f}"

def calculate_atr(df_price: pd.DataFrame, period: int = 14) -> float:
    """ATR (Average True Range) 계산"""
    try:
        if len(df_price) < period + 1:
            return 0.0
        
        # 컬럼명 대소문자 및 한국어 처리
        high_col = None
        low_col = None
        close_col = None
        
        # 한국어 컬럼명 우선 확인
        if '고가' in df_price.columns:
            high_col = '고가'
        elif 'high' in df_price.columns:
            high_col = 'high'
        elif 'High' in df_price.columns:
            high_col = 'High'
            
        if '저가' in df_price.columns:
            low_col = '저가'
        elif 'low' in df_price.columns:
            low_col = 'low'
        elif 'Low' in df_price.columns:
            low_col = 'Low'
            
        if '종가' in df_price.columns:
            close_col = '종가'
        elif 'close' in df_price.columns:
            close_col = 'close'
        elif 'Close' in df_price.columns:
            close_col = 'Close'
        
        if not all([high_col, low_col, close_col]):
            logger.error(f"필요한 컬럼을 찾을 수 없음: {df_price.columns.tolist()}")
            return 0.0
        
        high = df_price[high_col]
        low = df_price[low_col]
        close = df_price[close_col]
        
        # True Range 계산
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # ATR 계산 (단순 이동평균)
        atr = true_range.rolling(window=period).mean().iloc[-1]
        
        return float(atr) if not pd.isna(atr) else 0.0
        
    except Exception as e:
        logger.error(f"ATR 계산 실패: {e}")
        return 0.0

def calculate_rsi(prices: List[float], period: int = 14) -> float:
    """RSI 계산"""
    try:
        if len(prices) < period + 1:
            return 50.0
        
        # prices가 pandas Series인지 리스트인지 확인
        if hasattr(prices, 'iloc'):
            # pandas Series인 경우
            deltas = [prices.iloc[i] - prices.iloc[i-1] for i in range(1, len(prices))]
        else:
            # 리스트인 경우
            deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        
        if len(gains) < period:
            return 50.0
            
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        # avg_loss가 0이면 RSI 100.0 (14일 연속 상승)
        # 이는 매우 드문 상황이므로 경고 로그 추가
        if avg_loss == 0:
            if avg_gain > 0:
                logger.warning(
                    f"RSI 계산: avg_loss=0 (14일 연속 상승) → RSI=100.0. "
                    f"이는 비정상적으로 높은 값입니다. 데이터를 확인하세요."
                )
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # RSI 값 검증 (99.0 이상은 극단값)
        if rsi >= 99.0:
            logger.warning(
                f"RSI 극단값 감지: {rsi:.2f} (≥99.0). "
                f"데이터 문제 또는 실제로 매우 강한 상승 추세일 수 있습니다."
            )
        
        return rsi
        
    except Exception as e:
        logger.error(f"RSI 계산 실패: {e}")
        return 50.0

def calculate_macd(prices: List[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Dict[str, float]:
    """MACD 계산"""
    try:
        if len(prices) < slow_period:
            return {"macd": 0.0, "signal": 0.0, "histogram": 0.0}
        
        prices_array = np.array(prices)
        
        # EMA 계산
        def calculate_ema(data, period):
            alpha = 2.0 / (period + 1)
            ema = np.zeros_like(data)
            ema[0] = data[0]
            for i in range(1, len(data)):
                ema[i] = alpha * data[i] + (1 - alpha) * ema[i-1]
            return ema
        
        fast_ema = calculate_ema(prices_array, fast_period)
        slow_ema = calculate_ema(prices_array, slow_period)
        
        macd_line = fast_ema - slow_ema
        signal_line = calculate_ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        
        return {
            "macd": macd_line[-1],
            "signal": signal_line[-1],
            "histogram": histogram[-1]
        }
        
    except Exception as e:
        logger.error(f"MACD 계산 실패: {e}")
        return {"macd": 0.0, "signal": 0.0, "histogram": 0.0}

def calculate_bollinger_bands(prices: List[float], period: int = 20, std_dev: float = 2.0) -> Dict[str, float]:
    """볼린저 밴드 계산"""
    try:
        if len(prices) < period:
            return {"upper": 0.0, "middle": 0.0, "lower": 0.0}
        
        recent_prices = prices[-period:]
        middle = np.mean(recent_prices)
        std = np.std(recent_prices)
        
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)
        
        return {
            "upper": upper,
            "middle": middle,
            "lower": lower
        }
        
    except Exception as e:
        logger.error(f"볼린저 밴드 계산 실패: {e}")
        return {"upper": 0.0, "middle": 0.0, "lower": 0.0}

def calculate_technical_score(ticker: str, prices: List[float], volumes: List[float]) -> float:
    """기술적 점수 계산"""
    try:
        if len(prices) < 20:
            return 0.0
        
        score = 0.0
        
        # RSI 점수 (30-70 범위에서 선호)
        rsi = calculate_rsi(prices)
        if 30 <= rsi <= 70:
            score += 0.2
        elif rsi < 30:  # 과매도
            score += 0.3
        elif rsi > 70:  # 과매수
            score += 0.1
        
        # MACD 점수
        macd_data = calculate_macd(prices)
        if macd_data["macd"] > macd_data["signal"]:
            score += 0.2
        
        # 볼린저 밴드 점수
        bb_data = calculate_bollinger_bands(prices)
        current_price = prices[-1]
        if bb_data["lower"] < current_price < bb_data["upper"]:
            score += 0.2
        elif current_price < bb_data["lower"]:  # 하단 터치
            score += 0.3
        
        # 가격 모멘텀 점수
        if len(prices) >= 5:
            short_ma = np.mean(prices[-5:])
            long_ma = np.mean(prices[-20:])
            if short_ma > long_ma:
                score += 0.2
        
        # 거래량 점수
        if len(volumes) >= 20:
            recent_volume = np.mean(volumes[-5:])
            avg_volume = np.mean(volumes[-20:])
            if recent_volume > avg_volume * 1.2:  # 거래량 증가
                score += 0.1
        
        return min(1.0, max(0.0, score))
        
    except Exception as e:
        logger.error(f"기술적 점수 계산 실패: {e}")
        return 0.0

def calculate_market_adjusted_score(base_score: float, market_state: MarketState) -> float:
    """시장 상황에 따른 점수 조정"""
    try:
        # 시장 상황에 따른 가중치
        regime_weights = {
            MarketRegime.BULL: 1.1,
            MarketRegime.BEAR: 0.9,
            MarketRegime.SIDEWAYS: 1.0,
            MarketRegime.VOLATILE: 0.95
        }
        
        # 변동성에 따른 가중치
        volatility_weights = {
            "low": 1.05,
            "medium": 1.0,
            "high": 0.95
        }
        
        regime_weight = regime_weights.get(market_state.regime, 1.0)
        volatility_weight = volatility_weights.get(market_state.volatility_level, 1.0)
        
        adjusted_score = base_score * regime_weight * volatility_weight
        return min(1.0, max(0.0, adjusted_score))
        
    except Exception as e:
        logger.error(f"시장 조정 점수 계산 실패: {e}")
        return base_score

def get_market_aware_screening_params(market_state: MarketState, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """시장 상황별 스크리닝 파라미터 (기본값에 base 오버레이 가능)"""
    try:
        base_params = {
            "min_volume": 100000,
            "min_price": 1000,
            "max_price": 1000000,
            "min_market_cap": 100000000000,  # 1000억
            "min_rsi": 20,
            "max_rsi": 80
        }
        if isinstance(base, dict) and base:
            # caller가 가진 screener_params를 기본값 위에 얹는다.
            base_params.update(base)
        
        # 시장 상황별 조정
        if market_state.regime == MarketRegime.BULL:
            base_params["min_rsi"] = 30
            base_params["max_rsi"] = 85
        elif market_state.regime == MarketRegime.BEAR:
            base_params["min_rsi"] = 15
            base_params["max_rsi"] = 70
        elif market_state.regime == MarketRegime.VOLATILE:
            base_params["min_rsi"] = 25
            base_params["max_rsi"] = 75
        
        return base_params
        
    except Exception as e:
        logger.error(f"시장 상황별 스크리닝 파라미터 생성 실패: {e}")
        return {
            "min_volume": 100000,
            "min_price": 1000,
            "max_price": 1000000,
            "min_market_cap": 100000000000,
            "min_rsi": 20,
            "max_rsi": 80
        }

# 거래 비용 계산 (기본)
def calculate_transaction_costs(sell_amount: int, buy_amount: int, settings: Dict[str, Any]) -> Dict[str, Any]:
    """거래 비용 계산"""
    try:
        commission_rate = settings.get("trading_params", {}).get("commission_rate", 0.00015)
        securities_tax_rate = settings.get("trading_params", {}).get("securities_tax_rate", 0.0015)
        agricultural_tax_rate = settings.get("trading_params", {}).get("agricultural_tax_rate", 0.0008)
        slippage_rate = settings.get("trading_params", {}).get("slippage_rate", 0.001)
        
        # 수수료 계산
        commission_sell = int(sell_amount * commission_rate)
        commission_buy = int(buy_amount * commission_rate)
        
        # 세금 계산
        tax_sell = int(sell_amount * securities_tax_rate)
        tax_buy = int(buy_amount * agricultural_tax_rate)
        
        # 슬리피지 계산
        slippage_sell = int(sell_amount * slippage_rate)
        slippage_buy = int(buy_amount * slippage_rate)
        
        total_cost = commission_sell + commission_buy + tax_sell + tax_buy + slippage_sell + slippage_buy
        
        return {
            "commission_sell": commission_sell,
            "commission_buy": commission_buy,
            "tax_sell": tax_sell,
            "tax_buy": tax_buy,
            "slippage_sell": slippage_sell,
            "slippage_buy": slippage_buy,
            "total_cost": total_cost
        }
        
    except Exception as e:
        logger.error(f"거래 비용 계산 실패: {e}")
        return {
            "commission_sell": 0,
            "commission_buy": 0,
            "tax_sell": 0,
            "tax_buy": 0,
            "slippage_sell": 0,
            "slippage_buy": 0,
            "total_cost": 0
        }

def calculate_net_profit_rotation(
    sell_ticker: str,
    buy_ticker: str,
    sell_amount: int,
    buy_amount: int,
    expected_gain: float,
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """순수익 기반 회전 매매 판단"""
    try:
        # 거래 비용 계산
        costs = calculate_transaction_costs(sell_amount, buy_amount, settings)
        
        # 예상 수익 계산
        expected_profit = int(expected_gain)
        net_profit = expected_profit - costs["total_cost"]
        
        # 최소 수익률 및 비용 효과성 확인
        min_profit_rate = settings.get("rotation", {}).get("min_profit_rate", 0.02)
        min_cost_effectiveness = settings.get("rotation", {}).get("min_cost_effectiveness", 2.0)
        
        profit_rate = net_profit / sell_amount if sell_amount > 0 else 0
        cost_effectiveness = expected_profit / costs["total_cost"] if costs["total_cost"] > 0 else 0
        
        should_rotate = (
            net_profit > 0 and
            profit_rate >= min_profit_rate and
            cost_effectiveness >= min_cost_effectiveness
        )
        
        return {
            "should_rotate": should_rotate,
            "net_profit": net_profit,
            "expected_profit": expected_profit,
            "total_costs": costs["total_cost"],
            "profit_rate": profit_rate,
            "cost_effectiveness": cost_effectiveness
        }
        
    except Exception as e:
        logger.error(f"순수익 회전 매매 판단 실패: {e}")
        return {
            "should_rotate": False,
            "net_profit": 0,
            "expected_profit": 0,
            "total_costs": 0,
            "profit_rate": 0,
            "cost_effectiveness": 0
        }

# ── RSI 개선 전략용 추가 지표 계산 함수 ────────────────────────────────
def calculate_ma20(prices: pd.Series, period: int = 20) -> float:
    """
    20일 이동평균 계산
    
    Args:
        prices: 종가 시리즈
        period: 이동평균 기간 (기본 20)
    
    Returns:
        MA20 값 (계산 실패 시 0.0)
    """
    try:
        if len(prices) < period:
            return 0.0
        
        ma20 = prices.rolling(window=period, min_periods=period).mean().iloc[-1]
        return float(ma20) if not pd.isna(ma20) else 0.0
    except Exception as e:
        logger.error(f"MA20 계산 실패: {e}")
        return 0.0

def calculate_ma20_slope(prices: pd.Series, period: int = 20, lookback_days: int = 5) -> float:
    """
    MA20 기울기 계산 (현재 MA20 - N일 전 MA20)
    
    Args:
        prices: 종가 시리즈
        period: 이동평균 기간 (기본 20)
        lookback_days: 기울기 계산을 위한 이전 일수 (기본 5)
    
    Returns:
        MA20 기울기 (양수=상승, 음수=하락, 계산 실패 시 0.0)
    """
    try:
        if len(prices) < period + lookback_days:
            return 0.0
        
        ma20_series = prices.rolling(window=period, min_periods=period).mean()
        if len(ma20_series) < lookback_days + 1:
            return 0.0
        
        current_ma20 = ma20_series.iloc[-1]
        prev_ma20 = ma20_series.iloc[-(lookback_days + 1)]
        
        if pd.isna(current_ma20) or pd.isna(prev_ma20):
            return 0.0
        
        slope = float(current_ma20 - prev_ma20) / lookback_days
        return slope
    except Exception as e:
        logger.error(f"MA20 기울기 계산 실패: {e}")
        return 0.0

def calculate_volume_ratio(df: pd.DataFrame, short_period: int = 3, long_period: int = 10) -> float:
    """
    거래량 비율 계산 (최근 N일 평균 / 최근 M일 평균)
    
    Args:
        df: OHLCV 데이터프레임
        short_period: 단기 기간 (기본 3일)
        long_period: 장기 기간 (기본 10일)
    
    Returns:
        거래량 비율 (단기/장기, 계산 실패 시 1.0)
    """
    try:
        # 거래량 컬럼 찾기
        volume_col = None
        if '거래량' in df.columns:
            volume_col = '거래량'
        elif 'Volume' in df.columns:
            volume_col = 'Volume'
        elif 'volume' in df.columns:
            volume_col = 'volume'
        else:
            logger.warning(f"거래량 컬럼을 찾을 수 없음: {df.columns.tolist()}")
            return 1.0
        
        if len(df) < long_period:
            return 1.0
        
        volumes = df[volume_col].dropna()
        if len(volumes) < long_period:
            return 1.0
        
        short_avg = volumes.tail(short_period).mean()
        long_avg = volumes.tail(long_period).mean()
        
        if long_avg == 0:
            return 1.0
        
        ratio = float(short_avg / long_avg)
        return ratio
    except Exception as e:
        logger.error(f"거래량 비율 계산 실패: {e}")
        return 1.0

def detect_bearish_divergence(df: pd.DataFrame, lookback_period: int = 10) -> bool:
    """
    약세 다이버전스 감지 (가격은 상승고점, RSI는 하락고점)
    
    Args:
        df: OHLCV 데이터프레임 (Close 컬럼 필요)
        lookback_period: 확인 기간 (기본 10일)
    
    Returns:
        약세 다이버전스 감지 여부
    """
    try:
        if len(df) < lookback_period + 5:  # RSI 계산을 위한 최소 데이터
            return False
        
        # 종가 컬럼 찾기
        close_col = None
        if '종가' in df.columns:
            close_col = '종가'
        elif 'Close' in df.columns:
            close_col = 'Close'
        elif 'close' in df.columns:
            close_col = 'close'
        else:
            return False
        
        prices = df[close_col].dropna()
        if len(prices) < lookback_period + 5:
            return False
        
        # RSI 계산
        rsi_values = []
        for i in range(14, len(prices)):
            window = prices.iloc[i-14:i+1]
            rsi = calculate_rsi(window)
            rsi_values.append(rsi)
        
        if len(rsi_values) < lookback_period:
            return False
        
        # 최근 lookback_period 동안의 가격 고점과 RSI 고점 찾기
        recent_prices = prices.tail(lookback_period)
        recent_rsi = pd.Series(rsi_values).tail(lookback_period)
        
        # 가격 고점 (최근 5일 중)
        price_window = recent_prices.tail(5)
        price_high_idx = price_window.idxmax()
        price_high = price_window.max()
        
        # 이전 고점 (나머지 기간 중)
        price_prev_window = recent_prices.head(-5) if len(recent_prices) > 5 else recent_prices
        if len(price_prev_window) > 0:
            price_prev_high = price_prev_window.max()
            
            # RSI 고점
            rsi_window = recent_rsi.tail(5)
            rsi_high = rsi_window.max()
            
            rsi_prev_window = recent_rsi.head(-5) if len(recent_rsi) > 5 else recent_rsi
            if len(rsi_prev_window) > 0:
                rsi_prev_high = rsi_prev_window.max()
                
                # 약세 다이버전스: 가격은 상승고점, RSI는 하락고점
                price_higher = price_high > price_prev_high
                rsi_lower = rsi_high < rsi_prev_high
                
                if price_higher and rsi_lower:
                    logger.debug(f"약세 다이버전스 감지: 가격 고점 {price_prev_high:.0f} → {price_high:.0f}, RSI 고점 {rsi_prev_high:.1f} → {rsi_high:.1f}")
                    return True
        
        return False
    except Exception as e:
        logger.error(f"약세 다이버전스 감지 실패: {e}")
        return False
