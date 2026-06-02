# src/rotation_manager.py
# -*- coding: utf-8 -*-
"""
회전 매매 전용 관리자

주요 기능:
- 포트폴리오 리밸런싱을 통한 회전 매매
- 동적 임계값 계산
- 거래 비용 최적화
- 성과 추적 및 분석
"""

import logging
import time
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

from utils import KST, _to_int, _to_float
from screener_core import MarketAnalyzer, MarketState
from rotation_policy import (
    min_holding_days as policy_min_holding_days,
    pair_passes_budget,
    pair_passes_economics,
    resolve_delta_threshold,
    sell_eligible_for_rotation,
)

logger = logging.getLogger(__name__)

@dataclass
class RotationCandidate:
    """회전 매매 후보 정보"""
    ticker: str
    name: str
    price: int
    score: float
    raw_data: Dict[str, Any]
    
@dataclass
class HoldingInfo:
    """보유 종목 정보"""
    ticker: str
    name: str
    quantity: int
    score: float
    price: int
    proceeds: int
    
@dataclass
class RotationResult:
    """회전 매매 결과"""
    success: bool
    sell_ticker: str
    buy_ticker: str
    delta_score: float
    transaction_cost: int
    expected_gain: float
    timestamp: datetime
    reason: str

class RotationManager:
    """회전 매매 전용 관리자"""
    
    def __init__(self, settings, trader_instance=None):
        self.settings = settings
        self.trader = trader_instance
        
        # settings가 딕셔너리인지 Settings 객체인지 확인
        if hasattr(settings, 'rotation'):
            self.rotation_config = settings.rotation
        else:
            self.rotation_config = settings.get("rotation", {})
        
        # 시장 분석기 초기화 (가능하면 trader의 KIS로 결정적 판단)
        kis = None
        try:
            kis = getattr(self.trader, "kis", None)
        except Exception:
            kis = None
        self.market_analyzer = MarketAnalyzer(settings, kis=kis, market="KOSPI")
        self._current_market_state = None
        
        # 회전 매매 상태 추적
        self._sold_once = set()  # 한 번 매도된 티커
        self._attempted_pairs = set()  # 시도된 매도-매수 페어
        self._rotation_history = []  # 회전 매매 이력
        
        # 성과 추적
        self._performance_metrics = {
            "total_rotations": 0,
            "successful_rotations": 0,
            "total_gain": 0.0,
            "total_cost": 0,
            "avg_delta_score": 0.0
        }
        
        logger.info("RotationManager 초기화 완료")
    
    def try_rotation(self, candidates: List[Dict[str, Any]], holdings: List[Dict[str, Any]], usable_cash: int) -> bool:
        """
        회전 매매 시도
        
        Args:
            candidates: 신규 후보 종목 리스트
            holdings: 현재 보유 종목 리스트
            usable_cash: 사용 가능한 현금
            
        Returns:
            bool: 회전 매매 성공 여부
        """
        if not self._is_rotation_enabled():
            logger.info("회전 매매 비활성화됨")
            return False

        # trader가 회전 페어 쿼터를 관리하는 경우(1회 buy 사이클당 상한), 남은 쿼터가 없으면 스킵
        if self.trader and hasattr(self.trader, "_rotation_quota_remaining"):
            try:
                if self.trader._rotation_quota_remaining() <= 0:
                    logger.info("이번 매수 사이클 회전 한도 소진 → try_rotation 스킵")
                    return False
            except Exception:
                pass
            
        if not candidates or not holdings:
            logger.info("회전 매매 스킵: 후보 또는 보유 종목 없음")
            return False
        
        # 시장 상태 분석 (동적 임계값 계산을 위해)
        self._current_market_state = self.market_analyzer.analyze_market_state()
        logger.info(f"시장 분석: {self.market_analyzer.get_market_summary(self._current_market_state)}")
        
        # 데이터 변환
        holding_list = self._convert_holdings(holdings)
        candidate_list = self._convert_candidates(candidates)
        
        if not holding_list or not candidate_list:
            logger.info("회전 매매 스킵: 유효한 데이터 없음")
            return False
        
        # 회전 매매 실행
        result = self._execute_rotation(holding_list, candidate_list, usable_cash)
        
        if result:
            self._update_performance_metrics(result)
            self._rotation_history.append(result)
            logger.info(f"회전 매매 성공: {result.sell_ticker} → {result.buy_ticker}")
        else:
            logger.info("회전 매매 조건 미충족")
        
        return bool(result)
    
    def _is_rotation_enabled(self) -> bool:
        """회전 매매 활성화 여부 확인"""
        return bool(self.rotation_config.get("enabled", False))
    
    def _convert_holdings(self, holdings: List[Dict[str, Any]]) -> List[HoldingInfo]:
        """보유 종목 데이터 변환 (최소 보유기간 체크 포함)"""
        scores_map, _ = self._load_latest_scores()
        if not scores_map:
            logger.warning("점수 캐시 없음")
            return []
        
        current_time = datetime.now(KST)
        min_days = policy_min_holding_days(self.settings)
        
        holding_list = []
        for h in holdings:
            ticker = str(h.get("pdno", "")).zfill(6)
            name = h.get("prdt_name", "N/A")
            quantity = _to_int(h.get("hldg_qty", 0))
            price = _to_int(h.get("prpr", 0))
            score = float(scores_map.get(ticker, 0.0))
            proceeds = price * quantity
            
            if quantity > 0:
                is_eligible, _ = sell_eligible_for_rotation(ticker, self.settings, current_time)
                if is_eligible:
                    holding_list.append(HoldingInfo(
                        ticker=ticker,
                        name=name,
                        quantity=quantity,
                        score=score,
                        price=price,
                        proceeds=proceeds
                    ))
                else:
                    logger.debug(f"회전 매매 제외: {name}({ticker}) - 최소 보유기간 {min_days}일 미달")
        
        return holding_list
    
    def _convert_candidates(self, candidates: List[Dict[str, Any]]) -> List[RotationCandidate]:
        """후보 종목 데이터 변환"""
        scores_map, _ = self._load_latest_scores()
        
        candidate_list = []
        for c in candidates:
            ticker = str(c.get("Ticker", "")).zfill(6)
            name = c.get("Name", "N/A")
            price = _to_int(c.get("Price", 0))
            score = _to_float(c.get("Score", scores_map.get(ticker, 0.0)), 0.0)
            
            if ticker and price > 0:
                candidate_list.append(RotationCandidate(
                    ticker=ticker,
                    name=name,
                    price=price,
                    score=score,
                    raw_data=c
                ))
        
        return candidate_list
    
    def _load_latest_scores(self) -> Tuple[Dict[str, float], Optional[str]]:
        """최신 점수 데이터 로드"""
        if self.trader and hasattr(self.trader, '_load_latest_scores'):
            return self.trader._load_latest_scores()
        return {}, None
    
    # Phase 1: 최소 보유기간 체크 로직은 utils.check_min_holding_period로 통일됨
    
    def _execute_rotation(self, holdings: List[HoldingInfo], candidates: List[RotationCandidate], usable_cash: int) -> Optional[RotationResult]:
        """회전 매매 실행"""
        # 정렬
        holdings_sorted = sorted(holdings, key=lambda x: x.score)  # 점수 낮은 순
        candidates_sorted = sorted(candidates, key=lambda x: x.score, reverse=True)  # 점수 높은 순
        
        # 동적 임계값 계산
        delta_threshold = self._calculate_dynamic_threshold()
        
        # 회전 매매 페어 검색
        for holding in holdings_sorted:
            if self._should_skip_holding(holding):
                continue
                
            for candidate in candidates_sorted:
                if self._should_skip_candidate(holding, candidate):
                    continue
                
                # 회전 조건 검증
                if self._validate_rotation_conditions(holding, candidate, usable_cash, delta_threshold):
                    # 회전 매매 실행
                    result = self._perform_rotation(holding, candidate, usable_cash)
                    if result:
                        return result
        
        return None
    
    def _calculate_dynamic_threshold(self) -> float:
        """동적 임계값 계산"""
        return resolve_delta_threshold(
            self.settings,
            self._current_market_state,
            self.market_analyzer,
        )
    
    def _should_skip_holding(self, holding: HoldingInfo) -> bool:
        """보유 종목 스킵 여부 확인"""
        if holding.ticker in self._sold_once:
            return True
        if self._is_in_cooldown(holding.ticker):
            logger.info(f"회전 매매 스킵: {holding.name}({holding.ticker}) 쿨다운 중")
            return True
        return False
    
    def _should_skip_candidate(self, holding: HoldingInfo, candidate: RotationCandidate) -> bool:
        """후보 종목 스킵 여부 확인"""
        if candidate.ticker == holding.ticker:
            return True
        if (holding.ticker, candidate.ticker) in self._attempted_pairs:
            return True
        if self._is_in_cooldown(candidate.ticker):
            logger.info(f"회전 매매 스킵: {candidate.name}({candidate.ticker}) 쿨다운 중")
            return True
        return False
    
    def _is_in_cooldown(self, ticker: str) -> bool:
        """쿨다운 여부 확인"""
        if self.trader and hasattr(self.trader, '_is_in_cooldown'):
            return self.trader._is_in_cooldown(ticker)
        return False
    
    def _validate_rotation_conditions(
        self,
        holding: HoldingInfo,
        candidate: RotationCandidate,
        usable_cash: int,
        delta_threshold: float,
    ) -> bool:
        """회전 조건 검증 (rotation_policy와 동일 기준)"""
        # 점수 차이 확인
        delta_score = candidate.score - holding.score
        if delta_score < delta_threshold:
            return False
        
        # 예산 확인
        if not pair_passes_budget(usable_cash, holding.proceeds, candidate.price, self.settings):
            return False
        
        # 거래 비용 대비 경제성 확인(순수익 기반)
        if not pair_passes_economics(
            holding.proceeds, candidate.price, holding.score, candidate.score, self.settings
        ):
            logger.info(
                "회전 매매 중단(비용): %s → %s",
                holding.ticker,
                candidate.ticker,
            )
            return False
        
        logger.info(
            f"회전 조건 충족: {holding.name}({holding.ticker})[{holding.score:.3f}] "
            f"→ {candidate.name}({candidate.ticker})[{candidate.score:.3f}] "
            f"delta=+{delta_score:.3f} thr={delta_threshold:.3f}"
        )
        
        return True
    
    def _estimate_transaction_cost(self, holding: HoldingInfo, candidate: RotationCandidate) -> int:
        """거래 비용 추정"""
        # 매도 비용 (수수료 + 세금)
        sell_cost = int(holding.proceeds * 0.0015)  # 0.15% 추정
        
        # 매수 비용 (수수료)
        buy_cost = int(candidate.price * 0.0015)  # 0.15% 추정
        
        return sell_cost + buy_cost
    
    def _perform_rotation(self, holding: HoldingInfo, candidate: RotationCandidate, usable_cash: int) -> Optional[RotationResult]:
        """실제 회전 매매 수행"""
        if not self.trader:
            logger.error("Trader 인스턴스가 없어 회전 매매를 수행할 수 없습니다")
            return None
        
        # 매도 시도
        sell_result = self.trader._execute_market_sell(
            holding.ticker, 
            holding.quantity, 
            holding.name,
            reason_text=f"ROTATION_SWAP → {candidate.ticker}",
            reason_code="ROTATION_SWAP"
        )
        
        if sell_result.get("status") != "executed":
            logger.info(f"회전 매매 실패: 매도 확정 실패 ({holding.ticker})")
            return None
        
        # 매도 완료 후 상태 업데이트
        self._sold_once.add(holding.ticker)
        self._attempted_pairs.add((holding.ticker, candidate.ticker))
        
        # 계좌 정보 갱신
        time.sleep(3)
        self.trader._update_account_info(force=True)
        new_cash, new_holdings, _ = self.trader._load_snapshot()
        
        # 매수 시도
        buy_result = self.trader._execute_buy_single(candidate.raw_data, new_cash, batch_name="ROTATION")
        
        if buy_result:
            # 회전 매매 결과 생성
            delta_score = candidate.score - holding.score
            transaction_cost = self._estimate_transaction_cost(holding, candidate)
            expected_gain = delta_score * holding.proceeds / 100
            
            return RotationResult(
                success=True,
                sell_ticker=holding.ticker,
                buy_ticker=candidate.ticker,
                delta_score=delta_score,
                transaction_cost=transaction_cost,
                expected_gain=expected_gain,
                timestamp=datetime.now(KST),
                reason="회전 매매 성공"
            )
        else:
            logger.warning(f"회전 매매 부분 실패: 매수 실패 ({candidate.ticker})")
            return None
    
    def _update_performance_metrics(self, result: RotationResult):
        """성과 지표 업데이트"""
        self._performance_metrics["total_rotations"] += 1
        if result.success:
            self._performance_metrics["successful_rotations"] += 1
            self._performance_metrics["total_gain"] += result.expected_gain
            self._performance_metrics["total_cost"] += result.transaction_cost
        
        # 평균 점수 차이 업데이트
        total_delta = sum(r.delta_score for r in self._rotation_history if r.success)
        if self._performance_metrics["successful_rotations"] > 0:
            self._performance_metrics["avg_delta_score"] = total_delta / self._performance_metrics["successful_rotations"]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """성과 요약 반환"""
        success_rate = 0
        if self._performance_metrics["total_rotations"] > 0:
            success_rate = self._performance_metrics["successful_rotations"] / self._performance_metrics["total_rotations"]
        
        net_gain = self._performance_metrics["total_gain"] - self._performance_metrics["total_cost"]
        
        return {
            "total_rotations": self._performance_metrics["total_rotations"],
            "successful_rotations": self._performance_metrics["successful_rotations"],
            "success_rate": success_rate,
            "total_gain": self._performance_metrics["total_gain"],
            "total_cost": self._performance_metrics["total_cost"],
            "net_gain": net_gain,
            "avg_delta_score": self._performance_metrics["avg_delta_score"]
        }
    
    def reset_state(self):
        """상태 초기화 (일일 리셋용)"""
        self._sold_once.clear()
        self._attempted_pairs.clear()
        logger.info("회전 매매 상태 초기화 완료")
