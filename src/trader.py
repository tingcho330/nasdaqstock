# src/trader.py
# -*- coding: utf-8 -*-
"""
자동매매 트레이더 본체

주요 개선점 (2025-09-12):
- [P0] 매수 직전 지정가에 슬리피지(-bps) 반영 후 유효호가 하향 라운딩 적용
- [P0] fee_buffer_pct(수수료/세금 버퍼) 고려한 수량 재산정(usable_cash 초과 방지)
- [P0] 브로커 거절(REJECT) 시 max_retries/retry_delay_ms 기반 재시도 정책
- [P0] 주문 원시 응답 민감정보 마스킹 후 DEBUG 로깅
- [P0] 리포팅: 최종 요약은 실거래(체결 기준) 집계로 통일, 추천 집계는 별도
- [P1] 분할 매수 라더 기본 방향을 매수 하향(0,-1,-2틱)으로 정규화
- [P1] 모의(vps 등) 환경은 종료 시 실계좌 스냅샷 출력 생략(혼동 방지)
"""

import json
import logging
import os
import random
import time
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from collections import defaultdict
import uuid
import re
import pandas as pd

# ── 공통 유틸리티 / 설정 ──────────────────────────────────────────────
from utils import (
    setup_logging,
    find_latest_file,
    OUTPUT_DIR,
    extract_cash_from_summary,
    normalize_ticker_6,
    is_us_market,
    KST,
    in_time_windows,
    get_account_snapshot_cached,
    get_tick_size,
    round_to_tick,
    convert_screener_data_to_trader_format,
    check_min_holding_period,
    check_min_holding_hours,
    extract_broker_order_id,
)
from api.kis_auth import KIS
from risk_manager import RiskManager
from settings import settings
from rotation_manager import RotationManager

# recorder: DB 초기화/기록 + 마지막 매수 조회(FIFO 연결)
from recorder import (
    initialize_db,
    record_trade,
    fetch_trades_by_tickers,
    mark_pending_buy_cancelled,
    mark_pending_order_cancelled,
)

try:
    from db_debug import log as _db_dbg_log, log_trade_in as _db_dbg_trade_in, log_skip as _db_dbg_skip
except ImportError:
    def _db_dbg_log(*args, **kwargs):
        pass
    def _db_dbg_trade_in(*args, **kwargs):
        pass
    def _db_dbg_skip(*args, **kwargs):
        pass

# GPT 분석기: 리밸런싱 함수들
from gpt_analyzer import (
    _call_openai_json as gpt_call_openai,
    get_top_screener_candidates,
    analyze_rebalance_with_gpt,
    build_rebalance_prompt,
    parse_gpt_rebalance_decisions,
    get_gpt_enhanced_rebalance_candidates,
    fallback_rebalance_logic
)

# ── 디스코드 노티파이어 ───────────────────────────────────────────────
from notifier import (
    DiscordLogHandler,
    WEBHOOK_URL,
    is_valid_webhook,
    send_discord_message,
    create_trade_embed,
    create_alert_embed,
)

# ── 로깅 초기화 ───────────────────────────────────────────────────────
setup_logging()
logger = logging.getLogger("trader")

# 루트 로거에 디스코드 에러 핸들러 장착(중복 방지)
_root = logging.getLogger()
if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
    if not any(isinstance(h, DiscordLogHandler) for h in _root.handlers):
        _root.addHandler(DiscordLogHandler(WEBHOOK_URL))
        logger.info("DiscordLogHandler attached to root logger.")
else:
    logger.warning("유효한 DISCORD_WEBHOOK_URL이 없어 에러 로그의 디스코드 전송을 비활성화합니다.")

# ── 간단 레이트 리밋(스팸 방지) ───────────────────────────────────────
_last_sent_ts = defaultdict(float)
DEFAULT_COOLDOWN_SEC = 120  # 동일 키 알림 최소 간격(초)

def _can_send(key: str, cooldown: int = DEFAULT_COOLDOWN_SEC) -> bool:
    now = time.time()
    if now - _last_sent_ts[key] >= cooldown:
        _last_sent_ts[key] = now
        return True
    return False

def _scope_key_with_run_id(key: str) -> str:
    """key 가 'run:'으로 시작하지 않으면 RUN_ID 네임스페이스를 앞에 붙인다."""
    if key.startswith("run:"):
        return key
    return f"run:{os.getenv('RUN_ID', 'na')}:{key}"

def _notify_text(content: str, key: str = "trader_generic", cooldown: int = DEFAULT_COOLDOWN_SEC):
    key = _scope_key_with_run_id(key)
    if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL) and _can_send(key, cooldown):
        try:
            send_discord_message(content=content)
        except Exception:
            pass

def _notify(content: str, key: str = "trader_generic", cooldown_sec: int = DEFAULT_COOLDOWN_SEC):
    """간단한 텍스트 알림 함수"""
    _notify_text(content, key, cooldown_sec)

def _notify_embed(embed: Dict, key: str, cooldown: int = DEFAULT_COOLDOWN_SEC):
    key = _scope_key_with_run_id(key)
    if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL) and _can_send(key, cooldown):
        try:
            send_discord_message(embeds=[embed])
        except Exception:
            pass

# ── 경로/상수 ─────────────────────────────────────────────────────────
COOLDOWN_FILE = OUTPUT_DIR / "cooldown.json"
PARTIAL_SELL_FLAGS_FILE = OUTPUT_DIR / "partial_sell_flags.json"
ACCOUNT_SCRIPT_PATH = "/app/src/account.py"  # 계좌 스냅샷 생성 전용 스크립트

# ── 보조 파서 ─────────────────────────────────────────────────────────
def _to_int(v) -> int:
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        s = v.replace(",", "").strip()
        try:
            return int(float(s))
        except Exception:
            return 0
    return 0

def _to_float(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace(",", "").strip()
        return float(s)
    except Exception:
        return default

# ── 응답 마스킹 ──────────────────────────────────────────────────────
_SENSITIVE_KEYS = [
    "Authorization", "authorization", "auth", "access_token", "refresh_token",
    "appkey", "secret", "tr_id", "partner", "custtype"
]
_SENSITIVE_RE = re.compile(r'("?(?:' + "|".join(map(re.escape, _SENSITIVE_KEYS)) + r')"?\s*:\s*")([^"]+)(")', re.I)

def _mask_sensitive(text_or_obj: Any) -> str:
    """
    JSON/dict/str 형태의 응답에서 민감 키값을 마스킹한다.
    """
    try:
        s = text_or_obj if isinstance(text_or_obj, str) else json.dumps(text_or_obj, ensure_ascii=False)
    except Exception:
        s = str(text_or_obj)
    s = _SENSITIVE_RE.sub(r'\1***\3', s)
    return s[:2000]  # 너무 긴 로그 방지

# ── 스키마 관용성(디폴트) ────────────────────────────────────────────
SCHEMA_DEFAULTS: Dict[str, Any] = {
    "Sector": "N/A",
    "SectorSource": "unknown",
    "ATR": 0.0,
    "RSI": 50.0,
    "MA50": None,
    "MA200": None,
    "손절가": None,
    "목표가": None,
    "source": "unknown",
    "daily_chart": [],
    "investor_flow": [],
    "Price": 0,
    "Score": 0.0,
}

# ── 가성비 통계 로그 ───────────────────────────────────────────
def log_affordability_stats(usable_cash: int, buffer_ratio: float, candidates: List[Dict[str, Any]], min_order_cash: Optional[int] = None):
    cheapest = min((_to_int(c.get("Price", 0)) for c in candidates), default=0)
    buyable_cnt = sum(1 for c in candidates if _to_int(c.get("Price", 0)) <= int(usable_cash * (1 - buffer_ratio)))
    base = f"[Affordability] usable_cash={usable_cash:,}, buffer={buffer_ratio:.2%}, cheapest={cheapest:,}, buyable_count={buyable_cnt}"
    if isinstance(min_order_cash, (int, float)) and min_order_cash is not None:
        base += f", min_order_cash={int(min_order_cash):,}"
    logger.info(base)

# ── Trader 본체 ───────────────────────────────────────────────────────
class Trader:
    def __init__(self, settings_obj):
        self.settings = settings_obj._config
        self.env = self.settings.get("trading_environment", "vps")
        self.is_real_trading = (self.env == "prod")
        self.risk_params = self.settings.get("risk_params", {}) or {}
        self.trading_params = self.settings.get("trading_params", {}) or {}
        self.trading_guards = self.settings.get("trading_guards", {}) or {}
        self.screener_params = self.settings.get("screener_params", {}) or {}
        self.reporting = self.settings.get("reporting", {}) or {}

        # 시간대 로직
        self.buy_time_windows: List[str] = self.trading_params.get("buy_time_windows", ["09:05-14:50"])
        self.sell_time_windows: List[str] = self.trading_params.get("sell_time_windows", ["09:05-15:10"])

        # REBUY 파라미터
        self.allow_rebuy = bool(self.trading_params.get("allow_rebuy", False))
        self.max_positions = int(self.trading_params.get("max_positions", self.risk_params.get("max_positions", 5)))
        self.max_legs_per_ticker = int(self.trading_params.get("max_legs_per_ticker", 1))
        self.per_ticker_max_weight = float(self.trading_params.get("per_ticker_max_weight", 1.0))
        self.min_order_cash = int(self.trading_params.get("min_order_cash", 0))
        self.market = os.getenv("MARKET", "NASDAQ100")
        self.rebuy_atr_k = float(self.trading_params.get("rebuy_atr_k", 0.0))
        self.rebuy_rsi_ceiling = float(self.trading_params.get("rebuy_rsi_ceiling", 100.0))
        self.min_cash_reserve = int(self.trading_params.get("min_cash_reserve", 0))
        self.cash_buffer_ratio = float(self.trading_params.get("cash_buffer_ratio", 0.0))

        # ⬇️ 신규 주입 파라미터
        self.fee_buffer_pct = float(self.trading_params.get("fee_buffer_pct", 0.0))
        self.retry_on_reject = bool(self.trading_params.get("retry_on_reject", False))
        
        # 중복 주문 방지 시스템
        self._processed_orders = set()  # 처리된 주문 추적
        self._order_lock = {}  # 종목별 주문 락
        
        # 계좌 조회 최적화
        self._account_cache = {}  # 계좌 정보 캐시
        self._last_account_update = 0  # 마지막 계좌 업데이트 시간
        self._account_cache_ttl = 5  # 캐시 유효 시간 (초)
        
        # 통합 분석 시스템
        self.integrated_analysis = self.settings.get("integrated_analysis", {})
        
        # 동적 현금 관리 시스템
        self.dynamic_cash_config = self.trading_params.get("dynamic_cash_management", {})
        self.dynamic_cash_enabled = bool(self.dynamic_cash_config.get("enabled", False))
        self.market_regime_adjustment = self.dynamic_cash_config.get("market_regime_adjustment", {})
        self.volatility_threshold = float(self.dynamic_cash_config.get("volatility_threshold", 0.25))
        self.rebalance_frequency_hours = int(self.dynamic_cash_config.get("rebalance_frequency_hours", 6))
        self._last_cash_rebalance = 0  # 마지막 현금 리밸런싱 시간
        self.gpt_analysis_expansion = self.settings.get("gpt_params", {}).get("analysis_expansion", {})
        self._gpt_hold_decisions = set()  # GPT 보류 결정 추적
        self._analysis_log = []  # 분석 과정 로그

        # 스크리너 시장 상태 로드 (dynamic_cash_management에서 재사용)
        self.market_state_from_screener: Optional[Dict[str, Any]] = self._load_latest_market_state()
        
        # 회전 매매 관리자 초기화
        self.rotation_manager = RotationManager(self.settings, self)
        self.max_retries_on_reject = int(self.trading_params.get("max_retries", 0))
        self.retry_delay_ms = int(self.trading_params.get("retry_delay_ms", 0))
        self.slippage_bps = float(self.trading_params.get("slippage_bps", 0.0))

        self.cooldown_list = self._load_cooldown_list()
        self.cooldown_period_days = self.risk_params.get("cooldown_period_days", 10)

        # 쿨다운: 연속 실패 카운트(메모리), 임계치
        self._fail_counts = defaultdict(int)
        self.cooldown_fail_threshold = int(self.risk_params.get("cooldown_fail_threshold", 2))

        # 부분익절 후 재매수(재진입/추가매수) 차단 정책
        self.post_partial_sell_buy_cooldown_days = int(self.trading_params.get("post_partial_sell_buy_cooldown_days", 1))
        self.partial_sell_flag_ttl_days = int(self.trading_params.get("partial_sell_flag_ttl_days", 7))

        # 회전 중복 방지 셋
        # 회전 매매 상태는 RotationManager에서 관리

        # 런타임 ID
        self.run_id = os.getenv("RUN_ID") or (datetime.now(KST).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6])

        # 알림 레이트 리밋
        notif_cfg = self.settings.get("notifications", {}) or {}
        self._account_update_min_interval = float(notif_cfg.get("discord_cooldown_sec", 60))
        self._last_account_update_ts = 0.0

        # 요약/통계 (실거래 기준)
        self.stats = {"buy": 0, "sell": 0, "hold": 0}
        self.recomm_stats = {"buy": 0, "sell": 0}  # 추천 집계(표시만)
        self.summary_reason_code: Optional[str] = None
        self.summary_reason_detail: Optional[str] = None

        # 15시 20분 일괄 체결 확인 시스템
        self.pending_orders = []  # 미체결 주문 추적
        batch_config = self.settings.get("batch_execution_check", {})
        self.batch_check_time = batch_config.get("check_time", "15:20")  # 설정에서 시간 읽어오기
        self.batch_check_enabled = batch_config.get("enabled", True)

        # 부분익절 이력(전량매도 후 재진입 차단 갱신에 사용)
        self.partial_sell_flags = self._load_partial_sell_flags()

        initialize_db()
        logger.info("거래 기록용 데이터베이스가 초기화되었습니다.")

        # KIS 초기화
        try:
            self.kis = KIS(config={}, env=self.env)
            if not getattr(self, "kis", None) or not getattr(self.kis, "auth_token", None):
                raise ConnectionError("KIS API 인증에 실패했습니다 (토큰 없음).")
            logger.info(f"'{self.env}' 모드로 KIS API 인증 완료.")
        except Exception as e:
            logger.error(f"KIS API 초기화 중 오류 발생: {e}", exc_info=True)
            raise ConnectionError("KIS API 초기화에 실패했습니다.") from e

        self.risk_manager = RiskManager(settings_obj)

        # 스크리너 전체 데이터(랭킹/후보)
        self.all_stock_data = self._load_all_stock_data()

        _notify_text(
            f" Trader 초기화 완료 (env={self.env}, real_trading={self.is_real_trading}, run_id={self.run_id})",
            key=f"phase:init:{self.run_id}", cooldown=60
        )

    def _t(self, ticker) -> str:
        """종목코드 정규화 (KR 6자리 / US 심볼)."""
        return normalize_ticker_6(ticker, self.market)

    def _load_latest_market_state(self) -> Optional[Dict[str, Any]]:
        """
        screener가 output/market_state_YYYYMMDD_MARKET.json 로 저장한 시장 상태를 로드한다.
        실패 시 None 반환(폴백은 기존 sideways_market).
        """
        try:
            f = find_latest_file("market_state_*_*.json")
            if not f:
                return None
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                return None
            regime = str(data.get("regime") or "").strip()
            if regime:
                logger.info("screener market_state 로드: %s (file=%s)", regime, f.name)
            return data
        except Exception as e:
            logger.debug("screener market_state 로드 실패: %s", e)
            return None

    # ── 공통 버퍼 통일: cash_buffer_ratio vs affordability_buffer ───────
    def _eff_buffer(self) -> float:
        """
        예산/후보필터/수량 산정에 적용할 단일 버퍼.
        trading_params.cash_buffer_ratio와 screener_params.affordability_buffer 중 더 큰 값 사용.
        """
        tb = float(self.trading_params.get("cash_buffer_ratio", 0.0))
        sb = float(self.screener_params.get("affordability_buffer", 0.0))
        return max(tb, sb)

    # ── 스크리너 전체 데이터 로드 ──────────────────────────────────────
    def _load_all_stock_data(self) -> Dict[str, Dict]:
        # 확장된 Screener 데이터 우선 로드
        patterns = [
            "screener_scores_*_*.json",  # 확장된 데이터 우선
            "screener_candidates_full_*_*.json",
            "screener_rank_full_*_*.json",
            "screener_candidates_*_*.json",
        ]
        picked = None
        for pat in patterns:
            f = find_latest_file(pat)
            if f:
                picked = f
                break

        if not picked:
            logger.info("스크리너 결과 파일을 찾지 못했습니다. (scores/candidates_full/rank_full/candidates)")
            _notify_text("ℹ️ 스크리너 전체 데이터 없음 -> 실시간 조회로 진행",
                         key=f"phase:load_full_missing:{self.run_id}", cooldown=600)
            return {}

        try:
            with open(picked, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, list):
                logger.info(f"{picked.name}의 형식이 리스트가 아닙니다. 빈 데이터로 진행합니다.")
                return {}

            mapping: Dict[str, Dict] = {}
            missing_total = 0
            missing_by_key: Dict[str, int] = defaultdict(int)

            for stock in data:
                if not isinstance(stock, dict):
                    continue
                
                # 확장된 데이터 구조 처리 (screener_scores_*.json)
                if 'ticker' in stock:
                    # 공통 변환 함수 사용
                    converted_stock = convert_screener_data_to_trader_format(stock)
                    if not converted_stock:  # 빈 딕셔너리면 건너뛰기
                        continue
                    
                    t = converted_stock['Ticker']
                    
                    # 기본값 적용
                    for k, dv in SCHEMA_DEFAULTS.items():
                        if k not in converted_stock or converted_stock.get(k) is None:
                            converted_stock[k] = dv
                            missing_total += 1
                            missing_by_key[k] += 1
                    
                    mapping[t] = converted_stock
                    
                else:
                    # 기존 screener_candidates_*.json 형태
                    t = self._t(stock.get('Ticker', ''))
                    if not t or t == "000000":
                        continue
                    
                    for k, dv in SCHEMA_DEFAULTS.items():
                        if k not in stock or stock.get(k) is None:
                            stock[k] = dv
                            missing_total += 1
                            missing_by_key[k] += 1
                    mapping[t] = stock

            if missing_total > 0:
                logger.info(
                    f"{picked.name} 스키마 결손 자동 보정: 총 {missing_total}건 | "
                    + ", ".join([f"{k}={v}" for k, v in sorted(missing_by_key.items())])
                )
            logger.info(f"스크리너 데이터 로드 완료: {picked.name} ({len(mapping)}종목)")
            return mapping

        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"{picked.name} 파일 로드 실패: {e}")
            _notify_text(f"⚠️ 스크리너 데이터 로드 실패: {e}",
                         key=f"phase:load_full_error:{self.run_id}", cooldown=600)
            return {}

    # ── account.py 트리거 ────────────────────────────────────────────
    def _update_account_info(self, force: bool = False):
        now = time.time()
        # 회전 핵심 지점에서는 force=True로 강제 갱신 허용
        # 성능 최적화: 기본 간격을 30초로 늘림 (기존 20초)
        if not force and (now - self._last_account_update_ts) < max(30, self._account_update_min_interval / 2):
            logger.debug("account.py 호출 스킵(최근에 갱신됨).")  # INFO -> DEBUG로 변경
            return
        self._last_account_update_ts = now

        logger.info("[CALL] account.py 실행(계좌 스냅샷 갱신)...")
        try:
            for attempt in range(2):
                try:
                    subprocess.run(
                        [sys.executable, str(ACCOUNT_SCRIPT_PATH)],  # 현재 인터프리터로 고정
                        capture_output=True, text=True, check=True, encoding="utf-8",
                        timeout=60
                    )
                    logger.info("[RET] account.py 실행 완료.")
                    _notify_text(" account.py 실행 완료(요약/잔고 갱신)",
                                 key=f"phase:account_update:{self.run_id}", cooldown=60)
                    break
                except subprocess.TimeoutExpired:
                    logger.error("account.py 타임아웃(60s). 재시도 중...")
                    if attempt == 1:
                        raise
                except subprocess.CalledProcessError as e:
                    head = (e.stderr or "")[:400]
                    logger.error(f"account.py 실행 오류(Exit {e.returncode}). stderr:\n{head}")
                    if attempt == 1:
                        raise
        except FileNotFoundError:
            msg = f"스크립트를 찾을 수 없습니다: {ACCOUNT_SCRIPT_PATH}"
            logger.error(msg)
            _notify_text(f"❗ {msg}", key=f"phase:account_not_found:{self.run_id}", cooldown=300)
        except Exception as e:
            msg = f"계좌 정보 업데이트 중 예외: {e}"
            logger.error(msg, exc_info=True)
            _notify_text(f"❗ {msg}", key=f"phase:account_exc:{self.run_id}", cooldown=300)

    def _batch_update_account_info(self):
        """배치 계좌 정보 갱신 (중복 호출 방지)"""
        now = time.time()
        
        # 마지막 갱신으로부터 5초 이내면 스킵 (기존 3초에서 증가)
        if hasattr(self, '_last_batch_update_ts') and (now - self._last_batch_update_ts) < 5:
            logger.debug("배치 갱신 스킵: 최근 갱신됨")
            return
            
        self._last_batch_update_ts = now
        time.sleep(1)  # 매도 후 대기 시간 단축 (2초 -> 1초)
        self._update_account_info(force=True)

    # ── 스냅샷 로더/헬퍼 ──────────────────────────────────────────────
    def _load_snapshot(self) -> Tuple[int, List[Dict], Dict[str, int]]:
        summary_dict, balance_list, *_ = get_account_snapshot_cached(
            summary_pattern="summary_*.json",
            balance_pattern="balance_*.json",
            ttl_sec=None,
        )
        cash_map = extract_cash_from_summary(summary_dict, market=self.market)
        available_cash = cash_map.get("available_cash", 0)
        holdings: List[Dict] = []
        if balance_list:
            holdings = [h for h in balance_list if _to_int(h.get("hldg_qty", 0)) > 0]
        return available_cash, holdings, cash_map

    def _get_holdings_snapshot(self) -> List[Dict]:
        """현재 보유 종목 스냅샷 반환 (지연 체결 확인용)"""
        try:
            _, balance_list, *_ = get_account_snapshot_cached(
                summary_pattern="summary_*.json",
                balance_pattern="balance_*.json",
                ttl_sec=5,  # 5초 캐시
            )
            holdings: List[Dict] = []
            if balance_list:
                holdings = [h for h in balance_list if _to_int(h.get("hldg_qty", 0)) > 0]
            return holdings
        except Exception as e:
            logger.error(f"보유 종목 스냅샷 조회 실패: {e}")
            return []

    @staticmethod
    def _get_qty(holdings: List[Dict], ticker: str) -> int:
        for h in holdings:
            if self._t(h.get("pdno", "")) == ticker:
                return _to_int(h.get("hldg_qty", 0))
        return 0

    def _portfolio_snapshot(self, holdings: List[Dict]) -> Dict[str, Any]:
        by_val: Dict[str, float] = defaultdict(float)
        avg_price: Dict[str, float] = {}
        pv = 0.0
        for h in holdings:
            t = self._t(h.get("pdno", ""))
            qty = _to_int(h.get("hldg_qty", 0))
            prpr = _to_float(h.get("prpr"), 0.0)
            avgp = _to_float(h.get("pchs_avg_pric"), 0.0) or prpr
            val = prpr * qty if (prpr and qty) else 0.0
            by_val[t] += val
            pv += val
            if qty > 0:
                avg_price[t] = avgp
        return {
            "by_ticker_value": by_val,
            "avg_price_by_ticker": avg_price,
            "portfolio_value": pv,
        }

    def _legs_count_for_ticker(self, holdings: List[Dict], ticker: str) -> int:
        return 1 if any(self._t(h.get("pdno", "")) == ticker and _to_int(h.get("hldg_qty", 0)) > 0 for h in holdings) else 0

    def _can_rebuy(self, ticker: str, info: Dict[str, Any], holdings: List[Dict], available_cash: int) -> Tuple[bool, str]:
        if self._legs_count_for_ticker(holdings, ticker) >= self.max_legs_per_ticker:
            return False, "레그 한도 초과"

        snap = self._portfolio_snapshot(holdings)
        pv = float(snap["portfolio_value"])
        tv = float(snap["by_ticker_value"].get(ticker, 0.0))
        avgp = float(snap["avg_price_by_ticker"].get(ticker, 0.0))

        if pv > 0 and (tv / pv) >= self.per_ticker_max_weight:
            return False, "티커 비중 한도 초과"

        if available_cash < max(self.min_order_cash, 0):
            return False, "현금 부족"

        price = _to_float(info.get("Price"), 0.0)
        atr = _to_float(info.get("ATR"), 0.0)
        rsi = _to_float(info.get("RSI"), 50.0)

        if atr > 0 and price < (avgp + self.rebuy_atr_k * atr):
            return False, f"가격조건 미충족(px<{avgp}+{self.rebuy_atr_k}*ATR)"
        if rsi > self.rebuy_rsi_ceiling:
            return False, f"RSI 상한 초과({rsi:.1f}>{self.rebuy_rsi_ceiling})"

        return True, "OK"

    # ── 계좌 파일에서 가용 현금/보유 종목 로드 ─────────────────────────
    def get_account_info_from_files(self) -> Tuple[int, List[Dict], Dict[str, int]]:
        available_cash, holdings, cash_map = self._load_snapshot()

        d2 = cash_map.get("prvs_rcdl_excc_amt", 0)
        nx = cash_map.get("nxdy_excc_amt", 0)
        dn = cash_map.get("dnca_tot_amt", 0)
        tot = cash_map.get("tot_evlu_amt", 0)

        # 동적 현금 관리 적용
        if self.dynamic_cash_enabled:
            total_value = sum(_to_int(h.get("evlu_amt", 0)) for h in holdings if _to_int(h.get("hldg_qty", 0)) > 0) + available_cash
            adjusted_cash = self._apply_dynamic_cash_management(available_cash, total_value)
            
            if adjusted_cash != available_cash:
                logger.info(f"동적 현금 관리 적용: {available_cash:,}원 → {adjusted_cash:,}원")
                available_cash = adjusted_cash

        logger.info(
            f" 계좌 조회 완료\n"
            f"보유종목: {len(holdings)}개\n"
            f"D+2: {d2:,}원\n"
            f"익일: {nx:,}원\n"
            f"예수금: {dn:,}원\n"
            f"총평가: {tot:,}원\n"
            f"→ 사용 가용예산: {available_cash:,}원"
        )
        return available_cash, holdings, cash_map

    # ── 주문 안전 래퍼 ────────────────────────────────────────────────
    def _order_cash_safe(self, **kwargs) -> Dict[str, Any]:
        """
        kis.order_cash() 응답 형태 다양성(DataFrame/dict/str/예외)을 안전하게 표준화.
        반환 키: ok, rt_cd, msg_cd, msg1, http_status, raw, df(optional)
        """
        try:
            # order_cash()가 받는 인자만 필터링 (ord_dv, pdno, ord_dvsn, ord_qty, ord_unpr)
            # ticker가 전달되면 pdno로 변환
            if 'ticker' in kwargs and 'pdno' not in kwargs:
                kwargs['pdno'] = kwargs.pop('ticker')
            valid_params = ['ord_dv', 'pdno', 'ord_dvsn', 'ord_qty', 'ord_unpr']
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params}
            if filtered_kwargs.get("pdno"):
                filtered_kwargs["pdno"] = self._t(filtered_kwargs["pdno"])
            res = self.kis.order_cash(**filtered_kwargs, market=self.market)
            # 1) DataFrame 성공 경로
            if hasattr(res, "empty"):
                if res is None or res.empty:
                    out = {'ok': False, 'rt_cd': None, 'msg_cd': None, 'msg1': 'API 응답 없음', 'http_status': None, 'raw': None}
                    logger.debug("[order_raw:df-empty] %s", _mask_sensitive(out))
                    return out
                rec = res.to_dict('records')[0]
                rt_cd = rec.get('rt_cd', '')
                
                # 개선된 성공 판단 로직
                # ODNO(주문번호)가 있으면 부분 성공으로 간주
                odno = rec.get('ODNO', '')
                has_order_number = bool(odno and odno.strip())
                
                # rt_cd가 '0'이거나 주문번호가 있으면 성공으로 판단
                ok = (rt_cd == '0') or has_order_number
                
                out = {
                    'ok': ok, 'rt_cd': rt_cd, 'msg_cd': rec.get('msg_cd'),
                    'msg1': rec.get('msg1', '메시지 없음'),
                    'http_status': rec.get('status_code'),
                    'raw': rec, 'df': res, 'odno': odno
                }
                logger.debug("[order_raw:df] %s", _mask_sensitive(out.get("raw")))
                return out
            # 2) dict/obj 경로(에러 시 'output' 없음 포함)
            if isinstance(res, dict):
                raw_json = res
                rt_cd = str(res.get('rt_cd', 'EXC'))
                msg_cd = res.get('msg_cd')
                msg1  = res.get('msg1') or res.get('error') or '주문 실패(비표준 응답)'
                
                # 개선된 성공 판단 로직
                # ODNO(주문번호)가 있으면 부분 성공으로 간주
                odno = res.get('ODNO', '')
                has_order_number = bool(odno and odno.strip())
                
                # rt_cd가 '0'이거나 주문번호가 있거나 성공 키워드가 있으면 성공으로 간주
                ok = (rt_cd == '0') or has_order_number or ('성공' in str(msg1)) or ('완료' in str(msg1))
                
                out = {
                    'ok': ok, 'rt_cd': rt_cd, 'msg_cd': msg_cd, 'msg1': msg1,
                    'http_status': res.get('status_code'), 'raw': raw_json, 'odno': odno
                }
                logger.debug("[order_raw:dict] %s", _mask_sensitive(out.get("raw")))
                return out
            # 3) 기타: 문자열 등
            raw_str = str(res)[:800]
            out = {'ok': False, 'rt_cd': 'EXC', 'msg_cd': None, 'msg1': '예상치 못한 응답형태', 'http_status': None, 'raw': raw_str}
            logger.debug("[order_raw:str] %s", _mask_sensitive(raw_str))
            return out
        except Exception as e:
            em = str(e)
            if "'output'" in em:
                out = {'ok': False, 'rt_cd': 'EXC', 'msg_cd': None, 'msg1': "브로커 거절(응답에 output 없음)", 'http_status': None, 'error': em}
                logger.debug("[order_exc:no-output] %s", _mask_sensitive(out))
                return out
            logger.error(f"주문 API 호출 중 예외 발생: {e}", exc_info=True)
            out = {'ok': False, 'rt_cd': 'EXC', 'msg_cd': None, 'msg1': em, 'http_status': None, 'error': em}
            logger.debug("[order_exc] %s", _mask_sensitive(out))
            return out

    def emit_final_summary(self, start_ts: float, status: str = "SUCCESS", warnings: int = 0):
        duration = int(time.time() - start_ts)
        reason = self.summary_reason_code or "N/A"
        detail = (f" | {self.summary_reason_detail}" if self.summary_reason_detail else "")
        line1 = f"RUN: {status} | WARNINGS: {warnings} | DURATION: {duration}s"
        line2 = f"TRADES: {self.stats['buy']} buy / {self.stats['sell']} sell / {self.stats['hold']} hold | REASON: {reason}{detail}"
        logger.info(line1)
        logger.info(line2)

        error_summary = self._get_error_summary()
        if error_summary != "에러 없음":
            logger.info(error_summary)
        if sum(self.recomm_stats.values()) > 0:
            logger.info(f"RECOMMENDATIONS: {self.recomm_stats['buy']} buy / {self.recomm_stats['sell']} sell")
        if self.reporting.get("coherent_summary", False):
            txt = f"✅ 파이프라인 요약\n{line1}\n{line2}"
            if error_summary != "에러 없음":
                txt += f"\n{error_summary}"
            if sum(self.recomm_stats.values()) > 0:
                txt += f"\nRECOMMENDATIONS: {self.recomm_stats['buy']} buy / {self.recomm_stats['sell']} sell"
            stats = {
                "매수": self.stats["buy"],
                "매도": self.stats["sell"],
                "홀드": self.stats["hold"],
            }
            from notifier import create_summary_embed
            _notify_embed(
                create_summary_embed(txt, statistics=stats),
                key=f"phase:summary:{self.run_id}", cooldown=60
            )
    @staticmethod
    def _is_transient_error(result: Dict[str, Any]) -> bool:
        msg = (result.get('msg1') or '').lower()
        status = result.get('http_status')
        hints = ['timeout','timed out','temporarily','일시','too many requests',
                 'service unavailable','bad gateway','gateway','네트워크',
                 'api 응답 없음','overload','busy']
        rt_cd = str(result.get('rt_cd', '')).strip()
        if isinstance(status, int) and 500 <= status < 600:
            return True
        if isinstance(status, int) and status in (408, 429):
            return True
        if rt_cd in {'-1','8'}:
            return True
        return any(h in msg for h in hints)

    @staticmethod
    def _is_reject(result: Dict[str, Any]) -> bool:
        txt = f"{result.get('msg1','')} {result.get('rt_cd','')} {result.get('msg_cd','')}".upper()
        return ('REJECT' in txt) or ('거절' in txt) or ('BROKER' in txt)

    def _order_cash_retry(self, max_retries: int = 3, backoff_base: float = 0.5, **kwargs) -> Dict[str, Any]:
        """
        주문 재시도 래퍼:
        - 첫 실패가 가격밴드/호가 관련이면 호가 1틱(매수: -1틱 / 매도: +1틱) 보정 후 즉시 1회 재시도.
        - 일시 오류는 지수 백오프.
        - 브로커 REJECT이고 retry_on_reject=True면 max_retries_on_reject 만큼 추가 재시도(+ retry_delay_ms 간격)
          * 매수 시 첫 REJECT 재시도에는 1틱 하향 보정 시도
        """
        backoff = backoff_base
        last_res = None

        # kwargs는 내부에서 가격 보정 시 mutate됨(의도)
        for attempt in range(1, max_retries + 1):
            res = self._order_cash_safe(**kwargs)
            last_res = res
            if res.get('ok'):
                return res

            # 가격대역/호가 오류 → 1회 가격 보정 재시도
            msg = (res.get('msg1') or '').lower()
            if attempt == 1 and any(k in msg for k in ['상한', '하한', 'price band', '가격제한', '호가']):
                try:
                    ou = int(kwargs.get('ord_unpr', '0'))
                    if kwargs.get('ord_dv') == '02':  # 매수
                        ou = max(1, ou - get_tick_size(ou))
                    elif kwargs.get('ord_dv') == '01':  # 매도
                        ou = ou + get_tick_size(ou)
                    kwargs['ord_unpr'] = str(ou)
                    continue  # 즉시 다음 루프(보정 가격으로 재시도)
                except Exception:
                    pass

            # 일시 오류면 백오프
            if self._is_transient_error(res):
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                else:
                    break  # 더 이상 시도 불가
            else:
                break  # 일반 실패(비일시)

        # ======= 여기서부터 REJECT 기반 추가 재시도 =======
        if self.retry_on_reject and self._is_reject(last_res) and self.max_retries_on_reject > 0:
            rej_attempts = 0
            # 매수/매도 구분
            is_buy = (kwargs.get('ord_dv') == '02')
            # 첫 REJECT 재시도 시 가격 1틱 보정(매수는 하향/매도는 상향)
            try:
                ou = int(kwargs.get('ord_unpr', '0'))
                if ou > 0:
                    tick = get_tick_size(ou)
                    if is_buy:
                        ou = max(1, ou - tick)
                    else:
                        ou = ou + tick
                    kwargs['ord_unpr'] = str(ou)
            except Exception:
                pass

            while rej_attempts < self.max_retries_on_reject:
                time.sleep(max(0, self.retry_delay_ms) / 1000.0)
                res2 = self._order_cash_safe(**kwargs)
                last_res = res2
                if res2.get('ok'):
                    return res2
                # 추가로 일시 오류가 섞이면 한 번 더 짧게 대기 후 재시도
                if self._is_transient_error(res2):
                    time.sleep(0.4)
                rej_attempts += 1

        return last_res if last_res is not None else {'ok': False, 'rt_cd': None, 'msg1': 'no result'}

    # ── 슬리피지/수수료 적용 보조 ─────────────────────────────────────
    def _apply_buy_slippage_and_round(self, ref_price: int) -> int:
        """
        기준가격(ref_price)에 슬리피지(-bps)를 적용하고 하향 라운딩한다.
        """
        if ref_price <= 0:
            return ref_price
        # 슬리피지는 -bps (예: 10bps => 0.1% 낮게)
        adj = ref_price * (1.0 - max(0.0, self.slippage_bps) / 10000.0)
        # 유효호가 하향 라운딩
        return int(round_to_tick(int(adj), mode="down"))

    def _cap_qty_by_fee_buffer(self, qty: int, price: int, budget: int) -> int:
        """
        수수료/세금 버퍼(fee_buffer_pct)까지 고려하여 qty를 budget 이하로 낮춘다.
        """
        # qty를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        qty = _to_int(qty)
        price = _to_int(price)
        budget = _to_int(budget)
        if qty <= 0 or price <= 0:
            return 0
        fee_mult = 1.0 + max(0.0, self.fee_buffer_pct)
        while qty > 0 and int(qty * price * fee_mult) > int(budget):
            qty -= 1
        return qty

    # ── 쿨다운 보조: 연속 실패 누적 후에만 등록 ───────────────────────
    def _maybe_add_cooldown(self, ticker: str, reason: str, increment_fail: bool = True):
        # [NEW] 확정 실패만 쿨다운(옵션)
        guards = self.trading_guards or {}
        confirmed_only = bool(guards.get("cooldown_only_on_confirmed_failure", False))

        if increment_fail:
            self._fail_counts[ticker] += 1
        else:
            self._fail_counts[ticker] = 0  # 성공 시 리셋

        cnt = self._fail_counts[ticker]
        if cnt >= self.cooldown_fail_threshold:
            # 지연 체결/미체결 확인 후 쿨다운 등록
            executed = self._check_delayed_execution(ticker)
            # 선택적으로, 미체결 주문이 남아있으면 쿨다운 보류
            if confirmed_only:
                try:
                    pending_df = self.kis.get_pending_orders()
                    has_pending = (pending_df is not None and not pending_df.empty and any(self._t(p) == ticker for p in pending_df.get('pdno', [])))
                except Exception:
                    has_pending = False
            else:
                has_pending = False

            if not executed and not has_pending:
                self._add_to_cooldown(ticker, f"{reason} (연속실패 {cnt}회)")
                self._fail_counts[ticker] = 0
            else:
                logger.info(f"  ->  {ticker} 지연 체결 확인으로 쿨다운 등록 생략")
                self._fail_counts[ticker] = 0  # 지연 체결 확인 시 실패 카운터 리셋

    # ── REJECT 응답 처리 개선 ────────────────────────────────────────
    def _classify_reject_type(self, response: Dict, ticker: str) -> str:
        """REJECT 타입 분류"""
        error_msg = response.get('error', '') or response.get('msg1', '')
        reason_code = response.get('reason_code', '')
        
        # 일시적 REJECT 패턴
        temporary_patterns = [
            '시장가 주문 불가',
            '일시적 시스템 오류',
            '주문량 초과',
            '시스템 점검',
            '일시적 오류',
            '네트워크 오류'
        ]
        
        # 영구적 REJECT 패턴  
        permanent_patterns = [
            '잔고 부족',
            '상장폐지',
            '거래정지',
            '계좌 잔고 부족',
            '주문 금액 초과',
            '한도 초과'
        ]
        
        for pattern in temporary_patterns:
            if pattern in error_msg:
                return 'TEMPORARY'
        
        for pattern in permanent_patterns:
            if pattern in error_msg:
                return 'PERMANENT'
        
        return 'UNKNOWN'

    def _handle_reject_response(self, response: Dict, ticker: str, expected_qty: int = None) -> bool:
        """REJECT 응답 처리 - 체결 확인 우선"""
        # 즉시 체결 상태 확인 (지연 체결 가능성 고려)
        delayed_execution = self._check_delayed_execution(ticker, expected_qty)
        
        if delayed_execution:
            logger.info(f"[{ticker}] REJECT 응답이지만 실제 체결 확인됨: {delayed_execution['executed_qty']}주")
            return False  # 실패가 아님
        
        # 체결되지 않은 경우에만 실패 처리
        reject_type = self._classify_reject_type(response, ticker)
        
        if reject_type == 'TEMPORARY':
            # 일시적 REJECT - 재시도만 수행
            logger.info(f"[{ticker}] 일시적 REJECT 감지, 재시도 수행")
            return False
        elif reject_type == 'PERMANENT':
            # 영구적 REJECT - 즉시 쿨다운 등록
            logger.warning(f"[{ticker}] 영구적 REJECT 감지, 쿨다운 등록")
            self._add_to_cooldown(ticker, "영구적 매수 주문 실패")
            return True
        else:
            # 알 수 없는 REJECT - 쿨다운 등록
            logger.warning(f"[{ticker}] 알 수 없는 REJECT, 쿨다운 등록")
            self._maybe_add_cooldown(ticker, "매수 주문 실패")
            return True

    # ── 15시 20분 일괄 체결 확인 시스템 ──────────────────────────────────
    def add_pending_order(self, ticker: str, name: str, side: str, qty: int, price: float, order_id: str = None):
        """미체결 주문 추가 - 상세 로깅 및 알림"""
        pending_order = {
            "ticker": ticker,
            "name": name,
            "side": side,
            "qty": qty,
            "price": price,
            "order_id": order_id,
            "order_time": datetime.now(KST),
            "status": "pending"
        }
        self.pending_orders.append(pending_order)
        
        # 상세 로깅
        logger.info(f" 미체결 주문 추가: {name}({ticker}) {side} {qty}주 @ {price:,.0f}원")
        logger.info(f"   주문번호: {order_id}, 주문시간: {pending_order['order_time']}")
        logger.info(f"   현재 미체결 주문 총 {len(self.pending_orders)}개")
        
        # 디스코드 알림 (간단한 형태)
        _notify(f" 미체결 주문 추가: {name}({ticker}) {side} {qty}주", 
                key=f"pending_order_{ticker}_{side}", cooldown_sec=60)

    def should_run_batch_check(self) -> bool:
        """15시 20분 일괄 체결 확인 실행 여부 확인"""
        if not self.batch_check_enabled:
            return False
        now = datetime.now(KST)
        current_time = now.strftime("%H:%M")
        return current_time >= self.batch_check_time

    def batch_execution_check_and_cancel(self):
        """15시 20분 일괄 체결 확인 + 미체결 주문 취소 처리"""
        if not self.pending_orders:
            logger.info("미체결 주문이 없습니다.")
            return

        # pending_orders 상세 로깅
        buy_count = sum(1 for o in self.pending_orders if o['side'] == 'buy')
        sell_count = sum(1 for o in self.pending_orders if o['side'] == 'sell')
        logger.info(
            f" 미체결 주문 상세: 총 {len(self.pending_orders)}개 | "
            f"매수: {buy_count}개, 매도: {sell_count}개"
        )
        logger.info(f"15시 20분 일괄 체결 확인 시작: {len(self.pending_orders)}개 주문")
        
        # 계좌 스냅샷 갱신
        self._update_account_info(force=True)
        cash, holdings, _ = self._load_snapshot()
        
        executed_orders = []
        cancelled_orders = []
        
        # 미체결 주문별 상세 체결 확인
        for order in self.pending_orders:
            ticker = order["ticker"]
            name = order["name"]
            side = order["side"]
            expected_qty = order["qty"]
            price = order["price"]
            order_time = order.get("order_time", "N/A")
            order_id = order.get("order_id")
            
            logger.info(f"체결 확인 중: {name}({ticker}) {side} {expected_qty}주 @ {price:,.0f}원 (주문시간: {order_time})")
            
            # 현재 보유 수량 확인
            current_qty = self._get_qty(holdings, ticker)
            
            if side == "buy":
                # 매수 주문: 보유 수량이 증가했으면 체결
                if current_qty > 0:
                    executed_qty = current_qty
                    logger.info(f"✅ 매수 체결 확인: {name}({ticker}) {executed_qty}주 (예상: {expected_qty}주)")
                    
                    # 체결 수량이 예상과 다른 경우 경고
                    if executed_qty != expected_qty:
                        logger.warning(f"⚠️ 체결 수량 불일치: {name}({ticker}) 실제={executed_qty}주, 예상={expected_qty}주")
                    
                    # 거래 기록 저장
                    _batch_buy_payload = {
                        "side": side, "ticker": ticker, "name": name,
                        "qty": executed_qty, "price": price, "trade_status": "executed",
                        "order_id": order_id,
                        "requested_qty": expected_qty,
                        "executed_qty": executed_qty,
                        "_debug_context": "batch_check_buy",
                        "strategy_details": {
                            "batch": "BATCH_CHECK", 
                            "order_type": "batch_executed",
                            "expected_qty": expected_qty,
                            "actual_qty": executed_qty,
                            "order_time": str(order_time)
                        }
                    }
                    _db_dbg_trade_in("trader.batch_check.BUY_EXECUTED_DB", _batch_buy_payload)
                    record_trade(_batch_buy_payload)
                    
                    executed_orders.append(order)
                    self.stats[side] += 1
                    logger.info(f" 통계 업데이트: {side} +1 (15시 20분 체결 확인) | {name}({ticker}) {executed_qty}주")
                else:
                    # 미체결: 취소 처리 + DB pending 기록을 cancelled로 정리 (당일 매도 방지가 잘못 적용되지 않도록)
                    _db_dbg_log(
                        "trader.batch_check.BUY_NOT_FILLED",
                        ticker=ticker,
                        order_id=order_id or "(empty)",
                        expected_qty=expected_qty,
                        current_qty=current_qty,
                    )
                    if order_id:
                        mark_pending_order_cancelled(order_id)
                    else:
                        mark_pending_buy_cancelled(ticker)
                    logger.info(f"❌ 매수 미체결 취소: {name}({ticker}) {expected_qty}주")
                    cancelled_orders.append(order)
            else:
                # 매도 주문: 보유 수량이 감소했으면 체결
                if current_qty == 0:
                    executed_qty = expected_qty
                    logger.info(f"✅ 매도 체결 확인: {name}({ticker}) {executed_qty}주")
                    
                    # 매도 시 가격이 0이면 현재가 조회
                    final_price = price
                    if price <= 0:
                        try:
                            price_df = self.kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd=ticker)
                            if price_df is not None and not price_df.empty and 'stck_prpr' in price_df.columns:
                                final_price = _to_int(price_df['stck_prpr'].iloc[0])
                                logger.info(f"  -> 매도 체결가 조회: {final_price:,}원")
                        except Exception as e:
                            logger.warning(f"  -> 매도 체결가 조회 실패: {e}")
                    
                    # 거래 기록 저장
                    _batch_sell_payload = {
                        "side": side, "ticker": ticker, "name": name,
                        "qty": executed_qty, "price": final_price, "trade_status": "executed",
                        "order_id": order_id,
                        "requested_qty": expected_qty,
                        "executed_qty": executed_qty,
                        "_debug_context": "batch_check_sell",
                        "strategy_details": {
                            "batch": "BATCH_CHECK",
                            "order_type": "batch_executed",
                            "order_time": str(order_time)
                        }
                    }
                    _db_dbg_trade_in("trader.batch_check.SELL_EXECUTED_DB", _batch_sell_payload)
                    record_trade(_batch_sell_payload)
                    
                    executed_orders.append(order)
                    self.stats[side] += 1
                    logger.info(f" 통계 업데이트: {side} +1 (15시 20분 체결 확인) | {name}({ticker}) {executed_qty}주")
                else:
                    # 미체결: 취소 처리
                    _db_dbg_log(
                        "trader.batch_check.SELL_NOT_FILLED",
                        ticker=ticker,
                        order_id=order_id or "(empty)",
                        expected_qty=expected_qty,
                        current_qty=current_qty,
                    )
                    logger.info(f"❌ 매도 미체결 취소: {name}({ticker}) {expected_qty}주 (현재 보유: {current_qty}주)")
                    cancelled_orders.append(order)
        
        # 미체결 주문 리스트 정리
        self.pending_orders = []
        
        # 결과 요약
        logger.info(f"일괄 체결 확인 완료: 체결 {len(executed_orders)}개, 취소 {len(cancelled_orders)}개")
        
        # 통계 업데이트 요약
        stats_summary = f"통계 업데이트: buy={self.stats['buy']}, sell={self.stats['sell']}, hold={self.stats['hold']}"
        logger.info(f" {stats_summary}")
        
        # 디스코드 알림
        if executed_orders or cancelled_orders:
            message = f" 15시 20분 체결 확인 결과\n"
            message += f"✅ 체결: {len(executed_orders)}개\n"
            message += f"❌ 취소: {len(cancelled_orders)}개"
            
            # 체결된 주문 상세 정보 추가
            if executed_orders:
                message += "\n\n 체결 내역:"
                for order in executed_orders:
                    message += f"\n• {order['name']}({order['ticker']}) {order['side']} {order['qty']}주"
            
            # 취소된 주문 상세 정보 추가
            if cancelled_orders:
                message += "\n\n❌ 취소 내역:"
                for order in cancelled_orders:
                    message += f"\n• {order['name']}({order['ticker']}) {order['side']} {order['qty']}주"
            
            _notify(message, key="batch_check_result", cooldown_sec=0)

    # ── 지연 체결 확인 (개선된 버전) - 주문번호 기반 정확한 체결 확인 ──────────────────────────────────
    def _check_delayed_execution(self, ticker: str, expected_qty: int = None, odno: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        지연 체결 확인 - 주문번호 기반 정확한 체결 확인
        BROKER_REJECT 응답이지만 실제로는 체결된 경우를 감지
        """
        # expected_qty를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        if expected_qty is not None:
            expected_qty = _to_int(expected_qty)
        try:
            # ODNO가 제공되면 해당 주문에 대해 우선 폴링(최대 3회)
            for _ in range(3):
                recent_orders = self._get_recent_orders(ticker, limit=10)
                if recent_orders:
                    for order in recent_orders:
                        if odno and order.get('order_id') != odno:
                            continue
                        if order.get('status') == 'executed' and order.get('executed_qty', 0) > 0:
                            executed_qty = _to_int(order.get('executed_qty', 0))
                            logger.info(f"[{ticker}] 지연 체결 확인: {executed_qty}주 체결 (주문번호: {order.get('order_id', 'N/A')})")
                            if expected_qty and abs(executed_qty - expected_qty) <= (expected_qty * 0.1):
                                logger.info(f"[{ticker}] 예상 수량과 일치하는 체결 확인: {executed_qty}/{expected_qty}")
                            return {
                                'executed_qty': executed_qty,
                                'order_id': order.get('order_id'),
                                'execution_time': order.get('execution_time'),
                                'price': order.get('price'),
                                'status': 'executed'
                            }
                time.sleep(0.6)
            logger.debug(f"[{ticker}] 주문번호 기준 체결 확인 실패(ODNO={odno or 'N/A'})")
            return None
            
        except Exception as e:
            logger.error(f"[{ticker}] 지연 체결 확인 실패: {e}")
            return None
    
    def _get_recent_orders(self, ticker: str, limit: int = 10) -> List[Dict]:
        """최근 주문 내역 조회"""
        try:
            if not self.is_real_trading:
                return []
            
            # 통합 주문 조회 유틸 사용
            return self._list_orders(ticker=ticker, side=None, executed=None, limit=limit)
            
        except Exception as e:
            logger.error(f"[{ticker}] 최근 주문 조회 실패: {e}")
            return []

    def _list_orders(self, ticker: str, side: Optional[str] = None, executed: Optional[bool] = None, limit: int = 10) -> List[Dict]:
        """주문 목록을 조회하여 공통 포맷으로 반환한다.
        get_pending_orders()를 우선 사용하고, 실패 시 inquire_daily_order()로 폴백.
        side: 'buy'|'sell'|None, executed: True|False|None
        """
        try:
            if not self.is_real_trading:
                return []
            
            # 1차: get_pending_orders() 우선 사용 (inquire_orders() 기반)
            try:
                pending_df = self.kis.get_pending_orders()
                if pending_df is not None and not pending_df.empty:
                    # ticker 필터링
                    if 'pdno' in pending_df.columns:
                        df = pending_df[pending_df['pdno'] == ticker]
                    else:
                        df = pending_df
                    
                    # side 필터링
                    if side in ("buy", "sell") and 'sll_buy_dvsn_cd' in df.columns:
                        df = df[df['sll_buy_dvsn_cd'] == ('02' if side == 'buy' else '01')]
                    
                    # executed 필터링
                    if executed is not None and 'tot_ccld_qty' in df.columns:
                        if executed is True:
                            df = df[pd.to_numeric(df['tot_ccld_qty'], errors='coerce') > 0]
                        elif executed is False:
                            df = df[pd.to_numeric(df['tot_ccld_qty'], errors='coerce') == 0]
                    
                    if not df.empty:
                        df = df.head(limit)
                        orders: List[Dict] = []
                        for _, row in df.iterrows():
                            executed_qty = int(pd.to_numeric(row.get('tot_ccld_qty', 0), errors='coerce') or 0)
                            orders.append({
                                'order_id': str(row.get('odno', '')),
                                'ticker': self._t(row.get('pdno', '')),
                                'side': 'buy' if str(row.get('sll_buy_dvsn_cd', '')) == '02' else 'sell',
                                'quantity': int(pd.to_numeric(row.get('ord_qty', 0), errors='coerce') or 0),
                                'executed_qty': executed_qty,
                                'price': int(pd.to_numeric(row.get('ord_unpr', 0), errors='coerce') or 0),
                                'status': 'executed' if executed_qty > 0 else 'pending',
                                'execution_time': str(row.get('ord_tmd', '')),
                                'order_time': str(row.get('ord_tmd', ''))
                            })
                        if orders:
                            logger.debug(f"[{ticker}] get_pending_orders()로 주문 조회 성공: {len(orders)}개")
                            return orders
            except Exception as e1:
                logger.debug(f"[{ticker}] get_pending_orders() 실패, inquire_daily_order()로 폴백: {e1}")
            
            # 2차: inquire_daily_order() 폴백 (기존 로직)
            orders_df = self.kis.inquire_daily_order(
                cano=self.kis.cano,
                acnt_prdt_cd=self.kis.acnt_prdt_cd,
                inqr_strt_dt=datetime.now(KST).strftime("%Y%m%d"),
                inqr_end_dt=datetime.now(KST).strftime("%Y%m%d"),
                sll_buy_dvsn_cd="00",  # 전체
                inqr_dvsn="00",        # 전체
                sort_ord="2",          # 시간 역순
                ord_gnno_yn="N",
                odno="",
                inqr_dvsn_3="00",
                inqr_dvsn_1="",
                tot_ccld_qty_smtl_yn="N"
            )
            if orders_df is not None and not orders_df.empty:
                df = orders_df[orders_df['pdno'] == ticker]
                if side in ("buy", "sell"):
                    df = df[df['sll_buy_dvsn_cd'] == ('02' if side == 'buy' else '01')]
                if executed is True:
                    df = df[df['tot_ccld_qty'].astype(int) > 0]
                elif executed is False:
                    df = df[df['tot_ccld_qty'].astype(int) == 0]
                df = df.head(limit)
                orders: List[Dict] = []
                for _, row in df.iterrows():
                    orders.append({
                        'order_id': row.get('odno', ''),
                        'ticker': row.get('pdno', ''),
                        'side': 'buy' if row.get('sll_buy_dvsn_cd') == '02' else 'sell',
                        'quantity': int(row.get('ord_qty', 0)),
                        'executed_qty': int(row.get('tot_ccld_qty', 0)),
                        'price': int(float(row.get('ord_unpr', 0)) if row.get('ord_unpr', 0) is not None else 0),
                        'status': 'executed' if int(row.get('tot_ccld_qty', 0)) > 0 else 'pending',
                        'execution_time': row.get('ord_tmd', ''),
                        'order_time': row.get('ord_tmd', '')
                    })
                logger.debug(f"[{ticker}] inquire_daily_order()로 주문 조회 성공: {len(orders)}개")
                return orders
            
            return []
        except Exception as e:
            # 중복 오류 메시지 방지: 같은 오류는 DEBUG 레벨로만 로깅
            error_msg = str(e)
            if "inquire_daily_order" in error_msg or "get_pending_orders" in error_msg:
                logger.debug(f"[{ticker}] 주문 목록 조회 실패 (API 메서드 누락): {e}")
            else:
                logger.error(f"[{ticker}] 주문 목록 조회 실패: {e}")
            return []

    # ── 중복 주문 방지 시스템 ──────────────────────────────────────────────
    def _check_pending_orders(self, ticker: str) -> List[Dict]:
        """특정 종목의 미체결 주문 확인 (공통 유틸 사용)"""
        try:
            return self._list_orders(ticker=ticker, side="buy", executed=False, limit=50)
        except Exception as e:
            # 중복 오류 메시지 방지: 같은 오류는 DEBUG 레벨로만 로깅
            error_msg = str(e)
            if "inquire_daily_order" in error_msg:
                logger.debug(f"[{ticker}] 미체결 주문 확인 실패 (API 메서드 누락): {e}")
            else:
                logger.error(f"[{ticker}] 미체결 주문 확인 실패: {e}")
            return []

    # ── 주문 후처리 공통화 ───────────────────────────────────────────────
    @staticmethod
    def _extract_order_id(res: Optional[Dict[str, Any]], primary_key: str = "ODNO") -> str:
        """주문 응답(dict)에서 주문번호를 관용적으로 추출한다."""
        if not isinstance(res, dict):
            _db_dbg_skip("trader.extract_order_id.SKIP", reason="res not dict", res_type=type(res).__name__)
            return ""
        odno = extract_broker_order_id(res, primary_key=primary_key)
        _db_dbg_log(
            "trader.extract_order_id",
            primary_key=primary_key,
            extracted=odno or "(empty)",
            res_keys=sorted(res.keys())[:25],
            res_status=res.get("status"),
            res_ok=res.get("ok"),
            has_output=bool(res.get("output")),
        )
        return odno

    def _finalize_buy_result(self, *,
                             context: str,
                             name: str,
                             ticker: str,
                             requested_qty: int,
                             price: int,
                             res: Dict,
                             add_pending: bool = True,
                             order_id_key: str = 'ODNO') -> Tuple[str, int]:
        """주문 결과에 대한 공통 후처리.
        반환: (status, executed_qty) status in {'blocked','executed','pending','failed'}
        """
        # 중복 방지로 차단
        if res.get("status") == "blocked":
            logger.warning(f"[{context}] 중복 주문 방지로 차단: {name}({ticker})")
            return ("blocked", 0)

        # 성공 또는 즉시 체결 확인된 경우
        if res.get("status") == "executed" or int(res.get('executed_qty', 0)) > 0:
            executed_qty = int(res.get('executed_qty', 0))
            validation = res.get('validation', {})
            if validation.get('alert'):
                logger.warning(f"[{context}] 체결 수량 검증 경고: {validation['message']}")
            if executed_qty > 0:
                logger.info(f"[{context}] ✅ 즉시 체결: {name}({ticker}) {executed_qty}주 @ {price:,.0f}원")
                odno = self._extract_order_id(res, order_id_key)
                _executed_payload = {
                    "side": "buy", "ticker": ticker, "name": name,
                    "qty": executed_qty, "price": price, "trade_status": "executed",
                    "order_id": odno,
                    "requested_qty": requested_qty,
                    "executed_qty": executed_qty,
                    "_debug_context": context,
                    "strategy_details": {"batch": context, "order_type": "immediate_execution"}
                }
                _db_dbg_trade_in("trader.finalize_buy.EXECUTED_DB", _executed_payload, res=res)
                record_trade(_executed_payload)
                self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=False)
                return ("executed", executed_qty)
        # 접수만 된 경우 또는 제출됨 → pending 기록으로 당일 매도 방지가 즉시 적용되도록 DB에 남김
        if res.get("ok") or res.get("status") == "submitted":
            if add_pending:
                odno = self._extract_order_id(res, order_id_key)
                if odno:
                    self.add_pending_order(ticker, name, "buy", requested_qty, price, odno)
                    _pending_payload = {
                        "side": "buy", "ticker": ticker, "name": name,
                        "qty": requested_qty, "price": price, "trade_status": "pending",
                        "order_id": odno,
                        "requested_qty": requested_qty,
                        "executed_qty": 0,
                        "_debug_context": context,
                        "strategy_details": {"batch": "PENDING", "order_type": "pending_15h20_confirmation", "context": context}
                    }
                    _db_dbg_trade_in("trader.finalize_buy.PENDING_DB", _pending_payload, res=res)
                    record_trade(_pending_payload)
                    logger.info(f"[{context}] 주문 제출 완료: {name}({ticker}) {requested_qty}주 @ {price:,.0f}원 (15시 20분 체결 확인, 당일 매도 방지 적용)")
                else:
                    logger.warning(
                        f"[{context}] 주문번호 없음 → DB pending 생략: {name}({ticker}) "
                        "(order_reconciler 연동 불가)"
                    )
            return ("pending", 0)

        # 실패 케이스: REJECT 처리 및 기록
        err = res.get("msg1", res.get("message", "Unknown error"))
        should_cooldown = self._handle_reject_response(res, ticker, requested_qty)
        odno = self._extract_order_id(res, order_id_key)
        _failed_payload = {
            "side": "buy", "ticker": ticker, "name": name,
            "qty": requested_qty, "price": price, "trade_status": "failed",
            "order_id": odno,
            "requested_qty": requested_qty,
            "executed_qty": 0,
            "_debug_context": context,
            "strategy_details": {"error": err, "reason_code": "BROKER_REJECT", "context": context}
        }
        _db_dbg_trade_in("trader.finalize_buy.FAILED_DB", _failed_payload, res=res)
        record_trade(_failed_payload)
        return ("failed", 0)

    def _prevent_duplicate_orders(self, ticker: str, quantity: int) -> bool:
        """중복 주문 방지"""
        # quantity를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        quantity = _to_int(quantity)
        try:
            pending_orders = self._check_pending_orders(ticker)
            
            if pending_orders:
                total_pending = sum(_to_int(order.get('quantity', 0)) for order in pending_orders)
                logger.warning(f"[{ticker}] 미체결 주문 존재: {total_pending}주")
                
                # 미체결 주문이 목표 수량의 50% 이상이면 새 주문 차단
                if total_pending >= quantity * 0.5:
                    logger.warning(f"[{ticker}] 중복 주문 방지: 미체결 수량 충분 ({total_pending}/{quantity})")
                    
                    # 디스코드 알림
                    message = f" 중복 주문 방지\n"
                    message += f"종목: {ticker}\n"
                    message += f"목표 수량: {quantity}주\n"
                    message += f"미체결 수량: {total_pending}주\n"
                    message += f"미체결 비율: {total_pending/quantity:.1%}"
                    
                    _notify(message, key=f"duplicate_prevention_{ticker}", cooldown_sec=300)
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"[{ticker}] 중복 주문 방지 확인 실패: {e}")
            return True  # 오류 시 허용

    def _execute_order_with_validation(self, ticker: str, name: str, quantity: int, price: float, batch_name: str) -> Dict:
        """검증된 주문 실행"""
        # quantity를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        quantity = _to_int(quantity)
        # 1. 미체결 주문 확인
        if not self._prevent_duplicate_orders(ticker, quantity):
            return {
                "status": "blocked", 
                "reason": "duplicate_prevention",
                "message": f"미체결 주문으로 인한 중복 주문 방지"
            }
        
        # 2. 주문 실행
        result = self._order_cash_retry(
            ord_dv="01", pdno=ticker, ord_dvsn="01", 
            ord_qty=str(quantity), ord_unpr=str(int(price))
        )
        
        # 3. 즉시 체결 확인
        if not result.get('ok', False):
            execution_check = self._check_delayed_execution(ticker, quantity)
            if execution_check:
                logger.info(f"[{ticker}] 주문 실패 응답이지만 실제 체결 확인됨")
                result['ok'] = True
                result['executed_qty'] = execution_check['executed_qty']
                result['status'] = 'executed'
        
        return result

    # ── 체결 수량 검증 시스템 ──────────────────────────────────────────────
    def _validate_execution_quantity(self, ticker: str, target_qty: int, actual_qty: int) -> Dict:
        """체결 수량 검증"""
        # target_qty와 actual_qty를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        target_qty = _to_int(target_qty)
        actual_qty = _to_int(actual_qty)
        try:
            tolerance = 0.1  # 10% 허용 오차
            
            if actual_qty == target_qty:
                return {
                    "status": "exact", 
                    "message": "정확한 수량 체결",
                    "alert": False
                }
            
            elif abs(actual_qty - target_qty) <= target_qty * tolerance:
                return {
                    "status": "acceptable", 
                    "message": f"허용 범위 내 체결 ({actual_qty}/{target_qty})",
                    "alert": False
                }
            
            elif actual_qty > target_qty * 1.5:
                return {
                    "status": "excessive", 
                    "message": f"과도한 체결 ({actual_qty}/{target_qty})",
                    "alert": True,
                    "excess_ratio": actual_qty / target_qty
                }
            
            else:
                return {
                    "status": "insufficient", 
                    "message": f"부족한 체결 ({actual_qty}/{target_qty})",
                    "alert": False
                }
                
        except Exception as e:
            logger.error(f"[{ticker}] 체결 수량 검증 실패: {e}")
            return {
                "status": "error",
                "message": f"검증 오류: {e}",
                "alert": True
            }

    def _handle_excessive_execution(self, ticker: str, name: str, target_qty: int, actual_qty: int):
        """과도한 체결 처리"""
        # target_qty와 actual_qty를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        target_qty = _to_int(target_qty)
        actual_qty = _to_int(actual_qty)
        try:
            excess_ratio = actual_qty / target_qty if target_qty > 0 else 0
            excess_qty = actual_qty - target_qty
            
            # 즉시 알림
            message = f" 과도한 체결 감지\n"
            message += f"종목: {name}({ticker})\n"
            message += f"목표: {target_qty:,}주\n"
            message += f"실제: {actual_qty:,}주\n"
            message += f"초과: {excess_qty:,}주\n"
            message += f"초과율: {excess_ratio:.1f}배"
            
            _notify(message, key=f"excessive_execution_{ticker}", cooldown_sec=0)
            
            # 추가 매도 검토 (2배 초과 시)
            if excess_ratio > 2.0:
                logger.warning(f"[{ticker}] 과도한 체결로 인한 추가 매도 검토: {excess_qty}주")
                
                # 추가 매도 알림
                sell_message = f"⚠️ 추가 매도 검토 필요\n"
                sell_message += f"종목: {name}({ticker})\n"
                sell_message += f"초과 수량: {excess_qty:,}주\n"
                sell_message += f"추천 액션: 부분 매도 고려"
                
                _notify(sell_message, key=f"excessive_sell_review_{ticker}", cooldown_sec=600)
                
                # 향후 자동 매도 로직 구현 가능
                # self._consider_partial_sell(ticker, excess_qty)
            
        except Exception as e:
            logger.error(f"[{ticker}] 과도한 체결 처리 실패: {e}")
    def _execute_order_with_full_validation(self, ticker: str, name: str, quantity: int, price: float, batch_name: str) -> Dict:
        """완전한 검증을 포함한 주문 실행"""
        # quantity를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        quantity = _to_int(quantity)
        try:
            # 1. 미체결 주문 확인
            if not self._prevent_duplicate_orders(ticker, quantity):
                return {
                    "status": "blocked", 
                    "reason": "duplicate_prevention",
                    "message": f"미체결 주문으로 인한 중복 주문 방지"
                }
            
            # 2. 주문 실행
            logger.info(f"[{batch_name}] 주문 실행: {name}({ticker}) {quantity}주 @ {price:,.0f}원")
            result = self._order_cash_retry(
                ord_dv="01", pdno=ticker, ord_dvsn="01", 
                ord_qty=str(quantity), ord_unpr=str(int(price))
            )
            try:
                self._last_order_odno = result.get('odno')
            except Exception:
                pass
            
            # 3. 즉시 체결 확인
            executed_qty = 0
            if not result.get('ok', False):
                execution_check = self._check_delayed_execution(ticker, quantity, result.get('odno'))
                if execution_check:
                    logger.info(f"[{ticker}] 주문 실패 응답이지만 실제 체결 확인됨")
                    result['ok'] = True
                    executed_qty = execution_check['executed_qty']
                    result['executed_qty'] = executed_qty
                    result['status'] = 'executed'
            else:
                # 응답 ok는 '주문 접수'로만 간주 (즉시 체결로 간주하지 않음)
                executed_qty = 0
            
            # 4. 체결 수량 검증
            if executed_qty > 0:
                validation = self._validate_execution_quantity(ticker, quantity, executed_qty)
                
                if validation['alert']:
                    logger.warning(f"[{ticker}] 체결 수량 검증 경고: {validation['message']}")
                    
                    if validation['status'] == 'excessive':
                        self._handle_excessive_execution(ticker, name, quantity, executed_qty)
                
                result['validation'] = validation
            
            return result
            
        except Exception as e:
            logger.error(f"[{ticker}] 완전 검증 주문 실행 실패: {e}")
            return {
                "status": "error",
                "error": str(e),
                "message": f"주문 실행 중 오류 발생"
            }

    # ── 공용 시장가 매도 (리밸런싱 등) ────────────────────────────────
    def _execute_market_sell(self, ticker: str, quantity: int, name: str, reason_text: str, reason_code: str = "REBALANCE_SWAP") -> Dict[str, Any]:
        # quantity를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        quantity = _to_int(quantity)
        if quantity <= 0:
            return {"status": "sell_fail", "filled_qty": 0, "rt_cd": None, "msg1": "qty<=0"}

        logger.info(f"[REBALANCE] 매도 실행: {name}({ticker}) {quantity}주 | 사유={reason_text}")

        if self.is_real_trading:
            pre_qty = quantity

            result = self._order_cash_retry(
                ord_dv="01", pdno=ticker, ord_dvsn="01", ord_qty=str(quantity), ord_unpr="0"
            )

            time.sleep(2)
            self._update_account_info(force=True)
            _, holdings_after, _ = self._load_snapshot()
            post_qty = self._get_qty(holdings_after, ticker)
            filled_qty = max(0, pre_qty - post_qty)

            # 체결가 근사(시세 조회 실패 시 0 허용)
            try:
                price_df = self.kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd=ticker)
                current_price = _to_int(price_df['stck_prpr'].iloc[0]) if (price_df is not None and not price_df.empty) else 0
            except Exception:
                current_price = 0

            # 결과 기록 및 쿨다운 처리
            if result.get('ok') or filled_qty > 0:
                if filled_qty > 0:
                    # 즉시 체결된 경우
                    trade_status = "completed"
                    record_trade({
                        "side": "sell", "ticker": ticker, "name": name,
                        "qty": filled_qty, "price": current_price,
                        "trade_status": trade_status,
                        "strategy_details": {
                            "reason": reason_text,
                            "reason_code": reason_code,
                            "broker_msg": result.get('msg1')
                        },
                        "sell_reason": reason_text
                    })
                    _notify_embed(create_trade_embed({
                        "side": "SELL", "name": name, "ticker": ticker,
                        "qty": filled_qty, "price": current_price, "trade_status": trade_status,
                        "strategy_details": {"reason": reason_text, "reason_code": reason_code, "broker_msg": result.get('msg1')}
                    }), key=f"phase:rebalance_sell:{ticker}:{self.run_id}", cooldown=30)

                    # ✅ 매도 통계 반영
                    self.stats["sell"] += 1
                    return {"status": "executed", "filled_qty": filled_qty, "rt_cd": result.get('rt_cd'), "msg1": result.get('msg1')}
                else:
                    # 주문 제출되었지만 미체결: 미체결 주문 리스트에 추가
                    odno = self._extract_order_id(result)
                    self.add_pending_order(ticker, name, "sell", quantity, current_price, odno)
                    logger.info(f"[REBALANCE] 매도 주문 제출 완료: {name}({ticker}) {quantity}주 (15시 20분 체결 확인)")
                    return {"status": "submitted", "filled_qty": 0, "rt_cd": result.get('rt_cd'), "msg1": result.get('msg1')}
            else:
                err = result.get('msg1', 'Unknown error')
                record_trade({
                    "side": "sell", "ticker": ticker, "name": name,
                    "qty": quantity, "price": current_price,
                    "trade_status": "failed",
                    "strategy_details": {
                        "error": err,
                        "rt_cd": result.get('rt_cd'),
                        "msg_cd": result.get('msg_cd'),
                        "reason": reason_text,
                        "reason_code": reason_code
                    },
                    "sell_reason": reason_text
                })
                _notify_embed(create_trade_embed({
                    "side": "SELL", "name": name, "ticker": ticker,
                    "qty": quantity, "price": current_price, "trade_status": "failed",
                    "strategy_details": {"error": err, "reason_code": reason_code}
                }), key=f"phase:rebalance_sell_fail:{ticker}:{self.run_id}", cooldown=30)

                # 연속 실패 누적 → 기준치 도달 시에만 쿨다운
                self._maybe_add_cooldown(ticker, "리밸런스 매도 주문 실패", increment_fail=True)
                return {"status": "sell_fail", "filled_qty": 0, "rt_cd": result.get('rt_cd'), "msg1": result.get('msg1')}
        else:
            record_trade({
                "side": "sell", "ticker": ticker, "name": name,
                "qty": quantity, "price": 0, "trade_status": "completed",
                "strategy_details": {"reason": reason_text, "reason_code": reason_code},
                "sell_reason": reason_text
            })
            logger.info(f"  -> [모의] REBALANCE SELL {name}({ticker}) x{quantity}")
            _notify_text(
                f" [모의] REBALANCE SELL {name}({ticker}) x{quantity}",
                key=f"phase:paper_rebalance_sell:{ticker}:{self.run_id}", cooldown=30
            )
            # ✅ 매도 통계 반영(모의)
            self.stats["sell"] += 1
            return {"status": "executed", "filled_qty": quantity, "rt_cd": "0", "msg1": "paper"}

    # ── 점수 캐시 로드 ────────────────────────────────────────────────
    def _load_latest_scores(self) -> Tuple[Dict[str, float], Optional[str]]:
        f = find_latest_file("screener_scores_*.json")
        if not f:
            logger.info("점수 캐시 파일(screener_scores_*.json)을 찾지 못했습니다.")
            return {}, None
        try:
            with open(str(f), "r", encoding="utf-8") as fh:
                arr = json.load(fh)
            if not isinstance(arr, list):
                return {}, None
            m: Dict[str, float] = {}
            for row in arr:
                t = self._t(row.get("ticker", ""))
                sc = _to_float(row.get("score_total"), 0.0)
                if t and t != "000000":
                    m[t] = sc
            logger.info("점수 캐시 로드: %s (tickers=%d)", f.name, len(m))
            return m, f.name
        except Exception as e:
            logger.warning("점수 캐시 로드 실패(%s): %s", f.name, e)
            return {}, None

    def _load_holdings_scores(self) -> Dict[str, Dict[str, Any]]:
        """screener_holdings 파일에서 보유 종목 스코어를 로드합니다."""
        f = find_latest_file("screener_holdings_*.json")
        if not f:
            logger.info("보유 종목 스코어 파일(screener_holdings_*.json)을 찾지 못했습니다.")
            return {}
        try:
            with open(str(f), "r", encoding="utf-8") as fh:
                holdings_data = json.load(fh)
            if not isinstance(holdings_data, list):
                return {}
            
            holdings_scores = {}
            for holding in holdings_data:
                ticker = self._t(holding.get("ticker", ""))
                if ticker and ticker != "000000":
                    holdings_scores[ticker] = {
                        "name": holding.get("name", ""),
                        "sector": holding.get("sector", ""),
                        "price": holding.get("price", 0),
                        "score": holding.get("score", 0.0),
                        "fin_score": holding.get("fin_score", 0.0),
                        "tech_score": holding.get("tech_score", 0.0),
                        "mkt_score": holding.get("mkt_score", 0.0),
                        "sector_score": holding.get("sector_score", 0.0),
                        "vol_kki": holding.get("vol_kki", 0.0),
                        "pos_52w": holding.get("pos_52w", 0.0),
                        "per": holding.get("per"),
                        "pbr": holding.get("pbr"),
                        "rsi": holding.get("rsi", 0.0),
                        "atr": holding.get("atr", 0.0),
                        "ma50": holding.get("ma50", 0.0),
                        "ma200": holding.get("ma200", 0.0),
                        "updated_at": holding.get("updated_at", "")
                    }
            
            logger.info("보유 종목 스코어 로드: %s (tickers=%d)", f.name, len(holdings_scores))
            return holdings_scores
        except Exception as e:
            logger.warning("보유 종목 스코어 로드 실패(%s): %s", f.name, e)
            return {}

    # ── 회전(교체) 시도 ───────────────────────────────────────────────
    def _is_rotation_enabled(self) -> bool:
        """config.json의 rotation.enabled 값을 안전하게 확인한다."""
        return bool((self.settings.get("rotation") or {}).get("enabled", False))

    def try_rotation(self, candidates: List[Dict[str, Any]], holdings: List[Dict[str, Any]], usable_cash: int) -> bool:
        """
        회전 매매 시도 (RotationManager 위임)
        
        rotation.enabled=false이면 어떤 경로에서도 회전 매매를 실행하지 않는다.
        """
        if not self._is_rotation_enabled():
            logger.info("회전 매매 비활성화(rotation.enabled=false) → 회전 매매 시도 생략")
            return False
        return self.rotation_manager.try_rotation(candidates, holdings, usable_cash)
    
    def get_rotation_performance(self) -> Dict[str, Any]:
        """회전 매매 성과 조회"""
        return self.rotation_manager.get_performance_summary()
    
    def reset_rotation_state(self):
        """회전 매매 상태 초기화 (일일 리셋용)"""
        self.rotation_manager.reset_state()

    # ── 분할 매수 추적 클래스 ──────────────────────────────────────────
    class SplitOrderTracker:
        def __init__(self, ticker: str, total_qty: int, splits: int):
            self.ticker = ticker
            self.total_qty = total_qty
            self.splits = splits
            self.executed_qty = 0
            self.pending_orders = []
            self.failed_orders = []
            self.start_time = time.time()
        
        def track_split_execution(self, split_num: int, qty: int, status: str, price: int = 0):
            if status == 'executed':
                self.executed_qty += qty
                self.pending_orders.append((split_num, qty, 'executed', price))
                logger.info(f"[SPLIT {split_num}/{self.splits}] ✅ {self.ticker} x{qty} @ {price:,} [EXECUTED]")
            else:
                self.failed_orders.append((split_num, qty, status, price))
                logger.info(f"[SPLIT {split_num}/{self.splits}] ❌ {self.ticker} x{qty} @ {price:,} [{status}]")
        
        def is_fully_executed(self) -> bool:
            return self.executed_qty >= self.total_qty
        
        def get_remaining_qty(self) -> int:
            return max(0, self.total_qty - self.executed_qty)
        
        def get_execution_summary(self) -> Dict[str, Any]:
            return {
                'ticker': self.ticker,
                'total_qty': self.total_qty,
                'executed_qty': self.executed_qty,
                'remaining_qty': self.get_remaining_qty(),
                'execution_rate': self.executed_qty / self.total_qty if self.total_qty > 0 else 0,
                'pending_count': len(self.pending_orders),
                'failed_count': len(self.failed_orders),
                'duration': time.time() - self.start_time
            }

    # ── 분할 매수 헬퍼 (개선된 검증 시스템 적용) ────────────────────────────────────────────────
    def _place_split_buy(self, name: str, ticker: str, total_qty: int, base_price: int, batch_name: str) -> Tuple[bool, int]:
        """
        슬라이스 분할 매수 실행 (완전한 검증 시스템 적용).
        - 설정: trading_params.split_buy.enabled, slices, weights[], ladder_ticks[], interval_sec, jitter_sec, min_qty, min_cash_per_slice
        - 가격은 base_price(하향 라운딩된 주문가)를 기준으로 **하향 라더(0, -1틱, -2틱)** 적용.
        - 중복 주문 방지 및 체결 수량 검증 포함.
        - 반환: (any_success, spent_cash_estimate)
        """
        cfg = (self.trading_params.get("split_buy") or {})
        if not cfg.get("enabled"):
            return (False, 0)

        slices = int(cfg.get("slices", 1))
        weights = list(cfg.get("weights", []))
        ladder = list(cfg.get("ladder_ticks", []))
        iv = float(cfg.get("interval_sec", 0.5))
        jit = float(cfg.get("jitter_sec", 0.1))
        min_qty = int(cfg.get("min_qty", 0))
        min_cash_per_slice = int(cfg.get("min_cash_per_slice", 0))

        # [NEW] 전체 타임박스 및 스텝 최소 지연 적용
        oe = self.trading_params.get("order_execution", {}) or {}
        split_timebox_sec = float(oe.get("split_timebox_sec", 20))
        step_min_delay = float(oe.get("split_step_min_delay_sec", 1.0))

        # 파라미터 검증/정규화
        if slices <= 1 or total_qty < max(1, min_qty):
            return (False, 0)
        if len(weights) != slices or len(ladder) != slices:
            logger.warning("split_buy 파라미터 불일치 → 단일 주문으로 대체")
            return (False, 0)
        s = sum(weights)
        if s <= 0:
            logger.warning("split_buy weights 합계가 0 이하 → 단일 주문으로 대체")
            return (False, 0)
        if abs(s - 1.0) > 1e-6:
            weights = [w / s for w in weights]  # 정규화

        tick = get_tick_size(base_price)
        any_ok = False
        spent_cash = 0
        
        # 분할 매수 추적기 생성
        tracker = self.SplitOrderTracker(ticker, total_qty, slices)

        start_ts = time.time()
        for i in range(slices):
            # 타임박스 초과 시 중단
            if time.time() - start_ts > split_timebox_sec:
                logger.warning(f"[SPLIT] 타임박스 초과({split_timebox_sec}s) → 잔여 슬라이스 중단")
                break
            qty_i = max(0, int(round(total_qty * weights[i])))
            if qty_i == 0:
                continue
            # 하향 라더 가격 (0, -1틱, -2틱 …)
            step = -abs(int(ladder[i]))
            price_i = round_to_tick(base_price + tick * step, mode="down")
            cash_i = qty_i * price_i
            # 최소 슬라이스 금액/최소 주문 금액 체크
            if cash_i < max(self.min_order_cash, min_cash_per_slice):
                logger.info(f"[SPLIT {i+1}/{slices}] 최소 슬라이스 금액 미달 → 스킵 "
                            f"(need>={max(self.min_order_cash, min_cash_per_slice):,}, got={cash_i:,})")
                continue

            # 수수료 버퍼 고려: qty_i 캡
            qty_capped = self._cap_qty_by_fee_buffer(qty_i, price_i, cash_i)
            if qty_capped <= 0:
                logger.info(f"[SPLIT {i+1}/{slices}] 수수료 버퍼로 수량 0 → 스킵")
                continue

            logger.info(f"[SPLIT {i+1}/{slices}] BUY {name}({ticker}) x{qty_capped} @ {price_i:,} [{batch_name}]")
            if self.is_real_trading:
                # 완전한 검증 시스템을 사용한 주문 실행 + 공통 후처리
                context = f"SPLIT {i+1}/{slices}"
                res = self._execute_order_with_full_validation(ticker, name, qty_capped, price_i, context)
                status, executed_qty = self._finalize_buy_result(
                    context=context,
                    name=name,
                    ticker=ticker,
                    requested_qty=qty_capped,
                    price=price_i,
                    res=res,
                    add_pending=True,
                    order_id_key='ODNO'
                )
                if status == 'blocked':
                    tracker.track_split_execution(i+1, qty_capped, 'blocked', price_i)
                    continue
                if status == 'executed':
                    any_ok = True
                    spent_cash += executed_qty * price_i
                    tracker.track_split_execution(i+1, executed_qty, 'executed', price_i)
                elif status == 'pending':
                    any_ok = True  # 주문 제출 성공으로 간주
                    tracker.track_split_execution(i+1, qty_capped, 'submitted', price_i)
                else:
                    # 실패: 세부 처리는 _finalize_buy_result에서 수행됨(기록/쿨다운 등)
                    tracker.track_split_execution(i+1, qty_capped, 'failed', price_i)

                # 인터벌(지터)
                wait = max(step_min_delay, iv + (random.uniform(-jit, jit) if jit > 0 else 0.0))
                time.sleep(wait)
            else:
                any_ok = True
                spent_cash += qty_capped * price_i
                record_trade({
                    "side": "buy", "ticker": ticker, "name": name,
                    "qty": qty_capped, "price": price_i, "trade_status": "completed",
                    "strategy_details": {"batch": f"{batch_name}:SPLIT#{i+1}/{slices}"}
                })
                _notify_text(f" [모의] BUY {name}({ticker}) x{qty_capped} @ {price_i:,} [{batch_name}:SPLIT#{i+1}/{slices}]",
                             key=f"phase:paper_buy:{ticker}:{self.run_id}", cooldown=20)

        if any_ok and self.is_real_trading:
            # 슬라이스 완료 후 1회만 계좌 갱신, 통계 1회만 반영
            self._update_account_info(force=True)
            self.stats["buy"] += 1
        elif any_ok:
            self.stats["buy"] += 1

        # 분할 매수 추적기 요약 출력
        summary = tracker.get_execution_summary()
        logger.info(f"  ->  분할 매수 요약: {name}({ticker}) - "
                   f"체결: {summary['executed_qty']}/{summary['total_qty']}주 "
                   f"({summary['execution_rate']:.1%}), "
                   f"성공: {summary['pending_count']}회, 실패: {summary['failed_count']}회, "
                   f"소요시간: {summary['duration']:.1f}초")

        return (any_ok, spent_cash)

    # ── 단일 매수 실행(회전/특수 케이스용) ────────────────────────────
    def _execute_buy_single(self, stock_info: Dict[str, Any], available_cash: int, batch_name: str = "SINGLE") -> bool:
        name = stock_info.get("Name", "N/A")
        ticker = self._t(stock_info.get("Ticker", ""))
        if not ticker:
            return False

        # 예산 계산 (통일 버퍼 적용)
        buffer = self._eff_buffer()
        budget = int(available_cash * (1 - buffer))
        if budget < 1:
            logger.info(f"[{batch_name}] 예산 부족으로 매수 불가: {name}({ticker}), budget={budget:,}")
            return False

        # 현재가 조회 (실패 시 stock_info 가격 사용)
        current_price = _to_int(stock_info.get("Price", 0))
        if current_price <= 0:
            try:
                price_df = self.kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd=ticker)
                if price_df is not None and not price_df.empty and 'stck_prpr' in price_df.columns:
                    current_price = _to_int(price_df['stck_prpr'].iloc[0])
            except Exception:
                current_price = 0
        if current_price <= 0:
            logger.info(f"[{batch_name}] 현재가 조회 실패: {name}({ticker})")
            return False

        # 지정가 = (현재가에 슬리피지 -bps 적용) → 하향 라운딩
        order_price = self._apply_buy_slippage_and_round(current_price)
        if order_price <= 0:
            logger.info(f"[{batch_name}] 계산된 지정가가 0 이하: {name}({ticker})")
            return False

        # 수수료 버퍼 적용한 실예산
        effective_budget = int(budget * (1 - max(0.0, self.fee_buffer_pct)))
        qty = int(effective_budget // order_price)
        qty = self._cap_qty_by_fee_buffer(qty, order_price, budget)  # 최종 안전 캡

        # 최소 주문 금액 판정은 **금액 기준**으로
        if qty <= 0 or (self.min_order_cash > 0 and (qty * order_price) < self.min_order_cash):
            logger.info(f"[{batch_name}] 예산/최소주문 조건 불충족: {name}({ticker}) qty={qty}, "
                        f"price={order_price:,}, spent={qty*order_price:,}, min_order_cash={self.min_order_cash:,}")
            return False

        # ★ 먼저 분할 매수 시도 (15시 20분 일괄 처리로 변경)
        split_ok, _ = self._place_split_buy(name, ticker, qty, order_price, batch_name)
        if split_ok:
            return True  # 분할 경로에서 미체결 주문 리스트에 추가 완료

        # 분할이 비활성 또는 조건 미충족이면 단일 주문 (완전한 검증 시스템 적용)
        logger.info(f"[{batch_name}] BUY {name}({ticker}) x{qty} @ {order_price:,}")

        if self.is_real_trading:
            # 완전 검증 + 공통 후처리 사용
            res = self._execute_order_with_full_validation(ticker, name, qty, order_price, batch_name)
            status, executed_qty = self._finalize_buy_result(
                context=batch_name,
                name=name,
                ticker=ticker,
                requested_qty=qty,
                price=order_price,
                res=res,
                add_pending=True,
                order_id_key='ODNO'
            )
            if status == 'blocked':
                return False
            if status in ('executed', 'pending'):
                return True
            return False
        else:
            record_trade({
                "side": "buy", "ticker": ticker, "name": name,
                "qty": qty, "price": order_price, "trade_status": "completed",
                "strategy_details": {"batch": batch_name}
            })
            _notify_text(
                f" [모의] BUY {name}({ticker}) x{qty} @ {order_price:,} [{batch_name}]",
                key=f"phase:paper_buy_single:{ticker}:{self.run_id}", cooldown=30
            )
            # ✅ 매수 통계 반영(모의)
            self.stats["buy"] += 1
            return True

    # ── 매도 로직 ────────────────────────────────────────────────────
    def _calculate_sell_price_with_slippage(self, current_price: int, slippage_bps: int = 50) -> int:
        """매도 가격에 슬리피지 적용 (기본 50bps = 0.5%)"""
        if current_price <= 0:
            return 0
        
        # 슬리피지 적용 (매도는 가격을 낮춤)
        slippage_factor = 1.0 - (slippage_bps / 10000.0)
        target_price = current_price * slippage_factor
        
        # 호가 단위로 라운딩
        tick_size = get_tick_size(current_price)
        return round_to_tick(target_price, mode="down")
    
    def _determine_sell_order_type(self, current_price: int, structured_context: Optional[Dict] = None) -> Tuple[str, int]:
        """
        매도 주문 타입 결정 (시장가 vs 지정가)
        급락 상황에서는 시장가, 일반적인 경우 지정가 사용
        """
        # 급락 감지 (5% 이상 하락 시 시장가)
        if structured_context and structured_context.get("type") == "StopLoss":
            # 손절 상황에서는 시장가 사용
            return "02", 0  # 시장가
        
        # 일반적인 매도는 지정가 사용 (슬리피지 적용)
        sell_price = self._calculate_sell_price_with_slippage(current_price, slippage_bps=30)
        return "00", sell_price  # 지정가

    def _parse_reason_code(self, reason: str, structured_context: Optional[Dict] = None) -> str:
        # 구조화된 컨텍스트가 있으면 우선 사용
        if structured_context and isinstance(structured_context, dict):
            context_type = structured_context.get("type", "")
            if context_type == "StopLoss":
                return "STOP_LOSS_HIT"
            elif context_type == "TakeProfit":
                return "TAKE_PROFIT_HIT"
            elif context_type == "RSI_OVERBOUGHT":
                return "RSI_OVERBOUGHT"
            elif context_type == "MaxHoldingDays":
                return "MAX_HOLDING_DAYS"
            elif context_type == "PrevCloseBreak":
                return "PREV_CLOSE_BREAK"
            elif context_type == "KEEP":
                return "HOLD"
            elif context_type == "AdvancedStrategy":
                return "ADVANCED_STRATEGY"
            elif context_type == "AdvancedStrategyError":
                return "ADVANCED_STRATEGY_ERROR"
        
        # 하위 호환: 기존 문자열 파싱
        if "전략=StopLoss" in reason or "손절가 도달" in reason:
            return "STOP_LOSS_HIT"
        if "전략=TakeProfit" in reason or "목표가 도달" in reason:
            return "TAKE_PROFIT_HIT"
        if "전략=RSI_TP" in reason or "RSI 과열" in reason:
            return "RSI_OVERBOUGHT"
        if "전략=MaxHoldingDays" in reason or "보유일수 초과" in reason:
            return "MAX_HOLDING_DAYS"
        if "전략=PrevCloseBreak" in reason or "전일 종가 이탈" in reason:
            return "PREV_CLOSE_BREAK"
        if reason.startswith("유지"):
            return "HOLD"
        return "UNKNOWN"
    def run_sell_logic(self, holdings: List[Dict]) -> bool:
        logger.info(f"--------- 보유 종목 {len(holdings)}개 매도 로직 실행 ---------")

        executed_sell = False
        if not holdings:
            logger.info("매도할 보유 종목이 없습니다.")
            return False

        #  포트폴리오 비중 초과 시 자동 리밸런싱 (최우선 실행)
        if holdings:
            logger.info("포트폴리오 비중 리밸런싱 체크 시작...")
            rebalanced_holdings = self._check_and_rebalance_portfolio(holdings)
            if rebalanced_holdings is not None:
                # 리밸런싱이 발생한 경우 계좌 정보 갱신
                self._update_account_info(force=True)
                cash_after_rebalance, holdings_after_rebalance, _ = self._load_snapshot()
                holdings = holdings_after_rebalance  # 갱신된 보유 종목으로 교체
                logger.info(f"리밸런싱 완료. 현재 보유 종목: {len(holdings)}개")
                executed_sell = True

        # 통합된 시간대 체크
        if not self._check_trading_hours("sell"):
            return

        holding_tickers = [self._t(h.get("pdno", "")) for h in holdings if _to_int(h.get("hldg_qty", 0)) > 0]
        last_buy_trades = fetch_trades_by_tickers(holding_tickers)

        for holding in holdings:
            ticker = self._t(holding.get("pdno", ""))
            name = holding.get("prdt_name", "N/A")
            quantity = _to_int(holding.get("hldg_qty", 0))
            if not ticker or quantity <= 0:
                continue

            stock_info = self.all_stock_data.get(ticker, {})
            for k, dv in SCHEMA_DEFAULTS.items():
                stock_info.setdefault(k, dv)

            # 실시간 레벨/RSI/Price 오버레이
            try:
                current_price = 0
                try:
                    price_df = self.kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd=ticker)
                    if price_df is not None and not price_df.empty and 'stck_prpr' in price_df.columns:
                        current_price = _to_int(price_df['stck_prpr'].iloc[0])
                except Exception:
                    current_price = 0
                if current_price <= 0:
                    current_price = _to_int(stock_info.get("Price", 0))
                if current_price > 0:
                    stock_info["Price"] = current_price

                rt_levels = self.risk_manager.compute_realtime_levels(ticker, current_price) or {}
                if "RSI" in rt_levels and rt_levels["RSI"] is not None:
                    stock_info["RSI"] = float(rt_levels["RSI"])
                if "손절가" in rt_levels and rt_levels["손절가"] is not None:
                    stock_info["손절가"] = _to_int(rt_levels["손절가"])
                if "목표가" in rt_levels and rt_levels["목표가"] is not None:
                    stock_info["목표가"] = _to_int(rt_levels["목표가"])
                if "Price" in rt_levels and rt_levels["Price"] is not None:
                    stock_info["Price"] = _to_int(rt_levels["Price"])
                if "source" in rt_levels and rt_levels["source"]:
                    stock_info["source"] = rt_levels["source"]
            except Exception as e:
                logger.debug(f"[{ticker}] 실시간 레벨 오버레이 실패: {e}")

            decision, reason, structured_context = self.risk_manager.check_sell_condition(holding, stock_info)
            
            # reason_code는 모든 결정에 대해 먼저 파싱
            reason_code = self._parse_reason_code(reason, structured_context)
            
            # Phase 2: 부분 익절 처리
            if decision == "PARTIAL_SELL":
                # Phase 1.4: 당일 매도 방지 체크 (부분 익절도 매도이므로 체크 필요)
                trading_params = self.settings.get("trading_params", {})
                min_holding_hours = trading_params.get("min_holding_hours", 0)
                if min_holding_hours > 0:
                    is_eligible, holding_hours = check_min_holding_hours(ticker, min_holding_hours)
                    if not is_eligible:
                        logger.info(
                            f"[{ticker}] ⚠️ 부분 익절 당일 매도 방지: 보유시간 {holding_hours:.1f}시간 < 최소 {min_holding_hours}시간 → 유지"
                        )
                        self.stats["hold"] += 1
                        continue
                
                partial_ratio = structured_context.get("context", {}).get("partial_profit_ratio", 0.5) if structured_context else 0.5
                partial_qty = int(quantity * partial_ratio)
                
                if partial_qty <= 0:
                    logger.warning(f"⚠️ 부분 익절 수량 계산 오류: {name}({ticker}) 수량={quantity}, 비율={partial_ratio:.0%}, 계산된 수량={partial_qty}")
                    self.stats["hold"] += 1
                    continue
                
                if not self.is_real_trading:
                    logger.info(f"ℹ️ 부분 익절 조건 충족했으나 실거래 모드 아님: {name}({ticker}) {partial_qty}주 ({partial_ratio:.0%}) - 시뮬레이션 모드")
                    # 부분익절 후 재매수(재진입/추가매수) 차단: 모의환경도 동일하게 처리
                    try:
                        self._mark_partial_sell(ticker)
                        self._add_to_cooldown_for_days(
                            ticker,
                            self.post_partial_sell_buy_cooldown_days,
                            reason=f"부분익절 후 매수 차단({self.post_partial_sell_buy_cooldown_days}d)"
                        )
                    except Exception as e:
                        logger.debug(f"[{ticker}] 부분익절 쿨다운 등록 실패(모의): {e}")
                    self.stats["sell"] += 1
                    executed_sell = True
                    continue
                
                logger.info(f"부분 익절 실행: {name}({ticker}) {partial_qty}주 ({partial_ratio:.0%})")
                order_type, order_price = self._determine_sell_order_type(current_price, structured_context)
                result = self._order_cash_retry(
                    ord_dv="01", pdno=ticker, ord_dvsn=order_type, ord_qty=str(partial_qty), ord_unpr=str(order_price)
                )
                if result:
                    self.stats["sell"] += 1
                    executed_sell = True
                    logger.info(f"✅ 부분 익절 주문 성공: {name}({ticker}) {partial_qty}주")
                    # 부분익절 후 재매수(재진입/추가매수) 차단
                    try:
                        self._mark_partial_sell(ticker)
                        self._add_to_cooldown_for_days(
                            ticker,
                            self.post_partial_sell_buy_cooldown_days,
                            reason=f"부분익절 후 매수 차단({self.post_partial_sell_buy_cooldown_days}d)"
                        )
                    except Exception as e:
                        logger.debug(f"[{ticker}] 부분익절 쿨다운 등록 실패: {e}")
                else:
                    logger.warning(f"❌ 부분 익절 주문 실패: {name}({ticker})")
                continue
            
            if decision != "SELL":
                logger.info(f"유지 판단: {reason}")
                self.stats["hold"] += 1
                continue

            # Phase 1.4: 당일 매도 방지 체크
            trading_params = self.settings.get("trading_params", {})
            min_holding_hours = trading_params.get("min_holding_hours", 0)
            if min_holding_hours > 0:
                is_eligible, holding_hours = check_min_holding_hours(ticker, min_holding_hours)
                if not is_eligible:
                    logger.info(
                        f"[{ticker}] ⚠️ 당일 매도 방지: 보유시간 {holding_hours:.1f}시간 < 최소 {min_holding_hours}시간 → 유지"
                    )
                    self.stats["hold"] += 1
                    continue
            
            # 손절가 도달 시 상세 정보 로깅
            if structured_context and structured_context.get("type") == "StopLoss":
                context_info = structured_context.get("context", {})
                stop_price = context_info.get("stop_price")
                stop_threshold = context_info.get("stop_threshold")
                avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
                if avg_price > 0 and current_price > 0:
                    loss_pct = ((current_price - avg_price) / avg_price) * 100
                    logger.warning(
                        f"⚠️ 손절가 도달: {name}({ticker}) | "
                        f"현재가={current_price:,}원, 매수가={avg_price:,.0f}원, 손실률={loss_pct:.2f}% | "
                        f"손절가={stop_price:,}원, 손절임계값={stop_threshold:,}원"
                    )
                else:
                    logger.warning(
                        f"⚠️ 손절가 도달: {name}({ticker}) | "
                        f"현재가={current_price:,}원 | 손절가={stop_price:,}원, 손절임계값={stop_threshold:,}원"
                    )
            else:
                logger.info(f"매도 결정: {name}({ticker}) {quantity}주. 사유: {reason} | code={reason_code}")
            
            executed_sell = True

            if self.is_real_trading:
                pre_qty = self._get_qty(holdings, ticker)

                # 매도 주문 타입 결정 (시장가 vs 지정가)
                order_type, order_price = self._determine_sell_order_type(current_price, structured_context)
                
                result = self._order_cash_retry(
                    ord_dv="01", pdno=ticker, ord_dvsn=order_type, ord_qty=str(quantity), ord_unpr=str(order_price)
                )

                # 향상된 매도 체결 확인 로직 적용
                execution_result = self._enhanced_execution_check(ticker, result, pre_qty)
                if execution_result.get('executed'):
                    filled_qty = execution_result.get('executed_qty', pre_qty)
                elif execution_result.get('status') == 'partial':
                    filled_qty = execution_result.get('executed_qty', 0)
                else:
                    # 타임아웃 시 수동 확인
                    self._update_account_info(force=True)
                    _, holdings_after, _ = self._load_snapshot()
                    post_qty = self._get_qty(holdings_after, ticker)
                    filled_qty = max(0, pre_qty - post_qty)

                try:
                    price_df = self.kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd=ticker)
                    current_price = _to_int(price_df['stck_prpr'].iloc[0]) if (price_df is not None and not price_df.empty) else 0
                except Exception:
                    current_price = 0
                
                # 가격 조회 실패 시 보유 평균가 사용 (최후의 수단)
                if current_price <= 0:
                    avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
                    if avg_price > 0:
                        current_price = int(avg_price)
                        logger.warning(f"⚠️ [{ticker}] 가격 조회 실패, 보유 평균가 사용: {current_price:,}원")
                    else:
                        logger.error(f"❌ [{ticker}] 가격 정보 없음 (API 조회 실패, 평균가도 없음)")

                parent_trade_id = None
                pnl_amount = None
                ticker_trades = last_buy_trades.get(ticker, [])
                if ticker_trades and filled_qty > 0:
                    # 매수 거래만 필터링하고 최신 거래 찾기
                    buy_trades = [t for t in ticker_trades if t.get('action') == 'buy']
                    if buy_trades:
                        last_buy = buy_trades[-1]  # 가장 최근 매수 거래
                        parent_trade_id = last_buy.get('id')
                        buy_price = _to_int(last_buy.get('price', holding.get('pchs_avg_pric', 0)))
                        if buy_price and current_price:
                            pnl_amount = (current_price - buy_price) * filled_qty

                if result.get('ok') or filled_qty > 0:
                    trade_status = "completed" if filled_qty > 0 else "submitted"
                    
                    # structured_context의 상세 정보를 strategy_details에 포함
                    strategy_details = {
                        "reason": reason,
                        "reason_code": reason_code,
                        "broker_msg": result.get('msg1')
                    }
                    if structured_context:
                        strategy_details["sell_type"] = structured_context.get("type")
                        context_info = structured_context.get("context", {})
                        if context_info:
                            # 손절가 관련 정보 추가
                            if "stop_price" in context_info:
                                strategy_details["stop_price"] = context_info["stop_price"]
                                strategy_details["stop_threshold"] = context_info.get("stop_threshold")
                                strategy_details["stop_loss_buffer"] = context_info.get("stop_loss_buffer")
                            if "levels_source" in context_info:
                                strategy_details["levels_source"] = context_info["levels_source"]
                            if "time_window" in context_info:
                                strategy_details["time_window"] = context_info["time_window"]
                    
                    record_trade({
                        "side": "sell", "ticker": ticker, "name": name,
                        "qty": filled_qty if filled_qty > 0 else quantity,
                        "price": current_price,
                        "trade_status": trade_status,
                        "order_id": result.get("ODNO") or result.get("odno") or result.get("order_id"),
                        "requested_qty": quantity,
                        "executed_qty": filled_qty if filled_qty > 0 else 0,
                        "reason_code": reason_code,
                        "structured_context": structured_context or {},
                        "strategy_details": strategy_details,
                        "parent_trade_id": parent_trade_id,
                        "pnl_amount": pnl_amount,
                        "sell_reason": reason
                    })
                    _notify_embed(create_trade_embed({
                        "side": "SELL", "name": name, "ticker": ticker,
                        "qty": filled_qty if filled_qty > 0 else quantity,
                        "price": current_price, "trade_status": trade_status,
                        "strategy_details": strategy_details
                    }), key=f"phase:sell:{ticker}:{self.run_id}", cooldown=30)

                    # ✅ 매도 통계 반영
                    self.stats["sell"] += 1

                    # 성공/체결확인 → 실패카운트 리셋
                    self._maybe_add_cooldown(ticker, "매도 주문 실패", increment_fail=False)

                    # 부분익절 이력이 있었고 이번 매도가 전량(포지션 0)에 해당하면,
                    # 재진입 차단을 위해 쿨다운을 1일로 갱신한 뒤 flag를 정리한다.
                    try:
                        full_exit = (filled_qty > 0 and filled_qty >= pre_qty)
                        if full_exit and self._had_partial_sell(ticker):
                            self._add_to_cooldown_for_days(
                                ticker,
                                self.post_partial_sell_buy_cooldown_days,
                                reason=f"부분익절 후 전량매도 → 재진입 차단({self.post_partial_sell_buy_cooldown_days}d)"
                            )
                            self._clear_partial_sell_flag(ticker)
                    except Exception as e:
                        logger.debug(f"[{ticker}] 전량매도 후 쿨다운 갱신 실패: {e}")

                    if filled_qty == 0:
                        logger.info("  -> 응답은 성공이나 즉시 체결 없음(미체결 가능). submitted로 기록.")
                else:
                    err = result.get('msg1', 'Unknown error')
                    record_trade({
                        "side": "sell", "ticker": ticker, "name": name,
                        "qty": quantity, "price": current_price,
                        "trade_status": "failed",
                        "order_id": result.get("ODNO") or result.get("odno") or result.get("order_id"),
                        "requested_qty": quantity,
                        "executed_qty": 0,
                        "reason_code": reason_code,
                        "structured_context": structured_context or {},
                        "strategy_details": {
                            "error": err,
                            "rt_cd": result.get('rt_cd'),
                            "msg_cd": result.get('msg_cd'),
                            "reason": reason,
                            "reason_code": reason_code
                        },
                        "sell_reason": reason
                    })
                    _notify_embed(create_trade_embed({
                        "side": "SELL", "name": name, "ticker": ticker,
                        "qty": quantity, "price": current_price, "trade_status": "failed",
                        "strategy_details": {"error": err, "reason_code": reason_code}
                    }), key=f"phase:sell_fail:{ticker}:{self.run_id}", cooldown=30)
                    # 연속 실패 누적 → 기준 도달 시에만 쿨독
                    self._maybe_add_cooldown(ticker, "매도 주문 실패", increment_fail=True)

            else:
                logger.info(f"[모의] {name}({ticker}) {quantity}주 시장가 매도 실행.")
                record_trade({
                    "side": "sell", "ticker": ticker, "name": name,
                    "qty": quantity, "price": 0, "trade_status": "completed",
                    "order_id": "",
                    "requested_qty": quantity,
                    "executed_qty": quantity,
                    "reason_code": reason_code,
                    "strategy_details": {"reason": reason, "reason_code": reason_code},
                    "sell_reason": reason
                })
                _notify_text(
                    f" [모의] SELL {name}({ticker}) x{quantity} | {reason_code}",
                    key=f"phase:paper_sell:{ticker}:{self.run_id}", cooldown=30
                )
                # ✅ 매도 통계 반영(모의)
                self.stats["sell"] += 1
                self._add_to_cooldown(ticker, "모의 매도 완료")
                # 모의환경: 부분익절 이력이 있었다면 전량매도 후에도 동일하게 재진입 차단 갱신
                try:
                    if self._had_partial_sell(ticker):
                        self._add_to_cooldown_for_days(
                            ticker,
                            self.post_partial_sell_buy_cooldown_days,
                            reason=f"부분익절 후 전량매도(모의) → 재진입 차단({self.post_partial_sell_buy_cooldown_days}d)"
                        )
                        self._clear_partial_sell_flag(ticker)
                except Exception as e:
                    logger.debug(f"[{ticker}] 전량매도 후 쿨다운 갱신 실패(모의): {e}")

        if executed_sell:
            # 매도 후 계좌 정보 갱신 (배치 처리)
            self._batch_update_account_info()
        
        return executed_sell

    # ── (도우미) 현금 기반 슬롯 계산 ──────────────────────────────────
    def _compute_effective_slots(self, cash: int) -> int:
        """
        auto_shrink_slots 구성에 따라 현금으로 가능한 슬롯 수를 계산.
        - 기본: max_positions
        - auto_shrink_slots=True 이고 min_order_cash>0 인 경우: cash // min_order_cash 와의 최솟값
        """
        eff_slots_cap = int(self.trading_params.get("max_positions", self.max_positions))
        if self.trading_guards.get("auto_shrink_slots", False) and self.min_order_cash > 0:
            eff_slots_by_cash = max(cash // self.min_order_cash, 0)
            return min(eff_slots_cap, eff_slots_by_cash)
        return eff_slots_cap

    # ── 동적 현금 관리 시스템 ───────────────────────────────────────────
    def _calculate_optimal_cash_ratio(self, market_regime: str = None, volatility: float = None) -> float:
        """
        시장 상황에 따른 최적 현금 비율 계산
        """
        if not self.dynamic_cash_enabled:
            return self.cash_buffer_ratio
        
        # 시장 상황별 기본 현금 비율
        base_ratios = {
            "bull_market": 0.15,
            "sideways_market": 0.20, 
            "bear_market": 0.35,
            "volatile_market": 0.30
        }
        
        # 시장 상황별 조정값 적용
        if market_regime and market_regime in self.market_regime_adjustment:
            target_ratio = self.market_regime_adjustment[market_regime]
        else:
            target_ratio = base_ratios.get(market_regime, 0.20)
        
        # 변동성 기반 추가 조정
        if volatility and volatility > self.volatility_threshold:
            target_ratio = min(0.40, target_ratio + 0.10)  # 고변동성 시 현금 비율 증가
        
        return max(0.10, min(0.50, target_ratio))  # 10-50% 범위로 제한
    
    def _should_rebalance_cash(self) -> bool:
        """
        현금 리밸런싱 필요 여부 판단
        """
        if not self.dynamic_cash_enabled:
            return False
        
        import time
        current_time = time.time()
        hours_since_last = (current_time - self._last_cash_rebalance) / 3600
        
        return hours_since_last >= self.rebalance_frequency_hours
    
    def _apply_dynamic_cash_management(self, available_cash: int, total_value: int) -> int:
        """
        동적 현금 관리 적용
        """
        if not self.dynamic_cash_enabled or not self._should_rebalance_cash():
            return available_cash
        
        # 시장 상황 분석: 스크리너 market_state 우선, 없으면 폴백(sideways)
        market_regime = None
        try:
            ms = getattr(self, "market_state_from_screener", None)
            regime = str(ms.get("regime") or "").strip() if isinstance(ms, dict) else ""
            if regime:
                market_regime = f"{regime}_market"
        except Exception:
            market_regime = None
        if not market_regime:
            market_regime = self._detect_market_regime()
        volatility = self._estimate_portfolio_volatility()
        
        # 최적 현금 비율 계산
        optimal_cash_ratio = self._calculate_optimal_cash_ratio(market_regime, volatility)
        target_cash = int(total_value * optimal_cash_ratio)
        
        # 현재 현금 비율
        current_cash_ratio = available_cash / total_value if total_value > 0 else 0
        
        # 조정 필요성 판단 (5% 이상 차이 시)
        if abs(current_cash_ratio - optimal_cash_ratio) > 0.05:
            logger.info(f"동적 현금 관리: {current_cash_ratio:.1%} → {optimal_cash_ratio:.1%} (시장: {market_regime}, 변동성: {volatility:.2f})")
            
            # 리밸런싱 시간 업데이트
            import time
            self._last_cash_rebalance = time.time()
            
            return target_cash
        
        return available_cash
    
    def _detect_market_regime(self) -> str:
        """
        시장 상황 감지 (간단한 휴리스틱)
        """
        try:
            # 최근 5일간 KOSPI 수익률 기반 판단
            from datetime import datetime, timedelta
            import pandas as pd
            
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            
            # KOSPI 데이터 조회 (간단한 구현)
            # 실제로는 더 정교한 시장 분석이 필요
            return "sideways_market"  # 기본값
            
        except Exception as e:
            logger.debug(f"시장 상황 감지 실패: {e}")
            return "sideways_market"
    
    def _estimate_portfolio_volatility(self) -> float:
        """
        포트폴리오 변동성 추정
        """
        try:
            # 보유 종목들의 변동성 추정
            # 실제로는 더 정교한 계산이 필요
            return 0.20  # 기본값 (20%)
            
        except Exception as e:
            logger.debug(f"변동성 추정 실패: {e}")
            return 0.20

    # ── 포트폴리오 집중도 모니터링 ───────────────────────────────────────
    def _check_portfolio_concentration(self, holdings: List[Dict], available_cash: int) -> Dict[str, Any]:
        """
        포트폴리오 집중도를 체크하고 리스크 경고를 제공.
        """
        if not holdings:
            return {"concentration_risk": "low", "max_weight": 0.0, "warnings": []}
        
        # 포트폴리오 총 가치 계산
        total_value = sum(_to_int(h.get("evlu_amt", 0)) for h in holdings if _to_int(h.get("hldg_qty", 0)) > 0)
        total_value += available_cash
        
        if total_value <= 0:
            return {"concentration_risk": "unknown", "max_weight": 0.0, "warnings": ["총 가치 계산 불가"]}
        
        # 종목별 비중 계산
        ticker_weights = {}
        for h in holdings:
            if _to_int(h.get("hldg_qty", 0)) > 0:
                ticker = self._t(h.get("pdno", ""))
                value = _to_int(h.get("evlu_amt", 0))
                weight = value / total_value
                ticker_weights[ticker] = weight
        
        # 최대 비중과 경고 생성
        max_weight = max(ticker_weights.values()) if ticker_weights else 0.0
        warnings = []
        
        if max_weight > self.per_ticker_max_weight:
            warnings.append(f"종목 비중 초과: {max_weight:.1%} > {self.per_ticker_max_weight:.1%}")
        
        if max_weight > 0.20:  # 20% 초과 시 추가 경고
            warnings.append(f"고집중 포트폴리오: {max_weight:.1%}")
        
        # 집중도 리스크 레벨 결정
        if max_weight > 0.25:
            risk_level = "high"
        elif max_weight > 0.15:
            risk_level = "medium"
        else:
            risk_level = "low"
        
        # 섹터 집중도 체크 추가
        sector_concentration = self._check_sector_concentration(holdings, total_value)
        warnings.extend(sector_concentration.get("warnings", []))
        
        # 섹터 집중도가 높으면 리스크 레벨 상향 조정
        if sector_concentration.get("max_sector_weight", 0) > 0.30:  # 30% 초과
            if risk_level == "low":
                risk_level = "medium"
            elif risk_level == "medium":
                risk_level = "high"

        return {
            "concentration_risk": risk_level,
            "max_weight": max_weight,
            "ticker_weights": ticker_weights,
            "warnings": warnings,
            "total_value": total_value,
            "sector_concentration": sector_concentration
        }

    def _check_sector_concentration(self, holdings: List[Dict], total_value: int) -> Dict[str, Any]:
        """섹터별 집중도 체크"""
        if not holdings or total_value <= 0:
            return {"max_sector_weight": 0.0, "sector_weights": {}, "warnings": []}
        
        # 섹터별 가치 계산
        sector_values = {}
        for h in holdings:
            if _to_int(h.get("hldg_qty", 0)) > 0:
                sector = h.get("sector", "Unknown")
                value = _to_int(h.get("evlu_amt", 0))
                sector_values[sector] = sector_values.get(sector, 0) + value
        
        # 섹터별 비중 계산
        sector_weights = {sector: value / total_value for sector, value in sector_values.items()}
        max_sector_weight = max(sector_weights.values()) if sector_weights else 0.0
        
        warnings = []
        if max_sector_weight > 0.30:  # 30% 초과
            warnings.append(f"섹터 집중도 높음: {max_sector_weight:.1%}")
        elif max_sector_weight > 0.20:  # 20% 초과
            warnings.append(f"섹터 집중도 주의: {max_sector_weight:.1%}")
        
        # 상위 섹터들 로깅
        sorted_sectors = sorted(sector_weights.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_sectors) > 1:
            logger.info(f"섹터 분포: {', '.join([f'{sector} {weight:.1%}' for sector, weight in sorted_sectors[:3]])}")
        
        return {
            "max_sector_weight": max_sector_weight,
            "sector_weights": sector_weights,
            "warnings": warnings
        }

    def _check_trading_hours(self, operation: str) -> bool:
        """통합된 시간대 체크"""
        now_kst = datetime.now(KST)
        
        if operation == "sell":
            time_windows = self.sell_time_windows
            operation_name = "매도"
        elif operation == "buy":
            time_windows = self.buy_time_windows
            operation_name = "매수"
        else:
            logger.warning(f"알 수 없는 거래 유형: {operation}")
            return False
        
        if not in_time_windows(now_kst, time_windows):
            logger.info(f"현재 시간 {now_kst.strftime('%H:%M')}은 {operation_name} 시간대가 아닙니다. {operation}_time_windows={time_windows}")
            if operation == "buy":
                _notify_text(f"ℹ️ {operation_name} 시간대 외 → {operation_name} 스킵",
                             key=f"phase:{operation}_out_of_window:{self.run_id}", cooldown=300)
            return False
        
        return True

    # ── 포트폴리오 리밸런싱 로직 ─────────────────────────────────────────
    def _check_and_rebalance_portfolio(self, holdings: List[Dict]) -> Optional[List[Dict]]:
        """
        포트폴리오 비중 초과 시 자동 리밸런싱
        Returns: 리밸런싱이 발생한 경우 갱신된 holdings, 없으면 None
        """
        try:
            # 현재 계좌 정보 로드
            cash, _, _ = self._load_snapshot()
            total_value = sum(_to_int(h.get("evlu_amt", 0)) for h in holdings if _to_int(h.get("hldg_qty", 0)) > 0)
            total_value += cash
            
            if total_value <= 0:
                logger.warning("총 포트폴리오 가치 계산 불가로 리밸런싱을 건너뜁니다.")
                return None
            
            # 포트폴리오 집중도 체크
            concentration_check = self._check_portfolio_concentration(holdings, cash)
            max_weight = concentration_check['max_weight']
            ticker_weights = concentration_check['ticker_weights']
            
            # 비중 초과 종목 식별
            overweight_tickers = [
                ticker for ticker, weight in ticker_weights.items()
                if weight > self.per_ticker_max_weight
            ]
            
            if not overweight_tickers:
                logger.info("포트폴리오 비중이 정상 범위 내입니다. 리밸런싱 불필요.")
                return None
            
            logger.warning(f"포트폴리오 비중 초과 종목 발견: {overweight_tickers}")
            logger.info(f"최대 비중: {max_weight:.1%} > 제한: {self.per_ticker_max_weight:.1%}")
            
            # 비중 초과 종목들을 목표 비중까지 매도
            rebalanced_count = 0
            for ticker in overweight_tickers:
                current_weight = ticker_weights[ticker]
                if self._reduce_position_to_target_weight(ticker, self.per_ticker_max_weight, total_value, holdings):
                    rebalanced_count += 1
                    logger.info(f"✅ {ticker} 비중 조정 완료: {current_weight:.1%} → {self.per_ticker_max_weight:.1%}")
                else:
                    logger.warning(f"❌ {ticker} 비중 조정 실패")
            
            if rebalanced_count > 0:
                logger.info(f"포트폴리오 리밸런싱 완료: {rebalanced_count}개 종목 조정")
                _notify_text(f" 포트폴리오 리밸런싱: {rebalanced_count}개 종목 비중 조정", 
                           key=f"rebalance:{self.run_id}", cooldown=300)
                return []  # 리밸런싱 발생 신호
            else:
                logger.warning("포트폴리오 리밸런싱 시도했으나 성공한 종목이 없습니다.")
                return None
                
        except Exception as e:
            logger.error(f"포트폴리오 리밸런싱 중 오류: {e}", exc_info=True)
            _notify_text(f"❌ 포트폴리오 리밸런싱 오류: {e}", 
                       key=f"rebalance_error:{self.run_id}", cooldown=300)
            return None

    def _reduce_position_to_target_weight(self, ticker: str, target_weight: float, total_value: float, holdings: List[Dict]) -> bool:
        """
        특정 종목을 목표 비중까지 매도
        Returns: 매도 성공 여부
        """
        try:
            # 현재 보유 정보 찾기
            holding = None
            for h in holdings:
                if self._t(h.get("pdno", "")) == ticker:
                    holding = h
                    break
            
            if not holding:
                logger.warning(f"종목 {ticker}의 보유 정보를 찾을 수 없습니다.")
                return False
            
            current_qty = _to_int(holding.get("hldg_qty", 0))
            current_price = _to_int(holding.get("prpr", 0))
            
            if current_qty <= 0 or current_price <= 0:
                logger.warning(f"종목 {ticker}의 수량 또는 가격 정보가 유효하지 않습니다.")
                return False
            
            # 현재 비중과 목표 비중 계산
            current_value = current_qty * current_price
            current_weight = current_value / total_value
            target_value = total_value * target_weight
            
            if current_weight <= target_weight:
                logger.info(f"종목 {ticker}의 현재 비중({current_weight:.1%})이 목표 비중({target_weight:.1%}) 이하입니다.")
                return True
            
            # 매도할 수량 계산
            excess_value = current_value - target_value
            sell_qty = int(excess_value / current_price)
            
            if sell_qty <= 0:
                logger.info(f"종목 {ticker}의 매도 수량이 0 이하입니다.")
                return True
            
            # 최소 매도 수량 체크 (1주 이상)
            if sell_qty < 1:
                sell_qty = 1
            
            # 전체 수량의 90% 이상 매도 방지 (완전 매도 방지)
            max_sell_qty = int(current_qty * 0.9)
            sell_qty = min(sell_qty, max_sell_qty)
            
            if sell_qty <= 0:
                logger.info(f"종목 {ticker}의 안전 매도 수량이 0 이하입니다.")
                return True
            
            logger.info(f"종목 {ticker} 비중 조정 매도: {sell_qty}주 (현재: {current_qty}주, 비중: {current_weight:.1%} → {target_weight:.1%})")
            
            # 매도 주문 실행
            if self.is_real_trading:
                result = self._order_cash_retry(
                    ord_dv="01",  # 매도
                    pdno=ticker, 
                    ord_dvsn="01",  # 시장가
                    ord_qty=str(sell_qty), 
                    ord_unpr="0"  # 시장가는 가격 0
                )
                
                # 향상된 체결 확인 로직 적용
                execution_result = self._enhanced_execution_check(ticker, result, sell_qty)
                
                if execution_result.get('executed'):
                    logger.info(f"✅ {ticker} 비중 조정 매도 성공: {sell_qty}주 (확인방식: {execution_result.get('status', 'unknown')})")
                    _notify_text(f" {ticker} 비중 조정 매도: {sell_qty}주", 
                               key=f"rebalance_sell:{ticker}:{self.run_id}", cooldown=300)
                    return True
                else:
                    logger.error(f"❌ {ticker} 비중 조정 매도 실패: {result}")
                    return False
            else:
                logger.info(f"[SIMULATION] {ticker} 비중 조정 매도 시뮬레이션: {sell_qty}주")
                return True
                
        except Exception as e:
            logger.error(f"종목 {ticker} 비중 조정 중 오류: {e}", exc_info=True)
            return False

    # ── 에러 처리 및 로깅 시스템 강화 ───────────────────────────────────
    def _log_order_execution(self, ticker: str, name: str, result: Dict[str, Any], order_type: str = "limit"):
        """주문 실행 결과 상세 로깅"""
        status = result.get('status', 'unknown')
        executed_qty = result.get('executed_qty', 0)
        wait_time = result.get('wait_time', 0)
        error_msg = result.get('error', '')
        
        log_data = {
            'ticker': ticker,
            'name': name,
            'order_type': order_type,
            'status': status,
            'executed_qty': executed_qty,
            'wait_time': wait_time,
            'error': error_msg,
            'timestamp': datetime.now(KST).isoformat()
        }
        
        if status == 'executed':
            logger.info(f"✅ 주문 성공: {name}({ticker}) - {order_type} 주문, "
                       f"체결수량: {executed_qty}주, 대기시간: {wait_time:.1f}초")
        elif status == 'partial':
            logger.warning(f"⚠️ 부분 체결: {name}({ticker}) - {order_type} 주문, "
                          f"체결수량: {executed_qty}주, 대기시간: {wait_time:.1f}초")
        elif status == 'timeout':
            logger.error(f"❌ 체결 타임아웃: {name}({ticker}) - {order_type} 주문, "
                        f"대기시간: {wait_time:.1f}초")
        else:
            logger.error(f"❌ 주문 실패: {name}({ticker}) - {order_type} 주문, "
                        f"오류: {error_msg}")
        
        # 상세 로그를 파일에 저장 (선택적)
        self._save_detailed_log(log_data)

    def _save_detailed_log(self, log_data: Dict[str, Any]):
        """상세 로그를 파일에 저장"""
        try:
            log_file = OUTPUT_DIR / "detailed_trading_log.json"
            log_file.parent.mkdir(parents=True, exist_ok=True)
            
            # 기존 로그 로드
            if log_file.exists():
                with open(log_file, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
            else:
                logs = []
            
            # 새 로그 추가
            logs.append(log_data)
            
            # 최근 1000개만 유지
            if len(logs) > 1000:
                logs = logs[-1000:]
            
            # 파일에 저장
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"상세 로그 저장 실패: {e}")
    def _handle_order_error(self, ticker: str, name: str, error: str, context: str = ""):
        """주문 에러 통합 처리"""
        error_context = f"{context}: " if context else ""
        logger.error(f"주문 에러 {error_context}{name}({ticker}): {error}")
        
        # 에러 타입별 처리
        if "토큰" in error or "인증" in error:
            logger.warning("토큰 관련 에러 감지, 재인증 시도")
            try:
                self.kis.reauthenticate()
            except Exception as e:
                logger.error(f"재인증 실패: {e}")
        elif "가격" in error or "호가" in error:
            logger.warning("가격/호가 관련 에러 감지")
        elif "수량" in error:
            logger.warning("수량 관련 에러 감지")
        else:
            logger.warning("기타 주문 에러")
        
        # 에러 통계 업데이트
        if not hasattr(self, '_error_stats'):
            self._error_stats = {}
        
        error_type = self._categorize_error(error)
        self._error_stats[error_type] = self._error_stats.get(error_type, 0) + 1

    def _categorize_error(self, error: str) -> str:
        """에러를 카테고리별로 분류"""
        error_lower = error.lower()
        
        if any(keyword in error_lower for keyword in ["토큰", "인증", "auth", "token"]):
            return "AUTH_ERROR"
        elif any(keyword in error_lower for keyword in ["가격", "호가", "price", "tick"]):
            return "PRICE_ERROR"
        elif any(keyword in error_lower for keyword in ["수량", "quantity", "qty"]):
            return "QUANTITY_ERROR"
        elif any(keyword in error_lower for keyword in ["네트워크", "network", "timeout", "연결"]):
            return "NETWORK_ERROR"
        elif any(keyword in error_lower for keyword in ["거절", "reject", "거부"]):
            return "BROKER_REJECT"
        else:
            return "UNKNOWN_ERROR"

    def _get_error_summary(self) -> str:
        """에러 통계 요약 반환"""
        if not hasattr(self, '_error_stats') or not self._error_stats:
            return "에러 없음"
        
        error_lines = ["에러 통계:"]
        for error_type, count in self._error_stats.items():
            error_lines.append(f"  - {error_type}: {count}건")
        
        return "\n".join(error_lines)

    # ── 보유 종목 스코어링 현행화 ───────────────────────────────────────
    def _update_holdings_scores(self, holdings: List[Dict]) -> Dict[str, float]:
        """보유 종목들의 최신 스코어 계산 (screener_holdings 파일 우선 사용)"""
        scores_map = {}
        
        # screener_holdings 파일에서 스코어 로드
        holdings_scores = self._load_holdings_scores()
        
        for holding in holdings:
            ticker = self._t(holding.get("pdno", ""))
            if not ticker or ticker == "000000":
                continue
            
            # screener_holdings에서 스코어 확인
            if ticker in holdings_scores:
                screener_score = holdings_scores[ticker].get("score", 0.0)
                scores_map[ticker] = screener_score
                logger.debug(f"보유 종목 스코어(screener): {ticker} = {screener_score:.3f}")
                continue
            
            # screener_holdings에 없으면 기존 방식으로 실시간 계산
            try:
                # 실시간 가격 정보 조회
                price_info = self._get_realtime_price_with_quotes(ticker)
                if not price_info:
                    logger.warning(f"보유 종목 스코어링 실패: {ticker} - 가격 정보 없음")
                    continue
                
                # 기본 스코어 계산 (간단한 버전)
                current_price = price_info['current_price']
                change_rate = price_info['change_rate']
                volume = price_info['volume']
                
                # 기본 스코어 계산 (가격 변동률과 거래량 고려)
                base_score = 0.5  # 기본 점수
                
                # 가격 변동률에 따른 점수 조정
                if change_rate > 0:
                    price_score = min(0.3, change_rate / 100)  # 상승률에 따른 점수
                else:
                    price_score = max(-0.2, change_rate / 100)  # 하락률에 따른 점수
                
                # 거래량에 따른 점수 조정 (상대적)
                volume_score = min(0.2, volume / 1000000)  # 거래량 100만주당 0.2점
                
                # 최종 스코어
                final_score = max(0.0, min(1.0, base_score + price_score + volume_score))
                scores_map[ticker] = final_score
                
                logger.debug(f"보유 종목 스코어(실시간): {ticker} = {final_score:.3f} "
                           f"(변동률: {change_rate:.2f}%, 거래량: {volume:,})")
                
            except Exception as e:
                logger.error(f"보유 종목 스코어링 중 오류: {ticker} - {e}")
                continue
        
        return scores_map

    def _get_updated_holdings_with_scores(self, holdings: List[Dict]) -> List[Dict]:
        """스코어가 업데이트된 보유 종목 리스트 반환 (screener_holdings 상세 정보 포함)"""
        updated_holdings = []
        scores_map = self._update_holdings_scores(holdings)
        holdings_scores = self._load_holdings_scores()
        
        for holding in holdings:
            ticker = self._t(holding.get("pdno", ""))
            updated_holding = holding.copy()
            
            # 최신 스코어 추가
            if ticker in scores_map:
                updated_holding['current_score'] = scores_map[ticker]
            else:
                updated_holding['current_score'] = 0.0
            
            # screener_holdings에서 상세 정보 추가
            if ticker in holdings_scores:
                screener_data = holdings_scores[ticker]
                updated_holding.update({
                    'screener_name': screener_data.get('name', ''),
                    'screener_sector': screener_data.get('sector', ''),
                    'screener_price': screener_data.get('price', 0),
                    'fin_score': screener_data.get('fin_score', 0.0),
                    'tech_score': screener_data.get('tech_score', 0.0),
                    'mkt_score': screener_data.get('mkt_score', 0.0),
                    'sector_score': screener_data.get('sector_score', 0.0),
                    'vol_kki': screener_data.get('vol_kki', 0.0),
                    'pos_52w': screener_data.get('pos_52w', 0.0),
                    'rsi': screener_data.get('rsi', 0.0),
                    'ma50': screener_data.get('ma50', 0.0),
                    'ma200': screener_data.get('ma200', 0.0),
                    'updated_at': screener_data.get('updated_at', '')
                })
                
            updated_holdings.append(updated_holding)
        
        return updated_holdings

    # ── 실시간 가격 조회 및 동적 지정가 계산 ───────────────────────────
    def _get_realtime_price_with_quotes(self, ticker: str) -> Dict[str, Any]:
        """실시간 현재가 및 호가 정보 조회"""
        try:
            price_info = self.kis.get_realtime_price_with_quotes(ticker)
            if price_info:
                return price_info
            else:
                logger.warning(f"실시간 가격 조회 실패: {ticker}")
                return None
        except Exception as e:
            logger.error(f"실시간 가격 조회 중 오류: {e}")
            return None

    def _calculate_dynamic_order_price(self, current_price: int, bid_price: int, 
                                     ask_price: int, quantity: int) -> int:
        """현재가와 주문수량을 고려한 동적 지정가 계산"""
        from utils import get_tick_size, round_to_tick
        
        # quantity를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        quantity = _to_int(quantity)
        current_price = _to_int(current_price)
        bid_price = _to_int(bid_price)
        ask_price = _to_int(ask_price)
        
        tick_size = get_tick_size(current_price)
        
        # 주문수량에 따른 틱 조정
        if quantity >= 1000:  # 대량 주문
            tick_adjustment = 3
        elif quantity >= 500:  # 중량 주문
            tick_adjustment = 2
        else:  # 소량 주문
            tick_adjustment = 1
        
        # 호가 스프레드 고려
        if bid_price > 0 and ask_price > 0:
            spread = ask_price - bid_price
            if spread > tick_size * 2:  # 넓은 스프레드
                tick_adjustment = min(tick_adjustment + 1, 3)
        
        # 최종 지정가 계산 (매수이므로 현재가보다 높게)
        target_price = current_price + (tick_size * tick_adjustment)
        return round_to_tick(target_price, mode="up")

    def _is_batch_window(self) -> bool:
        """장마감 일괄 체결 윈도우(설정 기반) 구간에만 배치 모드로 처리."""
        now = datetime.now(KST)
        if now.weekday() > 4:
            return False
        try:
            # 설정 기반 기준 시각 (기본 15:20)
            base = getattr(self, "batch_check_time", "15:20") or "15:20"
            hh, mm = base.split(":")
            base_min = int(hh) * 60 + int(mm)
            now_min = int(now.strftime("%H")) * 60 + int(now.strftime("%M"))
            window = int(getattr(self, "batch_window_minutes", 3) or 3)
            return (base_min - window) <= now_min <= (base_min + window)
        except Exception:
            hhmm = now.strftime("%H:%M")
            return hhmm >= "15:17" and hhmm <= "15:23"

    def _enhanced_execution_check(self, ticker: str, order_result: Dict, expected_qty: int = 0) -> Dict[str, Any]:
        """실시간 체결 확인: 배치 윈도우에만 비활성화, 평시에는 ODNO 기반 간단 폴링."""
        if self._is_batch_window():
            logger.info(f"실시간 체결 확인 비활성화: {ticker} (15시 20분 일괄 처리)")
            if order_result.get('ok') and order_result.get('odno'):
                logger.info(f"주문 제출 성공: {ticker} {expected_qty}주 (15시 20분 체결 확인)")
                return {'status': 'submitted', 'executed': False, 'executed_qty': 0, 'wait_time': 0}
            else:
                logger.info(f"주문 제출 실패: {ticker} {expected_qty}주 (15시 20분 체결 확인)")
                return {'status': 'failed', 'executed': False, 'executed_qty': 0, 'wait_time': 0}
        
        # 평시: ok=True + ODNO 있으면 주문 접수 성공으로 간주하고 체결 확인 시도
        if order_result.get('ok') and order_result.get('odno'):
            odno = order_result.get('odno')
            exec_info = self._check_delayed_execution(ticker, expected_qty, odno)
            if exec_info:
                return {'status': 'executed', 'executed': True, 'executed_qty': exec_info.get('executed_qty', 0), 'wait_time': 1}
            # 주문 접수 성공, 미체결
            return {'status': 'submitted', 'executed': False, 'executed_qty': 0, 'wait_time': 0}
        
        # ok=False인 경우에만 실패로 간주
        return {'status': 'failed', 'executed': False, 'executed_qty': 0, 'wait_time': 0}

    def _check_order_status_direct(self, order_raw: Dict) -> bool:
        """주문 상태 직접 확인"""
        if not order_raw:
            return False
        
        try:
            # 주문번호로 상태 조회 시도
            order_no = order_raw.get('ODNO')
            if order_no:
                # KIS API로 주문 상태 조회 (실제 구현 시 API 호출)
                # 현재는 계좌 변화로 간접 확인
                self._update_account_info(force=True)
                return True  # 일단 True 반환 (실제 API 연동 시 구현)
        except Exception as e:
            logger.debug(f"주문 상태 직접 확인 실패: {e}")
        
        return False

    def _wait_for_execution(self, ticker: str, expected_qty: int, 
                           timeout_sec: int = 30) -> Dict[str, Any]:
        """실시간 체결 대기: 배치 윈도우엔 비활성화, 평시엔 짧은 폴링."""
        if self._is_batch_window():
            logger.info(f"실시간 체결 대기 비활성화: {ticker} (15시 20분 일괄 처리)")
            return {
                'status': 'submitted',
                'executed': False,
                'executed_qty': 0,
                'wait_time': 0
            }
        # 평시: 짧은 대기와 함께 2회 확인
        odno = None
        try:
            # 최근 제출 주문의 ODNO가 있으면 활용
            odno = getattr(self, '_last_order_odno', None)
        except Exception:
            pass
        for _ in range(2):
            info = self._check_delayed_execution(ticker, expected_qty, odno)
            if info:
                return {'status': 'executed', 'executed': True, 'executed_qty': info.get('executed_qty', 0), 'wait_time': 1}
            time.sleep(0.5)
        return {'status': 'submitted', 'executed': False, 'executed_qty': 0, 'wait_time': 1}

    def _execute_market_order(self, ticker: str, name: str, quantity: int, batch_name: str, pre_qty: int = None) -> Dict[str, Any]:
        """시장가 주문 실행 - 개선된 체결 확인 로직"""
        # quantity를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        quantity = _to_int(quantity)
        if pre_qty is not None:
            pre_qty = _to_int(pre_qty)
        logger.info(f"시장가 주문 실행: {name}({ticker}) {quantity}주")
        
        try:
            # 주문 전 보유 수량 확인 (중복 주문 방지)
            # pre_qty가 전달되지 않으면 현재 스냅샷에서 읽기
            if pre_qty is None:
                _, holdings_before, _ = self._get_optimized_account_info(force=True)
                pre_qty = self._get_qty(holdings_before, ticker)
            
            result = self._order_cash_retry(
                ord_dv="02", pdno=ticker, ord_dvsn="01",  # 시장가 주문
                ord_qty=str(quantity), ord_unpr="0"  # 시장가는 가격 0
            )
            
            logger.info(f"시장가 주문 응답: {result}")
            
            # 개선된 체결 확인 로직: API 응답보다 실제 계좌 변화를 우선 확인
            return self._confirm_execution_with_retry(ticker, name, quantity, batch_name, result, pre_qty)
                
        except Exception as e:
            logger.error(f"시장가 주문 중 오류: {e}")
            return {"status": "error", "error": str(e)}

    def _confirm_execution_with_retry(self, ticker: str, name: str, quantity: int, batch_name: str, 
                                    result: Dict, pre_qty: int) -> Dict[str, Any]:
        """체결 확인을 위한 재시도 로직 - 최적화된 대기 시간"""
        # quantity와 pre_qty를 정수로 변환 (문자열이 전달될 수 있는 경우 대비)
        quantity = _to_int(quantity)
        pre_qty = _to_int(pre_qty)
        # 설정 기반으로 재시도/간격 사용
        oe = self.trading_params.get("order_execution", {}) or {}
        max_retries = int(oe.get("execution_check_retries", 3))
        retry_delay = float(oe.get("execution_check_interval", 0.5))
        
        for attempt in range(max_retries):
            if attempt > 0:
                logger.info(f"체결 확인 재시도 {attempt}/{max_retries}: {name}({ticker})")
                time.sleep(retry_delay)
            
            # 계좌 정보 갱신 (최적화된 조회)
            _, holdings_after, _ = self._get_optimized_account_info(force=True)
            
            # 현재 보유 수량 확인
            current_qty = self._get_qty(holdings_after, ticker)
            executed_qty = current_qty - pre_qty
            
            if executed_qty > 0:
                # 실제로 체결됨 - 추가 검증
                excess_qty = 0
                excess_ratio = None
                if executed_qty > quantity:
                    excess_ratio = executed_qty / quantity
                    logger.warning(f"체결 수량이 주문 수량 초과: {name}({ticker}) {executed_qty}주 > {quantity}주 (초과율: {excess_ratio:.1f}배)")
                    # 임계 초과일 때만 과도 체결 처리
                    if excess_ratio > 1.5:
                        self._handle_excessive_execution(ticker, name, quantity, executed_qty)
                        excess_qty = executed_qty - quantity
                    # 집계 안정화를 위해 주문 수량까지만 인정
                    executed_qty = min(executed_qty, quantity)

                # 단일 거래 기록만 저장 (과도 체결 정보는 strategy_details에 포함)
                record_trade({
                    "side": "buy", "ticker": ticker, "name": name,
                    "qty": executed_qty, "price": 0, "trade_status": "market_executed",
                    "order_id": result.get("ODNO") or result.get("odno") or getattr(self, "_last_order_odno", None),
                    "requested_qty": quantity,
                    "executed_qty": executed_qty,
                    "strategy_details": {
                        "batch": batch_name, 
                        "order_type": "market", 
                        "delayed_confirmation": attempt > 0,
                        "confirmation_attempt": attempt + 1,
                        "pre_qty": pre_qty,
                        "current_qty": current_qty,
                        "excess_qty": excess_qty if excess_qty > 0 else None,
                        "excess_ratio": excess_ratio if excess_ratio is not None and excess_qty > 0 else None
                    }
                })
                
                logger.info(f"✅ 시장가 주문 체결 확인: {name}({ticker}) {executed_qty}주 "
                           f"(시도 {attempt + 1}/{max_retries}, 지연: {attempt > 0}, "
                           f"이전: {pre_qty}주 → 현재: {current_qty}주)")
                return {
                    "status": "executed", 
                    "result": result, 
                    "quantity": executed_qty, 
                    "delayed_confirmation": attempt > 0,
                    "confirmation_attempt": attempt + 1,
                    "pre_qty": pre_qty,
                    "current_qty": current_qty
                }
        
        # 모든 재시도 후에도 체결되지 않음
        rt_cd = result.get('rt_cd', '')
        msg = result.get('msg1', 'Unknown error')
        logger.error(f"❌ 시장가 주문 최종 실패: {name}({ticker}) - "
                    f"rt_cd:{rt_cd}, msg:{msg}, 시도: {max_retries}회")
        return {"status": "failed", "result": result, "error": msg}

    def _mark_as_low_priority(self, ticker: str):
        """최후순위 종목으로 분류"""
        if not hasattr(self, '_low_priority_tickers'):
            self._low_priority_tickers = set()
        self._low_priority_tickers.add(ticker)
        logger.info(f"최후순위 종목으로 분류: {ticker}")

    def _is_low_priority(self, ticker: str) -> bool:
        """최후순위 종목 여부 확인"""
        return hasattr(self, '_low_priority_tickers') and ticker in self._low_priority_tickers

    def _prevent_duplicate_orders(self, ticker: str, batch_name: str) -> bool:
        """중복 주문 방지 - 동일 배치에서 같은 종목 중복 주문 차단"""
        order_key = f"{ticker}_{batch_name}"
        if order_key in self._processed_orders:
            logger.warning(f"중복 주문 방지: {ticker} in {batch_name}")
            return False
        return True

    def _mark_order_processed(self, ticker: str, batch_name: str):
        """주문 처리 완료 표시"""
        order_key = f"{ticker}_{batch_name}"
        self._processed_orders.add(order_key)
        logger.debug(f"주문 처리 완료 표시: {order_key}")

    def _clear_processed_orders(self):
        """처리된 주문 목록 초기화 (새로운 실행 시)"""
        self._processed_orders.clear()
        self._order_lock.clear()
        logger.info("처리된 주문 목록 초기화 완료")

    def _is_account_cache_valid(self) -> bool:
        """계좌 캐시가 유효한지 확인"""
        current_time = time.time()
        return (current_time - self._last_account_update) < self._account_cache_ttl

    def _get_cached_account_info(self) -> Optional[Tuple[int, List[Dict], Dict]]:
        """캐시된 계좌 정보 반환"""
        if self._is_account_cache_valid() and self._account_cache:
            logger.debug("캐시된 계좌 정보 사용")
            return self._account_cache.get('data')
        return None

    def _update_account_cache(self, cash: int, holdings: List[Dict], summary: Dict):
        """계좌 정보 캐시 업데이트"""
        self._account_cache = {
            'data': (cash, holdings, summary),
            'timestamp': time.time()
        }
        self._last_account_update = time.time()
        logger.debug("계좌 정보 캐시 업데이트 완료")

    def _get_optimized_account_info(self, force: bool = False) -> Tuple[int, List[Dict], Dict]:
        """최적화된 계좌 정보 조회"""
        if not force:
            cached_data = self._get_cached_account_info()
            if cached_data:
                return cached_data
        
        # 캐시가 없거나 강제 업데이트인 경우 실제 조회
        logger.debug("계좌 정보 실제 조회 실행")
        self._update_account_info(force=True)
        cash, holdings, summary = self._load_snapshot()
        self._update_account_cache(cash, holdings, summary)
        return cash, holdings, summary

    def _load_gpt_analysis_results(self) -> Dict[str, Any]:
        """GPT 분석 결과 로드"""
        try:
            gpt_file = find_latest_file("gpt_trades_*.json")
            if not gpt_file:
                logger.warning("GPT 분석 파일을 찾을 수 없습니다.")
                return {}
            
            with open(gpt_file, 'r', encoding='utf-8') as f:
                gpt_raw = json.load(f)
            # 래핑/레거시 형식 모두 지원
            if isinstance(gpt_raw, dict) and 'plans' in gpt_raw:
                gpt_data = gpt_raw.get('plans') or []
            else:
                gpt_data = gpt_raw if isinstance(gpt_raw, list) else []
            # GPT 분석 결과 파싱
            gpt_decisions = {}
            for item in gpt_data:
                ticker = self._t(item.get("stock_info", {}).get("Ticker", ""))
                decision = item.get("결정", "")
                analysis = item.get("분석", "")
                
                gpt_decisions[ticker] = {
                    "decision": decision,
                    "analysis": analysis,
                    "stock_info": item.get("stock_info", {}),
                    "rank": item.get("rank", 999)
                }
                
                # 보류 결정 추적
                if decision == "보류":
                    self._gpt_hold_decisions.add(ticker)
            
            logger.info(f"GPT 분석 결과 로드 완료: {len(gpt_decisions)}종목")
            return gpt_decisions
            
        except Exception as e:
            logger.error(f"GPT 분석 결과 로드 실패: {e}")
            return {}

    def _calculate_integrated_score(self, ticker: str, internal_score: float, gpt_decision: str) -> float:
        """통합 점수 계산 (GPT + 내부 점수)"""
        gpt_weight = self.integrated_analysis.get("gpt_weight", 0.7)
        internal_weight = self.integrated_analysis.get("internal_weight", 0.3)
        
        # GPT 결정을 점수로 변환
        gpt_score = 0.0
        if gpt_decision == "매수":
            gpt_score = 1.0
        elif gpt_decision == "보류":
            gpt_score = 0.3
        elif gpt_decision == "매도":
            gpt_score = 0.0
        
        # 가중 평균 계산
        integrated_score = (gpt_score * gpt_weight + internal_score * internal_weight)
        
        if self.integrated_analysis.get("enhanced_logging", False):
            logger.info(f"통합 점수 계산: {ticker} - GPT: {gpt_score:.3f}({gpt_decision}) * {gpt_weight} + "
                       f"내부: {internal_score:.3f} * {internal_weight} = {integrated_score:.3f}")
        
        return integrated_score

    def _filter_gpt_hold_decisions(self, candidates: List[Dict]) -> List[Dict]:
        """GPT 보류 결정 반영하여 후보 필터링"""
        if not self.integrated_analysis.get("respect_gpt_hold", True):
            return candidates
        
        filtered_candidates = []
        for candidate in candidates:
            # 대소문자 폴백 처리: Ticker/ticker, Name/name 모두 지원
            ticker = self._t(candidate.get("Ticker") or candidate.get("ticker", ""))
            name = candidate.get("Name") or candidate.get("name", "N/A")
            
            if ticker not in self._gpt_hold_decisions:
                filtered_candidates.append(candidate)
            else:
                logger.info(f"GPT 보류 결정 반영: {name}({ticker}) 제외")
                self._log_analysis_step(f"GPT 보류 제외: {name}({ticker})")
        
        return filtered_candidates

    def _log_analysis_step(self, message: str):
        """분석 과정 로깅"""
        if self.integrated_analysis.get("enhanced_logging", False):
            timestamp = datetime.now(KST).strftime("%H:%M:%S")
            self._analysis_log.append(f"[{timestamp}] {message}")
            logger.info(f"[ANALYSIS] {message}")


    def _validate_to_sell_list(self, to_sell_list: List[Dict]) -> List[Dict]:
        """to_sell_list 데이터 검증 및 정리"""
        logger.debug(f"[DEBUG] 매도 항목 검증 시작: {len(to_sell_list)}건")
        
        validated = []
        for i, item in enumerate(to_sell_list):
            logger.debug(f"[DEBUG] 매도 항목 {i} 검증 중: {item}")
            
            if not isinstance(item, dict):
                logger.warning(f"매도 항목 {i}: 딕셔너리가 아님 - {type(item)}")
                continue
            
            # stock_info가 있는 경우와 없는 경우 모두 처리
            if "stock_info" in item:
                logger.debug(f"[DEBUG] 매도 항목 {i}: stock_info 구조 감지")
                stock_info = item.get("stock_info", {})
                ticker = self._t(stock_info.get("Ticker", ""))
                name = stock_info.get("Name", "")
                qty = item.get("qty", 0)
                logger.debug(f"[DEBUG] 매도 항목 {i}: stock_info에서 추출 - ticker:{ticker}, name:{name}, qty:{qty}")
            else:
                logger.debug(f"[DEBUG] 매도 항목 {i}: 직접 구조 감지")
                ticker = self._t(item.get("ticker", ""))
                name = item.get("name", "")
                qty = item.get("qty", 0)
                logger.debug(f"[DEBUG] 매도 항목 {i}: 직접에서 추출 - ticker:{ticker}, name:{name}, qty:{qty}")
            
            # 필수 키 검증
            if not ticker or not name or not qty:
                missing_info = []
                if not ticker: missing_info.append("ticker")
                if not name: missing_info.append("name")
                if not qty: missing_info.append("qty")
                logger.warning(f"매도 항목 {i}: 필수 정보 누락 {missing_info} - ticker:{ticker}, name:{name}, qty:{qty}")
                continue
            
            # ticker 형식 검증
            if ticker == "000000" or len(ticker) != 6:
                logger.warning(f"매도 항목 {i}: 잘못된 ticker 형식 - {ticker}")
                continue
            
            # qty 형식 검증
            try:
                qty_int = int(qty)
                if qty_int <= 0:
                    logger.warning(f"매도 항목 {i}: 잘못된 수량 - {qty_int}")
                    continue
            except (ValueError, TypeError):
                logger.warning(f"매도 항목 {i}: 수량 변환 실패 - {qty}")
                continue
            
            # 표준화된 구조로 변환 (qty는 정수로 보장)
            standardized_item = {
                **item,  # 기타 정보 유지
                "ticker": ticker,
                "name": name,
                "qty": qty_int,  # 정수로 변환된 값으로 덮어쓰기
            }
            
            # 필수 스코어 키가 없으면 기본값 추가 (GPT 기반 리밸런싱 호환성)
            if "old_score" not in standardized_item:
                standardized_item["old_score"] = float(item.get("old_score", item.get("score", 0.0)))
            if "new_score" not in standardized_item:
                standardized_item["new_score"] = float(item.get("new_score", item.get("score", 0.0)))
            if "new_ticker" not in standardized_item:
                standardized_item["new_ticker"] = self._t(item.get("new_ticker", item.get("target_ticker", "")))
            
            validated.append(standardized_item)
            logger.debug(f"[DEBUG] 매도 항목 {i}: 검증 통과 - {standardized_item}")
        
        if len(validated) != len(to_sell_list):
            logger.warning(f"매도 항목 검증: {len(to_sell_list)} → {len(validated)} (제외: {len(to_sell_list) - len(validated)})")
        else:
            logger.debug(f"[DEBUG] 매도 항목 검증 완료: 모든 {len(validated)}건 통과")
        
        return validated

    def _validate_to_buy_plans(self, to_buy_plans: List[Dict]) -> List[Dict]:
        """to_buy_plans 데이터 검증 및 정리"""
        logger.debug(f"[DEBUG] 매수 계획 검증 시작: {len(to_buy_plans)}건")
        
        validated = []
        for i, plan in enumerate(to_buy_plans):
            logger.debug(f"[DEBUG] 매수 계획 {i} 검증 중: {plan}")
            
            if not isinstance(plan, dict):
                logger.warning(f"매수 계획 {i}: 딕셔너리가 아님 - {type(plan)}")
                continue
            
            # stock_info 검증
            stock_info = plan.get("stock_info", {})
            if not isinstance(stock_info, dict):
                logger.warning(f"매수 계획 {i}: stock_info가 딕셔너리가 아님 - {type(stock_info)}")
                continue
            
            ticker = self._t(stock_info.get("Ticker", ""))
            name = stock_info.get("Name", "")
            logger.debug(f"[DEBUG] 매수 계획 {i}: stock_info에서 추출 - ticker:{ticker}, name:{name}")
            
            if ticker == "000000" or len(ticker) != 6:
                logger.warning(f"매수 계획 {i}: 잘못된 ticker 형식 - {ticker}")
                continue
            
            if not name:
                logger.warning(f"매수 계획 {i}: 종목명 누락 - ticker:{ticker}")
                continue
            
            validated.append(plan)
            logger.debug(f"[DEBUG] 매수 계획 {i}: 검증 통과 - {name}({ticker})")
        
        if len(validated) != len(to_buy_plans):
            logger.warning(f"매수 계획 검증: {len(to_buy_plans)} → {len(validated)} (제외: {len(to_buy_plans) - len(validated)})")
        else:
            logger.debug(f"[DEBUG] 매수 계획 검증 완료: 모든 {len(validated)}건 통과")
        
        return validated
    # Phase 1: 최소 보유기간 체크 로직은 utils.check_min_holding_period로 통일됨

    def _get_enhanced_rebalance_candidates(self, holdings: List[Dict], gpt_decisions: Dict) -> List[Dict]:
        """향상된 리밸런싱 후보 선정 (GPT 분석 반영)"""
        self._log_analysis_step("리밸런싱 후보 선정 시작")
        logger.debug(f"[DEBUG] 리밸런싱 시작 - 보유종목: {len(holdings)}개, GPT분석: {len(gpt_decisions)}개")
        
        # GPT 기반 리밸런싱 사용 여부 확인
        use_gpt_rebalance = self.settings.get("rebalance_params", {}).get("use_gpt_analysis", True)
        
        if use_gpt_rebalance:
            logger.debug(f"[DEBUG] GPT 기반 리밸런싱 모드 활성화")
            try:
                # gpt_analyzer.py의 함수 호출
                to_sell_list, to_buy_plans = get_gpt_enhanced_rebalance_candidates(
                    holdings, 
                    self.all_stock_data, 
                    self.settings
                )
                
                # 데이터 검증 및 정리
                logger.debug(f"[DEBUG] GPT 리밸런싱 데이터 검증 시작")
                to_sell_list = self._validate_to_sell_list(to_sell_list)
                to_buy_plans = self._validate_to_buy_plans(to_buy_plans)
                logger.debug(f"[DEBUG] GPT 리밸런싱 데이터 검증 완료 - 매도: {len(to_sell_list)}건, 매수: {len(to_buy_plans)}건")
                
            except Exception as e:
                logger.error(f"GPT 기반 리밸런싱 실패: {e}", exc_info=True)
                logger.info("[FALLBACK] GPT 리밸런싱 실패 - 기존 점수 기반 리밸런싱으로 폴백")
                to_sell_list, to_buy_plans = fallback_rebalance_logic(holdings, self.all_stock_data)
        else:
            logger.debug(f"[DEBUG] 기존 점수 기반 리밸런싱 모드")
            # 기존 로직 사용
            to_sell_list, to_buy_plans = self._determine_rebalance_swaps([], holdings)
            
            # 원본 데이터 로깅
            for i, item in enumerate(to_sell_list):
                logger.debug(f"[DEBUG] 원본 매도 {i}: {item}")
            for i, plan in enumerate(to_buy_plans):
                stock_info = plan.get("stock_info", {})
                logger.debug(f"[DEBUG] 원본 매수 {i}: {stock_info.get('Name', 'N/A')}({stock_info.get('Ticker', 'N/A')})")
            
            # 데이터 검증 및 정리
            logger.debug(f"[DEBUG] 데이터 검증 시작")
            to_sell_list = self._validate_to_sell_list(to_sell_list)
            to_buy_plans = self._validate_to_buy_plans(to_buy_plans)
            logger.debug(f"[DEBUG] 데이터 검증 완료 - 매도: {len(to_sell_list)}건, 매수: {len(to_buy_plans)}건")
            
            # GPT 분석 결과 반영 (기존 로직)
            if gpt_decisions:
                logger.debug(f"[DEBUG] GPT 분석 결과 반영 시작 - {len(gpt_decisions)}개 분석")
                # 매수 후보에 GPT 분석 결과 추가
                enhanced_buy_plans = []
                for plan in to_buy_plans:
                    ticker = self._t(plan.get("stock_info", {}).get("Ticker", ""))
                    logger.debug(f"[DEBUG] GPT 분석 확인 중: {ticker}")
                    if ticker in gpt_decisions:
                        gpt_info = gpt_decisions[ticker]
                        plan["gpt_analysis"] = gpt_info
                        plan["integrated_score"] = self._calculate_integrated_score(
                            ticker, 
                            plan.get("stock_info", {}).get("Score", 0.0),
                            gpt_info.get("decision", "")
                        )
                        logger.debug(f"[DEBUG] GPT 분석 적용: {ticker} - {gpt_info.get('decision', 'N/A')} (통합점수: {plan['integrated_score']:.3f})")
                        enhanced_buy_plans.append(plan)
                    else:
                        # GPT 분석 없는 종목은 내부 점수만 사용
                        plan["integrated_score"] = plan.get("stock_info", {}).get("Score", 0.0)
                        logger.debug(f"[DEBUG] GPT 분석 없음: {ticker} - 내부점수만 사용 (점수: {plan['integrated_score']:.3f})")
                        enhanced_buy_plans.append(plan)
                
                # 통합 점수 기준으로 정렬
                enhanced_buy_plans.sort(key=lambda x: x.get("integrated_score", 0.0), reverse=True)
                to_buy_plans = enhanced_buy_plans
                logger.debug(f"[DEBUG] GPT 분석 반영 완료 - 정렬된 매수계획: {len(to_buy_plans)}건")
            else:
                logger.debug(f"[DEBUG] GPT 분석 결과 없음 - 내부 점수만 사용")
            
            # GPT 보류 결정 반영
            logger.debug(f"[DEBUG] GPT 보류 결정 필터링 시작")
            original_count = len(to_buy_plans)
            to_buy_plans = self._filter_gpt_hold_decisions(to_buy_plans)
            filtered_count = original_count - len(to_buy_plans)
            if filtered_count > 0:
                logger.debug(f"[DEBUG] GPT 보류 결정으로 {filtered_count}건 제외")
        
        self._log_analysis_step(f"리밸런싱 후보 선정 완료: 매도 {len(to_sell_list)}건, 매수 {len(to_buy_plans)}건")
        logger.debug(f"[DEBUG] 최종 리밸런싱 결과 - 매도: {len(to_sell_list)}건, 매수: {len(to_buy_plans)}건")
        
        return to_sell_list, to_buy_plans

    def _log_decision_transparency(self, to_sell_list: List[Dict], to_buy_plans: List[Dict], gpt_decisions: Dict):
        """의사결정 투명성 로깅 (최적화된 버전)"""
        logger.info("=== 의사결정 투명성 보고서 ===")
        
        # GPT 분석 결과 요약
        gpt_buy_count = sum(1 for decision in gpt_decisions.values() if decision.get("decision") == "매수")
        gpt_hold_count = sum(1 for decision in gpt_decisions.values() if decision.get("decision") == "보류")
        gpt_sell_count = sum(1 for decision in gpt_decisions.values() if decision.get("decision") == "매도")
        
        logger.info(f"GPT 분석 결과: 매수 {gpt_buy_count}건, 보류 {gpt_hold_count}건, 매도 {gpt_sell_count}건")
        
        # 매도 결정 요약 (상세 로깅은 DEBUG 레벨로)
        if to_sell_list:
            logger.info(f"=== 매도 결정 상세 ({len(to_sell_list)}건) ===")
            for i, sell_item in enumerate(to_sell_list[:5]):  # 최대 5건만 표시
                ticker = sell_item.get("ticker", "")
                name = sell_item.get("name", "N/A")
                old_score = sell_item.get("old_score", 0.0)
                new_score = sell_item.get("new_score", 0.0)
                logger.info(f"  [{i+1}] {name}({ticker}): {old_score:.3f} → {new_score:.3f} (Δ={new_score-old_score:.3f})")
            
            if len(to_sell_list) > 5:
                logger.info(f"  ... 외 {len(to_sell_list) - 5}건")
        else:
            logger.info("=== 매도 결정 상세 ===")
            logger.info("  매도 대상 없음")
        
        # 매수 결정 요약 (상세 로깅은 DEBUG 레벨로)
        if to_buy_plans:
            logger.info(f"=== 매수 결정 상세 ({len(to_buy_plans)}건) ===")
            for i, buy_plan in enumerate(to_buy_plans[:3]):  # 최대 3건만 표시
                stock_info = buy_plan.get("stock_info", {})
                ticker = self._t(stock_info.get("Ticker", ""))
                name = stock_info.get("Name", "N/A")
                internal_score = stock_info.get("Score", 0.0)
                integrated_score = buy_plan.get("integrated_score", internal_score)
                
                # GPT 분석 정보
                gpt_info = buy_plan.get("gpt_analysis", {})
                gpt_decision = gpt_info.get("decision", "분석없음")
                
                logger.info(f"  [{i+1}] {name}({ticker}): 내부={internal_score:.3f}, 통합={integrated_score:.3f}, GPT={gpt_decision}")
            
            if len(to_buy_plans) > 3:
                logger.info(f"  ... 외 {len(to_buy_plans) - 3}건")
        else:
            logger.info("=== 매수 결정 상세 ===")
            logger.info("  매수 대상 없음")
        
        # 제외된 종목 요약 (GPT 보류)
        if hasattr(self, '_gpt_hold_decisions') and self._gpt_hold_decisions:
            logger.info(f"=== GPT 보류로 제외된 종목 ({len(self._gpt_hold_decisions)}건) ===")
            for i, ticker in enumerate(list(self._gpt_hold_decisions)[:3]):  # 최대 3건만 표시
                gpt_info = gpt_decisions.get(ticker, {})
                name = gpt_info.get("stock_info", {}).get("Name", "N/A")
                logger.info(f"  - {name}({ticker}): GPT 보류 결정")
            
            if len(self._gpt_hold_decisions) > 3:
                logger.info(f"  ... 외 {len(self._gpt_hold_decisions) - 3}건")
        
        logger.info("=== 의사결정 투명성 보고서 완료 ===")

    # ── GPT 회전(샌드박스) 제안 로깅 ────────────────────────────────────
    def _log_gpt_rotation_sandbox(self):
        try:
            ia = self.integrated_analysis or {}
            if not ia.get("log_gpt_rotation_suggestions", True):
                return
            # 가장 최근 회전 제안 파일 탐색
            rot_file = find_latest_file("gpt_rotations_*.json")
            if not rot_file:
                logger.info("[RotationSandbox] 회전 제안 파일이 없습니다.")
                return
            with open(rot_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            suggestions = data.get("suggestions", []) or []
            min_conf = float(ia.get("min_confidence_for_rotation", 0.7))
            total = len(suggestions)
            kept = [s for s in suggestions if float(s.get("confidence", 0.0)) >= min_conf]
            dropped = total - len(kept)
            logger.info(f"[RotationSandbox] 제안 로드: 총 {total}건, 하한(conf>={min_conf:.2f}) 통과 {len(kept)}건, 제외 {dropped}건")
            for i, s in enumerate(sorted(kept, key=lambda x: x.get("priority", 0), reverse=True)[:5], 1):
                logger.info(
                    f"  [{i}] SELL {s.get('name','N/A')}({self._t(s.get('ticker',''))}) "
                    f"w={float(s.get('current_weight',0)):.3f}→{float(s.get('target_weight',0)):.3f} "
                    f"conf={float(s.get('confidence',0)):.2f} reasons={s.get('reasons', [])}"
                )
        except Exception as e:
            logger.warning(f"[RotationSandbox] 로깅 중 오류: {e}")

    # ── 미체결 주문 관리 ───────────────────────────────────────────────
    def _check_and_cancel_pending_orders(self):
        """기존 미체결 주문 확인 및 취소"""
        try:
            logger.info("미체결 주문 확인 중...")
            pending_orders = self.kis.get_pending_orders()
            
            if pending_orders.empty:
                logger.info("미체결 주문이 없습니다.")
                return
            
            logger.info(f"미체결 주문 {len(pending_orders)}건 발견")
            
            # 미체결 주문 상세 정보 로깅
            for _, order in pending_orders.iterrows():
                ticker = self._t(order.get('pdno', ''))
                name = order.get('prdt_name', 'N/A')
                side = '매수' if order.get('sll_buy_dvsn_cd', '') == '02' else '매도'
                qty = order.get('ord_qty', '')
                price = order.get('ord_unpr', '')
                logger.info(f"  - {name}({ticker}) {side} {qty}주 @ {price}원")
            
            # 모든 미체결 주문 취소
            cancelled_orders = self.kis.cancel_all_pending_orders()
            
            if cancelled_orders:
                logger.info(f"미체결 주문 {len(cancelled_orders)}건 취소 완료")
                _notify_text(f"미체결 주문 {len(cancelled_orders)}건 취소 완료", 
                           key=f"phase:cancel_orders:{self.run_id}", cooldown=300)
                
                # 취소된 주문 상세 정보 로깅
                for order in cancelled_orders:
                    logger.info(f"  ✓ {order['name']}({order['ticker']}) {order['side']} {order['qty']}주 취소")
            else:
                logger.warning("미체결 주문 취소 실패")
                
        except Exception as e:
            # 404 오류는 정상적인 경우일 수 있음 (미체결 주문이 없는 경우)
            if "404" in str(e):
                logger.info("미체결 주문이 없습니다. (404 응답)")
            else:
                logger.error(f"미체결 주문 확인/취소 중 오류: {e}", exc_info=True)
                _notify_text(f"미체결 주문 처리 중 오류: {e}", 
                           key=f"phase:cancel_orders_error:{self.run_id}", cooldown=300)

    # ── 매수 로직 ─────────────────────────────────────────────────────
    # Phase 2: 일일 손실 제한 체크
    def _check_daily_loss_limit(self) -> Tuple[bool, str]:
        """일일 손실 제한 체크"""
        try:
            max_daily_loss_pct = self.risk_params.get("max_daily_loss_pct", 0.0)
            max_daily_loss_amount = self.risk_params.get("max_daily_loss_amount", 0)
            
            if max_daily_loss_pct <= 0 and max_daily_loss_amount <= 0:
                return True, ""  # 제한 없음
            
            # 오늘 날짜 기준으로 거래 기록 조회
            today = datetime.now(KST).date()
            today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=KST)
            today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=KST)
            
            from recorder import DataRecorder
            recorder = DataRecorder()
            today_trades = recorder.get_trade_records(start_date=today_start, end_date=today_end, action="SELL")
            
            # 오늘 총 손실 계산
            total_loss = sum(t.profit_loss for t in today_trades if t.profit_loss < 0)
            
            # 계좌 총액 조회 (퍼센트 계산용)
            summary, balance, _, _ = get_account_snapshot_cached()
            total_value = _to_int(summary.get("dnca_tot_amt", 0))
            
            # 손실 제한 체크
            if max_daily_loss_amount > 0 and abs(total_loss) >= max_daily_loss_amount:
                return False, f"일일 손실 금액 제한 초과: {abs(total_loss):,.0f}원 >= {max_daily_loss_amount:,}원"
            
            if max_daily_loss_pct > 0 and total_value > 0:
                loss_pct = abs(total_loss) / total_value
                if loss_pct >= max_daily_loss_pct:
                    return False, f"일일 손실 비율 제한 초과: {loss_pct:.2%} >= {max_daily_loss_pct:.2%}"
            
            return True, ""
        except Exception as e:
            logger.warning(f"일일 손실 제한 체크 실패: {e}")
            return True, ""  # 오류 시 안전하게 통과
    
    # Phase 2: 연속 손실 체크
    def _check_consecutive_losses(self) -> Tuple[bool, str]:
        """연속 손실 체크 (일일 리셋: 날짜가 바뀌면 자동으로 리셋됨)"""
        try:
            stop_on_losses = self.risk_params.get("stop_trading_on_consecutive_losses", 0)
            
            if stop_on_losses <= 0:
                return True, ""  # 제한 없음
            
            # 오늘 날짜 기준으로 거래 기록 조회 (일일 리셋을 위해)
            today = datetime.now(KST).date()
            today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=KST)
            today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=KST)
            
            from recorder import DataRecorder
            recorder = DataRecorder()
            recent_trades = recorder.get_trade_records(start_date=today_start, end_date=today_end, action="SELL")
            
            # 최근 거래를 시간순으로 정렬
            recent_trades.sort(key=lambda x: x.timestamp if isinstance(x.timestamp, datetime) else datetime.fromisoformat(str(x.timestamp)), reverse=True)
            
            # 연속 손실 횟수 계산 (오늘 거래만)
            consecutive_losses = 0
            for trade in recent_trades:
                if trade.profit_loss < 0:
                    consecutive_losses += 1
                else:
                    break  # 손익이 나면 중단
            
            if consecutive_losses >= stop_on_losses:
                return False, f"연속 손실 {consecutive_losses}회 >= {stop_on_losses}회로 거래 중단 (오늘 기준)"
            
            return True, ""
        except Exception as e:
            logger.warning(f"연속 손실 체크 실패: {e}")
            return True, ""  # 오류 시 안전하게 통과

    def run_buy_logic(self, available_cash: int, holdings: List[Dict]):
        # [NEW] 매수 로직 활성화 여부 체크
        if not self.trading_params.get("buy_enabled", True):
            logger.info("매수 로직이 비활성화되어 있습니다 (trading_params.buy_enabled=false)")
            _notify_text("ℹ️ 매수 로직 비활성화됨", key=f"buy_disabled:{self.run_id}", cooldown=600)
            return
        
        logger.info(f"--------- 신규/추가 매수 로직 실행 (가용 예산: {available_cash:,} 원) ---------")

        # Phase 2: 일일 손실 제한 체크
        daily_loss_ok, daily_loss_msg = self._check_daily_loss_limit()
        if not daily_loss_ok:
            logger.warning(f"⚠️ {daily_loss_msg} → 매수 중단")
            _notify_text(f"⚠️ {daily_loss_msg} → 매수 중단", key=f"daily_loss_limit:{self.run_id}", cooldown=300)
            return
        
        # Phase 2: 연속 손실 체크
        consecutive_ok, consecutive_msg = self._check_consecutive_losses()
        if not consecutive_ok:
            logger.warning(f"⚠️ {consecutive_msg} → 매수 중단")
            _notify_text(f"⚠️ {consecutive_msg} → 매수 중단", key=f"consecutive_loss:{self.run_id}", cooldown=300)
            return

        # 통합된 시간대 체크
        if not self._check_trading_hours("buy"):
            return

        # 기존 미체결 주문 확인 및 취소
        self._check_and_cancel_pending_orders()

        # 포트폴리오 집중도 체크
        concentration_check = self._check_portfolio_concentration(holdings, available_cash)
        logger.info(f"[PORTFOLIO_RISK] 집중도: {concentration_check['concentration_risk']}, 최대비중: {concentration_check['max_weight']:.1%}")
        
        if concentration_check['warnings']:
            for warning in concentration_check['warnings']:
                logger.warning(f"[RISK_WARNING] {warning}")
                _notify_text(f"⚠️ {warning}", key=f"risk_warning:{self.run_id}", cooldown=300)

        # 동적 슬롯 축소 (초기 계산)
        if self.trading_guards.get("auto_shrink_slots", False):
            effective_slots = self._compute_effective_slots(available_cash)
        else:
            effective_slots = self.max_positions

        # GPT 추천 계획 로드
        trade_plan_file = find_latest_file("gpt_trades_*.json")
        buy_plans = []
        if not trade_plan_file:
            logger.info("매수 계획 파일(gpt_trades_*.json)이 없어 매수를 건너뜁니다.")
            _notify_text("ℹ️ gpt_trades 파일 없음 → 매수 스킵",
                         key=f"phase:no_trades:{self.run_id}", cooldown=600)
            # gpt 트레이드가 없어도 리밸런스는 동작하도록 진행(신규 타겟은 all_stock_data 기반)
        else:
            with open(trade_plan_file, 'r', encoding='utf-8') as f:
                trade_data = json.load(f)
            
            # 신버전/구버전 호환성 처리
            if isinstance(trade_data, dict) and "plans" in trade_data:
                # 신버전: { "plans": [...] }
                trade_plans = trade_data["plans"]
            elif isinstance(trade_data, list):
                # 구버전: [...]
                trade_plans = trade_data
            else:
                logger.error(f"알 수 없는 trade_plans 형식: {type(trade_data)}")
                trade_plans = []
            
            buy_plans = [p for p in trade_plans if p.get("결정") == "매수"]
            # 추천 집계
            self.recomm_stats["buy"] += len(buy_plans)
            # 안전 디폴트 주입
            for p in buy_plans:
                p.setdefault("stock_info", {})
                for k, dv in SCHEMA_DEFAULTS.items():
                    p["stock_info"].setdefault(k, dv)

        # 1차 후보(가성비) 판단: **주가 vs (현금×(1-버퍼))**만 적용
        buffer = self._eff_buffer()
        min_order = int(self.trading_params.get("min_order_cash", 0))

        candidates_all = list(self.all_stock_data.values())
        affordable = [c for c in candidates_all if _to_int(c.get("Price", 0)) <= int(available_cash * (1 - buffer))]

        # 통계 로그(참고용) — 필터에는 min_order_cash를 사용하지 않음
        log_affordability_stats(available_cash, buffer, candidates_all, min_order_cash=min_order)

        if self.screener_params.get("affordability_filter", False) and not affordable:
            # LOW_FUNDS 전에 회전 시도. 단, rotation.enabled=false이면 회전 매매를 절대 실행하지 않음
            if not self._is_rotation_enabled():
                cheapest = min((_to_int(c.get("Price", 0)) for c in candidates_all), default=0)
                self._set_summary_reason("SKIPPED_LOW_FUNDS_ROTATION_DISABLED",
                                         f"cheapest={cheapest:,} cash={available_cash:,} buffer={buffer:.2%} min_order_cash={min_order:,}")
                logger.info("가용 예산 부족 & 회전매매 비활성화 → 매수 종료.")
                return

            rotated = self.try_rotation(candidates_all, holdings, available_cash)
            if not rotated:
                cheapest = min((_to_int(c.get("Price", 0)) for c in candidates_all), default=0)
                self._set_summary_reason("SKIPPED_LOW_FUNDS_NO_ROTATION",
                                         f"cheapest={cheapest:,} cash={available_cash:,} buffer={buffer:.2%} min_order_cash={min_order:,}")
                logger.info("가용 예산 부족 & 회전 실패 → 매수 종료.")
                return
            else:
                # 회전 성공 시 현금/보유 최신화 후 계속 신규/추가 매수 진행
                time.sleep(2)  # 3초 -> 2초로 단축
                self._update_account_info(force=True)
                available_cash, holdings, _ = self._load_snapshot()
                # 슬롯 재계산
                effective_slots = self._compute_effective_slots(available_cash) if self.trading_guards.get("auto_shrink_slots", False) else self.max_positions

        # 보유 집합
        holding_tickers = {self._t(h.get("pdno", "")) for h in holdings if _to_int(h.get("hldg_qty", 0)) > 0}

        # 후보 분리: 신규 / 추가매수 (gpt 계획이 있을 경우에만 사용)
        new_targets = []
        rebuy_candidates = []
        if buy_plans:
            for plan in buy_plans:
                info = plan["stock_info"]
                ticker = self._t(info.get("Ticker", ""))
                name = info.get("Name", "N/A")

                if ticker in holding_tickers:
                    if not self.allow_rebuy:
                        logger.info(f"[{name}({ticker})] 이미 보유 → 추가매수 비활성이라 제외")
                        continue
                    if self._is_in_cooldown(ticker):
                        logger.info(f"[{name}({ticker})] 쿨다운 중 → 추가매수 제외")
                        continue
                    ok, why = self._can_rebuy(ticker, info, holdings, available_cash)
                    if not ok:
                        logger.info(f"[REBUY-블록] {name}({ticker}) 제외: {why}")
                        continue
                    logger.info(f"[REBUY] {name}({ticker}) 추가매수 후보 등록 ({why})")
                    rebuy_candidates.append(plan)
                else:
                    if self._is_in_cooldown(ticker):
                        logger.info(f"[{name}({ticker})] 쿨다운 중 → 신규매수 제외")
                        continue
                    new_targets.append(plan)
        remaining_cash = available_cash
        any_order_placed = False
        def _execute_buy_batch(plans: List[Dict], batch_name: str):
            nonlocal remaining_cash, any_order_placed, effective_slots
            if not plans:
                return
            if remaining_cash <= max(self.min_order_cash, self.min_cash_reserve):
                logger.info(f"잔여 현금이 최소치 이하({remaining_cash:,}원)로 {batch_name} 스킵.")
                return

            _notify_text(f" {batch_name} 매수 시도 {len(plans)}종목 (예산 {remaining_cash:,}원)",
                         key=f"phase:{batch_name.lower()}_start:{self.run_id}", cooldown=120)
            logger.info(f"총 {len(plans)}개 종목 {batch_name} 매수 시도. 유동적 예산 배분 + 버퍼 적용.")

            for i, plan in enumerate(plans):
                info = plan.get("stock_info", {})
                for k, dv in SCHEMA_DEFAULTS.items():  # 안전 디폴트
                    info.setdefault(k, dv)
                ticker, name = self._t(info.get("Ticker", "")), info.get("Name", "N/A")
                slots_left = len(plans) - i

                # 중복 주문 방지 체크
                if not self._prevent_duplicate_orders(ticker, batch_name):
                    logger.info(f"  -> [{name}({ticker})] 중복 주문 방지로 스킵")
                    continue

                # 최신 현금 기준으로 슬롯 사용량 재평가
                if self.trading_guards.get("auto_shrink_slots", False):
                    effective_slots = self._compute_effective_slots(remaining_cash)

                # 개선된 포지션 사이징 로직
                # 1. 종목당 최대 투입 가능 금액 계산 (per_ticker_max_weight 적용)
                max_allowed_per_stock = int(remaining_cash * self.per_ticker_max_weight)
                
                # 2. 균등 분배 계산
                equal_share = remaining_cash // max(1, slots_left if effective_slots <= 0 else min(slots_left, effective_slots))
                
                # 3. 최대 제한과 균등 분배 중 작은 값 선택
                slot_cash = min(equal_share, max_allowed_per_stock)
                budget_for_this_stock = int(slot_cash * (1 - buffer))
                
                # 4. 리스크 가드 적용 (enforce_per_ticker_limit)
                if self.trading_guards.get("enforce_per_ticker_limit", False):
                    budget_for_this_stock = min(budget_for_this_stock, max_allowed_per_stock)
                logger.info(f"  -> [{i+1}/{len(plans)}] {name}({ticker}) 배분 예산: {budget_for_this_stock:,.0f}원")
                logger.info(f"      [POSITION_SIZING] max_allowed={max_allowed_per_stock:,.0f}, equal_share={equal_share:,.0f}, final_budget={budget_for_this_stock:,.0f}")

                # 실시간 현재가 및 호가 정보 조회
                price_info = self._get_realtime_price_with_quotes(ticker)
                if not price_info:
                    logger.info(f"  -> [{name}({ticker})] 실시간 가격 조회 실패. 매수를 건너뜁니다.")
                    _notify_embed(create_trade_embed({
                        "side": "BUY", "name": name, "ticker": ticker,
                        "qty": 0, "price": 0, "trade_status": "skipped",
                        "strategy_details": {"reason_code": "PRICE_FETCH_FAILED", "batch": batch_name}
                    }), key=f"phase:buy_skip_price:{ticker}:{self.run_id}", cooldown=120)
                    continue

                current_price = price_info['current_price']
                bid_price = price_info['bid_price']
                ask_price = price_info['ask_price']
                
                logger.info(f"  -> [{name}({ticker})] 현재가: {current_price:,}원, 매수호가: {bid_price:,}원, 매도호가: {ask_price:,}원")

                # 수수료 버퍼 적용한 실예산
                effective_budget = int(budget_for_this_stock * (1 - max(0.0, self.fee_buffer_pct)))
                quantity = int(effective_budget // current_price)  # 임시 수량으로 동적 지정가 계산
                quantity = self._cap_qty_by_fee_buffer(quantity, current_price, budget_for_this_stock)

                # 동적 지정가 계산
                order_price = self._calculate_dynamic_order_price(current_price, bid_price, ask_price, quantity)
                if order_price <= 0:
                    logger.info(f"  -> [{name}({ticker})] 동적 지정가 계산 실패. 스킵.")
                    continue

                # 최종 수량 재계산 (동적 지정가 기준)
                quantity = int(effective_budget // order_price)
                quantity = self._cap_qty_by_fee_buffer(quantity, order_price, budget_for_this_stock)

                # 최소 주문 금액은 **수량×가격**으로 판정
                if quantity == 0 or (self.min_order_cash > 0 and (quantity * order_price) < self.min_order_cash):
                    logger.info(f"  -> [{name}({ticker})] 예산/최소주문 미충족. "
                                f"price={order_price:,}, budget={budget_for_this_stock:,}, "
                                f"qty={quantity}, spent={quantity*order_price:,}, min_order_cash={self.min_order_cash:,}")
                    _notify_embed(create_trade_embed({
                        "side": "BUY", "name": name, "ticker": ticker,
                        "qty": 0, "price": order_price, "trade_status": "skipped",
                        "strategy_details": {
                            "reason_code": "INSUFFICIENT_CASH",
                            "required": int(max(order_price, self.min_order_cash)),
                            "available": int(budget_for_this_stock),
                            "batch": batch_name
                        }
                    }), key=f"phase:buy_insufficient:{ticker}:{self.run_id}", cooldown=120)
                    continue

                pre_qty = 0  # 신규/리밸런스에서는 0 기준
                logger.info(f"  -> 매수 준비: {name}({ticker}), 수량: {quantity}주, 지정가: {order_price:,.0f}원 [{batch_name}]")

                # ① 분할 매수 먼저 시도
                split_ok, spent_est = self._place_split_buy(name, ticker, quantity, order_price, batch_name)
                if split_ok:
                    any_order_placed = True
                    # 분할 경로는 내부에서 스냅샷/통계 반영. remaining_cash는 보수적으로 추정 차감
                    remaining_cash = max(0, remaining_cash - spent_est)
                    logger.info(f"  -> [SPLIT] 남은 예산(추정): {remaining_cash:,.0f}원")
                    continue

                # ② 분할이 아니면 단일 주문 (개선된 로직)
                if self.is_real_trading:
                    # 1차: 동적 지정가로 주문 시도
                    result = self._order_cash_retry(
                        ord_dv="02", pdno=ticker, ord_dvsn="00", ord_qty=str(quantity), ord_unpr=str(int(order_price))
                    )

                    # 향상된 체결 확인 로직 적용
                    execution_result = self._enhanced_execution_check(ticker, result, quantity)
                    
                    if execution_result.get('executed'):
                            # 완전 체결
                            executed_qty = execution_result.get('executed_qty', quantity)
                            wait_time = execution_result.get('wait_time', 0)
                            
                            # 계좌 정보 갱신
                            self._update_account_info(force=True)
                            new_cash, holdings_after, _ = self._load_snapshot()
                            
                            # 매수 통계 반영
                            self.stats["buy"] += 1
                            record_trade({
                                "side": "buy", "ticker": ticker, "name": name,
                                "qty": executed_qty,
                                "price": order_price,
                                "trade_status": "completed",
                                "gpt_analysis": plan,
                                "strategy_details": {
                                    "broker_msg": result.get('msg1'), 
                                    "batch": batch_name,
                                    "execution_time": wait_time
                                }
                            })
                            
                            remaining_cash = new_cash
                            any_order_placed = True
                            
                            _notify_embed(create_trade_embed({
                                "side": "BUY", "name": name, "ticker": ticker,
                                "qty": executed_qty,
                                "price": order_price, 
                                "trade_status": "completed",
                                "strategy_details": {
                                    "broker_msg": result.get('msg1'), 
                                    "batch": batch_name,
                                    "execution_time": f"{wait_time:.1f}초"
                                }
                            }), key=f"phase:buy_success:{ticker}:{self.run_id}", cooldown=30)
                            
                            # 성공 → 실패 카운트 리셋
                            self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=False)
                            
                            # 주문 처리 완료 표시
                            self._mark_order_processed(ticker, batch_name)
                            
                    elif execution_result.get('status') == 'partial':
                        # 부분 체결
                        executed_qty = execution_result.get('executed_qty', 0)
                        logger.warning(f"  -> [{name}({ticker})] 부분 체결: {executed_qty}주/{quantity}주")
                        
                        # 계좌 정보 갱신
                        self._update_account_info(force=True)
                        new_cash, holdings_after, _ = self._load_snapshot()
                        
                        # 부분 체결 기록
                        record_trade({
                            "side": "buy", "ticker": ticker, "name": name,
                            "qty": executed_qty,
                            "price": order_price,
                            "trade_status": "partial",
                            "gpt_analysis": plan,
                            "strategy_details": {
                                "broker_msg": result.get('msg1'), 
                                "batch": batch_name,
                                "execution_time": execution_result.get('wait_time', 0)
                            }
                        })
                        
                        remaining_cash = new_cash
                        any_order_placed = True
                        
                        # 주문 처리 완료 표시 (부분 체결도 처리 완료로 간주)
                        self._mark_order_processed(ticker, batch_name)
                        
                    else:
                        # 체결 실패 - 시장가 주문으로 폴백 (설정 확인)
                        oe = self.trading_params.get("order_execution", {}) or {}
                        market_fallback_enabled = oe.get("market_order_fallback", True)
                        
                        if not market_fallback_enabled:
                            logger.warning(f"  -> [{name}({ticker})] 지정가 체결 실패, 시장가 주문 폴백 비활성화됨 - 주문 실패로 처리")
                            market_result = {"ok": False, "status": "failed", "error": "시장가 폴백 비활성화"}
                        else:
                            logger.warning(f"  -> [{name}({ticker})] 지정가 체결 실패, 시장가 주문으로 전환")
                            # [NEW] 폴백 전 미체결 취소 및 남은 수량만 주문
                            if oe.get("cancel_pendings_before_fallback", True):
                                self._check_and_cancel_pending_orders()
                            # 최신 보유 수량 확인 후 남은 수량 계산
                            _, holdings_after_fb, _ = self._get_optimized_account_info(force=True)
                            current_qty_fb = self._get_qty(holdings_after_fb, ticker)
                            remaining_qty = max(0, quantity - max(0, current_qty_fb - pre_qty))
                            if remaining_qty <= 0:
                                logger.info(f"  -> [{name}({ticker})] 남은 수량 0 → 시장가 폴백 스킵")
                                market_result = {"ok": True}
                            else:
                                market_result = self._execute_market_order(ticker, name, remaining_qty, batch_name)
                        
                        if market_result.get('ok'):
                            # 시장가 주문 성공
                            self._update_account_info(force=True)
                            new_cash, holdings_after, _ = self._load_snapshot()
                            post_qty = self._get_qty(holdings_after, ticker)
                            qty_delta = max(0, post_qty - pre_qty)
                            
                            if qty_delta > 0:
                                self.stats["buy"] += 1
                                record_trade({
                                    "side": "buy", "ticker": ticker, "name": name,
                                    "qty": qty_delta,
                                    "price": 0,  # 시장가는 가격 0
                                    "trade_status": "completed",
                                    "gpt_analysis": plan,
                                    "strategy_details": {
                                        "broker_msg": market_result.get('msg1'), 
                                        "batch": batch_name,
                                        "order_type": "market"
                                    }
                                })
                                
                                remaining_cash = new_cash
                                any_order_placed = True
                                
                                # 주문 처리 완료 표시
                                self._mark_order_processed(ticker, batch_name)
                                
                                _notify_embed(create_trade_embed({
                                    "side": "BUY", "name": name, "ticker": ticker,
                                    "qty": qty_delta,
                                    "price": 0, 
                                    "trade_status": "completed",
                                    "strategy_details": {
                                        "broker_msg": market_result.get('msg1'), 
                                        "batch": batch_name,
                                        "order_type": "market"
                                    }
                                }), key=f"phase:buy_market_success:{ticker}:{self.run_id}", cooldown=30)
                                
                                self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=False)
                            else:
                                # 시장가 주문도 실패 - 최후순위로 분류
                                self._mark_as_low_priority(ticker)
                                self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=True)
                        else:
                            # 시장가 주문도 실패 - 최후순위로 분류
                            logger.error(f"  -> [{name}({ticker})] 시장가 주문도 실패, 최후순위로 분류")
                            self._mark_as_low_priority(ticker)
                            self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=True)
                    
                    # 지정가 주문 자체가 실패한 경우 시장가 주문으로 폴백 (설정 확인)
                    if not execution_result.get('executed') and execution_result.get('status') != 'partial':
                        err = result.get('msg1', 'Unknown error')
                        oe = self.trading_params.get("order_execution", {}) or {}
                        market_fallback_enabled = oe.get("market_order_fallback", True)
                        
                        if not market_fallback_enabled:
                            logger.warning(f"  -> [{name}({ticker})] 지정가 주문 실패: {err}, 시장가 주문 폴백 비활성화됨 - 주문 실패로 처리")
                            market_result = {"ok": False, "status": "failed", "error": "시장가 폴백 비활성화"}
                        else:
                            logger.warning(f"  -> [{name}({ticker})] 지정가 주문 실패: {err}, 시장가 주문으로 전환")
                            # [NEW] 폴백 전 미체결 취소 및 남은 수량만 주문
                            if oe.get("cancel_pendings_before_fallback", True):
                                self._check_and_cancel_pending_orders()
                            _, holdings_after_fb2, _ = self._get_optimized_account_info(force=True)
                            current_qty_fb2 = self._get_qty(holdings_after_fb2, ticker)
                            remaining_qty2 = max(0, quantity - max(0, current_qty_fb2 - pre_qty))
                            if remaining_qty2 <= 0:
                                logger.info(f"  -> [{name}({ticker})] 남은 수량 0 → 시장가 폴백 스킵")
                                market_result = {"ok": True}
                            else:
                                market_result = self._execute_market_order(ticker, name, remaining_qty2, batch_name)
                        
                        if market_result.get('ok'):
                            # 시장가 주문 성공
                            self._update_account_info(force=True)
                            new_cash, holdings_after, _ = self._load_snapshot()
                            post_qty = self._get_qty(holdings_after, ticker)
                            qty_delta = max(0, post_qty - pre_qty)
                            
                            if qty_delta > 0:
                                self.stats["buy"] += 1
                                record_trade({
                                    "side": "buy", "ticker": ticker, "name": name,
                                    "qty": qty_delta,
                                    "price": 0,
                                    "trade_status": "completed",
                                    "gpt_analysis": plan,
                                    "strategy_details": {
                                        "broker_msg": market_result.get('msg1'), 
                                        "batch": batch_name,
                                        "order_type": "market"
                                    }
                                })
                                
                                remaining_cash = new_cash
                                any_order_placed = True
                                
                                _notify_embed(create_trade_embed({
                                    "side": "BUY", "name": name, "ticker": ticker,
                                    "qty": qty_delta,
                                    "price": 0, 
                                    "trade_status": "completed",
                                    "strategy_details": {
                                        "broker_msg": market_result.get('msg1'), 
                                        "batch": batch_name,
                                        "order_type": "market"
                                    }
                                }), key=f"phase:buy_market_success:{ticker}:{self.run_id}", cooldown=30)
                                
                                self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=False)
                            else:
                                # 시장가 주문도 실패 - 최후순위로 분류
                                self._mark_as_low_priority(ticker)
                                self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=True)
                        else:
                            # 시장가 주문도 실패 - 최후순위로 분류
                            logger.error(f"  -> [{name}({ticker})] 시장가 주문도 실패, 최후순위로 분류")
                            self._mark_as_low_priority(ticker)
                            self._maybe_add_cooldown(ticker, "매수 주문 실패", increment_fail=True)
                else:
                    actual_spent = quantity * order_price
                    remaining_cash -= actual_spent
                    any_order_placed = True
                    # ✅ 매수 통계 반영(모의)
                    self.stats["buy"] += 1
                    record_trade({
                        "side": "buy", "ticker": ticker, "name": name,
                        "qty": quantity, "price": order_price, "trade_status": "completed",
                        "gpt_analysis": plan,
                        "strategy_details": {"batch": batch_name}
                    })
                    logger.info(f"  -> [모의] {name}({ticker}) {quantity}주 @{order_price:,.0f}원 지정가 매수 실행. [{batch_name}]")
                    _notify_text(
                        f" [모의] BUY {name}({ticker}) x{quantity} @ {order_price:,.0f} [{batch_name}]",
                        key=f"phase:paper_buy:{ticker}:{self.run_id}", cooldown=30
                    )

                logger.info(f"  -> 남은 예산: {remaining_cash:,.0f}원")
                # time.sleep(0.3)  # 불필요한 대기 시간 제거

        # 1) 추가매수 먼저
        _execute_buy_batch(rebuy_candidates, batch_name="REBUY")

        # 2) 신규 진입: 슬롯 확인 (동적 슬롯 반영)
        current_slots_used = len({self._t(h.get("pdno", "")) for h in holdings if _to_int(h.get("hldg_qty", 0)) > 0})
        slots_to_fill = max(0, effective_slots - current_slots_used)

        if slots_to_fill <= 0:
            # 보유 슬롯이 꽉 찬 경우, rotation.enabled=true일 때만 교체(리밸런싱) 진입
            if not self._is_rotation_enabled():
                self._set_summary_reason("SKIPPED_NO_SLOT_ROTATION_DISABLED",
                                         f"effective_slots={effective_slots} current_slots_used={current_slots_used}")
                logger.info("신규 슬롯 없음 & 회전매매 비활성화(rotation.enabled=false) → 신규 매수 생략")
                return

            # 보유 최저 점수 vs 신규 후보 비교 → 교체(리밸런싱)
            logger.info("=== 회전 매매(리밸런싱) 시작 ===")
            
            # GPT 분석 결과 로드
            gpt_decisions = self._load_gpt_analysis_results()
            
            # 통합 분석 시스템 사용
            if self.integrated_analysis.get("enabled", True):
                try:
                    to_sell_list, to_buy_plans = self._get_enhanced_rebalance_candidates(holdings, gpt_decisions)
                except Exception as e:
                    logger.error(f"향상된 리밸런싱 후보 선정 실패: {e}", exc_info=True)
                    # 폴백: 기존 로직 사용
                    to_buy_plans, to_sell_list = self._determine_rebalance_swaps([], holdings)
            else:
                # 기존 로직 (하위 호환성)
                to_buy_plans, to_sell_list = self._determine_rebalance_swaps([], holdings)
            if to_sell_list:
                logger.info(f"회전 매매 대상 발견: 매도 {len(to_sell_list)}건, 매수 {len(to_buy_plans)}건")
                
                # 의사결정 투명성 로깅
                if self.integrated_analysis.get("decision_transparency", {}).get("enabled", False):
                    self._log_decision_transparency(to_sell_list, to_buy_plans, gpt_decisions)
                # ── 정책1: 스왑 전 시뮬레이션 ───────────────────────────
                expected_proceeds = 0
                # est_proceeds가 없으면 보유 목록에서 추정
                h_pr_map = {self._t(h.get("pdno", "")): (_to_int(h.get("prpr", 0)), _to_int(h.get("hldg_qty", 0))) for h in holdings}
                for s in to_sell_list:
                    # 데이터 검증 강화
                    if not isinstance(s, dict):
                        logger.warning(f"잘못된 to_sell_list 항목 타입: {type(s)} - {s}")
                        continue
                    if "ticker" not in s or not s["ticker"]:
                        logger.warning(f"ticker 키가 없거나 비어있는 to_sell_list 항목: {s}")
                        continue
                    if "qty" not in s:
                        logger.warning(f"qty 키가 없는 to_sell_list 항목: {s}")
                        continue
                    
                    t = s["ticker"]
                    qty = int(s["qty"])
                    if "est_proceeds" in s:
                        expected_proceeds += int(s["est_proceeds"])
                    else:
                        pr, _ = h_pr_map.get(t, (0, 0))
                        expected_proceeds += pr * qty

                expected_cash = int(available_cash + expected_proceeds)
                eff_after = self._compute_effective_slots(expected_cash) if self.trading_guards.get("auto_shrink_slots", False) else self.max_positions
                slots_after = max(0, eff_after - (current_slots_used - len(to_sell_list)))

                # 최소 한 종목이라도 매수 가능할지(버퍼/최소주문 고려) 점검
                feasible_buy = False
                if to_buy_plans:
                    first = to_buy_plans[0]
                    info = first.get("stock_info", {})
                    for k, dv in SCHEMA_DEFAULTS.items():
                        info.setdefault(k, dv)
                    px = _to_int(info.get("Price", 0))
                    budget_after = int(expected_cash * (1 - buffer))
                    qty1 = self._cap_qty_by_fee_buffer(budget_after // px if px > 0 else 0, px, budget_after) if px > 0 else 0
                    feasible_buy = (px > 0) and (qty1 >= 1) and ((self.min_order_cash <= 0) or (qty1 * px >= self.min_order_cash))

                logger.info(
                    f"[SWAP-SIM] expected_cash={expected_cash:,}, eff_slots_after={eff_after}, "
                    f"slots_after={slots_after}, feasible_buy={'YES' if feasible_buy else 'NO'}"
                )

                if slots_after < 1 or not feasible_buy:
                    logger.info("스왑 사전 시뮬레이션 결과: 매수 불가 혹은 슬롯 부족 → 리밸런스 매도 전체 취소.")
                    _notify_text("⚠️ 리밸런스 취소: 매수 불가/슬롯 부족(정책1 시뮬레이션)", key=f"phase:rebalance_sim_cancel:{self.run_id}", cooldown=120)
                else:
                    logger.info(f"회전 매매 실행: 매도 {len(to_sell_list)}건 → 매수 {len(to_buy_plans)}건")
                    _notify_text(f" 리밸런싱 매도 {len(to_sell_list)}건 실행", key=f"phase:rebalance_sell_batch:{self.run_id}", cooldown=120)
                    
                    # 매도 주문 상세 로깅
                    for i, s in enumerate(to_sell_list):
                        old_score = s.get('old_score', 0.0)
                        new_score = s.get('new_score', 0.0)
                        new_ticker = s.get('new_ticker', 'N/A')
                        logger.info(f"  -> 매도 [{i+1}/{len(to_sell_list)}] {s['name']}({s['ticker']}) {s['qty']}주 "
                                   f"[스코어: {old_score:.3f} → {new_score:.3f}, "
                                   f"섹터: {s.get('sector', 'N/A')}, RSI: {s.get('rsi', 0):.1f}]")
                        _ = self._execute_market_sell(
                            ticker=s["ticker"],
                            quantity=s["qty"],
                            name=s["name"],
                            reason_text=f"REBALANCE_SWAP (old={old_score:.3f} → new={new_score:.3f} for {new_ticker})",
                            reason_code="REBALANCE_SWAP"
                        )
                        # time.sleep(0.5)  # 불필요한 대기 시간 제거

                    # 매도 후 최신 잔고/현금 재조회 및 슬롯 재계산
                    time.sleep(1)  # 2초 -> 1초로 단축
                    self._update_account_info(force=True)
                    new_cash, holdings_after, _ = self._load_snapshot()
                    eff_now = self._compute_effective_slots(new_cash) if self.trading_guards.get("auto_shrink_slots", False) else self.max_positions
                    slots_now = max(0, eff_now - len(holdings_after))

                    logger.info(f"[SWAP-AFTER] new_cash={new_cash:,}, eff_slots_now={eff_now}, slots_now={slots_now}")
                    if slots_now > 0:
                        buy_now = to_buy_plans[:slots_now]
                        logger.info(f"회전 매매 매수 시작: {len(buy_now)}개 종목 (슬롯: {slots_now}개)")
                        
                        # 매수 대상 상세 로깅
                        for i, plan in enumerate(buy_now):
                            info = plan.get("stock_info", {})
                            ticker = self._t(info.get("Ticker", ""))
                            name = info.get("Name", "N/A")
                            score = info.get("Score", 0.0)
                            logger.info(f"  -> 매수 [{i+1}/{len(buy_now)}] {name}({ticker}) [스코어: {score:.3f}]")
                        
                        _execute_buy_batch(buy_now, batch_name="REBALANCE_NEW")
                        logger.info("회전 매매 매수 완료")
                        logger.info("=== 회전 매매(리밸런싱) 완료 ===")
                    else:
                        logger.info("리밸런스 이후에도 신규 슬롯이 없어 매수 생략.")
                        _notify_text("ℹ️ 리밸런스 후 신규 슬롯 0 → 매수 생략", key=f"phase:rebalance_no_slots_after:{self.run_id}", cooldown=120)
                        logger.info("=== 회전 매매(리밸런싱) 완료 ===")
            else:
                logger.info("회전 매매 기준을 충족하는 교체 대상이 없어 신규 매수는 생략합니다.")
                logger.info("=== 회전 매매(리밸런싱) 완료 ===")

        else:
            targets_to_buy = new_targets[:slots_to_fill] if buy_plans else []
            if not targets_to_buy:
                logger.info("신규로 매수할 최종 대상이 없습니다.")
                _notify_text("ℹ️ 신규 매수 대상 없음",
                             key=f"phase:no_targets:{self.run_id}", cooldown=300)
            else:
                self._execute_sequential_buy(targets_to_buy, available_cash, holdings)

        if any_order_placed:
            time.sleep(3)  # 5초 -> 3초로 단축
            self._update_account_info(force=True)

    def _get_realtime_price_parallel(self, ticker: str) -> Optional[Dict]:
        """병렬 처리를 위한 가격 조회 래퍼 함수"""
        try:
            return self._get_realtime_price_with_quotes(ticker)
        except Exception as e:
            logger.warning(f"[{ticker}] 병렬 가격 조회 실패: {e}")
            return None

    def _execute_sequential_buy(self, targets: List[Dict], available_cash: int, holdings: List[Dict]) -> None:
        """순차적 매수 실행 (실패 시 재시도 및 금액 재배분)"""
        if not targets:
            return
            
        logger.info(f"순차적 매수 시작: {len(targets)}개 종목, 예산: {available_cash:,}원")
        
        # 병렬로 모든 종목의 가격 정보 미리 조회
        logger.info("병렬 가격 조회 시작...")
        price_cache = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_ticker = {
                executor.submit(self._get_realtime_price_parallel, self._t(plan.get("stock_info", {}).get("Ticker", ""))): 
                self._t(plan.get("stock_info", {}).get("Ticker", "")) 
                for plan in targets
            }
            
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    price_info = future.result()
                    if price_info:
                        price_cache[ticker] = price_info
                except Exception as e:
                    logger.warning(f"[{ticker}] 가격 조회 실패: {e}")
        
        logger.info(f"가격 조회 완료: {len(price_cache)}/{len(targets)}개 종목")
        
        remaining_cash = available_cash
        successful_purchases = []
        failed_targets = []
        max_rounds = 3  # 최대 3라운드까지 재시도
        
        for round_num in range(max_rounds):
            if not targets:
                break
                
            logger.info(f"=== 매수 라운드 {round_num + 1} 시작 (대상: {len(targets)}개) ===")
            
            round_success = False
            round_targets = targets.copy()
            
            for i, plan in enumerate(round_targets):
                if remaining_cash <= max(self.min_order_cash, self.min_cash_reserve):
                    logger.info(f"잔여 현금 부족({remaining_cash:,}원)으로 매수 중단")
                    break
                    
                info = plan.get("stock_info", {})
                for k, dv in SCHEMA_DEFAULTS.items():
                    info.setdefault(k, dv)
                ticker, name = self._t(info.get("Ticker", "")), info.get("Name", "N/A")
                
                # 개선된 포지션 사이징 로직 (회전 매매용)
                slots_left = len(round_targets) - i
                
                # 1. 종목당 최대 투입 가능 금액 계산 (per_ticker_max_weight 적용)
                max_allowed_per_stock = int(remaining_cash * self.per_ticker_max_weight)
                
                # 2. 균등 분배 계산
                equal_share = remaining_cash // max(1, slots_left)
                
                # 3. 최대 제한과 균등 분배 중 작은 값 선택
                slot_cash = min(equal_share, max_allowed_per_stock)
                budget_for_this_stock = int(slot_cash * 0.95)  # 5% 버퍼
                
                # 4. 리스크 가드 적용
                if self.trading_guards.get("enforce_per_ticker_limit", False):
                    budget_for_this_stock = min(budget_for_this_stock, max_allowed_per_stock)
                
                logger.info(f"  -> [{i+1}/{len(round_targets)}] {name}({ticker}) 배분 예산: {budget_for_this_stock:,}원")
                logger.info(f"      [ROTATION_SIZING] max_allowed={max_allowed_per_stock:,}, equal_share={equal_share:,}, final_budget={budget_for_this_stock:,}")
                
                # 매수 시도
                success = self._try_single_buy(plan, budget_for_this_stock, round_num + 1)
                
                if success:
                    successful_purchases.append(plan)
                    # 계좌 정보 업데이트 및 잔여 현금 재계산 (배치 처리로 최적화)
                    time.sleep(0.5)  # 1초 -> 0.5초로 단축
                    self._batch_update_account_info()  # force=True 대신 배치 처리 사용
                    new_cash, new_holdings, _ = self._load_snapshot()
                    remaining_cash = new_cash
                    round_success = True
                    logger.info(f"  -> ✅ {name}({ticker}) 매수 성공, 잔여 현금: {remaining_cash:,}원")
                else:
                    failed_targets.append(plan)
                    logger.info(f"  -> ❌ {name}({ticker}) 매수 실패")
                
                # 성공한 종목은 다음 라운드에서 제외
                if success:
                    targets.remove(plan)
            
            if not round_success:
                logger.info(f"라운드 {round_num + 1}에서 모든 매수가 실패하여 중단")
                break
                
            # 다음 라운드를 위해 실패한 종목들을 다시 시도
            if failed_targets and round_num < max_rounds - 1:
                targets = failed_targets.copy()
                failed_targets = []
                logger.info(f"실패한 {len(targets)}개 종목으로 다음 라운드 진행")
        
        logger.info(f"순차적 매수 완료: 성공 {len(successful_purchases)}개, 실패 {len(failed_targets)}개")
    def _try_single_buy(self, plan: Dict, budget: int, round_num: int) -> bool:
        """단일 종목 매수 시도"""
        info = plan.get("stock_info", {})
        ticker, name = self._t(info.get("Ticker", "")), info.get("Name", "N/A")
        
        try:
            # 실시간 가격 조회
            price_info = self._get_realtime_price_with_quotes(ticker)
            if not price_info:
                logger.warning(f"  -> [{name}({ticker})] 가격 조회 실패")
                return False
            
            current_price = price_info['current_price']
            bid_price = price_info['bid_price']
            ask_price = price_info['ask_price']

            if not current_price or current_price <= 0:
                logger.warning(f"  -> [{name}({ticker})] 현재가가 0이거나 없음. 매수를 건너뜁니다.")
                return False

            # 수량 계산 (더 유연한 로직)
            effective_budget = int(budget * (1 - max(0.0, self.fee_buffer_pct)))
            quantity = int(effective_budget // current_price)
            quantity = self._cap_qty_by_fee_buffer(quantity, current_price, budget)
            
            # 수량이 0이면 최소 주문 금액을 고려하여 재시도
            if quantity <= 0:
                # 최소 주문 금액을 고려한 수량 계산
                min_qty_by_price = max(1, int(self.min_order_cash // current_price)) if self.min_order_cash > 0 else 1
                if min_qty_by_price * current_price <= budget:
                    quantity = min_qty_by_price
                    logger.info(f"  -> [{name}({ticker})] 최소 주문 금액 기준으로 수량 조정: {quantity}주")
                else:
                    logger.warning(f"  -> [{name}({ticker})] 수량 계산 실패 (예산: {budget:,}원, 최소주문: {self.min_order_cash:,}원)")
                    return False
            
            # 동적 지정가 계산
            order_price = self._calculate_dynamic_order_price(current_price, bid_price, ask_price, quantity)
            if order_price <= 0:
                logger.warning(f"  -> [{name}({ticker})] 지정가 계산 실패")
                return False
            
            # 최종 수량 재계산
            quantity = int(effective_budget // order_price)
            quantity = self._cap_qty_by_fee_buffer(quantity, order_price, budget)
            
            if quantity <= 0 or (self.min_order_cash > 0 and (quantity * order_price) < self.min_order_cash):
                logger.warning(f"  -> [{name}({ticker})] 최소 주문 금액 미충족")
                return False
            
            logger.info(f"  -> 매수 준비: {name}({ticker}), 수량: {quantity}주, 지정가: {order_price:,}원 [라운드{round_num}]")
            
            # 예산이 충분하면 분할 매수 시도, 아니면 단일 주문
            min_cash_per_slice = self.trading_params.get("split_buy", {}).get("min_cash_per_slice", 50000)
            total_order_value = quantity * order_price
            
            if total_order_value >= min_cash_per_slice * 3:  # 분할 매수 가능한 경우
                # 지정가 주문 시도 (분할 매수 사용)
                split_ok, spent_est = self._place_split_buy(name, ticker, quantity, order_price, "NEW")
                
                if split_ok:
                    logger.info(f"  -> ✅ 분할 지정가 주문 성공: {name}({ticker})")
                    return True
                else:
                    # 분할 매수가 거절되었지만, 시간차를 두고 실제 체결 여부 확인
                    logger.warning(f"  -> 분할 지정가 주문 응답이 거절되었지만, 실제 체결 여부를 확인합니다: {name}({ticker})")
                    
                    # 지연 체결 확인 (개선된 버전) - 분할 주문의 마지막 ODNO 확인
                    last_odno = getattr(self, '_last_order_odno', None)
                    delayed_execution = self._check_delayed_execution(ticker, quantity, last_odno)
                    if delayed_execution:
                        executed_qty = delayed_execution.get('executed_qty', 0)
                        # 정확한 로그 출력
                        logger.info(f"  -> ✅ 분할 지정가 주문 실제 체결 확인: {name}({ticker}) {executed_qty}주 (지연 확인)")
                        
                        # 정확한 거래 기록
                        record_trade({
                            "side": "buy", "ticker": ticker, "name": name,
                            "qty": executed_qty, "price": order_price, "trade_status": "split_executed",
                            "strategy_details": {"batch": "NEW", "order_type": "split", "delayed_confirmation": True}
                        })
                        
                        # 통계 업데이트
                        self.stats["buy"] += 1
                        return True
                    else:
                        logger.warning(f"  -> 분할 지정가 주문 실패 확인, 단일 지정가 주문 시도: {name}({ticker})")
            else:
                logger.info(f"  -> 예산 부족으로 단일 지정가 주문 시도: {name}({ticker})")
            
            # 단일 지정가 주문 시도
            if self.is_real_trading:
                logger.info(f"  -> 단일 지정가 주문 시도: {name}({ticker}) {quantity}주 @ {order_price:,}원")
                res = self._order_cash_retry(
                    ord_dv="02", pdno=ticker, ord_dvsn="00",
                    ord_qty=str(quantity), ord_unpr=str(int(order_price))
                )
                logger.info(f"  -> 주문 응답: {res}")
                
                # ok=True + ODNO 있으면 주문 접수 성공
                if res.get('ok') and res.get('odno'):
                    # 향상된 체결 확인 로직 적용
                    execution_result = self._enhanced_execution_check(ticker, res, quantity)
                    
                    if execution_result.get('executed'):
                        executed_qty = execution_result.get('executed_qty', quantity)
                        logger.info(f"  -> ✅ 단일 지정가 주문 성공: {name}({ticker}) {executed_qty}주 (확인방식: {execution_result.get('status', 'unknown')})")
                        record_trade({
                            "side": "buy", "ticker": ticker, "name": name,
                            "qty": executed_qty, "price": order_price, "trade_status": "limit_executed",
                            "strategy_details": {"broker_msg": res.get('msg1'), "batch": "NEW", "order_type": "limit"}
                        })
                        
                        # 통계 업데이트
                        self.stats["buy"] += 1
                        
                        return True
                    else:
                        # 주문 접수 성공, 미체결 상태
                        logger.info(f"  -> 단일 지정가 주문 접수 완료: {name}({ticker}) {quantity}주 @ {order_price:,}원 (미체결, 15시 20분 체결 확인)")
                        odno = self._extract_order_id(res)
                        if odno:
                            record_trade({
                                "side": "buy", "ticker": ticker, "name": name,
                                "qty": quantity, "price": order_price, "trade_status": "pending",
                                "order_id": odno,
                                "requested_qty": quantity,
                                "executed_qty": 0,
                                "strategy_details": {"broker_msg": res.get('msg1'), "batch": "NEW", "odno": odno}
                            })
                        return True  # 주문 접수 성공으로 간주하고 종료
                else:
                    # ok=False인 경우에만 거절로 간주하고 체결 확인 시도
                    logger.warning(f"  -> 단일 지정가 주문 거절 응답, 실제 체결 여부 확인: {name}({ticker})")
                    
                    # 지연 체결 확인 (개선된 버전) - 주문 응답의 ODNO 사용
                    odno = res.get('odno')
                    delayed_execution = self._check_delayed_execution(ticker, quantity, odno)
                    if delayed_execution:
                        executed_qty = delayed_execution.get('executed_qty', 0)
                        # 정확한 로그 출력
                        logger.info(f"  -> ✅ 단일 지정가 주문 실제 체결 확인: {name}({ticker}) {executed_qty}주 (지연 확인)")
                        
                        # 정확한 거래 기록
                        record_trade({
                            "side": "buy", "ticker": ticker, "name": name,
                            "qty": executed_qty, "price": order_price, "trade_status": "limit_executed",
                            "strategy_details": {"batch": "NEW", "order_type": "limit", "delayed_confirmation": True}
                        })
                        
                        # 통계 업데이트
                        self.stats["buy"] += 1
                        return True
                    else:
                        # 단일 지정가 주문이 실패로 확인됨 - 시장가 폴백 전 이전 주문 체결 확인
                        logger.warning(f"  -> 단일 지정가 주문 실패 확인: {res.get('msg1', 'Unknown error')}")
                        
                        # 시장가 폴백 전, 이전에 실행한 모든 주문(분할/단일)의 체결 여부 최종 확인
                        _, holdings_after, _ = self._get_optimized_account_info(force=True)
                        current_qty = self._get_qty(holdings_after, ticker)
                        # 매수 시작 전 보유 수량과 비교 (함수 시작 시점의 보유 수량 필요)
                        # 간단히: 현재 보유 수량이 목표 수량 이상이면 이미 체결된 것으로 간주
                        if current_qty >= quantity:
                            logger.info(f"  -> ✅ 이전 주문 체결 확인: {name}({ticker}) {current_qty}주 (시장가 폴백 불필요)")
                            record_trade({
                                "side": "buy", "ticker": ticker, "name": name,
                                "qty": quantity, "price": order_price, "trade_status": "limit_executed",
                                "strategy_details": {"batch": "NEW", "order_type": "limit", "delayed_confirmation": True}
                            })
                            self.stats["buy"] += 1
                            return True
                        
                        logger.warning(f"  -> 대체 주문 절차 진행: {name}({ticker})")
            else:
                logger.info(f"  -> [모의] 단일 지정가 주문 성공: {name}({ticker})")
                record_trade({
                    "side": "buy", "ticker": ticker, "name": name,
                    "qty": quantity, "price": order_price, "trade_status": "completed",
                    "strategy_details": {"batch": "NEW"}
                })
                
                # 통계 업데이트
                self.stats["buy"] += 1
                
                return True
            
            # 1) 기존 미체결 취소 시도(티커 단위)
            try:
                cancelled = []
                if hasattr(self.kis, 'cancel_all_pending_orders'):
                    cancelled = self.kis.cancel_all_pending_orders(ticker=ticker)
                if cancelled:
                    logger.info(f"  -> 미체결 {len(cancelled)}건 취소 완료 후 대체 주문 진행: {name}({ticker})")
            except Exception as e:
                logger.debug(f"  -> 미체결 취소 중 오류(무시하고 진행): {e}")

            # 2) 시장성 지정가(호가 기반)로 대체 주문 시도
            marketable_limit_ok = False
            try:
                quote = self._get_realtime_price_with_quotes(ticker)
                if quote and isinstance(quote, dict):
                    best_ask = int(quote.get('ask_price', 0) or quote.get('ask', 0) or 0)
                    best_bid = int(quote.get('bid_price', 0) or quote.get('bid', 0) or 0)
                    from utils import get_tick_size, round_to_tick
                    if best_ask > 0:
                        # 매수: 최우선 매도호가 또는 +1틱
                        px = best_ask
                        try:
                            px = round_to_tick(best_ask + get_tick_size(best_ask), mode="up")
                        except Exception:
                            pass
                        logger.info(f"  -> 시장성 지정가 주문 시도: {name}({ticker}) {quantity}주 @ {px:,}원")
                        alt_res = self._order_cash_retry(
                            ord_dv="02", pdno=ticker, ord_dvsn="00",
                            ord_qty=str(quantity), ord_unpr=str(int(px))
                        )
                        exec_res = self._enhanced_execution_check(ticker, alt_res, quantity)
                        if exec_res.get('executed'):
                            executed_qty = exec_res.get('executed_qty', quantity)
                            logger.info(f"  -> ✅ 시장성 지정가 주문 성공: {name}({ticker}) {executed_qty}주")
                            record_trade({
                                "side": "buy", "ticker": ticker, "name": name,
                                "qty": executed_qty, "price": px, "trade_status": "limit_executed",
                                "strategy_details": {"batch": "NEW", "order_type": "marketable_limit"}
                            })
                            self.stats["buy"] += 1
                            return True
                        else:
                            logger.warning(f"  -> 시장성 지정가 미체결 → 최종 폴백 검토: {name}({ticker})")
                else:
                    logger.debug("  -> 호가 조회 실패, 시장성 지정가 스킵")
            except Exception as e:
                logger.debug(f"  -> 시장성 지정가 처리 중 예외: {e}")

            # 3) 최종 폴백: 시장가 주문 시도(설정 확인)
            oe = self.trading_params.get("order_execution", {}) or {}
            market_fallback_enabled = oe.get("market_order_fallback", True)
            
            if not market_fallback_enabled:
                logger.warning(f"  -> 시장가 주문 폴백 비활성화됨 - 모든 주문 실패로 처리: {name}({ticker})")
                logger.error(f"  -> ❌ 모든 주문 실패: {name}({ticker}) - 시장가 폴백 비활성화")
                return False
            else:
                # 매수 시작 시점의 보유 수량 확인 (중복 체결 방지)
                _, holdings_before_market, _ = self._get_optimized_account_info(force=True)
                pre_qty_market = self._get_qty(holdings_before_market, ticker)
                market_result = self._execute_market_order(ticker, name, quantity, "NEW", pre_qty=pre_qty_market)
                
                if market_result and market_result.get("status") == "executed":
                    logger.info(f"  -> ✅ 시장가 주문 성공: {name}({ticker})")
                    
                    # 통계 업데이트
                    self.stats["buy"] += 1
                    
                    return True
                else:
                    logger.error(f"  -> ❌ 모든 주문 실패: {name}({ticker}) - 시장가 주문도 실패")
                    return False
                    
        except Exception as e:
            logger.error(f"  -> [{name}({ticker})] 매수 시도 중 오류: {type(e).__name__}: {str(e)}")
            logger.debug(f"  -> [{name}({ticker})] 매수 시도 상세 오류:", exc_info=True)
            return False

    # ── 리밸런싱 페어링 (점수 기반 + Δscore 임계치) ───────────────────
    def _determine_rebalance_swaps(self, new_targets: List[Dict], holdings: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        logger.debug(f"[DEBUG] _determine_rebalance_swaps 시작 - new_targets: {len(new_targets)}개, holdings: {len(holdings)}개")
        
        # 보유 종목 스코어 현행화
        updated_holdings = self._get_updated_holdings_with_scores(holdings)
        logger.debug(f"[DEBUG] 보유 종목 스코어 현행화 완료: {len(updated_holdings)}개")
        
        # 기존 스코어 캐시도 로드 (백업용)
        scores_map, score_file = self._load_latest_scores()
        logger.debug(f"[DEBUG] 스코어 캐시 로드 완료: {len(scores_map)}개")
        
        # 최소 보유일 설정 로드
        min_holding_days = self.settings.get("rotation", {}).get("min_holding_days", 0)
        current_time = datetime.now(KST)
        logger.debug(f"[DEBUG] 최소 보유일: {min_holding_days}일")
        
        # 보유 (ticker, name, qty, score, prpr, current_score, screener_info)
        held: List[Tuple[str, str, int, float, int, float, Dict]] = []
        excluded_count = 0
        for h in updated_holdings:
            t = self._t(h.get("pdno", ""))
            nm = h.get("prdt_name", "N/A")
            qty = _to_int(h.get("hldg_qty", 0))
            
            # 최소 보유일 체크
            if min_holding_days > 0:
                is_eligible, holding_days = check_min_holding_period(t, min_holding_days, current_time)
                if not is_eligible:
                    logger.info(f"최소 보유일 미충족으로 리밸런싱 제외: {nm}({t}) - {min_holding_days}일 미만")
                    excluded_count += 1
                    continue
            
            # 현재 스코어 우선, 없으면 캐시 스코어 사용
            current_score = h.get('current_score', 0.0)
            cached_score = float(scores_map.get(t, 0.0)) if scores_map else 0.0
            sc = current_score if current_score > 0 else cached_score
            pr = _to_int(h.get("prpr", 0))
            
            # screener_holdings 상세 정보 추출
            screener_info = {
                'sector': h.get('screener_sector', ''),
                'fin_score': h.get('fin_score', 0.0),
                'tech_score': h.get('tech_score', 0.0),
                'mkt_score': h.get('mkt_score', 0.0),
                'sector_score': h.get('sector_score', 0.0),
                'vol_kki': h.get('vol_kki', 0.0),
                'pos_52w': h.get('pos_52w', 0.0),
                'rsi': h.get('rsi', 0.0),
                'ma50': h.get('ma50', 0.0),
                'ma200': h.get('ma200', 0.0),
                'updated_at': h.get('updated_at', '')
            }
            
            if qty > 0:
                held.append((t, nm, qty, sc, pr, current_score, screener_info))
                logger.debug(f"[DEBUG] 보유 종목 추가: {nm}({t}) - qty:{qty}, score:{sc:.3f}, price:{pr}")
        
        logger.debug(f"[DEBUG] 보유 종목 처리 완료: {len(held)}개 (제외: {excluded_count}개)")
        if not held:
            logger.debug(f"[DEBUG] 보유 종목이 없어 빈 결과 반환")
            return [], []

        # 신규 후보: (score, plan) — gpt 기반이 없으면 all_stock_data 상위 점수를 사용
        new_list: List[Tuple[float, Dict]] = []
        if new_targets:
            logger.debug(f"[DEBUG] new_targets 사용: {len(new_targets)}개")
            for plan in new_targets:
                info = plan.get("stock_info", {})
                sc = _to_float(info.get("Score"), 0.0)
                new_list.append((float(sc), plan))
                logger.debug(f"[DEBUG] 신규 후보 추가: {info.get('Name', 'N/A')}({info.get('Ticker', 'N/A')}) - 점수:{sc:.3f}")
        else:
            logger.debug(f"[DEBUG] all_stock_data에서 후보 구성")
            # all_stock_data에서 점수 높은 상위 후보 구성
            tmp = []
            for t, rec in self.all_stock_data.items():
                sc = _to_float(rec.get("Score"), 0.0)
                if sc > 0:
                    tmp.append((sc, {"stock_info": rec}))
            new_list = sorted(tmp, key=lambda x: x[0], reverse=True)[:max(1, len(held))]
            logger.debug(f"[DEBUG] all_stock_data 후보: {len(new_list)}개 (상위 {max(1, len(held))}개)")

        if not new_list:
            logger.debug(f"[DEBUG] 신규 후보가 없어 빈 결과 반환")
            return [], []

        held_sorted = sorted(held, key=lambda x: x[3])              # 점수 오름차순(최약체 우선)
        new_sorted = sorted(new_list, key=lambda x: x[0], reverse=True)  # 후보 고점수 우선
        logger.debug(f"[DEBUG] 정렬 완료 - 보유: {len(held_sorted)}개, 신규: {len(new_sorted)}개")

        delta_thr = float(self.settings.get("rotation", {}).get("delta_score_min", 0.10))
        logger.debug(f"[DEBUG] 점수 차이 임계치: {delta_thr:.3f}")

        to_buy: List[Dict] = []
        to_sell: List[Dict] = []

        hi = 0
        for new_score, plan in new_sorted:
            if hi >= len(held_sorted):
                logger.debug(f"[DEBUG] 모든 보유 종목 처리 완료 (hi:{hi} >= held:{len(held_sorted)})")
                break
            worst_t, worst_name, worst_qty, worst_score, worst_pr, worst_current_score, worst_screener_info = held_sorted[hi]
            
            # 현재 스코어와 신규 스코어 비교
            current_worst_score = worst_current_score if worst_current_score > 0 else worst_score
            score_delta = new_score - current_worst_score
            
            logger.debug(f"[DEBUG] 매칭 시도 {hi}: {worst_name}({worst_t}) [{current_worst_score:.3f}] vs 신규 [{new_score:.3f}] (Δ={score_delta:.3f})")
            
            if score_delta >= delta_thr:  # ✅ Δscore 임계치 적용
                info = plan.get("stock_info", {})
                to_buy.append(plan)
                to_sell.append({
                    "ticker": worst_t,
                    "name": worst_name,
                    "qty": worst_qty,
                    "old_score": current_worst_score,
                    "cached_score": worst_score,
                    "new_score": new_score,
                    "score_delta": score_delta,
                    "new_ticker": self._t(info.get("Ticker", "")),
                    "est_proceeds": int(worst_pr * worst_qty),
                    # screener_holdings 상세 정보 추가
                    "sector": worst_screener_info.get('sector', ''),
                    "fin_score": worst_screener_info.get('fin_score', 0.0),
                    "tech_score": worst_screener_info.get('tech_score', 0.0),
                    "mkt_score": worst_screener_info.get('mkt_score', 0.0),
                    "sector_score": worst_screener_info.get('sector_score', 0.0),
                    "rsi": worst_screener_info.get('rsi', 0.0),
                    "pos_52w": worst_screener_info.get('pos_52w', 0.0),
                    "updated_at": worst_screener_info.get('updated_at', '')
                })
                hi += 1
                
                logger.info(f"리밸런싱 매칭: {worst_name}({worst_t}) [{current_worst_score:.3f}] "
                           f"[섹터:{worst_screener_info.get('sector', 'N/A')}, "
                           f"RSI:{worst_screener_info.get('rsi', 0):.1f}, "
                           f"52W:{worst_screener_info.get('pos_52w', 0):.1f}] → "
                           f"{info.get('Name', 'N/A')}({self._t(info.get('Ticker', ''))}) [{new_score:.3f}] "
                           f"(Δ={score_delta:.3f})")
                logger.debug(f"[DEBUG] 매칭 성공: to_buy={len(to_buy)}, to_sell={len(to_sell)}")
            else:
                # 임계치 못 넘으면 더 이상 교체 진행 X (보수적)
                logger.debug(f"리밸런싱 스킵: {worst_name}({worst_t}) [{current_worst_score:.3f}] vs "
                           f"{info.get('Name', 'N/A')}({self._t(info.get('Ticker', ''))}) [{new_score:.3f}] "
                           f"(Δ={score_delta:.3f} < {delta_thr:.3f})")
                break

        logger.debug(f"[DEBUG] 매칭 결과 - to_buy: {len(to_buy)}개, to_sell: {len(to_sell)}개")
        
        if to_buy and to_sell:
            pairs = len(to_buy)
            logger.info(f"회전 매매 매칭 완료: {pairs}건 (스코어 차이 ≥{delta_thr:.2f})")
            logger.debug(f"[DEBUG] 매칭 상세 정보:")
            msg_lines = [f"점수 기반 리밸런싱 매칭 {pairs}건 (Δ≥{delta_thr:.2f})"]
            for i in range(pairs):
                s = to_sell[i]
                new_plan = to_buy[i]
                nt = self._t(new_plan.get('stock_info', {}).get('Ticker', ''))
                nn = new_plan.get('stock_info', {}).get('Name', 'N/A')
                old_score = s.get('old_score', 0.0)
                new_score = s.get('new_score', 0.0)
                score_delta = s.get('score_delta', 0.0)
                logger.info(f"  매칭 {i+1}: {s.get('name', 'N/A')}({s.get('ticker', 'N/A')}) [{old_score:.3f}] → {nn}({nt}) [{new_score:.3f}] (Δ={score_delta:.3f})")
                logger.debug(f"[DEBUG] 매칭 {i+1} 상세: SELL={s}, BUY={new_plan}")
                msg_lines.append(
                    f"- SELL {s.get('name', 'N/A')}({s.get('ticker', 'N/A')}) [{old_score:.3f}]  →  BUY {nn}({nt}) [{new_score:.3f}]"
                )
            _notify_text(" " + "\n".join(msg_lines), key=f"phase:rebalance_pairs:{self.run_id}", cooldown=120)
        else:
            logger.info(f"회전 매매 조건(스코어 차이 ≥{delta_thr:.2f})을 만족하는 신규 후보가 없습니다.")
            logger.debug(f"[DEBUG] 매칭 실패 원인 - to_buy: {len(to_buy)}개, to_sell: {len(to_sell)}개")
        
        logger.debug(f"[DEBUG] _determine_rebalance_swaps 완료 - 반환: 매도 {len(to_sell)}건, 매수 {len(to_buy)}건")
        return to_buy, to_sell

    # ── 쿨다운 관리 ───────────────────────────────────────────────────
    def _load_cooldown_list(self) -> dict:
        if not COOLDOWN_FILE.exists():
            return {}
        try:
            with open(COOLDOWN_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError):
            return {}

    def _save_cooldown_list(self):
        COOLDOWN_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(COOLDOWN_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.cooldown_list, f, indent=2, ensure_ascii=False)

    def _add_to_cooldown(self, ticker: str, reason: str):
        end_date = (datetime.now(KST) + timedelta(days=self.cooldown_period_days)).isoformat()
        self.cooldown_list[ticker] = end_date
        self._save_cooldown_list()
        logger.info(f"[{ticker}] {reason}으로 인해 쿨다운 목록에 추가. ({end_date}까지)")
        from notifier import create_alert_embed
        _notify_embed(
            create_alert_embed(f"쿨다운 등록: {ticker}", "cooldown", [
                {"name": "사유", "value": reason, "inline": False},
                {"name": "만료일", "value": end_date[:19], "inline": True}
            ]),
            key=f"phase:cooldown:{ticker}:{self.run_id}", cooldown=60
        )

    def _add_to_cooldown_for_days(self, ticker: str, days: int, reason: str):
        """
        ticker를 지정한 일수만큼 쿨다운으로 등록한다.
        - 기존 cooldown.json 포맷(ticker -> iso string)을 유지한다.
        - 이미 더 긴 쿨다운이 걸려 있으면 만료일을 늘리지 않는다(최대값 유지).
        """
        try:
            days = int(days)
        except Exception:
            days = 0
        if days <= 0:
            return

        until_dt = datetime.now(KST) + timedelta(days=days)
        prev = self.cooldown_list.get(ticker)
        if isinstance(prev, str):
            try:
                prev_dt = datetime.fromisoformat(prev)
                if prev_dt > until_dt:
                    until_dt = prev_dt
            except Exception:
                pass

        end_date = until_dt.isoformat()
        self.cooldown_list[ticker] = end_date
        self._save_cooldown_list()
        logger.info(f"[{ticker}] {reason}으로 인해 쿨다운 등록/갱신. ({end_date}까지)")
        from notifier import create_alert_embed
        _notify_embed(
            create_alert_embed(f"쿨다운 등록: {ticker}", "cooldown", [
                {"name": "사유", "value": reason, "inline": False},
                {"name": "만료일", "value": end_date[:19], "inline": True}
            ]),
            key=f"phase:cooldown:{ticker}:{self.run_id}", cooldown=60
        )

    def _is_in_cooldown(self, ticker: str) -> bool:
        if ticker not in self.cooldown_list:
            return False
        cooldown_end_date = datetime.fromisoformat(self.cooldown_list[ticker])
        # KST 현재 시각과 비교
        if datetime.now(KST) < cooldown_end_date:
            return True
        else:
            del self.cooldown_list[ticker]
            self._save_cooldown_list()
            return False

    # ── 부분익절 이력(전량매도 후 재진입 차단 갱신용) ────────────────────
    def _load_partial_sell_flags(self) -> dict:
        if not PARTIAL_SELL_FLAGS_FILE.exists():
            return {}
        try:
            with open(PARTIAL_SELL_FLAGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (IOError, json.JSONDecodeError):
            return {}

    def _save_partial_sell_flags(self):
        PARTIAL_SELL_FLAGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(PARTIAL_SELL_FLAGS_FILE, "w", encoding="utf-8") as f:
            json.dump(self.partial_sell_flags, f, indent=2, ensure_ascii=False)

    def _mark_partial_sell(self, ticker: str):
        self.partial_sell_flags[ticker] = datetime.now(KST).isoformat()
        self._save_partial_sell_flags()

    def _had_partial_sell(self, ticker: str) -> bool:
        """
        최근 N일(ttl) 내 부분익절 이력이 있는지 판단한다.
        - TTL이 지나면 flag를 자동 정리한다.
        """
        raw = self.partial_sell_flags.get(ticker)
        if not isinstance(raw, str) or not raw:
            return False
        try:
            ts = datetime.fromisoformat(raw)
        except Exception:
            # 파싱 실패 시 안전하게 제거
            try:
                del self.partial_sell_flags[ticker]
                self._save_partial_sell_flags()
            except Exception:
                pass
            return False

        ttl = int(getattr(self, "partial_sell_flag_ttl_days", 7) or 7)
        if datetime.now(KST) - ts <= timedelta(days=max(1, ttl)):
            return True

        # TTL 경과 → 정리
        try:
            del self.partial_sell_flags[ticker]
            self._save_partial_sell_flags()
        except Exception:
            pass
        return False

    def _clear_partial_sell_flag(self, ticker: str):
        if ticker in self.partial_sell_flags:
            del self.partial_sell_flags[ticker]
            self._save_partial_sell_flags()

    # ── 코히어런트 요약 ──────────────────────────────────────────────
    def _set_summary_reason(self, code: str, detail: str = ""):
        self.summary_reason_code = code
        self.summary_reason_detail = detail

    def emit_final_summary(self, start_ts: float, status: str = "SUCCESS", warnings: int = 0):
        # 통계 검증 로직
        stats_before = {
            'buy': self.stats['buy'],
            'sell': self.stats['sell'],
            'hold': self.stats['hold']
        }
        
        # 15시 20분 이후이고 pending_orders가 있었는지 확인
        if self.should_run_batch_check() and len(self.pending_orders) > 0:
            logger.warning(
                f"⚠️ 통계 검증: pending_orders가 남아있음 ({len(self.pending_orders)}개) "
                f"→ batch_execution_check_and_cancel() 실행 필요"
            )
        
        # 통계 업데이트 이력 확인
        logger.debug(
            f" 최종 통계 상태: buy={self.stats['buy']}, sell={self.stats['sell']}, "
            f"hold={self.stats['hold']} (15시 20분 체결 확인 반영 여부 확인 필요)"
        )
        
        duration = int(time.time() - start_ts)
        reason = self.summary_reason_code or "N/A"
        detail = (f" | {self.summary_reason_detail}" if self.summary_reason_detail else "")
        line1 = f"RUN: {status} | WARNINGS: {warnings} | DURATION: {duration}s"
        line2 = f"TRADES: {self.stats['buy']} buy / {self.stats['sell']} sell / {self.stats['hold']} hold | REASON: {reason}{detail}"
        logger.info(line1)
        logger.info(line2)
        
        # 에러 통계 추가
        error_summary = self._get_error_summary()
        if error_summary != "에러 없음":
            logger.info(error_summary)
        
        # 추천 집계(있을 경우만)
        if sum(self.recomm_stats.values()) > 0:
            logger.info(f"RECOMMENDATIONS: {self.recomm_stats['buy']} buy / {self.recomm_stats['sell']} sell")
        if self.reporting.get("coherent_summary", False):
            txt = f"✅ 파이프라인 요약\n{line1}\n{line2}"
            if error_summary != "에러 없음":
                txt += f"\n{error_summary}"
            if sum(self.recomm_stats.values()) > 0:
                txt += f"\nRECOMMENDATIONS: {self.recomm_stats['buy']} buy / {self.recomm_stats['sell']} sell"
            stats = {
                "매수": self.stats["buy"],
                "매도": self.stats["sell"],
                "홀드": self.stats["hold"],
            }
            from notifier import create_summary_embed
            _notify_embed(
                create_summary_embed(txt, statistics=stats),
                key=f"phase:summary:{self.run_id}", cooldown=60
            )

# ── 엔트리포인트 ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    
    # 명령행 인수 파싱
    parser = argparse.ArgumentParser(description="자동매매 트레이더")
    parser.add_argument("--batch-check-only", action="store_true", help="15시 20분 일괄 체결 확인만 실행")
    args = parser.parse_args()
    
    start_ts = time.time()
    try:
        trader = Trader(settings)
        
        # 15시 20분 일괄 체결 확인만 실행하는 경우
        if args.batch_check_only:
            logger.info("15시 20분 일괄 체결 확인 모드로 실행")
            if trader.should_run_batch_check():
                trader.batch_execution_check_and_cancel()
                logger.info("15시 20분 일괄 체결 확인 완료")
            else:
                logger.info("15시 20분이 아니므로 일괄 체결 확인을 건너뜁니다.")
            sys.exit(0)

        # 새로운 실행 시작 시 처리된 주문 목록 초기화
        trader._clear_processed_orders()

        # 최신 계좌 스냅샷 생성(파일 갱신) 후 파일에서 로드
        trader._update_account_info(force=True)
        cash0, holdings0, _ = trader.get_account_info_from_files()

        # 세션 시작 가드
        usable_cash = cash0
        if trader.reporting.get("include_cash_breakdown", False):
            logger.info(f"usable_cash={usable_cash:,}")
        if trader.trading_guards.get("skip_when_low_funds", False) and \
           usable_cash < int(trader.trading_guards.get("min_total_cash_to_trade", 0)):
            trader._set_summary_reason(
                "SKIPPED_LOW_FUNDS_SESSION",
                f"cash {usable_cash:,} < min_total_cash_to_trade {int(trader.trading_guards.get('min_total_cash_to_trade', 0)):,}"
            )
            trader.emit_final_summary(start_ts, status="SUCCESS", warnings=0)
            sys.exit(0)

        # 매도 로직 실행
        executed_sells = False
        if holdings0:
            executed_sells = trader.run_sell_logic(holdings0)
        else:
            logger.info("보유 종목이 없어 매도 로직을 건너뜁니다.")

        # 매도 실행 후 계좌 정보 갱신 및 동기화
        if executed_sells:
            logger.info("매도 실행됨 → 계좌 정보 갱신 중...")
            trader._update_account_info(force=True)
            time.sleep(3)  # 계좌 정보 갱신 대기 (2초 → 3초로 증가)
            
            # 동기화 검증
            max_retries = 3
            for attempt in range(max_retries):
                cash1, holdings1, _ = trader.get_account_info_from_files()
                if cash1 > cash0:  # 현금이 증가했으면 매도 성공
                    logger.info(f"매도 후 계좌 상태: 현금 {cash1:,}원, 보유종목 {len(holdings1)}개")
                    break
                elif attempt < max_retries - 1:
                    logger.warning(f"매도 후 현금 증가 미확인 (시도 {attempt + 1}/{max_retries}), 재시도...")
                    time.sleep(2)
                else:
                    logger.warning("매도 후 현금 증가 미확인, 현재 상태로 진행")
        else:
            # 매도가 없었으면 기존 정보 사용
            cash1, holdings1 = cash0, holdings0
            logger.info(f"매도 없음 → 기존 계좌 상태 유지: 현금 {cash1:,}원, 보유종목 {len(holdings1)}개")
        
        # 회전 샌드박스 제안 로깅
        trader._log_gpt_rotation_sandbox()

        # 매수 로직 실행 (갱신된 현금과 보유 종목 사용)
        trader.run_buy_logic(cash1, holdings1)

        # 15시 20분 일괄 체결 확인 실행
        if trader.should_run_batch_check():
            logger.info("15시 20분 일괄 체결 확인 실행")
            stats_before_batch = {
                'buy': trader.stats['buy'],
                'sell': trader.stats['sell'],
                'hold': trader.stats['hold']
            }
            
            trader.batch_execution_check_and_cancel()
            
            # 통계 업데이트 확인
            stats_after_batch = {
                'buy': trader.stats['buy'],
                'sell': trader.stats['sell'],
                'hold': trader.stats['hold']
            }
            
            if stats_before_batch != stats_after_batch:
                logger.info(
                    f"✅ 통계 업데이트 확인: "
                    f"buy {stats_before_batch['buy']}→{stats_after_batch['buy']}, "
                    f"sell {stats_before_batch['sell']}→{stats_after_batch['sell']}, "
                    f"hold {stats_before_batch['hold']}→{stats_after_batch['hold']}"
                )
            else:
                logger.debug("통계 변화 없음 (체결된 주문 없음)")

        # 성능 메트릭 로깅
        execution_time = time.time() - start_ts
        logger.info(f"모든 트레이딩 로직 실행 완료. (실행시간: {execution_time:.1f}초)")
        
        # 성능 개선 알림
        if execution_time < 300:  # 5분 미만
            _notify_text(f"✅ 트레이딩 로직 실행 완료 (빠른 실행: {execution_time:.1f}초)", 
                        key=f"phase:done:{trader.run_id}", cooldown=60)
        else:
            _notify_text(f"✅ 트레이딩 로직 실행 완료 (실행시간: {execution_time:.1f}초)", 
                        key=f"phase:done:{trader.run_id}", cooldown=60)

        # 최종 요약
        trader.emit_final_summary(start_ts, status="SUCCESS", warnings=0)

        # 종료 시점 간단 상태 로그(참고용)
        if trader.is_real_trading:
            cash_end, holdings_end, _ = trader.get_account_info_from_files()
            logger.info(f"[END] 보유: {len(holdings_end)}개, 예수금: {cash_end:,}원")
        else:
            logger.info("[END][MOCK] 실거래 아님 → 최종 보유/예수금은 실계좌 스냅샷과 다를 수 있음(모의 체결은 DB에만 기록).")

    except Exception as e:
        logger.critical(f"트레이더 실행 중 심각한 오류 발생: {e}", exc_info=True)
        _notify_text(f" 트레이더 치명적 오류: {str(e)[:1800]}",
                     key=f"phase:fatal:{os.getenv('RUN_ID','na')}", cooldown=30)
        try:
            trader._set_summary_reason("FATAL_EXCEPTION", str(e))
            trader.emit_final_summary(start_ts, status="FAILED", warnings=1)
        except Exception:
            pass