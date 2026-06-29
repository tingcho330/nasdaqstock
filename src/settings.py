# src/settings.py
import json
import os
from pathlib import Path
from typing import Dict, Any

from env_loader import load_project_env

load_project_env()

class Settings:
    def __init__(self, config_path: Path = None):
        if config_path is None:
            # 환경 변수에서 경로 가져오기, 없으면 기본값 사용
            config_path = Path(os.getenv("CONFIG_PATH", "/app/config/config.json"))
        self._config = self._load_config(config_path)

        # ── 최상위 기본값 ─────────────────────────────────────────────
        # trading_environment는 config.json에서 명시적으로 설정되어야 함
        if "trading_environment" not in self._config:
            self._config["trading_environment"] = "vps"  # 기본값은 vps

        # 섹션 존재 보장
        self._config.setdefault("strategy_params", {})
        self._config.setdefault("risk_params", {})
        self._config.setdefault("gpt_params", {})
        self._config.setdefault("notifications", {})
        self._config.setdefault("trading_params", {})
        self._config.setdefault("trading_guards", {})
        self._config.setdefault("screener_params", {})
        self._config.setdefault("reporting", {})
        self._config.setdefault("rotation", {})

        # 섹션 핸들
        self.strategy_params: Dict[str, Any]   = self._config["strategy_params"]
        self.risk_params: Dict[str, Any]       = self._config["risk_params"]
        self.gpt_params: Dict[str, Any]        = self._config["gpt_params"]
        self.notifications: Dict[str, Any]     = self._config["notifications"]
        self.trading_params: Dict[str, Any]    = self._config["trading_params"]
        self.trading_guards: Dict[str, Any]    = self._config["trading_guards"]
        self.screener_params: Dict[str, Any]   = self._config["screener_params"]
        self.reporting: Dict[str, Any]         = self._config["reporting"]
        self.rotation: Dict[str, Any]          = self._config["rotation"]

        # ── 기본 전략 파라미터(기존) ─────────────────────────────────
        self.strategy_params.setdefault("atr_k_stop", 2.0)
        self.strategy_params.setdefault("atr_k_profit", 4.0)
        self.strategy_params.setdefault("sell_threshold", 1.0)
        self.strategy_params.setdefault("weights", {
            "RsiReversalStrategy": 0.5,
            "TrendFollowingStrategy": 0.8,
            "AdvancedTechnicalStrategy": 0.6,
            "DynamicAtrStrategy": 0.7
        })

        # ── 리스크 파라미터 ─────────────────────────────────────────
        self.risk_params.setdefault("atr_period", 14)
        self.risk_params.setdefault("cooldown_period_days", 10)
        self.risk_params.setdefault("max_positions", 8)  # Phase 1: 4 → 8
        self.risk_params.setdefault("cooldown_fail_threshold", 2)
        # [NEW] 손절 강제 집행/직접 매도 보조 플래그
        self.risk_params.setdefault("enforce_stoploss_sell", True)
        self.risk_params.setdefault("stoploss_sell_order", "market")  # market | ioc_limit

        # ── 노티 기본값 ──────────────────────────────────────────────
        self.notifications.setdefault("discord_cooldown_sec", 60)
        self.notifications.setdefault("snapshot_change_threshold_pct", 1.0)

        # ── 트레이딩 파라미터(새로 추가된 섹션) ───────────────────────
        tp = self.trading_params
        tp.setdefault("buy_time_windows", ["09:05-14:50"])
        tp.setdefault("sell_time_windows", ["09:05-15:10"])
        tp.setdefault("allow_rebuy", True)  # Phase 1: False → True
        tp.setdefault("max_positions", self.risk_params.get("max_positions", 4))
        tp.setdefault("max_legs_per_ticker", 1)
        tp.setdefault("per_ticker_max_weight", 0.20)  # Phase 1: 1.0 → 0.20
        tp.setdefault("min_order_cash", 0)            # 금액 기준 최소 주문
        tp.setdefault("rebuy_atr_k", 0.0)
        tp.setdefault("rebuy_rsi_ceiling", 100.0)
        tp.setdefault("min_cash_reserve", 0)
        tp.setdefault("cash_buffer_ratio", 0.0)       # 가용 현금 버퍼

        # 분할 매수 설정 (P1 대응)
        split = tp.setdefault("split_buy", {})
        split.setdefault("enabled", False)
        split.setdefault("slices", 3)                 # 분할 개수
        # weights 미설정 시 내부에서 균등 분배
        split.setdefault("weights", [])               # 예: [0.5, 0.3, 0.2]
        split.setdefault("ladder_ticks", [0, 1, 2])   # 각 슬라이스의 틱 가산
        split.setdefault("interval_sec", 0.6)         # 슬라이스 간 인터벌
        split.setdefault("jitter_sec", 0.15)          # 인터벌 지터
        split.setdefault("min_qty", 250)              # 분할 전환 최소 수량
        split.setdefault("min_cash_per_slice", tp.get("min_order_cash", 0))

        # ── 트레이딩 가드 ────────────────────────────────────────────
        tg = self.trading_guards
        tg.setdefault("skip_when_low_funds", False)
        tg.setdefault("min_total_cash_to_trade", 0)
        tg.setdefault("auto_shrink_slots", True)      # 현금에 따라 슬롯 자동 축소

        # ── 스크리너 동작 파라미터 ───────────────────────────────────
        sp = self.screener_params
        sp.setdefault("affordability_filter", False)  # 가용현금 기반 필터 강제 여부

        # ── 주문 실행 파라미터 (새로 추가) ─────────────────────────────
        order_exec = tp.setdefault("order_execution", {})
        order_exec.setdefault("execution_timeout_sec", 30)        # 체결 대기 시간
        order_exec.setdefault("execution_check_interval", 2)      # 체결 확인 간격
        order_exec.setdefault("execution_check_retries", 3)       # [NEW] 확인 재시도 횟수
        order_exec.setdefault("dynamic_tick_enabled", True)       # 동적 틱 조정 활성화
        order_exec.setdefault("max_tick_adjustment", 3)           # 최대 틱 조정 수
        order_exec.setdefault("market_order_fallback", True)      # 시장가 주문 폴백 활성화
        order_exec.setdefault("retry_on_partial_execution", True) # 부분 체결시 재시도
        order_exec.setdefault("low_priority_threshold", 2)        # 최후순위 분류 임계값
        # [NEW] 안전장치들
        order_exec.setdefault("cancel_pendings_before_fallback", True)
        order_exec.setdefault("confirm_previous_done", True)
        order_exec.setdefault("fallback_only_remaining_qty", True)
        order_exec.setdefault("split_timebox_sec", 20)
        order_exec.setdefault("split_step_min_delay_sec", 1.0)

        # ── 트레이딩 가드 확장 ───────────────────────────────────────
        self.trading_guards.setdefault("cooldown_only_on_confirmed_failure", True)
        self.trading_guards.setdefault("allow_file_snapshot_fallback", False)

        # ── 리포팅 ───────────────────────────────────────────────────
        rp = self.reporting
        rp.setdefault("coherent_summary", True)
        rp.setdefault("include_cash_breakdown", True)

    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        from utils import load_json_config

        if not config_path.exists():
            raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {config_path}")
        data = load_json_config(config_path)
        if data is None:
            raise ValueError(f"config.json 파싱 실패: {config_path}")
        return data

# 싱글턴처럼 사용할 수 있도록 인스턴스 생성
settings = Settings()
