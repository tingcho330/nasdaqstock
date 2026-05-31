# src/risk_manager.py
import os
import json
import logging
import subprocess
import time as pytime
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from typing import Dict, Tuple, Optional, List

# ── 공통 유틸/노티파이어 ────────────────────────────────────────────────
from utils import (
    KST,
    OUTPUT_DIR,
    extract_cash_from_summary,
    setup_logging,
    in_time_windows,
    normalize_ticker_6,
    get_account_snapshot_cached,
    check_min_holding_period,
    check_min_holding_hours,
)
from notifier import (
    DiscordLogHandler,
    WEBHOOK_URL,
    is_valid_webhook,
    send_discord_message,
)

# ── 계산 전용 코어 모듈 사용 ───────────────────────────────────────────
from screener_core import (
    _compute_levels,          # 손절/목표가 계산 (ATR/스윙 기반, 퍼센트 백업)
    get_historical_prices,    # 과거 시세 조회 (pykrx 우선, fdr 백업)
    calculate_rsi,            # RSI 계산
)

# ── 전략 시스템 사용 ───────────────────────────────────────────────────
from strategies import StrategyMixer, BaseStrategy
from api.kis_auth import KIS
from settings import Settings

# ───────────────── 로깅 기본 설정 ─────────────────
# LOG_LEVEL 환경변수로 로깅 레벨 제어 (기본 INFO)
_lvl_name = os.getenv("LOG_LEVEL", "INFO").upper().strip()
_lvl = getattr(logging, _lvl_name, logging.INFO)
setup_logging(level=_lvl)
logger = logging.getLogger("RiskManager")
logger.setLevel(_lvl)
# 루트 로거도 동일 레벨로 맞춤(하위 핸들러 일관성)
root = logging.getLogger()
root.setLevel(_lvl)
# 모든 핸들러 레벨도 업데이트
for handler in root.handlers:
    handler.setLevel(_lvl)
logger.info(f"RiskManager 로깅 레벨 설정: {_lvl_name} ({_lvl})")

# 루트 로거에 디스코드 에러 핸들러 장착(중복 방지)
_root = logging.getLogger()
if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
    if not any(isinstance(h, DiscordLogHandler) for h in _root.handlers):
        _root.addHandler(DiscordLogHandler(WEBHOOK_URL))
        logger.info("DiscordLogHandler attached to root logger.")
else:
    logger.warning("유효한 DISCORD_WEBHOOK_URL이 없어 에러 로그의 디스코드 전송을 비활성화합니다.")

ACCOUNT_SCRIPT_PATH = "/app/src/account.py"
TRADER_SCRIPT_PATH = "/app/src/trader.py"  # [NEW] 파이프라인 트리거 대상

# ── 장중 정의(평일 09:00~15:30) ────────────────────────────────────────
MARKET_START = dt_time(9, 0)
MARKET_END   = dt_time(15, 30)

def is_market_hours(now: Optional[datetime] = None) -> bool:
    """평일 09:00~15:30 (KST) 에만 True"""
    if now is None:
        now = datetime.now(KST)
    if now.weekday() > 4:  # 0=월 ~ 4=금
        return False
    now_t = now.time()
    return MARKET_START <= now_t <= MARKET_END

def next_market_open_kst(now: Optional[datetime] = None) -> datetime:
    """다음 장 시작(평일 09:00) 시각 계산"""
    if now is None:
        now = datetime.now(KST)

    # 이미 장중이면 지금 반환
    if is_market_hours(now):
        return now

    # 오늘 09:00 기준
    candidate = now.replace(hour=9, minute=0, second=0, microsecond=0)

    # 오늘 장이 끝났으면 익일 09:00
    if now.time() >= MARKET_END:
        candidate = candidate + timedelta(days=1)

    # 주말 건너뛰기
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)

    return candidate

def sleep_until_kst(when_dt: datetime):
    """지정한 KST 시각까지 대기. 15분 단위로 쪼개서 sleep."""
    while True:
        now = datetime.now(KST)
        remain = (when_dt - now).total_seconds()
        if remain <= 0:
            return
        pytime.sleep(min(remain, 900))  # 최대 15분 간격으로 슬립

# ── 알림/트리거 쿨다운 ─────────────────────────────────────────────────
_last_sent: Dict[str, float] = {}
_last_trigger: Dict[str, float] = {}  # [NEW] 파이프라인 트리거 쿨다운

def _notify(msg: str, key: str = "risk_manager", cooldown_sec: int = 300) -> None:
    """디스코드 알림(쿨다운 적용). 실패해도 파이프라인 저지하지 않음."""
    try:
        now = pytime.time()
        if key not in _last_sent or now - _last_sent[key] >= cooldown_sec:
            _last_sent[key] = now
            if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
                send_discord_message(content=msg)
    except Exception:
        pass

def _notify_sell_embed(holding: Dict, reason: str, decision_type: str = "SELL", 
                       structured_context: Optional[Dict] = None, 
                       key: str = "risk_sell", cooldown_sec: int = 300) -> None:
    """매도 판단 시 하이라이트된 Discord embed 메시지 전송 (쿨다운 적용)."""
    try:
        now = pytime.time()
        ticker = normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
        unique_key = f"{key}_{ticker}"
        
        if unique_key not in _last_sent or now - _last_sent[unique_key] >= cooldown_sec:
            _last_sent[unique_key] = now
            
            if not WEBHOOK_URL or not is_valid_webhook(WEBHOOK_URL):
                return
            
            # 보유 종목 정보 추출
            name = holding.get("prdt_name", "N/A")
            qty = _to_int(holding.get("hldg_qty", 0))
            cur_price = _to_int(holding.get("prpr", 0))
            avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
            eval_amt = _to_int(holding.get("evlu_amt", 0))
            profit_loss = _to_int(holding.get("evlu_pfls_amt", 0))
            profit_rate = _to_float(holding.get("evlu_pfls_rt", 0.0))
            
            # 판단 타입에 따른 설정
            if decision_type == "SELL":
                emoji = "⚠️"
                title = f"{emoji} 매도 판단"
                color = 0xff0000  # 빨간색 (하이라이트)
            elif decision_type == "PARTIAL_SELL":
                emoji = "💰"
                title = f"{emoji} 부분 익절 판단"
                color = 0xff6600  # 주황색
            else:
                emoji = "ℹ️"
                title = f"{emoji} 매도 판단"
                color = 0xffaa00  # 주황색
            
            # 매도 타입 확인
            sell_type = "Unknown"
            if structured_context:
                sell_type = structured_context.get("type", "Unknown")
            
            # Embed 필드 구성
            fields = [
                {
                    "name": "종목명",
                    "value": f"{name} ({ticker})",
                    "inline": True
                },
                {
                    "name": "보유 수량",
                    "value": f"{qty:,}주",
                    "inline": True
                },
                {
                    "name": "현재가",
                    "value": f"{cur_price:,}원",
                    "inline": True
                },
                {
                    "name": "매수가",
                    "value": f"{avg_price:,.0f}원",
                    "inline": True
                },
                {
                    "name": "손익",
                    "value": f"{profit_loss:+,}원 ({profit_rate:+.2f}%)",
                    "inline": True
                },
                {
                    "name": "평가금액",
                    "value": f"{eval_amt:,}원",
                    "inline": True
                },
                {
                    "name": "판단 사유",
                    "value": reason[:1024],  # Discord 필드 최대 길이 제한
                    "inline": False
                }
            ]
            
            # 매도 타입이 있으면 추가
            if sell_type != "Unknown":
                fields.append({
                    "name": "매도 타입",
                    "value": sell_type,
                    "inline": True
                })
            
            embed = {
                "title": title,
                "color": color,
                "fields": fields,
                "timestamp": datetime.now(KST).isoformat()
            }
            
            send_discord_message("", embeds=[embed])
            
    except Exception as e:
        # 알림 실패해도 파이프라인 저지하지 않음
        logger.debug(f"매도 embed 알림 전송 실패: {e}")

def _can_trigger(key: str, cooldown_sec: int) -> bool:
    """[NEW] 파이프라인 기동 쿨다운"""
    now = pytime.time()
    last = _last_trigger.get(key, 0.0)
    if now - last >= cooldown_sec:
        _last_trigger[key] = now
        return True
    return False

# ── 데이터 클래스: 규칙 파라미터 ─────────────────────────────────────────
@dataclass
class SellRules:
    """매도 판단 규칙 파라미터"""
    stop_loss_buffer: float = 0.0     # 손절가 대비 추가 버퍼(비율). 예: 0.003 -> 손절가*1.003
    take_profit_buffer: float = 0.0   # 목표가 대비 추가 버퍼(비율)
    rsi_take_profit: Optional[float] = 75.0  # RSI가 이 값 이상이면 이익실현 고려(None이면 비활성)
    max_holding_days: Optional[int] = None   # 보유일수 상한(None이면 비활성)
    # [NEW] 전일 종가 이탈 + 시간대 로직
    prev_close_break_sell: bool = False          # 전일 종가 하회 시 매도 규칙 활성화
    prev_close_buffer_pct: float = 0.003         # 전일 종가 대비 추가 버퍼(예: 0.003 => -0.3%)
    time_windows_for_sells: Optional[List[str]] = None  # 매도 전반 허용 시간대(예: ["09:05-14:50","15:00-15:20"])
    time_windows_for_take_profit: Optional[List[str]] = None  # 이익실현만 허용 시간대(없으면 전반 윈도우 사용)
    confirm_bars_for_break: int = 0              # [단순 버전] 확인봉 개수(일봉 기준, 0=미사용)

# ── 유틸 함수들 ────────────────────────────────────────────────────────
def _to_int(x) -> int:
    if isinstance(x, (int, float)):
        return int(x)
    if isinstance(x, str):
        s = x.replace(",", "").strip()
        try:
            return int(float(s))
        except Exception:
            return 0
    return 0

def _to_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(",", "").strip()
        return float(s)
    except Exception:
        return default

def _percent_backup_levels(entry_price: float, risk_params: Dict) -> Dict[str, float]:
    """손절/목표가가 없을 때 즉시 산출하는 퍼센트 백업"""
    stop_pct = float(risk_params.get("stop_pct", 0.03))
    rr = float(risk_params.get("reward_risk", 2.0))
    stop_px = entry_price * (1.0 - stop_pct)
    risk = max(1e-6, entry_price - stop_px)
    tgt_px = entry_price + rr * risk
    
    return {
        "손절가": int(round(stop_px)),
        "목표가": int(round(tgt_px)),
        "source": "percent_backup",
    }

# ── RiskManager 본체 ───────────────────────────────────────────────────
class RiskManager:
    """
    - settings(settings.py의 settings 객체)를 받아 리스크 파라미터를 로드
    - check_sell_condition(holding, stock_info) 제공
    - 필요 시 계좌 스냅샷(account.py) 트리거하는 헬퍼 제공
    - [NEW] 보유 0일 때 트레이딩 파이프라인 자동 기동(조건부)
    """

    def __init__(self, settings_obj):
        self.settings_obj = settings_obj
        self.config = getattr(settings_obj, "_config", {}) or {}
        self.env = self.config.get("trading_environment", "prod")

        # risk_params에서 룰 추출
        rp = self.config.get("risk_params", {}) or {}
        self.rules = SellRules(
            stop_loss_buffer=float(rp.get("stop_loss_buffer", 0.0)),
            take_profit_buffer=float(rp.get("take_profit_buffer", 0.0)),
            rsi_take_profit=(float(rp["rsi_take_profit"]) if rp.get("rsi_take_profit") is not None else None),
            max_holding_days=(int(rp["max_holding_days"]) if rp.get("max_holding_days") is not None else None),
            # [NEW] 전일 종가 + 시간대
            prev_close_break_sell=bool(rp.get("prev_close_break_sell", False)),
            prev_close_buffer_pct=float(rp.get("prev_close_buffer_pct", 0.003)),
            time_windows_for_sells=rp.get("time_windows_for_sells") or rp.get("time_windows") or None,
            time_windows_for_take_profit=rp.get("time_windows_for_take_profit") or None,
            confirm_bars_for_break=int(rp.get("confirm_bars_for_break", 0)),
        )

        # [NEW] 자동 파이프라인 트리거 설정
        self.auto_trigger_when_empty: bool = bool(rp.get("auto_trigger_trader_when_empty", True))
        self.auto_trigger_cooldown_sec: int = int(rp.get("auto_trigger_cooldown_sec", 900))  # 15분
        self.min_cash_to_trigger: int = int(rp.get("min_cash_to_trigger", 100_000))
        self.buy_time_windows: List[str] = rp.get("buy_time_windows") or ["09:05-14:50"]

        # [NEW] 즉시 매도 옵션 및 브로커 초기화
        auto_sell_cfg = rp.get("auto_sell", {}) or {}
        self._direct_execute_sell_enabled: bool = bool(auto_sell_cfg.get("direct_execute", False))
        self._direct_sell_cooldown: int = int(auto_sell_cfg.get("cooldown_sec_per_ticker", 20))
        # [NEW] 긴급 낙폭(드로우다운) 임계값(없으면 stop_pct 사용)
        self.emergency_drop_pct: float = float(auto_sell_cfg.get("emergency_drop_pct", rp.get("stop_pct", 0.0)))
        self._recent_direct_sell: Dict[str, float] = {}
        self._kis: KIS | None = None
        try:
            kis_cfg = self.config.get("kis_broker", {})
            self._kis = KIS(config=kis_cfg, env=self.env)
        except Exception:
            self._kis = None

        # [NEW] 전략 시스템 설정
        self.strategy_mode = rp.get("strategy_mode", "simple")
        self.enable_advanced = bool(rp.get("enable_advanced_strategies", False))
        self.strategy_mixer = None
        
        # 요약 파일 자동 복구를 위한 초기화
        self._ensure_summary_file_exists()
        
        # 전략 시스템 초기화
        if self.strategy_mode in ["advanced", "hybrid"] and self.enable_advanced:
            try:
                self.strategy_mixer = StrategyMixer(settings_obj)
                logger.info(f"전략 시스템 초기화 완료 (mode={self.strategy_mode})")
            except Exception as e:
                logger.error(f"전략 시스템 초기화 실패: {e}, 기본 모드로 전환")
                self.strategy_mode = "simple"
                self.enable_advanced = False

        logger.info(f"RiskManager 초기화 완료 (env={self.env}, strategy_mode={self.strategy_mode})")
        # [NEW] 전일 종가 캐시(세션 단위)
        self._prev_close_cache: Dict[str, int] = {}
        
    def _load_holdings(self) -> Dict[str, Dict]:
        """보유 종목 정보 로드 (백그라운드 모니터링용)"""
        try:
            # 계좌 스냅샷에서 보유 종목 정보 추출
            result = get_account_snapshot_cached()
            if isinstance(result, tuple) and len(result) >= 2:
                # get_account_snapshot_cached returns (cash, holdings, ...)
                _, snapshot = result[0], result[1]
            else:
                snapshot = result
                
            if not snapshot or not isinstance(snapshot, list):
                return {}
                
            holdings = {}
            for item in snapshot:
                if isinstance(item, dict):
                    ticker = item.get("pdno", "")
                    if ticker and item.get("hldg_qty", "0") != "0":
                        holdings[ticker] = {
                            "name": item.get("prdt_name", ""),
                            "quantity": int(item.get("hldg_qty", "0")),
                            "current_price": int(item.get("prpr", "0")),
                            "prev_close": int(item.get("stck_sdpr", "0")),
                            "evaluation": int(item.get("evlu_amt", "0")),
                            "profit_loss": int(item.get("evlu_pfls_amt", "0")),
                            "profit_rate": float(item.get("evlu_pfls_rt", "0"))
                        }
            
            return holdings
            
        except Exception as e:
            logger.error(f"보유 종목 정보 로드 실패: {e}")
            return {}
    
    def _update_account_snapshot(self):
        """계좌 스냅샷 갱신 (백그라운드 모니터링용)"""
        try:
            # 계좌 스냅샷 캐시 갱신
            get_account_snapshot_cached()
            logger.debug("계좌 스냅샷 갱신 완료")
        except Exception as e:
            logger.error(f"계좌 스냅샷 갱신 실패: {e}")

    def _can_direct_sell_now(self, ticker: str) -> bool:
        if not self._direct_execute_sell_enabled:
            logger.debug(f"[DIRECT_SELL] skip: disabled by config (ticker={ticker})")
            return False
        if self._kis is None:
            logger.debug(f"[DIRECT_SELL] skip: KIS not initialized (ticker={ticker})")
            return False

        # [SAFETY] 당일 매도 방지(보유시간) 재검증
        # check_sell_condition에서 이미 체크하지만, DB/환경 이슈로 체크가 스킵되는 경우를 막기 위해
        # direct_execute 직전에도 동일 가드를 한 번 더 적용한다.
        try:
            trading_params = self.config.get("trading_params", {}) or {}
            min_holding_hours = int(trading_params.get("min_holding_hours", 0) or 0)
        except Exception:
            min_holding_hours = 0
        if min_holding_hours > 0:
            is_eligible, holding_hours = check_min_holding_hours(ticker, min_holding_hours)
            if not is_eligible:
                logger.info(
                    f"[DIRECT_SELL] skip: same-day sell prevented (ticker={ticker}, "
                    f"holding_hours={holding_hours:.1f} < min_holding_hours={min_holding_hours})"
                )
                return False

        now = pytime.time()
        last = self._recent_direct_sell.get(ticker, 0.0)
        if now - last < self._direct_sell_cooldown:
            logger.debug(f"[DIRECT_SELL] skip: cooldown ({now - last:.1f}s < {self._direct_sell_cooldown}s, ticker={ticker})")
            return False
        # 시간대 가드
        now_kst = datetime.now(KST)
        if now_kst.weekday() > 4:
            logger.debug(f"[DIRECT_SELL] skip: weekend (ticker={ticker})")
            return False
        sell_win_ok = True if not self.rules.time_windows_for_sells else in_time_windows(now_kst, self.rules.time_windows_for_sells)
        if not sell_win_ok:
            logger.debug(f"[DIRECT_SELL] skip: outside sell window (ticker={ticker}, now={now_kst.strftime('%H:%M:%S')})")
            return False
        self._recent_direct_sell[ticker] = now
        return True

    def _direct_execute_sell(self, ticker: str, name: str, qty: int, reason_meta: Dict) -> bool:
        """[NEW] 즉시 시장가 매도(브로커 지원 기준: ord_unpr=0)"""
        try:
            if qty <= 0:
                logger.debug(f"[DIRECT_SELL] skip: qty<=0 (ticker={ticker})")
                return False
            logger.info(f"[DIRECT_SELL] try submit: {name}({ticker}) qty={qty}, reason={reason_meta.get('type','')}")
            df = self._kis.order_cash(
                ord_dv="01", pdno=ticker, ord_dvsn="01", ord_qty=str(qty), ord_unpr="0"
            )
            from utils import extract_broker_order_id
            odno = extract_broker_order_id(df)
            ok = False
            rt_cd = ""
            if hasattr(df, "empty") and not df.empty:
                rec = df.to_dict("records")[0]
                if not odno:
                    odno = extract_broker_order_id(rec)
                rt_cd = str(rec.get("rt_cd", "")).strip()
                ok = (rt_cd == "0") or bool(odno)
            elif odno:
                ok = True
            if ok:
                logger.info(f"[DIRECT_SELL] ✅ submitted: {name}({ticker}) {qty}주 (ODNO={odno or 'N/A'})")
                try:
                    # 가격 조회 시도
                    current_price = 0
                    try:
                        price_df = self._kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd=ticker)
                        if price_df is not None and not price_df.empty and 'stck_prpr' in price_df.columns:
                            current_price = int(price_df['stck_prpr'].iloc[0])
                    except Exception:
                        pass
                    
                    from recorder import record_trade
                    try:
                        from db_debug import log_trade_in as _db_dbg_trade_in
                    except ImportError:
                        def _db_dbg_trade_in(*args, **kwargs):
                            pass
                    if not odno:
                        logger.warning(
                            f"[DIRECT_SELL] 주문번호 없음 → DB pending 생략: {name}({ticker}) "
                            "(order_reconciler 연동 불가)"
                        )
                    else:
                        _direct_sell_payload = {
                            "side": "sell", "ticker": ticker, "name": name,
                            "qty": qty, "price": current_price, "trade_status": "pending",
                            "order_id": odno,
                            "requested_qty": qty,
                            "executed_qty": 0,
                            "_debug_context": "risk_manager.direct_execute",
                            "strategy_details": {"mode": "direct_execute", **reason_meta}
                        }
                        _db_dbg_trade_in("risk_manager.DIRECT_SELL_DB", _direct_sell_payload)
                        record_trade(_direct_sell_payload)
                except Exception as e:
                    logger.debug(f"[DIRECT_SELL] record_trade 실패: {e}")
                return True
            else:
                logger.warning(f"[DIRECT_SELL] submit failed: {name}({ticker}) qty={qty}, df_empty={getattr(df,'empty',True)}")
        except Exception as e:
            logger.error(f"[DIRECT_SELL] exception: {e}")
        return False

    def _ensure_summary_file_exists(self):
        """요약 파일이 없으면 account.py를 실행하여 생성"""
        try:
            # 요약 파일 존재 확인
            summary_files = list(OUTPUT_DIR.glob("summary_*.json"))
            if not summary_files:
                logger.warning("요약 파일이 없습니다. account.py를 실행합니다...")
                self._run_account_snapshot()
            else:
                # 최신 파일 확인 (5분 이내)
                latest_file = max(summary_files, key=lambda f: f.stat().st_mtime)
                if pytime.time() - latest_file.stat().st_mtime > 300:  # 5분
                    logger.warning("요약 파일이 오래되었습니다. account.py를 실행합니다...")
                    self._run_account_snapshot()
                else:
                    logger.debug(f"요약 파일 확인 완료: {latest_file.name}")
        except Exception as e:
            logger.error(f"요약 파일 확인 중 오류: {e}")
            self._run_account_snapshot()

    def _run_account_snapshot(self):
        """account.py 실행하여 계좌 스냅샷 생성"""
        try:
            logger.info("account.py 실행 시작...")
            result = subprocess.run(
                ["python", "/app/src/account.py"],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
                encoding="utf-8"
            )
            logger.info("account.py 실행 완료")
            logger.debug(f"account.py 출력: {result.stdout}")
        except subprocess.TimeoutExpired:
            logger.error("account.py 실행 시간 초과 (30초)")
        except subprocess.CalledProcessError as e:
            logger.error(f"account.py 실행 실패 (exit={e.returncode}): {e.stderr}")
        except Exception as e:
            logger.error(f"account.py 실행 중 오류: {e}")

    def _normalize_column_names(self, df):
        """컬럼명을 영어로 정규화"""
        if df is None or df.empty:
            return df
            
        column_mapping = {
            '시가': 'Open',
            '고가': 'High', 
            '저가': 'Low',
            '종가': 'Close',
            '거래량': 'Volume',
            '등락률': 'ChangeRate'
        }
        
        # 한국어 컬럼명이 있으면 영어로 변환
        if any(col in df.columns for col in column_mapping.keys()):
            df = df.rename(columns=column_mapping)
            logger.debug(f"컬럼명 정규화 완료: {list(df.columns)}")
            
        return df

    # ── screener_core 호출로 실시간 지표/레벨 ───────────────────────────
    def compute_realtime_levels(self, ticker: str, entry_price: float) -> Dict:
        """
        손절가/목표가/RSI 계산(파일 참조 없이 함수 호출).
        - entry_price: 진입가가 없다면 현재가를 그대로 넣어도 됨
        인터페이스 보장: 항상 {'손절가','목표가','RSI','Price','source'} 포함.
        """
        t = normalize_ticker_6(ticker, os.getenv("MARKET", "NASDAQ100"))
        ep = float(entry_price)
        risk_params = self.config.get("risk_params", {}) or {}
        strategy_params = self.config.get("strategy_params", {}) or {}  # Phase 1: strategy_params 추가

        # 기본 페이로드 + 퍼센트 백업(선적용, 이후 코어 계산 성공 시 덮어씀)
        out: Dict = {
            "Ticker": t,
            "Price": int(round(ep)),
            "RSI": 50.0,
            **_percent_backup_levels(ep, risk_params),
        }

        # 1) 손절/목표가 (성공 시만 덮어쓰기)
        try:
            date_str = datetime.now(KST).strftime("%Y%m%d")
            
            # 현재 가격을 안전하게 변환
            safe_ep = _to_float(ep, 0)
            if safe_ep <= 0:
                logger.warning(f"[{t}] 잘못된 현재가: {ep}, 백업 사용")
                raise ValueError(f"Invalid current price: {ep}")
            
            # Phase 1: strategy_params 전달
            levels = _compute_levels(t, safe_ep, date_str, risk_params, strategy_params)
            if isinstance(levels, dict):
                if "손절가" in levels and "목표가" in levels:
                    stop_loss = _to_float(levels["손절가"], out["손절가"])
                    target = _to_float(levels["목표가"], out["목표가"])
                    
                    if stop_loss > 0 and target > 0:
                        out["손절가"] = int(round(stop_loss))
                        out["목표가"] = int(round(target))
                        out["source"] = str(levels.get("source", "core_levels"))
                        logger.debug(f"[{t}] 손절/목표가 업데이트: {out['손절가']:,}/{out['목표가']:,} ({out['source']})")
                    else:
                        logger.warning(f"[{t}] 잘못된 손절/목표가 값: {stop_loss}/{target}")
        except Exception as e:
            logger.warning(f"[{t}] 손절/목표가 계산 실패: {e} (백업 사용)")

        # 2) RSI
        try:
            end_dt = datetime.now(KST)
            start_dt = end_dt - timedelta(days=365)
            df = get_historical_prices(t, start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
            if df is not None and not df.empty:
                # 컬럼명 정규화
                df = self._normalize_column_names(df)
                
                # 가격 컬럼 찾기 (정규화 후)
                close_col = None
                if "Close" in df.columns:
                    close_col = "Close"
                elif "close" in df.columns:
                    close_col = "close"
                else:
                    close_cols = [c for c in df.columns if c.lower() == "close"]
                    if close_cols:
                        close_col = close_cols[0]
                
                if close_col and close_col in df.columns:
                    prices = df[close_col].dropna()
                    if len(prices) > 14:
                        rsi_val = calculate_rsi(prices, 14)
                        if 0 <= rsi_val <= 100:
                            out["RSI"] = round(float(rsi_val), 2)
                            logger.debug(f"[{t}] RSI 계산 성공: {out['RSI']}")
                        else:
                            logger.warning(f"[{t}] RSI 값이 범위를 벗어남: {rsi_val}")
                    else:
                        logger.warning(f"[{t}] RSI 계산을 위한 데이터 부족: {len(prices)}개")
                else:
                    # 중복 경고 방지
                    if not hasattr(self, '_price_column_warning_logged'):
                        logger.warning(f"[{t}] 가격 컬럼을 찾을 수 없음: {df.columns.tolist()}")
                        self._price_column_warning_logged = True
            else:
                # 중복 경고 방지
                if not hasattr(self, '_no_data_warning_logged'):
                    logger.warning(f"[{t}] 과거 가격 데이터 없음")
                    self._no_data_warning_logged = True
        except Exception as e:
            logger.warning(f"[{t}] RSI 계산 실패: {e} (기본 50.0 사용)")

        # [NEW] Phase 2: 고급 전략을 위한 추가 데이터
        if self.enable_advanced and self.strategy_mode in ["advanced", "hybrid"]:
            try:
                # 이동평균선 계산 (MA50, MA200)
                end_dt = datetime.now(KST)
                start_dt = end_dt - timedelta(days=400)  # MA200을 위해 충분한 데이터
                df = get_historical_prices(t, start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
                
                if df is not None and not df.empty:
                    # 컬럼명 정규화
                    df = self._normalize_column_names(df)
                    
                    # Close 컬럼 찾기 (정규화 후)
                    close_col = None
                    if "Close" in df.columns:
                        close_col = "Close"
                    else:
                        close_candidates = [c for c in df.columns if c.lower() == "close"]
                        if close_candidates:
                            close_col = close_candidates[0]
                    
                    if close_col is None:
                        # 중복 경고 방지를 위해 한 번만 로깅
                        if not hasattr(self, '_close_column_warning_logged'):
                            logger.warning(f"[{t}] Close 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df.columns)}")
                            self._close_column_warning_logged = True
                        # Close 컬럼이 없으면 고급 전략 데이터 계산을 건너뛰고 기본값 사용
                        pass
                    else:
                        close_prices = df[close_col]
                        
                        # MA50, MA200 계산
                        ma50 = close_prices.rolling(50, min_periods=50).mean()
                        ma200 = close_prices.rolling(200, min_periods=200).mean()
                        
                        # MA50 계산 (최소 50일 데이터 필요)
                        if not ma50.empty and len(ma50) >= 50 and not pd.isna(ma50.iloc[-1]):
                            out["MA50"] = round(float(ma50.iloc[-1]), 2)
                        
                        # MA200 계산 (최소 200일 데이터 필요)
                        if not ma200.empty and len(ma200) >= 200 and not pd.isna(ma200.iloc[-1]):
                            out["MA200"] = round(float(ma200.iloc[-1]), 2)
                        
                        # ATR 계산 (DynamicAtrStrategy용)
                        if "ATR" in levels:
                            out["ATR"] = float(levels.get("ATR", 0.0))
                        
                        # daily_chart 데이터 (AdvancedTechnicalStrategy용)
                        if len(df) >= 20:  # 최소 20일 데이터
                            daily_chart = df.tail(20).copy()
                            # 컬럼명 정규화
                            daily_chart.columns = [col.title() for col in daily_chart.columns]
                            out["daily_chart"] = daily_chart.to_dict('records')
                        
                        # investor_flow 데이터 (기관/외국인 매매 데이터)
                        # 실제 구현에서는 별도 API 호출이 필요하지만, 여기서는 기본값 설정
                        out["investor_flow"] = []  # 실제 구현 시 데이터 추가
                    
            except Exception as e:
                logger.warning(f"[{t}] 고급 전략 데이터 계산 실패: {e}")
                logger.debug(f"[{t}] 고급 전략 데이터 계산 상세 오류: {type(e).__name__}: {str(e)}", exc_info=True)

        # [NEW] 개선된 RSI 전략을 위한 추가 지표 계산
        rsi_strategy = risk_params.get("rsi_sell_strategy", {})
        if rsi_strategy.get("enabled", False):
            try:
                from screener_core import (
                    calculate_ma20,
                    calculate_ma20_slope,
                    calculate_volume_ratio,
                )
                from recorder import save_rsi_snapshot
                
                # RSI 이력 저장
                try:
                    save_rsi_snapshot(t, out["RSI"], ep)
                except Exception as e:
                    logger.debug(f"[{t}] RSI 이력 저장 실패: {e}")
                
                # MA20 및 기울기 계산
                end_dt = datetime.now(KST)
                start_dt = end_dt - timedelta(days=60)  # MA20 + 기울기 계산을 위한 충분한 데이터
                df = get_historical_prices(t, start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
                
                if df is not None and not df.empty:
                    df = self._normalize_column_names(df)
                    
                    # Close 컬럼 찾기
                    close_col = None
                    if "Close" in df.columns:
                        close_col = "Close"
                    else:
                        close_candidates = [c for c in df.columns if c.lower() == "close"]
                        if close_candidates:
                            close_col = close_candidates[0]
                    
                    if close_col and close_col in df.columns:
                        close_prices = df[close_col].dropna()
                        
                        # MA20 계산
                        ma20 = calculate_ma20(close_prices, period=20)
                        if ma20 > 0:
                            out["MA20"] = round(ma20, 2)
                        
                        # MA20 기울기 계산
                        ma20_slope = calculate_ma20_slope(close_prices, period=20, lookback_days=5)
                        out["MA20_slope"] = round(ma20_slope, 4)
                        
                        # 거래량 비율 계산
                        volume_ratio = calculate_volume_ratio(df, short_period=3, long_period=10)
                        out["volume_ratio"] = round(volume_ratio, 3)
                        
                        logger.debug(f"[{t}] 개선된 RSI 전략 지표: MA20={out.get('MA20', 'N/A')}, MA20_slope={ma20_slope:.4f}, volume_ratio={volume_ratio:.3f}")
            except Exception as e:
                logger.warning(f"[{t}] 개선된 RSI 전략 지표 계산 실패: {e}")

        # (선택) 미러링 필드
        out["levels_source"] = out.get("source")
        return out

    # ── [NEW] 전일 종가 조회(세션 캐시) ───────────────────────────────
    def _get_prev_close(self, ticker: str) -> Optional[int]:
        t = normalize_ticker_6(ticker, os.getenv("MARKET", "NASDAQ100"))
        if t in self._prev_close_cache:
            return self._prev_close_cache[t]
        try:
            end_dt = datetime.now(KST)
            start_dt = end_dt - timedelta(days=10)  # 주말 포함 여유
            df = get_historical_prices(t, start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
            if df is None or len(df) < 2:
                return None
            close_col = "Close" if "Close" in df.columns else [c for c in df.columns if c.lower() == "close"][0]
            prev_close = float(df[close_col].iloc[-2])
            val = int(round(prev_close))
            self._prev_close_cache[t] = val
            return val
        except Exception as e:
            logger.debug(f"[{t}] 전일 종가 조회 실패: {e}")
            return None

    # Phase 1: 최소 보유기간 체크 로직은 utils.check_min_holding_period로 통일됨

    # ── 계좌 스냅샷 트리거/읽기 분리 ────────────────────────────────────
    def trigger_account_snapshot(self) -> bool:
        """
        account.py를 실행해 최신 summary/balance 파일을 생성만 합니다.
        읽기는 호출측(트레이더)에서 utils.get_account_snapshot_cached 사용 권장.
        """
        try:
            subprocess.run(
                ["python", str(ACCOUNT_SCRIPT_PATH)],
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8",
            )
            logger.info("(RiskManager) account.py 자동 실행 완료")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"(RiskManager) account.py 실행 실패: exit={e.returncode}\n{e.stderr}")
        except FileNotFoundError:
            logger.error(f"(RiskManager) account.py 경로를 찾지 못했습니다: {ACCOUNT_SCRIPT_PATH}")
        except Exception as e:
            logger.error(f"(RiskManager) account.py 실행 중 예외: {e}")
        return False

    def refresh_account_snapshot(self) -> Tuple[Dict[str, int], List[Dict], Optional[str], Optional[str]]:
        """
        [호환 유지] 최신 스냅샷을 생성 → utils 캐시로 읽어 반환합니다.
        return: (cash_info_dict, holdings_list, summary_file, balance_file)
        """
        self.trigger_account_snapshot()
        summary_dict, balance_list, summary_path, balance_path = get_account_snapshot_cached(
            summary_pattern="summary_*.json",
            balance_pattern="balance_*.json",
            ttl_sec=5,  # 즉시 재로딩 유도(파일 mtime 변화 감지)
        )
        cash_map = extract_cash_from_summary(
            summary_dict,
            market=os.getenv("MARKET", "NASDAQ100"),
        )
        return (
            cash_map,
            balance_list,
            str(summary_path) if summary_path else None,
            str(balance_path) if balance_path else None,
        )

    # ── [NEW] 보유 0일 때 트레이딩 파이프라인 자동 기동 ────────────────
    def _should_trigger_trader(self, cash_map: Dict[str, int], holdings: List[Dict]) -> Tuple[bool, str]:
        """
        트레이더 자동 기동 조건 판단.
        - 보유수량 총합 0
        - 장중 & 매수 시간 창
        - available_cash(없으면 dnca_tot_amt) >= min_cash_to_trigger
        - 쿨다운 내 중복 트리거 방지
        """
        if not self.auto_trigger_when_empty:
            return False, "auto_trigger_trader_when_empty=False"

        # 보유수량 총합
        total_qty = sum(int(str(h.get("hldg_qty", 0)).replace(",", "")) for h in holdings)
        if total_qty > 0:
            return False, f"holdings_qty>0 ({total_qty})"

        now = datetime.now(KST)
        if not is_market_hours(now):
            return False, "장외"

        # 매수 시간 창
        if not in_time_windows(now, self.buy_time_windows):
            return False, f"매수 시간대 아님: {self.buy_time_windows}"

        available = int(cash_map.get("available_cash") or cash_map.get("dnca_tot_amt") or 0)
        if available < self.min_cash_to_trigger:
            return False, f"가용 현금 부족 {available:,} < {self.min_cash_to_trigger:,}"

        # 쿨다운
        if not _can_trigger("trigger_trader_when_empty", self.auto_trigger_cooldown_sec):
            return False, "쿨다운"

        return True, "OK"

    def _trigger_trader_pipeline_once(self) -> bool:
        """
        trader.py를 단발 실행. 성공/실패 여부 반환.
        """
        try:
            logger.info("[AUTO] 보유 0 & 조건 충족 → trader.py 자동 기동")
            res = subprocess.run(
                ["python", str(TRADER_SCRIPT_PATH)],
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8",
                timeout=1200,  # 20분 안전 타임아웃
            )
            head = (res.stdout or "")[-600:]
            logger.info("[AUTO] trader.py 완료. tail:\n%s", head)
            _notify("🤖 보유 0 → 트레이딩 파이프라인 자동 기동 완료", key="auto_trigger_trader_ok", cooldown_sec=300)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("[AUTO] trader.py 실패: exit=%s\nstdout:\n%s\nstderr:\n%s",
                         e.returncode, (e.stdout or "")[-600:], (e.stderr or "")[-600:])
            _notify("❌ 보유 0 → 트레이딩 파이프라인 기동 실패", key="auto_trigger_trader_fail", cooldown_sec=300)
        except subprocess.TimeoutExpired:
            logger.error("[AUTO] trader.py 타임아웃")
            _notify("⏱️ 보유 0 → 트레이딩 파이프라인 타임아웃", key="auto_trigger_trader_fail", cooldown_sec=300)
        except FileNotFoundError:
            logger.error("[AUTO] trader.py 경로를 찾지 못함: %s", TRADER_SCRIPT_PATH)
            _notify("❗ trader.py를 찾지 못했습니다.", key="auto_trigger_trader_fail", cooldown_sec=300)
        except Exception as e:
            logger.error("[AUTO] trader.py 기동 중 예외: %s", e, exc_info=True)
            _notify(f"❗ trader.py 기동 중 예외: {e}", key="auto_trigger_trader_fail", cooldown_sec=300)
        return False

    # ── 매도 판단 로직 ────────────────────────────────────────────────
    def check_sell_condition(self, holding: Dict, stock_info: Dict) -> Tuple[str, str, Dict]:
        """
        보유 종목/스크리너 정보 기반 매도 판단 (하이브리드 모드).
        return: ("SELL" or "PARTIAL_SELL" or "KEEP", reason, structured_context)
        
        하이브리드 로직:
        1. 기본 규칙 (안전장치) - 손절가/목표가/RSI/전일종가 이탈/보유일수/부분 익절
        2. 고급 전략 (strategies.py) - 가중치 조합 전략
        """
        ticker = normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
        name = holding.get("prdt_name", "N/A")
        qty = _to_int(holding.get("hldg_qty", 0))
        cur_price = _to_int(holding.get("prpr", 0))  # 현재가
        avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
        eval_amt = _to_int(holding.get("evlu_amt", 0))
        profit_loss = _to_int(holding.get("evlu_pfls_amt", 0))
        profit_rate = _to_float(holding.get("evlu_pfls_rt", 0.0))
        
        # 판단 시작 로그
        logger.info(
            f"[판단시작] {name}({ticker}) | "
            f"수량={qty:,}주, 현재가={cur_price:,}원, 매수가={avg_price:,.0f}원 | "
            f"평가금액={eval_amt:,}원, 손익={profit_loss:+,}원, 손익률={profit_rate:.2f}%"
        )
        
        if qty <= 0 or cur_price <= 0:
            logger.warning(f"[판단종료] {name}({ticker}) 수량/가격 정보 부족 (qty={qty}, price={cur_price})")
            return "KEEP", f"{name}({ticker}) 수량/가격 정보 부족", {"type": "KEEP", "context": {"reason": "insufficient_data"}}

        # stock_info 요약 로깅
        stock_info_summary = {
            "손절가": stock_info.get("손절가"),
            "목표가": stock_info.get("목표가"),
            "RSI": stock_info.get("RSI"),
            "source": stock_info.get("source"),
            "Price": stock_info.get("Price")
        }
        logger.debug(f"[입력정보] {ticker} stock_info: {stock_info_summary}")

        # 1차: 기본 규칙 체크 (안전장치)
        logger.debug(f"[기본규칙체크] {ticker} 시작")
        basic_decision, basic_reason, basic_context = self._check_basic_rules(holding, stock_info)
        if basic_decision == "SELL":
            logger.info(f"[판단결과] {ticker} 기본규칙 → SELL: {basic_reason}")
            return basic_decision, f"기본규칙: {basic_reason}", basic_context
        elif basic_decision == "PARTIAL_SELL":
            logger.info(f"[판단결과] {ticker} 기본규칙 → PARTIAL_SELL: {basic_reason}")
            return basic_decision, f"기본규칙: {basic_reason}", basic_context

        # 2차: 고급 전략 체크 (strategies.py)
        if self.strategy_mode in ["advanced", "hybrid"] and self.enable_advanced and self.strategy_mixer:
            logger.debug(f"[고급전략체크] {ticker} 시작 (mode={self.strategy_mode})")
            advanced_decision, advanced_reason, advanced_context = self._check_advanced_strategies(holding, stock_info)
            if advanced_decision == "SELL":
                logger.info(f"[판단결과] {ticker} 고급전략 → SELL: {advanced_reason}")
                return advanced_decision, f"고급전략: {advanced_reason}", advanced_context
            else:
                logger.debug(f"[고급전략체크] {ticker} → KEEP: {advanced_reason}")

        # 기본 규칙에서 유지 판단된 경우
        logger.info(f"[판단결과] {ticker} → KEEP: {basic_reason}")
        return basic_decision, basic_reason, basic_context

    def _check_basic_rules(self, holding: Dict, stock_info: Dict) -> Tuple[str, str, Dict]:
        """Phase 1: 당일 매도 방지 체크 추가"""
        ticker = normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
        
        # Phase 1.4: 당일 매도 방지 체크
        trading_params = self.config.get("trading_params", {})
        min_holding_hours = trading_params.get("min_holding_hours", 0)
        if min_holding_hours > 0:
            is_eligible, holding_hours = check_min_holding_hours(ticker, min_holding_hours)
            if not is_eligible:
                logger.info(
                    f"[{ticker}] ⚠️ 당일 매도 방지: 보유시간 {holding_hours:.1f}시간 < 최소 {min_holding_hours}시간 → 유지"
                )
                return (
                    "KEEP",
                    f"당일 매도 방지 (보유시간 {holding_hours:.1f}시간 < 최소 {min_holding_hours}시간)",
                    {
                        "type": "KEEP_SAME_DAY",
                        "context": {
                            "holding_hours": holding_hours,
                            "min_holding_hours": min_holding_hours,
                            "reason": "same_day_sell_prevented"
                        }
                    }
                )
        
        # 기존 로직 계속
        """기본 매도 규칙 체크 (기존 로직)"""
        ticker = normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
        name = holding.get("prdt_name", "N/A")
        qty = _to_int(holding.get("hldg_qty", 0))
        cur_price = _to_int(holding.get("prpr", 0))

        # 입력 손절/목표 우선
        stop_px_in = _to_float(stock_info.get("손절가"), 0.0)
        take_px_in = _to_float(stock_info.get("목표가"), 0.0)
        levels_source = str(stock_info.get("source") or "").strip()

        # 없으면 퍼센트 백업 즉시 산출
        if stop_px_in <= 0 or take_px_in <= 0:
            # 매수 평균가 우선 사용, 없으면 현재가 사용
            entry_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
            if entry_price <= 0:
                entry_price = float(cur_price)
                logger.warning(f"[{ticker}] 매수 평균가 없음, 현재가로 손절가 계산: {entry_price:,}원")
            else:
                logger.debug(f"[{ticker}] 매수 평균가 기준 손절가 계산: {entry_price:,}원")
            backup = _percent_backup_levels(entry_price, self.config.get("risk_params", {}) or {})
            stop_px = float(backup["손절가"]); take_px = float(backup["목표가"])
            levels_source = "percent_backup"
        else:
            stop_px, take_px = float(stop_px_in), float(take_px_in)
            if not levels_source:
                levels_source = "unknown"

        # 버퍼 적용
        stop_threshold = stop_px * (1.0 + self.rules.stop_loss_buffer) if (self.rules.stop_loss_buffer and stop_px > 0) else stop_px
        tp_threshold   = take_px * (1.0 - self.rules.take_profit_buffer) if (self.rules.take_profit_buffer and take_px > 0) else take_px
        
        # 손절가 계산 디버깅 로그
        logger.debug(f"[{ticker}] 손절가 체크: 현재가={cur_price:,}, 손절가={stop_px:,.0f}, 손절임계값={stop_threshold:,.0f}, 손절버퍼={self.rules.stop_loss_buffer}, 도달여부={cur_price <= stop_threshold}")

        # RSI 확보 (미존재 시 50.0 + 지표부재 표기)
        rsi_raw = stock_info.get("RSI")
        rsi_missing = (rsi_raw is None or str(rsi_raw).strip() == "")
        rsi = _to_float(rsi_raw, 50.0)
        rsi_note = " (지표부재)" if rsi_missing else ""
        logger.debug(f"[{ticker}] RSI: {rsi:.1f}{rsi_note}, RSI_TP 임계값={self.rules.rsi_take_profit}")

        # 공통: 시간대 허용 여부
        now_kst = datetime.now(KST)
        sell_win_ok = True if not self.rules.time_windows_for_sells else in_time_windows(now_kst, self.rules.time_windows_for_sells)
        tp_base = self.rules.time_windows_for_take_profit or self.rules.time_windows_for_sells
        tp_win_ok = True if not tp_base else in_time_windows(now_kst, tp_base)
        logger.debug(f"[{ticker}] 시간대 체크: 현재={now_kst.strftime('%H:%M:%S')}, 매도허용={sell_win_ok}, 이익실현허용={tp_win_ok}, 매도시간대={self.rules.time_windows_for_sells}")
        
        # 판단 조건 요약 로그
        logger.info(
            f"[판단조건] {name}({ticker}) | "
            f"손절: 현재가={cur_price:,} vs 임계값={stop_threshold:,.0f} ({'도달' if cur_price <= stop_threshold else '미도달'}) | "
            f"목표: 현재가={cur_price:,} vs 임계값={tp_threshold:,.0f} ({'도달' if cur_price >= tp_threshold else '미도달'}) | "
            f"RSI: {rsi:.1f} vs {self.rules.rsi_take_profit} ({'과열' if (self.rules.rsi_take_profit and rsi >= self.rules.rsi_take_profit) else '정상'})"
        )

        # 0) [NEW] 긴급 낙폭(드로우다운) 손절 (평가손익률 기반)
        logger.debug(f"[{ticker}] 긴급낙폭체크] 임계값={self.emergency_drop_pct:.2%}, 시간대허용={sell_win_ok}")
        try:
            pl_rate = holding.get("evlu_pfls_rt")
            profit_pct = None
            if pl_rate is not None and str(pl_rate) != "":
                profit_pct = float(str(pl_rate).replace("%", "").replace(",", "")) / 100.0
            else:
                avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
                if avg_price > 0 and cur_price > 0:
                    profit_pct = (float(cur_price) - float(avg_price)) / float(avg_price)
            if profit_pct is not None:
                logger.debug(f"[{ticker}] 긴급낙폭] 손익률={profit_pct:.4f}, 임계값={-abs(self.emergency_drop_pct):.4f}, 도달여부={profit_pct <= -abs(self.emergency_drop_pct)}")
            if profit_pct is not None and self.emergency_drop_pct > 0 and profit_pct <= -abs(self.emergency_drop_pct) and sell_win_ok:
                logger.info(f"[{ticker}] ✅ 긴급낙폭 손절 조건 충족: {name}({ticker}) 손익률 {profit_pct:.2%} ≤ -{self.emergency_drop_pct:.2%}")
                return (
                    "SELL",
                    f"긴급 낙폭 손절({profit_pct:.2%} ≤ -{self.emergency_drop_pct:.2%}) | 전략=EmergencyDrop, levels_source={levels_source} | win={self.rules.time_windows_for_sells or 'ALL'}",
                    {
                        "type": "EmergencyDrop",
                        "context": {
                            "profit_pct": profit_pct,
                            "threshold": -abs(self.emergency_drop_pct),
                            "levels_source": levels_source,
                            "time_window": self.rules.time_windows_for_sells or 'ALL'
                        }
                    }
                )
        except Exception:
            pass

        # 1) 손절 전략 (시간대 필터 적용)
        logger.debug(f"[{ticker}] 손절가체크] 손절가={stop_px:,.0f}, 임계값={stop_threshold:,.0f}, 현재가={cur_price:,}, 조건={stop_threshold > 0 and cur_price <= stop_threshold}")
        if stop_threshold > 0 and cur_price <= stop_threshold:
            if not sell_win_ok:
                logger.info(f"[{ticker}] ⚠️ 손절가 도달했으나 매도 시간대 아님: 현재가={cur_price:,}, 손절임계값={stop_threshold:,.0f}, 시간대={self.rules.time_windows_for_sells}")
            else:
                # 최소 보유일수 체크
                rotation_config = self.config.get("rotation", {})
                min_holding_days = rotation_config.get("min_holding_days", 0)
                is_eligible, holding_days = check_min_holding_period(ticker, min_holding_days)
                
                if min_holding_days > 0 and not is_eligible:
                    logger.info(
                        f"[{ticker}] ⚠️ 손절가 도달했으나 최소 보유일수 미충족: "
                        f"현재가={cur_price:,}, 손절임계값={stop_threshold:,.0f}, "
                        f"보유일수={holding_days}일 < 최소 {min_holding_days}일 → 유지"
                    )
                    return (
                        "KEEP",
                        f"손절가 도달했으나 최소 보유일수 미충족 ({holding_days}일 < {min_holding_days}일) | "
                        f"현재가={cur_price:,}, 손절임계값={stop_threshold:,.0f}",
                        {
                            "type": "KEEP_MIN_HOLDING",
                            "context": {
                                "current_price": cur_price,
                                "stop_threshold": int(round(stop_threshold)),
                                "holding_days": holding_days,
                                "min_holding_days": min_holding_days,
                                "reason": "min_holding_period_not_met"
                            }
                        }
                    )
                
                # 최소 보유일수 충족 또는 체크 없음 → 손절 진행
                log_msg = (
                    f"[{ticker}] ✅ 손절가 도달 조건 충족: 현재가={cur_price:,} ≤ 손절임계값={stop_threshold:,.0f} "
                    f"(손절가={stop_px:,.0f}, 버퍼={self.rules.stop_loss_buffer}"
                )
                if holding_days > 0:
                    log_msg += f", 보유일수={holding_days}일"
                log_msg += ")"
                logger.warning(log_msg)
                
                context = {
                    "type": "StopLoss",
                    "context": {
                        "current_price": cur_price,
                        "stop_threshold": int(round(stop_threshold)),
                        "stop_price": int(round(stop_px)),
                        "stop_loss_buffer": self.rules.stop_loss_buffer,
                        "levels_source": levels_source,
                        "time_window": self.rules.time_windows_for_sells or 'ALL'
                    }
                }
                if holding_days > 0:
                    context["context"]["holding_days"] = holding_days
                
                return (
                    "SELL",
                    f"손절가 도달({cur_price:,} ≤ {int(round(stop_threshold)):,}) | "
                    f"전략=StopLoss, levels_source={levels_source} | "
                    f"win={self.rules.time_windows_for_sells or 'ALL'}",
                    context
                )
        # Phase 2: 부분 익절 체크 (목표가 도달 전)
        auto_sell_config = self.config.get("risk_params", {}).get("auto_sell", {})
        partial_profit_pct = auto_sell_config.get("partial_profit_pct", 0.0)
        partial_profit_ratio = auto_sell_config.get("partial_profit_ratio", 0.0)
        
        if partial_profit_pct > 0 and partial_profit_ratio > 0:
            avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
            if avg_price > 0:
                profit_pct = (cur_price - avg_price) / avg_price
                # 수정: tp_threshold 대신 take_px 사용 (목표가 기준 비교)
                # take_px가 0이거나 유효하지 않으면 부분 익절을 건너뜀 (목표가가 없으면 부분 익절 불가)
                if take_px > 0:
                    target_profit_pct = (take_px / avg_price - 1.0) if avg_price > 0 else 1.0
                    
                    if profit_pct >= partial_profit_pct and profit_pct < target_profit_pct:
                        # 부분 익절 조건 충족 (목표가 도달 전)
                        # 시간대 필터 확인 (부분 익절도 매도이므로 시간대 체크 필요)
                        partial_win_ok = tp_win_ok  # 목표가 시간대 필터 사용 (없으면 매도 시간대 사용)
                        if not partial_win_ok:
                            logger.info(
                                f"[{ticker}] ⚠️ 부분 익절 조건 충족했으나 매도 시간대 아님: "
                                f"수익률 {profit_pct:.2%} ≥ {partial_profit_pct:.2%}, "
                                f"시간대={self.rules.time_windows_for_take_profit or self.rules.time_windows_for_sells}"
                            )
                            # 시간대가 아니면 유지 (다음 사이클에서 다시 체크)
                            return (
                                "KEEP",
                                f"부분 익절 조건 충족했으나 매도 시간대 아님 ({profit_pct:.2%} ≥ {partial_profit_pct:.2%})",
                                {
                                    "type": "KEEP_TIME_WINDOW",
                                    "context": {
                                        "profit_pct": profit_pct,
                                        "partial_profit_pct": partial_profit_pct,
                                        "time_window": self.rules.time_windows_for_take_profit or self.rules.time_windows_for_sells,
                                        "reason": "time_window_not_allowed"
                                    }
                                }
                            )
                        
                        logger.info(
                            f"[{ticker}] ✅ 부분 익절 조건 충족: 수익률 {profit_pct:.2%} ≥ {partial_profit_pct:.2%}, "
                            f"일부 {partial_profit_ratio:.0%} 익절"
                        )
                        return (
                            "PARTIAL_SELL",
                            f"부분 익절({profit_pct:.2%} ≥ {partial_profit_pct:.2%}, {partial_profit_ratio:.0%} 익절) | 전략=PartialProfit, levels_source={levels_source} | win={self.rules.time_windows_for_take_profit or self.rules.time_windows_for_sells or 'ALL'}",
                            {
                                "type": "PartialProfit",
                                "context": {
                                    "current_price": cur_price,
                                    "profit_pct": profit_pct,
                                    "partial_profit_pct": partial_profit_pct,
                                    "partial_profit_ratio": partial_profit_ratio,
                                    "levels_source": levels_source,
                                    "time_window": self.rules.time_windows_for_take_profit or self.rules.time_windows_for_sells or 'ALL'
                                }
                            }
                        )
        
        # 2) 목표가 전략 (시간대 필터 적용: 없으면 전반 윈도우 사용)
        logger.debug(f"[{ticker}] 목표가체크] 목표가={take_px:,.0f}, 임계값={tp_threshold:,.0f}, 현재가={cur_price:,}, 조건={tp_threshold > 0 and cur_price >= tp_threshold and tp_win_ok}")
        if tp_threshold > 0 and cur_price >= tp_threshold and tp_win_ok:
            logger.info(f"[{ticker}] ✅ 목표가 도달 조건 충족: 현재가={cur_price:,} ≥ 목표임계값={tp_threshold:,.0f} (목표가={take_px:,.0f}, 버퍼={self.rules.take_profit_buffer})")
            return (
                "SELL",
                f"목표가 도달({cur_price:,} ≥ {int(round(tp_threshold)):,}) | 전략=TakeProfit, levels_source={levels_source} | win={self.rules.time_windows_for_take_profit or self.rules.time_windows_for_sells or 'ALL'}",
                {
                    "type": "TakeProfit",
                    "context": {
                        "current_price": cur_price,
                        "take_profit_threshold": int(round(tp_threshold)),
                        "levels_source": levels_source,
                        "time_window": self.rules.time_windows_for_take_profit or self.rules.time_windows_for_take_profit or self.rules.time_windows_for_sells or 'ALL'
                    }
                }
            )
        # 3) RSI 과열 전략 (개선된 버전 또는 기존 버전)
        rsi_strategy = self.config.get("risk_params", {}).get("rsi_sell_strategy", {})
        use_improved_rsi = rsi_strategy.get("enabled", False)
        
        if use_improved_rsi:
            # [NEW] 개선된 RSI 로직 사용
            improved_result = self._check_improved_rsi_strategy(holding, stock_info, rsi_strategy)
            if improved_result[0] != "KEEP" or improved_result[0] == "KEEP" and "RSI 경고" not in improved_result[1]:
                # KEEP이 아니거나 경고가 아닌 경우 반환
                return improved_result
            # 경고 단계면 계속 진행 (다음 조건 체크)
        
        # [기존] 하위 호환성: 기존 로직 사용
        logger.debug(f"[{ticker}] RSI체크] RSI={rsi:.1f}, 임계값={self.rules.rsi_take_profit}, 조건={self.rules.rsi_take_profit is not None and rsi >= float(self.rules.rsi_take_profit)}")
        if self.rules.rsi_take_profit is not None and rsi >= float(self.rules.rsi_take_profit):
            # RSI 극단값 검증 (100.0은 비정상적으로 높음)
            if rsi >= 99.0:
                logger.warning(
                    f"[{ticker}] ⚠️ RSI 극단값 감지: {rsi:.1f} (14일 연속 상승 가능성 또는 데이터 문제) "
                    f"→ 추가 검증 필요"
                )
            
            # 최소 수익률 체크 (수수료 고려)
            avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
            if avg_price > 0:
                profit_pct = (cur_price - avg_price) / avg_price
                # 최소 수익률 임계값 (기본 0.5%, 수수료 고려)
                min_profit_pct = float(self.config.get("risk_params", {}).get("rsi_min_profit_pct", 0.005))
                
                if profit_pct < min_profit_pct:
                    logger.info(
                        f"[{ticker}] ⚠️ RSI 과열 조건 충족했으나 최소 수익률 미충족: "
                        f"RSI={rsi:.1f} ≥ {float(self.rules.rsi_take_profit):.1f}, "
                        f"수익률={profit_pct:.2%} < 최소 {min_profit_pct:.2%} (수수료 고려) → 유지"
                    )
                    return (
                        "KEEP",
                        f"RSI 과열이지만 최소 수익률 미충족 ({profit_pct:.2%} < {min_profit_pct:.2%}) | "
                        f"RSI={rsi:.1f} ≥ {float(self.rules.rsi_take_profit):.1f}{rsi_note}",
                        {
                            "type": "KEEP_MIN_PROFIT",
                            "context": {
                                "rsi_value": rsi,
                                "rsi_threshold": float(self.rules.rsi_take_profit),
                                "profit_pct": profit_pct,
                                "min_profit_pct": min_profit_pct,
                                "rsi_missing": rsi_missing,
                                "reason": "min_profit_not_met"
                            }
                        }
                    )
                
                # 목표가 미도달 시 RSI 임계값 상향 조정 (더 보수적으로)
                take_px = _to_float(stock_info.get("목표가"), 0.0)
                if take_px > 0:
                    target_profit_pct = (take_px - avg_price) / avg_price if avg_price > 0 else 0.0
                    # 목표가 대비 현재 수익률 비율
                    progress_to_target = profit_pct / target_profit_pct if target_profit_pct > 0 else 0.0
                    
                    # 목표가의 50% 미만 도달 시 RSI 임계값을 더 높게 설정 (기본 75.0 → 85.0)
                    if progress_to_target < 0.5:
                        adjusted_rsi_threshold = float(self.config.get("risk_params", {}).get("rsi_take_profit_early", 85.0))
                        if rsi < adjusted_rsi_threshold:
                            logger.info(
                                f"[{ticker}] ⚠️ RSI 과열이지만 목표가 미도달로 임계값 상향 조정: "
                                f"RSI={rsi:.1f} < 조정된 임계값={adjusted_rsi_threshold:.1f} "
                                f"(목표가 진행률={progress_to_target:.1%}) → 유지"
                            )
                            return (
                                "KEEP",
                                f"RSI 과열이지만 목표가 미도달로 임계값 상향 조정 "
                                f"(RSI={rsi:.1f} < {adjusted_rsi_threshold:.1f}, 목표가 진행률={progress_to_target:.1%})",
                                {
                                    "type": "KEEP_TARGET_PROGRESS",
                                    "context": {
                                        "rsi_value": rsi,
                                        "rsi_threshold": float(self.rules.rsi_take_profit),
                                        "adjusted_rsi_threshold": adjusted_rsi_threshold,
                                        "profit_pct": profit_pct,
                                        "target_profit_pct": target_profit_pct,
                                        "progress_to_target": progress_to_target,
                                        "reason": "target_progress_too_low"
                                    }
                                }
                            )
            
            # 최소 보유일수 체크
            rotation_config = self.config.get("rotation", {})
            min_holding_days = rotation_config.get("min_holding_days", 0)
            is_eligible, holding_days = check_min_holding_period(ticker, min_holding_days)
            
            if min_holding_days > 0 and not is_eligible:
                logger.info(
                    f"[{ticker}] ⚠️ RSI 과열 조건 충족했으나 최소 보유일수 미충족: "
                    f"RSI={rsi:.1f} ≥ {float(self.rules.rsi_take_profit):.1f}, "
                    f"보유일수={holding_days}일 < 최소 {min_holding_days}일 → 유지"
                )
                return (
                    "KEEP",
                    f"RSI 과열 조건 충족했으나 최소 보유일수 미충족 ({holding_days}일 < {min_holding_days}일) | "
                    f"RSI={rsi:.1f} ≥ {float(self.rules.rsi_take_profit):.1f}{rsi_note}",
                    {
                        "type": "KEEP_MIN_HOLDING",
                        "context": {
                            "rsi_value": rsi,
                            "rsi_threshold": float(self.rules.rsi_take_profit),
                            "holding_days": holding_days,
                            "min_holding_days": min_holding_days,
                            "rsi_missing": rsi_missing,
                            "reason": "min_holding_period_not_met"
                        }
                    }
                )
            
            # 최소 보유일수 충족 또는 체크 없음 → RSI 과열 매도 진행
            profit_pct_str = ""
            if avg_price > 0:
                profit_pct = (cur_price - avg_price) / avg_price
                profit_pct_str = f", 수익률={profit_pct:.2%}"
            
            log_msg = (
                f"[{ticker}] ✅ RSI 과열 조건 충족: RSI={rsi:.1f} ≥ {float(self.rules.rsi_take_profit):.1f}{rsi_note}{profit_pct_str}"
            )
            if holding_days > 0:
                log_msg += f", 보유일수={holding_days}일"
            logger.info(log_msg)
            
            context = {
                "type": "RSI_OVERBOUGHT",
                "context": {
                    "rsi_value": rsi,
                    "rsi_threshold": float(self.rules.rsi_take_profit),
                    "rsi_missing": rsi_missing,
                    "levels_source": levels_source
                }
            }
            if holding_days > 0:
                context["context"]["holding_days"] = holding_days
            if avg_price > 0:
                profit_pct = (cur_price - avg_price) / avg_price
                context["context"]["profit_pct"] = profit_pct
            
            return (
                "SELL",
                f"RSI 과열({rsi:.1f}≥{float(self.rules.rsi_take_profit):.1f}{rsi_note}) | 전략=RSI_TP, levels_source={levels_source}",
                context
            )
        # 4) 전일 종가 이탈 전략 (시간대 필터 + 버퍼)
        logger.debug(f"[{ticker}] 전일종가체크] 활성화={self.rules.prev_close_break_sell}, 시간대허용={sell_win_ok}")
        if self.rules.prev_close_break_sell and sell_win_ok:
            prev_close = self._get_prev_close(ticker)
            if prev_close and prev_close > 0:
                thresh = int(round(prev_close * (1.0 - float(self.rules.prev_close_buffer_pct))))
                logger.debug(f"[{ticker}] 전일종가체크] 전일종가={prev_close:,}, 임계값={thresh:,}, 현재가={cur_price:,}, 조건={cur_price <= thresh}")
                if cur_price <= thresh:
                    # 최소 보유일수 체크
                    is_eligible, holding_days = self._check_min_holding_period(ticker)
                    rotation_config = self.config.get("rotation", {})
                    min_holding_days = rotation_config.get("min_holding_days", 0)
                    
                    if min_holding_days > 0 and not is_eligible:
                        logger.info(
                            f"[{ticker}] ⚠️ 전일 종가 이탈 조건 충족했으나 최소 보유일수 미충족: "
                            f"현재가={cur_price:,} ≤ 임계값={thresh:,}, "
                            f"보유일수={holding_days}일 < 최소 {min_holding_days}일 → 유지"
                        )
                        return (
                            "KEEP",
                            f"전일 종가 이탈 조건 충족했으나 최소 보유일수 미충족 ({holding_days}일 < {min_holding_days}일) | "
                            f"현재가={cur_price:,} ≤ 임계값={thresh:,} (전일종가={prev_close:,})",
                            {
                                "type": "KEEP_MIN_HOLDING",
                                "context": {
                                    "current_price": cur_price,
                                    "threshold": thresh,
                                    "prev_close": prev_close,
                                    "holding_days": holding_days,
                                    "min_holding_days": min_holding_days,
                                    "reason": "min_holding_period_not_met"
                                }
                            }
                        )
                    
                    # 최소 보유일수 충족 또는 체크 없음 → 전일 종가 이탈 매도 진행
                    confirm_note = f", confirm={self.rules.confirm_bars_for_break}D" if self.rules.confirm_bars_for_break > 0 else ""
                    log_msg = (
                        f"[{ticker}] ✅ 전일 종가 이탈 조건 충족: 현재가={cur_price:,} ≤ 임계값={thresh:,} "
                        f"(전일종가={prev_close:,}, 버퍼={self.rules.prev_close_buffer_pct:.2%}"
                    )
                    if holding_days > 0:
                        log_msg += f", 보유일수={holding_days}일"
                    log_msg += ")"
                    logger.info(log_msg)
                    
                    context = {
                        "type": "PrevCloseBreak",
                        "context": {
                            "current_price": cur_price,
                            "threshold": thresh,
                            "prev_close": prev_close,
                            "confirm_bars": self.rules.confirm_bars_for_break,
                            "levels_source": levels_source,
                            "time_window": self.rules.time_windows_for_sells or 'ALL'
                        }
                    }
                    if holding_days > 0:
                        context["context"]["holding_days"] = holding_days
                    
                    return (
                        "SELL",
                        f"전일 종가 이탈({cur_price:,} ≤ {thresh:,}) | 전략=PrevCloseBreak{confirm_note}, levels_source={levels_source} | prev_close={prev_close:,} | win={self.rules.time_windows_for_sells or 'ALL'}",
                        context
                    )
        # 5) 보유일수 상한
        logger.debug(f"[{ticker}] 보유일수체크] 최대보유일수={self.rules.max_holding_days}, 진입일={stock_info.get('entry_date')}")
        if self.rules.max_holding_days and stock_info.get("entry_date"):
            try:
                dt = datetime.fromisoformat(str(stock_info["entry_date"]))
                days = (datetime.now(KST) - dt.astimezone(KST)).days
                logger.debug(f"[{ticker}] 보유일수체크] 보유일수={days}일, 최대={int(self.rules.max_holding_days)}일, 조건={days >= int(self.rules.max_holding_days)}")
                if days >= int(self.rules.max_holding_days):
                    logger.info(f"[{ticker}] ✅ 보유일수 초과 조건 충족: {days}일 ≥ {int(self.rules.max_holding_days)}일")
                    return (
                        "SELL",
                        f"보유일수 초과({days}d ≥ {int(self.rules.max_holding_days)}d) | 전략=MaxHoldingDays, levels_source={levels_source}",
                        {
                            "type": "MaxHoldingDays",
                            "context": {
                                "holding_days": days,
                                "max_holding_days": int(self.rules.max_holding_days),
                                "levels_source": levels_source
                            }
                        }
                    )
            except Exception:
                pass

        # 모든 매도 조건 미충족 → 유지
        logger.info(
            f"[{ticker}] ✅ 모든 매도 조건 미충족 → 유지 결정 | "
            f"손절: {cur_price:,} > {stop_threshold:,.0f} | "
            f"목표: {cur_price:,} < {tp_threshold:,.0f} | "
            f"RSI: {rsi:.1f} < {self.rules.rsi_take_profit if self.rules.rsi_take_profit else 'N/A'} | "
            f"전일종가이탈: {('활성화' if self.rules.prev_close_break_sell else '비활성화')} | "
            f"보유일수: {('정상' if not self.rules.max_holding_days else '정상')}"
        )
        return (
            "KEEP",
            f"유지: {name}({ticker}) 현재가={cur_price:,}, 손절={int(round(stop_px)) if stop_px else 'N/A'}, "
            f"목표={int(round(take_px)) if take_px else 'N/A'}, RSI={rsi:.1f}{rsi_note}, levels_source={levels_source}"
            f", win_sell={self.rules.time_windows_for_sells or 'ALL'}",
            {
                "type": "KEEP",
                "context": {
                    "current_price": cur_price,
                    "stop_price": int(round(stop_px)) if stop_px else None,
                    "take_profit_price": int(round(take_px)) if take_px else None,
                    "rsi": rsi,
                    "rsi_missing": rsi_missing,
                    "levels_source": levels_source,
                    "time_window": self.rules.time_windows_for_sells or 'ALL'
                }
            }
        )

    def _check_advanced_strategies(self, holding: Dict, stock_info: Dict) -> Tuple[str, str, Dict]:
        """고급 전략 체크 (strategies.py 사용)"""
        ticker = normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
        name = holding.get("prdt_name", "N/A")
        cur_price = _to_int(holding.get("prpr", 0))
        avg_price = _to_float(holding.get("pchs_avg_pric"), 0.0)
        
        # 손익률 계산
        profit_pct = None
        if avg_price > 0 and cur_price > 0:
            profit_pct = (cur_price - avg_price) / avg_price
        
        try:
            logger.debug(f"[{ticker}] 고급전략] StrategyMixer 실행 시작")
            # strategies.py의 StrategyMixer 사용
            should_sell, reason = self.strategy_mixer.decide_sell(holding, stock_info)
            
            # 손실 상태에서 고급전략 매도 신호 억제
            if should_sell and profit_pct is not None and profit_pct < 0:
                # 손절가 임계값 확인 (기본 -3%)
                stop_loss_pct = float(self.config.get("risk_params", {}).get("stop_pct", 0.03))
                # 손실이 손절가 임계값의 80% 이상이면 고급전략 매도 허용 (예: -2.4% 이상 손실)
                # 그 외에는 고급전략 매도 억제
                if profit_pct > -stop_loss_pct * 0.8:
                    logger.info(
                        f"[{ticker}] ⚠️ 고급전략 매도 신호 발생했으나 손실 상태에서 억제: "
                        f"수익률={profit_pct:.2%}, 손절가 임계값={-stop_loss_pct:.2%} → 유지"
                    )
                    return (
                        "KEEP",
                        f"고급전략 매도 신호였으나 손실 상태에서 억제 ({profit_pct:.2%} > {-stop_loss_pct*0.8:.2%}) | 원인: {reason}",
                        {
                            "type": "KEEP_LOSS_PROTECTION",
                            "context": {
                                "strategy_mixer": str(type(self.strategy_mixer).__name__),
                                "reason": reason,
                                "profit_pct": profit_pct,
                                "stop_loss_pct": stop_loss_pct,
                                "suppressed": True
                            }
                        }
                    )
                else:
                    # 손절가 근처(-2.4% 이하 손실)에서는 고급전략 매도 허용
                    logger.info(
                        f"[{ticker}] ✅ 고급전략 매도 신호 + 손절가 근처 손실: "
                        f"수익률={profit_pct:.2%} ≤ {-stop_loss_pct*0.8:.2%} → 매도 진행"
                    )
            
            logger.info(f"[{ticker}] 고급전략] 판단 결과: {'SELL' if should_sell else 'KEEP'} - {reason}")
            return (
                "SELL" if should_sell else "KEEP", 
                reason,
                {
                    "type": "AdvancedStrategy",
                    "context": {
                        "strategy_mixer": str(type(self.strategy_mixer).__name__),
                        "reason": reason,
                        "profit_pct": profit_pct
                    }
                }
            )
        except Exception as e:
            logger.warning(f"[{ticker}] 고급 전략 실행 중 오류: {e}")
            return (
                "KEEP", 
                f"고급전략 오류: {e}",
                {
                    "type": "AdvancedStrategyError",
                    "context": {
                        "error": str(e),
                        "strategy_mixer": str(type(self.strategy_mixer).__name__) if self.strategy_mixer else "None"
                    }
                }
            )
    
    # ── 개선된 RSI 전략 확인 함수들 ────────────────────────────────────────
    def _check_rsi_turning_down(self, ticker: str, current_rsi: float) -> bool:
        """
        RSI 하락 전환 확인: 이전 RSI >= 80 AND 현재 RSI < 이전 RSI
        
        Args:
            ticker: 종목 코드
            current_rsi: 현재 RSI 값
        
        Returns:
            RSI 하락 전환 여부
        """
        try:
            from recorder import get_rsi_history
            history = get_rsi_history(ticker, days=5)
            if len(history) >= 2:
                prev_rsi = history[-2]['rsi_value']
                return prev_rsi >= 80.0 and current_rsi < prev_rsi
            return False
        except Exception as e:
            logger.debug(f"[{ticker}] RSI 하락 전환 확인 실패: {e}")
            return False
    
    def _check_bearish_divergence(self, ticker: str, df: pd.DataFrame) -> bool:
        """
        약세 다이버전스 확인
        
        Args:
            ticker: 종목 코드
            df: OHLCV 데이터프레임
        
        Returns:
            약세 다이버전스 감지 여부
        """
        try:
            from screener_core import detect_bearish_divergence
            return detect_bearish_divergence(df, lookback_period=10)
        except Exception as e:
            logger.debug(f"[{ticker}] 약세 다이버전스 확인 실패: {e}")
            return False
    
    def _check_trend_weakening(self, stock_info: Dict) -> bool:
        """
        추세 약화 확인: close < MA20 OR MA20_slope < 0
        
        Args:
            stock_info: 종목 정보 딕셔너리
        
        Returns:
            추세 약화 여부
        """
        try:
            current_price = stock_info.get("Price", 0)
            ma20 = stock_info.get("MA20", 0)
            ma20_slope = stock_info.get("MA20_slope", 0)
            
            price_below_ma20 = (ma20 > 0 and current_price < ma20)
            ma20_slope_negative = (ma20_slope < 0)
            
            return price_below_ma20 or ma20_slope_negative
        except Exception as e:
            logger.debug(f"추세 약화 확인 실패: {e}")
            return False
    
    def _check_volume_weakening(self, stock_info: Dict) -> bool:
        """
        거래량 약화 확인: avg(volume last 3) < avg(volume last 10)
        
        Args:
            stock_info: 종목 정보 딕셔너리
        
        Returns:
            거래량 약화 여부
        """
        try:
            volume_ratio = stock_info.get("volume_ratio", 1.0)
            return volume_ratio < 1.0  # 3일 평균 < 10일 평균
        except Exception as e:
            logger.debug(f"거래량 약화 확인 실패: {e}")
            return False
    
    def _evaluate_rsi_sell_conditions(self, holding: Dict, stock_info: Dict, rsi_strategy: Dict) -> Tuple[int, List[str]]:
        """
        RSI 매도 확인 조건 평가 (충족된 조건 개수와 목록 반환)
        
        Args:
            holding: 보유 종목 정보
            stock_info: 종목 지표 정보
            rsi_strategy: RSI 전략 설정
        
        Returns:
            (충족된 조건 개수, 충족된 조건 목록)
        """
        ticker = normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
        current_rsi = stock_info.get("RSI", 50.0)
        satisfied_conditions = []
        
        # A. RSI 하락 전환
        if self._check_rsi_turning_down(ticker, current_rsi):
            satisfied_conditions.append("RSI_TURNING_DOWN")
        
        # B. 약세 다이버전스 (설정에서 활성화된 경우만)
        if rsi_strategy.get("check_divergence", True):
            try:
                from screener_core import get_historical_prices
                from datetime import datetime, timedelta
                end_dt = datetime.now(KST)
                start_dt = end_dt - timedelta(days=30)
                df = get_historical_prices(ticker, start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
                if df is not None and self._check_bearish_divergence(ticker, df):
                    satisfied_conditions.append("BEARISH_DIVERGENCE")
            except Exception as e:
                logger.debug(f"[{ticker}] 다이버전스 확인 실패: {e}")
        
        # C. 추세 약화 (설정에서 활성화된 경우만)
        if rsi_strategy.get("check_trend_weakening", True):
            if self._check_trend_weakening(stock_info):
                satisfied_conditions.append("TREND_WEAKENING")
        
        # D. 거래량 약화 (설정에서 활성화된 경우만)
        if rsi_strategy.get("check_volume_weakening", True):
            if self._check_volume_weakening(stock_info):
                satisfied_conditions.append("VOLUME_WEAKENING")
        
        return len(satisfied_conditions), satisfied_conditions
    
    def _check_improved_rsi_strategy(self, holding: Dict, stock_info: Dict, rsi_strategy: Dict) -> Tuple[str, str, Dict]:
        """
        개선된 RSI 매도 전략 체크
        
        Args:
            holding: 보유 종목 정보
            stock_info: 종목 지표 정보
            rsi_strategy: RSI 전략 설정
        
        Returns:
            (판단 결과, 이유, 컨텍스트)
        """
        ticker = normalize_ticker_6(holding.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
        name = holding.get("prdt_name", "N/A")
        rsi = _to_float(stock_info.get("RSI"), 50.0)
        cur_price = _to_int(holding.get("prpr", 0))
        levels_source = str(stock_info.get("source") or "").strip()
        
        warning_level = rsi_strategy.get("warning_level", 75.0)
        sell_candidate_level = rsi_strategy.get("sell_candidate_level", 80.0)
        full_exit_level = rsi_strategy.get("full_exit_level", 70.0)
        confirm_required = rsi_strategy.get("confirm_conditions_required", 2)
        mode = rsi_strategy.get("mode", "partial")
        partial_ratio = rsi_strategy.get("partial_ratio", 0.3)
        
        # 경고 단계: 모니터링만
        if warning_level <= rsi < sell_candidate_level:
            logger.debug(f"[{ticker}] RSI 경고 단계: {rsi:.1f} (매도 없음)")
            return (
                "KEEP",
                f"RSI 경고 단계({rsi:.1f})",
                {
                    "type": "KEEP_RSI_WARNING",
                    "context": {
                        "rsi_value": rsi,
                        "warning_level": warning_level,
                        "sell_candidate_level": sell_candidate_level
                    }
                }
            )
        
        # 매도 후보 단계: 확인 조건 평가
        if rsi >= sell_candidate_level:
            # 최소 보유일수 체크
            rotation_config = self.config.get("rotation", {})
            min_holding_days = rotation_config.get("min_holding_days", 0)
            is_eligible, holding_days = check_min_holding_period(ticker, min_holding_days)
            
            if min_holding_days > 0 and not is_eligible:
                logger.info(
                    f"[{ticker}] ⚠️ RSI 과열 조건 충족했으나 최소 보유일수 미충족: "
                    f"RSI={rsi:.1f} ≥ {sell_candidate_level:.1f}, "
                    f"보유일수={holding_days}일 < 최소 {min_holding_days}일 → 유지"
                )
                return (
                    "KEEP",
                    f"RSI 과열이지만 최소 보유일수 미충족 ({holding_days}일 < {min_holding_days}일)",
                    {
                        "type": "KEEP_MIN_HOLDING",
                        "context": {
                            "rsi_value": rsi,
                            "holding_days": holding_days,
                            "min_holding_days": min_holding_days
                        }
                    }
                )
            
            # 확인 조건 평가
            condition_count, satisfied_conditions = self._evaluate_rsi_sell_conditions(holding, stock_info, rsi_strategy)
            
            if condition_count < confirm_required:
                logger.info(
                    f"[{ticker}] RSI 과열({rsi:.1f})이지만 확인 조건 부족 "
                    f"({condition_count}/{confirm_required}): {satisfied_conditions}"
                )
                return (
                    "KEEP",
                    f"RSI 과열({rsi:.1f})이지만 확인 조건 부족 ({condition_count}/{confirm_required})",
                    {
                        "type": "KEEP_RSI_INSUFFICIENT_CONDITIONS",
                        "context": {
                            "rsi_value": rsi,
                            "condition_count": condition_count,
                            "confirm_required": confirm_required,
                            "satisfied_conditions": satisfied_conditions
                        }
                    }
                )
            
            # 전량 매도 조건 체크
            if rsi < full_exit_level:
                logger.info(f"[{ticker}] ✅ RSI 전량 매도: {rsi:.1f} < {full_exit_level}")
                return (
                    "SELL",
                    f"RSI 전량 매도({rsi:.1f} < {full_exit_level}) | 전략=ImprovedRSI_FullExit, levels_source={levels_source}",
                    {
                        "type": "ImprovedRSI_FullExit",
                        "context": {
                            "rsi_value": rsi,
                            "full_exit_level": full_exit_level,
                            "satisfied_conditions": satisfied_conditions,
                            "levels_source": levels_source
                        }
                    }
                )
            
            # 부분/전량 매도 결정
            if mode == "partial":
                logger.info(
                    f"[{ticker}] ✅ RSI 부분 매도: {rsi:.1f}, 조건={satisfied_conditions}, "
                    f"비율={partial_ratio:.0%}"
                )
                return (
                    "PARTIAL_SELL",
                    f"RSI 부분 매도({rsi:.1f}, {partial_ratio:.0%}) | 전략=ImprovedRSI_Partial, levels_source={levels_source}",
                    {
                        "type": "ImprovedRSI_Partial",
                        "context": {
                            "rsi_value": rsi,
                            "partial_ratio": partial_ratio,
                            "satisfied_conditions": satisfied_conditions,
                            "levels_source": levels_source
                        }
                    }
                )
            else:
                logger.info(f"[{ticker}] ✅ RSI 전량 매도: {rsi:.1f}, 조건={satisfied_conditions}")
                return (
                    "SELL",
                    f"RSI 전량 매도({rsi:.1f}) | 전략=ImprovedRSI_Full, levels_source={levels_source}",
                    {
                        "type": "ImprovedRSI_Full",
                        "context": {
                            "rsi_value": rsi,
                            "satisfied_conditions": satisfied_conditions,
                            "levels_source": levels_source
                        }
                    }
                )
        
        # RSI가 경고 레벨 미만이면 유지
        return (
            "KEEP",
            f"RSI 정상({rsi:.1f})",
            {
                "type": "KEEP_RSI_NORMAL",
                "context": {
                    "rsi_value": rsi
                }
            }
        )

    # ── 상태 요약(디스코드/로그) ────────────────────────────────────────
    def summarize_account_state(self, cash_map: Dict[str, int], holdings: List[Dict]) -> str:
        d2 = cash_map.get("prvs_rcdl_excc_amt", 0)
        nx = cash_map.get("nxdy_excc_amt", 0)
        dn = cash_map.get("dnca_tot_amt", 0)
        total = cash_map.get("tot_evlu_amt", 0) or 0
        return (
            f"보유종목: {len([h for h in holdings if _to_int(h.get('hldg_qty', 0))>0])}개\n"
            f"D+2 출금가능: {d2:,}원\n"
            f"익일 출금가능: {nx:,}원\n"
            f"예수금: {dn:,}원\n"
            f"총평가(요약): {total:,}원"
        )

# ── 실행 루틴 ──────────────────────────────────────────────────────────
def _run_cycle(rm: RiskManager, *, notify_summary: bool = True) -> None:
    """리스크 체크 1회 사이클"""
    # 1) 계좌 스냅샷 갱신 및 요약 로그
    cash, holds, s_path, b_path = rm.refresh_account_snapshot()
    msg = rm.summarize_account_state(cash, holds)
    logger.info("\n" + msg + f"\nfiles: {b_path}, {s_path}")
    if notify_summary:
        _notify(" 계좌 요약\n" + msg, key="risk_summary", cooldown_sec=600)

    # [NEW] 보유 0이면 트레이딩 파이프라인 조건부 기동
    ok, why = rm._should_trigger_trader(cash, holds)
    if ok:
        rm._trigger_trader_pipeline_once()
    else:
        logger.info(f"[AUTO] 파이프라인 미기동: {why}")

    # 2) 각 보유 종목: 손절/목표가/RSI 즉시 계산 후 판단
    if holds:
        # 수량 0인 종목 필터링 (매도 완료 종목 제외)
        valid_holdings = [h for h in holds if _to_int(h.get("hldg_qty", 0)) > 0]
        skipped_count = len(holds) - len(valid_holdings)
        
        if skipped_count > 0:
            skipped_tickers = [
                f"{h.get('prdt_name', 'N/A')}({normalize_ticker_6(h.get('pdno', ''), os.getenv('MARKET', 'NASDAQ100'))})"
                for h in holds
                if _to_int(h.get("hldg_qty", 0)) <= 0
            ]
            logger.debug(
                f"[리스크체크] 수량 0인 종목 {skipped_count}개 제외: {', '.join(skipped_tickers)}"
            )
            
            # 수량 0이지만 평가금액이 있는 경우 경고 (계좌 동기화 지연 감지)
            for h in holds:
                qty = _to_int(h.get("hldg_qty", 0))
                if qty <= 0:
                    ticker = normalize_ticker_6(h.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
                    name = h.get("prdt_name", "N/A")
                    eval_amt = _to_int(h.get("evlu_amt", 0))
                    if eval_amt > 0:
                        logger.warning(
                            f"[리스크체크] {name}({ticker}) 수량 0이지만 평가금액 {eval_amt:,}원 → "
                            f"계좌 동기화 지연 가능성 (다음 사이클에서 재확인)"
                        )
        
        if valid_holdings:
            logger.info(f"[리스크체크] 보유 종목 {len(valid_holdings)}개 모니터링 시작")
            for idx, h in enumerate(valid_holdings, 1):
                ticker = normalize_ticker_6(h.get("pdno", ""), os.getenv("MARKET", "NASDAQ100"))
                name = h.get("prdt_name", "N/A")
                qty = _to_int(h.get("hldg_qty", 0))
                cur_price = _to_float(h.get("prpr"), 0.0)
            
                logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {name}({ticker}) 시작 - 수량={qty:,}주, 현재가={cur_price:,.0f}원")
                
                if cur_price <= 0:
                    logger.warning(f"[리스크체크] [{idx}/{len(valid_holdings)}] {name}({ticker}) 현재가 정보 없음 → 스킵")
                    continue

                logger.debug(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} 실시간 레벨 계산 시작")
                stock_info = rm.compute_realtime_levels(ticker, cur_price)
                logger.debug(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} 매도 조건 판단 시작")
                decision, reason, structured_context = rm.check_sell_condition(h, stock_info)
                if decision == "SELL":
                    log_msg = f" 매도 판단: {reason}"
                    logger.warning(f"[리스크체크] [{idx}/{len(valid_holdings)}] {name}({ticker}) ⚠️ {log_msg}")
                    # 하이라이트된 embed 메시지 전송
                    _notify_sell_embed(h, reason, "SELL", structured_context, 
                                     key="risk_sell", cooldown_sec=300)
                    
                    # 매도 타입 확인
                    sell_type = structured_context.get("type", "Unknown") if structured_context else "Unknown"
                    logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} 매도 판단 상세: 타입={sell_type}, 이유={reason}")
                    
                    # [NEW] 즉시 매도 실행(옵션)
                    if rm._can_direct_sell_now(ticker):
                        qty = int(h.get("hldg_qty", 0))
                        logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} [DIRECT_SELL] 즉시 매도 시도: {name} {qty:,}주")
                        ok = rm._direct_execute_sell(ticker, h.get("prdt_name", ""), qty, {
                            "type": sell_type,
                            "reason": reason,
                            "time_window": rm.rules.time_windows_for_sells or 'ALL',
                            **(structured_context.get("context", {}) if structured_context else {})
                        })
                        if ok:
                            logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} [DIRECT_SELL] ✅ 즉시 매도 주문 제출 성공")
                        else:
                            logger.warning(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} [DIRECT_SELL] ❌ 주문 제출 실패 (다음 사이클에서 trader.py 처리)")
                    else:
                        # direct_sell이 비활성화된 경우에도 로그 출력
                        logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} 매도 판단되었으나 direct_sell 비활성화 또는 조건 불충족. trader.py에서 처리 필요.")
                elif decision == "PARTIAL_SELL":
                    log_msg = f" 부분 익절 판단: {reason}"
                    logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {name}({ticker}) ℹ️ {log_msg}")
                    # 하이라이트된 embed 메시지 전송
                    _notify_sell_embed(h, reason, "PARTIAL_SELL", structured_context, 
                                     key="risk_partial_sell", cooldown_sec=300)
                    
                    # 부분 익절 타입 확인
                    sell_type = structured_context.get("type", "PartialProfit") if structured_context else "PartialProfit"
                    logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} 부분 익절 판단 상세: 타입={sell_type}, 이유={reason}")
                    
                    # 부분 익절은 trader.py에서 처리 (수량 계산 등 복잡한 로직)
                    logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {ticker} 부분 익절 판단되었으나 trader.py에서 처리 필요.")
                else:
                    logger.info(f"[리스크체크] [{idx}/{len(valid_holdings)}] {name}({ticker}) ✅ 유지 판단: {reason}")
            
            logger.info(f"[리스크체크] 보유 종목 {len(valid_holdings)}개 모니터링 완료")

# ── 메인 (레거시 지원) ─────────────────────────────────────────────
if __name__ == "__main__":
    # integrated_manager.py를 사용하도록 안내
    logger.warning("risk_manager.py는 더 이상 독립 실행되지 않습니다.")
    logger.info("통합 매니저를 사용하세요: python run_integrated_manager.py")
    logger.info("또는: docker-compose up integrated_manager")
