# src/integrated_manager.py
"""
통합 매니저: Risk Manager + Scheduler + 일일 잔액 비교
- 장중 리스크 모니터링
- 스케줄된 파이프라인 실행
- 장시작/종료시 잔액 캡처 및 비교
- 디스코드 알림 통합 관리
"""

import os
import json
import logging
import subprocess
import time as pytime
import schedule
import time
import threading
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from typing import Dict, Tuple, Optional, List
from pathlib import Path

# 공통 유틸리티
from utils import (
    KST,
    OUTPUT_DIR,
    extract_cash_from_summary,
    setup_logging,
    in_time_windows,
    get_account_snapshot_cached,
    is_market_open_day,
    previous_trading_day,
    is_us_market,
    is_regular_session,
    next_session_open_kst,
    normalize_ticker_6,
    fmt_money,
    fmt_money_signed,
    resolve_pipeline_context,
    pipeline_artifact_path,
    format_pipeline_artifact,
)

# 디스코드 노티파이어
from notifier import (
    DiscordLogHandler,
    WEBHOOK_URL,
    is_valid_webhook,
    send_discord_message,
)

# Risk Manager 기능
from risk_manager import RiskManager, SellRules

# 설정
from settings import settings

# ───────────────── 로깅 초기화 ─────────────────
print("=== integrated_manager.py 모듈 로드됨 ===")
# LOG_LEVEL 환경변수로 로깅 레벨 제어 (기본 INFO)
_lvl_name = os.getenv("LOG_LEVEL", "INFO").upper().strip()
_lvl = getattr(logging, _lvl_name, logging.INFO)
setup_logging(level=_lvl)
logger = logging.getLogger("IntegratedManager")
logger.setLevel(_lvl)
# 루트 로거도 동일 레벨로 맞춤
root = logging.getLogger()
root.setLevel(_lvl)
# 모든 핸들러 레벨도 업데이트
for handler in root.handlers:
    handler.setLevel(_lvl)
logger.info(f"IntegratedManager 로깅 레벨 설정: {_lvl_name} ({_lvl})")

# ───────────────── 백그라운드 RiskManager ─────────────────
class BackgroundRiskManager:
    """장중 백그라운드 리스크 모니터링"""
    
    def __init__(self, settings_obj):
        self.settings = settings_obj
        self.risk_manager = None
        self.is_running = False
        self.thread = None
        self.stop_event = threading.Event()
        
    def start(self):
        """백그라운드 RiskManager 시작"""
        if self.thread and self.thread.is_alive():
            logger.warning("백그라운드 RiskManager가 이미 실행 중입니다.")
            return

        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run_background, daemon=True)
        self.thread.start()
        self.is_running = True
        logger.info("백그라운드 RiskManager 시작됨")

    def is_thread_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()
        
    def stop(self):
        """백그라운드 RiskManager 중지"""
        if not self.is_running:
            return
            
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        self.is_running = False
        logger.info("백그라운드 RiskManager 중지됨")
        
    def _run_background(self):
        """백그라운드 실행 루프"""
        try:
            # RiskManager 초기화
            self.risk_manager = RiskManager(self.settings)
            
            while not self.stop_event.is_set():
                try:
                    now = datetime.now(KST)
                    if self._is_trading_hours(now):
                        self._run_risk_check()
                        if self.stop_event.wait(300):  # 장중 5분 주기
                            break
                    else:
                        self._wait_until_session_or_stop(now)
                            
                except Exception as e:
                    logger.error(f"백그라운드 RiskManager 실행 중 오류: {e}")
                    if self.stop_event.wait(60):  # 1분 대기 후 재시도
                        break
                        
        except Exception as e:
            logger.error(f"백그라운드 RiskManager 초기화 실패: {e}")
        finally:
            self.is_running = False
            
    def _is_trading_hours(self, now: datetime) -> bool:
        """장중 세션 (US: risk_poll_windows + NYSE 거래일 / KR: 09:00-15:30)."""
        cfg = getattr(self.settings, "_config", None) or {}
        return is_regular_session(now, MARKET, config=cfg)

    def _wait_until_session_or_stop(self, now: datetime) -> None:
        """장외: 다음 risk_poll 세션(예: 23:15 KST)까지 대기. 60초 단위로 stop_event 확인."""
        cfg = getattr(self.settings, "_config", None) or {}
        session_open = next_session_open_kst(now, MARKET, config=cfg)
        remain = (session_open - now).total_seconds()
        if remain > 60:
            logger.info(
                "장외 대기 — 다음 리스크 세션 %s KST (약 %.0f분 후)",
                session_open.strftime("%H:%M"),
                remain / 60,
            )
        deadline = session_open
        while not self.stop_event.is_set():
            now = datetime.now(KST)
            if self._is_trading_hours(now):
                logger.info("리스크 세션 시작 — 모니터링 재개")
                return
            remain = (deadline - now).total_seconds()
            if remain <= 0:
                return
            if self.stop_event.wait(min(remain, 60)):
                return
        
    def _run_risk_check(self):
        """리스크 체크 실행 - RiskManager의 완전한 판단 로직 사용"""
        try:
            if self.risk_manager:
                # RiskManager의 _run_cycle을 직접 호출하여 완전한 판단 로직 사용
                from risk_manager import _run_cycle
                logger.info("백그라운드 리스크 체크 시작 (RiskManager._run_cycle 사용)")
                _run_cycle(self.risk_manager, notify_summary=False)
                logger.info("백그라운드 리스크 체크 완료")
                            
        except Exception as e:
            logger.error(f"백그라운드 리스크 체크 실행 실패: {e}", exc_info=True)

# 루트 로거에 디스코드 에러 핸들러 장착
_root = logging.getLogger()
if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
    if not any(isinstance(h, DiscordLogHandler) for h in _root.handlers):
        _root.addHandler(DiscordLogHandler(WEBHOOK_URL))
        logger.info("DiscordLogHandler attached to root logger.")
else:
    logger.warning("유효한 DISCORD_WEBHOOK_URL이 없어 에러 로그의 디스코드 전송을 비활성화합니다.")

# ───────────────── 설정 ─────────────────
MARKET = os.getenv("MARKET", "SP500")
SLOTS = os.getenv("SLOTS", "3")
MAX_ATTEMPTS = int(os.getenv("SCHED_MAX_ATTEMPTS", "3"))
INITIAL_BACKOFF_MINUTES = int(os.getenv("SCHED_INITIAL_BACKOFF_MINUTES", "2"))
SCRIPT_TIMEOUT_SEC = int(os.getenv("SCRIPT_TIMEOUT_SEC", "600"))
SCREENER_TIMEOUT_SEC = int(os.getenv("SCREENER_TIMEOUT_SEC", "1200"))
SLOW_STEP_SEC = int(os.getenv("SLOW_STEP_SEC", "90"))

# 파이프라인 스크립트(매 거래일 실행)
# 주의: reviewer.py, cleanup_output.py 는 일일 파이프라인에서 제외하고
#       월 1회 유지보수 작업(run_monthly_maintenance)으로 별도 실행한다.
PIPELINE_SCRIPTS = [
    "health_check.py",
    "news_collector.py", 
    "gpt_analyzer.py",
    "trader.py",
]

# 각 단계별 의존성 정의
STEP_DEPENDENCIES = {
    "health_check.py": [],  # 의존성 없음
    "news_collector.py": ["health_check.py"],
    "gpt_analyzer.py": ["news_collector.py"],
    "trader.py": ["gpt_analyzer.py"],
}

# 월 1회 유지보수 스크립트(성과 리뷰 → 설정 튜닝, 산출물 정리)
MONTHLY_SCRIPTS = [
    "reviewer.py",
    "cleanup_output.py",
]
# 매월 실행일(일자)과 시각(KST). 환경변수/ config 로 오버라이드 가능.
MONTHLY_MAINTENANCE_DAY = int(os.getenv("MONTHLY_MAINTENANCE_DAY", "1"))
MONTHLY_MAINTENANCE_TIME = os.getenv("MONTHLY_MAINTENANCE_TIME", "16:00")

# 잔액 저장 경로
BALANCE_STORAGE_PATH = OUTPUT_DIR / "daily_balances"
BALANCE_STORAGE_PATH.mkdir(exist_ok=True)

# 파이프라인 상태 저장 경로
PIPELINE_STATE_PATH = OUTPUT_DIR / "pipeline_state.json"

# 월간 유지보수 마지막 실행 기록(중복 실행 방지)
MONTHLY_STATE_PATH = OUTPUT_DIR / "monthly_maintenance_state.json"

# ───────────────── 파이프라인 상태 관리 ─────────────────
class PipelineStateManager:
    """파이프라인 상태 저장 및 관리"""
    
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.state_file = PIPELINE_STATE_PATH
        self.completed_steps = set()
        self.failed_step = None
        self.start_time = None
        
    def save_state(self, step_name: str, success: bool, duration: float = 0.0):
        """파이프라인 상태 저장"""
        if success:
            self.completed_steps.add(step_name)
            self.failed_step = None
        else:
            self.failed_step = step_name
        
        state = {
            "run_id": self.run_id,
            "completed_steps": list(self.completed_steps),
            "failed_step": self.failed_step,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "last_update": datetime.now(KST).isoformat(),
            "step_durations": getattr(self, 'step_durations', {})
        }
        
        # 단계별 실행 시간 저장
        if not hasattr(self, 'step_durations'):
            self.step_durations = {}
        self.step_durations[step_name] = duration
        
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            logger.debug(f"파이프라인 상태 저장: {step_name} ({'성공' if success else '실패'})")
        except Exception as e:
            logger.error(f"파이프라인 상태 저장 실패: {e}")
    
    def load_state(self) -> Optional[Dict]:
        """저장된 파이프라인 상태 로드 (run_id 일치 시에만)"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    # run_id가 일치하는 경우에만 상태를 로드
                    saved_run_id = state.get("run_id")
                    if saved_run_id == self.run_id:
                        self.completed_steps = set(state.get("completed_steps", []))
                        self.failed_step = state.get("failed_step")
                        if state.get("start_time"):
                            self.start_time = datetime.fromisoformat(state["start_time"])
                        return state
                    else:
                        # run_id가 다르면 이전 실행의 상태이므로 무시하고 초기화
                        logger.info(f"이전 실행 상태 무시 (saved_run_id={saved_run_id}, current_run_id={self.run_id})")
                        self.completed_steps = set()
                        self.failed_step = None
                        self.start_time = None
                        return None
        except Exception as e:
            logger.error(f"파이프라인 상태 로드 실패: {e}")
            # 오류 발생 시 초기화
            self.completed_steps = set()
            self.failed_step = None
            self.start_time = None
        return None
    
    def should_skip_step(self, step_name: str) -> bool:
        """이미 완료된 단계는 건너뛰기"""
        return step_name in self.completed_steps
    
    def get_required_steps(self, failed_step: str) -> List[str]:
        """실패한 단계를 포함하여 재실행해야 할 단계들 반환"""
        required = [failed_step]
        
        # 의존성 체인 추적
        for step, deps in STEP_DEPENDENCIES.items():
            if failed_step in deps:
                required.extend(self.get_required_steps(step))
        
        return list(set(required))  # 중복 제거
    
    def clear_state(self):
        """파이프라인 상태 초기화"""
        try:
            if self.state_file.exists():
                self.state_file.unlink()
            self.completed_steps.clear()
            self.failed_step = None
            self.start_time = None
            logger.info("파이프라인 상태 초기화 완료")
        except Exception as e:
            logger.error(f"파이프라인 상태 초기화 실패: {e}")

# ───────────────── 알림 관리 ─────────────────
_last_sent: Dict[str, float] = {}

def _notify(msg: str, key: str = "integrated_manager", cooldown_sec: int = 300) -> None:
    """디스코드 알림(쿨다운 적용)"""
    try:
        now = pytime.time()
        if key not in _last_sent or now - _last_sent[key] >= cooldown_sec:
            _last_sent[key] = now
            if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
                send_discord_message(content=msg)
    except Exception:
        pass

def _ensure_summary_file_for_risk_manager():
    """RiskManager 실행을 위한 요약 파일 자동 복구"""
    try:
        # 요약 파일 존재 확인
        summary_files = list(OUTPUT_DIR.glob("summary_*.json"))
        if not summary_files:
            logger.warning("요약 파일이 없습니다. account.py를 실행합니다...")
            _run_account_snapshot()
        else:
            # 최신 파일 확인 (5분 이내)
            latest_file = max(summary_files, key=lambda f: f.stat().st_mtime)
            if pytime.time() - latest_file.stat().st_mtime > 300:  # 5분
                logger.warning("요약 파일이 오래되었습니다. account.py를 실행합니다...")
                _run_account_snapshot()
            else:
                logger.debug(f"요약 파일 확인 완료: {latest_file.name}")
    except Exception as e:
        logger.error(f"요약 파일 확인 중 오류: {e}")
        _run_account_snapshot()

def _run_account_snapshot():
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

# ───────────────── 잔액 캡처 및 비교 ─────────────────
def _parse_amount(val) -> int:
    try:
        if val is None or val == "":
            return 0
        return int(round(float(str(val).replace(",", "").strip())))
    except (TypeError, ValueError):
        return 0


def _holdings_detail_from_rows(holdings: List[Dict]) -> List[Dict]:
    """보유 종목 상세 (USD 소수 현재가 허용)."""
    mkt = MARKET
    out: List[Dict] = []
    for h in holdings:
        qty = _parse_amount(h.get("hldg_qty", 0))
        if qty <= 0:
            continue
        price = _parse_amount(h.get("prpr", 0))
        evlu = _parse_amount(h.get("evlu_amt", 0))
        if evlu <= 0 and price > 0:
            evlu = qty * price
        out.append({
            "ticker": normalize_ticker_6(h.get("pdno", ""), mkt),
            "name": h.get("prdt_name", "N/A"),
            "qty": qty,
            "price": price,
            "value": evlu,
        })
    return out


def _holdings_value_from_rows(holdings: List[Dict]) -> int:
    return sum(h["value"] for h in _holdings_detail_from_rows(holdings))


def _portfolio_totals_from_cash_map(
    cash_map: Dict, holdings: List[Dict]
) -> Tuple[int, int, int]:
    """(총평가, 예수금, 보유평가) — US는 예수금+보유평가가 KIS 총평가보다 신뢰될 때 우선."""
    hv = _holdings_value_from_rows(holdings)
    if hv <= 0:
        hv = sum(_parse_amount(h.get("evlu_amt")) for h in holdings)
        if hv <= 0:
            hv = sum(
                _parse_amount(h.get("hldg_qty")) * _parse_amount(h.get("prpr"))
                for h in holdings
            )

    if is_us_market(MARKET):
        cash = _parse_amount(cash_map.get("dnca_tot_amt"))
        if cash <= 0:
            cash = _parse_amount(cash_map.get("frcr_buy_amt"))
        orderable = _parse_amount(cash_map.get("available_cash")) or _parse_amount(
            cash_map.get("ord_psbl_frcr_amt")
        )
        if cash <= 0:
            cash = orderable

        kis_total = _parse_amount(cash_map.get("tot_evlu_amt_usd"))
        if kis_total <= 0:
            kis_total = _parse_amount(cash_map.get("tot_evlu_amt"))

        computed = cash + hv
        cash_only_ref = max(cash, orderable)
        if hv > 0 and (kis_total <= 0 or kis_total <= cash_only_ref + 1):
            total = computed
        elif hv > 0:
            total = max(kis_total, computed)
        else:
            total = kis_total if kis_total > 0 else cash_only_ref
        return total, cash, hv

    total = _parse_amount(cash_map.get("tot_evlu_amt"))
    cash = _parse_amount(cash_map.get("dnca_tot_amt"))
    if total <= 0:
        total = cash + hv
    return total, cash, hv


def _portfolio_totals_from_snapshot(snap: Dict) -> Tuple[int, int, int]:
    """저장된 스냅샷의 총평가 보정 (과거 cash-only total_balance 호환)."""
    cash = _parse_amount(snap.get("cash"))
    hv = _parse_amount(snap.get("holdings_value"))
    if hv <= 0 and snap.get("holdings_detail"):
        hv = sum(_parse_amount(h.get("value")) for h in snap["holdings_detail"])
    total = _parse_amount(snap.get("total_balance"))
    computed = cash + hv
    orderable = cash
    if hv > 0 and (total <= 0 or total <= orderable + 1):
        total = computed
    elif hv > 0 and computed > total:
        total = computed
    elif total <= 0:
        total = computed if computed > 0 else cash
    return total, cash, hv


def _iter_open_snapshot_dates(close_date: str) -> List[str]:
    """
    종료일(close)에 맞는 장시작(open) 스냅샷 파일 날짜 후보.
    US: balance_open_time(KST, 기본 23:55) 캡처 → balance_open_{전일}.json + balance_close_{당일}.json
    KR: 동일 일자 open/close
    """
    d = datetime.strptime(close_date, "%Y%m%d").date()
    seen: set = set()
    out: List[str] = []

    def _add(ds: str) -> None:
        if ds not in seen:
            seen.add(ds)
            out.append(ds)

    if is_us_market(MARKET):
        _add((d - timedelta(days=1)).strftime("%Y%m%d"))
        probe = d - timedelta(days=1)
        for _ in range(10):
            if is_market_open_day(probe, MARKET):
                _add(probe.strftime("%Y%m%d"))
                break
            probe -= timedelta(days=1)
        probe2 = previous_trading_day(d - timedelta(days=1), MARKET)
        _add(probe2.strftime("%Y%m%d"))
    _add(close_date)
    probe = d
    for _ in range(10):
        probe -= timedelta(days=1)
        _add(probe.strftime("%Y%m%d"))
    return out


def load_daily_balance_pair(
    session_close_date: str,
) -> Tuple[Optional[Dict], Optional[Dict], Optional[str]]:
    """(open_snapshot, close_snapshot, open_file_date) — close는 session_close_date 고정."""
    close_balance = load_balance_snapshot(session_close_date, "close")
    if not close_balance:
        return None, None, None
    for open_date in _iter_open_snapshot_dates(session_close_date):
        open_balance = load_balance_snapshot(open_date, "open")
        if open_balance:
            if open_date != session_close_date:
                logger.info(
                    "일일 요약 open 스냅샷: close=%s ← open_%s (US 세션)",
                    session_close_date,
                    open_date,
                )
            return open_balance, close_balance, open_date
    return None, close_balance, None


def capture_balance_snapshot(snapshot_type: str) -> Optional[Dict]:
    """잔액 스냅샷 캡처 (open/close)"""
    try:
        # 계좌 스냅샷 생성
        import subprocess
        result = subprocess.run(
            ["python", "/app/src/account.py"],
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
        )
        
        # 최신 스냅샷 로드
        cash_map, holdings, summary_path, balance_path = get_account_snapshot_cached(
            summary_pattern="summary_*.json",
            balance_pattern="balance_*.json", 
            ttl_sec=5
        )
        
        holdings_detail = _holdings_detail_from_rows(holdings)
        total_balance, cash, holdings_value = _portfolio_totals_from_cash_map(
            cash_map, holdings
        )
        if holdings_value <= 0 and total_balance > cash:
            holdings_value = total_balance - cash

        now = datetime.now(KST)
        snap_date = now.strftime("%Y%m%d")
        # US 장시작(23:25): 다음 KST 아침 종료일에 세션 귀속 (요약 시 open/close 짝 맞춤)
        session_close_date = snap_date
        if is_us_market(MARKET) and snapshot_type == "open":
            nxt = now.date() + timedelta(days=1)
            for _ in range(5):
                if is_market_open_day(nxt, MARKET):
                    session_close_date = nxt.strftime("%Y%m%d")
                    break
                nxt += timedelta(days=1)

        # 스냅샷 데이터 구성
        snapshot = {
            "date": snap_date,
            "session_close_date": session_close_date,
            "timestamp": now.isoformat(),
            "type": snapshot_type,
            "total_balance": total_balance,
            "cash": cash,
            "holdings_value": holdings_value,
            "holdings_count": len(holdings_detail),
            "holdings_detail": holdings_detail,
            "summary_file": str(summary_path) if summary_path else None,
            "balance_file": str(balance_path) if balance_path else None
        }
        
        # 파일 저장 (US open: session_close_date 키로도 저장 → 06:15 요약과 짝)
        save_dates = {snap_date}
        if is_us_market(MARKET) and snapshot_type == "open" and session_close_date != snap_date:
            save_dates.add(session_close_date)
        for save_date in save_dates:
            filename = f"balance_{snapshot_type}_{save_date}.json"
            filepath = BALANCE_STORAGE_PATH / filename
            snap_copy = {**snapshot, "file_date": save_date}
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(snap_copy, f, ensure_ascii=False, indent=2)
            logger.info(
                "잔액 스냅샷 저장: %s (capture=%s session_close=%s)",
                filepath,
                snap_date,
                session_close_date,
            )
        return snapshot
        
    except Exception as e:
        logger.error(f"잔액 스냅샷 캡처 실패: {type(e).__name__}: {str(e)}")
        logger.debug("잔액 스냅샷 캡처 상세 오류:", exc_info=True)
        return None

def load_balance_snapshot(date: str, snapshot_type: str) -> Optional[Dict]:
    """저장된 잔액 스냅샷 로드"""
    try:
        filename = f"balance_{snapshot_type}_{date}.json"
        filepath = BALANCE_STORAGE_PATH / filename
        if filepath.exists():
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"잔액 스냅샷 로드 실패: {type(e).__name__}: {str(e)}")
        logger.debug("잔액 스냅샷 로드 상세 오류:", exc_info=True)
    return None

def compare_balances(open_balance: Dict, close_balance: Dict) -> Dict:
    """장시작/종료 총평가(예수금+보유평가) 비교 분석"""
    try:
        open_total, open_cash, open_hv = _portfolio_totals_from_snapshot(open_balance)
        close_total, close_cash, close_hv = _portfolio_totals_from_snapshot(close_balance)

        total_change = close_total - open_total
        cash_change = close_cash - open_cash
        holdings_change = close_hv - open_hv

        daily_return_pct = (total_change / open_total) * 100 if open_total > 0 else 0
        
        # 보유종목 변화 분석
        open_tickers = {h["ticker"] for h in open_balance["holdings_detail"]}
        close_tickers = {h["ticker"] for h in close_balance["holdings_detail"]}
        
        # 매도된 종목 (장시작에만 있던 종목)
        sold_tickers = open_tickers - close_tickers
        
        # 새로 매수된 종목 (장종료에만 있는 종목)
        bought_tickers = close_tickers - open_tickers
        
        # 계속 보유된 종목
        held_tickers = open_tickers & close_tickers
        
        # 실현 손익 추정 (매도된 종목의 가치 변화)
        realized_pnl = 0
        for ticker in sold_tickers:
            open_holding = next((h for h in open_balance["holdings_detail"] if h["ticker"] == ticker), None)
            if open_holding:
                realized_pnl += open_holding["value"]
        
        # 미실현 손익 (계속 보유된 종목의 가치 변화)
        unrealized_pnl = 0
        for ticker in held_tickers:
            open_holding = next((h for h in open_balance["holdings_detail"] if h["ticker"] == ticker), None)
            close_holding = next((h for h in close_balance["holdings_detail"] if h["ticker"] == ticker), None)
            if open_holding and close_holding:
                unrealized_pnl += close_holding["value"] - open_holding["value"]
        
        # 수수료 추정 (총 변화량에서 실현/미실현 손익 차감)
        estimated_fees = total_change - realized_pnl - unrealized_pnl
        
        return {
            "date": close_balance["date"],
            "total_change": total_change,
            "cash_change": cash_change,
            "holdings_change": holdings_change,
            "daily_return_pct": round(daily_return_pct, 2),
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "estimated_fees": estimated_fees,
            "net_pnl": total_change,
            "sold_tickers": list(sold_tickers),
            "bought_tickers": list(bought_tickers),
            "held_tickers": list(held_tickers),
            "open_balance": open_total,
            "close_balance": close_total,
            "open_cash": open_cash,
            "close_cash": close_cash,
            "open_holdings_value": open_hv,
            "close_holdings_value": close_hv,
        }
        
    except Exception as e:
        logger.error(f"잔액 비교 분석 실패: {type(e).__name__}: {str(e)}")
        logger.debug("잔액 비교 분석 상세 오류:", exc_info=True)
        return {}

def send_daily_trading_summary():
    """장종료시 당일 매매 내역을 디스코드로 전송"""
    try:
        today = datetime.now(KST).strftime("%Y%m%d")
        open_balance, close_balance, open_date = load_daily_balance_pair(today)
        late_close_capture = False

        if not close_balance:
            logger.warning(
                "%s 장종료 스냅샷 없음 → account 즉시 캡처 시도 (06:00 스케줄 누락·컨테이너 중단 가능)",
                today,
            )
            if capture_balance_snapshot("close"):
                late_close_capture = True
                open_balance, close_balance, open_date = load_daily_balance_pair(today)

        if not close_balance:
            _notify(
                f"⚠️ {today} 일일 요약: 장종료 스냅샷 없음 "
                f"({BALANCE_STORAGE_PATH}/balance_close_{today}.json). "
                f"`docker compose exec integrated_manager python /app/run_integrated_manager.py --capture-close` 실행 후 재시도.",
                key="daily_summary_error",
            )
            return
        if not open_balance:
            tried = ", ".join(_iter_open_snapshot_dates(today)[:5])
            _notify(
                f"⚠️ {today} 일일 요약: 장시작 스냅샷 없음 "
                f"(balance_open_*.json, balance_open_time 캡처 확인 / 후보: {tried})",
                key="daily_summary_error",
            )
            return
        
        # 잔액 비교 분석
        comparison = compare_balances(open_balance, close_balance)
        
        # 디스코드 임베드 생성
        total_change = comparison["total_change"]
        daily_return = comparison["daily_return_pct"]
        realized_pnl = comparison["realized_pnl"]
        unrealized_pnl = comparison["unrealized_pnl"]
        estimated_fees = comparison["estimated_fees"]
        
        # 변화량 이모지
        change_emoji = "📈" if total_change > 0 else "📉" if total_change < 0 else "➡️"
        mkt = os.getenv("MARKET", "SP500")

        open_total = comparison["open_balance"]
        close_total = comparison["close_balance"]
        open_hv = comparison.get("open_holdings_value", 0)
        close_hv = comparison.get("close_holdings_value", 0)
        open_cash = comparison.get("open_cash", open_balance.get("cash", 0))
        close_cash = comparison.get("close_cash", close_balance.get("cash", 0))

        fields = [
            {
                "name": f"{change_emoji} 총평가 변화 (예수금+보유평가)",
                "value": (
                    f"**{fmt_money(open_total, mkt)}** → "
                    f"**{fmt_money(close_total, mkt)}** "
                    f"({fmt_money_signed(total_change, mkt)})"
                ),
                "inline": False,
            },
            {
                "name": "💵 예수금",
                "value": (
                    f"{fmt_money(open_cash, mkt)} → {fmt_money(close_cash, mkt)} "
                    f"({fmt_money_signed(comparison['cash_change'], mkt)})"
                ),
                "inline": True,
            },
            {
                "name": "📈 보유평가",
                "value": (
                    f"{fmt_money(open_hv, mkt)} → {fmt_money(close_hv, mkt)} "
                    f"({fmt_money_signed(comparison['holdings_change'], mkt)})"
                ),
                "inline": True,
            },
        ]

        if (
            abs(daily_return) > 0.01
            or abs(total_change) > 0
            or open_hv > 0
            or close_hv > 0
        ):
            fields.extend([
                {
                    "name": "📊 일일 수익률",
                    "value": f"**{daily_return:+.2f}%**",
                    "inline": True
                },
                {
                    "name": "💰 실현 손익",
                    "value": fmt_money_signed(realized_pnl, mkt),
                    "inline": True
                },
                {
                    "name": "📈 미실현 손익",
                    "value": fmt_money_signed(unrealized_pnl, mkt),
                    "inline": True
                }
            ])
            
            if abs(estimated_fees) > 0:
                fields.append({
                    "name": "💸 추정 수수료",
                    "value": fmt_money_signed(estimated_fees, mkt),
                    "inline": True
                })
        
        # 매매 내역 추가 (변화가 있을 때만)
        if comparison["sold_tickers"] or comparison["bought_tickers"]:
            trading_summary = []
            if comparison["sold_tickers"]:
                trading_summary.append(f"🔴 매도 {len(comparison['sold_tickers'])}종목")
            if comparison["bought_tickers"]:
                trading_summary.append(f"🟢 매수 {len(comparison['bought_tickers'])}종목")
            
            fields.append({
                "name": "📋 매매 내역",
                "value": " | ".join(trading_summary),
                "inline": False
            })
        
        # 임베드 전송
        session_note = (
            f"open_{open_date} → close_{today}"
            if open_date and open_date != today
            else today
        )
        desc = f"⏰ {open_balance['timestamp'][:19]} → {close_balance['timestamp'][:19]}"
        if late_close_capture:
            desc += " | ⚠️ close는 요약 시점 즉시 캡처(06:00 스냅샷 누락)"
        embed = {
            "type": "rich",
            "title": f"📊 {today} 당일 매매 성과 ({session_note})",
            "description": desc,
            "fields": fields,
            "color": 0x00ff00 if total_change > 0 else 0xff0000 if total_change < 0 else 0x808080,
            "footer": {
                "text": (
                    f"보유종목: open {open_balance['holdings_count']}개 → "
                    f"close {close_balance['holdings_count']}개"
                )
            },
        }
        
        if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
            send_discord_message(embeds=[embed])
            logger.info(f"일일 매매 요약 전송 완료: {today}")
        
    except Exception as e:
        logger.error(f"일일 매매 요약 전송 실패: {type(e).__name__}: {str(e)}")
        logger.debug("일일 매매 요약 전송 상세 오류:", exc_info=True)
        _notify(f"❌ 일일 매매 요약 전송 실패: {type(e).__name__}: {str(e)}", key="daily_summary_error")

# ───────────────── 파이프라인 실행 ─────────────────
def _tail(text: str, n: int = 12) -> str:
    """로그 텍스트의 꼬리 n줄만 반환"""
    if not text:
        return ""
    lines = text.strip().splitlines()
    return "\n".join(lines[-n:])

def _apply_pipeline_env(run_id: str) -> Dict[str, str]:
    """파이프라인 AM/PM·거래일을 환경변수로 고정 (자정 넘김 시 산출물 짝 유지)."""
    cfg = getattr(settings, "_config", None) or {}
    ctx = resolve_pipeline_context(market=MARKET, config=cfg)
    os.environ["PIPELINE_SESSION"] = ctx["session"]
    os.environ["PIPELINE_TRADE_DATE"] = ctx["trade_date"]
    logger.info(
        "[%s] pipeline context: session=%s trade_date=%s kst_date=%s",
        run_id,
        ctx["session"],
        ctx["trade_date"],
        ctx["kst_date"],
    )
    return ctx


def _resolve_screener_input_for_news(ctx: Dict[str, str]) -> Optional[Path]:
    """news_collector가 읽을 스크리너 결과 (세션·거래일 우선, 레거시 폴백)."""
    for sess in (ctx.get("session"), None):
        p = pipeline_artifact_path(
            "screener_candidates",
            ctx["trade_date"],
            MARKET,
            session=sess,
        )
        if p.exists():
            return p
    return None


def run_script(script_name: str, run_id: str, pipeline_ctx: Optional[Dict[str, str]] = None) -> Tuple[bool, bool, float]:
    """주어진 파이썬 스크립트를 실행 (자동 복구 포함)"""
    timeout_sec = SCREENER_TIMEOUT_SEC if script_name == "screener.py" else SCRIPT_TIMEOUT_SEC
    ctx = pipeline_ctx or {
        "session": os.environ.get("PIPELINE_SESSION", ""),
        "trade_date": os.environ.get("PIPELINE_TRADE_DATE", ""),
    }
    trade_date = ctx.get("trade_date") or ""
    session = ctx.get("session") or ""

    args = []
    if script_name == "screener.py":
        args = ["--market", MARKET, "--date", trade_date]
        if session:
            args.extend(["--session", session])
    elif script_name == "gpt_analyzer.py":
        args = ["--market", MARKET, "--slots", SLOTS, "--date", trade_date]
        if session:
            args.extend(["--session", session])
    elif script_name == "news_collector.py":
        screener_path = _resolve_screener_input_for_news(ctx)
        if screener_path:
            args = ["--file", str(screener_path)]
        else:
            logger.warning(
                "[%s] 세션 스크리너 없음 (%s) → news_collector 자동 탐색",
                run_id,
                format_pipeline_artifact("screener_candidates", trade_date, MARKET, session),
            )

    command = ["python", f"/app/src/{script_name}"] + args
    cmd_str = " ".join(command)

    child_env = dict(os.environ)
    child_env["RUN_ID"] = os.environ.get("RUN_ID", run_id)
    child_env["RUN_STARTED_AT"] = os.environ.get("RUN_STARTED_AT", str(time.time()))
    child_env.setdefault("MARKET", MARKET)
    child_env.setdefault("SLOTS", SLOTS)

    logger.info(f"[{run_id}] ▶ STEP START: {script_name} | cmd='{cmd_str}' (timeout={timeout_sec}s)")
    t0 = time.perf_counter()
    warned = False
    
    # RiskManager 실행 전 요약 파일 자동 복구
    if script_name == "trader.py":  # trader.py에서 RiskManager를 사용
        _ensure_summary_file_for_risk_manager()
    
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
            timeout=timeout_sec,
            env=child_env,
        )
        dur = time.perf_counter() - t0
        stdout_tail = _tail(result.stdout, 12)
        logger.info(f"[{run_id}] ✅ STEP OK: {script_name} | {dur:.1f}s")
        logger.debug(f"[{run_id}] --- {script_name} tail ---\n{stdout_tail}")

        if dur > SLOW_STEP_SEC:
            warned = True
            logger.warning(f"[{run_id}] ⚠️ SLOW STEP: {script_name} ({dur:.1f}s > {SLOW_STEP_SEC}s)")

        return True, warned, dur

    except subprocess.TimeoutExpired:
        dur = time.perf_counter() - t0
        logger.error(f"[{run_id}] ❌ STEP TIMEOUT: {script_name} ({timeout_sec}s) | {dur:.1f}s 경과")
        return False, warned, dur

    except subprocess.CalledProcessError as e:
        dur = time.perf_counter() - t0
        stderr_tail = _tail(e.stderr, 80)
        logger.error(f"[{run_id}] ❌ STEP FAIL: {script_name} (exit={e.returncode}) | {dur:.1f}s")
        logger.error(f"[{run_id}] --- STDERR tail ---\n{stderr_tail}")
        return False, warned, dur

    except Exception as e:
        dur = time.perf_counter() - t0
        logger.critical(f"[{run_id}] ⛔ STEP EXCEPTION: {script_name} | {dur:.1f}s | {e}", exc_info=True)
        return False, warned, dur

def run_screener_job():
    """스크리너 단독 실행"""
    try:
        if not is_market_open_day(market=MARKET):
            label = "US(NYSE)" if is_us_market(MARKET) else "국내"
            msg = f"오늘은 {label} 휴장일이므로 screener 실행을 건너뜁니다."
            logger.info(msg)
            _notify(msg=f"ℹ️ {msg}", key="screener_holiday", cooldown_sec=600)
            return

        run_id = datetime.now(KST).strftime("%Y%m%d-%H%M%S")
        os.environ["RUN_ID"] = run_id
        os.environ["RUN_STARTED_AT"] = str(time.time())
        pipeline_ctx = _apply_pipeline_env(run_id)

        start = (
            f"🔍 스크리너 실행 시작 (MARKET={MARKET}, "
            f"session={pipeline_ctx['session']}, trade_date={pipeline_ctx['trade_date']})"
        )
        logger.info(f"[{run_id}] KST {datetime.now(KST):%Y-%m-%d %H:%M:%S} - {start}")
        _notify(start, key=f"{run_id}:screener_start", cooldown_sec=30)

        ok, warned, dur = run_script("screener.py", run_id, pipeline_ctx)
        status = "✅ 완료" if ok else "❌ 실패"
        warn_tag = " (⚠️ slow)" if (ok and warned) else ""
        _notify(f"🔍 스크리너 {status}{warn_tag} - {dur:.1f}초", key=f"{run_id}:screener_end", cooldown_sec=30)

    except Exception as e:
        logger.error(f"스크리너 실행 중 오류: {e}")

def _resolve_batch_reconcile_times(config: Optional[dict] = None) -> Tuple[str, str, bool, bool]:
    """config·MARKET 기준 일괄 체결·리컨실 시각(KST) 및 활성 여부."""
    cfg = config if config is not None else (getattr(settings, "_config", None) or {})
    batch_cfg = cfg.get("batch_execution_check") or {}
    recon_cfg = cfg.get("order_reconcile") or {}
    default_batch = "06:05" if is_us_market(MARKET) else "15:20"
    batch_time = str(batch_cfg.get("check_time") or default_batch)
    batch_enabled = bool(batch_cfg.get("enabled", True))
    recon_enabled = bool(recon_cfg.get("enabled", True))
    if recon_cfg.get("reconcile_time"):
        reconcile_time = str(recon_cfg["reconcile_time"])
    else:
        offset = int(recon_cfg.get("minutes_after_batch", 2))
        reconcile_time = _add_minutes_hhmm(batch_time, offset)
    return batch_time, reconcile_time, batch_enabled, recon_enabled


def run_batch_execution_check():
    """설정 시각(KST) 일괄 체결 확인 — trader.py --batch-check-only"""
    batch_time, _, _, _ = _resolve_batch_reconcile_times()
    try:
        if not is_market_open_day(market=MARKET):
            logger.info("휴장일 → 일괄 체결 확인 스킵 (%s)", batch_time)
            return

        logger.info("%s KST 일괄 체결 확인 시작", batch_time)

        result = subprocess.run([
            sys.executable, "src/trader.py", "--batch-check-only"
        ], capture_output=True, text=True, cwd=os.getcwd())

        if result.returncode == 0:
            logger.info("%s KST 일괄 체결 확인 완료", batch_time)
            _notify(f"✅ {batch_time} 일괄 체결 확인 완료", key="batch_execution_check", cooldown_sec=60)
        else:
            logger.error("일괄 체결 확인 실패: %s", result.stderr)
            _notify(
                f"❌ {batch_time} 일괄 체결 확인 실패: {result.stderr[:200]}",
                key="batch_execution_check_fail",
                cooldown_sec=60,
            )

    except Exception as e:
        logger.error("일괄 체결 확인 중 오류: %s", e)
        _notify(
            f"❌ {batch_time} 일괄 체결 확인 오류: {str(e)[:200]}",
            key="batch_execution_check_error",
            cooldown_sec=60,
        )

def _add_minutes_hhmm(hhmm: str, minutes: int) -> str:
    """'HH:MM' 문자열에 분을 더해 'HH:MM'로 반환 (24h wrap)."""
    try:
        h, m = hhmm.split(":")
        total = int(h) * 60 + int(m) + int(minutes)
        total %= (24 * 60)
        nh = total // 60
        nm = total % 60
        return f"{nh:02d}:{nm:02d}"
    except Exception:
        return hhmm

def run_order_reconcile_job(*, skip_holiday_check: bool = False):
    """DB pending/partial 주문 리컨실 실행"""
    _, reconcile_time, _, _ = _resolve_batch_reconcile_times()
    try:
        if not skip_holiday_check and not is_market_open_day(market=MARKET):
            logger.info("휴장일 → 주문 리컨실 스킵 (%s)", reconcile_time)
            return

        logger.info("%s KST 주문 리컨실(order_reconciler.py) 실행 시작", reconcile_time)
        # 별도 스크립트 실행 (DB 상태 정리)
        result = subprocess.run(
            [sys.executable, "src/order_reconciler.py", "--since-hours", "36", "--limit", "800"],
            capture_output=True,
            text=True,
            cwd=os.getcwd(),
        )
        if result.returncode == 0:
            logger.info("주문 리컨실 실행 완료")
        else:
            logger.error(f"주문 리컨실 실패: {result.stderr[:300]}")
            _notify(f"❌ 주문 리컨실 실패: {result.stderr[:180]}", key="order_reconcile_fail", cooldown_sec=120)
    except Exception as e:
        logger.error(f"주문 리컨실 실행 중 오류: {e}")
        _notify(f"❌ 주문 리컨실 오류: {str(e)[:180]}", key="order_reconcile_error", cooldown_sec=120)

def _monthly_already_ran(year_month: str) -> bool:
    """이번 달 유지보수가 이미 실행됐는지 확인(컨테이너 재시작 등 중복 방지)."""
    try:
        if MONTHLY_STATE_PATH.exists():
            with open(MONTHLY_STATE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("last_run_month") == year_month
    except Exception as e:
        logger.warning(f"월간 유지보수 상태 로드 실패: {e}")
    return False


def _mark_monthly_ran(year_month: str, results: Dict[str, str]) -> None:
    """이번 달 유지보수 실행 기록 저장."""
    try:
        with open(MONTHLY_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "last_run_month": year_month,
                    "ran_at": datetime.now(KST).isoformat(),
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        logger.error(f"월간 유지보수 상태 저장 실패: {e}")


def run_monthly_maintenance(force: bool = False):
    """월 1회 유지보수: 성과 리뷰(reviewer) → 산출물 정리(cleanup)."""
    try:
        run_id = datetime.now(KST).strftime("%Y%m%d-%H%M%S")
        os.environ["RUN_ID"] = run_id
        os.environ["RUN_STARTED_AT"] = str(time.time())

        start_msg = "🗓️ 월간 유지보수 시작 (성과 리뷰 → 산출물 정리)"
        logger.info(f"[{run_id}] {start_msg}")
        _notify(start_msg, key=f"{run_id}:monthly_start", cooldown_sec=30)

        results: Dict[str, str] = {}
        for script in MONTHLY_SCRIPTS:
            ok, warned, dur = run_script(script, run_id)
            results[script] = ("OK" if ok else "FAIL") + (f" {dur:.1f}s")
            if not ok:
                logger.error(f"[{run_id}] 월간 유지보수 단계 실패: {script}")

        summary = " | ".join(f"{k}:{v}" for k, v in results.items())
        _notify(f"🗓️ 월간 유지보수 완료 - {summary}", key=f"{run_id}:monthly_end", cooldown_sec=30)
        logger.info(f"[{run_id}] 월간 유지보수 종료: {summary}")
        return results
    except Exception as e:
        logger.error(f"월간 유지보수 실행 중 오류: {e}")
        return {}


def run_monthly_maintenance_if_due():
    """매일 호출되어 '실행일(MONTHLY_MAINTENANCE_DAY)' 인 경우에만 1회 실행."""
    try:
        now = datetime.now(KST)
        if now.day != MONTHLY_MAINTENANCE_DAY:
            return
        year_month = now.strftime("%Y%m")
        if _monthly_already_ran(year_month):
            logger.info(f"월간 유지보수 이미 실행됨({year_month}) → 건너뜀")
            return
        results = run_monthly_maintenance()
        _mark_monthly_ran(year_month, results)
    except Exception as e:
        logger.error(f"월간 유지보수 스케줄 처리 중 오류: {e}")


def run_trading_pipeline():
    """전체 파이프라인 실행 (상태 관리 및 의존성 기반 재시작 포함)"""
    try:
        if not is_market_open_day(market=MARKET):
            label = "US(NYSE)" if is_us_market(MARKET) else "국내"
            msg = f"오늘은 {label} 휴장일이므로 자동매매 파이프라인을 실행하지 않습니다."
            logger.info(msg)
            _notify(msg=f"ℹ️ {msg}", key="holiday", cooldown_sec=600)
            return

        run_id = datetime.now(KST).strftime("%Y%m%d-%H%M%S")
        os.environ["RUN_ID"] = run_id
        os.environ["RUN_STARTED_AT"] = str(time.time())
        pipeline_ctx = _apply_pipeline_env(run_id)

        # 파이프라인 상태 관리자 초기화
        state_manager = PipelineStateManager(run_id)
        state_manager.start_time = datetime.now(KST)
        
        # 이전 상태 로드 (load_state에서 이미 run_id 체크함)
        saved_state = state_manager.load_state()
        if saved_state:
            logger.info(f"이전 실행 상태 복구: 완료된 단계 {saved_state['completed_steps']}")
            if saved_state.get('failed_step'):
                logger.info(f"실패한 단계부터 재시작: {saved_state['failed_step']}")

        start_msg = (
            f"🤖 자동매매 파이프라인 시작 (MARKET={MARKET}, SLOTS={SLOTS}, "
            f"session={pipeline_ctx['session']}, trade_date={pipeline_ctx['trade_date']})"
        )
        logger.info(f"[{run_id}] KST {datetime.now(KST):%Y-%m-%d %H:%M:%S} - {start_msg}")
        _notify(msg=start_msg, key=f"{run_id}:pipeline_start", cooldown_sec=30)

        pipeline_ok = True
        warn_count_total = 0
        attempts_used = 0
        last_error = None
        last_failed_step = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            attempts_used = attempt
            try:
                logger.info(f"[{run_id}] --- 시도 {attempt}/{MAX_ATTEMPTS} ---")
                
                # 실행할 스크립트 목록 결정 (의존성 기반)
                scripts_to_run = []
                if saved_state and saved_state.get('failed_step'):
                    # 실패한 단계부터 재시작
                    failed_step = saved_state['failed_step']
                    required_steps = state_manager.get_required_steps(failed_step)
                    scripts_to_run = [s for s in PIPELINE_SCRIPTS if s in required_steps]
                    logger.info(f"[{run_id}] 의존성 기반 재시작: {scripts_to_run}")
                else:
                    # 전체 파이프라인 실행
                    scripts_to_run = PIPELINE_SCRIPTS
                
                for script in scripts_to_run:
                    # 이미 완료된 단계는 건너뛰기
                    if state_manager.should_skip_step(script):
                        logger.info(f"[{run_id}] 이미 완료된 단계 건너뛰기: {script}")
                        continue
                    
                    # 의존성 체크
                    dependencies = STEP_DEPENDENCIES.get(script, [])
                    missing_deps = [dep for dep in dependencies if dep not in state_manager.completed_steps]
                    if missing_deps:
                        logger.warning(f"[{run_id}] 의존성 미완료로 {script} 건너뛰기: {missing_deps}")
                        continue
                    
                    logger.info(f"[{run_id}] 단계 실행: {script}")
                    ok, warned, dur = run_script(script, run_id, pipeline_ctx)
                    
                    # 상태 저장
                    state_manager.save_state(script, ok, dur)
                    
                    if warned:
                        warn_count_total += 1
                    if script == "health_check.py" and not ok:
                        pipeline_ok = False
                        last_failed_step = script
                        raise Exception("헬스체크 실패로 파이프라인 중단")
                    if not ok:
                        pipeline_ok = False
                        last_failed_step = script
                        raise Exception(f"'{script}' 실행 실패")
                
                # 모든 단계 완료 시 상태 초기화
                if pipeline_ok:
                    state_manager.clear_state()
                    logger.info(f"[{run_id}] 파이프라인 완료, 상태 초기화")
                break

            except Exception as e:
                last_error = str(e)
                logger.error(f"[{run_id}] 파이프라인 실행 중 오류 발생 (시도 {attempt}/{MAX_ATTEMPTS}): {e}")
                if attempt < MAX_ATTEMPTS:
                    warn_count_total += 1
                    wait_time_minutes = INITIAL_BACKOFF_MINUTES * (2 ** (attempt - 1))
                    logger.info(f"[{run_id}] {wait_time_minutes}분 후 재시도합니다...")
                    time.sleep(wait_time_minutes * 60)
                else:
                    logger.critical(f"[{run_id}] 최대 재시도 횟수 초과. 파이프라인 최종 중단.")
                    break

        # 상태 결정
        status = "SUCCESS_WITH_WARNINGS" if (pipeline_ok and warn_count_total > 0) else ("SUCCESS" if pipeline_ok else "FAIL")
        
        # 요약 전송
        elapsed = 0.0
        try:
            started_at = float(os.environ.get("RUN_STARTED_AT", "0") or 0.0)
            if started_at:
                elapsed = time.time() - started_at
        except Exception:
            pass

        status_emoji = "✅" if status == "SUCCESS" else ("⚠️" if status == "SUCCESS_WITH_WARNINGS" else "❌")
        
        # 간결한 요약 메시지
        summary_msg = f"{status_emoji} 파이프라인 완료: {status}"
        if warn_count_total > 0:
            summary_msg += f" (경고 {warn_count_total}건)"
        if elapsed > 0:
            summary_msg += f" - {elapsed:.0f}초"
        
        _notify(summary_msg, key=f"{run_id}:pipeline_summary", cooldown_sec=15)

        end_msg = f"[{run_id}] 파이프라인 사이클 종료 (status={status}, warnings={warn_count_total})"
        logger.info(end_msg)

    except Exception as e:
        logger.error(f"파이프라인 실행 중 오류: {e}")

# ───────────────── 스케줄 설정 ─────────────────
# 스케줄 시간 설정 (config.json에서 오버라이드 가능)
SCHEDULE_TIMES = {
    "balance_open": "09:00",      # 장시작 잔액 캡처
    "screener": "09:05",          # 스크리너 실행
    "pipeline": "10:10",          # 파이프라인 실행
    "balance_close": "15:30",     # 장종료 잔액 캡처
    "daily_summary": "15:31",     # 일일 요약 전송
}

def load_schedule_config():
    """config.json에서 스케줄 시간 설정 로드"""
    try:
        from settings import settings
        config = getattr(settings, "_config", {}) or {}
        daily_summary_config = config.get("daily_summary", {})
        
        if daily_summary_config.get("enabled", True):
            # config.json에서 시간 오버라이드
            if "balance_open_time" in daily_summary_config:
                SCHEDULE_TIMES["balance_open"] = daily_summary_config["balance_open_time"]
            if "balance_close_time" in daily_summary_config:
                SCHEDULE_TIMES["balance_close"] = daily_summary_config["balance_close_time"]
            if "summary_send_time" in daily_summary_config:
                SCHEDULE_TIMES["daily_summary"] = daily_summary_config["summary_send_time"]
        
        # schedule_times에서 파이프라인 시간 오버라이드
        schedule_times = config.get("schedule_times", {})
        if "screener_time" in schedule_times:
            SCHEDULE_TIMES["screener"] = schedule_times["screener_time"]
        if "pipeline_time" in schedule_times:
            SCHEDULE_TIMES["pipeline"] = schedule_times["pipeline_time"]

        # 월간 유지보수 설정 오버라이드 (config.json > 환경변수 > 기본값)
        monthly_cfg = config.get("monthly_maintenance", {})
        if monthly_cfg:
            global MONTHLY_MAINTENANCE_DAY, MONTHLY_MAINTENANCE_TIME
            if "day" in monthly_cfg:
                MONTHLY_MAINTENANCE_DAY = int(monthly_cfg["day"])
            if "time" in monthly_cfg:
                MONTHLY_MAINTENANCE_TIME = str(monthly_cfg["time"])

    except Exception as e:
        logger.warning(f"스케줄 설정 로드 실패, 기본값 사용: {e}")

# ───────────────── 스케줄 등록 ─────────────────
def register_jobs():
    """스케줄 작업 등록"""
    # 설정 로드
    load_schedule_config()
    
    # 스케줄 등록
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        # 장시작시 잔액 캡처
        getattr(schedule.every(), day).at(SCHEDULE_TIMES["balance_open"]).do(capture_balance_snapshot, "open")
        
        # 스크리너 실행
        getattr(schedule.every(), day).at(SCHEDULE_TIMES["screener"]).do(run_screener_job)
        
        # 파이프라인 실행
        getattr(schedule.every(), day).at(SCHEDULE_TIMES["pipeline"]).do(run_trading_pipeline)
        
        batch_check_time, reconcile_time, batch_on, recon_on = _resolve_batch_reconcile_times(
            getattr(settings, "_config", None) or {}
        )
        if batch_on:
            getattr(schedule.every(), day).at(batch_check_time).do(run_batch_execution_check)
        if recon_on:
            getattr(schedule.every(), day).at(reconcile_time).do(run_order_reconcile_job)
        
        # 장종료시 잔액 캡처
        getattr(schedule.every(), day).at(SCHEDULE_TIMES["balance_close"]).do(capture_balance_snapshot, "close")
        
        # 일일 요약 전송
        getattr(schedule.every(), day).at(SCHEDULE_TIMES["daily_summary"]).do(send_daily_trading_summary)

    # 월 1회 유지보수: 매일 점검하여 실행일에만 1회 실행(주말/휴일 포함)
    schedule.every().day.at(MONTHLY_MAINTENANCE_TIME).do(run_monthly_maintenance_if_due)
    batch_t, recon_t, batch_on, recon_on = _resolve_batch_reconcile_times(
        getattr(settings, "_config", None) or {}
    )
    logger.info(
        "[SCHEDULE] batch_check=%s (%s) | order_reconcile=%s (%s) | MARKET=%s",
        batch_t,
        "on" if batch_on else "off",
        recon_t,
        "on" if recon_on else "off",
        MARKET,
    )
    logger.info(
        f"[SCHEDULE] 월간 유지보수 등록: 매월 {MONTHLY_MAINTENANCE_DAY}일 {MONTHLY_MAINTENANCE_TIME} "
        f"(reviewer.py → cleanup_output.py)"
    )

def list_jobs():
    """등록된 작업 목록 출력 (요약)"""
    try:
        local_tz = datetime.now().astimezone().tzinfo
        jobs_by_type = {}
        
        for j in schedule.get_jobs():
            job_name = str(j.job_func.__name__)
            if job_name not in jobs_by_type:
                jobs_by_type[job_name] = []
            
            nr = j.next_run
            if nr:
                nr_local = nr.replace(tzinfo=local_tz)
                nr_kst = nr_local.astimezone(KST)
                jobs_by_type[job_name].append(nr_kst.strftime("%m-%d %H:%M"))
        
        # 요약 출력
        for job_name, times in jobs_by_type.items():
            if times:
                next_time = min(times)
                logger.info(f"[SCHEDULE] {job_name}: 다음 실행 {next_time} (총 {len(times)}개)")
    except Exception:
        pass

# ───────────────── 메인 실행 ─────────────────
print("=== integrated_manager.py 스크립트 로드됨 ===")

if __name__ == "__main__":
    print("=== integrated_manager.py 메인 실행 ===")
    import argparse
    
    parser = argparse.ArgumentParser(description="통합 매니저 실행")
    parser.add_argument("--once", action="store_true", help="단발 실행 (스케줄 없이)")
    parser.add_argument("--capture-open", action="store_true", help="장시작 잔액 캡처")
    parser.add_argument("--capture-close", action="store_true", help="장종료 잔액 캡처")
    parser.add_argument("--send-summary", action="store_true", help="일일 요약 전송")
    parser.add_argument("--no-background-risk", action="store_true", help="백그라운드 RiskManager 비활성화")
    args = parser.parse_args()
    
    print(f"인수 파싱 완료: {args}")
    
    # 백그라운드 RiskManager 인스턴스
    background_risk_manager = None
    
    if args.capture_open:
        capture_balance_snapshot("open")
    elif args.capture_close:
        capture_balance_snapshot("close")
    elif args.send_summary:
        send_daily_trading_summary()
    elif args.once:
        # 단발 실행
        logger.info("통합 매니저 단발 실행")
        capture_balance_snapshot("open")
        run_screener_job()
        run_trading_pipeline()
        capture_balance_snapshot("close")
        send_daily_trading_summary()
    else:
        # 스케줄 실행
        register_jobs()
        list_jobs()
        
        # 백그라운드 RiskManager는 별도 컨테이너에서 실행됨
        logger.info("백그라운드 RiskManager는 별도 컨테이너에서 실행됩니다.")
        
        # 시작 알림
        _notify("🚀 통합 매니저가 시작되었습니다. 스케줄 대기 중...", key="integrated_manager_startup")

        # 부팅 직후 1회 리컨실(전일/당일 pending 잔존 정리 시도)
        try:
            run_order_reconcile_job(skip_holiday_check=True)
        except Exception:
            pass
        
        logger.info("통합 매니저가 시작되었습니다. 다음 작업 대기 중...")
        
        # 시그널 핸들러 설정
        def signal_handler(signum, frame):
            logger.info("종료 신호 수신")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("사용자에 의한 종료")
        except Exception as e:
            logger.error(f"통합 매니저 실행 중 오류: {e}")
