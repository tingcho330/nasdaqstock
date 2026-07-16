# src/integrated_manager.py
"""
통합 매니저: Risk Manager + Scheduler + 일일 잔액 비교
- 장중 리스크 모니터링
- 스케줄된 파이프라인 실행
- 장시작/종료시 잔액 캡처 및 비교
- 디스코드 알림 통합 관리
"""

import os
import re
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
    _parse_summary_payload,
    resolve_pipeline_context,
    resolve_market_session_identity,
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
# 주의: performance_review.py, cleanup_output.py 는 일일 파이프라인에서 제외하고
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

# 월 1회 유지보수 스크립트(성과 리뷰 → 산출물 정리)
MONTHLY_SCRIPTS = [
    "performance_review.py",
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


def _parse_float_amount(val) -> float:
    try:
        if val is None or val == "":
            return 0.0
        return round(float(str(val).replace(",", "").strip()), 2)
    except (TypeError, ValueError):
        return 0.0


def _fmt_krw(amount: int) -> str:
    return f"{int(amount):,}원"


def _fmt_krw_signed(amount: int) -> str:
    v = int(amount)
    if v > 0:
        return f"+{v:,}원"
    if v < 0:
        return f"{v:,}원"
    return "0원"


def _kis_metrics_from_row(row: Dict) -> Dict:
    """US 일일 요약: summary KIS 필드 (원화/USD/환율/예수금 세분화)."""
    empty = {
        "tot_evlu_amt_krw": 0,
        "tot_evlu_amt_usd": 0.0,
        "ord_psbl_frcr_amt": 0,
        "usd_cash_total": 0.0,
        "usd_withdrawable": 0.0,
        "usd_sell_reuse": 0.0,
        "usd_buy_margin": 0.0,
        "krw_cash": 0,
        "bass_exrt": 0.0,
        "ovrs_rlzt_pfls_amt": 0.0,
        "evlu_pfls_smtl_amt": 0,
    }
    if not row:
        return empty
    orderable = (
        _parse_amount(row.get("ord_psbl_frcr_amt"))
        or _parse_amount(row.get("available_cash"))
        or _parse_amount(row.get("prvs_rcdl_excc_amt"))
    )
    return {
        "tot_evlu_amt_krw": _parse_amount(row.get("tot_evlu_amt_krw")),
        "tot_evlu_amt_usd": _parse_float_amount(row.get("tot_evlu_amt_usd")),
        "ord_psbl_frcr_amt": orderable,
        "usd_cash_total": _parse_float_amount(row.get("usd_cash_total"))
        or _parse_float_amount(row.get("dnca_tot_amt"))
        or _parse_float_amount(row.get("frcr_buy_amt")),
        "usd_withdrawable": _parse_float_amount(row.get("usd_withdrawable"))
        or float(orderable),
        "usd_sell_reuse": _parse_float_amount(row.get("usd_sell_reuse")),
        "usd_buy_margin": _parse_float_amount(row.get("usd_buy_margin")),
        "krw_cash": _parse_amount(row.get("krw_cash")),
        "bass_exrt": _parse_float_amount(row.get("bass_exrt")),
        "ovrs_rlzt_pfls_amt": _parse_float_amount(row.get("ovrs_rlzt_pfls_amt")),
        "evlu_pfls_smtl_amt": _parse_amount(row.get("evlu_pfls_smtl_amt")),
    }


def _usd_portfolio_total(cash: float, holdings_value: float) -> float:
    return round(float(cash) + float(holdings_value), 2)


def _fmt_fx(rate: float) -> str:
    if rate <= 0:
        return "N/A"
    return f"{rate:,.2f}원/USD"


def _load_summary_row_from_path(path: Path) -> Dict:
    try:
        if not path.is_file():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return _parse_summary_payload(json.load(f))
    except Exception as e:
        logger.debug("summary 로드 실패 %s: %s", path, e)
        return {}


def _summary_date_for_snapshot(snap: Dict) -> Optional[str]:
    """summary_YYYYMMDD.json 조회용 — KST 캡처일 우선 (US open의 session_close_date 제외)."""
    return snap.get("date") or snap.get("file_date")


def _kis_metrics_from_snapshot(snap: Dict) -> Dict:
    """balance 스냅샷에 embedded kis_summary 또는 summary_*.json 참조."""
    date_key = _summary_date_for_snapshot(snap)
    if date_key:
        row = _load_summary_row_from_path(OUTPUT_DIR / f"summary_{date_key}.json")
        if row:
            return _kis_metrics_from_row(row)

    embedded = snap.get("kis_summary")
    if isinstance(embedded, dict) and embedded:
        return _kis_metrics_from_row(embedded)

    summary_path = snap.get("summary_file")
    if summary_path and date_key:
        p = Path(summary_path)
        if not p.is_file():
            p = OUTPUT_DIR / p.name
        if date_key in p.name:
            row = _load_summary_row_from_path(p)
            if row:
                return _kis_metrics_from_row(row)

    return _kis_metrics_from_row({})


def _holdings_detail_from_rows(holdings: List[Dict]) -> List[Dict]:
    """보유 종목 상세 (USD 소수 현재가·평가금액 허용)."""
    mkt = MARKET
    out: List[Dict] = []
    for h in holdings:
        qty = _parse_amount(h.get("hldg_qty", 0))
        if qty <= 0:
            continue
        price = _parse_float_amount(h.get("prpr", 0))
        evlu = _parse_float_amount(h.get("evlu_amt", 0))
        if evlu <= 0 and price > 0:
            evlu = round(qty * price, 2)
        out.append({
            "ticker": normalize_ticker_6(h.get("pdno", ""), mkt),
            "name": h.get("prdt_name", "N/A"),
            "qty": qty,
            "price": price,
            "value": evlu,
        })
    return out


def _holdings_value_from_rows(holdings: List[Dict]) -> float:
    return sum(h["value"] for h in _holdings_detail_from_rows(holdings))


def _portfolio_totals_from_cash_map(
    cash_map: Dict, holdings: List[Dict]
) -> Tuple[int, int, float]:
    """(총평가, 예수금, 보유평가) — US는 예수금+보유평가가 KIS 총평가보다 신뢰될 때 우선."""
    hv = _holdings_value_from_rows(holdings)
    if hv <= 0:
        hv = sum(_parse_float_amount(h.get("evlu_amt")) for h in holdings)
        if hv <= 0:
            hv = sum(
                _parse_amount(h.get("hldg_qty")) * _parse_float_amount(h.get("prpr"))
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


def _portfolio_totals_from_snapshot(snap: Dict) -> Tuple[int, int, float]:
    """저장된 스냅샷의 총평가 보정 (과거 cash-only total_balance 호환)."""
    cash = _parse_amount(snap.get("cash"))
    hv = _parse_float_amount(snap.get("holdings_value"))
    if hv <= 0 and snap.get("holdings_detail"):
        hv = sum(_parse_float_amount(h.get("value")) for h in snap["holdings_detail"])
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


# ───────────────── daily balance 파일 레이아웃 ─────────────────
# canonical (신규 primary): daily_balances/canonical/balance_{type}_trade_{trade_date}.json
# legacy (읽기 전용 fallback): daily_balances/balance_{type}_{YYYYMMDD}.json
#   └ 파일명 날짜가 trade_date일 수도, KST session_close alias일 수도 있으므로
#     반드시 metadata.trade_date 기준으로만 선택한다.
_BALANCE_FILENAME_RE = re.compile(r"^balance_(open|close)_(\d{8})\.json$")


def _canonical_balance_dir() -> Path:
    return BALANCE_STORAGE_PATH / "canonical"


def _canonical_balance_path(snapshot_type: str, trade_date: str) -> Path:
    return _canonical_balance_dir() / f"balance_{snapshot_type}_trade_{trade_date}.json"


def _legacy_balance_path(snapshot_type: str, file_date: str) -> Path:
    return BALANCE_STORAGE_PATH / f"balance_{snapshot_type}_{file_date}.json"


def _load_balance_json(path: Path) -> Optional[Dict]:
    try:
        if not path.is_file():
            return None
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else None
    except Exception as e:
        logger.warning("daily balance 로드 실패 %s: %s", path, e)
        return None


def _snapshot_is_valid(payload: Dict) -> bool:
    return payload.get("valid", True) is not False


def _snapshot_trade_date(payload: Dict, path: Optional[Path] = None) -> Optional[str]:
    """metadata.trade_date 우선. 구버전 파일은 date → 파일명 순 폴백."""
    td = str(payload.get("trade_date") or "").strip()
    if td:
        return td
    td = str(payload.get("date") or "").strip()
    if td:
        return td
    if path is not None:
        m = _BALANCE_FILENAME_RE.match(path.name)
        if m:
            return m.group(2)
    return None


def _legacy_candidate_sort(candidates: List[Tuple[Dict, Path]]) -> List[Tuple[Dict, Path]]:
    """동일 trade_date 후보 우선순위: canonical > non-alias > kis_account_snapshot > 최신."""
    ordered = sorted(
        candidates,
        key=lambda it: str(
            it[0].get("generated_at_kst") or it[0].get("timestamp") or ""
        ),
        reverse=True,
    )
    ordered.sort(
        key=lambda it: (
            0 if it[0].get("canonical") is True else 1,
            0 if it[0].get("legacy_alias") is not True else 1,
            0 if it[0].get("source") == "kis_account_snapshot" else 1,
        )
    )
    return ordered


def find_balance_snapshot(
    trade_date: str,
    snapshot_type: str,
) -> Tuple[Optional[Dict], Optional[Path], str]:
    """
    target trade_date의 스냅샷 조회 — (snapshot, path, resolution).

    1) canonical/balance_{type}_trade_{trade_date}.json
    2) legacy daily_balances/*.json 을 metadata 기준으로 스캔
       (파일명 날짜 연산 금지 — valid=true, type 일치, trade_date 일치만 후보)
    resolution: canonical | legacy_exact | legacy_metadata | missing
    """
    cand = _canonical_balance_path(snapshot_type, trade_date)
    payload = _load_balance_json(cand)
    if payload is not None:
        if (
            _snapshot_is_valid(payload)
            and str(payload.get("type") or snapshot_type) == snapshot_type
            and _snapshot_trade_date(payload, cand) == trade_date
        ):
            return payload, cand, "canonical"
        logger.warning(
            "canonical daily balance invalid/mismatch, legacy fallback: %s", cand
        )

    candidates: List[Tuple[Dict, Path]] = []
    for p in sorted(BALANCE_STORAGE_PATH.glob(f"balance_{snapshot_type}_*.json")):
        if not _BALANCE_FILENAME_RE.match(p.name):
            continue
        pl = _load_balance_json(p)
        if pl is None or not _snapshot_is_valid(pl):
            continue
        if str(pl.get("type") or snapshot_type) != snapshot_type:
            continue
        if _snapshot_trade_date(pl, p) != trade_date:
            continue
        candidates.append((pl, p))

    if not candidates:
        return None, None, "missing"

    ordered = _legacy_candidate_sort(candidates)
    pl, p = ordered[0]
    resolution = (
        "legacy_exact"
        if p.name == f"balance_{snapshot_type}_{trade_date}.json"
        else "legacy_metadata"
    )
    logger.info(
        "daily balance legacy fallback 선택: type=%s trade_date=%s file=%s "
        "(canonical=%s legacy_alias=%s source=%s, 이유=metadata.trade_date 일치)",
        snapshot_type,
        trade_date,
        p.name,
        pl.get("canonical"),
        pl.get("legacy_alias"),
        pl.get("source"),
    )
    return pl, p, resolution


def _validate_balance_pair(
    open_snap: Optional[Dict],
    close_snap: Optional[Dict],
    target_trade_date: str,
    open_path: Optional[Path] = None,
    close_path: Optional[Path] = None,
) -> List[str]:
    """open/close pair 정합성 — 반드시 같은 trade_date + type/valid 일치."""
    errors: List[str] = []
    if open_snap is not None:
        if str(open_snap.get("type") or "open") != "open":
            errors.append(f"open.type={open_snap.get('type')}")
        if not _snapshot_is_valid(open_snap):
            errors.append("open.valid=false")
        otd = _snapshot_trade_date(open_snap, open_path)
        if otd != target_trade_date:
            errors.append(f"open.trade_date={otd} != target={target_trade_date}")
    if close_snap is not None:
        if str(close_snap.get("type") or "close") != "close":
            errors.append(f"close.type={close_snap.get('type')}")
        if not _snapshot_is_valid(close_snap):
            errors.append("close.valid=false")
        ctd = _snapshot_trade_date(close_snap, close_path)
        if ctd != target_trade_date:
            errors.append(f"close.trade_date={ctd} != target={target_trade_date}")
    if open_snap is not None and close_snap is not None:
        otd = _snapshot_trade_date(open_snap, open_path)
        ctd = _snapshot_trade_date(close_snap, close_path)
        if otd != ctd:
            errors.append(f"open.trade_date={otd} != close.trade_date={ctd}")
    return errors


def load_daily_balance_pair(target_trade_date: str) -> Dict:
    """
    target trade_date의 open/close pair — metadata.trade_date 기준으로만 pairing.

    금지: 파일명에서 ±1일 연산으로 open 선택, session_close_date만 같은 pairing.
    불일치 시 DAILY_BALANCE_PAIR_TRADE_DATE_MISMATCH를 ERROR로 기록한다.
    """
    open_snap, open_path, open_res = find_balance_snapshot(target_trade_date, "open")
    close_snap, close_path, close_res = find_balance_snapshot(target_trade_date, "close")
    errors = _validate_balance_pair(
        open_snap, close_snap, target_trade_date, open_path, close_path
    )
    if errors:
        logger.error(
            "DAILY_BALANCE_PAIR_TRADE_DATE_MISMATCH trade_date=%s: %s",
            target_trade_date,
            "; ".join(errors),
        )
    return {
        "trade_date": target_trade_date,
        "open": open_snap,
        "close": close_snap,
        "open_path": open_path,
        "close_path": close_path,
        "open_resolution": open_res,
        "close_resolution": close_res,
        "errors": errors,
        "pair_ok": bool(open_snap and close_snap and not errors),
    }


def capture_balance_snapshot(
    snapshot_type: str,
    *,
    rebuild: bool = False,
    trade_date: Optional[str] = None,
) -> Dict:
    """잔액 스냅샷 캡처 (open/close) — 구조화 결과 반환.

    Returns:
        {"status": "existing_valid" | "created" | "failed",
         "trade_date": ..., "path": ..., "source": ..., "snapshot": ..., "reason": ...}

    Source priority:
    1) KIS account_snapshot_{market}_{trade_date}*.json (valid)
    2) Same trade_date valid=true KIS-derived cached snapshot
    Forbidden:
    - copying past dated balance JSON as current; DB position-only substitute
    - 과거 trade_date 스냅샷을 현재 계좌 상태로 재생성 (historical recapture)
    Past daily balance files are not overwritten unless rebuild=True / --rebuild-daily-balance.
    """
    def _result(status: str, **kw) -> Dict:
        base = {
            "status": status,
            "trade_date": kw.pop("trade_date", None),
            "path": kw.pop("path", None),
            "source": kw.pop("source", None),
        }
        base.update(kw)
        return base

    try:
        from account_snapshot import extract_account_file_date

        identity = resolve_market_session_identity(
            MARKET, explicit_trade_date=trade_date
        )
        target_td = identity["trade_date"]

        # 이미 valid 스냅샷이 있으면 재캡처하지 않는다 (canonical 우선, metadata fallback)
        if not rebuild:
            existing, existing_path, existing_res = find_balance_snapshot(
                target_td, snapshot_type
            )
            if existing is not None:
                logger.info(
                    "%s_SNAPSHOT_PRESENT: trade_date=%s file=%s resolution=%s → 캡처 스킵",
                    snapshot_type.upper(),
                    target_td,
                    existing_path,
                    existing_res,
                )
                return _result(
                    "existing_valid",
                    trade_date=target_td,
                    path=str(existing_path),
                    source=existing.get("source"),
                    snapshot=existing,
                    resolution=existing_res,
                )

        # 과거 세션 스냅샷을 현재 계좌 상태로 만들지 않는다
        if not identity["is_live_trade_date"]:
            logger.error(
                "[HISTORICAL_SNAPSHOT_RECAPTURE_FORBIDDEN] type=%s trade_date=%s "
                "live_trade_date=%s — 현재 계좌 상태로 과거 스냅샷 재생성 금지",
                snapshot_type,
                target_td,
                identity["live_trade_date"],
            )
            return _result(
                "failed",
                trade_date=target_td,
                reason="historical_recapture_forbidden",
            )

        ctx = resolve_pipeline_context(market=MARKET, mode="live")
        trade_date = target_td
        session = ctx.get("session") or "pm"

        # Prefer live KIS account snapshot artifact
        snap_candidates = [
            OUTPUT_DIR / f"account_snapshot_{MARKET}_{trade_date}_{session}.json",
            OUTPUT_DIR / f"account_snapshot_{MARKET}_{trade_date}.json",
            OUTPUT_DIR / f"account_snapshot_latest_{MARKET}.json",
        ]
        kis_payload = None
        source_snapshot_file = None
        for cand in snap_candidates:
            if not cand.exists():
                continue
            try:
                with open(cand, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("valid") is False:
                continue
            payload_td = str(payload.get("trade_date") or "").strip()
            if cand.name.startswith("account_snapshot_latest_"):
                if payload_td and payload_td != trade_date:
                    logger.warning(
                        "skip stale latest account_snapshot trade_date=%s want=%s",
                        payload_td,
                        trade_date,
                    )
                    continue
            elif payload_td and payload_td != trade_date:
                continue
            kis_payload = payload
            source_snapshot_file = str(cand)
            break

        if kis_payload is None:
            # Trigger fresh KIS snapshot via account.py then trader-style endpoint if needed
            import subprocess
            try:
                subprocess.run(
                    ["python", "/app/src/account.py"],
                    capture_output=True,
                    text=True,
                    check=False,
                    encoding="utf-8",
                    timeout=60,
                )
            except Exception as e:
                logger.warning("account.py refresh for daily balance failed: %s", e)

            cash_map, holdings, summary_path, balance_path = get_account_snapshot_cached(
                summary_pattern="summary_*.json",
                balance_pattern="balance_*.json",
                ttl_sec=5,
                exact_trade_date=trade_date,
            )
            file_meta = extract_account_file_date(balance_path)
            if file_meta.get("account_file_date") and file_meta["account_file_date"] != trade_date:
                logger.error(
                    "[ACCOUNT_FILE_STALE] refusing stale balance as daily source "
                    "file_date=%s trade_date=%s — no stale fallback",
                    file_meta.get("account_file_date"),
                    trade_date,
                )
                return _result(
                    "failed", trade_date=trade_date, reason="account_file_stale"
                )

            holdings_detail = _holdings_detail_from_rows(holdings)
            total_balance, cash, holdings_value = _portfolio_totals_from_cash_map(
                cash_map, holdings
            )
            if holdings_value <= 0 and total_balance > cash:
                holdings_value = total_balance - cash
            kis_summary = _kis_metrics_from_row(cash_map)
            source = "kis_account_file_same_date"
            source_snapshot_file = str(balance_path) if balance_path else None
            snapshot_ts = datetime.now(KST).isoformat()
        else:
            cash_usd = float(kis_payload.get("available_cash_usd") or 0)
            holdings_value = float(kis_payload.get("holdings_value_usd") or 0)
            total_balance = float(kis_payload.get("total_asset_usd") or (cash_usd + holdings_value))
            cash = cash_usd
            tickers = kis_payload.get("tickers") or []
            holdings_detail = [
                {"ticker": t, "qty": None, "value": None} for t in tickers
            ]
            # Prefer sellable evidence for qty when present
            sellable = kis_payload.get("sellable_qty_by_ticker") or {}
            if sellable:
                holdings_detail = [
                    {"ticker": t, "qty": sellable.get(t), "value": None}
                    for t in tickers
                ]
            kis_summary = {
                "ord_psbl_frcr_amt": cash_usd,
                "tot_evlu_amt_usd": total_balance,
                "tot_evlu_amt_krw": float(kis_payload.get("total_asset_krw") or 0),
                "bass_exrt": None,
                "krw_cash": float(kis_payload.get("krw_cash") or 0),
            }
            source = "kis_account_snapshot"
            snapshot_ts = (
                kis_payload.get("snapshot_ts_kst")
                or kis_payload.get("generated_at_kst")
                or datetime.now(KST).isoformat()
            )
            summary_path = None
            balance_path = None

        now = datetime.now(KST)
        # Canonical daily balance key = trade_date only.
        # KST alias 파일은 더 이상 생성하지 않는다 — pairing은 metadata.trade_date 기준.
        snap_date = trade_date

        broker_only_count = 0
        try:
            reconcile_path = OUTPUT_DIR / f"order_reconcile_latest_{MARKET}.json"
            if reconcile_path.exists():
                with open(reconcile_path, "r", encoding="utf-8") as f:
                    recon = json.load(f) or {}
                broker_only_count = int(
                    (recon.get("db_reconcile") or {}).get("broker_only_order_count")
                    or (recon.get("db_reconcile") or {}).get("broker_only_count")
                    or recon.get("broker_only_order_count")
                    or 0
                )
                for finding in recon.get("findings") or []:
                    if finding.get("title") == "BROKER_TRADE_MISSING_IN_DB":
                        broker_only_count = max(broker_only_count, 1)
        except Exception:
            pass

        position_reconciled = broker_only_count == 0

        # ── 통화·단위 명시 (US: USD primary / KR: KRW) ──
        us = is_us_market(MARKET)
        fx_rate = _parse_float_amount(kis_summary.get("bass_exrt")) or None
        if us:
            currency_meta = {
                "base_currency": "USD",
                "total_asset_usd": round(float(cash) + float(holdings_value), 2),
                "available_cash_usd": round(float(cash), 2),
                "holdings_value_usd": round(float(holdings_value), 2),
                "total_asset_krw": _parse_amount(kis_summary.get("tot_evlu_amt_krw")) or None,
                "available_cash_krw": _parse_amount(kis_summary.get("krw_cash")) or None,
                "holdings_value_krw": None,
                "fx_rate_used": fx_rate,
                "fx_rate_timestamp": snapshot_ts if fx_rate else None,
                "value_semantics": "usd_cash_plus_holdings",
            }
        else:
            currency_meta = {
                "base_currency": "KRW",
                "total_asset_usd": None,
                "available_cash_usd": None,
                "holdings_value_usd": None,
                "total_asset_krw": _parse_amount(total_balance),
                "available_cash_krw": _parse_amount(cash),
                "holdings_value_krw": _parse_amount(holdings_value),
                "fx_rate_used": None,
                "fx_rate_timestamp": None,
                "value_semantics": "krw_total",
            }

        snapshot = {
            "date": snap_date,
            "trade_date": trade_date,
            "session_open_date_kst": identity["session_open_date_kst"],
            "session_close_date_kst": identity["session_close_date_kst"],
            "session_close_date": identity["session_close_date_kst"],
            "timestamp": now.isoformat(),
            "snapshot_ts_kst": snapshot_ts,
            "generated_at_kst": now.isoformat(),
            "type": snapshot_type,
            "source": source,
            "source_snapshot_file": source_snapshot_file,
            "valid": True,
            "legacy_alias": False,
            "canonical": True,
            "position_reconciled": position_reconciled,
            "broker_only_order_count": broker_only_count,
            "db_vs_kis_position_match": position_reconciled,
            "total_balance": total_balance,
            "cash": cash,
            "holdings_value": holdings_value,
            "holdings_count": len(holdings_detail),
            "holdings_detail": holdings_detail,
            "kis_summary": kis_summary,
            "summary_file": str(summary_path) if summary_path else None,
            "balance_file": str(balance_path) if balance_path else None,
            **currency_meta,
        }

        # 신규 canonical: canonical/balance_{type}_trade_{trade_date}.json (primary)
        # 호환용 legacy: balance_{type}_{trade_date}.json (기존 리더 호환, alias 아님)
        canonical_path = _canonical_balance_path(snapshot_type, trade_date)
        legacy_path = _legacy_balance_path(snapshot_type, trade_date)
        writes: List[Tuple[Path, Dict]] = [
            (canonical_path, {**snapshot, "file_date": snap_date}),
            (legacy_path, {**snapshot, "file_date": snap_date}),
        ]
        for filepath, snap_copy in writes:
            if filepath.exists() and not rebuild:
                logger.info(
                    "daily balance exists, skip overwrite (use --rebuild-daily-balance): %s",
                    filepath,
                )
                continue
            filepath.parent.mkdir(parents=True, exist_ok=True)
            tmp = filepath.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(snap_copy, f, ensure_ascii=False, indent=2)
            tmp.replace(filepath)
            logger.info(
                "잔액 스냅샷 저장: %s source=%s trade_date=%s canonical=%s",
                filepath,
                source,
                trade_date,
                snap_copy.get("canonical"),
            )
        return _result(
            "created",
            trade_date=trade_date,
            path=str(canonical_path),
            source=source,
            snapshot=snapshot,
        )

    except Exception as e:
        logger.error(f"잔액 스냅샷 캡처 실패: {type(e).__name__}: {str(e)}")
        logger.debug("잔액 스냅샷 캡처 상세 오류:", exc_info=True)
        return _result("failed", trade_date=trade_date, reason=f"{type(e).__name__}: {e}")

def migrate_daily_balance_layout(
    trade_date: Optional[str] = None,
    *,
    apply: bool = False,
) -> Dict:
    """
    legacy daily balance → 신규 canonical 레이아웃 마이그레이션.

    - legacy 파일은 삭제하지 않는다 (복사만).
    - metadata.trade_date 기준으로 후보를 선택한다 (파일명 날짜 무시).
    - 현재 계좌를 다시 조회해 과거 snapshot을 만들지 않는다.
    - apply=False(dry-run)면 파일을 변경하지 않는다.
    """
    groups: Dict[Tuple[str, str], List[Tuple[Dict, Path]]] = {}
    for p in sorted(BALANCE_STORAGE_PATH.glob("balance_*_*.json")):
        m = _BALANCE_FILENAME_RE.match(p.name)
        if not m:
            continue
        pl = _load_balance_json(p)
        if pl is None or not _snapshot_is_valid(pl):
            continue
        snap_type = str(pl.get("type") or m.group(1))
        if snap_type not in ("open", "close"):
            continue
        td = _snapshot_trade_date(pl, p)
        if not td or not re.fullmatch(r"\d{8}", td):
            continue
        if trade_date and td != trade_date:
            continue
        groups.setdefault((snap_type, td), []).append((pl, p))

    actions: List[Dict] = []
    for (snap_type, td), candidates in sorted(groups.items()):
        dest = _canonical_balance_path(snap_type, td)
        if dest.exists():
            actions.append({
                "action": "skip_exists",
                "type": snap_type,
                "trade_date": td,
                "dest": str(dest),
            })
            continue
        pl, src = _legacy_candidate_sort(candidates)[0]
        try:
            session_ident = resolve_market_session_identity(
                MARKET, explicit_trade_date=td
            )
            session_open_kst = session_ident["session_open_date_kst"]
            session_close_kst = session_ident["session_close_date_kst"]
        except Exception:
            session_open_kst = session_close_kst = td

        migrated = dict(pl)
        migrated.pop("alias_of_trade_date", None)
        migrated.pop("alias_purpose", None)
        migrated.update({
            "trade_date": td,
            "file_date": td,
            "session_open_date_kst": session_open_kst,
            "session_close_date_kst": session_close_kst,
            "session_close_date": session_close_kst,
            "canonical": True,
            "legacy_alias": False,
            "migrated_from": src.name,
            "migrated_at_kst": datetime.now(KST).isoformat(),
        })
        action = {
            "action": "copy" if apply else "would_copy",
            "type": snap_type,
            "trade_date": td,
            "source": str(src),
            "dest": str(dest),
        }
        if apply:
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(migrated, f, ensure_ascii=False, indent=2)
            tmp.replace(dest)
        actions.append(action)
        logger.info(
            "[MIGRATE_DAILY_BALANCE] %s type=%s trade_date=%s %s → %s",
            action["action"],
            snap_type,
            td,
            src.name,
            dest.name,
        )

    result = {"dry_run": not apply, "actions": actions}
    logger.info(
        "[MIGRATE_DAILY_BALANCE] %s: %d개 대상 (copy=%d, skip=%d) — legacy 파일 삭제 없음",
        "dry-run" if not apply else "apply",
        len(actions),
        sum(1 for a in actions if a["action"] in ("copy", "would_copy")),
        sum(1 for a in actions if a["action"] == "skip_exists"),
    )
    return result


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
    """장시작/종료 잔액 비교 (US: USD 수익률 Primary + KIS 환율 분해 / KR: 스냅샷 총평가)."""
    try:
        us_kis = is_us_market(MARKET)
        if us_kis:
            om = _kis_metrics_from_snapshot(open_balance)
            cm = _kis_metrics_from_snapshot(close_balance)
            open_total = om["tot_evlu_amt_krw"]
            close_total = cm["tot_evlu_amt_krw"]

            # USD 값은 동일 단위끼리만 비교: 명시적 *_usd 필드 우선, KIS metric 폴백
            def _usd_cash(snap: Dict, metrics: Dict) -> float:
                if snap.get("available_cash_usd") is not None:
                    return _parse_float_amount(snap.get("available_cash_usd"))
                return float(metrics["ord_psbl_frcr_amt"])

            def _usd_hv(snap: Dict) -> float:
                if snap.get("holdings_value_usd") is not None:
                    return _parse_float_amount(snap.get("holdings_value_usd"))
                _, _, hv = _portfolio_totals_from_snapshot(snap)
                return hv

            open_cash = _usd_cash(open_balance, om)
            close_cash = _usd_cash(close_balance, cm)
            open_hv = _usd_hv(open_balance)
            close_hv = _usd_hv(close_balance)

            open_usd_total = _usd_portfolio_total(open_cash, open_hv)
            close_usd_total = _usd_portfolio_total(close_cash, close_hv)
            usd_change = round(close_usd_total - open_usd_total, 2)
            daily_return_usd_pct = (
                (usd_change / open_usd_total) * 100 if open_usd_total > 0 else 0.0
            )
            daily_return_pct = daily_return_usd_pct

            open_fx = om.get("bass_exrt") or 0.0
            close_fx = cm.get("bass_exrt") or 0.0
            open_krw_cash = om.get("krw_cash") or 0
            close_krw_cash = cm.get("krw_cash") or 0

            trading_pnl_krw = 0
            fx_impact_krw = 0
            if open_fx > 0:
                trading_pnl_krw = int(round(
                    usd_change * open_fx + (close_krw_cash - open_krw_cash)
                ))
                close_at_open_fx = int(round(close_usd_total * open_fx + close_krw_cash))
                fx_impact_krw = close_total - close_at_open_fx
        else:
            open_total, open_cash, open_hv = _portfolio_totals_from_snapshot(open_balance)
            close_total, close_cash, close_hv = _portfolio_totals_from_snapshot(close_balance)
            open_usd_total = close_usd_total = usd_change = 0.0
            daily_return_usd_pct = 0.0
            open_fx = close_fx = 0.0
            trading_pnl_krw = fx_impact_krw = 0
            open_krw_cash = close_krw_cash = 0

        total_change = close_total - open_total
        cash_change = close_cash - open_cash
        holdings_change = close_hv - open_hv

        if not us_kis:
            daily_return_pct = (total_change / open_total) * 100 if open_total > 0 else 0
        
        # 보유종목 변화 분석
        open_detail = open_balance.get("holdings_detail") or []
        close_detail = close_balance.get("holdings_detail") or []
        open_tickers = {h["ticker"] for h in open_detail}
        close_tickers = {h["ticker"] for h in close_detail}
        
        sold_tickers = open_tickers - close_tickers
        bought_tickers = close_tickers - open_tickers
        held_tickers = open_tickers & close_tickers
        
        # 실현 손익: KIS ovrs_rlzt_pfls_amt delta (US) / 매도 종목 open 평가 (KR 폴백)
        realized_pnl = 0.0
        if us_kis:
            open_rlzt = om.get("ovrs_rlzt_pfls_amt") or 0.0
            close_rlzt = cm.get("ovrs_rlzt_pfls_amt") or 0.0
            if close_rlzt > 0 or open_rlzt > 0:
                realized_pnl = round(close_rlzt - open_rlzt, 2)
            else:
                realized_pnl = 0.0
        else:
            for ticker in sold_tickers:
                open_holding = next(
                    (h for h in open_balance["holdings_detail"] if h["ticker"] == ticker),
                    None,
                )
                if open_holding:
                    realized_pnl += open_holding["value"]
        
        unrealized_pnl = 0.0
        for ticker in held_tickers:
            open_holding = next((h for h in open_balance["holdings_detail"] if h["ticker"] == ticker), None)
            close_holding = next((h for h in close_balance["holdings_detail"] if h["ticker"] == ticker), None)
            if open_holding and close_holding:
                unrealized_pnl += close_holding["value"] - open_holding["value"]
        
        estimated_fees = total_change - realized_pnl - unrealized_pnl
        
        result = {
            "date": close_balance["date"],
            "total_change": total_change,
            "cash_change": cash_change,
            "holdings_change": holdings_change,
            "daily_return_pct": round(daily_return_pct, 2),
            "daily_return_usd_pct": round(daily_return_usd_pct, 2),
            "usd_change": usd_change,
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
            "open_usd_total": open_usd_total,
            "close_usd_total": close_usd_total,
            "us_kis_summary": us_kis,
        }
        if us_kis:
            result.update({
                "open_fx": open_fx,
                "close_fx": close_fx,
                "trading_pnl_krw": trading_pnl_krw,
                "fx_impact_krw": fx_impact_krw,
                "open_krw_cash": open_krw_cash,
                "close_krw_cash": close_krw_cash,
                "open_cash_detail": om,
                "close_cash_detail": cm,
            })
        return result
        
    except Exception as e:
        logger.error(f"잔액 비교 분석 실패: {type(e).__name__}: {str(e)}")
        logger.debug("잔액 비교 분석 상세 오류:", exc_info=True)
        return {}

def _resolve_usd_comparison_values(snap: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """
    US 일일 요약 비교용 USD 자산값 — (values, error_reason).

    - 신규 스냅샷: 명시적 total_asset_usd / available_cash_usd / holdings_value_usd 사용
    - legacy 스냅샷: cash+holdings_value 합과 total_balance 정합성 검사 후 USD로 간주
    - total_balance가 cash+holdings 대비 크게 어긋나면(통화 혼합 의심) ambiguous 처리
    """
    total_usd = snap.get("total_asset_usd")
    if total_usd is not None:
        cash_usd = _parse_float_amount(snap.get("available_cash_usd"))
        hv_usd = _parse_float_amount(snap.get("holdings_value_usd"))
        return (
            {
                "total": _parse_float_amount(total_usd),
                "cash": cash_usd,
                "hv": hv_usd,
                "semantics": "explicit_usd",
            },
            None,
        )

    total = _parse_float_amount(snap.get("total_balance"))
    cash = _parse_float_amount(snap.get("cash"))
    hv = _parse_float_amount(snap.get("holdings_value"))
    computed = round(cash + hv, 2)
    if computed <= 0 and total <= 0:
        return None, "no_asset_values"
    if total > 0 and computed > 0 and (total > computed * 3 or computed > total * 3):
        return (
            None,
            f"ambiguous_total_balance total_balance={total} cash+holdings={computed} "
            "(통화 혼합 의심 — USD/KRW 명시 필드 없음)",
        )
    return (
        {
            "total": computed if computed > 0 else total,
            "cash": cash,
            "hv": hv,
            "semantics": "legacy_assumed_usd",
        },
        None,
    )


def _close_capture_evidence_exists(session_close_date_kst: str) -> bool:
    """balance_close 캡처가 해당 KST 날짜에 실행된 흔적(생성 timestamp) 확인."""
    paths: List[Path] = []
    if _canonical_balance_dir().is_dir():
        paths.extend(sorted(_canonical_balance_dir().glob("balance_close_trade_*.json")))
    paths.extend(sorted(BALANCE_STORAGE_PATH.glob("balance_close_*.json")))
    for p in paths:
        pl = _load_balance_json(p)
        if not pl:
            continue
        gen = str(pl.get("generated_at_kst") or pl.get("timestamp") or "")
        if gen[:10].replace("-", "") == session_close_date_kst:
            return True
    return False


def _fmt_date_dash(yyyymmdd: str) -> str:
    s = str(yyyymmdd or "")
    if len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def _fmt_usd_cash_detail(metrics: Dict, prefix: str = "") -> str:
    """KIS 예수금 세분화 (close 스냅샷 기준 1줄 요약)."""
    if not metrics:
        return ""
    parts: List[str] = []
    total = metrics.get("usd_cash_total") or 0.0
    wdr = metrics.get("usd_withdrawable") or metrics.get("ord_psbl_frcr_amt") or 0.0
    reuse = metrics.get("usd_sell_reuse") or 0.0
    margin = metrics.get("usd_buy_margin") or 0.0
    krw = metrics.get("krw_cash") or 0
    if total > 0:
        parts.append(f"외화예수 ${total:,.2f}")
    if wdr > 0:
        parts.append(f"출금가능 ${float(wdr):,.2f}")
    if reuse > 0:
        parts.append(f"매도재사용 ${reuse:,.2f}")
    if margin > 0:
        parts.append(f"매수증거금 ${margin:,.2f}")
    line = " | ".join(parts) if parts else ""
    if krw > 0:
        krw_line = f"원화예수 {_fmt_krw(krw)}"
        line = f"{line}\n{krw_line}" if line else krw_line
    return f"{prefix}{line}" if line else ""


def send_daily_trading_summary(target_trade_date: Optional[str] = None):
    """장종료 후 당일 매매 요약 전송.

    open/close pairing은 metadata.trade_date 기준(파일명 날짜 연산 금지).
    상태: CLOSE_SNAPSHOT_PRESENT / CLOSE_SNAPSHOT_MISSING /
          CLOSE_CAPTURE_SCHEDULE_MISSED / OPEN_SNAPSHOT_UNAVAILABLE /
          DAILY_BALANCE_PAIR_TRADE_DATE_MISMATCH / DAILY_BALANCE_CURRENCY_MISMATCH
    """
    try:
        # 1) 공통 session identity — capture와 동일한 날짜 계산
        identity = resolve_market_session_identity(
            MARKET, explicit_trade_date=target_trade_date
        )
        trade_date = identity["trade_date"]
        session_close_kst = identity["session_close_date_kst"]
        logger.info(
            "[DAILY_SUMMARY_SESSION] market=%s trade_date=%s "
            "session_open_date_kst=%s session_close_date_kst=%s source=%s",
            MARKET,
            trade_date,
            identity["session_open_date_kst"],
            session_close_kst,
            identity["resolution_source"],
        )

        # 2) metadata 기준 open/close pair 조회
        pair = load_daily_balance_pair(trade_date)
        open_balance = pair["open"]
        close_balance = pair["close"]
        late_close_capture = False

        # 3) close 스냅샷 상태 판정 — 존재하면 즉시 사용, capture 재시도 금지
        if close_balance is not None:
            logger.info(
                "CLOSE_SNAPSHOT_PRESENT trade_date=%s file=%s resolution=%s",
                trade_date,
                pair["close_path"],
                pair["close_resolution"],
            )
        else:
            logger.warning(
                "CLOSE_SNAPSHOT_MISSING trade_date=%s → recovery capture 시도",
                trade_date,
            )
            cap = capture_balance_snapshot("close", trade_date=trade_date)
            if cap.get("status") in ("existing_valid", "created") and cap.get("path"):
                late_close_capture = cap["status"] == "created"
                # 반환된 path를 재로드한다 — KST 날짜로 다른 파일을 다시 찾지 않는다
                close_balance = _load_balance_json(Path(cap["path"]))
                pair["close"] = close_balance
                pair["close_path"] = Path(cap["path"])
                pair["close_resolution"] = cap.get("resolution") or "recovered"
            else:
                # 스케줄 누락은 06:00 실행 evidence까지 없을 때만
                if not _close_capture_evidence_exists(session_close_kst):
                    logger.error(
                        "CLOSE_CAPTURE_SCHEDULE_MISSED trade_date=%s "
                        "(close 스냅샷 없음 + recovery 실패 + %s 실행 evidence 없음)",
                        trade_date,
                        session_close_kst,
                    )
                else:
                    logger.error(
                        "CLOSE_SNAPSHOT_MISSING trade_date=%s (recovery 실패, "
                        "close 캡처 evidence는 존재 — 스케줄 누락 아님)",
                        trade_date,
                    )

        if not close_balance:
            _notify(
                f"⚠️ trade_date={trade_date} 일일 요약: 장종료 스냅샷 없음. "
                f"`python /app/run_integrated_manager.py --capture-close --trade-date {trade_date}` 실행 후 "
                f"`--send-summary --trade-date {trade_date}` 재시도.",
                key="daily_summary_error",
            )
            return

        # 4) open 스냅샷 — 과거 open은 현재 계좌로 재생성하지 않는다
        if not open_balance:
            logger.error(
                "OPEN_SNAPSHOT_UNAVAILABLE trade_date=%s — "
                "현재 계좌 상태로 과거 open 재생성 금지",
                trade_date,
            )
            _notify(
                f"⚠️ trade_date={trade_date} 일일 요약: 장시작 스냅샷 없음 "
                f"(metadata.trade_date={trade_date}, type=open 인 daily balance 없음). "
                "과거 open은 재생성하지 않습니다 (OPEN_SNAPSHOT_UNAVAILABLE).",
                key="daily_summary_error",
            )
            return

        # 5) 같은 trade_date pair 검증 — 불일치 시 손익 계산 중단
        pair_errors = _validate_balance_pair(
            open_balance, close_balance, trade_date, pair["open_path"], pair["close_path"]
        )
        if pair_errors:
            logger.error(
                "DAILY_BALANCE_PAIR_TRADE_DATE_MISMATCH trade_date=%s: %s",
                trade_date,
                "; ".join(pair_errors),
            )
            _notify(
                f"❌ trade_date={trade_date} 일일 요약 중단: open/close trade_date 불일치 "
                f"({'; '.join(pair_errors)})",
                key="daily_summary_error",
            )
            return

        open_file_name = pair["open_path"].name if pair["open_path"] else None
        close_file_name = pair["close_path"].name if pair["close_path"] else None
        if close_balance.get("legacy_alias") is True or pair["close_resolution"] in (
            "legacy_metadata",
        ):
            logger.info(
                "CLOSE_SNAPSHOT_LEGACY_FALLBACK_USED trade_date=%s file=%s "
                "legacy_alias=%s",
                trade_date,
                close_file_name,
                close_balance.get("legacy_alias"),
            )
        logger.info(
            "[DAILY_BALANCE_PAIR] trade_date=%s open=%s open_trade_date=%s "
            "close=%s close_trade_date=%s legacy_open=%s canonical_close=%s",
            trade_date,
            pair["open_path"],
            _snapshot_trade_date(open_balance, pair["open_path"]),
            pair["close_path"],
            _snapshot_trade_date(close_balance, pair["close_path"]),
            open_balance.get("legacy_alias") is True,
            close_balance.get("canonical") is True,
        )

        # 6) 동일 통화 기준 자산값 검증 (US: USD끼리만 비교)
        if is_us_market(MARKET):
            open_ccy, open_ccy_err = _resolve_usd_comparison_values(open_balance)
            close_ccy, close_ccy_err = _resolve_usd_comparison_values(close_balance)
            if open_ccy_err or close_ccy_err:
                logger.error(
                    "DAILY_BALANCE_CURRENCY_MISMATCH trade_date=%s open=%s close=%s "
                    "— 증감·수익률·손익 계산 생략",
                    trade_date,
                    open_ccy_err,
                    close_ccy_err,
                )
                _notify(
                    f"❌ trade_date={trade_date} 일일 요약 중단 (DAILY_BALANCE_CURRENCY_MISMATCH): "
                    f"open={open_ccy_err or 'ok'} / close={close_ccy_err or 'ok'} — "
                    "단위 불명확한 total_balance로 수익률을 계산하지 않습니다.",
                    key="daily_summary_error",
                )
                return

        # 7) 잔액 비교 분석
        comparison = compare_balances(open_balance, close_balance)
        
        # 디스코드 임베드 생성
        total_change = comparison["total_change"]
        daily_return = comparison["daily_return_pct"]
        daily_return_usd = comparison.get("daily_return_usd_pct", daily_return)
        realized_pnl = comparison["realized_pnl"]
        unrealized_pnl = comparison["unrealized_pnl"]
        estimated_fees = comparison["estimated_fees"]
        
        mkt = os.getenv("MARKET", "SP500")
        us_kis = comparison.get("us_kis_summary", is_us_market(mkt))

        # Primary 수익률: US는 USD, KR은 원화/통화
        primary_return = daily_return_usd if us_kis else daily_return
        primary_change = comparison.get("usd_change", total_change) if us_kis else total_change
        change_emoji = "📈" if primary_change > 0 else "📉" if primary_change < 0 else "➡️"
        krw_ref_emoji = "📈" if total_change > 0 else "📉" if total_change < 0 else "➡️"

        open_total = comparison["open_balance"]
        close_total = comparison["close_balance"]
        open_hv = comparison.get("open_holdings_value", 0)
        close_hv = comparison.get("close_holdings_value", 0)
        open_cash = comparison.get("open_cash", open_balance.get("cash", 0))
        close_cash = comparison.get("close_cash", close_balance.get("cash", 0))
        open_usd_total = comparison.get("open_usd_total", 0)
        close_usd_total = comparison.get("close_usd_total", 0)

        if us_kis:
            usd_total_line = (
                f"**{fmt_money(open_usd_total, mkt)}** → **{fmt_money(close_usd_total, mkt)}** "
                f"({fmt_money_signed(comparison.get('usd_change', 0), mkt)})"
            )
            hv_line = (
                f"{fmt_money(open_hv, mkt)} → {fmt_money(close_hv, mkt)} "
                f"({fmt_money_signed(comparison['holdings_change'], mkt)})"
            )
            cash_line = (
                f"{fmt_money(open_cash, mkt)} → {fmt_money(close_cash, mkt)} "
                f"({fmt_money_signed(comparison['cash_change'], mkt)})"
            )
            close_detail = _fmt_usd_cash_detail(comparison.get("close_cash_detail") or {})
            if close_detail:
                cash_line = f"{cash_line}\n{close_detail}"

            total_line = (
                f"**{_fmt_krw(open_total)}** → **{_fmt_krw(close_total)}** "
                f"({_fmt_krw_signed(total_change)})"
            )

            fields = [
                {
                    "name": f"{change_emoji} 일일 수익률 (USD)",
                    "value": (
                        f"**{daily_return_usd:+.2f}%** "
                        f"({fmt_money_signed(comparison.get('usd_change', 0), mkt)})"
                    ),
                    "inline": False,
                },
                {
                    "name": "💼 USD 총자산 (예수금+보유)",
                    "value": usd_total_line,
                    "inline": False,
                },
                {
                    "name": "📈 보유평가 (USD, 시장가)",
                    "value": hv_line,
                    "inline": True,
                },
                {
                    "name": "💵 예수금 (USD, 주문가능)",
                    "value": cash_line,
                    "inline": True,
                },
                {
                    "name": f"{krw_ref_emoji} 총평가 (원화환산, 참고)",
                    "value": total_line,
                    "inline": False,
                },
            ]

            open_fx = comparison.get("open_fx") or 0.0
            close_fx = comparison.get("close_fx") or 0.0
            if open_fx > 0 and close_fx > 0:
                fx_delta = close_fx - open_fx
                fx_lines = [
                    f"**{_fmt_fx(open_fx)}** → **{_fmt_fx(close_fx)}** "
                    f"({fx_delta:+,.2f}원/USD)",
                ]
                trading_krw = comparison.get("trading_pnl_krw")
                fx_impact = comparison.get("fx_impact_krw")
                if trading_krw is not None:
                    fx_lines.append(f"매매기여(원@open환율): {_fmt_krw_signed(trading_krw)}")
                if fx_impact is not None:
                    fx_lines.append(f"환율영향: {_fmt_krw_signed(fx_impact)}")
                fields.append({
                    "name": "💱 환율 · 원화 분해",
                    "value": "\n".join(fx_lines),
                    "inline": False,
                })
        else:
            total_line = (
                f"**{fmt_money(open_total, mkt)}** → "
                f"**{fmt_money(close_total, mkt)}** "
                f"({fmt_money_signed(total_change, mkt)})"
            )
            cash_line = (
                f"{fmt_money(open_cash, mkt)} → {fmt_money(close_cash, mkt)} "
                f"({fmt_money_signed(comparison['cash_change'], mkt)})"
            )
            hv_line = (
                f"{fmt_money(open_hv, mkt)} → {fmt_money(close_hv, mkt)} "
                f"({fmt_money_signed(comparison['holdings_change'], mkt)})"
            )
            total_title = f"{change_emoji} 총평가 변화 (예수금+보유평가)"
            cash_title = "💵 예수금"
            hv_title = "📈 보유평가"

            fields = [
                {"name": total_title, "value": total_line, "inline": False},
                {"name": cash_title, "value": cash_line, "inline": True},
                {"name": hv_title, "value": hv_line, "inline": True},
            ]

        if (
            abs(primary_return) > 0.01
            or abs(primary_change) > 0
            or open_hv > 0
            or close_hv > 0
        ):
            if not us_kis:
                fields.extend([
                    {
                        "name": "📊 일일 수익률",
                        "value": f"**{daily_return:+.2f}%**",
                        "inline": True,
                    },
                ])
            fields.extend([
                {
                    "name": "💰 실현 손익 (KIS)" if us_kis else "💰 실현 손익",
                    "value": fmt_money_signed(realized_pnl, mkt),
                    "inline": True,
                },
                {
                    "name": "📈 미실현·보유변동" if us_kis else "📈 미실현 손익",
                    "value": (
                        fmt_money_signed(comparison["holdings_change"], mkt)
                        if us_kis
                        else fmt_money_signed(unrealized_pnl, mkt)
                    ),
                    "inline": True,
                },
            ])
            
            if abs(estimated_fees) > 0 and not us_kis:
                fields.append({
                    "name": "💸 추정 수수료",
                    "value": fmt_money_signed(estimated_fees, mkt),
                    "inline": True,
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
        
        # 임베드 전송 — 미국 거래일과 KST 종료일을 명확히 구분해 표시
        if is_us_market(MARKET):
            title = f"📊 {_fmt_date_dash(trade_date)} 당일 매매 성과 (미국 거래일)"
            session_line = (
                f"🇺🇸 미국 거래일: {_fmt_date_dash(trade_date)} | "
                f"🇰🇷 한국 기준 종료일: {_fmt_date_dash(session_close_kst)}"
            )
        else:
            title = f"📊 {_fmt_date_dash(trade_date)} 당일 매매 성과"
            session_line = f"거래일: {_fmt_date_dash(trade_date)}"
        desc = (
            f"{session_line}\n"
            f"⏰ {open_balance['timestamp'][:19]} → {close_balance['timestamp'][:19]}"
        )
        if late_close_capture:
            desc += "\n⚠️ close는 요약 시점 recovery 캡처"
        embed = {
            "type": "rich",
            "title": title,
            "description": desc,
            "fields": fields,
            "color": (
                0x00ff00 if primary_change > 0
                else 0xff0000 if primary_change < 0
                else 0x808080
            ),
            "footer": {
                "text": (
                    f"보유종목: open {open_balance.get('holdings_count', 0)}개 → "
                    f"close {close_balance.get('holdings_count', 0)}개 | "
                    f"open={open_file_name} close={close_file_name}"
                )
            },
        }

        # summary metadata — 날짜 관계·선택된 스냅샷 기록
        summary_meta = {
            "trade_date": trade_date,
            "session_open_date_kst": identity["session_open_date_kst"],
            "session_close_date_kst": session_close_kst,
            "open_snapshot_file": open_file_name,
            "close_snapshot_file": close_file_name,
            "open_snapshot_legacy_alias": open_balance.get("legacy_alias") is True,
            "close_snapshot_canonical": close_balance.get("canonical") is True,
            "open_resolution": pair["open_resolution"],
            "close_resolution": pair["close_resolution"],
            "late_close_capture": late_close_capture,
            "generated_at_kst": datetime.now(KST).isoformat(),
        }
        try:
            meta_path = BALANCE_STORAGE_PATH / f"daily_summary_meta_{trade_date}.json"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(summary_meta, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("daily summary metadata 저장 실패: %s", e)

        if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
            send_discord_message(embeds=[embed])
            logger.info(
                "일일 매매 요약 전송 완료: trade_date=%s (KST 종료일 %s)",
                trade_date,
                session_close_kst,
            )

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
    elif script_name == "performance_review.py":
        cfg = getattr(settings, "_config", None) or {}
        pr_cfg = cfg.get("performance_review") or {}
        period = os.environ.get("PERF_REVIEW_PERIOD") or "monthly"
        args = ["--market", MARKET, "--period", period, "--no-discord"]
        if pr_cfg.get("strict_kis_endpoints", True):
            args.append("--strict-kis-endpoints")

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
    """월 1회 유지보수: performance_review → cleanup."""
    try:
        run_id = datetime.now(KST).strftime("%Y%m%d-%H%M%S")
        os.environ["RUN_ID"] = run_id
        os.environ["RUN_STARTED_AT"] = str(time.time())
        os.environ["PERF_REVIEW_PERIOD"] = "monthly"

        start_msg = "🗓️ 월간 유지보수 시작 (performance_review → cleanup)"
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


def run_weekly_performance_review_if_due():
    """주 1회 performance review (config performance_review.weekly_enabled)."""
    try:
        cfg = getattr(settings, "_config", None) or {}
        pr_cfg = cfg.get("performance_review") or {}
        if not pr_cfg.get("weekly_enabled", False):
            return
        now = datetime.now(KST)
        if now.weekday() != 0:  # Monday
            return
        week_key = now.strftime("%Y-W%W")
        state_path = OUTPUT_DIR / "weekly_performance_review_state.json"
        if state_path.exists():
            try:
                prev = json.loads(state_path.read_text(encoding="utf-8"))
                if prev.get("week") == week_key:
                    return
            except Exception:
                pass
        os.environ["PERF_REVIEW_PERIOD"] = "weekly"
        run_id = now.strftime("%Y%m%d-%H%M%S")
        ok, _, dur = run_script("performance_review.py", run_id)
        state_path.write_text(
            json.dumps({"week": week_key, "ok": ok, "duration": dur}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("[weekly] performance_review ok=%s dur=%.1fs", ok, dur)
    except Exception as e:
        logger.error("주간 performance review 실패: %s", e)


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
    schedule.every().monday.at("09:00").do(run_weekly_performance_review_if_due)
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
        f"(performance_review.py → cleanup_output.py)"
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
    parser.add_argument(
        "--rebuild-daily-balance",
        action="store_true",
        help="기존 daily balance 파일을 덮어쓰고 재생성",
    )
    parser.add_argument(
        "--rebuild-type",
        choices=["open", "close", "both"],
        default="both",
        help="--rebuild-daily-balance 대상",
    )
    parser.add_argument(
        "--trade-date",
        default=None,
        help="대상 미국 거래일 YYYYMMDD (capture/send-summary/migration)",
    )
    parser.add_argument(
        "--migrate-daily-balance-layout",
        action="store_true",
        help="legacy daily balance를 metadata 기준 canonical 레이아웃으로 복사",
    )
    parser.add_argument("--dry-run", action="store_true", help="migration 미리보기")
    parser.add_argument("--apply", action="store_true", help="migration 실제 적용")
    args = parser.parse_args()
    
    print(f"인수 파싱 완료: {args}")
    
    # 백그라운드 RiskManager 인스턴스
    background_risk_manager = None
    
    if args.migrate_daily_balance_layout:
        migrate_daily_balance_layout(
            trade_date=args.trade_date,
            apply=bool(args.apply and not args.dry_run),
        )
    elif args.rebuild_daily_balance:
        types = ["open", "close"] if args.rebuild_type == "both" else [args.rebuild_type]
        for t in types:
            capture_balance_snapshot(t, rebuild=True, trade_date=args.trade_date)
    elif args.capture_open:
        capture_balance_snapshot("open", trade_date=args.trade_date)
    elif args.capture_close:
        capture_balance_snapshot("close", trade_date=args.trade_date)
    elif args.send_summary:
        send_daily_trading_summary(target_trade_date=args.trade_date)
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
