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
import shutil
import subprocess
import time as pytime
import schedule
import time
import threading
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Dict, Tuple, Optional, List
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
) -> Tuple[float, float, float]:
    """(총평가, 예수금, 보유평가).

    US authoritative:
      available_cash = available_cash_usd | ord_psbl_frcr_amt (주문가능 외화)
      holdings = holdings_detail / evlu_amt 합
      total = available_cash + holdings
    금지: usd_cash_total, usd_buy_margin, dnca_tot_amt(외화예수금+증거금),
          polluted tot_evlu_amt_usd (KRW alias)
    """
    hv = _holdings_value_from_rows(holdings)
    if hv <= 0:
        hv = sum(_parse_float_amount(h.get("evlu_amt")) for h in holdings)
        if hv <= 0:
            hv = sum(
                _parse_amount(h.get("hldg_qty")) * _parse_float_amount(h.get("prpr"))
                for h in holdings
            )

    if is_us_market(MARKET):
        from daily_balance_values import (
            detect_legacy_usd_field_pollution,
            normalize_account_values,
        )

        raw = {
            "market": MARKET,
            "currency": "USD",
            "available_cash_usd": cash_map.get("available_cash_usd"),
            "holdings_value_usd": cash_map.get("holdings_value_usd") or hv,
            "cash_map": {"USD": {
                "available_cash": cash_map.get("available_cash_usd")
                or cash_map.get("available_cash"),
                "holdings_value": cash_map.get("holdings_value_usd") or hv,
            }},
            "endpoint_evidence": cash_map.get("endpoint_evidence") or {},
            "kis_summary": {
                "currency": "USD",
                "ord_psbl_frcr_amt": cash_map.get("ord_psbl_frcr_amt")
                or cash_map.get("available_cash"),
                "usd_withdrawable": cash_map.get("usd_withdrawable"),
                "usd_cash_total": cash_map.get("usd_cash_total")
                or cash_map.get("dnca_tot_amt")
                or cash_map.get("frcr_buy_amt"),
                "usd_buy_margin": cash_map.get("usd_buy_margin"),
                "tot_evlu_amt_usd": cash_map.get("tot_evlu_amt_usd")
                or cash_map.get("tot_evlu_amt"),
                "tot_evlu_amt_krw": cash_map.get("tot_evlu_amt_krw"),
                "available_cash_krw": cash_map.get("available_cash_krw"),
                "krw_cash": cash_map.get("krw_cash"),
                "bass_exrt": cash_map.get("bass_exrt"),
            },
            "holdings_detail": _holdings_detail_from_rows(holdings),
            "available_cash_krw": cash_map.get("available_cash_krw"),
            "krw_cash": cash_map.get("krw_cash"),
        }
        # drop polluted tot from being used as total_asset_usd input
        poll = detect_legacy_usd_field_pollution(
            tot_evlu_amt_usd=_parse_float_amount(
                cash_map.get("tot_evlu_amt_usd") or cash_map.get("tot_evlu_amt")
            ),
            available_cash_krw=_parse_float_amount(cash_map.get("available_cash_krw")),
            krw_cash=_parse_float_amount(cash_map.get("krw_cash")),
            total_asset_krw=_parse_float_amount(cash_map.get("tot_evlu_amt_krw")),
            available_cash_usd=_parse_float_amount(
                cash_map.get("available_cash_usd") or cash_map.get("available_cash")
            ),
            holdings_value_usd=float(hv),
            fx_rate=_parse_float_amount(cash_map.get("bass_exrt")),
        )
        if poll:
            logger.warning(
                "LEGACY_USD_FIELD_POLLUTED_BY_KRW: %s — tot_evlu_amt_usd 제외",
                poll[0].get("reasons"),
            )
        norm = normalize_account_values(raw, market=MARKET)
        cash = float(norm.get("available_cash_usd") or 0)
        hv_n = float(norm.get("holdings_value_usd") or hv)
        total = float(norm.get("total_asset_usd") or (cash + hv_n))
        return total, cash, hv_n

    total = _parse_amount(cash_map.get("tot_evlu_amt"))
    cash = _parse_amount(cash_map.get("dnca_tot_amt"))
    if total <= 0:
        total = cash + hv
    return float(total), float(cash), float(hv)


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
            # Mark USD currency on holdings for provenance
            for h in holdings_detail:
                h.setdefault("currency", "USD")
            total_balance, cash, holdings_value = _portfolio_totals_from_cash_map(
                cash_map, holdings
            )
            if holdings_value <= 0 and total_balance > cash:
                holdings_value = total_balance - cash
            kis_summary = _kis_metrics_from_row(cash_map)
            kis_summary["currency"] = "USD"
            source = "kis_account_file_same_date"
            source_snapshot_file = str(balance_path) if balance_path else None
            snapshot_ts = datetime.now(KST).isoformat()
            source_payload_for_copy = None
            if balance_path and Path(balance_path).is_file():
                try:
                    with open(balance_path, "r", encoding="utf-8") as f:
                        source_payload_for_copy = json.load(f)
                except Exception:
                    source_payload_for_copy = None
        else:
            cash_usd = float(kis_payload.get("available_cash_usd") or 0)
            holdings_value = float(kis_payload.get("holdings_value_usd") or 0)
            # Never trust polluted tot_evlu as total — recompute from components
            total_balance = round(cash_usd + holdings_value, 2)
            cash = cash_usd
            tickers = kis_payload.get("tickers") or []
            holdings_detail = [
                {"ticker": t, "qty": None, "value": None, "currency": "USD"}
                for t in tickers
            ]
            sellable = kis_payload.get("sellable_qty_by_ticker") or {}
            if sellable:
                holdings_detail = [
                    {"ticker": t, "qty": sellable.get(t), "value": None, "currency": "USD"}
                    for t in tickers
                ]
            # Prefer cash_map holdings detail if present in payload
            cm = kis_payload.get("cash_map") or {}
            kis_summary = {
                "currency": "USD",
                "ord_psbl_frcr_amt": cash_usd,
                "tot_evlu_amt_usd": kis_payload.get("total_asset_usd"),
                "tot_evlu_amt_krw": float(kis_payload.get("total_asset_krw") or 0),
                "available_cash_krw": float(kis_payload.get("available_cash_krw") or 0),
                "bass_exrt": None,
                "krw_cash": float(kis_payload.get("krw_cash") or 0),
                "usd_withdrawable": cash_usd,
                "usd_cash_total": None,
                "usd_buy_margin": None,
            }
            if isinstance(cm.get("USD"), dict):
                pass
            source = "kis_account_snapshot"
            snapshot_ts = (
                kis_payload.get("snapshot_ts_kst")
                or kis_payload.get("generated_at_kst")
                or datetime.now(KST).isoformat()
            )
            summary_path = None
            balance_path = None
            source_payload_for_copy = kis_payload

        now = datetime.now(KST)
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

        from daily_balance_values import (
            apply_normalized_fields_to_snapshot,
            normalize_account_values,
            save_immutable_source_copy,
            versioned_balance_filename,
        )

        # Immutable source evidence (before mutable balance_YYYYMMDD.json is overwritten)
        src_path = Path(source_snapshot_file) if source_snapshot_file else None
        provenance_meta = save_immutable_source_copy(
            src_path,
            balance_storage=BALANCE_STORAGE_PATH,
            market=MARKET,
            trade_date=trade_date,
            snapshot_type=snapshot_type,
            snapshot_ts_kst=snapshot_ts,
            embedded_payload=source_payload_for_copy,
        )

        us = is_us_market(MARKET)
        raw_for_norm = {
            "market": MARKET,
            "currency": "USD" if us else "KRW",
            "base_currency": "USD" if us else "KRW",
            "available_cash_usd": cash if us else None,
            "holdings_value_usd": holdings_value if us else None,
            "holdings_detail": holdings_detail,
            "kis_summary": kis_summary,
            "available_cash_krw": kis_summary.get("available_cash_krw")
            or kis_summary.get("krw_cash"),
            "krw_cash": kis_summary.get("krw_cash"),
            "cash_map": {
                "USD": {
                    "available_cash": cash if us else None,
                    "holdings_value": holdings_value if us else None,
                }
            },
            "endpoint_evidence": (source_payload_for_copy or {}).get("endpoint_evidence")
            if isinstance(source_payload_for_copy, dict)
            else {},
        }
        if us:
            normalized = normalize_account_values(
                raw_for_norm, market=MARKET, currency_status="explicit"
            )
        else:
            normalized = {
                "schema_version": "2.0",
                "base_currency": "KRW",
                "total_asset_usd": None,
                "available_cash_usd": None,
                "holdings_value_usd": None,
                "total_asset_krw": _parse_amount(total_balance),
                "available_cash_krw": _parse_amount(cash),
                "holdings_value_krw": _parse_amount(holdings_value),
                "krw_cash": _parse_amount(cash),
                "fx_rate_used": None,
                "value_semantics": "krw_total",
                "balance_currency": "KRW",
                "total_balance": total_balance,
                "cash": cash,
                "holdings_value": holdings_value,
                "financial_values_valid": True,
                "return_calculation_usable": True,
                "currency_status": "normalized",
                "usd_components_consistent": True,
                "field_provenance": {},
                "rejected_fields": [],
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
            "holdings_count": len(holdings_detail),
            "holdings_detail": holdings_detail,
            "kis_summary": kis_summary,
            "summary_file": str(summary_path) if summary_path else None,
            "balance_file": str(balance_path) if balance_path else None,
            **provenance_meta,
        }
        snapshot = apply_normalized_fields_to_snapshot(snapshot, normalized)
        # Prefer normalized totals for compatibility fields
        if snapshot.get("total_asset_usd") is not None:
            snapshot["total_balance"] = snapshot["total_asset_usd"]
            snapshot["cash"] = snapshot.get("available_cash_usd")
            snapshot["holdings_value"] = snapshot.get("holdings_value_usd")

        # 신규 canonical + legacy 호환 + versioned immutable session evidence
        canonical_path = _canonical_balance_path(snapshot_type, trade_date)
        legacy_path = _legacy_balance_path(snapshot_type, trade_date)
        versioned_name = versioned_balance_filename(
            MARKET, trade_date, snapshot_type, snapshot_ts
        )
        versioned_path = BALANCE_STORAGE_PATH / "sessions" / versioned_name
        writes: List[Tuple[Path, Dict]] = [
            (canonical_path, {**snapshot, "file_date": snap_date}),
            (legacy_path, {**snapshot, "file_date": snap_date}),
            (versioned_path, {**snapshot, "file_date": snap_date, "immutable_session": True}),
        ]
        for filepath, snap_copy in writes:
            if filepath.exists() and not rebuild and filepath != versioned_path:
                logger.info(
                    "daily balance exists, skip overwrite (use --rebuild-daily-balance): %s",
                    filepath,
                )
                continue
            if filepath == versioned_path and filepath.exists() and not rebuild:
                continue
            filepath.parent.mkdir(parents=True, exist_ok=True)
            tmp = filepath.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(snap_copy, f, ensure_ascii=False, indent=2)
            tmp.replace(filepath)
            logger.info(
                "잔액 스냅샷 저장: %s source=%s trade_date=%s canonical=%s "
                "total_asset_usd=%s financial_valid=%s",
                filepath,
                source,
                trade_date,
                snap_copy.get("canonical"),
                snap_copy.get("total_asset_usd"),
                snap_copy.get("financial_values_valid"),
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

def repair_daily_balance_currency(
    trade_date: str,
    snapshot_type: str = "open",
    *,
    apply: bool = False,
) -> Dict:
    """
    Embedded kis_summary + holdings_detail 로 historical currency repair.

    gates_ok=False 이면 금액을 절대 쓰지 않는다.
    품질 메타(currency_status=ambiguous 등)만 별도로 기록할 수 있다.
    """
    from daily_balance_values import (
        apply_normalized_fields_to_snapshot,
        apply_quality_metadata_only,
        build_ambiguous_quality_metadata,
        propose_currency_repair_from_embedded,
        sha256_file,
    )

    mode = "apply" if apply else "dry_run"
    snap, path, resolution = find_balance_snapshot(trade_date, snapshot_type)

    def _base_result(**extra) -> Dict[str, Any]:
        base = {
            "trade_date": trade_date,
            "snapshot_type": snapshot_type,
            "mode": mode,
            "status": "failed",
            "gates_ok": False,
            "updated": 0,
            "financial_value_updated": False,
            "target_path": str(path) if path else None,
            "selected_target": str(path) if path else None,
            "resolution": resolution,
            "proposed_changes": {},
            "applied_changes": [],
            "rejected_fields": [],
            "unresolved_fields": [],
            "currency_status_before": None,
            "currency_status_after": None,
            "arithmetic_candidate_total_asset_usd": None,
            "candidate_currency_unverified": True,
            "dry_run": not apply,
            "applied": False,
        }
        base.update(extra)
        return base

    if snap is None or path is None:
        result = _base_result(status="target_not_found", error="SNAPSHOT_NOT_FOUND")
        logger.error(
            "[CURRENCY_REPAIR]\nmode=%s\ntrade_date=%s\nsnapshot_type=%s\n"
            "status=target_not_found\ngates_ok=false\nupdated=0",
            mode, trade_date, snapshot_type,
        )
        return result

    currency_before = snap.get("currency_status")
    sha_before = sha256_file(path)

    # Already repaired & usable → idempotent
    if (
        snap.get("financial_values_valid") is True
        and snap.get("return_calculation_usable") is True
        and snap.get("currency_status") in ("reconstructed", "normalized", "explicit")
        and abs(
            _parse_float_amount(snap.get("total_asset_usd"))
            - _parse_float_amount(snap.get("available_cash_usd"))
            - _parse_float_amount(snap.get("holdings_value_usd"))
        )
        <= 0.02
    ):
        result = _base_result(
            status="already_valid",
            gates_ok=True,
            currency_status_before=currency_before,
            currency_status_after=snap.get("currency_status"),
            arithmetic_candidate_total_asset_usd=snap.get("total_asset_usd"),
            candidate_currency_unverified=False,
            proposed_changes={},
            target_sha256_before=sha_before,
            target_sha256_after=sha_before,
        )
        logger.info(
            "[CURRENCY_REPAIR]\nmode=%s\ntrade_date=%s\nsnapshot_type=%s\n"
            "status=already_valid\ngates_ok=true\nupdated=0\n"
            "total_asset_usd=%s",
            mode, trade_date, snapshot_type, snap.get("total_asset_usd"),
        )
        return result

    proposal = propose_currency_repair_from_embedded(snap, market=MARKET)
    norm = proposal["normalized"]
    rejected = proposal.get("rejected_fields") or []
    rejected_reasons = [
        str(r.get("reason") or r) if isinstance(r, dict) else str(r)
        for r in rejected
    ]
    # unique preserve order
    rejected_reasons = list(dict.fromkeys(rejected_reasons))
    arith = proposal.get("arithmetic_candidate_total_asset_usd")
    gates_ok = bool(proposal.get("gates_ok"))

    proposed_changes: Dict[str, Any] = {}
    if gates_ok:
        proposed_changes = {
            "total_asset_usd": proposal.get("proposed_total_asset_usd"),
            "available_cash_usd": proposal.get("proposed_available_cash_usd"),
            "holdings_value_usd": proposal.get("proposed_holdings_value_usd"),
            "currency_status": "reconstructed",
            "financial_values_valid": True,
            "return_calculation_usable": True,
        }

    result = _base_result(
        gates_ok=gates_ok,
        currency_status_before=currency_before,
        currency_status_after=currency_before,
        rejected_fields=rejected_reasons,
        unresolved_fields=rejected_reasons if not gates_ok else [],
        arithmetic_candidate_total_asset_usd=arith,
        candidate_currency_unverified=not gates_ok,
        proposed_changes=proposed_changes,
        # back-compat: only when gates_ok (confirmed proposal)
        proposed_total_asset_usd=proposal.get("proposed_total_asset_usd") if gates_ok else None,
        proposed_available_cash_usd=proposal.get("proposed_available_cash_usd") if gates_ok else None,
        proposed_holdings_value_usd=proposal.get("proposed_holdings_value_usd") if gates_ok else None,
        gates=proposal.get("gates"),
        embedded_evidence={
            "ord_psbl_frcr_amt": (snap.get("kis_summary") or {}).get("ord_psbl_frcr_amt"),
            "usd_withdrawable": (snap.get("kis_summary") or {}).get("usd_withdrawable"),
            "usd_buy_margin": (snap.get("kis_summary") or {}).get("usd_buy_margin"),
            "usd_cash_total": (snap.get("kis_summary") or {}).get("usd_cash_total"),
            "tot_evlu_amt_usd": (snap.get("kis_summary") or {}).get("tot_evlu_amt_usd"),
            "holdings_detail_sum": sum(
                _parse_float_amount(h.get("value"))
                for h in (snap.get("holdings_detail") or [])
                if isinstance(h, dict)
            ),
        },
        field_provenance=norm.get("field_provenance") if gates_ok else {},
        target_sha256_before=sha_before,
        target_sha256_after=sha_before,
        note=(
            "cash와 holdings의 산술 합계이나 USD 및 source integrity가 "
            "확인되지 않아 적용하지 않음"
            if not gates_ok and arith is not None
            else None
        ),
    )

    def _write_evidence(payload: Dict) -> str:
        tag = "apply" if apply else "dry_run"
        ep = (
            BALANCE_STORAGE_PATH / "evidence"
            / f"daily_balance_currency_repair_{trade_date}_{snapshot_type}_{tag}.json"
        )
        ep.parent.mkdir(parents=True, exist_ok=True)
        with open(ep, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return str(ep)

    # ── gates false: never write amount fields ──
    if not gates_ok:
        result["status"] = "evidence_insufficient"
        result["updated"] = 0
        result["financial_value_updated"] = False
        result["applied_changes"] = []
        logger.info(
            "[CURRENCY_REPAIR]\n"
            "mode=%s\n"
            "trade_date=%s\n"
            "snapshot_type=%s\n"
            "status=evidence_insufficient\n"
            "gates_ok=false\n"
            "updated=0\n"
            "arithmetic_candidate_total_asset_usd=%s\n"
            "candidate_currency_unverified=true\n"
            "rejected=%s",
            mode,
            trade_date,
            snapshot_type,
            arith,
            ",".join(rejected_reasons),
        )

        # Quality metadata only (금액 필드 불변). updated는 금액 기준이므로 항상 0.
        if apply:
            quality = build_ambiguous_quality_metadata(rejected_reasons)
            already = (
                snap.get("currency_status") == "ambiguous"
                and snap.get("financial_values_valid") is False
                and snap.get("return_calculation_usable") is False
            )
            amount_keys = ("total_balance", "cash", "holdings_value", "holdings_detail", "kis_summary")
            amounts_before = {k: snap.get(k) for k in amount_keys}

            if already:
                result["status"] = "evidence_insufficient"
                result["currency_status_after"] = "ambiguous"
                result["target_sha256_after"] = sha_before
            else:
                targets = [path]
                canon = _canonical_balance_path(snapshot_type, trade_date)
                if canon.exists() and canon.resolve() != path.resolve():
                    targets.append(canon)
                legacy_twin = _legacy_balance_path(snapshot_type, trade_date)
                if legacy_twin.exists() and legacy_twin.resolve() != path.resolve():
                    twin = _load_balance_json(legacy_twin)
                    if twin and _snapshot_trade_date(twin, legacy_twin) == trade_date:
                        targets.append(legacy_twin)

                for tpath in targets:
                    cur = _load_balance_json(tpath) or snap
                    amt = {k: cur.get(k) for k in amount_keys}
                    merged = apply_quality_metadata_only(cur, quality)
                    for k, v in amt.items():
                        merged[k] = v
                    tmp = tpath.with_suffix(".tmp")
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(merged, f, ensure_ascii=False, indent=2)
                    tmp.replace(tpath)

                after_snap = _load_balance_json(path) or {}
                amounts_unchanged = (
                    after_snap.get("total_balance") == amounts_before.get("total_balance")
                    and after_snap.get("cash") == amounts_before.get("cash")
                    and after_snap.get("holdings_value") == amounts_before.get("holdings_value")
                )
                result["status"] = "quality_metadata_updated"
                result["updated"] = 0  # amount fields never updated
                result["financial_value_updated"] = False
                result["currency_status_after"] = "ambiguous"
                result["applied_changes"] = ["quality_metadata"]
                result["target_sha256_after"] = sha256_file(path)
                result["amounts_unchanged"] = amounts_unchanged
                logger.info(
                    "[CURRENCY_REPAIR]\nmode=apply\ntrade_date=%s\nsnapshot_type=%s\n"
                    "status=quality_metadata_updated\ngates_ok=false\n"
                    "updated=0\nfinancial_value_updated=false\n"
                    "currency_status_after=ambiguous\n"
                    "rejected=%s",
                    trade_date, snapshot_type, ",".join(rejected_reasons),
                )

        result["evidence_path"] = _write_evidence(result)
        return result

    # ── gates ok ──
    if not apply:
        result["status"] = "dry_run_proposal"
        result["proposed_changes"] = proposed_changes
        logger.info(
            "[CURRENCY_REPAIR]\nmode=dry_run\ntrade_date=%s\nsnapshot_type=%s\n"
            "status=dry_run_proposal\ngates_ok=true\nupdated=0\n"
            "proposed_total_asset_usd=%s",
            trade_date, snapshot_type, proposed_changes.get("total_asset_usd"),
        )
        result["evidence_path"] = _write_evidence(result)
        return result

    # apply amount repair
    repaired = apply_normalized_fields_to_snapshot(snap, norm)
    repaired["currency_repaired_at_kst"] = datetime.now(KST).isoformat()
    repaired["currency_repair_source"] = "embedded_kis_summary_holdings_detail"

    backup_dir = BALANCE_STORAGE_PATH / "evidence" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{path.name}.before_currency_repair"
    shutil.copy2(path, backup_path)

    targets = [path]
    legacy_twin = _legacy_balance_path(snapshot_type, trade_date)
    if legacy_twin.exists() and legacy_twin.resolve() != path.resolve():
        twin = _load_balance_json(legacy_twin)
        if twin and _snapshot_trade_date(twin, legacy_twin) == trade_date:
            targets.append(legacy_twin)
    canon = _canonical_balance_path(snapshot_type, trade_date)
    if canon.exists() and canon.resolve() != path.resolve():
        targets.append(canon)
    for p in BALANCE_STORAGE_PATH.glob(f"balance_{snapshot_type}_*.json"):
        if p.resolve() in {t.resolve() for t in targets}:
            continue
        pl = _load_balance_json(p)
        if not pl:
            continue
        if _snapshot_trade_date(pl, p) == trade_date and str(pl.get("type") or snapshot_type) == snapshot_type:
            targets.append(p)

    updated = 0
    for tpath in targets:
        tmp = tpath.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({**repaired, "file_date": repaired.get("file_date")}, f, ensure_ascii=False, indent=2)
        tmp.replace(tpath)
        updated += 1

    sha_after = sha256_file(path)
    result.update({
        "status": "applied",
        "updated": updated,
        "financial_value_updated": True,
        "applied": True,
        "applied_changes": list(proposed_changes.keys()),
        "currency_status_after": "reconstructed",
        "candidate_currency_unverified": False,
        "backup_path": str(backup_path),
        "target_sha256_after": sha_after,
    })
    logger.info(
        "[CURRENCY_REPAIR]\nmode=apply\ntrade_date=%s\nsnapshot_type=%s\n"
        "status=applied\ngates_ok=true\nupdated=%s\n"
        "total_asset_usd=%s",
        trade_date, snapshot_type, updated, repaired.get("total_asset_usd"),
    )
    result["evidence_path"] = _write_evidence(result)
    return result


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

def _is_finite_number(val: Any) -> bool:
    """True iff val is a real int/float (not bool, not None, not NaN)."""
    if isinstance(val, bool) or val is None:
        return False
    if not isinstance(val, (int, float)):
        return False
    return val == val


def _nullable_positive_fx(val: Any) -> Optional[float]:
    """유효 양수 환율만 반환. None/0/음수/비숫자는 None — 0으로 치환하지 않음."""
    if val is None or val == "":
        return None
    try:
        rate = float(val)
    except (TypeError, ValueError):
        return None
    if rate != rate or rate <= 0:
        return None
    return rate


def _extract_fx_rate(snap: Dict, metrics: Dict) -> Optional[float]:
    """스냅샷 embedded kis_summary의 bass_exrt를 우선 (명시 null 보존)."""
    embedded = snap.get("kis_summary") if isinstance(snap.get("kis_summary"), dict) else None
    if isinstance(embedded, dict) and "bass_exrt" in embedded:
        return _nullable_positive_fx(embedded.get("bass_exrt"))
    return _nullable_positive_fx(metrics.get("bass_exrt"))


def _extract_optional_krw_amount(snap: Dict, metrics: Dict, key: str) -> Optional[float]:
    """선택 KRW 금액 — 키 존재·None 구분. 없으면 metrics 양수/0 허용, 파싱 실패는 None."""
    embedded = snap.get("kis_summary") if isinstance(snap.get("kis_summary"), dict) else None
    if isinstance(embedded, dict) and key in embedded:
        raw = embedded.get(key)
        if raw is None or raw == "":
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    raw_m = metrics.get(key)
    if raw_m is None:
        return None
    try:
        return float(raw_m)
    except (TypeError, ValueError):
        return None


def _canonical_usd_components(snap: Dict) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    """canonical USD 필드만 사용. 누락 시 0 치환 금지."""
    for key in ("total_asset_usd", "available_cash_usd", "holdings_value_usd"):
        if snap.get(key) is None:
            return None, None, None, f"missing_{key}"
    try:
        total = float(snap["total_asset_usd"])
        cash = float(snap["available_cash_usd"])
        hv = float(snap["holdings_value_usd"])
    except (TypeError, ValueError) as e:
        return None, None, None, f"non_numeric_usd_component:{e}"
    if not (_is_finite_number(total) and _is_finite_number(cash) and _is_finite_number(hv)):
        return None, None, None, "non_finite_usd_component"
    return total, cash, hv, None


def _compare_balances_failure(
    exc: BaseException,
    *,
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """예외 시 빈 dict 대신 구조화된 PARTIAL 실패 결과."""
    return {
        "date": trade_date,
        "analysis_success": False,
        "status": "PARTIAL",
        "status_code": "DAILY_BALANCE_ANALYSIS_INCOMPLETE",
        "return_metrics_available": False,
        "asset_change_metrics_available": False,
        "fx_metrics_available": False,
        "fx_calculation_status": "FX_RATE_INCOMPLETE",
        "total_change": None,
        "cash_change": None,
        "holdings_change": None,
        "daily_return_pct": None,
        "daily_return_usd_pct": None,
        "usd_change": None,
        "analysis_error": {
            "code": "DAILY_BALANCE_ANALYSIS_ERROR",
            "type": type(exc).__name__,
            "message": str(exc),
        },
    }


def _optional_fx_krw_analysis(
    open_balance: Dict,
    close_balance: Dict,
    om: Dict,
    cm: Dict,
    *,
    open_usd_total: float,
    close_usd_total: float,
    usd_change: float,
    trade_date: Optional[str],
) -> Dict[str, Any]:
    """선택적 FX/KRW 분석 — 실패해도 핵심 USD 결과를 무효화하지 않음."""
    open_fx = _extract_fx_rate(open_balance, om)
    close_fx = _extract_fx_rate(close_balance, cm)
    open_krw_cash = _extract_optional_krw_amount(open_balance, om, "krw_cash")
    close_krw_cash = _extract_optional_krw_amount(close_balance, cm, "krw_cash")
    open_krw_total = _extract_optional_krw_amount(open_balance, om, "tot_evlu_amt_krw")
    close_krw_total = _extract_optional_krw_amount(close_balance, cm, "tot_evlu_amt_krw")

    fx_out: Dict[str, Any] = {
        "open_fx": open_fx,
        "close_fx": close_fx,
        "open_krw_cash": open_krw_cash,
        "close_krw_cash": close_krw_cash,
        "open_krw_total": open_krw_total,
        "close_krw_total": close_krw_total,
        "krw_total_change": (
            round(close_krw_total - open_krw_total, 2)
            if _is_finite_number(open_krw_total) and _is_finite_number(close_krw_total)
            else None
        ),
        "trading_pnl_krw": None,
        "fx_impact_krw": None,
        "fx_metrics_available": False,
        "fx_calculation_status": "FX_RATE_INCOMPLETE",
        "open_cash_detail": om,
        "close_cash_detail": cm,
    }

    if open_fx is None or close_fx is None:
        logger.warning(
            "[DAILY_BALANCE_FX_RATE_INCOMPLETE]\n"
            "market=%s\n"
            "trade_date=%s\n"
            "open_fx=%s\n"
            "close_fx=%s\n"
            "fx_metrics_available=false\n"
            "core_usd_comparison_available=true",
            MARKET,
            trade_date,
            open_fx,
            close_fx,
        )
        return fx_out

    # 양쪽 환율이 유효한 양수일 때만 FX 산술 수행 (None→0 금지)
    try:
        ok_cash = _is_finite_number(open_krw_cash) and _is_finite_number(close_krw_cash)
        krw_cash_delta = (close_krw_cash - open_krw_cash) if ok_cash else 0.0
        trading_pnl_krw = int(round(usd_change * open_fx + krw_cash_delta))
        fx_impact_krw = None
        if _is_finite_number(close_krw_total) and _is_finite_number(close_krw_cash):
            close_at_open_fx = int(round(close_usd_total * open_fx + close_krw_cash))
            fx_impact_krw = int(close_krw_total) - close_at_open_fx
        fx_out.update({
            "trading_pnl_krw": trading_pnl_krw,
            "fx_impact_krw": fx_impact_krw,
            "fx_metrics_available": True,
            "fx_calculation_status": "COMPLETE",
        })
    except Exception as fx_exc:
        logger.warning(
            "[DAILY_BALANCE_FX_RATE_INCOMPLETE] FX 산술 실패 trade_date=%s: %s: %s",
            trade_date,
            type(fx_exc).__name__,
            fx_exc,
        )
        fx_out["fx_calculation_status"] = "FX_RATE_INCOMPLETE"
        fx_out["fx_metrics_available"] = False
        fx_out["trading_pnl_krw"] = None
        fx_out["fx_impact_krw"] = None
    return fx_out


def compare_balances(open_balance: Dict, close_balance: Dict) -> Dict:
    """장시작/종료 잔액 비교.

    A) 핵심 USD(또는 KR 총평가) 비교 — 환율과 독립
    B) 선택적 FX/KRW 분석 — 실패해도 핵심 결과를 유지
    """
    trade_date = (
        (close_balance or {}).get("trade_date")
        or (close_balance or {}).get("date")
        or (open_balance or {}).get("trade_date")
    )
    try:
        us_kis = is_us_market(MARKET)
        om: Dict = {}
        cm: Dict = {}

        # ── A. 핵심 자산 비교 ──
        if us_kis:
            open_total, open_cash, open_hv, open_err = _canonical_usd_components(open_balance)
            close_total, close_cash, close_hv, close_err = _canonical_usd_components(close_balance)
            if open_err or close_err or not all(
                _is_finite_number(v)
                for v in (open_total, open_cash, open_hv, close_total, close_cash, close_hv)
            ):
                err = open_err or close_err or "core_usd_components_incomplete"
                return {
                    **_compare_balances_failure(
                        ValueError(err), trade_date=trade_date
                    ),
                    "analysis_error": {
                        "code": "DAILY_BALANCE_ANALYSIS_ERROR",
                        "type": "ValueError",
                        "message": err,
                    },
                    "us_kis_summary": True,
                }

            open_usd_total = round(float(open_total), 2)
            close_usd_total = round(float(close_total), 2)
            total_change = round(close_usd_total - open_usd_total, 2)
            cash_change = round(float(close_cash) - float(open_cash), 2)
            holdings_change = round(float(close_hv) - float(open_hv), 2)
            daily_return_pct = (
                (total_change / open_usd_total) * 100 if open_usd_total > 0 else 0.0
            )
            daily_return_usd_pct = daily_return_pct
            usd_change = total_change
            om = _kis_metrics_from_snapshot(open_balance)
            cm = _kis_metrics_from_snapshot(close_balance)
        else:
            open_total, open_cash, open_hv = _portfolio_totals_from_snapshot(open_balance)
            close_total, close_cash, close_hv = _portfolio_totals_from_snapshot(close_balance)
            if not all(
                _is_finite_number(v)
                for v in (open_total, open_cash, open_hv, close_total, close_cash, close_hv)
            ):
                return _compare_balances_failure(
                    ValueError("core_portfolio_components_incomplete"),
                    trade_date=trade_date,
                )
            total_change = close_total - open_total
            cash_change = close_cash - open_cash
            holdings_change = close_hv - open_hv
            daily_return_pct = (
                (total_change / open_total) * 100 if open_total > 0 else 0.0
            )
            daily_return_usd_pct = 0.0
            usd_change = 0.0
            open_usd_total = close_usd_total = 0.0

        # 보유종목 변화 분석 (None-safe)
        open_detail = open_balance.get("holdings_detail") or []
        close_detail = close_balance.get("holdings_detail") or []
        open_tickers = {
            h["ticker"] for h in open_detail if isinstance(h, dict) and h.get("ticker")
        }
        close_tickers = {
            h["ticker"] for h in close_detail if isinstance(h, dict) and h.get("ticker")
        }
        sold_tickers = open_tickers - close_tickers
        bought_tickers = close_tickers - open_tickers
        held_tickers = open_tickers & close_tickers

        realized_pnl = None
        realized_meta: Dict[str, Any] = {}
        if us_kis:
            from daily_balance_values import resolve_realized_pnl_delta

            resolved = resolve_realized_pnl_delta(
                open_balance, close_balance, expected_trade_date=trade_date
            )
            if resolved.get("available"):
                realized_pnl = resolved.get("value")
                realized_meta = {
                    "realized_pnl_available": True,
                    "realized_pnl_status": "OK",
                    "realized_pnl_currency": resolved.get("currency"),
                    "realized_pnl_source": resolved.get("source"),
                }
            else:
                realized_meta = {
                    "realized_pnl_available": False,
                    "realized_pnl_status": resolved.get("status") or "EVIDENCE_INCOMPLETE",
                    "realized_pnl_currency": None,
                    "realized_pnl_source": None,
                    "realized_pnl_error_reasons": list(resolved.get("error_reasons") or []),
                }
        else:
            realized_pnl = 0.0
            for ticker in sold_tickers:
                open_holding = next(
                    (
                        h for h in open_detail
                        if isinstance(h, dict) and h.get("ticker") == ticker
                    ),
                    None,
                )
                if open_holding is not None and _is_finite_number(open_holding.get("value")):
                    realized_pnl += float(open_holding["value"])
            realized_meta = {
                "realized_pnl_available": True,
                "realized_pnl_status": "OK",
                "realized_pnl_currency": None,
                "realized_pnl_source": "sold_holdings_value",
            }

        unrealized_pnl = 0.0
        for ticker in held_tickers:
            open_holding = next(
                (h for h in open_detail if isinstance(h, dict) and h.get("ticker") == ticker),
                None,
            )
            close_holding = next(
                (h for h in close_detail if isinstance(h, dict) and h.get("ticker") == ticker),
                None,
            )
            if (
                open_holding
                and close_holding
                and _is_finite_number(open_holding.get("value"))
                and _is_finite_number(close_holding.get("value"))
            ):
                unrealized_pnl += float(close_holding["value"]) - float(open_holding["value"])

        if realized_pnl is not None and _is_finite_number(total_change):
            estimated_fees = float(total_change) - float(realized_pnl) - unrealized_pnl
        else:
            estimated_fees = None

        result: Dict[str, Any] = {
            "date": trade_date,
            "analysis_success": True,
            "status": "OK",
            "status_code": "DAILY_BALANCE_COMPARISON_COMPLETE",
            "return_metrics_available": True,
            "asset_change_metrics_available": True,
            "analysis_error": None,
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
            "open_balance": open_usd_total if us_kis else open_total,
            "close_balance": close_usd_total if us_kis else close_total,
            "open_cash": open_cash,
            "close_cash": close_cash,
            "open_holdings_value": open_hv,
            "close_holdings_value": close_hv,
            "open_usd_total": open_usd_total if us_kis else open_total,
            "close_usd_total": close_usd_total if us_kis else close_total,
            "us_kis_summary": us_kis,
            "fx_metrics_available": False if us_kis else True,
            "fx_calculation_status": "FX_RATE_INCOMPLETE" if us_kis else "N/A",
        }
        result.update(realized_meta)

        # ── B. 선택적 FX/KRW 분석 (US만) ──
        if us_kis:
            try:
                fx_part = _optional_fx_krw_analysis(
                    open_balance,
                    close_balance,
                    om,
                    cm,
                    open_usd_total=open_usd_total,
                    close_usd_total=close_usd_total,
                    usd_change=usd_change,
                    trade_date=trade_date,
                )
                result.update(fx_part)
            except Exception as fx_exc:
                logger.warning(
                    "[DAILY_BALANCE_FX_RATE_INCOMPLETE] optional FX block failed "
                    "trade_date=%s: %s: %s",
                    trade_date,
                    type(fx_exc).__name__,
                    fx_exc,
                )
                result.update({
                    "open_fx": None,
                    "close_fx": None,
                    "trading_pnl_krw": None,
                    "fx_impact_krw": None,
                    "open_krw_cash": None,
                    "close_krw_cash": None,
                    "fx_metrics_available": False,
                    "fx_calculation_status": "FX_RATE_INCOMPLETE",
                    "open_cash_detail": om,
                    "close_cash_detail": cm,
                })

        logger.info(
            "[DAILY_BALANCE_COMPARISON_COMPLETE]\n"
            "trade_date=%s\n"
            "total_change=%s\n"
            "cash_change=%s\n"
            "holdings_change=%s\n"
            "total_change_pct=%s",
            trade_date,
            result["total_change"],
            result["cash_change"],
            result["holdings_change"],
            result["daily_return_pct"],
        )
        return result

    except Exception as e:
        logger.error(
            "잔액 비교 분석 실패: %s: %s",
            type(e).__name__,
            e,
        )
        logger.debug("잔액 비교 분석 상세 오류:", exc_info=True)
        return _compare_balances_failure(e, trade_date=trade_date)

def _resolve_usd_comparison_values(snap: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """
    US 일일 요약 비교용 USD 자산값 — (values, error_reason).

    return_calculation_usable=True 이고 financial_values_valid=True 인
    explicit USD 구성요소만 우선 허용.

    Legacy 폴백(embedded normalize)은 source integrity gate를 통과할 때만.
    SOURCE_SNAPSHOT_SHA_UNKNOWN / MUTATED 등이면 PARTIAL (산술 금지).
    """
    from daily_balance_values import (
        is_return_calculation_usable,
        propose_currency_repair_from_embedded,
    )

    if is_return_calculation_usable(snap):
        return (
            {
                "total": _parse_float_amount(snap.get("total_asset_usd")),
                "cash": _parse_float_amount(snap.get("available_cash_usd")),
                "hv": _parse_float_amount(snap.get("holdings_value_usd")),
                "semantics": snap.get("currency_status") or "explicit_usd",
            },
            None,
        )

    # Explicitly marked unusable / ambiguous — do not invent USD totals
    if snap.get("financial_values_valid") is False or snap.get("return_calculation_usable") is False:
        reasons = snap.get("normalization_errors") or []
        return None, (
            f"currency_status={snap.get('currency_status')} "
            f"rejected={list(reasons)[:4]} — DAILY_BALANCE_CURRENCY_MISMATCH"
        )
    if snap.get("currency_status") == "ambiguous":
        return None, "currency_status=ambiguous — DAILY_BALANCE_CURRENCY_MISMATCH"

    # Legacy: embedded normalize only when repair gates would allow amount use
    try:
        prop = propose_currency_repair_from_embedded(snap, market=MARKET)
        if prop.get("integrity_blocked") or not prop.get("gates_ok"):
            reasons = prop.get("rejected_reasons") or [
                r.get("reason") for r in (prop.get("rejected_fields") or [])
            ]
            return None, (
                f"currency_status={prop.get('normalized', {}).get('currency_status')} "
                f"rejected={list(reasons)[:4]} — DAILY_BALANCE_CURRENCY_MISMATCH"
            )
        norm = prop["normalized"]
        if norm.get("return_calculation_usable"):
            return (
                {
                    "total": norm["total_asset_usd"],
                    "cash": norm["available_cash_usd"],
                    "hv": norm["holdings_value_usd"],
                    "semantics": "normalized_embedded",
                },
                None,
            )
        reasons = [r.get("reason") for r in (norm.get("rejected_fields") or [])]
        return None, (
            f"currency_status={norm.get('currency_status')} "
            f"rejected={reasons[:3]} — DAILY_BALANCE_CURRENCY_MISMATCH"
        )
    except Exception as e:
        return None, f"normalize_failed: {e}"


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


PARTIAL_OMITTED_METRICS = [
    "total_change",
    "total_change_pct",
    "cash_change",
    "holdings_change",
    "daily_asset_pnl",
    "investment_return_pct",
    "asset_value_change",
]


def _holdings_ticker_sets(open_balance: Dict, close_balance: Dict) -> Dict[str, List[str]]:
    open_detail = open_balance.get("holdings_detail") or []
    close_detail = close_balance.get("holdings_detail") or []
    open_tickers = {h["ticker"] for h in open_detail if isinstance(h, dict) and h.get("ticker")}
    close_tickers = {h["ticker"] for h in close_detail if isinstance(h, dict) and h.get("ticker")}
    return {
        "sold_tickers": list(open_tickers - close_tickers),
        "bought_tickers": list(close_tickers - open_tickers),
        "held_tickers": list(open_tickers & close_tickers),
    }


def _realized_gross_pnl_from_pair(
    open_balance: Dict,
    close_balance: Dict,
    *,
    expected_trade_date: Optional[str] = None,
) -> Optional[float]:
    """Backward-compat wrapper — prefer resolve_realized_pnl_delta for new callers."""
    from daily_balance_values import resolve_realized_pnl_delta

    if not is_us_market(MARKET):
        return None
    resolved = resolve_realized_pnl_delta(
        open_balance,
        close_balance,
        expected_trade_date=expected_trade_date,
    )
    if resolved.get("available"):
        logger.info(
            "[DAILY_REALIZED_PNL_RESOLVED]\ntrade_date=%s\nvalue=%s\ncurrency=%s\nsource=%s",
            resolved.get("trade_date"),
            resolved.get("value"),
            resolved.get("currency"),
            resolved.get("source"),
        )
        return resolved.get("value")
    logger.info(
        "[DAILY_REALIZED_PNL_UNAVAILABLE]\ntrade_date=%s\nstatus=%s\nreasons=%s",
        expected_trade_date
        or (open_balance or {}).get("trade_date")
        or (close_balance or {}).get("trade_date"),
        resolved.get("status"),
        ",".join(resolved.get("error_reasons") or []),
    )
    return None


def _attach_realized_pnl_fields(
    target: Dict[str, Any],
    open_balance: Dict,
    close_balance: Dict,
    *,
    expected_trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Attach independent realized PnL fields (None when evidence incomplete)."""
    from daily_balance_values import resolve_realized_pnl_delta

    if not is_us_market(MARKET):
        target["realized_pnl"] = None
        target["realized_pnl_available"] = False
        target["realized_pnl_status"] = "NOT_US_MARKET"
        target["realized_pnl_currency"] = None
        target["realized_pnl_source"] = None
        return target

    resolved = resolve_realized_pnl_delta(
        open_balance,
        close_balance,
        expected_trade_date=expected_trade_date,
    )
    if resolved.get("available"):
        logger.info(
            "[DAILY_REALIZED_PNL_RESOLVED]\ntrade_date=%s\nvalue=%s\ncurrency=%s\nsource=%s",
            resolved.get("trade_date"),
            resolved.get("value"),
            resolved.get("currency"),
            resolved.get("source"),
        )
        target["realized_pnl"] = resolved.get("value")
        target["realized_pnl_available"] = True
        target["realized_pnl_status"] = "OK"
        target["realized_pnl_currency"] = resolved.get("currency")
        target["realized_pnl_source"] = resolved.get("source")
    else:
        logger.info(
            "[DAILY_REALIZED_PNL_UNAVAILABLE]\ntrade_date=%s\nstatus=%s\nreasons=%s",
            expected_trade_date
            or (open_balance or {}).get("trade_date")
            or (close_balance or {}).get("trade_date"),
            resolved.get("status"),
            ",".join(resolved.get("error_reasons") or []),
        )
        target["realized_pnl"] = None
        target["realized_pnl_available"] = False
        target["realized_pnl_status"] = resolved.get("status") or "EVIDENCE_INCOMPLETE"
        target["realized_pnl_currency"] = None
        target["realized_pnl_source"] = None
        target["realized_pnl_error_reasons"] = list(resolved.get("error_reasons") or [])
    return target

def _currency_reject_reasons(snap: Dict) -> List[str]:
    errs = snap.get("normalization_errors")
    if isinstance(errs, list) and errs:
        return [str(e) for e in errs]
    rejected = snap.get("rejected_fields") or []
    out: List[str] = []
    for r in rejected:
        if isinstance(r, dict):
            out.append(str(r.get("reason") or r))
        else:
            out.append(str(r))
    if out:
        return list(dict.fromkeys(out))
    try:
        from daily_balance_values import propose_currency_repair_from_embedded
        prop = propose_currency_repair_from_embedded(snap, market=MARKET)
        return list(dict.fromkeys(prop.get("rejected_reasons") or []))
    except Exception:
        return ["CURRENCY_AMBIGUOUS"]


def _snapshot_quality_label(
    snap: Dict,
    vals: Optional[Dict],
    err: Optional[str],
) -> str:
    if vals is not None and snap.get("financial_values_valid") is not False:
        return "valid"
    if (
        snap.get("currency_status") == "ambiguous"
        or snap.get("financial_values_valid") is False
        or snap.get("return_calculation_usable") is False
        or err
    ):
        return "ambiguous"
    return "unknown"


def _has_external_cash_flow_evidence(open_balance: Dict, close_balance: Dict) -> bool:
    """입출금·환전 등 외부 현금흐름 증거가 있을 때만 투자수익률 확정."""
    for snap in (open_balance, close_balance):
        if not isinstance(snap, dict):
            continue
        if snap.get("external_cash_flow_evidence") or snap.get("cash_flow_events"):
            return True
        ks = snap.get("kis_summary") or {}
        if ks.get("external_cash_flow_evidence") or ks.get("cash_flow_events"):
            return True
        if ks.get("deposit_withdraw_usd") is not None or ks.get("fx_conversion_usd") is not None:
            return True
    return False


def pair_supports_complete_summary(
    open_balance: Dict,
    close_balance: Dict,
    *,
    open_vals: Optional[Dict] = None,
    close_vals: Optional[Dict] = None,
) -> bool:
    """COMPLETE summary 전환 조건 (자산 증감 산술 가능)."""
    if not open_balance or not close_balance:
        return False
    open_td = str(open_balance.get("trade_date") or "")
    close_td = str(close_balance.get("trade_date") or "")
    if not open_td or open_td != close_td:
        return False
    if str(open_balance.get("base_currency") or "").upper() != "USD":
        return False
    if str(close_balance.get("base_currency") or "").upper() != "USD":
        return False
    if open_balance.get("financial_values_valid") is not True:
        return False
    if close_balance.get("financial_values_valid") is not True:
        return False
    if open_balance.get("return_calculation_usable") is not True:
        return False
    if close_balance.get("return_calculation_usable") is not True:
        return False
    if open_vals is None or close_vals is None:
        return False
    try:
        ot = float(open_vals["total"])
        ct = float(close_vals["total"])
    except (TypeError, ValueError, KeyError):
        return False
    if ot != ot or ct != ct:  # NaN
        return False
    # component consistency
    for vals in (open_vals, close_vals):
        try:
            tot = float(vals["total"])
            cash = float(vals["cash"])
            hv = float(vals["hv"])
        except (TypeError, ValueError, KeyError):
            return False
        if abs(tot - cash - hv) > 0.02:
            return False
    return True


def build_partial_daily_summary(
    open_balance: Dict,
    close_balance: Dict,
    trade_date: str,
    *,
    open_vals: Optional[Dict] = None,
    close_vals: Optional[Dict] = None,
    open_err: Optional[str] = None,
    close_err: Optional[str] = None,
) -> Dict[str, Any]:
    """
    open valid + close ambiguous 등에서 수익률 산술을 수행하지 않는 고정 schema.
    금지: close_total - open_total, None - float, float(None), 0으로 위장.
    """
    tickers = _holdings_ticker_sets(open_balance, close_balance)
    findings: List[Dict[str, Any]] = []
    if close_err or close_vals is None:
        findings.append({
            "code": "DAILY_BALANCE_CURRENCY_AMBIGUOUS",
            "severity": "WARNING",
            "snapshot_type": "close",
            "reasons": _currency_reject_reasons(close_balance) or ["CURRENCY_AMBIGUOUS"],
        })
    if open_err or open_vals is None:
        findings.append({
            "code": "DAILY_BALANCE_CURRENCY_AMBIGUOUS",
            "severity": "WARNING",
            "snapshot_type": "open",
            "reasons": _currency_reject_reasons(open_balance) or ["CURRENCY_AMBIGUOUS"],
        })
    if not findings:
        findings.append({
            "code": "DAILY_BALANCE_CURRENCY_AMBIGUOUS",
            "severity": "WARNING",
            "snapshot_type": "unknown",
            "reasons": ["CURRENCY_AMBIGUOUS"],
        })

    open_total = open_vals["total"] if open_vals else None
    close_total = close_vals["total"] if close_vals else None

    out: Dict[str, Any] = {
        "status": "PARTIAL",
        "summary_status": "PARTIAL",
        "status_code": "DAILY_SUMMARY_PARTIAL",
        "return_metrics_available": False,
        "return_calculation_status": "OMITTED_CURRENCY_AMBIGUOUS",
        "partial_summary": True,
        "base_currency": "USD",
        "date": trade_date,
        "open_total_asset": open_total,
        "close_total_asset": close_total,
        "asset_value_change": None,
        "total_change": None,
        "total_change_pct": None,
        "investment_return_pct": None,
        "cash_change": None,
        "holdings_change": None,
        "daily_asset_pnl": None,
        "daily_return_pct": None,
        "daily_return_usd_pct": None,
        "usd_change": None,
        "omitted_metrics": list(PARTIAL_OMITTED_METRICS),
        "data_quality_findings": findings,
        "unrealized_pnl": None,
        "estimated_fees": None,
        "sold_tickers": tickers["sold_tickers"],
        "bought_tickers": tickers["bought_tickers"],
        "held_tickers": tickers["held_tickers"],
        "open_balance": open_total,
        "close_balance": close_total,
        "open_cash": open_vals["cash"] if open_vals else None,
        "close_cash": close_vals["cash"] if close_vals else None,
        "open_holdings_value": open_vals["hv"] if open_vals else None,
        "close_holdings_value": close_vals["hv"] if close_vals else None,
        "open_usd_total": open_total,
        "close_usd_total": close_total,
        "us_kis_summary": True,
        "open_ccy_error": open_err,
        "close_ccy_error": close_err,
    }
    _attach_realized_pnl_fields(
        out, open_balance, close_balance, expected_trade_date=trade_date
    )
    return out


def comparison_supports_complete_summary(raw_cmp: Any) -> bool:
    """compare_balances 결과가 COMPLETE 승격에 필요한 핵심 metric을 갖췄는지."""
    if not isinstance(raw_cmp, dict) or not raw_cmp:
        return False
    if raw_cmp.get("analysis_success") is not True:
        return False
    if raw_cmp.get("analysis_error"):
        return False
    if raw_cmp.get("return_metrics_available") is not True:
        return False
    for key in ("total_change", "cash_change", "holdings_change", "daily_return_pct"):
        if not _is_finite_number(raw_cmp.get(key)):
            return False
    return True


def build_complete_daily_summary(
    open_balance: Dict,
    close_balance: Dict,
    comparison: Dict[str, Any],
) -> Dict[str, Any]:
    """FULL pair 산술 결과에 COMPLETE 메타를 부착. 자산증감 ≠ 투자수익률.

    필수 metric이 없으면 COMPLETE를 생성하지 않고 PARTIAL을 반환한다.
    """
    out = dict(comparison) if isinstance(comparison, dict) else {}
    required_ok = comparison_supports_complete_summary(out)
    if not required_ok:
        out["status"] = "PARTIAL"
        out["summary_status"] = "PARTIAL"
        out["status_code"] = "DAILY_BALANCE_ANALYSIS_INCOMPLETE"
        out["return_metrics_available"] = False
        out["asset_change_metrics_available"] = False
        out["investment_return_available"] = False
        out["partial_summary"] = True
        out["total_change"] = out.get("total_change") if _is_finite_number(out.get("total_change")) else None
        out["total_change_pct"] = None
        out["cash_change"] = out.get("cash_change") if _is_finite_number(out.get("cash_change")) else None
        out["holdings_change"] = (
            out.get("holdings_change") if _is_finite_number(out.get("holdings_change")) else None
        )
        out["daily_return_pct"] = (
            out.get("daily_return_pct") if _is_finite_number(out.get("daily_return_pct")) else None
        )
        out["daily_asset_pnl"] = None
        out["investment_return_pct"] = None
        out["asset_value_change"] = None
        out["omitted_metrics"] = list(PARTIAL_OMITTED_METRICS)
        out["data_quality_findings"] = list(out.get("data_quality_findings") or [])
        if not any(
            f.get("code") == "DAILY_BALANCE_ANALYSIS_INCOMPLETE"
            for f in out["data_quality_findings"]
            if isinstance(f, dict)
        ):
            out["data_quality_findings"].append({
                "code": "DAILY_BALANCE_ANALYSIS_INCOMPLETE",
                "severity": "WARNING",
                "reasons": ["required_core_metrics_missing_or_analysis_failed"],
            })
        return out

    asset_change = out.get("usd_change")
    if not _is_finite_number(asset_change):
        asset_change = out.get("total_change")

    out["partial_summary"] = False
    out["return_metrics_available"] = True
    out["asset_change_metrics_available"] = True
    out["asset_value_change"] = asset_change
    out["total_change"] = out.get("total_change")
    out["total_change_pct"] = out.get("daily_return_pct")
    out["open_total_asset"] = out.get("open_usd_total", out.get("open_balance"))
    out["close_total_asset"] = out.get("close_usd_total", out.get("close_balance"))
    out["omitted_metrics"] = list(out.get("omitted_metrics") or [])
    out["data_quality_findings"] = list(out.get("data_quality_findings") or [])
    out["analysis_error"] = None

    fx_ok = out.get("fx_metrics_available") is True or not is_us_market(MARKET)
    if is_us_market(MARKET) and out.get("fx_metrics_available") is not True:
        out["fx_metrics_available"] = False
        out["fx_calculation_status"] = out.get("fx_calculation_status") or "FX_RATE_INCOMPLETE"
        if not any(
            f.get("code") == "FX_RATE_INCOMPLETE"
            for f in out["data_quality_findings"]
            if isinstance(f, dict)
        ):
            out["data_quality_findings"].append({
                "code": "FX_RATE_INCOMPLETE",
                "severity": "WARNING",
                "reasons": ["open_or_close_fx_rate_missing_or_invalid"],
            })

    if _has_external_cash_flow_evidence(open_balance, close_balance):
        out["investment_return_pct"] = out.get("daily_return_usd_pct", out.get("daily_return_pct"))
        out["daily_asset_pnl"] = asset_change
        out["investment_return_available"] = True
        out["return_calculation_status"] = "COMPLETE"
    else:
        # 자산 증감은 표시 가능, 투자수익률·daily_asset_pnl은 미확정
        out["investment_return_pct"] = None
        out["daily_asset_pnl"] = None
        out["investment_return_available"] = False
        out["return_calculation_status"] = "CASH_FLOW_EVIDENCE_INCOMPLETE"
        if "investment_return_pct" not in out["omitted_metrics"]:
            out["omitted_metrics"] = list(out["omitted_metrics"]) + ["investment_return_pct"]
        if "daily_asset_pnl" not in out["omitted_metrics"]:
            out["omitted_metrics"] = list(out["omitted_metrics"]) + ["daily_asset_pnl"]
        if not any(
            f.get("code") == "CASH_FLOW_EVIDENCE_INCOMPLETE"
            for f in out["data_quality_findings"]
            if isinstance(f, dict)
        ):
            out["data_quality_findings"].append({
                "code": "CASH_FLOW_EVIDENCE_INCOMPLETE",
                "severity": "WARNING",
                "reasons": ["external_deposit_withdraw_fx_evidence_missing"],
            })

    # COMPLETE: 핵심 metric + (US면 FX) 모두 충족할 때만
    if fx_ok:
        out["status"] = "COMPLETE"
        out["summary_status"] = "COMPLETE"
        out["status_code"] = "DAILY_SUMMARY_COMPLETE"
    else:
        out["status"] = "PARTIAL"
        out["summary_status"] = "PARTIAL"
        out["status_code"] = "DAILY_SUMMARY_PARTIAL"
        # 핵심 USD metric은 유지 (return_metrics_available=true)
    return out


def enforce_summary_state_invariants(comparison: Dict[str, Any]) -> Dict[str, Any]:
    """Summary 저장·전송 직전 모순 상태 차단. WARNING demotion, FAILED 아님."""
    out = dict(comparison) if isinstance(comparison, dict) else {}
    findings = list(out.get("data_quality_findings") or [])
    violations: List[str] = []

    status = out.get("summary_status")
    rma = bool(out.get("return_metrics_available", False))
    tc = out.get("total_change")
    tcp = out.get("total_change_pct")
    analysis_success = out.get("analysis_success")
    analysis_error = out.get("analysis_error")
    ret_status = out.get("return_calculation_status")

    if status == "COMPLETE" and not _is_finite_number(tc):
        violations.append("COMPLETE_WITH_NULL_TOTAL_CHANGE")
    if status == "COMPLETE" and analysis_error:
        violations.append("COMPLETE_WITH_ANALYSIS_ERROR")
    if rma and not _is_finite_number(tc):
        violations.append("RETURN_METRICS_TRUE_WITH_NULL_TOTAL_CHANGE")
    if rma and not _is_finite_number(tcp):
        violations.append("RETURN_METRICS_TRUE_WITH_NULL_TOTAL_CHANGE_PCT")
    if ret_status == "CASH_FLOW_EVIDENCE_INCOMPLETE" and not _is_finite_number(tc):
        violations.append("CASH_FLOW_INCOMPLETE_WITH_NULL_TOTAL_CHANGE")
    if analysis_success is False and status == "COMPLETE":
        violations.append("ANALYSIS_FAILED_WITH_COMPLETE")
    if (not out or out == {}) and status == "COMPLETE":
        violations.append("EMPTY_COMPARISON_WITH_COMPLETE")

    if not violations:
        return out

    logger.warning(
        "[SUMMARY_STATE_INVARIANT_VIOLATION] violations=%s summary_status=%s → PARTIAL",
        ",".join(violations),
        status,
    )
    out["summary_status"] = "PARTIAL"
    out["status"] = "PARTIAL"
    out["status_code"] = "SUMMARY_STATE_INVARIANT_VIOLATION"
    out["return_metrics_available"] = False
    findings.append({
        "code": "SUMMARY_STATE_INVARIANT_VIOLATION",
        "severity": "WARNING",
        "reasons": violations,
    })
    out["data_quality_findings"] = findings
    return out


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
            return {
                "ok": False,
                "summary_status": "FAILED",
                "status_code": "DAILY_BALANCE_SNAPSHOT_MISSING",
                "return_metrics_available": False,
            }

        # 4) open 스냅샷 — 과거 open은 현재 계좌로 재생성하지 않는다
        if not open_balance:
            logger.error(
                "[DAILY_BALANCE_SNAPSHOT_MISSING] OPEN_SNAPSHOT_UNAVAILABLE "
                "trade_date=%s — 현재 계좌 상태로 과거 open 재생성 금지",
                trade_date,
            )
            _notify(
                f"⚠️ trade_date={trade_date} 일일 요약: 장시작 스냅샷 없음 "
                f"(metadata.trade_date={trade_date}, type=open 인 daily balance 없음). "
                "과거 open은 재생성하지 않습니다 (OPEN_SNAPSHOT_UNAVAILABLE).",
                key="daily_summary_error",
            )
            return {
                "ok": False,
                "summary_status": "FAILED",
                "status_code": "DAILY_BALANCE_SNAPSHOT_MISSING",
                "return_metrics_available": False,
            }

        # 5) 같은 trade_date pair 검증 — 불일치 시 손익 계산 중단
        pair_errors = _validate_balance_pair(
            open_balance, close_balance, trade_date, pair["open_path"], pair["close_path"]
        )
        if pair_errors:
            logger.error(
                "[DAILY_BALANCE_PAIR_TRADE_DATE_MISMATCH] trade_date=%s: %s",
                trade_date,
                "; ".join(pair_errors),
            )
            _notify(
                f"❌ trade_date={trade_date} 일일 요약 중단: open/close trade_date 불일치 "
                f"({'; '.join(pair_errors)})",
                key="daily_summary_error",
            )
            return {
                "ok": False,
                "summary_status": "FAILED",
                "status_code": "DAILY_BALANCE_PAIR_TRADE_DATE_MISMATCH",
                "return_metrics_available": False,
            }

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
        skip_return_metrics = False
        open_ccy = close_ccy = None
        open_ccy_err = close_ccy_err = None
        if is_us_market(MARKET):
            open_ccy, open_ccy_err = _resolve_usd_comparison_values(open_balance)
            close_ccy, close_ccy_err = _resolve_usd_comparison_values(close_balance)
            if open_ccy_err or close_ccy_err:
                # 데이터 품질 경고 — 정상 PARTIAL 부분 성공 (장애 ERROR 아님)
                open_label = _snapshot_quality_label(open_balance, open_ccy, open_ccy_err)
                close_label = _snapshot_quality_label(close_balance, close_ccy, close_ccy_err)
                rejected = []
                if close_ccy_err or close_ccy is None:
                    rejected.extend(_currency_reject_reasons(close_balance))
                if open_ccy_err or open_ccy is None:
                    rejected.extend(_currency_reject_reasons(open_balance))
                rejected = list(dict.fromkeys([r for r in rejected if r]))
                omitted = ",".join(PARTIAL_OMITTED_METRICS)
                logger.warning(
                    "[DAILY_BALANCE_CURRENCY_AMBIGUOUS]\n"
                    "market=%s\n"
                    "trade_date=%s\n"
                    "summary_status=PARTIAL\n"
                    "return_metrics_available=false\n"
                    "open_status=%s\n"
                    "close_status=%s\n"
                    "rejected=%s\n"
                    "omitted_metrics=%s",
                    MARKET,
                    trade_date,
                    open_label,
                    close_label,
                    ",".join(rejected) if rejected else "CURRENCY_AMBIGUOUS",
                    omitted,
                )
                logger.warning(
                    "[DAILY_BALANCE_RETURN_METRICS_OMITTED]\n"
                    "market=%s\n"
                    "trade_date=%s\n"
                    "reason=currency_ambiguous\n"
                    "omitted_metrics=%s",
                    MARKET,
                    trade_date,
                    omitted,
                )
                skip_return_metrics = True

        # 7) 잔액 비교 분석 (usable pair만 full PnL)
        if skip_return_metrics:
            # PARTIAL: 산술 금지 — null schema, 0으로 위장하지 않음
            comparison = build_partial_daily_summary(
                open_balance,
                close_balance,
                trade_date,
                open_vals=open_ccy,
                close_vals=close_ccy,
                open_err=open_ccy_err,
                close_err=close_ccy_err,
            )
            logger.info(
                "[DAILY_SUMMARY_PARTIAL]\n"
                "market=%s\n"
                "trade_date=%s\n"
                "summary_status=PARTIAL\n"
                "return_metrics_available=false",
                MARKET,
                trade_date,
            )
        else:
            raw_cmp = compare_balances(open_balance, close_balance)
            pair_ok = is_us_market(MARKET) and pair_supports_complete_summary(
                open_balance,
                close_balance,
                open_vals=open_ccy,
                close_vals=close_ccy,
            )
            cmp_ok = comparison_supports_complete_summary(raw_cmp)
            # pair_supports_complete_summary만으로 COMPLETE 승격 금지 —
            # raw_cmp 핵심 metric·analysis_success까지 모두 충족해야 함
            if is_us_market(MARKET) and pair_ok and cmp_ok:
                comparison = build_complete_daily_summary(
                    open_balance, close_balance, raw_cmp
                )
            elif is_us_market(MARKET) and cmp_ok:
                # USD 산술은 성공했으나 COMPLETE 자격(플래그/FX 등) 미충족
                comparison = build_complete_daily_summary(
                    open_balance, close_balance, raw_cmp
                )
                if comparison.get("summary_status") == "COMPLETE":
                    comparison["summary_status"] = "PARTIAL"
                    comparison["status"] = "PARTIAL"
                    comparison["status_code"] = "DAILY_SUMMARY_PARTIAL"
            elif is_us_market(MARKET):
                # 비교 실패 또는 핵심 metric 부재 — COMPLETE 금지
                comparison = build_complete_daily_summary(
                    open_balance,
                    close_balance,
                    raw_cmp if isinstance(raw_cmp, dict) else {},
                )
                if comparison.get("summary_status") == "COMPLETE":
                    comparison["summary_status"] = "PARTIAL"
                    comparison["status"] = "PARTIAL"
                    comparison["status_code"] = "DAILY_BALANCE_ANALYSIS_INCOMPLETE"
                    comparison["return_metrics_available"] = False
            else:
                comparison = build_complete_daily_summary(
                    open_balance, close_balance, raw_cmp
                )

            if comparison.get("summary_status") == "COMPLETE":
                logger.info(
                    "[DAILY_SUMMARY_COMPLETE]\n"
                    "market=%s\n"
                    "trade_date=%s\n"
                    "summary_status=COMPLETE\n"
                    "return_metrics_available=true\n"
                    "return_calculation_status=%s",
                    MARKET,
                    trade_date,
                    comparison.get("return_calculation_status"),
                )
            else:
                logger.info(
                    "[DAILY_SUMMARY_PARTIAL]\n"
                    "summary_status=PARTIAL\n"
                    "return_metrics_available=%s\n"
                    "investment_return_available=%s\n"
                    "fx_metrics_available=%s\n"
                    "return_calculation_status=%s",
                    str(comparison.get("return_metrics_available", False)).lower(),
                    str(comparison.get("investment_return_available", False)).lower(),
                    str(comparison.get("fx_metrics_available", False)).lower(),
                    comparison.get("return_calculation_status"),
                )

        comparison = enforce_summary_state_invariants(comparison)
        return_metrics_available = bool(comparison.get("return_metrics_available", False))
        summary_status = comparison.get("summary_status") or "PARTIAL"
        if summary_status == "OK":
            # raw compare OK ≠ COMPLETE — 자격 재검증 후에만 COMPLETE 유지
            if comparison_supports_complete_summary(comparison) and (
                not is_us_market(MARKET)
                or comparison.get("fx_metrics_available") is True
            ):
                summary_status = "COMPLETE"
                comparison["summary_status"] = "COMPLETE"
            else:
                summary_status = "PARTIAL"
                comparison["summary_status"] = "PARTIAL"
        comparison = enforce_summary_state_invariants(comparison)
        summary_status = comparison.get("summary_status") or "PARTIAL"
        return_metrics_available = bool(comparison.get("return_metrics_available", False))

        # 디스코드 임베드 — 고정 schema 키만 사용 (KeyError 방지)
        total_change = comparison.get("total_change")
        daily_return = comparison.get("daily_return_pct")
        daily_return_usd = comparison.get("daily_return_usd_pct", daily_return)
        realized_pnl = comparison.get("realized_pnl")
        unrealized_pnl = comparison.get("unrealized_pnl")
        estimated_fees = comparison.get("estimated_fees")

        mkt = os.getenv("MARKET", "SP500")
        us_kis = comparison.get("us_kis_summary", is_us_market(mkt))

        primary_return = daily_return_usd if us_kis else daily_return
        primary_change = (
            comparison.get("usd_change", total_change) if us_kis else total_change
        )
        if return_metrics_available and primary_change is not None:
            change_emoji = "📈" if primary_change > 0 else "📉" if primary_change < 0 else "➡️"
            krw_ref_emoji = (
                "📈" if (total_change or 0) > 0
                else "📉" if (total_change or 0) < 0
                else "➡️"
            )
            embed_color = (
                0x00ff00 if primary_change > 0
                else 0xff0000 if primary_change < 0
                else 0x808080
            )
        else:
            change_emoji = "⚠️"
            krw_ref_emoji = "⚠️"
            embed_color = 0xF0A020  # amber — PARTIAL

        open_total = comparison.get("open_balance")
        close_total = comparison.get("close_balance")
        open_hv = comparison.get("open_holdings_value")
        close_hv = comparison.get("close_holdings_value")
        open_cash = comparison.get("open_cash")
        close_cash = comparison.get("close_cash")
        open_usd_total = comparison.get("open_usd_total")
        close_usd_total = comparison.get("close_usd_total")

        if us_kis and not return_metrics_available:
            findings = comparison.get("data_quality_findings") or []
            reason_txt = ""
            if findings:
                r0 = findings[0]
                reason_txt = ", ".join(r0.get("reasons") or [])
            fields = [
                {
                    "name": "⚠️ PARTIAL — 자산 수익률 생략",
                    "value": (
                        "Open/Close 세션 매칭은 정상입니다.\n"
                        "Close snapshot의 통화와 원본 무결성을 확인할 수 없어 "
                        "일일 자산 증감과 수익률 계산을 생략했습니다.\n"
                        "거래내역, 주문상태, 보유종목 및 realized gross P&L은 "
                        "정상 집계되었습니다."
                        + (f"\nreasons: {reason_txt}" if reason_txt else "")
                    ),
                    "inline": False,
                },
                {
                    "name": "💼 Open 총자산 (USD, 검증됨)"
                    if open_usd_total is not None
                    else "💼 Open 총자산",
                    "value": (
                        fmt_money(open_usd_total, mkt)
                        if open_usd_total is not None
                        else "계산 생략"
                    ),
                    "inline": True,
                },
                {
                    "name": "💼 Close 총자산",
                    "value": "계산 생략 (통화 ambiguous)",
                    "inline": True,
                },
                {
                    "name": "📊 일일 자산 증감 / 수익률",
                    "value": "계산 생략",
                    "inline": False,
                },
            ]
            if realized_pnl is not None:
                fields.append({
                    "name": "💰 실현 손익 (KIS gross)",
                    "value": fmt_money_signed(realized_pnl, mkt),
                    "inline": True,
                })
            # 보유 종목
            close_detail = close_balance.get("holdings_detail") or []
            if close_detail:
                lines = []
                for h in close_detail[:12]:
                    if not isinstance(h, dict):
                        continue
                    t = h.get("ticker") or "?"
                    q = h.get("qty") or h.get("hldg_qty") or ""
                    lines.append(f"{t} x{q}" if q != "" else str(t))
                fields.append({
                    "name": "📦 보유 종목 (close)",
                    "value": ", ".join(lines) if lines else "(없음)",
                    "inline": False,
                })
        elif us_kis:
            usd_total_line = (
                f"**{fmt_money(open_usd_total, mkt)}** → **{fmt_money(close_usd_total, mkt)}** "
                f"({fmt_money_signed(comparison.get('usd_change') or 0, mkt)})"
            )
            hv_line = (
                f"{fmt_money(open_hv, mkt)} → {fmt_money(close_hv, mkt)} "
                f"({fmt_money_signed(comparison.get('holdings_change') or 0, mkt)})"
            )
            cash_line = (
                f"{fmt_money(open_cash, mkt)} → {fmt_money(close_cash, mkt)} "
                f"({fmt_money_signed(comparison.get('cash_change') or 0, mkt)})"
            )
            close_detail = _fmt_usd_cash_detail(comparison.get("close_cash_detail") or {})
            if close_detail:
                cash_line = f"{cash_line}\n{close_detail}"

            fields = [
                {
                    "name": f"{change_emoji} 일일 수익률 (USD)",
                    "value": (
                        f"**{(daily_return_usd or 0):+.2f}%** "
                        f"({fmt_money_signed(comparison.get('usd_change') or 0, mkt)})"
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
            ]

            # KRW 총평가는 선택 지표 — 값이 있을 때만 표시 (USD open_balance와 혼동 금지)
            open_krw = comparison.get("open_krw_total")
            close_krw = comparison.get("close_krw_total")
            krw_change = comparison.get("krw_total_change")
            if _is_finite_number(open_krw) and _is_finite_number(close_krw):
                fields.append({
                    "name": f"{krw_ref_emoji} 총평가 (원화환산, 참고)",
                    "value": (
                        f"**{_fmt_krw(int(open_krw))}** → **{_fmt_krw(int(close_krw))}** "
                        f"({_fmt_krw_signed(int(krw_change or 0))})"
                    ),
                    "inline": False,
                })

            open_fx = _nullable_positive_fx(comparison.get("open_fx"))
            close_fx = _nullable_positive_fx(comparison.get("close_fx"))
            if open_fx is not None and close_fx is not None:
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
                f"({fmt_money_signed(total_change or 0, mkt)})"
            )
            cash_line = (
                f"{fmt_money(open_cash, mkt)} → {fmt_money(close_cash, mkt)} "
                f"({fmt_money_signed(comparison.get('cash_change') or 0, mkt)})"
            )
            hv_line = (
                f"{fmt_money(open_hv, mkt)} → {fmt_money(close_hv, mkt)} "
                f"({fmt_money_signed(comparison.get('holdings_change') or 0, mkt)})"
            )
            fields = [
                {"name": f"{change_emoji} 총평가 변화 (예수금+보유평가)", "value": total_line, "inline": False},
                {"name": "💵 예수금", "value": cash_line, "inline": True},
                {"name": "📈 보유평가", "value": hv_line, "inline": True},
            ]

        if return_metrics_available and (
            abs(primary_return or 0) > 0.01
            or abs(primary_change or 0) > 0
            or (open_hv or 0) > 0
            or (close_hv or 0) > 0
        ):
            if not us_kis:
                fields.extend([
                    {
                        "name": "📊 일일 수익률",
                        "value": f"**{(daily_return or 0):+.2f}%**",
                        "inline": True,
                    },
                ])
            fields.extend([
                {
                    "name": "💰 실현 손익 (KIS)" if us_kis else "💰 실현 손익",
                    "value": fmt_money_signed(realized_pnl or 0, mkt),
                    "inline": True,
                },
                {
                    "name": "📈 미실현·보유변동" if us_kis else "📈 미실현 손익",
                    "value": (
                        fmt_money_signed(comparison.get("holdings_change") or 0, mkt)
                        if us_kis
                        else fmt_money_signed(unrealized_pnl or 0, mkt)
                    ),
                    "inline": True,
                },
            ])

            if estimated_fees is not None and abs(estimated_fees) > 0 and not us_kis:
                fields.append({
                    "name": "💸 추정 수수료",
                    "value": fmt_money_signed(estimated_fees, mkt),
                    "inline": True,
                })

        # 매매 내역 (PARTIAL에서도 표시)
        sold = comparison.get("sold_tickers") or []
        bought = comparison.get("bought_tickers") or []
        if sold or bought:
            trading_summary = []
            if sold:
                trading_summary.append(f"🔴 매도 {len(sold)}종목 ({', '.join(sold[:8])})")
            if bought:
                trading_summary.append(f"🟢 매수 {len(bought)}종목 ({', '.join(bought[:8])})")
            fields.append({
                "name": "📋 매매 내역",
                "value": " | ".join(trading_summary),
                "inline": False,
            })

        # 임베드 전송 — 미국 거래일과 KST 종료일을 명확히 구분해 표시
        if is_us_market(MARKET):
            title = f"📊 {_fmt_date_dash(trade_date)} 당일 매매 성과 (미국 거래일)"
            if not return_metrics_available:
                title = f"📊 {_fmt_date_dash(trade_date)} 당일 매매 요약 [PARTIAL]"
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
        if not return_metrics_available:
            desc += (
                "\nOpen/Close 세션 매칭은 정상입니다.\n"
                "Close snapshot의 통화와 원본 무결성을 확인할 수 없어 "
                "일일 자산 증감과 수익률 계산을 생략했습니다.\n"
                "거래내역, 주문상태, 보유종목 및 realized gross P&L은 "
                "정상 집계되었습니다."
            )
        if late_close_capture:
            desc += "\n⚠️ close는 요약 시점 recovery 캡처"
        embed = {
            "type": "rich",
            "title": title,
            "description": desc,
            "fields": fields,
            "color": embed_color,
            "footer": {
                "text": (
                    f"보유종목: open {open_balance.get('holdings_count', 0)}개 → "
                    f"close {close_balance.get('holdings_count', 0)}개 | "
                    f"open={open_file_name} close={close_file_name} | "
                    f"summary_status={summary_status}"
                )
            },
        }

        # summary metadata — flat schema (null metrics, never fake zeros)
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
            "summary_status": summary_status,
            "status_code": comparison.get("status_code") or (
                "DAILY_SUMMARY_PARTIAL" if summary_status == "PARTIAL" else "DAILY_SUMMARY_COMPLETE"
            ),
            "return_metrics_available": return_metrics_available,
            "asset_change_metrics_available": bool(
                comparison.get("asset_change_metrics_available", return_metrics_available)
            ),
            "investment_return_available": bool(
                comparison.get("investment_return_available", False)
            ),
            "fx_metrics_available": bool(comparison.get("fx_metrics_available", False)),
            "return_calculation_status": comparison.get("return_calculation_status"),
            "fx_calculation_status": comparison.get("fx_calculation_status"),
            "total_change": comparison.get("total_change"),
            "total_change_pct": comparison.get("total_change_pct"),
            "cash_change": comparison.get("cash_change"),
            "holdings_change": comparison.get("holdings_change"),
            "daily_asset_pnl": comparison.get("daily_asset_pnl"),
            "investment_return_pct": comparison.get("investment_return_pct"),
            "asset_value_change": comparison.get("asset_value_change"),
            "omitted_metrics": list(comparison.get("omitted_metrics") or []),
            "data_quality_findings": list(comparison.get("data_quality_findings") or []),
            "analysis_error": comparison.get("analysis_error"),
            "realized_pnl": comparison.get("realized_pnl"),
            "open_total_asset": comparison.get("open_total_asset"),
            "close_total_asset": comparison.get("close_total_asset"),
            "generated_at_kst": datetime.now(KST).isoformat(),
        }
        try:
            meta_path = BALANCE_STORAGE_PATH / f"daily_summary_meta_{trade_date}.json"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(summary_meta, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("daily summary metadata 저장 실패: %s", e)

        discord_sent = False
        if WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL):
            try:
                send_discord_message(embeds=[embed])
                discord_sent = True
                status_tag = (
                    "DAILY_SUMMARY_PARTIAL"
                    if summary_status == "PARTIAL"
                    else "DAILY_SUMMARY_COMPLETE"
                )
                logger.info(
                    "[%s] 일일 매매 요약 전송 완료: trade_date=%s "
                    "summary_status=%s (KST 종료일 %s)",
                    status_tag,
                    trade_date,
                    summary_status,
                    session_close_kst,
                )
            except Exception as e:
                logger.error(
                    "[DAILY_SUMMARY_DELIVERY_FAILED] trade_date=%s error=%s",
                    trade_date,
                    e,
                )
                _notify(
                    f"❌ 일일 매매 요약 Discord 전송 실패: {type(e).__name__}: {e}",
                    key="daily_summary_delivery_error",
                )
                return {
                    "ok": False,
                    "summary_status": "FAILED",
                    "status_code": "DAILY_SUMMARY_DELIVERY_FAILED",
                    "return_metrics_available": return_metrics_available,
                    "trade_date": trade_date,
                    "analysis": comparison,
                    "discord_sent": False,
                    "error": f"{type(e).__name__}: {e}",
                }

        return {
            "ok": True,
            "summary_status": summary_status,
            "status_code": summary_meta.get("status_code"),
            "return_metrics_available": return_metrics_available,
            "trade_date": trade_date,
            "analysis": comparison,
            "discord_sent": discord_sent,
            "meta_path": str(BALANCE_STORAGE_PATH / f"daily_summary_meta_{trade_date}.json"),
        }

    except Exception as e:
        logger.error(
            "[DAILY_SUMMARY_FAILED] 일일 매매 요약 전송 실패: %s: %s",
            type(e).__name__,
            e,
        )
        logger.debug("일일 매매 요약 상세 오류:", exc_info=True)
        _notify(f"❌ 일일 매매 요약 전송 실패: {type(e).__name__}: {str(e)}", key="daily_summary_error")
        return {
            "ok": False,
            "summary_status": "FAILED",
            "status_code": "DAILY_SUMMARY_FAILED",
            "return_metrics_available": False,
            "error": f"{type(e).__name__}: {e}",
        }

# ───────────────── 파이프라인 실행 ─────────────────
def _tail(text: str, n: int = 12) -> str:
    """로그 텍스트의 꼬리 n줄만 반환"""
    if not text:
        return ""
    lines = text.strip().splitlines()
    return "\n".join(lines[-n:])


def _extract_screener_summary(stdout: str) -> str:
    """운영 INFO용 스크리너 핵심 요약 라인만 추출."""
    try:
        from screener_ops import extract_screener_summary_lines

        lines = extract_screener_summary_lines(stdout or "")
        return "\n".join(lines) if lines else _tail(stdout, 20)
    except Exception:
        return _tail(stdout, 20)


def _save_subprocess_stdout_log(
    script_name: str,
    run_id: str,
    pipeline_ctx: Dict[str, str],
    stdout: str,
    stderr: str = "",
) -> Optional[Path]:
    """성공/실패 모두 자식 stdout/stderr를 파일로 보존."""
    try:
        from screener_ops import save_subprocess_log

        trade_date = pipeline_ctx.get("trade_date") or datetime.now(KST).strftime("%Y%m%d")
        session = pipeline_ctx.get("session") or "pm"
        stem = Path(script_name).stem
        logs_dir = OUTPUT_DIR / "logs"
        return save_subprocess_log(
            logs_dir,
            script_stem=stem,
            trade_date=trade_date,
            session=session,
            market=MARKET,
            run_id=run_id,
            stdout=stdout or "",
            stderr=stderr or "",
        )
    except Exception as e:
        logger.warning("[%s] subprocess log 저장 실패(%s): %s", run_id, script_name, e)
        return None

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
        log_path = _save_subprocess_stdout_log(
            script_name, run_id, ctx, result.stdout or "", result.stderr or ""
        )
        logger.info(f"[{run_id}] ✅ STEP OK: {script_name} | {dur:.1f}s")
        if log_path:
            logger.info(f"[{run_id}] subprocess log saved: {log_path}")
        if script_name == "screener.py":
            summary = _extract_screener_summary(result.stdout or "")
            if summary:
                logger.info(f"[{run_id}] --- screener summary ---\n{summary}")
        else:
            logger.debug(f"[{run_id}] --- {script_name} tail ---\n{_tail(result.stdout, 12)}")

        if dur > SLOW_STEP_SEC:
            warned = True
            logger.warning(f"[{run_id}] ⚠️ SLOW STEP: {script_name} ({dur:.1f}s > {SLOW_STEP_SEC}s)")

        return True, warned, dur

    except subprocess.TimeoutExpired as e:
        dur = time.perf_counter() - t0
        out = getattr(e, "stdout", "") or ""
        err = getattr(e, "stderr", "") or ""
        if isinstance(out, bytes):
            out = out.decode("utf-8", errors="replace")
        if isinstance(err, bytes):
            err = err.decode("utf-8", errors="replace")
        log_path = _save_subprocess_stdout_log(script_name, run_id, ctx, out, err)
        logger.error(f"[{run_id}] ❌ STEP TIMEOUT: {script_name} ({timeout_sec}s) | {dur:.1f}s 경과")
        if log_path:
            logger.error(f"[{run_id}] subprocess log saved: {log_path}")
        logger.error(f"[{run_id}] --- STDOUT tail ---\n{_tail(out, 40)}")
        logger.error(f"[{run_id}] --- STDERR tail ---\n{_tail(err, 40)}")
        return False, warned, dur

    except subprocess.CalledProcessError as e:
        dur = time.perf_counter() - t0
        stdout_tail = _tail(e.stdout, 80)
        stderr_tail = _tail(e.stderr, 80)
        log_path = _save_subprocess_stdout_log(
            script_name, run_id, ctx, e.stdout or "", e.stderr or ""
        )
        logger.error(f"[{run_id}] ❌ STEP FAIL: {script_name} (exit={e.returncode}) | {dur:.1f}s")
        if log_path:
            logger.error(f"[{run_id}] subprocess log saved: {log_path}")
        logger.error(f"[{run_id}] --- STDOUT tail ---\n{stdout_tail}")
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
    parser.add_argument(
        "--repair-daily-balance-currency",
        action="store_true",
        help="embedded evidence로 daily balance USD/KRW 필드 보정",
    )
    parser.add_argument(
        "--snapshot-type",
        choices=["open", "close"],
        default="open",
        help="--repair-daily-balance-currency 대상 (open|close)",
    )
    parser.add_argument("--dry-run", action="store_true", help="migration/repair 미리보기")
    parser.add_argument("--apply", action="store_true", help="migration/repair 실제 적용")
    args = parser.parse_args()
    
    print(f"인수 파싱 완료: {args}")
    
    # 백그라운드 RiskManager 인스턴스
    background_risk_manager = None
    
    if args.repair_daily_balance_currency:
        if not args.trade_date:
            raise SystemExit("--repair-daily-balance-currency requires --trade-date YYYYMMDD")
        repair_daily_balance_currency(
            args.trade_date,
            snapshot_type=args.snapshot_type,
            apply=bool(args.apply and not args.dry_run),
        )
    elif args.migrate_daily_balance_layout:
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
