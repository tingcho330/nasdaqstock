# src/notifier.py
import os

from utils import normalize_ticker_6
import time
import re
import logging
import threading
from typing import Optional, List, Dict, Any

import httpx

# ────────────────────────────────────────────────────────────────────
# 기본 설정
# ────────────────────────────────────────────────────────────────────
WEBHOOK_URL: str = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

# httpx 타임아웃 (connect/read/write 개별 설정)
HTTPX_TIMEOUT = httpx.Timeout(10.0, connect=5.0, read=5.0, write=5.0)

DEFAULT_TIMEOUT = 7.0
MAX_RETRIES = 2              # 429/일시 오류 재시도 횟수
BASE_BACKOFF = 0.6           # 지수 백오프 시작(초)
MIN_INTERVAL = 0.75          # 메시지 최소 간격(초) - 러프한 토큰버킷

# noisy 로거 셋 (emit에서 필터링)
_NOISY_LOGGERS = {"httpx", "httpcore", "urllib3"}

# emit 재진입 가드용 thread-local
_emit_local = threading.local()

# 내부 전용 로거(핸들러 재귀 방지 위해 사용 최소화: 기본적으로 찍지 않음)
_logger = logging.getLogger("notifier")
_logger.propagate = False  # 상위(root)로 전파 금지

# 전역 HTTP 클라이언트
_client = httpx.Client(timeout=HTTPX_TIMEOUT, headers={"Content-Type": "application/json"})

# 간단 레이트리밋(최소 간격)
_last_sent_ts = 0.0

# ────────────────────────────────────────────────────────────────────
# 유틸
# ────────────────────────────────────────────────────────────────────
_WEBHOOK_RE = re.compile(r"^https://discord\.com/api/webhooks/\d+/[A-Za-z0-9_\-]+$")

# 공용 알림 쿨다운 관리
_notification_cooldowns = {}
_notification_lock = threading.Lock()

def notify_with_cooldown(message: str, key: str, cooldown_sec: int = 300, 
                        webhook_url: Optional[str] = None, 
                        retry_on_failure: bool = True) -> bool:
    """
    공용 알림 쿨다운 헬퍼
    - key: 쿨다운 식별자
    - cooldown_sec: 쿨다운 시간 (초)
    - webhook_url: 사용할 웹훅 URL (None이면 기본값)
    - retry_on_failure: 실패 시 재시도 여부
    """
    current_time = time.time()
    
    with _notification_lock:
        last_sent = _notification_cooldowns.get(key, 0)
        if current_time - last_sent < cooldown_sec:
            return False  # 쿨다운 중
        
        # 알림 전송 시도
        success = send_discord_message(message, webhook_url=webhook_url, 
                                     retry_on_failure=retry_on_failure)
        
        if success:
            _notification_cooldowns[key] = current_time
        
        return success

def is_valid_webhook(url: Optional[str]) -> bool:
    # 간결 체크(사용자 제안) + 정규식 강화 체크 병행
    if not url:
        return False
    if not url.startswith("https://discord.com/api/webhooks/"):
        return False
    return bool(_WEBHOOK_RE.match(url))

# ────────────────────────────────────────────────────────────────────
# 디스코드 전송
#  - content 없이 embeds만으로도 전송 가능
#  - 429/일시 오류 백오프 재시도
#  - 내부에서 logging 호출하지 않아 핸들러 재귀 방지
# ────────────────────────────────────────────────────────────────────
def send_discord_message(
    content: Optional[str] = None,
    embeds: Optional[List[Dict[str, Any]]] = None,
    username: Optional[str] = None,
) -> None:
    """Discord Webhook 전송. 실패는 조용히 무시(로깅 재귀 방지)."""
    global _last_sent_ts
    if not is_valid_webhook(WEBHOOK_URL):
        return

    # 최소 간격 레이트리밋
    now = time.time()
    gap = now - _last_sent_ts
    if gap < MIN_INTERVAL:
        try:
            time.sleep(MIN_INTERVAL - gap)
        except Exception:
            # sleep 중 인터럽트 등은 무시
            pass

    payload: Dict[str, Any] = {}
    if content:
        # 디스코드 content 최대 2000자, 여유 있게 1900자로 제한
        payload["content"] = str(content)[:1900]
    if embeds:
        # embed 10개 제한 고려(보수적으로 5개)
        payload["embeds"] = embeds[:5]
    if username:
        payload["username"] = username

    if not payload.get("content") and not payload.get("embeds"):
        # 아무 것도 없으면 전송 안 함
        return

    backoff = BASE_BACKOFF
    for attempt in range(0, MAX_RETRIES + 1):
        try:
            resp = _client.post(WEBHOOK_URL, json=payload)
            if resp.status_code == 204:
                _last_sent_ts = time.time()
                return
            if resp.status_code == 429:
                # 디스코드 레이트리밋 헤더 기반 대기
                retry_after = 0.0
                try:
                    # 우선순위: Retry-After(초) → X-RateLimit-Reset-After(초)
                    retry_after = float(resp.headers.get("Retry-After", "0"))
                except Exception:
                    retry_after = 0.0
                if retry_after <= 0:
                    retry_after = backoff
                    backoff *= 2
                try:
                    time.sleep(min(10.0, max(0.5, retry_after)))
                except Exception:
                    pass
                continue  # 재시도
            # 5xx/일시적 장애: 지수 백오프 후 재시도
            if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
                try:
                    time.sleep(backoff)
                except Exception:
                    pass
                backoff *= 2
                continue
            # 이외 오류는 조용히 중단
            return
        except Exception:
            # 네트워크 예외 등: 지수 백오프 재시도
            if attempt < MAX_RETRIES:
                try:
                    time.sleep(backoff)
                except Exception:
                    pass
                backoff *= 2
                continue
            return

# ────────────────────────────────────────────────────────────────────
# 로깅 핸들러
#  - noisy 로거 무시
#  - emit 내에서 어떤 로거도 호출하지 않음(재귀 방지)
#  - 재진입 가드(thread-local) 적용
#  - content-only 간결 메시지 전송
# ────────────────────────────────────────────────────────────────────
class DiscordLogHandler(logging.Handler):
    def __init__(self, webhook_url: Optional[str] = None, level=logging.ERROR):
        super().__init__(level=level)
        self.webhook_url = webhook_url or WEBHOOK_URL

    def emit(self, record: logging.LogRecord) -> None:
        # noisy 로거 무시
        if record.name in _NOISY_LOGGERS:
            return

        # 재진입 가드
        if getattr(_emit_local, "emitting", False):
            return

        if not is_valid_webhook(self.webhook_url):
            return

        # 너무 긴 메시지 방어 및 포맷
        try:
            msg = self.format(record)
        except Exception:
            try:
                msg = record.getMessage()
            except Exception:
                msg = ""

        # 스택트레이스 등은 길어질 수 있으므로 content에만 담고 잘라냄
        text = msg[:1900] if msg else ""

        # 절대 logging 호출 금지 (여기서 다시 로깅하면 재귀됨)
        try:
            _emit_local.emitting = True
            if text:
                send_discord_message(content=text)
        finally:
            _emit_local.emitting = False

# ────────────────────────────────────────────────────────────────────
# 주문/체결용 임베드 포맷터
# ────────────────────────────────────────────────────────────────────
def get_embed_color(event_type: str) -> int:
    """
    Discord embed 색상 팔레트 (decimal RGB)
    - success/info: 파랑/초록
    - warning: 노랑/주황, error/fail: 빨강, cooldown: 회색
    """
    table = {
        'success': 0x00C853,    # 초록
        'info': 0x1976d2,       # 파랑
        'warning': 0xffc107,    # 노랑
        'fail': 0xd32f2f,       # 빨강
        'cooldown': 0x757575,   # 회색
        'summary': 0x607d8b,    # 블루그레이
    }
    return table.get(event_type, 0x1976d2)


def get_event_emoji(event_type: str) -> str:
    table = {
        'success': '✅',
        'info': '📢',
        'warning': '⚠️',
        'fail': '❌',
        'cooldown': '⛔',
        'summary': '📊',
        'market': '💹',
    }
    return table.get(event_type, '')


def create_trade_embed(payload: Dict[str, Any]) -> Dict[str, Any]:
    side = str(payload.get("side", "?")).upper()
    name = payload.get("name", "N/A")
    ticker = normalize_ticker_6(payload.get("ticker", "N/A"), os.getenv("MARKET", "SP500"))
    qty = payload.get("qty", 0)
    price = payload.get("price", 0)
    trade_status = payload.get("trade_status", "submitted")
    details = payload.get("strategy_details") or {}

    # event/severity 분기
    if trade_status in ("completed", "executed", "partial"):
        color = get_embed_color('success')
        emoji = get_event_emoji('success')
    elif trade_status in ("failed", "rejected"):
        color = get_embed_color('fail')
        emoji = get_event_emoji('fail')
    elif trade_status in ("cooldown",):
        color = get_embed_color('cooldown')
        emoji = get_event_emoji('cooldown')
    elif trade_status in ("submitted", "pending"):
        color = get_embed_color('info')
        emoji = get_event_emoji('info')
    else:
        color = get_embed_color('info')
        emoji = get_event_emoji('info')

    title_emoj = f"{emoji} {'매수' if side == 'BUY' else '매도'}"
    fields = [
        {"name": "티커", "value": f"`{ticker}`", "inline": True},
        {"name": "수량", "value": f"{qty}", "inline": True},
        {"name": "가격", "value": f"{price:,}", "inline": True},
        {"name": "상태", "value": f"{trade_status}", "inline": True},
    ]
    # 상세(TEXT/사유/사이드/체결시간/주문ID 등)
    if details:
        try:
            import json as _json
            pretty = _json.dumps(details, ensure_ascii=False, indent=2)
            fields.append({"name": "Details", "value": f"```json\n{pretty[:900]}\n```", "inline": False})
        except Exception:
            fields.append({"name": "Details", "value": str(details)[:900], "inline": False})
    # 가장 흔한 오류/경고 메시지는 독립 노출
    if isinstance(details, dict):
        for k in ("broker_msg", "reason", "error", "summary_reason"):
            v = details.get(k)
            if v:
                fields.append({"name": k.capitalize(), "value": str(v)[:256], "inline": False})
    # 시장가 폴백 등 이벤트도 노출
    if details.get("order_type", "").lower() == "market":
        fields.append({"name": "주문유형", "value": "시장가(💹)", "inline": True})

    return {
        "type": "rich",
        "title": f"{title_emoj} {name}",
        "description": f"{name} ({ticker})",
        "fields": fields,
        "color": color,
    }


def create_alert_embed(event_text: str, level: str = 'info', fields: list = None) -> Dict[str, Any]:
    color = get_embed_color(level)
    emoji = get_event_emoji(level)
    embed = {
        "type": "rich",
        "title": f"{emoji} {event_text}",
        "color": color,
    }
    if fields:
        embed["fields"] = fields
    return embed


def create_summary_embed(summary_text: str, statistics: Dict[str, Any] = None) -> Dict[str, Any]:
    color = get_embed_color('summary')
    emoji = get_event_emoji('summary')
    embed = {
        "type": "rich",
        "title": f"{emoji} 파이프라인 요약",
        "description": summary_text,
        "color": color,
    }
    if statistics:
        flds = []
        for k, v in statistics.items():
            flds.append({"name": str(k), "value": str(v), "inline": True})
        embed["fields"] = flds
    return embed
