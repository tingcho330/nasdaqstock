#!/usr/bin/env python3
"""
백그라운드 RiskManager 실행 스크립트
- 장중 5분마다 보유 종목 모니터링
- 급격한 가격 변동 감지 및 알림
"""

import sys
import os
import time
import signal
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
try:
    project_root = Path(__file__).parent
except NameError:
    # __file__이 정의되지 않은 경우 (exec로 실행될 때)
    project_root = Path("/app")
sys.path.insert(0, str(project_root / "src"))

from env_loader import load_project_env

load_project_env()

# 리스크 전용 Discord: config/.env 의 DISCORD_WEBHOOK_URL_RISK (config/.env.risk 미사용)
_risk_webhook = os.getenv("DISCORD_WEBHOOK_URL_RISK", "").strip()
if not _risk_webhook:
    _risk_webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if _risk_webhook:
        print(
            "경고: DISCORD_WEBHOOK_URL_RISK 미설정 → DISCORD_WEBHOOK_URL 사용 "
            "(리스크 전용 채널을 쓰려면 config/.env 에 DISCORD_WEBHOOK_URL_RISK 설정)"
        )
if _risk_webhook:
    os.environ["DISCORD_WEBHOOK_URL"] = _risk_webhook
else:
    print("경고: Discord 웹훅 없음 (DISCORD_WEBHOOK_URL_RISK 또는 DISCORD_WEBHOOK_URL)")

print("=== 백그라운드 RiskManager 시작 ===")

# 환경 변수 설정
os.environ.setdefault("MARKET", "KOSPI")
os.environ.setdefault("SLOTS", "3")

# 통합 매니저에서 BackgroundRiskManager import
from integrated_manager import BackgroundRiskManager
from settings import settings

def signal_handler(signum, frame):
    print("종료 신호 수신, 백그라운드 RiskManager 정리 중...")
    if 'background_risk_manager' in globals():
        background_risk_manager.stop()
    sys.exit(0)

if __name__ == "__main__":
    print("백그라운드 RiskManager 초기화 중...")
    
    # 시그널 핸들러 설정
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # BackgroundRiskManager 인스턴스 생성 및 시작
        background_risk_manager = BackgroundRiskManager(settings)
        background_risk_manager.start()
        print("백그라운드 RiskManager가 시작되었습니다.")
        
        # 메인 루프 - 컨테이너가 종료될 때까지 실행
        while True:
            time.sleep(60)  # 1분마다 상태 확인
            
    except KeyboardInterrupt:
        print("사용자에 의한 종료")
        if 'background_risk_manager' in globals():
            background_risk_manager.stop()
    except Exception as e:
        print(f"백그라운드 RiskManager 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()
        if 'background_risk_manager' in globals():
            background_risk_manager.stop()
