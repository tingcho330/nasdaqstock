#!/usr/bin/env python3
"""
통합 매니저 실행 스크립트
- 기존 risk_manager.py와 scheduler.py를 대체
- 장시작/종료시 잔액 비교 기능 포함
"""

print("=== run_integrated_manager.py 로드됨 ===")

import sys
import os
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

# 통합 매니저 실행
if __name__ == "__main__":
    print("=== run_integrated_manager.py 시작 ===")
    
    # 환경 변수 설정
    os.environ.setdefault("MARKET", "NASDAQ100")
    os.environ.setdefault("SLOTS", "3")
    os.environ.setdefault("SCHED_MAX_ATTEMPTS", "3")
    os.environ.setdefault("SCHED_INITIAL_BACKOFF_MINUTES", "2")
    os.environ.setdefault("SCRIPT_TIMEOUT_SEC", "600")
    os.environ.setdefault("SLOW_STEP_SEC", "90")
    
    print("환경 변수 설정 완료")
    
    # 통합 매니저 실행
    import integrated_manager
    import sys
    print("integrated_manager 모듈 로드 완료")
    
    # sys.argv를 integrated_manager에 전달
    original_argv = sys.argv
    sys.argv = original_argv
    
    # 명령행 인수 처리
    import argparse
    parser = argparse.ArgumentParser(description="통합 매니저 실행")
    parser.add_argument("--once", action="store_true", help="단발 실행 (스케줄 없이)")
    parser.add_argument("--capture-open", action="store_true", help="장시작 잔액 캡처")
    parser.add_argument("--capture-close", action="store_true", help="장종료 잔액 캡처")
    parser.add_argument("--send-summary", action="store_true", help="일일 요약 전송")
    parser.add_argument("--no-background-risk", action="store_true", help="백그라운드 RiskManager 비활성화")
    args = parser.parse_args()
    
    if args.capture_open:
        integrated_manager.capture_balance_snapshot("open")
    elif args.capture_close:
        integrated_manager.capture_balance_snapshot("close")
    elif args.send_summary:
        integrated_manager.send_daily_trading_summary()
    elif args.once:
        # 단발 실행
        print("통합 매니저 단발 실행")
        integrated_manager.capture_balance_snapshot("open")
        integrated_manager.run_screener_job()
        integrated_manager.run_trading_pipeline()
        integrated_manager.capture_balance_snapshot("close")
        integrated_manager.send_daily_trading_summary()
    else:
        # 스케줄 실행
        integrated_manager.register_jobs()
        integrated_manager.list_jobs()
        
        print("통합 매니저가 시작되었습니다. 다음 작업 대기 중...")
        
        # 시그널 핸들러 설정
        import signal
        def signal_handler(signum, frame):
            print("종료 신호 수신")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        import time
        try:
            while True:
                integrated_manager.schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("사용자에 의한 종료")
        except Exception as e:
            print(f"통합 매니저 실행 중 오류: {e}")
