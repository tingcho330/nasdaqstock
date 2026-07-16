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
    os.environ.setdefault("MARKET", "SP500")
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
    parser.add_argument(
        "--trade-date",
        default=None,
        help="대상 미국 거래일 YYYYMMDD (capture/send-summary/migration)",
    )
    parser.add_argument(
        "--migrate-daily-balance-layout",
        action="store_true",
        help="legacy daily balance를 metadata 기준 canonical 레이아웃으로 복사 (legacy 파일 삭제 없음)",
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
        help="--repair-daily-balance-currency 대상",
    )
    parser.add_argument("--dry-run", action="store_true", help="migration/repair 미리보기 (파일 변경 없음)")
    parser.add_argument("--apply", action="store_true", help="migration/repair 실제 적용")
    args = parser.parse_args()
    
    if args.repair_daily_balance_currency:
        if not args.trade_date:
            raise SystemExit("--repair-daily-balance-currency requires --trade-date YYYYMMDD")
        result = integrated_manager.repair_daily_balance_currency(
            args.trade_date,
            snapshot_type=args.snapshot_type,
            apply=bool(args.apply and not args.dry_run),
        )
        print(
            f"status={result.get('status')} gates_ok={result.get('gates_ok')} "
            f"updated={result.get('updated')} "
            f"arithmetic_candidate_total_asset_usd={result.get('arithmetic_candidate_total_asset_usd')} "
            f"candidate_currency_unverified={result.get('candidate_currency_unverified')} "
            f"rejected={result.get('rejected_fields')}"
        )
        sys.exit(0)
    elif args.migrate_daily_balance_layout:
        integrated_manager.migrate_daily_balance_layout(
            trade_date=args.trade_date,
            apply=bool(args.apply and not args.dry_run),
        )
    elif args.capture_open:
        integrated_manager.capture_balance_snapshot("open", trade_date=args.trade_date)
    elif args.capture_close:
        integrated_manager.capture_balance_snapshot("close", trade_date=args.trade_date)
    elif args.send_summary:
        summary = integrated_manager.send_daily_trading_summary(
            target_trade_date=args.trade_date
        )
        status = (summary or {}).get("summary_status", "UNKNOWN")
        print(f"summary_status={status}")
        # PARTIAL/OK 모두 정상 종료 (exit 0). FAILED만 non-zero.
        sys.exit(0 if status not in ("FAILED",) else 1)
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
