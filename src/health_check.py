# src/health_check.py
"""
KIS API 헬스체크
- KR: 삼성전자(005930) 국내 현재가
- US: AAPL 해외 현재가 (HHDFS00000300)
"""

import os
import sys
import logging

from api.kis_auth import KIS
from utils import setup_logging, is_us_market, us_excd
from notifier import DiscordLogHandler, WEBHOOK_URL, send_discord_message

setup_logging()
logger = logging.getLogger("HealthCheck")

_root = logging.getLogger()
if WEBHOOK_URL and WEBHOOK_URL.startswith(("http://", "https://")):
    if not any(isinstance(h, DiscordLogHandler) for h in _root.handlers):
        _root.addHandler(DiscordLogHandler(WEBHOOK_URL))
else:
    logger.warning("유효한 DISCORD_WEBHOOK_URL이 없어 디스코드 전송을 비활성화합니다.")


def _notify(msg: str):
    try:
        if WEBHOOK_URL and WEBHOOK_URL.startswith(("http://", "https://")):
            send_discord_message(content=msg)
    except Exception:
        pass


def main():
    market = os.getenv("MARKET", "NASDAQ100")
    logger.info("API 헬스 체크 시작 (MARKET=%s)...", market)
    _notify(f"🔍 KIS API 헬스체크 시작 (MARKET={market})")

    try:
        kis = KIS(env=os.getenv("TRADING_ENV", "prod"))
        if not getattr(kis, "auth_token", None):
            raise ConnectionError("KIS API 인증 실패 (토큰 없음)")

        if is_us_market(market):
            excd = us_excd(market)
            symb = os.getenv("HEALTH_CHECK_TICKER", "AAPL")
            price_df = kis.overseas_price(excd, symb)
            if price_df is None or price_df.empty:
                raise ValueError("해외 현재가 API가 빈 데이터를 반환했습니다.")
            row = price_df.iloc[0]
            price = row.get("last") or row.get("last_pr") or row.get("stck_prpr") or row.get("prpr")
            msg = f"✅ API 헬스체크 통과 (US {symb} @ {excd}: {price})"
        else:
            price_df = kis.inquire_price(fid_cond_mrkt_div_code="J", fid_input_iscd="005930")
            if price_df is None or price_df.empty:
                raise ValueError("API가 빈 데이터를 반환했습니다.")
            price = price_df["stck_prpr"].iloc[0]
            msg = f"✅ API 헬스체크 통과 (삼성전자 현재가: {price})"

        logger.info(msg)
        _notify(msg)
        sys.exit(0)

    except Exception as e:
        msg = f"❌ API 헬스체크 실패: {e}"
        logger.error(msg, exc_info=True)
        _notify(msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
