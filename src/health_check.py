# src/health_check.py
"""
KIS API 헬스체크
- KR: 삼성전자(005930) 국내 현재가
- US: AAPL 해외 현재가 (HHDFS00000300)
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from api.kis_auth import KIS
from utils import (
    setup_logging,
    is_us_market,
    get_us_regime_config,
    resolve_us_excd,
    set_us_ticker_excd_map,
    set_us_ticker_ovrs_excg_map,
    KST,
)
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
    market = os.getenv("MARKET", "SP500")
    logger.info("API 헬스 체크 시작 (MARKET=%s)...", market)
    _notify(f"🔍 KIS API 헬스체크 시작 (MARKET={market})")

    try:
        kis = KIS(env=os.getenv("TRADING_ENV", "prod"))
        if not getattr(kis, "auth_token", None):
            raise ConnectionError("KIS API 인증 실패 (토큰 없음)")

        if is_us_market(market):
            try:
                from kis_master import load_kis_master

                mst = load_kis_master(market, cache_key=datetime.now(KST).strftime("%Y%m%d"))
                if mst is not None and not mst.empty:
                    if "EXCD" in mst.columns:
                        set_us_ticker_excd_map(
                            dict(zip(mst.index.astype(str), mst["EXCD"].astype(str)))
                        )
                    if "OvrsExcg" in mst.columns:
                        set_us_ticker_ovrs_excg_map(
                            dict(zip(mst.index.astype(str), mst["OvrsExcg"].astype(str)))
                        )
            except Exception as e:
                logger.warning("US 거래소 맵 로드 실패: %s", e)

            checks = [
                (os.getenv("HEALTH_CHECK_TICKER_NAS", "AAPL"), None),
                (os.getenv("HEALTH_CHECK_TICKER_NYS", "JPM"), None),
            ]
            parts = []
            for symb, _ in checks:
                excd = resolve_us_excd(symb, market)
                price_df = kis.overseas_price(excd, symb)
                if price_df is None or price_df.empty:
                    raise ValueError(f"해외 현재가 API 빈 응답: {symb}@{excd}")
                row = price_df.iloc[0]
                price = row.get("last") or row.get("last_pr") or row.get("stck_prpr") or row.get("prpr")
                parts.append(f"{symb}@{excd}={price}")

            rc = get_us_regime_config()
            idx_sym = str(rc.get("index_symbol") or "SPX").strip().upper()
            idx_mc = str(rc.get("index_market_code") or "N").strip().upper()
            end_d = datetime.now(KST).strftime("%Y%m%d")
            start_d = (datetime.now(KST) - timedelta(days=14)).strftime("%Y%m%d")
            idx_df = kis.overseas_daily_chart_price(idx_mc, idx_sym, start_d, end_d, period="D")
            if idx_df is None or idx_df.empty:
                raise ValueError(
                    f"해외지수 일봉 API 빈 응답: {idx_sym} (market_code={idx_mc})"
                )
            parts.append(f"{idx_sym}(index,{len(idx_df)}bars)")

            msg = "✅ API 헬스체크 통과 (US " + ", ".join(parts) + ")"
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
