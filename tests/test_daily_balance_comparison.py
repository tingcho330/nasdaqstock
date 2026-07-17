"""compare_balances core-USD / optional-FX 분리, COMPLETE 승격, Summary invariant."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import os  # noqa: E402

os.environ.setdefault("OUTPUT_DIR", str(ROOT / "output_test"))
os.environ.setdefault("CONFIG_PATH", str(ROOT / "config" / "config.json"))
os.environ.setdefault("MARKET", "SP500")

import integrated_manager as im  # noqa: E402

TD = "20260716"
CLOSE_KST = "20260717"


def _usd_pair(
    *,
    open_fx=1488.8,
    close_fx=1488.8,
    open_krw_total=2430000,
    close_krw_total=3012000,
    include_fx_key_open=True,
    include_fx_key_close=True,
):
    """20260716 운영 수치 기반 Open/Close pair."""
    open_ks = {
        "ord_psbl_frcr_amt": 556,
        "tot_evlu_amt_krw": open_krw_total,
        "krw_cash": 0,
        "ovrs_rlzt_pfls_amt": 0,
    }
    close_ks = {
        "ord_psbl_frcr_amt": 361,
        "tot_evlu_amt_krw": close_krw_total,
        "krw_cash": 0,
        "ovrs_rlzt_pfls_amt": 0,
    }
    if include_fx_key_open:
        open_ks["bass_exrt"] = open_fx
    if include_fx_key_close:
        close_ks["bass_exrt"] = close_fx

    open_b = {
        "date": TD,
        "trade_date": TD,
        "type": "open",
        "timestamp": "2026-07-16T22:45:00+09:00",
        "base_currency": "USD",
        "total_asset_usd": 1631.54,
        "available_cash_usd": 556.00,
        "holdings_value_usd": 1075.54,
        "financial_values_valid": True,
        "return_calculation_usable": True,
        "usd_components_consistent": True,
        "currency_status": "explicit",
        "holdings_detail": [
            {"ticker": "MU", "qty": 1, "value": 1075.54, "currency": "USD"}
        ],
        "kis_summary": open_ks,
    }
    close_b = {
        **open_b,
        "type": "close",
        "timestamp": "2026-07-17T06:00:00+09:00",
        "total_asset_usd": 2024.16,
        "available_cash_usd": 361.00,
        "holdings_value_usd": 1663.16,
        "holdings_detail": [
            {"ticker": "MU", "qty": 1, "value": 1663.16, "currency": "USD"}
        ],
        "kis_summary": close_ks,
    }
    return open_b, close_b


@pytest.fixture()
def balance_dir(tmp_path, monkeypatch):
    d = tmp_path / "daily_balances"
    d.mkdir()
    monkeypatch.setattr(im, "BALANCE_STORAGE_PATH", d)
    return d


@pytest.fixture()
def discord(monkeypatch):
    sent = {"embeds": [], "contents": []}

    def _send(content=None, embeds=None, **kw):
        if content:
            sent["contents"].append(content)
        if embeds:
            sent["embeds"].extend(embeds)

    monkeypatch.setattr(im, "WEBHOOK_URL", "https://discord.com/api/webhooks/1/x")
    monkeypatch.setattr(im, "is_valid_webhook", lambda url: True)
    monkeypatch.setattr(im, "send_discord_message", _send)
    im._last_sent.clear()
    return sent


class TestCompareBalancesFxNoneSafe:
    def test_open_fx_ok_close_fx_none_no_typeerror(self):
        open_b, close_b = _usd_pair(close_fx=None)
        result = im.compare_balances(open_b, close_b)
        assert result != {}
        assert result["analysis_success"] is True
        assert result["return_metrics_available"] is True
        assert result["total_change"] == 392.62
        assert result["cash_change"] == -195.0
        assert result["holdings_change"] == 587.62
        assert result["daily_return_pct"] == pytest.approx(24.06, abs=0.01)
        assert result["trading_pnl_krw"] is None
        assert result["fx_impact_krw"] is None
        assert result["close_fx"] is None
        assert result["fx_metrics_available"] is False
        assert result["fx_calculation_status"] == "FX_RATE_INCOMPLETE"
        assert result.get("analysis_error") is None

    def test_open_fx_none_close_fx_ok(self):
        open_b, close_b = _usd_pair(open_fx=None)
        result = im.compare_balances(open_b, close_b)
        assert result["analysis_success"] is True
        assert result["total_change"] == 392.62
        assert result["open_fx"] is None
        assert result["fx_metrics_available"] is False
        assert result["trading_pnl_krw"] is None

    def test_both_fx_none(self):
        open_b, close_b = _usd_pair(open_fx=None, close_fx=None)
        result = im.compare_balances(open_b, close_b)
        assert result["analysis_success"] is True
        assert result["return_metrics_available"] is True
        assert result["fx_metrics_available"] is False
        assert result["total_change"] == 392.62

    def test_core_usd_expected_values(self):
        open_b, close_b = _usd_pair()
        result = im.compare_balances(open_b, close_b)
        assert result["total_change"] == 392.62
        assert result["cash_change"] == -195.0
        assert result["holdings_change"] == 587.62
        assert result["daily_return_pct"] == pytest.approx(24.06, abs=0.01)
        assert result["fx_metrics_available"] is True

    def test_optional_fx_failure_preserves_core_usd(self, monkeypatch):
        open_b, close_b = _usd_pair()

        def _boom(*a, **k):
            raise RuntimeError("fx boom")

        monkeypatch.setattr(im, "_optional_fx_krw_analysis", _boom)
        result = im.compare_balances(open_b, close_b)
        assert result["analysis_success"] is True
        assert result["total_change"] == 392.62
        assert result["fx_metrics_available"] is False
        assert result["trading_pnl_krw"] is None

    def test_exception_returns_structured_not_empty(self, monkeypatch):
        def _boom(*a, **k):
            raise TypeError("NoneType - float")

        monkeypatch.setattr(im, "_canonical_usd_components", _boom)
        result = im.compare_balances({"trade_date": TD}, {"trade_date": TD})
        assert result != {}
        assert result["analysis_success"] is False
        assert result["return_metrics_available"] is False
        assert result["total_change"] is None
        assert result["analysis_error"]["code"] == "DAILY_BALANCE_ANALYSIS_ERROR"
        assert result["analysis_error"]["type"] == "TypeError"
        assert result["status_code"] == "DAILY_BALANCE_ANALYSIS_INCOMPLETE"


class TestCompletePromotionGuards:
    def test_empty_raw_cmp_blocks_complete(self):
        out = im.build_complete_daily_summary({}, {}, {})
        assert out["summary_status"] == "PARTIAL"
        assert out["return_metrics_available"] is False
        assert not im.comparison_supports_complete_summary({})

    def test_analysis_success_false_blocks_complete(self):
        raw = {
            "analysis_success": False,
            "return_metrics_available": False,
            "total_change": None,
            "cash_change": None,
            "holdings_change": None,
            "daily_return_pct": None,
            "analysis_error": {"code": "DAILY_BALANCE_ANALYSIS_ERROR", "type": "TypeError", "message": "x"},
        }
        assert im.comparison_supports_complete_summary(raw) is False
        out = im.build_complete_daily_summary({}, {}, raw)
        assert out["summary_status"] == "PARTIAL"
        assert out["return_metrics_available"] is False

    def test_total_change_none_blocks_return_metrics(self):
        raw = {
            "analysis_success": True,
            "return_metrics_available": True,
            "total_change": None,
            "cash_change": -195.0,
            "holdings_change": 587.62,
            "daily_return_pct": 24.06,
        }
        assert im.comparison_supports_complete_summary(raw) is False
        out = im.build_complete_daily_summary({}, {}, raw)
        assert out["summary_status"] != "COMPLETE"
        assert out["return_metrics_available"] is False

    def test_pair_ok_but_compare_fail_is_partial(self):
        open_b, close_b = _usd_pair()
        open_vals = {"total": 1631.54, "cash": 556.0, "hv": 1075.54}
        close_vals = {"total": 2024.16, "cash": 361.0, "hv": 1663.16}
        assert im.pair_supports_complete_summary(
            open_b, close_b, open_vals=open_vals, close_vals=close_vals
        )
        failed = im._compare_balances_failure(TypeError("NoneType - float"), trade_date=TD)
        out = im.build_complete_daily_summary(open_b, close_b, failed)
        assert out["summary_status"] == "PARTIAL"
        assert out["return_metrics_available"] is False

    def test_cash_flow_incomplete_keeps_asset_metrics(self):
        open_b, close_b = _usd_pair()
        raw = im.compare_balances(open_b, close_b)
        out = im.build_complete_daily_summary(open_b, close_b, raw)
        assert out["return_calculation_status"] == "CASH_FLOW_EVIDENCE_INCOMPLETE"
        assert out["total_change"] == 392.62
        assert out["cash_change"] == -195.0
        assert out["holdings_change"] == 587.62
        assert out["investment_return_pct"] is None
        assert out["daily_asset_pnl"] is None
        assert out["return_metrics_available"] is True
        assert out["summary_status"] == "COMPLETE"  # FX complete

    def test_fx_incomplete_is_partial_with_metrics(self):
        open_b, close_b = _usd_pair(close_fx=None)
        raw = im.compare_balances(open_b, close_b)
        out = im.build_complete_daily_summary(open_b, close_b, raw)
        assert out["summary_status"] == "PARTIAL"
        assert out["return_metrics_available"] is True
        assert out["fx_metrics_available"] is False
        assert out["total_change"] == 392.62
        assert out["investment_return_pct"] is None
        codes = {f["code"] for f in out["data_quality_findings"]}
        assert "CASH_FLOW_EVIDENCE_INCOMPLETE" in codes
        assert "FX_RATE_INCOMPLETE" in codes
        assert "DAILY_BALANCE_ANALYSIS_ERROR" not in codes


class TestSummaryInvariants:
    def test_invariant_blocks_complete_with_null_total_change(self):
        bad = {
            "summary_status": "COMPLETE",
            "return_metrics_available": True,
            "total_change": None,
            "total_change_pct": 24.06,
            "analysis_success": True,
            "data_quality_findings": [],
        }
        out = im.enforce_summary_state_invariants(bad)
        assert out["summary_status"] == "PARTIAL"
        assert out["return_metrics_available"] is False
        assert out["status_code"] == "SUMMARY_STATE_INVARIANT_VIOLATION"
        assert out["data_quality_findings"][-1]["severity"] == "WARNING"

    def test_invariant_blocks_return_metrics_without_pct(self):
        bad = {
            "summary_status": "PARTIAL",
            "return_metrics_available": True,
            "total_change": 392.62,
            "total_change_pct": None,
            "data_quality_findings": [],
        }
        out = im.enforce_summary_state_invariants(bad)
        assert out["return_metrics_available"] is False
        assert out["status_code"] == "SUMMARY_STATE_INVARIANT_VIOLATION"


class TestEndToEnd20260716:
    def test_fx_null_summary_partial_exit_ok(self, balance_dir, discord, caplog, monkeypatch):
        open_b, close_b = _usd_pair(close_fx=None, close_krw_total=None)
        (balance_dir / f"balance_open_{CLOSE_KST}.json").write_text(
            json.dumps(open_b), encoding="utf-8"
        )
        (balance_dir / f"balance_close_{TD}.json").write_text(
            json.dumps(close_b), encoding="utf-8"
        )
        # summary 파일의 FX 폴백이 테스트 의을 가리지 않도록
        monkeypatch.setattr(im, "_load_summary_row_from_path", lambda p: {})

        with caplog.at_level("INFO", logger="IntegratedManager"):
            result = im.send_daily_trading_summary(target_trade_date=TD)

        assert result["ok"] is True
        assert result["summary_status"] == "PARTIAL"
        assert result["return_metrics_available"] is True
        analysis = result["analysis"]
        assert analysis["total_change"] == 392.62
        assert analysis["cash_change"] == -195.0
        assert analysis["holdings_change"] == 587.62
        assert analysis["total_change_pct"] == pytest.approx(24.06, abs=0.01)
        assert analysis["trading_pnl_krw"] is None
        assert analysis["fx_impact_krw"] is None
        assert analysis["investment_return_pct"] is None
        assert analysis["daily_asset_pnl"] is None
        assert analysis["return_calculation_status"] == "CASH_FLOW_EVIDENCE_INCOMPLETE"
        assert analysis["fx_calculation_status"] == "FX_RATE_INCOMPLETE"
        assert "DAILY_BALANCE_FX_RATE_INCOMPLETE" in caplog.text
        assert "DAILY_BALANCE_COMPARISON_COMPLETE" in caplog.text
        assert "DAILY_SUMMARY_PARTIAL" in caplog.text
        assert "잔액 비교 분석 실패" not in caplog.text
        assert not any(
            r.levelname == "ERROR" and "잔액 비교" in r.getMessage()
            for r in caplog.records
        )
        # Summary 전송 성공 → CLI exit 0 정책과 동일
        assert result["summary_status"] in {"PARTIAL", "COMPLETE", "OK"}
        assert len(discord["embeds"]) == 1
