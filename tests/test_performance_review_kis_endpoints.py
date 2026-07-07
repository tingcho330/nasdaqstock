"""KIS 해외주식 performance_review endpoint 검증 테스트."""

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

from performance_review import (  # noqa: E402
    PerformanceReviewResult,
    ReviewArtifacts,
    build_kis_endpoint_review,
    collect_artifacts,
    kis_review_to_json_summary,
    load_order_reconcile_evidences_for_period,
    render_markdown,
    review_balance,
    review_ccnl,
    review_present_balance,
    review_orders,
    KisNccsReview,
    KisBalanceReview,
)


def _artifacts(tmp_path: Path, review_date: str = "20260707", **kwargs) -> ReviewArtifacts:
    market = kwargs.get("market", "SP500")
    art = ReviewArtifacts(
        market=market,
        start_date=kwargs.get("start_date", review_date),
        review_date=review_date,
        period=kwargs.get("period", "daily"),
        session=kwargs.get("session"),
        output_dir=tmp_path,
        db_path=tmp_path / "trading_data.db",
        account_snapshot_paths=sorted(tmp_path.glob("account_snapshot_*.json")),
        order_reconcile_paths=sorted(tmp_path.glob("order_reconcile_*.json")),
        trade_rows=kwargs.get("trade_rows", []),
        logs_text=kwargs.get("logs_text", ""),
    )
    return art


def _write_snap(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _base_snap(trade_date: str = "20260707", **ep_overrides) -> dict:
    balance = {
        "endpoint": "/uapi/overseas-stock/v1/trading/inquire-balance",
        "expected_tr_ids": ["TTTS3012R"],
        "observed_tr_ids": ["TTTS3012R"],
        "exchange_coverage": ["NASD", "NYSE", "AMEX"],
        "status_by_exchange": {"NASD": "OK", "NYSE": "EMPTY", "AMEX": "EMPTY"},
        "row_count_by_exchange": {"NASD": 2, "NYSE": 0, "AMEX": 0},
    }
    present = {
        "endpoint": "/uapi/overseas-stock/v1/trading/inquire-present-balance",
        "expected_tr_ids": ["CTRP6504R"],
        "observed_tr_ids": ["CTRP6504R"],
        "call_count": 1,
        "krw_aux_call_count": 0,
        "status": "OK",
        "available_cash_usd": 1000.0,
        "total_asset_usd": 6000.0,
        "available_cash_krw": 1300000.0,
        "total_asset_krw": 7800000.0,
    }
    nccs = {
        "endpoint": "/uapi/overseas-stock/v1/trading/inquire-nccs",
        "expected_tr_ids": ["TTTS3018R"],
        "observed_tr_ids": ["TTTS3018R"],
        "exchange_coverage": ["NASD", "NYSE", "AMEX"],
        "status_by_exchange": {"NASD": "EMPTY", "NYSE": "EMPTY", "AMEX": "EMPTY"},
        "open_orders_count": 0,
        "all_exchanges_failed": False,
    }
    present.update(ep_overrides.get("present_balance", {}))
    balance.update(ep_overrides.get("balance", {}))
    return {
        "schema_version": "1.0",
        "source": "kis_endpoint",
        "market": "SP500",
        "trade_date": trade_date,
        "snapshot_ts_kst": f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}T10:00:00+09:00",
        "generated_at_kst": f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}T10:00:00+09:00",
        "pipeline_context_source": "resolve_pipeline_context",
        "valid": True,
        "endpoint_evidence": {"balance": balance, "present_balance": present, "nccs": nccs},
        "exchange_coverage": balance["exchange_coverage"],
        "holding_exchange_coverage": ["NASD"],
        "available_cash_usd": present.get("available_cash_usd", 1000.0),
        "holdings_value_usd": 5000.0,
        "total_asset_usd": present.get("total_asset_usd", 6000.0),
        "available_cash_krw": 1300000.0,
        "total_asset_krw": 7800000.0,
        "sellable_qty_by_ticker": {"AAPL": 10},
    }


def _finding_titles(findings) -> set:
    return {f.title for f in findings}


def test_present_balance_call_count_one_no_duplicate(tmp_path):
    snap = _base_snap()
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = _artifacts(tmp_path)
    bal = review_balance(art, strict=True)
    pr = review_present_balance(art, bal, strict=True)
    assert "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP" not in _finding_titles(pr.findings)


def test_present_balance_call_count_three_duplicated_critical(tmp_path):
    snap = _base_snap(present_balance={"call_count": 3, "total_asset_usd": 12000.0})
    snap["total_asset_usd"] = 12000.0
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = _artifacts(tmp_path)
    bal = review_balance(art, strict=True)
    pr = review_present_balance(art, bal, strict=True)
    titles = _finding_titles(pr.findings)
    assert "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP" in titles
    assert any(f.severity == "CRITICAL" for f in pr.findings if f.title == "KIS_PRESENT_BALANCE_DUPLICATED_BY_EXCHANGE_LOOP")


def test_present_balance_evidence_missing_warn(tmp_path):
    snap = _base_snap()
    del snap["endpoint_evidence"]["present_balance"]["call_count"]
    snap["endpoint_evidence"]["present_balance"] = {"status": "OK"}
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = _artifacts(tmp_path)
    bal = review_balance(art, strict=True)
    pr = review_present_balance(art, bal, strict=True)
    assert "KIS_PRESENT_BALANCE_EVIDENCE_MISSING" in _finding_titles(pr.findings)


def test_present_balance_currency_mixed(tmp_path):
    snap = _base_snap(present_balance={"call_count": 1, "total_asset_usd": 60000.0})
    snap["total_asset_usd"] = 60000.0
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = _artifacts(tmp_path)
    bal = review_balance(art, strict=True)
    pr = review_present_balance(art, bal, strict=True)
    assert "KIS_PRESENT_BALANCE_CURRENCY_MIXED" in _finding_titles(pr.findings)


def test_balance_ok_empty_no_partial_coverage(tmp_path):
    snap = _base_snap()
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = _artifacts(tmp_path)
    bal = review_balance(art, strict=True)
    assert "KIS_BALANCE_PARTIAL_EXCHANGE_COVERAGE" not in _finding_titles(bal.findings)


def test_balance_row_count_zero_not_missing(tmp_path):
    snap = _base_snap()
    snap["endpoint_evidence"]["balance"]["row_count_by_exchange"] = {"NASD": 2, "NYSE": 0, "AMEX": 0}
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = _artifacts(tmp_path)
    bal = review_balance(art, strict=True)
    assert "KIS_BALANCE_PARTIAL_EXCHANGE_COVERAGE" not in _finding_titles(bal.findings)


def test_snapshot_trade_date_mismatch(tmp_path):
    snap = _base_snap(trade_date="20260701")
    snap["snapshot_ts_kst"] = "2026-07-07T10:00:00+09:00"
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = _artifacts(tmp_path, review_date="20260707")
    bal = review_balance(art, strict=True)
    assert "ACCOUNT_SNAPSHOT_DATE_MISMATCH" in _finding_titles(bal.findings)


def test_latest_fallback_warn(tmp_path):
    snap = _base_snap(trade_date="20260701")
    _write_snap(tmp_path, "account_snapshot_latest_SP500.json", snap)
    art = _artifacts(tmp_path, review_date="20260707")
    art.account_snapshot_paths = sorted(tmp_path.glob("account_snapshot_*.json"))
    bal = review_balance(art, strict=True)
    titles = _finding_titles(bal.findings)
    assert "ACCOUNT_SNAPSHOT_DATE_MISMATCH" in titles
    assert any(f.latest_fallback_used for f in bal.findings if f.title == "ACCOUNT_SNAPSHOT_DATE_MISMATCH")


def test_weekly_collects_all_order_reconcile_files(tmp_path):
    for d in ("20260705", "20260706", "20260707"):
        payload = {
            "trade_date": d,
            "evidence_trade_date": d,
            "generated_at_kst": f"2026-07-0{d[-1]}T18:00:00+09:00",
            "ccnl": {"query_start_date": d, "query_end_date": d, "order_count": 0, "status": "EMPTY"},
            "nccs": {"status": "EMPTY", "all_exchanges_failed": False},
        }
        _write_snap(tmp_path, f"order_reconcile_SP500_{d}.json", payload)
    art = _artifacts(tmp_path, review_date="20260707", start_date="20260705", period="weekly")
    art.order_reconcile_paths = sorted(tmp_path.glob("order_reconcile_*.json"))
    sels = load_order_reconcile_evidences_for_period(art)
    assert len(sels) == 3


def test_ccnl_period_coverage_incomplete(tmp_path):
    payload = {
        "trade_date": "20260707",
        "ccnl": {"query_start_date": "20260707", "query_end_date": "20260707", "order_count": 0},
        "nccs": {"status": "EMPTY"},
    }
    _write_snap(tmp_path, "order_reconcile_SP500_20260707.json", payload)
    art = _artifacts(
        tmp_path,
        review_date="20260707",
        start_date="20260705",
        period="weekly",
        trade_rows=[{"timestamp": "2026-07-06 10:00:00", "order_id": "X1", "order_status": "executed", "action": "BUY"}],
    )
    art.order_reconcile_paths = sorted(tmp_path.glob("order_reconcile_*.json"))
    cr = review_ccnl(art, strict=True)
    assert "KIS_CCNL_PERIOD_COVERAGE_INCOMPLETE" in _finding_titles(cr.findings)


def test_ccnl_coverage_incomplete_no_db_mismatch(tmp_path):
    payload = {
        "trade_date": "20260707",
        "ccnl": {"query_start_date": "20260707", "query_end_date": "20260707", "order_count": 0, "status": "EMPTY"},
        "nccs": {"status": "EMPTY", "all_exchanges_failed": False},
    }
    _write_snap(tmp_path, "order_reconcile_SP500_20260707.json", payload)
    art = _artifacts(
        tmp_path,
        trade_rows=[{"timestamp": "2026-07-06 10:00:00", "order_id": "ORD1", "order_status": "executed", "action": "BUY"}],
    )
    art.order_reconcile_paths = sorted(tmp_path.glob("order_reconcile_*.json"))
    cr = review_ccnl(art, strict=True)
    assert "KIS_DB_CCNL_STATUS_MISMATCH" not in _finding_titles(cr.findings)


def test_executed_without_ccnl_unverified_not_without_fill(tmp_path):
    art = _artifacts(
        tmp_path,
        trade_rows=[{"timestamp": "2026-07-07 10:00:00", "order_id": "E1", "order_status": "executed", "executed_qty": 1, "action": "BUY"}],
    )
    cr = review_ccnl(art, strict=True)
    titles = _finding_titles(cr.findings)
    assert "KIS_EXECUTED_FILL_UNVERIFIED" in titles
    assert "KIS_EXECUTED_WITHOUT_FILL" not in titles


def test_executed_with_ccnl_zero_fill_error(tmp_path):
    payload = {
        "trade_date": "20260707",
        "ccnl": {
            "query_start_date": "20260707",
            "query_end_date": "20260707",
            "order_count": 1,
            "status": "OK",
            "all_exchanges_failed": False,
        },
        "nccs": {"status": "EMPTY"},
        "ccnl_orders": [{"order_id": "E2", "executed_qty": 0, "status": "pending"}],
    }
    _write_snap(tmp_path, "order_reconcile_SP500_20260707.json", payload)
    art = _artifacts(
        tmp_path,
        trade_rows=[{"timestamp": "2026-07-07 10:00:00", "order_id": "E2", "order_status": "executed", "executed_qty": 1, "action": "BUY"}],
    )
    art.order_reconcile_paths = sorted(tmp_path.glob("order_reconcile_*.json"))
    cr = review_ccnl(art, strict=True)
    assert "KIS_EXECUTED_WITHOUT_FILL" in _finding_titles(cr.findings)


def test_sellable_checked_skips_without_check(tmp_path):
    art = _artifacts(
        tmp_path,
        trade_rows=[{
            "timestamp": "2026-07-07 10:00:00",
            "order_id": "S1",
            "order_status": "executed",
            "action": "SELL",
            "ticker": "AAPL",
            "quantity": 1,
            "structured_context": json.dumps({"sellable_qty_checked": True, "sellable_qty": 5}),
        }],
    )
    nccs = KisNccsReview()
    bal = KisBalanceReview()
    orr = review_orders(art, nccs, bal, strict=True, cfg={})
    assert "KIS_SELL_WITHOUT_SELLABLE_CHECK" not in _finding_titles(orr.findings)


def test_zero_sellable_sell_critical(tmp_path):
    art = _artifacts(
        tmp_path,
        trade_rows=[{
            "timestamp": "2026-07-07 10:00:00",
            "order_id": "S2",
            "order_status": "executed",
            "action": "SELL",
            "ticker": "AAPL",
            "quantity": 1,
            "structured_context": json.dumps({
                "sellable_qty_checked": True,
                "sellable_qty": 0,
                "clamp_action": "none",
            }),
        }],
    )
    nccs = KisNccsReview()
    bal = KisBalanceReview(total_holding_qty_by_ticker={"AAPL": 10})
    orr = review_orders(art, nccs, bal, strict=True, cfg={"critical_on_sell_sent_with_zero_sellable_qty": True})
    titles = _finding_titles(orr.findings)
    assert "KIS_SELL_SENT_WITH_ZERO_SELLABLE_QTY" in titles
    assert any(f.severity == "CRITICAL" for f in orr.findings if f.title == "KIS_SELL_SENT_WITH_ZERO_SELLABLE_QTY")


def test_all_findings_have_category(tmp_path):
    snap = _base_snap()
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = collect_artifacts("SP500", "20260707", "20260707", None, tmp_path)
    ker = build_kis_endpoint_review(art, strict=True, cfg={}, include_logs=False)
    for f in ker.endpoint_findings:
        assert f.category, f"missing category for {f.title}"


def test_json_markdown_evidence_fields(tmp_path):
    snap = _base_snap()
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = collect_artifacts("SP500", "20260707", "20260707", None, tmp_path)
    ker = build_kis_endpoint_review(art, strict=False, cfg={}, include_logs=False)
    summary = kis_review_to_json_summary(ker)
    for key in ("balance", "present_balance", "nccs", "ccnl"):
        assert "evidence_source_file" in summary[key]
    result = PerformanceReviewResult(
        context={"market": "SP500", "period": "daily", "start_date": "20260707", "end_date": "20260707"},
        kis_endpoint_review=ker,
        findings=ker.endpoint_findings,
        summary_text="test",
    )
    md = render_markdown(result)
    assert "evidence_trade_date" in md
    assert "evidence_generated_at" in md
    for f in ker.endpoint_findings:
        d = f.to_dict()
        assert "evidence_source_file" in d
        assert "category" in d


def test_no_sensitive_info_in_output(tmp_path):
    snap = _base_snap()
    snap["endpoint_evidence"]["balance"]["cano"] = "1234567890"
    snap["appkey"] = "SECRETKEY"
    _write_snap(tmp_path, "account_snapshot_SP500_20260707.json", snap)
    art = collect_artifacts("SP500", "20260707", "20260707", None, tmp_path)
    ker = build_kis_endpoint_review(art, strict=False, cfg={}, include_logs=False)
    summary = kis_review_to_json_summary(ker)
    blob = json.dumps(summary)
    assert "SECRETKEY" not in blob
    assert "1234567890" not in blob
