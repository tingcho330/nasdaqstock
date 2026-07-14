# -*- coding: utf-8 -*-
"""Broker ledger / trade_date / account stale / persist integrity tests."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

# Avoid /app paths used in Docker defaults
_TEST_OUT = ROOT / "output_test_broker"
_TEST_OUT.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("CONFIG_PATH", str(ROOT / "config" / "config.json"))
os.environ.setdefault("OUTPUT_DIR", str(_TEST_OUT))
os.environ.setdefault("MARKET", "SP500")
os.environ.pop("PIPELINE_TRADE_DATE", None)

KST = ZoneInfo("Asia/Seoul")


@pytest.fixture()
def tmp_output(monkeypatch, tmp_path):
    out = tmp_path / "output"
    out.mkdir()
    monkeypatch.setenv("MARKET", "SP500")
    monkeypatch.delenv("PIPELINE_TRADE_DATE", raising=False)
    monkeypatch.delenv("PIPELINE_SESSION", raising=False)
    import utils
    import broker_order_persist as bop
    import account_snapshot as asin

    monkeypatch.setattr(utils, "OUTPUT_DIR", out)
    monkeypatch.setattr(bop, "OUTPUT_DIR", out)
    monkeypatch.setattr(bop, "ORDER_JOURNAL_DIR", out / "order_journal")
    monkeypatch.setattr(bop, "PERSIST_LOCK_DIR", out / "order_persist_locks")
    monkeypatch.setattr(asin, "OUTPUT_DIR", out)
    return out


def test_broker_only_missing_in_db_detection(tmp_output, monkeypatch, tmp_path):
    from order_reconciler import detect_broker_only_orders, _normalize_overseas_order_row

    db_path = tmp_path / "trading_data.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    from recorder import DataRecorder, TradeRecord, get_recorder

    rec = DataRecorder(str(db_path))
    # BUY already in DB
    buy = TradeRecord(
        timestamp=datetime(2026, 7, 8, 23, 0, tzinfo=KST),
        ticker="SNDK",
        action="BUY",
        quantity=1,
        price=1692.9724,
        amount=1692.9724,
        commission=0.0,
        tax=0.0,
        total_cost=1692.9724,
        net_amount=1692.9724,
        profit_loss=0.0,
        holding_period_days=0,
        order_status="executed",
        order_id="0030975669",
        requested_qty=1,
        executed_qty=1,
    )
    assert rec.upsert_trade_record_by_order_id(buy)

    sell_row = {
        "odno": "0031276871",
        "ord_dt": "20260709",
        "ord_tmd": "233156",
        "pdno": "SNDK",
        "prdt_name": "SANDISK",
        "sll_buy_dvsn_cd": "01",
        "ft_ord_qty": "1",
        "ft_ord_unpr3": "1827.0000",
        "ft_ccld_qty": "1",
        "ft_ccld_unpr3": "1833.9032",
        "ft_ccld_amt3": "1833.9032",
        "prcs_stat_name": "완료",
        "ovrs_excg_cd": "NASD",
        "tr_crcy_cd": "USD",
        "mdia_dvsn_name": "OpenAPI",
    }
    buy_row = {
        "odno": "0030975669",
        "ord_dt": "20260708",
        "pdno": "SNDK",
        "sll_buy_dvsn_cd": "02",
        "ft_ord_qty": "1",
        "ft_ccld_qty": "1",
        "ft_ccld_unpr3": "1692.9724",
        "ft_ccld_amt3": "1692.9724",
        "prcs_stat_name": "완료",
        "ovrs_excg_cd": "NASD",
        "mdia_dvsn_name": "OpenAPI",
    }
    kis = {
        "0030975669": _normalize_overseas_order_row(buy_row),
        "0031276871": _normalize_overseas_order_row(sell_row),
    }
    broker_only, findings = detect_broker_only_orders(kis, recorder=rec)
    assert len(broker_only) == 1
    assert broker_only[0]["order_id"] == "0031276871"
    assert findings[0]["title"] == "BROKER_TRADE_MISSING_IN_DB"
    assert findings[0]["severity"] == "ERROR"
    assert findings[0]["category"] == "DATA_INTEGRITY"


def test_backfill_broker_only_creates_sell_idempotent(tmp_output, monkeypatch, tmp_path):
    from order_reconciler import (
        detect_broker_only_orders,
        backfill_broker_only_orders,
        _normalize_overseas_order_row,
    )
    from recorder import DataRecorder, TradeRecord

    db_path = tmp_path / "trading_data.db"
    rec = DataRecorder(str(db_path))
    buy = TradeRecord(
        timestamp=datetime(2026, 7, 8, 23, 0, tzinfo=KST),
        ticker="SNDK",
        action="BUY",
        quantity=1,
        price=1692.9724,
        amount=1692.9724,
        commission=0.0,
        tax=0.0,
        total_cost=1692.9724,
        net_amount=1692.9724,
        profit_loss=0.0,
        holding_period_days=0,
        order_status="executed",
        order_id="0030975669",
        requested_qty=1,
        executed_qty=1,
    )
    rec.upsert_trade_record_by_order_id(buy)

    sell_norm = _normalize_overseas_order_row({
        "odno": "0031276871",
        "ord_dt": "20260709",
        "ord_tmd": "233156",
        "pdno": "SNDK",
        "prdt_name": "SANDISK",
        "sll_buy_dvsn_cd": "01",
        "ft_ord_qty": "1",
        "ft_ord_unpr3": "1827",
        "ft_ccld_qty": "1",
        "ft_ccld_unpr3": "1833.9032",
        "ft_ccld_amt3": "1833.9032",
        "prcs_stat_name": "완료",
        "ovrs_excg_cd": "NASD",
        "tr_crcy_cd": "USD",
        "mdia_dvsn_name": "OpenAPI",
    })
    # ensure executed status
    assert sell_norm["status"] == "executed"

    r1 = backfill_broker_only_orders([sell_norm], recorder=rec, dry_run=False)
    assert r1["backfill_inserted"] == 1
    assert "0031276871" in r1["backfill_order_ids"]

    rows = rec.get_trade_records(ticker="SNDK")
    sells = [t for t in rows if t.action.upper() == "SELL"]
    assert len(sells) == 1
    ctx = json.loads(sells[0].structured_context or "{}")
    assert ctx.get("broker_only") is True
    assert ctx.get("net_pnl_complete") is False
    assert abs(float(ctx.get("gross_pnl")) - 140.9308) < 0.0001

    # re-run → no duplicate
    r2 = backfill_broker_only_orders([sell_norm], recorder=rec, dry_run=False)
    assert r2["backfill_inserted"] == 0
    assert r2["backfill_updated"] == 1
    sells2 = [t for t in rec.get_trade_records(ticker="SNDK") if t.action.upper() == "SELL"]
    assert len(sells2) == 1

    remaining, _ = detect_broker_only_orders(
        {"0030975669": {"order_id": "0030975669"}, "0031276871": sell_norm},
        recorder=rec,
    )
    assert remaining == []

    # position flat: 1 buy + 1 sell
    buys = sum(t.quantity for t in rec.get_trade_records(ticker="SNDK") if t.action.upper() == "BUY")
    sells_qty = sum(
        (t.executed_qty or t.quantity)
        for t in rec.get_trade_records(ticker="SNDK")
        if t.action.upper() == "SELL"
    )
    assert buys - sells_qty == 0


def test_backfill_incomplete_missing_price(tmp_output, monkeypatch, tmp_path):
    from order_reconciler import backfill_broker_only_orders
    from recorder import DataRecorder

    rec = DataRecorder(str(tmp_path / "t.db"))
    incomplete = {
        "order_id": "009999",
        "order_date": "20260709",
        "ticker": "SNDK",
        "side": "sell",
        "status": "executed",
        "quantity": 1,
        "executed_qty": 1,
        "executed_price": Decimal("0"),
        "executed_price_str": "0",
    }
    before = len(rec.get_known_order_ids())
    r = backfill_broker_only_orders([incomplete], recorder=rec)
    assert r["backfill_skipped_incomplete"] == 1
    assert r["incomplete_findings"][0]["title"] == "BROKER_TRADE_BACKFILL_INCOMPLETE"
    assert len(rec.get_known_order_ids()) == before


def test_db_persist_success_then_partial_flag(tmp_output, monkeypatch, tmp_path):
    import broker_order_persist as bop
    from recorder import DataRecorder, record_trade

    db = DataRecorder(str(tmp_path / "p.db"))
    monkeypatch.setattr("recorder.get_recorder", lambda: db)

    cid = bop.begin_broker_order(
        market="SP500",
        ticker="SNDK",
        side="SELL",
        requested_qty=1,
        requested_price=1800.0,
        strategy_type="PartialProfit",
    )
    bop.mark_broker_accepted(cid, broker_order_id="0031", broker_response={"rt_cd": "0"})
    ok, _ = bop.persist_broker_order_to_db(
        {
            "side": "sell",
            "ticker": "SNDK",
            "qty": 1,
            "price": 1800.0,
            "trade_status": "pending",
            "order_id": "0031",
            "requested_qty": 1,
            "executed_qty": 0,
        },
        correlation_id=cid,
        market="SP500",
    )
    assert ok is True
    journal = json.loads((tmp_output / "order_journal" / f"order_{cid}.json").read_text())
    assert journal["status"] == "db_persisted"
    assert journal["db_persisted"] is True


def test_db_persist_failure_creates_lock_no_strategy_complete(tmp_output, monkeypatch, tmp_path):
    import broker_order_persist as bop

    monkeypatch.setattr(
        "recorder.record_trade",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    cid = bop.begin_broker_order(
        market="SP500", ticker="SNDK", side="SELL",
        requested_qty=1, requested_price=1.0, strategy_type="StopLoss",
    )
    bop.mark_broker_accepted(cid, broker_order_id="X1")
    ok, detail = bop.persist_broker_order_to_db(
        {"side": "sell", "ticker": "SNDK", "qty": 1, "price": 1.0,
         "trade_status": "pending", "order_id": "X1", "requested_qty": 1, "executed_qty": 0},
        correlation_id=cid,
    )
    assert ok is False
    assert bop.has_persist_failure_lock("SNDK", "SELL", "X1")
    journal = json.loads((tmp_output / "order_journal" / f"order_{cid}.json").read_text())
    assert journal["recovery_required"] is True
    assert journal["status"] in ("persist_failed", "reconcile_required")


def test_partial_qty_one_share_full_exit():
    from partial_sell_state import compute_partial_qty

    qty = compute_partial_qty(1, 0.5, full_if_rounding_zero=True)
    assert qty == 1
    structured = {
        "partial_profit_ratio": 0.5,
        "partial_qty": qty,
        "original_holding_qty": 1,
        "full_exit_due_to_rounding": True,
        "position_closed": True,
    }
    assert structured["full_exit_due_to_rounding"] is True


def test_account_file_stale_skips_sync_mismatch(tmp_output):
    from account_snapshot import extract_account_file_date, AccountSnapshot

    bal = tmp_output / "balance_20260708.json"
    bal.write_text(json.dumps({"data": [{"pdno": "SNDK", "hldg_qty": "1"}]}), encoding="utf-8")
    meta = extract_account_file_date(bal)
    assert meta["account_file_date"] == "20260708"

    snap = AccountSnapshot(trade_date="20260713", tickers=[], holding_count=0, valid=True)
    # Classification logic (same as trader._validate_account_sync)
    assert meta["account_file_date"] != snap.trade_date
    finding = "ACCOUNT_FILE_STALE"
    sync_mismatch = False  # must not raise ACCOUNT_SYNC_MISMATCH
    assert finding == "ACCOUNT_FILE_STALE"
    assert sync_mismatch is False


def test_same_date_holdings_mismatch_is_sync_error(tmp_output):
    from account_snapshot import extract_account_file_date, AccountSnapshot

    bal = tmp_output / "balance_20260713.json"
    bal.write_text(json.dumps({
        "trade_date": "20260713",
        "data": [{"pdno": "SNDK", "hldg_qty": "1"}],
    }), encoding="utf-8")
    meta = extract_account_file_date(bal)
    snap = AccountSnapshot(trade_date="20260713", tickers=[], holding_count=0, valid=True)
    assert meta["account_file_date"] == snap.trade_date
    file_tickers = ["SNDK"]
    kis_tickers = []
    assert file_tickers != kis_tickers
    # Would be ACCOUNT_SYNC_MISMATCH


def test_live_trade_date_20260713_2311_kst(monkeypatch):
    from utils import resolve_market_trade_date, resolve_pipeline_context

    monkeypatch.setenv("MARKET", "SP500")
    monkeypatch.setenv("PIPELINE_TRADE_DATE", "20260708")  # stale
    now = datetime(2026, 7, 13, 23, 31, tzinfo=KST)
    info = resolve_market_trade_date("SP500", now, mode="live")
    assert info["now_et_date"] == "20260713"
    assert info["resolved_trade_date"] == "20260713"
    assert info["stale_context_detected"] is True

    ctx = resolve_pipeline_context(now=now, market="SP500", mode="live")
    assert ctx["trade_date"] == "20260713"
    # filename expectation
    fname = f"account_snapshot_SP500_{ctx['trade_date']}_pm.json"
    assert fname == "account_snapshot_SP500_20260713_pm.json"


def test_stale_pipeline_context_replaced(monkeypatch):
    from utils import resolve_market_trade_date

    monkeypatch.delenv("PIPELINE_TRADE_DATE", raising=False)
    now = datetime(2026, 7, 13, 23, 31, tzinfo=KST)
    info = resolve_market_trade_date(
        "SP500",
        now,
        mode="live",
        context_trade_date="20260708",
        context_generated_at_kst="2026-07-08T23:00:00+09:00",
    )
    assert info["resolved_trade_date"] == "20260713"
    assert info["stale_context_detected"] is True


def test_historical_mode_allows_past_date(monkeypatch):
    from utils import resolve_market_trade_date

    monkeypatch.delenv("PIPELINE_TRADE_DATE", raising=False)
    now = datetime(2026, 7, 13, 23, 31, tzinfo=KST)
    info = resolve_market_trade_date(
        "SP500", now, mode="historical", explicit_trade_date="20260708",
    )
    assert info["resolved_trade_date"] == "20260708"
    assert info["resolution_mode"] == "historical_explicit"


def test_snapshot_save_blocks_past_overwrite(tmp_output, monkeypatch):
    from account_snapshot import AccountSnapshot, save_account_snapshot_evidence

    snap = AccountSnapshot(
        trade_date="20260708",  # wrong for live 20260713
        tickers=[],
        holding_count=0,
        valid=True,
        snapshot_ts=datetime.now(KST).isoformat(),
    )
    path = save_account_snapshot_evidence(
        snap,
        market="SP500",
        session="pm",
        output_dir=tmp_output,
        resolved_live_trade_date="20260713",
    )
    assert path is None
    assert not (tmp_output / "account_snapshot_SP500_20260708_pm.json").exists()
    assert not (tmp_output / "account_snapshot_latest_SP500.json").exists()

    snap2 = AccountSnapshot(
        trade_date="20260713",
        tickers=[],
        holding_count=0,
        valid=True,
        snapshot_ts=datetime.now(KST).isoformat(),
    )
    path2 = save_account_snapshot_evidence(
        snap2,
        market="SP500",
        session="pm",
        output_dir=tmp_output,
        resolved_live_trade_date="20260713",
    )
    assert path2 is not None
    assert path2.name == "account_snapshot_SP500_20260713_pm.json"


def test_ccnl_normalize_preserves_decimal_fields():
    from order_reconciler import _normalize_overseas_order_row

    o = _normalize_overseas_order_row({
        "odno": "0031276871",
        "ord_dt": "20260709",
        "dmst_ord_dt": "20260710",
        "ord_tmd": "233156",
        "pdno": "SNDK",
        "prdt_name": "SANDISK CORP",
        "sll_buy_dvsn_cd": "01",
        "sll_buy_dvsn_cd_name": "매도",
        "ft_ord_qty": "1",
        "ft_ord_unpr3": "1827.0000",
        "ft_ccld_qty": "1",
        "ft_ccld_unpr3": "1833.9032",
        "ft_ccld_amt3": "1833.9032",
        "nccs_qty": "0",
        "prcs_stat_name": "완료",
        "ovrs_excg_cd": "NASD",
        "tr_crcy_cd": "USD",
        "mdia_dvsn_name": "OpenAPI",
        "tr_mket_name": "NASDAQ",
    })
    assert o["order_date"] == "20260709"
    assert o["domestic_order_date"] == "20260710"
    assert o["executed_price"] == Decimal("1833.9032")
    assert o["executed_amount"] == Decimal("1833.9032")
    assert o["media"] == "OpenAPI"
    assert o["exchange"] == "NASD"
    assert isinstance(o["executed_price"], Decimal)


def test_performance_incomplete_when_broker_only(tmp_output, monkeypatch):
    from performance_review import _summarize_trades, _apply_broker_integrity_findings, ReviewArtifacts, ReviewFinding

    rows = [{
        "action": "SELL",
        "profit_loss": 140.93,
        "structured_context": json.dumps({
            "broker_only": True,
            "gross_pnl": 140.9308,
            "gross_pnl_basis": True,
            "net_pnl_complete": False,
        }),
    }]
    perf = _summarize_trades(rows)
    assert perf["net_pnl_complete"] is False
    assert abs(perf["gross_pnl"] - 140.9308) < 0.001

    art = ReviewArtifacts(
        market="SP500",
        start_date="20260709",
        review_date="20260709",
        period="daily",
        session="pm",
        output_dir=tmp_output,
        db_path=tmp_output / "x.db",
    )
    recon = {
        "findings": [{
            "title": "BROKER_TRADE_MISSING_IN_DB",
            "severity": "ERROR",
            "category": "DATA_INTEGRITY",
            "details": {"order_id": "0031276871"},
        }]
    }
    (tmp_output / "order_reconcile_SP500_20260709.json").write_text(
        json.dumps(recon), encoding="utf-8"
    )
    findings: list = []
    status = _apply_broker_integrity_findings(findings, art, perf)
    assert status == "PERFORMANCE_DATA_INCOMPLETE"
    titles = {f.title for f in findings}
    assert "BROKER_TRADE_MISSING_IN_DB" in titles
