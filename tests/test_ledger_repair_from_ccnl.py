# -*- coding: utf-8 -*-
"""KIS CCNL ledger repair + performance completeness tests (tmp SQLite, mock CCNL)."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

os.environ.setdefault("CONFIG_PATH", str(ROOT / "config" / "config.json"))
_TEST_OUT = ROOT / "output_test_ledger_repair"
_TEST_OUT.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("OUTPUT_DIR", str(_TEST_OUT))
os.environ.setdefault("MARKET", "SP500")
os.environ.pop("PIPELINE_TRADE_DATE", None)

KST = ZoneInfo("Asia/Seoul")


def _ccnl_row(
    *,
    odno: str,
    ticker: str,
    side: str,
    ord_dt: str,
    dmst_ord_dt: str,
    ord_tmd: str,
    req_qty: int,
    exe_qty: int,
    order_price: str,
    executed_price: str,
    executed_amount: str,
    thco_ord_tmd: str = "",
):
    from order_reconciler import _normalize_overseas_order_row
    return _normalize_overseas_order_row({
        "odno": odno,
        "ord_dt": ord_dt,
        "dmst_ord_dt": dmst_ord_dt,
        "ord_tmd": ord_tmd,
        "thco_ord_tmd": thco_ord_tmd or ord_tmd,
        "pdno": ticker,
        "sll_buy_dvsn_cd": "02" if side.upper() == "BUY" else "01",
        "ft_ord_qty": str(req_qty),
        "ft_ccld_qty": str(exe_qty),
        "ft_ord_unpr3": order_price,
        "ft_ccld_unpr3": executed_price,
        "ft_ccld_amt3": executed_amount,
        "prcs_stat_name": "완료",
        "ovrs_excg_cd": "NASD",
        "tr_crcy_cd": "USD",
        "mdia_dvsn_name": "OpenAPI",
    })


def intc_ccnl_orders():
    rows = [
        _ccnl_row(
            odno="0031108904", ticker="INTC", side="BUY",
            ord_dt="20260617", dmst_ord_dt="20260617", ord_tmd="233259",
            req_qty=10, exe_qty=10,
            order_price="122.00000000", executed_price="120.48000000",
            executed_amount="1204.80000",
        ),
        _ccnl_row(
            odno="0031205450", ticker="INTC", side="SELL",
            ord_dt="20260617", dmst_ord_dt="20260618", ord_tmd="034539",
            req_qty=5, exe_qty=5,
            order_price="124", executed_price="124.55",
            executed_amount="622.75",
        ),
        _ccnl_row(
            odno="0031428784", ticker="INTC", side="SELL",
            ord_dt="20260625", dmst_ord_dt="20260625", ord_tmd="225953",
            req_qty=2, exe_qty=2,
            order_price="125", executed_price="125.475",
            executed_amount="250.95",
        ),
        _ccnl_row(
            odno="0031357062", ticker="INTC", side="SELL",
            ord_dt="20260702", dmst_ord_dt="20260702", ord_tmd="232217",
            req_qty=1, exe_qty=1,
            order_price="127", executed_price="127.785",
            executed_amount="127.785",
        ),
        _ccnl_row(
            odno="0030524091", ticker="INTC", side="SELL",
            ord_dt="20260707", dmst_ord_dt="20260707", ord_tmd="223058",
            req_qty=2, exe_qty=2,
            order_price="113", executed_price="114.595",
            executed_amount="229.19",
        ),
    ]
    return {r["order_id"]: r for r in rows}


def seed_wrong_intc_db(rec):
    from recorder import TradeRecord
    buy = TradeRecord(
        timestamp=datetime(2026, 6, 17, 23, 32, 59, tzinfo=KST),
        ticker="INTC", action="BUY", quantity=5, price=122.0,
        amount=610.0, commission=0, tax=0, total_cost=610.0, net_amount=610.0,
        profit_loss=0, holding_period_days=0, order_status="executed",
        order_id="0031108904", requested_qty=5, executed_qty=5,
    )
    sells = [
        ("0031205450", datetime(2026, 6, 17, 3, 45, 39, tzinfo=KST), 5, 124.0, 0.0),
        ("0031428784", datetime(2026, 6, 25, 22, 59, 53, tzinfo=KST), 2, 125.0, 0.0),
        ("0031357062", datetime(2026, 7, 2, 23, 22, 17, tzinfo=KST), 1, 127.0, 0.0),
        ("0030524091", datetime(2026, 7, 7, 22, 30, 58, tzinfo=KST), 2, 113.0, 0.0),
    ]
    assert rec.upsert_trade_record_by_order_id(buy)
    for oid, ts, qty, px, pnl in sells:
        assert rec.upsert_trade_record_by_order_id(TradeRecord(
            timestamp=ts, ticker="INTC", action="SELL", quantity=qty, price=px,
            amount=float(px) * qty, commission=0, tax=0,
            total_cost=float(px) * qty, net_amount=float(px) * qty,
            profit_loss=pnl, holding_period_days=0, order_status="executed",
            order_id=oid, requested_qty=qty, executed_qty=qty,
        ))


def test_repair_refuses_missing_db(tmp_path):
    from ledger_repair import repair_ledger_from_ccnl
    missing = tmp_path / "nope" / "trading_data.db"
    r = repair_ledger_from_ccnl(
        ticker="INTC", date_from="20260601", date_to="20260714",
        apply=False, db_path=missing, kis_orders={}, output_dir=tmp_path,
    )
    assert r["error"]
    assert "not found" in r["error"].lower() or "refusing" in r["error"].lower()
    assert not missing.exists()


def test_dry_run_checksum_unchanged(tmp_path):
    from ledger_repair import repair_ledger_from_ccnl, file_checksum
    from recorder import DataRecorder

    db = tmp_path / "trading_data.db"
    rec = DataRecorder(str(db))
    seed_wrong_intc_db(rec)
    before = file_checksum(db)
    mtime = db.stat().st_mtime
    size = db.stat().st_size

    r = repair_ledger_from_ccnl(
        ticker="INTC", date_from="20260601", date_to="20260714",
        apply=False, db_path=db, kis_orders=intc_ccnl_orders(), output_dir=tmp_path,
    )
    assert r["mode"] == "dry_run"
    assert r["rows_updated"] == 0
    assert r["rows_needing_update"] >= 1
    assert file_checksum(db) == before
    assert db.stat().st_mtime == mtime
    assert db.stat().st_size == size
    assert r.get("db_unchanged") is True


def test_without_apply_repair_no_db_change(tmp_path):
    """Default (apply=False) never mutates DB."""
    from ledger_repair import repair_ledger_from_ccnl, file_checksum
    from recorder import DataRecorder

    db = tmp_path / "trading_data.db"
    rec = DataRecorder(str(db))
    seed_wrong_intc_db(rec)
    before = file_checksum(db)
    repair_ledger_from_ccnl(
        ticker="INTC", date_from="20260601", date_to="20260714",
        apply=False, db_path=db, kis_orders=intc_ccnl_orders(), output_dir=tmp_path,
    )
    assert file_checksum(db) == before


def test_intc_apply_fifo_and_idempotent(tmp_path):
    from ledger_repair import repair_ledger_from_ccnl
    from recorder import DataRecorder
    import sqlite3

    db = tmp_path / "trading_data.db"
    rec = DataRecorder(str(db))
    seed_wrong_intc_db(rec)
    orders = intc_ccnl_orders()

    r1 = repair_ledger_from_ccnl(
        ticker="INTC", date_from="20260601", date_to="20260714",
        apply=True, db_path=db, kis_orders=orders, output_dir=tmp_path,
    )
    assert r1["mode"] == "apply"
    assert r1["error"] is None
    assert r1["backup_path"]
    assert Path(r1["backup_path"]).exists()
    assert r1["rows_updated"] >= 1
    assert abs(float(r1["gross_pnl_total"]) - 25.875) < 1e-6
    assert r1["gross_complete"] is True
    assert r1["broker_only_detected_count"] == 0
    assert r1["ccnl_corrected_order_count"] == r1["rows_updated"]
    # corrections are NOT counted as broker-only backfill
    assert "broker_only_backfilled_count" not in r1 or r1.get("broker_only_backfilled_count", 0) == 0

    buy = [t for t in rec.get_trade_records(ticker="INTC") if t.order_id == "0031108904"][0]
    assert buy.quantity == 10
    assert buy.executed_qty == 10
    assert buy.requested_qty == 10
    assert abs(float(buy.price) - 120.48) < 1e-6
    assert buy.timestamp.isoformat().startswith("2026-06-17T23:32:59")

    expected = {
        "0031205450": (Decimal("20.350"), "2026-06-18T03:45:39"),
        "0031428784": (Decimal("9.990"), "2026-06-25T22:59:53"),
        "0031357062": (Decimal("7.305"), "2026-07-02T23:22:17"),
        "0030524091": (Decimal("-11.770"), "2026-07-07T22:30:58"),
    }
    for oid, (g, ts_prefix) in expected.items():
        sell = [t for t in rec.get_trade_records(ticker="INTC") if t.order_id == oid][0]
        assert abs(Decimal(str(sell.profit_loss)) - g) < Decimal("0.001")
        assert sell.timestamp.isoformat().startswith(ts_prefix)
        ctx = json.loads(sell.structured_context or "{}")
        assert ctx["gross_pnl_complete"] is True
        assert abs(Decimal(str(ctx["gross_pnl"])) - g) < Decimal("0.001")
        assert ctx["price_column_semantics"] == "executed_price"
        assert ctx["net_pnl_complete"] is False

    count1 = sqlite3.connect(str(db)).execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]

    # idempotent re-run
    r2 = repair_ledger_from_ccnl(
        ticker="INTC", date_from="20260601", date_to="20260714",
        apply=True, db_path=db, kis_orders=orders, output_dir=tmp_path,
    )
    count2 = sqlite3.connect(str(db)).execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]
    assert count2 == count1
    assert r2["rows_needing_update"] == 0 or r2["rows_updated"] == 0


def test_dmst_ord_dt_priority_timestamp():
    from ledger_repair import effective_trade_timestamp_from_ccnl
    dt, reason = effective_trade_timestamp_from_ccnl({
        "ord_dt": "20260617",
        "dmst_ord_dt": "20260618",
        "ord_tmd": "034539",
        "thco_ord_tmd": "",
    })
    assert reason is None
    assert dt.isoformat() == "2026-06-18T03:45:39+09:00"

    dt2, _ = effective_trade_timestamp_from_ccnl({
        "ord_dt": "20260617",
        "dmst_ord_dt": "20260618",
        "thco_ord_tmd": "034539",
        "ord_tmd": "999999",  # ignored when thco present with dmst
    })
    assert dt2.isoformat() == "2026-06-18T03:45:39+09:00"


def test_transaction_rollback_on_failure(tmp_path, monkeypatch):
    from ledger_repair import repair_ledger_from_ccnl
    from recorder import DataRecorder
    import ledger_repair as lr

    db = tmp_path / "trading_data.db"
    rec = DataRecorder(str(db))
    seed_wrong_intc_db(rec)

    real_count = lr._row_count
    calls = {"n": 0}

    def boom(conn):
        calls["n"] += 1
        c = real_count(conn)
        # First call = before; second call = after check → fake drift
        return c + (1 if calls["n"] >= 2 else 0)

    monkeypatch.setattr(lr, "_row_count", boom)
    r = repair_ledger_from_ccnl(
        ticker="INTC", date_from="20260601", date_to="20260714",
        apply=True, db_path=db, kis_orders=intc_ccnl_orders(), output_dir=tmp_path,
    )
    assert r.get("error")
    assert "rolled back" in (r.get("error") or "").lower()
    monkeypatch.setattr(lr, "_row_count", real_count)
    buy = [t for t in rec.get_trade_records(ticker="INTC") if t.order_id == "0031108904"][0]
    assert buy.quantity == 5  # untouched after rollback


def test_no_insert_for_broker_only_during_ledger_repair(tmp_path):
    from ledger_repair import repair_ledger_from_ccnl
    from recorder import DataRecorder
    import sqlite3

    db = tmp_path / "trading_data.db"
    rec = DataRecorder(str(db))
    # Only BUY in DB — sells are broker-only
    from recorder import TradeRecord
    rec.upsert_trade_record_by_order_id(TradeRecord(
        timestamp=datetime(2026, 6, 17, 23, 32, 59, tzinfo=KST),
        ticker="INTC", action="BUY", quantity=5, price=122.0,
        amount=610, commission=0, tax=0, total_cost=610, net_amount=610,
        profit_loss=0, holding_period_days=0, order_status="executed",
        order_id="0031108904", requested_qty=5, executed_qty=5,
    ))
    before = sqlite3.connect(str(db)).execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]
    r = repair_ledger_from_ccnl(
        ticker="INTC", date_from="20260601", date_to="20260714",
        apply=True, db_path=db, kis_orders=intc_ccnl_orders(), output_dir=tmp_path,
    )
    after = sqlite3.connect(str(db)).execute("SELECT COUNT(*) FROM trade_records").fetchone()[0]
    assert after == before
    assert r["broker_only_detected_count"] == 4


def test_performance_report_filename_separation(tmp_output_pr, monkeypatch):
    from performance_review import performance_report_date_tag, write_reports, PerformanceReviewResult, KisEndpointReview

    assert performance_report_date_tag("daily", "20260714", "20260714") == "20260714"
    assert performance_report_date_tag("daily", "20260708", "20260714") == "20260708_20260714"
    assert performance_report_date_tag("weekly", "20260707", "20260714") == "20260707_20260714"

    out = tmp_output_pr / "performance_reviews"
    out.mkdir(parents=True)
    result = PerformanceReviewResult(
        context={
            "market": "SP500", "period": "daily",
            "start_date": "20260708", "end_date": "20260714",
            "date_from": "20260708", "date_to": "20260714",
            "review_date": "20260708_20260714",
            "review_scope": "date_range",
            "report_key": "SP500_daily_20260708_20260714",
            "generated_at_kst": "2026-07-14T12:00:00+09:00",
            "output_filename": "performance_review_SP500_daily_20260708_20260714.json",
        },
        kis_endpoint_review=KisEndpointReview(),
    )
    jp, _ = write_reports(result, out, "SP500", "daily", "20260708_20260714", json_only=True)
    assert jp.name == "performance_review_SP500_daily_20260708_20260714.json"
    payload = json.loads(jp.read_text(encoding="utf-8"))
    for key in (
        "review_scope", "review_date", "date_from", "date_to", "period",
        "market", "report_key", "generated_at_kst", "output_filename",
    ):
        assert key in payload and payload[key]


@pytest.fixture()
def tmp_output_pr(tmp_path, monkeypatch):
    out = tmp_path / "output"
    out.mkdir()
    monkeypatch.setenv("OUTPUT_DIR", str(out))
    return out


def test_weekly_known_gross_pnl_129_1608(tmp_path):
    from performance_review import _summarize_trades, _apply_broker_integrity_findings, ReviewArtifacts

    rows = [
        {
            "action": "SELL", "order_id": "0030524091", "profit_loss": -11.77,
            "structured_context": json.dumps({
                "gross_pnl": "-11.7700", "gross_pnl_complete": True,
                "gross_pnl_basis": True, "net_pnl_complete": False,
            }),
        },
        {
            "action": "SELL", "order_id": "0031276871", "profit_loss": 140.9308,
            "structured_context": json.dumps({
                "gross_pnl": "140.9308", "gross_pnl_complete": True,
                "gross_pnl_basis": True, "net_pnl_complete": False,
                "broker_only": True, "backfilled_from": "kis_ccnl",
                "source": "order_reconciler",
            }),
        },
    ]
    perf = _summarize_trades(rows)
    assert perf["sell_trade_count"] == 2
    assert perf["gross_complete_sell_count"] == 2
    assert perf["gross_incomplete_sell_count"] == 0
    assert abs(perf["known_gross_pnl"] - 129.1608) < 1e-4
    assert perf["gross_pnl_complete"] is True
    assert perf["net_pnl_complete"] is False
    assert perf["gross_incomplete_orders"] == []

    art = ReviewArtifacts(
        market="SP500", start_date="20260707", review_date="20260714",
        period="weekly", session="pm", output_dir=tmp_path, db_path=tmp_path / "x.db",
    )
    # broker-only already 0
    (tmp_path / "order_reconcile_SP500_20260714.json").write_text(
        json.dumps({
            "broker_only_order_count": 0,
            "broker_only_orders": [],
            "findings": [],
            "db_reconcile": {"broker_only_order_count": 0, "broker_only_orders": []},
        }),
        encoding="utf-8",
    )
    findings = []
    status = _apply_broker_integrity_findings(findings, art, perf)
    assert status == "GROSS_COMPLETE_NET_INCOMPLETE"
    assert any(f.title == "GROSS_COMPLETE_NET_INCOMPLETE" for f in findings)
    assert not any("broker-only backfill" in (f.recommendation or "").lower() for f in findings)


def test_backfilled_sell_sellable_finding():
    from performance_review import _is_broker_only_backfilled_sell
    assert _is_broker_only_backfilled_sell(
        {"broker_only": True, "backfilled_from": "kis_ccnl", "source": "order_reconciler"},
        reason_code="BROKER_ONLY_BACKFILL",
    )
    assert not _is_broker_only_backfilled_sell({}, reason_code="")


def test_cli_mutex_dry_run_apply(tmp_path, monkeypatch):
    import order_reconciler as orc
    monkeypatch.setattr(sys, "argv", [
        "order_reconciler.py",
        "--repair-ledger-from-ccnl",
        "--ticker", "INTC",
        "--from", "20260601",
        "--to", "20260714",
        "--dry-run",
        "--apply-repair",
    ])
    with pytest.raises(SystemExit):
        orc.main()
