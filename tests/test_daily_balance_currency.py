"""USD/KRW mapping, pollution detection, currency repair, immutable provenance."""

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

from daily_balance_values import (  # noqa: E402
    detect_legacy_usd_field_pollution,
    is_return_calculation_usable,
    normalize_account_values,
    propose_currency_repair_from_embedded,
    save_immutable_source_copy,
    sha256_file,
    verify_source_snapshot_not_mutated,
    versioned_balance_filename,
)
import integrated_manager as im  # noqa: E402

TD = "20260715"
CLOSE_KST = "20260716"


def ops_polluted_open() -> dict:
    """운영 20260715 open (balance_open_20260716.json) 오염 상태 재현."""
    return {
        "date": TD,
        "trade_date": TD,
        "type": "open",
        "valid": True,
        "canonical": False,
        "legacy_alias": True,
        "alias_of_trade_date": TD,
        "source": "kis_account_file_same_date",
        "snapshot_ts_kst": "2026-07-15T22:45:06+09:00",
        "timestamp": "2026-07-15T22:45:06+09:00",
        "generated_at_kst": "2026-07-15T22:45:06+09:00",
        "total_balance": 829469,
        "cash": 2120,
        "holdings_value": 955.87,
        "holdings_count": 1,
        "holdings_detail": [
            {"ticker": "MU", "qty": 1, "price": 955.87, "value": 955.87, "currency": "USD"}
        ],
        "kis_summary": {
            "currency": "USD",
            "tot_evlu_amt_krw": 4347907,
            "tot_evlu_amt_usd": 829469.0,
            "ord_psbl_frcr_amt": 556,
            "usd_cash_total": 2120.48,
            "usd_withdrawable": 555.87,
            "usd_sell_reuse": 0.0,
            "usd_buy_margin": 1564.61,
            "krw_cash": 1189783,
            "available_cash_krw": 829469,
            "bass_exrt": 1492.2,
            "evlu_pfls_smtl_amt": 956,
        },
        "source_snapshot_file": "/app/output/balance_20260715.json",
        "available_cash_krw": 829469,
    }


@pytest.fixture()
def balance_dir(tmp_path, monkeypatch):
    d = tmp_path / "daily_balances"
    d.mkdir()
    monkeypatch.setattr(im, "BALANCE_STORAGE_PATH", d)
    return d


class TestPollutionDetection:
    def test_829469_excluded_as_usd_total(self):
        poll = detect_legacy_usd_field_pollution(
            tot_evlu_amt_usd=829469,
            available_cash_krw=829469,
            available_cash_usd=556,
            holdings_value_usd=955.87,
            fx_rate=1492.2,
        )
        assert poll
        assert poll[0]["code"] == "LEGACY_USD_FIELD_POLLUTED_BY_KRW"

    def test_available_cash_krw_equals_tot_evlu_usd(self):
        poll = detect_legacy_usd_field_pollution(
            tot_evlu_amt_usd=829469, available_cash_krw=829469
        )
        assert any("available_cash_krw" in r for r in poll[0]["reasons"])


class TestNormalize:
    def test_usd_cash_total_not_used_as_asset_cash(self):
        raw = {
            "market": "SP500",
            "currency": "USD",
            "kis_summary": {
                "currency": "USD",
                "ord_psbl_frcr_amt": 556,
                "usd_cash_total": 2120.48,
                "usd_buy_margin": 1564.61,
                "usd_withdrawable": 555.87,
                "tot_evlu_amt_usd": 829469,
                "available_cash_krw": 829469,
                "bass_exrt": 1492.2,
            },
            "holdings_detail": [
                {"ticker": "MU", "value": 955.87, "currency": "USD"}
            ],
            "available_cash_krw": 829469,
        }
        n = normalize_account_values(raw, market="SP500")
        assert n["available_cash_usd"] == 556.0
        assert n["holdings_value_usd"] == 955.87
        assert n["total_asset_usd"] == 1511.87
        assert n["buying_power_margin_usd"] == 1564.61
        assert n["total_asset_usd"] != pytest.approx(556 + 955.87 + 1564.61)
        reasons = [r["reason"] for r in n["rejected_fields"]]
        assert "NOT_ASSET_CASH_BUYING_POWER_INCLUDED" in reasons
        assert "LEGACY_USD_FIELD_POLLUTED_BY_KRW" in reasons
        assert n["financial_values_valid"] is True
        assert n["usd_components_consistent"] is True

    def test_ord_psbl_maps_to_available_cash(self):
        n = normalize_account_values(
            {
                "market": "SP500",
                "currency": "USD",
                "kis_summary": {"currency": "USD", "ord_psbl_frcr_amt": 556},
                "holdings_detail": [{"ticker": "MU", "value": 955.87, "currency": "USD"}],
            },
            market="SP500",
        )
        assert n["available_cash_usd"] == 556.0
        assert n["field_provenance"]["available_cash_usd"] == "$.kis_summary.ord_psbl_frcr_amt"

    def test_compat_fields(self):
        n = normalize_account_values(
            {
                "market": "SP500",
                "currency": "USD",
                "kis_summary": {"currency": "USD", "ord_psbl_frcr_amt": 556},
                "holdings_detail": [{"ticker": "MU", "value": 955.87, "currency": "USD"}],
            },
            market="SP500",
            currency_status="reconstructed",
        )
        assert n["total_balance"] == 1511.87
        assert n["cash"] == 556.0
        assert n["holdings_value"] == 955.87
        assert n["balance_currency"] == "USD"
        assert n["currency_status"] == "reconstructed"


class TestCurrencyRepair:
    def test_propose_from_embedded_ops_open(self):
        snap = ops_polluted_open()
        prop = propose_currency_repair_from_embedded(snap, market="SP500")
        assert prop["proposed_total_asset_usd"] == 1511.87
        assert prop["proposed_available_cash_usd"] == 556.0
        assert prop["proposed_holdings_value_usd"] == 955.87
        assert prop["gates_ok"] is True
        reasons = [r["reason"] for r in prop["rejected_fields"]]
        assert "LEGACY_USD_FIELD_POLLUTED_BY_KRW" in reasons
        assert "NOT_ASSET_CASH_BUYING_POWER_INCLUDED" in reasons
        assert any(r.get("field") == "source_snapshot_file" for r in prop["rejected_fields"])

    def test_dry_run_no_file_change(self, balance_dir):
        snap = ops_polluted_open()
        p = balance_dir / f"balance_open_{CLOSE_KST}.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        before = p.read_bytes()
        result = im.repair_daily_balance_currency(TD, "open", apply=False)
        assert result["dry_run"] is True
        assert result["updated"] == 0
        assert result["proposed_total_asset_usd"] == 1511.87
        assert p.read_bytes() == before

    def test_apply_atomic_and_idempotent(self, balance_dir):
        snap = ops_polluted_open()
        p = balance_dir / f"balance_open_{CLOSE_KST}.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        canon = balance_dir / "canonical"
        canon.mkdir()
        (canon / f"balance_open_trade_{TD}.json").write_text(json.dumps(snap), encoding="utf-8")

        r1 = im.repair_daily_balance_currency(TD, "open", apply=True)
        assert r1["applied"] is True
        assert r1["updated"] >= 1
        repaired = json.loads(p.read_text(encoding="utf-8"))
        assert repaired["total_asset_usd"] == 1511.87
        assert repaired["available_cash_usd"] == 556.0
        assert repaired["cash"] == 556.0
        assert repaired["total_balance"] == 1511.87
        assert repaired["financial_values_valid"] is True
        assert repaired["return_calculation_usable"] is True
        assert repaired["kis_summary"]["tot_evlu_amt_usd"] == 829469.0
        assert repaired["holdings_detail"][0]["value"] == 955.87
        assert is_return_calculation_usable(repaired)

        r2 = im.repair_daily_balance_currency(TD, "open", apply=True)
        assert r2["updated"] == 0
        assert r2["status"] == "already_valid"


class TestProvenance:
    def test_sha_mismatch_detected(self, tmp_path):
        src = tmp_path / "balance_20260715.json"
        src.write_text(json.dumps({"holdings": [{"value": 955.87}]}), encoding="utf-8")
        digest = sha256_file(src)
        src.write_text(
            json.dumps({"holdings": [{"value": 916.43}, {"value": 210.18}]}),
            encoding="utf-8",
        )
        snap = {
            "source_snapshot_file": str(src),
            "source_snapshot_sha256": digest,
            "trade_date": TD,
            "source_snapshot_trade_date": TD,
        }
        ok, reason = verify_source_snapshot_not_mutated(snap)
        assert ok is False
        assert reason == "SOURCE_SNAPSHOT_MUTATED"

    def test_immutable_copy_created(self, balance_dir, tmp_path):
        src = tmp_path / "balance_20260715.json"
        payload = {"trade_date": TD, "generated_at_kst": "2026-07-15T22:45:06+09:00", "x": 1}
        src.write_text(json.dumps(payload), encoding="utf-8")
        meta = save_immutable_source_copy(
            src,
            balance_storage=balance_dir,
            market="SP500",
            trade_date=TD,
            snapshot_type="open",
            snapshot_ts_kst="2026-07-15T22:45:06+09:00",
        )
        assert meta["source_snapshot_sha256"]
        assert meta["source_snapshot_immutable_copy"]
        assert Path(meta["source_snapshot_immutable_copy"]).is_file()
        src.write_text(json.dumps({"x": 999}), encoding="utf-8")
        copied = json.loads(Path(meta["source_snapshot_immutable_copy"]).read_text())
        assert copied["x"] == 1

    def test_versioned_filename(self):
        name = versioned_balance_filename(
            "SP500", TD, "open", "2026-07-15T22:45:06+09:00"
        )
        assert name.startswith("balance_SP500_20260715_open_")
        assert "224506" in name
        assert name.endswith(".json")

    def test_portfolio_totals_rejects_pollution(self):
        cash_map = {
            "dnca_tot_amt": 2120,
            "frcr_buy_amt": 2120,
            "available_cash": 556,
            "ord_psbl_frcr_amt": 556,
            "tot_evlu_amt_usd": 829469,
            "available_cash_krw": 829469,
            "krw_cash": 1189783,
            "bass_exrt": 1492.2,
            "usd_cash_total": 2120.48,
            "usd_buy_margin": 1564.61,
        }
        holdings = [{"pdno": "MU", "hldg_qty": 1, "prpr": 955.87, "evlu_amt": 955.87}]
        total, cash, hv = im._portfolio_totals_from_cash_map(cash_map, holdings)
        assert cash == 556.0
        assert hv == 955.87
        assert total == 1511.87


class TestCloseNotOverwrittenByLaterSnapshot:
    def test_repair_close_uses_embedded_only(self, balance_dir, tmp_path):
        """06:05 mutated balance file must not rewrite 06:00 close values."""
        close = {
            "trade_date": TD,
            "type": "close",
            "valid": True,
            "canonical": True,
            "snapshot_ts_kst": "2026-07-16T06:00:00+09:00",
            "timestamp": "2026-07-16T06:00:00+09:00",
            "total_balance": 1687.74,
            "cash": 556.0,
            "holdings_value": 1131.74,
            "holdings_detail": [
                {"ticker": "MU", "value": 920.0, "currency": "USD"},
                {"ticker": "NVDA", "value": 211.74, "currency": "USD"},
            ],
            "kis_summary": {
                "currency": "USD",
                "ord_psbl_frcr_amt": 556,
                "usd_withdrawable": 556,
                "tot_evlu_amt_usd": 1687.74,
            },
            "source_snapshot_file": str(tmp_path / "balance_20260715.json"),
            "base_currency": "USD",
            "total_asset_usd": 1687.74,
            "available_cash_usd": 556.0,
            "holdings_value_usd": 1131.74,
            "financial_values_valid": True,
            "return_calculation_usable": True,
            "currency_status": "normalized",
            "usd_components_consistent": True,
        }
        mutated = tmp_path / "balance_20260715.json"
        mutated.write_text(json.dumps({
            "holdings": [{"value": 9999}],
            "generated_at_kst": "2026-07-16T06:05:41+09:00",
        }), encoding="utf-8")

        p = balance_dir / f"balance_close_{TD}.json"
        p.write_text(json.dumps(close), encoding="utf-8")
        result = im.repair_daily_balance_currency(TD, "close", apply=True)
        assert result["status"] == "already_valid"
        assert result["updated"] == 0
        after = json.loads(p.read_text())
        assert after["holdings_value_usd"] == 1131.74
        assert after["total_asset_usd"] == 1687.74
