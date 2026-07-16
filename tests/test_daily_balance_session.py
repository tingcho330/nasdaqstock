"""US daily balance session identity / open-close pairing / currency consistency.

운영 증거 (2026-07-16):
- 06:00 KST close 캡처 → balance_close_20260715.json (trade_date=20260715, canonical)
- 06:15 KST summary가 balance_close_20260716.json을 요구하며 실패했고,
  open은 trade_date=20260714인 balance_open_20260715.json과 잘못 pairing됐다.
이 테스트는 metadata.trade_date 기준 pairing으로의 수정을 검증한다.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
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

from utils import KST, resolve_market_session_identity  # noqa: E402
import integrated_manager as im  # noqa: E402

# 운영 시나리오 고정 시각: 2026-07-16 06:15 KST (US 거래일 2026-07-15 마감 직후)
NOW_0615 = datetime(2026, 7, 16, 6, 15, tzinfo=KST)
TD = "20260715"          # US trade_date
CLOSE_KST = "20260716"   # KST session close calendar date
PREV_TD = "20260714"


# ───────────────────────── fixtures / helpers ─────────────────────────

@pytest.fixture()
def balance_dir(tmp_path, monkeypatch):
    d = tmp_path / "daily_balances"
    d.mkdir()
    monkeypatch.setattr(im, "BALANCE_STORAGE_PATH", d)
    return d


@pytest.fixture()
def fixed_identity(monkeypatch):
    """scheduled 06:15 KST 실행을 시뮬레이션 (capture/summary 공통 identity)."""
    def _ident(market=None, now_kst=None, explicit_trade_date=None):
        return resolve_market_session_identity(
            market, now_kst=NOW_0615, explicit_trade_date=explicit_trade_date
        )

    monkeypatch.setattr(im, "resolve_market_session_identity", _ident)
    return _ident


@pytest.fixture()
def discord(monkeypatch):
    """전송된 embed / content 캡처."""
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


def make_snap(
    snap_type: str,
    trade_date: str,
    *,
    canonical: bool = True,
    legacy_alias: bool = False,
    alias_of: str = None,
    source: str = "kis_account_snapshot",
    generated_at: str = None,
    cash: float = 1000.0,
    hv: float = 500.0,
    explicit_usd: bool = True,
    total_balance: float = None,
    valid: bool = True,
) -> dict:
    total = round(cash + hv, 2) if total_balance is None else total_balance
    snap = {
        "date": trade_date,
        "trade_date": trade_date,
        "type": snap_type,
        "valid": valid,
        "canonical": canonical,
        "legacy_alias": legacy_alias,
        "source": source,
        "timestamp": generated_at or "2026-07-16T06:00:00+09:00",
        "generated_at_kst": generated_at or "2026-07-16T06:00:00+09:00",
        "total_balance": total,
        "cash": cash,
        "holdings_value": hv,
        "holdings_count": 0,
        "holdings_detail": [],
        "kis_summary": {"ord_psbl_frcr_amt": cash, "tot_evlu_amt_krw": 0},
    }
    if alias_of:
        snap["alias_of_trade_date"] = alias_of
    if explicit_usd:
        snap.update({
            "base_currency": "USD",
            "total_asset_usd": round(cash + hv, 2),
            "available_cash_usd": cash,
            "holdings_value_usd": hv,
            "value_semantics": "base_currency_account_value",
            "financial_values_valid": True,
            "return_calculation_usable": True,
            "currency_status": "normalized",
            "usd_components_consistent": True,
        })
    return snap


def write_legacy(balance_dir: Path, file_date: str, snap: dict) -> Path:
    p = balance_dir / f"balance_{snap['type']}_{file_date}.json"
    p.write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")
    return p


def write_canonical(balance_dir: Path, snap: dict) -> Path:
    d = balance_dir / "canonical"
    d.mkdir(exist_ok=True)
    p = d / f"balance_{snap['type']}_trade_{snap['trade_date']}.json"
    p.write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")
    return p


def ops_files(balance_dir: Path):
    """운영에서 확인된 실제 파일 상태 재현 (수동 alias 제외)."""
    # 06:00 canonical close (trade_date=20260715)
    close_path = write_legacy(balance_dir, TD, make_snap("close", TD))
    # open alias: balance_open_20260715.json → trade_date=20260714
    write_legacy(
        balance_dir,
        TD,
        make_snap("open", PREV_TD, canonical=False, legacy_alias=True,
                  alias_of=PREV_TD, source="kis_account_file_same_date",
                  generated_at="2026-07-14T22:45:00+09:00"),
    )
    # open alias: balance_open_20260716.json → trade_date=20260715 (올바른 open)
    open_path = write_legacy(
        balance_dir,
        CLOSE_KST,
        make_snap("open", TD, canonical=False, legacy_alias=True,
                  alias_of=TD, source="kis_account_file_same_date",
                  generated_at="2026-07-15T22:45:00+09:00"),
    )
    return open_path, close_path


# ───────────────────────── 1. session identity ─────────────────────────

class TestSessionIdentity:
    def test_us_identity_at_0615_kst(self):
        """테스트 1: 2026-07-16 06:15 KST SP500 → trade_date=20260715."""
        ident = resolve_market_session_identity("SP500", now_kst=NOW_0615)
        assert ident["trade_date"] == TD
        assert ident["session_open_date_kst"] == TD
        assert ident["session_close_date_kst"] == CLOSE_KST
        assert ident["resolution_source"] == "market_calendar"

    def test_us_identity_explicit_trade_date(self):
        ident = resolve_market_session_identity(
            "SP500", now_kst=datetime(2026, 7, 20, 12, 0, tzinfo=KST),
            explicit_trade_date=TD,
        )
        assert ident["trade_date"] == TD
        assert ident["session_close_date_kst"] == CLOSE_KST
        assert ident["resolution_source"] == "explicit_trade_date"
        assert ident["is_live_trade_date"] is False

    def test_kr_identity_same_dates(self):
        """테스트 15: KR 시장 날짜 처리 회귀 없음 (open=close=trade_date)."""
        ident = resolve_market_session_identity(
            "KOSPI", now_kst=datetime(2026, 7, 16, 10, 0, tzinfo=KST)
        )
        assert ident["trade_date"] == "20260716"
        assert ident["session_open_date_kst"] == "20260716"
        assert ident["session_close_date_kst"] == "20260716"


# ───────────────────────── snapshot lookup ─────────────────────────

class TestFindSnapshot:
    def test_open_selected_by_metadata_not_filename(self, balance_dir):
        """테스트 4: open_20260715(td=0714) vs open_20260716(td=0715) → 후자."""
        ops_files(balance_dir)
        snap, path, res = im.find_balance_snapshot(TD, "open")
        assert snap is not None
        assert path.name == f"balance_open_{CLOSE_KST}.json"
        assert snap["trade_date"] == TD
        assert res == "legacy_metadata"

    def test_wrong_filename_date_excluded(self, balance_dir):
        """balance_open_20260715.json(trade_date=20260714)은 20260715 후보가 아님."""
        write_legacy(
            balance_dir, TD,
            make_snap("open", PREV_TD, canonical=False, legacy_alias=True, alias_of=PREV_TD),
        )
        snap, path, res = im.find_balance_snapshot(TD, "open")
        assert snap is None and res == "missing"
        # 반대로 20260714 조회에는 사용된다
        snap, path, _ = im.find_balance_snapshot(PREV_TD, "open")
        assert snap is not None and snap["trade_date"] == PREV_TD

    def test_canonical_dir_preferred(self, balance_dir):
        """테스트 7: 신규 canonical 파일이 legacy보다 우선."""
        write_legacy(balance_dir, TD, make_snap("close", TD, cash=1.0, hv=1.0))
        canon = write_canonical(balance_dir, make_snap("close", TD, cash=2.0, hv=2.0))
        snap, path, res = im.find_balance_snapshot(TD, "close")
        assert path == canon
        assert res == "canonical"
        assert snap["cash"] == 2.0

    def test_canonical_metadata_beats_manual_alias(self, balance_dir):
        """테스트 13: 수동 alias(balance_close_20260716.json)보다 canonical 우선."""
        write_legacy(balance_dir, TD, make_snap("close", TD))
        write_legacy(
            balance_dir, CLOSE_KST,
            make_snap("close", TD, canonical=False, legacy_alias=True, alias_of=TD,
                      generated_at="2026-07-16T07:30:00+09:00"),
        )
        snap, path, res = im.find_balance_snapshot(TD, "close")
        assert path.name == f"balance_close_{TD}.json"
        assert snap["canonical"] is True
        assert res == "legacy_exact"


# ───────────────────────── pairing ─────────────────────────

class TestPairing:
    def test_pair_success_with_different_filenames(self, balance_dir):
        """테스트 6: 파일명 날짜가 달라도 metadata.trade_date가 같으면 pairing."""
        ops_files(balance_dir)
        pair = im.load_daily_balance_pair(TD)
        assert pair["pair_ok"] is True
        assert pair["open_path"].name == f"balance_open_{CLOSE_KST}.json"
        assert pair["close_path"].name == f"balance_close_{TD}.json"
        assert pair["errors"] == []

    def test_pair_trade_date_mismatch_detected(self, balance_dir):
        """테스트 5: open/close trade_date 불일치 → 에러."""
        open_snap = make_snap("open", PREV_TD)
        close_snap = make_snap("close", TD)
        errors = im._validate_balance_pair(open_snap, close_snap, TD)
        assert any("open.trade_date" in e for e in errors)
        assert any(f"open.trade_date={PREV_TD} != close.trade_date={TD}" in e for e in errors)

    def test_pair_type_and_valid_checked(self, balance_dir):
        bad_open = make_snap("close", TD)  # type 잘못
        invalid_close = make_snap("close", TD, valid=False)
        errors = im._validate_balance_pair(bad_open, invalid_close, TD)
        assert any("open.type" in e for e in errors)
        assert any("close.valid=false" in e for e in errors)

    def test_kr_same_date_pair(self, balance_dir, monkeypatch):
        """테스트 15: KR은 동일 날짜 open/close pairing 유지."""
        monkeypatch.setattr(im, "MARKET", "KOSPI")
        d = "20260716"
        write_legacy(balance_dir, d, make_snap("open", d, explicit_usd=False))
        write_legacy(balance_dir, d, make_snap("close", d, explicit_usd=False))
        pair = im.load_daily_balance_pair(d)
        assert pair["pair_ok"] is True
        assert pair["open_path"].name == f"balance_open_{d}.json"
        assert pair["close_path"].name == f"balance_close_{d}.json"


# ───────────────────────── capture ─────────────────────────

class TestCapture:
    def test_existing_valid_close_not_recaptured(self, balance_dir, fixed_identity):
        """테스트 2/9(close): canonical close가 있으면 capture-close는 skip."""
        _, close_path = ops_files(balance_dir)
        before = close_path.read_text(encoding="utf-8")
        result = im.capture_balance_snapshot("close", trade_date=TD)
        assert result["status"] == "existing_valid"
        assert result["trade_date"] == TD
        assert Path(result["path"]) == close_path
        assert result["source"] == "kis_account_snapshot"
        assert close_path.read_text(encoding="utf-8") == before

    def test_historical_open_recapture_forbidden(self, balance_dir):
        """테스트 9: 과거 open snapshot을 현재 계좌로 재생성 금지."""
        result = im.capture_balance_snapshot("open", trade_date="20200103")
        assert result["status"] == "failed"
        assert result["reason"] == "historical_recapture_forbidden"
        assert list(balance_dir.rglob("*.json")) == []

    def test_canonical_filename_no_collision(self):
        """테스트 10: 신규 canonical 파일명은 legacy 파일명과 충돌하지 않음."""
        canon = im._canonical_balance_path("open", TD)
        legacy = im._legacy_balance_path("open", TD)
        assert canon != legacy
        assert canon.parent.name == "canonical"
        assert canon.name == f"balance_open_trade_{TD}.json"
        assert im._BALANCE_FILENAME_RE.match(canon.name) is None


# ───────────────────────── daily summary ─────────────────────────

class TestDailySummary:
    def test_summary_success_without_kst_alias(
        self, balance_dir, fixed_identity, discord, monkeypatch, caplog
    ):
        """테스트 2/3/8/15장: balance_close_20260716.json(수동 alias) 없이 성공,
        capture 재시도·스케줄누락 경고 없음, open은 open_20260716 선택."""
        ops_files(balance_dir)
        capture_calls = []
        monkeypatch.setattr(
            im, "capture_balance_snapshot",
            lambda *a, **k: capture_calls.append((a, k)) or {"status": "failed"},
        )
        with caplog.at_level("INFO", logger="IntegratedManager"):
            im.send_daily_trading_summary()

        assert capture_calls == []  # 테스트 2: capture 재시도 없음
        assert len(discord["embeds"]) == 1  # summary 성공
        text = caplog.text
        assert "CLOSE_SNAPSHOT_PRESENT" in text
        assert "CLOSE_CAPTURE_SCHEDULE_MISSED" not in text
        assert "스케줄 누락" not in text
        assert "[DAILY_SUMMARY_SESSION]" in text
        assert f"trade_date={TD}" in text
        assert f"session_close_date_kst={CLOSE_KST}" in text
        # [DAILY_BALANCE_PAIR] open/close trade_date 동일 확인
        assert "[DAILY_BALANCE_PAIR]" in text
        assert f"open_trade_date={TD}" in text
        assert f"close_trade_date={TD}" in text
        # 잘못된 pairing 로그가 없어야 함
        assert f"open_{PREV_TD}" not in text

        meta = json.loads(
            (balance_dir / f"daily_summary_meta_{TD}.json").read_text(encoding="utf-8")
        )
        assert meta["trade_date"] == TD
        assert meta["session_close_date_kst"] == CLOSE_KST
        assert meta["open_snapshot_file"] == f"balance_open_{CLOSE_KST}.json"
        assert meta["close_snapshot_file"] == f"balance_close_{TD}.json"
        assert meta["open_snapshot_legacy_alias"] is True
        assert meta["close_snapshot_canonical"] is True

    def test_summary_with_explicit_trade_date(self, balance_dir, discord):
        """테스트 16: --send-summary --trade-date 20260715 성공 (실시각과 무관)."""
        ops_files(balance_dir)
        im.send_daily_trading_summary(target_trade_date=TD)
        assert len(discord["embeds"]) == 1

    def test_summary_displays_us_and_kst_dates(self, balance_dir, discord):
        """테스트 11: 미국 거래일·한국 종료일을 각각 표시."""
        ops_files(balance_dir)
        im.send_daily_trading_summary(target_trade_date=TD)
        embed = discord["embeds"][0]
        assert "2026-07-15" in embed["title"]
        assert "미국 거래일: 2026-07-15" in embed["description"]
        assert "한국 기준 종료일: 2026-07-16" in embed["description"]

    def test_summary_usd_only_comparison(self, balance_dir, discord):
        """테스트 12: total_asset_usd끼리 비교한 수익률."""
        write_legacy(balance_dir, TD, make_snap("close", TD, cash=1100.0, hv=550.0))
        write_legacy(
            balance_dir, CLOSE_KST,
            make_snap("open", TD, canonical=False, legacy_alias=True, alias_of=TD,
                      cash=1000.0, hv=500.0),
        )
        im.send_daily_trading_summary(target_trade_date=TD)
        embed = discord["embeds"][0]
        usd_field = next(f for f in embed["fields"] if "일일 수익률 (USD)" in f["name"])
        assert "+10.00%" in usd_field["value"]  # (1650-1500)/1500

    def test_summary_currency_mismatch_aborts(self, balance_dir, discord, caplog):
        """ambiguous/polluted without reconstructable embedded → PARTIAL (WARNING, not ERROR)."""
        open_snap = make_snap(
            "open", TD, canonical=False, legacy_alias=True, alias_of=TD,
            explicit_usd=False, cash=2120.0, hv=0.0, total_balance=829469.0,
        )
        open_snap["kis_summary"] = {
            "usd_cash_total": 2120.48,
            "usd_buy_margin": 1564.61,
            "tot_evlu_amt_usd": 829469.0,
            "available_cash_krw": 829469,
            "krw_cash": 1189783,
            "tot_evlu_amt_krw": 4347907,
            "bass_exrt": 1492.2,
            "currency": "USD",
            # deliberately omit usable ord_psbl / holdings
        }
        open_snap["holdings_detail"] = []
        open_snap["holdings_value"] = 0
        open_snap["available_cash_krw"] = 829469
        write_legacy(balance_dir, CLOSE_KST, open_snap)
        write_legacy(balance_dir, TD, make_snap("close", TD, cash=556.0, hv=1131.74))
        with caplog.at_level("INFO", logger="IntegratedManager"):
            result = im.send_daily_trading_summary(target_trade_date=TD)
        assert result["summary_status"] == "PARTIAL"
        assert "DAILY_BALANCE_CURRENCY_AMBIGUOUS" in caplog.text
        assert "DAILY_SUMMARY_PARTIAL" in caplog.text
        # PARTIAL은 장애가 아님 — CURRENCY 관련 ERROR 없어야 함
        err_currency = [
            r for r in caplog.records
            if r.levelname == "ERROR" and "CURRENCY" in r.getMessage()
        ]
        assert err_currency == []
        assert len(discord["embeds"]) == 1
        assert "PARTIAL" in discord["embeds"][0]["title"]
        for f in discord["embeds"][0]["fields"]:
            assert "일일 수익률 (USD)" not in f["name"]
        meta = json.loads((balance_dir / f"daily_summary_meta_{TD}.json").read_text())
        assert meta["summary_status"] == "PARTIAL"
        assert meta["return_metrics_available"] is False
        assert meta["total_change"] is None
        assert meta["daily_asset_pnl"] is None
        assert all(f.get("severity") == "WARNING" for f in meta["data_quality_findings"])

    def test_summary_pair_mismatch_aborts(self, balance_dir, discord, caplog, monkeypatch):
        """테스트 5: open/close trade_date 불일치 시 요약 중단."""
        open_snap = make_snap("open", PREV_TD)
        close_snap = make_snap("close", TD)
        monkeypatch.setattr(
            im, "load_daily_balance_pair",
            lambda td: {
                "trade_date": td,
                "open": open_snap, "close": close_snap,
                "open_path": Path("balance_open_x.json"),
                "close_path": Path("balance_close_x.json"),
                "open_resolution": "legacy_metadata",
                "close_resolution": "legacy_exact",
                "errors": [], "pair_ok": True,
            },
        )
        with caplog.at_level("ERROR", logger="IntegratedManager"):
            im.send_daily_trading_summary(target_trade_date=TD)
        assert discord["embeds"] == []
        assert "DAILY_BALANCE_PAIR_TRADE_DATE_MISMATCH" in caplog.text

    def test_summary_missing_open_never_regenerated(
        self, balance_dir, discord, caplog, monkeypatch
    ):
        """테스트 9: open 없음 → OPEN_SNAPSHOT_UNAVAILABLE, 재캡처 금지."""
        write_legacy(balance_dir, TD, make_snap("close", TD))
        # open은 trade_date=20260714 alias만 존재 (20260715 open 없음)
        write_legacy(
            balance_dir, TD.replace("15", "15"),  # balance_open_20260715.json
            make_snap("open", PREV_TD, canonical=False, legacy_alias=True, alias_of=PREV_TD),
        )
        capture_calls = []
        monkeypatch.setattr(
            im, "capture_balance_snapshot",
            lambda *a, **k: capture_calls.append((a, k)) or {"status": "failed"},
        )
        with caplog.at_level("ERROR", logger="IntegratedManager"):
            im.send_daily_trading_summary(target_trade_date=TD)
        assert discord["embeds"] == []
        assert capture_calls == []  # open 재캡처 시도 없음
        assert "OPEN_SNAPSHOT_UNAVAILABLE" in caplog.text
        # trade_date=20260714 open과 잘못 pairing되지 않음
        assert "close=20260716" not in caplog.text

    def test_summary_schedule_missed_only_without_evidence(
        self, balance_dir, discord, caplog, monkeypatch
    ):
        """테스트 10(경고 분리): close 없음+recovery 실패+evidence 없음 → SCHEDULE_MISSED."""
        monkeypatch.setattr(
            im, "capture_balance_snapshot",
            lambda *a, **k: {"status": "failed", "reason": "historical_recapture_forbidden"},
        )
        with caplog.at_level("WARNING", logger="IntegratedManager"):
            im.send_daily_trading_summary(target_trade_date=TD)
        assert "CLOSE_CAPTURE_SCHEDULE_MISSED" in caplog.text

    def test_summary_no_schedule_missed_with_evidence(
        self, balance_dir, discord, caplog, monkeypatch
    ):
        """close 캡처 evidence가 있으면 스케줄 누락으로 판단하지 않음."""
        # 다른 trade_date의 close지만 20260716 KST에 생성됨 → 06:00 실행 evidence
        write_legacy(
            balance_dir, "20260710",
            make_snap("close", "20260710", generated_at="2026-07-16T06:00:00+09:00"),
        )
        monkeypatch.setattr(
            im, "capture_balance_snapshot",
            lambda *a, **k: {"status": "failed", "reason": "x"},
        )
        with caplog.at_level("WARNING", logger="IntegratedManager"):
            im.send_daily_trading_summary(target_trade_date=TD)
        assert "CLOSE_CAPTURE_SCHEDULE_MISSED" not in caplog.text
        assert "CLOSE_SNAPSHOT_MISSING" in caplog.text

    def test_summary_recovery_reloads_returned_path(
        self, balance_dir, discord, monkeypatch
    ):
        """테스트(9장): recovery capture 후 반환 path 재로드 (KST 날짜 재탐색 금지)."""
        write_legacy(
            balance_dir, CLOSE_KST,
            make_snap("open", TD, canonical=False, legacy_alias=True, alias_of=TD),
        )
        created = make_snap("close", TD)
        capture_calls = []

        def _fake_capture(snapshot_type, **kw):
            assert snapshot_type == "close"
            assert kw.get("trade_date") == TD
            path = write_canonical(balance_dir, created)
            capture_calls.append(path)
            return {
                "status": "created",
                "trade_date": TD,
                "path": str(path),
                "source": "kis_account_snapshot",
                "snapshot": created,
            }

        monkeypatch.setattr(im, "capture_balance_snapshot", _fake_capture)
        im.send_daily_trading_summary(target_trade_date=TD)
        assert len(capture_calls) == 1
        assert len(discord["embeds"]) == 1
        assert "recovery" in discord["embeds"][0]["description"]


# ───────────────────────── migration ─────────────────────────

class TestMigration:
    def test_dry_run_makes_no_changes(self, balance_dir):
        """테스트 17: dry-run은 파일 변경 없음."""
        ops_files(balance_dir)
        before = sorted(p.name for p in balance_dir.rglob("*.json"))
        result = im.migrate_daily_balance_layout(trade_date=TD, apply=False)
        after = sorted(p.name for p in balance_dir.rglob("*.json"))
        assert result["dry_run"] is True
        assert before == after
        assert {a["action"] for a in result["actions"]} == {"would_copy"}

    def test_apply_copies_metadata_matched_without_delete(self, balance_dir):
        """테스트 12장/18: metadata 일치 스냅샷을 canonical로 복사, legacy 삭제 없음."""
        open_path, close_path = ops_files(balance_dir)
        result = im.migrate_daily_balance_layout(trade_date=TD, apply=True)
        assert result["dry_run"] is False

        canon_open = balance_dir / "canonical" / f"balance_open_trade_{TD}.json"
        canon_close = balance_dir / "canonical" / f"balance_close_trade_{TD}.json"
        assert canon_open.exists() and canon_close.exists()
        # 기존 legacy 파일 보존
        assert open_path.exists() and close_path.exists()

        migrated_open = json.loads(canon_open.read_text(encoding="utf-8"))
        assert migrated_open["migrated_from"] == f"balance_open_{CLOSE_KST}.json"
        assert migrated_open["trade_date"] == TD
        assert migrated_open["canonical"] is True
        assert migrated_open["legacy_alias"] is False
        assert "alias_of_trade_date" not in migrated_open

        migrated_close = json.loads(canon_close.read_text(encoding="utf-8"))
        assert migrated_close["migrated_from"] == f"balance_close_{TD}.json"

    def test_migration_apply_idempotent(self, balance_dir):
        ops_files(balance_dir)
        im.migrate_daily_balance_layout(trade_date=TD, apply=True)
        result = im.migrate_daily_balance_layout(trade_date=TD, apply=True)
        assert {a["action"] for a in result["actions"]} == {"skip_exists"}


# ───────────────────────── performance review 제외 ─────────────────────────

class TestPerformanceReviewAliasExclusion:
    def test_legacy_alias_excluded_from_review_artifacts(self, tmp_path):
        """테스트 14: legacy alias는 performance review 집계에서 제외."""
        from performance_review import collect_artifacts

        d = tmp_path / "daily_balances"
        d.mkdir()
        write_legacy(d, TD, make_snap("close", TD))
        write_legacy(
            d, CLOSE_KST,
            make_snap("close", TD, canonical=False, legacy_alias=True, alias_of=TD),
        )
        art = collect_artifacts("SP500", TD, TD, "pm", tmp_path)
        names = [p.name for p in art.daily_balance_paths]
        assert f"balance_close_{TD}.json" in names
        assert f"balance_close_{CLOSE_KST}.json" not in names
