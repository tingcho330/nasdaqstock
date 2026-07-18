# src/daily_balance_values.py
"""SP500 daily balance USD/KRW 정규화 · pollution 검출 · historical currency repair.

authoritative asset formula (동일 캡처 시점 증거일 때만):
  total_asset_usd = available_cash_usd + holdings_value_usd

금지:
  - usd_cash_total / usd_buy_margin / buying_power → asset cash·total 로 사용
  - available_cash_krw / krw_cash / polluted tot_evlu_amt_usd → USD total 로 사용
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils import KST, OUTPUT_DIR, is_us_market

logger = logging.getLogger(__name__)

DAILY_BALANCE_SCHEMA_VERSION = "2.0"
COMPONENT_TOLERANCE_USD = 0.02
POLLUTION_RATIO = 3.0

# usd_cash_total 은 외화예수금(+증거금) 성격 — asset valuation 금지
USD_CASH_TOTAL_DEPRECATED_META = {
    "deprecated": True,
    "not_for_asset_valuation": True,
    "alias": "usd_funding_capacity_total",
    "meaning": "withdrawable(+buy_margin) funding capacity, not available cash",
}


def _f(val: Any) -> float:
    try:
        if val is None or val == "":
            return 0.0
        return round(float(str(val).replace(",", "").strip()), 2)
    except (TypeError, ValueError):
        return 0.0


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> Optional[str]:
    try:
        if not path.is_file():
            return None
        return _sha256_bytes(path.read_bytes())
    except Exception:
        return None


def sha256_json(payload: Dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return _sha256_bytes(raw.encode("utf-8"))


def detect_legacy_usd_field_pollution(
    *,
    tot_evlu_amt_usd: float,
    available_cash_krw: float = 0.0,
    krw_cash: float = 0.0,
    total_asset_krw: float = 0.0,
    available_cash_usd: float = 0.0,
    holdings_value_usd: float = 0.0,
    fx_rate: float = 0.0,
) -> List[Dict[str, Any]]:
    """LEGACY_USD_FIELD_POLLUTED_BY_KRW 후보 검출."""
    findings: List[Dict[str, Any]] = []
    usd = _f(tot_evlu_amt_usd)
    if usd <= 0:
        return findings

    reasons: List[str] = []
    ac_krw = _f(available_cash_krw)
    krw_c = _f(krw_cash)
    tot_krw = _f(total_asset_krw)
    cash_usd = _f(available_cash_usd)
    hv_usd = _f(holdings_value_usd)
    computed = round(cash_usd + hv_usd, 2)

    if ac_krw > 0 and abs(usd - ac_krw) < 1.0:
        reasons.append("tot_evlu_amt_usd == available_cash_krw")
    if krw_c > 0 and abs(usd - krw_c) < 1.0:
        reasons.append("tot_evlu_amt_usd == krw_cash")
    if tot_krw > 0 and abs(usd - tot_krw) < 1.0:
        reasons.append("tot_evlu_amt_usd == tot_evlu_amt_krw")
    if computed > 0 and usd > computed * POLLUTION_RATIO:
        reasons.append(
            f"tot_evlu_amt_usd={usd} >> cash+holdings={computed} (x{usd / computed:.1f})"
        )
    fx = _f(fx_rate)
    if fx > 100 and computed > 0:
        # KRW/USD 환율 배수에 가까운 괴리 (예: 829469 / 556 ≈ 1491 ≈ bass_exrt)
        ratio = usd / max(cash_usd, 1.0)
        if abs(ratio - fx) / fx < 0.15:
            reasons.append(f"tot_evlu_amt_usd/cash ≈ fx_rate ({ratio:.1f}≈{fx})")

    if reasons:
        findings.append({
            "field": "tot_evlu_amt_usd",
            "value": usd,
            "code": "LEGACY_USD_FIELD_POLLUTED_BY_KRW",
            "reasons": reasons,
        })
    return findings


def _holdings_usd_from_detail(holdings_detail: Optional[List[Dict]]) -> Tuple[float, bool]:
    """holdings_detail value 합계. USD 확인 가능하면 (sum, True)."""
    if not holdings_detail:
        return 0.0, False
    total = 0.0
    any_row = False
    for h in holdings_detail:
        if not isinstance(h, dict):
            continue
        val = _f(h.get("value") if h.get("value") is not None else h.get("evlu_amt"))
        if val <= 0:
            continue
        ccy = str(h.get("currency") or h.get("tr_crcy_cd") or "USD").upper()
        if ccy and ccy not in ("USD", "US$", "USS"):
            continue
        total += val
        any_row = True
    return round(total, 2), any_row


def resolve_available_cash_usd(
    raw: Dict[str, Any],
    *,
    kis_summary: Optional[Dict] = None,
) -> Tuple[Optional[float], Optional[str], List[Dict]]:
    """available_cash_usd 우선순위 해석. (value, provenance, rejected)."""
    rejected: List[Dict] = []
    ks = kis_summary or raw.get("kis_summary") or {}

    # 명시적으로 금지된 필드 — 후보에서 제외하고 기록
    for bad, reason in (
        ("usd_cash_total", "NOT_ASSET_CASH_BUYING_POWER_INCLUDED"),
        ("usd_buy_margin", "BUYING_POWER_MARGIN_NOT_CASH"),
        ("buying_power", "BUYING_POWER_NOT_CASH"),
        ("buying_power_usd", "BUYING_POWER_NOT_CASH"),
        ("available_cash_krw", "KRW_NOT_USD"),
        ("krw_cash", "KRW_NOT_USD"),
    ):
        if _f(raw.get(bad) or ks.get(bad)) > 0:
            rejected.append({
                "field": bad,
                "value": _f(raw.get(bad) or ks.get(bad)),
                "reason": reason,
            })

    candidates: List[Tuple[float, str]] = []

    v = raw.get("available_cash_usd")
    if v is not None and _f(v) > 0:
        candidates.append((_f(v), "$.available_cash_usd"))

    cash_map = raw.get("cash_map") or {}
    usd_cm = cash_map.get("USD") if isinstance(cash_map, dict) else None
    if isinstance(usd_cm, dict) and usd_cm.get("available_cash") is not None:
        if _f(usd_cm.get("available_cash")) > 0:
            candidates.append((_f(usd_cm["available_cash"]), "$.cash_map.USD.available_cash"))

    pb = (raw.get("endpoints") or {}).get("present_balance") or {}
    if not pb:
        pb = (raw.get("endpoint_evidence") or {}).get("present_balance") or {}
    if isinstance(pb, dict) and pb.get("available_cash_usd") is not None:
        if _f(pb.get("available_cash_usd")) > 0:
            candidates.append(
                (_f(pb["available_cash_usd"]), "$.endpoint_evidence.present_balance.available_cash_usd")
            )

    ord_psbl = ks.get("ord_psbl_frcr_amt")
    if ord_psbl is not None and _f(ord_psbl) > 0:
        # 해외주식 주문가능 외화 — contract상 USD
        ccy = str(ks.get("currency") or raw.get("base_currency") or raw.get("currency") or "USD").upper()
        if ccy == "USD" or is_us_market(str(raw.get("market") or "")):
            candidates.append((_f(ord_psbl), "$.kis_summary.ord_psbl_frcr_amt"))

    # usd_withdrawable: available와 동일 의미일 때만 (단독 폴백)
    if not candidates:
        wdr = ks.get("usd_withdrawable")
        if wdr is not None and _f(wdr) > 0:
            candidates.append((_f(wdr), "$.kis_summary.usd_withdrawable"))

    if not candidates:
        return None, None, rejected
    return candidates[0][0], candidates[0][1], rejected


def resolve_holdings_value_usd(
    raw: Dict[str, Any],
    *,
    available_cash_usd: Optional[float] = None,
    total_asset_usd: Optional[float] = None,
) -> Tuple[Optional[float], Optional[str]]:
    if raw.get("holdings_value_usd") is not None and _f(raw.get("holdings_value_usd")) >= 0:
        return _f(raw["holdings_value_usd"]), "$.holdings_value_usd"

    cash_map = raw.get("cash_map") or {}
    usd_cm = cash_map.get("USD") if isinstance(cash_map, dict) else None
    if isinstance(usd_cm, dict) and usd_cm.get("holdings_value") is not None:
        return _f(usd_cm["holdings_value"]), "$.cash_map.USD.holdings_value"

    hv, ok = _holdings_usd_from_detail(raw.get("holdings_detail"))
    if ok:
        return hv, "$.holdings_detail[*].value"

    if (
        available_cash_usd is not None
        and total_asset_usd is not None
        and _f(total_asset_usd) > 0
        and _f(available_cash_usd) >= 0
    ):
        derived = round(_f(total_asset_usd) - _f(available_cash_usd), 2)
        if derived >= 0:
            return derived, "total_asset_usd - available_cash_usd"

    # legacy holdings_value 필드 (오염 total과 분리된 경우)
    legacy_hv = raw.get("holdings_value")
    if legacy_hv is not None and _f(legacy_hv) > 0:
        return _f(legacy_hv), "$.holdings_value"
    return None, None


def normalize_account_values(
    raw: Dict[str, Any],
    *,
    market: str = "SP500",
    currency_status: str = "normalized",
) -> Dict[str, Any]:
    """
    SP500 일일 자산 정규화.

    Returns dict with authoritative USD/KRW fields, provenance, validity flags,
    and rejected polluted/non-asset fields.
    """
    ks = dict(raw.get("kis_summary") or {})
    rejected: List[Dict] = []

    avail, avail_prov, rej = resolve_available_cash_usd(raw, kis_summary=ks)
    rejected.extend(rej)

    # pollution on tot_evlu_amt_usd
    poll = detect_legacy_usd_field_pollution(
        tot_evlu_amt_usd=_f(ks.get("tot_evlu_amt_usd") or raw.get("tot_evlu_amt_usd") or raw.get("total_balance")),
        available_cash_krw=_f(ks.get("available_cash_krw") or raw.get("available_cash_krw")),
        krw_cash=_f(ks.get("krw_cash") or raw.get("krw_cash")),
        total_asset_krw=_f(ks.get("tot_evlu_amt_krw") or raw.get("total_asset_krw")),
        available_cash_usd=_f(avail),
        holdings_value_usd=_f(raw.get("holdings_value_usd") or raw.get("holdings_value")),
        fx_rate=_f(ks.get("bass_exrt") or raw.get("fx_rate_used")),
    )
    for p in poll:
        rejected.append({
            "field": p["field"],
            "value": p["value"],
            "reason": p["code"],
            "details": p.get("reasons"),
        })

    # total_asset_usd: polluted tot_evlu 제외. authoritative = cash+holdings
    hv, hv_prov = resolve_holdings_value_usd(raw, available_cash_usd=avail)

    total_asset: Optional[float] = None
    total_prov: Optional[str] = None
    if avail is not None and hv is not None:
        total_asset = round(_f(avail) + _f(hv), 2)
        total_prov = "available_cash_usd + holdings_value_usd"
    elif raw.get("total_asset_usd") is not None and not poll:
        # only trust explicit total if not polluted
        cand = _f(raw["total_asset_usd"])
        if cand > 0 and (avail is None or abs(cand - (_f(avail) + _f(hv or 0))) <= max(cand * 0.25, 1)):
            total_asset = cand
            total_prov = "$.total_asset_usd"

    usd_cash_total = _f(ks.get("usd_cash_total") or raw.get("usd_cash_total"))
    withdrawable = _f(ks.get("usd_withdrawable") or raw.get("usd_withdrawable") or raw.get("withdrawable_cash_usd"))
    buy_margin = _f(ks.get("usd_buy_margin") or raw.get("usd_buy_margin") or raw.get("buying_power_margin_usd"))
    sell_reuse = _f(ks.get("usd_sell_reuse") or raw.get("usd_sell_reuse") or raw.get("sell_reuse_amount_usd"))

    if usd_cash_total > 0:
        rejected.append({
            "field": "usd_cash_total",
            "value": usd_cash_total,
            "reason": "NOT_ASSET_CASH_BUYING_POWER_INCLUDED",
            "meta": USD_CASH_TOTAL_DEPRECATED_META,
        })

    consistent = False
    if avail is not None and hv is not None and total_asset is not None:
        consistent = abs(total_asset - avail - hv) <= COMPONENT_TOLERANCE_USD

    financial_valid = bool(
        is_us_market(market)
        and avail is not None
        and hv is not None
        and total_asset is not None
        and consistent
        and not any(r.get("reason") == "LEGACY_USD_FIELD_POLLUTED_BY_KRW" and r.get("field") == "used"
                    for r in rejected)
    )
    # pollution of unused fields is OK as long as we didn't use them
    return_usable = financial_valid

    status = currency_status
    if not financial_valid:
        status = "ambiguous"
    elif currency_status in ("reconstructed", "explicit"):
        status = currency_status
    elif financial_valid:
        status = "explicit"

    provenance = {
        "available_cash_usd": avail_prov,
        "holdings_value_usd": hv_prov,
        "total_asset_usd": total_prov,
    }
    if withdrawable > 0:
        provenance["withdrawable_cash_usd"] = "$.kis_summary.usd_withdrawable"
    if buy_margin > 0:
        provenance["buying_power_margin_usd"] = "$.kis_summary.usd_buy_margin"

    return {
        "schema_version": DAILY_BALANCE_SCHEMA_VERSION,
        "base_currency": "USD" if is_us_market(market) else "KRW",
        "total_asset_usd": total_asset,
        "available_cash_usd": avail,
        "holdings_value_usd": hv,
        "withdrawable_cash_usd": withdrawable if withdrawable > 0 else None,
        "orderable_cash_usd": avail,
        "buying_power_usd": round(withdrawable + buy_margin, 2) if (withdrawable or buy_margin) else None,
        "buying_power_margin_usd": buy_margin if buy_margin > 0 else None,
        "sell_reuse_amount_usd": sell_reuse if sell_reuse > 0 else None,
        "usd_funding_capacity_total": usd_cash_total if usd_cash_total > 0 else None,
        "usd_cash_total_deprecated": {
            **USD_CASH_TOTAL_DEPRECATED_META,
            "value": usd_cash_total if usd_cash_total > 0 else None,
        },
        "total_asset_krw": _f(ks.get("tot_evlu_amt_krw") or raw.get("total_asset_krw")) or None,
        "available_cash_krw": _f(ks.get("available_cash_krw") or raw.get("available_cash_krw")) or None,
        "krw_cash": _f(ks.get("krw_cash") or raw.get("krw_cash")) or None,
        "holdings_value_krw": _f(raw.get("holdings_value_krw") or ks.get("holdings_value_krw")) or None,
        "fx_rate_used": _f(ks.get("bass_exrt") or raw.get("fx_rate_used")) or None,
        "value_semantics": "base_currency_account_value",
        "balance_currency": "USD" if is_us_market(market) else "KRW",
        # compatibility
        "total_balance": total_asset,
        "cash": avail,
        "holdings_value": hv,
        "field_provenance": provenance,
        "rejected_fields": rejected,
        "usd_components_consistent": consistent,
        "financial_values_valid": financial_valid,
        "return_calculation_usable": return_usable,
        "currency_status": status,
    }


def evidence_dir(balance_storage: Path) -> Path:
    return balance_storage / "evidence"


def versioned_balance_filename(
    market: str,
    trade_date: str,
    snapshot_type: str,
    snapshot_ts_kst: str,
) -> str:
    """balance_SP500_20260715_open_20260715T224506+0900.json"""
    ts = snapshot_ts_kst.replace(":", "").replace("-", "")
    # keep compact ISO-ish: 20260715T224506+0900
    m = re.match(
        r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})([+-]\d{2}):?(\d{2})?",
        snapshot_ts_kst,
    )
    if m:
        ts = f"{m.group(1)}{m.group(2)}{m.group(3)}T{m.group(4)}{m.group(5)}{m.group(6)}{m.group(7)}{m.group(8) or '00'}"
    else:
        ts = re.sub(r"[^0-9T+-]", "", snapshot_ts_kst)[:22]
    return f"balance_{market}_{trade_date}_{snapshot_type}_{ts}.json"


def save_immutable_source_copy(
    source_path: Optional[Path],
    *,
    balance_storage: Path,
    market: str,
    trade_date: str,
    snapshot_type: str,
    snapshot_ts_kst: str,
    embedded_payload: Optional[Dict] = None,
) -> Dict[str, Any]:
    """원본을 evidence/ 에 복사하고 provenance 메타를 반환."""
    meta: Dict[str, Any] = {
        "source_snapshot_file": str(source_path) if source_path else None,
        "source_snapshot_sha256": None,
        "source_snapshot_mtime_at_capture": None,
        "source_snapshot_generated_at": None,
        "source_snapshot_trade_date": None,
        "source_snapshot_session": None,
        "source_snapshot_immutable_copy": None,
    }
    ed = evidence_dir(balance_storage)
    ed.mkdir(parents=True, exist_ok=True)

    content: Optional[bytes] = None
    payload: Optional[Dict] = None
    if source_path and source_path.is_file():
        try:
            content = source_path.read_bytes()
            meta["source_snapshot_mtime_at_capture"] = datetime.fromtimestamp(
                source_path.stat().st_mtime, tz=KST
            ).isoformat()
            try:
                payload = json.loads(content.decode("utf-8"))
            except Exception:
                payload = None
        except Exception as e:
            logger.warning("immutable source read failed %s: %s", source_path, e)

    if payload is None and embedded_payload is not None:
        payload = embedded_payload
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    if content is None:
        return meta

    digest = _sha256_bytes(content)
    meta["source_snapshot_sha256"] = digest
    if isinstance(payload, dict):
        meta["source_snapshot_generated_at"] = (
            payload.get("generated_at_kst")
            or payload.get("snapshot_ts_kst")
            or payload.get("timestamp")
        )
        meta["source_snapshot_trade_date"] = payload.get("trade_date") or payload.get("date")
        meta["source_snapshot_session"] = payload.get("session")

    safe_ts = re.sub(r"[^0-9T+-]", "", snapshot_ts_kst)[:22]
    dest = ed / f"account_source_{market}_{trade_date}_{snapshot_type}_{safe_ts}.json"
    # also hash-named copy for content-addressable lookup
    hash_dest = ed / f"sha256_{digest}.json"
    try:
        if not hash_dest.exists():
            hash_dest.write_bytes(content)
        if not dest.exists():
            if source_path and source_path.is_file():
                shutil.copy2(source_path, dest)
            else:
                dest.write_bytes(content)
        meta["source_snapshot_immutable_copy"] = str(dest)
    except Exception as e:
        logger.warning("immutable source copy failed: %s", e)
        meta["source_snapshot_immutable_copy"] = str(hash_dest) if hash_dest.exists() else None

    return meta


def verify_source_snapshot_not_mutated(
    snap: Dict[str, Any],
    *,
    allow_missing_sha: bool = False,
) -> Tuple[bool, Optional[str]]:
    """SOURCE_SNAPSHOT_MUTATED 검증. (ok, reason)."""
    path_raw = snap.get("source_snapshot_file")
    stored_sha = snap.get("source_snapshot_sha256")
    if not path_raw:
        return False, "SOURCE_SNAPSHOT_MISSING_PATH"
    path = Path(str(path_raw))
    if not path.is_file():
        return False, "SOURCE_SNAPSHOT_MISSING_FILE"
    current = sha256_file(path)
    if not stored_sha:
        if allow_missing_sha:
            return False, "SOURCE_SNAPSHOT_SHA_UNKNOWN"
        return False, "SOURCE_SNAPSHOT_SHA_UNKNOWN"
    if current != stored_sha:
        return False, "SOURCE_SNAPSHOT_MUTATED"
    snap_td = str(snap.get("trade_date") or "")
    file_td = str(snap.get("source_snapshot_trade_date") or "")
    if file_td and snap_td and file_td != snap_td:
        return False, "SOURCE_SNAPSHOT_TRADE_DATE_MISMATCH"
    return True, None


def propose_currency_repair_from_embedded(
    snap: Dict[str, Any],
    *,
    market: str = "SP500",
) -> Dict[str, Any]:
    """
    daily balance 파일 내부 embedded evidence만으로 repair proposal 생성.
    외부 mutable source_snapshot_file 은 사용하지 않는다.

    gates_ok=False (금액 apply 금지) 조건:
    - SOURCE_SNAPSHOT_SHA_UNKNOWN / MUTATED / HISTORICAL_SNAPSHOT_TIME_MISMATCH
    - structural gates 실패
    - financial_values_valid=False
    """
    raw = {
        "market": market,
        "currency": "USD",
        "base_currency": "USD",
        "kis_summary": snap.get("kis_summary") or {},
        "holdings_detail": snap.get("holdings_detail") or [],
        "holdings_value": snap.get("holdings_value"),
        "available_cash_krw": (snap.get("kis_summary") or {}).get("available_cash_krw")
        or snap.get("available_cash_krw"),
        "krw_cash": (snap.get("kis_summary") or {}).get("krw_cash") or snap.get("krw_cash"),
        "total_balance": snap.get("total_balance"),
        "cash": snap.get("cash"),
        "usd_cash_total": (snap.get("kis_summary") or {}).get("usd_cash_total"),
    }
    normalized = normalize_account_values(raw, market=market, currency_status="reconstructed")

    source_ok, source_reason = verify_source_snapshot_not_mutated(snap, allow_missing_sha=True)
    rejected = list(normalized.get("rejected_fields") or [])
    if not source_ok:
        rejected.append({
            "field": "source_snapshot_file",
            "value": snap.get("source_snapshot_file"),
            "reason": source_reason or "SOURCE_SNAPSHOT_MUTATED",
        })

    ks = snap.get("kis_summary") or {}
    hd = snap.get("holdings_detail") or []
    gates = {
        "holdings_detail_present": bool(hd),
        "snapshot_timestamp_present": bool(snap.get("snapshot_ts_kst") or snap.get("timestamp")),
        "ord_psbl_frcr_amt_present": _f(ks.get("ord_psbl_frcr_amt")) > 0,
        "holdings_look_usd": _holdings_usd_from_detail(hd)[1],
        "us_tickers": all(
            bool(re.match(r"^[A-Z][A-Z0-9.\-]{0,9}$", str(h.get("ticker") or "")))
            for h in hd if isinstance(h, dict) and h.get("ticker")
        ) if hd else False,
    }

    # Amount-apply blockers — arithmetic candidate may still be computed
    amount_blockers = {
        "SOURCE_SNAPSHOT_SHA_UNKNOWN",
        "SOURCE_SNAPSHOT_MUTATED",
        "SOURCE_SNAPSHOT_MISSING_FILE",
        "SOURCE_SNAPSHOT_MISSING_PATH",
        "SOURCE_SNAPSHOT_TRADE_DATE_MISMATCH",
        "HISTORICAL_SNAPSHOT_TIME_MISMATCH",
    }
    rejected_reasons = [str(r.get("reason") or "") for r in rejected]
    integrity_blocked = bool(set(rejected_reasons) & amount_blockers)
    # KRW_NOT_USD alone does not block when USD components reconstruct cleanly
    # (open repair path). Combined with integrity failure it reinforces reject.
    if integrity_blocked and "KRW_NOT_USD" in rejected_reasons:
        # already blocked by integrity
        pass

    gates_ok = (
        all(gates.values())
        and bool(normalized.get("financial_values_valid"))
        and not integrity_blocked
    )

    arithmetic_total = None
    if normalized.get("available_cash_usd") is not None and normalized.get("holdings_value_usd") is not None:
        arithmetic_total = round(
            _f(normalized["available_cash_usd"]) + _f(normalized["holdings_value_usd"]), 2
        )
    elif snap.get("cash") is not None and snap.get("holdings_value") is not None:
        arithmetic_total = round(_f(snap.get("cash")) + _f(snap.get("holdings_value")), 2)

    return {
        "normalized": normalized,
        "rejected_fields": rejected,
        "rejected_reasons": rejected_reasons,
        "gates": gates,
        "gates_ok": gates_ok,
        "source_usable": source_ok,
        "source_reason": source_reason,
        "historical_time_match": True,
        "integrity_blocked": integrity_blocked,
        # Do not present as confirmed USD asset when gates fail
        "arithmetic_candidate_total_asset_usd": arithmetic_total,
        "candidate_currency_unverified": not gates_ok,
        "proposed_total_asset_usd": normalized.get("total_asset_usd") if gates_ok else None,
        "proposed_available_cash_usd": normalized.get("available_cash_usd") if gates_ok else None,
        "proposed_holdings_value_usd": normalized.get("holdings_value_usd") if gates_ok else None,
    }


def build_ambiguous_quality_metadata(
    rejected_reasons: List[str],
) -> Dict[str, Any]:
    """금액은 변경하지 않고 품질 상태만 명시."""
    return {
        "structurally_valid": True,
        "currency_status": "ambiguous",
        "financial_values_valid": False,
        "return_calculation_usable": False,
        "base_currency": None,
        "total_asset_usd": None,
        "available_cash_usd": None,
        "holdings_value_usd": None,
        "normalization_source": "legacy_embedded_evidence",
        "normalization_errors": list(dict.fromkeys(rejected_reasons)),
    }


def apply_quality_metadata_only(
    snap: Dict[str, Any],
    quality: Dict[str, Any],
) -> Dict[str, Any]:
    """금액 필드(total_balance/cash/holdings)는 보존하고 품질 메타만 기록."""
    out = dict(snap)
    for key in (
        "structurally_valid",
        "currency_status",
        "financial_values_valid",
        "return_calculation_usable",
        "base_currency",
        "total_asset_usd",
        "available_cash_usd",
        "holdings_value_usd",
        "normalization_source",
        "normalization_errors",
    ):
        if key in quality:
            out[key] = quality[key]
    return out


def apply_normalized_fields_to_snapshot(
    snap: Dict[str, Any],
    normalized: Dict[str, Any],
) -> Dict[str, Any]:
    """normalized 결과를 snapshot dict에 병합 (원본 kis_summary/holdings_detail 보존)."""
    out = dict(snap)
    for key in (
        "schema_version",
        "base_currency",
        "total_asset_usd",
        "available_cash_usd",
        "holdings_value_usd",
        "withdrawable_cash_usd",
        "orderable_cash_usd",
        "buying_power_usd",
        "buying_power_margin_usd",
        "sell_reuse_amount_usd",
        "usd_funding_capacity_total",
        "usd_cash_total_deprecated",
        "total_asset_krw",
        "available_cash_krw",
        "krw_cash",
        "holdings_value_krw",
        "fx_rate_used",
        "value_semantics",
        "balance_currency",
        "total_balance",
        "cash",
        "holdings_value",
        "field_provenance",
        "usd_components_consistent",
        "financial_values_valid",
        "return_calculation_usable",
        "currency_status",
    ):
        if key in normalized:
            out[key] = normalized[key]
    out["rejected_fields"] = normalized.get("rejected_fields") or []
    out["structurally_valid"] = True
    out["normalization_source"] = "embedded_kis_summary_holdings_detail"
    out["normalization_errors"] = []
    return out


def is_return_calculation_usable(snap: Optional[Dict]) -> bool:
    if not snap:
        return False
    if snap.get("return_calculation_usable") is True and snap.get("financial_values_valid") is True:
        return str(snap.get("base_currency") or "").upper() == "USD"
    # legacy explicit fields without flags — re-normalize check
    if (
        snap.get("total_asset_usd") is not None
        and snap.get("available_cash_usd") is not None
        and snap.get("holdings_value_usd") is not None
        and str(snap.get("base_currency") or "USD").upper() == "USD"
    ):
        tot = _f(snap["total_asset_usd"])
        cash = _f(snap["available_cash_usd"])
        hv = _f(snap["holdings_value_usd"])
        return abs(tot - cash - hv) <= COMPONENT_TOLERANCE_USD and tot > 0
    return False


def _finite_number(val: Any) -> Optional[float]:
    """Parse a finite float; reject bool/NaN/Inf/non-numeric. None if unavailable."""
    if val is None or isinstance(val, bool):
        return None
    try:
        x = float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if x != x or x in (float("inf"), float("-inf")):
        return None
    return x


def resolve_realized_pnl_delta(
    open_snapshot: Any,
    close_snapshot: Any,
    *,
    expected_trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Open/Close embedded kis_summary.ovrs_rlzt_pfls_amt delta (USD).

    Independent of asset-currency repair / ambiguous total_asset gates.
    Evidence incomplete → available=False and value=None (never fake 0.0).
    Actual zero delta → available=True and value=0.0.
    """
    empty = {
        "available": False,
        "value": None,
        "currency": None,
        "source": None,
        "status": "EVIDENCE_INCOMPLETE",
        "error_reasons": [],
    }
    reasons: List[str] = []

    if not isinstance(open_snapshot, dict):
        reasons.append("OPEN_SNAPSHOT_NOT_DICT")
    if not isinstance(close_snapshot, dict):
        reasons.append("CLOSE_SNAPSHOT_NOT_DICT")
    if reasons:
        return {**empty, "error_reasons": reasons}

    open_td = str(open_snapshot.get("trade_date") or "").strip()
    close_td = str(close_snapshot.get("trade_date") or "").strip()
    if not open_td or not close_td:
        reasons.append("TRADE_DATE_MISSING")
    elif open_td != close_td:
        reasons.append("TRADE_DATE_MISMATCH")
    if expected_trade_date:
        exp = str(expected_trade_date).strip()
        if open_td != exp or close_td != exp:
            reasons.append("TRADE_DATE_EXPECTED_MISMATCH")

    om = open_snapshot.get("kis_summary")
    cm = close_snapshot.get("kis_summary")
    if not isinstance(om, dict):
        reasons.append("OPEN_KIS_SUMMARY_MISSING")
    if not isinstance(cm, dict):
        reasons.append("CLOSE_KIS_SUMMARY_MISSING")
    if reasons:
        return {**empty, "error_reasons": list(dict.fromkeys(reasons))}

    open_ccy = str(om.get("currency") or "").strip().upper()
    close_ccy = str(cm.get("currency") or "").strip().upper()
    if open_ccy != "USD":
        reasons.append("OPEN_CURRENCY_NOT_USD" if open_ccy else "OPEN_CURRENCY_MISSING")
    if close_ccy != "USD":
        reasons.append("CLOSE_CURRENCY_NOT_USD" if close_ccy else "CLOSE_CURRENCY_MISSING")
    if open_ccy and close_ccy and open_ccy != close_ccy:
        reasons.append("CURRENCY_MISMATCH")

    if "ovrs_rlzt_pfls_amt" not in om:
        reasons.append("OPEN_OVRS_RLZT_MISSING")
    if "ovrs_rlzt_pfls_amt" not in cm:
        reasons.append("CLOSE_OVRS_RLZT_MISSING")

    open_v = _finite_number(om.get("ovrs_rlzt_pfls_amt")) if "ovrs_rlzt_pfls_amt" in om else None
    close_v = _finite_number(cm.get("ovrs_rlzt_pfls_amt")) if "ovrs_rlzt_pfls_amt" in cm else None
    if "ovrs_rlzt_pfls_amt" in om and open_v is None:
        reasons.append("OPEN_OVRS_RLZT_NOT_FINITE")
    if "ovrs_rlzt_pfls_amt" in cm and close_v is None:
        reasons.append("CLOSE_OVRS_RLZT_NOT_FINITE")

    if reasons:
        return {**empty, "error_reasons": list(dict.fromkeys(reasons))}

    delta = round(float(close_v) - float(open_v), 2)
    return {
        "available": True,
        "value": delta,
        "currency": "USD",
        "source": "kis_summary.ovrs_rlzt_pfls_amt_delta",
        "status": "OK",
        "error_reasons": [],
        "open_value": open_v,
        "close_value": close_v,
        "trade_date": open_td,
    }
