"""Read-only GPT quality analysis joined to screener candidates and outcomes.

Never mutates gpt_trades JSON, prompts, model selection, trader ranking, or
integrated score Production behavior.
"""
from __future__ import annotations

import json
import logging
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("gpt_quality")

_GPT_TRADES_RE = re.compile(
    r"^gpt_trades_(?P<date>\d{8})_(?P<session>am|pm)_(?P<market>[A-Z0-9]+)\.json$",
    re.I,
)


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def _ticker_of(row: Dict[str, Any]) -> str:
    si = row.get("stock_info") if isinstance(row.get("stock_info"), dict) else {}
    return str(
        row.get("Ticker")
        or row.get("ticker")
        or si.get("Ticker")
        or si.get("ticker")
        or ""
    ).upper()


def normalize_gpt_decision(raw: Any) -> str:
    """Map GPT 결정 strings to BUY / HOLD / SELL / OTHER."""
    s = str(raw or "").strip()
    if not s:
        return "OTHER"
    low = s.lower()
    if s in ("매수",) or "매수" in s and "보류" not in s:
        return "BUY"
    if s in ("보류",) or "보류" in s:
        return "HOLD"
    if s in ("매도",) or "매도" in s or low in ("sell", "reject"):
        return "SELL"
    if low in ("buy", "long"):
        return "BUY"
    if low in ("hold", "pass"):
        return "HOLD"
    return "OTHER"


def parse_gpt_trades_payload(payload: Any) -> List[Dict[str, Any]]:
    """Adapt actual gpt_trades schema without guessing field names blindly."""
    rows: List[Dict[str, Any]] = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for key in ("plans", "results", "trades", "candidates", "items", "data"):
            if isinstance(payload.get(key), list):
                items = payload[key]
                break
        else:
            # Single wrapped plan
            if payload.get("결정") is not None or payload.get("stock_info") is not None:
                items = [payload]
            else:
                items = []
    else:
        items = []

    for i, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        si = item.get("stock_info") if isinstance(item.get("stock_info"), dict) else {}
        decision_raw = item.get("결정")
        if decision_raw is None:
            decision_raw = item.get("decision")
        ticker = _ticker_of(item)
        if not ticker:
            continue
        score = _safe_float(si.get("Score") if "Score" in si else item.get("Score") or item.get("score"))
        rows.append(
            {
                "ticker": ticker,
                "gpt_decision_raw": decision_raw,
                "gpt_decision": normalize_gpt_decision(decision_raw),
                "gpt_rank": item.get("rank") if item.get("rank") is not None else (i + 1),
                "screener_score": score,
                "integrated_score": _safe_float(item.get("integrated_score")),
                "gpt_reason": item.get("분석") or item.get("reason") or item.get("분석_요약"),
                "strategy_class": item.get("전략_클래스") or item.get("strategy"),
                "stock_info": si,
                "raw": item,
            }
        )
    return rows


def discover_gpt_trades_files(
    output_dir: Path,
    *,
    market: str,
    session: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Path]:
    out = Path(output_dir)
    found: List[Path] = []
    for p in sorted(out.glob("gpt_trades_*.json")):
        m = _GPT_TRADES_RE.match(p.name)
        if not m:
            continue
        if m.group("market").upper() != str(market).upper():
            continue
        if session and m.group("session").lower() != str(session).lower():
            continue
        d = m.group("date")
        if date_from and d < str(date_from):
            continue
        if date_to and d > str(date_to):
            continue
        found.append(p)
    return found


def load_gpt_observations(
    output_dir: Path,
    *,
    market: str,
    session: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    obs: List[Dict[str, Any]] = []
    for path in discover_gpt_trades_files(
        output_dir, market=market, session=session, date_from=date_from, date_to=date_to
    ):
        m = _GPT_TRADES_RE.match(path.name)
        if not m:
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            logger.warning("gpt_trades parse failed %s: %s", path, e)
            continue
        for row in parse_gpt_trades_payload(payload):
            row["trade_date"] = m.group("date")
            row["session"] = m.group("session").lower()
            row["market"] = m.group("market").upper()
            row["source_file"] = str(path)
            obs.append(row)
    return obs


def join_gpt_with_outcomes(
    gpt_rows: Sequence[Dict[str, Any]],
    outcome_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Join on trade_date + ticker (+ session/market when present)."""
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for o in outcome_rows:
        if str(o.get("candidate_type") or "") not in ("", "PRODUCTION"):
            # Prefer PRODUCTION outcomes for GPT join; fall back later
            if str(o.get("candidate_type") or "") != "PRODUCTION":
                continue
        key = (str(o.get("trade_date") or ""), str(o.get("ticker") or "").upper())
        idx[key] = o
    # Fallback index without candidate_type filter
    fallback: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for o in outcome_rows:
        key = (str(o.get("trade_date") or ""), str(o.get("ticker") or "").upper())
        fallback.setdefault(key, o)

    joined: List[Dict[str, Any]] = []
    for g in gpt_rows:
        key = (str(g.get("trade_date") or ""), str(g.get("ticker") or "").upper())
        o = idx.get(key) or fallback.get(key) or {}
        rec = dict(g)
        for h in (1, 3, 5, 10):
            rec[f"return_{h}d_pct"] = o.get(f"return_{h}d_pct")
        rec["max_drawdown_5d_pct"] = o.get("max_drawdown_5d_pct")
        rec["outcome_status"] = o.get("outcome_status")
        rec["maturity"] = o.get("maturity")
        rec["screener_candidate_type"] = o.get("candidate_type") or "PRODUCTION"
        if rec.get("screener_score") is None:
            rec["screener_score"] = o.get("decision_score")
        joined.append(rec)
    return joined


def _group_stats(rows: Sequence[Dict[str, Any]], horizon: int = 5) -> Dict[str, Any]:
    key = f"return_{horizon}d_pct"
    vals = [_safe_float(r.get(key)) for r in rows]
    matured = [v for v in vals if v is not None]
    tickers = {str(r.get("ticker") or "").upper() for r in rows if r.get("ticker")}
    n = len(rows)
    nm = len(matured)
    if nm == 0:
        return {
            "n": n,
            "unique_tickers": len(tickers),
            "matured": 0,
            "mean": None,
            "median": None,
            "win_rate": None,
        }
    matured_sorted = sorted(matured)
    wins = sum(1 for v in matured if v > 0)
    return {
        "n": n,
        "unique_tickers": len(tickers),
        "matured": nm,
        "mean": round(sum(matured) / nm, 6),
        "median": matured_sorted[nm // 2],
        "win_rate": round(wins / nm, 6),
    }


def evaluate_gpt_incremental_value(joined: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    by_dec: Dict[str, List[Dict[str, Any]]] = {"BUY": [], "HOLD": [], "SELL": [], "OTHER": []}
    for r in joined:
        by_dec.setdefault(str(r.get("gpt_decision") or "OTHER"), []).append(r)

    groups = {
        "SCREENER_PRODUCTION_ALL": list(joined),
        "GPT_BUY": by_dec.get("BUY") or [],
        "GPT_HOLD": by_dec.get("HOLD") or [],
        "GPT_SELL": by_dec.get("SELL") or [],
    }
    stats: Dict[str, Any] = {}
    for name, rows in groups.items():
        stats[name] = {f"{h}d": _group_stats(rows, h) for h in (1, 3, 5, 10)}

    buy_5 = stats["GPT_BUY"]["5d"].get("mean")
    all_5 = stats["SCREENER_PRODUCTION_ALL"]["5d"].get("mean")
    hold_5 = stats["GPT_HOLD"]["5d"].get("mean")
    incremental = None
    if buy_5 is not None and all_5 is not None:
        incremental = round(buy_5 - all_5, 6)
    buy_vs_hold = None
    if buy_5 is not None and hold_5 is not None:
        buy_vs_hold = round(buy_5 - hold_5, 6)

    buy_n = stats["GPT_BUY"]["5d"].get("matured") or 0
    all_n = stats["SCREENER_PRODUCTION_ALL"]["5d"].get("matured") or 0
    if buy_n < 10 or all_n < 10:
        status = "INSUFFICIENT_GPT_SAMPLE"
    elif incremental is None:
        status = "OBSERVATIONAL_ONLY"
    elif incremental > 0.5 and (buy_vs_hold or 0) > 0:
        status = "GPT_ADDS_VALUE_CANDIDATE"
    elif incremental < -0.5:
        status = "GPT_DEGRADES_SELECTION_CANDIDATE"
    else:
        status = "GPT_NO_CLEAR_VALUE"

    return {
        "groups": stats,
        "gpt_incremental_alpha_5d": incremental,
        "gpt_buy_vs_hold_5d": buy_vs_hold,
        "status": status,
        "gpt_analysis_days": len({r.get("trade_date") for r in joined}),
        "gpt_buy_count": len(by_dec.get("BUY") or []),
        "gpt_hold_count": len(by_dec.get("HOLD") or []),
        "gpt_sell_count": len(by_dec.get("SELL") or []),
        "used_by_trader": False,
        "production_unchanged": True,
        "note": "Observational only — does not change GPT weight or trader behavior.",
    }


def render_gpt_quality_markdown(report: Dict[str, Any]) -> str:
    lines = [
        f"# GPT Quality Report — {report.get('market')} "
        f"{report.get('date_from')}→{report.get('date_to')}",
        "",
        f"- analysis_days: {report.get('gpt_analysis_days')}",
        f"- BUY/HOLD/SELL: {report.get('gpt_buy_count')}/"
        f"{report.get('gpt_hold_count')}/{report.get('gpt_sell_count')}",
        f"- status: `{report.get('status')}`",
        f"- GPT incremental alpha 5D: {report.get('gpt_incremental_alpha_5d')}",
        f"- BUY vs HOLD 5D: {report.get('gpt_buy_vs_hold_5d')}",
        "",
        "## Groups (5D)",
    ]
    groups = report.get("groups") or {}
    for name, st in groups.items():
        s5 = (st or {}).get("5d") or {}
        lines.append(
            f"- {name}: n={s5.get('n')} matured={s5.get('matured')} "
            f"mean={s5.get('mean')} win={s5.get('win_rate')}"
        )
    lines.append("")
    lines.append(report.get("note") or "")
    lines.append("")
    return "\n".join(lines)


def build_gpt_quality_report(
    output_dir: Path,
    *,
    market: str,
    session: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    outcome_ledger_path: Optional[Path] = None,
) -> Dict[str, Any]:
    gpt_rows = load_gpt_observations(
        output_dir, market=market, session=session, date_from=date_from, date_to=date_to
    )
    outcomes: List[Dict[str, Any]] = []
    ledger = outcome_ledger_path or (
        Path(output_dir) / "quality" / "screener_candidate_observations.jsonl"
    )
    if ledger.exists():
        with open(ledger, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    outcomes.append(json.loads(line))
                except Exception:
                    continue
    joined = join_gpt_with_outcomes(gpt_rows, outcomes)
    eval_ = evaluate_gpt_incremental_value(joined)
    report = {
        "schema_version": 1,
        "market": market,
        "session": session,
        "date_from": date_from,
        "date_to": date_to,
        "observations": len(joined),
        **eval_,
    }
    return report


def write_gpt_quality_report(
    report: Dict[str, Any],
    output_dir: Path,
    *,
    stem: Optional[str] = None,
) -> Tuple[Path, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = stem or (
        f"gpt_quality_{report.get('date_from') or 'NA'}_"
        f"{report.get('date_to') or 'NA'}_{report.get('market') or 'MKT'}"
    )
    jp = out / f"{stem}.json"
    mp = out / f"{stem}.md"
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    mp.write_text(render_gpt_quality_markdown(report), encoding="utf-8")
    return jp, mp
