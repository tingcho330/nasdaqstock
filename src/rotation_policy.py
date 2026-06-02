#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""회전 매매 공통 정책: 최소 보유일, Δscore, 비용, 페어 상한."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from utils import KST, _to_float, _to_int, check_min_holding_period

logger = logging.getLogger(__name__)

SettingsLike = Union[Dict[str, Any], Any]


@dataclass
class RotationPair:
    sell_ticker: str
    sell_name: str
    sell_qty: int
    sell_score: float
    buy_plan: Dict[str, Any]
    buy_score: float
    delta_score: float
    est_proceeds: int = 0
    sell_extra: Dict[str, Any] = field(default_factory=dict)


def _settings_dict(settings: SettingsLike) -> Dict[str, Any]:
    if hasattr(settings, "_config"):
        return settings._config  # type: ignore[attr-defined]
    if isinstance(settings, dict):
        return settings
    return {}


def rotation_config(settings: SettingsLike) -> Dict[str, Any]:
    if hasattr(settings, "rotation"):
        return getattr(settings, "rotation") or {}
    return _settings_dict(settings).get("rotation") or {}


def max_pairs_per_run(settings: SettingsLike) -> int:
    return max(1, int(rotation_config(settings).get("max_pairs_per_run", 1)))


def min_holding_days(settings: SettingsLike) -> int:
    return int(rotation_config(settings).get("min_holding_days", 0))


def delta_score_min(settings: SettingsLike) -> float:
    cfg = rotation_config(settings)
    fb = _settings_dict(settings).get("rebalance_params", {}).get("fallback_delta_score_min")
    if fb is not None:
        return float(fb)
    return float(cfg.get("delta_score_min", 0.10))


def fee_buffer_pct(settings: SettingsLike) -> float:
    tp = _settings_dict(settings).get("trading_params") or {}
    return float(tp.get("fee_buffer_pct", 0.005))


def sell_eligible_for_rotation(
    ticker: str,
    settings: SettingsLike,
    current_time: Optional[datetime] = None,
) -> Tuple[bool, int]:
    """최소 보유일 충족 시에만 회전 매도 허용."""
    days = min_holding_days(settings)
    if days <= 0:
        return True, 0
    return check_min_holding_period(ticker, days, current_time)


def resolve_delta_threshold(
    settings: SettingsLike,
    market_state: Any = None,
    market_analyzer: Any = None,
) -> float:
    base = delta_score_min(settings)
    if market_state is None or market_analyzer is None:
        return base
    if not rotation_config(settings).get("use_dynamic_threshold", True):
        return base
    try:
        return float(market_analyzer.calculate_dynamic_threshold(base, market_state))
    except Exception as e:
        logger.warning("동적 임계값 계산 실패, 기본값 사용: %s", e)
        return base


def pair_passes_economics(
    sell_proceeds: int,
    buy_price: int,
    sell_score: float,
    buy_score: float,
    settings: SettingsLike,
) -> bool:
    from screener_core import calculate_net_profit_rotation

    delta = buy_score - sell_score
    expected_gain = delta * sell_proceeds / 100.0
    judgment = calculate_net_profit_rotation(
        sell_ticker="",
        buy_ticker="",
        sell_amount=sell_proceeds,
        buy_amount=buy_price,
        expected_gain=expected_gain,
        settings=_settings_dict(settings),
    )
    return bool(judgment.get("should_rotate", False))


def pair_passes_budget(usable_cash: int, sell_proceeds: int, buy_price: int, settings: SettingsLike) -> bool:
    if buy_price <= 0:
        return False
    buffer = fee_buffer_pct(settings)
    affordable = int((usable_cash + sell_proceeds) * (1 - buffer))
    return buy_price <= affordable


def lists_to_pairs(to_sell_list: List[Dict], to_buy_plans: List[Dict]) -> List[RotationPair]:
    pairs: List[RotationPair] = []
    n = min(len(to_sell_list), len(to_buy_plans))
    for i in range(n):
        s = to_sell_list[i]
        b = to_buy_plans[i]
        info = b.get("stock_info", {}) if isinstance(b, dict) else {}
        sell_ticker = str(s.get("ticker", "")).zfill(6)
        buy_ticker = str(info.get("Ticker", "")).zfill(6)
        if not sell_ticker or not buy_ticker or sell_ticker == buy_ticker:
            continue
        sell_score = _to_float(s.get("old_score", s.get("cached_score", 0.0)), 0.0)
        buy_score = _to_float(s.get("new_score", info.get("Score", 0.0)), 0.0)
        if buy_score <= 0:
            buy_score = _to_float(info.get("Score", 0.0), 0.0)
        delta = buy_score - sell_score if s.get("score_delta") is None else _to_float(
            s.get("score_delta"), buy_score - sell_score
        )
        qty = _to_int(s.get("qty", 0))
        est = _to_int(s.get("est_proceeds", 0))
        if est <= 0 and s.get("prpr") is not None:
            est = _to_int(s.get("prpr", 0)) * qty
        extra = {k: v for k, v in s.items() if k not in ("ticker", "name", "qty")}
        pairs.append(
            RotationPair(
                sell_ticker=sell_ticker,
                sell_name=s.get("name", "N/A"),
                sell_qty=qty,
                sell_score=sell_score,
                buy_plan=b,
                buy_score=buy_score,
                delta_score=delta,
                est_proceeds=est,
                sell_extra=extra,
            )
        )
    return pairs


def pairs_to_legacy_lists(pairs: List[RotationPair]) -> Tuple[List[Dict], List[Dict]]:
    to_buy: List[Dict] = []
    to_sell: List[Dict] = []
    for p in pairs:
        to_buy.append(p.buy_plan)
        info = p.buy_plan.get("stock_info", {})
        row = {
            "ticker": p.sell_ticker,
            "name": p.sell_name,
            "qty": p.sell_qty,
            "old_score": p.sell_score,
            "new_score": p.buy_score,
            "score_delta": p.delta_score,
            "new_ticker": str(info.get("Ticker", "")).zfill(6),
            "est_proceeds": p.est_proceeds,
        }
        row.update(p.sell_extra)
        to_sell.append(row)
    return to_buy, to_sell


def filter_valid_pairs(
    pairs: List[RotationPair],
    settings: SettingsLike,
    *,
    usable_cash: int = 0,
    delta_threshold: Optional[float] = None,
    check_economics: bool = True,
    current_time: Optional[datetime] = None,
) -> List[RotationPair]:
    if current_time is None:
        current_time = datetime.now(KST)
    thr = delta_threshold if delta_threshold is not None else delta_score_min(settings)
    valid: List[RotationPair] = []
    for p in pairs:
        ok_hold, hold_days = sell_eligible_for_rotation(p.sell_ticker, settings, current_time)
        if not ok_hold:
            logger.info(
                "회전 제외(최소 보유일): %s(%s) 보유 %s일 < %s일",
                p.sell_name,
                p.sell_ticker,
                hold_days,
                min_holding_days(settings),
            )
            continue
        if p.delta_score < thr:
            logger.info(
                "회전 제외(Δscore): %s→%s Δ=%.3f < %.3f",
                p.sell_ticker,
                str(p.buy_plan.get("stock_info", {}).get("Ticker", "")).zfill(6),
                p.delta_score,
                thr,
            )
            continue
        buy_price = _to_int(p.buy_plan.get("stock_info", {}).get("Price", 0))
        if not pair_passes_budget(usable_cash, p.est_proceeds, buy_price, settings):
            logger.info("회전 제외(예산): 매수가 %s > 가용", f"{buy_price:,}")
            continue
        if check_economics and p.est_proceeds > 0:
            if not pair_passes_economics(p.est_proceeds, buy_price, p.sell_score, p.buy_score, settings):
                continue
        valid.append(p)
    return valid


def cap_pairs(pairs: List[RotationPair], limit: int) -> List[RotationPair]:
    if limit <= 0:
        return []
    return pairs[:limit]


def apply_rotation_policy(
    to_sell_list: List[Dict],
    to_buy_plans: List[Dict],
    settings: SettingsLike,
    *,
    usable_cash: int = 0,
    max_pairs: Optional[int] = None,
    delta_threshold: Optional[float] = None,
    check_economics: bool = True,
    market_state: Any = None,
    market_analyzer: Any = None,
) -> Tuple[List[Dict], List[Dict]]:
    """매도/매수 리스트를 1:1 페어로 맞춘 뒤 공통 정책 적용."""
    limit = max_pairs if max_pairs is not None else max_pairs_per_run(settings)
    thr = delta_threshold
    if thr is None:
        thr = resolve_delta_threshold(settings, market_state, market_analyzer)
    pairs = lists_to_pairs(to_sell_list, to_buy_plans)
    pairs = filter_valid_pairs(
        pairs,
        settings,
        usable_cash=usable_cash,
        delta_threshold=thr,
        check_economics=check_economics,
    )
    pairs = cap_pairs(pairs, limit)
    return pairs_to_legacy_lists(pairs)


def pair_gpt_rebalance_lists(
    to_sell_list: List[Dict],
    to_buy_plans: List[Dict],
) -> Tuple[List[Dict], List[Dict]]:
    """GPT SELL/BUY를 우선순위·점수 기준 1:1 페어로 정렬 (orphan 제거)."""
    if not to_sell_list or not to_buy_plans:
        return [], []

    sells = sorted(to_sell_list, key=lambda x: float(x.get("priority", 0)), reverse=True)
    buys = sorted(
        to_buy_plans,
        key=lambda x: float(x.get("priority", x.get("confidence", 0))),
        reverse=True,
    )
    n = min(len(sells), len(buys))
    paired_sells: List[Dict] = []
    paired_buys: List[Dict] = []
    for i in range(n):
        s = sells[i]
        b = buys[i]
        info = b.get("stock_info", {})
        s = dict(s)
        if "new_score" not in s:
            s["new_score"] = _to_float(info.get("Score", 0.0), 0.0)
        if "new_ticker" not in s:
            s["new_ticker"] = str(info.get("Ticker", "")).zfill(6)
        paired_sells.append(s)
        paired_buys.append(b)
    if len(sells) != len(buys):
        logger.info(
            "GPT 회전 페어링: SELL %d, BUY %d → %d페어 (미매칭 제외)",
            len(sells),
            len(buys),
            n,
        )
    return paired_sells, paired_buys

