#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reviewer Module - 성과 분석 및 리뷰 시스템
"""

import logging
import os
import json
import shutil
import subprocess
import re
import numpy as np
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum

class MarketRegime(Enum):
    BULL = "bull"
    BEAR = "bear"
    SIDEWAYS = "sideways"
    VOLATILE = "volatile"

@dataclass
class MarketState:
    regime: MarketRegime
    volatility_level: str
    trend_direction: str
    confidence: float
    timestamp: datetime

@dataclass
class PerformanceMetrics:
    total_return: float = 0.0
    annualized_return: float = 0.0
    volatility: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    calmar_ratio: float = 0.0
    sortino_ratio: float = 0.0
    var_95: float = 0.0
    cvar_95: float = 0.0
    turnover_rate: float = 0.0
    transaction_costs: float = 0.0
    net_return: float = 0.0

@dataclass
class TradeRecord:
    timestamp: datetime
    ticker: str
    action: str
    quantity: int
    price: float
    amount: float
    commission: float
    tax: float
    total_cost: float
    net_amount: float
    profit_loss: float = 0.0
    holding_period_days: int = 0
    sector: str = ""
    market_regime: str = ""

@dataclass
class PortfolioSnapshot:
    timestamp: datetime
    total_value: float
    cash: float
    holdings: List[Dict[str, Any]]
    performance_metrics: PerformanceMetrics
    market_state: Optional[MarketState] = None

class PerformanceReviewer:
    def __init__(self, settings: Dict[str, Any]):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
    
    def calculate_advanced_performance_metrics(
        self,
        trade_records: List[TradeRecord],
        portfolio_snapshots: List[PortfolioSnapshot],
        risk_free_rate: float = 0.03
    ) -> PerformanceMetrics:
        """고급 성과 지표 계산"""
        try:
            if not trade_records or not portfolio_snapshots:
                return PerformanceMetrics()
            
            total_return = self._calculate_total_return(portfolio_snapshots)
            annualized_return = self._calculate_annualized_return(portfolio_snapshots)
            volatility = self._calculate_volatility(portfolio_snapshots)
            sharpe_ratio = self._calculate_sharpe_ratio(portfolio_snapshots, risk_free_rate)
            max_drawdown = self._calculate_max_drawdown(portfolio_snapshots)
            win_rate = self._calculate_win_rate(trade_records)
            profit_factor = self._calculate_profit_factor(trade_records)
            calmar_ratio = self._calculate_calmar_ratio(annualized_return, max_drawdown)
            sortino_ratio = self._calculate_sortino_ratio(portfolio_snapshots, risk_free_rate)
            var_95, cvar_95 = self._calculate_var_cvar(portfolio_snapshots)
            turnover_rate = self._calculate_turnover_rate(trade_records, portfolio_snapshots)
            transaction_costs = self._calculate_transaction_costs(trade_records)
            net_return = total_return - transaction_costs
            
            return PerformanceMetrics(
                total_return=total_return,
                annualized_return=annualized_return,
                volatility=volatility,
                sharpe_ratio=sharpe_ratio,
                max_drawdown=max_drawdown,
                win_rate=win_rate,
                profit_factor=profit_factor,
                calmar_ratio=calmar_ratio,
                sortino_ratio=sortino_ratio,
                var_95=var_95,
                cvar_95=cvar_95,
                turnover_rate=turnover_rate,
                transaction_costs=transaction_costs,
                net_return=net_return
            )
            
        except Exception as e:
            self.logger.error(f"성과 지표 계산 실패: {e}")
            return PerformanceMetrics()
    
    def _calculate_total_return(self, snapshots: List[PortfolioSnapshot]) -> float:
        if len(snapshots) < 2:
            return 0.0
        initial_value = snapshots[0].total_value
        final_value = snapshots[-1].total_value
        return (final_value - initial_value) / initial_value if initial_value > 0 else 0.0
    
    def _calculate_annualized_return(self, snapshots: List[PortfolioSnapshot]) -> float:
        if len(snapshots) < 2:
            return 0.0
        total_return = self._calculate_total_return(snapshots)
        days = (snapshots[-1].timestamp - snapshots[0].timestamp).days
        if days <= 0:
            return 0.0
        return (1 + total_return) ** (365 / days) - 1
    
    def _calculate_volatility(self, snapshots: List[PortfolioSnapshot]) -> float:
        if len(snapshots) < 2:
            return 0.0
        values = [s.total_value for s in snapshots]
        returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]
        return np.std(returns) * np.sqrt(252) if returns else 0.0
    
    def _calculate_sharpe_ratio(self, snapshots: List[PortfolioSnapshot], risk_free_rate: float) -> float:
        if len(snapshots) < 2:
            return 0.0
        annualized_return = self._calculate_annualized_return(snapshots)
        volatility = self._calculate_volatility(snapshots)
        return (annualized_return - risk_free_rate) / volatility if volatility > 0 else 0.0
    
    def _calculate_max_drawdown(self, snapshots: List[PortfolioSnapshot]) -> float:
        if len(snapshots) < 2:
            return 0.0
        values = [s.total_value for s in snapshots]
        peak = values[0]
        max_dd = 0.0
        for value in values:
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            max_dd = max(max_dd, dd)
        return max_dd
    
    def _calculate_win_rate(self, trade_records: List[TradeRecord]) -> float:
        sell_trades = [t for t in trade_records if str(getattr(t, "action", "")).upper() == "SELL"]
        if not sell_trades:
            return 0.0
        winning_trades = [t for t in sell_trades if t.profit_loss > 0]
        return len(winning_trades) / len(sell_trades)
    
    def _calculate_profit_factor(self, trade_records: List[TradeRecord]) -> float:
        sell_trades = [t for t in trade_records if str(getattr(t, "action", "")).upper() == "SELL"]
        if not sell_trades:
            return 0.0
        total_profit = sum(t.profit_loss for t in sell_trades if t.profit_loss > 0)
        total_loss = abs(sum(t.profit_loss for t in sell_trades if t.profit_loss < 0))
        return total_profit / total_loss if total_loss > 0 else float('inf')
    
    def _calculate_calmar_ratio(self, annualized_return: float, max_drawdown: float) -> float:
        return annualized_return / max_drawdown if max_drawdown > 0 else 0.0
    
    def _calculate_sortino_ratio(self, snapshots: List[PortfolioSnapshot], risk_free_rate: float) -> float:
        if len(snapshots) < 2:
            return 0.0
        annualized_return = self._calculate_annualized_return(snapshots)
        values = [s.total_value for s in snapshots]
        returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]
        negative_returns = [r for r in returns if r < 0]
        downside_volatility = np.std(negative_returns) * np.sqrt(252) if negative_returns else 0.0
        return (annualized_return - risk_free_rate) / downside_volatility if downside_volatility > 0 else 0.0
    
    def _calculate_var_cvar(self, snapshots: List[PortfolioSnapshot], confidence_level: float = 0.95) -> tuple:
        if len(snapshots) < 2:
            return 0.0, 0.0
        values = [s.total_value for s in snapshots]
        returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]
        if not returns:
            return 0.0, 0.0
        sorted_returns = sorted(returns)
        var_index = int((1 - confidence_level) * len(sorted_returns))
        var = sorted_returns[var_index] if var_index < len(sorted_returns) else sorted_returns[0]
        cvar_returns = [r for r in sorted_returns if r <= var]
        cvar = np.mean(cvar_returns) if cvar_returns else var
        return var, cvar
    
    def _calculate_turnover_rate(self, trade_records: List[TradeRecord], snapshots: List[PortfolioSnapshot]) -> float:
        if not snapshots:
            return 0.0
        total_volume = sum(abs(t.amount) for t in trade_records)
        avg_portfolio_value = np.mean([s.total_value for s in snapshots])
        return total_volume / avg_portfolio_value if avg_portfolio_value > 0 else 0.0
    
    def _calculate_transaction_costs(self, trade_records: List[TradeRecord]) -> float:
        return sum(t.total_cost for t in trade_records)

    def analyze_sector_performance(
        self,
        trade_records: List[TradeRecord],
        portfolio_snapshots: List[PortfolioSnapshot]
    ) -> Dict[str, Dict[str, float]]:
        """섹터별 성과 분석"""
        try:
            sector_analysis = {}
            
            # 섹터별 거래 기록 그룹화
            sector_trades = {}
            for record in trade_records:
                if record.sector not in sector_trades:
                    sector_trades[record.sector] = []
                sector_trades[record.sector].append(record)
            
            # 섹터별 성과 계산
            for sector, trades in sector_trades.items():
                if not trades:
                    continue
                
                total_profit = sum(t.profit_loss for t in trades if str(getattr(t, "action", "")).upper() == "SELL")
                total_volume = sum(t.amount for t in trades if str(getattr(t, "action", "")).upper() == "SELL")
                trade_count = len([t for t in trades if str(getattr(t, "action", "")).upper() == "SELL"])
                win_count = len([t for t in trades if str(getattr(t, "action", "")).upper() == "SELL" and t.profit_loss > 0])
                
                sector_analysis[sector] = {
                    "total_profit": total_profit,
                    "total_volume": total_volume,
                    "trade_count": trade_count,
                    "win_rate": win_count / trade_count if trade_count > 0 else 0,
                    "avg_profit": total_profit / trade_count if trade_count > 0 else 0,
                    "weight": total_volume / sum(
                        t.amount
                        for s in sector_trades.values()
                        for t in s
                        if str(getattr(t, "action", "")).upper() == "SELL"
                    ) if any(sector_trades.values()) else 0
                }
            
            return sector_analysis
            
        except Exception as e:
            self.logger.error(f"섹터별 성과 분석 실패: {e}")
            return {}
    
    def analyze_market_regime_performance(
        self,
        trade_records: List[TradeRecord],
        portfolio_snapshots: List[PortfolioSnapshot]
    ) -> Dict[str, Dict[str, float]]:
        """시장 상황별 성과 분석"""
        try:
            regime_analysis = {}
            
            # 시장 상황별 거래 기록 그룹화
            regime_trades = {}
            for record in trade_records:
                if record.market_regime not in regime_trades:
                    regime_trades[record.market_regime] = []
                regime_trades[record.market_regime].append(record)
            
            # 시장 상황별 성과 계산
            for regime, trades in regime_trades.items():
                if not trades:
                    continue
                
                total_profit = sum(t.profit_loss for t in trades if str(getattr(t, "action", "")).upper() == "SELL")
                total_volume = sum(t.amount for t in trades if str(getattr(t, "action", "")).upper() == "SELL")
                trade_count = len([t for t in trades if str(getattr(t, "action", "")).upper() == "SELL"])
                win_count = len([t for t in trades if str(getattr(t, "action", "")).upper() == "SELL" and t.profit_loss > 0])
                
                regime_analysis[regime] = {
                    "total_profit": total_profit,
                    "total_volume": total_volume,
                    "trade_count": trade_count,
                    "win_rate": win_count / trade_count if trade_count > 0 else 0,
                    "avg_profit": total_profit / trade_count if trade_count > 0 else 0
                }
            
            return regime_analysis
            
        except Exception as e:
            self.logger.error(f"시장 상황별 성과 분석 실패: {e}")
            return {}

    def analyze_sector_performance_fixed(
        self,
        trade_records: List[TradeRecord],
        portfolio_snapshots: List[PortfolioSnapshot]
    ) -> Dict[str, Dict[str, float]]:
        """섹터별 성과 분석 (수정된 버전)"""
        try:
            sector_analysis = {}
            
            # 섹터별 거래 기록 그룹화
            sector_trades = {}
            for record in trade_records:
                if record.sector and record.sector not in sector_trades:
                    sector_trades[record.sector] = []
                if record.sector:
                    sector_trades[record.sector].append(record)
            
            # 섹터별 성과 계산
            for sector, trades in sector_trades.items():
                if not trades:
                    continue
                
                total_profit = sum(t.profit_loss for t in trades if str(getattr(t, "action", "")).upper() == "SELL")
                total_volume = sum(t.amount for t in trades if str(getattr(t, "action", "")).upper() == "SELL")
                trade_count = len([t for t in trades if str(getattr(t, "action", "")).upper() == "SELL"])
                win_count = len([t for t in trades if str(getattr(t, "action", "")).upper() == "SELL" and t.profit_loss > 0])
                
                # 전체 거래량 계산
                all_volume = sum(
                    t.amount
                    for s in sector_trades.values()
                    for t in s
                    if str(getattr(t, "action", "")).upper() == "SELL"
                )
                
                sector_analysis[sector] = {
                    "total_profit": total_profit,
                    "total_volume": total_volume,
                    "trade_count": trade_count,
                    "win_rate": win_count / trade_count if trade_count > 0 else 0,
                    "avg_profit": total_profit / trade_count if trade_count > 0 else 0,
                    "weight": total_volume / all_volume if all_volume > 0 else 0
                }
            
            return sector_analysis
            
        except Exception as e:
            self.logger.error(f"섹터별 성과 분석 실패: {e}")
            return {}


# ════════════════════════════════════════════════════════════════════
#  최근 1개월 DB 승패 분석 → config.json 자동 튜닝 (파이프라인 단계)
# ════════════════════════════════════════════════════════════════════
logger = logging.getLogger("reviewer")

# ── 조정 대상 파라미터의 안전 범위(클램프) ──────────────────────────
AUTOSELL_STOP_LOSS_MIN = 0.02
AUTOSELL_STOP_LOSS_MAX = 0.08
AUTOSELL_TARGET_MIN = 0.04
AUTOSELL_TARGET_MAX = 0.15
# 한 회차당 최대 조정 폭(과적합/급변 방지)
ADJUST_STEP = 0.005

# 기본값(설정에 값이 없을 때)
DEFAULT_STOP_LOSS_PCT = 0.045
DEFAULT_TARGET_PCT = 0.07

# GPT 가 수정할 수 있는 config 섹션(화이트리스트)
TUNABLE_SECTIONS = ("screener_params", "risk_params", "strategy_params")
# 한 회차당 숫자 값의 최대 상대 변경 폭(과격한 변경 방지). env 로 조정.
DEFAULT_MAX_REL_CHANGE = 0.30
# 프롬프트에 포함할 코스피 뉴스 최대 개수
NEWS_MAX_ITEMS = 40

# GitHub 커밋 조회 (코드 변경 ↔ 이상 거래 교차 검증)
DEFAULT_GITHUB_REPO_URL = "https://github.com/tingcho330/nasdaqstock.git"
DEFAULT_GITHUB_BRANCH = "main"
DEFAULT_COMMIT_BUFFER_HOURS = 72
TRADING_CRITICAL_PATH_PREFIXES = (
    "src/trader.py",
    "src/risk_manager.py",
    "src/recorder.py",
    "src/order_reconciler.py",
    "src/gpt_analyzer.py",
    "src/screener.py",
    "src/screener_core.py",
    "src/integrated_manager.py",
    "src/kis_overseas_account.py",
    "src/kis_market_data.py",
    "src/rotation_policy.py",
    "src/rotation_manager.py",
    "config/config.json",
)


def _normalize_dt(dt: datetime) -> datetime:
    """naive/aware datetime → UTC aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _trade_timestamp(trade: Any) -> datetime:
    ts = getattr(trade, "timestamp", None) or datetime.min.replace(tzinfo=timezone.utc)
    return _normalize_dt(ts)


def _parse_github_repo_url(url: str) -> Tuple[str, str]:
    """https://github.com/owner/repo(.git) → (owner, repo)."""
    url = (url or "").strip().rstrip("/")
    m = re.match(r"(?:https?://)?github\.com/([^/]+)/([^/.]+)", url)
    if not m:
        raise ValueError(f"invalid GitHub repo URL: {url}")
    return m.group(1), m.group(2)


def _github_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "nasdaqstock-reviewer",
    }
    token = os.getenv("GITHUB_TOKEN") or os.getenv("REVIEWER_GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _commit_entry_from_api(item: Dict[str, Any]) -> Dict[str, Any]:
    commit = item.get("commit") or {}
    author = commit.get("author") or {}
    date_str = author.get("date") or ""
    committed_at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return {
        "sha": (item.get("sha") or "")[:12],
        "full_sha": item.get("sha") or "",
        "message": (commit.get("message") or "").split("\n")[0][:200],
        "committed_at": committed_at.isoformat(),
        "author": author.get("name") or "",
        "html_url": item.get("html_url") or "",
        "files": [],
        "source": "github_api",
    }


def _fetch_github_commits_api(
    owner: str,
    repo: str,
    since: datetime,
    until: datetime,
    branch: str,
) -> List[Dict[str, Any]]:
    """GitHub REST API로 기간 내 커밋 목록을 페이지네이션 조회."""
    since_utc = _normalize_dt(since)
    until_utc = _normalize_dt(until)
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    params: Dict[str, Any] = {
        "sha": branch,
        "since": since_utc.isoformat().replace("+00:00", "Z"),
        "until": until_utc.isoformat().replace("+00:00", "Z"),
        "per_page": 100,
    }
    out: List[Dict[str, Any]] = []
    page = 1
    while page <= 10:
        params["page"] = page
        resp = requests.get(url, params=params, headers=_github_headers(), timeout=30)
        if resp.status_code != 200:
            logger.warning(f"GitHub commits API 실패 ({resp.status_code}): {resp.text[:300]}")
            break
        batch = resp.json()
        if not isinstance(batch, list) or not batch:
            break
        for item in batch:
            entry = _commit_entry_from_api(item)
            committed = datetime.fromisoformat(entry["committed_at"].replace("Z", "+00:00"))
            if since_utc <= committed <= until_utc:
                out.append(entry)
        if len(batch) < 100:
            break
        page += 1
    out.sort(key=lambda c: c["committed_at"])
    return out


def _fetch_github_commit_files(owner: str, repo: str, full_sha: str) -> List[str]:
    """단일 커밋의 변경 파일 목록."""
    if not full_sha:
        return []
    url = f"https://api.github.com/repos/{owner}/{repo}/commits/{full_sha}"
    resp = requests.get(url, headers=_github_headers(), timeout=30)
    if resp.status_code != 200:
        return []
    data = resp.json()
    files = data.get("files") or []
    return [str(f.get("filename") or "") for f in files if f.get("filename")]


def _commit_touches_trading_code(files: List[str]) -> bool:
    if not files:
        return True
    for path in files:
        for prefix in TRADING_CRITICAL_PATH_PREFIXES:
            if path == prefix or path.startswith(prefix.rstrip("/") + "/"):
                return True
    return False


def _fetch_commits_local_git(
    repo_path: str,
    since: datetime,
    until: datetime,
    branch: str,
) -> List[Dict[str, Any]]:
    """로컬 .git 이 있을 때 git log 로 커밋 조회 (API 폴백)."""
    git_dir = os.path.join(repo_path, ".git")
    if not os.path.isdir(git_dir):
        return []
    fmt = "%H%x1f%an%x1f%ad%x1f%s"
    cmd = [
        "git", "-C", repo_path, "log",
        f"--since={_normalize_dt(since).isoformat()}",
        f"--until={_normalize_dt(until).isoformat()}",
        branch, f"--format={fmt}", "--date=iso-strict",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    except Exception as e:
        logger.debug(f"local git log 실패: {e}")
        return []
    if proc.returncode != 0:
        return []
    out: List[Dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = line.split("\x1f", 3)
        if len(parts) < 4:
            continue
        full_sha, author, date_str, message = parts
        try:
            committed_at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        files: List[str] = []
        try:
            show = subprocess.run(
                ["git", "-C", repo_path, "show", "--name-only", "--pretty=format:", full_sha],
                capture_output=True, text=True, timeout=15, check=False,
            )
            if show.returncode == 0:
                files = [ln.strip() for ln in show.stdout.splitlines() if ln.strip()]
        except Exception:
            pass
        out.append({
            "sha": full_sha[:12],
            "full_sha": full_sha,
            "message": message[:200],
            "committed_at": committed_at.isoformat(),
            "author": author,
            "html_url": "",
            "files": files,
            "source": "local_git",
        })
    out.sort(key=lambda c: c["committed_at"])
    return out


def fetch_repo_commits(
    since: datetime,
    until: datetime,
    repo_url: Optional[str] = None,
    branch: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """GitHub API(우선) 또는 로컬 git 으로 분석 기간 커밋을 수집."""
    repo_url = repo_url or os.getenv("REVIEWER_GITHUB_REPO_URL", DEFAULT_GITHUB_REPO_URL)
    branch = branch or os.getenv("REVIEWER_GITHUB_BRANCH", DEFAULT_GITHUB_BRANCH)
    filter_files = os.getenv("REVIEWER_GITHUB_FILTER_FILES", "1") == "1"
    max_detail = int(os.getenv("REVIEWER_GITHUB_MAX_COMMIT_DETAILS", "50"))

    commits: List[Dict[str, Any]] = []
    try:
        owner, repo = _parse_github_repo_url(repo_url)
        commits = _fetch_github_commits_api(owner, repo, since, until, branch)
        if filter_files and commits:
            for i, c in enumerate(commits[:max_detail]):
                files = _fetch_github_commit_files(owner, repo, c.get("full_sha", ""))
                c["files"] = files
                c["touches_trading_code"] = _commit_touches_trading_code(files)
            for c in commits[max_detail:]:
                c["touches_trading_code"] = True
        else:
            for c in commits:
                c["touches_trading_code"] = True
    except Exception as e:
        logger.warning(f"GitHub 커밋 조회 실패 → local git 시도: {e}")

    if not commits:
        local_path = os.getenv("REVIEWER_GITHUB_LOCAL_PATH", "")
        if not local_path:
            local_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        commits = _fetch_commits_local_git(local_path, since, until, branch)
        for c in commits:
            c["touches_trading_code"] = _commit_touches_trading_code(c.get("files") or [])

    relevant = [c for c in commits if c.get("touches_trading_code", True)]
    logger.info(f"[github] commits total={len(commits)} trading_relevant={len(relevant)}")
    return relevant


def build_commit_windows(
    commits: List[Dict[str, Any]],
    buffer_hours: int,
) -> List[Dict[str, Any]]:
    """커밋 시각 이후 buffer_hours 동안을 '코드 변경 영향 구간'으로 본다."""
    windows: List[Dict[str, Any]] = []
    for c in commits:
        try:
            start = datetime.fromisoformat(c["committed_at"].replace("Z", "+00:00"))
        except ValueError:
            continue
        end = start + timedelta(hours=buffer_hours)
        windows.append({
            "sha": c.get("sha", ""),
            "message": c.get("message", ""),
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "committed_at": c.get("committed_at", ""),
        })
    return windows


def _trade_in_commit_window(trade_ts: datetime, windows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """거래 시각이 속한 커밋 영향 구간 목록."""
    trade_ts = _normalize_dt(trade_ts)
    hits: List[Dict[str, str]] = []
    for w in windows:
        try:
            start = datetime.fromisoformat(w["window_start"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(w["window_end"].replace("Z", "+00:00"))
        except ValueError:
            continue
        if start <= trade_ts <= end:
            hits.append({"sha": w.get("sha", ""), "message": w.get("message", "")})
    return hits


def detect_suspicious_sell(trade: Any, sell_pnls: List[float]) -> Tuple[bool, List[str]]:
    """매도 거래의 이상 징후를 탐지한다."""
    if str(getattr(trade, "action", "")).lower() != "sell":
        return False, []

    reasons: List[str] = []
    pl = float(getattr(trade, "profit_loss", 0.0) or 0.0)
    qty = int(getattr(trade, "quantity", 0) or 0)
    price = float(getattr(trade, "price", 0.0) or 0.0)
    amount = float(getattr(trade, "amount", 0.0) or 0.0)
    status = str(getattr(trade, "order_status", "executed") or "executed").lower()
    exec_qty = int(getattr(trade, "executed_qty", 0) or 0)

    if qty <= 0 or price <= 0:
        reasons.append("invalid_qty_or_price")
    if status not in ("executed", ""):
        reasons.append(f"non_executed_status:{status}")
    if qty > 0 and exec_qty == 0:
        reasons.append("zero_executed_qty")
    if pl == 0 and amount > 0:
        reasons.append("zero_pnl_sell")

    if len(sell_pnls) >= 5:
        arr = np.array(sell_pnls, dtype=float)
        std = float(np.std(arr))
        if std > 0 and abs(pl - float(np.mean(arr))) > 2 * std:
            reasons.append("pnl_outlier")

    return len(reasons) > 0, reasons


def filter_trades_for_analysis(
    trades: List[Any],
    commits: List[Dict[str, Any]],
    buffer_hours: int,
) -> Tuple[List[Any], Dict[str, Any]]:
    """이상 매도 + 코드 변경 영향 구간 겹침 거래를 종합 분석에서 제외."""
    windows = build_commit_windows(commits, buffer_hours)
    sells = [t for t in trades if str(getattr(t, "action", "")).lower() == "sell"]
    sell_pnls = [float(getattr(t, "profit_loss", 0.0) or 0.0) for t in sells]

    excluded_keys: set = set()
    excluded_details: List[Dict[str, Any]] = []

    for trade in trades:
        if str(getattr(trade, "action", "")).lower() != "sell":
            continue
        suspicious, sus_reasons = detect_suspicious_sell(trade, sell_pnls)
        if not suspicious:
            continue
        nearby = _trade_in_commit_window(_trade_timestamp(trade), windows)
        if not nearby:
            continue
        key = (
            getattr(trade, "ticker", ""),
            _trade_timestamp(trade).isoformat(),
            getattr(trade, "order_id", ""),
        )
        excluded_keys.add(key)
        excluded_details.append({
            "ticker": getattr(trade, "ticker", ""),
            "timestamp": _trade_timestamp(trade).isoformat(),
            "action": "sell",
            "profit_loss": float(getattr(trade, "profit_loss", 0.0) or 0.0),
            "order_id": getattr(trade, "order_id", ""),
            "reason_code": getattr(trade, "reason_code", ""),
            "suspicious_reasons": sus_reasons,
            "nearby_commits": nearby,
            "exclusion_reason": "suspicious_trade_during_code_change_window",
        })

    filtered = []
    for trade in trades:
        if str(getattr(trade, "action", "")).lower() != "sell":
            filtered.append(trade)
            continue
        key = (
            getattr(trade, "ticker", ""),
            _trade_timestamp(trade).isoformat(),
            getattr(trade, "order_id", ""),
        )
        if key not in excluded_keys:
            filtered.append(trade)

    meta = {
        "commits_count": len(commits),
        "commit_windows": windows,
        "commits": [
            {
                "sha": c.get("sha"),
                "message": c.get("message"),
                "committed_at": c.get("committed_at"),
                "files": (c.get("files") or [])[:8],
            }
            for c in commits
        ],
        "total_trades": len(trades),
        "sell_trades": len(sells),
        "excluded_sell_trades": len(excluded_details),
        "analyzed_sell_trades": len(sells) - len(excluded_details),
        "excluded_details": excluded_details,
        "buffer_hours": buffer_hours,
    }
    return filtered, meta


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def analyze_win_loss(trade_records: List[Any]) -> Dict[str, Any]:
    """매도 거래 기록으로부터 승패 통계를 계산한다.

    trade_records 의 각 항목은 .action / .profit_loss / .timestamp 속성을
    가지면 되므로 reviewer.TradeRecord, recorder.TradeRecord 모두 호환된다.
    """
    sells = [t for t in trade_records if str(getattr(t, "action", "")).lower() == "sell"]
    n = len(sells)
    wins = [t for t in sells if (getattr(t, "profit_loss", 0.0) or 0.0) > 0]
    losses = [t for t in sells if (getattr(t, "profit_loss", 0.0) or 0.0) < 0]

    total_profit = sum((getattr(t, "profit_loss", 0.0) or 0.0) for t in wins)
    total_loss = abs(sum((getattr(t, "profit_loss", 0.0) or 0.0) for t in losses))

    win_rate = (len(wins) / n) if n > 0 else 0.0
    if total_loss > 0:
        profit_factor = total_profit / total_loss
    elif total_profit > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    # 최대 연속 손실(시간 오름차순)
    ordered = sorted(sells, key=lambda t: getattr(t, "timestamp", datetime.min))
    max_consec_losses = 0
    cur = 0
    for t in ordered:
        if (getattr(t, "profit_loss", 0.0) or 0.0) < 0:
            cur += 1
            max_consec_losses = max(max_consec_losses, cur)
        else:
            cur = 0

    return {
        "sell_trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 4),
        "profit_factor": (round(profit_factor, 4) if profit_factor != float("inf") else "inf"),
        "total_profit": round(total_profit, 2),
        "total_loss": round(total_loss, 2),
        "net_pnl": round(total_profit - total_loss, 2),
        "max_consecutive_losses": max_consec_losses,
    }


def decide_autosell_adjustments(stats: Dict[str, Any], auto_sell: Dict[str, Any]) -> Tuple[Dict[str, float], List[str]]:
    """승패 통계에 따라 auto_sell.stop_loss_pct / target_pct 를 조정한다.

    반환: (변경된 값 dict, 사람이 읽는 변경 사유 리스트)
    실제 변경이 없으면 빈 dict 를 반환한다.
    """
    cur_stop = float(auto_sell.get("stop_loss_pct", DEFAULT_STOP_LOSS_PCT))
    cur_target = float(auto_sell.get("target_pct", DEFAULT_TARGET_PCT))
    new_stop, new_target = cur_stop, cur_target

    win_rate = float(stats.get("win_rate", 0.0))
    pf_raw = stats.get("profit_factor", 0.0)
    profit_factor = float("inf") if pf_raw == "inf" else float(pf_raw)

    reasons: List[str] = []

    # R1) 승률이 낮으면 손절을 더 타이트하게(작게) → 손실 거래 폭 축소
    if win_rate < 0.40:
        new_stop = _clamp(cur_stop - ADJUST_STEP, AUTOSELL_STOP_LOSS_MIN, AUTOSELL_STOP_LOSS_MAX)
        reasons.append(f"승률 낮음({win_rate:.0%}<40%) → 손절 타이트화 {cur_stop:.3f}→{new_stop:.3f}")

    # R2) 승률·손익비가 모두 좋으면 익절 목표를 더 높임 → 수익 추구
    if win_rate > 0.60 and profit_factor > 1.5:
        new_target = _clamp(cur_target + ADJUST_STEP, AUTOSELL_TARGET_MIN, AUTOSELL_TARGET_MAX)
        reasons.append(f"승률·PF 양호({win_rate:.0%},PF={profit_factor:.2f}) → 익절 목표 상향 {cur_target:.3f}→{new_target:.3f}")

    # R3) 손익비가 1 미만(손실 우위)이면 익절을 빨리(작게) → 수익 조기 실현
    if profit_factor < 1.0:
        new_target = _clamp(new_target - ADJUST_STEP, AUTOSELL_TARGET_MIN, AUTOSELL_TARGET_MAX)
        reasons.append(f"손익비 부진(PF={profit_factor:.2f}<1.0) → 익절 목표 하향 {cur_target:.3f}→{new_target:.3f}")

    changes: Dict[str, float] = {}
    if abs(new_stop - cur_stop) > 1e-9:
        changes["stop_loss_pct"] = round(new_stop, 4)
    if abs(new_target - cur_target) > 1e-9:
        changes["target_pct"] = round(new_target, 4)

    return changes, reasons


def _write_config_atomic(config_path, cfg: Dict[str, Any]) -> str:
    """config.json 을 백업 후 원자적으로 덮어쓴다. 백업 경로 반환."""
    config_path = str(config_path)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{config_path}.bak.{ts}"
    shutil.copy2(config_path, backup_path)

    tmp_path = f"{config_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp_path, config_path)
    return backup_path


# ── 코스피 한 달 주요 뉴스 수집 ─────────────────────────────────────
def collect_kospi_news(lookback_days: int, max_items: int = NEWS_MAX_ITEMS) -> List[Dict[str, str]]:
    """최근 lookback_days 일간의 코스피/증시 관련 주요 뉴스를 수집한다.

    news_collector 의 네이버 뉴스 API 헬퍼를 재사용한다. NAVER_ID/SECRET 이
    없거나 실패하면 빈 리스트를 반환한다(GPT 프롬프트에서 뉴스 생략).
    """
    try:
        from news_collector import _fetch_naver_news_api, _parse_pubdate, _dedupe_items_by_title, _clean_text
    except Exception as e:
        logger.warning(f"news_collector import 실패 → 뉴스 생략: {e}")
        return []

    raw: List[Dict] = []
    for kw in ("코스피", "증시", "코스닥"):
        try:
            raw.extend(_fetch_naver_news_api(kw, 100) or [])
        except Exception as e:
            logger.debug(f"뉴스 조회 실패(kw={kw}): {e}")

    if not raw:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    filtered: List[Dict] = []
    for it in raw:
        pub_dt = None
        try:
            pub_dt = _parse_pubdate(it.get("pubDate", ""))
        except Exception:
            pub_dt = None
        if pub_dt is not None and pub_dt < cutoff:
            continue
        it["_pub_dt"] = pub_dt
        filtered.append(it)

    try:
        filtered = _dedupe_items_by_title(filtered)
    except Exception:
        pass

    filtered.sort(key=lambda x: x.get("_pub_dt") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    out: List[Dict[str, str]] = []
    for it in filtered[:max_items]:
        title = _clean_text((it.get("title") or "").strip())
        desc = _clean_text((it.get("description") or "").strip())
        pub_dt = it.get("_pub_dt")
        out.append({
            "title": title,
            "desc": desc[:200],
            "date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
        })
    return out


# ── GPT 기반 config 변경 제안 ───────────────────────────────────────
def _extract_tunable(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """튜닝 대상 3개 섹션만 추출(GPT 프롬프트용)."""
    return {sec: cfg.get(sec, {}) for sec in TUNABLE_SECTIONS}


def gpt_propose_config_changes(stats: Dict[str, Any], news: List[Dict[str, str]],
                               cfg: Dict[str, Any],
                               commit_context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """GPT 에 승패 통계 + 코스피 뉴스 + GitHub 커밋 + 현재 설정을 주고 변경안을 받는다.

    반환 형식(검증 전): {"screener_params": {...}, "risk_params": {...},
                       "strategy_params": {...}, "reasons": [...]} 또는 None
    """
    try:
        from gpt_analyzer import _call_openai_json
    except Exception as e:
        logger.warning(f"gpt_analyzer import 실패 → GPT 제안 생략: {e}")
        return None

    current = _extract_tunable(cfg)
    news_lines = "\n".join(
        f"- [{n.get('date','')}] {n.get('title','')} :: {n.get('desc','')}" for n in news
    ) or "(수집된 뉴스 없음)"

    commit_lines = "(커밋 정보 없음)"
    if commit_context:
        commits = commit_context.get("commits") or []
        excluded = commit_context.get("excluded_details") or []
        commit_lines = "\n".join(
            f"- [{c.get('committed_at','')[:10]}] {c.get('sha','')}: {c.get('message','')}"
            for c in commits[:20]
        ) or "(분석 기간 내 코드 변경 없음)"
        if excluded:
            ex_lines = "\n".join(
                f"- {e.get('ticker')} @ {e.get('timestamp','')[:19]} "
                f"PnL={e.get('profit_loss')} (제외: {', '.join(e.get('suspicious_reasons', []))})"
                for e in excluded[:10]
            )
            commit_lines += (
                f"\n\n## 분석에서 제외된 거래 (코드 변경 영향 구간 + 이상 징후)\n{ex_lines}"
            )

    system_prompt = (
        "당신은 한국 주식 자동매매 시스템의 리스크/전략 파라미터를 검토하는 퀀트 전문가입니다. "
        "최근 한 달 실제 매매 승패 통계, 코스피 시장 뉴스, GitHub 코드 변경 이력을 근거로 "
        "주어진 설정값을 점진적으로 개선하세요. "
        "코드 변경 직후 발생한 이상 거래는 이미 분석에서 제외되었으므로, "
        "제외된 거래를 전략 성과로 해석하지 마세요. "
        "반드시 보수적으로 조정하고(한 번에 큰 변화 금지), 근거가 약하면 변경하지 마세요. "
        "응답은 반드시 단일 JSON 객체여야 합니다."
    )
    user_prompt = (
        "## 최근 매매 승패 통계 (코드 변경 영향 거래 제외 후)\n"
        f"{json.dumps(stats, ensure_ascii=False)}\n\n"
        "## GitHub 코드 변경 이력 (분석 기간)\n"
        f"{commit_lines}\n\n"
        "## 코스피 한 달 주요 뉴스\n"
        f"{news_lines}\n\n"
        "## 현재 설정값 (이 키들만 조정 가능)\n"
        f"{json.dumps(current, ensure_ascii=False, indent=2)}\n\n"
        "## 지시\n"
        "- 위 3개 섹션(screener_params, risk_params, strategy_params)에 '이미 존재하는' 숫자/불리언 키만 조정하세요.\n"
        "- 새 키 추가, 키 삭제, 리스트/구조 변경은 금지합니다.\n"
        "- 변경이 필요 없는 키는 응답에 포함하지 마세요. 변경할 키만 새 값으로 포함하세요.\n"
        "- 각 숫자 값은 현재값 대비 과도하게 바꾸지 마세요(작은 폭의 점진적 조정).\n"
        "- 승률이 낮고 손실이 크면 리스크를 줄이고(손절 타이트, 포지션/익절 보수화), 성과가 좋고 시장이 우호적이면 소폭 공격적으로.\n\n"
        "## 출력 JSON 형식 (변경할 키만)\n"
        "{\n"
        '  "screener_params": { "<키>": <새값> },\n'
        '  "risk_params": { "<키>": <새값>, "auto_sell": { "<키>": <새값> } },\n'
        '  "strategy_params": { "<키>": <새값> },\n'
        '  "reasons": ["<한국어 근거 문장>", "..."]\n'
        "}"
    )

    proposal = _call_openai_json(system_prompt, user_prompt, max_retries=3)
    if not isinstance(proposal, dict):
        logger.info("GPT 제안 없음/실패")
        return None
    return proposal


def _sanitize_proposal(proposal: Dict[str, Any], cfg: Dict[str, Any],
                       max_rel_change: float) -> Tuple[Dict[str, Any], List[str]]:
    """GPT 제안을 검증해 안전한 변경만 추려낸다.

    규칙:
      - TUNABLE_SECTIONS 의 '기존 키'만 허용(신규 키/구조 변경 무시).
      - leaf 타입이 현재값과 일치해야 함(숫자↔숫자, bool↔bool).
      - 숫자는 현재값 대비 ±max_rel_change 범위로 클램프. 현재값이 0이면 변경 무시.
      - 1단계 중첩(auto_sell, rsi_sell_strategy, strategy_weights 등)까지만 허용.
    반환: (적용 가능한 변경 dict[section -> {key/path: newval}], 사람이 읽는 사유)
    """
    applied_changes: Dict[str, Any] = {}
    notes: List[str] = []

    def _coerce_numeric(cur, new):
        if isinstance(cur, bool):
            return bool(new) if isinstance(new, bool) else None
        if isinstance(cur, (int, float)) and isinstance(new, (int, float)) and not isinstance(new, bool):
            if cur == 0:
                return None  # 상대 변경폭 계산 불가 → 안전하게 무시
            lo = cur * (1 - max_rel_change)
            hi = cur * (1 + max_rel_change)
            clamped = max(min(float(new), max(lo, hi)), min(lo, hi))
            # int 키는 int 유지
            if isinstance(cur, int) and not isinstance(cur, bool):
                clamped = int(round(clamped))
            else:
                clamped = round(clamped, 6)
            return clamped
        return None

    for section in TUNABLE_SECTIONS:
        prop_sec = proposal.get(section)
        if not isinstance(prop_sec, dict):
            continue
        cur_sec = cfg.get(section, {})
        if not isinstance(cur_sec, dict):
            continue

        sec_changes: Dict[str, Any] = {}
        for key, new_val in prop_sec.items():
            if key not in cur_sec:
                continue  # 신규 키 금지
            cur_val = cur_sec[key]

            # 1단계 중첩 dict
            if isinstance(cur_val, dict) and isinstance(new_val, dict):
                nested: Dict[str, Any] = {}
                for nk, nv in new_val.items():
                    if nk not in cur_val:
                        continue
                    coerced = _coerce_numeric(cur_val[nk], nv)
                    if coerced is not None and coerced != cur_val[nk]:
                        nested[nk] = coerced
                        notes.append(f"{section}.{key}.{nk}: {cur_val[nk]} → {coerced}")
                if nested:
                    sec_changes[key] = nested
                continue

            coerced = _coerce_numeric(cur_val, new_val)
            if coerced is not None and coerced != cur_val:
                sec_changes[key] = coerced
                notes.append(f"{section}.{key}: {cur_val} → {coerced}")

        if sec_changes:
            applied_changes[section] = sec_changes

    return applied_changes, notes


def _apply_changes_to_cfg(cfg: Dict[str, Any], changes: Dict[str, Any]) -> None:
    """검증된 changes 를 cfg 에 in-place 반영(1단계 중첩 지원)."""
    for section, sec_changes in changes.items():
        target = cfg.setdefault(section, {})
        for key, val in sec_changes.items():
            if isinstance(val, dict) and isinstance(target.get(key), dict):
                target[key].update(val)
            else:
                target[key] = val


def _notify_summary(stats: Dict[str, Any], changes: Dict[str, Any], reasons: List[str],
                    lookback_days: int, applied: bool, news_count: int, source: str,
                    commit_meta: Optional[Dict[str, Any]] = None) -> None:
    """Discord 로 분석/튜닝 요약 전송(실패는 조용히 무시)."""
    try:
        from notifier import send_discord_message, WEBHOOK_URL, is_valid_webhook
        if not (WEBHOOK_URL and is_valid_webhook(WEBHOOK_URL)):
            return

        pf = stats.get("profit_factor", 0.0)
        fields = [
            {"name": "📅 분석 기간", "value": f"최근 {lookback_days}일", "inline": True},
            {"name": "📊 매도 거래", "value": f"{stats.get('sell_trades', 0)}건 (승 {stats.get('wins', 0)}/패 {stats.get('losses', 0)})", "inline": True},
            {"name": "🎯 승률", "value": f"{float(stats.get('win_rate', 0.0)):.1%}", "inline": True},
            {"name": "💹 손익비(PF)", "value": f"{pf}", "inline": True},
            {"name": "💰 순손익", "value": f"{stats.get('net_pnl', 0):,}", "inline": True},
            {"name": "📰 뉴스/판단", "value": f"{news_count}건 / {source}", "inline": True},
        ]
        if commit_meta:
            excluded = int(commit_meta.get("excluded_sell_trades", 0))
            commits_n = int(commit_meta.get("commits_count", 0))
            fields.append({
                "name": "🔧 GitHub 커밋",
                "value": f"{commits_n}건 (분석 제외 매도 {excluded}건)",
                "inline": True,
            })
        if reasons:
            change_lines = "\n".join(f"• {r}" for r in reasons)
            status = "✅ 적용됨" if applied else "🧪 미적용(드라이런)"
            fields.append({"name": f"⚙️ config 조정 {status}", "value": change_lines[:1000], "inline": False})
        else:
            fields.append({"name": "⚙️ config 조정", "value": "변경 없음", "inline": False})

        embed = {
            "type": "rich",
            "title": "🔎 Reviewer 성과 분석 & 자동 튜닝",
            "fields": fields,
            "color": 0x3498db,
            "timestamp": datetime.now().isoformat(),
        }
        send_discord_message(embeds=[embed])
    except Exception as e:
        logger.debug(f"Discord 요약 전송 실패: {e}")


def run_review() -> Dict[str, Any]:
    """파이프라인 진입점: GitHub 커밋 + DB 승패 + 코스피 뉴스를 GPT 가 리뷰해
    config.json 의 screener_params/risk_params/strategy_params 를 조정한다."""
    from utils import setup_logging, CONFIG_PATH, OUTPUT_DIR
    setup_logging()

    lookback_days = int(os.getenv("REVIEWER_LOOKBACK_DAYS", "30"))
    min_trades = int(os.getenv("REVIEWER_MIN_TRADES", "10"))
    dry_run = os.getenv("REVIEWER_DRY_RUN", "0") == "1"
    max_rel_change = float(os.getenv("REVIEWER_MAX_REL_CHANGE", str(DEFAULT_MAX_REL_CHANGE)))
    commit_buffer_hours = int(
        os.getenv("REVIEWER_COMMIT_BUFFER_HOURS", str(DEFAULT_COMMIT_BUFFER_HOURS))
    )

    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days)

    logger.info(
        f"=== reviewer start (lookback={lookback_days}d, min_trades={min_trades}, "
        f"dry_run={dry_run}, max_rel_change={max_rel_change}, "
        f"commit_buffer={commit_buffer_hours}h) ==="
    )

    # 1) GitHub 커밋 조회 (코드 변경 ↔ 이상 거래 교차 검증)
    commits = fetch_repo_commits(start_dt, end_dt)

    # 2) DB 에서 기간 내 거래 조회
    try:
        from recorder import get_recorder
        recorder = get_recorder()
        trades = recorder.get_trade_records(start_date=start_dt, end_date=end_dt)
    except Exception as e:
        logger.error(f"거래 기록 조회 실패: {e}")
        trades = []

    # 3) 코드 변경 영향 구간의 이상 거래 제외
    filtered_trades, commit_meta = filter_trades_for_analysis(
        trades, commits, commit_buffer_hours,
    )
    stats_raw = analyze_win_loss(trades)
    stats = analyze_win_loss(filtered_trades)
    logger.info(f"[stats] raw={stats_raw} filtered={stats}")
    if commit_meta.get("excluded_sell_trades"):
        logger.info(
            f"[exclude] {commit_meta['excluded_sell_trades']}건 매도 제외 "
            f"(commits={commit_meta['commits_count']}, buffer={commit_buffer_hours}h)"
        )

    # 4) 코스피 한 달 주요 뉴스 수집
    news = collect_kospi_news(lookback_days)
    logger.info(f"[news] 수집 {len(news)}건")

    # 5) config 로드
    config_path = CONFIG_PATH
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        logger.error(f"config 로드 실패({config_path}): {e}")
        cfg = None

    result: Dict[str, Any] = {
        "timestamp": end_dt.isoformat(),
        "lookback_days": lookback_days,
        "stats": stats,
        "stats_raw": stats_raw,
        "github_commits": commit_meta,
        "news_count": len(news),
        "changes": {},
        "reasons": [],
        "source": "none",
        "applied": False,
        "skipped": False,
        "config_path": str(config_path),
    }

    # 6) 표본 부족/설정 로드 실패 시 스킵
    if stats["sell_trades"] < min_trades:
        result["skipped"] = True
        result["skip_reason"] = f"매도 거래 {stats['sell_trades']}건 < 최소 {min_trades}건"
        logger.info(f"[skip] {result['skip_reason']} → config 변경 없음")
    elif cfg is None:
        result["skipped"] = True
        result["skip_reason"] = "config 로드 실패"
    else:
        changes: Dict[str, Any] = {}
        reasons: List[str] = []
        source = "none"

        # 7) GPT 리뷰 → 변경 제안
        proposal = gpt_propose_config_changes(stats, news, cfg, commit_context=commit_meta)
        if proposal:
            result["gpt_raw_proposal"] = proposal
            changes, notes = _sanitize_proposal(proposal, cfg, max_rel_change)
            gpt_reasons = proposal.get("reasons") if isinstance(proposal.get("reasons"), list) else []
            reasons = [str(r) for r in gpt_reasons][:10] + notes
            if changes:
                source = "gpt"

        # 8) GPT 불가/무변경 시 규칙 기반 auto_sell 폴백
        if not changes:
            auto_sell = cfg.setdefault("risk_params", {}).setdefault("auto_sell", {})
            fb_changes, fb_reasons = decide_autosell_adjustments(stats, auto_sell)
            if fb_changes:
                changes = {"risk_params": {"auto_sell": fb_changes}}
                reasons = fb_reasons
                source = "rule_fallback"

        result["changes"] = changes
        result["reasons"] = reasons
        result["source"] = source

        if changes:
            _apply_changes_to_cfg(cfg, changes)
            if dry_run:
                logger.info(f"[dry-run] source={source} 조정안: {changes} (config 미수정)")
            else:
                try:
                    backup = _write_config_atomic(config_path, cfg)
                    result["applied"] = True
                    result["backup"] = backup
                    logger.info(f"[applied] source={source} config 수정 완료: {changes} | backup={backup}")
                except Exception as e:
                    result["apply_error"] = str(e)
                    logger.error(f"config 수정 실패: {e}")
        else:
            logger.info("[no-change] 적용할 변경 없음")

    # 9) review_log.json 저장
    try:
        out_path = OUTPUT_DIR / "review_log.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info(f"[review_log] 저장: {out_path}")
    except Exception as e:
        logger.error(f"review_log 저장 실패: {e}")

    # 10) Discord 요약
    _notify_summary(stats, result.get("changes", {}), result.get("reasons", []),
                    lookback_days, result.get("applied", False),
                    len(news), result.get("source", "none"),
                    commit_meta=commit_meta)

    logger.info("=== reviewer done ===")
    return result


if __name__ == "__main__":
    run_review()
