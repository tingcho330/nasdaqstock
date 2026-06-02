#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recorder Module - 데이터 기록 및 관리 시스템 (수정된 버전)
"""

import logging
import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, asdict

try:
    from db_debug import is_enabled as _db_dbg_enabled, log as _db_dbg_log, log_trade_in as _db_dbg_trade_in
    from db_debug import log_trade_out as _db_dbg_trade_out, log_skip as _db_dbg_skip, caller as _db_dbg_caller
except ImportError:
    def _db_dbg_enabled():
        return False
    def _db_dbg_log(*args, **kwargs):
        pass
    def _db_dbg_trade_in(*args, **kwargs):
        pass
    def _db_dbg_trade_out(*args, **kwargs):
        pass
    def _db_dbg_skip(*args, **kwargs):
        pass
    def _db_dbg_caller(depth=2):
        return ""
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None

from utils import normalize_ticker_6

_MARKET = lambda: os.getenv("MARKET", "SP500")


def is_completed_sell(trade: Any) -> bool:
    """체결 완료 매도 여부 (reviewer·집계 공통)."""
    action = str(getattr(trade, "action", "")).upper()
    if action != "SELL":
        return False
    status = str(getattr(trade, "order_status", "executed") or "").lower()
    if status in ("pending", "partial", "cancelled", "failed", "submitted"):
        return False
    exe = int(getattr(trade, "executed_qty", 0) or 0)
    qty = int(getattr(trade, "quantity", 0) or 0)
    price = float(getattr(trade, "price", 0) or 0)
    if exe > 0:
        return True
    return status == "executed" and qty > 0 and price > 0

@dataclass
class TradeRecord:
    """거래 기록"""
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
    order_status: str = "executed"  # executed | pending | cancelled (당일 매도 방지용)
    # ── 주문/체결 추적 (pending 리컨실용) ───────────────────────────────
    order_id: str = ""             # KIS 주문번호(ODNO)
    requested_qty: int = 0         # 주문 요청 수량
    executed_qty: int = 0          # 체결 수량(부분 체결 포함)
    last_status_update_ts: str = ""  # 마지막 상태 갱신 시각(ISO)
    # ── 매도 사유 기록 ────────────────────────────────────────────────
    sell_reason: str = ""          # 매도 사유(문장)
    reason_code: str = ""          # 매도 사유 코드(집계용)
    structured_context: str = ""   # JSON 문자열(선택)

@dataclass
class PortfolioSnapshot:
    """포트폴리오 스냅샷"""
    timestamp: datetime
    total_value: float
    cash: float
    holdings: List[Dict[str, Any]]
    performance_metrics: Dict[str, Any]  # Dict로 변경
    market_state: Optional[Dict[str, Any]] = None

class DataRecorder:
    """데이터 기록 및 관리 시스템"""
    
    def __init__(self, db_path: str = "output/trading_data.db"):
        # 경로 정규화
        if not db_path:
            db_path = "output/trading_data.db"
        
        # 절대 경로로 변환
        if not os.path.isabs(db_path):
            db_path = os.path.abspath(db_path)
        
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self._init_database()
    
    def _init_database(self):
        """데이터베이스 초기화"""
        try:
            # 디렉토리 생성
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
                self.logger.info(f"DB 디렉토리 생성/확인: {db_dir}")
            
            # DB 파일 생성 및 접근 가능 여부 확인
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 거래 기록 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trade_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        ticker TEXT NOT NULL,
                        action TEXT NOT NULL,
                        quantity INTEGER NOT NULL,
                        price REAL NOT NULL,
                        amount REAL NOT NULL,
                        commission REAL NOT NULL,
                        tax REAL NOT NULL,
                        total_cost REAL NOT NULL,
                        net_amount REAL NOT NULL,
                        profit_loss REAL DEFAULT 0.0,
                        holding_period_days INTEGER DEFAULT 0,
                        sector TEXT DEFAULT '',
                        market_regime TEXT DEFAULT '',
                        order_status TEXT DEFAULT 'executed',
                        order_id TEXT DEFAULT '',
                        requested_qty INTEGER DEFAULT 0,
                        executed_qty INTEGER DEFAULT 0,
                        last_status_update_ts TEXT DEFAULT '',
                        sell_reason TEXT DEFAULT '',
                        reason_code TEXT DEFAULT '',
                        structured_context TEXT DEFAULT ''
                    )
                ''')

                # 포지션 테이블 (A안): 티커 단위로 entry/stop/target을 영속 저장
                # - 기존 보유 종목이 trade_records에 BUY 기록이 없어도 동작하게 하기 위함
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS positions (
                        ticker TEXT PRIMARY KEY,
                        entry_price REAL NOT NULL,
                        stop_price REAL NOT NULL,
                        target_price REAL NOT NULL,
                        levels_source TEXT NOT NULL,
                        levels_updated_at TEXT NOT NULL,
                        entry_updated_at TEXT NOT NULL,
                        qty INTEGER DEFAULT 0,
                        avg_price REAL DEFAULT 0.0,
                        last_seen_at TEXT DEFAULT ''
                    )
                ''')
                
                # 포트폴리오 스냅샷 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        total_value REAL NOT NULL,
                        cash REAL NOT NULL,
                        holdings TEXT NOT NULL,
                        performance_metrics TEXT NOT NULL,
                        market_state TEXT
                    )
                ''')
                
                # 성과 지표 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS performance_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        total_return REAL NOT NULL,
                        annualized_return REAL NOT NULL,
                        volatility REAL NOT NULL,
                        sharpe_ratio REAL NOT NULL,
                        max_drawdown REAL NOT NULL,
                        win_rate REAL NOT NULL,
                        profit_factor REAL NOT NULL,
                        calmar_ratio REAL NOT NULL,
                        sortino_ratio REAL NOT NULL,
                        var_95 REAL NOT NULL,
                        cvar_95 REAL NOT NULL,
                        turnover_rate REAL NOT NULL,
                        transaction_costs REAL NOT NULL,
                        net_return REAL NOT NULL
                    )
                ''')
                
                # 시장 데이터 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS market_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        market_regime TEXT NOT NULL,
                        volatility_level TEXT NOT NULL,
                        trend_direction TEXT NOT NULL,
                        confidence REAL NOT NULL,
                        vix REAL DEFAULT 0.0,
                        atr_ratio REAL DEFAULT 0.0,
                        price_volatility REAL DEFAULT 0.0,
                        volume_volatility REAL DEFAULT 0.0,
                        momentum REAL DEFAULT 0.0,
                        trend_strength REAL DEFAULT 0.0
                    )
                ''')
                
                # RSI 이력 테이블 (개선된 RSI 전략용)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS rsi_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        ticker TEXT NOT NULL,
                        rsi_value REAL NOT NULL,
                        price REAL NOT NULL,
                        UNIQUE(ticker, timestamp)
                    )
                ''')
                
                # 인덱스 생성 (조회 성능 향상)
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_rsi_history_ticker_timestamp 
                    ON rsi_history(ticker, timestamp DESC)
                ''')

                # positions 인덱스 (운영 편의)
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_last_seen_at ON positions(last_seen_at)")
                    conn.commit()
                except Exception:
                    pass
                
                # order_status 컬럼 추가 (기존 DB 마이그레이션, 당일 매도 방지용)
                try:
                    cursor.execute(
                        "ALTER TABLE trade_records ADD COLUMN order_status TEXT DEFAULT 'executed'"
                    )
                    conn.commit()
                    self.logger.info("trade_records.order_status 컬럼 추가 완료")
                except sqlite3.OperationalError as e:
                    if "duplicate column" not in str(e).lower():
                        raise

                # 주문/체결 추적 + 매도 사유 컬럼들 (기존 DB 마이그레이션)
                for col_sql, col_name in [
                    ("ALTER TABLE trade_records ADD COLUMN order_id TEXT DEFAULT ''", "order_id"),
                    ("ALTER TABLE trade_records ADD COLUMN requested_qty INTEGER DEFAULT 0", "requested_qty"),
                    ("ALTER TABLE trade_records ADD COLUMN executed_qty INTEGER DEFAULT 0", "executed_qty"),
                    ("ALTER TABLE trade_records ADD COLUMN last_status_update_ts TEXT DEFAULT ''", "last_status_update_ts"),
                    ("ALTER TABLE trade_records ADD COLUMN sell_reason TEXT DEFAULT ''", "sell_reason"),
                    ("ALTER TABLE trade_records ADD COLUMN reason_code TEXT DEFAULT ''", "reason_code"),
                    ("ALTER TABLE trade_records ADD COLUMN structured_context TEXT DEFAULT ''", "structured_context"),
                ]:
                    try:
                        cursor.execute(col_sql)
                        conn.commit()
                        self.logger.info(f"trade_records.{col_name} 컬럼 추가 완료")
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e).lower():
                            raise

                # 인덱스 (조회/업데이트 성능)
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_records_order_id ON trade_records(order_id)")
                    conn.commit()
                except Exception:
                    pass
                
                conn.commit()
                
                # DB 파일 접근 가능 여부 확인
                if os.path.exists(self.db_path) and os.access(self.db_path, os.R_OK | os.W_OK):
                    self.logger.info(f"데이터베이스 초기화 완료: {self.db_path}")
                    self.logger.info(f"DB 파일 크기: {os.path.getsize(self.db_path)} bytes")
                else:
                    raise Exception(f"DB 파일 접근 불가: {self.db_path}")
                
        except Exception as e:
            self.logger.error(f"데이터베이스 초기화 실패: {e}")
            self.logger.error(f"DB 경로: {self.db_path}")
            self.logger.error(f"디렉토리 존재 여부: {os.path.exists(os.path.dirname(self.db_path))}")
            self.logger.error(f"디렉토리 권한: {os.access(os.path.dirname(self.db_path), os.W_OK) if os.path.exists(os.path.dirname(self.db_path)) else 'N/A'}")
            raise
    
    def save_trade_record(self, trade_record: TradeRecord) -> bool:
        """거래 기록 저장"""
        try:
            status = getattr(trade_record, "order_status", "executed")
            _db_dbg_log(
                "recorder.save_trade_record.IN",
                caller=_db_dbg_caller(2),
                db_path=self.db_path,
                ticker=trade_record.ticker,
                action=trade_record.action,
                order_status=status,
                order_id=getattr(trade_record, "order_id", "") or "",
                qty=trade_record.quantity,
                executed_qty=getattr(trade_record, "executed_qty", 0),
            )
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO trade_records 
                    (timestamp, ticker, action, quantity, price, amount, commission, tax, 
                     total_cost, net_amount, profit_loss, holding_period_days, sector, market_regime,
                     order_status, order_id, requested_qty, executed_qty, last_status_update_ts,
                     sell_reason, reason_code, structured_context)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_record.timestamp.isoformat(),
                    trade_record.ticker,
                    trade_record.action,
                    trade_record.quantity,
                    trade_record.price,
                    trade_record.amount,
                    trade_record.commission,
                    trade_record.tax,
                    trade_record.total_cost,
                    trade_record.net_amount,
                    trade_record.profit_loss,
                    trade_record.holding_period_days,
                    trade_record.sector,
                    trade_record.market_regime,
                    status,
                    getattr(trade_record, "order_id", "") or "",
                    int(getattr(trade_record, "requested_qty", 0) or 0),
                    int(getattr(trade_record, "executed_qty", 0) or 0),
                    getattr(trade_record, "last_status_update_ts", "") or "",
                    getattr(trade_record, "sell_reason", "") or "",
                    getattr(trade_record, "reason_code", "") or "",
                    getattr(trade_record, "structured_context", "") or "",
                ))
                row_id = cursor.lastrowid
                conn.commit()
                self.logger.info(f"거래 기록 저장 완료: {trade_record.ticker} {trade_record.action} (status={status})")
                _db_dbg_log(
                    "recorder.save_trade_record.OK",
                    row_id=row_id,
                    ticker=trade_record.ticker,
                    order_status=status,
                    order_id=getattr(trade_record, "order_id", "") or "",
                )
                return True
                
        except Exception as e:
            self.logger.error(f"거래 기록 저장 실패: {e}")
            _db_dbg_log("recorder.save_trade_record.FAIL", error=str(e), ticker=getattr(trade_record, "ticker", ""))
            return False

    def upsert_trade_record_by_order_id(self, trade_record: TradeRecord) -> bool:
        """
        order_id가 있으면 해당 주문 기준으로 UPDATE/INSERT 수행.
        - pending → executed/partial/cancelled 정리에 사용.
        """
        try:
            order_id = (getattr(trade_record, "order_id", "") or "").strip()
            _db_dbg_log(
                "recorder.upsert.IN",
                caller=_db_dbg_caller(2),
                order_id=order_id or "(empty)",
                ticker=trade_record.ticker,
                action=trade_record.action,
                order_status=getattr(trade_record, "order_status", "executed"),
            )
            if not order_id:
                _db_dbg_skip(
                    "recorder.upsert.NO_ORDER_ID",
                    reason="order_id empty -> fallback save_trade_record (new INSERT)",
                    ticker=trade_record.ticker,
                    action=trade_record.action,
                )
                return self.save_trade_record(trade_record)

            status = getattr(trade_record, "order_status", "executed")
            now_iso = datetime.now().isoformat()
            last_ts = getattr(trade_record, "last_status_update_ts", "") or now_iso

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM trade_records WHERE order_id = ? ORDER BY id DESC LIMIT 1",
                    (order_id,),
                )
                row = cursor.fetchone()
                if row:
                    _db_dbg_log(
                        "recorder.upsert.UPDATE",
                        existing_row_id=int(row[0]),
                        order_id=order_id,
                        new_status=status,
                    )
                    cursor.execute(
                        '''
                        UPDATE trade_records
                        SET timestamp = ?,
                            ticker = ?,
                            action = ?,
                            quantity = ?,
                            price = ?,
                            amount = ?,
                            commission = ?,
                            tax = ?,
                            total_cost = ?,
                            net_amount = ?,
                            profit_loss = ?,
                            holding_period_days = ?,
                            sector = ?,
                            market_regime = ?,
                            order_status = ?,
                            requested_qty = ?,
                            executed_qty = ?,
                            last_status_update_ts = ?,
                            sell_reason = COALESCE(NULLIF(?, ''), sell_reason),
                            reason_code = COALESCE(NULLIF(?, ''), reason_code),
                            structured_context = COALESCE(NULLIF(?, ''), structured_context)
                        WHERE id = ?
                        ''',
                        (
                            trade_record.timestamp.isoformat(),
                            trade_record.ticker,
                            trade_record.action,
                            int(trade_record.quantity),
                            float(trade_record.price),
                            float(trade_record.amount),
                            float(trade_record.commission),
                            float(trade_record.tax),
                            float(trade_record.total_cost),
                            float(trade_record.net_amount),
                            float(trade_record.profit_loss),
                            int(trade_record.holding_period_days),
                            trade_record.sector,
                            trade_record.market_regime,
                            status,
                            int(getattr(trade_record, "requested_qty", 0) or 0),
                            int(getattr(trade_record, "executed_qty", 0) or 0),
                            last_ts,
                            getattr(trade_record, "sell_reason", "") or "",
                            getattr(trade_record, "reason_code", "") or "",
                            getattr(trade_record, "structured_context", "") or "",
                            int(row[0]),
                        ),
                    )
                else:
                    # INSERT
                    _db_dbg_log(
                        "recorder.upsert.INSERT_FALLBACK",
                        order_id=order_id,
                        reason="no existing row for order_id",
                    )
                    trade_record.last_status_update_ts = last_ts
                    return self.save_trade_record(trade_record)

                conn.commit()
                self.logger.info(f"거래 기록 UPSERT 완료: order_id={order_id} {trade_record.ticker} {trade_record.action} (status={status})")
                _db_dbg_log(
                    "recorder.upsert.OK",
                    order_id=order_id,
                    row_id=int(row[0]),
                    order_status=status,
                    rows_updated=1,
                )
                return True
        except Exception as e:
            self.logger.error(f"거래 기록 UPSERT 실패: {e}")
            _db_dbg_log("recorder.upsert.FAIL", error=str(e), order_id=order_id)
            return False

    def mark_pending_buy_cancelled(self, ticker: str, target_date: Optional[datetime] = None) -> int:
        """
        해당 종목·해당 일자의 pending 매수 기록을 cancelled로 표시 (15시 20분 미체결 취소 시 호출).
        반환: 업데이트된 행 수.
        """
        try:
            ticker = normalize_ticker_6(ticker, _MARKET())
            if target_date is None:
                target_date = datetime.now()
            date_str = target_date.strftime("%Y-%m-%d")
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE trade_records
                    SET order_status = 'cancelled'
                    WHERE ticker = ? AND action = 'BUY'
                      AND date(timestamp) = ?
                      AND (order_status = 'pending' OR order_status IS NULL)
                ''', (ticker, date_str))
                n = cursor.rowcount
                conn.commit()
                if n:
                    self.logger.info(f"pending 매수 취소 처리: {ticker} {n}건")
                return n
        except Exception as e:
            self.logger.error(f"pending 매수 취소 처리 실패: {e}")
            return 0

    def mark_pending_order_cancelled(self, order_id: str) -> int:
        """order_id 기준 pending 주문을 cancelled로 표시. 반환: 업데이트된 행 수."""
        try:
            order_id = (order_id or "").strip()
            if not order_id:
                return 0
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''
                    UPDATE trade_records
                    SET order_status = 'cancelled',
                        last_status_update_ts = ?
                    WHERE order_id = ?
                      AND (order_status = 'pending' OR order_status IS NULL OR order_status = '')
                    ''',
                    (datetime.now().isoformat(), order_id),
                )
                n = cursor.rowcount
                conn.commit()
                if n:
                    self.logger.info(f"pending 주문 취소 처리(order_id): {order_id} {n}건")
                return n
        except Exception as e:
            self.logger.error(f"pending 주문 취소 처리(order_id) 실패: {e}")
            return 0
    
    def save_portfolio_snapshot(self, snapshot: PortfolioSnapshot) -> bool:
        """포트폴리오 스냅샷 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO portfolio_snapshots 
                    (timestamp, total_value, cash, holdings, performance_metrics, market_state)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    snapshot.timestamp.isoformat(),
                    snapshot.total_value,
                    snapshot.cash,
                    json.dumps(snapshot.holdings),
                    json.dumps(snapshot.performance_metrics),
                    json.dumps(snapshot.market_state) if snapshot.market_state else None
                ))
                conn.commit()
                self.logger.info(f"포트폴리오 스냅샷 저장 완료: {snapshot.timestamp}")
                return True
                
        except Exception as e:
            self.logger.error(f"포트폴리오 스냅샷 저장 실패: {e}")
            return False
    
    def save_performance_metrics(self, timestamp: datetime, metrics: Dict[str, Any]) -> bool:
        """성과 지표 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO performance_metrics 
                    (timestamp, total_return, annualized_return, volatility, sharpe_ratio, 
                     max_drawdown, win_rate, profit_factor, calmar_ratio, sortino_ratio, 
                     var_95, cvar_95, turnover_rate, transaction_costs, net_return)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp.isoformat(),
                    metrics.get('total_return', 0.0),
                    metrics.get('annualized_return', 0.0),
                    metrics.get('volatility', 0.0),
                    metrics.get('sharpe_ratio', 0.0),
                    metrics.get('max_drawdown', 0.0),
                    metrics.get('win_rate', 0.0),
                    metrics.get('profit_factor', 0.0),
                    metrics.get('calmar_ratio', 0.0),
                    metrics.get('sortino_ratio', 0.0),
                    metrics.get('var_95', 0.0),
                    metrics.get('cvar_95', 0.0),
                    metrics.get('turnover_rate', 0.0),
                    metrics.get('transaction_costs', 0.0),
                    metrics.get('net_return', 0.0)
                ))
                conn.commit()
                self.logger.info(f"성과 지표 저장 완료: {timestamp}")
                return True
                
        except Exception as e:
            self.logger.error(f"성과 지표 저장 실패: {e}")
            return False
    
    def save_market_data(self, timestamp: datetime, market_data: Dict[str, Any]) -> bool:
        """시장 데이터 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO market_data 
                    (timestamp, market_regime, volatility_level, trend_direction, confidence,
                     vix, atr_ratio, price_volatility, volume_volatility, momentum, trend_strength)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp.isoformat(),
                    market_data.get('market_regime', ''),
                    market_data.get('volatility_level', ''),
                    market_data.get('trend_direction', ''),
                    market_data.get('confidence', 0.0),
                    market_data.get('vix', 0.0),
                    market_data.get('atr_ratio', 0.0),
                    market_data.get('price_volatility', 0.0),
                    market_data.get('volume_volatility', 0.0),
                    market_data.get('momentum', 0.0),
                    market_data.get('trend_strength', 0.0)
                ))
                conn.commit()
                self.logger.info(f"시장 데이터 저장 완료: {timestamp}")
                return True
                
        except Exception as e:
            self.logger.error(f"시장 데이터 저장 실패: {e}")
            return False
    
    def get_trade_records(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        ticker: Optional[str] = None,
        action: Optional[str] = None
    ) -> List[TradeRecord]:
        """거래 기록 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM trade_records WHERE 1=1"
                params = []
                
                if start_date:
                    query += " AND timestamp >= ?"
                    params.append(start_date.isoformat())
                
                if end_date:
                    query += " AND timestamp <= ?"
                    params.append(end_date.isoformat())
                
                if ticker:
                    query += " AND ticker = ?"
                    params.append(ticker)
                
                if action:
                    query += " AND action = ?"
                    params.append(action)
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                trade_records = []
                for row in rows:
                    order_status = (row[15] or "executed") if len(row) > 15 else "executed"
                    trade_records.append(TradeRecord(
                        timestamp=datetime.fromisoformat(row[1]),
                        ticker=row[2],
                        action=row[3],
                        quantity=row[4],
                        price=row[5],
                        amount=row[6],
                        commission=row[7],
                        tax=row[8],
                        total_cost=row[9],
                        net_amount=row[10],
                        profit_loss=row[11],
                        holding_period_days=row[12],
                        sector=row[13],
                        market_regime=row[14],
                        order_status=order_status
                    ))
                
                return trade_records
                
        except Exception as e:
            self.logger.error(f"거래 기록 조회 실패: {e}")
            return []
    
    def get_portfolio_snapshots(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[PortfolioSnapshot]:
        """포트폴리오 스냅샷 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM portfolio_snapshots WHERE 1=1"
                params = []
                
                if start_date:
                    query += " AND timestamp >= ?"
                    params.append(start_date.isoformat())
                
                if end_date:
                    query += " AND timestamp <= ?"
                    params.append(end_date.isoformat())
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                snapshots = []
                for row in rows:
                    snapshots.append(PortfolioSnapshot(
                        timestamp=datetime.fromisoformat(row[1]),
                        total_value=row[2],
                        cash=row[3],
                        holdings=json.loads(row[4]),
                        performance_metrics=json.loads(row[5]),
                        market_state=json.loads(row[6]) if row[6] else None
                    ))
                
                return snapshots
                
        except Exception as e:
            self.logger.error(f"포트폴리오 스냅샷 조회 실패: {e}")
            return []
    
    def get_performance_metrics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """성과 지표 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM performance_metrics WHERE 1=1"
                params = []
                
                if start_date:
                    query += " AND timestamp >= ?"
                    params.append(start_date.isoformat())
                
                if end_date:
                    query += " AND timestamp <= ?"
                    params.append(end_date.isoformat())
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                metrics_list = []
                for row in rows:
                    metrics_list.append({
                        'timestamp': datetime.fromisoformat(row[1]),
                        'total_return': row[2],
                        'annualized_return': row[3],
                        'volatility': row[4],
                        'sharpe_ratio': row[5],
                        'max_drawdown': row[6],
                        'win_rate': row[7],
                        'profit_factor': row[8],
                        'calmar_ratio': row[9],
                        'sortino_ratio': row[10],
                        'var_95': row[11],
                        'cvar_95': row[12],
                        'turnover_rate': row[13],
                        'transaction_costs': row[14],
                        'net_return': row[15]
                    })
                
                return metrics_list
                
        except Exception as e:
            self.logger.error(f"성과 지표 조회 실패: {e}")
            return []
    
    def get_market_data(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """시장 데이터 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM market_data WHERE 1=1"
                params = []
                
                if start_date:
                    query += " AND timestamp >= ?"
                    params.append(start_date.isoformat())
                
                if end_date:
                    query += " AND timestamp <= ?"
                    params.append(end_date.isoformat())
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                market_data_list = []
                for row in rows:
                    market_data_list.append({
                        'timestamp': datetime.fromisoformat(row[1]),
                        'market_regime': row[2],
                        'volatility_level': row[3],
                        'trend_direction': row[4],
                        'confidence': row[5],
                        'vix': row[6],
                        'atr_ratio': row[7],
                        'price_volatility': row[8],
                        'volume_volatility': row[9],
                        'momentum': row[10],
                        'trend_strength': row[11]
                    })
                
                return market_data_list
                
        except Exception as e:
            self.logger.error(f"시장 데이터 조회 실패: {e}")
            return []

    # ───────────────── 주문/체결 리컨실 지원 ─────────────────
    def update_order_status(
        self,
        *,
        order_id: str,
        order_status: str,
        executed_qty: Optional[int] = None,
        price: Optional[float] = None,
        profit_loss: Optional[float] = None,
        sell_reason: Optional[str] = None,
        reason_code: Optional[str] = None,
        structured_context: Optional[str] = None,
    ) -> int:
        """order_id 기준 상태 갱신. 반환: 업데이트 행 수."""
        try:
            order_id = (order_id or "").strip()
            if not order_id:
                return 0

            now_iso = datetime.now().isoformat()
            sets = ["order_status = ?", "last_status_update_ts = ?"]
            params: List[Any] = [str(order_status).lower(), now_iso]

            if executed_qty is not None:
                sets.append("executed_qty = ?")
                params.append(int(executed_qty))
            if price is not None:
                sets.append("price = ?")
                params.append(float(price))
            if profit_loss is not None:
                sets.append("profit_loss = ?")
                params.append(float(profit_loss))
            if sell_reason:
                sets.append("sell_reason = ?")
                params.append(str(sell_reason))
            if reason_code:
                sets.append("reason_code = ?")
                params.append(str(reason_code))
            if structured_context:
                sets.append("structured_context = ?")
                params.append(str(structured_context))

            params.append(order_id)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"UPDATE trade_records SET {', '.join(sets)} WHERE order_id = ?",
                    params,
                )
                n = cursor.rowcount
                conn.commit()
                if n and str(order_status).lower() == "executed":
                    try:
                        self.recompute_profit_loss_for_order_id(order_id)
                    except Exception:
                        pass
                return n
        except Exception as e:
            self.logger.error(f"order_status 업데이트 실패: {e}")
            return 0

    def get_open_orders(
        self,
        *,
        statuses: Tuple[str, ...] = ("pending", "partial"),
        since_ts: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        리컨실 대상 주문 조회.
        - order_id가 있는 pending/partial만 반환.
        """
        try:
            st = tuple(s.lower() for s in statuses)
            where = "WHERE order_id IS NOT NULL AND order_id != '' AND lower(order_status) IN ({})".format(
                ",".join(["?"] * len(st))
            )
            params: List[Any] = list(st)
            if since_ts:
                where += " AND timestamp >= ?"
                params.append(since_ts)
            q = f"""
                SELECT id, timestamp, ticker, action, quantity, price, requested_qty, executed_qty, order_status, order_id
                FROM trade_records
                {where}
                ORDER BY timestamp DESC
                LIMIT ?
            """
            params.append(int(limit))
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(q, params)
                rows = cursor.fetchall()
            out: List[Dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "id": r[0],
                        "timestamp": r[1],
                        "ticker": r[2],
                        "action": r[3],
                        "quantity": r[4],
                        "price": r[5],
                        "requested_qty": r[6],
                        "executed_qty": r[7],
                        "order_status": r[8],
                        "order_id": r[9],
                    }
                )
            return out
        except Exception as e:
            self.logger.error(f"open 주문 조회 실패: {e}")
            return []

    def get_orphan_trade_records(
        self,
        *,
        since_ts: str,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """order_id가 비어 있는 거래 기록 (backfill 대상)."""
        try:
            q = """
                SELECT id, timestamp, ticker, action, quantity, price, requested_qty, executed_qty, order_status, order_id
                FROM trade_records
                WHERE (order_id IS NULL OR order_id = '')
                  AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT ?
            """
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(q, (since_ts, int(limit)))
                rows = cursor.fetchall()
            out: List[Dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "id": r[0],
                        "timestamp": r[1],
                        "ticker": r[2],
                        "action": r[3],
                        "quantity": r[4],
                        "price": r[5],
                        "requested_qty": r[6],
                        "executed_qty": r[7],
                        "order_status": r[8],
                        "order_id": r[9],
                    }
                )
            return out
        except Exception as e:
            self.logger.error(f"orphan 거래 조회 실패: {e}")
            return []

    def backfill_order_id(
        self,
        *,
        row_id: int,
        order_id: str,
        executed_qty: Optional[int] = None,
        order_status: Optional[str] = None,
    ) -> int:
        """빈 order_id 행에 KIS 주문번호·체결 메타 backfill (기존 order_id 있으면 스킵)."""
        try:
            order_id = (order_id or "").strip()
            if not order_id:
                return 0

            sets = ["order_id = ?", "last_status_update_ts = ?"]
            params: List[Any] = [order_id, datetime.now().isoformat()]

            if executed_qty is not None:
                sets.append("executed_qty = ?")
                params.append(int(executed_qty))
            if order_status is not None:
                sets.append("order_status = ?")
                params.append(str(order_status).lower())

            params.extend([int(row_id)])
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                    UPDATE trade_records
                    SET {', '.join(sets)}
                    WHERE id = ? AND (order_id IS NULL OR order_id = '')
                    """,
                    params,
                )
                n = cursor.rowcount
                conn.commit()
            return n
        except Exception as e:
            self.logger.error(f"order_id backfill 실패: {e}")
            return 0

    # ───────────────── 포지션 레벨(진입가 기준) ─────────────────
    def get_position(self, ticker: str) -> Optional[Dict[str, Any]]:
        """positions 테이블에서 티커 포지션(레벨) 조회."""
        try:
            ticker_str = normalize_ticker_6(ticker, _MARKET())
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT
                        ticker, entry_price, stop_price, target_price,
                        levels_source, levels_updated_at, entry_updated_at,
                        qty, avg_price, last_seen_at
                    FROM positions
                    WHERE ticker = ?
                    """,
                    (ticker_str,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "ticker": row[0],
                    "entry_price": float(row[1]),
                    "stop_price": float(row[2]),
                    "target_price": float(row[3]),
                    "levels_source": row[4],
                    "levels_updated_at": row[5],
                    "entry_updated_at": row[6],
                    "qty": int(row[7] or 0),
                    "avg_price": float(row[8] or 0.0),
                    "last_seen_at": row[9] or "",
                }
        except Exception as e:
            self.logger.error(f"포지션 조회 실패 ({ticker}): {e}")
            return None

    def upsert_position_levels(
        self,
        *,
        ticker: str,
        entry_price: float,
        stop_price: float,
        target_price: float,
        levels_source: str,
        levels_updated_at: str,
        entry_updated_at: str,
        qty: int = 0,
        avg_price: float = 0.0,
        last_seen_at: str = "",
    ) -> bool:
        """positions 테이블에 포지션(레벨) upsert."""
        try:
            ticker_str = normalize_ticker_6(ticker, _MARKET())
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO positions (
                        ticker, entry_price, stop_price, target_price,
                        levels_source, levels_updated_at, entry_updated_at,
                        qty, avg_price, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(ticker) DO UPDATE SET
                        entry_price = excluded.entry_price,
                        stop_price = excluded.stop_price,
                        target_price = excluded.target_price,
                        levels_source = excluded.levels_source,
                        levels_updated_at = excluded.levels_updated_at,
                        entry_updated_at = excluded.entry_updated_at,
                        qty = excluded.qty,
                        avg_price = excluded.avg_price,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (
                        ticker_str,
                        float(entry_price),
                        float(stop_price),
                        float(target_price),
                        str(levels_source or ""),
                        str(levels_updated_at or ""),
                        str(entry_updated_at or ""),
                        int(qty or 0),
                        float(avg_price or 0.0),
                        str(last_seen_at or ""),
                    ),
                )
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"포지션 upsert 실패 ({ticker}): {e}")
            return False

    def delete_position(self, ticker: str) -> bool:
        """전량매도 등으로 오픈 포지션 정리(삭제)."""
        try:
            ticker_str = normalize_ticker_6(ticker, _MARKET())
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM positions WHERE ticker = ?", (ticker_str,))
                conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"포지션 삭제 실패 ({ticker}): {e}")
            return False

    # ───────────────── PnL 재계산(간단 FIFO) ─────────────────
    def compute_fifo_sell_pnl(
        self,
        *,
        ticker: str,
        sell_qty: int,
        sell_price: float,
        sell_timestamp: Optional[datetime] = None,
    ) -> Tuple[float, int]:
        """매도 1건에 대한 FIFO 손익·보유일수 (가장 최근 선행 매수 기준)."""
        ticker = normalize_ticker_6(ticker, _MARKET())
        sell_qty = int(sell_qty)
        sell_price = float(sell_price)
        if sell_qty <= 0 or sell_price <= 0:
            return 0.0, 0

        all_trades = self.get_trade_records(ticker=ticker)
        buys = [
            t for t in all_trades
            if str(t.action).upper() == "BUY"
            and int(t.quantity or 0) > 0
            and float(t.price or 0) > 0
        ]
        if sell_timestamp is not None:
            buys = [b for b in buys if b.timestamp <= sell_timestamp]
        if not buys:
            return 0.0, 0

        last_buy = sorted(buys, key=lambda x: x.timestamp)[-1]
        buy_price = float(last_buy.price)
        pnl = (sell_price - buy_price) * sell_qty
        hold_days = 0
        if sell_timestamp is not None:
            hold_days = max(0, (sell_timestamp - last_buy.timestamp).days)
        return round(pnl, 2), hold_days

    def recompute_profit_loss_for_order_id(self, order_id: str) -> int:
        """체결 매도 행의 profit_loss·holding_period_days 재계산."""
        order_id = (order_id or "").strip()
        if not order_id:
            return 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, timestamp, ticker, action, quantity, price, executed_qty, order_status
                    FROM trade_records WHERE order_id = ? LIMIT 1
                    """,
                    (order_id,),
                )
                row = cur.fetchone()
            if not row:
                return 0
            row_id, ts, ticker, action, qty, price, exe_qty, status = row
            if str(action).upper() != "SELL":
                return 0
            if str(status or "").lower() not in ("executed", "completed", "partial"):
                return 0
            sell_qty = int(exe_qty or qty or 0)
            sell_price = float(price or 0)
            if sell_qty <= 0 or sell_price <= 0:
                return 0
            sell_ts = datetime.fromisoformat(ts) if ts else None
            pnl, hold_days = self.compute_fifo_sell_pnl(
                ticker=str(ticker),
                sell_qty=sell_qty,
                sell_price=sell_price,
                sell_timestamp=sell_ts,
            )
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE trade_records
                    SET profit_loss = ?, holding_period_days = ?
                    WHERE id = ?
                    """,
                    (pnl, hold_days, int(row_id)),
                )
                n = cur.rowcount
                conn.commit()
            return n
        except Exception as e:
            self.logger.error(f"PnL 재계산 실패 order_id={order_id}: {e}")
            return 0
    
    def export_data_to_csv(self, output_dir: str = "data/export") -> bool:
        """데이터를 CSV로 내보내기"""
        try:
            if pd is None:
                self.logger.error("pandas가 설치되어 있지 않아 CSV 내보내기를 수행할 수 없습니다.")
                return False
            os.makedirs(output_dir, exist_ok=True)
            
            # 거래 기록 내보내기
            trade_records = self.get_trade_records()
            if trade_records:
                df_trades = pd.DataFrame([asdict(tr) for tr in trade_records])
                df_trades.to_csv(f"{output_dir}/trade_records.csv", index=False)
            
            # 포트폴리오 스냅샷 내보내기
            snapshots = self.get_portfolio_snapshots()
            if snapshots:
                df_snapshots = pd.DataFrame([asdict(s) for s in snapshots])
                df_snapshots.to_csv(f"{output_dir}/portfolio_snapshots.csv", index=False)
            
            # 성과 지표 내보내기
            metrics = self.get_performance_metrics()
            if metrics:
                df_metrics = pd.DataFrame(metrics)
                df_metrics.to_csv(f"{output_dir}/performance_metrics.csv", index=False)
            
            # 시장 데이터 내보내기
            market_data = self.get_market_data()
            if market_data:
                df_market = pd.DataFrame(market_data)
                df_market.to_csv(f"{output_dir}/market_data.csv", index=False)
            
            self.logger.info(f"데이터 내보내기 완료: {output_dir}")
            return True
            
        except Exception as e:
            self.logger.error(f"데이터 내보내기 실패: {e}")
            return False
    
    def backup_database(self, backup_path: str) -> bool:
        """데이터베이스 백업"""
        try:
            import shutil
            shutil.copy2(self.db_path, backup_path)
            self.logger.info(f"데이터베이스 백업 완료: {backup_path}")
            return True
        except Exception as e:
            self.logger.error(f"데이터베이스 백업 실패: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """데이터베이스 통계"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                stats = {}
                
                # 거래 기록 수
                cursor.execute("SELECT COUNT(*) FROM trade_records")
                stats['trade_records_count'] = cursor.fetchone()[0]
                
                # 포트폴리오 스냅샷 수
                cursor.execute("SELECT COUNT(*) FROM portfolio_snapshots")
                stats['portfolio_snapshots_count'] = cursor.fetchone()[0]
                
                # 성과 지표 수
                cursor.execute("SELECT COUNT(*) FROM performance_metrics")
                stats['performance_metrics_count'] = cursor.fetchone()[0]
                
                # 시장 데이터 수
                cursor.execute("SELECT COUNT(*) FROM market_data")
                stats['market_data_count'] = cursor.fetchone()[0]
                
                # 최신 데이터 시간
                cursor.execute("SELECT MAX(timestamp) FROM trade_records")
                stats['latest_trade_time'] = cursor.fetchone()[0]
                
                cursor.execute("SELECT MAX(timestamp) FROM portfolio_snapshots")
                stats['latest_snapshot_time'] = cursor.fetchone()[0]
                
                return stats
                
        except Exception as e:
            self.logger.error(f"데이터베이스 통계 조회 실패: {e}")
            return {}
    
    def save_rsi_snapshot(self, ticker: str, rsi: float, price: float) -> bool:
        """
        RSI 스냅샷 저장 (개선된 RSI 전략용)
        
        Args:
            ticker: 종목 코드
            rsi: RSI 값
            price: 현재 가격
        
        Returns:
            저장 성공 여부
        """
        try:
            ticker_str = normalize_ticker_6(ticker, _MARKET())
            timestamp = datetime.now().isoformat()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # INSERT OR REPLACE로 중복 방지 (같은 ticker, timestamp 조합)
                cursor.execute('''
                    INSERT OR REPLACE INTO rsi_history 
                    (timestamp, ticker, rsi_value, price)
                    VALUES (?, ?, ?, ?)
                ''', (timestamp, ticker_str, float(rsi), float(price)))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"RSI 스냅샷 저장 실패 ({ticker}): {e}")
            return False
    
    def get_rsi_history(self, ticker: str, days: int = 30) -> List[Dict[str, Any]]:
        """
        RSI 이력 조회 (개선된 RSI 전략용)
        
        Args:
            ticker: 종목 코드
            days: 조회 기간 (일수)
        
        Returns:
            RSI 이력 리스트 (최신순)
        """
        try:
            ticker_str = normalize_ticker_6(ticker, _MARKET())
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT timestamp, rsi_value, price
                    FROM rsi_history
                    WHERE ticker = ? AND timestamp >= ?
                    ORDER BY timestamp DESC
                ''', (ticker_str, cutoff_date))
                
                rows = cursor.fetchall()
                history = []
                for row in rows:
                    history.append({
                        'timestamp': row[0],
                        'rsi_value': row[1],
                        'price': row[2]
                    })
                
                return history
        except Exception as e:
            self.logger.error(f"RSI 이력 조회 실패 ({ticker}): {e}")
            return []

# 전역 DataRecorder 인스턴스
_recorder_instance = None

def get_recorder() -> DataRecorder:
    """전역 DataRecorder 인스턴스 반환"""
    global _recorder_instance
    if _recorder_instance is None:
        _recorder_instance = DataRecorder()
    return _recorder_instance

def initialize_db():
    """데이터베이스 초기화 (하위 호환성)"""
    get_recorder()

def record_trade(trade_data: Dict[str, Any]) -> bool:
    """거래 기록 저장 (하위 호환성)"""
    try:
        recorder = get_recorder()
        logger = logging.getLogger(__name__)
        _db_dbg_trade_in("record_trade.IN", trade_data)

        # 수량이 0이거나 음수인 거래는 기록하지 않음
        quantity = int(trade_data.get("qty", 0))
        if quantity <= 0:
            logger.warning(f"거래 기록 건너뜀: 수량이 0 이하 (ticker={trade_data.get('ticker')}, qty={quantity})")
            _db_dbg_skip("record_trade.SKIP", reason="qty<=0", ticker=trade_data.get("ticker"), qty=quantity)
            return False
        
        ticker = normalize_ticker_6(trade_data.get("ticker", ""), _MARKET())
        action = trade_data.get("side", "").upper()
        price = float(trade_data.get("price", 0))
        
        # 가격이 0인 경우 경고 (매도 거래에서 특히 중요)
        if price <= 0 and action == "SELL":
            logger.warning(f"⚠️ 매도 거래 가격이 0: {ticker} (qty={quantity})")
        
        # 손익 계산: pnl_amount가 있으면 사용, 없으면 매도 시 매수 가격 찾기
        profit_loss = 0.0
        holding_period_days = 0
        
        if action == "SELL":
            # pnl_amount가 직접 제공된 경우 사용
            if "pnl_amount" in trade_data and trade_data["pnl_amount"] is not None:
                profit_loss = float(trade_data["pnl_amount"])
            else:
                # 매수 거래를 찾아서 손익 계산
                ticker_trades = recorder.get_trade_records(ticker=ticker)
                buy_trades = [t for t in ticker_trades if t.action.upper() == "BUY" and t.quantity > 0]
                
                if buy_trades and price > 0:
                    # 가장 최근 매수 거래 사용 (FIFO 방식)
                    last_buy = buy_trades[-1]
                    buy_price = last_buy.price
                    if buy_price > 0:
                        profit_loss = (price - buy_price) * quantity
                        # 보유 기간 계산
                        if hasattr(last_buy, 'timestamp'):
                            buy_time = last_buy.timestamp
                            if isinstance(buy_time, str):
                                buy_time = datetime.fromisoformat(buy_time)
                            holding_period_days = (datetime.now() - buy_time).days
        
        amount = quantity * price if price > 0 else 0

        # 주문/체결 메타
        order_id = (
            trade_data.get("order_id")
            or trade_data.get("odno")
            or trade_data.get("ODNO")
            or trade_data.get("order_no")
            or ""
        )
        order_id = str(order_id).strip()
        if not order_id:
            _db_dbg_skip(
                "record_trade.EMPTY_ORDER_ID",
                reason="no order_id in payload; upsert will INSERT not UPDATE",
                ticker=ticker,
                side=action,
                trade_status=trade_data.get("trade_status"),
                payload_keys=sorted(trade_data.keys()),
            )
        requested_qty = trade_data.get("requested_qty", quantity)
        executed_qty = trade_data.get("executed_qty", 0)
        try:
            requested_qty = int(requested_qty)
        except Exception:
            requested_qty = quantity
        try:
            executed_qty = int(executed_qty)
        except Exception:
            executed_qty = 0

        # 매도 사유/코드(있으면 저장)
        sell_reason = trade_data.get("sell_reason") or trade_data.get("reason") or ""
        reason_code = trade_data.get("reason_code") or trade_data.get("strategy_details", {}).get("reason_code", "") or ""
        structured_context = trade_data.get("structured_context") or trade_data.get("strategy_details", {})
        try:
            if isinstance(structured_context, (dict, list)):
                structured_context = json.dumps(structured_context, ensure_ascii=False)
            else:
                structured_context = str(structured_context or "")
        except Exception:
            structured_context = ""
        
        # order_status: pending(주문 접수만) | executed(체결) | cancelled(미체결 취소)
        order_status = str(trade_data.get("trade_status", "executed")).lower()
        if order_status not in ("pending", "submitted", "executed", "completed", "partial", "failed", "cancelled", "market_executed"):
            order_status = "executed"
        if order_status in ("submitted",):
            order_status = "pending"
        if order_status in ("completed", "market_executed"):
            order_status = "executed"

        # pending without order_id → reconciler가 추적 불가한 orphan INSERT 방지
        if order_status == "pending" and not order_id:
            _db_dbg_skip(
                "record_trade.SKIP_PENDING_NO_ORDER_ID",
                reason="pending requires order_id for reconciler",
                ticker=ticker,
                side=action,
                payload_keys=sorted(trade_data.keys()),
            )
            logging.getLogger(__name__).warning(
                f"pending 거래 기록 생략 (order_id 없음): {ticker} {action}"
            )
            return False
        
        # TradeRecord 객체 생성
        trade_record = TradeRecord(
            timestamp=datetime.now(),
            ticker=ticker,
            action=action,
            quantity=quantity,
            price=price,
            amount=amount,
            commission=0.0,  # 기본값
            tax=0.0,  # 기본값
            total_cost=amount,
            net_amount=amount,
            profit_loss=profit_loss,
            holding_period_days=holding_period_days,
            sector=trade_data.get("strategy_details", {}).get("sector", ""),
            market_regime=trade_data.get("strategy_details", {}).get("market_regime", ""),
            order_status=order_status,
            order_id=order_id,
            requested_qty=requested_qty,
            executed_qty=executed_qty,
            last_status_update_ts=datetime.now().isoformat(),
            sell_reason=str(sell_reason or ""),
            reason_code=str(reason_code or ""),
            structured_context=structured_context,
        )

        # order_id가 있으면 UPSERT로 상태를 갱신하고, 없으면 기존 저장 방식 유지
        path = "upsert" if order_id else "insert_via_upsert_fallback"
        ok = recorder.upsert_trade_record_by_order_id(trade_record)
        _db_dbg_trade_out(
            "record_trade.OUT",
            ok=ok,
            path=path,
            ticker=ticker,
            action=action,
            order_status=order_status,
            order_id=order_id or "(empty)",
            profit_loss=profit_loss,
        )
        return ok
        
    except Exception as e:
        logging.getLogger(__name__).error(f"거래 기록 저장 실패: {e}", exc_info=True)
        _db_dbg_log("record_trade.EXCEPTION", error=str(e), caller=_db_dbg_caller(2))
        return False

def fetch_trades_by_tickers(tickers: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """티커별 거래 기록 조회 (하위 호환성)"""
    try:
        recorder = get_recorder()
        result = {}
        
        for ticker in tickers:
            ticker_str = normalize_ticker_6(ticker, _MARKET())
            trades = recorder.get_trade_records(ticker=ticker_str)
            
            # TradeRecord를 딕셔너리로 변환
            trade_dicts = []
            for trade in trades:
                trade_dict = {
                    "timestamp": trade.timestamp,
                    "ticker": trade.ticker,
                    "action": trade.action,
                    "quantity": trade.quantity,
                    "price": trade.price,
                    "order_status": getattr(trade, "order_status", "executed"),
                    "amount": trade.amount,
                    "commission": trade.commission,
                    "tax": trade.tax,
                    "total_cost": trade.total_cost,
                    "net_amount": trade.net_amount,
                    "profit_loss": trade.profit_loss,
                    "holding_period_days": trade.holding_period_days,
                    "sector": trade.sector,
                    "market_regime": trade.market_regime
                }
                trade_dicts.append(trade_dict)
            
            result[ticker_str] = trade_dicts
            
        return result
        
    except Exception as e:
        logging.getLogger(__name__).error(f"거래 기록 조회 실패: {e}")
        return {}

def mark_pending_buy_cancelled(ticker: str, target_date: Optional[datetime] = None) -> int:
    """당일 pending 매수 기록을 취소로 표시 (15시 20분 미체결 취소 시). 반환: 업데이트 행 수."""
    try:
        return get_recorder().mark_pending_buy_cancelled(ticker, target_date)
    except Exception as e:
        logging.getLogger(__name__).error(f"pending 매수 취소 표시 실패: {e}")
        return 0

def mark_pending_order_cancelled(order_id: str) -> int:
    """order_id 기준 pending 주문을 취소로 표시. 반환: 업데이트 행 수."""
    try:
        return get_recorder().mark_pending_order_cancelled(order_id)
    except Exception as e:
        logging.getLogger(__name__).error(f"pending 주문 취소 표시 실패(order_id): {e}")
        return 0

def save_rsi_snapshot(ticker: str, rsi: float, price: float) -> bool:
    """RSI 스냅샷 저장 (하위 호환성)"""
    try:
        recorder = get_recorder()
        return recorder.save_rsi_snapshot(ticker, rsi, price)
    except Exception as e:
        logging.getLogger(__name__).error(f"RSI 스냅샷 저장 실패: {e}")
        return False

def get_rsi_history(ticker: str, days: int = 30) -> List[Dict[str, Any]]:
    """RSI 이력 조회 (하위 호환성)"""
    try:
        recorder = get_recorder()
        return recorder.get_rsi_history(ticker, days)
    except Exception as e:
        logging.getLogger(__name__).error(f"RSI 이력 조회 실패: {e}")
        return []


# ── positions 하위 호환성 헬퍼 ───────────────────────────────────────────
def get_position(ticker: str) -> Optional[Dict[str, Any]]:
    return get_recorder().get_position(ticker)


def upsert_position_levels(
    *,
    ticker: str,
    entry_price: float,
    stop_price: float,
    target_price: float,
    levels_source: str,
    levels_updated_at: str,
    entry_updated_at: str,
    qty: int = 0,
    avg_price: float = 0.0,
    last_seen_at: str = "",
) -> bool:
    return get_recorder().upsert_position_levels(
        ticker=ticker,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
        levels_source=levels_source,
        levels_updated_at=levels_updated_at,
        entry_updated_at=entry_updated_at,
        qty=qty,
        avg_price=avg_price,
        last_seen_at=last_seen_at,
    )


def delete_position(ticker: str) -> bool:
    return get_recorder().delete_position(ticker)
