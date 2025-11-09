"""Database models for the quant platform."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, DateTime, Float, Integer, JSON, Boolean, Index, Text
from sqlalchemy.dialects.postgresql import UUID

from backend.database import Base


class MarketData(Base):
    """Market data (EOD and intraday) storage."""

    __tablename__ = "market_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String(20), nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)  # 1D, 1H, etc.
    date = Column(DateTime, nullable=False, index=True)

    # OHLCV data
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_symbol_exchange_timeframe_date', 'symbol', 'exchange', 'timeframe', 'date', unique=True),
    )

    def __repr__(self):
        return f"<MarketData {self.symbol}.{self.exchange} {self.date} tf={self.timeframe}>"


class FundamentalData(Base):
    """Fundamental data storage with point-in-time integrity."""

    __tablename__ = "fundamental_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String(20), nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)

    # Point-in-time fields
    period_date = Column(DateTime, nullable=False, index=True)  # Quarter/year end
    report_date = Column(DateTime, index=True)  # When data became available
    effective_date = Column(DateTime, index=True)  # Earliest date to use data (PIT)

    # Statement type
    statement_kind = Column(String(20))  # quarterly, annual, ttm

    # Fundamentals as JSON (flexible schema)
    data = Column(JSON, nullable=False)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_symbol_period', 'symbol', 'exchange', 'period_date'),
    )

    def __repr__(self):
        return f"<FundamentalData {self.symbol}.{self.exchange} {self.period_date}>"


class Backtest(Base):
    """Backtest configuration and results."""

    __tablename__ = "backtests"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(200))

    # Configuration
    strategy_name = Column(String(100), nullable=False)
    strategy_params = Column(JSON)
    symbols = Column(JSON)  # List of symbols
    exchange = Column(String(20))
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    timeframe = Column(String(10), default="1D")
    use_fundamentals = Column(Boolean, default=False)

    # Backtest settings
    initial_cash = Column(Float, default=10000.0)
    commission = Column(Float, default=0.001)

    # Execution status
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    progress = Column(Float, default=0.0)
    error_message = Column(Text)

    # Results (summary metrics)
    sharpe_ratio = Column(Float)
    total_return = Column(Float)
    max_drawdown = Column(Float)
    num_trades = Column(Integer)
    win_rate = Column(Float)

    # Full results as JSON
    results_json = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    def __repr__(self):
        return f"<Backtest {self.id} {self.strategy_name} {self.status}>"


class DataFetchTask(Base):
    """Track background data fetch tasks."""

    __tablename__ = "data_fetch_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbols = Column(JSON)  # List of symbols to fetch
    exchange = Column(String(20))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    timeframe = Column(String(10))
    data_type = Column(String(20))  # eod, intraday, fundamentals

    # Status
    status = Column(String(20), default="pending")
    progress = Column(Float, default=0.0)
    error_message = Column(Text)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    def __repr__(self):
        return f"<DataFetchTask {self.id} {self.data_type} {self.status}>"
