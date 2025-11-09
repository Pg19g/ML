"""Pydantic schemas for data models."""

from datetime import datetime, date
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field


class FetchDataRequest(BaseModel):
    """Request to fetch market data."""
    symbols: List[str]
    exchange: str
    start_date: date
    end_date: date
    timeframe: str = "1D"
    data_type: str = "eod"  # eod, intraday, fundamentals


class FetchDataResponse(BaseModel):
    """Response from data fetch request."""
    task_id: str
    status: str
    message: str


class MarketDataPoint(BaseModel):
    """Single market data point."""
    symbol: str
    exchange: str
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: str


class SymbolInfo(BaseModel):
    """Exchange symbol information."""
    code: str
    name: Optional[str] = None
    country: Optional[str] = None
    exchange: str
    type: Optional[str] = None


class ExchangeListResponse(BaseModel):
    """List of supported exchanges."""
    exchanges: List[str]


class SymbolListResponse(BaseModel):
    """List of symbols for an exchange."""
    exchange: str
    symbols: List[SymbolInfo]
    count: int
