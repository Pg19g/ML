"""Data manager with PostgreSQL caching."""

from datetime import datetime, date
from typing import List, Optional
from uuid import UUID

import pandas as pd
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import and_

from backend.data.eodhd_client import EODHDClient
from backend.models import MarketData, FundamentalData, DataFetchTask


class DataManager:
    """
    Manages market data with PostgreSQL caching.

    Workflow:
    1. Check if data exists in cache (PostgreSQL)
    2. If not, fetch from EODHD API
    3. Store in cache for future use
    """

    def __init__(self, db: Session, eodhd_client: Optional[EODHDClient] = None):
        """Initialize data manager."""
        self.db = db
        self.client = eodhd_client or EODHDClient()

    async def get_or_fetch_eod(
        self,
        symbol: str,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        Get EOD data from cache or fetch from API.

        Args:
            symbol: Ticker symbol
            exchange: Exchange code
            start_date: Start date
            end_date: End date

        Returns:
            DataFrame with OHLCV data indexed by date
        """
        # Check cache
        cached_data = self._get_cached_data(
            symbol, exchange, "1D", start_date, end_date
        )

        if len(cached_data) > 0:
            # Check if we have complete coverage
            expected_days = (end_date - start_date).days
            if len(cached_data) >= expected_days * 0.7:  # Allow for weekends/holidays
                logger.info(f"Using cached EOD data for {symbol}.{exchange}")
                return self._records_to_dataframe(cached_data)

        # Fetch from API
        logger.info(f"Fetching EOD data from API for {symbol}.{exchange}")
        df = await self.client.fetch_eod_data(symbol, exchange, start_date, end_date)

        if not df.empty:
            # Cache the data
            self._cache_dataframe(df, symbol, exchange, "1D")

        return df

    async def get_or_fetch_intraday(
        self,
        symbol: str,
        exchange: str,
        interval: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Get intraday data from cache or fetch from API."""
        timeframe = interval.upper()  # 1H, 5M, etc.

        # Check cache
        cached_data = self._get_cached_data(
            symbol, exchange, timeframe, start_date, end_date
        )

        if len(cached_data) > 0:
            logger.info(f"Using cached {timeframe} data for {symbol}.{exchange}")
            return self._records_to_dataframe(cached_data)

        # Fetch from API
        logger.info(f"Fetching {timeframe} data from API for {symbol}.{exchange}")
        df = await self.client.fetch_intraday_data(
            symbol, exchange, interval, start_date, end_date
        )

        if not df.empty:
            # Cache the data
            self._cache_dataframe(df, symbol, exchange, timeframe)

        return df

    async def get_or_fetch_fundamentals(
        self,
        symbol: str,
        exchange: str,
    ) -> dict:
        """Get fundamentals from cache or fetch from API."""
        # Check cache (look for most recent entry)
        cached = (
            self.db.query(FundamentalData)
            .filter(
                FundamentalData.symbol == symbol,
                FundamentalData.exchange == exchange,
            )
            .order_by(FundamentalData.created_at.desc())
            .first()
        )

        # Refresh if older than 24 hours
        if cached:
            age = datetime.utcnow() - cached.created_at
            if age.total_seconds() < 86400:
                logger.info(f"Using cached fundamentals for {symbol}.{exchange}")
                return cached.data

        # Fetch from API
        logger.info(f"Fetching fundamentals from API for {symbol}.{exchange}")
        data = await self.client.fetch_fundamentals(symbol, exchange)

        if data:
            # Cache fundamentals (simplified - not full PIT implementation)
            fundamental = FundamentalData(
                symbol=symbol,
                exchange=exchange,
                period_date=datetime.utcnow(),  # Placeholder
                data=data,
            )
            self.db.add(fundamental)
            self.db.commit()

        return data

    def _get_cached_data(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_date: date,
        end_date: date,
    ) -> List[MarketData]:
        """Query cached market data."""
        return (
            self.db.query(MarketData)
            .filter(
                and_(
                    MarketData.symbol == symbol,
                    MarketData.exchange == exchange,
                    MarketData.timeframe == timeframe,
                    MarketData.date >= start_date,
                    MarketData.date <= end_date,
                )
            )
            .order_by(MarketData.date)
            .all()
        )

    def _records_to_dataframe(self, records: List[MarketData]) -> pd.DataFrame:
        """Convert database records to DataFrame."""
        data = []
        for record in records:
            data.append({
                "date": record.date,
                "Open": record.open,
                "High": record.high,
                "Low": record.low,
                "Close": record.close,
                "Volume": record.volume,
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df = df.set_index("date")

        return df

    def _cache_dataframe(
        self,
        df: pd.DataFrame,
        symbol: str,
        exchange: str,
        timeframe: str,
    ):
        """Cache DataFrame to database."""
        records = []

        for idx, row in df.iterrows():
            # Check if already exists
            existing = (
                self.db.query(MarketData)
                .filter(
                    and_(
                        MarketData.symbol == symbol,
                        MarketData.exchange == exchange,
                        MarketData.timeframe == timeframe,
                        MarketData.date == idx,
                    )
                )
                .first()
            )

            if not existing:
                record = MarketData(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=timeframe,
                    date=idx,
                    open=float(row["Open"]) if "Open" in row else None,
                    high=float(row["High"]) if "High" in row else None,
                    low=float(row["Low"]) if "Low" in row else None,
                    close=float(row["Close"]) if "Close" in row else None,
                    volume=float(row["Volume"]) if "Volume" in row else None,
                )
                records.append(record)

        if records:
            self.db.bulk_save_objects(records)
            self.db.commit()
            logger.info(f"Cached {len(records)} records for {symbol}.{exchange}")

    async def refresh_data(
        self,
        symbols: List[str],
        exchange: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1D",
    ) -> int:
        """Force refresh data from API."""
        count = 0
        for symbol in symbols:
            if timeframe == "1D":
                df = await self.client.fetch_eod_data(
                    symbol, exchange, start_date, end_date
                )
            else:
                df = await self.client.fetch_intraday_data(
                    symbol, exchange, timeframe.lower(), start_date, end_date
                )

            if not df.empty:
                self._cache_dataframe(df, symbol, exchange, timeframe)
                count += 1

        return count
