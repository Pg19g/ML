"""Async EODHD API client for fetching market data."""

import asyncio
from datetime import date, datetime
from typing import Dict, List, Optional, Any

import aiohttp
import pandas as pd
from loguru import logger

from backend.config import get_settings


class EODHDClient:
    """
    Async client for EODHD Historical Data API.

    Supports:
    - End-of-day prices
    - Intraday data (1h, 5m, 1m)
    - Fundamentals data
    - Exchange symbol lists
    """

    BASE_URL = "https://eodhistoricaldata.com/api"

    def __init__(self, api_key: Optional[str] = None):
        """Initialize EODHD client."""
        settings = get_settings()
        self.api_key = api_key or settings.eodhd_api_key

        if not self.api_key:
            raise ValueError(
                "EODHD API key required. Set EODHD_API_KEY env variable."
            )

    async def _make_request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make async API request with retry logic."""
        if params is None:
            params = {}

        params["api_token"] = self.api_key
        params["fmt"] = "json"

        url = f"{self.BASE_URL}/{endpoint}"

        retry_attempts = 3
        for attempt in range(retry_attempts):
            try:
                async with session.get(url, params=params, timeout=30) as response:
                    response.raise_for_status()
                    return await response.json()

            except Exception as e:
                wait_time = 2 ** attempt
                logger.warning(f"Request failed (attempt {attempt + 1}/{retry_attempts}): {e}")

                if attempt < retry_attempts - 1:
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All retries failed for {endpoint}")
                    raise

    async def fetch_eod_data(
        self,
        symbol: str,
        exchange: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Fetch end-of-day data.

        Args:
            symbol: Ticker symbol
            exchange: Exchange code (US, LSE, WSE, etc.)
            start_date: Start date
            end_date: End date

        Returns:
            DataFrame with columns: Date (index), Open, High, Low, Close, Volume
        """
        full_symbol = f"{symbol}.{exchange}"
        logger.info(f"Fetching EOD data for {full_symbol}")

        params = {}
        if start_date:
            params["from"] = start_date.strftime("%Y-%m-%d")
        if end_date:
            params["to"] = end_date.strftime("%Y-%m-%d")

        async with aiohttp.ClientSession() as session:
            data = await self._make_request(session, f"eod/{full_symbol}", params)

        if not data:
            logger.warning(f"No data returned for {full_symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.rename(columns={
            "adjusted_close": "adj_close",
        })

        # Set date as index for backtesting.py compatibility
        df = df.set_index("date")

        # Ensure numeric types
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Rename to title case for backtesting.py (requires Open, High, Low, Close, Volume)
        df = df.rename(columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        })

        logger.info(f"Fetched {len(df)} EOD records for {full_symbol}")
        return df

    async def fetch_intraday_data(
        self,
        symbol: str,
        exchange: str,
        interval: str = "1h",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Fetch intraday data.

        Args:
            symbol: Ticker symbol
            exchange: Exchange code
            interval: Time interval (1m, 5m, 1h)
            start_date: Start date
            end_date: End date

        Returns:
            DataFrame with columns: Datetime (index), Open, High, Low, Close, Volume
        """
        full_symbol = f"{symbol}.{exchange}"
        logger.info(f"Fetching intraday {interval} data for {full_symbol}")

        params = {"interval": interval}
        if start_date:
            params["from"] = int(start_date.timestamp())
        if end_date:
            params["to"] = int(end_date.timestamp())

        async with aiohttp.ClientSession() as session:
            data = await self._make_request(session, f"intraday/{full_symbol}", params)

        if not data:
            logger.warning(f"No intraday data returned for {full_symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")

        # Rename columns
        df = df.rename(columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        })

        # Ensure numeric types
        numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info(f"Fetched {len(df)} intraday records for {full_symbol}")
        return df

    async def fetch_fundamentals(
        self,
        symbol: str,
        exchange: str,
    ) -> Dict[str, Any]:
        """
        Fetch fundamental data.

        Args:
            symbol: Ticker symbol
            exchange: Exchange code

        Returns:
            Dictionary with fundamentals data
        """
        full_symbol = f"{symbol}.{exchange}"
        logger.info(f"Fetching fundamentals for {full_symbol}")

        async with aiohttp.ClientSession() as session:
            data = await self._make_request(session, f"fundamentals/{full_symbol}")

        logger.info(f"Fetched fundamentals for {full_symbol}")
        return data

    async def list_exchange_symbols(self, exchange: str) -> List[Dict[str, Any]]:
        """
        List all symbols for an exchange.

        Args:
            exchange: Exchange code (US, LSE, WSE, etc.)

        Returns:
            List of symbol information dictionaries
        """
        logger.info(f"Fetching symbols for exchange {exchange}")

        async with aiohttp.ClientSession() as session:
            data = await self._make_request(
                session, f"exchange-symbol-list/{exchange}"
            )

        logger.info(f"Fetched {len(data)} symbols for {exchange}")
        return data
