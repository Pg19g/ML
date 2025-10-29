"""EODHD API client with caching and retry logic."""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

import pandas as pd
import requests
from loguru import logger


class EODHDClient:
    """
    Client for EODHD Historical Data API with caching and retry logic.

    Supports:
    - End-of-day prices (adjusted and unadjusted)
    - Fundamentals data (quarterly and annual)
    - Corporate actions (splits, dividends)
    - Exchange tickers list
    """

    BASE_URL = "https://eodhistoricaldata.com/api"

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_dir: str = "data/cache",
        retry_attempts: int = 3,
        retry_backoff: float = 2.0,
    ):
        """
        Initialize EODHD client.

        Args:
            api_key: EODHD API key (reads from EODHD_API_KEY env if not provided)
            cache_dir: Directory for caching responses
            retry_attempts: Number of retry attempts on failure
            retry_backoff: Exponential backoff multiplier
        """
        self.api_key = api_key or os.getenv("EODHD_API_KEY")
        if not self.api_key:
            raise ValueError(
                "EODHD API key not found. Set EODHD_API_KEY env variable or pass api_key."
            )

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.retry_attempts = retry_attempts
        self.retry_backoff = retry_backoff

        logger.info(f"Initialized EODHD client with cache at {self.cache_dir}")

    def _make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make API request with retry logic.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response object

        Raises:
            requests.RequestException: If all retries fail
        """
        if params is None:
            params = {}

        params["api_token"] = self.api_key
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response

            except requests.RequestException as e:
                wait_time = self.retry_backoff ** attempt
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}"
                )

                if attempt < self.retry_attempts - 1:
                    logger.info(f"Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All retry attempts failed for {endpoint}")
                    raise

    def get_eod_prices(
        self,
        ticker: str,
        exchange: str = "US",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch end-of-day prices for a ticker.

        Args:
            ticker: Ticker symbol
            exchange: Exchange code (e.g., 'US', 'LSE')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            use_cache: Whether to use cached data

        Returns:
            DataFrame with columns: date, open, high, low, close, adjusted_close, volume
        """
        symbol = f"{ticker}.{exchange}"
        cache_file = self.cache_dir / f"eod_{symbol}_{start_date}_{end_date}.parquet"

        # Check cache
        if use_cache and cache_file.exists():
            logger.debug(f"Loading cached prices for {symbol}")
            return pd.read_parquet(cache_file)

        # Fetch from API
        logger.info(f"Fetching EOD prices for {symbol}")
        params = {"fmt": "json"}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date

        try:
            response = self._make_request(f"eod/{symbol}", params)
            data = response.json()

            if not data:
                logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()

            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"])
            df["ticker"] = ticker
            df["exchange"] = exchange

            # Rename columns for consistency
            df = df.rename(columns={"adjusted_close": "adj_close"})

            # Ensure numeric types
            numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Cache result
            if use_cache:
                df.to_parquet(cache_file, index=False)

            return df

        except Exception as e:
            logger.error(f"Failed to fetch prices for {symbol}: {e}")
            return pd.DataFrame()

    def get_fundamentals(
        self,
        ticker: str,
        exchange: str = "US",
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Fetch fundamentals data for a ticker.

        Args:
            ticker: Ticker symbol
            exchange: Exchange code
            use_cache: Whether to use cached data

        Returns:
            Dictionary with fundamentals data
        """
        import json

        symbol = f"{ticker}.{exchange}"
        cache_file = self.cache_dir / f"fundamentals_{symbol}.json"

        # Check cache (refresh daily)
        if use_cache and cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(days=1):
                logger.debug(f"Loading cached fundamentals for {symbol}")
                with open(cache_file, 'r') as f:
                    return json.load(f)

        # Fetch from API
        logger.info(f"Fetching fundamentals for {symbol}")

        try:
            response = self._make_request(f"fundamentals/{symbol}", {"fmt": "json"})
            data = response.json()

            # Cache result as JSON (better for complex nested structures)
            if use_cache and data:
                with open(cache_file, 'w') as f:
                    json.dump(data, f, indent=2)

            return data

        except Exception as e:
            logger.error(f"Failed to fetch fundamentals for {symbol}: {e}")
            return {}

    def get_full_fundamentals(self, symbol: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Fetch complete fundamentals payload for a symbol.

        Alias for get_fundamentals that accepts full symbol notation (TICKER.EXCHANGE).
        No date filters - returns entire historical fundamentals dataset.

        Args:
            symbol: Full symbol (e.g., "AAPL.US", "BPN.WAR")
            use_cache: Whether to use cached data

        Returns:
            Complete fundamentals dictionary from EODHD
        """
        # Parse symbol
        if "." in symbol:
            ticker, exchange = symbol.rsplit(".", 1)
        else:
            ticker = symbol
            exchange = "US"  # Default

        return self.get_fundamentals(ticker, exchange, use_cache)

    def list_exchange_symbols(self, exchange: str, use_cache: bool = True) -> pd.DataFrame:
        """
        List all symbols for an exchange.

        Alias for get_exchange_tickers with consistent naming.

        Args:
            exchange: Exchange code (e.g., "US", "WAR", "LSE")
            use_cache: Whether to use cached data

        Returns:
            DataFrame with symbol information
        """
        return self.get_exchange_tickers(exchange, use_cache)

    def get_exchange_tickers(
        self, exchange: str = "US", use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Get list of tickers for an exchange.

        Args:
            exchange: Exchange code
            use_cache: Whether to use cached data

        Returns:
            DataFrame with ticker information
        """
        cache_file = self.cache_dir / f"tickers_{exchange}.parquet"

        # Check cache (refresh weekly)
        if use_cache and cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(days=7):
                logger.debug(f"Loading cached tickers for {exchange}")
                return pd.read_parquet(cache_file)

        # Fetch from API
        logger.info(f"Fetching tickers for exchange {exchange}")

        try:
            response = self._make_request(f"exchange-symbol-list/{exchange}", {"fmt": "json"})
            data = response.json()

            df = pd.DataFrame(data)
            df["exchange"] = exchange

            # Cache result
            if use_cache:
                df.to_parquet(cache_file, index=False)

            return df

        except Exception as e:
            logger.error(f"Failed to fetch tickers for {exchange}: {e}")
            return pd.DataFrame()

    def get_bulk_fundamentals(
        self,
        tickers: List[str],
        exchange: str = "US",
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch fundamentals for multiple tickers.

        Args:
            tickers: List of ticker symbols
            exchange: Exchange code
            use_cache: Whether to use cached data

        Returns:
            DataFrame with fundamentals for all tickers
        """
        results = []

        for ticker in tickers:
            fundamentals = self.get_fundamentals(ticker, exchange, use_cache)
            if fundamentals:
                fundamentals["ticker"] = ticker
                fundamentals["exchange"] = exchange
                results.append(fundamentals)

            # Rate limiting
            time.sleep(0.1)

        if not results:
            return pd.DataFrame()

        return pd.DataFrame(results)

    def parse_financials_to_pit(
        self, fundamentals: Dict[str, Any], ticker: str
    ) -> pd.DataFrame:
        """
        Parse fundamentals JSON into point-in-time records.

        Extracts quarterly and annual financials with filing dates.

        Args:
            fundamentals: Raw fundamentals dictionary from API
            ticker: Ticker symbol

        Returns:
            DataFrame with point-in-time financial records
        """
        records = []

        # Extract quarterly financials
        if "Financials" in fundamentals:
            financials = fundamentals["Financials"]

            # Quarterly data
            if "Balance_Sheet" in financials and "quarterly" in financials["Balance_Sheet"]:
                for date_str, data in financials["Balance_Sheet"]["quarterly"].items():
                    record = {
                        "ticker": ticker,
                        "filing_date": pd.to_datetime(data.get("filing_date", date_str)),
                        "date": pd.to_datetime(data.get("date", date_str)),
                        "period": "quarterly",
                        "period_end": pd.to_datetime(date_str),
                    }
                    # Add all numeric fields
                    for key, value in data.items():
                        if isinstance(value, (int, float)):
                            record[key] = value
                    records.append(record)

            # Annual data
            if "Balance_Sheet" in financials and "yearly" in financials["Balance_Sheet"]:
                for date_str, data in financials["Balance_Sheet"]["yearly"].items():
                    record = {
                        "ticker": ticker,
                        "filing_date": pd.to_datetime(data.get("filing_date", date_str)),
                        "date": pd.to_datetime(data.get("date", date_str)),
                        "period": "yearly",
                        "period_end": pd.to_datetime(date_str),
                    }
                    for key, value in data.items():
                        if isinstance(value, (int, float)):
                            record[key] = value
                    records.append(record)

        if not records:
            logger.warning(f"No financial records found for {ticker}")
            return pd.DataFrame()

        return pd.DataFrame(records)

    def clear_cache(self, pattern: str = "*") -> int:
        """
        Clear cached files matching pattern.

        Args:
            pattern: Glob pattern for files to delete

        Returns:
            Number of files deleted
        """
        files = list(self.cache_dir.glob(pattern))
        for file in files:
            file.unlink()

        logger.info(f"Cleared {len(files)} cached files")
        return len(files)
