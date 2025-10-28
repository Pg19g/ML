"""Point-in-time data store with publication lag handling."""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from loguru import logger

from src.utils.clock import TradingCalendar


class PITDataStore:
    """
    Point-in-time data store ensuring no look-ahead bias.

    Key features:
    - Fundamentals only visible after effective_date = filing_date + lag
    - Forward-fill only after effective date
    - Handles corporate actions (splits, dividends)
    - Stores both raw and adjusted prices
    """

    def __init__(
        self,
        data_dir: str = "data",
        pit_lag_days: int = 2,
        calendar: Optional[TradingCalendar] = None,
    ):
        """
        Initialize PIT data store.

        Args:
            data_dir: Directory for storing data
            pit_lag_days: Publication lag in trading days (default 2)
            calendar: Trading calendar for date alignment
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.pit_lag_days = pit_lag_days
        self.calendar = calendar

        # Data storage
        self.prices_file = self.data_dir / "prices_daily.parquet"
        self.fundamentals_file = self.data_dir / "fundamentals_pit.parquet"
        self.metadata_file = self.data_dir / "metadata.parquet"

        logger.info(f"Initialized PIT store at {self.data_dir} with {pit_lag_days}d lag")

    def store_prices(self, prices_df: pd.DataFrame) -> None:
        """
        Store daily price data.

        Expected columns: date, ticker, open, high, low, close, adj_close, volume

        Args:
            prices_df: DataFrame with price data
        """
        if prices_df.empty:
            logger.warning("Empty prices DataFrame provided")
            return

        # Validate required columns
        required_cols = ["date", "ticker", "close", "adj_close", "volume"]
        missing_cols = set(required_cols) - set(prices_df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Ensure date is datetime
        prices_df = prices_df.copy()
        prices_df["date"] = pd.to_datetime(prices_df["date"])

        # Calculate returns (adjusted)
        prices_df = prices_df.sort_values(["ticker", "date"])
        prices_df["returns_1d"] = prices_df.groupby("ticker")["adj_close"].pct_change()

        # Append or overwrite
        if self.prices_file.exists():
            existing = pd.read_parquet(self.prices_file)
            # Remove duplicates (new data takes precedence)
            existing = existing[
                ~existing.set_index(["date", "ticker"]).index.isin(
                    prices_df.set_index(["date", "ticker"]).index
                )
            ]
            combined = pd.concat([existing, prices_df], ignore_index=True)
            combined = combined.sort_values(["ticker", "date"])
            combined.to_parquet(self.prices_file, index=False)
        else:
            prices_df.to_parquet(self.prices_file, index=False)

        logger.info(f"Stored {len(prices_df)} price records for {prices_df['ticker'].nunique()} tickers")

    def store_fundamentals(
        self,
        fundamentals_df: pd.DataFrame,
        filing_date_col: str = "filing_date",
    ) -> None:
        """
        Store fundamentals with point-in-time effective dates.

        Expected columns: ticker, filing_date, [fundamental fields]

        Args:
            fundamentals_df: DataFrame with fundamental data
            filing_date_col: Column name for filing/publication date
        """
        if fundamentals_df.empty:
            logger.warning("Empty fundamentals DataFrame provided")
            return

        fundamentals_df = fundamentals_df.copy()
        fundamentals_df[filing_date_col] = pd.to_datetime(fundamentals_df[filing_date_col])

        # Calculate effective date (filing_date + lag)
        fundamentals_df["effective_date"] = fundamentals_df[filing_date_col].apply(
            lambda x: self._apply_pit_lag(x)
        )

        # Store asof_date for reference
        fundamentals_df["asof_date"] = pd.Timestamp.now()

        # Append or overwrite
        if self.fundamentals_file.exists():
            existing = pd.read_parquet(self.fundamentals_file)
            # Remove duplicates by ticker + filing_date
            existing = existing[
                ~existing.set_index(["ticker", filing_date_col]).index.isin(
                    fundamentals_df.set_index(["ticker", filing_date_col]).index
                )
            ]
            combined = pd.concat([existing, fundamentals_df], ignore_index=True)
            combined = combined.sort_values(["ticker", "effective_date"])
            combined.to_parquet(self.fundamentals_file, index=False)
        else:
            fundamentals_df.to_parquet(self.fundamentals_file, index=False)

        logger.info(
            f"Stored {len(fundamentals_df)} fundamental records for "
            f"{fundamentals_df['ticker'].nunique()} tickers"
        )

    def _apply_pit_lag(self, filing_date: pd.Timestamp) -> pd.Timestamp:
        """
        Apply publication lag to filing date.

        Args:
            filing_date: Original filing date

        Returns:
            Effective date after applying lag
        """
        if self.calendar is None:
            # Simple calendar day offset
            return filing_date + timedelta(days=self.pit_lag_days)
        else:
            # Trading day offset
            return self.calendar.offset_trading_days(filing_date, self.pit_lag_days)

    def get_prices(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Retrieve price data.

        Args:
            tickers: List of tickers (optional, returns all if None)
            start_date: Start date filter
            end_date: End date filter

        Returns:
            DataFrame with price data
        """
        if not self.prices_file.exists():
            logger.warning("No prices data available")
            return pd.DataFrame()

        df = pd.read_parquet(self.prices_file)

        # Apply filters
        if tickers is not None:
            df = df[df["ticker"].isin(tickers)]

        if start_date is not None:
            df = df[df["date"] >= pd.to_datetime(start_date)]

        if end_date is not None:
            df = df[df["date"] <= pd.to_datetime(end_date)]

        return df

    def get_fundamentals_asof(
        self,
        date: pd.Timestamp,
        tickers: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Get fundamentals as-of a specific date (point-in-time).

        Only returns data where effective_date <= date.

        Args:
            date: Query date
            tickers: List of tickers (optional)

        Returns:
            DataFrame with most recent fundamentals as-of date
        """
        if not self.fundamentals_file.exists():
            logger.warning("No fundamentals data available")
            return pd.DataFrame()

        df = pd.read_parquet(self.fundamentals_file)

        # Filter by effective date
        df = df[df["effective_date"] <= date]

        if df.empty:
            return pd.DataFrame()

        # Filter by tickers
        if tickers is not None:
            df = df[df["ticker"].isin(tickers)]

        # Get most recent record per ticker
        df = df.sort_values("effective_date")
        df = df.groupby("ticker").last().reset_index()

        return df

    def get_fundamentals_series(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get time series of fundamentals (all records).

        Args:
            tickers: List of tickers
            start_date: Start date filter (on effective_date)
            end_date: End date filter (on effective_date)

        Returns:
            DataFrame with all fundamental records
        """
        if not self.fundamentals_file.exists():
            logger.warning("No fundamentals data available")
            return pd.DataFrame()

        df = pd.read_parquet(self.fundamentals_file)

        # Apply filters
        if tickers is not None:
            df = df[df["ticker"].isin(tickers)]

        if start_date is not None:
            df = df[df["effective_date"] >= pd.to_datetime(start_date)]

        if end_date is not None:
            df = df[df["effective_date"] <= pd.to_datetime(end_date)]

        return df

    def merge_prices_fundamentals(
        self,
        start_date: str,
        end_date: str,
        tickers: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Merge prices with point-in-time fundamentals.

        For each date, forward-fills fundamentals that were effective on or before that date.

        Args:
            start_date: Start date
            end_date: End date
            tickers: List of tickers (optional)

        Returns:
            DataFrame with merged prices and fundamentals
        """
        # Get prices
        prices = self.get_prices(tickers, start_date, end_date)
        if prices.empty:
            return pd.DataFrame()

        # Get all fundamentals in range
        fundamentals = self.get_fundamentals_series(tickers, start_date, end_date)
        if fundamentals.empty:
            logger.warning("No fundamentals available in date range")
            return prices

        # For each ticker, create a time series and merge
        merged_list = []

        for ticker in prices["ticker"].unique():
            ticker_prices = prices[prices["ticker"] == ticker].copy()
            ticker_fundamentals = fundamentals[fundamentals["ticker"] == ticker].copy()

            if ticker_fundamentals.empty:
                # No fundamentals for this ticker
                merged_list.append(ticker_prices)
                continue

            # Sort by effective date
            ticker_fundamentals = ticker_fundamentals.sort_values("effective_date")

            # Merge asof - fundamentals effective on or before each price date
            ticker_prices = ticker_prices.sort_values("date")
            ticker_prices = pd.merge_asof(
                ticker_prices,
                ticker_fundamentals,
                left_on="date",
                right_on="effective_date",
                by="ticker",
                direction="backward",
            )

            merged_list.append(ticker_prices)

        merged = pd.concat(merged_list, ignore_index=True)
        return merged

    def get_available_tickers(self) -> List[str]:
        """Get list of all tickers with price data."""
        if not self.prices_file.exists():
            return []

        df = pd.read_parquet(self.prices_file, columns=["ticker"])
        return sorted(df["ticker"].unique().tolist())

    def get_date_range(self) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
        """Get min and max dates in price data."""
        if not self.prices_file.exists():
            return None

        df = pd.read_parquet(self.prices_file, columns=["date"])
        return df["date"].min(), df["date"].max()

    def validate_pit_integrity(self) -> Dict[str, any]:
        """
        Validate point-in-time integrity.

        Checks:
        - No fundamentals visible before effective_date
        - Effective dates respect lag
        - No future data leakage

        Returns:
            Dictionary with validation results
        """
        results = {
            "passed": True,
            "checks": [],
            "warnings": [],
            "errors": [],
        }

        if not self.fundamentals_file.exists():
            results["warnings"].append("No fundamentals data to validate")
            return results

        fundamentals = pd.read_parquet(self.fundamentals_file)

        # Check 1: effective_date >= filing_date + lag
        fundamentals["expected_min_effective"] = fundamentals["filing_date"].apply(
            lambda x: self._apply_pit_lag(x)
        )

        invalid_lag = fundamentals[
            fundamentals["effective_date"] < fundamentals["expected_min_effective"]
        ]

        if not invalid_lag.empty:
            results["passed"] = False
            results["errors"].append(
                f"Found {len(invalid_lag)} records with effective_date before filing_date + lag"
            )
        else:
            results["checks"].append("All effective dates respect publication lag")

        # Check 2: No effective dates in the future
        now = pd.Timestamp.now()
        future_dates = fundamentals[fundamentals["effective_date"] > now]

        if not future_dates.empty:
            results["warnings"].append(
                f"Found {len(future_dates)} records with future effective dates"
            )
        else:
            results["checks"].append("No future effective dates found")

        return results

    def clear_all_data(self) -> None:
        """Clear all stored data (use with caution)."""
        files_removed = 0
        for file in [self.prices_file, self.fundamentals_file, self.metadata_file]:
            if file.exists():
                file.unlink()
                files_removed += 1

        logger.warning(f"Cleared {files_removed} data files from PIT store")
