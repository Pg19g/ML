"""
Data loading module that integrates PIT prices and fundamentals.

Combines:
- Prices from PITDataStore (old system, still works)
- Fundamentals from PITStore (new snapshot system)
"""

from typing import List, Optional
from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger

from src.pit_store import PITDataStore
from src.pit_snapshots import PITStore


class DataLoader:
    """
    Unified data loader for prices and fundamentals.

    Uses:
    - PITDataStore for prices (legacy, but works)
    - PITStore for fundamentals (new snapshot system with zero leakage)
    """

    def __init__(
        self,
        data_dir: Path | str = "data",
        pit_snapshot_dir: Path | str = "data/pit",
        pit_lag_days: int = 2,
    ):
        """
        Initialize data loader.

        Args:
            data_dir: Data directory for prices (PITDataStore)
            pit_snapshot_dir: Snapshot directory for fundamentals (PITStore)
            pit_lag_days: Lag days for both systems (should match)
        """
        self.data_dir = Path(data_dir)
        self.pit_snapshot_dir = Path(pit_snapshot_dir)

        # Legacy PITDataStore for prices
        self.price_store = PITDataStore(
            data_dir=str(data_dir),
            pit_lag_days=pit_lag_days,
        )

        # New PITStore for fundamentals
        self.fundamental_store = PITStore(
            snapshot_dir=pit_snapshot_dir,
            extra_lag_trading_days=pit_lag_days,
        )

        logger.info(
            f"Initialized DataLoader (prices: {data_dir}, "
            f"fundamentals: {pit_snapshot_dir})"
        )

    def load_prices(
        self,
        start: date | str,
        end: date | str,
        tickers: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Load prices using legacy PITDataStore.

        Args:
            start: Start date
            end: End date
            tickers: Optional list of tickers to filter

        Returns:
            DataFrame with prices (date, ticker, open, high, low, close, adj_close, volume)
        """
        logger.info(f"Loading prices from {start} to {end}")

        df = self.price_store.get_prices_asof(
            query_date=end,  # Get all prices up to end date
            start_date=start,
            tickers=tickers,
        )

        if df.empty:
            logger.warning("No price data loaded")
            return pd.DataFrame()

        # Filter date range
        df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]

        logger.info(
            f"Loaded {len(df)} price records for {df['ticker'].nunique()} tickers"
        )

        return df

    def load_fundamentals_pit(
        self,
        symbols: List[str],
        start: date | str,
        end: date | str,
    ) -> pd.DataFrame:
        """
        Load fundamentals using new PIT snapshot system.

        Args:
            symbols: List of symbols (e.g., ["AAPL.US", "MSFT.US"])
            start: Start date
            end: End date

        Returns:
            DataFrame with PIT fundamentals
        """
        logger.info(f"Building PIT fundamentals panel for {len(symbols)} symbols")

        panel = self.fundamental_store.build_panel(
            symbols=symbols,
            start=pd.to_datetime(start).date(),
            end=pd.to_datetime(end).date(),
        )

        if panel.empty:
            logger.warning("Empty PIT fundamentals panel")
            return pd.DataFrame()

        # Validate PIT integrity
        try:
            self.fundamental_store.validate_pit_integrity(panel)
            logger.info("✓ PIT integrity validated - no information leakage detected")
        except AssertionError as e:
            logger.error(f"PIT integrity violation: {e}")
            raise

        logger.info(
            f"Loaded {len(panel)} fundamental records "
            f"({panel['symbol'].nunique()} symbols, {panel['date'].nunique()} dates)"
        )

        return panel

    def merge_prices_fundamentals(
        self,
        start: date | str,
        end: date | str,
        tickers: Optional[List[str]] = None,
        symbols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Load and merge prices with PIT fundamentals.

        Args:
            start: Start date
            end: End date
            tickers: Tickers for prices (e.g., ["AAPL", "MSFT"])
            symbols: Symbols for fundamentals (e.g., ["AAPL.US", "MSFT.US"])
                    If not provided, will be derived from tickers

        Returns:
            Merged DataFrame with prices and fundamentals
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        # Load prices
        prices = self.load_prices(start, end, tickers)

        if prices.empty:
            logger.warning("No prices loaded - returning empty DataFrame")
            return pd.DataFrame()

        # Determine symbols for fundamentals
        if symbols is None:
            # Derive from tickers (assume .US exchange if not specified)
            unique_tickers = prices["ticker"].unique()
            symbols = [f"{t}.US" if "." not in t else t for t in unique_tickers]

        # Load fundamentals
        fundamentals = self.load_fundamentals_pit(symbols, start, end)

        if fundamentals.empty:
            logger.warning("No fundamentals loaded - returning prices only")
            return prices

        # Prepare fundamentals for merge
        # Map symbol -> ticker (remove exchange suffix for merge)
        fundamentals["ticker"] = fundamentals["symbol"].str.split(".").str[0]

        # Select columns to merge (avoid duplicates)
        fundamental_cols = [
            "date",
            "ticker",
            "effective_date",
            "statement_kind",
            "period_end",
            "pit_source",
            # Fundamental fields
            "market_cap",
            "shares_outstanding",
            "ebitda_ttm",
            "pe_ratio",
            "revenue_ttm",
            "gross_profit_ttm",
            "enterprise_value",
            "ev_to_ebitda",
            "net_income",
            "total_revenue",
            "operating_income",
            "gross_profit",
            "total_assets",
            "total_liabilities",
            "total_stockholder_equity",
            "cash",
            "free_cash_flow",
            "operating_cash_flow",
        ]

        # Filter to available columns
        available_cols = [col for col in fundamental_cols if col in fundamentals.columns]
        fundamentals_subset = fundamentals[available_cols].copy()

        # Merge on date and ticker
        merged = pd.merge(
            prices,
            fundamentals_subset,
            on=["date", "ticker"],
            how="left",
        )

        logger.info(
            f"Merged DataFrame: {len(merged)} rows, "
            f"{merged['ticker'].nunique()} tickers"
        )

        # Log coverage
        has_fundamentals = merged["market_cap"].notna().sum()
        coverage_pct = 100 * has_fundamentals / len(merged)
        logger.info(f"Fundamentals coverage: {coverage_pct:.1f}%")

        return merged

    def validate_pit_integrity_end_to_end(
        self,
        merged_df: pd.DataFrame,
    ) -> bool:
        """
        Validate PIT integrity for merged prices+fundamentals DataFrame.

        Checks that effective_date <= date for all rows with fundamentals.

        Args:
            merged_df: Merged DataFrame from merge_prices_fundamentals()

        Returns:
            True if validation passes

        Raises:
            AssertionError if leakage detected
        """
        if "effective_date" not in merged_df.columns:
            logger.warning("No effective_date column - skipping PIT validation")
            return True

        # Filter to rows with fundamentals
        with_fundamentals = merged_df[merged_df["effective_date"].notna()].copy()

        if with_fundamentals.empty:
            logger.info("No fundamentals to validate")
            return True

        # Check effective_date <= date
        violations = with_fundamentals[
            with_fundamentals["effective_date"] > with_fundamentals["date"]
        ]

        if not violations.empty:
            logger.error(
                f"PIT INTEGRITY VIOLATION: {len(violations)} rows with "
                f"effective_date > date"
            )
            logger.error(f"Sample violations:\n{violations.head()}")
            raise AssertionError(
                f"PIT integrity violated: {len(violations)} rows have "
                f"effective_date > date (information leakage detected)"
            )

        logger.info(
            f"✓ PIT integrity validated: {len(with_fundamentals)} rows, "
            f"no leakage detected"
        )
        return True
