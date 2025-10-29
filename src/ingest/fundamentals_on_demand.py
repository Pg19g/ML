"""
On-demand fundamentals fetcher with PIT snapshot materialization.

Key features:
- Check if snapshots exist locally
- If missing, fetch from EODHD once
- Extract all periods (quarterly, annual, TTM)
- Materialize as PIT snapshots
- Thereafter work offline from snapshots
"""

from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Any, Optional

import pandas as pd
from loguru import logger

from src.pit_snapshots import PITStore
from src.eodhd_client import EODHDClient


class FundamentalsOnDemand:
    """
    On-demand fundamentals fetcher with snapshot materialization.

    Workflow:
    1. Check PITStore for existing snapshots
    2. If missing/incomplete, fetch from EODHD
    3. Parse all periods from API response
    4. Materialize as PIT snapshots with effective_date
    5. Save manifest

    Thereafter, all data comes from local snapshots (zero API calls).
    """

    def __init__(
        self,
        pit_store: PITStore,
        eodhd_client: EODHDClient,
        min_periods_required: int = 4,  # At least 4 quarters of data
    ):
        """
        Initialize on-demand fetcher.

        Args:
            pit_store: PIT snapshot store
            eodhd_client: EODHD API client
            min_periods_required: Minimum periods to consider complete
        """
        self.pit_store = pit_store
        self.eodhd_client = eodhd_client
        self.min_periods_required = min_periods_required

        logger.info("Initialized FundamentalsOnDemand")

    def ensure_snapshots(self, symbol: str, force_refresh: bool = False) -> int:
        """
        Ensure PIT snapshots exist for a symbol.

        Args:
            symbol: Ticker symbol (e.g., "AAPL.US", "BPN.WAR")
            force_refresh: Force re-fetch even if snapshots exist

        Returns:
            Number of snapshots created
        """
        # Check existing snapshots
        existing_snapshots = self.pit_store.load_snapshots(symbol)

        if not force_refresh and len(existing_snapshots) >= self.min_periods_required:
            logger.debug(
                f"Snapshots already exist for {symbol} ({len(existing_snapshots)} periods)"
            )
            return 0

        # Fetch fundamentals from EODHD
        logger.info(f"Fetching fundamentals for {symbol} from EODHD...")

        try:
            fundamentals = self.eodhd_client.get_full_fundamentals(symbol)

            if not fundamentals:
                logger.warning(f"No fundamentals returned for {symbol}")
                return 0

            # Parse and materialize snapshots
            created_count = self._materialize_snapshots(symbol, fundamentals)

            # Save manifest
            self.pit_store.save_manifest(symbol)

            logger.info(f"Created {created_count} snapshots for {symbol}")

            return created_count

        except Exception as e:
            logger.error(f"Failed to fetch/materialize fundamentals for {symbol}: {e}")
            return 0

    def _materialize_snapshots(
        self, symbol: str, fundamentals: Dict[str, Any]
    ) -> int:
        """
        Parse fundamentals payload and materialize as PIT snapshots.

        Args:
            symbol: Ticker symbol
            fundamentals: Complete fundamentals dict from EODHD

        Returns:
            Number of snapshots created
        """
        created_count = 0

        # Extract financials section
        if "Financials" not in fundamentals:
            logger.warning(f"No Financials section in payload for {symbol}")
            return 0

        financials = fundamentals["Financials"]

        # Process quarterly statements
        created_count += self._process_statement_block(
            symbol, financials, "quarterly", fundamentals
        )

        # Process annual statements
        created_count += self._process_statement_block(
            symbol, financials, "annual", fundamentals
        )

        return created_count

    def _process_statement_block(
        self,
        symbol: str,
        financials: Dict[str, Any],
        statement_kind: str,  # "quarterly" or "annual"
        full_payload: Dict[str, Any],
    ) -> int:
        """
        Process a block of statements (all quarters or all years).

        Returns:
            Number of snapshots created
        """
        created_count = 0

        # Look in Income_Statement, Balance_Sheet, Cash_Flow
        for statement_type in ["Income_Statement", "Balance_Sheet", "Cash_Flow"]:
            if statement_type not in financials:
                continue

            statements = financials[statement_type]

            if statement_kind not in statements:
                continue

            periods = statements[statement_kind]

            if not periods or not isinstance(periods, dict):
                continue

            # Each key is a date string (period_end)
            for period_end_str, period_data in periods.items():
                try:
                    period_end = pd.to_datetime(period_end_str).date()

                    # Try to find reported_date
                    reported_date = None
                    if "filing_date" in period_data:
                        reported_date = pd.to_datetime(period_data["filing_date"]).date()
                    elif "date" in period_data and period_data["date"] != period_end_str:
                        # Sometimes 'date' field contains filing date
                        try:
                            reported_date = pd.to_datetime(period_data["date"]).date()
                        except:
                            pass

                    # Create snapshot for this period
                    # Payload = the specific period data + full context
                    snapshot_payload = {
                        "statement_type": statement_type,
                        "period_data": period_data,
                        "full_payload": full_payload,  # Keep full context for flattening
                    }

                    self.pit_store.append_snapshot(
                        symbol=symbol,
                        payload=snapshot_payload,
                        period_end=period_end,
                        statement_kind=statement_kind,
                        reported_date=reported_date,
                    )

                    created_count += 1

                except Exception as e:
                    logger.warning(
                        f"Failed to process period {period_end_str} for {symbol}: {e}"
                    )
                    continue

        return created_count

    def ensure_snapshots_bulk(
        self, symbols: List[str], force_refresh: bool = False
    ) -> Dict[str, int]:
        """
        Ensure snapshots for multiple symbols.

        Args:
            symbols: List of symbols
            force_refresh: Force re-fetch

        Returns:
            Dict mapping symbol -> number of snapshots created
        """
        results = {}

        for i, symbol in enumerate(symbols):
            logger.info(f"Processing {i+1}/{len(symbols)}: {symbol}")

            created = self.ensure_snapshots(symbol, force_refresh)
            results[symbol] = created

        total_created = sum(results.values())
        logger.info(
            f"Completed bulk ingestion: {total_created} snapshots created "
            f"for {len(symbols)} symbols"
        )

        return results

    def get_coverage_report(self, symbols: List[str]) -> pd.DataFrame:
        """
        Get coverage report for symbols.

        Returns:
            DataFrame with columns: symbol, has_data, count, min_date, max_date
        """
        reports = []

        for symbol in symbols:
            manifest = self.pit_store.get_manifest(symbol)
            reports.append(manifest)

        return pd.DataFrame(reports)
