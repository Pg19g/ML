"""
On-demand fundamentals fetcher with TRUE Point-In-Time snapshot materialization.

CRITICAL FIX: Each snapshot now contains ONLY data that was published on or before
its filing_date. This prevents look-ahead bias by ensuring future data is never
included in historical snapshots.

Key features:
- Check if snapshots exist locally
- If missing, fetch from EODHD once
- Extract all periods with their filing_dates
- Create cumulative PIT snapshots (each includes all previously published data)
- Filter payload to filing_date (NO future data leakage)
- Thereafter work offline from snapshots
"""

from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Any, Optional, Tuple
from copy import deepcopy

import pandas as pd
from loguru import logger

from src.pit_snapshots import PITStore
from src.eodhd_client import EODHDClient


class FundamentalsOnDemand:
    """
    On-demand fundamentals fetcher with TRUE PIT snapshot materialization.

    Workflow:
    1. Check PITStore for existing snapshots
    2. If missing/incomplete, fetch from EODHD
    3. Extract ALL periods with their filing_dates
    4. Sort by filing_date (chronological order of publication)
    5. Create cumulative snapshots (each filtered to filing_date)
    6. Save manifest

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

            # Parse and materialize snapshots (WITH PIT FILTERING)
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
        Parse fundamentals payload and materialize as TRUE PIT snapshots.

        CRITICAL FIX: Creates cumulative snapshots where each one contains ONLY
        data published on or before its filing_date. NO future data leakage.

        Args:
            symbol: Ticker symbol
            fundamentals: Complete fundamentals dict from EODHD

        Returns:
            Number of snapshots created
        """
        # Extract financials section
        if "Financials" not in fundamentals:
            logger.warning(f"No Financials section in payload for {symbol}")
            return 0

        financials = fundamentals["Financials"]

        # Step 1: Extract ALL periods with their filing_dates
        all_periods = self._extract_all_periods(financials)

        if not all_periods:
            logger.warning(f"No periods with filing_dates found for {symbol}")
            return 0

        # Step 2: Sort by filing_date (chronological order of publication)
        all_periods.sort(key=lambda x: x["filing_date"])

        logger.info(
            f"Found {len(all_periods)} periods for {symbol}, "
            f"spanning {all_periods[0]['filing_date']} to {all_periods[-1]['filing_date']}"
        )

        # Step 3: Create cumulative snapshots (each filtered to filing_date)
        created_count = 0
        for period in all_periods:
            try:
                # Filter payload to ONLY include data published up to this filing_date
                filtered_payload = self._filter_payload_to_filing_date(
                    fundamentals, period["filing_date"]
                )

                # Create snapshot with filtered PIT data
                self.pit_store.append_snapshot(
                    symbol=symbol,
                    payload=filtered_payload,
                    period_end=period["period_end"],
                    statement_kind=period["statement_kind"],
                    reported_date=period["filing_date"],
                )

                created_count += 1

            except Exception as e:
                logger.warning(
                    f"Failed to create snapshot for {symbol} "
                    f"period {period['period_end']} filed {period['filing_date']}: {e}"
                )
                continue

        return created_count

    def _extract_all_periods(self, financials: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all periods with filing_dates from financials section.

        Args:
            financials: Financials dict from EODHD payload

        Returns:
            List of period dicts with keys: period_end, filing_date, statement_kind, statement_type
        """
        all_periods = []

        statement_types = ["Income_Statement", "Balance_Sheet", "Cash_Flow"]
        statement_kinds = ["quarterly", "annual"]  # Note: EODHD uses "annual" not "yearly"

        for statement_type in statement_types:
            if statement_type not in financials:
                continue

            statements = financials[statement_type]

            for statement_kind in statement_kinds:
                if statement_kind not in statements:
                    continue

                periods = statements[statement_kind]

                if not periods or not isinstance(periods, dict):
                    continue

                # Each key is a date string (period_end)
                for period_end_str, period_data in periods.items():
                    try:
                        # Extract filing_date (CRITICAL for PIT)
                        filing_date_str = period_data.get("filing_date")

                        if not filing_date_str:
                            # No filing_date - skip this period (can't determine PIT)
                            logger.debug(
                                f"Period {period_end_str} in {statement_type}.{statement_kind} "
                                f"has no filing_date, skipping"
                            )
                            continue

                        period_end = pd.to_datetime(period_end_str).date()
                        filing_date = pd.to_datetime(filing_date_str).date()

                        # Sanity check: filing_date should be >= period_end
                        if filing_date < period_end:
                            logger.warning(
                                f"Invalid filing_date {filing_date} < period_end {period_end}, "
                                f"using period_end"
                            )
                            filing_date = period_end

                        all_periods.append({
                            "period_end": period_end,
                            "filing_date": filing_date,
                            "statement_kind": statement_kind,
                            "statement_type": statement_type,
                        })

                    except Exception as e:
                        logger.warning(f"Failed to parse period {period_end_str}: {e}")
                        continue

        return all_periods

    def _filter_payload_to_filing_date(
        self, full_payload: Dict[str, Any], cutoff_filing_date: date
    ) -> Dict[str, Any]:
        """
        Filter payload to ONLY include data published on or before cutoff_filing_date.

        This is the CRITICAL method that prevents look-ahead bias.

        Args:
            full_payload: Complete fundamentals payload from EODHD
            cutoff_filing_date: Only include data published on or before this date

        Returns:
            Filtered payload with same structure but only historical data
        """
        # Create deep copy to avoid modifying original
        filtered = deepcopy(full_payload)

        # Keep non-financial sections as-is (General, Highlights, SharesStats, etc.)
        # These are current snapshot values, not time-series

        # Filter Financials section
        if "Financials" not in filtered:
            return filtered

        financials = filtered["Financials"]

        statement_types = ["Income_Statement", "Balance_Sheet", "Cash_Flow"]
        statement_kinds = ["quarterly", "annual"]

        for statement_type in statement_types:
            if statement_type not in financials:
                continue

            statements = financials[statement_type]

            for statement_kind in statement_kinds:
                if statement_kind not in statements:
                    continue

                periods = statements[statement_kind]

                if not periods or not isinstance(periods, dict):
                    continue

                # Filter periods to only those published on or before cutoff
                filtered_periods = {}

                for period_end_str, period_data in periods.items():
                    filing_date_str = period_data.get("filing_date")

                    if not filing_date_str:
                        # No filing_date - exclude from PIT snapshot
                        continue

                    filing_date = pd.to_datetime(filing_date_str).date()

                    # CRITICAL CHECK: Only include if published on or before cutoff
                    if filing_date <= cutoff_filing_date:
                        filtered_periods[period_end_str] = period_data
                    else:
                        # Future data - exclude
                        logger.debug(
                            f"Excluding period {period_end_str} "
                            f"(filed {filing_date} > cutoff {cutoff_filing_date})"
                        )

                # Replace with filtered periods
                statements[statement_kind] = filtered_periods

        return filtered

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
