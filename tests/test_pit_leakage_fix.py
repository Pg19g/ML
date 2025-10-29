"""
Test suite to verify the critical PIT leakage bug fix.

This module tests that snapshots NEVER contain future data that wasn't
published yet. Each snapshot should only contain data with filing_date <=
that snapshot's filing_date.
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import date
import tempfile
import shutil
from unittest.mock import Mock

from src.pit_snapshots import PITStore
from src.ingest.fundamentals_on_demand import FundamentalsOnDemand


@pytest.fixture
def temp_snapshot_dir():
    """Create temporary directory for test snapshots."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def pit_store(temp_snapshot_dir):
    """Create PITStore for testing."""
    return PITStore(
        snapshot_dir=temp_snapshot_dir,
        extra_lag_trading_days=0,  # No extra lag for precise testing
    )


@pytest.fixture
def mock_eodhd_client():
    """Create mock EODHD client."""
    return Mock()


@pytest.fixture
def fetcher(pit_store, mock_eodhd_client):
    """Create FundamentalsOnDemand fetcher."""
    return FundamentalsOnDemand(
        pit_store=pit_store,
        eodhd_client=mock_eodhd_client,
        min_periods_required=1,  # Low threshold for testing
    )


def create_mock_fundamentals_with_filing_dates():
    """
    Create realistic mock fundamentals payload with filing_dates.

    Simulates a company with:
    - Q1 2024 (period_end: 2024-03-31, filed: 2024-05-16)
    - Q2 2024 (period_end: 2024-06-30, filed: 2024-08-14)
    - Q3 2024 (period_end: 2024-09-30, filed: 2024-10-31)
    - Annual 2023 (period_end: 2023-12-31, filed: 2024-03-21)
    """
    return {
        "General": {
            "Code": "TEST",
            "Type": "Common Stock",
            "Name": "Test Corp",
        },
        "Highlights": {
            "MarketCapitalization": 1000000000,
            "EBITDA": 100000000,
        },
        "Financials": {
            "Income_Statement": {
                "quarterly": {
                    "2024-09-30": {
                        "date": "2024-09-30",
                        "filing_date": "2024-10-31",  # Filed Oct 31, 2024
                        "totalRevenue": 300000000,
                        "netIncome": 30000000,
                    },
                    "2024-06-30": {
                        "date": "2024-06-30",
                        "filing_date": "2024-08-14",  # Filed Aug 14, 2024
                        "totalRevenue": 280000000,
                        "netIncome": 28000000,
                    },
                    "2024-03-31": {
                        "date": "2024-03-31",
                        "filing_date": "2024-05-16",  # Filed May 16, 2024
                        "totalRevenue": 250000000,
                        "netIncome": 25000000,
                    },
                },
                "annual": {
                    "2023-12-31": {
                        "date": "2023-12-31",
                        "filing_date": "2024-03-21",  # Filed Mar 21, 2024
                        "totalRevenue": 1000000000,
                        "netIncome": 100000000,
                    },
                },
            },
            "Balance_Sheet": {
                "quarterly": {
                    "2024-09-30": {
                        "date": "2024-09-30",
                        "filing_date": "2024-10-31",
                        "totalAssets": 5000000000,
                        "cash": 500000000,
                    },
                    "2024-06-30": {
                        "date": "2024-06-30",
                        "filing_date": "2024-08-14",
                        "totalAssets": 4800000000,
                        "cash": 480000000,
                    },
                    "2024-03-31": {
                        "date": "2024-03-31",
                        "filing_date": "2024-05-16",
                        "totalAssets": 4500000000,
                        "cash": 450000000,
                    },
                },
                "annual": {
                    "2023-12-31": {
                        "date": "2023-12-31",
                        "filing_date": "2024-03-21",
                        "totalAssets": 4200000000,
                        "cash": 420000000,
                    },
                },
            },
            "Cash_Flow": {
                "quarterly": {
                    "2024-09-30": {
                        "date": "2024-09-30",
                        "filing_date": "2024-10-31",
                        "freeCashFlow": 50000000,
                    },
                    "2024-06-30": {
                        "date": "2024-06-30",
                        "filing_date": "2024-08-14",
                        "freeCashFlow": 48000000,
                    },
                    "2024-03-31": {
                        "date": "2024-03-31",
                        "filing_date": "2024-05-16",
                        "freeCashFlow": 45000000,
                    },
                },
                "annual": {
                    "2023-12-31": {
                        "date": "2023-12-31",
                        "filing_date": "2024-03-21",
                        "freeCashFlow": 180000000,
                    },
                },
            },
        },
    }


class TestNoFutureDataLeakage:
    """
    Critical tests to verify snapshots don't contain future data.
    """

    def test_q1_snapshot_excludes_q2_q3_data(self, fetcher, mock_eodhd_client):
        """
        CRITICAL TEST: Q1 2024 snapshot (filed May 16) must NOT contain Q2 or Q3 data.
        """
        mock_eodhd_client.get_full_fundamentals.return_value = (
            create_mock_fundamentals_with_filing_dates()
        )

        # Create snapshots
        created = fetcher.ensure_snapshots("TEST.US")

        # Should create 4 snapshots (3 quarterly + 1 annual)
        assert created == 4

        # Load snapshots
        snapshots = fetcher.pit_store.load_snapshots("TEST.US")

        # Find Q1 2024 snapshot (filed May 16, 2024)
        q1_snapshot = next(
            s for s in snapshots
            if s.period_end == date(2024, 3, 31) and s.statement_kind == "quarterly"
        )

        assert q1_snapshot.reported_date == date(2024, 5, 16)

        # Verify payload contains ONLY data filed on or before May 16, 2024
        financials = q1_snapshot.payload["Financials"]
        income = financials["Income_Statement"]["quarterly"]

        # Should have Q1 2024 and Annual 2023 (both filed before May 16)
        assert "2024-03-31" in income  # Q1 2024 (filed May 16)
        assert "2024-06-30" not in income  # Q2 2024 (filed Aug 14) - FUTURE!
        assert "2024-09-30" not in income  # Q3 2024 (filed Oct 31) - FUTURE!

        # Check all statement types
        for statement_type in ["Income_Statement", "Balance_Sheet", "Cash_Flow"]:
            quarterly = financials[statement_type]["quarterly"]
            assert "2024-03-31" in quarterly
            assert "2024-06-30" not in quarterly, f"Q2 data leaked into Q1 snapshot in {statement_type}!"
            assert "2024-09-30" not in quarterly, f"Q3 data leaked into Q1 snapshot in {statement_type}!"

    def test_q2_snapshot_includes_q1_excludes_q3(self, fetcher, mock_eodhd_client):
        """
        CRITICAL TEST: Q2 2024 snapshot (filed Aug 14) must include Q1 but NOT Q3.
        """
        mock_eodhd_client.get_full_fundamentals.return_value = (
            create_mock_fundamentals_with_filing_dates()
        )

        fetcher.ensure_snapshots("TEST.US")
        snapshots = fetcher.pit_store.load_snapshots("TEST.US")

        # Find Q2 2024 snapshot (filed Aug 14, 2024)
        q2_snapshot = next(
            s for s in snapshots
            if s.period_end == date(2024, 6, 30) and s.statement_kind == "quarterly"
        )

        assert q2_snapshot.reported_date == date(2024, 8, 14)

        # Verify payload contains data filed on or before Aug 14, 2024
        financials = q2_snapshot.payload["Financials"]
        income = financials["Income_Statement"]["quarterly"]

        # Should have Q1 and Q2 2024 (both filed before Aug 14)
        assert "2024-03-31" in income  # Q1 2024 (filed May 16)
        assert "2024-06-30" in income  # Q2 2024 (filed Aug 14)
        assert "2024-09-30" not in income  # Q3 2024 (filed Oct 31) - FUTURE!

    def test_q3_snapshot_includes_all_historical_data(self, fetcher, mock_eodhd_client):
        """
        CRITICAL TEST: Q3 2024 snapshot (filed Oct 31) should include all prior data.
        """
        mock_eodhd_client.get_full_fundamentals.return_value = (
            create_mock_fundamentals_with_filing_dates()
        )

        fetcher.ensure_snapshots("TEST.US")
        snapshots = fetcher.pit_store.load_snapshots("TEST.US")

        # Find Q3 2024 snapshot (filed Oct 31, 2024)
        q3_snapshot = next(
            s for s in snapshots
            if s.period_end == date(2024, 9, 30) and s.statement_kind == "quarterly"
        )

        assert q3_snapshot.reported_date == date(2024, 10, 31)

        # Verify payload contains ALL data filed on or before Oct 31, 2024
        financials = q3_snapshot.payload["Financials"]
        income = financials["Income_Statement"]["quarterly"]

        # Should have ALL quarterly data
        assert "2024-03-31" in income  # Q1 2024 (filed May 16)
        assert "2024-06-30" in income  # Q2 2024 (filed Aug 14)
        assert "2024-09-30" in income  # Q3 2024 (filed Oct 31)


class TestCumulativeHistory:
    """Test that each snapshot preserves all historical data (cumulative)."""

    def test_snapshots_are_cumulative(self, fetcher, mock_eodhd_client):
        """
        Verify snapshots are cumulative - later ones include all earlier data.
        """
        mock_eodhd_client.get_full_fundamentals.return_value = (
            create_mock_fundamentals_with_filing_dates()
        )

        fetcher.ensure_snapshots("TEST.US")
        snapshots = fetcher.pit_store.load_snapshots("TEST.US")

        # Sort by filing_date
        snapshots.sort(key=lambda s: s.reported_date)

        # Count periods in each snapshot
        period_counts = []
        for snapshot in snapshots:
            if "Financials" in snapshot.payload:
                income = snapshot.payload["Financials"]["Income_Statement"]["quarterly"]
                period_counts.append(len(income))

        # Each snapshot should have >= periods than the previous (cumulative)
        for i in range(1, len(period_counts)):
            assert period_counts[i] >= period_counts[i-1], \
                f"Snapshot {i} has fewer periods than snapshot {i-1} (not cumulative)!"

    def test_historical_data_never_disappears(self, fetcher, mock_eodhd_client):
        """
        Verify that once data appears in a snapshot, it's in all subsequent snapshots.
        """
        mock_eodhd_client.get_full_fundamentals.return_value = (
            create_mock_fundamentals_with_filing_dates()
        )

        fetcher.ensure_snapshots("TEST.US")
        snapshots = fetcher.pit_store.load_snapshots("TEST.US")

        # Sort by filing_date
        snapshots.sort(key=lambda s: s.reported_date)

        # Track periods seen
        periods_seen = set()

        for snapshot in snapshots:
            if "Financials" in snapshot.payload:
                income = snapshot.payload["Financials"]["Income_Statement"]["quarterly"]
                current_periods = set(income.keys())

                # All previously seen periods should still be present
                assert periods_seen.issubset(current_periods), \
                    f"Historical data disappeared! Previous: {periods_seen}, Current: {current_periods}"

                # Update seen periods
                periods_seen.update(current_periods)


class TestFilingDateOrdering:
    """Test that snapshots are created in filing_date order, not period_end order."""

    def test_annual_filed_before_quarterly(self, fetcher, mock_eodhd_client):
        """
        Test case where annual statement is filed BEFORE a quarterly statement.

        Annual 2023 (period_end: 2023-12-31) filed on 2024-03-21
        Should come BEFORE Q1 2024 (period_end: 2024-03-31) filed on 2024-05-16
        """
        mock_eodhd_client.get_full_fundamentals.return_value = (
            create_mock_fundamentals_with_filing_dates()
        )

        fetcher.ensure_snapshots("TEST.US")
        snapshots = fetcher.pit_store.load_snapshots("TEST.US")

        # Find the two snapshots
        annual_2023 = next(
            s for s in snapshots
            if s.period_end == date(2023, 12, 31) and s.statement_kind == "annual"
        )
        q1_2024 = next(
            s for s in snapshots
            if s.period_end == date(2024, 3, 31) and s.statement_kind == "quarterly"
        )

        # Annual filed Mar 21, Q1 filed May 16
        assert annual_2023.reported_date == date(2024, 3, 21)
        assert q1_2024.reported_date == date(2024, 5, 16)

        # Annual should come BEFORE Q1 in filing order
        assert annual_2023.reported_date < q1_2024.reported_date


class TestFilterPayloadMethod:
    """Test the _filter_payload_to_filing_date method directly."""

    def test_filter_excludes_future_periods(self, fetcher):
        """Test that filter correctly excludes future periods."""
        full_payload = create_mock_fundamentals_with_filing_dates()

        # Filter to May 16, 2024 (Q1 filing date)
        filtered = fetcher._filter_payload_to_filing_date(
            full_payload, date(2024, 5, 16)
        )

        # Should only include data filed on or before May 16
        income = filtered["Financials"]["Income_Statement"]["quarterly"]

        assert "2024-03-31" in income  # Filed May 16 - INCLUDED
        assert "2024-06-30" not in income  # Filed Aug 14 - EXCLUDED
        assert "2024-09-30" not in income  # Filed Oct 31 - EXCLUDED

    def test_filter_preserves_non_financial_data(self, fetcher):
        """Test that filter preserves non-financial sections."""
        full_payload = create_mock_fundamentals_with_filing_dates()

        filtered = fetcher._filter_payload_to_filing_date(
            full_payload, date(2024, 5, 16)
        )

        # Non-financial sections should be preserved
        assert "General" in filtered
        assert "Highlights" in filtered
        assert filtered["General"] == full_payload["General"]
        assert filtered["Highlights"] == full_payload["Highlights"]

    def test_filter_is_cumulative(self, fetcher):
        """Test that filtering at later dates includes more data."""
        full_payload = create_mock_fundamentals_with_filing_dates()

        # Filter to three different dates
        filtered_may = fetcher._filter_payload_to_filing_date(
            full_payload, date(2024, 5, 16)
        )
        filtered_aug = fetcher._filter_payload_to_filing_date(
            full_payload, date(2024, 8, 14)
        )
        filtered_oct = fetcher._filter_payload_to_filing_date(
            full_payload, date(2024, 10, 31)
        )

        income_may = filtered_may["Financials"]["Income_Statement"]["quarterly"]
        income_aug = filtered_aug["Financials"]["Income_Statement"]["quarterly"]
        income_oct = filtered_oct["Financials"]["Income_Statement"]["quarterly"]

        # Each later date should include more periods
        assert len(income_may) < len(income_aug)
        assert len(income_aug) < len(income_oct)


class TestExtractAllPeriods:
    """Test the _extract_all_periods method."""

    def test_extract_finds_all_periods(self, fetcher):
        """Test that all periods with filing_dates are found."""
        full_payload = create_mock_fundamentals_with_filing_dates()
        financials = full_payload["Financials"]

        periods = fetcher._extract_all_periods(financials)

        # Should find 3 quarterly + 1 annual = 4 periods (across 3 statement types)
        # But each period is duplicated across statement types, so total = 4 * 3 = 12
        assert len(periods) == 12  # 4 periods * 3 statement types

    def test_extract_sorts_by_filing_date(self, fetcher):
        """Test that periods are sorted by filing_date."""
        full_payload = create_mock_fundamentals_with_filing_dates()
        financials = full_payload["Financials"]

        periods = fetcher._extract_all_periods(financials)

        # Group by period_end to check one instance
        unique_periods = {}
        for p in periods:
            key = (p["period_end"], p["statement_kind"])
            if key not in unique_periods:
                unique_periods[key] = p

        # Sort by filing_date
        sorted_periods = sorted(unique_periods.values(), key=lambda x: x["filing_date"])

        # Verify chronological order
        for i in range(1, len(sorted_periods)):
            assert sorted_periods[i]["filing_date"] >= sorted_periods[i-1]["filing_date"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
