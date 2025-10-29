"""
Test FundamentalsOnDemand - On-demand snapshot ingestion.

Tests lazy fetching, caching, and coverage reporting.
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import date
import tempfile
import shutil
from unittest.mock import Mock, MagicMock, patch

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
        extra_lag_trading_days=2,
    )


@pytest.fixture
def mock_eodhd_client():
    """Create mock EODHD client."""
    client = Mock()
    return client


@pytest.fixture
def fetcher(pit_store, mock_eodhd_client):
    """Create FundamentalsOnDemand fetcher."""
    return FundamentalsOnDemand(
        pit_store=pit_store,
        eodhd_client=mock_eodhd_client,
        min_periods_required=4,
    )


def create_mock_fundamentals_payload():
    """Create mock EODHD fundamentals response."""
    return {
        "General": {
            "Code": "AAPL",
            "Type": "Common Stock",
            "Name": "Apple Inc",
        },
        "Highlights": {
            "MarketCapitalization": 2500000000000,
            "EBITDA": 120000000000,
        },
        "Financials": {
            "Income_Statement": {
                "quarterly": {
                    "2023-06-30": {
                        "date": "2023-06-30",
                        "filing_date": "2023-08-03",
                        "currency_symbol": "USD",
                        "totalRevenue": 100000000000,
                        "netIncome": 25000000000,
                    },
                    "2023-03-31": {
                        "date": "2023-03-31",
                        "filing_date": "2023-05-04",
                        "currency_symbol": "USD",
                        "totalRevenue": 95000000000,
                        "netIncome": 23000000000,
                    },
                    "2022-12-31": {
                        "date": "2022-12-31",
                        "filing_date": "2023-02-02",
                        "currency_symbol": "USD",
                        "totalRevenue": 92000000000,
                        "netIncome": 22000000000,
                    },
                    "2022-09-30": {
                        "date": "2022-09-30",
                        "filing_date": "2022-11-03",
                        "currency_symbol": "USD",
                        "totalRevenue": 90000000000,
                        "netIncome": 21000000000,
                    },
                },
                "yearly": {
                    "2022-09-30": {
                        "date": "2022-09-30",
                        "filing_date": "2022-11-03",
                        "currency_symbol": "USD",
                        "totalRevenue": 400000000000,
                        "netIncome": 100000000000,
                    },
                },
            },
        },
        "updatedAt": "2023-08-05",
    }


class TestBasicFetching:
    """Test basic ensure_snapshots functionality."""

    def test_ensure_snapshots_fetches_if_missing(self, fetcher, mock_eodhd_client):
        """Test that ensure_snapshots fetches when no local data exists."""
        # Mock API response
        mock_eodhd_client.get_full_fundamentals.return_value = create_mock_fundamentals_payload()

        # Ensure snapshots (should fetch)
        created = fetcher.ensure_snapshots("AAPL.US")

        # Verify API was called
        mock_eodhd_client.get_full_fundamentals.assert_called_once_with("AAPL.US")

        # Verify snapshots were created
        assert created > 0
        snapshots = fetcher.pit_store.load_snapshots("AAPL.US")
        assert len(snapshots) >= 4  # At least min_periods_required

    def test_ensure_snapshots_skips_if_sufficient(self, fetcher, mock_eodhd_client, pit_store):
        """Test that ensure_snapshots skips fetch if sufficient data exists."""
        # Pre-populate snapshots
        payload = {"Highlights": {"MarketCapitalization": 1e12}}
        for i in range(5):
            pit_store.append_snapshot(
                symbol="AAPL.US",
                payload=payload,
                period_end=date(2023, 3 * i + 3, 30),
                statement_kind="quarterly",
                reported_date=date(2023, 3 * i + 5, 3),
            )

        # Ensure snapshots (should NOT fetch)
        created = fetcher.ensure_snapshots("AAPL.US")

        # Verify API was NOT called
        mock_eodhd_client.get_full_fundamentals.assert_not_called()

        # Verify no new snapshots created
        assert created == 0

    def test_ensure_snapshots_force_refresh(self, fetcher, mock_eodhd_client, pit_store):
        """Test force_refresh flag bypasses cache."""
        # Pre-populate snapshots
        payload = {"Highlights": {"MarketCapitalization": 1e12}}
        for i in range(5):
            pit_store.append_snapshot(
                symbol="AAPL.US",
                payload=payload,
                period_end=date(2023, 3 * i + 3, 30),
                statement_kind="quarterly",
                reported_date=date(2023, 3 * i + 5, 3),
            )

        # Mock API response
        mock_eodhd_client.get_full_fundamentals.return_value = create_mock_fundamentals_payload()

        # Force refresh (should fetch even though data exists)
        created = fetcher.ensure_snapshots("AAPL.US", force_refresh=True)

        # Verify API WAS called
        mock_eodhd_client.get_full_fundamentals.assert_called_once()

    def test_ensure_snapshots_insufficient_data(self, fetcher, mock_eodhd_client, pit_store):
        """Test fetching when existing data is insufficient."""
        # Pre-populate with only 2 snapshots (< min_periods_required=4)
        payload = {"Highlights": {"MarketCapitalization": 1e12}}
        for i in range(2):
            pit_store.append_snapshot(
                symbol="AAPL.US",
                payload=payload,
                period_end=date(2023, 3 * i + 3, 30),
                statement_kind="quarterly",
                reported_date=date(2023, 3 * i + 5, 3),
            )

        # Mock API response
        mock_eodhd_client.get_full_fundamentals.return_value = create_mock_fundamentals_payload()

        # Ensure snapshots (should fetch because insufficient)
        created = fetcher.ensure_snapshots("AAPL.US")

        # Verify API was called
        mock_eodhd_client.get_full_fundamentals.assert_called_once()


class TestBulkOperations:
    """Test bulk fetching functionality."""

    def test_ensure_snapshots_bulk(self, fetcher, mock_eodhd_client):
        """Test bulk fetching for multiple symbols."""
        mock_eodhd_client.get_full_fundamentals.return_value = create_mock_fundamentals_payload()

        symbols = ["AAPL.US", "MSFT.US", "GOOGL.US"]

        results = fetcher.ensure_snapshots_bulk(symbols)

        # Verify all symbols processed (returns dict of symbol -> count)
        assert len(results) == 3
        assert all(symbol in results for symbol in symbols)

        # Verify API called for each
        assert mock_eodhd_client.get_full_fundamentals.call_count == 3

    def test_ensure_snapshots_bulk_partial_failure(self, fetcher, mock_eodhd_client):
        """Test bulk fetching with some failures."""
        def mock_get_fundamentals(symbol):
            if symbol == "FAIL.US":
                raise Exception("API Error")
            return create_mock_fundamentals_payload()

        mock_eodhd_client.get_full_fundamentals.side_effect = mock_get_fundamentals

        symbols = ["AAPL.US", "FAIL.US", "MSFT.US"]

        results = fetcher.ensure_snapshots_bulk(symbols)

        # Verify results (dict of symbol -> count)
        assert len(results) == 3

        # Check success (created > 0 snapshots)
        assert results["AAPL.US"] > 0

        # Check failure (created 0 snapshots due to exception)
        assert results["FAIL.US"] == 0

        # Check second success
        assert results["MSFT.US"] > 0


class TestCoverageReporting:
    """Test coverage report functionality."""

    def test_get_coverage_report_no_data(self, fetcher):
        """Test coverage report for symbols with no data."""
        symbols = ["MISSING1.US", "MISSING2.US"]

        coverage = fetcher.get_coverage_report(symbols)

        assert len(coverage) == 2
        assert (coverage["has_data"] == False).all()
        assert (coverage["count"] == 0).all()

    def test_get_coverage_report_with_data(self, fetcher, pit_store):
        """Test coverage report for symbols with data."""
        # Populate AAPL
        payload = {"Highlights": {"MarketCapitalization": 1e12}}
        for i in range(5):
            pit_store.append_snapshot(
                symbol="AAPL.US",
                payload=payload,
                period_end=date(2023, 3 * i + 3, 30),
                statement_kind="quarterly",
                reported_date=date(2023, 3 * i + 5, 3),
            )

        symbols = ["AAPL.US", "MISSING.US"]

        coverage = fetcher.get_coverage_report(symbols)

        assert len(coverage) == 2

        # AAPL has data
        aapl = coverage[coverage["symbol"] == "AAPL.US"].iloc[0]
        assert aapl["has_data"] is True
        assert aapl["count"] == 5

        # MISSING has no data
        missing = coverage[coverage["symbol"] == "MISSING.US"].iloc[0]
        assert missing["has_data"] is False
        assert missing["count"] == 0

    def test_get_coverage_report_date_ranges(self, fetcher, pit_store):
        """Test coverage report includes date ranges."""
        payload = {"Highlights": {"MarketCapitalization": 1e12}}

        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=payload,
            period_end=date(2023, 3, 31),
            statement_kind="quarterly",
            reported_date=date(2023, 5, 3),
        )

        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=payload,
            period_end=date(2023, 9, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 11, 2),
        )

        coverage = fetcher.get_coverage_report(["AAPL.US"])

        aapl = coverage.iloc[0]
        assert "min_effective_date" in aapl
        assert "max_effective_date" in aapl
        assert pd.notna(aapl["min_effective_date"])
        assert pd.notna(aapl["max_effective_date"])


class TestMaterialization:
    """Test snapshot materialization from EODHD payload."""

    def test_materialize_quarterly_periods(self, fetcher):
        """Test extracting quarterly periods."""
        payload = create_mock_fundamentals_payload()

        created = fetcher._materialize_snapshots("AAPL.US", payload)

        # Should create snapshots for all quarters in payload
        assert created >= 4

        snapshots = fetcher.pit_store.load_snapshots("AAPL.US")
        quarterly = [s for s in snapshots if s.statement_kind == "quarterly"]

        assert len(quarterly) >= 4

        # Check period_ends
        period_ends = [s.period_end for s in quarterly]
        assert date(2023, 6, 30) in period_ends
        assert date(2023, 3, 31) in period_ends

    def test_materialize_annual_periods(self, fetcher):
        """Test extracting annual periods."""
        payload = create_mock_fundamentals_payload()

        created = fetcher._materialize_snapshots("AAPL.US", payload)

        snapshots = fetcher.pit_store.load_snapshots("AAPL.US")
        annual = [s for s in snapshots if s.statement_kind == "annual"]

        assert len(annual) >= 1

    def test_materialize_uses_filing_date(self, fetcher):
        """Test that filing_date from payload is used as reported_date."""
        payload = create_mock_fundamentals_payload()

        fetcher._materialize_snapshots("AAPL.US", payload)

        snapshots = fetcher.pit_store.load_snapshots("AAPL.US")

        # Find Q2 2023 snapshot
        q2_snapshot = next(
            s for s in snapshots
            if s.period_end == date(2023, 6, 30) and s.statement_kind == "quarterly"
        )

        # Should use filing_date as reported_date
        assert q2_snapshot.reported_date == date(2023, 8, 3)

    def test_materialize_empty_payload(self, fetcher):
        """Test handling empty/minimal payload."""
        payload = {"General": {"Code": "TEST"}}

        created = fetcher._materialize_snapshots("TEST.US", payload)

        # Should handle gracefully (0 snapshots created)
        assert created == 0


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_ensure_snapshots_api_error(self, fetcher, mock_eodhd_client):
        """Test handling of API errors."""
        mock_eodhd_client.get_full_fundamentals.side_effect = Exception("API Error")

        # Should raise exception
        with pytest.raises(Exception, match="API Error"):
            fetcher.ensure_snapshots("FAIL.US")

    def test_ensure_snapshots_invalid_symbol(self, fetcher, mock_eodhd_client):
        """Test handling of invalid symbol format."""
        mock_eodhd_client.get_full_fundamentals.return_value = None

        # Should handle None response
        created = fetcher.ensure_snapshots("INVALID")

        assert created == 0

    def test_ensure_snapshots_malformed_payload(self, fetcher, mock_eodhd_client):
        """Test handling of malformed API response."""
        mock_eodhd_client.get_full_fundamentals.return_value = {
            "Financials": "invalid_structure"  # Wrong type
        }

        # Should handle gracefully without crashing
        created = fetcher.ensure_snapshots("BAD.US")

        # May create 0 snapshots due to malformed data
        assert created >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
