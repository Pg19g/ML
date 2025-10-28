"""Test point-in-time data integrity."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.pit_store import PITDataStore
from src.utils.clock import TradingCalendar


def test_pit_lag_enforcement():
    """Test that fundamentals are not visible before effective_date."""
    # Create test store
    pit_store = PITDataStore(data_dir="data/test", pit_lag_days=2)

    # Create sample fundamentals
    fundamentals = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "filing_date": pd.to_datetime(["2023-01-10", "2023-01-15"]),
        "net_income_ttm": [100e9, 80e9],
        "revenue_ttm": [400e9, 300e9],
    })

    # Store fundamentals
    pit_store.store_fundamentals(fundamentals)

    # Query before effective date (should return empty)
    query_date = pd.Timestamp("2023-01-11")
    result = pit_store.get_fundamentals_asof(query_date, tickers=["AAPL"])

    assert result.empty, "Fundamentals should not be visible before effective_date"

    # Query after effective date (should return data)
    query_date = pd.Timestamp("2023-01-13")  # After filing + 2 days
    result = pit_store.get_fundamentals_asof(query_date, tickers=["AAPL"])

    assert not result.empty, "Fundamentals should be visible after effective_date"
    assert result.iloc[0]["ticker"] == "AAPL"

    # Cleanup
    pit_store.clear_all_data()


def test_pit_forward_fill():
    """Test that fundamentals forward-fill correctly."""
    pit_store = PITDataStore(data_dir="data/test", pit_lag_days=2)

    # Create fundamentals with different dates
    fundamentals = pd.DataFrame({
        "ticker": ["AAPL", "AAPL"],
        "filing_date": pd.to_datetime(["2023-01-10", "2023-04-10"]),
        "net_income_ttm": [100e9, 110e9],
    })

    pit_store.store_fundamentals(fundamentals)

    # Query between the two filings (should get first value)
    query_date = pd.Timestamp("2023-02-01")
    result = pit_store.get_fundamentals_asof(query_date, tickers=["AAPL"])

    assert not result.empty
    assert result.iloc[0]["net_income_ttm"] == 100e9

    # Query after second filing (should get updated value)
    query_date = pd.Timestamp("2023-05-01")
    result = pit_store.get_fundamentals_asof(query_date, tickers=["AAPL"])

    assert not result.empty
    assert result.iloc[0]["net_income_ttm"] == 110e9

    # Cleanup
    pit_store.clear_all_data()


def test_pit_validation():
    """Test PIT integrity validation."""
    pit_store = PITDataStore(data_dir="data/test", pit_lag_days=2)

    # Create valid fundamentals
    fundamentals = pd.DataFrame({
        "ticker": ["AAPL"],
        "filing_date": pd.to_datetime(["2023-01-10"]),
        "net_income_ttm": [100e9],
    })

    pit_store.store_fundamentals(fundamentals)

    # Validate
    validation = pit_store.validate_pit_integrity()

    assert validation["passed"], "Valid PIT data should pass validation"
    assert len(validation["errors"]) == 0

    # Cleanup
    pit_store.clear_all_data()


def test_merge_prices_fundamentals():
    """Test merging prices with PIT fundamentals."""
    pit_store = PITDataStore(data_dir="data/test", pit_lag_days=2)

    # Create prices
    prices = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-10", "2023-01-11", "2023-01-12", "2023-01-13"]),
        "ticker": ["AAPL"] * 4,
        "close": [150, 151, 152, 153],
        "adj_close": [150, 151, 152, 153],
        "volume": [1000000] * 4,
    })

    pit_store.store_prices(prices)

    # Create fundamentals
    fundamentals = pd.DataFrame({
        "ticker": ["AAPL"],
        "filing_date": pd.to_datetime(["2023-01-10"]),
        "net_income_ttm": [100e9],
    })

    pit_store.store_fundamentals(fundamentals)

    # Merge
    merged = pit_store.merge_prices_fundamentals(
        start_date="2023-01-10",
        end_date="2023-01-13",
        tickers=["AAPL"],
    )

    assert not merged.empty

    # Check that fundamentals are only visible after effective_date
    early_dates = merged[merged["date"] < pd.Timestamp("2023-01-13")]
    if not early_dates.empty:
        assert early_dates["net_income_ttm"].isna().all(), \
            "Fundamentals should not be visible before effective_date"

    # Cleanup
    pit_store.clear_all_data()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
