"""Test feature engineering."""

import pytest
import pandas as pd
import numpy as np

from src.features.factors import FactorEngine
from src.features.utils import zscore_by_group, winsorize


def test_zscore_sector_neutrality():
    """Test that sector-neutral z-scores sum to ~0 per sector."""
    # Create sample data
    df = pd.DataFrame({
        "ticker": [f"TICK{i}" for i in range(100)],
        "sector": ["Tech"] * 50 + ["Finance"] * 50,
        "value": np.random.randn(100) * 10 + 100,
    })

    # Compute z-scores
    zscores = zscore_by_group(df, "value", "sector")

    # Check sector means are close to 0
    df["zscore"] = zscores
    sector_means = df.groupby("sector")["zscore"].mean()

    for sector, mean in sector_means.items():
        assert abs(mean) < 1e-10, f"Sector {sector} z-score mean should be ~0, got {mean}"


def test_winsorization():
    """Test winsorization clips outliers."""
    values = pd.Series(np.concatenate([
        np.random.randn(100),
        [100, -100],  # Outliers
    ]))

    winsorized = winsorize(values, lower=0.01, upper=0.99)

    # Check outliers are clipped
    assert winsorized.max() < 100
    assert winsorized.min() > -100


def test_factor_engine_output_shape():
    """Test that factor engine produces expected outputs."""
    # Create sample data with fundamentals
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "sector": ["Tech", "Tech", "Tech"],
        "date": pd.Timestamp("2023-01-01"),
        "adj_close": [150, 250, 100],
        "close": [150, 250, 100],
        "volume": [1000000, 2000000, 1500000],
        "shares_outstanding": [16e9, 8e9, 12e9],
        "net_income_ttm": [100e9, 80e9, 70e9],
        "fcf_ttm": [90e9, 70e9, 60e9],
        "ebitda_ttm": [120e9, 100e9, 90e9],
        "total_assets": [350e9, 400e9, 380e9],
        "total_liabilities": [250e9, 300e9, 280e9],
    })

    factor_engine = FactorEngine(min_sector_size=1)

    result = factor_engine.compute_all_factors(df, compute_composite=True)

    # Check expected columns exist
    assert "earnings_yield" in result.columns
    assert "earnings_yield_z" in result.columns
    assert "composite_alpha" in result.columns

    # Check no infinities or all-NaN columns
    numeric_cols = result.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        assert not np.isinf(result[col]).any(), f"Column {col} contains infinities"


def test_sector_filtering():
    """Test that small sectors are filtered out."""
    df = pd.DataFrame({
        "ticker": [f"TICK{i}" for i in range(15)],
        "sector": ["Tech"] * 10 + ["SmallSector"] * 5,  # SmallSector has only 5 tickers
        "adj_close": [100] * 15,
        "close": [100] * 15,
        "volume": [1000000] * 15,
        "date": [pd.Timestamp("2023-01-01")] * 15,
        "shares_outstanding": [1e9] * 15,
        "net_income_ttm": [1e9] * 15,
    })

    factor_engine = FactorEngine(min_sector_size=8)

    result = factor_engine.compute_all_factors(df)

    # SmallSector should be filtered out
    assert "SmallSector" not in result["sector"].values
    assert len(result) == 10  # Only Tech sector remains


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
