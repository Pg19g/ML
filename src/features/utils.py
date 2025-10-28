"""Utility functions for feature engineering."""

from typing import Optional, Tuple

import pandas as pd
import numpy as np
from scipy import stats


def winsorize(
    series: pd.Series,
    lower: float = 0.01,
    upper: float = 0.99,
) -> pd.Series:
    """
    Winsorize a series at specified quantiles.

    Args:
        series: Input series
        lower: Lower quantile
        upper: Upper quantile

    Returns:
        Winsorized series
    """
    lower_bound = series.quantile(lower)
    upper_bound = series.quantile(upper)
    return series.clip(lower=lower_bound, upper=upper_bound)


def zscore_by_group(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
    winsorize_quantiles: Optional[Tuple[float, float]] = (0.01, 0.99),
) -> pd.Series:
    """
    Compute z-scores within groups (e.g., sectors).

    Args:
        df: Input DataFrame
        value_col: Column to standardize
        group_col: Grouping column (e.g., 'sector')
        winsorize_quantiles: Quantiles for winsorization (optional)

    Returns:
        Series of z-scores
    """
    result = pd.Series(index=df.index, dtype=float)

    for group, group_df in df.groupby(group_col):
        values = group_df[value_col].copy()

        # Winsorize if specified
        if winsorize_quantiles:
            values = winsorize(values, winsorize_quantiles[0], winsorize_quantiles[1])

        # Z-score
        mean = values.mean()
        std = values.std()

        if std > 0:
            zscores = (values - mean) / std
        else:
            zscores = 0.0

        result.loc[group_df.index] = zscores

    return result


def rank_by_group(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
    ascending: bool = True,
) -> pd.Series:
    """
    Rank values within groups.

    Args:
        df: Input DataFrame
        value_col: Column to rank
        group_col: Grouping column
        ascending: Rank in ascending order

    Returns:
        Series of ranks (normalized to [0, 1])
    """
    result = pd.Series(index=df.index, dtype=float)

    for group, group_df in df.groupby(group_col):
        values = group_df[value_col]

        # Rank and normalize to [0, 1]
        if ascending:
            ranks = values.rank(method="average", na_option="keep")
        else:
            ranks = values.rank(method="average", ascending=False, na_option="keep")

        n = ranks.notna().sum()
        if n > 0:
            ranks = (ranks - 1) / (n - 1) if n > 1 else 0.5

        result.loc[group_df.index] = ranks

    return result


def handle_missing_by_group(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
    method: str = "median",
) -> pd.Series:
    """
    Fill missing values using group statistics.

    Args:
        df: Input DataFrame
        value_col: Column with missing values
        group_col: Grouping column
        method: Fill method ('median', 'mean', 'zero')

    Returns:
        Series with filled values
    """
    result = df[value_col].copy()

    for group, group_df in df.groupby(group_col):
        mask = group_df[value_col].isna()

        if mask.any():
            if method == "median":
                fill_value = group_df[value_col].median()
            elif method == "mean":
                fill_value = group_df[value_col].mean()
            elif method == "zero":
                fill_value = 0.0
            else:
                fill_value = group_df[value_col].median()

            if pd.notna(fill_value):
                result.loc[mask] = fill_value

    return result


def calculate_ttm(
    df: pd.DataFrame,
    value_col: str,
    date_col: str = "date",
    ticker_col: str = "ticker",
    periods: int = 4,
) -> pd.Series:
    """
    Calculate trailing-twelve-months (TTM) sum for quarterly data.

    Args:
        df: Input DataFrame with quarterly data
        value_col: Column to sum
        date_col: Date column
        ticker_col: Ticker column
        periods: Number of periods to sum (default 4 for TTM)

    Returns:
        Series with TTM values
    """
    df = df.sort_values([ticker_col, date_col])

    ttm = df.groupby(ticker_col)[value_col].transform(
        lambda x: x.rolling(window=periods, min_periods=periods).sum()
    )

    return ttm


def calculate_rolling_stability(
    df: pd.DataFrame,
    value_col: str,
    date_col: str = "date",
    ticker_col: str = "ticker",
    periods: int = 12,
) -> pd.Series:
    """
    Calculate coefficient of variation as stability metric.

    Lower values indicate more stable metrics.

    Args:
        df: Input DataFrame
        value_col: Column to analyze
        date_col: Date column
        ticker_col: Ticker column
        periods: Lookback periods

    Returns:
        Series with stability scores (std/mean)
    """
    df = df.sort_values([ticker_col, date_col])

    def cv(x):
        if len(x) < 2:
            return np.nan
        mean = x.mean()
        if abs(mean) < 1e-6:
            return np.nan
        return x.std() / abs(mean)

    stability = df.groupby(ticker_col)[value_col].transform(
        lambda x: x.rolling(window=periods, min_periods=3).apply(cv, raw=False)
    )

    return stability


def calculate_growth_rate(
    df: pd.DataFrame,
    value_col: str,
    date_col: str = "date",
    ticker_col: str = "ticker",
    periods: int = 4,
) -> pd.Series:
    """
    Calculate growth rate over N periods.

    Args:
        df: Input DataFrame
        value_col: Column to analyze
        date_col: Date column
        ticker_col: Ticker column
        periods: Periods for growth calculation

    Returns:
        Series with growth rates
    """
    df = df.sort_values([ticker_col, date_col])

    growth = df.groupby(ticker_col)[value_col].transform(
        lambda x: x.pct_change(periods=periods)
    )

    return growth


def filter_min_sector_size(
    df: pd.DataFrame,
    sector_col: str = "sector",
    min_size: int = 10,
) -> pd.DataFrame:
    """
    Filter out small sectors.

    Args:
        df: Input DataFrame
        sector_col: Sector column
        min_size: Minimum number of tickers per sector

    Returns:
        Filtered DataFrame
    """
    sector_counts = df[sector_col].value_counts()
    valid_sectors = sector_counts[sector_counts >= min_size].index

    filtered = df[df[sector_col].isin(valid_sectors)].copy()

    dropped = len(df) - len(filtered)
    if dropped > 0:
        from loguru import logger
        logger.info(f"Filtered {dropped} rows from small sectors")

    return filtered


def calculate_percentile_by_group(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
) -> pd.Series:
    """
    Calculate percentile rank within groups.

    Args:
        df: Input DataFrame
        value_col: Column to rank
        group_col: Grouping column

    Returns:
        Series of percentile ranks [0, 100]
    """
    result = pd.Series(index=df.index, dtype=float)

    for group, group_df in df.groupby(group_col):
        values = group_df[value_col]
        percentiles = values.rank(pct=True, method="average") * 100
        result.loc[group_df.index] = percentiles

    return result
