"""Test for data leakage in train/test splits."""

import pytest
import pandas as pd
import numpy as np

from src.models.train import WalkForwardCV


def test_walk_forward_no_overlap():
    """Test that walk-forward CV has no overlap between train and test."""
    # Create sample data
    dates = pd.date_range("2020-01-01", "2023-01-01", freq="D")
    df = pd.DataFrame({
        "date": dates,
        "value": np.random.randn(len(dates)),
    })

    # Initialize CV
    cv = WalkForwardCV(
        train_period_days=252,
        test_period_days=63,
        purge_days=21,
        embargo_days=21,
        date_col="date",
    )

    # Generate folds
    folds = cv.generate_folds(df)

    assert len(folds) > 0, "Should generate at least one fold"

    # Check each fold for overlap
    for fold in folds:
        # Train should end before test starts (with purge gap)
        train_end = fold.train_end
        test_start = fold.test_start

        # Calculate days between
        days_between = (test_start - train_end).days

        assert days_between >= fold.purge_days, \
            f"Insufficient purge between train and test: {days_between} < {fold.purge_days}"


def test_walk_forward_embargo():
    """Test that embargo period is respected."""
    dates = pd.date_range("2020-01-01", "2023-01-01", freq="D")
    df = pd.DataFrame({
        "date": dates,
        "value": np.random.randn(len(dates)),
    })

    cv = WalkForwardCV(
        train_period_days=252,
        test_period_days=63,
        purge_days=21,
        embargo_days=21,
        date_col="date",
    )

    folds = cv.generate_folds(df)

    # Check consecutive folds
    for i in range(len(folds) - 1):
        current_fold = folds[i]
        next_fold = folds[i + 1]

        # Next fold's training should start after current fold's test end + embargo
        gap = (next_fold.train_start - current_fold.test_end).days

        assert gap >= current_fold.embargo_days, \
            f"Insufficient embargo: {gap} < {current_fold.embargo_days}"


def test_split_preserves_dates():
    """Test that split correctly separates train and test dates."""
    dates = pd.date_range("2020-01-01", "2021-01-01", freq="D")
    df = pd.DataFrame({
        "date": dates,
        "ticker": ["AAPL"] * len(dates),
        "value": np.random.randn(len(dates)),
    })

    cv = WalkForwardCV(
        train_period_days=180,
        test_period_days=30,
        purge_days=10,
        embargo_days=10,
        date_col="date",
    )

    folds = cv.generate_folds(df)

    for fold in folds:
        train_df, test_df = cv.split(df, fold)

        # Check dates are in correct ranges
        assert train_df["date"].min() >= fold.train_start
        assert train_df["date"].max() <= fold.train_end

        assert test_df["date"].min() >= fold.test_start
        assert test_df["date"].max() <= fold.test_end

        # Check no overlap
        train_dates = set(train_df["date"])
        test_dates = set(test_df["date"])

        assert len(train_dates & test_dates) == 0, "Train and test dates should not overlap"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
