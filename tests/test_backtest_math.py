"""Test backtest math and invariants."""

import pytest
import pandas as pd
import numpy as np

from src.backtest.costs import TransactionCostModel
from src.portfolio.optimizer import PortfolioOptimizer, PortfolioConstraints


def test_transaction_costs_positive():
    """Test that transaction costs are always non-negative."""
    cost_model = TransactionCostModel(bps_per_side=5.0, spread_proxy_bps=3.0)

    # Create sample trades
    trades = pd.Series({
        "AAPL": 0.05,  # Buy 5%
        "MSFT": -0.03,  # Sell 3%
        "GOOGL": 0.02,  # Buy 2%
    })

    prices = pd.Series({
        "AAPL": 150,
        "MSFT": 250,
        "GOOGL": 100,
    })

    costs = cost_model.compute_costs(trades, prices, portfolio_value=1_000_000)

    # All costs should be positive
    assert (costs >= 0).all(), "Transaction costs should be non-negative"

    # Total cost should be positive
    total_cost = cost_model.compute_total_cost(trades, prices, portfolio_value=1_000_000)
    assert total_cost >= 0


def test_portfolio_weights_sum():
    """Test that portfolio weights respect constraints."""
    constraints = PortfolioConstraints(
        long_pct=0.20,
        short_pct=0.20,
        gross_leverage=2.0,
        net_exposure_target=0.0,
    )

    optimizer = PortfolioOptimizer(constraints, use_pca_risk=False)

    # Create sample scores
    n = 50
    scores = pd.Series(
        np.random.randn(n),
        index=[f"TICK{i}" for i in range(n)],
    )

    sectors = pd.Series(
        ["Tech"] * 25 + ["Finance"] * 25,
        index=scores.index,
    )

    prices = pd.Series(
        np.random.uniform(50, 200, n),
        index=scores.index,
    )

    weights = optimizer.optimize(scores, sectors, prices)

    if len(weights) > 0:
        # Check gross leverage
        gross_leverage = np.abs(weights).sum()
        assert gross_leverage <= constraints.gross_leverage * 1.01, \
            f"Gross leverage {gross_leverage} exceeds limit {constraints.gross_leverage}"

        # Check net exposure (market neutral)
        net_exposure = weights.sum()
        assert abs(net_exposure) <= 0.2, \
            f"Net exposure {net_exposure} too far from target {constraints.net_exposure_target}"

        # Check position limits
        max_position = weights.abs().max()
        assert max_position <= constraints.single_name_max_weight * 1.01, \
            f"Max position {max_position} exceeds limit {constraints.single_name_max_weight}"


def test_turnover_calculation():
    """Test turnover calculation correctness."""
    prev_weights = pd.Series({
        "AAPL": 0.05,
        "MSFT": 0.03,
        "GOOGL": -0.04,
    })

    new_weights = pd.Series({
        "AAPL": 0.04,  # Reduced
        "MSFT": 0.03,  # Same
        "GOOGL": -0.02,  # Reduced (less short)
        "TSLA": 0.02,  # New position
    })

    # Calculate turnover
    all_tickers = prev_weights.index.union(new_weights.index)
    prev = prev_weights.reindex(all_tickers).fillna(0)
    new = new_weights.reindex(all_tickers).fillna(0)

    turnover = (new - prev).abs().sum()

    # Expected turnover
    # AAPL: |0.04 - 0.05| = 0.01
    # MSFT: |0.03 - 0.03| = 0.00
    # GOOGL: |-0.02 - (-0.04)| = 0.02
    # TSLA: |0.02 - 0| = 0.02
    # Total: 0.05

    expected_turnover = 0.05
    assert abs(turnover - expected_turnover) < 1e-6, \
        f"Turnover {turnover} != expected {expected_turnover}"


def test_returns_calculation():
    """Test that portfolio returns are calculated correctly."""
    # Portfolio weights
    weights = pd.Series({
        "AAPL": 0.5,
        "MSFT": 0.5,
    })

    # Asset returns
    returns = pd.Series({
        "AAPL": 0.02,  # 2% return
        "MSFT": 0.01,  # 1% return
    })

    # Portfolio return
    portfolio_return = (weights * returns).sum()

    expected_return = 0.5 * 0.02 + 0.5 * 0.01
    assert abs(portfolio_return - expected_return) < 1e-6


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
