"""Backtest runner for strategy evaluation."""

from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import numpy as np
from loguru import logger

from src.backtest.costs import TransactionCostModel
from src.utils.clock import TradingCalendar


@dataclass
class BacktestResult:
    """Container for backtest results."""
    portfolio_values: pd.Series
    returns: pd.Series
    weights_history: pd.DataFrame
    trades_history: pd.DataFrame
    costs_history: pd.Series
    metrics: Dict[str, float]
    daily_stats: pd.DataFrame = field(default_factory=pd.DataFrame)


class Backtester:
    """
    Event-driven backtester with realistic cost modeling.

    Features:
    - Rebalance on specified dates
    - Transaction cost modeling
    - Position tracking
    - Performance metrics
    """

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        cost_model: Optional[TransactionCostModel] = None,
        calendar: Optional[TradingCalendar] = None,
    ):
        """
        Initialize backtester.

        Args:
            initial_capital: Starting capital
            cost_model: Transaction cost model
            calendar: Trading calendar
        """
        self.initial_capital = initial_capital
        self.cost_model = cost_model or TransactionCostModel()
        self.calendar = calendar

        logger.info(f"Initialized Backtester with ${initial_capital:,.0f} initial capital")

    def run(
        self,
        rebalance_dates: List[pd.Timestamp],
        weight_func: Callable[[pd.Timestamp], pd.Series],
        price_data: pd.DataFrame,
        execution_lag: int = 1,
    ) -> BacktestResult:
        """
        Run backtest.

        Args:
            rebalance_dates: List of rebalance dates
            weight_func: Function that returns target weights for a date
            price_data: DataFrame with columns [date, ticker, adj_close, volume]
            execution_lag: Days between signal and execution (default 1 = next day open)

        Returns:
            BacktestResult
        """
        logger.info(f"Running backtest over {len(rebalance_dates)} rebalance periods")

        # Initialize tracking
        portfolio_value = self.initial_capital
        current_weights = pd.Series(dtype=float)

        portfolio_values = []
        returns_list = []
        weights_history = []
        trades_history = []
        costs_history = []

        # Get all trading dates in price data
        all_dates = sorted(price_data["date"].unique())

        # Track daily performance
        daily_portfolio_value = portfolio_value

        for i, rebal_date in enumerate(rebalance_dates):
            logger.info(f"Rebalance {i+1}/{len(rebalance_dates)}: {rebal_date.date()}")

            # Get target weights
            target_weights = weight_func(rebal_date)

            if target_weights is None or len(target_weights) == 0:
                logger.warning(f"No target weights for {rebal_date}")
                continue

            # Filter to valid tickers
            target_weights = target_weights[target_weights != 0]

            # Execution date (with lag)
            if self.calendar:
                exec_date = self.calendar.next_trading_day(rebal_date, execution_lag)
            else:
                exec_date_idx = all_dates.index(rebal_date) + execution_lag
                if exec_date_idx >= len(all_dates):
                    logger.warning("Execution date beyond data range")
                    break
                exec_date = all_dates[exec_date_idx]

            # Get execution prices
            exec_prices = price_data[price_data["date"] == exec_date].set_index("ticker")["adj_close"]
            exec_volumes = price_data[price_data["date"] == exec_date].set_index("ticker")["volume"]

            if exec_prices.empty:
                logger.warning(f"No price data for execution date {exec_date}")
                continue

            # Align target weights with available prices
            target_weights = target_weights.reindex(exec_prices.index).fillna(0)
            target_weights = target_weights[target_weights != 0]

            # Compute trades
            trades = target_weights.subtract(current_weights, fill_value=0)

            # Compute transaction costs
            costs = self.cost_model.compute_total_cost(
                trades, exec_prices, exec_volumes, portfolio_value
            )

            # Update portfolio
            portfolio_value -= costs
            current_weights = target_weights.copy()

            # Track
            weights_history.append(
                {"date": exec_date, "weights": current_weights.to_dict()}
            )
            trades_history.append(
                {"date": exec_date, "trades": trades.to_dict()}
            )
            costs_history.append({"date": exec_date, "costs": costs})

            # Compute returns until next rebalance
            if i < len(rebalance_dates) - 1:
                next_rebal_date = rebalance_dates[i + 1]
            else:
                # Last period: hold until end of data
                next_rebal_date = all_dates[-1]

            # Get price data between rebalances
            period_mask = (price_data["date"] > exec_date) & (price_data["date"] <= next_rebal_date)
            period_data = price_data[period_mask].copy()

            if period_data.empty:
                continue

            # Compute daily returns for this period
            period_dates = sorted(period_data["date"].unique())

            for date in period_dates:
                date_prices = period_data[period_data["date"] == date].set_index("ticker")["adj_close"]

                # Previous day prices
                prev_date_idx = all_dates.index(date) - 1
                if prev_date_idx < 0:
                    continue
                prev_date = all_dates[prev_date_idx]
                prev_prices = price_data[price_data["date"] == prev_date].set_index("ticker")["adj_close"]

                # Compute returns for held positions
                position_returns = pd.Series(dtype=float)
                for ticker in current_weights.index:
                    if ticker in date_prices and ticker in prev_prices:
                        if prev_prices[ticker] > 0:
                            ret = (date_prices[ticker] / prev_prices[ticker]) - 1
                            position_returns[ticker] = ret * current_weights[ticker]

                # Portfolio return
                if len(position_returns) > 0:
                    portfolio_return = position_returns.sum()
                else:
                    portfolio_return = 0.0

                # Update portfolio value
                daily_portfolio_value *= (1 + portfolio_return)

                # Track
                portfolio_values.append({"date": date, "value": daily_portfolio_value})
                returns_list.append({"date": date, "return": portfolio_return})

            # Update for next rebalance
            portfolio_value = daily_portfolio_value

        # Convert to DataFrames
        portfolio_values_series = pd.DataFrame(portfolio_values).set_index("date")["value"]
        returns_series = pd.DataFrame(returns_list).set_index("date")["return"]

        # Compute metrics
        metrics = self._compute_metrics(portfolio_values_series, returns_series, costs_history)

        # Create daily stats
        daily_stats = pd.DataFrame({
            "portfolio_value": portfolio_values_series,
            "returns": returns_series,
        })

        # Convert history to DataFrames
        weights_df = pd.DataFrame(weights_history)
        trades_df = pd.DataFrame(trades_history)
        costs_series = pd.DataFrame(costs_history).set_index("date")["costs"]

        logger.info(f"Backtest complete. Final value: ${portfolio_value:,.0f}")

        return BacktestResult(
            portfolio_values=portfolio_values_series,
            returns=returns_series,
            weights_history=weights_df,
            trades_history=trades_df,
            costs_history=costs_series,
            metrics=metrics,
            daily_stats=daily_stats,
        )

    def _compute_metrics(
        self,
        portfolio_values: pd.Series,
        returns: pd.Series,
        costs_history: List[Dict],
    ) -> Dict[str, float]:
        """
        Compute backtest performance metrics.

        Args:
            portfolio_values: Time series of portfolio values
            returns: Time series of returns
            costs_history: List of cost records

        Returns:
            Dictionary of metrics
        """
        metrics = {}

        # Total return
        total_return = (portfolio_values.iloc[-1] / self.initial_capital) - 1
        metrics["total_return"] = total_return

        # CAGR
        n_years = len(portfolio_values) / 252  # Approximate
        if n_years > 0:
            metrics["cagr"] = (1 + total_return) ** (1 / n_years) - 1
        else:
            metrics["cagr"] = 0.0

        # Volatility (annualized)
        metrics["volatility"] = returns.std() * np.sqrt(252)

        # Sharpe ratio (assuming 0 risk-free rate)
        if metrics["volatility"] > 0:
            metrics["sharpe"] = metrics["cagr"] / metrics["volatility"]
        else:
            metrics["sharpe"] = 0.0

        # Sortino ratio
        downside_returns = returns[returns < 0]
        if len(downside_returns) > 0:
            downside_std = downside_returns.std() * np.sqrt(252)
            if downside_std > 0:
                metrics["sortino"] = metrics["cagr"] / downside_std
            else:
                metrics["sortino"] = 0.0
        else:
            metrics["sortino"] = metrics["sharpe"]

        # Max drawdown
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        metrics["max_drawdown"] = drawdown.min()

        # Hit rate
        metrics["hit_rate"] = (returns > 0).sum() / len(returns) if len(returns) > 0 else 0.0

        # Average turnover
        if costs_history:
            total_costs = sum(c["costs"] for c in costs_history)
            metrics["total_costs"] = total_costs
            metrics["avg_rebalance_cost"] = total_costs / len(costs_history)

            # Approximate turnover from costs (assuming X bps per side)
            bps_total = self.cost_model.total_bps
            if bps_total > 0:
                implied_turnover = (total_costs / self.initial_capital) / (bps_total / 10000)
                metrics["total_turnover"] = implied_turnover
                metrics["avg_turnover_per_rebalance"] = implied_turnover / len(costs_history)
        else:
            metrics["total_costs"] = 0.0
            metrics["avg_rebalance_cost"] = 0.0
            metrics["total_turnover"] = 0.0
            metrics["avg_turnover_per_rebalance"] = 0.0

        # Number of trades
        metrics["num_rebalances"] = len(costs_history)

        return metrics


def compute_backtest_summary(result: BacktestResult) -> pd.DataFrame:
    """
    Generate summary table of backtest results.

    Args:
        result: BacktestResult object

    Returns:
        DataFrame with summary statistics
    """
    summary = []

    for key, value in result.metrics.items():
        # Format value
        if "return" in key.lower() or "cagr" in key.lower():
            formatted = f"{value * 100:.2f}%"
        elif "sharpe" in key.lower() or "sortino" in key.lower():
            formatted = f"{value:.2f}"
        elif "drawdown" in key.lower():
            formatted = f"{value * 100:.2f}%"
        elif "cost" in key.lower():
            formatted = f"${value:,.0f}"
        elif "turnover" in key.lower():
            formatted = f"{value:.2f}"
        elif "rate" in key.lower():
            formatted = f"{value * 100:.1f}%"
        else:
            formatted = f"{value:.2f}"

        summary.append({"Metric": key, "Value": formatted})

    return pd.DataFrame(summary)
