"""Backtesting engine wrapper for backtesting.py library."""

from datetime import date
from typing import Dict, Any, Type, List

import pandas as pd
from backtesting import Backtest, Strategy
from loguru import logger


class BacktestEngine:
    """
    Wrapper around backtesting.py library for our use case.

    Features:
    - Single asset backtesting
    - Portfolio backtesting (multi-asset)
    - Standardized results format
    """

    def run_single_asset(
        self,
        data: pd.DataFrame,
        strategy_class: Type[Strategy],
        strategy_params: Dict[str, Any],
        cash: float = 10000,
        commission: float = 0.001,
    ) -> Dict[str, Any]:
        """
        Run backtest on single asset.

        Args:
            data: OHLCV DataFrame with DatetimeIndex
            strategy_class: Strategy class (subclass of Strategy)
            strategy_params: Strategy parameters
            cash: Initial cash
            commission: Commission rate (0.001 = 0.1%)

        Returns:
            Dictionary with metrics, trades, equity curve
        """
        logger.info(f"Running backtest with {strategy_class.__name__}")
        logger.info(f"Data range: {data.index[0]} to {data.index[-1]} ({len(data)} bars)")
        logger.info(f"Parameters: {strategy_params}")

        try:
            # Create backtest instance
            bt = Backtest(
                data,
                strategy_class,
                cash=cash,
                commission=commission,
                exclusive_orders=True,
            )

            # Run backtest with parameters
            stats = bt.run(**strategy_params)

            # Extract results
            result = {
                "success": True,
                "metrics": {
                    "sharpe_ratio": float(stats.get("Sharpe Ratio", 0)),
                    "total_return": float(stats.get("Return [%]", 0)),
                    "max_drawdown": float(stats.get("Max. Drawdown [%]", 0)),
                    "num_trades": int(stats.get("# Trades", 0)),
                    "win_rate": float(stats.get("Win Rate [%]", 0)),
                    "avg_trade": float(stats.get("Avg. Trade [%]", 0)),
                    "max_trade_duration": str(stats.get("Max. Trade Duration", "")),
                    "avg_trade_duration": str(stats.get("Avg. Trade Duration", "")),
                    "profit_factor": float(stats.get("Profit Factor", 0)),
                    "expectancy": float(stats.get("Expectancy [%]", 0)),
                    "sqn": float(stats.get("SQN", 0)),
                },
                "stats_summary": str(stats),
            }

            # Extract trades if available
            if hasattr(stats, "_trades") and stats._trades is not None:
                trades_df = stats._trades
                result["trades"] = trades_df.to_dict("records")
                result["num_trades"] = len(trades_df)
            else:
                result["trades"] = []
                result["num_trades"] = 0

            # Extract equity curve if available
            if hasattr(stats, "_equity_curve") and stats._equity_curve is not None:
                equity_df = stats._equity_curve
                result["equity_curve"] = {
                    "dates": equity_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                    "equity": equity_df["Equity"].tolist(),
                    "drawdown": equity_df.get("DrawdownPct", pd.Series([0] * len(equity_df))).tolist(),
                }
            else:
                result["equity_curve"] = {
                    "dates": [],
                    "equity": [],
                    "drawdown": [],
                }

            logger.info(f"Backtest completed successfully")
            logger.info(f"Sharpe: {result['metrics']['sharpe_ratio']:.2f}, "
                       f"Return: {result['metrics']['total_return']:.2f}%, "
                       f"Trades: {result['metrics']['num_trades']}")

            return result

        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "metrics": {},
                "trades": [],
                "equity_curve": {"dates": [], "equity": [], "drawdown": []},
            }

    def run_portfolio(
        self,
        symbols_data: Dict[str, pd.DataFrame],
        strategy_class: Type[Strategy],
        strategy_params: Dict[str, Any],
        cash: float = 10000,
        commission: float = 0.001,
        allocation_per_position: float = 0.1,
    ) -> Dict[str, Any]:
        """
        Run strategy on multiple assets and aggregate results.

        Args:
            symbols_data: Dict mapping symbol to OHLCV DataFrame
            strategy_class: Strategy class
            strategy_params: Strategy parameters
            cash: Total initial cash
            commission: Commission rate
            allocation_per_position: Cash allocation per position (e.g., 0.1 = 10%)

        Returns:
            Aggregated portfolio results
        """
        logger.info(f"Running portfolio backtest on {len(symbols_data)} symbols")

        results = {}
        cash_per_symbol = cash * allocation_per_position

        # Run backtest for each symbol
        for symbol, data in symbols_data.items():
            logger.info(f"Backtesting {symbol}...")
            result = self.run_single_asset(
                data,
                strategy_class,
                strategy_params,
                cash=cash_per_symbol,
                commission=commission,
            )
            results[symbol] = result

        # Aggregate results
        portfolio_result = self._aggregate_portfolio_results(
            results, cash, allocation_per_position
        )

        return portfolio_result

    def _aggregate_portfolio_results(
        self,
        results: Dict[str, Dict[str, Any]],
        total_cash: float,
        allocation_per_position: float,
    ) -> Dict[str, Any]:
        """Aggregate individual backtest results into portfolio metrics."""
        # Collect all equity curves
        equity_curves = {}
        for symbol, result in results.items():
            if result.get("success") and result.get("equity_curve"):
                equity_curves[symbol] = result["equity_curve"]

        # Combine equity curves (simplified - equal weight)
        if equity_curves:
            # Find common dates (intersection)
            all_dates = None
            for symbol, curve in equity_curves.items():
                dates = pd.to_datetime(curve["dates"])
                if all_dates is None:
                    all_dates = dates
                else:
                    all_dates = all_dates.intersection(dates)

            # Calculate portfolio equity
            portfolio_equity = []
            for dt in all_dates:
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                equity_sum = 0
                for symbol, curve in equity_curves.items():
                    idx = curve["dates"].index(dt_str)
                    equity_sum += curve["equity"][idx]

                # Average equity across symbols
                portfolio_equity.append(equity_sum / len(equity_curves))

            # Calculate portfolio metrics
            initial_equity = portfolio_equity[0] if portfolio_equity else total_cash
            final_equity = portfolio_equity[-1] if portfolio_equity else total_cash
            total_return = ((final_equity - initial_equity) / initial_equity) * 100

            # Simple drawdown calculation
            peak = initial_equity
            max_drawdown = 0
            for equity in portfolio_equity:
                if equity > peak:
                    peak = equity
                drawdown = ((peak - equity) / peak) * 100
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

            # Aggregate trade statistics
            total_trades = sum(r.get("num_trades", 0) for r in results.values() if r.get("success"))
            all_trades = []
            for result in results.values():
                if result.get("success") and result.get("trades"):
                    all_trades.extend(result["trades"])

            return {
                "success": True,
                "portfolio_metrics": {
                    "total_return": total_return,
                    "max_drawdown": max_drawdown,
                    "num_trades": total_trades,
                    "num_symbols": len(results),
                    "initial_equity": initial_equity,
                    "final_equity": final_equity,
                },
                "equity_curve": {
                    "dates": [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in all_dates],
                    "equity": portfolio_equity,
                },
                "individual_results": results,
                "trades": all_trades,
            }
        else:
            return {
                "success": False,
                "error": "No successful backtests",
                "portfolio_metrics": {},
                "equity_curve": {"dates": [], "equity": []},
                "individual_results": results,
            }
