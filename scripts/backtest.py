#!/usr/bin/env python3
"""Backtest script: run strategy simulation."""

import argparse
import yaml
import pickle
from pathlib import Path

import pandas as pd
from loguru import logger

from src.pit_store import PITDataStore
from src.features.factors import FactorEngine
from src.portfolio.optimizer import PortfolioOptimizer, PortfolioConstraints
from src.backtest.runner import Backtester, compute_backtest_summary
from src.backtest.costs import TransactionCostModel
from src.utils.logging import setup_logging
from src.utils.clock import TradingCalendar, get_rebalance_dates


def main():
    parser = argparse.ArgumentParser(description="Run strategy backtest")
    parser.add_argument(
        "--config",
        type=str,
        default="config/defaults.yaml",
        help="Path to config file",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="models/model.pkl",
        help="Path to trained model",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="results/backtest",
        help="Output directory for results",
    )

    args = parser.parse_args()

    # Load config
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    # Setup logging
    setup_logging(
        level=config.get("logging", {}).get("level", "INFO"),
        log_file=config.get("logging", {}).get("file"),
    )

    logger.info(f"Starting backtest with config: {args.config}")

    # Load model
    logger.info(f"Loading model from {args.model}")
    with open(args.model, "rb") as f:
        model_data = pickle.load(f)

    model = model_data["model"]
    feature_cols = model_data["feature_cols"]
    target_col = model_data["target_col"]

    # Load data
    pit_store = PITDataStore(data_dir="data")

    start_date = config["start_date"]
    end_date = config["end_date"]

    logger.info(f"Loading data from {start_date} to {end_date}")

    df = pit_store.merge_prices_fundamentals(start_date, end_date)

    if df.empty:
        logger.error("No data loaded")
        return

    logger.info(f"Loaded {len(df)} rows for {df['ticker'].nunique()} tickers")

    # Filter universe
    min_price = config["universe"].get("min_price", 5.0)
    min_volume = config["universe"].get("min_median_dollar_vol", 1_000_000)

    df["dollar_volume"] = df["adj_close"] * df["volume"]
    median_dollar_vol = df.groupby("ticker")["dollar_volume"].transform("median")

    df = df[
        (df["adj_close"] >= min_price) &
        (median_dollar_vol >= min_volume)
    ].copy()

    logger.info(f"After filters: {len(df)} rows, {df['ticker'].nunique()} tickers")

    # Compute factors
    logger.info("Computing factors...")
    factor_engine = FactorEngine(
        winsorize_quantiles=tuple(config["features"].get("winsorize_quantiles", [0.01, 0.99])),
        min_sector_size=config["features"].get("min_sector_size", 10),
    )

    factor_dfs = []
    for date, date_df in df.groupby("date"):
        date_df = factor_engine.compute_all_factors(
            date_df,
            compute_composite=False,  # Will use model predictions
        )
        factor_dfs.append(date_df)

    df = pd.concat(factor_dfs, ignore_index=True)

    # Generate predictions
    logger.info("Generating predictions...")
    X = df[feature_cols].fillna(0)
    df["alpha_score"] = model.predict(df)

    # Setup calendar and rebalance dates
    calendar = TradingCalendar(start_date, end_date, exchange="US")
    rebalance_dates = get_rebalance_dates(
        start_date,
        end_date,
        frequency=config.get("rebalance_freq", "weekly"),
        calendar=calendar,
    )

    logger.info(f"Generated {len(rebalance_dates)} rebalance dates")

    # Setup portfolio constraints
    portfolio_config = config["portfolio"]
    constraints = PortfolioConstraints(
        long_pct=portfolio_config.get("long_pct", 0.20),
        short_pct=portfolio_config.get("short_pct", 0.20),
        target_beta=portfolio_config.get("target_beta", 0.0),
        beta_tolerance=portfolio_config.get("beta_tolerance", 0.05),
        sector_max_weight=portfolio_config.get("sector_max_weight", 0.15),
        single_name_max_weight=portfolio_config.get("single_name_max_weight", 0.05),
        gross_leverage=portfolio_config.get("gross_leverage", 2.0),
        net_exposure_target=portfolio_config.get("net_exposure_target", 0.0),
        turnover_penalty=config["optimizer"].get("turnover_penalty", 0.01),
    )

    # Setup optimizer
    optimizer_config = config["optimizer"]
    optimizer = PortfolioOptimizer(
        constraints=constraints,
        use_pca_risk=optimizer_config.get("risk_model", {}).get("use_pca", True),
        pca_components=optimizer_config.get("risk_model", {}).get("pca_components", 20),
    )

    # Weight function for backtester
    prev_weights = None

    def get_weights(date):
        nonlocal prev_weights

        # Get data as-of date
        date_df = df[df["date"] == date].copy()

        if date_df.empty:
            return pd.Series(dtype=float)

        # Get scores, sectors, prices
        date_df = date_df.set_index("ticker")
        scores = date_df["alpha_score"]
        sectors = date_df["sector"]
        prices = date_df["adj_close"]

        # Optimize
        weights = optimizer.optimize(
            scores=scores,
            sectors=sectors,
            prices=prices,
            prev_weights=prev_weights,
        )

        prev_weights = weights.copy()

        return weights

    # Setup cost model
    costs_config = config["costs"]
    cost_model = TransactionCostModel(
        bps_per_side=costs_config.get("bps_per_side", 5.0),
        spread_proxy_bps=costs_config.get("spread_proxy_bps", 3.0),
    )

    # Setup backtester
    backtester = Backtester(
        initial_capital=1_000_000.0,
        cost_model=cost_model,
        calendar=calendar,
    )

    # Prepare price data for backtester
    price_data = df[["date", "ticker", "adj_close", "volume"]].copy()

    # Run backtest
    logger.info("Running backtest...")
    result = backtester.run(
        rebalance_dates=rebalance_dates,
        weight_func=get_weights,
        price_data=price_data,
        execution_lag=1,
    )

    # Save results
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    # Save metrics
    summary = compute_backtest_summary(result)
    summary_path = output_path / "summary.csv"
    summary.to_csv(summary_path, index=False)

    logger.info(f"Summary saved to {summary_path}")

    # Save daily stats
    stats_path = output_path / "daily_stats.csv"
    result.daily_stats.to_csv(stats_path)

    logger.info(f"Daily stats saved to {stats_path}")

    # Save backtest result object
    result_path = output_path / "backtest_result.pkl"
    with open(result_path, "wb") as f:
        pickle.dump(result, f)

    logger.info(f"Full result saved to {result_path}")

    # Print summary
    logger.info("=" * 50)
    logger.info("BACKTEST SUMMARY")
    logger.info("=" * 50)

    for _, row in summary.iterrows():
        logger.info(f"{row['Metric']}: {row['Value']}")

    logger.info("\nBacktest complete!")


if __name__ == "__main__":
    main()
