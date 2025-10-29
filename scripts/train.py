#!/usr/bin/env python3
"""Model training script with walk-forward CV."""

import argparse
import yaml
import pickle
from pathlib import Path

import pandas as pd
from loguru import logger

from src.data_loader import DataLoader
from src.features.factors import FactorEngine, compute_forward_returns
from src.models.train import train_with_cv
from src.utils.logging import setup_logging
from src.utils.clock import TradingCalendar


def main():
    parser = argparse.ArgumentParser(description="Train alpha model")
    parser.add_argument(
        "--config",
        type=str,
        default="config/defaults.yaml",
        help="Path to config file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="models/model.pkl",
        help="Path to save trained model",
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

    logger.info(f"Starting model training with config: {args.config}")

    # Load data using new integrated DataLoader
    # (combines prices from legacy PITDataStore + fundamentals from new PITStore)
    pit_config = config.get("pit", {})
    data_loader = DataLoader(
        data_dir="data",
        pit_snapshot_dir=pit_config.get("snapshot_dir", "data/pit"),
        pit_lag_days=config.get("pit_lag_days", 2),
    )

    start_date = config["start_date"]
    end_date = config["end_date"]

    logger.info(f"Loading data from {start_date} to {end_date}")

    # Get merged prices and fundamentals (now uses PIT snapshots for fundamentals)
    df = data_loader.merge_prices_fundamentals(start_date, end_date)

    if df.empty:
        logger.error("No data loaded")
        return

    logger.info(f"Loaded {len(df)} rows for {df['ticker'].nunique()} tickers")

    # Validate PIT integrity (ensure no information leakage)
    logger.info("Validating PIT integrity...")
    data_loader.validate_pit_integrity_end_to_end(df)

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

    # Group by date and compute factors for each cross-section
    factor_dfs = []

    for date, date_df in df.groupby("date"):
        date_df = factor_engine.compute_all_factors(
            date_df,
            compute_composite=True,
            composite_weights=config["features"].get("composite_weights"),
        )
        factor_dfs.append(date_df)

    df = pd.concat(factor_dfs, ignore_index=True)

    # Compute forward returns (labels)
    logger.info("Computing forward returns...")
    horizon_days = config["targets"].get("horizon_days", 21)
    df = compute_forward_returns(df, horizon_days=horizon_days)

    target_col = config["targets"].get("primary", "next_21d_excess_vs_sector")

    # Remove samples without target
    df = df[df[target_col].notna()].copy()

    logger.info(f"Training samples: {len(df)}")

    # Get feature columns
    feature_cols = factor_engine.get_feature_columns(standardized=True)

    # Filter to available features
    feature_cols = [col for col in feature_cols if col in df.columns]

    logger.info(f"Using {len(feature_cols)} features")

    # Train with walk-forward CV
    logger.info("Training model with walk-forward CV...")

    model, cv_results = train_with_cv(
        df=df,
        model_config=config["model"],
        cv_config=config["cv"],
        feature_cols=feature_cols,
        target_col=target_col,
        date_col="date",
    )

    # Save model
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
        pickle.dump(
            {
                "model": model,
                "feature_cols": feature_cols,
                "target_col": target_col,
                "config": config,
            },
            f,
        )

    logger.info(f"Model saved to {output_path}")

    # Save CV results
    cv_path = output_path.parent / "cv_results.csv"
    cv_results.to_csv(cv_path, index=False)

    logger.info(f"CV results saved to {cv_path}")

    # Print summary
    logger.info("=" * 50)
    logger.info("TRAINING SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Mean Test IC: {cv_results['test_ic'].mean():.4f}")
    logger.info(f"Mean Rank IC: {cv_results['test_rank_ic'].mean():.4f}")
    logger.info(f"IC Std: {cv_results['test_ic'].std():.4f}")
    logger.info(f"Number of Folds: {len(cv_results)}")

    # Get feature importance
    importance = model.get_feature_importance()
    if importance is not None:
        importance_path = output_path.parent / "feature_importance.csv"
        importance.to_csv(importance_path, index=False)
        logger.info(f"Feature importance saved to {importance_path}")

        logger.info("\nTop 10 Features:")
        for idx, row in importance.head(10).iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.4f}")

    logger.info("\nTraining complete!")


if __name__ == "__main__":
    main()
