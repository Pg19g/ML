#!/usr/bin/env python3
"""Data ingestion script: fetch and store EODHD data."""

import argparse
from pathlib import Path
import yaml
from datetime import datetime

import pandas as pd
from loguru import logger

from src.eodhd_client import EODHDClient
from src.pit_store import PITDataStore
from src.utils.logging import setup_logging
from src.utils.clock import TradingCalendar


def main():
    parser = argparse.ArgumentParser(description="Ingest EODHD data")
    parser.add_argument(
        "--config",
        type=str,
        default="config/defaults.yaml",
        help="Path to config file",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        help="Specific tickers to fetch (overrides config)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD, overrides config)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD, overrides config)",
    )
    parser.add_argument(
        "--skip-fundamentals",
        action="store_true",
        help="Skip fundamentals ingestion",
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

    logger.info(f"Starting data ingestion with config: {args.config}")

    # Override with CLI args
    if args.tickers:
        tickers = args.tickers
    else:
        tickers = config["universe"].get("tickers")

    start_date = args.start_date or config["start_date"]
    end_date = args.end_date or config["end_date"]

    # Initialize clients
    api_config = config.get("api", {})
    client = EODHDClient(
        cache_dir=api_config.get("cache_dir", "data/cache"),
        retry_attempts=api_config.get("retry_attempts", 3),
        retry_backoff=api_config.get("retry_backoff", 2.0),
    )

    calendar = TradingCalendar(start_date, end_date, exchange="US")

    pit_store = PITDataStore(
        data_dir="data",
        pit_lag_days=config.get("pit_lag_days", 2),
        calendar=calendar,
    )

    # Get universe of tickers
    if tickers is None:
        logger.info("Fetching ticker list from exchange...")
        exchanges = config["universe"].get("exchanges", ["US"])

        all_tickers = []
        for exchange in exchanges:
            tickers_df = client.get_exchange_tickers(exchange)
            if not tickers_df.empty:
                all_tickers.extend(tickers_df["Code"].tolist())

        # Apply universe filters
        max_tickers = config["universe"].get("max_tickers")
        if max_tickers and len(all_tickers) > max_tickers:
            logger.info(f"Limiting to {max_tickers} tickers")
            all_tickers = all_tickers[:max_tickers]

        tickers = all_tickers
    else:
        tickers = list(tickers)

    logger.info(f"Fetching data for {len(tickers)} tickers from {start_date} to {end_date}")

    # Fetch prices
    logger.info("Fetching prices...")
    all_prices = []

    for i, ticker in enumerate(tickers):
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i+1}/{len(tickers)}")

        try:
            prices = client.get_eod_prices(
                ticker=ticker,
                exchange=exchanges[0] if not args.tickers else "US",
                start_date=start_date,
                end_date=end_date,
            )

            if not prices.empty:
                # Add sector placeholder (will fetch from fundamentals)
                prices["sector"] = "Unknown"
                all_prices.append(prices)

        except Exception as e:
            logger.error(f"Failed to fetch prices for {ticker}: {e}")
            continue

    if all_prices:
        combined_prices = pd.concat(all_prices, ignore_index=True)
        logger.info(f"Storing {len(combined_prices)} price records...")
        pit_store.store_prices(combined_prices)
    else:
        logger.warning("No price data fetched")

    # Fetch fundamentals
    if not args.skip_fundamentals:
        logger.info("Fetching fundamentals...")
        all_fundamentals = []

        for i, ticker in enumerate(tickers):
            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i+1}/{len(tickers)}")

            try:
                fundamentals = client.get_fundamentals(
                    ticker=ticker,
                    exchange=exchanges[0] if not args.tickers else "US",
                )

                if fundamentals:
                    # Parse to PIT records
                    pit_records = client.parse_financials_to_pit(fundamentals, ticker)
                    if not pit_records.empty:
                        all_fundamentals.append(pit_records)

            except Exception as e:
                logger.error(f"Failed to fetch fundamentals for {ticker}: {e}")
                continue

        if all_fundamentals:
            combined_fundamentals = pd.concat(all_fundamentals, ignore_index=True)
            logger.info(f"Storing {len(combined_fundamentals)} fundamental records...")
            pit_store.store_fundamentals(combined_fundamentals)
        else:
            logger.warning("No fundamentals data fetched")

    # Validate PIT integrity
    logger.info("Validating point-in-time integrity...")
    validation = pit_store.validate_pit_integrity()

    if validation["passed"]:
        logger.info("PIT validation passed ✓")
    else:
        logger.error("PIT validation failed:")
        for error in validation["errors"]:
            logger.error(f"  - {error}")

    for warning in validation["warnings"]:
        logger.warning(f"  - {warning}")

    logger.info("Data ingestion complete!")


if __name__ == "__main__":
    main()
