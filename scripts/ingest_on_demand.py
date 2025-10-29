#!/usr/bin/env python3
"""
On-demand fundamentals ingestion using PIT snapshot architecture.

Ensures fundamentals snapshots exist for specified symbols.
Fetches from EODHD API only if missing or forced.
"""

import argparse
from pathlib import Path
import yaml
import sys

import pandas as pd
from loguru import logger

from src.eodhd_client import EODHDClient
from src.pit_snapshots import PITStore
from src.ingest.fundamentals_on_demand import FundamentalsOnDemand
from src.utils.logging import setup_logging


def main():
    parser = argparse.ArgumentParser(
        description="On-demand fundamentals ingestion with PIT snapshots"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/defaults.yaml",
        help="Path to config file",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        help="Specific symbols to fetch (e.g., AAPL.US MSFT.US)",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Fetch all symbols from exchange (e.g., US, WAR, LSE)",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force re-fetch even if snapshots exist",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        help="Limit number of symbols (for testing)",
    )
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Only show coverage report, don't fetch",
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

    logger.info(f"On-demand fundamentals ingestion (config: {args.config})")

    # Extract PIT config
    pit_config = config.get("pit", {})
    snapshot_dir = pit_config.get("snapshot_dir", "data/pit")
    extra_lag_trading_days = pit_config.get("extra_lag_trading_days", 2)
    conservative_lag_days = pit_config.get("conservative_lag_days", {})
    availability_source_priority = pit_config.get("availability_source_priority", [])
    min_periods_required = pit_config.get("min_periods_required", 4)

    # Initialize EODHD client
    api_config = config.get("api", {})
    client = EODHDClient(
        cache_dir=api_config.get("cache_dir", "data/cache"),
        retry_attempts=api_config.get("retry_attempts", 3),
        retry_backoff=api_config.get("retry_backoff", 2.0),
    )

    # Initialize PIT store
    pit_store = PITStore(
        snapshot_dir=snapshot_dir,
        extra_lag_trading_days=extra_lag_trading_days,
        conservative_lag_days=conservative_lag_days,
        availability_source_priority=availability_source_priority,
    )

    # Initialize on-demand fetcher
    fetcher = FundamentalsOnDemand(
        pit_store=pit_store,
        eodhd_client=client,
        min_periods_required=min_periods_required,
    )

    # Determine symbol universe
    symbols = []

    if args.symbols:
        symbols = args.symbols
        logger.info(f"Using {len(symbols)} symbols from command line")

    elif args.exchange:
        logger.info(f"Fetching symbol list from exchange: {args.exchange}")
        try:
            tickers_df = client.list_exchange_symbols(args.exchange)
            if not tickers_df.empty:
                # Add exchange suffix
                symbols = [
                    f"{code}.{args.exchange}"
                    for code in tickers_df["Code"].tolist()
                ]
                logger.info(f"Found {len(symbols)} symbols on {args.exchange}")
            else:
                logger.error(f"No symbols found for exchange {args.exchange}")
                return 1
        except Exception as e:
            logger.error(f"Failed to fetch exchange symbols: {e}")
            return 1

    else:
        # Use config universe
        universe = config.get("universe", {})

        if "tickers" in universe and universe["tickers"]:
            symbols = universe["tickers"]
            logger.info(f"Using {len(symbols)} symbols from config universe.tickers")

        elif "exchanges" in universe:
            exchanges = universe["exchanges"]
            logger.info(f"Fetching symbols from config exchanges: {exchanges}")

            for exchange in exchanges:
                try:
                    tickers_df = client.list_exchange_symbols(exchange)
                    if not tickers_df.empty:
                        exchange_symbols = [
                            f"{code}.{exchange}"
                            for code in tickers_df["Code"].tolist()
                        ]
                        symbols.extend(exchange_symbols)
                except Exception as e:
                    logger.error(f"Failed to fetch {exchange} symbols: {e}")
                    continue

            logger.info(f"Found {len(symbols)} symbols from {len(exchanges)} exchanges")

        else:
            logger.error("No symbol universe specified (use --symbols, --exchange, or config)")
            return 1

    # Apply max_symbols limit
    if args.max_symbols and len(symbols) > args.max_symbols:
        logger.info(f"Limiting to {args.max_symbols} symbols")
        symbols = symbols[:args.max_symbols]
    elif "max_tickers" in config.get("universe", {}):
        max_tickers = config["universe"]["max_tickers"]
        if len(symbols) > max_tickers:
            logger.info(f"Limiting to {max_tickers} symbols (from config)")
            symbols = symbols[:max_tickers]

    if not symbols:
        logger.error("No symbols to process")
        return 1

    # Coverage report (always generate first)
    logger.info("Checking coverage...")
    coverage = fetcher.get_coverage_report(symbols)

    existing_count = coverage["has_data"].sum()
    missing_count = len(coverage) - existing_count

    logger.info(f"Coverage: {existing_count} symbols with data, {missing_count} missing")

    if not coverage.empty:
        print("\n" + "="*80)
        print("COVERAGE REPORT")
        print("="*80)
        print(coverage.to_string(index=False))
        print("="*80 + "\n")

    # If coverage-only mode, exit
    if args.coverage_only:
        logger.info("Coverage-only mode, exiting")
        return 0

    # Ensure snapshots
    logger.info(f"Ensuring snapshots for {len(symbols)} symbols...")

    success_count = 0
    skip_count = 0
    error_count = 0
    total_snapshots_created = 0

    for i, symbol in enumerate(symbols):
        if (i + 1) % 10 == 0 or (i + 1) == len(symbols):
            logger.info(f"Progress: {i+1}/{len(symbols)}")

        try:
            created = fetcher.ensure_snapshots(
                symbol=symbol,
                force_refresh=args.force_refresh,
            )

            if created > 0:
                success_count += 1
                total_snapshots_created += created
                logger.info(f"✓ {symbol}: Created {created} snapshots")
            else:
                skip_count += 1
                logger.debug(f"⊙ {symbol}: Using existing snapshots")

        except Exception as e:
            error_count += 1
            logger.error(f"✗ {symbol}: {e}")
            continue

    # Final summary
    logger.info("\n" + "="*80)
    logger.info("INGESTION SUMMARY")
    logger.info("="*80)
    logger.info(f"Total symbols processed: {len(symbols)}")
    logger.info(f"  - Fetched (new/refresh): {success_count}")
    logger.info(f"  - Skipped (existing):    {skip_count}")
    logger.info(f"  - Errors:                {error_count}")
    logger.info(f"Total snapshots created:   {total_snapshots_created}")
    logger.info("="*80)

    # Generate final coverage report
    logger.info("\nFinal coverage:")
    final_coverage = fetcher.get_coverage_report(symbols)
    final_existing = final_coverage["has_data"].sum()
    logger.info(f"Symbols with data: {final_existing}/{len(symbols)}")

    if error_count > 0:
        logger.warning(f"{error_count} symbols failed - check logs for details")
        return 1

    logger.info("On-demand ingestion complete! ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main())
