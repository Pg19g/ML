#!/usr/bin/env python3
"""Generate comprehensive backtest report."""

import argparse
import pickle
from pathlib import Path

import pandas as pd
from loguru import logger

from src.reporting.report import ReportGenerator
from src.utils.logging import setup_logging


def main():
    parser = argparse.ArgumentParser(description="Generate backtest report")
    parser.add_argument(
        "--backtest",
        type=str,
        default="results/backtest/backtest_result.pkl",
        help="Path to backtest result",
    )
    parser.add_argument(
        "--cv-results",
        type=str,
        default="models/cv_results.csv",
        help="Path to CV results",
    )
    parser.add_argument(
        "--feature-importance",
        type=str,
        default="models/feature_importance.csv",
        help="Path to feature importance",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="reports/latest",
        help="Output directory",
    )

    args = parser.parse_args()

    setup_logging(level="INFO")

    logger.info("Generating report...")

    # Load backtest result
    logger.info(f"Loading backtest result from {args.backtest}")
    with open(args.backtest, "rb") as f:
        backtest_result = pickle.load(f)

    # Load CV results
    cv_results = None
    if Path(args.cv_results).exists():
        logger.info(f"Loading CV results from {args.cv_results}")
        cv_results = pd.read_csv(args.cv_results)

    # Load feature importance
    feature_importance = None
    if Path(args.feature_importance).exists():
        logger.info(f"Loading feature importance from {args.feature_importance}")
        feature_importance = pd.read_csv(args.feature_importance)

    # Generate report
    report_gen = ReportGenerator(out_dir=args.output)

    report_path = report_gen.generate_full_report(
        backtest_result=backtest_result,
        cv_results=cv_results,
        feature_importance=feature_importance,
    )

    logger.info(f"Report generated: {report_path}")
    logger.info(f"Open in browser: file://{Path(report_path).absolute()}")


if __name__ == "__main__":
    main()
