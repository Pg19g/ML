"""Factor engineering: Value, Quality, Momentum, and Short-term Reversion."""

from typing import Dict, List, Optional

import pandas as pd
import numpy as np
from loguru import logger

from src.features.utils import (
    winsorize,
    zscore_by_group,
    rank_by_group,
    handle_missing_by_group,
    filter_min_sector_size,
)


class FactorEngine:
    """
    Factor engineering engine for cross-sectional equity alpha.

    Computes:
    - Value factors (earnings yield, FCF yield, EBIT/EV)
    - Quality factors (ROIC, margins, accruals)
    - Momentum factors (12-1 month momentum)
    - Short-term reversion (5-day reversal)
    - Event-like signals (buybacks, dividend changes)
    """

    def __init__(
        self,
        winsorize_quantiles: tuple = (0.01, 0.99),
        sector_col: str = "sector",
        min_sector_size: int = 10,
    ):
        """
        Initialize factor engine.

        Args:
            winsorize_quantiles: Quantiles for winsorization
            sector_col: Column name for sector
            min_sector_size: Minimum tickers per sector
        """
        self.winsorize_quantiles = winsorize_quantiles
        self.sector_col = sector_col
        self.min_sector_size = min_sector_size

        logger.info("Initialized FactorEngine")

    def compute_all_factors(
        self,
        df: pd.DataFrame,
        compute_composite: bool = True,
        composite_weights: Optional[Dict[str, float]] = None,
    ) -> pd.DataFrame:
        """
        Compute all factors for a cross-section.

        Args:
            df: DataFrame with prices and fundamentals
            compute_composite: Whether to compute composite alpha score
            composite_weights: Weights for composite (default: equal-weighted)

        Returns:
            DataFrame with all factors
        """
        df = df.copy()

        # Filter small sectors
        df = filter_min_sector_size(df, self.sector_col, self.min_sector_size)

        if df.empty:
            logger.warning("Empty DataFrame after filtering small sectors")
            return df

        # Compute factor families
        logger.info("Computing value factors...")
        df = self._compute_value_factors(df)

        logger.info("Computing quality factors...")
        df = self._compute_quality_factors(df)

        logger.info("Computing momentum factors...")
        df = self._compute_momentum_factors(df)

        logger.info("Computing short-term reversion...")
        df = self._compute_reversion_factors(df)

        logger.info("Computing event signals...")
        df = self._compute_event_signals(df)

        # Sector-neutralize and standardize
        logger.info("Standardizing factors...")
        df = self._standardize_factors(df)

        # Compute composite if requested
        if compute_composite:
            logger.info("Computing composite alpha score...")
            df = self._compute_composite_score(df, composite_weights)

        return df

    def _compute_value_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute value factors."""
        # Market cap
        if "shares_outstanding" in df.columns and "close" in df.columns:
            df["market_cap"] = df["shares_outstanding"] * df["close"]
        elif "market_cap" not in df.columns:
            logger.warning("Cannot compute market_cap: missing shares_outstanding or close")

        # Earnings yield = Net Income / Market Cap
        if "net_income_ttm" in df.columns and "market_cap" in df.columns:
            df["earnings_yield"] = df["net_income_ttm"] / df["market_cap"]
        else:
            logger.warning("Cannot compute earnings_yield: missing net_income_ttm or market_cap")
            df["earnings_yield"] = np.nan

        # FCF yield = Free Cash Flow / Market Cap
        if "fcf_ttm" in df.columns and "market_cap" in df.columns:
            df["fcf_yield"] = df["fcf_ttm"] / df["market_cap"]
        elif "free_cash_flow" in df.columns and "market_cap" in df.columns:
            df["fcf_yield"] = df["free_cash_flow"] / df["market_cap"]
        else:
            logger.warning("Cannot compute fcf_yield: missing fcf_ttm or market_cap")
            df["fcf_yield"] = np.nan

        # EBIT/EV (or inverse EV/EBITDA)
        if "ebitda_ttm" in df.columns and "market_cap" in df.columns:
            # Simplified EV = market_cap + total_debt - cash
            if "total_debt" in df.columns and "cash" in df.columns:
                df["enterprise_value"] = (
                    df["market_cap"] + df["total_debt"] - df["cash"]
                )
            else:
                df["enterprise_value"] = df["market_cap"]

            df["ebitda_ev"] = df["ebitda_ttm"] / df["enterprise_value"]
        elif "ebit_ttm" in df.columns and "market_cap" in df.columns:
            df["enterprise_value"] = df["market_cap"]
            df["ebitda_ev"] = df["ebit_ttm"] / df["enterprise_value"]
        else:
            logger.warning("Cannot compute ebitda_ev: missing ebitda_ttm")
            df["ebitda_ev"] = np.nan

        return df

    def _compute_quality_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute quality factors."""
        # ROIC = NOPAT / Invested Capital
        if all(col in df.columns for col in ["net_income_ttm", "total_assets", "total_liabilities"]):
            # Simplified: use net income as proxy for NOPAT
            invested_capital = df["total_assets"] - df["total_liabilities"]
            df["roic"] = df["net_income_ttm"] / invested_capital.replace(0, np.nan)
        else:
            logger.warning("Cannot compute roic: missing required fields")
            df["roic"] = np.nan

        # Gross margin
        if "gross_margin" in df.columns:
            df["gross_margin_factor"] = df["gross_margin"]
        elif "gross_profit" in df.columns and "revenue_ttm" in df.columns:
            df["gross_margin_factor"] = df["gross_profit"] / df["revenue_ttm"].replace(0, np.nan)
        else:
            logger.warning("Cannot compute gross_margin")
            df["gross_margin_factor"] = np.nan

        # Operating margin
        if "operating_margin" in df.columns:
            df["operating_margin_factor"] = df["operating_margin"]
        elif "operating_income" in df.columns and "revenue_ttm" in df.columns:
            df["operating_margin_factor"] = (
                df["operating_income"] / df["revenue_ttm"].replace(0, np.nan)
            )
        else:
            logger.warning("Cannot compute operating_margin")
            df["operating_margin_factor"] = np.nan

        # Accruals (lower is better)
        if all(col in df.columns for col in ["total_assets", "total_current_assets", "cash"]):
            # Simplified accruals
            # Accruals = (ΔWC - ΔCash) / Total Assets
            # For simplicity, use current assets - cash as proxy
            df["accruals"] = (df["total_current_assets"] - df["cash"]) / df["total_assets"].replace(0, np.nan)
            # Negative because lower accruals is better
            df["accruals_factor"] = -df["accruals"]
        else:
            logger.warning("Cannot compute accruals")
            df["accruals_factor"] = np.nan

        return df

    def _compute_momentum_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute momentum factors."""
        # Assumes df has price history columns or we compute returns
        # For cross-sectional point-in-time data, we need historical returns

        # 12-1 month momentum: return(252d) - return(21d)
        if "returns_252d" in df.columns and "returns_21d" in df.columns:
            df["momentum_12_1"] = df["returns_252d"] - df["returns_21d"]
        elif "adj_close" in df.columns:
            # If we have historical prices, compute on-the-fly
            # This requires ticker-level sorting and grouping
            # For now, mark as missing
            logger.warning("Cannot compute momentum_12_1: need historical returns")
            df["momentum_12_1"] = np.nan
        else:
            df["momentum_12_1"] = np.nan

        return df

    def _compute_reversion_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute short-term reversion factors."""
        # 5-day reversion: negative of 5-day return
        if "returns_5d" in df.columns:
            df["short_reversion_5d"] = -df["returns_5d"]
        elif "adj_close" in df.columns:
            logger.warning("Cannot compute short_reversion_5d: need historical returns")
            df["short_reversion_5d"] = np.nan
        else:
            df["short_reversion_5d"] = np.nan

        return df

    def _compute_event_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute event-like signals (buybacks, dividend changes)."""
        # Buyback yield = -Δ(shares_outstanding) / market_cap
        # Requires historical shares outstanding
        if "shares_outstanding_change" in df.columns and "market_cap" in df.columns:
            df["buyback_yield"] = -df["shares_outstanding_change"] / df["market_cap"].replace(0, np.nan)
        else:
            logger.warning("Cannot compute buyback_yield: need shares_outstanding_change")
            df["buyback_yield"] = np.nan

        # Dividend change
        if "dividend_change_pct" in df.columns:
            df["dividend_change"] = df["dividend_change_pct"]
        else:
            logger.warning("Cannot compute dividend_change")
            df["dividend_change"] = np.nan

        return df

    def _standardize_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sector-neutralize and standardize all factors."""
        factor_cols = [
            "earnings_yield",
            "fcf_yield",
            "ebitda_ev",
            "roic",
            "gross_margin_factor",
            "operating_margin_factor",
            "accruals_factor",
            "momentum_12_1",
            "short_reversion_5d",
            "buyback_yield",
            "dividend_change",
        ]

        for col in factor_cols:
            if col not in df.columns:
                continue

            # Handle missing values by sector median
            df[col] = handle_missing_by_group(
                df, col, self.sector_col, method="median"
            )

            # Z-score by sector
            standardized_col = f"{col}_z"
            df[standardized_col] = zscore_by_group(
                df,
                col,
                self.sector_col,
                winsorize_quantiles=self.winsorize_quantiles,
            )

        return df

    def _compute_composite_score(
        self,
        df: pd.DataFrame,
        weights: Optional[Dict[str, float]] = None,
    ) -> pd.DataFrame:
        """
        Compute composite alpha score from factor families.

        Args:
            df: DataFrame with standardized factors
            weights: Weights for each family (default: equal-weighted)

        Returns:
            DataFrame with composite_alpha column
        """
        if weights is None:
            weights = {
                "value": 0.35,
                "quality": 0.25,
                "momentum": 0.25,
                "short_reversion": 0.15,
            }

        # Value composite
        value_factors = ["earnings_yield_z", "fcf_yield_z", "ebitda_ev_z"]
        value_available = [f for f in value_factors if f in df.columns]
        if value_available:
            df["value_composite"] = df[value_available].mean(axis=1, skipna=True)
        else:
            df["value_composite"] = 0.0

        # Quality composite
        quality_factors = [
            "roic_z",
            "gross_margin_factor_z",
            "operating_margin_factor_z",
            "accruals_factor_z",
        ]
        quality_available = [f for f in quality_factors if f in df.columns]
        if quality_available:
            df["quality_composite"] = df[quality_available].mean(axis=1, skipna=True)
        else:
            df["quality_composite"] = 0.0

        # Momentum composite
        momentum_factors = ["momentum_12_1_z"]
        momentum_available = [f for f in momentum_factors if f in df.columns]
        if momentum_available:
            df["momentum_composite"] = df[momentum_available].mean(axis=1, skipna=True)
        else:
            df["momentum_composite"] = 0.0

        # Short-term reversion composite
        reversion_factors = ["short_reversion_5d_z"]
        reversion_available = [f for f in reversion_factors if f in df.columns]
        if reversion_available:
            df["reversion_composite"] = df[reversion_available].mean(axis=1, skipna=True)
        else:
            df["reversion_composite"] = 0.0

        # Weighted composite
        df["composite_alpha"] = (
            weights.get("value", 0.0) * df["value_composite"]
            + weights.get("quality", 0.0) * df["quality_composite"]
            + weights.get("momentum", 0.0) * df["momentum_composite"]
            + weights.get("short_reversion", 0.0) * df["reversion_composite"]
        )

        return df

    def get_feature_columns(self, standardized: bool = True) -> List[str]:
        """
        Get list of feature columns for ML training.

        Args:
            standardized: Return standardized (_z) columns

        Returns:
            List of feature column names
        """
        base_features = [
            "earnings_yield",
            "fcf_yield",
            "ebitda_ev",
            "roic",
            "gross_margin_factor",
            "operating_margin_factor",
            "accruals_factor",
            "momentum_12_1",
            "short_reversion_5d",
            "buyback_yield",
            "dividend_change",
        ]

        if standardized:
            return [f"{f}_z" for f in base_features]
        else:
            return base_features


def compute_forward_returns(
    df: pd.DataFrame,
    horizon_days: int = 21,
    ticker_col: str = "ticker",
    date_col: str = "date",
    price_col: str = "adj_close",
    sector_col: str = "sector",
) -> pd.DataFrame:
    """
    Compute forward returns for labeling.

    Computes:
    - next_Nd_return: raw forward return
    - next_Nd_excess_vs_sector: sector-relative return

    Args:
        df: DataFrame with prices
        horizon_days: Forward horizon in days
        ticker_col: Ticker column name
        date_col: Date column name
        price_col: Price column name
        sector_col: Sector column name

    Returns:
        DataFrame with forward return labels
    """
    df = df.copy().sort_values([ticker_col, date_col])

    # Compute raw forward return
    df[f"next_{horizon_days}d_return"] = df.groupby(ticker_col)[price_col].transform(
        lambda x: x.pct_change(periods=horizon_days).shift(-horizon_days)
    )

    # Compute sector average forward return
    df[f"sector_next_{horizon_days}d_return"] = df.groupby([date_col, sector_col])[
        f"next_{horizon_days}d_return"
    ].transform("mean")

    # Excess return vs sector
    df[f"next_{horizon_days}d_excess_vs_sector"] = (
        df[f"next_{horizon_days}d_return"] - df[f"sector_next_{horizon_days}d_return"]
    )

    return df
