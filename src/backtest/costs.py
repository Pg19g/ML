"""Transaction cost models."""

from typing import Dict, Optional

import pandas as pd
import numpy as np
from loguru import logger


class TransactionCostModel:
    """
    Transaction cost model with multiple components.

    Components:
    - Fixed bps per side
    - Spread proxy
    - Market impact (optional)
    """

    def __init__(
        self,
        bps_per_side: float = 5.0,
        spread_proxy_bps: float = 3.0,
        use_market_impact: bool = False,
        impact_coef: float = 0.1,
    ):
        """
        Initialize cost model.

        Args:
            bps_per_side: Fixed cost in bps per side
            spread_proxy_bps: Bid-ask spread proxy in bps
            use_market_impact: Whether to model market impact
            impact_coef: Market impact coefficient
        """
        self.bps_per_side = bps_per_side
        self.spread_proxy_bps = spread_proxy_bps
        self.use_market_impact = use_market_impact
        self.impact_coef = impact_coef

        self.total_bps = bps_per_side + spread_proxy_bps

        logger.info(
            f"Initialized TransactionCostModel: {self.total_bps:.1f} bps total "
            f"({bps_per_side:.1f} fixed + {spread_proxy_bps:.1f} spread)"
        )

    def compute_costs(
        self,
        trades: pd.Series,
        prices: pd.Series,
        volumes: Optional[pd.Series] = None,
        portfolio_value: float = 1.0,
    ) -> pd.Series:
        """
        Compute transaction costs for trades.

        Args:
            trades: Trade weights (positive = buy, negative = sell)
            prices: Current prices
            volumes: Trading volumes (for market impact)
            portfolio_value: Total portfolio value

        Returns:
            Cost per ticker (positive = cost)
        """
        # Align series
        tickers = trades.index
        trades = trades.reindex(tickers).fillna(0)
        prices = prices.reindex(tickers).fillna(0)

        # Filter valid prices
        valid_mask = prices > 0
        trades = trades[valid_mask]
        prices = prices[valid_mask]

        if volumes is not None:
            volumes = volumes.reindex(trades.index).fillna(0)

        # Trade notional
        trade_notional = np.abs(trades) * portfolio_value

        # Fixed + spread cost
        fixed_costs = trade_notional * (self.total_bps / 10000)

        # Market impact (optional)
        if self.use_market_impact and volumes is not None:
            # Simple square-root impact model
            # Impact ∝ sqrt(trade_size / daily_volume)
            daily_dollar_volume = volumes * prices
            daily_dollar_volume = daily_dollar_volume.replace(0, np.nan)

            impact_ratio = trade_notional / daily_dollar_volume
            impact_ratio = impact_ratio.fillna(0).clip(0, 1)  # Cap at 100% of ADV

            market_impact = (
                self.impact_coef * np.sqrt(impact_ratio) * trade_notional
            )

            total_costs = fixed_costs + market_impact
        else:
            total_costs = fixed_costs

        return total_costs

    def compute_total_cost(
        self,
        trades: pd.Series,
        prices: pd.Series,
        volumes: Optional[pd.Series] = None,
        portfolio_value: float = 1.0,
    ) -> float:
        """
        Compute total transaction cost.

        Args:
            trades: Trade weights
            prices: Current prices
            volumes: Trading volumes
            portfolio_value: Portfolio value

        Returns:
            Total cost (positive)
        """
        costs = self.compute_costs(trades, prices, volumes, portfolio_value)
        return costs.sum()

    def analyze_cost_sensitivity(
        self,
        trades: pd.Series,
        prices: pd.Series,
        portfolio_value: float = 1.0,
        multipliers: list = [0.5, 1.0, 2.0],
    ) -> Dict[str, float]:
        """
        Analyze sensitivity to cost assumptions.

        Args:
            trades: Trade weights
            prices: Current prices
            portfolio_value: Portfolio value
            multipliers: Cost multipliers to test

        Returns:
            Dictionary of costs at different multipliers
        """
        results = {}

        for mult in multipliers:
            # Temporarily adjust costs
            original_bps = self.bps_per_side
            original_spread = self.spread_proxy_bps

            self.bps_per_side *= mult
            self.spread_proxy_bps *= mult
            self.total_bps = self.bps_per_side + self.spread_proxy_bps

            cost = self.compute_total_cost(trades, prices, portfolio_value=portfolio_value)

            results[f"cost_{mult}x"] = cost

            # Restore original
            self.bps_per_side = original_bps
            self.spread_proxy_bps = original_spread
            self.total_bps = self.bps_per_side + self.spread_proxy_bps

        return results
