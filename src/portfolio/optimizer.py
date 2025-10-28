"""Portfolio optimizer with sector neutrality and constraints."""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import pandas as pd
import numpy as np
from loguru import logger

try:
    import cvxpy as cp
except ImportError:
    cp = None
    logger.warning("CVXPY not available")


@dataclass
class PortfolioConstraints:
    """Portfolio construction constraints."""
    long_pct: float = 0.20  # Top 20% long
    short_pct: float = 0.20  # Bottom 20% short
    target_beta: float = 0.0
    beta_tolerance: float = 0.05
    sector_max_weight: float = 0.15
    single_name_max_weight: float = 0.05
    gross_leverage: float = 2.0
    net_exposure_target: float = 0.0
    turnover_penalty: float = 0.01


class PortfolioOptimizer:
    """
    Portfolio optimizer with sector neutrality and risk controls.

    Features:
    - Score-based tilting
    - Sector exposure constraints
    - Position size constraints
    - Turnover penalty
    - Beta neutrality
    - PCA-based risk model (optional)
    """

    def __init__(
        self,
        constraints: PortfolioConstraints,
        use_pca_risk: bool = True,
        pca_components: int = 20,
        risk_aversion: float = 1.0,
    ):
        """
        Initialize optimizer.

        Args:
            constraints: Portfolio constraints
            use_pca_risk: Use PCA-based risk model
            pca_components: Number of PCA components
            risk_aversion: Risk aversion parameter (lambda_risk)
        """
        self.constraints = constraints
        self.use_pca_risk = use_pca_risk
        self.pca_components = pca_components
        self.risk_aversion = risk_aversion

        if cp is None:
            raise ImportError("CVXPY is required for portfolio optimization")

        logger.info("Initialized PortfolioOptimizer")

    def optimize(
        self,
        scores: pd.Series,
        sectors: pd.Series,
        prices: pd.Series,
        prev_weights: Optional[pd.Series] = None,
        returns_history: Optional[pd.DataFrame] = None,
        betas: Optional[pd.Series] = None,
    ) -> pd.Series:
        """
        Optimize portfolio weights.

        Args:
            scores: Alpha scores (higher is better)
            sectors: Sector labels
            prices: Current prices (for validation)
            prev_weights: Previous portfolio weights (for turnover penalty)
            returns_history: Historical returns for risk model
            betas: Stock betas (for beta neutrality)

        Returns:
            Portfolio weights (Series indexed by ticker)
        """
        # Validate inputs
        if len(scores) == 0:
            logger.warning("Empty scores provided")
            return pd.Series(dtype=float)

        # Align all series
        tickers = scores.index
        scores = scores.reindex(tickers).fillna(0)
        sectors = sectors.reindex(tickers).fillna("Unknown")
        prices = prices.reindex(tickers)

        # Filter out invalid prices
        valid_mask = prices > 0
        tickers = tickers[valid_mask]
        scores = scores[valid_mask]
        sectors = sectors[valid_mask]
        prices = prices[valid_mask]

        if len(tickers) < 10:
            logger.warning("Too few valid tickers for optimization")
            return pd.Series(dtype=float)

        n = len(tickers)

        # Normalize scores to sum to zero (market neutral)
        scores = scores - scores.mean()

        # Define optimization variable
        w = cp.Variable(n)

        # Objective: maximize score-weighted portfolio
        objective = scores.values @ w

        # Turnover penalty
        if prev_weights is not None:
            prev_weights = prev_weights.reindex(tickers).fillna(0).values
            turnover = cp.sum(cp.abs(w - prev_weights))
            objective -= self.constraints.turnover_penalty * turnover

        # Risk penalty (optional)
        if self.use_pca_risk and returns_history is not None:
            cov_matrix = self._compute_covariance(returns_history, tickers)
            if cov_matrix is not None:
                risk = cp.quad_form(w, cov_matrix)
                objective -= self.risk_aversion * risk

        # Constraints
        constraints = []

        # 1. Gross leverage constraint
        constraints.append(cp.sum(cp.abs(w)) <= self.constraints.gross_leverage)

        # 2. Net exposure constraint (market neutral)
        net_tol = 0.1  # 10% tolerance
        constraints.append(cp.sum(w) >= self.constraints.net_exposure_target - net_tol)
        constraints.append(cp.sum(w) <= self.constraints.net_exposure_target + net_tol)

        # 3. Position size constraints
        constraints.append(w <= self.constraints.single_name_max_weight)
        constraints.append(w >= -self.constraints.single_name_max_weight)

        # 4. Sector exposure constraints
        unique_sectors = sectors.unique()
        for sector in unique_sectors:
            sector_mask = (sectors == sector).values
            sector_exposure = cp.sum(w[sector_mask])
            constraints.append(sector_exposure <= self.constraints.sector_max_weight)
            constraints.append(sector_exposure >= -self.constraints.sector_max_weight)

        # 5. Beta neutrality (if betas provided)
        if betas is not None:
            betas = betas.reindex(tickers).fillna(1.0).values
            portfolio_beta = betas @ w
            constraints.append(portfolio_beta >= self.constraints.target_beta - self.constraints.beta_tolerance)
            constraints.append(portfolio_beta <= self.constraints.target_beta + self.constraints.beta_tolerance)

        # Solve optimization problem
        problem = cp.Problem(cp.Maximize(objective), constraints)

        try:
            problem.solve(solver=cp.OSQP, verbose=False)

            if problem.status not in ["optimal", "optimal_inaccurate"]:
                logger.warning(f"Optimization status: {problem.status}")
                # Fallback to simple ranking
                return self._fallback_weights(scores, sectors)

            weights = pd.Series(w.value, index=tickers)

            # Post-process: zero out very small weights
            weights[np.abs(weights) < 1e-4] = 0.0

            logger.info(
                f"Optimized portfolio: {len(weights[weights != 0])} positions, "
                f"gross leverage: {np.abs(weights).sum():.2f}, "
                f"net exposure: {weights.sum():.3f}"
            )

            return weights

        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            return self._fallback_weights(scores, sectors)

    def _fallback_weights(
        self, scores: pd.Series, sectors: pd.Series
    ) -> pd.Series:
        """
        Fallback to simple ranking-based weights if optimization fails.

        Args:
            scores: Alpha scores
            sectors: Sector labels

        Returns:
            Simple long/short weights
        """
        logger.info("Using fallback ranking-based weights")

        n = len(scores)
        weights = pd.Series(0.0, index=scores.index)

        # Rank by score
        ranks = scores.rank(method="first")

        # Long top N%
        long_threshold = n * (1 - self.constraints.long_pct)
        long_mask = ranks > long_threshold
        if long_mask.sum() > 0:
            weights[long_mask] = 1.0 / long_mask.sum()

        # Short bottom N%
        short_threshold = n * self.constraints.short_pct
        short_mask = ranks <= short_threshold
        if short_mask.sum() > 0:
            weights[short_mask] = -1.0 / short_mask.sum()

        # Normalize to target gross leverage
        current_gross = np.abs(weights).sum()
        if current_gross > 0:
            weights = weights * (self.constraints.gross_leverage / current_gross)

        return weights

    def _compute_covariance(
        self, returns: pd.DataFrame, tickers: pd.Index
    ) -> Optional[np.ndarray]:
        """
        Compute covariance matrix for risk model.

        Uses PCA for dimensionality reduction if enabled.

        Args:
            returns: Historical returns DataFrame
            tickers: Tickers to include

        Returns:
            Covariance matrix or None
        """
        # Filter returns to relevant tickers
        available_tickers = returns.columns.intersection(tickers)

        if len(available_tickers) < 10:
            logger.warning("Insufficient tickers for covariance estimation")
            return None

        returns_subset = returns[available_tickers].fillna(0)

        if self.use_pca_risk and len(returns_subset) > self.pca_components:
            # Use PCA-based covariance
            from sklearn.decomposition import PCA

            pca = PCA(n_components=min(self.pca_components, len(available_tickers) - 1))
            pca.fit(returns_subset)

            # Reconstruct covariance
            components = pca.components_
            explained_var = pca.explained_variance_

            cov_pca = (components.T * explained_var) @ components

            # Add idiosyncratic risk
            residual_var = returns_subset.var() - np.diag(cov_pca)
            residual_var = np.maximum(residual_var, 0)  # Ensure non-negative

            cov_matrix = cov_pca + np.diag(residual_var)

        else:
            # Simple sample covariance
            cov_matrix = returns_subset.cov().values

        # Ensure positive semi-definite
        eigenvalues = np.linalg.eigvalsh(cov_matrix)
        if eigenvalues.min() < 0:
            cov_matrix += np.eye(cov_matrix.shape[0]) * abs(eigenvalues.min()) * 1.1

        # Reindex to match tickers order
        ticker_to_idx = {ticker: i for i, ticker in enumerate(available_tickers)}
        reindexed_cov = np.zeros((len(tickers), len(tickers)))

        for i, ticker_i in enumerate(tickers):
            if ticker_i not in ticker_to_idx:
                # Missing ticker: use diagonal element only
                reindexed_cov[i, i] = returns_subset.var().mean()
                continue

            for j, ticker_j in enumerate(tickers):
                if ticker_j not in ticker_to_idx:
                    continue

                idx_i = ticker_to_idx[ticker_i]
                idx_j = ticker_to_idx[ticker_j]
                reindexed_cov[i, j] = cov_matrix[idx_i, idx_j]

        return reindexed_cov

    def compute_portfolio_metrics(
        self,
        weights: pd.Series,
        scores: pd.Series,
        sectors: pd.Series,
        prev_weights: Optional[pd.Series] = None,
    ) -> Dict[str, float]:
        """
        Compute portfolio metrics.

        Args:
            weights: Portfolio weights
            scores: Alpha scores
            sectors: Sector labels
            prev_weights: Previous weights (for turnover)

        Returns:
            Dictionary of metrics
        """
        metrics = {}

        # Basic metrics
        metrics["num_positions"] = (weights != 0).sum()
        metrics["num_long"] = (weights > 0).sum()
        metrics["num_short"] = (weights < 0).sum()
        metrics["gross_leverage"] = np.abs(weights).sum()
        metrics["net_exposure"] = weights.sum()

        # Score-weighted exposure
        if len(scores) > 0:
            aligned_scores = scores.reindex(weights.index).fillna(0)
            metrics["score_weighted_exposure"] = (weights * aligned_scores).sum()

        # Turnover
        if prev_weights is not None:
            prev_weights = prev_weights.reindex(weights.index).fillna(0)
            metrics["turnover"] = np.abs(weights - prev_weights).sum()
        else:
            metrics["turnover"] = np.abs(weights).sum()

        # Sector exposures
        aligned_sectors = sectors.reindex(weights.index).fillna("Unknown")
        sector_exposures = weights.groupby(aligned_sectors).sum()
        metrics["max_sector_exposure"] = sector_exposures.abs().max()

        # Concentration
        metrics["max_position_weight"] = weights.abs().max()
        metrics["herfindahl_index"] = (weights ** 2).sum()

        return metrics
