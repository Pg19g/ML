"""Baseline linear models for comparison."""

from typing import Dict, Any, Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet, Ridge
from sklearn.preprocessing import StandardScaler
from loguru import logger


class BaselineLinearModel:
    """
    Baseline linear model (ElasticNet or Ridge).

    Serves as a transparent, interpretable benchmark.
    """

    def __init__(
        self,
        model_type: str = "elasticnet",
        alpha: float = 0.01,
        l1_ratio: float = 0.5,
        random_state: int = 42,
        **kwargs,
    ):
        """
        Initialize baseline model.

        Args:
            model_type: 'elasticnet' or 'ridge'
            alpha: Regularization strength
            l1_ratio: L1 ratio for ElasticNet
            random_state: Random seed
            **kwargs: Additional model parameters
        """
        self.model_type = model_type
        self.alpha = alpha
        self.l1_ratio = l1_ratio
        self.random_state = random_state
        self.kwargs = kwargs

        self.scaler = StandardScaler()

        if model_type == "elasticnet":
            self.model = ElasticNet(
                alpha=alpha,
                l1_ratio=l1_ratio,
                random_state=random_state,
                max_iter=1000,
                **kwargs,
            )
        elif model_type == "ridge":
            self.model = Ridge(
                alpha=alpha,
                random_state=random_state,
                max_iter=1000,
                **kwargs,
            )
        else:
            raise ValueError(f"Unknown model_type: {model_type}")

        logger.info(f"Initialized {model_type} baseline model")

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "BaselineLinearModel":
        """
        Fit the model.

        Args:
            X: Feature matrix
            y: Target variable

        Returns:
            Self
        """
        # Handle missing values
        X = X.fillna(0)

        # Scale features
        X_scaled = self.scaler.fit_transform(X)

        # Fit model
        self.model.fit(X_scaled, y)

        logger.info(f"Fitted {self.model_type} with {X.shape[1]} features on {X.shape[0]} samples")

        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Make predictions.

        Args:
            X: Feature matrix

        Returns:
            Predictions
        """
        X = X.fillna(0)
        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)

    def get_feature_importance(self, feature_names: list) -> pd.DataFrame:
        """
        Get feature importances (coefficients).

        Args:
            feature_names: List of feature names

        Returns:
            DataFrame with feature importances
        """
        coefficients = self.model.coef_

        importance_df = pd.DataFrame(
            {
                "feature": feature_names,
                "coefficient": coefficients,
                "abs_coefficient": np.abs(coefficients),
            }
        )

        importance_df = importance_df.sort_values("abs_coefficient", ascending=False)

        return importance_df

    def get_params(self) -> Dict[str, Any]:
        """Get model parameters."""
        return {
            "model_type": self.model_type,
            "alpha": self.alpha,
            "l1_ratio": self.l1_ratio,
            "random_state": self.random_state,
            **self.kwargs,
        }
