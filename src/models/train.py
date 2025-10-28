"""Model training with purged walk-forward cross-validation."""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

import pandas as pd
import numpy as np
from loguru import logger

from sklearn.metrics import mean_squared_error, mean_absolute_error
from scipy.stats import spearmanr, pearsonr

try:
    import lightgbm as lgb
except ImportError:
    lgb = None
    logger.warning("LightGBM not available")

try:
    import catboost as cb
except ImportError:
    cb = None
    logger.warning("CatBoost not available")

from src.models.baselines import BaselineLinearModel


@dataclass
class CVFold:
    """Represents a single cross-validation fold."""
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    purge_days: int
    embargo_days: int


class WalkForwardCV:
    """
    Walk-forward cross-validation with purging and embargo.

    Prevents data leakage by:
    - Purging overlapping samples between train and test
    - Adding embargo period after test set
    """

    def __init__(
        self,
        train_period_days: int = 252,
        test_period_days: int = 63,
        purge_days: int = 21,
        embargo_days: int = 21,
        date_col: str = "date",
    ):
        """
        Initialize walk-forward CV.

        Args:
            train_period_days: Training window size in days
            test_period_days: Test window size in days
            purge_days: Days to purge between train/test
            embargo_days: Days to embargo after test
            date_col: Date column name
        """
        self.train_period_days = train_period_days
        self.test_period_days = test_period_days
        self.purge_days = purge_days
        self.embargo_days = embargo_days
        self.date_col = date_col

        logger.info(
            f"Initialized WalkForwardCV: train={train_period_days}d, "
            f"test={test_period_days}d, purge={purge_days}d, embargo={embargo_days}d"
        )

    def generate_folds(
        self,
        df: pd.DataFrame,
        start_date: Optional[pd.Timestamp] = None,
        end_date: Optional[pd.Timestamp] = None,
    ) -> List[CVFold]:
        """
        Generate walk-forward CV folds.

        Args:
            df: DataFrame with date column
            start_date: Earliest date to consider
            end_date: Latest date to consider

        Returns:
            List of CV folds
        """
        dates = pd.to_datetime(df[self.date_col]).sort_values().unique()

        if start_date:
            dates = dates[dates >= pd.to_datetime(start_date)]
        if end_date:
            dates = dates[dates <= pd.to_datetime(end_date)]

        if len(dates) < self.train_period_days + self.test_period_days:
            logger.warning("Insufficient data for walk-forward CV")
            return []

        folds = []

        # Start with first training window
        train_start_idx = 0

        while True:
            # Training window
            train_end_idx = train_start_idx + self.train_period_days
            if train_end_idx >= len(dates):
                break

            train_start = dates[train_start_idx]
            train_end = dates[train_end_idx - 1]

            # Test window (after purge)
            test_start_idx = train_end_idx + self.purge_days
            test_end_idx = test_start_idx + self.test_period_days

            if test_end_idx >= len(dates):
                break

            test_start = dates[test_start_idx]
            test_end = dates[test_end_idx - 1]

            fold = CVFold(
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                purge_days=self.purge_days,
                embargo_days=self.embargo_days,
            )

            folds.append(fold)

            # Move to next fold (considering embargo)
            train_start_idx = test_end_idx + self.embargo_days

        logger.info(f"Generated {len(folds)} walk-forward CV folds")

        return folds

    def split(
        self, df: pd.DataFrame, fold: CVFold
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split data into train and test for a fold.

        Args:
            df: Full dataset
            fold: CV fold specification

        Returns:
            (train_df, test_df)
        """
        dates = pd.to_datetime(df[self.date_col])

        train_mask = (dates >= fold.train_start) & (dates <= fold.train_end)
        test_mask = (dates >= fold.test_start) & (dates <= fold.test_end)

        train_df = df[train_mask].copy()
        test_df = df[test_mask].copy()

        return train_df, test_df


class QuantModel:
    """
    Quantitative model with walk-forward CV.

    Supports:
    - LightGBM regressor
    - CatBoost regressor
    - ElasticNet baseline
    """

    def __init__(
        self,
        model_type: str = "lightgbm_regressor",
        params: Optional[Dict[str, Any]] = None,
        feature_cols: Optional[List[str]] = None,
        target_col: str = "next_21d_excess_vs_sector",
        date_col: str = "date",
        random_state: int = 42,
    ):
        """
        Initialize model.

        Args:
            model_type: Model type ('lightgbm_regressor', 'catboost_regressor', 'elasticnet')
            params: Model hyperparameters
            feature_cols: List of feature columns
            target_col: Target column name
            date_col: Date column name
            random_state: Random seed
        """
        self.model_type = model_type
        self.params = params or {}
        self.feature_cols = feature_cols
        self.target_col = target_col
        self.date_col = date_col
        self.random_state = random_state

        self.model = None
        self.baseline_model = None

        # Ensure random state is set
        if "random_state" not in self.params and "random_seed" not in self.params:
            self.params["random_state"] = random_state

        logger.info(f"Initialized QuantModel: {model_type}")

    def train(
        self,
        train_df: pd.DataFrame,
        valid_df: Optional[pd.DataFrame] = None,
    ) -> "QuantModel":
        """
        Train the model.

        Args:
            train_df: Training data
            valid_df: Validation data (optional, for early stopping)

        Returns:
            Self
        """
        # Prepare data
        X_train, y_train = self._prepare_data(train_df)

        if X_train.empty or len(y_train) == 0:
            logger.error("Empty training data")
            raise ValueError("Empty training data")

        # Remove samples with missing target
        valid_mask = y_train.notna()
        X_train = X_train[valid_mask]
        y_train = y_train[valid_mask]

        logger.info(f"Training on {len(X_train)} samples with {len(self.feature_cols)} features")

        # Train based on model type
        if self.model_type == "lightgbm_regressor":
            self._train_lightgbm(X_train, y_train, valid_df)

        elif self.model_type == "catboost_regressor":
            self._train_catboost(X_train, y_train, valid_df)

        elif self.model_type == "elasticnet":
            self._train_elasticnet(X_train, y_train)

        else:
            raise ValueError(f"Unknown model type: {self.model_type}")

        return self

    def _train_lightgbm(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        valid_df: Optional[pd.DataFrame] = None,
    ) -> None:
        """Train LightGBM model."""
        if lgb is None:
            raise ImportError("LightGBM not installed")

        train_data = lgb.Dataset(X_train, label=y_train)

        # Validation data for early stopping
        valid_sets = [train_data]
        if valid_df is not None:
            X_valid, y_valid = self._prepare_data(valid_df)
            valid_mask = y_valid.notna()
            X_valid = X_valid[valid_mask]
            y_valid = y_valid[valid_mask]
            if not X_valid.empty:
                valid_data = lgb.Dataset(X_valid, label=y_valid, reference=train_data)
                valid_sets.append(valid_data)

        # Train
        self.model = lgb.train(
            self.params,
            train_data,
            valid_sets=valid_sets,
            callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
        )

        logger.info("LightGBM training completed")

    def _train_catboost(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        valid_df: Optional[pd.DataFrame] = None,
    ) -> None:
        """Train CatBoost model."""
        if cb is None:
            raise ImportError("CatBoost not installed")

        # Prepare eval set
        eval_set = None
        if valid_df is not None:
            X_valid, y_valid = self._prepare_data(valid_df)
            valid_mask = y_valid.notna()
            X_valid = X_valid[valid_mask]
            y_valid = y_valid[valid_mask]
            if not X_valid.empty:
                eval_set = (X_valid, y_valid)

        self.model = cb.CatBoostRegressor(**self.params)
        self.model.fit(
            X_train,
            y_train,
            eval_set=eval_set,
            early_stopping_rounds=50,
            verbose=False,
        )

        logger.info("CatBoost training completed")

    def _train_elasticnet(self, X_train: pd.DataFrame, y_train: pd.Series) -> None:
        """Train ElasticNet model."""
        self.model = BaselineLinearModel(**self.params)
        self.model.fit(X_train, y_train)

        logger.info("ElasticNet training completed")

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """
        Make predictions.

        Args:
            df: Input data

        Returns:
            Predictions
        """
        X, _ = self._prepare_data(df)

        if X.empty:
            return np.array([])

        if self.model is None:
            raise ValueError("Model not trained")

        return self.model.predict(X)

    def _prepare_data(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare features and target.

        Args:
            df: Input DataFrame

        Returns:
            (X, y)
        """
        # Auto-detect feature columns if not specified
        if self.feature_cols is None:
            self.feature_cols = [
                col for col in df.columns
                if col.endswith("_z") or col in ["composite_alpha"]
            ]
            logger.info(f"Auto-detected {len(self.feature_cols)} feature columns")

        # Extract features
        X = df[self.feature_cols].copy()
        X = X.fillna(0)  # Simple imputation

        # Extract target
        if self.target_col in df.columns:
            y = df[self.target_col].copy()
        else:
            y = pd.Series(index=df.index, dtype=float)

        return X, y

    def evaluate(
        self, df: pd.DataFrame, prefix: str = "test"
    ) -> Dict[str, float]:
        """
        Evaluate model on a dataset.

        Args:
            df: Evaluation data
            prefix: Metric prefix

        Returns:
            Dictionary of metrics
        """
        X, y_true = self._prepare_data(df)

        # Filter valid samples
        valid_mask = y_true.notna()
        X = X[valid_mask]
        y_true = y_true[valid_mask]

        if len(y_true) < 10:
            logger.warning("Insufficient samples for evaluation")
            return {}

        # Predictions
        y_pred = self.predict(df.loc[valid_mask])

        # Metrics
        metrics = {}

        # Regression metrics
        metrics[f"{prefix}_mse"] = mean_squared_error(y_true, y_pred)
        metrics[f"{prefix}_mae"] = mean_absolute_error(y_true, y_pred)
        metrics[f"{prefix}_rmse"] = np.sqrt(metrics[f"{prefix}_mse"])

        # Correlation metrics (IC)
        ic, ic_pval = spearmanr(y_true, y_pred)
        metrics[f"{prefix}_ic"] = ic
        metrics[f"{prefix}_ic_pval"] = ic_pval

        pearson_ic, pearson_pval = pearsonr(y_true, y_pred)
        metrics[f"{prefix}_pearson_ic"] = pearson_ic

        # Rank metrics
        y_true_rank = y_true.rank()
        y_pred_rank = pd.Series(y_pred, index=y_true.index).rank()
        rank_ic, _ = spearmanr(y_true_rank, y_pred_rank)
        metrics[f"{prefix}_rank_ic"] = rank_ic

        return metrics

    def get_feature_importance(self) -> Optional[pd.DataFrame]:
        """Get feature importances."""
        if self.model is None:
            return None

        if self.model_type == "lightgbm_regressor":
            importance = self.model.feature_importance(importance_type="gain")
            importance_df = pd.DataFrame(
                {
                    "feature": self.feature_cols,
                    "importance": importance,
                }
            )
            importance_df = importance_df.sort_values("importance", ascending=False)
            return importance_df

        elif self.model_type == "catboost_regressor":
            importance = self.model.feature_importances_
            importance_df = pd.DataFrame(
                {
                    "feature": self.feature_cols,
                    "importance": importance,
                }
            )
            importance_df = importance_df.sort_values("importance", ascending=False)
            return importance_df

        elif self.model_type == "elasticnet":
            return self.model.get_feature_importance(self.feature_cols)

        return None


def train_with_cv(
    df: pd.DataFrame,
    model_config: Dict[str, Any],
    cv_config: Dict[str, Any],
    feature_cols: List[str],
    target_col: str = "next_21d_excess_vs_sector",
    date_col: str = "date",
) -> Tuple[QuantModel, pd.DataFrame]:
    """
    Train model with walk-forward cross-validation.

    Args:
        df: Full dataset
        model_config: Model configuration
        cv_config: CV configuration
        feature_cols: Feature columns
        target_col: Target column
        date_col: Date column

    Returns:
        (trained_model, cv_results_df)
    """
    # Initialize CV
    cv = WalkForwardCV(
        train_period_days=cv_config.get("train_period_days", 252),
        test_period_days=cv_config.get("test_period_days", 63),
        purge_days=cv_config.get("purge_days", 21),
        embargo_days=cv_config.get("embargo_days", 21),
        date_col=date_col,
    )

    # Generate folds
    folds = cv.generate_folds(df)

    if not folds:
        logger.error("No CV folds generated")
        raise ValueError("Insufficient data for CV")

    # Train on each fold and collect metrics
    cv_results = []

    for i, fold in enumerate(folds):
        logger.info(f"Training fold {i+1}/{len(folds)}: {fold.test_start} to {fold.test_end}")

        # Split data
        train_df, test_df = cv.split(df, fold)

        # Initialize model
        model = QuantModel(
            model_type=model_config.get("type", "lightgbm_regressor"),
            params=model_config.get("params", {}),
            feature_cols=feature_cols,
            target_col=target_col,
            date_col=date_col,
            random_state=model_config.get("params", {}).get("random_state", 42),
        )

        # Train
        model.train(train_df)

        # Evaluate on test set
        test_metrics = model.evaluate(test_df, prefix="test")
        train_metrics = model.evaluate(train_df, prefix="train")

        result = {
            "fold": i,
            "test_start": fold.test_start,
            "test_end": fold.test_end,
            "train_samples": len(train_df),
            "test_samples": len(test_df),
            **train_metrics,
            **test_metrics,
        }

        cv_results.append(result)

        logger.info(f"Fold {i+1} - Test IC: {test_metrics.get('test_ic', 0.0):.4f}")

    # Train final model on all data (optional: last N periods)
    logger.info("Training final model on full data...")
    final_model = QuantModel(
        model_type=model_config.get("type", "lightgbm_regressor"),
        params=model_config.get("params", {}),
        feature_cols=feature_cols,
        target_col=target_col,
        date_col=date_col,
        random_state=model_config.get("params", {}).get("random_state", 42),
    )
    final_model.train(df)

    cv_results_df = pd.DataFrame(cv_results)

    logger.info(f"CV completed. Mean test IC: {cv_results_df['test_ic'].mean():.4f}")

    return final_model, cv_results_df
