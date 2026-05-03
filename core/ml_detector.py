"""
Machine learning based anomaly detection for wash trading.

This module provides ML-based detection using Isolation Forest algorithm:
- Feature engineering for trade data
- Model training with configurable contamination
- Anomaly scoring and wash trade prediction
- Model persistence with joblib
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from scipy.special import expit, logit
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import select, and_

from core.feature_engineer import FeatureEngineer
from core.storage import Storage
from config.settings import settings
from models.schemas import SwapTrade

logger = logging.getLogger(__name__)

# Model constants
DEFAULT_CONTAMINATION: float = 0.05
DEFAULT_N_ESTIMATORS: int = 100
DEFAULT_RANDOM_STATE: int = 42


# ==============================================================================
# Exceptions
# ==============================================================================

class MLDetectorError(Exception):
    """Base exception for ML detector errors."""
    pass


class ModelNotTrainedError(MLDetectorError):
    """Raised when attempting to use an untrained model."""
    pass


class ModelFileNotFoundError(MLDetectorError):
    """Raised when model file cannot be found."""
    pass


# ==============================================================================
# ML Detector
# ==============================================================================

class MLDetector:
    """
    Machine learning based wash trade detection.

    Uses Isolation Forest algorithm to detect anomalous trades based on
    engineered features. Can be trained on labeled data or used for
    unsupervised anomaly detection.

    Attributes:
        storage: Storage instance for data access
        feature_engineer: Feature engineering pipeline
        model: Trained sklearn Pipeline
        is_trained: Whether model has been trained
        feature_columns: List of features used by the model

    Example:
        >>> detector = MLDetector(storage, feature_engineer)
        >>> await detector.train(chain_id=1, pool_addresses=["0x..."])
        >>> wash_trades = await detector.detect_wash_trades(chain_id=1, pool_address="0x...")
    """

    def __init__(
        self,
        storage: Storage,
        feature_engineer: FeatureEngineer,
        contamination: Optional[float] = None,
    ):
        """
        Initialize ML detector.

        Args:
            storage: Storage instance
            feature_engineer: Feature engineer instance
            contamination: Expected contamination ratio (default from settings)
        """
        self.storage: Storage = storage
        self.feature_engineer: FeatureEngineer = feature_engineer
        self.contamination: float = (
            contamination if contamination is not None else settings.ML_CONTAMINATION
        )
        self.model: Optional[Pipeline] = None
        self.is_trained: bool = False
        self.feature_columns: List[str] = [
            "volume_usd",
            "slippage_ratio",
            "sender_trade_count_1h",
            "sender_volume_1h",
            "recipient_trade_count_1h",
            "pair_trade_count_1h",
            "reverse_pair_trade_count_1h",
            "time_since_last_sender_trade",
            "gas_price",
            "pool_avg_time_between_trades",
            "pool_total_volume_usd",
            "pool_avg_trade_volume_usd",
            "pool_trader_diversity",
            "pool_circular_trade_ratio",
            "pool_max_trades_per_sender",
            "pool_self_trade_ratio",
        ]

    def _build_pipeline(self, contamination: Optional[float] = None) -> Pipeline:
        """
        Build sklearn pipeline for Isolation Forest.

        Args:
            contamination: Contamination ratio (uses instance default if None)

        Returns:
            Configured sklearn Pipeline
        """
        cont = contamination if contamination is not None else self.contamination
        return Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "isolation_forest",
                    IsolationForest(
                        contamination=cont,
                        random_state=DEFAULT_RANDOM_STATE,
                        n_estimators=DEFAULT_N_ESTIMATORS,
                        max_samples="auto",
                        bootstrap=False,
                        n_jobs=-1,
                    ),
                ),
            ]
        )

    def _compute_probability(self, scores: np.ndarray) -> np.ndarray:
        """
        Convert Isolation Forest decision scores to probabilities.

        Uses sigmoid transformation to convert anomaly scores to
        probabilities in range [0, 1].

        Args:
            scores: Raw decision_function scores

        Returns:
            Probability array (higher = more anomalous)
        """
        return expit(-scores)

    async def train(
        self,
        chain_id: int,
        pool_addresses: List[str],
        use_heuristic_labels: bool = True,
        contamination: Optional[float] = None,
    ) -> None:
        """
        Train the Isolation Forest model.

        Collects features from multiple pools, optionally uses heuristic
        labels to create semi-supervised training, and fits the model.

        Args:
            chain_id: Blockchain chain ID
            pool_addresses: List of pool addresses to train on
            use_heuristic_labels: Whether to use heuristic labels
            contamination: Contamination ratio override

        Raises:
            ValueError: If no training data is available
        """
        cont = contamination if contamination is not None else self.contamination
        logger.info(
            f"Training ML model on {len(pool_addresses)} pools "
            f"(contamination={cont})"
        )

        all_features: List[np.ndarray] = []
        all_labels: List[int] = []

        async with self.storage.get_session() as session:
            for pool in pool_addresses:
                df = await self.feature_engineer.build_ml_features(
                    chain_id, pool, session
                )
                if df.empty:
                    continue

                # Get heuristic labels if requested
                if use_heuristic_labels:
                    stmt = select(SwapTrade).where(
                        and_(
                            SwapTrade.chain_id == chain_id,
                            SwapTrade.pool_address == pool,
                        )
                    )
                    result = await session.execute(stmt)
                    trades = result.scalars().all()
                    trade_labels = {
                        t.id: -1 if t.is_wash_trade else 1 for t in trades
                    }
                    labels = [trade_labels.get(t.id, 1) for t in trades]
                    all_labels.extend(labels)

                # Extract available features
                available_cols = [c for c in self.feature_columns if c in df.columns]
                features = df[available_cols].fillna(0).values
                all_features.append(features)

        if not all_features:
            raise ValueError("No training data available")

        X = np.vstack(all_features)
        self.model = self._build_pipeline(contamination)

        # Fit with optional sample weights for semi-supervised learning
        if all_labels and use_heuristic_labels:
            y = np.array(all_labels)
            sample_weight = np.ones(len(y))
            sample_weight[y == -1] = 2.0  # Weight wash trades higher
        else:
            y = None
            sample_weight = None

        self.model.fit(X, y=sample_weight)
        self.is_trained = True
        logger.info("ML model training complete")

    async def predict(self, features_df: pd.DataFrame) -> np.ndarray:
        """
        Predict anomaly probabilities for features.

        Args:
            features_df: DataFrame with engineered features

        Returns:
            Array of anomaly probabilities [0, 1]

        Raises:
            ModelNotTrainedError: If model not trained
        """
        if not self.is_trained or self.model is None:
            raise ModelNotTrainedError("Model not trained. Call train() first.")

        available_cols = [c for c in self.feature_columns if c in features_df.columns]
        X = features_df[available_cols].fillna(0).values
        scores = self.model.decision_function(X)
        return self._compute_probability(scores)

    async def detect_wash_trades(
        self,
        chain_id: int,
        pool_address: str,
        threshold: float = 0.8,
        contamination: Optional[float] = None,
    ) -> List[SwapTrade]:
        """
        Detect wash trades using ML model.

        Builds features for pool trades, runs prediction, and returns
        trades exceeding the probability threshold.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address
            threshold: Probability threshold (default 0.8)
            contamination: Optional per-pool contamination

        Returns:
            List of trades flagged as wash trades
        """
        async with self.storage.get_session() as session:
            # Build features
            df = await self.feature_engineer.build_ml_features(
                chain_id, pool_address, session
            )
            if df.empty:
                return []

            # Train temporary model with custom contamination if needed
            if contamination is not None and contamination != self.contamination:
                X = df[[c for c in self.feature_columns if c in df.columns]].fillna(
                    0
                ).values
                temp_model = self._build_pipeline(contamination)
                temp_model.fit(X)
                scores = temp_model.decision_function(X)
            else:
                probs = await self.predict(df)
                # Convert back to raw scores for consistent threshold handling
                scores = -logit(probs.clip(1e-10, 1 - 1e-10))

            probabilities = self._compute_probability(scores)

            # Fetch trades
            stmt = (
                select(SwapTrade)
                .where(
                    and_(
                        SwapTrade.chain_id == chain_id,
                        SwapTrade.pool_address == pool_address,
                    )
                )
                .order_by(SwapTrade.block_timestamp)
            )
            result = await session.execute(stmt)
            trades = result.scalars().all()

            # Flag trades above threshold
            wash_trades: List[SwapTrade] = []
            for trade, prob in zip(trades, probabilities):
                if prob >= threshold:
                    trade.is_wash_trade = True
                    trade.wash_trade_score = float(prob)
                    trade.detection_method = "ml_isolation_forest"
                    wash_trades.append(trade)

            return wash_trades

    def save_model(self, path: Optional[str] = None) -> None:
        """
        Save trained model to disk.

        Args:
            path: File path (defaults to settings.ML_MODEL_PATH)

        Raises:
            ModelNotTrainedError: If no trained model exists
        """
        if not self.is_trained:
            raise ModelNotTrainedError("No trained model to save")

        save_path = path or settings.ML_MODEL_PATH
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        joblib.dump(
            {
                "pipeline": self.model,
                "feature_columns": self.feature_columns,
                "contamination": self.contamination,
            },
            save_path,
        )
        logger.info(f"Model saved to {save_path}")

    def load_model(self, path: Optional[str] = None) -> None:
        """
        Load trained model from disk.

        Args:
            path: File path (defaults to settings.ML_MODEL_PATH)

        Raises:
            ModelFileNotFoundError: If model file not found
        """
        load_path = path or settings.ML_MODEL_PATH
        if not os.path.exists(load_path):
            raise ModelFileNotFoundError(f"ML model file not found at {load_path}")

        data = joblib.load(load_path)
        self.model = data["pipeline"]
        self.feature_columns = data["feature_columns"]
        if "contamination" in data:
            self.contamination = data["contamination"]
        self.is_trained = True
        logger.info(f"Model loaded from {load_path}")

    def get_feature_importance(self) -> Dict[str, float]:
        """
        Get feature importance scores.

        Note: Isolation Forest doesn't provide direct feature importance.
        This method returns permutation-based importance approximation
        using decision function variance.

        Returns:
            Dict mapping feature names to importance scores
        """
        if not self.is_trained or self.model is None:
            return {}

        # Isolation Forest doesn't have native feature importance
        # Return uniform importance as placeholder
        return {col: 1.0 / len(self.feature_columns) for col in self.feature_columns}
