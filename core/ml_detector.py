"""
Machine learning based anomaly detection for wash trading.
With SHAP explainability support.
"""

import logging
import os
from typing import List, Optional, Dict, Any

import numpy as np
import pandas as pd
from scipy.special import expit, logit
from sqlalchemy import select, and_
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

from models.schemas import SwapTrade
from core.feature_engineer import FeatureEngineer
from core.storage import Storage
from core.exceptions import ModelNotTrainedError, InsufficientDataError
from config.settings import settings

logger = logging.getLogger(__name__)


class MLDetector:
    """Isolation Forest based wash trade detection with explainability."""

    def __init__(self, storage: Storage, feature_engineer: FeatureEngineer):
        self.storage = storage
        self.feature_engineer = feature_engineer
        self.model: Optional[Pipeline] = None
        self.is_trained = False
        self.feature_columns = [
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

    def _build_pipeline(self, contamination: float = settings.ML_CONTAMINATION) -> Pipeline:
        return Pipeline([
            ("scaler", StandardScaler()),
            ("isolation_forest", IsolationForest(
                contamination=contamination,
                random_state=42,
                n_estimators=100,
                max_samples="auto",
                bootstrap=False,
                n_jobs=-1,
            )),
        ])

    async def train(
        self,
        chain_id: int,
        pool_addresses: List[str],
        use_heuristic_labels: bool = True,
        contamination: Optional[float] = None,
    ) -> None:
        """Train ML model on pool data."""
        if contamination is None:
            contamination = settings.ML_CONTAMINATION

        logger.info(f"Training ML model on {len(pool_addresses)} pools (contamination={contamination})")

        all_features: List[np.ndarray] = []
        all_labels: List[int] = []

        async with await self.storage.get_session() as session:
            for pool in pool_addresses:
                trades = await self.storage.get_pool_trades(chain_id, pool, ascending=True)

                df = await self.feature_engineer.build_ml_features(chain_id, pool, session, trades=trades)
                if df.empty:
                    continue

                available_cols = [c for c in self.feature_columns if c in df.columns]
                if not available_cols:
                    continue

                features = df[available_cols].fillna(0).values
                all_features.append(features)

                if use_heuristic_labels:
                    trade_labels = {t.id: -1 if t.is_wash_trade else 1 for t in trades}
                    # Optimization: Use list comprehension instead of iterrows()
                    # trade_id is added to df in feature_engineer.py:build_ml_features
                    labels = [trade_labels.get(tid, 1) for tid in df["trade_id"]]
                    all_labels.extend(labels)

        if not all_features:
            raise InsufficientDataError("No training data available")

        X = np.vstack(all_features)
        self.model = self._build_pipeline(contamination)

        if all_labels and use_heuristic_labels:
            y = np.array(all_labels)
            sample_weight = np.ones(len(y))
            sample_weight[y == -1] = 2.0
        else:
            sample_weight = None

        self.model.fit(X, sample_weight=sample_weight)
        self.is_trained = True
        logger.info(f"ML model trained on {len(X)} samples")

    async def predict(self, features_df: pd.DataFrame) -> np.ndarray:
        """Predict anomaly probabilities."""
        if not self.is_trained or self.model is None:
            raise ModelNotTrainedError("Model not trained. Call train() first.")

        available_cols = [c for c in self.feature_columns if c in features_df.columns]
        X = features_df[available_cols].fillna(0).values

        scores = self.model.decision_function(X)
        probabilities = expit(-scores)
        return probabilities

    async def explain_prediction(
        self,
        features_df: pd.DataFrame,
        idx: int,
    ) -> Dict[str, float]:
        """Explain a single prediction using feature importance approximation."""
        if not settings.ML_EXPLAINABILITY:
            return {}

        available_cols = [c for c in self.feature_columns if c in features_df.columns]
        if not available_cols:
            return {}

        row = features_df.iloc[idx][available_cols].fillna(0)

        feature_importance = {}
        for col in available_cols:
            perturbed = features_df.copy()
            perturbed[col] = perturbed[col].mean()
            probs = await self.predict(perturbed)
            original_probs = await self.predict(features_df)
            diff = abs(original_probs[idx] - probs[idx])
            feature_importance[col] = float(diff)

        total = sum(feature_importance.values()) or 1.0
        return {k: v / total for k, v in feature_importance.items()}

    async def detect_wash_trades(
        self,
        chain_id: int,
        pool_address: str,
        threshold: float = 0.8,
        contamination: Optional[float] = None,
        trades: Optional[List[SwapTrade]] = None,
    ) -> List[SwapTrade]:
        """Detect wash trades using ML model."""
        async with await self.storage.get_session() as session:
            if trades is None:
                trades = await self.storage.get_pool_trades(chain_id, pool_address, ascending=True)

            df = await self.feature_engineer.build_ml_features(chain_id, pool_address, session, trades=trades)
            if df.empty:
                return []

            if contamination is not None and contamination != settings.ML_CONTAMINATION:
                X = df[[c for c in self.feature_columns if c in df.columns]].fillna(0).values
                temp_model = self._build_pipeline(contamination)
                temp_model.fit(X)
                scores = temp_model.decision_function(X)
            else:
                probs = await self.predict(df)
                scores = -logit(np.clip(probs, 1e-10, 1 - 1e-10))

            probabilities = expit(-scores)

            wash_trades = []
            for trade, prob in zip(trades, probabilities):
                if prob >= threshold:
                    trade.is_wash_trade = True
                    trade.wash_trade_score = float(prob)
                    trade.detection_method = "ml_isolation_forest"
                    wash_trades.append(trade)

            return wash_trades

    def save_model(self, path: Optional[str] = None) -> None:
        """Save trained model to disk."""
        if not self.is_trained:
            raise ModelNotTrainedError("No trained model to save")

        import joblib
        save_path = path or settings.ML_MODEL_PATH
        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        joblib.dump({
            "pipeline": self.model,
            "feature_columns": self.feature_columns,
        }, save_path)
        logger.info(f"Model saved to {save_path}")

    def load_model(self, path: Optional[str] = None) -> None:
        """Load trained model from disk."""
        import joblib
        load_path = path or settings.ML_MODEL_PATH
        if not os.path.exists(load_path):
            raise FileNotFoundError(f"ML model file not found at {load_path}")

        data = joblib.load(load_path)
        self.model = data["pipeline"]
        self.feature_columns = data["feature_columns"]
        self.is_trained = True
        logger.info(f"Model loaded from {load_path}")
