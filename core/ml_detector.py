"""
Machine learning based anomaly detection for wash trading.
"""

import logging
import joblib
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

from models.schemas import SwapTrade
from core.feature_engineer import FeatureEngineer
from core.storage import Storage
from config.settings import settings

logger = logging.getLogger(__name__)


class MLDetector:
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

    def _build_pipeline(self) -> Pipeline:
        return Pipeline([
            ("scaler", StandardScaler()),
            ("isolation_forest", IsolationForest(
                contamination=settings.ML_CONTAMINATION,
                random_state=42,
                n_estimators=100,
                max_samples="auto",
                bootstrap=False,
                n_jobs=-1,
            ))
        ])

    async def train(
        self,
        chain_id: int,
        pool_addresses: List[str],
        use_heuristic_labels: bool = True
    ) -> None:
        logger.info(f"Training ML model on {len(pool_addresses)} pools")
        all_features = []
        all_labels = []
        async with self.storage.get_session() as session:
            for pool in pool_addresses:
                df = await self.feature_engineer.build_ml_features(chain_id, pool, session)
                if df.empty:
                    continue
                if use_heuristic_labels:
                    from sqlalchemy import select, and_
                    stmt = select(SwapTrade).where(
                        and_(
                            SwapTrade.chain_id == chain_id,
                            SwapTrade.pool_address == pool,
                        )
                    )
                    result = await session.execute(stmt)
                    trades = result.scalars().all()
                    trade_labels = {t.id: -1 if t.is_wash_trade else 1 for t in trades}
                    labels = [trade_labels.get(t.id, 1) for t in trades]
                    all_labels.extend(labels)
                available_cols = [c for c in self.feature_columns if c in df.columns]
                features = df[available_cols].fillna(0).values
                all_features.append(features)
        if not all_features:
            raise ValueError("No training data available")
        X = np.vstack(all_features)
        self.model = self._build_pipeline()
        if all_labels and use_heuristic_labels:
            y = np.array(all_labels)
            sample_weight = np.ones(len(y))
            sample_weight[y == -1] = 2.0
        else:
            y = None
            sample_weight = None
        self.model.fit(X, y=sample_weight)
        self.is_trained = True
        logger.info("ML model training complete")

    async def predict(self, features_df: pd.DataFrame) -> np.ndarray:
        if not self.is_trained or self.model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        available_cols = [c for c in self.feature_columns if c in features_df.columns]
        X = features_df[available_cols].fillna(0).values
        scores = self.model.decision_function(X)
        from scipy.special import expit
        probabilities = expit(-scores)
        return probabilities

    async def detect_wash_trades(
        self,
        chain_id: int,
        pool_address: str,
        threshold: float = 0.8
    ) -> List[SwapTrade]:
        async with self.storage.get_session() as session:
            df = await self.feature_engineer.build_ml_features(chain_id, pool_address, session)
            if df.empty:
                return []
            probs = await self.predict(df)
            from sqlalchemy import select, and_
            stmt = select(SwapTrade).where(
                and_(
                    SwapTrade.chain_id == chain_id,
                    SwapTrade.pool_address == pool_address,
                )
            ).order_by(SwapTrade.block_timestamp)
            result = await session.execute(stmt)
            trades = result.scalars().all()
            wash_trades = []
            for trade, prob in zip(trades, probs):
                if prob >= threshold:
                    trade.is_wash_trade = True
                    trade.wash_trade_score = float(prob)
                    trade.detection_method = "ml_isolation_forest"
                    wash_trades.append(trade)
            return wash_trades

    def save_model(self, path: Optional[str] = None):
        if not self.is_trained:
            raise RuntimeError("No trained model to save")
        save_path = path or settings.ML_MODEL_PATH
        joblib.dump({
            "pipeline": self.model,
            "feature_columns": self.feature_columns,
        }, save_path)
        logger.info(f"Model saved to {save_path}")

    def load_model(self, path: Optional[str] = None):
        load_path = path or settings.ML_MODEL_PATH
        data = joblib.load(load_path)
        self.model = data["pipeline"]
        self.feature_columns = data["feature_columns"]
        self.is_trained = True
        logger.info(f"Model loaded from {load_path}")