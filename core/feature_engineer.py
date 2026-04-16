"""
Feature engineering for wash trade detection.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from models.schemas import SwapTrade
from core.storage import Storage

logger = logging.getLogger(__name__)


class FeatureEngineer:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def compute_trade_features(
        self,
        trade: SwapTrade,
        session: AsyncSession
    ) -> Dict[str, float]:
        features = {}
        features["volume_usd"] = trade.volume_usd or 0.0
        features["amount_in_usd"] = trade.amount_in_usd or 0.0
        features["amount_out_usd"] = trade.amount_out_usd or 0.0
        if features["amount_in_usd"] > 0 and features["amount_out_usd"] > 0:
            features["slippage_ratio"] = abs(
                features["amount_out_usd"] - features["amount_in_usd"]
            ) / features["amount_in_usd"]
        else:
            features["slippage_ratio"] = 0.0
        one_hour_ago = trade.block_timestamp - timedelta(hours=1)
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == trade.chain_id,
                SwapTrade.sender == trade.sender,
                SwapTrade.block_timestamp >= one_hour_ago,
                SwapTrade.block_timestamp < trade.block_timestamp,
            )
        )
        result = await session.execute(stmt)
        sender_trades = result.scalars().all()
        features["sender_trade_count_1h"] = len(sender_trades)
        features["sender_volume_1h"] = sum(t.volume_usd or 0 for t in sender_trades)
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == trade.chain_id,
                SwapTrade.recipient == trade.recipient,
                SwapTrade.block_timestamp >= one_hour_ago,
                SwapTrade.block_timestamp < trade.block_timestamp,
            )
        )
        result = await session.execute(stmt)
        recipient_trades = result.scalars().all()
        features["recipient_trade_count_1h"] = len(recipient_trades)
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == trade.chain_id,
                SwapTrade.sender == trade.sender,
                SwapTrade.recipient == trade.recipient,
                SwapTrade.block_timestamp >= one_hour_ago,
                SwapTrade.block_timestamp < trade.block_timestamp,
            )
        )
        result = await session.execute(stmt)
        pair_trades = result.scalars().all()
        features["pair_trade_count_1h"] = len(pair_trades)
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == trade.chain_id,
                SwapTrade.sender == trade.recipient,
                SwapTrade.recipient == trade.sender,
                SwapTrade.block_timestamp >= one_hour_ago,
                SwapTrade.block_timestamp < trade.block_timestamp,
            )
        )
        result = await session.execute(stmt)
        reverse_pair_trades = result.scalars().all()
        features["reverse_pair_trade_count_1h"] = len(reverse_pair_trades)
        if sender_trades:
            last_sender_trade = max(sender_trades, key=lambda t: t.block_timestamp)
            features["time_since_last_sender_trade"] = (
                trade.block_timestamp - last_sender_trade.block_timestamp
            ).total_seconds()
        else:
            features["time_since_last_sender_trade"] = 3600.0
        features["gas_price"] = trade.gas_price or 0.0
        return features

    async def compute_pool_features(
        self,
        chain_id: int,
        pool_address: str,
        session: AsyncSession
    ) -> Dict[str, float]:
        features = {}
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == chain_id,
                SwapTrade.pool_address == pool_address,
            )
        ).order_by(SwapTrade.block_timestamp)
        result = await session.execute(stmt)
        trades = result.scalars().all()
        if not trades:
            return features
        df = pd.DataFrame([{
            "timestamp": t.block_timestamp,
            "volume_usd": t.volume_usd or 0.0,
            "sender": t.sender,
            "recipient": t.recipient,
        } for t in trades])
        time_diffs = df["timestamp"].diff().dt.total_seconds().fillna(0)
        features["avg_time_between_trades"] = time_diffs.mean()
        features["std_time_between_trades"] = time_diffs.std()
        features["total_volume_usd"] = df["volume_usd"].sum()
        features["avg_trade_volume_usd"] = df["volume_usd"].mean()
        features["max_trade_volume_usd"] = df["volume_usd"].max()
        features["volume_volatility"] = df["volume_usd"].std() / (df["volume_usd"].mean() + 1e-9)
        unique_senders = df["sender"].nunique()
        unique_recipients = df["recipient"].nunique()
        features["unique_senders"] = unique_senders
        features["unique_recipients"] = unique_recipients
        features["trader_diversity"] = (unique_senders + unique_recipients) / (2 * len(trades) + 1)
        circular_count = 0
        for i, row in df.iterrows():
            matching = df[
                (df["sender"] == row["recipient"]) &
                (df["recipient"] == row["sender"]) &
                (df["timestamp"] > row["timestamp"]) &
                (df["timestamp"] <= row["timestamp"] + timedelta(hours=1))
            ]
            circular_count += len(matching)
        features["circular_trade_ratio"] = circular_count / (len(trades) + 1)
        sender_counts = df["sender"].value_counts()
        features["max_trades_per_sender"] = sender_counts.max() if not sender_counts.empty else 0
        features["avg_trades_per_sender"] = sender_counts.mean() if not sender_counts.empty else 0
        self_trades = df[df["sender"] == df["recipient"]]
        features["self_trade_ratio"] = len(self_trades) / (len(trades) + 1)
        features["self_trade_volume"] = self_trades["volume_usd"].sum()
        return features

    async def build_ml_features(
        self,
        chain_id: int,
        pool_address: str,
        session: AsyncSession
    ) -> pd.DataFrame:
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == chain_id,
                SwapTrade.pool_address == pool_address,
            )
        ).order_by(SwapTrade.block_timestamp)
        result = await session.execute(stmt)
        trades = result.scalars().all()
        if not trades:
            return pd.DataFrame()
        feature_list = []
        for trade in trades:
            trade_features = await self.compute_trade_features(trade, session)
            feature_list.append(trade_features)
        df = pd.DataFrame(feature_list)
        df["chain_id"] = chain_id
        df["pool_address"] = pool_address
        pool_features = await self.compute_pool_features(chain_id, pool_address, session)
        for key, value in pool_features.items():
            df[f"pool_{key}"] = value
        return df