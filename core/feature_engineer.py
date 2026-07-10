"""
Feature engineering for wash trade detection.
"""

import logging
import math
from collections import Counter
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.storage import Storage
from models.schemas import SwapTrade

logger = logging.getLogger(__name__)

# Benford's law: expected frequency of each first significant digit 1-9.
_BENFORD = {d: math.log10(1 + 1 / d) for d in range(1, 10)}


def significant_digits(value: float, max_digits: int = 8) -> int:
    """Count significant digits of a number, capped at `max_digits`.

    Round amounts (1000, 2.5) need few digits; organic on-chain amounts carry
    long mantissas from AMM pricing. Low values are a wash/bot fingerprint.
    """
    if not value or not math.isfinite(value):
        return 0
    mantissa = f"{abs(value):.{max_digits - 1}e}".split("e")[0].rstrip("0").replace(".", "")
    return max(len(mantissa), 1)


def benford_deviation(values: list[float]) -> float:
    """Mean absolute deviation of first-digit frequencies from Benford's law.

    Organic trade amounts follow Benford closely (deviation near 0); fabricated
    or repeated amounts do not. Returns 0.0 when no usable values exist.
    """
    digits = []
    for v in values:
        if v and v > 0 and math.isfinite(v):
            first = int(f"{v:e}"[0])
            if 1 <= first <= 9:
                digits.append(first)
    if not digits:
        return 0.0
    counts = Counter(digits)
    total = len(digits)
    return sum(abs(counts.get(d, 0) / total - _BENFORD[d]) for d in range(1, 10)) / 9


def normalized_hour_entropy(timestamps: list[datetime]) -> float:
    """Shannon entropy of the hour-of-day distribution, normalized to [0, 1].

    Human trading has diurnal structure (entropy well below 1); wash bots
    trade uniformly around the clock (entropy near 1).
    """
    if not timestamps:
        return 0.0
    counts = Counter(ts.hour for ts in timestamps)
    total = len(timestamps)
    entropy = -sum((c / total) * math.log2(c / total) for c in counts.values())
    return entropy / math.log2(24)


class FeatureEngineer:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def compute_trade_features(
        self, trade: SwapTrade, session: AsyncSession
    ) -> dict[str, float]:
        features: dict[str, float] = {}
        features["trade_id"] = trade.id
        features["volume_usd"] = trade.volume_usd or 0.0
        features["amount_in_usd"] = trade.amount_in_usd or 0.0
        features["amount_out_usd"] = trade.amount_out_usd or 0.0
        if features["amount_in_usd"] > 0 and features["amount_out_usd"] > 0:
            features["slippage_ratio"] = (
                abs(features["amount_out_usd"] - features["amount_in_usd"])
                / features["amount_in_usd"]
            )
        else:
            features["slippage_ratio"] = 0.0
        one_hour_ago = trade.block_timestamp - timedelta(hours=1)
        # Fetch a 24h sender window in one query; the 1h stats are derived from
        # it in Python so this stays a single round-trip.
        one_day_ago = trade.block_timestamp - timedelta(hours=24)
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == trade.chain_id,
                SwapTrade.sender == trade.sender,
                SwapTrade.block_timestamp >= one_day_ago,
                SwapTrade.block_timestamp < trade.block_timestamp,
            )
        )
        result = await session.execute(stmt)
        sender_trades_24h = result.scalars().all()
        sender_trades = [t for t in sender_trades_24h if t.block_timestamp >= one_hour_ago]
        features["sender_trade_count_1h"] = len(sender_trades)
        features["sender_volume_1h"] = sum(t.volume_usd or 0 for t in sender_trades)
        features["sender_trade_count_24h"] = len(sender_trades_24h)
        features["sender_hour_entropy_24h"] = normalized_hour_entropy(
            [t.block_timestamp for t in sender_trades_24h] + [trade.block_timestamp]
        )
        features["amount_significant_digits"] = significant_digits(trade.amount_in)
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
        self, chain_id: int, pool_address: str, session: AsyncSession
    ) -> dict[str, float]:
        features: dict[str, float] = {}
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
        if not trades:
            return features
        df = pd.DataFrame(
            [
                {
                    "timestamp": t.block_timestamp,
                    "volume_usd": t.volume_usd or 0.0,
                    "sender": t.sender,
                    "recipient": t.recipient,
                }
                for t in trades
            ]
        )
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
        for _i, row in df.iterrows():
            matching = df[
                (df["sender"] == row["recipient"])
                & (df["recipient"] == row["sender"])
                & (df["timestamp"] > row["timestamp"])
                & (df["timestamp"] <= row["timestamp"] + timedelta(hours=1))
            ]
            circular_count += len(matching)
        features["circular_trade_ratio"] = circular_count / (len(trades) + 1)
        sender_counts = df["sender"].value_counts()
        features["max_trades_per_sender"] = sender_counts.max() if not sender_counts.empty else 0
        features["avg_trades_per_sender"] = sender_counts.mean() if not sender_counts.empty else 0
        self_trades = df[df["sender"] == df["recipient"]]
        features["self_trade_ratio"] = len(self_trades) / (len(trades) + 1)
        features["self_trade_volume"] = self_trades["volume_usd"].sum()
        features["benford_deviation"] = benford_deviation([t.amount_in for t in trades])
        features["hour_entropy"] = normalized_hour_entropy([t.block_timestamp for t in trades])
        return features

    async def build_ml_features(
        self, chain_id: int, pool_address: str, session: AsyncSession
    ) -> pd.DataFrame:
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
