"""
Feature engineering for wash trade detection.
"""

import bisect
import logging
import math
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.advanced_heuristics import trader_of
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


def _window_start(times: list[float], index: int, seconds: float) -> int:
    """First position in `times` (ascending) at or after times[index] - seconds."""
    return bisect.bisect_left(times, times[index] - seconds, 0, index)


def trade_features(trades: Sequence[SwapTrade]) -> list[dict[str, float]]:
    """Per-trade features computed in memory from one pool's trades.

    Activity features describe the trader -- the transaction sender, see
    core.advanced_heuristics.trader_of -- over the hour or day before each
    trade, within the pool. (Column names keep their historical ``sender_``
    prefix so saved models stay loadable.) O(n log n): every window is found
    by binary search over per-trader time series instead of a query per trade.
    """
    ordered = sorted(trades, key=lambda t: (t.block_timestamp, t.block_number, t.log_index or 0))
    by_trader: dict[str, list[int]] = defaultdict(list)
    by_recipient: dict[str, list[int]] = defaultdict(list)
    by_pair: dict[tuple[str, str], list[int]] = defaultdict(list)
    for i, trade in enumerate(ordered):
        trader = trader_of(trade)
        recipient = (trade.recipient or "").lower()
        by_trader[trader].append(i)
        by_recipient[recipient].append(i)
        by_pair[(trader, recipient)].append(i)

    def series(indexes: list[int]) -> list[float]:
        return [ordered[i].block_timestamp.timestamp() for i in indexes]

    trader_times = {k: series(v) for k, v in by_trader.items()}
    recipient_times = {k: series(v) for k, v in by_recipient.items()}
    pair_times = {k: series(v) for k, v in by_pair.items()}
    trader_volume_prefix: dict[str, list[float]] = {}
    for key, indexes in by_trader.items():
        prefix = [0.0]
        for i in indexes:
            prefix.append(prefix[-1] + (ordered[i].volume_usd or 0.0))
        trader_volume_prefix[key] = prefix

    def ranks(groups: dict[Any, list[int]]) -> list[int]:
        """Position of each trade within its own group's time series."""
        rank = [0] * len(ordered)
        for indexes in groups.values():
            for r, i in enumerate(indexes):
                rank[i] = r
        return rank

    trader_rank = ranks(by_trader)
    recipient_rank = ranks(by_recipient)
    pair_rank = ranks(by_pair)

    rows: list[dict[str, float]] = []
    for i, trade in enumerate(ordered):
        trader = trader_of(trade)
        recipient = (trade.recipient or "").lower()
        f: dict[str, float] = {}
        f["trade_id"] = trade.id
        f["volume_usd"] = trade.volume_usd or 0.0
        f["amount_in_usd"] = trade.amount_in_usd or 0.0
        f["amount_out_usd"] = trade.amount_out_usd or 0.0
        if f["amount_in_usd"] > 0 and f["amount_out_usd"] > 0:
            f["slippage_ratio"] = abs(f["amount_out_usd"] - f["amount_in_usd"]) / f["amount_in_usd"]
        else:
            f["slippage_ratio"] = 0.0

        t_rank = trader_rank[i]
        times = trader_times[trader]
        start_1h = _window_start(times, t_rank, 3600)
        start_24h = _window_start(times, t_rank, 86_400)
        prefix = trader_volume_prefix[trader]
        f["sender_trade_count_1h"] = t_rank - start_1h
        f["sender_volume_1h"] = prefix[t_rank] - prefix[start_1h]
        f["sender_trade_count_24h"] = t_rank - start_24h
        day = [ordered[j].block_timestamp for j in by_trader[trader][start_24h : t_rank + 1]]
        f["sender_hour_entropy_24h"] = normalized_hour_entropy(day)
        f["time_since_last_sender_trade"] = (
            times[t_rank] - times[t_rank - 1] if t_rank > start_1h else 3600.0
        )
        f["amount_significant_digits"] = significant_digits(trade.amount_in)

        r_rank = recipient_rank[i]
        f["recipient_trade_count_1h"] = r_rank - _window_start(
            recipient_times[recipient], r_rank, 3600
        )
        p_rank = pair_rank[i]
        f["pair_trade_count_1h"] = p_rank - _window_start(
            pair_times[(trader, recipient)], p_rank, 3600
        )
        reverse = pair_times.get((recipient, trader), [])
        now = trade.block_timestamp.timestamp()
        f["reverse_pair_trade_count_1h"] = bisect.bisect_left(reverse, now) - bisect.bisect_left(
            reverse, now - 3600
        )
        f["gas_price"] = trade.gas_price or 0.0
        rows.append(f)
    return rows


def pool_features(trades: Sequence[SwapTrade]) -> dict[str, float]:
    """Pool-level descriptors, from one pool's trades."""
    features: dict[str, float] = {}
    if not trades:
        return features
    ordered = sorted(trades, key=lambda t: t.block_timestamp)
    df = pd.DataFrame(
        [
            {
                "timestamp": t.block_timestamp,
                "volume_usd": t.volume_usd or 0.0,
                "sender": t.sender,
                "recipient": t.recipient,
            }
            for t in ordered
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
    features["trader_diversity"] = (unique_senders + unique_recipients) / (2 * len(ordered) + 1)

    # Reverse trades (recipient -> sender) within the hour after each trade.
    pair_times: dict[tuple[str, str], list[float]] = defaultdict(list)
    for t in ordered:
        pair_times[(t.sender, t.recipient)].append(t.block_timestamp.timestamp())
    circular_count = 0
    for t in ordered:
        reverse = pair_times.get((t.recipient, t.sender), [])
        now = t.block_timestamp.timestamp()
        circular_count += bisect.bisect_right(reverse, now + 3600) - bisect.bisect_right(
            reverse, now
        )
    features["circular_trade_ratio"] = circular_count / (len(ordered) + 1)

    sender_counts = df["sender"].value_counts()
    features["max_trades_per_sender"] = sender_counts.max() if not sender_counts.empty else 0
    features["avg_trades_per_sender"] = sender_counts.mean() if not sender_counts.empty else 0
    self_trades = df[df["sender"] == df["recipient"]]
    features["self_trade_ratio"] = len(self_trades) / (len(ordered) + 1)
    features["self_trade_volume"] = self_trades["volume_usd"].sum()
    features["benford_deviation"] = benford_deviation([t.amount_in for t in ordered])
    features["hour_entropy"] = normalized_hour_entropy([t.block_timestamp for t in ordered])
    return features


def features_from_trades(trades: Sequence[SwapTrade]) -> pd.DataFrame:
    """The ML feature frame for one pool's trades (rows in time order)."""
    if not trades:
        return pd.DataFrame()
    df = pd.DataFrame(trade_features(trades))
    for key, value in pool_features(trades).items():
        df[f"pool_{key}"] = value
    return df


class FeatureEngineer:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def _pool_trades(
        self, chain_id: int, pool_address: str, session: AsyncSession
    ) -> list[SwapTrade]:
        stmt = (
            select(SwapTrade)
            .where(
                and_(
                    SwapTrade.chain_id == chain_id,
                    SwapTrade.pool_address == pool_address,
                )
            )
            .order_by(SwapTrade.block_timestamp, SwapTrade.block_number, SwapTrade.log_index)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def compute_pool_features(
        self, chain_id: int, pool_address: str, session: AsyncSession
    ) -> dict[str, float]:
        return pool_features(await self._pool_trades(chain_id, pool_address, session))

    async def build_ml_features(
        self, chain_id: int, pool_address: str, session: AsyncSession
    ) -> pd.DataFrame:
        """One query for the pool's trades; every feature is computed in memory."""
        trades = await self._pool_trades(chain_id, pool_address, session)
        df = features_from_trades(trades)
        if df.empty:
            return df
        df["chain_id"] = chain_id
        df["pool_address"] = pool_address
        return df
