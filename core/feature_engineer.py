"""
Feature engineering for wash trade detection.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from bisect import bisect_left, bisect_right

import numpy as np
import pandas as pd
from sqlalchemy import select, and_, or_
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
        session: AsyncSession,
        trade_history: Optional[Dict[str, Any]] = None
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

        if trade_history:
            # Sender trades
            s_ts, s_trades = trade_history["by_sender"].get(trade.sender, ([], []))
            sender_trades = self._get_window_trades(s_ts, s_trades, one_hour_ago, trade.block_timestamp)

            # Recipient trades
            r_ts, r_trades = trade_history["by_recipient"].get(trade.recipient, ([], []))
            recipient_trades = self._get_window_trades(r_ts, r_trades, one_hour_ago, trade.block_timestamp)

            # Pair trades (Sender -> Recipient)
            pair_trades = [t for t in sender_trades if t.recipient == trade.recipient]

            # Reverse pair trades (Recipient -> Sender)
            rs_ts, rs_trades = trade_history["by_sender"].get(trade.recipient, ([], []))
            reverse_pair_trades = [t for t in self._get_window_trades(rs_ts, rs_trades, one_hour_ago, trade.block_timestamp)
                                   if t.recipient == trade.sender]
        else:
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

        features["sender_trade_count_1h"] = len(sender_trades)
        features["sender_volume_1h"] = sum(t.volume_usd or 0 for t in sender_trades)
        features["recipient_trade_count_1h"] = len(recipient_trades)
        features["pair_trade_count_1h"] = len(pair_trades)
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

        # Optimization: Process trades directly to avoid expensive DataFrame creation and iterrows
        volumes = []
        timestamps = []
        senders = []
        recipients = []
        pair_timestamps = defaultdict(list)
        self_trades_count = 0
        self_trades_volume = 0.0

        for t in trades:
            vol = t.volume_usd or 0.0
            volumes.append(vol)
            ts = t.block_timestamp
            timestamps.append(ts)
            senders.append(t.sender)
            recipients.append(t.recipient)
            pair_timestamps[(t.sender, t.recipient)].append(ts)
            if t.sender == t.recipient:
                self_trades_count += 1
                self_trades_volume += vol

        vols_array = np.array(volumes)
        mean_vol = vols_array.mean()
        features["total_volume_usd"] = float(vols_array.sum())
        features["avg_trade_volume_usd"] = float(mean_vol)
        features["max_trade_volume_usd"] = float(vols_array.max())
        # Use ddof=1 to match pandas.std() behavior
        vol_std = vols_array.std(ddof=1) if len(vols_array) > 1 else 0.0
        features["volume_volatility"] = float(vol_std / (mean_vol + 1e-9))

        if len(timestamps) > 0:
            ts_floats = np.array([t.timestamp() for t in timestamps])
            time_diffs = np.diff(ts_floats)
            # Match pandas .diff().fillna(0) behavior
            time_diffs_with_zero = np.concatenate(([0.0], time_diffs))
            features["avg_time_between_trades"] = float(time_diffs_with_zero.mean())
            features["std_time_between_trades"] = float(time_diffs_with_zero.std(ddof=1)) if len(time_diffs_with_zero) > 1 else 0.0
        else:
            features["avg_time_between_trades"] = 0.0
            features["std_time_between_trades"] = 0.0

        unique_senders = len(set(senders))
        unique_recipients = len(set(recipients))
        features["unique_senders"] = float(unique_senders)
        features["unique_recipients"] = float(unique_recipients)
        features["trader_diversity"] = (unique_senders + unique_recipients) / (2 * len(trades) + 1)

        # Optimization: O(N log N) circular trade calculation
        # Iterating over unique pairs that have reverse trades is much faster than iterrows
        circular_count = 0
        for (s, r), ts_list in pair_timestamps.items():
            reverse_pair = (r, s)
            if reverse_pair in pair_timestamps:
                reverse_ts_list = pair_timestamps[reverse_pair]
                for start_time in ts_list:
                    end_time = start_time + timedelta(hours=1)
                    idx1 = bisect_right(reverse_ts_list, start_time)
                    idx2 = bisect_right(reverse_ts_list, end_time)
                    circular_count += (idx2 - idx1)

        features["circular_trade_ratio"] = circular_count / (len(trades) + 1)

        sender_counts_dict = defaultdict(int)
        for s in senders:
            sender_counts_dict[s] += 1

        counts = list(sender_counts_dict.values())
        if counts:
            features["max_trades_per_sender"] = float(max(counts))
            features["avg_trades_per_sender"] = float(sum(counts) / len(counts))
        else:
            features["max_trades_per_sender"] = 0.0
            features["avg_trades_per_sender"] = 0.0

        features["self_trade_ratio"] = self_trades_count / (len(trades) + 1)
        features["self_trade_volume"] = float(self_trades_volume)
        return features

    def _get_window_trades(self, ts_list: List[datetime], trades_list: List[SwapTrade], start_ts: datetime, end_ts: datetime) -> List[SwapTrade]:
        """Helper for binary search window lookups."""
        idx_start = bisect_left(ts_list, start_ts)
        idx_end = bisect_left(ts_list, end_ts)
        return trades_list[idx_start:idx_end]

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

        # Pre-fetch trade history for all participants to avoid N+1 queries
        all_addresses = set()
        for t in trades:
            all_addresses.add(t.sender)
            all_addresses.add(t.recipient)

        min_ts = min(t.block_timestamp for t in trades)
        max_ts = max(t.block_timestamp for t in trades)

        hist_stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == chain_id,
                or_(
                    SwapTrade.sender.in_(all_addresses),
                    SwapTrade.recipient.in_(all_addresses)
                ),
                SwapTrade.block_timestamp >= min_ts - timedelta(hours=1),
                SwapTrade.block_timestamp <= max_ts
            )
        ).order_by(SwapTrade.block_timestamp)

        hist_result = await session.execute(hist_stmt)
        history = hist_result.scalars().all()

        trade_history = {
            "by_sender": defaultdict(lambda: ([], [])),
            "by_recipient": defaultdict(lambda: ([], []))
        }
        for ht in history:
            s_ts, s_tr = trade_history["by_sender"][ht.sender]
            s_ts.append(ht.block_timestamp)
            s_tr.append(ht)

            r_ts, r_tr = trade_history["by_recipient"][ht.recipient]
            r_ts.append(ht.block_timestamp)
            r_tr.append(ht)

        feature_list = []
        for trade in trades:
            trade_features = await self.compute_trade_features(trade, session, trade_history=trade_history)
            feature_list.append(trade_features)
        df = pd.DataFrame(feature_list)
        df["chain_id"] = chain_id
        df["pool_address"] = pool_address
        pool_features = await self.compute_pool_features(chain_id, pool_address, session)
        for key, value in pool_features.items():
            df[f"pool_{key}"] = value
        return df