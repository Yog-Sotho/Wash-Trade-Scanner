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
            p_ts, p_trades = trade_history["by_pair"].get((trade.sender, trade.recipient), ([], []))
            pair_trades = self._get_window_trades(p_ts, p_trades, one_hour_ago, trade.block_timestamp)

            # Reverse pair trades (Recipient -> Sender)
            rp_ts, rp_trades = trade_history["by_pair"].get((trade.recipient, trade.sender), ([], []))
            reverse_pair_trades = self._get_window_trades(rp_ts, rp_trades, one_hour_ago, trade.block_timestamp)
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
        session: AsyncSession,
        trades: Optional[List[SwapTrade]] = None
    ) -> Dict[str, float]:
        features = {}
        if trades is None:
            stmt = select(SwapTrade).where(
                and_(
                    SwapTrade.chain_id == chain_id,
                    SwapTrade.pool_address == pool_address,
                )
            ).order_by(SwapTrade.block_timestamp)
            result = await session.execute(stmt)
            trades = list(result.scalars().all())

        if not trades:
            return features

        # Use NumPy for faster calculations (replaces slow pandas DataFrame creation/ops)
        volumes = np.array([t.volume_usd or 0.0 for t in trades])
        timestamps = np.array([t.block_timestamp.timestamp() for t in trades])

        # Calculate time diffs (seconds)
        if len(timestamps) > 1:
            time_diffs = np.diff(timestamps)
            # Prepend 0.0 to match pandas diff().fillna(0) behavior
            time_diffs = np.insert(time_diffs, 0, 0.0)
        else:
            time_diffs = np.array([0.0])

        features["avg_time_between_trades"] = float(np.mean(time_diffs))
        features["std_time_between_trades"] = float(np.std(time_diffs, ddof=1)) if len(time_diffs) > 1 else 0.0
        features["total_volume_usd"] = float(np.sum(volumes))
        avg_vol = float(np.mean(volumes))
        features["avg_trade_volume_usd"] = avg_vol
        features["max_trade_volume_usd"] = float(np.max(volumes))
        features["volume_volatility"] = float(np.std(volumes, ddof=1) / (avg_vol + 1e-9)) if len(volumes) > 1 else 0.0

        senders = [t.sender for t in trades]
        recipients = [t.recipient for t in trades]
        unique_senders = len(set(senders))
        unique_recipients = len(set(recipients))

        features["unique_senders"] = float(unique_senders)
        features["unique_recipients"] = float(unique_recipients)
        features["trader_diversity"] = (unique_senders + unique_recipients) / (2 * len(trades) + 1)

        # Optimization: O(N log N) circular trade calculation using bisect
        # Replacing iterrows with a single-pass loop over trades
        pair_timestamps = defaultdict(list)
        for t in trades:
            pair_timestamps[(t.sender, t.recipient)].append(t.block_timestamp)

        circular_count = 0
        for t in trades:
            reverse_pair = (t.recipient, t.sender)
            if reverse_pair in pair_timestamps:
                r_timestamps = pair_timestamps[reverse_pair]
                start_time = t.block_timestamp
                end_time = t.block_timestamp + timedelta(hours=1)

                idx1 = bisect_right(r_timestamps, start_time)
                idx2 = bisect_right(r_timestamps, end_time)
                circular_count += (idx2 - idx1)

        features["circular_trade_ratio"] = circular_count / (len(trades) + 1)

        sender_counts = defaultdict(int)
        for s in senders:
            sender_counts[s] += 1

        counts_vals = list(sender_counts.values())
        features["max_trades_per_sender"] = float(max(counts_vals)) if counts_vals else 0.0
        features["avg_trades_per_sender"] = float(np.mean(counts_vals)) if counts_vals else 0.0

        self_trade_vol = 0.0
        self_trade_count = 0
        for t in trades:
            if t.sender == t.recipient:
                self_trade_count += 1
                self_trade_vol += (t.volume_usd or 0.0)

        features["self_trade_ratio"] = self_trade_count / (len(trades) + 1)
        features["self_trade_volume"] = self_trade_vol
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
        session: AsyncSession,
        trades: Optional[List[SwapTrade]] = None,
    ) -> pd.DataFrame:
        if trades is None:
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
            "by_recipient": defaultdict(lambda: ([], [])),
            "by_pair": defaultdict(lambda: ([], []))
        }
        for ht in history:
            s_ts, s_tr = trade_history["by_sender"][ht.sender]
            s_ts.append(ht.block_timestamp)
            s_tr.append(ht)

            r_ts, r_tr = trade_history["by_recipient"][ht.recipient]
            r_ts.append(ht.block_timestamp)
            r_tr.append(ht)

            p_ts, p_tr = trade_history["by_pair"][(ht.sender, ht.recipient)]
            p_ts.append(ht.block_timestamp)
            p_tr.append(ht)

        feature_list = []
        for trade in trades:
            trade_features = await self.compute_trade_features(trade, session, trade_history=trade_history)
            feature_list.append(trade_features)
        df = pd.DataFrame(feature_list)
        df["chain_id"] = chain_id
        df["pool_address"] = pool_address
        # Optimization: Pass pre-fetched trades to avoid redundant DB query
        pool_features = await self.compute_pool_features(chain_id, pool_address, session, trades=trades)
        for key, value in pool_features.items():
            df[f"pool_{key}"] = value
        return df