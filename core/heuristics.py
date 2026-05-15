"""
High-signal heuristic detection rules for wash trading.
Uses robust statistical methods (MAD/IQR) instead of z-score.
"""

import logging
import math
from typing import List, Dict, Any, Set, Tuple, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from bisect import bisect_left, bisect_right

import networkx as nx
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from models.schemas import SwapTrade, AddressCluster
from config.settings import settings

logger = logging.getLogger(__name__)


def _load_allowlist() -> Set[str]:
    """Load allowed bot addresses from settings."""
    return settings.bot_allowlist_set


class RobustAnomalyDetector:
    """
    Median Absolute Deviation (MAD) based anomaly detection.
    Robust to outliers, no normality assumption required.
    """

    def __init__(self, method: str = "mad"):
        self.method = method
        self.median: Optional[float] = None
        self.mad: Optional[float] = None
        self.q1: Optional[float] = None
        self.q3: Optional[float] = None
        self.iqr: Optional[float] = None
        self._fitted = False

    def fit(self, volumes: List[float]) -> None:
        """Fit detector on log-transformed volumes."""
        if not volumes:
            raise ValueError("Cannot fit on empty data")

        log_volumes = [math.log1p(max(v, 0.0)) for v in volumes]

        if self.method == "mad":
            sorted_vals = sorted(log_volumes)
            n = len(sorted_vals)
            self.median = sorted_vals[n // 2] if n % 2 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            self.mad = sorted([abs(v - self.median) for v in log_volumes])[n // 2]
        elif self.method == "iqr":
            sorted_vals = sorted(log_volumes)
            n = len(sorted_vals)
            self.q1 = sorted_vals[n // 4]
            self.q3 = sorted_vals[3 * n // 4]
            self.iqr = self.q3 - self.q1
        else:
            raise ValueError(f"Unknown method: {self.method}")

        self._fitted = True

    def score(self, volume: float) -> float:
        """Compute anomaly score for a volume."""
        if not self._fitted:
            raise RuntimeError("Detector not fitted")

        log_vol = math.log1p(max(volume, 0.0))

        if self.method == "mad":
            if self.mad == 0:
                return 0.0
            modified_z = 0.6745 * (log_vol - self.median) / self.mad
            return abs(modified_z)
        elif self.method == "iqr":
            if self.iqr == 0:
                return 0.0
            if log_vol < self.q1 - 1.5 * self.iqr:
                return (self.q1 - log_vol) / self.iqr
            elif log_vol > self.q3 + 1.5 * self.iqr:
                return (log_vol - self.q3) / self.iqr
            return 0.0
        return 0.0

    def is_anomaly(self, volume: float, threshold: float = 3.5) -> bool:
        """Check if volume is anomalous."""
        return self.score(volume) > threshold


class HeuristicDetector:
    """Wash trade detection using configurable heuristics."""

    def __init__(self):
        self.confidence_threshold = settings.SUSPICIOUS_ACTIVITY_THRESHOLD
        self.bot_allowlist = _load_allowlist()

    async def detect_self_trading(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """Detect trades where sender equals recipient."""
        wash_trades = []
        for trade in trades:
            if trade.sender.lower() == trade.recipient.lower():
                trade.is_wash_trade = True
                trade.wash_trade_score = 1.0
                trade.detection_method = "self_trading"
                wash_trades.append(trade)
        logger.info(f"Detected {len(wash_trades)} self-trades")
        return wash_trades

    async def detect_circular_trading(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """Detect circular trading via strongly connected components."""
        wash_trades = []
        pool_groups = defaultdict(list)
        for trade in trades:
            pool_groups[trade.pool_address].append(trade)

        for pool_address, pool_trades in pool_groups.items():
            G = nx.DiGraph()
            edges = defaultdict(float)
            # Optimization: pre-group trades by (sender, recipient) pairs
            pair_timestamps = defaultdict(list)
            for trade in pool_trades:
                key = (trade.sender, trade.recipient)
                edges[key] += trade.volume_usd or 0.0
                G.add_edge(trade.sender, trade.recipient, volume=edges[key])
                pair_timestamps[key].append(trade.block_timestamp)

            sccs = list(nx.strongly_connected_components(G))
            # Optimization: map addresses to SCC index for O(1) lookup
            addr_to_scc = {}
            for i, scc in enumerate(sccs):
                if len(scc) >= 2:
                    for addr in scc:
                        addr_to_scc[addr] = i

            for trade in pool_trades:
                scc_idx = addr_to_scc.get(trade.sender)
                if scc_idx is None or addr_to_scc.get(trade.recipient) != scc_idx:
                    continue

                window_minutes = settings.WASH_TRADE_TIME_WINDOW_MINUTES
                window_start = trade.block_timestamp - timedelta(minutes=window_minutes)
                window_end = trade.block_timestamp + timedelta(minutes=window_minutes)

                # Fast lookup of potential reverse trades using binary search O(log K)
                reverse_key = (trade.recipient, trade.sender)
                timestamps = pair_timestamps.get(reverse_key, [])
                if timestamps:
                    idx1 = bisect_left(timestamps, window_start)
                    idx2 = bisect_right(timestamps, window_end)
                    if idx2 > idx1:
                        trade.is_wash_trade = True
                        trade.wash_trade_score = 0.9
                        trade.detection_method = "circular_trading"
                        wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} circular trades")
        return wash_trades

    async def detect_high_frequency_bot(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """Detect high-frequency bot patterns with configurable thresholds."""
        wash_trades = []
        sender_groups = defaultdict(list)

        for trade in trades:
            sender_groups[trade.sender].append(trade)

        for sender, sender_trades in sender_groups.items():
            if sender.lower() in self.bot_allowlist:
                continue

            count_threshold = settings.BOT_TRADE_COUNT_THRESHOLD
            time_threshold = settings.BOT_TRADE_TIME_THRESHOLD_SECONDS
            cv_threshold = settings.BOT_VOLUME_CV_THRESHOLD

            if len(sender_trades) < count_threshold:
                continue

            # Optimization: trades are already sorted by block_timestamp from DB
            inter_trade_times = []
            volumes = []

            for i in range(1, len(sender_trades)):
                delta = (sender_trades[i].block_timestamp - sender_trades[i - 1].block_timestamp).total_seconds()
                inter_trade_times.append(delta)
                volumes.append(sender_trades[i].volume_usd or 0.0)

            if not inter_trade_times:
                continue

            avg_time = sum(inter_trade_times) / len(inter_trade_times)
            mean_vol = sum(volumes) / len(volumes) if volumes else 0
            volume_variance = sum((v - mean_vol) ** 2 for v in volumes) / len(volumes) if volumes else 0
            volume_std = volume_variance ** 0.5
            volume_cv = volume_std / (mean_vol + 1e-9)

            if avg_time < time_threshold and volume_cv < cv_threshold:
                for trade in sender_trades:
                    trade.is_wash_trade = True
                    trade.wash_trade_score = 0.8
                    trade.detection_method = "high_frequency_bot"
                    wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} bot trades")
        return wash_trades

    async def detect_volume_anomaly(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """Detect volume anomalies using MAD instead of z-score."""
        wash_trades = []
        min_trades = settings.VOLUME_ANOMALY_MIN_TRADES

        if len(trades) < min_trades:
            return wash_trades

        bucket_minutes = settings.VOLUME_ANOMALY_BUCKET_MINUTES
        pool_bucket_groups = defaultdict(list)

        for trade in trades:
            bucket = trade.block_timestamp.replace(
                minute=(trade.block_timestamp.minute // bucket_minutes) * bucket_minutes,
                second=0,
                microsecond=0,
            )
            key = (trade.pool_address, bucket)
            pool_bucket_groups[key].append(trade)

        for (pool, hour), bucket_trades in pool_bucket_groups.items():
            if len(bucket_trades) < min_trades:
                continue

            volumes = [t.volume_usd or 0.0 for t in bucket_trades]

            try:
                detector = RobustAnomalyDetector(method=settings.VOLUME_ANOMALY_METHOD)
                detector.fit(volumes)
            except (ValueError, RuntimeError):
                continue

            threshold = settings.VOLUME_ANOMALY_THRESHOLD

            for trade in bucket_trades:
                vol = trade.volume_usd or 0.0
                score = detector.score(vol)

                if detector.is_anomaly(vol, threshold):
                    trade.is_wash_trade = True
                    trade.wash_trade_score = min(0.7 + (score - threshold) * 0.05, 1.0)
                    trade.detection_method = "volume_anomaly"
                    wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} volume anomalies")
        return wash_trades

    async def detect_wash_clusters(
        self,
        trades: List[SwapTrade],
        address_clusters: List[AddressCluster],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """Detect trades within same address cluster."""
        wash_trades = []
        addr_to_cluster = {}

        for cluster in address_clusters:
            for addr in cluster.addresses:
                addr_to_cluster[addr.lower()] = cluster.cluster_id

        for trade in trades:
            sender_cluster = addr_to_cluster.get(trade.sender.lower())
            recipient_cluster = addr_to_cluster.get(trade.recipient.lower())

            if sender_cluster and recipient_cluster and sender_cluster == recipient_cluster:
                trade.is_wash_trade = True
                trade.wash_trade_score = 0.95
                trade.detection_method = "wash_cluster"
                wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} wash cluster trades")
        return wash_trades

    async def run_all_heuristics(
        self,
        chain_id: int,
        pool_address: str,
        session: AsyncSession,
    ) -> Tuple[List[SwapTrade], Dict[str, int]]:
        """Run all heuristic detectors and return combined results."""
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
        trades = list(result.scalars().all())

        if not trades:
            return [], {}

        stmt = select(AddressCluster).where(
            AddressCluster.cluster_id.like(f"{chain_id}:%")
        )
        result = await session.execute(stmt)
        clusters = list(result.scalars().all())

        all_wash_trades = []
        stats = {}

        detectors = [
            ("self_trading", self.detect_self_trading),
            ("circular_trading", self.detect_circular_trading),
            ("high_frequency_bot", self.detect_high_frequency_bot),
            ("volume_anomaly", self.detect_volume_anomaly),
        ]

        for name, detector in detectors:
            detected = await detector(trades, session)
            all_wash_trades.extend(detected)
            stats[name] = len(detected)

        if clusters:
            detected = await self.detect_wash_clusters(trades, clusters, session)
            all_wash_trades.extend(detected)
            stats["wash_cluster"] = len(detected)

        unique_wash_trades = list({t.id: t for t in all_wash_trades}.values())
        return unique_wash_trades, stats
