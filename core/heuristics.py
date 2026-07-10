"""
High-signal heuristic detection rules for wash trading.
Uses robust statistical methods (MAD/IQR) instead of z-score.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import timedelta

import networkx as nx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from core.advanced_heuristics import AdvancedHeuristicDetector, flag_trade
from models.schemas import AddressCluster, SwapTrade

logger = logging.getLogger(__name__)


def _load_allowlist() -> set[str]:
    """Load allowed bot addresses from settings."""
    return settings.bot_allowlist_set


class RobustAnomalyDetector:
    """
    Median Absolute Deviation (MAD) based anomaly detection.
    Robust to outliers, no normality assumption required.
    """

    def __init__(self, method: str = "mad"):
        self.method = method
        self.median: float | None = None
        self.mad: float | None = None
        self.q1: float | None = None
        self.q3: float | None = None
        self.iqr: float | None = None
        self._fitted = False

    def fit(self, volumes: list[float]) -> None:
        """Fit detector on log-transformed volumes."""
        if not volumes:
            raise ValueError("Cannot fit on empty data")

        log_volumes = [math.log1p(max(v, 0.0)) for v in volumes]

        if self.method == "mad":
            sorted_vals = sorted(log_volumes)
            n = len(sorted_vals)
            self.median = (
                sorted_vals[n // 2]
                if n % 2
                else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            )
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
            assert self.median is not None and self.mad is not None
            if self.mad == 0:
                return 0.0
            modified_z = 0.6745 * (log_vol - self.median) / self.mad
            return abs(modified_z)
        elif self.method == "iqr":
            assert self.q1 is not None and self.q3 is not None and self.iqr is not None
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

    def __init__(self) -> None:
        self.confidence_threshold = settings.SUSPICIOUS_ACTIVITY_THRESHOLD
        self.bot_allowlist = _load_allowlist()
        self.advanced = AdvancedHeuristicDetector()

    async def detect_self_trading(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
    ) -> list[SwapTrade]:
        """Detect trades where sender equals recipient."""
        wash_trades = []
        for trade in trades:
            if trade.sender.lower() == trade.recipient.lower():
                flag_trade(trade, 1.0, "self_trading")
                wash_trades.append(trade)
        logger.info(f"Detected {len(wash_trades)} self-trades")
        return wash_trades

    async def detect_circular_trading(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
    ) -> list[SwapTrade]:
        """Detect circular trading via strongly connected components."""
        wash_trades = []
        pool_groups = defaultdict(list)
        for trade in trades:
            pool_groups[trade.pool_address].append(trade)

        for _pool_address, pool_trades in pool_groups.items():
            G: nx.DiGraph[str] = nx.DiGraph()
            edges: dict[tuple[str, str], float] = defaultdict(float)
            for trade in pool_trades:
                key = (trade.sender, trade.recipient)
                edges[key] += trade.volume_usd or 0.0
                G.add_edge(trade.sender, trade.recipient, volume=edges[key])

            sccs = list(nx.strongly_connected_components(G))
            for scc in sccs:
                if len(scc) < 2:
                    continue
                scc_addresses = set(scc)
                for trade in pool_trades:
                    if trade.sender not in scc_addresses or trade.recipient not in scc_addresses:
                        continue
                    window_minutes = settings.WASH_TRADE_TIME_WINDOW_MINUTES
                    window_start = trade.block_timestamp - timedelta(minutes=window_minutes)
                    window_end = trade.block_timestamp + timedelta(minutes=window_minutes)
                    reverse_trades = [
                        t
                        for t in pool_trades
                        if t.sender == trade.recipient
                        and t.recipient == trade.sender
                        and window_start <= t.block_timestamp <= window_end
                    ]
                    if reverse_trades:
                        flag_trade(trade, 0.9, "circular_trading")
                        wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} circular trades")
        return wash_trades

    async def detect_high_frequency_bot(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
    ) -> list[SwapTrade]:
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

            sender_trades.sort(key=lambda t: t.block_timestamp)
            inter_trade_times = []
            volumes = []

            for i in range(1, len(sender_trades)):
                delta = (
                    sender_trades[i].block_timestamp - sender_trades[i - 1].block_timestamp
                ).total_seconds()
                inter_trade_times.append(delta)
                volumes.append(sender_trades[i].volume_usd or 0.0)

            if not inter_trade_times:
                continue

            avg_time = sum(inter_trade_times) / len(inter_trade_times)
            mean_vol = sum(volumes) / len(volumes) if volumes else 0
            volume_variance = (
                sum((v - mean_vol) ** 2 for v in volumes) / len(volumes) if volumes else 0
            )
            volume_std = volume_variance**0.5
            volume_cv = volume_std / (mean_vol + 1e-9)

            if avg_time < time_threshold and volume_cv < cv_threshold:
                for trade in sender_trades:
                    flag_trade(trade, 0.8, "high_frequency_bot")
                    wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} bot trades")
        return wash_trades

    async def detect_volume_anomaly(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
    ) -> list[SwapTrade]:
        """Detect volume anomalies using MAD instead of z-score."""
        wash_trades: list[SwapTrade] = []
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

        for (_pool, _hour), bucket_trades in pool_bucket_groups.items():
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
                    flag_trade(trade, min(0.7 + (score - threshold) * 0.05, 1.0), "volume_anomaly")
                    wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} volume anomalies")
        return wash_trades

    async def detect_wash_clusters(
        self,
        trades: list[SwapTrade],
        address_clusters: list[AddressCluster],
        session: AsyncSession,
    ) -> list[SwapTrade]:
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
                flag_trade(trade, 0.95, "wash_cluster")
                wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} wash cluster trades")
        return wash_trades

    async def run_all_heuristics(
        self,
        chain_id: int,
        pool_address: str,
        session: AsyncSession,
    ) -> tuple[list[SwapTrade], dict[str, int]]:
        """Run all heuristic detectors and return combined results."""
        trades_stmt = (
            select(SwapTrade)
            .where(
                and_(
                    SwapTrade.chain_id == chain_id,
                    SwapTrade.pool_address == pool_address,
                )
            )
            .order_by(SwapTrade.block_timestamp)
        )
        trades_result = await session.execute(trades_stmt)
        trades = list(trades_result.scalars().all())

        if not trades:
            return [], {}

        clusters_stmt = select(AddressCluster).where(
            AddressCluster.cluster_id.like(f"{chain_id}:%")
        )
        clusters_result = await session.execute(clusters_stmt)
        clusters = list(clusters_result.scalars().all())

        all_wash_trades: list[SwapTrade] = []
        stats: dict[str, int] = {}

        detectors = [
            ("self_trading", self.detect_self_trading),
            ("circular_trading", self.detect_circular_trading),
            ("position_neutral_scc", self.advanced.detect_position_neutral_scc),
            ("closed_cluster", self.advanced.detect_closed_cluster),
            ("repeated_amounts", self.advanced.detect_repeated_amounts),
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
