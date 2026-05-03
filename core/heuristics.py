"""
High-signal heuristic detection rules for wash trading.

This module provides rule-based detection of potential wash trading patterns:
- Self-trading detection (same sender and recipient)
- Circular trading detection (trading pairs between same addresses)
- High-frequency bot detection (rapid trades with low volume variance)
- Volume anomaly detection (statistical outliers in trade volumes)
- Wash cluster detection (trades within entity clusters)

Each detector returns trades flagged with confidence scores and detection method.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from models.schemas import AddressCluster, SwapTrade
from config.settings import settings

logger = logging.getLogger(__name__)

# ==============================================================================
# Detection Constants
# ==============================================================================

# Self-trading detection
SELF_TRADE_SCORE: float = 1.0
SELF_TRADE_METHOD: str = "self_trading"

# Circular trading detection
CIRCULAR_TRADE_SCORE: float = 0.9
CIRCULAR_TRADE_METHOD: str = "circular_trading"

# High-frequency bot detection
BOT_MIN_TRADE_COUNT: int = 10
BOT_TIME_THRESHOLD_SECONDS: float = 60.0
BOT_VOLUME_CV_THRESHOLD: float = 0.5
BOT_TRADE_SCORE: float = 0.8
BOT_TRADE_METHOD: str = "high_frequency_bot"

# Volume anomaly detection
VOLUME_ANOMALY_MIN_TRADES: int = 20
VOLUME_ANOMALY_MIN_HOUR_TRADES: int = 5
VOLUME_ANOMALY_Z_THRESHOLD: float = 3.0
VOLUME_ANOMALY_Z_BONUS: float = 0.1
VOLUME_ANOMALY_BASE_SCORE: float = 0.7
VOLUME_ANOMALY_MAX_SCORE: float = 1.0
VOLUME_ANOMALY_METHOD: str = "volume_anomaly"

# Wash cluster detection
WASH_CLUSTER_SCORE: float = 0.95
WASH_CLUSTER_METHOD: str = "wash_cluster"

# Tolerance for float comparisons
EPSILON: float = 1e-9


# ==============================================================================
# Exceptions
# ==============================================================================

class HeuristicError(Exception):
    """Base exception for heuristic detection errors."""
    pass


class DetectionTimeoutError(HeuristicError):
    """Raised when detection takes too long."""
    pass


# ==============================================================================
# Helper Functions
# ==============================================================================

def _load_allowlist() -> Set[str]:
    """
    Load allowed bot addresses from environment variable.

    Addresses should be comma-separated. Returns lowercase set for
    case-insensitive matching.

    Returns:
        Set of lowercase address strings
    """
    env_val = os.getenv("BOT_ALLOWLIST", "")
    if not env_val:
        return set()
    return {addr.strip().lower() for addr in env_val.split(",") if addr.strip()}


def _calculate_coefficient_of_variation(values: List[float]) -> float:
    """
    Calculate coefficient of variation (CV) for a list of values.

    CV = standard_deviation / mean
    Used to measure relative variability regardless of scale.

    Args:
        values: List of numeric values

    Returns:
        Coefficient of variation, or 0 if mean is near zero
    """
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if abs(mean) < EPSILON:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std_dev = variance ** 0.5
    return std_dev / mean


def _calculate_z_score(value: float, mean: float, std_dev: float) -> float:
    """
    Calculate z-score for a value.

    Args:
        value: Value to score
        mean: Distribution mean
        std_dev: Distribution standard deviation

    Returns:
        Z-score (number of standard deviations from mean)
    """
    if abs(std_dev) < EPSILON:
        return 0.0
    return abs(value - mean) / std_dev


# ==============================================================================
# Heuristic Detector
# ==============================================================================

@dataclass
class DetectionResult:
    """Result of a heuristic detection run."""

    wash_trades: List[SwapTrade]
    stats: Dict[str, int]
    duration_seconds: float
    methods_used: List[str]


class HeuristicDetector:
    """
    Rule-based wash trade detection using heuristic patterns.

    Provides multiple detection methods that can be run individually
    or combined. Each method has configurable thresholds and returns
    trades with confidence scores.

    Attributes:
        confidence_threshold: Minimum score to flag as wash trade
        bot_allowlist: Set of addresses to skip in bot detection

    Example:
        >>> detector = HeuristicDetector()
        >>> async with storage.get_session() as session:
        ...     wash_trades, stats = await detector.run_all_heuristics(
        ...         chain_id=1, pool_address="0x...", session=session
        ...     )
    """

    def __init__(
        self,
        confidence_threshold: Optional[float] = None,
        time_window_minutes: Optional[int] = None,
    ):
        """
        Initialize heuristic detector.

        Args:
            confidence_threshold: Minimum score threshold (default from settings)
            time_window_minutes: Circular trade time window (default from settings)
        """
        self.confidence_threshold: float = (
            confidence_threshold
            if confidence_threshold is not None
            else settings.SUSPICIOUS_ACTIVITY_THRESHOLD
        )
        self.time_window_minutes: int = (
            time_window_minutes
            if time_window_minutes is not None
            else settings.WASH_TRADE_TIME_WINDOW_MINUTES
        )
        self.bot_allowlist: Set[str] = _load_allowlist()
        self._detection_count: int = 0

    async def detect_self_trading(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """
        Detect self-trading (sender equals recipient).

        The simplest wash trade pattern where an address trades
        with itself. Always flagged with maximum confidence.

        Args:
            trades: List of trades to analyze
            session: Database session (unused but required for interface)

        Returns:
            List of trades flagged as self-trading
        """
        wash_trades: List[SwapTrade] = []
        for trade in trades:
            if trade.sender.lower() == trade.recipient.lower():
                trade.is_wash_trade = True
                trade.wash_trade_score = SELF_TRADE_SCORE
                trade.detection_method = SELF_TRADE_METHOD
                wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} self-trades")
        self._detection_count += 1
        return wash_trades

    async def detect_circular_trading(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """
        Detect circular trading patterns.

        Identifies pairs of addresses that trade back and forth
        within a time window, creating artificial volume.

        Args:
            trades: List of trades to analyze
            session: Database session

        Returns:
            List of trades flagged as circular trading
        """
        wash_trades: List[SwapTrade] = []
        pool_groups: Dict[str, List[SwapTrade]] = defaultdict(list)

        # Group trades by pool
        for trade in trades:
            pool_groups[trade.pool_address].append(trade)

        for pool_address, pool_trades in pool_groups.items():
            if len(pool_trades) < 2:
                continue

            # Build directed graph of trading relationships
            G = nx.DiGraph()
            edges: Dict[Tuple[str, str], float] = defaultdict(float)

            for trade in pool_trades:
                key = (trade.sender, trade.recipient)
                edges[key] += trade.volume_usd or 0.0
                G.add_edge(trade.sender, trade.recipient, volume=edges[key])

            # Find strongly connected components
            sccs = list(nx.strongly_connected_components(G))

            for scc in sccs:
                if len(scc) < 2:
                    continue

                scc_addresses = set(scc)
                time_window = timedelta(minutes=self.time_window_minutes)

                for trade in pool_trades:
                    # Only check trades within the SCC
                    if trade.sender not in scc_addresses or trade.recipient not in scc_addresses:
                        continue

                    window_start = trade.block_timestamp - time_window
                    window_end = trade.block_timestamp + time_window

                    # Look for reverse trade in the window
                    reverse_trades = [
                        t for t in pool_trades
                        if t.sender == trade.recipient
                        and t.recipient == trade.sender
                        and window_start <= t.block_timestamp <= window_end
                    ]

                    if reverse_trades:
                        trade.is_wash_trade = True
                        trade.wash_trade_score = CIRCULAR_TRADE_SCORE
                        trade.detection_method = CIRCULAR_TRADE_METHOD
                        wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} circular trades")
        self._detection_count += 1
        return wash_trades

    async def detect_high_frequency_bot(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """
        Detect high-frequency trading bots.

        Identifies addresses that trade rapidly with similar volumes,
        suggesting programmatic (bot) trading activity often used
        in wash trading schemes.

        Detection criteria:
        - Minimum 10 trades from same sender
        - Average inter-trade time < 60 seconds
        - Volume coefficient of variation < 0.5

        Args:
            trades: List of trades to analyze
            session: Database session (unused)

        Returns:
            List of trades flagged as bot activity
        """
        wash_trades: List[SwapTrade] = []
        sender_groups: Dict[str, List[SwapTrade]] = defaultdict(list)

        # Group trades by sender
        for trade in trades:
            sender_groups[trade.sender].append(trade)

        for sender, sender_trades in sender_groups.items():
            # Skip allowlisted addresses
            if sender.lower() in self.bot_allowlist:
                continue

            # Require minimum number of trades
            if len(sender_trades) < BOT_MIN_TRADE_COUNT:
                continue

            # Sort by timestamp
            sender_trades.sort(key=lambda t: t.block_timestamp)

            # Calculate inter-trade times and volumes
            inter_trade_times: List[float] = []
            volumes: List[float] = []

            for i in range(1, len(sender_trades)):
                delta = (
                    sender_trades[i].block_timestamp - sender_trades[i - 1].block_timestamp
                ).total_seconds()
                inter_trade_times.append(delta)
                volumes.append(sender_trades[i].volume_usd or 0.0)

            if not inter_trade_times:
                continue

            # Calculate average inter-trade time
            avg_time = sum(inter_trade_times) / len(inter_trade_times)

            # Calculate volume coefficient of variation
            volume_cv = _calculate_coefficient_of_variation(volumes)

            # Detect bot pattern: fast trades with similar volumes
            if avg_time < BOT_TIME_THRESHOLD_SECONDS and volume_cv < BOT_VOLUME_CV_THRESHOLD:
                for trade in sender_trades:
                    trade.is_wash_trade = True
                    trade.wash_trade_score = BOT_TRADE_SCORE
                    trade.detection_method = BOT_TRADE_METHOD
                    wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} bot trades")
        self._detection_count += 1
        return wash_trades

    async def detect_volume_anomaly(
        self,
        trades: List[SwapTrade],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """
        Detect volume anomalies using statistical analysis.

        Groups trades by pool and hour, then identifies trades with
        volumes that are statistical outliers (high z-score).

        Args:
            trades: List of trades to analyze
            session: Database session (unused)

        Returns:
            List of trades flagged as volume anomalies
        """
        wash_trades: List[SwapTrade] = []

        if len(trades) < VOLUME_ANOMALY_MIN_TRADES:
            return wash_trades

        # Group trades by pool and hour
        pool_hour_groups: Dict[Tuple[str, datetime], List[SwapTrade]] = defaultdict(list)
        for trade in trades:
            hour_bucket = trade.block_timestamp.replace(
                minute=0, second=0, microsecond=0
            )
            key = (trade.pool_address, hour_bucket)
            pool_hour_groups[key].append(trade)

        for (pool, hour), hour_trades in pool_hour_groups.items():
            if len(hour_trades) < VOLUME_ANOMALY_MIN_HOUR_TRADES:
                continue

            # Extract volumes and calculate statistics
            volumes = [t.volume_usd or 0.0 for t in hour_trades]
            mean_vol = sum(volumes) / len(volumes)
            variance = sum((v - mean_vol) ** 2 for v in volumes) / len(volumes)
            std_vol = variance ** 0.5

            # Flag anomalies
            for trade, vol in zip(hour_trades, volumes):
                z_score = _calculate_z_score(vol, mean_vol, std_vol)

                if z_score > VOLUME_ANOMALY_Z_THRESHOLD:
                    trade.is_wash_trade = True
                    # Scale score based on z-score magnitude
                    score = min(
                        VOLUME_ANOMALY_BASE_SCORE
                        + (z_score - VOLUME_ANOMALY_Z_THRESHOLD) * VOLUME_ANOMALY_Z_BONUS,
                        VOLUME_ANOMALY_MAX_SCORE,
                    )
                    trade.wash_trade_score = score
                    trade.detection_method = VOLUME_ANOMALY_METHOD
                    wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} volume anomalies")
        self._detection_count += 1
        return wash_trades

    async def detect_wash_clusters(
        self,
        trades: List[SwapTrade],
        address_clusters: List[AddressCluster],
        session: AsyncSession,
    ) -> List[SwapTrade]:
        """
        Detect trades within address clusters.

        Uses entity clustering to identify trades where both sender
        and recipient belong to the same cluster (likely same entity).

        Args:
            trades: List of trades to analyze
            address_clusters: Pre-computed address clusters
            session: Database session (unused)

        Returns:
            List of trades flagged as wash cluster activity
        """
        wash_trades: List[SwapTrade] = []

        # Build address to cluster mapping
        addr_to_cluster: Dict[str, str] = {}
        for cluster in address_clusters:
            for addr in cluster.addresses:
                addr_to_cluster[addr.lower()] = cluster.cluster_id

        for trade in trades:
            sender_cluster = addr_to_cluster.get(trade.sender.lower())
            recipient_cluster = addr_to_cluster.get(trade.recipient.lower())

            if sender_cluster and recipient_cluster and sender_cluster == recipient_cluster:
                trade.is_wash_trade = True
                trade.wash_trade_score = WASH_CLUSTER_SCORE
                trade.detection_method = WASH_CLUSTER_METHOD
                wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} wash cluster trades")
        self._detection_count += 1
        return wash_trades

    async def run_all_heuristics(
        self,
        chain_id: int,
        pool_address: str,
        session: AsyncSession,
    ) -> Tuple[List[SwapTrade], Dict[str, int]]:
        """
        Run all enabled heuristic detectors.

        Fetches trades for the pool, runs each detection method,
        and returns deduplicated results.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address
            session: Database session

        Returns:
            Tuple of (flagged_trades, detection_stats)

        Raises:
            HeuristicError: If detection fails
        """
        # Fetch trades for this pool
        stmt = select(SwapTrade).where(
            and_(
                SwapTrade.chain_id == chain_id,
                SwapTrade.pool_address == pool_address,
            )
        ).order_by(SwapTrade.block_timestamp)

        result = await session.execute(stmt)
        trades = result.scalars().all()

        if not trades:
            return [], {}

        # Fetch address clusters for this chain
        stmt = select(AddressCluster).where(
            AddressCluster.cluster_id.like(f"{chain_id}:%")
        )
        result = await session.execute(stmt)
        clusters = result.scalars().all()

        # Run all detectors
        all_wash_trades: List[SwapTrade] = []
        stats: Dict[str, int] = {}

        detectors: List[Tuple[str, callable]] = [
            ("self_trading", self.detect_self_trading),
            ("circular_trading", self.detect_circular_trading),
            ("high_frequency_bot", self.detect_high_frequency_bot),
            ("volume_anomaly", self.detect_volume_anomaly),
        ]

        for name, detector in detectors:
            try:
                wash_trades = await detector(trades, session)
                all_wash_trades.extend(wash_trades)
                stats[name] = len(wash_trades)
            except Exception as e:
                logger.error(f"Detector {name} failed: {e}")
                stats[name] = 0

        # Run wash cluster detection if clusters exist
        if clusters:
            try:
                wash_trades = await self.detect_wash_clusters(trades, clusters, session)
                all_wash_trades.extend(wash_trades)
                stats["wash_cluster"] = len(wash_trades)
            except Exception as e:
                logger.error(f"Detector wash_cluster failed: {e}")
                stats["wash_cluster"] = 0

        # Deduplicate by trade ID
        unique_wash_trades = list({t.id: t for t in all_wash_trades}.values())

        logger.info(
            f"Total wash trades detected: {len(unique_wash_trades)} "
            f"(from {len(all_wash_trades)} detections)"
        )

        return unique_wash_trades, stats

    def get_detection_count(self) -> int:
        """Get number of detection runs performed."""
        return self._detection_count

    def reset_detection_count(self) -> None:
        """Reset the detection counter."""
        self._detection_count = 0
