"""
High-signal heuristic detection rules for wash trading.
Uses robust statistical methods (MAD/IQR) instead of z-score.
"""

from __future__ import annotations

import logging
import math
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timedelta

import networkx as nx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from core.advanced_heuristics import AdvancedHeuristicDetector, flag_trade, trader_of
from core.calibration import wash_probability
from core.evidence import EvidenceLog, Finding
from core.mev import without_sandwiches
from models.schemas import AddressCluster, SwapTrade

logger = logging.getLogger(__name__)

# The stats key counting trades taken out as MEV before detection: not a
# detector's hits.
EXCLUDED_MEV = "excluded_mev_sandwich"


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
    """Wash trade detection using configurable heuristics.

    `cross_pool`: judge the entity- and time-based detectors per token pair
    across the pools given, not per pool (AdvancedHeuristicDetector)."""

    def __init__(self, cross_pool: bool = False) -> None:
        self.cross_pool = cross_pool
        self.confidence_threshold = settings.SUSPICIOUS_ACTIVITY_THRESHOLD
        self.bot_allowlist = _load_allowlist()
        self.advanced = AdvancedHeuristicDetector(cross_pool=cross_pool)

    async def detect_circular_trading(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        evidence: EvidenceLog | None = None,
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

            # A trade is circular when both ends are in one strongly connected
            # component (of 2+ addresses) and the reverse leg (recipient ->
            # sender) occurs within the window around it. Reverse legs are
            # looked up in each pair's sorted times: O(n log n), where scanning
            # every trade for each candidate was O(n^2).
            component: dict[str, int] = {}
            sizes: dict[int, int] = {}
            for index, scc in enumerate(nx.strongly_connected_components(G)):
                if len(scc) >= 2:
                    component.update(dict.fromkeys(scc, index))
                    sizes[index] = len(scc)
            candidates: dict[int, list[SwapTrade]] = defaultdict(list)
            for trade in pool_trades:
                index = component.get(trade.sender, -1)
                if index >= 0 and component.get(trade.recipient) == index:
                    candidates[index].append(trade)
            if not candidates:
                continue

            by_pair: dict[tuple[str, str], list[SwapTrade]] = defaultdict(list)
            for trade in pool_trades:
                by_pair[(trade.sender, trade.recipient)].append(trade)
            pair_times: dict[tuple[str, str], list[datetime]] = {}
            for pair, pair_trades in by_pair.items():
                pair_trades.sort(key=lambda t: t.block_timestamp)
                pair_times[pair] = [t.block_timestamp for t in pair_trades]
            window = timedelta(minutes=settings.WASH_TRADE_TIME_WINDOW_MINUTES)
            # Component by component, each in trade order: the order the
            # per-component scan produced.
            for index in sorted(candidates):
                for trade in candidates[index]:
                    reverse_pair = (trade.recipient, trade.sender)
                    reverse = pair_times.get(reverse_pair, [])
                    first = bisect_left(reverse, trade.block_timestamp - window)
                    if first < bisect_right(reverse, trade.block_timestamp + window):
                        flag_trade(trade, 0.9, "circular_trading")
                        wash_trades.append(trade)
                        if evidence is not None:
                            partner = by_pair[reverse_pair][first]
                            evidence.add(
                                Finding(
                                    detector="circular_trading",
                                    trades=[trade, partner],
                                    entity=f"{trade.sender}->{trade.recipient}",
                                    summary=(
                                        f"Swap from {trade.sender} to {trade.recipient}, both in "
                                        f"one cycle of {sizes[index]} swap senders and "
                                        f"recipients, reversed within "
                                        f"{settings.WASH_TRADE_TIME_WINDOW_MINUTES} minutes"
                                    ),
                                    details={
                                        "cycle_size": sizes[index],
                                        "window_minutes": settings.WASH_TRADE_TIME_WINDOW_MINUTES,
                                    },
                                )
                            )

        logger.info(f"Detected {len(wash_trades)} circular trades")
        return wash_trades

    async def detect_high_frequency_bot(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        evidence: EvidenceLog | None = None,
    ) -> list[SwapTrade]:
        """Detect high-frequency bot patterns with configurable thresholds."""
        wash_trades = []
        trader_groups: dict[str, list[SwapTrade]] = defaultdict(list)

        for trade in trades:
            trader_groups[trader_of(trade)].append(trade)

        for trader, sender_trades in trader_groups.items():
            if trader in self.bot_allowlist:
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
                if evidence is not None:
                    evidence.add(
                        Finding(
                            detector="high_frequency_bot",
                            trades=list(sender_trades),
                            entity=trader,
                            summary=(
                                f"{len(sender_trades)} trades on average {avg_time:.0f} s apart "
                                f"(threshold {time_threshold} s), volumes varying by a "
                                f"coefficient of {volume_cv:.2f} (threshold {cv_threshold})"
                            ),
                            details={
                                "trades": len(sender_trades),
                                "mean_interval_seconds": round(avg_time, 3),
                                "volume_cv": round(volume_cv, 6),
                            },
                        )
                    )

        logger.info(f"Detected {len(wash_trades)} bot trades")
        return wash_trades

    async def detect_volume_anomaly(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        evidence: EvidenceLog | None = None,
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
                    if evidence is not None:
                        evidence.add(
                            Finding(
                                detector="volume_anomaly",
                                trades=[trade],
                                entity=trader_of(trade),
                                summary=(
                                    f"Volume ${vol:,.2f} scores {score:.1f} "
                                    f"({settings.VOLUME_ANOMALY_METHOD}) among the "
                                    f"{len(bucket_trades)} trades of its {bucket_minutes}-minute "
                                    f"bucket, over the threshold {threshold}"
                                ),
                                details={
                                    "score": round(float(score), 6),
                                    "threshold": threshold,
                                    "bucket_minutes": bucket_minutes,
                                    "bucket_trades": len(bucket_trades),
                                },
                            )
                        )

        logger.info(f"Detected {len(wash_trades)} volume anomalies")
        return wash_trades

    async def detect_wash_clusters(
        self,
        trades: list[SwapTrade],
        address_clusters: list[AddressCluster],
        session: AsyncSession,
        evidence: EvidenceLog | None = None,
    ) -> list[SwapTrade]:
        """Detect swaps whose output a trader sends to another address of its
        own funding cluster: value moved between one entity's wallets through
        the pool."""
        wash_trades = []
        addr_to_cluster = {}

        for cluster in address_clusters:
            for addr in cluster.addresses:
                addr_to_cluster[addr.lower()] = cluster.cluster_id

        for trade in trades:
            trader = trader_of(trade)
            recipient = trade.recipient.lower()
            if trader == recipient:
                continue  # a trader receiving its own output is ordinary
            trader_cluster = addr_to_cluster.get(trader)
            recipient_cluster = addr_to_cluster.get(recipient)

            if trader_cluster and recipient_cluster and trader_cluster == recipient_cluster:
                flag_trade(trade, 0.95, "wash_cluster")
                wash_trades.append(trade)
                if evidence is not None:
                    evidence.add(
                        Finding(
                            detector="wash_cluster",
                            trades=[trade],
                            entity=f"cluster:{trader_cluster}",
                            summary=(
                                f"The swap's output went to {recipient}, linked by funding to "
                                f"the trader {trader} (cluster {trader_cluster})"
                            ),
                            details={"recipient": recipient, "cluster_id": trader_cluster},
                        )
                    )

        logger.info(f"Detected {len(wash_trades)} wash cluster trades")
        return wash_trades

    async def run_detectors_on_trades(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        clusters: list[AddressCluster] | None = None,
    ) -> tuple[list[SwapTrade], dict[str, int]]:
        """Run every detector over an in-memory trade list and score the hits.

        Each trade's wash_trade_score is the calibrated probability that it is
        wash given the detectors that fired on it (core/calibration.py), and
        detection_method names the most probative of them. Only trades whose
        probability reaches SUSPICIOUS_ACTIVITY_THRESHOLD are labelled wash;
        a detector whose measured false-positive rate is too high for its
        recall therefore cannot label a trade on its own.

        Labels the trades carried in are discarded: the result reflects this
        run only. Used both for full-pool audits and for the rolling window of
        the real-time monitor. Returns (wash trades, hits per detector).
        """
        if not trades:
            return [], {}

        stats: dict[str, int] = {}
        hits: dict[int, list[str]] = defaultdict(list)
        evidence = EvidenceLog()
        all_trades = trades
        # Sandwich attackers' legs are MEV, not wash: no detector sees them
        # (core/mev.py). They are counted, and never labelled.
        trades, sandwiches = without_sandwiches(trades)
        stats[EXCLUDED_MEV] = 2 * len(sandwiches)

        detectors = [
            ("round_trip", self.advanced.detect_round_trips),
            ("circular_trading", self.detect_circular_trading),
            ("closed_cluster", self.advanced.detect_closed_cluster),
            ("repeated_amounts", self.advanced.detect_repeated_amounts),
            ("high_frequency_bot", self.detect_high_frequency_bot),
            ("volume_anomaly", self.detect_volume_anomaly),
        ]

        for name, detector in detectors:
            detected = await detector(trades, session, evidence=evidence)
            for trade in {t.id: t for t in detected}.values():
                hits[trade.id].append(name)
            stats[name] = len({t.id for t in detected})

        for name, entity_detector in (
            ("position_neutral", self.advanced.detect_position_neutral),
            ("coordinated_trading", self.advanced.detect_coordinated_trading),
        ):
            detected = await entity_detector(trades, session, clusters, evidence=evidence)
            for trade in {t.id: t for t in detected}.values():
                hits[trade.id].append(name)
            stats[name] = len({t.id for t in detected})

        if clusters:
            detected = await self.detect_wash_clusters(trades, clusters, session, evidence=evidence)
            for trade in {t.id: t for t in detected}.values():
                hits[trade.id].append("wash_cluster")
            stats["wash_cluster"] = len({t.id for t in detected})

        prior = settings.WASH_PRIOR
        wash_trades: list[SwapTrade] = []
        for trade in all_trades:
            trade.is_wash_trade = False
            trade.wash_trade_score = 0.0
            trade.detection_method = None
            trade.wash_evidence = None
            methods = hits.get(trade.id)
            if not methods:
                continue
            score, method = max((wash_probability(m, prior), m) for m in methods)
            trade.wash_trade_score = score
            trade.detection_method = method
            if score >= self.confidence_threshold:
                trade.is_wash_trade = True
                # Why, for every labelled trade: each detector that fired on it,
                # what it saw and what its hit is worth (core/evidence.py).
                trade.wash_evidence = evidence.bundle(
                    trade, methods, prior, self.confidence_threshold
                )
                wash_trades.append(trade)
        return wash_trades, stats

    async def run_all_heuristics(
        self,
        chain_id: int,
        pool_address: str,
        session: AsyncSession,
    ) -> tuple[list[SwapTrade], dict[str, int]]:
        """Run all heuristic detectors over a pool's stored trades."""
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

        return await self.run_detectors_on_trades(trades, session, clusters)
