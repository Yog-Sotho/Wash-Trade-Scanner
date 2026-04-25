"""
High-signal heuristic detection rules for wash trading.
"""

import logging
import os
from typing import List, Dict, Any, Set, Tuple, Optional
from datetime import datetime, timedelta
from collections import defaultdict

import networkx as nx
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from models.schemas import SwapTrade, AddressCluster
from config.settings import settings

logger = logging.getLogger(__name__)


def _load_allowlist() -> Set[str]:
    """Load allowed bot addresses from env var (comma-separated)."""
    env_val = os.getenv("BOT_ALLOWLIST", "")
    if not env_val:
        return set()
    return {addr.strip().lower() for addr in env_val.split(",") if addr.strip()}


class HeuristicDetector:
    def __init__(self):
        self.confidence_threshold = settings.SUSPICIOUS_ACTIVITY_THRESHOLD
        self.bot_allowlist = _load_allowlist()

    async def detect_self_trading(
        self,
        trades: List[SwapTrade],
        session: AsyncSession
    ) -> List[SwapTrade]:
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
        session: AsyncSession
    ) -> List[SwapTrade]:
        wash_trades = []
        pool_groups = defaultdict(list)
        for trade in trades:
            pool_groups[trade.pool_address].append(trade)
        for pool_address, pool_trades in pool_groups.items():
            G = nx.DiGraph()
            edges = defaultdict(float)
            for trade in pool_trades:
                key = (trade.sender, trade.recipient)
                edges[key] += trade.volume_usd or 0.0
                G.add_edge(trade.sender, trade.recipient, volume=edges[key])
            sccs = list(nx.strongly_connected_components(G))
            for scc in sccs:
                if len(scc) > 1:
                    scc_addresses = set(scc)
                    for trade in pool_trades:
                        if trade.sender in scc_addresses and trade.recipient in scc_addresses:
                            window_start = trade.block_timestamp - timedelta(minutes=settings.WASH_TRADE_TIME_WINDOW_MINUTES)
                            window_end = trade.block_timestamp + timedelta(minutes=settings.WASH_TRADE_TIME_WINDOW_MINUTES)
                            reverse_trades = [
                                t for t in pool_trades
                                if t.sender == trade.recipient
                                and t.recipient == trade.sender
                                and window_start <= t.block_timestamp <= window_end
                            ]
                            if reverse_trades:
                                trade.is_wash_trade = True
                                trade.wash_trade_score = 0.9
                                trade.detection_method = "circular_trading"
                                wash_trades.append(trade)
        logger.info(f"Detected {len(wash_trades)} circular trades")
        return wash_trades

    async def detect_high_frequency_bot(
        self,
        trades: List[SwapTrade],
        session: AsyncSession
    ) -> List[SwapTrade]:
        wash_trades = []
        sender_groups = defaultdict(list)
        for trade in trades:
            sender_groups[trade.sender].append(trade)
        for sender, sender_trades in sender_groups.items():
            # Skip allowlisted addresses
            if sender.lower() in self.bot_allowlist:
                continue
            if len(sender_trades) < 10:
                continue
            sender_trades.sort(key=lambda t: t.block_timestamp)
            inter_trade_times = []
            volumes = []
            for i in range(1, len(sender_trades)):
                delta = (sender_trades[i].block_timestamp - sender_trades[i-1].block_timestamp).total_seconds()
                inter_trade_times.append(delta)
                volumes.append(sender_trades[i].volume_usd or 0.0)
            if not inter_trade_times:
                continue
            avg_time = sum(inter_trade_times) / len(inter_trade_times)
            volume_variance = sum((v - sum(volumes)/len(volumes))**2 for v in volumes) / len(volumes) if volumes else 0
            volume_cv = volume_variance**0.5 / (sum(volumes)/len(volumes) + 1e-9)
            if avg_time < 60 and volume_cv < 0.5:
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
        session: AsyncSession
    ) -> List[SwapTrade]:
        wash_trades = []
        if len(trades) < 20:
            return wash_trades
        pool_hour_groups = defaultdict(list)
        for trade in trades:
            hour_bucket = trade.block_timestamp.replace(minute=0, second=0, microsecond=0)
            key = (trade.pool_address, hour_bucket)
            pool_hour_groups[key].append(trade)
        for (pool, hour), hour_trades in pool_hour_groups.items():
            if len(hour_trades) < 5:
                continue
            volumes = [t.volume_usd or 0.0 for t in hour_trades]
            mean_vol = sum(volumes) / len(volumes)
            std_vol = (sum((v - mean_vol)**2 for v in volumes) / len(volumes))**0.5
            z_threshold = 3.0
            for trade, vol in zip(hour_trades, volumes):
                z_score = abs(vol - mean_vol) / (std_vol + 1e-9)
                if z_score > z_threshold:
                    trade.is_wash_trade = True
                    trade.wash_trade_score = min(0.7 + (z_score - z_threshold) * 0.1, 1.0)
                    trade.detection_method = "volume_anomaly"
                    wash_trades.append(trade)
        logger.info(f"Detected {len(wash_trades)} volume anomalies")
        return wash_trades

    async def detect_wash_clusters(
        self,
        trades: List[SwapTrade],
        address_clusters: List[AddressCluster],
        session: AsyncSession
    ) -> List[SwapTrade]:
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
        session: AsyncSession
    ) -> Tuple[List[SwapTrade], Dict[str, int]]:
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
        stmt = select(AddressCluster).where(
            AddressCluster.cluster_id.like(f"{chain_id}:%")
        )
        result = await session.execute(stmt)
        clusters = result.scalars().all()
        all_wash_trades = []
        stats = {}
        detectors = [
            ("self_trading", self.detect_self_trading),
            ("circular_trading", self.detect_circular_trading),
            ("high_frequency_bot", self.detect_high_frequency_bot),
            ("volume_anomaly", self.detect_volume_anomaly),
        ]
        for name, detector in detectors:
            wash_trades = await detector(trades, session)
            all_wash_trades.extend(wash_trades)
            stats[name] = len(wash_trades)
        if clusters:
            wash_trades = await self.detect_wash_clusters(trades, clusters, session)
            all_wash_trades.extend(wash_trades)
            stats["wash_cluster"] = len(wash_trades)
        unique_wash_trades = list({t.id: t for t in all_wash_trades}.values())
        return unique_wash_trades, stats
