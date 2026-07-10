"""
Research-grade wash trade detectors.

Implements detection methods from the academic and forensic literature that go
beyond simple pairwise heuristics:

- Position-neutral SCC analysis (Victor & Weintraud, "Detecting and Quantifying
  Wash Trading on Decentralized Cryptocurrency Exchanges", WWW '21): within a
  strongly connected component of the trade graph, find time windows in which
  every participant's net token position change is approximately zero while
  gross traded volume is large — the legal definition of wash trading.

- Closed-cluster analysis (network-based detection, 2025): wash traders form
  approximately closed clusters of colluding counterparties that seldom
  transact with anyone outside the cluster. Communities whose trade volume is
  overwhelmingly internal are flagged.

- Repeated-amount fingerprinting: wash bots recycle identical (or
  near-identical) trade sizes; genuine order flow almost never repeats the
  same amount many times from one sender.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import timedelta

import networkx as nx
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from models.schemas import SwapTrade

logger = logging.getLogger(__name__)


def round_to_sig_figs(value: float, sig_figs: int) -> float:
    """Round a positive number to `sig_figs` significant digits."""
    if not value or not math.isfinite(value):
        return 0.0
    magnitude = math.floor(math.log10(abs(value)))
    factor = 10.0 ** (magnitude - sig_figs + 1)
    return round(value / factor) * factor


def flag_trade(trade: SwapTrade, score: float, method: str) -> None:
    """Mark a trade as wash, keeping the highest-confidence detection.

    Detectors run in sequence over the same ORM objects; without this guard a
    later low-confidence detector would overwrite a higher-confidence label.
    """
    current = trade.wash_trade_score or 0.0
    if score > current or not trade.is_wash_trade:
        trade.is_wash_trade = True
        trade.wash_trade_score = score
        trade.detection_method = method


class AdvancedHeuristicDetector:
    """Graph- and statistics-based wash trade detection."""

    async def detect_position_neutral_scc(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
    ) -> list[SwapTrade]:
        """Detect SCC time windows where every trader's net position is ~zero.

        Multi-pass over configurable window sizes (default 1h / 24h / 7d),
        mirroring the multi-window passes of Victor & Weintraud (WWW '21).
        A window of trades among SCC members is wash if, for every
        (address, token) pair touched, |net position change| is within
        POSITION_NEUTRAL_MARGIN of the gross volume moved.
        """
        wash_trades: list[SwapTrade] = []
        flagged: set[int] = set()

        pool_groups: dict[str, list[SwapTrade]] = defaultdict(list)
        for trade in trades:
            pool_groups[trade.pool_address].append(trade)

        for pool_trades in pool_groups.values():
            graph: nx.DiGraph[str] = nx.DiGraph()
            for trade in pool_trades:
                sender = trade.sender.lower()
                recipient = trade.recipient.lower()
                if sender != recipient:
                    graph.add_edge(sender, recipient)

            for scc in nx.strongly_connected_components(graph):
                if len(scc) < 2:
                    continue
                scc_trades = [
                    t for t in pool_trades if t.sender.lower() in scc and t.recipient.lower() in scc
                ]
                for window_hours in settings.POSITION_NEUTRAL_WINDOWS_HOURS:
                    window_seconds = timedelta(hours=window_hours).total_seconds()
                    buckets: dict[int, list[SwapTrade]] = defaultdict(list)
                    for t in scc_trades:
                        bucket = int(t.block_timestamp.timestamp() // window_seconds)
                        buckets[bucket].append(t)

                    for bucket_trades in buckets.values():
                        if len(bucket_trades) < settings.POSITION_NEUTRAL_MIN_TRADES:
                            continue
                        if not self._is_position_neutral(bucket_trades):
                            continue
                        for t in bucket_trades:
                            if t.id in flagged:
                                continue
                            flagged.add(t.id)
                            flag_trade(t, 0.95, "position_neutral_scc")
                            wash_trades.append(t)

        logger.info(f"Detected {len(wash_trades)} position-neutral SCC trades")
        return wash_trades

    @staticmethod
    def _is_position_neutral(trades: list[SwapTrade]) -> bool:
        """True if every (address, token) net position change is within margin."""
        net: dict[tuple[str, str], float] = defaultdict(float)
        gross: dict[tuple[str, str], float] = defaultdict(float)

        for t in trades:
            amount_in = t.amount_in or 0.0
            amount_out = t.amount_out or 0.0
            out_key = (t.sender.lower(), (t.token_in or "").lower())
            in_key = (t.recipient.lower(), (t.token_out or "").lower())
            net[out_key] -= amount_in
            gross[out_key] += abs(amount_in)
            net[in_key] += amount_out
            gross[in_key] += abs(amount_out)

        margin = settings.POSITION_NEUTRAL_MARGIN
        return all(abs(net[key]) <= margin * volume for key, volume in gross.items() if volume > 0)

    async def detect_closed_cluster(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
    ) -> list[SwapTrade]:
        """Detect approximately closed trading clusters.

        Builds a volume-weighted undirected trade graph per pool, partitions it
        into communities (greedy modularity), and flags communities whose trade
        volume is overwhelmingly internal (>= CLOSED_CLUSTER_INTERNAL_RATIO):
        colluding wash traders rarely transact outside their own ring.
        """
        wash_trades: list[SwapTrade] = []

        pool_groups: dict[str, list[SwapTrade]] = defaultdict(list)
        for trade in trades:
            pool_groups[trade.pool_address].append(trade)

        for pool_trades in pool_groups.values():
            graph: nx.Graph[str] = nx.Graph()
            for trade in pool_trades:
                sender = trade.sender.lower()
                recipient = trade.recipient.lower()
                if sender == recipient:
                    continue
                volume = trade.volume_usd or 0.0
                if graph.has_edge(sender, recipient):
                    graph[sender][recipient]["weight"] += volume
                else:
                    graph.add_edge(sender, recipient, weight=volume)

            if graph.number_of_edges() == 0:
                continue

            communities = nx.community.greedy_modularity_communities(graph, weight="weight")

            for community in communities:
                members = {str(node) for node in community}
                if not (
                    settings.CLOSED_CLUSTER_MIN_MEMBERS
                    <= len(members)
                    <= settings.CLOSED_CLUSTER_MAX_MEMBERS
                ):
                    continue

                internal_trades: list[SwapTrade] = []
                internal_volume = 0.0
                boundary_volume = 0.0
                volume_out: dict[str, float] = defaultdict(float)
                volume_in: dict[str, float] = defaultdict(float)
                for trade in pool_trades:
                    sender = trade.sender.lower()
                    recipient = trade.recipient.lower()
                    if sender == recipient:
                        continue
                    sender_in = sender in members
                    recipient_in = recipient in members
                    if sender_in and recipient_in:
                        volume = trade.volume_usd or 0.0
                        internal_trades.append(trade)
                        internal_volume += volume
                        volume_out[sender] += volume
                        volume_in[recipient] += volume
                    elif sender_in or recipient_in:
                        boundary_volume += trade.volume_usd or 0.0

                if len(internal_trades) < settings.CLOSED_CLUSTER_MIN_TRADES:
                    continue

                total = internal_volume + boundary_volume
                if total <= 0:
                    continue
                if internal_volume / total < settings.CLOSED_CLUSTER_INTERNAL_RATIO:
                    continue

                # Wash rings recycle volume: each member's internal outflow and
                # inflow roughly match. Organic flow through a community is
                # directional and fails this balance test.
                if not self._members_balanced(members, volume_out, volume_in):
                    continue

                for trade in internal_trades:
                    flag_trade(trade, 0.85, "closed_cluster")
                    wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} closed-cluster trades")
        return wash_trades

    @staticmethod
    def _members_balanced(
        members: set[str],
        volume_out: dict[str, float],
        volume_in: dict[str, float],
    ) -> bool:
        """True if every active member's internal in/out volume roughly matches."""
        tolerance = settings.CLOSED_CLUSTER_BALANCE_TOLERANCE
        for member in members:
            out_vol = volume_out.get(member, 0.0)
            in_vol = volume_in.get(member, 0.0)
            gross = out_vol + in_vol
            if gross <= 0:
                continue
            if abs(out_vol - in_vol) / gross > tolerance:
                return False
        return True

    async def detect_repeated_amounts(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
    ) -> list[SwapTrade]:
        """Detect senders recycling the same trade size.

        Trade amounts are rounded to REPEATED_AMOUNT_SIG_FIGS significant
        digits; a sender repeating one amount >= REPEATED_AMOUNT_MIN_COUNT
        times is a strong volume-bot fingerprint (organic flow almost never
        repeats exact sizes at scale).
        """
        wash_trades: list[SwapTrade] = []
        allowlist = settings.bot_allowlist_set

        groups: dict[tuple[str, str, float], list[SwapTrade]] = defaultdict(list)
        for trade in trades:
            sender = trade.sender.lower()
            if sender in allowlist:
                continue
            amount = round_to_sig_figs(trade.amount_in, settings.REPEATED_AMOUNT_SIG_FIGS)
            if amount <= 0:
                continue
            groups[(trade.pool_address, sender, amount)].append(trade)

        for group_trades in groups.values():
            if len(group_trades) < settings.REPEATED_AMOUNT_MIN_COUNT:
                continue
            for trade in group_trades:
                flag_trade(trade, 0.75, "repeated_amounts")
                wash_trades.append(trade)

        logger.info(f"Detected {len(wash_trades)} repeated-amount trades")
        return wash_trades
