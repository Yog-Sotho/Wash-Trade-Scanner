"""
Research-grade wash trade detectors.

Implements detection methods from the academic and forensic literature that go
beyond simple pairwise heuristics. On an AMM every swap's counterparty is the
pool, so traders are identified by the account that sent the transaction
(``tx_from``) -- the swap event's ``sender``/``to`` are usually the router or
bot contract that called the pool:

- Round-trip (A-A) wash trading (Chainalysis, 2025 Crypto Crime Report,
  wash-trading heuristic 1): one address buys and sells the same pool within
  25 blocks at USD values within 1%, three or more times.

- Position neutrality (Victor & Weintraud, "Detecting and Quantifying Wash
  Trading on Decentralized Cryptocurrency Exchanges", WWW '21, adapted to AMMs
  as by Gan et al., "Exposing Stealthy Wash Trading on Automated Market Maker
  Exchanges", ACM TOIT 2024): an entity -- one address, or the addresses a
  funding cluster links -- trades both sides within a window while its net
  position in every token stays approximately zero.

- Closed-cluster analysis (network-based detection, 2025): wash traders form
  approximately closed clusters of colluding counterparties that seldom
  transact with anyone outside the cluster. Communities whose trade volume is
  overwhelmingly internal are flagged.

- Repeated-amount fingerprinting: wash bots recycle identical (or
  near-identical) trade sizes; genuine order flow almost never repeats the
  same amount many times from one trader.

Every detector's precision, recall and false-positive rate on real pools is
measured by the evaluation harness (``evaluation/``, ``docs/benchmarks.md``).
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import timedelta

import networkx as nx
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from core.evidence import EvidenceLog, Finding
from models.schemas import AddressCluster, SwapTrade

logger = logging.getLogger(__name__)


def round_to_sig_figs(value: float, sig_figs: int) -> float:
    """Round a positive number to `sig_figs` significant digits."""
    if not value or not math.isfinite(value):
        return 0.0
    magnitude = math.floor(math.log10(abs(value)))
    factor = 10.0 ** (magnitude - sig_figs + 1)
    return round(value / factor) * factor


def trader_of(trade: SwapTrade) -> str:
    """The account behind a swap: the transaction sender.

    Rows stored before transaction senders were recorded (or whose transaction
    could not be fetched) fall back to the swap event's sender.
    """
    return (trade.tx_from or trade.sender).lower()


def _direction(trade: SwapTrade) -> tuple[str, str]:
    return (trade.token_in or "").lower(), (trade.token_out or "").lower()


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


def market_of(trade: SwapTrade, cross_pool: bool) -> str:
    """The market a trade is judged in: its pool or, across pools, its chain
    and token pair, so that a trader buying in one USDC/WETH pool and selling
    in another is seen trading both sides of one market."""
    if not cross_pool:
        return trade.pool_address
    a, b = sorted(((trade.token_in or "").lower(), (trade.token_out or "").lower()))
    return f"{trade.chain_id}:{a}:{b}"


class AdvancedHeuristicDetector:
    """Graph- and statistics-based wash trade detection.

    With `cross_pool`, round trips, position neutrality and coordination are
    judged per token pair across the given pools instead of per pool (roadmap
    Phase 6, cross-pool correlation)."""

    def __init__(self, cross_pool: bool = False) -> None:
        self.cross_pool = cross_pool

    async def detect_round_trips(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        evidence: EvidenceLog | None = None,
    ) -> list[SwapTrade]:
        """Detect address-level (A-A) round trips: Chainalysis heuristic 1.

        A trader's swap and a later swap in the opposite direction in the same
        pool form a round trip when they are at most ROUND_TRIP_MAX_BLOCKS
        apart and their USD values differ by at most ROUND_TRIP_VALUE_TOLERANCE
        (unpriced swaps compare the amount sold back with the amount bought).
        A trader with ROUND_TRIP_MIN_COUNT or more round trips is flagged.

        Two legs in the same block with another trader's swap between them are
        a sandwich -- an MEV attack on that trader, not wash trading -- and are
        not paired. Legs in a single transaction are paired: buying and
        selling atomically is how volume bots manufacture volume cheaply.
        """
        wash_trades: list[SwapTrade] = []
        max_blocks = settings.ROUND_TRIP_MAX_BLOCKS
        tolerance = settings.ROUND_TRIP_VALUE_TOLERANCE

        pool_groups: dict[str, list[SwapTrade]] = defaultdict(list)
        for trade in trades:
            pool_groups[market_of(trade, self.cross_pool)].append(trade)

        for pool_trades in pool_groups.values():
            ordered = sorted(pool_trades, key=lambda t: (t.block_number, t.log_index))
            position = {id(t): i for i, t in enumerate(ordered)}
            by_trader: dict[str, list[SwapTrade]] = defaultdict(list)
            for trade in ordered:
                by_trader[trader_of(trade)].append(trade)

            for trader, own in by_trader.items():
                if len(own) < 2 * settings.ROUND_TRIP_MIN_COUNT:
                    continue
                used: set[int] = set()
                pairs: list[tuple[SwapTrade, SwapTrade]] = []
                for i, first in enumerate(own):
                    if id(first) in used:
                        continue
                    bought = _direction(first)
                    for second in own[i + 1 :]:
                        if second.block_number - first.block_number > max_blocks:
                            break
                        if id(second) in used or _direction(second) != bought[::-1]:
                            continue
                        if not self._values_match(first, second, tolerance):
                            continue
                        if self._is_sandwich(first, second, ordered, position, trader):
                            continue
                        used.update((id(first), id(second)))
                        pairs.append((first, second))
                        break
                if len(pairs) < settings.ROUND_TRIP_MIN_COUNT:
                    continue
                for first, second in pairs:
                    for trade in (first, second):
                        flag_trade(trade, 1.0, "round_trip")
                        wash_trades.append(trade)
                    if evidence is not None:
                        evidence.add(self._round_trip_finding(trader, first, second, len(pairs)))

        logger.info(f"Detected {len(wash_trades)} round-trip trades")
        return wash_trades

    @staticmethod
    def _value_gap(first: SwapTrade, second: SwapTrade) -> float | None:
        """Relative difference of the two legs' USD values or, unpriced, of
        the amount sold back and the amount bought; None if not comparable."""
        a, b = first.volume_usd, second.volume_usd
        if a and b:
            return abs(a - b) / max(a, b)
        bought, sold = first.amount_out or 0.0, second.amount_in or 0.0
        if bought <= 0 or sold <= 0:
            return None
        return abs(bought - sold) / max(bought, sold)

    @classmethod
    def _values_match(cls, first: SwapTrade, second: SwapTrade, tolerance: float) -> bool:
        gap = cls._value_gap(first, second)
        return gap is not None and gap <= tolerance

    @classmethod
    def _round_trip_finding(
        cls, trader: str, first: SwapTrade, second: SwapTrade, round_trips: int
    ) -> Finding:
        blocks = second.block_number - first.block_number
        gap = cls._value_gap(first, second) or 0.0
        priced = bool(first.volume_usd and second.volume_usd)
        where = (
            "in the same transaction"
            if first.transaction_hash == second.transaction_hash
            else f"{blocks} block{'s' if blocks != 1 else ''} later"
        )
        return Finding(
            detector="round_trip",
            trades=[first, second],
            entity=trader,
            summary=(
                f"Bought and sold back {where}, the two legs' "
                f"{'USD values' if priced else 'amounts'} {gap:.2%} apart; one of "
                f"{round_trips} such round trips by this trader in this pool"
            ),
            details={
                "blocks_apart": blocks,
                "value_gap": round(gap, 6),
                "compared": "usd" if priced else "amounts",
                "same_transaction": first.transaction_hash == second.transaction_hash,
                "round_trips_by_trader": round_trips,
                "max_blocks": settings.ROUND_TRIP_MAX_BLOCKS,
                "tolerance": settings.ROUND_TRIP_VALUE_TOLERANCE,
            },
        )

    @staticmethod
    def _is_sandwich(
        first: SwapTrade,
        second: SwapTrade,
        ordered: list[SwapTrade],
        position: dict[int, int],
        trader: str,
    ) -> bool:
        if first.block_number != second.block_number:
            return False
        between = ordered[position[id(first)] + 1 : position[id(second)]]
        return any(trader_of(t) != trader for t in between)

    async def detect_position_neutral(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        clusters: list[AddressCluster] | None = None,
        evidence: EvidenceLog | None = None,
    ) -> list[SwapTrade]:
        """Detect entities whose net position stays ~zero while they trade.

        An entity is one trader, or all traders in one funding cluster (the
        addresses entity clustering links by ETH transfers). For each window
        size in POSITION_NEUTRAL_WINDOWS_HOURS (multi-pass, as in Victor &
        Weintraud), an entity's trades in a window are wash if there are at
        least POSITION_NEUTRAL_MIN_TRADES of them, on both sides, and its net
        change in every token is within POSITION_NEUTRAL_MARGIN of the gross
        amount it moved: the entity bought and sold without changing what it
        holds.
        """
        wash_trades: list[SwapTrade] = []
        flagged: set[int] = set()

        entity_of: dict[str, str] = {}
        members_of: dict[str, list[str]] = {}
        for cluster in clusters or []:
            members_of[f"cluster:{cluster.cluster_id}"] = sorted(
                a.lower() for a in cluster.addresses
            )
            for address in cluster.addresses:
                entity_of[address.lower()] = f"cluster:{cluster.cluster_id}"

        pool_groups: dict[str, list[SwapTrade]] = defaultdict(list)
        for trade in trades:
            pool_groups[market_of(trade, self.cross_pool)].append(trade)

        for pool_trades in pool_groups.values():
            by_entity: dict[str, list[SwapTrade]] = defaultdict(list)
            for trade in pool_trades:
                trader = trader_of(trade)
                by_entity[entity_of.get(trader, trader)].append(trade)

            for entity, entity_trades in by_entity.items():
                if len(entity_trades) < settings.POSITION_NEUTRAL_MIN_TRADES:
                    continue
                for window_hours in settings.POSITION_NEUTRAL_WINDOWS_HOURS:
                    window_seconds = timedelta(hours=window_hours).total_seconds()
                    buckets: dict[int, list[SwapTrade]] = defaultdict(list)
                    for t in entity_trades:
                        bucket = int(t.block_timestamp.timestamp() // window_seconds)
                        buckets[bucket].append(t)

                    for bucket_trades in buckets.values():
                        if len(bucket_trades) < settings.POSITION_NEUTRAL_MIN_TRADES:
                            continue
                        if len({_direction(t) for t in bucket_trades}) < 2:
                            continue  # one-sided flow is accumulation, not wash
                        if not self._is_position_neutral(bucket_trades):
                            continue
                        if evidence is not None:
                            evidence.add(
                                self._neutral_finding(
                                    entity, bucket_trades, window_hours, members_of.get(entity)
                                )
                            )
                        for t in bucket_trades:
                            if t.id in flagged:
                                continue
                            flagged.add(t.id)
                            flag_trade(t, 0.95, "position_neutral")
                            wash_trades.append(t)

        logger.info(f"Detected {len(wash_trades)} position-neutral trades")
        return wash_trades

    @staticmethod
    def _net_to_gross(trades: list[SwapTrade]) -> dict[str, float]:
        """Per token: the entity's net change as a share of the gross amount
        of it the entity moved (0: it ends where it started)."""
        net: dict[str, float] = defaultdict(float)
        gross: dict[str, float] = defaultdict(float)

        for t in trades:
            token_in, token_out = _direction(t)
            amount_in = t.amount_in or 0.0
            amount_out = t.amount_out or 0.0
            net[token_in] -= amount_in
            gross[token_in] += abs(amount_in)
            net[token_out] += amount_out
            gross[token_out] += abs(amount_out)
        return {token: abs(net[token]) / volume for token, volume in gross.items() if volume > 0}

    @classmethod
    def _is_position_neutral(cls, trades: list[SwapTrade]) -> bool:
        """True if the entity's net change in every token is within margin of
        the gross amount of that token it moved."""
        margin = settings.POSITION_NEUTRAL_MARGIN
        return all(ratio <= margin for ratio in cls._net_to_gross(trades).values())

    @classmethod
    def _neutral_finding(
        cls,
        entity: str,
        trades: list[SwapTrade],
        window_hours: float,
        members: list[str] | None,
    ) -> Finding:
        ordered = sorted(trades, key=lambda t: (t.block_number, t.log_index))
        ratios = cls._net_to_gross(ordered)
        who = f"Funding cluster of {len(members)} addresses" if members else "One trader"
        details: dict[str, object] = {
            "window_hours": window_hours,
            "first_block": ordered[0].block_number,
            "last_block": ordered[-1].block_number,
            "trades_in_window": len(ordered),
            "net_to_gross": {token: round(r, 6) for token, r in ratios.items()},
            "margin": settings.POSITION_NEUTRAL_MARGIN,
        }
        if members:
            details["members"] = members[:20]
            details["members_total"] = len(members)
        return Finding(
            detector="position_neutral",
            trades=ordered,
            entity=entity,
            summary=(
                f"{who} made {len(ordered)} trades on both sides within a {window_hours:g} h "
                f"window (blocks {ordered[0].block_number}-{ordered[-1].block_number}) and "
                f"ended with a net change of at most {max(ratios.values()):.2%} of what it "
                f"moved in each token (margin {settings.POSITION_NEUTRAL_MARGIN:.0%})"
            ),
            details=details,
        )

    async def detect_coordinated_trading(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        clusters: list[AddressCluster] | None = None,
        evidence: EvidenceLog | None = None,
    ) -> list[SwapTrade]:
        """Detect wallets that wash-trade with each other: temporal motifs.

        On an AMM no swap names a counterparty, so a ring of wallets trading
        "with each other" shows only in time: one wallet sells what another
        buys a moment later, for the same value (Chainalysis's wash-trading
        heuristic 2; Gan et al., TOIT 2024: colluding wallets trade almost
        simultaneously and keep their holdings constant). Such a matched pair
        is a two-node delta-temporal motif (Paranjape et al., WSDM '17): two
        swaps by different traders, in opposite directions, at most
        COORDINATION_MAX_BLOCKS apart, values within
        COORDINATION_VALUE_TOLERANCE.

        One pair proves nothing on a busy pool; it happens by chance. A group
        is flagged when its wallets are linked by such pairs (or by funding,
        when clusters are given) into COORDINATION_MIN_PAIRS pairs or more,
        when nearly everything each of them trades is in a pair
        (COORDINATION_MIN_COVERAGE: busy traders, matched by chance, are not),
        and when the group as a whole ends flat (COORDINATION_NET_MARGIN).
        The snapshot SCC detector (detect_circular_trading) looks for cycles
        among swap senders and recipients, which on an AMM are routers; this
        is its temporal counterpart on traders.
        """
        wash_trades: list[SwapTrade] = []
        max_blocks = settings.COORDINATION_MAX_BLOCKS
        tolerance = settings.COORDINATION_VALUE_TOLERANCE

        cluster_of: dict[str, str] = {}
        for cluster in clusters or []:
            for address in cluster.addresses:
                cluster_of[address.lower()] = cluster.cluster_id

        pool_groups: dict[str, list[SwapTrade]] = defaultdict(list)
        for trade in trades:
            pool_groups[market_of(trade, self.cross_pool)].append(trade)

        for pool_trades in pool_groups.values():
            # Matching is in block time: a trade without a block cannot be placed.
            ordered = sorted(
                (t for t in pool_trades if t.block_number is not None),
                key=lambda t: (t.block_number, t.log_index or 0),
            )
            by_trader: dict[str, list[SwapTrade]] = defaultdict(list)
            for trade in ordered:
                by_trader[trader_of(trade)].append(trade)

            # Matched pairs: each swap against the later opposite swaps by other
            # traders within the block window.
            pairs: list[tuple[SwapTrade, SwapTrade]] = []
            for i, first in enumerate(ordered):
                direction = _direction(first)
                trader = trader_of(first)
                for second in ordered[i + 1 :]:
                    if second.block_number - first.block_number > max_blocks:
                        break
                    if _direction(second) != direction[::-1] or trader_of(second) == trader:
                        continue
                    gap = self._pair_gap(first, second)
                    if gap is not None and gap <= tolerance:
                        pairs.append((first, second))
            if not pairs:
                continue

            for group, group_pairs in self._coordinated_groups(pairs, by_trader, cluster_of):
                if len(group_pairs) < settings.COORDINATION_MIN_PAIRS:
                    continue
                group_trades = [t for member in group for t in by_trader[member]]
                ratios = self._net_to_gross(group_trades)
                if not ratios or max(ratios.values()) > settings.COORDINATION_NET_MARGIN:
                    continue
                legs = {id(t): t for pair in group_pairs for t in pair}
                flagged = sorted(legs.values(), key=lambda t: (t.block_number, t.log_index))
                for trade in flagged:
                    flag_trade(trade, 0.9, "coordinated_trading")
                    wash_trades.append(trade)
                if evidence is not None:
                    evidence.add(
                        self._coordination_finding(group, group_pairs, flagged, by_trader, ratios)
                    )

        logger.info(f"Detected {len(wash_trades)} coordinated trades")
        return wash_trades

    @staticmethod
    def _coordinated_groups(
        pairs: list[tuple[SwapTrade, SwapTrade]],
        by_trader: dict[str, list[SwapTrade]],
        cluster_of: dict[str, str],
    ) -> list[tuple[set[str], list[tuple[SwapTrade, SwapTrade]]]]:
        """Groups of traders linked by matched pairs (and funding), pruned
        until at least COORDINATION_MIN_COVERAGE of each member's trades are
        in pairs with the group's other members. Pruning one member can drop
        another below the bar, so it repeats until nothing changes: a busy
        trader matched by chance with a few others is cut away, with the
        pairs it alone held together."""
        members = {trader_of(t) for pair in pairs for t in pair}
        while True:
            graph: nx.Graph[str] = nx.Graph()
            graph.add_nodes_from(members)
            for first, second in pairs:
                a, b = trader_of(first), trader_of(second)
                if a in members and b in members:
                    graph.add_edge(a, b)
            by_cluster: dict[str, list[str]] = defaultdict(list)
            for trader in members:
                if trader in cluster_of:
                    by_cluster[cluster_of[trader]].append(trader)
            for linked in by_cluster.values():
                nx.add_path(graph, linked)  # a funding cluster is one entity
            component_of = {
                str(node): index
                for index, component in enumerate(nx.connected_components(graph))
                for node in component
            }
            in_group: dict[str, set[int]] = defaultdict(set)
            group_pairs: dict[int, list[tuple[SwapTrade, SwapTrade]]] = defaultdict(list)
            for first, second in pairs:
                a, b = trader_of(first), trader_of(second)
                if a in members and b in members and component_of[a] == component_of[b]:
                    in_group[a].add(id(first))
                    in_group[b].add(id(second))
                    group_pairs[component_of[a]].append((first, second))
            keep = {
                trader
                for trader in members
                if len(in_group[trader]) / len(by_trader[trader])
                >= settings.COORDINATION_MIN_COVERAGE
            }
            if keep == members:
                break
            members = keep
        groups: dict[int, set[str]] = defaultdict(set)
        for trader in members:
            groups[component_of[trader]].add(trader)
        return [(group, group_pairs[index]) for index, group in groups.items()]

    @staticmethod
    def _pair_gap(first: SwapTrade, second: SwapTrade) -> float | None:
        """Relative difference of two opposite swaps' values: USD when both
        are priced, else what the first sold against what the second bought."""
        a, b = first.volume_usd, second.volume_usd
        if a and b:
            return abs(a - b) / max(a, b)
        sold, bought = first.amount_in or 0.0, second.amount_out or 0.0
        if sold <= 0 or bought <= 0:
            return None
        return abs(sold - bought) / max(sold, bought)

    @classmethod
    def _coordination_finding(
        cls,
        group: set[str],
        group_pairs: list[tuple[SwapTrade, SwapTrade]],
        flagged: list[SwapTrade],
        by_trader: dict[str, list[SwapTrade]],
        ratios: dict[str, float],
    ) -> Finding:
        members = sorted(group)
        legs = {id(t) for pair in group_pairs for t in pair}
        coverage = {
            m: round(sum(1 for t in by_trader[m] if id(t) in legs) / len(by_trader[m]), 4)
            for m in members
        }
        listed = sorted(group_pairs, key=lambda p: (p[0].block_number, p[0].log_index))[:10]
        return Finding(
            detector="coordinated_trading",
            trades=flagged,
            entity=",".join(members[:20]),
            summary=(
                f"{len(members)} wallets made {len(group_pairs)} pairs of opposite trades, each "
                f"pair at most {settings.COORDINATION_MAX_BLOCKS} blocks and "
                f"{settings.COORDINATION_VALUE_TOLERANCE:.0%} in value apart; at least "
                f"{min(coverage.values()):.0%} of each wallet's trades are in such pairs, and "
                f"the group ended with a net change of at most {max(ratios.values()):.1%} of "
                "what it moved"
            ),
            details={
                "members": members[:20],
                "members_total": len(members),
                "pairs": len(group_pairs),
                "coverage": {m: coverage[m] for m in members[:20]},
                "net_to_gross": {token: round(r, 6) for token, r in ratios.items()},
                "pair_examples": [
                    {
                        "first": first.transaction_hash,
                        "second": second.transaction_hash,
                        "blocks_apart": second.block_number - first.block_number,
                        "value_gap": round(cls._pair_gap(first, second) or 0.0, 6),
                    }
                    for first, second in listed
                ],
            },
        )

    async def detect_closed_cluster(
        self,
        trades: list[SwapTrade],
        session: AsyncSession,
        evidence: EvidenceLog | None = None,
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
                if evidence is not None:
                    ratio = internal_volume / total
                    evidence.add(
                        Finding(
                            detector="closed_cluster",
                            trades=internal_trades,
                            entity=f"community of {len(members)} addresses",
                            summary=(
                                f"{len(internal_trades)} swaps among a community of "
                                f"{len(members)} swap senders and recipients, {ratio:.1%} of "
                                "whose volume stays inside it, each member's in- and outflow "
                                "balanced"
                            ),
                            details={
                                "members": sorted(members)[:20],
                                "members_total": len(members),
                                "internal_volume_ratio": round(ratio, 6),
                                "internal_trades": len(internal_trades),
                            },
                        )
                    )

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
        evidence: EvidenceLog | None = None,
    ) -> list[SwapTrade]:
        """Detect traders recycling the same trade size.

        Trade amounts are rounded to REPEATED_AMOUNT_SIG_FIGS significant
        digits; a trader repeating one amount >= REPEATED_AMOUNT_MIN_COUNT
        times is a strong volume-bot fingerprint (organic flow almost never
        repeats exact sizes at scale).
        """
        wash_trades: list[SwapTrade] = []
        allowlist = settings.bot_allowlist_set

        groups: dict[tuple[str, str, str, float], list[SwapTrade]] = defaultdict(list)
        for trade in trades:
            trader = trader_of(trade)
            if trader in allowlist:
                continue
            amount = round_to_sig_figs(trade.amount_in, settings.REPEATED_AMOUNT_SIG_FIGS)
            if amount <= 0:
                continue
            # Keyed by token too: the same number of different tokens is not
            # the same size.
            groups[(trade.pool_address, trader, (trade.token_in or "").lower(), amount)].append(
                trade
            )

        for (_pool, trader, token, amount), group_trades in groups.items():
            if len(group_trades) < settings.REPEATED_AMOUNT_MIN_COUNT:
                continue
            for trade in group_trades:
                flag_trade(trade, 0.75, "repeated_amounts")
                wash_trades.append(trade)
            if evidence is not None:
                sig = settings.REPEATED_AMOUNT_SIG_FIGS
                evidence.add(
                    Finding(
                        detector="repeated_amounts",
                        trades=group_trades,
                        entity=trader,
                        summary=(
                            f"Sold {amount:.{sig}g} raw units of {token} {len(group_trades)} "
                            f"times (equal to {sig} significant digits)"
                        ),
                        details={
                            "token": token,
                            "amount": amount,
                            "times": len(group_trades),
                            "significant_digits": sig,
                            "min_times": settings.REPEATED_AMOUNT_MIN_COUNT,
                        },
                    )
                )

        logger.info(f"Detected {len(wash_trades)} repeated-amount trades")
        return wash_trades
