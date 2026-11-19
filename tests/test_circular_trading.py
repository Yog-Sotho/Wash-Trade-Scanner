"""
detect_circular_trading looks reverse legs up in per-pair sorted times
(roadmap Phase 5) instead of scanning every trade for each candidate. It must
flag exactly what the scan flagged, in the same order.
"""

import asyncio
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta

import networkx as nx
import pytest

from config.settings import settings
from core.advanced_heuristics import flag_trade
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

START = datetime(2026, 1, 15, 12, 0)


async def scan_reference(trades: list[SwapTrade]) -> list[SwapTrade]:
    """The O(n^2) implementation this replaced, verbatim but for logging."""
    wash_trades = []
    pool_groups = defaultdict(list)
    for trade in trades:
        pool_groups[trade.pool_address].append(trade)
    for _pool, pool_trades in pool_groups.items():
        G = nx.DiGraph()
        for trade in pool_trades:
            G.add_edge(trade.sender, trade.recipient)
        for scc in list(nx.strongly_connected_components(G)):
            if len(scc) < 2:
                continue
            scc_addresses = set(scc)
            for trade in pool_trades:
                if trade.sender not in scc_addresses or trade.recipient not in scc_addresses:
                    continue
                window = timedelta(minutes=settings.WASH_TRADE_TIME_WINDOW_MINUTES)
                reverse_trades = [
                    t
                    for t in pool_trades
                    if t.sender == trade.recipient
                    and t.recipient == trade.sender
                    and trade.block_timestamp - window
                    <= t.block_timestamp
                    <= trade.block_timestamp + window
                ]
                if reverse_trades:
                    flag_trade(trade, 0.9, "circular_trading")
                    wash_trades.append(trade)
    return wash_trades


def random_pool(
    seed: int, n: int, addresses: int, pools: int, span_minutes: int
) -> list[SwapTrade]:
    rng = random.Random(seed)
    names = [f"0x{i:040x}" for i in range(addresses)]
    trades = []
    for i in range(n):
        sender = rng.choice(names)
        # Some self-trades, which a multi-address component flags by themselves.
        recipient = sender if rng.random() < 0.05 else rng.choice(names)
        trades.append(
            SwapTrade(
                id=i,
                chain_id=1,
                pool_address=f"0xpool{rng.randrange(pools)}",
                sender=sender,
                recipient=recipient,
                # Whole minutes, so trades land exactly on the window's edges too.
                block_timestamp=START + timedelta(minutes=rng.randrange(span_minutes)),
                volume_usd=rng.choice([None, 10.0, 1000.0]),
            )
        )
    return trades


def labels(trades: list[SwapTrade]) -> list[tuple]:
    return [(t.id, t.is_wash_trade, t.wash_trade_score, t.detection_method) for t in trades]


@pytest.mark.parametrize("seed", range(40))
async def test_flags_exactly_what_the_scan_flagged_in_the_same_order(seed):
    rng = random.Random(seed)
    n = rng.choice([0, 1, 5, 30, 200, 600])
    shape = {
        "addresses": rng.choice([2, 3, 6, 20, 80]),
        "pools": rng.choice([1, 1, 3]),
        "span_minutes": rng.choice([10, 60, 600, 6000]),
    }
    expected_trades = random_pool(seed, n, **shape)
    actual_trades = random_pool(seed, n, **shape)

    expected = await scan_reference(expected_trades)
    actual = await HeuristicDetector().detect_circular_trading(actual_trades, None)

    assert [t.id for t in actual] == [t.id for t in expected]
    assert labels(actual_trades) == labels(expected_trades)


async def test_the_window_edges_are_inclusive():
    window = settings.WASH_TRADE_TIME_WINDOW_MINUTES
    a, b = "0x" + "a" * 40, "0x" + "b" * 40

    def trades(gap_minutes: float) -> list[SwapTrade]:
        return [
            SwapTrade(id=1, pool_address="p", sender=a, recipient=b, block_timestamp=START),
            SwapTrade(
                id=2,
                pool_address="p",
                sender=b,
                recipient=a,
                block_timestamp=START + timedelta(minutes=gap_minutes),
            ),
        ]

    detector = HeuristicDetector()
    assert len(await detector.detect_circular_trading(trades(window), None)) == 2
    assert await detector.detect_circular_trading(trades(window + 1 / 60), None) == []


def test_a_dense_component_is_no_longer_quadratic():
    """20,000 trades among three addresses: the scan made 20,000 passes over
    20,000 trades (4e8 comparisons, minutes); the index takes well under a
    second."""
    trades = random_pool(seed=1, n=20_000, addresses=3, pools=1, span_minutes=60 * 24)
    start = time.perf_counter()
    flagged = asyncio.run(HeuristicDetector().detect_circular_trading(trades, None))
    elapsed = time.perf_counter() - start

    assert len(flagged) > 19_000
    assert elapsed < 5.0, elapsed
