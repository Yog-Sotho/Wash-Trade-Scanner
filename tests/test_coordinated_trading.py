"""
Coordinated trading (roadmap Phase 6, temporal graph modelling): wallets
wash-trading with each other show up as repeated, matched, opposite trades
a few blocks apart.
"""

from datetime import datetime, timedelta

import pytest

from config.settings import settings
from core.advanced_heuristics import AdvancedHeuristicDetector
from core.evidence import EvidenceLog
from models.schemas import AddressCluster, SwapTrade

WETH, USDC = "0x" + "e" * 40, "0x" + "c" * 40
START = datetime(2026, 1, 15, 12, 0)
WALLETS = ["0x" + c * 40 for c in "1234"]
BUSY = "0x" + "b" * 40

_ids = iter(range(1, 10**6))


def swap(trader: str, block: int, sold: str, usd: float) -> SwapTrade:
    bought = WETH if sold == USDC else USDC
    i = next(_ids)
    return SwapTrade(
        id=i,
        chain_id=1,
        pool_address="0xpool",
        token_in=sold,
        token_out=bought,
        amount_in=usd,
        amount_out=usd,
        sender="0x" + "7" * 40,
        recipient=trader,
        tx_from=trader,
        transaction_hash="0x" + f"{i:064x}",
        log_index=i,
        block_number=block,
        block_timestamp=START + timedelta(seconds=12 * block),
        volume_usd=usd,
        is_wash_trade=False,
        wash_trade_score=0.0,
    )


def ring(events: int = 4, wallets: list[str] = WALLETS, gap: int = 2) -> list[SwapTrade]:
    """`events` matched pairs, each between two of the wallets, `gap` blocks
    apart and 2% apart in value, alternating which side each wallet takes so
    that the group ends flat."""
    trades = []
    for n in range(events):
        a, b = wallets[n % len(wallets)], wallets[(n + 1) % len(wallets)]
        block = 100 + 200 * n
        sold = USDC if n % 2 == 0 else WETH
        trades.append(swap(a, block, sold, 1000.0))
        trades.append(swap(b, block + gap, WETH if sold == USDC else USDC, 1020.0))
    return trades


async def detect(trades, clusters=None, evidence=None):
    return await AdvancedHeuristicDetector().detect_coordinated_trading(
        trades, None, clusters, evidence=evidence
    )


async def test_a_ring_of_matched_opposite_trades_is_flagged_with_its_evidence():
    trades = ring()
    log = EvidenceLog()
    flagged = await detect(trades, evidence=log)

    assert {t.id for t in flagged} == {t.id for t in trades}
    assert all(t.detection_method == "coordinated_trading" for t in flagged)
    (finding,) = log.findings(trades[0])
    assert finding.details["members"] == sorted(WALLETS)
    assert finding.details["pairs"] == 4
    assert set(finding.details["coverage"].values()) == {1.0}
    assert finding.details["pair_examples"][0]["blocks_apart"] == 2
    assert finding.details["pair_examples"][0]["value_gap"] == pytest.approx(20 / 1020, abs=1e-6)
    assert finding.summary.startswith("4 wallets made 4 pairs of opposite trades")


async def test_one_matched_pair_is_chance_not_coordination():
    assert await detect(ring(events=settings.COORDINATION_MIN_PAIRS - 1)) == []


@pytest.mark.parametrize(
    ("change", "flagged"),
    [
        ({"gap": settings.COORDINATION_MAX_BLOCKS}, True),
        ({"gap": settings.COORDINATION_MAX_BLOCKS + 1}, False),  # too far apart in time
    ],
)
async def test_pairs_must_be_close_in_time(change, flagged):
    assert bool(await detect(ring(**change))) is flagged


async def test_pairs_must_match_in_value():
    trades = ring()
    for trade in trades[1::2]:
        trade.volume_usd = 1000.0 * (1 + settings.COORDINATION_VALUE_TOLERANCE) + 10
    assert await detect(trades) == []


async def test_one_traders_own_opposite_trades_are_a_round_trip_not_a_pair():
    trades = ring(wallets=[WALLETS[0]])
    assert await detect(trades) == []


async def test_a_busy_trader_matched_by_chance_is_pruned_and_the_ring_still_flagged():
    trades = ring()
    # A busy trader happens to trade opposite the ring's first leg, one block
    # later and at the same value; its other ten trades match nothing.
    trades.append(swap(BUSY, 101, WETH, 1000.0))
    trades += [swap(BUSY, 2000 + 50 * k, USDC, 37.0 * (k + 1)) for k in range(10)]

    flagged = await detect(trades)

    assert BUSY not in {t.tx_from for t in flagged}
    assert {t.tx_from for t in flagged} == set(WALLETS)


async def test_a_member_matched_mostly_with_outsiders_is_pruned():
    """The structure of the one clean-pool false positive before pruning: a
    wallet all of whose trades matched someone, but mostly a busy outsider,
    with one match into the group."""
    x = "0x" + "9" * 40
    trades = ring()
    trades.append(swap(x, 101, WETH, 1000.0))  # opposite the ring's first leg
    for k in range(3):  # three matches with the busy trader
        block = 3000 + 100 * k
        trades.append(swap(x, block, USDC, 500.0))
        trades.append(swap(BUSY, block + 1, WETH, 500.0))
    trades += [swap(BUSY, 5000 + 50 * k, USDC, 37.0 * (k + 1)) for k in range(10)]

    flagged = await detect(trades)

    assert {t.tx_from for t in flagged} == set(WALLETS)  # x and BUSY pruned


async def test_a_group_that_does_not_end_flat_is_not_flagged():
    trades = ring()
    for trade in trades:
        trade.amount_out = trade.amount_in * 2  # everyone accumulates
    assert await detect(trades) == []


async def test_funding_links_join_wallets_into_one_ring():
    # Two separate pairs of pairs: neither half alone has enough pairs.
    half = settings.COORDINATION_MIN_PAIRS - 1
    trades = ring(events=half, wallets=WALLETS[:2]) + [
        swap(
            t.tx_from.replace("1", "3").replace("2", "4"),
            t.block_number + 5000,
            t.token_in,
            t.volume_usd,
        )
        for t in ring(events=half, wallets=WALLETS[:2])
    ]
    assert await detect(trades) == []

    cluster = AddressCluster(cluster_id="c", addresses=WALLETS, confidence_score=0.8)
    flagged = await detect(trades, clusters=[cluster])
    assert {t.tx_from for t in flagged} == set(WALLETS)
