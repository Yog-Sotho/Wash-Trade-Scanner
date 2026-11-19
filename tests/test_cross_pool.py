"""
Cross-pool correlation (roadmap Phase 6): wash trading split over pools of
one token pair, invisible pool by pool, is seen when the pools are audited
together, while arbitrage between them is not mistaken for it.
"""

from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from core.advanced_heuristics import AdvancedHeuristicDetector, market_of
from core.evidence import EvidenceLog
from core.validators import AuditParameters
from models.schemas import SwapTrade

WETH, USDC = "0x" + "e" * 40, "0x" + "c" * 40
POOL_A, POOL_B = "0x" + "a" * 40, "0x" + "b" * 40
TRADER = "0x" + "1" * 40

_ids = iter(range(1, 10**6))


def swap(
    pool: str, block: int, sold: str, amount_in: float, amount_out: float, tx: str = ""
) -> SwapTrade:
    i = next(_ids)
    usd = amount_in if sold == USDC else amount_out
    return SwapTrade(
        id=i,
        chain_id=1,
        pool_address=pool,
        token_in=sold,
        token_out=WETH if sold == USDC else USDC,
        amount_in=amount_in,
        amount_out=amount_out,
        sender="0x" + "7" * 40,
        recipient=TRADER,
        tx_from=TRADER,
        transaction_hash=tx or "0x" + f"{i:064x}",
        log_index=i,
        block_number=block,
        block_timestamp=datetime(2026, 1, 15) + timedelta(seconds=12 * block),
        volume_usd=usd,
        is_wash_trade=False,
        wash_trade_score=0.0,
    )


def split_round_trips(rounds: int = 3) -> list[SwapTrade]:
    """Bought in one pool, sold back in the other 3 blocks later, at a loss."""
    trades = []
    for n in range(rounds):
        block = 100 + 100 * n
        first, second = (POOL_A, POOL_B) if n % 2 == 0 else (POOL_B, POOL_A)
        trades.append(swap(first, block, USDC, 3000.0, 1.0))
        trades.append(swap(second, block + 3, WETH, 1.0, 2994.0))
    return trades


def test_pools_of_one_token_pair_are_one_market_across_pools():
    a, b = swap(POOL_A, 1, USDC, 1.0, 1.0), swap(POOL_B, 1, WETH, 1.0, 1.0)
    assert market_of(a, cross_pool=False) != market_of(b, cross_pool=False)
    assert market_of(a, cross_pool=True) == market_of(b, cross_pool=True)


async def test_round_trips_split_over_two_pools_are_seen_only_across_them():
    trades = split_round_trips()
    assert await AdvancedHeuristicDetector().detect_round_trips(trades, None) == []

    log = EvidenceLog()
    flagged = await AdvancedHeuristicDetector(cross_pool=True).detect_round_trips(
        trades, None, evidence=log
    )
    assert {t.id for t in flagged} == {t.id for t in trades}
    (finding,) = log.findings(trades[0])
    assert {leg.pool_address for leg in finding.trades} == {POOL_A, POOL_B}


async def test_atomic_arbitrage_between_the_pools_is_not_a_round_trip():
    """One transaction buying in one pool and selling dearer in the other."""
    trades = []
    for n in range(3):
        tx = "0x" + f"{n + 1:064x}"
        trades.append(swap(POOL_A, 100 * (n + 1), USDC, 3000.0, 1.0, tx))
        trades.append(swap(POOL_B, 100 * (n + 1), WETH, 1.0, 3006.0, tx))
    assert await AdvancedHeuristicDetector(cross_pool=True).detect_round_trips(trades, None) == []


async def test_an_atomic_cross_pool_round_trip_at_a_loss_is_still_one():
    """Volume manufactured in two pools in one transaction pays both fees."""
    trades = []
    for n in range(3):
        tx = "0x" + f"{n + 1:064x}"
        trades.append(swap(POOL_A, 100 * (n + 1), USDC, 3000.0, 1.0, tx))
        trades.append(swap(POOL_B, 100 * (n + 1), WETH, 1.0, 2991.0, tx))
    flagged = await AdvancedHeuristicDetector(cross_pool=True).detect_round_trips(trades, None)
    assert len(flagged) == 6


async def test_a_ring_with_its_legs_in_different_pools_is_seen_across_them():
    wallets = ["0x" + c * 40 for c in "2345"]
    trades = []
    for n in range(4):
        a, b = wallets[n], wallets[(n + 1) % 4]
        sold, other = (USDC, WETH) if n % 2 == 0 else (WETH, USDC)
        first = swap(POOL_A, 100 + 200 * n, sold, 1000.0, 1000.0)
        second = swap(POOL_B, 102 + 200 * n, other, 1010.0, 1010.0)
        first.tx_from, second.tx_from = a, b
        trades += [first, second]

    assert await AdvancedHeuristicDetector().detect_coordinated_trading(trades, None) == []
    flagged = await AdvancedHeuristicDetector(cross_pool=True).detect_coordinated_trading(
        trades, None
    )
    assert {t.tx_from for t in flagged} == set(wallets)


def test_related_pools_are_validated_lowercased_and_deduplicated():
    params = AuditParameters(
        chain_id=1,
        pool_address=POOL_A.upper().replace("0X", "0x"),
        related_pools=[POOL_B.upper().replace("0X", "0x"), POOL_B, POOL_A],
    )
    assert params.related_pools == [POOL_B]  # the audited pool itself dropped
    with pytest.raises(ValidationError):
        AuditParameters(chain_id=1, pool_address=POOL_A, related_pools=["0xnot-an-address"])
    with pytest.raises(ValidationError):
        AuditParameters(
            chain_id=1, pool_address=POOL_A, related_pools=[f"0x{i:040x}" for i in range(1, 11)]
        )
