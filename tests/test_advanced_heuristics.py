"""
Unit tests for the research-grade detectors in core/advanced_heuristics.py.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from core.advanced_heuristics import (
    AdvancedHeuristicDetector,
    flag_trade,
    round_to_sig_figs,
)
from models.schemas import SwapTrade

ALICE = "0x" + "a" * 40
BOB = "0x" + "b" * 40
CAROL = "0x" + "c" * 40
DAVE = "0x" + "d" * 40
TOKEN_X = "0x" + "1" * 40
TOKEN_Y = "0x" + "2" * 40
POOL = "0x" + "9" * 40

BASE_TIME = datetime(2024, 6, 1, 12, 0, 0)

_next_id = iter(range(1, 10_000))


def make_trade(
    sender: str,
    recipient: str,
    amount_in: float,
    amount_out: float,
    minutes: int = 0,
    token_in: str = TOKEN_X,
    token_out: str = TOKEN_Y,
    volume_usd: float = 1000.0,
    pool: str = POOL,
) -> SwapTrade:
    return SwapTrade(
        id=next(_next_id),
        chain_id=1,
        dex_name="uniswap_v2",
        pool_address=pool,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        amount_out=amount_out,
        sender=sender,
        recipient=recipient,
        transaction_hash="0x" + "f" * 64,
        block_number=1,
        block_timestamp=BASE_TIME + timedelta(minutes=minutes),
        log_index=0,
        volume_usd=volume_usd,
    )


@pytest.fixture
def detector():
    return AdvancedHeuristicDetector()


def test_round_to_sig_figs():
    assert round_to_sig_figs(123_456, 3) == 123_000
    assert round_to_sig_figs(0.0012345, 3) == pytest.approx(0.00123)
    assert round_to_sig_figs(0, 3) == 0.0
    assert round_to_sig_figs(1000.0, 3) == 1000.0


def test_flag_trade_keeps_higher_confidence_label():
    trade = make_trade(ALICE, BOB, 1.0, 1.0)
    flag_trade(trade, 0.95, "position_neutral_scc")
    flag_trade(trade, 0.75, "repeated_amounts")  # must not downgrade
    assert trade.wash_trade_score == 0.95
    assert trade.detection_method == "position_neutral_scc"

    flag_trade(trade, 1.0, "self_trading")  # upgrades
    assert trade.wash_trade_score == 1.0
    assert trade.detection_method == "self_trading"


@pytest.mark.asyncio
async def test_position_neutral_scc_flags_round_trip(detector):
    # Alice sells 100 X to Bob, Bob sells 100 X back: both end flat while
    # gross volume doubles - the canonical wash pattern.
    trades = [
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=1),
        make_trade(BOB, ALICE, 200.0, 100.0, minutes=2, token_in=TOKEN_Y, token_out=TOKEN_X),
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=3),
        make_trade(BOB, ALICE, 200.0, 100.0, minutes=4, token_in=TOKEN_Y, token_out=TOKEN_X),
    ]

    result = await detector.detect_position_neutral_scc(trades, AsyncMock())

    assert len(result) == 4
    assert all(t.detection_method == "position_neutral_scc" for t in result)
    assert all(t.wash_trade_score == 0.95 for t in result)


@pytest.mark.asyncio
async def test_position_neutral_scc_ignores_directional_flow(detector):
    # Alice keeps buying from Bob who never buys back the same amount:
    # positions change materially, so nothing is flagged.
    trades = [
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=1),
        make_trade(BOB, ALICE, 50.0, 25.0, minutes=2, token_in=TOKEN_Y, token_out=TOKEN_X),
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=3),
        make_trade(BOB, ALICE, 50.0, 25.0, minutes=4, token_in=TOKEN_Y, token_out=TOKEN_X),
    ]

    result = await detector.detect_position_neutral_scc(trades, AsyncMock())

    assert result == []


@pytest.mark.asyncio
async def test_position_neutral_scc_requires_min_trades(detector):
    # A single round trip (2 trades) is below POSITION_NEUTRAL_MIN_TRADES (4).
    trades = [
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=1),
        make_trade(BOB, ALICE, 200.0, 100.0, minutes=2, token_in=TOKEN_Y, token_out=TOKEN_X),
    ]

    result = await detector.detect_position_neutral_scc(trades, AsyncMock())

    assert result == []


@pytest.mark.asyncio
async def test_position_neutral_scc_requires_scc(detector):
    # One-directional edges (no cycle) never form an SCC of size >= 2.
    trades = [
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=1),
        make_trade(CAROL, BOB, 100.0, 200.0, minutes=2),
        make_trade(ALICE, CAROL, 100.0, 200.0, minutes=3),
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=4),
    ]

    result = await detector.detect_position_neutral_scc(trades, AsyncMock())

    assert result == []


@pytest.mark.asyncio
async def test_closed_cluster_flags_isolated_ring(detector):
    # Alice/Bob trade exclusively with each other (6 internal trades); Carol
    # and Dave provide organic background flow in the same pool.
    trades = [
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=i, volume_usd=10_000.0) for i in range(3)
    ] + [
        make_trade(BOB, ALICE, 200.0, 100.0, minutes=10 + i, volume_usd=10_000.0) for i in range(3)
    ]
    trades += [
        make_trade(CAROL, DAVE, 5.0, 10.0, minutes=30, volume_usd=100.0),
        make_trade(DAVE, CAROL, 10.0, 5.0, minutes=31, volume_usd=100.0),
    ]

    result = await detector.detect_closed_cluster(trades, AsyncMock())

    flagged = {t.sender for t in result} | {t.recipient for t in result}
    assert ALICE in flagged and BOB in flagged
    assert all(t.detection_method == "closed_cluster" for t in result)
    assert len(result) == 6


@pytest.mark.asyncio
async def test_closed_cluster_ignores_open_traders(detector):
    # Alice trades reciprocally with Bob but her flow to Carol and Dave is
    # one-directional (organic distribution): her in/out volume is heavily
    # unbalanced, so no community passes the balance test.
    trades = [
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=1, volume_usd=100.0),
        make_trade(BOB, ALICE, 200.0, 100.0, minutes=2, volume_usd=100.0),
        make_trade(ALICE, CAROL, 100.0, 200.0, minutes=3, volume_usd=5_000.0),
        make_trade(ALICE, DAVE, 100.0, 200.0, minutes=4, volume_usd=5_000.0),
        make_trade(ALICE, CAROL, 100.0, 200.0, minutes=5, volume_usd=5_000.0),
        make_trade(ALICE, DAVE, 100.0, 200.0, minutes=6, volume_usd=5_000.0),
    ]

    result = await detector.detect_closed_cluster(trades, AsyncMock())

    assert result == []


@pytest.mark.asyncio
async def test_closed_cluster_requires_min_trades(detector):
    # Only 2 internal trades - below CLOSED_CLUSTER_MIN_TRADES (6).
    trades = [
        make_trade(ALICE, BOB, 100.0, 200.0, minutes=1),
        make_trade(BOB, ALICE, 200.0, 100.0, minutes=2),
    ]

    result = await detector.detect_closed_cluster(trades, AsyncMock())

    assert result == []


@pytest.mark.asyncio
async def test_repeated_amounts_flags_recycled_sizes(detector):
    # Five trades with amounts identical to 3 significant figures.
    trades = [
        make_trade(ALICE, BOB, 1000.0 + i, 2000.0, minutes=i)  # 1000..1004 -> 1000
        for i in range(5)
    ]
    trades.append(make_trade(CAROL, DAVE, 777.7, 1555.4, minutes=10))

    result = await detector.detect_repeated_amounts(trades, AsyncMock())

    assert len(result) == 5
    assert all(t.sender == ALICE for t in result)
    assert all(t.detection_method == "repeated_amounts" for t in result)


@pytest.mark.asyncio
async def test_repeated_amounts_ignores_organic_variation(detector):
    trades = [
        make_trade(ALICE, BOB, amount, 2 * amount, minutes=i)
        for i, amount in enumerate([137.2, 954.1, 22.9, 4810.0, 66.3])
    ]

    result = await detector.detect_repeated_amounts(trades, AsyncMock())

    assert result == []


@pytest.mark.asyncio
async def test_repeated_amounts_respects_allowlist(detector, monkeypatch):
    from config.settings import settings

    monkeypatch.setattr(
        type(settings), "bot_allowlist_set", property(lambda _self: {ALICE.lower()})
    )
    trades = [make_trade(ALICE, BOB, 1000.0, 2000.0, minutes=i) for i in range(5)]

    result = await detector.detect_repeated_amounts(trades, AsyncMock())

    assert result == []
