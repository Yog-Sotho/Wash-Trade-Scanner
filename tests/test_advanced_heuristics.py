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
    trader_of,
)
from models.schemas import AddressCluster, SwapTrade

ALICE = "0x" + "a" * 40
BOB = "0x" + "b" * 40
CAROL = "0x" + "c" * 40
DAVE = "0x" + "d" * 40
TOKEN_X = "0x" + "1" * 40
TOKEN_Y = "0x" + "2" * 40
POOL = "0x" + "9" * 40
ROUTER = "0x" + "7" * 40

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
    tx_from: str | None = None,
    block: int = 1,
    log_index: int = 0,
    tx_hash: str | None = None,
) -> SwapTrade:
    trade_id = next(_next_id)
    return SwapTrade(
        id=trade_id,
        chain_id=1,
        dex_name="uniswap_v2",
        pool_address=pool,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        amount_out=amount_out,
        sender=sender,
        recipient=recipient,
        tx_from=tx_from,
        transaction_hash=tx_hash or "0x" + f"{trade_id:064x}",
        block_number=block,
        block_timestamp=BASE_TIME + timedelta(minutes=minutes),
        log_index=log_index,
        volume_usd=volume_usd,
    )


def routed(
    trader: str,
    block: int,
    sell_x: bool = True,
    volume_usd: float = 1000.0,
    amount_in: float = 100.0,
    amount_out: float = 200.0,
    log_index: int = 0,
    **kwargs,
) -> SwapTrade:
    """A swap `trader` sent through the router: the event names the router as
    sender, as on a real AMM."""
    token_in, token_out = (TOKEN_X, TOKEN_Y) if sell_x else (TOKEN_Y, TOKEN_X)
    return make_trade(
        ROUTER,
        trader,
        amount_in,
        amount_out,
        minutes=block,
        token_in=token_in,
        token_out=token_out,
        volume_usd=volume_usd,
        tx_from=trader,
        block=block,
        log_index=log_index,
        **kwargs,
    )


def round_trips(trader: str, count: int, gap: int = 5, start: int = 100, value_drift=0.003):
    """`count` buy-then-sell-back pairs, legs `gap` blocks apart."""
    trades = []
    block = start
    for _ in range(count):
        trades.append(routed(trader, block, sell_x=True, amount_in=100.0, amount_out=200.0))
        trades.append(
            routed(
                trader,
                block + gap,
                sell_x=False,
                amount_in=200.0,
                amount_out=99.4,
                volume_usd=1000.0 * (1 - value_drift),
            )
        )
        block += gap + 50
    return trades


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


def test_trader_is_the_transaction_sender_with_event_sender_fallback():
    assert trader_of(routed(ALICE, 1)) == ALICE
    assert trader_of(make_trade(BOB.upper().replace("0X", "0x"), CAROL, 1, 1)) == BOB


# --------------------------------------------------------------- round trips


@pytest.fixture(autouse=True)
def chainalysis_round_trips(monkeypatch):
    """Pin the round-trip parameters to Chainalysis heuristic 1 (25 blocks, 1%,
    3+ round trips) so these tests exercise the logic, not the tuned defaults."""
    from config.settings import settings

    monkeypatch.setattr(settings, "ROUND_TRIP_MAX_BLOCKS", 25)
    monkeypatch.setattr(settings, "ROUND_TRIP_VALUE_TOLERANCE", 0.01)
    monkeypatch.setattr(settings, "ROUND_TRIP_MIN_COUNT", 3)


@pytest.mark.asyncio
async def test_round_trips_flag_three_value_matched_pairs(detector):
    trades = round_trips(ALICE, 3)

    result = await detector.detect_round_trips(trades, AsyncMock())

    assert {t.id for t in result} == {t.id for t in trades}
    assert all(t.detection_method == "round_trip" for t in result)


@pytest.mark.asyncio
async def test_round_trips_need_three_pairs(detector):
    assert await detector.detect_round_trips(round_trips(ALICE, 2), AsyncMock()) == []


@pytest.mark.asyncio
async def test_round_trip_legs_must_match_within_one_percent(detector):
    trades = round_trips(ALICE, 3, value_drift=0.02)
    assert await detector.detect_round_trips(trades, AsyncMock()) == []


@pytest.mark.asyncio
async def test_round_trip_legs_must_be_within_25_blocks(detector):
    assert await detector.detect_round_trips(round_trips(ALICE, 3, gap=25), AsyncMock())
    assert await detector.detect_round_trips(round_trips(ALICE, 3, gap=26), AsyncMock()) == []


@pytest.mark.asyncio
async def test_round_trips_group_by_transaction_sender_not_router(detector):
    """Regression: every router swap shares the router as event sender, so
    keying on it lumped unrelated traders together."""
    alice = round_trips(ALICE, 3)
    # Bob and Carol each make one leg of every "pair" -- no one round-trips.
    mixed = []
    for i, trade in enumerate(alice):
        trader = BOB if i % 2 == 0 else CAROL
        trade.tx_from = trader
        trade.recipient = trader
        mixed.append(trade)
    assert await detector.detect_round_trips(mixed, AsyncMock()) == []


@pytest.mark.asyncio
async def test_sandwich_legs_are_not_round_trips(detector):
    """Front-run and back-run in one block around a victim: MEV, not wash."""
    trades = []
    for block in (100, 200, 300):
        trades.append(routed(ALICE, block, sell_x=True, log_index=1))
        trades.append(routed(BOB, block, sell_x=True, log_index=2, volume_usd=5000.0))
        trades.append(
            routed(ALICE, block, sell_x=False, log_index=3, amount_in=200.0, volume_usd=999.0)
        )
    result = await detector.detect_round_trips(trades, AsyncMock())
    assert result == []


@pytest.mark.asyncio
async def test_atomic_buy_and_sell_in_one_transaction_counts(detector):
    """Volume bots buy and sell in one transaction; nobody trades in between."""
    trades = []
    for block in (100, 200, 300):
        tx = "0x" + f"{block:064x}"
        trades.append(routed(ALICE, block, sell_x=True, log_index=1, tx_hash=tx))
        trades.append(
            routed(
                ALICE,
                block,
                sell_x=False,
                log_index=2,
                amount_in=200.0,
                volume_usd=998.0,
                tx_hash=tx,
            )
        )
    result = await detector.detect_round_trips(trades, AsyncMock())
    assert len(result) == 6


@pytest.mark.asyncio
async def test_unpriced_round_trips_compare_amounts_sold_back(detector):
    trades = round_trips(ALICE, 3)
    for trade in trades:
        trade.volume_usd = None
    result = await detector.detect_round_trips(trades, AsyncMock())
    assert len(result) == 6


# -------------------------------------------------------- position neutrality


def _cycle(trader: str, start: int) -> list[SwapTrade]:
    """Sell 100 X for 200 Y, then 200 Y back for 100 X: flat, volume doubled."""
    return [
        routed(trader, start, sell_x=True, amount_in=100.0, amount_out=200.0),
        routed(trader, start + 1, sell_x=False, amount_in=200.0, amount_out=100.0),
    ]


@pytest.mark.asyncio
async def test_position_neutral_flags_a_trader_who_ends_flat(detector):
    trades = _cycle(ALICE, 1) + _cycle(ALICE, 3)

    result = await detector.detect_position_neutral(trades, AsyncMock())

    assert len(result) == 4
    assert all(t.detection_method == "position_neutral" for t in result)
    assert all(t.wash_trade_score == 0.95 for t in result)


@pytest.mark.asyncio
async def test_position_neutral_ignores_directional_flow(detector):
    trades = [
        routed(ALICE, 1, sell_x=True, amount_in=100.0, amount_out=200.0),
        routed(ALICE, 2, sell_x=False, amount_in=50.0, amount_out=25.0),
        routed(ALICE, 3, sell_x=True, amount_in=100.0, amount_out=200.0),
        routed(ALICE, 4, sell_x=False, amount_in=50.0, amount_out=25.0),
    ]
    assert await detector.detect_position_neutral(trades, AsyncMock()) == []


@pytest.mark.asyncio
async def test_position_neutral_ignores_one_sided_flow(detector):
    trades = [routed(ALICE, b, sell_x=True) for b in range(1, 6)]
    assert await detector.detect_position_neutral(trades, AsyncMock()) == []


@pytest.mark.asyncio
async def test_position_neutral_requires_min_trades(detector):
    assert await detector.detect_position_neutral(_cycle(ALICE, 1), AsyncMock()) == []


@pytest.mark.asyncio
async def test_position_neutral_does_not_merge_different_traders(detector):
    """Regression: positions were keyed by event sender, i.e. the router, so
    many unrelated traders netted out against each other."""
    trades = [
        routed(ALICE, 1, sell_x=True, amount_in=100.0, amount_out=200.0),
        routed(BOB, 2, sell_x=False, amount_in=200.0, amount_out=100.0),
        routed(CAROL, 3, sell_x=True, amount_in=100.0, amount_out=200.0),
        routed(DAVE, 4, sell_x=False, amount_in=200.0, amount_out=100.0),
    ]
    assert await detector.detect_position_neutral(trades, AsyncMock()) == []


@pytest.mark.asyncio
async def test_position_neutral_nets_a_funding_cluster_as_one_entity(detector):
    """Each wallet only buys or only sells, but together they end flat."""
    trades = [
        routed(ALICE, 1, sell_x=True, amount_in=100.0, amount_out=200.0),
        routed(BOB, 2, sell_x=False, amount_in=200.0, amount_out=100.0),
        routed(CAROL, 3, sell_x=True, amount_in=100.0, amount_out=200.0),
        routed(DAVE, 4, sell_x=False, amount_in=200.0, amount_out=100.0),
    ]
    cluster = AddressCluster(cluster_id="c1", addresses=[ALICE, BOB, CAROL, DAVE])

    result = await detector.detect_position_neutral(trades, AsyncMock(), [cluster])

    assert len(result) == 4


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


@pytest.mark.asyncio
async def test_repeated_amounts_group_by_trader_not_router(detector):
    """Regression: keyed by event sender (the router), five different users who
    happened to trade the same round size were one 'bot'."""
    trades = [
        routed(trader, i, amount_in=1000.0)
        for i, trader in enumerate([ALICE, BOB, CAROL, DAVE, "0x" + "e" * 40])
    ]
    assert await detector.detect_repeated_amounts(trades, AsyncMock()) == []

    same_trader = [routed(ALICE, i, amount_in=1000.0) for i in range(5)]
    assert len(await detector.detect_repeated_amounts(same_trader, AsyncMock())) == 5


@pytest.mark.asyncio
async def test_repeated_amounts_does_not_mix_tokens(detector):
    """1000 units of one token and 1000 of the other are different sizes."""
    # Six repeats of the number 1000, but only three of each token (< 5).
    trades = [routed(ALICE, i, sell_x=i % 2 == 0, amount_in=1000.0) for i in range(6)]
    assert await detector.detect_repeated_amounts(trades, AsyncMock()) == []
