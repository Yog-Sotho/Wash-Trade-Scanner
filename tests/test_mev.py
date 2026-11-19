"""
MEV discrimination (roadmap Phase 6): sandwich attackers trade both sides
and end flat, like wash traders, but they are recognised by structure and
taken out before detection.
"""

from datetime import datetime

import pytest

from config.settings import settings
from core.calibration import DetectorCalibration, override_calibration
from core.heuristics import EXCLUDED_MEV, HeuristicDetector
from core.mev import find_sandwiches, without_sandwiches
from models.schemas import SwapTrade

WETH, USDC = "0x" + "e" * 40, "0x" + "c" * 40
ATTACKER, VICTIM = "0x" + "a" * 40, "0x" + "f" * 40
BOT, ROUTER = "0x" + "b" * 40, "0x" + "7" * 40

_ids = iter(range(1, 10**6))


def swap(
    trader: str,
    block: int,
    log_index: int,
    sold: str,
    amount_in: float,
    amount_out: float,
    bot: str | None = None,
) -> SwapTrade:
    i = next(_ids)
    return SwapTrade(
        id=i,
        chain_id=1,
        pool_address="0xpool",
        token_in=sold,
        token_out=WETH if sold == USDC else USDC,
        amount_in=amount_in,
        amount_out=amount_out,
        sender=bot or ROUTER,
        recipient=bot or trader,
        tx_from=trader,
        transaction_hash="0x" + f"{i:064x}",
        log_index=log_index,
        block_number=block,
        block_timestamp=datetime(2026, 1, 15, 12, 0),
        volume_usd=amount_in if sold == USDC else amount_out,
        is_wash_trade=False,
        wash_trade_score=0.0,
    )


def sandwich(block: int = 100, victim_side: str = USDC, back_amount: float = 1.0):
    """Attacker buys 1 WETH, the victim buys after it, the attacker sells
    `back_amount` WETH back."""
    front = swap(ATTACKER, block, 0, USDC, 3000.0, 1.0)
    victim = swap(VICTIM, block, 1, victim_side, 5000.0, 1.6)
    back = swap(ATTACKER, block, 2, WETH, back_amount, 3010.0)
    return front, victim, back


def test_a_front_run_victim_and_back_run_in_one_block_is_a_sandwich():
    front, victim, back = sandwich()
    (found,) = find_sandwiches([back, victim, front])  # any input order
    assert (found.front, found.back, found.victims) == (front, back, (victim,))


def test_legs_with_nobody_between_are_not_a_sandwich():
    front = swap(ATTACKER, 100, 0, USDC, 3000.0, 1.0)
    back = swap(ATTACKER, 100, 1, WETH, 1.0, 2990.0)
    other = swap(VICTIM, 100, 2, USDC, 5000.0, 1.6)
    assert find_sandwiches([front, back, other]) == []


def test_a_swap_between_in_the_other_direction_is_no_victim():
    assert find_sandwiches(list(sandwich(victim_side=WETH))) == []


def test_legs_in_different_blocks_are_not_a_sandwich():
    front, victim, back = sandwich()
    back.block_number = 101
    victim.block_number = 101
    assert find_sandwiches([front, victim, back]) == []


@pytest.mark.parametrize(
    ("back_amount", "found"),
    [
        (1.0 * (1 + settings.SANDWICH_AMOUNT_TOLERANCE) - 1e-9, True),
        (1.0 * (1 + settings.SANDWICH_AMOUNT_TOLERANCE) + 1e-6, False),
    ],
)
def test_the_back_run_sells_what_the_front_run_bought(back_amount, found):
    assert bool(find_sandwiches(list(sandwich(back_amount=back_amount)))) is found


def test_a_bot_contract_sending_from_two_accounts_is_one_attacker():
    front = swap(ATTACKER, 100, 0, USDC, 3000.0, 1.0, bot=BOT)
    victim = swap(VICTIM, 100, 1, USDC, 5000.0, 1.6)
    back = swap("0x" + "d" * 40, 100, 2, WETH, 1.0, 3010.0, bot=BOT)
    assert len(find_sandwiches([front, victim, back])) == 1


def test_only_the_attackers_legs_are_taken_out():
    front, victim, back = sandwich()
    kept, found = without_sandwiches([front, victim, back])
    assert kept == [victim] and len(found) == 1


async def test_a_sandwich_bot_is_never_labelled_and_its_legs_are_counted():
    """Four sandwiches by one bot: flat in every window, so position
    neutrality alone would call it wash."""
    override_calibration({"position_neutral": DetectorCalibration(tpr=0.9, fpr_upper=0.0001)})
    try:
        trades = [t for block in range(100, 104) for t in sandwich(block)]
        for trade in trades:  # end exactly flat
            if trade.tx_from == ATTACKER and trade.token_in == WETH:
                trade.amount_out = 3000.0

        candidates, _ = without_sandwiches(trades)
        h = HeuristicDetector()
        assert await h.advanced.detect_position_neutral(trades, None) != []  # would fire

        wash, stats = await h.run_detectors_on_trades(trades, None)
    finally:
        override_calibration(None)

    assert wash == []
    assert stats[EXCLUDED_MEV] == 8
    assert all(t.tx_from == VICTIM for t in candidates)
