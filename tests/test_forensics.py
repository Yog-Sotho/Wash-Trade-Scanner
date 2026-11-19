"""
Trade-size forensics (Cong, Li, Tang and Yang 2023): round amounts and
Benford conformity of a pool's trade sizes.
"""

import random
from datetime import datetime

import pytest

from core.forensics import (
    MIN_TRADES,
    conformity,
    is_round,
    significant_digits,
    trade_size_forensics,
)
from evaluation.dataset import fixture_names, load_fixture, to_swap_trades
from models.schemas import SwapTrade


def swap(amount_in: float, amount_out: float) -> SwapTrade:
    return SwapTrade(
        chain_id=1,
        pool_address="0xpool",
        token_in="0xa",
        token_out="0xb",
        amount_in=amount_in,
        amount_out=amount_out,
        sender="0xs",
        recipient="0xr",
        transaction_hash="0xtx",
        log_index=0,
        block_number=1,
        block_timestamp=datetime(2026, 1, 1),
    )


@pytest.mark.parametrize(
    ("amount", "digits"),
    [
        (1500.0, "15"),
        (0.25, "25"),
        (1.5e18, "15"),  # 1.5 tokens of 18 decimals, raw
        (2_000_000_000.0, "2"),  # 2,000 USDC, raw
        (21827650210.0, "2182765021"),
        (9.344128879913286e18, "9344128879913286"),
        (0.0, ""),
        (-3.0, ""),
    ],
)
def test_significant_digits(amount, digits):
    assert significant_digits(amount) == digits


def test_round_amounts_have_at_most_two_significant_digits():
    assert is_round(1.5e18) and is_round(2e9) and is_round(250.0)
    assert not is_round(1.25e18) and not is_round(0.0) and not is_round(None)


def test_nigrini_bands():
    assert conformity(0.006) == "close"
    assert conformity(0.0061) == "acceptable"
    assert conformity(0.012) == "acceptable"
    assert conformity(0.0121) == "marginal"
    assert conformity(0.015) == "marginal"
    assert conformity(0.0151) == "nonconformity"


def benford_amounts(n: int, rng: random.Random) -> list[float]:
    """Log-uniform over six decades: first digits follow Benford's law."""
    return [10 ** rng.uniform(3, 9) for _ in range(n)]


def test_benford_distributed_sizes_conform_and_uniform_digits_do_not():
    rng = random.Random(7)
    natural = trade_size_forensics([swap(a, a * 1.0001) for a in benford_amounts(5000, rng)])
    assert natural.benford_conformity == "close"

    # First digits equally likely: what drawing sizes uniformly in [1, 10) gives.
    flat = [rng.uniform(1, 10) * 1e6 for _ in range(5000)]
    assert trade_size_forensics([swap(a, a) for a in flat]).benford_conformity == "nonconformity"


def test_either_side_counts_as_round():
    rng = random.Random(3)
    trades = [swap(a, a * 1.0001) for a in benford_amounts(MIN_TRADES, rng)]
    assert trade_size_forensics(trades).round_share == 0.0
    trades[0].amount_in = 2e18  # exact input of 2 tokens
    trades[1].amount_out = 5e8  # exact output of 500 USDC
    assert trade_size_forensics(trades).round_share == pytest.approx(2 / MIN_TRADES, abs=1e-4)


def test_statistics_do_not_depend_on_the_tokens_decimals():
    rng = random.Random(11)
    amounts = benford_amounts(1000, rng) + [1500.0] * 100
    base = trade_size_forensics([swap(a, a * 3.7) for a in amounts])
    scaled = trade_size_forensics([swap(a * 1e12, a * 3.7 * 1e12) for a in amounts])
    assert base == scaled


def test_too_few_trades_are_not_read():
    result = trade_size_forensics([swap(1.0, 2.0)] * (MIN_TRADES - 1))
    assert (result.round_share, result.benford_mad) == (None, None)
    assert result.benford_conformity == "insufficient data"


def test_the_documented_wash_market_stands_out_from_every_clean_pool():
    """The measured claim in docs/benchmarks.md: the FBI's NexF market has
    almost no round trades and nonconforming first digits; the clean pools
    conform and have many."""
    results = {
        name: (
            load_fixture(name).kind,
            trade_size_forensics(to_swap_trades(load_fixture(name).rows)),
        )
        for name in fixture_names()
    }
    clean = [f for kind, f in results.values() if kind == "clean"]
    wash = [f for kind, f in results.values() if kind == "documented_wash"]
    assert clean and wash
    for f in wash:
        assert f.benford_conformity == "nonconformity"
        assert all(f.round_share < c.round_share / 10 for c in clean)
    for c in clean:
        assert c.benford_conformity in ("close", "acceptable")
