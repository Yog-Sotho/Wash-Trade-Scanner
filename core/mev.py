"""
MEV discrimination (roadmap Phase 6): trades that are MEV extraction, not
wash trading, are taken out before any detector runs.

A sandwich attack buys just before a victim's swap and sells just after it,
in the same block: the attacker trades both sides, ends flat and repeats, so
position-neutrality and round-trip tests would call it wash trading. It is
recognised by structure, as in the MEV literature (Torres et al., "Frontrunner
Jones and the Raiders of the Dark Forest", USENIX Security 2021; Qin et al.,
"Quantifying Blockchain Extractable Value", IEEE S&P 2022): in one block and
pool,

1. the attacker's front-run swap,
2. then at least one swap by someone else in the same direction (the victim,
   who pays the worse price),
3. then the attacker's back-run swap in the opposite direction, selling what
   the front-run bought (within SANDWICH_AMOUNT_TOLERANCE).

The attacker is the transaction sender or, as sandwich bots often send the two
legs from different accounts, the bot contract that called the pool and kept
the output. Only the attacker's two legs are taken out; the victim's swap is
an ordinary trade.

When several pools are audited together (cross-pool correlation), atomic
arbitrage is taken out too: one transaction buying in one pool and selling
in another, ending with more of a token and less of none. Its legs look like
a round trip across pools, but an atomic transaction can only gain from a
price difference between the pools: that is arbitrage, not wash trading,
which pays the pools' fees and loses.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from config.settings import settings
from models.schemas import SwapTrade


def _trader(trade: SwapTrade) -> str:
    return (trade.tx_from or trade.sender or "").lower()


def _bot_contract(trade: SwapTrade) -> str | None:
    """The contract that called the pool and kept the output, if any: a
    sandwich bot's identity across the accounts it sends from."""
    sender = (trade.sender or "").lower()
    return sender if sender and sender == (trade.recipient or "").lower() else None


def _same_attacker(a: SwapTrade, b: SwapTrade) -> bool:
    if _trader(a) == _trader(b):
        return True
    bot = _bot_contract(a)
    return bot is not None and bot == _bot_contract(b)


def _direction(trade: SwapTrade) -> tuple[str, str]:
    return (trade.token_in or "").lower(), (trade.token_out or "").lower()


@dataclass(frozen=True)
class Sandwich:
    front: SwapTrade
    back: SwapTrade
    victims: tuple[SwapTrade, ...]


def find_sandwiches(trades: list[SwapTrade]) -> list[Sandwich]:
    """Every sandwich attack among `trades`, each trade in at most one."""
    tolerance = settings.SANDWICH_AMOUNT_TOLERANCE
    blocks: dict[tuple[str, int], list[SwapTrade]] = defaultdict(list)
    for trade in trades:
        if trade.block_number is not None:  # without a block it sandwiches nothing
            blocks[(trade.pool_address, trade.block_number)].append(trade)

    found: list[Sandwich] = []
    for block_trades in blocks.values():
        if len(block_trades) < 3:
            continue
        ordered = sorted(block_trades, key=lambda t: t.log_index or 0)
        used: set[int] = set()
        for i, front in enumerate(ordered):
            if id(front) in used:
                continue
            direction = _direction(front)
            for j in range(i + 2, len(ordered)):
                back = ordered[j]
                if id(back) in used or _direction(back) != direction[::-1]:
                    continue
                if not _same_attacker(front, back):
                    continue
                bought, sold = front.amount_out or 0.0, back.amount_in or 0.0
                if bought <= 0 or abs(sold - bought) / bought > tolerance:
                    continue
                victims = tuple(
                    t
                    for t in ordered[i + 1 : j]
                    if _direction(t) == direction and not _same_attacker(t, front)
                )
                if not victims:
                    continue
                found.append(Sandwich(front, back, victims))
                used.update((id(front), id(back)))
                break
    return found


@dataclass(frozen=True)
class Arbitrage:
    legs: tuple[SwapTrade, ...]
    gain: dict[str, float]  # per token: what the transaction ended with more of


def find_atomic_arbitrage(trades: list[SwapTrade]) -> list[Arbitrage]:
    """Transactions that swap in two or more of the given pools and end with
    more of a token and less of none: a cycle (two pools of one pair, or a
    triangle through three) that gained. A route that is not a cycle spends
    its input, and a purchase spends what it pays with, so neither passes."""
    by_tx: dict[str, list[SwapTrade]] = defaultdict(list)
    for trade in trades:
        by_tx[trade.transaction_hash].append(trade)

    found: list[Arbitrage] = []
    for legs in by_tx.values():
        if len(legs) < 2 or len({t.pool_address for t in legs}) < 2:
            continue
        net: dict[str, float] = defaultdict(float)
        gross: dict[str, float] = defaultdict(float)
        for t in legs:
            token_in, token_out = _direction(t)
            net[token_in] -= t.amount_in or 0.0
            net[token_out] += t.amount_out or 0.0
            gross[token_in] += t.amount_in or 0.0
            gross[token_out] += t.amount_out or 0.0
        # Float noise aside (raw amounts are integers stored as floats).
        if any(net[token] < -1e-9 * gross[token] for token in net):
            continue  # it lost something: not arbitrage
        gain = {token: value for token, value in net.items() if value > 1e-9 * gross[token]}
        if gain:
            ordered = tuple(sorted(legs, key=lambda t: t.log_index or 0))
            found.append(Arbitrage(ordered, gain))
    return found


@dataclass(frozen=True)
class MevFindings:
    sandwiches: list[Sandwich]
    arbitrages: list[Arbitrage]

    @property
    def legs(self) -> set[int]:
        sandwich_legs = {id(t) for s in self.sandwiches for t in (s.front, s.back)}
        return sandwich_legs | {id(t) for a in self.arbitrages for t in a.legs}


def without_mev(trades: list[SwapTrade]) -> tuple[list[SwapTrade], MevFindings]:
    """`trades` minus the legs of every sandwich attack and atomic arbitrage."""
    findings = MevFindings(find_sandwiches(trades), find_atomic_arbitrage(trades))
    legs = findings.legs
    return [t for t in trades if id(t) not in legs], findings
