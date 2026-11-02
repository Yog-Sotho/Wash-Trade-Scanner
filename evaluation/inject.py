"""
Labelled wash-trading campaigns injected into real pool histories.

Ground truth for wash trading is scarce, so the harness takes a real swap
history as the honest background and injects campaigns whose every trade is a
known positive. Each pattern reproduces a mechanism documented by a primary
source; the parameters below say which:

- ``aa_round_trip``: one address buys and sells back within 25 blocks at
  matching USD value, three or more times -- Chainalysis's 2025 wash-trading
  heuristic 1 (Crypto Crime Report, "Crypto Market Manipulation" chapter).
- ``aa_round_trip_slow``: the same address-level round trips, spread beyond
  that 25-block window (26-300 blocks), to measure recall past it.
- ``funded_ring``: a controller funds five or more wallets that trade against
  each other's flow so the group's buy and sell volume match within 5% --
  Chainalysis heuristic 2 -- with wallets trading almost simultaneously and
  the group's holdings kept near constant (Gan et al., "Exposing Stealthy Wash
  Trading on Automated Market Maker Exchanges", ACM TOIT 24(4), 2024). The
  funding links are given to the detectors, as entity clustering would find.
- ``unlinked_ring``: the same ring when no funding link is observable.
- ``repeated_size_bot``: one bot repeating identical trade sizes at a fixed
  cadence, alternating sides -- the pattern Kaiko reported in the FBI's
  NexFundAI market (identical trades, same size and timing, for a month).

Injected trades are valued with the production pricing rules from the USD
value the background itself shows for each token at that time, so a campaign
is priced exactly as an audit would price it.
"""

from __future__ import annotations

import bisect
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from statistics import median
from typing import Any

from config.tokens import get_reference_asset
from core.pricing import _volume_usd

PATTERNS = (
    "aa_round_trip",
    "aa_round_trip_slow",
    "funded_ring",
    "unlinked_ring",
    "repeated_size_bot",
)

_LOG_INDEX_BASE = 10_000  # keeps injected log indexes clear of real ones


@dataclass
class Campaign:
    pattern: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    wallets: list[str] = field(default_factory=list)
    # Funding clusters an entity clusterer would recover (empty when unlinked).
    clusters: list[list[str]] = field(default_factory=list)


class Market:
    """What the background tells us about a pool: tokens, unit prices over time,
    fee, routing contracts, gas prices and the block clock."""

    def __init__(self, rows: list[dict[str, Any]], chain_id: int) -> None:
        if not rows:
            raise ValueError("empty background")
        self.chain_id = chain_id
        self.rows = sorted(rows, key=lambda r: (r["block_number"], r["log_index"]))
        self.pool = self.rows[0]["pool_address"]
        self.dex_name = self.rows[0]["dex_name"]
        tokens = Counter(r["token_in"] for r in self.rows) + Counter(
            r["token_out"] for r in self.rows
        )
        self.tokens = [token for token, _ in tokens.most_common(2)]
        if len(self.tokens) != 2:
            raise ValueError("background must be a two-token pool")

        # Per-token USD value of one raw unit, as observed at each trade.
        self._unit_usd: dict[str, tuple[list[int], list[float]]] = {}
        for token in self.tokens:
            blocks: list[int] = []
            values: list[float] = []
            for r in self.rows:
                for leg_token, amount, usd in (
                    (r["token_in"], r["amount_in"], r.get("amount_in_usd")),
                    (r["token_out"], r["amount_out"], r.get("amount_out_usd")),
                ):
                    if leg_token == token and usd and amount:
                        blocks.append(r["block_number"])
                        values.append(usd / amount)
            self._unit_usd[token] = (blocks, values)

        ratios = [
            r["amount_out_usd"] / r["amount_in_usd"]
            for r in self.rows
            if r.get("amount_in_usd") and r.get("amount_out_usd")
        ]
        # Median out/in value ratio: the pool fee plus typical price impact.
        self.out_in_ratio = median(ratios) if ratios else 0.997
        self.median_volume_usd = float(
            median(r["volume_usd"] for r in self.rows if r.get("volume_usd"))
        )
        router_counts = Counter(
            r["tx_to"]
            for r in self.rows
            if r.get("tx_to") and r["sender"] == r["tx_to"] and r["recipient"] == r["tx_from"]
        )
        self.router = (
            router_counts.most_common(1)[0][0] if router_counts else self.rows[0]["sender"]
        )
        self.gas_prices = [r["gas_price"] for r in self.rows if r.get("gas_price")]
        self._blocks = [r["block_number"] for r in self.rows]
        self.first_block = self._blocks[0]
        self.last_block = self._blocks[-1]

    def unit_usd(self, token: str, block: int) -> float | None:
        blocks, values = self._unit_usd[token]
        if not blocks:
            return None
        i = bisect.bisect_left(blocks, block)
        candidates = [j for j in (i - 1, i) if 0 <= j < len(blocks)]
        nearest = min(candidates, key=lambda j: abs(blocks[j] - block))
        return values[nearest]

    def timestamp(self, block: int) -> Any:
        """Block time, extrapolated at 12 s per slot from the nearest real block."""
        i = bisect.bisect_left(self._blocks, block)
        j = min(max(i, 0), len(self._blocks) - 1)
        anchor = self.rows[j]
        return anchor["block_timestamp"] + timedelta(seconds=12 * (block - anchor["block_number"]))

    @property
    def priced_tokens(self) -> list[str]:
        return [t for t in self.tokens if self._unit_usd[t][0]]


class _Builder:
    def __init__(self, market: Market, rng: random.Random, pattern: str) -> None:
        self.market = market
        self.rng = rng
        self.campaign = Campaign(pattern=pattern)
        self._log_index = _LOG_INDEX_BASE

    def address(self) -> str:
        return "0x" + f"{self.rng.getrandbits(160):040x}"

    def swap(
        self,
        block: int,
        trader: str,
        token_in: str,
        usd: float,
        via_bot: str | None = None,
        amount_in: float | None = None,
    ) -> dict[str, Any]:
        """One swap of `usd` worth of `token_in`, sent by `trader` through the
        background's main router, or through `via_bot` (a contract that calls
        the pool directly and keeps the output)."""
        m = self.market
        token_out = m.tokens[1] if token_in == m.tokens[0] else m.tokens[0]
        unit_in = m.unit_usd(token_in, block)
        unit_out = m.unit_usd(token_out, block)
        if amount_in is None:
            if unit_in is None:
                raise ValueError("cannot size a trade in an unpriced token")
            amount_in = usd / unit_in
        in_usd = amount_in * unit_in if unit_in is not None else usd
        out_usd = in_usd * m.out_in_ratio
        amount_out = out_usd / unit_out if unit_out else 0.0
        if via_bot is None:
            sender, recipient, tx_to = m.router, trader, m.router
        else:
            sender, recipient, tx_to = via_bot, via_bot, via_bot
        asset_in = get_reference_asset(m.chain_id, token_in)
        asset_out = get_reference_asset(m.chain_id, token_out)
        leg_in = in_usd if asset_in is not None else None
        leg_out = out_usd if asset_out is not None else None
        self._log_index += 1
        row = {
            "chain_id": m.chain_id,
            "dex_name": m.dex_name,
            "pool_address": m.pool,
            "token_in": token_in,
            "token_out": token_out,
            "amount_in": float(round(amount_in)),
            "amount_out": float(round(amount_out)),
            "sender": sender,
            "recipient": recipient,
            "tx_from": trader,
            "tx_to": tx_to,
            "transaction_hash": "0x" + f"{self.rng.getrandbits(256):064x}",
            "block_number": block,
            "block_timestamp": m.timestamp(block),
            "gas_price": self.rng.choice(m.gas_prices) if m.gas_prices else None,
            "gas_used": None,
            "log_index": self._log_index,
            "amount_in_usd": leg_in,
            "amount_out_usd": leg_out,
            "volume_usd": _volume_usd(asset_in, leg_in, asset_out, leg_out),
        }
        self.campaign.rows.append(row)
        return row

    def notional(self) -> float:
        return self.market.median_volume_usd * self.rng.uniform(0.5, 5.0)

    def start_block(self, span: int) -> int:
        m = self.market
        latest_start = max(m.first_block, m.last_block - span)
        return self.rng.randint(m.first_block, latest_start)


def _round_trips(b: _Builder, gap_range: tuple[int, int]) -> None:
    m = b.market
    wallet = b.address()
    b.campaign.wallets.append(wallet)
    rounds = b.rng.randint(3, 8)
    spacing = (30, 600)
    block = b.start_block(rounds * (gap_range[1] + spacing[1]))
    priced = m.priced_tokens
    for _ in range(rounds):
        token_in = b.rng.choice(priced)
        buy = b.swap(block, wallet, token_in, b.notional())
        sell_block = block + b.rng.randint(*gap_range)
        # Sell back everything the first leg bought.
        b.swap(sell_block, wallet, buy["token_out"], 0.0, amount_in=buy["amount_out"])
        block = sell_block + b.rng.randint(*spacing)


def _ring(b: _Builder, linked: bool) -> None:
    m = b.market
    wallets = [b.address() for _ in range(b.rng.randint(5, 10))]
    b.campaign.wallets.extend(wallets)
    if linked:
        b.campaign.clusters.append(list(wallets))
    events = b.rng.randint(4, 10)
    block = b.start_block(events * 400)
    priced = m.priced_tokens
    for _ in range(events):
        buyer, seller = b.rng.sample(wallets, 2)
        usd = b.notional()
        token = b.rng.choice(priced)
        other = m.tokens[1] if token == m.tokens[0] else m.tokens[0]
        # Opposite sides within 0-3 blocks, sized to within 5% of each other.
        b.swap(block, buyer, token, usd)
        b.swap(block + b.rng.randint(0, 3), seller, other, usd * b.rng.uniform(0.95, 1.05))
        block += b.rng.randint(20, 400)


def _repeated_size_bot(b: _Builder) -> None:
    m = b.market
    operator = b.address()
    bot = b.address()
    b.campaign.wallets.append(operator)
    trades = b.rng.randint(10, 30)
    cadence = b.rng.randint(5, 50)
    block = b.start_block(trades * cadence)
    token = b.rng.choice(m.priced_tokens)
    other = m.tokens[1] if token == m.tokens[0] else m.tokens[0]
    unit = m.unit_usd(token, block)
    assert unit is not None
    size = round(b.notional() / unit, -3)  # a fixed, round size
    for i in range(trades):
        if i % 2 == 0:
            leg = b.swap(block, operator, token, 0.0, via_bot=bot, amount_in=size)
        else:
            b.swap(block, operator, other, 0.0, via_bot=bot, amount_in=leg["amount_out"])
        block += cadence


def inject(pattern: str, market: Market, seed: int) -> Campaign:
    """Build one campaign of `pattern`, deterministic in `seed`."""
    # Seeded for reproducible simulations, not for anything security-related.
    rng = random.Random(f"{pattern}:{market.pool}:{seed}")  # nosec B311
    builder = _Builder(market, rng, pattern)
    if pattern == "aa_round_trip":
        _round_trips(builder, (1, 25))
    elif pattern == "aa_round_trip_slow":
        _round_trips(builder, (26, 300))
    elif pattern == "funded_ring":
        _ring(builder, linked=True)
    elif pattern == "unlinked_ring":
        _ring(builder, linked=False)
    elif pattern == "repeated_size_bot":
        _repeated_size_bot(builder)
    else:
        raise ValueError(f"unknown pattern {pattern}")
    return builder.campaign
