"""
Trade-size forensics for a whole pool, after Cong, Li, Tang and Yang, "Crypto
Wash Trading", Management Science 69(11), 2023.

People choose round amounts ("sell 1.5 ETH", "buy with 2,000 USDC"); scripts
that manufacture volume draw arbitrary ones. Authentic trade sizes also follow
Benford's law in their first significant digit, as most naturally occurring
magnitudes do; generated ones tend not to. Cong et al. used both tests to
tell regulated exchanges from those inflating their volume.

Both statistics describe the pool, not a trade: they label nothing, and are
reported with the audit next to the detectors' verdicts.

- **Round share**: the fraction of swaps where either side is a round amount,
  at most two significant digits (1.5, 2000, 0.25). Whichever side the trader
  fixed (the input of an exact-input swap, the output of an exact-output one)
  is the side a person would have typed.
- **Benford MAD**: the mean absolute deviation of the input amounts' first
  digits from Benford's proportions, read against Nigrini's thresholds for
  first digits (M. Nigrini, "Benford's Law", Wiley 2012, Table 7.1). A
  chi-square test is not used: with thousands of trades it rejects every real
  pool for trivial deviations.

Both are invariant to the token's decimals: scaling by a power of ten changes
neither the significant digits nor the first digit, so raw on-chain amounts
are used as they are.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any

from core.feature_engineer import benford_deviation
from models.schemas import SwapTrade

# Nigrini's first-digit MAD thresholds: the upper bound of each band.
NIGRINI_FIRST_DIGIT = (
    (0.006, "close"),
    (0.012, "acceptable"),
    (0.015, "marginal"),
)

# Below this many trades the first-digit proportions are too noisy to read
# against thresholds a few thousandths apart.
MIN_TRADES = 300

ROUND_MAX_SIGNIFICANT_DIGITS = 2


def significant_digits(amount: float) -> str:
    """The significant digits of a positive amount: 1500.0 -> "15",
    0.25 -> "25", 9.344128879913286e18 -> "9344128879913286". Raw amounts
    are integers stored as floats; repr gives the shortest digits that
    round-trip, so a round amount stays round."""
    if not amount or amount <= 0 or not math.isfinite(amount):
        return ""
    mantissa = repr(float(amount)).split("e")[0]
    return mantissa.replace(".", "").strip("0")


def is_round(amount: float | None) -> bool:
    digits = significant_digits(amount or 0.0)
    return 0 < len(digits) <= ROUND_MAX_SIGNIFICANT_DIGITS


def conformity(mad: float) -> str:
    for upper, band in NIGRINI_FIRST_DIGIT:
        if mad <= upper:
            return band
    return "nonconformity"


@dataclass(frozen=True)
class TradeSizeForensics:
    trades: int
    round_share: float | None
    benford_mad: float | None
    benford_conformity: str  # close, acceptable, marginal, nonconformity, or insufficient data

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def trade_size_forensics(trades: Iterable[SwapTrade]) -> TradeSizeForensics:
    """Round share and Benford conformity of a pool's trade sizes."""
    swaps = [t for t in trades if (t.amount_in or 0) > 0 or (t.amount_out or 0) > 0]
    if len(swaps) < MIN_TRADES:
        return TradeSizeForensics(len(swaps), None, None, "insufficient data")

    round_share = sum(1 for t in swaps if is_round(t.amount_in) or is_round(t.amount_out)) / len(
        swaps
    )
    inputs = [t.amount_in for t in swaps if (t.amount_in or 0) > 0]
    if len(inputs) < MIN_TRADES:
        return TradeSizeForensics(len(swaps), round(round_share, 4), None, "insufficient data")
    mad = benford_deviation(inputs)
    return TradeSizeForensics(len(swaps), round(round_share, 4), round(mad, 4), conformity(mad))
