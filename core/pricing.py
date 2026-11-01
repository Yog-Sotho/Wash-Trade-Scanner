"""
USD valuation of swaps from reference assets.

Each swap leg is valued only if its token is a reference asset (see
config/tokens.py): stablecoins at $1.00, market assets such as WETH at their
historical USD price. A swap's `volume_usd` is taken from its stablecoin leg
when it has one (an exact valuation), otherwise from its market-priced leg.

Legs that cannot be valued are left as None -- never guessed. That covers
tokens outside the registry (including Curve swaps, whose events identify
coins by index rather than address) and market prices that are unavailable
or too far in time from the swap. Pricing never fails ingestion: errors are
logged and the affected legs stay unpriced.
"""

from __future__ import annotations

import asyncio
import bisect
import logging
import math
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import aiohttp

from config.settings import settings
from config.tokens import AssetKind, ReferenceAsset, get_reference_asset

logger = logging.getLogger(__name__)

# CoinGecko returns hourly points for ranges of 2-90 days and daily points
# beyond that. Windows are 88 days plus up to 2 x 12h of staleness padding
# (PRICE_MAX_STALENESS_MINUTES is capped at 720), so at most 89 days: always
# hourly.
_WINDOW_SECONDS = 88 * 86_400
# A window reaching into the last hour may still gain data points, so it is
# refetched after this long instead of being cached for good. Failed fetches
# are retried after the same delay.
_REFRESH_SECONDS = 600
_MAX_ATTEMPTS = 3
_MAX_RETRY_AFTER_SECONDS = 60.0


class HistoricalPriceSource(Protocol):
    async def get_usd_prices(
        self, coin_id: str, timestamps: Iterable[float]
    ) -> dict[float, float | None]:
        """Map each UNIX timestamp (seconds) to a USD price, or None if unavailable."""
        ...


@dataclass
class _Series:
    times: list[float]  # UNIX seconds, ascending
    prices: list[float]
    fetched_at: float
    complete: bool  # window entirely in the past and fetched successfully


class CoinGeckoPriceSource:
    """Historical USD prices from CoinGecko's /coins/{id}/market_chart/range.

    Requests are split into fixed 88-day windows (cached per coin and window)
    so CoinGecko returns hourly points. A timestamp is priced only when a data
    point lies within `max_staleness_seconds` of it.

    Failures -- HTTP errors, throttling that outlasts the retries, and
    requests beyond the plan's history limit (the Demo/public API serves only
    the past 365 days and answers HTTP 401 beyond that) -- are logged and
    leave the affected timestamps unpriced.
    """

    _warned_no_key = False

    def __init__(
        self,
        api_url: str,
        api_key: str,
        timeout_seconds: float,
        max_staleness_seconds: float,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.max_staleness = max_staleness_seconds
        self._cache: dict[tuple[str, int], _Series] = {}
        self._clock = time.time  # wall clock (UNIX seconds); replaceable in tests
        # Pro keys only work against the Pro host; everything else uses the
        # Demo header (which the public host ignores when no key is set).
        self._key_header = (
            "x-cg-pro-api-key" if "pro-api.coingecko.com" in self.api_url else "x-cg-demo-api-key"
        )

    async def get_usd_prices(
        self, coin_id: str, timestamps: Iterable[float]
    ) -> dict[float, float | None]:
        wanted = sorted(set(timestamps))
        if not wanted:
            return {}

        windows = {self._window(t) for t in wanted}
        stale = sorted(w for w in windows if not self._fresh(coin_id, w))
        if stale:
            self._warn_if_keyless()
            async with aiohttp.ClientSession(timeout=self.timeout, trust_env=True) as session:
                for window in stale:
                    self._cache[(coin_id, window)] = await self._fetch(session, coin_id, window)

        return {t: self._lookup(self._cache[(coin_id, self._window(t))], t) for t in wanted}

    @staticmethod
    def _window(timestamp: float) -> int:
        return math.floor(timestamp / _WINDOW_SECONDS)

    def _fresh(self, coin_id: str, window: int) -> bool:
        series = self._cache.get((coin_id, window))
        if series is None:
            return False
        return series.complete or self._clock() - series.fetched_at < _REFRESH_SECONDS

    def _lookup(self, series: _Series, timestamp: float) -> float | None:
        if not series.times:
            return None
        i = bisect.bisect_left(series.times, timestamp)
        nearest = min(
            (j for j in (i - 1, i) if 0 <= j < len(series.times)),
            key=lambda j: abs(series.times[j] - timestamp),
        )
        if abs(series.times[nearest] - timestamp) > self.max_staleness:
            return None
        return series.prices[nearest]

    async def _fetch(self, session: aiohttp.ClientSession, coin_id: str, window: int) -> _Series:
        now = self._clock()
        start = window * _WINDOW_SECONDS - self.max_staleness
        end = (window + 1) * _WINDOW_SECONDS + self.max_staleness
        url = f"{self.api_url}/coins/{coin_id}/market_chart/range"
        params = {"vs_currency": "usd", "from": str(int(start)), "to": str(int(min(end, now)))}
        headers = {"accept": "application/json"}
        if self.api_key:
            headers[self._key_header] = self.api_key
        failed = _Series([], [], fetched_at=now, complete=False)

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        points = _parse_points(await resp.json(content_type=None))
                        if points is None:
                            logger.warning(f"CoinGecko {coin_id} returned a malformed price series")
                            return failed
                        return _Series(
                            times=[t for t, _ in points],
                            prices=[p for _, p in points],
                            fetched_at=now,
                            # The newest hourly point may still be pending for up to an hour.
                            complete=end < now - 3600,
                        )
                    if resp.status != 429 or attempt == _MAX_ATTEMPTS:
                        body = (await resp.text())[:300]
                        logger.warning(
                            f"CoinGecko {coin_id} prices unavailable (HTTP {resp.status}): {body}"
                        )
                        return failed
                    delay = _retry_delay(resp.headers.get("Retry-After"), attempt)
            except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                logger.warning(f"CoinGecko {coin_id} price request failed: {exc!r}")
                return failed
            await asyncio.sleep(delay)
        return failed

    def _warn_if_keyless(self) -> None:
        if not self.api_key and not CoinGeckoPriceSource._warned_no_key:
            CoinGeckoPriceSource._warned_no_key = True
            logger.info(
                "COINGECKO_API_KEY is not set: the keyless public API is heavily "
                "rate-limited. Set a free Demo key for reliable market-asset pricing."
            )


def _parse_points(payload: Any) -> list[tuple[float, float]] | None:
    """Extract ascending (unix_seconds, usd) points, or None if the shape is wrong.

    CoinGecko timestamps are in milliseconds. Non-positive or non-finite
    prices are dropped rather than trusted.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("prices", []), list):
        return None
    points = []
    for entry in payload.get("prices", []):
        if not isinstance(entry, list | tuple) or len(entry) != 2:
            return None
        try:
            ms, price = float(entry[0]), float(entry[1])
        except (TypeError, ValueError):
            return None
        if math.isfinite(ms) and math.isfinite(price) and price > 0:
            points.append((ms / 1000.0, price))
    return sorted(points)


def _retry_delay(retry_after: str | None, attempt: int) -> float:
    try:
        delay = float(retry_after) if retry_after is not None else 2.0**attempt
    except ValueError:
        delay = 2.0**attempt
    return min(max(delay, 0.0), _MAX_RETRY_AFTER_SECONDS)


def _epoch(timestamp: datetime) -> float:
    """Block timestamps are stored as naive UTC datetimes."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.timestamp()


@dataclass
class PricingStats:
    total: int = 0
    priced: int = 0
    no_reference_asset: int = 0
    market_price_unavailable: int = 0


class PriceOracle:
    """Values swap legs from the reference-asset registry."""

    def __init__(self, market_source: HistoricalPriceSource | None) -> None:
        self.market_source = market_source

    @classmethod
    def from_settings(cls) -> PriceOracle:
        if settings.NATIVE_PRICE_SOURCE == "none":
            return cls(market_source=None)
        return cls(
            market_source=CoinGeckoPriceSource(
                api_url=settings.COINGECKO_API_URL,
                api_key=settings.COINGECKO_API_KEY,
                timeout_seconds=settings.COINGECKO_TIMEOUT_SECONDS,
                max_staleness_seconds=settings.PRICE_MAX_STALENESS_MINUTES * 60,
            )
        )

    async def price_trades(self, chain_id: int, trades: list[dict[str, Any]]) -> PricingStats:
        """Set amount_in_usd, amount_out_usd and volume_usd on every trade dict.

        All three keys are always set (None where unpriced), which keeps the
        dicts uniform for multi-row INSERTs.
        """
        stats = PricingStats(total=len(trades))
        resolved = [
            (
                get_reference_asset(chain_id, trade["token_in"]),
                get_reference_asset(chain_id, trade["token_out"]),
                _epoch(trade["block_timestamp"]),
            )
            for trade in trades
        ]
        market_prices = await self._market_prices(resolved)

        for trade, (asset_in, asset_out, ts) in zip(trades, resolved, strict=True):
            in_usd = _leg_usd(asset_in, trade["amount_in"], ts, market_prices)
            out_usd = _leg_usd(asset_out, trade["amount_out"], ts, market_prices)
            volume = _volume_usd(asset_in, in_usd, asset_out, out_usd)
            trade["amount_in_usd"] = in_usd
            trade["amount_out_usd"] = out_usd
            trade["volume_usd"] = volume

            if volume is not None:
                stats.priced += 1
            elif asset_in is None and asset_out is None:
                stats.no_reference_asset += 1
            else:
                stats.market_price_unavailable += 1

        if trades:
            logger.info(
                f"Priced {stats.priced}/{stats.total} swaps on chain {chain_id} "
                f"({stats.no_reference_asset} without a reference asset, "
                f"{stats.market_price_unavailable} with market price unavailable)"
            )
        return stats

    async def _market_prices(
        self,
        resolved: list[tuple[ReferenceAsset | None, ReferenceAsset | None, float]],
    ) -> dict[tuple[str, float], float | None]:
        needed: dict[str, set[float]] = defaultdict(set)
        for asset_in, asset_out, ts in resolved:
            for asset in (asset_in, asset_out):
                if asset is not None and asset.kind is AssetKind.MARKET and asset.coingecko_id:
                    needed[asset.coingecko_id].add(ts)
        if not needed or self.market_source is None:
            return {}

        prices: dict[tuple[str, float], float | None] = {}
        for coin_id, timestamps in needed.items():
            try:
                found = await self.market_source.get_usd_prices(coin_id, timestamps)
            except Exception as exc:  # pricing must never fail ingestion
                logger.warning(f"Market price lookup for {coin_id} failed: {exc!r}")
                continue
            prices.update({(coin_id, ts): price for ts, price in found.items()})
        return prices


def _leg_usd(
    asset: ReferenceAsset | None,
    raw_amount: float,
    ts: float,
    market_prices: dict[tuple[str, float], float | None],
) -> float | None:
    if asset is None:
        return None
    if asset.kind is AssetKind.STABLECOIN:
        unit_price: float | None = 1.0
    else:
        unit_price = market_prices.get((asset.coingecko_id or "", ts))
    if unit_price is None:
        return None
    return raw_amount / 10.0**asset.decimals * unit_price


def _volume_usd(
    asset_in: ReferenceAsset | None,
    in_usd: float | None,
    asset_out: ReferenceAsset | None,
    out_usd: float | None,
) -> float | None:
    """Prefer a stablecoin leg (exact), then a market-priced leg; ties go to the input leg."""
    for kind in (AssetKind.STABLECOIN, AssetKind.MARKET):
        for asset, usd in ((asset_in, in_usd), (asset_out, out_usd)):
            if asset is not None and asset.kind is kind and usd is not None:
                return usd
    return None
