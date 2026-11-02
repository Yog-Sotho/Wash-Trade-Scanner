"""
Record a pool's real swap history as an evaluation fixture.

The fixture is produced by the production ingestor (the same log decoding,
transaction-sender lookup and USD valuation an audit uses), so the harness
measures detectors on exactly the data they see in production. Only the
storage is replaced, by an in-memory capture.

ETH is valued with Coinbase Exchange's public hourly ETH-USD candles rather
than CoinGecko: CoinGecko's keyless history is limited to the last 365 days,
and the documented wash-trading market in the benchmark is older than that.
Stablecoin legs are valued at $1.00 by the production registry either way.

Usage:
    python -m evaluation.record --chain-id 1 --pool 0x... \\
        --from-block N --to-block M --rpc-url https://... \\
        --kind clean --out evaluation/fixtures/name.json.gz
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

import aiohttp

from config.chains import get_chain_config
from core.ingestor import ChainIngestor
from core.pricing import PriceOracle

logger = logging.getLogger(__name__)

_COINBASE_CANDLES = "https://api.exchange.coinbase.com/products/{product}/candles"
_PRODUCTS = {"ethereum": "ETH-USD", "weth": "ETH-USD"}
_HOUR = 3600
_MAX_CANDLES = 300  # Coinbase Exchange's per-request limit


class CoinbaseHourlyPriceSource:
    """USD prices from Coinbase Exchange hourly candles (public, keyless).

    A timestamp is valued at the open of the hourly candle containing it, the
    last traded price at or before the start of that hour.
    """

    def __init__(self, base_url: str = _COINBASE_CANDLES, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._opens: dict[tuple[str, int], float] = {}

    async def get_usd_prices(
        self, coin_id: str, timestamps: Iterable[float]
    ) -> dict[float, float | None]:
        stamps = list(timestamps)
        product = _PRODUCTS.get(coin_id)
        if product is None or not stamps:
            return dict.fromkeys(stamps)
        hours = sorted({int(ts) - int(ts) % _HOUR for ts in stamps})
        missing = [h for h in hours if (product, h) not in self._opens]
        if missing:
            await self._fetch(product, missing[0], missing[-1])
        return {ts: self._opens.get((product, int(ts) - int(ts) % _HOUR)) for ts in stamps}

    async def _fetch(self, product: str, first_hour: int, last_hour: int) -> None:
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            start = first_hour
            while start <= last_hour:
                end = min(start + (_MAX_CANDLES - 1) * _HOUR, last_hour)
                params = {
                    "granularity": str(_HOUR),
                    "start": datetime.fromtimestamp(start, tz=UTC).isoformat(),
                    "end": datetime.fromtimestamp(end + _HOUR, tz=UTC).isoformat(),
                }
                url = self.base_url.format(product=product)
                async with session.get(url, params=params) as response:
                    response.raise_for_status()
                    candles = await response.json()
                # [time, low, high, open, close, volume], newest first
                for candle in candles:
                    self._opens[(product, int(candle[0]))] = float(candle[3])
                start = end + _HOUR


class CaptureStorage:
    """Stands in for Storage: keeps the batches the ingestor would save."""

    def __init__(self) -> None:
        self.trades: list[dict[str, Any]] = []

    async def save_trades_batch(self, trades_data: list[dict[str, Any]]) -> int:
        self.trades.extend(trades_data)
        return len(trades_data)


async def record_pool(
    chain_id: int,
    pool_address: str,
    from_block: int,
    to_block: int,
    rpc_url: str,
) -> list[dict[str, Any]]:
    """Ingest a pool's swaps through the production ingestor, in memory."""
    chain_config = get_chain_config(chain_id)
    chain_config["rpc_url"] = rpc_url
    capture = CaptureStorage()
    ingestor = ChainIngestor(
        chain_config,
        capture,  # type: ignore[arg-type]
        price_oracle=PriceOracle(CoinbaseHourlyPriceSource()),
    )
    await ingestor.connect()
    await ingestor.sync_pool(pool_address, from_block, to_block)
    capture.trades.sort(key=lambda t: (t["block_number"], t["log_index"]))
    return capture.trades


def write_fixture(path: str, meta: dict[str, Any], trades: list[dict[str, Any]]) -> None:
    rows = [
        {
            **trade,
            "block_timestamp": trade["block_timestamp"].isoformat(),
        }
        for trade in trades
    ]
    payload = json.dumps({"meta": meta, "trades": rows}, sort_keys=True, separators=(",", ":"))
    # mtime=0 keeps the archive byte-identical across re-recordings; writing to
    # a temporary file and renaming means a reader never sees a partial fixture.
    tmp = f"{path}.tmp"
    with gzip.GzipFile(tmp, "wb", mtime=0) as fh:
        fh.write(payload.encode())
    os.replace(tmp, path)


async def main() -> int:
    parser = argparse.ArgumentParser(description="Record a pool as an evaluation fixture")
    parser.add_argument("--chain-id", type=int, required=True)
    parser.add_argument("--pool", required=True)
    parser.add_argument("--from-block", type=int, required=True)
    parser.add_argument("--to-block", type=int, required=True)
    parser.add_argument("--rpc-url", required=True, help="archive-capable RPC endpoint")
    parser.add_argument("--kind", choices=["clean", "documented_wash"], required=True)
    parser.add_argument("--name", required=True, help="short fixture name")
    parser.add_argument("--description", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    trades = await record_pool(
        args.chain_id, args.pool.lower(), args.from_block, args.to_block, args.rpc_url
    )
    meta = {
        "name": args.name,
        "kind": args.kind,
        "description": args.description,
        "chain_id": args.chain_id,
        "pool_address": args.pool.lower(),
        "from_block": args.from_block,
        "to_block": args.to_block,
        "eth_price_source": "Coinbase Exchange ETH-USD hourly candle open",
        "trades": len(trades),
        "with_tx_from": sum(1 for t in trades if t.get("tx_from")),
        "priced": sum(1 for t in trades if t.get("volume_usd") is not None),
    }
    write_fixture(args.out, meta, trades)
    logger.info(f"Wrote {len(trades)} trades to {args.out}: {meta}")
    return 0 if trades else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(asyncio.run(main()))
