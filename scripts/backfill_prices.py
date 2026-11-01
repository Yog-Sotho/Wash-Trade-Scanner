#!/usr/bin/env python3
"""
Value stored trades that have no USD volume yet.

New swaps are priced at ingestion. This command prices rows stored before
pricing existed, or rows whose market price was unavailable when they were
ingested (e.g. CoinGecko throttling). Trades that still cannot be priced are
left untouched, so it is safe to re-run.
"""

import argparse
import asyncio
import logging
import sys

from pydantic import ValidationError as PydanticValidationError

from config.settings import settings
from core.pricing import PriceOracle
from core.storage import Storage
from core.validators import validate_address

logger = logging.getLogger(__name__)


async def backfill_prices(
    storage: Storage,
    oracle: PriceOracle,
    chain_id: int,
    pool_address: str | None = None,
    batch_size: int = 1000,
) -> tuple[int, int]:
    """Price unpriced trades in id order. Returns (examined, priced)."""
    examined = priced = 0
    after_id = 0
    while True:
        rows = await storage.get_unpriced_trades(chain_id, after_id, batch_size, pool_address)
        if not rows:
            break
        after_id = rows[-1].id
        trades = [
            {
                "id": row.id,
                "token_in": row.token_in,
                "amount_in": row.amount_in,
                "token_out": row.token_out,
                "amount_out": row.amount_out,
                "block_timestamp": row.block_timestamp,
            }
            for row in rows
        ]
        await oracle.price_trades(chain_id, trades)
        updates = [
            {
                "id": trade["id"],
                "amount_in_usd": trade["amount_in_usd"],
                "amount_out_usd": trade["amount_out_usd"],
                "volume_usd": trade["volume_usd"],
            }
            for trade in trades
            if trade["volume_usd"] is not None
        ]
        priced += await storage.update_trade_prices(updates)
        examined += len(rows)
    logger.info(f"Backfill on chain {chain_id}: priced {priced} of {examined} unpriced trades")
    return examined, priced


async def main() -> int:
    parser = argparse.ArgumentParser(description="Price stored trades that lack USD values")
    parser.add_argument("--chain-id", type=int, required=True, help="Blockchain chain ID")
    parser.add_argument("--pool", type=str, help="Only this pool address")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per batch")
    args = parser.parse_args()

    pool = None
    if args.pool:
        try:
            validate_address(args.pool)
        except (ValueError, PydanticValidationError) as exc:
            logger.error(f"Invalid pool address: {exc}")
            return 1
        pool = args.pool.lower()
    if args.batch_size < 1:
        logger.error("--batch-size must be at least 1")
        return 1

    storage = Storage()
    await storage.initialize()
    try:
        examined, priced = await backfill_prices(
            storage, PriceOracle.from_settings(), args.chain_id, pool, args.batch_size
        )
    finally:
        await storage.close()
    print(f"Priced {priced} of {examined} unpriced trades on chain {args.chain_id}")
    return 0


def cli() -> None:
    """Synchronous entry point for the `wash-reprice` console script."""
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    cli()
