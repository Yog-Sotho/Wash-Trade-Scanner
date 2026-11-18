#!/usr/bin/env python3
"""
Delete stored trades older than a retention period.

Nothing deletes data automatically; this command does, and only when asked:
without --yes it reports how many trades would go.

    wash-prune                 # count trades older than TRADE_RETENTION_DAYS
    wash-prune --days 30 --yes # delete trades older than 30 days
"""

import argparse
import asyncio
import logging
import sys

from config.settings import settings
from core.log_config import configure_logging
from core.storage import Storage

logger = logging.getLogger(__name__)


async def prune(storage: Storage, days: int, apply: bool) -> int:
    """Count (or with `apply`, delete) trades older than `days` days."""
    cutoff = storage.retention_cutoff(days)
    if not apply:
        count = await storage.count_old_trades(days)
        print(f"{count} trades are older than {days} days (before {cutoff:%Y-%m-%d %H:%M} UTC).")
        if count:
            print("Run again with --yes to delete them.")
        return count
    deleted = await storage.cleanup_old_data(days)
    print(f"Deleted {deleted} trades older than {days} days (before {cutoff:%Y-%m-%d %H:%M} UTC).")
    return deleted


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Delete trades older than a retention period")
    parser.add_argument(
        "--days",
        type=int,
        default=settings.TRADE_RETENTION_DAYS,
        help="retention period in days (default: TRADE_RETENTION_DAYS)",
    )
    parser.add_argument("--yes", action="store_true", help="actually delete (default: count only)")
    args = parser.parse_args(argv)
    if args.days < 1:
        parser.error("--days must be at least 1")

    storage = Storage()
    await storage.initialize()
    try:
        await prune(storage, args.days, args.yes)
    finally:
        await storage.close()
    return 0


def cli() -> None:
    """Synchronous entry point for the `wash-prune` console script."""
    configure_logging()
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    cli()
