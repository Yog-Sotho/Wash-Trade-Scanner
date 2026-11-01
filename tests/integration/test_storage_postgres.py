"""
Storage against a real PostgreSQL: schema creation, upserts, bulk updates and
the aggregate queries behind the dashboard. Unit tests mock the session, so
none of this SQL had ever been executed before these tests existed.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from core.storage import Storage

pytestmark = pytest.mark.integration

POOL = "0x" + "a" * 40


def _trade(log_index: int, **overrides):
    trade = {
        "chain_id": 1,
        "dex_name": "UniswapV2",
        "pool_address": POOL,
        "token_in": "0x" + "1" * 40,
        "token_out": "0x" + "2" * 40,
        "amount_in": 1.0,
        "amount_out": 2.0,
        "sender": "0x" + "3" * 40,
        "recipient": "0x" + "4" * 40,
        "transaction_hash": "0x" + f"{log_index:064x}",
        "block_number": 100 + log_index,
        "block_timestamp": datetime(2026, 1, 15, 12, 0) + timedelta(minutes=log_index),
        "log_index": log_index,
        "gas_price": None,
        "gas_used": None,
        "amount_in_usd": None,
        "amount_out_usd": None,
        "volume_usd": None,
    }
    trade.update(overrides)
    return trade


async def test_initialize_is_idempotent(pg_storage: Storage):
    await pg_storage.initialize()  # second create_all must not fail
    assert await pg_storage.health_check() is True


async def test_save_trades_batch_is_idempotent(pg_storage: Storage):
    batch = [_trade(0), _trade(1)]
    assert await pg_storage.save_trades_batch(batch) == 2
    # Re-syncing an overlapping range leaves stored trades alone.
    assert await pg_storage.save_trades_batch(batch) == 0
    assert len(await pg_storage.get_pool_trades(1, POOL)) == 2


async def test_get_pool_trades_newest_first_with_pagination(pg_storage: Storage):
    await pg_storage.save_trades_batch([_trade(i) for i in range(5)])

    page = await pg_storage.get_pool_trades(1, POOL, limit=2, offset=1)

    assert [t.log_index for t in page] == [3, 2]


async def test_unpriced_trades_and_bulk_price_update(pg_storage: Storage):
    await pg_storage.save_trades_batch([_trade(0), _trade(1, volume_usd=5.0), _trade(2), _trade(3)])
    unpriced = await pg_storage.get_unpriced_trades(1, after_id=0, limit=10)
    assert [t.log_index for t in unpriced] == [0, 2, 3]

    updated = await pg_storage.update_trade_prices(
        [
            {"id": unpriced[0].id, "amount_in_usd": 7.0, "amount_out_usd": None, "volume_usd": 7.0},
            {"id": unpriced[1].id, "amount_in_usd": 1.5, "amount_out_usd": 1.4, "volume_usd": 1.5},
        ]
    )

    assert updated == 2
    by_index = {t.log_index: t for t in await pg_storage.get_pool_trades(1, POOL)}
    assert (by_index[0].amount_in_usd, by_index[0].volume_usd) == (7.0, 7.0)
    assert (by_index[2].amount_out_usd, by_index[2].volume_usd) == (1.4, 1.5)
    assert by_index[3].volume_usd is None  # untouched
    remaining = await pg_storage.get_unpriced_trades(1, after_id=0, limit=10)
    assert [t.log_index for t in remaining] == [3]


async def test_update_trade_labels(pg_storage: Storage):
    await pg_storage.save_trades_batch([_trade(0), _trade(1)])
    ids = [t.id for t in await pg_storage.get_pool_trades(1, POOL)]

    assert await pg_storage.update_trade_labels(ids[:1], True, 0.95, "self_trading") == 1


async def test_dashboard_aggregates_on_empty_database(pg_storage: Storage):
    stats = await pg_storage.get_global_stats()

    assert stats["total_trades"] == 0
    assert stats["total_volume_usd"] == 0.0
    assert stats["wash_volume_usd"] == 0.0
    assert stats["by_method"] == {}
    assert await pg_storage.get_top_wash_pools() == []


async def test_dashboard_aggregates(pg_storage: Storage):
    other_pool = "0x" + "b" * 40
    await pg_storage.save_trades_batch(
        [
            _trade(0, volume_usd=100.0),
            _trade(1, volume_usd=300.0),
            _trade(2, volume_usd=None),
            _trade(3, pool_address=other_pool, volume_usd=50.0),
        ]
    )
    by_index = {t.log_index: t for t in await pg_storage.get_pool_trades(1, POOL)}
    await pg_storage.update_trade_labels([by_index[1].id], True, 1.0, "self_trading")

    stats = await pg_storage.get_global_stats()
    top = await pg_storage.get_top_wash_pools()

    assert stats["total_trades"] == 4
    assert stats["total_volume_usd"] == 450.0
    assert stats["pools_tracked"] == 2
    assert stats["wash_trades"] == 1
    assert stats["wash_volume_usd"] == 300.0
    assert stats["by_method"] == {"self_trading": {"trades": 1, "volume_usd": 300.0}}
    assert stats["by_chain"][1]["wash_volume_usd"] == 300.0
    assert top == [
        {
            "chain_id": 1,
            "pool_address": POOL,
            "trades": 3,
            "volume_usd": 400.0,
            "wash_trades": 1,
            "wash_volume_usd": 300.0,
        }
    ]


async def test_audit_log_roundtrip(pg_storage: Storage):
    await pg_storage.create_audit_log(
        chain_id=1,
        pool_address=POOL,
        detection_type="combined",
        start_block=1,
        end_block=2,
        trades_processed=3,
        wash_trades_detected=1,
        detection_duration_seconds=0.5,
        parameters_used={"use_ml": False},
        results_summary={"severity": "HIGH"},
    )

    logs = await pg_storage.get_recent_audit_logs()

    assert len(logs) == 1
    assert logs[0].results_summary == {"severity": "HIGH"}
    assert logs[0].created_at is not None


async def test_cleanup_old_data(pg_storage: Storage):
    old = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=400)
    recent = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=1)
    await pg_storage.save_trades_batch(
        [_trade(0, block_timestamp=old), _trade(1, block_timestamp=recent)]
    )

    assert await pg_storage.cleanup_old_data(retention_days=90) == 1
    assert [t.log_index for t in await pg_storage.get_pool_trades(1, POOL)] == [1]


async def test_backfill_prices_unpriced_rows(pg_storage: Storage):
    from core.pricing import PriceOracle
    from scripts.backfill_prices import backfill_prices

    usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # Ethereum USDC, 6 decimals
    await pg_storage.save_trades_batch(
        [
            _trade(0, token_in=usdc, amount_in=25 * 10**6),
            _trade(1),  # no reference asset: stays unpriced
        ]
    )

    examined, priced = await backfill_prices(pg_storage, PriceOracle(None), chain_id=1)

    assert (examined, priced) == (2, 1)
    by_index = {t.log_index: t for t in await pg_storage.get_pool_trades(1, POOL)}
    assert by_index[0].volume_usd == 25.0
    assert by_index[1].volume_usd is None
    # Re-running is safe: the unpriceable row is examined again, nothing changes.
    assert await backfill_prices(pg_storage, PriceOracle(None), chain_id=1) == (1, 0)


async def test_resync_completes_a_trade_stored_without_its_sender(pg_storage: Storage):
    """A row synced before tx_from existed (or whose tx fetch failed) gains it on
    re-sync, but a stored sender is never overwritten and labels are untouched."""
    sender = "0x" + "e" * 40
    await pg_storage.save_trades_batch([_trade(0, tx_from=None, tx_to=None)])
    await pg_storage.save_trades_batch([_trade(1, tx_from=sender, tx_to=None)])
    stored = {t.log_index: t for t in await pg_storage.get_pool_trades(1, POOL)}
    await pg_storage.update_trade_labels(
        [stored[0].id], is_wash_trade=True, wash_trade_score=0.9, detection_method="x"
    )

    healed = await pg_storage.save_trades_batch(
        [
            _trade(0, tx_from=sender, tx_to="0x" + "f" * 40, gas_price=3.5),
            _trade(1, tx_from="0x" + "9" * 40, tx_to=None),
        ]
    )

    assert healed == 1  # only the row that lacked a sender
    stored = {t.log_index: t for t in await pg_storage.get_pool_trades(1, POOL)}
    assert stored[0].tx_from == sender
    assert stored[0].tx_to == "0x" + "f" * 40
    assert stored[0].gas_price == 3.5
    assert stored[0].is_wash_trade is True
    assert stored[1].tx_from == sender


async def test_initialize_adds_columns_missing_from_an_existing_table(pg_storage: Storage):
    """create_all() skips existing tables; a database created before tx_from
    existed must gain the column (and its index) instead of failing every query."""
    assert pg_storage.engine is not None
    await pg_storage.save_trades_batch([_trade(0)])
    async with pg_storage.engine.begin() as conn:
        await conn.execute(text("DROP INDEX ix_swap_trades_tx_from"))
        await conn.execute(text("ALTER TABLE swap_trades DROP COLUMN tx_from"))
        await conn.execute(text("ALTER TABLE swap_trades DROP COLUMN tx_to"))

    await pg_storage.initialize()

    async with pg_storage.engine.connect() as conn:
        columns = {
            row[0]
            for row in await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'swap_trades'"
                )
            )
        }
        indexes = {
            row[0]
            for row in await conn.execute(
                text("SELECT indexname FROM pg_indexes WHERE tablename = 'swap_trades'")
            )
        }
    assert {"tx_from", "tx_to"} <= columns
    assert "ix_swap_trades_tx_from" in indexes
    trades = await pg_storage.get_pool_trades(1, POOL)
    assert len(trades) == 1 and trades[0].tx_from is None
