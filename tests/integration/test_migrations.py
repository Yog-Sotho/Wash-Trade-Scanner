"""
Schema migrations against a real PostgreSQL.

The schema is created by Alembic migrations (Storage.initialize, or the
`alembic upgrade head` CLI), not by create_all. These tests pin the contract:
the migrations build exactly the schema the models describe, from empty, and
can be reversed.
"""

import asyncio
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text

from core.storage import Storage, alembic_config
from models.schemas import Base
from tests.integration.conftest import drop_everything

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]


def _head() -> str:
    head = ScriptDirectory.from_config(alembic_config()).get_current_head()
    assert head is not None
    return head


def _model_diff(connection) -> list:
    context = MigrationContext.configure(connection, opts={"compare_type": True})
    return compare_metadata(context, Base.metadata)


async def test_migrations_build_exactly_the_models_schema(pg_storage: Storage):
    """If a model changes without a migration (or vice versa), this fails:
    the database an upgrade produces must match models/schemas.py."""
    assert pg_storage.engine is not None
    async with pg_storage.engine.connect() as conn:
        diff = await conn.run_sync(_model_diff)
        version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar()
    assert diff == []
    assert version == _head()


async def test_migrations_downgrade_to_empty_and_upgrade_again(pg_storage: Storage):
    assert pg_storage.engine is not None

    def downgrade(connection) -> list[str]:
        command.downgrade(alembic_config(connection), "base")
        return sorted(inspect(connection).get_table_names())

    def upgrade(connection) -> list:
        command.upgrade(alembic_config(connection), "head")
        return _model_diff(connection)

    async with pg_storage.engine.begin() as conn:
        assert await conn.run_sync(downgrade) == ["alembic_version"]
    async with pg_storage.engine.begin() as conn:
        assert await conn.run_sync(upgrade) == []


async def test_initialize_on_a_migrated_database_is_a_no_op(pg_storage: Storage):
    await pg_storage.save_trades_batch(
        [
            {
                "chain_id": 1,
                "dex_name": "UniswapV2",
                "pool_address": "0x" + "a" * 40,
                "token_in": "0x" + "1" * 40,
                "token_out": "0x" + "2" * 40,
                "amount_in": 1.0,
                "amount_out": 2.0,
                "sender": "0x" + "3" * 40,
                "recipient": "0x" + "4" * 40,
                "transaction_hash": "0x" + "0" * 64,
                "block_number": 1,
                "block_timestamp": datetime(2026, 1, 1),
                "log_index": 0,
            }
        ]
    )
    await pg_storage.initialize()
    assert len(await pg_storage.get_pool_trades(1, "0x" + "a" * 40)) == 1


async def test_alembic_cli_upgrades_an_empty_database():
    """The documented command, run as an operator would: `alembic upgrade head`."""
    await drop_everything()
    env = {k: v for k, v in os.environ.items() if k.startswith(("DATABASE_", "PATH", "HOME"))}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr

    storage = Storage()
    await storage.initialize()  # finds the schema at head and leaves it
    try:
        assert storage.engine is not None
        async with storage.engine.connect() as conn:
            assert await conn.run_sync(_model_diff) == []
            version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar()
        assert version == _head()
    finally:
        await storage.close()


async def test_instances_starting_together_migrate_one_at_a_time():
    """Regression: two API instances starting on an empty database both
    created alembic_version, and one failed to start."""
    await drop_everything()
    storages = [Storage() for _ in range(4)]
    try:
        # Bounded: without the migration lock the race can also deadlock.
        await asyncio.wait_for(asyncio.gather(*(s.initialize() for s in storages)), 60)
        assert storages[0].engine is not None
        async with storages[0].engine.connect() as conn:
            version = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar()
        assert version == _head()
    finally:
        for storage in storages:
            await storage.close()
