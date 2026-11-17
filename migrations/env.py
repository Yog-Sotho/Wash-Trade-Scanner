"""
Alembic environment.

Two ways in:
- Storage.initialize() runs migrations on its own engine and passes the open
  (sync-adapted) connection in ``config.attributes["connection"]``.
- The ``alembic`` CLI (``alembic upgrade head`` with alembic.ini at the repo
  root) builds an async engine from DATABASE_URL in the settings.
"""

import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from models.schemas import Base

config = context.config
target_metadata = Base.metadata


def _run(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def _run_with_new_engine() -> None:
    from config.settings import settings

    engine = create_async_engine(settings.DATABASE_URL)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_run)
            await connection.commit()
    finally:
        await engine.dispose()


def run_migrations_offline() -> None:
    from config.settings import settings

    context.configure(
        url=settings.DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    connection = config.attributes.get("connection")
    if connection is not None:
        _run(connection)
    else:
        asyncio.run(_run_with_new_engine())
