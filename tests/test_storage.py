"""
Tests for the Storage layer.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from core.storage import Storage
from models.schemas import SwapTrade

@pytest.fixture
def storage():
    return Storage("postgresql+asyncpg://user:pass@localhost/testdb")

@pytest.mark.asyncio
async def test_initialize(storage):
    with patch("core.storage.create_async_engine") as mock_engine, \
         patch("core.storage.async_sessionmaker") as mock_sessionmaker:
        await storage.initialize()
        assert storage.engine is not None
        assert storage.session_factory is not None

@pytest.mark.asyncio
async def test_close(storage):
    mock_engine = AsyncMock()
    storage.engine = mock_engine
    await storage.close()
    mock_engine.dispose.assert_awaited_once()
    assert storage.engine is None

@pytest.mark.asyncio
async def test_save_trade_new(storage):
    mock_session = AsyncMock()
    mock_cm = AsyncMock()
    mock_cm.__aenter__.return_value = mock_session
    storage.session_factory = MagicMock(return_value=mock_cm)

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_result

    await storage.save_trade({
        "transaction_hash": "0xabc",
        "log_index": 0,
        "chain_id": 1,
        "dex_name": "TestDEX",
        "pool_address": "0xpool",
        "token_in": "0xtoken0",
        "token_out": "0xtoken1",
        "amount_in": 1.0,
        "amount_out": 2.0,
        "sender": "0xalice",
        "recipient": "0xbob",
        "block_number": 100,
        "block_timestamp": None,
    })
    mock_session.add.assert_called_once()
    mock_session.commit.assert_awaited_once()

@pytest.mark.asyncio
async def test_cleanup_old_data(storage):
    mock_session = AsyncMock()
    mock_cm = AsyncMock()
    mock_cm.__aenter__.return_value = mock_session
    storage.session_factory = MagicMock(return_value=mock_cm)

    mock_result = MagicMock()
    mock_result.rowcount = 5
    mock_session.execute.return_value = mock_result

    count = await storage.cleanup_old_data(30)

    assert count == 5
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_awaited_once()
