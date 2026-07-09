"""
Tests for the Storage layer.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from core.storage import Storage


@pytest.fixture
def storage():
    return Storage("postgresql+asyncpg://user:pass@localhost/testdb")


def _mock_session(storage: Storage) -> AsyncMock:
    """Wire up storage.session_factory so `async with await storage.get_session()`
    yields a controllable, correctly-spec'd AsyncSession mock."""
    session = AsyncMock(spec=AsyncSession)
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    storage.session_factory = MagicMock(return_value=session_cm)
    return session


@pytest.mark.asyncio
async def test_initialize(storage):
    with (
        patch("core.storage.create_async_engine"),
        patch("core.storage.async_sessionmaker"),
    ):
        await storage.initialize()
        assert storage.engine is not None
        assert storage.session_factory is not None


@pytest.mark.asyncio
async def test_close(storage):
    engine_mock = AsyncMock()
    storage.engine = engine_mock
    await storage.close()
    engine_mock.dispose.assert_awaited_once()
    assert storage.engine is None


@pytest.mark.asyncio
async def test_save_trade_new(storage):
    # Storage.get_session() calls self.session_factory() *synchronously* (that's how
    # the real async_sessionmaker behaves - it returns an AsyncSession directly, not
    # a coroutine). execute()'s return value needs its own (synchronous) mock since
    # AsyncMock's un-spec'd descendants default to AsyncMock.
    session = _mock_session(storage)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute.return_value = mock_result
    await storage.save_trade(
        {
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
        }
    )
    session.add.assert_called_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_save_trade_updates_existing(storage):
    session = _mock_session(storage)
    existing = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = existing
    session.execute.return_value = mock_result

    await storage.save_trade(
        {
            "transaction_hash": "0xabc",
            "log_index": 0,
            "amount_in": 5.0,
        }
    )
    assert existing.amount_in == 5.0
    session.add.assert_not_called()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_save_trades_batch_empty(storage):
    assert await storage.save_trades_batch([]) == 0


@pytest.mark.asyncio
async def test_save_trades_batch_upsert(storage):
    session = _mock_session(storage)
    cursor_result = MagicMock()
    cursor_result.rowcount = 2
    session.execute.return_value = cursor_result

    count = await storage.save_trades_batch(
        [
            {"transaction_hash": "0x1", "log_index": 0},
            {"transaction_hash": "0x2", "log_index": 0},
        ]
    )
    assert count == 2
    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_trade_labels_empty(storage):
    assert await storage.update_trade_labels([], True, 0.9, "ml") == 0


@pytest.mark.asyncio
async def test_update_trade_labels(storage):
    session = _mock_session(storage)
    cursor_result = MagicMock()
    cursor_result.rowcount = 3
    session.execute.return_value = cursor_result

    count = await storage.update_trade_labels([1, 2, 3], True, 0.9, "ml")
    assert count == 3
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_cleanup_old_data(storage):
    session = _mock_session(storage)
    cursor_result = MagicMock()
    cursor_result.rowcount = 5
    session.execute.return_value = cursor_result

    count = await storage.cleanup_old_data(retention_days=90)
    assert count == 5
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_pool_trades(storage):
    session = _mock_session(storage)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = ["trade1", "trade2"]
    session.execute.return_value = mock_result

    trades = await storage.get_pool_trades(chain_id=1, pool_address="0xpool")
    assert trades == ["trade1", "trade2"]


@pytest.mark.asyncio
async def test_health_check_no_engine(storage):
    assert await storage.health_check() is False


@pytest.mark.asyncio
async def test_health_check_success(storage):
    conn = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar.return_value = "now"
    conn.execute.return_value = mock_result
    conn_cm = AsyncMock()
    conn_cm.__aenter__.return_value = conn
    storage.engine = MagicMock()
    storage.engine.connect = MagicMock(return_value=conn_cm)

    assert await storage.health_check() is True


@pytest.mark.asyncio
async def test_health_check_failure(storage):
    storage.engine = MagicMock()
    storage.engine.connect = MagicMock(side_effect=RuntimeError("db down"))

    assert await storage.health_check() is False


@pytest.mark.asyncio
async def test_get_session_without_initialize_raises(storage):
    with pytest.raises(RuntimeError):
        await storage.get_session()


@pytest.mark.asyncio
async def test_update_token_risk_profile_creates_new(storage):
    session = _mock_session(storage)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute.return_value = mock_result

    profile = await storage.update_token_risk_profile(
        chain_id=1,
        pool_address="0xpool",
        token_address="0xtoken",
        risk_metrics={"overall_risk_score": 0.5},
    )
    assert profile.chain_id == 1
    session.add.assert_called_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_token_risk_profile_updates_existing(storage):
    session = _mock_session(storage)
    existing = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = existing
    session.execute.return_value = mock_result

    await storage.update_token_risk_profile(
        chain_id=1,
        pool_address="0xpool",
        token_address="0xtoken",
        risk_metrics={"overall_risk_score": 0.9},
    )
    assert existing.overall_risk_score == 0.9
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_create_audit_log(storage):
    session = _mock_session(storage)

    log = await storage.create_audit_log(
        chain_id=1,
        pool_address="0xpool",
        detection_type="combined",
        start_block=0,
        end_block=100,
        trades_processed=10,
        wash_trades_detected=2,
        detection_duration_seconds=1.5,
        parameters_used={"use_ml": True},
        results_summary={"risk": 0.2},
    )
    assert log.chain_id == 1
    session.add.assert_called_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_address_clusters(storage):
    session = _mock_session(storage)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = ["cluster1"]
    session.execute.return_value = mock_result

    clusters = await storage.get_address_clusters(chain_id=1)
    assert clusters == ["cluster1"]
