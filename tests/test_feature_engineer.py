"""
Unit tests for feature engineering, including the trade_id label-lookup fix.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.feature_engineer import FeatureEngineer
from models.schemas import SwapTrade


@pytest.fixture
def engineer():
    return FeatureEngineer(storage=AsyncMock())


def _session_returning(rows):
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = rows
    session.execute.return_value = result
    return session


@pytest.mark.asyncio
async def test_compute_trade_features_includes_trade_id(engineer):
    trade = SwapTrade(
        id=42,
        chain_id=1,
        pool_address="0xpool",
        sender="0xalice",
        recipient="0xbob",
        volume_usd=1000.0,
        amount_in_usd=1000.0,
        amount_out_usd=990.0,
        block_timestamp=datetime(2024, 1, 1, 12, 0, 0),
        gas_price=10.0,
    )
    session = _session_returning([])

    features = await engineer.compute_trade_features(trade, session)

    # This is the field ml_detector.train() relies on to match heuristic labels
    # back to specific trades - without it, sample weighting silently never fires.
    assert features["trade_id"] == 42
    assert features["volume_usd"] == 1000.0
    assert features["sender_trade_count_1h"] == 0
    assert features["slippage_ratio"] == pytest.approx(0.01, abs=1e-6)


@pytest.mark.asyncio
async def test_compute_pool_features_empty(engineer):
    session = _session_returning([])
    features = await engineer.compute_pool_features(1, "0xpool", session)
    assert features == {}


@pytest.mark.asyncio
async def test_build_ml_features_empty(engineer):
    session = _session_returning([])
    df = await engineer.build_ml_features(1, "0xpool", session)
    assert df.empty
