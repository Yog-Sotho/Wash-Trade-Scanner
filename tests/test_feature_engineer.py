
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from core.feature_engineer import FeatureEngineer
from models.schemas import SwapTrade

@pytest.fixture
def fe():
    return FeatureEngineer(MagicMock())

@pytest.fixture
def sample_trades():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trades = [
        SwapTrade(id=1, chain_id=1, pool_address="0xpool", sender="0xAlice", recipient="0xBob",
                  volume_usd=1000.0, amount_in_usd=1000.0, amount_out_usd=990.0,
                  block_timestamp=base_time, gas_price=10.0),
        SwapTrade(id=2, chain_id=1, pool_address="0xpool", sender="0xBob", recipient="0xAlice",
                  volume_usd=500.0, amount_in_usd=500.0, amount_out_usd=495.0,
                  block_timestamp=base_time + timedelta(minutes=10), gas_price=10.0),
        SwapTrade(id=3, chain_id=1, pool_address="0xpool", sender="0xAlice", recipient="0xBob",
                  volume_usd=200.0, amount_in_usd=200.0, amount_out_usd=198.0,
                  block_timestamp=base_time + timedelta(minutes=20), gas_price=10.0),
    ]
    return trades

@pytest.mark.asyncio
async def test_compute_pool_features(fe, sample_trades):
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = sample_trades
    mock_session.execute.return_value = mock_result

    features = await fe.compute_pool_features(1, "0xpool", mock_session)

    assert "circular_trade_ratio" in features
    # Trade 1 Alice -> Bob. Trade 2 Bob -> Alice is within 1h. (1 circular)
    # Trade 2 Bob -> Alice. Trade 3 Alice -> Bob is within 1h. (1 circular)
    # Total 2 circular trades.
    # Ratio = 2 / (3 + 1) = 0.5
    assert features["circular_trade_ratio"] == 0.5

@pytest.mark.asyncio
async def test_build_ml_features(fe, sample_trades):
    mock_session = AsyncMock()

    # 1. First call in build_ml_features to get trades
    mock_result_pool = MagicMock()
    mock_result_pool.scalars.return_value.all.return_value = sample_trades

    # 2. Second call in build_ml_features to get history
    mock_result_hist = MagicMock()
    mock_result_hist.scalars.return_value.all.return_value = sample_trades

    # 3. Call in compute_pool_features
    mock_result_pool_fe = MagicMock()
    mock_result_pool_fe.scalars.return_value.all.return_value = sample_trades

    mock_session.execute.side_effect = [mock_result_pool, mock_result_hist, mock_result_pool_fe]

    df = await fe.build_ml_features(1, "0xpool", mock_session)

    assert len(df) == 3
    assert "sender_trade_count_1h" in df.columns
    # Trade 3 Alice -> Bob. Trade 1 is 20 mins before.
    assert df.iloc[2]["sender_trade_count_1h"] == 1
    assert df.iloc[2]["reverse_pair_trade_count_1h"] == 1 # Trade 2 is Bob -> Alice
