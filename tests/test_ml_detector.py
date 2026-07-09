"""
Unit tests for the ML wash-trade detector.
"""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from core.exceptions import InsufficientDataError, ModelNotTrainedError
from core.feature_engineer import FeatureEngineer
from core.ml_detector import MLDetector
from core.storage import Storage
from models.schemas import SwapTrade


def _features_df(n: int, with_trade_id: bool = True) -> pd.DataFrame:
    data = {
        "volume_usd": [100.0 + i for i in range(n)],
        "slippage_ratio": [0.01] * n,
        "sender_trade_count_1h": [1] * n,
        "gas_price": [10.0] * n,
    }
    if with_trade_id:
        data["trade_id"] = list(range(1, n + 1))
    return pd.DataFrame(data)


def _session_returning_trades(trades):
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = trades
    session.execute.return_value = result
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    return session_cm


@pytest.fixture
def detector():
    storage = AsyncMock(spec=Storage)
    feature_engineer = AsyncMock(spec=FeatureEngineer)
    return MLDetector(storage, feature_engineer)


@pytest.mark.asyncio
async def test_train_raises_without_data(detector):
    detector.feature_engineer.build_ml_features.return_value = pd.DataFrame()
    detector.storage.get_session.return_value = _session_returning_trades([])

    with pytest.raises(InsufficientDataError):
        await detector.train(chain_id=1, pool_addresses=["0xpool"], use_heuristic_labels=False)


@pytest.mark.asyncio
async def test_train_and_predict_round_trip(detector):
    df = _features_df(30)
    detector.feature_engineer.build_ml_features.return_value = df
    detector.storage.get_session.return_value = _session_returning_trades([])

    await detector.train(chain_id=1, pool_addresses=["0xpool"], use_heuristic_labels=False)

    assert detector.is_trained
    probs = await detector.predict(df)
    assert len(probs) == len(df)
    assert all(0.0 <= p <= 1.0 for p in probs)


@pytest.mark.asyncio
async def test_train_uses_heuristic_labels(detector):
    df = _features_df(20)
    detector.feature_engineer.build_ml_features.return_value = df
    trades = [
        SwapTrade(id=i, is_wash_trade=(i == 1), chain_id=1, pool_address="0xpool")
        for i in range(1, 21)
    ]
    detector.storage.get_session.return_value = _session_returning_trades(trades)

    await detector.train(chain_id=1, pool_addresses=["0xpool"], use_heuristic_labels=True)
    assert detector.is_trained


@pytest.mark.asyncio
async def test_predict_before_train_raises(detector):
    with pytest.raises(ModelNotTrainedError):
        await detector.predict(_features_df(5))


@pytest.mark.asyncio
async def test_explain_prediction(detector):
    df = _features_df(15)
    detector.feature_engineer.build_ml_features.return_value = df
    detector.storage.get_session.return_value = _session_returning_trades([])
    await detector.train(chain_id=1, pool_addresses=["0xpool"], use_heuristic_labels=False)

    importance = await detector.explain_prediction(df, idx=0)
    assert importance
    assert pytest.approx(sum(importance.values()), abs=1e-6) == 1.0


@pytest.mark.asyncio
async def test_detect_wash_trades_empty_features(detector):
    detector.feature_engineer.build_ml_features.return_value = pd.DataFrame()
    detector.storage.get_session.return_value = _session_returning_trades([])

    result = await detector.detect_wash_trades(chain_id=1, pool_address="0xpool")
    assert result == []


@pytest.mark.asyncio
async def test_detect_wash_trades_flags_high_probability(detector):
    df = _features_df(10)
    detector.feature_engineer.build_ml_features.return_value = df
    detector.storage.get_session.return_value = _session_returning_trades([])
    await detector.train(chain_id=1, pool_addresses=["0xpool"], use_heuristic_labels=False)

    trades = [SwapTrade(id=i, chain_id=1, pool_address="0xpool") for i in range(1, 11)]
    detector.storage.get_session.return_value = _session_returning_trades(trades)

    # threshold=0.0 guarantees every trade is flagged regardless of score
    result = await detector.detect_wash_trades(chain_id=1, pool_address="0xpool", threshold=0.0)
    assert len(result) == 10
    assert all(t.is_wash_trade for t in result)
    assert all(t.detection_method == "ml_isolation_forest" for t in result)


def test_save_model_without_training_raises(detector):
    with pytest.raises(ModelNotTrainedError):
        detector.save_model()


def test_load_model_missing_file_raises(detector):
    with pytest.raises(FileNotFoundError):
        detector.load_model("/nonexistent/path/model.pkl")


@pytest.mark.asyncio
async def test_save_and_load_model_round_trip(detector, tmp_path):
    df = _features_df(20)
    detector.feature_engineer.build_ml_features.return_value = df
    detector.storage.get_session.return_value = _session_returning_trades([])
    await detector.train(chain_id=1, pool_addresses=["0xpool"], use_heuristic_labels=False)

    model_path = str(tmp_path / "model.pkl")
    detector.save_model(model_path)

    storage = AsyncMock(spec=Storage)
    feature_engineer = AsyncMock(spec=FeatureEngineer)
    fresh_detector = MLDetector(storage, feature_engineer)
    fresh_detector.load_model(model_path)

    assert fresh_detector.is_trained
    assert fresh_detector.feature_columns == detector.feature_columns
