from unittest.mock import AsyncMock, MagicMock

import pytest

from core.exceptions import WashTradeError
from core.validators import TrainingParameters
from scripts.train_model import main, train_model


@pytest.mark.asyncio
async def test_train_model_success(mocker):
    # Mock dependencies
    mock_storage = mocker.patch("scripts.train_model.Storage", autospec=True)
    mock_storage_inst = mock_storage.return_value
    mock_storage_inst.initialize = AsyncMock()
    mock_storage_inst.close = AsyncMock()

    mock_fe = mocker.patch("scripts.train_model.FeatureEngineer", autospec=True)
    mock_ml = mocker.patch("scripts.train_model.MLDetector", autospec=True)
    mock_ml_inst = mock_ml.return_value
    mock_ml_inst.train = AsyncMock()
    mock_ml_inst.save_model = MagicMock()

    params = TrainingParameters(
        chain_id=1,
        pool_addresses=["0x0000000000000000000000000000000000000000"],
        use_heuristic_labels=True,
        contamination=0.1,
    )

    await train_model(params, save_model=True)

    mock_storage_inst.initialize.assert_called_once()
    mock_ml_inst.train.assert_called_once_with(
        chain_id=1,
        pool_addresses=["0x0000000000000000000000000000000000000000"],
        use_heuristic_labels=True,
        contamination=0.1,
    )
    mock_ml_inst.save_model.assert_called_once()
    mock_storage_inst.close.assert_called_once()


@pytest.mark.asyncio
async def test_main_invalid_params(mocker):
    # Mock sys.argv
    mocker.patch(
        "sys.argv", ["train_model.py", "--chain-id", "0", "--pools", "invalid"]
    )

    # Capture exit code
    exit_code = await main()
    assert exit_code == 1


@pytest.mark.asyncio
async def test_main_unexpected_error(mocker):
    # Mock TrainingParameters to return valid params
    mocker.patch("scripts.train_model.TrainingParameters")
    # Mock train_model to raise an unexpected error
    mocker.patch("scripts.train_model.train_model", side_effect=Exception("Unexpected"))
    mocker.patch(
        "sys.argv",
        [
            "train_model.py",
            "--chain-id",
            "1",
            "--pools",
            "0x0000000000000000000000000000000000000000",
        ],
    )

    exit_code = await main()
    assert exit_code == 1


@pytest.mark.asyncio
async def test_main_application_error(mocker):
    mocker.patch("scripts.train_model.TrainingParameters")
    mocker.patch(
        "scripts.train_model.train_model",
        side_effect=WashTradeError("Application error"),
    )
    mocker.patch(
        "sys.argv",
        [
            "train_model.py",
            "--chain-id",
            "1",
            "--pools",
            "0x0000000000000000000000000000000000000000",
        ],
    )

    exit_code = await main()
    assert exit_code == 1
