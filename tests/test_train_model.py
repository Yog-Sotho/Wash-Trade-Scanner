"""
Unit tests for the train_model script.
"""

from unittest.mock import AsyncMock, patch

import pytest

from scripts.train_model import train_model


@pytest.mark.asyncio
async def test_train_model_saves_by_default():
    with (
        patch("scripts.train_model.Storage") as MockStorage,
        patch("scripts.train_model.FeatureEngineer"),
        patch("scripts.train_model.MLDetector") as MockMLDetector,
    ):
        MockStorage.return_value.initialize = AsyncMock()
        detector_instance = MockMLDetector.return_value
        detector_instance.train = AsyncMock()

        await train_model(chain_id=1, pool_addresses=["0x" + "a" * 40])

        detector_instance.train.assert_awaited_once()
        detector_instance.save_model.assert_called_once()


@pytest.mark.asyncio
async def test_train_model_skips_save_when_disabled():
    with (
        patch("scripts.train_model.Storage") as MockStorage,
        patch("scripts.train_model.FeatureEngineer"),
        patch("scripts.train_model.MLDetector") as MockMLDetector,
    ):
        MockStorage.return_value.initialize = AsyncMock()
        detector_instance = MockMLDetector.return_value
        detector_instance.train = AsyncMock()

        await train_model(chain_id=1, pool_addresses=["0x" + "a" * 40], save_model=False)

        detector_instance.train.assert_awaited_once()
        detector_instance.save_model.assert_not_called()
