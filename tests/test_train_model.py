"""
Unit tests for the train_model script: models are measured before they are saved.
"""

from unittest.mock import AsyncMock, patch

import pytest

from scripts.train_model import train_model

GOOD = {"threshold": 0.6, "tpr": 0.4, "fpr_upper": 0.001, "pr_auc": 0.30, "prevalence": 0.01}
WEAK = {**GOOD, "pr_auc": 0.015}  # < 2x the random-ranking PR-AUC (0.01)


def _patched(metrics):
    storage = patch("scripts.train_model.Storage")
    engineer = patch("scripts.train_model.FeatureEngineer")
    detector = patch("scripts.train_model.MLDetector")
    measure = patch("scripts.train_model._measure", AsyncMock(return_value=dict(metrics)))
    return storage, engineer, detector, measure


async def _run(metrics, **kwargs):
    storage, engineer, detector, measure = _patched(metrics)
    with storage as MockStorage, engineer, detector as MockMLDetector, measure as m:
        MockStorage.return_value.initialize = AsyncMock()
        MockStorage.return_value.close = AsyncMock()
        instance = MockMLDetector.return_value
        instance.train = AsyncMock(return_value=[])
        instance.predict = AsyncMock()
        result = await train_model(chain_id=1, **kwargs)
        return result, instance, m


@pytest.mark.asyncio
async def test_good_model_is_measured_and_saved_with_its_metrics():
    result, instance, _ = await _run(GOOD, pool_addresses=["0x" + "a" * 40])

    assert result["saved"] is True
    instance.save_model.assert_called_once()
    assert instance.metrics["pr_auc"] == 0.30  # rates travel with the model
    # Chronological split: the most recent 20% is held out of training.
    assert instance.train.await_args.kwargs["holdout_fraction"] == 0.2


@pytest.mark.asyncio
async def test_weak_model_is_not_saved():
    result, instance, _ = await _run(WEAK, pool_addresses=["0x" + "a" * 40])

    assert result["saved"] is False
    instance.save_model.assert_not_called()


@pytest.mark.asyncio
async def test_weak_model_can_be_saved_explicitly():
    result, instance, _ = await _run(WEAK, pool_addresses=["0x" + "a" * 40], allow_weak_model=True)
    assert result["saved"] is True
    instance.save_model.assert_called_once()


@pytest.mark.asyncio
async def test_no_save_flag_skips_saving():
    result, instance, _ = await _run(GOOD, pool_addresses=["0x" + "a" * 40], save_model=False)
    assert result["saved"] is False
    instance.save_model.assert_not_called()


@pytest.mark.asyncio
async def test_multiple_pools_are_cross_validated_leaving_one_out():
    pools = ["0x" + c * 40 for c in "abc"]
    result, instance, measure = await _run(GOOD, pool_addresses=pools)

    # Three folds plus the final model.
    assert instance.train.await_count == 4
    fold_pools = [call.args[1] for call in instance.train.await_args_list[:3]]
    assert fold_pools == [pools[1:], [pools[0], pools[2]], pools[:2]]
    assert measure.await_count == 4
    assert result["cv"]["mean_pr_auc"] == pytest.approx(0.30)
    assert [f["held_out"] for f in result["cv"]["folds"]] == pools


@pytest.mark.asyncio
async def test_holdout_flagged_share_is_reported():
    import numpy as np
    import pandas as pd

    storage, engineer, detector, measure = _patched(GOOD)
    with storage as MockStorage, engineer, detector as MockMLDetector, measure:
        MockStorage.return_value.initialize = AsyncMock()
        MockStorage.return_value.close = AsyncMock()
        instance = MockMLDetector.return_value
        instance.train = AsyncMock(return_value=[pd.DataFrame({"x": [1, 2, 3, 4]})])
        instance.predict = AsyncMock(return_value=np.array([0.1, 0.7, 0.2, 0.9]))
        result = await train_model(chain_id=1, pool_addresses=["0x" + "a" * 40])

    assert result["holdout_trades"] == 4
    assert result["holdout_flagged_share"] == pytest.approx(0.5)  # >= threshold 0.6
