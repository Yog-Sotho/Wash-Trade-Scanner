"""wash-prune: retention is enforced only on request."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripts.prune import main, prune


@pytest.mark.asyncio
async def test_prune_without_apply_only_counts():
    storage = AsyncMock()
    storage.retention_cutoff = MagicMock(return_value=datetime(2026, 1, 1))
    storage.count_old_trades = AsyncMock(return_value=7)

    assert await prune(storage, 90, apply=False) == 7
    storage.cleanup_old_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_prune_defaults_to_the_retention_setting(monkeypatch):
    from config.settings import settings

    monkeypatch.setattr(settings, "TRADE_RETENTION_DAYS", 45)
    with (
        patch("scripts.prune.Storage") as MockStorage,
        patch("scripts.prune.prune", AsyncMock(return_value=0)) as run,
    ):
        MockStorage.return_value.initialize = AsyncMock()
        MockStorage.return_value.close = AsyncMock()
        assert await main([]) == 0

    assert run.await_args.args[1:] == (45, False)


@pytest.mark.asyncio
async def test_prune_rejects_non_positive_days():
    with pytest.raises(SystemExit):
        await main(["--days", "0"])
