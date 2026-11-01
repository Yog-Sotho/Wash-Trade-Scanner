"""
Tests for the USD price backfill command.
"""

import sys
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core.pricing import PriceOracle
from scripts.backfill_prices import backfill_prices, main

USDC_ETH = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
UNKNOWN = "0x" + "1" * 40


def _row(row_id, token_in, amount_in, token_out=UNKNOWN):
    return SimpleNamespace(
        id=row_id,
        token_in=token_in,
        amount_in=float(amount_in),
        token_out=token_out,
        amount_out=1.0,
        block_timestamp=datetime(2026, 1, 15, 12, 0, 0),
    )


@pytest.mark.asyncio
async def test_backfill_prices_priceable_rows_and_pages_by_id():
    storage = AsyncMock()
    storage.get_unpriced_trades.side_effect = [
        [_row(3, USDC_ETH, 5 * 10**6), _row(7, UNKNOWN, 1)],
        [_row(9, USDC_ETH, 2 * 10**6)],
        [],
    ]
    storage.update_trade_prices.side_effect = lambda updates: len(updates)

    examined, priced = await backfill_prices(storage, PriceOracle(None), 1, batch_size=2)

    assert (examined, priced) == (3, 2)
    # Keyset pagination advances past unpriceable rows, so the loop terminates.
    after_ids = [c.args[1] for c in storage.get_unpriced_trades.call_args_list]
    assert after_ids == [0, 7, 9]
    first_updates = storage.update_trade_prices.call_args_list[0].args[0]
    assert first_updates == [
        {"id": 3, "amount_in_usd": 5.0, "amount_out_usd": None, "volume_usd": 5.0}
    ]


@pytest.mark.asyncio
async def test_backfill_prices_passes_pool_filter():
    storage = AsyncMock()
    storage.get_unpriced_trades.return_value = []

    await backfill_prices(storage, PriceOracle(None), 1, pool_address="0xpool", batch_size=10)

    storage.get_unpriced_trades.assert_awaited_once_with(1, 0, 10, "0xpool")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "argv",
    [
        ["--chain-id", "1", "--pool", "not-an-address"],
        ["--chain-id", "1", "--batch-size", "0"],
    ],
)
async def test_main_rejects_invalid_arguments(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["backfill_prices.py", *argv])
    with patch("scripts.backfill_prices.Storage") as mock_storage:
        assert await main() == 1
    mock_storage.assert_not_called()  # nothing touched before validation


@pytest.mark.asyncio
async def test_main_normalizes_pool_and_closes_storage(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_prices.py",
            "--chain-id",
            "1",
            "--pool",
            "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        ],
    )
    with (
        patch("scripts.backfill_prices.Storage") as mock_storage,
        patch("scripts.backfill_prices.backfill_prices", AsyncMock(return_value=(0, 0))) as run,
    ):
        mock_storage.return_value.initialize = AsyncMock()
        mock_storage.return_value.close = AsyncMock()
        assert await main() == 0

    assert run.call_args.args[3] == "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    mock_storage.return_value.close.assert_awaited_once()
