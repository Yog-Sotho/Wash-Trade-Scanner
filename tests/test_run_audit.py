"""
Tests for the audit runner.
"""

import json
import sys
from unittest.mock import AsyncMock, patch

import pytest

from core.storage import Storage
from core.validators import AuditParameters, validate_address
from scripts.run_audit import AuditRunner, main


def test_validate_address_valid():
    validate_address("0x" + "a" * 40)


def test_validate_address_invalid():
    with pytest.raises(ValueError):
        validate_address("0x123")


@pytest.mark.asyncio
async def test_run_audit_basic():
    # Mock all heavy components to verify the flow
    with (
        patch("scripts.run_audit.MultiChainIngestor") as MockIngestor,
        patch("scripts.run_audit.FeatureEngineer"),
        patch("scripts.run_audit.HeuristicDetector"),
        patch("scripts.run_audit.MLDetector") as MockMLDetector,
        patch("scripts.run_audit.EntityClusterer"),
    ):
        MockIngestor.return_value.initialize = AsyncMock()
        MockIngestor.return_value.audit_pool = AsyncMock(return_value=0)
        MockMLDetector.return_value.load_model.side_effect = FileNotFoundError

        runner = AuditRunner()
        runner.storage = AsyncMock(spec=Storage)
        runner.storage.get_pool_trades.return_value = []

        session = AsyncMock()
        session_cm = AsyncMock()
        session_cm.__aenter__.return_value = session
        runner.storage.get_session.return_value = session_cm

        params = AuditParameters(
            chain_id=1,
            pool_address="0x" + "b" * 40,
            use_ml=True,
            use_heuristics=False,
        )

        result = await runner.run_audit(params)

        assert result["trades_processed"] == 0
        MockIngestor.return_value.audit_pool.assert_awaited_once()
        runner.storage.create_audit_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_initialize_and_cleanup():
    runner = AuditRunner()
    with patch("scripts.run_audit.Storage") as MockStorage:
        storage_instance = MockStorage.return_value
        storage_instance.initialize = AsyncMock()
        storage_instance.close = AsyncMock()

        await runner.initialize()
        assert runner.storage is storage_instance
        storage_instance.initialize.assert_awaited_once()

        await runner.cleanup()
        storage_instance.close.assert_awaited_once()
        assert runner.storage is None


@pytest.mark.asyncio
async def test_cleanup_without_initialize_is_noop():
    runner = AuditRunner()
    await runner.cleanup()  # must not raise
    assert runner.storage is None


def test_signal_handler_sets_shutdown_event():
    runner = AuditRunner()
    assert not runner._shutdown_event.is_set()
    runner._signal_handler(2, None)
    assert runner._shutdown_event.is_set()


@pytest.mark.asyncio
async def test_export_results_json(tmp_path):
    runner = AuditRunner()
    export_file = tmp_path / "out.json"
    params = AuditParameters(chain_id=1, pool_address="0x" + "b" * 40)

    await runner._export_results(
        params,
        trades_processed=5,
        wash_trades_detected=1,
        risk_metrics={"overall_risk_score": 0.2, "total_volume_usd": 100.0},
        detection_methods=["self_trading"],
        duration=1.23,
        export_format="json",
        export_path=str(export_file),
    )

    data = json.loads(export_file.read_text())
    assert data["trades_processed"] == 5
    assert data["chain_id"] == 1


@pytest.mark.asyncio
async def test_export_results_csv(tmp_path):
    runner = AuditRunner()
    export_file = tmp_path / "out.csv"
    params = AuditParameters(chain_id=1, pool_address="0x" + "b" * 40)

    await runner._export_results(
        params,
        trades_processed=5,
        wash_trades_detected=1,
        risk_metrics={"overall_risk_score": 0.2, "total_volume_usd": 100.0},
        detection_methods=["self_trading"],
        duration=1.23,
        export_format="csv",
        export_path=str(export_file),
    )

    content = export_file.read_text()
    assert "overall_risk_score" in content
    assert "0.2" in content


def test_print_results_smoke(capsys):
    runner = AuditRunner()
    runner._print_results(
        pool_address="0x" + "b" * 40,
        chain_id=1,
        trades_analyzed=10,
        wash_trades=2,
        risk_metrics={
            "overall_risk_score": 0.2,
            "total_volume_usd": 1000.0,
            "wash_trade_volume_usd": 200.0,
        },
        detection_methods=["self_trading"],
        duration=1.5,
    )
    captured = capsys.readouterr()
    assert "Wash Trades Detected: 2" in captured.out


@pytest.mark.asyncio
async def test_main_rejects_invalid_pool_address(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["run_audit.py", "--chain-id", "1", "--pool", "not-an-address"]
    )
    exit_code = await main()
    assert exit_code == 1


@pytest.mark.asyncio
async def test_main_happy_path(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_audit.py",
            "--chain-id",
            "1",
            "--pool",
            "0x" + "c" * 40,
            "--no-ml",
            "--no-heuristics",
        ],
    )
    with (
        patch("scripts.run_audit.AuditRunner.initialize", AsyncMock()),
        patch("scripts.run_audit.AuditRunner.cleanup", AsyncMock()),
        patch("scripts.run_audit.AuditRunner.run_audit", AsyncMock(return_value={})),
        patch("scripts.run_audit.AuditRunner.initialize_signal_handlers"),
    ):
        exit_code = await main()
    assert exit_code == 0
