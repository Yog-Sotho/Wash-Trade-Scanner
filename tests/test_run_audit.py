"""
Tests for the audit runner.
"""

import json
import sys
from unittest.mock import AsyncMock, patch

import pytest

from core.storage import Storage
from core.validators import AuditParameters, validate_address
from models.schemas import SwapTrade
from scripts.run_audit import AuditRunner, classify_severity, main


def test_classify_severity_levels():
    assert classify_severity(0.75) == "CRITICAL"
    assert classify_severity(0.30) == "HIGH"
    assert classify_severity(0.15) == "MEDIUM"
    assert classify_severity(0.05) == "LOW"
    assert classify_severity(0.001) == "MINIMAL"


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
async def test_run_audit_reports_severity_and_method_breakdown():
    from datetime import datetime

    wash = SwapTrade(
        id=1,
        chain_id=1,
        pool_address="0x" + "b" * 40,
        sender="0x" + "a" * 40,
        recipient="0x" + "a" * 40,
        volume_usd=600.0,
        is_wash_trade=True,
        wash_trade_score=0.95,
        detection_method="position_neutral_scc",
        block_timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )
    clean = SwapTrade(
        id=2,
        chain_id=1,
        pool_address="0x" + "b" * 40,
        sender="0x" + "c" * 40,
        recipient="0x" + "d" * 40,
        volume_usd=400.0,
        is_wash_trade=False,
        block_timestamp=datetime(2024, 1, 1, 13, 0, 0),
    )

    with (
        patch("scripts.run_audit.MultiChainIngestor") as MockIngestor,
        patch("scripts.run_audit.FeatureEngineer"),
        patch("scripts.run_audit.HeuristicDetector"),
        patch("scripts.run_audit.MLDetector") as MockMLDetector,
        patch("scripts.run_audit.EntityClusterer"),
    ):
        MockIngestor.return_value.initialize = AsyncMock()
        MockIngestor.return_value.audit_pool = AsyncMock(return_value=2)
        MockMLDetector.return_value.load_model.side_effect = FileNotFoundError

        runner = AuditRunner()
        runner.storage = AsyncMock(spec=Storage)
        # get_pool_trades returns newest-first
        runner.storage.get_pool_trades.return_value = [clean, wash]

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

    metrics = result["risk_metrics"]
    assert metrics["wash_trade_volume_usd"] == 600.0
    assert metrics["wash_trade_volume_ratio"] == pytest.approx(0.6)
    assert metrics["severity"] == "CRITICAL"
    assert metrics["wash_volume_by_method"] == {"position_neutral_scc": 600.0}
    # first_trade_timestamp must be the chronologically earliest trade.
    assert metrics["first_trade_timestamp"] == wash.block_timestamp


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
