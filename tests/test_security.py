
import pytest
import os
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from scripts.run_audit import AuditRunner
from core.validators import AuditParameters

@pytest.mark.asyncio
async def test_export_results_path_traversal_protection():
    runner = AuditRunner()
    params = AuditParameters(
        chain_id=1,
        pool_address="0x" + "0" * 40,
        use_ml=False,
        use_heuristics=False
    )

    risk_metrics = {
        "overall_risk_score": 0.1,
        "wash_trade_volume_ratio": 0.1,
        "total_trades_analyzed": 10,
        "total_volume_usd": 1000.0,
        "wash_trade_volume_usd": 100.0,
        "first_trade_timestamp": None
    }

    # Attempting to export to a "traversal" path
    traversal_path = "/tmp/sentinel_test_traversal.json"

    # We expect the file to be created in the CURRENT directory, not /tmp/
    expected_filename = "sentinel_test_traversal.json"

    if os.path.exists(traversal_path):
        os.remove(traversal_path)
    if os.path.exists(expected_filename):
        os.remove(expected_filename)

    await runner._export_results(
        params=params,
        trades_processed=10,
        wash_trades_detected=1,
        risk_metrics=risk_metrics,
        detection_methods=["heuristic"],
        duration=1.0,
        export_format="json",
        export_path=traversal_path
    )

    # Check that it DOES NOT exist in /tmp/
    assert not os.path.exists(traversal_path)

    # Check that it DOES exist in current directory (sanitized)
    assert os.path.exists(expected_filename)

    # Cleanup
    if os.path.exists(expected_filename):
        os.remove(expected_filename)

@pytest.mark.asyncio
async def test_entity_clusterer_dos_protection():
    """Verify that EntityClusterer rejects oversized block ranges."""
    from core.entity_clustering import EntityClusterer
    from core.storage import Storage

    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    session = AsyncMock()

    with patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:
        mock_web3 = mock_web3_cls.return_value
        mock_web3.eth.block_number = AsyncMock(return_value=20_000_000)

        # Test oversized range
        with pytest.raises(ValueError, match="Block range 10000001 exceeds maximum"):
            await clusterer.build_funding_graph(
                chain_id=1,
                addresses=["0x" + "0" * 40],
                session=session,
                from_block_override=1,
                to_block_override=10_000_002
            )

        # Test to_block < from_block
        with pytest.raises(ValueError, match="cannot be less than from_block"):
            await clusterer.build_funding_graph(
                chain_id=1,
                addresses=["0x" + "0" * 40],
                session=session,
                from_block_override=100,
                to_block_override=50
            )

@pytest.mark.asyncio
async def test_run_audit_no_stack_trace_leak():
    """Verify that run_audit.main does not log stack traces for unexpected errors."""
    from scripts.run_audit import main
    import argparse

    # Mock arguments
    with patch("argparse.ArgumentParser.parse_args") as mock_args:
        mock_args.return_value = argparse.Namespace(
            chain_id=1,
            pool="0x" + "0" * 40,
            start_block=None,
            end_block=None,
            no_ml=False,
            no_heuristics=False,
            export=None,
            export_path=None
        )

        # Force an unexpected exception during initialization
        with patch("scripts.run_audit.AuditRunner.initialize", side_effect=RuntimeError("Secret database error")):
            with patch("scripts.run_audit.logger") as mock_logger:
                exit_code = await main()
                assert exit_code == 1

                # Ensure logger.error was called, not logger.exception
                mock_logger.error.assert_called_with("Unexpected error: Secret database error")
                mock_logger.exception.assert_not_called()
