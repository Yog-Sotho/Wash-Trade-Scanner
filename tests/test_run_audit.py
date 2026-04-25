"""
Tests for the audit runner.
"""

import pytest
from unittest.mock import AsyncMock, patch
from scripts.run_audit import validate_address, run_audit

def test_validate_address_valid():
    validate_address("0x" + "a"*40)

def test_validate_address_invalid():
    with pytest.raises(ValueError):
        validate_address("0x123")

@pytest.mark.asyncio
async def test_run_audit_basic():
    # Mock all heavy components to verify the flow
    with patch("scripts.run_audit.Storage") as MockStorage, \
         patch("scripts.run_audit.MultiChainIngestor") as MockIngestor, \
         patch("scripts.run_audit.FeatureEngineer"), \
         patch("scripts.run_audit.HeuristicDetector"), \
         patch("scripts.run_audit.MLDetector"), \
         patch("scripts.run_audit.EntityClusterer"):
        storage_instance = MockStorage.return_value
        storage_instance.get_pool_trades.return_value = []
        storage_instance.get_session.return_value.__aenter__.return_value = AsyncMock()
        await run_audit(chain_id=1, pool_address="0x" + "b"*40, sync_historical=False, use_ml=False)
        MockIngestor.return_value.audit_pool.assert_not_called()
        storage_instance.create_audit_log.assert_awaited_once()
