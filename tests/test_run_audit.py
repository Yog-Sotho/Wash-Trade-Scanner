"""
Tests for the audit runner script.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from scripts.run_audit import run_audit, AuditRunner
from core.validators import validate_address

@pytest.mark.asyncio
async def test_validate_address_valid():
    addr = "0x" + "a" * 40
    validated = validate_address(addr)
    assert validated.startswith("0x")
    assert len(validated) == 42

@pytest.mark.asyncio
async def test_validate_address_invalid():
    with pytest.raises(ValueError):
        validate_address("invalid")

@pytest.mark.asyncio
async def test_run_audit_basic():
    with patch("scripts.run_audit.Storage") as mock_storage_class, \
         patch("scripts.run_audit.MultiChainIngestor") as mock_ingestor_class, \
         patch("scripts.run_audit.HeuristicDetector") as mock_heuristic_class, \
         patch("scripts.run_audit.MLDetector") as mock_ml_class:

        mock_storage = AsyncMock()
        mock_storage_class.return_value = mock_storage

        mock_ingestor = AsyncMock()
        mock_ingestor_class.return_value = mock_ingestor

        mock_heuristic = AsyncMock()
        mock_heuristic.run_all_heuristics.return_value = ([], {})
        mock_heuristic_class.return_value = mock_heuristic

        mock_ml = AsyncMock()
        mock_ml_class.return_value = mock_ml

        # Mock get_session to return an async context manager
        mock_session = AsyncMock()
        mock_cm = AsyncMock()
        mock_cm.__aenter__.return_value = mock_session
        mock_storage.get_session.return_value = mock_cm

        mock_storage.get_pool_trades.return_value = []

        result = await run_audit(chain_id=1, pool_address="0x" + "b"*40, use_ml=False)

        assert "trades_processed" in result
        mock_ingestor.audit_pool.assert_called_once()
