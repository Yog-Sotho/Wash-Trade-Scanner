
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from core.ingestor import MultiChainIngestor, ChainIngestor
from core.validators import AuditParameters
from pydantic import ValidationError

@pytest.mark.asyncio
async def test_audit_pool_block_range_dos_vulnerability():
    # Mock storage
    storage = MagicMock()

    # Mock Web3 and Ingestor
    ingestor = MultiChainIngestor(storage)

    chain_id = 1
    mock_chain_ingestor = MagicMock(spec=ChainIngestor)
    mock_chain_ingestor.web3 = MagicMock()
    mock_chain_ingestor.web3.eth = MagicMock()
    mock_chain_ingestor.web3.eth.block_number = AsyncMock(return_value=20_000_000)
    mock_chain_ingestor.circuit_breaker = AsyncMock()
    mock_chain_ingestor.circuit_breaker.call = AsyncMock(side_effect=lambda func, *args: func(*args) if callable(func) else func)
    mock_chain_ingestor.rate_limiter = AsyncMock()
    mock_chain_ingestor.sync_historical_swaps = AsyncMock(return_value=0)

    ingestors = {chain_id: mock_chain_ingestor}
    ingestor.ingestors = ingestors

    # CASE 1: start_block is default (12,000,000), end_block is 25,000,000 -> 13M range
    # This should now FAIL
    with pytest.raises(ValueError, match="exceeds maximum of 10,000,000"):
        await ingestor.audit_pool(
            chain_id=chain_id,
            pool_address="0x" + "1" * 40,
            start_block=None,
            end_block=25_000_000
        )

    # Verify it did NOT call sync_historical_swaps
    mock_chain_ingestor.sync_historical_swaps.assert_not_called()

@pytest.mark.asyncio
async def test_audit_parameters_partial_validation_vulnerability():
    # AuditParameters should now catch > 10M range even if start_block is None (defaults to 0)
    with pytest.raises(ValidationError, match="exceeds maximum of 10,000,000"):
        AuditParameters(
            chain_id=1,
            pool_address="0x" + "2" * 40,
            start_block=None,
            end_block=15_000_000
        )
