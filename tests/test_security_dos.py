import pytest
import asyncio
from pydantic import ValidationError
from core.validators import AuditParameters
from core.ingestor import MultiChainIngestor
from unittest.mock import MagicMock, AsyncMock, patch

def test_audit_parameters_block_range_validation():
    # Valid range
    params = AuditParameters(
        chain_id=1,
        pool_address="0x" + "0" * 40,
        start_block=100,
        end_block=200
    )
    assert params.start_block == 100
    assert params.end_block == 200

    # Invalid range (end <= start)
    with pytest.raises(ValidationError, match="end_block must be greater than start_block"):
        AuditParameters(
            chain_id=1,
            pool_address="0x" + "0" * 40,
            start_block=200,
            end_block=100
        )

    # Invalid range (> 10M)
    with pytest.raises(ValidationError, match="Block range exceeds maximum of 10,000,000"):
        AuditParameters(
            chain_id=1,
            pool_address="0x" + "0" * 40,
            start_block=0,
            end_block=10_000_001
        )

@pytest.mark.asyncio
async def test_ingestor_audit_pool_block_range_enforcement():
    storage = MagicMock()
    ingestor = MultiChainIngestor(storage)

    mock_chain_ingestor = MagicMock()
    mock_chain_ingestor.web3.eth.block_number = AsyncMock(return_value=20_000_000)
    mock_chain_ingestor.circuit_breaker = MagicMock()
    mock_chain_ingestor.rate_limiter = MagicMock()
    mock_chain_ingestor.rate_limiter.acquire = AsyncMock()

    async def mock_call(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    mock_chain_ingestor.circuit_breaker.call.side_effect = mock_call

    ingestor.ingestors[1] = mock_chain_ingestor

    with patch("core.ingestor.get_chain_config", return_value={"dexes": [], "start_block": 0}):
        # Case where end_block is resolved to something much larger than start_block
        # start_block=0, end_block defaults to 20,000,000
        with pytest.raises(ValueError, match="exceeds maximum of 10,000,000"):
            await ingestor.audit_pool(chain_id=1, pool_address="0x" + "0" * 40, start_block=0, end_block=None)

        # Valid range should not raise
        await ingestor.audit_pool(chain_id=1, pool_address="0x" + "0" * 40, start_block=15_000_000, end_block=None)
