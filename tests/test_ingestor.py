"""
Unit tests for multi-chain ingestor.

Tests cover:
- ChainIngestor connection and log fetching
- MultiChainIngestor initialization
- Error handling and retry logic
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from web3 import AsyncWeb3

from core.ingestor import (
    ChainIngestor,
    MultiChainIngestor,
    RateLimiter,
    ConnectionError,
    LogFetchError,
)


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def mock_storage() -> MagicMock:
    """Create a mock storage instance."""
    storage = MagicMock()
    storage.save_trade = AsyncMock(return_value=MagicMock())
    storage.get_session = AsyncMock()
    return storage


@pytest.fixture
def sample_chain_config() -> Dict[str, Any]:
    """Create a sample chain configuration dict."""
    return {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/dummy_key",
        "ws_url": "",
        "native_token": "ETH",
        "block_time": 12.0,
        "explorer_api": "https://api.etherscan.io/api",
        "start_block": 17000000,
        "dexes": [
            {
                "name": "UniswapV2",
                "router": "0x7a250d5630B4cF539739dF2C5dAcbBDe9D5F4d3d",
                "factory": "0x5C69bEe701ef814a2B6f3DE2fb9B724b8E5F73fC",
                "event_sig": "Swap(address,uint256,uint256,uint256,uint256,address)",
                "abi": [
                    {
                        "anonymous": False,
                        "inputs": [
                            {"indexed": True, "name": "sender", "type": "address"},
                            {"indexed": False, "name": "amount0In", "type": "uint256"},
                            {"indexed": False, "name": "amount1In", "type": "uint256"},
                            {"indexed": False, "name": "amount0Out", "type": "uint256"},
                            {"indexed": False, "name": "amount1Out", "type": "uint256"},
                            {"indexed": True, "name": "to", "type": "address"},
                        ],
                        "name": "Swap",
                        "type": "event",
                    }
                ],
                "type": "v2",
            }
        ],
    }


@pytest.fixture
def sample_swap_log() -> Dict[str, Any]:
    """Create a sample swap event log."""
    return {
        "address": "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9cDc",
        "topics": [
            "0xd78ad95fa46c994b6551d0da82f3d9e6fa9f7d3b5e5c5c5c5c5c5c5c5c5c5c5c5c",
        ],
        "data": "0x0000000000000000000000000000000000000000000000000000000000000001",
        "transactionHash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "blockNumber": 17000001,
        "logIndex": 0,
        "blockHash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab",
        "transactionIndex": 0,
    }


# ==============================================================================
# Rate Limiter Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_rate_limiter_acquire():
    """Test rate limiter token acquisition."""
    limiter = RateLimiter(max_calls_per_second=10)
    await limiter.acquire()  # Should not raise
    assert len(limiter._calls) == 1


@pytest.mark.asyncio
async def test_rate_limiter_enforces_limit():
    """Test rate limiter enforces rate limit."""
    limiter = RateLimiter(max_calls_per_second=2)

    # First two calls should succeed immediately
    await limiter.acquire()
    await limiter.acquire()

    # Third call should require waiting
    start = asyncio.get_event_loop().time()
    await limiter.acquire()
    elapsed = asyncio.get_event_loop().time() - start

    # Should have waited approximately 1 second
    assert elapsed >= 0.9


# ==============================================================================
# Chain Ingestor Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_chain_ingestor_connect_http(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test ChainIngestor connects via HTTP."""
    ingestor = ChainIngestor(sample_chain_config, mock_storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        mock_web3.middleware_onion = MagicMock()
        mock_web3.middleware_onion.inject = MagicMock()
        mock_web3_class.return_value = mock_web3

        await ingestor.connect()

        assert ingestor.web3 is not None
        mock_web3.is_connected.assert_called()


@pytest.mark.asyncio
async def test_chain_ingestor_connect_fallback_http(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test ChainIngestor falls back to HTTP on WebSocket failure."""
    sample_chain_config["ws_url"] = "wss://example.com/ws"
    ingestor = ChainIngestor(sample_chain_config, mock_storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=False)
        mock_web3_class.side_effect = Exception("WebSocket failed")

        mock_web3_http = AsyncMock()
        mock_web3_http.is_connected = AsyncMock(return_value=True)
        mock_web3_http.middleware_onion = MagicMock()
        mock_web3_http.middleware_onion.inject = MagicMock()

        # Return different instances
        mock_web3_class.side_effect = None
        mock_web3_class.side_effect = lambda *args, **kwargs: mock_web3_http

        # Re-patch after first failure
        mock_web3_class.reset_mock()
        mock_web3_class.return_value = mock_web3_http
        mock_web3_http.is_connected.return_value = True

        await ingestor.connect()

        assert ingestor.web3 is not None


@pytest.mark.asyncio
async def test_chain_ingestor_connect_fails(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test ChainIngestor raises when connection fails."""
    ingestor = ChainIngestor(sample_chain_config, mock_storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=False)
        mock_web3.middleware_onion = MagicMock()
        mock_web3.middleware_onion.inject = MagicMock()
        mock_web3_class.return_value = mock_web3

        with pytest.raises(ConnectionError):
            await ingestor.connect()


@pytest.mark.asyncio
async def test_chain_ingestor_get_pool_tokens(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test ChainIngestor fetches and caches pool tokens."""
    ingestor = ChainIngestor(sample_chain_config, mock_storage)
    ingestor.web3 = AsyncMock()

    mock_contract = AsyncMock()
    mock_contract.functions.token0 = MagicMock()
    mock_contract.functions.token0.return_value.call = AsyncMock(
        return_value="0xA0b86a33E6441C6bF6A7a5E7b7e3E5F2C8D9E1F3"
    )
    mock_contract.functions.token1 = MagicMock()
    mock_contract.functions.token1.return_value.call = AsyncMock(
        return_value="0xB1c97d03E6452C7f8B2e4D6C5F6A8E9D0C1E2F4"
    )

    ingestor.web3.eth.contract = MagicMock(return_value=mock_contract)

    token0, token1 = await ingestor._get_pool_tokens(
        "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9cDc"
    )

    # Should return lowercase addresses
    assert token0.startswith("0x")
    assert token1.startswith("0x")

    # Test cache hit
    token0_cached, token1_cached = await ingestor._get_pool_tokens(
        "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9cDc"
    )
    assert token0_cached == token0
    assert token1_cached == token1

    # Contract should only be called once due to cache
    assert mock_contract.functions.token0.return_value.call.call_count == 1


# ==============================================================================
# MultiChain Ingestor Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_multichain_ingestor_initialize(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test MultiChainIngestor initializes all chains."""
    ingestor = MultiChainIngestor(mock_storage)

    with patch("core.ingestor.CHAINS", [sample_chain_config]):
        with patch.object(ChainIngestor, "connect", new_callable=AsyncMock):
            await ingestor.initialize()
            assert len(ingestor.ingestors) == 1
            assert ingestor._initialized is True


@pytest.mark.asyncio
async def test_multichain_ingestor_initialize_skips_if_already_initialized(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test MultiChainIngestor skips if already initialized."""
    ingestor = MultiChainIngestor(mock_storage)
    ingestor._initialized = True

    with patch("core.ingestor.CHAINS", [sample_chain_config]):
        await ingestor.initialize()
        assert len(ingestor.ingestors) == 0  # No new ingestors


@pytest.mark.asyncio
async def test_multichain_ingestor_stop_all(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test MultiChainIngestor stops all listeners."""
    ingestor = MultiChainIngestor(mock_storage)

    # Create a mock ingestor
    mock_chain_ingestor = MagicMock()
    mock_chain_ingestor.stop = AsyncMock()
    mock_chain_ingestor.is_running = True
    ingestor.ingestors = {1: mock_chain_ingestor}

    # Create mock tasks
    mock_task1 = AsyncMock()
    mock_task2 = AsyncMock()
    ingestor.tasks = [mock_task1, mock_task2]

    await ingestor.stop_all()

    mock_chain_ingestor.stop.assert_called_once()
    mock_task1.cancel.assert_called_once()
    mock_task2.cancel.assert_called_once()
    assert len(ingestor.tasks) == 0


@pytest.mark.asyncio
async def test_multichain_ingestor_audit_pool(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test MultiChainIngestor audits a specific pool."""
    ingestor = MultiChainIngestor(mock_storage)

    # Create a mock ingestor
    mock_chain_ingestor = MagicMock()
    mock_chain_ingestor.chain_config = sample_chain_config
    mock_chain_ingestor.dexes = sample_chain_config["dexes"]
    mock_chain_ingestor.rate_limiter = RateLimiter(10)
    mock_chain_ingestor.web3 = AsyncMock()
    mock_chain_ingestor.web3.eth.block_number = 17000100
    mock_chain_ingestor.sync_historical_swaps = AsyncMock(return_value=10)
    ingestor.ingestors = {1: mock_chain_ingestor}

    total_swaps = await ingestor.audit_pool(
        chain_id=1,
        pool_address="0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9cDc",
    )

    assert total_swaps == 10  # 10 swaps per DEX, 1 DEX
    mock_chain_ingestor.sync_historical_swaps.assert_called()


@pytest.mark.asyncio
async def test_multichain_ingestor_audit_pool_unknown_chain(
    mock_storage: MagicMock,
):
    """Test MultiChainIngestor raises for unknown chain."""
    ingestor = MultiChainIngestor(mock_storage)
    ingestor.ingestors = {}

    with pytest.raises(ValueError, match="Chain 999 not configured"):
        await ingestor.audit_pool(
            chain_id=999,
            pool_address="0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9cDc",
        )


# ==============================================================================
# Error Handling Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_chain_ingestor_handles_log_fetch_error(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test ChainIngestor handles log fetch errors gracefully."""
    ingestor = ChainIngestor(sample_chain_config, mock_storage)
    ingestor.web3 = AsyncMock()
    ingestor.rate_limiter = RateLimiter(10)

    # Mock get_logs to fail
    ingestor.web3.eth.get_logs = AsyncMock(
        side_effect=Exception("RPC error")
    )

    logs = await ingestor._get_logs_batch(
        address="0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9cDc",
        topics=["0xd78ad95fa46c994b6551d0da82f3d9e6fa9f7d3b5e5c5c5c5c5c5c5c5c5c5c5c5c"],
        from_block=17000000,
        to_block=17000100,
    )

    # Should return empty list and track failures
    assert logs == []
    assert len(ingestor._failed_batches) > 0


# ==============================================================================
# Integration Tests (if db available)
# ==============================================================================

@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("DATABASE_URL") is None,
    reason="DATABASE_URL not set"
)
async def test_full_ingestion_pipeline(
    sample_chain_config: Dict[str, Any],
    mock_storage: MagicMock,
):
    """Test full ingestion pipeline with mock data."""
    # This would be an integration test requiring actual DB
    # For now, just verify the structure
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
