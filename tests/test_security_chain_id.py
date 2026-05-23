"""
Security tests for RPC connection validation.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio

from core.ingestor import ChainIngestor
from core.storage import Storage

@pytest.fixture
def chain_config():
    return {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": "http://mock-rpc:8545",
        "dexes": []
    }

@pytest.mark.asyncio
async def test_connect_verifies_chain_id(chain_config):
    """
    Test that implementation now verifies chain ID.
    """
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        # Mock eth.chain_id to return a DIFFERENT chain ID
        # In web3.py v6, eth.chain_id is a coroutine
        mock_web3.eth.chain_id = AsyncMock(return_value=56)()
        mock_web3_class.return_value = mock_web3

        with pytest.raises(ConnectionError, match="Chain ID mismatch"):
            await ingestor.connect()

@pytest.mark.asyncio
async def test_connect_rejects_non_http_protocols(chain_config):
    """
    Test that implementation rejects non-HTTP/HTTPS protocols.
    """
    bad_config = chain_config.copy()
    bad_config["rpc_url"] = "ftp://malicious-node:21"
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(bad_config, storage)

    with pytest.raises(ValueError, match="Insecure or invalid RPC protocol"):
        await ingestor.connect()

@pytest.mark.asyncio
async def test_connect_success_with_matching_chain_id(chain_config):
    """
    Test that connection succeeds when chain ID matches.
    """
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        # Match expected chain ID 1
        mock_web3.eth.chain_id = AsyncMock(return_value=1)()
        mock_web3_class.return_value = mock_web3

        await ingestor.connect()
        assert ingestor.web3 is not None
