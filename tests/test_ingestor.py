"""
Unit tests for multi-chain ingestor.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config.chains import ChainConfig, DEXConfig
from core.ingestor import ChainIngestor, MultiChainIngestor
from core.storage import Storage


@pytest.fixture
def mock_chain_config():
    return {
        "chain_id": 1,
        "name": "TestChain",
        "rpc_url": "http://localhost:8545",
        "ws_url": "",
        "native_token": "TEST",
        "block_time": 12.0,
        "explorer_api": "",
        "dexes": [
            {
                "name": "TestDEX",
                "router": "0x1234",
                "factory": "0x5678",
                "event_sig": "Swap(address,uint256,uint256,uint256,uint256,address)",
                "abi": [],
                "type": "v2",
            }
        ],
        "start_block": 0,
    }


@pytest.mark.asyncio
async def test_chain_ingestor_connection(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        # Mock eth.chain_id as a future
        import asyncio

        f = asyncio.Future()
        f.set_result(mock_chain_config["chain_id"])
        mock_web3.eth.chain_id = f
        mock_web3_class.return_value = mock_web3

        await ingestor.connect()

        assert ingestor.web3 is not None
        mock_web3.is_connected.assert_called_once()


@pytest.mark.asyncio
async def test_multichain_ingestor_initialize():
    storage = AsyncMock(spec=Storage)
    ingestor = MultiChainIngestor(storage)

    with patch("core.ingestor.CHAINS", []):  # Empty chain list for test
        await ingestor.initialize()
        assert len(ingestor.ingestors) == 0
