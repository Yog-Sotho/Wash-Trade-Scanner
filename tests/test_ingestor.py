"""
Unit tests for multi-chain ingestor.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from core.ingestor import ChainIngestor, MultiChainIngestor
from core.storage import Storage
from config.chains import ChainConfig, DEXConfig


@pytest.fixture
def mock_chain_config():
    return ChainConfig(
        chain_id=1,
        name="TestChain",
        rpc_url="http://localhost:8545",
        ws_url="",
        native_token="TEST",
        block_time=12.0,
        explorer_api="",
        dexes=[
            DEXConfig(
                name="TestDEX",
                router_address="0x1234",
                factory_address="0x5678",
                swap_event_signature="Swap(address,uint256,uint256,uint256,uint256,address)",
                abi=[],
            )
        ],
        start_block=0,
    )


@pytest.mark.asyncio
async def test_chain_ingestor_connection(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
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