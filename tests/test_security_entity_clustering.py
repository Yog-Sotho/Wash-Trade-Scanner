import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage

@pytest.mark.asyncio
async def test_build_funding_graph_protocol_validation():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    # Mock settings.rpc_urls
    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "ftp://localhost:8545"}

        with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
            await clusterer.build_funding_graph(1, ["0x" + "1" * 40], MagicMock())

@pytest.mark.asyncio
async def test_build_funding_graph_chain_id_mismatch():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls, \
         patch("core.entity_clustering.get_chain_config") as mock_get_chain_config:

        mock_settings.rpc_urls = {1: "http://localhost:8545"}
        mock_get_chain_config.return_value = {"chain_id": 1, "rpc_url": "http://localhost:8545"}

        mock_web3 = mock_web3_cls.return_value

        f_block = asyncio.Future()
        f_block.set_result(1000)
        mock_web3.eth.block_number = f_block

        # Node returns Chain ID 56 instead of 1
        f_chain = asyncio.Future()
        f_chain.set_result(56)
        mock_web3.eth.chain_id = f_chain

        with pytest.raises(ConnectionError, match="Chain ID mismatch"):
            await clusterer.build_funding_graph(1, ["0x" + "1" * 40], MagicMock())

@pytest.mark.asyncio
async def test_build_funding_graph_dos_protection():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls, \
         patch("core.entity_clustering.get_chain_config") as mock_get_chain_config:

        mock_settings.rpc_urls = {1: "http://localhost:8545"}
        mock_get_chain_config.return_value = {"chain_id": 1, "rpc_url": "http://localhost:8545"}

        mock_web3 = mock_web3_cls.return_value

        f_block = asyncio.Future()
        f_block.set_result(20_000_000)
        mock_web3.eth.block_number = f_block

        f_chain = asyncio.Future()
        f_chain.set_result(1)
        mock_web3.eth.chain_id = f_chain

        # Attempting a range of 11M blocks
        with pytest.raises(ValueError, match="Block range 11000000 exceeds maximum"):
            await clusterer.build_funding_graph(
                1, ["0x" + "1" * 40], MagicMock(),
                from_block_override=0,
                to_block_override=11_000_000
            )
