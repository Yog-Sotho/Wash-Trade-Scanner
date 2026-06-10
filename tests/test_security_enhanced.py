import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage

@pytest.mark.asyncio
async def test_entity_clusterer_rpc_protocol_validation():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    # Test invalid protocol (ftp)
    with patch("core.entity_clustering.get_chain_config") as mock_get_chain_config, \
         patch("core.entity_clustering.settings") as mock_settings:
        mock_get_chain_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "http://ignore-me"
        }
        # Force an invalid URL via settings to ensure the check triggers regardless of config source
        mock_settings.rpc_urls = {1: "ftp://localhost:8545"}

        with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
            await clusterer.build_funding_graph(1, ["0x123"], MagicMock())

@pytest.mark.asyncio
async def test_entity_clusterer_chain_id_mismatch():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.get_chain_config") as mock_get_chain_config, \
         patch("core.entity_clustering.AsyncHTTPProvider"), \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

        mock_get_chain_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "http://localhost:8545"
        }

        mock_web3 = mock_web3_cls.return_value
        mock_web3.is_connected = AsyncMock(return_value=True)

        # Node returns Chain ID 56 (BSC) instead of 1 (Ethereum)
        f = asyncio.Future()
        f.set_result(56)
        mock_web3.eth.chain_id = f

        with pytest.raises(ConnectionError, match="Chain ID mismatch"):
            await clusterer.build_funding_graph(1, ["0x123"], MagicMock())

@pytest.mark.asyncio
async def test_entity_clusterer_block_range_limit():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.get_chain_config") as mock_get_chain_config, \
         patch("core.entity_clustering.AsyncHTTPProvider"), \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

        mock_get_chain_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "http://localhost:8545"
        }

        mock_web3 = mock_web3_cls.return_value
        mock_web3.is_connected = AsyncMock(return_value=True)

        f = asyncio.Future()
        f.set_result(1)
        mock_web3.eth.chain_id = f

        # Range of 10,000,001 blocks (exceeds 10,000,000)
        with pytest.raises(ValueError, match="Block range 10000001 exceeds maximum of 10,000,000"):
            await clusterer.build_funding_graph(
                1, ["0x123"], MagicMock(),
                from_block_override=0,
                to_block_override=10_000_001
            )
