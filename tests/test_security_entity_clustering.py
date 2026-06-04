
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import SecretStr

@pytest.mark.asyncio
async def test_build_funding_graph_protocol_validation():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    session = MagicMock(spec=AsyncSession)

    # Test invalid protocol (ftp)
    with patch("core.entity_clustering.get_chain_config") as mock_get_config, \
         patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.AsyncHTTPProvider"), \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

        mock_settings.rpc_urls = {}
        mock_get_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "ftp://localhost:8545"
        }

        mock_web3 = mock_web3_cls.return_value
        f_chain = asyncio.Future()
        f_chain.set_result(1)
        mock_web3.eth.chain_id = f_chain

        with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
            await clusterer.build_funding_graph(1, ["0x" + "0" * 40], session)

    # Test that it proceeds past protocol validation with http
    with patch("core.entity_clustering.get_chain_config") as mock_get_config, \
         patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.AsyncHTTPProvider"), \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

        mock_settings.rpc_urls = {}
        mock_get_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "http://localhost:8545"
        }

        mock_web3 = mock_web3_cls.return_value
        f_chain = asyncio.Future()
        f_chain.set_result(1)
        mock_web3.eth.chain_id = f_chain

        # Mock _node_supports_trace_filter to make it return quickly.
        clusterer._node_supports_trace_filter = AsyncMock(return_value=False)
        clusterer._fetch_funding_edges_block_scan = AsyncMock(return_value=[])

        f_block = asyncio.Future()
        f_block.set_result(100)
        mock_web3.eth.block_number = f_block

        await clusterer.build_funding_graph(
            1, ["0x" + "0" * 40], session,
            from_block_override=0, to_block_override=10
        )

@pytest.mark.asyncio
async def test_build_funding_graph_chain_id_mismatch():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    session = MagicMock(spec=AsyncSession)

    with patch("core.entity_clustering.get_chain_config") as mock_get_config, \
         patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.AsyncHTTPProvider"), \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

        mock_settings.rpc_urls = {}
        mock_get_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "https://localhost:8545"
        }

        mock_web3 = mock_web3_cls.return_value
        # Node returns Chain ID 56 instead of 1
        f = asyncio.Future()
        f.set_result(56)
        mock_web3.eth.chain_id = f

        with pytest.raises(ConnectionError, match="Chain ID mismatch"):
            await clusterer.build_funding_graph(1, ["0x" + "0" * 40], session)

@pytest.mark.asyncio
async def test_build_funding_graph_dos_protection():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    session = MagicMock(spec=AsyncSession)

    with patch("core.entity_clustering.get_chain_config") as mock_get_config, \
         patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.AsyncHTTPProvider"), \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

        mock_settings.rpc_urls = {}
        mock_get_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "https://localhost:8545"
        }

        mock_web3 = mock_web3_cls.return_value
        f_chain = asyncio.Future()
        f_chain.set_result(1)
        mock_web3.eth.chain_id = f_chain

        # Test range > 10,000,000
        with pytest.raises(ValueError, match="Block range 10000001 exceeds maximum"):
            await clusterer.build_funding_graph(
                1, ["0x" + "0" * 40], session,
                from_block_override=0, to_block_override=10_000_001
            )
