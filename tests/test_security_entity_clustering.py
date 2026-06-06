import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage

@pytest.mark.asyncio
async def test_build_funding_graph_protocol_validation():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    # Test invalid protocol (ftp)
    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "ftp://localhost:8545"}
        with patch("config.chains.get_chain_config") as mock_get_config:
            mock_get_config.return_value = {"rpc_url": "ftp://localhost:8545", "chain_id": 1}
            with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
                await clusterer.build_funding_graph(1, ["0x123"], MagicMock())

    # Test invalid protocol (javascript)
    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "javascript:alert(1)"}
        with patch("config.chains.get_chain_config") as mock_get_config:
            mock_get_config.return_value = {"rpc_url": "javascript:alert(1)", "chain_id": 1}
            with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
                await clusterer.build_funding_graph(1, ["0x123"], MagicMock())

@pytest.mark.asyncio
async def test_build_funding_graph_chain_id_mismatch():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "http://localhost:8545"}
        with patch("config.chains.get_chain_config") as mock_get_config, \
             patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

            mock_get_config.return_value = {"rpc_url": "http://localhost:8545", "chain_id": 1}
            mock_web3 = mock_web3_cls.return_value
            mock_web3.is_connected = AsyncMock(return_value=True)

            # Node returns Chain ID 56 (BSC) instead of 1 (Ethereum)
            f = asyncio.Future()
            f.set_result(56)
            mock_web3.eth.chain_id = f

            with pytest.raises(ConnectionError, match="Chain ID mismatch"):
                await clusterer.build_funding_graph(1, ["0x123"], MagicMock())

@pytest.mark.asyncio
async def test_build_funding_graph_block_range_limit():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "http://localhost:8545"}
        with patch("config.chains.get_chain_config") as mock_get_config, \
             patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

            mock_get_config.return_value = {"rpc_url": "http://localhost:8545", "chain_id": 1}
            mock_web3 = mock_web3_cls.return_value
            mock_web3.is_connected = AsyncMock(return_value=True)

            f = asyncio.Future()
            f.set_result(1)
            mock_web3.eth.chain_id = f

            # Range of 10,000,001 blocks (exceeds limit)
            with pytest.raises(ValueError, match="Block range 10000001 exceeds maximum of 10,000,000"):
                await clusterer.build_funding_graph(1, ["0x123"], MagicMock(), from_block_override=0, to_block_override=10_000_001)

@pytest.mark.asyncio
async def test_build_funding_graph_placeholder_rpc():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "https://mainnet.infura.io/v3/YOUR_KEY"}
        with patch("config.chains.get_chain_config") as mock_get_config:
            mock_get_config.return_value = {"rpc_url": "https://mainnet.infura.io/v3/YOUR_KEY", "chain_id": 1}
            with pytest.raises(ValueError, match="contains placeholder"):
                await clusterer.build_funding_graph(1, ["0x123"], MagicMock())

@pytest.mark.asyncio
async def test_build_funding_graph_success():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "http://localhost:8545"}
        with patch("config.chains.get_chain_config") as mock_get_config, \
             patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

            mock_get_config.return_value = {"rpc_url": "http://localhost:8545", "chain_id": 1}
            mock_web3 = mock_web3_cls.return_value
            mock_web3.is_connected = AsyncMock(return_value=True)

            f1 = asyncio.Future()
            f1.set_result(1)
            mock_web3.eth.chain_id = f1

            # Mock other things needed for build_funding_graph to complete
            f2 = asyncio.Future()
            f2.set_result(100)
            mock_web3.eth.block_number = f2

            clusterer._node_supports_trace_filter = AsyncMock(return_value=False)
            clusterer._fetch_funding_edges_block_scan = AsyncMock(return_value=[])

            await clusterer.build_funding_graph(1, ["0x123"], MagicMock())
