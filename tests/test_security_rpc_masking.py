import pytest
from pydantic import SecretStr
from config.settings import Settings
from core.ingestor import ChainIngestor
from core.entity_clustering import EntityClusterer
from unittest.mock import MagicMock, patch, AsyncMock
import asyncio

def test_rpc_url_masking():
    """Verify that RPC URLs are masked when converted to strings."""
    settings = Settings(
        DATABASE_HOST="localhost",
        DATABASE_NAME="test",
        DATABASE_USER="user",
        DATABASE_PASSWORD="password123",
        ETH_RPC_URL="https://eth-mainnet.alchemy.com/v2/secret_key"
    )

    rpc_url = settings.ETH_RPC_URL
    assert isinstance(rpc_url, SecretStr)

    # Check masking
    assert "secret_key" not in str(rpc_url)
    assert "**********" in str(rpc_url)

    # Check retrieval
    assert rpc_url.get_secret_value() == "https://eth-mainnet.alchemy.com/v2/secret_key"

@pytest.mark.asyncio
async def test_ingestor_handles_secret_rpc():
    """Verify that ChainIngestor correctly uses SecretStr RPC URL."""
    storage = MagicMock()
    chain_config = {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": SecretStr("https://secret.rpc.url")
    }

    ingestor = ChainIngestor(chain_config, storage)

    with patch("core.ingestor.AsyncHTTPProvider") as mock_provider, \
         patch("core.ingestor.AsyncWeb3") as mock_web3_cls:

        mock_web3 = mock_web3_cls.return_value
        mock_web3.is_connected = AsyncMock(return_value=True)

        f = asyncio.Future()
        f.set_result(1)
        mock_web3.eth.chain_id = f

        await ingestor.connect()

        # Verify that the actual URL was passed to the provider
        mock_provider.assert_called_once_with("https://secret.rpc.url")

@pytest.mark.asyncio
async def test_entity_clusterer_handles_secret_rpc():
    """Verify that EntityClusterer correctly uses SecretStr RPC URL."""
    storage = MagicMock()
    clusterer = EntityClusterer(storage)

    # Mock settings.rpc_urls to return a SecretStr
    with patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.get_chain_config") as mock_get_chain_config, \
         patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls, \
         patch("core.entity_clustering.AsyncWeb3.AsyncHTTPProvider") as mock_provider:

        mock_settings.rpc_urls = {1: SecretStr("https://secret.rpc.url")}
        mock_web3 = mock_web3_cls.return_value

        # We just need it to get past the AsyncWeb3 initialization in build_funding_graph
        # Mocking chain_id which is called early in build_funding_graph
        f_chain = asyncio.Future()
        f_chain.set_result(1)
        mock_web3.eth.chain_id = f_chain

        # Mocking block_number which is called early in build_funding_graph
        f = asyncio.Future()
        f.set_result(123)
        mock_web3.eth.block_number = f

        # Mock _node_supports_trace_filter to avoid more network calls
        clusterer._node_supports_trace_filter = AsyncMock(return_value=False)
        clusterer._fetch_funding_edges_block_scan = AsyncMock(return_value=[])

        await clusterer.build_funding_graph(1, ["0xaddr"], MagicMock())

        # Verify that the actual URL was passed to the provider
        mock_provider.assert_called_once_with("https://secret.rpc.url")
