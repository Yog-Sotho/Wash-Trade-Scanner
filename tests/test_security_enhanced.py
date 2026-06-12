import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage

@pytest.mark.asyncio
async def test_build_funding_graph_protocol_validation():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    addresses = ["0x" + "1" * 40]

    # We want to test that it fails protocol validation BEFORE it attempts to connect
    # When chain_config['rpc_url'] is ftp://, it should raise ValueError

    with patch("core.entity_clustering.get_chain_config") as mock_get_config:
        mock_get_config.return_value = {
            "chain_id": 1,
            "rpc_url": "ftp://malicious-rpc.com"
        }

        # We need to make sure settings.rpc_urls.get(1) doesn't override it with a valid one
        with patch("core.entity_clustering.settings") as mock_settings:
            mock_settings.rpc_urls = {}

            with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
                await clusterer.build_funding_graph(1, addresses, MagicMock())

@pytest.mark.asyncio
async def test_build_funding_graph_chain_id_mismatch():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    addresses = ["0x" + "1" * 40]

    with patch("core.entity_clustering.get_chain_config") as mock_get_config:
        mock_get_config.return_value = {
            "chain_id": 1,
            "rpc_url": "http://localhost:8545"
        }

        with patch("core.entity_clustering.settings") as mock_settings:
            mock_settings.rpc_urls = {}

            with patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:
                mock_web3 = mock_web3_cls.return_value
                mock_web3.is_connected = AsyncMock(return_value=True)

                f = asyncio.Future()
                f.set_result(56) # Returns BSC instead of Ethereum
                mock_web3.eth.chain_id = f

                with pytest.raises(ConnectionError, match="Chain ID mismatch"):
                    await clusterer.build_funding_graph(1, addresses, MagicMock())

@pytest.mark.asyncio
async def test_build_funding_graph_dos_range_limit():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    addresses = ["0x" + "1" * 40]

    with patch("core.entity_clustering.get_chain_config") as mock_get_config:
        mock_get_config.return_value = {
            "chain_id": 1,
            "rpc_url": "http://localhost:8545"
        }

        with patch("core.entity_clustering.settings") as mock_settings:
            mock_settings.rpc_urls = {}

            with patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:
                mock_web3 = mock_web3_cls.return_value
                mock_web3.is_connected = AsyncMock(return_value=True)

                f = asyncio.Future()
                f.set_result(1)
                mock_web3.eth.chain_id = f

                mock_web3.eth.block_number = AsyncMock(return_value=20_000_000)

                # Range is 15M, exceeds 10M limit
                with pytest.raises(ValueError, match="exceeds maximum of 10,000,000"):
                    await clusterer.build_funding_graph(1, addresses, MagicMock(), from_block_override=0, to_block_override=15_000_000)
