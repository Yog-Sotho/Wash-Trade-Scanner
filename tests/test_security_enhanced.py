import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage

@pytest.mark.asyncio
async def test_build_funding_graph_block_range_limit():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    session = AsyncMock()

    with patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:
        mock_web3 = mock_web3_cls.return_value

        f_chain = asyncio.Future()
        f_chain.set_result(1)
        mock_web3.eth.chain_id = f_chain

        mock_web3.eth.block_number = AsyncMock(return_value=11_000_000)

        # 10,000,001 block range
        with pytest.raises(ValueError, match="Block range .* exceeds maximum of 10,000,000"):
            await clusterer.build_funding_graph(
                chain_id=1,
                addresses=["0x1234567890123456789012345678901234567890"],
                session=session,
                from_block_override=0,
                to_block_override=10_000_001
            )

@pytest.mark.asyncio
async def test_build_funding_graph_protocol_validation():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    session = AsyncMock()

    # We need to mock settings.rpc_urls to return None for chain_id=1
    # and mock get_chain_config to return an insecure URL
    with patch("core.entity_clustering.settings") as mock_settings, \
         patch("core.entity_clustering.get_chain_config") as mock_get_config:

        mock_settings.rpc_urls = {}
        mock_get_config.return_value = {
            "chain_id": 1,
            "name": "Ethereum",
            "rpc_url": "ftp://malicious-rpc.com"
        }

        with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
            await clusterer.build_funding_graph(
                chain_id=1,
                addresses=["0x1234567890123456789012345678901234567890"],
                session=session
            )

@pytest.mark.asyncio
async def test_build_funding_graph_chain_id_verification():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)
    session = AsyncMock()

    with patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:
        mock_web3 = mock_web3_cls.return_value

        # Node returns Chain ID 56 (BSC) instead of 1 (Ethereum)
        f_chain = asyncio.Future()
        f_chain.set_result(56)
        mock_web3.eth.chain_id = f_chain

        with pytest.raises(ConnectionError, match="Chain ID mismatch"):
            await clusterer.build_funding_graph(
                chain_id=1,
                addresses=["0x1234567890123456789012345678901234567890"],
                session=session,
                from_block_override=0,
                to_block_override=10
            )
