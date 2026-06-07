
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage

@pytest.mark.asyncio
async def test_build_funding_graph_protocol_validation():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    # Mock settings to return an insecure protocol URL
    with patch("core.entity_clustering.settings") as mock_settings:
        mock_settings.rpc_urls = {1: "ftp://malicious-rpc.com"}

        with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
            await clusterer.build_funding_graph(
                chain_id=1,
                addresses=["0x" + "0" * 40],
                session=AsyncMock()
            )

@pytest.mark.asyncio
async def test_build_funding_graph_chain_id_verification():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    # Mock web3 to return a different chain ID
    mock_web3 = MagicMock()
    # In web3.py v6, eth.chain_id is an awaitable
    future = asyncio.Future()
    future.set_result(2) # Connected to Chain 2
    mock_web3.eth.chain_id = future

    with patch("core.entity_clustering.AsyncWeb3", return_value=mock_web3):
        with patch("core.entity_clustering.AsyncHTTPProvider"):
            with pytest.raises(ConnectionError, match="Chain ID mismatch"):
                await clusterer.build_funding_graph(
                    chain_id=1, # Expected Chain 1
                    addresses=["0x" + "0" * 40],
                    session=AsyncMock()
                )

@pytest.mark.asyncio
async def test_build_funding_graph_block_range_limit():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    # Mock web3
    mock_web3 = MagicMock()
    future_chain = asyncio.Future()
    future_chain.set_result(1)
    mock_web3.eth.chain_id = future_chain

    future_block = asyncio.Future()
    future_block.set_result(1000)
    mock_web3.eth.block_number = future_block

    with patch("core.entity_clustering.AsyncWeb3", return_value=mock_web3):
        with patch("core.entity_clustering.AsyncHTTPProvider"):
            # Attempting a 10M+1 block range
            with pytest.raises(ValueError, match="exceeds maximum of 10,000,000"):
                await clusterer.build_funding_graph(
                    chain_id=1,
                    addresses=["0x" + "0" * 40],
                    session=AsyncMock(),
                    from_block_override=100,
                    to_block_override=10_000_101
                )
