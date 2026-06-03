import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.entity_clustering import EntityClusterer
from core.storage import Storage

@pytest.mark.asyncio
async def test_build_funding_graph_dos_protection():
    storage = MagicMock(spec=Storage)
    clusterer = EntityClusterer(storage)

    # Mock settings and chain config
    with patch("core.entity_clustering.get_chain_config") as mock_get_config, \
         patch("core.entity_clustering.settings") as mock_settings:

        mock_get_config.return_value = {
            "name": "Ethereum",
            "rpc_url": "http://localhost:8545"
        }
        mock_settings.rpc_urls = {}

        # Mock Web3
        with patch("core.entity_clustering.AsyncHTTPProvider"), \
             patch("core.entity_clustering.AsyncWeb3") as mock_web3_cls:

            mock_web3 = mock_web3_cls.return_value
            mock_web3.is_connected = AsyncMock(return_value=True)

            f_chain_id = asyncio.Future()
            f_chain_id.set_result(1)
            mock_web3.eth.chain_id = f_chain_id

            # Test block range exceeding 10,000,000
            from_block = 1000
            to_block = 1000 + 10_000_001

            with pytest.raises(ValueError, match="exceeds maximum of 10,000,000"):
                await clusterer.build_funding_graph(
                    chain_id=1,
                    addresses=["0x123"],
                    session=MagicMock(),
                    from_block_override=from_block,
                    to_block_override=to_block
                )

            # Test block range within limit (exactly 10,000,000)
            to_block_ok = from_block + 10_000_000

            # We need to mock more things to let it complete or just verify it didn't raise the range error
            mock_web3.eth.block_number = AsyncMock(return_value=to_block_ok)
            clusterer._node_supports_trace_filter = AsyncMock(return_value=True)
            clusterer._fetch_funding_edges_trace_filter = AsyncMock(return_value=[])

            # This should NOT raise the block range ValueError
            await clusterer.build_funding_graph(
                chain_id=1,
                addresses=["0x123"],
                session=MagicMock(),
                from_block_override=from_block,
                to_block_override=to_block_ok
            )
