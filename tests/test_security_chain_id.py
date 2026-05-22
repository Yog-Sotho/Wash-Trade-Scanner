
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from core.ingestor import ChainIngestor
from core.storage import Storage

@pytest.fixture
def mock_chain_config():
    return {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
        "ws_url": "",
        "native_token": "ETH",
        "block_time": 12.0,
        "explorer_api": "",
        "dexes": [],
        "start_block": 0,
    }

@pytest.mark.asyncio
async def test_connect_invalid_protocol(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    mock_chain_config["rpc_url"] = "ftp://malicious.com/rpc"
    ingestor = ChainIngestor(mock_chain_config, storage)

    with pytest.raises(ValueError, match="only http/https allowed"):
        await ingestor.connect()

@pytest.mark.asyncio
async def test_connect_chain_id_mismatch(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    mock_chain_config["rpc_url"] = "http://localhost:8545"
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = MagicMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        # Mock eth.chain_id as an AsyncMock so it can be awaited
        mock_web3.eth.chain_id = AsyncMock(return_value=56)
        mock_web3_class.return_value = mock_web3

        with pytest.raises(ConnectionError, match="Chain ID mismatch"):
            await ingestor.connect()

@pytest.mark.asyncio
async def test_connect_success(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    mock_chain_config["rpc_url"] = "http://localhost:8545"
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = MagicMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        mock_web3.eth.chain_id = AsyncMock(return_value=1)
        mock_web3_class.return_value = mock_web3

        await ingestor.connect()
        assert ingestor.web3 is not None
