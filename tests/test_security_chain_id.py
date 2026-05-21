import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from core.ingestor import ChainIngestor
from core.storage import Storage

@pytest.mark.asyncio
async def test_connect_protocol_validation():
    storage = MagicMock(spec=Storage)

    # Test invalid protocol (ftp)
    chain_config = {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": "ftp://localhost:8545"
    }
    ingestor = ChainIngestor(chain_config, storage)
    with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
        await ingestor.connect()

    # Test invalid protocol (javascript)
    chain_config["rpc_url"] = "javascript:alert(1)"
    ingestor = ChainIngestor(chain_config, storage)
    with pytest.raises(ValueError, match="Invalid RPC URL protocol"):
        await ingestor.connect()

    # Test valid protocol (http) - should proceed past protocol check
    chain_config["rpc_url"] = "http://localhost:8545"
    ingestor = ChainIngestor(chain_config, storage)
    with patch("core.ingestor.AsyncHTTPProvider"), \
         patch("core.ingestor.AsyncWeb3") as mock_web3_cls:

        mock_web3 = mock_web3_cls.return_value
        mock_web3.is_connected = AsyncMock(return_value=True)

        f = asyncio.Future()
        f.set_result(1)
        mock_web3.eth.chain_id = f

        await ingestor.connect() # Should not raise ValueError

@pytest.mark.asyncio
async def test_connect_chain_id_mismatch():
    storage = MagicMock(spec=Storage)
    chain_config = {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": "http://localhost:8545"
    }
    ingestor = ChainIngestor(chain_config, storage)

    with patch("core.ingestor.AsyncHTTPProvider"), \
         patch("core.ingestor.AsyncWeb3") as mock_web3_cls:

        mock_web3 = mock_web3_cls.return_value
        mock_web3.is_connected = AsyncMock(return_value=True)

        # Node returns Chain ID 56 (BSC) instead of 1 (Ethereum)
        f = asyncio.Future()
        f.set_result(56)
        mock_web3.eth.chain_id = f

        with pytest.raises(ConnectionError, match="Chain ID mismatch"):
            await ingestor.connect()

@pytest.mark.asyncio
async def test_connect_success():
    storage = MagicMock(spec=Storage)
    chain_config = {
        "chain_id": 1,
        "name": "Ethereum",
        "rpc_url": "https://eth-mainnet.example.com"
    }
    ingestor = ChainIngestor(chain_config, storage)

    with patch("core.ingestor.AsyncHTTPProvider"), \
         patch("core.ingestor.AsyncWeb3") as mock_web3_cls:

        mock_web3 = mock_web3_cls.return_value
        mock_web3.is_connected = AsyncMock(return_value=True)

        f = asyncio.Future()
        f.set_result(1)
        mock_web3.eth.chain_id = f

        await ingestor.connect()
        assert ingestor.web3 is not None
