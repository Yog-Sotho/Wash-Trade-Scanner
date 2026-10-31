"""
Unit tests for multi-chain ingestor.
"""

import asyncio
import time
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.ingestor import ChainIngestor, MultiChainIngestor, RateLimiter
from core.storage import Storage


@pytest.fixture
def mock_chain_config():
    # ChainIngestor is always constructed from the plain dicts in config.chains.CHAINS
    # (dict access like chain_config["rpc_url"]), not the unused ChainConfig dataclass.
    return {
        "chain_id": 1,
        "name": "TestChain",
        "rpc_url": "http://localhost:8545",
        "ws_url": "",
        "native_token": "TEST",
        "block_time": 12.0,
        "explorer_api": "",
        "start_block": 0,
        "dexes": [
            {
                "name": "TestDEX",
                "router": "0x1234",
                "factory": "0x5678",
                "event_sig": "Swap(address,uint256,uint256,uint256,uint256,address)",
                "abi": [],
                "type": "v2",
            }
        ],
    }


@pytest.mark.asyncio
async def test_chain_ingestor_connection(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        mock_web3_class.return_value = mock_web3

        await ingestor.connect()

        assert ingestor.web3 is not None
        mock_web3.is_connected.assert_called_once()


def _connected_web3_mock():
    web3 = AsyncMock()
    web3.is_connected = AsyncMock(return_value=True)
    web3.middleware_onion = MagicMock()  # .inject() is synchronous
    return web3


@pytest.mark.asyncio
async def test_multichain_initialize_connects_nothing_by_default():
    ingestor = MultiChainIngestor(AsyncMock(spec=Storage))

    with patch("core.ingestor.ChainIngestor") as mock_chain_ingestor:
        await ingestor.initialize()

    mock_chain_ingestor.assert_not_called()
    assert ingestor.ingestors == {}


@pytest.mark.asyncio
async def test_multichain_initialize_connects_only_requested_chains():
    ingestor = MultiChainIngestor(AsyncMock(spec=Storage))

    with patch("core.ingestor.ChainIngestor") as mock_chain_ingestor:
        mock_chain_ingestor.return_value.connect = AsyncMock()
        await ingestor.initialize([56])

    mock_chain_ingestor.assert_called_once()
    assert mock_chain_ingestor.call_args[0][0]["chain_id"] == 56
    assert list(ingestor.ingestors) == [56]


@pytest.mark.asyncio
async def test_get_ingestor_reuses_connection():
    ingestor = MultiChainIngestor(AsyncMock(spec=Storage))

    with patch("core.ingestor.ChainIngestor") as mock_chain_ingestor:
        mock_chain_ingestor.return_value.connect = AsyncMock()
        first = await ingestor.get_ingestor(56)
        second = await ingestor.get_ingestor(56)

    assert first is second
    mock_chain_ingestor.return_value.connect.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_ingestor_does_not_cache_failed_connection():
    ingestor = MultiChainIngestor(AsyncMock(spec=Storage))

    with patch("core.ingestor.ChainIngestor") as mock_chain_ingestor:
        mock_chain_ingestor.return_value.connect = AsyncMock(
            side_effect=[ConnectionError("down"), None]
        )
        with pytest.raises(ConnectionError):
            await ingestor.get_ingestor(56)
        assert ingestor.ingestors == {}

        await ingestor.get_ingestor(56)  # retried, now succeeds

    assert list(ingestor.ingestors) == [56]


@pytest.mark.asyncio
async def test_audit_connects_with_rpc_url_from_environment(monkeypatch):
    """Regression: ETH_RPC_URL used to be ignored in favour of a hardcoded
    YOUR_KEY placeholder, so no audit could run on any chain."""
    from config.settings import settings

    monkeypatch.setattr(settings, "ETH_RPC_URL", "https://eth.example.org/real-key")
    ingestor = MultiChainIngestor(AsyncMock(spec=Storage))

    with (
        patch("core.ingestor.AsyncHTTPProvider") as mock_provider,
        patch("core.ingestor.AsyncWeb3", return_value=_connected_web3_mock()),
    ):
        await ingestor.initialize([1])

    mock_provider.assert_called_once_with("https://eth.example.org/real-key")


@pytest.mark.asyncio
async def test_misconfigured_chain_does_not_break_other_chains(monkeypatch):
    """Regression: initialize() used to connect all 23 chains, so one bad
    endpoint aborted audits on every chain."""
    from config.settings import settings

    monkeypatch.setattr(settings, "ETH_RPC_URL", "https://eth.example.org/real-key")
    monkeypatch.setattr(settings, "BSC_RPC_URL", "https://bsc.example.org/YOUR_KEY")
    ingestor = MultiChainIngestor(AsyncMock(spec=Storage))

    with (
        patch("core.ingestor.AsyncHTTPProvider"),
        patch("core.ingestor.AsyncWeb3", return_value=_connected_web3_mock()),
    ):
        with pytest.raises(ValueError, match="BSC_RPC_URL"):
            await ingestor.get_ingestor(56)
        await ingestor.get_ingestor(1)

    assert list(ingestor.ingestors) == [1]


@pytest.mark.asyncio
async def test_multichain_audit_pool_unknown_chain():
    storage = AsyncMock(spec=Storage)
    ingestor = MultiChainIngestor(storage)

    with pytest.raises(ValueError, match="not configured"):
        await ingestor.audit_pool(chain_id=999, pool_address="0x" + "a" * 40)


@pytest.mark.asyncio
async def test_rate_limiter_allows_under_limit():
    limiter = RateLimiter(max_calls_per_second=5)
    start = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    assert time.monotonic() - start < 0.5


@pytest.mark.asyncio
async def test_rate_limiter_throttles_over_limit():
    limiter = RateLimiter(max_calls_per_second=2)
    start = time.monotonic()
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()  # 3rd call within the same second must wait
    assert time.monotonic() - start >= 0.9


@pytest.mark.asyncio
async def test_rate_limiter_concurrent_calls_are_serialized():
    limiter = RateLimiter(max_calls_per_second=100)
    await asyncio.gather(*(limiter.acquire() for _ in range(20)))
    assert len(limiter.calls) == 20


@pytest.mark.asyncio
async def test_process_swap_event_v2(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch.object(
        ingestor, "_get_pool_tokens", AsyncMock(return_value=("0xtoken0", "0xtoken1"))
    ):
        log = {
            "address": "0xPoolAddress",
            "args": {
                "sender": "0xSender",
                "to": "0xRecipient",
                "amount0In": 100,
                "amount1In": 0,
                "amount0Out": 0,
                "amount1Out": 95,
            },
            "transactionHash": b"\x01" * 32,
            "blockNumber": 12345,
            "logIndex": 3,
        }
        dex = mock_chain_config["dexes"][0]
        result = await ingestor._process_swap_event(dex, log, datetime(2024, 1, 1))

        assert result is not None
        assert result["amount_in"] == 100.0
        assert result["amount_out"] == 95.0
        assert result["sender"] == "0xsender"
        assert result["recipient"] == "0xrecipient"
        assert result["token_in"] == "0xtoken0"
        assert result["token_out"] == "0xtoken1"
        # Regression: the raw HexBytes was stored, which asyncpg rejects for
        # VARCHAR, so no swap could ever be saved to PostgreSQL.
        assert result["transaction_hash"] == "0x" + "01" * 32


@pytest.mark.asyncio
async def test_process_swap_event_unknown_dex_type(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)
    dex = {**mock_chain_config["dexes"][0], "type": "unknown"}
    log = {"address": "0xPool", "args": {}}

    result = await ingestor._process_swap_event(dex, log, datetime(2024, 1, 1))
    assert result is None


@pytest.mark.asyncio
async def test_process_swap_event_handles_malformed_log(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)
    dex = mock_chain_config["dexes"][0]

    result = await ingestor._process_swap_event(dex, None, datetime(2024, 1, 1))
    assert result is None


@pytest.mark.asyncio
async def test_connect_rejects_placeholder_rpc(mock_chain_config):
    mock_chain_config["rpc_url"] = "https://eth-mainnet.example.com/YOUR_KEY"
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)

    # The message must name the variable that actually fixes it.
    with pytest.raises(ValueError, match="placeholder.*ETH_RPC_URL"):
        await ingestor.connect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("amount0", "amount1", "expected"),
    [
        # Pool received 100 token0 and paid out 95 token1: trader sold token0.
        (100, -95, ("0xtoken0", 100.0, "0xtoken1", 95.0)),
        # Pool received 95 token1 and paid out 100 token0: trader sold token1.
        (-100, 95, ("0xtoken1", 95.0, "0xtoken0", 100.0)),
    ],
)
async def test_process_swap_event_v3_uses_pool_perspective(
    mock_chain_config, amount0, amount1, expected
):
    """Regression: V3 legs were inverted (the token the pool paid out was
    recorded as the trader's input)."""
    ingestor = ChainIngestor(mock_chain_config, AsyncMock(spec=Storage))
    dex = {**mock_chain_config["dexes"][0], "type": "v3"}
    log = {
        "address": "0xPool",
        "args": {
            "sender": "0xRouter",
            "recipient": "0xTrader",
            "amount0": amount0,
            "amount1": amount1,
        },
        "transactionHash": b"\x01" * 32,
        "blockNumber": 1,
        "logIndex": 0,
    }

    with patch.object(
        ingestor, "_get_pool_tokens", AsyncMock(return_value=("0xtoken0", "0xtoken1"))
    ):
        result = await ingestor._process_swap_event(dex, log, datetime(2024, 1, 1))

    assert result is not None
    token_in, amount_in, token_out, amount_out = expected
    assert result["token_in"] == token_in
    assert result["amount_in"] == amount_in
    assert result["token_out"] == token_out
    assert result["amount_out"] == amount_out


@pytest.mark.asyncio
async def test_connect_raises_when_not_connected(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=False)
        mock_web3_class.return_value = mock_web3

        with pytest.raises(ConnectionError):
            await ingestor.connect()


@pytest.mark.asyncio
async def test_connect_injects_poa_middleware_for_bsc(mock_chain_config):
    mock_chain_config["chain_id"] = 56
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)

    with patch("core.ingestor.AsyncWeb3") as mock_web3_class:
        mock_web3 = AsyncMock()
        mock_web3.is_connected = AsyncMock(return_value=True)
        mock_web3.middleware_onion = MagicMock()  # .inject() is synchronous
        mock_web3_class.return_value = mock_web3

        await ingestor.connect()

        mock_web3.middleware_onion.inject.assert_called_once()


@pytest.mark.asyncio
async def test_get_pool_tokens_caches_result(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)
    # web3.eth.contract(...) is synchronous in the real API, so the mock must be a
    # plain MagicMock (an AsyncMock would turn that call into an unawaited coroutine).
    ingestor.web3 = MagicMock()

    contract = MagicMock()
    contract.functions.token0.return_value.call = AsyncMock(return_value="0xTokenA")
    contract.functions.token1.return_value.call = AsyncMock(return_value="0xTokenB")
    ingestor.web3.eth.contract.return_value = contract

    addr = "0x" + "1" * 40
    token0, token1 = await ingestor._get_pool_tokens(addr)
    assert token0 == "0xtokena"
    assert token1 == "0xtokenb"

    # Second call must hit the cache, not the contract, again.
    ingestor.web3.eth.contract.reset_mock()
    token0_again, token1_again = await ingestor._get_pool_tokens(addr)
    assert (token0_again, token1_again) == (token0, token1)
    ingestor.web3.eth.contract.assert_not_called()


@pytest.mark.asyncio
async def test_get_pool_tokens_falls_back_on_rpc_failure(mock_chain_config):
    from core.exceptions import RPCError

    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)
    ingestor.web3 = MagicMock()

    contract = MagicMock()
    contract.functions.token0.return_value.call = AsyncMock(side_effect=RPCError("boom"))
    ingestor.web3.eth.contract.return_value = contract

    addr = "0x" + "2" * 40
    token0, token1 = await ingestor._get_pool_tokens(addr)
    assert token0 == addr.lower()
    assert token1 == addr.lower()


@pytest.mark.asyncio
async def test_sync_historical_swaps_decodes_and_saves(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)
    ingestor.web3 = MagicMock()
    ingestor.web3.eth.get_block = AsyncMock(return_value={"timestamp": 1700000000})

    swap_event_abi = [{"name": "Swap", "type": "event", "inputs": []}]
    mock_chain_config["dexes"][0]["abi"] = swap_event_abi

    raw_log = {"blockNumber": 100}
    decoded_log = {
        "address": "0xpool",
        "args": {
            "sender": "0xSender",
            "to": "0xRecipient",
            "amount0In": 100,
            "amount1In": 0,
            "amount0Out": 0,
            "amount1Out": 95,
        },
        "transactionHash": b"\x01" * 32,
        "blockNumber": 100,
        "logIndex": 0,
    }

    event = MagicMock()
    event.process_log.return_value = decoded_log
    contract = MagicMock()
    contract.events.Swap.return_value = event
    ingestor.web3.eth.contract.return_value = contract

    with (
        patch.object(ingestor, "_get_logs_batch", AsyncMock(return_value=[raw_log])),
        patch.object(
            ingestor, "_get_pool_tokens", AsyncMock(return_value=("0xtoken0", "0xtoken1"))
        ),
    ):
        dex = mock_chain_config["dexes"][0]
        synced = await ingestor.sync_historical_swaps(dex, from_block=1, to_block=100)

    assert synced == 1
    storage.save_trades_batch.assert_awaited_once()
    saved = storage.save_trades_batch.call_args[0][0]
    assert saved[0]["amount_in"] == 100.0
    assert saved[0]["sender"] == "0xsender"


@pytest.mark.asyncio
async def test_sync_historical_swaps_prices_trades_before_saving(mock_chain_config):
    """Regression: volume_usd was never populated, so every USD figure was 0."""
    from core.pricing import PriceOracle

    usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # Ethereum USDC, 6 decimals
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage, price_oracle=PriceOracle(None))
    ingestor.web3 = MagicMock()
    ingestor.web3.eth.get_block = AsyncMock(return_value={"timestamp": 1700000000})
    mock_chain_config["dexes"][0]["abi"] = [{"name": "Swap", "type": "event", "inputs": []}]

    event = MagicMock()
    event.process_log.return_value = {
        "address": "0xpool",
        "args": {
            "sender": "0xSender",
            "to": "0xRecipient",
            "amount0In": 250 * 10**6,
            "amount1In": 0,
            "amount0Out": 0,
            "amount1Out": 7,
        },
        "transactionHash": b"\x01" * 32,
        "blockNumber": 100,
        "logIndex": 0,
    }
    ingestor.web3.eth.contract.return_value.events.Swap.return_value = event

    with (
        patch.object(ingestor, "_get_logs_batch", AsyncMock(return_value=[{"blockNumber": 100}])),
        patch.object(ingestor, "_get_pool_tokens", AsyncMock(return_value=(usdc, "0xother"))),
    ):
        await ingestor.sync_historical_swaps(mock_chain_config["dexes"][0], 1, 100)

    saved = storage.save_trades_batch.call_args[0][0]
    assert saved[0]["amount_in_usd"] == pytest.approx(250.0)
    assert saved[0]["volume_usd"] == pytest.approx(250.0)
    assert saved[0]["amount_out_usd"] is None


@pytest.mark.asyncio
async def test_pool_address_is_checksummed_for_rpc_and_lowercased_for_storage(
    mock_chain_config,
):
    """Regression: web3 rejects lowercase addresses in eth_getLogs, so the
    real-time monitor (which passes lowercase) never ingested a swap; and pools
    were stored checksummed while the API queried lowercase, so the panel's
    pool views were always empty."""
    from core.pricing import PriceOracle

    checksummed = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage, price_oracle=PriceOracle(None))
    ingestor.web3 = MagicMock()
    ingestor.web3.eth.get_block = AsyncMock(return_value={"timestamp": 1700000000})
    mock_chain_config["dexes"][0]["abi"] = [{"name": "Swap", "type": "event", "inputs": []}]
    event = MagicMock()
    event.process_log.return_value = {
        "address": checksummed,  # web3 returns log addresses checksummed
        "args": {"sender": "0xS", "to": "0xR", "amount0In": 1, "amount1In": 0, "amount0Out": 0},
        "transactionHash": b"\x01" * 32,
        "blockNumber": 100,
        "logIndex": 0,
    }
    ingestor.web3.eth.contract.return_value.events.Swap.return_value = event
    get_logs = AsyncMock(return_value=[{"blockNumber": 100}])

    with (
        patch.object(ingestor, "_get_logs_batch", get_logs),
        patch.object(ingestor, "_get_pool_tokens", AsyncMock(return_value=("0xa", "0xb"))),
    ):
        await ingestor.sync_historical_swaps(
            mock_chain_config["dexes"][0], 1, 100, pool_address=checksummed.lower()
        )

    assert get_logs.call_args[0][0] == checksummed
    assert storage.save_trades_batch.call_args[0][0][0]["pool_address"] == checksummed.lower()


@pytest.mark.asyncio
async def test_sync_historical_swaps_no_logs_returns_zero(mock_chain_config):
    storage = AsyncMock(spec=Storage)
    ingestor = ChainIngestor(mock_chain_config, storage)
    ingestor.web3 = AsyncMock()

    with patch.object(ingestor, "_get_logs_batch", AsyncMock(return_value=[])):
        dex = mock_chain_config["dexes"][0]
        synced = await ingestor.sync_historical_swaps(dex, from_block=1, to_block=100)

    assert synced == 0
    storage.save_trades_batch.assert_not_awaited()
