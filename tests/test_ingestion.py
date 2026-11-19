"""
Ingestion as a pipeline (roadmap Phase 5): batches, one full block per
block, skipping what is stored, shutdown, failures, shared rate limits.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config.settings import settings
from core import ingestor as ingestor_module
from core.exceptions import (
    AuditCancelledError,
    CircuitBreakerOpenError,
    IngestionError,
    RPCError,
)
from core.ingestor import (
    LOG_BATCH_BLOCKS,
    ChainIngestor,
    RateLimiter,
    chain_rate_limiter,
)
from core.pricing import PriceOracle
from core.storage import Storage

POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
TRADER = "0x" + "ab" * 20
ROUTER = "0x" + "7a" * 20


def tx_hash(n: int) -> str:
    return "0x" + f"{n:064x}"


def config() -> dict:
    return {
        "chain_id": 1,
        "name": "TestChain",
        "rpc_url": "http://localhost:8545",
        "dexes": [
            {
                "name": "TestDEX",
                "router": "0x1234",
                "factory": "0x5678",
                "event_sig": "Swap(address,uint256,uint256,uint256,uint256,address)",
                "abi": [{"name": "Swap", "type": "event", "inputs": []}],
                "type": "v2",
            }
        ],
    }


def swap(n: int, block: int, log_index: int = 0) -> dict:
    """A decoded V2 swap log (Chain.get_logs serves it without `args`)."""
    return {
        "address": POOL,
        "args": {
            "sender": ROUTER,
            "to": ROUTER,
            "amount0In": 100,
            "amount1In": 0,
            "amount0Out": 0,
            "amount1Out": 95,
        },
        "transactionHash": bytes.fromhex(tx_hash(n)[2:]),
        "blockNumber": block,
        "logIndex": log_index,
    }


class Chain:
    """A fake chain behind a ChainIngestor: swaps by block, answering
    eth_getLogs (web3.eth.get_logs) and eth_getBlockByNumber (raw
    provider.make_request) and counting each."""

    def __init__(self, swaps: list[dict], full_transactions: bool = True) -> None:
        self.swaps = swaps
        self.full_transactions = full_transactions
        self.log_ranges: list[tuple[int, int]] = []
        self.blocks_fetched: list[int] = []
        self.fail_logs: set[int] = set()  # batch start blocks
        self.fail_blocks: set[int] = set()
        self.decoded = 0

    async def get_logs(self, flt: dict) -> list[dict]:
        start, end = flt["fromBlock"], flt["toBlock"]
        self.log_ranges.append((start, end))
        if start in self.fail_logs:
            raise RPCError("query timeout")
        # As web3 formats them: hash bytes, integer index and block, no args.
        return [
            {k: s[k] for k in ("address", "transactionHash", "logIndex", "blockNumber")}
            for s in self.swaps
            if start <= s["blockNumber"] <= end
        ]

    def decode(self, raw: dict) -> dict:
        self.decoded += 1
        return next(
            s
            for s in self.swaps
            if s["transactionHash"] == raw["transactionHash"] and s["logIndex"] == raw["logIndex"]
        )

    async def make_request(self, method: str, params: list) -> dict:
        assert method == "eth_getBlockByNumber" and params[1] is settings.INGEST_FULL_BLOCKS
        number = int(params[0], 16)
        self.blocks_fetched.append(number)
        if number in self.fail_blocks:
            return {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "busy"}}
        txs = []
        for s in self.swaps:
            if s["blockNumber"] == number:
                h = tx_hash(int.from_bytes(s["transactionHash"], "big"))
                if self.full_transactions:
                    txs.append(
                        {"hash": h, "from": TRADER, "to": ROUTER, "gasPrice": hex(30 * 10**9)}
                    )
                else:
                    txs.append(h)
        txs.append(
            {"hash": tx_hash(10**9), "from": "0x" + "ee" * 20, "to": None, "gasPrice": "0x1"}
        )
        block = {"number": params[0], "timestamp": hex(1_700_000_000 + number), "transactions": txs}
        return {"jsonrpc": "2.0", "id": 1, "result": block}


def make_ingestor(chain: Chain, stored: set | None = None) -> tuple[ChainIngestor, AsyncMock]:
    storage = AsyncMock(spec=Storage)
    stored = stored or set()

    async def stored_trade_keys(keys):
        return {key for key in keys if key in stored}

    storage.stored_trade_keys.side_effect = stored_trade_keys
    ingestor = ChainIngestor(config(), storage, price_oracle=PriceOracle(None))
    ingestor.web3 = MagicMock()
    ingestor.web3.eth.get_logs = AsyncMock(side_effect=chain.get_logs)
    ingestor.web3.provider.make_request = AsyncMock(side_effect=chain.make_request)
    ingestor.web3.eth.get_transaction = AsyncMock(
        return_value={"from": TRADER, "to": ROUTER, "gasPrice": 30 * 10**9}
    )
    event = MagicMock()
    event.process_log.side_effect = chain.decode
    ingestor.web3.eth.contract.return_value.events.Swap.return_value = event
    ingestor._get_pool_tokens = AsyncMock(return_value=("0xt0", "0xt1"))  # type: ignore[method-assign]
    return ingestor, storage


def saved_trades(storage: AsyncMock) -> list[dict]:
    return [t for call in storage.save_trades_batch.await_args_list for t in call.args[0]]


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    monkeypatch.setattr(settings, "RPC_RETRIES", 1)
    monkeypatch.setattr(ingestor_module, "RETRY_BACKOFF_SECONDS", 0.0)


async def sync(ingestor: ChainIngestor, start: int, end: int, stop=None) -> int:
    return await ingestor.sync_historical_swaps(
        config()["dexes"][0], start, end, pool_address=POOL, stop=stop
    )


# ------------------------------------------------------------ one block, one request


async def test_one_request_per_block_gives_the_time_and_the_trader():
    chain = Chain([swap(1, 100, 0), swap(1, 100, 1), swap(2, 100, 2), swap(3, 101)])
    ingestor, storage = make_ingestor(chain)

    assert await sync(ingestor, 100, 101) == 4

    assert sorted(chain.blocks_fetched) == [100, 101]  # not one request per transaction
    ingestor.web3.eth.get_transaction.assert_not_awaited()
    trades = saved_trades(storage)
    assert {t["tx_from"] for t in trades} == {TRADER}
    assert {t["tx_to"] for t in trades} == {ROUTER}
    assert [t["gas_price"] for t in trades] == pytest.approx([30.0] * 4)
    stamps = {t["block_number"]: t["block_timestamp"] for t in trades}
    assert stamps[101].timestamp() - stamps[100].timestamp() == 1  # each block's own time


async def test_a_node_listing_only_hashes_falls_back_to_each_transaction():
    chain = Chain([swap(1, 100), swap(2, 100)], full_transactions=False)
    ingestor, storage = make_ingestor(chain)

    await sync(ingestor, 100, 100)

    assert ingestor.web3.eth.get_transaction.await_count == 2
    assert {t["tx_from"] for t in saved_trades(storage)} == {TRADER}


async def test_without_full_blocks_each_transaction_is_fetched(monkeypatch):
    """INGEST_FULL_BLOCKS=false: small block requests, one more per transaction."""
    monkeypatch.setattr(settings, "INGEST_FULL_BLOCKS", False)
    chain = Chain([swap(1, 100), swap(2, 100), swap(3, 101)], full_transactions=False)
    ingestor, storage = make_ingestor(chain)

    assert await sync(ingestor, 100, 101) == 3

    flags = {call.args[1][1] for call in ingestor.web3.provider.make_request.await_args_list}
    assert flags == {False}
    assert ingestor.web3.eth.get_transaction.await_count == 3
    assert {t["tx_from"] for t in saved_trades(storage)} == {TRADER}


async def test_a_block_that_cannot_be_fetched_fails_instead_of_inventing_a_time():
    """A failed block used to stamp its swaps with the current time."""
    chain = Chain([swap(1, 100), swap(2, 101)])
    chain.fail_blocks = {101}
    ingestor, storage = make_ingestor(chain)

    with pytest.raises(IngestionError, match="block 101"):
        await sync(ingestor, 100, 101)

    storage.save_trades_batch.assert_not_awaited()
    assert chain.blocks_fetched.count(101) == 2  # RPC_RETRIES + 1 attempts


async def test_block_requests_stay_within_the_concurrency_cap(monkeypatch):
    monkeypatch.setattr(settings, "RPC_CONCURRENCY", 3)
    chain = Chain([swap(n, 100 + n) for n in range(30)])
    ingestor, _ = make_ingestor(chain)  # built after the setting: its cap is 3
    in_flight = peak = 0

    async def slow(method, params):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.002)
        in_flight -= 1
        return await chain.make_request(method, params)

    ingestor.web3.provider.make_request = AsyncMock(side_effect=slow)
    assert await sync(ingestor, 100, 129) == 30
    assert peak == 3


async def test_an_open_breaker_is_not_retried():
    chain = Chain([swap(1, 100)])
    ingestor, _ = make_ingestor(chain)
    ingestor.circuit_breaker.call = AsyncMock(side_effect=CircuitBreakerOpenError("open"))

    with pytest.raises(CircuitBreakerOpenError):
        await sync(ingestor, 100, 100)

    assert ingestor.circuit_breaker.call.await_count == 1


# ------------------------------------------------------------ batches


async def test_the_range_is_fetched_and_saved_batch_by_batch():
    end = 2 * LOG_BATCH_BLOCKS + 500
    chain = Chain([swap(1, 10), swap(2, LOG_BATCH_BLOCKS + 10), swap(3, end)])
    ingestor, storage = make_ingestor(chain)

    assert await sync(ingestor, 1, end) == 3

    assert chain.log_ranges == [
        (1, LOG_BATCH_BLOCKS),
        (LOG_BATCH_BLOCKS + 1, 2 * LOG_BATCH_BLOCKS),
        (2 * LOG_BATCH_BLOCKS + 1, end),
    ]
    assert [len(c.args[0]) for c in storage.save_trades_batch.await_args_list] == [1, 1, 1]


async def test_log_batches_are_fetched_ahead_of_processing():
    chain = Chain([])
    ingestor, _ = make_ingestor(chain)
    in_flight = peak = 0

    async def slow_get_logs(_flt):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return []

    ingestor.web3.eth.get_logs = AsyncMock(side_effect=slow_get_logs)
    await sync(ingestor, 1, 10 * LOG_BATCH_BLOCKS)

    assert ingestor.web3.eth.get_logs.await_count == 10
    assert peak > 1


async def test_a_lost_log_batch_fails_the_sync_and_keeps_earlier_batches():
    """A batch failing every attempt used to be skipped: a silent gap."""
    chain = Chain([swap(1, 10), swap(2, LOG_BATCH_BLOCKS + 10), swap(3, 2 * LOG_BATCH_BLOCKS + 10)])
    chain.fail_logs = {LOG_BATCH_BLOCKS + 1}
    ingestor, storage = make_ingestor(chain)

    with pytest.raises(IngestionError, match=f"logs of blocks {LOG_BATCH_BLOCKS + 1}-"):
        await sync(ingestor, 1, 3 * LOG_BATCH_BLOCKS)

    assert [t["block_number"] for t in saved_trades(storage)] == [10]


# ------------------------------------------------------------ skipping stored trades


async def test_trades_stored_with_their_sender_are_not_fetched_again():
    chain = Chain([swap(1, 100), swap(2, 101), swap(3, 102)])
    stored = {(tx_hash(1), 0), (tx_hash(2), 0)}
    ingestor, storage = make_ingestor(chain, stored)

    assert await sync(ingestor, 100, 102) == 3  # stored ones count

    assert chain.blocks_fetched == [102]
    assert [t["transaction_hash"] for t in saved_trades(storage)] == [tx_hash(3)]


async def test_a_fully_stored_range_costs_only_its_log_requests():
    chain = Chain([swap(1, 100), swap(2, 101)])
    ingestor, storage = make_ingestor(chain, {(tx_hash(1), 0), (tx_hash(2), 0)})

    assert await sync(ingestor, 100, 101) == 2

    assert chain.blocks_fetched == []
    storage.save_trades_batch.assert_not_awaited()
    assert len(chain.log_ranges) == 1
    assert chain.decoded == 0  # skipped before decoding: most of a re-audit's CPU


async def test_only_the_new_logs_are_decoded():
    chain = Chain([swap(1, 100), swap(2, 100, 1), swap(3, 101)])
    ingestor, _ = make_ingestor(chain, {(tx_hash(1), 0)})

    assert await sync(ingestor, 100, 101) == 3
    assert chain.decoded == 2


# ------------------------------------------------------------ shutdown


async def test_shutdown_stops_before_the_next_batch_and_keeps_what_was_saved():
    chain = Chain([swap(1, 10), swap(2, LOG_BATCH_BLOCKS + 10)])
    ingestor, storage = make_ingestor(chain)
    stop = asyncio.Event()

    async def save_then_signal(trades):
        stop.set()  # SIGTERM arrives while the first batch is being saved
        return len(trades)

    storage.save_trades_batch.side_effect = save_then_signal

    with pytest.raises(AuditCancelledError, match=f"block {LOG_BATCH_BLOCKS + 1}"):
        await sync(ingestor, 1, 2 * LOG_BATCH_BLOCKS, stop=stop)

    assert [t["block_number"] for t in saved_trades(storage)] == [10]
    assert chain.blocks_fetched == [10]


async def test_the_audit_passes_its_shutdown_event_to_ingestion():
    from core.validators import AuditParameters
    from scripts.run_audit import AuditRunner

    runner = AuditRunner()
    runner.storage = AsyncMock(spec=Storage)
    params = AuditParameters(chain_id=1, pool_address=POOL, start_block=1, end_block=2)
    with (
        patch("scripts.run_audit.MultiChainIngestor") as multi,
        patch("scripts.run_audit.MLDetector"),
    ):
        multi.return_value.initialize = AsyncMock()
        multi.return_value.audit_pool = AsyncMock(side_effect=AuditCancelledError("stop"))
        with pytest.raises(AuditCancelledError):
            await runner.run_audit(params)

    assert multi.return_value.audit_pool.await_args.kwargs["stop"] is runner._shutdown_event


# ------------------------------------------------------------ DEXes in parallel


async def test_candidate_dexes_sync_concurrently_and_a_failure_cancels_the_rest():
    ingestor = ChainIngestor(config(), AsyncMock(spec=Storage))
    dexes = [{"name": "A"}, {"name": "B"}]
    ingestor.resolve_pool_dexes = AsyncMock(return_value=dexes)  # type: ignore[method-assign]
    started: list[str] = []
    cancelled: list[str] = []

    async def fake_sync(dex, *_args, **_kwargs):
        started.append(dex["name"])
        if dex["name"] == "A":
            await asyncio.sleep(0)
            raise IngestionError("A failed")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(dex["name"])
            raise
        return 1

    with (
        patch.object(ingestor, "sync_historical_swaps", side_effect=fake_sync),
        pytest.raises(IngestionError, match="A failed"),
    ):
        await asyncio.wait_for(ingestor.sync_pool(POOL, 1, 2), timeout=2)

    assert started == ["A", "B"]  # B started before A finished
    assert cancelled == ["B"]


# ------------------------------------------------------------ the rate limit


async def test_the_rate_limit_is_shared_by_every_ingestor_of_a_chain():
    a = ChainIngestor(config(), AsyncMock(spec=Storage))
    b = ChainIngestor(config(), AsyncMock(spec=Storage))
    other = ChainIngestor({**config(), "chain_id": 56}, AsyncMock(spec=Storage))

    assert a.rate_limiter is b.rate_limiter is chain_rate_limiter(1)
    assert other.rate_limiter is not a.rate_limiter


async def test_no_one_second_window_holds_more_than_the_limit():
    limiter = RateLimiter(max_calls_per_second=5)
    starts: list[float] = []

    async def call():
        await limiter.acquire()
        starts.append(time.monotonic())

    await asyncio.gather(*(call() for _ in range(12)))

    starts.sort()
    assert starts[-1] - starts[0] >= 2 * 0.95  # 12 calls at 5/s span two windows
    for i in range(len(starts) - 5):
        assert starts[i + 5] - starts[i] >= 0.95


def test_one_limiter_serves_several_event_loops():
    """Shared process-wide, so it must not bind to the loop that used it first
    (an asyncio.Lock would)."""
    limiter = RateLimiter(max_calls_per_second=1)
    asyncio.run(limiter.acquire())
    start = time.monotonic()
    asyncio.run(limiter.acquire())  # a different loop; waits for the window
    assert time.monotonic() - start >= 0.9
