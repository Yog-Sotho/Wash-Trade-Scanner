"""
Multi-chain swap event ingestor with circuit breaker protection.
"""

import asyncio
import contextlib
import logging
import time
from collections import defaultdict, deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeVar, cast

from web3 import AsyncWeb3, Web3
from web3.middleware import async_geth_poa_middleware  # type: ignore[attr-defined]
from web3.types import EventData, FilterParams, LogReceipt, RPCEndpoint

from config.chains import get_chain_config
from config.settings import settings
from core.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, shared_breaker
from core.exceptions import AuditCancelledError, CircuitBreakerOpenError, IngestionError, RPCError
from core.pricing import PriceOracle
from core.rpc import InstrumentedHTTPProvider
from core.storage import Storage

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Blocks per eth_getLogs request: hosted nodes cap a request's block range or
# result size, and 1,000 blocks of one pool's swaps stays under both.
LOG_BATCH_BLOCKS = 1000
# eth_getLogs requests fetched ahead of the batch being processed.
LOG_PREFETCH_BATCHES = 4
# First retry delay; it doubles with each further retry.
RETRY_BACKOFF_SECONDS = 1.0

# A transaction's (from, to, gas price in gwei).
TxInfo = tuple[str, str | None, float | None]


@dataclass(frozen=True, slots=True)
class _Block:
    timestamp: datetime  # naive UTC, as stored
    transactions: dict[str, TxInfo]  # only the ones asked for, by hash


def _quantity(value: Any) -> int:
    """A JSON-RPC quantity ("0x1a") as an int."""
    return int(value, 16) if isinstance(value, str) else int(value)


async def _gather_or_cancel(awaitables: Iterable[Awaitable[T]]) -> list[T]:
    """Await all; on the first failure cancel the others and raise it."""
    tasks = [asyncio.ensure_future(aw) for aw in awaitables]
    try:
        return list(await asyncio.gather(*tasks))
    finally:
        pending = [task for task in tasks if not task.done()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in tasks:  # retrieved, so none is reported "never retrieved"
            if not task.cancelled():
                task.exception()


def _check_stop(stop: asyncio.Event | None, what: str) -> None:
    if stop is not None and stop.is_set():
        raise AuditCancelledError(f"Shutdown requested; stopped before {what}")


def _hex_str(value: Any) -> str:
    """Normalize a hash to the stored form: a lowercase 0x-prefixed str.

    web3 returns hashes as HexBytes, which asyncpg rejects for VARCHAR columns.
    Web3.to_hex is used rather than HexBytes.hex(), whose 0x prefix was dropped
    in hexbytes 1.0.
    """
    if isinstance(value, bytes | bytearray):
        return Web3.to_hex(bytes(value))
    return str(value).lower()


class RateLimiter:
    """At most `max_calls_per_second` calls start in any one-second window.

    acquire() reserves the earliest slot the window allows, then sleeps until
    it. Reserving is synchronous, so concurrent callers need no lock (none is
    held while one sleeps) and the limiter is tied to no event loop: one
    instance serves every audit and monitor in the process (chain_rate_limiter).
    """

    def __init__(self, max_calls_per_second: int):
        self.max_calls = max_calls_per_second
        # The latest `max_calls` reserved slots (some may be in the future).
        self.calls: deque[float] = deque(maxlen=max_calls_per_second)

    def _reserve(self) -> float:
        now = time.monotonic()
        slot = now
        if len(self.calls) == self.max_calls:
            slot = max(now, self.calls[0] + 1.0)
        self.calls.append(slot)
        return slot - now

    async def acquire(self) -> None:
        wait = self._reserve()
        if wait > 0:
            await asyncio.sleep(wait)


_rate_limiters: dict[int, RateLimiter] = {}


def chain_rate_limiter(chain_id: int) -> RateLimiter:
    """The process-wide limiter for a chain's RPC endpoint (RPC_RATE_LIMIT).

    RPC_RATE_LIMIT is the endpoint's budget, so it is shared: two audits and a
    monitor of one chain together stay under it, rather than each using all of
    it.
    """
    limiter = _rate_limiters.get(chain_id)
    if limiter is None:
        limiter = _rate_limiters[chain_id] = RateLimiter(settings.RPC_RATE_LIMIT)
    return limiter


def reset_rate_limiters() -> None:
    """Forget every shared limiter (tests)."""
    _rate_limiters.clear()


def chain_breaker(chain_id: int) -> CircuitBreaker:
    """The process-wide breaker for a chain's RPC endpoint.

    One per chain for the whole process: audits, monitors and readiness probes
    calling the same endpoint share its health (core/circuit_breaker.py).
    """
    return shared_breaker(
        f"rpc_{chain_id}",
        CircuitBreakerConfig(
            failure_threshold=settings.RPC_MAX_FAILURES,
            recovery_timeout=settings.RPC_RECOVERY_TIMEOUT,
        ),
    )


class ChainIngestor:
    """Per-chain blockchain data ingestor."""

    def __init__(
        self,
        chain_config: dict[str, Any],
        storage: Storage,
        price_oracle: PriceOracle | None = None,
    ):
        self.chain_config = chain_config
        self.storage = storage
        self.price_oracle = price_oracle or PriceOracle.from_settings()
        self.web3: AsyncWeb3 | None = None
        self.rate_limiter = chain_rate_limiter(chain_config["chain_id"])
        self.circuit_breaker = chain_breaker(chain_config["chain_id"])
        # Requests this ingestor has in flight at once; the rate limiter caps
        # their rate.
        self._rpc_slots = asyncio.Semaphore(settings.RPC_CONCURRENCY)
        self.is_running = False
        self.latest_block = 0
        self.contracts: dict[str, Any] = {}
        self._pool_tokens: dict[str, tuple[str, str]] = {}
        self._pool_tokens_lock = asyncio.Lock()
        self._pool_dexes: dict[str, list[dict[str, Any]]] = {}

    async def connect(self) -> None:
        """Connect to RPC with validation."""
        rpc = self.chain_config["rpc_url"]
        if "YOUR_KEY" in rpc or "placeholder" in rpc.lower():
            env_var = settings.RPC_URL_ENV_VARS.get(
                self.chain_config["chain_id"], "this chain's RPC URL setting"
            )
            raise ValueError(
                f"RPC URL for {self.chain_config['name']} contains a placeholder. "
                f"Set {env_var} in the environment to a real endpoint."
            )

        provider = InstrumentedHTTPProvider(rpc, self.chain_config["chain_id"])
        self.web3 = AsyncWeb3(provider)

        if self.chain_config.get("chain_id") in (56, 97):
            self.web3.middleware_onion.inject(async_geth_poa_middleware, layer=0)

        try:
            connected = await self.circuit_breaker.call(self.web3.is_connected)
            if not connected:
                raise ConnectionError(f"Failed to connect to {self.chain_config['name']}")
            logger.info(f"Connected to {self.chain_config['name']} via HTTP")
        except CircuitBreakerOpenError as exc:
            raise ConnectionError(f"Circuit breaker open for {self.chain_config['name']}") from exc

    async def _get_pool_tokens(self, pool_address: str) -> tuple[str, str]:
        """Fetch token0 and token1 from pool contract with caching."""
        assert self.web3 is not None
        addr = Web3.to_checksum_address(pool_address)
        async with self._pool_tokens_lock:
            if addr in self._pool_tokens:
                return self._pool_tokens[addr]

        pool_abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "token0",
                "outputs": [{"name": "", "type": "address"}],
                "type": "function",
            },
            {
                "constant": True,
                "inputs": [],
                "name": "token1",
                "outputs": [{"name": "", "type": "address"}],
                "type": "function",
            },
        ]
        contract = self.web3.eth.contract(address=addr, abi=pool_abi)

        try:
            token0 = await self.circuit_breaker.call(contract.functions.token0().call)
            token1 = await self.circuit_breaker.call(contract.functions.token1().call)
        except (RPCError, CircuitBreakerOpenError) as exc:
            logger.error(f"Failed to fetch tokens for {addr}: {exc}")
            token0 = addr
            token1 = addr

        token0 = token0.lower()
        token1 = token1.lower()

        async with self._pool_tokens_lock:
            self._pool_tokens[addr] = (token0, token1)

        return token0, token1

    async def _with_retries(self, what: str, call: Callable[[], Awaitable[T]]) -> T:
        """`call()` through the concurrency cap, the rate limiter and the
        breaker, with RPC_RETRIES retries after 1, 2, 4... seconds
        (RETRY_BACKOFF_SECONDS doubling). An open
        breaker is not retried. Raises IngestionError once every attempt failed.
        """
        attempts = settings.RPC_RETRIES + 1
        error: Exception | None = None
        for attempt in range(attempts):
            if attempt:
                await asyncio.sleep(RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1))
            try:
                async with self._rpc_slots:
                    await self.rate_limiter.acquire()
                    result: T = await self.circuit_breaker.call(call)
                    return result
            except CircuitBreakerOpenError:
                raise
            except Exception as exc:
                error = exc
                logger.warning(f"Fetching {what} failed (attempt {attempt + 1}/{attempts}): {exc}")
        raise IngestionError(
            f"Could not fetch {what} after {attempts} attempt(s): {error}"
        ) from error

    async def _request(self, method: str, params: list[Any]) -> Any:
        """A raw JSON-RPC call: the result as the node sent it, without web3's
        result formatting (for a block of a few hundred transactions, that
        formatting costs more CPU than the request)."""
        assert self.web3 is not None
        response = await self.web3.provider.make_request(RPCEndpoint(method), params)
        error = response.get("error")
        if error:
            message = error.get("message") if isinstance(error, dict) else error
            raise RPCError(f"{method}: {message}")
        return response.get("result")

    async def _get_logs_batch(
        self, address: str, topics: list[str], from_block: int, to_block: int
    ) -> list[LogReceipt]:
        """One eth_getLogs request (at most LOG_BATCH_BLOCKS blocks), retried.

        Raises IngestionError when every attempt fails: a range whose logs are
        missing must fail the sync, not leave a silent gap in the trades.
        """
        assert self.web3 is not None
        web3 = self.web3
        log_filter = cast(
            FilterParams,
            {"address": address, "topics": topics, "fromBlock": from_block, "toBlock": to_block},
        )
        logs = await self._with_retries(
            f"logs of blocks {from_block}-{to_block}", lambda: web3.eth.get_logs(log_filter)
        )
        logger.debug(f"Fetched {len(logs)} logs from {from_block} to {to_block}")
        return list(logs)

    async def _log_batches(
        self, address: str, topics: list[str], from_block: int, to_block: int
    ) -> AsyncGenerator[tuple[int, int, list[LogReceipt]], None]:
        """(first block, last block, logs) per LOG_BATCH_BLOCKS-block batch, in
        order, with up to LOG_PREFETCH_BATCHES requests fetched ahead."""
        starts = iter(range(from_block, to_block + 1, LOG_BATCH_BLOCKS))
        pending: deque[tuple[int, int, asyncio.Future[list[LogReceipt]]]] = deque()

        def fetch_next() -> None:
            start = next(starts, None)
            if start is not None:
                end = min(start + LOG_BATCH_BLOCKS - 1, to_block)
                task = asyncio.ensure_future(self._get_logs_batch(address, topics, start, end))
                pending.append((start, end, task))

        for _ in range(LOG_PREFETCH_BATCHES):
            fetch_next()
        try:
            while pending:
                start, end, task = pending.popleft()
                logs = await task
                fetch_next()
                yield start, end, logs
        finally:
            for *_, task in pending:
                task.cancel()
            await asyncio.gather(*(task for *_, task in pending), return_exceptions=True)

    async def _process_swap_event(
        self,
        dex: dict[str, Any],
        log: EventData,
        block_timestamp: datetime,
    ) -> dict[str, Any] | None:
        """Process a single swap event log."""
        try:
            event_type = dex["type"]
            args = log.get("args", {})

            sender = args.get("sender", args.get("buyer", log.get("address", "")))
            recipient = args.get("to", args.get("recipient", args.get("buyer", "")))
            amount_in = 0.0
            amount_out = 0.0
            token_in: str = log.get("address", "")
            token_out: str = log.get("address", "")

            if event_type == "v2":
                amount0In = float(args.get("amount0In", 0))
                amount1In = float(args.get("amount1In", 0))
                amount0Out = float(args.get("amount0Out", 0))
                amount1Out = float(args.get("amount1Out", 0))
                if amount0In > 0:
                    amount_in = amount0In
                    amount_out = amount1Out
                else:
                    amount_in = amount1In
                    amount_out = amount0Out

            elif event_type == "v3":
                # amount0/amount1 are deltas of the *pool's* balances: the positive
                # one is what the pool received (the trader's input), the negative
                # one what it paid out (UniswapV3Pool.swap: for zeroForOne, amount0
                # > 0 is pulled in and token1 is sent for -amount1).
                amount0 = float(args.get("amount0", 0))
                amount1 = float(args.get("amount1", 0))
                if amount0 > 0:
                    amount_in = amount0
                    amount_out = abs(amount1)
                else:
                    amount_in = amount1
                    amount_out = abs(amount0)

            elif event_type == "curve":
                amount_in = float(args.get("tokens_sold", 0))
                amount_out = float(args.get("tokens_bought", 0))

            elif event_type == "balancer":
                amount_in = float(args.get("amountIn", 0))
                amount_out = float(args.get("amountOut", 0))
                token_in = args.get("tokenIn", "")
                token_out = args.get("tokenOut", "")

            elif event_type == "syncswap":
                amount_in = float(args.get("amountIn", 0))
                amount_out = float(args.get("amountOut", 0))
                token_in = args.get("tokenIn", "")
                token_out = args.get("tokenOut", "")
                sender = args.get("sender", sender)
                recipient = args.get("to", recipient)

            else:
                logger.warning(f"Unknown DEX type {event_type}")
                return None

            if event_type in ("v2", "v3", "curve"):
                try:
                    token0, token1 = await self._get_pool_tokens(log.get("address", ""))
                except Exception:
                    token0 = token_in
                    token1 = token_out

                if event_type == "v2":
                    if amount_in == float(args.get("amount0In", 0)):
                        token_in = token0
                        token_out = token1
                    else:
                        token_in = token1
                        token_out = token0
                elif event_type == "v3":
                    if float(args.get("amount0", 0)) > 0:
                        token_in = token0
                        token_out = token1
                    else:
                        token_in = token1
                        token_out = token0

            return {
                "chain_id": self.chain_config["chain_id"],
                "dex_name": dex["name"],
                "pool_address": log.get("address", "").lower(),
                "token_in": token_in.lower(),
                "token_out": token_out.lower(),
                "amount_in": amount_in,
                "amount_out": amount_out,
                "sender": sender.lower(),
                "recipient": recipient.lower(),
                "transaction_hash": _hex_str(log.get("transactionHash", "")),
                "block_number": log.get("blockNumber", 0),
                "block_timestamp": block_timestamp,
                "log_index": log.get("logIndex", 0),
                "tx_from": None,
                "tx_to": None,
                "gas_price": None,
                "gas_used": None,
            }

        except Exception as exc:
            logger.error(f"Error processing swap event: {exc}")
            return None

    async def _fetch_block(self, number: int, tx_hashes: set[str]) -> _Block:
        """A block's timestamp and the given transactions' (from, to, gas
        price), in one request: eth_getBlockByNumber with full transaction
        objects. Fetching each transaction instead costs one more request per
        transaction, three in four of an audit's requests. With
        INGEST_FULL_BLOCKS off the block lists only hashes, and the
        transactions are fetched one by one (_fetch_transactions)."""

        full = settings.INGEST_FULL_BLOCKS

        async def get() -> dict[str, Any]:
            block = await self._request("eth_getBlockByNumber", [hex(number), full])
            if not isinstance(block, dict):
                raise RPCError(f"block {number} not found")
            return block

        block = await self._with_retries(f"block {number}", get)
        transactions: dict[str, TxInfo] = {}
        for tx in block.get("transactions") or []:
            # A node that ignores the flag lists bare hashes; those transactions
            # are then fetched one by one (_fetch_transactions).
            if not isinstance(tx, dict) or not tx.get("from"):
                continue
            tx_hash = str(tx.get("hash", "")).lower()
            if tx_hash in tx_hashes:
                to, gas_price = tx.get("to"), tx.get("gasPrice")
                transactions[tx_hash] = (
                    str(tx["from"]).lower(),
                    str(to).lower() if to else None,
                    _quantity(gas_price) / 1e9 if gas_price is not None else None,
                )
        timestamp = datetime.fromtimestamp(_quantity(block["timestamp"]), tz=UTC)
        return _Block(timestamp.replace(tzinfo=None), transactions)

    async def _fetch_blocks(self, wanted: dict[int, set[str]]) -> dict[int, _Block]:
        """_fetch_block for each block number -> transaction hashes in `wanted`,
        concurrently (RPC_CONCURRENCY at a time). A block that cannot be
        fetched fails the batch: its swaps would have no time."""
        blocks: dict[int, _Block] = {}

        async def fetch(number: int) -> None:
            blocks[number] = await self._fetch_block(number, wanted[number])

        await _gather_or_cancel(fetch(number) for number in sorted(wanted))
        return blocks

    async def _fetch_transactions(self, tx_hashes: set[str]) -> dict[str, TxInfo]:
        """Fetch (from, to, gas price in gwei) for each transaction, one request
        each: the fallback for transactions their block did not include.

        Swap events name the router or bot contract that called the pool, not
        the account that traded; only the transaction carries that. A
        transaction that cannot be fetched is left out (its swaps keep a NULL
        tx_from, which the next audit fills in) rather than failing the sync.
        """
        assert self.web3 is not None
        web3 = self.web3
        results: dict[str, TxInfo] = {}

        async def fetch(tx_hash: str) -> None:
            async with self._rpc_slots:
                try:
                    await self.rate_limiter.acquire()
                    tx = await self.circuit_breaker.call(web3.eth.get_transaction, tx_hash)
                except Exception as exc:
                    logger.error(f"Failed to fetch transaction {tx_hash}: {exc}")
                    return
            to = tx.get("to")
            gas_price = tx.get("gasPrice")
            results[tx_hash] = (
                str(tx["from"]).lower(),
                str(to).lower() if to else None,
                gas_price / 1e9 if gas_price is not None else None,
            )

        await asyncio.gather(*(fetch(tx_hash) for tx_hash in sorted(tx_hashes)))
        if len(results) < len(tx_hashes):
            logger.warning(
                f"{len(tx_hashes) - len(results)} of {len(tx_hashes)} transactions could not "
                "be fetched; their swaps have no tx_from"
            )
        return results

    async def sync_historical_swaps(
        self,
        dex: dict[str, Any],
        from_block: int,
        to_block: int,
        pool_address: str | None = None,
        stop: asyncio.Event | None = None,
    ) -> int:
        """Ingest a DEX's swaps in [from_block, to_block]; returns how many the
        range holds (already stored ones included).

        The range is processed LOG_BATCH_BLOCKS blocks at a time, each batch
        saved before the next starts, so memory stays bounded and an audit
        that stops early (shutdown via `stop`, or a range that cannot be
        fetched) keeps what it saved: the next audit skips it.
        """
        logger.info(f"Syncing historical swaps for {dex['name']} on {self.chain_config['name']}")
        assert self.web3 is not None
        topics = [Web3.keccak(text=dex["event_sig"]).hex()]
        # web3 rejects non-checksummed addresses in eth_getLogs filters, while
        # storage and callers use lowercase.
        address = Web3.to_checksum_address(pool_address) if pool_address else dex["router"]
        web3 = self.web3
        event: Any = None

        def decoder() -> Callable[[LogReceipt], EventData]:
            # eth_getLogs returns raw, undecoded logs (no `args`): each is
            # decoded against the DEX's event ABI. Built on first use.
            nonlocal event
            if event is None:
                decode_contract = web3.eth.contract(abi=dex["abi"])
                event = getattr(decode_contract.events, dex["abi"][0]["name"])()
            process_log: Callable[[LogReceipt], EventData] = event.process_log
            return process_log

        found = 0
        batches = self._log_batches(address, topics, from_block, to_block)
        async with contextlib.aclosing(batches):
            async for start, end, raw_logs in batches:
                _check_stop(stop, f"block {start} ({found} swaps up to there are saved)")
                found += await self._ingest_logs(dex, raw_logs, decoder)
                logger.debug(f"{dex['name']}: blocks {start}-{end} done, {found} swaps so far")

        logger.info(f"Synced {found} historical swaps for {dex['name']}")
        return found

    async def _ingest_logs(
        self,
        dex: dict[str, Any],
        raw_logs: list[LogReceipt],
        decoder: Callable[[], Callable[[LogReceipt], EventData]],
    ) -> int:
        """Store one batch's swaps; returns how many it holds.

        Swaps already stored with their transaction's sender are skipped
        before anything else, decoding included: saving them again would
        change nothing (save_trades_batch only fills in a missing tx_from), and
        fetching their blocks is most of an audit's RPC work, so a re-audit
        costs one eth_getLogs per batch.
        """
        raw_logs = [log for log in raw_logs if log.get("blockNumber")]
        if not raw_logs:
            return 0
        keys = [
            (_hex_str(log.get("transactionHash", "")), int(log.get("logIndex", 0)))
            for log in raw_logs
        ]
        stored = await self.storage.stored_trade_keys(keys)
        pending = [
            (raw_log, key[0])
            for raw_log, key in zip(raw_logs, keys, strict=True)
            if key not in stored
        ]
        new: list[tuple[EventData, str]] = []
        if pending:
            decode = decoder()
            for raw_log, tx_hash in pending:
                try:
                    new.append((decode(raw_log), tx_hash))
                except Exception as exc:
                    logger.error(
                        f"Failed to decode log at block {raw_log.get('blockNumber')}: {exc}"
                    )
        if not new:
            return sum(key in stored for key in keys)

        wanted: dict[int, set[str]] = defaultdict(set)
        for log, tx_hash in new:
            wanted[log["blockNumber"]].add(tx_hash)
        blocks = await self._fetch_blocks(wanted)

        trades: list[dict[str, Any]] = []
        for log, _ in new:
            trade = await self._process_swap_event(dex, log, blocks[log["blockNumber"]].timestamp)
            if trade:
                trades.append(trade)

        missing = {
            t["transaction_hash"]
            for t in trades
            if t["transaction_hash"] not in blocks[t["block_number"]].transactions
        }
        fallback = await self._fetch_transactions(missing) if missing else {}
        for trade in trades:
            tx_hash = trade["transaction_hash"]
            tx = blocks[trade["block_number"]].transactions.get(tx_hash) or fallback.get(tx_hash)
            if tx is not None:
                trade["tx_from"], trade["tx_to"], trade["gas_price"] = tx

        if trades:
            await self.price_oracle.price_trades(self.chain_config["chain_id"], trades)
            await self.storage.save_trades_batch(trades)
        return sum(key in stored for key in keys) + len(trades)

    async def resolve_pool_dexes(self, pool_address: str) -> list[dict[str, Any]]:
        """The configured DEX(es) whose events this pool emits.

        DEXes that share an event signature (e.g. Uniswap V2 and Sushiswap)
        would otherwise each fetch, price and count the pool's swaps once
        more, under a DEX label that depends on list order. A V2/V3-style pool
        names its factory, which identifies the DEX exactly. Pools without a
        recognised factory fall back to one DEX per distinct event signature.
        """
        assert self.web3 is not None
        cached = self._pool_dexes.get(pool_address.lower())
        if cached is not None:
            return cached
        resolved, by_factory = await self._resolve_pool_dexes(pool_address)
        if by_factory:
            # Only an answer from the pool itself is final; a fallback caused
            # by a failed (or unsupported) factory() call is retried next time.
            self._pool_dexes[pool_address.lower()] = resolved
        return resolved

    async def _resolve_pool_dexes(self, pool_address: str) -> tuple[list[dict[str, Any]], bool]:
        assert self.web3 is not None
        dexes: list[dict[str, Any]] = self.chain_config["dexes"]
        factory_abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "factory",
                "outputs": [{"name": "", "type": "address"}],
                "type": "function",
            }
        ]
        contract = self.web3.eth.contract(
            address=Web3.to_checksum_address(pool_address), abi=factory_abi
        )
        answered = False
        try:
            await self.rate_limiter.acquire()
            factory = str(await self.circuit_breaker.call(contract.functions.factory().call))
        except Exception as exc:
            logger.debug(f"Pool {pool_address} has no factory(): {exc}")
        else:
            answered = True
            matches = [d for d in dexes if str(d.get("factory", "")).lower() == factory.lower()]
            if matches:
                return matches[:1], True
            logger.warning(
                f"Pool {pool_address} was created by unconfigured factory {factory}; "
                "trying each configured event signature once"
            )

        unique: dict[str, dict[str, Any]] = {}
        for dex in dexes:
            unique.setdefault(dex["event_sig"], dex)
        return list(unique.values()), answered

    async def sync_pool(
        self,
        pool_address: str,
        from_block: int,
        to_block: int,
        stop: asyncio.Event | None = None,
    ) -> int:
        """Sync one pool's swaps from the DEX that actually created it (from
        each candidate DEX concurrently, when the factory is unknown)."""
        dexes = await self.resolve_pool_dexes(pool_address)
        counts = await _gather_or_cancel(
            self.sync_historical_swaps(
                dex, from_block, to_block, pool_address=pool_address, stop=stop
            )
            for dex in dexes
        )
        return sum(counts)

    async def stop(self) -> None:
        self.is_running = False


class MultiChainIngestor:
    """Manages per-chain ingestors, connecting each chain only when it is needed.

    Chains are connected lazily on first use rather than all at once: an audit
    of one chain must not depend on the reachability or configuration of the
    other 22.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.ingestors: dict[int, ChainIngestor] = {}
        self._connect_lock = asyncio.Lock()

    async def initialize(self, chain_ids: Iterable[int] = ()) -> None:
        """Eagerly connect the given chains, failing fast on a bad endpoint.

        With no arguments nothing is connected; chains connect on first use.
        """
        for chain_id in chain_ids:
            await self.get_ingestor(chain_id)

    async def get_ingestor(self, chain_id: int) -> ChainIngestor:
        """Return a connected ingestor for `chain_id`, connecting it if needed.

        A failed connection is not cached, so a later call retries.
        """
        async with self._connect_lock:
            existing = self.ingestors.get(chain_id)
            if existing is not None:
                return existing
            try:
                chain_config = get_chain_config(chain_id)
            except ValueError as exc:
                raise ValueError(f"Chain {chain_id} not configured") from exc
            ingestor = ChainIngestor(chain_config, self.storage)
            await ingestor.connect()
            self.ingestors[chain_id] = ingestor
            return ingestor

    async def audit_pool(
        self,
        chain_id: int,
        pool_address: str,
        start_block: int | None = None,
        end_block: int | None = None,
        stop: asyncio.Event | None = None,
    ) -> int:
        """Sync trades for a specific pool; `stop`, once set, ends the sync
        before its next batch (AuditCancelledError)."""
        ingestor = await self.get_ingestor(chain_id)
        assert ingestor.web3 is not None, "ingestor.connect() must be called first"

        chain_config = ingestor.chain_config

        if start_block is None:
            start_block = chain_config.get("start_block", 0)

        if end_block is None:
            try:
                await ingestor.rate_limiter.acquire()
                # web3.eth.block_number is an async property, not a callable: accessing
                # it already returns a coroutine, so it must be wrapped in a zero-arg
                # callable for CircuitBreaker.call() (which invokes `coro(*args)` itself).
                web3 = ingestor.web3
                end_block = await ingestor.circuit_breaker.call(lambda: web3.eth.block_number)
            except CircuitBreakerOpenError as exc:
                raise ValueError("Cannot get latest block: circuit breaker open") from exc

        total_swaps = await ingestor.sync_pool(pool_address, start_block, end_block, stop=stop)
        return total_swaps
