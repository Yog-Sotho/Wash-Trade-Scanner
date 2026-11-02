"""
Multi-chain swap event ingestor with circuit breaker protection.
"""

import asyncio
import logging
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from web3 import AsyncHTTPProvider, AsyncWeb3, Web3
from web3.middleware import async_geth_poa_middleware  # type: ignore[attr-defined]
from web3.types import EventData, LogReceipt

from config.chains import get_chain_config
from config.settings import settings
from core.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from core.exceptions import CircuitBreakerOpenError, RPCError
from core.pricing import PriceOracle
from core.storage import Storage

logger = logging.getLogger(__name__)

# Concurrent per-block / per-transaction RPC calls in a sync; the rate limiter
# still bounds the request rate.
_RPC_FETCH_CONCURRENCY = 8


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
    """Token bucket rate limiter for RPC calls."""

    def __init__(self, max_calls_per_second: int):
        self.max_calls = max_calls_per_second
        self.calls: list[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if t > now - 1.0]
            if len(self.calls) >= self.max_calls:
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                self.calls = self.calls[1:]
            self.calls.append(time.time())


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
        self.rate_limiter = RateLimiter(settings.RPC_RATE_LIMIT)
        self.circuit_breaker = CircuitBreaker(
            name=f"rpc_{chain_config['chain_id']}",
            config=CircuitBreakerConfig(
                failure_threshold=settings.RPC_MAX_FAILURES,
                recovery_timeout=settings.RPC_RECOVERY_TIMEOUT,
            ),
        )
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

        provider = AsyncHTTPProvider(rpc)
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

    async def _get_logs_batch(
        self,
        address: str,
        topics: list[str],
        from_block: int,
        to_block: int,
        max_retries: int = 3,
    ) -> list[LogReceipt]:
        """Fetch logs with retry and circuit breaker protection."""
        assert self.web3 is not None
        batch_size = 1000
        all_logs: list[LogReceipt] = []

        for batch_start in range(from_block, to_block + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, to_block)

            for attempt in range(max_retries):
                try:
                    await self.rate_limiter.acquire()
                    logs = await self.circuit_breaker.call(
                        self.web3.eth.get_logs,
                        {
                            "address": address,
                            "topics": topics,
                            "fromBlock": batch_start,
                            "toBlock": batch_end,
                        },
                    )
                    all_logs.extend(logs)
                    logger.debug(f"Fetched {len(logs)} logs from {batch_start} to {batch_end}")
                    break
                except CircuitBreakerOpenError:
                    logger.error("Circuit breaker open, aborting log fetch")
                    raise
                except Exception as exc:
                    logger.error(
                        f"Error fetching logs {batch_start}-{batch_end}: {exc}. "
                        f"Attempt {attempt + 1}/{max_retries}"
                    )
                    await asyncio.sleep(2**attempt)
            else:
                logger.error(
                    f"Failed to fetch logs for blocks {batch_start}-{batch_end} after {max_retries} attempts"
                )

        return all_logs

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

    async def _fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, datetime]:
        """Fetch block timestamps concurrently (bounded; the rate limiter still
        caps the request rate). A block that cannot be fetched is stamped with
        the current time, as before."""
        assert self.web3 is not None
        web3 = self.web3
        semaphore = asyncio.Semaphore(_RPC_FETCH_CONCURRENCY)
        timestamps: dict[int, datetime] = {}

        async def fetch(block_number: int) -> None:
            async with semaphore:
                try:
                    await self.rate_limiter.acquire()
                    block = await self.circuit_breaker.call(web3.eth.get_block, block_number)
                    timestamps[block_number] = datetime.fromtimestamp(
                        block["timestamp"], tz=UTC
                    ).replace(tzinfo=None)
                except Exception as exc:
                    logger.error(f"Failed to fetch block {block_number}: {exc}")
                    timestamps[block_number] = datetime.now(UTC).replace(tzinfo=None)

        await asyncio.gather(*(fetch(bn) for bn in sorted(block_numbers)))
        return timestamps

    async def _fetch_transactions(
        self, tx_hashes: set[str]
    ) -> dict[str, tuple[str, str | None, float | None]]:
        """Fetch (from, to, gas price in gwei) for each transaction.

        Swap events name the router or bot contract that called the pool, not
        the account that traded; only the transaction carries that. A
        transaction that cannot be fetched is left out (its swaps keep a NULL
        tx_from) rather than failing the whole sync.
        """
        assert self.web3 is not None
        web3 = self.web3
        semaphore = asyncio.Semaphore(_RPC_FETCH_CONCURRENCY)
        results: dict[str, tuple[str, str | None, float | None]] = {}

        async def fetch(tx_hash: str) -> None:
            async with semaphore:
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
    ) -> int:
        """Sync historical swap events."""
        logger.info(f"Syncing historical swaps for {dex['name']} on {self.chain_config['name']}")

        event_signature_hash = Web3.keccak(text=dex["event_sig"]).hex()
        topics = [event_signature_hash]
        # web3 rejects non-checksummed addresses in eth_getLogs filters, while
        # storage and callers use lowercase.
        address = Web3.to_checksum_address(pool_address) if pool_address else dex["router"]

        try:
            raw_logs = await self._get_logs_batch(address, topics, from_block, to_block)
        except CircuitBreakerOpenError:
            logger.error("Circuit breaker open, aborting sync")
            return 0

        if not raw_logs:
            return 0

        # eth_getLogs returns raw, undecoded logs (no `args`). Decode each one against
        # the DEX's event ABI so downstream code can read sender/recipient/amounts.
        assert self.web3 is not None
        event_name = dex["abi"][0]["name"]
        decode_contract = self.web3.eth.contract(abi=dex["abi"])
        event = getattr(decode_contract.events, event_name)()
        logs: list[EventData] = []
        for raw_log in raw_logs:
            try:
                logs.append(event.process_log(raw_log))
            except Exception as exc:
                logger.error(f"Failed to decode log at block {raw_log.get('blockNumber')}: {exc}")

        if not logs:
            return 0

        block_numbers = set()
        for log in logs:
            bn = log.get("blockNumber")
            if bn:
                block_numbers.add(bn)

        block_timestamps = await self._fetch_block_timestamps(block_numbers)

        trades_data = []
        for log in logs:
            bn = log.get("blockNumber")
            ts = block_timestamps.get(bn, datetime.now(UTC).replace(tzinfo=None))
            trade_data = await self._process_swap_event(dex, log, ts)
            if trade_data:
                trades_data.append(trade_data)

        transactions = await self._fetch_transactions(
            {trade["transaction_hash"] for trade in trades_data}
        )
        for trade in trades_data:
            tx = transactions.get(trade["transaction_hash"])
            if tx is not None:
                trade["tx_from"], trade["tx_to"], trade["gas_price"] = tx

        if trades_data:
            await self.price_oracle.price_trades(self.chain_config["chain_id"], trades_data)
            await self.storage.save_trades_batch(trades_data)

        logger.info(f"Synced {len(trades_data)} historical swaps for {dex['name']}")
        return len(trades_data)

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

    async def sync_pool(self, pool_address: str, from_block: int, to_block: int) -> int:
        """Sync one pool's swaps from the DEX that actually created it."""
        total = 0
        for dex in await self.resolve_pool_dexes(pool_address):
            total += await self.sync_historical_swaps(
                dex, from_block, to_block, pool_address=pool_address
            )
        return total

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
    ) -> int:
        """Sync trades for a specific pool."""
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

        total_swaps = await ingestor.sync_pool(pool_address, start_block, end_block)
        return total_swaps
