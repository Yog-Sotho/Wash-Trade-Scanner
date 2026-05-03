"""
Multi-chain swap event ingestor using Web3.py with complete log decoding.

This module provides:
- Async blockchain data ingestion from multiple chains
- Event decoding for various DEX types (Uniswap V2/V3, Curve, Balancer, etc.)
- Token address resolution from pool contracts
- Rate limiting and retry logic for RPC calls
- Historical sync and real-time event listening

Supports DEX types:
- v2: Uniswap V2, Sushiswap, PancakeSwap, and forks
- v3: Uniswap V3 and similar concentrated liquidity AMMs
- curve: Curve Finance-style AMMs
- balancer: Balancer V2 style pools
- syncswap: SyncSwap and similar ZkSync-era AMMs
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TypeVar

from web3 import AsyncWeb3, Web3
from web3.middleware import ExtraDataToPOAMiddleware
from web3.providers.rpc import AsyncHTTPProvider
from web3 import WebSocketProvider
from web3.types import BlockData, LogReceipt
from web3.contract import AsyncContract

from config.chains import CHAINS, get_chain_config
from config.settings import settings
from core.storage import Storage

logger = logging.getLogger(__name__)

# Type variables for generic typing
T = TypeVar("T")

# Constants
DEFAULT_BATCH_SIZE: int = 1000
MAX_RETRY_ATTEMPTS: int = 3
RETRY_BASE_DELAY: float = 1.0
RETRY_MAX_DELAY: float = 10.0


# ==============================================================================
# Exceptions
# ==============================================================================

class IngestorError(Exception):
    """Base exception for ingestor errors."""
    pass


class ConnectionError(IngestorError):
    """Raised when connection to blockchain node fails."""
    pass


class LogFetchError(IngestorError):
    """Raised when fetching logs from the blockchain fails."""
    pass


class EventProcessingError(IngestorError):
    """Raised when processing a swap event fails."""
    pass


# ==============================================================================
# Rate Limiter
# ==============================================================================

@dataclass
class RateLimiter:
    """
    Token bucket rate limiter for RPC calls.

    Ensures that RPC calls do not exceed the configured rate limit
    by tracking call timestamps and enforcing delays when necessary.

    Attributes:
        max_calls_per_second: Maximum number of calls allowed per second
        _calls: Internal list of recent call timestamps
        _lock: Lock for thread-safe operations
    """

    max_calls_per_second: int
    _calls: List[float] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self) -> None:
        """
        Acquire a rate limit token, waiting if necessary.

        This method blocks until a call can be made within the rate limit.
        Uses exponential backoff when rate limit is reached.
        """
        async with self._lock:
            now = time.time()
            # Remove calls older than 1 second
            self._calls = [t for t in self._calls if t > now - 1.0]

            if len(self._calls) >= self.max_calls_per_second:
                # Calculate wait time until oldest call expires
                sleep_time = 1.0 - (now - self._calls[0])
                sleep_time = max(0.01, min(sleep_time, RETRY_MAX_DELAY))

                await asyncio.sleep(sleep_time)

                # Remove the oldest call that just expired
                if self._calls:
                    self._calls = self._calls[1:]

            self._calls.append(time.time())


# ==============================================================================
# Chain Ingestor
# ==============================================================================

@dataclass
class ChainIngestor:
    """
    Ingests swap events from a specific blockchain.

    This class handles:
    - Web3 connection management (HTTP/WebSocket)
    - Log fetching with batching and rate limiting
    - Event decoding for various DEX types
    - Token address resolution from pool contracts
    - Real-time event listening

    Attributes:
        chain_config: Configuration dict for the blockchain
        storage: Storage instance for persisting trades
        web3: AsyncWeb3 instance (initialized via connect())
        rate_limiter: Rate limiter for RPC calls
        is_running: Whether real-time listening is active
        latest_block: Last processed block number
        contracts: Cached contract instances by DEX name
        _pool_tokens: Cache of pool token addresses
        _pool_tokens_lock: Lock for pool token cache
        _failed_batches: Track failed batches for retry tracking
    """

    chain_config: Dict[str, Any]
    storage: Storage
    web3: Optional[AsyncWeb3] = None
    rate_limiter: Optional[RateLimiter] = None
    is_running: bool = False
    latest_block: int = 0
    contracts: Dict[str, AsyncContract] = field(default_factory=dict)
    _pool_tokens: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    _pool_tokens_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _failed_batches: List[Dict[str, Any]] = field(default_factory=list)
    _batch_failures: int = 0

    def __post_init__(self) -> None:
        """Initialize rate limiter after dataclass construction."""
        if self.rate_limiter is None:
            self.rate_limiter = RateLimiter(settings.RPC_RATE_LIMIT)

    @property
    def chain_id(self) -> int:
        """Get the chain ID."""
        return self.chain_config["chain_id"]

    @property
    def chain_name(self) -> str:
        """Get the chain name."""
        return self.chain_config["name"]

    @property
    def rpc_url(self) -> str:
        """Get the RPC URL."""
        return self.chain_config["rpc_url"]

    @property
    def ws_url(self) -> str:
        """Get the WebSocket URL."""
        return self.chain_config.get("ws_url", "")

    @property
    def block_time(self) -> float:
        """Get the average block time in seconds."""
        return self.chain_config["block_time"]

    @property
    def dexes(self) -> List[Dict[str, Any]]:
        """Get the list of DEX configurations."""
        return self.chain_config["dexes"]

    async def connect(self) -> None:
        """
        Establish connection to the blockchain node.

        Attempts WebSocket connection first, falling back to HTTP.
        Validates connection before returning.

        Raises:
            ConnectionError: If neither WebSocket nor HTTP connection succeeds
        """
        if self.ws_url and not self.ws_url.endswith("YOUR_KEY"):
            try:
                provider = WebSocketProvider(self.ws_url)
                self.web3 = AsyncWeb3(provider)
                if await self.web3.is_connected():
                    logger.info(f"Connected to {self.chain_name} via WebSocket")
                    return
            except Exception as e:
                logger.warning(f"WebSocket connection failed for {self.chain_name}: {e}")

        provider = AsyncHTTPProvider(self.rpc_url)
        self.web3 = AsyncWeb3(provider)
        self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        if await self.web3.is_connected():
            logger.info(f"Connected to {self.chain_name} via HTTP")
        else:
            raise ConnectionError(f"Failed to connect to {self.chain_name}")

    async def _get_pool_tokens(self, pool_address: str) -> Tuple[str, str]:
        """
        Fetch token0 and token1 addresses from pool contract.

        Uses caching to avoid redundant RPC calls for the same pool.
        Falls back to pool address if token fetch fails.

        Args:
            pool_address: The pool contract address

        Returns:
            Tuple of (token0_address, token1_address)
        """
        addr = Web3.to_checksum_address(pool_address)

        # Check cache first (with lock)
        async with self._pool_tokens_lock:
            if addr in self._pool_tokens:
                return self._pool_tokens[addr]

        # Minimal ABI for token0/token1
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
            token0, token1 = await asyncio.gather(
                contract.functions.token0().call(),
                contract.functions.token1().call(),
            )
        except Exception as e:
            logger.error(f"Failed to fetch token0/token1 for {addr}: {e}")
            # Fallback to pool address
            token0 = addr
            token1 = addr

        # Normalize to lowercase
        token0 = token0.lower()
        token1 = token1.lower()

        # Cache the result
        async with self._pool_tokens_lock:
            self._pool_tokens[addr] = (token0, token1)

        return token0, token1

    def _get_contract(self, dex_config: Dict[str, Any]) -> AsyncContract:
        """
        Get or create a contract instance for the DEX.

        Caches contract instances to avoid redundant creation.

        Args:
            dex_config: DEX configuration dict

        Returns:
            AsyncContract instance for the DEX router
        """
        name = dex_config["name"]
        if name not in self.contracts:
            self.contracts[name] = self.web3.eth.contract(
                address=Web3.to_checksum_address(dex_config["router"]),
                abi=dex_config["abi"],
            )
        return self.contracts[name]

    async def _get_logs_batch(
        self,
        address: str,
        topics: List[str],
        from_block: int,
        to_block: int,
        retry_count: int = 0,
    ) -> List[LogReceipt]:
        """
        Fetch logs in batches with retry logic.

        Splits large ranges into smaller batches and implements
        exponential backoff retry on failures.

        Args:
            address: Contract address to fetch logs from
            topics: Event signature topics
            from_block: Starting block (inclusive)
            to_block: Ending block (inclusive)
            retry_count: Current retry attempt number

        Returns:
            List of log receipts

        Raises:
            LogFetchError: If all retry attempts fail
        """
        batch_size = DEFAULT_BATCH_SIZE
        all_logs: List[LogReceipt] = []

        for batch_start in range(from_block, to_block + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, to_block)
            await self.rate_limiter.acquire()

            try:
                logs = await self.web3.eth.get_logs({
                    "address": address,
                    "topics": topics,
                    "fromBlock": batch_start,
                    "toBlock": batch_end,
                })
                all_logs.extend(logs)
                logger.debug(f"Fetched {len(logs)} logs from {batch_start} to {batch_end}")

            except Exception as e:
                logger.error(f"Error fetching logs for {address} [{batch_start}-{batch_end}]: {e}")

                # Track failed batch for potential retry
                self._failed_batches.append({
                    "address": address,
                    "topics": topics,
                    "from_block": batch_start,
                    "to_block": batch_end,
                    "error": str(e),
                    "timestamp": datetime.utcnow(),
                })
                self._batch_failures += 1

                # Retry logic with exponential backoff
                if retry_count < MAX_RETRY_ATTEMPTS:
                    delay = min(RETRY_BASE_DELAY * (2 ** retry_count), RETRY_MAX_DELAY)
                    logger.info(f"Retrying in {delay:.1f}s (attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS})")
                    await asyncio.sleep(delay)
                    # Recursive retry
                    retry_logs = await self._get_logs_batch(
                        address, topics, batch_start, batch_end, retry_count + 1
                    )
                    all_logs.extend(retry_logs)
                else:
                    logger.warning(f"Max retries exceeded for batch [{batch_start}-{batch_end}]")
                    # Continue with empty batch but track failure
                    await asyncio.sleep(RETRY_BASE_DELAY)

        return all_logs

    async def _fetch_block_timestamp(self, block_number: int) -> datetime:
        """
        Fetch timestamp for a single block.

        Args:
            block_number: Block number to fetch

        Returns:
            datetime object for the block timestamp
        """
        await self.rate_limiter.acquire()
        block: BlockData = await self.web3.eth.get_block(block_number)
        return datetime.fromtimestamp(block["timestamp"])

    async def _batch_fetch_timestamps(
        self, block_numbers: List[int]
    ) -> Dict[int, datetime]:
        """
        Fetch timestamps for multiple blocks concurrently.

        Args:
            block_numbers: List of block numbers

        Returns:
            Dict mapping block number to timestamp
        """
        # Deduplicate
        unique_blocks = list(set(block_numbers))

        # Fetch concurrently with semaphore to limit concurrency
        semaphore = asyncio.Semaphore(10)
        async def fetch_one(block_num: int) -> Tuple[int, datetime]:
            async with semaphore:
                ts = await self._fetch_block_timestamp(block_num)
                return (block_num, ts)

        results = await asyncio.gather(
            *[fetch_one(bn) for bn in unique_blocks],
            return_exceptions=True,
        )

        timestamps: Dict[int, datetime] = {}
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Failed to fetch timestamp: {result}")
            else:
                block_num, ts = result
                timestamps[block_num] = ts

        return timestamps

    async def _process_swap_event(
        self,
        dex: Dict[str, Any],
        log: LogReceipt,
        block_timestamp: datetime,
    ) -> Optional[Dict[str, Any]]:
        """
        Process a single swap event log.

        Decodes the event based on DEX type, resolves token addresses,
        and returns trade data dict.

        Args:
            dex: DEX configuration
            log: Raw log receipt
            block_timestamp: Timestamp of the containing block

        Returns:
            Trade data dict or None if processing failed
        """
        try:
            contract = self._get_contract(dex)
            event_type = dex["type"]
            decoded = contract.events.Swap().process_log(log)
            args = decoded["args"]

            # Default values
            sender = args.get("sender", args.get("buyer", log["address"]))
            recipient = args.get("to", args.get("recipient", args.get("buyer", log["address"])))
            amount_in: int = 0
            amount_out: int = 0
            token_in: str = log["address"]
            token_out: str = log["address"]

            # Decode based on DEX type
            if event_type == "v2":
                amount0In = args.get("amount0In", 0)
                amount1In = args.get("amount1In", 0)
                amount0Out = args.get("amount0Out", 0)
                amount1Out = args.get("amount1Out", 0)

                if amount0In > 0:
                    amount_in = amount0In
                    amount_out = amount1Out
                else:
                    amount_in = amount1In
                    amount_out = amount0Out

            elif event_type == "v3":
                amount0 = args["amount0"]
                amount1 = args["amount1"]

                if amount0 < 0:
                    amount_in = abs(amount0)
                    amount_out = amount1
                else:
                    amount_in = abs(amount1)
                    amount_out = amount0

            elif event_type == "curve":
                amount_in = args["tokens_sold"]
                amount_out = args["tokens_bought"]

            elif event_type == "balancer":
                amount_in = args["amountIn"]
                amount_out = args["amountOut"]
                token_in = args["tokenIn"]
                token_out = args["tokenOut"]

            elif event_type == "syncswap":
                amount_in = args["amountIn"]
                amount_out = args["amountOut"]
                token_in = args["tokenIn"]
                token_out = args["tokenOut"]
                sender = args["sender"]
                recipient = args["to"]

            else:
                logger.warning(f"Unknown DEX type {event_type} for {dex['name']}, skipping")
                return None

            # Resolve actual token addresses for v2/v3/curve pools
            if event_type in ("v2", "v3", "curve"):
                token0, token1 = await self._get_pool_tokens(log["address"])
                # Determine which is in/out based on amounts direction
                if event_type == "v2":
                    if amount_in == args.get("amount0In", 0):
                        token_in = token0
                        token_out = token1
                    else:
                        token_in = token1
                        token_out = token0
                elif event_type == "v3":
                    if int(args.get("amount0", 0)) < 0:
                        token_in = token0
                        token_out = token1
                    else:
                        token_in = token1
                        token_out = token0
                else:  # curve
                    token_in = token0
                    token_out = token1

            trade_data = {
                "chain_id": self.chain_id,
                "dex_name": dex["name"],
                "pool_address": log["address"],
                "token_in": token_in,
                "token_out": token_out,
                "amount_in": float(amount_in),
                "amount_out": float(amount_out),
                "sender": sender,
                "recipient": recipient,
                "transaction_hash": log["transactionHash"].hex(),
                "block_number": log["blockNumber"],
                "block_timestamp": block_timestamp,
                "log_index": log["logIndex"],
                "gas_price": float(log.get("gasPrice", 0)) if log.get("gasPrice") else None,
                "gas_used": float(log.get("gasUsed", 0)) if log.get("gasUsed") else None,
            }
            return trade_data

        except Exception as e:
            logger.error(f"Error processing swap event: {e}")
            return None

    async def sync_historical_swaps(
        self,
        dex: Dict[str, Any],
        from_block: int,
        to_block: int,
        pool_address: Optional[str] = None,
    ) -> int:
        """
        Sync historical swap events for a DEX.

        Args:
            dex: DEX configuration
            from_block: Starting block
            to_block: Ending block
            pool_address: Optional specific pool address

        Returns:
            Number of swaps synced
        """
        logger.info(f"Syncing historical swaps for {dex['name']} on {self.chain_name}")
        event_signature_hash = Web3.keccak(text=dex["event_sig"]).hex()
        topics = [event_signature_hash]
        address = pool_address if pool_address else dex["router"]

        logs = await self._get_logs_batch(address, topics, from_block, to_block)

        if not logs:
            logger.info(f"No logs found for {dex['name']} in range [{from_block}, {to_block}]")
            return 0

        # Fetch block timestamps
        block_numbers = [log["blockNumber"] for log in logs]
        block_timestamps = await self._batch_fetch_timestamps(block_numbers)

        # Process events concurrently
        semaphore = asyncio.Semaphore(20)
        async def process_with_semaphore(log: LogReceipt) -> None:
            async with semaphore:
                ts = block_timestamps.get(log["blockNumber"])
                if ts:
                    trade_data = await self._process_swap_event(dex, log, ts)
                    if trade_data:
                        await self.storage.save_trade(trade_data)

        await asyncio.gather(
            *[process_with_semaphore(log) for log in logs],
            return_exceptions=True,
        )

        logger.info(f"Synced {len(logs)} historical swaps for {dex['name']}")
        return len(logs)

    async def listen_realtime(self, dexes: List[Dict[str, Any]]) -> None:
        """
        Listen for real-time swap events.

        Continuously polls for new blocks and processes any new swap events.

        Args:
            dexes: List of DEX configurations to listen to
        """
        self.is_running = True
        logger.info(f"Starting real-time listener for {self.chain_name}")
        self.latest_block = await self.web3.eth.block_number

        event_filters = {}
        for dex in dexes:
            event_signature_hash = Web3.keccak(text=dex["event_sig"]).hex()
            event_filters[dex["name"]] = {
                "address": dex["router"],
                "topics": [event_signature_hash],
            }

        while self.is_running:
            try:
                await self.rate_limiter.acquire()
                latest = await self.web3.eth.block_number

                if latest > self.latest_block:
                    for dex in dexes:
                        logs = await self._get_logs_batch(
                            event_filters[dex["name"]]["address"],
                            event_filters[dex["name"]]["topics"],
                            self.latest_block + 1,
                            latest,
                        )

                        for log in logs:
                            ts = await self._fetch_block_timestamp(log["blockNumber"])
                            trade_data = await self._process_swap_event(dex, log, ts)
                            if trade_data:
                                await self.storage.save_trade(trade_data)

                    self.latest_block = latest

                await asyncio.sleep(self.block_time)

            except Exception as e:
                logger.error(f"Error in real-time listener for {self.chain_name}: {e}")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the real-time listener."""
        self.is_running = False
        logger.info(f"Stopped listener for {self.chain_name}")

    def get_failed_batches(self) -> List[Dict[str, Any]]:
        """Get list of failed batches for retry or investigation."""
        return self._failed_batches.copy()

    def clear_failed_batches(self) -> None:
        """Clear the failed batches list."""
        self._failed_batches.clear()
        self._batch_failures = 0


# ==============================================================================
# Multi-Chain Ingestor
# ==============================================================================

@dataclass
class MultiChainIngestor:
    """
    Manages ingestion across multiple blockchain networks.

    Provides unified interface for:
    - Initializing connections to all configured chains
    - Syncing historical data across chains
    - Starting/stopping real-time listeners
    - Pool-specific audits

    Attributes:
        storage: Storage instance for persisting trades
        ingestors: Dict mapping chain_id to ChainIngestor
        tasks: List of active asyncio tasks
    """

    storage: Storage
    ingestors: Dict[int, ChainIngestor] = field(default_factory=dict)
    tasks: List[asyncio.Task] = field(default_factory=list)
    _initialized: bool = False

    async def initialize(self) -> None:
        """
        Initialize connections to all configured chains.

        Creates ChainIngestor instances and establishes connections.
        """
        if self._initialized:
            logger.warning("MultiChainIngestor already initialized")
            return

        for chain_config in CHAINS:
            ingestor = ChainIngestor(chain_config, self.storage)
            await ingestor.connect()
            self.ingestors[chain_config["chain_id"]] = ingestor

        self._initialized = True
        logger.info(f"Initialized ingestors for {len(self.ingestors)} chains")

    async def sync_historical_all(
        self, from_block_override: Optional[Dict[int, int]] = None
    ) -> int:
        """
        Sync historical data for all configured chains.

        Args:
            from_block_override: Optional dict mapping chain_id to start block

        Returns:
            Total number of swaps synced across all chains
        """
        tasks = []
        for chain_id, ingestor in self.ingestors.items():
            chain_config = ingestor.chain_config
            start_block = (
                from_block_override.get(chain_id)
                if from_block_override
                else chain_config["start_block"]
            )

            await ingestor.rate_limiter.acquire()
            latest_block = await ingestor.web3.eth.block_number

            for dex in chain_config["dexes"]:
                task = asyncio.create_task(
                    ingestor.sync_historical_swaps(dex, start_block, latest_block)
                )
                tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_swaps = 0
        for r in results:
            if isinstance(r, int):
                total_swaps += r
            elif isinstance(r, Exception):
                logger.error(f"Sync task failed: {r}")

        logger.info(f"Historical sync complete. Total swaps synced: {total_swaps}")
        return total_swaps

    async def start_realtime_all(self) -> None:
        """Start real-time listeners for all chains."""
        for ingestor in self.ingestors.values():
            task = asyncio.create_task(
                ingestor.listen_realtime(ingestor.dexes)
            )
            self.tasks.append(task)
            logger.info(f"Started realtime listener for {ingestor.chain_name}")

    async def stop_all(self) -> None:
        """Stop all listeners and cancel tasks."""
        for ingestor in self.ingestors.values():
            await ingestor.stop()

        for task in self.tasks:
            task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        logger.info("All listeners stopped")

    async def audit_pool(
        self,
        chain_id: int,
        pool_address: str,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> int:
        """
        Audit a specific pool for wash trading.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address
            start_block: Starting block (optional)
            end_block: Ending block (optional)

        Returns:
            Number of swaps synced
        """
        ingestor = self.ingestors.get(chain_id)
        if not ingestor:
            raise ValueError(f"Chain {chain_id} not configured")

        total_swaps = 0
        for dex in ingestor.dexes:
            if start_block is None:
                start_block = ingestor.chain_config["start_block"]
            if end_block is None:
                await ingestor.rate_limiter.acquire()
                end_block = await ingestor.web3.eth.block_number

            swaps = await ingestor.sync_historical_swaps(
                dex,
                start_block,
                end_block,
                pool_address=pool_address,
            )
            total_swaps += swaps

        return total_swaps
