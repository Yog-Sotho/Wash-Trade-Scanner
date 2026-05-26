"""
Multi-chain swap event ingestor with circuit breaker protection.
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from pydantic import SecretStr
from web3 import AsyncWeb3, Web3, AsyncHTTPProvider
from web3.middleware import async_geth_poa_middleware
from web3.types import LogReceipt

from config.chains import CHAINS, get_chain_config
from config.settings import settings
from core.storage import Storage
from core.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from core.exceptions import RPCError, CircuitBreakerOpen

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token bucket rate limiter for RPC calls."""

    def __init__(self, max_calls_per_second: int):
        self.max_calls = max_calls_per_second
        self.calls: List[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now = time.time()
                self.calls = [t for t in self.calls if t > now - 1.0]
                if len(self.calls) < self.max_calls:
                    self.calls.append(time.time())
                    return

                sleep_time = 1.0 - (now - self.calls[0])

            # SECURITY: Release lock during sleep to prevent serializing all concurrent tasks.
            # Holding the lock during sleep blocks other tasks from checking the window,
            # effectively turning the parallel ingestor into a serial one (self-DoS).
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)


class ChainIngestor:
    """Per-chain blockchain data ingestor."""

    def __init__(self, chain_config: Dict[str, Any], storage: Storage):
        self.chain_config = chain_config
        self.storage = storage
        self.web3: Optional[AsyncWeb3] = None
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
        self.contracts: Dict[str, Any] = {}
        self._pool_tokens: Dict[str, Tuple[str, str]] = {}
        self._pool_tokens_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to RPC with validation."""
        rpc = self.chain_config["rpc_url"]
        rpc_str = rpc.get_secret_value() if isinstance(rpc, SecretStr) else rpc

        if "YOUR_KEY" in rpc_str or "placeholder" in rpc_str.lower():
            raise ValueError(
                f"RPC URL for {self.chain_config['name']} contains placeholder. "
                "Set a real endpoint in environment."
            )

        # SECURITY: Ensure RPC URL uses a secure and supported protocol
        if not rpc_str.startswith(("http://", "https://")):
            raise ValueError(f"Invalid RPC URL protocol: {rpc}. Only http/https supported.")

        provider = AsyncHTTPProvider(rpc_str)
        self.web3 = AsyncWeb3(provider)

        if self.chain_config.get("chain_id") in (56, 97):
            self.web3.middleware_onion.inject(async_geth_poa_middleware, layer=0)

        try:
            connected = await self.circuit_breaker.call(self.web3.is_connected)
            if not connected:
                raise ConnectionError(f"Failed to connect to {self.chain_config['name']}")

            # SECURITY: Verify the chain ID matches the expected configuration
            # This prevents connecting to the wrong network (e.g. Testnet vs Mainnet)
            actual_chain_id = await self.circuit_breaker.call(lambda: self.web3.eth.chain_id)
            expected_chain_id = self.chain_config.get("chain_id")

            if expected_chain_id and actual_chain_id != expected_chain_id:
                raise ConnectionError(
                    f"Chain ID mismatch for {self.chain_config['name']}: "
                    f"expected {expected_chain_id}, got {actual_chain_id}"
                )

            logger.info(f"Connected to {self.chain_config['name']} via HTTP (Chain ID: {actual_chain_id})")
        except CircuitBreakerOpen as exc:
            raise ConnectionError(f"Circuit breaker open for {self.chain_config['name']}") from exc

    async def _get_pool_tokens(self, pool_address: str) -> Tuple[str, str]:
        """Fetch token0 and token1 from pool contract with caching."""
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
        except (RPCError, CircuitBreakerOpen) as exc:
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
        topics: List[str],
        from_block: int,
        to_block: int,
        max_retries: int = 3,
    ) -> List[LogReceipt]:
        """Fetch logs with retry and circuit breaker protection."""
        batch_size = 1000
        all_logs = []

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
                except CircuitBreakerOpen:
                    logger.error(f"Circuit breaker open, aborting log fetch")
                    raise
                except Exception as exc:
                    logger.error(
                        f"Error fetching logs {batch_start}-{batch_end}: {exc}. "
                        f"Attempt {attempt + 1}/{max_retries}"
                    )
                    await asyncio.sleep(2 ** attempt)
            else:
                logger.error(f"Failed to fetch logs for blocks {batch_start}-{batch_end} after {max_retries} attempts")

        return all_logs

    async def _process_swap_event(
        self,
        dex: Dict[str, Any],
        log: LogReceipt,
        block_timestamp: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Process a single swap event log."""
        try:
            event_type = dex["type"]
            args = log.get("args", {})

            sender = args.get("sender", args.get("buyer", log.get("address", "")))
            recipient = args.get("to", args.get("recipient", args.get("buyer", "")))
            amount_in = 0.0
            amount_out = 0.0
            token_in = log.get("address", "")
            token_out = log.get("address", "")

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
                amount0 = float(args.get("amount0", 0))
                amount1 = float(args.get("amount1", 0))
                if amount0 < 0:
                    amount_in = abs(amount0)
                    amount_out = amount1
                else:
                    amount_in = abs(amount1)
                    amount_out = amount0

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
                    if float(args.get("amount0", 0)) < 0:
                        token_in = token0
                        token_out = token1
                    else:
                        token_in = token1
                        token_out = token0

            return {
                "chain_id": self.chain_config["chain_id"],
                "dex_name": dex["name"],
                "pool_address": log.get("address", ""),
                "token_in": token_in.lower(),
                "token_out": token_out.lower(),
                "amount_in": amount_in,
                "amount_out": amount_out,
                "sender": sender.lower(),
                "recipient": recipient.lower(),
                "transaction_hash": log.get("transactionHash", ""),
                "block_number": log.get("blockNumber", 0),
                "block_timestamp": block_timestamp,
                "log_index": log.get("logIndex", 0),
                "gas_price": None,
                "gas_used": None,
            }

        except Exception as exc:
            logger.error(f"Error processing swap event: {exc}")
            return None

    async def sync_historical_swaps(
        self,
        dex: Dict[str, Any],
        from_block: int,
        to_block: int,
        pool_address: Optional[str] = None,
    ) -> int:
        """Sync historical swap events."""
        logger.info(f"Syncing historical swaps for {dex['name']} on {self.chain_config['name']}")

        event_signature_hash = Web3.keccak(text=dex["event_sig"]).hex()
        topics = [event_signature_hash]
        address = pool_address if pool_address else dex["router"]

        try:
            logs = await self._get_logs_batch(address, topics, from_block, to_block)
        except CircuitBreakerOpen:
            logger.error("Circuit breaker open, aborting sync")
            return 0

        if not logs:
            return 0

        block_numbers = set()
        for log in logs:
            bn = log.get("blockNumber")
            if bn:
                block_numbers.add(bn)

        block_timestamps = {}
        for bn in block_numbers:
            try:
                await self.rate_limiter.acquire()
                block = await self.circuit_breaker.call(self.web3.eth.get_block, bn)
                block_timestamps[bn] = datetime.fromtimestamp(block["timestamp"])
            except Exception as exc:
                logger.error(f"Failed to fetch block {bn}: {exc}")
                block_timestamps[bn] = datetime.utcnow()

        trades_data = []
        for log in logs:
            bn = log.get("blockNumber")
            ts = block_timestamps.get(bn, datetime.utcnow())
            trade_data = await self._process_swap_event(dex, log, ts)
            if trade_data:
                trades_data.append(trade_data)

        if trades_data:
            await self.storage.save_trades_batch(trades_data)

        logger.info(f"Synced {len(trades_data)} historical swaps for {dex['name']}")
        return len(trades_data)

    async def stop(self) -> None:
        self.is_running = False


class MultiChainIngestor:
    """Manages multiple chain ingestors."""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.ingestors: Dict[int, ChainIngestor] = {}

    async def initialize(self) -> None:
        for chain in CHAINS:
            # Prioritize environment-provided RPC URLs
            chain_id = chain["chain_id"]
            env_rpc = settings.rpc_urls.get(chain_id)
            if env_rpc:
                chain["rpc_url"] = env_rpc

            ingestor = ChainIngestor(chain, self.storage)
            await ingestor.connect()
            self.ingestors[chain_id] = ingestor

    async def audit_pool(
        self,
        chain_id: int,
        pool_address: str,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> int:
        """Sync trades for a specific pool."""
        ingestor = self.ingestors.get(chain_id)
        if not ingestor:
            raise ValueError(f"Chain {chain_id} not configured")

        total_swaps = 0
        chain_config = get_chain_config(chain_id)

        if start_block is None:
            start_block = chain_config.get("start_block", 0)

        if end_block is None:
            try:
                await ingestor.rate_limiter.acquire()
                end_block = await ingestor.circuit_breaker.call(ingestor.web3.eth.block_number)
            except CircuitBreakerOpen as exc:
                raise ValueError(f"Cannot get latest block: circuit breaker open") from exc

        # SECURITY: Enforce block range limits to prevent Denial of Service
        if end_block < start_block:
            raise ValueError(f"end_block ({end_block}) must be >= start_block ({start_block})")

        if end_block - start_block > 10_000_000:
            raise ValueError(
                f"Block range {end_block - start_block:,} exceeds maximum of 10,000,000. "
                "Please specify a smaller range."
            )

        for dex in chain_config["dexes"]:
            swaps = await ingestor.sync_historical_swaps(
                dex,
                start_block,
                end_block,
                pool_address=pool_address,
            )
            total_swaps += swaps

        return total_swaps
