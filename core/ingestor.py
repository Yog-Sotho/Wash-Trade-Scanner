"""
Multi‑chain swap event ingestor using Web3.py with complete log decoding.
Now resolves token addresses from pool contracts.
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

from web3 import AsyncWeb3, Web3
from web3.middleware import async_geth_poa_middleware
from web3.providers.rpc import AsyncHTTPProvider
from web3.providers.websocket import WebsocketProviderV2
from web3.types import EventData, LogReceipt
from web3.contract import AsyncContract

from config.chains import CHAINS, ChainConfig, get_chain_config
from config.settings import settings
from core.storage import Storage

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, max_calls_per_second: int):
        self.max_calls = max_calls_per_second
        self.calls = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if t > now - 1.0]
            if len(self.calls) >= self.max_calls:
                sleep_time = 1.0 - (now - self.calls[0])
                await asyncio.sleep(sleep_time)
                self.calls = self.calls[1:]
            self.calls.append(time.time())


class ChainIngestor:
    def __init__(self, chain_config: ChainConfig, storage: Storage):
        self.chain_config = chain_config
        self.storage = storage
        self.web3: Optional[AsyncWeb3] = None
        self.rate_limiter = RateLimiter(settings.RPC_RATE_LIMIT)
        self.is_running = False
        self.latest_block = 0
        self.contracts: Dict[str, AsyncContract] = {}
        # Cache pool token addresses: pool_address -> (token0, token1)
        self._pool_tokens: Dict[str, tuple[str, str]] = {}
        self._pool_tokens_lock = asyncio.Lock()

    async def connect(self) -> None:
        if self.chain_config.ws_url:
            try:
                provider = WebsocketProviderV2(self.chain_config.ws_url)
                self.web3 = AsyncWeb3(provider)
                if await self.web3.is_connected():
                    logger.info(f"Connected to {self.chain_config.name} via WebSocket")
                    return
            except Exception as e:
                logger.warning(f"WebSocket connection failed for {self.chain_config.name}: {e}")
        provider = AsyncHTTPProvider(self.chain_config.rpc_url)
        self.web3 = AsyncWeb3(provider)
        self.web3.middleware_onion.inject(async_geth_poa_middleware, layer=0)
        if await self.web3.is_connected():
            logger.info(f"Connected to {self.chain_config.name} via HTTP")
        else:
            raise ConnectionError(f"Failed to connect to {self.chain_config.name}")

    async def _get_pool_tokens(self, pool_address: str) -> tuple[str, str]:
        """Fetch token0 and token1 from pool contract, with caching."""
        addr = Web3.to_checksum_address(pool_address)
        async with self._pool_tokens_lock:
            if addr in self._pool_tokens:
                return self._pool_tokens[addr]

        # Minimal ABI for token0/token1
        pool_abi = [
            {"constant": True, "inputs": [], "name": "token0", "outputs": [{"name": "", "type": "address"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "token1", "outputs": [{"name": "", "type": "address"}], "type": "function"}
        ]
        contract = self.web3.eth.contract(address=addr, abi=pool_abi)
        try:
            token0 = await contract.functions.token0().call()
            token1 = await contract.functions.token1().call()
        except Exception as e:
            logger.error(f"Failed to fetch token0/token1 for {addr}: {e}")
            token0 = addr  # fallback to pool address
            token1 = addr
        token0 = token0.lower()
        token1 = token1.lower()
        async with self._pool_tokens_lock:
            self._pool_tokens[addr] = (token0, token1)
        return token0, token1

    def _get_contract(self, dex_config: Dict[str, Any]) -> AsyncContract:
        name = dex_config["name"]
        if name not in self.contracts:
            self.contracts[name] = self.web3.eth.contract(
                address=Web3.to_checksum_address(dex_config["router"]),
                abi=dex_config["abi"]
            )
        return self.contracts[name]

    async def _get_logs_batch(
        self,
        address: str,
        topics: List[str],
        from_block: int,
        to_block: int
    ) -> List[LogReceipt]:
        batch_size = 1000
        all_logs = []
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
                logger.error(f"Error fetching logs for {address}: {e}")
                await asyncio.sleep(1)
        return all_logs

    async def _process_swap_event(
        self,
        dex: Dict[str, Any],
        log: LogReceipt,
        block_timestamp: datetime
    ) -> None:
        try:
            contract = self._get_contract(dex)
            event_type = dex["type"]
            decoded = contract.events.Swap().process_log(log)
            args = decoded["args"]

            # Default values
            sender = args.get("sender", args.get("buyer", log["address"]))
            recipient = args.get("to", args.get("recipient", args.get("buyer", log["address"])))
            amount_in = 0
            amount_out = 0
            token_in = log["address"]
            token_out = log["address"]

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
                return

            # Resolve actual token addresses for v2/v3/curve pools
            if event_type in ("v2", "v3", "curve"):
                token0, token1 = await self._get_pool_tokens(log["address"])
                # Determine which is in/out based on amounts direction
                if amount_in == args.get("amount0In", 0) or (event_type == "v3" and int(amount0) < 0):
                    token_in = token0
                    token_out = token1
                else:
                    token_in = token1
                    token_out = token0

            trade_data = {
                "chain_id": self.chain_config.chain_id,
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
            await self.storage.save_trade(trade_data)
        except Exception as e:
            logger.error(f"Error processing swap event: {e}")

    async def sync_historical_swaps(
        self,
        dex: Dict[str, Any],
        from_block: int,
        to_block: int,
        pool_address: Optional[str] = None
    ) -> int:
        logger.info(f"Syncing historical swaps for {dex['name']} on {self.chain_config.name}")
        event_signature_hash = Web3.keccak(text=dex["event_sig"]).hex()
        topics = [event_signature_hash]
        address = pool_address if pool_address else dex["router"]
        logs = await self._get_logs_batch(address, topics, from_block, to_block)
        block_numbers = set(log["blockNumber"] for log in logs)
        block_timestamps = {}
        for block_num in block_numbers:
            await self.rate_limiter.acquire()
            block = await self.web3.eth.get_block(block_num)
            block_timestamps[block_num] = datetime.fromtimestamp(block["timestamp"])
        for log in logs:
            await self._process_swap_event(dex, log, block_timestamps[log["blockNumber"]])
        logger.info(f"Synced {len(logs)} historical swaps for {dex['name']}")
        return len(logs)

    async def listen_realtime(self, dexes: List[Dict[str, Any]]) -> None:
        self.is_running = True
        logger.info(f"Starting real-time listener for {self.chain_config.name}")
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
                            await self.rate_limiter.acquire()
                            block = await self.web3.eth.get_block(log["blockNumber"])
                            await self._process_swap_event(
                                dex,
                                log,
                                datetime.fromtimestamp(block["timestamp"])
                            )
                    self.latest_block = latest
                await asyncio.sleep(self.chain_config.block_time)
            except Exception as e:
                logger.error(f"Error in real-time listener for {self.chain_config.name}: {e}")
                await asyncio.sleep(5)

    async def stop(self):
        self.is_running = False


class MultiChainIngestor:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.ingestors: Dict[int, ChainIngestor] = {}
        self.tasks: List[asyncio.Task] = []

    async def initialize(self):
        for chain_config in CHAINS:
            ingestor = ChainIngestor(chain_config, self.storage)
            await ingestor.connect()
            self.ingestors[chain_config.chain_id] = ingestor

    async def sync_historical_all(self, from_block_override: Optional[Dict[int, int]] = None):
        tasks = []
        for chain_id, ingestor in self.ingestors.items():
            chain_config = ingestor.chain_config
            start_block = from_block_override.get(chain_id) if from_block_override else chain_config.start_block
            await ingestor.rate_limiter.acquire()
            latest_block = await ingestor.web3.eth.block_number
            for dex in chain_config.dexes:
                task = asyncio.create_task(
                    ingestor.sync_historical_swaps(dex, start_block, latest_block)
                )
                tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_swaps = sum(r for r in results if isinstance(r, int))
        logger.info(f"Historical sync complete. Total swaps synced: {total_swaps}")

    async def start_realtime_all(self):
        for ingestor in self.ingestors.values():
            task = asyncio.create_task(
                ingestor.listen_realtime(ingestor.chain_config.dexes)
            )
            self.tasks.append(task)

    async def stop_all(self):
        for ingestor in self.ingestors.values():
            await ingestor.stop()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def audit_pool(
        self,
        chain_id: int,
        pool_address: str,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None
    ) -> int:
        ingestor = self.ingestors.get(chain_id)
        if not ingestor:
            raise ValueError(f"Chain {chain_id} not configured")
        total_swaps = 0
        for dex in ingestor.chain_config.dexes:
            if start_block is None:
                start_block = ingestor.chain_config.start_block
            if end_block is None:
                await ingestor.rate_limiter.acquire()
                end_block = await ingestor.web3.eth.block_number
            swaps = await ingestor.sync_historical_swaps(
                dex,
                start_block,
                end_block,
                pool_address=pool_address
            )
            total_swaps += swaps
        return total_swaps
