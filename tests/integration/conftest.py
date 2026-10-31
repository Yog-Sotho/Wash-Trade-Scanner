"""
Fixtures for integration tests: a real PostgreSQL-backed Storage and a fake
EVM chain served over real HTTP JSON-RPC.

The fake chain encodes swap logs with the actual event ABI and serves them to
the production web3 client, so tests exercise the real path: settings ->
chain config -> web3 -> log decoding -> pricing -> PostgreSQL. Only the
blockchain node and the price API are simulated.
"""

import json
from typing import Any

import pytest
from aiohttp import web
from eth_abi import encode
from sqlalchemy.ext.asyncio import create_async_engine
from web3 import Web3

from config.settings import settings
from core.storage import Storage
from models.schemas import Base

V2_SWAP_TOPIC = Web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
_SELECTOR_TOKEN0 = Web3.keccak(text="token0()").hex()[:10]
_SELECTOR_TOKEN1 = Web3.keccak(text="token1()").hex()[:10]


def _word(address: str) -> str:
    return "0x" + "00" * 12 + address.lower().removeprefix("0x")


class FakeChain:
    """An in-memory chain answering the JSON-RPC methods the scanner uses."""

    def __init__(self, chain_id: int = 1, head: int = 100) -> None:
        self.chain_id = chain_id
        self.head = head
        self.block_timestamps: dict[int, int] = {}
        self.logs: list[dict[str, Any]] = []
        self.call_results: dict[tuple[str, str], str] = {}
        self.usd_prices: dict[str, float] = {}
        self.rpc_methods: list[str] = []
        self.rpc_url = ""
        self.coingecko_url = ""

    def set_pool_tokens(self, pool: str, token0: str, token1: str) -> None:
        self.call_results[(pool.lower(), _SELECTOR_TOKEN0)] = _word(token0)
        self.call_results[(pool.lower(), _SELECTOR_TOKEN1)] = _word(token1)

    def add_v2_swap(
        self,
        block: int,
        timestamp: int,
        pool: str,
        sender: str,
        to: str,
        amounts: tuple[int, int, int, int],  # amount0In, amount1In, amount0Out, amount1Out
    ) -> str:
        """Append a Uniswap V2 Swap log, ABI-encoded exactly as a node returns it."""
        self.block_timestamps[block] = timestamp
        self.head = max(self.head, block)
        log_index = sum(1 for log in self.logs if int(log["blockNumber"], 16) == block)
        tx_hash = Web3.keccak(text=f"tx-{block}-{log_index}").hex()
        self.logs.append(
            {
                "address": pool.lower(),
                "topics": [V2_SWAP_TOPIC, _word(sender), _word(to)],
                "data": "0x" + encode(["uint256"] * 4, list(amounts)).hex(),
                "blockNumber": hex(block),
                "blockHash": Web3.keccak(text=f"block-{block}").hex(),
                "transactionHash": tx_hash,
                "transactionIndex": hex(log_index),
                "logIndex": hex(log_index),
                "removed": False,
            }
        )
        return tx_hash

    # ------------------------------------------------------------- JSON-RPC

    def _block(self, number: int) -> dict[str, Any]:
        zero32 = "0x" + "00" * 32
        return {
            "number": hex(number),
            "hash": Web3.keccak(text=f"block-{number}").hex(),
            "parentHash": Web3.keccak(text=f"block-{number - 1}").hex(),
            "timestamp": hex(self.block_timestamps.get(number, 1_700_000_000 + number * 12)),
            "transactions": [],
            "gasLimit": hex(30_000_000),
            "gasUsed": "0x0",
            "miner": "0x" + "00" * 20,
            "extraData": "0x",
            "logsBloom": "0x" + "00" * 256,
            "nonce": "0x0000000000000000",
            "difficulty": "0x0",
            "totalDifficulty": "0x0",
            "size": "0x0",
            "stateRoot": zero32,
            "receiptsRoot": zero32,
            "transactionsRoot": zero32,
            "sha3Uncles": zero32,
            "mixHash": zero32,
            "uncles": [],
            "baseFeePerGas": "0x1",
        }

    def _get_logs(self, flt: dict[str, Any]) -> list[dict[str, Any]]:
        addresses = flt.get("address")
        if isinstance(addresses, str):
            addresses = [addresses]
        wanted = {a.lower() for a in addresses} if addresses else None
        topic0 = (flt.get("topics") or [None])[0]
        start, end = int(flt["fromBlock"], 16), int(flt["toBlock"], 16)
        return [
            log
            for log in self.logs
            if (wanted is None or log["address"] in wanted)
            and (topic0 is None or log["topics"][0] == topic0)
            and start <= int(log["blockNumber"], 16) <= end
        ]

    def _dispatch(self, method: str, params: list[Any]) -> Any:
        self.rpc_methods.append(method)
        if method == "web3_clientVersion":
            return "FakeChain/1.0"
        if method == "eth_chainId":
            return hex(self.chain_id)
        if method == "eth_blockNumber":
            return hex(self.head)
        if method == "eth_getLogs":
            return self._get_logs(params[0])
        if method == "eth_getBlockByNumber":
            return self._block(int(params[0], 16))
        if method == "eth_call":
            key = (params[0]["to"].lower(), params[0]["data"][:10])
            if key in self.call_results:
                return self.call_results[key]
            raise LookupError("execution reverted")
        raise NotImplementedError(method)

    async def handle_rpc(self, request: web.Request) -> web.Response:
        payload = await request.json()
        requests = payload if isinstance(payload, list) else [payload]
        responses = []
        for req in requests:
            try:
                result = self._dispatch(req["method"], req.get("params", []))
                responses.append({"jsonrpc": "2.0", "id": req["id"], "result": result})
            except NotImplementedError:
                error = {"code": -32601, "message": "Method not found"}
                responses.append({"jsonrpc": "2.0", "id": req["id"], "error": error})
            except LookupError as exc:
                error = {"code": 3, "message": str(exc)}
                responses.append({"jsonrpc": "2.0", "id": req["id"], "error": error})
        body = responses if isinstance(payload, list) else responses[0]
        return web.Response(text=json.dumps(body), content_type="application/json")

    async def handle_coingecko(self, request: web.Request) -> web.Response:
        """Hourly points at a constant price across the requested range."""
        price = self.usd_prices.get(request.match_info["coin_id"])
        start, end = int(request.query["from"]), int(request.query["to"])
        first_hour = start - start % 3600 + 3600
        prices = [] if price is None else [[t * 1000, price] for t in range(first_hour, end, 3600)]
        return web.json_response({"prices": prices, "market_caps": [], "total_volumes": []})


@pytest.fixture
async def fake_chain():
    chain = FakeChain()
    app = web.Application()
    app.router.add_post("/", chain.handle_rpc)
    app.router.add_get("/api/v3/coins/{coin_id}/market_chart/range", chain.handle_coingecko)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    chain.rpc_url = f"http://{host}:{port}/"
    chain.coingecko_url = f"http://{host}:{port}/api/v3"
    yield chain
    await runner.cleanup()


@pytest.fixture
async def pg_storage():
    """A real Storage on the DATABASE_* PostgreSQL with a freshly created schema.

    Tables are dropped first so that Storage.initialize() always runs the full
    CREATE TABLE / CREATE INDEX DDL. Truncating instead would let an existing
    schema hide DDL bugs (create_all skips tables -- and their indexes -- that
    already exist), making the outcome depend on the database's history.
    """
    engine = create_async_engine(settings.DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()

    storage = Storage()
    await storage.initialize()
    yield storage
    await storage.close()
