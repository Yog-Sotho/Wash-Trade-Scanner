"""
Performance benchmark (roadmap Phase 5): a full audit of a real pool, as an
operator runs it, with the cost of every stage measured.

The pool is the first N trades (default 10,000) of a recorded fixture
(evaluation/fixtures/univ3_usdc_weth_005: one day of the Uniswap V3 USDC/WETH
0.05% pool). A fake node, in its own process, replays them as real Uniswap
V3 Swap logs over HTTP JSON-RPC, answering each request after a fixed delay
that stands for a hosted node's round trip. Its blocks carry
FILLER_TRANSACTIONS other transactions each, so a block request weighs what a
real one does. The audit runs through AuditRunner against a real
PostgreSQL, exactly as `wash-audit` does: ingestion (logs, blocks,
transactions, USD prices), funding-link clustering, the detectors, an ML
model and the report. Per stage it records wall time, SQL statements and RPC
requests.

    python -m evaluation.perf --out evaluation/perf_results.json

It needs DATABASE_* to name a disposable database (the name must contain
"test", "perf" or "bench"): every table in it is dropped first.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import io
import json
import multiprocessing
import os
import resource
import statistics
import sys
import tempfile
import time
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator
from contextvars import ContextVar
from datetime import UTC
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web
from eth_abi.abi import encode
from sqlalchemy import event, text
from web3 import Web3

from config.chains import get_chain_config
from config.settings import settings
from evaluation.dataset import load_fixture, to_swap_trades

DEFAULT_FIXTURE = "univ3_usdc_weth_005"
DEFAULT_TRADES = 10_000
DEFAULT_LATENCY_MS = 20.0
# Other transactions in every block: an assumption standing for a typical
# Ethereum block, stated so that the payload figures can be scaled.
FILLER_TRANSACTIONS = 160
# Their calldata sizes in bytes, cycled (a transfer, swaps, a multicall...).
_FILLER_INPUT_BYTES = (68, 68, 68, 132, 196, 260, 324, 452, 580, 900)

V3_SWAP_TOPIC = Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
_SELECTORS = {
    Web3.keccak(text="token0()").hex()[:10]: "token0",
    Web3.keccak(text="token1()").hex()[:10]: "token1",
    Web3.keccak(text="factory()").hex()[:10]: "factory",
}


def _word(address: str) -> str:
    return "0x" + "00" * 12 + address.lower().removeprefix("0x")


# ---------------------------------------------------------------- the node


class RawJSON:
    """A result already serialised."""

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text


class ReplayNode:
    """A JSON-RPC node holding one pool's swaps, indexed by block.

    Answers what an audit asks: logs, blocks, transactions, the pool's
    token0/token1/factory, the head. trace_filter is unsupported, as on most
    hosted nodes. Each HTTP request is answered after `latency` seconds.
    """

    def __init__(self, rows: list[dict[str, Any]], latency: float, chain_id: int = 1) -> None:
        self.latency = latency
        self.chain_id = chain_id
        self.requests: Counter[str] = Counter()
        self.response_bytes: Counter[str] = Counter()
        self.pool = rows[0]["pool_address"].lower()
        first = rows[0]
        token_in, token_out = first["token_in"].lower(), first["token_out"].lower()
        self.token0, self.token1 = sorted((token_in, token_out))
        v3 = next(d for d in get_chain_config(chain_id)["dexes"] if d["type"] == "v3")
        self.factory = v3["factory"]
        self.logs_by_block: dict[int, list[dict[str, Any]]] = defaultdict(list)
        self.timestamps: dict[int, int] = {}
        self.transactions: dict[str, dict[str, Any]] = {}
        for row in rows:
            self._add(row)
        self.blocks = sorted(self.logs_by_block)
        self.head = self.blocks[-1]
        self.usd_per_eth = self._eth_price(rows)
        self.tx_hashes_by_block: dict[int, list[str]] = defaultdict(list)
        for tx_hash, tx in self.transactions.items():
            self.tx_hashes_by_block[int(tx["blockNumber"], 16)].append(tx_hash)
        # Pre-serialised: the same other transactions in every block.
        filler = [self._filler_tx(i) for i in range(FILLER_TRANSACTIONS)]
        self.filler_full = ",".join(json.dumps(tx) for tx in filler)
        self.filler_hashes = ",".join(json.dumps(tx["hash"]) for tx in filler)

    def _add(self, row: dict[str, Any]) -> None:
        block = int(row["block_number"])
        self.timestamps[block] = int(row["block_timestamp"].replace(tzinfo=UTC).timestamp())
        amount_in, amount_out = int(round(row["amount_in"])), int(round(row["amount_out"]))
        if row["token_in"].lower() == self.token0:
            amount0, amount1 = amount_in, -amount_out
        else:
            amount0, amount1 = -amount_out, amount_in
        data = encode(
            ["int256", "int256", "uint160", "uint128", "int24"],
            [amount0, amount1, 2**96, 10**18, 0],
        )
        tx_hash = row["transaction_hash"]
        self.logs_by_block[block].append(
            {
                "address": self.pool,
                "topics": [V3_SWAP_TOPIC, _word(row["sender"]), _word(row["recipient"])],
                "data": "0x" + data.hex(),
                "blockNumber": hex(block),
                "blockHash": Web3.keccak(text=f"block-{block}").hex(),
                "transactionHash": tx_hash,
                "transactionIndex": "0x0",
                "logIndex": hex(int(row["log_index"])),
                "removed": False,
            }
        )
        gas_price_wei = int(round((row.get("gas_price") or 1.0) * 10**9))
        self.transactions[tx_hash] = {
            "blockHash": Web3.keccak(text=f"block-{block}").hex(),
            "blockNumber": hex(block),
            "from": Web3.to_checksum_address(row["tx_from"]),
            "to": Web3.to_checksum_address(row["tx_to"]) if row.get("tx_to") else None,
            "gas": hex(300_000),
            "gasPrice": hex(gas_price_wei),
            "hash": tx_hash,
            "input": "0x" + "ab" * 580,  # a router swap's calldata
            "nonce": "0x0",
            "transactionIndex": "0x0",
            "value": "0x0",
            "type": "0x0",
            "chainId": hex(self.chain_id),
            "v": "0x25",
            "r": "0x" + "11" * 32,
            "s": "0x" + "22" * 32,
        }

    def _filler_tx(self, i: int) -> dict[str, Any]:
        """An EIP-1559 transaction as a node returns it in a full block."""
        return {
            "blockHash": "0x" + "00" * 32,
            "blockNumber": "0x0",
            "from": Web3.to_checksum_address(f"0x{i + 1:040x}"),
            "gas": hex(150_000),
            "gasPrice": hex(12 * 10**9),
            "maxFeePerGas": hex(20 * 10**9),
            "maxPriorityFeePerGas": hex(10**9),
            "hash": Web3.keccak(text=f"filler-{i}").hex(),
            "input": "0x" + "cd" * _FILLER_INPUT_BYTES[i % len(_FILLER_INPUT_BYTES)],
            "nonce": hex(i),
            "to": Web3.to_checksum_address(f"0x{i + 7:040x}"),
            "transactionIndex": hex(i),
            "value": "0x0",
            "type": "0x2",
            "accessList": [],
            "chainId": hex(self.chain_id),
            "v": "0x1",
            "yParity": "0x1",
            "r": "0x" + "33" * 32,
            "s": "0x" + "44" * 32,
        }

    def _eth_price(self, rows: list[dict[str, Any]]) -> float:
        """Median USD per ETH implied by the fixture's priced WETH legs."""
        weth = self.token1  # USDC < WETH by address
        prices = [
            r["amount_out_usd"] / (r["amount_out"] / 1e18)
            for r in rows
            if r["token_out"].lower() == weth and r.get("amount_out_usd") and r["amount_out"]
        ]
        return statistics.median(prices) if prices else 3000.0

    def _block(self, number: int, full: bool) -> RawJSON:
        """The block as JSON text, its transactions as objects (`full`) or as
        hashes: the pool's swaps' transactions, then the filler."""
        ours = [
            json.dumps(self.transactions[h] if full else h) for h in self.tx_hashes_by_block[number]
        ]
        transactions = ",".join([*ours, self.filler_full if full else self.filler_hashes])
        header = json.dumps(self._block_header(number))
        return RawJSON(f'{header[:-1]}, "transactions": [{transactions}]}}')

    def _block_header(self, number: int) -> dict[str, Any]:
        zero32 = "0x" + "00" * 32
        return {
            "number": hex(number),
            "hash": Web3.keccak(text=f"block-{number}").hex(),
            "parentHash": Web3.keccak(text=f"block-{number - 1}").hex(),
            "timestamp": hex(self.timestamps.get(number, 1_700_000_000 + number * 12)),
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

    def _logs(self, flt: dict[str, Any]) -> list[dict[str, Any]]:
        start, end = int(flt["fromBlock"], 16), int(flt["toBlock"], 16)
        topic0 = (flt.get("topics") or [None])[0]
        if topic0 not in (None, V3_SWAP_TOPIC):
            return []
        out: list[dict[str, Any]] = []
        for block in self.blocks[bisect_left(self.blocks, start) : bisect_right(self.blocks, end)]:
            out.extend(self.logs_by_block[block])
        return out

    def dispatch(self, method: str, params: list[Any]) -> Any:
        self.requests[method] += 1
        if method == "web3_clientVersion":
            return "ReplayNode/1.0"
        if method == "eth_chainId":
            return hex(self.chain_id)
        if method == "eth_blockNumber":
            return hex(self.head)
        if method == "eth_getLogs":
            return self._logs(params[0])
        if method == "eth_getBlockByNumber":
            return self._block(int(params[0], 16), bool(params[1]) if len(params) > 1 else False)
        if method == "eth_getTransactionByHash":
            return self.transactions.get(params[0])
        if method == "eth_call":
            to, selector = params[0]["to"].lower(), params[0]["data"][:10]
            name = _SELECTORS.get(selector) if to == self.pool else None
            if name == "token0":
                return _word(self.token0)
            if name == "token1":
                return _word(self.token1)
            if name == "factory":
                return _word(self.factory)
            raise LookupError("execution reverted")
        raise NotImplementedError(method)

    async def handle_rpc(self, request: web.Request) -> web.Response:
        payload = await request.json()
        await asyncio.sleep(self.latency)
        batch = payload if isinstance(payload, list) else [payload]
        replies = []
        for req in batch:
            head = f'{{"jsonrpc": "2.0", "id": {json.dumps(req["id"])}'
            try:
                result = self.dispatch(req["method"], req.get("params", []))
                raw = result.text if isinstance(result, RawJSON) else json.dumps(result)
                reply = f'{head}, "result": {raw}}}'
            except NotImplementedError:
                reply = f'{head}, "error": {{"code": -32601, "message": "Method not found"}}}}'
            except LookupError as exc:
                reply = f'{head}, "error": {{"code": 3, "message": {json.dumps(str(exc))}}}}}'
            self.response_bytes[req["method"]] += len(reply)
            replies.append(reply)
        body = f"[{','.join(replies)}]" if isinstance(payload, list) else replies[0]
        return web.Response(text=body, content_type="application/json")

    async def handle_stats(self, request: web.Request) -> web.Response:
        return web.json_response(
            {"requests": dict(self.requests), "bytes": dict(self.response_bytes)}
        )

    async def handle_prices(self, request: web.Request) -> web.Response:
        """CoinGecko's market_chart/range: hourly points at the fixture's price."""
        self.requests["coingecko:market_chart_range"] += 1
        await asyncio.sleep(self.latency)
        start, end = int(request.query["from"]), int(request.query["to"])
        first_hour = start - start % 3600
        prices = [[t * 1000, self.usd_per_eth] for t in range(first_hour, end + 3600, 3600)]
        body = json.dumps({"prices": prices, "market_caps": [], "total_volumes": []})
        self.response_bytes["coingecko:market_chart_range"] += len(body)
        return web.Response(text=body, content_type="application/json")


def _serve(rows: list[dict[str, Any]], latency: float, conn: Connection, parent: int) -> None:
    """Child process: serve a ReplayNode, send its URL, run until the
    benchmark kills it or exits (even if it is killed itself)."""

    async def serve() -> None:
        node = ReplayNode(rows, latency)
        app = web.Application(client_max_size=2**24)
        app.router.add_post("/", node.handle_rpc)
        app.router.add_get("/api/v3/coins/{coin_id}/market_chart/range", node.handle_prices)
        app.router.add_get("/__stats", node.handle_stats)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        host, port = runner.addresses[0][:2]
        conn.send(
            {
                "url": f"http://{host}:{port}",
                "pool": node.pool,
                "filler_block_bytes": len(node.filler_full) + 2,
            }
        )
        while os.getppid() == parent:
            await asyncio.sleep(1)

    asyncio.run(serve())


class NodeProcess:
    """A ReplayNode in a child process, so that serving it costs the audit
    no CPU; its request counters are read over HTTP."""

    def __init__(self, rows: list[dict[str, Any]], latency: float) -> None:
        ctx = multiprocessing.get_context("spawn")
        parent, child = ctx.Pipe()
        self.process = ctx.Process(
            target=_serve, args=(rows, latency, child, os.getpid()), daemon=True
        )
        self.process.start()
        if not parent.poll(120):
            self.process.kill()
            raise RuntimeError("the replay node did not start")
        info = parent.recv()
        self.url: str = info["url"]
        self.pool: str = info["pool"]
        self.filler_block_bytes: int = info["filler_block_bytes"]
        self._session: aiohttp.ClientSession | None = None

    async def stats(self) -> tuple[Counter[str], Counter[str]]:
        """(requests, response bytes) by method, since the node started."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        async with self._session.get(f"{self.url}/__stats") as response:
            data = await response.json()
        return Counter(data["requests"]), Counter(data["bytes"])

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
        self.process.kill()
        self.process.join(10)


# ----------------------------------------------------------- measurement

_stage: ContextVar[str] = ContextVar("benchmark_stage", default="other")


class Meter:
    """Wall time, SQL statements and RPC requests per audit stage."""

    STAGES = {
        # stage: (module path, class name, method name)
        "ingest": ("core.ingestor", "MultiChainIngestor", "audit_pool"),
        "load_trades": ("core.storage", "Storage", "get_pool_trades"),
        "clear_labels": ("core.storage", "Storage", "clear_pool_labels"),
        "clustering": ("core.entity_clustering", "EntityClusterer", "cluster_addresses"),
        "heuristics": ("core.heuristics", "HeuristicDetector", "run_all_heuristics"),
        "write_labels": ("core.storage", "Storage", "update_trade_labels"),
        "ml": ("core.ml_detector", "MLDetector", "detect_wash_trades"),
        "audit_log": ("core.storage", "Storage", "create_audit_log"),
    }

    def __init__(self, node: NodeProcess) -> None:
        self.node = node
        self.seconds: defaultdict[str, float] = defaultdict(float)
        self.calls: Counter[str] = Counter()
        self.sql: Counter[str] = Counter()
        self.rpc: dict[str, Counter[str]] = defaultdict(Counter)
        self.rpc_bytes: Counter[str] = Counter()
        self._patches: list[tuple[Any, str, Any]] = []

    def on_sql(self, *_args: Any) -> None:
        self.sql[_stage.get()] += 1

    def _wrap(self, stage: str, method: Callable[..., Any]) -> Callable[..., Any]:
        meter = self

        @functools.wraps(method)
        async def timed(*args: Any, **kwargs: Any) -> Any:
            token = _stage.set(stage)
            requests_before, bytes_before = await meter.node.stats()
            start = time.perf_counter()
            try:
                return await method(*args, **kwargs)
            finally:
                meter.seconds[stage] += time.perf_counter() - start
                meter.calls[stage] += 1
                requests_after, bytes_after = await meter.node.stats()
                meter.rpc[stage].update(requests_after - requests_before)
                meter.rpc_bytes[stage] += sum((bytes_after - bytes_before).values())
                _stage.reset(token)

        return timed

    @contextlib.contextmanager
    def installed(self) -> Iterator[None]:
        import importlib

        for stage, (module, cls_name, name) in self.STAGES.items():
            cls = getattr(importlib.import_module(module), cls_name)
            original = getattr(cls, name)
            setattr(cls, name, self._wrap(stage, original))
            self._patches.append((cls, name, original))
        try:
            yield
        finally:
            for cls, name, original in reversed(self._patches):
                setattr(cls, name, original)
            self._patches.clear()

    def summary(
        self, total_seconds: float, run_requests: Counter[str], run_bytes: Counter[str]
    ) -> dict[str, Any]:
        """Per stage; `run_requests`/`run_bytes` are the node's counts for the
        whole run, so requests made outside the stages count as "other"."""
        self.rpc["other"] = run_requests - sum(self.rpc.values(), Counter())
        self.rpc_bytes["other"] = sum(run_bytes.values()) - sum(self.rpc_bytes.values())
        stages = {}
        for stage in [*self.STAGES, "other"]:
            seconds = self.seconds.get(stage, 0.0)
            if stage == "other":
                seconds = max(0.0, total_seconds - sum(self.seconds.values()))
            rpc = self.rpc.get(stage, Counter())
            if not (seconds or self.sql.get(stage) or rpc):
                continue
            stages[stage] = {
                "seconds": round(seconds, 3),
                "calls": self.calls.get(stage, 0),
                "sql_statements": self.sql.get(stage, 0),
                "rpc_requests": sum(rpc.values()),
                "rpc_mb": round(self.rpc_bytes.get(stage, 0) / 1e6, 2),
                "rpc_by_method": dict(sorted(rpc.items())),
            }
        return {
            "wall_seconds": round(total_seconds, 3),
            "sql_statements": sum(self.sql.values()),
            "rpc_requests": sum(run_requests.values()),
            "rpc_mb": round(sum(run_bytes.values()) / 1e6, 2),
            "rpc_by_method": dict(sorted(run_requests.items())),
            "stages": stages,
        }


# ------------------------------------------------------------------ runs


def _check_disposable_database() -> None:
    name = settings.DATABASE_NAME.lower()
    if not any(word in name for word in ("test", "perf", "bench")):
        raise SystemExit(
            f"Refusing to drop every table in {settings.DATABASE_NAME!r}: point DATABASE_NAME "
            "at a disposable database whose name contains 'test', 'perf' or 'bench'."
        )


async def _empty_database() -> None:
    from sqlalchemy.ext.asyncio import create_async_engine

    from models.schemas import Base

    engine = create_async_engine(settings.DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await engine.dispose()


def _fit_model(rows: list[dict[str, Any]], path: Path) -> None:
    """An Isolation Forest fitted on this pool's own features, with measured
    rates that let its hits label trades, so the ML stage does its full work."""
    from core.feature_engineer import features_from_trades
    from core.ml_detector import MLDetector

    detector = MLDetector(storage=None, feature_engineer=None)  # type: ignore[arg-type]
    frame = features_from_trades(to_swap_trades(rows))
    columns = [c for c in detector.feature_columns if c in frame.columns]
    detector.fit_matrix(frame[columns].fillna(0).values)
    detector.metrics = {"threshold": 0.55, "tpr": 0.5, "fpr_upper": 0.001}
    detector.save_model(str(path))


async def run_benchmark(
    trades: int = DEFAULT_TRADES,
    latency_ms: float = DEFAULT_LATENCY_MS,
    fixture: str = DEFAULT_FIXTURE,
) -> dict[str, Any]:
    from core.storage import Storage
    from core.validators import AuditParameters
    from scripts.run_audit import AuditRunner

    _check_disposable_database()
    loaded = load_fixture(fixture)
    rows = sorted(loaded.rows, key=lambda r: (r["block_number"], r["log_index"]))[:trades]
    node = NodeProcess(rows, latency_ms / 1000)

    results: dict[str, Any] = {
        "fixture": fixture,
        "trades": len(rows),
        "blocks": [rows[0]["block_number"], rows[-1]["block_number"]],
        "blocks_with_swaps": len({r["block_number"] for r in rows}),
        "transactions": len({r["transaction_hash"] for r in rows}),
        "rpc_latency_ms": latency_ms,
        "rpc_rate_limit": settings.RPC_RATE_LIMIT,
        "filler_transactions_per_block": FILLER_TRANSACTIONS,
        "filler_kb_per_full_block": round(node.filler_block_bytes / 1000, 1),
        "runs": {},
    }
    with tempfile.TemporaryDirectory() as tmp:
        model_path = Path(tmp) / "model.pkl"
        _fit_model(rows, model_path)
        overrides = {
            "ETH_RPC_URL": f"{node.url}/",
            "COINGECKO_API_URL": f"{node.url}/api/v3",
            "NATIVE_PRICE_SOURCE": "coingecko",
            "ML_MODEL_PATH": str(model_path),
        }
        saved = {key: getattr(settings, key) for key in overrides}
        for key, value in overrides.items():
            setattr(settings, key, value)
        try:
            await _empty_database()
            storage = Storage()
            await storage.initialize()
            assert storage.engine is not None
            params = AuditParameters(
                chain_id=1,
                pool_address=Web3.to_checksum_address(node.pool),
                start_block=rows[0]["block_number"],
                end_block=rows[-1]["block_number"],
                use_ml=True,
                use_heuristics=True,
            )
            # First on an empty database, then again with the trades stored.
            for run_name in ("audit", "re_audit"):
                run_requests, run_bytes = await node.stats()
                meter = Meter(node)
                event.listen(storage.engine.sync_engine, "before_cursor_execute", meter.on_sql)
                audit = AuditRunner()
                audit.storage = storage
                with meter.installed(), contextlib.redirect_stdout(io.StringIO()):
                    start = time.perf_counter()
                    outcome = await audit.run_audit(params)
                    total = time.perf_counter() - start
                event.remove(storage.engine.sync_engine, "before_cursor_execute", meter.on_sql)
                requests_after, bytes_after = await node.stats()
                summary = meter.summary(
                    total, requests_after - run_requests, bytes_after - run_bytes
                )
                summary["trades_processed"] = outcome["trades_processed"]
                summary["wash_trades_detected"] = outcome["wash_trades_detected"]
                summary["peak_rss_mb"] = round(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1
                )
                results["runs"][run_name] = summary
            await storage.close()
        finally:
            for key, value in saved.items():
                setattr(settings, key, value)
            await node.close()
    return results


def render(results: dict[str, Any]) -> str:
    lines = [
        f"{results['trades']} trades ({results['fixture']}, blocks "
        f"{results['blocks'][0]}-{results['blocks'][1]}), node round trip "
        f"{results['rpc_latency_ms']:g} ms",
    ]
    for run_name, run in results["runs"].items():
        lines.append(
            f"\n{run_name}: {run['wall_seconds']:.1f} s, {run['sql_statements']} SQL statements, "
            f"{run['rpc_requests']} RPC requests ({run['rpc_mb']} MB), "
            f"{run['trades_processed']} trades, {run['wash_trades_detected']} wash, "
            f"peak RSS {run['peak_rss_mb']} MB"
        )
        lines.append(f"  {'stage':<14}{'seconds':>9}{'SQL':>7}{'RPC':>8}{'MB':>9}")
        for stage, row in run["stages"].items():
            lines.append(
                f"  {stage:<14}{row['seconds']:>9.2f}{row['sql_statements']:>7}"
                f"{row['rpc_requests']:>8}{row['rpc_mb']:>9.2f}"
            )
        lines.append(f"  by method: {run['rpc_by_method']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--trades", type=int, default=DEFAULT_TRADES)
    parser.add_argument("--latency-ms", type=float, default=DEFAULT_LATENCY_MS)
    parser.add_argument("--fixture", default=DEFAULT_FIXTURE)
    parser.add_argument("--out", type=Path, help="write the results as JSON")
    args = parser.parse_args(argv)
    from core.log_config import configure_logging

    configure_logging(level="WARNING")
    results = asyncio.run(run_benchmark(args.trades, args.latency_ms, args.fixture))
    print(render(results))
    if args.out:
        args.out.write_text(json.dumps(results, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
