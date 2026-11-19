"""
End-to-end: a full audit through every real component -- settings, chain
config, web3 over HTTP JSON-RPC, ABI log decoding, transaction-sender lookup,
USD pricing, PostgreSQL, the detector stack with calibrated scoring
(config/detector_calibration.json), reporting -- then the API and the
real-time monitor on the same data. Only the blockchain node and the price
API are simulated.

Each defect fixed in Phase 1 breaks at least one of these tests:
ETH_RPC_URL ignored / all chains connected eagerly (no audit could start),
duplicate index in the schema (no entry point could start), USD values never
populated (all $0), pool address case mismatch (API views empty, real-time
monitor never ingested). Phase 2: trades are attributed to the transaction's
sender, not the router or bot contract that called the pool.

Scenario (Ethereum, a Uniswap-V2-style USDC/WETH pool, within one hour):
  1-6. A, through the router: three round trips, legs 2 blocks apart, each
       selling the 0.5 WETH back for 995 USDC (a 0.5% loss, like a pool fee):
       values match within 1%, but A does not end flat -> round_trip
  7.   D, through the router: one ordinary swap   -> clean
  8.   E through its own bot contract K (the swap's sender and recipient
       are both K -- labelled "self_trading" before Phase 2)  -> clean
  9.   F, through the router: one ordinary swap   -> clean
Every other swap moves 1,000 USDC against 0.5 WETH, with ETH at $2,000, so
those legs are worth exactly $1,000.
"""

import asyncio
import hashlib
import hmac
import json
import time
from datetime import UTC, datetime

import httpx
import pytest
from aiohttp import web
from prometheus_client import REGISTRY
from sqlalchemy import select
from web3 import Web3

from api.server import create_app
from config.settings import settings
from core.alerts import AlertDispatcher, WebhookSink
from core.calibration import wash_probability
from core.realtime_monitor import RealtimeMonitor
from core.storage import Storage
from core.validators import AuditParameters
from core.watch import PoolWatcher
from models.schemas import SwapTrade
from scripts.run_audit import AuditRunner

pytestmark = pytest.mark.integration

USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # token0, 6 decimals
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # token1, 18 decimals
POOL = "0x9e8ce2cc2fd75fb0de6a1e1b6b1bf6c7f33a4d6e"
POOL_CHECKSUMMED = Web3.to_checksum_address(POOL)
A, D, E, F, K = ("0x" + ch * 40 for ch in "adef9")
ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"  # Uniswap V2 Router02

USDC_1000 = 1_000 * 10**6
WETH_HALF = 5 * 10**17
SELL_USDC = (USDC_1000, 0, 0, WETH_HALF)  # amount0In, amount1In, amount0Out, amount1Out
SELL_WETH = (0, WETH_HALF, USDC_1000, 0)
USDC_995 = 995 * 10**6
SELL_BACK = (0, WETH_HALF, USDC_995, 0)  # 0.5 WETH back for 995 USDC
T0 = int(datetime(2026, 1, 15, 12, 0, tzinfo=UTC).timestamp())


@pytest.fixture
def chain(fake_chain, monkeypatch):
    monkeypatch.setattr(settings, "ETH_RPC_URL", fake_chain.rpc_url)
    monkeypatch.setattr(settings, "COINGECKO_API_URL", fake_chain.coingecko_url)
    monkeypatch.setattr(settings, "NATIVE_PRICE_SOURCE", "coingecko")
    fake_chain.usd_prices["ethereum"] = 2000.0
    fake_chain.set_pool_tokens(POOL, USDC, WETH)
    return fake_chain


def _add_scenario(chain) -> None:
    # (block, swap sender, swap recipient, transaction sender, amounts)
    swaps = [
        (101, ROUTER, A, A, SELL_USDC),
        (103, ROUTER, A, A, SELL_BACK),
        (110, ROUTER, A, A, SELL_USDC),
        (112, ROUTER, A, A, SELL_BACK),
        (120, ROUTER, A, A, SELL_USDC),
        (122, ROUTER, A, A, SELL_BACK),
        (125, ROUTER, D, D, SELL_USDC),
        (126, K, K, E, SELL_WETH),
        (127, ROUTER, F, F, SELL_USDC),
    ]
    for block, sender, to, tx_from, amounts in swaps:
        chain.add_v2_swap(block, T0 + 12 * (block - 101), POOL, sender, to, amounts, tx_from)


async def _stored_trades(storage: Storage) -> list[SwapTrade]:
    async with await storage.get_session() as session:
        result = await session.execute(select(SwapTrade).order_by(SwapTrade.block_number))
        return list(result.scalars().all())


async def _run_audit(storage: Storage) -> dict:
    runner = AuditRunner()
    runner.storage = storage
    params = AuditParameters(
        chain_id=1, pool_address=POOL_CHECKSUMMED, start_block=100, end_block=130, use_ml=False
    )
    return await runner.run_audit(params)


async def test_funding_links_join_an_operators_wallets_and_not_a_routers_users(
    pg_storage, chain, monkeypatch
):
    """First-funder clustering over a real audit: the operator who funded D
    and E makes them one entity; the router that refunded ETH to A and F,
    and the bot contract K, link nobody."""
    monkeypatch.setattr(settings, "ENTITY_SCAN_MAX_BLOCKS", 1_000)
    _add_scenario(chain)
    operator = "0x" + "b" * 40
    chain.add_transfer(105, operator, D, 10**17)
    chain.add_transfer(106, operator, E, 10**17)
    chain.add_transfer(107, ROUTER, F, 10**15)
    chain.add_transfer(108, ROUTER, A, 10**15)
    chain.add_transfer(109, A, F, 10**15)  # after F's first funding: a payment
    chain.contracts |= {ROUTER, K}

    result = await _run_audit(pg_storage)

    assert result["funding_clusters"]["method"] == "block_scan"
    assert result["funding_clusters"]["complete"] is True
    clusters = await pg_storage.get_address_clusters(1)
    assert [set(c.addresses) for c in clusters] == [{operator, D, E}]
    # Wallets are known from their transactions, and K, a recipient nobody
    # funded, is in no link: only the two funders needed a code lookup.
    assert chain.rpc_methods.count("eth_getCode") == 2  # the router, the operator


async def test_full_audit_detects_prices_and_reports(pg_storage, chain):
    _add_scenario(chain)

    result = await _run_audit(pg_storage)

    # The configured endpoint was actually used, and logs were fetched from it.
    assert "eth_getLogs" in chain.rpc_methods
    # One request per block gave each swap its time and its transaction's
    # sender: none per transaction.
    swap_blocks = {int(log["blockNumber"], 16) for log in chain.logs}
    assert chain.rpc_methods.count("eth_getBlockByNumber") == len(swap_blocks)
    assert "eth_getTransactionByHash" not in chain.rpc_methods
    assert result["trades_processed"] == 9
    assert result["wash_trades_detected"] == 6
    # The fake node, like most hosted ones, has no trace_filter: the audit says
    # its funding links were not looked for.
    assert result["excluded_as_mev"] == {"sandwich": 0, "arbitrage": 0}  # none here
    assert result["funding_clusters"]["method"] == "skipped"
    assert result["funding_clusters"]["complete"] is False
    # Nine swaps are too few to read their sizes' statistics.
    assert result["trade_size_forensics"] == {
        "trades": 9,
        "round_share": None,
        "benford_mad": None,
        "benford_conformity": "insufficient data",
    }

    trades = await _stored_trades(pg_storage)
    assert len(trades) == 9
    sell_backs = {1, 3, 5}  # A's second legs: 995 USDC out
    for i, trade in enumerate(trades):
        assert trade.pool_address == POOL  # stored lowercase
        assert trade.amount_in_usd == pytest.approx(1000.0)  # USDC at $1 or 0.5 WETH at $2,000
        expected_out = 995.0 if i in sell_backs else 1000.0
        assert trade.amount_out_usd == pytest.approx(expected_out)
        assert trade.volume_usd == pytest.approx(expected_out if i in sell_backs else 1000.0)
        assert trade.gas_price == pytest.approx(25.0)  # gwei, from the transaction
        assert trade.dex_name == "UniswapV2"  # synced once, not once per V2-style DEX

    labels = [(t.tx_from, t.detection_method, t.is_wash_trade) for t in trades]
    assert labels == [(A, "round_trip", True)] * 6 + [
        (D, None, False),
        (E, None, False),  # the bot contract's swap: sender == recipient, not wash
        (F, None, False),
    ]
    expected_score = wash_probability("round_trip", settings.WASH_PRIOR)
    assert expected_score >= settings.SUSPICIOUS_ACTIVITY_THRESHOLD
    assert all(t.wash_trade_score == pytest.approx(expected_score) for t in trades[:6])
    # Every label is stored with its evidence: the round trip's two legs (the
    # partner leg found on chain), their gap, and what the hit is worth.
    for trade in trades[:6]:
        entry = trade.wash_evidence["detectors"][0]
        assert trade.wash_evidence["decided_by"] == "round_trip"
        assert entry["probability"] == pytest.approx(expected_score, rel=1e-5)
        assert trade.transaction_hash in [leg["tx"] for leg in entry["trades"]]
        assert entry["details"]["round_trips_by_trader"] == 3
        assert all(leg["trader"] == A for leg in entry["trades"])
    assert all(t.wash_evidence is None for t in trades[6:])

    metrics = result["risk_metrics"]
    assert metrics["total_volume_usd"] == pytest.approx(8985.0)
    assert metrics["wash_trade_volume_usd"] == pytest.approx(5985.0)
    assert metrics["wash_trade_volume_ratio"] == pytest.approx(5985 / 8985)
    assert metrics["severity"] == "CRITICAL"
    assert metrics["wash_volume_by_method"] == pytest.approx({"round_trip": 5985.0})

    logs = await pg_storage.get_recent_audit_logs()
    assert len(logs) == 1
    assert logs[0].pool_address == POOL
    assert logs[0].results_summary["severity"] == "CRITICAL"
    # Regression: datetimes in the results made the JSON column write fail,
    # so every audit crashed at its final step.
    assert logs[0].results_summary["first_trade_timestamp"] == "2026-01-15T12:00:00"


async def test_reaudit_is_idempotent(pg_storage, chain):
    _add_scenario(chain)

    await _run_audit(pg_storage)
    first = await _stored_trades(pg_storage)
    chain.rpc_methods.clear()
    result = await _run_audit(pg_storage)

    assert result["trades_processed"] == 9
    assert result["wash_trades_detected"] == 6
    again = await _stored_trades(pg_storage)
    assert [(t.id, t.block_timestamp, t.tx_from, t.volume_usd) for t in again] == [
        (t.id, t.block_timestamp, t.tx_from, t.volume_usd) for t in first
    ]
    # The stored swaps were not fetched again: the re-audit's node requests
    # are the head, the pool's factory and the logs.
    assert "eth_getBlockByNumber" not in chain.rpc_methods
    assert "eth_getTransactionByHash" not in chain.rpc_methods
    assert "eth_getLogs" in chain.rpc_methods


async def test_reaudit_clears_labels_the_current_detectors_do_not_give(pg_storage, chain):
    """Regression: re-auditing only ever added labels, so a trade flagged by an
    earlier run (or an earlier detector) stayed flagged forever."""
    _add_scenario(chain)
    await _run_audit(pg_storage)
    trades = await _stored_trades(pg_storage)
    bot_swap = next(t for t in trades if t.tx_from == E)
    await pg_storage.save_trade_labels(
        [
            {
                "id": bot_swap.id,
                "is_wash_trade": True,
                "wash_trade_score": 1.0,
                "detection_method": "self_trading",
                "wash_evidence": {"decided_by": "self_trading", "detectors": []},
            }
        ]
    )

    result = await _run_audit(pg_storage)

    assert result["wash_trades_detected"] == 6
    relabelled = next(t for t in await _stored_trades(pg_storage) if t.tx_from == E)
    assert relabelled.is_wash_trade is False
    assert relabelled.detection_method is None
    assert relabelled.wash_evidence is None  # stale evidence goes with the stale label


async def test_api_serves_audited_pool(pg_storage, chain):
    """The panel's views must find what the audit stored, whichever case the
    pool address is given in."""
    _add_scenario(chain)
    await _run_audit(pg_storage)

    app = create_app(storage=pg_storage)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        report = (await client.get(f"/api/v1/pools/1/{POOL_CHECKSUMMED}/report")).json()
        wash = (await client.get(f"/api/v1/pools/1/{POOL}/trades?wash_only=true")).json()
        overview = (await client.get("/api/v1/stats/overview")).json()

    assert report["total_trades_analyzed"] == 9
    assert report["wash_trade_volume_usd"] == pytest.approx(5985.0)
    assert report["severity"] == "CRITICAL"
    assert wash["count"] == 6
    assert overview["wash_volume_usd"] == pytest.approx(5985.0)
    assert overview["top_wash_pools"][0]["pool_address"] == POOL
    assert len(overview["recent_audits"]) == 1


async def test_readiness_against_the_real_database_and_node(pg_storage, chain, monkeypatch):
    """Ready with PostgreSQL and the node over HTTP JSON-RPC; not ready when
    a chain's endpoint serves another network."""
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "1")
    app = create_app(storage=pg_storage)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        ready = await client.get("/health/ready")
    assert ready.status_code == 200
    assert {name: check["ok"] for name, check in ready.json()["checks"].items()} == {
        "database": True,
        "rpc_1": True,
    }
    assert "eth_chainId" in chain.rpc_methods

    # Base's setting points at the Ethereum node: a configuration mistake that
    # would silently audit the wrong chain.
    monkeypatch.setattr(settings, "BASE_RPC_URL", chain.rpc_url)
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "1,8453")
    app = create_app(storage=pg_storage)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        wrong = await client.get("/health/ready")
    assert wrong.status_code == 503
    assert wrong.json()["checks"]["rpc_8453"]["reason"] == "wrong_chain"
    assert wrong.json()["checks"]["rpc_1"]["ok"] is True


def _metric(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_audit_is_visible_in_prometheus_metrics(pg_storage, chain):
    """A real audit over HTTP JSON-RPC shows up at /metrics: the node calls it
    made, its duration, what the detectors fired on and what it labelled."""
    _add_scenario(chain)
    get_logs = {"chain_id": "1", "method": "eth_getLogs", "outcome": "ok"}
    before = {
        "get_logs": _metric("wash_rpc_requests_total", **get_logs),
        "audits": _metric("wash_audit_duration_seconds_count", outcome="completed"),
        "hits": _metric("wash_audit_detector_hits_total", detector="round_trip"),
        "flagged": _metric("wash_audit_flagged_trades_total", method="round_trip"),
    }

    await _run_audit(pg_storage)

    assert _metric("wash_rpc_requests_total", **get_logs) > before["get_logs"]
    assert _metric("wash_rpc_request_duration_seconds_count", chain_id="1", method="eth_getLogs")
    assert _metric("wash_audit_duration_seconds_count", outcome="completed") == before["audits"] + 1
    assert _metric("wash_audit_detector_hits_total", detector="round_trip") == before["hits"] + 6
    assert _metric("wash_audit_flagged_trades_total", method="round_trip") == before["flagged"] + 6
    assert _metric("wash_rpc_circuit_state", breaker="rpc_1") == 0

    app = create_app(storage=pg_storage)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        body = (await client.get("/metrics")).text
    assert 'wash_rpc_requests_total{chain_id="1",method="eth_getLogs",outcome="ok"}' in body
    # The fake node's URL is never exported.
    assert chain.rpc_url not in body


async def _until(predicate, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition never became true"
        await asyncio.sleep(0.05)


async def test_watched_pool_alerts_a_signed_webhook_once(pg_storage, chain, monkeypatch):
    """MONITOR_POOLS end to end: new swaps on the node become one signed
    webhook call; after a restart, the same trades are not announced again."""
    monkeypatch.setattr(settings, "NATIVE_PRICE_SOURCE", "none")
    monkeypatch.setattr(settings, "MONITOR_POLL_INTERVAL_SECONDS", 1.0)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    received: list[tuple[dict, bytes]] = []

    async def hook(request: web.Request) -> web.Response:
        received.append((dict(request.headers), await request.read()))
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_post("/hook", hook)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    host, port = runner.addresses[0][:2]
    sink = WebhookSink(f"http://{host}:{port}/hook", secret="s3cret")
    chain.head = 200

    async def watch():
        dispatcher = AlertDispatcher([sink])
        watcher = PoolWatcher(pg_storage, [(1, POOL)], dispatcher)
        await dispatcher.start()
        await watcher.start()
        await _until(lambda: (1, POOL) in watcher._monitors)
        monitor = watcher._monitors[(1, POOL)]
        await _until(lambda: monitor.last_block >= chain.head)
        return dispatcher, watcher, monitor

    try:
        dispatcher, watcher, monitor = await watch()
        now = int(time.time()) - 60
        for i, amounts in enumerate([SELL_USDC, SELL_BACK] * 3):
            chain.add_v2_swap(201 + i, now + i, POOL, ROUTER, A, amounts, A)
        await _until(lambda: len(received) == 1)
        await watcher.stop()
        await dispatcher.stop()

        headers, body = received[0]
        alert = json.loads(body)
        assert alert["chain_id"] == 1 and alert["pool_address"] == POOL
        assert alert["trade_count"] == 6
        assert alert["methods"] == {"round_trip": 6}
        assert alert["wash_volume_usd"] == pytest.approx(5985.0)
        signed = headers["X-Wash-Timestamp"].encode() + b"." + body
        digest = hmac.new(b"s3cret", signed, hashlib.sha256).hexdigest()
        assert headers["X-Wash-Signature"] == f"sha256={digest}"

        # Restart. A new swap makes the monitor re-run detection over the
        # window, which flags the six trades again; they are already labelled,
        # so nothing is sent.
        dispatcher, watcher, monitor = await watch()
        chain.add_v2_swap(207, now + 7, POOL, ROUTER, D, SELL_USDC, D)
        await _until(lambda: monitor.last_block >= 207)
        await dispatcher.drain()
        await watcher.stop()
        await dispatcher.stop()
        assert len(received) == 1
    finally:
        await runner.cleanup()


async def test_realtime_monitor_ingests_new_swaps_and_alerts(pg_storage, chain, monkeypatch):
    """Regression: the monitor passed a lowercase pool address to eth_getLogs,
    which web3 rejects, so it never ingested a single swap."""
    # Stablecoin legs need no price lookup; keeps this test off the wall clock.
    monkeypatch.setattr(settings, "NATIVE_PRICE_SOURCE", "none")
    chain.head = 200
    monitor = RealtimeMonitor(pg_storage, 1, POOL_CHECKSUMMED, poll_interval=1, window_minutes=60)
    await monitor.initialize()
    assert monitor.last_block == 200

    now = int(time.time()) - 60
    for i, amounts in enumerate([SELL_USDC, SELL_BACK] * 3):
        chain.add_v2_swap(201 + i, now + i, POOL, ROUTER, A, amounts, A)
    alerts, stats = await monitor.poll_once()

    assert len(alerts) == 6
    assert {a["detection_method"] for a in alerts} == {"round_trip"}
    assert alerts[0]["volume_usd"] == pytest.approx(1000.0)
    assert stats["round_trip"] == 6
    assert monitor.last_block == 206

    again, _ = await monitor.poll_once()  # no new blocks
    assert again == []


async def test_the_json_export_lists_each_flagged_trade_with_its_evidence(
    pg_storage, chain, tmp_path
):
    import json

    _add_scenario(chain)
    runner = AuditRunner()
    runner.storage = pg_storage
    params = AuditParameters(
        chain_id=1, pool_address=POOL_CHECKSUMMED, start_block=100, end_block=130, use_ml=False
    )
    path = tmp_path / "audit.json"
    await runner.run_audit(params, export_format="json", export_path=str(path))

    exported = json.loads(path.read_text())
    flagged = exported["flagged_trades"]
    assert len(flagged) == exported["wash_trades_detected"] == 6
    assert [t["block_number"] for t in flagged] == sorted(t["block_number"] for t in flagged)
    assert all(t["wash_evidence"]["decided_by"] == "round_trip" for t in flagged)


POOL_2 = "0x" + "5" * 40  # a second USDC/WETH pool


async def test_round_trips_split_over_two_pools_are_found_only_when_audited_together(
    pg_storage, chain
):
    """A buys in one pool and sells back in the other, three times, switching
    pools each time: each pool alone holds at most one of A's round trips,
    below the two the detector requires; audited together, all three
    (roadmap Phase 6, cross-pool correlation)."""
    chain.set_pool_tokens(POOL_2, USDC, WETH)
    for n, block in enumerate((101, 110, 120)):
        first, second = (POOL, POOL_2) if n % 2 == 0 else (POOL_2, POOL)
        chain.add_v2_swap(block, T0 + 12 * (block - 101), first, ROUTER, A, SELL_USDC, A)
        chain.add_v2_swap(block + 2, T0 + 12 * (block - 99), second, ROUTER, A, SELL_BACK, A)

    runner = AuditRunner()
    runner.storage = pg_storage
    alone = await runner.run_audit(
        AuditParameters(
            chain_id=1, pool_address=POOL_CHECKSUMMED, start_block=100, end_block=130, use_ml=False
        )
    )
    assert alone["wash_trades_detected"] == 0

    together = await runner.run_audit(
        AuditParameters(
            chain_id=1,
            pool_address=POOL_CHECKSUMMED,
            start_block=100,
            end_block=130,
            use_ml=False,
            related_pools=[POOL_2],
        )
    )
    assert together["wash_trades_detected"] == 6
    assert together["related_pools"] == [POOL_2]
    few = {"trades": 3, "round_share": None, "benford_mad": None}
    few["benford_conformity"] = "insufficient data"
    assert together["pools"] == {
        POOL: {"trades": 3, "wash_trades_detected": 3, "trade_size_forensics": few},
        POOL_2: {"trades": 3, "wash_trades_detected": 3, "trade_size_forensics": few},
    }
    trades = await _stored_trades(pg_storage)
    assert all(t.is_wash_trade and t.detection_method == "round_trip" for t in trades)
    legs = trades[0].wash_evidence["detectors"][0]["trades"]
    assert {leg["tx"] for leg in legs} <= {t.transaction_hash for t in trades}
    assert len({t.pool_address for t in trades}) == 2
