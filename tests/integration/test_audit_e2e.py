"""
End-to-end: a full audit through every real component -- settings, chain
config, web3 over HTTP JSON-RPC, ABI log decoding, USD pricing, PostgreSQL,
the detector stack, reporting -- then the API and the real-time monitor on
the same data. Only the blockchain node and the price API are simulated.

Each defect fixed in Phase 1 breaks at least one of these tests:
ETH_RPC_URL ignored / all chains connected eagerly (no audit could start),
duplicate index in the schema (no entry point could start), USD values never
populated (all $0), pool address case mismatch (API views empty, real-time
monitor never ingested).

Scenario (Ethereum, a Uniswap-V2-style USDC/WETH pool, all within one hour):
  1. A -> A        self-trade                      -> self_trading
  2-5. B <-> C     two round trips, net position 0  -> position_neutral_scc
  6. D -> E, 7. F -> G   ordinary one-off swaps    -> clean
Every swap moves 1,000 USDC against 0.5 WETH, with ETH at $2,000, so every
leg is worth exactly $1,000.
"""

import time
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import select
from web3 import Web3

from api.server import create_app
from config.settings import settings
from core.realtime_monitor import RealtimeMonitor
from core.storage import Storage
from core.validators import AuditParameters
from models.schemas import SwapTrade
from scripts.run_audit import AuditRunner

pytestmark = pytest.mark.integration

USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # token0, 6 decimals
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # token1, 18 decimals
POOL = "0x9e8ce2cc2fd75fb0de6a1e1b6b1bf6c7f33a4d6e"
POOL_CHECKSUMMED = Web3.to_checksum_address(POOL)
A, B, C, D, E, F, G = ("0x" + ch * 40 for ch in "abcdef7")

USDC_1000 = 1_000 * 10**6
WETH_HALF = 5 * 10**17
SELL_USDC = (USDC_1000, 0, 0, WETH_HALF)  # amount0In, amount1In, amount0Out, amount1Out
SELL_WETH = (0, WETH_HALF, USDC_1000, 0)
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
    swaps = [
        (A, A, SELL_USDC),
        (B, C, SELL_USDC),
        (C, B, SELL_WETH),
        (B, C, SELL_USDC),
        (C, B, SELL_WETH),
        (D, E, SELL_USDC),
        (F, G, SELL_USDC),
    ]
    for i, (sender, to, amounts) in enumerate(swaps):
        chain.add_v2_swap(101 + i, T0 + 12 * i, POOL, sender, to, amounts)


async def _stored_trades(storage: Storage) -> list[SwapTrade]:
    async with await storage.get_session() as session:
        result = await session.execute(select(SwapTrade).order_by(SwapTrade.block_number))
        return list(result.scalars().all())


async def _run_audit(storage: Storage) -> dict:
    runner = AuditRunner()
    runner.storage = storage
    params = AuditParameters(
        chain_id=1, pool_address=POOL_CHECKSUMMED, start_block=100, end_block=110, use_ml=False
    )
    return await runner.run_audit(params)


async def test_full_audit_detects_prices_and_reports(pg_storage, chain):
    _add_scenario(chain)

    result = await _run_audit(pg_storage)

    # The configured endpoint was actually used, and logs were fetched from it.
    assert "eth_getLogs" in chain.rpc_methods
    assert result["trades_processed"] == 7
    assert result["wash_trades_detected"] == 5

    trades = await _stored_trades(pg_storage)
    assert len(trades) == 7
    for trade in trades:
        assert trade.pool_address == POOL  # stored lowercase
        assert trade.volume_usd == pytest.approx(1000.0)
        assert trade.amount_in_usd == pytest.approx(1000.0)  # USDC at $1 or 0.5 WETH at $2,000
        assert trade.amount_out_usd == pytest.approx(1000.0)

    methods = [(t.sender, t.recipient, t.detection_method) for t in trades]
    assert methods == [
        (A, A, "self_trading"),
        (B, C, "position_neutral_scc"),
        (C, B, "position_neutral_scc"),
        (B, C, "position_neutral_scc"),
        (C, B, "position_neutral_scc"),
        (D, E, None),
        (F, G, None),
    ]

    metrics = result["risk_metrics"]
    assert metrics["total_volume_usd"] == pytest.approx(7000.0)
    assert metrics["wash_trade_volume_usd"] == pytest.approx(5000.0)
    assert metrics["wash_trade_volume_ratio"] == pytest.approx(5 / 7)
    assert metrics["severity"] == "CRITICAL"
    assert metrics["wash_volume_by_method"] == pytest.approx(
        {"self_trading": 1000.0, "position_neutral_scc": 4000.0}
    )

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
    result = await _run_audit(pg_storage)

    assert result["trades_processed"] == 7
    assert result["wash_trades_detected"] == 5
    assert len(await _stored_trades(pg_storage)) == 7


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

    assert report["total_trades_analyzed"] == 7
    assert report["wash_trade_volume_usd"] == pytest.approx(5000.0)
    assert report["severity"] == "CRITICAL"
    assert wash["count"] == 5
    assert overview["wash_volume_usd"] == pytest.approx(5000.0)
    assert overview["top_wash_pools"][0]["pool_address"] == POOL
    assert len(overview["recent_audits"]) == 1


async def test_realtime_monitor_ingests_new_swaps_and_alerts(pg_storage, chain, monkeypatch):
    """Regression: the monitor passed a lowercase pool address to eth_getLogs,
    which web3 rejects, so it never ingested a single swap."""
    # Stablecoin legs need no price lookup; keeps this test off the wall clock.
    monkeypatch.setattr(settings, "NATIVE_PRICE_SOURCE", "none")
    chain.head = 200
    monitor = RealtimeMonitor(pg_storage, 1, POOL_CHECKSUMMED, poll_interval=1, window_minutes=60)
    await monitor.initialize()
    assert monitor.last_block == 200

    chain.add_v2_swap(201, int(time.time()) - 30, POOL, A, A, (500 * 10**6, 0, 0, WETH_HALF))
    alerts, stats = await monitor.poll_once()

    assert len(alerts) == 1
    assert alerts[0]["detection_method"] == "self_trading"
    assert alerts[0]["volume_usd"] == pytest.approx(500.0)
    assert stats["self_trading"] == 1
    assert monitor.last_block == 201

    again, _ = await monitor.poll_once()  # no new blocks
    assert again == []
