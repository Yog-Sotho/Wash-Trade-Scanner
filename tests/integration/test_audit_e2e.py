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

import time
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import select
from web3 import Web3

from api.server import create_app
from config.settings import settings
from core.calibration import wash_probability
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


async def test_full_audit_detects_prices_and_reports(pg_storage, chain):
    _add_scenario(chain)

    result = await _run_audit(pg_storage)

    # The configured endpoint was actually used, and logs were fetched from it.
    assert "eth_getLogs" in chain.rpc_methods
    assert result["trades_processed"] == 9
    assert result["wash_trades_detected"] == 6

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
    result = await _run_audit(pg_storage)

    assert result["trades_processed"] == 9
    assert result["wash_trades_detected"] == 6
    assert len(await _stored_trades(pg_storage)) == 9


async def test_reaudit_clears_labels_the_current_detectors_do_not_give(pg_storage, chain):
    """Regression: re-auditing only ever added labels, so a trade flagged by an
    earlier run (or an earlier detector) stayed flagged forever."""
    _add_scenario(chain)
    await _run_audit(pg_storage)
    trades = await _stored_trades(pg_storage)
    bot_swap = next(t for t in trades if t.tx_from == E)
    await pg_storage.update_trade_labels([bot_swap.id], True, 1.0, "self_trading")

    result = await _run_audit(pg_storage)

    assert result["wash_trades_detected"] == 6
    relabelled = next(t for t in await _stored_trades(pg_storage) if t.tx_from == E)
    assert relabelled.is_wash_trade is False
    assert relabelled.detection_method is None


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
