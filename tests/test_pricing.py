"""
Tests for USD pricing: the reference-asset registry, swap valuation rules,
and the CoinGecko historical price client (exercised over real HTTP against
a local fake server).
"""

import os
import re
import time
from datetime import UTC, datetime

import pytest
from aiohttp import web

from config.chains import CHAINS
from config.tokens import REFERENCE_ASSETS, AssetKind, get_reference_asset
from core.pricing import CoinGeckoPriceSource, PriceOracle

USDC_ETH = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # 6 decimals
USDT_ETH = "0xdac17f958d2ee523a2206206994597c13d831ec7"  # 6 decimals
WETH_ETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # 18 decimals
USDT_BSC = "0x55d398326f99059ff775485246999027b3197955"  # Binance-Peg: 18 decimals
UNKNOWN = "0x" + "1" * 40

T0 = datetime(2026, 1, 15, 12, 0, 0)  # naive UTC, as stored by the ingestor
T0_EPOCH = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC).timestamp()


def _trade(token_in, amount_in, token_out, amount_out, ts=T0):
    return {
        "token_in": token_in,
        "amount_in": float(amount_in),
        "token_out": token_out,
        "amount_out": float(amount_out),
        "block_timestamp": ts,
    }


class FakeMarketSource:
    def __init__(self, prices=None, exc=None):
        self.prices = prices or {}
        self.exc = exc
        self.calls = []

    async def get_usd_prices(self, coin_id, timestamps):
        timestamps = sorted(timestamps)
        self.calls.append((coin_id, timestamps))
        if self.exc:
            raise self.exc
        return {t: self.prices.get(coin_id) for t in timestamps}


# ------------------------------------------------------------------ registry


def test_registry_addresses_are_normalized():
    for chain_id, assets in REFERENCE_ASSETS.items():
        for address in assets:
            assert re.fullmatch(r"0x[0-9a-f]{40}", address), (chain_id, address)


def test_registry_entries_are_well_formed():
    for assets in REFERENCE_ASSETS.values():
        for asset in assets.values():
            assert asset.decimals in (6, 18)
            if asset.kind is AssetKind.MARKET:
                assert asset.coingecko_id
            else:
                assert asset.coingecko_id is None


def test_registry_chains_exist():
    assert set(REFERENCE_ASSETS) <= {c["chain_id"] for c in CHAINS}


def test_bsc_pegged_stablecoins_use_18_decimals():
    """BNB Smart Chain's Binance-Peg USDT/USDC are 18 decimals, unlike the
    6 used everywhere else - getting this wrong misprices by 10^12."""
    assert get_reference_asset(56, USDT_BSC).decimals == 18
    assert get_reference_asset(56, "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d").decimals == 18


def test_reference_lookup_is_case_insensitive_and_chain_scoped():
    assert get_reference_asset(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48") is not None
    assert get_reference_asset(56, USDC_ETH) is None  # same address, wrong chain
    assert get_reference_asset(999_999, USDC_ETH) is None


# ------------------------------------------------------------------ valuation


@pytest.mark.asyncio
async def test_stablecoin_in_market_out_prices_both_legs_and_uses_stable_for_volume():
    oracle = PriceOracle(FakeMarketSource({"ethereum": 1990.0}))
    trade = _trade(USDC_ETH, 2_000 * 10**6, WETH_ETH, 1 * 10**18)

    stats = await oracle.price_trades(1, [trade])

    assert trade["amount_in_usd"] == pytest.approx(2000.0)
    assert trade["amount_out_usd"] == pytest.approx(1990.0)
    assert trade["volume_usd"] == pytest.approx(2000.0)
    assert stats.priced == 1


@pytest.mark.asyncio
async def test_stablecoin_leg_preferred_over_market_leg_even_on_output_side():
    oracle = PriceOracle(FakeMarketSource({"ethereum": 1990.0}))
    trade = _trade(WETH_ETH, 1 * 10**18, USDC_ETH, 1_995 * 10**6)

    await oracle.price_trades(1, [trade])

    assert trade["amount_in_usd"] == pytest.approx(1990.0)
    assert trade["volume_usd"] == pytest.approx(1995.0)


@pytest.mark.asyncio
async def test_market_leg_values_swap_against_unknown_token():
    oracle = PriceOracle(FakeMarketSource({"ethereum": 1990.0}))
    trade = _trade(WETH_ETH, 5 * 10**17, UNKNOWN, 123)

    await oracle.price_trades(1, [trade])

    assert trade["amount_in_usd"] == pytest.approx(995.0)
    assert trade["amount_out_usd"] is None
    assert trade["volume_usd"] == pytest.approx(995.0)


@pytest.mark.asyncio
async def test_bsc_usdt_valued_with_18_decimals():
    oracle = PriceOracle(FakeMarketSource())
    trade = _trade(UNKNOWN, 1, USDT_BSC, 250 * 10**18)

    await oracle.price_trades(56, [trade])

    assert trade["amount_out_usd"] == pytest.approx(250.0)
    assert trade["volume_usd"] == pytest.approx(250.0)


@pytest.mark.asyncio
async def test_two_stablecoin_legs_tie_goes_to_input_leg():
    oracle = PriceOracle(FakeMarketSource())
    trade = _trade(USDC_ETH, 100 * 10**6, USDT_ETH, 99 * 10**6)

    await oracle.price_trades(1, [trade])

    assert trade["volume_usd"] == pytest.approx(100.0)
    assert trade["amount_out_usd"] == pytest.approx(99.0)


@pytest.mark.asyncio
async def test_swap_without_reference_asset_is_unpriced():
    oracle = PriceOracle(FakeMarketSource())
    trade = _trade(UNKNOWN, 10, "0x" + "2" * 40, 20)

    stats = await oracle.price_trades(1, [trade])

    assert trade["amount_in_usd"] is None
    assert trade["amount_out_usd"] is None
    assert trade["volume_usd"] is None
    assert stats.no_reference_asset == 1


@pytest.mark.asyncio
async def test_unavailable_market_price_leaves_leg_unpriced():
    oracle = PriceOracle(FakeMarketSource(prices={}))  # source knows no price
    trade = _trade(WETH_ETH, 10**18, UNKNOWN, 1)

    stats = await oracle.price_trades(1, [trade])

    assert trade["volume_usd"] is None
    assert stats.market_price_unavailable == 1


@pytest.mark.asyncio
async def test_market_source_failure_never_fails_pricing():
    oracle = PriceOracle(FakeMarketSource(exc=RuntimeError("boom")))
    weth_trade = _trade(WETH_ETH, 10**18, UNKNOWN, 1)
    usdc_trade = _trade(USDC_ETH, 5 * 10**6, UNKNOWN, 1)

    await oracle.price_trades(1, [weth_trade, usdc_trade])

    assert weth_trade["volume_usd"] is None
    assert usdc_trade["volume_usd"] == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_without_market_source_only_stablecoin_legs_are_priced():
    oracle = PriceOracle(market_source=None)
    trade = _trade(USDC_ETH, 2_000 * 10**6, WETH_ETH, 10**18)

    await oracle.price_trades(1, [trade])

    assert trade["amount_in_usd"] == pytest.approx(2000.0)
    assert trade["amount_out_usd"] is None
    assert trade["volume_usd"] == pytest.approx(2000.0)


@pytest.mark.asyncio
async def test_every_trade_gets_all_three_usd_keys():
    """save_trades_batch issues one multi-row INSERT, which needs uniform dicts."""
    oracle = PriceOracle(FakeMarketSource())
    trades = [_trade(USDC_ETH, 10**6, UNKNOWN, 1), _trade(UNKNOWN, 1, "0x" + "3" * 40, 1)]

    await oracle.price_trades(1, trades)

    for trade in trades:
        assert {"amount_in_usd", "amount_out_usd", "volume_usd"} <= trade.keys()


@pytest.mark.asyncio
async def test_unknown_chain_is_unpriced():
    oracle = PriceOracle(FakeMarketSource({"ethereum": 1990.0}))
    trade = _trade(USDC_ETH, 10**6, WETH_ETH, 10**18)

    await oracle.price_trades(999_999, [trade])

    assert trade["volume_usd"] is None


@pytest.mark.asyncio
async def test_naive_block_timestamps_are_interpreted_as_utc():
    """A naive datetime's .timestamp() uses the *local* zone; block times are UTC."""
    source = FakeMarketSource({"ethereum": 1.0})
    oracle = PriceOracle(source)
    original_tz = os.environ.get("TZ")
    os.environ["TZ"] = "America/New_York"
    time.tzset()
    try:
        await oracle.price_trades(1, [_trade(WETH_ETH, 1, UNKNOWN, 1)])
    finally:
        if original_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original_tz
        time.tzset()

    assert source.calls == [("ethereum", [T0_EPOCH])]


def test_from_settings_respects_native_price_source(monkeypatch):
    from config.settings import settings

    monkeypatch.setattr(settings, "NATIVE_PRICE_SOURCE", "none")
    assert PriceOracle.from_settings().market_source is None

    monkeypatch.setattr(settings, "NATIVE_PRICE_SOURCE", "coingecko")
    monkeypatch.setattr(settings, "PRICE_MAX_STALENESS_MINUTES", 30)
    source = PriceOracle.from_settings().market_source
    assert isinstance(source, CoinGeckoPriceSource)
    assert source.max_staleness == 30 * 60


# ------------------------------------------------------------ CoinGecko client


class FakeCoinGecko:
    """Serves /coins/{id}/market_chart/range from in-memory hourly points."""

    def __init__(self, points):
        self.points = points  # [(unix_seconds, price)]
        self.queued = []  # [(status, body, headers)] served before the default response
        self.requests = []
        self.base_url = ""

    async def handle(self, request):
        self.requests.append(
            (request.match_info["coin_id"], dict(request.query), dict(request.headers))
        )
        if self.queued:
            status, body, headers = self.queued.pop(0)
            if isinstance(body, str):
                return web.Response(text=body, status=status, headers=headers)
            return web.json_response(body, status=status, headers=headers)
        start, end = float(request.query["from"]), float(request.query["to"])
        prices = [[t * 1000, p] for t, p in self.points if start <= t <= end]
        return web.json_response({"prices": prices, "market_caps": [], "total_volumes": []})


@pytest.fixture
async def coingecko():
    hourly = [(T0_EPOCH + h * 3600, 2000.0 + h) for h in range(-3, 4)]
    server = FakeCoinGecko(hourly)
    app = web.Application()
    app.router.add_get("/api/v3/coins/{coin_id}/market_chart/range", server.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    server.base_url = f"http://{host}:{port}/api/v3"
    yield server
    await runner.cleanup()


def _source(server, api_key="", staleness=7200):
    return CoinGeckoPriceSource(server.base_url, api_key, 10.0, staleness)


@pytest.mark.asyncio
async def test_coingecko_returns_nearest_point_within_staleness(coingecko):
    prices = await _source(coingecko).get_usd_prices("ethereum", [T0_EPOCH + 600])
    assert prices == {T0_EPOCH + 600: 2000.0}  # nearest point is T0 itself


@pytest.mark.asyncio
async def test_coingecko_refuses_prices_beyond_staleness(coingecko):
    far = T0_EPOCH + 5 * 3600 + 1  # last point is T0+3h; 2h+1s away
    prices = await _source(coingecko).get_usd_prices("ethereum", [far])
    assert prices == {far: None}


@pytest.mark.asyncio
async def test_coingecko_sends_demo_key_and_expected_params(coingecko):
    await _source(coingecko, api_key="demo-key").get_usd_prices("ethereum", [T0_EPOCH])

    coin_id, params, headers = coingecko.requests[0]
    assert coin_id == "ethereum"
    assert params["vs_currency"] == "usd"
    assert int(params["from"]) < T0_EPOCH < int(params["to"])
    assert headers["x-cg-demo-api-key"] == "demo-key"


def test_coingecko_uses_pro_header_for_pro_host():
    source = CoinGeckoPriceSource("https://pro-api.coingecko.com/api/v3", "k", 10.0, 7200)
    assert source._key_header == "x-cg-pro-api-key"


@pytest.mark.asyncio
async def test_coingecko_caches_windows(coingecko):
    source = _source(coingecko)
    await source.get_usd_prices("ethereum", [T0_EPOCH])
    await source.get_usd_prices("ethereum", [T0_EPOCH + 3600])

    assert len(coingecko.requests) == 1


@pytest.mark.asyncio
async def test_coingecko_windows_stay_within_hourly_granularity_range(coingecko):
    await _source(coingecko).get_usd_prices("ethereum", [T0_EPOCH, T0_EPOCH - 200 * 86_400])

    assert len(coingecko.requests) == 2
    for _, params, _ in coingecko.requests:
        span = int(params["to"]) - int(params["from"])
        assert span <= 89 * 86_400  # CoinGecko degrades to daily points beyond 90 days


@pytest.mark.asyncio
async def test_coingecko_retries_after_throttling(coingecko):
    coingecko.queued.append((429, "Throttled", {"Retry-After": "0"}))

    prices = await _source(coingecko).get_usd_prices("ethereum", [T0_EPOCH])

    assert prices == {T0_EPOCH: 2000.0}
    assert len(coingecko.requests) == 2


@pytest.mark.asyncio
async def test_coingecko_gives_up_after_persistent_throttling(coingecko):
    coingecko.queued.extend([(429, "Throttled", {"Retry-After": "0"})] * 3)

    prices = await _source(coingecko).get_usd_prices("ethereum", [T0_EPOCH])

    assert prices == {T0_EPOCH: None}
    assert len(coingecko.requests) == 3


@pytest.mark.asyncio
async def test_coingecko_history_limit_error_leaves_prices_unpriced(coingecko):
    # What the Demo/public API answers for ranges older than 365 days.
    limit_error = {"error": {"status": {"error_code": 10012, "error_message": "exceeds range"}}}
    coingecko.queued.append((401, limit_error, {}))

    prices = await _source(coingecko).get_usd_prices("ethereum", [T0_EPOCH])

    assert prices == {T0_EPOCH: None}


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [[1, 2], {"prices": [[1, "not-a-number"]]}, {"prices": "x"}])
async def test_coingecko_rejects_malformed_payloads(coingecko, body):
    coingecko.queued.append((200, body, {}))

    prices = await _source(coingecko).get_usd_prices("ethereum", [T0_EPOCH])

    assert prices == {T0_EPOCH: None}


@pytest.mark.asyncio
async def test_coingecko_failed_window_is_retried_only_after_refresh_period(coingecko):
    source = _source(coingecko)
    now = [time.time()]
    source._clock = lambda: now[0]
    coingecko.queued.append((500, "upstream error", {}))

    assert await source.get_usd_prices("ethereum", [T0_EPOCH]) == {T0_EPOCH: None}
    assert await source.get_usd_prices("ethereum", [T0_EPOCH]) == {T0_EPOCH: None}
    assert len(coingecko.requests) == 1  # failure is briefly cached, not hammered

    now[0] += 601
    assert await source.get_usd_prices("ethereum", [T0_EPOCH]) == {T0_EPOCH: 2000.0}
    assert len(coingecko.requests) == 2
