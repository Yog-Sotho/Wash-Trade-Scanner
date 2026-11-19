"""
The Redis limit store against a real Redis (REDIS_URL, default database 15
on localhost): the same contract as the in-memory store, and limits shared
by two API instances.
"""

import os
from unittest.mock import AsyncMock

import httpx
import pytest
import redis.asyncio as aioredis

from api.auth import generate_api_key
from api.limits import RedisLimitStore
from api.server import create_app
from config.settings import settings
from tests import limit_contract
from tests.limit_contract import lockout_settings

pytestmark = pytest.mark.integration

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
POOL = "0x" + "b" * 40


@pytest.fixture
async def redis_url():
    client = aioredis.from_url(REDIS_URL)
    await client.flushdb()  # a database of its own: unreachable Redis is an error
    yield REDIS_URL
    await client.flushdb()
    await client.aclose()


@limit_contract.contract_params()
async def test_redis_store_contract(check, redis_url, monkeypatch):
    store = RedisLimitStore(redis_url)
    try:
        await limit_contract.run(check, store, monkeypatch)
    finally:
        await store.close()


def storage():
    mock = AsyncMock()
    mock.health_check.return_value = True
    mock.get_pool_trades.return_value = []
    return mock


async def test_two_instances_share_lockouts_and_rate_limits(redis_url, monkeypatch):
    key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)
    monkeypatch.setattr(settings, "RATE_LIMIT_REDIS_URL", redis_url)
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "")
    lockout_settings(monkeypatch)
    first, second = create_app(storage=storage()), create_app(storage=storage())
    trades = f"/api/v1/pools/1/{POOL}/trades"

    async with (
        httpx.AsyncClient(transport=httpx.ASGITransport(app=first), base_url="http://a") as a,
        httpx.AsyncClient(transport=httpx.ASGITransport(app=second), base_url="http://b") as b,
    ):
        # Guesses spread over both instances add up to one lockout.
        await a.get(trades, headers={"X-API-Key": "guess"})
        await b.get(trades, headers={"X-API-Key": "guess"})
        await a.get(trades, headers={"X-API-Key": "guess"})
        locked = await b.get(trades, headers={"X-API-Key": key})
        assert locked.status_code == 429
        assert locked.json()["detail"] == "Too many failed authentication attempts"

        ready = await a.get("/health/ready")
        assert ready.status_code == 200
        assert ready.json()["checks"]["rate_limit_store"]["ok"] is True

    # The rate limit is one budget across instances too.
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", False)
    monkeypatch.setattr(settings, "API_RATE_LIMIT_PER_MINUTE", 3)
    await aioredis.from_url(redis_url).flushdb()
    first, second = create_app(storage=storage()), create_app(storage=storage())
    async with (
        httpx.AsyncClient(transport=httpx.ASGITransport(app=first), base_url="http://a") as a,
        httpx.AsyncClient(transport=httpx.ASGITransport(app=second), base_url="http://b") as b,
    ):
        codes = [
            (await a.get("/health/live")).status_code,
            (await b.get("/health/live")).status_code,
            (await a.get("/health/live")).status_code,
            (await b.get("/health/live")).status_code,
        ]
    assert codes == [200, 200, 200, 429]
    await first.state.limits.close()
    await second.state.limits.close()
