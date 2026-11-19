"""
Rate limits and authentication lockouts (roadmap 4.4) on the in-memory
store, through the API. The same contract runs against real Redis in
tests/integration/test_redis_limits.py.
"""

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from starlette.websockets import WebSocketDisconnect

from api.auth import SESSION_COOKIE, generate_api_key
from api.limits import Limits, MemoryLimitStore, RedisLimitStore, lockout_seconds
from api.server import create_app
from config.settings import settings
from tests import limit_contract
from tests.limit_contract import lockout_settings

POOL = "0x" + "b" * 40
TRADES = f"/api/v1/pools/1/{POOL}/trades"


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


@limit_contract.contract_params()
async def test_memory_store_contract(check, monkeypatch):
    await limit_contract.run(check, MemoryLimitStore(), monkeypatch)


def test_lockout_doubles_from_the_threshold_and_is_capped(monkeypatch):
    lockout_settings(monkeypatch)
    assert [lockout_seconds(n) for n in range(1, 8)] == [0, 0, 1, 2, 4, 4, 4]
    assert lockout_seconds(10_000) == 4  # no overflow on huge counts


@pytest.fixture
def storage():
    mock = AsyncMock()
    mock.health_check.return_value = True
    mock.get_pool_trades.return_value = []
    return mock


@pytest.fixture
def auth(monkeypatch):
    """Auth on with one key; lock from the 3rd wrong key."""
    key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)
    lockout_settings(monkeypatch)
    return key


@pytest.fixture
def client(storage, request):
    request.getfixturevalue("auth")
    with TestClient(create_app(storage=storage)) as test_client:
        yield test_client


def wrong(client, path=TRADES):
    return client.get(path, headers={"X-API-Key": "not-the-key"})


def test_repeated_wrong_keys_lock_the_client_out(client, auth):
    failures = sample("wash_auth_failures_total")
    lockouts = sample("wash_auth_lockouts_total")

    assert [wrong(client).status_code for _ in range(3)] == [401, 401, 401]
    locked = client.get(TRADES, headers={"X-API-Key": auth})  # even the right key

    assert locked.status_code == 429
    assert locked.json()["detail"] == "Too many failed authentication attempts"
    assert locked.headers["Retry-After"] == "1"
    assert sample("wash_auth_failures_total") == failures + 3
    assert sample("wash_auth_lockouts_total") == lockouts + 1


def test_every_route_counts_toward_one_lockout(client):
    wrong(client, "/api/v1/stats/overview")
    wrong(client, "/metrics")
    client.post("/panel/login", json={"api_key": "guess"})
    assert wrong(client).status_code == 429


def test_no_key_or_an_expired_session_is_not_a_guess(client, auth):
    for _ in range(10):
        assert client.get("/panel/session").status_code == 401  # the logged-out panel's probe
    client.cookies.set(SESSION_COOKIE, "1.deadbeef")
    for _ in range(10):
        assert client.get(TRADES).status_code == 401
    client.cookies.clear()
    assert client.get(TRADES, headers={"X-API-Key": auth}).status_code == 200


def test_a_success_forgets_earlier_failures(client, auth):
    wrong(client)
    wrong(client)
    assert client.get(TRADES, headers={"X-API-Key": auth}).status_code == 200
    wrong(client)
    wrong(client)
    assert client.get(TRADES, headers={"X-API-Key": auth}).status_code == 200


def test_the_panel_login_is_throttled(client, auth):
    for _ in range(3):
        assert client.post("/panel/login", json={"api_key": "guess"}).status_code == 401
    response = client.post("/panel/login", json={"api_key": auth})
    assert response.status_code == 429
    assert SESSION_COOKIE not in response.cookies


def ws_close_code(client, headers=None) -> int:
    url = f"/api/v1/ws/monitor/1/{POOL}"
    with (
        pytest.raises(WebSocketDisconnect) as closed,
        client.websocket_connect(url, headers=headers or {}),
    ):
        pass
    return closed.value.code


def test_websocket_guesses_count_and_a_locked_client_is_refused(client):
    codes = [ws_close_code(client, {"X-API-Key": "guess"}) for _ in range(3)]
    assert codes == [4401, 4401, 4401]
    assert ws_close_code(client) == 4429  # locked out: refused before any check


def test_nothing_is_throttled_with_auth_off(storage, monkeypatch):
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", False)
    lockout_settings(monkeypatch)
    with TestClient(create_app(storage=storage)) as client:
        for _ in range(10):
            assert wrong(client).status_code == 200


def test_rate_limited_requests_are_counted(storage, monkeypatch):
    monkeypatch.setattr(settings, "API_RATE_LIMIT_PER_MINUTE", 2)
    limited = sample("wash_rate_limited_total")
    with TestClient(create_app(storage=storage)) as client:
        codes = [client.get("/health/live").status_code for _ in range(4)]
    assert codes == [200, 200, 429, 429]
    assert sample("wash_rate_limited_total") == limited + 2


# ------------------------------------------------------- an unreachable Redis


DEAD_REDIS = "redis://:hunter2@127.0.0.1:9/0"  # nothing listens on port 9


async def test_an_unreachable_store_fails_open_and_is_counted():
    limits = Limits(RedisLimitStore(DEAD_REDIS, timeout=0.2))
    errors = sample("wash_limit_store_errors_total", operation="hit")
    try:
        assert await limits.rate_limit("10.0.0.9") == 0  # let through
        state = await limits.auth_state("10.0.0.9")
        assert (state.locked_for, state.failures) == (0, 0)
        await limits.auth_failed("10.0.0.9")  # does not raise
        assert await limits.ping() is False
    finally:
        await limits.close()
    assert sample("wash_limit_store_errors_total", operation="hit") == errors + 1


def test_the_api_keeps_serving_without_redis_and_readiness_says_why(storage, monkeypatch):
    monkeypatch.setattr(settings, "RATE_LIMIT_REDIS_URL", DEAD_REDIS)
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "")
    app = create_app(storage=storage)
    assert app.state.limits.shared
    with TestClient(app) as client:
        assert client.get("/health/live").status_code == 200  # served, unlimited
        ready = client.get("/health/ready")
    assert ready.status_code == 503
    assert ready.json()["checks"]["rate_limit_store"]["reason"] == "unreachable"
    assert "hunter2" not in ready.text


def test_without_redis_readiness_has_no_store_check(storage, monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "")
    with TestClient(create_app(storage=storage)) as client:
        assert set(client.get("/health/ready").json()["checks"]) == {"database"}


def test_the_redis_password_is_redacted(monkeypatch):
    from core.redaction import redact

    monkeypatch.setattr(settings, "RATE_LIMIT_REDIS_URL", "redis://:hunter2@cache.internal:6379")
    assert redact("Error connecting to redis://:hunter2@cache.internal:6379") == (
        "Error connecting to redis://cache.internal:6379/<redacted>"
    )


def test_a_non_redis_url_is_rejected():
    from pydantic import ValidationError

    from config.settings import Settings

    with pytest.raises(ValidationError):
        Settings(RATE_LIMIT_REDIS_URL="http://cache.internal:6379")


def test_a_shared_session_secret_makes_sessions_valid_on_every_instance(monkeypatch):
    from api import auth

    token = auth.create_session_token()
    monkeypatch.setattr(auth, "_process_secret", b"another-process" * 3)
    assert not auth.verify_session_token(token)  # per-process: another instance refuses it

    monkeypatch.setattr(settings, "PANEL_SESSION_SECRET", "s" * 64)
    shared = auth.create_session_token()
    monkeypatch.setattr(auth, "_process_secret", b"a-third-process" * 3)
    assert auth.verify_session_token(shared)  # signed with the shared secret


def test_a_short_session_secret_is_rejected():
    from pydantic import ValidationError

    from config.settings import Settings

    with pytest.raises(ValidationError):
        Settings(PANEL_SESSION_SECRET="too-short")
