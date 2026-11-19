"""
Liveness and readiness (roadmap 4.2): /health/live checks nothing, and
/health/ready needs the database and each configured chain's RPC endpoint.
"""

import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from web3 import AsyncHTTPProvider

from api.auth import generate_api_key
from api.health import ReadinessChecker
from api.server import create_app
from config.settings import Settings, settings
from core.circuit_breaker import CircuitState
from core.ingestor import chain_breaker

RPC_URL = "https://eth.example.org/v3/secret-key-123"


def rpc_answer(chain_id_hex: str) -> dict:
    return {"jsonrpc": "2.0", "id": 0, "result": chain_id_hex}


@pytest.fixture
def node(monkeypatch):
    """The chain-1 node: an AsyncMock standing in for the HTTP transport."""
    monkeypatch.setattr(settings, "ETH_RPC_URL", RPC_URL)
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "1")
    monkeypatch.setattr(settings, "HEALTH_CHECK_TIMEOUT_SECONDS", 0.5)
    transport = AsyncMock(return_value=rpc_answer("0x1"))
    monkeypatch.setattr(AsyncHTTPProvider, "make_request", transport)
    return transport


@pytest.fixture
def storage():
    mock = AsyncMock()
    mock.health_check.return_value = True
    return mock


@pytest.fixture
def client(storage, request):
    request.getfixturevalue("node")  # every app in these tests talks to the mock node
    with TestClient(create_app(storage=storage)) as test_client:
        yield test_client


# ------------------------------------------------------------------ liveness


def test_liveness_checks_no_dependency(client, storage, node):
    storage.health_check.side_effect = RuntimeError("database gone")
    node.side_effect = ConnectionError("node gone")

    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive"}
    storage.health_check.assert_not_awaited()
    node.assert_not_awaited()


# ----------------------------------------------------------------- readiness


def test_ready_when_database_and_node_answer(client, node):
    response = client.get("/health/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["checks"]["database"]["ok"] is True
    assert body["checks"]["rpc_1"]["ok"] is True
    assert "reason" not in body["checks"]["rpc_1"]
    assert node.await_args.args[0] == "eth_chainId"


def test_not_ready_when_the_database_is_down(client, storage):
    storage.health_check.return_value = False

    response = client.get("/health/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["database"] == {
        "ok": False,
        "latency_ms": body["checks"]["database"]["latency_ms"],
        "reason": "unreachable",
    }
    assert body["checks"]["rpc_1"]["ok"] is True  # every check is reported


def test_a_hung_database_is_a_timeout_not_a_hung_probe(client, storage, monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_CHECK_TIMEOUT_SECONDS", 0.05)

    async def hang() -> bool:
        await asyncio.sleep(5)
        return True

    storage.health_check.side_effect = hang

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["database"]["reason"] == "timeout"


@pytest.mark.parametrize(
    ("answer", "reason"),
    [
        (rpc_answer("0x38"), "wrong_chain"),  # an endpoint serving BNB Chain
        (ConnectionError(f"cannot reach {RPC_URL}"), "unreachable"),
    ],
)
def test_not_ready_when_the_node_fails(client, node, answer, reason):
    if isinstance(answer, BaseException):
        node.side_effect = answer
    else:
        node.return_value = answer

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["rpc_1"]["reason"] == reason
    # Unauthenticated callers never see the endpoint or its key.
    assert "secret-key" not in response.text and "example.org" not in response.text


def test_a_hung_node_is_a_timeout(client, node, monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_CHECK_TIMEOUT_SECONDS", 0.05)

    async def hang(*_args):
        await asyncio.sleep(5)

    node.side_effect = hang

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["rpc_1"]["reason"] == "timeout"


def test_an_unconfigured_chain_is_not_ready_and_is_not_called(client, node, monkeypatch):
    monkeypatch.setattr(settings, "ETH_RPC_URL", "")  # falls back to a YOUR_KEY placeholder

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["rpc_1"]["reason"] == "not_configured"
    node.assert_not_awaited()


def test_an_open_circuit_is_reported_without_calling_the_node(client, node):
    breaker = chain_breaker(1)
    breaker.state = CircuitState.OPEN
    breaker.last_failure_time = 10**12  # far in the future: stays open

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["rpc_1"]["reason"] == "circuit_open"
    node.assert_not_awaited()


def test_node_answers_are_cached_between_probes(client, node, monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_RPC_CACHE_SECONDS", 60.0)
    for _ in range(3):
        assert client.get("/health/ready").status_code == 200
    assert node.await_count == 1

    monkeypatch.setattr(settings, "HEALTH_RPC_CACHE_SECONDS", 0.0)
    client.get("/health/ready")
    client.get("/health/ready")
    assert node.await_count == 3


@pytest.mark.asyncio
async def test_concurrent_probes_share_one_node_request(node, monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_RPC_CACHE_SECONDS", 60.0)

    async def slow(*_args):
        await asyncio.sleep(0.05)
        return rpc_answer("0x1")

    node.side_effect = slow
    checker = ReadinessChecker(AsyncMock(return_value=True))

    results = await asyncio.gather(*(checker.check() for _ in range(5)))

    assert all(ready for ready, _ in results)
    assert node.await_count == 1


@pytest.mark.usefixtures("node")
def test_every_listed_chain_is_checked_and_none_when_empty(storage, monkeypatch):
    monkeypatch.setattr(settings, "BASE_RPC_URL", "https://base.example.org/key")

    async def by_chain(self, _method, _params):
        return rpc_answer(hex(self.chain_id))

    monkeypatch.setattr(AsyncHTTPProvider, "make_request", by_chain)
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "1, 8453")
    with TestClient(create_app(storage=storage)) as client:
        checks = client.get("/health/ready").json()["checks"]
    assert set(checks) == {"database", "rpc_1", "rpc_8453"}
    assert all(check["ok"] for check in checks.values())

    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "")
    with TestClient(create_app(storage=storage)) as client:
        response = client.get("/health/ready")
    assert response.status_code == 200
    assert set(response.json()["checks"]) == {"database"}


def test_an_unknown_readiness_chain_fails_at_startup(storage, monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "1,999999")
    with pytest.raises(ValueError, match="999999"):
        create_app(storage=storage)


def test_chain_id_list_parsing(monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", " 1, 56,1 ")
    assert settings.health_rpc_chain_id_list == [1, 56]
    with pytest.raises(ValidationError):
        Settings(HEALTH_RPC_CHAIN_IDS="ethereum")


@pytest.mark.usefixtures("node")
def test_probes_need_no_api_key(storage, monkeypatch):
    _key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)
    with TestClient(create_app(storage=storage)) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 200
        assert client.get("/metrics").status_code == 401  # the rest stays closed


def test_the_image_healthcheck_probes_liveness():
    from pathlib import Path

    dockerfile = (Path(__file__).parent.parent / "Dockerfile").read_text()
    healthcheck = dockerfile[dockerfile.index("HEALTHCHECK") :].split("\n\n")[0]
    assert "/health/live" in healthcheck


def test_probes_are_logged_at_debug(client):
    import io
    import json
    import logging

    from core.log_config import configure_logging

    root = logging.getLogger()
    level = root.level
    buffer = io.StringIO()
    handler = configure_logging(level="DEBUG", fmt="json", stream=buffer)
    try:
        client.get("/health/live")
        client.get("/health/ready")
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
    access = [json.loads(line) for line in buffer.getvalue().splitlines()]
    access = [line for line in access if line["logger"] == "api.access"]
    assert [line["http_path"] for line in access] == ["/health/live", "/health/ready"]
    assert {line["level"] for line in access} == {"DEBUG"}
