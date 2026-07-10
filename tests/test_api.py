"""
Tests for the FastAPI service: auth, endpoints, websocket streaming, hardening.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.auth import generate_api_key, hash_api_key, verify_api_key
from api.server import _is_loopback, create_app
from config.settings import settings
from core.realtime_monitor import MonitorEvent
from models.schemas import SwapTrade

POOL = "0x" + "b" * 40


def _trade(trade_id: int, is_wash: bool = False) -> SwapTrade:
    return SwapTrade(
        id=trade_id,
        chain_id=1,
        dex_name="uniswap_v2",
        pool_address=POOL,
        token_in="0x" + "1" * 40,
        token_out="0x" + "2" * 40,
        amount_in=100.0,
        amount_out=200.0,
        sender="0x" + "a" * 40,
        recipient="0x" + "c" * 40,
        transaction_hash="0x" + "f" * 64,
        block_number=100,
        block_timestamp=datetime(2024, 1, 1, 12, 0, 0),
        log_index=trade_id,
        volume_usd=1000.0,
        is_wash_trade=is_wash,
        wash_trade_score=0.9 if is_wash else 0.0,
        detection_method="self_trading" if is_wash else None,
    )


@pytest.fixture
def storage():
    mock = AsyncMock()
    mock.health_check.return_value = True
    mock.get_pool_trades.return_value = []
    return mock


@pytest.fixture
def client(storage):
    app = create_app(storage=storage)
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------- auth unit


def test_generate_and_verify_api_key(monkeypatch):
    key, key_hash = generate_api_key()
    assert hash_api_key(key) == key_hash
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)
    assert verify_api_key(key) is True
    assert verify_api_key("wrong-key") is False
    assert verify_api_key(None) is False
    assert verify_api_key("") is False


def test_is_loopback():
    assert _is_loopback("127.0.0.1") is True
    assert _is_loopback("localhost") is True
    assert _is_loopback("::1") is True
    assert _is_loopback("0.0.0.0") is False  # noqa: S104
    assert _is_loopback("192.168.1.10") is False
    assert _is_loopback("not-an-ip") is False


# ------------------------------------------------------------- HTTP routes


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": True}


def test_security_headers_present(client):
    response = client.get("/health")
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert "default-src 'none'" in response.headers["Content-Security-Policy"]


def test_trades_endpoint(client, storage):
    storage.get_pool_trades.return_value = [_trade(1, is_wash=True), _trade(2)]
    response = client.get(f"/api/v1/pools/1/{POOL}/trades")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["trades"][0]["id"] == 1


def test_trades_endpoint_wash_only(client, storage):
    storage.get_pool_trades.return_value = [_trade(1, is_wash=True), _trade(2)]
    response = client.get(f"/api/v1/pools/1/{POOL}/trades?wash_only=true")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["trades"][0]["is_wash_trade"] is True


def test_trades_endpoint_rejects_bad_address(client):
    response = client.get("/api/v1/pools/1/not-an-address/trades")
    assert response.status_code == 422


def test_report_endpoint(client, storage):
    storage.get_pool_trades.return_value = [_trade(1, is_wash=True), _trade(2)]
    response = client.get(f"/api/v1/pools/1/{POOL}/report")
    assert response.status_code == 200
    body = response.json()
    assert body["total_trades_analyzed"] == 2
    assert body["wash_trades_count"] == 1
    assert body["wash_trade_volume_usd"] == 1000.0
    assert body["severity"] == "CRITICAL"  # 50% of volume is wash
    assert body["wash_volume_by_method"] == {"self_trading": 1000.0}


def _wait_for_task(client, task_id, attempts=100):
    """Poll a background audit task until it leaves the running state.

    The task runs on the app's event loop (portal thread), so sleeping in
    the test thread lets it progress.
    """
    import time

    for _ in range(attempts):
        body = client.get(f"/api/v1/audits/{task_id}").json()
        if body["status"] != "running":
            return body
        time.sleep(0.02)
    raise AssertionError("audit task never finished")


def test_audit_task_lifecycle(client):
    fake_result = {"trades_processed": 5, "wash_trades_detected": 1}
    with patch("scripts.run_audit.AuditRunner.run_audit", AsyncMock(return_value=fake_result)):
        response = client.post(
            "/api/v1/audits",
            json={"chain_id": 1, "pool_address": POOL},
        )
        assert response.status_code == 202
        task_id = response.json()["task_id"]

        body = _wait_for_task(client, task_id)
        assert body["status"] == "completed"
        assert body["result"] == fake_result
        assert "task" not in body  # internal handle must not leak


def test_audit_task_failure_reported(client):
    with patch(
        "scripts.run_audit.AuditRunner.run_audit",
        AsyncMock(side_effect=RuntimeError("rpc exploded")),
    ):
        response = client.post("/api/v1/audits", json={"chain_id": 1, "pool_address": POOL})
        task_id = response.json()["task_id"]
        body = _wait_for_task(client, task_id)
        assert body["status"] == "failed"
        assert "rpc exploded" in body["error"]


def test_audit_status_unknown_task(client):
    assert client.get("/api/v1/audits/deadbeef").status_code == 404


# ------------------------------------------------------------ auth on HTTP


def test_auth_blocks_without_key(storage, monkeypatch):
    key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)

    app = create_app(storage=storage)
    with TestClient(app) as client:
        assert client.get(f"/api/v1/pools/1/{POOL}/trades").status_code == 401
        assert (
            client.get(f"/api/v1/pools/1/{POOL}/trades", headers={"X-API-Key": "wrong"}).status_code
            == 401
        )
        assert (
            client.get(f"/api/v1/pools/1/{POOL}/trades", headers={"X-API-Key": key}).status_code
            == 200
        )
        # health stays open for liveness probes
        assert client.get("/health").status_code == 200


# ------------------------------------------------------------- rate limit


def test_rate_limit(storage, monkeypatch):
    monkeypatch.setattr(settings, "API_RATE_LIMIT_PER_MINUTE", 3)
    app = create_app(storage=storage)
    with TestClient(app) as client:
        for _ in range(3):
            assert client.get("/health").status_code == 200
        response = client.get("/health")
        assert response.status_code == 429
        assert response.headers["Retry-After"] == "60"


# -------------------------------------------------------------- websocket


def test_websocket_streams_monitor_events(client):
    async def fake_stream(_self):
        yield MonitorEvent(type="status", payload={"state": "monitoring"})
        yield MonitorEvent(type="alert", payload={"id": 7, "detection_method": "self_trading"})
        yield MonitorEvent(type="status", payload={"state": "stopped"})

    with (
        patch("core.realtime_monitor.RealtimeMonitor.stream", fake_stream),
        client.websocket_connect(f"/api/v1/ws/monitor/1/{POOL}") as ws,
    ):
        first = ws.receive_json()
        assert first == {"type": "status", "data": {"state": "monitoring"}}
        second = ws.receive_json()
        assert second["type"] == "alert"
        assert second["data"]["id"] == 7


def test_websocket_rejects_bad_address(client):
    from starlette.websockets import WebSocketDisconnect

    with (
        pytest.raises(WebSocketDisconnect) as excinfo,
        client.websocket_connect("/api/v1/ws/monitor/1/nope"),
    ):
        pass
    assert excinfo.value.code == 4422


def test_websocket_auth_required(storage, monkeypatch):
    key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)

    from starlette.websockets import WebSocketDisconnect

    async def fake_stream(_self):
        yield MonitorEvent(type="status", payload={"state": "monitoring"})

    app = create_app(storage=storage)
    with TestClient(app) as client:
        with (
            pytest.raises(WebSocketDisconnect) as excinfo,
            client.websocket_connect(f"/api/v1/ws/monitor/1/{POOL}"),
        ):
            pass
        assert excinfo.value.code == 4401

        with (
            patch("core.realtime_monitor.RealtimeMonitor.stream", fake_stream),
            client.websocket_connect(
                f"/api/v1/ws/monitor/1/{POOL}", headers={"X-API-Key": key}
            ) as ws,
        ):
            assert ws.receive_json()["type"] == "status"


# ---------------------------------------------------------- public binding


def test_run_server_refuses_public_bind_without_auth(monkeypatch):
    from api.server import run_server

    monkeypatch.setattr(settings, "API_HOST", "0.0.0.0")  # noqa: S104
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", False)
    with pytest.raises(SystemExit):
        run_server()


def test_run_server_refuses_auth_without_keys(monkeypatch):
    from api.server import run_server

    monkeypatch.setattr(settings, "API_HOST", "0.0.0.0")  # noqa: S104
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", "")
    with pytest.raises(SystemExit):
        run_server()


# ------------------------------------------------------------ sessions


def test_session_token_roundtrip():
    from api.auth import create_session_token, verify_session_token

    token = create_session_token(ttl_minutes=5)
    assert verify_session_token(token) is True
    assert verify_session_token(None) is False
    assert verify_session_token("garbage") is False
    assert verify_session_token("123.deadbeef") is False  # bad signature
    # Tampered expiry keeps the old signature -> must fail.
    payload, _, signature = token.partition(".")
    assert verify_session_token(f"{int(payload) + 9999}.{signature}") is False
    # Expired token (ttl 0 -> already past).
    assert verify_session_token(create_session_token(ttl_minutes=0)) is False


def test_panel_login_logout_flow(storage, monkeypatch):
    key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)

    app = create_app(storage=storage)
    with TestClient(app) as client:
        # Unauthenticated probe fails; wrong key fails.
        assert client.get("/panel/session").status_code == 401
        assert client.post("/panel/login", json={"api_key": "wrong"}).status_code == 401

        # Login sets an HttpOnly session cookie the client then reuses.
        response = client.post("/panel/login", json={"api_key": key})
        assert response.status_code == 200
        cookie_header = response.headers["set-cookie"]
        assert "wash_session=" in cookie_header
        assert "HttpOnly" in cookie_header
        assert "samesite=strict" in cookie_header.lower()

        # Cookie now authenticates both panel probes and API routes.
        session_info = client.get("/panel/session")
        assert session_info.status_code == 200
        assert session_info.json() == {"authenticated": True, "auth_required": True}
        assert client.get(f"/api/v1/pools/1/{POOL}/trades").status_code == 200

        # Logout clears the cookie.
        client.post("/panel/logout")
        assert client.get("/panel/session").status_code == 401


def test_panel_local_mode_needs_no_key(client):
    # Auth disabled: probe succeeds and login works with an empty key.
    info = client.get("/panel/session")
    assert info.status_code == 200
    assert info.json() == {"authenticated": True, "auth_required": False}
    assert client.post("/panel/login", json={}).status_code == 200


def test_websocket_accepts_session_cookie(storage, monkeypatch):
    from api.auth import create_session_token

    key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)

    async def fake_stream(_self):
        yield MonitorEvent(type="status", payload={"state": "monitoring"})

    token = create_session_token(ttl_minutes=5)
    app = create_app(storage=storage)
    with (
        TestClient(app) as client,
        patch("core.realtime_monitor.RealtimeMonitor.stream", fake_stream),
        client.websocket_connect(
            f"/api/v1/ws/monitor/1/{POOL}",
            headers={"Cookie": f"wash_session={token}"},
        ) as ws,
    ):
        assert ws.receive_json()["type"] == "status"


def test_panel_index_and_csp(client):
    response = client.get("/panel")
    assert response.status_code == 200
    assert "Wash Trade Scanner" in response.text
    csp = response.headers["Content-Security-Policy"]
    assert "script-src 'self'" in csp
    assert "connect-src 'self' ws: wss:" in csp

    asset = client.get("/panel/static/panel.js")
    assert asset.status_code == 200


def test_stats_overview_endpoint(client, storage):
    storage.get_global_stats.return_value = {
        "total_trades": 10,
        "total_volume_usd": 1_000.0,
        "pools_tracked": 2,
        "chains_active": 1,
        "wash_trades": 3,
        "wash_volume_usd": 400.0,
        "by_method": {"self_trading": {"trades": 3, "volume_usd": 400.0}},
        "by_chain": {},
    }
    storage.get_top_wash_pools.return_value = [
        {
            "chain_id": 1,
            "pool_address": POOL,
            "trades": 10,
            "volume_usd": 1_000.0,
            "wash_trades": 3,
            "wash_volume_usd": 400.0,
        }
    ]
    fake_log = MagicMock()
    fake_log.id = 1
    fake_log.chain_id = 1
    fake_log.pool_address = POOL
    fake_log.trades_processed = 10
    fake_log.wash_trades_detected = 3
    fake_log.detection_duration_seconds = 1.5
    fake_log.created_at = datetime(2024, 1, 1, 12, 0, 0)
    fake_log.results_summary = {"severity": "HIGH"}
    storage.get_recent_audit_logs.return_value = [fake_log]

    response = client.get("/api/v1/stats/overview")
    assert response.status_code == 200
    body = response.json()
    assert body["total_trades"] == 10
    assert body["top_wash_pools"][0]["pool_address"] == POOL
    assert body["recent_audits"][0]["wash_trades_detected"] == 3
    assert body["recent_audits"][0]["created_at"] == "2024-01-01T12:00:00"
