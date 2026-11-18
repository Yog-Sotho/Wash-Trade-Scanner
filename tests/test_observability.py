"""
Observability (roadmap 4.1): structured logs, request ids, Prometheus
metrics and trace spans.
"""

import asyncio
import io
import json
import logging
import time
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from prometheus_client import REGISTRY, generate_latest
from web3 import AsyncHTTPProvider

from api.auth import generate_api_key
from api.server import create_app
from config.settings import settings
from core import rpc
from core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    reset_shared_breakers,
    shared_breaker,
)
from core.exceptions import CircuitBreakerOpenError
from core.log_config import configure_logging, request_id_var
from core.storage import Storage
from core.validators import AuditParameters
from models.schemas import SwapTrade
from scripts.run_audit import AuditRunner

POOL = "0x" + "b" * 40
TRADES_ROUTE = "/api/v1/pools/{chain_id}/{pool_address}/trades"


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture
def log_lines():
    """Route all logging, as JSON, into a buffer; returns a reader."""
    root = logging.getLogger()
    level = root.level
    buffer = io.StringIO()
    handler = configure_logging(level="DEBUG", fmt="json", stream=buffer)

    def read() -> list[dict]:
        return [json.loads(line) for line in buffer.getvalue().splitlines()]

    yield read
    root.removeHandler(handler)
    root.setLevel(level)


@pytest.fixture
def storage():
    mock = AsyncMock()
    mock.health_check.return_value = True
    mock.get_pool_trades.return_value = []
    return mock


@pytest.fixture
def client(storage):
    with TestClient(create_app(storage=storage)) as test_client:
        yield test_client


# ----------------------------------------------------------------- logging


def test_json_log_lines_carry_context_extras_and_tracebacks(log_lines):
    log = logging.getLogger("test.obs")
    token = request_id_var.set("req-42")
    try:
        log.info("swap %s", "seen", extra={"pool": POOL})
        try:
            raise ValueError("boom")
        except ValueError:
            log.exception("failed\nsecond line")
    finally:
        request_id_var.reset(token)
    log.warning("outside any request")

    info, error, outside = log_lines()
    assert info["message"] == "swap seen"
    assert info["level"] == "INFO"
    assert info["logger"] == "test.obs"
    assert info["request_id"] == "req-42"
    assert info["pool"] == POOL
    assert info["timestamp"].endswith("+00:00")
    # One record, one line: a newline in a message cannot forge a log line.
    assert error["message"] == "failed\nsecond line"
    assert "ValueError: boom" in error["exception"]
    assert "request_id" not in outside


def test_json_logs_drop_uvicorns_colour_copy_of_the_message(log_lines):
    logging.getLogger("uvicorn.error").info(
        "Started server process [%d]", 7, extra={"color_message": "\x1b[36m%d\x1b[0m"}
    )
    (line,) = log_lines()
    assert line["message"] == "Started server process [7]"
    assert "color_message" not in line


def test_json_logs_carry_the_active_trace_ids(log_lines):
    tracer = TracerProvider().get_tracer("test")
    with tracer.start_as_current_span("work") as span:
        logging.getLogger("test.obs").info("inside a span")
        context = span.get_span_context()
    (line,) = log_lines()
    assert line["trace_id"] == format(context.trace_id, "032x")
    assert line["span_id"] == format(context.span_id, "016x")


def test_text_format_and_reconfiguration_replace_the_handler():
    root = logging.getLogger()
    level = root.level
    first, second = io.StringIO(), io.StringIO()
    configure_logging(level="INFO", fmt="text", stream=first)
    handler = configure_logging(level="INFO", fmt="text", stream=second)
    try:
        token = request_id_var.set("abc")
        logging.getLogger("test.obs").info("hello")
        request_id_var.reset(token)
        assert first.getvalue() == ""  # the first handler is gone
        assert "INFO - [abc] hello" in second.getvalue()
        assert sum(getattr(h, "_wash_handler", False) for h in root.handlers) == 1
    finally:
        root.removeHandler(handler)
        root.setLevel(level)


def test_log_format_setting_selects_json(monkeypatch):
    monkeypatch.setattr(settings, "LOG_FORMAT", "json")
    root = logging.getLogger()
    level = root.level
    buffer = io.StringIO()
    handler = configure_logging(level="INFO", stream=buffer)
    try:
        logging.getLogger("test.obs").info("hi")
        assert json.loads(buffer.getvalue())["message"] == "hi"
    finally:
        root.removeHandler(handler)
        root.setLevel(level)


# -------------------------------------------------------------- request id


def test_every_response_gets_a_request_id(client):
    generated = client.get("/health").headers["X-Request-ID"]
    assert len(generated) == 32 and int(generated, 16) >= 0
    assert client.get("/health").headers["X-Request-ID"] != generated


def test_a_valid_caller_request_id_is_kept_and_an_unsafe_one_replaced(client):
    kept = client.get("/health", headers={"X-Request-ID": "lb-7f3a.9:x_y-z"})
    assert kept.headers["X-Request-ID"] == "lb-7f3a.9:x_y-z"
    for unsafe in ("has space", "<script>", "x" * 129):
        replaced = client.get("/health", headers={"X-Request-ID": unsafe})
        assert replaced.headers["X-Request-ID"] != unsafe
        assert len(replaced.headers["X-Request-ID"]) == 32


def test_rate_limited_responses_carry_a_request_id(storage, monkeypatch):
    monkeypatch.setattr(settings, "API_RATE_LIMIT_PER_MINUTE", 1)
    with TestClient(create_app(storage=storage)) as client:
        client.get("/health")
        limited = client.get("/health")
    assert limited.status_code == 429
    assert limited.headers["X-Request-ID"]


def test_access_log_is_structured_and_quiet_for_probes(client, log_lines):
    response = client.get(f"/api/v1/pools/1/{POOL}/trades", headers={"X-Request-ID": "trace-me"})
    client.get("/health")

    access = [line for line in log_lines() if line["logger"] == "api.access"]
    trades, health = access
    assert trades["level"] == "INFO"
    assert trades["request_id"] == "trace-me" == response.headers["X-Request-ID"]
    assert trades["http_method"] == "GET"
    assert trades["http_path"] == f"/api/v1/pools/1/{POOL}/trades"
    assert trades["http_route"] == TRADES_ROUTE
    assert trades["http_status"] == 200
    assert trades["duration_ms"] >= 0
    assert health["level"] == "DEBUG"


def test_an_audit_started_by_a_request_logs_with_its_request_id(client, log_lines):
    with patch(
        "scripts.run_audit.AuditRunner.run_audit",
        AsyncMock(side_effect=RuntimeError("rpc exploded")),
    ):
        response = client.post("/api/v1/audits", json={"chain_id": 1, "pool_address": POOL})
        task_id = response.json()["task_id"]
        for _ in range(200):
            if client.get(f"/api/v1/audits/{task_id}").json()["status"] != "running":
                break
            time.sleep(0.01)

    failure = next(line for line in log_lines() if line["message"].startswith("Audit task"))
    assert failure["request_id"] == response.headers["X-Request-ID"]
    assert "rpc exploded" in failure["exception"]


# ------------------------------------------------------------------ metrics


def test_metrics_endpoint_counts_requests_by_route_template(client):
    labels = {"method": "GET", "route": TRADES_ROUTE, "status": "200"}
    before = sample("wash_http_requests_total", **labels)
    unmatched = sample("wash_http_requests_total", method="GET", route="unmatched", status="404")

    client.get(f"/api/v1/pools/1/{POOL}/trades")
    client.get("/no/such/path")
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "wash_http_request_duration_seconds_bucket" in response.text
    assert "process_resident_memory_bytes" in response.text or "python_info" in response.text
    assert sample("wash_http_requests_total", **labels) == before + 1
    assert (
        sample("wash_http_requests_total", method="GET", route="unmatched", status="404")
        == unmatched + 1
    )
    # Raw paths (and the addresses in them) never become label values.
    assert POOL not in generate_latest().decode()


def test_metrics_require_the_api_key_when_auth_is_on(storage, monkeypatch):
    key, key_hash = generate_api_key()
    monkeypatch.setattr(settings, "API_AUTH_ENABLED", True)
    monkeypatch.setattr(settings, "API_KEY_HASHES", key_hash)
    with TestClient(create_app(storage=storage)) as client:
        assert client.get("/metrics").status_code == 401
        assert client.get("/metrics", headers={"X-API-Key": key}).status_code == 200


def test_run_server_keeps_our_logging_and_access_log(monkeypatch):
    from api.server import run_server

    monkeypatch.setattr(settings, "API_HOST", "127.0.0.1")
    with patch("uvicorn.run") as uvicorn_run:
        run_server()
    kwargs = uvicorn_run.call_args.kwargs
    assert kwargs["log_config"] is None
    assert kwargs["access_log"] is False


# ---------------------------------------------------------------------- RPC


@pytest.fixture
def spans(monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(rpc, "_tracer", provider.get_tracer("test"))
    return exporter


SECRET_URL = "https://eth.example.org/v3/secret-key-123"


@pytest.mark.parametrize(
    ("answer", "outcome"),
    [
        ({"jsonrpc": "2.0", "id": 1, "result": "0x10"}, "ok"),
        ({"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": "limit"}}, "rpc_error"),
        (ConnectionError(f"cannot reach {SECRET_URL}"), "transport_error"),
        (asyncio.CancelledError(), "cancelled"),
    ],
)
@pytest.mark.asyncio
async def test_every_rpc_request_is_counted_timed_and_traced(monkeypatch, spans, answer, outcome):
    if isinstance(answer, BaseException):
        parent = AsyncMock(side_effect=answer)
    else:
        parent = AsyncMock(return_value=answer)
    monkeypatch.setattr(AsyncHTTPProvider, "make_request", parent)
    provider = rpc.InstrumentedHTTPProvider(SECRET_URL, chain_id=8453)
    labels = {"chain_id": "8453", "method": "eth_blockNumber"}
    count = sample("wash_rpc_requests_total", **labels, outcome=outcome)
    timed = sample("wash_rpc_request_duration_seconds_count", **labels)

    if isinstance(answer, BaseException):
        with pytest.raises(type(answer)):
            await provider.make_request("eth_blockNumber", [])
    else:
        assert await provider.make_request("eth_blockNumber", []) == answer

    assert sample("wash_rpc_requests_total", **labels, outcome=outcome) == count + 1
    assert sample("wash_rpc_request_duration_seconds_count", **labels) == timed + 1
    (span,) = spans.get_finished_spans()
    assert span.name == "eth_blockNumber"
    assert span.attributes["rpc.system"] == "jsonrpc"
    assert span.attributes["wash.chain_id"] == 8453
    # The endpoint (and the key in its URL) is never recorded.
    assert "secret-key" not in repr(dict(span.attributes))
    assert span.events == ()  # no exception event: its message would hold the URL
    if outcome == "ok":
        assert span.status.status_code == StatusCode.UNSET
    elif outcome == "rpc_error":
        assert span.status.status_code == StatusCode.ERROR
        assert span.attributes["rpc.jsonrpc.error_code"] == -32005
    elif outcome == "transport_error":
        assert span.status.description == "ConnectionError"


# ---------------------------------------------------------- circuit breaker


@pytest.mark.asyncio
async def test_breaker_state_transitions_are_published():
    name = "rpc_test_obs"
    breaker = CircuitBreaker(
        name,
        CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.01, success_threshold=1),
    )
    opened = sample("wash_rpc_circuit_opened_total", breaker=name)
    rejected = sample("wash_rpc_circuit_rejected_total", breaker=name)
    assert sample("wash_rpc_circuit_state", breaker=name) == 0

    async def fail() -> None:
        raise ConnectionError

    for _ in range(2):
        with pytest.raises(ConnectionError):
            await breaker.call(fail)
    assert sample("wash_rpc_circuit_state", breaker=name) == 2
    assert sample("wash_rpc_circuit_opened_total", breaker=name) == opened + 1

    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(fail)
    assert sample("wash_rpc_circuit_rejected_total", breaker=name) == rejected + 1

    await asyncio.sleep(0.02)
    seen: list[float] = []

    async def probe() -> str:
        seen.append(sample("wash_rpc_circuit_state", breaker=name))
        return "ok"

    assert await breaker.call(probe) == "ok"
    assert seen == [1]  # half-open while the trial call runs
    assert sample("wash_rpc_circuit_state", breaker=name) == 0


def test_breakers_are_shared_per_name():
    first = shared_breaker("rpc_shared_test")
    assert shared_breaker("rpc_shared_test") is first
    assert shared_breaker("rpc_other_test") is not first
    reset_shared_breakers()
    assert shared_breaker("rpc_shared_test") is not first


def test_audits_and_monitors_on_one_chain_share_a_breaker():
    from config.chains import get_chain_config
    from core.ingestor import ChainIngestor

    storage = AsyncMock(spec=Storage)
    first = ChainIngestor(get_chain_config(1), storage)
    second = ChainIngestor(get_chain_config(1), storage)
    other_chain = ChainIngestor(get_chain_config(10), storage)
    assert first.circuit_breaker is second.circuit_breaker
    assert other_chain.circuit_breaker is not first.circuit_breaker


# ------------------------------------------------------------------- audits


def _audit_runner(trades):
    runner = AuditRunner()
    runner.storage = AsyncMock(spec=Storage)
    runner.storage.get_pool_trades.return_value = trades
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = AsyncMock()
    runner.storage.get_session.return_value = session_cm
    return runner


def _trade(trade_id: int, method: str | None) -> SwapTrade:
    return SwapTrade(
        id=trade_id,
        chain_id=1,
        pool_address=POOL,
        sender="0x" + "a" * 40,
        recipient="0x" + "c" * 40,
        volume_usd=100.0,
        is_wash_trade=method is not None,
        wash_trade_score=0.9 if method else 0.0,
        detection_method=method,
        block_timestamp=datetime(2024, 1, 1, 12, trade_id),
    )


@pytest.fixture
def audit_spans(monkeypatch):
    import scripts.run_audit

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(scripts.run_audit, "_tracer", provider.get_tracer("test"))
    return exporter


@pytest.mark.asyncio
async def test_a_completed_audit_records_duration_hits_and_verdicts(audit_spans):
    wash, clean = _trade(1, "round_trip"), _trade(2, None)
    completed = sample("wash_audit_duration_seconds_count", outcome="completed")
    hits = sample("wash_audit_detector_hits_total", detector="round_trip")
    anomaly_hits = sample("wash_audit_detector_hits_total", detector="volume_anomaly")
    flagged = sample("wash_audit_flagged_trades_total", method="round_trip")

    with (
        patch("scripts.run_audit.MultiChainIngestor") as ingestor,
        patch("scripts.run_audit.FeatureEngineer"),
        patch("scripts.run_audit.HeuristicDetector") as heuristics,
        patch("scripts.run_audit.MLDetector") as ml,
        patch("scripts.run_audit.EntityClusterer") as clusterer,
    ):
        ingestor.return_value.initialize = AsyncMock()
        ingestor.return_value.audit_pool = AsyncMock(return_value=2)
        clusterer.return_value.cluster_addresses = AsyncMock()
        heuristics.return_value.run_all_heuristics = AsyncMock(
            return_value=([wash], {"round_trip": 3, "volume_anomaly": 1})
        )
        ml.return_value.load_model.side_effect = FileNotFoundError
        runner = _audit_runner([clean, wash])
        await runner.run_audit(AuditParameters(chain_id=1, pool_address=POOL))

    assert sample("wash_audit_duration_seconds_count", outcome="completed") == completed + 1
    # Hits are what each detector fired on; verdicts are the final labels.
    assert sample("wash_audit_detector_hits_total", detector="round_trip") == hits + 3
    assert sample("wash_audit_detector_hits_total", detector="volume_anomaly") == anomaly_hits + 1
    assert sample("wash_audit_flagged_trades_total", method="round_trip") == flagged + 1
    assert sample("wash_audits_in_progress") == 0
    (span,) = audit_spans.get_finished_spans()
    assert span.name == "audit"
    assert span.attributes["wash.chain_id"] == 1
    assert span.attributes["wash.pool_address"] == POOL
    assert span.attributes["wash.trades_processed"] == 2
    assert span.attributes["wash.wash_trades_detected"] == 1


@pytest.mark.asyncio
async def test_a_failed_audit_is_timed_as_failed_and_counts_no_hits():
    failed = sample("wash_audit_duration_seconds_count", outcome="failed")
    completed = sample("wash_audit_duration_seconds_count", outcome="completed")
    before = generate_latest().decode()

    with (
        patch("scripts.run_audit.MultiChainIngestor") as ingestor,
        patch("scripts.run_audit.FeatureEngineer"),
        patch("scripts.run_audit.HeuristicDetector"),
        patch("scripts.run_audit.MLDetector"),
        patch("scripts.run_audit.EntityClusterer"),
    ):
        ingestor.return_value.initialize = AsyncMock(side_effect=ConnectionError("down"))
        runner = _audit_runner([])
        with pytest.raises(ConnectionError):
            await runner.run_audit(AuditParameters(chain_id=1, pool_address=POOL))

    assert sample("wash_audit_duration_seconds_count", outcome="failed") == failed + 1
    assert sample("wash_audit_duration_seconds_count", outcome="completed") == completed
    assert sample("wash_audits_in_progress") == 0
    hit_lines = [
        line
        for line in generate_latest().decode().splitlines()
        if line.startswith(("wash_audit_detector_hits_total", "wash_audit_flagged_trades_total"))
    ]
    assert all(line in before for line in hit_lines)


# ---------------------------------------------------------------- redaction

KEYED_URL = "http://127.0.0.1:18602/v3/SECRETKEY123"


def test_redact_url_keeps_only_scheme_and_host():
    from core.redaction import redact_url

    assert redact_url("https://u:pw@node.example:8545/v3/key?x=1") == (
        "https://node.example:8545/<redacted>"
    )


def test_only_configured_urls_with_a_path_or_query_are_redacted(monkeypatch):
    from core.redaction import redact

    monkeypatch.setattr(settings, "ETH_RPC_URL", "https://mainnet.example.io/v3/abc123/")
    monkeypatch.setattr(settings, "BASE_RPC_URL", "https://mainnet.base.org")
    text = (
        "url='https://mainnet.example.io/v3/abc123/' and https://mainnet.example.io/v3/abc123 "
        "and https://mainnet.base.org"
    )
    assert redact(text) == (
        "url='https://mainnet.example.io/<redacted>' and https://mainnet.example.io/<redacted> "
        "and https://mainnet.base.org"
    )


@pytest.mark.parametrize("fmt", ["json", "text"])
@pytest.mark.asyncio
async def test_a_failed_node_request_logs_without_the_key(monkeypatch, fmt):
    """aiohttp puts the request URL, key included, in its error message."""
    from aiohttp import web

    monkeypatch.setattr(settings, "ETH_RPC_URL", KEYED_URL)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")

    async def unauthorized(_request: web.Request) -> web.Response:
        return web.Response(status=401)

    app = web.Application()
    app.router.add_post("/v3/{key}", unauthorized)
    runner = web.AppRunner(app, access_log=None)  # the fake node's own log is not ours
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 18602).start()
    root = logging.getLogger()
    level = root.level
    buffer = io.StringIO()
    handler = configure_logging(level="INFO", fmt=fmt, stream=buffer)
    try:
        provider = rpc.InstrumentedHTTPProvider(KEYED_URL, chain_id=1)
        try:
            await provider.make_request("eth_blockNumber", [])
        except Exception as exc:
            assert "SECRETKEY123" in str(exc)  # the leak this guards against
            logging.getLogger("core.ingestor").exception(f"Failed to fetch block 7: {exc}")
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
        await runner.cleanup()

    output = buffer.getvalue()
    assert "Failed to fetch block 7: 401" in output
    assert "http://127.0.0.1:18602/<redacted>" in output
    assert "SECRETKEY123" not in output


def test_api_errors_do_not_return_the_key(client, monkeypatch):
    monkeypatch.setattr(settings, "ETH_RPC_URL", KEYED_URL)
    error = RuntimeError(f"401, message='Unauthorized', url='{KEYED_URL}'")
    with patch("scripts.run_audit.AuditRunner.run_audit", AsyncMock(side_effect=error)):
        task_id = client.post("/api/v1/audits", json={"chain_id": 1, "pool_address": POOL}).json()[
            "task_id"
        ]
        for _ in range(200):
            body = client.get(f"/api/v1/audits/{task_id}").json()
            if body["status"] != "running":
                break
            time.sleep(0.01)
    assert body["status"] == "failed"
    assert "SECRETKEY123" not in body["error"]
    assert "http://127.0.0.1:18602/<redacted>" in body["error"]


@pytest.mark.asyncio
async def test_monitor_error_events_do_not_carry_the_key(monkeypatch):
    from core.realtime_monitor import RealtimeMonitor

    monkeypatch.setattr(settings, "ETH_RPC_URL", KEYED_URL)
    monitor = RealtimeMonitor(AsyncMock(), chain_id=1, pool_address=POOL, poll_interval=0.01)
    monitor.ingestor = AsyncMock()
    failure = RuntimeError(f"url='{KEYED_URL}'")
    with patch.object(monitor, "poll_once", AsyncMock(side_effect=failure)):
        events = monitor.stream()
        await anext(events)  # status
        error = await anext(events)
        monitor.stop()
    assert error.type == "error"
    assert "SECRETKEY123" not in error.payload["reason"]
