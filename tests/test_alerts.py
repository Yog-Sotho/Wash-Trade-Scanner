"""
Alert sinks and their dispatcher (roadmap 4.3), against real local HTTP and
SMTP servers.
"""

import asyncio
import hashlib
import hmac
import io
import json
import logging
import time
from datetime import UTC, datetime
from email import message_from_string, policy

import pytest
from aiohttp import web
from prometheus_client import REGISTRY
from pydantic import ValidationError

from config.settings import Settings, settings
from core.alerts import (
    AlertBatch,
    AlertDispatcher,
    DeliveryError,
    EmailSink,
    SlackSink,
    WebhookSink,
    sinks_from_settings,
)
from core.log_config import configure_logging

POOL = "0x" + "9" * 40


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def trade(i: int, method: str = "round_trip", volume: float = 1000.0) -> dict:
    return {
        "id": i,
        "transaction_hash": "0x" + f"{i:064x}",
        "block_number": 100 + i,
        "volume_usd": volume,
        "detection_method": method,
        "wash_trade_score": 0.97,
    }


def batch(n: int = 2, **kwargs) -> AlertBatch:
    return AlertBatch(
        chain_id=1,
        chain_name="Ethereum",
        pool_address=POOL,
        trades=tuple(trade(i, **kwargs) for i in range(n)),
        detected_at=datetime(2026, 11, 19, 12, 0, tzinfo=UTC),
    )


class Receiver:
    """A real HTTP endpoint answering with a scripted list of statuses."""

    def __init__(self, statuses=(200,), headers=None):
        self.statuses = list(statuses)
        self.headers = headers or {}
        self.requests: list[tuple[dict, bytes]] = []
        self.url = ""

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append((dict(request.headers), await request.read()))
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return web.Response(status=status, text="ok", headers=self.headers)


@pytest.fixture
async def serve(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    runners = []

    async def start(receiver: Receiver) -> Receiver:
        app = web.Application()
        app.router.add_post("/hook/{tail:.*}", receiver.handle)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        host, port = runner.addresses[0][:2]
        receiver.url = f"http://{host}:{port}/hook/T00/B00/s3cr3t-token"
        runners.append(runner)
        return receiver

    yield start
    for runner in runners:
        await runner.cleanup()


async def deliver(sink, alert: AlertBatch, **kwargs) -> AlertDispatcher:
    dispatcher = AlertDispatcher([sink], backoff_base=0.01, **kwargs)
    await dispatcher.start()
    dispatcher.publish(alert)
    await asyncio.wait_for(dispatcher.drain(), 10)
    await dispatcher.stop()
    return dispatcher


# --------------------------------------------------------------- the batch


def test_each_listed_trade_says_why_it_was_flagged():
    explained = trade(1)
    explained["wash_evidence"] = {
        "detectors": [{"detector": "round_trip", "summary": "Bought and sold back 2 blocks later"}]
    }
    lines = AlertBatch(
        chain_id=1,
        chain_name="Ethereum",
        pool_address=POOL,
        trades=(explained, trade(2)),
        detected_at=datetime(2026, 11, 19, 12, 0, tzinfo=UTC),
    ).lines()
    assert lines[3] == "  why: round_trip: Bought and sold back 2 blocks later"
    assert not any(line.startswith("  why") for line in lines[4:])  # no evidence, no line


def test_a_batch_summarises_and_lists_its_trades():
    one, many = batch(1), batch(12, volume=10.0)
    assert one.title() == f"1 wash trade flagged in Ethereum pool {POOL}"
    assert many.title().startswith("12 wash trades")
    lines = many.lines()
    assert lines[0] == "Wash volume: $120.00 (round_trip: 12)"
    assert lines[-1] == "... and 2 more"  # ten written out
    body = many.to_json()
    assert body["type"] == "wash_trade_alert"
    assert body["trade_count"] == 12 and len(body["trades"]) == 12
    assert body["methods"] == {"round_trip": 12}
    assert body["detected_at"] == "2026-11-19T12:00:00+00:00"


# ------------------------------------------------------------------- sinks


async def test_webhook_posts_the_batch_signed_with_hmac(serve):
    receiver = await serve(Receiver())
    secret = "shared-secret"
    sent = sample("wash_alert_deliveries_total", sink="webhook", outcome="sent")

    await deliver(WebhookSink(receiver.url, secret), batch(3))

    ((headers, body),) = receiver.requests
    assert json.loads(body)["trade_count"] == 3
    assert headers["Content-Type"] == "application/json"
    # What a receiver does to verify it (docs/alerting.md):
    timestamp = headers["X-Wash-Timestamp"]
    expected = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    assert headers["X-Wash-Signature"] == f"sha256={expected.hexdigest()}"
    assert abs(int(timestamp) - time.time()) < 60
    assert sample("wash_alert_deliveries_total", sink="webhook", outcome="sent") == sent + 1


async def test_an_unsigned_webhook_carries_no_signature(serve):
    receiver = await serve(Receiver())
    await deliver(WebhookSink(receiver.url), batch())
    ((headers, _body),) = receiver.requests
    assert "X-Wash-Signature" not in headers and "X-Wash-Timestamp" not in headers


async def test_slack_gets_one_escaped_text_message(serve):
    receiver = await serve(Receiver())
    await deliver(SlackSink(receiver.url), batch(2, method="<b>&"))
    ((_headers, body),) = receiver.requests
    text = json.loads(body)["text"]
    assert text.startswith(f"*2 wash trades flagged in Ethereum pool {POOL}*")
    assert "&lt;b&gt;&amp;" in text and "<b>" not in text


@pytest.mark.parametrize(
    ("statuses", "headers", "attempts", "outcome"),
    [
        ([500, 502, 200], {}, 3, "sent"),  # server errors are retried
        ([429, 200], {"Retry-After": "0.05"}, 2, "sent"),  # the receiver's wait is honoured
        ([400], {}, 1, "failed"),  # a refusal is final
        ([503], {}, 3, "failed"),  # attempts run out
    ],
)
async def test_retries_follow_the_receivers_answer(serve, statuses, headers, attempts, outcome):
    receiver = await serve(Receiver(statuses, headers))
    before = sample("wash_alert_deliveries_total", sink="webhook", outcome=outcome)

    await deliver(WebhookSink(receiver.url), batch(), max_attempts=3)

    assert len(receiver.requests) == attempts
    assert sample("wash_alert_deliveries_total", sink="webhook", outcome=outcome) == before + 1


async def test_retry_after_sets_the_wait(serve, monkeypatch):
    receiver = await serve(Receiver([429, 200], {"Retry-After": "7"}))
    waits: list[float] = []
    real_sleep = asyncio.sleep

    async def record(delay, *_args):
        waits.append(delay)
        await real_sleep(0)

    monkeypatch.setattr("core.alerts.asyncio.sleep", record)
    await deliver(WebhookSink(receiver.url), batch())
    assert waits == [7.0]


async def test_an_unreachable_receiver_is_retried_then_given_up(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    failed = sample("wash_alert_deliveries_total", sink="webhook", outcome="failed")
    # Port 9 (discard) on loopback: nothing listens, the connection is refused.
    await deliver(WebhookSink("http://127.0.0.1:9/hook"), batch(), max_attempts=2)
    assert sample("wash_alert_deliveries_total", sink="webhook", outcome="failed") == failed + 1


async def test_a_failed_delivery_log_never_holds_the_secret_url(serve, monkeypatch):
    receiver = await serve(Receiver([400]))
    monkeypatch.setattr(settings, "ALERT_SLACK_WEBHOOK_URL", receiver.url)
    root = logging.getLogger()
    level = root.level
    buffer = io.StringIO()
    handler = configure_logging(level="INFO", fmt="text", stream=buffer)
    try:
        await deliver(SlackSink(receiver.url), batch())
        logging.getLogger("test").error(f"for the record: {receiver.url}")
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
    output = buffer.getvalue()
    assert "Alert to slack failed after 1 attempt(s): HTTP 400" in output
    assert "s3cr3t-token" not in output  # even when something logs the URL itself


# -------------------------------------------------------------- dispatcher


class HangingSink:
    name = "hanging"

    def __init__(self):
        self.started = asyncio.Event()

    async def send(self, batch, session):
        self.started.set()
        await asyncio.sleep(3600)


async def test_publishing_never_waits_for_delivery():
    sink = HangingSink()
    dispatcher = AlertDispatcher([sink])
    await dispatcher.start()
    start = time.perf_counter()
    for _ in range(50):
        dispatcher.publish(batch())
    assert time.perf_counter() - start < 0.05
    await asyncio.wait_for(sink.started.wait(), 5)
    start = time.perf_counter()
    await dispatcher.stop(grace=0.1)  # a hung sink cannot hold up shutdown
    assert time.perf_counter() - start < 2


async def test_a_full_queue_drops_its_oldest_batch():
    sink = HangingSink()
    dispatcher = AlertDispatcher([sink], queue_size=2)  # not started: nothing drains
    dropped = sample("wash_alert_deliveries_total", sink="hanging", outcome="dropped")
    first, second, third = batch(1), batch(2), batch(3)
    for alert in (first, second, third):
        dispatcher.publish(alert)
    assert sample("wash_alert_deliveries_total", sink="hanging", outcome="dropped") == dropped + 1
    queue = dispatcher._queues["hanging"]
    assert [queue.get_nowait() for _ in range(2)] == [second, third]


async def test_every_sink_gets_every_batch(serve):
    hook, slack = await serve(Receiver()), await serve(Receiver())
    dispatcher = AlertDispatcher([WebhookSink(hook.url), SlackSink(slack.url)])
    await dispatcher.start()
    dispatcher.publish(batch(1))
    dispatcher.publish(batch(2))
    await asyncio.wait_for(dispatcher.drain(), 10)
    await dispatcher.stop()
    assert len(hook.requests) == 2 and len(slack.requests) == 2


async def test_stop_delivers_what_is_queued(serve):
    receiver = await serve(Receiver())
    dispatcher = AlertDispatcher([WebhookSink(receiver.url)])
    await dispatcher.start()
    dispatcher.publish(batch())
    await dispatcher.stop(grace=5)
    assert len(receiver.requests) == 1


# ------------------------------------------------------------------- email


class FakeSMTP:
    """Just enough of an SMTP server for smtplib: EHLO, AUTH PLAIN, MAIL,
    RCPT, DATA, QUIT."""

    def __init__(self, refuse_recipients: bool = False):
        self.refuse_recipients = refuse_recipients
        self.messages: list[dict] = []
        self.logins: list[str] = []
        self.connections = 0

    async def handle(self, reader, writer):
        self.connections += 1

        def say(line: str) -> None:
            writer.write((line + "\r\n").encode())

        say("220 fake ESMTP")
        await writer.drain()
        in_data, lines, sender, recipients = False, [], "", []
        while line := await reader.readline():
            text = line.decode().rstrip("\r\n")
            if in_data:
                if text == ".":
                    self.messages.append(
                        {"from": sender, "to": recipients, "data": "\n".join(lines)}
                    )
                    in_data, lines = False, []
                    say("250 queued")
                else:
                    lines.append(text[1:] if text.startswith("..") else text)
            else:
                command = text.upper()
                if command.startswith("EHLO"):
                    say("250-fake")
                    say("250 AUTH PLAIN")
                elif command.startswith("AUTH"):
                    self.logins.append(text)
                    say("235 authenticated")
                elif command.startswith("MAIL FROM"):
                    sender = text
                    say("250 ok")
                elif command.startswith("RCPT TO"):
                    if self.refuse_recipients:
                        say("550 no such user")
                    else:
                        recipients.append(text)
                        say("250 ok")
                elif command == "DATA":
                    in_data = True
                    say("354 go ahead")
                elif command == "QUIT":
                    say("221 bye")
                    await writer.drain()
                    break
                else:
                    say("250 ok")
            await writer.drain()
        writer.close()


@pytest.fixture
async def smtp_server():
    servers = []

    async def start(fake: FakeSMTP) -> int:
        server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
        servers.append(server)
        return server.sockets[0].getsockname()[1]

    yield start
    for server in servers:
        server.close()
        await server.wait_closed()


async def test_email_is_sent_over_smtp(smtp_server):
    fake = FakeSMTP()
    port = await smtp_server(fake)
    sink = EmailSink(
        "127.0.0.1",
        port,
        "scanner@example.org",
        ["ops@example.org", "risk@example.org"],
        username="scanner",
        password="pw",
        tls="none",
    )

    await deliver(sink, batch(2))

    (message,) = fake.messages
    assert len(message["to"]) == 2
    assert fake.logins and fake.logins[0].startswith("AUTH PLAIN")
    email = message_from_string(message["data"], policy=policy.default)  # unfolds headers
    assert email["Subject"] == f"[wash-trade-scanner] 2 wash trades flagged in Ethereum pool {POOL}"
    assert email["From"] == "scanner@example.org"
    assert email["To"] == "ops@example.org, risk@example.org"
    assert trade(1)["transaction_hash"] in email.get_content()


async def test_refused_recipients_are_not_retried(smtp_server):
    fake = FakeSMTP(refuse_recipients=True)
    port = await smtp_server(fake)
    failed = sample("wash_alert_deliveries_total", sink="email", outcome="failed")
    sink = EmailSink("127.0.0.1", port, "a@example.org", ["b@example.org"], tls="none")

    await deliver(sink, batch(), max_attempts=3)

    assert fake.messages == []
    assert fake.connections == 1  # refused for good: no second attempt
    assert sample("wash_alert_deliveries_total", sink="email", outcome="failed") == failed + 1


async def test_an_unreachable_smtp_server_is_retryable():
    sink = EmailSink("127.0.0.1", 9, "a@example.org", ["b@example.org"], tls="none", timeout=2)
    with pytest.raises(DeliveryError) as raised:
        await sink.send(batch(), session=None)
    assert raised.value.retryable


# ------------------------------------------------------------ configuration


def test_no_sink_without_configuration():
    assert sinks_from_settings() == []


def test_sinks_are_built_from_settings(monkeypatch):
    monkeypatch.setattr(settings, "ALERT_WEBHOOK_URL", "https://hooks.example.org/x")
    monkeypatch.setattr(
        settings, "ALERT_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X"
    )
    monkeypatch.setattr(settings, "ALERT_EMAIL_TO", "ops@example.org, risk@example.org")
    monkeypatch.setattr(settings, "ALERT_EMAIL_FROM", "scanner@example.org")
    monkeypatch.setattr(settings, "ALERT_SMTP_HOST", "smtp.example.org")
    sinks = sinks_from_settings()
    assert [s.name for s in sinks] == ["webhook", "slack", "email"]
    assert sinks[2].recipients == ["ops@example.org", "risk@example.org"]


def test_a_half_configured_email_sink_is_refused(monkeypatch):
    monkeypatch.setattr(settings, "ALERT_EMAIL_TO", "ops@example.org")
    with pytest.raises(ValueError, match="ALERT_SMTP_HOST and ALERT_EMAIL_FROM"):
        sinks_from_settings()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ALERT_WEBHOOK_URL", "ftp://example.org/hook"),
        ("ALERT_SLACK_WEBHOOK_URL", "http://hooks.slack.com/services/T/B/X"),  # https only
        ("ALERT_SMTP_TLS", "maybe"),
        ("MONITOR_POOLS", "1:0x1234"),
        ("MONITOR_POOLS", "ethereum:0x" + "a" * 40),
    ],
)
def test_bad_alert_settings_are_rejected(field, value):
    with pytest.raises(ValidationError):
        Settings(**{field: value})


def test_monitor_pools_parse_to_lowercase_pairs(monkeypatch):
    monkeypatch.setattr(settings, "MONITOR_POOLS", f" 1:0x{'A' * 40}, 8453:{POOL},1:0x{'a' * 40}")
    assert settings.monitor_pool_list == [(1, "0x" + "a" * 40), (8453, POOL)]


def test_the_documented_verification_accepts_real_signatures_only():
    """docs/alerting.md's snippet is what receivers copy: run it as written."""
    from pathlib import Path

    doc = (Path(__file__).parent.parent / "docs" / "alerting.md").read_text()
    snippet = doc.split("```python\n", 1)[1].split("```", 1)[0]
    namespace: dict = {}
    exec(snippet, namespace)  # noqa: S102 - our own documentation
    verify = namespace["verify"]

    sink = WebhookSink("http://unused", secret="shared-secret")
    body = json.dumps(batch().to_json()).encode()
    headers = sink.headers_for(body, int(time.time()))
    assert verify(b"shared-secret", headers, body)
    assert not verify(b"other-secret", headers, body)
    assert not verify(b"shared-secret", headers, body + b" ")
    stale = sink.headers_for(body, int(time.time()) - 3600)
    assert not verify(b"shared-secret", stale, body)
