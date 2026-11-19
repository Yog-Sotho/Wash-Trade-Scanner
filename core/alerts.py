"""
Alert sinks: where the API server's background monitors (MONITOR_POOLS) send
newly flagged wash trades. See docs/alerting.md.

One notification per monitor pass: an AlertBatch holds every trade a pass
newly labelled wash in one pool, so twenty trades from one burst make one
Slack message, not twenty.

Sinks:
  webhook  JSON POST to ALERT_WEBHOOK_URL, signed with HMAC-SHA256 over
           "<timestamp>.<body>" when ALERT_WEBHOOK_SECRET is set
  slack    a Slack incoming webhook (ALERT_SLACK_WEBHOOK_URL)
  email    SMTP (ALERT_SMTP_*), to ALERT_EMAIL_TO

Delivery never blocks a monitor: AlertDispatcher queues each batch for each
sink and delivers in the background, retrying a failed attempt with
exponential backoff (or the receiver's Retry-After) up to ALERT_MAX_ATTEMPTS.
A full queue drops its oldest batch. Every outcome is counted in
wash_alert_deliveries_total{sink, outcome}.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import smtplib
import ssl
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from typing import Any, Protocol

import aiohttp

from config.settings import settings
from core import metrics

logger = logging.getLogger(__name__)

USER_AGENT = "wash-trade-scanner"
SIGNATURE_HEADER = "X-Wash-Signature"
TIMESTAMP_HEADER = "X-Wash-Timestamp"
QUEUE_SIZE = 1000  # batches waiting per sink
MAX_BACKOFF_SECONDS = 300.0
LISTED_TRADES = 10  # trades written out in a Slack message or an email


@dataclass(frozen=True)
class AlertBatch:
    """The trades one monitor pass newly labelled wash in one pool."""

    chain_id: int
    chain_name: str
    pool_address: str
    trades: tuple[dict[str, Any], ...]  # SwapTradeResponse dicts
    detected_at: datetime

    @property
    def wash_volume_usd(self) -> float:
        return sum(t.get("volume_usd") or 0.0 for t in self.trades)

    @property
    def methods(self) -> dict[str, int]:
        return dict(Counter(t.get("detection_method") or "unknown" for t in self.trades))

    def to_json(self) -> dict[str, Any]:
        return {
            "type": "wash_trade_alert",
            "version": 1,
            "chain_id": self.chain_id,
            "chain_name": self.chain_name,
            "pool_address": self.pool_address,
            "detected_at": self.detected_at.isoformat(),
            "trade_count": len(self.trades),
            "wash_volume_usd": self.wash_volume_usd,
            "methods": self.methods,
            "trades": list(self.trades),
        }

    def title(self) -> str:
        n = len(self.trades)
        return (
            f"{n} wash trade{'s' if n != 1 else ''} flagged in {self.chain_name} "
            f"pool {self.pool_address}"
        )

    def lines(self) -> list[str]:
        methods = ", ".join(f"{m}: {c}" for m, c in sorted(self.methods.items()))
        out = [
            f"Wash volume: ${self.wash_volume_usd:,.2f} ({methods})",
            f"Detected at {self.detected_at.isoformat()}",
        ]
        for trade in self.trades[:LISTED_TRADES]:
            out.append(
                f"- block {trade.get('block_number')} tx {trade.get('transaction_hash')} "
                f"${trade.get('volume_usd') or 0.0:,.2f} "
                f"{trade.get('detection_method')} (score {trade.get('wash_trade_score', 0.0):.3f})"
            )
        if len(self.trades) > LISTED_TRADES:
            out.append(f"... and {len(self.trades) - LISTED_TRADES} more")
        return out


class DeliveryError(Exception):
    """A failed delivery attempt. `retryable`: worth another attempt;
    `retry_after`: the receiver's requested wait, in seconds."""

    def __init__(self, message: str, retryable: bool, retry_after: float | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after


class AlertSink(Protocol):
    name: str

    async def send(self, batch: AlertBatch, session: aiohttp.ClientSession) -> None:
        """Deliver once, or raise DeliveryError."""


def _retry_after(value: str | None) -> float | None:
    try:
        return max(0.0, float(value)) if value else None
    except ValueError:
        return None  # an HTTP date: fall back to backoff


async def _post(
    session: aiohttp.ClientSession, url: str, body: bytes, headers: dict[str, str]
) -> None:
    """POST and classify the outcome: 2xx is delivered; 429 and 5xx (and no
    answer) are worth retrying; any other status is refused for good."""
    try:
        async with session.post(url, data=body, headers=headers) as response:
            if 200 <= response.status < 300:
                return
            retryable = response.status == 429 or response.status >= 500
            raise DeliveryError(
                f"HTTP {response.status}",
                retryable=retryable,
                retry_after=_retry_after(response.headers.get("Retry-After")),
            )
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise DeliveryError(type(exc).__name__, retryable=True) from exc


class WebhookSink:
    name = "webhook"

    def __init__(self, url: str, secret: str | None = None) -> None:
        self.url = url
        self.secret = (secret or "").encode()  # empty: unsigned

    def headers_for(self, body: bytes, timestamp: int) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}
        if self.secret:
            signed = str(timestamp).encode() + b"." + body
            digest = hmac.new(self.secret, signed, hashlib.sha256).hexdigest()
            headers[TIMESTAMP_HEADER] = str(timestamp)
            headers[SIGNATURE_HEADER] = f"sha256={digest}"
        return headers

    async def send(self, batch: AlertBatch, session: aiohttp.ClientSession) -> None:
        body = json.dumps(batch.to_json(), separators=(",", ":")).encode()
        await _post(session, self.url, body, self.headers_for(body, int(time.time())))


def _slack_escape(text: str) -> str:
    # Slack's mrkdwn reserves &, < and > (its docs ask senders to escape them).
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class SlackSink:
    name = "slack"

    def __init__(self, url: str) -> None:
        self.url = url

    def message(self, batch: AlertBatch) -> dict[str, str]:
        lines = [f"*{_slack_escape(batch.title())}*", *map(_slack_escape, batch.lines())]
        return {"text": "\n".join(lines)}

    async def send(self, batch: AlertBatch, session: aiohttp.ClientSession) -> None:
        body = json.dumps(self.message(batch)).encode()
        headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}
        await _post(session, self.url, body, headers)


class EmailSink:
    name = "email"

    def __init__(
        self,
        host: str,
        port: int,
        sender: str,
        recipients: list[str],
        username: str | None = None,  # None or empty: no login
        password: str | None = None,
        tls: str = "starttls",
        timeout: float = 10.0,
    ) -> None:
        self.host, self.port, self.tls, self.timeout = host, port, tls, timeout
        self.sender, self.recipients = sender, recipients
        self.username, self.password = username, password

    def message(self, batch: AlertBatch) -> EmailMessage:
        message = EmailMessage()
        message["Subject"] = f"[wash-trade-scanner] {batch.title()}"
        message["From"] = self.sender
        message["To"] = ", ".join(self.recipients)
        message.set_content("\n".join([batch.title(), "", *batch.lines()]) + "\n")
        return message

    def _send_blocking(self, message: EmailMessage) -> None:
        context = ssl.create_default_context()
        smtp: smtplib.SMTP
        if self.tls == "ssl":
            smtp = smtplib.SMTP_SSL(self.host, self.port, timeout=self.timeout, context=context)
        else:
            smtp = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
        with smtp:
            if self.tls == "starttls":
                smtp.starttls(context=context)
            if self.username:
                smtp.login(self.username, self.password or "")
            smtp.send_message(message)

    async def send(self, batch: AlertBatch, session: aiohttp.ClientSession) -> None:
        try:
            await asyncio.to_thread(self._send_blocking, self.message(batch))
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPRecipientsRefused) as exc:
            raise DeliveryError(type(exc).__name__, retryable=False) from exc
        except (smtplib.SMTPException, OSError) as exc:  # TimeoutError is an OSError
            raise DeliveryError(type(exc).__name__, retryable=True) from exc


def sinks_from_settings() -> list[AlertSink]:
    """The sinks whose address is configured. Raises ValueError on a
    half-configured email sink, so the server fails at startup."""
    sinks: list[AlertSink] = []
    if settings.ALERT_WEBHOOK_URL:
        sinks.append(WebhookSink(settings.ALERT_WEBHOOK_URL, settings.ALERT_WEBHOOK_SECRET))
    if settings.ALERT_SLACK_WEBHOOK_URL:
        sinks.append(SlackSink(settings.ALERT_SLACK_WEBHOOK_URL))
    if settings.alert_email_recipients:
        missing = [
            name
            for name, value in (
                ("ALERT_SMTP_HOST", settings.ALERT_SMTP_HOST),
                ("ALERT_EMAIL_FROM", settings.ALERT_EMAIL_FROM),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"ALERT_EMAIL_TO is set but {' and '.join(missing)} is not")
        sinks.append(
            EmailSink(
                settings.ALERT_SMTP_HOST,
                settings.ALERT_SMTP_PORT,
                settings.ALERT_EMAIL_FROM,
                settings.alert_email_recipients,
                settings.ALERT_SMTP_USERNAME,
                settings.ALERT_SMTP_PASSWORD,
                settings.ALERT_SMTP_TLS,
                settings.ALERT_TIMEOUT_SECONDS,
            )
        )
    return sinks


class AlertDispatcher:
    """Queues each batch for every sink and delivers in the background."""

    def __init__(
        self,
        sinks: list[AlertSink],
        max_attempts: int | None = None,
        timeout: float | None = None,
        backoff_base: float = 1.0,
        queue_size: int = QUEUE_SIZE,
    ) -> None:
        self.sinks = sinks
        self.max_attempts = max_attempts or settings.ALERT_MAX_ATTEMPTS
        self.timeout = timeout or settings.ALERT_TIMEOUT_SECONDS
        self.backoff_base = backoff_base
        self._queues: dict[str, asyncio.Queue[AlertBatch]] = {
            sink.name: asyncio.Queue(maxsize=queue_size) for sink in sinks
        }
        self._workers: list[asyncio.Task[None]] = []
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        if not self.sinks:
            return
        # trust_env: honour HTTPS_PROXY / NO_PROXY, as deployments behind an
        # egress proxy need.
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout), trust_env=True
        )
        self._workers = [
            asyncio.create_task(self._work(sink), name=f"alerts-{sink.name}") for sink in self.sinks
        ]

    def publish(self, batch: AlertBatch) -> None:
        """Queue `batch` for every sink. Never blocks; a full queue loses its
        oldest batch."""
        for sink in self.sinks:
            queue = self._queues[sink.name]
            if queue.full():
                queue.get_nowait()
                queue.task_done()
                metrics.ALERT_DELIVERIES.labels(sink.name, "dropped").inc()
                logger.error(f"Alert queue for {sink.name} is full: dropped its oldest batch")
            queue.put_nowait(batch)

    async def drain(self) -> None:
        """Wait until every queued batch has been delivered or given up."""
        await asyncio.gather(*(queue.join() for queue in self._queues.values()))

    async def stop(self, grace: float = 5.0) -> None:
        """Deliver what is queued for up to `grace` seconds, then stop."""
        if self._workers:
            try:
                await asyncio.wait_for(self.drain(), grace)
            except TimeoutError:
                logger.warning("Stopping with undelivered alerts in the queue")
            for worker in self._workers:
                worker.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers = []
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _work(self, sink: AlertSink) -> None:
        queue = self._queues[sink.name]
        while True:
            batch = await queue.get()
            try:
                await self._deliver(sink, batch)
            except Exception:
                logger.exception(f"Alert delivery to {sink.name} crashed")
            finally:
                queue.task_done()

    async def _deliver(self, sink: AlertSink, batch: AlertBatch) -> None:
        assert self._session is not None, "call start() first"
        for attempt in range(1, self.max_attempts + 1):
            try:
                await sink.send(batch, self._session)
            except DeliveryError as exc:
                last = attempt == self.max_attempts or not exc.retryable
                if last:
                    metrics.ALERT_DELIVERIES.labels(sink.name, "failed").inc()
                    # exc holds a status or an exception type, never a URL.
                    logger.error(
                        f"Alert to {sink.name} failed after {attempt} attempt(s): {exc} "
                        f"({batch.title()})"
                    )
                    return
                delay = exc.retry_after
                if delay is None:
                    delay = self.backoff_base * 2 ** (attempt - 1)
                delay = min(delay, MAX_BACKOFF_SECONDS)
                logger.warning(f"Alert to {sink.name} failed ({exc}); retrying in {delay:.1f}s")
                await asyncio.sleep(delay)
            else:
                metrics.ALERT_DELIVERIES.labels(sink.name, "sent").inc()
                return
