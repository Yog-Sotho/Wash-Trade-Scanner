"""
API middleware: request context (request id, access log, HTTP metrics),
per-client rate limiting and security headers.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid

from starlette.datastructures import Headers, MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from config.settings import settings
from core import metrics
from core.log_config import request_id_var

REQUEST_ID_HEADER = "X-Request-ID"
# A caller's id is kept only if it is short and plain, so it cannot forge
# log lines or carry markup into whatever displays the logs.
_VALID_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")
# Probed every few seconds by the container healthcheck and the metrics
# scraper: logged at DEBUG so they do not drown the access log.
_QUIET_PATHS = frozenset({"/health", "/health/live", "/health/ready", "/metrics"})

access_logger = logging.getLogger("api.access")


class RequestContextMiddleware:
    """Outermost middleware: gives every HTTP request and websocket a request id.

    The id is the caller's X-Request-ID when it is valid, else a new one. It
    is set in the logging context (core.log_config.request_id_var) for the
    whole request, including tasks the request starts, and returned in the
    X-Request-ID response header. Each HTTP request is then counted and timed
    by route template (wash_http_requests_total, ..._duration_seconds) and
    written to the `api.access` log.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        incoming = Headers(scope=scope).get(REQUEST_ID_HEADER)
        if incoming and _VALID_REQUEST_ID.fullmatch(incoming):
            request_id = incoming
        else:
            request_id = uuid.uuid4().hex
        token = request_id_var.set(request_id)
        try:
            if scope["type"] == "http":
                await self._http(scope, receive, send, request_id)
            else:
                await self.app(scope, receive, send)
        finally:
            request_id_var.reset(token)

    async def _http(self, scope: Scope, receive: Receive, send: Send, request_id: str) -> None:
        # 500 unless a response starts: an exception escaping the app here is
        # turned into a 500 by Starlette's outer error handler.
        status = 500
        start = time.perf_counter()

        async def send_with_id(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                MutableHeaders(scope=message)[REQUEST_ID_HEADER] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        finally:
            duration = time.perf_counter() - start
            # The router stores the matched route in the scope; its path is the
            # template (/api/v1/pools/{chain_id}/...), which keeps the label
            # set bounded. Unmatched paths (404s, probes) share one label.
            route = getattr(scope.get("route"), "path", None) or "unmatched"
            method = scope["method"]
            metrics.HTTP_REQUESTS.labels(method, route, str(status)).inc()
            metrics.HTTP_DURATION.labels(method, route).observe(duration)
            path = scope["path"]
            client = scope.get("client")
            access_logger.log(
                logging.DEBUG if path in _QUIET_PATHS else logging.INFO,
                "%s %s %d %.1fms",
                method,
                path,
                status,
                duration * 1000,
                extra={
                    "http_method": method,
                    "http_path": path,
                    "http_route": route,
                    "http_status": status,
                    "duration_ms": round(duration * 1000, 3),
                    "client_ip": client[0] if client else None,
                },
            )


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window per-client-IP rate limiter (in-memory).

    Suitable for a single-process deployment, which is the supported topology
    for this service; put a reverse proxy with its own limiter in front when
    scaling out.
    """

    def __init__(self, app, requests_per_minute: int | None = None) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self.requests_per_minute = requests_per_minute or settings.API_RATE_LIMIT_PER_MINUTE
        self._hits: dict[str, list[float]] = {}
        self._lock = asyncio.Lock()

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        client_ip = request.client.host if request.client else "unknown"
        now = time.monotonic()

        async with self._lock:
            window = [t for t in self._hits.get(client_ip, []) if t > now - 60.0]
            if len(window) >= self.requests_per_minute:
                self._hits[client_ip] = window
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded"},
                    headers={"Retry-After": "60"},
                )
            window.append(now)
            self._hits[client_ip] = window
            # Bound memory: drop idle clients once the table grows large.
            if len(self._hits) > 10_000:
                cutoff = now - 60.0
                self._hits = {
                    ip: hits for ip, hits in self._hits.items() if hits and hits[-1] > cutoff
                }

        return await call_next(request)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add defensive headers to every response."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Cache-Control", "no-store")
        path = request.url.path
        if path.startswith("/panel"):
            # The panel is fully self-hosted: same-origin scripts/styles only,
            # websockets for the live monitor, no external requests possible.
            response.headers.setdefault(
                "Content-Security-Policy",
                "default-src 'none'; script-src 'self'; style-src 'self'; "
                "img-src 'self' data:; connect-src 'self' ws: wss:; "
                "frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            )
        elif not path.startswith(("/docs", "/redoc", "/openapi.json")):
            # The interactive docs pages need scripts/styles; everything else
            # is a JSON API and gets a deny-all policy.
            response.headers.setdefault(
                "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'"
            )
        if settings.API_HSTS_ENABLED:
            response.headers.setdefault(
                "Strict-Transport-Security", "max-age=63072000; includeSubDomains"
            )
        return response
