"""
Hardening middleware: per-client rate limiting and security headers.
"""

from __future__ import annotations

import asyncio
import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from config.settings import settings


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
        # The interactive docs pages need scripts/styles; everything else is
        # a JSON API and gets a deny-all policy.
        if not request.url.path.startswith(("/docs", "/redoc", "/openapi.json")):
            response.headers.setdefault(
                "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'"
            )
        if settings.API_HSTS_ENABLED:
            response.headers.setdefault(
                "Strict-Transport-Security", "max-age=63072000; includeSubDomains"
            )
        return response
