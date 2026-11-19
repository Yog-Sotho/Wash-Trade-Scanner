"""
API-key and session-cookie authentication.

Security model:
- Plaintext keys are never stored server-side: `API_KEY_HASHES` holds
  SHA-256 hex digests, and incoming keys are hashed before comparison.
- Comparison is constant-time (`hmac.compare_digest`) against every
  configured hash, so neither key content nor which key matched leaks
  through timing.
- The web panel logs in with an API key once and receives an HMAC-signed,
  expiring session token in an HttpOnly SameSite=Strict cookie. The signing
  secret is generated per process, so restarting the server invalidates all
  sessions (no secret is ever persisted), unless PANEL_SESSION_SECRET sets
  one shared by every instance. Cookies are also how browser
  websockets authenticate - the WebSocket API cannot send custom headers.
- Auth is off by default because the server binds to loopback by default;
  the launcher refuses to bind a public interface without auth enabled.
- Wrong API keys are throttled per client (api/limits.py): repeated failures
  lock the client out with a doubling backoff, during which even a right key
  is refused, so guessing is slow on every route, not only the panel login.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import secrets
import time

from fastapi import Cookie, HTTPException, Request, Security, WebSocket, status
from fastapi.security import APIKeyHeader

from api.limits import Limits, client_ip
from config.settings import settings

API_KEY_HEADER = "X-API-Key"
SESSION_COOKIE = "wash_session"

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)

# Per-process signing secret, used unless PANEL_SESSION_SECRET is set: then
# sessions never survive a restart and are valid on this instance only.
_process_secret = secrets.token_bytes(32)


def _session_secret() -> bytes:
    configured = settings.PANEL_SESSION_SECRET
    return configured.encode("utf-8") if configured else _process_secret


# Websocket close codes (4000-4999 = application-defined).
WS_UNAUTHORIZED = 4401
WS_LOCKED_OUT = 4429
LOCKED_OUT_DETAIL = "Too many failed authentication attempts"


def generate_api_key() -> tuple[str, str]:
    """Create a new API key. Returns (plaintext_key, sha256_hash)."""
    key = secrets.token_urlsafe(32)
    return key, hash_api_key(key)


def hash_api_key(key: str) -> str:
    """SHA-256 hex digest of an API key."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def verify_api_key(provided_key: str | None) -> bool:
    """Constant-time check of a presented key against all configured hashes."""
    if not provided_key:
        return False
    provided_hash = hash_api_key(provided_key)
    accepted = settings.api_key_hash_set
    # Evaluate every hash so timing does not reveal which (if any) matched.
    matched = False
    for accepted_hash in accepted:
        if hmac.compare_digest(provided_hash, accepted_hash):
            matched = True
    return matched


def create_session_token(ttl_minutes: int | None = None) -> str:
    """Create an HMAC-signed session token with an absolute expiry."""
    ttl = ttl_minutes if ttl_minutes is not None else settings.PANEL_SESSION_TTL_MINUTES
    expires_at = int(time.time()) + ttl * 60
    payload = str(expires_at)
    signature = hmac.new(_session_secret(), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def verify_session_token(token: str | None) -> bool:
    """Validate a session token's signature and expiry (constant-time)."""
    if not token:
        return False
    payload, _, signature = token.partition(".")
    if not payload or not signature:
        return False
    expected = hmac.new(_session_secret(), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return False
    try:
        return int(payload) > time.time()
    except ValueError:
        return False


def is_authenticated(api_key: str | None, session_token: str | None) -> bool:
    """True if either credential is valid (or auth is disabled)."""
    if not settings.API_AUTH_ENABLED:
        return True
    return verify_api_key(api_key) or verify_session_token(session_token)


async def check_credentials(
    limits: Limits, client: str, api_key: str | None, session_token: str | None
) -> tuple[bool, float]:
    """(authenticated, seconds of lockout left), throttling wrong keys.

    A locked-out client is refused before its credentials are looked at. Only
    a wrong key counts as a failure: no credential at all, or an expired
    session cookie, is not a guess.
    """
    if not settings.API_AUTH_ENABLED:
        return True, 0.0
    state = await limits.auth_state(client)
    if state.locked_for > 0:
        return False, state.locked_for
    if is_authenticated(api_key, session_token):
        await limits.auth_succeeded(client, state)
        return True, 0.0
    if api_key:
        await limits.auth_failed(client)
    return False, 0.0


def locked_out(seconds: float) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail=LOCKED_OUT_DETAIL,
        headers={"Retry-After": str(max(1, math.ceil(seconds)))},
    )


async def require_api_key(
    request: Request,
    api_key: str | None = Security(_api_key_header),
    session_token: str | None = Cookie(None, alias=SESSION_COOKIE),
) -> None:
    """FastAPI dependency: accept an API key header or a panel session cookie."""
    ok, locked = await check_credentials(
        request.app.state.limits, client_ip(request), api_key, session_token
    )
    if locked:
        raise locked_out(locked)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )


async def authorize_websocket(websocket: WebSocket) -> bool:
    """Check websocket auth before accepting; closes the socket on failure.

    Accepts the X-API-Key header (programmatic clients) or the session
    cookie (browsers, which cannot set custom websocket headers).
    """
    ok, locked = await check_credentials(
        websocket.app.state.limits,
        client_ip(websocket),
        websocket.headers.get(API_KEY_HEADER.lower()),
        websocket.cookies.get(SESSION_COOKIE),
    )
    if ok:
        return True
    if locked:
        await websocket.close(code=WS_LOCKED_OUT, reason=LOCKED_OUT_DETAIL)
    else:
        await websocket.close(code=WS_UNAUTHORIZED, reason="Invalid or missing API key")
    return False
