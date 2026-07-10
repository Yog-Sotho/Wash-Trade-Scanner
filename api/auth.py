"""
API-key authentication.

Security model:
- Plaintext keys are never stored server-side: `API_KEY_HASHES` holds
  SHA-256 hex digests, and incoming keys are hashed before comparison.
- Comparison is constant-time (`hmac.compare_digest`) against every
  configured hash, so neither key content nor which key matched leaks
  through timing.
- Auth is off by default because the server binds to loopback by default;
  the launcher refuses to bind a public interface without auth enabled.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

from fastapi import HTTPException, Security, WebSocket, status
from fastapi.security import APIKeyHeader

from config.settings import settings

API_KEY_HEADER = "X-API-Key"

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)

# Websocket close code for failed auth (4000-4999 = application-defined).
WS_UNAUTHORIZED = 4401


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


async def require_api_key(api_key: str | None = Security(_api_key_header)) -> None:
    """FastAPI dependency enforcing API-key auth on HTTP routes."""
    if not settings.API_AUTH_ENABLED:
        return
    if not verify_api_key(api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )


async def authorize_websocket(websocket: WebSocket) -> bool:
    """Check websocket auth before accepting; closes the socket on failure."""
    if not settings.API_AUTH_ENABLED:
        return True
    if verify_api_key(websocket.headers.get(API_KEY_HEADER.lower())):
        return True
    await websocket.close(code=WS_UNAUTHORIZED, reason="Invalid or missing API key")
    return False
