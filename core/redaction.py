"""
Keep secret-bearing URLs out of logs and error messages.

Hosted node endpoints carry their API key in the URL
(https://mainnet.infura.io/v3/<key>), a Slack incoming webhook URL is itself
the credential, and aiohttp puts the full request URL in its error messages
("401, message='Unauthorized', url='https://...'"), which reach logs and API
responses. redact() replaces every configured RPC and alert-webhook URL with
its scheme and host.
"""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlsplit

from config.settings import settings

REDACTED = "<redacted>"


def redact_url(url: str) -> str:
    """https://user:pw@host:8545/v3/key?x=1 -> https://host:8545/<redacted>"""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return f"{parts.scheme}://{host}/{REDACTED}"


@lru_cache(maxsize=8)
def _replacements(urls: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    pairs: dict[str, str] = {}
    for url in urls:
        parts = urlsplit(url)
        # Only a URL with something after the host can hold a key.
        if not parts.scheme or not (parts.path.strip("/") or parts.query or parts.username):
            continue
        for form in (url, url.rstrip("/")):
            pairs[form] = redact_url(url)
    # Longest first, so a URL is not half-replaced via a shorter one it contains.
    return tuple(sorted(pairs.items(), key=lambda kv: -len(kv[0])))


def redact(text: str) -> str:
    """`text` with every configured RPC or alert-webhook URL replaced by
    scheme://host/<redacted>."""
    configured = [*settings.rpc_urls.values()]
    configured += [settings.ALERT_WEBHOOK_URL, settings.ALERT_SLACK_WEBHOOK_URL]
    urls = tuple(u for u in configured if u)
    for secret, replacement in _replacements(urls):
        if secret in text:
            text = text.replace(secret, replacement)
    return text
