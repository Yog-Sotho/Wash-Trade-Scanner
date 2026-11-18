"""
Logging for every entry point: plain text for a terminal, one JSON object per
line for log collectors (LOG_FORMAT), with the current request id and trace
ids on every record. Configured RPC URLs are redacted from every line
(core/redaction.py): they carry API keys, and error messages repeat them.

The request id lives in a context variable set by the API's request
middleware. Context variables follow asyncio tasks, so the logs of an audit
started by a request carry that request's id even after the response is sent.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import IO, Any

from opentelemetry import trace

from config.settings import settings
from core.redaction import redact

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

TEXT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] %(message)s"

# Attributes every LogRecord has; anything else on a record came from
# `extra=` and is emitted as a field of its own in JSON. uvicorn adds
# color_message, a copy of the message with terminal colour codes.
_RECORD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message",
    "asctime",
    "taskName",
    "request_id",
    "trace_id",
    "span_id",
    "color_message",
}


class ContextFilter(logging.Filter):
    """Adds request_id, and trace_id/span_id while a trace span is active."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get() or "-"
        context = trace.get_current_span().get_span_context()
        if context.is_valid:
            record.trace_id = format(context.trace_id, "032x")
            record.span_id = format(context.span_id, "016x")
        return True


class TextFormatter(logging.Formatter):
    """The text format, with RPC URLs redacted."""

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


class JsonFormatter(logging.Formatter):
    """One JSON object per record: timestamp, level, logger, message, the
    context fields, any `extra=` fields, and the traceback if there is one."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = getattr(record, "request_id", "-")
        if request_id != "-":
            entry["request_id"] = request_id
        for key in ("trace_id", "span_id"):
            if hasattr(record, key):
                entry[key] = getattr(record, key)
        for key, value in record.__dict__.items():
            if key not in _RECORD_ATTRS and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            entry["stack"] = self.formatStack(record.stack_info)
        return redact(json.dumps(entry, default=str))


def configure_logging(
    level: str | None = None, fmt: str | None = None, stream: IO[str] | None = None
) -> logging.Handler:
    """Send all logs to `stream` (stderr) at LOG_LEVEL in LOG_FORMAT.

    Safe to call more than once: it replaces only the handler it installed.
    Library loggers (uvicorn included, when started with log_config=None)
    propagate to the root logger and so share the format.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "_wash_handler", False):
            root.removeHandler(handler)

    handler = logging.StreamHandler(stream or sys.stderr)
    handler._wash_handler = True  # type: ignore[attr-defined]
    handler.addFilter(ContextFilter())
    if (fmt or settings.LOG_FORMAT) == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(TextFormatter(TEXT_FORMAT))
    root.addHandler(handler)
    root.setLevel(level or settings.LOG_LEVEL)
    return handler
