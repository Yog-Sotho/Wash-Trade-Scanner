"""
The JSON-RPC transport every chain call goes through, instrumented.

Each request -- including each retry web3 makes -- is counted and timed
(wash_rpc_requests_total, wash_rpc_request_duration_seconds) and, when
tracing is on, becomes a client span named after the JSON-RPC method.

The endpoint URL is never recorded: hosted endpoints carry API keys in it.
For the same reason a failed request's span gets only the exception type:
aiohttp's error messages include the request URL.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from web3 import AsyncHTTPProvider
from web3.types import RPCEndpoint, RPCResponse

from core import metrics

_tracer = trace.get_tracer(__name__)


class InstrumentedHTTPProvider(AsyncHTTPProvider):
    """AsyncHTTPProvider that records metrics and spans for one chain."""

    def __init__(self, endpoint_uri: str, chain_id: int, **kwargs: Any) -> None:
        super().__init__(endpoint_uri, **kwargs)
        self.chain_id = chain_id

    async def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        chain = str(self.chain_id)
        outcome = "transport_error"
        start = time.perf_counter()
        with _tracer.start_as_current_span(
            str(method),
            kind=SpanKind.CLIENT,
            attributes={
                "rpc.system": "jsonrpc",
                "rpc.method": str(method),
                "wash.chain_id": self.chain_id,
            },
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                response = await super().make_request(method, params)
                error = response.get("error")
                if error:
                    outcome = "rpc_error"
                    code = error.get("code") if isinstance(error, dict) else None
                    if isinstance(code, int):
                        span.set_attribute("rpc.jsonrpc.error_code", code)
                    span.set_status(StatusCode.ERROR, "JSON-RPC error")
                else:
                    outcome = "ok"
                return response
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except BaseException as exc:
                span.set_status(StatusCode.ERROR, type(exc).__name__)
                raise
            finally:
                metrics.RPC_REQUESTS.labels(chain, str(method), outcome).inc()
                metrics.RPC_DURATION.labels(chain, str(method)).observe(time.perf_counter() - start)
