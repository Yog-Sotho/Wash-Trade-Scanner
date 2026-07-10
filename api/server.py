"""
FastAPI application: REST endpoints for audits, trades and risk reports,
plus a websocket streaming live wash-trade detections.

Run locally with `wash-api`. The server binds to loopback by default;
binding a public interface requires API auth to be enabled (see
`run_server`), per-IP rate limiting and security headers are always on.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from contextlib import asynccontextmanager
from ipaddress import ip_address
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from api.auth import authorize_websocket, require_api_key
from api.middleware import RateLimitMiddleware, SecurityHeadersMiddleware
from config.settings import settings
from core.realtime_monitor import RealtimeMonitor
from core.reporting import compute_risk_metrics
from core.storage import Storage
from core.validators import AuditParameters, validate_address
from models.schemas import AuditRequest, SwapTradeResponse

logger = logging.getLogger(__name__)

API_VERSION = "1"
PREFIX = f"/api/v{API_VERSION}"


def _validated_pool(pool_address: str) -> str:
    try:
        validate_address(pool_address)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return pool_address.lower()


def create_app(storage: Storage | None = None) -> FastAPI:
    """Application factory. Pass `storage` to reuse an existing instance
    (tests inject a mock here); otherwise one is created on startup."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        if app.state.storage is None:
            app.state.storage = Storage()
            await app.state.storage.initialize()
            app.state.owns_storage = True
        yield
        if app.state.owns_storage:
            await app.state.storage.close()

    app = FastAPI(
        title="Wash Trade Scanner API",
        version="1.0.0",
        docs_url="/docs" if settings.API_DOCS_ENABLED else None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.storage = storage
    app.state.owns_storage = False
    app.state.audit_tasks = {}

    # Order matters: middleware added last runs first (outermost), so the
    # security headers wrap even rate-limited (429) responses.
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)
    if settings.cors_origins_list:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins_list,
            allow_methods=["GET", "POST"],
            allow_headers=["X-API-Key", "Content-Type"],
        )

    def get_storage() -> Storage:
        if app.state.storage is None:
            raise HTTPException(status_code=503, detail="Storage not initialized")
        return app.state.storage  # type: ignore[no-any-return]

    @app.get("/health")
    async def health() -> dict[str, Any]:
        """Liveness + database connectivity. Unauthenticated by design."""
        storage = get_storage()
        db_ok = await storage.health_check()
        return {"status": "ok" if db_ok else "degraded", "database": db_ok}

    @app.get(
        f"{PREFIX}/pools/{{chain_id}}/{{pool_address}}/trades",
        dependencies=[Depends(require_api_key)],
    )
    async def pool_trades(
        chain_id: int,
        pool_address: str,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        wash_only: bool = Query(False),
    ) -> dict[str, Any]:
        """Paginated trades for a pool, optionally only flagged ones."""
        pool = _validated_pool(pool_address)
        storage = get_storage()
        trades = await storage.get_pool_trades(chain_id, pool, limit=limit, offset=offset)
        if wash_only:
            trades = [t for t in trades if t.is_wash_trade]
        return {
            "chain_id": chain_id,
            "pool_address": pool,
            "count": len(trades),
            "trades": [SwapTradeResponse.model_validate(t).model_dump(mode="json") for t in trades],
        }

    @app.get(
        f"{PREFIX}/pools/{{chain_id}}/{{pool_address}}/report",
        dependencies=[Depends(require_api_key)],
    )
    async def pool_report(chain_id: int, pool_address: str) -> dict[str, Any]:
        """Risk metrics computed over all stored trades of a pool."""
        pool = _validated_pool(pool_address)
        storage = get_storage()
        trades = await storage.get_pool_trades(chain_id, pool)
        metrics = compute_risk_metrics(trades)
        return {"chain_id": chain_id, "pool_address": pool, **metrics}

    async def _run_audit_task(task_id: str, request: AuditRequest) -> None:
        # Imported lazily: scripts.run_audit configures logging at import time
        # and pulls in the whole pipeline, which API-only deployments may not
        # need until an audit is actually requested.
        from scripts.run_audit import AuditRunner

        tasks: dict[str, dict[str, Any]] = app.state.audit_tasks
        try:
            runner = AuditRunner()
            runner.storage = get_storage()
            params = AuditParameters(
                chain_id=request.chain_id,
                pool_address=request.pool_address,
                start_block=request.start_block,
                end_block=request.end_block,
                use_ml=request.use_ml,
                use_heuristics=request.use_heuristics,
            )
            result = await runner.run_audit(params)
            tasks[task_id] = {"status": "completed", "result": result}
        except Exception as exc:
            logger.exception(f"Audit task {task_id} failed")
            tasks[task_id] = {"status": "failed", "error": str(exc)}

    @app.post(
        f"{PREFIX}/audits",
        status_code=202,
        dependencies=[Depends(require_api_key)],
    )
    async def start_audit(request: AuditRequest) -> dict[str, str]:
        """Kick off a full audit in the background; poll the returned task id."""
        _validated_pool(request.pool_address)
        task_id = uuid.uuid4().hex
        app.state.audit_tasks[task_id] = {"status": "running"}
        task = asyncio.create_task(_run_audit_task(task_id, request))
        # Keep a reference so the task isn't garbage-collected mid-flight.
        app.state.audit_tasks[task_id]["task"] = task
        return {"task_id": task_id, "status": "running"}

    @app.get(
        f"{PREFIX}/audits/{{task_id}}",
        dependencies=[Depends(require_api_key)],
    )
    async def audit_status(task_id: str) -> dict[str, Any]:
        entry = app.state.audit_tasks.get(task_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="Unknown audit task")
        return {k: v for k, v in entry.items() if k != "task"}

    @app.websocket(f"{PREFIX}/ws/monitor/{{chain_id}}/{{pool_address}}")
    async def ws_monitor(websocket: WebSocket, chain_id: int, pool_address: str) -> None:
        """Stream live detection events for a pool.

        Events: {"type": "status"|"alert"|"stats"|"error", ...payload}.
        Auth uses the same X-API-Key header as HTTP routes.
        """
        if not await authorize_websocket(websocket):
            return
        try:
            validate_address(pool_address)
        except ValueError:
            await websocket.close(code=4422, reason="Invalid pool address")
            return

        await websocket.accept()
        monitor = RealtimeMonitor(get_storage(), chain_id, pool_address.lower())
        try:
            async for event in monitor.stream():
                await websocket.send_json({"type": event.type, "data": event.payload})
        except WebSocketDisconnect:
            logger.info(f"Monitor client disconnected from pool {pool_address}")
        except Exception as exc:
            logger.exception(f"Websocket monitor failed: {exc}")
            # Best-effort notification; the socket may already be gone.
            try:
                await websocket.send_json(
                    {"type": "error", "data": {"reason": str(exc), "recoverable": False}}
                )
                await websocket.close(code=1011)
            except Exception:  # nosec B110
                pass
        finally:
            monitor.stop()

    return app


def _is_loopback(host: str) -> bool:
    if host in ("localhost",):
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def run_server() -> None:
    """Start uvicorn with the hardening policy enforced.

    Binding a non-loopback interface without auth enabled (or with auth
    enabled but no keys configured) is refused rather than warned about.
    """
    if not _is_loopback(settings.API_HOST):
        if not settings.API_AUTH_ENABLED:
            logger.error(
                "Refusing to bind %s without authentication. "
                "Set API_AUTH_ENABLED=true and configure API_KEY_HASHES "
                "(generate a key with `wash-genkey`).",
                settings.API_HOST,
            )
            sys.exit(1)
        if not settings.api_key_hash_set:
            logger.error(
                "API_AUTH_ENABLED is true but API_KEY_HASHES is empty; "
                "no request could ever authenticate. Generate a key with `wash-genkey`."
            )
            sys.exit(1)

    import uvicorn

    uvicorn.run(
        "api.server:app_factory",
        factory=True,
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level=settings.LOG_LEVEL.lower(),
    )


def app_factory() -> FastAPI:
    """Uvicorn factory entry point."""
    return create_app()


def cli() -> None:
    """Synchronous entry point for the `wash-api` console script."""
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run_server()
