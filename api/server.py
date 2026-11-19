"""
FastAPI application: REST endpoints for audits, trades and risk reports,
plus a websocket streaming live wash-trade detections.

Run locally with `wash-api`. The server binds to loopback by default;
binding a public interface requires API auth to be enabled (see
`run_server`), per-IP rate limiting and security headers are always on.
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from ipaddress import ip_address
from pathlib import Path
from typing import Any

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from api.audits import AuditTasks, task_view
from api.auth import (
    SESSION_COOKIE,
    authorize_websocket,
    check_credentials,
    create_session_token,
    locked_out,
    require_api_key,
)
from api.health import ReadinessChecker
from api.limits import Limits, client_ip
from api.middleware import (
    REQUEST_ID_HEADER,
    RateLimitMiddleware,
    RequestContextMiddleware,
    SecurityHeadersMiddleware,
)
from config.chains import get_chain_config
from config.settings import settings
from core.alerts import AlertDispatcher, sinks_from_settings
from core.log_config import configure_logging
from core.realtime_monitor import RealtimeMonitor
from core.redaction import redact
from core.reporting import compute_risk_metrics
from core.storage import Storage
from core.validators import AuditParameters, validate_address
from core.watch import PoolWatcher
from models.schemas import AuditRequest, SwapTradeResponse

logger = logging.getLogger(__name__)

API_VERSION = "1"
PREFIX = f"/api/v{API_VERSION}"
STATIC_DIR = Path(__file__).parent / "static"


class LoginRequest(BaseModel):
    api_key: str = ""


def _validated_pool(pool_address: str) -> str:
    try:
        validate_address(pool_address)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return pool_address.lower()


def create_app(storage: Storage | None = None) -> FastAPI:
    """Application factory. Pass `storage` to reuse an existing instance
    (tests inject a mock here); otherwise one is created on startup."""
    # An unknown chain in HEALTH_RPC_CHAIN_IDS or MONITOR_POOLS, or a
    # half-configured alert sink, is a configuration error: fail at startup.
    for chain_id in settings.health_rpc_chain_id_list:
        get_chain_config(chain_id)
    for chain_id, _pool in settings.monitor_pool_list:
        get_chain_config(chain_id)
    sinks = sinks_from_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        if app.state.storage is None:
            app.state.storage = Storage()
            await app.state.storage.initialize()
            app.state.owns_storage = True
        # Background monitors of MONITOR_POOLS and the sinks they alert.
        dispatcher = AlertDispatcher(sinks)
        watcher = PoolWatcher(app.state.storage, settings.monitor_pool_list, dispatcher)
        if settings.monitor_pool_list and not sinks:
            logger.warning("MONITOR_POOLS is set but no alert sink is: alerts are only logged")
        await dispatcher.start()
        await watcher.start()
        app.state.watcher = watcher
        yield
        await audits.shutdown()  # running audits record themselves interrupted
        await watcher.stop()  # no new alerts
        await dispatcher.stop()  # then deliver what is queued
        await app.state.limits.close()
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
    # Rate limits and auth lockouts: per process, or shared through Redis.
    limits = Limits.from_settings()
    app.state.limits = limits

    # Order matters: middleware added last runs first (outermost), so the
    # security headers wrap even rate-limited (429) responses, and the request
    # context (id, access log, metrics) wraps everything, CORS preflights too.
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)
    if settings.cors_origins_list:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins_list,
            allow_methods=["GET", "POST"],
            allow_headers=["X-API-Key", "Content-Type", REQUEST_ID_HEADER],
            expose_headers=[REQUEST_ID_HEADER],
        )
    app.add_middleware(RequestContextMiddleware)

    def get_storage() -> Storage:
        if app.state.storage is None:
            raise HTTPException(status_code=503, detail="Storage not initialized")
        return app.state.storage  # type: ignore[no-any-return]

    async def database_answers() -> bool:
        storage: Storage | None = app.state.storage
        return storage is not None and await storage.health_check()

    # A shared limit store is a dependency: without it, limits are not enforced.
    readiness = ReadinessChecker(
        database_answers, {"rate_limit_store": limits.ping} if limits.shared else None
    )

    # Probes are unauthenticated by design (orchestrators send no API key) and
    # say nothing beyond up/down and, for readiness, which check failed.
    @app.get("/health/live")
    async def health_live() -> dict[str, str]:
        """Liveness: the process serves requests. Checks no dependency, so a
        database or RPC outage never gets a working process restarted."""
        return {"status": "alive"}

    @app.get("/health/ready")
    async def health_ready(response: Response) -> dict[str, Any]:
        """Readiness: 200 when the database and each HEALTH_RPC_CHAIN_IDS
        endpoint answer (see api/health.py), else 503 with the failed checks."""
        ready, checks = await readiness.check()
        response.status_code = 200 if ready else 503
        return {"status": "ready" if ready else "not_ready", "checks": checks}

    @app.get("/health")
    async def health() -> dict[str, Any]:
        """Deprecated: use /health/live or /health/ready. Always 200 while the
        process runs; the body reports database connectivity."""
        storage = get_storage()
        db_ok = await storage.health_check()
        return {"status": "ok" if db_ok else "degraded", "database": db_ok}

    @app.get("/metrics", dependencies=[Depends(require_api_key)], include_in_schema=False)
    async def prometheus_metrics() -> Response:
        """Prometheus metrics (docs/observability.md). Authenticated like the
        API: give the scraper the key in an X-API-Key header."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

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

    async def run_audit(params: AuditParameters) -> dict[str, Any]:
        # Imported lazily: scripts.run_audit pulls in the whole pipeline, which
        # API-only deployments may not need until an audit is requested.
        from scripts.run_audit import AuditRunner

        runner = AuditRunner()
        runner.storage = get_storage()
        return await runner.run_audit(params)

    # Background audits (persisted, bounded, expiring): api/audits.py.
    audits = AuditTasks(get_storage, run_audit)
    app.state.audits = audits

    @app.post(
        f"{PREFIX}/audits",
        status_code=202,
        dependencies=[Depends(require_api_key)],
        responses={409: {"description": "This pool is already being audited"}},
    )
    async def start_audit(request: AuditRequest) -> Any:
        """Start a full audit in the background; poll the returned task id.

        429 when AUDIT_MAX_CONCURRENT audits are running; 409 (with the
        running task's id) when this pool is already being audited.
        """
        _validated_pool(request.pool_address)
        params = AuditParameters(
            chain_id=request.chain_id,
            pool_address=request.pool_address,
            start_block=request.start_block,
            end_block=request.end_block,
            use_ml=request.use_ml,
            use_heuristics=request.use_heuristics,
        )
        outcome, task_id = await audits.start(params)
        if outcome == "busy":
            raise HTTPException(
                status_code=429,
                detail=f"{settings.AUDIT_MAX_CONCURRENT} audit(s) already running; retry later",
                headers={"Retry-After": "60"},
            )
        if outcome == "duplicate":
            return JSONResponse(
                status_code=409,
                content={"detail": "This pool is already being audited", "task_id": task_id},
            )
        return {"task_id": task_id, "status": "running"}

    @app.get(
        f"{PREFIX}/audits/{{task_id}}",
        dependencies=[Depends(require_api_key)],
    )
    async def audit_status(task_id: str) -> dict[str, Any]:
        """A task's status and, once finished, its result or error. Finished
        tasks are kept for AUDIT_TASK_TTL_HOURS."""
        task = await audits.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Unknown or expired audit task")
        return task_view(task)

    @app.get(f"{PREFIX}/stats/overview", dependencies=[Depends(require_api_key)])
    async def stats_overview() -> dict[str, Any]:
        """Dashboard aggregates: totals, per-method/per-chain breakdowns,
        top wash pools and recent audit runs."""
        storage = get_storage()
        stats = await storage.get_global_stats()
        stats["top_wash_pools"] = await storage.get_top_wash_pools()
        stats["recent_audits"] = [
            {
                "id": log.id,
                "chain_id": log.chain_id,
                "pool_address": log.pool_address,
                "trades_processed": log.trades_processed,
                "wash_trades_detected": log.wash_trades_detected,
                "duration_seconds": log.detection_duration_seconds,
                "created_at": log.created_at.isoformat() if log.created_at else None,
                "results_summary": log.results_summary,
            }
            for log in await storage.get_recent_audit_logs()
        ]
        return stats

    # ------------------------------------------------------------- panel

    @app.post("/panel/login")
    async def panel_login(
        body: LoginRequest, request: Request, response: Response
    ) -> dict[str, bool]:
        """Exchange an API key for an HttpOnly session cookie.

        With auth disabled (local mode) the login always succeeds so the
        panel works out of the box on loopback. Wrong keys are throttled like
        everywhere else (api/limits.py): a locked-out client gets 429.
        """
        ok, locked = await check_credentials(limits, client_ip(request), body.api_key, None)
        if locked:
            raise locked_out(locked)
        if not ok:
            raise HTTPException(status_code=401, detail="Invalid API key")
        response.set_cookie(
            SESSION_COOKIE,
            create_session_token(),
            max_age=settings.PANEL_SESSION_TTL_MINUTES * 60,
            httponly=True,
            samesite="strict",
            secure=settings.API_HSTS_ENABLED,
            path="/",
        )
        return {"authenticated": True}

    @app.post("/panel/logout")
    async def panel_logout(response: Response) -> dict[str, bool]:
        response.delete_cookie(SESSION_COOKIE, path="/")
        return {"authenticated": False}

    @app.get("/panel/session", dependencies=[Depends(require_api_key)])
    async def panel_session() -> dict[str, bool]:
        """Probe used by the panel to decide whether to show the login view.

        Returns 401 (handled client-side) when the caller has no valid
        credential; succeeding means the current cookie/key is accepted.
        """
        return {"authenticated": True, "auth_required": settings.API_AUTH_ENABLED}

    if settings.PANEL_ENABLED and STATIC_DIR.is_dir():

        @app.get("/panel", include_in_schema=False)
        async def panel_index() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")

        app.mount("/panel/static", StaticFiles(directory=STATIC_DIR), name="panel-static")

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
                    {"type": "error", "data": {"reason": redact(str(exc)), "recoverable": False}}
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
        # Keep the logging configure_logging() set up (uvicorn's own config
        # would replace it), and log requests from RequestContextMiddleware,
        # which knows the request id, instead of uvicorn's access logger.
        log_config=None,
        access_log=False,
    )


def app_factory() -> FastAPI:
    """Uvicorn factory entry point."""
    return create_app()


def cli() -> None:
    """Synchronous entry point for the `wash-api` console script."""
    configure_logging()
    run_server()
