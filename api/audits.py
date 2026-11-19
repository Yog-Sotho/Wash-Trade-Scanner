"""
Background audits started through the API, persisted in the database
(audit_tasks) so that any API instance can report them and a restart does
not lose them.

- Bounded: at most AUDIT_MAX_CONCURRENT run at a time across all instances
  (more get 429), and a pool already being audited is not audited twice
  (409, with the running task's id).
- Expiring: finished tasks are deleted AUDIT_TASK_TTL_HOURS after they end.
- Never stuck at "running": a running task refreshes its heartbeat every
  HEARTBEAT_SECONDS; one whose heartbeat is older than STALE_AFTER_SECONDS
  belongs to an instance that went away and is marked interrupted. An
  instance shutting down marks its own running tasks interrupted.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from config.settings import settings
from core.log_config import request_id_var
from core.redaction import redact
from core.storage import Storage
from core.validators import AuditParameters
from models.schemas import AuditTask

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 30.0
STALE_AFTER_SECONDS = 120.0  # four missed heartbeats

AuditRun = Callable[[AuditParameters], Awaitable[dict[str, Any]]]


def task_view(task: AuditTask) -> dict[str, Any]:
    """The task as the API returns it."""
    view: dict[str, Any] = {
        "task_id": task.id,
        "status": task.status,
        "chain_id": task.chain_id,
        "pool_address": task.pool_address,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "finished_at": task.finished_at.isoformat() if task.finished_at else None,
    }
    if task.result is not None:
        view["result"] = task.result
    if task.error is not None:
        view["error"] = task.error
    return view


class AuditTasks:
    def __init__(
        self,
        storage: Callable[[], Storage],
        run_audit: AuditRun,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        stale_after_seconds: float = STALE_AFTER_SECONDS,
    ) -> None:
        self._storage = storage
        self._run_audit = run_audit
        self.heartbeat_seconds = heartbeat_seconds
        self.stale_after_seconds = stale_after_seconds
        self._running: dict[str, asyncio.Task[None]] = {}

    async def reap(self) -> None:
        interrupted, deleted = await self._storage().reap_audit_tasks(
            self.stale_after_seconds, settings.AUDIT_TASK_TTL_HOURS
        )
        if interrupted:
            logger.warning(f"Marked {interrupted} audit task(s) interrupted: their server stopped")
        if deleted:
            logger.info(f"Deleted {deleted} expired audit task(s)")

    async def start(self, params: AuditParameters) -> tuple[str, str | None]:
        """Start an audit unless the limits forbid it.
        Returns ("created" | "duplicate" | "busy", task id or None)."""
        await self.reap()  # frees the slots of tasks whose server went away
        task_id = uuid.uuid4().hex
        outcome, existing = await self._storage().create_audit_task(
            task_id,
            params.chain_id,
            params.pool_address.lower(),
            params.model_dump(mode="json"),
            request_id_var.get(),
            settings.AUDIT_MAX_CONCURRENT,
        )
        if outcome != "created":
            return outcome, existing
        # The task inherits this context: its logs carry the request id.
        self._running[task_id] = asyncio.create_task(self._run(task_id, params))
        return outcome, task_id

    async def get(self, task_id: str) -> AuditTask | None:
        await self.reap()
        return await self._storage().get_audit_task(task_id)

    async def _run(self, task_id: str, params: AuditParameters) -> None:
        storage = self._storage()
        heartbeat = asyncio.create_task(self._heartbeat(task_id))
        try:
            result = await self._run_audit(params)
        except asyncio.CancelledError:
            await storage.finish_audit_task(
                task_id, "interrupted", error="The server stopped before the audit finished"
            )
            raise
        except Exception as exc:
            logger.exception(f"Audit task {task_id} failed")
            await storage.finish_audit_task(task_id, "failed", error=redact(str(exc)))
        else:
            await storage.finish_audit_task(task_id, "completed", result=result)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            self._running.pop(task_id, None)

    async def _heartbeat(self, task_id: str) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            try:
                await self._storage().heartbeat_audit_task(task_id)
            except Exception:
                logger.warning(f"Heartbeat of audit task {task_id} failed", exc_info=True)

    async def shutdown(self) -> None:
        """Cancel this instance's running audits; each records itself as
        interrupted on the way out."""
        tasks = list(self._running.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
