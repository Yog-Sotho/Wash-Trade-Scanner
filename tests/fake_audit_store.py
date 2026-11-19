"""
An in-memory stand-in for Storage's audit-task methods, for API unit tests
that use a mocked Storage. The real methods (advisory lock, heartbeat and
TTL SQL) are tested against PostgreSQL in tests/integration.
"""

from datetime import UTC, datetime
from typing import Any

from models.schemas import AuditTask


class FakeAuditStore:
    def __init__(self) -> None:
        self.tasks: dict[str, AuditTask] = {}

    def install(self, storage: Any) -> Any:
        storage.create_audit_task = self.create_audit_task
        storage.heartbeat_audit_task = self.heartbeat_audit_task
        storage.finish_audit_task = self.finish_audit_task
        storage.get_audit_task = self.get_audit_task
        storage.reap_audit_tasks = self.reap_audit_tasks
        return storage

    async def create_audit_task(
        self, task_id, chain_id, pool_address, parameters, request_id, max_running
    ):
        running = [t for t in self.tasks.values() if t.status == "running"]
        for task in running:
            if (task.chain_id, task.pool_address) == (chain_id, pool_address):
                return "duplicate", task.id
        if len(running) >= max_running:
            return "busy", None
        now = datetime.now(UTC).replace(tzinfo=None)
        self.tasks[task_id] = AuditTask(
            id=task_id,
            status="running",
            chain_id=chain_id,
            pool_address=pool_address,
            parameters=parameters,
            request_id=request_id,
            created_at=now,
            heartbeat_at=now,
        )
        return "created", task_id

    async def heartbeat_audit_task(self, task_id):
        self.tasks[task_id].heartbeat_at = datetime.now(UTC).replace(tzinfo=None)

    async def finish_audit_task(self, task_id, status, result=None, error=None):
        task = self.tasks[task_id]
        task.status, task.result, task.error = status, result, error
        task.finished_at = datetime.now(UTC).replace(tzinfo=None)

    async def get_audit_task(self, task_id):
        return self.tasks.get(task_id)

    async def reap_audit_tasks(self, stale_after_seconds, ttl_hours):
        return 0, 0
