"""
Background audit tasks persisted in PostgreSQL (roadmap 4.4): bounded across
instances, deduplicated per pool, expiring, and never left "running" by a
server that went away.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import text

from api.server import create_app
from config.settings import settings

pytestmark = pytest.mark.integration

POOL = "0x" + "b" * 40


def pool(i: int) -> str:
    return "0x" + f"{i:040x}"


async def create(storage, task_id, chain_id=1, pool_address=POOL, max_running=2):
    return await storage.create_audit_task(
        task_id, chain_id, pool_address, {"chain_id": chain_id}, "req-1", max_running
    )


async def test_running_tasks_are_bounded_and_deduplicated(pg_storage):
    assert await create(pg_storage, "a" * 32, pool_address=pool(1)) == ("created", "a" * 32)
    assert await create(pg_storage, "b" * 32, pool_address=pool(2)) == ("created", "b" * 32)
    assert await create(pg_storage, "c" * 32, pool_address=pool(3)) == ("busy", None)
    assert await create(pg_storage, "d" * 32, pool_address=pool(1)) == ("duplicate", "a" * 32)

    await pg_storage.finish_audit_task("a" * 32, "completed", result={"trades_processed": 9})
    assert await create(pg_storage, "e" * 32, pool_address=pool(3)) == ("created", "e" * 32)
    task = await pg_storage.get_audit_task("a" * 32)
    assert (task.status, task.result, task.request_id) == (
        "completed",
        {"trades_processed": 9},
        "req-1",
    )
    assert task.finished_at is not None


async def test_the_bound_holds_under_concurrent_starts(pg_storage):
    """Ten starts at once (each its own connection, as from ten instances):
    the advisory lock lets exactly two through."""
    outcomes = await asyncio.gather(
        *(create(pg_storage, f"{i:032x}", pool_address=pool(i)) for i in range(10))
    )
    assert sorted(o for o, _ in outcomes) == ["busy"] * 8 + ["created"] * 2


async def test_stale_tasks_are_interrupted_and_old_ones_deleted(pg_storage):
    await create(pg_storage, "a" * 32, pool_address=pool(1))  # its server died
    await create(pg_storage, "b" * 32, pool_address=pool(2))  # still alive
    await create(pg_storage, "c" * 32, pool_address=pool(3), max_running=3)
    await pg_storage.finish_audit_task("c" * 32, "completed", result={})
    async with await pg_storage.get_session() as session:
        await session.execute(
            text(
                "UPDATE audit_tasks SET heartbeat_at = now() - interval '10 minutes' WHERE id = :i"
            ),
            {"i": "a" * 32},
        )
        await session.execute(
            text("UPDATE audit_tasks SET finished_at = now() - interval '25 hours' WHERE id = :i"),
            {"i": "c" * 32},
        )
        await session.commit()

    assert await pg_storage.reap_audit_tasks(120, 24) == (1, 1)

    stale = await pg_storage.get_audit_task("a" * 32)
    assert stale.status == "interrupted" and "stopped" in stale.error
    assert (await pg_storage.get_audit_task("b" * 32)).status == "running"
    assert await pg_storage.get_audit_task("c" * 32) is None
    # An interrupted task frees its slot and its pool.
    assert await create(pg_storage, "d" * 32, pool_address=pool(1)) == ("created", "d" * 32)


async def test_a_heartbeat_keeps_a_task_alive(pg_storage):
    await create(pg_storage, "a" * 32)
    async with await pg_storage.get_session() as session:
        await session.execute(
            text("UPDATE audit_tasks SET heartbeat_at = now() - interval '10 minutes'")
        )
        await session.commit()
    await pg_storage.heartbeat_audit_task("a" * 32)
    assert await pg_storage.reap_audit_tasks(120, 24) == (0, 0)


async def test_instances_share_tasks_and_shutdown_interrupts_its_own(pg_storage, monkeypatch):
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "")
    release = asyncio.Event()
    started = asyncio.Event()

    async def slow_audit(_params):
        started.set()
        await release.wait()
        return {"trades_processed": 3, "first_trade": "2026-01-15T12:00:00"}

    first, second = create_app(storage=pg_storage), create_app(storage=pg_storage)
    body = {"chain_id": 1, "pool_address": POOL, "use_ml": False}
    with patch("scripts.run_audit.AuditRunner.run_audit", AsyncMock(side_effect=slow_audit)):
        async with (
            httpx.AsyncClient(transport=httpx.ASGITransport(app=first), base_url="http://a") as a,
            httpx.AsyncClient(transport=httpx.ASGITransport(app=second), base_url="http://b") as b,
        ):
            started_response = await a.post("/api/v1/audits", json=body)
            assert started_response.status_code == 202
            task_id = started_response.json()["task_id"]
            await asyncio.wait_for(started.wait(), 5)

            duplicate = await b.post("/api/v1/audits", json=body)  # the other instance
            assert duplicate.status_code == 409
            assert duplicate.json()["task_id"] == task_id
            assert (await b.get(f"/api/v1/audits/{task_id}")).json()["status"] == "running"

            release.set()
            for _ in range(100):
                status = (await b.get(f"/api/v1/audits/{task_id}")).json()
                if status["status"] != "running":
                    break
                await asyncio.sleep(0.02)
            assert status["status"] == "completed"
            assert status["result"] == {"trades_processed": 3, "first_trade": "2026-01-15T12:00:00"}
            assert status["finished_at"] is not None

        # A shutdown mid-audit records the audit as interrupted.
        release.clear()
        started.clear()
        async with first.router.lifespan_context(first):
            transport = httpx.ASGITransport(app=first)
            async with httpx.AsyncClient(transport=transport, base_url="http://a") as a:
                other = {**body, "pool_address": "0x" + "c" * 40}
                task_id = (await a.post("/api/v1/audits", json=other)).json()["task_id"]
                await asyncio.wait_for(started.wait(), 5)
        task = await pg_storage.get_audit_task(task_id)
        assert task.status == "interrupted"
        assert task.finished_at is not None


async def test_the_concurrency_limit_answers_429(pg_storage, monkeypatch):
    monkeypatch.setattr(settings, "AUDIT_MAX_CONCURRENT", 1)
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "")
    release = asyncio.Event()

    async def slow_audit(_params):
        await release.wait()
        return {}

    app = create_app(storage=pg_storage)
    with patch("scripts.run_audit.AuditRunner.run_audit", AsyncMock(side_effect=slow_audit)):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://a"
        ) as client:
            assert (
                await client.post("/api/v1/audits", json={"chain_id": 1, "pool_address": pool(1)})
            ).status_code == 202
            busy = await client.post(
                "/api/v1/audits", json={"chain_id": 1, "pool_address": pool(2)}
            )
            assert busy.status_code == 429
            assert busy.headers["Retry-After"] == "60"
            release.set()
            await app.state.audits.shutdown()
