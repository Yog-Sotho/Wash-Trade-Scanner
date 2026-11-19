"""
What every limit store (api/limits.py) must do. Run against the in-memory
store in tests/test_limits.py and against real Redis in
tests/integration/test_redis_limits.py, so the two cannot drift apart.
"""

import asyncio

import pytest

from config.settings import settings


def lockout_settings(monkeypatch, window: int = 60) -> None:
    """Lock from the 3rd failure: 1 s, then 2 s, 4 s, capped at 4 s."""
    monkeypatch.setattr(settings, "AUTH_MAX_FAILURES", 3)
    monkeypatch.setattr(settings, "AUTH_LOCKOUT_SECONDS", 1)
    monkeypatch.setattr(settings, "AUTH_LOCKOUT_MAX_SECONDS", 4)
    monkeypatch.setattr(settings, "AUTH_FAILURE_WINDOW_SECONDS", window)


async def requests_past_the_limit_wait_for_the_window(store) -> None:
    for _ in range(3):
        assert await store.hit("10.0.0.1", limit=3, window=0.5) == 0
    wait = await store.hit("10.0.0.1", limit=3, window=0.5)
    assert 0 < wait <= 0.5
    assert await store.hit("10.0.0.2", limit=3, window=0.5) == 0  # clients are separate
    await asyncio.sleep(wait + 0.05)
    assert await store.hit("10.0.0.1", limit=3, window=0.5) == 0


async def a_refused_request_is_not_counted(store) -> None:
    assert await store.hit("10.0.0.3", limit=1, window=0.4) == 0
    for _ in range(5):  # hammering while limited does not extend the wait
        assert await store.hit("10.0.0.3", limit=1, window=0.4) > 0
    await asyncio.sleep(0.45)
    assert await store.hit("10.0.0.3", limit=1, window=0.4) == 0


async def the_window_slides(store) -> None:
    """Old requests leave the window one by one, and a refused request does
    not occupy it: limit 2 per second, requests at 0 s, 0.6 s (allowed) and
    0.6 s (refused); at 1.1 s only the 0.6 s request is in the window."""
    assert await store.hit("10.0.0.4", limit=2, window=1.0) == 0
    await asyncio.sleep(0.6)
    assert await store.hit("10.0.0.4", limit=2, window=1.0) == 0
    assert await store.hit("10.0.0.4", limit=2, window=1.0) > 0
    await asyncio.sleep(0.5)
    assert await store.hit("10.0.0.4", limit=2, window=1.0) == 0
    # ...and that request was recorded: with 0.6 s and 1.1 s in the window,
    # the next one is refused.
    assert await store.hit("10.0.0.4", limit=2, window=1.0) > 0


async def failures_lock_out_with_a_doubling_backoff(store, monkeypatch) -> None:
    lockout_settings(monkeypatch)
    client = "10.0.1.1"
    assert [await store.auth_failure(client) for _ in range(6)] == [0, 0, 1, 2, 4, 4]
    state = await store.auth_state(client)
    assert state.failures == 6
    assert 0 < state.locked_for <= 4
    other = await store.auth_state("10.0.1.2")
    assert (other.locked_for, other.failures) == (0, 0)


async def a_reset_forgets_failures_and_lockout(store, monkeypatch) -> None:
    lockout_settings(monkeypatch)
    client = "10.0.1.3"
    for _ in range(3):
        await store.auth_failure(client)
    assert (await store.auth_state(client)).locked_for > 0
    await store.auth_reset(client)
    state = await store.auth_state(client)
    assert (state.locked_for, state.failures) == (0, 0)
    assert await store.auth_failure(client) == 0  # counting starts over


async def failures_are_forgotten_after_the_window(store, monkeypatch) -> None:
    lockout_settings(monkeypatch, window=1)
    client = "10.0.1.4"
    await store.auth_failure(client)
    await store.auth_failure(client)
    assert (await store.auth_state(client)).failures == 2
    await asyncio.sleep(1.2)
    assert (await store.auth_state(client)).failures == 0
    assert await store.auth_failure(client) == 0


CONTRACT = [
    requests_past_the_limit_wait_for_the_window,
    a_refused_request_is_not_counted,
    the_window_slides,
    failures_lock_out_with_a_doubling_backoff,
    a_reset_forgets_failures_and_lockout,
    failures_are_forgotten_after_the_window,
]


def contract_params():
    return pytest.mark.parametrize("check", CONTRACT, ids=[c.__name__ for c in CONTRACT])


async def run(check, store, monkeypatch) -> None:
    if "monkeypatch" in check.__code__.co_varnames[: check.__code__.co_argcount]:
        await check(store, monkeypatch)
    else:
        await check(store)
