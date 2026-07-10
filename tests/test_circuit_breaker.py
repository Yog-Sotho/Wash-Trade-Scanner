"""
Unit tests for the circuit breaker state machine.
"""

import pytest

from core.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState
from core.exceptions import CircuitBreakerOpenError


@pytest.fixture
def breaker():
    return CircuitBreaker(
        name="test",
        config=CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=0.05,
            half_open_max_calls=1,
            success_threshold=1,
        ),
    )


@pytest.mark.asyncio
async def test_successful_call_stays_closed(breaker):
    async def ok():
        return "ok"

    result = await breaker.call(ok)
    assert result == "ok"
    assert breaker.is_closed


@pytest.mark.asyncio
async def test_opens_after_failure_threshold(breaker):
    async def fail():
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(fail)

    assert breaker.is_open


@pytest.mark.asyncio
async def test_open_circuit_rejects_calls(breaker):
    async def fail():
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(fail)

    async def ok():
        return "ok"

    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(ok)


@pytest.mark.asyncio
async def test_recovers_to_closed_after_timeout(breaker):
    async def fail():
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(fail)
    assert breaker.is_open

    import asyncio

    await asyncio.sleep(0.1)

    async def ok():
        return "ok"

    result = await breaker.call(ok)
    assert result == "ok"
    assert breaker.is_closed
    assert breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_half_open_failure_reopens(breaker):
    async def fail():
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(fail)

    import asyncio

    await asyncio.sleep(0.1)

    with pytest.raises(RuntimeError):
        await breaker.call(fail)

    assert breaker.is_open
