"""
Circuit Breaker + Backpressure 테스트

Circuit Breaker 단위 테스트는 브로커 연결 없이 실행 가능.
API 엔드포인트 테스트는 FastAPI TestClient 사용.
"""

import asyncio
import time

import pytest

from app.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    circuit_breakers,
)
from app.resilience.backpressure import BackpressureGuard


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Circuit Breaker 단위 테스트 (브로커 불필요)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_cb_initial_state_is_closed():
    cb = CircuitBreaker(name="unit-test-init")
    assert cb.state == CircuitState.CLOSED
    assert cb._failure_count == 0


@pytest.mark.asyncio
async def test_cb_opens_after_threshold():
    cb = CircuitBreaker(name="unit-test-open", failure_threshold=3, recovery_timeout=60)
    for _ in range(3):
        await cb._on_failure()
    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_cb_rejects_when_open():
    cb = CircuitBreaker(name="unit-test-reject", failure_threshold=1, recovery_timeout=60)
    await cb._on_failure()
    assert cb.state == CircuitState.OPEN

    with pytest.raises(CircuitOpenError) as exc_info:
        await cb.call(asyncio.sleep, 0)

    assert exc_info.value.broker == "unit-test-reject"


@pytest.mark.asyncio
async def test_cb_transitions_to_half_open_after_timeout():
    cb = CircuitBreaker(name="unit-test-half", failure_threshold=1, recovery_timeout=0.05)
    await cb._on_failure()
    assert cb.state == CircuitState.OPEN

    await asyncio.sleep(0.1)
    assert cb.state == CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_cb_recovers_to_closed_on_success():
    cb = CircuitBreaker(name="unit-test-recover", failure_threshold=1, recovery_timeout=0.05)
    await cb._on_failure()
    await asyncio.sleep(0.1)
    assert cb.state == CircuitState.HALF_OPEN

    async def ok():
        return "ok"

    result = await cb.call(ok)
    assert result == "ok"
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_cb_stays_open_on_half_open_failure():
    cb = CircuitBreaker(name="unit-test-halfopen-fail", failure_threshold=1, recovery_timeout=0.05)
    await cb._on_failure()
    await asyncio.sleep(0.1)

    async def bad():
        raise ValueError("still broken")

    with pytest.raises(ValueError):
        await cb.call(bad)

    assert cb.state == CircuitState.OPEN


def test_cb_status_dict_keys():
    cb = CircuitBreaker(name="unit-test-status")
    status = cb.status()
    assert set(status.keys()) == {"broker", "state", "failure_count", "threshold", "retry_after_seconds"}


def test_module_level_circuit_breakers_exist():
    assert "redis" in circuit_breakers
    assert "rabbitmq" in circuit_breakers
    assert "kafka" in circuit_breakers
    for cb in circuit_breakers.values():
        assert cb.state == CircuitState.CLOSED


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Backpressure 단위 테스트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_bp_normal_flow():
    guard = BackpressureGuard("bp-test-normal", max_concurrent=2, max_waiting=5)
    async with guard:
        assert guard._active == 1
    assert guard._active == 0


@pytest.mark.asyncio
async def test_bp_overload_raises_503():
    from fastapi import HTTPException
    # max_waiting=0 → _waiting(0) >= max_waiting(0) 이 True
    # → 대기 큐 없음, 모든 요청 즉시 503
    guard = BackpressureGuard("bp-test-overload", max_concurrent=5, max_waiting=0)

    with pytest.raises(HTTPException) as exc_info:
        async with guard:
            pass

    assert exc_info.value.status_code == 503
    assert "backpressure" in exc_info.value.detail["error"]


@pytest.mark.asyncio
async def test_bp_timeout_raises_503():
    from fastapi import HTTPException
    guard = BackpressureGuard("bp-test-timeout", max_concurrent=1, max_waiting=10, timeout_seconds=0.05)

    async with guard:
        with pytest.raises(HTTPException) as exc_info:
            async with guard:
                pass

    assert exc_info.value.status_code == 503
    assert "timeout" in exc_info.value.detail["error"]


def test_bp_status_dict():
    guard = BackpressureGuard("bp-test-status", max_concurrent=50, max_waiting=100)
    status = guard.status()
    assert status["broker"] == "bp-test-status"
    assert status["max_concurrent"] == 50
    assert status["overloaded"] is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Resilience API 엔드포인트 테스트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_circuit_breakers_list(client):
    resp = await client.get("/resilience/circuit-breakers")
    assert resp.status_code == 200
    data = resp.json()
    assert "redis" in data
    assert "rabbitmq" in data
    assert "kafka" in data


@pytest.mark.asyncio
async def test_circuit_breaker_single(client):
    resp = await client.get("/resilience/circuit-breakers/redis")
    assert resp.status_code == 200
    data = resp.json()
    assert data["broker"] == "redis"
    assert data["state"] in ("closed", "open", "half_open")


@pytest.mark.asyncio
async def test_circuit_breaker_unknown_returns_404(client):
    resp = await client.get("/resilience/circuit-breakers/unknown-broker")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_circuit_breaker_reset(client):
    resp = await client.post("/resilience/circuit-breakers/redis/reset")
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "closed"


@pytest.mark.asyncio
async def test_backpressure_status(client):
    resp = await client.get("/resilience/backpressure")
    assert resp.status_code == 200
    data = resp.json()
    assert "redis" in data
    assert "kafka" in data
