"""
Circuit Breaker + Backpressure 상태 조회 API
"""

from fastapi import APIRouter, HTTPException

from app.resilience import backpressure_guards, circuit_breakers

router = APIRouter(prefix="/resilience", tags=["Resilience"])


@router.get("/circuit-breakers")
async def get_circuit_breakers():
    """모든 Circuit Breaker 상태 조회"""
    return {
        name: cb.status()
        for name, cb in circuit_breakers.items()
    }


@router.get("/circuit-breakers/{broker}")
async def get_circuit_breaker(broker: str):
    """특정 브로커 Circuit Breaker 상태"""
    if broker not in circuit_breakers:
        raise HTTPException(status_code=404, detail=f"Unknown broker: {broker}. Available: {list(circuit_breakers)}")
    return circuit_breakers[broker].status()


@router.post("/circuit-breakers/{broker}/reset")
async def reset_circuit_breaker(broker: str):
    """Circuit Breaker 강제 리셋 (OPEN → CLOSED) — 운영 도구"""
    if broker not in circuit_breakers:
        raise HTTPException(status_code=404, detail=f"Unknown broker: {broker}")
    cb = circuit_breakers[broker]
    from app.resilience.circuit_breaker import CircuitState
    cb._state = CircuitState.CLOSED
    cb._failure_count = 0
    cb._half_open_attempts = 0
    return {"status": "reset", "broker": broker, "state": cb.state.value}


@router.get("/backpressure")
async def get_backpressure():
    """모든 Backpressure 가드 상태 조회"""
    return {
        name: guard.status()
        for name, guard in backpressure_guards.items()
    }
