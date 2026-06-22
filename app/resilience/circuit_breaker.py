"""
Circuit Breaker 패턴
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
브로커 장애 시 연쇄 실패(Cascading Failure)를 방지.

상태 전이:
  CLOSED ─(threshold 초과)→ OPEN ─(timeout 경과)→ HALF_OPEN ─(성공)→ CLOSED
                                                              └─(실패)→ OPEN

사용법:
  cb = circuit_breakers["redis"]
  result = await cb.call(redis_broker.publish, "channel", msg)

  # 또는 데코레이터:
  @with_circuit_breaker("kafka")
  async def my_func():
      ...
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable

from prometheus_client import Counter, Gauge

# ── Prometheus 메트릭 ──
CB_STATE = Gauge(
    "circuit_breaker_state",
    "Circuit Breaker 상태 (0=CLOSED, 1=HALF_OPEN, 2=OPEN)",
    labelnames=["broker"],
)
CB_TRANSITIONS = Counter(
    "circuit_breaker_transitions_total",
    "Circuit Breaker 상태 전환 횟수",
    labelnames=["broker", "from_state", "to_state"],
)
CB_REJECTIONS = Counter(
    "circuit_breaker_rejections_total",
    "Circuit Breaker가 차단한 요청 수",
    labelnames=["broker"],
)


class CircuitState(str, Enum):
    CLOSED = "closed"       # 정상
    OPEN = "open"           # 차단 중
    HALF_OPEN = "half_open" # 복구 시도 중


class CircuitOpenError(Exception):
    """Circuit이 OPEN 상태일 때 발생"""
    def __init__(self, broker: str, retry_after: float):
        self.broker = broker
        self.retry_after = retry_after
        super().__init__(
            f"Circuit breaker OPEN for '{broker}'. "
            f"Retry after {retry_after:.1f}s"
        )


@dataclass
class CircuitBreaker:
    """
    브로커별 Circuit Breaker.

    failure_threshold: 이 횟수 실패 시 OPEN 전환
    recovery_timeout:  OPEN 상태 유지 시간 (초) — 이후 HALF_OPEN 시도
    half_open_limit:   HALF_OPEN에서 허용할 최대 동시 요청 수
    """
    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_limit: int = 2

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _half_open_attempts: int = field(default=0, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self):
        CB_STATE.labels(broker=self.name).set(0)

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            elapsed = time.time() - self._last_failure_time
            if elapsed >= self.recovery_timeout:
                return CircuitState.HALF_OPEN
        return self._state

    @property
    def retry_after(self) -> float:
        if self._state == CircuitState.OPEN:
            remaining = self.recovery_timeout - (time.time() - self._last_failure_time)
            return max(0.0, remaining)
        return 0.0

    def status(self) -> dict:
        return {
            "broker": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "threshold": self.failure_threshold,
            "retry_after_seconds": round(self.retry_after, 1),
        }

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        current_state = self.state

        if current_state == CircuitState.OPEN:
            CB_REJECTIONS.labels(broker=self.name).inc()
            raise CircuitOpenError(self.name, self.retry_after)

        if current_state == CircuitState.HALF_OPEN:
            async with self._lock:
                if self._half_open_attempts >= self.half_open_limit:
                    CB_REJECTIONS.labels(broker=self.name).inc()
                    raise CircuitOpenError(self.name, self.retry_after)
                self._half_open_attempts += 1

        try:
            result = await func(*args, **kwargs)
            await self._on_success(current_state)
            return result
        except CircuitOpenError:
            raise
        except Exception:
            await self._on_failure()
            raise

    async def _on_success(self, prev_state: CircuitState) -> None:
        async with self._lock:
            if self._state != CircuitState.CLOSED:
                self._transition(CircuitState.CLOSED)
            self._failure_count = 0
            self._half_open_attempts = 0

    async def _on_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if (self._state == CircuitState.CLOSED
                    and self._failure_count >= self.failure_threshold):
                self._transition(CircuitState.OPEN)
            elif self._state == CircuitState.HALF_OPEN:
                self._transition(CircuitState.OPEN)
                self._half_open_attempts = 0

    def _transition(self, new_state: CircuitState) -> None:
        old_state = self._state
        self._state = new_state
        state_num = {"closed": 0, "half_open": 1, "open": 2}[new_state.value]
        CB_STATE.labels(broker=self.name).set(state_num)
        CB_TRANSITIONS.labels(
            broker=self.name,
            from_state=old_state.value,
            to_state=new_state.value,
        ).inc()


def with_circuit_breaker(broker_name: str):
    """Circuit Breaker 데코레이터 — async 함수에 적용"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cb = circuit_breakers[broker_name]
            return await cb.call(func, *args, **kwargs)
        return wrapper
    return decorator


# 브로커별 Circuit Breaker 인스턴스
circuit_breakers: dict[str, CircuitBreaker] = {
    "redis":    CircuitBreaker(name="redis",    failure_threshold=5, recovery_timeout=30),
    "rabbitmq": CircuitBreaker(name="rabbitmq", failure_threshold=5, recovery_timeout=30),
    "kafka":    CircuitBreaker(name="kafka",    failure_threshold=3, recovery_timeout=60),
}
