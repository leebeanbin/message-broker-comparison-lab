"""
Backpressure 패턴
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
처리 속도보다 요청이 빠를 때 스스로 속도를 조절.

전략:
  - Semaphore: 동시 처리 가능한 최대 요청 수 제한
  - Waiting Queue: 대기 중인 요청이 임계값 초과 시 즉시 503 반환
  - Timeout: 슬롯 대기 최대 시간 초과 시 503 반환

FastAPI 의존성으로 사용:
  @router.post("/heavy")
  async def heavy(bp: BackpressureGuard = Depends(backpressure_guards["kafka"])):
      async with bp:
          ...

  # 또는 직접 컨텍스트 매니저:
  async with backpressure_guards["redis"]:
      await redis_broker.publish(...)
"""

import asyncio
import time
from dataclasses import dataclass, field

from fastapi import HTTPException
from prometheus_client import Gauge, Histogram

# ── Prometheus 메트릭 ──
BP_ACTIVE = Gauge(
    "backpressure_active_requests",
    "현재 처리 중인 요청 수",
    labelnames=["broker"],
)
BP_WAITING = Gauge(
    "backpressure_waiting_requests",
    "슬롯 대기 중인 요청 수",
    labelnames=["broker"],
)
BP_WAIT_LATENCY = Histogram(
    "backpressure_wait_seconds",
    "슬롯 획득까지 대기 시간 (초)",
    labelnames=["broker"],
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)


@dataclass
class BackpressureGuard:
    """
    Semaphore 기반 Backpressure 가드.

    max_concurrent:  동시 처리 최대 요청 수
    max_waiting:     슬롯 대기 허용 최대 수 (초과 시 즉시 503)
    timeout_seconds: 슬롯 대기 최대 시간
    """
    broker_name: str
    max_concurrent: int = 50
    max_waiting: int = 100
    timeout_seconds: float = 5.0

    _semaphore: asyncio.Semaphore = field(init=False)
    _active: int = field(default=0, init=False)
    _waiting: int = field(default=0, init=False)

    def __post_init__(self):
        self._semaphore = asyncio.Semaphore(self.max_concurrent)

    @property
    def is_overloaded(self) -> bool:
        return self._waiting >= self.max_waiting

    def status(self) -> dict:
        return {
            "broker": self.broker_name,
            "active": self._active,
            "waiting": self._waiting,
            "max_concurrent": self.max_concurrent,
            "max_waiting": self.max_waiting,
            "overloaded": self.is_overloaded,
        }

    async def __aenter__(self) -> "BackpressureGuard":
        if self.is_overloaded:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "backpressure",
                    "broker": self.broker_name,
                    "message": f"Too many requests. Active={self._active}, Waiting={self._waiting}",
                    "retry_after_seconds": 2,
                },
            )

        self._waiting += 1
        BP_WAITING.labels(broker=self.broker_name).set(self._waiting)
        wait_start = time.perf_counter()

        try:
            await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError:
            self._waiting -= 1
            BP_WAITING.labels(broker=self.broker_name).set(self._waiting)
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "backpressure_timeout",
                    "broker": self.broker_name,
                    "message": f"Timed out waiting for slot after {self.timeout_seconds}s",
                    "retry_after_seconds": 1,
                },
            )
        finally:
            wait_ms = time.perf_counter() - wait_start
            BP_WAIT_LATENCY.labels(broker=self.broker_name).observe(wait_ms)

        self._waiting -= 1
        self._active += 1
        BP_WAITING.labels(broker=self.broker_name).set(self._waiting)
        BP_ACTIVE.labels(broker=self.broker_name).set(self._active)
        return self

    async def __aexit__(self, *_) -> None:
        self._semaphore.release()
        self._active -= 1
        BP_ACTIVE.labels(broker=self.broker_name).set(self._active)

    async def __call__(self) -> "BackpressureGuard":
        """FastAPI Depends()용 callable"""
        return self


# 브로커별 Backpressure 인스턴스
backpressure_guards: dict[str, BackpressureGuard] = {
    "redis":    BackpressureGuard("redis",    max_concurrent=100, max_waiting=200),
    "rabbitmq": BackpressureGuard("rabbitmq", max_concurrent=50,  max_waiting=100),
    "kafka":    BackpressureGuard("kafka",    max_concurrent=30,  max_waiting=60),
}
