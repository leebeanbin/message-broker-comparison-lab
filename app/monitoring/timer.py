"""
AOP 스타일 시간 측정 유틸리티
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
제공하는 도구:

1. @measure_time(broker, operation)
   - async 메서드에 붙이는 데코레이터
   - 실행 시간 자동 측정 → 결과 dict에 elapsed_ms 주입
   - Prometheus 메트릭 자동 기록
   - 함수 시그니처/docstring 보존 (functools.wraps)

2. Stopwatch (컨텍스트 매니저)
   - 벤치마크 루프 내 개별 iteration 측정용
   - with stopwatch.lap(): 으로 한 번의 측정 수행
   - 최종 report()로 통계 일괄 산출

3. @measure_time_sync(broker, operation)
   - 동기 함수용 데코레이터 (필요 시)

설계 원칙:
  - 측정 로직은 비즈니스 로직과 완전히 분리
  - 데코레이터 하나로 Prometheus 기록 + elapsed_ms 주입을 동시에 처리
  - 브로커 코드에 time.perf_counter()가 단 한 줄도 없어야 함
"""

import functools
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from app.monitoring.metrics import metrics_collector

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1) @measure_time - async 메서드 데코레이터
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 사용 예:
#   @measure_time(broker="redis", operation="publish")
#   async def publish(self, channel, message) -> dict:
#       ...
#       return {"broker": "redis", "channel": channel}
#       # → elapsed_ms가 자동으로 주입됨
#       # → Prometheus에 자동 기록됨
#

def measure_time(
    broker: str,
    operation: str = "publish",
    record_metric: bool = True,
) -> Callable:
    """
    async 함수의 실행 시간을 자동 측정하는 데코레이터.

    Args:
        broker: 브로커 이름 (Prometheus label로 사용)
                예: "redis", "rabbitmq", "kafka"
        operation: 작업 종류 ("publish", "consume", "read" 등)
        record_metric: True이면 Prometheus 메트릭에 자동 기록

    동작:
        1. time.perf_counter()로 시작/종료 시간 측정
        2. 반환값이 dict이면 "elapsed_ms" 키를 자동 주입
        3. record_metric=True이면 MetricsCollector에 기록
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            result = await func(*args, **kwargs)
            elapsed_ms = round((time.perf_counter() - start) * 1000, 3)

            # 결과가 dict이면 elapsed_ms 주입
            if isinstance(result, dict):
                result["elapsed_ms"] = elapsed_ms

            # Prometheus 메트릭 기록
            if record_metric:
                if operation == "publish":
                    metrics_collector.record_publish(broker, elapsed_ms)
                elif operation == "consume":
                    metrics_collector.record_consume(broker, elapsed_ms)

            return result

        # 데코레이터 메타정보 (디버깅용)
        wrapper._measure_time_broker = broker
        wrapper._measure_time_operation = operation
        return wrapper

    return decorator


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2) Stopwatch - 벤치마크용 컨텍스트 매니저
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 사용 예:
#   sw = Stopwatch()
#   for i in range(1000):
#       with sw.lap():
#           await client.publish(...)
#
#   return sw.report(broker="redis", count=1000)
#

class Stopwatch:
    """
    벤치마크 루프용 정밀 시간 측정기.

    - lap(): 컨텍스트 매니저로 한 iteration의 시간 측정
    - report(): 전체 통계를 dict로 반환
    """

    def __init__(self):
        self.latencies: list[float] = []
        self._total_start: float | None = None
        self._total_end: float | None = None

    def start(self):
        """전체 벤치마크 타이머 시작"""
        self._total_start = time.perf_counter()
        return self

    def stop(self):
        """전체 벤치마크 타이머 종료"""
        self._total_end = time.perf_counter()
        return self

    @contextmanager
    def lap(self):
        """
        한 iteration의 시간을 측정하는 컨텍스트 매니저.

        사용:
            with sw.lap():
                await some_operation()
        """
        start = time.perf_counter()
        yield
        elapsed_ms = (time.perf_counter() - start) * 1000
        self.latencies.append(elapsed_ms)

    @property
    def total_ms(self) -> float:
        """전체 소요 시간 (ms)"""
        if self._total_start and self._total_end:
            return (self._total_end - self._total_start) * 1000
        # start/stop을 안 쓴 경우 latencies 합산
        return sum(self.latencies)

    def report(self, broker: str, count: int | None = None) -> dict:
        """
        벤치마크 결과를 표준 형식의 dict로 반환.

        Args:
            broker: 브로커 이름
            count: 메시지 수 (None이면 latencies 길이)

        Returns:
            벤치마크 결과 dict (MetricsCollector.add_benchmark() 호환)
        """
        n = count or len(self.latencies)
        total = self.total_ms

        if self.latencies:
            return {
                "broker": broker,
                "total_messages": n,
                "total_ms": round(total, 2),
                "avg_latency_ms": round(
                    sum(self.latencies) / len(self.latencies), 4
                ),
                "min_latency_ms": round(min(self.latencies), 4),
                "max_latency_ms": round(max(self.latencies), 4),
                "throughput_msg_per_sec": round(
                    n / (total / 1000), 1
                ) if total > 0 else 0,
                "p50_latency_ms": round(
                    sorted(self.latencies)[len(self.latencies) // 2], 4
                ),
                "p99_latency_ms": round(
                    sorted(self.latencies)[int(len(self.latencies) * 0.99)], 4
                ),
            }

        # lap()을 안 쓴 경우 (배치 전송 등)
        return {
            "broker": broker,
            "total_messages": n,
            "total_ms": round(total, 2),
            "avg_latency_ms": round(total / n, 4) if n > 0 else 0,
            "min_latency_ms": 0,
            "max_latency_ms": 0,
            "throughput_msg_per_sec": round(
                n / (total / 1000), 1
            ) if total > 0 else 0,
            "p50_latency_ms": 0,
            "p99_latency_ms": 0,
        }
