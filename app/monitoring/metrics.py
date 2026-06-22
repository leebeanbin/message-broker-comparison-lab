"""
Prometheus 메트릭 + 커스텀 모니터링
- 각 브로커별 발행/소비 지연시간 추적
- 처리량(throughput) 카운터
- Kafka Consumer Lag (kafka_lag.py에서 업데이트)
- Circuit Breaker 상태 (resilience/circuit_breaker.py에서 업데이트)
- Backpressure 동시성 (resilience/backpressure.py에서 업데이트)
- Prometheus /metrics 엔드포인트 자동 노출
"""

import time
from collections import defaultdict
from dataclasses import dataclass, field

from prometheus_client import Counter, Gauge, Histogram

# ============================================
# Prometheus 메트릭 정의
# ============================================

PUBLISH_LATENCY = Histogram(
    "broker_publish_latency_seconds",
    "메시지 발행 지연시간 (초)",
    labelnames=["broker"],
    buckets=[0.0001, 0.0002, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

PUBLISH_COUNT = Counter(
    "broker_publish_total",
    "발행된 메시지 총 개수",
    labelnames=["broker"],
)

CONSUME_LATENCY = Histogram(
    "broker_consume_latency_seconds",
    "메시지 소비 지연시간 (초)",
    labelnames=["broker"],
)

CONSUME_COUNT = Counter(
    "broker_consume_total",
    "소비된 메시지 총 개수",
    labelnames=["broker"],
)

ACTIVE_CONNECTIONS = Gauge(
    "broker_active_connections",
    "현재 활성 연결 수",
    labelnames=["broker"],
)


# ============================================
# 인메모리 벤치마크 결과 저장소
# ============================================

@dataclass
class BenchmarkResult:
    broker: str
    total_messages: int
    total_ms: float
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    throughput_msg_per_sec: float
    p50_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    note: str = ""
    timestamp: float = field(default_factory=time.time)


class MetricsCollector:
    """벤치마크 결과를 수집하고 비교 데이터를 제공"""

    def __init__(self):
        self.results: list[BenchmarkResult] = []
        self.publish_history: dict[str, list[float]] = defaultdict(list)

    def record_publish(self, broker: str, latency_ms: float):
        """발행 지연시간 기록"""
        PUBLISH_LATENCY.labels(broker=broker).observe(latency_ms / 1000)
        PUBLISH_COUNT.labels(broker=broker).inc()
        self.publish_history[broker].append(latency_ms)

    def record_consume(self, broker: str, latency_ms: float):
        """소비 지연시간 기록"""
        CONSUME_LATENCY.labels(broker=broker).observe(latency_ms / 1000)
        CONSUME_COUNT.labels(broker=broker).inc()

    def add_benchmark(self, result: dict):
        """벤치마크 결과 저장"""
        self.results.append(BenchmarkResult(**result))

    def get_comparison(self) -> dict:
        """모든 브로커 벤치마크 결과 비교"""
        if not self.results:
            return {"message": "아직 벤치마크 결과가 없습니다. /benchmark 엔드포인트를 호출하세요."}

        comparison = {}
        for r in self.results:
            if r.broker not in comparison or r.timestamp > comparison[r.broker].timestamp:
                comparison[r.broker] = r

        return {
            "brokers": {
                name: {
                    "total_messages": r.total_messages,
                    "total_ms": r.total_ms,
                    "avg_latency_ms": r.avg_latency_ms,
                    "min_latency_ms": r.min_latency_ms,
                    "max_latency_ms": r.max_latency_ms,
                    "throughput_msg_per_sec": r.throughput_msg_per_sec,
                }
                for name, r in comparison.items()
            },
            "ranking_by_throughput": sorted(
                comparison.keys(),
                key=lambda k: comparison[k].throughput_msg_per_sec,
                reverse=True,
            ),
            "ranking_by_latency": sorted(
                comparison.keys(),
                key=lambda k: comparison[k].avg_latency_ms,
            ),
        }

    def get_history(self) -> dict:
        """각 브로커별 발행 지연시간 히스토리"""
        return {
            broker: {
                "count": len(latencies),
                "avg_ms": round(sum(latencies) / len(latencies), 4) if latencies else 0,
                "last_10": latencies[-10:],
            }
            for broker, latencies in self.publish_history.items()
        }


# 싱글턴 인스턴스
metrics_collector = MetricsCollector()
