"""
벤치마크 엔드포인트 (개별 + 전체 비교)
"""

from fastapi import APIRouter

from app.brokers import kafka_broker, rabbitmq_broker, redis_broker
from app.monitoring import metrics_collector
from app.schemas import BenchmarkRequest

router = APIRouter(prefix="/benchmark", tags=["Benchmark"])


@router.post("/redis")
async def benchmark_redis(req: BenchmarkRequest):
    """Redis Pub/Sub 벤치마크"""
    result = await redis_broker.benchmark("bench-channel", req.message_count)
    metrics_collector.add_benchmark(result)
    return result


@router.post("/redis-stream")
async def benchmark_redis_stream(req: BenchmarkRequest):
    """Redis Stream 벤치마크 (영속적 쓰기)"""
    result = await redis_broker.benchmark_stream(
        "bench-stream", req.message_count
    )
    metrics_collector.add_benchmark(result)
    return result


@router.post("/rabbitmq")
async def benchmark_rabbitmq(req: BenchmarkRequest):
    """RabbitMQ Direct Queue 벤치마크"""
    result = await rabbitmq_broker.benchmark("bench-queue", req.message_count)
    metrics_collector.add_benchmark(result)
    return result


@router.post("/kafka")
async def benchmark_kafka(req: BenchmarkRequest):
    """Kafka send_and_wait 벤치마크"""
    result = await kafka_broker.benchmark("bench-topic", req.message_count)
    metrics_collector.add_benchmark(result)
    return result


@router.post("/kafka-batch")
async def benchmark_kafka_batch(req: BenchmarkRequest):
    """Kafka Batch 벤치마크 (send + flush)"""
    result = await kafka_broker.benchmark_batch("bench-topic", req.message_count)
    metrics_collector.add_benchmark(result)
    return result


@router.post("/all")
async def benchmark_all(req: BenchmarkRequest):
    """전체 브로커 벤치마크 + 비교"""
    results = {}

    brokers = [
        ("redis", lambda: redis_broker.benchmark(
            "bench-channel", req.message_count
        )),
        ("redis_stream", lambda: redis_broker.benchmark_stream(
            "bench-stream", req.message_count
        )),
        ("rabbitmq", lambda: rabbitmq_broker.benchmark(
            "bench-queue", req.message_count
        )),
        ("kafka", lambda: kafka_broker.benchmark(
            "bench-topic", req.message_count
        )),
        ("kafka_batch", lambda: kafka_broker.benchmark_batch(
            "bench-topic", req.message_count
        )),
    ]

    for name, bench_fn in brokers:
        try:
            r = await bench_fn()
            metrics_collector.add_benchmark(r)
            results[name] = r
        except Exception as e:
            results[name] = {"error": str(e)}

    return {
        "benchmark_results": results,
        "comparison": metrics_collector.get_comparison(),
    }
