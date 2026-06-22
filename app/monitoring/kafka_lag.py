"""
Kafka Consumer Lag 모니터링
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Consumer Group이 Producer보다 얼마나 뒤처졌는지 추적.

Lag = End Offset - Committed Offset

lag=0     → consumer가 실시간으로 따라잡음 (이상적)
lag↑      → consumer 처리 속도 부족 신호
lag > 10K → 🚨 프로덕션 알람 기준

Prometheus 지표:
  kafka_consumer_group_lag{group, topic, partition}
  kafka_consumer_group_lag_sum{group, topic}  (토픽 전체 lag)
"""

import asyncio
import logging

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from prometheus_client import Gauge

from app.config import settings

logger = logging.getLogger(__name__)

# ── Prometheus 메트릭 ──
CONSUMER_LAG = Gauge(
    "kafka_consumer_group_lag",
    "Kafka Consumer Group Lag (파티션 단위)",
    labelnames=["group", "topic", "partition"],
)
CONSUMER_LAG_SUM = Gauge(
    "kafka_consumer_group_lag_sum",
    "Kafka Consumer Group 전체 Lag (토픽 합계)",
    labelnames=["group", "topic"],
)

# 모니터링할 Consumer Group + Topic 목록
# 새 Consumer Group을 추적하려면 여기에 추가
MONITORED_GROUPS: list[dict] = [
    {"group": "test-group",      "topic": "test-topic"},
    {"group": "bench-group",     "topic": "bench-topic"},
    {"group": "challenge-group", "topic": "challenge-topic"},
]

# 마지막 측정 결과 캐시 — /monitoring/kafka-lag 엔드포인트용
_lag_cache: dict[str, dict] = {}  # key: "group/topic"


async def _measure_lag_once(admin: AIOKafkaAdminClient, group: str, topic: str) -> dict:
    """한 번의 Lag 측정 — consumer group + topic 단위"""
    try:
        # Committed offsets
        committed = await admin.list_consumer_group_offsets(group)

        # Partition 필터: 해당 topic만
        tps = [tp for tp in committed if tp.topic == topic]
        if not tps:
            return {}

        # End offsets — 임시 Consumer로 조회
        consumer = AIOKafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        await consumer.start()
        try:
            end_offsets = await consumer.end_offsets(tps)
        finally:
            await consumer.stop()

        lag_by_partition: dict[int, int] = {}
        for tp in tps:
            committed_offset = committed[tp].offset
            end_offset = end_offsets.get(tp, 0)
            # committed_offset이 -1이면 아직 커밋 없음 → lag = end_offset
            lag = max(0, end_offset - (committed_offset if committed_offset >= 0 else 0))
            lag_by_partition[tp.partition] = lag

            CONSUMER_LAG.labels(
                group=group, topic=topic, partition=str(tp.partition)
            ).set(lag)

        total_lag = sum(lag_by_partition.values())
        CONSUMER_LAG_SUM.labels(group=group, topic=topic).set(total_lag)

        result = {"group": group, "topic": topic, "lag": lag_by_partition, "total": total_lag}
        _lag_cache[f"{group}/{topic}"] = result
        return result

    except Exception as e:
        logger.debug("Lag check skipped (%s/%s): %s", group, topic, e)
        return {}


async def lag_monitor_loop(interval_seconds: float = 15.0) -> None:
    """
    백그라운드 루프: 주기적으로 모든 Consumer Group lag 측정.
    lifespan에서 asyncio.create_task()로 실행.
    """
    await asyncio.sleep(10)  # 브로커 연결 안정화 대기

    admin: AIOKafkaAdminClient | None = None

    while True:
        try:
            if admin is None:
                admin = AIOKafkaAdminClient(
                    bootstrap_servers=settings.kafka_bootstrap_servers
                )
                await admin.start()

            results = []
            for target in MONITORED_GROUPS:
                result = await _measure_lag_once(admin, target["group"], target["topic"])
                if result:
                    results.append(result)

            if results:
                total_lags = [r["total"] for r in results]
                logger.debug(
                    "Lag check complete: %d groups, max_lag=%d",
                    len(results),
                    max(total_lags) if total_lags else 0,
                )

        except Exception as e:
            logger.warning("Lag monitor error (will retry): %s", e)
            if admin:
                try:
                    await admin.close()
                except Exception:
                    pass
                admin = None

        await asyncio.sleep(interval_seconds)


def register_consumer_group(group: str, topic: str) -> None:
    """런타임에 모니터링할 Consumer Group 동적 등록"""
    entry = {"group": group, "topic": topic}
    if entry not in MONITORED_GROUPS:
        MONITORED_GROUPS.append(entry)
        logger.info("Consumer group registered for lag monitoring: %s/%s", group, topic)


def get_lag_snapshot() -> dict:
    """마지막 측정된 Consumer Lag 캐시 반환 (HTTP 엔드포인트용)"""
    return {
        "monitored_groups": MONITORED_GROUPS,
        "lag_data": list(_lag_cache.values()),
        "cache_size": len(_lag_cache),
    }
