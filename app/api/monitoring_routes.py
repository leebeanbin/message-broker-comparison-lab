"""
모니터링 + 하위 호환(Legacy) 엔드포인트
"""

from fastapi import APIRouter

from app.monitoring import metrics_collector
from app.schemas import MessageRequest

from .kafka_routes import basic_publish as kafka_basic_publish
from .rabbitmq_routes import direct_publish as rabbitmq_direct_publish
from .redis_routes import pubsub_publish as redis_pubsub_publish

router = APIRouter()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 모니터링
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/monitoring/kafka-lag", tags=["Monitoring"])
async def get_kafka_lag():
    """
    Consumer Group Lag 현재 스냅샷.

    lag=0 → 소비자가 실시간으로 따라잡음 (이상적)
    lag↑  → 소비자 처리 속도 부족 신호
    lag>10K → 프로덕션 알람 기준

    15초 주기로 백그라운드 업데이트 (lag_monitor_loop).
    Kafka가 연결되지 않은 경우 캐시가 비어 있을 수 있습니다.
    """
    from app.monitoring.kafka_lag import get_lag_snapshot
    return get_lag_snapshot()


@router.get("/monitoring/comparison", tags=["Monitoring"])
async def get_comparison():
    """최신 벤치마크 결과 비교 (처리량/레이턴시 순위)"""
    return metrics_collector.get_comparison()


@router.get("/monitoring/history", tags=["Monitoring"])
async def get_history():
    """발행 지연시간 히스토리 (브로커별)"""
    return metrics_collector.get_history()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 하위 호환 (기존 엔드포인트 유지)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/publish/redis", tags=["Legacy"], include_in_schema=False)
async def publish_redis_legacy(msg: MessageRequest):
    return await redis_pubsub_publish(msg)


@router.post("/publish/rabbitmq", tags=["Legacy"], include_in_schema=False)
async def publish_rabbitmq_legacy(msg: MessageRequest):
    return await rabbitmq_direct_publish(msg)


@router.post("/publish/kafka", tags=["Legacy"], include_in_schema=False)
async def publish_kafka_legacy(msg: MessageRequest):
    return await kafka_basic_publish(msg)
