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
