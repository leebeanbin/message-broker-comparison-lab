"""
헬스체크 + Direct API (기준선)
"""

from fastapi import APIRouter

from app.brokers import AbstractBroker, kafka_broker, rabbitmq_broker, redis_broker
from app.monitoring.timer import measure_time
from app.schemas import MessageRequest

router = APIRouter()

# 등록된 브로커 목록
_BROKERS: list[AbstractBroker] = [redis_broker, rabbitmq_broker, kafka_broker]


@router.get("/health", tags=["Health"])
async def health_check():
    """
    서비스 + 브로커 연결 상태 확인.
    AbstractBroker.is_connected 속성으로 통일된 상태 조회.
    """
    status = {"api": "running"}

    for broker in _BROKERS:
        try:
            info = await broker.health_check()
            status[broker.name] = "connected" if info["connected"] else "disconnected"
        except Exception:
            status[broker.name] = "disconnected"

    return status


@router.post("/api/direct", tags=["Direct API"])
@measure_time(broker="direct_api", operation="publish")
async def direct_api(msg: MessageRequest):
    """순수 API 호출 (브로커 미사용) → 비교 기준선"""
    return {
        "broker": "none (direct API)",
        "content": msg.content,
        "metadata": msg.metadata,
        "processed": True,
    }
