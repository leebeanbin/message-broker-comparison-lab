"""
Kafka 엔드포인트 (Basic, Keyed, Batch, Topic Management)
"""

from fastapi import APIRouter, HTTPException, Query

from app.brokers import kafka_broker
from app.schemas import BatchMessageRequest, KeyedMessageRequest, MessageRequest

router = APIRouter(prefix="/kafka", tags=["Kafka"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Basic Produce
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/basic/publish", tags=["Kafka - Basic"])
async def basic_publish(msg: MessageRequest):
    """Kafka Basic: 메시지 발행 (자동 파티셔닝)"""
    try:
        return await kafka_broker.publish(
            "test-topic", {"content": msg.content, **msg.metadata}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka 발행 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Keyed Produce
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/keyed/publish", tags=["Kafka - Keyed"])
async def keyed_publish(req: KeyedMessageRequest):
    """Kafka Keyed: 키 기반 파티셔닝 (동일 키 → 동일 파티션)"""
    try:
        return await kafka_broker.publish_keyed(
            "test-topic", req.key, {"content": req.content, **req.metadata}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka Keyed 발행 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Batch Produce
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/batch/publish", tags=["Kafka - Batch"])
async def batch_publish(req: BatchMessageRequest):
    """Kafka Batch: 대량 메시지 일괄 발행 (send + flush)"""
    try:
        return await kafka_broker.publish_batch("test-topic", req.messages)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka Batch 발행 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Topic Management
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/topic/create", tags=["Kafka - Topic"])
async def topic_create(
    topic: str = Query(description="토픽 이름"),
    partitions: int = Query(default=3, ge=1, le=12),
):
    """Kafka Topic: 토픽 생성 (파티션 수 지정)"""
    try:
        return await kafka_broker.create_topic(topic, num_partitions=partitions)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"토픽 생성 실패: {e}")


@router.get("/topics", tags=["Kafka - Topic"])
async def list_topics():
    """Kafka Topic: 토픽 목록 조회"""
    try:
        return await kafka_broker.list_topics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"토픽 목록 조회 실패: {e}")


@router.get("/topic/info/{topic}", tags=["Kafka - Topic"])
async def topic_info(topic: str):
    """Kafka Topic: 토픽 상세 정보"""
    try:
        return await kafka_broker.topic_info(topic)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"토픽 정보 조회 실패: {e}")
