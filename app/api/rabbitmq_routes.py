"""
RabbitMQ 엔드포인트 (Direct, Fanout, Topic, DLQ, Priority, TTL)
"""

from fastapi import APIRouter, HTTPException, Query

from app.brokers import rabbitmq_broker
from app.schemas import (
    MessageRequest,
    PriorityMessageRequest,
    TopicExchangeRequest,
    TTLMessageRequest,
)

router = APIRouter(prefix="/rabbitmq", tags=["RabbitMQ"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Direct Queue
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/direct/publish", tags=["RabbitMQ - Direct"])
async def direct_publish(
    msg: MessageRequest,
    queue_name: str = Query(default="order-queue", description="대상 큐 이름"),
):
    """RabbitMQ Direct: 큐에 직접 메시지 전달"""
    try:
        return await rabbitmq_broker.publish(
            queue_name, {"content": msg.content, **msg.metadata}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RabbitMQ Direct 실패: {e}")


@router.get("/queue/info/{queue_name}", tags=["RabbitMQ - Direct"])
async def queue_info(queue_name: str):
    """RabbitMQ: 큐 상태 정보"""
    try:
        return await rabbitmq_broker.queue_info(queue_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"큐 정보 조회 실패: {e}")


@router.get("/queue/messages/{queue_name}", tags=["RabbitMQ - Direct"])
async def queue_messages(
    queue_name: str,
    count: int = Query(default=10, ge=1, le=100),
):
    """RabbitMQ: 큐에서 메시지 peek (꺼내지 않고 확인)"""
    try:
        return await rabbitmq_broker.get_dlq_messages(queue_name, count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"큐 메시지 조회 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fanout Exchange
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/fanout/bind", tags=["RabbitMQ - Fanout"])
async def fanout_bind(
    queue_name: str = Query(description="바인딩할 큐 이름"),
):
    """RabbitMQ Fanout: 큐를 브로드캐스트 Exchange에 바인딩"""
    try:
        return await rabbitmq_broker.bind_fanout("broadcast-exchange", queue_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fanout 바인딩 실패: {e}")


@router.post("/fanout/publish", tags=["RabbitMQ - Fanout"])
async def fanout_publish(msg: MessageRequest):
    """RabbitMQ Fanout: 모든 바인딩 큐에 브로드캐스트"""
    try:
        return await rabbitmq_broker.publish_fanout(
            "broadcast-exchange", {"content": msg.content, **msg.metadata}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fanout 발행 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Topic Exchange
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/topic/bind", tags=["RabbitMQ - Topic"])
async def topic_bind(
    queue_name: str = Query(description="큐 이름"),
    binding_key: str = Query(description="바인딩 패턴 (예: order.*, log.#)"),
):
    """RabbitMQ Topic: 패턴 키로 큐 바인딩 (* = 한 단어, # = 여러 단어)"""
    try:
        return await rabbitmq_broker.bind_topic(
            "topic-exchange", queue_name, binding_key
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Topic 바인딩 실패: {e}")


@router.post("/topic/publish", tags=["RabbitMQ - Topic"])
async def topic_publish(req: TopicExchangeRequest):
    """RabbitMQ Topic: 라우팅 키 기반 선별 전달"""
    try:
        return await rabbitmq_broker.publish_topic(
            "topic-exchange",
            req.routing_key,
            {"content": req.content, **req.metadata},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Topic 발행 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DLQ (Dead Letter Queue)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/dlq/setup", tags=["RabbitMQ - DLQ"])
async def dlq_setup(
    queue_name: str = Query(default="task-queue", description="메인 큐 이름"),
):
    """RabbitMQ DLQ: 메인 큐 + Dead Letter Queue 구성"""
    try:
        return await rabbitmq_broker.setup_dlq(queue_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DLQ 설정 실패: {e}")


@router.get("/dlq/messages", tags=["RabbitMQ - DLQ"])
async def dlq_messages(
    queue_name: str = Query(default="task-queue"),
    count: int = Query(default=10, ge=1, le=100),
):
    """RabbitMQ DLQ: 실패 메시지 조회"""
    try:
        return await rabbitmq_broker.get_dlq_messages(queue_name, count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DLQ 조회 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Priority Queue
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/priority/publish", tags=["RabbitMQ - Priority"])
async def priority_publish(
    req: PriorityMessageRequest,
    queue_name: str = Query(default="task-queue", description="대상 큐 이름"),
):
    """RabbitMQ Priority: 우선순위 메시지 발행 (0~10, 높을수록 우선)"""
    try:
        return await rabbitmq_broker.publish_priority(
            queue_name,
            {"content": req.content, **req.metadata},
            priority=req.priority,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Priority 발행 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TTL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/ttl/publish", tags=["RabbitMQ - TTL"])
async def ttl_publish(
    req: TTLMessageRequest,
    queue_name: str = Query(default="temp-queue", description="대상 큐 이름"),
):
    """RabbitMQ TTL: 만료 시간 있는 메시지 발행"""
    try:
        return await rabbitmq_broker.publish_ttl(
            queue_name,
            {"content": req.content, **req.metadata},
            ttl_ms=req.ttl_ms,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"TTL 발행 실패: {e}")
