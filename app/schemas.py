"""
API Request/Response 스키마 (Pydantic 모델)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
라우트와 분리하여 단일 책임 원칙(SRP) 준수.
다른 모듈(테스트, 노트북 등)에서도 재사용 가능.
"""

from pydantic import BaseModel, Field

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 공통
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MessageRequest(BaseModel):
    """메시지 발행 요청"""
    content: str
    metadata: dict = {}


class BenchmarkRequest(BaseModel):
    """벤치마크 요청"""
    message_count: int = Field(default=1000, ge=1, le=50000)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Redis
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CacheRequest(BaseModel):
    """Redis 캐시 요청"""
    key: str
    value: dict = {}
    ttl: int = Field(default=60, ge=1, le=86400, description="TTL (초)")


class KVSetRequest(BaseModel):
    """Redis 단순 Key-Value 저장 요청 (문자열 값)"""
    key: str
    value: str
    ttl: int = Field(default=0, ge=0, description="TTL(초), 0이면 만료 없음")


class ListPushRequest(BaseModel):
    """Redis List LPUSH 요청"""
    key: str
    value: str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RabbitMQ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TopicExchangeRequest(BaseModel):
    """RabbitMQ Topic Exchange 발행 요청"""
    routing_key: str = Field(description="예: order.created, log.error")
    content: str
    metadata: dict = {}


class PriorityMessageRequest(BaseModel):
    """RabbitMQ 우선순위 메시지 요청"""
    content: str
    priority: int = Field(
        default=0, ge=0, le=10, description="0~10 (높을수록 우선)"
    )
    metadata: dict = {}


class TTLMessageRequest(BaseModel):
    """RabbitMQ TTL 메시지 요청"""
    content: str
    ttl_ms: int = Field(default=30000, ge=1000, description="밀리초 단위 TTL")
    metadata: dict = {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Kafka
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class KeyedMessageRequest(BaseModel):
    """Kafka 키 기반 발행 요청"""
    key: str = Field(description="파티션 키 (예: user-123)")
    content: str
    metadata: dict = {}


class BatchMessageRequest(BaseModel):
    """Kafka 배치 발행 요청"""
    messages: list[dict] = Field(description="메시지 리스트")
