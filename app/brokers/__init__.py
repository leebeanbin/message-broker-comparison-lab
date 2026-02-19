"""
브로커 모듈 공개 인터페이스
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AbstractBroker (공통 계약) + 싱글턴 인스턴스를 외부에 노출.

사용 예:
    from app.brokers import AbstractBroker, redis_broker, rabbitmq_broker, kafka_broker

    # 타입 힌팅으로 공통 인터페이스 활용
    async def publish_all(brokers: list[AbstractBroker], msg: dict):
        for broker in brokers:
            await broker.publish("test-dest", msg)
"""

from .base import AbstractBroker
from .kafka_broker import kafka_broker
from .rabbitmq_broker import rabbitmq_broker
from .redis_broker import redis_broker

__all__ = [
    "AbstractBroker",
    "redis_broker",
    "rabbitmq_broker",
    "kafka_broker",
]
