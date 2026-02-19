"""
브로커 추상 기반 클래스 (Abstract Base Class)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
모든 메시지 브로커가 구현해야 하는 공통 계약(Contract)을 정의.

설계 원칙:
  - 공통 인터페이스만 강제 (connect, disconnect, publish, benchmark)
  - 브로커 고유 기능은 서브클래스에서 자유롭게 추가
  - publish()의 첫 번째 인자를 "destination"으로 통일
    (Redis의 channel, RabbitMQ의 queue, Kafka의 topic → 모두 destination)

왜 SQLAlchemy가 아니라 ABC인가?
  - SQLAlchemy = DB ORM (테이블 ↔ 객체 매핑)
  - 여기서 필요한 건 "서로 다른 브로커의 공통 행위 강제"
  - 이것은 Strategy Pattern / Template Method Pattern 영역
  - Python에서는 ABC(Abstract Base Class)로 해결

Spring 비유:
  - AbstractBroker = Java의 interface MessageBroker
  - RedisBroker    = @Service class RedisBrokerImpl implements MessageBroker
  - 각 브로커의 고유 메서드 = interface에 없는 추가 public 메서드

사용 예:
    brokers: list[AbstractBroker] = [redis_broker, rabbitmq_broker, kafka_broker]

    for broker in brokers:
        await broker.connect()
        result = await broker.publish("test-dest", {"hello": "world"})
        print(f"{broker.name}: {result['elapsed_ms']}ms")
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any


class AbstractBroker(ABC):
    """
    메시지 브로커 공통 인터페이스.

    모든 브로커는 이 클래스를 상속하고 아래 메서드를 반드시 구현해야 합니다.
    구현하지 않으면 TypeError가 발생합니다 (ABC 강제).
    """

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 필수 속성
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @property
    @abstractmethod
    def name(self) -> str:
        """
        브로커 고유 이름.
        Prometheus label, 로그, 벤치마크 결과 등에 사용.

        예: "redis", "rabbitmq", "kafka"
        """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """
        현재 연결 상태.
        헬스체크(/health)에서 일관되게 조회 가능.
        """

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 라이프사이클
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @abstractmethod
    async def connect(self) -> None:
        """
        브로커에 연결.
        Kafka의 경우 내부적으로 Producer를 시작합니다.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """
        브로커 연결 종료.
        리소스 정리를 포함합니다.
        """

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 메시징 (공통)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @abstractmethod
    async def publish(self, destination: str, message: dict) -> dict:
        """
        메시지를 발행합니다.

        Args:
            destination: 메시지 목적지
                - Redis: channel 이름
                - RabbitMQ: queue 이름
                - Kafka: topic 이름
            message: 발행할 메시지 (dict)

        Returns:
            표준 응답 dict. 최소한 아래 키를 포함해야 합니다:
                {
                    "broker": str,         # 브로커 이름
                    "pattern": str,        # 사용 패턴 (pubsub, direct, basic 등)
                    "destination": str,    # 목적지
                    "elapsed_ms": float,   # @measure_time이 자동 주입
                    ...                    # 브로커별 추가 정보
                }
        """

    @abstractmethod
    async def subscribe(self, destination: str, callback: Callable) -> None:
        """
        메시지를 구독합니다.

        Args:
            destination: 구독 대상 (channel/queue/topic)
            callback: 메시지 수신 시 호출할 async 함수
        """

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 벤치마크
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @abstractmethod
    async def benchmark(self, destination: str, count: int = 1000) -> dict:
        """
        벤치마크를 실행합니다.

        Args:
            destination: 벤치마크 대상 (channel/queue/topic)
            count: 발행할 메시지 수

        Returns:
            Stopwatch.report() 호환 dict:
                {
                    "broker": str,
                    "total_messages": int,
                    "total_ms": float,
                    "avg_latency_ms": float,
                    "min_latency_ms": float,
                    "max_latency_ms": float,
                    "throughput_msg_per_sec": float,
                    "p50_latency_ms": float,
                    "p99_latency_ms": float,
                }
        """

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 유틸리티 (공통 구현, 오버라이드 가능)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def __repr__(self) -> str:
        status = "connected" if self.is_connected else "disconnected"
        return f"<{self.__class__.__name__} name={self.name!r} status={status}>"

    async def health_check(self) -> dict[str, Any]:
        """
        헬스체크 (기본 구현: is_connected 반환).
        브로커별로 오버라이드하여 상세 정보를 추가할 수 있습니다.
        """
        return {
            "broker": self.name,
            "connected": self.is_connected,
        }
