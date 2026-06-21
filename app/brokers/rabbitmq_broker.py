"""
RabbitMQ 메시지 브로커 (AMQP 0-9-1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
지원 패턴:
  1. Direct Queue    - 기본 큐에 직접 전달 (point-to-point)
  2. Fanout Exchange - 모든 바인딩 큐에 브로드캐스트
  3. Topic Exchange  - 라우팅 키 패턴 매칭으로 선별 전달
  4. DLQ (Dead Letter Queue) - 실패 메시지 격리
  5. Priority Queue  - 우선순위 기반 처리
  6. TTL (Time-To-Live) - 메시지 만료 정책
"""

import json
import time
from collections.abc import Callable

import aio_pika

from app.brokers.base import AbstractBroker
from app.config import settings
from app.monitoring.timer import Stopwatch, measure_time


class RabbitMQBroker(AbstractBroker):
    def __init__(self):
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # AbstractBroker 필수 구현
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @property
    def name(self) -> str:
        return "rabbitmq"

    @property
    def is_connected(self) -> bool:
        return (
            self.connection is not None
            and not self.connection.is_closed
        )

    async def connect(self) -> None:
        """RabbitMQ 연결"""
        self.connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)
        print(f"✅ {self.name} 연결 성공")

    async def disconnect(self) -> None:
        """RabbitMQ 연결 종료"""
        if self.connection:
            await self.connection.close()
        self.connection = None
        self.channel = None
        print(f"🔌 {self.name} 연결 종료")

    @measure_time(broker="rabbitmq", operation="publish")
    async def publish(self, destination: str, message: dict) -> dict:
        """Direct: 큐에 메시지 직접 전달"""
        queue = await self.channel.declare_queue(destination, durable=True)

        payload = json.dumps({
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "direct",
        })

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=payload.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=destination,
        )

        return {
            "broker": self.name,
            "pattern": "direct",
            "destination": destination,
            "queue_message_count": queue.declaration_result.message_count,
        }

    async def subscribe(self, destination: str, callback: Callable) -> None:
        """Direct: 큐에서 메시지 소비"""
        queue = await self.channel.declare_queue(destination, durable=True)
        print(f"📡 RabbitMQ '{destination}' 큐 구독 시작")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())
                    await callback(data)

    async def benchmark(self, destination: str, count: int = 1000) -> dict:
        """Direct Queue 벤치마크"""
        await self.channel.declare_queue(destination, durable=True)
        sw = Stopwatch()
        sw.start()

        for i in range(count):
            with sw.lap():
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps({"seq": i, "data": f"bench-{i}"}).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=destination,
                )

        sw.stop()
        return sw.report(broker=self.name, count=count)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RabbitMQ 고유 기능: Fanout Exchange
    #   Producer → [Fanout Exchange] → Queue A / Queue B / Queue C
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="rabbitmq_fanout", operation="publish")
    async def publish_fanout(self, exchange_name: str, message: dict) -> dict:
        """Fanout: 모든 바인딩 큐에 브로드캐스트"""
        exchange = await self.channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.FANOUT, durable=True
        )

        payload = json.dumps({
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "fanout",
        })

        await exchange.publish(
            aio_pika.Message(
                body=payload.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="",
        )

        return {
            "broker": self.name,
            "pattern": "fanout",
            "destination": exchange_name,
        }

    async def bind_fanout(self, exchange_name: str, queue_name: str) -> dict:
        """Fanout: 큐를 Exchange에 바인딩"""
        exchange = await self.channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.FANOUT, durable=True
        )
        queue = await self.channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange)

        return {
            "status": "bound",
            "exchange": exchange_name,
            "queue": queue_name,
            "type": "fanout",
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RabbitMQ 고유 기능: Topic Exchange
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="rabbitmq_topic", operation="publish")
    async def publish_topic(
        self, exchange_name: str, routing_key: str, message: dict
    ) -> dict:
        """Topic: 라우팅 키 기반 선별 전달"""
        exchange = await self.channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        payload = json.dumps({
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "topic",
            "routing_key": routing_key,
        })

        await exchange.publish(
            aio_pika.Message(
                body=payload.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )

        return {
            "broker": self.name,
            "pattern": "topic",
            "destination": exchange_name,
            "routing_key": routing_key,
        }

    async def bind_topic(
        self, exchange_name: str, queue_name: str, binding_key: str
    ) -> dict:
        """Topic: 패턴 키로 큐 바인딩"""
        exchange = await self.channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        queue = await self.channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=binding_key)

        return {
            "status": "bound",
            "exchange": exchange_name,
            "queue": queue_name,
            "binding_key": binding_key,
            "type": "topic",
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RabbitMQ 고유 기능: DLQ
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def setup_dlq(self, queue_name: str) -> dict:
        """DLQ: 메인 큐 + DLQ 세트 구성"""
        dlq_name = f"{queue_name}.dlq"
        dlx_name = f"{queue_name}.dlx"

        dlx = await self.channel.declare_exchange(
            dlx_name, aio_pika.ExchangeType.FANOUT, durable=True
        )
        dlq = await self.channel.declare_queue(dlq_name, durable=True)
        await dlq.bind(dlx)

        main_queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dlq_name,
            },
        )

        return {
            "status": "configured",
            "main_queue": queue_name,
            "dlq": dlq_name,
            "dlx": dlx_name,
            "main_queue_messages": main_queue.declaration_result.message_count,
        }

    async def get_dlq_messages(self, queue_name: str, count: int = 10) -> dict:
        """DLQ: 실패 메시지 조회 (peek)"""
        dlq_name = f"{queue_name}.dlq"
        queue = await self.channel.declare_queue(dlq_name, durable=True)

        messages = []
        for _ in range(count):
            msg = await queue.get(fail=False)
            if msg is None:
                break
            data = json.loads(msg.body.decode())
            messages.append({
                "data": data,
                "headers": dict(msg.headers) if msg.headers else {},
                "redelivered": msg.redelivered,
            })
            await msg.nack(requeue=True)

        return {"dlq": dlq_name, "messages": messages, "count": len(messages)}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RabbitMQ 고유 기능: Priority Queue
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="rabbitmq_priority", operation="publish")
    async def publish_priority(
        self, queue_name: str, message: dict, priority: int = 0
    ) -> dict:
        """Priority: 우선순위 메시지 발행 (0~10)"""
        queue = await self.channel.declare_queue(
            queue_name, durable=True, arguments={"x-max-priority": 10},
        )

        payload = json.dumps({
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "priority",
            "priority": priority,
        })

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=payload.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=priority,
            ),
            routing_key=queue_name,
        )

        return {
            "broker": self.name,
            "pattern": "priority",
            "destination": queue_name,
            "priority": priority,
            "queue_message_count": queue.declaration_result.message_count,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RabbitMQ 고유 기능: TTL
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="rabbitmq_ttl", operation="publish")
    async def publish_ttl(
        self, queue_name: str, message: dict, ttl_ms: int = 30000
    ) -> dict:
        """TTL: 만료 시간 있는 메시지 발행"""
        queue = await self.channel.declare_queue(queue_name, durable=True)

        payload = json.dumps({
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "ttl",
            "ttl_ms": ttl_ms,
        })

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=payload.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                expiration=int(ttl_ms),
            ),
            routing_key=queue_name,
        )

        return {
            "broker": self.name,
            "pattern": "ttl",
            "destination": queue_name,
            "ttl_ms": ttl_ms,
            "queue_message_count": queue.declaration_result.message_count,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 큐 정보 조회
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def queue_info(self, queue_name: str) -> dict:
        """큐 상태 정보 조회"""
        try:
            queue = await self.channel.declare_queue(
                queue_name, durable=True, passive=True
            )
            return {
                "queue": queue_name,
                "message_count": queue.declaration_result.message_count,
                "consumer_count": queue.declaration_result.consumer_count,
            }
        except Exception as e:
            return {"queue": queue_name, "error": str(e)}


# 싱글턴 인스턴스
rabbitmq_broker = RabbitMQBroker()
