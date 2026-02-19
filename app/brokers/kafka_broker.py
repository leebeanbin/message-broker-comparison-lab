"""
Apache Kafka 메시지 브로커
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
지원 패턴:
  1. Basic Produce   - 단일 메시지 발행 (자동 파티셔닝)
  2. Keyed Produce   - 키 기반 파티셔닝 (동일 키 → 동일 파티션)
  3. Batch Produce   - 대량 메시지 일괄 발행
  4. Topic Metadata   - 토픽/파티션 메타데이터 조회
  5. Consumer Group   - 그룹 기반 병렬 소비
"""

import json
import time
from collections.abc import Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from app.brokers.base import AbstractBroker
from app.config import settings
from app.monitoring.timer import Stopwatch, measure_time


class KafkaBroker(AbstractBroker):
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.admin: AIOKafkaAdminClient | None = None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # AbstractBroker 필수 구현
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @property
    def name(self) -> str:
        return "kafka"

    @property
    def is_connected(self) -> bool:
        return self.producer is not None

    async def connect(self) -> None:
        """Kafka Producer 연결 (공통 인터페이스)"""
        await self.connect_producer()

    async def disconnect(self) -> None:
        """Kafka 전체 연결 종료"""
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()
        if self.admin:
            await self.admin.close()
        self.producer = None
        self.consumer = None
        self.admin = None
        print(f"🔌 {self.name} 연결 종료")

    @measure_time(broker="kafka", operation="publish")
    async def publish(self, destination: str, message: dict) -> dict:
        """Basic: 메시지 발행 (자동 파티셔닝)"""
        payload = {
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "basic",
        }

        result = await self.producer.send_and_wait(destination, payload)

        return {
            "broker": self.name,
            "pattern": "basic",
            "destination": destination,
            "partition": result.partition,
            "offset": result.offset,
        }

    async def subscribe(self, destination: str, callback: Callable) -> None:
        """메시지 구독 (Consumer)"""
        if not self.consumer:
            await self.connect_consumer(destination)
        print(f"📡 Kafka Consumer 수신 대기 중 (topic: {destination})...")
        async for msg in self.consumer:
            await callback(msg.value)

    async def benchmark(self, destination: str, count: int = 1000) -> dict:
        """send_and_wait 벤치마크 (개별 확인)"""
        sw = Stopwatch()
        sw.start()

        for i in range(count):
            with sw.lap():
                await self.producer.send_and_wait(
                    destination,
                    {"seq": i, "data": f"bench-{i}"},
                )

        sw.stop()
        return sw.report(broker=self.name, count=count)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Kafka 고유: 세부 연결 관리
    #   (connect()는 connect_producer()를 호출하지만,
    #    admin/consumer는 필요 시 개별 연결)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def connect_producer(self) -> None:
        """Kafka Producer 연결"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            linger_ms=5,
            max_batch_size=16384,
            compression_type="gzip",
        )
        await self.producer.start()
        print(f"✅ {self.name} Producer 연결 성공")

    async def connect_admin(self) -> None:
        """Kafka Admin Client 연결"""
        self.admin = AIOKafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        await self.admin.start()
        print(f"✅ {self.name} Admin 연결 성공")

    async def connect_consumer(
        self, topic: str, group_id: str = "test-group"
    ) -> None:
        """Kafka Consumer 연결"""
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        await self.consumer.start()
        print(f"✅ {self.name} Consumer 연결 성공 (topic: {topic})")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Kafka 고유 기능: Keyed Produce
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="kafka_keyed", operation="publish")
    async def publish_keyed(self, topic: str, key: str, message: dict) -> dict:
        """Keyed: 키 기반 파티셔닝으로 발행"""
        payload = {
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "keyed",
            "key": key,
        }

        result = await self.producer.send_and_wait(topic, payload, key=key)

        return {
            "broker": self.name,
            "pattern": "keyed",
            "destination": topic,
            "key": key,
            "partition": result.partition,
            "offset": result.offset,
            "note": (
                f"key '{key}'는 항상 "
                f"partition {result.partition}으로 전달됨"
            ),
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Kafka 고유 기능: Batch Produce
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="kafka_batch", operation="publish")
    async def publish_batch(self, topic: str, messages: list[dict]) -> dict:
        """Batch: 대량 메시지 일괄 발행"""
        futures = []
        for i, msg in enumerate(messages):
            payload = {
                **msg,
                "timestamp": time.time(),
                "broker": self.name,
                "pattern": "batch",
                "batch_index": i,
            }
            future = await self.producer.send(topic, payload)
            futures.append(future)

        await self.producer.flush()

        partitions = set()
        for f in futures:
            meta = await f
            partitions.add(meta.partition)

        return {
            "broker": self.name,
            "pattern": "batch",
            "destination": topic,
            "total_messages": len(messages),
            "partitions_used": sorted(partitions),
        }

    async def benchmark_batch(self, topic: str, count: int = 1000) -> dict:
        """배치 벤치마크 (send + flush)"""
        sw = Stopwatch()
        sw.start()

        for i in range(count):
            await self.producer.send(
                topic, {"seq": i, "data": f"bench-batch-{i}"},
            )

        await self.producer.flush()
        sw.stop()

        report = sw.report(broker="kafka_batch", count=count)
        report["note"] = "배치 전송 (개별 latency 미측정, flush 기준)"
        return report

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Kafka 고유 기능: Topic Management
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def create_topic(
        self, topic: str, num_partitions: int = 3, replication_factor: int = 1
    ) -> dict:
        """토픽 생성"""
        if not self.admin:
            await self.connect_admin()
        try:
            new_topic = NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            await self.admin.create_topics([new_topic])
            return {
                "status": "created",
                "topic": topic,
                "partitions": num_partitions,
                "replication_factor": replication_factor,
            }
        except Exception as e:
            if "TopicExistsError" in str(type(e).__name__) or "already exists" in str(e):
                return {"status": "already_exists", "topic": topic}
            raise

    async def list_topics(self) -> dict:
        """토픽 목록 조회"""
        if not self.admin:
            await self.connect_admin()

        metadata = await self.admin.describe_cluster()
        topics_meta = await self.admin.list_topics()

        ctrl_id = (
            metadata.controller_id
            if hasattr(metadata, "controller_id")
            else None
        )
        broker_count = (
            len(metadata.brokers)
            if hasattr(metadata, "brokers")
            else 1
        )

        return {
            "cluster": {
                "controller_id": ctrl_id,
                "brokers": broker_count,
            },
            "topics": sorted(
                [t for t in topics_meta if not t.startswith("__")],
            ),
            "internal_topics": sorted(
                [t for t in topics_meta if t.startswith("__")],
            ),
        }

    async def topic_info(self, topic: str) -> dict:
        """토픽 상세 정보"""
        partitions = self.producer.partitions_for(topic)

        partition_info = []
        if partitions:
            for p in sorted(partitions):
                partition_info.append({"partition": p})

        return {
            "topic": topic,
            "num_partitions": len(partitions) if partitions else 0,
            "partitions": partition_info,
        }


# 싱글턴 인스턴스
kafka_broker = KafkaBroker()
