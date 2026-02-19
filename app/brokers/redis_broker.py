"""
Redis 메시지 브로커 + 고급 기능
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
지원 패턴:
  1. Pub/Sub       - fire-and-forget 실시간 브로드캐스트
  2. Stream        - 영속성 있는 이벤트 로그 (Kafka-like)
  3. List Queue    - 간단한 작업 큐 (LPUSH/BRPOP)
  4. Cache         - TTL 기반 캐시 (SET/GET)
  5. Rate Limiter  - 슬라이딩 윈도우 속도 제한
"""

import json
import time
import uuid
from collections.abc import Callable

import redis.asyncio as aioredis

from app.brokers.base import AbstractBroker
from app.config import settings
from app.monitoring.timer import Stopwatch, measure_time


class RedisBroker(AbstractBroker):
    def __init__(self):
        self.client: aioredis.Redis | None = None
        self._pubsub: aioredis.client.PubSub | None = None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # AbstractBroker 필수 구현
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @property
    def name(self) -> str:
        return "redis"

    @property
    def is_connected(self) -> bool:
        return self.client is not None

    async def connect(self) -> None:
        """Redis 연결"""
        self.client = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
        )
        await self.client.ping()
        print(f"✅ {self.name} 연결 성공")

    async def disconnect(self) -> None:
        """Redis 연결 종료"""
        if self._pubsub:
            await self._pubsub.close()
        if self.client:
            await self.client.close()
        self.client = None
        print(f"🔌 {self.name} 연결 종료")

    @measure_time(broker="redis", operation="publish")
    async def publish(self, destination: str, message: dict) -> dict:
        """Pub/Sub: 메시지 발행"""
        payload = json.dumps({
            **message,
            "timestamp": time.time(),
            "broker": self.name,
            "pattern": "pubsub",
        })
        subscribers = await self.client.publish(destination, payload)

        return {
            "broker": self.name,
            "pattern": "pubsub",
            "destination": destination,
            "subscribers_received": subscribers,
        }

    async def subscribe(self, destination: str, callback: Callable) -> None:
        """Pub/Sub: 메시지 구독"""
        self._pubsub = self.client.pubsub()
        await self._pubsub.subscribe(destination)
        print(f"📡 Redis Pub/Sub '{destination}' 채널 구독 시작")

        async for msg in self._pubsub.listen():
            if msg["type"] == "message":
                data = json.loads(msg["data"])
                await callback(data)

    async def benchmark(self, destination: str, count: int = 1000) -> dict:
        """Pub/Sub 벤치마크"""
        sw = Stopwatch()
        sw.start()

        for i in range(count):
            with sw.lap():
                await self.client.publish(
                    destination,
                    json.dumps({"seq": i, "data": f"bench-{i}"}),
                )

        sw.stop()
        return sw.report(broker=self.name, count=count)

    async def health_check(self) -> dict:
        """Redis 상세 헬스체크"""
        base = await super().health_check()
        if self.client:
            try:
                await self.client.ping()
                base["ping"] = "pong"
            except Exception as e:
                base["ping"] = str(e)
        return base

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Redis 고유 기능: Stream 패턴 (Redis 5.0+)
    #   - Kafka처럼 영속적인 이벤트 로그
    #   - Consumer Group으로 병렬 처리 가능
    #   - 메시지 ID 기반 재처리 지원
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="redis_stream", operation="publish")
    async def stream_add(self, stream: str, message: dict) -> dict:
        """Stream: 이벤트 추가 (XADD)"""
        entry = {
            "data": json.dumps(message),
            "broker": self.name,
            "pattern": "stream",
            "produced_at": str(time.time()),
        }
        msg_id = await self.client.xadd(stream, entry)
        length = await self.client.xlen(stream)

        return {
            "broker": self.name,
            "pattern": "stream",
            "destination": stream,
            "message_id": msg_id,
            "stream_length": length,
        }

    @measure_time(broker="redis_stream", operation="consume", record_metric=False)
    async def stream_read(
        self, stream: str, count: int = 10, last_id: str = "0"
    ) -> dict:
        """Stream: 이벤트 읽기 (XREAD)"""
        messages = await self.client.xread({stream: last_id}, count=count)

        parsed = []
        if messages:
            for _stream_name, entries in messages:
                for msg_id, fields in entries:
                    parsed.append({
                        "id": msg_id,
                        "data": json.loads(fields.get("data", "{}")),
                        "produced_at": fields.get("produced_at"),
                    })

        return {
            "broker": self.name,
            "pattern": "stream",
            "destination": stream,
            "messages": parsed,
            "count": len(parsed),
        }

    async def stream_create_group(self, stream: str, group: str) -> dict:
        """Stream: Consumer Group 생성"""
        try:
            await self.client.xgroup_create(stream, group, id="0", mkstream=True)
            return {"status": "created", "stream": stream, "group": group}
        except Exception as e:
            if "BUSYGROUP" in str(e):
                return {"status": "already_exists", "stream": stream, "group": group}
            raise

    @measure_time(broker="redis_stream", operation="consume", record_metric=False)
    async def stream_read_group(
        self, stream: str, group: str, consumer: str, count: int = 10
    ) -> dict:
        """Stream: Consumer Group으로 읽기 (XREADGROUP)"""
        messages = await self.client.xreadgroup(
            group, consumer, {stream: ">"}, count=count
        )

        parsed = []
        if messages:
            for _stream_name, entries in messages:
                for msg_id, fields in entries:
                    parsed.append({
                        "id": msg_id,
                        "data": json.loads(fields.get("data", "{}")),
                        "produced_at": fields.get("produced_at"),
                    })
                    await self.client.xack(stream, group, msg_id)

        return {
            "broker": self.name,
            "pattern": "stream_group",
            "destination": stream,
            "group": group,
            "consumer": consumer,
            "messages": parsed,
            "count": len(parsed),
        }

    async def stream_info(self, stream: str) -> dict:
        """Stream: 상세 정보 조회"""
        try:
            info = await self.client.xinfo_stream(stream)
            groups_raw = await self.client.xinfo_groups(stream)
            groups = [
                {
                    "name": g.get("name"),
                    "consumers": g.get("consumers"),
                    "pending": g.get("pending"),
                    "last_delivered_id": g.get("last-delivered-id"),
                }
                for g in groups_raw
            ]
            return {
                "stream": stream,
                "length": info.get("length"),
                "first_entry": info.get("first-entry"),
                "last_entry": info.get("last-entry"),
                "groups": groups,
            }
        except Exception as e:
            return {"stream": stream, "error": str(e)}

    async def benchmark_stream(self, stream: str, count: int = 1000) -> dict:
        """Stream 벤치마크"""
        sw = Stopwatch()
        sw.start()

        for i in range(count):
            with sw.lap():
                await self.client.xadd(
                    stream,
                    {"data": json.dumps({"seq": i}), "produced_at": str(time.time())},
                )

        sw.stop()
        await self.client.xtrim(stream, maxlen=100)
        return sw.report(broker="redis_stream", count=count)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Redis 고유 기능: List Queue
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="redis_queue", operation="publish")
    async def queue_push(self, queue: str, message: dict) -> dict:
        """List Queue: 작업 등록 (LPUSH)"""
        payload = json.dumps({
            **message,
            "job_id": str(uuid.uuid4())[:8],
            "queued_at": time.time(),
        })
        length = await self.client.lpush(queue, payload)

        return {
            "broker": self.name,
            "pattern": "queue",
            "destination": queue,
            "queue_length": length,
        }

    @measure_time(broker="redis_queue", operation="consume", record_metric=False)
    async def queue_pop(self, queue: str, timeout: int = 1) -> dict:
        """List Queue: 작업 가져오기 (BRPOP)"""
        result = await self.client.brpop(queue, timeout=timeout)

        if result:
            _key, value = result
            data = json.loads(value)
            remaining = await self.client.llen(queue)
            return {
                "broker": self.name,
                "pattern": "queue",
                "destination": queue,
                "message": data,
                "remaining": remaining,
            }
        return {
            "broker": self.name,
            "pattern": "queue",
            "destination": queue,
            "message": None,
            "note": f"{timeout}초 대기 후 타임아웃",
        }

    async def queue_length(self, queue: str) -> dict:
        """List Queue: 큐 길이 조회"""
        length = await self.client.llen(queue)
        return {"queue": queue, "length": length}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Redis 고유 기능: Cache
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @measure_time(broker="redis_cache", operation="publish")
    async def cache_set(self, key: str, value: dict, ttl: int = 60) -> dict:
        """Cache: 값 저장 (TTL 기본 60초)"""
        payload = json.dumps(value)
        await self.client.set(key, payload, ex=ttl)

        return {
            "broker": self.name,
            "pattern": "cache",
            "operation": "SET",
            "key": key,
            "ttl_seconds": ttl,
        }

    @measure_time(broker="redis_cache", operation="consume", record_metric=False)
    async def cache_get(self, key: str) -> dict:
        """Cache: 값 조회 (hit/miss 반환)"""
        raw = await self.client.get(key)

        if raw:
            ttl = await self.client.ttl(key)
            return {
                "broker": self.name,
                "pattern": "cache",
                "operation": "GET",
                "key": key,
                "hit": True,
                "value": json.loads(raw),
                "remaining_ttl": ttl,
            }
        return {
            "broker": self.name,
            "pattern": "cache",
            "operation": "GET",
            "key": key,
            "hit": False,
            "value": None,
        }

    async def cache_delete(self, key: str) -> dict:
        """Cache: 키 삭제 (캐시 무효화)"""
        deleted = await self.client.delete(key)
        return {
            "broker": self.name,
            "pattern": "cache",
            "operation": "DELETE",
            "key": key,
            "deleted": bool(deleted),
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Redis 고유 기능: Rate Limiter
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def rate_limit_check(
        self, key: str, max_requests: int = 10, window_seconds: int = 60
    ) -> dict:
        """Rate Limiter: 요청 가능 여부 확인 + 기록"""
        now = time.time()
        window_start = now - window_seconds
        redis_key = f"ratelimit:{key}"

        pipe = self.client.pipeline()
        pipe.zremrangebyscore(redis_key, 0, window_start)
        pipe.zcard(redis_key)
        pipe.zadd(redis_key, {f"{now}:{uuid.uuid4().hex[:6]}": now})
        pipe.expire(redis_key, window_seconds)
        results = await pipe.execute()

        current_count = results[1]
        allowed = current_count < max_requests

        return {
            "broker": self.name,
            "pattern": "rate_limiter",
            "key": key,
            "allowed": allowed,
            "current_requests": current_count,
            "max_requests": max_requests,
            "window_seconds": window_seconds,
            "remaining": max(0, max_requests - current_count),
        }


# 싱글턴 인스턴스
redis_broker = RedisBroker()
