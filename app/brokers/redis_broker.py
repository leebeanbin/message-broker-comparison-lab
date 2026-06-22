"""
Redis 메시지 브로커 + 고급 기능
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
지원 패턴:
  1. Pub/Sub       - fire-and-forget 실시간 브로드캐스트
  2. Stream        - 영속성 있는 이벤트 로그 (Kafka-like)
  3. List Queue    - 간단한 작업 큐 (LPUSH/BRPOP)
  4. Cache         - TTL 기반 캐시 (SET/GET)
  5. Rate Limiter  - 슬라이딩 윈도우 속도 제한
  6. Bloom Filter  - 확률적 중복 제거 (Redis BITFIELD 기반)
  7. TimeSeries    - 시계열 저장/조회 (Redis Sorted Set 기반)
  8. Vector Set    - 코사인 유사도 검색 (Redis 8.0+ VADD/VSIM)
"""

import hashlib
import json
import math
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


    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Redis 고유 기능: Bloom Filter (BITFIELD 기반)
    #   Redis Stack 없이 표준 Redis BITFIELD로 구현.
    #   false positive 가능, false negative 불가.
    #   용도: 메시지 중복 처리 방지, 이미 처리한 ID 추적
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _bf_hash_positions(self, item: str, bit_size: int, num_hashes: int) -> list[int]:
        """여러 해시 함수로 비트 위치 계산 (double hashing 기법)"""
        h1 = int(hashlib.md5(item.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(item.encode()).hexdigest(), 16)
        return [(h1 + i * h2) % bit_size for i in range(num_hashes)]

    async def bloom_add(
        self, filter_key: str, item: str, capacity: int = 10000, error_rate: float = 0.01
    ) -> dict:
        """
        Bloom Filter: 항목 추가.
        capacity: 예상 항목 수, error_rate: 허용 false positive 비율
        """
        bit_size = math.ceil(-capacity * math.log(error_rate) / (math.log(2) ** 2))
        num_hashes = max(1, round((bit_size / capacity) * math.log(2)))

        positions = self._bf_hash_positions(item, bit_size, num_hashes)
        pipe = self.client.pipeline()
        for pos in positions:
            pipe.setbit(f"bf:{filter_key}", pos, 1)
        await pipe.execute()

        return {
            "filter": filter_key,
            "item": item,
            "operation": "ADD",
            "bit_positions": positions[:3],
            "bit_size": bit_size,
            "num_hashes": num_hashes,
        }

    async def bloom_exists(
        self, filter_key: str, item: str, capacity: int = 10000, error_rate: float = 0.01
    ) -> dict:
        """
        Bloom Filter: 항목 존재 여부 확인.
        결과가 False이면 확실히 없음. True이면 있을 수도 있음 (false positive 가능).
        """
        bit_size = math.ceil(-capacity * math.log(error_rate) / (math.log(2) ** 2))
        num_hashes = max(1, round((bit_size / capacity) * math.log(2)))

        positions = self._bf_hash_positions(item, bit_size, num_hashes)
        pipe = self.client.pipeline()
        for pos in positions:
            pipe.getbit(f"bf:{filter_key}", pos)
        bits = await pipe.execute()

        exists = all(bits)
        return {
            "filter": filter_key,
            "item": item,
            "operation": "EXISTS",
            "result": exists,
            "note": "false positive 가능 (false negative 불가)" if exists else "확실히 없음",
        }

    async def bloom_info(self, filter_key: str) -> dict:
        """Bloom Filter 비트 배열 크기 조회"""
        key = f"bf:{filter_key}"
        bit_count = await self.client.bitcount(key)
        byte_size = await self.client.strlen(key)
        return {
            "filter": filter_key,
            "set_bits": bit_count,
            "byte_size": byte_size,
            "bit_size": byte_size * 8,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Redis 고유 기능: TimeSeries (Sorted Set 기반)
    #   Redis TimeSeries 모듈 없이 Sorted Set으로 구현.
    #   score=timestamp(ms), member=JSON 값
    #   용도: AI 응답 시간, 브로커 처리량, 사용자 활동 추적
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def ts_add(
        self, series_key: str, value: float, timestamp_ms: int | None = None
    ) -> dict:
        """TimeSeries: 데이터 포인트 추가 (score=타임스탬프, member=값)"""
        ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        member = f"{ts}:{value}"
        await self.client.zadd(f"ts:{series_key}", {member: ts})

        return {
            "series": series_key,
            "timestamp_ms": ts,
            "value": value,
            "operation": "ADD",
        }

    async def ts_range(
        self,
        series_key: str,
        from_ms: int | None = None,
        to_ms: int | None = None,
        count: int = 100,
    ) -> dict:
        """TimeSeries: 시간 범위 조회 (ZRANGEBYSCORE)"""
        now_ms = int(time.time() * 1000)
        from_ms = from_ms if from_ms is not None else now_ms - 3_600_000  # 기본: 1시간
        to_ms = to_ms if to_ms is not None else now_ms

        raw = await self.client.zrangebyscore(
            f"ts:{series_key}", from_ms, to_ms, withscores=True, start=0, num=count
        )

        points = []
        for member, score in raw:
            try:
                _, val = member.split(":", 1)
                points.append({"timestamp_ms": int(score), "value": float(val)})
            except ValueError:
                continue

        return {
            "series": series_key,
            "from_ms": from_ms,
            "to_ms": to_ms,
            "count": len(points),
            "points": points,
        }

    async def ts_latest(self, series_key: str, n: int = 10) -> dict:
        """TimeSeries: 최근 N개 데이터 포인트"""
        raw = await self.client.zrange(
            f"ts:{series_key}", -n, -1, withscores=True
        )
        points = []
        for member, score in raw:
            try:
                _, val = member.split(":", 1)
                points.append({"timestamp_ms": int(score), "value": float(val)})
            except ValueError:
                continue

        return {"series": series_key, "count": len(points), "points": points}

    async def ts_trim(self, series_key: str, max_age_seconds: int = 86400) -> dict:
        """TimeSeries: 오래된 데이터 삭제 (보존 정책)"""
        cutoff_ms = int((time.time() - max_age_seconds) * 1000)
        removed = await self.client.zremrangebyscore(f"ts:{series_key}", 0, cutoff_ms)
        return {
            "series": series_key,
            "removed_points": removed,
            "cutoff_ms": cutoff_ms,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Redis 8.0+ 기능: Vector Set (VADD/VSIM)
    #   코사인 유사도 기반 최근접 이웃 검색.
    #   용도: AI Semantic Memory, RAG 유사도 검색
    #   요구사항: Redis 8.0+ (VADD 명령어)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def vector_add(
        self, vset_key: str, element_id: str, vector: list[float]
    ) -> dict:
        """Vector Set: 벡터 추가 (VADD)"""
        import struct
        dim = len(vector)
        fp32_bytes = struct.pack(f"{dim}f", *vector)
        try:
            await self.client.execute_command(
                "VADD", vset_key, "REDUCE", dim, "FP32", fp32_bytes, element_id
            )
            return {
                "vset": vset_key,
                "element_id": element_id,
                "dimensions": dim,
                "operation": "VADD",
            }
        except Exception as e:
            return {"error": str(e), "note": "Redis 8.0+ 필요 (VADD 명령어)"}

    async def vector_search(
        self, vset_key: str, query_vector: list[float], top_k: int = 5
    ) -> dict:
        """Vector Set: 코사인 유사도 검색 (VSIM)"""
        import struct
        dim = len(query_vector)
        fp32_bytes = struct.pack(f"{dim}f", *query_vector)
        try:
            raw = await self.client.execute_command(
                "VSIM", vset_key, "FP32", fp32_bytes, "COUNT", top_k, "WITHSCORES"
            )
            results = []
            for i in range(0, len(raw), 2):
                element_id = raw[i].decode() if isinstance(raw[i], bytes) else raw[i]
                score = float(raw[i + 1])
                results.append({"id": element_id, "similarity": score})
            return {
                "vset": vset_key,
                "top_k": top_k,
                "results": results,
                "operation": "VSIM",
            }
        except Exception as e:
            return {"error": str(e), "note": "Redis 8.0+ 필요 (VSIM 명령어)"}


# 싱글턴 인스턴스
redis_broker = RedisBroker()
