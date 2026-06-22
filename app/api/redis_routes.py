"""
Redis 엔드포인트 (Pub/Sub, Stream, Queue, Cache, Rate Limiter, Bloom Filter, TimeSeries, Vector Set)
"""

from fastapi import APIRouter, HTTPException, Query

from app.brokers import redis_broker
from app.schemas import (
    BloomAddRequest,
    BloomExistsRequest,
    CacheRequest,
    KVSetRequest,
    ListPushRequest,
    MessageRequest,
    TimeSeriesAddRequest,
    VectorAddRequest,
    VectorSearchRequest,
)

router = APIRouter(prefix="/redis", tags=["Redis"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pub/Sub
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/pubsub/publish", tags=["Redis - Pub/Sub"])
async def pubsub_publish(msg: MessageRequest):
    """Redis Pub/Sub: 채널에 메시지 발행 (fire-and-forget)"""
    try:
        return await redis_broker.publish(
            "test-channel", {"content": msg.content, **msg.metadata}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis Pub/Sub 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Stream
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/stream/add", tags=["Redis - Stream"])
async def stream_add(msg: MessageRequest):
    """Redis Stream: 이벤트 추가 (영속적, Kafka-like)"""
    try:
        return await redis_broker.stream_add(
            "test-stream", {"content": msg.content, **msg.metadata}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis Stream 실패: {e}")


@router.get("/stream/read", tags=["Redis - Stream"])
async def stream_read(
    count: int = Query(default=10, ge=1, le=100),
    last_id: str = Query(default="0", description="이 ID 이후 메시지 조회"),
):
    """Redis Stream: 이벤트 읽기 (특정 ID 이후)"""
    try:
        return await redis_broker.stream_read(
            "test-stream", count=count, last_id=last_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stream 읽기 실패: {e}")


@router.post("/stream/group/create", tags=["Redis - Stream"])
async def stream_group_create(
    group: str = Query(default="my-group", description="Consumer Group 이름"),
):
    """Redis Stream: Consumer Group 생성"""
    try:
        return await redis_broker.stream_create_group("test-stream", group)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Group 생성 실패: {e}")


@router.get("/stream/group/read", tags=["Redis - Stream"])
async def stream_group_read(
    group: str = Query(default="my-group"),
    consumer: str = Query(default="consumer-1"),
    count: int = Query(default=10, ge=1, le=100),
):
    """Redis Stream: Consumer Group으로 읽기 (분산 처리)"""
    try:
        return await redis_broker.stream_read_group(
            "test-stream", group, consumer, count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Group 읽기 실패: {e}")


@router.get("/stream/info", tags=["Redis - Stream"])
async def stream_info():
    """Redis Stream: 스트림 상세 정보"""
    try:
        return await redis_broker.stream_info("test-stream")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stream 정보 조회 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# List Queue
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/queue/push", tags=["Redis - Queue"])
async def queue_push(msg: MessageRequest):
    """Redis Queue: 작업 등록 (LPUSH)"""
    try:
        return await redis_broker.queue_push(
            "test-queue", {"content": msg.content, **msg.metadata}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Queue push 실패: {e}")


@router.get("/queue/pop", tags=["Redis - Queue"])
async def queue_pop():
    """Redis Queue: 작업 가져오기 (BRPOP)"""
    try:
        return await redis_broker.queue_pop("test-queue", timeout=2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Queue pop 실패: {e}")


@router.get("/queue/length", tags=["Redis - Queue"])
async def queue_length():
    """Redis Queue: 큐 길이 조회"""
    try:
        return await redis_broker.queue_length("test-queue")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Queue 길이 조회 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cache
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/cache/set", tags=["Redis - Cache"])
async def cache_set(req: CacheRequest):
    """Redis Cache: 값 저장 (TTL 기본 60초)"""
    try:
        return await redis_broker.cache_set(req.key, req.value, req.ttl)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache set 실패: {e}")


@router.get("/cache/get/{key}", tags=["Redis - Cache"])
async def cache_get(key: str):
    """Redis Cache: 값 조회 (hit/miss 확인)"""
    try:
        return await redis_broker.cache_get(key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache get 실패: {e}")


@router.delete("/cache/delete/{key}", tags=["Redis - Cache"])
async def cache_delete(key: str):
    """Redis Cache: 키 삭제 (캐시 무효화)"""
    try:
        return await redis_broker.cache_delete(key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache delete 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# KV (Key-Value) - 단순 문자열 조작
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/kv/set", tags=["Redis - KV"])
async def kv_set(req: KVSetRequest):
    """Redis KV: 문자열 값 저장 (TTL 선택)"""
    try:
        client = redis_broker.client
        if req.ttl > 0:
            await client.set(req.key, req.value, ex=req.ttl)
        else:
            await client.set(req.key, req.value)
        return {"key": req.key, "ttl": req.ttl, "status": "OK"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KV set 실패: {e}")


@router.get("/kv/get/{key:path}", tags=["Redis - KV"])
async def kv_get(key: str):
    """Redis KV: 문자열 값 조회"""
    try:
        client = redis_broker.client
        value = await client.get(key)
        return {"key": key, "value": value, "exists": value is not None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KV get 실패: {e}")


@router.delete("/kv/delete/{key:path}", tags=["Redis - KV"])
async def kv_delete(key: str):
    """Redis KV: 키 삭제"""
    try:
        client = redis_broker.client
        deleted = await client.delete(key)
        return {"key": key, "deleted": bool(deleted)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KV delete 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# List
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/list/push", tags=["Redis - List"])
async def list_push(req: ListPushRequest):
    """Redis List: LPUSH (왼쪽에 추가)"""
    try:
        client = redis_broker.client
        length = await client.lpush(req.key, req.value)
        return {"key": req.key, "length": length}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List push 실패: {e}")


@router.get("/list/range", tags=["Redis - List"])
async def list_range(
    key: str = Query(description="리스트 키"),
    start: int = Query(default=0),
    stop: int = Query(default=-1),
):
    """Redis List: LRANGE (범위 조회)"""
    try:
        client = redis_broker.client
        values = await client.lrange(key, start, stop)
        return {"key": key, "values": values, "count": len(values)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List range 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Rate Limiter
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/ratelimit/check", tags=["Redis - Rate Limiter"])
async def rate_limit_check(
    key: str = Query(default="user:test", description="제한 대상 키"),
    max_requests: int = Query(default=10),
    window_seconds: int = Query(default=60),
):
    """Redis Rate Limiter: 요청 가능 여부 확인 (슬라이딩 윈도우)"""
    try:
        return await redis_broker.rate_limit_check(
            key, max_requests, window_seconds
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Rate limit 체크 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Bloom Filter (Redis BITFIELD 기반 확률적 자료구조)
#   false positive 가능, false negative 불가
#   용도: 메시지 중복 처리 방지, 이미 처리한 ID 추적
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/bloom/add", tags=["Redis - Bloom Filter"])
async def bloom_add(req: BloomAddRequest):
    """Bloom Filter: 항목 추가. 같은 파라미터로 EXISTS를 해야 올바른 결과."""
    try:
        return await redis_broker.bloom_add(
            req.filter_key, req.item, req.capacity, req.error_rate
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bloom ADD 실패: {e}")


@router.post("/bloom/exists", tags=["Redis - Bloom Filter"])
async def bloom_exists(req: BloomExistsRequest):
    """Bloom Filter: 항목 존재 여부 확인 (False=확실히 없음, True=있을 수도 있음)"""
    try:
        return await redis_broker.bloom_exists(
            req.filter_key, req.item, req.capacity, req.error_rate
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bloom EXISTS 실패: {e}")


@router.get("/bloom/info/{filter_key}", tags=["Redis - Bloom Filter"])
async def bloom_info(filter_key: str):
    """Bloom Filter: 필터 크기 및 세트된 비트 수 조회"""
    try:
        return await redis_broker.bloom_info(filter_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bloom INFO 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TimeSeries (Redis Sorted Set 기반)
#   score=타임스탬프(ms), member=값
#   용도: AI 응답 시간, 브로커 처리량, 사용자 활동
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/ts/add", tags=["Redis - TimeSeries"])
async def ts_add(req: TimeSeriesAddRequest):
    """TimeSeries: 데이터 포인트 추가"""
    try:
        return await redis_broker.ts_add(
            req.series_key, req.value, req.timestamp_ms
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"TimeSeries ADD 실패: {e}")


@router.get("/ts/range/{series_key}", tags=["Redis - TimeSeries"])
async def ts_range(
    series_key: str,
    from_ms: int | None = Query(default=None, description="시작 타임스탬프(ms)"),
    to_ms: int | None = Query(default=None, description="종료 타임스탬프(ms)"),
    count: int = Query(default=100, ge=1, le=10000),
):
    """TimeSeries: 시간 범위 조회"""
    try:
        return await redis_broker.ts_range(series_key, from_ms, to_ms, count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"TimeSeries RANGE 실패: {e}")


@router.get("/ts/latest/{series_key}", tags=["Redis - TimeSeries"])
async def ts_latest(
    series_key: str,
    n: int = Query(default=10, ge=1, le=1000),
):
    """TimeSeries: 최근 N개 데이터 포인트"""
    try:
        return await redis_broker.ts_latest(series_key, n)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"TimeSeries LATEST 실패: {e}")


@router.delete("/ts/trim/{series_key}", tags=["Redis - TimeSeries"])
async def ts_trim(
    series_key: str,
    max_age_seconds: int = Query(default=86400, description="이보다 오래된 데이터 삭제"),
):
    """TimeSeries: 보존 정책 적용 (오래된 데이터 삭제)"""
    try:
        return await redis_broker.ts_trim(series_key, max_age_seconds)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"TimeSeries TRIM 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Vector Set (Redis 8.0+ VADD/VSIM)
#   코사인 유사도 기반 최근접 이웃 검색
#   용도: AI Semantic Memory, RAG, 추천
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.post("/vector/add", tags=["Redis - Vector Set"])
async def vector_add(req: VectorAddRequest):
    """Vector Set: 벡터 추가 (Redis 8.0+ VADD)"""
    try:
        return await redis_broker.vector_add(req.vset_key, req.element_id, req.vector)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vector ADD 실패: {e}")


@router.post("/vector/search", tags=["Redis - Vector Set"])
async def vector_search(req: VectorSearchRequest):
    """Vector Set: 코사인 유사도 기반 최근접 이웃 검색 (Redis 8.0+ VSIM)"""
    try:
        return await redis_broker.vector_search(req.vset_key, req.query_vector, req.top_k)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vector SEARCH 실패: {e}")
