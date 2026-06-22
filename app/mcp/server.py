"""
MCP (Model Context Protocol) 서버
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
이 랩의 3개 브로커를 AI가 직접 사용할 수 있는 MCP 도구로 노출.

도구 목록:
  [메모리] memory_set / memory_get / memory_delete / memory_search
  [스트림] stream_publish / stream_read
  [이벤트] kafka_emit / kafka_topic_info
  [태스크] task_enqueue / task_queue_info
  [제한]   rate_limit_check
  [모니터] broker_health / run_benchmark
  [벡터]   vector_store / vector_search

연결 방법:
  1. FastAPI SSE (웹): http://localhost:8000/mcp/sse
  2. stdio (Claude Desktop/Code): uv run mcp_server.py

Claude Desktop ~/.claude/claude_desktop_config.json:
  {
    "mcpServers": {
      "broker-lab": {
        "url": "http://localhost:8000/mcp/sse"
      }
    }
  }

Claude Code .claude/settings.json (또는 global):
  {
    "mcpServers": {
      "broker-lab": {
        "type": "sse",
        "url": "http://localhost:8000/mcp/sse"
      }
    }
  }
"""

import json

from mcp.server.fastmcp import FastMCP

# MCP 인스턴스 — FastAPI에 마운트하거나 독립 실행 가능
mcp = FastMCP(
    name="Message Broker Lab",
    instructions=(
        "메시지 브로커 학습 랩 MCP 서버. "
        "Redis(메모리/캐시/벡터), Kafka(이벤트 스트림), RabbitMQ(태스크 큐)를 "
        "AI 도구로 제공합니다. "
        "브로커가 실행 중이어야 합니다: docker compose up -d"
    ),
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [메모리] Redis Cache 기반 AI 컨텍스트 저장소
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
async def memory_set(key: str, value: str, ttl_seconds: int = 3600) -> str:
    """
    AI 컨텍스트/메모리를 Redis에 저장합니다.

    Args:
        key: 메모리 식별자 (예: "user:123:preference", "session:abc:history")
        value: 저장할 값 (문자열 또는 JSON 문자열)
        ttl_seconds: 만료 시간 (기본 1시간, 0이면 영구 저장)
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected. Run: docker compose up redis -d"
    try:
        if ttl_seconds > 0:
            await redis_broker.client.set(f"mcp:memory:{key}", value, ex=ttl_seconds)
        else:
            await redis_broker.client.set(f"mcp:memory:{key}", value)
        return f"Stored '{key}' (TTL: {ttl_seconds}s)"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def memory_get(key: str) -> str:
    """
    Redis에서 AI 컨텍스트/메모리를 조회합니다.

    Args:
        key: 메모리 식별자
    Returns:
        저장된 값, 없으면 null 반환
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        value = await redis_broker.client.get(f"mcp:memory:{key}")
        ttl = await redis_broker.client.ttl(f"mcp:memory:{key}")
        if value is None:
            return f"Key '{key}' not found"
        return json.dumps({"key": key, "value": value, "ttl_remaining": ttl})
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def memory_delete(key: str) -> str:
    """
    Redis에서 특정 메모리를 삭제합니다.

    Args:
        key: 삭제할 메모리 식별자
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        deleted = await redis_broker.client.delete(f"mcp:memory:{key}")
        return f"Deleted '{key}': {'success' if deleted else 'key not found'}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def memory_list(pattern: str = "*", limit: int = 20) -> str:
    """
    저장된 메모리 키 목록을 조회합니다.

    Args:
        pattern: 키 패턴 (예: "user:*", "session:abc:*")
        limit: 최대 반환 개수
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        keys = []
        async for key in redis_broker.client.scan_iter(f"mcp:memory:{pattern}", count=100):
            keys.append(key.removeprefix("mcp:memory:"))
            if len(keys) >= limit:
                break
        return json.dumps({"keys": keys, "count": len(keys)})
    except Exception as e:
        return f"Error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [벡터] Redis Vector Set — Semantic Memory
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
async def vector_store(
    vset_key: str, element_id: str, text: str, vector: list[float]
) -> str:
    """
    텍스트와 벡터를 Redis Vector Set에 저장합니다 (AI 시맨틱 메모리).
    Redis 8.0+ 필요. 벡터는 사전에 임베딩 모델로 생성해야 합니다.

    Args:
        vset_key: 벡터 세트 이름 (예: "ai:memories", "rag:docs")
        element_id: 요소 식별자 (예: "doc_001", "memory_abc")
        text: 원본 텍스트 (메타데이터로 별도 저장)
        vector: FP32 임베딩 벡터
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        result = await redis_broker.vector_add(vset_key, element_id, vector)
        if "error" not in result:
            await redis_broker.client.set(
                f"vec:text:{vset_key}:{element_id}", text
            )
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def vector_search(vset_key: str, query_vector: list[float], top_k: int = 5) -> str:
    """
    Redis Vector Set에서 코사인 유사도 기반 시맨틱 검색 (Redis 8.0+).
    가장 유사한 top_k개 요소와 원본 텍스트를 반환합니다.

    Args:
        vset_key: 검색할 벡터 세트
        query_vector: 쿼리 임베딩 벡터
        top_k: 반환할 최대 결과 수
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        result = await redis_broker.vector_search(vset_key, query_vector, top_k)
        if "error" in result:
            return json.dumps(result)
        # 원본 텍스트 조회
        for item in result.get("results", []):
            text = await redis_broker.client.get(
                f"vec:text:{vset_key}:{item['id']}"
            )
            item["text"] = text or "(no text stored)"
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [스트림] Redis Stream
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
async def stream_publish(stream: str, data: dict) -> str:
    """
    Redis Stream에 이벤트를 발행합니다 (영속적, 순서 보장).
    LLM 응답 토큰 스트리밍, AI 처리 결과 이벤트 소싱에 활용.

    Args:
        stream: 스트림 이름 (예: "ai:responses", "events:order")
        data: 발행할 데이터 (dict)
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        result = await redis_broker.stream_add(stream, data)
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def stream_read(stream: str, count: int = 10, last_id: str = "0") -> str:
    """
    Redis Stream에서 이벤트를 읽습니다.

    Args:
        stream: 스트림 이름
        count: 읽을 최대 메시지 수
        last_id: 이 ID 이후 메시지만 반환 (페이징)
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        result = await redis_broker.stream_read(stream, count, last_id)
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [이벤트] Kafka
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
async def kafka_emit(topic: str, payload: dict, key: str | None = None) -> str:
    """
    Kafka 토픽에 이벤트를 발행합니다.
    대용량 이벤트 스트림, AI 파이프라인 결과 전달, 감사 로그에 활용.

    Args:
        topic: Kafka 토픽 이름 (예: "ai-results", "user-events")
        payload: 발행할 데이터
        key: 파티션 키 (같은 키는 같은 파티션 → 순서 보장)
    """
    from app.brokers import kafka_broker
    if not kafka_broker.is_connected:
        return "Kafka not connected. Run: docker compose up kafka -d"
    try:
        if key:
            result = await kafka_broker.publish_keyed(topic, key, payload)
        else:
            result = await kafka_broker.publish(topic, payload)
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def kafka_topic_info(topic: str) -> str:
    """
    Kafka 토픽의 파티션 수, 오프셋 정보를 조회합니다.

    Args:
        topic: 조회할 토픽 이름
    """
    from app.brokers import kafka_broker
    if not kafka_broker.is_connected:
        return "Kafka not connected"
    try:
        result = await kafka_broker.topic_info(topic)
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [태스크] RabbitMQ Task Queue
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
async def task_enqueue(queue: str, task: dict, priority: int = 0) -> str:
    """
    RabbitMQ 큐에 작업을 등록합니다.
    AI Agent 병렬 처리, 백그라운드 작업 분배에 활용.

    Args:
        queue: 큐 이름 (예: "ai-tasks", "embed-queue")
        task: 작업 데이터 (dict)
        priority: 우선순위 0~10 (높을수록 먼저 처리)
    """
    from app.brokers import rabbitmq_broker
    if not rabbitmq_broker.is_connected:
        return "RabbitMQ not connected. Run: docker compose up rabbitmq -d"
    try:
        if priority > 0:
            result = await rabbitmq_broker.publish_priority(queue, task, priority)
        else:
            result = await rabbitmq_broker.publish(queue, task)
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def task_queue_info(queue: str) -> str:
    """
    RabbitMQ 큐의 현재 메시지 수, 소비자 수를 조회합니다.

    Args:
        queue: 큐 이름
    """
    from app.brokers import rabbitmq_broker
    if not rabbitmq_broker.is_connected:
        return "RabbitMQ not connected"
    try:
        result = await rabbitmq_broker.queue_info(queue)
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [제한] Rate Limiting
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
async def rate_limit_check(
    user_id: str, max_requests: int = 20, window_seconds: int = 60
) -> str:
    """
    특정 사용자/키의 요청 가능 여부를 확인하고 카운트합니다.
    AI API 호출 제한, 사용자별 쿼터 관리에 활용.

    Args:
        user_id: 제한 대상 식별자 (예: "user:123", "api-key:abc")
        max_requests: 윈도우 내 최대 요청 수
        window_seconds: 슬라이딩 윈도우 크기 (초)
    Returns:
        allowed(bool), current_requests(int), remaining(int) 포함
    """
    from app.brokers import redis_broker
    if not redis_broker.is_connected:
        return "Redis not connected"
    try:
        result = await redis_broker.rate_limit_check(
            user_id, max_requests, window_seconds
        )
        return json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [모니터] 상태 + 성능
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
async def broker_health() -> str:
    """
    모든 브로커(Redis, RabbitMQ, Kafka)의 연결 상태와
    Circuit Breaker 상태를 한번에 조회합니다.
    """
    from app.brokers import kafka_broker, rabbitmq_broker, redis_broker
    from app.resilience import circuit_breakers

    health = {
        "redis":    {"connected": redis_broker.is_connected},
        "rabbitmq": {"connected": rabbitmq_broker.is_connected},
        "kafka":    {"connected": kafka_broker.is_connected},
    }

    for name, cb in circuit_breakers.items():
        if name in health:
            health[name]["circuit_breaker"] = cb.state.value

    return json.dumps(health)


@mcp.tool()
async def run_benchmark(broker: str = "redis", message_count: int = 500) -> str:
    """
    특정 브로커의 성능 벤치마크를 실행합니다.
    처리량(msg/s), 평균/P99 레이턴시를 반환합니다.

    Args:
        broker: "redis", "rabbitmq", "kafka" 중 하나
        message_count: 벤치마크 메시지 수 (1~5000)
    """
    from app.brokers import kafka_broker, rabbitmq_broker, redis_broker

    message_count = max(1, min(5000, message_count))
    broker_map = {
        "redis":    (redis_broker,    "bench-channel"),
        "rabbitmq": (rabbitmq_broker, "bench-queue"),
        "kafka":    (kafka_broker,    "bench-topic"),
    }

    if broker not in broker_map:
        return f"Unknown broker '{broker}'. Choose: redis, rabbitmq, kafka"

    b, destination = broker_map[broker]
    if not b.is_connected:
        return f"{broker} not connected"

    try:
        result = await b.benchmark(destination, message_count)
        return json.dumps({
            k: v for k, v in result.items()
            if k in ("broker", "total_messages", "throughput_msg_per_sec",
                     "avg_latency_ms", "p99_latency_ms", "total_ms")
        })
    except Exception as e:
        return f"Benchmark error: {e}"
