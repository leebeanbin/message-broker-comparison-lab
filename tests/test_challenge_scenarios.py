"""과제 시나리오 E2E 테스트 (~7개)

각 과제 노트북(11-17)의 핵심 플로우를 검증.
모든 브로커 연결 필요: docker compose up -d redis rabbitmq kafka
"""

import asyncio
import uuid

import pytest


def unique(prefix: str = "test") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 11. 결제 시스템 플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_payment_flow(client):
    """Rate Limit → Priority Queue → Cache → Kafka Event"""
    user_key = unique("pay-user")

    # 1. Rate limit check
    resp = await client.get(
        "/redis/ratelimit/check",
        params={"key": user_key, "max_requests": 5, "window_seconds": 60},
    )
    assert resp.status_code == 200
    assert resp.json()["allowed"] is True

    # 2. Priority queue publish
    resp = await client.post(
        "/rabbitmq/priority/publish",
        json={"content": "결제 요청: 350000원", "priority": 9, "metadata": {"user": user_key}},
    )
    assert resp.status_code == 200

    # 3. Cache: 재고 차감 시뮬레이션
    stock_key = unique("stock")
    resp = await client.post(
        "/redis/cache/set",
        json={"key": stock_key, "value": {"remaining": 9}, "ttl": 300},
    )
    assert resp.status_code == 200

    resp = await client.get(f"/redis/cache/get/{stock_key}")
    assert resp.json()["value"]["remaining"] == 9

    # 4. Kafka: 결제 이벤트 발행
    resp = await client.post(
        "/kafka/keyed/publish",
        json={"key": user_key, "content": "payment_completed", "metadata": {"amount": 350000}},
    )
    assert resp.status_code == 200


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 12. 티켓 예매 플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_ticket_booking_flow(client):
    """Stream → Consumer Group → Cache → Fanout"""
    # 1. Stream: 예매 요청
    resp = await client.post(
        "/redis/stream/add",
        json={"content": "티켓 요청: VIP석 1매", "metadata": {"user": "fan_001"}},
    )
    assert resp.status_code == 200

    # 2. Consumer Group 생성 + 읽기
    group = unique("ticket-grp")
    await client.post("/redis/stream/group/create", params={"group": group})
    resp = await client.get(
        "/redis/stream/group/read",
        params={"group": group, "consumer": "worker-1", "count": 5},
    )
    assert resp.status_code == 200

    # 3. Cache: 좌석 차감
    seat_key = unique("seats")
    await client.post(
        "/redis/cache/set",
        json={"key": seat_key, "value": {"available": 49}, "ttl": 300},
    )

    # 4. Fanout: 알림
    queue = unique("ticket-notify")
    await client.post("/rabbitmq/fanout/bind", params={"queue_name": queue})
    resp = await client.post(
        "/rabbitmq/fanout/publish",
        json={"content": "좌석 업데이트: 49석 남음", "metadata": {}},
    )
    assert resp.status_code == 200


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 13. 채팅 플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_chat_flow(client):
    """Pub/Sub → Stream 이력 → Topic 멘션"""
    # 1. Pub/Sub: 실시간 메시지
    resp = await client.post(
        "/redis/pubsub/publish",
        json={"content": "안녕하세요!", "metadata": {"room": "room_001"}},
    )
    assert resp.status_code == 200

    # 2. Stream: 이력 저장
    resp = await client.post(
        "/redis/stream/add",
        json={"content": "안녕하세요!", "metadata": {"room": "room_001", "sender": "user_001"}},
    )
    assert resp.status_code == 200

    # 3. Stream 읽기
    resp = await client.get("/redis/stream/read", params={"count": 5, "last_id": "0"})
    assert resp.status_code == 200

    # 4. Topic: 멘션 알림
    queue = unique("mention-q")
    await client.post(
        "/rabbitmq/topic/bind",
        params={"queue_name": queue, "binding_key": "chat.mention.*"},
    )
    resp = await client.post(
        "/rabbitmq/topic/publish",
        json={"routing_key": "chat.mention.user_002", "content": "@개발새발 확인 부탁", "metadata": {}},
    )
    assert resp.status_code == 200


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 14. 대용량 처리 플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_bulk_flow(client):
    """개별 publish vs batch publish 비교"""
    # 개별 발행
    for i in range(5):
        resp = await client.post(
            "/kafka/basic/publish",
            json={"content": f"individual-{i}", "metadata": {}},
        )
        assert resp.status_code == 200

    # 배치 발행
    messages = [{"content": f"batch-{i}", "metadata": {}} for i in range(5)]
    resp = await client.post("/kafka/batch/publish", json={"messages": messages})
    assert resp.status_code == 200


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 15. Saga 패턴 플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_saga_flow(client):
    """상태 추적 → 이벤트 발행 → 보상 트랜잭션"""
    order_id = unique("saga")

    # 1. 각 단계 상태 저장
    steps = ["create_order", "process_payment", "reserve_inventory"]
    for step in steps:
        resp = await client.post(
            "/redis/cache/set",
            json={"key": f"{order_id}:{step}", "value": {"status": "completed"}, "ttl": 300},
        )
        assert resp.status_code == 200

        # 이벤트 발행
        resp = await client.post(
            "/rabbitmq/direct/publish",
            json={"content": f"{step} completed", "metadata": {"order_id": order_id}},
        )
        assert resp.status_code == 200

    # 2. 상태 확인
    resp = await client.get(f"/redis/cache/get/{order_id}:process_payment")
    assert resp.json()["hit"] is True
    assert resp.json()["value"]["status"] == "completed"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 16. 배달 추적 플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_delivery_flow(client):
    """TTL → Topic → Pub/Sub"""
    # 1. TTL: 시간 제한 메시지
    resp = await client.post(
        "/rabbitmq/ttl/publish",
        json={"content": "주문 접수 - 1분 내 확인 필요", "ttl_ms": 5000, "metadata": {}},
    )
    assert resp.status_code == 200

    # 2. Topic: 상태별 라우팅
    queue = unique("delivery-q")
    await client.post(
        "/rabbitmq/topic/bind",
        params={"queue_name": queue, "binding_key": "delivery.status.*"},
    )
    resp = await client.post(
        "/rabbitmq/topic/publish",
        json={"routing_key": "delivery.status.cooking", "content": "조리 시작", "metadata": {}},
    )
    assert resp.status_code == 200

    # 3. Pub/Sub: 실시간 알림
    resp = await client.post(
        "/redis/pubsub/publish",
        json={"content": "배달 출발!", "metadata": {"delivery_id": "DLV-001"}},
    )
    assert resp.status_code == 200


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 17. 이미지 파이프라인 플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_image_pipeline_flow(client):
    """Direct publish → Cache 결과 저장"""
    task_id = unique("img")

    # 1. 이미지 처리 요청 (RabbitMQ)
    resp = await client.post(
        "/rabbitmq/direct/publish",
        json={
            "content": f"이미지 처리 요청: {task_id}",
            "metadata": {"task_id": task_id, "format": "jpeg", "width": 800},
        },
    )
    assert resp.status_code == 200

    # 2. 결과 캐시 저장 (실제로는 Go 서비스가 하지만 테스트에서 시뮬레이션)
    resp = await client.post(
        "/redis/cache/set",
        json={
            "key": f"img-result:{task_id}",
            "value": {"status": "completed", "url": f"https://cdn.example.com/{task_id}.jpg"},
            "ttl": 3600,
        },
    )
    assert resp.status_code == 200

    resp = await client.get(f"/redis/cache/get/img-result:{task_id}")
    assert resp.json()["hit"] is True
    assert resp.json()["value"]["status"] == "completed"
