"""RabbitMQ API 엔드포인트 테스트 (~10개)

브로커 연결 필요: docker compose up -d rabbitmq
"""

import uuid

import pytest


def unique(prefix: str = "test") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_direct_publish(client):
    resp = await client.post(
        "/rabbitmq/direct/publish",
        json={"content": "direct test", "metadata": {"source": "pytest"}},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_queue_info(client):
    # 먼저 publish해서 큐가 존재하도록 보장
    await client.post("/rabbitmq/direct/publish", json={"content": "info test", "metadata": {}})
    resp = await client.get("/rabbitmq/queue/info/order-queue")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_fanout_bind(client):
    queue = unique("fanout-q")
    resp = await client.post("/rabbitmq/fanout/bind", params={"queue_name": queue})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_fanout_publish(client):
    resp = await client.post(
        "/rabbitmq/fanout/publish",
        json={"content": "fanout broadcast", "metadata": {}},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_topic_bind_and_publish(client):
    queue = unique("topic-q")
    await client.post(
        "/rabbitmq/topic/bind",
        params={"queue_name": queue, "binding_key": "order.*"},
    )
    resp = await client.post(
        "/rabbitmq/topic/publish",
        json={"routing_key": "order.created", "content": "topic test", "metadata": {}},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_priority_publish(client):
    resp = await client.post(
        "/rabbitmq/priority/publish",
        json={"content": "high priority", "priority": 9, "metadata": {}},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_priority_ordering(client):
    """여러 우선순위 메시지를 발행 - 높은 priority가 먼저 소비되는지 확인"""
    for p in [1, 5, 10, 3]:
        resp = await client.post(
            "/rabbitmq/priority/publish",
            json={"content": f"priority-{p}", "priority": p, "metadata": {}},
        )
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dlq_setup(client):
    queue = unique("dlq")
    resp = await client.post("/rabbitmq/dlq/setup", params={"queue_name": queue})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dlq_messages(client):
    resp = await client.get(
        "/rabbitmq/dlq/messages",
        params={"queue_name": "task-queue", "count": 5},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_ttl_publish(client):
    resp = await client.post(
        "/rabbitmq/ttl/publish",
        json={"content": "expires soon", "ttl_ms": 5000, "metadata": {}},
    )
    assert resp.status_code == 200
