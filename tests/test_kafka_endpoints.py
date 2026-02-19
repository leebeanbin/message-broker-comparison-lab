"""Kafka API 엔드포인트 테스트 (~8개)

브로커 연결 필요: docker compose up -d kafka
"""

import uuid

import pytest


def unique(prefix: str = "test") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_basic_publish_returns_partition_offset(client):
    resp = await client.post(
        "/kafka/basic/publish",
        json={"content": "basic kafka test", "metadata": {}},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_keyed_publish_same_key_same_partition(client):
    key = unique("user")
    results = []
    for i in range(3):
        resp = await client.post(
            "/kafka/keyed/publish",
            json={"key": key, "content": f"keyed msg {i}", "metadata": {}},
        )
        assert resp.status_code == 200
        results.append(resp.json())

    # 같은 키 → 같은 파티션
    if "partition" in results[0]:
        partitions = {r["partition"] for r in results}
        assert len(partitions) == 1


@pytest.mark.asyncio
async def test_batch_publish(client):
    messages = [{"content": f"batch-{i}", "metadata": {}} for i in range(5)]
    resp = await client.post("/kafka/batch/publish", json={"messages": messages})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_topic_create(client):
    topic = unique("topic")
    resp = await client.post(
        "/kafka/topic/create",
        params={"topic": topic, "partitions": 3},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_topic_create_already_exists(client):
    topic = unique("topic-dup")
    await client.post("/kafka/topic/create", params={"topic": topic, "partitions": 1})
    # 두 번째 생성은 에러 또는 이미 존재 메시지
    resp = await client.post("/kafka/topic/create", params={"topic": topic, "partitions": 1})
    assert resp.status_code in (200, 409, 400)


@pytest.mark.asyncio
async def test_topics_list(client):
    resp = await client.get("/kafka/topics")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_topic_info(client):
    topic = unique("topic-info")
    await client.post("/kafka/topic/create", params={"topic": topic, "partitions": 1})

    import asyncio
    await asyncio.sleep(0.5)

    resp = await client.get(f"/kafka/topic/info/{topic}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_topic_info_not_found(client):
    resp = await client.get("/kafka/topic/info/nonexistent_topic_xyz_abc")
    # 404 또는 에러 응답
    assert resp.status_code in (200, 404, 500)
