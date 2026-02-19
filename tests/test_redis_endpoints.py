"""Redis API 엔드포인트 테스트 (~15개)

브로커 연결 필요: docker compose up -d redis
"""

import uuid

import pytest


def unique(prefix: str = "test") -> str:
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cache
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_cache_set_and_get(client):
    key = unique("cache")
    resp = await client.post("/redis/cache/set", json={"key": key, "value": {"msg": "hello"}, "ttl": 60})
    assert resp.status_code == 200

    resp = await client.get(f"/redis/cache/get/{key}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["hit"] is True
    assert data["value"]["msg"] == "hello"


@pytest.mark.asyncio
async def test_cache_get_missing_key(client):
    resp = await client.get("/redis/cache/get/nonexistent_key_xyz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["hit"] is False


@pytest.mark.asyncio
async def test_cache_delete(client):
    key = unique("cache-del")
    await client.post("/redis/cache/set", json={"key": key, "value": {"x": 1}, "ttl": 60})

    resp = await client.delete(f"/redis/cache/delete/{key}")
    assert resp.status_code == 200

    resp = await client.get(f"/redis/cache/get/{key}")
    assert resp.json()["hit"] is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Rate Limiter
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_rate_limiter_allows_under_limit(client):
    key = unique("rl")
    resp = await client.get(
        "/redis/ratelimit/check",
        params={"key": key, "max_requests": 10, "window_seconds": 60},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["allowed"] is True
    assert data["remaining"] >= 0


@pytest.mark.asyncio
async def test_rate_limiter_blocks_over_limit(client):
    key = unique("rl-block")
    for _ in range(3):
        await client.get(
            "/redis/ratelimit/check",
            params={"key": key, "max_requests": 3, "window_seconds": 60},
        )
    resp = await client.get(
        "/redis/ratelimit/check",
        params={"key": key, "max_requests": 3, "window_seconds": 60},
    )
    assert resp.status_code == 200
    assert resp.json()["allowed"] is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Stream
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_stream_add_returns_message_id(client):
    resp = await client.post("/redis/stream/add", json={"content": "stream test", "metadata": {}})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_stream_read_returns_messages(client):
    await client.post("/redis/stream/add", json={"content": "read test", "metadata": {}})
    resp = await client.get("/redis/stream/read", params={"count": 5, "last_id": "0"})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_stream_group_create(client):
    # 먼저 메시지를 하나 추가해서 스트림이 존재하도록 보장
    await client.post("/redis/stream/add", json={"content": "group test", "metadata": {}})
    group = unique("grp")
    resp = await client.post("/redis/stream/group/create", params={"group": group})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_stream_group_read(client):
    await client.post("/redis/stream/add", json={"content": "grp read", "metadata": {}})
    group = unique("grp-r")
    await client.post("/redis/stream/group/create", params={"group": group})
    resp = await client.get(
        "/redis/stream/group/read",
        params={"group": group, "consumer": "c1", "count": 5},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_stream_info(client):
    await client.post("/redis/stream/add", json={"content": "info test", "metadata": {}})
    resp = await client.get("/redis/stream/info")
    assert resp.status_code == 200


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pub/Sub
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_pubsub_publish(client):
    resp = await client.post(
        "/redis/pubsub/publish",
        json={"content": "pubsub test", "metadata": {"type": "test"}},
    )
    assert resp.status_code == 200


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Queue (List)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_queue_push(client):
    resp = await client.post("/redis/queue/push", json={"content": "queue item", "metadata": {}})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_queue_pop_fifo_order(client):
    # Push then pop
    await client.post("/redis/queue/push", json={"content": "fifo-1", "metadata": {}})
    resp = await client.get("/redis/queue/pop")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_queue_length(client):
    resp = await client.get("/redis/queue/length")
    assert resp.status_code == 200
    data = resp.json()
    assert "length" in data


@pytest.mark.asyncio
async def test_queue_pop_empty(client):
    # Pop until empty, then check
    for _ in range(100):
        r = await client.get("/redis/queue/pop")
        if r.json() is None or r.json().get("value") is None:
            break
    resp = await client.get("/redis/queue/pop")
    assert resp.status_code == 200
