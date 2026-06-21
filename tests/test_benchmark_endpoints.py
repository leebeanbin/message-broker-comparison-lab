"""벤치마크 API 테스트 (~3개)

모든 브로커 연결 필요: docker compose up -d redis rabbitmq kafka
"""

import pytest


@pytest.mark.asyncio
async def test_benchmark_redis(client):
    resp = await client.post("/benchmark/redis", json={"message_count": 10})
    assert resp.status_code == 200
    data = resp.json()
    assert "total_ms" in data


@pytest.mark.asyncio
async def test_benchmark_kafka_batch(client):
    resp = await client.post("/benchmark/kafka-batch", json={"message_count": 10})
    assert resp.status_code == 200
    data = resp.json()
    assert "total_ms" in data


@pytest.mark.asyncio
async def test_benchmark_all(client):
    resp = await client.post("/benchmark/all", json={"message_count": 10})
    assert resp.status_code == 200
