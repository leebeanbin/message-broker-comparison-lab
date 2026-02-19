"""GET /health 엔드포인트 테스트"""

import pytest


@pytest.mark.asyncio
async def test_health_endpoint_returns_ok(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["api"] == "running"


@pytest.mark.asyncio
async def test_health_shows_broker_status(client):
    resp = await client.get("/health")
    data = resp.json()
    assert "redis" in data
    assert "rabbitmq" in data
    assert "kafka" in data
    for broker in ("redis", "rabbitmq", "kafka"):
        assert data[broker] in ("connected", "disconnected")
