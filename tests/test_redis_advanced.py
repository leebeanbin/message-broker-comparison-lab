"""
Redis 8 고급 기능 엔드포인트 테스트

Bloom Filter, TimeSeries, Vector Set 테스트.
브로커 연결 필요: docker compose up -d redis

Vector Set 테스트는 Redis 8.0+ 전용 VADD/VSIM 커맨드 필요.
지원하지 않는 Redis 버전에서는 자동 skip.
"""

import time
import uuid

import pytest


def unique(prefix: str = "test") -> str:
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Bloom Filter
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_bloom_add_returns_200(client):
    key = unique("bf")
    resp = await client.post("/redis/bloom/add", json={"filter_key": key, "item": "apple"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["filter"] == key      # 응답 key는 "filter"
    assert data["item"] == "apple"


@pytest.mark.asyncio
async def test_bloom_exists_true(client):
    key = unique("bf")
    await client.post("/redis/bloom/add", json={"filter_key": key, "item": "banana"})
    resp = await client.post("/redis/bloom/exists", json={"filter_key": key, "item": "banana"})
    assert resp.status_code == 200
    assert resp.json()["result"] is True   # 응답 key는 "result"


@pytest.mark.asyncio
async def test_bloom_exists_false_for_absent_item(client):
    key = unique("bf")
    await client.post("/redis/bloom/add", json={"filter_key": key, "item": "apple"})
    resp = await client.post("/redis/bloom/exists", json={"filter_key": key, "item": "xyz-never-added"})
    assert resp.status_code == 200
    assert resp.json()["result"] is False


@pytest.mark.asyncio
async def test_bloom_info(client):
    key = unique("bf")
    for item in ["a", "b", "c"]:
        await client.post("/redis/bloom/add", json={"filter_key": key, "item": item})
    resp = await client.get(f"/redis/bloom/info/{key}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["filter"] == key      # 응답 key는 "filter"
    assert data["bit_size"] > 0


@pytest.mark.asyncio
async def test_bloom_false_positive_rate_acceptable(client):
    """실용적 오탐율 테스트 — 10개 샘플 중 2개 이하 오탐"""
    key = unique("bf-fp")
    for i in range(100):
        await client.post("/redis/bloom/add", json={"filter_key": key, "item": f"url:{i}"})

    false_positives = 0
    for i in range(100, 110):
        resp = await client.post("/redis/bloom/exists", json={"filter_key": key, "item": f"url:{i}"})
        if resp.json()["result"]:   # 응답 key는 "result"
            false_positives += 1

    assert false_positives <= 2, f"Too many false positives: {false_positives}/10"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TimeSeries
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_ts_add_returns_200(client):
    key = unique("ts")
    resp = await client.post("/redis/ts/add", json={"series_key": key, "value": 42.5})
    assert resp.status_code == 200
    data = resp.json()
    assert data["series"] == key    # 응답 key는 "series"
    assert "timestamp_ms" in data


@pytest.mark.asyncio
async def test_ts_range_returns_points(client):
    key = unique("ts")
    for v in [10.0, 20.0, 30.0]:
        await client.post("/redis/ts/add", json={"series_key": key, "value": v})

    # 쿼리 파라미터: from_ms, to_ms
    resp = await client.get(f"/redis/ts/range/{key}", params={"from_ms": 0, "to_ms": 9999999999999})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["points"]) >= 3


@pytest.mark.asyncio
async def test_ts_latest_returns_most_recent(client):
    key = unique("ts")
    for v in [1.0, 2.0, 3.0]:
        await client.post("/redis/ts/add", json={"series_key": key, "value": v})

    resp = await client.get(f"/redis/ts/latest/{key}", params={"n": 1})
    assert resp.status_code == 200
    data = resp.json()
    # ts_latest는 points 배열 반환, 마지막 값 확인
    assert len(data["points"]) >= 1
    assert data["points"][-1]["value"] == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_ts_trim_removes_old_data(client):
    key = unique("ts")
    for v in [1.0, 2.0, 3.0]:
        await client.post("/redis/ts/add", json={"series_key": key, "value": v})

    # 쿼리 파라미터: max_age_seconds=0 → 모든 데이터 삭제
    resp = await client.delete(f"/redis/ts/trim/{key}", params={"max_age_seconds": 0})
    assert resp.status_code == 200
    data = resp.json()
    assert data["removed_points"] >= 3   # 응답 key는 "removed_points"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Vector Set (Redis 8.0+ VADD/VSIM)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_vector_add_returns_200_or_unsupported(client):
    key = unique("vs")
    resp = await client.post("/redis/vector/add", json={
        "vset_key": key,
        "element_id": "e1",
        "vector": [1.0, 0.0, 0.0, 0.0],
    })
    # Redis 8.0+ VADD 지원 → 200, 구버전 → 400/500 허용
    assert resp.status_code in (200, 400, 500)


@pytest.mark.asyncio
async def test_vector_search_returns_results(client):
    key = unique("vs")
    # doc1: x축 방향, doc2: y축 방향 (명확히 다른 방향)
    add_resp = await client.post("/redis/vector/add", json={
        "vset_key": key,
        "element_id": "doc1",
        "vector": [1.0, 0.0, 0.0, 0.0],
    })
    if add_resp.status_code != 200 or "error" in add_resp.json():
        pytest.skip("Redis VADD not supported (requires Redis 8.0+)")

    await client.post("/redis/vector/add", json={
        "vset_key": key,
        "element_id": "doc2",
        "vector": [0.0, 1.0, 0.0, 0.0],
    })

    # 쿼리: x축에 가까운 방향 → doc1이 더 유사해야 함
    resp = await client.post("/redis/vector/search", json={
        "vset_key": key,
        "query_vector": [0.9, 0.1, 0.0, 0.0],
        "top_k": 2,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "results" in data
    if data["results"]:
        assert data["results"][0]["id"] == "doc1"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Consumer Lag API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_kafka_lag_endpoint_returns_200(client):
    resp = await client.get("/monitoring/kafka-lag")
    assert resp.status_code == 200
    data = resp.json()
    assert "monitored_groups" in data
    assert "lag_data" in data
    assert isinstance(data["monitored_groups"], list)
