"""
테스트 공통 Fixture
━━━━━━━━━━━━━━━━━━━
- AsyncClient: FastAPI ASGI 테스트 클라이언트
- Faker: 한국어 가짜 데이터 생성기

주의: 통합 테스트 → docker compose up -d redis rabbitmq kafka 필요
"""

import pytest
from faker import Faker
from httpx import ASGITransport, AsyncClient

from main import app
from app.brokers import kafka_broker, rabbitmq_broker, redis_broker

ALL_BROKERS = [redis_broker, rabbitmq_broker, kafka_broker]

@pytest.fixture
async def client():
    """FastAPI ASGI 테스트 클라이언트 (브로커 명시적 연결 추가)"""
    for broker in ALL_BROKERS:
        try:
            await broker.connect()
        except Exception as e:
            print(f"⚠️ [Test Setup] {broker.name} 연결 실패: {e}")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    for broker in reversed(ALL_BROKERS):
        try:
            await broker.disconnect()
        except Exception as e:
            print(f"⚠️ [Test Teardown] {broker.name} 해제 실패: {e}")


@pytest.fixture
def fake():
    """Faker 한국어 인스턴스 (seed 고정)"""
    Faker.seed(42)
    return Faker("ko_KR")
