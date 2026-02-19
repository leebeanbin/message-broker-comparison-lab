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


@pytest.fixture
async def client():
    """FastAPI ASGI 테스트 클라이언트"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def fake():
    """Faker 한국어 인스턴스 (seed 고정)"""
    Faker.seed(42)
    return Faker("ko_KR")
