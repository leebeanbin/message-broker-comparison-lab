"""
앱 라이프사이클 관리
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FastAPI의 lifespan 이벤트 핸들러.
앱 시작 시 브로커 연결, 종료 시 연결 해제.
연결 실패해도 앱은 계속 동작 (graceful degradation).

백그라운드 태스크:
  - Kafka Consumer Lag 모니터링 (15초 간격)
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.brokers import AbstractBroker, kafka_broker, rabbitmq_broker, redis_broker
from app.config import settings

# 등록된 브로커 목록 (순서대로 연결/해제)
ALL_BROKERS: list[AbstractBroker] = [redis_broker, rabbitmq_broker, kafka_broker]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 브로커 연결 + 백그라운드 모니터링 시작"""
    # ── 시작 ──
    print("=" * 60)
    print("🚀 Message Broker Comparison Lab 시작")
    print("=" * 60)

    for broker in ALL_BROKERS:
        try:
            await broker.connect()
        except Exception as e:
            import traceback
            print(f"⚠️  {broker.name} 연결 실패 (나중에 연결 가능): {e}")
            traceback.print_exc()

    # Kafka Consumer Lag 백그라운드 모니터링 시작
    from app.monitoring.kafka_lag import lag_monitor_loop
    lag_task = asyncio.create_task(lag_monitor_loop(interval_seconds=15.0))

    print("=" * 60)
    print(f"📋 Swagger UI:        http://localhost:{settings.app_port}/docs")
    print(f"📊 Prometheus:        http://localhost:{settings.app_port}/metrics")
    print(f"🔗 MCP SSE:           http://localhost:{settings.app_port}/mcp/sse")
    print(f"🛡  Circuit Breakers:  http://localhost:{settings.app_port}/resilience/circuit-breakers")
    print("=" * 60)

    yield

    # ── 종료 ──
    lag_task.cancel()
    try:
        await lag_task
    except asyncio.CancelledError:
        pass

    print("\n🛑 서버 종료 중...")
    for broker in reversed(ALL_BROKERS):
        try:
            await broker.disconnect()
        except Exception as e:
            print(f"⚠️  {broker.name} 종료 중 에러: {e}")
