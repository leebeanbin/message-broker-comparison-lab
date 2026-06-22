"""
FastAPI 메인 애플리케이션
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Swagger UI:        http://localhost:8000/docs
- Prometheus Metrics: http://localhost:8000/metrics
- MCP SSE:           http://localhost:8000/mcp/sse
- Circuit Breakers:  http://localhost:8000/resilience/circuit-breakers
"""

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api import router
from app.config import settings
from app.lifespan import lifespan
from app.mcp import mcp

# FastAPI 앱 생성
app = FastAPI(
    title="Message Broker Comparison Lab",
    description="""
## 메시지 브로커 비교 학습 환경 v0.2

Redis, RabbitMQ, Kafka의 성능과 특성을 비교 실험할 수 있습니다.

### 주요 기능
- **Redis**: Pub/Sub, Stream, Queue, Cache, Rate Limiter, **Bloom Filter**, **TimeSeries**, **Vector Set**
- **RabbitMQ**: Direct, Fanout, Topic, DLQ, Priority, TTL
- **Kafka**: Basic, Keyed, Batch, Topic Management + **Consumer Lag 모니터링**
- **MCP**: AI가 직접 브로커를 제어하는 MCP 서버 (`/mcp/sse`)
- **Circuit Breaker**: 브로커별 장애 격리 (`/resilience/circuit-breakers`)
- **Backpressure**: 동시성 제어 (`/resilience/backpressure`)
- **Benchmark**: P50/P99 포함 성능 비교

### MCP 연결 (Claude Code/Desktop)
```json
{
  "mcpServers": {
    "broker-lab": {
      "type": "sse",
      "url": "http://localhost:8000/mcp/sse"
    }
  }
}
```
    """,
    version="0.2.0",
    lifespan=lifespan,
)

# Prometheus 자동 계측
Instrumentator().instrument(app).expose(app)

# 브로커 + 기능 라우트 등록
app.include_router(router)

# MCP SSE 엔드포인트 마운트 (/mcp/sse)
app.mount("/mcp", mcp.sse_app())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=True,
    )
