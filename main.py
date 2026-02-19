"""
FastAPI 메인 애플리케이션
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Swagger UI: http://localhost:8000/docs
- Prometheus Metrics: http://localhost:8000/metrics
"""

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api import router
from app.config import settings
from app.lifespan import lifespan

# FastAPI 앱 생성
app = FastAPI(
    title="Message Broker Comparison Lab",
    description="""
## 메시지 브로커 비교 학습 환경

Redis, RabbitMQ, Kafka의 성능과 특성을 비교 실험할 수 있습니다.

### 주요 기능
- **Direct API**: 브로커 없는 순수 API (기준선)
- **Redis**: Pub/Sub, Stream, Queue, Cache, Rate Limiter
- **RabbitMQ**: Direct, Fanout, Topic, DLQ, Priority, TTL
- **Kafka**: Basic, Keyed, Batch, Topic Management
- **Benchmark**: 성능 비교 벤치마크
- **Monitoring**: Prometheus + 커스텀 메트릭
    """,
    version="0.1.0",
    lifespan=lifespan,
)

# Prometheus 자동 계측
Instrumentator().instrument(app).expose(app)

# 라우트 등록
app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=True,
    )
