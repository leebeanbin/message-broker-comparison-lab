"""
API 라우터 통합 모듈
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
각 도메인별 서브 라우터를 하나의 router로 통합.
main.py에서는 이 router만 include하면 됨.
"""

from fastapi import APIRouter

from . import (
    benchmark_routes,
    health,
    kafka_routes,
    monitoring_routes,
    rabbitmq_routes,
    redis_routes,
)

router = APIRouter()

# 도메인별 서브 라우터 등록
router.include_router(health.router)
router.include_router(redis_routes.router)
router.include_router(rabbitmq_routes.router)
router.include_router(kafka_routes.router)
router.include_router(benchmark_routes.router)
router.include_router(monitoring_routes.router)
