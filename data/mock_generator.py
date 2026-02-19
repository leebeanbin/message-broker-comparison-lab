"""
Faker 기반 Mock 데이터 생성기
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
seed 파라미터로 재현 가능한 데이터 생성.
노트북과 테스트에서 공통 사용.

사용법:
    # 모듈로 import
    from data.mock_generator import generate_payments

    # CLI로 전체 JSON 재생성
    python data/mock_generator.py
"""

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

MOCK_DIR = Path(__file__).parent / "mock"


def generate_payments(count: int = 100, vip_ratio: float = 0.2, seed: int = 42) -> dict:
    """쿠팡 타임딜 결제 데이터 생성"""
    fake = Faker("ko_KR")
    Faker.seed(seed)
    random.seed(seed)

    products = [
        {"product_id": "prod_001", "name": "맥북 프로 16인치", "price": 350000, "stock": 10},
        {"product_id": "prod_002", "name": "갤럭시 S24", "price": 120000, "stock": 10},
        {"product_id": "prod_003", "name": "에어팟 프로", "price": 35000, "stock": 10},
        {"product_id": "prod_004", "name": "아이패드 에어", "price": 85000, "stock": 10},
        {"product_id": "prod_005", "name": "LG 그램 17", "price": 250000, "stock": 10},
    ]

    vip_count = int(count * vip_ratio)
    methods = ["card", "transfer", "point"]
    base_time = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)

    payments = []
    for i in range(count):
        is_vip = i < vip_count
        product = random.choice(products)
        payments.append({
            "id": f"pay_{i + 1:03d}",
            "user_id": f"vip_{i + 1:03d}" if is_vip else f"user_{i - vip_count + 1:03d}",
            "user_tier": "VIP" if is_vip else "NORMAL",
            "user_name": fake.name(),
            "amount": product["price"],
            "product_id": product["product_id"],
            "payment_method": random.choice(methods),
            "timestamp": (base_time + timedelta(minutes=i * 4, seconds=random.randint(0, 59))).isoformat(),
            "status": "pending",
        })

    return {"products": products, "payments": payments}


def generate_tickets(request_count: int = 200, total_seats: int = 50, seed: int = 42) -> dict:
    """BTS 콘서트 티켓팅 데이터 생성"""
    fake = Faker("ko_KR")
    Faker.seed(seed)
    random.seed(seed)

    concert = {
        "concert_id": "concert_001",
        "name": "BTS 월드투어 서울 콘서트",
        "venue": "잠실종합운동장",
        "date": "2024-03-15",
        "total_seats": total_seats,
        "available_seats": total_seats,
        "sections": [
            {"name": "VIP석", "seats": 10, "price": 330000},
            {"name": "R석", "seats": 15, "price": 220000},
            {"name": "S석", "seats": 15, "price": 165000},
            {"name": "A석", "seats": 10, "price": 110000},
        ],
    }

    sections = ["VIP석", "R석", "S석", "A석"]
    requests = []
    for i in range(request_count):
        requests.append({
            "request_id": f"req_{i + 1:03d}",
            "user_id": f"fan_{i + 1:03d}",
            "user_name": fake.name(),
            "requested_section": random.choice(sections),
            "quantity": random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0],
            "timestamp": (
                datetime(2024, 3, 1, 20, 0, 0, tzinfo=timezone.utc)
                + timedelta(milliseconds=i * random.randint(10, 200))
            ).isoformat(),
        })

    return {"concert": concert, "requests": requests}


def generate_chat_messages(count: int = 200, room_count: int = 3, seed: int = 42) -> dict:
    """카카오톡 스타일 채팅 데이터 생성"""
    fake = Faker("ko_KR")
    Faker.seed(seed)
    random.seed(seed)

    nicknames_pool = [
        "코딩마스터", "개발새발", "버그킬러", "커피중독자", "야근전사", "리팩토링왕", "주니어코더",
        "맛집헌터", "야식의신", "미식가", "먹방러버", "배고픈하마", "라면왕", "치킨광", "디저트요정",
        "여행가자", "산책러", "일상기록", "노을사진", "고양이집사", "강아지아빠",
    ]
    room_names = ["개발자 모임방", "맛집 추천방", "여행 동호회"]

    rooms = []
    user_idx = 1
    for r in range(room_count):
        member_count = random.randint(5, 8)
        members = []
        for _ in range(member_count):
            members.append({
                "user_id": f"user_{user_idx:03d}",
                "nickname": nicknames_pool[(user_idx - 1) % len(nicknames_pool)],
            })
            user_idx += 1
        rooms.append({
            "room_id": f"room_{r + 1:03d}",
            "name": room_names[r % len(room_names)],
            "members": members,
        })

    msg_templates = [
        "안녕하세요! 오늘 날씨 좋네요 ☀️",
        "다들 뭐하고 계세요?",
        "오늘 점심 뭐 먹을까요?",
        "이거 한번 봐보세요!",
        "@{mention} 님 이거 어떻게 생각하세요?",
        "ㅋㅋㅋ 대박",
        "저도 동의합니다!",
        "오늘 회의 몇 시죠?",
        "감사합니다 🙏",
        "주말에 같이 갈까요?",
        "이번 프로젝트 화이팅!",
        "@{mention} 확인 부탁드립니다",
        "좋은 아침이에요~",
        "퇴근했습니다 ✌️",
        "맛있겠다!",
    ]

    messages = []
    base_time = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    for i in range(count):
        room = random.choice(rooms)
        sender = random.choice(room["members"])
        template = random.choice(msg_templates)

        has_mention = "{mention}" in template
        mention_target = None
        if has_mention:
            others = [m for m in room["members"] if m["user_id"] != sender["user_id"]]
            if others:
                mention_target = random.choice(others)
                template = template.replace("{mention}", mention_target["nickname"])

        messages.append({
            "message_id": f"msg_{i + 1:04d}",
            "room_id": room["room_id"],
            "sender_id": sender["user_id"],
            "sender_nickname": sender["nickname"],
            "content": template,
            "has_mention": has_mention and mention_target is not None,
            "mention_user_id": mention_target["user_id"] if mention_target and has_mention else None,
            "timestamp": (base_time + timedelta(seconds=i * random.randint(1, 30))).isoformat(),
        })

    return {"rooms": rooms, "messages": messages}


def generate_bulk_orders(count: int = 1000, seed: int = 42) -> dict:
    """대용량 주문 데이터 생성"""
    fake = Faker("ko_KR")
    Faker.seed(seed)
    random.seed(seed)

    categories = ["전자제품", "의류", "식품", "가구", "스포츠"]
    statuses = ["pending", "confirmed", "shipped", "delivered"]
    base_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    orders = []
    for i in range(count):
        orders.append({
            "order_id": f"ORD-{i + 1:05d}",
            "user_id": f"user_{random.randint(1, 200):04d}",
            "user_name": fake.name(),
            "product": fake.catch_phrase(),
            "category": random.choice(categories),
            "amount": random.randint(1000, 500000),
            "quantity": random.randint(1, 10),
            "status": random.choice(statuses),
            "region": fake.city(),
            "timestamp": (base_time + timedelta(hours=i * random.randint(1, 5))).isoformat(),
        })

    return {"orders": orders, "total_count": count}


def generate_saga_orders(seed: int = 42) -> dict:
    """Saga 패턴 주문 시나리오 생성"""
    fake = Faker("ko_KR")
    Faker.seed(seed)
    random.seed(seed)

    steps = [
        {"step": "create_order", "description": "주문 생성", "timeout_seconds": 5},
        {"step": "process_payment", "description": "결제 처리", "timeout_seconds": 10},
        {"step": "reserve_inventory", "description": "재고 예약", "timeout_seconds": 5},
        {"step": "arrange_shipping", "description": "배송 준비", "timeout_seconds": 5},
    ]

    compensations = [
        {"step": "cancel_shipping", "description": "배송 취소", "timeout_seconds": 5},
        {"step": "release_inventory", "description": "재고 복원", "timeout_seconds": 5},
        {"step": "refund_payment", "description": "결제 환불", "timeout_seconds": 10},
        {"step": "cancel_order", "description": "주문 취소", "timeout_seconds": 5},
    ]

    scenarios = [
        {
            "scenario_type": "SUCCESS",
            "order_id": "SAGA-001",
            "user": fake.name(),
            "product": "맥북 프로 16인치",
            "amount": 3990000,
            "currency": "KRW",
            "timestamp": "2026-02-19T10:30:00Z",
            "steps": [dict(s, expected="success") for s in steps],
            "compensations": compensations,
        },
        {
            "scenario_type": "PAYMENT_FAIL",
            "order_id": "SAGA-002",
            "user": fake.name(),
            "product": "갤럭시 S24 울트라",
            "amount": 1650000,
            "currency": "KRW",
            "timestamp": "2026-02-19T10:35:00Z",
            "steps": [
                dict(steps[0], expected="success"),
                dict(steps[1], expected="fail", fail_reason="잔액 부족"),
            ],
            "compensations": [compensations[3]],
        },
        {
            "scenario_type": "INVENTORY_FAIL",
            "order_id": "SAGA-003",
            "user": fake.name(),
            "product": "에어팟 프로 2",
            "amount": 359000,
            "currency": "KRW",
            "timestamp": "2026-02-19T10:40:00Z",
            "steps": [
                dict(steps[0], expected="success"),
                dict(steps[1], expected="success"),
                dict(steps[2], expected="fail", fail_reason="재고 소진"),
            ],
            "compensations": [compensations[2], compensations[3]],
        },
        {
            "scenario_type": "SHIPPING_FAIL",
            "order_id": "SAGA-004",
            "user": fake.name(),
            "product": "아이패드 에어 5",
            "amount": 929000,
            "currency": "KRW",
            "timestamp": "2026-02-19T10:45:00Z",
            "steps": [
                dict(steps[0], expected="success"),
                dict(steps[1], expected="success"),
                dict(steps[2], expected="success"),
                dict(steps[3], expected="fail", fail_reason="배송 불가 지역"),
            ],
            "compensations": [compensations[1], compensations[2], compensations[3]],
        },
        {
            "scenario_type": "TIMEOUT",
            "order_id": "SAGA-005",
            "user": fake.name(),
            "product": "LG 그램 17",
            "amount": 2190000,
            "currency": "KRW",
            "timestamp": "2026-02-19T10:50:00Z",
            "steps": [
                dict(steps[0], expected="success"),
                dict(steps[1], expected="timeout", timeout_seconds=3),
            ],
            "compensations": [compensations[3]],
        },
    ]

    return {"scenarios": scenarios}


def generate_delivery_timeline(count: int = 5, seed: int = 42) -> dict:
    """배달의민족 실시간 배달 추적 데이터 생성"""
    fake = Faker("ko_KR")
    Faker.seed(seed)
    random.seed(seed)

    restaurants = [
        "맘스터치 강남점", "BBQ치킨 역삼점", "본죽 서초점",
        "명동교자 신촌점", "홍콩반점 합정점",
    ]

    timeline_template = [
        {"stage": "order_accepted", "display": "주문 접수", "delay_seconds": 0, "description": "가맹점에서 주문을 확인했습니다"},
        {"stage": "cooking", "display": "조리 시작", "delay_seconds": 5, "description": "음식을 조리하고 있습니다"},
        {"stage": "delivering", "display": "배달 출발", "delay_seconds": 10, "description": "라이더가 배달을 시작했습니다"},
        {"stage": "delivered", "display": "배달 완료", "delay_seconds": 15, "description": "배달이 완료되었습니다"},
    ]

    ttl_stages = {
        "order_accepted": {"ttl_ms": 60000, "reason": "가맹점이 1분 내 확인하지 않으면 자동 취소"},
        "cooking": {"ttl_ms": 45000, "reason": "예상 조리 시간 초과 시 지연 알림"},
        "delivering": {"ttl_ms": 30000, "reason": "예상 배달 시간 초과 시 고객 알림"},
    }

    deliveries = []
    for i in range(count):
        restaurant = restaurants[i % len(restaurants)]
        deliveries.append({
            "delivery_id": f"DLV-20260219-{i + 1:03d}",
            "order_id": f"ORD-20260219-{1001 + i}",
            "restaurant_name": restaurant,
            "restaurant_phone": fake.phone_number(),
            "customer_name": fake.name(),
            "customer_phone": fake.phone_number(),
            "customer_address": fake.address(),
            "rider_name": fake.name(),
            "rider_phone": fake.phone_number(),
            "order_amount": random.randint(10000, 50000),
            "estimated_time_minutes": random.randint(20, 45),
            "timeline": timeline_template,
            "ttl_config": ttl_stages,
            "topic_routing": {
                "order_accepted": "delivery.status.accepted",
                "cooking": "delivery.status.cooking",
                "delivering": "delivery.status.delivering",
                "delivered": "delivery.status.delivered",
            },
        })

    return {"deliveries": deliveries}


def generate_image_requests(count: int = 50, seed: int = 42) -> dict:
    """이미지 처리 요청 데이터 생성"""
    Faker.seed(seed)
    random.seed(seed)

    formats = ["jpeg", "png", "webp"]
    filters_pool = ["sharpen", "blur", "sepia", "grayscale", "contrast"]
    resolutions = [
        (1920, 1080), (3840, 2160), (1280, 720),
        (2560, 1440), (800, 600),
    ]
    targets = [
        (800, 600), (400, 300), (200, 150),
        (1280, 720), (640, 480),
    ]

    requests = []
    for i in range(count):
        orig = random.choice(resolutions)
        target = random.choice(targets)
        n_filters = random.randint(0, 3)
        requests.append({
            "task_id": f"img_task_{i + 1:03d}",
            "image_url": f"https://picsum.photos/id/{random.randint(1, 1000)}/{orig[0]}/{orig[1]}",
            "original_width": orig[0],
            "original_height": orig[1],
            "target_width": target[0],
            "target_height": target[1],
            "format": random.choice(formats),
            "quality": random.choice([75, 80, 85, 90, 95]),
            "filters": random.sample(filters_pool, n_filters),
            "priority": random.randint(1, 5),
            "callback_queue": "image-result-queue",
        })

    return {"processing_requests": requests}


ALL_GENERATORS = {
    "payments.json": lambda: generate_payments(count=100),
    "tickets.json": lambda: generate_tickets(request_count=200),
    "chat_messages.json": lambda: generate_chat_messages(count=200),
    "bulk_orders.json": lambda: generate_bulk_orders(count=1000),
    "saga_orders.json": lambda: generate_saga_orders(),
    "delivery_timeline.json": lambda: generate_delivery_timeline(count=5),
    "images_metadata.json": lambda: generate_image_requests(count=50),
}


def regenerate_all():
    """모든 Mock JSON 파일 재생성"""
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    for filename, gen_func in ALL_GENERATORS.items():
        filepath = MOCK_DIR / filename
        data = gen_func()
        filepath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  {filename}: {filepath.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    print("Mock 데이터 재생성 중...")
    regenerate_all()
    print("완료!")
