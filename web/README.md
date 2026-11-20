# Django 손님 상점 · FR-SVC-01

## 개발 환경 실행

1. 이 디렉터리에서 `.env.example`을 `.env`로 복사.
2. `DJANGO_SECRET_KEY`와 `POSTGRES_PASSWORD`에 서로 다른 임의의 비밀값 설정. 각 값 생성 예시 — `python -c "import secrets; print(secrets.token_urlsafe(48))"`.
3. `docker compose up -d --build` 실행. PostgreSQL 마이그레이션 후 웹 서버 기동. 개발 PC 접속 주소 — <http://127.0.0.1:8005/store/>.
4. VM에서 외부 접속이 필요하면 `WEB_BIND_ADDRESS`에 공개할 인터페이스 주소, `DJANGO_ALLOWED_HOSTS`에 접속용 호스트명 또는 IP 설정. 개발용 Django 서버 구성.

## 사용자 흐름

- 상품 목록·상세·장바구니에서 브랜드·카테고리·가격 표시. 초기 상품 4개는 로컬 샘플.
- Django 서명 쿠키 `deskroom_session`으로 손님 식별. PostgreSQL에 수량과 행동 ID 저장.
- 같은 손님·같은 행동 ID 재전송 시 1회 반영. 수량 변경·삭제·전체 비우기 지원.
- 워커에서 커밋된 행동을 Redis에 비동기 반영. Kafka·추천 API와 구매 예측은 별도 통합 범위.

| 요청 | 용도 |
| --- | --- |
| GET `/store/`, `/store/products/<상품 ID>/`, `/store/cart/` | 목록·상세·장바구니 |
| GET `/api/cart/` | 현재 손님 장바구니 |
| POST `/api/cart/add/`, `/api/cart/remove/` | 상품 ID와 행동 ID로 담기·삭제 |
| POST `/api/cart/update/` | 상품 ID·수량·행동 ID로 수량 지정 |
| POST `/api/cart/clear/` | 행동 ID로 전체 비우기 |

변경 요청은 같은 브라우저 세션의 CSRF 토큰을 `X-CSRFToken` 헤더에 전달. 요청 본문은 JSON, 필드명은 `product_id`, `quantity`, `event_id`(UUID).
