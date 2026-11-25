# 데이터 파이프라인

공용 행동 데이터의 변환·재생, Redis 피처 반영, Delta 이력 적재, 학습·평가용 테이블 생성에 사용하는 로컬 구현이다.

```text
CSV.GZ → Parquet → 재생기 → Kafka 행동 토픽 → Spark 판정 → Kafka 정제 토픽
                                │                            ├→ Redis 상태 반영
                                └────────────→ Delta 적재 ←──┘
```

| 위치 | 코드 |
| --- | --- |
| `reco/` | Parquet 변환·사용자 표본·재생, 학습 피처·방문 결과·예측 정답 생성, 성능 지표 집계 |
| `data06/` | 공통 행동 스키마, Kafka/Spark/Redis Docker 설정, 행동 판정·Redis 반영·Delta 적재, 피드백 로그, 초기 적재·복구, 장애·부하 검증과 로컬 대시보드 |
| `data08/` | PostgreSQL 시점별 상품 카탈로그와 적재·조회 검증 |
| `data09/` | Redis 시작 상태 준비, 일 배치 품질 검사와 Soda 계약 |
| `tests/` | 행동 스키마, 재생 시계, 학습 테이블, 정답 생성 단위 테스트 |

## 공통 행동 스키마

[`data06/event_schema.py`](data06/event_schema.py)의 `ACTION_FIELDS`에서 공통 컬럼의 이름·순서·논리 타입을 관리한다. CSV 변환과 Parquet 스키마, Kafka 행동 메시지를 읽는 Spark 스키마가 이 정의를 참조한다. `event_time`은 Parquet에서 UTC 밀리초 타임스탬프로 저장하며, Kafka JSON에서는 ISO 8601 문자열 표현을 사용한다. 기존 Parquet 파일과 이벤트 ID 형식을 지원한다.

잘못된 행동에는 `invalid` 판정과 제외 사유를 붙여 Delta `rejected`에 기록한다. 늦은 도착은 Kafka 파티션별 이벤트 시각을 기준으로 판단한다. 변경된 판정 정책을 적용할 때는 새 실행 ID와 토픽을 사용한다.

## 로컬 실행

아래 명령은 이 디렉터리에서 Python 3.12 환경으로 실행한다. 공용 원본과 Parquet은 팀 공유 저장소에서 받아 `data/` 등 로컬 경로에 둔다.

```text
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m reco.convert_parquet "data/2019-Nov.csv.gz" --output "data/2019-Nov.parquet"
python -m reco.replay_parquet "data/2019-Nov.parquet" --output "data/replay-100.jsonl" --max-events 100 --speed 10
```

Kafka·Spark·Redis·Delta 통합 검증에는 Docker Compose와 시간순으로 정렬된 11월 표본 Parquet이 필요하다. 이 명령은 로컬 시험용 브로커와 Redis를 올리고 격리한 토픽으로 재시작·재전송을 검증한다.

```text
docker compose -p mlo-data06-split -f data06/compose.yaml --profile worker build spark
python data06/verify_refined.py --source "data/2019-Nov-sample.parquet"
python data06/verify_invalid_actions.py
python data06/verify_late_batches.py
python data06/verify_revision_sync.py --source "data/2019-Nov-sample.parquet"
```

검증 결과는 `data/data-06-refined/` 아래 실행별 폴더에 생성된다. Docker의 호스트 포트는 로컬 시험용으로 제한한다. 팀 통합은 공용 토픽·체크포인트·저장소 연결 설정을 확정한 뒤 수행한다. Redis의 Kafka 직접 읽기는 `data06/redis_kafka.py`의 로컬 비교 경로다.

로컬 대시보드는 `python data06/dashboard.py`로 실행한 뒤 `http://127.0.0.1:8765`에서 연다. 별도 Kafka 브로커 3개와 Spark 워커 2개가 있는 로컬 환경에서는 `python data06/run_direct_day_check.py --pilot --source "data/2019-Nov-sample.parquet"`로 짧은 10배속 Redis 복구 시험을 실행할 수 있다.
