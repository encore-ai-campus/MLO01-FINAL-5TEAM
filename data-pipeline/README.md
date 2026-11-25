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
| `data06/` | Kafka/Spark/Redis Docker 설정, 행동 판정·Redis 반영·Delta 적재, 피드백 로그, 초기 적재·복구 및 장애·부하 검증 |
| `data08/` | PostgreSQL 시점별 상품 카탈로그와 적재·조회 검증 |
| `data09/` | Redis 시작 상태 준비, 일 배치 품질 검사와 Soda 계약 |
| `tests/` | 재생 시계, 학습 테이블, 정답 생성 단위 테스트 |

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
```

검증 결과는 `data/data-06-refined/` 아래 실행별 폴더에 생성된다. Docker의 호스트 포트는 로컬 시험용으로 제한한다. 팀 통합은 공용 토픽·체크포인트·저장소 연결 설정을 확정한 뒤 수행한다. Redis의 Kafka 직접 읽기는 `data06/redis_kafka.py`의 로컬 비교 경로다.
