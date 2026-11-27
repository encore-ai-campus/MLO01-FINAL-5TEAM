# 통합 호환성 검사

- 경로 `srcs/support/compatibility/integration.py`
- 시그니처 verify(spark, root)
- 파라미터 Spark 세션, 임시 작업 경로
- 호출 Kafka Producer, Spark Kafka 소스, Delta, Feast get_historical_features
- 기능 Kafka 입력값 21을 처리해 Feast 조회값 42 검증
