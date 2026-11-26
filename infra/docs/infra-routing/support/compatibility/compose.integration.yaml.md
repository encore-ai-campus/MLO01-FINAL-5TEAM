# 통합 검사 서비스

- 경로 `srcs/support/compatibility/compose.integration.yaml`
- 시그니처 kafka, verify 서비스
- 파라미터 기존 Spark 이미지, integration.py
- 호출 Kafka 준비 확인 후 spark-submit 실행
- 기능 단일 브로커에서 통합 검사 수행
