# Docker 이미지 빌드

- 경로 `srcs/support/compatibility/Dockerfile`
- 시그니처 base, airflow, spark 단계
- 파라미터 SPARK_PACKAGES, 두 requirements.txt
- 호출 pip check, airflow version, spark-submit smoke.py
- 기능 Spark·Airflow 이미지 생성 및 기본 동작 검증
