# 개발 서비스 정의

- 경로 `srcs/support/compatibility/compose.yaml`
- 시그니처 Docker Compose 서비스
- 파라미터 POSTGRES_PASSWORD, k3s 프로필
- 호출 Dockerfile의 spark·airflow 단계, init-vector.sql
- 기능 Spark·Airflow 이미지 빌드 및 지원 서비스 구성
