# Ubuntu 24.04 데이터 스택 사전 검증

Ubuntu 24.04 VM에 설치할 라이브러리 조합을 Docker에서 먼저 시험한다. Spark는 컨테이너 안에서 `local[1]`로 실행한다. 통합 검사에서는 Kafka 브로커 한 대를 함께 실행한다.

| 구성 요소 | 역할과 관계 |
|---|---|
| 공통 기반 | 하나의 `Dockerfile`에서 Ubuntu 24.04, Python 3.12, Java 21을 기반으로 Spark와 Airflow 이미지를 각각 만든다. |
| Spark 이미지 | `requirements.txt`에 PySpark 4.2.0, Delta Lake 4.4.1과 데이터·ML·API 라이브러리 버전을 고정한다. Delta, Kafka, Hadoop S3A, AWS SDK용 JAR도 이미지에 포함한다. |
| Airflow 이미지 | `airflow/requirements.txt`로 Airflow 3.4.0과 Spark provider 6.3.4를 별도 환경에 설치한다. 공식 제약 파일의 `pyspark-client`는 4.2.0으로 맞춘다. |
| 별도 서비스 | `compose.yaml`에는 PostgreSQL 18 + pgvector 0.8.7, Prometheus 3.16.0, Grafana 13.3.1, 선택 실행용 k3s v1.36.6+k3s1이 정의돼 있다. |

**테스트와 결과** — `docker compose build spark airflow`가 성공하면 두 환경의 패키지가 설치되고 `pip check`를 통과한 것이다. Spark는 네트워크를 끈 상태에서도 Kafka·S3A·AWS SDK 클래스를 불러오고 로컬 Delta 테이블에 3행을 쓴 뒤 다시 읽는다. Airflow는 버전 명령이 실행되는지 확인한다.

**검증 결과 (2026-11-26)** — Spark·Airflow 이미지 빌드와 Spark의 오프라인 Delta 읽기·쓰기 검사가 통과했다. 통합 검사에서는 Kafka 4.4.0에 발행한 `amount=21`을 Spark가 두 배로 계산해 Delta에 저장했다. Delta에서 다시 읽은 데이터를 Parquet으로 내보내 Feast 0.68.0에서 `score=42`를 조회했다. [실행 로그](stack-check-e2518b1c442a47a28890140a4ad4b1ce.log)

다시 실행하려면 [검증 소스 폴더](../../../srcs/support/compatibility/)에서 `docker compose build spark airflow` 후 PowerShell로 `.\verify.ps1`을 실행한다. 새 실행 로그는 스크립트와 같은 폴더에 생성된다.
