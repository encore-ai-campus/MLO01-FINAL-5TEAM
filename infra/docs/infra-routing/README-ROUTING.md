# 인프라 라우팅

| 구성 | 소스·설정 파일 | 라우팅 문서 |
|---|---|---|
| SSD 용량 수집 일정 | [`teamstorage.cron`](../../srcs/support/nextcloud/teamstorage.cron) | [경로와 역할](support/nextcloud/teamstorage.cron.md) |
| SSD 표시 앱 정보 | [`teamstorage/appinfo/info.xml`](../../srcs/support/nextcloud/teamstorage/appinfo/info.xml) | [경로와 역할](support/nextcloud/teamstorage/appinfo/info.xml.md) |
| 파일 화면에 SSD 표시 등록 | [`teamstorage/lib/AppInfo/Application.php`](../../srcs/support/nextcloud/teamstorage/lib/AppInfo/Application.php) | [경로와 역할](support/nextcloud/teamstorage/lib/AppInfo/Application.php.md) |
| SSD 측정값 조회 | [`teamstorage/lib/Controller/SpaceController.php`](../../srcs/support/nextcloud/teamstorage/lib/Controller/SpaceController.php) | [경로와 역할](support/nextcloud/teamstorage/lib/Controller/SpaceController.php.md) |
| SSD 사용량 막대 | [`teamstorage/js/sidebar.js`](../../srcs/support/nextcloud/teamstorage/js/sidebar.js) | [경로와 역할](support/nextcloud/teamstorage/js/sidebar.js.md) |
| SSD 막대 모양 | [`teamstorage/css/sidebar.css`](../../srcs/support/nextcloud/teamstorage/css/sidebar.css) | [경로와 역할](support/nextcloud/teamstorage/css/sidebar.css.md) |
| Nextcloud 환경 설정 예제 | [`.env.example`](../../srcs/support/nextcloud/.env.example) | [설정 항목](support/nextcloud/.env.example.md) |
| Docker 이미지 빌드 | [`Dockerfile`](../../srcs/support/compatibility/Dockerfile) | [경로와 역할](support/compatibility/Dockerfile.md) |
| Spark 패키지 목록 | [`requirements.txt`](../../srcs/support/compatibility/requirements.txt) | [경로와 역할](support/compatibility/requirements.txt.md) |
| Airflow 패키지 목록 | [`airflow/requirements.txt`](../../srcs/support/compatibility/airflow/requirements.txt) | [경로와 역할](support/compatibility/airflow/requirements.txt.md) |
| 개발 서비스 정의 | [`compose.yaml`](../../srcs/support/compatibility/compose.yaml) | [경로와 역할](support/compatibility/compose.yaml.md) |
| 통합 검사 서비스 | [`compose.integration.yaml`](../../srcs/support/compatibility/compose.integration.yaml) | [경로와 역할](support/compatibility/compose.integration.yaml.md) |
| pgvector 초기화 | [`init-vector.sql`](../../srcs/support/compatibility/init-vector.sql) | [경로와 역할](support/compatibility/init-vector.sql.md) |
| Spark 기본 설정 | [`spark-defaults.conf`](../../srcs/support/compatibility/spark-defaults.conf) | [경로와 역할](support/compatibility/spark-defaults.conf.md) |
| 기본 호환성 검사 | [`smoke.py`](../../srcs/support/compatibility/smoke.py) | [경로와 역할](support/compatibility/smoke.py.md) |
| 통합 호환성 검사 | [`integration.py`](../../srcs/support/compatibility/integration.py) | [경로와 역할](support/compatibility/integration.py.md) |
| Windows 통합 검사 실행 | [`verify.ps1`](../../srcs/support/compatibility/verify.ps1) | [경로와 역할](support/compatibility/verify.ps1.md) |
