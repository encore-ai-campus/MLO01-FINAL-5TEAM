# Nextcloud Docker Compose

| 항목 | 내용 |
|---|---|
| 경로 | [`srcs/dev/nextcloud/compose.yaml`](../../../../srcs/dev/nextcloud/compose.yaml) |
| 시그니처 | Compose 서비스 `db`, `nextcloud`, `cron` |
| 파라미터 | `MARIADB_IMAGE`, `NEXTCLOUD_IMAGE`, `NEXTCLOUD_ADMIN_USER`, `db_password`, `db_root_password`, `admin_password` |
| 호출 | `docker compose config`, `docker compose up -d` |
| 기능 | MariaDB, Nextcloud 웹, Cron을 순서대로 시작한다. 웹은 `127.0.0.1:8080`, DB는 내부 `db:3311`을 사용한다. 파일과 DB를 각각 Named Volume에 보관한다. |
