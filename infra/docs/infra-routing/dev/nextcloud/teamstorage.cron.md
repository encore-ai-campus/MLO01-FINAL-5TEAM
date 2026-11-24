# SSD 용량 수집 일정

- 경로: `srcs/dev/nextcloud/teamstorage.cron`
- 시그니처: WSL cron의 `*/10 * * * *`
- 파라미터: `/mnt/c`, 컨테이너 `nextcloud-nextcloud-1`
- 호출: `mountpoint`, `df`, `docker exec` → `php occ config:app:set`
- 기능: SSD의 측정 시각·전체 바이트·여유 바이트를 `teamstorage/host_space`에 저장한다.
