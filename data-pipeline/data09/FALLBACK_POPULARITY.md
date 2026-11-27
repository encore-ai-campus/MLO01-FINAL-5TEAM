# 대체 인기 목록 일 배치

11월 재생의 하루 경계(UTC 00:00)를 마감으로 직전 7일의 고유 구매자 수를 집계한다. 동점은 상품 ID의 숫자 값 오름차순으로 정렬한다. 최초 `base`는 10월 구매만 사용하고 이후에는 처리가 끝난 시점의 정제 Delta 버전을 고정해 읽는다. 첫 주에는 기간에 포함되는 10월 구매도 집계한다.

파일은 [19-1 명세](https://ohio-carlo-lip-associate.trycloudflare.com/team3/projects/81e08c8d-b01a-47e4-a7ab-8d97691bfccd/pages/d0cba846-aa21-4e4a-97b7-fced60fa7b6f)에 따라 `fallback/{replay_run_id}/{list_version}/popular.json`, 같은 경로의 `popular.json.sha256`, 실행별 `fallback/{replay_run_id}/manifest.json` 순으로 게시한다. manifest는 `replay_run_id`와 `versions` 배열을 사용한다. 배열 항목은 `list_version`, `data_cutoff_utc`, `key`(버킷 내 객체 키), `sha256`을 담는다. 게시 재시도는 생성한 JSON과 체크섬 파일을 그대로 사용한다. 이미 등록된 날짜의 명시적 수정본은 다음 `--revision` 번호를 지정한다.

## 로컬 검증

Spark 4.2.0과 Delta 4.4.1 환경에서 `data-pipeline` 디렉터리를 기준으로 실행한다. `OCTOBER`, `NOVEMBER`는 공용 2% 표본 Parquet 경로다. 선택 인자 `--previous`에는 이전 형식의 일별 JSON이 있는 디렉터리를 지정할 수 있다.

```sh
PYTHONPATH="$PWD/data06${PYTHONPATH:+:$PYTHONPATH}" spark-submit --master 'local[2]' \
  --packages io.delta:delta-spark_2.13:4.4.1 data09/verify_fallback_popularity.py \
  --october "$OCTOBER" --november "$NOVEMBER" --output "$OUTPUT"
python -m pip install 'boto3==1.44.12'
python data09/verify_fallback_publication.py \
  --endpoint-url "$LOCAL_S3" --bucket "$TEST_BUCKET" --input "$OUTPUT"
```

순위 검증은 새 출력 디렉터리를 사용한다. 게시 검증은 비어 있는 전용 로컬 S3Mock 버킷에서 실행한다.

- 10월 초기 목록과 11월 2~8일 목록 8개를 독립 계산과 대조했다. 이전 형식과 전체·카테고리별 순서 차이는 0건이었다.
- 체크섬 쓰기와 manifest 갱신에 각각 HTTP 503을 주입했다. 두 경우 기존 목록이 유지됐고 동일한 파일과 버전으로 재시도해 게시했다.
- 기존 마감 시각의 `r2` 등록 후 `r1` 보존, 초기 목록의 별도 `base` manifest를 확인했다.

팀 스케줄러의 자동 실행·재시도와 서비스 API의 시점별 목록 조회는 팀 환경에서 검증한다. 관련 작업은 [#55](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/issues/55)에서 이어간다.
