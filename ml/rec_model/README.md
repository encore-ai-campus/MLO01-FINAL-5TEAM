# 추천 순위 재생

저장소 최상위에서 실행한다. 기존 데이터 파이프라인을 의존 경로에 추가하고, 입력 이벤트 DB와 동반 명세는 읽기 전용으로 사용한다. 관계표 재현 캐시는 지정한 결과 폴더에 저장된다.

```powershell
$pipeline = (Resolve-Path ./data-pipeline).Path
$env:PYTHONPATH = (@($pipeline, $env:PYTHONPATH) | Where-Object { $_ }) -join [IO.Path]::PathSeparator
```

Linux에서는 `export PYTHONPATH="$PWD/data-pipeline${PYTHONPATH:+:$PYTHONPATH}"`으로 지정한다. 두 명령 모두 이미 설정한 Python 패키지 경로를 보존한다.

같은 요청에서 기본 규칙순과 LightGBM 결합 순위를 확인할 수 있다. 결합 순위는 장바구니·최근 관심 상품을 먼저 배치하고, 남은 후보를 모델 점수 내림차순과 상품 ID 오름차순으로 정렬한다.

```text
python -m ml.rec_model.ranking_replay --events <이벤트DB> --sample team --output <결과폴더> --event-id <행동번호> --availability-receipt <관계표준비기록JSON>
python -m ml.rec_model.ranking_replay --events <이벤트DB> --sample team --output <결과폴더> --event-id <행동번호> --ranking hybrid --model-config <모델설정JSON> --availability-receipt <관계표및모델준비기록JSON>
```

기본은 규칙순이다. 모델 설정 파일이 있어도 `--ranking hybrid`를 명시한 재생에서만 결합 순위를 사용한다.

팀 재생의 준비 기록 JSON에는 규칙순이면 `relation`을, 결합 순위면 `relation`과 `ranker`를 넣는다. 각 항목에 실제 파일의 `sha256`, 당시 적재 또는 등록 기록 번호 `record_id`, 시간대가 포함된 초 단위 `ready_at_utc`를 기록한다. 관계표 마감 또는 모델 검증 종료 시각 ≤ 준비 완료 시각 < 요청 시각이어야 한다. 이 조건을 벗어나거나 체크섬이 다르면 해당 자료로 추천하지 않고 재생을 중단한다. `--sample local`에서 준비 기록을 생략한 재현에는 `readiness.status=unverified`가 남는다.

모델 설정 JSON에는 `evaluation.sample`, `evaluation.report_file`, `ranking.model_file`, `ranking.model_sha256`이 필요하다. 모델과 보고서의 상대 경로는 설정 파일의 폴더가 기준이다. 보고서에서 입력 명세 SHA-256, 검증 종료 시각, 모델 SHA-256을 확인하고 모델 입력 18개의 순서를 검사한다. 실제 모델과 보고서는 팀의 모델 전달 경로에서 받는다.

## 시험

저장소 최상위에서 위의 `PYTHONPATH` 설정을 적용한 후 다음 세 시험을 실행한다.

```text
python -B -m unittest discover -s ml/rec_model/tests
python -B -m unittest discover -s ml/rec_model/evaluation/tests
python -B -m unittest discover -s data-pipeline/tests
```

가상 이벤트와 관계표, 고정 모델 점수로 학습·순위·평가를 검증한다. 기존 데이터 파이프라인은 회귀 시험으로 대조한다. 실제 팀 PostgreSQL 적재 및 API 연결은 후속 시험이다.

## 추천 LightGBM 학습

저장소 최상위에서 위의 `PYTHONPATH` 설정을 적용한 뒤 실행한다. 입력은 닫혀 있는 SQLite 이벤트 사본과 같은 이름의 `.manifest.json`이다. 팀 입력은 공용 2% 명세·사용자 명단과 연결된 이벤트 사본을 사용한다. 가상 표본만 실행할 때는 `--sample local`을 지정하고 `--sample-manifest`를 생략한다.

```text
python -B -m ml.rec_model.ranking_training --events events.sqlite --sample team --sample-manifest manifest.json --output trial-ranker
```

- 2019년 10월 8일 00:00 UTC부터 25일 00:00 직전까지 학습하고, 25일부터 11월 1일 00:00 직전까지 검증한다. 조회·장바구니에서 요청을 만들고 관측이 완료된 같은 세션의 요청 이후 구매를 정답으로 쓴다.
- 원본을 읽기 전용으로 사용한다. 가격 0은 모델 상태에 넣기 전에 결측으로 바꾼다. 동일한 후보 생성 함수로 요청마다 입력 18개를 정해진 순서와 `float32` 형식으로 만들고 LightGBM에 전달한다.
- 새 결과 디렉터리에 `ranker.txt`, `policy.json`, `price-model.json`을 저장한다. 모델은 UTF-8로 저장하고 읽으며 출력 경로에 한글과 공백을 사용할 수 있다. 명세에는 입력 표본·정책·기간·모델 체크섬이 들어간다. 저장 모델을 다시 읽어 검증 점수가 같은지 확인한 뒤 결과를 남긴다.
- `input_code_sources`는 가격 처리와 공용 입력 계산·후보 생성·재생에 쓰인 소스별 SHA-256이다. 줄바꿈 형식과 경로를 제외한 소스 버전을 `input_code_version`으로 기록한다.
- 여기서 만드는 모델은 학습 검증용이다. 실제 API에서 새 가격 처리와 모델 묶음을 적용하는 작업은 후속 연동에서 확인한다.
