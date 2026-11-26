# 추천 순위 재생

저장소 최상위에서 실행한다. 기존 데이터 파이프라인을 의존 경로에 추가하고, 입력 이벤트 DB와 동반 명세는 읽기 전용으로 사용한다. 관계표 재현 캐시는 지정한 결과 폴더에 저장된다.

```powershell
$env:PYTHONPATH = (Resolve-Path ./data-pipeline).Path
```

Linux에서는 `export PYTHONPATH="$PWD/data-pipeline"`으로 지정한다.

같은 요청에서 기본 규칙순과 LightGBM 결합 순위를 확인할 수 있다. 결합 순위는 장바구니·최근 관심 상품을 먼저 배치하고, 남은 후보를 모델 점수 내림차순과 상품 ID 오름차순으로 정렬한다.

```text
python -m ml.rec_model.ranking_replay --events <이벤트DB> --sample team --output <결과폴더> --event-id <행동번호> --availability-receipt <관계표준비기록JSON>
python -m ml.rec_model.ranking_replay --events <이벤트DB> --sample team --output <결과폴더> --event-id <행동번호> --ranking hybrid --model-config <모델설정JSON> --availability-receipt <관계표및모델준비기록JSON>
```

기본은 규칙순이다. 모델 설정 파일이 있어도 `--ranking hybrid`를 명시한 재생에서만 결합 순위를 사용한다.

팀 재생의 준비 기록 JSON에는 규칙순이면 `relation`을, 결합 순위면 `relation`과 `ranker`를 넣는다. 각 항목에 실제 파일의 `sha256`, 당시 적재 또는 등록 기록 번호 `record_id`, 시간대가 포함된 초 단위 `ready_at_utc`를 기록한다. 관계표 마감 또는 모델 검증 종료 시각 ≤ 준비 완료 시각 < 요청 시각이어야 한다. 이 조건을 벗어나거나 체크섬이 다르면 해당 자료로 추천하지 않고 재생을 중단한다. `--sample local`에서 준비 기록을 생략한 재현에는 `readiness.status=unverified`가 남는다.

모델 설정 JSON에는 `evaluation.sample`, `evaluation.report_file`, `ranking.model_file`, `ranking.model_sha256`이 필요하다. 모델과 보고서의 상대 경로는 설정 파일의 폴더가 기준이다. 보고서에서 입력 명세 SHA-256, 검증 종료 시각, 모델 SHA-256을 확인하고 모델 입력 18개의 순서를 검사한다. 실제 모델과 보고서는 팀의 모델 전달 경로에서 받는다.

## 시험

저장소 최상위에서 `python -B -m unittest discover -s ml/rec_model/tests`를 실행한다. 가상 이벤트와 관계표, 고정 모델 점수를 사용해 순위·대체 규칙과 준비 시각을 검증한다. 실제 팀 PostgreSQL 적재 및 API 연결은 후속 시험이다.
