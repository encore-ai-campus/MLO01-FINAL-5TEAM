# 추천 결과 비교

후보 모델과 비교 기준이 같은 요청에서 낸 추천 결과를 채점하고 비교한다. 요청별 추천 목록과 구매 정답을 파일로 읽는다. 실행에는 데이터 파이프라인 소스, DB 연결, 모델 학습이 필요 없다.

## 입력과 실행

후보와 기준을 각각 JSONL 파일 한 개로 준비한다. 새 평가에서 두 파일은 동일한 평가 기간·사용자 표본·구매 정답을 사용한다. 요청마다 한 줄이며 두 파일의 순서도 같아야 한다.

~~~json
{"request_id":"event-1","user":"user-1","eligible_purchase_ids":[42],"candidate_ids":[17,42],"ranked_ids":[17,42]}
~~~

| 항목 | 값 |
| --- | --- |
| `request_id`, `user` | 비어 있지 않은 문자열 또는 정수. 요청 번호는 각 파일에서 고유하다. |
| `eligible_purchase_ids` | 확정된 적격 구매 상품. 후보 100개 밖에 있는 적격 상품도 포함한다. 관측이 끝났고 적격 구매가 없으면 빈 목록을 쓴다. |
| `candidate_ids`, `ranked_ids` | 후보 상품 최대 100개와 순서대로 기록한 최종 추천 상품 최대 10개. 각 목록의 상품 ID는 고유하다. |

관측 미완료 요청은 파일을 만들 때 제외한다. 기존에 채점한 결과를 가져올 때는 `--input-format scored`를 사용한다. 각 행에 `request_id`, `user`, 참·거짓인 `has_truth`, 그리고 비교할 지표의 점수(0~1)를 기록한다.

기존 ML 환경의 NumPy로 저장소 최상위에서 실행한다.

~~~text
python -B -m ml.rec_model.evaluation.comparison --candidate candidate.jsonl --baseline baseline.jsonl --metric ndcg --repeats 1000 --seed 42 --output comparison.json
python -B -m unittest discover -s ml/rec_model/evaluation/tests
~~~

요청 건수·번호·사용자·정답 유무가 다르거나 번호가 중복되면 실패한다. 추천 목록으로 직접 채점할 때는 구매 정답의 상품 집합까지 일치해야 한다. `--metric`으로 후보 회수율, Recall@10, 적중률@10, NDCG@10 중 하나를 비교한다. 적격 정답이 있는 요청의 점수 차이를 구하고, 사용자 단위로 다시 표집해 95% 신뢰구간을 계산한다. 정답이 있는 사용자가 2명 미만이면 차이와 구간을 `null`로 남긴다. 출력에는 표집 단위·반복 횟수·시드와 입력 파일 SHA-256이 들어간다.

## 전환 범위

기존 누적 평가가 저장한 `cumulative-request-metrics.jsonl`과 `rolling30-request-metrics.jsonl`도 바로 읽을 수 있다. 한 파일에 여러 순위 방식이 들어 있으므로 `scored` 형식과 비교할 순위 방식을 지정한다. 반복 횟수와 시드는 기존 `protocol.json` 값을 사용한다.

~~~text
python -B -m ml.rec_model.evaluation.comparison --candidate cumulative-request-metrics.jsonl --baseline rolling30-request-metrics.jsonl --input-format scored --candidate-ranking ranker --baseline-ranking ranker --metric ndcg --repeats 1000 --seed 42 --output comparison.json
~~~

새로 채점할 때는 ML 실행에서 만든 후보·추천 순서에 확정 구매 정답을 연결해 기본 형식의 JSONL을 준비한다. 팀 실행을 전환할 때는 동일 표본의 기존 보고서와 새 결과를 먼저 대조한다.
