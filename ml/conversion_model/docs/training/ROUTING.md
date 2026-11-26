# 구매 전환 예측 10월 학습 실행

저장소 루트에서 Python 3.12와 `ml/conversion_model/requirements-training.txt`의 버전을 사용한다.

```text
python -m ml.conversion_model.workflows.train_october --config ml/conversion_model/config/october_training.json --sample <사용자_명단_명세.json> --snapshot <준비된_관측과_정답_폴더> --out <새_출력_폴더>
```

- 사용자 명단 명세는 `version`과 `tiers`를 담은 명세를 받는다. 사용 비율에 해당하는 사용자 목록의 파일명·인원·내용 SHA-256을 검사한다. 입력 폴더의 메타데이터에도 사용자 명단이 기록되어 있으면 설정과 일치해야 한다.
- 입력 폴더에는 `snapshot.json`, `observations.jsonl`, `visits.jsonl`, `truth.jsonl`이 있어야 한다. 기존 전처리에서 만든 `sequence-input-v1`·`purchase-label-v3` 준비 자료를 사용한다. 관측의 예측 번호를 정답과 연결하고, 세션 종료와 정답 확정 시각을 확인한다.
- 출력 폴더에는 `logistic.json`, `lightgbm.txt`, `preprocessor.json`, `report.json`을 저장한다. 보고서에서 설정·사용자 명단·입력·모델 해시, 기간별 행 수, 사용한 라이브러리 버전과 저장 후 확률 재현 결과를 확인할 수 있다.
- 표본 시험은 `python -m unittest discover -s ml/conversion_model/tests -v`로 실행한다.
