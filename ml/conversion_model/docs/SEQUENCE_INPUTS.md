# 구매 전환 예측 순서 입력

구매 모델의 학습과 서비스 호출이 같은 최근 행동 목록에서 입력 6개를 계산할 때 `libs.features.purchase_sequence.sequence_inputs`를 사용한다.

구매 코드의 `models/logistic/features.py`는 기존 호출을 공용 함수로 연결한다. 과거 로컬 자료의 정수형 상품 번호는 기존 호환 모드에서만 문자열로 바꾼다. 방문의 행동을 미리 시간순으로 정리한 학습 경로도 같은 입력 계산을 사용한다.

같은 세션에서 첫 구매 전까지, 기준 행동을 포함한 최근 조회·장바구니 행동 1~5개를 전달한다. 반복 행동은 보존한다. 각 행동에는 문자열 `product_id`, `event_type`(view 또는 cart), UTC `event_time_utc`, 0 이상의 정수 `sequence`가 필요하다. 요청의 시각과 순서 번호도 함께 넘긴다. 함수는 시각·순서로 정렬하고 기준 행동 이후의 기록이나 중복된 순서 키를 거부한다. 세션 구분과 최근 행동 조회는 호출 전에 완료한다.

| 입력 | B 조회 → D 장바구니 |
| --- | --- |
| `sequence_actions` | 2 |
| `same_product_view_run` | 0 |
| `product_switch_rate` | 1.0 |
| `returned_to_product` | 0 |
| `last_pair` | `view_cart_other` |
| `product_pattern` | `AB` |

저장소 최상위에서 `python -B -m unittest discover -s ml/conversion_model/tests`를 실행한다. 합의 예시, 같은 초의 행동 순서, 반복 조회와 오류 기록을 검증한다. 실제 팀의 학습 자료와 서비스 호출에서 입력이 같은지는 3주 차 연동에서 확인한다.
