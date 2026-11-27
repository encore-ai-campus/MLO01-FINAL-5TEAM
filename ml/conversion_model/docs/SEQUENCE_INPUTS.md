# 구매 전환 예측 순서 입력

구매 모델의 학습과 서비스 호출이 같은 최근 행동 목록에서 입력 6개를 계산할 때 `libs.features.purchase_sequence.sequence_inputs`를 사용한다.

구매 코드의 `models/logistic/features.py`는 기존 호출을 공용 함수로 연결한다. 과거 로컬 자료의 정수형 상품 번호는 기존 호환 모드에서만 문자열로 바꾼다. 방문의 행동을 미리 시간순으로 정리한 학습 경로도 같은 입력 계산을 사용한다.

같은 세션에서 첫 구매 전까지, 기준 행동을 포함한 최근 조회·장바구니 행동 1~5개를 전달한다. 반복 행동은 보존한다. 각 행동의 공통 필드는 문자열 `product_id`, `event_type`(view 또는 cart), UTC `event_time_utc`, 원본 순번 `sequence`다. 세션 구분과 최근 행동 조회는 호출 전에 완료한다.

- 팀 목록의 각 행동은 세션 순번 `seq`와 원본 `event_id`를 포함한다. 같은 시각·원본 순번이어도 두 행동을 세션 순번으로 구분한다. 목록의 `seq`는 연속이어야 하고, 원본 시각·순번은 역행할 수 없다. 같은 `event_id`가 반복되거나 일부 행동에만 `seq`가 있으면 입력 오류다.
- 팀 목록을 호출할 때 기준 행동의 `event_id`, `event_time_utc`, `sequence`를 넘긴다. 함수는 `seq`로 정렬한 뒤 마지막 행동이 세 값과 같은지 확인한다.
- `seq`가 없는 기존 저장 목록은 UTC 시각과 원본 순번으로 처리한다. 팀 API는 예측 요청에 이미 담긴 입력 6개를 받는다.

### Redis 반영 작업에서 호출

팀 Redis 반영 작업의 실행 환경에서 저장소 루트의 `libs`를 import할 수 있게 하고, 예측 요청 발행 설정의 `--prediction-input-calculator`에 `libs.features.redis_purchase_inputs:calculate`를 지정한다. `--prediction-input-version`은 학습과 합의한 입력 버전을 사용한다.

발행기는 `calculate(actions)`를 호출한다. 연결 함수는 전달된 목록의 마지막 행동에서 `event_time_utc`, `sequence`, `event_id`를 읽어 기존 공용 계산에 넘긴다. 목록은 `seq` 오름차순으로 연속해야 한다. 순서가 뒤집히거나 빠지면 입력 오류로 중단한다. 같은 시각·같은 원본 순번의 행동도 `seq`와 `event_id`로 구별한다.

| 입력 | B 조회 → D 장바구니 |
| --- | --- |
| `sequence_actions` | 2 |
| `same_product_view_run` | 0 |
| `product_switch_rate` | 1.0 |
| `returned_to_product` | 0 |
| `last_pair` | `view_cart_other` |
| `product_pattern` | `AB` |

저장소 최상위에서 `python -B -m unittest discover -s ml/conversion_model/tests`를 실행한다. 합의 예시, 같은 초의 행동 순서, 반복 조회와 오류 기록을 검증한다. 실제 팀의 학습 자료와 서비스 호출에서 입력이 같은지는 3주 차 연동에서 확인한다.
