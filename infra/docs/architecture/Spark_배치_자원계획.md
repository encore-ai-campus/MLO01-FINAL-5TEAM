# Spark 일 배치 데이터 품질 검사

최종 프로젝트의 `dev` 브랜치에서 확인한 검사 코드 안내다. 하단에 원본 대조 함수와 Soda 검사 규칙을 실었다. 코드 기준 커밋은 `86d0c6ba4220`이다.

## 팀 원문

| 자료 | 확인할 내용 |
| --- | --- |
| [데이터 파이프라인 README](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/README.md) | 실시간 처리, Delta 적재, 일 배치 품질 검사의 역할 |
| [품질 검사 요구사항 FR-DATA-06](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/6eab3d4d24ab75727bd59294dceaa5fe83738c6b/docs/%EC%9A%94%EA%B5%AC%EC%82%AC%ED%95%AD_%EC%A0%95%EC%9D%98%EC%84%9C.md#L133) · [보고서 요구사항 NFR-16](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/6eab3d4d24ab75727bd59294dceaa5fe83738c6b/docs/%EC%9A%94%EA%B5%AC%EC%82%AC%ED%95%AD_%EC%A0%95%EC%9D%98%EC%84%9C.md#L246) | 실패·알림 검증과 항목별 통과·실패 기준 |
| [품질 검사 규칙 및 보고서 입력 자료 · 팀 이슈 #38](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/issues/38) | Delta 결과 기록, 실패 시 후속 작업 중단, 하루치 검사 시간 측정 기준 |

## 무엇을 검사하나

1. **원본과 정제본의 일치 여부.** Kafka 파티션·오프셋을 기준으로 원본과 정제본을 대조한다. 양쪽 건수가 같아도 한 위치가 빠지고 다른 위치가 추가되면 이를 검출하는 시험이 있다. [대조 함수](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/daily_quality.py#L20-L49) · [시나리오 코드](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/daily_quality.py#L89-L171)
2. **이벤트 품질.** Soda 규칙 10개로 비어 있는 하루, 이벤트 ID 누락·중복, 출처·순번과 ID의 일치, 필수값 누락을 검사한다. 허용하는 행동은 `view`, `cart`, `purchase`다. [원본 규칙](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/events_contract.yaml)
3. **실패 처리.** 정상·규칙 위반·SQL 오류·필수 검사 누락·원본 대조 불일치·재시도 통과 시나리오를 검증한다. 통과한 시도만 후속 집계를 쓰고 실패한 시도는 로컬 알림 JSON을 남긴다. 시도별 판정은 Delta `quality_audit`에 기록한다. [판정](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/quality_gate.py#L36-L62) · [결과 기록](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/quality_gate.py#L65-L70)

## 실행 결과와 예산의 관계

로컬 검증 프로그램은 입력과 기존 Kafka·Delta 결과 경로, 새 출력 경로를 받아 실행한다. 출력에는 `count_report.csv`(건수 대조), `verification.json`(종합 결과), `quality_audit/`(시도별 기록), `alerts/`(실패 상세)가 있다. [실행 인자](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/daily_quality.py#L174-L187)

`quality_audit`의 `elapsed_seconds`는 개별 검사 시도의 측정값이다. 하이브리드 비용을 실측할 때는 배치·재처리·학습을 위해 EC2를 시작한 시각부터 중지한 시각까지를 합산한다.

## 원본 대조 함수

[전체 Python 파일](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/daily_quality.py)

```python
def reconcile(raw, refined):
    raw_keys = raw.groupBy(*KEY).agg(F.count('*').alias('raw_rows'), F.first('event_id').alias('raw_id'))
    refined_keys = refined.groupBy(*KEY).agg(
        F.count('*').alias('refined_rows'), F.first('event_id').alias('refined_id'),
        F.count_if(F.col('status') != 'invalid').alias('valid_id_rows'))
    pairs = raw_keys.join(refined_keys, list(KEY), 'full')
    positions = pairs.agg(
        F.count_if(F.col('refined_rows').isNull()).alias('missing_refined_positions'),
        F.count_if(F.col('raw_rows').isNull()).alias('extra_refined_positions'),
        F.count_if(F.col('raw_rows') > 1).alias('duplicate_raw_positions'),
        F.count_if(F.col('refined_rows') > 1).alias('duplicate_refined_positions'),
        F.count_if(F.col('raw_rows').isNotNull() & (F.col('valid_id_rows') > 0)
                   & ~F.col('raw_id').eqNullSafe(F.col('refined_id'))).alias('event_id_mismatches'),
    ).first().asDict()
    counts = {row['bucket']: row['count'] for row in (
        refined.withColumn('bucket',
                           F.when(F.col('status').isin('duplicate', 'late', 'invalid'), F.col('status'))
                           .when((F.col('status') == 'accepted') & (F.col('origin_type') == 'replay'), 'eligible')
                           .when((F.col('status') == 'accepted') & F.col('origin_type').isin('human_demo', 'load_test'),
                                 F.col('origin_type')).otherwise('unknown'))
        .groupBy('bucket').count().collect())}
    raw_count = raw.count()
    refined_count = sum(counts.values())
    exclusions = {name: counts.get(name, 0) for name in EXCLUSIONS}
    passed = not any(positions.values()) and raw_count == refined_count and counts.get('unknown', 0) == 0
    eligible = counts.get('eligible', 0)
    return {'raw_rows': raw_count, 'refined_rows': refined_count,
            'accepted_rows': eligible + exclusions['human_demo'] + exclusions['load_test'],
            'eligible_rows': eligible, 'excluded': exclusions, 'unknown_rows': counts.get('unknown', 0),
            **positions, 'passed': passed}
```

## 원본 Soda 검사 규칙

[전체 YAML 파일](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/blob/86d0c6ba42200cae1064f7998e82585ae3cdb3ed/data-pipeline/data09/events_contract.yaml)

```yaml
dataset: local_spark/default/events_good
checks:
  - row_count:
      name: nonempty_day
      threshold:
        must_be_greater_than: 0
  - failed_rows:
      name: event_identity_matches_source
      expression: event_id <> concat(source, ':', cast(event_seq as string)) OR event_seq < 0
columns:
  - name: event_id
    checks:
      - missing:
          name: event_id_present
      - duplicate:
          name: event_id_unique
  - name: event_time
    checks:
      - missing:
          name: event_time_present
  - name: event_type
    checks:
      - invalid:
          name: event_type_allowed
          valid_values: [view, cart, purchase]
  - name: source
    checks:
      - missing:
          name: source_present
  - name: event_seq
    checks:
      - missing:
          name: event_seq_present
  - name: user_id
    checks:
      - missing:
          name: user_id_present
  - name: product_id
    checks:
      - missing:
          name: product_id_present
```
