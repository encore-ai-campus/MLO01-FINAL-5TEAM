"""Run a local daily quality gate on Delta with count reconciliation and a Soda contract."""
import argparse
import csv
import json
from pathlib import Path
from time import perf_counter

import yaml
from pyspark.sql import Window, functions as F
from soda_sparkdf import SparkDataFrameDataSource

from quality_gate import append_audit, evaluate, required_checks
from verify_tools import DAY, NEXT_DAY, spark_session


KEY = ('partition', 'offset')
EXCLUSIONS = ('duplicate', 'late', 'invalid', 'human_demo', 'load_test')


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


def verified_pipeline_counts(spark, path):
    raw = spark.read.format('delta').load(str(path / 'raw')).select(
        *KEY, F.get_json_object('value', '$.event_id').alias('event_id'),
        F.coalesce(F.get_json_object('value', '$.origin_type'), F.lit('replay')).alias('origin_type'))
    refined = spark.read.format('delta').load(str(path / 'refined')).select(
        F.col('origin_partition').alias('partition'), F.col('origin_offset').alias('offset'), 'event_id', 'status')
    return reconcile(raw, refined.join(raw.select(*KEY, 'origin_type'), list(KEY), 'left'))


def stage_day(spark, input_path):
    data = spark.read.parquet(input_path).where(
        (F.col('event_time') >= F.lit(DAY).cast('timestamp'))
        & (F.col('event_time') < F.lit(NEXT_DAY).cast('timestamp')))
    order = Window.orderBy('event_time', 'source', 'event_seq')
    base = (data.withColumn('offset', F.row_number().over(order).cast('long'))
            .withColumn('partition', F.lit(0)).withColumn('origin_type', F.lit('replay'))
            .withColumn('status', F.lit('accepted')))
    duplicates = (base.where('offset <= 2').withColumn('offset', F.col('offset') + 1_000_000)
                  .withColumn('status', F.lit('duplicate')))
    extras = (base.where('offset <= 3').withColumn('offset', F.col('offset') + 2_000_000)
              .withColumn('event_seq', F.col('event_seq') + 1_000_000_000)
              .withColumn('event_id', F.concat_ws(':', 'source', F.col('event_seq').cast('string')))
              .withColumn('status', F.when(F.col('offset') == 2_000_001, 'late').otherwise('accepted'))
              .withColumn('origin_type', F.when(F.col('offset') == 2_000_002, 'human_demo')
                          .when(F.col('offset') == 2_000_003, 'load_test').otherwise('replay')))
    classified = base.unionByName(duplicates).unionByName(extras)
    classified.write.format('delta').saveAsTable('daily_stage')
    saved = spark.table('daily_stage')
    # Materialize bronze and silver separately, like the streaming pipeline.
    saved.drop('status').write.format('delta').saveAsTable('daily_raw')
    saved.write.format('delta').saveAsTable('daily_refined')
    eligible = saved.where("status = 'accepted' AND origin_type = 'replay'").select(*data.columns)
    eligible.write.format('delta').saveAsTable('events_good')
    good = spark.table('events_good')
    good.unionByName(good.limit(1)).write.format('delta').saveAsTable('events_duplicate')


def run(spark, root, input_path, pipeline_path):
    pipeline = verified_pipeline_counts(spark, pipeline_path)
    assert pipeline['passed'] and pipeline['excluded']['duplicate'] > 0 and pipeline['excluded']['late'] > 0
    print('Existing Kafka/Delta run reconciled', flush=True)
    stage_day(spark, input_path)
    raw = spark.table('daily_raw')
    refined = spark.table('daily_refined')
    good = spark.table('events_good')
    counts = reconcile(raw, refined)
    assert counts['passed'] and counts['excluded'] == {
        'duplicate': 2, 'late': 1, 'invalid': 0, 'human_demo': 1, 'load_test': 1}
    assert good.count() == counts['eligible_rows']
    print(f'Day counts reconciled: {counts}', flush=True)

    source = SparkDataFrameDataSource.from_existing_session(session=spark, name='local_spark')
    contract = Path(__file__).with_name('events_contract.yaml').read_text()
    required = required_checks(contract)
    audit = root / 'quality_audit'
    cases = [
        ('good', 1, 'pass'), ('violation', 1, 'violation'), ('sql_error', 1, 'execution_error'),
        ('missing_check', 1, 'missing_check'), ('count_swap', 1, 'count_mismatch'), ('violation', 2, 'pass'),
    ]
    actual = []
    for case, attempt, expected in cases:
        definition = yaml.safe_load(contract)
        if expected == 'violation':
            definition['dataset'] = 'local_spark/default/events_duplicate'
        if case == 'sql_error':
            definition['checks'][1]['failed_rows']['expression'] = 'unavailable_column > 0'
        if case == 'missing_check':
            del definition['checks'][1]
        contract_path = root / f'{case}-{attempt}.yaml'
        contract_path.write_text(yaml.safe_dump(definition, sort_keys=False))
        current = counts
        if case == 'count_swap':
            started = perf_counter()
            changed = refined.withColumn('offset', F.when(F.col('offset') == 1, 3_000_001).otherwise(F.col('offset')))
            current = reconcile(raw, changed)
            assert current['raw_rows'] == current['refined_rows']
            assert current['missing_refined_positions'] == current['extra_refined_positions'] == 1
            reconcile_seconds = round(perf_counter() - started, 3)
        else:
            reconcile_seconds = 0.0
        if current['passed']:
            result = evaluate(source, contract_path, required)
        else:
            result = ('count_mismatch', 0, 0, 0, reconcile_seconds, 0, 'Raw and refined input failed reconciliation')
        status = result[0]
        append_audit(spark, audit, case, attempt, result)
        assert status == expected, result
        if status == 'pass':
            good.groupBy('event_type').count().write.json(str(root / 'downstream' / f'{case}-{attempt}'))
        else:
            target = root / 'alerts' / f'{case}-{attempt}.json'
            target.parent.mkdir(exist_ok=True)
            target.write_text(json.dumps({'case': case, 'attempt': attempt, 'status': status,
                                          'details': result[-1], 'checks_observed': result[1],
                                          'checks_required': len(required), 'failed_checks': result[3],
                                          'counts': current}, indent=2))
        print(f'{case}-{attempt}: {status}', flush=True)
        actual.append({'case': case, 'attempt': attempt, 'status': status, 'counts': current})

    saved = [row.asDict() for row in spark.read.format('delta').load(str(audit)).orderBy('case', 'attempt').collect()]
    assert {(row['case'], row['attempt'], row['status']) for row in saved} == set(cases)
    downstream = sorted(path.name for path in (root / 'downstream').iterdir())
    alerts = sorted(path.stem for path in (root / 'alerts').glob('*.json'))
    assert downstream == ['good-1', 'violation-2']
    assert alerts == ['count_swap-1', 'missing_check-1', 'sql_error-1', 'violation-1']
    for name in downstream:
        assert spark.read.json(str(root / 'downstream' / name)).agg(F.sum('count')).first()[0] == counts['eligible_rows']
    with (root / 'count_report.csv').open('w', newline='', encoding='utf-8') as file:
        rows = []
        for scope, results in [('daily_sample', counts), ('kafka_delta_snapshot', pipeline),
                               ('injected_position_swap', actual[4]['counts'])]:
            rows.append({'scope': scope, **{name: value for name, value in results.items() if name != 'excluded'},
                         **{f'excluded_{name}': value for name, value in results['excluded'].items()}})
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    report = {'utc_day': DAY, 'spark': spark.version, 'daily_sample': counts,
              'existing_kafka_delta_snapshot': pipeline, 'soda_checks': sorted(required),
              'cases': actual, 'audit_rows': saved, 'downstream_outputs': downstream, 'local_alerts': alerts}
    (root / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--pipeline-path', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True)
    spark = spark_session(root)
    spark.sparkContext.setLogLevel('ERROR')
    try:
        run(spark, root, args.input, args.pipeline_path)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
