"""Local Feast and Soda v4 evaluation on the intended Spark/Delta versions."""
import argparse
import json
import math
import platform
import statistics
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
from pathlib import Path
from time import perf_counter

import pandas as pd
import redis
import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


DAY = '2019-11-17'
NEXT_DAY = '2019-11-18'
REDIS = 'redis:6379'
PACKAGES = ('pyspark', 'delta-spark', 'feast', 'soda-sparkdf', 'soda-core', 'redis', 'prometheus-client')


def spark_session(root):
    builder = (SparkSession.builder.master('local[2]').appName('data09-tools')
               .config('spark.ui.enabled', 'false')
               .config('spark.sql.shuffle.partitions', '4')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.warehouse.dir', (root / 'warehouse').as_uri())
               .config('spark.jars.ivy', '/ivy')
               .config('spark.jars.repositories', 'https://repo.maven.apache.org/maven2')
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def verify_feast(spark, root):
    from feast import Entity, FeatureStore, FeatureView, Field
    from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
    from feast.types import Int64

    root.mkdir()
    config = {
        'project': 'data09_batch', 'registry': str(root / 'registry.db'), 'provider': 'local',
        'offline_store': {'type': 'spark', 'spark_conf': {'spark.sql.session.timeZone': 'UTC'}},
        'online_store': {'type': 'redis', 'connection_string': REDIS},
        'entity_key_serialization_version': 3,
    }
    (root / 'feature_store.yaml').write_text(yaml.safe_dump(config))
    ts = lambda hour: datetime(2019, 11, 17, hour, tzinfo=timezone.utc)
    (spark.createDataFrame([(71, 3, ts(1), ts(2)), (71, 9, ts(4), ts(6)), (72, 4, ts(1), ts(2))],
                           'user_id long, view_count long, observed_at timestamp, available_at timestamp')
     .write.format('delta').saveAsTable('batch_features'))
    source = SparkSource(name='batch_delta', table='default.batch_features', timestamp_field='available_at')
    user = Entity(name='user', join_keys=['user_id'])
    view = FeatureView(name='batch_user', entities=[user], ttl=timedelta(days=1),
                       schema=[Field(name='view_count', dtype=Int64)], source=source)
    store = FeatureStore(repo_path=str(root))
    store.apply([user, view])
    entity_rows = pd.DataFrame({'user_id': [71, 71, 71, 72], 'event_timestamp': [ts(5), ts(6), ts(7), ts(7)]})
    historical = store.get_historical_features(entity_df=entity_rows, features=['batch_user:view_count']).to_df()
    ordered = historical.sort_values(['user_id', 'event_timestamp'])['view_count'].tolist()
    ordered = list(map(int, ordered))
    assert ordered[0] == 3 and ordered[2:] == [9, 4], ordered
    adjusted_queries = entity_rows.assign(
        request_time=entity_rows['event_timestamp'],
        event_timestamp=entity_rows['event_timestamp'] - pd.Timedelta(microseconds=1))
    adjusted = store.get_historical_features(
        entity_df=adjusted_queries, features=['batch_user:view_count']).to_df()
    adjusted_values = list(map(int, adjusted.sort_values(['user_id', 'request_time'])['view_count']))

    with redis.Redis(host='redis') as client:
        reads = []
        for start, end in [(ts(0), ts(5)), (ts(5), ts(7)), (ts(5), ts(7)), (ts(0), ts(5))]:
            before = perf_counter()
            store.materialize(start, end, feature_views=['batch_user'])
            materialize_seconds = perf_counter() - before
            samples = []
            for _ in range(20):
                then = perf_counter()
                online = store.get_online_features(features=['batch_user:view_count'],
                                                   entity_rows=[{'user_id': 71}, {'user_id': 72}]).to_dict()
                samples.append((perf_counter() - then) * 1000)
            reads.append({'materialize_seconds': round(materialize_seconds, 3), 'values': online['view_count'],
                          'redis_keys': client.dbsize(), 'online_lookup_median_ms': round(statistics.median(samples), 3),
                          'online_lookup_p95_ms': round(sorted(samples)[math.ceil(.95 * len(samples)) - 1], 3)})
        redis_server = client.info('server')['redis_version']
    return {'redis_server': redis_server, 'historical_values': ordered,
            'strictly_before_expected': [3, 3, 9, 4], 'strict_availability_passed': ordered == [3, 3, 9, 4],
            'cutoff_minus_one_microsecond_values': adjusted_values,
            'adjusted_cutoff_passed': adjusted_values == [3, 3, 9, 4],
            'materializations': reads,
            'duplicate_free_retry': reads[1]['values'] == reads[2]['values']
            and reads[1]['redis_keys'] == reads[2]['redis_keys'],
            'older_replay_preserved_latest': reads[3]['values'] == reads[2]['values']}


def verify_soda(spark, root, input_path):
    from soda_sparkdf import SparkDataFrameDataSource
    from quality_gate import append_audit, evaluate, required_checks

    root.mkdir()
    data = spark.read.parquet(input_path).where(
        (F.col('event_time') >= F.lit(DAY).cast('timestamp'))
        & (F.col('event_time') < F.lit(NEXT_DAY).cast('timestamp')))
    row_count = data.count()
    assert row_count > 10000
    data.write.format('delta').saveAsTable('events_good')
    good = spark.table('events_good')
    good.unionByName(good.limit(1)).write.format('delta').saveAsTable('events_duplicate')
    source = SparkDataFrameDataSource.from_existing_session(session=spark, name='local_spark')
    contract = Path(__file__).with_name('events_contract.yaml').read_text()
    required = required_checks(contract)
    result_rows = []
    for attempt, case, expected in [(1, 'good', 'pass'), (1, 'violation', 'violation'),
                                    (1, 'sql_error', 'execution_error'), (1, 'missing_check', 'missing_check'),
                                    (2, 'violation', 'pass')]:
        definition = yaml.safe_load(contract)
        if case == 'violation' and attempt == 1:
            definition['dataset'] = 'local_spark/default/events_duplicate'
        if case == 'sql_error':
            definition['checks'][1]['failed_rows']['expression'] = 'unavailable_column > 0'
        if case == 'missing_check':
            del definition['checks'][1]
        path = root / f'{case}-{attempt}.yaml'
        path.write_text(yaml.safe_dump(definition, sort_keys=False))
        result = evaluate(source, path, required)
        status, evaluated, passed, failed, _, _, errors = result
        print(f'Soda {case} attempt {attempt}: {status}; checks={evaluated} passed={passed} failed={failed}',
              flush=True)
        assert status == expected, (case, status, errors)
        # Use a local stand-in for downstream work and preserve an actionable alert on failure.
        action = root / ('downstream' if status == 'pass' else 'alerts')
        action.mkdir(exist_ok=True)
        marker = action / f'{case}-{attempt}.json'
        marker.write_text(json.dumps({'case': case, 'attempt': attempt, 'status': status}))
        audit = (root / 'quality_audit').as_posix()
        result_rows.append(append_audit(spark, audit, case, attempt, result))
    assert sorted(path.stem for path in (root / 'downstream').glob('*.json')) == ['good-1', 'violation-2']
    assert sorted(path.stem for path in (root / 'alerts').glob('*.json')) == [
        'missing_check-1', 'sql_error-1', 'violation-1']
    saved = spark.read.format('delta').load(audit).orderBy('case', 'attempt')
    assert [tuple(row) for row in saved.collect()] == sorted(result_rows)
    saved.coalesce(1).write.mode('errorifexists').option('header', True).csv((root / 'quality_report').as_posix())
    return {'input_utc': DAY, 'input_rows': row_count, 'required_checks': sorted(required),
            'audit_rows': [row.asDict() for row in saved.collect()]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('tool', choices=('feast', 'soda'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--input', default='/input/2019-Nov-sample.parquet')
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True)
    spark = spark_session(root)
    spark.sparkContext.setLogLevel('ERROR')
    try:
        data = (verify_feast(spark, root / 'feast') if args.tool == 'feast'
                else verify_soda(spark, root / 'soda', args.input))
        report = {'python': platform.python_version(), 'versions': {name: version(name) for name in PACKAGES},
                  'spark_runtime': spark.version, 'tool': args.tool, 'verification': data}
        (root / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, indent=2), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
