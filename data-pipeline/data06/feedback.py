"""Dedicated Kafka-to-Delta feedback worker for the local interface draft."""
import argparse
import json
import re
from functools import reduce
from operator import and_
from pathlib import Path

from pyspark.sql import Window, functions as F

from refined import delta_merge, kafka_stream, run_query
from stream import spark_session


FIELDS = {
    'log_id': 'string', 'request_id': 'string', 'replay_run_id': 'string', 'source': 'string',
    'log_type': 'string', 'event_time': 'string', 'replay_time': 'string',
    'user_id': 'string', 'session_id': 'string', 'model_version': 'string',
    'feature_as_of': 'string', 'feature_revision': 'string',
    'product_id': 'long', 'position': 'int', 'impression_id': 'string',
    'prediction_id': 'string', 'prediction_probability': 'double',
}
SCHEMA = ', '.join(f'{name} {dtype}' for name, dtype in FIELDS.items())
TIME_FIELDS = ('event_time', 'replay_time', 'feature_as_of')
SOURCES = ('replay', 'human_demo', 'load_test')
FAIL_AFTER_DELTA = 'INJECTED_FEEDBACK_FAILURE_AFTER_DELTA'


def parse(frame):
    parsed = (frame.withColumn('record', F.from_json('value', SCHEMA, {'mode': 'FAILFAST'}))
              .select('partition', 'offset', 'broker_ms', 'key', 'record.*'))
    for field in TIME_FIELDS:
        parsed = parsed.withColumn(field, F.to_timestamp(field))
    return parsed


def validate(rows):
    required = ('log_id', 'request_id', 'replay_run_id', 'session_id', 'model_version', 'feature_revision')
    common = (F.col('key') == F.col('log_id')) & F.col('source').isin(*SOURCES)
    for name in required:
        common = common & (F.length(F.trim(name)) > 0)
    for name in TIME_FIELDS:
        common = common & F.col(name).isNotNull()
    product = (F.col('product_id') > 0) & (F.col('position') > 0)
    no_prediction = F.col('prediction_id').isNull() & F.col('prediction_probability').isNull()
    impression = ((F.col('log_type') == 'impression') & product & no_prediction
                  & F.col('impression_id').isNull())
    click = ((F.col('log_type') == 'click') & product & no_prediction & (F.length(F.trim('impression_id')) > 0))
    prediction = ((F.col('log_type') == 'prediction') & (F.length(F.trim('prediction_id')) > 0)
                  & F.col('prediction_probability').between(0.0, 1.0)
                  & F.col('product_id').isNull() & F.col('position').isNull() & F.col('impression_id').isNull())
    if rows.where(~F.coalesce(common & (impression | click | prediction), F.lit(False))).take(1):
        raise ValueError('Invalid feedback record or Kafka key')


def ingest(spark, args, root):
    checkpoint = root / 'checkpoint'
    table = root / 'logs'
    source = parse(kafka_stream(spark, args, args.topic))

    def process(batch, batch_id):
        rows = batch.persist()
        try:
            if rows.isEmpty():
                return
            validate(rows)
            hashed = rows.withColumn('payload_hash', F.sha2(
                F.to_json(F.struct(*FIELDS), {'ignoreNullFields': 'false'}), 256))
            key_fields = ('replay_run_id', 'log_id')
            conflict = (hashed.groupBy(*key_fields).agg(F.countDistinct('payload_hash').alias('hashes'))
                        .where('hashes > 1').take(1))
            if conflict:
                raise ValueError('Conflicting payload for the same feedback log ID')
            window = Window.partitionBy(*key_fields).orderBy('partition', 'offset')
            canonical = (hashed.withColumn('first_copy', F.row_number().over(window))
                         .where('first_copy = 1').drop('first_copy', 'key'))
            if (table / '_delta_log').is_dir():
                existing = spark.read.format('delta').load(str(table)).select(
                    *key_fields, F.col('payload_hash').alias('stored_hash'))
                if canonical.join(existing, list(key_fields)).where('payload_hash <> stored_hash').take(1):
                    raise ValueError('Conflicting payload for the same feedback log ID')
            delta_merge(canonical, table, key_fields)
            if args.fail_after_delta:
                raise RuntimeError(FAIL_AFTER_DELTA)
            print(f'FEEDBACK_COMMITTED batch={batch_id}', flush=True)
        finally:
            rows.unpersist()

    run_query(source, checkpoint, process, args)


def inspect(spark, root):
    rows = spark.read.format('delta').load(str(root / 'logs'))
    context = ('replay_run_id', 'request_id', 'source', 'session_id')
    click = rows.where("log_type = 'click'").alias('c')
    impression = rows.where("log_type = 'impression'").alias('i')
    prediction = rows.where("log_type = 'prediction'").alias('p')
    click_key = [F.col(f'c.{name}') == F.col(f'i.{name}') for name in (*context, 'product_id', 'position')]
    linked = (click.join(impression, (F.col('c.impression_id') == F.col('i.log_id'))
                         & reduce(and_, click_key), 'left')
              .select('c.*', F.col('i.log_id').alias('matched_impression')))
    predictions = prediction.select(*context, F.col('prediction_id').alias('matched_prediction_id'))
    linked = linked.join(predictions, list(context), 'left')
    for row in rows.select(*FIELDS, 'partition', 'offset').orderBy('replay_run_id', 'log_id').toLocalIterator():
        print('FEEDBACK_ROW ' + json.dumps(row.asDict(recursive=True), default=str), flush=True)
    for row in linked.select('replay_run_id', 'source', 'log_id', 'matched_impression',
                             F.col('matched_prediction_id').alias('prediction_id')).orderBy(
            'replay_run_id', 'log_id').toLocalIterator():
        print('FEEDBACK_JOIN ' + json.dumps(row.asDict()), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feedback-stage', choices=('ingest', 'inspect'), required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--kafka', default='kafka:9092')
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--continuous', action='store_true')
    parser.add_argument('--trigger', default='30 seconds')
    parser.add_argument('--fail-after-delta', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9-]+', args.run_id) or args.batch_size <= 0:
        parser.error('Use a lowercase run ID and a positive batch size')
    args.benchmark_progress = False
    root = Path('/state') / 'feedback' / args.run_id
    root.mkdir(parents=True, exist_ok=True)
    spark = spark_session(f'data-feedback-{args.feedback_stage}')
    try:
        if args.feedback_stage == 'ingest':
            ingest(spark, args, root)
        else:
            inspect(spark, root)
    finally:
        spark.stop()
