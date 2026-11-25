"""Local Data-06 Kafka to Delta and Redis integration."""
import argparse
import hashlib
import json
import re
import sys
from datetime import timedelta
from itertools import groupby
from pathlib import Path
from time import monotonic, time

import redis
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


EVENT_FIELDS = {
    'event_id': StringType(), 'source': StringType(), 'event_seq': LongType(),
    'event_time': StringType(), 'event_type': StringType(), 'product_id': LongType(),
    'category_id': LongType(), 'category_code': StringType(), 'brand': StringType(),
    'price': DoubleType(), 'user_id': LongType(), 'user_session': StringType(),
}
EVENT_SCHEMA = StructType([StructField(name, kind) for name, kind in EVENT_FIELDS.items()])


def user_state(events):
    previous = None
    session_start = None
    views, carts, purchases = set(), set(), {}
    count = 0
    for event in events:
        now = event.event_ts
        if previous is None or now - previous.event_ts > timedelta(minutes=30):
            session_start = event.event_id
            views, carts = set(), set()
        if event.event_type == 'view':
            views.add(event.product_id)
        elif event.event_type == 'cart':
            carts.add(event.product_id)
        else:
            purchases[event.product_id] = now
        previous = event
        count += 1
    return {'event_count': count, 'last_event_id': previous.event_id,
            'last_event_time': previous.event_ts.isoformat(timespec='seconds') + 'Z',
            'session_start_event_id': session_start, 'viewed_product_ids': sorted(views),
            'carted_product_ids': sorted(carts),
            'purchased_product_ids_30d': sorted(
                item for item, at in purchases.items() if at > previous.event_ts - timedelta(days=30))}


def write_redis(spark, batch, table, client, namespace):
    users = batch.select('user_id').distinct()
    history = spark.table(table).join(users, 'user_id').select(
        'user_id', 'event_id', 'event_ts', 'source', 'event_seq', 'event_type', 'product_id'
    ).orderBy('user_id', 'event_ts', 'source', 'event_seq')
    finished = {}
    for user_id, events in groupby(history.toLocalIterator(), key=lambda row: row.user_id):
        # Replace the committed state, so outcomes are identical on retries.
        client.set(f'data06:{namespace}:user:{user_id}', json.dumps(user_state(events), sort_keys=True))
        finished[user_id] = int(time() * 1000)
    return finished


def poll(spark, args, path):
    table = f'delta.`{path}`'
    client = redis.Redis.from_url(args.redis_url, socket_timeout=15)
    client.ping()
    raw = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', args.kafka)
           .option('subscribe', args.topic).option('startingOffsets', 'earliest')
           .option('maxOffsetsPerTrigger', 1000).load())
    parsed = (raw.select(F.col('key').cast('string').alias('kafka_key'), 'offset',
                         F.unix_millis('timestamp').alias('kafka_ms'),
                         F.from_json(F.col('value').cast('string'), EVENT_SCHEMA,
                                     {'mode': 'FAILFAST'}).alias('event'))
              .select('kafka_key', 'offset', 'kafka_ms', 'event.*').withColumn('event_ts', F.to_timestamp('event_time')))
    latest_batch = None

    def process(batch, batch_id):
        nonlocal latest_batch
        started = monotonic()
        rows = batch.persist()
        try:
            if rows.isEmpty():
                return
            valid = (F.col('event_id').isNotNull() & F.col('source').isNotNull() &
                     (F.length('source') > 0) & F.col('event_ts').isNotNull() &
                     (F.col('event_seq') >= 0) & F.col('user_id').isNotNull() &
                     F.col('product_id').isNotNull() & F.col('event_type').isin('view', 'cart', 'purchase') &
                     (F.col('event_id') == F.concat_ws(':', 'source', F.col('event_seq').cast('string'))) &
                     (F.col('kafka_key') == F.col('event_id')))
            if rows.where(~F.coalesce(valid, F.lit(False))).take(1):
                raise ValueError('Invalid Kafka event or key')
            hashed = (rows.drop('kafka_key', 'offset', 'kafka_ms').withColumn(
                'payload_hash', F.sha2(F.to_json(F.struct(*EVENT_FIELDS), {'ignoreNullFields': 'false'}), 256)))
            if hashed.groupBy('event_id').agg(F.countDistinct('payload_hash').alias('n')).where('n > 1').take(1):
                raise ValueError('One event ID has conflicting payloads in this batch')
            incoming = hashed.dropDuplicates(['event_id'])
            session = incoming.sparkSession
            if not (path / '_delta_log').is_dir():
                incoming.limit(0).write.format('delta').mode('errorifexists').save(str(path))
            existing = session.table(table).select('event_id', F.col('payload_hash').alias('stored_hash'))
            if incoming.join(existing, 'event_id').where('payload_hash <> stored_hash').take(1):
                raise ValueError('An existing event ID has a conflicting payload')
            incoming.createOrReplaceTempView('incoming_data06')
            session.sql(f'MERGE INTO {table} t USING incoming_data06 s ON t.event_id = s.event_id '
                        'WHEN NOT MATCHED THEN INSERT *')
            if args.fail_after_delta:
                raise RuntimeError('DATA06_TEST_FAILURE_AFTER_DELTA')
            kafka_rows = rows.select('offset', 'kafka_ms', 'user_id').collect() if args.through_offset else ()
            finished = write_redis(session, incoming, table, client, args.run_id)
            if kafka_rows:
                end_offset = max(row.offset for row in kafka_rows) + 1
                points = [[row.offset, row.kafka_ms, finished[row.user_id]] for row in kafka_rows]
                progress = {'batch_id': batch_id, 'next_offset': end_offset, 'events': len(points),
                            'elapsed_seconds': monotonic() - started, 'points': points}
                client.set(f'data06:{args.run_id}:progress', json.dumps(
                    {key: value for key, value in progress.items() if key != 'points'}))
                print('DATA07_BATCH ' + json.dumps(progress), flush=True)
                latest_batch = (batch_id, end_offset)
            else:
                print(json.dumps({'batch_id': batch_id, 'delta_and_redis_applied': True}), flush=True)
        finally:
            rows.unpersist()

    writer = parsed.writeStream.option('checkpointLocation', f'/state/{args.run_id}/checkpoint').foreachBatch(process)
    if not args.through_offset:
        writer.trigger(availableNow=True).start().awaitTermination()
        return
    query = writer.trigger(processingTime='1 second').start()
    ready = False
    while not query.awaitTermination(1):
        progress = query.lastProgress
        if progress is None:
            continue
        if not ready:
            print('DATA07_READY', flush=True)
            ready = True
        if latest_batch is not None and progress['batchId'] >= latest_batch[0] and latest_batch[1] >= args.through_offset:
            query.stop()
            break


def inspect(spark, path):
    events = spark.read.format('delta').load(str(path))
    summary = events.agg(F.count('*').alias('rows'), F.countDistinct('event_id').alias('unique_ids')).first().asDict()
    digest = hashlib.sha256()
    for row in events.select('event_id').orderBy('event_id').toLocalIterator():
        digest.update((row.event_id + '\n').encode('utf-8'))
    summary['event_id_sha256'] = digest.hexdigest()
    print('DATA06_DELTA ' + json.dumps(summary), flush=True)


def spark_session(app_name):
    spark = (SparkSession.builder.appName(app_name)
             .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
             .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.adaptive.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--run-id', required=True)
    cli.add_argument('--kafka', default='kafka:9092')
    cli.add_argument('--topic', required=True)
    cli.add_argument('--redis-url', default='redis://redis:6379/0')
    cli.add_argument('--fail-after-delta', action='store_true')
    cli.add_argument('--inspect', action='store_true')
    cli.add_argument('--through-offset', type=int)
    opts = cli.parse_args()
    if not re.fullmatch(r'[a-z0-9-]+', opts.run_id):
        cli.error('run-id must contain lowercase letters, digits or hyphens')
    delta_path = Path('/state') / opts.run_id / 'delta'
    spark = spark_session('data-06-local')
    try:
        if opts.inspect:
            inspect(spark, delta_path)
        else:
            poll(spark, opts, delta_path)
    finally:
        spark.stop()


if __name__ == '__main__':
    if '--feedback-stage' in sys.argv:
        from feedback import main as feedback_main
        feedback_main()
    elif '--recover-redis' in sys.argv:
        from recover_redis import main as recover_main
        recover_main()
    elif '--bootstrap-input' in sys.argv:
        from bootstrap_redis import main as bootstrap_main
        bootstrap_main()
    elif '--stage' in sys.argv:
        from refined import main as refined_main
        refined_main()
    else:
        main()
