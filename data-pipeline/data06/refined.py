"""Local split streaming workers. Each stage has its own Spark checkpoint."""
import argparse
import hashlib
import json
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from time import perf_counter, time

import redis
from pyspark.sql import Window, functions as F

from stream import EVENT_FIELDS, EVENT_SCHEMA, spark_session
from redis_contract import parse_time, user_key, utc_string


def now_ms():
    return int(time() * 1000)


def kafka_stream(spark, args, topics, starting_offsets='earliest'):
    return (spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', args.kafka).option('subscribe', topics)
            .option('startingOffsets', starting_offsets).option('maxOffsetsPerTrigger', args.batch_size).load()
            .select('topic', 'partition', 'offset', F.unix_millis('timestamp').alias('broker_ms'),
                    F.col('key').cast('string').alias('key'), F.col('value').cast('string').alias('value')))


def run_query(frame, checkpoint, callback, args):
    writer = frame.writeStream.option('checkpointLocation', str(checkpoint)).foreachBatch(callback)
    if args.continuous:
        writer = writer.trigger(processingTime=args.trigger)
    else:
        writer = writer.trigger(availableNow=True)
    stop_signal = getattr(args, 'stop_signal', None)
    if stop_signal is not None and stop_signal.is_set():
        return
    query = writer.start()
    if not args.benchmark_progress and stop_signal is None:
        query.awaitTermination()
        return
    last_batch = -1
    try:
        while stop_signal is None or not stop_signal.is_set():
            terminated = query.awaitTermination(0.25)
            if args.benchmark_progress:
                for batch in query.recentProgress:
                    batch_id = batch['batchId']
                    if batch_id <= last_batch or not batch['numInputRows']:
                        continue
                    started_ms = int(datetime.fromisoformat(
                        batch['timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
                    duration_ms = batch['durationMs']['triggerExecution']
                    version = {
                        'stage': args.stage, 'batch': batch_id, 'committed_ms': started_ms + duration_ms,
                        'end_offsets': batch['sources'][0]['endOffset'], 'trigger_ms': duration_ms,
                        'durations_ms': batch['durationMs'],
                    }
                    print('DATA07_SPLIT ' + json.dumps(version), flush=True)
                    last_batch = batch_id
            if terminated:
                break
    finally:
        if query.isActive:
            query.stop()


def decision_state(path, contract):
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS contract (id INTEGER PRIMARY KEY CHECK (id = 1), value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS completed (batch INTEGER PRIMARY KEY, last_event_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS seen (event_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, event_ms INTEGER, source TEXT, seq INTEGER
        );
    """)
    with db:
        db.execute('INSERT OR IGNORE INTO contract VALUES (1, ?)', (contract,))
        if db.execute('SELECT value FROM contract WHERE id = 1').fetchone()[0] != contract:
            raise ValueError('Decision input, output and lateness must stay fixed within one replay')
    return db


def classify(db, event, event_ms, prior_max, allowed_lateness_ms):
    event_id = event['event_id']
    digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
    known = db.execute('SELECT payload_hash FROM seen WHERE event_id = ?', (event_id,)).fetchone()
    if known:
        if known[0] != digest:
            raise ValueError(f'Conflicting payload for event {event_id}')
        return 'duplicate'
    db.execute('INSERT INTO seen VALUES (?, ?)', (event_id, digest))
    user = (event['user_id'],)
    order = (event_ms, event['source'], event['event_seq'])
    previous = db.execute(
        'SELECT event_ms, source, seq FROM users WHERE user_id = ?', user
    ).fetchone()
    if event_ms < prior_max - allowed_lateness_ms or (previous and order < previous):
        return 'late'
    db.execute('INSERT OR REPLACE INTO users VALUES (?, ?, ?, ?)', (*user, *order))
    return 'accepted'


def decision_worker(spark, args, root):
    raw = kafka_stream(spark, args, args.topic)
    parsed = raw.withColumn('event', F.from_json('value', EVENT_SCHEMA, {'mode': 'FAILFAST'}))
    parsed = parsed.withColumn('event_ms', F.unix_millis(F.to_timestamp('event.event_time')))
    contract = json.dumps([args.topic, args.refined_topic, args.allowed_lateness_seconds])
    state_path = root / 'decision.sqlite'
    checkpoint = root / 'decision-checkpoint'
    if (checkpoint / 'commits').exists() and not state_path.exists():
        raise RuntimeError('Decision checkpoint exists without its local state database')

    producer = None
    if args.decision_publisher == 'driver':
        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers=args.kafka, acks='all', enable_idempotence=True,
                                 linger_ms=5, max_block_ms=30_000)

    def process(batch, batch_id):
        entered = now_ms()
        with closing(decision_state(state_path, contract)) as db, db:
            latest = db.execute('SELECT batch, last_event_ms FROM completed ORDER BY batch DESC LIMIT 1').fetchone()
            if latest and latest[0] >= batch_id:
                return
            prior_max = latest[1] if latest else -(2**63)
            outgoing = []
            batch_max = prior_max
            # The Kafka trigger bounds the batch. Sort locally to avoid a Spark shuffle.
            read_started = now_ms()
            rows = sorted(batch.collect(), key=lambda row: (row.partition, row.offset))
            read_ended = now_ms()
            for row in rows:
                event = row.event.asDict() if row.event else {}
                if (not event.get('source') or row.event_ms is None or event['event_seq'] is None
                        or event['event_seq'] < 0 or event['user_id'] is None or event['product_id'] is None
                        or event['event_type'] not in ('view', 'cart', 'purchase')
                        or event['event_id'] != f"{event['source']}:{event['event_seq']}"
                        or row.key != str(event['user_id'])):
                    raise ValueError(f'Invalid action at partition {row.partition}, offset {row.offset}')
                status = classify(db, event, row.event_ms, prior_max, args.allowed_lateness_seconds * 1000)
                batch_max = max(batch_max, row.event_ms)
                message = {'run_id': args.run_id, 'decision_batch': batch_id, 'status': status,
                           'origin_partition': row.partition, 'origin_offset': row.offset,
                           'origin_broker_ms': row.broker_ms, 'decision_ms': now_ms(), **event}
                outgoing.append((str(event['user_id']), json.dumps(message), row.partition, row.offset))
            if not outgoing:
                return
            publish_started = now_ms()
            if producer is None:
                records = spark.createDataFrame(
                    outgoing, 'key string, value string, partition int, origin_offset long')
                (records.coalesce(1).sortWithinPartitions('partition', 'origin_offset')
                 .select('key', 'value', 'partition').write.format('kafka')
                 .option('kafka.bootstrap.servers', args.kafka).option('topic', args.refined_topic).save())
            else:
                # Rows already follow Kafka partition order. Commit SQLite only after broker acknowledgments.
                pending = [producer.send(args.refined_topic, partition=partition, key=key.encode(), value=value.encode())
                           for key, value, partition, _ in outgoing]
                for result in pending:
                    result.get(timeout=30)
            if args.benchmark_progress:
                print('DATA06_DECISION_PROFILE ' + json.dumps({
                    'batch': batch_id, 'entered_ms': entered, 'read_start_ms': read_started,
                    'read_end_ms': read_ended, 'publish_start_ms': publish_started, 'acked_ms': now_ms(),
                    'positions': [[row.partition, row.offset] for row in rows],
                }), flush=True)
            if args.fail == 'decision-after-publish' and batch_id >= args.fail_decision_after_batch:
                raise RuntimeError('INJECTED_DECISION_AFTER_PUBLISH')
            # On failed publication SQLite rolls back. A retry may publish copies.
            db.execute('INSERT INTO completed VALUES (?, ?)', (batch_id, batch_max))

    try:
        run_query(parsed, checkpoint, process, args)
    finally:
        if producer is not None:
            producer.close(timeout=5)


def refined_rows(frame):
    fields = (', '.join(f'{name} {kind.simpleString()}' for name, kind in EVENT_FIELDS.items())
              + ', run_id string, decision_batch long, status string, origin_partition int, origin_offset long'
              + ', origin_broker_ms long, decision_ms long')
    return (frame.select(F.col('partition').alias('refined_partition'), F.col('offset').alias('refined_offset'),
                         F.col('broker_ms').alias('refined_broker_ms'),
                         F.from_json('value', fields, {'mode': 'FAILFAST'}).alias('record'))
            .select('refined_partition', 'refined_offset', 'refined_broker_ms', 'record.*'))


REDIS_APPLY = """
local current = redis.call('GET', KEYS[1])
if current then
    local state = cjson.decode(current)
    if state.origin_partition ~= cjson.null and tostring(state.origin_partition) ~= ARGV[1] then
        return redis.error_reply('User changed Kafka partition within one replay')
    end
    if state.origin_offset ~= cjson.null and (string.len(state.origin_offset) > string.len(ARGV[2])
       or (string.len(state.origin_offset) == string.len(ARGV[2]) and state.origin_offset >= ARGV[2])) then
        return 0
    end
end
if (current or '') ~= ARGV[3] then
    return redis.error_reply('Concurrent writer for this user')
end
redis.call('SET', KEYS[1], ARGV[4])
return 1
"""


def next_user_state(previous, event):
    features = previous.get('features', {})
    now = parse_time(event.event_time)
    last = parse_time(previous['event_time']) if previous else None
    if last and now < last:
        raise ValueError('Accepted events must be in time order for this feature calculation')
    new_session = last is None or now - last > timedelta(minutes=30)
    session_start = event.event_id if new_session else features['session_start_event_id']
    if previous and 'history' not in previous:
        raise ValueError('Redis state uses the old schema; rebuild from Delta before resuming')
    history = previous.get('history', [])
    if history and (now, event.source, event.event_seq) <= (
            last, history[-1]['source'], history[-1]['event_seq']):
        raise ValueError('Accepted event overlaps or precedes the Redis seed')
    history_cutoff = now - timedelta(days=7)
    history = [row for row in history if row['session_start_event_id'] == session_start or
               parse_time(row['event_time']) >= history_cutoff]
    at = utc_string(now)
    history.append({'event_id': event.event_id, 'source': event.source, 'event_seq': event.event_seq,
                    'event_time': at, 'event_type': event.event_type, 'product_id': event.product_id,
                    'session_start_event_id': session_start})
    views = set() if new_session else set(features['viewed_product_ids'])
    carts = set() if new_session else set(features['carted_product_ids'])
    cutoff = (now - timedelta(days=30)).timestamp()
    purchases = {item: at for item, at in previous.get('purchases', {}).items() if at > cutoff}
    if event.event_type == 'view':
        views.add(event.product_id)
    elif event.event_type == 'cart':
        carts.add(event.product_id)
    else:
        purchases[str(event.product_id)] = now.timestamp()
    return {'origin_partition': event.origin_partition,
            'origin_offset': None if event.origin_offset is None else str(event.origin_offset),
            'refined_offset': None if event.refined_offset is None else str(event.refined_offset),
            'event_time': at, 'purchases': purchases, 'history': history,
            'features': {'event_count': features.get('event_count', 0) + 1, 'last_event_id': event.event_id,
                         'last_event_time': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
                         'session_start_event_id': session_start,
                         'viewed_product_ids': sorted(views), 'carted_product_ids': sorted(carts),
                         'purchased_product_ids_30d': sorted(map(int, purchases))}}


def redis_input_state(raw, event):
    previous = json.loads(raw) if raw else {}
    if previous and previous['origin_partition'] not in (None, event.origin_partition):
        raise ValueError('User changed Kafka partition within one replay')
    if previous and previous['origin_offset'] is not None and int(previous['origin_offset']) >= event.origin_offset:
        return None
    return previous


def apply_redis_pipeline(client, apply, run_id, events, publish_latency):
    if not events:
        return 0, 0
    keys = list(dict.fromkeys(user_key(run_id, event.user_id) for event in events))
    current = dict(zip(keys, client.mget(keys)))
    lookup_ended = now_ms()
    pending = []
    skipped = 0
    with client.pipeline(transaction=False) as pipe:
        for event in events:
            key = user_key(run_id, event.user_id)
            raw = current[key]
            previous = redis_input_state(raw, event)
            if previous is None:
                skipped += 1
                continue
            payload = json.dumps(next_user_state(previous, event))
            current[key] = payload
            apply(keys=[key], args=[event.origin_partition, event.origin_offset, raw or '', payload], client=pipe)
            pending.append((event, now_ms()))
        replies = pipe.execute()
    acked = now_ms()
    applied = 0
    for (event, computed), reply in zip(pending, replies, strict=True):
        if reply:
            applied += 1
            publish_latency(event, lookup_ended, computed, acked)
        else:
            skipped += 1
    return applied, skipped


def redis_recovery_plan(args):
    recovery = json.loads(args.redis_recovery.read_text()) if args.redis_recovery else None
    if recovery and (recovery['run_id'] != args.run_id or recovery['refined_topic'] != args.refined_topic):
        raise ValueError('Recovery boundary belongs to another replay')
    if recovery and not re.fullmatch(r'[a-f0-9]{32}', recovery['generation']):
        raise ValueError('Invalid recovery generation')
    return recovery


def redis_worker(spark, args, root):
    recovery = redis_recovery_plan(args)
    starting = json.dumps({args.refined_topic: recovery['restart_offsets']}) if recovery else 'earliest'
    checkpoint = root / (f"redis-recovery-{recovery['generation']}" if recovery else 'redis-checkpoint')
    frame = refined_rows(kafka_stream(spark, args, args.refined_topic, starting))
    with redis.Redis.from_url(args.redis_url, decode_responses=True, socket_timeout=5) as client:
        apply = client.register_script(REDIS_APPLY)

        def process(batch, batch_id):
            skipped, applied, boundary_skipped = 0, 0, 0
            entered = now_ms()
            started = perf_counter()
            accepted = sorted(batch.where("status = 'accepted'").collect(),
                              key=lambda event: (event.refined_partition, event.refined_offset))
            collected = perf_counter()
            read_ended = now_ms()
            profile = []
            eligible = []
            for event in accepted:
                if recovery and event.refined_offset < recovery['boundary'][str(event.refined_partition)]:
                    boundary_skipped += 1
                else:
                    eligible.append(event)

            def publish_latency(event, lookup_ended, computed, acked):
                point = [event.origin_broker_ms, event.decision_ms, event.refined_broker_ms, acked]
                print('DATA06_LATENCY ' + json.dumps(point), flush=True)
                if args.benchmark_progress:
                    profile.append([event.refined_broker_ms, entered, read_ended, lookup_ended, computed, acked])

            if args.redis_batch_writes and not args.fail:
                applied, skipped = apply_redis_pipeline(client, apply, args.run_id, eligible, publish_latency)
            else:
                for event in eligible:
                    key = user_key(args.run_id, event.user_id)
                    raw = client.get(key)
                    lookup_ended = now_ms()
                    previous = redis_input_state(raw, event)
                    if previous is None:
                        skipped += 1
                        continue
                    payload = json.dumps(next_user_state(previous, event))
                    computed = now_ms()
                    if apply(keys=[key], args=[event.origin_partition, event.origin_offset, raw or '', payload]):
                        applied += 1
                        publish_latency(event, lookup_ended, computed, now_ms())
                    if args.fail == 'redis-after-first' and applied == 1:
                        raise RuntimeError('INJECTED_REDIS_AFTER_FIRST')
            stats = {'batch': batch_id, 'applied': applied, 'skipped': skipped}
            if recovery:
                stats['boundary_skipped'] = boundary_skipped
            if args.benchmark_progress:
                stats['collect_sort_ms'] = round((collected - started) * 1000, 2)
                stats['apply_loop_ms'] = round((perf_counter() - collected) * 1000, 2)
                if profile:
                    print('DATA06_REDIS_PROFILE ' + json.dumps({'batch': batch_id, 'points': profile}), flush=True)
            print('DATA06_REDIS ' + json.dumps(stats), flush=True)

        run_query(frame, checkpoint, process, args)


def delta_merge(frame, path, keys):
    if frame.isEmpty():
        return
    table = f'delta.`{path}`'
    if not (path / '_delta_log').is_dir():
        frame.limit(0).write.format('delta').mode('errorifexists').save(str(path))
    frame.createOrReplaceTempView('incoming_refined')
    condition = ' AND '.join(f't.{key} = s.{key}' for key in keys)
    frame.sparkSession.sql(
        f'MERGE INTO {table} t USING incoming_refined s ON {condition} WHEN NOT MATCHED THEN INSERT *'
    )


def delta_worker(spark, args, root):
    frame = kafka_stream(spark, args, f'{args.topic},{args.refined_topic}')

    def process(batch, _batch_id):
        batch.persist()
        try:
            if args.fail == 'delta-before-write' and not batch.isEmpty():
                raise RuntimeError('INJECTED_DELTA_BEFORE_WRITE')
            delta_merge(batch.where(F.col('topic') == args.topic), root / 'raw', ('partition', 'offset'))
            refined = refined_rows(batch.where(F.col('topic') == args.refined_topic))
            by_origin = Window.partitionBy('origin_partition', 'origin_offset').orderBy('refined_offset')
            first_copies = refined.withColumn('n', F.row_number().over(by_origin)).where('n = 1').drop('n')
            delta_merge(first_copies, root / 'refined', ('origin_partition', 'origin_offset'))
            if args.fail == 'delta-after-write' and not batch.isEmpty():
                raise RuntimeError('INJECTED_DELTA_AFTER_WRITE')
        finally:
            batch.unpersist()

    run_query(frame, root / 'delta-checkpoint', process, args)


def inspect_worker(spark, _args, root):
    for name in ('raw', 'refined'):
        path = root / name
        if not (path / '_delta_log').is_dir():
            continue
        if name == 'raw':
            columns = ('partition', 'offset', 'value')
        else:
            columns = ('origin_partition', 'origin_offset', 'refined_partition', 'refined_offset', 'status', 'event_id')
        data = spark.read.format('delta').load(str(path)).select(*columns).orderBy(*columns[:2])
        for row in data.toLocalIterator():
            print('DATA06_INSPECT ' + json.dumps([name, row.asDict()]), flush=True)


def bundled_worker(spark, args, root):
    workers = {'decision': decision_worker, 'redis': redis_worker, 'delta': delta_worker}
    stop_signal = Event()
    with ThreadPoolExecutor(max_workers=len(workers)) as pool:
        tasks = []
        try:
            for stage, worker in workers.items():
                stage_args = argparse.Namespace(**vars(args))
                stage_args.stage = stage
                stage_args.trigger = args.trigger if stage == 'delta' else '1 second'
                stage_args.stop_signal = stop_signal
                tasks.append(pool.submit(worker, spark.newSession(), stage_args, root))
            done, _ = wait(tasks, return_when=FIRST_EXCEPTION)
            for task in done:
                task.result()
        finally:
            stop_signal.set()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', required=True, choices=('decision', 'redis', 'delta', 'inspect', 'bundled'))
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--refined-topic', required=True)
    parser.add_argument('--kafka', default='kafka:9092')
    parser.add_argument('--redis-url', default='redis://redis:6379/0')
    parser.add_argument('--redis-recovery', type=Path)
    parser.add_argument('--allowed-lateness-seconds', type=int, default=60)
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--continuous', action='store_true')
    parser.add_argument('--trigger', default=None)
    parser.add_argument('--decision-publisher', choices=('spark', 'driver'), default='spark')
    parser.add_argument('--redis-batch-writes', action='store_true')
    parser.add_argument('--redis-direct', action='store_true', help='Use the local Kafka polling Redis sink')
    parser.add_argument('--fail-decision-after-batch', type=int, default=0)
    parser.add_argument('--benchmark-progress', action='store_true', help='Log committed Kafka positions')
    parser.add_argument('--fail', choices=('decision-after-publish', 'redis-after-first',
                                           'delta-before-write', 'delta-after-write'))
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9-]+', args.run_id) or args.topic == args.refined_topic:
        parser.error('Use a lowercase run ID and distinct action and refined topics')
    if args.batch_size < 1 or args.allowed_lateness_seconds < 0:
        parser.error('Batch size must be positive and lateness cannot be negative')
    failure_stages = {'decision-after-publish': 'decision', 'redis-after-first': 'redis',
                      'delta-before-write': 'delta', 'delta-after-write': 'delta'}
    if args.fail and failure_stages[args.fail] != args.stage:
        parser.error('Injected failure must match the selected stage')
    if args.stage == 'bundled' and not args.continuous:
        parser.error('Bundled workers require --continuous')
    if args.redis_recovery and args.stage != 'redis':
        parser.error('Redis recovery offsets apply only to the Redis worker')
    if args.redis_direct and (args.stage != 'redis' or not args.continuous or args.fail):
        parser.error('Direct Redis mode requires the continuous Redis worker')
    args.trigger = args.trigger or ('30 seconds' if args.stage in ('delta', 'bundled') else '1 second')
    root = Path('/state') / 'split' / args.run_id
    root.mkdir(parents=True, exist_ok=True)
    if args.redis_direct:
        from redis_kafka import consume

        consume(args, root, redis_recovery_plan(args))
        return
    spark = spark_session(f'data-06-{args.stage}')
    workers = {'decision': decision_worker, 'redis': redis_worker, 'delta': delta_worker,
               'inspect': inspect_worker, 'bundled': bundled_worker}
    try:
        workers[args.stage](spark, args, root)
    finally:
        spark.stop()
