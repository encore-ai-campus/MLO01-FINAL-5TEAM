"""Local split streaming workers. Each stage has its own Spark checkpoint."""
import argparse
import hashlib
import json
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from time import perf_counter, time

import redis
import event_schema as fields
from pyspark.sql import Window, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from stream import EVENT_FIELDS, EVENT_SCHEMA, spark_session
from redis_contract import parse_time, user_key, utc_string
from recent_actions import ACTION_TYPES, ConversionRequests, apply_list_event, session_keys


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
                    if getattr(args, 'profile_decision_state', False):
                        print('DATA07_STATE ' + json.dumps({'batch': batch_id, 'operators': batch['stateOperators']}),
                              flush=True)
                    last_batch = batch_id
            if terminated:
                break
    finally:
        if query.isActive:
            query.stop()


DECISION_POLICY = 'partition-event-time-quarantine-v1'
INVALID = 'invalid'
ACTION_MESSAGE_FIELDS = {
    'raw_session_id': StringType(), 'replay_run_id': StringType(), 'sequence': LongType(),
    'event_time_utc': StringType(), 'replay_time_utc': StringType(),
    'source_file': StringType(), 'source_row': LongType(),
}
WEB_EVENT_ID = re.compile(r'web:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', re.IGNORECASE)


def action_source(event):
    return 'web' if event.get('source') == 'web' else 'replay'


def decision_group(event, key, run_id):
    source = action_source(event)
    if source == 'web':
        return source, event.get('replay_run_id'), event['raw_session_id']
    # Replay keys use user_id. The source data's session value can be shared by users.
    return source, event.get('replay_run_id') or run_id, key


def replay_position(event):
    return (event.get('source_file') or event.get(fields.SOURCE_FILE),
            event.get('source_row') if event.get('source_row') is not None else event.get(fields.SOURCE_ROW))


def decision_order(event, event_ms, partition, offset):
    if action_source(event) == 'web':
        return event_ms, '', event['sequence'], partition, offset
    source_file, source_row = replay_position(event)
    return event_ms, source_file, source_row, partition, offset


def decision_state(path, contract):
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS contract (id INTEGER PRIMARY KEY CHECK (id = 1), value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS completed (batch INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS partition_clock (partition INTEGER PRIMARY KEY, last_event_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS seen (event_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, event_ms INTEGER, source TEXT, seq INTEGER
        );
    """)
    with db:
        db.execute('INSERT OR IGNORE INTO contract VALUES (1, ?)', (contract,))
        if db.execute('SELECT value FROM contract WHERE id = 1').fetchone()[0] != contract:
            raise ValueError('Decision input, output, lateness and policy must stay fixed within one replay; '
                             'use a new run and new topics to apply a changed decision policy')
    return db


def classify(db, event, event_ms, prior_max, allowed_lateness_ms):
    event_id = event[fields.EVENT_ID]
    digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
    known = db.execute('SELECT payload_hash FROM seen WHERE event_id = ?', (event_id,)).fetchone()
    if known:
        if known[0] != digest:
            return INVALID
        return 'duplicate'
    db.execute('INSERT INTO seen VALUES (?, ?)', (event_id, digest))
    user = (event[fields.USER_ID],)
    order = (event_ms, event[fields.SOURCE_FILE], event[fields.SOURCE_ROW])
    previous = db.execute(
        'SELECT event_ms, source, seq FROM users WHERE user_id = ?', user
    ).fetchone()
    if event_ms < prior_max - allowed_lateness_ms or (previous and order < previous):
        return 'late'
    db.execute('INSERT OR REPLACE INTO users VALUES (?, ?, ?, ?)', (*user, *order))
    return 'accepted'


def rejection_reason(event, event_ms, key, run_id=None):
    if not event or event.get('_corrupt_record') is not None:
        return 'malformed_payload'
    if action_source(event) == 'web':
        if run_id is None or event.get('replay_run_id') not in (None, run_id):
            return 'invalid_replay_run'
        raw_session = event.get('raw_session_id')
        if not raw_session or not raw_session.startswith('web-') or len(raw_session) == 4 or key != raw_session:
            return 'invalid_kafka_key'
        if not WEB_EVENT_ID.fullmatch(event.get(fields.EVENT_ID) or ''):
            return 'invalid_event_id'
        if event.get('sequence') is None or event['sequence'] < 1:
            return 'invalid_source_or_sequence'
        if event_ms is None:
            return 'invalid_event_time'
        if event.get(fields.PRODUCT_ID) is None:
            return 'missing_product_id'
        if event.get(fields.EVENT_TYPE) not in ('view', 'cart', 'purchase'):
            return 'invalid_event_type'
        return None
    if run_id is not None and event.get('replay_run_id') not in (None, run_id):
        return 'invalid_replay_run'
    source_file, source_row = replay_position(event)
    if not source_file or source_row is None or source_row < 0:
        return 'invalid_source_or_sequence'
    if event[fields.EVENT_ID] != f'{source_file}:{source_row}':
        return 'invalid_event_id'
    if event_ms is None:
        return 'invalid_event_time'
    if event[fields.USER_ID] is None or event[fields.PRODUCT_ID] is None:
        return 'missing_user_or_product_id'
    if event[fields.EVENT_TYPE] not in ('view', 'cart', 'purchase'):
        return 'invalid_event_type'
    if key != str(event[fields.USER_ID]):
        return 'invalid_kafka_key'
    return None


def decision_input(spark, args, include_web=False):
    raw = kafka_stream(spark, args, args.topic)
    schema = EVENT_SCHEMA
    if include_web:
        # The team wire format sends IDs as decimal strings. Parse both the old numeric
        # replay format and the new string format without losing bigint precision.
        message_fields = {**EVENT_FIELDS, **ACTION_MESSAGE_FIELDS}
        for name in (fields.USER_ID, fields.PRODUCT_ID, fields.CATEGORY_ID):
            message_fields[name] = StringType()
        schema = StructType([StructField(name, kind) for name, kind in message_fields.items()])
    schema = StructType([*schema.fields, StructField('_corrupt_record', StringType())])
    parsed = raw.withColumn('event', F.from_json('value', schema, {
        'mode': 'PERMISSIVE', 'columnNameOfCorruptRecord': '_corrupt_record'}))
    event_time = F.col(f'event.{fields.EVENT_TIME}')
    if include_web:
        event_time = F.coalesce(F.col('event.event_time_utc'), event_time)
        user_id = F.col(f'event.{fields.USER_ID}')
        event = F.col('event').withField(
            '_corrupt_record',
            F.when(user_id.isNotNull() & user_id.try_cast('bigint').isNull(), F.col('value'))
            .otherwise(F.col('event._corrupt_record')),
        ).withField(fields.EVENT_TIME, event_time)
        for name in (fields.USER_ID, fields.PRODUCT_ID, fields.CATEGORY_ID):
            event = event.withField(name, F.col(f'event.{name}').try_cast('bigint'))
        parsed = parsed.withColumn('event', event)
    return parsed.withColumn('event_ms', F.unix_millis(F.try_to_timestamp(event_time)))


def decision_message(args, batch_id, row, event, status, reason, session_id=None):
    message = {'run_id': args.run_id, 'decision_batch': batch_id, 'status': status,
               'rejection_reason': reason, 'origin_partition': row.partition, 'origin_offset': row.offset,
               'origin_broker_ms': row.broker_ms, 'decision_ms': now_ms(), **event, 'session_id': session_id}
    key = row.key if status != INVALID else f'rejected:{row.partition}:{row.offset}'
    return key, json.dumps(message), row.partition, row.offset


def publish_decisions(spark, args, producer, outgoing):
    if producer is None:
        records = spark.createDataFrame(outgoing, 'key string, value string, partition int, origin_offset long')
        (records.coalesce(1).sortWithinPartitions('partition', 'origin_offset')
         .select('key', 'value', 'partition').write.format('kafka')
         .option('kafka.bootstrap.servers', args.kafka).option('topic', args.refined_topic)
         .option('kafka.acks', 'all').save())
    else:
        from confluent_kafka import KafkaException

        errors = []

        def on_delivery(error, _message):
            if error is not None:
                errors.append(error)

        for key, value, partition, _ in outgoing:
            producer.produce(args.refined_topic, partition=partition, key=key.encode(), value=value.encode(),
                             on_delivery=on_delivery)
        if producer.flush(30):
            raise TimeoutError('Kafka did not acknowledge all decisions')
        if errors:
            raise KafkaException(errors[0])


def decision_producer(args):
    if args.decision_publisher == 'driver':
        from confluent_kafka import Producer

        return Producer({'bootstrap.servers': args.kafka, 'acks': 'all', 'enable.idempotence': True,
                         'linger.ms': 5, 'delivery.timeout.ms': 30000})
    return None


def spark_decision_worker(spark, args, root):
    from spark_decision import decisions

    checkpoint = root / 'decision-spark-checkpoint'
    contract_file = root / 'decision-spark-contract.json'
    if (root / 'decision.sqlite').exists() or (root / 'decision-checkpoint').exists():
        raise ValueError('Switching decision backends requires a new run ID and fresh topics/checkpoints')
    contract = [args.topic, args.refined_topic, args.allowed_lateness_seconds, 'keyed-prior-batch-clock-v4-session-contract']
    try:
        with contract_file.open('x', encoding='utf-8') as stream:
            json.dump(contract, stream)
    except FileExistsError:
        if json.loads(contract_file.read_text(encoding='utf-8')) != contract:
            raise ValueError('Changing decision topics or rules requires a new run ID and checkpoint')

    spark.conf.set('spark.sql.streaming.stateStore.providerClass',
                   'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    if args.decision_state_partitions is not None:
        spark.conf.set('spark.sql.shuffle.partitions', str(args.decision_state_partitions))
    if args.decision_changelog:
        spark.conf.set('spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled', 'true')
    # This query reads the watermark as a clock and does not register Spark timers.
    spark.conf.set('spark.sql.streaming.noDataMicroBatches.enabled', 'false')
    classified = decisions(decision_input(spark, args, include_web=True), args.allowed_lateness_seconds * 1000, args.run_id)
    producer = decision_producer(args)

    def process(batch, batch_id):
        collect_started = now_ms()
        rows = sorted(batch.collect(), key=lambda row: (row.partition, row.offset))
        collected = now_ms()
        if not rows:
            return
        outgoing = [decision_message(args, batch_id, row, json.loads(row.event_json), row.status, row.rejection_reason,
                                     row.session_id)
                    for row in rows]
        publish_started = now_ms()
        publish_decisions(spark, args, producer, outgoing)
        if args.benchmark_progress:
            print('DATA06_KEYED_TIMING ' + json.dumps({
                'batch': batch_id, 'collect_started_ms': collect_started, 'collected_ms': collected,
                'publish_started_ms': publish_started, 'acked_ms': now_ms(),
                'rows': [[row.broker_ms, row.classified_ms, row.classify_us] for row in rows],
            }), flush=True)
        if args.fail == 'decision-after-publish' and batch_id >= args.fail_decision_after_batch:
            raise RuntimeError('INJECTED_DECISION_AFTER_PUBLISH')

    try:
        run_query(classified, checkpoint, process, args)
    finally:
        if producer is not None:
            producer.flush(5)


def decision_worker(spark, args, root):
    if args.decision_state_store == 'spark':
        spark_decision_worker(spark, args, root)
        return
    if (root / 'decision-spark-contract.json').exists():
        raise ValueError('Switching decision backends requires a new run ID and fresh topics/checkpoints')
    parsed = decision_input(spark, args)
    contract = json.dumps([args.topic, args.refined_topic, args.allowed_lateness_seconds, DECISION_POLICY])
    state_path = root / 'decision.sqlite'
    checkpoint = root / 'decision-checkpoint'
    if (checkpoint / 'commits').exists() and not state_path.exists():
        raise RuntimeError('Decision checkpoint exists without its local state database')

    producer = decision_producer(args)

    def process(batch, batch_id):
        entered = now_ms()
        with closing(decision_state(state_path, contract)) as db, db:
            latest = db.execute('SELECT MAX(batch) FROM completed').fetchone()[0]
            if latest is not None and latest >= batch_id:
                return
            partition_times = dict(db.execute('SELECT partition, last_event_ms FROM partition_clock'))
            outgoing = []
            updated_partitions = set()
            # The Kafka trigger bounds the batch. Sort locally to avoid a Spark shuffle.
            read_started = now_ms()
            rows = sorted(batch.collect(), key=lambda row: (row.partition, row.offset))
            read_ended = now_ms()
            for row in rows:
                event = row.event.asDict() if row.event else {}
                reason = rejection_reason(event, row.event_ms, row.key)
                event.pop('_corrupt_record', None)
                status = INVALID
                if reason is None:
                    # Compare only with records already seen in this partition's stable offset order.
                    prior_max = partition_times.get(row.partition, -(2**63))
                    status = classify(db, event, row.event_ms, prior_max, args.allowed_lateness_seconds * 1000)
                    if status == INVALID:
                        reason = 'conflicting_event_payload'
                    else:
                        partition_times[row.partition] = max(prior_max, row.event_ms)
                        updated_partitions.add(row.partition)
                outgoing.append(decision_message(args, batch_id, row, event, status, reason))
            if not outgoing:
                return
            publish_started = now_ms()
            publish_decisions(spark, args, producer, outgoing)
            if args.benchmark_progress:
                print('DATA06_DECISION_PROFILE ' + json.dumps({
                    'batch': batch_id, 'entered_ms': entered, 'read_start_ms': read_started,
                    'read_end_ms': read_ended, 'publish_start_ms': publish_started, 'acked_ms': now_ms(),
                    'positions': [[row.partition, row.offset] for row in rows],
                }), flush=True)
            if args.fail == 'decision-after-publish' and batch_id >= args.fail_decision_after_batch:
                raise RuntimeError('INJECTED_DECISION_AFTER_PUBLISH')
            # On failed publication SQLite rolls back. A retry may publish copies.
            db.executemany('INSERT OR REPLACE INTO partition_clock VALUES (?, ?)',
                           ((partition, partition_times[partition]) for partition in updated_partitions))
            db.execute('INSERT INTO completed VALUES (?)', (batch_id,))

    try:
        run_query(parsed, checkpoint, process, args)
    finally:
        if producer is not None:
            producer.flush(5)


def refined_rows(frame):
    schema = (', '.join(f'{name} {kind.simpleString()}' for name, kind in {**EVENT_FIELDS, **ACTION_MESSAGE_FIELDS}.items())
              + ', run_id string, decision_batch long, status string, rejection_reason string, session_id string'
              + ', origin_partition int, origin_offset long'
              + ', origin_broker_ms long, decision_ms long')
    return (frame.select(F.col('partition').alias('refined_partition'), F.col('offset').alias('refined_offset'),
                         F.col('broker_ms').alias('refined_broker_ms'),
                         F.from_json('value', schema, {'mode': 'FAILFAST'}).alias('record'))
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
if #KEYS > 1 then
    local kind = redis.call('TYPE', KEYS[2]).ok
    local list_kind = redis.call('TYPE', KEYS[3]).ok
    if (kind ~= 'none' and kind ~= 'hash') or (list_kind ~= 'none' and list_kind ~= 'list') then
        return redis.error_reply('Session key has the wrong type')
    end
    local owner = redis.call('HGET', KEYS[2], 'run_id')
    if owner and owner ~= ARGV[5] then
        return redis.error_reply('Use a separate Redis database for each replay')
    end
    local ttl = tonumber(ARGV[6])
    local actions = cjson.decode(ARGV[7])
    local previous_owner = #KEYS > 3 and redis.call('HGET', KEYS[4], 'run_id') or false
    if not ttl or ttl < 1 or #actions > 5 or (previous_owner and previous_owner ~= ARGV[5]) then
        return redis.error_reply('Invalid session write')
    end
    local clock = redis.call('TIME')
    local expires = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000) + ttl * 1000
    redis.call('HSET', KEYS[2], 'run_id', ARGV[5], 'features', ARGV[8], 'feature_revision', ARGV[9],
               'origin_partition', ARGV[1], 'origin_offset', ARGV[2], 'purchased', ARGV[10])
    redis.call('DEL', KEYS[3])
    for _, action in ipairs(actions) do redis.call('RPUSH', KEYS[3], action) end
    redis.call('PEXPIREAT', KEYS[2], expires)
    if #actions > 0 then redis.call('PEXPIREAT', KEYS[3], expires) end
    if #KEYS > 3 and previous_owner then redis.call('DEL', KEYS[4], KEYS[5]) end
end
redis.call('SET', KEYS[1], ARGV[4])
return 1
"""


def next_user_state(previous, event):
    features = previous.get('features', {})
    now = parse_time(getattr(event, fields.EVENT_TIME))
    event_id = getattr(event, fields.EVENT_ID)
    source_file = getattr(event, fields.SOURCE_FILE)
    source_row = getattr(event, fields.SOURCE_ROW)
    event_type = getattr(event, fields.EVENT_TYPE)
    product_id = getattr(event, fields.PRODUCT_ID)
    last = parse_time(previous['event_time']) if previous else None
    if last and now < last:
        raise ValueError('Accepted events must be in time order for this feature calculation')
    new_session = last is None or now - last > timedelta(minutes=30)
    session_start = event_id if new_session else features['session_start_event_id']
    decided_session = getattr(event, 'session_id', None)
    if decided_session is not None:
        session_start = decided_session
        new_session = session_start != features.get('session_start_event_id')
    if previous and 'history' not in previous:
        raise ValueError('Redis state uses the old schema; rebuild from Delta before resuming')
    history = previous.get('history', [])
    if history and (now, source_file, source_row) <= (
            last, history[-1]['source'], history[-1]['event_seq']):
        raise ValueError('Accepted event overlaps or precedes the Redis seed')
    history_cutoff = now - timedelta(days=7)
    history = [row for row in history if row['session_start_event_id'] == session_start or
               parse_time(row['event_time']) >= history_cutoff]
    at = utc_string(now)
    history.append({'event_id': event_id, 'source': source_file, 'event_seq': source_row,
                    'event_time': at, 'event_type': event_type, 'product_id': product_id,
                    'session_start_event_id': session_start})
    views = set() if new_session else set(features['viewed_product_ids'])
    carts = set() if new_session else set(features['carted_product_ids'])
    cutoff = (now - timedelta(days=30)).timestamp()
    purchases = {item: at for item, at in previous.get('purchases', {}).items() if at > cutoff}
    if event_type == 'view':
        views.add(product_id)
    elif event_type == 'cart':
        carts.add(product_id)
    else:
        purchases[str(product_id)] = now.timestamp()
    return {'origin_partition': event.origin_partition,
            'origin_offset': None if event.origin_offset is None else str(event.origin_offset),
            'refined_offset': None if event.refined_offset is None else str(event.refined_offset),
            'event_time': at, 'purchases': purchases, 'history': history,
            'features': {'event_count': features.get('event_count', 0) + 1, 'last_event_id': event_id,
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


def apply_recent_actions(client, apply, run_id, events, publish_latency, requests):
    applied = skipped = 0
    for event in events:
        key = user_key(run_id, getattr(event, fields.USER_ID))
        raw = client.get(key)
        previous = redis_input_state(raw, event)
        if previous is None:
            skipped += 1
            continue
        session_id = getattr(event, 'session_id', None)
        if not session_id:
            raise ValueError('Recent actions require Spark-assigned session IDs')
        old_session = previous.get('features', {}).get('session_start_event_id')
        current_session = old_session == session_id
        session_key, list_key = session_keys(session_id)
        if client.hget(session_key, 'run_id') not in (None, run_id):
            raise ValueError('Use a separate Redis database for each replay')
        actions = [json.loads(item) for item in client.lrange(list_key, 0, -1)] if current_session else []
        if current_session and (actions[-1]['seq'] if actions else 0) != previous.get('recent_action_seq'):
            raise ValueError('Recent actions need to be restored before resuming this session')
        lookup = now_ms()
        state = next_user_state(previous, event)
        actions = apply_list_event(actions, event)
        event_type = getattr(event, fields.EVENT_TYPE)
        purchased = current_session and previous.get('session_purchased', False)
        state['session_purchased'] = purchased or event_type == 'purchase'
        state['recent_action_seq'] = actions[-1]['seq'] if actions else 0
        revision = f'{run_id}:{event.origin_partition}:{event.origin_offset}'
        keys = [key, session_key, list_key]
        if old_session and not current_session:
            keys.extend(session_keys(old_session))
        computed = now_ms()
        if event_type in ACTION_TYPES and not purchased:
            requests.publish(run_id, event, session_id, actions)
        if apply(keys=keys, args=[event.origin_partition, event.origin_offset, raw or '', json.dumps(state),
                                 run_id, requests.ttl, json.dumps([json.dumps(item) for item in actions]),
                                 json.dumps(state['features']), revision, int(state['session_purchased'])]):
            applied += 1
            publish_latency(event, lookup, computed, now_ms())
        else:
            skipped += 1
    return applied, skipped


def apply_redis_pipeline(client, apply, run_id, events, publish_latency, requests=None):
    if requests is not None:
        return apply_recent_actions(client, apply, run_id, events, publish_latency, requests)
    if not events:
        return 0, 0
    keys = list(dict.fromkeys(user_key(run_id, getattr(event, fields.USER_ID)) for event in events))
    current = dict(zip(keys, client.mget(keys)))
    lookup_ended = now_ms()
    pending = []
    skipped = 0
    with client.pipeline(transaction=False) as pipe:
        for event in events:
            key = user_key(run_id, getattr(event, fields.USER_ID))
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
    requests = ConversionRequests(args) if args.prediction_topic else None
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

            if requests is not None or (args.redis_batch_writes and not args.fail):
                if args.fail == 'redis-after-first':
                    eligible = eligible[:1]
                applied, skipped = apply_redis_pipeline(client, apply, args.run_id, eligible, publish_latency,
                                                       requests=requests)
                if args.fail == 'redis-after-first' and applied:
                    raise RuntimeError('INJECTED_REDIS_AFTER_FIRST')
            else:
                for event in eligible:
                    key = user_key(args.run_id, event[fields.USER_ID])
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
                        acked = now_ms()
                        publish_latency(event, lookup_ended, computed, acked)
                        if args.trace_revisions:
                            print('DATA06_REDIS_REVISION ' + json.dumps([
                                event.refined_partition, event.refined_offset, event[fields.USER_ID], acked,
                                json.loads(payload)]), flush=True)
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
            origin = ('origin_partition', 'origin_offset')
            delta_merge(first_copies, root / 'refined', origin)
            rejected = first_copies.where(F.col('status') == INVALID).select(
                'run_id', *origin, 'origin_broker_ms', fields.EVENT_ID, 'rejection_reason')
            delta_merge(rejected, root / 'rejected', origin)
            if args.fail == 'delta-after-write' and not batch.isEmpty():
                raise RuntimeError('INJECTED_DELTA_AFTER_WRITE')
        finally:
            batch.unpersist()

    run_query(frame, root / 'delta-checkpoint', process, args)


def inspect_worker(spark, args, root):
    for name in ('raw', 'refined', 'rejected'):
        path = root / name
        if not (path / '_delta_log').is_dir():
            continue
        if name == 'raw':
            columns = ('partition', 'offset', 'value')
        elif name == 'refined':
            columns = ('origin_partition', 'origin_offset', 'refined_partition', 'refined_offset', 'status', fields.EVENT_ID)
        else:
            columns = ('origin_partition', 'origin_offset', fields.EVENT_ID, 'rejection_reason')
        data = spark.read.format('delta').load(str(path)).select(*columns).orderBy(*columns[:2])
        for row in data.toLocalIterator():
            print('DATA06_INSPECT ' + json.dumps([name, row.asDict()]), flush=True)
    if args.trace_revisions:
        states = {}
        events = (spark.read.format('delta').load(str(root / 'refined'))
                  .where("status = 'accepted'").orderBy('refined_partition', 'refined_offset'))
        for row in events.toLocalIterator():
            user = row[fields.USER_ID]
            state = next_user_state(states.get(user, {}), row)
            states[user] = state
            print('DATA06_DELTA_REVISION ' + json.dumps([
                row.refined_partition, row.refined_offset, user, state]), flush=True)


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
    parser.add_argument('--prediction-topic', help='Enable the 19-2 Redis List and conversion request publishing')
    parser.add_argument('--prediction-input-calculator', help='Shared ML function as module:function')
    parser.add_argument('--prediction-input-version')
    parser.add_argument('--session-safety-ttl-seconds', type=int)
    parser.add_argument('--replay-prediction-permille', type=int, default=100)
    parser.add_argument('--allowed-lateness-seconds', type=int, default=60)
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--decision-state-store', choices=('spark', 'sqlite'),
                        default=os.environ.get('DATA06_DECISION_STATE_STORE', 'spark'))
    parser.add_argument('--continuous', action='store_true')
    parser.add_argument('--trigger', default=None)
    parser.add_argument('--decision-publisher', choices=('spark', 'driver'), default='spark')
    parser.add_argument('--redis-batch-writes', action='store_true')
    parser.add_argument('--redis-direct', action='store_true', help='Use the local Kafka polling Redis sink')
    parser.add_argument('--redis-direct-offsets', choices=('sqlite', 'kafka'), default='sqlite',
                        help='Use broker-committed offsets for the direct Redis consumer')
    parser.add_argument('--redis-session-timeout-ms', type=int, default=10000,
                        help='Kafka group failure detection timeout for the direct Redis consumer')
    parser.add_argument('--redis-heartbeat-interval-ms', type=int, default=2000,
                        help='Kafka group heartbeat interval for the direct Redis consumer')
    parser.add_argument('--decision-state-partitions', type=int,
                        help='Set shuffle partitions for a fresh Spark decision checkpoint')
    parser.add_argument('--profile-decision-state', action='store_true')
    parser.add_argument('--decision-changelog', action='store_true')
    parser.add_argument('--fail-decision-after-batch', type=int, default=0)
    parser.add_argument('--benchmark-progress', action='store_true', help='Log committed Kafka positions')
    parser.add_argument('--trace-revisions', action='store_true', help='Trace test states in Redis and from Delta')
    parser.add_argument('--fail', choices=('decision-after-publish', 'redis-after-first',
                                           'delta-before-write', 'delta-after-write'))
    args = parser.parse_args()
    if any((args.prediction_topic, args.prediction_input_calculator, args.prediction_input_version,
            args.session_safety_ttl_seconds is not None)) and (
            not all((args.prediction_topic, args.prediction_input_calculator, args.prediction_input_version,
                     args.session_safety_ttl_seconds)) or args.session_safety_ttl_seconds < 1
            or args.stage not in ('redis', 'bundled') or args.prediction_topic in (args.topic, args.refined_topic)
            or args.trace_revisions):
        parser.error('19-2 requires a separate prediction topic, calculator, version and positive safety TTL on Redis')
    if not 0 <= args.replay_prediction_permille <= 1000:
        parser.error('Replay prediction sampling must be between 0 and 1000 permille')
    if args.decision_state_store not in ('spark', 'sqlite'):
        parser.error('DATA06_DECISION_STATE_STORE must be spark or sqlite')
    if not re.fullmatch(r'[a-z0-9-]+', args.run_id) or args.topic == args.refined_topic:
        parser.error('Use a lowercase run ID and distinct action and refined topics')
    if args.batch_size < 1 or args.allowed_lateness_seconds < 0:
        parser.error('Batch size must be positive and lateness cannot be negative')
    if args.decision_state_partitions is not None and (
            args.decision_state_partitions < 1 or args.stage != 'decision' or args.decision_state_store != 'spark'):
        parser.error('Decision state partitions require the Spark decision stage and a positive count')
    if (args.profile_decision_state or args.decision_changelog) and (
            args.stage != 'decision' or args.decision_state_store != 'spark'):
        parser.error('Decision profiling and changelog require the Spark decision stage')
    if args.profile_decision_state and not args.benchmark_progress:
        parser.error('Decision profiling requires benchmark progress')
    failure_stages = {'decision-after-publish': 'decision', 'redis-after-first': 'redis',
                      'delta-before-write': 'delta', 'delta-after-write': 'delta'}
    if args.fail and failure_stages[args.fail] != args.stage:
        parser.error('Injected failure must match the selected stage')
    if args.stage == 'bundled' and not args.continuous:
        parser.error('Bundled workers require --continuous')
    if args.redis_recovery and args.stage != 'redis':
        parser.error('Redis recovery offsets apply only to the Redis worker')
    if args.redis_direct and (args.stage != 'redis' or not args.continuous):
        parser.error('Direct Redis mode requires the continuous Redis worker')
    if args.redis_direct and args.redis_direct_offsets == 'kafka' and not (
            0 < args.redis_heartbeat_interval_ms < args.redis_session_timeout_ms):
        parser.error('Redis consumer heartbeat must be positive and shorter than its session timeout')
    if args.trace_revisions and (args.stage not in ('redis', 'inspect') or args.redis_direct or args.redis_batch_writes):
        parser.error('Revision tracing requires the Spark Redis worker or the Delta inspector')
    args.trigger = args.trigger or ('30 seconds' if args.stage in ('delta', 'bundled') else '1 second')
    root = Path('/state') / 'split' / args.run_id
    root.mkdir(parents=True, exist_ok=True)
    if args.redis_direct:
        from redis_kafka import consume

        consume(args, root, redis_recovery_plan(args))
        return
    event_log_dir = root / 'decision-events' if args.profile_decision_state else None
    spark = spark_session(f'data-06-{args.stage}', event_log_dir=event_log_dir)
    workers = {'decision': decision_worker, 'redis': redis_worker, 'delta': delta_worker,
               'inspect': inspect_worker, 'bundled': bundled_worker}
    try:
        workers[args.stage](spark, args, root)
    finally:
        spark.stop()
