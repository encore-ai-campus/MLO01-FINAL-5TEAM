"""Local Redis sink that polls refined Kafka without scheduling Spark microbatches."""
import fcntl
import json
import os
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import redis
from confluent_kafka import Consumer, KafkaException, OFFSET_INVALID, TopicPartition

from refined import INVALID, REDIS_APPLY, apply_redis_pipeline, now_ms, record_session_positions
from recent_actions import ConversionRequests


def group_id(args, recovery):
    suffix = f"-{recovery['generation']}" if recovery else ''
    return f'{args.run_id}-redis-group{suffix}'


def consume_group(args, recovery, config, client):
    group = group_id(args, recovery)
    settings = {
        'group.protocol': 'classic',
        'session.timeout.ms': args.redis_session_timeout_ms,
        'heartbeat.interval.ms': args.redis_heartbeat_interval_ms,
    }

    def group_event(event, partitions):
        print('DATA06_REDIS_GROUP ' + json.dumps({
            'event': event, 'group': group, 'at_ms': now_ms(),
            'partitions': sorted(partition.partition for partition in partitions),
        }), flush=True)

    # Always validate the stored position. A truncated Kafka range must fail closed.
    with closing(Consumer({**config, **settings, 'group.id': group})) as source:
        def on_assign(consumer, partitions):
            positions = {}
            committed = consumer.committed(partitions, timeout=10)
            for partition in committed:
                number = str(partition.partition)
                beginning, end = consumer.get_watermark_offsets(partition, timeout=10)
                if partition.error is not None:
                    raise KafkaException(partition.error)
                offset = partition.offset
                if offset == OFFSET_INVALID:
                    offset = recovery['restart_offsets'][number] if recovery else 0
                if not beginning <= offset <= end:
                    raise ValueError(f'Redis consumer checkpoint outside Kafka retention: {partition}')
                partition.offset = offset
                positions[number] = offset
            consumer.assign(committed)
            print('DATA06_REDIS_READY ' + json.dumps({'positions': positions, 'group': group, 'settings': settings}),
                  flush=True)
            group_event('assigned', committed)

        def commit(positions):
            offsets = [TopicPartition(args.refined_topic, int(partition), offset)
                       for partition, offset in positions.items()]
            for result in source.commit(offsets=offsets, asynchronous=False):
                if result.error is not None:
                    raise KafkaException(result.error)

        source.subscribe(
            [args.refined_topic], on_assign=on_assign,
            on_revoke=lambda _, partitions: group_event('revoked', partitions),
            on_lost=lambda _, partitions: group_event('lost', partitions),
        )
        poll_records(args, recovery, source, client, commit)


def consume(args, root, recovery):
    config = {'bootstrap.servers': args.kafka, 'group.id': f'{args.run_id}-redis-direct',
              'enable.auto.commit': False, 'enable.auto.offset.store': False,
              'auto.offset.reset': 'error', 'allow.auto.create.topics': False}
    with redis.Redis.from_url(args.redis_url, decode_responses=True, socket_timeout=5) as client:
        if args.redis_direct_offsets == 'kafka':
            consume_group(args, recovery, config, client)
            return

        suffix = f"-{recovery['generation']}" if recovery else ''
        checkpoint = root / f'redis-direct{suffix}.sqlite'
        with checkpoint.with_suffix('.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with closing(sqlite3.connect(checkpoint)) as db, closing(Consumer(config)) as source:
                poll_local(args, recovery, db, source, client)


def poll_local(args, recovery, db, source, client):
    topic = source.list_topics(args.refined_topic, timeout=15).topics.get(args.refined_topic)
    if topic is None or topic.error is not None or not topic.partitions:
        raise ValueError('Refined topic has no partitions')
    ranges = {str(partition): source.get_watermark_offsets(TopicPartition(args.refined_topic, partition), timeout=15)
              for partition in sorted(topic.partitions)}
    db.execute('CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY, topic TEXT, positions TEXT)')
    saved = db.execute('SELECT topic, positions FROM checkpoint WHERE id = 1').fetchone()
    if saved and saved[0] != args.refined_topic:
        raise ValueError('Direct Redis checkpoint belongs to a different topic')
    positions = json.loads(saved[1]) if saved else (recovery['restart_offsets'] if recovery else
                {partition: bounds[0] for partition, bounds in ranges.items()})
    if positions.keys() != ranges.keys():
        raise ValueError('Kafka partitions changed during this replay')
    for partition, (beginning, end) in ranges.items():
        if not beginning <= positions[partition] <= end:
            raise ValueError('Direct Redis checkpoint falls outside retained Kafka offsets')
    source.assign([TopicPartition(args.refined_topic, int(partition), positions[partition]) for partition in ranges])

    print('DATA06_REDIS_READY ' + json.dumps({'positions': positions}), flush=True)

    def commit(updated):
        positions.update(updated)
        with db:
            db.execute('INSERT OR REPLACE INTO checkpoint VALUES (1, ?, ?)',
                       (args.refined_topic, json.dumps(positions)))

    poll_records(args, recovery, source, client, commit, dict(positions))


def poll_records(args, recovery, source, client, commit, progress=None):
    apply = client.register_script(REDIS_APPLY)
    requests = ConversionRequests(args) if args.prediction_topic else None
    progress = {} if progress is None else progress
    batch = 0
    limit = 1 if args.fail == 'redis-after-first' else min(args.batch_size, 1_000_000)
    while True:
        records = source.consume(num_messages=limit, timeout=0.05)
        if not records:
            continue
        received = now_ms()
        events = []
        session_rows = []
        positions = {}
        for record in records:
            if record.error() is not None:
                raise KafkaException(record.error())
            partition, offset = record.partition(), record.offset()
            payload = json.loads(record.value())
            if payload['run_id'] != args.run_id or payload['origin_partition'] != partition:
                raise ValueError('Refined record belongs to a different run or partition')
            if payload['status'] not in ('accepted', 'duplicate', 'late', INVALID):
                raise ValueError('Invalid refined decision')
            if not recovery or offset >= recovery['boundary'][str(partition)]:
                event = SimpleNamespace(**payload, refined_offset=offset, refined_broker_ms=record.timestamp()[1])
                if event.status in ('accepted', 'late', 'duplicate'):
                    session_rows.append(event)
                if event.status == 'accepted':
                    events.append(event)
            positions[str(partition)] = offset + 1
        decoded = now_ms()
        profile = []

        def emit(event, lookup, computed, acked):
            print('DATA06_LATENCY ' + json.dumps([
                event.origin_broker_ms, event.decision_ms, event.refined_broker_ms, acked]), flush=True)
            if args.benchmark_progress:
                profile.append([event.refined_broker_ms, received, decoded, lookup, computed, acked])
            applied_positions.append([event.origin_partition, event.origin_offset, event.origin_broker_ms, acked])

        applied_positions = []
        applied, skipped = apply_redis_pipeline(client, apply, args.run_id, events, emit, requests=requests)
        if args.fail == 'redis-after-first' and applied:
            event = events[0]
            print('INJECTED_REDIS_AFTER_WRITE ' + json.dumps({
                'partition': event.origin_partition, 'offset': event.refined_offset,
                'applied': applied, 'positions': positions,
            }), flush=True)
            os._exit(91)
        record_session_positions(client, args.run_id, session_rows, args.session_safety_ttl_seconds or 300)
        commit(positions)
        progress.update(positions)
        committed = now_ms()
        if args.benchmark_progress:
            print('DATA07_SPLIT ' + json.dumps({'stage': 'redis', 'batch': batch, 'committed_ms': committed,
                  'trigger_ms': committed - received, 'end_offsets': {args.refined_topic: progress}}), flush=True)
            if profile:
                print('DATA06_REDIS_PROFILE ' + json.dumps({'batch': batch, 'points': profile}), flush=True)
                print('DATA06_REDIS_APPLIED ' + json.dumps(applied_positions), flush=True)
        print('DATA06_REDIS ' + json.dumps({'batch': batch, 'applied': applied, 'skipped': skipped}), flush=True)
        batch += 1
