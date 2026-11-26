"""Local Redis sink that polls refined Kafka without scheduling Spark microbatches."""
import fcntl
import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import redis
from confluent_kafka import Consumer, KafkaException, TopicPartition

from refined import INVALID, REDIS_APPLY, apply_redis_pipeline, now_ms


def consume(args, root, recovery):
    suffix = f"-{recovery['generation']}" if recovery else ''
    checkpoint = root / f'redis-direct{suffix}.sqlite'
    with checkpoint.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        config = {'bootstrap.servers': args.kafka, 'group.id': f'{args.run_id}-redis-direct',
                  'enable.auto.commit': False, 'enable.auto.offset.store': False,
                  'auto.offset.reset': 'error', 'allow.auto.create.topics': False}
        with closing(sqlite3.connect(checkpoint)) as db, closing(Consumer(config)) as source, redis.Redis.from_url(
                    args.redis_url, decode_responses=True, socket_timeout=5) as client:
            poll(args, recovery, db, source, client)


def poll(args, recovery, db, source, client):
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

    apply = client.register_script(REDIS_APPLY)
    print('DATA06_REDIS_READY ' + json.dumps({'positions': positions}), flush=True)
    batch = 0
    while True:
        records = source.consume(num_messages=min(args.batch_size, 1_000_000), timeout=0.05)
        if not records:
            continue
        received = now_ms()
        events = []
        for record in records:
            if record.error() is not None:
                raise KafkaException(record.error())
            partition, offset = record.partition(), record.offset()
            payload = json.loads(record.value())
            if payload['run_id'] != args.run_id or payload['origin_partition'] != partition:
                raise ValueError('Refined record belongs to a different run or partition')
            if payload['status'] not in ('accepted', 'duplicate', 'late', INVALID):
                raise ValueError('Invalid refined decision')
            if payload['status'] == 'accepted' and (not recovery or offset >= recovery['boundary'][str(partition)]):
                events.append(SimpleNamespace(**payload, refined_offset=offset, refined_broker_ms=record.timestamp()[1]))
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
        applied, skipped = apply_redis_pipeline(client, apply, args.run_id, events, emit)
        with db:
            db.execute('INSERT OR REPLACE INTO checkpoint VALUES (1, ?, ?)',
                       (args.refined_topic, json.dumps(positions)))
        committed = now_ms()
        if args.benchmark_progress:
            print('DATA07_SPLIT ' + json.dumps({'stage': 'redis', 'batch': batch, 'committed_ms': committed,
                  'trigger_ms': committed - received, 'end_offsets': {args.refined_topic: positions}}), flush=True)
            if profile:
                print('DATA06_REDIS_PROFILE ' + json.dumps({'batch': batch, 'points': profile}), flush=True)
                print('DATA06_REDIS_APPLIED ' + json.dumps(applied_positions), flush=True)
        print('DATA06_REDIS ' + json.dumps({'batch': batch, 'applied': applied, 'skipped': skipped}), flush=True)
        batch += 1
