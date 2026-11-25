"""Local Redis sink that polls refined Kafka without scheduling Spark microbatches."""
import fcntl
import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import redis
from kafka import KafkaConsumer, TopicPartition

from refined import INVALID, REDIS_APPLY, apply_redis_pipeline, now_ms


def consume(args, root, recovery):
    suffix = f"-{recovery['generation']}" if recovery else ''
    checkpoint = root / f'redis-direct{suffix}.sqlite'
    with checkpoint.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with closing(sqlite3.connect(checkpoint)) as db, closing(KafkaConsumer(
                bootstrap_servers=args.kafka, enable_auto_commit=False, auto_offset_reset='none',
                max_poll_records=args.batch_size)) as source, redis.Redis.from_url(
                    args.redis_url, decode_responses=True, socket_timeout=5) as client:
            poll(args, recovery, db, source, client)


def poll(args, recovery, db, source, client):
    source.topics()
    partitions = source.partitions_for_topic(args.refined_topic)
    if not partitions:
        raise ValueError('Refined topic has no partitions')
    assigned = [TopicPartition(args.refined_topic, partition) for partition in sorted(partitions)]
    source.assign(assigned)
    beginning = source.beginning_offsets(assigned)
    end = source.end_offsets(assigned)
    db.execute('CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY, topic TEXT, positions TEXT)')
    saved = db.execute('SELECT topic, positions FROM checkpoint WHERE id = 1').fetchone()
    if saved and saved[0] != args.refined_topic:
        raise ValueError('Direct Redis checkpoint belongs to a different topic')
    positions = json.loads(saved[1]) if saved else (recovery['restart_offsets'] if recovery else
                {str(partition.partition): offset for partition, offset in beginning.items()})
    if set(positions) != {str(partition) for partition in partitions}:
        raise ValueError('Kafka partitions changed during this replay')
    for partition in assigned:
        offset = positions[str(partition.partition)]
        if not beginning[partition] <= offset <= end[partition]:
            raise ValueError('Direct Redis checkpoint falls outside retained Kafka offsets')
        source.seek(partition, offset)

    apply = client.register_script(REDIS_APPLY)
    print('DATA06_REDIS_READY ' + json.dumps({'positions': positions}), flush=True)
    batch = 0
    while True:
        records = source.poll(timeout_ms=50)
        if not records:
            continue
        received = now_ms()
        events = []
        for partition in assigned:
            for record in records.get(partition, []):
                payload = json.loads(record.value)
                if payload['run_id'] != args.run_id or payload['origin_partition'] != partition.partition:
                    raise ValueError('Refined record belongs to a different run or partition')
                if payload['status'] not in ('accepted', 'duplicate', 'late', INVALID):
                    raise ValueError('Invalid refined decision')
                if payload['status'] == 'accepted' and (not recovery or
                        record.offset >= recovery['boundary'][str(partition.partition)]):
                    events.append(SimpleNamespace(**payload, refined_offset=record.offset,
                                                   refined_broker_ms=record.timestamp))
                positions[str(partition.partition)] = record.offset + 1
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
