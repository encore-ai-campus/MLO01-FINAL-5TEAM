"""Exercise the split workers with real November events and isolated local services."""
import argparse
import json
import math
import os
import subprocess
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import islice
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

import redis
from verify import ROOT, redis_expected
from reco.replay_parquet import events_in_order


def markers(output, prefix):
    decoder = json.JSONDecoder()
    records = []
    for line in output.splitlines():
        position = line.find(prefix)
        if position >= 0:
            records.append(decoder.raw_decode(line, position + len(prefix))[0])
    return records


def on_wire(event):
    return {**event, 'event_time': event['event_time'].isoformat()}


def compare_delta(output, raw, first_copies):
    rows = defaultdict(list)
    for table, row in markers(output, 'DATA06_INSPECT '):
        rows[table].append(row)
    actual_raw = {(row['partition'], row['offset']): json.loads(row['value']) for row in rows['raw']}
    assert len(rows['raw']) == len(actual_raw) and actual_raw == raw
    actual_refined = {(row['origin_partition'], row['origin_offset']): row for row in rows['refined']}
    assert len(rows['refined']) == len(actual_refined) and actual_refined == first_copies
    return Counter(row['status'] for row in rows['refined'])


def percentiles(points, left, right):
    values = sorted(row[right] - row[left] for row in points)
    return {f'p{p}_ms': values[math.ceil(p * len(values) / 100) - 1] for p in (95, 99)}


def compare_redis(client, run_id, events, copies):
    users = redis_expected(events)
    by_id = {row['event_id']: row for row in copies.values() if row['status'] == 'accepted'}
    expected_positions = {}
    for user, features in users.items():
        row = by_id[features['last_event_id']]
        expected_positions[user] = (row['origin_partition'], str(row['origin_offset']), str(row['refined_offset']))
    prefix = f'data06:{run_id}:user:'
    actual = {int(key.removeprefix(prefix)): json.loads(client.get(key))
              for key in client.scan_iter(match=prefix + '*')}
    assert {user: row['features'] for user, row in actual.items()} == users
    assert {user: (row['origin_partition'], row['origin_offset'], row['refined_offset'])
            for user, row in actual.items()} == expected_positions
    return len(users)


def verify(source, output, *, decision_publisher='spark'):
    started = time.monotonic()
    run_id = 'split-' + uuid.uuid4().hex[:12]
    result = output / run_id
    result.mkdir(parents=True)
    env = {**os.environ, 'DATA06_KAFKA_PORT': '39192', 'DATA06_REDIS_PORT': '36479'}
    compose = ['docker', 'compose', '-p', 'mlo-data06-split', '-f', str(Path(__file__).with_name('compose.yaml'))]
    bootstrap = '127.0.0.1:39192'
    topic, refined = f'{run_id}-actions', f'{run_id}-refined'
    partitions = 2

    def command(label, invocation, failure=None):
        print(f'Running {label}', flush=True)
        log_path = result / f'{label}.log'
        with log_path.open('w', encoding='utf-8') as log:
            task = subprocess.run(compose + invocation, env=env, cwd=ROOT, stdout=log,
                                  stderr=subprocess.STDOUT, timeout=330)
        log = log_path.read_text(encoding='utf-8', errors='replace')
        if not (task.returncode == 0 if failure is None else task.returncode != 0 and failure in log):
            raise RuntimeError(f'{label} returned {task.returncode}\n{log[-5500:]}')
        return log

    def stage(label, worker, *options, failure=None):
        if worker == 'decision':
            options += ('--decision-publisher', decision_publisher)
        return command(label, ['run', '--no-deps', '--rm', 'spark', '--stage', worker, '--run-id', run_id,
                               '--topic', topic, '--refined-topic', refined, *options], failure)

    def first_refined_copies(expected):
        copies = {}
        messages = []
        client = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
        try:
            positions = [TopicPartition(refined, partition) for partition in range(partitions)]
            client.assign(positions)
            ends = client.end_offsets(positions)
            remaining = sum(ends.values())
            for partition in positions:
                client.seek(partition, 0)
            deadline = time.monotonic() + 20
            while remaining:
                assert time.monotonic() < deadline, 'Refined Kafka topic read timed out'
                for partition, batch in client.poll(timeout_ms=1000).items():
                    for record in batch:
                        if record.offset >= ends[partition]:
                            continue
                        remaining -= 1
                        message = json.loads(record.value)
                        origin = (message['origin_partition'], message['origin_offset'])
                        assert record.partition == origin[0]
                        assert (message['event_id'], message['status']) == expected[origin]
                        messages.append(origin)
                        copies.setdefault(origin, {
                            'origin_partition': origin[0], 'origin_offset': origin[1],
                            'refined_partition': record.partition, 'refined_offset': record.offset,
                            'event_id': message['event_id'], 'status': message['status'],
                        })
        finally:
            client.close()
        assert set(copies) == set(expected)
        return copies, len(messages) - len(copies)

    command('00-services', ['up', '-d', '--wait', 'kafka', 'redis'])
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name, partitions, 1, topic_configs={'message.timestamp.type': 'LogAppendTime'})
                             for name in (topic, refined)])
    finally:
        admin.close()
    events = list(islice(events_in_order(source), 180))
    event_types = Counter(event['event_type'] for event in events)
    assert len(events) == 180 and all(event_types[name] for name in ('view', 'cart', 'purchase'))
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks='all', enable_idempotence=True)
    raw, expected = {}, {}
    seen = set()

    def send(rows, late_id=None):
        for event in rows:
            payload = on_wire(event)
            metadata = producer.send(topic, key=str(event['user_id']).encode(),
                                     value=json.dumps(payload).encode()).get(timeout=20)
            position = (metadata.partition, metadata.offset)
            event_id = event['event_id']
            status = 'duplicate' if event_id in seen else 'late' if event_id == late_id else 'accepted'
            raw[position] = payload
            expected[position] = (event_id, status)
            seen.add(event_id)

    client = redis.Redis.from_url('redis://127.0.0.1:36479/0', decode_responses=True, socket_timeout=5)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            send(events[:50])
            stage('01-decision-crash', 'decision', '--fail', 'decision-after-publish',
                  failure='INJECTED_DECISION_AFTER_PUBLISH')
            copies, count = first_refined_copies(expected)
            assert count == 0
            baseline_delta = pool.submit(stage, '02-initial-delta', 'delta')
            baseline_redis = stage('03-initial-redis', 'redis', '--batch-size', '17')
            baseline_delta.result()
            compare_redis(client, run_id, events[:50], copies)

            command('04-stop-redis', ['stop', 'redis'])
            try:
                client.ping()
            except redis.ConnectionError:
                pass
            else:
                raise AssertionError('Redis unexpectedly available during outage')
            late = {**events[0], 'event_seq': 9_000_000_000, 'event_time': events[0]['event_time'] - timedelta(days=1)}
            late['event_id'] = f"{late['source']}:{late['event_seq']}"
            send(events[:150] + [late], late['event_id'])
            stage('05-restart-decision', 'decision')
            copies, count = first_refined_copies(expected)
            assert count >= 50
            stage('06-delta-with-redis-down', 'delta', '--batch-size', '90')
            snapshot = stage('07-inspect-delta-before-redis-restart', 'inspect')
            outage_counts = compare_delta(snapshot, raw, copies)
            assert outage_counts == {'accepted': 150, 'duplicate': 50, 'late': 1}
            command('08-start-redis', ['up', '-d', '--wait', 'redis'])

            send(events[150:])
            stage('09-decision-new-events', 'decision')
            copies, count = first_refined_copies(expected)
            delta_failure = pool.submit(stage, '10-delta-write-failure', 'delta', '--fail', 'delta-before-write',
                                        failure='INJECTED_DELTA_BEFORE_WRITE')
            stage('11-redis-partial-write', 'redis', '--fail', 'redis-after-first',
                  failure='INJECTED_REDIS_AFTER_FIRST')
            delta_failure.result()
            caught_up = stage('12-redis-restarts-with-delta-down', 'redis', '--batch-size', '23')
            users = compare_redis(client, run_id, events, copies)
            assert sum(batch['applied'] for batch in markers(caught_up, 'DATA06_REDIS ')) == 129
            stage('13-delta-crash-after-commit', 'delta', '--fail', 'delta-after-write',
                  failure='INJECTED_DELTA_AFTER_WRITE')
            stage('14-restart-delta', 'delta')
            final = compare_delta(stage('15-inspect-final-delta', 'inspect'), raw, copies)
    finally:
        client.close()
        producer.close()
    assert final == {'accepted': 180, 'duplicate': 50, 'late': 1}
    points = markers(baseline_redis, 'DATA06_LATENCY ')
    assert len(points) == 50
    report = {
        'run_id': run_id, 'source': source.name, 'kafka_partitions': partitions,
        'decision_publisher': decision_publisher,
        'raw_records': len(raw), 'refined_physical_records': len(copies) + count, 'refined_republished_copies': count,
        'delta_raw_rows': len(raw), 'delta_refined_rows': len(copies), 'delta_decisions': dict(final),
        'accepted_event_types': dict(event_types), 'missing_or_extra_origin_positions': 0,
        'duplicate_delta_origin_positions': 0, 'redis_double_applications': 0, 'verified_redis_users': users,
        'redis_and_delta_final_positions_match': True,
        'delta_processed_while_redis_stopped': dict(outage_counts),
        'redis_caught_up_while_delta_failed': True,
        'tested_failures': ['decision after publish', 'Redis process stopped', 'Redis partial apply',
                            'Delta before write', 'Delta after commit'],
        'baseline_latency': {'sample': len(points), 'context': 'sequential local available-now startup',
                             'action_kafka_to_decision': percentiles(points, 0, 1),
                             'decision_to_refined_kafka': percentiles(points, 1, 2),
                             'refined_kafka_to_redis': percentiles(points, 2, 3),
                             'action_kafka_to_redis': percentiles(points, 0, 3)},
        'elapsed_seconds': round(time.monotonic() - started, 2),
    }
    (result / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2), flush=True)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-06-refined')
    parser.add_argument('--decision-publisher', choices=('spark', 'driver'), default='spark')
    options = parser.parse_args()
    verify(options.source, options.output, decision_publisher=options.decision_publisher)
