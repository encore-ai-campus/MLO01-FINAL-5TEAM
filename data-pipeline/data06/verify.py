"""Exercise resend and Spark failure recovery using the fixed November replay prefix."""
import argparse
import hashlib
import json
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from datetime import timedelta
from itertools import islice
from pathlib import Path

import redis
from kafka import KafkaConsumer, TopicPartition

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from reco.replay_parquet import events_in_order


COMPOSE = ['docker', 'compose', '-p', 'mlo-data06', '-f', str(Path(__file__).with_name('compose.yaml'))]
BOOTSTRAP = '127.0.0.1:39092'
REDIS = 'redis://127.0.0.1:36379/0'


def redis_expected(events):
    users = defaultdict(list)
    for event in events:
        users[event['user_id']].append(event)
    expected = {}
    for user, history in users.items():
        start = 0
        for i, pair in enumerate(zip(history, history[1:]), 1):
            if pair[1]['event_time'] - pair[0]['event_time'] > timedelta(minutes=30):
                start = i
        last = history[-1]
        session = history[start:]
        purchased = {event['product_id'] for event in history
                     if event['event_type'] == 'purchase'
                     and event['event_time'] > last['event_time'] - timedelta(days=30)}
        expected[user] = {'event_count': len(history), 'last_event_id': last['event_id'],
                          'last_event_time': last['event_time'].strftime('%Y-%m-%dT%H:%M:%SZ'),
                          'session_start_event_id': session[0]['event_id'],
                          'viewed_product_ids': sorted({e['product_id'] for e in session if e['event_type'] == 'view'}),
                          'carted_product_ids': sorted({e['product_id'] for e in session if e['event_type'] == 'cart'}),
                          'purchased_product_ids_30d': sorted(purchased)}
    return expected


def assert_redis(client, run_id, expected):
    prefix = f'data06:{run_id}:user:'
    keys = sorted(client.scan_iter(match=prefix + '*'))
    actual = {int(key.removeprefix(prefix)): json.loads(payload)
              for key, payload in zip(keys, client.mget(keys))} if keys else {}
    if actual != expected:
        raise AssertionError(f'Redis differs for {run_id}: {len(actual)} observed, {len(expected)} expected users')


def verify(source, results_root):
    run_id = 'data06-' + uuid.uuid4().hex[:12]
    results = results_root / run_id
    results.mkdir(parents=True)

    def execute(command, label, expected_failure=None):
        path = results / f'{label}.log'
        print(f'Running {label}', flush=True)
        with path.open('w', encoding='utf-8') as log:
            task = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=300)
        output = path.read_text(encoding='utf-8', errors='replace')
        if (expected_failure and (task.returncode == 0 or expected_failure not in output)) or (
                not expected_failure and task.returncode):
            raise RuntimeError(f'{label} returned {task.returncode}. {output[-4000:]}')
        return output

    def spark(label, *flags, expected_failure=None):
        return execute(COMPOSE + ['run', '--rm', 'spark', '--run-id', run_id, '--topic', run_id, *flags],
                       label, expected_failure)

    def send(count, label):
        result = execute([sys.executable, '-m', 'reco.replay_parquet', str(source.resolve()),
                          '--speed', '1000000', '--max-events', str(count),
                          '--kafka-bootstrap', BOOTSTRAP, '--kafka-topic', run_id], label)
        completed = json.loads(result.splitlines()[-1])
        assert completed['emitted'] == count

    def assert_delta(label, rows):
        output = spark(label, '--inspect')
        measured = json.loads(next(line.removeprefix('DATA06_DELTA ') for line in output.splitlines()
                                   if line.startswith('DATA06_DELTA ')))
        ids = sorted(row['event_id'] for row in rows)
        expected_hash = hashlib.sha256(''.join(event_id + '\n' for event_id in ids).encode()).hexdigest()
        assert measured == {'rows': len(ids), 'unique_ids': len(ids), 'event_id_sha256': expected_hash}
        return measured

    topic_command = COMPOSE + ['exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
                               '--bootstrap-server', 'kafka:9092', '--create', '--topic', run_id,
                               '--partitions', '1', '--replication-factor', '1']
    execute(topic_command, '01-create-topic')
    events = []
    action_counts = dict.fromkeys(('view', 'cart', 'purchase'), 0)
    for event in islice(events_in_order(source), 1000):
        events.append(event)
        action_counts[event['event_type']] += 1
        if len(events) >= 100 and all(action_counts.values()):
            break
    if len(events) < 100 or not all(action_counts.values()):
        raise ValueError('The test requires a real prefix with view, cart and purchase events')
    first_state, final_state = redis_expected(events[:50]), redis_expected(events)
    client = redis.Redis.from_url(REDIS, decode_responses=True, socket_timeout=15)
    started = time.monotonic()
    send(50, '02-send-first-50')
    spark('03-initial-spark')
    assert_redis(client, run_id, first_state)
    send(len(events), '04-resend-first-50-and-send-new')
    spark('05-fail-after-delta', '--fail-after-delta', expected_failure='DATA06_TEST_FAILURE_AFTER_DELTA')
    assert_redis(client, run_id, first_state)
    assert_delta('06-delta-committed-before-restart', events)
    spark('07-restart-same-checkpoint')
    assert_redis(client, run_id, final_state)
    summary = assert_delta('08-verify-final-delta', events)
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, request_timeout_ms=15000)
    try:
        partition = TopicPartition(run_id, 0)
        broker_messages = consumer.end_offsets([partition])[partition]
    finally:
        consumer.close()
    assert broker_messages == 50 + len(events)
    report = {'run_id': run_id, 'source': source.name, 'kafka_messages': broker_messages,
              'resent_events': 50, 'delta': summary, 'missing_events': 0, 'duplicate_delta_events': 0,
              'redis_users_before_failure': len(first_state), 'redis_users_after_restart': len(final_state),
              'redis_matches_source_after_restart': True, 'redis_unchanged_after_injected_failure': True,
              'restart_used_same_checkpoint': True,
              'event_types': action_counts,
              'test_wall_seconds': round(time.monotonic() - started, 2)}
    (results / 'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    print(f'Files: {results}', flush=True)


if __name__ == '__main__':
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--source', type=Path, default=ROOT/'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    cli.add_argument('--output', type=Path, default=ROOT/'data/data-06-kafka-spark')
    args = cli.parse_args()
    verify(args.source, args.output)
