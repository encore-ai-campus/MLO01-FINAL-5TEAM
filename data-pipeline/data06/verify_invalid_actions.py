"""Verify that invalid inputs are quarantined across a Kafka publication retry."""
import argparse
import json
import subprocess
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import redis
from kafka import KafkaProducer

from benchmark_refined import COMPOSE, ROOT, Workload, read_topic
from verify_refined import compare_redis, markers, on_wire


def fixture():
    start = datetime(2019, 11, 17, 12, tzinfo=timezone.utc)

    def action(seq, kind):
        return {'event_id': f'invalid-actions-test:{seq}', 'source': 'invalid-actions-test', 'event_seq': seq,
                'event_time': start + timedelta(seconds=seq), 'event_type': kind, 'product_id': 101, 'user_id': 10}

    view, cart, purchase = action(0, 'view'), action(3, 'cart'), action(7, 'purchase')
    incoming = [
        ('10', on_wire(view), 'accepted', None),
        ('10', '{"event_id":', 'invalid', 'malformed_payload'),
        ('999', on_wire({**action(2, 'view'), 'event_time': start + timedelta(days=1)}),
         'invalid', 'invalid_kafka_key'),
        ('10', on_wire(cart), 'accepted', None),
        ('10', on_wire(cart), 'duplicate', None),
        ('10', on_wire({**cart, 'product_id': 999}), 'invalid', 'conflicting_event_payload'),
        ('10', {**on_wire(action(6, 'view')), 'event_time': 'bad-date'}, 'invalid', 'invalid_event_time'),
        ('10', on_wire(purchase), 'accepted', None),
    ]
    return incoming, [view, cart, purchase]


def run_stage(test, log_name, stage, *options, failure=None):
    path = test.folder / log_name
    command = test.compose + ['run', '--no-deps', '--rm', 'spark', '--stage', stage, '--run-id', test.run_id,
                               '--topic', test.actions, '--refined-topic', test.refined, *options]
    with path.open('wb') as log:
        result = subprocess.run(command, cwd=ROOT, env=test.environment,
                                stdout=log, stderr=subprocess.STDOUT, timeout=180)
    output = path.read_text(encoding='utf-8', errors='replace')
    if not (result.returncode == 0 if failure is None else result.returncode != 0 and failure in output):
        raise RuntimeError(f'{stage} returned {result.returncode}\n{output[-5000:]}')
    return output


def verify(output):
    folder = output / f'invalid-actions-{uuid4().hex[:8]}'
    folder.mkdir(parents=True)
    print(f'Reports: {folder}', flush=True)
    override = folder / 'compose.override.json'
    override.write_text(json.dumps({'services': {'spark': {'volumes': [
        {'type': 'bind', 'source': Path(__file__).with_name(name).resolve().as_posix(),
         'target': f'/opt/data06/{name}', 'read_only': True} for name in ('refined.py', 'redis_kafka.py')
    ]}}}), encoding='utf-8')
    incoming, accepted = fixture()
    test = Workload(folder, 'check', '2 seconds', 0, len(incoming), decision_publisher='driver',
                    redis_direct=True, compose=COMPOSE + ['-f', str(override)])
    raw, expected = {}, {}
    with closing(KafkaProducer(bootstrap_servers=test.bootstrap, acks='all', enable_idempotence=True)) as producer:
        for key, payload, status, reason in incoming:
            value = payload if isinstance(payload, str) else json.dumps(payload)
            sent = producer.send(test.actions, partition=0, key=key.encode(), value=value.encode()).get(timeout=20)
            origin = (sent.partition, sent.offset)
            raw[origin] = value
            expected[origin] = (status, reason)

    print('Forcing a failure after Kafka publication', flush=True)
    run_stage(test, 'decision-failure.log', 'decision', '--decision-publisher', 'driver',
              '--fail', 'decision-after-publish', failure='INJECTED_DECISION_AFTER_PUBLISH')
    assert len(read_topic(test.refined, test.bootstrap)) == len(incoming)
    print('Restarting decision and checking Redis and Delta', flush=True)
    with test.workers('restart') as (workers, _):
        test.wait(workers, 'restart', len(incoming), refined_count=2 * len(incoming))

    copies, physical = {}, Counter()
    for partition, offset, _, row in read_topic(test.refined, test.bootstrap):
        origin = (row['origin_partition'], row['origin_offset'])
        assert partition == origin[0]
        assert (row['status'], row['rejection_reason']) == expected[origin]
        physical[origin] += 1
        copies.setdefault(origin, {'origin_partition': origin[0], 'origin_offset': origin[1],
                                   'refined_partition': partition, 'refined_offset': offset,
                                   'status': row['status'], 'event_id': row.get('event_id')})
    assert physical == {origin: 2 for origin in expected}

    rows = defaultdict(list)
    for table, row in markers(run_stage(test, 'delta-inspect.log', 'inspect'), 'DATA06_INSPECT '):
        rows[table].append(row)
    assert len(rows['raw']) == len(raw) and {(r['partition'], r['offset']): r['value'] for r in rows['raw']} == raw
    assert len(rows['refined']) == len(copies)
    assert {(r['origin_partition'], r['origin_offset']): r for r in rows['refined']} == copies
    rejected = {origin: reason for origin, (status, reason) in expected.items() if status == 'invalid'}
    assert len(rows['rejected']) == len(rejected)
    assert {(r['origin_partition'], r['origin_offset']): r['rejection_reason'] for r in rows['rejected']} == rejected

    with redis.Redis(host='127.0.0.1', port=int(test.environment['DATA06_REDIS_PORT']),
                     decode_responses=True, socket_timeout=5) as client:
        users = compare_redis(client, test.run_id, accepted, copies)
        state = json.loads(client.get(f'data06:{test.run_id}:user:10'))
    assert [event['event_id'] for event in state['history']] == [event['event_id'] for event in accepted]
    progress = (folder / f'{test.run_id}-restart-redis.log').read_text(encoding='utf-8')
    assert sum(row['applied'] for row in markers(progress, 'DATA06_REDIS ')) == len(accepted)
    report = {'complete': True, 'raw_count': len(raw), 'refined_kafka_count_after_retry': sum(physical.values()),
              'delta_counts': {name: len(records) for name, records in rows.items()},
              'decision_counts': dict(Counter(status for status, _ in expected.values())),
              'rejection_reasons': dict(Counter(rejected.values())),
              'redis_users': users, 'redis_applied_once': len(accepted),
              'redis_and_delta_missing_or_duplicate': 0, 'rejected_records': rows['rejected']}
    (folder / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-invalid-actions-check')
    verify(parser.parse_args().output.resolve())
