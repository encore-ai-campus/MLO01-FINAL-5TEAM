"""Compare Kafka decisions and Redis state at two Spark input batch sizes."""
import argparse
import hashlib
import json
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import redis
from kafka import KafkaProducer

from benchmark_refined import COMPOSE, ENVIRONMENT, ROOT, Workload, offsets, read_topic
from verify_refined import compare_redis, on_wire


def fixture():
    start = datetime(2019, 11, 17, 12, tzinfo=timezone.utc)
    source = 'late-batch-test'
    rows = [
        (0, 0, 180, 10, 'view', 'accepted'),
        (0, 1, 120, 11, 'cart', 'accepted'),  # Exactly 60 seconds behind the partition.
        (0, 2, 119, 12, 'purchase', 'late'),
        (0, 3, 175, 10, 'view', 'late'),  # This user's earlier action arrived out of order.
        (0, 4, 200, 10, 'purchase', 'accepted'),
        (0, 0, 180, 10, 'view', 'duplicate'),
        (0, 6, 139, 13, 'view', 'late'),
        (0, 7, 200, 10, 'cart', 'accepted'),
        (1, 8, 30, 20, 'view', 'accepted'),  # Another partition cannot advance this one's clock.
        (1, 9, 160, 21, 'view', 'accepted'),
        (1, 10, 99, 22, 'purchase', 'late'),
        (1, 11, 100, 23, 'cart', 'accepted'),
    ]
    return [(part, {'event_id': f'{source}:{seq}', 'source': source, 'event_seq': seq,
                    'event_time': start + timedelta(seconds=second), 'event_type': event_type,
                    'product_id': 100 + seq, 'user_id': user}, status)
            for part, seq, second, user, event_type, status in rows]


def check_case(test, rows, batch_size):
    expected, accepted, expected_raw = {}, [], {}
    with closing(KafkaProducer(bootstrap_servers=test.bootstrap, acks='all', enable_idempotence=True)) as producer:
        for partition, event, status in rows:
            payload = on_wire(event)
            receipt = producer.send(test.actions, partition=partition, key=str(event['user_id']).encode(),
                                    value=json.dumps(payload).encode()).get(timeout=20)
            origin = (receipt.partition, receipt.offset)
            expected[origin] = (event['event_id'], status)
            expected_raw[origin] = payload
            if status == 'accepted':
                accepted.append(event)
    assert {(p, o): row for p, o, _, row in read_topic(test.actions, test.bootstrap)} == expected_raw

    options = {'decision': ('--batch-size', str(batch_size))}
    with test.workers('live', stage_options=options) as (workers, _):
        progress = test.wait(workers, 'live', len(rows))
    copies = {}
    refined = read_topic(test.refined, test.bootstrap)
    for partition, offset, _, row in refined:
        origin = (row['origin_partition'], row['origin_offset'])
        assert origin not in copies and partition == origin[0]
        copies[origin] = {'event_id': row['event_id'], 'status': row['status'], 'origin_partition': origin[0],
                          'origin_offset': origin[1], 'refined_offset': offset}
    actual = {origin: (row['event_id'], row['status']) for origin, row in copies.items()}
    assert actual == expected, f'Wrong decisions at batch size {batch_size}: {actual}'

    with redis.Redis(host='127.0.0.1', port=int(test.environment['DATA06_REDIS_PORT']),
                     decode_responses=True, socket_timeout=5) as client:
        compare_redis(client, test.run_id, accepted, copies)
        prefix = f'data06:{test.run_id}:user:'
        states = {key.removeprefix(prefix): json.loads(client.get(key))
                  for key in client.scan_iter(match=prefix + '*')}
    batches = [offsets(record)[test.actions] for record in progress['decision']]
    return {'run_id': test.run_id, 'decision_batch_size': batch_size, 'decision_batches': batches,
            'status_counts': dict(Counter(status for _, status in actual.values())),
            'redis_keys': len(states)}, actual, states


def verify(output):
    folder = output / f'late-batch-{uuid4().hex[:8]}'
    folder.mkdir(parents=True)
    print(f'Reports: {folder}', flush=True)
    # Run the workspace code without replacing the image used by other local runs.
    override = folder / 'compose.override.json'
    worker = Path(__file__).with_name('refined.py').resolve().as_posix()
    override.write_text(json.dumps({'services': {'spark': {'volumes': [
        {'type': 'bind', 'source': worker, 'target': '/opt/data06/refined.py', 'read_only': True}
    ]}}}), encoding='utf-8')
    rows = fixture()
    runs, results = [], []
    for batch_size in (1, 100):
        print(f'Decision batch size {batch_size}', flush=True)
        test = Workload(folder, f'size-{batch_size}', '2 seconds', 0, len(rows),
                        decision_trigger='500 milliseconds', decision_publisher='driver', redis_direct=True,
                        compose=COMPOSE + ['-f', str(override)])
        run, decisions, states = check_case(test, rows, batch_size)
        runs.append(run)
        results.append((decisions, states))
    assert len(runs[0]['decision_batches']) > len(runs[1]['decision_batches']) == 1
    (first_decisions, first_states), (second_decisions, second_states) = results
    assert first_decisions == second_decisions and first_states == second_states
    report = {'complete': True, 'input_records': len(rows), 'runs': runs, 'decision_differences': 0,
              'redis_state_differences': 0, 'full_redis_state_sha256': hashlib.sha256(
                  json.dumps(first_states, sort_keys=True).encode()).hexdigest(),
              'decisions': [{'partition': p, 'offset': o, 'event_id': event_id, 'status': status}
                            for (p, o), (event_id, status) in sorted(first_decisions.items())]}
    (folder / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f"Both runs: {runs[0]['status_counts']}, {runs[0]['redis_keys']} matching Redis keys", flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-late-batch-check')
    args = parser.parse_args()
    verify(args.output.resolve())
