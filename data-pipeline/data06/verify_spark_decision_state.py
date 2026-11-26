"""Verify batch independence and restart recovery of Spark decision state."""
import json
import os
import subprocess
from collections import Counter, defaultdict
from contextlib import closing
from uuid import uuid4

import redis
from kafka import KafkaProducer

from benchmark_refined import ROOT, Workload, offsets, read_topic
from verify_invalid_actions import run_stage
from verify_late_batches import fixture
from verify_refined import compare_delta, compare_redis, markers, on_wire


def check_case(test, rows, batch_size):
    expected, raw, accepted = {}, {}, []
    stage_options = ('--decision-state-store', 'spark', '--decision-publisher', 'driver',
                     '--batch-size', str(batch_size), '--benchmark-progress')
    prefix = 2
    with closing(KafkaProducer(bootstrap_servers=test.bootstrap, acks='all', enable_idempotence=True)) as producer:
        def emit(selected):
            for partition, event, status in selected:
                payload = on_wire(event)
                response = producer.send(test.actions, partition=partition, key=str(event['user_id']).encode(),
                                         value=json.dumps(payload).encode()).get(timeout=30)
                origin = response.partition, response.offset
                raw[origin] = payload
                expected[origin] = event['event_id'], status
                if status == 'accepted':
                    accepted.append(event)

        emit(rows[:prefix])
        run_stage(test, f'batch-{batch_size}-committed-prefix.log', 'decision', *stage_options)
        assert len(read_topic(test.refined, test.bootstrap)) == prefix
        emit(rows[prefix:])
    run_stage(test, f'batch-{batch_size}-fault-after-publish.log', 'decision', *stage_options,
              '--fail', 'decision-after-publish', failure='INJECTED_DECISION_AFTER_PUBLISH')
    republished = len(read_topic(test.refined, test.bootstrap)) - prefix
    assert republished > 0
    print(f'Batch size {batch_size}: already committed {prefix}, retrying {republished} published outputs', flush=True)
    with test.workers('restart', stage_options={'decision': stage_options}) as (workers, _):
        progress = test.wait(workers, 'restart', len(rows), refined_count=len(rows) + republished, timeout=300)

    assert {(p, o): row for p, o, _, row in read_topic(test.actions, test.bootstrap)} == raw
    physical, copies = defaultdict(list), {}
    for partition, offset, _, row in read_topic(test.refined, test.bootstrap):
        origin = row['origin_partition'], row['origin_offset']
        assert partition == origin[0]
        physical[origin].append((row['event_id'], row['status']))
        copies.setdefault(origin, {'origin_partition': origin[0], 'origin_offset': origin[1],
                                   'refined_partition': partition, 'refined_offset': offset,
                                   'event_id': row['event_id'], 'status': row['status']})
    assert physical.keys() == expected.keys()
    assert all(set(candidates) == {expected[origin]} for origin, candidates in physical.items())
    assert sum(len(candidates) - 1 for candidates in physical.values()) == republished
    assert {origin for origin, candidates in physical.items() if len(candidates) > 1}.isdisjoint({(0, 0), (0, 1)})
    decisions = compare_delta(run_stage(test, f'batch-{batch_size}-inspect-delta.log', 'inspect'), raw, copies)
    assert decisions == Counter(status for _, status in expected.values())

    with redis.Redis(host='127.0.0.1', port=int(test.environment['DATA06_REDIS_PORT']), decode_responses=True) as client:
        users = compare_redis(client, test.run_id, accepted, copies)
        prefix = f'data06:{test.run_id}:user:'
        states = {key.removeprefix(prefix): json.loads(client.get(key))
                  for key in client.scan_iter(match=prefix + '*')}
    # Refined offsets can differ because the injected failure replays different-sized batches.
    for state in states.values():
        state.pop('refined_offset')
    redis_log = (test.folder / f'{test.run_id}-restart-redis.log').read_text(encoding='utf-8')
    assert sum(row['applied'] for row in markers(redis_log, 'DATA06_REDIS ')) == len(accepted)
    subprocess.run(test.compose + ['run', '--no-deps', '--rm', '--entrypoint', '/bin/sh', 'spark', '-ec',
                   'test -d "$1/decision-spark-checkpoint/state" && test ! -e "$1/decision.sqlite"',
                   'checkpoint-check', f'/state/split/{test.run_id}'], cwd=ROOT, env=test.environment,
                   check=True, timeout=60, capture_output=True)
    return ({'batch_size': batch_size, 'resume_batch_offsets': [offsets(b)[test.actions] for b in progress['decision']],
             'republished_after_failure': republished, 'statuses': dict(decisions), 'verified_redis_users': users,
             'delta_raw_rows': len(raw), 'delta_refined_rows': len(copies), 'sink_missing_or_duplicate': 0,
             'spark_state_exists_without_decision_sqlite': True},
            {origin: copies[origin]['status'] for origin in copies}, states)


def verify():
    output = ROOT / 'data/data-spark-decision-state-check' / f'spark-state-{uuid4().hex[:8]}'
    output.mkdir(parents=True)
    print(f'Reports: {output}', flush=True)
    env = {**os.environ, 'DATA06_KAFKA_PORT': '39362', 'DATA06_REDIS_PORT': '36562',
           'DATA06_DECISION_STATE_STORE': 'spark'}
    compose = ['docker', 'compose', '-p', 'mlo-data06-state-v42', '-f', str(ROOT / 'data06/compose.yaml')]
    subprocess.run(compose + ['up', '-d', '--wait', 'kafka', 'redis'], cwd=ROOT, env=env, check=True, timeout=150)
    rows = fixture()
    cases = []
    for batch_size in (1, 100):
        test = Workload(output, f'batch-{batch_size}', '2 seconds', 0, len(rows),
                        compose=compose, environment=env, bootstrap='127.0.0.1:39362',
                        decision_publisher='driver', redis_direct=True)
        cases.append(check_case(test, rows, batch_size))
    first, first_decisions, first_states = cases[0]
    second, second_decisions, second_states = cases[1]
    assert first_decisions == second_decisions and first_states == second_states
    assert len(first['resume_batch_offsets']) > len(second['resume_batch_offsets'])
    result = {'complete': True, 'input_records': len(rows), 'runs': [first, second],
              'decision_differences': 0, 'redis_state_differences_excluding_refined_offsets': 0}
    (output / 'verification.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    verify()
