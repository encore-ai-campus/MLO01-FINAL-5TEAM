"""Measure freshness and compare Redis revisions with state rebuilt from committed Delta rows."""
import argparse
import json
import subprocess
from collections import Counter
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import redis

from benchmark_refined import (COMPOSE, ENVIRONMENT, ROOT, Workload, committed_at,
                               offsets, read_topic, read_window)
from verify_refined import compare_delta, compare_redis, markers, percentiles
from reco.replay_parquet import events_in_order


def freshness(arrivals, acks, cutoff_ms, threshold_ms):
    pending = sum(key not in acks or acks[key] > cutoff_ms for key in arrivals)
    timely = sum(key in acks and arrived <= acks[key] <= min(cutoff_ms, arrived + threshold_ms)
                 for key, arrived in arrivals.items())
    pending_overdue = sum(arrived + threshold_ms <= cutoff_ms and
                          (key not in acks or acks[key] > cutoff_ms)
                          for key, arrived in arrivals.items())
    late = len(arrivals) - pending - timely
    decided = timely + late + pending_overdue
    return {'threshold_ms': threshold_ms, 'cutoff_ms': cutoff_ms, 'total': len(arrivals),
            'unapplied_at_cutoff': pending, 'within_threshold': timely,
            'applied_late_by_cutoff': late,
            'failed_including_unapplied': len(arrivals) - timely,
            'percent': round(100 * timely / len(arrivals), 4),
            'unapplied_overdue': pending_overdue, 'unapplied_not_due': pending - pending_overdue,
            'deadline_failures': late + pending_overdue,
            'deadline_percent_excluding_not_due': round(100 * timely / decided, 4) if decided else None}


def index_revisions(records):
    indexed = {(p, offset, user): values for p, offset, user, *values in records}
    if len(indexed) != len(records):
        raise ValueError('A revision was emitted more than once')
    return indexed


def aggregates(refined, final_states):
    decisions = Counter(row['status'] for row in refined)
    history = [event for state in final_states.values() for event in state['history']]
    if len(history) != decisions['accepted']:
        raise ValueError('Redis history coverage does not match accepted events')
    return {'decisions': dict(decisions), 'events_by_type': dict(Counter(e['event_type'] for e in history)),
            'events_by_product_and_type': dict(Counter(f"{e['product_id']}:{e['event_type']}" for e in history)),
            'events_by_user': {str(user): state['features']['event_count'] for user, state in final_states.items()}}


def measure_freshness(arrivals, acks):
    cutoff = max(arrivals.values())
    return {name: [freshness(arrivals, acks, boundary, n) for n in (3000, 5000)]
            for name, boundary in (('ingress_end', cutoff), ('five_seconds_after_ingress', cutoff + 5000),
                                   ('five_minute_window', min(arrivals.values()) + 300000))}


def run_case(test, events, measured_ids):
    test.publish('warm', 1_000_000)
    with test.workers('live', stage_options={'redis': ('--trace-revisions',)}) as (workers, _):
        test.wait(workers, 'live', test.warm)
        test.publish('workload', 10)
        progress = test.wait(workers, 'live', test.warm + test.size)
    raw = read_topic(test.actions, test.bootstrap)
    refined = read_topic(test.refined, test.bootstrap)
    by_origin = {(p, o): row for p, o, _, row in raw}
    arrivals = {(p, o): at for p, o, at, row in raw if row['event_id'] in measured_ids}
    copies = {}
    for p, o, _, row in refined:
        origin = (row['origin_partition'], row['origin_offset'])
        copies.setdefault(origin, {'origin_partition': origin[0], 'origin_offset': origin[1],
                                   'refined_partition': p, 'refined_offset': o,
                                   'event_id': row['event_id'], 'status': row['status']})
    if len(raw) != len(by_origin) or len(refined) != len(copies) or len(arrivals) != test.size:
        raise ValueError('Kafka event coverage or uniqueness differs from input')
    if Counter(row['event_id'] for row in by_origin.values()) != Counter(e['event_id'] for e in events):
        raise ValueError('Original topic event IDs differ from the replay input')

    report_path = test.folder / f'{test.run_id}-inspect.log'
    with report_path.open('wb') as log:
        subprocess.run(test.compose + ['run', '--no-deps', '--rm', 'spark', '--stage', 'inspect',
                       '--trace-revisions', '--kafka', test.kafka, '--run-id', test.run_id,
                       '--topic', test.actions, '--refined-topic', test.refined],
                       cwd=ROOT, env=test.environment, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=180)
    inspection = report_path.read_text(encoding='utf-8', errors='replace')
    if compare_delta(inspection, by_origin, copies) != {'accepted': len(events)}:
        raise ValueError('Delta did not preserve exactly one accepted row per input')
    online = index_revisions(markers((test.folder / f'{test.run_id}-live-redis.log')
                                     .read_text(encoding='utf-8', errors='replace'),
                                     'DATA06_REDIS_REVISION '))
    offline = index_revisions(markers(inspection, 'DATA06_DELTA_REVISION '))
    states = {key: values[0] for key, values in offline.items()}
    if len(states) != len(events) or {key: values[1] for key, values in online.items()} != states:
        raise ValueError('Redis and Delta state disagree at identical revisions')

    acks, sync_points, final = {}, [], {}
    for (p, o, user), values in sorted(online.items()):
        acked, state = values
        origin = state['origin_partition'], int(state['origin_offset'])
        final[user] = state
        if origin in arrivals:
            acks[origin] = acked
            stored = committed_at(progress['delta'], test.refined, p, o)
            sync_points.append((acked, max(acked, stored)))
    if acks.keys() != arrivals.keys() or any(acks[key] < arrivals[key] for key in arrivals):
        raise ValueError('Redis write acknowledgements do not cover the measured events')

    with redis.Redis(host='127.0.0.1', port=int(test.environment['DATA06_REDIS_PORT']),
                     decode_responses=True) as client:
        users = compare_redis(client, test.run_id, events, copies)
        for user, expected in final.items():
            if json.loads(client.get(f'data06:{test.run_id}:user:{user}')) != expected:
                raise ValueError('Final Redis value differs from the last revision')
    sync_max = max(right - left for left, right in sync_points)
    return {'run_id': test.run_id, 'users': users, 'revisions_compared': len(states),
            'redis_batches': len(progress['redis']), 'delta_batches': len(progress['delta']),
            'freshness': measure_freshness(arrivals, acks),
            'redis_latency': percentiles([(at, acks[key]) for key, at in arrivals.items()], 0, 1),
            'sync': {**percentiles(sync_points, 0, 1), 'max_ms': sync_max,
                     'within_60s': sum(right - left <= 60000 for left, right in sync_points),
                     'measured_revisions': len(sync_points)},
            'raw_rows': len(by_origin), 'refined_rows': len(copies)}, states, aggregates(copies.values(), final), {
                worker: [{topic.split('-')[-1]: value for topic, value in offsets(batch).items()}
                         for batch in progress[worker]] for worker in ('redis', 'delta')}


def run(source, output):
    folder = output / f'revision-sync-{uuid4().hex[:8]}'
    folder.mkdir(parents=True)
    print(f'Reports: {folder}', flush=True)
    window, warm, size = read_window(source, folder)
    events = [event for name in ('warm', 'workload') for event in events_in_order(folder / f'{name}.parquet')]
    ids = set(pq.read_table(folder / 'workload.parquet', columns=['event_id'])['event_id'].to_pylist())
    report = {'complete': False, 'window_utc': window, 'speed': 10, 'warm': warm, 'events': size, 'runs': []}
    results = []
    for name, redis_trigger, delta_trigger in (('baseline', '1 second', '30 seconds'),
                                               ('replay', '2 seconds', '10 seconds')):
        print(f'{name}: Redis {redis_trigger}, Delta {delta_trigger}', flush=True)
        test = Workload(folder, name, delta_trigger, warm, size, redis_trigger=redis_trigger,
                        decision_trigger='500 milliseconds', decision_publisher='driver',
                        compose=COMPOSE, environment=ENVIRONMENT)
        result, *comparison = run_case(test, events, ids)
        report['runs'].append({**result, 'redis_trigger': redis_trigger, 'delta_trigger': delta_trigger})
        results.append(comparison)
        (folder / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    (first, first_counts, first_boundaries), (second, second_counts, second_boundaries) = results
    boundary_changes = {worker: first_boundaries[worker] != second_boundaries[worker] for worker in ('redis', 'delta')}
    if not all(boundary_changes.values()) or first.keys() != second.keys():
        raise ValueError('Test did not align revisions across different batch boundaries')
    differences = sum(first[key] != second[key] for key in first)
    aggregate_differences = {key: sum(first_counts[key].get(bucket, 0) != second_counts[key].get(bucket, 0)
                                      for bucket in first_counts[key].keys() | second_counts[key].keys())
                             for key in first_counts}
    report.update({'complete': True, 'different_batch_boundaries': boundary_changes,
                   'cross_run_revision_differences': differences,
                   'aggregate_differences_by_group': aggregate_differences,
                   'aggregate_totals': {key: sum(values.values()) for key, values in first_counts.items()}})
    (folder / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    if differences or any(aggregate_differences.values()):
        raise ValueError('Reprocessing changed revision or aggregate values')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-revision-sync')
    args = parser.parse_args()
    run(args.source, args.output)
