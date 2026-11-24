"""Replay a November day at 10x and restore the direct Redis sink at midday."""
import argparse
import json
import subprocess
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.compute as pc
import pyarrow.parquet as pq
import redis

from benchmark_refined import ROOT, PROGRESS, Workload, offsets, read_topic
from compare_cluster_modes import LOCAL_SPARK
from run_cluster_check import BOOTSTRAP, COMPOSE, ENV, KAFKA, compose, wait_for_workers
from run_load_failure_check import compare_sinks
from verify_refined import markers, percentiles


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


def inputs(source, folder, start, midpoint, end):
    table = pq.read_table(source, filters=[('event_time', '>=', start - timedelta(days=1)),
                                           ('event_time', '<', end)])
    times = table['event_time']
    previous = table.filter(pc.less(times, start))
    before = table.filter(pc.and_(pc.greater_equal(times, start), pc.less(times, midpoint)))
    after = table.filter(pc.greater_equal(times, midpoint))
    warm = previous.slice(max(0, previous.num_rows - 20))
    counts = {}
    for name, data in (('warm', warm), ('before', before), ('after', after)):
        if not data.num_rows:
            raise ValueError(f'Empty replay input: {name}')
        pq.write_table(data, folder / f'{name}.parquet')
        counts[name] = data.num_rows
    return counts


def last_offsets(test, worker, phase):
    path = test.folder / f'{test.run_id}-{phase}-{worker}.log'
    with path.open('rb') as log:
        size = log.seek(0, 2)
        log.seek(max(0, size - 2 * 1024 * 1024))
        lines = log.read().decode('utf-8', errors='replace').rpartition('\n')[0].splitlines()
    batches = markers('\n'.join(line for line in lines if line.startswith(PROGRESS)), PROGRESS)
    return offsets(batches[-1]) if batches else {}


def wait_offsets(test, workers, phases, target, fallback=None):
    # Phase completion requires both topics on Delta and the refined topic on Redis.
    deadline = time.monotonic() + 1200
    while time.monotonic() < deadline:
        positions = dict(fallback or {})
        for worker, process in workers.items():
            if process.poll() is not None:
                raise RuntimeError(f'{worker} exited; see the {phases[worker]} log')
            positions[worker] = last_offsets(test, worker, phases[worker]) or (fallback or {}).get(worker, {})
        if (sum(positions['decision'].get(test.actions, {}).values()) == target
                and sum(positions['redis'].get(test.refined, {}).values()) == target
                and positions['delta'].get(test.actions) == positions['decision'].get(test.actions)
                and positions['delta'].get(test.refined) == positions['redis'].get(test.refined)):
            return positions['redis'][test.refined]
        time.sleep(1)
    raise TimeoutError(f'Workers did not reach {target} records: {positions}')


def snapshot(client, prefix):
    keys = sorted(set(client.scan_iter(match=prefix + '*')))
    return {key: json.loads(value) for index in range(0, len(keys), 1000)
            for key, value in zip(keys[index:index + 1000], client.mget(keys[index:index + 1000]), strict=True)}


def publish_segment(test, name, duration, workers):
    def supervise():
        for stage, process in workers.items():
            if process.poll() is not None:
                raise RuntimeError(f'{stage} exited during {name}; check its worker log')

    test.publish(name, 10, timeout=int(duration.total_seconds() / 10) + 600, supervise=supervise)


def await_reader(test, process, expected):
    path = test.folder / f'{test.run_id}-after-redis.log'
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError('Restored Redis reader exited before becoming ready')
        log = path.read_text(encoding='utf-8', errors='replace').rpartition('\n')[0]
        ready = markers(log, 'DATA06_REDIS_READY ')
        if ready:
            if ready[-1]['positions'] != expected:
                raise ValueError('Redis reader resumed at unexpected Kafka offsets')
            return
        time.sleep(.5)
    raise TimeoutError('Restored Redis reader did not become ready')


def latency(test, raw, measured_ids, before_ids):
    expected = {(p, o): (arrived, message) for p, o, arrived, message in raw}
    seen = set()
    groups = {'overall': [], 'before_recovery': [], 'after_recovery': []}
    hourly = {}
    for phase in ('before', 'after'):
        path = test.folder / f'{test.run_id}-{phase}-redis.log'
        with path.open(encoding='utf-8', errors='replace') as log:
            for line in log:
                for batch in markers(line, 'DATA06_REDIS_APPLIED '):
                    for partition, offset, arrived, acked in batch:
                        position = (partition, offset)
                        broker_time, message = expected[position]
                        if position in seen or broker_time != arrived or acked < arrived:
                            raise ValueError(f'Invalid Redis application for {position}')
                        seen.add(position)
                        event = message['event_id']
                        if event not in measured_ids:
                            continue
                        segment = 'before_recovery' if event in before_ids else 'after_recovery'
                        point = (arrived, acked)
                        groups['overall'].append(point)
                        groups[segment].append(point)
                        hour = datetime.fromisoformat(message['event_time']).astimezone(timezone.utc).replace(
                            minute=0, second=0, microsecond=0).isoformat()
                        hourly.setdefault(hour, []).append(point)
    if seen != set(expected) or len(groups['overall']) != len(measured_ids):
        raise ValueError('Redis application log does not cover every source offset exactly once')

    def summarize(rows):
        metrics = percentiles(rows, 0, 1)
        return {'events': len(rows), **metrics, 'passes': metrics['p95_ms'] <= 3000 and metrics['p99_ms'] <= 5000}

    return {**{name: summarize(rows) for name, rows in groups.items()},
            'hourly': {hour: summarize(hourly[hour]) for hour in sorted(hourly)}}


def run(source, output, day, *, pilot=False, resume_first_drain=None):
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    if pilot:
        start += timedelta(hours=12, minutes=37)
    duration = timedelta(minutes=3) if pilot else timedelta(days=1)
    midpoint, end = start + duration / 2, start + duration
    folder = resume_first_drain.resolve() if resume_first_drain else output.resolve() / ('direct-day-' + uuid4().hex[:8])
    if not resume_first_drain:
        folder.mkdir(parents=True)
    print(f'Reports and logs: {folder}', flush=True)
    if resume_first_drain:
        report = json.loads((folder / 'verification.json').read_text(encoding='utf-8'))
        if report['phase'] != 'draining_first_half' or report['complete']:
            raise ValueError('Resume requires a previously published, uncompleted first half')
    else:
        report = {'complete': False, 'pilot': pilot, 'window_utc': [start.isoformat(), end.isoformat()], 'speed': 10,
                  'counts': inputs(source, folder, start, midpoint, end)}
    n = report['counts']

    def record(phase):
        report['phase'] = phase
        save(folder / 'verification.json', report)
        print(phase, flush=True)

    if not resume_first_drain:
        record('starting')
    compose('up', '-d', '--wait', '--wait-timeout', '120')
    wait_for_workers()
    test = Workload(folder, 'check', '30 seconds', n['warm'], n['before'] + n['after'],
                    decision_trigger='500 milliseconds', compose=COMPOSE, bootstrap=BOOTSTRAP,
                    environment=ENV, replication=3, kafka=KAFKA, decision_publisher='driver',
                    redis_compose=LOCAL_SPARK, redis_direct=True, create_topics=not resume_first_drain)
    prefix = f'data06:{test.run_id}:user:'
    phases = {'decision': 'pipeline', 'delta': 'pipeline', 'redis': 'before'}
    stored = {stage: last_offsets(test, stage, phase) for stage, phase in phases.items()} if resume_first_drain else {}
    pipeline_phase = 'pipeline-resumed' if resume_first_drain else 'pipeline'
    if resume_first_drain:
        phases.update({'decision': pipeline_phase, 'delta': pipeline_phase})
    with test.workers(pipeline_phase, stages=('decision', 'delta')) as (workers, _), redis.Redis(
            host='127.0.0.1', port=int(ENV['DATA06_REDIS_PORT']), decode_responses=True, socket_timeout=15) as redis_client:
        if resume_first_drain:
            total_before = n['warm'] + n['before']
            if sum(stored['redis'].get(test.refined, {}).values()) != total_before:
                raise ValueError('Redis must have fully consumed the first half before resuming the drain')
            raw_before = read_topic(test.actions, test.bootstrap, timeout=240)
            if len(raw_before) != total_before:
                raise ValueError('Kafka must contain exactly the already published first half')
            pause_started = time.monotonic() - (time.time() - max(row[2] for row in raw_before) / 1000)
            # The direct reader is already caught up. Keep it stopped for restoration.
            boundary = wait_offsets(test, workers, phases, total_before, fallback=stored)
        else:
            test.publish('warm', 1_000_000)
            with test.workers('before', stages=('redis',)) as (reader, _):
                wait_offsets(test, {**workers, **reader}, phases, n['warm'])
                record('replaying_first_half')
                publish_segment(test, 'before', duration, {**workers, **reader})
                pause_started = time.monotonic()
                record('draining_first_half')
                boundary = wait_offsets(test, {**workers, **reader}, phases, n['warm'] + n['before'])

        record('redis_restore')
        previous = snapshot(redis_client, prefix)
        if not previous or redis_client.delete(*previous) != len(previous) or snapshot(redis_client, prefix):
            raise RuntimeError('Could not clear the isolated Redis test keys')
        restore_start = time.monotonic()
        receipt_path = f'/state/split/{test.run_id}/direct-day-recovery.json'
        restore_log = folder / 'restore.log'
        with restore_log.open('wb') as log:
            subprocess.run(COMPOSE + ['run', '--no-deps', '--rm', 'spark',
                           '--recover-redis', receipt_path, '--run-id', test.run_id,
                           '--refined-topic', test.refined, '--boundary', json.dumps(boundary),
                           '--redis-url', 'redis://redis:6379/0'], cwd=ROOT, env=ENV, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=1200)
        receipt, = markers(restore_log.read_text(encoding='utf-8', errors='replace'), 'REDIS_RECOVERY ')
        save(folder / 'recovery.json', receipt)
        if receipt['boundary'] != boundary or snapshot(redis_client, prefix) != previous:
            raise ValueError('Redis restored snapshot differs from the pre-loss state')
        report['recovery'] = {'keys_restored': len(previous), 'snapshot_matches': True,
                              'seconds': round(time.monotonic() - restore_start, 3),
                              'boundary': boundary, 'restart_offsets': receipt['restart_offsets']}

        phases['redis'] = 'after'
        with test.workers('after', stages=('redis',), stage_options={
                'redis': ('--redis-recovery', receipt_path)}) as (reader, _):
            await_reader(test, reader['redis'], receipt['restart_offsets'])
            report['recovery']['pause_to_resume_seconds'] = round(time.monotonic() - pause_started, 3)
            record('replaying_second_half')
            publish_segment(test, 'after', duration, {**workers, **reader})
            record('draining_second_half')
            report['final_refined_offsets'] = wait_offsets(test, {**workers, **reader}, phases, sum(n.values()))

    record('verifying')
    raw = read_topic(test.actions, test.bootstrap, timeout=240)
    refined = read_topic(test.refined, test.bootstrap, timeout=240)
    by_name = {name: set(pq.read_table(folder / f'{name}.parquet', columns=['event_id'])['event_id'].to_pylist())
               for name in n}
    expected = set.union(*by_name.values())
    if len(expected) != sum(n.values()) or Counter(row[3]['event_id'] for row in raw) != Counter(expected):
        raise ValueError('Kafka action IDs differ from Parquet event IDs')
    decisions = Counter(row[3]['status'] for row in refined)
    if decisions != {'accepted': len(expected)}:
        raise ValueError(f'Unexpected refined decisions: {decisions}')
    report['latency'] = latency(test, raw, by_name['before'] | by_name['after'], by_name['before'])
    report['sink_integrity'] = compare_sinks(test, inputs=tuple(n), timeout=900, raw=raw, refined=refined)
    report['complete'] = True
    record('complete')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-direct-day-check')
    parser.add_argument('--day', type=date.fromisoformat, default=date(2019, 11, 17))
    parser.add_argument('--pilot', action='store_true', help='Verify the orchestration over the known 556-event window')
    parser.add_argument('--resume-first-drain', type=Path, help='Continue a fully published first half from its checkpoints')
    args = parser.parse_args()
    run(args.source, args.output, args.day, pilot=args.pilot, resume_first_drain=args.resume_first_drain)
