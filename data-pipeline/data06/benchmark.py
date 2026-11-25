"""Data-07 local Kafka ingress, Spark throughput and Redis latency benchmark."""
import argparse
from bisect import bisect_right
from datetime import datetime
import json
import math
from pathlib import Path
import subprocess
import sys
from time import monotonic, sleep

import pyarrow.parquet as pq

from verify import BOOTSTRAP, COMPOSE, ROOT


def percentile(values, p):
    return values[math.ceil(p * len(values)) - 1]


def summarize(speed, batches, warm, expected):
    points = [point for batch in batches for point in batch['points'] if point[0] >= warm]
    if sorted(p[0] for p in points) != list(range(warm, warm + expected)):
        raise ValueError('Spark metrics do not cover the expected Kafka offsets exactly once')
    arrived = sorted(p[1] for p in points)
    applied = sorted(p[2] for p in points)
    latency = sorted(p[2] - p[1] for p in points)
    if latency[0] < 0:
        raise ValueError('Kafka and Redis timing clocks are not aligned')
    first, last = arrived[0], arrived[-1]
    half = (first + last) / 2

    def backlog(at):
        return bisect_right(arrived, at) - bisect_right(applied, at)

    changes = sorted(set(arrived) | set(applied))
    middle, end = backlog(half), backlog(last)
    elapsed = sum(batch['elapsed_seconds'] for batch in batches
                  if any(point[0] >= warm for point in batch['points']))
    return {
        'speed': speed, 'events': expected,
        'kafka_input_eps': round(expected * 1000 / max(1, last - first), 2),
        'kafka_peak_one_second_events': max(
            i + 1 - bisect_right(arrived, at - 1000, 0, i + 1) for i, at in enumerate(arrived)),
        'spark_active_eps': round(expected / elapsed, 2),
        'end_to_end_eps': round(expected * 1000 / max(1, applied[-1] - first), 2),
        'pending_midpoint': middle, 'pending_at_end_of_send': end,
        'pending_peak': max(map(backlog, changes)), 'pending_after_drain': backlog(applied[-1]),
        'lag_grew': end - middle >= math.ceil(expected * 0.10) and end >= math.ceil(expected * 0.20),
        'drain_after_send_seconds': round((applied[-1] - last) / 1000, 3),
        'redis_latency_ms': {'p50': percentile(latency, .5), 'p95': percentile(latency, .95),
                             'p99': percentile(latency, .99), 'max': latency[-1]},
    }


def replay(path, topic, speed):
    command = [sys.executable, '-m', 'reco.replay_parquet', str(path), '--speed', str(speed),
               '--kafka-bootstrap', BOOTSTRAP, '--kafka-topic', topic, '--kafka-in-flight', '256']
    subprocess.run(command, cwd=ROOT, check=True, timeout=180)


def measure(speed, warm_file, workload_file, warm, size, result_folder):
    topic = f'{result_folder.name}-{speed}x'
    run = COMPOSE + ['exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
                     '--bootstrap-server', 'kafka:9092', '--create', '--topic', topic,
                     '--partitions', '1', '--replication-factor', '1',
                     '--config', 'message.timestamp.type=LogAppendTime']
    subprocess.run(run, cwd=ROOT, check=True, timeout=45)
    replay(warm_file, topic, 1_000_000)
    log = result_folder / f'{speed}x-spark.log'
    spark = COMPOSE + ['run', '--rm', 'spark', '--run-id', topic, '--topic', topic,
                       '--through-offset', str(warm + size)]
    with log.open('wb') as log_stream:
        process = subprocess.Popen(spark, cwd=ROOT, stdout=log_stream, stderr=subprocess.STDOUT)
        try:
            deadline = monotonic() + 100
            while 'DATA07_BATCH ' not in log.read_text(encoding='utf-8', errors='replace'):
                if process.poll() is not None or monotonic() >= deadline:
                    raise RuntimeError(f'Spark did not finish warm-up. See {log}')
                sleep(0.5)
            replay(workload_file, topic, speed)
            if process.wait(timeout=150):
                raise RuntimeError(f'Spark benchmark failed. See {log}')
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=15)
    batches = [json.loads(line.removeprefix('DATA07_BATCH ')) for line in
               log.read_text(encoding='utf-8', errors='replace').splitlines() if line.startswith('DATA07_BATCH ')]
    return {'topic': topic, **summarize(speed, batches, warm, size)}


def benchmark(source, folder, speeds):
    folder.mkdir(parents=True)
    start = datetime.fromisoformat('2019-11-17T12:37:00+00:00')
    end = datetime.fromisoformat('2019-11-17T12:40:00+00:00')
    warm_start = datetime.fromisoformat('2019-11-17T12:36:00+00:00')
    events = pq.read_table(source, filters=[('event_time', '>=', warm_start), ('event_time', '<', end)])
    import pyarrow.compute as pc

    warm_events = events.filter(pc.less(events['event_time'], start)).slice(0, 20)
    workload = events.filter(pc.greater_equal(events['event_time'], start))
    if warm_events.num_rows != 20 or not workload.num_rows:
        raise ValueError('The frozen November workload window is missing rows')
    warm_file, work_file = folder / 'warm.parquet', folder / 'workload.parquet'
    pq.write_table(warm_events, warm_file)
    pq.write_table(workload, work_file)
    report = {'complete': False, 'window_utc': [start.isoformat(), end.isoformat()],
              'source': source.name, 'warm_events_excluded': warm_events.num_rows,
              'lag_criterion': 'pending at send end >=20% and increase since midpoint >=10% of input',
              'redis_latency_definition': 'Kafka LogAppendTime to completion of the per-user Redis SET',
              'runs': []}
    for speed in speeds:
        print(f'Testing {speed}x', flush=True)
        result = measure(speed, warm_file, work_file, warm_events.num_rows, workload.num_rows, folder)
        report['runs'].append(result)
        (folder/'verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(result), flush=True)
    report['lowest_tested_speed_with_backlog_growth'] = next(
        (r['speed'] for r in report['runs'] if r['lag_grew']), None)
    report['complete'] = True
    (folder/'verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT/'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--speeds', nargs='+', type=int, default=[6, 12, 24, 96])
    opts = parser.parse_args()
    benchmark(opts.source, opts.output.resolve(), sorted(set(opts.speeds)))
