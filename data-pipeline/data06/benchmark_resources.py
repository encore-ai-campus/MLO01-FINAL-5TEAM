"""Compare the local Spark topologies and read actual Delta MERGE commit metrics."""
import argparse
import json
import re
import statistics
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

from benchmark_refined import COMPOSE, ENVIRONMENT, ROOT, Workload, read_topic, read_window


UNITS = {'B': 1, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3, 'TiB': 1024**4}


def memory_bytes(value):
    number, unit = re.fullmatch(r'([0-9.]+)([A-Za-z]+)', value.split('/')[0].strip()).groups()
    return int(float(number) * UNITS[unit])


def docker(*args):
    return subprocess.check_output(['docker', *args], cwd=ROOT, text=True, encoding='utf-8', timeout=35)


def sample_resources(names, state, done):
    samples = []
    while not done.is_set():
        usage = [json.loads(line) for line in docker('stats', '--no-stream', '--format', '{{json .}}', *names).splitlines()]
        if len(usage) != len(names):
            raise RuntimeError('Missing Spark worker resource statistics')
        sizes = json.loads(docker('inspect', '--size', *names))
        disk = int(docker('exec', names[0], 'du', '-s', '-B1', state).split()[0])
        samples.append({
            'epoch_seconds': time.time(),
            'spark_memory_bytes': sum(memory_bytes(row['MemUsage']) for row in usage),
            'spark_writable_layer_bytes': sum(row['SizeRw'] for row in sizes),
            'spark_state_allocated_bytes': disk,
        })
        done.wait(3)
    return samples


def commit_history(run_id):
    state = f'/state/split/{run_id}'
    reader = '''
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
for table in ('raw', 'refined'):
    for log in sorted((root / table / '_delta_log').glob('*.json')):
        for line in log.read_text().splitlines():
            info = json.loads(line).get('commitInfo', {})
            if info.get('operation') != 'MERGE':
                continue
            m = info['operationMetrics']
            print(json.dumps({'table': table, 'version': int(log.stem), 'timestamp_ms': info['timestamp'],
                              'execution_ms': int(m['executionTimeMs']),
                              'inserted_rows': int(m['numTargetRowsInserted'])}))
'''
    output = docker('run', '--rm', '-v', 'mlo-data06-split_state:/state:ro', '--entrypoint', 'python3',
                    'mlo-data06-split-spark:latest', '-c', reader, state)
    return [json.loads(line) for line in output.splitlines()]


def measure(test, speed):
    state = f'/state/split/{test.run_id}'
    test.publish('warm', 1_000_000)
    with test.workers('live') as (processes, _):
        test.wait(processes, 'live', test.warm)
        names = [f'{test.run_id}-live-{stage}' for stage in processes]
        stop = Event()
        with ThreadPoolExecutor(max_workers=1) as pool:
            sampler = pool.submit(sample_resources, names, state, stop)
            try:
                test.publish('workload', speed)
                progress = test.wait(processes, 'live', test.warm + test.size)
            finally:
                stop.set()
            samples = sampler.result()

    result = test.summarize(speed, progress)
    result['topology'] = 'bundled' if test.bundled else 'separate'
    result['delta_trigger'] = test.trigger
    result['samples'] = samples
    result['peaks'] = {field: max(row[field] for row in samples) for field in
                       ('spark_memory_bytes', 'spark_writable_layer_bytes', 'spark_state_allocated_bytes')}
    result['peaks']['spark_total_disk_bytes'] = max(
        row['spark_writable_layer_bytes'] + row['spark_state_allocated_bytes'] for row in samples)
    commits = commit_history(test.run_id)
    for table in ('raw', 'refined'):
        inserted = sum(row['inserted_rows'] for row in commits if row['table'] == table)
        if inserted != test.size + test.warm:
            raise ValueError(f'{table} MERGE history has {inserted} inserted rows')
    workload_ids = set(read_workload_ids(test.folder))
    start = min(row[2] for row in read_topic(test.actions) if row[3]['event_id'] in workload_ids)
    result['redis_write_latency'] = test.redis_write_latency('live', start)
    result['delta_merge_history'] = commits
    result['delta_measured_commits'] = {}
    for table in ('raw', 'refined'):
        values = [row['execution_ms'] for row in commits if row['table'] == table and row['timestamp_ms'] >= start]
        if not values:
            raise ValueError(f'No measured Delta commit for {table}')
        result['delta_measured_commits'][table] = {
            'count': len(values), 'median_execution_ms': statistics.median(values),
            'min_execution_ms': min(values), 'max_execution_ms': max(values),
        }
    return result


def read_workload_ids(folder):
    import pyarrow.parquet as pq

    return pq.read_table(folder / 'workload.parquet', columns=['event_id'])['event_id'].to_pylist()


def run(source, output, speed):
    folder = output.resolve() / ('split08-' + uuid.uuid4().hex[:8])
    folder.mkdir(parents=True)
    print(f'Logs: {folder}', flush=True)
    subprocess.run(COMPOSE + ['up', '-d', '--wait', 'kafka', 'redis'], env=ENVIRONMENT, check=True, timeout=100)
    window, warm, size = read_window(source, folder)
    report = {'complete': False, 'source': source.name, 'window_utc': window, 'warm_events': warm,
              'events_per_run': size, 'speed': speed, 'runs': [],
              'resource_scope': 'Spark container memory working set, writable layers and replay state volume. '
                                'Kafka and Redis servers, base images and pre-existing Ivy cache are excluded.',
              'commit_scope': 'Delta transaction log MERGE executionTimeMs, local filesystem, excluding warmup.'}
    target = folder / 'verification.json'
    for name, trigger, bundled in [('split30', '30 seconds', False),
                                    ('group30', '30 seconds', True), ('group60', '60 seconds', True)]:
        print(f'Running {name}', flush=True)
        result = measure(Workload(folder, name, trigger, warm, size, bundled), speed)
        report['runs'].append(result)
        target.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(json.dumps({key: result[key] for key in ('peaks', 'delta_measured_commits')}), flush=True)
    report['complete'] = True
    target.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f'Complete: {target}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-08-spark')
    parser.add_argument('--speed', type=int, default=2)
    args = parser.parse_args()
    if args.speed < 1:
        parser.error('Speed must be positive')
    run(args.source, args.output, args.speed)
