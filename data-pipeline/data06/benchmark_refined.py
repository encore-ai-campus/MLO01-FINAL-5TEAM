"""Measure independent Spark consumer lag during timed November replays."""
import argparse
import json
import math
import os
import subprocess
import sys
import time
import uuid
from bisect import bisect_right
from collections import Counter
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from verify import ROOT
from verify_refined import markers, percentiles


WORKERS = ('decision', 'redis', 'delta')
ENVIRONMENT = {**os.environ, 'DATA06_KAFKA_PORT': os.getenv('DATA06_KAFKA_PORT', '39192'),
               'DATA06_REDIS_PORT': os.getenv('DATA06_REDIS_PORT', '36479')}
BOOTSTRAP = '127.0.0.1:' + ENVIRONMENT['DATA06_KAFKA_PORT']
COMPOSE = ['docker', 'compose', '-p', os.getenv('DATA06_BENCHMARK_PROJECT', 'mlo-data06-split'),
           '-f', str(Path(__file__).with_name('compose.yaml'))]
PROGRESS = 'DATA07_SPLIT '


def event_topic_configs(replication):
    return {'message.timestamp.type': 'LogAppendTime', 'min.insync.replicas': str(min(replication, 2))}


def wall_ms():
    return int(time.time() * 1000)


def read_window(source, folder):
    start, end = map(datetime.fromisoformat, ('2019-11-17T12:37:00+00:00', '2019-11-17T12:40:00+00:00'))
    warm_start = datetime.fromisoformat('2019-11-17T12:36:00+00:00')
    events = pq.read_table(source, filters=[('event_time', '>=', warm_start), ('event_time', '<', end)])
    warm = events.filter(pc.less(events['event_time'], start)).slice(0, 20)
    measured = events.filter(pc.greater_equal(events['event_time'], start))
    if warm.num_rows != 20 or measured.num_rows == 0:
        raise ValueError('Incomplete November benchmark window')
    for name, table in [('warm', warm), ('workload', measured)]:
        pq.write_table(table, folder / f'{name}.parquet')
    return [start.isoformat(), end.isoformat()], warm.num_rows, measured.num_rows


def read_topic(topic, bootstrap=BOOTSTRAP, *, timeout=25):
    partitions = [TopicPartition(topic, index) for index in range(2)]
    with closing(KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)) as client:
        client.assign(partitions)
        ends = client.end_offsets(partitions)
        remaining = sum(ends.values())
        for partition in partitions:
            client.seek(partition, 0)
        rows = []
        deadline = time.monotonic() + timeout
        while remaining:
            if time.monotonic() > deadline:
                raise TimeoutError(f'Kafka records missing from {topic}')
            for partition, records in client.poll(timeout_ms=500).items():
                for record in records:
                    if record.offset < ends[partition]:
                        rows.append((record.partition, record.offset, record.timestamp, json.loads(record.value)))
                        remaining -= 1
        return rows


def offsets(batch):
    value = batch['end_offsets']
    return json.loads(value) if isinstance(value, str) else value


def committed_at(progress, topic, partition, offset):
    for batch in progress:
        if offsets(batch).get(topic, {}).get(str(partition), 0) > offset:
            return batch['committed_ms']
    raise ValueError(f'{topic}:{partition}:{offset} has no committed offset')


def summarize_stage(arrivals, completions):
    arrivals, completions = sorted(arrivals), sorted(completions)
    total = len(arrivals)
    first, last = arrivals[0], arrivals[-1]
    if any(done < sent for sent, done in zip(arrivals, completions)):
        raise ValueError('Completion precedes input. Check Docker and Kafka clocks.')

    def pending(at):
        return bisect_right(arrivals, at) - bisect_right(completions, at)

    middle = pending((first + last) / 2)
    end = pending(last)
    return {
        'effective_eps': round(total * 1000 / max(1, completions[-1] - first), 2),
        'pending_midpoint': middle, 'pending_at_send_end': end,
        'pending_after_drain': pending(completions[-1]),
        'drain_after_send_seconds': round(max(0, completions[-1] - last) / 1000, 3),
        'backlog_growth': end >= math.ceil(total * .20) and end - middle >= math.ceil(total * .10),
        'last_commit_ms': completions[-1],
    }


class Workload:
    def __init__(self, folder, name, trigger, warm, size, bundled=False, *,
                 decision_trigger='1 second', redis_trigger='1 second', compose=COMPOSE,
                 bootstrap=BOOTSTRAP, environment=ENVIRONMENT, replication=1, kafka='kafka:9092',
                 decision_publisher='spark', redis_batch_writes=False, redis_compose=None, redis_direct=False,
                 create_topics=True):
        self.run_id = f'{folder.name}-{name}'
        self.actions = self.run_id + '-actions'
        self.refined = self.run_id + '-refined'
        self.folder = folder
        self.trigger = trigger
        self.triggers = {'decision': decision_trigger, 'redis': redis_trigger, 'delta': trigger, 'bundled': trigger}
        self.warm = warm
        self.size = size
        self.bundled = bundled
        self.decision_publisher = decision_publisher
        self.redis_batch_writes = redis_batch_writes
        self.redis_compose = redis_compose
        self.redis_direct = redis_direct
        self.compose, self.bootstrap, self.environment, self.kafka = compose, bootstrap, environment, kafka
        if create_topics:
            with closing(KafkaAdminClient(bootstrap_servers=bootstrap)) as admin:
                admin.create_topics([NewTopic(name, 2, replication, topic_configs=event_topic_configs(replication))
                                     for name in (self.actions, self.refined)])

    def publish(self, file, speed, *, timeout=300, supervise=None):
        path = self.folder / (file + '.parquet')
        with (self.folder / f'{self.run_id}-{file}.log').open('wb') as log:
            command = [sys.executable, '-m', 'reco.replay_parquet', str(path), '--speed', str(speed),
                            '--kafka-bootstrap', self.bootstrap, '--kafka-topic', self.actions,
                            '--kafka-user-key', '--kafka-in-flight', '256']
            with subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT) as publisher:
                deadline = time.monotonic() + timeout
                try:
                    while publisher.poll() is None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise subprocess.TimeoutExpired(command, timeout)
                        if supervise:
                            supervise()
                        try:
                            publisher.wait(min(5, remaining))
                        except subprocess.TimeoutExpired:
                            continue
                    if publisher.returncode:
                        raise subprocess.CalledProcessError(publisher.returncode, command)
                finally:
                    if publisher.poll() is None:
                        publisher.kill()

    def progress(self, worker, phase):
        name = 'bundled' if self.bundled else worker
        path = self.folder / f'{self.run_id}-{phase}-{name}.log'
        # Docker writes the log concurrently; parse only complete lines while polling.
        complete, _, _ = path.read_text(encoding='utf-8', errors='replace').rpartition('\n')
        batches = markers(complete, PROGRESS)
        return [batch for batch in batches if batch.get('stage', name) == worker]

    @contextmanager
    def workers(self, phase, *, stages=WORKERS, stage_options=None):
        processes, logs, names = {}, [], []
        start_ms = wall_ms()
        try:
            for worker in ('bundled',) if self.bundled else stages:
                name = f'{self.run_id}-{phase}-{worker}'
                compose = (self.redis_compose or self.compose) if worker == 'redis' else self.compose
                command = compose + ['run', '--no-deps', '--rm', '--name', name,
                                     '-e', f'SPARK_LOCAL_HOSTNAME={name}', 'spark', '--kafka', self.kafka,
                                     '--stage', worker, '--run-id', self.run_id, '--topic', self.actions,
                                     '--refined-topic', self.refined, '--continuous', '--benchmark-progress']
                command.extend(['--trigger', self.triggers[worker]])
                if worker in ('decision', 'bundled'):
                    command.extend(['--decision-publisher', self.decision_publisher])
                if worker in ('redis', 'bundled') and self.redis_batch_writes:
                    command.append('--redis-batch-writes')
                if worker == 'redis' and self.redis_direct:
                    command.append('--redis-direct')
                command.extend((stage_options or {}).get(worker, ()))
                log = (self.folder / f'{name}.log').open('wb')
                logs.append(log)
                processes[worker] = subprocess.Popen(command, cwd=ROOT, env=self.environment, stdout=log,
                                                     stderr=subprocess.STDOUT)
                names.append(name)
            yield processes, start_ms
        finally:
            if names:
                subprocess.run(['docker', 'stop', '--time', '2', *names], cwd=ROOT, capture_output=True, timeout=25)
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=10)
            for log in logs:
                log.close()

    def wait(self, processes, phase, count, *, timeout=180, decision_phase=None, refined_count=None):
        expected = {'decision': (self.actions,), 'redis': (self.refined,), 'delta': (self.actions, self.refined)}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for worker, process in processes.items():
                if process.poll() is not None:
                    raise RuntimeError(f'{worker} exited with {process.returncode}, see {self.run_id}-{phase}-{worker}.log')
            progress = {worker: self.progress(worker, decision_phase if worker == 'decision' and decision_phase else phase)
                        for worker in WORKERS}
            if all(progress[worker] and all(
                    sum(offsets(progress[worker][-1]).get(topic, {}).values()) >= (
                        refined_count if topic == self.refined and refined_count is not None else count)
                    for topic in expected[worker])
                   for worker in WORKERS):
                return progress
            time.sleep(.4)
        raise TimeoutError(f'{self.run_id} {phase} failed to reach {count} records. See worker logs.')

    def summarize(self, speed, progress, restart_ms=None, *, expected_refined_copies=0):
        raw, refined = read_topic(self.actions, self.bootstrap), read_topic(self.refined, self.bootstrap)
        origin = {}
        for p, o, at, msg in refined:
            origin.setdefault((msg['origin_partition'], msg['origin_offset']), (p, o, at, msg))
        assert len(raw) == len(origin) == self.warm + self.size
        assert len(refined) == len(origin) + expected_refined_copies
        assert set(origin) == {(p, o) for p, o, _, _ in raw}
        workload_ids = set(pq.read_table(self.folder / 'workload.parquet', columns=['event_id'])['event_id'].to_pylist())
        measured = [(p, o, at, msg) for p, o, at, msg in raw if msg['event_id'] in workload_ids]
        assert len(measured) == len(workload_ids) == self.size
        outcomes = Counter(msg['status'] for _, _, _, msg in origin.values())
        assert outcomes == {'accepted': len(raw)}, outcomes
        arrivals = sorted(at for _, _, at, _ in measured)
        points = {worker: [] for worker in WORKERS}
        refined_arrivals = []
        refined_delta_commits = []
        for p, o, _, _ in measured:
            refined_p, refined_o, refined_at, _ = origin[p, o]
            refined_arrivals.append(refined_at)
            points['decision'].append(committed_at(progress['decision'], self.actions, p, o))
            points['redis'].append(committed_at(progress['redis'], self.refined, refined_p, refined_o))
            refined_delta_commits.append(committed_at(progress['delta'], self.refined, refined_p, refined_o))
            points['delta'].append(max(refined_delta_commits[-1],
                                       committed_at(progress['delta'], self.actions, p, o)))
        summary = {
            'run_id': self.run_id, 'speed': speed, 'events': self.size, 'accepted_with_warmup': outcomes['accepted'],
            'ingress_eps': round(max(1, self.size - 1) * 1000 / max(1, arrivals[-1] - arrivals[0]), 2),
            'peak_one_second_ingress': max(i + 1 - bisect_right(arrivals, at - 1000, 0, i + 1)
                                          for i, at in enumerate(arrivals)),
            'send_seconds': round((arrivals[-1] - arrivals[0]) / 1000, 3),
            'stages': {worker: summarize_stage(arrivals, points[worker]) for worker in WORKERS},
            'refined_consumer_queues': {
                'redis': summarize_stage(refined_arrivals, points['redis']),
                'delta': summarize_stage(refined_arrivals, refined_delta_commits),
            },
            'kafka_to_stage_commit_latency': {
                worker: percentiles(list(zip((row[2] for row in measured), points[worker], strict=True)), 0, 1)
                for worker in WORKERS
            },
        }
        if restart_ms is not None:
            summary['queue_at_restart'] = self.size
            summary['restart_to_catchup_seconds'] = {
                worker: round((max(points[worker]) - restart_ms) / 1000, 3) for worker in WORKERS}
        return summary

    def redis_write_latency(self, phase, first_ms):
        worker = 'bundled' if self.bundled else 'redis'
        log = (self.folder / f'{self.run_id}-{phase}-{worker}.log').read_text(encoding='utf-8', errors='replace')
        points = [row for row in markers(log, 'DATA06_LATENCY ') if row[0] >= first_ms]
        if len(points) != self.size:
            raise ValueError(f'Expected {self.size} Redis writes, found {len(points)}')
        return {
            'action_kafka_to_decision': percentiles(points, 0, 1),
            'decision_to_refined_kafka': percentiles(points, 1, 2),
            'refined_kafka_to_redis_write': percentiles(points, 2, 3),
            'action_kafka_to_redis_write': percentiles(points, 0, 3),
        }

    def decision_profile(self, phase):
        name = 'bundled' if self.bundled else 'decision'
        path = self.folder / f'{self.run_id}-{phase}-{name}.log'
        profiles = markers(path.read_text(encoding='utf-8', errors='replace'), 'DATA06_DECISION_PROFILE ')
        progress = {row['batch']: row for row in self.progress('decision', phase)}
        workload_ids = set(pq.read_table(self.folder / 'workload.parquet', columns=['event_id'])['event_id'].to_pylist())
        arrivals = {(p, o): at for p, o, at, msg in read_topic(self.actions, self.bootstrap)
                    if msg['event_id'] in workload_ids}
        values = []
        for profile in profiles:
            batch_arrivals = [arrivals.pop(tuple(pos)) for pos in profile['positions'] if tuple(pos) in arrivals]
            if not batch_arrivals:
                continue
            batch = progress[profile['batch']]
            trigger = batch['committed_ms'] - batch['trigger_ms']
            for arrival in batch_arrivals:
                values.append((arrival, trigger, profile['entered_ms'], profile['read_start_ms'],
                               profile['read_end_ms'], profile['publish_start_ms'], profile['acked_ms']))
        if arrivals or len(values) != self.size:
            raise ValueError(f'Decision profiling did not cover every workload event: {len(values)}/{self.size}')
        return {'event_count': len(values), **{
            label: percentiles(values, left, right) for label, left, right in (
                ('kafka_to_trigger_start', 0, 1), ('trigger_start_to_callback', 1, 2),
                ('sqlite_open_and_read_watermark', 2, 3), ('spark_read_parse_collect_and_sort', 3, 4),
                ('classify_and_encode_batch', 4, 5), ('publish_through_ack', 5, 6))}}

    def redis_profile(self, phase, first_ms):
        worker = 'bundled' if self.bundled else 'redis'
        path = self.folder / f'{self.run_id}-{phase}-{worker}.log'
        profiles = markers(path.read_text(encoding='utf-8', errors='replace'), 'DATA06_REDIS_PROFILE ')
        progress = {row['batch']: row for row in self.progress('redis', phase)}
        values = []
        for profile in profiles:
            for point in profile['points']:
                if point[0] >= first_ms:
                    batch = progress[profile['batch']]
                    values.append([point[0], batch['committed_ms'] - batch['trigger_ms'], *point[1:]])
        if len(values) != self.size:
            raise ValueError(f'Redis profiling did not cover all workload events: {len(values)}/{self.size}')
        return {'event_count': len(values), **{
            label: percentiles(values, left, right) for label, left, right in (
                ('refined_kafka_to_trigger_start', 0, 1), ('trigger_start_to_callback', 1, 2),
                ('spark_read_parse_collect_and_sort', 2, 3), ('redis_lookup_including_batch_queue', 3, 4),
                ('state_compute', 4, 5), ('write_and_ack', 5, 6))}}


def benchmark(options):
    target_eps = None
    if options.target_config:
        target_eps = json.loads(options.target_config.read_text(encoding='utf-8-sig'))['T-THR-01']
        if type(target_eps) not in (int, float) or not math.isfinite(target_eps) or target_eps <= 0:
            raise ValueError('T-THR-01 must be a positive finite number of events per second')
    folder = options.output.resolve() / ('split07-' + uuid.uuid4().hex[:8])
    folder.mkdir(parents=True)
    subprocess.run(COMPOSE + ['up', '-d', '--wait', 'kafka', 'redis'], env=ENVIRONMENT, check=True, timeout=100)
    window, warm, size = read_window(options.source, folder)
    report = {'complete': False, 'source': options.source.name, 'window_utc': window, 'warm_events': warm,
              'delta_trigger': options.delta_trigger, 'decision_trigger': '1 second', 'redis_trigger': '1 second',
              'lag_rule': 'end pending >= 20% of run and end-minus-midpoint >= 10% of run',
              'throughput_rule': 'committed workload rows / elapsed from first Kafka arrival to final stage commit',
              'completion_clock': 'Spark progress batch start plus triggerExecution, after sink and checkpoint',
              'runs': []}
    target = folder / 'verification.json'

    def record():
        target.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')

    for speed in sorted(set(options.speeds)):
        print(f'Benchmark {speed}x; logs: {folder}', flush=True)
        test = Workload(folder, f'{speed}x', options.delta_trigger, warm, size)
        test.publish('warm', 1_000_000)
        with test.workers('live') as (workers, _):
            test.wait(workers, 'live', warm)
            test.publish('workload', speed)
            progress = test.wait(workers, 'live', warm + size)
        row = test.summarize(speed, progress)
        if target_eps is not None:
            report['throughput_target_eps'] = target_eps
            row['stage_effective_eps_minus_target'] = {
                stage: round(result['effective_eps'] - target_eps, 2)
                for stage, result in row['stages'].items()
            }
        report['runs'].append(row)
        record()
        print(json.dumps(row), flush=True)

    print('Benchmark restart and catch-up', flush=True)
    test = Workload(folder, 'restart', options.delta_trigger, warm, size)
    test.publish('warm', 1_000_000)
    with test.workers('before') as (workers, _):
        test.wait(workers, 'before', warm)
    test.publish('workload', options.restart_speed)
    with test.workers('after') as (workers, start):
        progress = test.wait(workers, 'after', warm + size)
    report['restart'] = test.summarize(options.restart_speed, progress, start)
    report['first_tested_growth_speed'] = {
        worker: next((row['speed'] for row in report['runs'] if row['stages'][worker]['backlog_growth']), None)
        for worker in WORKERS}
    report['complete'] = True
    record()
    print(json.dumps(report['restart']), flush=True)
    print(f'Complete: {target}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-07-refined')
    parser.add_argument('--speeds', type=int, nargs='+', default=[2, 6, 12, 24])
    parser.add_argument('--restart-speed', type=int, default=24)
    parser.add_argument('--delta-trigger', default='5 seconds')
    parser.add_argument('--target-config', type=Path, help='JSON export of the budget target, {"T-THR-01": events_per_second}')
    args = parser.parse_args()
    if min(*args.speeds, args.restart_speed) < 1:
        parser.error('Replay speeds must be positive')
    benchmark(args)
