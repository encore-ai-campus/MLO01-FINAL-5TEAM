"""Compare 10x local replay in Standalone Spark and a replicated Kafka test stack."""
import argparse
import json
import os
import subprocess
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

from kafka.admin import KafkaAdminClient

from benchmark_refined import ROOT, WORKERS, Workload, read_topic, read_window, wall_ms
from run_load_failure_check import compare_sinks
from verify_refined import markers


COMPOSE = ['docker', 'compose', '-p', 'mlo-data06-cluster', '-f', str(Path(__file__).with_name('compose.cluster.yaml'))]
ENV = {**os.environ, 'DATA06_REDIS_PORT': '36589'}
BOOTSTRAP = '127.0.0.1:39291,127.0.0.1:39292,127.0.0.1:39293'
KAFKA = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
SPARK_WORKERS = ('spark-worker-1', 'spark-worker-2')


def compose(*args, capture=False):
    return subprocess.run(COMPOSE + list(args), env=ENV, cwd=ROOT, check=True, timeout=150,
                          capture_output=capture, text=True).stdout


def partitions(topic):
    with closing(KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=8000)) as admin:
        return {p['partition']: {key: p[key] for key in ('leader', 'replicas', 'isr')}
                for p in admin.describe_topics([topic])[0]['partitions']}


def spark_workers():
    with urlopen('http://127.0.0.1:38089/json/', timeout=5) as response:
        return [row for row in json.load(response)['workers'] if row['state'] == 'ALIVE']


def wait_for_workers():
    deadline = time.monotonic() + 70
    while time.monotonic() < deadline:
        try:
            if len(spark_workers()) == 2:
                return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError('Spark workers did not both register')


def plan_fault(test, kind):
    if kind == 'broker':
        before = partitions(test.actions)
        if any(len(row['isr']) != 3 for row in before.values()):
            raise RuntimeError(f'Action replicas are not all in sync: {before}')
        target = f"kafka-{before[0]['leader']}"
        return target, before
    workers = spark_workers()
    if len(workers) != 2 or any(row['coresused'] < 1 for row in workers):
        raise RuntimeError(f'Both Spark workers must have assigned executors: {workers}')
    return SPARK_WORKERS[0], {row['id']: row['coresused'] for row in workers}


def run_case(test, kind, *, standalone=True, wait_timeout=180):
    test.publish('warm', 1_000_000)
    fault = None
    with test.workers('live') as (workers, _), ThreadPoolExecutor(max_workers=1) as pool:
        test.wait(workers, 'live', test.warm, timeout=wait_timeout)
        target = before = None
        if kind != 'normal':
            target, before = plan_fault(test, kind)
        publishing = pool.submit(test.publish, 'workload', 10)
        if target:
            try:
                time.sleep(6)
                if publishing.done():
                    raise RuntimeError('Publisher already finished before the outage')
                print(f'{kind}: stopping {target} during replay', flush=True)
                requested = wall_ms()
                compose('stop', '-t', '2', target)
                stopped = wall_ms()
                fault = {'service': target, 'requested_ms': requested, 'stopped_ms': stopped, 'before': before}
                if kind == 'broker':
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        try:
                            after = partitions(test.actions)
                            if after[0]['leader'] > 0 and after[0]['leader'] != before[0]['leader']:
                                break
                        except (OSError, RuntimeError):
                            pass
                        time.sleep(0.25)
                    else:
                        raise RuntimeError('Kafka did not elect a replacement action topic leader')
                    fault['leader_after'] = after
                    fault['leader_observed_ms'] = wall_ms()
                else:
                    time.sleep(8)
                    fault['restart_requested_ms'] = wall_ms()
                    compose('start', target)
                publishing.result()
                progress = test.wait(workers, 'live', test.warm + test.size, timeout=wait_timeout)
                if kind == 'broker':
                    fault['restart_requested_ms'] = wall_ms()
            finally:
                compose('start', target)
        else:
            publishing.result()
            progress = test.wait(workers, 'live', test.warm + test.size, timeout=wait_timeout)
        assigned = [{key: row[key] for key in ('id', 'host', 'coresused')}
                    for row in (spark_workers() if standalone else [])]
    result = test.summarize(10, progress)
    arrivals = sorted(row[2] for row in read_topic(test.actions, test.bootstrap))[test.warm:]
    result['redis_write_latency'] = test.redis_write_latency('live', arrivals[0])
    result['redis_profile'] = test.redis_profile('live', arrivals[0])
    result['spark_workers_at_completion'] = assigned
    if fault:
        if not arrivals[0] < fault['stopped_ms'] < arrivals[-1]:
            raise RuntimeError('Outage did not overlap the measured Kafka arrivals')
        fault['events_before_stop'] = sum(at < fault['stopped_ms'] for at in arrivals)
        fault['stop_to_catchup_seconds'] = {
            stage: round((result['stages'][stage]['last_commit_ms'] - fault['stopped_ms']) / 1000, 3)
            for stage in WORKERS}
        fault['restart_to_catchup_seconds'] = {
            stage: round((result['stages'][stage]['last_commit_ms'] - fault['restart_requested_ms']) / 1000, 3)
            for stage in WORKERS}
        result['fault'] = fault
    result['sink_integrity'] = compare_sinks(test)
    return result


def run_decision_restart(test):
    test.publish('warm', 1_000_000)
    failure = {'decision': ('--fail', 'decision-after-publish', '--fail-decision-after-batch', '1')}
    with test.workers('live', stages=('redis', 'delta')) as (sinks, _), ThreadPoolExecutor(max_workers=1) as pool:
        with test.workers('fault', stages=('decision',), stage_options=failure) as (decision, _):
            warm = test.wait({**sinks, **decision}, 'live', test.warm, timeout=420, decision_phase='fault')
            if warm['decision'][-1]['batch'] != 0:
                raise RuntimeError('The 20 warm-up events must commit in batch zero')
            publishing = pool.submit(test.publish, 'workload', 10)
            exit_code = decision['decision'].wait(timeout=240)
            exited_ms = wall_ms()
            log = (test.folder / f'{test.run_id}-fault-decision.log').read_text(encoding='utf-8', errors='replace')
            failed = markers(log, 'DATA06_DECISION_PROFILE ')[-1]
            if not exit_code or 'INJECTED_DECISION_AFTER_PUBLISH' not in log or failed['batch'] != 1:
                raise RuntimeError('Decision did not fail at the expected post-ack batch')
            if len(test.progress('decision', 'fault')) != 1:
                raise RuntimeError('The failed decision batch unexpectedly committed')

        positions = {tuple(position) for position in failed['positions']}
        total = test.warm + test.size
        with test.workers('resume', stages=('decision',)) as (resumed, restarted):
            publishing.result()
            progress = test.wait({**sinks, **resumed}, 'live', total, timeout=420,
                                 decision_phase='resume', refined_count=total + len(positions))

    refined = read_topic(test.refined, test.bootstrap)
    copies = Counter((message['origin_partition'], message['origin_offset']) for _, _, _, message in refined)
    if {position: count for position, count in copies.items() if count > 1} != dict.fromkeys(positions, 2):
        raise RuntimeError('Reprocessed Kafka positions differ from the failed batch')
    summary = test.summarize(10, progress, expected_refined_copies=len(positions))
    arrivals = sorted(row[2] for row in read_topic(test.actions, test.bootstrap))[test.warm:]
    summary['redis_write_latency'] = test.redis_write_latency('live', arrivals[0])
    summary['sink_integrity'] = compare_sinks(test)
    summary['fault'] = {'batch': failed['batch'], 'broker_acks_received_ms': failed['acked_ms'],
                        'process_exit_ms': exited_ms, 'restart_requested_ms': restarted,
                        'refined_republished_copies': len(positions),
                        'first_action_arrival_ms': arrivals[0], 'last_action_arrival_ms': arrivals[-1],
                        'restart_to_catchup_seconds': {worker: round(
                            (progress[worker][-1]['committed_ms'] - restarted) / 1000, 3)
                            for worker in WORKERS}}
    return summary


def run(source, output, cases):
    folder = output.resolve() / ('cluster-' + uuid4().hex[:8])
    folder.mkdir(parents=True)
    print(f'Reports and logs: {folder}', flush=True)
    compose('up', '-d', '--wait', '--wait-timeout', '120')
    wait_for_workers()
    window, warm, size = read_window(source, folder)
    report = {'complete': False, 'window_utc': window, 'events': size, 'warm_events': warm, 'speed': 10,
              'triggers': {'decision': '500 milliseconds', 'redis': '500 milliseconds', 'delta': '30 seconds'},
              'kafka_brokers': 3, 'kafka_replication': 3, 'spark_workers': 2, 'cases': {}}
    for kind in cases:
        print(f'Running {kind}', flush=True)
        test = Workload(folder, kind, '30 seconds', warm, size, decision_trigger='500 milliseconds',
                        redis_trigger='500 milliseconds', compose=COMPOSE, bootstrap=BOOTSTRAP,
                        environment=ENV, replication=3, kafka=KAFKA,
                        decision_publisher='driver' if kind == 'decision-driver' else 'spark')
        report['cases'][kind] = run_decision_restart(test) if kind == 'decision-driver' else run_case(test, kind)
        report['complete'] = len(report['cases']) == len(cases)
        (folder / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(f'Completed {kind}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-cluster-check')
    parser.add_argument('--cases', nargs='+', choices=('normal', 'broker', 'worker', 'decision-driver'),
                        default=['normal', 'broker', 'worker'])
    args = parser.parse_args()
    run(args.source, args.output, args.cases)
