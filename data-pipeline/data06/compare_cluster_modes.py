"""Replay identical local input with one Kafka or Spark deployment change at a time."""
import argparse
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from benchmark_refined import ROOT, Workload, read_window
from run_cluster_check import BOOTSTRAP, COMPOSE, ENV, KAFKA, SPARK_WORKERS, compose, run_case, wait_for_workers


BROKERS = ('kafka-1', 'kafka-2', 'kafka-3')
SPARK_NODES = ('spark-master', *SPARK_WORKERS)
LOCAL_SPARK = COMPOSE + ['-f', str(Path(__file__).with_name('compose.cluster-local-spark.yaml'))]
MODES = {'baseline': (False, False), 'kafka-only': (True, False),
         'spark-only': (False, True), 'both': (True, True), 'both-driver': (True, True),
         'both-driver-redis-local': (True, True), 'both-driver-redis-direct': (True, True)}


def run(source, output, modes):
    folder = output.resolve() / ('compare-' + uuid4().hex[:8])
    folder.mkdir(parents=True)
    print(f'Reports and logs: {folder}', flush=True)
    window, warm, size = read_window(source, folder)
    report = {'complete': False, 'window_utc': window, 'events': size, 'warm_events': warm, 'speed': 10,
              'default_triggers': {'decision': '500 milliseconds', 'redis': '500 milliseconds', 'delta': '30 seconds'},
              'sha256': {name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
                         for name in ('warm.parquet', 'workload.parquet')}, 'cases': {}}
    try:
        for mode in modes:
            kafka_cluster, standalone = MODES[mode]
            local_redis = mode == 'both-driver-redis-local'
            direct_redis = mode == 'both-driver-redis-direct'
            print(f'Running {mode}', flush=True)
            other = ('kafka-single',) if kafka_cluster else BROKERS
            compose('--profile', 'comparison', 'stop', *other, *(() if standalone else SPARK_NODES))
            compose('--profile', 'comparison', 'up', '-d', '--wait', '--wait-timeout', '120', 'redis',
                    *(BROKERS if kafka_cluster else ('kafka-single',)), *(SPARK_NODES if standalone else ()))
            if standalone:
                wait_for_workers()
            test = Workload(folder, mode, '30 seconds', warm, size, decision_trigger='500 milliseconds',
                            redis_trigger='250 milliseconds' if local_redis else '500 milliseconds',
                            compose=COMPOSE if standalone else LOCAL_SPARK,
                            bootstrap=BOOTSTRAP if kafka_cluster else '127.0.0.1:39290', environment=ENV,
                            replication=3 if kafka_cluster else 1, kafka=KAFKA if kafka_cluster else 'kafka-single:9092',
                            decision_publisher='driver' if mode.startswith('both-driver') else 'spark',
                            redis_batch_writes=local_redis, redis_direct=direct_redis,
                            redis_compose=LOCAL_SPARK if local_redis or direct_redis else None)
            row = run_case(test, 'normal', standalone=standalone, wait_timeout=420)
            row['decision_publisher'] = test.decision_publisher
            row['redis_mode'] = {'spark_master': None if direct_redis else (
                                 'local[2]' if local_redis or not standalone else 'standalone'),
                                 'batch_writes': local_redis or direct_redis,
                                 'trigger': None if direct_redis else test.triggers['redis'],
                                 'poll_timeout_ms': 50 if direct_redis else None}
            row['decision_profile'] = test.decision_profile('live')
            if standalone and not all(worker['coresused'] for worker in row['spark_workers_at_completion']):
                raise RuntimeError('Expected executors on both Spark workers')
            report['cases'][mode] = row
            report['complete'] = len(report['cases']) == len(modes)
            (folder / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
            print(f'Completed {mode}', flush=True)
    finally:
        compose('--profile', 'comparison', 'stop', 'kafka-single')
        compose('up', '-d', '--wait', '--wait-timeout', '120', 'redis', *BROKERS, *SPARK_NODES)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-cluster-compare')
    parser.add_argument('--modes', nargs='+', choices=MODES, default=['baseline', 'kafka-only', 'spark-only', 'both'])
    args = parser.parse_args()
    run(args.source, args.output, args.modes)
