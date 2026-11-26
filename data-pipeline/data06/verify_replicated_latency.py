"""Measure Redis write latency on an isolated three-broker Kafka deployment."""
import json
import os
import subprocess
from contextlib import closing
from pathlib import Path
from uuid import uuid4

from kafka.admin import KafkaAdminClient, NewTopic

from benchmark_refined import ROOT, Workload, event_topic_configs, read_window
from run_cluster_check import KAFKA, run_case


def main():
    folder = ROOT / 'data/data-replicated-latency' / f'replicated-{uuid4().hex[:8]}'
    folder.mkdir(parents=True)
    print(f'Reports: {folder}', flush=True)
    config = folder / 'compose.json'
    env = {**os.environ, 'DATA06_REDIS_PORT': '36593', 'DATA06_DECISION_STATE_STORE': 'spark'}
    source = Path(__file__).with_name('compose.cluster.yaml')
    local_spark = source.with_name('compose.cluster-local-spark.yaml')
    resolved = subprocess.check_output(
        ['docker', 'compose', '-p', folder.name, '--profile', 'driver', '-f', str(source), '-f', str(local_spark),
         'config', '--format', 'json'], cwd=ROOT, env=env, text=True, encoding='utf-8')
    stack = json.loads(resolved)
    stack['services']['redis']['healthcheck']['interval'] = '2s'
    stack['volumes']['state']['name'] = f'{folder.name}_state'
    brokers = [f'kafka-{n}' for n in range(1, 4)]
    ports = [39411, 39412, 39413]
    for name, port in zip(brokers, ports, strict=True):
        service = stack['services'][name]
        service['ports'][0]['published'] = str(port)
        service['environment']['KAFKA_ADVERTISED_LISTENERS'] = (
            f'INTERNAL://{name}:9092,HOST://127.0.0.1:{port}')
    config.write_text(json.dumps(stack), encoding='utf-8')
    compose = ['docker', 'compose', '-p', folder.name, '-f', str(config)]

    def cli(*args):
        return subprocess.check_output(compose + list(args), cwd=ROOT, env=env,
                                       text=True, encoding='utf-8', timeout=140)

    bootstrap = ','.join(f'127.0.0.1:{port}' for port in ports)
    try:
        if cache := os.getenv('DATA06_SPARK_CACHE_VOLUME'):
            state_volume = stack['volumes']['state']['name']
            subprocess.run(['docker', 'volume', 'create', state_volume], check=True, timeout=30)
            subprocess.run([
                'docker', 'run', '--rm', '--network', 'none',
                '--mount', f'type=volume,source={cache},target=/cache-source,readonly',
                '--mount', f'type=volume,source={state_volume},target=/state',
                '--entrypoint', '/bin/sh', stack['services']['spark']['image'],
                '-ec', 'test -d /cache-source/ivy && cp -a /cache-source/ivy /state/ivy',
            ], check=True, timeout=90)
        cli('up', '-d', '--wait', *brokers, 'redis')
        window, warm, size = read_window(ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet', folder)
        test = Workload(folder, '10x', '5 seconds', warm, size, compose=compose, environment=env,
                        bootstrap=bootstrap, replication=3, kafka=KAFKA)
        feedback = test.run_id + '-feedback'
        with closing(KafkaAdminClient(bootstrap_servers=bootstrap)) as admin:
            admin.create_topics([NewTopic(feedback, 2, 3, topic_configs=event_topic_configs(3))])
        settings = {}
        for topic in (test.actions, test.refined, feedback):
            command = ['exec', '-T', 'kafka-1']
            endpoint = ['--bootstrap-server', 'kafka-1:9092']
            cfg = cli(*command, '/opt/kafka/bin/kafka-configs.sh', *endpoint,
                      '--entity-type', 'topics', '--entity-name', topic, '--describe')
            desc = cli(*command, '/opt/kafka/bin/kafka-topics.sh', *endpoint, '--topic', topic, '--describe')
            if any(value not in cfg for value in ('message.timestamp.type=LogAppendTime', 'min.insync.replicas=2')):
                raise RuntimeError(cfg)
            if 'ReplicationFactor: 3' not in desc:
                raise RuntimeError(desc)
            settings[topic] = {'configs': cfg, 'partitions': desc}
        (folder / 'topic-settings.json').write_text(json.dumps(settings, indent=2), encoding='utf-8')
        print('Topic settings verified, starting 10x replay', flush=True)
        result = run_case(test, 'normal', standalone=False, wait_timeout=420)
        report = {'complete': True, 'window_utc': window, 'triggers': test.triggers, 'replication': 3,
                  'min_insync_replicas': 2, 'producer_acks': 'all', 'result': result}
        (folder / 'verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps({key: result[key] for key in ('redis_write_latency', 'sink_integrity')}), flush=True)
    finally:
        cli('stop', *brokers, 'redis')


if __name__ == '__main__':
    main()
