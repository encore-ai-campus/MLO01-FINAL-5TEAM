"""Exercise replay acknowledgements and application-level resend on the local cluster."""
import argparse
import json
import subprocess
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import redis
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from benchmark_refined import Workload, event_topic_configs, read_topic, wall_ms
from run_cluster_check import BOOTSTRAP, COMPOSE, ENV, KAFKA, ROOT, wait_for_workers
from verify_invalid_actions import run_stage
from verify_refined import compare_delta, compare_redis, markers
from reco.replay_parquet import events_in_order


def kafka_cli(tool, *args):
    return subprocess.run(COMPOSE + ['exec', '-T', 'kafka-1', f'/opt/kafka/bin/{tool}.sh',
                                   '--bootstrap-server', 'kafka-1:9092', *args],
                          cwd=ROOT, env=ENV, check=True, timeout=75, text=True, capture_output=True).stdout


def topic_config(topic, *args):
    return kafka_cli('kafka-configs', '--entity-type', 'topics', '--entity-name', topic, *args)


def check_topic(topic):
    configs = topic_config(topic, '--describe')
    partitions = kafka_cli('kafka-topics', '--topic', topic, '--describe')
    assert 'message.timestamp.type=LogAppendTime' in configs and 'min.insync.replicas=2' in configs, configs
    assert 'ReplicationFactor: 3' in partitions, partitions
    return configs + partitions


def verify(source, output):
    folder = output / f'producer-retry-{uuid4().hex[:8]}'
    folder.mkdir(parents=True)
    print(f'Reports: {folder}', flush=True)
    parquet = pq.ParquetFile(source)
    row = next(parquet.iter_batches(batch_size=1))
    pq.write_table(pa.Table.from_batches([row]), folder / 'event.parquet')
    event, = events_in_order(folder / 'event.parquet')
    override = folder / 'compose.override.json'
    override.write_text(json.dumps({'services': {'spark': {'volumes': [
        {'type': 'bind', 'source': Path(__file__).with_name(name).resolve().as_posix(),
         'target': f'/opt/data06/{name}', 'read_only': True}
        for name in ('event_schema.py', 'stream.py', 'refined.py', 'redis_kafka.py')
    ]}}}), encoding='utf-8')
    wait_for_workers()
    compose = COMPOSE + ['-f', str(override)]
    direct = compose + ['-f', str(Path(__file__).with_name('compose.redis-direct.yaml'))]
    test = Workload(folder, 'check', '2 seconds', 0, 2, compose=compose, environment=ENV, bootstrap=BOOTSTRAP,
                    kafka=KAFKA, replication=3, decision_publisher='driver', redis_direct=True, redis_compose=direct,
                    create_topics=False)
    feedback = f'{test.run_id}-feedback'
    with closing(KafkaAdminClient(bootstrap_servers=BOOTSTRAP)) as admin:
        admin.create_topics([NewTopic(feedback, 2, 3, topic_configs=event_topic_configs(3))])
    with closing(KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', enable_idempotence=True)) as producer:
        sent_at = wall_ms()
        producer.send(feedback, key=b'feedback-time-probe', value=b'{"probe":true}',
                      timestamp_ms=946684800000).get(timeout=30)
    (_, _, broker_ms, _), = read_topic(feedback, BOOTSTRAP)
    assert abs(broker_ms - sent_at) < 10000 and broker_ms != 946684800000

    replay_log = folder / f'{test.run_id}-event.log'
    print('Trying to send before the isolated action topic is available', flush=True)
    assert test.actions not in kafka_cli('kafka-topics', '--list').splitlines()
    try:
        test.publish('event', 10, timeout=110)
    except subprocess.CalledProcessError:
        failed = replay_log.read_text(encoding='utf-8')
        assert 'kafka_acknowledged' not in failed, failed
        assert any(error in failed for error in ('_MSG_TIMED_OUT', 'UNKNOWN_TOPIC_OR_PART')), failed
        replay_log.rename(folder / 'replay-failed.log')
    else:
        raise AssertionError('Replay reported success before the action topic existed')
    assert test.actions not in kafka_cli('kafka-topics', '--list').splitlines()
    with closing(KafkaAdminClient(bootstrap_servers=BOOTSTRAP)) as admin:
        admin.create_topics([NewTopic(name, 2, 3, topic_configs=event_topic_configs(3))
                             for name in (test.actions, test.refined)])
    assert not read_topic(test.actions, BOOTSTRAP)

    settings = ''.join(check_topic(topic) for topic in (test.actions, test.refined, feedback))
    (folder / 'kafka-topic-describe.log').write_text(settings, encoding='utf-8')
    print('Resending the same input row and then replaying its duplicate', flush=True)
    for attempt in ('retry', 'duplicate'):
        test.publish('event', 10)
        replay_lines = replay_log.read_text(encoding='utf-8').splitlines()
        assert {'stage': 'kafka_acknowledged', 'emitted': 1} in [
            json.loads(line) for line in replay_lines if line.startswith('{')]
        replay_log.rename(folder / f'replay-{attempt}.log')

    with test.workers('live') as (workers, _):
        test.wait(workers, 'live', 2)
    raw = {(p, o): message for p, o, _, message in read_topic(test.actions, BOOTSTRAP)}
    assert len(raw) == 2 and all(row['event_id'] == event['event_id'] for row in raw.values())
    assert len({json.dumps(row, sort_keys=True) for row in raw.values()}) == 1
    records = read_topic(test.refined, BOOTSTRAP)
    copies = {(message['origin_partition'], message['origin_offset']): {
        'origin_partition': message['origin_partition'], 'origin_offset': message['origin_offset'],
        'refined_partition': p, 'refined_offset': o, 'status': message['status'], 'event_id': message['event_id']}
        for p, o, _, message in records}
    assert len(records) == len(copies) == 2 and copies.keys() == raw.keys()
    result = run_stage(test, 'delta-inspect.log', 'inspect', '--kafka', KAFKA)
    decisions = compare_delta(result, raw, copies)
    assert decisions == {'accepted': 1, 'duplicate': 1}
    with redis.Redis(host='127.0.0.1', port=int(ENV['DATA06_REDIS_PORT']), decode_responses=True) as client:
        users = compare_redis(client, test.run_id, [event], copies)
    progress = (folder / f'{test.run_id}-live-redis.log').read_text(encoding='utf-8')
    assert sum(item['applied'] for item in markers(progress, 'DATA06_REDIS ')) == 1
    report = {'complete': True, 'event_id': event['event_id'], 'fault': 'action_topic_unavailable',
              'failed_attempt_acknowledged': False,
              'kafka_rows_after_failed_attempt': 0, 'kafka_rows_after_retry_and_duplicate': len(raw),
              'redis_applied_once': users, 'delta_rows_by_decision': dict(decisions), 'delta_raw_rows': len(raw),
              'kafka_replication_factor': 3, 'min_insync_replicas': 2,
              'feedback_log_append_time': True, 'feedback_producer_timestamp_ms': 946684800000,
              'feedback_broker_timestamp_ms': broker_ms}
    (folder / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-producer-retry-check')
    args = parser.parse_args()
    verify(args.source, args.output.resolve())
