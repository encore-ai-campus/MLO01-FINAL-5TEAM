"""Local 19-2 integration test with the Spark decision and Kafka Redis workers."""
import json
import os
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import redis
from confluent_kafka.admin import NewTopic

from tool_kafka import acknowledged_producer, create_topics, read_snapshot, send_record


CODE = Path(__file__).resolve().parent
ROOT = CODE.parent
OUTPUT = ROOT / 'data/recent-actions-19-2'
BOOTSTRAP = '127.0.0.1:39440'
COMPOSE = ['docker', 'compose', '-p', 'mlo-recent-actions-19-2',
           '-f', str(CODE / 'compose.yaml'), '-f', str(OUTPUT / 'network.json')]
DOCKER = COMPOSE + ['run', '--no-deps', '--rm', '-v', f'{CODE.as_posix()}:/code:ro']


def sample_inputs(actions):
    """Test double for the separate ML 6-feature package, never used in serving."""
    return {'action_count': len(actions), 'last_seq': actions[-1]['seq'],
            'last_sequence': actions[-1]['sequence'], 'last_product': actions[-1]['product_id'],
            'last_event': actions[-1]['event_id'], 'last_kind': actions[-1]['event_type']}


def read_topic(topic):
    return [json.loads(message.value()) for message in read_snapshot(BOOTSTRAP, topic, timeout=15)]


@contextmanager
def worker(output, name, options):
    script = ('for p in /opt/spark/python/lib/py4j-*-src.zip; do '
              'export PYTHONPATH="/code:/opt/data06-python:/opt/spark/python/lib/pyspark.zip:$p"; done; '
              'exec python3 -u /code/stream.py "$@"')
    log_path = output / f'{name}.log'
    with log_path.open('wb') as log:
        process = subprocess.Popen(DOCKER + ['--name', name, '--entrypoint', '/bin/sh', 'spark',
                                    '-ec', script, 'recent-actions', *options],
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            yield process, log_path
        finally:
            if process.poll() is None:
                subprocess.run(['docker', 'stop', '-t', '3', name], capture_output=True, timeout=15)
                process.wait(timeout=20)


def wait_for(check, process, log, timeout=50):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if check():
            return
        if process.poll() is not None:
            raise RuntimeError(log.read_text(encoding='utf-8', errors='replace')[-5000:])
        sleep(0.2)
    raise TimeoutError(f'Redis worker did not apply the expected position. See {log}')


def verify():
    run_id = 'actions19-' + uuid4().hex[:8]
    output = OUTPUT / run_id
    output.mkdir()
    print(output, flush=True)
    action_topic, refined, prediction = (run_id + suffix for suffix in ('-actions', '-refined', '-conversion'))
    topic = lambda name: NewTopic(name, 1, 1, config={'message.timestamp.type': 'LogAppendTime'})
    create_topics(BOOTSTRAP, [topic(action_topic), topic(refined)])
    start = datetime(2019, 11, 17, tzinfo=timezone.utc)
    def event(seq, user, seconds, kind='view', product=55):
        return {'event_id': f'{run_id}.csv.gz:{seq}', 'source': f'{run_id}.csv.gz', 'event_seq': seq,
                'user_id': user, 'event_time': (start + timedelta(seconds=seconds, milliseconds=123)).isoformat(),
                'event_type': kind, 'product_id': product}
    main = [event(i, 71, i, 'cart' if i == 3 else 'view') for i in range(1, 8)]
    main += [event(8, 71, 8, 'purchase'), event(9, 71, 9)]
    edge = [event(11, 72, 10), event(12, 72, 1810), event(13, 72, 1809), event(14, 72, 3611)]
    inputs = [*main, edge[0], edge[1], edge[1], *edge[2:]]
    with acknowledged_producer(BOOTSTRAP) as producer:
        for row in inputs:
            send_record(producer, action_topic, key=str(row['user_id']).encode(), value=json.dumps(row).encode(),
                        partition=0, timeout=10)

    common = ['--run-id', run_id, '--topic', action_topic, '--refined-topic', refined]
    spark_options = ['--master', 'local[2]', '--driver-memory', '1g',
                     '--conf', 'spark.jars.ivySettings=/code/ivy.xml',
                     '--conf', 'spark.jars.ivy=/state/ivy',
                     '--packages', 'io.delta:delta-spark_2.13:4.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0']
    with (output / 'decision.log').open('wb') as log:
        subprocess.run(DOCKER + ['-e', 'PYTHONPATH=/code:/opt/data06-python',
                       '--entrypoint', '/opt/spark/bin/spark-submit', 'spark', *spark_options, '/code/stream.py',
                       '--stage', 'decision', '--decision-publisher', 'driver', *common],
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=160)
    decisions = read_topic(refined)
    assert len(decisions) == len(inputs)
    older, exact, duplicate, late, fresh = decisions[-5:]
    assert [row['status'] for row in decisions[-5:]] == ['accepted', 'accepted', 'duplicate', 'late', 'accepted']
    assert older['session_id'] == exact['session_id'] == edge[0]['event_id']
    assert duplicate['session_id'] is late['session_id'] is None
    assert fresh['session_id'] == edge[-1]['event_id']

    options = ['--stage', 'redis', '--continuous', '--redis-direct', '--redis-direct-offsets', 'kafka',
               '--prediction-topic', prediction, '--prediction-input-calculator', 'verify_recent_actions:sample_inputs',
               '--prediction-input-version', 'local-fixture', '--session-safety-ttl-seconds', '300',
               '--replay-prediction-permille', '1000', *common]
    with redis.Redis(host='127.0.0.1', port=36606, decode_responses=True, socket_timeout=5) as client:
        ns = lambda user: f'data06:{run_id}:user:{user}'
        session = lambda row: f"feat:session:{row['event_id']}"
        with worker(output, f'{run_id}-missing-topic', options) as (process, log):
            assert process.wait(timeout=75) != 0
            error = log.read_text(encoding='utf-8', errors='replace')
            assert 'Kafka did not acknowledge' in error or 'UNKNOWN_TOPIC' in error or 'MSG_TIMED_OUT' in error, error
            assert not client.exists(ns(71), session(main[0]), session(main[0]) + ':actions')

        create_topics(BOOTSTRAP, [topic(prediction)])
        with worker(output, f'{run_id}-retry', options) as (process, log):
            def finished():
                last = client.get(ns(72))
                return last and json.loads(last)['features']['last_event_id'] == edge[-1]['event_id']
            wait_for(finished, process, log)
            actions = [json.loads(row) for row in client.lrange(session(main[0]) + ':actions', 0, -1)]
            assert [row['seq'] for row in actions] == [4, 5, 6, 7, 8]
            assert [row['sequence'] for row in actions] == [4, 5, 6, 7, 9]
            assert all(row['product_id'] == '55' and row['event_time_utc'].endswith('.123Z') for row in actions)
            same_expiry = client.pexpiretime(session(main[0])) == client.pexpiretime(session(main[0]) + ':actions') > 0
            assert same_expiry
            new_actions = [json.loads(row) for row in client.lrange(session(edge[-1]) + ':actions', 0, -1)]
            assert len(new_actions) == new_actions[0]['seq'] == 1
            assert not client.exists(session(edge[0]), session(edge[0]) + ':actions')
            sent = read_topic(prediction)
            expected = [row['event_id'] for row in [*main[:7], edge[0], edge[1], edge[-1]]]
            assert [row['input_event_id'] for row in sent] == expected
            assert [row['inputs']['action_count'] for row in sent] == [1, 2, 3, 4, 5, 5, 5, 1, 2, 1]
            assert sent[-1]['feature_revision'] == client.hget(session(edge[-1]), 'feature_revision')

    report = {'input_records': len(inputs), 'predictions': len(sent), 'prediction_event_ids': expected,
              'missing_ack_left_redis_unchanged': True, 'retry_published_and_applied': True,
              'kept_action_sequence': [row['sequence'] for row in actions], 'same_product_repeats': len(actions),
              'exactly_30_minutes_same_session': True, 'over_30_minutes_reset_seq': 1,
              'late_and_duplicate_not_added': True, 'purchase_not_added_and_stops_prediction': True,
              'session_and_list_expire_together': same_expiry, 'session_id': fresh['session_id'],
              'input_calculator': 'test double'}
    (output / 'verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    os.environ.update(DATA06_KAFKA_PORT='39440', DATA06_REDIS_PORT='36606')
    verify()
