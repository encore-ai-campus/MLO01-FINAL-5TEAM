"""Verify the 19-3 decision contract using local Kafka and Spark."""
import json
import subprocess
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from confluent_kafka.admin import NewTopic

from tool_kafka import acknowledged_producer, create_topics, send_record
from verify_recent_actions import BOOTSTRAP, DOCKER, OUTPUT, read_topic


def verify():
    run_id = 'session19-' + uuid4().hex[:8]
    output = OUTPUT / run_id
    output.mkdir(parents=True)
    action_topic, refined_topic = (f'{run_id}-{suffix}' for suffix in ('actions', 'refined'))
    create_topics(BOOTSTRAP, [NewTopic(name, 1, 1, config={'message.timestamp.type': 'LogAppendTime'})
                              for name in (action_topic, refined_topic)])
    start = datetime(2019, 11, 17, tzinfo=timezone.utc)
    browser = f'web-{uuid4()}'
    other_browser = f'web-{uuid4()}'

    def web(seconds, sequence, user=None, session=browser, replay=run_id):
        return {
            'source': 'web', 'replay_run_id': replay, 'raw_session_id': session,
            'sequence': sequence, 'event_id': f'web:{uuid4()}',
            'event_time_utc': (start + timedelta(seconds=seconds)).isoformat(),
            'product_id': '55', 'event_type': 'view', 'user_id': user,
        }

    first = web(0, 1)
    login = web(1800, 2, '71')
    older_same_time = web(1800, 1, '71')
    same_time = web(1800, 3, '71')
    late = web(1799, 4, '71')
    fresh = web(3600.001, 5, '71')
    other = web(3600.5, 1, '71', other_browser)
    wrong_run = web(3600.75, 6, '71', replay='another-run')
    wrong_key = web(3600.8, 7, '71')
    no_run = web(3600.9, 8, '71', replay=None)
    replay = {
        'event_id': 'nov.csv.gz:1', 'source': 'nov.csv.gz', 'event_seq': 1,
        'raw_session_id': browser, 'replay_run_id': run_id,
        'event_time': (start + timedelta(seconds=3601)).isoformat(),
        'product_id': 55, 'event_type': 'view', 'user_id': 71,
    }
    canonical_replay = {**replay, 'event_id': 'nov.csv.gz:2', 'source': 'replay',
                        'source_file': 'nov.csv.gz', 'source_row': 2, 'event_seq': None,
                        'event_time_utc': replay['event_time'], 'event_time': None,
                        'product_id': '55', 'user_id': '71', 'raw_session_id': 'catalog-session-2'}
    cases = [
        (browser, first, 'accepted', first['event_id'], None),
        (browser, login, 'accepted', first['event_id'], None),
        (browser, login, 'duplicate', None, None),
        (browser, older_same_time, 'late', None, None),
        (browser, same_time, 'accepted', first['event_id'], None),
        (browser, late, 'late', None, None),
        (browser, fresh, 'accepted', fresh['event_id'], None),
        (other_browser, other, 'accepted', other['event_id'], None),
        (browser, wrong_run, 'invalid', None, 'invalid_replay_run'),
        ('71', wrong_key, 'invalid', None, 'invalid_kafka_key'),
        (browser, no_run, 'accepted', no_run['event_id'], None),
        ('71', replay, 'accepted', replay['event_id'], None),
        ('71', canonical_replay, 'accepted', replay['event_id'], None),
    ]

    def send(rows):
        with acknowledged_producer(BOOTSTRAP) as producer:
            for key, event, *_ in rows:
                send_record(producer, action_topic, key=key.encode(), value=json.dumps(event).encode(),
                            partition=0, timeout=10)

    def decide(name):
        options = ['--master', 'local[2]', '--driver-memory', '1g',
                   '--conf', 'spark.jars.ivySettings=/code/ivy.xml',
                   '--conf', 'spark.jars.ivy=/state/ivy',
                   '--packages', 'io.delta:delta-spark_2.13:4.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0']
        log_path = output / f'{name}.log'
        with log_path.open('wb') as log:
            job = subprocess.run(DOCKER + ['-e', 'PYTHONPATH=/code:/opt/data06-python',
                                 '--entrypoint', '/opt/spark/bin/spark-submit', 'spark',
                                 *options, '/code/stream.py', '--stage', 'decision',
                                 '--decision-state-store', 'spark', '--decision-publisher', 'driver',
                                 '--decision-state-partitions', '2', '--run-id', run_id, '--topic',
                                 action_topic, '--refined-topic', refined_topic],
                                 stdout=log, stderr=subprocess.STDOUT, timeout=180)
        if job.returncode:
            raise RuntimeError(log_path.read_text(encoding='utf-8', errors='replace')[-9000:])

    send(cases[:1])
    decide('first')
    send(cases[1:])
    decide('restart')
    rows = sorted(read_topic(refined_topic), key=lambda row: row['origin_offset'])
    actual = [(row['event_id'], row['status'], row['session_id'], row['rejection_reason']) for row in rows]
    expected = [(event['event_id'], status, session, reason) for _, event, status, session, reason in cases]
    assert actual == expected, (actual, expected)
    assert rows[0]['user_id'] is None and str(rows[1]['user_id']) == '71'
    assert rows[0]['raw_session_id'] == rows[1]['raw_session_id'] == browser
    report = {'run_id': run_id, 'kafka_and_spark': True, 'restart': True, 'rows': rows}
    (output / 'verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(output / 'verification.json', flush=True)


if __name__ == '__main__':
    verify()
