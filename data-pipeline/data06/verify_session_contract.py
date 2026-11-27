"""Exercise PM-11 action contracts on two Kafka partitions, Spark, and Redis."""
import json
import os
import subprocess
from collections import Counter
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import redis
from confluent_kafka.admin import NewTopic

from tool_kafka import acknowledged_producer, create_topics, send_record
from verify_recent_actions import BOOTSTRAP, DOCKER, OUTPUT, read_topic, wait_for, worker


def verify():
    run_id = 'pm11-' + uuid4().hex[:8]
    output = OUTPUT / run_id
    output.mkdir(parents=True)
    action_topic, refined_topic = (f'{run_id}-{suffix}' for suffix in ('actions', 'refined'))
    live_run = run_id + '-live'
    live_action, live_refined = (f'{live_run}-{suffix}' for suffix in ('actions', 'refined'))
    create_topics(BOOTSTRAP, [NewTopic(name, 2, 1, config={'message.timestamp.type': 'LogAppendTime'})
                              for name in (action_topic, refined_topic, live_action, live_refined)])
    start = datetime(2019, 11, 17, tzinfo=timezone.utc)
    browser, browser2, browser3 = [f'web-{uuid4()}' for _ in range(3)]
    load_session = f'load-{run_id}-1'
    replay_user = str(int(uuid4().hex[:11], 16))

    def web(seconds, sequence, user='web-17', session=browser, replay=run_id):
        at = start + timedelta(seconds=seconds) if replay else datetime.now(timezone.utc)
        return {'schema_version': '1', 'source': 'web', 'replay_run_id': replay, 'raw_session_id': session,
                'sequence': sequence, 'event_id': f'web:{uuid4()}',
                'event_time_utc': at.isoformat(), 'replay_time_utc': at.isoformat(),
                'produced_at_utc': datetime.now(timezone.utc).isoformat(), 'product_id': '55',
                'category_id': '9', 'price': 42.0, 'event_type': 'view', 'user_id': user,
                'source_file': None, 'source_row': None}

    first, login, same_time = web(0, 1, None), web(1800, 2), web(1800, 3)
    same_time_same_seq = web(1800, 3)
    late, fresh = web(1799, 4), web(3600.001, 5)
    other = web(1800, 1, session=browser2)
    no_run = web(0, 1, replay=None)
    distinct_no_run = web(0, 2, replay=None, session=browser3)
    load = {**web(1801, 1), 'source': 'loadtest', 'raw_session_id': load_session,
            'event_id': f'load:{run_id}:1', 'user_id': 'load-17'}
    replay = {'event_id': f'{run_id}.csv.gz:1', 'source': f'{run_id}.csv.gz',
              'event_seq': 1, 'event_time': (start + timedelta(seconds=1801)).isoformat(),
              'product_id': 55, 'event_type': 'view', 'user_id': int(replay_user)}
    missing = web(1802, 6)
    del missing['price']
    valid_cases = [
        (0, browser, first, 'accepted', first['event_id'], None),
        (1, browser3, distinct_no_run, 'invalid', None, 'invalid_replay_run'),
        (0, browser, login, 'accepted', first['event_id'], None),
        (0, browser, same_time, 'accepted', first['event_id'], None),
        (0, browser, same_time_same_seq, 'accepted', first['event_id'], None),
        (0, browser, login, 'duplicate', None, None),
        (0, browser, late, 'late', None, None),
        (1, browser2, other, 'accepted', other['event_id'], None),
        (1, load_session, load, 'accepted', load['event_id'], None),
        (1, replay_user, replay, 'accepted', replay['event_id'], None),
    ]
    invalid_cases = [
        (0, browser, missing, 'invalid', None, 'invalid_message_fields'),
        (0, 'web-x', web(1802, 7, session='web-x'), 'invalid', None, 'invalid_kafka_key'),
        (0, browser, {**web(1802, 8), 'event_id': 'web:00000000-0000-0000-0000-000000000000'},
         'invalid', None, 'invalid_event_id'),
        (0, browser, web(1802, 9, '71'), 'invalid', None, 'invalid_user_id'),
        (0, browser, {**web(1802, 10), 'produced_at_utc': 'not-a-timestamp'},
         'invalid', None, 'invalid_event_time'),
        (0, browser, web(3601, 11, replay='another-run'), 'invalid', None, 'invalid_replay_run'),
    ]
    last_case = (0, browser, fresh, 'accepted', fresh['event_id'], None)

    def send(topic, cases, expected):
        with acknowledged_producer(BOOTSTRAP) as producer:
            for partition, key, event, status, session, reason in cases:
                record = send_record(producer, topic, key=key.encode(), value=json.dumps(event).encode(),
                                     partition=partition, timeout=10)
                expected[partition, record.offset()] = (event['event_id'], status, session, reason)

    spark_options = ['--master', 'local[2]', '--driver-memory', '1g',
                     '--conf', 'spark.jars.ivySettings=/code/ivy.xml', '--conf', 'spark.jars.ivy=/state/ivy',
                     '--packages', 'io.delta:delta-spark_2.13:4.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0']

    def spark(name, stage, run, action, refined, *args):
        log_path = output / f'{name}.log'
        with log_path.open('wb') as log:
            job = subprocess.run(DOCKER + ['-e', 'PYTHONPATH=/code:/opt/data06-python',
                                 '--entrypoint', '/opt/spark/bin/spark-submit', 'spark',
                                 *spark_options, '/code/stream.py', '--stage', stage, '--run-id', run,
                                 '--topic', action, '--refined-topic', refined, *args],
                                 stdout=log, stderr=subprocess.STDOUT, timeout=210)
        if job.returncode:
            raise RuntimeError(log_path.read_text(encoding='utf-8', errors='replace')[-10000:])

    def decide(name, run, action, refined, *args):
        spark(name, 'decision', run, action, refined, '--decision-state-store', 'spark',
              '--decision-publisher', 'driver', '--decision-state-partitions', '2', *args)

    def check(topic, expected):
        rows = read_topic(topic)
        assert len(rows) == len(expected), rows
        actual = {(row['origin_partition'], row['origin_offset']):
                  (row['event_id'], row['status'], row['session_id'], row['rejection_reason']) for row in rows}
        assert actual == expected, (actual, expected)
        return rows

    expected = {}
    send(action_topic, valid_cases[:2], expected)
    decide('first', run_id, action_topic, refined_topic)
    send(action_topic, [*valid_cases[2:], *invalid_cases], expected)
    decide('restart', run_id, action_topic, refined_topic)
    rows = check(refined_topic, expected)
    assert next(row['user_id'] for row in rows if row['event_id'] == same_time['event_id']) == 'web-17'
    assert next(row['user_id'] for row in rows if row['event_id'] == load['event_id']) == 'load-17'
    assert {row['origin_partition'] for row in rows if row['status'] == 'accepted'} == {0, 1}

    redis_url = 'redis://redis:6379/0'
    current = f'feat:current_session:{browser}'
    with redis.Redis(host='127.0.0.1', port=36606, decode_responses=True, socket_timeout=5) as client:
        spark('spark-redis', 'redis', run_id, action_topic, refined_topic, '--redis-url', redis_url)
        state = client.hgetall(current)
        assert state['session_id'] == first['event_id'] and state['processed_position'] == f'{run_id}:0:5', state
        assert state['feature_revision'] == f'{run_id}:0:3', state
        load_current = client.hgetall(f'feat:current_session:{load_session}')
        assert load_current['processed_position'] == f'{run_id}:1:2', load_current
        features = json.loads(client.get(f'data06:{run_id}:user:{browser}'))['features']
        assert features['event_count'] == 4
        # Another Spark launch reads from its Redis checkpoint. This accepted row goes into a new session.
        send(action_topic, [last_case], expected)
        decide('30min-plus', run_id, action_topic, refined_topic)
        rows = check(refined_topic, expected)
        spark('spark-redis-restart', 'redis', run_id, action_topic, refined_topic, '--redis-url', redis_url)
        assert client.hget(current, 'session_id') == fresh['event_id']

    # Exercise the direct Redis worker without prediction routing, in an independent DB.
    with redis.Redis(host='127.0.0.1', port=36606, db=12, decode_responses=True, socket_timeout=5) as client:
        options = ['--stage', 'redis', '--continuous', '--redis-direct', '--redis-direct-offsets', 'kafka',
                   '--redis-url', 'redis://redis:6379/12', '--run-id', run_id, '--topic', action_topic,
                   '--refined-topic', refined_topic]
        with worker(output, f'{run_id}-direct', options) as (process, log):
            wait_for(lambda: client.hget(current, 'session_id') == fresh['event_id'], process, log)
            assert client.hget(f'feat:current_session:{load_session}', 'processed_position') == f'{run_id}:1:2'

    live_expected = {}
    send(live_action, [(0, browser, no_run, 'accepted', no_run['event_id'], None),
                       (1, replay_user, replay, 'invalid', None, 'invalid_replay_run')], live_expected)
    decide('live-isolation', live_run, live_action, live_refined, '--live-action-clock')
    check(live_refined, live_expected)
    report = {'run_id': run_id, 'partitions': 2, 'restart': True, 'standalone_live_checkpoint': live_run,
              'spark_redis_processed_position': state['processed_position'],
              'spark_redis_feature_revision': state['feature_revision'], 'direct_redis_verified': True,
              'counts': dict(Counter(row['status'] for row in rows)), 'rows': rows}
    (output / 'verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(output / 'verification.json', flush=True)


if __name__ == '__main__':
    os.environ.update(DATA06_KAFKA_PORT='39440', DATA06_REDIS_PORT='36606')
    verify()
