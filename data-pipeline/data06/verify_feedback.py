"""Exercise local feedback transport, Delta idempotence and cross-log joins."""
import argparse
import json
import subprocess
import uuid
from collections import Counter
from contextlib import closing
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from benchmark_refined import BOOTSTRAP, COMPOSE, ENVIRONMENT, ROOT, event_topic_configs


def feedback_fixture():
    rows = []
    for source in ('replay', 'human_demo', 'load_test'):
        common = {
            'replay_run_id': f'visit-{source}', 'request_id': 'request-01', 'source': source,
            'session_id': 'session-01', 'user_id': None if source == 'human_demo' else f'{source}:user-1',
            'event_time': '2026-11-20T00:00:00Z', 'replay_time': '2019-11-17T12:37:00Z',
            'feature_as_of': '2019-11-17T12:36:59Z', 'feature_revision': 'run-1:0:24',
        }
        rows.extend([
            {**common, 'log_id': 'click-1', 'log_type': 'click', 'model_version': 'reco-v1',
             'impression_id': 'impression-1', 'product_id': 102, 'position': 1},
            {**common, 'log_id': 'impression-1', 'log_type': 'impression', 'model_version': 'reco-v1',
             'product_id': 102, 'position': 1},
            {**common, 'log_id': 'impression-2', 'log_type': 'impression', 'model_version': 'reco-v1',
             'product_id': 103, 'position': 2},
            {**common, 'log_id': 'prediction-1', 'log_type': 'prediction', 'model_version': 'purchase-v1',
             'prediction_id': f'prediction-{source}', 'prediction_probability': .42},
        ])
    return rows


def parse(output, marker):
    return [json.loads(line[len(marker):]) for line in output.splitlines() if line.startswith(marker)]


def verify(out):
    run_id = 'feedback-' + uuid.uuid4().hex[:10]
    artifacts = out / run_id
    artifacts.mkdir(parents=True)

    def run(label, options, error=None):
        path = artifacts / f'{label}.log'
        print(f'Running {label}', flush=True)
        with path.open('wb') as log:
            result = subprocess.run(COMPOSE + options, cwd=ROOT, env=ENVIRONMENT, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=180)
        output = path.read_text(encoding='utf-8', errors='replace')
        if (error is None and result.returncode) or (error and (not result.returncode or error not in output)):
            raise RuntimeError(f'{label} returned {result.returncode}. {output[-5000:]}')
        return output

    def spark(label, stage, *args, error=None):
        return run(label, ['run', '--no-deps', '--rm', 'spark', '--feedback-stage', stage,
                           '--run-id', run_id, '--topic', run_id, *args], error)

    run('00-services', ['up', '-d', '--wait', 'kafka'])
    with closing(KafkaAdminClient(bootstrap_servers=BOOTSTRAP)) as admin:
        admin.create_topics([NewTopic(run_id, 2, 1, topic_configs=event_topic_configs(1))])
    expected = feedback_fixture()
    (artifacts / 'fixture.json').write_text(json.dumps(expected, indent=2) + '\n', encoding='utf-8')
    first = [row for row in expected if row['log_type'] == 'click']
    parents = [row for row in expected if row['log_type'] != 'click']
    with closing(KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', enable_idempotence=True)) as producer:
        def emit(rows):
            for row in rows:
                producer.send(run_id, key=row['log_id'].encode(),
                              value=json.dumps(row).encode()).get(timeout=15)

        emit(first + first)
        spark('01-crash-after-commit', 'ingest', '--fail-after-delta', error='INJECTED_FEEDBACK_FAILURE_AFTER_DELTA')
        emit(parents + expected)
        spark('02-restart-and-resend', 'ingest')
        emit([{**parents[0], 'position': 99}])
        spark('03-reject-conflicting-resend', 'ingest', error='Conflicting payload for the same feedback log ID')
    output = spark('04-delta-readback-and-joins', 'inspect')
    rows = parse(output, 'FEEDBACK_ROW ')
    joins = parse(output, 'FEEDBACK_JOIN ')
    keys = {(row['replay_run_id'], row['log_id']) for row in rows}
    expected_by_id = {(row['replay_run_id'], row['log_id']): row for row in expected}
    assert len(rows) == len(keys) and keys == set(expected_by_id)
    timestamps = {'event_time', 'replay_time', 'feature_as_of'}
    for row in rows:
        original = expected_by_id[row['replay_run_id'], row['log_id']]
        assert all(row[field] == value for field, value in original.items() if field not in timestamps)
        assert all(row[field].replace(' ', 'T') + 'Z' == original[field] for field in timestamps)
    assert len(joins) == 3
    assert all(link['matched_impression'] == 'impression-1'
               and link['prediction_id'] == f"prediction-{link['source']}" for link in joins)
    counts = {source: dict(Counter(row['log_type'] for row in rows if row['source'] == source))
              for source in sorted({row['source'] for row in rows})}
    assert all(row == {'prediction': 1, 'impression': 2, 'click': 1} for row in counts.values())
    eligible = [row for row in rows if row['source'] == 'replay']
    assert len(eligible) == 4
    with closing(KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False)) as consumer:
        broker_records = sum(consumer.end_offsets([TopicPartition(run_id, n) for n in range(2)]).values())
    assert broker_records == 28
    report = {'run_id': run_id, 'runtime': {'spark': '4.0.1', 'delta': '4.0.0', 'kafka': '3.9.1'},
              'kafka_records': broker_records, 'delta_rows': len(rows),
              'resent_matching_messages': 15, 'conflicting_resend_rejected': 1,
              'unique_delta_keys': len(keys), 'missing_delta_keys': 0, 'duplicate_delta_keys': 0,
              'by_source': counts, 'fully_linked_clicks': len(joins), 'links': joins,
              'default_replay_filter_rows': len(eligible), 'default_replay_filter_excluded': len(rows) - len(eligible),
              'restarted_after_delta_commit': True, 'guest_user_id_preserved_as_null': True}
    (artifacts / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'data/week3-feedback')
    options = parser.parse_args()
    verify(options.output)
