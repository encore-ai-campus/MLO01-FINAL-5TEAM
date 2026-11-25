"""Send local predictions through Kafka/Delta and join them to known visit outcomes."""
import argparse
import json
import sqlite3
import subprocess
import uuid
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from benchmark_refined import BOOTSTRAP, COMPOSE, ENVIRONMENT, ROOT
from reco.prediction_labels import build_labels, read_visits, visit_events, write_labels
from reco.store import checksum
from reco.team_replay import data_receipt
from verify_feedback import parse


def fixture(database, visits, run_id):
    def first(predicate):
        return next(visit for visit in visits.values() if predicate(visit))

    purchased = first(lambda v: v['is_closed'] and v['purchased'])
    nonpurchase = first(lambda v: v['is_closed'] and not v['purchased'])
    pending = first(lambda v: not v['is_closed'])
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as connection:
        times = [datetime.fromtimestamp(row.ts, timezone.utc) for row in visit_events(connection, purchased)
                 if row.action == 2]
    assert purchased['started_at'] < times[0] and times[-1] + timedelta(seconds=1) < purchased['closed_at']
    cases = (
        ('future_purchase', purchased, purchased['started_at'], 'replay', 1, 'final', None),
        ('earlier_purchase_only', purchased, times[-1] + timedelta(seconds=1), 'replay', 0, 'final', None),
        ('no_purchase', nonpurchase, nonpurchase['started_at'], 'replay', 0, 'final', None),
        ('open_visit', pending, pending['started_at'], 'replay', None, 'pending', 'visit_open'),
        ('human_demo', purchased, purchased['started_at'], 'human_demo', None, 'excluded', 'human_demo'),
        ('load_test', purchased, purchased['started_at'], 'load_test', None, 'excluded', 'load_test'),
        ('same_second', purchased, times[0], 'replay', None, 'excluded', 'purchase_at_prediction_time'),
        ('missing_visit', {**nonpurchase, 'visit_id': 'unmatched:visit'}, nonpurchase['started_at'],
         'replay', None, 'pending', 'visit_not_found'),
    )
    rows, expected = [], {}
    for name, visit, at, source, label, status, reason in cases:
        rows.append({
            'log_id': f'log-{name}', 'request_id': f'request-{name}', 'replay_run_id': run_id,
            'source': source, 'log_type': 'prediction', 'event_time': '2026-11-20T00:00:00Z',
            'replay_time': at.isoformat(), 'user_id': str(visit['user_id']), 'session_id': visit['visit_id'],
            'model_version': 'purchase-local-fixture', 'feature_as_of': at.isoformat(),
            'feature_revision': f'{run_id}:fixture', 'prediction_id': name, 'prediction_probability': 0.42,
        })
        expected[name] = (label, status, reason)
    return rows, expected


def run(database, tables, destination):
    run_id = 'prediction-labels-' + uuid.uuid4().hex[:8]
    root = (destination / run_id).resolve()
    root.mkdir(parents=True)

    def command(name, args):
        print(f'Running {name}', flush=True)
        path = root / f'{name}.log'
        with path.open('wb') as log:
            proc = subprocess.run(COMPOSE + args, cwd=ROOT, env=ENVIRONMENT, stdout=log,
                                  stderr=subprocess.STDOUT, timeout=180)
        text = path.read_text(encoding='utf-8', errors='replace')
        if proc.returncode:
            raise RuntimeError(f'{name} failed: {text[-4200:]}')
        return text

    visits, receipt = read_visits(tables)
    assert data_receipt(database, 'local')['manifest_sha256'] == receipt['manifest_sha256']
    inputs, expected = fixture(database, visits, run_id)
    command('01-kafka', ['up', '-d', '--wait', 'kafka'])
    with closing(KafkaAdminClient(bootstrap_servers=BOOTSTRAP)) as admin:
        admin.create_topics([NewTopic(run_id, 2, 1)])
    with closing(KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', enable_idempotence=True)) as producer:
        for record in inputs + inputs[:1]:
            producer.send(run_id, key=record['log_id'].encode(), value=json.dumps(record).encode()).get(timeout=15)
    for stage in ('ingest', 'inspect'):
        output = command(f'02-{stage}', ['run', '--no-deps', '--rm', 'spark', '--feedback-stage', stage,
                                         '--run-id', run_id, '--topic', run_id])
    logs = parse(output, 'FEEDBACK_ROW ')
    assert len(logs) == len(inputs), 'Delta did not retain one record per prediction'
    labels = build_labels(logs, visits, database)
    actual = {row['prediction_id']: (row['label'], row['status'], row['reason']) for row in labels}
    assert actual == expected, actual
    positive = next(row for row in labels if row['label'] == 1)
    assert positive['first_purchase_after_prediction_at'] > positive['predicted_at']
    path = root / 'prediction_labels.parquet'
    write_labels(labels, path)
    report = {'run_id': run_id, 'visit_rows_loaded': len(visits), 'parquet_sha256': checksum(path),
              'kafka_messages': len(inputs) + 1, 'delta_prediction_logs': len(logs),
              'rows': len(labels), 'status_counts': dict(Counter(row['status'] for row in labels)),
              'positive': sum(row['label'] == 1 for row in labels), 'negative': sum(row['label'] == 0 for row in labels),
              'labels_unset': sum(row['label'] is None for row in labels), 'checks_passed': len(expected),
              'policy': 'Strictly later replay time within same closed visit. A purchase at the prediction second is excluded.',
              'cases': labels}
    raw = json.dumps(report, default=str, indent=2) + '\n'
    (root / 'verification.json').write_text(raw, encoding='utf-8')
    print(json.dumps({key: value for key, value in report.items() if key != 'cases'}, indent=2), flush=True)
    print(f'Artifacts: {root}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'data/events-through-dec-2pct.sqlite')
    parser.add_argument('--tables', type=Path, default=ROOT / 'data/week3-training-tables')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/week4-prediction-labels')
    args = parser.parse_args()
    run(args.database, args.tables, args.output)
