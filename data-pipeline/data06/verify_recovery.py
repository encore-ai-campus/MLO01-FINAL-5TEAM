"""Test Redis loss and catch-up against the local Kafka and Delta pipeline."""
import argparse
import json
import subprocess
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import redis
import pyarrow.parquet as pq
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from benchmark_refined import BOOTSTRAP, COMPOSE, ENVIRONMENT, ROOT, event_topic_configs, read_topic
from reco.replay_parquet import events_in_order
from verify import redis_expected
from verify_refined import compare_delta, markers, on_wire


def expected_batch_history(path, event_time):
    commit, = sorted((path / '_delta_log').glob('*.json'))
    actions = [json.loads(line) for line in commit.read_text(encoding='utf-8').splitlines()]
    allowed = datetime.fromisoformat(event_time)
    selected = {}
    for action in actions:
        if 'add' not in action:
            continue
        for row in pq.read_table(path / action['add']['path']).to_pylist():
            at = row['available_at'].replace(tzinfo=timezone.utc)
            key = f"batch:{row['kind']}:{row['entity']}"
            if at < allowed and (key not in selected or at > selected[key][0]):
                selected[key] = (at, row)
    names = {'user': ('views_30d', 'purchases_30d'), 'item': ('views_1d', 'views_7d')}
    return {key: dict(zip(names[row['kind']], (row['metric_a'], row['metric_b']), strict=True)) | {
        'as_of': row['as_of'].replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z'),
        'available_at': at.isoformat().replace('+00:00', 'Z'),
    } for key, (at, row) in selected.items()}


def verify(output, workload, batch_history, bootstrap_report):
    run_id = 'recovery-' + uuid.uuid4().hex[:10]
    results = (output / run_id).resolve()
    results.mkdir(parents=True)
    topics = (run_id + '-actions', run_id + '-refined')
    redis_url = f'redis://{run_id}:6379/0'
    prefix = f'data06:{run_id}:'

    def execute(label, command, failure=None):
        path = results / f'{label}.log'
        print(f'Running {label}', flush=True)
        with path.open('wb') as log:
            proc = subprocess.run(command, cwd=ROOT, env=ENVIRONMENT, stdout=log,
                                  stderr=subprocess.STDOUT, timeout=200)
        text = path.read_text(encoding='utf-8', errors='replace')
        if not (proc.returncode == 0 if failure is None else proc.returncode != 0 and failure in text):
            raise RuntimeError(f'{label}: {proc.returncode}\n{text[-4500:]}')
        return text

    def spark(label, *args, failure=None):
        return execute(label, COMPOSE + ['run', '--no-deps', '--rm',
                       '-v', f'{results}:/handoff', '-v', f'{batch_history.resolve()}:/batch-history:ro',
                       'spark', '--run-id', run_id, *args], failure)

    def stage(label, stage_name, *args):
        flags = ('--redis-url', redis_url) if stage_name == 'redis' else ()
        return spark(label, '--stage', stage_name, '--topic', topics[0], '--refined-topic', topics[1], *flags, *args)

    def restore(label, receipt, boundary, event_time, failure=None):
        return spark(label, '--recover-redis', '/handoff/' + receipt, '--refined-topic', topics[1],
                     '--boundary', json.dumps(boundary, separators=(',', ':')), '--redis-url', redis_url,
                     '--batch-history', '/batch-history', '--replay-time', event_time, failure=failure)

    def snapshot(client):
        return {name.removeprefix(prefix): json.loads(client.get(name))
                for name in sorted(client.scan_iter(match=prefix + '*'))}

    def kafka_snapshot():
        raw = {(p, o): event for p, o, _, event in read_topic(topics[0])}
        first = {}
        boundary = {'0': 0, '1': 0}
        for p, o, _, event in read_topic(topics[1]):
            key = (event['origin_partition'], event['origin_offset'])
            assert raw[key]['event_id'] == event['event_id'] and key[0] == p
            first.setdefault(key, {'origin_partition': key[0], 'origin_offset': key[1],
                                   'refined_partition': p, 'refined_offset': o, 'status': event['status'],
                                   'event_id': event['event_id']})
            boundary[str(p)] = max(boundary[str(p)], o + 1)
        assert set(first) == set(raw)
        return raw, first, boundary

    def check(users_and_batch, expected, first_copies):
        users = {int(key.split(':')[1]): value for key, value in users_and_batch.items() if key.startswith('user:')}
        assert {user: data['features'] for user, data in users.items()} == redis_expected(expected)
        positions = {row['event_id']: row for row in first_copies.values() if row['status'] == 'accepted'}
        for user in users.values():
            last = positions[user['features']['last_event_id']]
            assert (user['origin_partition'], user['origin_offset'], user['refined_offset']) == (
                last['origin_partition'], str(last['origin_offset']), str(last['refined_offset']))
        return len(users)

    execute('00-kafka', COMPOSE + ['up', '-d', '--wait', 'kafka'])
    with closing(KafkaAdminClient(bootstrap_servers=BOOTSTRAP)) as admin:
        admin.create_topics([NewTopic(name, 2, 1, topic_configs=event_topic_configs(1)) for name in topics])
    execute('01-dedicated-redis', ['docker', 'run', '--rm', '-d', '--name', run_id,
                                  '--network', 'mlo-data06-split_default',
                                  '--publish', '127.0.0.1::6379', 'redis:8.2-alpine'])
    try:
        binding = subprocess.check_output(['docker', 'port', run_id, '6379/tcp'], text=True).strip()
        host_client = redis.Redis.from_url('redis://' + binding + '/0', decode_responses=True, socket_timeout=5)
        with host_client as client, closing(KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all',
                                                         enable_idempotence=True)) as producer:
            def send(rows):
                for event in rows:
                    producer.send(topics[0], key=str(event['user_id']).encode(),
                                  value=json.dumps(on_wire(event)).encode()).get(timeout=15)

            events = list(events_in_order(workload))[:240]
            before, after = events[:150], events[150:]
            assert len(events) == 240
            late = {**before[0], 'event_seq': 9_000_000_000, 'event_time': before[0]['event_time'] - timedelta(days=1)}
            late['event_id'] = f"{late['source']}:{late['event_seq']}"
            send(before[:100])
            stage('02-decide-first', 'decision')
            with ThreadPoolExecutor(max_workers=2) as pool:
                left = pool.submit(stage, '03-store-first', 'delta')
                stage('04-redis-first', 'redis')
                left.result()
            send(before[100:] + before[:5] + [late])
            stage('05-freeze-decisions', 'decision')
            stage('06-redis-before-loss', 'redis')
            raw, originals, boundary = kafka_snapshot()
            event_time = before[-1]['event_time'].isoformat()
            bootstrap = json.loads(bootstrap_report.read_text(encoding='utf-8'))
            product_key, features = next((key, row) for key, row in bootstrap['first_request_keys'].items()
                                         if key.startswith('batch:item:'))
            known = {'views_1d': features['views_1d'], 'views_7d': features['views_7d'],
                     'as_of': features['as_of'].replace('+00:00', 'Z'),
                     'available_at': features['available_at'].replace('+00:00', 'Z')}
            batch_values = expected_batch_history(batch_history, event_time)
            assert expected_batch_history(batch_history, bootstrap['cutoff_utc'])[product_key] == known
            assert product_key in batch_values
            for key, value in batch_values.items():
                client.set(prefix + key, json.dumps(value))
            pre_loss = snapshot(client)
            users_before = check(pre_loss, before, originals)
            restore('07-reject-delta-behind', 'too-early.json', boundary, event_time,
                    failure='DELTA_BEHIND_RECOVERY_BOUNDARY')
            assert snapshot(client) == pre_loss and not (results / 'too-early.json').exists()
            stage('08-delta-catchup', 'delta')
            keys = list(client.scan_iter(match=prefix + '*'))
            assert client.delete(*keys) == len(keys)
            assert not snapshot(client)
            restore('09-restore-after-loss', 'recovery.json', boundary, event_time)
            restored = snapshot(client)
            assert restored == pre_loss
            receipt = json.loads((results / 'recovery.json').read_text())
            assert receipt['boundary'] == boundary and receipt['restored_user_keys'] == users_before
            assert any(receipt['restart_offsets'][p] < boundary[p] for p in boundary)

            send(before[20:25] + after)
            stage('10-resume-decisions', 'decision')
            with ThreadPoolExecutor(max_workers=2) as pool:
                left = pool.submit(stage, '11-resume-delta', 'delta')
                resume = stage('12-resume-with-new-redis-checkpoint', 'redis', '--redis-recovery', '/handoff/recovery.json')
                left.result()
            raw, originals, end = kafka_snapshot()
            final = snapshot(client)
            users_after = check(final, events, originals)
            assert {k: v for k, v in final.items() if k.startswith('batch:')} == {
                k: v for k, v in restored.items() if k.startswith('batch:')}
            stats = markers(resume, 'DATA06_REDIS ')
            applied = sum(item['applied'] for item in stats)
            boundary_skipped = sum(item['boundary_skipped'] for item in stats)
            assert applied == len(after) and boundary_skipped > 0
            counts = compare_delta(stage('13-verify-delta', 'inspect'), raw, originals)
            assert counts == Counter(accepted=240, duplicate=10, late=1)
        report = {'run_id': run_id, 'boundary_next_offsets': boundary, 'final_next_offsets': end,
                  'old_offsets_reread_but_not_applied': boundary_skipped, 'new_events_applied_once': applied,
                  'delta_behind_rejected_without_touching_redis': True, 'redis_emptied': True,
                  'restored_user_keys_matched': users_before, 'final_user_keys_matched': users_after,
                  'restored_batch_keys': receipt['restored_batch_keys'], 'independently_checked_batch_keys': len(batch_values),
                  'delta_decisions': dict(counts), 'missing_or_duplicate_delta_positions': 0,
                  'missing_or_duplicate_redis_applications': 0}
        (results / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(report, indent=2), flush=True)
    finally:
        subprocess.run(['docker', 'stop', '--timeout', '2', run_id], capture_output=True, timeout=20)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'data/week3-redis-recovery')
    parser.add_argument('--workload', type=Path, required=True)
    parser.add_argument('--bootstrap', type=Path, required=True)
    args = parser.parse_args()
    verify(args.output, args.workload, args.bootstrap / 'batch_history', args.bootstrap / 'verification.json')
