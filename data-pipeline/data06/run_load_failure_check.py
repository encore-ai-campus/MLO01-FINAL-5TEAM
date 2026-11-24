"""Run the local load, catch-up and fault checks from Data 06 through 08."""
import argparse
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import redis

from benchmark_refined import COMPOSE, ENVIRONMENT, ROOT, Workload, read_topic, read_window
from benchmark_resources import measure
from reco.replay_parquet import events_in_order
from verify_refined import compare_delta, compare_redis, verify


def compare_sinks(test, *, inputs=('warm', 'workload'), timeout=180, raw=None, refined=None):
    if raw is None:
        raw = read_topic(test.actions, test.bootstrap, timeout=timeout)
    if refined is None:
        refined = read_topic(test.refined, test.bootstrap, timeout=timeout)
    raw = {(partition, offset): message for partition, offset, _, message in raw}
    copies = {}
    for partition, offset, _, message in refined:
        origin = (message['origin_partition'], message['origin_offset'])
        copies.setdefault(origin, {'origin_partition': origin[0], 'origin_offset': origin[1],
                          'refined_partition': partition, 'refined_offset': offset,
                          'status': message['status'], 'event_id': message['event_id']})
    path = test.folder / f'{test.run_id}-inspect.log'
    with path.open('wb') as log:
        subprocess.run(test.compose + ['run', '--no-deps', '--rm', 'spark', '--stage', 'inspect', '--kafka', test.kafka,
                                  '--run-id', test.run_id, '--topic', test.actions, '--refined-topic', test.refined],
                       cwd=ROOT, env=test.environment, stdout=log, stderr=subprocess.STDOUT, timeout=timeout, check=True)
    counts = compare_delta(path.read_text(encoding='utf-8', errors='replace'), raw, copies)
    assert counts == {'accepted': test.warm + test.size}
    events = [event for name in inputs for event in events_in_order(test.folder / f'{name}.parquet')]
    redis_port = test.environment['DATA06_REDIS_PORT']
    with redis.Redis(host='127.0.0.1', port=int(redis_port), decode_responses=True, socket_timeout=5) as client:
        users = compare_redis(client, test.run_id, events, copies)
    return {'delta_raw_rows': len(raw), 'delta_refined_rows': len(copies), 'verified_redis_users': users,
            'missing_or_extra_origin_positions': 0, 'duplicate_delta_origin_positions': 0,
            'redis_double_applications': 0}


def run(source, output, delta_trigger, *, live_only=False, decision_trigger='1 second', redis_trigger='1 second'):
    speed = 10
    folder = output.resolve() / ('load-failure-' + uuid4().hex[:8])
    folder.mkdir(parents=True)
    path = folder / 'verification.json'
    print(f'Reports and logs: {folder}', flush=True)
    subprocess.run(COMPOSE + ['up', '-d', '--wait', 'kafka', 'redis'], env=ENVIRONMENT, check=True, timeout=100)
    window, warm, size = read_window(source, folder)
    report = {'complete': False, 'source': str(source.resolve()), 'window_utc': window,
              'warm_events': warm, 'events': size, 'speed': speed, 'live_only': live_only,
              'triggers': {'decision': decision_trigger, 'redis': redis_trigger, 'delta': delta_trigger}}
    stage_triggers = {'decision_trigger': decision_trigger, 'redis_trigger': redis_trigger}

    def write_report():
        path.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')

    print('10x live load with resource and MERGE sampling', flush=True)
    live = Workload(folder, 'live10x', delta_trigger, warm, size, **stage_triggers)
    report['live'] = measure(live, speed)
    report['live']['sink_integrity'] = compare_sinks(live)
    if live_only:
        report['complete'] = True
    write_report()
    if live_only:
        print(f'Complete: {path}', flush=True)
        return

    print('10x queued workload and checkpoint restart', flush=True)
    restart = Workload(folder, 'restart10x', delta_trigger, warm, size, **stage_triggers)
    restart.publish('warm', 1_000_000)
    with restart.workers('before') as (workers, _):
        restart.wait(workers, 'before', warm)
    restart.publish('workload', speed)
    with restart.workers('after') as (workers, started):
        progress = restart.wait(workers, 'after', warm + size)
    report['restart'] = restart.summarize(speed, progress, started)
    report['restart']['sink_integrity'] = compare_sinks(restart)
    write_report()

    print('Controlled split-worker fault injections', flush=True)
    report['fault_injection'] = verify(source, folder / 'fault-injection')
    report['complete'] = True
    write_report()
    print(f'Complete: {path}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'data/data-04-user-sample-2pct/2019-Nov-sample.parquet')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/data-load-failure-check')
    parser.add_argument('--delta-trigger', default='30 seconds')
    parser.add_argument('--decision-trigger', default='1 second')
    parser.add_argument('--redis-trigger', default='1 second')
    parser.add_argument('--live-only', action='store_true', help='Measure and verify only the live 10x replay')
    options = parser.parse_args()
    run(options.source, options.output, options.delta_trigger, live_only=options.live_only,
        decision_trigger=options.decision_trigger, redis_trigger=options.redis_trigger)
