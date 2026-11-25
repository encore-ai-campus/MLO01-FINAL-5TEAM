"""Read-only local dashboard for the Kafka–Spark–Redis/Delta test pipeline."""
import argparse
import json
import math
import re
import subprocess
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import urlopen

import redis
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

ROOT = Path(__file__).resolve().parents[1]
PAGE = Path(__file__).with_name('dashboard.html')
PROFILES = {
    'cluster': ('127.0.0.1:39291,127.0.0.1:39292,127.0.0.1:39293', 36589, 38089),
    'split': ('127.0.0.1:39192', 36479, None),
}
RUN_ID = re.compile(r'[a-z0-9][a-z0-9-]{0,160}\Z')
DECODER = json.JSONDecoder()
RECENT_ACTIONS = 100


def consumer(bootstrap):
    return KafkaConsumer(bootstrap_servers=bootstrap, group_id=None,
                         enable_auto_commit=False, request_timeout_ms=4000,
                         session_timeout_ms=2500, heartbeat_interval_ms=800,
                         api_version_auto_timeout_ms=1700)


def topic_bounds(client, topic):
    partitions = client.partitions_for_topic(topic)
    if partitions is None:
        return None
    references = [TopicPartition(topic, number) for number in sorted(partitions)]
    starts = client.beginning_offsets(references)
    ends = client.end_offsets(references)
    return {str(p.partition): [starts[p], ends[p]] for p in references}


def recent_actions(client, topic, bounds):
    """Read a bounded tail without committing offsets or joining a consumer group."""
    ends = {TopicPartition(topic, int(partition)): end for partition, (start, end) in bounds.items() if end > start}
    if not ends:
        return []
    client.assign(list(ends))
    positions = {}
    for partition, end in ends.items():
        start = max(bounds[str(partition.partition)][0], end - RECENT_ACTIONS)
        client.seek(partition, start)
        positions[partition] = start
    rows = []
    deadline = time.monotonic() + 1.2
    while time.monotonic() < deadline and any(positions[part] < end for part, end in ends.items()):
        for partition, records in client.poll(timeout_ms=200, max_records=500).items():
            for record in records:
                positions[partition] = record.offset + 1
                if record.offset >= ends[partition]:
                    break
                try:
                    event = json.loads(record.value)
                except (ValueError, TypeError):
                    event = {}
                if not isinstance(event, dict):
                    event = {}
                rows.append({'partition': record.partition, 'offset': record.offset, 'received_ms': record.timestamp,
                             **{field: event.get(field) for field in (
                                 'event_id', 'event_type', 'event_time', 'user_id', 'product_id', 'source')}})
    return sorted(rows, key=lambda row: (row['received_ms'], row['partition'], row['offset']),
                  reverse=True)[:RECENT_ACTIONS]


def log_tail(path, size=786432):
    try:
        with path.open('rb') as stream:
            length = stream.seek(0, 2)
            stream.seek(max(0, length - size))
            if length > size:
                stream.readline()
            raw = stream.read()
    except OSError:
        return []
    return raw.decode('utf-8', errors='replace').rpartition('\n')[0].splitlines()


def log_files(root):
    for area in root.iterdir():
        if not area.is_dir():
            continue
        yield from area.glob('*.log')
        for run in area.iterdir():
            if run.is_dir():
                yield from run.glob('*.log')


def log_marker(line, marker):
    position = line.find(marker)
    if position < 0:
        return None
    try:
        return DECODER.raw_decode(line, position + len(marker))[0]
    except (ValueError, TypeError):
        return None


def read_progress(paths, run):
    progress, redis_metrics = {}, []
    replay = None
    for path in paths:
        if not path.name.startswith(run + '-') or path.name.endswith('-inspect.log'):
            continue
        points = []
        for line in log_tail(path):
            item = log_marker(line, 'DATA07_SPLIT ')
            if item and item.get('stage') in ('decision', 'redis', 'delta'):
                stage = item['stage']
                offsets = item['end_offsets']
                item['end_offsets'] = json.loads(offsets) if isinstance(offsets, str) else offsets
                if item['committed_ms'] >= progress.get(stage, {}).get('committed_ms', 0):
                    progress[stage] = item
            point = log_marker(line, 'DATA06_LATENCY ')
            if isinstance(point, list) and len(point) == 4:
                points.append(point)
            if line.startswith('{'):
                clock = log_marker(line, '')
                if clock and clock.get('replay_time'):
                    timestamp = path.stat().st_mtime
                    if not replay or timestamp >= replay['modified']:
                        replay = {**clock, 'modified': timestamp}
        redis_metrics.extend(points[-250:])
    return progress, sorted(redis_metrics, key=lambda point: point[3])[-250:], replay


def stage_position(topic, bounds, progress=None):
    if bounds is None:
        return {'total': None, 'pending': None}
    if progress is None:
        return {'total': sum(end - start for start, end in bounds.values()), 'pending': None}
    if topic not in progress['end_offsets']:
        return {'total': None, 'pending': None}
    positions = progress['end_offsets'][topic]
    return {
        'total': sum(max(0, min(end, positions.get(partition, start)) - start)
                     for partition, (start, end) in bounds.items()),
        'pending': sum(max(0, end - max(start, positions.get(partition, start)))
                       for partition, (start, end) in bounds.items()),
    }


def checkpoint_view(topic, bounds, progress):
    positions = (progress or {}).get('end_offsets', {}).get(topic, {})
    bounds = bounds or {}
    rows = []
    for partition in sorted(bounds.keys() | positions.keys(), key=int):
        start, end = bounds.get(partition, (None, None))
        offset = positions.get(partition)
        pending = None if end is None or offset is None or offset > end else max(0, end - max(start, offset))
        rows.append({'partition': partition, 'next_offset': offset, 'kafka_end': end, 'pending': pending,
                     'before_retention': offset is not None and start is not None and offset < start})
    return {'topic': topic, 'partitions': rows, 'pending': sum(row['pending'] for row in rows)
            if rows and all(row['pending'] is not None for row in rows) else None,
            'last_commit_ms': (progress or {}).get('committed_ms'), 'batch': (progress or {}).get('batch')}


def recovery_receipt(paths, run, topic):
    if not run:
        return None
    recent = sorted((path for path in paths if path.parent.name in (run, run.removesuffix('-check'))
                     and ('restore' in path.stem or 'recover' in path.stem)),
                    key=lambda path: path.stat().st_mtime, reverse=True)
    for path in recent:
        for line in reversed(log_tail(path)):
            item = log_marker(line, 'REDIS_RECOVERY ')
            if isinstance(item, dict) and item.get('run_id') == run and item.get('refined_topic') == topic:
                return {key: item[key] for key in ('boundary', 'restart_offsets', 'restored_user_keys') if key in item}
    return None


def quantiles(points):
    values = sorted(point[3] - point[0] for point in points if point[3] >= point[0])
    if not values:
        return {'p95': None, 'p99': None, 'count': 0}
    return {**{f'p{p}': values[math.ceil(len(values) * p / 100) - 1] for p in (95, 99)},
            'count': len(values)}


def service_health(redis_port, spark_port):
    result = {'redis': False, 'spark_workers': None}
    try:
        with redis.Redis(host='127.0.0.1', port=redis_port, socket_timeout=0.8,
                         socket_connect_timeout=0.8) as client:
            result['redis'] = client.ping()
    except (redis.RedisError, OSError):
        pass
    if spark_port:
        try:
            with urlopen(f'http://127.0.0.1:{spark_port}/json/', timeout=1) as stream:
                result['spark_workers'] = sum(worker['state'] == 'ALIVE' for worker in json.load(stream)['workers'])
        except (OSError, URLError, ValueError, KeyError):
            pass
    return result


def running_jobs(run):
    if not run:
        return False
    try:
        output = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], capture_output=True,
                                text=True, timeout=2, check=True).stdout
        return any(name.startswith(run + '-') for name in output.splitlines())
    except (OSError, subprocess.SubprocessError):
        return None


def status(profile, run, root):
    bootstrap, redis_port, spark_port = PROFILES[profile]
    files = list(log_files(root))
    topics = set()
    runs = []
    source = refined = None
    events = []
    event_error = False
    kafka_online = False
    try:
        with closing(consumer(bootstrap)) as client:
            topics = client.topics()
            supported = {topic.removesuffix('-actions') for topic in topics if topic.endswith('-actions')}
            run_times = {}
            for path in files:
                if path.name.endswith(('-workload.log', '-before.log', '-warm.log')):
                    candidate = path.stem.rsplit('-', 1)[0]
                    if candidate in supported:
                        run_times[candidate] = max(run_times.get(candidate, 0), path.stat().st_mtime)
            runs = sorted(supported, key=lambda name: (run_times.get(name, 0), name), reverse=True)
            if not run and runs:
                run = runs[0]
            if run:
                action, clean = run + '-actions', run + '-refined'
                source = topic_bounds(client, action) if action in topics else None
                refined = topic_bounds(client, clean) if clean in topics else None
                if source:
                    try:
                        events = recent_actions(client, action, source)
                    except KafkaError:
                        event_error = True
            kafka_online = True
    except (KafkaError, OSError):
        pass
    progress, latencies, clock = read_progress(files, run) if run else ({}, [], None)
    action, clean = (run or '') + '-actions', (run or '') + '-refined'
    stages = {'kafka': stage_position(action, source), 'refined': stage_position(clean, refined)}
    for stage, topic, bounds in (('decision', action, source), ('redis', clean, refined), ('delta', clean, refined)):
        item = progress.get(stage)
        stages[stage] = ({**stage_position(topic, bounds, item), 'last_commit_ms': item['committed_ms']}
                         if item else {'total': None, 'pending': None})
    if 'delta' in progress:
        stages['delta']['raw'] = stage_position(action, source, progress['delta'])
    checkpoints = {key: checkpoint_view(topic, bounds, progress.get(stage)) for key, stage, topic, bounds in (
        ('decision', 'decision', action, source), ('redis', 'redis', clean, refined),
        ('delta_raw', 'delta', action, source), ('delta_refined', 'delta', clean, refined),
    )} if run else {}
    active = running_jobs(run)
    return {'observed_ms': int(time.time() * 1000), 'profile': profile, 'run': run, 'runs': runs,
            'active': active, 'kafka': kafka_online, **service_health(redis_port, spark_port),
            'stages': stages, 'latency': quantiles(latencies),
            'latency_series': [[point[3], point[3] - point[0]] for point in latencies if point[3] >= point[0]],
            'replay': clock, 'events': events, 'event_limit': RECENT_ACTIONS, 'event_error': event_error,
            'checkpoints': checkpoints, 'redis_recovery': recovery_receipt(files, run, clean)}


def peak_reference(root):
    """Return the latest completed full-day replay that recorded hourly latency."""
    reports = (root / 'data-direct-day-check').glob('*/verification.json')
    for path in sorted(reports, key=lambda file: file.stat().st_mtime, reverse=True):
        try:
            report = json.loads(path.read_text(encoding='utf-8'))
            if not report.get('complete') or report.get('pilot'):
                continue
            latency = report['latency']
            hourly = [{'start': hour, **value} for hour, value in sorted(latency['hourly'].items())]
            if len(hourly) != 24:
                continue
            peak = max(hourly, key=lambda row: row['events'])
            return {'run': path.parent.name, 'window_utc': report['window_utc'], 'speed': report['speed'],
                    'overall': latency['overall'], 'hours': hourly, 'peak': peak['start']}
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return None


def serve(port, root):
    class Handler(BaseHTTPRequestHandler):
        def respond(self, body, content_type, code=200):
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlsplit(self.path)
            if url.path == '/':
                self.respond(PAGE.read_bytes(), 'text/html; charset=utf-8')
                return
            if url.path == '/api/peak':
                self.respond(json.dumps(peak_reference(root)).encode(), 'application/json')
                return
            if url.path != '/api/status':
                self.respond(b'Not found', 'text/plain', 404)
                return
            query = parse_qs(url.query)
            profile = query.get('profile', ['split'])[0]
            run = query.get('run', [''])[0]
            if profile not in PROFILES or (run and not RUN_ID.fullmatch(run)):
                self.respond(b'Invalid profile or run ID', 'text/plain', 400)
                return
            self.respond(json.dumps(status(profile, run, root)).encode(), 'application/json')

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(('127.0.0.1', port), Handler) as server:
        print(f'Pipeline dashboard: http://127.0.0.1:{port}/', flush=True)
        server.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--log-root', type=Path, default=ROOT / 'data')
    args = parser.parse_args()
    if not args.log_root.is_dir():
        parser.error('Log root must be an existing directory')
    serve(args.port, args.log_root.resolve())
