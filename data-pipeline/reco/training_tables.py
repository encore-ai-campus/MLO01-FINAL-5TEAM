"""Export local learning rows and watermark-aware visit outcomes to Parquet."""
import argparse
import json
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from .core import FEATURES, Event, State, generate, iso, session_close_at, targets
from .store import checksum
from .team_replay import FixedRelations, data_receipt, load_policy


def build_tables(database, start, end, watermark, timeline, config):
    """Replay the covered prefix, then attach labels using only visits closed by its watermark."""
    if not start < end <= watermark + 1:
        raise ValueError('The request window must fit within the inclusive input watermark')
    state = State(config)
    visits = {}
    requests = []
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as connection:
        latest, = connection.execute('SELECT MAX(ts) FROM events').fetchone()
        if latest is None or watermark > latest:
            raise ValueError('The watermark exceeds observed event-store coverage')
        cursor = connection.execute('SELECT * FROM events WHERE ts <= ? ORDER BY ts, source, row_no', (watermark,))
        for order, row in enumerate(cursor):
            event = Event(*row, order=order)
            state.observe_live(event)
            if start <= event.ts < end and event.session == f'{event.user}:{event.event_id}':
                visits[event.session] = []
            visit = visits.get(event.session)
            if visit is None:
                continue
            visit.append(event)
            if event.ts < end and event.action in (0, 1):
                query = generate(state, event, timeline.for_time(event.ts), config['request_weights'])
                requests.append((event, query['items'], query['features'], query['excluded']))

    outcomes = {}
    for visit_id, events in visits.items():
        close_at = session_close_at(events[-1].ts, config['session_gap_seconds'], config['session_gap_boundary'])
        closed = close_at <= watermark
        purchases = sorted({event.item for event in events if event.action == 2}) if closed else None
        outcomes[visit_id] = {
            'visit_id': visit_id, 'user_id': events[0].user, 'started_at': utc_time(events[0].ts),
            'last_event_at': utc_time(events[-1].ts), 'closed_at': utc_time(close_at) if closed else None,
            'watermark': utc_time(watermark), 'is_closed': closed, 'event_count': len(events),
            'purchased': bool(purchases) if closed else None, 'purchased_product_ids': purchases,
        }

    training = []
    closed_requests = 0
    for event, candidates, matrix, excluded in requests:
        if not outcomes[event.session]['is_closed']:
            continue
        closed_requests += 1
        relevant, _ = targets(visits[event.session], event, set(candidates), excluded)
        for item, values in zip(candidates, matrix, strict=True):
            training.append({
                'request_event_id': event.event_id, 'request_time': utc_time(event.ts),
                'request_source': event.source, 'request_event_seq': event.row_no,
                'visit_id': event.session, 'user_id': event.user, 'candidate_product_id': item,
                **{name: float(value) for name, value in zip(FEATURES, values, strict=True)},
                'purchase_label': int(item in relevant),
            })
    summary = {
        'visits': len(outcomes), 'closed_visits': sum(row['is_closed'] for row in outcomes.values()),
        'open_visits': sum(not row['is_closed'] for row in outcomes.values()),
        'closed_purchase_visits': sum(row['purchased'] is True for row in outcomes.values()),
        'closed_no_purchase_visits': sum(row['purchased'] is False for row in outcomes.values()),
        'requests_seen': len(requests), 'requests_closed': closed_requests,
        'requests_pending': len(requests) - closed_requests, 'requests_exported': len({
            row['request_event_id'] for row in training}),
        'positive_candidate_rows': sum(row['purchase_label'] for row in training),
        'candidate_rows': len(training),
    }
    return training, list(outcomes.values()), summary


def utc_time(seconds):
    return datetime.fromisoformat(iso(seconds))


def event_time(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None or parsed.microsecond:
        raise argparse.ArgumentTypeError('Use a timezone-aware ISO timestamp at whole-second precision')
    return int(parsed.timestamp())


def export(training, visits, destination, receipt):
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    utc = pa.timestamp('s', tz='UTC')
    tables = {
        'training_features.parquet': pa.Table.from_pylist(training, schema=pa.schema([
            ('request_event_id', pa.string()), ('request_time', utc), ('request_source', pa.string()),
            ('request_event_seq', pa.int64()), ('visit_id', pa.string()), ('user_id', pa.int64()),
            ('candidate_product_id', pa.int64()),
            *((name, pa.float32()) for name in FEATURES), ('purchase_label', pa.int8()),
        ])),
        'visit_outcomes.parquet': pa.Table.from_pylist(visits, schema=pa.schema([
            ('visit_id', pa.string()), ('user_id', pa.int64()), ('started_at', utc), ('last_event_at', utc),
            ('closed_at', utc), ('watermark', utc), ('is_closed', pa.bool_()), ('event_count', pa.int64()),
            ('purchased', pa.bool_()), ('purchased_product_ids', pa.list_(pa.int64())),
        ])),
    }
    with TemporaryDirectory(prefix='.training-', dir=destination.parent) as temp:
        staging = Path(temp) / 'export'
        staging.mkdir()
        for filename, table in tables.items():
            path = staging / filename
            pq.write_table(table, path)
            restored = pq.read_table(path)
            if restored.num_rows != table.num_rows or restored.to_pylist() != table.to_pylist():
                raise ValueError(f'Parquet readback differs for {filename}')
            receipt['files'][filename] = {'rows': table.num_rows, 'sha256': checksum(path)}
        (staging / 'manifest.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
        staging.rename(destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=Path, required=True)
    parser.add_argument('--cf-bundle', type=Path, required=True)
    parser.add_argument('--start', type=event_time, required=True)
    parser.add_argument('--end', type=event_time, required=True)
    parser.add_argument('--watermark', type=event_time, required=True,
                        help='The event source is complete through this inclusive event-time second.')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    receipt = data_receipt(args.events, 'local')
    bundle = json.loads((args.cf_bundle / 'manifest.json').read_text(encoding='utf-8'))
    relations_path = args.cf_bundle / 'neighbors.json'
    relations_hash = checksum(relations_path)
    if relations_hash != bundle['checksums']['neighbors.json'] or bundle['data_version'] != receipt['manifest_sha256']:
        raise ValueError('CF snapshot and data receipt do not match the selected input')
    snapshot, = bundle['retriever_snapshots']
    if snapshot['cutoff_ts'] > args.start:
        raise ValueError('The relation cutoff must precede or match the start of the request window')
    relations = {int(item): value for item, value in json.loads(relations_path.read_text(encoding='utf-8')).items()}
    timeline = FixedRelations(relations, snapshot['cutoff_ts'])
    config = load_policy()
    training, visits, summary = build_tables(args.events, args.start, args.end, args.watermark, timeline, config)
    receipt.update(
        request_window=[iso(args.start), iso(args.end)], watermark=iso(args.watermark), feature_order=FEATURES,
        feature_dtype='float32', retriever_version=bundle['retriever_version'], retriever_cutoff=iso(timeline.cutoff),
        retriever_sha256=relations_hash, session_gap_seconds=config['session_gap_seconds'],
        session_gap_boundary=config['session_gap_boundary'], coverage='declared inclusive watermark on frozen local input',
        scope='visit starts inside request window; requests are view/cart inside the window; base candidate policy',
        counts=summary, files={})
    export(training, visits, args.output, receipt)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
