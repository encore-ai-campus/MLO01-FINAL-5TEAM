"""Join local prediction logs to mature visits and timestamped purchases."""
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .core import Event
from .store import checksum


def read_visits(folder):
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    path = folder / 'visit_outcomes.parquet'
    expected = manifest['files'][path.name]
    if checksum(path) != expected['sha256']:
        raise ValueError('Visit Parquet checksum differs')
    table = pq.read_table(path)
    visits = {row['visit_id']: row for row in table.to_pylist()}
    if not table.num_rows == len(visits) == expected['rows']:
        raise ValueError('Visit rows or keys differ from manifest')
    return visits, manifest


def visit_events(connection, visit):
    rows = [Event(*row) for row in connection.execute(
        'SELECT * FROM events WHERE user_id=? AND ts BETWEEN ? AND ? ORDER BY ts,source,row_no',
        (visit['user_id'], int(visit['started_at'].timestamp()), int(visit['last_event_at'].timestamp())))]
    start_id = visit['visit_id'].split(':', 1)[1]
    if not rows or rows[0].event_id != start_id or len(rows) != visit['event_count']:
        raise ValueError(f"Event history differs from visit {visit['visit_id']}")
    if visit['is_closed']:
        known = sorted({row.item for row in rows if row.action == 2})
        if known != visit['purchased_product_ids'] or bool(known) != visit['purchased']:
            raise ValueError(f"Purchase history differs from visit {visit['visit_id']}")
    return rows


def replay_timestamp(value):
    parsed = datetime.fromisoformat(value)
    # Spark's local Delta inspect uses an explicitly UTC SQL session.
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def classify(log, predicted, visit, events):
    if log['source'] != 'replay':
        return None, 'excluded', log['source'], None
    if visit is None:
        return None, 'pending', 'visit_not_found', None
    if log['user_id'] != str(visit['user_id']):
        return None, 'excluded', 'user_mismatch', None
    if not visit['started_at'] <= predicted <= visit['watermark']:
        return None, 'excluded', 'outside_observed_visit', None
    if not visit['is_closed']:
        return None, 'pending', 'visit_open', None
    if predicted >= visit['closed_at']:
        return None, 'excluded', 'outside_observed_visit', None
    purchased = [datetime.fromtimestamp(row.ts, timezone.utc) for row in events if row.action == 2]
    if predicted in purchased:
        return None, 'excluded', 'purchase_at_prediction_time', None
    future = next((at for at in purchased if at > predicted), None)
    return int(future is not None), 'final', None, future


def build_labels(logs, visits, database):
    result = []
    keys = set()
    histories = {}
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as connection:
        for log in logs:
            if log['log_type'] != 'prediction':
                continue
            key = (log['replay_run_id'], log['prediction_id'])
            if not all(key) or key in keys:
                raise ValueError(f'Missing or duplicate prediction key: {key}')
            keys.add(key)
            visit_id = log['session_id']
            visit = visits.get(visit_id)
            predicted = replay_timestamp(log['replay_time'])
            eligible = (log['source'] == 'replay' and visit and visit['is_closed']
                        and log['user_id'] == str(visit['user_id'])
                        and visit['started_at'] <= predicted < visit['closed_at'])
            if eligible and visit_id not in histories:
                histories[visit_id] = visit_events(connection, visit)
            label, state, reason, purchase = classify(log, predicted, visit, histories.get(visit_id, ()))
            result.append({
                'replay_run_id': key[0], 'prediction_id': key[1], 'request_id': log['request_id'],
                'visit_id': visit_id, 'user_id': log['user_id'], 'source': log['source'],
                'predicted_at': predicted, 'model_version': log['model_version'],
                'prediction_probability': log['prediction_probability'],
                'label': label, 'status': state, 'reason': reason,
                'visit_closed_at': visit['closed_at'] if state == 'final' else None,
                'first_purchase_after_prediction_at': purchase,
            })
    return result


def write_labels(rows, output: Path):
    utc = pa.timestamp('us', tz='UTC')
    schema = pa.schema([
        *((field, pa.string()) for field in ('replay_run_id', 'prediction_id', 'request_id', 'visit_id', 'user_id', 'source')),
        ('predicted_at', utc), ('model_version', pa.string()), ('prediction_probability', pa.float64()),
        ('label', pa.int8()), ('status', pa.string()), ('reason', pa.string()),
        ('visit_closed_at', utc), ('first_purchase_after_prediction_at', utc),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, output)
    if pq.read_table(output).to_pylist() != table.to_pylist():
        raise RuntimeError('Prediction label Parquet readback differs')
