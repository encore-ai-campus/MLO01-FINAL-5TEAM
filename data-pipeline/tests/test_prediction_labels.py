import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from reco.prediction_labels import build_labels


class PredictionLabelsTests(unittest.TestCase):
    def test_a_later_visit_purchase_never_labels_earlier_visit(self):
        def at(seconds):
            return datetime.fromtimestamp(seconds, timezone.utc)

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'events.sqlite'
            with closing(sqlite3.connect(database)) as db, db:
                db.execute('CREATE TABLE events(event_id TEXT, ts INT, row_no INT, source TEXT, '
                           'user_id INT, item_id INT, action INT, category TEXT, brand TEXT, price REAL, raw_session TEXT)')
                db.executemany('INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', [
                    ('first', 100, 1, 'test', 7, 11, 0, 'cat', 'brand', 5, ''),
                    ('second', 2_000, 2, 'test', 7, 12, 0, 'cat', 'brand', 5, ''),
                    ('purchased', 2_010, 3, 'test', 7, 12, 2, 'cat', 'brand', 5, ''),
                    ('third', 6_000, 4, 'test', 7, 13, 0, 'cat', 'brand', 5, ''),
                    ('early-purchase', 6_010, 5, 'test', 7, 13, 2, 'cat', 'brand', 5, ''),
                ])
            visits = {}
            for name, start, end, closed, count, ids in [
                    ('first', 100, 100, 1901, 1, []), ('second', 2_000, 2_010, 3811, 2, [12]),
                    ('third', 6_000, 6_010, None, 2, None)]:
                visit_id = '7:' + name
                visits[visit_id] = {'visit_id': visit_id, 'user_id': 7, 'started_at': at(start),
                                    'last_event_at': at(end), 'closed_at': at(closed) if closed else None,
                                    'watermark': at(6_100), 'is_closed': closed is not None, 'event_count': count,
                                    'purchased_product_ids': ids, 'purchased': bool(ids) if closed else None}
            common = {'replay_run_id': 'test', 'log_type': 'prediction', 'source': 'replay',
                      'model_version': 'test', 'prediction_probability': .5, 'user_id': '7'}
            cases = [dict(common, request_id=name, prediction_id=name, session_id='7:' + name,
                          replay_time=at(start).isoformat()) for name, start in
                     [('first', 100), ('second', 2_000), ('third', 6_000)]]
            rows = build_labels(cases, visits, database)
        self.assertEqual([row['label'] for row in rows], [0, 1, None])
        self.assertEqual([row['status'] for row in rows], ['final', 'final', 'pending'])
        self.assertEqual(rows[2]['reason'], 'visit_open')
