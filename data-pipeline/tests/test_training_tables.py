import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from reco.core import FEATURES
from reco.team_replay import FixedRelations, load_policy
from reco.training_tables import build_tables, export


class TrainingTablesTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.folder = Path(directory.name)
        self.database = self.folder / 'events.sqlite'
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute('CREATE TABLE events(event_id TEXT PRIMARY KEY, ts INTEGER, row_no INTEGER, source TEXT, '
                               'user_id INTEGER, item_id INTEGER, action INTEGER, category TEXT, brand TEXT, '
                               'price REAL, raw_session TEXT)')
            scenario = [
                ('catalog20', 9_800, 99, 20, 0, 12.0),
                ('catalog42', 9_801, 99, 42, 0, 15.0),
                ('request', 10_000, 1, 10, 0, 11.0),
                ('same_second_purchase', 10_000, 1, 20, 2, 19.0),
                ('after_request_metadata', 10_003, 99, 20, 0, 100.0),
                ('no_purchase_request', 10_020, 2, 10, 0, 11.0),
                ('other_visit_purchase', 11_821, 2, 42, 2, 15.0),
                ('exact_boundary', 13_200, 4, 10, 0, 11.0),
                ('pending_request', 14_000, 3, 10, 0, 11.0),
                ('pending_purchase', 14_400, 3, 20, 2, 19.0),
                ('coverage', 16_000, 99, 10, 0, 11.0),
            ]
            connection.executemany('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)', [
                (name, ts, order, 'fixture', user, item, action, 'category', 'brand', price, 'raw')
                for order, (name, ts, user, item, action, price) in enumerate(scenario)
            ])
        self.config = load_policy()
        self.timeline = FixedRelations({10: [(20, .9), (42, .8)]}, 9_999)

    def build(self, watermark=15_000):
        return build_tables(self.database, 10_000, 14_500, watermark, self.timeline, self.config)

    def test_only_closed_visits_get_labels_with_parquet_roundtrip(self):
        training, visits, report = self.build()
        visits = {row['visit_id']: row for row in visits}
        self.assertEqual(report['open_visits'], 2)
        self.assertIsNone(visits['3:pending_request']['purchased'])
        self.assertIsNone(visits['3:pending_request']['purchased_product_ids'])
        self.assertIsNone(visits['4:exact_boundary']['closed_at'])
        self.assertFalse(visits['2:no_purchase_request']['purchased'])
        self.assertEqual(visits['1:request']['purchased_product_ids'], [20])
        labels = {(row['request_event_id'], row['candidate_product_id']): row['purchase_label'] for row in training}
        self.assertEqual(labels['request', 20], 1)
        self.assertEqual(labels['no_purchase_request', 42], 0)
        self.assertFalse({'pending_request', 'exact_boundary'} & {key[0] for key in labels})
        at_next_second = {row['visit_id']: row for row in self.build(15_001)[1]}
        self.assertTrue(at_next_second['4:exact_boundary']['is_closed'])
        self.assertFalse(at_next_second['4:exact_boundary']['purchased'])
        destination = self.folder / 'export'
        export(training, list(visits.values()), destination, {'files': {}})
        parquet = pq.read_table(destination / 'training_features.parquet')
        self.assertEqual([f.name for f in parquet.schema if f.type == pa.float32()], FEATURES)
        self.assertEqual(parquet.num_rows, len(training))
        self.assertEqual(pq.read_table(destination / 'visit_outcomes.parquet')['purchased'].null_count, 2)

    def test_changing_future_purchase_and_metadata_changes_label_only(self):
        def earlier_rows():
            return [row for row in self.build()[0] if row['request_event_id'] == 'request']

        before = earlier_rows()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("UPDATE events SET action=0, brand='changed', price=500 "
                               "WHERE event_id IN ('same_second_purchase', 'after_request_metadata')")
        after = earlier_rows()
        self.assertTrue(before)
        self.assertEqual(sum(row['purchase_label'] for row in before), 1)
        self.assertEqual(sum(row['purchase_label'] for row in after), 0)
        drop_label = lambda rows: [{k: v for k, v in row.items() if k != 'purchase_label'} for row in rows]
        self.assertEqual(drop_label(before), drop_label(after))

    def test_watermark_requires_covered_input(self):
        with self.assertRaisesRegex(ValueError, 'exceeds observed'):
            self.build(16_001)
