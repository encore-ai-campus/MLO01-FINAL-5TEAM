import json
import sqlite3
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import lightgbm as lgb
import numpy as np

from reco import core, prior_interest
from reco.core import Event, FEATURES, timestamp
from reco.team_replay import load_policy

from ml.rec_model import ranking_replay, ranking_training
from ml.rec_model.input_contract import checksum, training_receipt
from ml.rec_model.price_policy import PRICE_POLICY, PriceState, request_candidates


CONTRACT_FEATURES = [
    'cf_score', 'cf_rank', 'source_cf', 'source_session', 'source_popular',
    'session_viewed', 'session_carted', 'session_views', 'session_carts',
    'session_elapsed_log', 'session_actions_log', 'category_affinity', 'brand_affinity',
    'price_distance', 'price_missing', 'purchase_popularity_log', 'last_item_gap_log',
    'history_size_log',
]


class TrainingTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.db = self.root / 'events.sqlite'
        self.manifest = self.db.with_suffix('.manifest.json')
        self.sample = self.root / 'sample.json'
        self.train_start = timestamp(ranking_training.TRAIN_START)
        self.valid_start = timestamp(ranking_training.VALID_START)

    def make_sample(self, *, train_requests=12, validation_requests=6):
        events = [(self.train_start - 120, 99, item, 0, float(item)) for item in (10, 20, 30, 40)]
        partitions = ((self.train_start, range(1, 1 + train_requests)),
                      (self.valid_start, range(100, 100 + validation_requests)))
        for start, users in partitions:
            for user in users:
                events.extend([(start + 60, user, 10, 1, 0.),
                               (start + 90, user, 20, 2, 20.)])
        events.append((self.valid_start + 4000, 99, 30, 2, 30.))
        with closing(sqlite3.connect(self.db)) as connection, connection:
            connection.execute('''CREATE TABLE events (
                event_id TEXT PRIMARY KEY, ts INTEGER, row_no INTEGER, source TEXT, user_id INTEGER,
                item_id INTEGER, action INTEGER, category TEXT, brand TEXT, price REAL, raw_session TEXT
            )''')
            connection.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)', [
                (f'e{row}', ts, row, 'fixture.csv.gz', user, item, action, 'unknown', 'unknown', price, '')
                for row, (ts, user, item, action, price) in enumerate(events)
            ])
        users = sorted({str(e[1]) for e in events})
        contents = ('\n'.join(users) + '\n').encode('utf-8')
        (self.root / 'sample-users.txt').write_bytes(contents)
        identity = {'version': 'fixture-2pct-v1', 'basis_points': 200, 'users': len(users),
                    'user_list_sha256': sha256(contents).hexdigest()}
        self.sample.write_text(json.dumps({'version': identity['version'],
                                          'tiers': [{**identity, 'users_file': 'sample-users.txt'}]}),
                               encoding='utf-8')
        self.export = {'complete': True, 'sample_rows': len(events), 'sample_users': len(users),
                       'event_db_sha256': checksum(self.db),
                       'common_sample': {**identity, 'manifest_sha256': checksum(self.sample)}}
        self.write_export()
        return identity

    def write_export(self):
        self.manifest.write_text(json.dumps(self.export), encoding='utf-8')

    def test_price_zero_equals_null_and_preserves_other_inputs(self):
        def features(price):
            config = load_policy()
            state = PriceState(config)
            state.observe_live(Event('first', 1, 1, 'fixture', 2, 20, 0, 'unknown', 'unknown', 20., ''))
            state.observe_live(Event('view', 2, 2, 'fixture', 1, 20, 0, 'unknown', 'unknown', 20., ''))
            request = Event('cart', 3, 3, 'fixture', 1, 10, 1, 'unknown', 'unknown', price, '')
            state.observe_live(request)
            query = request_candidates(state, request, {10: [(20, .8)]}, (1, 1, 1))
            self.assertEqual(request.price, price)
            return query['items'], query['features']

        items, zero = features(0.)
        null_items, null = features(None)
        paid_items, paid = features(200.)
        self.assertEqual(items, null_items)
        self.assertEqual(items, paid_items)
        self.assertEqual(zero.shape, (len(items), 18))
        self.assertEqual(zero.dtype, np.float32)
        np.testing.assert_array_equal(zero, null)
        columns = [FEATURES.index('price_distance'), FEATURES.index('price_missing')]
        current = items.index(10)
        np.testing.assert_array_equal(zero[current, columns], [0., 1.])
        self.assertGreater(paid[current, columns[0]], 0.)
        self.assertEqual(paid[current, columns[1]], 0.)
        other = [i for i in range(18) if i not in columns]
        np.testing.assert_array_equal(zero[:, other], paid[:, other])

    def test_shared_sample_is_validated_before_training(self):
        expected = self.make_sample()
        self.assertEqual(training_receipt(self.db, 'team', self.sample)['team_sample'], expected)
        for sample_change in ({'version': 'other'}, {'users': 999}, {'user_list_sha256': '0' * 64}):
            with self.subTest(sample_change=sample_change):
                saved = self.export['common_sample']
                self.export['common_sample'] = {**saved, **sample_change}
                self.write_export()
                with self.assertRaisesRegex(ValueError, 'different shared user list'):
                    training_receipt(self.db, 'team', self.sample)
                self.export['common_sample'] = saved
        self.write_export()
        (self.root / 'sample-users.txt').write_text('1\n99\n', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'count or SHA-256'):
            training_receipt(self.db, 'team', self.sample)

    def test_selected_request_matches_replay_input_and_has_later_purchase(self):
        self.make_sample()
        policy = load_policy()
        config = {**policy, 'rank_train': [ranking_training.TRAIN_START, ranking_training.VALID_START],
                  'validation': [ranking_training.VALID_START, ranking_training.VALID_END],
                  'test': [ranking_training.VALID_END] * 2, 'future': [ranking_training.VALID_END] * 2,
                  'queries_per_session': 3,
                  'query_limits': {'rank_train': 100, 'validation': 100, 'test': 0, 'future': 0}}
        relations = {10: [(20, .8)]}
        timeline = type('FixtureTimeline', (), {'for_time': lambda _, ts: relations})()
        train, valid, _ = ranking_training.partition_queries(self.db, config, timeline)
        self.assertEqual((len(train), len(valid)), (12, 6))
        request = next(q for q in train if q['user'] == 1)
        self.assertEqual(request['truth'], {20})
        captured = []
        def capture(event, query):
            self.assertEqual(event.action, 1)
            captured.append(query)
        with patch.object(ranking_replay, 'snapshot', return_value=(relations, {'sha256': 'fixture'})):
            ranking_replay.recommend(self.db, config, self.root, {'sample': 'local'}, self.train_start,
                                     'e4', state_factory=PriceState, on_features=capture)
        self.assertEqual(request['items'], captured[0]['items'])
        np.testing.assert_array_equal(request['features'], captured[0]['features'])

    def test_actual_training_saves_reloads_in_korean_path_and_preserves_source(self):
        expected = self.make_sample(train_requests=40, validation_requests=12)
        original = checksum(self.db)
        bundle = self.root / '추천 모델 검수' / 'trial-ranker'
        original_fit = ranking_training.fit_ranker
        before_save = []

        def fit_and_capture(train, validation, config):
            ranker, vectors, fit = original_fit(train, validation, config)
            query = validation[0]
            scores = ranker.predict(query['features'], num_threads=1)
            before_save.append((query['features'], scores, query['items'].index(20)))
            return ranker, vectors, fit

        with patch.object(ranking_training, 'fit_ranker', side_effect=fit_and_capture):
            summary = ranking_training.train(self.db, bundle, 'team', self.sample)
        self.assertEqual(summary, json.loads((bundle / 'price-model.json').read_bytes()))
        self.assertEqual(summary['price_policy'], PRICE_POLICY)
        self.assertEqual(summary['training_input']['team_sample'], expected)
        self.assertEqual(summary['training_input']['event_db_sha256'], original)
        self.assertEqual(checksum(self.db), original)
        self.assertEqual(summary['fit']['groups'], 40)
        self.assertEqual(summary['fit']['rows'], 160)
        self.assertEqual(summary['fit']['validation_groups'], 12)
        self.assertEqual(summary['fit']['validation_rows'], 48)
        for key, value in ranking_training.input_code_receipt().items():
            self.assertEqual(summary[key], value)
        for name, digest in summary['checksums'].items():
            self.assertEqual(checksum(bundle / name), digest)
        model = lgb.Booster(model_str=(bundle / 'ranker.txt').read_text(encoding='utf-8'))
        self.assertEqual(model.feature_name(), CONTRACT_FEATURES)
        self.assertEqual(summary['features'], CONTRACT_FEATURES)
        vectors, scores, purchased_index = before_save[0]
        self.assertTrue(np.isfinite(scores).all())
        self.assertGreater(np.unique(scores).size, 1)
        self.assertGreater(scores[purchased_index], min(np.delete(scores, purchased_index)))
        np.testing.assert_array_equal(model.predict(vectors, num_threads=1), scores)
        with self.assertRaises(FileExistsError):
            ranking_training.train(self.db, bundle, 'team', self.sample)

    def test_failed_contract_does_not_leave_a_model(self):
        self.make_sample()
        self.export['event_db_sha256'] = '0' * 64
        self.write_export()
        output = self.root / 'invalid'
        with self.assertRaisesRegex(ValueError, 'checksum'):
            ranking_training.train(self.db, output, 'team', self.sample)
        self.assertFalse(output.exists())

    def test_code_version_tracks_features_and_candidates_independently_of_line_endings(self):
        original = ranking_training.input_code_receipt()
        for module in (core, prior_interest):
            source = Path(module.__file__)
            content = source.read_bytes().replace(b'\r\n', b'\n')
            self.assertEqual(original['input_code_sources'][module.__name__], sha256(content).hexdigest())
            alternate = self.root / 'alternate.py'
            with self.subTest(module=module.__name__), patch.object(module, '__file__', str(alternate)):
                alternate.write_bytes(content.replace(b'\n', b'\r\n'))
                self.assertEqual(ranking_training.input_code_receipt(), original)
                alternate.write_bytes(content + b'\n# changed calculation\n')
                changed = ranking_training.input_code_receipt()
                self.assertNotEqual(changed['input_code_version'], original['input_code_version'])
                self.assertNotEqual(changed['input_code_sources'][module.__name__],
                                    original['input_code_sources'][module.__name__])
