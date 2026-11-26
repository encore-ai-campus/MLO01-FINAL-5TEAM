"""Exercise the team replay entry point with fixed local ranking cases."""

from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import numpy as np

from reco.core import FEATURES
from ml.rec_model.hybrid_replay import load_initial_ranker
from reco.team_replay import load_policy, replacement_result
from ml.rec_model.ranking_replay import recommend


REQUEST_TIME = 1572566400
CUTOFF = REQUEST_TIME - 2
VALIDATION_END = REQUEST_TIME - 2
DAY = 86400
RECEIPT = {'sample': 'team', 'manifest_sha256': 'fixture-manifest'}
RELATION_DIGEST = 'a' * 64
RANKER_DIGEST = 'b' * 64
RULES = 'cart_recent_purchase_popularity'
HYBRID = 'protected_interest_then_lightgbm'


class RecommendationRankingTest(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(folder)
        self.database = self.root / 'events.sqlite'
        self.model_config = self.root / 'model.json'
        self.model_config.touch()
        self.config = {**load_policy(), 'demo_model_config': self.root / 'missing.json'}
        self.ranker = Mock()
        self.ranker.predict.return_value = np.asarray([-100, -99, .2, .9, .9, .95])
        self.loader = self.enterContext(patch(
            'ml.rec_model.ranking_replay.load_initial_ranker', return_value=(self.ranker, (RANKER_DIGEST, VALIDATION_END)),
        ))
        self.relation_receipt = self.write_readiness(self.root / 'relation.json')
        self.hybrid_receipt = self.write_readiness(
            self.root / 'hybrid.json', ranker_ready='2019-10-31T23:59:59Z',
        )

    def write_readiness(self, path, *, relation_ready='2019-10-31T23:59:59Z', ranker_ready=None):
        records = {
            'relation': {'sha256': RELATION_DIGEST, 'record_id': 'relation-1', 'ready_at_utc': relation_ready},
        }
        if ranker_ready is not None:
            records['ranker'] = {'sha256': RANKER_DIGEST, 'record_id': 'ranker-1', 'ready_at_utc': ranker_ready}
        path.write_text(json.dumps(records), encoding='utf-8')
        return path

    def seed(self, events):
        with sqlite3.connect(self.database) as connection:
            connection.execute('''CREATE TABLE events (
                event_id TEXT PRIMARY KEY, ts INTEGER, row_no INTEGER, source TEXT,
                user_id INTEGER, item_id INTEGER, action INTEGER, category TEXT,
                brand TEXT, price REAL, raw_session TEXT)''')
            connection.executemany(
                'INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [(key, at, row, 'fixture.csv', user, item, action, category, '', None, '')
                 for row, (key, at, user, item, action, category) in enumerate(events)],
            )
        connection.close()

    def seed_rank_candidates(self):
        self.seed([
            ('old-purchase', REQUEST_TIME - 30 * DAY - 1, 1, 80, 2, 'c'),
            ('boundary-purchase', REQUEST_TIME - 30 * DAY, 1, 70, 2, 'c'),
            ('recent-purchase', REQUEST_TIME - 30 * DAY + 1, 1, 60, 2, 'c'),
            ('other-user-purchase', REQUEST_TIME - 1800, 2, 50, 2, 'c'),
            ('other-user-view', REQUEST_TIME - 1500, 2, 40, 0, 'c'),
            ('cart', REQUEST_TIME - 90, 1, 20, 1, 'c'),
            ('view', REQUEST_TIME - 20, 1, 30, 0, 'c'),
            ('request', REQUEST_TIME, 1, 10, 0, 'c'),
        ])
        return {10: [(60, .99), (40, .9), (50, .8), (70, .7), (80, .6), (10, .5)]}

    def replay(self, relations, ranking=None, model_config=None, on_features=None, *,
               readiness_file=None, cutoff=CUTOFF, receipt=RECEIPT):
        with patch('ml.rec_model.ranking_replay.snapshot', return_value=(relations, {'sha256': RELATION_DIGEST})):
            return recommend(
                self.database, self.config, self.root, receipt, cutoff, 'request', ranking,
                model_config=model_config, on_features=on_features, readiness_file=readiness_file,
            )

    def assert_order(self, result, items, mode):
        self.assertEqual(result['recommended_item_ids'], items)
        self.assertEqual(result['candidate_count'], len(items))
        self.assertEqual(result['ranking_mode'], mode)

    def test_protected_interest_then_lightgbm_with_item_id_tiebreak(self):
        scored_items = []

        def capture(_, query):
            scored_items.extend(query['items'])

        result = self.replay(self.seed_rank_candidates(), 'hybrid', self.model_config, capture,
                             readiness_file=self.hybrid_receipt)
        self.assert_order(result, [20, 30, 80, 50, 70, 40], HYBRID)
        self.assertEqual(scored_items, [20, 30, 40, 50, 70, 80])
        self.loader.assert_called_once_with(self.model_config, REQUEST_TIME, RECEIPT)
        self.ranker.predict.assert_called_once()
        self.assertEqual(self.ranker.predict.call_args.args[0].shape, (6, 18))

    def test_purchase_exclusion_expires_at_exactly_thirty_days(self):
        result = self.replay(self.seed_rank_candidates(), 'rules', readiness_file=self.relation_receipt)
        self.assert_order(result, [20, 30, 50, 40, 70, 80], RULES)
        self.loader.assert_not_called()

    def test_short_list_fills_from_popularity_and_known_catalog(self):
        self.seed([
            ('same-category-purchase', REQUEST_TIME - 90, 2, 301, 2, 'c'),
            ('other-category-purchase', REQUEST_TIME - 80, 3, 302, 2, 'other'),
            ('catalog-view', REQUEST_TIME - 70, 4, 303, 0, 'c'),
            ('request', REQUEST_TIME, 1, 300, 0, 'c'),
        ])
        result = self.replay({}, 'rules', readiness_file=self.relation_receipt)
        self.assert_order(result, [301, 302, 303], RULES)
        self.loader.assert_not_called()

    def test_empty_hybrid_list_does_not_call_predict(self):
        self.seed([('request', REQUEST_TIME, 1, 300, 0, 'c')])
        result = self.replay({}, 'hybrid', self.model_config, readiness_file=self.hybrid_receipt)
        self.assert_order(result, [], HYBRID)
        self.loader.assert_called_once()
        self.ranker.predict.assert_not_called()

    def test_keep_baseline_decision_uses_rule_order(self):
        baseline = {
            'queries_with_eligible_future_purchase': 100,
            'purchase_candidate_recall_at_100': .5,
            'purchase_recall_at_10': .5,
            'purchase_ndcg_at_10': .5,
        }
        self.assertEqual(replacement_result(baseline, {**baseline, 'purchase_ndcg_at_10': .49}, 100),
                         'keep_baseline')
        result = self.replay(self.seed_rank_candidates(), 'rules', self.model_config,
                             readiness_file=self.relation_receipt)
        self.assert_order(result, [20, 30, 50, 40, 70, 80], RULES)
        self.loader.assert_not_called()

    def test_default_replay_without_a_model_uses_rules_but_explicit_hybrid_requires_one(self):
        relations = self.seed_rank_candidates()
        result = self.replay(relations, readiness_file=self.relation_receipt)
        self.assert_order(result, [20, 30, 50, 40, 70, 80], RULES)
        with self.assertRaisesRegex(FileNotFoundError, '--model-config'):
            self.replay(relations, 'hybrid')
        with self.assertRaisesRegex(FileNotFoundError, 'does not exist'):
            self.replay(relations, model_config=self.root / 'missing.json')
        self.loader.assert_not_called()

    def test_model_config_does_not_switch_default_mode(self):
        self.config['demo_model_config'] = self.model_config
        relations = self.seed_rank_candidates()
        for path in (None, self.model_config):
            with self.subTest(explicit_config=path is not None):
                result = self.replay(relations, model_config=path, readiness_file=self.relation_receipt)
                self.assert_order(result, [20, 30, 50, 40, 70, 80], RULES)
        self.loader.assert_not_called()

    def test_team_requires_receipt_and_marks_local_without_one_unverified(self):
        relations = self.seed_rank_candidates()
        with self.assertRaisesRegex(ValueError, 'historical readiness records'):
            self.replay(relations)
        local = self.replay(relations, receipt={**RECEIPT, 'sample': 'local'})
        self.assertEqual(local['readiness'], {'status': 'unverified', 'missing': ['relation']})

    def test_relation_cutoff_at_or_after_request_is_rejected(self):
        relations = self.seed_rank_candidates()
        for cutoff in (REQUEST_TIME, REQUEST_TIME + 1):
            with self.subTest(cutoff=cutoff):
                with self.assertRaisesRegex(ValueError, 'strictly after the relation snapshot cutoff'):
                    self.replay(relations, cutoff=cutoff, readiness_file=self.relation_receipt)

    def test_readiness_one_second_before_request_is_accepted(self):
        result = self.replay(self.seed_rank_candidates(), 'hybrid', self.model_config,
                             readiness_file=self.hybrid_receipt)
        self.assertEqual(result['readiness']['status'], 'matched_supplied_records')
        self.assertEqual(result['readiness']['receipt_sha256'], sha256(self.hybrid_receipt.read_bytes()).hexdigest())
        self.assertEqual(set(result['readiness']['records']), {'relation', 'ranker'})

    def test_relation_and_ranker_ready_at_or_after_request_are_rejected(self):
        relations = self.seed_rank_candidates()
        for ready_at in ('2019-11-01T00:00:00Z', '2019-11-01T00:00:01Z'):
            with self.subTest(artifact='relation', ready_at=ready_at):
                path = self.write_readiness(self.relation_receipt, relation_ready=ready_at)
                with self.assertRaisesRegex(ValueError, 'relation must be ready'):
                    self.replay(relations, readiness_file=path)
            with self.subTest(artifact='ranker', ready_at=ready_at):
                path = self.write_readiness(self.hybrid_receipt, ranker_ready=ready_at)
                with self.assertRaisesRegex(ValueError, 'ranker must be ready'):
                    self.replay(relations, 'hybrid', self.model_config, readiness_file=path)
        self.ranker.predict.assert_not_called()

    def test_readiness_must_match_the_artifact_and_follow_its_cutoff(self):
        relations = self.seed_rank_candidates()
        path = self.write_readiness(self.relation_receipt, relation_ready='2019-10-31T23:59:57Z')
        with self.assertRaisesRegex(ValueError, 'relation must be ready'):
            self.replay(relations, readiness_file=path)
        path = self.write_readiness(self.relation_receipt)
        records = json.loads(path.read_bytes())
        records['relation']['sha256'] = RANKER_DIGEST
        path.write_text(json.dumps(records), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'Invalid readiness identity for relation'):
            self.replay(relations, readiness_file=path)

    def test_hybrid_requires_both_relation_and_ranker_records(self):
        relations = self.seed_rank_candidates()
        with self.assertRaisesRegex(ValueError, 'Expected readiness records for relation, ranker'):
            self.replay(relations, 'hybrid', self.model_config, readiness_file=self.relation_receipt)
        self.ranker.predict.assert_not_called()

    def test_model_config_checks_replay_identity_hash_time_and_feature_order(self):
        content = b'fixed ranking model\n'
        digest = sha256(content).hexdigest()
        (self.root / 'ranker.txt').write_bytes(content)
        (self.root / 'report.json').write_text(json.dumps({
            'manifest_sha256': RECEIPT['manifest_sha256'],
            'early_stopping_window': ['2019-10-25T00:00:00Z', '2019-10-31T23:59:58Z'],
            'model_sha256': digest,
        }), encoding='utf-8')
        self.model_config.write_text(json.dumps({
            'evaluation': {'sample': 'team', 'report_file': 'report.json'},
            'ranking': {'model_file': 'ranker.txt', 'model_sha256': digest},
        }), encoding='utf-8')
        self.ranker.feature_name.return_value = FEATURES
        with patch('ml.rec_model.hybrid_replay.lgb.Booster', return_value=self.ranker) as booster:
            self.assertEqual(load_initial_ranker(self.model_config, REQUEST_TIME, RECEIPT),
                             (self.ranker, (digest, VALIDATION_END)))
            booster.assert_called_once_with(model_str=content.decode())
            with self.assertRaisesRegex(ValueError, 'different replay input'):
                load_initial_ranker(self.model_config, REQUEST_TIME, {**RECEIPT, 'manifest_sha256': 'other'})
            for request_time in (VALIDATION_END, VALIDATION_END - 1):
                with self.subTest(request_time=request_time):
                    with self.assertRaisesRegex(ValueError, 'end of ranker validation'):
                        load_initial_ranker(self.model_config, request_time, RECEIPT)
            self.ranker.feature_name.return_value = FEATURES[::-1]
            with self.assertRaisesRegex(ValueError, 'feature order'):
                load_initial_ranker(self.model_config, REQUEST_TIME, RECEIPT)
            (self.root / 'ranker.txt').write_bytes(content + b'changed')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                load_initial_ranker(self.model_config, REQUEST_TIME, RECEIPT)
