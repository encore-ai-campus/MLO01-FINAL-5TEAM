import unittest

from reco.evaluate_cumulative import paired_ci
from reco.pipeline import evaluate


class RecommendationEvaluationTests(unittest.TestCase):
    protocol = {'bootstrap_repeats': 100, 'bootstrap_unit': 'user', 'seed': 42}

    def scored(self, request_id, user, *, has_truth=True):
        return {'request_id': request_id, 'user': user, 'has_truth': has_truth, 'ndcg': 0.5}

    def test_ranking_and_baseline_keep_expected_metrics_on_same_sample(self):
        items = list(range(1, 13))
        queries = [
            {'request_id': request_id, 'user': user, 'items': items, 'truth': truth,
             'popular': popular, 'exclusions': {}, 'sources': dict.fromkeys(items, ['cf'])}
            for request_id, user, truth, popular in (
                ('view-1', 101, {1}, [20, 21]),
                ('cart-2', 102, {12}, [12, 20]),
                ('view-3', 103, set(), [20, 21]),
            )
        ]
        scores = [list(range(12, 0, -1))] * len(queries)
        candidate_metrics, candidate_rows = evaluate(queries, 'ranker', scores)
        baseline_metrics, baseline_rows = evaluate(queries, 'popular')
        for metrics in (candidate_metrics, baseline_metrics):
            self.assertEqual(metrics['queries_all_mature'], 3)
            self.assertEqual(metrics['queries_with_eligible_future_purchase'], 2)
            self.assertEqual(metrics['purchase_candidate_recall_at_100'], 1.0)
            self.assertEqual(metrics['purchase_recall_at_10'], 0.5)
            self.assertEqual(metrics['purchase_ndcg_at_10'], 0.5)
        self.assertEqual(
            paired_ci(candidate_rows, baseline_rows, 'ndcg', self.protocol),
            {'unit': 'user', 'users': 2, 'mean_delta': 0.0, 'ci95': [-1.0, 1.0]},
        )

    def test_legacy_settings_record_user_unit_even_when_interval_is_unavailable(self):
        legacy = {'bootstrap_repeats': 100, 'seed': 42}
        rows = [self.scored('view-1', 101)]
        self.assertEqual(
            paired_ci(rows, rows, 'ndcg', legacy),
            {'unit': 'user', 'users': 1, 'mean_delta': None, 'ci95': [None, None]},
        )

    def test_rejects_a_different_bootstrap_unit(self):
        with self.assertRaisesRegex(ValueError, 'resample by user'):
            paired_ci([], [], 'ndcg', {**self.protocol, 'bootstrap_unit': 'request'})

    def test_rejects_unequal_request_counts_in_either_direction(self):
        matched = [self.scored('view-1', 101)]
        longer = [*matched, self.scored('view-2', 102, has_truth=False)]
        for candidate, baseline in ((matched, longer), (longer, matched)):
            with self.subTest(candidate=len(candidate), baseline=len(baseline)):
                with self.assertRaises(ValueError):
                    paired_ci(candidate, baseline, 'ndcg', self.protocol)

    def test_third_place_and_separate_popularity_candidate_pool(self):
        query = {'request_id': 'cart-1', 'user': 101, 'items': [11, 12, 13],
                 'truth': {13}, 'exclusions': {}, 'sources': {p: ['cf'] for p in (11, 12, 13)}}
        popular_items = [11, 12]
        popularity = {**query, 'items': popular_items, 'popular': popular_items,
                      'sources': {p: ['popular'] for p in popular_items}}
        ranking, _ = evaluate([query], 'ranker', [[3, 2, 1]])
        baseline, _ = evaluate([popularity], 'popular')
        self.assertEqual(ranking['purchase_candidate_recall_at_100'], 1.0)
        self.assertEqual(ranking['purchase_recall_at_10'], 1.0)
        self.assertAlmostEqual(ranking['purchase_ndcg_at_10'], 0.5)
        for metric in ('purchase_candidate_recall_at_100', 'purchase_recall_at_10', 'purchase_ndcg_at_10'):
            self.assertEqual(baseline[metric], 0.0)

    def test_rejects_invalid_bootstrap_counts(self):
        for repeats in (0, 1, 2.5, True):
            with self.subTest(repeats=repeats), self.assertRaises(ValueError):
                paired_ci([], [], 'ndcg', {**self.protocol, 'bootstrap_repeats': repeats})

    def test_rejects_mismatched_request_id_or_user_or_purchase_truth(self):
        candidate = [self.scored('view-1', 101)]
        for other in (self.scored('cart-1', 101), self.scored('view-1', 102),
                      self.scored('view-1', 101, has_truth=False)):
            with self.subTest(other=other):
                with self.assertRaises(ValueError):
                    paired_ci(candidate, [other], 'ndcg', self.protocol)

    def test_rejects_duplicate_ids_even_for_requests_without_purchase_truth(self):
        for has_truth in (True, False):
            with self.subTest(has_truth=has_truth):
                rows = [self.scored('view-1', 101, has_truth=has_truth)] * 2
                with self.assertRaises(ValueError):
                    paired_ci(rows, rows, 'ndcg', self.protocol)
