import unittest

from ml.conversion_model.evaluation.session_metrics import evaluate


def prediction(session, sequence, label):
    return {
        'prediction_id': f'{session}-{sequence}',
        'session_id': str(session),
        'event_time_utc': '2019-10-27T12:00:00Z',
        'sequence': sequence,
        'label': label,
        'cart_count': label,
    }


class PurchaseEvaluationTest(unittest.TestCase):
    def test_each_session_has_equal_weight_and_all_models_use_same_rows(self):
        # Ten positive sessions have one prediction each and ten negatives have three.
        rows = [prediction(i, j, int(i < 10)) for i in range(20) for j in range(1 if i < 10 else 3)]
        fixed = {row['prediction_id']: 0.5 for row in rows}
        tied = {}
        for row in rows:
            session = int(row['session_id'])
            tied[row['prediction_id']] = 0.8 if session < 5 or 10 <= session < 15 else (
                0.3 if row['label'] else 0.1)
        report = evaluate(rows, {'logistic': fixed, 'lightgbm': tied}, cart_rates_without_with=(0.2, 0.8))
        self.assertEqual((report['prediction_rows'], report['sessions'], report['top_10pct_slots']), (40, 20, 2))
        self.assertAlmostEqual(report['models']['logistic']['session_weighted_ap'], 0.5)
        self.assertAlmostEqual(report['models']['logistic']['session_weighted_brier'], 0.25)
        self.assertAlmostEqual(report['models']['logistic']['top_10pct_precision'], 0.5)
        self.assertAlmostEqual(report['models']['lightgbm']['session_weighted_ap'], 0.5 * 5 / 10 + 0.5 * 10 / 15)
        self.assertAlmostEqual(report['models']['cart']['session_weighted_ap'], 1)
        self.assertAlmostEqual(report['models']['cart']['session_weighted_brier'], 0.04)

    def test_top_uses_first_prediction_and_calibration_counts_boundaries(self):
        rows = [prediction(0, 2, 0), prediction(0, 1, 0), prediction(1, 1, 1), prediction(2, 1, 0)]
        scores = {'0-2': 1.0, '0-1': 0.1, '1-1': 0.3, '2-1': 0.0}
        model = evaluate(rows, {'logistic': scores}, cart_rates_without_with=(0.1, 0.3))['models']['logistic']
        self.assertEqual(model['top_10pct_precision'], 1.0)
        self.assertIsNone(model['session_weighted_ap'])
        self.assertEqual([band['rows'] for band in model['calibration_per_prediction']], [1, 1, 1, 0, 1])
        self.assertEqual(model['calibration_per_prediction'][2]['purchase_rate'], 1.0)
        self.assertIsNone(model['calibration_per_prediction'][3]['mean_probability'])

    def test_rejects_misaligned_predictions_and_ambiguous_order(self):
        row = prediction(1, 1, 1)
        for rows, scores, error in (
            ([row], {'extra': 0.5}, 'scores must match'),
            ([row, row], {row['prediction_id']: 0.5}, 'unique'),
            ([row, {**row, 'prediction_id': 'other'}], {}, 'Ambiguous'),
            ([row], {row['prediction_id']: float('nan')}, 'finite probability'),
        ):
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                evaluate(rows, {'logistic': scores}, cart_rates_without_with=(0.1, 0.3))

    def test_empty_sample_never_reports_metrics(self):
        report = evaluate([], {'logistic': {}}, cart_rates_without_with=(0.1, 0.3))
        self.assertEqual(report['sessions'], 0)
        for model in report['models'].values():
            self.assertIsNone(model['session_weighted_ap'])
            self.assertIsNone(model['session_weighted_brier'])
            self.assertIsNone(model['top_10pct_precision'])

    def test_rejects_conflicting_purchase_labels_in_a_session(self):
        rows = [prediction(1, 1, 0), prediction(1, 2, 1)]
        scores = {'1-1': 0.1, '1-2': 0.9}
        for order in (rows, list(reversed(rows))):
            with self.subTest(first_label=order[0]['label']), self.assertRaisesRegex(
                ValueError, 'Conflicting purchase labels in session 1'
            ):
                evaluate(order, {'logistic': scores}, cart_rates_without_with=(0.1, 0.3))
