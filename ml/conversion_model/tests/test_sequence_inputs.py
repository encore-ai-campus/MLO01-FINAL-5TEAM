import unittest
from types import SimpleNamespace

from libs.features.purchase_sequence import FEATURE_NAMES, sequence_inputs
from ml.conversion_model.models.logistic.features import sequence_inputs as model_sequence_inputs
from ml.conversion_model.models.logistic.features import visit_features


AT = '2019-11-01T00:02:00Z'


def action(product, kind, order, when=AT):
    return {'product_id': product, 'event_type': kind, 'sequence': order, 'event_time_utc': when}


class SequenceInputTests(unittest.TestCase):
    def test_agreed_example_matches_training_and_api_inputs(self):
        first = action('B', 'view', 1, '2019-10-31T23:59:00Z')
        second = action('D', 'cart', 2)
        observation = {'actions': [first, second], 'event_time_utc': AT, 'sequence': 2}
        training = model_sequence_inputs(observation['actions'], event_time=observation['event_time_utc'],
                                         sequence=observation['sequence'])
        serving = sequence_inputs([dict(second), dict(first)], event_time=AT, sequence=2)
        expected = dict(zip(FEATURE_NAMES, (2, 0, 1., 0, 'view_cart_other', 'AB'), strict=True))
        self.assertEqual(training, expected)
        self.assertEqual(serving, expected)
        self.assertEqual(tuple(type(v) for v in training.values()), (int, int, float, int, str, str))

    def test_model_adapter_preserves_legacy_integer_ids_without_mutating_actions(self):
        actions = [action(102, 'view', 1), action(104, 'cart', 2)]
        canonical = [action('102', 'view', 1), action('104', 'cart', 2)]
        expected = sequence_inputs(canonical, event_time=AT, sequence=2)
        self.assertEqual(model_sequence_inputs(actions, event_time=AT, sequence=2, legacy_ids=True), expected)
        self.assertEqual(actions[0]['product_id'], 102)
        with self.assertRaises(ValueError):
            model_sequence_inputs(actions, event_time=AT, sequence=2)
        with self.assertRaises(ValueError):
            model_sequence_inputs([action(True, 'view', 1)], event_time=AT, sequence=1, legacy_ids=True)

    def test_offline_visit_uses_the_same_input_calculation(self):
        actions = [action('B', 'view', 1), action('D', 'cart', 2), action('D', 'purchase', 3)]
        visit = SimpleNamespace(events=[SimpleNamespace(event_id=str(i), **row)
                                        for i, row in enumerate(actions)])
        training = visit_features(visit)
        self.assertEqual(set(training), {'0', '1'})
        self.assertEqual(training['1'], sequence_inputs(actions[:2], event_time=AT, sequence=2))

    def test_same_second_preserves_order_and_repeated_views(self):
        actions = [action('A', 'view', i) for i in (5, 3, 1, 4)] + [action('B', 'view', 2)]
        result = sequence_inputs(actions, event_time=AT, sequence=5)
        self.assertEqual(tuple(result.values()), (5, 3, .5, 1, 'view_view_same', 'AAA'))

    def test_first_action_and_same_product_cart(self):
        first = action('A', 'view', 1)
        self.assertEqual(tuple(sequence_inputs([first], event_time=AT, sequence=1).values()),
                         (1, 1, 0, 0, 'start', 'A'))
        same = [first, action('A', 'cart', 2)]
        self.assertEqual(tuple(sequence_inputs(same, event_time=AT, sequence=2).values()),
                         (2, 0, 0., 0, 'view_cart_same', 'AA'))

    def test_rejects_wrong_request_and_duplicate_order(self):
        first = action('A', 'view', 1)
        second = action('B', 'cart', 2)
        for actions, current in (([first, second], 1), ([first], 2),
                                 ([first, action('B', 'cart', 1)], 1)):
            with self.subTest(actions=actions, current=current), self.assertRaises(ValueError):
                sequence_inputs(actions, event_time=AT, sequence=current)

    def test_rejects_bad_action_contract(self):
        for actions in ([], [action('A', 'view', i) for i in range(6)],
                        [action('A', 'purchase', 1)], [action(101, 'view', 1)],
                        [action('A', 'view', 1, when='2019-11-01T00:02:00')],
                        [action('A', 'view', -1)]):
            with self.subTest(actions=actions), self.assertRaises(ValueError):
                sequence_inputs(actions, event_time=AT, sequence=1)
