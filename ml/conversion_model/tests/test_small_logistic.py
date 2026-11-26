import json
import subprocess
import sys
from math import log
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import numpy as np

from ml.conversion_model.models.logistic.features import sequence_inputs
from ml.conversion_model.models.logistic.model import LogisticModel, matrix
from ml.conversion_model.models.logistic.training.fit import fit_logistic


def example(day, history):
    actions = [
        {'product_id': product, 'event_type': kind, 'sequence': sequence,
         'event_time_utc': f'2019-10-{day:02d}T12:{sequence:02d}:00Z'}
        for sequence, (product, kind) in enumerate(history)
    ]
    target = actions[-1]
    request = {'actions': actions, 'event_time': target['event_time_utc'], 'sequence': target['sequence']}
    return request, sequence_inputs(**request)


def small_rows():
    train_cases = [
        (1, [('A', 'view')], 0),
        (2, [('B', 'view'), ('B', 'view')], 0),
        (3, [('A', 'view'), ('C', 'cart')], 1),
        (4, [('B', 'view'), ('B', 'cart')], 1),
    ]
    validation_cases = [
        (25, [('P', 'view')], 0),
        (26, [('Q', 'view'), ('Q', 'view')], 0),
        (27, [('P', 'view'), ('R', 'cart')], 1),
        (28, [('Q', 'view'), ('Q', 'cart')], 1),
    ]
    train = [example(day, history)[1] for day, history, _ in train_cases]
    validation = [example(day, history) for day, history, _ in validation_cases]
    return train, [label for _, _, label in train_cases], validation, [label for _, _, label in validation_cases]


class SmallPurchaseModelTest(unittest.TestCase):
    def test_learn_save_reload_and_predict(self):
        directory = Path(self.enterContext(TemporaryDirectory()))
        path = directory / 'logistic.json'
        train, train_labels, validation, validation_labels = small_rows()
        validation_inputs = [row for _, row in validation]
        fit_logistic(matrix(train), np.asarray(train_labels), matrix(validation_inputs),
                     np.asarray(validation_labels), path, regularization=(.1,))
        model = LogisticModel.load(path)
        from_saved = []
        for (request, inputs) in validation:
            prediction = model.predict_actions(**request)
            self.assertEqual(prediction, model.predict(inputs))
            self.assertEqual(prediction, LogisticModel.load(path).predict(inputs))
            from_saved.append(prediction['purchase_probability'])
        self.assertLess(max(from_saved[:2]), min(from_saved[2:]))
        self.assertTrue(all(0. <= value <= 1. for value in from_saved))

        request_file = directory / 'prediction-input.json'
        request_file.write_text(json.dumps(validation_inputs[2]), encoding='utf-8')
        run = subprocess.run(
            [sys.executable, '-B', '-m', 'ml.conversion_model.models.logistic.predict',
             '--model', str(path), '--input', str(request_file)],
            cwd=Path(__file__).resolve().parents[3], capture_output=True, text=True, check=True,
        )
        self.assertEqual(json.loads(run.stdout), model.predict(validation_inputs[2]))

    def test_validation_with_one_label_saves_and_reloads(self):
        directory = Path(self.enterContext(TemporaryDirectory()))
        train, labels, validation, validation_labels = small_rows()
        for label in (0, 1):
            with self.subTest(label=label):
                inputs = [row for (_, row), actual in zip(validation, validation_labels, strict=True)
                          if actual == label]
                path = directory / f'model-{label}.json'
                bundle = fit_logistic(matrix(train), np.asarray(labels), matrix(inputs),
                                      np.full(len(inputs), label), path, regularization=(.1,))
                model = LogisticModel.load(path)
                probabilities = [model.predict(row)['purchase_probability'] for row in inputs]
                expected_loss = -sum(log(p if label == 1 else 1 - p) for p in probabilities) / len(inputs)
                self.assertAlmostEqual(bundle['validation_log_loss']['0.1'], expected_loss, places=12)

    def test_invalid_bundle_and_missing_input_fail(self):
        directory = Path(self.enterContext(TemporaryDirectory()))
        path = directory / 'invalid.json'
        path.write_text('{}', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'model format'):
            LogisticModel.load(path)
        train, labels, validation, valid_labels = small_rows()
        path = directory / 'model.json'
        fit_logistic(matrix(train), np.asarray(labels), matrix([row for _, row in validation]),
                     np.asarray(valid_labels), path, regularization=(.1,))
        row = dict(validation[0][1])
        del row['last_pair']
        with self.assertRaisesRegex(ValueError, 'six sequence features'):
            LogisticModel.load(path).predict(row)
