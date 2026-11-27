"""Load and score the saved logistic model using its original feature order."""

import json
from hashlib import sha256
from math import isclose
from pathlib import Path

import numpy as np

from .features import (CATEGORIES, COLUMNS, CUMULATIVE_COLUMNS, CUMULATIVE_NUMERIC,
                       HISTORY_SIZE, NUMERIC, encode, possible_inputs, sequence_inputs)


DEFAULT_MODEL_PATH = Path(__file__).with_name('logistic.json')
MODEL_FORMAT = 'sequence-logistic-v1'
CUMULATIVE_FORMAT = 'sequence-logistic-v2'
SCHEMAS = {MODEL_FORMAT: (NUMERIC, COLUMNS), CUMULATIVE_FORMAT: (CUMULATIVE_NUMERIC, CUMULATIVE_COLUMNS)}
COUPON_MIN, COUPON_MAX = 0.10, 0.30


def matrix(rows, *, model_format=MODEL_FORMAT):
    numeric, _ = SCHEMAS[model_format]
    return np.asarray([encode(row, numeric=numeric) for row in rows], dtype=np.float64)


def score_logistic(bundle, values):
    normalized = values.copy()
    size = len(bundle['mean'])
    normalized[:, :size] = (normalized[:, :size] - bundle['mean']) / bundle['scale']
    logits = normalized @ bundle['coefficients'] + bundle['intercept']
    return np.exp(-np.logaddexp(0.0, -logits))


def validate_input(row):
    if not isinstance(row, dict) or any(key not in row for key in (*NUMERIC, 'returned_to_product', *CATEGORIES)):
        raise ValueError('Provide the six sequence features')
    actions, run, rate = (row[key] for key in NUMERIC)
    if type(actions) is not int or not 1 <= actions <= HISTORY_SIZE:
        raise ValueError(f'sequence_actions must be an integer from 1 to {HISTORY_SIZE}')
    if type(run) is not int or not 0 <= run <= actions:
        raise ValueError('same_product_view_run must be an integer between 0 and sequence_actions')
    if type(rate) not in (int, float) or not 0 <= rate <= 1:
        raise ValueError('product_switch_rate must be a number between 0 and 1')
    if type(row['returned_to_product']) is not int or row['returned_to_product'] not in (0, 1):
        raise ValueError('returned_to_product must be 0 or 1')
    if any(not isinstance(row[key], str) or not row[key] for key in CATEGORIES):
        raise ValueError('last_pair and product_pattern must be nonempty strings')

    switches = round(rate * (actions - 1))
    expected_rate = switches / (actions - 1) if actions > 1 else 0
    pair, pattern = (row[key] if row[key] in categories and row[key] != 'unknown' else None
                     for key, categories in CATEGORIES.items())
    if (not isclose(rate, expected_rate, rel_tol=0, abs_tol=1e-8)
            or (run, switches, row['returned_to_product'], pair, pattern) not in possible_inputs(actions)):
        raise ValueError('The six sequence features cannot come from the same action history')


class LogisticModel:
    def __init__(self, bundle, artifact_sha256):
        self.model_format = bundle.get('version')
        schema = SCHEMAS.get(self.model_format) if isinstance(self.model_format, str) else None
        saved_order = (bundle.get('scaled_columns'), bundle.get('columns'))
        if (schema is None or any(not isinstance(order, (list, tuple)) for order in saved_order)
                or tuple(map(tuple, saved_order)) != schema):
            raise ValueError('The logistic model format or feature order differs from the runtime')
        numeric, columns = schema
        parameters = {}
        for key, shape in (('mean', (len(numeric),)), ('scale', (len(numeric),)),
                           ('coefficients', (len(columns),)), ('intercept', ())):
            raw = bundle.get(key)
            items = raw if shape and isinstance(raw, list) else [raw]
            if any(type(item) not in (int, float) for item in items):
                raise ValueError(f'Invalid logistic model parameter: {key} must contain JSON numbers')
            try:
                value = np.asarray(raw, dtype=np.float64)
            except OverflowError as exc:
                raise ValueError(f'Invalid logistic model parameter: {key} exceeds the numeric range') from exc
            if value.shape != shape or not np.isfinite(value).all():
                raise ValueError(f'Invalid logistic model parameter: {key}')
            value.setflags(write=False)
            parameters[key] = value
        if np.any(parameters['scale'] <= 0):
            raise ValueError('Logistic input scales must be positive')
        self._bundle = parameters
        self.artifact_sha256 = artifact_sha256

    @classmethod
    def load(cls, path):
        return cls.from_bytes(path.read_bytes())

    @classmethod
    def from_bytes(cls, encoded):
        bundle = json.loads(encoded)
        if not isinstance(bundle, dict):
            raise ValueError('The logistic model file must contain a JSON object')
        return cls(bundle, sha256(encoded).hexdigest())

    def predict_actions(self, actions, *, event_time, sequence, event_id=None):
        return self.predict(sequence_inputs(actions, event_time=event_time, sequence=sequence, event_id=event_id))

    def predict(self, features):
        validate_input(features)
        probability = float(score_logistic(self._bundle, matrix([features], model_format=self.model_format))[0])
        return {
            'purchase_probability': probability,
            'coupon_eligible': COUPON_MIN <= probability < COUPON_MAX,
            'model_format': self.model_format,
            'model_artifact_sha256': self.artifact_sha256,
        }
