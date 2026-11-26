"""Validate purchase probability outputs and read or write common comparison records."""

import csv
import math
from pathlib import Path


CART_BASELINE_PATH = Path(__file__).with_name('cart_baseline.json')
BASE_LIGHTGBM = 'lightgbm_11'
LOGISTIC = 'sequence_logistic'


def validate_probabilities(predictions):
    if any(not math.isfinite(p) or not 0 <= p <= 1 for values in predictions.values() for p in values):
        raise ValueError('A model produced an invalid probability')

def is_long(row):
    return int(row['cart_count']) == 0 and int(row['view_count']) >= 6

def save_predictions(path, rows, predictions, *, extra_keys=()):
    with path.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.writer(stream)
        keys = ('event_id', 'visit_id', 'event_time_utc', 'label', *extra_keys)
        writer.writerow([*keys, *predictions])
        for row, values in zip(rows, zip(*predictions.values(), strict=True), strict=True):
            writer.writerow([*(row[key] for key in keys), *values])
