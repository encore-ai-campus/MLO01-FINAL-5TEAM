"""Compare purchase probabilities on one set of confirmed predictions.

Each row contains prediction_id, session_id, event_time_utc, sequence, label and
cart_count. All predictions in a session must have the same confirmed label.
Model scores are mappings from prediction ID to probability.
The two cart rates must be fixed from the training sample, in order of no cart,
then cart. Evaluation labels never set the baseline rates.
"""

from collections import Counter
from datetime import datetime, timedelta
from itertools import groupby
from math import fsum, isfinite
from numbers import Real


MIN_PURCHASE_SESSIONS_FOR_AP = 10
PROBABILITY_EDGES = (0.0, 0.10, 0.30, 0.50, 0.80, 1.0)


def evaluate(rows, model_scores, *, cart_rates_without_with):
    """Report session-weighted AP and Brier, calibration bands and top 10%."""
    rows = list(rows)
    ids, first_indices = _check_rows(rows)
    if 'cart' in model_scores:
        raise ValueError('cart is reserved for the cart baseline')
    compared = {}
    for name, scores in model_scores.items():
        if scores.keys() != ids:
            raise ValueError(f'{name} scores must match the evaluation prediction IDs')
        compared[name] = [_probability(scores[row['prediction_id']]) for row in rows]

    without_cart, with_cart = map(_probability, cart_rates_without_with)
    compared['cart'] = [with_cart if row['cart_count'] else without_cart for row in rows]

    labels = [row['label'] for row in rows]
    counts = Counter(row['session_id'] for row in rows)
    weights = [1 / counts[row['session_id']] for row in rows]
    purchase_sessions = {row['session_id'] for row in rows if row['label']}
    report_ap = len(set(labels)) == 2 and len(purchase_sessions) >= MIN_PURCHASE_SESSIONS_FOR_AP
    slots = (len(first_indices) + 9) // 10
    models = {}
    for name, probabilities in compared.items():
        ap, brier = _weighted_scores(labels, probabilities, weights, report_ap)
        models[name] = {
            'session_weighted_ap': ap,
            'session_weighted_brier': brier,
            'calibration_per_prediction': _probability_bands(labels, probabilities),
            'top_10pct_precision': _top_precision(labels, probabilities, first_indices, slots),
        }
    return {
        'prediction_rows': len(rows),
        'sessions': len(counts),
        'positive_prediction_rows': sum(labels),
        'purchase_sessions': len(purchase_sessions),
        'top_10pct_slots': slots,
        'models': models,
    }


def _check_rows(rows):
    ids, ordering_keys = set(), set()
    session_labels = {}
    first = {}
    for index, row in enumerate(rows):
        prediction_id, session_id = row['prediction_id'], row['session_id']
        if not isinstance(prediction_id, str) or not prediction_id.strip() or prediction_id in ids:
            raise ValueError('Expected a unique, nonempty string prediction_id')
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError('Expected a nonempty string session_id')
        if type(row['label']) is not int or row['label'] not in (0, 1):
            raise ValueError('Expected a confirmed purchase label of 0 or 1')
        if session_labels.setdefault(session_id, row['label']) != row['label']:
            raise ValueError(f'Conflicting purchase labels in session {session_id}')
        if type(row['cart_count']) is not int or row['cart_count'] < 0:
            raise ValueError('Expected a nonnegative integer cart_count')
        if type(row['sequence']) is not int or row['sequence'] < 0:
            raise ValueError('Expected a nonnegative integer sequence')
        event_time = datetime.fromisoformat(row['event_time_utc'])
        if event_time.utcoffset() != timedelta(0):
            raise ValueError('Expected event_time_utc to be in UTC')

        order = (event_time, row['sequence'])
        key = (session_id, order)
        if key in ordering_keys:
            raise ValueError(f'Ambiguous prediction order in session {session_id}')
        ids.add(prediction_id)
        ordering_keys.add(key)
        if session_id not in first or order < first[session_id][0]:
            first[session_id] = (order, index)
    return ids, [index for _, index in first.values()]


def _probability(value):
    if isinstance(value, bool) or not isinstance(value, Real) or not 0 <= value <= 1 or not isfinite(value):
        raise ValueError('Expected a finite probability between 0 and 1')
    return float(value)


def _weighted_scores(labels, probabilities, weights, report_ap):
    if not labels:
        return None, None
    brier = fsum(w * (p - y) ** 2 for p, y, w in zip(probabilities, labels, weights, strict=True)) / fsum(weights)
    if not report_ap:
        return None, brier

    pairs = sorted(zip(probabilities, labels, weights, strict=True), reverse=True)
    positive_weight = fsum(y * w for _, y, w in pairs)
    true_seen = seen = ap = 0.0
    # Count tied probabilities at one threshold, independent of row order.
    for _, group in groupby(pairs, key=lambda pair: pair[0]):
        tied = list(group)
        true_in_group = fsum(y * w for _, y, w in tied)
        true_seen += true_in_group
        seen += fsum(w for _, _, w in tied)
        ap += (true_in_group / positive_weight) * (true_seen / seen)
    return ap, brier


def _probability_bands(labels, probabilities):
    pairs = list(zip(probabilities, labels, strict=True))
    bands = []
    for lower, upper in zip(PROBABILITY_EDGES[:-1], PROBABILITY_EDGES[1:], strict=True):
        group = [(p, y) for p, y in pairs if lower <= p < upper or p == upper == 1]
        bands.append({
            'lower': lower,
            'upper': upper,
            'rows': len(group),
            'mean_probability': fsum(p for p, _ in group) / len(group) if group else None,
            'purchase_rate': sum(y for _, y in group) / len(group) if group else None,
        })
    return bands


def _top_precision(labels, probabilities, first_indices, slots):
    if not slots:
        return None
    ranked = sorted(((probabilities[i], labels[i]) for i in first_indices), reverse=True)
    remaining = slots
    purchases = 0.0
    for _, group in groupby(ranked, key=lambda pair: pair[0]):
        tied = list(group)
        selected = min(remaining, len(tied))
        purchases += sum(label for _, label in tied) * selected / len(tied)
        remaining -= selected
        if not remaining:
            break
    return purchases / slots
