"""Build order features using only the observed actions in the current visit."""

from collections import deque
from functools import lru_cache
from itertools import product
from typing import NamedTuple

from libs.features.purchase_sequence import ACTIONS, HISTORY_SIZE, utc_datetime
from libs.features.purchase_sequence import describe_actions as describe
from libs.features.purchase_sequence import sequence_inputs as shared_sequence_inputs

NUMERIC = ('sequence_actions', 'same_product_view_run', 'product_switch_rate')
BINARY = ('returned_to_product',)
CATEGORIES = {
    'last_pair': ('start', *(f'{left}_{right}_{relation}'
                             for left, right, relation in product(ACTIONS, ACTIONS, ('same', 'other'))), 'unknown'),
    'product_pattern': ('A', 'AA', 'AB', 'AAA', 'AAB', 'ABA', 'ABB', 'ABC', 'unknown'),
}
COLUMNS = NUMERIC + BINARY + tuple(f'{key}_{category}' for key, values in CATEGORIES.items() for category in values)
CUMULATIVE_NUMERIC = NUMERIC + ('view_count', 'distinct_product_count', 'cart_count')
CUMULATIVE_COLUMNS = CUMULATIVE_NUMERIC + COLUMNS[len(NUMERIC):]


def sequence_inputs(actions, *, event_time, sequence, legacy_ids=False):
    """Adapt historical numeric product IDs to the shared input contract."""
    if legacy_ids and isinstance(actions, list):
        actions = [
            {**action, 'product_id': str(action['product_id'])}
            if isinstance(action, dict) and type(action.get('product_id')) is int else action
            for action in actions
        ]
    return shared_sequence_inputs(actions, event_time=event_time, sequence=sequence)


class _Action(NamedTuple):
    event_type: str
    product_id: int | str


def _product_histories(size):
    if size == 1:
        yield (0,)
        return
    for prefix in _product_histories(size - 1):
        # Product identity is relative. The next item is a known product or the next new one.
        for item in range(max(prefix) + 2):
            yield (*prefix, item)


@lru_cache(maxsize=HISTORY_SIZE)
def possible_inputs(size):
    """Return signatures obtainable from the same calculator used for live inputs."""
    signatures = set()
    for products in _product_histories(size):
        for actions in product(ACTIONS, repeat=size):
            row = describe([_Action(action, item) for action, item in zip(actions, products, strict=True)])
            numbers = (row['same_product_view_run'], round(row['product_switch_rate'] * (size - 1)),
                       row['returned_to_product'])
            # Unknown categories skip just that category's comparison.
            signatures.update((*numbers, pair, pattern)
                              for pair in (row['last_pair'], None)
                              for pattern in (row['product_pattern'], None))
    return frozenset(signatures)


def visit_features(visit):
    history = deque(maxlen=HISTORY_SIZE)
    features = {}
    for event in visit.events:
        if event.event_type == 'purchase':
            break
        if event.event_type in ACTIONS:
            history.append(event)
            features[event.event_id] = describe(history)
    return features


def encode(row, *, numeric=NUMERIC):
    if numeric == CUMULATIVE_NUMERIC:
        for key, minimum in (('view_count', 0), ('distinct_product_count', 1), ('cart_count', 0)):
            if type(row.get(key)) is not int or row[key] < minimum:
                raise ValueError(f'{key} must be an integer of at least {minimum}')
    values = [row[key] for key in numeric + BINARY]
    for key, categories in CATEGORIES.items():
        category = row[key] if row[key] in categories else 'unknown'
        values.extend(int(category == option) for option in categories)
    return values
