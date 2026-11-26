"""Build order features using only the observed actions in the current visit."""

from collections import deque
from datetime import datetime, timedelta
from functools import lru_cache
from itertools import product
from typing import NamedTuple


HISTORY_SIZE = 5
ACTIONS = ('view', 'cart')
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


def utc_datetime(value):
    """Read an explicit UTC timestamp from the team data contract."""
    if not isinstance(value, str):
        raise ValueError('Expected a UTC timestamp')
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError('Expected a UTC timestamp')
    return parsed


def sequence_inputs(actions, *, event_time, sequence, legacy_ids=False):
    """Calculate the six v1 inputs from a single visit's recent actions."""
    if not isinstance(actions, list) or not 1 <= len(actions) <= HISTORY_SIZE:
        raise ValueError('Provide one to five recent actions')
    keys, observed = [], []
    for action in actions:
        if not isinstance(action, dict):
            raise ValueError('Each action must be an object')
        product_id, event_type, order = (action.get(key) for key in ('product_id', 'event_type', 'sequence'))
        if (not (isinstance(product_id, str) or (legacy_ids and type(product_id) is int)) or str(product_id).strip() == ''
                or event_type not in ACTIONS or type(order) is not int or order < 0):
            raise ValueError('Invalid product, action type or sequence number')
        keys.append((utc_datetime(action.get('event_time_utc')), order))
        observed.append(_Action(event_type, str(product_id)))
    current_key = (utc_datetime(event_time), sequence)
    if type(sequence) is not int or len(set(keys)) != len(keys) or max(keys) != current_key:
        raise ValueError('Recent actions must end at the requested action and have distinct ordering keys')
    return describe([item for _, item in sorted(zip(keys, observed), key=lambda pair: pair[0])])


def describe(history):
    current = history[-1]
    run = 0
    for action in reversed(history):
        if action.event_type != 'view' or action.product_id != current.product_id:
            break
        run += 1
    products = [action.product_id for action in history]
    switches = sum(a != b for a, b in zip(products, products[1:]))
    last_pair = 'start'
    if len(history) > 1:
        previous = history[-2]
        relation = 'same' if previous.product_id == current.product_id else 'other'
        last_pair = f'{previous.event_type}_{current.event_type}_{relation}'
    symbols = {}
    pattern = ''.join(symbols.setdefault(item, chr(65 + len(symbols))) for item in products[-3:])
    returned = any(action.event_type == 'view' and action.product_id == current.product_id
                   and any(item != current.product_id for item in products[index + 1:-1])
                   for index, action in enumerate(history) if index < len(history) - 1)
    return dict(zip(NUMERIC + BINARY + tuple(CATEGORIES),
                    (len(history), run, switches / (len(history) - 1) if len(history) > 1 else 0,
                     int(returned), last_pair, pattern), strict=True))


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
