"""Calculate the six purchase conversion inputs for one observed request."""

from datetime import datetime, timedelta
from typing import NamedTuple


HISTORY_SIZE = 5
ACTIONS = ('view', 'cart')
FEATURE_NAMES = ('sequence_actions', 'same_product_view_run', 'product_switch_rate',
                 'returned_to_product', 'last_pair', 'product_pattern')


class _Action(NamedTuple):
    event_type: str
    product_id: str


def utc_datetime(value):
    if not isinstance(value, str):
        raise ValueError('Expected a UTC timestamp')
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError('Expected a UTC timestamp')
    return parsed


def sequence_inputs(actions, *, event_time, sequence, event_id=None):
    """Calculate inputs from one to five actions of the same session."""
    if not isinstance(actions, list) or not 1 <= len(actions) <= HISTORY_SIZE:
        raise ValueError('Provide one to five recent actions')
    if type(sequence) is not int or sequence < 0:
        raise ValueError('Invalid request sequence number')
    if not all(isinstance(action, dict) for action in actions):
        raise ValueError('Each action must be an object')
    current_key = (utc_datetime(event_time), sequence)
    has_session_order = any('seq' in action for action in actions)
    observed = {}
    event_ids = set()
    for action in actions:
        product = action.get('product_id')
        kind = action.get('event_type')
        order = action.get('sequence')
        if (not isinstance(product, str) or not product.strip()
                or kind not in ACTIONS or type(order) is not int or order < 0):
            raise ValueError('Invalid product, action type or sequence number')
        source_key = (utc_datetime(action.get('event_time_utc')), order)
        action_id = action.get('event_id')
        position = source_key
        if has_session_order:
            position = action.get('seq')
            if type(position) is not int or position < 1:
                raise ValueError('Every action must have a positive integer session seq')
            if not isinstance(action_id, str) or not action_id.strip() or action_id in event_ids:
                raise ValueError('Recent actions must have distinct nonempty event IDs')
            event_ids.add(action_id)
        if position in observed:
            raise ValueError('Recent actions must have distinct ordering keys')
        observed[position] = (_Action(kind, product), source_key, action_id)
    positions = sorted(observed)
    ordered = [observed[position] for position in positions]
    if has_session_order:
        if positions[-1] - positions[0] != len(positions) - 1:
            raise ValueError('Recent actions must have consecutive session seq values')
        if any(previous[1] > following[1] for previous, following in zip(ordered, ordered[1:])):
            raise ValueError('Session action order conflicts with the source order')
        if not isinstance(event_id, str) or not event_id.strip() or ordered[-1][2] != event_id:
            raise ValueError('Recent actions must end at the requested event ID')
    if ordered[-1][1] != current_key:
        raise ValueError('Recent actions must end at the requested action')
    return describe_actions([action for action, _, _ in ordered])


def describe_actions(history):
    """Calculate inputs from actions that are already ordered by the caller."""
    current = history[-1]
    run = 0
    for action in reversed(history):
        if action.event_type != 'view' or action.product_id != current.product_id:
            break
        run += 1

    products = [action.product_id for action in history]
    switches = sum(first != second for first, second in zip(products, products[1:]))
    last_pair = 'start'
    if len(history) > 1:
        previous = history[-2]
        relation = 'same' if previous.product_id == current.product_id else 'other'
        last_pair = f'{previous.event_type}_{current.event_type}_{relation}'
    symbols = {}
    pattern = ''.join(symbols.setdefault(item, chr(65 + len(symbols))) for item in products[-3:])
    returned = any(action.event_type == 'view' and action.product_id == current.product_id
                   and any(item != current.product_id for item in products[index + 1:-1])
                   for index, action in enumerate(history) if index < len(products) - 1)

    values = (len(history), run, switches / (len(history) - 1) if len(history) > 1 else 0,
              int(returned), last_pair, pattern)
    return dict(zip(FEATURE_NAMES, values, strict=True))
