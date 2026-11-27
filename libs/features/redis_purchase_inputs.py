"""Adapt the ordered Redis recent-action list to the purchase input calculator."""

from libs.features.purchase_sequence import sequence_inputs


def calculate(actions):
    """Use the last action as the prediction request, as the Redis publisher does."""
    if not isinstance(actions, list) or not actions:
        raise ValueError('Expected a nonempty Redis action list')
    previous = None
    for action in actions:
        seq = action.get('seq') if isinstance(action, dict) else None
        if type(seq) is not int or (previous is not None and seq != previous + 1):
            raise ValueError('Redis actions must be listed in consecutive session seq order')
        previous = seq

    current = actions[-1]
    return sequence_inputs(actions, event_time=current.get('event_time_utc'),
                           sequence=current.get('sequence'), event_id=current.get('event_id'))
