"""Convert the online Redis payload into the recommendation user's replay context."""
from collections import deque

from .core import Event, timestamp
from .data import ACTIONS


def use_redis_context(state, request, payload):
    if state.c['history_days'] > 7:
        raise ValueError('Redis online history supports recommendation windows of up to seven days')
    history = deque(Event(event_id=row['event_id'], ts=timestamp(row['event_time']), row_no=row['event_seq'],
                          source=row['source'], user=request.user, item=row['product_id'],
                          action=ACTIONS[row['event_type']], category='', brand='', price=None, raw_session='',
                          session=f"{request.user}:{row['session_start_event_id']}")
                    for row in payload['history'])
    if not history or history[-1].event_id != request.event_id or history[-1].session != request.session:
        raise ValueError('Redis context must end at the same request and visit')
    state.history[request.user] = history
    state.purchases[request.user] = deque(sorted((at, int(item)) for item, at in payload['purchases'].items()))
