"""Candidate supplementation from earlier visits."""
from collections import Counter

from .core import generate


MIN_VIEWS = 2


def supplement_prior_interest(state, event, neighbors, weights, baseline, limit):
    if not limit:
        return baseline
    history, _, exclusions = state.context(event)
    unavailable = set(baseline['items']) | exclusions | {event.item}
    views = Counter()
    carted = set()
    last_seen = {}
    for action in history:
        if action.session == event.session or action.item in unavailable or action.action >= 2:
            continue
        if action.action == 0:
            views[action.item] += 1
        else:
            carted.add(action.item)
        last_seen[action.item] = action.ts
    additions = sorted((item for item in last_seen if item in carted or views[item] >= MIN_VIEWS),
                       key=lambda item: (item not in carted, -last_seen[item], item))[:limit]
    if not additions:
        return baseline
    return generate(state, event, neighbors, weights, prior_interest_candidates=additions)
