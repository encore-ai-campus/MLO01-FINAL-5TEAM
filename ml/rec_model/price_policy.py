"""Point-in-time price normalization and shared recommendation candidates."""

from dataclasses import replace

from reco.core import State, generate
from reco.prior_interest import supplement_prior_interest


PRICE_POLICY = 'zero-as-missing-v1'


class PriceState(State):
    def observe(self, event):
        super().observe(replace(event, price=None) if event.price == 0 else event)


def prior_visit_limit(config):
    limit = config.get('demo_prior_visit_candidate_limit', 0)
    if type(limit) is not int or not 0 <= limit <= min(10, config['candidate_limit']):
        raise ValueError('demo_prior_visit_candidate_limit must be an integer between 0 and the smaller of 10 and the candidate limit')
    return limit


def request_candidates(state, event, relations, weights):
    limit = prior_visit_limit(state.c)
    base = generate(state, event, relations, weights)
    return supplement_prior_interest(state, event, relations, weights, base, limit)
