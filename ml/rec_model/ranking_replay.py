"""Replay a single recommendation request without changing the event pipeline."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3

from audit_2pct import recent_scores
from reco.core import Event, State, generate, iso, timestamp
from reco.pipeline import ranked_items
from reco.prior_interest import supplement_prior_interest
from reco.team_replay import ROOT, data_receipt, load_policy, snapshot

from .artifact_readiness import verify_readiness
from .hybrid_replay import attach_context, load_initial_ranker, score_hybrid


HYBRID_RANKING_MODE = 'protected_interest_then_lightgbm'


def recommend(database, config, output, receipt, cutoff, event_id, ranking=None, *, on_features=None,
              user_context=None, model_config=None, readiness_file=None):
    if model_config is not None and not model_config.is_file():
        raise FileNotFoundError(f'Model configuration file does not exist: {model_config}')
    mode = ranking or 'rules'
    if mode == 'hybrid':
        if model_config is None and config.get('demo_model_config'):
            model_config = ROOT / config['demo_model_config']
        if model_config is None or not model_config.is_file():
            raise FileNotFoundError('Hybrid replay requires a model configuration file. Supply --model-config.')
    if receipt['sample'] == 'team' and readiness_file is None:
        raise ValueError('Team recommendations require historical readiness records. Supply --availability-receipt.')
    prior_limit = config.get('demo_prior_visit_candidate_limit', 0)
    if type(prior_limit) is not int or not 0 <= prior_limit <= min(10, config['candidate_limit']):
        raise ValueError('demo_prior_visit_candidate_limit must be an integer between 0 and the smaller of 10 and the candidate limit')
    state = State(config)
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        request_key = conn.execute(
            'SELECT ts, source, row_no FROM events WHERE event_id = ?', (event_id,)
        ).fetchone()
        if request_key is None or request_key[0] <= cutoff:
            raise ValueError('Use a known request strictly after the relation snapshot cutoff')
        relations, info = snapshot(database, config, output, receipt, cutoff)
        artifacts = {'relation': (info['sha256'], cutoff)}
        if mode == 'hybrid':
            booster, artifacts['ranker'] = load_initial_ranker(model_config, request_key[0], receipt)
        readiness = verify_readiness(readiness_file, artifacts, request_key[0])
        for row in conn.execute(
            'SELECT * FROM events WHERE (ts, source, row_no) <= (?, ?, ?) ORDER BY ts, source, row_no',
            request_key,
        ):
            event = Event(*row)
            state.observe_live(event)
            if event.event_id == event_id:
                if user_context is not None:
                    from reco.redis_context import use_redis_context
                    use_redis_context(state, event, user_context)
                weights = tuple(config['request_weights'])
                query = generate(state, event, relations, weights)
                query = supplement_prior_interest(state, event, relations, weights, query, prior_limit)
                if on_features is not None:
                    on_features(event, query)
                limit = config['recommendation_limit']
                rules = ranked_items(query, 'ranker', recent_scores(query), k=limit)
                items = rules
                if mode == 'hybrid':
                    attach_context(state, event, query)
                    items = ranked_items(query, 'ranker', score_hybrid(booster, query), k=limit)
                return {
                    **receipt, 'event_id': event_id, 'as_of': iso(event.ts), 'cutoff': iso(cutoff),
                    'relation_sha256': info['sha256'], 'readiness': readiness,
                    'ranking_mode': HYBRID_RANKING_MODE if mode == 'hybrid' else config['ranking_mode'],
                    'candidate_count': len(query['items']), 'recommended_item_ids': items,
                    'prior_visit_candidate_count': sum(
                        'prior_visit_interest' in sources for sources in query['sources'].values()),
                    'pm_rule_item_ids': rules,
                }
    raise ValueError('Request could not be replayed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', required=True, type=Path)
    parser.add_argument('--sample', required=True, choices=['local', 'team'])
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--event-id', required=True)
    parser.add_argument('--cutoff')
    parser.add_argument('--ranking', choices=['hybrid', 'rules'], default='rules')
    parser.add_argument('--model-config', type=Path)
    parser.add_argument('--availability-receipt', type=Path)
    args = parser.parse_args()
    if args.sample == 'team' and args.availability_receipt is None:
        parser.error('--sample team requires --availability-receipt')
    if args.availability_receipt is not None and not args.availability_receipt.is_file():
        parser.error('--availability-receipt must point to an existing file')
    config = load_policy()
    result = recommend(
        args.events, config, args.output, data_receipt(args.events, args.sample),
        timestamp(args.cutoff or config['first_cutoff']), args.event_id, args.ranking,
        model_config=args.model_config, readiness_file=args.availability_receipt,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
