"""October–December team replay tools. All times are recorded event times."""
import argparse
import csv
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from time import perf_counter

from audit_2pct import recent_scores

from .cf import training_bounds
from .core import Event, State, generate, iso, timestamp
from .cumulative import atomic_json
from .evaluate_cumulative import PastOnlyCF, read_month, replay_month
from .pipeline import evaluate, query_plan, ranked_items
from .prior_interest import supplement_prior_interest
from .store import checksum, install


ROOT = Path(__file__).resolve().parents[1]
METRICS = (
    'purchase_candidate_recall_at_100',
    'purchase_recall_at_10',
    'purchase_ndcg_at_10',
)


def load_policy():
    settings = json.loads((ROOT / 'config-team-replay.json').read_text('utf-8'))
    candidate = json.loads((ROOT / settings['candidate_policy_file']).read_text('utf-8'))
    return {**candidate, **settings}


def data_receipt(database, sample):
    manifest_file = database.with_suffix('.manifest.json')
    manifest = json.loads(manifest_file.read_text('utf-8'))
    if not manifest.get('complete'):
        raise ValueError('Event store manifest must describe a complete sample')
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        count = conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
    if count != manifest.get('sample_rows'):
        raise ValueError('Event store row count differs from its manifest')
    return {'manifest_sha256': checksum(manifest_file), 'sample': sample, 'event_count': count}


def snapshot(database, config, output, receipt, cutoff):
    # A fixed timeline pins one relation table for the whole comparison.
    policy = {**config, 'cf_cutoff_ts': cutoff, 'cf_refresh_days': 0}
    cache = output / 'snapshots' / receipt['manifest_sha256']
    timeline = PastOnlyCF(database, policy, cache)
    relations = timeline.for_time(cutoff)
    info = timeline.snapshots[cutoff]
    if not info['support']['relations']:
        raise ValueError('The relation snapshot contains no supported item pairs')
    return relations, info


def build(database, config, output, receipt, cutoff):
    started = perf_counter()
    relations, snapshot_info = snapshot(database, config, output, receipt, cutoff)
    digest = snapshot_info['sha256']
    version = 'cf-' + digest[:20]
    bundle_id = f'{cutoff}-{digest[:20]}-{receipt["manifest_sha256"][:12]}-{receipt["sample"]}'
    bundle = output / 'bundles' / bundle_id
    bundle.mkdir(parents=True, exist_ok=True)
    # Match the relation CSV and manifest consumed by existing RDB tooling.
    source = Path(snapshot_info['file'])
    with (bundle / 'neighbors.json').open('wb') as destination:
        destination.write(source.read_bytes())
    with (bundle / 'item_neighbors.csv').open('w', newline='', encoding='utf-8') as destination:
        writer = csv.writer(destination)
        writer.writerow(['retriever_version', 'source_item_id', 'neighbor_item_id', 'similarity', 'neighbor_rank'])
        for item, neighbors in relations.items():
            writer.writerows((version, item, neighbor, score, rank)
                             for rank, (neighbor, score) in enumerate(neighbors, 1))
    low, high = training_bounds(cutoff, config)
    atomic_json(bundle / 'policy.json', {
        'ranking_mode': config['ranking_mode'],
        'session_gap_seconds': config['session_gap_seconds'],
        'session_gap_boundary': config['session_gap_boundary'],
        'candidate_limit': config['candidate_limit'],
        'recommendation_limit': config['recommendation_limit'],
        'cf_weights': config['cf_weights'],
        'training_window_utc': [iso(low), iso(high)],
    })
    manifest = {
        'bundle_version': f'replay-{bundle_id}',
        'retriever_version': version,
        'session_policy_version': 'gap-1800-over-v1',
        'ranking_mode': config['ranking_mode'],
        'data_version': receipt['manifest_sha256'],
        'sample': receipt['sample'],
        'retriever_snapshots': [{'file': 'neighbors.json', 'version': version, 'cutoff_ts': cutoff}],
        'checksums': {name: checksum(bundle / name)
                      for name in ['neighbors.json', 'item_neighbors.csv', 'policy.json']},
        'promotion': {'accepted': False, 'decision': 'local_replay_export'},
    }
    atomic_json(bundle / 'manifest.json', manifest)
    install(bundle, output / 'serving.sqlite')
    report = {
        **receipt, 'bundle': str(bundle), 'cutoff': iso(cutoff), 'training_window': [iso(low), iso(high)],
        'support': snapshot_info['support'], 'cf_fit_seconds': snapshot_info['fit_seconds'],
        'elapsed_seconds': round(perf_counter() - started, 2), 'activated': False,
    }
    atomic_json(output / f'build-{cutoff}.json', report)
    return report


class FixedRelations:
    def __init__(self, relations, cutoff):
        self.relations = relations
        self.cutoff = cutoff

    def for_time(self, as_of):
        if as_of < self.cutoff:
            raise ValueError('A relation snapshot cannot use data after the request')
        return self.relations


def replacement_result(baseline, candidate, minimum):
    if baseline['queries_with_eligible_future_purchase'] < minimum:
        return 'insufficient_purchase_queries'
    return ('eligible_for_review' if all(candidate[key] >= baseline[key] for key in METRICS)
            else 'keep_baseline')


def evaluation_inputs(database, config, start, end, limit):
    if start >= end:
        raise ValueError('Evaluation start must precede its end')
    if limit < 1:
        raise ValueError('Query limit must be positive')
    events, sessions = read_month(database, start, end, config['session_gap_seconds'],
                                  config['session_gap_boundary'])
    if not events:
        raise ValueError('No events available for the selected evaluation window')
    windows = {key: [iso(start), iso(end)] for key in ('rank_train', 'validation', 'test', 'future')}
    policy = {**config, **windows, 'queries_per_session': 1,
              'query_limits': {key: limit if key == 'future' else 0 for key in windows}}
    plan, stats = query_plan(events, sessions, policy)
    if not plan:
        raise ValueError('No mature requests available for the evaluation window')
    return events, sessions, plan, stats['future'], policy


def evaluate_snapshot(database, config, output, receipt, cutoff, inputs):
    events, sessions, plan, _, policy = inputs
    relations, snapshot_info = snapshot(database, config, output, receipt, cutoff)
    queries = replay_month(database, events, sessions, plan, FixedRelations(relations, cutoff), policy)
    identity = [(row['request_id'], row['truth']) for row in queries]
    metrics, _ = evaluate(queries, 'ranker', [recent_scores(query) for query in queries],
                          k=config['recommendation_limit'])
    return {'cutoff': iso(cutoff), 'snapshot_sha256': snapshot_info['sha256'], 'metrics': metrics}, identity


def compare(database, config, output, receipt, start, end, baseline_cutoff, candidate_cutoff, limit):
    if not baseline_cutoff < candidate_cutoff <= start < end:
        raise ValueError('Require baseline cutoff < candidate cutoff <= evaluation start < evaluation end')
    inputs = evaluation_inputs(database, config, start, end, limit)
    results = {}
    previous_identity = None
    for name, cutoff in [('baseline', baseline_cutoff), ('candidate', candidate_cutoff)]:
        results[name], identity = evaluate_snapshot(database, config, output, receipt, cutoff, inputs)
        if previous_identity is not None and identity != previous_identity:
            raise ValueError('Comparison changed the selected requests or purchase labels')
        previous_identity = identity
    baseline, candidate = (results[name]['metrics'] for name in ('baseline', 'candidate'))
    report = {
        **receipt, 'window': [iso(start), iso(end)], 'ranking_mode': config['ranking_mode'],
        'requests': inputs[3], 'arms': results,
        'delta': {key: candidate[key] - baseline[key] if baseline[key] is not None else None for key in METRICS},
        'decision': replacement_result(baseline, candidate, config['min_purchase_queries']),
        'min_purchase_queries': config['min_purchase_queries'],
        'activated': False,
    }
    atomic_json(output / f'comparison-{start}-{end}-{baseline_cutoff}-{candidate_cutoff}-{limit}.json', report)
    return report


def common_evaluation(database, config, output, receipt, start, end, cutoff, limit):
    if cutoff > start:
        raise ValueError('Common evaluation snapshot cutoff must be at or before the window start')
    inputs = evaluation_inputs(database, config, start, end, limit)
    result, _ = evaluate_snapshot(database, config, output, receipt, cutoff, inputs)
    report = {**receipt, 'window': [iso(start), iso(end)], 'requests': inputs[3],
              'ranking_mode': config['ranking_mode'], 'status': config['common_evaluation_status'],
              'relation_policy': 'fixed_at_cutoff', 'result': result, 'activated': False}
    atomic_json(output / f'common-{start}-{end}-{cutoff}-{limit}.json', report)
    return report


def recommend(database, config, output, receipt, cutoff, event_id, ranking=None, *, on_features=None,
              user_context=None):
    mode = ranking or ('hybrid' if config.get('demo_ranking_mode') == 'protected_interest_then_lightgbm' else 'rules')
    prior_limit = config.get('demo_prior_visit_candidate_limit', 0)
    if type(prior_limit) is not int or not 0 <= prior_limit <= min(10, config['candidate_limit']):
        raise ValueError('demo_prior_visit_candidate_limit must be an integer between 0 and the smaller of 10 and the candidate limit')
    state = State(config)
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        request_key = conn.execute(
            'SELECT ts, source, row_no FROM events WHERE event_id = ?', (event_id,)
        ).fetchone()
        if request_key is None or request_key[0] < cutoff:
            raise ValueError('Use a known request at or after the relation snapshot cutoff')
        if mode == 'hybrid':
            from .hybrid_replay import attach_context, load_initial_ranker, score_hybrid
            booster = load_initial_ranker(ROOT, config['demo_model_config'], request_key[0], receipt)
        for row in conn.execute(
            'SELECT * FROM events WHERE (ts, source, row_no) <= (?, ?, ?) ORDER BY ts, source, row_no',
            request_key,
        ):
            event = Event(*row)
            state.observe_live(event)
            if event.event_id == event_id:
                if user_context is not None:
                    from .redis_context import use_redis_context
                    use_redis_context(state, event, user_context)
                relations, info = snapshot(database, config, output, receipt, cutoff)
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
                    'relation_sha256': info['sha256'],
                    'ranking_mode': config['demo_ranking_mode'] if mode == 'hybrid' else config['ranking_mode'],
                    'candidate_count': len(query['items']), 'recommended_item_ids': items,
                    'prior_visit_candidate_count': sum(
                        'prior_visit_interest' in sources for sources in query['sources'].values()),
                    'pm_rule_item_ids': rules,
                }
    raise ValueError('Request could not be replayed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['build', 'recommend', 'precheck', 'compare', 'evaluate'])
    parser.add_argument('--events', required=True, type=Path)
    parser.add_argument('--sample', required=True, choices=['local', 'team'])
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--cutoff')
    parser.add_argument('--event-id')
    parser.add_argument('--ranking', choices=['hybrid', 'rules'], help='Override the demo ranking for recommend')
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--baseline-cutoff')
    parser.add_argument('--candidate-cutoff')
    parser.add_argument('--limit', type=int, default=16000)
    args = parser.parse_args()
    config = load_policy()
    receipt = data_receipt(args.events, args.sample)
    args.output.mkdir(parents=True, exist_ok=True)
    if args.stage == 'recommend':
        if not args.event_id:
            parser.error('recommend requires --event-id')
        result = recommend(args.events, config, args.output, receipt,
                           timestamp(args.cutoff or config['first_cutoff']), args.event_id, args.ranking)
    elif args.stage == 'build':
        result = build(args.events, config, args.output, receipt, timestamp(args.cutoff or config['first_cutoff']))
    elif args.stage == 'evaluate':
        default_start, default_end = config['common_evaluation_month']
        start = timestamp(args.start or default_start)
        result = common_evaluation(args.events, config, args.output, receipt, start,
                                   timestamp(args.end or default_end), timestamp(args.cutoff) if args.cutoff else start,
                                   args.limit)
    else:
        if args.stage == 'precheck':
            precheck = config['precheck']
        else:
            precheck = {key: getattr(args, key) for key in ('start', 'end', 'baseline_cutoff', 'candidate_cutoff')}
            if not all(precheck.values()):
                parser.error('compare requires --start, --end, --baseline-cutoff and --candidate-cutoff')
        result = compare(args.events, config, args.output, receipt,
                         *(timestamp(precheck[key]) for key in ('start', 'end', 'baseline_cutoff', 'candidate_cutoff')),
                         args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
