"""Time-safe retrieval, ranking, validation, and artifact packaging."""
import hashlib
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import psutil

from . import cf
from .core import FEATURES, WEIGHTS, State, generate, iso, read_events, session_close_at, targets, timestamp
from .store import checksum, code_version, install, NeighborStore


def write_json(file, obj):
    Path(file).parent.mkdir(parents=True, exist_ok=True)
    Path(file).write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), 'utf-8')


def validate_config(c):
    cf.training_bounds(timestamp(c['cf_cutoff']),c)
    previous_end = timestamp(c['cf_cutoff'])
    for name in ['rank_train', 'validation', 'test', 'future']:
        start, end = map(timestamp, c[name])
        if start < previous_end or start >= end:
            raise ValueError(f'Invalid chronological split: {name}. CF must precede rank training and all windows must be disjoint.')
        previous_end = end
    if not 1 <= c['neighbors_per_item'] <= 100:
        raise ValueError('neighbors_per_item must be 1..100 for this serving schema')
    if not 1 <= c['recommendation_limit'] <= c['candidate_limit']:
        raise ValueError('Invalid candidate/recommendation limits')
    if not 0 <= c['direct_items'] <= c['candidate_limit']:
        raise ValueError('Invalid direct item allocation')
    if c['half_life_hours'] <= 0 or c['session_gap_seconds'] <= 0:
        raise ValueError('Time windows must be positive')
    if c.get('queries_per_session', 1) < 1 or c.get('query_min_interval_seconds', 60) < 0:
        raise ValueError('Invalid within-session request policy')
    if c.get('cf_refresh_days', 0) < 0:
        raise ValueError('CF refresh interval cannot be negative')


def query_plan(events, sessions, c):
    plan, stats = {}, {}
    coverage_end = events[-1].ts
    for split in ['rank_train', 'validation', 'test', 'future']:
        start, end = map(timestamp, c[split])
        eligible, unfinished, spanning = [], 0, 0
        for seq in sessions.values():
            request = next((e for e in seq if e.action < 2), None)
            if request is None or not start <= request.ts < end:
                continue
            if seq[0].ts < start:
                spanning += 1
                continue
            # Timestamps are whole seconds. With a strict gap, a visit can
            # still receive an event at the exact 30-minute mark.
            close_at = session_close_at(seq[-1].ts, c['session_gap_seconds'], c.get('session_gap_boundary', 'at_least'))
            if close_at > min(end, coverage_end):
                unfinished += 1
                continue
            # Select from the observed prefix, never from eventual purchases.
            chosen_in_session = [request]
            for candidate in seq:
                if len(chosen_in_session) >= c.get('queries_per_session', 1):
                    break
                if candidate.order <= request.order or candidate.action >= 2:
                    continue
                if candidate.action == 1 or candidate.ts-chosen_in_session[-1].ts >= c.get('query_min_interval_seconds', 60):
                    chosen_in_session.append(candidate)
            eligible.extend(chosen_in_session)
        # No target/outcome information is used to choose sampled queries.
        eligible.sort(key=lambda e: hashlib.sha256(f'{c["seed"]}:{e.event_id}'.encode()).digest())
        chosen = eligible[:c['query_limits'][split]]
        for e in chosen:
            plan[e.order] = split
        stats[split] = {'eligible_sessions': len({e.session for e in eligible}),
                        'eligible_requests': len(eligible), 'selected_queries': len(chosen),
                        'unfinished_or_crossing_end': unfinished, 'crossing_start': spanning,
                        'sampling': f'deterministic request hash; first view/cart plus up to {c.get("queries_per_session", 1)-1} prefix-triggered requests; outcomes unused'}
    return plan, stats


def replay(events, sessions, plan, neighbors, weights, c, splits):
    state = State(c)
    result = {s: [] for s in splits}
    last = max((order for order, split in plan.items() if split in splits), default=-1)
    for e in events:
        if e.order > last:
            break
        state.observe(e)
        split = plan.get(e.order)
        if split not in splits:
            continue
        current_neighbors = neighbors.for_time(e.ts) if hasattr(neighbors, 'for_time') else neighbors
        row = generate(state, e, current_neighbors, weights)
        truth, exclusions = targets(sessions[e.session], e, state.metadata.keys(), row['excluded'])
        # Evidence is reconstructed for serving; do not retain millions of dicts in evaluation.
        row.pop('evidence')
        result[split].append({**row, 'truth': truth, 'exclusions': exclusions,
                              'user': e.user, 'session_id': e.session,
                              'request_id': hashlib.sha256(e.event_id.encode()).hexdigest()[:20],
                              'as_of': e.ts, 'current_item_id': e.item,
                              'user_history_events': len(state.history[e.user])})
    return result


def ranked_items(q, mode, prediction=None, k=10):
    if mode == 'popular':
        return q['popular'][:k]
    values = prediction if mode == 'ranker' else q['cf_scores']
    indexes = sorted(range(len(q['items'])), key=lambda i: (-float(values[i]), q['items'][i]))
    return [q['items'][i] for i in indexes[:k]]


def evaluate(queries, mode, predictions=None, k=10):
    rows = []
    recommended = set()
    for index, q in enumerate(queries):
        order = ranked_items(q, mode, None if predictions is None else predictions[index], k)
        recommended.update(order)
        truth = q['truth']
        hits = len(set(order) & truth)
        dcg = sum(1/np.log2(i+2) for i, p in enumerate(order) if p in truth)
        idcg = sum(1/np.log2(i+2) for i in range(min(k, len(truth))))
        rows.append({'request_id': q['request_id'], 'user': q['user'],
                     'has_truth': bool(truth), 'hit': int(hits>0),
                     'recall': hits/len(truth) if truth else 0.,
                     'ndcg': float(dcg/idcg) if idcg else 0.,
                     'candidate_recall': len(set(q['items']) & truth)/len(truth) if truth else 0.})
    labeled = [r for r in rows if r['has_truth']]
    avg = lambda field: float(np.mean([r[field] for r in labeled])) if labeled else None
    output = {'queries_all_mature': len(rows), 'queries_with_eligible_future_purchase': len(labeled),
              'queries_without_eligible_purchase': len(rows)-len(labeled),
              'purchase_candidate_recall_at_100': avg('candidate_recall'),
              'purchase_recall_at_10': avg('recall'), 'purchase_hit_rate_at_10': avg('hit'),
              'purchase_ndcg_at_10': avg('ndcg'),
              'all_mature_queries_purchase_hit_fraction': float(np.mean([r['hit'] for r in rows])) if rows else None,
              'unique_recommended_products': len(recommended),
              'target_exclusions': dict(sum((Counter(q['exclusions']) for q in queries), Counter())),
              'candidate_source_counts': dict(Counter(s for q in queries for p in q['items'] for s in q['sources'][p])),
              'mean_candidate_count': float(np.mean([len(q['items']) for q in queries])) if queries else 0.}
    return output, rows


def pack_rank_data(queries):
    usable = []
    for q in queries:
        y = np.asarray([int(p in q['truth']) for p in q['items']], dtype=np.int32)
        if len(y)>1 and y.min()!=y.max():
            usable.append((q['features'], y))
    if not usable:
        raise ValueError('No rankable query groups. Increase user sampling or revise candidate recall, not labels.')
    return (np.concatenate([x for x, _ in usable]), np.concatenate([y for _, y in usable]),
            [len(y) for _, y in usable])


def predict_all(booster, queries, threads):
    lengths = [len(q['items']) for q in queries]
    if not sum(lengths):
        return [np.empty(0) for q in queries]
    matrix = np.concatenate([q['features'] for q in queries])
    return np.split(booster.predict(matrix, num_threads=threads), np.cumsum(lengths)[:-1])


def improvement_ci(model_rows, other_rows, repeats, seed):
    if type(repeats) is not int or repeats < 2:
        raise ValueError('A confidence interval requires at least two bootstrap resamples')
    by_user = {}
    seen = set()
    for a, b in zip(model_rows, other_rows, strict=True):
        request_id = a['request_id']
        if (request_id != b['request_id'] or request_id in seen or a['user'] != b['user']
                or a['has_truth'] != b['has_truth']):
            raise ValueError('Compared queries differ or contain duplicate request IDs')
        seen.add(request_id)
        if a['has_truth']:
            by_user.setdefault(a['user'], []).append(a['ndcg']-b['ndcg'])
    if len(by_user)<2:
        return {'users': len(by_user), 'mean_delta': None, 'ci95': [None, None]}
    sums = np.asarray([sum(x) for x in by_user.values()])
    counts = np.asarray([len(x) for x in by_user.values()])
    rng = np.random.default_rng(seed)
    samples = []
    for _ in range(repeats):
        ix = rng.integers(0, len(sums), len(sums))
        samples.append(float(sums[ix].sum()/counts[ix].sum()))
    return {'users': len(by_user), 'mean_delta': float(sums.sum()/counts.sum()),
            'ci95': [float(v) for v in np.quantile(samples, [.025, .975])]}


def train(database, config, output):
    started = time.monotonic()
    c = json.loads(Path(config).read_text('utf-8'))
    validate_config(c)
    c['cf_cutoff_ts'] = timestamp(c['cf_cutoff'])
    source_manifest = Path(database).with_suffix('.manifest.json')
    if not source_manifest.exists() or not json.loads(source_manifest.read_text('utf-8')).get('complete'):
        raise ValueError('Complete sampling manifest required')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    events, sessions = read_events(database, c['session_gap_seconds'], c.get('session_gap_boundary', 'at_least'))
    if not events:
        raise ValueError('Empty dataset')
    plan, query_stats = query_plan(events, sessions, c)
    print(f'Loaded {len(events):,} events, {len(sessions):,} sessions; selected queries {Counter(plan.values())}', flush=True)
    # Sequential coordinate search. Validation data is never used in CF fitting.
    trials, fitted = [], {}
    for name, w in WEIGHTS.items():
        tick = time.monotonic()
        neighbors, support = cf.fit(events, c['cf_cutoff_ts'], w, c)
        q = replay(events, sessions, plan, neighbors, WEIGHTS['initial'], c, {'validation'})['validation']
        metric, _ = evaluate(q, 'cf')
        trials.append({'phase': 'cf_training', 'weights': name, 'validation': metric, 'support': support})
        fitted[name] = neighbors
        print(f'CF {name}: items={support["items"]}, val candidate recall={metric["purchase_candidate_recall_at_100"]}, {time.monotonic()-tick:.1f}s', flush=True)
    def key(trial):
        m = trial['validation']
        return (m['purchase_candidate_recall_at_100'] if m['purchase_candidate_recall_at_100'] is not None else -1,
                m['purchase_ndcg_at_10'] if m['purchase_ndcg_at_10'] is not None else -1)
    best_cf = max(trials, key=key)['weights']
    neighbors = fitted[best_cf]
    del fitted
    request_trials = []
    for name, w in WEIGHTS.items():
        q = replay(events, sessions, plan, neighbors, w, c, {'validation'})['validation']
        metric, _ = evaluate(q, 'cf')
        request_trials.append({'phase': 'request', 'weights': name, 'validation': metric})
        print(f'Request {name}: val candidate recall={metric["purchase_candidate_recall_at_100"]}', flush=True)
    best_request = max(request_trials, key=key)['weights']
    trials.extend(request_trials)
    print(f'Selected CF={best_cf}, request={best_request}; building ranking data', flush=True)
    queries = replay(events, sessions, plan, neighbors, WEIGHTS[best_request], c, {'rank_train', 'validation', 'test', 'future'})
    x, y, groups = pack_rank_data(queries['rank_train'])
    vx, vy, vg = pack_rank_data(queries['validation'])
    if len(groups)<c['min_rank_groups']:
        raise ValueError(f'Only {len(groups)} informative training groups; increase sample size.')
    model = lgb.LGBMRanker(objective='lambdarank', label_gain=[0, 1], metric='ndcg',
                          n_estimators=c['trees'], learning_rate=.05, num_leaves=15,
                          max_depth=5, min_child_samples=20, reg_lambda=1.,
                          random_state=c['seed'], n_jobs=c['threads'], verbosity=-1,
                          deterministic=True, force_col_wise=True)
    model.fit(x, y, group=groups, feature_name=FEATURES,
              eval_set=[(vx, vy)], eval_group=[vg], eval_at=[10],
              callbacks=[lgb.early_stopping(20, verbose=False)])
    booster = model.booster_
    metrics, raw_rows = {}, {}
    predictions = {}
    for split in ['validation', 'test', 'future']:
        predictions[split] = predict_all(booster, queries[split], c['threads'])
        metrics[split], raw_rows[split] = {}, {}
        for mode in ['popular', 'cf', 'ranker']:
            metrics[split][mode], raw_rows[split][mode] = evaluate(queries[split], mode, predictions[split] if mode=='ranker' else None)
    comparisons = {m: improvement_ci(raw_rows['test']['ranker'], raw_rows['test'][m], c['bootstrap_repeats'], c['seed']) for m in ['popular', 'cf']}
    primary = metrics['test']['ranker']
    enough = primary['queries_with_eligible_future_purchase'] >= c['min_evaluation_purchase_queries']
    improves = all(v['ci95'][0] is not None and v['ci95'][0]>0 for v in comparisons.values())
    recall_ok = all(primary['purchase_recall_at_10'] is not None and
                    primary['purchase_recall_at_10'] >= (metrics['test'][m]['purchase_recall_at_10'] or 0)*.98 for m in ['popular', 'cf'])
    accepted = bool(enough and improves and recall_ok)
    promotion = {'accepted': accepted, 'minimum_purchase_queries': enough,
                 'positive_user_bootstrap_ci_vs_both': improves, 'recall_guardrail': recall_ok,
                 'decision': 'eligible_for_activation' if accepted else 'keep_current_or_purchase_popularity',
                 'note': 'November future audit is reported after the October decision; it is not used for selection.'}
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    bundle = output/'bundles'/run_id
    bundle.mkdir(parents=True)
    write_json(bundle/'neighbors.json', neighbors)
    retriever_version = 'cf-'+checksum(bundle/'neighbors.json')[:20]
    booster.save_model(str(bundle/'ranker.txt'))
    write_json(bundle/'policy.json', {**c, 'cf_weight_name': best_cf, 'request_weight_name': best_request,
                                     'cf_weights': WEIGHTS[best_cf], 'request_weights': WEIGHTS[best_request]})
    write_json(bundle/'feature_schema.json', {'features': FEATURES, 'dtype': 'float32', 'version': 1,
                                            'label': 'future purchase of candidate in same mature canonical session',
                                            'label_gain': [0, 1]})
    # CSV can be COPY-imported into the supplied PostgreSQL schema by the service owner.
    import csv
    with (bundle/'item_neighbors.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['retriever_version','source_item_id','neighbor_item_id','similarity','neighbor_rank'])
        for p, values in neighbors.items():
            writer.writerows((retriever_version, p, n, s, rank+1) for rank, (n, s) in enumerate(values))
    report = {'bundle_version': run_id, 'data_manifest': json.loads(source_manifest.read_text('utf-8')),
              'query_plan': query_stats, 'metrics': metrics, 'weight_trials': trials,
              'selected_cf_weights': best_cf, 'selected_request_weights': best_request,
              'rank_training': {'groups': len(groups), 'rows': len(y), 'validation_groups': len(vg),
                                'best_iteration': booster.current_iteration(),
                                'feature_importance_gain': dict(zip(FEATURES, map(float, booster.feature_importance('gain'))))},
              'test_comparisons': comparisons, 'promotion': promotion,
              'runtime_seconds': round(time.monotonic()-started, 2),
              'process_peak_working_set_mb': round(getattr(psutil.Process().memory_info(), 'peak_wset', psutil.Process().memory_info().rss)/1024**2, 2),
              'limitations': [
                  'Deterministic 1/modulus user sample; purchase co-occurrences can be sparse.',
                  'Only first view/cart per mature session is sampled; production refreshes every action.',
                  'No history before October 1: October 30-day purchase exclusion has partial coverage; November has prior October history.',
                  'No exposure logs: label 0 means no observed later same-session purchase, not dislike.',
                  'Current detail item and last-30-day purchases are excluded from targets; excluded counts are reported.',
                  'CF is frozen at October 8 through November audit; this is not a proven best or refreshed production model.',
                  'Offline full-file event-time replay assumes ordered, finalized data; late-event reconciliation is an integration contract.',
                  'Local RDB is SQLite; PostgreSQL schema and CSV are supplied but AWS/Redis/PostgreSQL are not live integrations.',
                  'No purchase conversion uplift or calibrated purchase probabilities are established.'
              ]}
    examples = []
    for q, pred in list(zip(queries['future'], predictions['future']))[:10]:
        examples.append({'request_id': q['request_id'], 'as_of': iso(q['as_of']),
                         'current_item_id': q['current_item_id'], 'eligible_purchase_truth': sorted(q['truth']),
                         'recommended': ranked_items(q, 'ranker', pred),
                         'candidate_count': len(q['items'])})
    report['future_examples'] = examples
    write_json(bundle/'evaluation.json', report)
    files = ['neighbors.json', 'ranker.txt', 'policy.json', 'feature_schema.json', 'item_neighbors.csv', 'evaluation.json']
    manifest = {'bundle_version': run_id, 'retriever_version': retriever_version,
                'ranker_version': 'lgbm-'+checksum(bundle/'ranker.txt')[:20],
                'feature_schema_version': 1, 'session_policy_version': 'gap-1800-v1',
                'data_cutoff': c['cf_cutoff'], 'ranker_fit_window': c['rank_train'],
                'development_validation_window': c['validation'], 'promotion': promotion,
                'data_version': checksum(source_manifest), 'code_version': code_version(),
                'environment': {'lightgbm': lgb.__version__, 'numpy': np.__version__},
                'checksums': {name: checksum(bundle/name) for name in files}}
    write_json(bundle/'manifest.json', manifest)
    serving_db = output/'serving.sqlite'
    install(bundle, serving_db)
    # Real SQL batched lookup must preserve the numerical model artifact exactly.
    store = NeighborStore(serving_db, retriever_version)
    try:
        check_items = list(neighbors)[:20]
        if store.get_many(check_items) != {p: neighbors[p] for p in check_items}:
            raise ValueError('RDB and source relations differ')
    finally:
        store.close()
    write_json(output/'latest.json', {'bundle_path': str(bundle.resolve()), 'database': str(serving_db.resolve()),
                                      'activated': False, 'promotion': promotion})
    write_json(output/'report.json', report)
    write_report(output/'report.html', report)
    print(json.dumps({'bundle': str(bundle.resolve()), 'report': str((output/'report.html').resolve()),
                      'selected_weights': [best_cf, best_request], 'test': metrics['test'],
                      'future': metrics['future'], 'promotion': promotion}, ensure_ascii=False, indent=2), flush=True)
    return bundle


def write_report(file, report):
    import html
    sections = []
    for split in ['validation','test','future']:
        rows = ''
        for mode, m in report['metrics'][split].items():
            fmt = lambda v: 'N/A' if v is None else f'{v:.4f}'
            rows += '<tr>'+''.join(f'<td>{html.escape(str(v))}</td>' for v in [mode, m['queries_with_eligible_future_purchase'], fmt(m['purchase_candidate_recall_at_100']), fmt(m['purchase_hit_rate_at_10']), fmt(m['purchase_ndcg_at_10'])])+'</tr>'
        sections.append(f'<section><h2>{split}</h2><table><tr><th>방법</th><th>구매 정답 질의</th><th>후보 회수율@100</th><th>적중률@10</th><th>NDCG@10</th></tr>{rows}</table></section>')
    text = '<!doctype html><html lang="ko"><meta charset="utf-8"><title>실제 데이터 추천 모델 학습 결과</title><style>body{font:16px/1.6 system-ui;max-width:1100px;margin:40px auto;padding:20px;background:#f5f7f5;color:#14372e}section{background:white;padding:24px;border-radius:14px;margin:18px 0}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:10px;border-bottom:1px solid #ddd}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>CF + LightGBM LambdaRank 실제 학습 결과</h1><p>이번 방문의 미래 구매를 정답으로 평가합니다. 구매가 있는 질의에 조건부인 지표이며 실제 구매 전환율이 아닙니다.</p>'
    text += '<section><b>배포 판정</b><pre>'+html.escape(json.dumps(report['promotion'], ensure_ascii=False, indent=2))+'</pre><b>선택 가중치</b><p>CF 학습: '+report['selected_cf_weights']+' / 요청: '+report['selected_request_weights']+'</p></section>'
    text += ''.join(sections)+'<section><h2>한계와 구현 범위</h2><ul>'+''.join('<li>'+html.escape(s)+'</li>' for s in report['limitations'])+'</ul></section></html>'
    Path(file).write_text(text, 'utf-8')
