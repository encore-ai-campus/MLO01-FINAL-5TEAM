"""Replay inference adapter: a real RDB read plus saved LightGBM prediction.

Production callers replace State replay with the same-schema Redis snapshot.
"""
import json
import sqlite3
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np

from .core import State, generate, iso, read_events, timestamp
from .store import NeighborStore, checksum, install
from .temporal_cf import cutoff_for_time


def select_retriever(manifest, config, as_of):
    snapshots = manifest.get('retriever_snapshots')
    if not snapshots:
        return manifest['retriever_version'], config.get('cf_cutoff_ts', timestamp(config['cf_cutoff']))
    required = cutoff_for_time(as_of, config['cf_cutoff_ts'], config.get('cf_refresh_days', 0))
    snapshot = next((s for s in snapshots if s['cutoff_ts'] == required), None)
    if snapshot is None:
        raise ValueError('Required past-only CF snapshot is unavailable. Build it before replaying this request.')
    return snapshot['version'], required


def recommend(events_db, serving_db, bundle=None, request_id=None, user_id=None, as_of=None):
    explicit_trial = bundle is not None
    if bundle is None:
        conn = sqlite3.connect(serving_db)
        active = conn.execute('SELECT bundle_path FROM active_bundle WHERE slot=1').fetchone()
        conn.close()
        if active is None:
            raise ValueError('No activated model. For an explicit local trial, provide --bundle.')
        bundle = active[0]
    bundle = Path(bundle).resolve()
    manifest = install(bundle, serving_db)
    c = json.loads((bundle/'policy.json').read_text('utf-8'))
    events, _ = read_events(events_db, c['session_gap_seconds'], c.get('session_gap_boundary', 'at_least'))
    limit = timestamp(as_of) if as_of else None
    matches = [e for e in events if (request_id is None or e.event_id==request_id)
               and (user_id is None or e.user==user_id) and (limit is None or e.ts<=limit)]
    if not matches or (request_id is None and user_id is None):
        raise ValueError('Supply a known --event-id or --user-id with an optional --as-of timestamp')
    request = matches[-1]
    # A later ranker cannot be used to claim historical real-time predictions.
    if request.ts < timestamp(c['test'][1]):
        raise ValueError('Use a request after the October evaluation window; earlier requests would use a future-trained ranker.')
    state = State(c)
    for e in events:
        if e.order > request.order:
            break
        state.observe(e)
    retriever_version, retriever_cutoff = select_retriever(manifest, c, request.ts)
    store = NeighborStore(serving_db, retriever_version)
    booster = lgb.Booster(model_file=str(bundle/'ranker.txt'))
    started = time.perf_counter()
    try:
        row = generate(state, request, store, tuple(c['request_weights']))
        scores = booster.predict(row['features'], num_threads=c['threads']) if len(row['items']) else np.empty(0)
        ix = sorted(range(len(scores)), key=lambda i: (-float(scores[i]), row['items'][i]))[:c['recommendation_limit']]
        items = []
        for rank, i in enumerate(ix, 1):
            p = row['items'][i]
            sources = row['sources'][p]
            reason = ('이번 방문에 관심을 보인 상품' if 'session_interest' in sources else
                      '최근 관심 상품과 함께 관심받은 상품' if 'cf' in sources else
                      '최근 구매 인기 상품' if 'purchase_popular' in sources else '추천 정보가 부족해 상품 목록에서 보충')
            items.append({'item_id': p, 'rank': rank, 'rank_score': float(scores[i]),
                          'metadata': state.metadata[p], 'sources': sources, 'reason': reason,
                          'cf_score': row['cf_scores'][i], 'evidence': row['evidence'].get(p, []),
                          'features': dict(zip(booster.feature_name(), map(float, row['features'][i])))})
        elapsed = (time.perf_counter()-started)*1000
    finally:
        store.close()
    return {'bundle_version': manifest['bundle_version'], 'retriever_version': retriever_version,
            'retriever_cutoff': iso(retriever_cutoff),
            'ranker_version': manifest['ranker_version'], 'features_as_of': iso(request.ts),
            'traffic_mode': 'offline_replay', 'local_trial': explicit_trial,
            'promotion_accepted': manifest['promotion']['accepted'], 'candidate_count': len(row['items']),
            'candidate_and_rank_ms': round(elapsed, 3),
            'latency_scope': 'local SQLite lookup + candidate/features/ranker; excludes replay, API, Redis, network',
            'items': items, 'score_semantics': 'ranking score, not purchase probability'}
