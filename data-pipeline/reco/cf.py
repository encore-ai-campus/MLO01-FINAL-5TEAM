"""Bounded-memory item-based CF with weighted implicit user-item vectors."""
from collections import Counter, defaultdict
from itertools import groupby
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json
import sqlite3

import numpy as np
from scipy import sparse
from .core import starts_new_session, timestamp


def training_bounds(cutoff, c):
    mode=c.get('cf_history_mode','cumulative')
    if mode not in {'cumulative','rolling'}:
        raise ValueError('cf_history_mode must be cumulative or rolling')
    start=c.get('cf_history_start')
    lower=timestamp(start) if start else None
    if mode=='rolling':
        days=c.get('cf_lookback_days',30)
        if type(days) is not int or days<=0:
            raise ValueError('Rolling CF requires positive cf_lookback_days')
        boundary=cutoff-days*86400
        lower=max(lower,boundary) if lower is not None else boundary
    if lower is not None and lower>=cutoff:
        raise ValueError('CF history start must precede the fit cutoff')
    return lower,cutoff


def cache_signature(c, weights):
    keys=['cf_history_mode','cf_history_start','cf_lookback_days','cf_user_items',
          'cf_min_users','cf_max_items','neighbors_per_item','cf_block_items','session_gap_seconds']
    settings={key:c.get(key) for key in keys}
    settings['session_gap_boundary']=c.get('session_gap_boundary','at_least')
    settings['cf_history_mode']=c.get('cf_history_mode','cumulative')
    settings['weights']=list(weights)
    h=hashlib.sha256(json.dumps(settings,sort_keys=True).encode())
    for name in ['cf.py','core.py']:
        h.update(Path(__file__).with_name(name).read_bytes())
    return h.hexdigest()


def fit(events, cutoff, weights, c):
    lower,_=training_bounds(cutoff,c)
    by_user = defaultdict(list)
    for e in events:
        if (lower is None or lower<=e.ts) and e.ts < cutoff:
            by_user[e.user].append(e)
    return _fit_histories(sorted(by_user.items()),cutoff,weights,c)


def fit_db(database, cutoff, weights, c):
    """Stream one user's history at a time instead of loading months of Events."""
    lower,_=training_bounds(cutoff,c)
    conn=sqlite3.connect(f'file:{Path(database).resolve().as_posix()}?mode=ro',uri=True)
    where='ts<?'+(' AND ts>=?' if lower is not None else '')
    params=(cutoff,lower) if lower is not None else (cutoff,)
    def records():
        previous_user,previous_time,session=None,None,None
        sql=f'SELECT user_id,item_id,ts,action,event_id FROM events WHERE {where} ORDER BY user_id,ts,source,row_no'
        for user,item,ts,action,event_id in conn.execute(sql,params):
            if user!=previous_user or starts_new_session(
                previous_time, ts, c['session_gap_seconds'], c.get('session_gap_boundary','at_least')
            ):
                session=f'{user}:{event_id}'
            yield SimpleNamespace(user=user,item=item,ts=ts,action=action,session=session)
            previous_user,previous_time=user,ts
    try:
        return _fit_histories(((user,list(rows)) for user,rows in groupby(records(),key=lambda e:e.user)),cutoff,weights,c)
    finally:
        conn.close()


def _fit_histories(histories, cutoff, weights, c):
    interactions = {}
    support = Counter()
    event_count=0;first_ts=None;last_ts=None
    for user, history in histories:
        event_count+=len(history)
        for e in history:
            first_ts=e.ts if first_ts is None else min(first_ts,e.ts)
            last_ts=e.ts if last_ts is None else max(last_ts,e.ts)
        last = {e.item: e.ts for e in history}
        ranked=sorted(last,key=lambda p:(-last[p],p))
        keep = set(ranked[:c['cf_user_items']] if c['cf_user_items'] else ranked)
        seen, counts = set(), defaultdict(float)
        for e in history:
            key = (e.item, e.session, e.action)
            if e.item in keep and key not in seen:
                counts[e.item] += weights[e.action]
                seen.add(key)
        counts = {p: v for p, v in counts.items() if v > 0}
        if counts:
            interactions[user] = counts
            support.update(counts.keys())
    items = sorted((p for p in support if support[p] >= c['cf_min_users']),
                   key=lambda p: (-support[p], p))
    if c['cf_max_items']:items=items[:c['cf_max_items']]
    item_map = {p: i for i, p in enumerate(items)}
    rows, cols, values = [], [], []
    for row, counts in enumerate(interactions.values()):
        for p, value in counts.items():
            if p in item_map:
                rows.append(row)
                cols.append(item_map[p])
                values.append(np.log1p(value))
    matrix = sparse.csr_matrix((np.asarray(values, dtype=np.float32), (rows, cols)),
                              shape=(len(interactions), len(items)), dtype=np.float32)
    norms = np.sqrt(np.asarray(matrix.power(2).sum(axis=0)).ravel())
    matrix = (matrix @ sparse.diags(np.divide(1., norms, out=np.zeros_like(norms), where=norms>0))).tocsc()
    neighbors = {}
    for start in range(0, len(items), c['cf_block_items']):
        products = (matrix[:, start:start+c['cf_block_items']].T @ matrix).tocsr()
        for local in range(products.shape[0]):
            index = start+local
            begin, end = products.indptr[local:local+2]
            pairs = [(items[int(j)], float(s)) for j, s in zip(products.indices[begin:end], products.data[begin:end])
                     if j != index and s > 0 and np.isfinite(s)]
            pairs.sort(key=lambda x: (-x[1], x[0]))
            neighbors[items[index]] = pairs[:c['neighbors_per_item']]
    return neighbors, {'users': len(interactions), 'items': len(items),
                       'nonzero_interactions': int(matrix.nnz),
                       'relations': sum(map(len, neighbors.values())),
                       'history_mode':c.get('cf_history_mode','cumulative'),
                       'events_considered':event_count,'oldest_event_ts':first_ts,
                       'newest_event_ts':last_ts,'cutoff_ts':cutoff,
                       'user_item_cap':c['cf_user_items'],'item_cap':c['cf_max_items']}
