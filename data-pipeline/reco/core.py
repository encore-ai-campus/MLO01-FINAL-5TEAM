"""Pure temporal policies and features shared by training and serving."""
import math
import sqlite3
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

DAY = 86400
WEIGHTS = {'equal': (1., 1., 1.), 'mild': (1., 2., 3.),
           'initial': (1., 3., 5.), 'purchase_only': (0., 0., 1.)}
FEATURES = ['cf_score', 'cf_rank', 'source_cf', 'source_session', 'source_popular',
            'session_viewed', 'session_carted', 'session_views', 'session_carts',
            'session_elapsed_log', 'session_actions_log', 'category_affinity',
            'brand_affinity', 'price_distance', 'price_missing',
            'purchase_popularity_log', 'last_item_gap_log', 'history_size_log']


def timestamp(value):
    return int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp())


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


@dataclass(slots=True)
class Event:
    event_id: str
    ts: int
    row_no: int
    source: str
    user: int
    item: int
    action: int
    category: str
    brand: str
    price: float | None
    raw_session: str
    session: str = ''
    order: int = 0


def starts_new_session(previous_ts, ts, gap, boundary='at_least'):
    gap_seconds = ts - previous_ts
    if boundary == 'over':
        return gap_seconds > gap
    if boundary == 'at_least':
        return gap_seconds >= gap
    raise ValueError('Unknown session boundary: ' + str(boundary))


def session_for(event, previous, gap, boundary):
    if previous is None or starts_new_session(previous.ts, event.ts, gap, boundary):
        return f'{event.user}:{event.event_id}'
    return previous.session


def session_close_at(last_event_ts, gap, boundary):
    """First whole second at which the visit can no longer accept another event."""
    return last_event_ts + gap + int(boundary == 'over')


def read_events(db, gap, boundary='at_least'):
    conn = sqlite3.connect(db)
    events = [Event(*r) for r in conn.execute('SELECT * FROM events ORDER BY ts,source,row_no')]
    conn.close()
    sessions = defaultdict(list)
    last = {}
    for order, e in enumerate(events):
        prev = last.get(e.user)
        e.session = session_for(e, prev, gap, boundary)
        e.order = order
        sessions[e.session].append(e)
        last[e.user] = e
    return events, dict(sessions)


class State:
    """Replay-only state. Features never read session outcomes or future rows."""
    def __init__(self, config):
        self.c = config
        self.history = defaultdict(deque)
        self.purchases = defaultdict(deque)
        self.metadata = {}
        self.purchase_window = deque()
        self.pair_counts = Counter()
        self.popularity = Counter()
        self.now = 0
        self.last_live_position = None

    def observe_live(self, event):
        """Apply a unique, ordered event from the local serving replay."""
        position = (event.ts, event.source, event.row_no)
        if self.last_live_position is not None and position <= self.last_live_position:
            raise ValueError('Live replay requires unique events in source order')
        history = self.history.get(event.user)
        event.session = session_for(event, history[-1] if history else None,
                                    self.c['session_gap_seconds'], self.c.get('session_gap_boundary', 'at_least'))
        self.observe(event)
        self.last_live_position = position

    def observe(self, e):
        if e.ts < self.now:
            raise ValueError('Events must arrive in event-time order')
        self.now = e.ts
        self.metadata[e.item] = {'category': e.category, 'brand': e.brand,
                                 'price': e.price, 'as_of': e.ts}
        h = self.history[e.user]
        while h and h[0].ts < e.ts-self.c['history_days']*DAY and h[0].session != e.session:
            h.popleft()
        h.append(e)
        bought = self.purchases[e.user]
        while bought and bought[0][0] <= e.ts-self.c['purchase_exclusion_days']*DAY:
            bought.popleft()
        if e.action == 2:
            bought.append((e.ts, e.item))
            self.purchase_window.append((e.ts, e.user, e.item))
            key = (e.user, e.item)
            if self.pair_counts[key] == 0:
                self.popularity[e.item] += 1
            self.pair_counts[key] += 1
        while self.purchase_window and self.purchase_window[0][0] < e.ts-7*DAY:
            _, u, p = self.purchase_window.popleft()
            self.pair_counts[u, p] -= 1
            if self.pair_counts[u, p] == 0:
                del self.pair_counts[u, p]
                self.popularity[p] -= 1
                if self.popularity[p] == 0:
                    del self.popularity[p]

    def context(self, e):
        h = [x for x in self.history[e.user]
             if x.ts >= e.ts-self.c['history_days']*DAY or x.session == e.session]
        session = [x for x in h if x.session == e.session]
        excluded = {p for ts, p in self.purchases[e.user]
                    if ts > e.ts-self.c['purchase_exclusion_days']*DAY}
        # Current item is only excluded on a view/detail-page request.
        if e.action == 0:
            excluded.add(e.item)
        return h, session, excluded

    def popular(self, e, excluded, limit):
        category = self.metadata[e.item]['category']
        ordered = sorted(self.popularity, key=lambda p: (-self.popularity[p], p))
        category_items = [p for p in ordered if self.metadata[p]['category'] == category]
        result = []
        for p in category_items + ordered:
            if p not in excluded and p not in result:
                result.append(p)
            if len(result) >= limit:
                break
        # No purchase support: deterministic known-catalog fallback.
        if len(result) < limit:
            for p in sorted(self.metadata):
                if p not in excluded and p not in result:
                    result.append(p)
                if len(result) >= limit:
                    break
        return result


def generate(state, e, neighbors, weights, *, purchase_candidates=(), purchase_slots=0,
             metadata_candidates=(), prior_interest_candidates=()):
    c = state.c
    history, session, excluded = state.context(e)
    interest = {}
    for x in history:
        prev = interest.get(x.item)
        interest[x.item] = (max(prev[0], x.action) if prev else x.action, x.ts)
    seeds = sorted(interest, key=lambda p: (-interest[p][1], p))[:c['history_items']]
    relation_rows = neighbors.get_many(seeds) if hasattr(neighbors, 'get_many') else {p: neighbors.get(p, []) for p in seeds}
    scores, reasons = defaultdict(float), defaultdict(list)
    for p in seeds:
        action, last = interest[p]
        decay = 2. ** (-(e.ts-last)/(c['half_life_hours']*3600))
        for other, similarity in relation_rows.get(p, []):
            if other in excluded or other not in state.metadata or weights[action] == 0:
                continue
            value = weights[action]*decay*similarity
            scores[other] += value
            reasons[other].append({'item_id': p, 'action': action, 'similarity': similarity,
                                   'weight': weights[action], 'decay': decay, 'contribution': value})
    cf_order = sorted(scores, key=lambda p: (-scores[p], p))
    cf_ranks = {p: i+1 for i, p in enumerate(cf_order)}
    direct = {}
    for x in session:
        if x.action < 2 and x.item not in excluded:
            prev = direct.get(x.item)
            direct[x.item] = (max(prev[0], x.action) if prev else x.action, x.ts)
    direct_order = sorted(direct, key=lambda p: (-direct[p][0], -direct[p][1], p))[:c['direct_items']]
    candidates = list(direct_order)
    sources = defaultdict(set)
    for p in direct_order:
        sources[p].add('session_interest')
    for items, source in ((metadata_candidates, 'product_metadata'),
                          (prior_interest_candidates, 'prior_visit_interest')):
        for p in items:
            if len(candidates) >= c['candidate_limit']:
                break
            if p not in excluded and p in state.metadata and p not in candidates:
                candidates.append(p)
                sources[p].add(source)
    if purchase_slots > 0:
        remaining = min(purchase_slots, c['candidate_limit'] - len(candidates))
        for p in purchase_candidates:
            if remaining <= 0:
                break
            if p not in excluded and p in state.metadata and p not in candidates:
                candidates.append(p)
                sources[p].add('view_then_purchase')
                remaining -= 1
    for p in cf_order:
        if p in candidates:
            sources[p].add('cf')
            continue
        if len(candidates) < c['candidate_limit']:
            candidates.append(p)
            sources[p].add('cf')
    popular = state.popular(e, excluded, c['candidate_limit'])
    for p in popular:
        if len(candidates) >= c['candidate_limit']:
            break
        if p not in candidates:
            candidates.append(p)
            sources[p].add('purchase_popular' if state.popularity[p] else 'catalog_fallback')
    candidates = candidates[:c['candidate_limit']]
    categories, brands = Counter(), Counter()
    for p in interest:
        categories[state.metadata[p]['category']] += 1
        brands[state.metadata[p]['brand']] += 1
    known_prices = [state.metadata[p]['price'] for p in interest if state.metadata[p]['price'] is not None]
    median_price = float(np.median(known_prices)) if known_prices else None
    action_counts = Counter((x.item, x.action) for x in session)
    last_times = {x.item: x.ts for x in history}
    elapsed = e.ts-session[0].ts if session else 0
    features = []
    for p in candidates:
        m = state.metadata[p]
        missing = median_price is None or m['price'] is None
        price_distance = abs(math.log1p(max(0., m['price']))-math.log1p(max(0., median_price))) if not missing else 0.
        features.append([scores[p], cf_ranks.get(p, c['history_items']*c['neighbors_per_item']+1),
                         int('cf' in sources[p]), int('session_interest' in sources[p]),
                         int('purchase_popular' in sources[p] or 'catalog_fallback' in sources[p]),
                         int(action_counts[p, 0]>0), int(action_counts[p, 1]>0),
                         math.log1p(action_counts[p, 0]), math.log1p(action_counts[p, 1]),
                         math.log1p(elapsed), math.log1p(len(session)),
                         categories[m['category']]/max(1, len(interest)),
                         brands[m['brand']]/max(1, len(interest)), price_distance, int(missing),
                         math.log1p(state.popularity[p]),
                         math.log1p(e.ts-last_times[p]) if p in last_times else -1.,
                         math.log1p(len(interest))])
    return {'items': candidates, 'features': np.asarray(features, dtype=np.float32).reshape((-1, len(FEATURES))),
            'cf_scores': [scores[p] for p in candidates],
            'sources': {p: sorted(sources[p]) for p in candidates},
            'evidence': {p: reasons[p] for p in candidates},
            'popular': popular[:c['recommendation_limit']], 'excluded': excluded}


def targets(session, request, known_items, excluded):
    future = {x.item for x in session if x.order > request.order and x.action == 2}
    allowed = (future & known_items)-excluded
    return allowed, {'all_future_purchases': len(future),
                     'unknown_future_products': len(future-known_items),
                     'policy_excluded_purchases': len(future & known_items & excluded)}
