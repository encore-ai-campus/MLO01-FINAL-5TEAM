"""Build and verify a local Redis replay bootstrap from Delta event and feature history."""
import argparse
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import redis
from pyspark.sql import Window, functions as F

from verify_tools import spark_session


GAP = 1800
DAY = 86400
TEST_USER = 9_000_000_001
EXPIRED_USER = 9_000_000_002
FUTURE_USER = 9_000_000_003
TEST_ITEM = 9_000_000_004


def stage_events(spark, inputs, root, start):
    columns = ('event_id', 'event_time', 'source', 'event_seq', 'user_id', 'product_id', 'event_type')
    raw = spark.read.parquet(*inputs).select(*columns)
    first = (raw.where((F.col('event_time') >= start) & F.col('event_type').isin('view', 'cart'))
             .orderBy('event_time', 'source', 'event_seq').first())
    if first is None or first.event_time.replace(tzinfo=timezone.utc) != start:
        raise ValueError('Choose a cutoff with an actual first request at that timestamp')
    # Keep explicit horizon-boundary cases beside the real input.
    rows = [
        (-30 * DAY, TEST_USER, TEST_ITEM + 1, 'purchase'),
        (-30 * DAY + 1, TEST_USER, TEST_ITEM + 2, 'purchase'),
        (-1800, TEST_USER, TEST_ITEM, 'view'),
        (-1800, TEST_USER, TEST_ITEM, 'cart'),
        (-1801, EXPIRED_USER, TEST_ITEM, 'view'),
        (0, TEST_USER, TEST_ITEM + 3, 'purchase'),
        (0, FUTURE_USER, TEST_ITEM, 'view'),
        (1, FUTURE_USER, TEST_ITEM, 'view'),
    ]
    fixture = spark.createDataFrame(
        [(f'bootstrap-fixture:{seq}', start + timedelta(seconds=delta), 'bootstrap-fixture', seq,
          user, item, action) for seq, (delta, user, item, action) in enumerate(rows)],
        schema=raw.schema)
    table = str(root / 'events')
    raw.where(F.col('event_time') <= start + timedelta(minutes=2)).unionByName(fixture).write.format('delta').save(table)
    return spark.read.format('delta').load(table), first


def online_state(events, start, users):
    before = events.where((F.col('event_time') < start) & F.col('user_id').isin(users))
    order = Window.partitionBy('user_id').orderBy('event_time', 'source', 'event_seq')
    previous = F.lag('event_time').over(order)
    visits = (before.withColumn('gap_seconds', F.unix_timestamp('event_time') - F.unix_timestamp(previous))
              .withColumn('new_visit', F.when(F.col('gap_seconds').isNull() | (F.col('gap_seconds') > GAP), 1).otherwise(0))
              .withColumn('visit', F.sum('new_visit').over(order.rowsBetween(Window.unboundedPreceding, 0))))
    grouped = visits.groupBy('user_id', 'visit').agg(
        F.min(F.struct('event_time', 'source', 'event_seq', 'event_id')).alias('first'),
        F.max(F.struct('event_time', 'source', 'event_seq', 'event_id')).alias('last'),
        F.count('*').alias('events'), F.count_if(F.col('event_type') == 'view').alias('views'),
        F.count_if(F.col('event_type') == 'cart').alias('carts'),
        F.slice(F.sort_array(F.collect_list(F.when(F.col('event_type') == 'view',
            F.struct('event_time', 'source', 'event_seq', 'product_id'))), asc=False), 1, 20).alias('recent'),
        F.sort_array(F.collect_set(F.when(F.col('event_type') == 'cart', F.col('product_id')))).alias('carted'))
    current = (grouped.withColumn('latest', F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc('visit'))))
               .where((F.col('latest') == 1) & (F.col('last.event_time') >= start - timedelta(seconds=GAP))))
    sessions = {row.user_id: {
        'start_event_id': row.first.event_id, 'last_event_id': row.last.event_id, 'events': row.events,
        'views': row.views, 'carts': row.carts,
        'recent_view_product_ids': [view.product_id for view in row.recent],
        'carted_product_ids': row.carted,
        'elapsed_seconds': int((start.replace(tzinfo=None) - row.first.event_time).total_seconds()),
    } for row in current.collect()}
    purchases = {}
    for row in (before.where((F.col('event_type') == 'purchase') & (F.col('event_time') > start - timedelta(days=30)))
                .groupBy('user_id', 'product_id').agg(F.max(F.unix_timestamp('event_time')).alias('bought_at')).collect()):
        purchases.setdefault(row.user_id, {})[str(row.product_id)] = row.bought_at
    return sessions, purchases


def batch_history(spark, events, start, users, items, root):
    as_of = start.replace(hour=0, minute=0, second=0)
    available = as_of + timedelta(minutes=5)
    if available >= start:
        raise ValueError('Choose a start after the local daily publication time')
    prefix = events.where(F.col('event_time') < as_of)
    user = (prefix.where(F.col('user_id').isin(users) & (F.col('event_time') >= as_of - timedelta(days=30)))
            .groupBy(F.col('user_id').alias('entity'))
            .agg(F.count_if(F.col('event_type') == 'view').alias('metric_a'),
                 F.count_if(F.col('event_type') == 'purchase').alias('metric_b'))
            .withColumn('kind', F.lit('user')))
    item = (prefix.where(F.col('product_id').isin(items) & (F.col('event_time') >= as_of - timedelta(days=7)))
            .groupBy(F.col('product_id').alias('entity'))
            .agg(F.count_if((F.col('event_type') == 'view') & (F.col('event_time') >= as_of - timedelta(days=1))).alias('metric_a'),
                 F.count_if(F.col('event_type') == 'view').alias('metric_b'))
            .withColumn('kind', F.lit('item')))
    valid = user.unionByName(item).withColumn('as_of', F.lit(as_of)).withColumn('available_at', F.lit(available))
    # Later versions deliberately hold conspicuously different values, including an exact-boundary revision.
    probes = valid.withColumn('metric_a', F.col('metric_a') + 999_999)
    history = valid.unionByName(probes.withColumn('available_at', F.lit(start))).unionByName(
        probes.withColumn('available_at', F.lit(start + timedelta(seconds=1))))
    table = str(root / 'batch_history')
    history.write.format('delta').save(table)
    rank = Window.partitionBy('kind', 'entity').orderBy(F.desc('available_at'), F.desc('as_of'))
    selected = (spark.read.format('delta').load(table).where(F.col('available_at') < start)
                .withColumn('rank', F.row_number().over(rank)).where('rank = 1'))
    batches = {}
    for row in selected.collect():
        names = ('views_30d', 'purchases_30d') if row.kind == 'user' else ('views_1d', 'views_7d')
        batches[row.kind, row.entity] = {
            names[0]: row.metric_a, names[1]: row.metric_b,
            'as_of': row.as_of.replace(tzinfo=timezone.utc).isoformat(),
            'available_at': row.available_at.replace(tzinfo=timezone.utc).isoformat(),
        }
    return batches, as_of, available, history.count() - selected.count()


def offline_reference(events, start, as_of, available, users, items):
    start = start.replace(tzinfo=None)
    as_of = as_of.replace(tzinfo=None)
    sessions, purchases, user_totals, item_totals = {}, {}, {}, {}
    sample = events.where(F.col('user_id').isin(users) | F.col('product_id').isin(items))
    for row in sample.orderBy('event_time', 'source', 'event_seq').toLocalIterator():
        at = row.event_time
        if at >= start:
            break
        if row.user_id in users:
            state = sessions.get(row.user_id)
            if state is None or (at - state['last']).total_seconds() > GAP:
                state = {'first': at, 'start_event_id': row.event_id, 'events': 0, 'views': 0, 'carts': 0,
                         'recent_view_product_ids': [], 'carted_product_ids': set()}
                sessions[row.user_id] = state
            state['last'], state['last_event_id'] = at, row.event_id
            state['events'] += 1
            state['views'] += row.event_type == 'view'
            state['carts'] += row.event_type == 'cart'
            if row.event_type == 'view':
                state['recent_view_product_ids'].append(row.product_id)
            if row.event_type == 'cart':
                state['carted_product_ids'].add(row.product_id)
            if row.event_type == 'purchase' and at > start - timedelta(days=30):
                purchases.setdefault(row.user_id, {})[str(row.product_id)] = int(at.replace(tzinfo=timezone.utc).timestamp())
            if as_of - timedelta(days=30) <= at < as_of:
                user_totals.setdefault(row.user_id, Counter())[row.event_type] += 1
        if row.product_id in items and as_of - timedelta(days=7) <= at < as_of:
            totals = item_totals.setdefault(row.product_id, Counter())
            if row.event_type == 'view':
                totals['week'] += 1
                totals['day'] += at >= as_of - timedelta(days=1)
    active = {}
    for user, state in sessions.items():
        if (start - state.pop('last')).total_seconds() <= GAP:
            state['elapsed_seconds'] = int((start - state.pop('first')).total_seconds())
            state['recent_view_product_ids'] = list(reversed(state['recent_view_product_ids'][-20:]))
            state['carted_product_ids'] = sorted(state['carted_product_ids'])
            active[user] = state
    stamp = {'as_of': as_of.replace(tzinfo=timezone.utc).isoformat(), 'available_at': available.isoformat()}
    batches = {('user', u): {'views_30d': c['view'], 'purchases_30d': c['purchase'], **stamp}
               for u, c in user_totals.items()}
    batches.update({('item', i): {'views_1d': c['day'], 'views_7d': c['week'], **stamp}
                    for i, c in item_totals.items()})
    return active, purchases, batches


def payloads(users, items, sessions, purchases, batches):
    result = {}
    for user in users:
        result[f'session:{user}'] = sessions.get(user)
        result[f'purchase:{user}'] = purchases.get(user, {})
        result[f'batch:user:{user}'] = batches.get(('user', user))
    for item in items:
        result[f'batch:item:{item}'] = batches.get(('item', item))
    return result


def run(spark, root, inputs, start, redis_url):
    events, first = stage_events(spark, inputs, root, start)
    print(f'First request {first.event_id}; calculating bootstrap', flush=True)
    users = sorted({first.user_id, TEST_USER, EXPIRED_USER, FUTURE_USER})
    items = sorted({first.product_id, TEST_ITEM})
    sessions, purchases = online_state(events, start, users)
    batches, as_of, available, rejected = batch_history(spark, events, start, users, items, root)
    offline = offline_reference(events, start, as_of, available, users, items)
    expected = payloads(users, items, *offline)
    initial = payloads(users, items, sessions, purchases, batches)
    namespace = f'bootstrap:{uuid4().hex}:'
    with redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=5) as client:
        keys = [namespace + key for key in initial]
        for _ in range(2):
            with client.pipeline(transaction=True) as pipeline:
                for key, value in initial.items():
                    pipeline.set(namespace + key, json.dumps(value, sort_keys=True))
                pipeline.execute()
            actual = dict(zip(initial, map(json.loads, client.mget(keys)), strict=True))
            assert actual == expected
            assert set(client.scan_iter(match=f'{namespace}*')) == set(keys)

    assert actual[f'session:{TEST_USER}']['events'] == 2
    assert actual[f'session:{TEST_USER}']['elapsed_seconds'] == GAP
    assert actual[f'session:{TEST_USER}']['recent_view_product_ids'] == [TEST_ITEM]
    assert actual[f'session:{TEST_USER}']['carted_product_ids'] == [TEST_ITEM]
    assert actual[f'purchase:{TEST_USER}'] == {str(TEST_ITEM + 2): int((start - timedelta(days=30) + timedelta(seconds=1)).timestamp())}
    assert actual[f'session:{EXPIRED_USER}'] is None
    assert actual[f'session:{FUTURE_USER}'] is None and actual[f'purchase:{FUTURE_USER}'] == {}
    assert rejected > 0
    frozen = events.where(F.col('event_time') < start)
    same_or_after = events.where(F.col('event_time') >= start).count()
    assert same_or_after > 2
    report = {'cutoff_utc': start.isoformat(), 'redis_namespace': namespace, 'pre_cutoff_events': frozen.count(),
              'cutoff_or_later_events_excluded': same_or_after, 'batch_revisions_excluded': rejected,
              'redis_keys_verified': len(keys), 'users_checked': len(users), 'items_checked': len(items),
              'redis_mismatches': 0, 'same_cutoff_reload_key_count': len(keys),
              'first_request_event_id': first.event_id, 'first_request_user_id': first.user_id,
              'first_request_session_active_before_input': actual[f'session:{first.user_id}'] is not None,
              'first_request_keys': {key: actual[key] for key in (
                  f'session:{first.user_id}', f'purchase:{first.user_id}', f'batch:user:{first.user_id}',
                  f'batch:item:{first.product_id}')},
              'fixture_checks': ['exact 30-minute session stays active', '30-minute-plus-one session expired',
                                 '30-day-old purchase excluded, newer purchase kept',
                                 'events exactly at and after cutoff excluded', 'batch availability equal/after excluded']}
    (root / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', nargs='+', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--redis-url', default='redis://redis:6379/0')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    start = datetime.fromisoformat(args.start)
    if start.utcoffset() is None or start.microsecond:
        parser.error('Provide a timezone-aware start at whole-second precision')
    root = args.output.resolve()
    root.mkdir(parents=True)
    spark = spark_session(root)
    spark.sparkContext.setLogLevel('ERROR')
    try:
        run(spark, root, args.input, start.astimezone(timezone.utc), args.redis_url)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
