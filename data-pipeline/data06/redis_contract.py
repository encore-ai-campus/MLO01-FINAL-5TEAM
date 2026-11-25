"""Run-scoped Redis keys and common payloads for online feature state."""
import json
from datetime import datetime, timezone


def run_prefix(run_id):
    return f'data06:{run_id}:'


def user_key(run_id, user_id):
    return f'{run_prefix(run_id)}user:{user_id}'


def utc_string(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def parse_time(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def batch_values(spark, run_id, path, replay_time):
    from pyspark.sql import Window, functions as F

    revisions = spark.read.format('delta').load(str(path))
    order = Window.partitionBy('kind', 'entity').orderBy(F.desc('available_at'), F.desc('as_of'))
    recent = (revisions.where(F.col('available_at') < F.to_timestamp(F.lit(replay_time)))
              .withColumn('rank', F.row_number().over(order)).where('rank = 1'))
    names = {'user': ('views_30d', 'purchases_30d'), 'item': ('views_1d', 'views_7d')}
    return {f'{run_prefix(run_id)}batch:{row.kind}:{row.entity}': {
        names[row.kind][0]: row.metric_a, names[row.kind][1]: row.metric_b,
        'as_of': utc_string(row.as_of), 'available_at': utc_string(row.available_at),
    } for row in recent.toLocalIterator()}


def replace_values(client, run_id, values, *, require_empty=False):
    prefix = run_prefix(run_id)
    old = list(client.scan_iter(match=prefix + '*'))
    if require_empty and old:
        raise ValueError('Bootstrap requires an empty Redis namespace')
    with client.pipeline(transaction=True) as pipeline:
        for key in old:
            pipeline.delete(key)
        for key, value in values.items():
            pipeline.set(key, json.dumps(value, sort_keys=True))
        pipeline.execute()
    restored = dict(zip(values, map(json.loads, client.mget(list(values))), strict=True)) if values else {}
    if restored != values or set(client.scan_iter(match=prefix + '*')) != set(values):
        raise RuntimeError('Redis readback differs from the expected values')
