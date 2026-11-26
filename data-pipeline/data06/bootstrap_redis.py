"""Seed the streaming Redis schema from events strictly before replay starts."""
import argparse
import re
from pathlib import Path
from types import SimpleNamespace

import redis
from pyspark.sql import functions as F

from redis_contract import batch_values, parse_time, replace_values, run_prefix, user_key, utc_string
from refined import next_user_state
from stream import spark_session


def seed_users(spark, root):
    path = root / 'bootstrap_history'
    if not path.exists():
        return {}
    events = spark.read.format('delta').load(str(path))
    users = {}
    for row in events.orderBy('user_id', 'event_time', 'source', 'event_seq').toLocalIterator():
        event = SimpleNamespace(event_id=row.event_id, event_time=utc_string(row.event_time),
                                event_type=row.event_type, product_id=row.product_id, source=row.source,
                                event_seq=row.event_seq, origin_partition=None, origin_offset=None,
                                refined_offset=None)
        users[row.user_id] = next_user_state(users.get(row.user_id, {}), event)
    return users


def bootstrap(spark, args, root):
    path = root / 'bootstrap_history'
    if any((root / name / 'commits').exists() for name in (
            'redis-checkpoint', 'decision-checkpoint', 'decision-spark-checkpoint')):
        raise ValueError('Bootstrap must run before the replay workers')
    source = spark.read.parquet(*args.bootstrap_input)
    replay_time = utc_string(args.start)
    seed = (source.where(F.col('event_time') < F.to_timestamp(F.lit(replay_time)))
            .select('event_id', 'event_time', 'source', 'event_seq', 'user_id', 'product_id', 'event_type'))
    # Keep the immutable seed alongside the run so Redis recovery can apply the same prefix.
    with redis.Redis.from_url(args.redis_url, decode_responses=True, socket_timeout=10) as client:
        if next(client.scan_iter(match=run_prefix(args.run_id) + '*'), None) is not None:
            raise ValueError('Bootstrap requires an empty Redis namespace')
        seed.write.format('delta').mode('errorifexists').save(str(path))
        users = seed_users(spark, root)
        values = {user_key(args.run_id, user): state for user, state in users.items()}
        if args.batch_history:
            values.update(batch_values(spark, args.run_id, args.batch_history, replay_time))
        replace_values(client, args.run_id, values, require_empty=True)
    print(f'BOOTSTRAP_REDIS users={len(users)} batch_keys={len(values) - len(users)}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap-input', nargs='+', required=True)
    parser.add_argument('--start', type=parse_time, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--redis-url', default='redis://redis:6379/0')
    parser.add_argument('--batch-history', type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9-]+', args.run_id) or args.start.utcoffset() is None:
        parser.error('Provide a lowercase run ID and a timezone-aware replay start')
    spark = spark_session('data-redis-bootstrap')
    try:
        bootstrap(spark, args, Path('/state') / 'split' / args.run_id)
    finally:
        spark.stop()
