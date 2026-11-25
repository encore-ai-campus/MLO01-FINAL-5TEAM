"""Restore isolated Redis state at a fully committed refined Kafka boundary."""
import argparse
import json
import re
from pathlib import Path
from uuid import uuid4

import redis
from pyspark.sql import functions as F

from bootstrap_redis import seed_users
from redis_contract import batch_values, replace_values, user_key
from refined import next_user_state
from stream import spark_session


def delta_committed_offsets(root, topic):
    checkpoint = root / 'delta-checkpoint'
    committed = max(int(path.name) for path in (checkpoint / 'commits').iterdir() if path.name.isdecimal())
    lines = (checkpoint / 'offsets' / str(committed)).read_text().splitlines()
    if len(lines) != 3 or lines[0] != 'v1':
        raise ValueError('Unexpected Delta checkpoint format or source count')
    return json.loads(lines[2]).get(topic, {})


def restore(spark, args, root):
    boundary = json.loads(args.boundary)
    if (not isinstance(boundary, dict) or not boundary or
            any(not key.isdecimal() or type(value) is not int or value < 0 for key, value in boundary.items())):
        raise ValueError('Provide a map of partition IDs to exclusive Kafka end offsets')
    observed = delta_committed_offsets(root, args.refined_topic)
    if set(boundary) != set(observed):
        raise ValueError('RECOVERY_BOUNDARY_PARTITIONS_MISMATCH')
    if any(observed.get(partition, -1) < offset for partition, offset in boundary.items()):
        raise ValueError('DELTA_BEHIND_RECOVERY_BOUNDARY')
    cutoffs = spark.createDataFrame([(int(partition), end) for partition, end in boundary.items()],
                                    'refined_partition int, boundary long')
    events = spark.read.format('delta').load(str(root / 'refined'))
    bounded = (events.join(F.broadcast(cutoffs), 'refined_partition')
               .where(F.col('refined_offset') < F.col('boundary')))
    last_batches = bounded.groupBy('refined_partition').agg(F.max('decision_batch').alias('decision_batch'))
    tail = bounded.join(last_batches, ['refined_partition', 'decision_batch'])
    restart_offsets = {**boundary, **{str(row.refined_partition): row.restart
                       for row in tail.groupBy('refined_partition').agg(F.min('refined_offset').alias('restart')).collect()}}
    accepted = bounded.where("status = 'accepted'").orderBy('refined_partition', 'refined_offset')
    users = seed_users(spark, root)
    for row in accepted.toLocalIterator():
        previous = users.get(row.user_id, {})
        if previous and previous['origin_partition'] not in (None, row.origin_partition):
            raise ValueError('User changed partitions across the recovery boundary')
        users[row.user_id] = next_user_state(previous, row)

    values = {user_key(args.run_id, user): state for user, state in users.items()}
    if args.batch_history:
        values.update(batch_values(spark, args.run_id, args.batch_history, args.replay_time))

    # Publishers and Redis workers must be paused for this local replacement.
    with redis.Redis.from_url(args.redis_url, decode_responses=True, socket_timeout=10) as client:
        replace_values(client, args.run_id, values)

    receipt = {'run_id': args.run_id, 'refined_topic': args.refined_topic, 'generation': uuid4().hex,
               'boundary': boundary, 'restart_offsets': restart_offsets, 'delta_committed_offsets': observed,
               'restored_user_keys': len(users), 'restored_batch_keys': len(values) - len(users)}
    with args.recover_redis.open('x', encoding='utf-8') as output:
        json.dump(receipt, output, indent=2)
    print('REDIS_RECOVERY ' + json.dumps(receipt), flush=True)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--recover-redis', type=Path, required=True)
    cli.add_argument('--run-id', required=True)
    cli.add_argument('--refined-topic', required=True)
    cli.add_argument('--boundary', required=True)
    cli.add_argument('--redis-url', required=True)
    cli.add_argument('--batch-history', type=Path)
    cli.add_argument('--replay-time')
    args = cli.parse_args()
    if not re.fullmatch(r'[a-z0-9-]+', args.run_id) or args.recover_redis.exists():
        cli.error('Use a lowercase run ID and a new recovery receipt file')
    if args.batch_history and not args.replay_time:
        cli.error('Batch history requires the replay time')
    root = Path('/state') / 'split' / args.run_id
    spark = spark_session('data-redis-restore')
    try:
        restore(spark, args, root)
    finally:
        spark.stop()
