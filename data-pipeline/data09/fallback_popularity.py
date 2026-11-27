"""Build a daily fallback ranking from an October seed and a pinned Delta snapshot."""
import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import Window, functions as F

import event_schema as fields
from stream import spark_session


REPLAY_START = datetime(2019, 11, 1, tzinfo=timezone.utc)
GLOBAL_LIMIT, CATEGORY_LIMIT = 200, 50


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('An explicit timezone is required')
    return parsed.astimezone(timezone.utc)


def utc(value):
    return value.isoformat(timespec='seconds').replace('+00:00', 'Z')


def json_bytes(document):
    return json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def stage(document, path):
    body = json_bytes(document)
    with path.open('xb') as file:
        file.write(body)
    with path.with_suffix(path.suffix + '.sha256').open('xb') as file:
        file.write(hashlib.sha256(body).hexdigest().encode('ascii'))


def purchases(frame, start, end):
    at = F.try_to_timestamp(F.col(fields.EVENT_TIME))
    category = F.trim(F.col(fields.CATEGORY_CODE))
    return (frame.where((F.col(fields.EVENT_TYPE) == 'purchase') & (at >= F.lit(start)) & (at < F.lit(end))
                        & F.col(fields.USER_ID).isNotNull() & F.col(fields.PRODUCT_ID).isNotNull())
            .select(F.col(fields.USER_ID).alias('buyer'), F.col(fields.PRODUCT_ID).cast('long').alias('product'),
                    F.coalesce(F.when(category != '', category), F.col(fields.CATEGORY_ID).cast('string'),
                               F.lit('unknown')).alias('category')))


def rank(frame):
    memberships = frame.unionByName(frame.withColumn('category', F.lit(None).cast('string')))
    scores = memberships.groupBy('category', 'product').agg(F.countDistinct('buyer').alias('buyers'))
    order = Window.partitionBy('category').orderBy(F.desc('buyers'), F.asc('product'))
    scored = (scores.withColumn('position', F.row_number().over(order))
              .where(F.col('position') <= F.when(F.col('category').isNull(), GLOBAL_LIMIT).otherwise(CATEGORY_LIMIT))
              .orderBy('category', 'position'))
    overall, categories = [], {}
    for row in scored.toLocalIterator():
        target = overall if row.category is None else categories.setdefault(row.category, [])
        target.append({'product_id': str(row.product), 'rank': row.position, 'buyer_count': row.buyers})
    return overall, categories


def build(spark, *, cutoff, processed_through, october, replay_run_id=None, revision=1,
          generated_at=None, refined=None, delta_version=None, legacy_replay_source=None):
    if cutoff < REPLAY_START or cutoff.time() != datetime.min.time():
        raise ValueError('Cutoff must be a UTC day boundary on or after the replay start')
    if processed_through < cutoff:
        raise ValueError('Replay processing has not reached this cutoff')
    if cutoff == REPLAY_START:
        if replay_run_id not in (None, 'base'):
            raise ValueError('The initial list uses replay_run_id=base')
        replay_run_id = 'base'
    elif not replay_run_id or replay_run_id == 'base':
        raise ValueError('A replay run ID is required for daily lists')
    if revision < 1:
        raise ValueError('Revision must be positive')
    start = cutoff - timedelta(days=7)
    frame = None
    if start < REPLAY_START:
        frame = purchases(spark.read.parquet(str(october)), start, min(cutoff, REPLAY_START))
    if cutoff > REPLAY_START:
        if refined is None or delta_version is None or delta_version < 0:
            raise ValueError('A pinned Delta table version is required after replay starts')
        rows = spark.read.format('delta').option('versionAsOf', delta_version).load(str(refined))
        rows = rows.where(F.col('status') == 'accepted')
        if 'origin_type' in rows.columns:
            rows = rows.where(F.col('origin_type') == 'replay')
        elif legacy_replay_source:
            rows = rows.where(F.col(fields.SOURCE_FILE) == legacy_replay_source)
        else:
            raise ValueError('Refined table lacks origin_type. Pass an explicit replay source for a legacy table')
        november = purchases(rows, max(start, REPLAY_START), cutoff)
        frame = november if frame is None else frame.unionByName(november)
    elif delta_version is not None or refined is not None:
        raise ValueError('The initial list is built from October only')
    overall, categories = rank(frame)
    if not overall:
        raise ValueError('No eligible purchases, retaining the previous published list')
    return {
        'format_version': 1, 'data_cutoff_utc': utc(cutoff),
        'list_version': f'pop-{cutoff:%Y%m%dT%H%M%SZ}-r{revision}',
        'replay_run_id': replay_run_id, 'publish_request_id': 'daily',
        'generated_at_utc': utc(generated_at or datetime.now(timezone.utc)),
        'window_days': 7, 'overall': overall, 'by_category': categories,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--october', required=True)
    parser.add_argument('--refined')
    parser.add_argument('--delta-version', type=int)
    parser.add_argument('--legacy-replay-source')
    parser.add_argument('--replay-run-id')
    parser.add_argument('--revision', type=int, default=1)
    parser.add_argument('--cutoff', required=True, type=timestamp)
    parser.add_argument('--processed-through', required=True, type=timestamp)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    spark = spark_session('fallback-popularity')
    try:
        document = build(spark, cutoff=args.cutoff, processed_through=args.processed_through,
                         october=args.october, refined=args.refined, delta_version=args.delta_version,
                         legacy_replay_source=args.legacy_replay_source,
                         replay_run_id=args.replay_run_id, revision=args.revision)
        stage(document, args.output)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
