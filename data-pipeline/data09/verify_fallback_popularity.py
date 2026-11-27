"""Verify the first eight fallback lists against an independent ranking of the input."""
import argparse
import hashlib
import json
from collections import defaultdict
from datetime import timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from fallback_popularity import REPLAY_START, build, stage, utc
from stream import spark_session


def reference(rows, cutoff):
    start = cutoff - timedelta(days=7)
    overall, categories = defaultdict(set), defaultdict(lambda: defaultdict(set))
    counts = {'october': 0, 'november': 0}
    for row in rows:
        event_time = row.event_time.replace(tzinfo=timezone.utc)
        if not start <= event_time < cutoff or row.user_id is None or row.product_id is None:
            continue
        key = (row.category_code or '').strip() or (str(row.category_id) if row.category_id is not None else 'unknown')
        overall[row.product_id].add(row.user_id)
        categories[key][row.product_id].add(row.user_id)
        counts['october' if event_time < REPLAY_START else 'november'] += 1

    def top(buyers, n):
        ordered = sorted(buyers, key=lambda item: (-len(buyers[item]), item))[:n]
        return [{'product_id': str(item), 'rank': position, 'buyer_count': len(buyers[item])}
                for position, item in enumerate(ordered, 1)]

    return top(overall, 200), {key: top(items, 50) for key, items in categories.items()}, counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--october', required=True)
    parser.add_argument('--november', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--previous', type=Path, help='Optional directory with the original-format lists')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    delta = args.output / 'refined-fixture'
    spark = spark_session('verify-fallback-popularity')
    try:
        columns = ('event_time', 'event_type', 'product_id', 'user_id', 'category_code', 'category_id')
        beginning = REPLAY_START - timedelta(days=7)
        ending = REPLAY_START + timedelta(days=7)
        october = (spark.read.parquet(args.october).where((F.col('event_time') >= F.lit(beginning)) &
                    (F.col('event_time') < F.lit(REPLAY_START)) & (F.col('event_type') == 'purchase'))
                    .select(*columns))
        november = (spark.read.parquet(args.november).where((F.col('event_time') >= F.lit(REPLAY_START)) &
                    (F.col('event_time') < F.lit(ending)) & (F.col('event_type') == 'purchase'))
                    .select(*columns))
        actual = november.withColumn('status', F.lit('accepted')).withColumn('origin_type', F.lit('replay'))
        sentinels = spark.createDataFrame([
            (REPLAY_START, 'purchase', 900000001, 1, None, None, 'duplicate', 'replay'),
            (REPLAY_START, 'purchase', 900000002, 1, None, None, 'late', 'replay'),
            (REPLAY_START, 'purchase', 900000003, 1, None, None, 'accepted', 'human_demo'),
            (REPLAY_START, 'purchase', 900000004, 1, None, None, 'accepted', 'load_test'),
        ], actual.schema)
        actual.unionByName(sentinels).write.format('delta').save(str(delta))
        late_arrival = spark.createDataFrame(
            [(REPLAY_START, 'purchase', 900000005, 1, 'pin.probe', None, 'accepted', 'replay')], actual.schema)
        late_arrival.write.format('delta').mode('append').save(str(delta))

        inputs = october.unionByName(november).collect()
        rows = []
        for day in range(8):
            cutoff = REPLAY_START + timedelta(days=day)
            pinned = {} if day == 0 else {'refined': delta, 'delta_version': 0,
                                         'replay_run_id': 'run-fallback-local'}
            document = build(spark, cutoff=cutoff, processed_through=cutoff, october=args.october, **pinned)
            overall, categories, counts = reference(inputs, cutoff)
            assert document['overall'] == overall, utc(cutoff)
            assert document['by_category'] == categories, utc(cutoff)
            day_path = args.output / f'{cutoff:%Y-%m-%d}'
            day_path.mkdir()
            path = day_path / 'popular.json'
            stage(document, path)
            if args.previous is not None:
                previous = json.loads((args.previous / f'{cutoff:%Y-%m-%d}.json').read_bytes())
                assert previous['overall'] == [row['product_id'] for row in overall], utc(cutoff)
                assert previous['categories'] == {
                    key: [row['product_id'] for row in ranks] for key, ranks in categories.items()
                }, utc(cutoff)
            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
            assert path.with_suffix('.json.sha256').read_bytes() == checksum.encode('ascii')
            rows.append({'cutoff': utc(cutoff), 'window_start': utc(cutoff - timedelta(days=7)),
                         'delta_version': pinned.get('delta_version'), 'purchase_rows': counts,
                         'top': overall[:5], 'overall_count': len(overall), 'category_count': len(categories),
                         'max_category_size': max(map(len, categories.values()), default=0),
                         'version': document['list_version'], 'sha256': checksum})
            print(json.dumps(rows[-1]), flush=True)
        first_day = REPLAY_START + timedelta(days=1)
        latest = build(spark, cutoff=first_day, processed_through=first_day, october=args.october,
                       refined=delta, delta_version=1, replay_run_id='run-fallback-local', revision=2)
        fixed = json.loads((args.output / '2019-11-02' / 'popular.json').read_bytes())
        assert 'pin.probe' not in fixed['by_category']
        assert latest['by_category']['pin.probe'] == [{'product_id': '900000005', 'rank': 1, 'buyer_count': 1}]
        (args.output / 'revision-02').mkdir()
        stage(latest, args.output / 'revision-02' / 'popular.json')
        assert rows[0]['purchase_rows']['november'] == rows[-1]['purchase_rows']['october'] == 0
        try:
            build(spark, cutoff=first_day, processed_through=first_day - timedelta(seconds=1),
                  october=args.october, refined=delta, delta_version=0)
        except ValueError as exc:
            assert 'has not reached' in str(exc)
        else:
            raise AssertionError('An undrained cutoff was accepted')
        (args.output / 'ranking-verification.json').write_text(json.dumps({
            'cutoffs': rows, 'rank_mismatches': 0,
            'prior_format_rank_mismatches': 0 if args.previous is not None else None,
            'pinned_version_verified': True,
            'unprocessed_cutoff_rejected': True, 'excluded_test_rows': 4,
        }, indent=2), encoding='utf-8')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
