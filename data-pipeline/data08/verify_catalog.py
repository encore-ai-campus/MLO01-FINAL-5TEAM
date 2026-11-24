"""Compare PostgreSQL as-of lookups with independently selected raw observations."""
import json
from datetime import datetime, timedelta
from pathlib import Path
import subprocess

import duckdb

from build_catalog import INPUT, OUTPUT, literal


def postgres(sql):
    process = subprocess.run(
        ['docker', 'exec', 'mlo-data08-postgres-1', 'psql', '-U', 'postgres',
         '-d', 'catalog', '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-c', sql],
        check=True, capture_output=True, text=True, encoding='utf-8',
    )
    return [json.loads(line) for line in process.stdout.splitlines()]


def db_string(value):
    return "'" + value.replace("'", "''") + "'"


def verify():
    price_change = postgres("""
        WITH transitions AS (
            SELECT *, lag(observed_at) OVER w AS previous_at, lag(price) OVER w AS previous_price
            FROM catalog.product_history
            WINDOW w AS (PARTITION BY product_id ORDER BY observed_at, source, event_seq)
        )
        SELECT to_jsonb(t)
        FROM transitions t
        WHERE observed_at > previous_at AND price IS NOT NULL AND previous_price IS NOT NULL
          AND price IS DISTINCT FROM previous_price
        ORDER BY product_id, observed_at, source, event_seq LIMIT 1
    """)[0]
    same_second = postgres("""
        WITH tied AS (
            SELECT product_id, observed_at FROM catalog.product_history
            GROUP BY product_id, observed_at HAVING count(*) > 1
            ORDER BY product_id, observed_at LIMIT 1
        )
        SELECT to_jsonb(h) FROM catalog.product_history h JOIN tied USING (product_id, observed_at)
        ORDER BY h.source, h.event_seq
    """)
    first = postgres(f"""SELECT to_jsonb(h) FROM catalog.product_history h
        WHERE product_id={int(price_change['product_id'])} ORDER BY observed_at, source, event_seq LIMIT 1""")[0]
    month_product = postgres("""
        SELECT jsonb_build_object('product_id', product_id)
        FROM catalog.product_history
        GROUP BY product_id
        HAVING min(observed_at) < '2019-11-01T00:00:00Z' AND max(observed_at) >= '2019-12-01T00:00:00Z'
        ORDER BY product_id LIMIT 1
    """)[0]['product_id']
    cases = []

    def add(label, product, moment, source=None, seq=None):
        cases.append((label, product, moment, source, seq))

    changed_at = datetime.fromisoformat(price_change['observed_at'])
    first_at = datetime.fromisoformat(first['observed_at'])
    add('before_first_observation', first['product_id'], first_at - timedelta(microseconds=1))
    add('at_first_observation', first['product_id'], first_at, first['source'], first['event_seq'])
    add('before_price_change', price_change['product_id'], changed_at - timedelta(microseconds=1))
    add('at_price_change', price_change['product_id'], changed_at, price_change['source'], price_change['event_seq'])
    for label, change in [('same_second_earlier_event', same_second[0]), ('same_second_later_event', same_second[-1])]:
        add(label, change['product_id'], datetime.fromisoformat(change['observed_at']),
            change['source'], change['event_seq'])
    add('same_second_time_only', same_second[0]['product_id'],
        datetime.fromisoformat(same_second[0]['observed_at']))
    for date in ('2019-11-01', '2019-12-01'):
        boundary = datetime.fromisoformat(date + 'T00:00:00+00:00')
        add(date + '_before_boundary', month_product, boundary - timedelta(microseconds=1))
        add(date + '_at_boundary', month_product, boundary)
    add('unknown_product', -1, changed_at)

    sources = [INPUT / f'2019-{month}.parquet' for month in ('Oct', 'Nov', 'Dec')]
    paths = '[' + ', '.join(map(literal, sources)) + ']'
    with duckdb.connect() as db:
        db.execute("SET timezone='UTC'; SET threads=4; SET memory_limit='3GB'")
        db.execute("""CREATE TEMP TABLE checks (label VARCHAR, product_id BIGINT, requested_at TIMESTAMPTZ,
                      source VARCHAR, seq BIGINT)""")
        db.executemany('INSERT INTO checks VALUES (?, ?, ?, ?, ?)', cases)
        raw_counts = dict(db.execute(
            f'SELECT source, count(*) FROM read_parquet({paths}) GROUP BY source').fetchall())
        excluded = db.execute(f'''SELECT count(*) FROM read_parquet({paths})
                                  WHERE product_id IS NULL OR event_time IS NULL''').fetchone()[0]
        expected = dict(db.execute(f"""
            SELECT c.label, arg_max(
                struct_pack(category_id := e.category_id, category_code := e.category_code,
                            brand := e.brand, price := e.price),
                struct_pack(event_time := e.event_time, source := e.source, event_seq := e.event_seq)
            ) FILTER (WHERE e.event_id IS NOT NULL)
            FROM checks c LEFT JOIN read_parquet({paths}) e ON c.product_id=e.product_id
              AND e.event_time <= c.requested_at
              AND (c.source IS NULL OR e.event_time < c.requested_at
                   OR (e.source, e.event_seq) <= (c.source, c.seq))
            GROUP BY c.label
        """).fetchall())
        csv_rows = db.execute(f"SELECT count(*) FROM read_csv({literal(OUTPUT/'product-history.csv')})").fetchone()[0]

    queries = []
    for label, product, moment, source, seq in cases:
        args = f"{int(product)}, {db_string(moment.isoformat())}::timestamptz"
        fn = 'product_at'
        if source is not None:
            fn = 'product_at_event'
            args += f', {db_string(source)}, {int(seq)}'
        queries.append(f"SELECT jsonb_build_object('label', {db_string(label)}, "
                       f"'row', (SELECT to_jsonb(p) FROM catalog.{fn}({args}) p))")
    actual = {row['label']: row['row'] for row in postgres(' UNION ALL '.join(queries))}
    fields = ('category_id', 'category_code', 'brand', 'price')
    checks = []
    for label, product, moment, source, seq in cases:
        row = actual[label]
        metadata = {field: row[field] for field in fields} if row else None
        if metadata != expected[label]:
            raise AssertionError(f'{label}: PostgreSQL {metadata} differs from raw {expected[label]}')
        if row:
            key = (datetime.fromisoformat(row['observed_at']), row['source'], row['event_seq'])
            if key[0] > moment or (source is not None and key > (moment, source, seq)):
                raise AssertionError(f'{label}: returned future metadata')
        checks.append({'case': label, 'product_id': product, 'requested_at': moment.isoformat(),
                       'requested_source': source, 'requested_event_seq': seq, 'result': row, 'passed': True})
    summary = postgres("""SELECT jsonb_build_object(
        'history_rows',count(*), 'products',count(DISTINCT product_id),
        'duplicate_event_ids',count(*)-count(DISTINCT event_id),
        'first_observed_at',min(observed_at), 'last_observed_at',max(observed_at))
        FROM catalog.product_history""")[0]
    assert summary['history_rows'] == csv_rows and summary['duplicate_event_ids'] == 0
    assert sum(raw_counts.values()) == 177_493_621
    assert actual['before_price_change']['price'] != actual['at_price_change']['price']
    assert actual['same_second_earlier_event']['event_id'] != actual['same_second_later_event']['event_id']
    assert actual['same_second_time_only'] == actual['same_second_later_event']
    report = {'raw_events_by_month': raw_counts, 'raw_events_excluded': excluded,
              'postgresql': summary, 'point_in_time_checks': checks, 'all_checks_passed': True}
    (OUTPUT/'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    verify()
