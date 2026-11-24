"""Extract point-in-time catalog changes from the complete Data-03 Parquet files."""
import argparse
import json
from pathlib import Path
from time import monotonic

import duckdb


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / 'data' / 'data-03-full'
OUTPUT = ROOT / 'data' / 'data-08-catalog'


def literal(path):
    return "'" + Path(path).resolve().as_posix().replace("'", "''") + "'"


def build(input_folder, output_folder):
    sources = [input_folder / f'2019-{month}.parquet' for month in ('Oct', 'Nov', 'Dec')]
    output_folder.mkdir(parents=True, exist_ok=True)
    output = output_folder / 'product-history.csv'
    if output.exists():
        raise FileExistsError(output)
    staged = output.with_suffix('.partial.csv')
    if staged.exists():
        raise FileExistsError(staged)
    start = monotonic()
    with duckdb.connect() as db:
        db.execute("SET timezone='UTC'; SET memory_limit='3GB'; SET threads=4; SET preserve_insertion_order=false")
        db.execute(f"SET temp_directory={literal(output_folder / 'duckdb-spill')}")
        paths = '[' + ', '.join(map(literal, sources)) + ']'
        print('Scanning October–December and retaining changes by product and event order', flush=True)
        db.execute(f"""
            COPY (
              WITH observations AS (
                SELECT product_id, event_time AS observed_at, source, event_seq, event_id,
                       struct_pack(category_id := category_id, category_code := category_code,
                                   brand := brand, price := price) AS metadata
                FROM read_parquet({paths})
                WHERE product_id IS NOT NULL AND event_time IS NOT NULL
              ), compared AS (
                SELECT *, lag(metadata) OVER (
                  PARTITION BY product_id ORDER BY observed_at, source, event_seq
                ) AS previous_metadata
                FROM observations
              )
              SELECT product_id, observed_at, source, event_seq, event_id,
                     metadata.category_id AS category_id, metadata.category_code AS category_code,
                     metadata.brand AS brand, metadata.price AS price
              FROM compared WHERE metadata IS DISTINCT FROM previous_metadata
              ORDER BY product_id, observed_at, source, event_seq
            ) TO {literal(staged)} (FORMAT CSV, HEADER TRUE, NULL '\\N')
        """)
    staged.replace(output)
    print(json.dumps({'file': str(output), 'bytes': output.stat().st_size,
                      'seconds': round(monotonic() - start, 2)}), flush=True)


if __name__ == '__main__':
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--input', type=Path, default=INPUT)
    cli.add_argument('--output', type=Path, default=OUTPUT)
    args = cli.parse_args()
    build(args.input, args.output)
