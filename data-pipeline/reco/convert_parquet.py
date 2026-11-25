"""Convert one original e-commerce CSV to typed Parquet with stable event IDs."""
import argparse
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq

from data06.event_schema import ACTION_FIELDS

from .data import FIELDS, source_event_id


PARQUET_TYPES = {
    'string': pa.string(),
    'int64': pa.int64(),
    'float64': pa.float64(),
    'utc_timestamp_ms': pa.timestamp('ms', tz='UTC'),
}
SCHEMA = pa.schema([(name, PARQUET_TYPES[kind]) for name, kind in ACTION_FIELDS])
DATA_TYPES = {field: SCHEMA.field(field).type for field in FIELDS}
DATA_TYPES['event_time'] = pa.timestamp('ms')


def event_ids(source_name, sequences):
    return pc.binary_join_element_wise(source_event_id(source_name, ''), pc.cast(sequences, pa.string()), '')


def convert(source, destination, max_rows=None, chunk_size=100_000):
    source, destination = Path(source), Path(destination)
    if max_rows is not None and max_rows < 1:
        raise ValueError('max_rows must be positive')
    if chunk_size < 1:
        raise ValueError('chunk_size must be positive')
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    offset = 0
    with tempfile.TemporaryDirectory(prefix='.parquet-', dir=destination.parent) as temporary:
        staged = Path(temporary) / 'events.parquet'
        read_options = csv.ReadOptions(block_size=8 * 1024 * 1024)
        convert_options = csv.ConvertOptions(include_columns=FIELDS, column_types=DATA_TYPES,
                                             strings_can_be_null=True,
                                             timestamp_parsers=['%Y-%m-%d %H:%M:%S UTC'])
        with pq.ParquetWriter(staged, SCHEMA, compression='snappy') as writer, csv.open_csv(
                str(source), read_options=read_options, convert_options=convert_options) as reader:
            for batch in reader:
                if max_rows is not None:
                    batch = batch.slice(0, max_rows - offset)
                sequences = pa.array(np.arange(offset, offset + len(batch), dtype=np.int64))
                columns = [event_ids(source.name, sequences), pa.repeat(source.name, len(batch)), sequences,
                           pc.cast(batch.column('event_time'), SCHEMA.field('event_time').type)]
                columns.extend(batch.column(name) for name in FIELDS[1:])
                writer.write_table(pa.Table.from_arrays(columns, schema=SCHEMA), row_group_size=chunk_size)
                offset += len(batch)
                if max_rows is not None and offset >= max_rows:
                    break
        # Publish a complete file and keep any existing destination intact.
        os.link(staged, destination)
    return {'source': source.name, 'output': str(destination), 'rows': offset}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-rows', type=int)
    parser.add_argument('--chunk-size', type=int, default=100_000)
    args = parser.parse_args()
    print(json.dumps(convert(args.source, args.output, args.max_rows, args.chunk_size), ensure_ascii=False))
