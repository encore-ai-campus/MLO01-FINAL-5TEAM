"""Select a consistent user hash sample from monthly Data-03 Parquet files."""
import argparse
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .convert_parquet import SCHEMA, event_ids
from .cumulative import atomic_json
from .data import sampled_users
from .store import checksum


BATCH_SIZE = 250_000


def sample_month(source, staged, modulus, seed):
    original = pq.ParquetFile(source)
    if not original.schema_arrow.equals(SCHEMA):
        raise ValueError(f'Unexpected source schema: {source}')
    source_rows = 0
    selected_rows = 0
    users = set()
    with pq.ParquetWriter(staged, SCHEMA, compression='snappy') as writer:
        for batch in original.iter_batches(batch_size=BATCH_SIZE):
            user_ids = batch.column('user_id')
            if user_ids.null_count:
                raise ValueError(f'Missing user ID: {source}')
            mask = sampled_users(user_ids.to_numpy(), modulus, seed)
            source_rows += len(batch)
            if mask.any():
                selected = batch.filter(pa.array(mask))
                writer.write_batch(selected)
                selected_rows += len(selected)
                users.update(np.unique(selected.column('user_id').to_numpy()).tolist())
    if source_rows != original.metadata.num_rows:
        raise ValueError(f'Source scan count differs from Parquet metadata: {source}')

    verified_rows = 0
    verified_users = set()
    previous = -1
    source_name = f'{source.stem}.csv.gz'
    sampled = pq.ParquetFile(staged)
    for batch in sampled.iter_batches(batch_size=BATCH_SIZE,
                                      columns=['event_id', 'source', 'event_seq', 'user_id']):
        ids, names, sequences, user_ids = batch.columns
        if any(column.null_count for column in batch.columns):
            raise ValueError(f'Missing required sample values: {source}')
        values = sequences.to_numpy()
        if (values[0] <= previous or values[-1] >= source_rows or np.any(values[1:] <= values[:-1])
                or not pc.all(pc.equal(names, source_name)).as_py()
                or not ids.equals(event_ids(source_name, sequences))):
            raise ValueError(f'Unexpected event ID, order or duplicate: {source}')
        ids_by_user = user_ids.to_numpy()
        if not sampled_users(ids_by_user, modulus, seed).all():
            raise ValueError(f'Sampled user is outside the hash selection: {source}')
        verified_users.update(np.unique(ids_by_user).tolist())
        verified_rows += len(batch)
        previous = values[-1]
    if selected_rows != sampled.metadata.num_rows or verified_rows != selected_rows or verified_users != users:
        raise ValueError(f'Sample verification failed: {source}')
    return {'source': source.name, 'source_rows': source_rows, 'sample_rows': verified_rows,
            'sample_users': len(users), 'file': staged.name, 'sha256': checksum(staged),
            'duplicate_event_ids': 0}, users


def sample_files(sources, output, modulus=50, seed=20261116):
    if modulus < 1 or seed < 0:
        raise ValueError('Use a positive modulus and a nonnegative seed')
    sources = list(map(Path, sources))
    output = Path(output)
    if not sources or len({p.stem for p in sources}) != len(sources):
        raise ValueError('Provide distinct monthly source files')
    output.mkdir(parents=True, exist_ok=True)
    files = [output / f'{p.stem}-sample.parquet' for p in sources]
    user_file, manifest = output / 'sample-users.parquet', output / 'manifest.json'
    for path in [*files, user_file, manifest]:
        if path.exists():
            raise FileExistsError(path)
    with tempfile.TemporaryDirectory(prefix='.sample-', dir=output) as temporary:
        directory = Path(temporary)
        staged_files = [directory / f.name for f in files]
        with ThreadPoolExecutor(max_workers=min(3, len(sources))) as threads:
            tasks = [threads.submit(sample_month, source, staged, modulus, seed)
                     for source, staged in zip(sources, staged_files)]
            months = []
            users = set()
            for task in tasks:
                stats, month_users = task.result()
                print(json.dumps(stats, ensure_ascii=False), flush=True)
                months.append(stats)
                users.update(month_users)
        staged_users = directory / user_file.name
        pq.write_table(pa.table({'user_id': pa.array(sorted(users), type=pa.int64())}),
                       staged_users, compression='snappy')
        report = {'complete': True, 'modulus': modulus, 'seed': seed,
                  'selection': 'reco.data.sampled_users; retain every event of selected users',
                  'schema': {field.name: str(field.type) for field in SCHEMA},
                  'months': months, 'sample_rows': sum(m['sample_rows'] for m in months),
                  'sample_users': len(users),
                  'user_file': {'file': user_file.name, 'sha256': checksum(staged_users)}}
        staged_manifest = directory / manifest.name
        atomic_json(staged_manifest, report)
        linked = []
        try:
            for staged, path in zip([*staged_files, staged_users, staged_manifest],
                                    [*files, user_file, manifest]):
                os.link(staged, path)
                linked.append(path)
        except OSError:
            for path in reversed(linked):
                path.unlink()
            raise
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sources', nargs='+', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--modulus', default=50, type=int)
    parser.add_argument('--seed', default=20261116, type=int)
    args = parser.parse_args()
    report = sample_files(args.sources, args.output, args.modulus, args.seed)
    print(f"Completed: {report['sample_rows']:,} events, {report['sample_users']:,} unique users", flush=True)
