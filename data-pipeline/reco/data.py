"""Read compressed monthly CSV files once and retain a deterministic user sample."""
import argparse
import hashlib
import json
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd

from data06.event_schema import SOURCE_FIELDS

FIELDS = list(SOURCE_FIELDS)
ACTIONS = {'view': 0, 'cart': 1, 'purchase': 2}


def source_event_id(source_name, row_number):
    """Stable identifier of a zero-based data row within an original source file."""
    return f'{source_name}:{row_number}'


def sampled_users(values, modulus=200, seed=20261116):
    """Stable integer mixing: a user has the same membership in every month."""
    x = np.asarray(values, dtype=np.uint64) ^ np.uint64(seed)
    x = (x ^ (x >> np.uint64(30))) * np.uint64(0xbf58476d1ce4e5b9)
    x = (x ^ (x >> np.uint64(27))) * np.uint64(0x94d049bb133111eb)
    x ^= x >> np.uint64(31)
    return x % np.uint64(modulus) == 0


def identity(file):
    """A quick identity, explicitly NOT a full-file checksum."""
    file = Path(file)
    stat = file.stat()
    h = hashlib.sha256()
    with file.open('rb') as f:
        h.update(f.read(1024 * 1024))
        f.seek(max(0, stat.st_size - 1024 * 1024))
        h.update(f.read())
    return {'path': str(file.resolve()), 'bytes': stat.st_size,
            'mtime_ns': stat.st_mtime_ns, 'head_tail_sha256': h.hexdigest()}


def sample(files, dest, modulus=200, seed=20261116, chunksize=300000):
    if modulus < 1:
        raise ValueError('modulus must be positive')
    dest = Path(dest)
    manifest = dest.with_suffix('.manifest.json')
    sources = [identity(x) for x in files]
    if dest.exists():
        old = json.loads(manifest.read_text('utf-8')) if manifest.exists() else {}
        if (old.get('sources'), old.get('modulus'), old.get('seed')) == (sources, modulus, seed) and old.get('complete'):
            print('Reusing completed sample: ' + str(dest), flush=True)
            return
        raise ValueError('Different or incomplete sample exists. Select a new destination; data is preserved.')
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix('.partial.sqlite')
    if partial.exists():
        raise ValueError('A partial sample exists. Preserve it and choose another destination.')
    conn = sqlite3.connect(partial)
    conn.execute('''CREATE TABLE events (
        event_id TEXT PRIMARY KEY, ts INTEGER NOT NULL, row_no INTEGER NOT NULL,
        source TEXT NOT NULL, user_id INTEGER NOT NULL, item_id INTEGER NOT NULL,
        action INTEGER NOT NULL, category TEXT NOT NULL, brand TEXT NOT NULL,
        price REAL, raw_session TEXT NOT NULL)''')
    report = {'sources': sources, 'modulus': modulus, 'seed': seed,
              'sampling': 'stable user hash, complete supplied months, not a chronological prefix',
              'complete': False, 'files': []}
    total = 0
    for file in files:
        started = time.monotonic()
        name = Path(file).name
        rows = kept = invalid = 0
        counts = {x: 0 for x in ACTIONS}
        dtype = {x: 'string' for x in FIELDS}
        dtype['user_id'] = 'uint64'
        for chunk in pd.read_csv(file, compression='infer', usecols=FIELDS,
                                 dtype=dtype, chunksize=chunksize):
            raw_count = len(chunk)
            selected = chunk.loc[sampled_users(chunk.user_id.to_numpy(), modulus, seed)].copy()
            valid = selected.event_type.isin(ACTIONS) & selected.product_id.notna()
            invalid += int((~valid).sum())
            selected = selected.loc[valid]
            if len(selected):
                timestamps = pd.to_datetime(selected.event_time, format='%Y-%m-%d %H:%M:%S UTC', utc=True, errors='coerce')
                good = timestamps.notna() & pd.to_numeric(selected.product_id, errors='coerce').notna()
                invalid += int((~good).sum())
                selected = selected.loc[good]
                timestamps = timestamps.loc[good].dt.as_unit('s').astype('int64')
                category = selected.category_code.fillna(selected.category_id).fillna('unknown')
                prices = pd.to_numeric(selected.price, errors='coerce')
                records = []
                for idx, ts, uid, item, action, cat, brand, price, session in zip(
                    selected.index, timestamps, selected.user_id, selected.product_id,
                    selected.event_type, category, selected.brand.fillna('unknown'),
                    prices, selected.user_session.fillna('')):
                    records.append((source_event_id(name, idx), int(ts), int(idx), name, int(uid), int(item),
                                    ACTIONS[action], str(cat), str(brand),
                                    float(price) if pd.notna(price) else None, str(session)))
                    counts[action] += 1
                conn.executemany('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)', records)
                kept += len(records)
            rows += raw_count
            conn.commit()
            if rows % (chunksize * 10) == 0:
                print(f'{name}: scanned {rows:,}, retained {kept:,}, {time.monotonic()-started:.0f}s', flush=True)
        detail = {'source': name, 'raw_rows': rows, 'sample_rows': kept,
                  'invalid_sample_rows': invalid, 'events': counts,
                  'elapsed_seconds': round(time.monotonic()-started, 2)}
        report['files'].append(detail)
        total += kept
        print(json.dumps(detail), flush=True)
    conn.execute('CREATE INDEX events_by_time ON events(ts,source,row_no)')
    conn.execute('CREATE INDEX events_by_user ON events(user_id,ts,source,row_no)')
    report['sample_rows'] = total
    report['sample_users'] = conn.execute('SELECT COUNT(DISTINCT user_id) FROM events').fetchone()[0]
    report['complete'] = True
    conn.commit()
    conn.close()
    partial.rename(dest)
    manifest.write_text(json.dumps(report, ensure_ascii=False, indent=2), 'utf-8')
    print('Completed sample: ' + str(dest.resolve()), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('files', nargs='+')
    p.add_argument('--output', default='data/events.sqlite')
    p.add_argument('--modulus', type=int, default=200)
    p.add_argument('--seed', type=int, default=20261116)
    p.add_argument('--chunksize', type=int, default=300000)
    a = p.parse_args()
    sample(a.files, a.output, a.modulus, a.seed, a.chunksize)
