"""Local relational serving store; PostgreSQL schema is provided separately."""
import hashlib
import json
import sqlite3
from pathlib import Path


def checksum(file):
    h = hashlib.sha256()
    with Path(file).open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def code_version():
    h = hashlib.sha256()
    root = Path(__file__).parent
    for file in sorted(root.glob('*.py')):
        h.update(file.name.encode())
        h.update(file.read_bytes())
    h.update((root.parent/'requirements.txt').read_bytes())
    return h.hexdigest()


SCHEMA = '''
CREATE TABLE IF NOT EXISTS relation_versions (
 retriever_version TEXT PRIMARY KEY, state TEXT NOT NULL,
 source_sha256 TEXT NOT NULL, row_count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS item_neighbors (
 retriever_version TEXT NOT NULL, source_item_id INTEGER NOT NULL,
 neighbor_item_id INTEGER NOT NULL, similarity REAL NOT NULL,
 neighbor_rank INTEGER NOT NULL CHECK(neighbor_rank BETWEEN 1 AND 100),
 PRIMARY KEY(retriever_version,source_item_id,neighbor_item_id),
 UNIQUE(retriever_version,source_item_id,neighbor_rank),
 CHECK(source_item_id != neighbor_item_id));
CREATE INDEX IF NOT EXISTS neighbors_lookup
 ON item_neighbors(retriever_version,source_item_id,neighbor_rank);
CREATE TABLE IF NOT EXISTS active_bundle (
 slot INTEGER PRIMARY KEY CHECK(slot=1), bundle_version TEXT NOT NULL,
 bundle_path TEXT NOT NULL, retriever_version TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS activation_history (
 id INTEGER PRIMARY KEY AUTOINCREMENT, changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 bundle_version TEXT NOT NULL, bundle_path TEXT NOT NULL, retriever_version TEXT NOT NULL);
'''


def install(bundle, database):
    bundle = Path(bundle).resolve()
    manifest = json.loads((bundle/'manifest.json').read_text('utf-8'))
    for name, expected in manifest['checksums'].items():
        if checksum(bundle/name) != expected:
            raise ValueError('Artifact checksum mismatch: '+name)
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(database)
    conn.executescript(SCHEMA)
    try:
        with conn:
            snapshots = manifest.get('retriever_snapshots') or [
                {'file': 'neighbors.json', 'version': manifest['retriever_version']}]
            for snapshot in snapshots:
                relation_file = bundle/snapshot['file']
                if snapshot['file'] not in manifest['checksums']:
                    raise ValueError('Relation snapshot is missing its manifest checksum')
                neighbors = json.loads(relation_file.read_text('utf-8'))
                version = snapshot['version']
                rows = [(version, int(p), int(n), float(s), i+1)
                        for p, values in neighbors.items() for i, (n, s) in enumerate(values)]
                existing = conn.execute('SELECT state,source_sha256,row_count FROM relation_versions WHERE retriever_version=?', (version,)).fetchone()
                digest = checksum(relation_file)
                if existing:
                    if existing != ('ready', digest, len(rows)):
                        raise ValueError('Existing version is inconsistent; it was preserved.')
                    actual = conn.execute('SELECT COUNT(*) FROM item_neighbors WHERE retriever_version=?', (version,)).fetchone()[0]
                    if actual != len(rows):
                        raise ValueError('Existing relation row count mismatch')
                    continue
                conn.execute('INSERT INTO relation_versions VALUES (?,?,?,?)', (version, 'loading', digest, len(rows)))
                conn.executemany('INSERT INTO item_neighbors VALUES (?,?,?,?,?)', rows)
                # Verify the SQL representation without float truncation.
                for p, values in neighbors.items():
                    actual = conn.execute('SELECT neighbor_item_id,similarity FROM item_neighbors WHERE retriever_version=? AND source_item_id=? ORDER BY neighbor_rank', (version, int(p))).fetchall()
                    if actual != [(int(n), float(s)) for n, s in values]:
                        raise ValueError('Relation validation failed')
                conn.execute("UPDATE relation_versions SET state='ready' WHERE retriever_version=?", (version,))
    finally:
        conn.close()
    return manifest


class NeighborStore:
    def __init__(self, database, version):
        self.conn = sqlite3.connect(f'file:{Path(database).resolve().as_posix()}?mode=ro', uri=True)
        state = self.conn.execute('SELECT state FROM relation_versions WHERE retriever_version=?', (version,)).fetchone()
        if state != ('ready',):
            self.conn.close()
            raise ValueError('Relation version is not ready')
        self.version = version

    def get_many(self, items):
        if not items:
            return {}
        sql = 'SELECT source_item_id,neighbor_item_id,similarity FROM item_neighbors WHERE retriever_version=? AND source_item_id IN ('+','.join('?' for _ in items)+') ORDER BY source_item_id,neighbor_rank'
        result = {p: [] for p in items}
        for p, n, s in self.conn.execute(sql, (self.version, *items)):
            result[p].append((n, s))
        return result

    def close(self):
        self.conn.close()


def activate(bundle, database):
    manifest = install(bundle, database)
    if not manifest['promotion']['accepted']:
        raise ValueError('Evaluation rejected this bundle. Trial inference is available; activation is blocked.')
    # Loading the ranker must succeed before the active pointer can change.
    from lightgbm import Booster
    Booster(model_file=str(Path(bundle)/'ranker.txt'))
    conn = sqlite3.connect(database)
    with conn:
        values = (manifest['bundle_version'], str(Path(bundle).resolve()), manifest['retriever_version'])
        conn.execute('INSERT INTO active_bundle VALUES(1,?,?,?) ON CONFLICT(slot) DO UPDATE SET bundle_version=excluded.bundle_version,bundle_path=excluded.bundle_path,retriever_version=excluded.retriever_version', values)
        conn.execute('INSERT INTO activation_history(bundle_version,bundle_path,retriever_version) VALUES(?,?,?)', values)
    conn.close()
    return manifest


def rollback(database):
    conn = sqlite3.connect(database)
    try:
        conn.execute('BEGIN IMMEDIATE')
        current = conn.execute('SELECT bundle_version FROM active_bundle WHERE slot=1').fetchone()
        rows = conn.execute('SELECT bundle_version,bundle_path,retriever_version FROM activation_history ORDER BY id DESC').fetchall()
        prior = next((r for r in rows if current and r[0] != current[0]), None)
        if prior is None:
            raise ValueError('No previous active version; current state is unchanged')
        bundle = Path(prior[1])
        manifest = json.loads((bundle/'manifest.json').read_text('utf-8'))
        for name, expected in manifest['checksums'].items():
            if checksum(bundle/name) != expected:
                raise ValueError('Rollback artifact checksum mismatch')
        if conn.execute('SELECT state FROM relation_versions WHERE retriever_version=?', (prior[2],)).fetchone() != ('ready',):
            raise ValueError('Previous relation version is not ready')
        from lightgbm import Booster
        Booster(model_file=str(bundle/'ranker.txt'))
        conn.execute('UPDATE active_bundle SET bundle_version=?,bundle_path=?,retriever_version=? WHERE slot=1', prior)
        conn.execute('INSERT INTO activation_history(bundle_version,bundle_path,retriever_version) VALUES(?,?,?)', prior)
        conn.commit()
        return prior[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
