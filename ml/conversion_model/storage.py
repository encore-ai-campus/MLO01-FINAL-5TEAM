"""Read, fingerprint and stage project inputs and outputs."""

import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


LOCAL_ARTIFACTS = Path('data/purchase_prediction_local/artifacts')


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + '\n',
                    encoding='utf-8')


def write_json_atomic(path, data):
    """Publish an immutable result, reusing an identical concurrent write."""
    with staging_directory(path.parent) as stage:
        temporary = stage / path.name
        write_json(temporary, data)
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            if temporary.read_bytes() != path.read_bytes():
                raise FileExistsError(f'Different result already exists: {path}') from exc


def update_run_manifest(path, data):
    """Replace progress for a run whose directory is owned by one writer."""
    with staging_directory(path.parent) as stage:
        temporary = stage / path.name
        write_json(temporary, data)
        temporary.replace(path)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def rows_hash(rows):
    checksum = hashlib.sha256()
    for row in rows:
        checksum.update(json.dumps(row, ensure_ascii=False, default=str).encode('utf-8'))
        checksum.update(b'\n')
    return checksum.hexdigest()


@contextmanager
def staging_directory(parent):
    """Create an isolated sibling directory and clean up unpublished contents."""
    parent = Path(parent).resolve()
    parent.mkdir(parents=True, exist_ok=True)
    staging = parent / f'.staging-{uuid4().hex}'
    # mkdir's normal mode inherits Windows ACLs. tempfile uses private mode 0700.
    staging.mkdir(mode=0o777)
    try:
        yield staging
    finally:
        if staging.exists():
            if staging.is_symlink() or staging.resolve().parent != parent:
                raise RuntimeError('Refusing to clean a staging path outside its parent')
            shutil.rmtree(staging)
