"""Validate the frozen recommendation input and the shared 2% sample."""

import json
import re
import sqlite3
from contextlib import closing
from hashlib import file_digest, sha256


def checksum(path):
    with path.open('rb') as source:
        return file_digest(source, 'sha256').hexdigest()


def require_closed(database):
    if any(database.with_name(database.name + suffix).exists() for suffix in ('-wal', '-journal')):
        raise ValueError('The event database must be a closed SQLite export')


def team_users(manifest_path):
    payload = manifest_path.read_bytes()
    manifest = json.loads(payload)
    version = manifest['version']
    tiers = [tier for tier in manifest['tiers'] if tier['basis_points'] == 200]
    if not isinstance(version, str) or not version.strip() or len(tiers) != 1:
        raise ValueError('Expected one versioned shared 2% sample')
    tier, = tiers
    lines = (manifest_path.parent / tier['users_file']).read_text(encoding='utf-8-sig').splitlines()
    users = set(lines)
    if not users or len(users) != len(lines) or any(not user or user.strip() != user for user in users):
        raise ValueError('Shared sample contains invalid or duplicate user IDs')
    digest = sha256(('\n'.join(sorted(users)) + '\n').encode('utf-8')).hexdigest()
    if type(tier['users']) is not int or tier['users'] != len(users) or digest != tier['user_list_sha256']:
        raise ValueError('Shared sample user count or SHA-256 differs from the manifest')
    identity = {'version': version, 'basis_points': 200, 'users': len(users), 'user_list_sha256': digest}
    return users, identity, sha256(payload).hexdigest()


def training_receipt(database, sample, sample_manifest=None):
    require_closed(database)
    if sample not in ('local', 'team') or (sample == 'team') != (sample_manifest is not None):
        raise ValueError('A shared sample manifest is required only for team input')
    manifest_path = database.with_suffix('.manifest.json')
    payload = manifest_path.read_bytes()
    manifest = json.loads(payload)
    if manifest.get('complete') is not True:
        raise ValueError('The event database needs a complete export manifest')
    identity = None
    if sample_manifest is not None:
        users, identity, manifest_hash = team_users(sample_manifest)
        binding = manifest.get('common_sample')
        if not isinstance(binding, dict) or any(binding.get(k) != v for k, v in identity.items()):
            raise ValueError('The event database is bound to a different shared user list')
        if binding.get('manifest_sha256') != manifest_hash:
            raise ValueError('The event database is bound to a different shared manifest')
    expected_hash = manifest.get('event_db_sha256')
    if (sample == 'team' or 'event_db_sha256' in manifest) and (
            not isinstance(expected_hash, str) or not re.fullmatch('[0-9a-fA-F]{64}', expected_hash)):
        raise ValueError('The event database manifest needs a valid SHA-256')
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as connection:
        connection.execute('BEGIN')
        count, = connection.execute('SELECT COUNT(*) FROM events').fetchone()
        if not count or type(manifest.get('sample_rows')) is not int or count != manifest['sample_rows']:
            raise ValueError('The event database row count differs from its manifest or is empty')
        db_hash = checksum(database)
        if expected_hash is not None and db_hash != expected_hash.lower():
            raise ValueError('The event database checksum differs from its manifest')
        receipt = {'sample': sample, 'event_count': count, 'event_db_sha256': db_hash,
                   'manifest_sha256': sha256(payload).hexdigest()}
        if identity is not None:
            observed = 0
            for (user,) in connection.execute('SELECT DISTINCT user_id FROM events'):
                if str(user) not in users:
                    raise ValueError('The database contains a user outside the shared 2% list')
                observed += 1
            if manifest.get('sample_users', observed) != observed:
                raise ValueError('The event database user count differs from its manifest')
            receipt.update(team_sample=identity, observed_users=observed, sample_manifest_sha256=manifest_hash)
        require_closed(database)
    return receipt
