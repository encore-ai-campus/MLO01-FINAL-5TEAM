"""Validate when the exact artifacts used in a replay became available."""

from datetime import datetime
from hashlib import sha256
import json


def utc_second(value):
    if not isinstance(value, str):
        raise ValueError('Expected a timestamp with a time zone and whole seconds')
    instant = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if instant.utcoffset() is None or instant.microsecond:
        raise ValueError('Expected a timestamp with a time zone and whole seconds')
    return int(instant.timestamp())


def verify_readiness(file, artifacts, as_of):
    if file is None:
        return {'status': 'unverified', 'missing': list(artifacts)}

    payload = file.read_bytes()
    entries = json.loads(payload)
    if not isinstance(entries, dict) or set(entries) != set(artifacts):
        raise ValueError('Expected readiness records for ' + ', '.join(artifacts))
    for name, (digest, cutoff) in artifacts.items():
        entry = entries[name]
        if (not isinstance(entry, dict) or entry.get('sha256') != digest
                or not isinstance(entry.get('record_id'), str) or not entry['record_id'].strip()):
            raise ValueError(f'Invalid readiness identity for {name}')
        if not cutoff <= utc_second(entry.get('ready_at_utc')) < as_of:
            raise ValueError(f'{name} must be ready at or after its cutoff and strictly before the request')
    return {'status': 'matched_supplied_records', 'records': entries,
            'receipt_sha256': sha256(payload).hexdigest()}
