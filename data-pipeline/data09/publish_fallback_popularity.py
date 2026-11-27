"""Publish a 19-1 fallback list, checksum and replay-scoped S3 manifest."""
import argparse
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError


def json_bytes(document):
    return json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def digest(body):
    return hashlib.sha256(body).hexdigest()


def verify_list(body):
    document = json.loads(body)
    if set(document) != {'format_version', 'list_version', 'replay_run_id', 'publish_request_id',
                         'data_cutoff_utc', 'generated_at_utc', 'window_days', 'overall', 'by_category'}:
        raise ValueError('Invalid fallback list fields')
    if document['format_version'] != 1 or document['window_days'] != 7:
        raise ValueError('Unknown format version or window size')
    request_id = document['publish_request_id']
    if not isinstance(request_id, str) or not request_id.strip():
        raise ValueError('Invalid publication request ID')
    run = document['replay_run_id']
    if re.fullmatch(r'[A-Za-z0-9_-]+', run) is None:
        raise ValueError('Invalid replay run ID')
    cutoff = datetime.strptime(document['data_cutoff_utc'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    datetime.strptime(document['generated_at_utc'], '%Y-%m-%dT%H:%M:%SZ')
    if cutoff.hour or cutoff.minute or cutoff.second:
        raise ValueError('Fallback list cutoff must be UTC midnight')
    if re.fullmatch(fr'pop-{cutoff:%Y%m%dT%H%M%SZ}-r[1-9][0-9]*', document['list_version']) is None:
        raise ValueError('List version does not match its cutoff')
    if (run == 'base') != (document['data_cutoff_utc'] == '2019-11-01T00:00:00Z'):
        raise ValueError('The initial list must use the base run')
    if not document['overall'] or len(document['overall']) > 200:
        raise ValueError('Invalid overall ranking length')
    categories = document['by_category']
    if not isinstance(categories, dict) or any(not key or not 1 <= len(ranks) <= 50
                                              for key, ranks in categories.items()):
        raise ValueError('Invalid category ranking')
    for ranks in (document['overall'], *categories.values()):
        products = set()
        for position, row in enumerate(ranks, 1):
            if (set(row) != {'product_id', 'rank', 'buyer_count'} or type(row['rank']) is not int
                    or row['rank'] != position or type(row['buyer_count']) is not int or row['buyer_count'] < 1
                    or re.fullmatch(r'0|[1-9][0-9]*', row['product_id']) is None or row['product_id'] in products):
                raise ValueError('Invalid ranked product')
            products.add(row['product_id'])
    return document


def read_object(s3, bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return None, None
        raise
    with obj['Body'] as stream:
        return stream.read(), obj['ETag']


def put_immutable(s3, bucket, key, body, content_type):
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch='*')
    except ClientError as exc:
        if exc.response['ResponseMetadata']['HTTPStatusCode'] != 412:
            raise
        if read_object(s3, bucket, key)[0] != body:
            raise ValueError(f'Staged content differs from immutable object {key}') from exc


def revision(version):
    return int(version.rsplit('-r', 1)[1])


def publish(s3, bucket, body, checksum):
    if checksum != digest(body).encode('ascii'):
        raise ValueError('Fallback list checksum mismatch')
    document = verify_list(body)
    run = document['replay_run_id']
    version = document['list_version']
    cutoff = document['data_cutoff_utc']
    root = f'fallback/{run}'
    manifest_key = f'{root}/manifest.json'
    list_key = f'{root}/{version}/popular.json'
    manifest_body, etag = read_object(s3, bucket, manifest_key)
    manifest = {'replay_run_id': run, 'versions': []} if manifest_body is None else json.loads(manifest_body)
    if manifest['replay_run_id'] != run:
        raise ValueError('Manifest run ID mismatch')
    entries = manifest['versions']
    previous_revisions = [revision(entry['list_version']) for entry in entries if entry['data_cutoff_utc'] == cutoff]
    number = revision(version)
    if number in previous_revisions or (number == 1 and previous_revisions):
        return 'already_published'
    if number != max(previous_revisions, default=0) + 1:
        raise ValueError('Use the next revision for an explicit correction')
    put_immutable(s3, bucket, list_key, body, 'application/json')
    put_immutable(s3, bucket, list_key + '.sha256', checksum, 'text/plain')
    entries.append({'list_version': version, 'data_cutoff_utc': cutoff, 'key': list_key,
                    'sha256': checksum.decode('ascii')})
    entries.sort(key=lambda entry: (entry['data_cutoff_utc'], revision(entry['list_version'])))
    condition = {'IfMatch': etag} if manifest_body is not None else {'IfNoneMatch': '*'}
    s3.put_object(Bucket=bucket, Key=manifest_key, Body=json_bytes(manifest), ContentType='application/json',
                  **condition)
    return 'published'


def main():
    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--staged', type=Path, required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--endpoint-url')
    args = parser.parse_args()
    client = boto3.client('s3', endpoint_url=args.endpoint_url)
    try:
        checksum = args.staged.with_suffix(args.staged.suffix + '.sha256').read_bytes()
        result = publish(client, args.bucket, args.staged.read_bytes(), checksum)
    except (BotoCoreError, ClientError) as exc:
        logging.warning('Fallback publication failed for %s. Retry with the same staged file: %s', args.staged, exc)
        raise SystemExit(1) from exc
    print(result)


if __name__ == '__main__':
    main()
