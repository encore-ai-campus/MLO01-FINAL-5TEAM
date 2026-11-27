"""Verify 19-1 publication and retries against S3Mock."""
import argparse
import json
from io import BytesIO
from pathlib import Path

import boto3
from botocore.awsrequest import AWSResponse
from botocore.config import Config
from botocore.exceptions import ClientError
from urllib3.response import HTTPResponse

from publish_fallback_popularity import digest, json_bytes, publish, read_object, verify_list


def verify(s3, bucket, args):
    def staged(day):
        folder = args.input / day
        return (folder / 'popular.json').read_bytes(), (folder / 'popular.json.sha256').read_bytes()

    def published_list(root, entry, payload):
        path = f"{root}/{entry['list_version']}/popular.json"
        assert entry['key'] == path
        assert read_object(s3, bucket, path)[0] == payload[0]
        assert read_object(s3, bucket, path + '.sha256')[0] == payload[1] == entry['sha256'].encode('ascii')
        assert entry['sha256'] == digest(payload[0])

    base, first, second, correction = map(staged, ('2019-11-01', '2019-11-02', '2019-11-03', 'revision-02'))
    old, new, corrected = map(lambda payload: verify_list(payload[0]), (first, second, correction))
    for payload in (base, first, second):
        assert verify_list(payload[0])['publish_request_id'] == 'daily'
    for invalid_id in (None, '', '  ', 123):
        try:
            verify_list(json_bytes({**new, 'publish_request_id': invalid_id}))
        except ValueError as err:
            assert str(err) == 'Invalid publication request ID'
        else:
            raise AssertionError('Invalid publication request ID was accepted')
    assert publish(s3, bucket, *base) == 'published'
    base_manifest = json.loads(read_object(s3, bucket, 'fallback/base/manifest.json')[0])
    published_list('fallback/base', base_manifest['versions'][0], base)
    assert publish(s3, bucket, *first) == 'published'
    root = f"fallback/{old['replay_run_id']}"
    manifest_key = f'{root}/manifest.json'
    previous, previous_etag = read_object(s3, bucket, manifest_key)
    old_entry, = json.loads(previous)['versions']
    published_list(root, old_entry, first)
    injected = []
    target = ''

    def fail_put(request, **_):
        if request.url.split('?', 1)[0].endswith(target):
            injected.append(request.url)
            return AWSResponse(request.url, 503, {'Content-Type': 'application/xml'},
                               HTTPResponse(body=BytesIO(b'<Error><Code>ServiceUnavailable</Code></Error>'),
                                            preload_content=False))

    s3.meta.events.register('before-send.s3.PutObject', fail_put, unique_id='publication-failure')
    try:
        new_key = f"{root}/{new['list_version']}/popular.json"
        for target in (f'/{new_key}.sha256', f'/{manifest_key}'):
            try:
                publish(s3, bucket, *second)
            except ClientError as err:
                assert err.response['ResponseMetadata']['HTTPStatusCode'] == 503
            else:
                raise AssertionError('Injected publication failure was ignored')
            assert read_object(s3, bucket, manifest_key) == (previous, previous_etag)
            published_list(root, old_entry, first)
            assert read_object(s3, bucket, new_key)[0] == second[0]
            assert read_object(s3, bucket, new_key + '.sha256')[0] == (
                None if target.endswith('.sha256') else second[1])
    finally:
        s3.meta.events.unregister('before-send.s3.PutObject', unique_id='publication-failure')

    assert len(injected) == 2
    staged_objects = [read_object(s3, bucket, key) for key in (new_key, new_key + '.sha256')]
    other_request = json_bytes({**new, 'publish_request_id': 'another-request'})
    try:
        publish(s3, bucket, other_request, digest(other_request).encode('ascii'))
    except ValueError as err:
        assert str(err) == f'Staged content differs from immutable object {new_key}'
    else:
        raise AssertionError('An orphan from another request was reused')
    assert read_object(s3, bucket, manifest_key) == (previous, previous_etag)
    assert publish(s3, bucket, *second) == 'published'
    assert [read_object(s3, bucket, key) for key in (new_key, new_key + '.sha256')] == staged_objects
    current, _ = read_object(s3, bucket, manifest_key)
    assert publish(s3, bucket, *second) == 'already_published'
    assert read_object(s3, bucket, manifest_key)[0] == current
    assert publish(s3, bucket, *correction) == 'published'
    final, _ = read_object(s3, bucket, manifest_key)
    entries = json.loads(final)['versions']
    assert [entry['list_version'] for entry in entries] == [
        old['list_version'], corrected['list_version'], new['list_version']]
    for entry, payload in zip(entries, (first, correction, second), strict=True):
        published_list(root, entry, payload)
    assert publish(s3, bucket, *first) == 'already_published'
    assert read_object(s3, bucket, manifest_key)[0] == final
    (args.input / 'manifest.json').write_bytes(final)
    return {'old_version': old['list_version'], 'attempted_version': new['list_version'],
            'published_version': entries[-1]['list_version'], 'old_list_preserved_on_failure': True,
            'retried_identical_bytes': True, 'same_version': True, 'idempotent_retry': True,
            'publish_request_id': new['publish_request_id'], 'cross_request_reuse_rejected': True,
            'explicit_revision_preserves_previous': True, 'base_manifest_isolated': True,
            'injected_s3_status': 503, 'failed_steps': ['checksum', 'manifest'],
            'object_sha256': entries[-1]['sha256'], 'bucket': bucket, 'manifest_key': manifest_key}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint-url', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--input', type=Path, required=True)
    args = parser.parse_args()
    client = boto3.client('s3', endpoint_url=args.endpoint_url, region_name='us-east-1',
                          aws_access_key_id='local-test', aws_secret_access_key='local-test',
                          config=Config(retries={'total_max_attempts': 1}, s3={'addressing_style': 'path'}))
    result = verify(client, args.bucket, args)
    (args.input / 'publication-verification.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
