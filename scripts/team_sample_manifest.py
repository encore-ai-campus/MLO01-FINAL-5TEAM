"""Read a versioned user list shared by the purchase and recommendation workflows."""

import json
from dataclasses import dataclass
from hashlib import sha256


@dataclass(frozen=True)
class Sample:
    version: str
    basis_points: int
    user_list_sha256: str
    users: frozenset

    def identity(self):
        return {'version': self.version, 'basis_points': self.basis_points,
                'user_list_sha256': self.user_list_sha256, 'users': len(self.users)}


def load_samples(path):
    manifest = json.loads(path.read_text(encoding='utf-8'))
    version = manifest['version']
    if not isinstance(version, str) or not version.strip():
        raise ValueError('Expected a team sample version')
    samples = {}
    previous = frozenset()
    tiers = manifest['tiers']
    if not isinstance(tiers, list) or not tiers:
        raise ValueError('The team needs to provide at least one sample tier')
    for tier in sorted(tiers, key=lambda tier: tier['basis_points']):
        ratio = tier['basis_points']
        if type(ratio) is not int or not 1 <= ratio <= 10_000 or ratio in samples:
            raise ValueError('Expected a distinct basis point value for each sample tier')
        ids = (path.parent / tier['users_file']).read_text(encoding='utf-8-sig').splitlines()
        users = frozenset(ids)
        if not ids or len(users) != len(ids) or any(not user or user.strip() != user for user in users):
            raise ValueError('Team user list contains empty, duplicate or invalid IDs')
        if 'users' in tier and (type(tier['users']) is not int or tier['users'] != len(users)):
            raise ValueError(f'Team user count differs from the manifest for {ratio} basis points')
        fingerprint = sha256(('\n'.join(sorted(users)) + '\n').encode()).hexdigest()
        if fingerprint != tier['user_list_sha256'] or not previous <= users:
            raise ValueError('Team user list checksum or nesting does not match the manifest')
        samples[ratio] = Sample(version, ratio, fingerprint, users)
        previous = users
    return samples
