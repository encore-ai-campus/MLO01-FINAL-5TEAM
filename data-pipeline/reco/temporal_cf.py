"""Replay CF refreshes: each snapshot uses strictly earlier events only."""
import json
from pathlib import Path

from . import cf
from .core import iso
from .store import checksum


def cutoff_for_time(as_of, anchor, refresh_days):
    if as_of < anchor:
        raise ValueError('No CF snapshot exists before the anchor')
    interval = int(refresh_days * 86400)
    return anchor if not interval else anchor + ((as_of-anchor)//interval)*interval


class TemporalCF:
    def __init__(self, events, config, weights, cache):
        self.events, self.config, self.weights = events, config, weights
        self.policy_signature=cf.cache_signature(config,weights)
        self.cache = Path(cache)/self.policy_signature[:24]
        self.cache.mkdir(parents=True, exist_ok=True)
        self.snapshots = {}
        self.current_cutoff, self.current = None, None

    def for_time(self, as_of):
        cutoff = cutoff_for_time(as_of, self.config['cf_cutoff_ts'], self.config.get('cf_refresh_days', 0))
        if cutoff == self.current_cutoff:
            return self.current
        file = self.cache / f'{cutoff}.json'
        metadata = file.with_suffix('.meta.json')
        if file.exists() and metadata.exists():
            info = json.loads(metadata.read_text('utf-8'))
            if checksum(file) != info['sha256']:
                raise ValueError('CF snapshot cache checksum mismatch')
            raw = json.loads(file.read_text('utf-8'))
            neighbors = {int(p): [(int(n), float(s)) for n,s in values] for p, values in raw.items()}
        else:
            print(f'Fitting CF snapshot before {iso(cutoff)}', flush=True)
            neighbors, support = cf.fit(self.events, cutoff, self.weights, self.config)
            file.write_text(json.dumps(neighbors, separators=(',', ':')), 'utf-8')
            info = {'cutoff_ts': cutoff, 'cutoff': iso(cutoff), 'support': support,
                    'policy_signature':self.policy_signature,
                    'sha256': checksum(file), 'version': 'cf-'+checksum(file)[:20]}
            metadata.write_text(json.dumps(info, indent=2), 'utf-8')
            print(f'CF snapshot ready: {support}', flush=True)
        self.snapshots[cutoff] = {**info, 'path': str(file.resolve())}
        self.current_cutoff, self.current = cutoff, neighbors
        return neighbors
