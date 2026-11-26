"""Compare recommendation results on the same requests."""

import argparse
import json
import math
from hashlib import sha256
from pathlib import Path

import numpy as np


BOOTSTRAP_UNIT = 'user'
METRICS = ('candidate_recall', 'recall', 'hit', 'ndcg')


def paired_interval(candidate, baseline, metric, repeats, seed, *, unit=BOOTSTRAP_UNIT):
    if metric not in METRICS:
        raise ValueError(f'Unsupported comparison metric: {metric}')
    if unit != BOOTSTRAP_UNIT:
        raise ValueError('Recommendation confidence intervals resample by user')
    if type(repeats) is not int or repeats < 2:
        raise ValueError('A confidence interval requires at least two bootstrap resamples')
    if len(candidate) != len(baseline):
        raise ValueError('Compared request counts differ')

    by_user = {}
    seen = set()
    for left, right in zip(candidate, baseline, strict=True):
        request_id = left['request_id']
        if (request_id != right['request_id'] or request_id in seen or left['user'] != right['user']
                or left['has_truth'] != right['has_truth']
                or left.get('eligible_purchase_ids') != right.get('eligible_purchase_ids')):
            raise ValueError('Compared queries differ or contain duplicate request IDs')
        seen.add(request_id)
        if left['has_truth']:
            by_user.setdefault(left['user'], []).append(left[metric] - right[metric])

    result = {'unit': unit, 'users': len(by_user), 'mean_delta': None, 'ci95': [None, None]}
    if len(by_user) < 2:
        return result

    sums = np.asarray([sum(deltas) for deltas in by_user.values()])
    counts = np.asarray([len(deltas) for deltas in by_user.values()])
    rng = np.random.default_rng(seed)
    samples = []
    for _ in range(repeats):
        indices = rng.integers(0, len(sums), len(sums))
        samples.append(float(sums[indices].sum() / counts[indices].sum()))
    return {**result, 'mean_delta': float(sums.sum() / counts.sum()),
            'ci95': [float(value) for value in np.quantile(samples, [.025, .975])]}


def product_ids(row, key, limit=None):
    values = row.get(key)
    if (not isinstance(values, list) or (limit is not None and len(values) > limit)
            or any(type(value) not in (int, str) or value == '' for value in values)
            or len(set(values)) != len(values)):
        raise ValueError(f'expected distinct product IDs in {key}')
    return values


def score_ranked_row(row):
    truth = frozenset(product_ids(row, 'eligible_purchase_ids'))
    candidates = product_ids(row, 'candidate_ids', 100)
    ranked = product_ids(row, 'ranked_ids', 10)
    hits = len(set(ranked) & truth)
    dcg = sum(1 / np.log2(i + 2) for i, item in enumerate(ranked) if item in truth)
    ideal = sum(1 / np.log2(i + 2) for i in range(min(10, len(truth))))
    return {
        'request_id': row['request_id'], 'user': row['user'],
        'has_truth': bool(truth), 'eligible_purchase_ids': truth,
        'candidate_recall': len(set(candidates) & truth) / len(truth) if truth else 0.,
        'recall': hits / len(truth) if truth else 0.,
        'hit': int(hits > 0), 'ndcg': float(dcg / ideal) if ideal else 0.,
    }


def read_results(path, metric, input_format, ranking=None):
    rows = []
    digest = sha256()
    with path.open('rb') as source:
        for line_number, line in enumerate(source, 1):
            digest.update(line)
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError('expected a JSON object')
                if ranking is not None and row.get('ranking') != ranking:
                    continue
                if (type(row.get('request_id')) not in (str, int)
                        or row['request_id'] == '' or type(row.get('user')) not in (str, int)
                        or row['user'] == ''):
                    raise ValueError('expected request_id and user')
                if input_format == 'ranked':
                    row = score_ranked_row(row)
                else:
                    if type(row.get('has_truth')) is not bool:
                        raise ValueError('expected boolean has_truth')
                    value = row.get(metric)
                    if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1:
                        raise ValueError(f'expected a finite {metric} score between 0 and 1')
                    row = {key: row[key] for key in ('request_id', 'user', 'has_truth', metric)}
            except (ValueError, OverflowError) as error:
                raise ValueError(f'{path.name}:{line_number}: {error}') from error
            rows.append(row)
    if ranking is not None and not rows:
        raise ValueError(f'{path.name}: no rows for ranking {ranking!r}')
    return rows, digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', required=True, type=Path)
    parser.add_argument('--baseline', required=True, type=Path)
    parser.add_argument('--input-format', choices=['ranked', 'scored'], default='ranked')
    parser.add_argument('--candidate-ranking', help='Select a ranking from a scored JSONL file')
    parser.add_argument('--baseline-ranking', help='Select a ranking from a scored JSONL file')
    parser.add_argument('--metric', choices=METRICS, default='ndcg')
    parser.add_argument('--repeats', required=True, type=int)
    parser.add_argument('--seed', required=True, type=int)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.input_format != 'scored' and (args.candidate_ranking or args.baseline_ranking):
        parser.error('Ranking selectors require --input-format scored')
    try:
        candidate, candidate_hash = read_results(
            args.candidate, args.metric, args.input_format, args.candidate_ranking,
        )
        baseline, baseline_hash = read_results(
            args.baseline, args.metric, args.input_format, args.baseline_ranking,
        )
        interval = paired_interval(candidate, baseline, args.metric, args.repeats, args.seed)
        report = {
            'metric': args.metric,
            'input_format': args.input_format,
            'candidate_ranking': args.candidate_ranking,
            'baseline_ranking': args.baseline_ranking,
            'requests': len(candidate),
            'requests_with_truth': sum(row['has_truth'] for row in candidate),
            'bootstrap': {'unit': BOOTSTRAP_UNIT, 'repeats': args.repeats, 'seed': args.seed},
            'candidate_sha256': candidate_hash,
            'baseline_sha256': baseline_hash,
            'comparison': interval,
        }
        with args.output.open('x', encoding='utf-8', newline='\n') as destination:
            destination.write(json.dumps(report, indent=2, allow_nan=False) + '\n')
    except (OSError, ValueError) as error:
        parser.exit(1, f'{error}\n')


if __name__ == '__main__':
    main()
