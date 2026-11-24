"""Evaluate mature purchase predictions and emit local alert records."""
import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from sklearn.metrics import average_precision_score, brier_score_loss

from .store import checksum


def assess(rows, policy):
    groups = defaultdict(list)
    prediction_keys = set()
    for row in rows:
        prediction_key = (row['replay_run_id'], row['prediction_id'])
        if not all(prediction_key) or prediction_key in prediction_keys:
            raise ValueError(f'Missing or duplicate prediction: {prediction_key}')
        prediction_keys.add(prediction_key)
        key = (row['replay_run_id'], row['model_version'])
        if not all(key):
            raise ValueError(f'Missing replay or model version: {key}')
        groups[key].append(row)

    results, alerts = [], []
    for (run, model), records in sorted(groups.items()):
        final = [row for row in records if row['status'] == 'final' and row['source'] == 'replay']
        for row in final:
            probability = row['prediction_probability']
            if (not row['visit_id'] or row['label'] not in (0, 1) or probability is None
                    or not math.isfinite(probability) or not 0 <= probability <= 1):
                raise ValueError(f"Invalid finalized prediction: {row['prediction_id']}")
        visits = Counter(row['visit_id'] for row in final)
        labels = [row['label'] for row in final]
        scores = [row['prediction_probability'] for row in final]
        weights = [1 / visits[row['visit_id']] for row in final]
        positive_count = sum(labels)
        metrics = {
            'average_precision': float(average_precision_score(labels, scores, sample_weight=weights))
            if positive_count else None,
            'brier': float(brier_score_loss(labels, scores, sample_weight=weights)) if labels else None,
        }
        reasons = []
        if len(visits) < policy['min_visits'] or set(labels) != {0, 1}:
            status = 'insufficient_data'
        else:
            if metrics['average_precision'] <= policy['baseline_ap'] - policy['ap_drop']:
                reasons.append('average_precision_drop')
            if metrics['brier'] >= policy['baseline_brier'] + policy['brier_rise']:
                reasons.append('brier_increase')
            status = 'degraded' if reasons else 'normal'
        row = {'replay_run_id': run, 'model_version': model, 'input_rows': len(records),
               'input_status_counts': dict(Counter(row['status'] for row in records)),
               'scored_rows': len(final), 'ignored_rows': len(records) - len(final), 'scored_visits': len(visits),
               'positive_rows': positive_count, 'negative_rows': len(final) - positive_count,
               'metrics': metrics, 'status': status, 'alert_reasons': reasons}
        results.append(row)
        if reasons:
            alerts.append({'type': 'purchase_performance_degraded', **row})
    return results, alerts


def monitor(labels, policy, output):
    rows = pq.read_table(labels).to_pylist()
    results, alerts = assess(rows, policy)
    output.mkdir(parents=True, exist_ok=True)
    report = {'input_sha256': checksum(labels), 'input_rows': len(rows), 'policy': policy, 'results': results,
              'alert_count': len(alerts)}
    for filename, data in [('metrics.json', report), ('alerts.json', alerts)]:
        destination = output / filename
        partial = destination.with_suffix('.tmp')
        partial.write_text(json.dumps(data, indent=2, allow_nan=False) + '\n', encoding='utf-8')
        partial.replace(destination)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--labels', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline-ap', type=float, required=True)
    parser.add_argument('--baseline-brier', type=float, required=True)
    parser.add_argument('--ap-drop', type=float, required=True)
    parser.add_argument('--brier-rise', type=float, required=True)
    parser.add_argument('--min-visits', type=int, required=True)
    args = parser.parse_args()
    if (not 0 <= args.baseline_ap <= 1 or not 0 <= args.baseline_brier <= 1
            or not 0 < args.ap_drop <= args.baseline_ap or not 0 < args.brier_rise <= 1 - args.baseline_brier
            or args.min_visits < 1):
        parser.error('Use valid probability metrics, thresholds and a positive visit minimum')
    policy = {key: getattr(args, key) for key in ('baseline_ap', 'baseline_brier', 'ap_drop', 'brier_rise', 'min_visits')}
    print(json.dumps(monitor(args.labels, policy, args.output), indent=2))
