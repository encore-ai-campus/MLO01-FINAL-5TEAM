"""Fit two purchase models on one versioned October input snapshot."""

import argparse
import json
import platform
from pathlib import Path

import lightgbm
import numpy as np
import sklearn
from threadpoolctl import threadpool_limits

from ml.conversion_model.data_prep.labels import CURRENT_LABEL_VERSION
from ml.conversion_model.data_prep.team_data import TeamSnapshot, load_samples
from ml.conversion_model.models.lightgbm.predict import load_booster
from ml.conversion_model.models.lightgbm.preprocessor import fit_rows, tabular_matrix
from ml.conversion_model.models.lightgbm.training.fit import PARAMS, fit_lightgbm
from ml.conversion_model.models.logistic.features import utc_datetime
from ml.conversion_model.models.logistic.model import LogisticModel, matrix, score_logistic
from ml.conversion_model.models.logistic.training.fit import fit_logistic
from ml.conversion_model.storage import digest, read_json, rows_hash, staging_directory, write_json


DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / 'config' / 'october_training.json'
SNAPSHOT_FILES = ('snapshot.json', 'observations.jsonl', 'visits.jsonl', 'truth.jsonl')
PERIODS = ('training_period_utc', 'validation_period_utc', 'development_evaluation_period_utc')
OCTOBER_START = utc_datetime('2019-10-01T00:00:00Z')
NOVEMBER_START = utc_datetime('2019-11-01T00:00:00Z')


def october_periods(config):
    windows = {}
    previous = OCTOBER_START
    for period in PERIODS:
        start, end = map(utc_datetime, config[period])
        if start != previous or not start < end <= NOVEMBER_START:
            raise ValueError('October windows must be contiguous and non-overlapping')
        windows[period] = (start, end)
        previous = end
    if previous != NOVEMBER_START:
        raise ValueError('The windows must cover the whole October UTC calendar')
    return windows


def training_rows(snapshot, sample, window):
    start, end = window
    if end > snapshot.complete_before:
        raise ValueError('Snapshot does not cover the complete training and validation windows')
    rows, counts = snapshot.rows(sample, start, end)
    shared = [row for row in rows if row['logistic_valid'] and row['lightgbm_valid']]
    labels = [row['label'] for row in shared]
    if set(labels) != {0, 1}:
        raise ValueError('Both purchase labels are required in each shared split')
    return shared, np.asarray(labels), {
        **counts, 'shared_rows': len(shared), 'excluded_invalid_either_model': len(rows) - len(shared),
        'shared_sessions': len({row['visit_id'] for row in shared}), 'shared_purchase_rows': sum(labels),
        'shared_rows_sha256': rows_hash(shared),
    }


def verify_scores(scores, reloaded, name):
    values = np.asarray(reloaded)
    if (not np.array_equal(scores, values) or not np.isfinite(values).all()
            or (values < 0).any() or (values > 1).any()):
        raise ValueError(f'{name} failed to reproduce valid saved probabilities')
    return {
        'count': len(values), 'min': float(values.min()), 'max': float(values.max()),
        'distinct': int(np.unique(values).size), 'reload_exact': True,
    }


def train_october(config_path, sample_manifest, snapshot_dir, output):
    config = read_json(config_path)
    windows = october_periods(config)
    samples = load_samples(sample_manifest)
    bps = config['sample_bps']
    if type(bps) is not int or bps not in samples:
        raise ValueError('Set sample_bps to a tier available in the versioned user manifest')
    sample = samples[bps]
    metadata = read_json(snapshot_dir / 'snapshot.json')
    if metadata['input_version'] != 'sequence-input-v1' or metadata['label_version'] != CURRENT_LABEL_VERSION:
        raise ValueError('Snapshot does not use the expected input and label contract')
    if 'sample' in metadata and metadata['sample'] != sample.identity():
        raise ValueError('Prepared snapshot and selected user sample do not match')
    snapshot_hashes = {name: digest(snapshot_dir / name) for name in SNAPSHOT_FILES}
    snapshot = TeamSnapshot.load(snapshot_dir)
    train, train_labels, train_counts = training_rows(snapshot, sample, windows[PERIODS[0]])
    validation, valid_labels, valid_counts = training_rows(snapshot, sample, windows[PERIODS[1]])
    if {row['visit_id'] for row in train} & {row['visit_id'] for row in validation}:
        raise ValueError('Training and validation share a session')

    if output.exists():
        raise FileExistsError(output)
    with staging_directory(output.parent) as stage, threadpool_limits(limits=2):
        result = stage / 'result'
        result.mkdir()
        logistic_path = result / 'logistic.json'
        valid_matrix = matrix(validation)
        fitted = fit_logistic(matrix(train), train_labels, valid_matrix, valid_labels, logistic_path,
                              regularization=(config['logistic_c'],))
        LogisticModel.load(logistic_path)
        logistic_check = verify_scores(
            score_logistic(fitted, valid_matrix),
            score_logistic(read_json(logistic_path), valid_matrix),
            'Logistic regression',
        )
        comparison_rows = [{**row, 'split': 'train'} for row in train]
        state = fit_rows(comparison_rows, training_sha256=rows_hash(comparison_rows),
                         training_period=config[PERIODS[0]], include_repeat=True, team_window=True)
        state_path = result / 'preprocessor.json'
        write_json(state_path, state)
        tree_path = result / 'lightgbm.txt'
        params = {**PARAMS, 'seed': config['lightgbm_seed']}
        valid_tabular = tabular_matrix(validation, state)
        booster, iterations = fit_lightgbm(tabular_matrix(train, state), train_labels,
                                          valid_tabular, valid_labels, state, params, tree_path)
        lightgbm_check = verify_scores(
            booster.predict(valid_tabular, num_threads=2),
            load_booster(tree_path).predict(tabular_matrix(validation, read_json(state_path)), num_threads=2),
            'LightGBM',
        )
        report = {
            'config': config, 'config_sha256': digest(config_path), 'sample': sample.identity(),
            'snapshot_sha256': snapshot_hashes, 'input_version': snapshot.input_version,
            'label_version': snapshot.label_version,
            'versions': {'python': platform.python_version(), 'numpy': np.__version__, 'scikit-learn': sklearn.__version__,
                         'lightgbm': lightgbm.__version__},
            'train': train_counts, 'validation': valid_counts,
            'models': {
                'logistic': {'c': fitted['selected_c'], 'validation_predictions': logistic_check},
                'lightgbm': {'iterations': iterations, 'price_policy': state['price_policy'],
                             'validation_predictions': lightgbm_check},
            },
            'files_sha256': {path.name: digest(path) for path in (logistic_path, tree_path, state_path)},
        }
        write_json(result / 'report.json', report)
        result.rename(output.resolve())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('--sample', type=Path, required=True, help='Versioned user list manifest')
    parser.add_argument('--snapshot', type=Path, required=True, help='Prepared observations, sessions and truth')
    parser.add_argument('--out', type=Path, required=True, help='New model output directory')
    args = parser.parse_args()
    print(json.dumps(train_october(args.config, args.sample, args.snapshot, args.out), ensure_ascii=True))


if __name__ == '__main__':
    main()
