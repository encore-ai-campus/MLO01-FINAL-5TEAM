import hashlib
import json
import subprocess
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from ml.conversion_model.data_prep.labels import CURRENT_LABEL_VERSION
from ml.conversion_model.models.lightgbm.predict import load_booster
from ml.conversion_model.models.lightgbm.preprocessor import transform
from ml.conversion_model.models.logistic.model import LogisticModel
from ml.conversion_model.storage import digest, read_json, write_json
from ml.conversion_model.workflows.train_october import DEFAULT_CONFIG, october_periods, train_october


def fixture(folder):
    """Use synthetic IDs and labels while exercising the October snapshot contract."""
    folder.mkdir()
    ids = 'synthetic-a\nsynthetic-b\n'
    (folder / 'users.txt').write_bytes(ids.encode())
    whole = ids + 'synthetic-out\n'
    (folder / 'whole-users.txt').write_bytes(whole.encode())
    sample = folder / 'sample.json'
    write_json(sample, {'version': 'synthetic-october-v1', 'tiers': [
        {'basis_points': bps, 'users_file': path, 'users': len(users.splitlines()),
         'user_list_sha256': hashlib.sha256(users.encode()).hexdigest()}
        for bps, path, users in ((200, 'users.txt', ids), (10000, 'whole-users.txt', whole))
    ]})
    write_json(folder / 'snapshot.json', {
        'observation_start_utc': '2019-10-01T00:00:00Z',
        'complete_before_utc': '2019-11-01T00:00:00Z',
        'input_version': 'sequence-input-v1',
        'label_version': CURRENT_LABEL_VERSION,
    })
    tables = {name: [] for name in ('observations', 'visits', 'truth')}

    def add(when, label, *, user=None, confirmed_at=None, price=15, valid=True):
        i = len(tables['observations'])
        identity = {'session_id': f'session-{i}', 'user_id': user or ('synthetic-b' if label else 'synthetic-a'),
                    'source': 'replay', 'replay_run_id': 'october-synthetic'}
        previous = when - timedelta(minutes=1)
        action = {'event_time_utc': when.isoformat(), 'event_type': 'cart' if label else 'view',
                  'product_id': str(i), 'sequence': 1}
        earlier = {**action, 'event_time_utc': previous.isoformat(), 'event_type': 'view', 'sequence': 0}
        fields = {'view_count': 1 + int(not label), 'cart_count': label, 'distinct_product_count': 1,
                  'visit_elapsed_seconds': 60, 'previous_event_gap_seconds': 60, 'price': price,
                  'is_cart': label, 'price_missing': 0, 'brand': 'brand', 'category_id': 'category',
                  'current_product_repeat_view_count': 1 + int(not label)}
        tables['observations'].append({**identity, 'prediction_id': f'p-{i}', 'event_id': f'e-{i}',
                                       'sequence': 1, 'actions': [earlier, action],
                                       'event_time_utc': when.isoformat(),
                                       'comparison_inputs': fields if valid else {}})
        ended = when + timedelta(minutes=31)
        tables['visits'].append({**identity, 'started_at_utc': previous.isoformat(), 'ended_at_utc': ended.isoformat(),
                                 'first_purchase_at_utc': (when + timedelta(minutes=1)).isoformat() if label else None})
        tables['truth'].append({**identity, 'prediction_id': f'p-{i}', 'status': 'confirmed', 'label': label,
                                'confirmed_at_utc': (confirmed_at or ended + timedelta(seconds=1)).isoformat()})

    october = datetime(2019, 10, 1, 12, tzinfo=timezone.utc)
    for split_day, n in ((1, 320), (21, 64)):
        for index in range(n):
            add(october + timedelta(days=split_day - 1, seconds=index), index % 2,
                price=0 if index % 4 == 0 else 15)
    add(datetime(2019, 10, 20, 23, tzinfo=timezone.utc), 0,
        confirmed_at=datetime(2019, 10, 21, tzinfo=timezone.utc))
    add(october, 1, valid=False)
    add(october, 1, user='synthetic-out')
    add(october + timedelta(days=26), 1)
    for name, records in tables.items():
        (folder / f'{name}.jsonl').write_text(
            ''.join(json.dumps(row) + '\n' for row in records), encoding='utf-8',
        )
    return sample


class OctoberTrainingTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(TemporaryDirectory()))
        self.snapshot = self.directory / 'snapshot'
        self.sample = fixture(self.snapshot)

    def test_cli_learns_nonconstant_scores_and_reopens_saved_artifacts(self):
        output = self.directory / 'run'
        run = subprocess.run(
            [sys.executable, '-B', '-m', 'ml.conversion_model.workflows.train_october',
             '--sample', str(self.sample), '--snapshot', str(self.snapshot), '--out', str(output)],
            cwd=Path(__file__).resolve().parents[3], capture_output=True, text=True,
        )
        self.assertEqual(run.returncode, 0, run.stderr)
        report = read_json(output / 'report.json')
        self.assertEqual(json.loads(run.stdout), report)
        self.assertEqual(report['train']['shared_rows'], 320)
        self.assertEqual(report['train']['shared_purchase_rows'], 160)
        self.assertEqual(report['validation']['shared_rows'], 64)
        self.assertEqual(report['validation']['shared_purchase_rows'], 32)
        self.assertEqual(report['train']['status_counts']['awaiting_truth'], 1)
        self.assertEqual(report['train']['excluded_invalid_either_model'], 1)
        self.assertEqual(report['sample']['basis_points'], 200)
        for model in report['models'].values():
            verified = model['validation_predictions']
            self.assertTrue(verified['reload_exact'])
            self.assertGreater(verified['distinct'], 1)
        for filename, sha in report['files_sha256'].items():
            self.assertEqual(digest(output / filename), sha)
        previous, row = (json.loads(line) for line in (self.snapshot / 'observations.jsonl')
                         .read_text(encoding='utf-8').splitlines()[:2])
        model = LogisticModel.load(output / 'logistic.json')
        scores = [model.predict_actions(item['actions'], event_time=item['event_time_utc'], sequence=1)
                  ['purchase_probability'] for item in (previous, row)]
        self.assertLess(*scores)
        model = load_booster(output / 'lightgbm.txt')
        state = read_json(output / 'preprocessor.json')
        self.assertEqual(len(model.feature_name()), 11)
        self.assertEqual(transform(previous['comparison_inputs'], state),
                         transform({**previous['comparison_inputs'], 'price': None, 'price_missing': 1}, state))
        self.assertEqual(state['price_median'], 15)

    def test_sample_tier_is_configurable_and_requires_manifest(self):
        config = read_json(DEFAULT_CONFIG)
        path = self.directory / 'config.json'
        output = self.directory / 'run'
        config['sample_bps'] = 500
        write_json(path, config)
        with self.assertRaisesRegex(ValueError, 'tier available'):
            train_october(path, self.sample, self.snapshot, output)
        self.assertFalse(output.exists())
        config['sample_bps'] = 10000
        write_json(path, config)
        result = train_october(path, self.sample, self.snapshot, output)
        self.assertEqual(result['sample']['users'], 3)
        self.assertEqual(result['train']['shared_rows'], 321)
        with self.assertRaises(FileExistsError):
            train_october(path, self.sample, self.snapshot, output)

        metadata_path = self.snapshot / 'snapshot.json'
        write_json(metadata_path, {**read_json(metadata_path), 'sample': result['sample']})
        with self.assertRaisesRegex(ValueError, 'selected user sample do not match'):
            train_october(DEFAULT_CONFIG, self.sample, self.snapshot, self.directory / 'mismatch')

    def test_overlapping_window_and_incomplete_snapshot_fail(self):
        config = read_json(DEFAULT_CONFIG)
        config['validation_period_utc'][0] = '2019-10-20T00:00:00Z'
        with self.assertRaisesRegex(ValueError, 'non-overlapping'):
            october_periods(config)
        path = self.snapshot / 'snapshot.json'
        write_json(path, {**read_json(path), 'complete_before_utc': '2019-10-26T00:00:00Z'})
        output = self.directory / 'incomplete'
        with self.assertRaisesRegex(ValueError, 'does not cover'):
            train_october(DEFAULT_CONFIG, self.sample, self.snapshot, output)
        self.assertFalse(output.exists())
