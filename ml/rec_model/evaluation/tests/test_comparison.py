import json
from hashlib import sha256
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from ml.rec_model.evaluation import comparison


def result(request, user, truth, score):
    return {'request_id': request, 'user': user, 'has_truth': truth, 'ndcg': score}


class PairedIntervalTests(unittest.TestCase):
    def test_resamples_users_and_preserves_request_weighting(self):
        candidate = [result('a', 'u1', True, .5), result('b', 'u1', True, .75),
                     result('c', 'u2', True, .5), result('d', 'u3', False, 0)]
        baseline = [result('a', 'u1', True, .25), result('b', 'u1', True, .25),
                    result('c', 'u2', True, .75), result('d', 'u3', False, 0)]
        actual = comparison.paired_interval(candidate, baseline, 'ndcg', 1000, 42)
        self.assertEqual(actual, {'unit': 'user', 'users': 2, 'mean_delta': 1 / 6,
                                  'ci95': [-.25, .375]})

    def test_rejects_unmatched_and_duplicate_requests(self):
        row = result('a', 'u1', False, 0)
        for candidate, baseline in (
            ([row], []), ([], [row]), ([row], [{**row, 'request_id': 'b'}]),
            ([row], [{**row, 'user': 'u2'}]), ([row], [{**row, 'has_truth': True}]),
            ([row, row], [row, row]),
        ):
            with self.subTest(candidate=candidate, baseline=baseline), self.assertRaises(ValueError):
                comparison.paired_interval(candidate, baseline, 'ndcg', 100, 42)

    def test_incomplete_interval_keeps_unit_and_rejects_bad_settings(self):
        row = result('a', 'u1', True, .5)
        self.assertEqual(comparison.paired_interval([row], [row], 'ndcg', 2, 42),
                         {'unit': 'user', 'users': 1, 'mean_delta': None, 'ci95': [None, None]})
        for repeats, unit in ((1, 'user'), (True, 'user'), (2, 'request')):
            with self.subTest(repeats=repeats, unit=unit), self.assertRaises(ValueError):
                comparison.paired_interval([row], [row], 'ndcg', repeats, 42, unit=unit)

    def test_ranking_metrics_and_different_truth_sets(self):
        row = {'request_id': 'a', 'user': 'u1', 'eligible_purchase_ids': [10, 20],
               'candidate_ids': [10, 30], 'ranked_ids': [30, 10]}
        scored = comparison.score_ranked_row(row)
        self.assertEqual(scored['candidate_recall'], .5)
        self.assertEqual(scored['recall'], .5)
        self.assertEqual(scored['hit'], 1)
        self.assertAlmostEqual(scored['ndcg'], .38685280723454163)
        different = comparison.score_ranked_row({**row, 'eligible_purchase_ids': [10, 30]})
        with self.assertRaisesRegex(ValueError, 'Compared queries differ'):
            comparison.paired_interval([scored], [different], 'recall', 2, 42)

    def test_reads_existing_multi_ranking_report_without_reexport(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        path = root / 'cumulative-request-metrics.jsonl'
        ranked = result('a', 'u1', True, .75)
        rows = [{**ranked, 'ranking': 'ranker'}, {**ranked, 'ranking': 'popular', 'ndcg': .5}]
        path.write_text(''.join(json.dumps(row) + '\n' for row in rows), encoding='utf-8')
        values, digest = comparison.read_results(path, 'ndcg', 'scored', 'ranker')
        self.assertEqual(values, [ranked])
        self.assertEqual(digest, sha256(path.read_bytes()).hexdigest())
        with self.assertRaisesRegex(ValueError, 'no rows for ranking'):
            comparison.read_results(path, 'ndcg', 'scored', 'missing')

    def test_cli_runs_without_any_pipeline_source(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        module = root / 'ml' / 'rec_model' / 'evaluation' / 'comparison.py'
        module.parent.mkdir(parents=True)
        shutil.copyfile(comparison.__file__, module)
        candidate = root / 'candidate.jsonl'
        baseline = root / 'baseline.jsonl'
        output = root / 'comparison.json'
        for path, ranking in ((candidate, [10, 20]), (baseline, [20, 30])):
            rows = [
                {'request_id': key, 'user': user, 'eligible_purchase_ids': [10, 30],
                 'candidate_ids': [10, 20, 30], 'ranked_ids': ranking}
                for key, user in (('a', 'u1'), ('b', 'u2'))
            ]
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows), encoding='utf-8')
        environment = os.environ.copy()
        environment['PYTHONPATH'] = str(Path(comparison.np.__file__).resolve().parents[1])
        command = [
            sys.executable, '-B', '-m', 'ml.rec_model.evaluation.comparison',
            '--candidate', str(candidate), '--baseline', str(baseline),
            '--repeats', '10', '--seed', '42', '--output', str(output),
        ]
        completed = subprocess.run(command, cwd=root, env=environment, capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        report = json.loads(output.read_text(encoding='utf-8'))
        self.assertEqual(report['candidate_sha256'], sha256(candidate.read_bytes()).hexdigest())
        self.assertEqual(report['baseline_sha256'], sha256(baseline.read_bytes()).hexdigest())
        self.assertEqual(report['requests_with_truth'], 2)
        self.assertEqual(report['comparison']['unit'], 'user')
        self.assertAlmostEqual(report['comparison']['mean_delta'], .2262943855309168)
        baseline.write_text('{"request_id":"a","user":"u1","has_truth":true,"ndcg":2}\n',
                            encoding='utf-8')
        failed = subprocess.run(
            command + ['--input-format', 'scored', '--candidate', str(baseline),
                       '--output', str(root / 'invalid.json')],
            cwd=root, env=environment, capture_output=True, text=True,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn('baseline.jsonl:1', failed.stderr)
        self.assertFalse((root / 'invalid.json').exists())
