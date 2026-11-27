"""Train a price-policy recommendation ranker from frozen October events."""

import argparse
import json
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import lightgbm as lgb
import numpy as np

from reco import cf, core, evaluate_cumulative, pipeline, prior_interest, temporal_cf
from reco.core import FEATURES, timestamp
from reco.evaluate_cumulative import PastOnlyCF, read_month, replay_month
from reco.pipeline import pack_rank_data, query_plan
from reco.team_replay import load_policy

from .input_contract import checksum, require_closed, team_users, training_receipt
from .price_policy import PRICE_POLICY, PriceState, request_candidates


TRAIN_START = '2019-10-08T00:00:00Z'
VALID_START = '2019-10-25T00:00:00Z'
VALID_END = '2019-11-01T00:00:00Z'


def input_code_receipt():
    sources = {
        'ml.rec_model.price_policy': Path(__file__).with_name('price_policy.py'),
        'ml.rec_model.ranking_training': Path(__file__),
        **{module.__name__: Path(module.__file__)
           for module in (cf, core, evaluate_cumulative, pipeline, prior_interest, temporal_cf)},
    }
    digests = {name: sha256(path.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
               for name, path in sources.items()}
    version = sha256(json.dumps(digests, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()
    return {'input_code_version': version, 'input_code_sources': digests}


def partition_queries(database, config, timeline):
    events, sessions = read_month(database, timestamp(TRAIN_START), timestamp(VALID_END),
                                  config['session_gap_seconds'], config['session_gap_boundary'])
    if not events:
        raise ValueError('There are no events in the October training window')
    if any(event.action not in (0, 1, 2) for event in events):
        raise ValueError('Supported events are view, cart and purchase')
    plan, stats = query_plan(events, sessions, config)
    if not {'rank_train', 'validation'} <= set(plan.values()):
        raise ValueError('Both training and validation need closed view or cart requests')
    queries = replay_month(database, events, sessions, plan, timeline, config,
                           state_factory=PriceState, candidate_builder=request_candidates)
    threshold = timestamp(VALID_START)
    train = [q for q in queries if q['as_of'] < threshold]
    validation = [q for q in queries if q['as_of'] >= threshold]
    return train, validation, stats


def rank_rows(queries):
    for query in queries:
        features = query['features']
        if features.dtype != np.float32 or features.shape != (len(query['items']), len(FEATURES)):
            raise ValueError('Request candidates must use the 18 ordered float32 inputs')
        if not np.isfinite(features).all():
            raise ValueError('Recommendation inputs must be finite after preprocessing')
    return pack_rank_data(queries)


def fit_ranker(train, validation, config):
    x, y, groups = rank_rows(train)
    vx, vy, valid_groups = rank_rows(validation)
    dataset = lgb.Dataset(x, label=y, group=groups, feature_name=FEATURES)
    validation_set = lgb.Dataset(vx, label=vy, group=valid_groups, reference=dataset)
    ranker = lgb.train(
        {'objective': 'lambdarank', 'label_gain': [0, 1], 'metric': 'ndcg', 'eval_at': [10],
         'learning_rate': .03, 'num_leaves': 15, 'max_depth': 5, 'min_data_in_leaf': 30,
         'lambda_l2': 2, 'seed': config['seed'], 'num_threads': config['threads'],
         'verbosity': -1, 'deterministic': True, 'force_col_wise': True},
        dataset, num_boost_round=300, valid_sets=[validation_set],
        callbacks=[lgb.early_stopping(30, verbose=False)],
    )
    fit = {'groups': len(groups), 'rows': len(y), 'validation_groups': len(valid_groups),
           'validation_rows': len(vy), 'best_iteration': ranker.best_iteration}
    return ranker, vx, fit


def train(database, output, sample, sample_manifest=None):
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    receipt = training_receipt(database, sample, sample_manifest)
    code_receipt = input_code_receipt()
    policy = load_policy()
    config = {**policy, 'rank_train': [TRAIN_START, VALID_START], 'validation': [VALID_START, VALID_END],
              'test': [VALID_END, VALID_END], 'future': [VALID_END, VALID_END],
              'queries_per_session': 3,
              'query_limits': {'rank_train': 20000, 'validation': 6000, 'test': 0, 'future': 0},
              'cf_cutoff_ts': timestamp(TRAIN_START)}
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix='.ranking-training-', dir=output.parent) as directory:
        temporary = Path(directory)
        timeline = PastOnlyCF(database, config, temporary / 'snapshots')
        training, validation, stats = partition_queries(database, config, timeline)
        ranker, validation_inputs, fit = fit_ranker(training, validation, config)
        staged = temporary / 'bundle'
        staged.mkdir()
        ranker_path = staged / 'ranker.txt'
        ranker_path.write_bytes(ranker.model_to_string().encode('utf-8'))
        restored = lgb.Booster(model_str=ranker_path.read_text(encoding='utf-8'))
        # Recheck a bounded set of validation scores without fitting another model.
        probe = validation_inputs[:128]
        if restored.feature_name() != FEATURES or not np.array_equal(
                restored.predict(probe, num_threads=1), ranker.predict(probe, num_threads=1)):
            raise ValueError('Saved ranker did not reproduce its validation predictions')
        (staged / 'policy.json').write_text(json.dumps(policy, indent=2) + '\n', encoding='utf-8')
        require_closed(database)
        if (checksum(database) != receipt['event_db_sha256']
                or checksum(database.with_suffix('.manifest.json')) != receipt['manifest_sha256']):
            raise ValueError('Training data changed before bundle completion')
        if sample_manifest is not None:
            _, identity, digest = team_users(sample_manifest)
            if identity != receipt['team_sample'] or digest != receipt['sample_manifest_sha256']:
                raise ValueError('The shared 2% sample changed before bundle completion')
        if input_code_receipt() != code_receipt:
            raise ValueError('Input code changed before bundle completion')
        manifest = {
            'price_policy': PRICE_POLICY, 'features': FEATURES, 'feature_dtype': 'float32',
            **code_receipt,
            'ranking_training_window': config['rank_train'], 'early_stopping_window': config['validation'],
            'validation_end_utc': VALID_END, 'training_input': receipt, 'requests': stats, 'fit': fit,
            'checksums': {name: checksum(staged / name) for name in ('ranker.txt', 'policy.json')},
        }
        (staged / 'price-model.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
        if output.exists() or output.is_symlink():
            raise FileExistsError(output)
        staged.rename(output)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=Path, required=True)
    parser.add_argument('--sample', choices=('local', 'team'), required=True)
    parser.add_argument('--sample-manifest', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if (args.sample == 'team') != (args.sample_manifest is not None):
        parser.error('--sample-manifest is required with --sample team only')
    manifest = train(args.events, args.output, args.sample, args.sample_manifest)
    print(json.dumps({'output': str(args.output), 'fit': manifest['fit'], 'price_policy': PRICE_POLICY}))


if __name__ == '__main__':
    main()
