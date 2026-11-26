"""CPU training for purchase LightGBM models."""

import lightgbm as lgb
import numpy as np
from pathlib import Path

from ml.conversion_model.models.lightgbm.predict import load_booster


PARAMS = {
    'objective': 'binary', 'metric': 'binary_logloss', 'device_type': 'cpu',
    'num_threads': 2, 'seed': 42, 'deterministic': True, 'force_col_wise': True,
    'learning_rate': 0.05, 'num_leaves': 15, 'max_depth': 4, 'min_data_in_leaf': 80,
    'lambda_l2': 1.0, 'verbosity': -1,
}
MAX_ROUNDS = 300
PATIENCE = 25


def fit_lightgbm(train, train_labels, validation, validation_labels, state, params, output, *, extra_columns=()):
    columns = state['numeric_order'] + state['category_order'] + list(extra_columns)
    training = lgb.Dataset(train, train_labels, feature_name=columns, categorical_feature=state['category_order'])
    valid = training.create_valid(validation, validation_labels)
    model = lgb.train(params, training, num_boost_round=MAX_ROUNDS, valid_sets=[valid], valid_names=['validation'],
                      callbacks=[lgb.early_stopping(PATIENCE, verbose=False)])
    Path(output).write_text(model.model_to_string(num_iteration=model.best_iteration), encoding='utf-8', newline='\n')
    restored = load_booster(output)
    if not np.array_equal(restored.predict(validation), model.predict(validation)):
        raise ValueError('Reloaded LightGBM predictions changed')
    return restored, model.best_iteration
