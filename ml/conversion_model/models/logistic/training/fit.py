"""Train a logistic purchase model on prepared, time-separated inputs."""

import json
import warnings
from time import perf_counter

import numpy as np

from ..model import MODEL_FORMAT, SCHEMAS, score_logistic


REGULARIZATION = (0.1, 1.0, 10.0)


def fit_logistic(train, train_labels, validation, validation_labels, output, *, diagnostics=None,
                 model_format=MODEL_FORMAT, regularization=REGULARIZATION):
    from sklearn import __version__ as sklearn_version
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import log_loss
    from sklearn.preprocessing import StandardScaler

    numeric, columns = SCHEMAS[model_format]
    size = len(numeric)
    scaler = StandardScaler().fit(train[:, :size])
    scaled_train = np.column_stack((scaler.transform(train[:, :size]), train[:, size:]))
    scaled_validation = np.column_stack((scaler.transform(validation[:, :size]), validation[:, size:]))
    candidates = {}
    validation_loss = {}
    for c in regularization:
        model = LogisticRegression(C=c, penalty='l2', solver='lbfgs', max_iter=1000)
        started = perf_counter()
        with warnings.catch_warnings():
            warnings.simplefilter('error', ConvergenceWarning)
            try:
                model.fit(scaled_train, train_labels)
            except ConvergenceWarning:
                if diagnostics is not None:
                    diagnostics[str(c)] = {'fit_seconds': perf_counter() - started, 'converged': False}
                raise
        if diagnostics is not None:
            diagnostics[str(c)] = {'fit_seconds': perf_counter() - started, 'converged': True,
                                   'iterations': int(model.n_iter_[0])}
        validation_loss[c] = log_loss(validation_labels, model.predict_proba(scaled_validation), labels=[0, 1])
        candidates[c] = model
    best = min(regularization, key=lambda c: (validation_loss[c], c))
    chosen = candidates[best]
    bundle = {
        'version': model_format, 'sklearn_version': sklearn_version,
        'columns': columns, 'scaled_columns': numeric, 'mean': scaler.mean_.tolist(), 'scale': scaler.scale_.tolist(),
        'coefficients': chosen.coef_[0].tolist(), 'intercept': float(chosen.intercept_[0]),
        'selected_c': best, 'validation_log_loss': validation_loss,
        'iterations_by_c': {c: int(model.n_iter_[0]) for c, model in candidates.items()},
    }
    output.write_text(json.dumps(bundle, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    restored = json.loads(output.read_text(encoding='utf-8'))
    if not np.allclose(score_logistic(restored, validation), chosen.predict_proba(scaled_validation)[:, 1],
                       atol=1e-12, rtol=0):
        raise ValueError('Reloaded logistic predictions changed')
    return restored
