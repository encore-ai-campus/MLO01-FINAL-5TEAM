"""Score LightGBM artifacts using their saved input transformation."""

from pathlib import Path

import numpy as np

from ml.conversion_model.evaluation.comparison_io import validate_probabilities as validate_probabilities
from ml.conversion_model.models.lightgbm.preprocessor import transform
from ml.conversion_model.storage import digest, read_json


REFERENCE_DIR = Path(__file__).parent


def reference_files():
    expected = read_json(REFERENCE_DIR / 'reference.json')['files_sha256']
    files = {name: REFERENCE_DIR / name for name in ('lightgbm.txt', 'preprocessor.json')}
    if any(digest(path) != expected[name] for name, path in files.items()):
        raise ValueError('The LightGBM model or its input transformation differs from the reference')
    return files


def load_booster(path):
    from scripts.lightgbm_artifact import load_booster_bytes
    path = Path(path)
    return load_booster_bytes(path.read_bytes(), path)


def load_reference():
    files = reference_files()
    return load_booster(files['lightgbm.txt']), read_json(files['preprocessor.json'])


def predict_lightgbm(model, state, rows):
    columns = state['numeric_order'] + state['category_order']
    if model.feature_name() != columns:
        raise ValueError('LightGBM and transformer feature orders differ')
    values = np.asarray([[row[key] for key in columns] for row in (transform(row, state) for row in rows)])
    return model.predict(values).tolist()
