"""Load a replay ranker and reserve the first positions for observed interest."""

from hashlib import sha256
import json
import math

import lightgbm as lgb
import numpy as np

from .artifact_readiness import utc_second
from reco.core import FEATURES


_CART = FEATURES.index('session_carted')
_GAP = FEATURES.index('last_item_gap_log')
_POPULARITY = FEATURES.index('purchase_popularity_log')


def attach_context(state, event, query):
    current = state.metadata[event.item]
    extra = []
    for item in query['items']:
        other = state.metadata[item]
        missing = current['price'] is None or other['price'] is None
        distance = (0.0 if missing else
                    abs(math.log1p(max(0, current['price'])) - math.log1p(max(0, other['price']))))
        extra.append((bool(current['category']) and current['category'] == other['category'],
                      bool(current['brand']) and current['brand'] == other['brand'], distance, missing))
    query['extra_features'] = np.asarray(extra, dtype=np.float32).reshape(-1, 4)


def load_initial_ranker(config_file, as_of, receipt):
    config = json.loads(config_file.read_bytes())
    evaluation = config['evaluation']
    if evaluation['sample'] != receipt['sample']:
        raise ValueError('The ranker configuration belongs to a different replay sample')
    report = json.loads((config_file.parent / evaluation['report_file']).read_bytes())
    if report['manifest_sha256'] != receipt['manifest_sha256']:
        raise ValueError('The ranker was evaluated on a different replay input')
    input_digest = receipt.get('event_db_sha256')
    if input_digest is not None and report.get('event_db_sha256') not in (None, input_digest):
        raise ValueError('The ranker was evaluated on a different event DB SHA-256')
    validation_end = utc_second(report['early_stopping_window'][1])
    if validation_end >= as_of:
        raise ValueError('The end of ranker validation must be strictly before the request')
    ranking = config['ranking']
    digest = ranking['model_sha256']
    content = (config_file.parent / ranking['model_file']).read_bytes()
    if sha256(content).hexdigest() != digest or report['model_sha256'] != digest:
        raise ValueError('Ranker checksum mismatch')
    booster = lgb.Booster(model_str=content.decode('utf-8'))
    if booster.feature_name() != FEATURES:
        raise ValueError('Ranker feature order mismatch')
    return booster, (digest, validation_end)


def score_hybrid(booster, query):
    items, features = query['items'], query['features']
    if len(items) == 0:
        return np.empty(0, dtype=np.float32)

    extra = query['extra_features']
    prediction = booster.predict(features, num_threads=1)
    protected = (features[:, _CART] > 0) | (features[:, _GAP] >= 0)
    head = sorted(np.flatnonzero(protected), key=lambda i: (
        -features[i, _CART],
        features[i, _GAP] < 0,
        features[i, _GAP] if features[i, _GAP] >= 0 else 1e9,
        -extra[i, 0], extra[i, 3], extra[i, 2] > math.log(2), -extra[i, 1],
        -features[i, _POPULARITY], items[i],
    ))
    tail = sorted(np.flatnonzero(~protected), key=lambda i: (-float(prediction[i]), items[i]))
    scores = np.empty(len(items), dtype=np.float32)
    for rank, index in enumerate(head + tail):
        scores[index] = len(items) - rank
    return scores
