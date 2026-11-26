"""Fit purchase input transformations on confirmed October training rows."""

import argparse
import csv
import hashlib
import json
import math
import statistics
import struct
from array import array
from pathlib import Path
from ml.conversion_model.storage import staging_directory
from ml.conversion_model.models.logistic.features import utc_datetime

NUMERIC = ('view_count', 'cart_count', 'distinct_product_count', 'visit_elapsed_seconds', 'previous_event_gap_seconds', 'price')
FLAGS = ('is_cart', 'price_missing')
ZERO_PRICE_AS_MISSING = 'zero_is_missing'
CATEGORIES = ('brand', 'category_id')
REPEAT_INPUT_VERSION = 'purchase-input-v2-repeat-view'
REPEAT_VIEW = 'current_product_repeat_view_count'
OCTOBER_TRAINING_PERIOD = ('2019-10-01T00:00:00Z', '2019-10-21T00:00:00Z')


def numeric_columns(state):
    order = tuple(state['numeric_order'])
    version = state['version']
    numeric = {'purchase-preprocessor-v1': NUMERIC,
               'purchase-preprocessor-v2': NUMERIC + (REPEAT_VIEW,)}.get(version)
    if numeric is None or order != numeric + FLAGS or tuple(state['category_order']) != CATEGORIES:
        raise ValueError('Unsupported preprocessor version or input order')
    return numeric


def extend_state(base, train):
    """Copy the ten-input state and fit only the extra scale on the extended training file."""
    if numeric_columns(base) != NUMERIC:
        raise ValueError('Expected the original ten-input preprocessor')
    rows = read_rows(train)
    if not rows or len(rows) != base['training_rows'] or any(
        row['split'] != 'train' or row['label_status'] != 'confirmed'
        or not OCTOBER_TRAINING_PERIOD[0] <= row['event_time_utc']
        < row['label_available_at_utc'] < OCTOBER_TRAINING_PERIOD[1]
        for row in rows
    ):
        raise ValueError('Expected the same confirmed October training split')
    values = []
    for row in rows:
        validate(row, NUMERIC + (REPEAT_VIEW,))
        values.append(math.log1p(int(row[REPEAT_VIEW])))
    std = statistics.pstdev(values)
    return {
        **base, 'version': 'purchase-preprocessor-v2', 'numeric_order': list(NUMERIC + (REPEAT_VIEW,) + FLAGS),
        'base_training_sha256': base['training_sha256'],
        'training_sha256': hashlib.sha256(train.read_bytes()).hexdigest(),
        'scales': {**base['scales'], REPEAT_VIEW: {'mean': statistics.mean(values), 'std': std, 'divisor': std or 1.0}},
    }


def read_rows(path):
    with path.open(encoding='utf-8', newline='') as file:
        return list(csv.DictReader(file))


def validate(row, numeric=NUMERIC):
    missing = [key for key in (*numeric, *FLAGS, *CATEGORIES) if key not in row]
    if missing:
        raise ValueError(f'Missing LightGBM inputs: {", ".join(missing)}')
    for key in numeric:
        if key == 'price' and row[key] in ('', None):
            continue
        value = float(row[key])
        if not math.isfinite(value) or value < 0:
            raise ValueError(f'Invalid {key}')
        if key.endswith('count') and not value.is_integer():
            raise ValueError(f'Non-integer {key}')
    for key in FLAGS:
        if str(row[key]) not in ('0', '1'):
            raise ValueError(f'Invalid {key}')
    if int(row['price_missing']) != int(row['price'] in ('', None)):
        raise ValueError('Price and missing flag disagree')


def model_price(raw, *, zero_is_missing):
    if raw in ('', None):
        return None
    price = float(raw)
    return None if zero_is_missing and price == 0 else price


def category(value):
    return '' if value is None else str(value).strip()


def fit(path, *, training_period=OCTOBER_TRAINING_PERIOD, include_repeat=False):
    return fit_rows(read_rows(path), training_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    training_period=training_period, include_repeat=include_repeat)


def fit_rows(rows, *, training_sha256, training_period=OCTOBER_TRAINING_PERIOD, include_repeat=False,
             team_window=False):
    start, end = training_period
    if team_window:
        start, end = map(utc_datetime, training_period)
    elif not OCTOBER_TRAINING_PERIOD[0] <= start < end <= OCTOBER_TRAINING_PERIOD[1]:
        raise ValueError('The training period must be inside October 1-20 UTC')
    if not rows:
        raise ValueError('Empty training data')
    numeric = NUMERIC + (REPEAT_VIEW,) if include_repeat else NUMERIC
    # Collect compact numeric columns in one pass over the source. Keep price
    # values in input order for the same exact median, including ties.
    values = {key: array('d') for key in numeric}
    categories = {key: set() for key in CATEGORIES}
    category_missing = dict.fromkeys(CATEGORIES, 0)
    price_missing = False
    for row in rows:
        event_time, available = (row['event_time_utc'], row['label_available_at_utc'])
        if team_window:
            event_time, available = map(utc_datetime, (event_time, available))
        if (row['split'] != 'train' or row['label_status'] != 'confirmed'
                or str(row['label']) not in ('0', '1')
                or not start <= event_time < available < end):
            raise ValueError('Expected confirmed data from the training period')
        validate(row, numeric)
        for key, column in values.items():
            raw = row[key]
            if key == 'price':
                price = model_price(raw, zero_is_missing=True)
                column.append(math.nan if price is None else price)
                price_missing |= price is None
            else:
                column.append(math.log1p(float(raw)))
        for key in CATEGORIES:
            value = category(row[key])
            if value:
                categories[key].add(value)
            else:
                category_missing[key] += 1

    prices = values['price']
    present = [value for value in prices if not math.isnan(value)]
    if not present:
        raise ValueError('No valid training prices')
    median = statistics.median(present)
    del present
    values['price'] = array('d', (math.log1p(median if math.isnan(value) else value) for value in prices))
    del prices
    scales = {}
    for key, column in values.items():
        std = statistics.pstdev(column)
        scales[key] = {'mean': statistics.mean(column), 'std': std, 'divisor': std or 1.0}
    maps = {key: {value: index + 2 for index, value in enumerate(sorted(known))}
            for key, known in categories.items()}
    return {
        'version': 'purchase-preprocessor-v2' if include_repeat else 'purchase-preprocessor-v1',
        'training_rows': len(rows),
        'training_sha256': training_sha256,
        'training_period_utc': list(training_period),
        'numeric_order': list(numeric + FLAGS), 'category_order': list(CATEGORIES),
        'price_policy': ZERO_PRICE_AS_MISSING,
        'price_median': median, 'scales': scales, 'category_maps': maps,
        'reserved_ids': {'missing': 0, 'unknown': 1},
        'price_missing_enabled': price_missing,
        'category_missing_counts': category_missing,
    }


def transform(row, state):
    numeric = numeric_columns(state)
    validate(row, numeric)
    policy = state.get('price_policy')
    if policy not in (None, ZERO_PRICE_AS_MISSING):
        raise ValueError('Unsupported price policy')
    price = model_price(row['price'], zero_is_missing=policy == ZERO_PRICE_AS_MISSING)
    result = {}
    for key in numeric:
        if key == 'price':
            value = state['price_median'] if price is None else price
        else:
            value = float(row[key])
        scale = state['scales'][key]
        value = (math.log1p(value) - scale['mean']) / scale['divisor']
        value = struct.unpack('f', struct.pack('f', value))[0]
        if not math.isfinite(value):
            raise ValueError(f'Non-finite transformed {key}')
        result[key] = value
    result['is_cart'] = float(row['is_cart'])
    result['price_missing'] = float(price is None and state['price_missing_enabled'])
    for key in CATEGORIES:
        value = category(row[key])
        result[key] = state['category_maps'][key].get(value, 1) if value else 0
    return result


def tabular_matrix(rows, state):
    import numpy as np

    keys = state['numeric_order'] + state['category_order']
    values = (tuple(converted[k] for k in keys) for converted in (transform(row, state) for row in rows))
    dtype = np.dtype((np.float64, (len(keys),)))
    return np.fromiter(values, dtype=dtype, count=len(rows))


def run(train, validation, output, *, base_state=None, training_period=OCTOBER_TRAINING_PERIOD, include_repeat=False):
    if output.exists():
        raise FileExistsError(output)
    state = (fit(train, training_period=training_period, include_repeat=include_repeat)
             if base_state is None else extend_state(base_state, train))
    serialized = json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2)
    loaded = json.loads(serialized)
    report = {'training_rows': state['training_rows'], 'price_missing_enabled': state['price_missing_enabled'], 'splits': {}}
    output.parent.mkdir(parents=True, exist_ok=True)
    with staging_directory(output.parent) as temp:
        result = Path(temp) / 'result'
        result.mkdir()
        (result / 'preprocessor.json').write_text(serialized + '\n', encoding='utf-8')
        for name, path in (('train', train), ('validation', validation)):
            rows = read_rows(path)
            unknown = {key: 0 for key in CATEGORIES}
            with (result / f'{name}_transformed.csv').open('w', encoding='utf-8', newline='') as file:
                columns = ['event_id', 'visit_id', 'label', 'raw_price_missing', *state['numeric_order'], *CATEGORIES]
                writer = csv.DictWriter(file, fieldnames=columns)
                writer.writeheader()
                for row in rows:
                    transformed = transform(row, loaded)
                    if transformed != transform(row, state):
                        raise AssertionError('Reload changed transformation')
                    for key in CATEGORIES:
                        unknown[key] += transformed[key] == 1
                    writer.writerow({**transformed, 'event_id': row['event_id'], 'visit_id': row['visit_id'], 'label': row['label'], 'raw_price_missing': row['price_missing']})
            report['splits'][name] = {'rows': len(rows), 'unknown_categories': unknown}
        if loaded != state:
            raise AssertionError('Transformation modified fitted state')
        report['reload_and_unchanged_state_verified'] = True
        (result / 'verification.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        result.rename(output)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--train', type=Path, required=True)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.train, args.validation, args.output), indent=2))
