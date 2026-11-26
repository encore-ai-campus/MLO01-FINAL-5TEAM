"""Read one prepared sequence input and predict with a saved logistic model."""

import argparse
import json
from pathlib import Path

from .model import DEFAULT_MODEL_PATH, LogisticModel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model', type=Path, default=DEFAULT_MODEL_PATH)
    parser.add_argument('--input', type=Path, required=True)
    args = parser.parse_args()
    try:
        model = LogisticModel.load(args.model)
        row = json.loads(args.input.read_text(encoding='utf-8-sig'))
        print(json.dumps(model.predict(row), allow_nan=False))
    except (OSError, KeyError, TypeError, ValueError) as exc:
        parser.exit(2, f'Prediction failed: {exc}\n')


if __name__ == '__main__':
    main()
