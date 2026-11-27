"""Save and check deployable LightGBM models without translating newlines."""
import re
from pathlib import Path


def validate_tree_offsets(content: bytes, path: Path) -> None:
    error = f'Invalid LightGBM tree offsets: {path.name}'
    header, marker, _ = content.partition(b'\nTree=0')
    sizes = re.search(rb'(?m)^tree_sizes=([0-9 ]+)\r?$', header)
    if not marker or not sizes or not sizes[1].split():
        raise ValueError(error)

    # LightGBM stores byte lengths. A text-mode copy can move the next tree
    # without changing the declared lengths, causing a native parser failure.
    offset = len(header) + 1
    for index, size in enumerate(map(int, sizes[1].split())):
        tree = f'Tree={index}'.encode('ascii')
        if size <= 0 or not content.startswith((tree + b'\n', tree + b'\r\n'), offset):
            raise ValueError(error)
        offset += size
    if not content.startswith((b'end of trees\n', b'end of trees\r\n'), offset):
        raise ValueError(error)


def load_booster_bytes(content: bytes, path: Path):
    from lightgbm import Booster

    validate_tree_offsets(content, path)
    return Booster(model_str=content.decode('utf-8'))
