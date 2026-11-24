"""Session-purchase recommendation. Workspace-local dependencies are optional."""
import sys
from pathlib import Path

deps = Path(__file__).resolve().parents[2] / '.deps'
if deps.exists():
    sys.path.insert(0, str(deps))
