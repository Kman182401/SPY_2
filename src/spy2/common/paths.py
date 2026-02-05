from __future__ import annotations

import os
from pathlib import Path


def repo_root(start: Path | None = None) -> Path:
    if start is None:
        start = Path.cwd()
    for path in [start, *start.parents]:
        if (path / "pyproject.toml").is_file():
            return path
    return start


def resolve_root(root: Path | None = None) -> Path:
    if root is not None:
        return Path(root).expanduser().resolve()
    env_root = os.getenv("SPY2_DATA_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return repo_root()
