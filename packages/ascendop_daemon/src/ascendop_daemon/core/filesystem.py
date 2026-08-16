from __future__ import annotations

import os
from pathlib import Path


def filesystem_path(path: Path) -> Path:
    """Return a path suitable for deep filesystem I/O on Windows."""

    resolved = str(path.resolve())
    if os.name != "nt" or resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved[2:])
    return Path("\\\\?\\" + resolved)
