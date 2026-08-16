from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import ascendop_protocol


def shared_protocol_source(root: Path) -> Path:
    source = root.resolve() / "packages" / "ascendop_protocol" / "src"
    if (source / "ascendop_protocol").is_dir():
        return source
    installed = Path(ascendop_protocol.__file__).resolve().parent.parent
    if (installed / "ascendop_protocol").is_dir():
        return installed
    raise ValueError(f"shared protocol package source is missing: {source}")


def prepend_pythonpath(
    environment: dict[str, str],
    roots: Iterable[Path | str],
) -> dict[str, str]:
    existing = environment.get("PYTHONPATH", "")
    values = [str(Path(root).resolve()) for root in roots]
    if existing:
        values.append(existing)
    environment["PYTHONPATH"] = os.pathsep.join(values)
    return environment
