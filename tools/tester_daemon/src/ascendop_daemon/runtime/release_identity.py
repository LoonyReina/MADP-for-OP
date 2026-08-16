from __future__ import annotations

import hashlib
import os
from pathlib import Path

from ascendop_daemon.control_plane.control_database import SCHEMA_VERSION


def source_generation(root: Path, *, policy_digest: str = "") -> str:
    override = os.environ.get("ASCENDOP_RELEASE_GENERATION", "").strip()
    if override:
        return override
    digest = hashlib.sha256()
    for source_root in (
        root / "packages" / "ascendop_protocol" / "src",
        root / "tools" / "tester_daemon" / "src",
    ):
        for path in sorted(source_root.rglob("*.py")):
            if "__pycache__" in path.parts or "legacy" in path.parts:
                continue
            digest.update(path.relative_to(root).as_posix().encode("utf-8"))
            digest.update(path.read_bytes())
    digest.update(str(SCHEMA_VERSION).encode("ascii"))
    digest.update(policy_digest.encode("ascii"))
    return digest.hexdigest()


def under_root(root: Path, value: Path) -> Path:
    resolved = value.resolve() if value.is_absolute() else (root / value).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"runtime path escapes workspace: {resolved}") from exc
    return resolved
