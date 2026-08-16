from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Iterable

from ascendop_daemon.control_plane.control_database import SCHEMA_VERSION


def source_generation(root: Path, *, policy_digest: str = "") -> str:
    override = os.environ.get("ASCENDOP_RELEASE_GENERATION", "").strip()
    if override:
        return override
    digest = hashlib.sha256()
    for source_root in (
        root / "packages" / "ascendop_protocol" / "src",
        root / "packages" / "ascendop_control" / "src",
        root / "packages" / "ascendop_agent_runner" / "src",
        root / "tools" / "tester_daemon" / "src",
        root / "tools" / "official_eval_daemon" / "official_eval",
    ):
        for path in _runtime_files(source_root):
            _update_digest(digest, root, path)
    workflow_adapter = root / "scripts" / "next_workflow.py"
    if workflow_adapter.is_file():
        _update_digest(digest, root, workflow_adapter)
    digest.update(str(SCHEMA_VERSION).encode("ascii"))
    digest.update(policy_digest.encode("ascii"))
    return digest.hexdigest()


def _runtime_files(source_root: Path) -> Iterable[Path]:
    if not source_root.is_dir():
        return ()
    files: list[Path] = []
    for path in source_root.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        if "__pycache__" in path.parts or "legacy" in path.parts:
            continue
        if path.suffix in {".pyc", ".pyo"}:
            continue
        files.append(path)
    return tuple(sorted(files, key=lambda item: item.as_posix()))


def _update_digest(digest: object, root: Path, path: Path) -> None:
    update = getattr(digest, "update")
    update(path.relative_to(root).as_posix().encode("utf-8"))
    update(path.read_bytes().replace(b"\r\n", b"\n"))


def under_root(root: Path, value: Path) -> Path:
    resolved = value.resolve() if value.is_absolute() else (root / value).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"runtime path escapes workspace: {resolved}") from exc
    return resolved
