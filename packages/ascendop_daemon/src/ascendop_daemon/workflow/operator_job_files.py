from __future__ import annotations

import hashlib
import os
import re
import shutil
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.operator_job_errors import EngineJobBuildError


MAX_ENGINE_BUILD_DIR_NAME = 56


def copy_tree_without_symlinks(
    source: Path,
    destination: Path,
    *,
    exclude_names: tuple[str, ...] = (),
) -> None:
    for path in source.rglob("*"):
        if path.is_symlink():
            raise EngineJobBuildError(f"engine payload cannot contain symlinks: {path}")
    shutil.copytree(
        filesystem_path(source),
        filesystem_path(destination),
        ignore=shutil.ignore_patterns(
            "__pycache__", "*.pyc", "*.pyo", *exclude_names
        ),
    )


def engine_build_dir_name(engine_job_id: str) -> str:
    if len(engine_job_id) <= MAX_ENGINE_BUILD_DIR_NAME:
        return engine_job_id
    digest = hashlib.sha256(engine_job_id.encode("utf-8")).hexdigest()[:16]
    prefix_length = MAX_ENGINE_BUILD_DIR_NAME - len(digest) - 1
    prefix = engine_job_id[:prefix_length].rstrip("-_")
    return f"{prefix}-{digest}"


def filesystem_path(path: Path) -> Path:
    resolved = str(path.resolve())
    if os.name != "nt" or resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved[2:])
    return Path("\\\\?\\" + resolved)


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if path.is_file():
            update_canonical_file_digest(digest, path)
            digest.update(b"\0")
    return digest.hexdigest()


def update_canonical_file_digest(digest: Any, path: Path) -> None:
    """Hash transport-stable bytes across Git CRLF/LF checkout policies."""
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            data = carry + chunk
            carry = b"\r" if data.endswith(b"\r") else b""
            if carry:
                data = data[:-1]
            digest.update(data.replace(b"\r\n", b"\n"))
    if carry:
        digest.update(carry)


def option_value(argv: list[str], name: str, default: str) -> str:
    try:
        return argv[argv.index(name) + 1]
    except (ValueError, IndexError):
        return default


def strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def safe_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    if not token:
        raise EngineJobBuildError(f"invalid engine token: {value}")
    return token


def release_name(test_version: str) -> str:
    parts = test_version.split("_")
    return "_".join(parts[:2]) if len(parts) >= 2 else test_version


def gitpartner_vendor(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()
    return clean if clean.endswith("_gitpartner") else clean + "_gitpartner"
