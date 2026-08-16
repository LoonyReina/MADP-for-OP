from __future__ import annotations

import hashlib
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
)


class GitBinaryMaterializationError(ValueError):
    pass


def enforce_binary_checkout(repo: Path) -> None:
    """Disable platform newline filters for protocol artifact worktrees."""

    repo = repo.resolve()
    if not (repo / ".git").exists():
        raise GitBinaryMaterializationError(f"Git worktree is missing: {repo}")
    if _binary_checkout_is_disabled(repo):
        return
    deadline = time.monotonic() + 5.0
    while True:
        completed = _run_git(repo, "config", "--local", "core.autocrlf", "false")
        if completed.returncode == 0 or _binary_checkout_is_disabled(repo):
            return
        detail = _command_error(completed)
        if "could not lock config file" not in detail.lower():
            raise GitBinaryMaterializationError(
                "unable to enforce binary-safe Git checkout: " + detail
            )
        if time.monotonic() >= deadline:
            raise GitBinaryMaterializationError(
                "unable to enforce binary-safe Git checkout: " + detail
            )
        time.sleep(0.05)


def _binary_checkout_is_disabled(repo: Path) -> bool:
    completed = _run_git(repo, "config", "--local", "--get", "core.autocrlf")
    return completed.returncode == 0 and str(completed.stdout).strip().lower() == "false"


def recover_manifest_parts_from_git(
    repo: Path,
    package_root: Path,
    payload: Mapping[str, Any],
    *,
    ref: str,
) -> tuple[str, ...]:
    """Restore only corrupt protocol parts from immutable Git blob bytes."""

    repo = repo.resolve()
    package_root = package_root.resolve()
    _require_bounded(repo, package_root, "payload package root")
    if not ref or ref.startswith("-"):
        raise GitBinaryMaterializationError(f"unsafe Git result ref: {ref}")
    repaired: list[str] = []
    parts = payload.get("parts", [])
    if not isinstance(parts, list):
        raise GitBinaryMaterializationError("payload parts must be a list")
    for expected_index, raw in enumerate(parts):
        if not isinstance(raw, Mapping):
            raise GitBinaryMaterializationError("payload part must be an object")
        if int(raw.get("index", -1)) != expected_index:
            raise GitBinaryMaterializationError(
                "payload part indexes are not contiguous"
            )
        relative = _safe_relative_path(str(raw.get("path") or ""))
        target = (package_root / relative).resolve()
        _require_bounded(package_root, target, "payload part")
        expected_size = int(raw.get("size_bytes", -1))
        expected_digest = str(raw.get("sha256") or "")
        if _matches(target, expected_size, expected_digest):
            continue
        repo_relative = target.relative_to(repo).as_posix()
        completed = _run_git(
            repo,
            "cat-file",
            "blob",
            f"{ref}:{repo_relative}",
            binary=True,
        )
        if completed.returncode != 0:
            raise GitBinaryMaterializationError(
                f"unable to recover payload part from Git: {repo_relative}: "
                + _command_error(completed)
            )
        content = bytes(completed.stdout)
        if len(content) != expected_size or _bytes_digest(content) != expected_digest:
            raise GitBinaryMaterializationError(
                f"Git blob does not match payload manifest: {repo_relative}"
            )
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with temporary.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        repaired.append(repo_relative)
    return tuple(repaired)


def _safe_relative_path(value: str) -> Path:
    candidate = Path(value.replace("\\", "/"))
    if not value or candidate.is_absolute() or ".." in candidate.parts:
        raise GitBinaryMaterializationError(f"unsafe payload part path: {value}")
    return candidate


def _require_bounded(root: Path, path: Path, label: str) -> None:
    if path != root and root not in path.parents:
        raise GitBinaryMaterializationError(f"{label} escapes Git worktree: {path}")


def _matches(path: Path, expected_size: int, expected_digest: str) -> bool:
    return (
        path.is_file()
        and path.stat().st_size == expected_size
        and _file_digest(path) == expected_digest
    )


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bytes_digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _run_git(
    repo: Path,
    *args: str,
    binary: bool = False,
) -> subprocess.CompletedProcess[Any]:
    return subprocess.run(
        ["git", "-c", f"safe.directory={repo.as_posix()}", *args],
        cwd=repo,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=not binary,
        encoding=None if binary else "utf-8",
        errors=None if binary else "replace",
        check=False,
        creationflags=process_creation_flags(),
        startupinfo=process_startupinfo(),
    )


def _command_error(completed: subprocess.CompletedProcess[Any]) -> str:
    value = completed.stderr or completed.stdout or b""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").strip()
    return str(value).strip()
