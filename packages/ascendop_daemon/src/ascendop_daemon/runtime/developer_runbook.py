from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ascendop_daemon.runtime.release_identity import under_root


class DeveloperRunbookIdentityError(RuntimeError):
    pass


@dataclass(frozen=True)
class DeveloperRunbookRuntime:
    path: Path
    relative_path: Path
    sha256: str


def resolve_developer_runbook(
    root: Path,
    configured_path: Path,
) -> DeveloperRunbookRuntime:
    root = root.resolve()
    active_path = root / ".ascendop-work" / "runtime" / "active-release.json"
    if active_path.is_file():
        active = _read_object(active_path)
        if str(active.get("schema") or "") != "ascendop.active-release.v4":
            raise DeveloperRunbookIdentityError(
                "active Flow V4 release has an unsupported schema"
            )
        path = Path(str(active.get("developer_runbook_path") or ""))
        expected = _digest(
            str(active.get("developer_runbook_sha256") or ""),
            "Developer runbook SHA-256",
        )
        if not path.is_absolute():
            raise DeveloperRunbookIdentityError(
                "active Developer runbook path must be absolute"
            )
        path = path.resolve()
        runtime_root = (root / ".ascendop-work" / "runtime").resolve()
        if path == runtime_root or runtime_root not in path.parents:
            raise DeveloperRunbookIdentityError(
                f"Developer runbook is outside the immutable runtime root: {path}"
            )
    else:
        try:
            path = under_root(root, configured_path)
        except ValueError as exc:
            raise DeveloperRunbookIdentityError(str(exc)) from exc
        expected = _file_digest(path) if path.is_file() else ""
    if not path.is_file() or path.is_symlink():
        raise DeveloperRunbookIdentityError(f"Developer runbook is missing: {path}")
    actual = _file_digest(path)
    if actual != expected:
        raise DeveloperRunbookIdentityError(
            "Developer runbook digest mismatch: "
            f"expected={expected} actual={actual}"
        )
    return DeveloperRunbookRuntime(
        path=path,
        relative_path=path.relative_to(root),
        sha256=actual,
    )


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DeveloperRunbookIdentityError(
            f"active Flow V4 release is unavailable: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise DeveloperRunbookIdentityError(
            "active Flow V4 release must be a JSON object"
        )
    return value


def _digest(value: str, label: str) -> str:
    if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise DeveloperRunbookIdentityError(f"invalid {label}: {value}")
    return value


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
