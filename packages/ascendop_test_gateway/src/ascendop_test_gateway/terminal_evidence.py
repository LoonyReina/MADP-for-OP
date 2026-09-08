"""Local return retention before a remote ACK can permit garbage collection.

This is Gateway evidence, not a MADP business receipt. Managed users must also
commit their own continuation before authorizing the independent ACK worker.
File fsync + replace protects process-crash recovery. POSIX directories are
synced too; Windows power-loss guarantees are not claimed by this module.
"""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import Any, Mapping

from .contracts import canonical_artifact_root

SCHEMA = "ascendop.gateway-terminal-retention.v1"


from ascendop_protocol.filesystem import sync_directory


def retain_terminal_evidence(run_dir: Path, event: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and sync the complete local payload; return its immutable index."""
    return _index(run_dir, event, sync=True)


def validate_terminal_evidence(
    run_dir: Path,
    event: Mapping[str, Any],
    retained: Mapping[str, Any],
) -> None:
    if dict(retained) != _index(run_dir, event, sync=False):
        raise ValueError("retained terminal evidence changed")


def read_retained_file(run_dir: Path, retained: Mapping[str, Any], path: Path) -> bytes:
    """Verify one consumed artifact against the accepted index, not all logs."""
    run_dir = run_dir.resolve()
    raw = path.absolute()
    resolved = raw.resolve()
    if run_dir / "results" not in resolved.parents:
        raise ValueError("retained artifact escaped request results")
    for parent in (raw, *raw.parents):
        if parent == run_dir:
            break
        if parent.is_symlink() or getattr(parent, "is_junction", lambda: False)():
            raise ValueError("linked retained artifact is forbidden")
    relative = resolved.relative_to(run_dir).as_posix()
    entries = [entry for entry in retained["files"] if entry["path"] == relative]
    if retained.get("schema") != SCHEMA or len(entries) != 1:
        raise ValueError("consumed artifact is not uniquely retained")
    if not stat.S_ISREG(resolved.stat().st_mode):
        raise ValueError("consumed artifact is not a regular file")
    data = resolved.read_bytes()
    if (len(data) != entries[0]["size"]
            or hashlib.sha256(data).hexdigest() != entries[0]["sha256"]):
        raise ValueError("consumed terminal artifact changed")
    return data


def _index(run_dir: Path, event: Mapping[str, Any], *, sync: bool) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    raw = Path(canonical_artifact_root(str(event.get("artifact_root") or "")))
    root = raw.resolve()
    if not raw.is_absolute() or run_dir / "results" not in root.parents:
        raise ValueError(
            "terminal evidence must be inside the request results directory"
        )
    for parent in (raw, *raw.parents):
        if parent == run_dir:
            break
        if parent.is_symlink() or getattr(parent, "is_junction", lambda: False)():
            raise ValueError("linked terminal evidence is forbidden")
    if not root.is_dir() or not (root / "result_bundle").is_dir():
        raise ValueError("terminal evidence result bundle is missing")
    for name in ("terminal.json", "state.json", "artifact_manifest.json"):
        if not (root / name).is_file():
            raise ValueError(f"terminal evidence is missing {name}")
    marker = root.parent / ".payload.sha256"
    if (
        not marker.is_file()
        or marker.read_text(encoding="ascii").strip().lower()
        != event["result_payload_sha256"]
    ):
        raise ValueError("terminal evidence payload digest marker mismatch")
    files, directories = [], [root, root.parent]
    for path in sorted([*root.rglob("*"), marker]):
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            raise ValueError("linked terminal evidence is forbidden")
        if path.is_dir():
            directories.append(path)
            continue
        if not stat.S_ISREG(path.stat().st_mode):
            raise ValueError("non-regular terminal evidence is forbidden")
        # Windows CRT fsync requires a writable handle. No bytes are modified;
        # an unsyncable/read-only file keeps the return unacknowledged.
        with path.open("r+b" if sync and os.name == "nt" else "rb") as handle:
            before = os.fstat(handle.fileno())
            hasher = hashlib.sha256()
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(block)
            digest = hasher.hexdigest()
            if sync:
                os.fsync(handle.fileno())
            after = os.fstat(handle.fileno())
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("terminal evidence changed while retaining it")
        files.append(
            {
                "path": path.relative_to(run_dir).as_posix(),
                "size": after.st_size,
                "sha256": digest,
            }
        )
    if sync:
        directories.extend(
            parent
            for parent in root.parents
            if parent == run_dir or run_dir in parent.parents
        )
        for directory in sorted(
            set(directories), key=lambda path: len(path.parts), reverse=True
        ):
            sync_directory(directory)
    return {"schema": SCHEMA, "event_id": event["event_id"], "files": files}
