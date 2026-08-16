from __future__ import annotations

import hashlib
import os
import shutil
import tarfile
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

from ascendop_protocol.wire_v3 import PART_MAX_BYTES


class FlowV3PayloadError(RuntimeError):
    pass


def package_payload(
    payload_root: Path,
    package_root: Path,
    *,
    request_id: str,
    chunk_bytes: int = PART_MAX_BYTES,
) -> dict[str, Any]:
    payload_root = payload_root.resolve()
    package_root = package_root.resolve()
    if not payload_root.is_dir():
        raise FlowV3PayloadError(f"payload root is missing: {payload_root}")
    if chunk_bytes < 1024 or chunk_bytes > PART_MAX_BYTES:
        raise FlowV3PayloadError(
            f"chunk_bytes must be within 1024..{PART_MAX_BYTES}"
        )
    reject_symlinks(payload_root)
    destination = package_root / request_id
    destination.mkdir(parents=True, exist_ok=True)
    for stale_part in destination.glob("payload.part-*"):
        if stale_part.is_file():
            stale_part.unlink()
    archive = destination / "payload.tar"
    temporary = destination / ".payload.tar.tmp"
    if temporary.exists():
        temporary.unlink()
    with tarfile.open(temporary, mode="w", format=tarfile.USTAR_FORMAT) as handle:
        for path in sorted(payload_root.rglob("*"), key=lambda item: item.as_posix()):
            relative = path.relative_to(payload_root)
            if path.is_symlink():
                raise FlowV3PayloadError(f"payload cannot contain symlinks: {path}")
            info = handle.gettarinfo(str(path), arcname=relative.as_posix())
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            if path.is_file():
                with path.open("rb") as source:
                    handle.addfile(info, source)
            elif path.is_dir():
                handle.addfile(info)
    os.replace(temporary, archive)
    digest = file_sha256(archive)
    parts = split_file(archive, destination, chunk_bytes=chunk_bytes)
    archive.unlink()
    return {
        "digest": digest,
        "format": "ustar",
        "total_bytes": sum(int(part["size_bytes"]) for part in parts),
        "parts": [
            {
                **part,
                "path": part_path.relative_to(package_root).as_posix(),
            }
            for part in parts
            for part_path in [Path(str(part["path"]))]
        ],
    }


def split_file(
    source: Path,
    destination: Path,
    *,
    chunk_bytes: int,
) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    with source.open("rb") as handle:
        index = 0
        while True:
            path = destination / f"payload.part-{index:05d}"
            size, digest = copy_chunk(handle, path, chunk_bytes)
            if size == 0:
                path.unlink(missing_ok=True)
                break
            parts.append(
                {
                    "index": index,
                    "part_id": f"part-{index:05d}",
                    "path": str(path.resolve()),
                    "size_bytes": size,
                    "sha256": digest,
                }
            )
            index += 1
    if not parts:
        raise FlowV3PayloadError("payload archive unexpectedly contained no bytes")
    return parts


def copy_chunk(source: BinaryIO, destination: Path, limit: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with destination.open("wb") as output:
        while size < limit:
            data = source.read(min(1024 * 1024, limit - size))
            if not data:
                break
            output.write(data)
            digest.update(data)
            size += len(data)
        output.flush()
        os.fsync(output.fileno())
    return size, digest.hexdigest()


def materialize_payload(
    package_root: Path,
    payload: dict[str, Any],
    destination: Path,
) -> Path:
    package_root = package_root.resolve()
    destination = io_path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    expected_digest = str(payload.get("digest") or "")
    extract_root = destination / "payload"
    marker = destination / ".payload.sha256"
    if (
        extract_root.is_dir()
        and marker.is_file()
        and marker.read_text(encoding="ascii").strip() == expected_digest
    ):
        return extract_root

    archive = destination / ".a"
    digest = hashlib.sha256()
    with archive.open("wb") as output:
        for expected_index, part in enumerate(payload.get("parts", [])):
            if int(part.get("index", 0)) != expected_index:
                raise FlowV3PayloadError("payload part indexes are not contiguous")
            path = (package_root / str(part.get("path") or "")).resolve()
            if path != package_root and package_root not in path.parents:
                raise FlowV3PayloadError(f"payload part escapes package root: {path}")
            if not path.is_file():
                raise FlowV3PayloadError(f"payload part is missing: {path}")
            if path.stat().st_size != int(part.get("size_bytes", -1)):
                raise FlowV3PayloadError(f"payload part size changed: {path}")
            if file_sha256(path) != str(part.get("sha256") or ""):
                raise FlowV3PayloadError(f"payload part digest changed: {path}")
            with path.open("rb") as source:
                while True:
                    data = source.read(1024 * 1024)
                    if not data:
                        break
                    output.write(data)
                    digest.update(data)
        output.flush()
        os.fsync(output.fileno())
    if digest.hexdigest() != expected_digest:
        raise FlowV3PayloadError("reassembled payload digest mismatch")
    staging = destination / ".p"
    retired: list[Path] = []
    if staging.exists():
        retired.append(retire_tree(staging))
    staging.mkdir(parents=True)
    try:
        with tarfile.open(archive, mode="r:") as handle:
            safe_extract(handle, staging)
        marker.unlink(missing_ok=True)
        if extract_root.exists():
            retired.append(retire_tree(extract_root))
        os.replace(staging, extract_root)
        marker_temporary = destination / ".payload.sha256.tmp"
        marker_temporary.write_text(expected_digest + "\n", encoding="ascii")
        os.replace(marker_temporary, marker)
    finally:
        archive.unlink(missing_ok=True)
        if staging.exists():
            retired.append(retire_tree(staging))
        for stale in retired:
            remove_retired_tree(stale)
    return extract_root


def retire_tree(path: Path) -> Path:
    """Atomically move a derived tree aside before publishing its replacement."""

    retired = path.with_name(f".{path.name}.stale-{uuid.uuid4().hex[:12]}")
    os.replace(path, retired)
    return retired


def remove_retired_tree(path: Path) -> None:
    """Best-effort cleanup that tolerates vanished children and Windows long paths."""

    def tolerate_missing(
        _function: Any,
        _path: str,
        error: tuple[type[BaseException], BaseException, Any],
    ) -> None:
        if isinstance(error[1], FileNotFoundError):
            return
        raise error[1]

    target = str(path.resolve())
    if os.name == "nt" and not target.startswith("\\\\?\\"):
        target = "\\\\?\\" + target
    try:
        shutil.rmtree(target, onerror=tolerate_missing)
    except FileNotFoundError:
        return
    except OSError:
        # The active payload has already been atomically published. A later
        # maintenance sweep may remove an inaccessible derived stale tree.
        return


def io_path(path: Path) -> Path:
    """Return an absolute path suitable for deep local artifact I/O."""

    resolved = path.resolve()
    value = str(resolved)
    if os.name == "nt" and not value.startswith("\\\\?\\"):
        return Path("\\\\?\\" + value)
    return resolved


def safe_extract(handle: tarfile.TarFile, destination: Path) -> None:
    destination = destination.resolve()
    members = handle.getmembers()
    for member in members:
        relative = PurePosixPath(member.name)
        if relative.is_absolute() or any(part == ".." for part in relative.parts):
            raise FlowV3PayloadError(
                f"payload archive member escapes destination: {member.name}"
            )
        target = destination.joinpath(*relative.parts).resolve()
        if target != destination and destination not in target.parents:
            raise FlowV3PayloadError(
                f"payload archive member escapes destination: {member.name}"
            )
        if member.issym() or member.islnk():
            raise FlowV3PayloadError(
                f"payload archive links are forbidden: {member.name}"
            )
        if not member.isdir() and not member.isfile():
            raise FlowV3PayloadError(
                f"payload archive member type is forbidden: {member.name}"
            )
    for member in members:
        relative = PurePosixPath(member.name)
        target = destination.joinpath(*relative.parts)
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        source = handle.extractfile(member)
        if source is None:
            raise FlowV3PayloadError(
                f"payload archive file cannot be read: {member.name}"
            )
        with source, target.open("wb") as output:
            shutil.copyfileobj(source, output, length=1024 * 1024)


def reject_symlinks(root: Path) -> None:
    if root.is_symlink():
        raise FlowV3PayloadError(f"payload root cannot be a symlink: {root}")
    for path in root.rglob("*"):
        if path.is_symlink():
            raise FlowV3PayloadError(f"payload cannot contain symlinks: {path}")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            data = handle.read(1024 * 1024)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()
