from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO


PROTOCOL_VERSION = "gitpartner-engine-payload-archive-v1"
MANIFEST_NAME = ".gitpartner_payload_archive.json"
CHUNK_DIR_NAME = ".gitpartner_payload_chunks"
DEFAULT_CHUNK_BYTES = 960 * 1024
MAX_CHUNK_COUNT = 8192
MAX_ARCHIVE_BYTES = 512 * 1024 * 1024
MAX_EXPANDED_BYTES = 4 * 1024 * 1024 * 1024
MAX_MEMBER_COUNT = 100_000


class PayloadArchiveError(RuntimeError):
    pass


def stage_payload_tree(
    source: Path,
    destination: Path,
    *,
    max_file_bytes: int,
) -> bool:
    """Copy a payload, archiving it only when a file exceeds the transport cap."""

    source = source.resolve()
    if not source.is_dir():
        raise PayloadArchiveError(f"payload root must be a directory: {source}")
    _validate_source_tree(source)
    if not _requires_archive(source, max_file_bytes=max_file_bytes):
        shutil.copytree(source, destination)
        return False
    if (source / MANIFEST_NAME).exists() or (source / CHUNK_DIR_NAME).exists():
        raise PayloadArchiveError(
            "payload uses reserved archive transport names: "
            f"{MANIFEST_NAME}, {CHUNK_DIR_NAME}"
        )

    destination.mkdir(parents=True)
    chunks_dir = destination / CHUNK_DIR_NAME
    chunks_dir.mkdir()
    archive_path = _temporary_path(destination.parent, ".payload-", ".tar.gz")
    try:
        _write_deterministic_archive(source, archive_path)
        archive_size = archive_path.stat().st_size
        if archive_size > MAX_ARCHIVE_BYTES:
            raise PayloadArchiveError(
                f"compressed payload exceeds {MAX_ARCHIVE_BYTES} bytes: {archive_size}"
            )
        chunk_bytes = min(DEFAULT_CHUNK_BYTES, max(1, int(max_file_bytes)))
        chunks = _split_archive(
            archive_path,
            chunks_dir,
            chunk_bytes=chunk_bytes,
        )
        manifest = {
            "protocol_version": PROTOCOL_VERSION,
            "archive_format": "tar+gzip",
            "archive_sha256": _file_sha256(archive_path),
            "archive_size_bytes": archive_size,
            "chunk_size_bytes": chunk_bytes,
            "chunks": chunks,
            "source_file_count": sum(1 for path in source.rglob("*") if path.is_file()),
            "source_size_bytes": sum(
                path.stat().st_size for path in source.rglob("*") if path.is_file()
            ),
        }
        (destination / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if (destination / MANIFEST_NAME).stat().st_size > max_file_bytes:
            raise PayloadArchiveError("payload archive manifest exceeds transport cap")
        return True
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    finally:
        archive_path.unlink(missing_ok=True)


def materialize_payload_tree(source: Path, destination: Path) -> bool:
    """Copy a normal payload or safely reconstruct an archived Engine payload."""

    source = source.resolve()
    if not source.is_dir():
        raise PayloadArchiveError(f"payload root must be a directory: {source}")
    manifest_path = source / MANIFEST_NAME
    if not manifest_path.is_file():
        _validate_source_tree(source)
        shutil.copytree(source, destination)
        return False

    manifest = _read_manifest(manifest_path)
    _validate_archive_layout(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    archive_path = _temporary_path(destination.parent, ".payload-", ".tar.gz")
    materialized = Path(
        tempfile.mkdtemp(prefix=".payload-materialized-", dir=str(destination.parent))
    )
    try:
        _join_chunks(source, manifest, archive_path)
        _extract_archive(archive_path, materialized)
        os.replace(materialized, destination)
        return True
    except Exception:
        shutil.rmtree(materialized, ignore_errors=True)
        raise
    finally:
        archive_path.unlink(missing_ok=True)


def _validate_source_tree(source: Path) -> None:
    for path in source.rglob("*"):
        if path.is_symlink():
            raise PayloadArchiveError(f"payload cannot contain symlinks: {path}")


def _requires_archive(source: Path, *, max_file_bytes: int) -> bool:
    return any(
        path.is_file() and path.stat().st_size > max_file_bytes
        for path in source.rglob("*")
    )


def _write_deterministic_archive(source: Path, archive_path: Path) -> None:
    with archive_path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.PAX_FORMAT,
            ) as archive:
                for path in sorted(source.rglob("*"), key=lambda item: item.as_posix()):
                    relative = path.relative_to(source).as_posix()
                    info = archive.gettarinfo(str(path), arcname=relative)
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mtime = 0
                    info.pax_headers = {}
                    if path.is_file():
                        with path.open("rb") as handle:
                            archive.addfile(info, handle)
                    elif path.is_dir():
                        archive.addfile(info)
                    else:
                        raise PayloadArchiveError(
                            f"unsupported payload filesystem entry: {path}"
                        )


def _split_archive(
    archive_path: Path,
    chunks_dir: Path,
    *,
    chunk_bytes: int,
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    with archive_path.open("rb") as source:
        for index in range(MAX_CHUNK_COUNT + 1):
            data = source.read(chunk_bytes)
            if not data:
                break
            if index >= MAX_CHUNK_COUNT:
                raise PayloadArchiveError(
                    f"payload archive requires more than {MAX_CHUNK_COUNT} chunks"
                )
            name = f"part-{index:06d}.bin"
            (chunks_dir / name).write_bytes(data)
            chunks.append(
                {
                    "name": name,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "size_bytes": len(data),
                }
            )
    if not chunks:
        raise PayloadArchiveError("payload archive produced no chunks")
    return chunks


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PayloadArchiveError(f"invalid payload archive manifest: {path}") from exc
    if not isinstance(raw, dict) or raw.get("protocol_version") != PROTOCOL_VERSION:
        raise PayloadArchiveError("unsupported payload archive protocol")
    chunks = raw.get("chunks")
    if not isinstance(chunks, list) or not chunks or len(chunks) > MAX_CHUNK_COUNT:
        raise PayloadArchiveError("payload archive chunk list is invalid")
    return raw


def _validate_archive_layout(source: Path) -> None:
    allowed = {MANIFEST_NAME, CHUNK_DIR_NAME}
    unexpected = sorted(path.name for path in source.iterdir() if path.name not in allowed)
    if unexpected:
        raise PayloadArchiveError(
            f"archived payload contains unexpected top-level entries: {unexpected}"
        )
    chunks_dir = source / CHUNK_DIR_NAME
    if not chunks_dir.is_dir() or chunks_dir.is_symlink():
        raise PayloadArchiveError("payload archive chunk directory is missing or unsafe")
    _validate_source_tree(source)


def _join_chunks(source: Path, manifest: dict[str, Any], archive_path: Path) -> None:
    expected_size = _bounded_int(
        manifest.get("archive_size_bytes"),
        "archive_size_bytes",
        minimum=1,
        maximum=MAX_ARCHIVE_BYTES,
    )
    expected_digest = _sha256_text(manifest.get("archive_sha256"), "archive_sha256")
    chunks = manifest["chunks"]
    digest = hashlib.sha256()
    observed_size = 0
    with archive_path.open("wb") as output:
        for index, raw in enumerate(chunks):
            if not isinstance(raw, dict):
                raise PayloadArchiveError(f"payload archive chunk {index} is invalid")
            expected_name = f"part-{index:06d}.bin"
            if raw.get("name") != expected_name:
                raise PayloadArchiveError(
                    f"payload archive chunk order is invalid at index {index}"
                )
            expected_chunk_size = _bounded_int(
                raw.get("size_bytes"),
                f"chunks[{index}].size_bytes",
                minimum=1,
                maximum=DEFAULT_CHUNK_BYTES,
            )
            expected_chunk_digest = _sha256_text(
                raw.get("sha256"),
                f"chunks[{index}].sha256",
            )
            path = source / CHUNK_DIR_NAME / expected_name
            if not path.is_file() or path.is_symlink():
                raise PayloadArchiveError(f"payload archive chunk is missing: {path}")
            data = path.read_bytes()
            if len(data) != expected_chunk_size:
                raise PayloadArchiveError(
                    f"payload archive chunk size mismatch: {expected_name}"
                )
            if hashlib.sha256(data).hexdigest() != expected_chunk_digest:
                raise PayloadArchiveError(
                    f"payload archive chunk digest mismatch: {expected_name}"
                )
            output.write(data)
            digest.update(data)
            observed_size += len(data)
    if observed_size != expected_size:
        raise PayloadArchiveError("payload archive size mismatch")
    if digest.hexdigest() != expected_digest:
        raise PayloadArchiveError("payload archive digest mismatch")


def _extract_archive(archive_path: Path, destination: Path) -> None:
    expanded_size = 0
    member_count = 0
    with tarfile.open(archive_path, mode="r:gz") as archive:
        for member in archive:
            member_count += 1
            if member_count > MAX_MEMBER_COUNT:
                raise PayloadArchiveError(
                    f"payload archive exceeds {MAX_MEMBER_COUNT} members"
                )
            relative = _safe_member_path(member.name)
            target = destination.joinpath(*relative.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise PayloadArchiveError(
                    f"payload archive contains unsupported member: {member.name}"
                )
            expanded_size += int(member.size)
            if expanded_size > MAX_EXPANDED_BYTES:
                raise PayloadArchiveError(
                    f"expanded payload exceeds {MAX_EXPANDED_BYTES} bytes"
                )
            extracted = archive.extractfile(member)
            if extracted is None:
                raise PayloadArchiveError(
                    f"payload archive file cannot be read: {member.name}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("wb") as output:
                _copy_stream(extracted, output)
            target.chmod(member.mode & 0o777)


def _safe_member_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(
        part in {"", ".", ".."} for part in path.parts
    ):
        raise PayloadArchiveError(f"unsafe payload archive path: {value}")
    return path


def _copy_stream(source: BinaryIO, destination: BinaryIO) -> None:
    shutil.copyfileobj(source, destination, length=1024 * 1024)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(value: object, label: str) -> str:
    text = str(value or "").lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise PayloadArchiveError(f"{label} must be a sha256 digest")
    return text


def _bounded_int(
    value: object,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PayloadArchiveError(f"{label} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise PayloadArchiveError(
            f"{label} must be between {minimum} and {maximum}"
        )
    return parsed


def _temporary_path(parent: Path, prefix: str, suffix: str) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=str(parent))
    os.close(descriptor)
    return Path(raw)
