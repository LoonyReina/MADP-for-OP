from __future__ import annotations

import hashlib
import os
import shutil
import tarfile
import tempfile
from pathlib import Path


class PythonProductInstallError(RuntimeError):
    pass


def install_python_product_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    package: str,
    label: str,
) -> Path:
    actual_archive_sha256 = _file_digest(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise PythonProductInstallError(
            f"{label} archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={actual_archive_sha256}"
        )
    generations = runtime_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / expected_generation
    if destination.is_dir():
        actual_generation = python_product_generation(destination, package)
        if actual_generation != expected_generation:
            raise PythonProductInstallError(
                f"immutable {label} generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        return destination / "src"

    staging = Path(tempfile.mkdtemp(prefix=f".{package}-", dir=generations))
    try:
        _safe_extract(archive, staging)
        actual_generation = python_product_generation(staging, package)
        if actual_generation != expected_generation:
            raise PythonProductInstallError(
                f"staged {label} generation mismatch: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        os.replace(staging, destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
    return destination / "src"


def python_product_generation(root: Path, package_name: str) -> str:
    paths = [root / "pyproject.toml"]
    package = root / "src" / package_name
    paths.extend(
        path
        for path in sorted(package.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )
    if not package.is_dir() or any(not path.is_file() for path in paths):
        raise PythonProductInstallError(
            f"{package_name} package is incomplete: {root}"
        )
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _safe_extract(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, "r:gz") as handle:
        destination_root = destination.resolve()
        members = handle.getmembers()
        for member in members:
            candidate = (destination / member.name).resolve()
            if destination_root != candidate and destination_root not in candidate.parents:
                raise PythonProductInstallError(
                    f"unsafe archive member for Python product: {member.name}"
                )
            if member.issym() or member.islnk() or member.isdev():
                raise PythonProductInstallError(
                    f"unsupported archive member for Python product: {member.name}"
                )
        handle.extractall(destination, members=members, filter="data")


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
