from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path


class ProtocolInstallError(RuntimeError):
    pass


def install_protocol_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    receipt_path: Path,
) -> dict[str, object]:
    archive = archive.resolve()
    runtime_root = runtime_root.resolve()
    receipt_path = receipt_path.resolve()
    _validate_digest(expected_generation, "protocol generation")
    _validate_digest(expected_archive_sha256, "protocol archive SHA-256")
    if not archive.is_file():
        raise ProtocolInstallError(f"protocol archive is missing: {archive}")
    actual_archive_sha256 = _file_digest(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise ProtocolInstallError(
            "protocol archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={actual_archive_sha256}"
        )

    generations = runtime_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / expected_generation
    if destination.exists():
        actual_generation = protocol_generation(destination)
        if actual_generation != expected_generation:
            raise ProtocolInstallError(
                "immutable protocol generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
    else:
        staging = Path(tempfile.mkdtemp(prefix=".protocol-", dir=generations))
        try:
            _safe_extract(archive, staging)
            actual_generation = protocol_generation(staging)
            if actual_generation != expected_generation:
                raise ProtocolInstallError(
                    "staged protocol generation mismatch: "
                    f"expected={expected_generation} actual={actual_generation}"
                )
            os.replace(staging, destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    _write_pointer(runtime_root / "current", expected_generation)
    receipt = {
        "schema": "ascendop.protocol-deployment-receipt.v3",
        "protocol_generation": expected_generation,
        "archive_sha256": actual_archive_sha256,
        "runtime_source": str(destination / "src"),
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_atomic(receipt_path, receipt)
    _write_json_atomic(destination / "DEPLOYMENT_RECEIPT.json", receipt)
    return receipt


def protocol_generation(root: Path) -> str:
    root = root.resolve()
    paths = [root / "pyproject.toml"]
    package = root / "src" / "ascendop_protocol"
    paths.extend(
        path
        for path in sorted(package.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )
    if not package.is_dir() or any(not path.is_file() for path in paths):
        raise ProtocolInstallError(f"protocol package is incomplete: {root}")
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
    destination = destination.resolve()
    with tarfile.open(archive, "r:gz") as handle:
        members = handle.getmembers()
        for member in members:
            target = (destination / member.name).resolve()
            if target != destination and destination not in target.parents:
                raise ProtocolInstallError(
                    f"protocol archive escapes destination: {member.name}"
                )
            if member.issym() or member.islnk():
                raise ProtocolInstallError(
                    f"protocol archive contains a link: {member.name}"
                )
        handle.extractall(destination, members=members)


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_digest(value: str, label: str) -> None:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ProtocolInstallError(f"invalid {label}: {value}")


def _write_pointer(path: Path, generation: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(generation + "\n", encoding="ascii")
    os.replace(temporary, path)


def _write_json_atomic(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Atomically install one immutable AscendOP protocol runtime."
    )
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--expected-archive-sha256", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = install_protocol_runtime(
            archive=args.archive,
            runtime_root=args.runtime_root,
            expected_generation=args.expected_generation,
            expected_archive_sha256=args.expected_archive_sha256,
            receipt_path=args.receipt,
        )
    except ProtocolInstallError as exc:
        print(str(exc), file=os.sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
