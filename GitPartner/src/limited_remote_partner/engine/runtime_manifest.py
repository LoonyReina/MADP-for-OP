from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Iterable

import ascendop_protocol


ENGINE_RUNTIME_FILES = (
    "core/atomic_file.py",
    "engine/batch_case_runner.py",
    "resources/case_cache.py",
    "engine/stages/correctness_pipeline.py",
    "engine/engine_identity.py",
    "endpoint/flow_v3_endpoint.py",
    "resources/operator_cache.py",
    "resources/payload_archive.py",
    "observability/runtime_readiness.py",
    "engine/stages/perf_pipeline.py",
    "engine/stages/profile_session_runner.py",
    "resources/shared_resource_lease.py",
    "core/process_utils.py",
    "engine/test_engine.py",
    "cli/test_engine_cli.py",
    "engine/test_engine_worker.py",
    "resources/wheel_cache.py",
    "engine/runtime_manifest.py",
)

def protocol_package_root() -> Path:
    return Path(ascendop_protocol.__file__).resolve().parent


def discover_shared_protocol_files(root: Path | None = None) -> tuple[str, ...]:
    package_root = (root or protocol_package_root()).resolve()
    return tuple(
        path.relative_to(package_root).as_posix()
        for path in sorted(package_root.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )


SHARED_PROTOCOL_FILES = discover_shared_protocol_files()


def runtime_generation(
    package_root: Path | None = None,
    protocol_root: Path | None = None,
) -> str:
    root = package_root or Path(__file__).resolve().parents[1]
    shared = protocol_root or protocol_package_root()
    last_error: OSError | None = None
    for attempt in range(3):
        try:
            return _digest_sources(root, shared)
        except OSError as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.02)
    raise OSError("Engine runtime changed while computing generation") from last_error


def manifest_entries() -> tuple[tuple[str, str], ...]:
    return tuple(
        ("limited_remote_partner", relative) for relative in ENGINE_RUNTIME_FILES
    ) + tuple(("ascendop_protocol", relative) for relative in SHARED_PROTOCOL_FILES)


def runtime_manifest_document(
    package_root: Path | None = None,
    protocol_root: Path | None = None,
) -> dict[str, object]:
    root = package_root or Path(__file__).resolve().parents[1]
    shared = protocol_root or protocol_package_root()
    files: list[dict[str, str]] = []
    for package, relative in manifest_entries():
        source_root = root if package == "limited_remote_partner" else shared
        payload = (source_root / relative).read_bytes().replace(b"\r\n", b"\n")
        files.append(
            {
                "package": package,
                "path": relative,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return {
        "schema": "ascendop.engine-runtime-manifest.v3",
        "generation": runtime_generation(root, shared),
        "files": files,
    }


def manifest_bytes(
    package_root: Path | None = None,
    protocol_root: Path | None = None,
) -> bytes:
    return (
        json.dumps(
            runtime_manifest_document(package_root, protocol_root),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _digest_sources(package_root: Path, protocol_root: Path) -> str:
    digest = hashlib.sha256()
    for package, relative in manifest_entries():
        root = package_root if package == "limited_remote_partner" else protocol_root
        _update_digest(digest, f"{package}/{relative}", root / relative)
    return digest.hexdigest()[:16]


def _update_digest(digest: object, name: str, path: Path) -> None:
    update = getattr(digest, "update")
    update(name.encode("utf-8"))
    update(path.read_bytes().replace(b"\r\n", b"\n"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Emit the immutable Engine V3 runtime manifest")
    parser.add_argument("--package-root", type=Path, required=True)
    parser.add_argument("--protocol-root", type=Path, required=True)
    args = parser.parse_args(argv)
    print(
        json.dumps(
            runtime_manifest_document(
                args.package_root.resolve(),
                args.protocol_root.resolve(),
            ),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
