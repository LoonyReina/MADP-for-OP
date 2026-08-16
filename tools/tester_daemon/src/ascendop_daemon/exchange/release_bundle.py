from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from ascendop_daemon.exchange.daemon_installer import daemon_generation
from ascendop_daemon.exchange.transport_installer import transport_generation
from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
)
from ascendop_daemon.storage.control_types import SCHEMA_VERSION


RELEASE_SCHEMA = "ascendop.endpoint-release.v3"


class ReleaseBundleError(RuntimeError):
    pass


def build_endpoint_release(
    *,
    workspace_root: Path,
    gp_root: Path,
    endpoint_config: Path,
    output_root: Path,
    daemon_config: Path | None = None,
    system_registry: Path | None = None,
    engine_manifest_provider: Callable[[Path, Path], dict[str, object]] | None = None,
) -> dict[str, object]:
    workspace_root = workspace_root.resolve()
    gp_root = gp_root.resolve()
    endpoint_config = endpoint_config.resolve()
    output_root = output_root.resolve()
    protocol_root = workspace_root / "packages" / "ascendop_protocol"
    daemon_root = workspace_root / "tools" / "tester_daemon"
    daemon_config = (
        daemon_config
        or daemon_root / "config" / "cann_ladder_910b_cann90.json"
    ).resolve()
    system_registry = (
        system_registry
        or workspace_root / "Develop" / "registry" / "system_registry.json"
    ).resolve()
    variable_registry = (
        workspace_root / "docs" / "engine_exchange_protocol" / "v3" / "variables.json"
    )
    _require_child(gp_root, endpoint_config, "endpoint config")
    if not endpoint_config.is_file():
        raise ReleaseBundleError(f"endpoint config is missing: {endpoint_config}")
    if not (protocol_root / "src" / "ascendop_protocol").is_dir():
        raise ReleaseBundleError(f"shared protocol package is missing: {protocol_root}")
    if not variable_registry.is_file():
        raise ReleaseBundleError(
            f"protocol variable registry is missing: {variable_registry}"
        )
    _require_child(workspace_root, daemon_config, "daemon config")
    _require_child(workspace_root, system_registry, "system registry")
    if not daemon_config.is_file():
        raise ReleaseBundleError(f"daemon config is missing: {daemon_config}")
    if not system_registry.is_file():
        raise ReleaseBundleError(f"system registry is missing: {system_registry}")

    transport_gen = transport_generation(gp_root / "src")
    protocol_files = tuple(_protocol_files(protocol_root))
    protocol_gen = _tree_generation(protocol_root, protocol_files)
    daemon_files = tuple(_daemon_files(daemon_root))
    daemon_gen = daemon_generation(daemon_root)
    engine_manifest = (
        engine_manifest_provider(gp_root, protocol_root)
        if engine_manifest_provider is not None
        else _load_engine_manifest(gp_root, protocol_root)
    )
    engine_files = _validate_engine_manifest(
        engine_manifest,
        gp_root=gp_root,
        protocol_root=protocol_root,
    )
    engine_generation = str(engine_manifest["generation"])
    identity = {
        "schema": RELEASE_SCHEMA,
        "wire_version": 3,
        "endpoint_config": endpoint_config.relative_to(gp_root).as_posix(),
        "endpoint_config_sha256": _file_digest(endpoint_config),
        "transport_generation": transport_gen,
        "protocol_generation": protocol_gen,
        "daemon_generation": daemon_gen,
        "control_database_schema": SCHEMA_VERSION,
        "engine_code_generation": engine_generation,
        "variable_registry_sha256": _file_digest(variable_registry),
        "daemon_config_path": daemon_config.relative_to(workspace_root).as_posix(),
        "daemon_config_sha256": _file_digest(daemon_config),
        "system_registry_path": system_registry.relative_to(workspace_root).as_posix(),
        "system_registry_sha256": _file_digest(system_registry),
    }
    release_generation = _object_digest(identity)
    release_root = output_root / release_generation
    release_root.mkdir(parents=True, exist_ok=True)
    transport_archive = release_root / "gitpartner-runtime.tar.gz"
    protocol_archive = release_root / "ascendop-protocol.tar.gz"
    daemon_archive = release_root / "ascendop-daemon.tar.gz"
    engine_archive = release_root / "engine-runtime.tar.gz"
    variable_registry_artifact = release_root / "variables.json"
    daemon_config_artifact = release_root / "daemon-config.json"
    system_registry_artifact = release_root / "system-registry.json"
    _write_archive(gp_root, _transport_files(gp_root), transport_archive)
    _write_archive(protocol_root, protocol_files, protocol_archive)
    _write_archive(daemon_root, daemon_files, daemon_archive)
    _write_engine_archive(
        gp_root=gp_root,
        protocol_root=protocol_root,
        files=engine_files,
        manifest=engine_manifest,
        destination=engine_archive,
    )
    _write_file_atomic(variable_registry, variable_registry_artifact)
    _write_file_atomic(daemon_config, daemon_config_artifact)
    _write_file_atomic(system_registry, system_registry_artifact)
    manifest = {
        **identity,
        "release_generation": release_generation,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "archives": {
            "transport": {
                "path": transport_archive.name,
                "sha256": _file_digest(transport_archive),
            },
            "protocol": {
                "path": protocol_archive.name,
                "sha256": _file_digest(protocol_archive),
            },
            "daemon": {
                "path": daemon_archive.name,
                "sha256": _file_digest(daemon_archive),
            },
            "engine": {
                "path": engine_archive.name,
                "sha256": _file_digest(engine_archive),
            },
            "variable_registry": {
                "path": variable_registry_artifact.name,
                "sha256": _file_digest(variable_registry_artifact),
            },
            "daemon_config": {
                "path": daemon_config_artifact.name,
                "sha256": _file_digest(daemon_config_artifact),
            },
            "system_registry": {
                "path": system_registry_artifact.name,
                "sha256": _file_digest(system_registry_artifact),
            },
        },
    }
    manifest_path = release_root / "RELEASE.json"
    _write_json_atomic(manifest_path, manifest)
    return {**manifest, "manifest_path": str(manifest_path)}


def _transport_files(root: Path) -> tuple[Path, ...]:
    selected = [root / "pyproject.toml", root / ".gitattributes"]
    for relative in ("src/limited_remote_partner", "scripts", "services", "configs"):
        selected.extend(_regular_files(root / relative))
    return _unique_existing(root, selected)


def _protocol_files(root: Path) -> Iterable[Path]:
    yield root / "pyproject.toml"
    yield from _regular_files(root / "src" / "ascendop_protocol")


def _daemon_files(root: Path) -> Iterable[Path]:
    yield root / "pyproject.toml"
    yield root / "daemon.py"
    yield root / "launch_s5_910b.py"
    yield root / "manage_s5_910b.ps1"
    yield from _regular_files(root / "src" / "ascendop_daemon")


def _regular_files(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return [
        path
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    ]


def _unique_existing(root: Path, paths: Iterable[Path]) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        resolved = path.resolve()
        _require_child(root, resolved, "release file")
        if not resolved.is_file() or resolved.is_symlink():
            raise ReleaseBundleError(f"release file is missing or unsafe: {resolved}")
        relative = resolved.relative_to(root).as_posix()
        if relative not in seen:
            seen.add(relative)
            result.append(resolved)
    return tuple(sorted(result, key=lambda item: item.relative_to(root).as_posix()))


def _tree_generation(root: Path, paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _write_archive(root: Path, paths: Iterable[Path], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                with tarfile.open(
                    fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT
                ) as archive:
                    for path in paths:
                        relative = path.relative_to(root).as_posix()
                        info = archive.gettarinfo(str(path), arcname=relative)
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        with path.open("rb") as source:
                            archive.addfile(info, source)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _load_engine_manifest(gp_root: Path, protocol_root: Path) -> dict[str, object]:
    script = (
        gp_root
        / "src"
        / "limited_remote_partner"
        / "engine"
        / "runtime_manifest.py"
    )
    if not script.is_file():
        raise ReleaseBundleError(f"Engine runtime manifest provider is missing: {script}")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (
            str(protocol_root / "src"),
            environment.get("PYTHONPATH", ""),
        )
        if item
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--package-root",
            str(gp_root / "src" / "limited_remote_partner"),
            "--protocol-root",
            str(protocol_root / "src" / "ascendop_protocol"),
        ],
        cwd=str(gp_root),
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        creationflags=process_creation_flags(),
        startupinfo=process_startupinfo(),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ReleaseBundleError(
            f"Engine runtime manifest provider failed: {detail[:1024]}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ReleaseBundleError("Engine runtime manifest provider returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ReleaseBundleError("Engine runtime manifest must be an object")
    return value


def _validate_engine_manifest(
    manifest: dict[str, object],
    *,
    gp_root: Path,
    protocol_root: Path,
) -> tuple[tuple[str, str, Path], ...]:
    if manifest.get("schema") != "ascendop.engine-runtime-manifest.v3":
        raise ReleaseBundleError("unsupported Engine runtime manifest schema")
    generation = str(manifest.get("generation") or "")
    if len(generation) != 16 or any(char not in "0123456789abcdef" for char in generation):
        raise ReleaseBundleError(f"invalid Engine code generation: {generation}")
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise ReleaseBundleError("Engine runtime manifest contains no files")
    roots = {
        "limited_remote_partner": gp_root / "src" / "limited_remote_partner",
        "ascendop_protocol": protocol_root / "src" / "ascendop_protocol",
    }
    result: list[tuple[str, str, Path]] = []
    seen: set[tuple[str, str]] = set()
    digest = hashlib.sha256()
    for raw in raw_files:
        if not isinstance(raw, dict):
            raise ReleaseBundleError("Engine runtime manifest entry must be an object")
        package = str(raw.get("package") or "")
        relative = str(raw.get("path") or "").replace("\\", "/")
        expected = str(raw.get("sha256") or "")
        if package not in roots:
            raise ReleaseBundleError(f"unsafe Engine runtime package: {package}")
        candidate = Path(relative)
        if (
            not relative
            or candidate.is_absolute()
            or ".." in candidate.parts
            or relative.startswith("/")
        ):
            raise ReleaseBundleError(f"unsafe Engine runtime path: {relative}")
        key = (package, relative)
        if key in seen:
            raise ReleaseBundleError(f"duplicate Engine runtime path: {package}/{relative}")
        seen.add(key)
        path = (roots[package] / candidate).resolve()
        _require_child(roots[package], path, "Engine runtime file")
        if not path.is_file() or path.is_symlink():
            raise ReleaseBundleError(f"Engine runtime file is missing: {path}")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            raise ReleaseBundleError(
                f"Engine runtime digest mismatch: {package}/{relative}"
            )
        digest.update(f"{package}/{relative}".encode("utf-8"))
        digest.update(payload)
        result.append((package, relative, path))
    if digest.hexdigest()[:16] != generation:
        raise ReleaseBundleError("Engine runtime manifest generation mismatch")
    return tuple(result)


def _write_engine_archive(
    *,
    gp_root: Path,
    protocol_root: Path,
    files: Iterable[tuple[str, str, Path]],
    manifest: dict[str, object],
    destination: Path,
) -> None:
    del gp_root, protocol_root
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(handle)
    temporary = Path(temporary_name)
    manifest_payload = (
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                with tarfile.open(
                    fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT
                ) as archive:
                    manifest_info = tarfile.TarInfo("runtime_manifest.json")
                    manifest_info.size = len(manifest_payload)
                    manifest_info.mtime = 0
                    archive.addfile(manifest_info, io.BytesIO(manifest_payload))
                    for package, relative, path in files:
                        info = archive.gettarinfo(
                            str(path), arcname=f"{package}/{relative}"
                        )
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        with path.open("rb") as source:
                            archive.addfile(info, source)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _require_child(root: Path, path: Path, label: str) -> None:
    root = root.resolve()
    path = path.resolve()
    if path != root and root not in path.parents:
        raise ReleaseBundleError(f"{label} escapes root: {path}")


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _object_digest(value: dict[str, object]) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_json_atomic(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _write_file_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_bytes(source.read_bytes())
    os.replace(temporary, destination)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an immutable endpoint V3 release")
    parser.add_argument("--workspace-root", type=Path, required=True)
    parser.add_argument("--gp-root", type=Path, required=True)
    parser.add_argument("--endpoint-config", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--daemon-config", type=Path)
    parser.add_argument("--system-registry", type=Path)
    args = parser.parse_args(argv)
    try:
        result = build_endpoint_release(
            workspace_root=args.workspace_root,
            gp_root=args.gp_root,
            endpoint_config=args.endpoint_config,
            output_root=args.output_root,
            daemon_config=args.daemon_config,
            system_registry=args.system_registry,
        )
    except ReleaseBundleError as exc:
        print(str(exc), file=os.sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
