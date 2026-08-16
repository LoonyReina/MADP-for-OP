from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

if __package__:
    from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs
    from limited_remote_partner.engine.runtime_manifest import (
        ENGINE_RUNTIME_FILES,
        SHARED_PROTOCOL_FILES,
    )
else:
    # This helper is also executed by path as the immutable maintenance payload.
    ENGINE_RUNTIME_FILES: tuple[str, ...] = ()
    SHARED_PROTOCOL_FILES: tuple[str, ...] = ()

    def hidden_subprocess_kwargs() -> dict[str, Any]:
        if os.name != "nt":
            return {}
        return {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0)}


MANIFEST_NAME = "runtime_manifest.json"
MANIFEST_SCHEMA = "ascendop.engine-runtime-manifest.v3"


class DirectEngineCodeSyncError(RuntimeError):
    pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Atomically install one generation-fenced Engine runtime bundle."
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target-repo", type=Path, required=True)
    parser.add_argument("--engine-root", type=Path, required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        receipt = apply_engine_runtime(
            source=args.source,
            target_repo=args.target_repo,
            engine_root=args.engine_root,
            expected_generation=args.expected_generation,
            receipt_path=args.receipt,
        )
    except DirectEngineCodeSyncError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(receipt, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


def apply_engine_runtime(
    *,
    source: Path,
    target_repo: Path,
    engine_root: Path,
    expected_generation: str,
    receipt_path: Path,
) -> dict[str, Any]:
    work_root = Path.cwd().resolve()
    source = bounded_path(work_root, source, "source")
    target_repo = bounded_path(work_root, target_repo, "target_repo")
    engine_root = bounded_path(work_root, engine_root, "engine_root")
    receipt_path = bounded_path(work_root, receipt_path, "receipt")
    target_source = target_repo / "src"
    target_package = target_source / "limited_remote_partner"
    if not target_package.is_dir():
        raise DirectEngineCodeSyncError(
            f"target GitPartner package is missing: {target_package}"
        )

    manifest = load_runtime_manifest(source / MANIFEST_NAME)
    source_generation = runtime_bundle_generation(source, manifest)
    if source_generation != expected_generation:
        raise DirectEngineCodeSyncError(
            "source Engine generation mismatch: "
            f"expected={expected_generation} actual={source_generation}"
        )

    runtime_root = engine_root / "runtime"
    generations_root = runtime_root / "generations"
    pointer_path = runtime_root / "current"
    previous_pointer = read_runtime_pointer(pointer_path)
    previous_source = runtime_source_for_generation(engine_root, previous_pointer)
    if previous_source is None:
        previous_source = target_source

    status = run_engine_cli(
        target_repo,
        engine_root,
        "status",
        runtime_source=previous_source,
    )
    nonterminal = sum(
        int(status.get(name, 0) or 0)
        for name in ("accepted_nonterminal", "queued_nonterminal", "active_nonterminal")
    )
    if nonterminal:
        raise DirectEngineCodeSyncError(
            f"Engine runtime sync requires an idle scheduler: nonterminal={nonterminal}"
        )
    run_engine_cli(
        target_repo,
        engine_root,
        "stop",
        "--wait-seconds",
        "10",
        runtime_source=previous_source,
    )

    generations_root.mkdir(parents=True, exist_ok=True)
    generation_root = generations_root / expected_generation
    generation_source = generation_root / "src"
    staging_root: Path | None = None
    try:
        if generation_root.exists():
            installed_manifest = load_runtime_manifest(
                generation_root / MANIFEST_NAME
            )
            installed_generation = runtime_source_generation(
                generation_source,
                installed_manifest,
            )
        else:
            staging_root = Path(
                tempfile.mkdtemp(
                    prefix=".i-",
                    dir=generations_root,
                )
            )
            staging_source = staging_root / "src"
            shutil.copytree(
                target_source,
                staging_source,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
            )
            _copy_manifest_files(source, staging_source, manifest)
            _copy_file(source / MANIFEST_NAME, staging_root / MANIFEST_NAME)
            installed_generation = runtime_source_generation(staging_source, manifest)
            if installed_generation != expected_generation:
                raise DirectEngineCodeSyncError(
                    "staged Engine generation mismatch: "
                    f"expected={expected_generation} actual={installed_generation}"
                )
            os.replace(staging_root, generation_root)
            staging_root = None
        if installed_generation != expected_generation:
            raise DirectEngineCodeSyncError(
                "installed Engine generation mismatch: "
                f"expected={expected_generation} actual={installed_generation}"
            )

        service = run_engine_cli(
            target_repo,
            engine_root,
            "start",
            "--interval-seconds",
            "0.25",
            runtime_source=generation_source,
        )
        if (
            str(service.get("current_code_generation") or "")
            != expected_generation
            or not bool(service.get("resident_ok"))
        ):
            raise DirectEngineCodeSyncError(
                "updated Engine resident did not adopt the requested generation"
            )
        write_runtime_pointer(pointer_path, expected_generation)
    except Exception as exc:
        if staging_root is not None:
            shutil.rmtree(staging_root, ignore_errors=True)
        _restore_runtime(
            target_repo=target_repo,
            engine_root=engine_root,
            failed_source=generation_source,
            previous_source=previous_source,
            previous_pointer=previous_pointer,
            pointer_path=pointer_path,
        )
        if isinstance(exc, DirectEngineCodeSyncError):
            raise
        raise DirectEngineCodeSyncError(str(exc)) from exc

    receipt = {
        "schema": "gitpartner.direct-engine-code-sync.v3",
        "state": "success",
        "before_generation": previous_pointer,
        "engine_code_generation": expected_generation,
        "installed_files": [
            f"{item['package']}/{item['path']}" for item in manifest["files"]
        ],
        "target_repo": str(target_repo.relative_to(work_root)),
        "engine_root": str(engine_root.relative_to(work_root)),
        "runtime_source": str(generation_source.relative_to(work_root)),
        "runtime_pointer": str(pointer_path.relative_to(work_root)),
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(receipt_path, receipt)
    return receipt


def load_runtime_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DirectEngineCodeSyncError(f"invalid Engine runtime manifest: {path}") from exc
    if not isinstance(value, dict) or value.get("schema") != MANIFEST_SCHEMA:
        raise DirectEngineCodeSyncError("unsupported Engine runtime manifest schema")
    generation = str(value.get("generation") or "")
    files = value.get("files")
    if not _safe_generation(generation) or not isinstance(files, list) or not files:
        raise DirectEngineCodeSyncError("incomplete Engine runtime manifest")
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in files:
        if not isinstance(raw, dict):
            raise DirectEngineCodeSyncError("runtime manifest file entry must be an object")
        package = str(raw.get("package") or "")
        relative = str(raw.get("path") or "").replace("\\", "/")
        digest = str(raw.get("sha256") or "")
        if package not in {"limited_remote_partner", "ascendop_protocol"}:
            raise DirectEngineCodeSyncError(f"unsafe runtime package: {package}")
        if not _safe_relative_path(relative) or not _sha256(digest):
            raise DirectEngineCodeSyncError(f"unsafe runtime manifest entry: {raw}")
        key = (package, relative)
        if key in seen:
            raise DirectEngineCodeSyncError(f"duplicate runtime manifest entry: {key}")
        seen.add(key)
        normalized.append({"package": package, "path": relative, "sha256": digest})
    return {"schema": MANIFEST_SCHEMA, "generation": generation, "files": normalized}


def runtime_bundle_generation(source: Path, manifest: dict[str, Any]) -> str:
    return _generation_for_entries(source, manifest["files"])


def runtime_source_generation(source: Path, manifest: dict[str, Any]) -> str:
    return _generation_for_entries(source, manifest["files"])


def runtime_generation(
    package_root: Path,
    protocol_root: Path | None = None,
    *,
    allow_missing: bool = False,
) -> str:
    protocol = protocol_root or package_root.parent / "ascendop_protocol"
    try:
        digest = hashlib.sha256()
        for package, root, files in (
            ("limited_remote_partner", package_root, ENGINE_RUNTIME_FILES),
            ("ascendop_protocol", protocol, SHARED_PROTOCOL_FILES),
        ):
            for relative in files:
                payload = _runtime_file_payload(root / relative)
                digest.update(f"{package}/{relative}".encode("utf-8"))
                digest.update(payload)
        return digest.hexdigest()[:16]
    except DirectEngineCodeSyncError:
        if allow_missing:
            return ""
        raise


def _generation_for_entries(
    source: Path,
    entries: Iterable[dict[str, str]],
    *,
    verify_sha: bool = True,
) -> str:
    digest = hashlib.sha256()
    for item in entries:
        package = item["package"]
        relative = item["path"]
        path = source / package / Path(relative)
        payload = _runtime_file_payload(path)
        if verify_sha and hashlib.sha256(payload).hexdigest() != item["sha256"]:
            raise DirectEngineCodeSyncError(f"Engine runtime digest mismatch: {package}/{relative}")
        digest.update(f"{package}/{relative}".encode("utf-8"))
        digest.update(payload)
    return digest.hexdigest()[:16]


def _copy_manifest_files(
    source: Path,
    target_source: Path,
    manifest: dict[str, Any],
) -> None:
    for item in manifest["files"]:
        relative = Path(item["path"])
        source_path = source / item["package"] / relative
        target_path = target_source / item["package"] / relative
        _mkdir(target_path.parent)
        _copy_file(source_path, target_path)


def _copy_file(source: Path, target: Path) -> None:
    """Copy through Win32 extended paths when nested generation paths exceed MAX_PATH."""
    shutil.copy2(_native_path(source), _native_path(target))


def _mkdir(path: Path) -> None:
    os.makedirs(_native_path(path), exist_ok=True)


def _runtime_file_payload(path: Path) -> bytes:
    native = _native_path(path)
    if not os.path.isfile(native) or os.path.islink(native):
        raise DirectEngineCodeSyncError(f"Engine runtime module is missing: {path}")
    with open(native, "rb") as handle:
        return handle.read().replace(b"\r\n", b"\n")


def _native_path(path: Path) -> str:
    value = str(path.resolve())
    if os.name == "nt" and not value.startswith("\\\\?\\"):
        return "\\\\?\\" + value
    return value


def _restore_runtime(
    *,
    target_repo: Path,
    engine_root: Path,
    failed_source: Path,
    previous_source: Path,
    previous_pointer: str,
    pointer_path: Path,
) -> None:
    try:
        run_engine_cli(
            target_repo,
            engine_root,
            "stop",
            "--wait-seconds",
            "10",
            runtime_source=failed_source,
        )
    except DirectEngineCodeSyncError:
        pass
    if previous_pointer:
        write_runtime_pointer(pointer_path, previous_pointer)
    else:
        pointer_path.unlink(missing_ok=True)
    try:
        run_engine_cli(
            target_repo,
            engine_root,
            "start",
            "--interval-seconds",
            "0.25",
            runtime_source=previous_source,
        )
    except DirectEngineCodeSyncError:
        pass


def run_engine_cli(
    target_repo: Path,
    engine_root: Path,
    command: str,
    *args: str,
    runtime_source: Path | None = None,
) -> dict[str, Any]:
    env = os.environ.copy()
    package_root = str(
        (runtime_source if runtime_source is not None else target_repo / "src").resolve()
    )
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = os.pathsep.join(
        item for item in (package_root, existing) if item
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "limited_remote_partner.cli.test_engine_cli",
            "--root",
            str(engine_root),
            command,
            *args,
        ],
        cwd=target_repo,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
        timeout=30,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        detail = "\n".join(
            part.strip() for part in (completed.stdout, completed.stderr) if part.strip()
        )
        raise DirectEngineCodeSyncError(
            f"Engine {command} failed rc={completed.returncode}: {detail[-2000:]}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise DirectEngineCodeSyncError(
            f"Engine {command} returned invalid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise DirectEngineCodeSyncError(f"Engine {command} response must be an object")
    return payload


def read_runtime_pointer(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        return ""
    value = path.read_text(encoding="utf-8", errors="replace").strip()
    return value if _safe_generation(value) else ""


def runtime_source_for_generation(engine_root: Path, generation: str) -> Path | None:
    if not _safe_generation(generation):
        return None
    source = engine_root / "runtime" / "generations" / generation / "src"
    packages = (source / "limited_remote_partner", source / "ascendop_protocol")
    return source if all(path.is_dir() and not path.is_symlink() for path in packages) else None


def write_runtime_pointer(path: Path, generation: str) -> None:
    if not _safe_generation(generation):
        raise DirectEngineCodeSyncError(f"unsafe Engine runtime generation: {generation}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(generation + "\n", encoding="utf-8")
    os.replace(temporary, path)


def bounded_path(root: Path, path: Path, name: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve()
    if resolved != root and root not in resolved.parents:
        raise DirectEngineCodeSyncError(f"{name} escapes work root: {path}")
    return resolved


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _safe_relative_path(value: str) -> bool:
    path = Path(value)
    return bool(value) and not path.is_absolute() and ".." not in path.parts


def _safe_generation(value: str) -> bool:
    return bool(value) and len(value) <= 64 and all(
        char in "0123456789abcdef" for char in value
    )


def _sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


if __name__ == "__main__":
    raise SystemExit(main())
