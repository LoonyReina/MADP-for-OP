from __future__ import annotations

import ast
import gzip
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Iterable

from ascendop_daemon.exchange.release_bundle_validation import ReleaseBundleError
from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
)


def load_engine_manifest(gp_root: Path, protocol_root: Path) -> dict[str, object]:
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


def validate_engine_manifest(
    manifest: dict[str, object],
    *,
    gp_root: Path,
    protocol_root: Path,
) -> tuple[tuple[str, str, Path], ...]:
    if manifest.get("schema") != "ascendop.engine-runtime-manifest.v3":
        raise ReleaseBundleError("unsupported Engine runtime manifest schema")
    generation = str(manifest.get("generation") or "")
    if len(generation) != 16 or any(
        char not in "0123456789abcdef" for char in generation
    ):
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
    _validate_runtime_import_closure(
        gp_root=gp_root,
        selected_files={
            relative
            for package, relative, _path in result
            if package == "limited_remote_partner"
        },
    )
    return tuple(result)


def _validate_runtime_import_closure(
    *,
    gp_root: Path,
    selected_files: set[str],
) -> None:
    package_root = gp_root / "src" / "limited_remote_partner"
    missing: set[str] = set()
    for relative in sorted(selected_files):
        if not relative.endswith(".py"):
            continue
        source = package_root / Path(relative)
        try:
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        except (OSError, SyntaxError) as exc:
            raise ReleaseBundleError(
                f"Engine runtime source cannot be inspected: {relative}: {exc}"
            ) from exc
        missing.update(
            path
            for path in _required_package_initializers(package_root, relative)
            if path not in selected_files
        )
        for module in _internal_import_modules(tree, relative):
            path = _resolve_internal_module(package_root, module)
            if path is not None and path not in selected_files:
                missing.add(path)
    if missing:
        raise ReleaseBundleError(
            "Engine runtime manifest import closure is incomplete: "
            + ", ".join(sorted(missing))
        )


def _required_package_initializers(
    package_root: Path,
    relative: str,
) -> tuple[str, ...]:
    required = ["__init__.py"] if (package_root / "__init__.py").is_file() else []
    parent = Path(relative).parent
    while parent != Path("."):
        candidate = parent / "__init__.py"
        if (package_root / candidate).is_file():
            required.append(candidate.as_posix())
        parent = parent.parent
    return tuple(required)


def _internal_import_modules(tree: ast.AST, relative: str) -> tuple[str, ...]:
    current = "limited_remote_partner." + relative.removesuffix(".py").replace("/", ".")
    current_package = (
        current.removesuffix(".__init__")
        if current.endswith(".__init__")
        else current.rsplit(".", 1)[0]
    )
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(
                alias.name
                for alias in node.names
                if alias.name == "limited_remote_partner"
                or alias.name.startswith("limited_remote_partner.")
            )
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        module = node.module or ""
        if node.level:
            try:
                module = importlib.util.resolve_name(
                    "." * node.level + module,
                    current_package,
                )
            except (ImportError, ValueError):
                continue
        if module == "limited_remote_partner" or module.startswith(
            "limited_remote_partner."
        ):
            modules.add(module)
            modules.update(
                f"{module}.{alias.name}"
                for alias in node.names
                if alias.name != "*"
            )
    return tuple(sorted(modules))


def _resolve_internal_module(package_root: Path, module: str) -> str | None:
    if module == "limited_remote_partner":
        return "__init__.py" if (package_root / "__init__.py").is_file() else None
    prefix = "limited_remote_partner."
    if not module.startswith(prefix):
        return None
    relative = Path(*module.removeprefix(prefix).split("."))
    source = relative.with_suffix(".py")
    if (package_root / source).is_file():
        return source.as_posix()
    initializer = relative / "__init__.py"
    if (package_root / initializer).is_file():
        return initializer.as_posix()
    return None


def write_engine_archive(
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
