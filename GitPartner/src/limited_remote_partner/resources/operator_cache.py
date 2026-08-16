from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs
from limited_remote_partner.engine.test_engine import atomic_write_json, utc_now
from limited_remote_partner.resources.wheel_cache import file_lock


class OperatorCacheError(RuntimeError):
    pass


PROTOCOL_VERSION = "engine-operator-cache-v1"


def resolve_operator_package(
    *,
    source: Path,
    build_python: Path,
    cache_root: Path,
    output: Path,
    receipt: Path,
    build_log: Path,
    target_arch: str,
) -> dict[str, Any]:
    total_started = time.monotonic()
    source = source.resolve()
    build_python = build_python.resolve()
    cache_root = cache_root.resolve()
    output = output.resolve()
    receipt = receipt.resolve()
    build_log = build_log.resolve()
    if not (source / "build.sh").is_file():
        raise OperatorCacheError(f"operator build.sh is missing: {source}")
    if not build_python.is_file():
        raise OperatorCacheError(f"build Python is missing: {build_python}")

    identity_started = time.monotonic()
    identity = build_identity(
        source=source,
        build_python=build_python,
        target_arch=target_arch,
    )
    identity_seconds = time.monotonic() - identity_started
    key = cache_key(identity)
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_dir = cache_root / key
    probe_started = time.monotonic()
    package = valid_cache_entry(cache_dir, key)
    probe_seconds = time.monotonic() - probe_started
    cache_hit = package is not None
    lock_wait_seconds = 0.0
    population_seconds = 0.0
    if package is None:
        lock_started = time.monotonic()
        with file_lock(cache_root / f"{key}.lock"):
            lock_wait_seconds = time.monotonic() - lock_started
            package = valid_cache_entry(cache_dir, key)
            if package is None:
                population_started = time.monotonic()
                package = populate_cache(
                    source=source,
                    cache_root=cache_root,
                    cache_dir=cache_dir,
                    key=key,
                    identity=identity,
                    build_log=build_log,
                )
                population_seconds = time.monotonic() - population_started
            else:
                cache_hit = True
    assert package is not None
    if cache_hit:
        build_log.parent.mkdir(parents=True, exist_ok=True)
        build_log.write_text(
            f"OPERATOR_CACHE_HIT key={key} package={package}\n",
            encoding="utf-8",
        )

    materialize_started = time.monotonic()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    try:
        os.link(package, output)
    except OSError:
        shutil.copy2(package, output)
    materialize_seconds = time.monotonic() - materialize_started
    result = {
        "protocol_version": PROTOCOL_VERSION,
        "state": "ready",
        "cache_key": key,
        "cache_hit": cache_hit,
        "cache_entry": str(cache_dir),
        "cached_package": str(package),
        "output_package": str(output),
        "package_sha256": file_sha256(output),
        "identity": identity,
        "timing_seconds": {
            "identity": round(identity_seconds, 6),
            "initial_probe": round(probe_seconds, 6),
            "lock_wait": round(lock_wait_seconds, 6),
            "population": round(population_seconds, 6),
            "materialize": round(materialize_seconds, 6),
            "total": round(time.monotonic() - total_started, 6),
        },
        "finished_at": utc_now(),
    }
    receipt.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(receipt, result)
    return result


def build_identity(
    *, source: Path, build_python: Path, target_arch: str
) -> dict[str, Any]:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "source_sha256": tree_digest(source),
        "build_python": python_identity(build_python),
        "target_arch": str(target_arch),
        "platform_machine": platform.machine(),
        "cann": cann_identity(),
    }


def python_identity(path: Path) -> dict[str, Any]:
    stat = path.stat()
    completed = subprocess.run(
        [
            str(path),
            "-c",
            "import json,platform,sys;print(json.dumps({"
            "'version':platform.python_version(),"
            "'implementation':platform.python_implementation(),"
            "'executable':sys.executable},sort_keys=True))",
        ],
        check=False,
        text=True,
        capture_output=True,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        raise OperatorCacheError(
            "build Python identity probe failed: "
            f"rc={completed.returncode} stderr={completed.stderr[-500:]}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise OperatorCacheError("build Python identity probe returned invalid JSON") from exc
    return {
        "realpath": os.path.realpath(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        **payload,
    }


def cann_identity() -> dict[str, Any]:
    paths = {
        name: os.path.realpath(value)
        for name in (
            "ASCEND_HOME_PATH",
            "ASCEND_AICPU_PATH",
            "ASCEND_OPP_PATH",
        )
        if (value := os.environ.get(name, "").strip())
    }
    versions: dict[str, str] = {}
    candidates: set[Path] = set()
    for value in paths.values():
        root = Path(value)
        for parent in (root, root.parent, root.parent.parent):
            candidates.add(parent / "version.info")
            candidates.add(parent / "ascend_toolkit_install.info")
    for path in sorted(candidates, key=lambda item: str(item)):
        if path.is_file():
            versions[str(path)] = file_sha256(path)
    return {"paths": paths, "version_files": versions}


def cache_key(identity: dict[str, Any]) -> str:
    payload = json.dumps(
        identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def populate_cache(
    *,
    source: Path,
    cache_root: Path,
    cache_dir: Path,
    key: str,
    identity: dict[str, Any],
    build_log: Path,
) -> Path:
    build_log.parent.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(source / "build_out", ignore_errors=True)
    shutil.rmtree(source / "build", ignore_errors=True)
    with build_log.open("w", encoding="utf-8") as handle:
        completed = subprocess.run(
            ["bash", "build.sh"],
            cwd=str(source),
            check=False,
            text=True,
            stdout=handle,
            stderr=subprocess.STDOUT,
            **hidden_subprocess_kwargs(),
        )
    if completed.returncode != 0:
        raise OperatorCacheError(f"operator build failed rc={completed.returncode}")
    packages = built_operator_packages(source)
    if len(packages) != 1:
        raise OperatorCacheError(
            "expected exactly one operator package under build_out/ or build/, "
            f"found {len(packages)}"
        )
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=str(cache_root)))
    try:
        cached = temp_dir / "operator.run"
        shutil.copy2(packages[0], cached)
        manifest = {
            "protocol_version": PROTOCOL_VERSION,
            "state": "complete",
            "cache_key": key,
            "identity": identity,
            "package": cached.name,
            "package_sha256": file_sha256(cached),
            "created_at": utc_now(),
        }
        atomic_write_json(temp_dir / "manifest.json", manifest)
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        os.replace(temp_dir, cache_dir)
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
    result = valid_cache_entry(cache_dir, key)
    if result is None:
        raise OperatorCacheError("published operator cache failed validation")
    return result


def built_operator_packages(source: Path) -> list[Path]:
    packages: dict[str, Path] = {}
    for directory in ("build_out", "build"):
        for package in sorted((source / directory).glob("*.run")):
            packages[str(package.resolve())] = package
    return [packages[key] for key in sorted(packages)]


def valid_cache_entry(cache_dir: Path, key: str) -> Path | None:
    manifest_path = cache_dir / "manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict):
        return None
    if (
        manifest.get("protocol_version") != PROTOCOL_VERSION
        or manifest.get("state") != "complete"
        or manifest.get("cache_key") != key
    ):
        return None
    package = cache_dir / str(manifest.get("package") or "")
    if not package.is_file():
        return None
    if file_sha256(package) != str(manifest.get("package_sha256") or ""):
        return None
    return package


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise OperatorCacheError(f"operator source contains symlink: {path}")
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if path.is_file():
            update_canonical_file_digest(digest, path)
            digest.update(b"\0")
    return digest.hexdigest()


def update_canonical_file_digest(digest: Any, path: Path) -> None:
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            data = carry + chunk
            carry = b"\r" if data.endswith(b"\r") else b""
            if carry:
                data = data[:-1]
            digest.update(data.replace(b"\r\n", b"\n"))
    if carry:
        digest.update(carry)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resolve an immutable content-addressed AscendOP .run package"
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build-python", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--build-log", type=Path, required=True)
    parser.add_argument("--target-arch", default="910B4")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = resolve_operator_package(
            source=args.source,
            build_python=args.build_python,
            cache_root=args.cache_root,
            output=args.output,
            receipt=args.receipt,
            build_log=args.build_log,
            target_arch=args.target_arch,
        )
    except (OperatorCacheError, OSError, ValueError) as exc:
        print(f"operator cache failed: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    else:
        print(result["output_package"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
