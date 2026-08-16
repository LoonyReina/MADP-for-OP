from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs


class WheelCacheError(RuntimeError):
    pass


ADAPTER_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".py"}


def resolve_wheel(
    source: Path,
    python: Path,
    cache_root: Path,
    output_dir: Path,
    *,
    target_arch: str = "910B4",
    site_output: Path | None = None,
    site_log: Path | None = None,
) -> dict[str, Any]:
    total_started = time.monotonic()
    source = source.resolve()
    # Preserve a virtualenv launcher path. Resolving the symlink to the base
    # interpreter would execute without the virtualenv's site-packages.
    python = Path(os.path.abspath(os.fspath(python)))
    cache_root = cache_root.resolve()
    output_dir = output_dir.resolve()
    if not (source / "setup.py").is_file():
        raise WheelCacheError(f"wheel source setup.py missing: {source}")
    identity_started = time.monotonic()
    identity = runtime_identity(python, target_arch=target_arch)
    identity_seconds = time.monotonic() - identity_started
    digest_started = time.monotonic()
    adapter_digest = adapter_source_digest(source)
    digest_seconds = time.monotonic() - digest_started
    key = cache_key(adapter_digest, identity)
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_dir = cache_root / key
    probe_started = time.monotonic()
    wheel = valid_cached_wheel(cache_dir, key)
    probe_seconds = time.monotonic() - probe_started
    cache_hit = wheel is not None
    cache_fast_path = cache_hit
    cache_lock_wait_seconds = 0.0
    cache_population_seconds = 0.0
    if wheel is None:
        lock_started = time.monotonic()
        with file_lock(cache_root / f"{key}.lock"):
            cache_lock_wait_seconds = time.monotonic() - lock_started
            # Another host stage may have populated this immutable key while
            # this process waited. Only the miss owner performs the build.
            wheel = valid_cached_wheel(cache_dir, key)
            if wheel is None:
                population_started = time.monotonic()
                wheel = populate_cache(
                    source, python, cache_root, cache_dir, key, identity
                )
                cache_population_seconds = time.monotonic() - population_started
            else:
                cache_hit = True
    assert wheel is not None
    materialize_started = time.monotonic()
    output_dir.mkdir(parents=True, exist_ok=True)
    for old in output_dir.glob("*.whl"):
        old.unlink()
    destination = output_dir / wheel.name
    try:
        os.link(wheel, destination)
    except OSError:
        shutil.copy2(wheel, destination)
    materialize_seconds = time.monotonic() - materialize_started
    result = {
        "cache_key": key,
        "cache_hit": cache_hit,
        "adapter_digest": adapter_digest,
        "cached_wheel": str(wheel),
        "output_wheel": str(destination),
        "wheel_sha256": file_sha256(destination),
        "identity": identity,
        "timing_seconds": {
            "identity": round(identity_seconds, 6),
            "adapter_digest": round(digest_seconds, 6),
            "initial_probe": round(probe_seconds, 6),
            "lock_wait": round(cache_lock_wait_seconds, 6),
            "population": round(cache_population_seconds, 6),
            "materialize": round(materialize_seconds, 6),
            "total": round(time.monotonic() - total_started, 6),
        },
        "cache_fast_path": cache_fast_path,
    }
    if site_output is not None:
        result["site_cache"] = resolve_site_cache(
            python=python,
            cache_root=cache_root,
            cache_dir=cache_dir,
            cache_key_value=key,
            wheel=wheel,
            output=site_output.resolve(),
            log=(site_log or site_output.parent / "pip_install.log").resolve(),
        )
        result["timing_seconds"]["total"] = round(
            time.monotonic() - total_started, 6
        )
    return result


def resolve_site_cache(
    *,
    python: Path,
    cache_root: Path,
    cache_dir: Path,
    cache_key_value: str,
    wheel: Path,
    output: Path,
    log: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    wheel_digest = file_sha256(wheel)
    site_dir = cache_dir / "site"
    site_manifest = valid_cached_site(
        site_dir,
        cache_key_value=cache_key_value,
        wheel_sha256=wheel_digest,
    )
    cache_hit = site_manifest is not None
    lock_wait_seconds = 0.0
    population_seconds = 0.0
    if site_manifest is None:
        lock_started = time.monotonic()
        with file_lock(cache_root / f"{cache_key_value}.lock"):
            lock_wait_seconds = time.monotonic() - lock_started
            site_manifest = valid_cached_site(
                site_dir,
                cache_key_value=cache_key_value,
                wheel_sha256=wheel_digest,
            )
            if site_manifest is None:
                population_started = time.monotonic()
                site_manifest = populate_site_cache(
                    python=python,
                    cache_root=cache_root,
                    site_dir=site_dir,
                    cache_key_value=cache_key_value,
                    wheel=wheel,
                    wheel_sha256=wheel_digest,
                    log=log,
                )
                population_seconds = time.monotonic() - population_started
            else:
                cache_hit = True
    assert site_manifest is not None
    materialize_started = time.monotonic()
    materialize_site(site_dir, output)
    materialize_seconds = time.monotonic() - materialize_started
    return {
        "protocol_version": "engine-wheel-site-cache-v1",
        "cache_hit": cache_hit,
        "cache_entry": str(site_dir),
        "output_site": str(output),
        "wheel_sha256": wheel_digest,
        "tree_sha256": site_manifest["tree_sha256"],
        "file_count": site_manifest["file_count"],
        "timing_seconds": {
            "lock_wait": round(lock_wait_seconds, 6),
            "population": round(population_seconds, 6),
            "materialize": round(materialize_seconds, 6),
            "total": round(time.monotonic() - started, 6),
        },
    }


def populate_site_cache(
    *,
    python: Path,
    cache_root: Path,
    site_dir: Path,
    cache_key_value: str,
    wheel: Path,
    wheel_sha256: str,
    log: Path,
) -> dict[str, Any]:
    temp_root = Path(
        tempfile.mkdtemp(prefix=f".site.{cache_key_value}.", dir=str(cache_root))
    )
    temp_site = temp_root / "site"
    log.parent.mkdir(parents=True, exist_ok=True)
    try:
        completed = subprocess.run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--target",
                str(temp_site),
                str(wheel),
            ],
            check=False,
            text=True,
            capture_output=True,
            **hidden_subprocess_kwargs(),
        )
        log.write_text(completed.stdout + completed.stderr, encoding="utf-8")
        if completed.returncode != 0:
            raise WheelCacheError(
                f"wheel site population failed rc={completed.returncode}"
            )
        remove_python_bytecode(temp_site)
        digest, file_count = site_tree_digest(temp_site)
        manifest = {
            "protocol_version": "engine-wheel-site-cache-v1",
            "cache_key": cache_key_value,
            "wheel_sha256": wheel_sha256,
            "tree_sha256": digest,
            "file_count": file_count,
        }
        (temp_site / "site_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        make_tree_read_only(temp_site)
        if site_dir.exists():
            make_tree_writable(site_dir)
            shutil.rmtree(site_dir)
        os.replace(temp_site, site_dir)
    finally:
        if temp_root.exists():
            shutil.rmtree(temp_root, ignore_errors=True)
    result = valid_cached_site(
        site_dir,
        cache_key_value=cache_key_value,
        wheel_sha256=wheel_sha256,
    )
    if result is None:
        raise WheelCacheError("published wheel site cache failed validation")
    return result


def valid_cached_site(
    site_dir: Path, *, cache_key_value: str, wheel_sha256: str
) -> dict[str, Any] | None:
    manifest_path = site_dir / "site_manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict):
        return None
    if (
        manifest.get("protocol_version") != "engine-wheel-site-cache-v1"
        or manifest.get("cache_key") != cache_key_value
        or manifest.get("wheel_sha256") != wheel_sha256
    ):
        return None
    digest, count = site_tree_digest(site_dir, exclude_manifest=True)
    if digest != manifest.get("tree_sha256") or count != manifest.get("file_count"):
        return None
    return manifest


def materialize_site(site_dir: Path, output: Path) -> None:
    if output.exists():
        # Linux jobs hard-link immutable cache files into py_site. Changing their
        # mode here would also mutate the shared cache inode; writable cleanup is
        # only needed for the copied Windows materialization.
        if os.name == "nt":
            make_tree_writable(output)
        shutil.rmtree(output)
    output.mkdir(parents=True)
    for path in sorted(site_dir.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(site_dir)
        destination = output / relative
        if path.is_dir():
            destination.mkdir(exist_ok=True)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            try:
                os.link(path, destination)
                continue
            except OSError:
                pass
        shutil.copy2(path, destination)


def site_tree_digest(root: Path, *, exclude_manifest: bool = False) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if exclude_manifest and relative == "site_manifest.json":
            continue
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_sha256(path).encode("ascii"))
        digest.update(b"\0")
        count += 1
    return digest.hexdigest(), count


def remove_python_bytecode(root: Path) -> None:
    for path in sorted(root.rglob("__pycache__"), reverse=True):
        shutil.rmtree(path, ignore_errors=True)
    for pattern in ("*.pyc", "*.pyo"):
        for path in root.rglob(pattern):
            path.unlink(missing_ok=True)


def make_tree_read_only(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            path.chmod(0o444)


def make_tree_writable(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            try:
                path.chmod(0o644)
            except OSError:
                pass


def adapter_source_digest(source: Path) -> str:
    paths = [source / "setup.py"]
    extension = source / "extension"
    if extension.is_dir():
        paths.extend(
            path
            for path in extension.rglob("*")
            if path.is_file() and path.suffix.lower() in ADAPTER_SUFFIXES
        )
    digest = hashlib.sha256()
    for path in sorted(set(paths), key=lambda item: item.relative_to(source).as_posix()):
        if not path.is_file():
            continue
        relative = path.relative_to(source).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def runtime_identity(python: Path, *, target_arch: str) -> dict[str, str]:
    probe = (
        "import importlib.metadata as im,json,platform,sys,sysconfig; "
        "\ndef version(*names):\n"
        " for name in names:\n"
        "  try: return im.version(name)\n"
        "  except im.PackageNotFoundError: pass\n"
        " return 'missing'\n"
        "data={'python':platform.python_version(),"
        "'soabi':str(sysconfig.get_config_var('SOABI') or ''),"
        "'platform':sysconfig.get_platform()};\n"
        "data['torch']=version('torch');\n"
        "data['torch_npu']=version('torch-npu','torch_npu');\n"
        "print(json.dumps(data,sort_keys=True))"
    )
    completed = subprocess.run(
        [str(python), "-c", probe],
        check=False,
        text=True,
        capture_output=True,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        raise WheelCacheError(
            f"python identity probe failed rc={completed.returncode}: {completed.stderr.strip()}"
        )
    try:
        identity = json.loads(completed.stdout.strip())
    except json.JSONDecodeError as exc:
        raise WheelCacheError("python identity probe returned invalid JSON") from exc
    if not isinstance(identity, dict):
        raise WheelCacheError("python identity probe must return an object")
    identity.update(
        {
            "python_executable": str(python),
            "cann_home": str(os.environ.get("ASCEND_HOME_PATH", "")),
            "cann_aicpu": str(os.environ.get("ASCEND_AICPU_PATH", "")),
            "target_arch": str(target_arch),
        }
    )
    return {str(key): str(value) for key, value in identity.items()}


def cache_key(adapter_digest: str, identity: dict[str, str]) -> str:
    payload = json.dumps(
        {"adapter_digest": adapter_digest, "identity": identity},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def populate_cache(
    source: Path,
    python: Path,
    cache_root: Path,
    cache_dir: Path,
    key: str,
    identity: dict[str, str],
) -> Path:
    shutil.rmtree(source / "build", ignore_errors=True)
    shutil.rmtree(source / "dist", ignore_errors=True)
    completed = subprocess.run(
        [str(python), "setup.py", "build", "bdist_wheel"],
        cwd=str(source),
        check=False,
        text=True,
        capture_output=True,
        **hidden_subprocess_kwargs(),
    )
    (source / "wheel_build.log").write_text(
        completed.stdout + completed.stderr,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise WheelCacheError(f"wheel build failed rc={completed.returncode}")
    wheels = sorted((source / "dist").glob("custom_ops*.whl"))
    if len(wheels) != 1:
        raise WheelCacheError(f"expected one custom_ops wheel, found {len(wheels)}")
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=str(cache_root)))
    try:
        cached_wheel = temp_dir / wheels[0].name
        shutil.copy2(wheels[0], cached_wheel)
        manifest = {
            "cache_key": key,
            "identity": identity,
            "wheel": cached_wheel.name,
            "wheel_sha256": file_sha256(cached_wheel),
        }
        (temp_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        os.replace(temp_dir, cache_dir)
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
    wheel = valid_cached_wheel(cache_dir, key)
    if wheel is None:
        raise WheelCacheError("published wheel cache failed validation")
    return wheel


def valid_cached_wheel(cache_dir: Path, key: str) -> Path | None:
    manifest_path = cache_dir / "manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict) or manifest.get("cache_key") != key:
        return None
    wheel = cache_dir / str(manifest.get("wheel") or "")
    if not wheel.is_file():
        return None
    if file_sha256(wheel) != str(manifest.get("wheel_sha256") or ""):
        return None
    return wheel


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def file_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve an immutable AscendOP test wheel")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--site-output", type=Path)
    parser.add_argument("--site-log", type=Path)
    parser.add_argument("--target-arch", default="910B4")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = resolve_wheel(
            args.source,
            args.python,
            args.cache_root,
            args.output_dir,
            target_arch=args.target_arch,
            site_output=args.site_output,
            site_log=args.site_log,
        )
    except WheelCacheError as exc:
        print(f"wheel cache failed: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    else:
        print(result["output_wheel"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
