from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from limited_remote_partner.resources.operator_cache import cann_identity
from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs
from limited_remote_partner.engine.test_engine import atomic_write_json, utc_now
from limited_remote_partner.resources.wheel_cache import file_lock


PROTOCOL_VERSION = "engine-runtime-readiness-v1"


class RuntimeReadinessError(RuntimeError):
    pass


def resolve_runtime_readiness(
    *,
    python_bin: Path,
    cache_root: Path,
    receipt: Path,
    force_refresh: bool = False,
) -> dict[str, Any]:
    total_started = time.monotonic()
    # A virtualenv Python is commonly a symlink to the system interpreter.
    # Resolving it before execution discards the virtualenv launch path and
    # therefore its site-packages.
    python_bin = Path(os.path.abspath(os.fspath(python_bin)))
    cache_root = cache_root.resolve()
    receipt = receipt.resolve()
    if not python_bin.is_file():
        raise RuntimeReadinessError(f"runtime Python is missing: {python_bin}")

    identity_started = time.monotonic()
    identity = build_identity(python_bin)
    identity_seconds = time.monotonic() - identity_started
    key = cache_key(identity)
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_dir = cache_root / key

    probe_started = time.monotonic()
    cached = None if force_refresh else valid_cache_entry(cache_dir, key, identity)
    probe_seconds = time.monotonic() - probe_started
    cache_hit = cached is not None
    lock_wait_seconds = 0.0
    verification_seconds = 0.0
    if cached is None:
        lock_started = time.monotonic()
        with file_lock(cache_root / f"{key}.lock"):
            lock_wait_seconds = time.monotonic() - lock_started
            cached = None if force_refresh else valid_cache_entry(cache_dir, key, identity)
            if cached is None:
                verification_started = time.monotonic()
                verification = verify_runtime_imports(python_bin)
                verification_seconds = time.monotonic() - verification_started
                cached = publish_cache_entry(
                    cache_root=cache_root,
                    cache_dir=cache_dir,
                    key=key,
                    identity=identity,
                    verification=verification,
                )
            else:
                cache_hit = True

    result = {
        "protocol_version": PROTOCOL_VERSION,
        "state": "ready",
        "cache_key": key,
        "cache_hit": cache_hit,
        "cache_entry": str(cache_dir),
        "identity": identity,
        "verification": cached["verification"],
        "timing_seconds": {
            "identity": round(identity_seconds, 6),
            "initial_probe": round(probe_seconds, 6),
            "lock_wait": round(lock_wait_seconds, 6),
            "verification": round(verification_seconds, 6),
            "total": round(time.monotonic() - total_started, 6),
        },
        "finished_at": utc_now(),
    }
    receipt.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(receipt, result)
    return result


def build_identity(python_bin: Path) -> dict[str, Any]:
    stat = python_bin.stat()
    script = r"""
import importlib.metadata as metadata
import importlib.util
import json
import os
import pathlib
import platform
import sys

def package_version(*names):
    for name in names:
        try:
            return metadata.version(name)
        except metadata.PackageNotFoundError:
            pass
    return "missing"

def module_identity(name):
    spec = importlib.util.find_spec(name)
    if spec is None:
        return {"present": False}
    result = {"present": True, "origin": str(spec.origin or "")}
    if spec.origin and os.path.isfile(spec.origin):
        stat = os.stat(spec.origin)
        result.update({"size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    return result

print(json.dumps({
    "python_executable": os.path.realpath(sys.executable),
    "python_prefix": sys.prefix,
    "python_base_prefix": sys.base_prefix,
    "python_implementation": platform.python_implementation(),
    "python_version": platform.python_version(),
    "python_path": list(sys.path),
    "pythonpath_env": os.environ.get("PYTHONPATH", ""),
    "packages": {
        "torch": package_version("torch"),
        "torch_npu": package_version("torch-npu", "torch_npu"),
    },
    "modules": {
        "torch": module_identity("torch"),
        "torch_npu": module_identity("torch_npu"),
    },
}, sort_keys=True))
"""
    completed = subprocess.run(
        [str(python_bin), "-c", script],
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        raise RuntimeReadinessError(
            "runtime identity probe failed: "
            f"rc={completed.returncode} output={(completed.stderr or completed.stdout)[-1000:]}"
        )
    try:
        python = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeReadinessError("runtime identity probe returned invalid JSON") from exc
    modules = python.get("modules", {}) if isinstance(python, dict) else {}
    missing = [name for name in ("torch", "torch_npu") if not modules.get(name, {}).get("present")]
    if missing:
        raise RuntimeReadinessError(
            "runtime modules are missing: "
            + ", ".join(missing)
            + "; identity="
            + json.dumps(python, ensure_ascii=True, sort_keys=True)[-3000:]
        )
    return {
        "protocol_version": PROTOCOL_VERSION,
        "boot_id": boot_id(),
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "kernel_release": platform.release(),
        "python": {
            "invocation_path": str(python_bin),
            "realpath": os.path.realpath(python_bin),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            **python,
        },
        "cann": cann_identity(),
    }


def boot_id() -> str:
    path = Path("/proc/sys/kernel/random/boot_id")
    if path.is_file():
        value = path.read_text(encoding="utf-8", errors="replace").strip()
        if value:
            return value
    return f"{platform.node()}:{platform.release()}"


def verify_runtime_imports(python_bin: Path) -> dict[str, Any]:
    script = (
        "import json,torch,torch_npu;"
        "print(json.dumps({'torch':str(torch.__version__),"
        "'torch_npu':str(getattr(torch_npu,'__version__','unknown'))},sort_keys=True))"
    )
    completed = subprocess.run(
        [str(python_bin), "-c", script],
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        raise RuntimeReadinessError(
            "torch/torch_npu import verification failed: "
            f"rc={completed.returncode} output={(completed.stderr or completed.stdout)[-1500:]}"
        )
    try:
        versions = json.loads(completed.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        raise RuntimeReadinessError(
            "torch/torch_npu import verification returned invalid JSON"
        ) from exc
    return {
        "state": "verified",
        "versions": versions,
        "verified_at": utc_now(),
    }


def cache_key(identity: dict[str, Any]) -> str:
    payload = json.dumps(
        identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def publish_cache_entry(
    *,
    cache_root: Path,
    cache_dir: Path,
    key: str,
    identity: dict[str, Any],
    verification: dict[str, Any],
) -> dict[str, Any]:
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=str(cache_root)))
    try:
        manifest = {
            "protocol_version": PROTOCOL_VERSION,
            "state": "complete",
            "cache_key": key,
            "identity": identity,
            "verification": verification,
            "created_at": utc_now(),
        }
        atomic_write_json(temp_dir / "manifest.json", manifest)
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        os.replace(temp_dir, cache_dir)
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
    result = valid_cache_entry(cache_dir, key, identity)
    if result is None:
        raise RuntimeReadinessError("published runtime readiness cache is invalid")
    return result


def valid_cache_entry(
    cache_dir: Path, key: str, identity: dict[str, Any]
) -> dict[str, Any] | None:
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
        or manifest.get("identity") != identity
        or not isinstance(manifest.get("verification"), dict)
        or manifest["verification"].get("state") != "verified"
    ):
        return None
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cache a boot- and environment-bound torch/torch_npu import probe"
    )
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = resolve_runtime_readiness(
            python_bin=args.python,
            cache_root=args.cache_root,
            receipt=args.receipt,
            force_refresh=args.force_refresh,
        )
    except (OSError, RuntimeReadinessError, ValueError) as exc:
        print(f"RUNTIME_READINESS_ERROR: {exc}")
        return 1
    if args.json:
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    else:
        print(
            "RUNTIME_READINESS_OK "
            f"key={result['cache_key']} cache_hit={str(result['cache_hit']).lower()}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
