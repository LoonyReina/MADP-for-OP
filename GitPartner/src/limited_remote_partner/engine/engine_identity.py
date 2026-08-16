from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
from pathlib import Path
from typing import Any

from limited_remote_partner.engine.test_engine import (
    atomic_write_json,
    hidden_process_creation_flags,
    hidden_process_startup_info,
    utc_now,
)


class EngineIdentityError(RuntimeError):
    pass


def tree_digest(root: Path) -> str:
    root = root.resolve()
    if not root.is_dir():
        raise EngineIdentityError(f"identity tree is missing: {root}")
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise EngineIdentityError(f"identity tree contains symlink: {path}")
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


def environment_manifest(python_bin: str) -> dict[str, Any]:
    script = """
import importlib.metadata as metadata
import json
import os
import platform
import sys

versions = {}
for label, names in {
    "numpy": ("numpy",),
    "torch": ("torch",),
    "torch_npu": ("torch-npu", "torch_npu"),
}.items():
    versions[label] = "missing"
    for name in names:
        try:
            versions[label] = metadata.version(name)
            break
        except metadata.PackageNotFoundError:
            pass
print(json.dumps({
    "python_executable": os.path.realpath(sys.executable),
    "python_implementation": platform.python_implementation(),
    "python_version": platform.python_version(),
    "packages": versions,
}, sort_keys=True))
"""
    completed = subprocess.run(
        [python_bin, "-c", script],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        creationflags=hidden_process_creation_flags(),
        startupinfo=hidden_process_startup_info(),
        check=False,
    )
    if completed.returncode != 0:
        raise EngineIdentityError(
            f"python environment probe failed rc={completed.returncode}: "
            f"{(completed.stderr or completed.stdout)[-1000:]}"
        )
    try:
        python = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise EngineIdentityError("python environment probe returned invalid JSON") from exc
    if not isinstance(python, dict):
        raise EngineIdentityError("python environment probe did not return an object")
    cann_paths = {
        name: canonical_environment_path(value)
        for name in (
            "ASCEND_HOME_PATH",
            "ASCEND_AICPU_PATH",
            "ASCEND_OPP_PATH",
            "ASCEND_CUSTOM_OPP_PATH",
        )
        if (value := os.environ.get(name, ""))
    }
    return {
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "kernel_release": platform.release(),
        "python": python,
        "cann_paths": cann_paths,
    }


def canonical_environment_path(value: str) -> str:
    """Remove the engine job id from otherwise equivalent runtime paths."""

    resolved = Path(os.path.realpath(value))
    job_root_raw = os.environ.get("ASCENDOP_ENGINE_JOB_ROOT", "").strip()
    if not job_root_raw:
        return str(resolved)
    job_root = Path(os.path.realpath(job_root_raw))
    try:
        relative = resolved.relative_to(job_root)
    except ValueError:
        return str(resolved)
    suffix = relative.as_posix()
    return "$ASCENDOP_ENGINE_JOB_ROOT" + (f"/{suffix}" if suffix else "")


def build_identity(
    *,
    test_version: str,
    source: Path,
    task_case: Path,
    attack_case: Path | None,
    python_bin: str,
    test_contract_sha256: str = "",
    correctness_case_count: int = 0,
    performance_case_count: int = 0,
    correctness_repetitions: int = 0,
    performance_samples_per_case: int = 0,
) -> dict[str, Any]:
    environment = environment_manifest(python_bin)
    environment_bytes = json.dumps(
        environment, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    case_root = attack_case if attack_case is not None and attack_case.is_dir() else task_case
    identity = {
        "protocol_version": "engine-runtime-identity-v1",
        "generated_at": utc_now(),
        "test_version": test_version,
        "source_sha256": tree_digest(source),
        "case_bundle_sha256": tree_digest(case_root),
        "golden_bundle_sha256": tree_digest(task_case),
        "environment_sha256": hashlib.sha256(environment_bytes).hexdigest(),
        "environment": environment,
    }
    if test_contract_sha256:
        identity.update(
            {
                "test_contract_sha256": test_contract_sha256,
                "correctness_case_count": correctness_case_count,
                "performance_case_count": performance_case_count,
                "correctness_repetitions": correctness_repetitions,
                "performance_samples_per_case": performance_samples_per_case,
            }
        )
    return identity


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Record reproducible AscendOP runtime identity")
    parser.add_argument("--test-version", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--task-case", required=True)
    parser.add_argument("--attack-case")
    parser.add_argument("--python-bin", required=True)
    parser.add_argument("--test-contract-sha256", default="")
    parser.add_argument("--correctness-case-count", type=int, default=0)
    parser.add_argument("--performance-case-count", type=int, default=0)
    parser.add_argument("--correctness-repetitions", type=int, default=0)
    parser.add_argument("--performance-samples-per-case", type=int, default=0)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    attack = Path(args.attack_case) if args.attack_case else None
    try:
        identity = build_identity(
            test_version=args.test_version,
            source=Path(args.source),
            task_case=Path(args.task_case),
            attack_case=attack,
            python_bin=args.python_bin,
            test_contract_sha256=args.test_contract_sha256,
            correctness_case_count=args.correctness_case_count,
            performance_case_count=args.performance_case_count,
            correctness_repetitions=args.correctness_repetitions,
            performance_samples_per_case=args.performance_samples_per_case,
        )
        atomic_write_json(Path(args.output), identity)
    except (EngineIdentityError, OSError, ValueError) as exc:
        print(f"ENGINE_IDENTITY_ERROR: {exc}")
        return 1
    print(json.dumps(identity, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
