from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DAEMON_SOURCE = Path(__file__).resolve().parents[2]
if str(DAEMON_SOURCE) not in sys.path:
    sys.path.insert(0, str(DAEMON_SOURCE))

try:
    from ascendop_daemon.runtime.process_adapter import (
        process_creation_flags,
        process_startupinfo,
    )
except ModuleNotFoundError:
    def process_creation_flags() -> int:
        if sys.platform == "win32":
            return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
        return 0

    def process_startupinfo() -> subprocess.STARTUPINFO | None:
        if sys.platform != "win32":
            return None
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        return startupinfo


class TransportInstallError(RuntimeError):
    pass


def transport_generation(source_root: Path) -> str:
    source_root = source_root.resolve()
    package_root = source_root / "limited_remote_partner"
    paths = sorted(package_root.rglob("*.py"), key=lambda item: item.as_posix())
    if not paths:
        raise TransportInstallError(
            f"GitPartner package source is missing: {package_root}"
        )
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(source_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        data = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def install_transport_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    worktree: Path,
    config_relative: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    receipt_path: Path,
    protocol_source: Path | None = None,
    service_environment_file: Path | None = None,
    cann_environment_script: Path | None = None,
    engine_code_generation: str = "",
    release_generation: str = "",
    service_role: str = "client",
    deployment_mode: str = "authorized-direct-ssh-atomic-maintenance",
    git_operation_timeout_seconds: int = 60,
    git_operation_lock_timeout_seconds: int = 60,
) -> dict[str, Any]:
    archive = archive.resolve()
    runtime_root = runtime_root.resolve()
    worktree = worktree.resolve()
    receipt_path = receipt_path.resolve()
    protocol_source = protocol_source.resolve() if protocol_source else None
    service_environment_file = (
        service_environment_file.resolve() if service_environment_file else None
    )
    cann_environment_script = (
        cann_environment_script.resolve() if cann_environment_script else None
    )
    if protocol_source is not None and not (
        protocol_source / "ascendop_protocol"
    ).is_dir():
        raise TransportInstallError(
            f"shared protocol source is missing: {protocol_source}"
        )
    if service_environment_file is not None:
        if protocol_source is None or cann_environment_script is None:
            raise TransportInstallError(
                "persistent service environment requires protocol and CANN sources"
            )
        if not cann_environment_script.is_file():
            raise TransportInstallError(
                f"CANN environment script is missing: {cann_environment_script}"
            )
    _validate_digest(expected_generation, 64, "transport generation")
    _validate_digest(expected_archive_sha256, 64, "archive SHA-256")
    if release_generation:
        _validate_digest(release_generation, 64, "release generation")
    if engine_code_generation:
        _validate_digest(engine_code_generation, 16, "Engine code generation")
    if service_role not in {"client", "server"}:
        raise TransportInstallError(
            f"service role must be client or server: {service_role!r}"
        )
    if deployment_mode not in {
        "authorized-direct-ssh-atomic-maintenance",
        "relay-gateway-atomic-maintenance",
    }:
        raise TransportInstallError(
            f"unsupported deployment mode: {deployment_mode!r}"
        )
    git_operation_timeout_seconds = max(15, int(git_operation_timeout_seconds))
    git_operation_lock_timeout_seconds = max(
        5,
        int(git_operation_lock_timeout_seconds),
    )
    installation_generation = release_generation or expected_generation
    if not archive.is_file():
        raise TransportInstallError(f"transport archive is missing: {archive}")
    archive_sha256 = _file_digest(archive)
    if archive_sha256 != expected_archive_sha256:
        raise TransportInstallError(
            "transport archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={archive_sha256}"
        )

    generations_root = runtime_root / "generations"
    generations_root.mkdir(parents=True, exist_ok=True)
    generation_root = generations_root / installation_generation
    if generation_root.is_dir():
        actual_generation = transport_generation(generation_root / "src")
        if actual_generation != expected_generation:
            raise TransportInstallError(
                "immutable transport generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
    else:
        staging = Path(
            tempfile.mkdtemp(
                prefix=".gp-",
                dir=generations_root,
            )
        )
        try:
            _safe_extract(archive, staging)
            actual_generation = transport_generation(staging / "src")
            if actual_generation != expected_generation:
                raise TransportInstallError(
                    "staged transport generation mismatch: "
                    f"expected={expected_generation} actual={actual_generation}"
                )
            if not (staging / config_relative).is_file():
                raise TransportInstallError(
                    f"staged transport config is missing: {config_relative}"
                )
            os.replace(staging, generation_root)
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    pointer_path = runtime_root / "current"
    previous_generation = _read_pointer(pointer_path)
    previous_root = (
        generations_root / previous_generation if previous_generation else None
    )
    new_config = generation_root / config_relative
    if not new_config.is_file():
        raise TransportInstallError(
            f"installed transport config is missing: {new_config}"
        )

    switched = False
    previous_service_environment = _read_optional_file(service_environment_file)
    try:
        stop_result: dict[str, Any] = {}
        if previous_root is not None and previous_root.is_dir():
            previous_config = previous_root / config_relative
            if previous_config.is_file():
                stop_result = _service_command(
                    # The incoming maintenance runtime owns upgrade semantics.
                    # Old runtimes may reject passive stop/status when their
                    # enrollment snapshot is stale, which can strand a
                    # release half-switched. The previous config still fences
                    # the exact service and process identity being stopped.
                    source_root=generation_root / "src",
                    protocol_source=protocol_source,
                    worktree=worktree,
                    config_path=previous_config,
                    action="stop",
                    service_role=service_role,
                    git_operation_timeout_seconds=git_operation_timeout_seconds,
                    git_operation_lock_timeout_seconds=(
                        git_operation_lock_timeout_seconds
                    ),
                )
        _write_pointer(pointer_path, installation_generation)
        switched = True
        if service_environment_file is not None:
            _write_service_environment(
                service_environment_file,
                worktree=worktree,
                source_root=generation_root / "src",
                protocol_source=protocol_source,
                config_path=new_config,
                cann_environment_script=cann_environment_script,
                engine_code_generation=engine_code_generation,
                git_operation_timeout_seconds=git_operation_timeout_seconds,
                git_operation_lock_timeout_seconds=(
                    git_operation_lock_timeout_seconds
                ),
            )
        start_result = _service_command(
            source_root=generation_root / "src",
            protocol_source=protocol_source,
            worktree=worktree,
            config_path=new_config,
            action="start",
            service_role=service_role,
            force_restart=True,
            engine_code_generation=engine_code_generation,
            git_operation_timeout_seconds=git_operation_timeout_seconds,
            git_operation_lock_timeout_seconds=(
                git_operation_lock_timeout_seconds
            ),
        )
        status_result = _service_command(
            source_root=generation_root / "src",
            protocol_source=protocol_source,
            worktree=worktree,
            config_path=new_config,
            action="status",
            service_role=service_role,
            engine_code_generation=engine_code_generation,
            git_operation_timeout_seconds=git_operation_timeout_seconds,
            git_operation_lock_timeout_seconds=(
                git_operation_lock_timeout_seconds
            ),
        )
        if not (
            bool(status_result.get("running"))
            and bool(status_result.get("child_running"))
        ):
            raise TransportInstallError(
                "new GitPartner resident did not report supervisor and child alive"
            )
    except Exception as exc:
        if switched:
            if previous_generation:
                _write_pointer(pointer_path, previous_generation)
            else:
                pointer_path.unlink(missing_ok=True)
        if previous_root is not None and previous_root.is_dir():
            previous_config = previous_root / config_relative
            if previous_config.is_file():
                try:
                    _service_command(
                        source_root=previous_root / "src",
                        protocol_source=protocol_source,
                        worktree=worktree,
                        config_path=previous_config,
                        action="start",
                        service_role=service_role,
                        force_restart=True,
                        git_operation_timeout_seconds=(
                            git_operation_timeout_seconds
                        ),
                        git_operation_lock_timeout_seconds=(
                            git_operation_lock_timeout_seconds
                        ),
                    )
                except TransportInstallError:
                    pass
        _restore_optional_file(
            service_environment_file,
            previous_service_environment,
        )
        if isinstance(exc, TransportInstallError):
            raise
        raise TransportInstallError(str(exc)) from exc

    receipt = {
        "schema": "ascendop.gitpartner.deployment-receipt.v3",
        "deployment_mode": deployment_mode,
        "service_role": service_role,
        "transport_generation": expected_generation,
        "release_generation": installation_generation,
        "previous_transport_generation": previous_generation,
        "engine_code_generation": engine_code_generation,
        "git_operation_timeout_seconds": git_operation_timeout_seconds,
        "git_operation_lock_timeout_seconds": (
            git_operation_lock_timeout_seconds
        ),
        "archive_sha256": archive_sha256,
        "runtime_root": str(runtime_root),
        "runtime_config": str(new_config),
        "protocol_source": str(protocol_source) if protocol_source else "",
        "service_environment_file": (
            str(service_environment_file) if service_environment_file else ""
        ),
        "cann_environment_script": (
            str(cann_environment_script) if cann_environment_script else ""
        ),
        "stop_result": stop_result,
        "start_result": start_result,
        "service_status": status_result,
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(receipt_path, receipt)
    _write_json(generation_root / "DEPLOYMENT_RECEIPT.json", receipt)
    return receipt


def _service_command(
    *,
    source_root: Path,
    protocol_source: Path | None = None,
    worktree: Path,
    config_path: Path,
    action: str,
    service_role: str = "client",
    force_restart: bool = False,
    engine_code_generation: str = "",
    git_operation_timeout_seconds: int = 60,
    git_operation_lock_timeout_seconds: int = 60,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (
            str(source_root),
            str(protocol_source) if protocol_source else "",
            env.get("PYTHONPATH", ""),
        )
        if item
    )
    env["GITPARTNER_RUNTIME_SOURCE"] = str(source_root)
    if protocol_source:
        env["GITPARTNER_PROTOCOL_SOURCE"] = str(protocol_source)
    if engine_code_generation:
        env["ASCENDOP_EXPECTED_ENGINE_CODE_GENERATION"] = engine_code_generation
    env["GITPARTNER_GIT_TIMEOUT_SECONDS"] = str(
        max(15, int(git_operation_timeout_seconds))
    )
    env["GITPARTNER_GIT_OPERATION_LOCK_TIMEOUT_SECONDS"] = str(
        max(5, int(git_operation_lock_timeout_seconds))
    )
    argv = [
        sys.executable,
        "-m",
        "limited_remote_partner.endpoint.node_service",
        action,
        "--repo-dir",
        str(worktree),
        "--config",
        str(config_path),
        "--role",
        service_role,
        "--stop-timeout-seconds",
        "20",
    ]
    if force_restart:
        argv.append("--force-restart")
    completed = subprocess.run(
        argv,
        cwd=str(worktree),
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
        creationflags=process_creation_flags(),
        startupinfo=process_startupinfo(),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise TransportInstallError(
            f"GitPartner service {action} failed with exit "
            f"{completed.returncode}: {detail[:1024]}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TransportInstallError(
            f"GitPartner service {action} returned invalid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise TransportInstallError(
            f"GitPartner service {action} returned a non-object payload"
        )
    return value


def _safe_extract(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with tarfile.open(archive, "r:gz") as tar:
        members = tar.getmembers()
        for member in members:
            target = (destination / member.name).resolve()
            if destination != target and destination not in target.parents:
                raise TransportInstallError(
                    f"transport archive escapes destination: {member.name}"
                )
            if member.issym() or member.islnk():
                raise TransportInstallError(
                    f"transport archive contains a link: {member.name}"
                )
        tar.extractall(destination, members=members)


def _read_pointer(path: Path) -> str:
    try:
        value = path.read_text(encoding="ascii").strip().lower()
    except OSError:
        return ""
    if not value:
        return ""
    _validate_digest(value, 64, "runtime pointer")
    return value


def _write_pointer(path: Path, generation: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(generation + "\n", encoding="ascii")
    os.replace(temporary, path)


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _read_optional_file(path: Path | None) -> tuple[bytes, int] | None:
    if path is None or not path.is_file():
        return None
    return path.read_bytes(), path.stat().st_mode & 0o777


def _restore_optional_file(
    path: Path | None,
    previous: tuple[bytes, int] | None,
) -> None:
    if path is None:
        return
    if previous is None:
        path.unlink(missing_ok=True)
        return
    _write_file_atomic(path, previous[0], mode=previous[1])


def _write_service_environment(
    path: Path,
    *,
    worktree: Path,
    source_root: Path,
    protocol_source: Path,
    config_path: Path,
    cann_environment_script: Path,
    engine_code_generation: str,
    git_operation_timeout_seconds: int,
    git_operation_lock_timeout_seconds: int,
) -> None:
    values = {
        "ASCENDOP_GP_ROOT": str(worktree),
        "GITPARTNER_RUNTIME_SOURCE": str(source_root),
        "GITPARTNER_PROTOCOL_SOURCE": str(protocol_source),
        "GITPARTNER_RUNTIME_CONFIG": str(config_path),
        "ASCENDOP_CANN_ENV_SCRIPT": str(cann_environment_script),
        "ASCENDOP_EXPECTED_ENGINE_CODE_GENERATION": engine_code_generation,
        "GITPARTNER_GIT_TIMEOUT_SECONDS": str(git_operation_timeout_seconds),
        "GITPARTNER_GIT_OPERATION_LOCK_TIMEOUT_SECONDS": str(
            git_operation_lock_timeout_seconds
        ),
    }
    payload = "".join(
        f"{name}={json.dumps(value)}\n" for name, value in values.items()
    ).encode("utf-8")
    _write_file_atomic(path, payload, mode=0o600)


def _write_file_atomic(path: Path, payload: bytes, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        temporary.write_bytes(payload)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_digest(value: str, length: int, label: str) -> None:
    if len(value) != length or any(char not in "0123456789abcdef" for char in value):
        raise TransportInstallError(f"invalid {label}: {value}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Atomically install one immutable Flow V3 GitPartner runtime."
    )
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--worktree", type=Path, required=True)
    parser.add_argument("--config-relative", type=Path, required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--expected-archive-sha256", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--protocol-source", type=Path)
    parser.add_argument("--service-environment-file", type=Path)
    parser.add_argument("--cann-environment-script", type=Path)
    parser.add_argument("--engine-code-generation", default="")
    parser.add_argument("--release-generation", default="")
    parser.add_argument(
        "--service-role",
        choices=("client", "server"),
        default="client",
    )
    parser.add_argument(
        "--deployment-mode",
        choices=(
            "authorized-direct-ssh-atomic-maintenance",
            "relay-gateway-atomic-maintenance",
        ),
        default="authorized-direct-ssh-atomic-maintenance",
    )
    parser.add_argument("--git-operation-timeout-seconds", type=int, default=60)
    parser.add_argument(
        "--git-operation-lock-timeout-seconds",
        type=int,
        default=60,
    )
    args = parser.parse_args(argv)
    try:
        receipt = install_transport_runtime(
            archive=args.archive,
            runtime_root=args.runtime_root,
            worktree=args.worktree,
            config_relative=args.config_relative,
            expected_generation=args.expected_generation,
            expected_archive_sha256=args.expected_archive_sha256,
            receipt_path=args.receipt,
            protocol_source=args.protocol_source,
            service_environment_file=args.service_environment_file,
            cann_environment_script=args.cann_environment_script,
            engine_code_generation=args.engine_code_generation,
            release_generation=args.release_generation,
            service_role=args.service_role,
            deployment_mode=args.deployment_mode,
            git_operation_timeout_seconds=args.git_operation_timeout_seconds,
            git_operation_lock_timeout_seconds=(
                args.git_operation_lock_timeout_seconds
            ),
        )
    except TransportInstallError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(receipt, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
