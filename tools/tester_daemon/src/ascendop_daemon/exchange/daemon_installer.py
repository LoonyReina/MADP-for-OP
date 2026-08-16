from __future__ import annotations

import argparse
import ctypes
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
from typing import Any, Iterable


class DaemonInstallError(RuntimeError):
    pass


def daemon_generation(root: Path) -> str:
    root = root.resolve()
    paths = tuple(_daemon_files(root))
    if not paths or any(not path.is_file() for path in paths):
        raise DaemonInstallError(f"daemon package is incomplete: {root}")
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def install_daemon_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    transport_archive: Path,
    transport_runtime_root: Path,
    protocol_archive: Path,
    protocol_runtime_root: Path,
    variable_registry: Path,
    daemon_config: Path,
    system_registry: Path,
    workspace_root: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    expected_transport_generation: str,
    expected_transport_archive_sha256: str,
    expected_protocol_generation: str,
    expected_protocol_archive_sha256: str,
    expected_control_database_schema: int,
    expected_engine_code_generation: str,
    expected_variable_registry_sha256: str,
    expected_daemon_config_sha256: str,
    expected_system_registry_sha256: str,
    release_generation: str,
    receipt_path: Path,
    database: Path,
    start: bool = False,
) -> dict[str, Any]:
    archive = archive.resolve()
    runtime_root = runtime_root.resolve()
    transport_archive = transport_archive.resolve()
    transport_runtime_root = transport_runtime_root.resolve()
    protocol_archive = protocol_archive.resolve()
    protocol_runtime_root = protocol_runtime_root.resolve()
    variable_registry = variable_registry.resolve()
    daemon_config = daemon_config.resolve()
    system_registry = system_registry.resolve()
    workspace_root = workspace_root.resolve()
    receipt_path = receipt_path.resolve()
    if int(expected_control_database_schema) <= 0:
        raise DaemonInstallError("control database schema must be positive")
    for value, label in (
        (expected_generation, "daemon generation"),
        (expected_archive_sha256, "daemon archive SHA-256"),
        (expected_transport_generation, "transport generation"),
        (expected_transport_archive_sha256, "transport archive SHA-256"),
        (expected_protocol_generation, "protocol generation"),
        (expected_protocol_archive_sha256, "protocol archive SHA-256"),
        (expected_variable_registry_sha256, "variable registry SHA-256"),
        (expected_daemon_config_sha256, "daemon config SHA-256"),
        (expected_system_registry_sha256, "system registry SHA-256"),
        (release_generation, "release generation"),
    ):
        _validate_digest(value, label)
    _validate_short_generation(
        expected_engine_code_generation,
        "Engine code generation",
    )
    if not archive.is_file():
        raise DaemonInstallError(f"daemon archive is missing: {archive}")
    if not transport_archive.is_file():
        raise DaemonInstallError(
            f"transport archive is missing: {transport_archive}"
        )
    if not protocol_archive.is_file():
        raise DaemonInstallError(f"protocol archive is missing: {protocol_archive}")
    if not variable_registry.is_file():
        raise DaemonInstallError(
            f"protocol variable registry is missing: {variable_registry}"
        )
    if not daemon_config.is_file():
        raise DaemonInstallError(f"daemon config is missing: {daemon_config}")
    if not system_registry.is_file():
        raise DaemonInstallError(f"system registry is missing: {system_registry}")
    _validate_variable_registry_schema(
        variable_registry,
        expected=int(expected_control_database_schema),
    )
    actual_archive_sha256 = _file_digest(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise DaemonInstallError(
            "daemon archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={actual_archive_sha256}"
        )

    transport_source = _install_transport_product_runtime(
        archive=transport_archive,
        runtime_root=transport_runtime_root,
        expected_generation=expected_transport_generation,
        expected_archive_sha256=expected_transport_archive_sha256,
        release_generation=release_generation,
    )
    protocol_source = _install_protocol_product_runtime(
        archive=protocol_archive,
        runtime_root=protocol_runtime_root,
        expected_generation=expected_protocol_generation,
        expected_archive_sha256=expected_protocol_archive_sha256,
    )
    installed_variable_registry = _install_variable_registry(
        source=variable_registry,
        runtime_root=runtime_root / "variable-registries",
        expected_sha256=expected_variable_registry_sha256,
    )
    installed_daemon_config = _install_policy_artifact(
        source=daemon_config,
        runtime_root=runtime_root / "policies" / release_generation,
        destination_name="daemon-config.json",
        expected_sha256=expected_daemon_config_sha256,
        label="daemon config",
    )
    installed_system_registry = _install_policy_artifact(
        source=system_registry,
        runtime_root=runtime_root / "policies" / release_generation,
        destination_name="system-registry.json",
        expected_sha256=expected_system_registry_sha256,
        label="system registry",
    )

    generations = runtime_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / expected_generation
    if destination.is_dir():
        actual_generation = daemon_generation(destination)
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "immutable daemon generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
    else:
        staging = Path(tempfile.mkdtemp(prefix=".daemon-", dir=generations))
        try:
            _safe_extract(archive, staging)
            actual_generation = daemon_generation(staging)
            if actual_generation != expected_generation:
                raise DaemonInstallError(
                    "staged daemon generation mismatch: "
                    f"expected={expected_generation} actual={actual_generation}"
                )
            os.replace(staging, destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    active_path = workspace_root / ".ascendop-work" / "runtime" / "active-release.json"
    previous = _read_json(active_path)
    previous_was_running = bool(previous) and _service_running(
        workspace_root=workspace_root,
        active=previous,
        config=installed_daemon_config,
        registry=installed_system_registry,
        database=database,
    )
    if previous_was_running:
        _service_command(
            action="stop",
            workspace_root=workspace_root,
            active=previous,
            config=installed_daemon_config,
            registry=installed_system_registry,
            database=database,
        )

    active = {
        "schema": "ascendop.active-release.v3",
        "release_generation": release_generation,
        "daemon_generation": expected_generation,
        "transport_generation": expected_transport_generation,
        "protocol_generation": expected_protocol_generation,
        "engine_code_generation": expected_engine_code_generation,
        "control_database_schema": int(expected_control_database_schema),
        "daemon_source": str(destination / "src"),
        "transport_source": str(transport_source),
        "protocol_source": str(protocol_source),
        "variable_registry_path": str(installed_variable_registry),
        "daemon_config_path": str(installed_daemon_config),
        "system_registry_path": str(installed_system_registry),
        "control_database_path": str(database.resolve()),
        "activated_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_atomic(active_path, active)
    try:
        status: dict[str, Any] = {"running": False, "healthy": False}
        if start:
            status = _service_command(
                action="start",
                workspace_root=workspace_root,
                active=active,
                config=installed_daemon_config,
                registry=installed_system_registry,
                database=database,
            )
            if not bool(status.get("healthy")):
                raise DaemonInstallError("new daemon did not become healthy")
    except Exception as exc:
        rollback_error = ""
        if previous:
            _write_json_atomic(active_path, previous)
            if previous_was_running:
                try:
                    restored = _service_command(
                        action="start",
                        workspace_root=workspace_root,
                        active=previous,
                        config=installed_daemon_config,
                        registry=installed_system_registry,
                        database=database,
                    )
                    if not bool(restored.get("healthy")):
                        raise DaemonInstallError(
                            "previous daemon did not become healthy during rollback"
                        )
                except (DaemonInstallError, subprocess.TimeoutExpired) as rollback_exc:
                    rollback_error = str(rollback_exc)
        else:
            active_path.unlink(missing_ok=True)
        detail = str(exc)
        if rollback_error:
            detail += f"; rollback failed: {rollback_error}"
        raise DaemonInstallError(detail) from exc

    receipt = {
        "schema": "ascendop.daemon-deployment-receipt.v3",
        **active,
        "archive_sha256": actual_archive_sha256,
        "transport_archive_sha256": expected_transport_archive_sha256,
        "protocol_archive_sha256": expected_protocol_archive_sha256,
        "variable_registry_sha256": expected_variable_registry_sha256,
        "daemon_config_sha256": expected_daemon_config_sha256,
        "system_registry_sha256": expected_system_registry_sha256,
        "previous_release_generation": str(previous.get("release_generation") or ""),
        "service_status": status,
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_atomic(receipt_path, receipt)
    _write_json_atomic(destination / "DEPLOYMENT_RECEIPT.json", receipt)
    return receipt


def _install_transport_product_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    release_generation: str,
) -> Path:
    actual_archive_sha256 = _file_digest(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise DaemonInstallError(
            "transport archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={actual_archive_sha256}"
        )
    generations = runtime_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / release_generation
    if destination.is_dir():
        actual_generation = _transport_generation(destination / "src")
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "immutable local transport generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        return destination / "src"

    staging = Path(tempfile.mkdtemp(prefix=".transport-", dir=generations))
    try:
        _safe_extract(archive, staging)
        actual_generation = _transport_generation(staging / "src")
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "staged local transport generation mismatch: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        os.replace(staging, destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
    return destination / "src"


def _install_protocol_product_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    expected_generation: str,
    expected_archive_sha256: str,
) -> Path:
    actual_archive_sha256 = _file_digest(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise DaemonInstallError(
            "protocol archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={actual_archive_sha256}"
        )
    generations = runtime_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / expected_generation
    if destination.is_dir():
        actual_generation = _protocol_generation(destination)
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "immutable protocol generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        return destination / "src"

    staging = Path(tempfile.mkdtemp(prefix=".protocol-", dir=generations))
    try:
        _safe_extract(archive, staging)
        actual_generation = _protocol_generation(staging)
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "staged protocol generation mismatch: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        os.replace(staging, destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
    return destination / "src"


def _protocol_generation(root: Path) -> str:
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
        raise DaemonInstallError(f"protocol package is incomplete: {root}")
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _install_variable_registry(
    *,
    source: Path,
    runtime_root: Path,
    expected_sha256: str,
) -> Path:
    actual_sha256 = _file_digest(source)
    if actual_sha256 != expected_sha256:
        raise DaemonInstallError(
            "variable registry digest mismatch: "
            f"expected={expected_sha256} actual={actual_sha256}"
        )
    destination = runtime_root / expected_sha256 / "variables.json"
    if destination.is_file():
        if _file_digest(destination) != expected_sha256:
            raise DaemonInstallError(
                f"immutable variable registry is corrupted: {destination}"
            )
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        shutil.copyfile(source, temporary)
        if _file_digest(temporary) != expected_sha256:
            raise DaemonInstallError("copied variable registry digest mismatch")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _install_policy_artifact(
    *,
    source: Path,
    runtime_root: Path,
    destination_name: str,
    expected_sha256: str,
    label: str,
) -> Path:
    actual_sha256 = _file_digest(source)
    if actual_sha256 != expected_sha256:
        raise DaemonInstallError(
            f"{label} digest mismatch: expected={expected_sha256} actual={actual_sha256}"
        )
    destination = runtime_root / destination_name
    if destination.is_file():
        if _file_digest(destination) != expected_sha256:
            raise DaemonInstallError(f"immutable {label} is corrupted: {destination}")
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        shutil.copyfile(source, temporary)
        if _file_digest(temporary) != expected_sha256:
            raise DaemonInstallError(f"copied {label} digest mismatch")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _transport_generation(source_root: Path) -> str:
    package_root = source_root / "limited_remote_partner"
    paths = sorted(package_root.rglob("*.py"), key=lambda item: item.as_posix())
    if not paths:
        raise DaemonInstallError(
            f"GitPartner package source is missing: {package_root}"
        )
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(source_root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _service_running(**kwargs: Any) -> bool:
    try:
        return bool(_service_command(action="status", **kwargs).get("running"))
    except DaemonInstallError:
        workspace_root = Path(kwargs["workspace_root"])
        metadata = _read_json(
            workspace_root
            / ".ascendop-work"
            / "runtime"
            / "v3-daemon-service.json"
        )
        return _process_identity_matches(
            int(metadata.get("pid") or 0),
            str(metadata.get("start_token") or ""),
        )


def _service_command(
    *,
    action: str,
    workspace_root: Path,
    active: dict[str, Any],
    config: Path,
    registry: Path,
    database: Path,
) -> dict[str, Any]:
    daemon_source = Path(str(active.get("daemon_source") or "")).resolve()
    protocol_source = Path(str(active.get("protocol_source") or "")).resolve()
    config = Path(str(active.get("daemon_config_path") or config)).resolve()
    registry = Path(str(active.get("system_registry_path") or registry)).resolve()
    database = Path(str(active.get("control_database_path") or database)).resolve()
    if not (daemon_source / "ascendop_daemon").is_dir():
        raise DaemonInstallError(f"daemon source is missing: {daemon_source}")
    environment = os.environ.copy()
    environment["ASCENDOP_RELEASE_GENERATION"] = str(
        active.get("release_generation") or ""
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["ASCENDOP_VARIABLE_REGISTRY_PATH"] = str(
        active.get("variable_registry_path") or ""
    )
    environment["GITPARTNER_RUNTIME_SOURCE"] = str(
        active.get("transport_source") or ""
    )
    environment["GITPARTNER_PROTOCOL_SOURCE"] = str(protocol_source)
    environment["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (
            str(daemon_source),
            str(protocol_source),
            environment.get("PYTHONPATH", ""),
        )
        if item
    )
    command = [
        sys.executable,
        "-m",
        "ascendop_daemon.cli.resident_main",
        action,
        "--root",
        str(workspace_root),
        "--config",
        str(config),
        "--registry",
        str(registry),
        "--database",
        str(database),
    ]
    if action == "stop":
        command.extend(("--force", "--reason", "atomic V3 release switch"))
    completed = subprocess.run(
        command,
        cwd=workspace_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
        creationflags=(
            int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
            if os.name == "nt"
            else 0
        ),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise DaemonInstallError(
            f"daemon service {action} failed with {completed.returncode}: {detail[:1024]}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise DaemonInstallError("daemon service returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise DaemonInstallError("daemon service returned a non-object payload")
    return value


def _daemon_files(root: Path) -> Iterable[Path]:
    for name in ("pyproject.toml", "daemon.py", "launch_s5_910b.py", "manage_s5_910b.ps1"):
        yield root / name
    package = root / "src" / "ascendop_daemon"
    yield from (
        path
        for path in sorted(package.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )


def _safe_extract(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with tarfile.open(archive, "r:gz") as handle:
        members = handle.getmembers()
        for member in members:
            target = (destination / member.name).resolve()
            if target != destination and destination not in target.parents:
                raise DaemonInstallError(f"daemon archive escapes destination: {member.name}")
            if member.issym() or member.islnk():
                raise DaemonInstallError(f"daemon archive contains a link: {member.name}")
        handle.extractall(destination, members=members)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_variable_registry_schema(path: Path, *, expected: int) -> None:
    value = _read_json(path)
    variables = value.get("variables")
    if not isinstance(variables, list):
        raise DaemonInstallError("variable registry has no variables array")
    matches = [
        item
        for item in variables
        if isinstance(item, dict)
        and str(item.get("id") or "") == "database.control_schema"
    ]
    if len(matches) != 1:
        raise DaemonInstallError(
            "variable registry must define database.control_schema exactly once"
        )
    definition = matches[0]
    observed = {
        int(definition.get(field) or 0)
        for field in ("default", "minimum", "maximum")
    }
    if observed != {expected}:
        raise DaemonInstallError(
            "variable registry control schema mismatch: "
            f"expected={expected} observed={sorted(observed)}"
        )


def _process_identity_matches(pid: int, expected_start_token: str) -> bool:
    expected = str(expected_start_token or "")
    if pid <= 0 or not expected:
        return False
    if os.name != "nt":
        try:
            raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
        except (OSError, ProcessLookupError):
            return False
        _prefix, separator, suffix = raw.rpartition(")")
        fields = suffix.strip().split() if separator else []
        token = fields[19] if len(fields) > 19 else ""
        return token == expected

    class FileTime(ctypes.Structure):
        _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    open_process.restype = ctypes.c_void_p
    get_process_times = kernel32.GetProcessTimes
    get_process_times.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
    ]
    get_process_times.restype = ctypes.c_int
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    handle = open_process(0x1000, 0, pid)
    if not handle:
        return False
    creation = FileTime()
    exit_time = FileTime()
    kernel = FileTime()
    user = FileTime()
    try:
        if not get_process_times(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return False
    finally:
        close_handle(handle)
    return f"win-{creation.high:08x}{creation.low:08x}" == expected


def _validate_digest(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise DaemonInstallError(f"invalid {label}: {value}")


def _validate_short_generation(value: str, label: str) -> None:
    if len(value) != 16 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise DaemonInstallError(f"invalid {label}: {value}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install one immutable Flow V3 daemon")
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--transport-archive", type=Path, required=True)
    parser.add_argument("--transport-runtime-root", type=Path, required=True)
    parser.add_argument("--protocol-archive", type=Path, required=True)
    parser.add_argument("--protocol-runtime-root", type=Path, required=True)
    parser.add_argument("--variable-registry", type=Path, required=True)
    parser.add_argument("--daemon-config", type=Path, required=True)
    parser.add_argument("--system-registry", type=Path, required=True)
    parser.add_argument("--workspace-root", type=Path, required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--expected-archive-sha256", required=True)
    parser.add_argument("--expected-transport-generation", required=True)
    parser.add_argument("--expected-transport-archive-sha256", required=True)
    parser.add_argument("--expected-protocol-generation", required=True)
    parser.add_argument("--expected-protocol-archive-sha256", required=True)
    parser.add_argument("--expected-control-database-schema", type=int, required=True)
    parser.add_argument("--expected-engine-code-generation", required=True)
    parser.add_argument("--expected-variable-registry-sha256", required=True)
    parser.add_argument("--expected-daemon-config-sha256", required=True)
    parser.add_argument("--expected-system-registry-sha256", required=True)
    parser.add_argument("--release-generation", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--start", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = install_daemon_runtime(
            archive=args.archive,
            runtime_root=args.runtime_root,
            transport_archive=args.transport_archive,
            transport_runtime_root=args.transport_runtime_root,
            protocol_archive=args.protocol_archive,
            protocol_runtime_root=args.protocol_runtime_root,
            variable_registry=args.variable_registry,
            daemon_config=args.daemon_config,
            system_registry=args.system_registry,
            workspace_root=args.workspace_root,
            expected_generation=args.expected_generation,
            expected_archive_sha256=args.expected_archive_sha256,
            expected_transport_generation=args.expected_transport_generation,
            expected_transport_archive_sha256=(
                args.expected_transport_archive_sha256
            ),
            expected_protocol_generation=args.expected_protocol_generation,
            expected_protocol_archive_sha256=(
                args.expected_protocol_archive_sha256
            ),
            expected_control_database_schema=args.expected_control_database_schema,
            expected_engine_code_generation=args.expected_engine_code_generation,
            expected_variable_registry_sha256=(
                args.expected_variable_registry_sha256
            ),
            expected_daemon_config_sha256=args.expected_daemon_config_sha256,
            expected_system_registry_sha256=args.expected_system_registry_sha256,
            release_generation=args.release_generation,
            receipt_path=args.receipt,
            database=args.database,
            start=args.start,
        )
    except DaemonInstallError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
