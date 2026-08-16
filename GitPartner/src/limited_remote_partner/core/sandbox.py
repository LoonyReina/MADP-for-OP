from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from limited_remote_partner.core.config import AppConfig, SandboxProfileConfig
from limited_remote_partner.core.request import ExecutionRequest


@dataclass(frozen=True)
class SandboxCommand:
    command: tuple[str, ...]
    profile_name: str
    requested_backend: str
    active_backend: str
    warnings: tuple[str, ...] = field(default_factory=tuple)


def build_sandbox_command(
    config: AppConfig,
    request: ExecutionRequest,
    profile_name: str | None,
    cwd: Path,
    env: dict[str, str],
) -> SandboxCommand:
    selected_profile = profile_name or request.sandbox_profile or config.sandbox.default_profile
    profile = config.sandbox.profiles.get(
        selected_profile,
        config.sandbox.profiles.get("process", SandboxProfileConfig()),
    )

    if not config.sandbox.enabled or profile.backend == "process":
        return SandboxCommand(
            command=tuple(request.command),
            profile_name=selected_profile,
            requested_backend=profile.backend,
            active_backend="process",
        )

    if profile.backend == "systemd":
        return _systemd_command(config, request, selected_profile, profile, env)

    if profile.backend == "bubblewrap":
        return _bubblewrap_command(config, request, selected_profile, profile, cwd)

    return SandboxCommand(
        command=tuple(request.command),
        profile_name=selected_profile,
        requested_backend=profile.backend,
        active_backend="process",
        warnings=(f"unknown sandbox backend {profile.backend}; using process",),
    )


def sandbox_status(sandbox: SandboxCommand) -> dict[str, object]:
    return {
        "profile": sandbox.profile_name,
        "requested_backend": sandbox.requested_backend,
        "active_backend": sandbox.active_backend,
        "warnings": list(sandbox.warnings),
    }


def _systemd_command(
    config: AppConfig,
    request: ExecutionRequest,
    profile_name: str,
    profile: SandboxProfileConfig,
    env: dict[str, str],
) -> SandboxCommand:
    executable = _resolve_executable(config.sandbox.systemd_run_path)
    if not executable:
        return _missing_backend_command(
            request,
            profile_name,
            requested_backend="systemd",
            executable=config.sandbox.systemd_run_path,
            required=profile.require_backend or not config.sandbox.allow_backend_fallback,
        )

    command = [
        executable,
        "--user",
        "--quiet",
        "--same-dir",
        "--wait",
        "--collect",
        "--pipe",
    ]
    for prop in profile.systemd_properties:
        command.append(f"--property={prop}")
    for key, value in _systemd_environment(env):
        command.append(f"--setenv={key}={value}")
    command.append("--")
    command.extend(request.command)
    return SandboxCommand(
        command=tuple(command),
        profile_name=profile_name,
        requested_backend="systemd",
        active_backend="systemd",
    )


def _bubblewrap_command(
    config: AppConfig,
    request: ExecutionRequest,
    profile_name: str,
    profile: SandboxProfileConfig,
    cwd: Path,
) -> SandboxCommand:
    executable = _resolve_executable(config.sandbox.bubblewrap_path)
    if not executable:
        return _missing_backend_command(
            request,
            profile_name,
            requested_backend="bubblewrap",
            executable=config.sandbox.bubblewrap_path,
            required=profile.require_backend or not config.sandbox.allow_backend_fallback,
        )

    command = [
        executable,
        "--die-with-parent",
        "--ro-bind",
        "/",
        "/",
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
    ]
    if not profile.network:
        command.append("--unshare-net")

    mounted_destinations: set[str] = set()
    for path in _expanded_existing_paths(profile.readonly_paths):
        if path in mounted_destinations:
            continue
        command.extend(_bubblewrap_readonly_bind_mount_args(path, path))
        mounted_destinations.add(path)
    tmp_source, tmp_destination = _tmp_bind_mount_pair()
    command.extend(_bubblewrap_bind_mount_args(tmp_source, tmp_destination))
    mounted_destinations.add(tmp_destination)
    writable_paths = profile.writable_paths or config.sandbox.default_bubblewrap_writable_paths
    for path in _expanded_existing_paths(writable_paths):
        if path in mounted_destinations:
            continue
        command.extend(_bubblewrap_bind_mount_args(path, path))
        mounted_destinations.add(path)

    command.extend(["--chdir", _resolved_existing_path(cwd)])
    command.extend(request.command)
    return SandboxCommand(
        command=tuple(command),
        profile_name=profile_name,
        requested_backend="bubblewrap",
        active_backend="bubblewrap",
    )


def _missing_backend_command(
    request: ExecutionRequest,
    profile_name: str,
    requested_backend: str,
    executable: str,
    required: bool,
) -> SandboxCommand:
    message = (
        f"sandbox backend {requested_backend} requested by profile {profile_name} "
        f"but executable was not found: {executable}"
    )
    if required:
        return SandboxCommand(
            command=(
                "bash",
                "-lc",
                f"printf '%s\\n' {message!r} >&2; exit 126",
            ),
            profile_name=profile_name,
            requested_backend=requested_backend,
            active_backend="missing",
            warnings=(message,),
        )
    return SandboxCommand(
        command=tuple(request.command),
        profile_name=profile_name,
        requested_backend=requested_backend,
        active_backend="process",
        warnings=(f"{message}; falling back to process backend",),
    )


def _resolve_executable(path_or_name: str) -> str | None:
    path = Path(path_or_name)
    if path.is_absolute():
        return str(path) if path.exists() else None
    return shutil.which(path_or_name)


def _tmp_bind_mount_args() -> list[str]:
    source, destination = _tmp_bind_mount_pair()
    return _bubblewrap_bind_mount_args(source, destination)


def _tmp_bind_mount_pair() -> tuple[str, str]:
    if os.name == "nt":
        return ("/tmp", "/tmp")

    tmp_path = Path("/tmp")
    try:
        resolved = tmp_path.resolve(strict=True)
    except OSError:
        resolved = tmp_path

    target = str(resolved)
    if target.startswith("/") and target != "/tmp":
        return (target, target)
    return ("/tmp", "/tmp")


def _bubblewrap_bind_mount_args(source: str, destination: str) -> list[str]:
    return _bubblewrap_mount_args("--bind", source, destination)


def _bubblewrap_readonly_bind_mount_args(source: str, destination: str) -> list[str]:
    return _bubblewrap_mount_args("--ro-bind", source, destination)


def _bubblewrap_mount_args(
    mount_option: str,
    source: str,
    destination: str,
) -> list[str]:
    args: list[str] = []
    for directory in _destination_directories(destination):
        args.extend(["--dir", directory])
    args.extend([mount_option, source, destination])
    return args


def _destination_directories(destination: str) -> list[str]:
    normalized = destination.replace("\\", "/").strip("/")
    if not normalized:
        return []
    directories: list[str] = []
    current = ""
    for part in normalized.split("/"):
        current += "/" + part
        directories.append(current)
    return directories


def _systemd_environment(env: dict[str, str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for key, value in sorted(env.items()):
        if not key or not key.replace("_", "").isalnum():
            continue
        upper_key = key.upper()
        if ("TOKEN" in upper_key or "SECRET" in upper_key) and not upper_key.endswith(
            "TOKEN_FILE"
        ):
            continue
        if len(value) > 4096:
            continue
        pairs.append((key, value))
    return pairs


def _expanded_existing_paths(paths: tuple[str, ...]) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    for item in paths:
        if not item:
            continue
        expanded_item = os.path.expanduser(os.path.expandvars(item))
        if os.name == "nt" and expanded_item.startswith("/"):
            candidates = [(expanded_item, True)]
        else:
            path = Path(expanded_item)
            try:
                resolved = path.resolve(strict=True)
            except OSError:
                resolved = path
            candidates = [(str(resolved), resolved.exists())]
        for text, exists in candidates:
            if text in seen or not text.startswith("/"):
                continue
            if exists or os.name == "nt":
                expanded.append(text)
                seen.add(text)
    return expanded


def _resolved_existing_path(path: Path) -> str:
    if os.name == "nt":
        return str(path)
    try:
        resolved = path.resolve(strict=True)
    except OSError:
        return str(path)
    return str(resolved)
