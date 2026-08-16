from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_MAX_FILE_BYTES = 1024 * 1024
DEFAULT_MAX_PARALLEL_REQUESTS = 16


@dataclass(frozen=True)
class RepoConfig:
    branch: str = "main"
    source_branch: str = "main"
    result_branch: str | None = None
    device_branches: dict[str, str] = field(default_factory=dict)
    device_result_branches: dict[str, str] = field(default_factory=dict)
    remote: str = "origin"
    token_file: str | None = "api.txt"
    auth_username: str | None = None
    author_name: str = "limited-remote-partner"
    author_email: str = "limited-remote-partner@example.local"


@dataclass(frozen=True)
class IoConfig:
    input_dir: str = "input"
    output_dir: str = "output"
    exchange_dir: str = "exchange"
    state_dir: str = ".partner_state"
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES


@dataclass(frozen=True)
class ExchangeWatchConfig:
    enabled: bool = True
    interval_seconds: int = 5
    commit_message: str = "limited_remote_partner: exchange sync"


@dataclass(frozen=True)
class ExecutorConfig:
    name: str = "default"
    request_file: str = "input/job.json"
    allow_request_command: bool = True
    default_command: tuple[str, ...] = field(default_factory=tuple)
    working_dir: str = "."
    log_name: str = "job.log"
    timeout_seconds: int = 0
    sync_interval_seconds: int = 30
    default_output_subdir: str = "latest"
    env: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RelayConfig:
    role: str = "local"
    transport_mode: str = "relay"
    client_ssh: str | None = None
    client_password_file: str | None = None
    client_inbox_dir: str = ".partner_inbox"
    client_work_dir: str = "."
    server_ssh: str | None = None
    server_password_file: str | None = None
    server_return_dir: str = ".partner_return"
    relay_v2_return_mode: str = "pullback"
    relay_v2_push_grace_seconds: int = 8
    ssh_options: tuple[str, ...] = field(default_factory=tuple)
    scp_options: tuple[str, ...] = field(default_factory=tuple)
    poll_interval_seconds: float = 0.1
    max_parallel_requests: int = DEFAULT_MAX_PARALLEL_REQUESTS
    reverse_log_initial_interval_seconds: int = 1
    reverse_log_active_interval_seconds: int = 1
    reverse_log_stall_checks: int = 2
    direct_claim_grace_seconds: int = 30
    direct_claim_poll_seconds: int = 5


@dataclass(frozen=True)
class NodeIdentityConfig:
    node_id: str = ""
    display_name: str = ""
    roles: tuple[str, ...] = ("local",)
    capabilities: tuple[str, ...] = ("git-sync", "execute", "exchange")
    tags: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RoutingConfig:
    enabled: bool = False
    registry_path: str = "Develop/registry/system_registry.json"
    source_node: str = ""
    target_node: str = ""
    require_explicit_target: bool = False
    served_nodes: tuple[str, ...] = field(default_factory=tuple)
    served_tags: tuple[str, ...] = field(default_factory=tuple)
    served_roles: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class EndpointRuntimeConfig:
    endpoint_id: str = ""
    execution_environment_id: str = ""
    gateway_id: str = ""
    transport_mode: str = ""
    generation: str = ""
    backend_pool: str = ""
    remote_root: str = ""
    engine_root: str = ""
    soc: tuple[str, ...] = field(default_factory=tuple)
    cann: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class NodeLifecycleConfig:
    enabled: bool = False
    registration_state: str = "uninitialized"
    report_branch: str = "gp/nodes"
    publish_mode: str = "auto"
    heartbeat_seconds: int = 30
    lease_seconds: int = 90
    probe_on_start: bool = True


@dataclass(frozen=True)
class NetworkEnvironmentConfig:
    login_shell_import: bool = False


@dataclass(frozen=True)
class AutoUpdateConfig:
    enabled: bool = True
    mode: str = "reexec"
    watch_paths: tuple[str, ...] = ("src", "configs", "pyproject.toml")


@dataclass(frozen=True)
class ErrorBackoffConfig:
    initial_seconds: float = 10.0
    max_seconds: float = 300.0
    multiplier: float = 2.0


@dataclass(frozen=True)
class SandboxProfileConfig:
    backend: str = "process"
    writable_paths: tuple[str, ...] = field(default_factory=tuple)
    readonly_paths: tuple[str, ...] = field(default_factory=tuple)
    network: bool = True
    require_backend: bool = False
    systemd_properties: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class SandboxConfig:
    enabled: bool = True
    default_profile: str = "process"
    systemd_run_path: str = "systemd-run"
    bubblewrap_path: str = "bwrap"
    allow_backend_fallback: bool = True
    default_bubblewrap_writable_paths: tuple[str, ...] = ("/opt/ascendop",)
    profiles: dict[str, SandboxProfileConfig] = field(default_factory=dict)


@dataclass(frozen=True)
class AppConfig:
    poll_interval_seconds: float
    repo_dir: Path
    repo: RepoConfig
    io: IoConfig
    exchange_watch: ExchangeWatchConfig
    executor: ExecutorConfig
    relay: RelayConfig = field(default_factory=RelayConfig)
    node: NodeIdentityConfig = field(default_factory=NodeIdentityConfig)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    endpoint: EndpointRuntimeConfig = field(default_factory=EndpointRuntimeConfig)
    node_lifecycle: NodeLifecycleConfig = field(default_factory=NodeLifecycleConfig)
    network_environment: NetworkEnvironmentConfig = field(
        default_factory=NetworkEnvironmentConfig
    )
    auto_update: AutoUpdateConfig = field(default_factory=AutoUpdateConfig)
    error_backoff: ErrorBackoffConfig = field(default_factory=ErrorBackoffConfig)
    sandbox: SandboxConfig = field(default_factory=SandboxConfig)


def load_config(path: Path, *, base_dir: Path | None = None) -> AppConfig:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    repo_dir = _resolve_from_base(raw.get("repo_dir", "."), base_dir)

    repo_raw = _optional_dict(raw.get("repo"))
    io_raw = _optional_dict(raw.get("io"))
    exchange_watch_raw = _optional_dict(raw.get("exchange_watch"))
    executor_raw = _optional_dict(raw.get("executor"))
    relay_raw = _optional_dict(raw.get("relay"))
    node_raw = _optional_dict(raw.get("node"))
    routing_raw = _optional_dict(raw.get("routing"))
    endpoint_raw = _optional_dict(raw.get("endpoint"))
    lifecycle_raw = _optional_dict(raw.get("node_lifecycle"))
    network_environment_raw = _optional_dict(raw.get("network_environment"))
    auto_update_raw = _optional_dict(raw.get("auto_update"))
    error_backoff_raw = _optional_dict(raw.get("error_backoff"))
    sandbox_raw = _optional_dict(raw.get("sandbox"))

    max_file_bytes = int(io_raw.get("max_file_bytes", DEFAULT_MAX_FILE_BYTES))
    if max_file_bytes > DEFAULT_MAX_FILE_BYTES:
        raise ValueError("io.max_file_bytes cannot exceed 1048576")

    node = _load_node_identity(node_raw, relay_raw)
    branch = _render_branch_template(
        str(repo_raw.get("branch", "main")),
        node_id=node.node_id,
    )
    result_branch_raw = _optional_string(repo_raw.get("result_branch"))
    result_branch = (
        _render_branch_template(result_branch_raw, node_id=node.node_id)
        if result_branch_raw
        else None
    )
    device_branches = {
        str(name): _validate_git_branch(str(value))
        for name, value in _optional_dict(repo_raw.get("device_branches")).items()
    }
    device_result_branches = {
        str(name): _validate_git_branch(str(value))
        for name, value in _optional_dict(repo_raw.get("device_result_branches")).items()
    }

    return AppConfig(
        poll_interval_seconds=float(raw.get("poll_interval_seconds", 2)),
        repo_dir=repo_dir,
        repo=RepoConfig(
            branch=_validate_git_branch(branch),
            source_branch=_validate_git_branch(
                str(repo_raw.get("source_branch", "main"))
            ),
            result_branch=_validate_git_branch(result_branch) if result_branch else None,
            device_branches=device_branches,
            device_result_branches=device_result_branches,
            remote=str(repo_raw.get("remote", "origin")),
            token_file="api.txt",
            auth_username=_optional_string(repo_raw.get("auth_username")),
            author_name=str(repo_raw.get("author_name", "limited-remote-partner")),
            author_email=str(
                repo_raw.get("author_email", "limited-remote-partner@example.local")
            ),
        ),
        io=IoConfig(
            input_dir=_normalize_repo_path(str(io_raw.get("input_dir", "input"))).rstrip("/"),
            output_dir=_normalize_repo_path(str(io_raw.get("output_dir", "output"))).rstrip("/"),
            exchange_dir=_normalize_repo_path(
                str(io_raw.get("exchange_dir", "exchange"))
            ).rstrip("/"),
            state_dir=_normalize_repo_path(
                str(io_raw.get("state_dir", ".partner_state"))
            ).rstrip("/"),
            max_file_bytes=max_file_bytes,
        ),
        exchange_watch=ExchangeWatchConfig(
            enabled=bool(exchange_watch_raw.get("enabled", True)),
            interval_seconds=max(1, int(exchange_watch_raw.get("interval_seconds", 5))),
            commit_message=str(
                exchange_watch_raw.get(
                    "commit_message",
                    "limited_remote_partner: exchange sync",
                )
            ),
        ),
        executor=_load_executor(executor_raw),
        relay=_load_relay(relay_raw, repo_dir),
        node=node,
        routing=_load_routing(routing_raw),
        endpoint=_load_endpoint_runtime(endpoint_raw),
        node_lifecycle=_load_node_lifecycle(lifecycle_raw),
        network_environment=_load_network_environment(network_environment_raw),
        auto_update=_load_auto_update(auto_update_raw),
        error_backoff=_load_error_backoff(error_backoff_raw),
        sandbox=_load_sandbox(sandbox_raw),
    )


def _load_node_identity(
    raw: dict[str, Any], relay_raw: dict[str, Any]
) -> NodeIdentityConfig:
    role = str(relay_raw.get("role", "local")).lower()
    return NodeIdentityConfig(
        node_id=_safe_token(str(raw.get("node_id") or "")),
        display_name=str(raw.get("display_name") or ""),
        roles=_string_tuple(raw.get("roles", [role]), "node.roles"),
        capabilities=_string_tuple(
            raw.get("capabilities", ["git-sync", "execute", "exchange"]),
            "node.capabilities",
        ),
        tags=_string_tuple(raw.get("tags", []), "node.tags"),
    )


def _load_routing(raw: dict[str, Any]) -> RoutingConfig:
    return RoutingConfig(
        enabled=bool(raw.get("enabled", False)),
        registry_path=str(
            raw.get("registry_path", "Develop/registry/system_registry.json")
        ),
        source_node=str(raw.get("source_node") or ""),
        target_node=str(raw.get("target_node") or ""),
        require_explicit_target=bool(raw.get("require_explicit_target", False)),
        served_nodes=_string_tuple(raw.get("served_nodes", []), "routing.served_nodes"),
        served_tags=_string_tuple(raw.get("served_tags", []), "routing.served_tags"),
        served_roles=_string_tuple(raw.get("served_roles", []), "routing.served_roles"),
    )


def _load_endpoint_runtime(raw: dict[str, Any]) -> EndpointRuntimeConfig:
    return EndpointRuntimeConfig(
        endpoint_id=_safe_token(str(raw.get("endpoint_id") or "")),
        execution_environment_id=_safe_token(
            str(raw.get("execution_environment_id") or "")
        ),
        gateway_id=_safe_token(str(raw.get("gateway_id") or "")),
        transport_mode=str(raw.get("transport_mode") or ""),
        generation=str(raw.get("generation") or ""),
        backend_pool=str(raw.get("backend_pool") or ""),
        remote_root=str(raw.get("remote_root") or ""),
        engine_root=str(raw.get("engine_root") or ""),
        soc=_string_tuple(raw.get("soc", []), "endpoint.soc"),
        cann=_string_tuple(raw.get("cann", []), "endpoint.cann"),
    )


def _load_node_lifecycle(raw: dict[str, Any]) -> NodeLifecycleConfig:
    publish_mode = str(raw.get("publish_mode", "auto")).lower()
    if publish_mode not in {"auto", "git", "relay", "file"}:
        raise ValueError(
            "node_lifecycle.publish_mode must be one of: auto, git, relay, file"
        )
    registration_state = str(
        raw.get("registration_state", "uninitialized")
    ).lower()
    if registration_state not in {"uninitialized", "enrolled", "accepted"}:
        raise ValueError(
            "node_lifecycle.registration_state must be one of: "
            "uninitialized, enrolled, accepted"
        )
    heartbeat_seconds = max(5, int(raw.get("heartbeat_seconds", 30)))
    lease_seconds = max(heartbeat_seconds * 2, int(raw.get("lease_seconds", 90)))
    return NodeLifecycleConfig(
        enabled=bool(raw.get("enabled", False)),
        registration_state=registration_state,
        report_branch=_validate_git_branch(
            str(raw.get("report_branch", "gp/nodes"))
        ),
        publish_mode=publish_mode,
        heartbeat_seconds=heartbeat_seconds,
        lease_seconds=lease_seconds,
        probe_on_start=bool(raw.get("probe_on_start", True)),
    )


def _load_network_environment(raw: dict[str, Any]) -> NetworkEnvironmentConfig:
    return NetworkEnvironmentConfig(
        login_shell_import=bool(raw.get("login_shell_import", False)),
    )


def _load_error_backoff(raw: dict[str, Any]) -> ErrorBackoffConfig:
    initial_seconds = float(raw.get("initial_seconds", 10))
    max_seconds = float(raw.get("max_seconds", 300))
    multiplier = float(raw.get("multiplier", 2))
    if initial_seconds < 0:
        raise ValueError("error_backoff.initial_seconds cannot be negative")
    if max_seconds < initial_seconds:
        raise ValueError("error_backoff.max_seconds cannot be below initial_seconds")
    if multiplier < 1:
        raise ValueError("error_backoff.multiplier must be at least 1")
    return ErrorBackoffConfig(
        initial_seconds=initial_seconds,
        max_seconds=max_seconds,
        multiplier=multiplier,
    )


def _load_executor(raw: dict[str, Any]) -> ExecutorConfig:
    default_command = raw.get("default_command", [])
    if not isinstance(default_command, list) or not all(
        isinstance(item, str) for item in default_command
    ):
        raise ValueError("executor.default_command must be a string list")

    env = raw.get("env", {})
    if not isinstance(env, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in env.items()
    ):
        raise ValueError("executor.env must be an object of string values")

    return ExecutorConfig(
        name=str(raw.get("name", "default")),
        request_file=_normalize_repo_path(str(raw.get("request_file", "input/job.json"))),
        allow_request_command=bool(raw.get("allow_request_command", True)),
        default_command=tuple(default_command),
        working_dir=_normalize_repo_path(str(raw.get("working_dir", "."))),
        log_name=str(raw.get("log_name", "job.log")),
        timeout_seconds=int(raw.get("timeout_seconds", 0)),
        sync_interval_seconds=max(1, int(raw.get("sync_interval_seconds", 30))),
        default_output_subdir=_normalize_repo_path(
            str(raw.get("default_output_subdir", "latest"))
        ).rstrip("/"),
        env={key: str(value) for key, value in env.items()},
    )


def _load_relay(raw: dict[str, Any], repo_dir: Path) -> RelayConfig:
    role = str(raw.get("role", "local")).lower()
    if role not in {"local", "server", "client"}:
        raise ValueError("relay.role must be one of: local, server, client")
    transport_mode = str(raw.get("transport_mode", raw.get("transport", "relay"))).lower()
    if transport_mode not in {"relay", "direct", "auto"}:
        raise ValueError("relay.transport_mode must be one of: relay, direct, auto")
    relay_v2_return_mode = str(raw.get("relay_v2_return_mode", "pullback")).lower()
    if relay_v2_return_mode not in {"pullback", "push-atomic"}:
        raise ValueError(
            "relay.relay_v2_return_mode must be one of: pullback, push-atomic"
        )

    return RelayConfig(
        role=role,
        transport_mode=transport_mode,
        client_ssh=_optional_string(raw.get("client_ssh")),
        client_password_file=_optional_path_string(raw.get("client_password_file"), repo_dir),
        client_inbox_dir=str(raw.get("client_inbox_dir", ".partner_inbox")).rstrip("/"),
        client_work_dir=str(raw.get("client_work_dir", ".")),
        server_ssh=_optional_string(raw.get("server_ssh")),
        server_password_file=_optional_path_string(raw.get("server_password_file"), repo_dir),
        server_return_dir=str(raw.get("server_return_dir", ".partner_return")),
        relay_v2_return_mode=relay_v2_return_mode,
        relay_v2_push_grace_seconds=max(
            0,
            int(raw.get("relay_v2_push_grace_seconds", 8)),
        ),
        ssh_options=_string_tuple(raw.get("ssh_options", []), "relay.ssh_options"),
        scp_options=_string_tuple(raw.get("scp_options", []), "relay.scp_options"),
        poll_interval_seconds=max(
            0.05,
            float(raw.get("poll_interval_seconds", 0.1)),
        ),
        max_parallel_requests=min(
            DEFAULT_MAX_PARALLEL_REQUESTS,
            max(
                1,
                int(
                    raw.get(
                        "max_parallel_requests",
                        DEFAULT_MAX_PARALLEL_REQUESTS,
                    )
                ),
            ),
        ),
        reverse_log_initial_interval_seconds=max(
            1,
            int(raw.get("reverse_log_initial_interval_seconds", 1)),
        ),
        reverse_log_active_interval_seconds=max(
            1,
            int(raw.get("reverse_log_active_interval_seconds", 1)),
        ),
        reverse_log_stall_checks=max(1, int(raw.get("reverse_log_stall_checks", 2))),
        direct_claim_grace_seconds=max(
            0,
            int(raw.get("direct_claim_grace_seconds", 30)),
        ),
        direct_claim_poll_seconds=max(
            1,
            int(raw.get("direct_claim_poll_seconds", 5)),
        ),
    )


def _load_auto_update(raw: dict[str, Any]) -> AutoUpdateConfig:
    mode = str(raw.get("mode", "reexec")).lower()
    if mode not in {"reexec", "off"}:
        raise ValueError("auto_update.mode must be one of: reexec, off")
    return AutoUpdateConfig(
        enabled=bool(raw.get("enabled", True)),
        mode=mode,
        watch_paths=_string_tuple(
            raw.get("watch_paths", ["src", "configs", "pyproject.toml"]),
            "auto_update.watch_paths",
        ),
    )


def _load_sandbox(raw: dict[str, Any]) -> SandboxConfig:
    default_writable = _string_tuple(
        raw.get("default_bubblewrap_writable_paths", ["/opt/ascendop"]),
        "sandbox.default_bubblewrap_writable_paths",
    )
    profiles = _default_sandbox_profiles(default_writable)
    raw_profiles = raw.get("profiles", {})
    if raw_profiles is None:
        raw_profiles = {}
    if not isinstance(raw_profiles, dict):
        raise ValueError("sandbox.profiles must be an object")
    for name, profile_raw in raw_profiles.items():
        profiles[str(name)] = _load_sandbox_profile(
            _optional_dict(profile_raw),
            f"sandbox.profiles.{name}",
        )

    return SandboxConfig(
        enabled=bool(raw.get("enabled", True)),
        default_profile=str(raw.get("default_profile", "process")),
        systemd_run_path=str(raw.get("systemd_run_path", "systemd-run")),
        bubblewrap_path=str(raw.get("bubblewrap_path", "bwrap")),
        allow_backend_fallback=bool(raw.get("allow_backend_fallback", True)),
        default_bubblewrap_writable_paths=default_writable,
        profiles=profiles,
    )


def _default_sandbox_profiles(
    default_writable: tuple[str, ...],
) -> dict[str, SandboxProfileConfig]:
    return {
        "process": SandboxProfileConfig(backend="process"),
        "read-only-probe": SandboxProfileConfig(
            backend="systemd",
            systemd_properties=("MemoryMax=8G", "TasksMax=256"),
        ),
        "process-no-install": SandboxProfileConfig(
            backend="systemd",
            systemd_properties=("MemoryMax=16G", "TasksMax=512"),
        ),
        "process-reviewed": SandboxProfileConfig(
            backend="systemd",
            systemd_properties=("MemoryMax=32G", "TasksMax=1024"),
        ),
        "bubblewrap-home": SandboxProfileConfig(
            backend="bubblewrap",
            writable_paths=default_writable,
            network=True,
        ),
        "ascend-compile": SandboxProfileConfig(
            backend="bubblewrap",
            writable_paths=(*default_writable, "/tmp", "/var/tmp"),
            readonly_paths=(
                "/usr/local/Ascend",
                "/usr/local/bin",
                "/usr/bin",
                "/usr/lib",
                "/lib",
                "/etc",
            ),
            network=True,
        ),
    }


def _load_sandbox_profile(
    raw: dict[str, Any],
    name: str,
) -> SandboxProfileConfig:
    backend = str(raw.get("backend", "process")).lower()
    if backend not in {"process", "systemd", "bubblewrap"}:
        raise ValueError(f"{name}.backend must be one of: process, systemd, bubblewrap")
    return SandboxProfileConfig(
        backend=backend,
        writable_paths=_string_tuple(raw.get("writable_paths", []), f"{name}.writable_paths"),
        readonly_paths=_string_tuple(raw.get("readonly_paths", []), f"{name}.readonly_paths"),
        network=bool(raw.get("network", True)),
        require_backend=bool(raw.get("require_backend", False)),
        systemd_properties=_string_tuple(
            raw.get("systemd_properties", []),
            f"{name}.systemd_properties",
        ),
    )


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{name} must be a string list")
    return tuple(value)


def _optional_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("config sections must be objects")
    return value


def _optional_path_string(value: Any, base_dir: Path) -> str | None:
    if value in (None, ""):
        return None
    raw = str(value)
    if raw.startswith("/"):
        return raw
    path = Path(raw)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _safe_token(value: str) -> str:
    cleaned = "".join(
        char if char.isalnum() or char in "._-" else "-" for char in value.strip()
    ).strip(".-")
    return cleaned


def _render_branch_template(value: str, *, node_id: str) -> str:
    channel_node = (
        os.environ.get("GITPARTNER_CHANNEL_NODE", "").strip()
        or node_id
        or "unnamed-node"
    )
    return (
        value.replace("{node_id}", _safe_token(node_id) or "unnamed-node")
        .replace("{channel_node}", _safe_token(channel_node) or "unnamed-node")
        .strip("/")
    )


def _validate_git_branch(value: str) -> str:
    invalid_tokens = ("..", "@{", "\\", " ", "~", "^", ":", "?", "*", "[")
    if (
        not value
        or any(token in value for token in invalid_tokens)
        or value.startswith(("/", "."))
        or value.endswith(("/", "."))
        or "//" in value
    ):
        raise ValueError(f"repo branch is not a safe Git ref: {value!r}")
    return value


def _resolve_from_base(value: Any, base_dir: Path | None) -> Path:
    path = Path(str(value))
    if not path.is_absolute():
        path = ((base_dir or Path.cwd()) / path).resolve()
    return path


def _normalize_repo_path(path: str) -> str:
    normalized = path.replace("\\", "/").lstrip("/")
    if normalized in ("", "."):
        return "."
    if ".." in Path(normalized).parts:
        raise ValueError(f"repo path cannot contain '..': {path}")
    return normalized
