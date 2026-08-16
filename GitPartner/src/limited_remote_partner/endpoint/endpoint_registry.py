from __future__ import annotations

import hashlib
import json
import os
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SYSTEM_REGISTRY_SCHEMA = "ascendop.system-registry.v2"
LEGACY_SYSTEM_REGISTRY_SCHEMA = "ascendop.system-registry.v1"
SUPPORTED_SYSTEM_REGISTRY_SCHEMAS = {
    LEGACY_SYSTEM_REGISTRY_SCHEMA,
    SYSTEM_REGISTRY_SCHEMA,
}
VALID_TRANSPORTS = {"direct", "relay", "auto"}
VALID_CHANNEL_MODES = {"isolated-worktree", "legacy-shared"}
VALID_TRANSPORT_BINDING_MODES = {"direct-git", "lan-relay"}


class EndpointRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class GPEndpoint:
    endpoint_id: str
    node_id: str
    execution_environment_id: str
    gateway_id: str
    gateway_ssh: str
    node_ssh: str
    transport_mode: str
    enabled: bool
    draining: bool
    gateway_enabled: bool
    gateway_draining: bool
    node_enabled: bool
    node_draining: bool
    environment_enabled: bool
    environment_draining: bool
    priority: int
    backend_pool: str
    transport: str
    gitpartner_repo: str
    result_worktree: str
    gitpartner_config: str
    node_gitpartner_config: str
    control_channel: str
    result_channel: str
    channel_mode: str
    remote_root: str
    engine_root: str
    cache_root: str
    capabilities: dict[str, Any] = field(default_factory=dict)
    tags: tuple[str, ...] = field(default_factory=tuple)
    import_login_network_environment: bool = False
    generation: str = ""

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["tags"] = list(self.tags)
        return value


@dataclass(frozen=True)
class RouteDecision:
    selected: GPEndpoint | None
    candidates: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected": self.selected.to_dict() if self.selected else None,
            "candidates": list(self.candidates),
        }


@dataclass(frozen=True)
class GPNodeLauncher:
    node_id: str
    endpoint_id: str
    role: str
    enabled: bool
    runtime_config: str
    remote: str
    import_login_network_environment: bool
    git_tls_verify: bool = True
    retire_endpoint_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EndpointRegistry:
    def __init__(
        self,
        path: Path,
        endpoints: tuple[GPEndpoint, ...],
        launchers: tuple[GPNodeLauncher, ...] = (),
    ) -> None:
        self.path = path.resolve()
        self.workspace_root = find_workspace_root(self.path)
        self.endpoints = endpoints
        self.launchers = launchers
        self._by_id = {endpoint.endpoint_id: endpoint for endpoint in endpoints}
        self._launchers_by_node = {
            launcher.node_id: launcher for launcher in launchers
        }
        self._validate()

    @classmethod
    def load(cls, path: Path) -> "EndpointRegistry":
        resolved = path.resolve()
        raw = json.loads(resolved.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise EndpointRegistryError("system registry must be a JSON object")
        schema = str(raw.get("schema") or "")
        if schema not in SUPPORTED_SYSTEM_REGISTRY_SCHEMAS:
            raise EndpointRegistryError(
                "system registry schema must be one of "
                + ", ".join(sorted(SUPPORTED_SYSTEM_REGISTRY_SCHEMAS))
            )
        endpoints = (
            _parse_v2_endpoints(raw)
            if schema == SYSTEM_REGISTRY_SCHEMA
            else _parse_legacy_endpoints(raw)
        )
        launchers = (
            _parse_v2_launchers(raw, endpoints)
            if schema == SYSTEM_REGISTRY_SCHEMA
            else ()
        )
        return cls(resolved, endpoints, launchers)

    def get(self, endpoint_id: str) -> GPEndpoint:
        try:
            return self._by_id[endpoint_id]
        except KeyError as exc:
            known = ", ".join(sorted(self._by_id)) or "none"
            raise EndpointRegistryError(
                f"unknown GP endpoint {endpoint_id!r}; known endpoints: {known}"
            ) from exc

    def get_launcher(self, node_id: str) -> GPNodeLauncher:
        try:
            return self._launchers_by_node[node_id]
        except KeyError as exc:
            known = ", ".join(sorted(self._launchers_by_node)) or "none"
            raise EndpointRegistryError(
                f"unknown GP launcher node {node_id!r}; known nodes: {known}"
            ) from exc

    def materialize_launcher_manifest(
        self,
        output_path: Path,
        *,
        source_repo: Path,
    ) -> dict[str, Any]:
        source_repo = source_repo.resolve()
        nodes: dict[str, dict[str, Any]] = {}
        for launcher in self.launchers:
            endpoint = self.get(launcher.endpoint_id)
            runtime_config = _safe_repo_relative(
                launcher.runtime_config,
                label=f"launcher {launcher.node_id} runtime_config",
            )
            config_path = source_repo / runtime_config
            if not config_path.is_file():
                raise EndpointRegistryError(
                    f"launcher {launcher.node_id} runtime config is missing: "
                    f"{config_path}"
                )
            config = json.loads(config_path.read_text(encoding="utf-8-sig"))
            _validate_launcher_runtime_config(
                launcher,
                endpoint,
                config,
                config_path=config_path,
            )
            worktree = self.resolve_path(endpoint.gitpartner_repo)
            try:
                worktree_relative = os.path.relpath(worktree, source_repo)
            except ValueError as exc:
                raise EndpointRegistryError(
                    f"launcher {launcher.node_id} worktree is not portable "
                    f"relative to {source_repo}"
                ) from exc
            nodes[launcher.node_id] = {
                "enabled": launcher.enabled,
                "control_branch": endpoint.control_channel,
                "worktree": Path(worktree_relative).as_posix(),
                "config": runtime_config.as_posix(),
                "role": launcher.role,
                "generation": endpoint.generation,
                "remote": launcher.remote,
                "import_login_network_env": (
                    launcher.import_login_network_environment
                ),
                "git_tls_verify": launcher.git_tls_verify,
                "retire_endpoint_ids": list(launcher.retire_endpoint_ids),
            }
        result = {
            "schema": "git-partner.node-launchers.v1",
            "registry": _portable_path(self.path, self.workspace_root),
            "nodes": dict(sorted(nodes.items())),
        }
        _write_json_atomic(output_path.resolve(), result)
        return result

    def route(self, requirements: dict[str, Any]) -> RouteDecision:
        rows: list[dict[str, Any]] = []
        accepted: list[GPEndpoint] = []
        for endpoint in self.endpoints:
            reasons = route_rejection_reasons(endpoint, requirements)
            if not reasons:
                accepted.append(endpoint)
            rows.append(
                {
                    "endpoint_id": endpoint.endpoint_id,
                    "accepted": not reasons,
                    "rejection_reasons": reasons,
                    "priority": endpoint.priority,
                    "node_id": endpoint.node_id,
                    "execution_environment_id": endpoint.execution_environment_id,
                }
            )
        selected = sorted(
            accepted,
            key=lambda endpoint: (-endpoint.priority, endpoint.endpoint_id),
        )[0] if accepted else None
        return RouteDecision(selected=selected, candidates=tuple(rows))

    def materialize_config(
        self,
        endpoint_id: str,
        base_config: Path,
        output_path: Path,
        *,
        portable_worktree: bool = False,
    ) -> dict[str, Any]:
        endpoint = self.get(endpoint_id)
        raw = json.loads(base_config.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise EndpointRegistryError("base GitPartner config must be an object")
        result = deepcopy(raw)
        result.pop("roles", None)
        resolved_repo_dir = self.resolve_path(endpoint.gitpartner_repo)
        result["repo_dir"] = (
            "."
            if portable_worktree
            else _portable_path(resolved_repo_dir, self.workspace_root)
        )
        repo = _object(result, "repo")
        repo["branch"] = endpoint.control_channel
        repo["source_branch"] = str(repo.get("source_branch") or "main")
        repo["result_branch"] = endpoint.result_channel
        if repo.get("token_file"):
            repo["token_file"] = "api.txt"
        relay = _object(result, "relay")
        relay["transport_mode"] = endpoint.transport
        relay_role = str(relay.get("role") or "local").lower()
        relay["client_password_file"] = ""
        relay["server_password_file"] = ""
        if endpoint.transport == "direct":
            relay["client_ssh"] = ""
            relay["server_ssh"] = ""
            relay["client_work_dir"] = endpoint.remote_root or "."
            relay["client_inbox_dir"] = "work/relay/inbox"
            relay["server_return_dir"] = "work/relay/return"
        elif relay_role == "server":
            relay["client_ssh"] = endpoint.node_ssh
            relay["server_ssh"] = ""
            relay["client_work_dir"] = endpoint.remote_root or "."
        elif relay_role == "client":
            relay["client_ssh"] = ""
            relay["server_ssh"] = endpoint.gateway_ssh
            relay["client_work_dir"] = endpoint.remote_root or "."
        else:
            raise EndpointRegistryError(
                f"relay endpoint {endpoint.endpoint_id} requires client or server role"
            )
        if relay_role != "server":
            network_environment = _object(result, "network_environment")
            network_environment["login_shell_import"] = (
                endpoint.import_login_network_environment
            )
        if endpoint.channel_mode == "isolated-worktree":
            exchange_watch = _object(result, "exchange_watch")
            exchange_watch["enabled"] = False
        if relay_role != "server" and endpoint.remote_root:
            sandbox = _object(result, "sandbox")
            writable_roots = [
                item
                for item in (endpoint.remote_root, endpoint.cache_root)
                if item
            ]
            sandbox["default_bubblewrap_writable_paths"] = [
                *writable_roots,
                "/tmp",
                "/var/tmp",
            ]
            profiles = _object(sandbox, "profiles")
            ascend_compile = _object(profiles, "ascend-compile")
            ascend_compile["writable_paths"] = [
                *writable_roots,
                "/tmp",
                "/var/tmp",
            ]
            bubblewrap_home = _object(profiles, "bubblewrap-home")
            bubblewrap_home["writable_paths"] = writable_roots
        if endpoint.cache_root:
            executor = _object(result, "executor")
            env = _object(executor, "env")
            env["ASCENDOP_ENGINE_CACHE_ROOT"] = endpoint.cache_root
        io = _object(result, "io")
        io["state_dir"] = f".partner_state/endpoints/{endpoint.endpoint_id}"
        result["node"] = {
            "node_id": endpoint.node_id,
            "display_name": endpoint.node_id,
            "roles": [str(relay.get("role") or "local")],
            "capabilities": sorted(_string_set(endpoint.capabilities.get("features"))),
            "tags": list(endpoint.tags),
        }
        result["routing"] = {
            "enabled": True,
            "registry_path": _portable_path(self.path, self.workspace_root),
            "target_node": endpoint.node_id,
            "require_explicit_target": True,
            "served_nodes": [endpoint.node_id],
            "served_tags": list(endpoint.tags),
            "served_roles": [],
        }
        result["endpoint"] = {
            "endpoint_id": endpoint.endpoint_id,
            "execution_environment_id": endpoint.execution_environment_id,
            "generation": endpoint.generation,
            "gateway_id": endpoint.gateway_id,
            "transport_mode": endpoint.transport_mode,
            "backend_pool": endpoint.backend_pool,
            "remote_root": endpoint.remote_root,
            "engine_root": endpoint.engine_root,
            "cache_root": endpoint.cache_root,
        }
        registration_state = (
            "accepted"
            if endpoint.transport_mode.startswith("legacy-")
            else "enrolled"
        )
        lifecycle_enabled = not (
            str(relay.get("role") or "").lower() == "server"
            and bool(endpoint.gateway_id)
        )
        result["node_lifecycle"] = {
            "enabled": lifecycle_enabled,
            "registration_state": registration_state,
            "report_branch": "gp/nodes",
            "publish_mode": "auto",
            "heartbeat_seconds": 30,
            "lease_seconds": 90,
            "probe_on_start": True,
        }
        output_path = output_path.resolve()
        _write_json_atomic(output_path, result)
        identity_path = (
            resolved_repo_dir
            / str(result["io"]["state_dir"])
            / "node_identity.json"
        )
        _write_json_atomic(
            identity_path,
            {
                "schema": "git-partner.node-identity.v1",
                "node_id": endpoint.node_id,
                "endpoint_id": endpoint.endpoint_id,
                "gateway_id": endpoint.gateway_id,
                "generation": endpoint.generation,
                "created_at": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "config_path": _portable_path(output_path, resolved_repo_dir),
                "source_branch": repo["source_branch"],
                "control_branch": endpoint.control_channel,
                "result_branch": endpoint.result_channel,
                "report_branch": "gp/nodes",
                "registration_state": registration_state,
                "transport_mode": endpoint.transport_mode,
            },
        )
        return result

    def resolve_path(self, value: str) -> Path:
        path = Path(value)
        return path.resolve() if path.is_absolute() else (self.workspace_root / path).resolve()

    def _validate(self) -> None:
        ids: set[str] = set()
        control_channels: set[str] = set()
        result_channels: set[str] = set()
        worktrees: dict[str, str] = {}
        for endpoint in self.endpoints:
            if endpoint.endpoint_id in ids:
                raise EndpointRegistryError(
                    f"duplicate GP endpoint id: {endpoint.endpoint_id}"
                )
            ids.add(endpoint.endpoint_id)
            if endpoint.transport not in VALID_TRANSPORTS:
                raise EndpointRegistryError(
                    f"invalid transport for {endpoint.endpoint_id}: {endpoint.transport}"
                )
            if endpoint.channel_mode not in VALID_CHANNEL_MODES:
                raise EndpointRegistryError(
                    f"invalid channel mode for {endpoint.endpoint_id}: "
                    f"{endpoint.channel_mode}"
                )
            _validate_git_ref(endpoint.control_channel)
            _validate_git_ref(endpoint.result_channel)
            if endpoint.channel_mode == "isolated-worktree":
                if endpoint.control_channel == endpoint.result_channel:
                    raise EndpointRegistryError(
                        f"isolated endpoint {endpoint.endpoint_id} must use distinct "
                        "control and result channels"
                    )
                if endpoint.control_channel == "main" or endpoint.result_channel == "main":
                    raise EndpointRegistryError(
                        f"isolated endpoint {endpoint.endpoint_id} cannot use main as a data channel"
                    )
            if not endpoint.enabled:
                continue
            if endpoint.control_channel in control_channels:
                raise EndpointRegistryError(
                    f"duplicate enabled control channel: {endpoint.control_channel}"
                )
            if endpoint.result_channel in result_channels:
                raise EndpointRegistryError(
                    f"duplicate enabled result channel: {endpoint.result_channel}"
                )
            control_channels.add(endpoint.control_channel)
            result_channels.add(endpoint.result_channel)
            worktree = str(self.resolve_path(endpoint.gitpartner_repo)).lower()
            _claim_worktree(
                worktrees,
                worktree,
                f"{endpoint.endpoint_id}:ingress",
            )
            if endpoint.result_worktree:
                result_worktree = str(
                    self.resolve_path(endpoint.result_worktree)
                ).lower()
                _claim_worktree(
                    worktrees,
                    result_worktree,
                    f"{endpoint.endpoint_id}:result",
                )
            if (
                "gp-duplex-lanes-v1"
                in _string_set(endpoint.capabilities.get("features"))
                and not endpoint.result_worktree
            ):
                raise EndpointRegistryError(
                    f"duplex endpoint {endpoint.endpoint_id} requires "
                    "result_worktree"
                )
        launcher_ids: set[str] = set()
        for launcher in self.launchers:
            if launcher.node_id in launcher_ids:
                raise EndpointRegistryError(
                    f"duplicate GP launcher node id: {launcher.node_id}"
                )
            launcher_ids.add(launcher.node_id)
            endpoint = self.get(launcher.endpoint_id)
            for retired in launcher.retire_endpoint_ids:
                if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}", retired):
                    raise EndpointRegistryError(
                        f"launcher {launcher.node_id} has invalid retired "
                        f"endpoint id {retired!r}"
                    )
                if retired == endpoint.endpoint_id:
                    raise EndpointRegistryError(
                        f"launcher {launcher.node_id} cannot retire its current "
                        f"endpoint id {retired}"
                    )
            if launcher.role == "server":
                if not endpoint.gateway_id:
                    raise EndpointRegistryError(
                        f"server launcher {launcher.node_id} requires a relayed endpoint"
                    )
                if launcher.node_id != endpoint.gateway_id:
                    raise EndpointRegistryError(
                        f"server launcher {launcher.node_id} must match endpoint "
                        f"gateway {endpoint.gateway_id}"
                    )
            elif launcher.role == "client":
                if launcher.node_id != endpoint.node_id:
                    raise EndpointRegistryError(
                        f"client launcher {launcher.node_id} must match endpoint "
                        f"node {endpoint.node_id}"
                    )


def route_rejection_reasons(
    endpoint: GPEndpoint,
    requirements: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not endpoint.enabled:
        reasons.append("endpoint-disabled")
    if endpoint.draining:
        reasons.append("endpoint-draining")
    if not endpoint.gateway_enabled:
        reasons.append("gateway-disabled")
    if endpoint.gateway_draining:
        reasons.append("gateway-draining")
    if not endpoint.node_enabled:
        reasons.append("node-disabled")
    if endpoint.node_draining:
        reasons.append("node-draining")
    if not endpoint.environment_enabled:
        reasons.append("environment-disabled")
    if endpoint.environment_draining:
        reasons.append("environment-draining")
    allowed = _string_set(requirements.get("allowed_endpoints"))
    if allowed and endpoint.endpoint_id not in allowed:
        reasons.append("endpoint-not-allowed")
    pool = str(requirements.get("backend_pool") or "")
    if pool and endpoint.backend_pool != pool:
        reasons.append(f"backend-pool-mismatch:{endpoint.backend_pool or 'none'}")
    transport = str(requirements.get("transport") or "")
    if transport and endpoint.transport not in {transport, "auto"}:
        reasons.append(f"transport-mismatch:{endpoint.transport}")
    _require_capability_value(reasons, endpoint, requirements, "soc")
    _require_capability_value(reasons, endpoint, requirements, "cann")
    for key in ("features", "cache_adapters"):
        required = _string_set(requirements.get(key))
        available = _string_set(endpoint.capabilities.get(key))
        missing = sorted(required - available)
        if missing:
            reasons.append(f"missing-{key}:" + ",".join(missing))
    return reasons


def find_workspace_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "Develop").is_dir() and (parent / "GitPartner").is_dir():
            return parent
    return path.parent


def canonical_digest(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _portable_path(path: Path, root: Path) -> str:
    try:
        relative = os.path.relpath(path.resolve(), root.resolve())
    except ValueError as exc:
        raise EndpointRegistryError(
            f"path cannot be represented relative to registry root: {path}"
        ) from exc
    return "." if relative == "." else relative.replace("\\", "/")


def _parse_legacy_endpoint(value: object) -> GPEndpoint:
    if not isinstance(value, dict):
        raise EndpointRegistryError("each gp_endpoints entry must be an object")
    endpoint_id = _required_token(value, "endpoint_id")
    node_id = _required_token(value, "node_id")
    environment_id = _required_token(value, "execution_environment_id")
    capabilities = value.get("capabilities", {})
    if not isinstance(capabilities, dict):
        raise EndpointRegistryError(
            f"capabilities must be an object for endpoint {endpoint_id}"
        )
    normalized = {
        "endpoint_id": endpoint_id,
        "node_id": node_id,
        "execution_environment_id": environment_id,
        "gateway_id": "",
        "gateway_ssh": "",
        "node_ssh": "",
        "transport_mode": "legacy-" + _legacy_transport_mode(
            str(value.get("transport") or "relay")
        ),
        "enabled": bool(value.get("enabled", True)),
        "draining": bool(value.get("draining", False)),
        "gateway_enabled": True,
        "gateway_draining": False,
        "node_enabled": True,
        "node_draining": False,
        "environment_enabled": True,
        "environment_draining": False,
        "priority": int(value.get("priority", 0)),
        "backend_pool": str(value.get("backend_pool") or ""),
        "transport": str(value.get("transport") or "relay").lower(),
        "gitpartner_repo": str(value.get("gitpartner_repo") or "GitPartner"),
        "result_worktree": str(value.get("result_worktree") or ""),
        "gitpartner_config": str(
            value.get("gitpartner_config") or "GitPartner/configs/partner.json"
        ),
        "node_gitpartner_config": str(
            value.get("node_gitpartner_config")
            or value.get("gitpartner_config")
            or "GitPartner/configs/partner.json"
        ),
        "control_channel": str(value.get("control_channel") or "main"),
        "result_channel": str(
            value.get("result_channel") or value.get("control_channel") or "main"
        ),
        "channel_mode": str(value.get("channel_mode") or "isolated-worktree"),
        "remote_root": str(value.get("remote_root") or ""),
        "engine_root": str(value.get("engine_root") or "test_engine_demo"),
        "cache_root": str(value.get("cache_root") or ""),
        "capabilities": capabilities,
        "tags": tuple(sorted(_string_set(value.get("tags")))),
        "import_login_network_environment": bool(
            value.get("import_login_network_environment", False)
        ),
    }
    generation = str(value.get("generation") or canonical_digest(normalized))
    return GPEndpoint(**normalized, generation=generation)


def _parse_legacy_endpoints(raw: dict[str, Any]) -> tuple[GPEndpoint, ...]:
    values = raw.get("gp_endpoints", [])
    if not isinstance(values, list):
        raise EndpointRegistryError("gp_endpoints must be a list")
    return tuple(_parse_legacy_endpoint(item) for item in values)


def _parse_v2_endpoints(raw: dict[str, Any]) -> tuple[GPEndpoint, ...]:
    gateway_values = _list(raw, "transport_gateways")
    node_values = _list(raw, "execution_nodes")
    environment_values = _list(raw, "execution_environments")
    endpoint_values = _list(raw, "route_endpoints")
    gateways = {
        row["gateway_id"]: row
        for row in (
            _parse_gateway(value) for value in gateway_values
        )
    }
    nodes = {
        row["node_id"]: row
        for row in (_parse_node(value) for value in node_values)
    }
    environments = {
        row["execution_environment_id"]: row
        for row in (
            _parse_environment(value) for value in environment_values
        )
    }
    if len(gateways) != len(gateway_values):
        raise EndpointRegistryError("duplicate transport gateway id")
    if len(nodes) != len(node_values):
        raise EndpointRegistryError("duplicate execution node id")
    if len(environments) != len(environment_values):
        raise EndpointRegistryError("duplicate execution environment id")
    for environment in environments.values():
        if environment["node_id"] not in nodes:
            raise EndpointRegistryError(
                f"execution environment {environment['execution_environment_id']} "
                f"references unknown node {environment['node_id']}"
            )
    return tuple(
        _parse_v2_endpoint(
            value,
            gateways=gateways,
            nodes=nodes,
            environments=environments,
        )
        for value in endpoint_values
    )


def _parse_v2_launchers(
    raw: dict[str, Any],
    endpoints: tuple[GPEndpoint, ...],
) -> tuple[GPNodeLauncher, ...]:
    values = _list(raw, "gp_launchers")
    endpoint_ids = {endpoint.endpoint_id for endpoint in endpoints}
    launchers: list[GPNodeLauncher] = []
    for value in values:
        row = _row(value, "GP launcher")
        node_id = _required_token(row, "node_id")
        endpoint_id = _required_token(row, "endpoint_id")
        if endpoint_id not in endpoint_ids:
            raise EndpointRegistryError(
                f"GP launcher {node_id} references unknown endpoint {endpoint_id}"
            )
        role = str(row.get("role") or "").lower()
        if role not in {"client", "server"}:
            raise EndpointRegistryError(
                f"GP launcher {node_id} role must be client or server"
            )
        runtime_config = str(row.get("runtime_config") or "").strip()
        if not runtime_config:
            raise EndpointRegistryError(
                f"GP launcher {node_id} requires runtime_config"
            )
        _safe_repo_relative(
            runtime_config,
            label=f"launcher {node_id} runtime_config",
        )
        remote = str(row.get("remote") or "origin").strip()
        if not re.fullmatch(r"[A-Za-z0-9._/-]+", remote):
            raise EndpointRegistryError(
                f"GP launcher {node_id} remote is invalid"
            )
        launchers.append(
            GPNodeLauncher(
                node_id=node_id,
                endpoint_id=endpoint_id,
                role=role,
                enabled=bool(row.get("enabled", True)),
                runtime_config=runtime_config,
                remote=remote,
                import_login_network_environment=bool(
                    row.get(
                        "import_login_network_environment",
                        False,
                    )
                ),
                git_tls_verify=bool(row.get("git_tls_verify", True)),
                retire_endpoint_ids=tuple(
                    sorted(_string_set(row.get("retire_endpoint_ids")))
                ),
            )
        )
    return tuple(launchers)


def _parse_gateway(value: object) -> dict[str, Any]:
    row = _row(value, "transport gateway")
    mode = str(row.get("mode") or "")
    if mode != "lan-relay":
        raise EndpointRegistryError(
            "transport gateway mode must be lan-relay; direct-git has no gateway"
        )
    normalized = {
        "gateway_id": _required_token(row, "gateway_id"),
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "mode": mode,
        "service_node_id": _optional_token(row, "service_node_id"),
        "relay_ssh": _optional_ssh_target(row, "relay_ssh"),
        "tags": tuple(sorted(_string_set(row.get("tags")))),
    }
    return {
        **normalized,
        "generation": str(row.get("generation") or canonical_digest(normalized)),
    }


def _parse_node(value: object) -> dict[str, Any]:
    row = _row(value, "execution node")
    node_id = _required_token(row, "node_id")
    normalized = {
        "node_id": node_id,
        "display_name": str(row.get("display_name") or node_id),
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "relay_ssh": _optional_ssh_target(row, "relay_ssh"),
        "import_login_network_environment": bool(
            (
                row.get("network_environment")
                if isinstance(row.get("network_environment"), dict)
                else {}
            ).get("login_shell_import", False)
        ),
        "tags": tuple(sorted(_string_set(row.get("tags")))),
    }
    return {
        **normalized,
        "generation": str(row.get("generation") or canonical_digest(normalized)),
    }


def _parse_environment(value: object) -> dict[str, Any]:
    row = _row(value, "execution environment")
    capabilities = row.get("capabilities", {})
    if not isinstance(capabilities, dict):
        raise EndpointRegistryError("execution environment capabilities must be an object")
    normalized = {
        "execution_environment_id": _required_token(
            row, "execution_environment_id"
        ),
        "node_id": _required_token(row, "node_id"),
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "backend_pool": str(row.get("backend_pool") or ""),
        "remote_root": str(row.get("remote_root") or ""),
        "engine_root": str(row.get("engine_root") or "test_engine_demo"),
        "cache_root": str(row.get("cache_root") or ""),
        "capabilities": deepcopy(capabilities),
        "tags": tuple(sorted(_string_set(row.get("tags")))),
    }
    return {
        **normalized,
        "generation": str(row.get("generation") or canonical_digest(normalized)),
    }


def _parse_v2_endpoint(
    value: object,
    *,
    gateways: dict[str, dict[str, Any]],
    nodes: dict[str, dict[str, Any]],
    environments: dict[str, dict[str, Any]],
) -> GPEndpoint:
    row = _row(value, "route endpoint")
    endpoint_id = _required_token(row, "endpoint_id")
    node_id = _required_token(row, "node_id")
    environment_id = _required_token(row, "execution_environment_id")
    node = nodes.get(node_id)
    if node is None:
        raise EndpointRegistryError(
            f"route endpoint {endpoint_id} references unknown node {node_id}"
        )
    environment = environments.get(environment_id)
    if environment is None:
        raise EndpointRegistryError(
            f"route endpoint {endpoint_id} references unknown environment "
            f"{environment_id}"
        )
    if environment["node_id"] != node_id:
        raise EndpointRegistryError(
            f"route endpoint {endpoint_id} binds environment {environment_id} "
            f"to node {node_id}, expected {environment['node_id']}"
        )
    binding = row.get("transport_binding", {})
    if not isinstance(binding, dict):
        raise EndpointRegistryError("transport_binding must be an object")
    mode = str(binding.get("mode") or "")
    if mode not in VALID_TRANSPORT_BINDING_MODES:
        raise EndpointRegistryError(
            f"route endpoint {endpoint_id} has invalid transport binding mode {mode!r}"
        )
    gateway_id = _optional_token(binding, "gateway_id")
    gateway: dict[str, Any] | None = None
    if mode == "lan-relay":
        if not gateway_id:
            raise EndpointRegistryError(
                f"relayed endpoint {endpoint_id} requires gateway_id"
            )
        gateway = gateways.get(gateway_id)
        if gateway is None:
            raise EndpointRegistryError(
                f"route endpoint {endpoint_id} references unknown gateway {gateway_id}"
            )
        if not gateway["relay_ssh"]:
            raise EndpointRegistryError(
                f"relayed endpoint {endpoint_id} gateway {gateway_id} "
                "requires relay_ssh"
            )
        if not node["relay_ssh"]:
            raise EndpointRegistryError(
                f"relayed endpoint {endpoint_id} node {node_id} requires relay_ssh"
            )
    elif gateway_id:
        raise EndpointRegistryError(
            f"direct endpoint {endpoint_id} cannot declare gateway_id"
        )
    tags = tuple(
        sorted(
            {
                *node["tags"],
                *environment["tags"],
                *(gateway["tags"] if gateway else ()),
                *_string_set(row.get("tags")),
            }
        )
    )
    normalized = {
        "endpoint_id": endpoint_id,
        "node_id": node_id,
        "execution_environment_id": environment_id,
        "gateway_id": gateway_id,
        "gateway_ssh": str(gateway["relay_ssh"]) if gateway else "",
        "node_ssh": str(node["relay_ssh"]),
        "transport_mode": mode,
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "gateway_enabled": bool(gateway["enabled"]) if gateway else True,
        "gateway_draining": bool(gateway["draining"]) if gateway else False,
        "node_enabled": bool(node["enabled"]),
        "node_draining": bool(node["draining"]),
        "environment_enabled": bool(environment["enabled"]),
        "environment_draining": bool(environment["draining"]),
        "priority": int(row.get("priority", 0)),
        "backend_pool": str(environment["backend_pool"]),
        "transport": "direct" if mode == "direct-git" else "relay",
        "gitpartner_repo": str(row.get("gitpartner_repo") or "GitPartner"),
        "result_worktree": str(row.get("result_worktree") or ""),
        "gitpartner_config": str(
            row.get("gitpartner_config") or "GitPartner/configs/partner.json"
        ),
        "node_gitpartner_config": str(
            row.get("node_gitpartner_config")
            or row.get("gitpartner_config")
            or "GitPartner/configs/partner.json"
        ),
        "control_channel": str(row.get("control_channel") or "main"),
        "result_channel": str(
            row.get("result_channel") or row.get("control_channel") or "main"
        ),
        "channel_mode": str(row.get("channel_mode") or "isolated-worktree"),
        "remote_root": str(environment["remote_root"]),
        "engine_root": str(environment["engine_root"]),
        "cache_root": str(environment["cache_root"]),
        "capabilities": deepcopy(environment["capabilities"]),
        "tags": tags,
        "import_login_network_environment": bool(
            node["import_login_network_environment"]
        ),
    }
    identity = {
        **normalized,
        "gateway_generation": gateway["generation"] if gateway else "",
        "node_generation": node["generation"],
        "environment_generation": environment["generation"],
    }
    return GPEndpoint(
        **normalized,
        generation=str(row.get("generation") or canonical_digest(identity)),
    )


def _legacy_transport_mode(value: str) -> str:
    normalized = value.lower()
    if normalized == "direct":
        return "direct-git"
    if normalized in {"relay", "auto"}:
        return "lan-relay"
    raise EndpointRegistryError(f"invalid legacy endpoint transport: {value}")


def _list(root: dict[str, Any], key: str) -> list[object]:
    value = root.get(key, [])
    if not isinstance(value, list):
        raise EndpointRegistryError(f"{key} must be a list")
    return value


def _row(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EndpointRegistryError(f"each {label} entry must be an object")
    return value


def _optional_token(value: dict[str, Any], key: str) -> str:
    raw = str(value.get(key) or "")
    safe = "".join(char if char.isalnum() or char in "._-" else "_" for char in raw)
    safe = safe.strip("._-")
    if raw and safe != raw:
        raise EndpointRegistryError(f"{key} must be a safe token when present")
    return raw


def _optional_ssh_target(value: dict[str, Any], key: str) -> str:
    raw = str(value.get(key) or "").strip()
    if raw and not re.fullmatch(
        r"[A-Za-z0-9._-]+@[A-Za-z0-9._:-]+",
        raw,
    ):
        raise EndpointRegistryError(
            f"{key} must be user@host with no options or shell syntax"
        )
    return raw


def _safe_repo_relative(value: str, *, label: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise EndpointRegistryError(f"{label} must be repository-relative")
    return path


def _validate_launcher_runtime_config(
    launcher: GPNodeLauncher,
    endpoint: GPEndpoint,
    config: object,
    *,
    config_path: Path,
) -> None:
    if not isinstance(config, dict):
        raise EndpointRegistryError(
            f"launcher runtime config must be an object: {config_path}"
        )
    repo = config.get("repo") if isinstance(config.get("repo"), dict) else {}
    relay = config.get("relay") if isinstance(config.get("relay"), dict) else {}
    node = config.get("node") if isinstance(config.get("node"), dict) else {}
    endpoint_config = (
        config.get("endpoint")
        if isinstance(config.get("endpoint"), dict)
        else {}
    )
    expected = {
        "repo_dir": ".",
        "control_branch": endpoint.control_channel,
        "result_branch": endpoint.result_channel,
        "node_id": launcher.node_id,
        "endpoint_id": endpoint.endpoint_id,
        "generation": endpoint.generation,
        "role": launcher.role,
        "transport": endpoint.transport,
    }
    observed = {
        "repo_dir": str(config.get("repo_dir") or ""),
        "control_branch": str(repo.get("branch") or ""),
        "result_branch": str(repo.get("result_branch") or ""),
        "node_id": str(node.get("node_id") or ""),
        "endpoint_id": str(endpoint_config.get("endpoint_id") or ""),
        "generation": str(endpoint_config.get("generation") or ""),
        "role": str(relay.get("role") or ""),
        "transport": str(relay.get("transport_mode") or ""),
    }
    mismatches = [
        f"{key}:expected={value!r}:observed={observed[key]!r}"
        for key, value in expected.items()
        if observed[key] != value
    ]
    if launcher.role == "server":
        if str(relay.get("client_ssh") or "") != endpoint.node_ssh:
            mismatches.append("client_ssh")
        if str(relay.get("server_ssh") or ""):
            mismatches.append("server_ssh-must-be-empty")
    elif endpoint.transport == "relay":
        if str(relay.get("server_ssh") or "") != endpoint.gateway_ssh:
            mismatches.append("server_ssh")
        if str(relay.get("client_ssh") or ""):
            mismatches.append("client_ssh-must-be-empty")
    elif str(relay.get("client_ssh") or "") or str(relay.get("server_ssh") or ""):
        mismatches.append("direct-runtime-must-not-contain-ssh")
    if mismatches:
        raise EndpointRegistryError(
            f"launcher runtime config drift for {launcher.node_id}: "
            + ", ".join(mismatches)
        )


def _claim_worktree(
    worktrees: dict[str, str],
    path: str,
    owner: str,
) -> None:
    other = worktrees.get(path)
    if other:
        raise EndpointRegistryError(
            f"enabled worktree owners {other} and {owner} share GP worktree "
            f"{path}"
        )
    worktrees[path] = owner


def _require_capability_value(
    reasons: list[str],
    endpoint: GPEndpoint,
    requirements: dict[str, Any],
    key: str,
) -> None:
    required = _string_set(requirements.get(key))
    if not required or "*" in required:
        return
    available = _string_set(endpoint.capabilities.get(key))
    if "*" not in available and not required.intersection(available):
        reasons.append(
            f"{key}-mismatch:required={','.join(sorted(required))};"
            f"available={','.join(sorted(available)) or 'none'}"
        )


def _string_set(value: object) -> set[str]:
    if value in (None, ""):
        return set()
    if isinstance(value, str):
        return {value}
    if not isinstance(value, (list, tuple, set)):
        raise EndpointRegistryError("capability values must be strings or string lists")
    result = {str(item) for item in value if str(item)}
    return result


def _required_token(value: dict[str, Any], key: str) -> str:
    raw = str(value.get(key) or "")
    safe = "".join(char if char.isalnum() or char in "._-" else "_" for char in raw)
    safe = safe.strip("._-")
    if not safe or safe != raw:
        raise EndpointRegistryError(f"{key} must be a non-empty safe token")
    return safe


def _validate_git_ref(value: str) -> None:
    invalid_tokens = ("..", "@{", "\\", " ", "~", "^", ":", "?", "*", "[")
    if (
        not value
        or any(token in value for token in invalid_tokens)
        or value.startswith(("/", "."))
        or value.endswith(("/", "."))
        or "//" in value
    ):
        raise EndpointRegistryError(f"unsafe GP channel ref: {value!r}")


def _object(root: dict[str, Any], key: str) -> dict[str, Any]:
    value = root.setdefault(key, {})
    if not isinstance(value, dict):
        raise EndpointRegistryError(f"GitPartner config section {key} must be an object")
    return value
