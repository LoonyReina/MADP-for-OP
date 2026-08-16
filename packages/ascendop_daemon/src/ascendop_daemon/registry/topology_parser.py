from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import OperatorSession
from ascendop_daemon.registry.management_binding import parse_management_binding
from ascendop_daemon.registry.models import (
    AgentPool, BackendEndpoint, ExecutionEnvironment, ExecutionNode,
    ManagementLink, SystemRegistryError, TransportGateway,
    VALID_MANAGEMENT_ACTIONS, VALID_TRANSPORT_BINDING_MODES,
)
from ascendop_protocol.agent import AGENT_POOL_SCHEMA, validate_agent_pool


def parse_agent_pools(raw: dict[str, Any]) -> tuple[AgentPool, ...]:
    values = list_value(raw, "agent_pools")
    pools: list[AgentPool] = []
    for value in values:
        row = object_row(value, "agent pool")
        normalized = {
            "schema": AGENT_POOL_SCHEMA,
            "pool_id": required_token(row, "pool_id"),
            "enabled": bool(row.get("enabled", True)),
            "roles": sorted(set(string_list(row.get("roles")))),
            "drivers": list(dict.fromkeys(string_list(row.get("drivers")))),
            "required_capabilities": {
                str(key): required
                for key, required in sorted(
                    object_value(row, "required_capabilities").items()
                )
            },
            "priority": int(row.get("priority", 100)),
            "registration_generation": str(
                row.get("registration_generation") or "pending"
            ),
        }
        if normalized["registration_generation"] == "pending":
            normalized["registration_generation"] = canonical_digest(
                {
                    key: item
                    for key, item in normalized.items()
                    if key != "registration_generation"
                }
            )
        pool = validate_agent_pool(normalized)
        pools.append(
            AgentPool(
                pool_id=pool["pool_id"],
                enabled=pool["enabled"],
                roles=tuple(pool["roles"]),
                drivers=tuple(pool["drivers"]),
                required_capabilities=dict(pool["required_capabilities"]),
                priority=int(pool["priority"]),
                generation=pool["registration_generation"],
            )
        )
    ensure_unique(tuple(pools), "pool_id", "agent pool")
    return tuple(pools)

def parse_legacy_endpoint(value: object) -> BackendEndpoint:
    if not isinstance(value, dict):
        raise SystemRegistryError("each gp_endpoints entry must be an object")
    capabilities = object_value(value, "capabilities")
    normalized = {
        "endpoint_id": required_token(value, "endpoint_id"),
        "node_id": required_token(value, "node_id"),
        "execution_environment_id": required_token(
            value, "execution_environment_id"
        ),
        "gateway_id": "",
        "transport_mode": legacy_transport_mode(
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
        "transport": str(value.get("transport") or "relay"),
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
        "tags": tuple(sorted(string_list(value.get("tags")))),
    }
    return BackendEndpoint(
        **normalized,
        generation=str(value.get("generation") or canonical_digest(normalized)),
    )


def parse_legacy_topology(
    raw: dict[str, Any],
) -> tuple[
    tuple[TransportGateway, ...],
    tuple[ExecutionNode, ...],
    tuple[ExecutionEnvironment, ...],
    tuple[BackendEndpoint, ...],
]:
    values = raw.get("gp_endpoints", [])
    if not isinstance(values, list):
        raise SystemRegistryError("gp_endpoints must be a list")
    endpoints = tuple(parse_legacy_endpoint(value) for value in values)
    gateways: list[TransportGateway] = []
    nodes: dict[str, ExecutionNode] = {}
    environments: dict[str, ExecutionEnvironment] = {}
    next_endpoints: list[BackendEndpoint] = []
    for endpoint in endpoints:
        gateway_id = ""
        if endpoint.transport_mode == "lan-relay":
            gateway_id = f"legacy-gateway-{endpoint.endpoint_id}"
            gateway_value = {
                "gateway_id": gateway_id,
                "enabled": endpoint.enabled,
                "draining": endpoint.draining,
                "mode": "lan-relay",
                "service_node_id": "",
                "tags": endpoint.tags,
            }
            gateways.append(
                TransportGateway(
                    **gateway_value,
                    generation=canonical_digest(gateway_value),
                )
            )
        node_value = {
            "node_id": endpoint.node_id,
            "display_name": endpoint.node_id,
            "enabled": endpoint.enabled,
            "draining": endpoint.draining,
            "tags": endpoint.tags,
        }
        nodes.setdefault(
            endpoint.node_id,
            ExecutionNode(
                **node_value,
                generation=canonical_digest(node_value),
            ),
        )
        environment_value = {
            "execution_environment_id": endpoint.execution_environment_id,
            "node_id": endpoint.node_id,
            "enabled": endpoint.enabled,
            "draining": endpoint.draining,
            "backend_pool": endpoint.backend_pool,
            "remote_root": endpoint.remote_root,
            "engine_root": endpoint.engine_root,
            "cache_root": endpoint.cache_root,
            "capabilities": endpoint.capabilities,
            "tags": endpoint.tags,
        }
        environments.setdefault(
            endpoint.execution_environment_id,
            ExecutionEnvironment(
                **environment_value,
                generation=canonical_digest(environment_value),
            ),
        )
        next_endpoints.append(
            BackendEndpoint(
                **{
                    **asdict(endpoint),
                    "gateway_id": gateway_id,
                }
            )
        )
    return (
        tuple(gateways),
        tuple(nodes.values()),
        tuple(environments.values()),
        tuple(next_endpoints),
    )


def parse_v2_topology(
    raw: dict[str, Any],
) -> tuple[
    tuple[TransportGateway, ...],
    tuple[ExecutionNode, ...],
    tuple[ExecutionEnvironment, ...],
    tuple[BackendEndpoint, ...],
]:
    gateway_values = list_value(raw, "transport_gateways")
    node_values = list_value(raw, "execution_nodes")
    environment_values = list_value(raw, "execution_environments")
    endpoint_values = list_value(raw, "route_endpoints")

    gateways = tuple(parse_gateway(value) for value in gateway_values)
    nodes = tuple(parse_node(value) for value in node_values)
    environments = tuple(parse_environment(value) for value in environment_values)
    ensure_unique(gateways, "gateway_id", "gateway")
    ensure_unique(nodes, "node_id", "execution node")
    ensure_unique(
        environments,
        "execution_environment_id",
        "execution environment",
    )
    gateway_map = {item.gateway_id: item for item in gateways}
    node_map = {item.node_id: item for item in nodes}
    environment_map = {
        item.execution_environment_id: item for item in environments
    }
    for environment in environments:
        if environment.node_id not in node_map:
            raise SystemRegistryError(
                f"execution environment {environment.execution_environment_id} "
                f"references unknown node {environment.node_id}"
            )
    endpoints = tuple(
        parse_route_endpoint(
            value,
            gateways=gateway_map,
            nodes=node_map,
            environments=environment_map,
        )
        for value in endpoint_values
    )
    return gateways, nodes, environments, endpoints


def parse_gateway(value: object) -> TransportGateway:
    row = object_row(value, "transport gateway")
    mode = str(row.get("mode") or "")
    if mode != "lan-relay":
        raise SystemRegistryError(
            "transport gateway mode must be lan-relay; direct-git has no gateway"
        )
    normalized = {
        "gateway_id": required_token(row, "gateway_id"),
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "mode": mode,
        "service_node_id": optional_token(row, "service_node_id"),
        "tags": tuple(sorted(string_list(row.get("tags")))),
    }
    return TransportGateway(
        **normalized,
        generation=str(row.get("generation") or canonical_digest(normalized)),
    )


def parse_node(value: object) -> ExecutionNode:
    row = object_row(value, "execution node")
    node_id = required_token(row, "node_id")
    normalized = {
        "node_id": node_id,
        "display_name": str(row.get("display_name") or node_id),
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "tags": tuple(sorted(string_list(row.get("tags")))),
    }
    return ExecutionNode(
        **normalized,
        generation=str(row.get("generation") or canonical_digest(normalized)),
    )


def parse_environment(value: object) -> ExecutionEnvironment:
    row = object_row(value, "execution environment")
    capabilities = object_value(row, "capabilities")
    normalized = {
        "execution_environment_id": required_token(
            row, "execution_environment_id"
        ),
        "node_id": required_token(row, "node_id"),
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "backend_pool": str(row.get("backend_pool") or ""),
        "remote_root": str(row.get("remote_root") or ""),
        "engine_root": str(row.get("engine_root") or "test_engine_demo"),
        "cache_root": str(row.get("cache_root") or ""),
        "capabilities": capabilities,
        "tags": tuple(sorted(string_list(row.get("tags")))),
    }
    return ExecutionEnvironment(
        **normalized,
        generation=str(row.get("generation") or canonical_digest(normalized)),
    )


def parse_route_endpoint(
    value: object,
    *,
    gateways: dict[str, TransportGateway],
    nodes: dict[str, ExecutionNode],
    environments: dict[str, ExecutionEnvironment],
) -> BackendEndpoint:
    row = object_row(value, "route endpoint")
    endpoint_id = required_token(row, "endpoint_id")
    node_id = required_token(row, "node_id")
    environment_id = required_token(row, "execution_environment_id")
    node = nodes.get(node_id)
    if node is None:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} references unknown node {node_id}"
        )
    environment = environments.get(environment_id)
    if environment is None:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} references unknown environment "
            f"{environment_id}"
        )
    if environment.node_id != node_id:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} binds environment {environment_id} "
            f"to node {node_id}, expected {environment.node_id}"
        )
    binding = object_value(row, "transport_binding")
    mode = str(binding.get("mode") or "")
    if mode not in VALID_TRANSPORT_BINDING_MODES:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} has invalid transport binding mode {mode!r}"
        )
    gateway_id = optional_token(binding, "gateway_id")
    gateway: TransportGateway | None = None
    if mode == "lan-relay":
        if not gateway_id:
            raise SystemRegistryError(
                f"relayed endpoint {endpoint_id} requires gateway_id"
            )
        gateway = gateways.get(gateway_id)
        if gateway is None:
            raise SystemRegistryError(
                f"route endpoint {endpoint_id} references unknown gateway {gateway_id}"
            )
    elif gateway_id:
        raise SystemRegistryError(
            f"direct endpoint {endpoint_id} cannot declare gateway_id"
        )
    tags = tuple(
        sorted(
            {
                *node.tags,
                *environment.tags,
                *(gateway.tags if gateway else ()),
                *string_list(row.get("tags")),
            }
        )
    )
    gitpartner_config = str(
        row.get("gitpartner_config") or "GitPartner/configs/partner.json"
    )
    node_gitpartner_config = str(
        row.get("node_gitpartner_config") or gitpartner_config
    )
    for value, field in (
        (gitpartner_config, "gitpartner_config"),
        (node_gitpartner_config, "node_gitpartner_config"),
    ):
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or ".." in candidate.parts
            or any(token in value for token in ("\x00", "\n", "\r"))
        ):
            raise SystemRegistryError(
                f"route endpoint {endpoint_id} {field} must be a safe "
                "workspace-relative path"
            )
    management_fields = parse_management_binding(
        object_value(row, "management_binding"),
        endpoint_id=endpoint_id,
        transport_mode=mode,
        transport_gateway_id=gateway_id,
        environment_remote_root=environment.remote_root,
    )
    normalized = {
        "endpoint_id": endpoint_id,
        "node_id": node_id,
        "execution_environment_id": environment_id,
        "gateway_id": gateway_id,
        "transport_mode": mode,
        "enabled": bool(row.get("enabled", True)),
        "draining": bool(row.get("draining", False)),
        "gateway_enabled": gateway.enabled if gateway else True,
        "gateway_draining": gateway.draining if gateway else False,
        "node_enabled": node.enabled,
        "node_draining": node.draining,
        "environment_enabled": environment.enabled,
        "environment_draining": environment.draining,
        "priority": int(row.get("priority", 0)),
        "backend_pool": environment.backend_pool,
        "transport": transport_for_binding(mode),
        "gitpartner_repo": str(row.get("gitpartner_repo") or "GitPartner"),
        "result_worktree": str(row.get("result_worktree") or ""),
        "gitpartner_config": gitpartner_config,
        "node_gitpartner_config": node_gitpartner_config,
        "control_channel": str(row.get("control_channel") or "main"),
        "result_channel": str(
            row.get("result_channel") or row.get("control_channel") or "main"
        ),
        "channel_mode": str(row.get("channel_mode") or "isolated-worktree"),
        "remote_root": environment.remote_root,
        "engine_root": environment.engine_root,
        "cache_root": environment.cache_root,
        "capabilities": environment.capabilities,
        "tags": tags,
        **management_fields,
    }
    identity = {
        **normalized,
        "gateway_generation": gateway.generation if gateway else "",
        "node_generation": node.generation,
        "environment_generation": environment.generation,
    }
    return BackendEndpoint(
        **normalized,
        generation=str(row.get("generation") or canonical_digest(identity)),
    )


def parse_management_links(
    raw: dict[str, Any],
    *,
    root: Path,
) -> tuple[ManagementLink, ...]:
    links: list[ManagementLink] = []
    for value in list_value(raw, "management_links"):
        row = object_row(value, "management link")
        link_id = required_token(row, "link_id")
        transport = str(row.get("transport") or "auto")
        if transport not in {"direct", "relay", "auto"}:
            raise SystemRegistryError(
                f"management link {link_id} has invalid transport {transport!r}"
            )
        target_role = str(row.get("target_role") or "client")
        if target_role != "client":
            raise SystemRegistryError(
                f"management link {link_id} target_role must be client"
            )
        target_host = str(row.get("target_host") or "")
        if not re.fullmatch(
            r"[A-Za-z0-9._-]+@[A-Za-z0-9._:\[\]-]+",
            target_host,
        ):
            raise SystemRegistryError(
                f"management link {link_id} target_host must be user@host"
            )
        target_dir = str(row.get("target_dir") or "")
        if (
            not target_dir.startswith("/")
            or "\x00" in target_dir
            or "\n" in target_dir
            or "\r" in target_dir
        ):
            raise SystemRegistryError(
                f"management link {link_id} target_dir must be an absolute POSIX path"
            )
        allowed_actions = tuple(
            sorted(set(string_list(row.get("allowed_actions") or ["lan-diagnose"])))
        )
        unsupported = sorted(set(allowed_actions) - VALID_MANAGEMENT_ACTIONS)
        if unsupported:
            raise SystemRegistryError(
                f"management link {link_id} has unsupported actions: "
                + ", ".join(unsupported)
            )
        if not allowed_actions:
            raise SystemRegistryError(
                f"management link {link_id} requires at least one allowed action"
            )
        control_channel = str(row.get("control_channel") or "main")
        result_channel = str(row.get("result_channel") or control_channel)
        validate_git_ref(control_channel)
        validate_git_ref(result_channel)
        gitpartner_repo = normalized_workspace_path(
            root,
            str(row.get("gitpartner_repo") or "GitPartner"),
        )
        result_worktree = normalized_workspace_path(
            root,
            str(row.get("result_worktree") or gitpartner_repo),
        )
        normalized = {
            "link_id": link_id,
            "enabled": bool(row.get("enabled", True)),
            "source_gateway_id": optional_token(row, "source_gateway_id"),
            "gitpartner_repo": gitpartner_repo,
            "result_worktree": result_worktree,
            "control_channel": control_channel,
            "result_channel": result_channel,
            "transport": transport,
            "target_node_id": required_token(row, "target_node_id"),
            "target_host": target_host,
            "target_dir": target_dir,
            "target_role": target_role,
            "allowed_actions": allowed_actions,
            "append_requests": bool(row.get("append_requests", True)),
        }
        links.append(
            ManagementLink(
                **normalized,
                generation=str(
                    row.get("generation") or canonical_digest(normalized)
                ),
            )
        )
    return tuple(links)


def legacy_transport_mode(value: str) -> str:
    normalized = value.lower()
    if normalized == "direct":
        return "direct-git"
    if normalized in {"relay", "auto"}:
        return "lan-relay"
    raise SystemRegistryError(f"invalid legacy endpoint transport: {value}")


def transport_for_binding(mode: str) -> str:
    return "direct" if mode == "direct-git" else "relay"


def list_value(root: dict[str, Any], key: str) -> list[object]:
    value = root.get(key, [])
    if not isinstance(value, list):
        raise SystemRegistryError(f"{key} must be a list")
    return value


def object_row(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemRegistryError(f"each {label} entry must be an object")
    return value


def optional_token(value: dict[str, Any], key: str) -> str:
    raw = str(value.get(key) or "")
    if raw and not re.fullmatch(r"[A-Za-z0-9._-]+", raw):
        raise SystemRegistryError(f"{key} must be a safe token when present")
    return raw


def ensure_unique(items: tuple[Any, ...], field: str, label: str) -> None:
    seen: set[str] = set()
    for item in items:
        identity = str(getattr(item, field))
        if identity in seen:
            raise SystemRegistryError(f"duplicate {label} id: {identity}")
        seen.add(identity)


def _claim_worktree(
    worktrees: dict[str, str],
    path: str,
    owner: str,
) -> None:
    other = worktrees.get(path)
    if other:
        raise SystemRegistryError(
            f"enabled worktree owners {other} and {owner} share GP worktree "
            f"{path}"
        )
    worktrees[path] = owner


def default_workspace(
    root: Path, op: str, season: str, session: OperatorSession
) -> dict[str, str]:
    values = {
        "source": root / "operators_workspace" / op,
        "official_case": root / "operators" / season / "case_910b" / op,
        "casegen": root / "TestUtils" / "casegen" / op,
        "pending": root / "TestUtils" / "pending" / op,
        "submit": root / "TestUtils" / "submit" / op,
        "result": root / "operators_testresult" / op,
        "knowledge": root / (session.knowledge_root or f"reference/op_knowledge/{op}"),
    }
    return {key: normalized_workspace_path(root, str(path)) for key, path in values.items()}


def agent_bindings(session: OperatorSession) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for role, thread_id, model, effort, enabled in (
        (
            "solver",
            session.solver_thread_id,
            session.solver_model,
            session.solver_thinking,
            bool(session.solver_thread_id),
        ),
        (
            "tester",
            session.tester_thread_id,
            session.tester_model,
            session.tester_thinking,
            bool(session.tester_thread_id and session.roles.get("casegen", False)),
        ),
    ):
        rows.append(
            {
                "role": role,
                "adapter": "codex-ide-session",
                "session_id": thread_id,
                "model": model,
                "effort": effort,
                "enabled": enabled,
            }
        )
    return tuple(rows)


def normalized_workspace_path(root: Path, value: str) -> str:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    if resolved != root and root not in resolved.parents:
        raise SystemRegistryError(f"registered workspace escapes root: {value}")
    return resolved.relative_to(root).as_posix()


def deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def canonical_digest(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def object_value(value: dict[str, Any], key: str) -> dict[str, Any]:
    result = value.get(key, {})
    if not isinstance(result, dict):
        raise SystemRegistryError(f"{key} must be an object")
    return deepcopy(result)


def string_list(value: object) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, (list, tuple, set)):
        raise SystemRegistryError("string list value has invalid type")
    return [str(item) for item in value if str(item)]


def required_token(value: dict[str, Any], key: str) -> str:
    raw = str(value.get(key) or "")
    if not raw or not re.fullmatch(r"[A-Za-z0-9._-]+", raw):
        raise SystemRegistryError(f"{key} must be a non-empty safe token")
    return raw


def validate_git_ref(value: str) -> None:
    invalid = ("..", "@{", "\\", " ", "~", "^", ":", "?", "*", "[")
    if (
        not value
        or any(token in value for token in invalid)
        or value.startswith(("/", "."))
        or value.endswith(("/", "."))
        or "//" in value
    ):
        raise SystemRegistryError(f"unsafe GP channel ref: {value!r}")


def slug(value: str) -> str:
    result = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if not result:
        raise SystemRegistryError(f"cannot derive registry id from {value!r}")
    return result


def find_workspace_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "Develop").is_dir() and (parent / "tools").is_dir():
            return parent
    return path.parent
