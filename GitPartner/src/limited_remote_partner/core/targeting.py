from __future__ import annotations

from dataclasses import replace

from limited_remote_partner.core.config import AppConfig
from limited_remote_partner.core.request import ExecutionRequest


def has_explicit_target(request: ExecutionRequest) -> bool:
    return bool(
        request.target_nodes
        or request.target_tags
        or request.target_roles
        or request.target_endpoint_id
        or request.target_environment_id
        or request.target_gateway_id
        or request.target_transport_mode
        or request.registration_generation
    )


def route_request_for_current_runtime(
    config: AppConfig,
    request: ExecutionRequest,
) -> ExecutionRequest | None:
    if not config.routing.enabled:
        return request
    if not has_explicit_target(request):
        return None if config.routing.require_explicit_target else request

    role = config.relay.role.lower()
    if not _runtime_identity_matches(config, request, role):
        return None
    node_id = config.node.node_id or config.routing.target_node
    if role == "server":
        execution_node = _server_execution_node(config, request, node_id)
    else:
        execution_node = _direct_execution_node(config, request, node_id, role)
    if not execution_node:
        return None
    return with_node_output_subdir(request, execution_node)


def _runtime_identity_matches(
    config: AppConfig,
    request: ExecutionRequest,
    role: str,
) -> bool:
    if (
        request.target_gateway_id
        and config.endpoint.gateway_id
        and request.target_gateway_id != config.endpoint.gateway_id
    ):
        return False
    if (
        request.target_transport_mode
        and config.endpoint.transport_mode
        and request.target_transport_mode != config.endpoint.transport_mode
    ):
        return False
    if role == "server":
        return True
    if (
        request.target_endpoint_id
        and request.target_endpoint_id != config.endpoint.endpoint_id
    ):
        return False
    if (
        request.target_environment_id
        and request.target_environment_id
        != config.endpoint.execution_environment_id
    ):
        return False
    if (
        request.registration_generation
        and request.registration_generation != config.endpoint.generation
    ):
        return False
    return True


def with_node_output_subdir(
    request: ExecutionRequest,
    node_id: str,
) -> ExecutionRequest:
    # A single-target request already has a globally unique request id and the
    # existing wait/export contract expects that path unchanged.  Only fanout
    # needs node-scoped output paths to prevent sibling results from colliding.
    if not request.fanout:
        return request
    safe_node = _safe_path_token(node_id or "node")
    base = _strip_node_scope(request.output_subdir or request.request_id)
    return replace(request, output_subdir=f"{base}/nodes/{safe_node}")


def _direct_execution_node(
    config: AppConfig,
    request: ExecutionRequest,
    node_id: str,
    role: str,
) -> str | None:
    if request.target_nodes and node_id in request.target_nodes:
        return node_id
    if _intersects(request.target_tags, config.node.tags):
        return node_id
    if _intersects(request.target_roles, tuple(config.node.roles) + (role,)):
        return node_id
    if request.target_nodes or request.target_tags or request.target_roles:
        return None
    return node_id


def _server_execution_node(
    config: AppConfig,
    request: ExecutionRequest,
    node_id: str,
) -> str | None:
    if request.server_action:
        direct = _direct_execution_node(config, request, node_id, "server")
        if direct:
            return direct
    for candidate in request.target_nodes:
        if candidate in config.routing.served_nodes:
            return candidate
    if _intersects(request.target_tags, config.routing.served_tags):
        return config.routing.served_nodes[0] if config.routing.served_nodes else node_id
    if _intersects(request.target_roles, config.routing.served_roles):
        return config.routing.served_nodes[0] if config.routing.served_nodes else node_id
    if not (request.target_nodes or request.target_tags or request.target_roles):
        return node_id
    return None


def _intersects(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    return bool(set(left).intersection(right))


def _strip_node_scope(path: str) -> str:
    parts = path.replace("\\", "/").strip("/").split("/")
    if "nodes" in parts:
        index = parts.index("nodes")
        if index > 0:
            return "/".join(parts[:index])
    return path.rstrip("/") or "latest"


def _safe_path_token(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "._-" else "_" for char in value)
    return cleaned.strip("._-") or "node"
