from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import sqlite3
from typing import Any

from ascendop_daemon.registry.system_registry import BackendEndpoint, SystemRegistry


class EngineRouteError(ValueError):
    pass


@dataclass(frozen=True)
class EngineTransportRoute:
    gitpartner_repo: str
    engine_root: str
    remote_root: str
    transport: str
    result_worktree: str = ""
    endpoint_id: str = ""
    node_id: str = ""
    execution_environment_id: str = ""
    gateway_id: str = ""
    transport_mode: str = ""
    registration_generation: str = ""
    control_channel: str = ""
    result_channel: str = ""
    append_requests: bool = False
    duplex_lanes: bool = False
    remote_gitpartner_repo: str = "ascend-git-partner"
    device_inventory: tuple[dict[str, Any], ...] = ()
    registered: bool = False
    management_mode: str = ""
    management_ssh_alias: str = ""
    management_remote_gitpartner_root: str = ""
    management_remote_runtime_root: str = ""
    management_service_environment_file: str = ""
    management_cann_environment_script: str = ""
    management_gateway_id: str = ""
    management_target_repo_relative: str = ""
    management_runtime_root_relative: str = ""
    management_drain_timeout_seconds: int = 0


def resolve_registered_engine_route(
    root: Path,
    *,
    registry_path: str,
    endpoint_id: str,
    control_database_path: str = ".ascendop-work/runtime/control.sqlite3",
) -> EngineTransportRoute:
    registry_file = Path(registry_path)
    if not registry_file.is_absolute():
        registry_file = root / registry_file
    registry = SystemRegistry.load(registry_file)
    endpoint = next(
        (item for item in registry.endpoints if item.endpoint_id == endpoint_id),
        None,
    )
    if endpoint is None:
        raise EngineRouteError(f"unknown Engine route endpoint: {endpoint_id}")
    route = route_from_endpoint(endpoint)
    database_file = Path(control_database_path)
    if not database_file.is_absolute():
        database_file = root / database_file
    if not database_file.exists():
        return route
    if database_is_flow_v3(database_file):
        return route
    from ascendop_daemon.control_plane.control_database import ControlDatabase

    database = ControlDatabase(database_file)
    live_capabilities = database.live_node_capabilities(route.node_id)
    inventory = device_inventory_from_capabilities(live_capabilities)
    if not inventory:
        inventory = device_inventory_from_capabilities(
            database.last_known_node_capabilities(route.node_id)
        )
    if not inventory:
        return route
    return replace(route, device_inventory=inventory)


def database_is_flow_v3(path: Path) -> bool:
    try:
        with sqlite3.connect(str(path)) as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='flow_v3_metadata'"
            ).fetchone()
            return row is not None
    except sqlite3.Error:
        return False


def route_from_endpoint(endpoint: BackendEndpoint) -> EngineTransportRoute:
    blockers: list[str] = []
    if not endpoint.enabled:
        blockers.append("endpoint disabled")
    if endpoint.draining:
        blockers.append("endpoint draining")
    if not endpoint.gateway_enabled:
        blockers.append("gateway disabled")
    if endpoint.gateway_draining:
        blockers.append("gateway draining")
    if not endpoint.node_enabled:
        blockers.append("node disabled")
    if endpoint.node_draining:
        blockers.append("node draining")
    if not endpoint.environment_enabled:
        blockers.append("environment disabled")
    if endpoint.environment_draining:
        blockers.append("environment draining")
    if blockers:
        raise EngineRouteError(
            f"Engine route endpoint {endpoint.endpoint_id} is unavailable: "
            + ", ".join(blockers)
        )
    features = {
        str(item)
        for item in endpoint.capabilities.get("features", [])
        if str(item)
    }
    device_inventory = device_inventory_from_capabilities(endpoint.capabilities)
    return EngineTransportRoute(
        gitpartner_repo=endpoint.gitpartner_repo,
        result_worktree=endpoint.result_worktree,
        engine_root=endpoint.engine_root,
        remote_root=endpoint.remote_root,
        transport=endpoint.transport,
        endpoint_id=endpoint.endpoint_id,
        node_id=endpoint.node_id,
        execution_environment_id=endpoint.execution_environment_id,
        gateway_id=endpoint.gateway_id,
        transport_mode=endpoint.transport_mode,
        registration_generation=endpoint.generation,
        control_channel=endpoint.control_channel,
        result_channel=endpoint.result_channel,
        append_requests="gp-append-request-v1" in features,
        duplex_lanes="gp-duplex-lanes-v1" in features,
        remote_gitpartner_repo=str(
            endpoint.capabilities.get("gitpartner_runtime_repo")
            or "ascend-git-partner"
        ),
        device_inventory=device_inventory,
        registered=True,
        management_mode=endpoint.management_mode,
        management_ssh_alias=endpoint.management_ssh_alias,
        management_remote_gitpartner_root=(
            endpoint.management_remote_gitpartner_root
        ),
        management_remote_runtime_root=endpoint.management_remote_runtime_root,
        management_service_environment_file=(
            endpoint.management_service_environment_file
        ),
        management_cann_environment_script=(
            endpoint.management_cann_environment_script
        ),
        management_gateway_id=endpoint.management_gateway_id,
        management_target_repo_relative=(
            endpoint.management_target_repo_relative
        ),
        management_runtime_root_relative=(
            endpoint.management_runtime_root_relative
        ),
        management_drain_timeout_seconds=(
            endpoint.management_drain_timeout_seconds
        ),
    )


def device_inventory_from_capabilities(
    capabilities: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    raw_inventory = capabilities.get("device_inventory", [])
    if isinstance(raw_inventory, list) and raw_inventory:
        return tuple(
            {
                "device_id": str(item.get("device_id") or ""),
                "physical_device_id": str(
                    item.get("physical_device_id") or ""
                ),
                "enabled": bool(item.get("enabled", True)),
                "draining": bool(item.get("draining", False)),
            }
            for item in raw_inventory
            if isinstance(item, dict) and str(item.get("device_id") or "")
        )
    return ()


def legacy_engine_route(policy: dict[str, Any], remote_root: str) -> EngineTransportRoute:
    return EngineTransportRoute(
        gitpartner_repo=str(
            policy.get("test_engine_gitpartner_repo", "GitPartner") or "GitPartner"
        ),
        result_worktree=str(
            policy.get("test_engine_result_worktree")
            or policy.get("test_engine_gitpartner_repo", "GitPartner")
            or "GitPartner"
        ),
        engine_root=str(
            policy.get("test_engine_root", "test_engine_demo") or "test_engine_demo"
        ),
        remote_root=str(policy.get("test_engine_remote_root") or remote_root),
        transport=str(
            policy.get("gitpartner_request_transport", "relay") or "relay"
        ),
        remote_gitpartner_repo=str(
            policy.get(
                "test_engine_remote_gitpartner_repo",
                "ascend-git-partner",
            )
            or "ascend-git-partner"
        ),
    )
