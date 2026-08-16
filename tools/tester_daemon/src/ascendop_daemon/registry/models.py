from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from ascendop_daemon.core.models import DaemonConfig, OperatorSession, operator_session, operator_season


SYSTEM_REGISTRY_SCHEMA = "ascendop.system-registry.v2"
LEGACY_SYSTEM_REGISTRY_SCHEMA = "ascendop.system-registry.v1"
SUPPORTED_SYSTEM_REGISTRY_SCHEMAS = {
    LEGACY_SYSTEM_REGISTRY_SCHEMA,
    SYSTEM_REGISTRY_SCHEMA,
}
VALID_TRANSPORT_BINDING_MODES = {"direct-git", "lan-relay"}
VALID_MANAGEMENT_ACTIONS = {"lan-diagnose"}


class SystemRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class TransportGateway:
    gateway_id: str
    enabled: bool
    draining: bool
    mode: str
    service_node_id: str
    tags: tuple[str, ...]
    generation: str

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["tags"] = list(self.tags)
        return value


@dataclass(frozen=True)
class ExecutionNode:
    node_id: str
    display_name: str
    enabled: bool
    draining: bool
    tags: tuple[str, ...]
    generation: str

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["tags"] = list(self.tags)
        return value


@dataclass(frozen=True)
class ExecutionEnvironment:
    execution_environment_id: str
    node_id: str
    enabled: bool
    draining: bool
    backend_pool: str
    remote_root: str
    engine_root: str
    cache_root: str
    capabilities: dict[str, Any]
    tags: tuple[str, ...]
    generation: str

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["tags"] = list(self.tags)
        return value


@dataclass(frozen=True)
class BackendEndpoint:
    endpoint_id: str
    node_id: str
    execution_environment_id: str
    gateway_id: str
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
    control_channel: str
    result_channel: str
    channel_mode: str
    remote_root: str
    engine_root: str
    cache_root: str
    capabilities: dict[str, Any]
    tags: tuple[str, ...]
    generation: str
    management_mode: str = ""
    management_ssh_alias: str = ""
    management_remote_gitpartner_root: str = ""
    management_remote_runtime_root: str = ""

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["tags"] = list(self.tags)
        return value


@dataclass(frozen=True)
class ManagementLink:
    link_id: str
    enabled: bool
    source_gateway_id: str
    gitpartner_repo: str
    result_worktree: str
    control_channel: str
    result_channel: str
    transport: str
    target_node_id: str
    target_host: str
    target_dir: str
    target_role: str
    allowed_actions: tuple[str, ...]
    append_requests: bool
    generation: str

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["allowed_actions"] = list(self.allowed_actions)
        value["scheduler_eligible"] = False
        value["workflow_ingest"] = False
        return value


@dataclass(frozen=True)
class RegisteredOperator:
    operator_id: str
    display_name: str
    season: str
    desired_state: str
    registration_generation: str
    definition: dict[str, Any]
    agent_bindings: tuple[dict[str, Any], ...]
    workspace: dict[str, str]
    test_profile: str
    execution_requirements: dict[str, Any]
    cache_policy: dict[str, Any]
    routing_policy: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["agent_bindings"] = list(self.agent_bindings)
        return value


@dataclass(frozen=True)
class RouteDecision:
    selected: BackendEndpoint | None
    candidates: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected": self.selected.to_dict() if self.selected else None,
            "candidates": list(self.candidates),
        }
