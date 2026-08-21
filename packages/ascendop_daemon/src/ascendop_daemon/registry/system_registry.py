from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig, OperatorSession, operator_session, operator_season
from ascendop_daemon.registry.models import (
    LEGACY_SYSTEM_REGISTRY_SCHEMA, SYSTEM_REGISTRY_SCHEMA,
    SUPPORTED_SYSTEM_REGISTRY_SCHEMAS, BackendEndpoint,
    ExecutionEnvironment, ExecutionNode, ManagementLink,
    RegisteredOperator, RouteDecision, SystemRegistryError, TransportGateway,
)
from ascendop_daemon.registry.routing import route_rejection_reasons
from ascendop_daemon.registry.topology_parser import (
    _claim_worktree, canonical_digest, deep_merge,
    default_workspace, ensure_unique, find_workspace_root,
    normalized_workspace_path, object_value, parse_legacy_endpoint,
    parse_legacy_topology, parse_management_links, parse_v2_topology,
    parse_environment, parse_gateway, parse_node, parse_route_endpoint,
    required_token, slug, string_list, validate_git_ref,
    legacy_transport_mode, transport_for_binding, list_value, object_row,
    optional_token, parse_agent_pools,
)
from ascendop_daemon.registry.actor_registry import (
    parse_actor_registrations,
    parse_role_bindings,
)

class SystemRegistry:
    def __init__(self, path: Path, raw: dict[str, Any]) -> None:
        self.path = path.resolve()
        self.root = find_workspace_root(self.path)
        self.raw = raw
        self.schema = str(raw.get("schema") or "")
        self.registry_generation = str(raw.get("registry_generation") or "")
        defaults = raw.get("operator_defaults", {})
        overrides = raw.get("operator_overrides", {})
        if not isinstance(defaults, dict) or not isinstance(overrides, dict):
            raise SystemRegistryError(
                "operator_defaults and operator_overrides must be objects"
            )
        self.operator_defaults = defaults
        self.operator_overrides = overrides
        self.agent_pools = parse_agent_pools(raw)
        self.actor_registrations = parse_actor_registrations(raw)
        self.role_bindings = parse_role_bindings(
            raw,
            actor_registrations=self.actor_registrations,
        )
        if self.schema == LEGACY_SYSTEM_REGISTRY_SCHEMA:
            (
                self.gateways,
                self.nodes,
                self.environments,
                self.endpoints,
            ) = parse_legacy_topology(raw)
        else:
            (
                self.gateways,
                self.nodes,
                self.environments,
                self.endpoints,
            ) = parse_v2_topology(raw)
        self.management_links = parse_management_links(raw, root=self.root)
        self._validate_endpoints()
        ensure_unique(self.management_links, "link_id", "management link")

    @classmethod
    def load(cls, path: Path) -> "SystemRegistry":
        resolved = path.resolve()
        raw = json.loads(resolved.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise SystemRegistryError("system registry must be a JSON object")
        schema = str(raw.get("schema") or "")
        if schema not in SUPPORTED_SYSTEM_REGISTRY_SCHEMAS:
            raise SystemRegistryError(
                "system registry schema must be one of "
                + ", ".join(sorted(SUPPORTED_SYSTEM_REGISTRY_SCHEMAS))
            )
        return cls(resolved, raw)

    def compile_operators(self, config: DaemonConfig) -> tuple[RegisteredOperator, ...]:
        names: list[str] = []
        for op in (
            *config.operators,
            *config.draining_operators,
            *config.operator_sessions.keys(),
        ):
            if op not in names:
                names.append(op)
        return tuple(self.compile_operator(config, op) for op in names)

    def compile_operator(self, config: DaemonConfig, op: str) -> RegisteredOperator:
        session = operator_session(config, op) or OperatorSession(enabled=False)
        override = self.operator_overrides.get(op, {})
        if not isinstance(override, dict):
            raise SystemRegistryError(f"operator override for {op} must be an object")
        settings = deep_merge(self.operator_defaults, override)
        season = operator_season(config, op)
        operator_id = str(settings.get("operator_id") or f"{slug(season)}.{slug(op)}")
        if op in config.draining_operators or session.drain_requested:
            desired_state = "draining"
        elif op in config.operators and session.enabled:
            desired_state = "enabled"
        else:
            desired_state = "disabled"
        workspace = default_workspace(self.root, op, season, session)
        configured_workspace = settings.get("workspace", {})
        if configured_workspace:
            if not isinstance(configured_workspace, dict):
                raise SystemRegistryError(f"workspace override for {op} must be an object")
            workspace.update(
                {
                    str(key): normalized_workspace_path(self.root, str(value))
                    for key, value in configured_workspace.items()
                }
            )
        requirements = object_value(settings, "execution_requirements")
        cache_policy = object_value(settings, "cache_policy")
        routing_policy = object_value(settings, "routing_policy")
        agent_routing = {
            str(role): str(pool_id)
            for role, pool_id in object_value(settings, "agent_routing").items()
            if str(pool_id)
        }
        self._validate_agent_routing(op, agent_routing)
        bindings: tuple[dict[str, Any], ...] = ()
        definition = {
            "operator_id": operator_id,
            "display_name": op,
            "runtime_operator_name": str(
                settings.get("runtime_operator_name") or op
            ),
            "profiler_kernel_name": str(
                settings.get("profiler_kernel_name")
                or settings.get("runtime_operator_name")
                or op
            ),
            "aliases": sorted({op, *string_list(settings.get("aliases"))}),
            "season": season,
            "desired_state": desired_state,
            "workflow_mode": session.workflow_mode,
            "roles": dict(sorted(session.roles.items())),
            "workspace": workspace,
            "test_profile": str(settings.get("test_profile") or ""),
            "execution_requirements": requirements,
            "cache_policy": cache_policy,
            "routing_policy": routing_policy,
            "agent_routing": agent_routing,
        }
        generation = canonical_digest(definition)
        return RegisteredOperator(
            operator_id=operator_id,
            display_name=op,
            season=season,
            desired_state=desired_state,
            registration_generation=generation,
            definition=definition,
            agent_bindings=bindings,
            agent_routing=agent_routing,
            workspace=workspace,
            test_profile=str(settings.get("test_profile") or ""),
            execution_requirements=requirements,
            cache_policy=cache_policy,
            routing_policy=routing_policy,
        )

    def _validate_agent_routing(
        self, op: str, agent_routing: dict[str, str]
    ) -> None:
        pools = {pool.pool_id: pool for pool in self.agent_pools}
        for role, pool_id in agent_routing.items():
            if role not in {"solver", "tester"}:
                raise SystemRegistryError(
                    f"operator {op} has unsupported Agent role route: {role}"
                )
            pool = pools.get(pool_id)
            if pool is None:
                raise SystemRegistryError(
                    f"operator {op} references unknown Agent pool {pool_id}"
                )
            if role not in pool.roles:
                raise SystemRegistryError(
                    f"Agent pool {pool_id} does not serve {role}"
                )

    def route(self, requirements: dict[str, Any]) -> RouteDecision:
        rows: list[dict[str, Any]] = []
        accepted: list[BackendEndpoint] = []
        for endpoint in self.endpoints:
            reasons = route_rejection_reasons(endpoint, requirements)
            if not reasons:
                accepted.append(endpoint)
            rows.append(
                {
                    "endpoint_id": endpoint.endpoint_id,
                    "node_id": endpoint.node_id,
                    "execution_environment_id": endpoint.execution_environment_id,
                    "priority": endpoint.priority,
                    "accepted": not reasons,
                    "rejection_reasons": reasons,
                }
            )
        selected = (
            sorted(accepted, key=lambda item: (-item.priority, item.endpoint_id))[0]
            if accepted
            else None
        )
        return RouteDecision(selected=selected, candidates=tuple(rows))

    def management_link(self, link_id: str) -> ManagementLink:
        for link in self.management_links:
            if link.link_id == link_id:
                return link
        raise SystemRegistryError(f"unknown management link: {link_id}")

    def _validate_endpoints(self) -> None:
        ids: set[str] = set()
        control_channels: set[str] = set()
        result_channels: set[str] = set()
        worktrees: dict[str, str] = {}
        for endpoint in self.endpoints:
            if endpoint.endpoint_id in ids:
                raise SystemRegistryError(
                    f"duplicate endpoint id: {endpoint.endpoint_id}"
                )
            ids.add(endpoint.endpoint_id)
            if endpoint.transport not in {"direct", "relay", "auto"}:
                raise SystemRegistryError(
                    f"invalid endpoint transport: {endpoint.transport}"
                )
            if endpoint.channel_mode not in {"isolated-worktree", "legacy-shared"}:
                raise SystemRegistryError(
                    f"invalid endpoint channel mode: {endpoint.channel_mode}"
                )
            validate_git_ref(endpoint.control_channel)
            validate_git_ref(endpoint.result_channel)
            if endpoint.channel_mode == "isolated-worktree" and (
                endpoint.control_channel == endpoint.result_channel
                or endpoint.control_channel == "main"
                or endpoint.result_channel == "main"
            ):
                raise SystemRegistryError(
                    f"isolated endpoint {endpoint.endpoint_id} requires distinct non-main channels"
                )
            if endpoint.enabled and endpoint.control_channel in control_channels:
                raise SystemRegistryError(
                    f"duplicate enabled control channel: {endpoint.control_channel}"
                )
            if endpoint.enabled and endpoint.result_channel in result_channels:
                raise SystemRegistryError(
                    f"duplicate enabled result channel: {endpoint.result_channel}"
                )
            control_channels.add(endpoint.control_channel)
            result_channels.add(endpoint.result_channel)
            if endpoint.channel_mode == "isolated-worktree" and endpoint.enabled:
                ingress_path = normalized_workspace_path(
                    self.root,
                    endpoint.gitpartner_repo,
                ).lower()
                _claim_worktree(
                    worktrees,
                    ingress_path,
                    f"{endpoint.endpoint_id}:ingress",
                )
                if endpoint.result_worktree:
                    result_path = normalized_workspace_path(
                        self.root,
                        endpoint.result_worktree,
                    ).lower()
                    _claim_worktree(
                        worktrees,
                        result_path,
                        f"{endpoint.endpoint_id}:result",
                    )
                if (
                    "gp-duplex-lanes-v1"
                    in set(string_list(endpoint.capabilities.get("features")))
                    and not endpoint.result_worktree
                ):
                    raise SystemRegistryError(
                        f"duplex endpoint {endpoint.endpoint_id} requires "
                        "result_worktree"
                    )
