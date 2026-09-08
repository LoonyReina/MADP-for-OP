from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_protocol.automation import (
    ASSISTANT_ACTION_REQUEST_SCHEMA,
    evaluate_trigger_rule,
    validate_action_receipt,
    validate_action_request,
)
from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.registry.system_registry import (
    SYSTEM_REGISTRY_SCHEMA,
    RouteDecision,
    SystemRegistry,
)
from ascendop_daemon.storage.row_decoders import (
    decode_assistant_action,
    decode_assistant_receipt,
    decode_attempt_row,
    decode_operator_row,
    decode_outbox_row,
    decode_request_row,
    decode_return_row,
)
from ascendop_daemon.storage.control_types import (
    ACTIVE_ATTEMPT_STATES,
    ACTIVE_NODE_STATES,
    BOOTSTRAP_CONTROL_PROBE_POLICY,
    OUTBOX_ACTIVE_STATES,
    OUTBOX_CLAIMABLE_STATES,
    SCHEMA_VERSION,
    SYSTEM_EXPERIMENT_GENERATION,
    SYSTEM_EXPERIMENT_OPERATOR_ID,
    ControlDatabaseError,
)
from ascendop_daemon.storage.control_validation import (
    _ensure_column,
    _format_timestamp,
    _is_bootstrap_control_probe,
    _node_report_is_newer,
    _normalized_node_lease,
    _parse_timestamp,
    _require_transport_claim,
    _runtime_route_rejection_reasons,
    _validate_node_report,
    _validate_observed_node_against_registry,
    _validate_transport_identity,
    canonical_json,
    utc_now,
)
from ascendop_daemon.storage.migrations import (
    migrate_agent_iteration_candidate_index_v2,
    migrate_agent_output_contract_v1,
    migrate_transport_protocol_v3,
)
from ascendop_daemon.storage.repositories.schema_sql import CONTROL_SCHEMA_SQL

class SchemaRegistryRepository:
    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            metadata_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata'"
            ).fetchone()
            existing = (
                conn.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
                if metadata_exists else None
            )
            if existing is not None and int(existing[0]) > SCHEMA_VERSION:
                raise ControlDatabaseError(
                    f"unsupported control database schema: {existing[0]}"
                )
            conn.executescript(CONTROL_SCHEMA_SQL)
            _ensure_column(
                conn,
                "agent_registrations_v4",
                "manager_runner_id",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "workflow_actions",
                "execution_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                conn,
                "backend_endpoints",
                "gateway_id",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "transport_protocol",
                "TEXT NOT NULL DEFAULT ''",
            )
            for column, definition in (
                ("projection_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("disposition", "TEXT NOT NULL DEFAULT 'unprojected'"),
                ("hold_reason", "TEXT NOT NULL DEFAULT ''"),
                ("terminal_revision", "INTEGER NOT NULL DEFAULT 0"),
                ("recovery_id", "TEXT NOT NULL DEFAULT ''"),
            ):
                _ensure_column(conn, "transport_returns", column, definition)
            _ensure_column(
                conn,
                "transport_outbox",
                "envelope_path",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "envelope_digest",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "package_root",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "claim_token",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "delivery_attempts",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "remote_receipt_json",
                "TEXT NOT NULL DEFAULT '{}'",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "next_attempt_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "next_poll_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "poll_attempts",
                "INTEGER NOT NULL DEFAULT 0",
            )
            query_sequence_added = _ensure_column(
                conn,
                "transport_outbox",
                "query_sequence",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "query_inflight_sequence",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "query_inflight_owner_generation",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "last_polled_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "accepted_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "transport_outbox",
                "completed_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                conn,
                "backend_endpoints",
                "transport_mode",
                "TEXT NOT NULL DEFAULT ''",
            )
            if existing is not None and int(existing[0]) < 7:
                migrate_transport_protocol_v3(conn, self._event)
            migrate_agent_output_contract_v1(conn, self._event)
            migrate_agent_iteration_candidate_index_v2(conn, self._event)
            if query_sequence_added:
                conn.execute(
                    "UPDATE transport_outbox SET query_sequence=CASE "
                    "WHEN poll_attempts>=30 THEN poll_attempts+1 "
                    "ELSE poll_attempts END WHERE last_polled_at<>''"
                )
            conn.execute(
                "INSERT INTO metadata(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),),
            )

    def reconcile(self, config: DaemonConfig, registry: SystemRegistry) -> dict[str, Any]:
        self.initialize()
        for registration in registry.actor_registrations:
            self.register_agent(
                registration,
                health_state="ready",
                manager_runner_id="flow-v5-role-binding",
                lease_seconds=300,
            )
        now = utc_now()
        operators = registry.compile_operators(config)
        with self.transaction() as conn:
            conn.execute("UPDATE operator_registrations SET source_present = 0")
            conn.execute("UPDATE agent_pools_v4 SET source_present = 0")
            conn.execute("UPDATE transport_gateways SET source_present = 0")
            conn.execute("UPDATE execution_nodes_desired SET source_present = 0")
            conn.execute("UPDATE execution_environments SET source_present = 0")
            conn.execute("UPDATE backend_endpoints SET source_present = 0")
            for operator in operators:
                conn.execute(
                    """
                    INSERT INTO operator_registrations(
                        operator_id, display_name, season, desired_state,
                        registration_generation, definition_json, workspace_json,
                        requirements_json, cache_policy_json, routing_policy_json,
                        test_profile, source_present, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    ON CONFLICT(operator_id) DO UPDATE SET
                        display_name=excluded.display_name,
                        season=excluded.season,
                        desired_state=excluded.desired_state,
                        registration_generation=excluded.registration_generation,
                        definition_json=excluded.definition_json,
                        workspace_json=excluded.workspace_json,
                        requirements_json=excluded.requirements_json,
                        cache_policy_json=excluded.cache_policy_json,
                        routing_policy_json=excluded.routing_policy_json,
                        test_profile=excluded.test_profile,
                        source_present=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        operator.operator_id,
                        operator.display_name,
                        operator.season,
                        operator.desired_state,
                        operator.registration_generation,
                        canonical_json(operator.definition),
                        canonical_json(operator.workspace),
                        canonical_json(operator.execution_requirements),
                        canonical_json(operator.cache_policy),
                        canonical_json(operator.routing_policy),
                        operator.test_profile,
                        now,
                    ),
                )
                conn.execute(
                    "DELETE FROM agent_bindings WHERE operator_id = ?",
                    (operator.operator_id,),
                )
            for pool in registry.agent_pools:
                value = pool.to_dict()
                conn.execute(
                    """
                    INSERT INTO agent_pools_v4(
                        pool_id, enabled, priority, registration_generation,
                        roles_json, drivers_json, required_capabilities_json,
                        config_json, source_present, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(pool_id) DO UPDATE SET
                        enabled=excluded.enabled,
                        priority=excluded.priority,
                        registration_generation=excluded.registration_generation,
                        roles_json=excluded.roles_json,
                        drivers_json=excluded.drivers_json,
                        required_capabilities_json=excluded.required_capabilities_json,
                        config_json=excluded.config_json,
                        source_present=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        pool.pool_id,
                        int(pool.enabled),
                        pool.priority,
                        pool.generation,
                        canonical_json(list(pool.roles)),
                        canonical_json(list(pool.drivers)),
                        canonical_json(pool.required_capabilities),
                        canonical_json(value),
                        now,
                        now,
                    ),
                )
            conn.execute(
                "UPDATE agent_pools_v4 SET enabled=0, source_present=0, updated_at=? "
                "WHERE source_present=0",
                (now,),
            )
            conn.execute(
                "UPDATE operator_registrations SET desired_state='archived', updated_at=? "
                "WHERE source_present=0",
                (now,),
            )
            for gateway in registry.gateways:
                gateway_value = gateway.to_dict()
                conn.execute(
                    """
                    INSERT INTO transport_gateways(
                        gateway_id, enabled, draining, mode, service_node_id,
                        registration_generation, config_json, source_present,
                        updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, 1, ?)
                    ON CONFLICT(gateway_id) DO UPDATE SET
                        enabled=excluded.enabled,
                        draining=excluded.draining,
                        mode=excluded.mode,
                        service_node_id=excluded.service_node_id,
                        registration_generation=excluded.registration_generation,
                        config_json=excluded.config_json,
                        source_present=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        gateway.gateway_id,
                        int(gateway.enabled),
                        int(gateway.draining),
                        gateway.mode,
                        gateway.service_node_id,
                        gateway.generation,
                        canonical_json(gateway_value),
                        now,
                    ),
                )
            for node in registry.nodes:
                node_value = node.to_dict()
                conn.execute(
                    """
                    INSERT INTO execution_nodes_desired(
                        node_id, display_name, enabled, draining,
                        registration_generation, config_json, source_present,
                        updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, 1, ?)
                    ON CONFLICT(node_id) DO UPDATE SET
                        display_name=excluded.display_name,
                        enabled=excluded.enabled,
                        draining=excluded.draining,
                        registration_generation=excluded.registration_generation,
                        config_json=excluded.config_json,
                        source_present=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        node.node_id,
                        node.display_name,
                        int(node.enabled),
                        int(node.draining),
                        node.generation,
                        canonical_json(node_value),
                        now,
                    ),
                )
            for environment in registry.environments:
                environment_value = environment.to_dict()
                conn.execute(
                    """
                    INSERT INTO execution_environments(
                        execution_environment_id, node_id, enabled, draining,
                        backend_pool, registration_generation, config_json,
                        source_present, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, 1, ?)
                    ON CONFLICT(execution_environment_id) DO UPDATE SET
                        node_id=excluded.node_id,
                        enabled=excluded.enabled,
                        draining=excluded.draining,
                        backend_pool=excluded.backend_pool,
                        registration_generation=excluded.registration_generation,
                        config_json=excluded.config_json,
                        source_present=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        environment.execution_environment_id,
                        environment.node_id,
                        int(environment.enabled),
                        int(environment.draining),
                        environment.backend_pool,
                        environment.generation,
                        canonical_json(environment_value),
                        now,
                    ),
                )
            conn.execute(
                "UPDATE transport_gateways SET enabled=0, draining=1, updated_at=? "
                "WHERE source_present=0",
                (now,),
            )
            conn.execute(
                "UPDATE execution_nodes_desired SET enabled=0, draining=1, updated_at=? "
                "WHERE source_present=0",
                (now,),
            )
            conn.execute(
                "UPDATE execution_environments SET enabled=0, draining=1, updated_at=? "
                "WHERE source_present=0",
                (now,),
            )
            for endpoint in registry.endpoints:
                endpoint_value = endpoint.to_dict()
                conn.execute(
                    """
                    INSERT INTO backend_endpoints(
                        endpoint_id, node_id, execution_environment_id, gateway_id,
                        transport_mode, enabled, draining, priority, backend_pool,
                        transport, control_channel, result_channel,
                        registration_generation, config_json, source_present,
                        updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    ON CONFLICT(endpoint_id) DO UPDATE SET
                        node_id=excluded.node_id,
                        execution_environment_id=excluded.execution_environment_id,
                        gateway_id=excluded.gateway_id,
                        transport_mode=excluded.transport_mode,
                        enabled=excluded.enabled,
                        draining=excluded.draining,
                        priority=excluded.priority,
                        backend_pool=excluded.backend_pool,
                        transport=excluded.transport,
                        control_channel=excluded.control_channel,
                        result_channel=excluded.result_channel,
                        registration_generation=excluded.registration_generation,
                        config_json=excluded.config_json,
                        source_present=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        endpoint.endpoint_id,
                        endpoint.node_id,
                        endpoint.execution_environment_id,
                        endpoint.gateway_id,
                        endpoint.transport_mode,
                        int(endpoint.enabled),
                        int(endpoint.draining),
                        endpoint.priority,
                        endpoint.backend_pool,
                        endpoint.transport,
                        endpoint.control_channel,
                        endpoint.result_channel,
                        endpoint.generation,
                        canonical_json(endpoint_value),
                        now,
                    ),
                )
            conn.execute(
                "UPDATE backend_endpoints SET enabled=0, draining=1, updated_at=? "
                "WHERE source_present=0",
                (now,),
            )
            conn.execute(
                "UPDATE backend_endpoints SET draining=1, updated_at=? "
                "WHERE endpoint_id IN (SELECT endpoint_id "
                "FROM endpoint_drain_overrides_v4) AND source_present=1",
                (now,),
            )
            self._event(
                conn,
                "registry-reconciled",
                "registry",
                registry.registry_generation or registry.path.name,
                {
                    "operators": len(operators),
                    "agent_pools": len(registry.agent_pools),
                    "active_operators": sum(
                        1 for item in operators if item.desired_state == "enabled"
                    ),
                    "gateways": len(registry.gateways),
                    "nodes": len(registry.nodes),
                    "environments": len(registry.environments),
                    "endpoints": len(registry.endpoints),
                },
            )
        for binding in registry.role_bindings:
            self.upsert_role_binding(binding)
        return {
            "schema_version": SCHEMA_VERSION,
            "database": str(self.path),
            "registry": str(registry.path),
            "operator_count": len(operators),
            "active_operator_count": sum(
                1 for item in operators if item.desired_state == "enabled"
            ),
            "agent_pool_count": len(registry.agent_pools),
            "actor_registration_count": len(registry.actor_registrations),
            "role_binding_count": len(registry.role_bindings),
            "gateway_count": len(registry.gateways),
            "node_count": len(registry.nodes),
            "environment_count": len(registry.environments),
            "endpoint_count": len(registry.endpoints),
        }

    def operator_for_display_name(self, display_name: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM operator_registrations WHERE display_name=? "
                "ORDER BY source_present DESC, updated_at DESC LIMIT 1",
                (display_name,),
            ).fetchone()
        if row is None:
            raise ControlDatabaseError(f"operator is not registered: {display_name}")
        return decode_operator_row(row)

    def operator_registrations(
        self,
        *,
        desired_state: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return reconciled registrations for workflow materialization.

        Runtime callers use this projection instead of reading desired-state
        JSON independently, so profile generation and request creation observe
        the same registration generation.
        """
        self.initialize()
        query = "SELECT * FROM operator_registrations WHERE source_present=1"
        parameters: tuple[Any, ...] = ()
        if desired_state is not None:
            query += " AND desired_state=?"
            parameters = (desired_state,)
        query += " ORDER BY season, display_name, operator_id"
        with self.connection() as conn:
            rows = conn.execute(query, parameters).fetchall()
        return [decode_operator_row(row) for row in rows]
