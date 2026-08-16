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
from ascendop_daemon.storage.migrations import migrate_transport_protocol_v3

class SchemaRegistryRepository:
    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS operator_registrations (
                    operator_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    season TEXT NOT NULL,
                    desired_state TEXT NOT NULL,
                    registration_generation TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    workspace_json TEXT NOT NULL,
                    requirements_json TEXT NOT NULL,
                    cache_policy_json TEXT NOT NULL,
                    routing_policy_json TEXT NOT NULL,
                    test_profile TEXT NOT NULL,
                    source_present INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_display_season
                    ON operator_registrations(display_name, season);

                CREATE TABLE IF NOT EXISTS agent_bindings (
                    operator_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    adapter TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    model TEXT NOT NULL,
                    effort TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (operator_id, role),
                    FOREIGN KEY (operator_id) REFERENCES operator_registrations(operator_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS transport_gateways (
                    gateway_id TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL,
                    draining INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    service_node_id TEXT NOT NULL,
                    registration_generation TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    source_present INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execution_nodes_desired (
                    node_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    draining INTEGER NOT NULL,
                    registration_generation TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    source_present INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execution_environments (
                    execution_environment_id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    draining INTEGER NOT NULL,
                    backend_pool TEXT NOT NULL,
                    registration_generation TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    source_present INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS backend_endpoints (
                    endpoint_id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    execution_environment_id TEXT NOT NULL,
                    gateway_id TEXT NOT NULL DEFAULT '',
                    transport_mode TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL,
                    draining INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    backend_pool TEXT NOT NULL,
                    transport TEXT NOT NULL,
                    control_channel TEXT NOT NULL,
                    result_channel TEXT NOT NULL,
                    registration_generation TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    source_present INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS test_requests (
                    request_id TEXT PRIMARY KEY,
                    request_digest TEXT NOT NULL UNIQUE,
                    operator_id TEXT NOT NULL,
                    test_version TEXT NOT NULL,
                    state TEXT NOT NULL,
                    registration_generation TEXT NOT NULL,
                    requirements_json TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    manifest_path TEXT NOT NULL,
                    blocker TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (operator_id) REFERENCES operator_registrations(operator_id)
                );

                CREATE INDEX IF NOT EXISTS idx_test_requests_state
                    ON test_requests(state, created_at);
                CREATE INDEX IF NOT EXISTS idx_test_requests_operator
                    ON test_requests(operator_id, created_at);

                CREATE TABLE IF NOT EXISTS execution_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    endpoint_id TEXT NOT NULL,
                    endpoint_generation TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    execution_environment_id TEXT NOT NULL,
                    route_json TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (request_id, ordinal),
                    FOREIGN KEY (request_id) REFERENCES test_requests(request_id),
                    FOREIGN KEY (endpoint_id) REFERENCES backend_endpoints(endpoint_id)
                );

                CREATE INDEX IF NOT EXISTS idx_attempts_state
                    ON execution_attempts(state, created_at);

                CREATE TABLE IF NOT EXISTS request_preparations (
                    preparation_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    proposed_attempt_id TEXT NOT NULL,
                    proposed_ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    endpoint_id TEXT NOT NULL,
                    endpoint_generation TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    execution_environment_id TEXT NOT NULL,
                    route_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    envelope_path TEXT NOT NULL DEFAULT '',
                    envelope_digest TEXT NOT NULL DEFAULT '',
                    package_root TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (request_id) REFERENCES test_requests(request_id),
                    FOREIGN KEY (endpoint_id) REFERENCES backend_endpoints(endpoint_id)
                );

                CREATE INDEX IF NOT EXISTS idx_request_preparations_state
                    ON request_preparations(state, created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_request_preparations_active
                    ON request_preparations(request_id) WHERE state='reserved';

                CREATE TABLE IF NOT EXISTS transport_outbox (
                    outbox_id TEXT PRIMARY KEY,
                    attempt_id TEXT NOT NULL UNIQUE,
                    endpoint_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    transport_protocol TEXT NOT NULL DEFAULT '',
                    envelope_path TEXT NOT NULL DEFAULT '',
                    envelope_digest TEXT NOT NULL DEFAULT '',
                    package_root TEXT NOT NULL DEFAULT '',
                    claimed_by TEXT NOT NULL DEFAULT '',
                    claim_token TEXT NOT NULL DEFAULT '',
                    claim_expires_at TEXT NOT NULL DEFAULT '',
                    delivery_attempts INTEGER NOT NULL DEFAULT 0,
                    remote_receipt_json TEXT NOT NULL DEFAULT '{}',
                    next_attempt_at TEXT NOT NULL DEFAULT '',
                    next_poll_at TEXT NOT NULL DEFAULT '',
                    poll_attempts INTEGER NOT NULL DEFAULT 0,
                    query_sequence INTEGER NOT NULL DEFAULT 0,
                    query_inflight_sequence INTEGER NOT NULL DEFAULT 0,
                    last_polled_at TEXT NOT NULL DEFAULT '',
                    accepted_at TEXT NOT NULL DEFAULT '',
                    completed_at TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (attempt_id) REFERENCES execution_attempts(attempt_id),
                    FOREIGN KEY (endpoint_id) REFERENCES backend_endpoints(endpoint_id)
                );

                CREATE INDEX IF NOT EXISTS idx_transport_outbox_state
                    ON transport_outbox(state, created_at);

                CREATE TABLE IF NOT EXISTS transport_returns (
                    return_id TEXT PRIMARY KEY,
                    attempt_id TEXT NOT NULL,
                    outbox_id TEXT NOT NULL,
                    endpoint_id TEXT NOT NULL,
                    endpoint_generation TEXT NOT NULL,
                    receipt_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    acknowledged_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(endpoint_id, receipt_id),
                    FOREIGN KEY (attempt_id) REFERENCES execution_attempts(attempt_id),
                    FOREIGN KEY (outbox_id) REFERENCES transport_outbox(outbox_id)
                );

                CREATE INDEX IF NOT EXISTS idx_transport_returns_state
                    ON transport_returns(state, received_at);

                CREATE TABLE IF NOT EXISTS observed_nodes (
                    node_id TEXT PRIMARY KEY,
                    endpoint_id TEXT NOT NULL,
                    generation TEXT NOT NULL,
                    admission_state TEXT NOT NULL DEFAULT 'discovered',
                    current_session_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    capability_generation TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_observed_nodes_state
                    ON observed_nodes(admission_state, state, lease_expires_at);

                CREATE TABLE IF NOT EXISTS node_sessions (
                    session_id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    boot_id TEXT NOT NULL,
                    generation TEXT NOT NULL,
                    state TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (node_id) REFERENCES observed_nodes(node_id)
                        DEFERRABLE INITIALLY DEFERRED
                );

                CREATE INDEX IF NOT EXISTS idx_node_sessions_node
                    ON node_sessions(node_id, started_at DESC);

                CREATE TABLE IF NOT EXISTS node_capabilities (
                    node_id TEXT NOT NULL,
                    capability_generation TEXT NOT NULL,
                    capability_json TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY (node_id, capability_generation)
                );

                CREATE TABLE IF NOT EXISTS control_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS assistant_action_requests (
                    action_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    rule_id TEXT NOT NULL,
                    assistant_target_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    claimed_by TEXT NOT NULL DEFAULT '',
                    claim_token TEXT NOT NULL DEFAULT '',
                    claim_expires_at TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_assistant_action_claim
                    ON assistant_action_requests(
                        assistant_target_id, state, created_at
                    );

                CREATE TABLE IF NOT EXISTS assistant_action_receipts (
                    action_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    FOREIGN KEY (action_id)
                        REFERENCES assistant_action_requests(action_id)
                );

                CREATE TABLE IF NOT EXISTS service_heartbeats (
                    service_id TEXT PRIMARY KEY,
                    role TEXT NOT NULL,
                    code_generation TEXT NOT NULL,
                    wire_version INTEGER NOT NULL,
                    database_schema INTEGER NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    boot_id TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_service_heartbeats_lease
                    ON service_heartbeats(state, lease_expires_at);
                """
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
            existing = conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            if existing is not None and int(existing[0]) > SCHEMA_VERSION:
                raise ControlDatabaseError(
                    f"unsupported control database schema: {existing[0]}"
                )
            if existing is not None and int(existing[0]) < 7:
                migrate_transport_protocol_v3(conn, self._event)
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
        now = utc_now()
        operators = registry.compile_operators(config)
        with self.transaction() as conn:
            conn.execute("UPDATE operator_registrations SET source_present = 0")
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
                for binding in operator.agent_bindings:
                    conn.execute(
                        """
                        INSERT INTO agent_bindings(
                            operator_id, role, adapter, session_id, model, effort,
                            enabled, updated_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            operator.operator_id,
                            binding["role"],
                            binding["adapter"],
                            binding["session_id"],
                            binding["model"],
                            binding["effort"],
                            int(bool(binding["enabled"])),
                            now,
                        ),
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
            self._event(
                conn,
                "registry-reconciled",
                "registry",
                registry.registry_generation or registry.path.name,
                {
                    "operators": len(operators),
                    "active_operators": sum(
                        1 for item in operators if item.desired_state == "enabled"
                    ),
                    "gateways": len(registry.gateways),
                    "nodes": len(registry.nodes),
                    "environments": len(registry.environments),
                    "endpoints": len(registry.endpoints),
                },
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "database": str(self.path),
            "registry": str(registry.path),
            "operator_count": len(operators),
            "active_operator_count": sum(
                1 for item in operators if item.desired_state == "enabled"
            ),
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
