from __future__ import annotations

from ascendop_control.storage import CONTROL_EXTENSION_SQL

CONTROL_SCHEMA_SQL = """
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
                    query_inflight_owner_generation TEXT NOT NULL DEFAULT '',
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
                    projection_json TEXT NOT NULL DEFAULT '{}',
                    disposition TEXT NOT NULL DEFAULT 'unprojected',
                    hold_reason TEXT NOT NULL DEFAULT '',
                    terminal_revision INTEGER NOT NULL DEFAULT 0,
                    recovery_id TEXT NOT NULL DEFAULT '',
                    received_at TEXT NOT NULL,
                    acknowledged_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(endpoint_id, receipt_id),
                    FOREIGN KEY (attempt_id) REFERENCES execution_attempts(attempt_id),
                    FOREIGN KEY (outbox_id) REFERENCES transport_outbox(outbox_id)
                );

                CREATE INDEX IF NOT EXISTS idx_transport_returns_state
                    ON transport_returns(state, received_at);

                CREATE TABLE IF NOT EXISTS postprocess_recoveries (
                    recovery_id TEXT PRIMARY KEY,
                    return_id TEXT NOT NULL UNIQUE,
                    outbox_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    endpoint_id TEXT NOT NULL,
                    endpoint_generation TEXT NOT NULL,
                    engine_job_id TEXT NOT NULL,
                    terminal_digest TEXT NOT NULL,
                    terminal_revision INTEGER NOT NULL,
                    stages_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    request_json TEXT NOT NULL DEFAULT '{}',
                    decision_json TEXT NOT NULL DEFAULT '{}',
                    receipt_json TEXT NOT NULL DEFAULT '{}',
                    dispatch_attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (return_id) REFERENCES transport_returns(return_id),
                    FOREIGN KEY (outbox_id) REFERENCES transport_outbox(outbox_id),
                    FOREIGN KEY (attempt_id) REFERENCES execution_attempts(attempt_id)
                );

                CREATE INDEX IF NOT EXISTS idx_postprocess_recoveries_state
                    ON postprocess_recoveries(state, next_attempt_at, created_at);

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

                CREATE TABLE IF NOT EXISTS workflow_actions (
                    action_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    action_kind TEXT NOT NULL,
                    campaign TEXT NOT NULL,
                    operator_id TEXT NOT NULL,
                    test_version TEXT NOT NULL DEFAULT '',
                    board_revision TEXT NOT NULL,
                    producer_generation TEXT NOT NULL,
                    state TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    action_json TEXT NOT NULL,
                    claimed_by TEXT NOT NULL DEFAULT '',
                    claim_token TEXT NOT NULL DEFAULT '',
                    claim_expires_at TEXT NOT NULL DEFAULT '',
                    execution_count INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL DEFAULT '',
                    completed_at TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_actions_claim
                    ON workflow_actions(state, priority DESC, created_at);

                CREATE TABLE IF NOT EXISTS workflow_action_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    action_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    claim_token TEXT NOT NULL UNIQUE,
                    claim_expires_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    worker_pid INTEGER NOT NULL DEFAULT 0,
                    worker_start_token TEXT NOT NULL DEFAULT '',
                    child_pid INTEGER NOT NULL DEFAULT 0,
                    child_start_token TEXT NOT NULL DEFAULT '',
                    boot_id TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL DEFAULT '',
                    completed_at TEXT NOT NULL DEFAULT '',
                    details_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(action_id, ordinal),
                    FOREIGN KEY (action_id) REFERENCES workflow_actions(action_id)
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_action_attempts_active
                    ON workflow_action_attempts(state, claim_expires_at);

                CREATE TABLE IF NOT EXISTS workflow_action_receipts (
                    action_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    FOREIGN KEY (action_id) REFERENCES workflow_actions(action_id)
                );

                CREATE TABLE IF NOT EXISTS scheduler_state (
                    scheduler_id TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
""" + CONTROL_EXTENSION_SQL
