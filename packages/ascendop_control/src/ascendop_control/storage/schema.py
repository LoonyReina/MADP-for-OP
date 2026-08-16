from __future__ import annotations


CONTROL_EXTENSION_SQL = """
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

CREATE TABLE IF NOT EXISTS agent_registrations_v4 (
    agent_id TEXT PRIMARY KEY,
    driver TEXT NOT NULL,
    executable TEXT NOT NULL,
    executable_digest TEXT NOT NULL,
    observed_version TEXT NOT NULL,
    registration_generation TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    health_state TEXT NOT NULL,
    boot_id TEXT NOT NULL DEFAULT '',
    manager_runner_id TEXT NOT NULL DEFAULT '',
    heartbeat_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    registration_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_registrations_v4_health
    ON agent_registrations_v4(health_state, lease_expires_at, driver);

CREATE TABLE IF NOT EXISTS agent_pools_v4 (
    pool_id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    registration_generation TEXT NOT NULL,
    roles_json TEXT NOT NULL,
    drivers_json TEXT NOT NULL,
    required_capabilities_json TEXT NOT NULL,
    config_json TEXT NOT NULL,
    source_present INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_pool_selections_v4 (
    pool_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    last_selected_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(pool_id, agent_id),
    FOREIGN KEY(pool_id) REFERENCES agent_pools_v4(pool_id) ON DELETE CASCADE,
    FOREIGN KEY(agent_id) REFERENCES agent_registrations_v4(agent_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_agent_pool_selections_v4_round_robin
    ON agent_pool_selections_v4(pool_id, last_selected_at, agent_id);

CREATE TABLE IF NOT EXISTS agent_role_bindings_v4 (
    operator_id TEXT NOT NULL,
    role TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    last_selected_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(operator_id, role, agent_id),
    FOREIGN KEY(agent_id) REFERENCES agent_registrations_v4(agent_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_agent_role_bindings_v4_selection
    ON agent_role_bindings_v4(operator_id, role, enabled, priority, last_selected_at);

CREATE TABLE IF NOT EXISTS agent_iterations_v4 (
    iteration_id TEXT PRIMARY KEY,
    operator_id TEXT NOT NULL,
    role TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    state TEXT NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    action_id TEXT NOT NULL DEFAULT '',
    source_before_digest TEXT NOT NULL DEFAULT '',
    source_after_digest TEXT NOT NULL DEFAULT '',
    iteration_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_iterations_v4_operator
    ON agent_iterations_v4(operator_id, role, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_iterations_v4_candidate
    ON agent_iterations_v4(operator_id, role, candidate_version)
    WHERE state IN ('queued','claimed','running','uncertain','retry-pending');

CREATE TABLE IF NOT EXISTS agent_actions_v4 (
    action_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    iteration_id TEXT NOT NULL UNIQUE,
    operator_id TEXT NOT NULL,
    role TEXT NOT NULL,
    preferred_agent_id TEXT NOT NULL DEFAULT '',
    assigned_agent_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    action_json TEXT NOT NULL,
    claimed_by TEXT NOT NULL DEFAULT '',
    current_attempt_id TEXT NOT NULL DEFAULT '',
    current_lease_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(iteration_id) REFERENCES agent_iterations_v4(iteration_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_actions_v4_claim
    ON agent_actions_v4(state, created_at);

CREATE TABLE IF NOT EXISTS agent_action_quarantine_v4 (
    action_id TEXT PRIMARY KEY,
    iteration_id TEXT NOT NULL,
    previous_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    action_json TEXT NOT NULL,
    quarantined_at TEXT NOT NULL,
    FOREIGN KEY(action_id) REFERENCES agent_actions_v4(action_id),
    FOREIGN KEY(iteration_id) REFERENCES agent_iterations_v4(iteration_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_action_quarantine_v4_time
    ON agent_action_quarantine_v4(quarantined_at, action_id);

CREATE TABLE IF NOT EXISTS agent_action_attempts_v4 (
    attempt_id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    agent_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    state TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL DEFAULT '',
    heartbeat_at TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(action_id, ordinal),
    FOREIGN KEY(action_id) REFERENCES agent_actions_v4(action_id)
);

CREATE TABLE IF NOT EXISTS agent_work_leases_v4 (
    lease_id TEXT PRIMARY KEY,
    lease_token TEXT NOT NULL UNIQUE,
    action_id TEXT NOT NULL UNIQUE,
    iteration_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    role TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    state TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT NOT NULL DEFAULT '',
    lease_json TEXT NOT NULL,
    FOREIGN KEY(action_id) REFERENCES agent_actions_v4(action_id),
    FOREIGN KEY(iteration_id) REFERENCES agent_iterations_v4(iteration_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_work_leases_v4_active_role
    ON agent_work_leases_v4(operator_id, role) WHERE state='active';

CREATE TABLE IF NOT EXISTS agent_context_snapshots_v4 (
    snapshot_id TEXT PRIMARY KEY,
    iteration_id TEXT NOT NULL UNIQUE,
    snapshot_digest TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(iteration_id) REFERENCES agent_iterations_v4(iteration_id)
);

CREATE TABLE IF NOT EXISTS agent_action_receipts_v4 (
    action_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    FOREIGN KEY(action_id) REFERENCES agent_actions_v4(action_id)
);

CREATE TABLE IF NOT EXISTS agent_questions_v4 (
    question_id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL,
    iteration_id TEXT NOT NULL,
    state TEXT NOT NULL,
    question_json TEXT NOT NULL,
    answer_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    answered_at TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(action_id) REFERENCES agent_actions_v4(action_id)
);

CREATE TABLE IF NOT EXISTS agent_artifacts_v4 (
    artifact_id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL,
    iteration_id TEXT NOT NULL,
    logical_name TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    storage_uri TEXT NOT NULL,
    artifact_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(action_id, logical_name, sha256),
    FOREIGN KEY(action_id) REFERENCES agent_actions_v4(action_id)
);

CREATE TABLE IF NOT EXISTS control_commands_v4 (
    command_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    command_kind TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    required_capability TEXT NOT NULL,
    state TEXT NOT NULL,
    command_json TEXT NOT NULL,
    claimed_by TEXT NOT NULL DEFAULT '',
    claim_token TEXT NOT NULL DEFAULT '',
    claim_expires_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_control_commands_v4_claim
    ON control_commands_v4(state, created_at);

CREATE TABLE IF NOT EXISTS endpoint_drain_overrides_v4 (
    endpoint_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_command_receipts_v4 (
    command_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    FOREIGN KEY(command_id) REFERENCES control_commands_v4(command_id)
);

CREATE TABLE IF NOT EXISTS public_resource_projections_v4 (
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    revision TEXT NOT NULL,
    resource_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY(resource_type, resource_id)
);
"""
