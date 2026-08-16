from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from ascendop_daemon.control_plane.flow_v3_policy import FailureRecord, RetryDecision
from ascendop_protocol.wire_v3 import (
    FLOW_VERSION,
    TERMINAL_STATES,
    ValidatedEnvelope,
    canonical_digest,
    validate_envelope,
    validate_lifecycle_transition,
)


FLOW_DB_SCHEMA = 5
FLOW_DB_GENERATION = "ascendop-control-v3"
ACTIVE_STATES = {
    "created",
    "validated",
    "queued",
    "admitted",
    "dispatched",
    "accepted",
    "running",
    "return-ready",
    "ingested",
    "acknowledged",
    "quarantined",
}
CLAIMABLE_OUTBOX_STATES = {"pending", "retry"}


class FlowV3StoreError(RuntimeError):
    pass


class FlowV3Store:
    """Wire V3 runtime state in the shared control.sqlite3 database."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS flow_v3_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS flow_v3_components (
                    component_id TEXT PRIMARY KEY,
                    role TEXT NOT NULL,
                    code_generation TEXT NOT NULL,
                    wire_min INTEGER NOT NULL,
                    wire_max INTEGER NOT NULL,
                    database_schema INTEGER NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    boot_id TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS flow_v3_requests (
                    request_id TEXT PRIMARY KEY,
                    request_identity_digest TEXT NOT NULL UNIQUE,
                    envelope_json TEXT NOT NULL,
                    envelope_digest TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    test_version TEXT NOT NULL,
                    operation_kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS flow_v3_attempts (
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    envelope_json TEXT NOT NULL,
                    envelope_digest TEXT NOT NULL UNIQUE,
                    endpoint_id TEXT NOT NULL,
                    endpoint_generation TEXT NOT NULL,
                    state TEXT NOT NULL,
                    accepted_at TEXT NOT NULL DEFAULT '',
                    device_session_started_at TEXT NOT NULL DEFAULT '',
                    device_session_finished_at TEXT NOT NULL DEFAULT '',
                    terminal_at TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(request_id, attempt_id),
                    UNIQUE(request_id, ordinal),
                    FOREIGN KEY(request_id) REFERENCES flow_v3_requests(request_id)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_flow_v3_one_active_attempt
                    ON flow_v3_attempts(request_id)
                    WHERE state IN (
                        'created', 'validated', 'queued', 'admitted', 'dispatched',
                        'accepted', 'running', 'return-ready', 'ingested',
                        'acknowledged', 'quarantined'
                    );

                CREATE TABLE IF NOT EXISTS flow_v3_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    previous_state TEXT NOT NULL,
                    current_state TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    UNIQUE(request_id, attempt_id, event_type, current_state, event_at)
                );

                CREATE INDEX IF NOT EXISTS idx_flow_v3_events_attempt
                    ON flow_v3_events(request_id, attempt_id, sequence);

                CREATE TABLE IF NOT EXISTS flow_v3_outbox (
                    outbox_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    envelope_digest TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    delivery_try INTEGER NOT NULL DEFAULT 0,
                    claimed_by TEXT NOT NULL DEFAULT '',
                    claim_token TEXT NOT NULL DEFAULT '',
                    claim_expires_at TEXT NOT NULL DEFAULT '',
                    next_attempt_at TEXT NOT NULL DEFAULT '',
                    receipt_json TEXT NOT NULL DEFAULT '{}',
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(request_id, attempt_id, destination),
                    FOREIGN KEY(request_id, attempt_id)
                        REFERENCES flow_v3_attempts(request_id, attempt_id)
                );

                CREATE INDEX IF NOT EXISTS idx_flow_v3_outbox_claim
                    ON flow_v3_outbox(state, next_attempt_at, created_at);

                CREATE TABLE IF NOT EXISTS flow_v3_resource_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    resource_class TEXT NOT NULL,
                    resource_key TEXT NOT NULL,
                    cpu_weight INTEGER NOT NULL DEFAULT 0,
                    memory_mb INTEGER NOT NULL DEFAULT 0,
                    io_weight INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT NOT NULL,
                    lease_token TEXT NOT NULL,
                    state TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    released_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(resource_key, lease_token)
                );

                CREATE INDEX IF NOT EXISTS idx_flow_v3_resource_active
                    ON flow_v3_resource_reservations(
                        resource_key, resource_class, state, expires_at
                    );

                CREATE TABLE IF NOT EXISTS flow_v3_stage_attempts (
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    stage_name TEXT NOT NULL,
                    stage_try INTEGER NOT NULL,
                    resource_class TEXT NOT NULL,
                    state TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL DEFAULT '',
                    exit_code INTEGER,
                    failure_id TEXT NOT NULL DEFAULT '',
                    evidence_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY(request_id, attempt_id, stage_name, stage_try),
                    FOREIGN KEY(request_id, attempt_id)
                        REFERENCES flow_v3_attempts(request_id, attempt_id)
                );

                CREATE TABLE IF NOT EXISTS flow_v3_failures (
                    failure_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    stage_name TEXT NOT NULL,
                    stage_try INTEGER NOT NULL,
                    domain TEXT NOT NULL,
                    code TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    retryable INTEGER NOT NULL,
                    pre_publish INTEGER NOT NULL,
                    result_visibility TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS flow_v3_retry_decisions (
                    decision_id TEXT PRIMARY KEY,
                    failure_id TEXT NOT NULL UNIQUE,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    consumes_execution_attempt INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    decided_at TEXT NOT NULL,
                    FOREIGN KEY(failure_id) REFERENCES flow_v3_failures(failure_id)
                );

                CREATE TABLE IF NOT EXISTS flow_v3_results (
                    result_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    correctness_state TEXT NOT NULL,
                    performance_state TEXT NOT NULL,
                    infrastructure_state TEXT NOT NULL,
                    terminal_state TEXT NOT NULL,
                    artifacts_json TEXT NOT NULL,
                    result_digest TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    UNIQUE(request_id, attempt_id, result_digest)
                );

                CREATE TABLE IF NOT EXISTS flow_v3_artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    result_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    required INTEGER NOT NULL,
                    local_path TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    UNIQUE(result_id, relative_path),
                    FOREIGN KEY(result_id) REFERENCES flow_v3_results(result_id)
                );

                CREATE TABLE IF NOT EXISTS flow_v3_receipts (
                    receipt_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    receipt_kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    UNIQUE(request_id, attempt_id, receipt_kind, payload_digest)
                );

                CREATE TABLE IF NOT EXISTS flow_v3_transport_controls (
                    control_ref TEXT PRIMARY KEY,
                    outbox_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    state TEXT NOT NULL,
                    published_at TEXT NOT NULL DEFAULT '',
                    terminal_at TEXT NOT NULL DEFAULT '',
                    observation_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS flow_v3_workflow_ingest (
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    try_count INTEGER NOT NULL DEFAULT 0,
                    claimed_by TEXT NOT NULL DEFAULT '',
                    claim_token TEXT NOT NULL DEFAULT '',
                    claim_expires_at TEXT NOT NULL DEFAULT '',
                    next_attempt_at TEXT NOT NULL DEFAULT '',
                    outcome_json TEXT NOT NULL DEFAULT '{}',
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(request_id, attempt_id),
                    FOREIGN KEY(request_id, attempt_id)
                        REFERENCES flow_v3_attempts(request_id, attempt_id)
                );

                CREATE TABLE IF NOT EXISTS flow_v3_spans (
                    span_id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    parent_span_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    stage_try INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    host TEXT NOT NULL,
                    boot_id TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    started_monotonic_ns INTEGER NOT NULL,
                    finished_monotonic_ns INTEGER NOT NULL,
                    duration_ns INTEGER NOT NULL,
                    clock_contract TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS flow_v3_quarantine (
                    quarantine_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    classification TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    source TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS flow_v3_cursors (
                    consumer_id TEXT PRIMARY KEY,
                    event_sequence INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            current = conn.execute(
                "SELECT value FROM flow_v3_metadata WHERE key='schema_version'"
            ).fetchone()
            if current is not None and int(current[0]) > FLOW_DB_SCHEMA:
                raise FlowV3StoreError(
                    f"unsupported Flow V3 database schema: {current[0]}"
                )
            quarantine_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(flow_v3_quarantine)")
            }
            if "classification" not in quarantine_columns:
                conn.execute(
                    "ALTER TABLE flow_v3_quarantine "
                    "ADD COLUMN classification TEXT NOT NULL "
                    "DEFAULT 'runtime-unclassified'"
                )
            conn.execute(
                "INSERT INTO flow_v3_metadata(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(FLOW_DB_SCHEMA),),
            )
            conn.execute(
                "INSERT INTO flow_v3_metadata(key, value) VALUES('schema_generation', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (FLOW_DB_GENERATION,),
            )

    def register_component(
        self,
        *,
        component_id: str,
        role: str,
        code_generation: str,
        wire_min: int,
        wire_max: int,
        capabilities: list[str],
        state: str,
        boot_id: str,
        database_schema: int = FLOW_DB_SCHEMA,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        self.initialize()
        if wire_min > FLOW_VERSION or wire_max < FLOW_VERSION:
            raise FlowV3StoreError(
                f"component {component_id} does not support Wire V3"
            )
        now = datetime.now(timezone.utc)
        heartbeat = iso(now)
        expires = iso(now + timedelta(seconds=max(1, lease_seconds)))
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO flow_v3_components(
                    component_id, role, code_generation, wire_min, wire_max,
                    database_schema, capabilities_json, state, boot_id,
                    heartbeat_at, lease_expires_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(component_id) DO UPDATE SET
                    role=excluded.role,
                    code_generation=excluded.code_generation,
                    wire_min=excluded.wire_min,
                    wire_max=excluded.wire_max,
                    database_schema=excluded.database_schema,
                    capabilities_json=excluded.capabilities_json,
                    state=excluded.state,
                    boot_id=excluded.boot_id,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at
                """,
                (
                    component_id,
                    role,
                    code_generation,
                    wire_min,
                    wire_max,
                    int(database_schema),
                    canonical_json(sorted(set(capabilities))),
                    state,
                    boot_id,
                    heartbeat,
                    expires,
                ),
            )
        return self.component(component_id)

    def component(self, component_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM flow_v3_components WHERE component_id=?",
                (component_id,),
            ).fetchone()
            if row is None:
                raise FlowV3StoreError(f"unknown Flow V3 component: {component_id}")
            return decode_component(row)

    def attempt(self, request_id: str, attempt_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if row is None:
                raise FlowV3StoreError(
                    f"unknown attempt: {request_id}/{attempt_id}"
                )
            return decode_attempt(row)

    def attempts_in_states(
        self,
        states: set[str],
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.initialize()
        if not states:
            return []
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM flow_v3_attempts WHERE state IN ("
                + ",".join("?" for _ in states)
                + ") ORDER BY updated_at, request_id, attempt_id LIMIT ?",
                (*sorted(states), max(1, int(limit))),
            ).fetchall()
            return [decode_attempt(row) for row in rows]

    def workflow_ingest_recovery_candidates(
        self,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Return terminal attempts whose explicitly reopened adapter retry is due."""

        self.initialize()
        now = utc_now()
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT attempt.*
                FROM flow_v3_attempts AS attempt
                JOIN flow_v3_workflow_ingest AS ingest
                  ON ingest.request_id=attempt.request_id
                 AND ingest.attempt_id=attempt.attempt_id
                WHERE attempt.state IN (
                    'terminal-success',
                    'terminal-business-failure',
                    'terminal-infrastructure-failure',
                    'terminal-cancelled'
                )
                  AND (
                    (ingest.state='retry'
                     AND (ingest.next_attempt_at='' OR ingest.next_attempt_at<=?))
                    OR
                    (ingest.state='claimed'
                     AND ingest.claim_expires_at!=''
                     AND ingest.claim_expires_at<=?)
                  )
                ORDER BY ingest.updated_at, attempt.request_id, attempt.attempt_id
                LIMIT ?
                """,
                (now, now, max(1, int(limit))),
            ).fetchall()
        return [decode_attempt(row) for row in rows]

    def recover_workflow_ingest(
        self,
        request_id: str,
        attempt_id: str,
        *,
        actor: str,
    ) -> dict[str, Any]:
        """Reopen only terminal, durable, failed workflow postprocessing."""

        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            attempt = conn.execute(
                "SELECT state FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if attempt is None:
                raise FlowV3StoreError(
                    f"unknown attempt: {request_id}/{attempt_id}"
                )
            attempt_state = str(attempt["state"])
            if attempt_state not in TERMINAL_STATES:
                raise FlowV3StoreError(
                    "workflow ingest recovery requires a terminal attempt"
                )
            durable = conn.execute(
                "SELECT 1 FROM flow_v3_results "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if durable is None:
                raise FlowV3StoreError(
                    "workflow ingest recovery requires a durable result"
                )
            row = conn.execute(
                "SELECT * FROM flow_v3_workflow_ingest "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if row is None:
                raise FlowV3StoreError(
                    "workflow ingest recovery requires a prior adapter attempt"
                )
            state = str(row["state"])
            if state == "succeeded":
                return decode_workflow_ingest(row)
            if state in {"retry", "claimed"}:
                return decode_workflow_ingest(row)
            if state != "failed":
                raise FlowV3StoreError(
                    f"workflow ingest recovery cannot reopen state: {state}"
                )
            previous_error = str(row["last_error"])
            conn.execute(
                """
                UPDATE flow_v3_workflow_ingest SET
                    state='retry', claimed_by='', claim_token='',
                    claim_expires_at='', next_attempt_at='', updated_at=?
                WHERE request_id=? AND attempt_id=? AND state='failed'
                """,
                (now, request_id, attempt_id),
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="workflow-result-ingest-recovery-requested",
                previous_state=attempt_state,
                current_state=attempt_state,
                actor=actor,
                payload={"previous_error": previous_error},
                event_at=now,
            )
            reopened = conn.execute(
                "SELECT * FROM flow_v3_workflow_ingest "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert reopened is not None
            return decode_workflow_ingest(reopened)

    def recover_generation_mismatch_terminals(
        self,
        *,
        actor: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Reopen only evidence-backed attempts stopped by a code generation barrier."""

        self.initialize()
        now = utc_now()
        recovered: list[dict[str, Any]] = []
        with self.transaction() as conn:
            rows = conn.execute(
                """
                SELECT attempt.*, failure.phase,
                       EXISTS(
                           SELECT 1 FROM flow_v3_results AS result
                           WHERE result.request_id=attempt.request_id
                             AND result.attempt_id=attempt.attempt_id
                       ) AS has_result,
                       EXISTS(
                           SELECT 1 FROM flow_v3_receipts AS receipt
                           WHERE receipt.request_id=attempt.request_id
                             AND receipt.attempt_id=attempt.attempt_id
                             AND receipt.receipt_kind='endpoint-accept'
                       ) AS has_accept_receipt
                FROM flow_v3_attempts AS attempt
                JOIN flow_v3_requests AS request
                  ON request.request_id=attempt.request_id
                JOIN flow_v3_failures AS failure
                  ON failure.failure_id=(
                      SELECT latest.failure_id
                      FROM flow_v3_failures AS latest
                      WHERE latest.request_id=attempt.request_id
                        AND latest.attempt_id=attempt.attempt_id
                      ORDER BY latest.recorded_at DESC, latest.failure_id DESC
                      LIMIT 1
                  )
                WHERE attempt.state='terminal-infrastructure-failure'
                  AND request.state='terminal-infrastructure-failure'
                  AND failure.code='transport-generation-mismatch'
                  AND failure.phase IN ('query', 'ack')
                  AND attempt.ordinal=(
                      SELECT MAX(newest.ordinal)
                      FROM flow_v3_attempts AS newest
                      WHERE newest.request_id=attempt.request_id
                  )
                  AND NOT EXISTS(
                      SELECT 1 FROM flow_v3_receipts AS receipt
                      WHERE receipt.request_id=attempt.request_id
                        AND receipt.attempt_id=attempt.attempt_id
                        AND receipt.receipt_kind='return-ack'
                  )
                ORDER BY attempt.updated_at, attempt.request_id, attempt.attempt_id
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            for row in rows:
                has_result = bool(row["has_result"])
                has_accept_receipt = bool(row["has_accept_receipt"])
                phase = str(row["phase"])
                if has_result:
                    target = "ingested"
                elif phase == "query" and has_accept_receipt:
                    target = "accepted"
                else:
                    continue
                request_id = str(row["request_id"])
                attempt_id = str(row["attempt_id"])
                conn.execute(
                    "UPDATE flow_v3_attempts SET state=?, terminal_at='', updated_at=? "
                    "WHERE request_id=? AND attempt_id=? "
                    "AND state='terminal-infrastructure-failure'",
                    (target, now, request_id, attempt_id),
                )
                conn.execute(
                    "UPDATE flow_v3_requests SET state=?, updated_at=? "
                    "WHERE request_id=? AND state='terminal-infrastructure-failure'",
                    (target, now, request_id),
                )
                self._event(
                    conn,
                    request_id=request_id,
                    attempt_id=attempt_id,
                    event_type="generation-mismatch-reconcile-reopened",
                    previous_state="terminal-infrastructure-failure",
                    current_state=target,
                    actor=actor,
                    payload={
                        "failure_phase": phase,
                        "has_accept_receipt": has_accept_receipt,
                        "has_durable_result": has_result,
                    },
                    event_at=now,
                )
                recovered.append(
                    {
                        "request_id": request_id,
                        "attempt_id": attempt_id,
                        "state": target,
                    }
                )
        return recovered

    def workflow_attempts(
        self,
        *,
        operation_kind: str,
    ) -> list[dict[str, Any]]:
        """Return every durable attempt for one workflow operation kind."""

        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT attempt.* FROM flow_v3_attempts AS attempt "
                "JOIN flow_v3_requests AS request "
                "ON request.request_id=attempt.request_id "
                "WHERE request.operation_kind=? "
                "ORDER BY attempt.created_at, attempt.request_id, attempt.attempt_id",
                (str(operation_kind),),
            ).fetchall()
        return [decode_attempt(row) for row in rows]

    def readiness(
        self,
        required_components: Mapping[str, Any],
        *,
        code_generation: str,
    ) -> dict[str, Any]:
        self.initialize()
        now = datetime.now(timezone.utc)
        with self.connection() as conn:
            rows = {
                str(row["component_id"]): row
                for row in conn.execute("SELECT * FROM flow_v3_components")
            }
        blockers: list[str] = []
        components: list[dict[str, Any]] = []
        for component_id, requirement in required_components.items():
            if isinstance(requirement, Mapping):
                role = str(requirement.get("role") or "")
                expected_schema = int(
                    requirement.get("database_schema", FLOW_DB_SCHEMA)
                )
                required_capabilities = {
                    str(value)
                    for value in requirement.get("capabilities", [])
                    if str(value)
                }
                expected_generation = str(
                    requirement.get("code_generation") or code_generation
                )
            else:
                role = str(requirement)
                expected_schema = FLOW_DB_SCHEMA
                required_capabilities = set()
                expected_generation = code_generation
            row = rows.get(component_id)
            if row is None:
                blockers.append(f"missing component: {component_id}")
                continue
            decoded = decode_component(row)
            components.append(decoded)
            if decoded["role"] != role:
                blockers.append(f"component role mismatch: {component_id}")
            if decoded["code_generation"] != expected_generation:
                blockers.append(f"component generation mismatch: {component_id}")
            if decoded["wire_min"] > FLOW_VERSION or decoded["wire_max"] < FLOW_VERSION:
                blockers.append(f"component Wire V3 mismatch: {component_id}")
            if decoded["database_schema"] != expected_schema:
                blockers.append(f"component DB schema mismatch: {component_id}")
            if not required_capabilities.issubset(
                set(decoded["capabilities"])
            ):
                blockers.append(
                    f"component capabilities mismatch: {component_id}"
                )
            if decoded["state"] != "ready":
                blockers.append(f"component not ready: {component_id}")
            if parse_time(decoded["lease_expires_at"]) <= now:
                blockers.append(f"component lease expired: {component_id}")
        return {
            "schema": "ascendop.flow.readiness.v3",
            "ready": not blockers,
            "code_generation": code_generation,
            "blockers": blockers,
            "components": components,
        }

    def create_request(
        self,
        envelope: Mapping[str, Any],
        *,
        actor: str,
        destination: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        validated = validate_envelope(envelope)
        request_id = str(validated.envelope["meta"]["request_id"])
        attempt_id = str(validated.envelope["meta"]["attempt_id"])
        ordinal = attempt_ordinal(attempt_id)
        identity_digest = request_identity_digest(validated.envelope)
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM flow_v3_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["request_identity_digest"]) != identity_digest:
                    raise FlowV3StoreError(
                        f"immutable request identity collision: {request_id}"
                    )
                attempt = conn.execute(
                    "SELECT * FROM flow_v3_attempts "
                    "WHERE request_id=? AND attempt_id=?",
                    (request_id, attempt_id),
                ).fetchone()
                if attempt is None:
                    raise FlowV3StoreError(
                        f"request exists with another active attempt: {request_id}"
                    )
                if str(attempt["envelope_digest"]) != validated.digest:
                    raise FlowV3StoreError(
                        f"immutable attempt envelope collision: {request_id}/{attempt_id}"
                    )
                return decode_request_attempt(existing, attempt)
            workflow = validated.envelope["workflow"]
            envelope_json = canonical_json(validated.envelope)
            conn.execute(
                """
                INSERT INTO flow_v3_requests(
                    request_id, request_identity_digest, envelope_json,
                    envelope_digest, payload_digest, operator, test_version,
                    operation_kind, state, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'created', ?, ?)
                """,
                (
                    request_id,
                    identity_digest,
                    envelope_json,
                    validated.digest,
                    str(validated.envelope["payload"]["digest"]),
                    str(workflow["operator"]),
                    str(workflow["test_version"]),
                    str(workflow["operation_kind"]),
                    now,
                    now,
                ),
            )
            identity = validated.envelope["identity"]
            conn.execute(
                """
                INSERT INTO flow_v3_attempts(
                    request_id, attempt_id, ordinal, envelope_json,
                    envelope_digest, endpoint_id, endpoint_generation,
                    state, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'created', ?, ?)
                """,
                (
                    request_id,
                    attempt_id,
                    ordinal,
                    envelope_json,
                    validated.digest,
                    str(identity["endpoint_id"]),
                    str(identity["endpoint_generation"]),
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="request-created",
                previous_state="",
                current_state="created",
                actor=actor,
                payload={"envelope_digest": validated.digest},
                event_at=now,
            )
            if destination:
                self._enqueue_outbox(
                    conn,
                    request_id=request_id,
                    attempt_id=attempt_id,
                    destination=destination,
                    envelope_digest=validated.digest,
                    payload=validated.envelope,
                    created_at=now,
                )
            request = conn.execute(
                "SELECT * FROM flow_v3_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            attempt = conn.execute(
                "SELECT * FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert request is not None and attempt is not None
            return decode_request_attempt(request, attempt)

    def create_execution_attempt(
        self,
        envelope: Mapping[str, Any],
        *,
        actor: str,
        destination: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        validated = validate_envelope(envelope)
        request_id = str(validated.envelope["meta"]["request_id"])
        attempt_id = str(validated.envelope["meta"]["attempt_id"])
        ordinal = attempt_ordinal(attempt_id)
        identity_digest = request_identity_digest(validated.envelope)
        now = utc_now()
        with self.transaction() as conn:
            request = conn.execute(
                "SELECT * FROM flow_v3_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if request is None:
                raise FlowV3StoreError(
                    f"execution retry has no original request: {request_id}"
                )
            if str(request["request_identity_digest"]) != identity_digest:
                raise FlowV3StoreError(
                    f"immutable request identity collision: {request_id}"
                )
            existing = conn.execute(
                "SELECT * FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if existing is not None:
                if str(existing["envelope_digest"]) != validated.digest:
                    raise FlowV3StoreError(
                        f"immutable attempt envelope collision: {request_id}/{attempt_id}"
                    )
                return decode_request_attempt(request, existing)
            active = conn.execute(
                "SELECT attempt_id, state FROM flow_v3_attempts "
                "WHERE request_id=? AND state IN ("
                + ",".join("?" for _ in ACTIVE_STATES)
                + ")",
                (request_id, *sorted(ACTIVE_STATES)),
            ).fetchone()
            if active is not None:
                raise FlowV3StoreError(
                    "request already has an unresolved attempt: "
                    f"{active['attempt_id']} ({active['state']})"
                )
            previous = conn.execute(
                "SELECT ordinal, state FROM flow_v3_attempts "
                "WHERE request_id=? ORDER BY ordinal DESC LIMIT 1",
                (request_id,),
            ).fetchone()
            expected_ordinal = int(previous["ordinal"]) + 1 if previous else 1
            if ordinal != expected_ordinal:
                raise FlowV3StoreError(
                    f"execution attempt ordinal must be {expected_ordinal}, got {ordinal}"
                )
            if previous is not None and str(previous["state"]) not in TERMINAL_STATES:
                raise FlowV3StoreError(
                    f"previous execution attempt is not terminal: {previous['state']}"
                )
            envelope_json = canonical_json(validated.envelope)
            identity = validated.envelope["identity"]
            conn.execute(
                """
                INSERT INTO flow_v3_attempts(
                    request_id, attempt_id, ordinal, envelope_json,
                    envelope_digest, endpoint_id, endpoint_generation,
                    state, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'created', ?, ?)
                """,
                (
                    request_id,
                    attempt_id,
                    ordinal,
                    envelope_json,
                    validated.digest,
                    str(identity["endpoint_id"]),
                    str(identity["endpoint_generation"]),
                    now,
                    now,
                ),
            )
            conn.execute(
                "UPDATE flow_v3_requests SET state='created', updated_at=? "
                "WHERE request_id=?",
                (now, request_id),
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="execution-attempt-created",
                previous_state=str(previous["state"]) if previous else "",
                current_state="created",
                actor=actor,
                payload={"envelope_digest": validated.digest, "ordinal": ordinal},
                event_at=now,
            )
            if destination:
                self._enqueue_outbox(
                    conn,
                    request_id=request_id,
                    attempt_id=attempt_id,
                    destination=destination,
                    envelope_digest=validated.digest,
                    payload=validated.envelope,
                    created_at=now,
                )
            attempt = conn.execute(
                "SELECT * FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            request = conn.execute(
                "SELECT * FROM flow_v3_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            assert request is not None and attempt is not None
            return decode_request_attempt(request, attempt)

    def transition(
        self,
        request_id: str,
        attempt_id: str,
        *,
        expected_state: str,
        current_state: str,
        event_type: str,
        actor: str,
        payload: Mapping[str, Any] | None = None,
        outbox_destination: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        validate_lifecycle_transition(expected_state, current_state)
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if row is None:
                raise FlowV3StoreError(f"unknown attempt: {request_id}/{attempt_id}")
            observed = str(row["state"])
            if observed != expected_state:
                raise FlowV3StoreError(
                    f"attempt state changed: expected {expected_state}, observed {observed}"
                )
            updates: dict[str, str] = {}
            if current_state == "accepted":
                updates["accepted_at"] = now
            if current_state in TERMINAL_STATES:
                updates["terminal_at"] = now
            assignments = ["state=?", "updated_at=?"]
            values: list[Any] = [current_state, now]
            for key, value in updates.items():
                assignments.append(f"{key}=?")
                values.append(value)
            values.extend([request_id, attempt_id, expected_state])
            cursor = conn.execute(
                f"UPDATE flow_v3_attempts SET {', '.join(assignments)} "
                "WHERE request_id=? AND attempt_id=? AND state=?",
                values,
            )
            if cursor.rowcount != 1:
                raise FlowV3StoreError("attempt transition lost its compare-and-swap")
            conn.execute(
                "UPDATE flow_v3_requests SET state=?, updated_at=? WHERE request_id=?",
                (current_state, now, request_id),
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type=event_type,
                previous_state=expected_state,
                current_state=current_state,
                actor=actor,
                payload=dict(payload or {}),
                event_at=now,
            )
            if outbox_destination:
                attempt = conn.execute(
                    "SELECT envelope_json, envelope_digest FROM flow_v3_attempts "
                    "WHERE request_id=? AND attempt_id=?",
                    (request_id, attempt_id),
                ).fetchone()
                assert attempt is not None
                self._enqueue_outbox(
                    conn,
                    request_id=request_id,
                    attempt_id=attempt_id,
                    destination=outbox_destination,
                    envelope_digest=str(attempt["envelope_digest"]),
                    payload=json.loads(str(attempt["envelope_json"])),
                    created_at=now,
                )
            result = conn.execute(
                "SELECT * FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert result is not None
            return decode_attempt(result)

    def start_stage(
        self,
        request_id: str,
        attempt_id: str,
        *,
        stage_name: str,
        stage_try: int,
        resource_class: str,
    ) -> dict[str, Any]:
        self.initialize()
        if stage_try < 1:
            raise FlowV3StoreError("stage_try must be positive")
        now = utc_now()
        with self.transaction() as conn:
            attempt = conn.execute(
                "SELECT state FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if attempt is None:
                raise FlowV3StoreError(f"unknown attempt: {request_id}/{attempt_id}")
            if str(attempt["state"]) != "running":
                raise FlowV3StoreError(
                    f"stage cannot start while attempt is {attempt['state']}"
                )
            conn.execute(
                """
                INSERT INTO flow_v3_stage_attempts(
                    request_id, attempt_id, stage_name, stage_try,
                    resource_class, state, started_at
                ) VALUES(?, ?, ?, ?, ?, 'running', ?)
                """,
                (
                    request_id,
                    attempt_id,
                    stage_name,
                    stage_try,
                    resource_class,
                    now,
                ),
            )
            return {
                "request_id": request_id,
                "attempt_id": attempt_id,
                "stage_name": stage_name,
                "stage_try": stage_try,
                "resource_class": resource_class,
                "state": "running",
                "started_at": now,
            }

    def finish_stage(
        self,
        request_id: str,
        attempt_id: str,
        *,
        stage_name: str,
        stage_try: int,
        state: str,
        exit_code: int | None,
        failure_id: str = "",
        evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        if state not in {"succeeded", "business-failed", "infrastructure-failed", "cancelled"}:
            raise FlowV3StoreError(f"unsupported stage terminal state: {state}")
        now = utc_now()
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE flow_v3_stage_attempts
                SET state=?, finished_at=?, exit_code=?, failure_id=?,
                    evidence_json=?
                WHERE request_id=? AND attempt_id=? AND stage_name=?
                    AND stage_try=? AND state='running'
                """,
                (
                    state,
                    now,
                    exit_code,
                    failure_id,
                    canonical_json(dict(evidence or {})),
                    request_id,
                    attempt_id,
                    stage_name,
                    stage_try,
                ),
            )
            if cursor.rowcount != 1:
                raise FlowV3StoreError(
                    f"stage attempt is not running: {stage_name}/{stage_try}"
                )
            row = conn.execute(
                "SELECT * FROM flow_v3_stage_attempts "
                "WHERE request_id=? AND attempt_id=? AND stage_name=? AND stage_try=?",
                (request_id, attempt_id, stage_name, stage_try),
            ).fetchone()
            assert row is not None
            return dict(row)

    def acquire_reservation(
        self,
        request_id: str,
        attempt_id: str,
        *,
        reservation_id: str,
        resource_class: str,
        resource_key: str,
        lease_owner: str,
        lease_token: str,
        lease_seconds: int,
        cpu_weight: int = 0,
        memory_mb: int = 0,
        io_weight: int = 0,
        capacity_cpu_weight: int = 0,
        capacity_memory_mb: int = 0,
        capacity_io_weight: int = 0,
        capacity_count: int = 0,
        exclusive: bool = False,
    ) -> dict[str, Any]:
        self.initialize()
        if lease_seconds < 1:
            raise FlowV3StoreError("resource lease_seconds must be positive")
        if min(cpu_weight, memory_mb, io_weight) < 0:
            raise FlowV3StoreError("resource weights cannot be negative")
        now_dt = datetime.now(timezone.utc)
        now = iso(now_dt)
        expires = iso(now_dt + timedelta(seconds=lease_seconds))
        with self.transaction() as conn:
            conn.execute(
                "UPDATE flow_v3_resource_reservations "
                "SET state='expired', released_at=? "
                "WHERE state='active' AND expires_at<=?",
                (now, now),
            )
            existing = conn.execute(
                "SELECT * FROM flow_v3_resource_reservations "
                "WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["request_id"]) != request_id
                    or str(existing["attempt_id"]) != attempt_id
                    or str(existing["lease_token"]) != lease_token
                ):
                    raise FlowV3StoreError(
                        f"resource reservation identity collision: {reservation_id}"
                    )
                return dict(existing)
            active = conn.execute(
                "SELECT COUNT(*) AS count, "
                "COALESCE(SUM(cpu_weight), 0) AS cpu, "
                "COALESCE(SUM(memory_mb), 0) AS memory, "
                "COALESCE(SUM(io_weight), 0) AS io "
                "FROM flow_v3_resource_reservations "
                "WHERE resource_key=? AND state='active'",
                (resource_key,),
            ).fetchone()
            assert active is not None
            if exclusive and int(active["count"]) > 0:
                raise FlowV3StoreError(f"resource is already leased: {resource_key}")
            if capacity_count and int(active["count"]) + 1 > capacity_count:
                raise FlowV3StoreError(f"resource count is exhausted: {resource_key}")
            capacity_checks = (
                ("cpu", cpu_weight, capacity_cpu_weight),
                ("memory", memory_mb, capacity_memory_mb),
                ("io", io_weight, capacity_io_weight),
            )
            for column, requested, capacity in capacity_checks:
                if capacity and int(active[column]) + requested > capacity:
                    raise FlowV3StoreError(
                        f"resource {column} budget is exhausted: {resource_key}"
                    )
            conn.execute(
                """
                INSERT INTO flow_v3_resource_reservations(
                    reservation_id, request_id, attempt_id, resource_class,
                    resource_key, cpu_weight, memory_mb, io_weight,
                    lease_owner, lease_token, state, acquired_at, expires_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    reservation_id,
                    request_id,
                    attempt_id,
                    resource_class,
                    resource_key,
                    cpu_weight,
                    memory_mb,
                    io_weight,
                    lease_owner,
                    lease_token,
                    now,
                    expires,
                ),
            )
            if resource_class == "device":
                conn.execute(
                    "UPDATE flow_v3_attempts "
                    "SET device_session_started_at=CASE "
                    "WHEN device_session_started_at='' THEN ? "
                    "ELSE device_session_started_at END, updated_at=? "
                    "WHERE request_id=? AND attempt_id=?",
                    (now, now, request_id, attempt_id),
                )
            row = conn.execute(
                "SELECT * FROM flow_v3_resource_reservations "
                "WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            assert row is not None
            return dict(row)

    def release_reservation(
        self,
        reservation_id: str,
        *,
        lease_token: str,
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM flow_v3_resource_reservations "
                "WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise FlowV3StoreError(f"unknown resource reservation: {reservation_id}")
            if str(row["lease_token"]) != lease_token:
                raise FlowV3StoreError("resource lease token mismatch")
            if str(row["state"]) == "released":
                return dict(row)
            if str(row["state"]) != "active":
                raise FlowV3StoreError(
                    f"resource reservation is {row['state']}: {reservation_id}"
                )
            conn.execute(
                "UPDATE flow_v3_resource_reservations "
                "SET state='released', released_at=? WHERE reservation_id=?",
                (now, reservation_id),
            )
            if str(row["resource_class"]) == "device":
                remaining = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM flow_v3_resource_reservations "
                        "WHERE request_id=? AND attempt_id=? "
                        "AND resource_class='device' AND state='active'",
                        (row["request_id"], row["attempt_id"]),
                    ).fetchone()[0]
                )
                if remaining == 0:
                    conn.execute(
                        "UPDATE flow_v3_attempts "
                        "SET device_session_finished_at=?, updated_at=? "
                        "WHERE request_id=? AND attempt_id=?",
                        (
                            now,
                            now,
                            row["request_id"],
                            row["attempt_id"],
                        ),
                    )
            result = conn.execute(
                "SELECT * FROM flow_v3_resource_reservations "
                "WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            assert result is not None
            return dict(result)

    def advance_cursor(self, consumer_id: str, event_sequence: int) -> dict[str, Any]:
        self.initialize()
        if event_sequence < 0:
            raise FlowV3StoreError("event cursor cannot be negative")
        now = utc_now()
        with self.transaction() as conn:
            current = conn.execute(
                "SELECT event_sequence FROM flow_v3_cursors WHERE consumer_id=?",
                (consumer_id,),
            ).fetchone()
            if current is not None and event_sequence < int(current[0]):
                raise FlowV3StoreError("event cursor cannot move backwards")
            conn.execute(
                "INSERT INTO flow_v3_cursors(consumer_id, event_sequence, updated_at) "
                "VALUES(?, ?, ?) ON CONFLICT(consumer_id) DO UPDATE SET "
                "event_sequence=excluded.event_sequence, updated_at=excluded.updated_at",
                (consumer_id, event_sequence, now),
            )
            return {
                "consumer_id": consumer_id,
                "event_sequence": event_sequence,
                "updated_at": now,
            }

    def claim_outbox(
        self,
        consumer_id: str,
        *,
        limit: int = 1,
        lease_seconds: int = 60,
        attempt_states: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        self.initialize()
        now_dt = datetime.now(timezone.utc)
        now = iso(now_dt)
        expires = iso(now_dt + timedelta(seconds=max(1, lease_seconds)))
        claimed: list[dict[str, Any]] = []
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE flow_v3_outbox SET
                    state='retry', claimed_by='', claim_token='',
                    claim_expires_at='', updated_at=?
                WHERE state='claimed' AND claim_expires_at!='' AND claim_expires_at<=?
                """,
                (now, now),
            )
            state_filter = ""
            parameters: list[Any] = [now]
            if attempt_states:
                state_filter = (
                    " AND a.state IN ("
                    + ",".join("?" for _ in attempt_states)
                    + ")"
                )
                parameters.extend(sorted(attempt_states))
            parameters.append(max(1, int(limit)))
            rows = conn.execute(
                "SELECT o.* FROM flow_v3_outbox o "
                "JOIN flow_v3_attempts a "
                "ON a.request_id=o.request_id AND a.attempt_id=o.attempt_id "
                "WHERE o.state IN ('pending', 'retry') "
                "AND (o.next_attempt_at='' OR o.next_attempt_at<=?)"
                + state_filter
                + " ORDER BY o.created_at, o.outbox_id LIMIT ?",
                tuple(parameters),
            ).fetchall()
            for row in rows:
                token = uuid.uuid4().hex
                cursor = conn.execute(
                    """
                    UPDATE flow_v3_outbox SET
                        state='claimed', claimed_by=?, claim_token=?,
                        claim_expires_at=?, delivery_try=delivery_try+1,
                        updated_at=?
                    WHERE outbox_id=? AND state IN ('pending', 'retry')
                    """,
                    (
                        consumer_id,
                        token,
                        expires,
                        now,
                        str(row["outbox_id"]),
                    ),
                )
                if cursor.rowcount != 1:
                    continue
                selected = conn.execute(
                    "SELECT * FROM flow_v3_outbox WHERE outbox_id=?",
                    (str(row["outbox_id"]),),
                ).fetchone()
                assert selected is not None
                claimed.append(decode_outbox(selected))
        return claimed

    def complete_outbox(
        self,
        outbox_id: str,
        *,
        consumer_id: str,
        claim_token: str,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE flow_v3_outbox SET
                    state='delivered', receipt_json=?, claimed_by='',
                    claim_token='', claim_expires_at='', updated_at=?
                WHERE outbox_id=? AND state='claimed'
                  AND claimed_by=? AND claim_token=?
                """,
                (
                    canonical_json(dict(receipt)),
                    now,
                    outbox_id,
                    consumer_id,
                    claim_token,
                ),
            )
            if cursor.rowcount != 1:
                raise FlowV3StoreError("outbox claim is stale or owned by another consumer")
            row = conn.execute(
                "SELECT * FROM flow_v3_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            assert row is not None
            return decode_outbox(row)

    def record_transport_control(
        self,
        *,
        control_ref: str,
        outbox_id: str,
        request_id: str,
        attempt_id: str,
        action: str,
        state: str,
        observation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        if action not in {"accept", "query", "ack", "status"}:
            raise FlowV3StoreError(f"unsupported transport control action: {action}")
        if state not in {"created", "published", "terminal", "failed"}:
            raise FlowV3StoreError(f"unsupported transport control state: {state}")
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM flow_v3_transport_controls WHERE control_ref=?",
                (control_ref,),
            ).fetchone()
            if existing is not None and (
                str(existing["outbox_id"]) != outbox_id
                or str(existing["request_id"]) != request_id
                or str(existing["attempt_id"]) != attempt_id
                or str(existing["action"]) != action
            ):
                raise FlowV3StoreError(
                    f"transport control identity collision: {control_ref}"
                )
            published_at = now if state == "published" else ""
            terminal_at = now if state in {"terminal", "failed"} else ""
            conn.execute(
                """
                INSERT INTO flow_v3_transport_controls(
                    control_ref, outbox_id, request_id, attempt_id, action,
                    state, published_at, terminal_at, observation_json,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(control_ref) DO UPDATE SET
                    state=excluded.state,
                    published_at=CASE
                        WHEN flow_v3_transport_controls.published_at=''
                        THEN excluded.published_at
                        ELSE flow_v3_transport_controls.published_at
                    END,
                    terminal_at=excluded.terminal_at,
                    observation_json=excluded.observation_json,
                    updated_at=excluded.updated_at
                """,
                (
                    control_ref,
                    outbox_id,
                    request_id,
                    attempt_id,
                    action,
                    state,
                    published_at,
                    terminal_at,
                    canonical_json(dict(observation or {})),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM flow_v3_transport_controls WHERE control_ref=?",
                (control_ref,),
            ).fetchone()
            assert row is not None
            value = dict(row)
            value["observation"] = json.loads(
                value.pop("observation_json")
            )
            return value

    def open_transport_control(
        self,
        *,
        outbox_id: str,
        request_id: str,
        attempt_id: str,
        action: str,
    ) -> dict[str, Any]:
        """Return the unresolved control or atomically allocate its successor."""
        self.initialize()
        if action not in {"accept", "query", "ack", "status"}:
            raise FlowV3StoreError(f"unsupported transport control action: {action}")
        now = utc_now()
        with self.transaction() as conn:
            pending = conn.execute(
                """
                SELECT * FROM flow_v3_transport_controls
                WHERE request_id=? AND attempt_id=? AND action=?
                  AND state IN ('created', 'published')
                ORDER BY created_at DESC, control_ref DESC
                LIMIT 1
                """,
                (request_id, attempt_id, action),
            ).fetchone()
            if pending is not None:
                ordinal_row = conn.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM flow_v3_transport_controls
                    WHERE request_id=? AND attempt_id=? AND action=?
                    """,
                    (request_id, attempt_id, action),
                ).fetchone()
                value = dict(pending)
                value["observation"] = json.loads(
                    value.pop("observation_json")
                )
                value["action_ordinal"] = int(ordinal_row["total"])
                return value
            ordinal_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM flow_v3_transport_controls
                WHERE request_id=? AND attempt_id=? AND action=?
                """,
                (request_id, attempt_id, action),
            ).fetchone()
            ordinal = int(ordinal_row["total"]) + 1
            material = (
                f"{request_id}:{attempt_id}:{action}:{ordinal}"
            ).encode("utf-8")
            control_ref = (
                f"flowv3-{action}-"
                f"{hashlib.sha256(material).hexdigest()[:24]}"
            )
            conn.execute(
                """
                INSERT INTO flow_v3_transport_controls(
                    control_ref, outbox_id, request_id, attempt_id, action,
                    state, published_at, terminal_at, observation_json,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, 'created', '', '', '{}', ?, ?)
                """,
                (
                    control_ref,
                    outbox_id,
                    request_id,
                    attempt_id,
                    action,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM flow_v3_transport_controls WHERE control_ref=?",
                (control_ref,),
            ).fetchone()
            assert row is not None
            value = dict(row)
            value["observation"] = json.loads(
                value.pop("observation_json")
            )
            value["action_ordinal"] = ordinal
            return value

    def accept_outbox_delivery(
        self,
        outbox_id: str,
        *,
        consumer_id: str,
        claim_token: str,
        observation: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        request_id = str(observation.get("request_id") or "")
        attempt_id = str(observation.get("attempt_id") or "")
        envelope_digest = str(observation.get("envelope_digest") or "")
        if str(observation.get("state") or "") != "accepted":
            raise FlowV3StoreError("endpoint delivery is not an acceptance receipt")
        receipt_payload = dict(observation)
        receipt_digest = canonical_digest(receipt_payload)
        receipt_id = f"accept-{receipt_digest[:24]}"
        with self.transaction() as conn:
            outbox = conn.execute(
                "SELECT * FROM flow_v3_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if outbox is None:
                raise FlowV3StoreError(f"unknown outbox item: {outbox_id}")
            if (
                str(outbox["state"]) != "claimed"
                or str(outbox["claimed_by"]) != consumer_id
                or str(outbox["claim_token"]) != claim_token
            ):
                raise FlowV3StoreError(
                    "outbox claim is stale or owned by another consumer"
                )
            if (
                str(outbox["request_id"]) != request_id
                or str(outbox["attempt_id"]) != attempt_id
                or str(outbox["envelope_digest"]) != envelope_digest
            ):
                raise FlowV3StoreError("endpoint acceptance identity mismatch")
            attempt = conn.execute(
                "SELECT state FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if attempt is None or str(attempt["state"]) != "dispatched":
                raise FlowV3StoreError("accepted attempt is not dispatched")
            conn.execute(
                """
                UPDATE flow_v3_outbox SET
                    state='delivered', receipt_json=?, claimed_by='',
                    claim_token='', claim_expires_at='', updated_at=?
                WHERE outbox_id=?
                """,
                (canonical_json(receipt_payload), now, outbox_id),
            )
            conn.execute(
                "UPDATE flow_v3_attempts SET state='accepted', accepted_at=?, "
                "updated_at=? WHERE request_id=? AND attempt_id=?",
                (now, now, request_id, attempt_id),
            )
            conn.execute(
                "UPDATE flow_v3_requests SET state='accepted', updated_at=? "
                "WHERE request_id=?",
                (now, request_id),
            )
            conn.execute(
                """
                INSERT INTO flow_v3_receipts(
                    receipt_id, request_id, attempt_id, receipt_kind,
                    payload_json, payload_digest, recorded_at
                ) VALUES(?, ?, ?, 'endpoint-accept', ?, ?, ?)
                ON CONFLICT(request_id, attempt_id, receipt_kind, payload_digest)
                DO NOTHING
                """,
                (
                    receipt_id,
                    request_id,
                    attempt_id,
                    canonical_json(receipt_payload),
                    receipt_digest,
                    now,
                ),
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="endpoint-accepted",
                previous_state="dispatched",
                current_state="accepted",
                actor=consumer_id,
                payload={"receipt_id": receipt_id},
                event_at=now,
            )
        return {
            "request_id": request_id,
            "attempt_id": attempt_id,
            "state": "accepted",
            "receipt_id": receipt_id,
        }

    def fail_outbox(
        self,
        outbox_id: str,
        *,
        consumer_id: str,
        claim_token: str,
        error: str,
        retry_at: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        state = "retry" if retry_at else "failed"
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE flow_v3_outbox SET
                    state=?, next_attempt_at=?, last_error=?, claimed_by='',
                    claim_token='', claim_expires_at='', updated_at=?
                WHERE outbox_id=? AND state='claimed'
                  AND claimed_by=? AND claim_token=?
                """,
                (
                    state,
                    retry_at,
                    error,
                    now,
                    outbox_id,
                    consumer_id,
                    claim_token,
                ),
            )
            if cursor.rowcount != 1:
                raise FlowV3StoreError("outbox claim is stale or owned by another consumer")
            row = conn.execute(
                "SELECT * FROM flow_v3_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            assert row is not None
            return decode_outbox(row)

    def record_failure_and_retry(
        self,
        request_id: str,
        attempt_id: str,
        *,
        stage_name: str,
        stage_try: int,
        failure: FailureRecord,
        decision: RetryDecision,
        evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        failure_id = uuid.uuid4().hex
        decision_id = uuid.uuid4().hex
        now = utc_now()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO flow_v3_failures(
                    failure_id, request_id, attempt_id, stage_name, stage_try,
                    domain, code, phase, detail, retryable, pre_publish,
                    result_visibility, evidence_json, recorded_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    failure_id,
                    request_id,
                    attempt_id,
                    stage_name,
                    int(stage_try),
                    failure.domain,
                    failure.code,
                    failure.phase,
                    failure.detail,
                    int(failure.retryable),
                    int(failure.pre_publish),
                    failure.result_visibility,
                    canonical_json(dict(evidence or {})),
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO flow_v3_retry_decisions(
                    decision_id, failure_id, request_id, attempt_id, action,
                    consumes_execution_attempt, reason, policy_version, decided_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    failure_id,
                    request_id,
                    attempt_id,
                    decision.action,
                    int(decision.consumes_execution_attempt),
                    decision.reason,
                    decision.policy_version,
                    now,
                ),
            )
        return {
            "failure_id": failure_id,
            "decision_id": decision_id,
            "failure": failure.to_dict(),
            "decision": decision.to_dict(),
        }

    def record_result(
        self,
        request_id: str,
        attempt_id: str,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.initialize()
        correctness = str(result.get("correctness_state") or "")
        performance = str(result.get("performance_state") or "")
        infrastructure = str(result.get("infrastructure_state") or "")
        terminal = str(result.get("terminal_state") or "")
        if correctness not in {"pass", "fail", "not-run", "incomplete"}:
            raise FlowV3StoreError(f"invalid correctness result state: {correctness}")
        if performance not in {"completed", "failed", "skipped", "not-run"}:
            raise FlowV3StoreError(f"invalid performance result state: {performance}")
        if infrastructure not in {"ok", "failed", "incomplete"}:
            raise FlowV3StoreError(
                f"invalid infrastructure result state: {infrastructure}"
            )
        if terminal not in TERMINAL_STATES:
            raise FlowV3StoreError(f"invalid terminal state: {terminal}")
        normalized = {
            **dict(result),
            "correctness_state": correctness,
            "performance_state": performance,
            "infrastructure_state": infrastructure,
            "terminal_state": terminal,
        }
        digest = canonical_digest(normalized)
        result_id = uuid.uuid4().hex
        now = utc_now()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO flow_v3_results(
                    result_id, request_id, attempt_id, correctness_state,
                    performance_state, infrastructure_state, terminal_state,
                    artifacts_json, result_digest, recorded_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id, attempt_id, result_digest) DO NOTHING
                """,
                (
                    result_id,
                    request_id,
                    attempt_id,
                    correctness,
                    performance,
                    infrastructure,
                    terminal,
                    canonical_json(list(result.get("artifacts", []))),
                    digest,
                    now,
                ),
            )
        return {"result_id": result_id, "result_digest": digest, **normalized}

    def ingest_result(
        self,
        request_id: str,
        attempt_id: str,
        result: Mapping[str, Any],
        *,
        actor: str,
    ) -> dict[str, Any]:
        self.initialize()
        normalized = validate_result_document(result)
        digest = canonical_digest(normalized)
        result_id = f"result-{digest[:24]}"
        artifacts = [
            dict(item)
            for item in normalized.get("artifacts", [])
            if isinstance(item, Mapping)
        ]
        endpoint_stages = [
            dict(item)
            for item in normalized.get("endpoint_stages", [])
            if isinstance(item, Mapping)
        ]
        spans = [
            dict(item)
            for item in normalized.get("spans", [])
            if isinstance(item, Mapping)
        ]
        now = utc_now()
        with self.transaction() as conn:
            attempt = conn.execute(
                "SELECT state FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if attempt is None:
                raise FlowV3StoreError(
                    f"unknown attempt: {request_id}/{attempt_id}"
                )
            if str(attempt["state"]) != "return-ready":
                existing = conn.execute(
                    "SELECT result_id FROM flow_v3_results "
                    "WHERE request_id=? AND attempt_id=? AND result_digest=?",
                    (request_id, attempt_id, digest),
                ).fetchone()
                if existing is not None and str(attempt["state"]) in {
                    "ingested",
                    "acknowledged",
                    *TERMINAL_STATES,
                }:
                    return {
                        "result_id": str(existing["result_id"]),
                        "result_digest": digest,
                        **normalized,
                    }
                raise FlowV3StoreError(
                    f"result cannot be ingested while attempt is {attempt['state']}"
                )
            conn.execute(
                """
                INSERT INTO flow_v3_results(
                    result_id, request_id, attempt_id, correctness_state,
                    performance_state, infrastructure_state, terminal_state,
                    artifacts_json, result_digest, recorded_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_id,
                    request_id,
                    attempt_id,
                    normalized["correctness_state"],
                    normalized["performance_state"],
                    normalized["infrastructure_state"],
                    normalized["terminal_state"],
                    canonical_json(artifacts),
                    digest,
                    now,
                ),
            )
            for item in artifacts:
                relative_path = str(item.get("relative_path") or "")
                artifact_digest = str(item.get("digest") or "")
                if not relative_path or not artifact_digest:
                    raise FlowV3StoreError(
                        "result artifact requires relative_path and digest"
                    )
                conn.execute(
                    """
                    INSERT INTO flow_v3_artifacts(
                        artifact_id, result_id, request_id, attempt_id,
                        relative_path, digest, size_bytes, required,
                        local_path, recorded_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"artifact-{canonical_digest(item)[:24]}",
                        result_id,
                        request_id,
                        attempt_id,
                        relative_path,
                        artifact_digest,
                        int(item.get("size_bytes", 0) or 0),
                        int(bool(item.get("required"))),
                        str(item.get("local_path") or ""),
                        now,
                    ),
                )
            for item in endpoint_stages:
                stage_name = str(item.get("stage_name") or "")
                stage_try = int(item.get("stage_try", 0) or 0)
                stage_state = str(item.get("state") or "")
                if not stage_name or stage_try < 1:
                    raise FlowV3StoreError(
                        "endpoint stage evidence requires stage_name and positive stage_try"
                    )
                if stage_state not in {
                    "succeeded",
                    "business-failed",
                    "infrastructure-failed",
                    "cancelled",
                }:
                    raise FlowV3StoreError(
                        f"invalid endpoint stage state: {stage_state}"
                    )
                conn.execute(
                    """
                    INSERT INTO flow_v3_stage_attempts(
                        request_id, attempt_id, stage_name, stage_try,
                        resource_class, state, started_at, finished_at,
                        exit_code, failure_id, evidence_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(request_id, attempt_id, stage_name, stage_try)
                    DO UPDATE SET
                        resource_class=excluded.resource_class,
                        state=excluded.state,
                        started_at=excluded.started_at,
                        finished_at=excluded.finished_at,
                        exit_code=excluded.exit_code,
                        failure_id=excluded.failure_id,
                        evidence_json=excluded.evidence_json
                    """,
                    (
                        request_id,
                        attempt_id,
                        stage_name,
                        stage_try,
                        str(item.get("resource_class") or ""),
                        stage_state,
                        str(item.get("started_at") or ""),
                        str(item.get("finished_at") or ""),
                        int(item.get("exit_code", 0) or 0),
                        str(item.get("failure_id") or ""),
                        canonical_json(item),
                    ),
                )
            for span in spans:
                required_span_fields = {
                    "span_id",
                    "trace_id",
                    "parent_span_id",
                    "stage_try",
                    "name",
                    "actor",
                    "resource",
                    "host",
                    "boot_id",
                    "pid",
                    "started_at",
                    "finished_at",
                    "started_monotonic_ns",
                    "finished_monotonic_ns",
                    "duration_ns",
                    "clock_contract",
                }
                missing = sorted(required_span_fields - set(span))
                if missing:
                    raise FlowV3StoreError(
                        "endpoint span is missing fields: " + ", ".join(missing)
                    )
                conn.execute(
                    """
                    INSERT INTO flow_v3_spans(
                        span_id, trace_id, parent_span_id, request_id, attempt_id,
                        stage_try, name, actor, resource, host, boot_id, pid,
                        started_at, finished_at, started_monotonic_ns,
                        finished_monotonic_ns, duration_ns, clock_contract,
                        payload_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(span_id) DO NOTHING
                    """,
                    (
                        str(span["span_id"]),
                        str(span["trace_id"]),
                        str(span["parent_span_id"]),
                        request_id,
                        attempt_id,
                        int(span["stage_try"]),
                        str(span["name"]),
                        str(span["actor"]),
                        str(span["resource"]),
                        str(span["host"]),
                        str(span["boot_id"]),
                        int(span["pid"]),
                        str(span["started_at"]),
                        str(span["finished_at"]),
                        int(span["started_monotonic_ns"]),
                        int(span["finished_monotonic_ns"]),
                        int(span["duration_ns"]),
                        str(span["clock_contract"]),
                        canonical_json(span),
                    ),
                )
            engine_state = normalized.get("engine_state", {})
            if not isinstance(engine_state, Mapping):
                engine_state = {}
            conn.execute(
                "UPDATE flow_v3_attempts SET state='ingested', "
                "device_session_started_at=?, device_session_finished_at=?, "
                "updated_at=? "
                "WHERE request_id=? AND attempt_id=?",
                (
                    str(engine_state.get("device_session_started_at") or ""),
                    str(engine_state.get("device_session_finished_at") or ""),
                    now,
                    request_id,
                    attempt_id,
                ),
            )
            conn.execute(
                "UPDATE flow_v3_requests SET state='ingested', updated_at=? "
                "WHERE request_id=?",
                (now, request_id),
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="result-ingested",
                previous_state="return-ready",
                current_state="ingested",
                actor=actor,
                payload={
                    "result_id": result_id,
                    "result_digest": digest,
                },
                event_at=now,
            )
        return {
            "result_id": result_id,
            "result_digest": digest,
            **normalized,
        }

    def result_for_attempt(
        self,
        request_id: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM flow_v3_results
                WHERE request_id=? AND attempt_id=?
                ORDER BY recorded_at DESC
                LIMIT 1
                """,
                (request_id, attempt_id),
            ).fetchone()
        if row is None:
            raise FlowV3StoreError(
                f"attempt has no durable result: {request_id}/{attempt_id}"
            )
        value = dict(row)
        value["artifacts"] = json.loads(value.pop("artifacts_json"))
        return value

    def claim_workflow_ingest(
        self,
        request_id: str,
        attempt_id: str,
        *,
        consumer_id: str,
        lease_seconds: int = 120,
    ) -> dict[str, Any]:
        self.initialize()
        now_dt = datetime.now(timezone.utc)
        now = iso(now_dt)
        token = uuid.uuid4().hex
        expires = iso(
            now_dt + timedelta(seconds=max(1, int(lease_seconds)))
        )
        with self.transaction() as conn:
            attempt = conn.execute(
                "SELECT state FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if attempt is None:
                raise FlowV3StoreError(
                    f"unknown attempt: {request_id}/{attempt_id}"
                )
            row = conn.execute(
                "SELECT * FROM flow_v3_workflow_ingest "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            attempt_state = str(attempt["state"])
            terminal_recovery = (
                attempt_state in TERMINAL_STATES
                and row is not None
                and str(row["state"]) in {"retry", "claimed", "succeeded"}
            )
            if attempt_state != "ingested" and not terminal_recovery:
                raise FlowV3StoreError(
                    "workflow ingest requires a durable ingested result "
                    "or an explicitly reopened terminal recovery"
                )
            if row is None:
                conn.execute(
                    """
                    INSERT INTO flow_v3_workflow_ingest(
                        request_id, attempt_id, state, try_count,
                        claimed_by, claim_token, claim_expires_at,
                        next_attempt_at, outcome_json, last_error,
                        created_at, updated_at
                    ) VALUES(?, ?, 'claimed', 1, ?, ?, ?, '', '{}', '', ?, ?)
                    """,
                    (
                        request_id,
                        attempt_id,
                        consumer_id,
                        token,
                        expires,
                        now,
                        now,
                    ),
                )
            else:
                state = str(row["state"])
                if state == "succeeded":
                    return decode_workflow_ingest(row)
                claim_expired = (
                    not str(row["claim_expires_at"])
                    or parse_time(str(row["claim_expires_at"])) <= now_dt
                )
                retry_due = (
                    not str(row["next_attempt_at"])
                    or parse_time(str(row["next_attempt_at"])) <= now_dt
                )
                if state == "claimed" and not claim_expired:
                    return decode_workflow_ingest(row)
                if state == "retry" and not retry_due:
                    return decode_workflow_ingest(row)
                if state == "failed":
                    return decode_workflow_ingest(row)
                conn.execute(
                    """
                    UPDATE flow_v3_workflow_ingest SET
                        state='claimed', try_count=try_count+1,
                        claimed_by=?, claim_token=?, claim_expires_at=?,
                        next_attempt_at='', updated_at=?
                    WHERE request_id=? AND attempt_id=?
                    """,
                    (
                        consumer_id,
                        token,
                        expires,
                        now,
                        request_id,
                        attempt_id,
                    ),
                )
            claimed = conn.execute(
                "SELECT * FROM flow_v3_workflow_ingest "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert claimed is not None
            return decode_workflow_ingest(claimed)

    def complete_workflow_ingest(
        self,
        request_id: str,
        attempt_id: str,
        *,
        consumer_id: str,
        claim_token: str,
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE flow_v3_workflow_ingest SET
                    state='succeeded', claimed_by='', claim_token='',
                    claim_expires_at='', next_attempt_at='',
                    outcome_json=?, last_error='', updated_at=?
                WHERE request_id=? AND attempt_id=? AND state='claimed'
                  AND claimed_by=? AND claim_token=?
                """,
                (
                    canonical_json(dict(outcome)),
                    now,
                    request_id,
                    attempt_id,
                    consumer_id,
                    claim_token,
                ),
            )
            if cursor.rowcount != 1:
                raise FlowV3StoreError(
                    "workflow ingest claim is stale or owned by another consumer"
                )
            row = conn.execute(
                "SELECT * FROM flow_v3_workflow_ingest "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert row is not None
            attempt = conn.execute(
                "SELECT state FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert attempt is not None
            attempt_state = str(attempt["state"])
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="workflow-result-ingested",
                previous_state=attempt_state,
                current_state=attempt_state,
                actor=consumer_id,
                payload=dict(outcome),
                event_at=now,
            )
            return decode_workflow_ingest(row)

    def fail_workflow_ingest(
        self,
        request_id: str,
        attempt_id: str,
        *,
        consumer_id: str,
        claim_token: str,
        error: str,
        retry_at: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        state = "retry" if retry_at else "failed"
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE flow_v3_workflow_ingest SET
                    state=?, claimed_by='', claim_token='',
                    claim_expires_at='', next_attempt_at=?,
                    last_error=?, updated_at=?
                WHERE request_id=? AND attempt_id=? AND state='claimed'
                  AND claimed_by=? AND claim_token=?
                """,
                (
                    state,
                    retry_at,
                    error,
                    now,
                    request_id,
                    attempt_id,
                    consumer_id,
                    claim_token,
                ),
            )
            if cursor.rowcount != 1:
                raise FlowV3StoreError(
                    "workflow ingest claim is stale or owned by another consumer"
                )
            row = conn.execute(
                "SELECT * FROM flow_v3_workflow_ingest "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert row is not None
            return decode_workflow_ingest(row)

    def acknowledge_result(
        self,
        request_id: str,
        attempt_id: str,
        *,
        receipt_id: str,
        receipt: Mapping[str, Any],
        terminal_state: str,
        actor: str,
    ) -> dict[str, Any]:
        self.initialize()
        if terminal_state not in TERMINAL_STATES:
            raise FlowV3StoreError(f"invalid terminal state: {terminal_state}")
        receipt_payload = dict(receipt)
        receipt_digest = canonical_digest(receipt_payload)
        now = utc_now()
        with self.transaction() as conn:
            existing_receipt = conn.execute(
                "SELECT request_id, attempt_id FROM flow_v3_receipts "
                "WHERE receipt_id=?",
                (receipt_id,),
            ).fetchone()
            if existing_receipt is not None:
                if (
                    str(existing_receipt["request_id"]) != request_id
                    or str(existing_receipt["attempt_id"]) != attempt_id
                ):
                    raise FlowV3StoreError(
                        f"return receipt identity collision: {receipt_id}"
                    )
                return {
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "state": terminal_state,
                    "receipt_id": receipt_id,
                }
            attempt = conn.execute(
                "SELECT state FROM flow_v3_attempts "
                "WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if attempt is None:
                raise FlowV3StoreError(
                    f"unknown attempt: {request_id}/{attempt_id}"
                )
            observed = str(attempt["state"])
            if observed == terminal_state:
                conn.execute(
                    """
                    INSERT INTO flow_v3_receipts(
                        receipt_id, request_id, attempt_id, receipt_kind,
                        payload_json, payload_digest, recorded_at
                    ) VALUES(?, ?, ?, 'return-ack', ?, ?, ?)
                    """,
                    (
                        receipt_id,
                        request_id,
                        attempt_id,
                        canonical_json(receipt_payload),
                        receipt_digest,
                        now,
                    ),
                )
                self._event(
                    conn,
                    request_id=request_id,
                    attempt_id=attempt_id,
                    event_type="return-acknowledged-after-terminal-recovery",
                    previous_state=observed,
                    current_state=observed,
                    actor=actor,
                    payload={"receipt_id": receipt_id},
                    event_at=now,
                )
                return {
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "state": terminal_state,
                    "receipt_id": receipt_id,
                }
            if observed != "ingested":
                raise FlowV3StoreError(
                    f"result cannot be acknowledged while attempt is {observed}"
                )
            conn.execute(
                """
                INSERT INTO flow_v3_receipts(
                    receipt_id, request_id, attempt_id, receipt_kind,
                    payload_json, payload_digest, recorded_at
                ) VALUES(?, ?, ?, 'return-ack', ?, ?, ?)
                """,
                (
                    receipt_id,
                    request_id,
                    attempt_id,
                    canonical_json(receipt_payload),
                    receipt_digest,
                    now,
                ),
            )
            conn.execute(
                "UPDATE flow_v3_attempts SET state=?, terminal_at=?, updated_at=? "
                "WHERE request_id=? AND attempt_id=?",
                (terminal_state, now, now, request_id, attempt_id),
            )
            conn.execute(
                "UPDATE flow_v3_requests SET state=?, updated_at=? "
                "WHERE request_id=?",
                (terminal_state, now, request_id),
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="return-acknowledged",
                previous_state="ingested",
                current_state="acknowledged",
                actor=actor,
                payload={"receipt_id": receipt_id},
                event_at=now,
            )
            self._event(
                conn,
                request_id=request_id,
                attempt_id=attempt_id,
                event_type="attempt-terminal",
                previous_state="acknowledged",
                current_state=terminal_state,
                actor=actor,
                payload={"receipt_id": receipt_id},
                event_at=now,
            )
        return {
            "request_id": request_id,
            "attempt_id": attempt_id,
            "state": terminal_state,
            "receipt_id": receipt_id,
        }

    def record_span(self, span: Mapping[str, Any]) -> None:
        self.initialize()
        required = {
            "span_id",
            "trace_id",
            "parent_span_id",
            "request_id",
            "attempt_id",
            "stage_try",
            "name",
            "actor",
            "resource",
            "host",
            "boot_id",
            "pid",
            "started_at",
            "finished_at",
            "started_monotonic_ns",
            "finished_monotonic_ns",
            "duration_ns",
            "clock_contract",
        }
        missing = sorted(required - set(span))
        if missing:
            raise FlowV3StoreError("span is missing fields: " + ", ".join(missing))
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO flow_v3_spans(
                    span_id, trace_id, parent_span_id, request_id, attempt_id,
                    stage_try, name, actor, resource, host, boot_id, pid,
                    started_at, finished_at, started_monotonic_ns,
                    finished_monotonic_ns, duration_ns, clock_contract, payload_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(span_id) DO NOTHING
                """,
                (
                    str(span["span_id"]),
                    str(span["trace_id"]),
                    str(span["parent_span_id"]),
                    str(span["request_id"]),
                    str(span["attempt_id"]),
                    int(span["stage_try"]),
                    str(span["name"]),
                    str(span["actor"]),
                    str(span["resource"]),
                    str(span["host"]),
                    str(span["boot_id"]),
                    int(span["pid"]),
                    str(span["started_at"]),
                    str(span["finished_at"]),
                    int(span["started_monotonic_ns"]),
                    int(span["finished_monotonic_ns"]),
                    int(span["duration_ns"]),
                    str(span["clock_contract"]),
                    canonical_json(dict(span)),
                ),
            )

    def quarantine(
        self,
        request_id: str,
        attempt_id: str,
        *,
        classification: str = "runtime-quarantine",
        reason: str,
        source: str,
        evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        if not classification.strip():
            raise FlowV3StoreError("quarantine classification must not be empty")
        quarantine_id = uuid.uuid4().hex
        now = utc_now()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO flow_v3_quarantine(
                    quarantine_id, request_id, attempt_id, classification,
                    reason, source, evidence_json, state, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'open', ?)
                """,
                (
                    quarantine_id,
                    request_id,
                    attempt_id,
                    classification,
                    reason,
                    source,
                    canonical_json(dict(evidence or {})),
                    now,
                ),
            )
        return {
            "quarantine_id": quarantine_id,
            "request_id": request_id,
            "attempt_id": attempt_id,
            "classification": classification,
            "reason": reason,
            "state": "open",
        }

    def status(self) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            request_states = {
                str(row["state"]): int(row["count"])
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS count FROM flow_v3_requests GROUP BY state"
                )
            }
            outbox_states = {
                str(row["state"]): int(row["count"])
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS count FROM flow_v3_outbox GROUP BY state"
                )
            }
            open_quarantine = int(
                conn.execute(
                    "SELECT COUNT(*) FROM flow_v3_quarantine WHERE state='open'"
                ).fetchone()[0]
            )
            active_attempts = int(
                conn.execute(
                    "SELECT COUNT(*) FROM flow_v3_attempts "
                    "WHERE state IN (" + ",".join("?" for _ in ACTIVE_STATES) + ")",
                    tuple(sorted(ACTIVE_STATES)),
                ).fetchone()[0]
            )
        return {
            "schema": "ascendop.flow.store-status.v3",
            "database": str(self.path),
            "database_generation": FLOW_DB_GENERATION,
            "database_schema": FLOW_DB_SCHEMA,
            "wire_version": FLOW_VERSION,
            "request_states": request_states,
            "outbox_states": outbox_states,
            "active_attempts": active_attempts,
            "open_quarantine": open_quarantine,
        }

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.commit()

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        *,
        request_id: str,
        attempt_id: str,
        event_type: str,
        previous_state: str,
        current_state: str,
        actor: str,
        payload: Mapping[str, Any],
        event_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO flow_v3_events(
                request_id, attempt_id, event_type, previous_state,
                current_state, actor, payload_json, event_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                attempt_id,
                event_type,
                previous_state,
                current_state,
                actor,
                canonical_json(dict(payload)),
                event_at,
            ),
        )

    @staticmethod
    def _enqueue_outbox(
        conn: sqlite3.Connection,
        *,
        request_id: str,
        attempt_id: str,
        destination: str,
        envelope_digest: str,
        payload: Mapping[str, Any],
        created_at: str,
    ) -> None:
        outbox_id = f"flow-{request_id}-{attempt_id}-{destination}"
        conn.execute(
            """
            INSERT INTO flow_v3_outbox(
                outbox_id, request_id, attempt_id, destination,
                envelope_digest, payload_json, state, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            ON CONFLICT(request_id, attempt_id, destination) DO NOTHING
            """,
            (
                outbox_id,
                request_id,
                attempt_id,
                destination,
                envelope_digest,
                canonical_json(dict(payload)),
                created_at,
                created_at,
            ),
        )


def request_identity_digest(envelope: Mapping[str, Any]) -> str:
    normalized = json.loads(canonical_json(envelope))
    meta = dict(normalized["meta"])
    meta.pop("attempt_id", None)
    meta.pop("created_at", None)
    normalized["meta"] = meta
    return canonical_digest(normalized)


def attempt_ordinal(attempt_id: str) -> int:
    tail = attempt_id.rsplit("-", 1)[-1]
    try:
        value = int(tail)
    except ValueError as exc:
        raise FlowV3StoreError(
            f"attempt id must end with an integer ordinal: {attempt_id}"
        ) from exc
    if value < 1:
        raise FlowV3StoreError("attempt ordinal must be positive")
    return value


def decode_component(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["capabilities"] = json.loads(value.pop("capabilities_json"))
    return value


def decode_workflow_ingest(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["outcome"] = json.loads(value.pop("outcome_json"))
    return value


def decode_attempt(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["envelope"] = json.loads(value.pop("envelope_json"))
    return value


def decode_request_attempt(
    request: sqlite3.Row,
    attempt: sqlite3.Row,
) -> dict[str, Any]:
    value = dict(request)
    value["envelope"] = json.loads(value.pop("envelope_json"))
    value["attempt"] = decode_attempt(attempt)
    return value


def decode_outbox(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["payload"] = json.loads(value.pop("payload_json"))
    value["receipt"] = json.loads(value.pop("receipt_json"))
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def validate_result_document(result: Mapping[str, Any]) -> dict[str, Any]:
    correctness = str(result.get("correctness_state") or "")
    performance = str(result.get("performance_state") or "")
    infrastructure = str(result.get("infrastructure_state") or "")
    terminal = str(result.get("terminal_state") or "")
    if correctness not in {"pass", "fail", "not-run", "incomplete"}:
        raise FlowV3StoreError(f"invalid correctness result state: {correctness}")
    if performance not in {"completed", "failed", "skipped", "not-run"}:
        raise FlowV3StoreError(f"invalid performance result state: {performance}")
    if infrastructure not in {"ok", "failed", "incomplete"}:
        raise FlowV3StoreError(
            f"invalid infrastructure result state: {infrastructure}"
        )
    if terminal not in TERMINAL_STATES:
        raise FlowV3StoreError(f"invalid terminal state: {terminal}")
    return {
        **dict(result),
        "correctness_state": correctness,
        "performance_state": performance,
        "infrastructure_state": infrastructure,
        "terminal_state": terminal,
    }
