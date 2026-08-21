from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Mapping

from ascendop_protocol.agent import (
    AGENT_ACTION_RECEIPT_SCHEMA,
    AGENT_ITERATION_SCHEMA,
    AGENT_WORK_LEASE_SCHEMA,
    validate_agent_action,
    validate_agent_action_receipt,
    validate_agent_context_snapshot,
    validate_agent_pool,
    validate_agent_registration,
)
from ascendop_protocol.evidence import (
    evidence_operation_definition,
    validate_evidence_operation_request,
    validate_evidence_operation_result,
)
from .errors import ControlRepositoryError
from .management_repository import ManagementRepository
from .service_repository import ServiceRepository


# Retry classification is a control-plane policy. It is intentionally kept out
# of the shared wire package so a local arbitration change cannot invalidate an
# otherwise compatible remote Engine generation.
AGENT_RETRYABLE_FAILURE_CLASSES = frozenset(
    {
        "agent-preflight",
        "agent-adapter",
        "agent-auth",
        "adapter-execution",
        "agent-output-validation",
    }
)


class _AgentRepository:
    """Schema-12 repository mixin for the authoritative control database.

    The host facade supplies ``connection``, ``transaction``, and ``_event``.
    This keeps domain transactions reusable without importing daemon code.
    """

    def register_agent(
        self,
        registration: Mapping[str, Any],
        *,
        health_state: str = "ready",
        boot_id: str = "",
        manager_runner_id: str = "",
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        value = validate_agent_registration(registration)
        if health_state not in {"ready", "degraded", "offline", "draining"}:
            raise ControlRepositoryError(f"unsupported agent health: {health_state}")
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        payload = _canonical_json(value)
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO agent_registrations_v4(
                    agent_id, driver, executable, executable_digest,
                    observed_version, registration_generation,
                    capabilities_json, health_state, boot_id, manager_runner_id,
                    heartbeat_at, lease_expires_at, registration_json, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_id) DO UPDATE SET
                    driver=excluded.driver,
                    executable=excluded.executable,
                    executable_digest=excluded.executable_digest,
                    observed_version=excluded.observed_version,
                    registration_generation=excluded.registration_generation,
                    capabilities_json=excluded.capabilities_json,
                    health_state=excluded.health_state,
                    boot_id=excluded.boot_id,
                    manager_runner_id=excluded.manager_runner_id,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at,
                    registration_json=excluded.registration_json,
                    updated_at=excluded.updated_at
                """,
                (
                    value["agent_id"],
                    value["driver"],
                    value["executable"],
                    value["executable_digest"],
                    value["observed_version"],
                    value["registration_generation"],
                    _canonical_json(value["capabilities"]),
                    health_state,
                    boot_id,
                    manager_runner_id,
                    now,
                    expires_at,
                    payload,
                    now,
                ),
            )
            self._event(
                conn,
                "agent-registration-observed",
                "agent",
                str(value["agent_id"]),
                {
                    "driver": value["driver"],
                    "health_state": health_state,
                    "registration_generation": value["registration_generation"],
                },
            )
        return self.agent_registration(str(value["agent_id"]))

    def register_agent_pool(self, pool: Mapping[str, Any]) -> dict[str, Any]:
        value = validate_agent_pool(pool)
        now = _utc_now()
        with self.transaction() as conn:
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
                    value["pool_id"],
                    int(value["enabled"]),
                    int(value["priority"]),
                    value["registration_generation"],
                    _canonical_json(value["roles"]),
                    _canonical_json(value["drivers"]),
                    _canonical_json(value["required_capabilities"]),
                    _canonical_json(value),
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "agent-pool-reconciled",
                "agent-pool",
                str(value["pool_id"]),
                {
                    "enabled": value["enabled"],
                    "registration_generation": value["registration_generation"],
                },
            )
        return self.agent_pool(str(value["pool_id"]))

    def agent_pool(self, pool_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT config_json, source_present FROM agent_pools_v4 "
                "WHERE pool_id=?",
                (pool_id,),
            ).fetchone()
        if row is None:
            raise ControlRepositoryError(f"Agent pool is not registered: {pool_id}")
        value = json.loads(str(row["config_json"]))
        value["source_present"] = bool(row["source_present"])
        return value

    def heartbeat_agent(
        self,
        agent_id: str,
        *,
        boot_id: str,
        health_state: str = "ready",
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        now = _utc_now()
        with self.transaction() as conn:
            changed = conn.execute(
                "UPDATE agent_registrations_v4 SET health_state=?, boot_id=?, "
                "heartbeat_at=?, lease_expires_at=?, updated_at=? WHERE agent_id=?",
                (
                    health_state,
                    boot_id,
                    now,
                    _future(now, lease_seconds),
                    now,
                    agent_id,
                ),
            ).rowcount
            if changed != 1:
                raise ControlRepositoryError(f"agent is not registered: {agent_id}")
        return self.agent_registration(agent_id)

    def quarantine_agent(
        self,
        agent_id: str,
        *,
        registration_generation: str,
        reason: str,
        evidence: Mapping[str, Any],
        boot_id: str,
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        now = _utc_now()
        with self.transaction() as conn:
            changed = conn.execute(
                "UPDATE agent_registrations_v4 SET health_state='degraded', "
                "boot_id=?, heartbeat_at=?, lease_expires_at=?, updated_at=? "
                "WHERE agent_id=? AND registration_generation=?",
                (
                    boot_id,
                    now,
                    _future(now, lease_seconds),
                    now,
                    agent_id,
                    registration_generation,
                ),
            ).rowcount
            if changed != 1:
                raise ControlRepositoryError(
                    "agent registration changed before quarantine"
                )
            self._event(
                conn,
                "agent-quarantined",
                "agent",
                agent_id,
                {
                    "registration_generation": registration_generation,
                    "reason": reason,
                    "evidence": dict(evidence),
                },
            )
        return self.agent_registration(agent_id)

    def agent_registration(self, agent_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM agent_registrations_v4 WHERE agent_id=?",
                (agent_id,),
            ).fetchone()
        if row is None:
            raise ControlRepositoryError(f"agent is not registered: {agent_id}")
        return _decode_agent(row)

    def agent_registrations_v4(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM agent_registrations_v4 ORDER BY driver, agent_id"
            ).fetchall()
        return [_decode_agent(row) for row in rows]

    def bind_agent(
        self,
        *,
        operator_id: str,
        role: str,
        agent_id: str,
        enabled: bool = True,
        priority: int = 100,
    ) -> dict[str, Any]:
        if role not in {"solver", "tester"}:
            raise ControlRepositoryError(f"unsupported agent role: {role}")
        now = _utc_now()
        with self.transaction() as conn:
            if conn.execute(
                "SELECT 1 FROM agent_registrations_v4 WHERE agent_id=?",
                (agent_id,),
            ).fetchone() is None:
                raise ControlRepositoryError(f"agent is not registered: {agent_id}")
            conn.execute(
                """
                INSERT INTO agent_role_bindings_v4(
                    operator_id, role, agent_id, enabled, priority,
                    last_selected_at, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, '', ?, ?)
                ON CONFLICT(operator_id, role, agent_id) DO UPDATE SET
                    enabled=excluded.enabled,
                    priority=excluded.priority,
                    updated_at=excluded.updated_at
                """,
                (operator_id, role, agent_id, int(enabled), int(priority), now, now),
            )
            self._event(
                conn,
                "agent-role-binding-upserted",
                "operator-role",
                f"{operator_id}:{role}",
                {"agent_id": agent_id, "enabled": enabled, "priority": priority},
            )
        return {
            "operator_id": operator_id,
            "role": role,
            "agent_id": agent_id,
            "enabled": enabled,
            "priority": int(priority),
        }

    def replace_agent_role_bindings(
        self,
        *,
        operator_id: str,
        role: str,
        bindings: list[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        """Atomically replace one role's eligible Agents for a topology generation."""
        if role not in {"solver", "tester"}:
            raise ControlRepositoryError(f"unsupported agent role: {role}")
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for binding in bindings:
            agent_id = str(binding.get("agent_id") or "").strip()
            if not agent_id or agent_id in seen:
                raise ControlRepositoryError("Agent role binding identity is invalid")
            seen.add(agent_id)
            normalized.append(
                {
                    "agent_id": agent_id,
                    "enabled": bool(binding.get("enabled", True)),
                    "priority": int(binding.get("priority", 100)),
                }
            )
        now = _utc_now()
        with self.transaction() as conn:
            if seen:
                placeholders = ",".join("?" for _ in seen)
                rows = conn.execute(
                    "SELECT agent_id FROM agent_registrations_v4 WHERE agent_id IN ("
                    + placeholders
                    + ")",
                    tuple(sorted(seen)),
                ).fetchall()
                observed = {str(row["agent_id"]) for row in rows}
                if observed != seen:
                    raise ControlRepositoryError(
                        "Agent role binding references unregistered Agents: "
                        + ", ".join(sorted(seen - observed))
                    )
            conn.execute(
                "UPDATE agent_role_bindings_v4 SET enabled=0, updated_at=? "
                "WHERE operator_id=? AND role=?",
                (now, operator_id, role),
            )
            for binding in normalized:
                conn.execute(
                    "INSERT INTO agent_role_bindings_v4(operator_id, role, agent_id, "
                    "enabled, priority, last_selected_at, created_at, updated_at) "
                    "VALUES(?, ?, ?, ?, ?, '', ?, ?) ON CONFLICT(operator_id, role, "
                    "agent_id) DO UPDATE SET enabled=excluded.enabled, "
                    "priority=excluded.priority, updated_at=excluded.updated_at",
                    (
                        operator_id,
                        role,
                        binding["agent_id"],
                        int(binding["enabled"]),
                        binding["priority"],
                        now,
                        now,
                    ),
                )
            self._event(
                conn,
                "agent-role-bindings-reconciled",
                "operator-role",
                f"{operator_id}:{role}",
                {"bindings": normalized},
            )
        return normalized

    def pin_agent_action(
        self,
        *,
        action_id: str,
        agent_id: str,
        expires_at: str,
    ) -> dict[str, Any]:
        now = _utc_now()
        try:
            expiry = datetime.fromisoformat(str(expires_at))
        except ValueError as exc:
            raise ControlRepositoryError("agent pin expiry must be ISO-8601") from exc
        if expiry.tzinfo is None or expiry <= datetime.fromisoformat(now):
            raise ControlRepositoryError("agent pin expiry must be in the future")
        with self.transaction() as conn:
            action = conn.execute(
                "SELECT operator_id, role, state FROM agent_actions_v4 "
                "WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None:
                raise ControlRepositoryError(f"agent action does not exist: {action_id}")
            if str(action["state"]) != "queued":
                raise ControlRepositoryError("only a queued agent action can be pinned")
            action_row = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE action_id=?", (action_id,)
            ).fetchone()
            compatible = self._select_agent(
                conn,
                action_row,
                now,
                required_agent_id=agent_id,
            )
            if compatible is None:
                raise ControlRepositoryError(
                    "pinned agent is not healthy and compatible with the action pool"
                )
            conn.execute(
                "UPDATE agent_actions_v4 SET preferred_agent_id=?, updated_at=? "
                "WHERE action_id=? AND state='queued'",
                (agent_id, now, action_id),
            )
            self._event(
                conn,
                "agent-action-debug-pinned",
                "agent-action",
                action_id,
                {"agent_id": agent_id, "expires_at": expiry.isoformat()},
            )
            return self._agent_action_in_connection(conn, action_id)

    def cancel_agent_action(
        self,
        *,
        action_id: str,
        actor_id: str,
        reason: str,
    ) -> dict[str, Any]:
        normalized_reason = str(reason).strip()
        if not normalized_reason:
            raise ControlRepositoryError("agent action cancellation reason is required")
        now = _utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state, iteration_id FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if row is None:
                raise ControlRepositoryError(f"agent action does not exist: {action_id}")
            state = str(row["state"])
            if state == "cancelled":
                return self._agent_action_in_connection(conn, action_id)
            if state in {"completed", "failed"}:
                raise ControlRepositoryError(
                    f"terminal agent action cannot be cancelled: {state}"
                )
            conn.execute(
                "UPDATE agent_actions_v4 SET state='cancelled', updated_at=? "
                "WHERE action_id=?",
                (now, action_id),
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET state='cancelled', "
                "completed_at=?, updated_at=? WHERE action_id=? "
                "AND state IN ('claimed','running','uncertain')",
                (now, now, action_id),
            )
            conn.execute(
                "UPDATE agent_work_leases_v4 SET state='released', released_at=?, "
                "heartbeat_at=? WHERE action_id=? AND state='active'",
                (now, now, action_id),
            )
            conn.execute(
                "UPDATE agent_iterations_v4 SET state='cancelled', updated_at=? "
                "WHERE iteration_id=?",
                (now, row["iteration_id"]),
            )
            self._event(
                conn,
                "agent-action-cancelled",
                "agent-action",
                action_id,
                {"actor_id": actor_id, "reason": normalized_reason},
            )
            return self._agent_action_in_connection(conn, action_id)

    def create_agent_action(
        self,
        action: Mapping[str, Any],
        context_snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        value = validate_agent_action(action)
        snapshot = validate_agent_context_snapshot(context_snapshot)
        if snapshot["iteration_id"] != value["iteration_id"]:
            raise ControlRepositoryError("context snapshot iteration mismatch")
        if snapshot["operator_id"] != value["operator_id"]:
            raise ControlRepositoryError("context snapshot operator mismatch")
        if snapshot["role"] != value["role"]:
            raise ControlRepositoryError("context snapshot role mismatch")
        now = _utc_now()
        action_json = _canonical_json(value)
        snapshot_json = _canonical_json(snapshot)
        snapshot_digest = hashlib.sha256(snapshot_json.encode("utf-8")).hexdigest()
        iteration = {
            "schema": AGENT_ITERATION_SCHEMA,
            "iteration_id": value["iteration_id"],
            "operator_id": value["operator_id"],
            "role": value["role"],
            "candidate_version": value["candidate_version"],
            "state": "queued",
            "source_before_digest": str(
                value.get("candidate_identity", {}).get("execution_source_digest")
                or ""
            ),
            "source_after_digest": "",
            "artifacts": [],
            "created_at": now,
            "updated_at": now,
        }
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT action_id, action_json FROM agent_actions_v4 "
                "WHERE idempotency_key=?",
                (value["idempotency_key"],),
            ).fetchone()
            if existing is not None:
                existing_action = json.loads(str(existing["action_json"]))
                if _without_created_at(existing_action) != _without_created_at(value):
                    raise ControlRepositoryError(
                        "agent action idempotency collision with different payload"
                    )
                return self._agent_action_in_connection(conn, str(existing["action_id"]))
            try:
                conn.execute(
                    "INSERT INTO agent_iterations_v4(iteration_id, operator_id, role, "
                    "candidate_version, state, source_before_digest, iteration_json, "
                    "created_at, updated_at) VALUES(?, ?, ?, ?, 'queued', ?, ?, ?, ?)",
                    (
                        value["iteration_id"],
                        value["operator_id"],
                        value["role"],
                        value["candidate_version"],
                        iteration["source_before_digest"],
                        _canonical_json(iteration),
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                active = conn.execute(
                    "SELECT iteration_id, state FROM agent_iterations_v4 "
                    "WHERE operator_id=? AND role=? AND candidate_version=? "
                    "AND state IN ('queued','claimed','running','uncertain',"
                    "'retry-pending') ORDER BY created_at, iteration_id LIMIT 1",
                    (
                        value["operator_id"],
                        value["role"],
                        value["candidate_version"],
                    ),
                ).fetchone()
                if active is not None:
                    raise ControlRepositoryError(
                        "agent candidate already has an active iteration: "
                        f"candidate={value['candidate_version']} "
                        f"iteration={active['iteration_id']} state={active['state']}"
                    ) from exc
                raise ControlRepositoryError(
                    "agent iteration identity collision: "
                    f"iteration={value['iteration_id']}"
                ) from exc
            conn.execute(
                "INSERT INTO agent_actions_v4(action_id, idempotency_key, "
                "iteration_id, operator_id, role, preferred_agent_id, state, "
                "action_json, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, "
                "'queued', ?, ?, ?)",
                (
                    value["action_id"],
                    value["idempotency_key"],
                    value["iteration_id"],
                    value["operator_id"],
                    value["role"],
                    str(value.get("preferred_agent_id") or ""),
                    action_json,
                    now,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO agent_context_snapshots_v4(snapshot_id, iteration_id, "
                "snapshot_digest, snapshot_json, created_at) VALUES(?, ?, ?, ?, ?)",
                (
                    snapshot["snapshot_id"],
                    snapshot["iteration_id"],
                    snapshot_digest,
                    snapshot_json,
                    now,
                ),
            )
            self._event(
                conn,
                "agent-action-created",
                "agent-action",
                str(value["action_id"]),
                {
                    "iteration_id": value["iteration_id"],
                    "operator_id": value["operator_id"],
                    "role": value["role"],
                    "snapshot_digest": snapshot_digest,
                },
            )
            return self._agent_action_in_connection(conn, str(value["action_id"]))

    def claim_agent_action(
        self,
        *,
        runner_id: str,
        boot_id: str,
        lease_seconds: int = 30,
        managed_agent_ids: set[str] | None = None,
    ) -> dict[str, Any] | None:
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            self._expire_agent_leases(conn, now)
            rows = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE state='queued' "
                "ORDER BY created_at, action_id"
            ).fetchall()
            for row in rows:
                action_id = str(row["action_id"])
                if not self._workflow_agent_gate_is_current(conn, row):
                    continue
                if conn.execute(
                    "SELECT 1 FROM agent_actions_v4 WHERE operator_id=? AND role=? "
                    "AND state IN ('claimed','running','uncertain') LIMIT 1",
                    (row["operator_id"], row["role"]),
                ).fetchone() is not None:
                    continue
                if conn.execute(
                    "SELECT 1 FROM agent_work_leases_v4 WHERE operator_id=? "
                    "AND role=? AND state='active' AND action_id<>?",
                    (row["operator_id"], row["role"], action_id),
                ).fetchone() is not None:
                    continue
                existing_lease = conn.execute(
                    "SELECT * FROM agent_work_leases_v4 WHERE action_id=?",
                    (action_id,),
                ).fetchone()
                # A retry normally remains sticky to its assigned Agent. A central
                # auth-failover decision clears that assignment so the same action
                # and lease can be rebound to another healthy pool member.
                required_agent_id = str(row["assigned_agent_id"] or "")
                agent = self._select_agent(
                    conn,
                    row,
                    now,
                    required_agent_id=required_agent_id,
                )
                if agent is None:
                    continue
                if (
                    managed_agent_ids is not None
                    and str(agent.get("agent_id") or "") not in managed_agent_ids
                ):
                    continue
                manager_runner_id = str(agent.get("manager_runner_id") or "")
                if manager_runner_id and manager_runner_id != runner_id:
                    continue
                selection_pool_id = str(agent.pop("_selection_pool_id"))
                ordinal = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM agent_action_attempts_v4 WHERE action_id=?",
                        (action_id,),
                    ).fetchone()[0]
                ) + 1
                attempt_id = f"aat-{uuid.uuid4().hex}"
                lease_id = (
                    str(existing_lease["lease_id"])
                    if existing_lease is not None
                    else f"awl-{uuid.uuid4().hex}"
                )
                # Preserve the durable lease identity across a bounded retry, but
                # rotate its fencing token for every attempt. Otherwise a stale
                # consumer from the prior attempt can mutate the newly claimed
                # attempt during the adapter-state handoff window.
                lease_token = secrets.token_urlsafe(32)
                acquired_at = (
                    str(existing_lease["acquired_at"])
                    if existing_lease is not None
                    else now
                )
                lease = {
                    "schema": AGENT_WORK_LEASE_SCHEMA,
                    "lease_id": lease_id,
                    "lease_token": lease_token,
                    "action_id": action_id,
                    "iteration_id": row["iteration_id"],
                    "operator_id": row["operator_id"],
                    "role": row["role"],
                    "agent_id": agent["agent_id"],
                    "state": "active",
                    "acquired_at": acquired_at,
                    "heartbeat_at": now,
                    "expires_at": expires_at,
                }
                conn.execute(
                    "INSERT INTO agent_action_attempts_v4(attempt_id, action_id, "
                    "ordinal, agent_id, runner_id, state, heartbeat_at, created_at, "
                    "updated_at) VALUES(?, ?, ?, ?, ?, 'claimed', ?, ?, ?)",
                    (
                        attempt_id,
                        action_id,
                        ordinal,
                        agent["agent_id"],
                        runner_id,
                        now,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    "INSERT INTO agent_pool_selections_v4(pool_id, agent_id, "
                    "last_selected_at, updated_at) VALUES(?, ?, ?, ?) "
                    "ON CONFLICT(pool_id, agent_id) DO UPDATE SET "
                    "last_selected_at=excluded.last_selected_at, "
                    "updated_at=excluded.updated_at",
                    (selection_pool_id, agent["agent_id"], now, now),
                )
                if existing_lease is None:
                    conn.execute(
                        "INSERT INTO agent_work_leases_v4(lease_id, lease_token, "
                        "action_id, iteration_id, operator_id, role, agent_id, "
                        "runner_id, state, acquired_at, heartbeat_at, expires_at, "
                        "lease_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, "
                        "?, ?)",
                        (
                            lease_id,
                            lease_token,
                            action_id,
                            row["iteration_id"],
                            row["operator_id"],
                            row["role"],
                            agent["agent_id"],
                            runner_id,
                            acquired_at,
                            now,
                            expires_at,
                            _canonical_json(lease),
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE agent_work_leases_v4 SET agent_id=?, runner_id=?, "
                        "lease_token=?, state='active', heartbeat_at=?, expires_at=?, "
                        "released_at='', lease_json=? "
                        "WHERE lease_id=?",
                        (
                            agent["agent_id"],
                            runner_id,
                            lease_token,
                            now,
                            expires_at,
                            _canonical_json(lease),
                            lease_id,
                        ),
                    )
                conn.execute(
                    "UPDATE agent_actions_v4 SET state='claimed', assigned_agent_id=?, "
                    "claimed_by=?, current_attempt_id=?, current_lease_id=?, "
                    "updated_at=? WHERE action_id=? AND state='queued'",
                    (
                        agent["agent_id"],
                        runner_id,
                        attempt_id,
                        lease_id,
                        now,
                        action_id,
                    ),
                )
                conn.execute(
                    "UPDATE agent_iterations_v4 SET state='claimed', agent_id=?, "
                    "action_id=?, updated_at=? WHERE iteration_id=?",
                    (agent["agent_id"], action_id, now, row["iteration_id"]),
                )
                conn.execute(
                    "UPDATE agent_role_bindings_v4 SET last_selected_at=?, updated_at=? "
                    "WHERE operator_id=? AND role=? AND agent_id=?",
                    (
                        now,
                        now,
                        row["operator_id"],
                        row["role"],
                        agent["agent_id"],
                    ),
                )
                self._event(
                    conn,
                    "agent-action-claimed",
                    "agent-action",
                    action_id,
                    {
                        "attempt_id": attempt_id,
                        "lease_id": lease_id,
                        "agent_id": agent["agent_id"],
                        "runner_id": runner_id,
                    },
                )
                attempt_context = self._attempt_context_in_connection(
                    conn,
                    action_id=action_id,
                    attempt_id=attempt_id,
                    ordinal=ordinal,
                )
                return {
                    "action": json.loads(str(row["action_json"])),
                    "context": self._context_in_connection(
                        conn, str(row["iteration_id"])
                    ),
                    "agent": agent,
                    "attempt_id": attempt_id,
                    "lease": lease,
                    "boot_id": boot_id,
                    "attempt_context": attempt_context,
                }
        return None

    def adopt_uncertain_agent_action(
        self,
        *,
        runner_id: str,
        boot_id: str,
        managed_agent_ids: set[str],
        lease_seconds: int = 30,
    ) -> dict[str, Any] | None:
        if not managed_agent_ids:
            return None
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        placeholders = ",".join("?" for _ in managed_agent_ids)
        with self.transaction() as conn:
            self._expire_agent_leases(conn, now)
            rows = conn.execute(
                "SELECT a.*, t.session_id, t.attempt_id, t.ordinal, l.lease_id, "
                "l.lease_token, l.acquired_at AS lease_acquired_at, "
                "l.state AS lease_state FROM agent_actions_v4 a "
                "JOIN agent_action_attempts_v4 t ON t.attempt_id=a.current_attempt_id "
                "JOIN agent_work_leases_v4 l ON l.lease_id=a.current_lease_id "
                "JOIN agent_registrations_v4 r ON r.agent_id=a.assigned_agent_id "
                "WHERE a.state='uncertain' AND a.assigned_agent_id IN ("
                + placeholders
                + ") AND r.health_state='ready' AND r.lease_expires_at>? "
                "AND (r.manager_runner_id='' OR r.manager_runner_id=?) "
                "ORDER BY a.updated_at, a.action_id",
                (*sorted(managed_agent_ids), now, runner_id),
            ).fetchall()
            for row in rows:
                conflict = conn.execute(
                    "SELECT 1 FROM agent_work_leases_v4 WHERE operator_id=? "
                    "AND role=? AND state='active' AND lease_id<>?",
                    (row["operator_id"], row["role"], row["lease_id"]),
                ).fetchone()
                if conflict is not None:
                    continue
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET runner_id=?, state='active', "
                    "heartbeat_at=?, expires_at=?, released_at='' WHERE lease_id=?",
                    (runner_id, now, expires_at, row["lease_id"]),
                )
                conn.execute(
                    "UPDATE agent_action_attempts_v4 SET runner_id=?, heartbeat_at=?, "
                    "updated_at=? WHERE attempt_id=?",
                    (runner_id, now, now, row["attempt_id"]),
                )
                self._event(
                    conn,
                    "agent-action-uncertain-adopted",
                    "agent-action",
                    str(row["action_id"]),
                    {
                        "agent_id": row["assigned_agent_id"],
                        "attempt_id": row["attempt_id"],
                        "lease_id": row["lease_id"],
                        "runner_id": runner_id,
                        "boot_id": boot_id,
                    },
                )
                agent_row = conn.execute(
                    "SELECT * FROM agent_registrations_v4 WHERE agent_id=?",
                    (row["assigned_agent_id"],),
                ).fetchone()
                lease = {
                    "schema": AGENT_WORK_LEASE_SCHEMA,
                    "lease_id": str(row["lease_id"]),
                    "lease_token": str(row["lease_token"]),
                    "action_id": str(row["action_id"]),
                    "iteration_id": str(row["iteration_id"]),
                    "operator_id": str(row["operator_id"]),
                    "role": str(row["role"]),
                    "agent_id": str(row["assigned_agent_id"]),
                    "state": "active",
                    "acquired_at": str(row["lease_acquired_at"]),
                    "heartbeat_at": now,
                    "expires_at": expires_at,
                }
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET lease_json=? WHERE lease_id=?",
                    (_canonical_json(lease), row["lease_id"]),
                )
                return {
                    "action": json.loads(str(row["action_json"])),
                    "context": self._context_in_connection(
                        conn, str(row["iteration_id"])
                    ),
                    "agent": _decode_agent(agent_row),
                    "attempt_id": str(row["attempt_id"]),
                    "session_id": str(row["session_id"]),
                    "lease": lease,
                    "boot_id": boot_id,
                    "attempt_context": self._attempt_context_in_connection(
                        conn,
                        action_id=str(row["action_id"]),
                        attempt_id=str(row["attempt_id"]),
                        ordinal=int(row["ordinal"]),
                    ),
                }
        return None

    def start_agent_action(
        self,
        *,
        action_id: str,
        lease_token: str,
        session_id: str,
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            lease = self._require_active_lease(conn, action_id, lease_token, now)
            action_row = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action_row is None:
                raise ControlRepositoryError("agent action disappeared before start")
            attempt_id = str(action_row["current_attempt_id"])
            attempt_row = conn.execute(
                "SELECT state, session_id FROM agent_action_attempts_v4 "
                "WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()
            if attempt_row is None:
                raise ControlRepositoryError("agent action attempt disappeared before start")
            prior_state = str(action_row["state"])
            existing_session_id = str(attempt_row["session_id"] or "")
            if existing_session_id and existing_session_id != session_id:
                raise ControlRepositoryError("agent action turn identity collision")
            if prior_state == "running":
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET heartbeat_at=?, expires_at=? "
                    "WHERE lease_id=?",
                    (now, expires_at, lease["lease_id"]),
                )
                return self._agent_action_in_connection(conn, action_id)
            if prior_state not in {"claimed", "uncertain"}:
                raise ControlRepositoryError(
                    f"agent action cannot start from state {prior_state!r}"
                )
            if prior_state == "claimed" and not self._workflow_agent_gate_is_current(
                conn, action_row
            ):
                details = {
                    "status": "cancelled",
                    "failure_class": "agent-gate-obsolete",
                    "reason": "workflow gate changed before Agent execution started",
                    "session_id": session_id,
                }
                conn.execute(
                    "UPDATE agent_actions_v4 SET state='cancelled', updated_at=? "
                    "WHERE action_id=? AND state='claimed'",
                    (now, action_id),
                )
                conn.execute(
                    "UPDATE agent_action_attempts_v4 SET state='cancelled', "
                    "session_id=?, completed_at=?, heartbeat_at=?, details_json=?, "
                    "updated_at=? WHERE attempt_id=?",
                    (
                        session_id,
                        now,
                        now,
                        _canonical_json(details),
                        now,
                        attempt_id,
                    ),
                )
                conn.execute(
                    "UPDATE agent_iterations_v4 SET state='cancelled', updated_at=? "
                    "WHERE iteration_id=?",
                    (now, lease["iteration_id"]),
                )
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET state='released', released_at=?, "
                    "heartbeat_at=? WHERE lease_id=?",
                    (now, now, lease["lease_id"]),
                )
                receipt = {
                    "schema": AGENT_ACTION_RECEIPT_SCHEMA,
                    "action_id": action_id,
                    "iteration_id": str(lease["iteration_id"]),
                    "agent_id": str(lease["agent_id"]),
                    "lease_id": str(lease["lease_id"]),
                    "status": "cancelled",
                    "started_at": now,
                    "completed_at": now,
                    "completion": details,
                    "artifacts": [],
                }
                conn.execute(
                    "INSERT INTO agent_action_receipts_v4(action_id, status, "
                    "receipt_json, completed_at) VALUES(?, 'cancelled', ?, ?) "
                    "ON CONFLICT(action_id) DO UPDATE SET status='cancelled', "
                    "receipt_json=excluded.receipt_json, "
                    "completed_at=excluded.completed_at",
                    (action_id, _canonical_json(receipt), now),
                )
                self._event(
                    conn,
                    "agent-action-obsolete-before-start",
                    "agent-action",
                    action_id,
                    {
                        "attempt_id": attempt_id,
                        "lease_id": lease["lease_id"],
                    },
                )
                return self._agent_action_in_connection(conn, action_id)
            conn.execute(
                "UPDATE agent_actions_v4 SET state='running', updated_at=? "
                "WHERE action_id=? AND state=?",
                (now, action_id, prior_state),
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET state='running', session_id=?, "
                "started_at=?, heartbeat_at=?, updated_at=? WHERE attempt_id=(SELECT "
                "current_attempt_id FROM agent_actions_v4 WHERE action_id=?) "
                "AND state IN ('claimed','uncertain')",
                (session_id, now, now, now, action_id),
            )
            conn.execute(
                "UPDATE agent_iterations_v4 SET state='running', updated_at=? "
                "WHERE iteration_id=?",
                (now, lease["iteration_id"]),
            )
            conn.execute(
                "UPDATE agent_work_leases_v4 SET heartbeat_at=?, expires_at=? "
                "WHERE lease_id=?",
                (now, expires_at, lease["lease_id"]),
            )
            self._event(
                conn,
                (
                    "agent-action-delivery-reconciled"
                    if prior_state == "uncertain"
                    else "agent-action-started"
                ),
                "agent-action",
                action_id,
                {
                    "session_id": session_id,
                    "lease_id": lease["lease_id"],
                    "prior_state": prior_state,
                    "lease_expires_at": expires_at,
                },
            )
            return self._agent_action_in_connection(conn, action_id)

    def heartbeat_agent_action(
        self,
        *,
        action_id: str,
        lease_token: str,
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            lease = self._require_active_lease(conn, action_id, lease_token, now)
            conn.execute(
                "UPDATE agent_work_leases_v4 SET heartbeat_at=?, expires_at=? "
                "WHERE lease_id=?",
                (now, expires_at, lease["lease_id"]),
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET heartbeat_at=?, updated_at=? "
                "WHERE attempt_id=(SELECT current_attempt_id FROM agent_actions_v4 "
                "WHERE action_id=?)",
                (now, now, action_id),
            )
        return {"action_id": action_id, "heartbeat_at": now, "expires_at": expires_at}

    def complete_agent_action(
        self,
        receipt: Mapping[str, Any],
        *,
        lease_token: str,
    ) -> dict[str, Any]:
        value = validate_agent_action_receipt(receipt)
        now = _utc_now()
        action_id = str(value["action_id"])
        with self.transaction() as conn:
            lease = self._require_active_lease(conn, action_id, lease_token, now)
            if str(value["lease_id"]) != str(lease["lease_id"]):
                raise ControlRepositoryError("agent receipt lease mismatch")
            if str(value["agent_id"]) != str(lease["agent_id"]):
                raise ControlRepositoryError("agent receipt identity mismatch")
            action_row = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action_row is None:
                raise ControlRepositoryError(
                    "agent action disappeared before completion"
                )
            reported_state = str(value["status"])
            obsolete_completion = (
                reported_state == "completed"
                and not self._workflow_agent_gate_is_current(conn, action_row)
            )
            if obsolete_completion:
                reported_completion = dict(value.get("completion", {}))
                completion = {
                    "summary": str(reported_completion.get("summary") or ""),
                    "session_id": str(
                        reported_completion.get("session_id") or ""
                    ),
                    "target_id": str(reported_completion.get("target_id") or ""),
                    "adapter_id": str(reported_completion.get("adapter_id") or ""),
                    "runner_generation": str(
                        reported_completion.get("runner_generation") or ""
                    ),
                    "agent_execution_contract_digest": str(
                        reported_completion.get(
                            "agent_execution_contract_digest"
                        )
                        or ""
                    ),
                    "failure_class": "agent-gate-obsolete",
                    "cancellation_reason": "workflow-gate-no-longer-current",
                    "reported_status": reported_state,
                    "reported_completion": reported_completion,
                }
                value = validate_agent_action_receipt(
                    {
                        **value,
                        "status": "cancelled",
                        "completion": completion,
                    }
                )
            state = str(value["status"])
            conn.execute(
                "UPDATE agent_actions_v4 SET state=?, updated_at=? WHERE action_id=?",
                (state, now, action_id),
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET state=?, completed_at=?, "
                "heartbeat_at=?, details_json=?, session_id=CASE WHEN ?='' THEN "
                "session_id ELSE ? END, updated_at=? WHERE attempt_id=(SELECT "
                "current_attempt_id FROM agent_actions_v4 WHERE action_id=?)",
                (
                    state,
                    now,
                    now,
                    _canonical_json(value["completion"]),
                    str(value["completion"].get("session_id") or ""),
                    str(value["completion"].get("session_id") or ""),
                    now,
                    action_id,
                ),
            )
            if state != "uncertain":
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET state='released', released_at=?, "
                    "heartbeat_at=? WHERE lease_id=?",
                    (now, now, lease["lease_id"]),
                )
            source_after = str(value.get("completion", {}).get("source_after_digest") or "")
            conn.execute(
                "UPDATE agent_iterations_v4 SET state=?, source_after_digest=?, "
                "iteration_json=?, updated_at=? WHERE iteration_id=?",
                (
                    state,
                    source_after,
                    _canonical_json(
                        {
                            "schema": AGENT_ITERATION_SCHEMA,
                            "iteration_id": value["iteration_id"],
                            "operator_id": lease["operator_id"],
                            "role": lease["role"],
                            "candidate_version": self._candidate_version(conn, action_id),
                            "state": state,
                            "agent_id": value["agent_id"],
                            "source_after_digest": source_after,
                            "artifacts": list(value.get("artifacts", [])),
                            "created_at": self._iteration_created_at(
                                conn, str(value["iteration_id"])
                            ),
                            "updated_at": now,
                        }
                    ),
                    now,
                    value["iteration_id"],
                ),
            )
            if state != "uncertain":
                conn.execute(
                    "INSERT INTO agent_action_receipts_v4(action_id, status, "
                    "receipt_json, completed_at) VALUES(?, ?, ?, ?) "
                    "ON CONFLICT(action_id) DO UPDATE SET status=excluded.status, "
                    "receipt_json=excluded.receipt_json, "
                    "completed_at=excluded.completed_at",
                    (action_id, state, _canonical_json(value), value["completed_at"]),
                )
            self._event(
                conn,
                (
                    "agent-action-obsolete-at-completion"
                    if obsolete_completion
                    else (
                        "agent-action-uncertain"
                        if state == "uncertain"
                        else "agent-action-terminal"
                    )
                ),
                "agent-action",
                action_id,
                {
                    "iteration_id": value["iteration_id"],
                    "agent_id": value["agent_id"],
                    "status": state,
                    "reported_status": reported_state,
                },
            )
            return self._agent_action_in_connection(conn, action_id)

    def defer_agent_action_retry(
        self,
        *,
        action_id: str,
        lease_token: str,
        failure: Mapping[str, Any],
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        """Record a retryable pre-turn attempt without terminalizing its action."""
        failure_class = str(failure.get("failure_class") or "")
        if failure_class not in AGENT_RETRYABLE_FAILURE_CLASSES:
            raise ControlRepositoryError(
                "only an agent preflight, adapter, or authentication failure can "
                "enter retry arbitration"
            )
        runner_generation = str(failure.get("runner_generation") or "").strip()
        if not runner_generation:
            raise ControlRepositoryError("agent retry failure requires runner_generation")
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            lease = self._require_active_lease(conn, action_id, lease_token, now)
            attempt_id = str(
                conn.execute(
                    "SELECT current_attempt_id FROM agent_actions_v4 WHERE action_id=?",
                    (action_id,),
                ).fetchone()["current_attempt_id"]
            )
            conn.execute(
                "UPDATE agent_actions_v4 SET state='retry-pending', updated_at=? "
                "WHERE action_id=? AND state IN ('claimed','running','uncertain')",
                (now, action_id),
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET state='retry-pending', "
                "completed_at=?, heartbeat_at=?, details_json=?, updated_at=? "
                "WHERE attempt_id=?",
                (now, now, _canonical_json(dict(failure)), now, attempt_id),
            )
            conn.execute(
                "UPDATE agent_iterations_v4 SET state='retry-pending', updated_at=? "
                "WHERE iteration_id=?",
                (now, lease["iteration_id"]),
            )
            conn.execute(
                "UPDATE agent_work_leases_v4 SET heartbeat_at=?, expires_at=? "
                "WHERE lease_id=?",
                (now, expires_at, lease["lease_id"]),
            )
            self._event(
                conn,
                "agent-action-retry-pending",
                "agent-action",
                action_id,
                {
                    "attempt_id": attempt_id,
                    "failure_class": failure_class,
                    "runner_generation": runner_generation,
                },
            )
            return self._agent_action_in_connection(conn, action_id)

    def agent_retry_candidates(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT a.action_id, a.iteration_id, a.state AS action_state, "
                "a.current_attempt_id, t.ordinal, t.state AS attempt_state, "
                "t.details_json, l.lease_id, l.agent_id, l.state AS lease_state "
                "FROM agent_actions_v4 a "
                "JOIN agent_action_attempts_v4 t "
                "ON t.attempt_id=a.current_attempt_id "
                "JOIN agent_work_leases_v4 l ON l.action_id=a.action_id "
                "WHERE a.state IN ('retry-pending','failed') "
                "ORDER BY a.updated_at, a.action_id LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                details = json.loads(str(row["details_json"] or "{}"))
                failure_class = str(details.get("failure_class") or "")
                base_failure_class = _base_agent_failure_class(failure_class)
                if base_failure_class not in AGENT_RETRYABLE_FAILURE_CLASSES:
                    normalized = _normalize_legacy_agent_retry_failure(
                        details,
                        action_state=str(row["action_state"]),
                    )
                    if normalized is None:
                        continue
                    details = normalized
                result.append({**dict(row), "failure": details})
            return result

    def reclassify_legacy_agent_evidence_validation(
        self,
        *,
        action_id: str,
        attempt_id: str,
        proof: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reclassify one proven zero-byte evidence false failure for retry policy."""

        proof_value = dict(proof)
        if (
            proof_value.get("schema")
            != "ascendop.agent-evidence-recovery-proof.v1"
            or proof_value.get("action_id") != action_id
            or not isinstance(proof_value.get("manifest_digest"), str)
            or len(str(proof_value.get("manifest_digest"))) != 64
            or not isinstance(proof_value.get("file_count"), int)
            or int(proof_value.get("file_count")) < 1
            or not isinstance(proof_value.get("zero_byte_file_count"), int)
            or int(proof_value.get("zero_byte_file_count")) < 1
            or not isinstance(proof_value.get("proof_digest"), str)
            or len(str(proof_value.get("proof_digest"))) != 64
        ):
            raise ControlRepositoryError("legacy Agent evidence recovery proof is invalid")
        proof_core = dict(proof_value)
        proof_digest = str(proof_core.pop("proof_digest"))
        if hashlib.sha256(_canonical_json(proof_core).encode("utf-8")).hexdigest() != proof_digest:
            raise ControlRepositoryError(
                "legacy Agent evidence recovery proof digest mismatch"
            )
        now = _utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT a.state AS action_state, a.current_attempt_id, "
                "t.state AS attempt_state, t.details_json "
                "FROM agent_actions_v4 a JOIN agent_action_attempts_v4 t "
                "ON t.attempt_id=a.current_attempt_id WHERE a.action_id=?",
                (action_id,),
            ).fetchone()
            if row is None or str(row["current_attempt_id"]) != attempt_id:
                raise ControlRepositoryError(
                    "legacy Agent evidence recovery candidate has changed"
                )
            details = json.loads(str(row["details_json"] or "{}"))
            if (
                str(row["action_state"]) != "failed"
                or str(row["attempt_state"]) != "failed"
                or details.get("failure_class") != "protocol"
                or details.get("adapter_id") != "codex-ide-task-adapter"
                or details.get("validation_error")
                != "Agent evidence blob changed during the turn"
                or details.get("summary") != "exact_codex_ide_turn_completed"
                or str(details.get("session_id") or "")
            ):
                raise ControlRepositoryError(
                    "Agent failure is not the legacy zero-byte evidence defect"
                )
            details.update(
                {
                    "failure_class": "agent-output-validation",
                    "legacy_classification": True,
                    "original_failure_class": "protocol",
                    "evidence_recovery_proof": proof_value,
                }
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET details_json=?, updated_at=? "
                "WHERE attempt_id=?",
                (_canonical_json(details), now, attempt_id),
            )
            conn.execute(
                "UPDATE agent_actions_v4 SET updated_at=? WHERE action_id=?",
                (now, action_id),
            )
            self._event(
                conn,
                "agent-action-legacy-evidence-reclassified",
                "agent-action",
                action_id,
                {
                    "attempt_id": attempt_id,
                    "from_failure_class": "protocol",
                    "to_failure_class": "agent-output-validation",
                    "proof_digest": proof_digest,
                },
            )
            return self._agent_action_in_connection(conn, action_id)

    def apply_agent_retry_decision(
        self,
        *,
        action_id: str,
        attempt_id: str,
        decision: str,
        reason: str,
        code_generation: str,
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        if decision not in {"retry", "exhausted"}:
            raise ControlRepositoryError(f"unsupported agent retry decision: {decision}")
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT a.*, t.ordinal, t.state AS attempt_state, t.details_json, "
                "l.lease_id, l.lease_token, "
                "l.agent_id, l.state AS lease_state FROM agent_actions_v4 a "
                "JOIN agent_action_attempts_v4 t "
                "ON t.attempt_id=a.current_attempt_id "
                "JOIN agent_work_leases_v4 l ON l.action_id=a.action_id "
                "WHERE a.action_id=?",
                (action_id,),
            ).fetchone()
            if row is None or str(row["current_attempt_id"]) != attempt_id:
                raise ControlRepositoryError("agent retry candidate has changed")
            details = json.loads(str(row["details_json"] or "{}"))
            failure_class = str(details.get("failure_class") or "")
            base_failure_class = _base_agent_failure_class(failure_class)
            if (
                decision == "exhausted"
                and str(row["state"]) == "failed"
                and str(row["attempt_state"]) == "failed"
                and failure_class.endswith("-retry-exhausted")
            ):
                return self._agent_action_in_connection(conn, action_id)
            if base_failure_class not in AGENT_RETRYABLE_FAILURE_CLASSES:
                normalized = _normalize_legacy_agent_retry_failure(
                    details,
                    action_state=str(row["state"]),
                )
                if normalized is None:
                    raise ControlRepositoryError("agent retry failure class has changed")
                details = normalized
                failure_class = str(details["failure_class"])
                base_failure_class = failure_class
            if decision == "retry":
                retry_details = {
                    **details,
                    "retry_decision": "retry",
                    "retry_reason": reason,
                }
                clear_assignment = base_failure_class == "agent-auth"
                conn.execute(
                    "UPDATE agent_actions_v4 SET state='queued', claimed_by='', "
                    "assigned_agent_id=CASE WHEN ? THEN '' ELSE assigned_agent_id END, "
                    "updated_at=? WHERE action_id=?",
                    (int(clear_assignment), now, action_id),
                )
                conn.execute(
                    "UPDATE agent_iterations_v4 SET state='queued', "
                    "agent_id=CASE WHEN ? THEN '' ELSE agent_id END, updated_at=? "
                    "WHERE iteration_id=?",
                    (int(clear_assignment), now, row["iteration_id"]),
                )
                conn.execute(
                    "UPDATE agent_action_attempts_v4 SET state='failed', "
                    "completed_at=?, heartbeat_at=?, details_json=?, updated_at=? "
                    "WHERE attempt_id=?",
                    (
                        now,
                        now,
                        _canonical_json(retry_details),
                        now,
                        attempt_id,
                    ),
                )
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET state='active', released_at='', "
                    "heartbeat_at=?, expires_at=? WHERE lease_id=?",
                    (now, expires_at, row["lease_id"]),
                )
                # Schema 12 stored an incorrect terminal projection for pre-V4.1
                # preflight failures. The immutable attempt and event remain evidence.
                conn.execute(
                    "DELETE FROM agent_action_receipts_v4 WHERE action_id=?",
                    (action_id,),
                )
                event_type = "agent-action-retry-authorized"
            else:
                terminal_details = {
                    **details,
                    "status": "failed",
                    "failure_class": f"{failure_class}-retry-exhausted",
                    "retry_reason": reason,
                }
                conn.execute(
                    "UPDATE agent_actions_v4 SET state='failed', updated_at=? "
                    "WHERE action_id=?",
                    (now, action_id),
                )
                conn.execute(
                    "UPDATE agent_iterations_v4 SET state='failed', updated_at=? "
                    "WHERE iteration_id=?",
                    (now, row["iteration_id"]),
                )
                conn.execute(
                    "UPDATE agent_action_attempts_v4 SET state='failed', "
                    "completed_at=?, heartbeat_at=?, details_json=?, updated_at=? "
                    "WHERE attempt_id=?",
                    (now, now, _canonical_json(terminal_details), now, attempt_id),
                )
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET state='released', released_at=?, "
                    "heartbeat_at=? WHERE lease_id=?",
                    (now, now, row["lease_id"]),
                )
                receipt = {
                    "schema": AGENT_ACTION_RECEIPT_SCHEMA,
                    "action_id": action_id,
                    "iteration_id": str(row["iteration_id"]),
                    "agent_id": str(row["agent_id"]),
                    "lease_id": str(row["lease_id"]),
                    "status": "failed",
                    "started_at": now,
                    "completed_at": now,
                    "completion": terminal_details,
                    "artifacts": list(details.get("artifacts", [])),
                }
                conn.execute(
                    "INSERT INTO agent_action_receipts_v4(action_id, status, "
                    "receipt_json, completed_at) VALUES(?, 'failed', ?, ?) "
                    "ON CONFLICT(action_id) DO UPDATE SET status='failed', "
                    "receipt_json=excluded.receipt_json, "
                    "completed_at=excluded.completed_at",
                    (action_id, _canonical_json(receipt), now),
                )
                event_type = "agent-action-retry-exhausted"
            self._event(
                conn,
                event_type,
                "agent-action",
                action_id,
                {
                    "attempt_id": attempt_id,
                    "attempt_ordinal": int(row["ordinal"]),
                    "decision": decision,
                    "reason": reason,
                    "code_generation": code_generation,
                },
            )
            return self._agent_action_in_connection(conn, action_id)

    def agent_actions_v4(self, *, state: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM agent_actions_v4"
        parameters: tuple[Any, ...] = ()
        if state is not None:
            query += " WHERE state=?"
            parameters = (state,)
        query += " ORDER BY created_at, action_id"
        with self.connection() as conn:
            rows = conn.execute(query, parameters).fetchall()
            return [self._agent_action_in_connection(conn, str(row["action_id"])) for row in rows]

    def agent_action(self, action_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT action_id FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if row is None:
                return None
            return self._agent_action_in_connection(conn, action_id)

    def agent_action_receipt(self, action_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT receipt_json FROM agent_action_receipts_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            return json.loads(str(row["receipt_json"])) if row is not None else None

    def reconcile_queued_workflow_agent_actions(
        self,
        current_action_ids: set[str],
    ) -> list[str]:
        now = _utc_now()
        with self.transaction() as conn:
            return self._cancel_obsolete_workflow_agent_actions_in_connection(
                conn,
                current_action_ids=current_action_ids,
                now=now,
            )

    def reconcile_workflow_agent_candidate(
        self,
        *,
        operator_id: str,
        role: str,
        candidate_version: str,
        keep_action_id: str,
    ) -> list[str]:
        """Cancel queued restagings for one fixed board-owned candidate."""

        now = _utc_now()
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT action_id, action_json FROM agent_actions_v4 "
                "WHERE state IN ('queued','retry-pending')"
            ).fetchall()
            current_action_ids = {keep_action_id}
            for row in rows:
                action = json.loads(str(row["action_json"]))
                same_candidate = (
                    action.get("operator_id") == operator_id
                    and action.get("role") == role
                    and action.get("candidate_version") == candidate_version
                )
                if not same_candidate:
                    current_action_ids.add(str(row["action_id"]))
            return self._cancel_obsolete_workflow_agent_actions_in_connection(
                conn,
                current_action_ids=current_action_ids,
                now=now,
            )

    def _cancel_obsolete_workflow_agent_actions_in_connection(
        self,
        conn: sqlite3.Connection,
        *,
        current_action_ids: set[str],
        now: str,
    ) -> list[str]:
        cancelled: list[str] = []
        rows = conn.execute(
            "SELECT a.action_id, a.iteration_id, a.action_json, "
            "a.state AS action_state, a.current_attempt_id, "
            "a.current_lease_id, t.state AS attempt_state, t.details_json, "
            "l.state AS lease_state FROM agent_actions_v4 a "
            "LEFT JOIN agent_action_attempts_v4 t "
            "ON t.attempt_id=a.current_attempt_id "
            "LEFT JOIN agent_work_leases_v4 l "
            "ON l.lease_id=a.current_lease_id "
            "WHERE a.state IN ('queued','retry-pending') "
            "ORDER BY a.created_at, a.action_id"
        ).fetchall()
        for row in rows:
            action_id = str(row["action_id"])
            if action_id in current_action_ids:
                continue
            action = json.loads(str(row["action_json"]))
            identity = action.get("candidate_identity", {})
            if not isinstance(identity, Mapping) or (
                identity.get("origin") != "workflow-gate"
            ):
                continue
            prior_state = str(row["action_state"])
            attempt_id = str(row["current_attempt_id"] or "")
            lease_id = str(row["current_lease_id"] or "")
            if prior_state == "retry-pending" and (
                str(row["attempt_state"] or "") != "retry-pending"
                or str(row["lease_state"] or "") != "active"
                or not attempt_id
                or not lease_id
            ):
                # A mismatched retry projection is uncertain state and must be
                # reconciled by recovery rather than discarded as obsolete.
                continue
            conn.execute(
                "UPDATE agent_actions_v4 SET state='cancelled', updated_at=? "
                "WHERE action_id=? AND state=?",
                (now, action_id, prior_state),
            )
            conn.execute(
                "UPDATE agent_iterations_v4 SET state='cancelled', updated_at=? "
                "WHERE iteration_id=? AND state=?",
                (now, row["iteration_id"], prior_state),
            )
            if prior_state == "retry-pending":
                details = json.loads(str(row["details_json"] or "{}"))
                details.update(
                    {
                        "status": "cancelled",
                        "cancellation_reason": "workflow-gate-no-longer-current",
                    }
                )
                conn.execute(
                    "UPDATE agent_action_attempts_v4 SET state='cancelled', "
                    "completed_at=?, heartbeat_at=?, details_json=?, "
                    "updated_at=? WHERE attempt_id=? AND state='retry-pending'",
                    (now, now, _canonical_json(details), now, attempt_id),
                )
                conn.execute(
                    "UPDATE agent_work_leases_v4 SET state='released', "
                    "released_at=?, heartbeat_at=? WHERE lease_id=? "
                    "AND state='active'",
                    (now, now, lease_id),
                )
            self._event(
                conn,
                "agent-action-obsolete",
                "agent-action",
                action_id,
                {
                    "reason": "workflow-gate-no-longer-current",
                    "prior_state": prior_state,
                    "attempt_id": attempt_id,
                    "lease_id": lease_id,
                },
            )
            cancelled.append(action_id)
        return cancelled

    def synchronize_workflow_agent_gate_heads(
        self,
        actions: list[Mapping[str, Any]],
        *,
        observed_at: str,
    ) -> list[str]:
        heads: dict[str, dict[str, str]] = {}
        current_action_ids: set[str] = set()
        for action in actions:
            action_id = str(action.get("action_id") or "")
            operator_id = str(action.get("operator_id") or "")
            role = str(action.get("role") or "")
            if not action_id or not operator_id or role not in {"solver", "tester"}:
                raise ControlRepositoryError("invalid workflow Agent gate head")
            heads.setdefault(operator_id, {})[role] = action_id
            current_action_ids.add(action_id)
        now = _utc_now()
        cancelled: list[str] = []
        with self.transaction() as conn:
            previous = conn.execute(
                "SELECT revision FROM scheduler_state WHERE scheduler_id=?",
                ("agent-gate-heads-v4",),
            ).fetchone()
            revision = int(previous["revision"] if previous is not None else 0) + 1
            state = {
                "schema": "ascendop.agent-gate-heads.v1",
                "revision": revision,
                "observed_at": observed_at,
                "heads": heads,
            }
            conn.execute(
                "INSERT INTO scheduler_state(scheduler_id, revision, state_json, "
                "updated_at) VALUES(?, ?, ?, ?) ON CONFLICT(scheduler_id) DO "
                "UPDATE SET revision=excluded.revision, "
                "state_json=excluded.state_json, updated_at=excluded.updated_at",
                (
                    "agent-gate-heads-v4",
                    revision,
                    _canonical_json(state),
                    now,
                ),
            )
            cancelled = self._cancel_obsolete_workflow_agent_actions_in_connection(
                conn,
                current_action_ids=current_action_ids,
                now=now,
            )
            self._event(
                conn,
                "agent-gate-heads-synchronized",
                "scheduler-state",
                "agent-gate-heads-v4",
                {
                    "revision": revision,
                    "head_count": len(current_action_ids),
                    "cancelled_count": len(cancelled),
                },
            )
        return cancelled

    def workflow_agent_action_is_current(self, action_id: str) -> bool:
        """Return whether a workflow-gate action is still its role's head."""

        with self.connection() as conn:
            action_row = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action_row is None:
                raise ControlRepositoryError(f"unknown agent action: {action_id}")
            return self._workflow_agent_gate_is_current(conn, action_row)

    def recover_cancelled_published_agent_action(
        self,
        *,
        action_id: str,
        attempt_id: str,
        turn_id: str,
        verification: str,
        runner_id: str,
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        """Recover one exact delivered turn cancelled by the old restaging bug."""

        if verification != "exact-delivered-turn-completed":
            raise ControlRepositoryError(
                "cancelled Agent recovery requires exact delivered-turn verification"
            )
        if not turn_id.strip():
            raise ControlRepositoryError("cancelled Agent recovery requires turn_id")
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            action_row = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action_row is None:
                raise ControlRepositoryError(f"unknown agent action: {action_id}")
            if str(action_row["state"]) != "cancelled":
                raise ControlRepositoryError(
                    "published Agent recovery requires a cancelled action"
                )
            if str(action_row["current_attempt_id"]) != attempt_id:
                raise ControlRepositoryError(
                    "published Agent recovery attempt identity changed"
                )
            if not self._workflow_agent_gate_is_current(conn, action_row):
                raise ControlRepositoryError(
                    "published Agent recovery rejected: workflow gate is obsolete"
                )
            attempt_row = conn.execute(
                "SELECT * FROM agent_action_attempts_v4 WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()
            lease_row = conn.execute(
                "SELECT * FROM agent_work_leases_v4 WHERE lease_id=?",
                (action_row["current_lease_id"],),
            ).fetchone()
            iteration_row = conn.execute(
                "SELECT * FROM agent_iterations_v4 WHERE iteration_id=?",
                (action_row["iteration_id"],),
            ).fetchone()
            receipt_row = conn.execute(
                "SELECT * FROM agent_action_receipts_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if None in (attempt_row, lease_row, iteration_row, receipt_row):
                raise ControlRepositoryError(
                    "published Agent recovery evidence is incomplete"
                )
            if (
                str(attempt_row["state"]) != "cancelled"
                or str(iteration_row["state"]) != "cancelled"
                or str(lease_row["state"]) != "released"
                or str(receipt_row["status"]) != "cancelled"
            ):
                raise ControlRepositoryError(
                    "published Agent recovery lifecycle evidence is inconsistent"
                )
            if str(attempt_row["session_id"] or "") != turn_id:
                raise ControlRepositoryError(
                    "published Agent recovery turn identity collision"
                )
            receipt = json.loads(str(receipt_row["receipt_json"]))
            completion = receipt.get("completion", {})
            if (
                not isinstance(completion, Mapping)
                or completion.get("failure_class") != "candidate-superseded"
                or completion.get("delivery_publish_state") != "not-published"
            ):
                raise ControlRepositoryError(
                    "published Agent recovery is limited to the legacy restaging error"
                )
            conflict = conn.execute(
                "SELECT action_id FROM agent_actions_v4 WHERE operator_id=? AND role=? "
                "AND state IN ('queued','claimed','running','uncertain','retry-pending') "
                "AND action_id<>? LIMIT 1",
                (action_row["operator_id"], action_row["role"], action_id),
            ).fetchone()
            if conflict is not None:
                raise ControlRepositoryError(
                    "another Agent action is active for this operator role"
                )
            agent_row = conn.execute(
                "SELECT * FROM agent_registrations_v4 WHERE agent_id=?",
                (action_row["assigned_agent_id"],),
            ).fetchone()
            if agent_row is None:
                raise ControlRepositoryError(
                    "published Agent recovery executor is no longer registered"
                )
            details = json.loads(str(attempt_row["details_json"] or "{}"))
            recovery = {
                "verification": verification,
                "turn_id": turn_id,
                "runner_id": runner_id,
                "prior_failure_class": str(completion.get("failure_class") or ""),
                "prior_receipt_sha256": hashlib.sha256(
                    str(receipt_row["receipt_json"]).encode("utf-8")
                ).hexdigest(),
                "recovered_at": now,
            }
            details["cancelled_published_recovery"] = recovery
            iteration = json.loads(str(iteration_row["iteration_json"] or "{}"))
            iteration.update(
                {
                    "state": "running",
                    "agent_id": str(action_row["assigned_agent_id"]),
                    "updated_at": now,
                }
            )
            lease = json.loads(str(lease_row["lease_json"] or "{}"))
            lease.update(
                {
                    "state": "active",
                    "heartbeat_at": now,
                    "expires_at": expires_at,
                }
            )
            conn.execute(
                "UPDATE agent_actions_v4 SET state='running', claimed_by=?, "
                "updated_at=? WHERE action_id=?",
                (runner_id, now, action_id),
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET state='running', runner_id=?, "
                "completed_at='', heartbeat_at=?, details_json=?, updated_at=? "
                "WHERE attempt_id=?",
                (runner_id, now, _canonical_json(details), now, attempt_id),
            )
            conn.execute(
                "UPDATE agent_iterations_v4 SET state='running', agent_id=?, "
                "iteration_json=?, updated_at=? WHERE iteration_id=?",
                (
                    action_row["assigned_agent_id"],
                    _canonical_json(iteration),
                    now,
                    action_row["iteration_id"],
                ),
            )
            conn.execute(
                "UPDATE agent_work_leases_v4 SET state='active', runner_id=?, "
                "released_at='', heartbeat_at=?, expires_at=?, lease_json=? "
                "WHERE lease_id=?",
                (
                    runner_id,
                    now,
                    expires_at,
                    _canonical_json(lease),
                    lease_row["lease_id"],
                ),
            )
            conn.execute(
                "DELETE FROM agent_action_receipts_v4 WHERE action_id=?",
                (action_id,),
            )
            self._event(
                conn,
                "agent-action-cancelled-published-recovered",
                "agent-action",
                action_id,
                recovery,
            )
            return {
                "action": json.loads(str(action_row["action_json"])),
                "context": self._context_in_connection(
                    conn, str(action_row["iteration_id"])
                ),
                "agent": _decode_agent(agent_row),
                "attempt_id": attempt_id,
                "session_id": turn_id,
                "lease": lease,
                "boot_id": str(agent_row["boot_id"]),
            }

    def recover_discarded_agent_turn_outcome(
        self,
        *,
        action_id: str,
        source_attempt_id: str,
        turn_id: str,
        completion_digest: str,
        verification: str,
        runner_id: str,
        lease_seconds: int = 30,
    ) -> dict[str, Any]:
        """Open a receipt-reconciliation attempt for one proven adapter omission."""

        if verification != "exact-terminal-turn-structured-result":
            raise ControlRepositoryError(
                "Agent outcome recovery requires exact terminal-turn verification"
            )
        if not turn_id.strip():
            raise ControlRepositoryError("Agent outcome recovery requires turn_id")
        if len(completion_digest) != 64 or any(
            char not in "0123456789abcdef" for char in completion_digest.lower()
        ):
            raise ControlRepositoryError(
                "Agent outcome recovery requires a SHA-256 completion digest"
            )
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT a.*, t.ordinal, t.state AS attempt_state, t.session_id, "
                "t.details_json, l.state AS lease_state, l.lease_json, "
                "l.acquired_at, l.lease_id, i.state AS iteration_state, "
                "i.iteration_json, r.status AS receipt_status, r.receipt_json "
                "FROM agent_actions_v4 a JOIN agent_action_attempts_v4 t "
                "ON t.attempt_id=a.current_attempt_id "
                "JOIN agent_work_leases_v4 l ON l.lease_id=a.current_lease_id "
                "JOIN agent_iterations_v4 i ON i.iteration_id=a.iteration_id "
                "JOIN agent_action_receipts_v4 r ON r.action_id=a.action_id "
                "WHERE a.action_id=?",
                (action_id,),
            ).fetchone()
            if row is None:
                raise ControlRepositoryError(
                    "Agent outcome recovery evidence is incomplete"
                )
            if str(row["current_attempt_id"]) != source_attempt_id:
                raise ControlRepositoryError(
                    "Agent outcome recovery attempt identity changed"
                )
            if not self._workflow_agent_gate_is_current(conn, row):
                raise ControlRepositoryError(
                    "Agent outcome recovery rejected: workflow gate is obsolete"
                )
            if (
                str(row["state"]) != "failed"
                or str(row["attempt_state"]) != "failed"
                or str(row["lease_state"]) != "released"
                or str(row["iteration_state"]) != "failed"
                or str(row["receipt_status"]) != "failed"
                or str(row["session_id"] or "") != turn_id
            ):
                raise ControlRepositoryError(
                    "Agent outcome recovery lifecycle evidence is inconsistent"
                )
            receipt = json.loads(str(row["receipt_json"]))
            completion = receipt.get("completion", {})
            if (
                not isinstance(completion, Mapping)
                or completion.get("failure_class")
                != "agent-output-validation-retry-exhausted"
                or completion.get("validation_error")
                != "completed Agent action produced no durable output or typed outcome"
                or completion.get("adapter_id") != "codex-ide-task-adapter"
                or completion.get("summary") != "exact_codex_ide_turn_completed"
                or str(completion.get("session_id") or "") != turn_id
            ):
                raise ControlRepositoryError(
                    "Agent outcome recovery is limited to the discarded structured-result defect"
                )
            conflict = conn.execute(
                "SELECT action_id FROM agent_actions_v4 WHERE operator_id=? AND role=? "
                "AND state IN ('queued','claimed','running','uncertain','retry-pending') "
                "AND action_id<>? LIMIT 1",
                (row["operator_id"], row["role"], action_id),
            ).fetchone()
            if conflict is not None:
                raise ControlRepositoryError(
                    "another Agent action is active for this operator role"
                )
            agent_row = conn.execute(
                "SELECT * FROM agent_registrations_v4 WHERE agent_id=?",
                (row["assigned_agent_id"],),
            ).fetchone()
            if agent_row is None:
                raise ControlRepositoryError(
                    "Agent outcome recovery executor is no longer registered"
                )

            attempt_id = f"aat-{uuid.uuid4().hex}"
            lease_token = secrets.token_urlsafe(32)
            prior_receipt_sha256 = hashlib.sha256(
                str(row["receipt_json"]).encode("utf-8")
            ).hexdigest()
            recovery = {
                "schema": "ascendop.agent-receipt-reconciliation.v1",
                "verification": verification,
                "source_attempt_id": source_attempt_id,
                "source_attempt_ordinal": int(row["ordinal"]),
                "turn_id": turn_id,
                "completion_digest": completion_digest.lower(),
                "prior_receipt_sha256": prior_receipt_sha256,
                "runner_id": runner_id,
                "recovered_at": now,
            }
            lease = json.loads(str(row["lease_json"] or "{}"))
            lease.update(
                {
                    "lease_token": lease_token,
                    "state": "active",
                    "heartbeat_at": now,
                    "expires_at": expires_at,
                }
            )
            iteration = json.loads(str(row["iteration_json"] or "{}"))
            iteration.update(
                {
                    "state": "running",
                    "agent_id": str(row["assigned_agent_id"]),
                    "updated_at": now,
                }
            )
            conn.execute(
                "INSERT INTO agent_action_attempts_v4(attempt_id, action_id, "
                "ordinal, agent_id, runner_id, state, session_id, started_at, "
                "heartbeat_at, details_json, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)",
                (
                    attempt_id,
                    action_id,
                    int(row["ordinal"]) + 1,
                    row["assigned_agent_id"],
                    runner_id,
                    turn_id,
                    now,
                    now,
                    _canonical_json({"receipt_reconciliation": recovery}),
                    now,
                    now,
                ),
            )
            conn.execute(
                "UPDATE agent_actions_v4 SET state='running', claimed_by=?, "
                "current_attempt_id=?, updated_at=? WHERE action_id=?",
                (runner_id, attempt_id, now, action_id),
            )
            conn.execute(
                "UPDATE agent_iterations_v4 SET state='running', agent_id=?, "
                "iteration_json=?, updated_at=? WHERE iteration_id=?",
                (
                    row["assigned_agent_id"],
                    _canonical_json(iteration),
                    now,
                    row["iteration_id"],
                ),
            )
            conn.execute(
                "UPDATE agent_work_leases_v4 SET lease_token=?, state='active', "
                "runner_id=?, released_at='', heartbeat_at=?, expires_at=?, "
                "lease_json=? WHERE lease_id=?",
                (
                    lease_token,
                    runner_id,
                    now,
                    expires_at,
                    _canonical_json(lease),
                    row["lease_id"],
                ),
            )
            conn.execute(
                "DELETE FROM agent_action_receipts_v4 WHERE action_id=?",
                (action_id,),
            )
            self._event(
                conn,
                "agent-action-structured-result-reconciled",
                "agent-action",
                action_id,
                recovery,
            )
            return {
                "action": json.loads(str(row["action_json"])),
                "context": self._context_in_connection(
                    conn, str(row["iteration_id"])
                ),
                "agent": _decode_agent(agent_row),
                "attempt_id": attempt_id,
                "session_id": turn_id,
                "lease": lease,
                "boot_id": str(agent_row["boot_id"]),
                "recovery": recovery,
            }

    @contextmanager
    def agent_action_promotion_guard(
        self,
        action_id: str,
        *,
        allow_obsolete_completed_tester_case: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Fence canonical promotion against a concurrent workflow-head change."""

        with self.transaction() as conn:
            action_row = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action_row is None:
                raise ControlRepositoryError(f"unknown agent action: {action_id}")
            gate_is_current = self._workflow_agent_gate_is_current(conn, action_row)
            if (
                not gate_is_current
                and not (
                    allow_obsolete_completed_tester_case
                    and str(action_row["role"]) == "tester"
                    and str(action_row["state"]) == "completed"
                )
            ):
                raise ControlRepositoryError(
                    "agent output promotion rejected: workflow gate is obsolete"
                )
            if str(action_row["state"]) != "completed":
                raise ControlRepositoryError(
                    "agent output promotion requires a completed action"
                )
            self._event(
                conn,
                "agent-action-promotion-started",
                "agent-action",
                action_id,
                {
                    "board_revision": json.loads(str(action_row["action_json"]))[
                        "board_revision"
                    ],
                    "historical_tester_case": not gate_is_current,
                },
            )
            yield self._agent_action_in_connection(conn, action_id)
            self._event(
                conn,
                "agent-action-promotion-committed",
                "agent-action",
                action_id,
                {},
            )

    @contextmanager
    def obsolete_agent_output_recovery_guard(
        self,
        action_id: str,
    ) -> Iterator[dict[str, Any]]:
        """Fence an evidence-backed rollback of one obsolete promotion."""

        with self.transaction() as conn:
            action_row = conn.execute(
                "SELECT * FROM agent_actions_v4 WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action_row is None:
                raise ControlRepositoryError(f"unknown agent action: {action_id}")
            if self._workflow_agent_gate_is_current(conn, action_row):
                raise ControlRepositoryError(
                    "current workflow Agent output cannot be recovered as obsolete"
                )
            if str(action_row["state"]) not in {"completed", "cancelled"}:
                raise ControlRepositoryError(
                    "obsolete output recovery requires a terminal Agent action"
                )
            self._event(
                conn,
                "agent-output-obsolete-recovery-started",
                "agent-action",
                action_id,
                {},
            )
            yield self._agent_action_in_connection(conn, action_id)
            self._event(
                conn,
                "agent-output-obsolete-recovery-committed",
                "agent-action",
                action_id,
                {},
            )

    @staticmethod
    def _workflow_agent_gate_is_current(
        conn: sqlite3.Connection,
        action_row: sqlite3.Row,
    ) -> bool:
        action = json.loads(str(action_row["action_json"]))
        identity = action.get("candidate_identity", {})
        if not isinstance(identity, Mapping) or identity.get("origin") != "workflow-gate":
            return True
        row = conn.execute(
            "SELECT state_json FROM scheduler_state WHERE scheduler_id=?",
            ("agent-gate-heads-v4",),
        ).fetchone()
        if row is None:
            return False
        state = json.loads(str(row["state_json"]))
        heads = state.get("heads", {}) if isinstance(state, Mapping) else {}
        operator_heads = (
            heads.get(str(action_row["operator_id"]), {})
            if isinstance(heads, Mapping)
            else {}
        )
        return (
            isinstance(operator_heads, Mapping)
            and operator_heads.get(str(action_row["role"])) == action_row["action_id"]
        )

    @staticmethod
    def _expire_agent_leases(conn: sqlite3.Connection, now: str) -> None:
        rows = conn.execute(
            "SELECT * FROM agent_work_leases_v4 WHERE state='active' AND expires_at<=?",
            (now,),
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE agent_work_leases_v4 SET state='expired', released_at=? "
                "WHERE lease_id=?",
                (now, row["lease_id"]),
            )
            conn.execute(
                "UPDATE agent_actions_v4 SET state='uncertain', updated_at=? "
                "WHERE action_id=? AND state IN ('claimed', 'running')",
                (now, row["action_id"]),
            )
            conn.execute(
                "UPDATE agent_action_attempts_v4 SET state='uncertain', updated_at=? "
                "WHERE action_id=? AND state IN ('claimed', 'running')",
                (now, row["action_id"]),
            )
            conn.execute(
                "UPDATE agent_iterations_v4 SET state='uncertain', updated_at=? "
                "WHERE iteration_id=?",
                (now, row["iteration_id"]),
            )

    @staticmethod
    def _select_agent(
        conn: sqlite3.Connection,
        action_row: sqlite3.Row,
        now: str,
        *,
        required_agent_id: str = "",
    ) -> dict[str, Any] | None:
        action = json.loads(str(action_row["action_json"]))
        pool_id = str(action.get("agent_pool_id") or "")
        if not pool_id:
            return None
        pool = conn.execute(
            "SELECT * FROM agent_pools_v4 WHERE pool_id=? AND enabled=1 "
            "AND source_present=1",
            (pool_id,),
        ).fetchone()
        if pool is None:
            return None
        roles = set(json.loads(str(pool["roles_json"])))
        if str(action_row["role"]) not in roles:
            return None
        drivers = list(json.loads(str(pool["drivers_json"])))
        required_capabilities = dict(
            json.loads(str(pool["required_capabilities_json"]))
        )
        preferred = required_agent_id or str(action_row["preferred_agent_id"] or "")
        binding_rows = conn.execute(
            "SELECT agent_id, enabled, priority FROM agent_role_bindings_v4 "
            "WHERE operator_id=? AND role=?",
            (action_row["operator_id"], action_row["role"]),
        ).fetchall()
        explicit = {
            str(row["agent_id"]): int(row["priority"])
            for row in binding_rows
            if bool(row["enabled"])
        }
        restrict_to_bindings = bool(binding_rows)
        selected_at = {
            str(row["agent_id"]): str(row["last_selected_at"])
            for row in conn.execute(
                "SELECT agent_id, last_selected_at FROM agent_pool_selections_v4 "
                "WHERE pool_id=?",
                (pool_id,),
            ).fetchall()
        }
        candidates: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        for row in conn.execute(
            "SELECT * FROM agent_registrations_v4 WHERE health_state='ready' "
            "AND lease_expires_at>?",
            (now,),
        ).fetchall():
            agent = _decode_agent(row)
            if agent["driver"] not in drivers:
                continue
            if preferred and agent["agent_id"] != preferred:
                continue
            if restrict_to_bindings and agent["agent_id"] not in explicit:
                continue
            capabilities = agent["capabilities"]
            if any(
                capabilities.get(key) is not expected
                for key, expected in required_capabilities.items()
            ):
                continue
            last = selected_at.get(agent["agent_id"], "")
            key = (
                explicit.get(agent["agent_id"], int(pool["priority"])),
                0 if not last else 1,
                last,
                agent["agent_id"],
            )
            candidates.append((key, agent))
        if not candidates:
            return None
        agent = min(candidates, key=lambda item: item[0])[1]
        agent["_selection_pool_id"] = pool_id
        return agent

    def create_evidence_operation_request(
        self, request: Mapping[str, Any]
    ) -> dict[str, Any]:
        value = validate_evidence_operation_request(request)
        if value["state"] != "queued":
            raise ControlRepositoryError(
                "new evidence operation request must be queued"
            )
        definition = evidence_operation_definition(str(value["operation_code"]))
        origin = dict(value["origin"])
        now = _utc_now()
        payload = _canonical_json(value)
        with self.transaction() as conn:
            action = conn.execute(
                "SELECT iteration_id, operator_id, role FROM agent_actions_v4 "
                "WHERE action_id=?",
                (origin["action_id"],),
            ).fetchone()
            if action is None:
                raise ControlRepositoryError(
                    "evidence operation origin action does not exist"
                )
            expected = (
                str(action["iteration_id"]),
                str(action["operator_id"]),
                str(action["role"]),
            )
            observed = (
                str(origin["iteration_id"]),
                str(origin["operator_id"]),
                str(origin["role"]),
            )
            if observed != expected:
                raise ControlRepositoryError(
                    f"evidence operation origin identity mismatch: {observed} != {expected}"
                )
            existing = conn.execute(
                "SELECT operation_request_id, request_json FROM "
                "evidence_operation_requests_v5 WHERE idempotency_key=?",
                (value["idempotency_key"],),
            ).fetchone()
            if existing is not None:
                previous = json.loads(str(existing["request_json"]))
                if _without_created_at(previous) != _without_created_at(value):
                    raise ControlRepositoryError(
                        "evidence operation idempotency collision with different payload"
                    )
                return self._evidence_request_in_connection(
                    conn, str(existing["operation_request_id"])
                )
            conn.execute(
                "INSERT INTO evidence_operation_requests_v5("
                "operation_request_id, idempotency_key, registry_generation, "
                "registry_digest, operation_code, origin_action_id, "
                "origin_iteration_id, operator_id, origin_role, expected_consumer, "
                "state, executor, resource_class, request_json, created_at, updated_at"
                ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)",
                (
                    value["operation_request_id"],
                    value["idempotency_key"],
                    value["registry_generation"],
                    value["registry_digest"],
                    value["operation_code"],
                    origin["action_id"],
                    origin["iteration_id"],
                    origin["operator_id"],
                    origin["role"],
                    value["expected_consumer"],
                    definition["executor"],
                    definition["resource_class"],
                    payload,
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "evidence-operation-requested",
                "evidence-operation-request",
                str(value["operation_request_id"]),
                {
                    "operation_code": value["operation_code"],
                    "origin_action_id": origin["action_id"],
                    "origin_iteration_id": origin["iteration_id"],
                    "operator_id": origin["operator_id"],
                    "expected_consumer": value["expected_consumer"],
                },
            )
            return self._evidence_request_in_connection(
                conn, str(value["operation_request_id"])
            )

    def evidence_operation_request(
        self, operation_request_id: str
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT operation_request_id FROM evidence_operation_requests_v5 "
                "WHERE operation_request_id=?",
                (operation_request_id,),
            ).fetchone()
            if row is None:
                return None
            return self._evidence_request_in_connection(conn, operation_request_id)

    def claim_evidence_operation(
        self,
        *,
        executor: str,
        consumer_id: str,
        lease_seconds: int = 300,
        operation_codes: set[str] | None = None,
    ) -> dict[str, Any] | None:
        if not str(executor).strip() or not str(consumer_id).strip():
            raise ControlRepositoryError(
                "evidence operation executor and consumer are required"
            )
        now = _utc_now()
        expires_at = _future(now, lease_seconds)
        with self.transaction() as conn:
            expired = conn.execute(
                "SELECT operation_request_id, request_json FROM "
                "evidence_operation_requests_v5 WHERE state='claimed' "
                "AND claim_expires_at != '' AND claim_expires_at <= ?",
                (now,),
            ).fetchall()
            for row in expired:
                payload = json.loads(str(row["request_json"]))
                payload["state"] = "queued"
                conn.execute(
                    "UPDATE evidence_operation_requests_v5 SET state='queued', "
                    "request_json=?, claimed_by='', claim_token='', "
                    "claim_expires_at='', updated_at=? WHERE operation_request_id=?",
                    (
                        _canonical_json(payload),
                        now,
                        str(row["operation_request_id"]),
                    ),
                )
            params: list[Any] = [executor]
            query = (
                "SELECT operation_request_id FROM evidence_operation_requests_v5 "
                "WHERE executor=? AND state='queued'"
            )
            if operation_codes is not None:
                codes = sorted(str(value) for value in operation_codes)
                if not codes:
                    return None
                query += " AND operation_code IN (" + ",".join("?" for _ in codes) + ")"
                params.extend(codes)
            query += " ORDER BY created_at, operation_request_id LIMIT 1"
            row = conn.execute(query, params).fetchone()
            if row is None:
                return None
            request_id = str(row["operation_request_id"])
            token = secrets.token_hex(24)
            current = self._evidence_request_in_connection(conn, request_id)
            payload = dict(current["request"])
            payload["state"] = "claimed"
            conn.execute(
                "UPDATE evidence_operation_requests_v5 SET state='claimed', "
                "request_json=?, claimed_by=?, claim_token=?, claim_expires_at=?, "
                "claim_attempts=claim_attempts+1, updated_at=? "
                "WHERE operation_request_id=? AND state='queued'",
                (
                    _canonical_json(payload),
                    consumer_id,
                    token,
                    expires_at,
                    now,
                    request_id,
                ),
            )
            self._event(
                conn,
                "evidence-operation-claimed",
                "evidence-operation-request",
                request_id,
                {"executor": executor, "consumer_id": consumer_id},
            )
            return self._evidence_request_in_connection(conn, request_id)

    def route_evidence_operation(
        self,
        *,
        operation_request_id: str,
        claim_token: str,
        test_request_id: str = "",
        wire_attempt_id: str = "",
        endpoint_id: str = "",
        execution_environment_id: str = "",
    ) -> dict[str, Any]:
        now = _utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM evidence_operation_requests_v5 "
                "WHERE operation_request_id=?",
                (operation_request_id,),
            ).fetchone()
            if row is None:
                raise ControlRepositoryError("evidence operation request does not exist")
            state = str(row["state"])
            route = (
                str(row["test_request_id"]),
                str(row["wire_attempt_id"]),
                str(row["endpoint_id"]),
                str(row["execution_environment_id"]),
            )
            requested_route = (
                str(test_request_id),
                str(wire_attempt_id),
                str(endpoint_id),
                str(execution_environment_id),
            )
            if state in {"routed", "running", "completed", "failed", "cancelled"}:
                if route != requested_route:
                    raise ControlRepositoryError(
                        "evidence operation route identity changed"
                    )
                return self._evidence_request_in_connection(
                    conn, operation_request_id
                )
            if state != "claimed" or str(row["claim_token"]) != claim_token:
                raise ControlRepositoryError(
                    "evidence operation route requires the active claim token"
                )
            if str(row["claim_expires_at"]) <= now:
                raise ControlRepositoryError("evidence operation claim has expired")
            if bool(test_request_id) != bool(wire_attempt_id):
                raise ControlRepositoryError(
                    "evidence operation route requires both test request and attempt"
                )
            payload = json.loads(str(row["request_json"]))
            payload["state"] = "routed"
            conn.execute(
                "UPDATE evidence_operation_requests_v5 SET state='routed', "
                "request_json=?, test_request_id=?, wire_attempt_id=?, endpoint_id=?, "
                "execution_environment_id=?, claimed_by='', claim_token='', "
                "claim_expires_at='', updated_at=? WHERE operation_request_id=?",
                (
                    _canonical_json(payload),
                    test_request_id,
                    wire_attempt_id,
                    endpoint_id,
                    execution_environment_id,
                    now,
                    operation_request_id,
                ),
            )
            self._event(
                conn,
                "evidence-operation-routed",
                "evidence-operation-request",
                operation_request_id,
                {
                    "test_request_id": test_request_id,
                    "wire_attempt_id": wire_attempt_id,
                    "endpoint_id": endpoint_id,
                    "execution_environment_id": execution_environment_id,
                },
            )
            return self._evidence_request_in_connection(conn, operation_request_id)

    def defer_evidence_operation(
        self,
        *,
        operation_request_id: str,
        claim_token: str,
        delay_seconds: int,
        failure_class: str,
    ) -> dict[str, Any]:
        if delay_seconds < 0:
            raise ControlRepositoryError(
                "evidence operation defer delay cannot be negative"
            )
        if not str(failure_class).strip():
            raise ControlRepositoryError(
                "evidence operation defer requires a failure class"
            )
        now = _utc_now()
        retry_at = now if delay_seconds == 0 else _future(now, delay_seconds)
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state, claim_token FROM evidence_operation_requests_v5 "
                "WHERE operation_request_id=?",
                (operation_request_id,),
            ).fetchone()
            if row is None:
                raise ControlRepositoryError("evidence operation request does not exist")
            if str(row["state"]) != "claimed" or str(row["claim_token"]) != claim_token:
                raise ControlRepositoryError(
                    "evidence operation defer requires the active claim token"
                )
            conn.execute(
                "UPDATE evidence_operation_requests_v5 SET claimed_by='', "
                "claim_token='', claim_expires_at=?, updated_at=? "
                "WHERE operation_request_id=?",
                (retry_at, now, operation_request_id),
            )
            self._event(
                conn,
                "evidence-operation-deferred",
                "evidence-operation-request",
                operation_request_id,
                {
                    "failure_class": failure_class,
                    "retry_at": retry_at,
                },
            )
            return self._evidence_request_in_connection(conn, operation_request_id)

    def complete_evidence_operation(
        self, result: Mapping[str, Any]
    ) -> dict[str, Any]:
        value = validate_evidence_operation_result(result)
        request_id = str(value["operation_request_id"])
        now = _utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM evidence_operation_requests_v5 "
                "WHERE operation_request_id=?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise ControlRepositoryError("evidence operation request does not exist")
            request = json.loads(str(row["request_json"]))
            for field in (
                "registry_generation",
                "registry_digest",
                "operation_code",
                "expected_consumer",
                "origin",
            ):
                if value[field] != request[field]:
                    raise ControlRepositoryError(
                        f"evidence result changed request identity: {field}"
                    )
            expected_execution = {
                "test_request_id": str(row["test_request_id"]),
                "wire_attempt_id": str(row["wire_attempt_id"]),
                "endpoint_id": str(row["endpoint_id"]),
                "execution_environment_id": str(row["execution_environment_id"]),
            }
            if dict(value["execution"]) != expected_execution:
                raise ControlRepositoryError(
                    "evidence result execution identity does not match its route"
                )
            existing = conn.execute(
                "SELECT result_json FROM evidence_operation_results_v5 "
                "WHERE operation_request_id=?",
                (request_id,),
            ).fetchone()
            result_json = _canonical_json(value)
            if existing is not None:
                previous = json.loads(str(existing["result_json"]))
                if previous != value:
                    raise ControlRepositoryError(
                        "evidence operation already has a different terminal result"
                    )
                return previous
            if str(row["state"]) not in {"claimed", "routed", "running"}:
                raise ControlRepositoryError(
                    f"evidence operation cannot complete from {row['state']}"
                )
            conn.execute(
                "INSERT INTO evidence_operation_results_v5("
                "operation_result_id, operation_request_id, status, "
                "expected_consumer, result_json, completed_at) VALUES(?, ?, ?, ?, ?, ?)",
                (
                    value["operation_result_id"],
                    request_id,
                    value["status"],
                    value["expected_consumer"],
                    result_json,
                    value["completed_at"],
                ),
            )
            terminal_state = str(value["status"])
            request["state"] = terminal_state
            conn.execute(
                "UPDATE evidence_operation_requests_v5 SET state=?, request_json=?, "
                "claimed_by='', claim_token='', claim_expires_at='', updated_at=? "
                "WHERE operation_request_id=?",
                (
                    terminal_state,
                    _canonical_json(request),
                    now,
                    request_id,
                ),
            )
            self._event(
                conn,
                "evidence-operation-completed",
                "evidence-operation-request",
                request_id,
                {
                    "operation_result_id": value["operation_result_id"],
                    "status": terminal_state,
                    "expected_consumer": value["expected_consumer"],
                    "origin_action_id": request["origin"]["action_id"],
                    "origin_iteration_id": request["origin"]["iteration_id"],
                },
            )
            return dict(value)

    def evidence_operation_result(
        self, operation_request_id: str
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT result_json FROM evidence_operation_results_v5 "
                "WHERE operation_request_id=?",
                (operation_request_id,),
            ).fetchone()
            return json.loads(str(row["result_json"])) if row is not None else None

    def evidence_operation_for_test_request(
        self, test_request_id: str
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT operation_request_id FROM evidence_operation_requests_v5 "
                "WHERE test_request_id=?",
                (test_request_id,),
            ).fetchone()
            if row is None:
                return None
            return self._evidence_request_in_connection(
                conn, str(row["operation_request_id"])
            )

    def evidence_operations_for_origin(
        self, *, action_id: str = "", iteration_id: str = ""
    ) -> list[dict[str, Any]]:
        if not action_id and not iteration_id:
            raise ControlRepositoryError(
                "evidence origin query requires action_id or iteration_id"
            )
        conditions: list[str] = []
        params: list[str] = []
        if action_id:
            conditions.append("origin_action_id=?")
            params.append(action_id)
        if iteration_id:
            conditions.append("origin_iteration_id=?")
            params.append(iteration_id)
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT operation_request_id FROM evidence_operation_requests_v5 "
                "WHERE " + " AND ".join(conditions) +
                " ORDER BY created_at, operation_request_id",
                params,
            ).fetchall()
            return [
                self._evidence_request_in_connection(
                    conn, str(row["operation_request_id"])
                )
                for row in rows
            ]

    def recent_evidence_operations(
        self,
        *,
        operator_id: str,
        expected_consumer: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        if not str(operator_id).strip() or not str(expected_consumer).strip():
            raise ControlRepositoryError(
                "recent evidence query requires operator and consumer"
            )
        if not 1 <= int(limit) <= 200:
            raise ControlRepositoryError("recent evidence limit must be in [1, 200]")
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT operation_request_id FROM evidence_operation_requests_v5 "
                "WHERE operator_id=? AND expected_consumer=? "
                "ORDER BY created_at DESC, operation_request_id DESC LIMIT ?",
                (operator_id, expected_consumer, int(limit)),
            ).fetchall()
            return [
                {
                    **self._evidence_request_in_connection(
                        conn, str(row["operation_request_id"])
                    ),
                    "result": (
                        json.loads(str(result["result_json"]))
                        if (
                            result := conn.execute(
                                "SELECT result_json FROM evidence_operation_results_v5 "
                                "WHERE operation_request_id=?",
                                (str(row["operation_request_id"]),),
                            ).fetchone()
                        )
                        is not None
                        else None
                    ),
                }
                for row in rows
            ]

    @staticmethod
    def _evidence_request_in_connection(
        conn: sqlite3.Connection, operation_request_id: str
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM evidence_operation_requests_v5 "
            "WHERE operation_request_id=?",
            (operation_request_id,),
        ).fetchone()
        if row is None:
            raise ControlRepositoryError("evidence operation request does not exist")
        request = json.loads(str(row["request_json"]))
        return {
            "operation_request_id": str(row["operation_request_id"]),
            "operation_code": str(row["operation_code"]),
            "operator_id": str(row["operator_id"]),
            "origin_action_id": str(row["origin_action_id"]),
            "origin_iteration_id": str(row["origin_iteration_id"]),
            "origin_role": str(row["origin_role"]),
            "expected_consumer": str(row["expected_consumer"]),
            "executor": str(row["executor"]),
            "resource_class": str(row["resource_class"]),
            "state": str(row["state"]),
            "request": request,
            "route": {
                "test_request_id": str(row["test_request_id"]),
                "wire_attempt_id": str(row["wire_attempt_id"]),
                "endpoint_id": str(row["endpoint_id"]),
                "execution_environment_id": str(
                    row["execution_environment_id"]
                ),
            },
            "claim": {
                "claimed_by": str(row["claimed_by"]),
                "claim_token": str(row["claim_token"]),
                "claim_expires_at": str(row["claim_expires_at"]),
                "attempts": int(row["claim_attempts"]),
            },
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _require_active_lease(
        conn: sqlite3.Connection,
        action_id: str,
        lease_token: str,
        now: str,
    ) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM agent_work_leases_v4 WHERE action_id=? AND "
            "lease_token=? AND state='active'",
            (action_id, lease_token),
        ).fetchone()
        if row is None:
            raise ControlRepositoryError("active agent work lease not found")
        if str(row["expires_at"]) <= now:
            raise ControlRepositoryError("agent work lease has expired")
        return row

    @staticmethod
    def _context_in_connection(
        conn: sqlite3.Connection, iteration_id: str
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT snapshot_json FROM agent_context_snapshots_v4 WHERE iteration_id=?",
            (iteration_id,),
        ).fetchone()
        if row is None:
            raise ControlRepositoryError("agent context snapshot not found")
        return json.loads(str(row["snapshot_json"]))

    @staticmethod
    def _attempt_context_in_connection(
        conn: sqlite3.Connection,
        *,
        action_id: str,
        attempt_id: str,
        ordinal: int,
    ) -> dict[str, Any]:
        rows = conn.execute(
            "SELECT attempt_id, ordinal, state, details_json "
            "FROM agent_action_attempts_v4 WHERE action_id=? AND ordinal<? "
            "ORDER BY ordinal",
            (action_id, ordinal),
        ).fetchall()
        history: list[dict[str, Any]] = []
        for row in rows:
            try:
                details = json.loads(str(row["details_json"] or "{}"))
            except json.JSONDecodeError:
                details = {}
            failure_class = str(details.get("failure_class") or "").strip()
            validation_error = str(details.get("validation_error") or "").strip()
            history.append(
                {
                    "attempt_id": str(row["attempt_id"]),
                    "ordinal": int(row["ordinal"]),
                    "state": str(row["state"]),
                    "failure_class": failure_class or None,
                    "validation_error": validation_error or None,
                }
            )
        prior = history[-1] if history else None
        output_repair: dict[str, Any] | None = None
        if (
            prior is not None
            and prior["failure_class"] == "agent-output-validation"
            and prior["validation_error"]
        ):
            output_repair = {
                "prior_attempt_id": prior["attempt_id"],
                "prior_attempt_ordinal": prior["ordinal"],
                "failure_class": prior["failure_class"],
                "validation_error": prior["validation_error"],
                "remaining_correction_turns": 1,
            }
        mode = (
            "output_repair"
            if output_repair is not None
            else ("execution_retry" if history else "initial")
        )
        return {
            "attempt_id": attempt_id,
            "ordinal": ordinal,
            "mode": mode,
            "history": history,
            "output_repair": output_repair,
        }

    @staticmethod
    def _agent_action_in_connection(
        conn: sqlite3.Connection, action_id: str
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM agent_actions_v4 WHERE action_id=?",
            (action_id,),
        ).fetchone()
        if row is None:
            raise ControlRepositoryError(f"agent action does not exist: {action_id}")
        return {
            "action_id": str(row["action_id"]),
            "iteration_id": str(row["iteration_id"]),
            "operator_id": str(row["operator_id"]),
            "role": str(row["role"]),
            "assigned_agent_id": str(row["assigned_agent_id"]),
            "state": str(row["state"]),
            "action": json.loads(str(row["action_json"])),
            "current_attempt_id": str(row["current_attempt_id"]),
            "current_lease_id": str(row["current_lease_id"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _candidate_version(conn: sqlite3.Connection, action_id: str) -> str:
        row = conn.execute(
            "SELECT i.candidate_version FROM agent_iterations_v4 i JOIN "
            "agent_actions_v4 a ON a.iteration_id=i.iteration_id WHERE a.action_id=?",
            (action_id,),
        ).fetchone()
        return str(row[0]) if row else ""

    @staticmethod
    def _iteration_created_at(conn: sqlite3.Connection, iteration_id: str) -> str:
        row = conn.execute(
            "SELECT created_at FROM agent_iterations_v4 WHERE iteration_id=?",
            (iteration_id,),
        ).fetchone()
        return str(row[0]) if row else _utc_now()


class V4ControlRepository(_AgentRepository, ManagementRepository, ServiceRepository):
    pass


def _decode_agent(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "agent_id": str(row["agent_id"]),
        "driver": str(row["driver"]),
        "executable": str(row["executable"]),
        "executable_digest": str(row["executable_digest"]),
        "observed_version": str(row["observed_version"]),
        "registration_generation": str(row["registration_generation"]),
        "capabilities": json.loads(str(row["capabilities_json"])),
        "health_state": str(row["health_state"]),
        "boot_id": str(row["boot_id"]),
        "manager_runner_id": str(row["manager_runner_id"]),
        "heartbeat_at": str(row["heartbeat_at"]),
        "lease_expires_at": str(row["lease_expires_at"]),
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _base_agent_failure_class(failure_class: str) -> str:
    suffix = "-retry-exhausted"
    return (
        failure_class[: -len(suffix)]
        if failure_class.endswith(suffix)
        else failure_class
    )


def _normalize_legacy_agent_retry_failure(
    details: Mapping[str, Any], *, action_state: str
) -> dict[str, Any] | None:
    if action_state != "failed":
        return None
    normalized = dict(details)
    if (
        normalized.get("status") == "failed"
        and normalized.get("changed_paths") == []
        and normalized.get("out_of_scope_paths") == []
        and not normalized.get("session_id")
        and not normalized.get("summary")
    ):
        normalized["failure_class"] = "agent-adapter"
        normalized["legacy_classification"] = True
        return normalized
    if (
        normalized.get("failure_class") == "protocol"
        and normalized.get("adapter_id") == "codex-ide-task-adapter"
        and isinstance(normalized.get("validation_error"), str)
        and str(normalized["validation_error"]).strip()
        and isinstance(normalized.get("session_id"), str)
        and str(normalized["session_id"]).strip()
    ):
        normalized["failure_class"] = "agent-output-validation"
        normalized["legacy_classification"] = True
        return normalized
    return None


def _without_created_at(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result.pop("created_at", None)
    return result


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _future(now: str, seconds: int) -> str:
    if not 1 <= int(seconds) <= 3600:
        raise ControlRepositoryError("lease seconds must be in [1, 3600]")
    value = datetime.fromisoformat(now)
    return (value + timedelta(seconds=int(seconds))).isoformat()
