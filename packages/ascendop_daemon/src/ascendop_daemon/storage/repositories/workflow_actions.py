from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from ascendop_protocol.workflow import (
    WORKFLOW_ACTION_RECEIPT_SCHEMA,
    validate_workflow_action,
    validate_workflow_action_receipt,
)

from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.storage.control_validation import canonical_json, utc_now


SEALED_AGENT_PROMOTION_KINDS = {
    "promote-agent-source",
    "promote-agent-case",
    "promote-agent-output",
}
SEALED_AGENT_PROMOTION_ORIGINS = {
    "agent-source-promotion",
    "agent-case-promotion",
    "agent-output-promotion",
}
OBSOLETE_PROMOTION_CANCELLATION_REASONS = {
    "producer-generation-obsolete",
    "board-revision-obsolete",
    "operator-no-longer-active",
}


class WorkflowActionRepository:
    def create_workflow_action(
        self,
        raw: dict[str, Any],
        *,
        scheduler_id: str = "",
        scheduler_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        action = validate_workflow_action(raw)
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT action_json, state FROM workflow_actions WHERE idempotency_key=?",
                (action["idempotency_key"],),
            ).fetchone()
            if existing is not None:
                stored = json.loads(str(existing[0]))
                stored_identity = dict(stored)
                action_identity = dict(action)
                stored_identity.pop("created_at", None)
                action_identity.pop("created_at", None)
                sealed_promotion = (
                    stored_identity.get("action_kind")
                    == action_identity.get("action_kind")
                    and stored_identity.get("action_kind") in SEALED_AGENT_PROMOTION_KINDS
                )
                if sealed_promotion:
                    # A sealed Agent promotion is release-independent.  On
                    # restart, the current daemon may rediscover it under a
                    # newer producer generation, but the immutable action and
                    # seal identities must still resolve to the original row.
                    stored_identity.pop("producer_generation", None)
                    action_identity.pop("producer_generation", None)
                if stored_identity != action_identity:
                    raise ControlDatabaseError(
                        "workflow action idempotency collision"
                    )
                if sealed_promotion and str(existing[1]) in {"cancelled", "failed"}:
                    receipt_row = conn.execute(
                        "SELECT receipt_json FROM workflow_action_receipts "
                        "WHERE action_id=?",
                        (action["action_id"],),
                    ).fetchone()
                    receipt = (
                        json.loads(str(receipt_row[0]))
                        if receipt_row is not None
                        else {}
                    )
                    reason = str((receipt.get("details") or {}).get("reason") or "")
                    failure_class = str(
                        (receipt.get("details") or {}).get("failure_class") or ""
                    )
                    previous_generation = str(stored.get("producer_generation") or "")
                    recoverable_terminal = (
                        str(existing[1]) == "cancelled"
                        and reason in OBSOLETE_PROMOTION_CANCELLATION_REASONS
                    ) or (
                        str(existing[1]) == "failed"
                        and failure_class == "protocol"
                        and previous_generation != str(action["producer_generation"])
                    )
                    if recoverable_terminal:
                        conn.execute(
                            """
                            UPDATE workflow_actions
                            SET producer_generation=?, state='queued', action_json=?,
                                claimed_by='', claim_token='', claim_expires_at='',
                                started_at='', completed_at='', updated_at=?
                            WHERE idempotency_key=? AND state IN ('cancelled', 'failed')
                            """,
                            (
                                action["producer_generation"],
                                canonical_json(action),
                                now,
                                action["idempotency_key"],
                            ),
                        )
                        conn.execute(
                            "DELETE FROM workflow_action_receipts WHERE action_id=?",
                            (action["action_id"],),
                        )
                        self._event(
                            conn,
                            "workflow_action_requeued_obsolete_promotion",
                            "workflow_action",
                            action["action_id"],
                            {
                                "previous_reason": reason,
                                "previous_failure_class": failure_class,
                                "previous_generation": previous_generation,
                            },
                        )
                        return action
                return stored
            conn.execute(
                """
                INSERT INTO workflow_actions(
                    action_id, idempotency_key, action_kind, campaign, operator_id,
                    test_version, board_revision, producer_generation, state,
                    priority, action_json, claimed_by, claim_token,
                    claim_expires_at, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, '', '', '', ?, ?)
                """,
                (
                    action["action_id"],
                    action["idempotency_key"],
                    action["action_kind"],
                    action["campaign"],
                    action["operator"],
                    action.get("test_version", ""),
                    action["board_revision"],
                    action["producer_generation"],
                    int(action["priority"]),
                    canonical_json(action),
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "workflow_action_queued",
                "workflow_action",
                action["action_id"],
                {"action_kind": action["action_kind"], "operator": action["operator"]},
            )
            if scheduler_id:
                self._write_scheduler_state(
                    conn,
                    scheduler_id,
                    scheduler_state or {},
                    now,
                )
        return action

    def read_scheduler_state(self, scheduler_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT state_json FROM scheduler_state WHERE scheduler_id=?",
                (scheduler_id,),
            ).fetchone()
        return json.loads(str(row[0])) if row is not None else {}

    def write_scheduler_state(
        self,
        scheduler_id: str,
        state: dict[str, Any],
    ) -> None:
        now = utc_now()
        with self.transaction() as conn:
            self._write_scheduler_state(conn, scheduler_id, state, now)

    @staticmethod
    def _write_scheduler_state(
        conn: Any,
        scheduler_id: str,
        state: dict[str, Any],
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO scheduler_state(scheduler_id, revision, state_json, updated_at)
            VALUES(?, 1, ?, ?)
            ON CONFLICT(scheduler_id) DO UPDATE SET
                revision=scheduler_state.revision+1,
                state_json=excluded.state_json,
                updated_at=excluded.updated_at
            """,
            (scheduler_id, canonical_json(state), now),
        )

    def claim_workflow_action(
        self,
        worker_id: str,
        *,
        producer_generation: str = "",
        lease_seconds: int = 60,
        operators: set[str] | None = None,
    ) -> dict[str, Any] | None:
        worker_id = worker_id.strip()
        if not worker_id:
            raise ValueError("worker_id must not be empty")
        if operators is not None and not operators:
            return None
        admitted = tuple(sorted(operators)) if operators is not None else ()
        operator_filter = (" AND operator_id IN (" + ",".join("?" for _ in admitted) + ")") if operators is not None else ""
        now = utc_now()
        expires = (
            datetime.now(timezone.utc) + timedelta(seconds=max(1, lease_seconds))
        ).replace(microsecond=0).isoformat()
        token = uuid.uuid4().hex
        with self.transaction() as conn:
            active = conn.execute(
                "SELECT action_id FROM workflow_actions "
                "WHERE state IN ('claimed', 'running') LIMIT 1"
            ).fetchone()
            if active is not None:
                return None
            if producer_generation:
                row = conn.execute(
                    f"""
                    SELECT action_id, action_json FROM workflow_actions
                    WHERE state='queued' AND (
                        producer_generation=? OR action_kind IN (
                            'promote-agent-source', 'promote-agent-case',
                            'promote-agent-output'
                        )
                    ){operator_filter}
                    ORDER BY priority DESC, created_at, action_id
                    LIMIT 1
                    """,
                    (producer_generation, *admitted),
                ).fetchone()
            else:
                row = conn.execute(
                    f"""
                    SELECT action_id, action_json FROM workflow_actions
                    WHERE state='queued'{operator_filter}
                    ORDER BY priority DESC, created_at, action_id
                    LIMIT 1
                    """,
                    admitted,
                ).fetchone()
            if row is None:
                return None
            action_id = str(row[0])
            ordinal_row = conn.execute(
                "SELECT COALESCE(MAX(ordinal), 0) + 1 "
                "FROM workflow_action_attempts WHERE action_id=?",
                (action_id,),
            ).fetchone()
            ordinal = int(ordinal_row[0])
            attempt_id = f"wfat-{uuid.uuid4().hex}"
            changed = conn.execute(
                """
                UPDATE workflow_actions
                SET state='claimed', claimed_by=?, claim_token=?,
                    claim_expires_at=?, updated_at=?
                WHERE action_id=? AND state='queued'
                """,
                (worker_id, token, expires, now, action_id),
            ).rowcount
            if changed != 1:
                return None
            conn.execute(
                """
                INSERT INTO workflow_action_attempts(
                    attempt_id, action_id, ordinal, state, worker_id,
                    claim_token, claim_expires_at, heartbeat_at,
                    created_at, updated_at
                ) VALUES(?, ?, ?, 'claimed', ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    action_id,
                    ordinal,
                    worker_id,
                    token,
                    expires,
                    now,
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "workflow_action_claimed",
                "workflow_action",
                action_id,
                {
                    "attempt_id": attempt_id,
                    "ordinal": ordinal,
                    "worker_id": worker_id,
                    "claim_expires_at": expires,
                },
            )
            action = json.loads(str(row[1]))
            action["attempt_id"] = attempt_id
            action["attempt_ordinal"] = ordinal
            action["claim_token"] = token
            action["claimed_by"] = worker_id
            action["claim_expires_at"] = expires
            return action

    def cancel_obsolete_workflow_actions(
        self,
        *,
        producer_generation: str,
        active_operators: set[str],
        current_action_ids: set[str],
        cancel_board_drift: bool,
        preserve_operators: set[str] | None = None,
    ) -> list[dict[str, str]]:
        now = utc_now()
        cancelled: list[dict[str, str]] = []
        with self.transaction() as conn:
            rows = conn.execute(
                """
                SELECT action_id, operator_id, producer_generation, action_json
                FROM workflow_actions
                WHERE state='queued'
                ORDER BY created_at, action_id
                """
            ).fetchall()
            for row in rows:
                action_id = str(row[0])
                operator_id = str(row[1])
                if preserve_operators and operator_id in preserve_operators:
                    continue  # Registered lane migration requires original-identity adoption.
                action_generation = str(row[2])
                action_payload = json.loads(str(row[3]))
                candidate_identity = action_payload.get("candidate_identity", {})
                if (
                    isinstance(candidate_identity, dict)
                    and candidate_identity.get("origin")
                    in SEALED_AGENT_PROMOTION_ORIGINS
                ):
                    continue
                reason = ""
                if action_generation != producer_generation:
                    reason = "producer-generation-obsolete"
                elif operator_id not in active_operators:
                    reason = "operator-no-longer-active"
                elif cancel_board_drift and action_id not in current_action_ids:
                    reason = "board-revision-obsolete"
                if not reason:
                    continue
                receipt = validate_workflow_action_receipt(
                    {
                        "schema": WORKFLOW_ACTION_RECEIPT_SCHEMA,
                        "action_id": action_id,
                        "status": "cancelled",
                        "worker_id": "workflow-v4-reconciler",
                        "producer_generation": producer_generation,
                        "started_at": now,
                        "completed_at": now,
                        "return_code": None,
                        "details": {"reason": reason},
                    }
                )
                changed = conn.execute(
                    """
                    UPDATE workflow_actions
                    SET state='cancelled', completed_at=?, updated_at=?
                    WHERE action_id=? AND state='queued'
                    """,
                    (now, now, action_id),
                ).rowcount
                if changed != 1:
                    continue
                conn.execute(
                    """
                    INSERT INTO workflow_action_receipts(
                        action_id, status, receipt_json, completed_at
                    ) VALUES(?, 'cancelled', ?, ?)
                    """,
                    (action_id, canonical_json(receipt), now),
                )
                self._event(
                    conn,
                    "workflow_action_cancelled_obsolete",
                    "workflow_action",
                    action_id,
                    {"operator_id": operator_id, "reason": reason},
                )
                cancelled.append(
                    {
                        "action_id": action_id,
                        "operator_id": operator_id,
                        "reason": reason,
                    }
                )
        return cancelled

    def start_workflow_action(
        self,
        action_id: str,
        claim_token: str,
    ) -> str:
        now = utc_now()
        with self.transaction() as conn:
            changed = conn.execute(
                """
                UPDATE workflow_actions SET state='running', started_at=?,
                    execution_count=execution_count+1, updated_at=?
                WHERE action_id=? AND state='claimed' AND claim_token=?
                """,
                (now, now, action_id, claim_token),
            ).rowcount
            if changed != 1:
                raise ControlDatabaseError("workflow action claim is not current")
            attempt_changed = conn.execute(
                """
                UPDATE workflow_action_attempts
                SET state='running', started_at=?, heartbeat_at=?, updated_at=?
                WHERE action_id=? AND state='claimed' AND claim_token=?
                """,
                (now, now, now, action_id, claim_token),
            ).rowcount
            if attempt_changed != 1:
                raise ControlDatabaseError("workflow action attempt is not current")
            self._event(
                conn,
                "workflow_action_started",
                "workflow_action",
                action_id,
                {"started_at": now},
            )
        return now

    def attach_workflow_action_process(
        self,
        action_id: str,
        claim_token: str,
        *,
        role: str,
        pid: int,
        start_token: str,
        boot_id: str,
        lease_seconds: int,
    ) -> None:
        if role not in {"worker", "child"}:
            raise ValueError(f"unsupported workflow process role: {role}")
        if pid <= 0 or not start_token:
            raise ValueError("workflow process identity must be complete")
        now = utc_now()
        expires = (
            datetime.now(timezone.utc) + timedelta(seconds=max(1, lease_seconds))
        ).replace(microsecond=0).isoformat()
        pid_column = f"{role}_pid"
        token_column = f"{role}_start_token"
        with self.transaction() as conn:
            changed = conn.execute(
                f"UPDATE workflow_action_attempts SET {pid_column}=?, "
                f"{token_column}=?, boot_id=?, heartbeat_at=?, "
                "claim_expires_at=?, updated_at=? "
                "WHERE action_id=? AND state='running' AND claim_token=?",
                (
                    int(pid),
                    start_token,
                    boot_id,
                    now,
                    expires,
                    now,
                    action_id,
                    claim_token,
                ),
            ).rowcount
            if changed != 1:
                raise ControlDatabaseError("workflow action process is not current")
            conn.execute(
                "UPDATE workflow_actions SET claim_expires_at=?, updated_at=? "
                "WHERE action_id=? AND state='running' AND claim_token=?",
                (expires, now, action_id, claim_token),
            )
            self._event(
                conn,
                "workflow_action_process_attached",
                "workflow_action",
                action_id,
                {"role": role, "pid": int(pid), "start_token": start_token},
            )

    def heartbeat_workflow_action(
        self,
        action_id: str,
        claim_token: str,
        *,
        lease_seconds: int,
    ) -> bool:
        now = utc_now()
        expires = (
            datetime.now(timezone.utc) + timedelta(seconds=max(1, lease_seconds))
        ).replace(microsecond=0).isoformat()
        with self.transaction() as conn:
            changed = conn.execute(
                "UPDATE workflow_action_attempts SET heartbeat_at=?, "
                "claim_expires_at=?, updated_at=? WHERE action_id=? "
                "AND state='running' AND claim_token=?",
                (now, expires, now, action_id, claim_token),
            ).rowcount
            if changed != 1:
                return False
            conn.execute(
                "UPDATE workflow_actions SET claim_expires_at=?, updated_at=? "
                "WHERE action_id=? AND state='running' AND claim_token=?",
                (expires, now, action_id, claim_token),
            )
        return True

    def complete_workflow_action(
        self,
        action_id: str,
        claim_token: str,
        *,
        status: str,
        worker_id: str,
        producer_generation: str,
        started_at: str,
        return_code: int | None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state, claim_token FROM workflow_actions WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError("unknown workflow action")
            existing = conn.execute(
                "SELECT receipt_json FROM workflow_action_receipts WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if existing is not None:
                return json.loads(str(existing[0]))
            if str(row[0]) != "running" or str(row[1]) != claim_token:
                raise ControlDatabaseError("workflow action is not running under claim")
            completed_at = utc_now()
            receipt = validate_workflow_action_receipt(
                {
                    "schema": WORKFLOW_ACTION_RECEIPT_SCHEMA,
                    "action_id": action_id,
                    "status": status,
                    "worker_id": worker_id,
                    "producer_generation": producer_generation,
                    "started_at": started_at,
                    "completed_at": completed_at,
                    "return_code": return_code,
                    "details": details or {},
                }
            )
            conn.execute(
                """
                UPDATE workflow_actions
                SET state=?, completed_at=?, updated_at=?
                WHERE action_id=?
                """,
                (status, completed_at, completed_at, action_id),
            )
            conn.execute(
                """
                INSERT INTO workflow_action_receipts(
                    action_id, status, receipt_json, completed_at
                ) VALUES(?, ?, ?, ?)
                """,
                (action_id, status, canonical_json(receipt), completed_at),
            )
            conn.execute(
                "UPDATE workflow_action_attempts SET state=?, completed_at=?, "
                "details_json=?, updated_at=? WHERE action_id=? AND claim_token=?",
                (
                    status,
                    completed_at,
                    canonical_json(details or {}),
                    completed_at,
                    action_id,
                    claim_token,
                ),
            )
            self._event(
                conn,
                f"workflow_action_{status}",
                "workflow_action",
                action_id,
                {"return_code": return_code, "worker_id": worker_id},
            )
        return receipt

    def workflow_action_recovery_candidates(
        self,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        now = utc_now()
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT a.action_json, a.state AS action_state,
                       t.attempt_id, t.ordinal, t.state AS attempt_state,
                       t.worker_id, t.claim_token, t.claim_expires_at,
                       t.heartbeat_at, t.worker_pid, t.worker_start_token,
                       t.child_pid, t.child_start_token, t.boot_id,
                       t.started_at
                FROM workflow_actions a
                JOIN workflow_action_attempts t ON t.action_id=a.action_id
                WHERE a.state IN ('claimed', 'running')
                  AND t.state IN ('claimed', 'running')
                  AND t.claim_expires_at<>'' AND t.claim_expires_at<=?
                ORDER BY t.claim_expires_at, t.attempt_id LIMIT ?
                """,
                (now, max(0, int(limit))),
            ).fetchall()
        candidates: list[dict[str, Any]] = []
        for row in rows:
            action = json.loads(str(row["action_json"]))
            candidates.append(
                {
                    "action": action,
                    "action_state": str(row["action_state"]),
                    "attempt_id": str(row["attempt_id"]),
                    "attempt_ordinal": int(row["ordinal"]),
                    "attempt_state": str(row["attempt_state"]),
                    "worker_id": str(row["worker_id"]),
                    "claim_token": str(row["claim_token"]),
                    "claim_expires_at": str(row["claim_expires_at"]),
                    "heartbeat_at": str(row["heartbeat_at"]),
                    "worker_pid": int(row["worker_pid"]),
                    "worker_start_token": str(row["worker_start_token"]),
                    "child_pid": int(row["child_pid"]),
                    "child_start_token": str(row["child_start_token"]),
                    "boot_id": str(row["boot_id"]),
                    "started_at": str(row["started_at"]),
                }
            )
        return candidates

    def apply_workflow_action_recovery_decision(
        self,
        action_id: str,
        claim_token: str,
        *,
        decision: str,
        reason: str,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        if decision not in {"wait", "retry-prepublish", "retry-idempotent", "reconcile-required"}:
            raise ControlDatabaseError(
                f"unsupported workflow action recovery decision: {decision}"
            )
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT a.state AS action_state, t.state AS attempt_state, "
                "t.attempt_id FROM workflow_actions a "
                "JOIN workflow_action_attempts t ON t.action_id=a.action_id "
                "WHERE a.action_id=? AND t.claim_token=?",
                (action_id, claim_token),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError("unknown workflow action recovery candidate")
            if str(row["action_state"]) not in {"claimed", "running"}:
                return {"action_id": action_id, "decision": "already-terminal"}
            if decision == "wait":
                expires = (
                    datetime.now(timezone.utc)
                    + timedelta(seconds=max(1, lease_seconds))
                ).replace(microsecond=0).isoformat()
                conn.execute(
                    "UPDATE workflow_actions SET claim_expires_at=?, updated_at=? "
                    "WHERE action_id=? AND claim_token=?",
                    (expires, now, action_id, claim_token),
                )
                conn.execute(
                    "UPDATE workflow_action_attempts SET claim_expires_at=?, "
                    "updated_at=? WHERE action_id=? AND claim_token=?",
                    (expires, now, action_id, claim_token),
                )
            elif decision in {"retry-prepublish", "retry-idempotent"}:
                conn.execute(
                    "UPDATE workflow_actions SET state='queued', claimed_by='', "
                    "claim_token='', claim_expires_at='', started_at='', updated_at=? "
                    "WHERE action_id=? AND claim_token=?",
                    (now, action_id, claim_token),
                )
                conn.execute(
                    "UPDATE workflow_action_attempts SET state='abandoned', "
                    "completed_at=?, details_json=?, updated_at=? "
                    "WHERE action_id=? AND claim_token=?",
                    (
                        now,
                        canonical_json({"decision": decision, "reason": reason}),
                        now,
                        action_id,
                        claim_token,
                    ),
                )
            else:
                conn.execute(
                    "UPDATE workflow_actions SET state='reconcile_required', "
                    "claim_expires_at='', updated_at=? "
                    "WHERE action_id=? AND claim_token=?",
                    (now, action_id, claim_token),
                )
                conn.execute(
                    "UPDATE workflow_action_attempts SET state='reconcile_required', "
                    "completed_at=?, details_json=?, updated_at=? "
                    "WHERE action_id=? AND claim_token=?",
                    (
                        now,
                        canonical_json({"decision": decision, "reason": reason}),
                        now,
                        action_id,
                        claim_token,
                    ),
                )
            self._event(
                conn,
                "workflow_action_recovery_decision",
                "workflow_action",
                action_id,
                {
                    "attempt_id": str(row["attempt_id"]),
                    "decision": decision,
                    "reason": reason,
                },
            )
        return {"action_id": action_id, "decision": decision, "reason": reason}

    def workflow_action_attempts(self, action_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM workflow_action_attempts WHERE action_id=? "
                "ORDER BY ordinal",
                (action_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def workflow_action_counts(self) -> dict[str, int]:
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) FROM workflow_actions GROUP BY state"
            ).fetchall()
        return {str(row[0]): int(row[1]) for row in rows}

    def workflow_action_state(self, idempotency_key: str) -> str:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT state FROM workflow_actions WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
        return str(row[0]) if row is not None else ""
