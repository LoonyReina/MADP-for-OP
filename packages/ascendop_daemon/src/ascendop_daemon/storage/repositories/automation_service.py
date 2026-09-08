from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_protocol.actor import (
    ACTOR_ACTION_RECEIPT_SCHEMA,
    validate_actor_action_envelope,
    validate_actor_action_receipt,
)
from ascendop_protocol.automation import (
    ASSISTANT_ACTION_RECEIPT_SCHEMA,
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
    SYSTEM_EXPERIMENT_GENERATION,
    SYSTEM_EXPERIMENT_OPERATOR_ID,
    ControlDatabaseError,
)
from ascendop_daemon.storage.control_validation import (
    _ensure_column,
    _is_bootstrap_control_probe,
    _node_report_is_newer,
    _normalized_node_lease,
    _require_transport_claim,
    _runtime_route_rejection_reasons,
    _validate_node_report,
    _validate_observed_node_against_registry,
    _validate_transport_identity,
    canonical_json,
    utc_now,
)

class AutomationServiceRepository:
    def assistant_action(self, action_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
        return decode_assistant_action(row, idempotent=True) if row is not None else None

    def renew_pending_actor_action_lease(
        self,
        action_id: str,
        *,
        lease_seconds: int,
    ) -> dict[str, Any]:
        """Renew an unclaimed action whose authorization lease expired in queue."""

        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
        if row is None:
            raise ControlDatabaseError(f"unknown Actor action: {action_id}")
        if str(row["state"]) != "pending":
            return decode_assistant_action(row, idempotent=True)
        if str(row["claimed_by"] or "") or str(row["claim_token"] or ""):
            raise ControlDatabaseError(
                "pending Actor action unexpectedly retains a claim identity"
            )

        raw = json.loads(str(row["request_json"]))
        current = validate_actor_action_envelope(raw)
        now_value = datetime.now(timezone.utc)
        current_expiry = datetime.fromisoformat(
            str(current["lease"]["expires_at"]).replace("Z", "+00:00")
        )
        if current_expiry > now_value:
            return decode_assistant_action(row, idempotent=True)

        renewed = dict(current)
        renewed["lease"] = dict(current["lease"])
        renewed["lease"]["expires_at"] = (
            now_value + timedelta(seconds=max(1, int(lease_seconds)))
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        renewed = self.authorize_actor_action(
            validate_actor_action_envelope(renewed)
        )
        renewed_json = canonical_json(renewed)
        now = now_value.isoformat()
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE assistant_action_requests SET request_json=?, updated_at=? "
                "WHERE action_id=? AND state='pending' AND request_json=? "
                "AND claimed_by='' AND claim_token=''",
                (renewed_json, now, action_id, str(row["request_json"])),
            )
            if cursor.rowcount == 1:
                self._event(
                    conn,
                    "actor-action-lease-renewed",
                    "actor-action",
                    action_id,
                    {
                        "lease_id": renewed["lease"]["lease_id"],
                        "previous_expires_at": current["lease"]["expires_at"],
                        "expires_at": renewed["lease"]["expires_at"],
                    },
                )
            current_row = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
        assert current_row is not None
        return decode_assistant_action(
            current_row,
            idempotent=cursor.rowcount != 1,
        )

    def create_actor_action_if_absent(
        self,
        action: dict[str, Any],
        *,
        target_id: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        value = self.authorize_actor_action(
            validate_actor_action_envelope(action)
        )
        target = target_id.strip()
        if not target:
            raise ControlDatabaseError("Actor action target must not be empty")
        binding = self.role_binding(str(value["role_binding_id"]))
        if binding is None or binding["native_session_id"] != target:
            raise ControlDatabaseError(
                "Actor action target does not match its role binding session"
            )
        request_json = canonical_json(value)
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE idempotency_key=?",
                (str(value["idempotency_key"]),),
            ).fetchone()
            if existing is not None:
                if str(existing["request_json"]) != request_json:
                    raise ControlDatabaseError(
                        "Actor action idempotency collision with different payload"
                    )
                return decode_assistant_action(existing, idempotent=True)
            by_id = conn.execute(
                "SELECT request_json FROM assistant_action_requests WHERE action_id=?",
                (str(value["action_id"]),),
            ).fetchone()
            if by_id is not None:
                raise ControlDatabaseError(
                    "Actor action identity collision with different idempotency key"
                )
            conn.execute(
                "INSERT INTO assistant_action_requests("
                "action_id, idempotency_key, rule_id, assistant_target_id, "
                "state, request_json, metrics_json, created_at, updated_at"
                ") VALUES(?, ?, ?, ?, 'pending', ?, ?, ?, ?)",
                (
                    str(value["action_id"]),
                    str(value["idempotency_key"]),
                    str(value["action_kind"]),
                    target,
                    request_json,
                    canonical_json(context or {}),
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "actor-action-created",
                "actor-action",
                str(value["action_id"]),
                {"action": value, "target_id": target},
            )
            row = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (str(value["action_id"]),),
            ).fetchone()
            assert row is not None
            return decode_assistant_action(row, idempotent=False)

    def claim_actor_actions(
        self,
        target_id: str,
        *,
        effective_role: str,
        consumer_id: str,
        max_items: int = 1,
        lease_seconds: int = 90,
    ) -> list[dict[str, Any]]:
        self.initialize()
        now_value = datetime.now(timezone.utc)
        now = now_value.isoformat()
        expires = (now_value + timedelta(seconds=max(1, lease_seconds))).isoformat()
        with self.transaction() as conn:
            conn.execute(
                "UPDATE assistant_action_requests SET state='pending', claimed_by='', "
                "claim_token='', claim_expires_at='', updated_at=? "
                "WHERE state='claimed' AND claim_expires_at<>'' AND claim_expires_at<=?",
                (now, now),
            )
            rows = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE assistant_target_id=? "
                "AND state='pending' ORDER BY created_at, action_id",
                (target_id,),
            ).fetchall()
            candidates: list[tuple[sqlite3.Row, dict[str, Any]]] = []
            for row in rows:
                raw = json.loads(str(row["request_json"]))
                if raw.get("schema") != "ascendop.actor-action-envelope.v1":
                    continue
                value = validate_actor_action_envelope(raw)
                if value["effective_role"] != effective_role:
                    continue
                lease_expiry = datetime.fromisoformat(
                    str(value["lease"]["expires_at"]).replace("Z", "+00:00")
                )
                if lease_expiry <= now_value:
                    continue
                value = self.authorize_actor_action(value)
                candidates.append((row, value))
                if len(candidates) >= max(1, int(max_items)):
                    break
            claimed: list[dict[str, Any]] = []
            for row, value in candidates:
                binding = self.role_binding(str(value["role_binding_id"]))
                if binding is None or binding["native_session_id"] != target_id:
                    raise ControlDatabaseError(
                        "Actor action target no longer matches its role binding"
                    )
                token = uuid.uuid4().hex
                cursor = conn.execute(
                    "UPDATE assistant_action_requests SET state='claimed', "
                    "claimed_by=?, claim_token=?, claim_expires_at=?, updated_at=? "
                    "WHERE action_id=? AND state='pending'",
                    (consumer_id, token, expires, now, str(row["action_id"])),
                )
                if cursor.rowcount != 1:
                    continue
                current = conn.execute(
                    "SELECT * FROM assistant_action_requests WHERE action_id=?",
                    (str(row["action_id"]),),
                ).fetchone()
                assert current is not None
                claimed.append(decode_assistant_action(current, idempotent=False))
            return claimed

    def record_actor_action_receipt(
        self,
        receipt: dict[str, Any],
        *,
        consumer_id: str,
        claim_token: str,
    ) -> dict[str, Any]:
        self.initialize()
        value = validate_actor_action_receipt(receipt)
        action_id = str(value["action_id"])
        now = utc_now()
        with self.transaction() as conn:
            action = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None:
                raise ControlDatabaseError(f"unknown Actor action: {action_id}")
            envelope = validate_actor_action_envelope(
                json.loads(str(action["request_json"]))
            )
            expected = {
                "action_id": envelope["action_id"],
                "action_kind": envelope["action_kind"],
                "effective_role": envelope["effective_role"],
                "role_binding_id": envelope["role_binding_id"],
                "lease_id": envelope["lease"]["lease_id"],
            }
            if any(value[field] != expected[field] for field in expected):
                raise ControlDatabaseError(
                    "Actor action receipt does not match its immutable action"
                )
            existing = conn.execute(
                "SELECT * FROM assistant_action_receipts WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["receipt_json"]) != canonical_json(value):
                    raise ControlDatabaseError(
                        f"Actor action receipt collision: {action_id}"
                    )
                return decode_assistant_receipt(existing, idempotent=True)
            if (
                str(action["state"]) != "claimed"
                or str(action["claimed_by"]) != consumer_id
                or str(action["claim_token"]) != claim_token
            ):
                raise ControlDatabaseError(f"Actor action claim is stale: {action_id}")
            conn.execute(
                "INSERT INTO assistant_action_receipts("
                "action_id, status, receipt_json, completed_at"
                ") VALUES(?, ?, ?, ?)",
                (
                    action_id,
                    str(value["status"]),
                    canonical_json(value),
                    str(value["completed_at"]),
                ),
            )
            conn.execute(
                "UPDATE assistant_action_requests SET state=?, claimed_by='', "
                "claim_token='', claim_expires_at='', updated_at=? WHERE action_id=?",
                (str(value["status"]), now, action_id),
            )
            self._event(
                conn,
                "actor-action-receipted",
                "actor-action",
                action_id,
                value,
            )
            row = conn.execute(
                "SELECT * FROM assistant_action_receipts WHERE action_id=?",
                (action_id,),
            ).fetchone()
            assert row is not None
            return decode_assistant_receipt(row, idempotent=False)

    def defer_actor_action_delivery(
        self,
        action_id: str,
        *,
        consumer_id: str,
        claim_token: str,
        error: str,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        self.initialize()
        error = error.strip() or "actor-delivery-failed"
        max_attempts = max(1, int(max_attempts))
        now = utc_now()
        with self.transaction() as conn:
            action = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None:
                raise ControlDatabaseError(f"unknown Actor action: {action_id}")
            envelope = validate_actor_action_envelope(
                json.loads(str(action["request_json"]))
            )
            if (
                str(action["state"]) != "claimed"
                or str(action["claimed_by"]) != consumer_id
                or str(action["claim_token"]) != claim_token
            ):
                raise ControlDatabaseError(f"Actor action claim is stale: {action_id}")
            previous = conn.execute(
                "SELECT COUNT(*) AS count FROM control_events "
                "WHERE event_type='actor-action-delivery-deferred' "
                "AND entity_type='actor-action' AND entity_id=?",
                (action_id,),
            ).fetchone()
            attempt = int(previous["count"] if previous is not None else 0) + 1
            terminal = attempt >= max_attempts
            state = "failed" if terminal else "pending"
            conn.execute(
                "UPDATE assistant_action_requests SET state=?, claimed_by='', "
                "claim_token='', claim_expires_at='', updated_at=? WHERE action_id=?",
                (state, now, action_id),
            )
            details = {
                "attempt": attempt,
                "max_attempts": max_attempts,
                "error": error,
                "terminal": terminal,
            }
            self._event(
                conn,
                "actor-action-delivery-deferred",
                "actor-action",
                action_id,
                details,
            )
            if terminal:
                receipt = validate_actor_action_receipt(
                    {
                        "schema": ACTOR_ACTION_RECEIPT_SCHEMA,
                        "action_id": action_id,
                        "action_kind": envelope["action_kind"],
                        "effective_role": envelope["effective_role"],
                        "role_binding_id": envelope["role_binding_id"],
                        "lease_id": envelope["lease"]["lease_id"],
                        "status": "failed",
                        "result": details,
                        "failure_class": "adapter_execution",
                        "completed_at": now,
                    }
                )
                conn.execute(
                    "INSERT INTO assistant_action_receipts("
                    "action_id, status, receipt_json, completed_at"
                    ") VALUES(?, 'failed', ?, ?)",
                    (action_id, canonical_json(receipt), now),
                )
            return {"action_id": action_id, "state": state, **details}

    def create_assistant_action_if_triggered(
        self,
        rule: dict[str, Any],
        metrics: dict[str, Any],
        *,
        operator: str,
        candidate_digest: str,
        workspace: str,
        runbook_path: str,
        evidence: list[str],
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        self.initialize()
        if not evaluate_trigger_rule(rule, metrics):
            return None
        identity = {
            "rule_id": str(rule["rule_id"]),
            "assistant_target_id": str(rule["assistant_target_id"]),
            "action": str(rule["action"]),
            "operator": operator,
            "candidate_digest": candidate_digest,
            "workspace": workspace,
        }
        identity_json = canonical_json(identity)
        idempotency_key = hashlib.sha256(identity_json.encode("utf-8")).hexdigest()
        action_id = f"assistant-{idempotency_key[:24]}"
        now = utc_now()
        request = validate_action_request(
            {
                "schema": ASSISTANT_ACTION_REQUEST_SCHEMA,
                "action_id": action_id,
                "rule_id": str(rule["rule_id"]),
                "assistant_target_id": str(rule["assistant_target_id"]),
                "action": str(rule["action"]),
                "operator": operator,
                "candidate_digest": candidate_digest,
                "workspace": workspace,
                "runbook_path": runbook_path,
                "evidence": list(evidence),
                "parameters": dict(parameters or {}),
                "created_at": now,
            }
        )
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM assistant_action_requests "
                "WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            superseded = conn.execute(
                """
                SELECT action_id FROM assistant_action_requests
                WHERE rule_id=? AND assistant_target_id=? AND state='pending'
                  AND action_id<>?
                ORDER BY created_at, action_id
                """,
                (
                    str(rule["rule_id"]),
                    str(rule["assistant_target_id"]),
                    action_id,
                ),
            ).fetchall()
            for stale in superseded:
                stale_action_id = str(stale["action_id"])
                receipt = validate_action_receipt(
                    {
                        "schema": ASSISTANT_ACTION_RECEIPT_SCHEMA,
                        "action_id": stale_action_id,
                        "status": "cancelled",
                        "completed_at": now,
                        "details": {
                            "reason": "superseded-before-delivery",
                            "superseded_by": action_id,
                        },
                    }
                )
                conn.execute(
                    """
                    INSERT INTO assistant_action_receipts(
                        action_id, status, receipt_json, completed_at
                    ) VALUES(?, 'cancelled', ?, ?)
                    """,
                    (stale_action_id, canonical_json(receipt), now),
                )
                conn.execute(
                    """
                    UPDATE assistant_action_requests SET
                        state='cancelled', updated_at=?
                    WHERE action_id=? AND state='pending'
                    """,
                    (now, stale_action_id),
                )
                self._event(
                    conn,
                    "assistant-action-superseded",
                    "assistant-action",
                    stale_action_id,
                    receipt,
                )
            if existing is not None:
                return decode_assistant_action(existing, idempotent=True)
            conn.execute(
                """
                INSERT INTO assistant_action_requests(
                    action_id, idempotency_key, rule_id, assistant_target_id,
                    state, request_json, metrics_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                """,
                (
                    action_id,
                    idempotency_key,
                    str(rule["rule_id"]),
                    str(rule["assistant_target_id"]),
                    canonical_json(request),
                    canonical_json(metrics),
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "assistant-action-created",
                "assistant-action",
                action_id,
                {"request": request, "metrics": metrics},
            )
            row = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
            assert row is not None
            return decode_assistant_action(row, idempotent=False)

    def claim_assistant_actions(
        self,
        assistant_target_id: str,
        *,
        consumer_id: str,
        max_items: int = 1,
        lease_seconds: int = 90,
    ) -> list[dict[str, Any]]:
        self.initialize()
        now_value = datetime.now(timezone.utc)
        now = now_value.isoformat()
        expires = (now_value + timedelta(seconds=max(1, lease_seconds))).isoformat()
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE assistant_action_requests SET
                    state='pending', claimed_by='', claim_token='',
                    claim_expires_at='', updated_at=?
                WHERE state='claimed' AND claim_expires_at<>''
                  AND claim_expires_at<=?
                """,
                (now, now),
            )
            rows = conn.execute(
                """
                SELECT action_id FROM assistant_action_requests
                WHERE assistant_target_id=? AND state='pending'
                ORDER BY created_at, action_id LIMIT ?
                """,
                (assistant_target_id, max(1, int(max_items))),
            ).fetchall()
            claimed: list[dict[str, Any]] = []
            for row in rows:
                token = uuid.uuid4().hex
                cursor = conn.execute(
                    """
                    UPDATE assistant_action_requests SET
                        state='claimed', claimed_by=?, claim_token=?,
                        claim_expires_at=?, updated_at=?
                    WHERE action_id=? AND state='pending'
                    """,
                    (consumer_id, token, expires, now, str(row["action_id"])),
                )
                if cursor.rowcount != 1:
                    continue
                current = conn.execute(
                    "SELECT * FROM assistant_action_requests WHERE action_id=?",
                    (str(row["action_id"]),),
                ).fetchone()
                assert current is not None
                claimed.append(decode_assistant_action(current, idempotent=False))
            return claimed

    def record_assistant_action_receipt(
        self,
        receipt: dict[str, Any],
        *,
        consumer_id: str,
        claim_token: str,
    ) -> dict[str, Any]:
        self.initialize()
        value = validate_action_receipt(receipt)
        action_id = str(value["action_id"])
        now = utc_now()
        with self.transaction() as conn:
            action = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None:
                raise ControlDatabaseError(f"unknown Assistant action: {action_id}")
            existing = conn.execute(
                "SELECT * FROM assistant_action_receipts WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["receipt_json"]) != canonical_json(value):
                    raise ControlDatabaseError(
                        f"Assistant action receipt collision: {action_id}"
                    )
                return decode_assistant_receipt(existing, idempotent=True)
            if (
                str(action["state"]) != "claimed"
                or str(action["claimed_by"]) != consumer_id
                or str(action["claim_token"]) != claim_token
            ):
                raise ControlDatabaseError(
                    f"Assistant action claim is stale: {action_id}"
                )
            conn.execute(
                """
                INSERT INTO assistant_action_receipts(
                    action_id, status, receipt_json, completed_at
                ) VALUES(?, ?, ?, ?)
                """,
                (
                    action_id,
                    str(value["status"]),
                    canonical_json(value),
                    str(value["completed_at"]),
                ),
            )
            conn.execute(
                """
                UPDATE assistant_action_requests SET
                    state=?, claimed_by='', claim_token='', claim_expires_at='',
                    updated_at=? WHERE action_id=?
                """,
                (str(value["status"]), now, action_id),
            )
            self._event(
                conn,
                "assistant-action-receipted",
                "assistant-action",
                action_id,
                value,
            )
            row = conn.execute(
                "SELECT * FROM assistant_action_receipts WHERE action_id=?",
                (action_id,),
            ).fetchone()
            assert row is not None
            return decode_assistant_receipt(row, idempotent=False)

    def defer_assistant_action_delivery(
        self,
        action_id: str,
        *,
        consumer_id: str,
        claim_token: str,
        error: str,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        self.initialize()
        error = error.strip() or "assistant-delivery-failed"
        max_attempts = max(1, int(max_attempts))
        now = utc_now()
        with self.transaction() as conn:
            action = conn.execute(
                "SELECT * FROM assistant_action_requests WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None:
                raise ControlDatabaseError(f"unknown Assistant action: {action_id}")
            if (
                str(action["state"]) != "claimed"
                or str(action["claimed_by"]) != consumer_id
                or str(action["claim_token"]) != claim_token
            ):
                raise ControlDatabaseError(
                    f"Assistant action claim is stale: {action_id}"
                )
            previous = conn.execute(
                """
                SELECT COUNT(*) AS count FROM control_events
                WHERE event_type='assistant-action-delivery-deferred'
                  AND entity_type='assistant-action' AND entity_id=?
                """,
                (action_id,),
            ).fetchone()
            attempt = int(previous["count"] if previous is not None else 0) + 1
            terminal = attempt >= max_attempts
            state = "failed" if terminal else "pending"
            conn.execute(
                """
                UPDATE assistant_action_requests SET
                    state=?, claimed_by='', claim_token='', claim_expires_at='',
                    updated_at=? WHERE action_id=?
                """,
                (state, now, action_id),
            )
            details = {
                "attempt": attempt,
                "max_attempts": max_attempts,
                "error": error,
                "terminal": terminal,
            }
            self._event(
                conn,
                "assistant-action-delivery-deferred",
                "assistant-action",
                action_id,
                details,
            )
            if terminal:
                receipt = validate_action_receipt(
                    {
                        "schema": ASSISTANT_ACTION_RECEIPT_SCHEMA,
                        "action_id": action_id,
                        "status": "failed",
                        "completed_at": now,
                        "details": details,
                    }
                )
                conn.execute(
                    """
                    INSERT INTO assistant_action_receipts(
                        action_id, status, receipt_json, completed_at
                    ) VALUES(?, 'failed', ?, ?)
                    """,
                    (action_id, canonical_json(receipt), now),
                )
            return {
                "action_id": action_id,
                "state": state,
                **details,
            }
