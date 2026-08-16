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

class AutomationServiceRepository:
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

    def record_service_heartbeat(
        self,
        *,
        service_id: str,
        role: str,
        code_generation: str,
        wire_version: int,
        capabilities: list[str],
        state: str,
        boot_id: str,
        lease_seconds: int = 30,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        if not all(
            isinstance(value, str) and value.strip()
            for value in (service_id, role, code_generation, state, boot_id)
        ):
            raise ControlDatabaseError("service heartbeat identity is incomplete")
        if isinstance(wire_version, bool) or int(wire_version) != 3:
            raise ControlDatabaseError("service heartbeat must advertise Wire V3")
        normalized_capabilities = sorted(
            {
                value.strip()
                for value in capabilities
                if isinstance(value, str) and value.strip()
            }
        )
        now_value = datetime.now(timezone.utc)
        now = _format_timestamp(now_value)
        lease_expires_at = _format_timestamp(
            now_value + timedelta(seconds=max(1, int(lease_seconds)))
        )
        capabilities_json = canonical_json(normalized_capabilities)
        details_json = canonical_json(details or {})
        with self.transaction() as conn:
            previous = conn.execute(
                "SELECT role, code_generation, wire_version, database_schema, "
                "capabilities_json, state, boot_id FROM service_heartbeats "
                "WHERE service_id=?",
                (service_id,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO service_heartbeats(
                    service_id, role, code_generation, wire_version,
                    database_schema, capabilities_json, state, boot_id,
                    heartbeat_at, lease_expires_at, details_json, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(service_id) DO UPDATE SET
                    role=excluded.role,
                    code_generation=excluded.code_generation,
                    wire_version=excluded.wire_version,
                    database_schema=excluded.database_schema,
                    capabilities_json=excluded.capabilities_json,
                    state=excluded.state,
                    boot_id=excluded.boot_id,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at,
                    details_json=excluded.details_json,
                    updated_at=excluded.updated_at
                """,
                (
                    service_id,
                    role,
                    code_generation,
                    3,
                    SCHEMA_VERSION,
                    capabilities_json,
                    state,
                    boot_id,
                    now,
                    lease_expires_at,
                    details_json,
                    now,
                ),
            )
            identity = (
                role,
                code_generation,
                3,
                SCHEMA_VERSION,
                capabilities_json,
                state,
                boot_id,
            )
            if previous is None or tuple(previous) != identity:
                self._event(
                    conn,
                    "service-registration-changed",
                    "service",
                    service_id,
                    {
                        "role": role,
                        "code_generation": code_generation,
                        "wire_version": 3,
                        "database_schema": SCHEMA_VERSION,
                        "capabilities": normalized_capabilities,
                        "state": state,
                        "boot_id": boot_id,
                    },
                )
        return {
            "service_id": service_id,
            "role": role,
            "code_generation": code_generation,
            "wire_version": 3,
            "database_schema": SCHEMA_VERSION,
            "capabilities": normalized_capabilities,
            "state": state,
            "boot_id": boot_id,
            "heartbeat_at": now,
            "lease_expires_at": lease_expires_at,
            "details": details or {},
            "live": state == "ready",
        }

    def service_health(self) -> list[dict[str, Any]]:
        self.initialize()
        now = datetime.now(timezone.utc)
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM service_heartbeats ORDER BY service_id"
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            lease_expires_at = _parse_timestamp(
                str(row["lease_expires_at"]),
                "lease_expires_at",
            )
            result.append(
                {
                    "service_id": str(row["service_id"]),
                    "role": str(row["role"]),
                    "code_generation": str(row["code_generation"]),
                    "wire_version": int(row["wire_version"]),
                    "database_schema": int(row["database_schema"]),
                    "capabilities": json.loads(row["capabilities_json"]),
                    "state": str(row["state"]),
                    "boot_id": str(row["boot_id"]),
                    "heartbeat_at": str(row["heartbeat_at"]),
                    "lease_expires_at": str(row["lease_expires_at"]),
                    "details": json.loads(row["details_json"]),
                    "live": (
                        str(row["state"]) == "ready"
                        and lease_expires_at >= now
                    ),
                }
            )
        return result
