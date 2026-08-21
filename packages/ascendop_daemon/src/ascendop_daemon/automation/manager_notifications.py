from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.actor import (
    flow_v5_catalog,
    validate_actor_action_envelope,
    validate_agent_action_outcome,
    validate_role_binding,
)
from ascendop_protocol.management import validate_manager_notification

from ascendop_daemon.core.atomic_io import write_json_atomic


class ManagerNotificationPublisher:
    """Project and deliver actionable blockers to the configured Manager."""

    def __init__(
        self,
        root: Path,
        database: Any,
        *,
        target_id: str,
        role_binding: Mapping[str, Any],
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.target_id = target_id.strip()
        self.binding = validate_role_binding(dict(role_binding))
        if self.binding["role"] != "manager":
            raise ValueError("Manager notification target requires manager binding")
        if self.binding["native_session_id"] != self.target_id:
            raise ValueError("Manager target does not match role binding session")

    def run_once(self) -> list[dict[str, Any]]:
        published: list[dict[str, Any]] = []
        for action_record in self.database.agent_actions_v4():
            if str(action_record.get("state") or "") != "completed":
                continue
            action = dict(action_record.get("action") or {})
            receipt = self.database.agent_action_receipt(
                str(action.get("action_id") or "")
            )
            completion = (
                dict(receipt.get("completion") or {})
                if isinstance(receipt, Mapping)
                else {}
            )
            raw_outcome = completion.get("agent_action_outcome")
            if not isinstance(raw_outcome, Mapping):
                continue
            outcome = validate_agent_action_outcome(raw_outcome)
            disposition = str(outcome.get("disposition") or "")
            if disposition not in {"blocked_external", "protocol_gap"}:
                continue
            published.append(
                self._publish_agent_blocker(action, dict(receipt or {}), outcome)
            )
        published.extend(self._publish_test_request_blockers())
        published.extend(self._publish_transport_holds())
        return published

    def _publish_agent_blocker(
        self,
        action: Mapping[str, Any],
        receipt: Mapping[str, Any],
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]:
        blocker = dict(outcome.get("blocker") or {})
        action_id = str(action["action_id"])
        kind = (
            "capability_gap"
            if outcome["disposition"] == "protocol_gap"
            else "user_decision"
            if blocker.get("kind") == "user_policy_decision"
            else "external_hold"
        )
        allowed = (
            ["developer.repair-capability"]
            if kind == "capability_gap"
            else ["manager.request-user-decision"]
            if kind == "user_decision"
            else ["manager.review-notification"]
        )
        identity = {
            "source_kind": "agent-action-outcome",
            "source_id": action_id,
            "notification_kind": kind,
            "blocker_code": str(blocker.get("code") or "unknown"),
        }
        return self._publish(
            identity=identity,
            flow_id=str(action.get("campaign") or "ascendop"),
            operator_id=str(action.get("operator_id") or "system"),
            summary=str(blocker.get("details") or outcome.get("summary")),
            requires_response=kind == "user_decision",
            allowed_commands=allowed,
            evidence={
                "action": dict(action),
                "receipt": dict(receipt),
                "outcome": dict(outcome),
            },
            parent_action_id=action_id,
            trace_id=action_id,
        )

    def _publish_test_request_blockers(self) -> list[dict[str, Any]]:
        with self.database.connection() as conn:
            rows = conn.execute(
                "SELECT request_id, operator_id, test_version, state, blocker, "
                "manifest_json, created_at, updated_at FROM test_requests "
                "WHERE blocker<>'' ORDER BY updated_at, request_id"
            ).fetchall()
        values: list[dict[str, Any]] = []
        for row in rows:
            blocker = str(row["blocker"])
            manifest = _json_object(row["manifest_json"])
            workflow = dict(manifest.get("workflow") or {})
            lineage = dict(workflow.get("lineage") or {})
            request_id = str(row["request_id"])
            origin_action_id = str(lineage.get("origin_action_id") or "")
            identity = {
                "source_kind": "test-request",
                "source_id": request_id,
                "notification_kind": "external_hold",
                "blocker_code": "test-request.blocked",
                "blocker_digest": hashlib.sha256(blocker.encode("utf-8")).hexdigest(),
            }
            values.append(
                self._publish(
                    identity=identity,
                    flow_id=str(
                        workflow.get("campaign")
                        or manifest.get("campaign")
                        or "ascendop"
                    ),
                    operator_id=str(row["operator_id"]),
                    summary=blocker,
                    requires_response=False,
                    allowed_commands=["manager.review-notification"],
                    evidence={
                        "request_id": request_id,
                        "operator_id": str(row["operator_id"]),
                        "test_version": str(row["test_version"]),
                        "state": str(row["state"]),
                        "blocker": blocker,
                        "lineage": lineage,
                        "created_at": str(row["created_at"]),
                        "updated_at": str(row["updated_at"]),
                    },
                    parent_action_id=origin_action_id or None,
                    trace_id=str(lineage.get("trace_id") or origin_action_id or request_id),
                    candidate_id=str(row["test_version"]),
                    request_id=request_id,
                )
            )
        return values

    def _publish_transport_holds(self) -> list[dict[str, Any]]:
        with self.database.connection() as conn:
            rows = conn.execute(
                "SELECT r.return_id, r.hold_reason, r.received_at, "
                "q.operator_id, q.test_version, a.attempt_id, q.request_id "
                "FROM transport_returns r "
                "JOIN execution_attempts a ON a.attempt_id=r.attempt_id "
                "JOIN test_requests q ON q.request_id=a.request_id "
                "WHERE r.hold_reason<>'' ORDER BY r.received_at, r.return_id"
            ).fetchall()
        values: list[dict[str, Any]] = []
        for row in rows:
            identity = {
                "source_kind": "transport-return",
                "source_id": str(row["return_id"]),
                "notification_kind": "external_hold",
                "blocker_code": "transport-result.held",
            }
            values.append(
                self._publish(
                    identity=identity,
                    flow_id="ascendop",
                    operator_id=str(row["operator_id"]),
                    summary=str(row["hold_reason"]),
                    requires_response=False,
                    allowed_commands=["manager.review-notification"],
                    evidence={
                        "return_id": str(row["return_id"]),
                        "request_id": str(row["request_id"]),
                        "attempt_id": str(row["attempt_id"]),
                        "test_version": str(row["test_version"]),
                        "hold_reason": str(row["hold_reason"]),
                    },
                    parent_action_id=None,
                    trace_id=str(row["request_id"]),
                    candidate_id=str(row["test_version"]),
                    request_id=str(row["request_id"]),
                    attempt_id=str(row["attempt_id"]),
                )
            )
        return values

    def _publish(
        self,
        *,
        identity: Mapping[str, str],
        flow_id: str,
        operator_id: str,
        summary: str,
        requires_response: bool,
        allowed_commands: list[str],
        evidence: Mapping[str, Any],
        parent_action_id: str | None,
        trace_id: str,
        candidate_id: str | None = None,
        request_id: str | None = None,
        attempt_id: str | None = None,
    ) -> dict[str, Any]:
        digest = hashlib.sha256(
            json.dumps(
                dict(identity),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        notification_id = f"manager-{digest[:24]}"
        evidence_path = (
            self.root
            / ".ascendop-work"
            / "runtime"
            / "manager-notifications"
            / f"{notification_id}.json"
        )
        evidence_document = {
            "schema": "ascendop.manager-notification-evidence.v1",
            "notification_id": notification_id,
            "identity": dict(identity),
            "evidence": dict(evidence),
        }
        if evidence_path.is_file():
            existing = json.loads(evidence_path.read_text(encoding="utf-8"))
            if existing != evidence_document:
                raise ValueError(f"Manager notification collision: {notification_id}")
        else:
            write_json_atomic(
                evidence_path,
                evidence_document,
                ensure_ascii=True,
                sort_keys=True,
            )
        evidence_ref = evidence_path.relative_to(self.root).as_posix()
        now = _utc_now()
        notification = validate_manager_notification(
            {
                "schema": "ascendop.manager-notification.v1",
                "notification_id": notification_id,
                "notification_kind": identity["notification_kind"],
                "flow_id": flow_id,
                "operator_id": operator_id,
                "summary": summary,
                "requires_response": requires_response,
                "evidence_refs": [evidence_ref],
                "allowed_commands": allowed_commands,
                "state": "open",
                "created_at": now,
            }
        )
        if not self.database.public_resources(
            "manager-notification", resource_id=notification_id
        ):
            self.database.upsert_public_resource(
                resource_type="manager-notification",
                resource_id=notification_id,
                revision=digest,
                attributes=notification,
                observed_at=now,
            )
        action_id = f"manager-review-{digest[:24]}"
        existing_action = self.database.assistant_action(action_id)
        if existing_action is not None:
            return existing_action
        expires = (
            datetime.now(timezone.utc) + timedelta(days=7)
        ).isoformat()
        envelope = validate_actor_action_envelope(
            {
                "schema": "ascendop.actor-action-envelope.v1",
                "action_id": action_id,
                "idempotency_key": f"manager.review-notification:{digest}",
                "action_kind": "manager.review-notification",
                "effective_role": "manager",
                "principal_id": self.binding["principal_id"],
                "role_binding_id": self.binding["role_binding_id"],
                "producer_generation": flow_v5_catalog()["generation"],
                "scope": dict(self.binding["scope"]),
                "lease": {
                    "lease_id": f"lease-{notification_id}",
                    "generation": self.binding["generation"],
                    "expires_at": expires,
                },
                "causation": {
                    "trace_id": trace_id,
                    "correlation_id": notification_id,
                    "parent_action_id": parent_action_id,
                    "candidate_id": candidate_id,
                    "promotion_receipt_id": None,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                },
                "payload": {
                    "notification_id": notification_id,
                    "notification_kind": identity["notification_kind"],
                    "flow_id": flow_id,
                    "operator_id": operator_id,
                    "summary": summary,
                    "requires_response": requires_response,
                    "evidence_refs": [evidence_ref],
                    "allowed_commands": allowed_commands,
                },
                "created_at": now,
            }
        )
        return self.database.create_actor_action_if_absent(
            envelope,
            target_id=self.target_id,
            context={"state": "manager-notification", "notification": notification},
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value:
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


__all__ = ["ManagerNotificationPublisher"]
