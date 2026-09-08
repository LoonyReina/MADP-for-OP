"""Normalize outcomes and read committed completion facts without side effects."""
from __future__ import annotations

from typing import Any, Mapping

from ascendop_protocol.actor import (
    validate_agent_action_outcome,
    validate_actor_action_receipt,
    validate_native_turn_outcome,
)


class AgentCompletionError(RuntimeError):
    pass


class OutcomeNormalizer:
    """Validate native terminal data and remove runtime-specific status drift."""

    def normalize(
        self,
        native_outcome: Mapping[str, Any],
        *,
        completion_metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        native = validate_native_turn_outcome(native_outcome)
        reported_status = str(native["terminal_status"])
        status = "failed" if reported_status == "interrupted" else reported_status
        structured = dict(native["structured_result"])
        completion = {
            **dict(completion_metadata or {}),
            "summary": str(structured.get("summary") or "Agent turn completed"),
            "session_id": str(native["native_session_id"]),
            "native_turn_id": str(native["native_turn_id"]),
            "native_observed_at": str(native["observed_at"]),
            "telemetry": dict(native["telemetry"]),
        }
        if reported_status == "interrupted":
            completion["reported_status"] = reported_status
            completion.setdefault("failure_class", "adapter-execution")
        raw_failure = str(
            structured.get("failure_class")
            or completion.get("failure_class")
            or ""
        )
        if raw_failure:
            completion["failure_class"] = raw_failure
        return {
            "native": native,
            "status": status,
            "reported_status": reported_status,
            "structured_result": structured,
            "artifact_refs": [str(item) for item in native["artifact_refs"]],
            "completion": completion,
        }


def read_standalone_late_outcome(database: Any, action_id: str) -> dict[str, Any] | None:
    rows = database.control_outbox(topic="agent.outcome", origin_id=action_id)
    if not rows:
        return None
    if len(rows) != 1 or rows[0]["payload"]["outcome"]["action_id"] != action_id:
        raise AgentCompletionError("accepted late outcome binding changed")
    validate_agent_action_outcome(rows[0]["payload"]["outcome"])
    accepted = read_standalone_completion(database, action_id)
    if (accepted is None or rows[0]["payload"]["completion_outbox_id"] != accepted["outbox_id"]
            or rows[0]["payload"]["binding"] != accepted["payload"]["binding"]):
        raise AgentCompletionError("late outcome original receipt differs")
    return rows[0]


def read_standalone_completion(database: Any, action_id: str) -> dict[str, Any] | None:
    rows = database.control_outbox(topic="agent.completion", origin_id=action_id)
    if not rows:
        return None
    if len(rows) != 1 or rows[0]["payload"].get("carrier") != "standalone-v5":
        raise AgentCompletionError("managed workspace completion carrier conflict")
    payload = rows[0]["payload"]
    receipt = validate_actor_action_receipt(payload["receipt"])
    if (receipt["action_id"] != action_id or receipt["lease_id"] != payload["binding"]["lease_id"]
            or payload["continuation"]["source_action_id"] != action_id):
        raise AgentCompletionError("accepted standalone completion binding changed")
    return rows[0]
