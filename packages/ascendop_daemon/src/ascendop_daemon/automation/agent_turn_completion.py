from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from ascendop_protocol.actor import ActorContractError
from ascendop_protocol.agent import (
    AgentContractError,
    agent_turn_delivery_identity,
    validate_agent_turn_completion,
)

from ascendop_daemon.control_plane.control_database import ControlDatabase


TERMINAL_STATUSES = {
    "completed",
    "failed",
    "interrupted",
    "cancelled",
    "uncertain",
}


class AgentTurnCompletionError(ValueError):
    """Raised when a native turn completion no longer matches its delivery."""


@dataclass(frozen=True)
class PreparedAgentTurnCompletion:
    terminal_status: str
    structured_result: dict[str, Any]
    reconciliation: dict[str, Any] | None = None
    state_updates: dict[str, Any] | None = None


def prepare_agent_turn_completion(
    *,
    database: ControlDatabase,
    action_id: str,
    state: Mapping[str, Any],
    delivery: Mapping[str, Any] | None,
    completion_envelope: Mapping[str, Any] | None,
    status: str,
    summary: str,
    failure_class: str,
    runner_id: str,
    lease_seconds: int,
    observed_at: str,
) -> PreparedAgentTurnCompletion:
    """Normalize one adapter terminal result and reconcile a lost old receipt."""

    reconciliation: dict[str, Any] | None = None
    state_updates: dict[str, Any] | None = None
    if completion_envelope is None:
        structured_result = {
            "summary": summary,
            **({"failure_class": failure_class} if failure_class else {}),
        }
    else:
        if delivery is None:
            raise AgentTurnCompletionError("Agent completion delivery is unavailable")
        envelope = _validate_native_completion(completion_envelope)
        identity = agent_turn_delivery_identity(delivery)
        expected = {
            "action_id": action_id,
            "attempt_id": str(state["attempt_id"]),
            "delivery_key": str(identity["delivery_key"]),
            "delivery_marker": str(identity["delivery_marker"]),
            "target_id": str(state["target_id"]),
            "turn_id": str(state.get("turn_id") or ""),
        }
        for field, expected_value in expected.items():
            if str(envelope[field]) != expected_value:
                raise AgentTurnCompletionError(
                    f"Agent completion {field} identity changed"
                )
        envelope_status = str(envelope["terminal_status"])
        if status and status != envelope_status:
            raise AgentTurnCompletionError("Agent completion terminal status changed")
        status = envelope_status
        structured_result = dict(envelope["structured_result"])
        output_error = str(envelope.get("output_error") or "")
        if output_error:
            structured_result = {
                "summary": summary
                or str(structured_result.get("summary") or "").strip()
                or "Agent turn output is not valid JSON",
                "native_output_validation_error": output_error,
            }
        completion_digest = hashlib.sha256(
            json.dumps(
                envelope,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        action_record = database.agent_action(action_id)
        if action_record is not None and action_record["state"] == "failed":
            recovered = database.recover_discarded_agent_turn_outcome(
                action_id=action_id,
                source_attempt_id=str(state["attempt_id"]),
                turn_id=str(envelope["turn_id"]),
                completion_digest=completion_digest,
                verification="exact-terminal-turn-structured-result",
                runner_id=runner_id,
                lease_seconds=lease_seconds,
            )
            reconciliation = dict(recovered["recovery"])
            lease = dict(recovered["lease"])
            state_updates = {
                "attempt_id": str(recovered["attempt_id"]),
                "lease_id": str(lease["lease_id"]),
                "lease_token": str(lease["lease_token"]),
                "phase": "running",
                "receipt_reconciliation": reconciliation,
                "updated_at": observed_at,
            }

    if status not in TERMINAL_STATUSES:
        raise AgentTurnCompletionError(
            f"unsupported Agent completion status: {status}"
        )
    return PreparedAgentTurnCompletion(
        terminal_status=status,
        structured_result=structured_result,
        reconciliation=reconciliation,
        state_updates=state_updates,
    )


def _validate_native_completion(
    completion_envelope: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep native formatting errors separate from delivery identity errors."""

    try:
        return validate_agent_turn_completion(completion_envelope)
    except (AgentContractError, ActorContractError) as exc:
        structured = completion_envelope.get("structured_result")
        if not isinstance(structured, Mapping) or structured.get("schema") != (
            "ascendop.agent-action-outcome.v1"
        ):
            raise
        action_id = str(completion_envelope.get("action_id") or "")
        terminal_status = str(completion_envelope.get("terminal_status") or "")
        expected_status = (
            "failed" if terminal_status == "interrupted" else terminal_status
        )
        if (
            not action_id
            or str(structured.get("action_id") or "") != action_id
            or str(structured.get("execution_status") or "") != expected_status
        ):
            raise
        summary = str(structured.get("summary") or "").strip()
        if not summary:
            raise
        fallback = {
            **dict(completion_envelope),
            "structured_result": {"summary": summary},
            "output_error": f"typed Agent outcome validation failed: {exc}",
        }
        return validate_agent_turn_completion(fallback)
