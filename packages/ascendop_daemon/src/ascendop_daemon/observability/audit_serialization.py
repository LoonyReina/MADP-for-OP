"""Pure audit serialization, independent of deployment status rendering."""
from __future__ import annotations

from ascendop_daemon.core.models import GateDecision, TransportObservation


def serialize_decision(decision: GateDecision | None) -> dict[str, object] | None:
    if decision is None:
        return None
    return {
        "op": decision.row.op,
        "gate_stage": decision.row.gate_stage,
        "next_owner": decision.row.next_owner,
        "action": decision.action.value,
        "reason": decision.reason,
        "command": decision.command,
        "priority": decision.priority,
        "blocks_operator": decision.blocks_operator,
        "action_id": decision.action_id,
    }


def serialize_transport(observation: TransportObservation) -> dict[str, object]:
    return {
        "op": observation.op,
        "test_version": observation.test_version,
        "request_id": observation.request_id,
        "heartbeat_path": observation.heartbeat_path,
        "output_status_path": observation.output_status_path,
        "state": observation.state,
        "client_state": observation.client_state,
        "client_updated_at": observation.client_updated_at,
        "client_progress_observed_at": observation.client_progress_observed_at,
        "terminal": observation.terminal,
        "stalled": observation.stalled,
        "remote_feedback_status": observation.remote_feedback_status,
        "stall_reason": observation.stall_reason,
        "first_observed_at_utc": observation.first_observed_at_utc,
        "observed_at_utc": observation.observed_at_utc,
        "last_feedback_at_utc": observation.last_feedback_at_utc,
        "elapsed_without_feedback_seconds": observation.elapsed_without_feedback_seconds,
        "elapsed_without_remote_feedback_seconds": observation.elapsed_without_remote_feedback_seconds,
        "relay_publish_verify": observation.relay_publish_verify,
        "client_ssh": observation.client_ssh,
        "summary": observation.summary,
    }
