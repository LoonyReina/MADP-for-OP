from __future__ import annotations

from typing import Any, Mapping

from ascendop_protocol.agent import AGENT_ACTION_RECEIPT_SCHEMA

from ascendop_daemon.automation.agent_workspace import AgentSourceIdentityChanged
from ascendop_daemon.automation.codex_ide_settings import ADAPTER_ID


def superseded_receipt(
    claimed: Mapping[str, Any],
    error: AgentSourceIdentityChanged,
    *,
    adapter_generation: str,
    execution_contract_digest: str,
    completed_at: str,
) -> dict[str, Any]:
    action = claimed["action"]
    agent = claimed["agent"]
    lease = claimed["lease"]
    return {
        "schema": AGENT_ACTION_RECEIPT_SCHEMA,
        "action_id": str(action["action_id"]),
        "iteration_id": str(action["iteration_id"]),
        "agent_id": str(agent["agent_id"]),
        "lease_id": str(lease["lease_id"]),
        "status": "cancelled",
        "started_at": str(lease["acquired_at"]),
        "completed_at": completed_at,
        "completion": {
            "summary": "candidate source changed before Agent delivery",
            "session_id": "",
            "adapter_id": ADAPTER_ID,
            "runner_generation": adapter_generation,
            "agent_execution_contract_digest": execution_contract_digest,
            "failure_class": "candidate-superseded",
            "expected_source_digest": error.expected,
            "actual_source_digest": error.actual,
            "delivery_publish_state": "not-published",
        },
        "artifacts": [],
    }


def superseded_adapter_state(
    claimed: Mapping[str, Any],
    *,
    completed_at: str,
) -> dict[str, Any]:
    action = claimed["action"]
    agent = claimed["agent"]
    lease = claimed["lease"]
    return {
        "schema": "ascendop.codex-ide-adapter-state.v1",
        "action_id": str(action["action_id"]),
        "attempt_id": str(claimed["attempt_id"]),
        "agent_id": str(agent["agent_id"]),
        "lease_id": str(lease["lease_id"]),
        "consumer_id": "",
        "phase": "cancelled",
        "turn_id": "",
        "claimed_at": str(lease["acquired_at"]),
        "updated_at": completed_at,
        "failure_class": "candidate-superseded",
    }
