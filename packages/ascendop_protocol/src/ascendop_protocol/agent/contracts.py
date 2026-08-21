from __future__ import annotations

import hashlib
import re
from typing import Any, Mapping


AGENT_REGISTRATION_SCHEMA = "ascendop.agent-registration.v1"
AGENT_POOL_SCHEMA = "ascendop.agent-pool.v1"
AGENT_ACTION_SCHEMA = "ascendop.agent-action.v1"
AGENT_ACTION_RECEIPT_SCHEMA = "ascendop.agent-action-receipt.v1"
AGENT_WORK_LEASE_SCHEMA = "ascendop.agent-work-lease.v1"
AGENT_ITERATION_SCHEMA = "ascendop.agent-iteration.v1"
AGENT_CONTEXT_SNAPSHOT_SCHEMA = "ascendop.agent-context-snapshot.v1"
AGENT_TURN_DELIVERY_SCHEMA = "ascendop.agent-turn-delivery.v1"
AGENT_TURN_DELIVERY_V2_SCHEMA = "ascendop.agent-turn-delivery.v2"
AGENT_TURN_COMPLETION_SCHEMA = "ascendop.agent-turn-completion.v1"
AGENT_OUTPUT_CONTRACT_SCHEMA = "ascendop.agent-output-contract.v1"
AGENT_OUTPUT_SEAL_SCHEMA = "ascendop.agent-output-seal.v1"
AGENT_OUTPUT_PROMOTION_RECEIPT_SCHEMA = (
    "ascendop.agent-output-promotion-receipt.v1"
)
SOLVER_CANDIDATE_PROPOSAL_SCHEMA = "ascendop.solver-candidate-proposal.v1"
SOLVER_CANDIDATE_PROMOTION_RECEIPT_SCHEMA = (
    "ascendop.solver-candidate-promotion-receipt.v1"
)

AGENT_DRIVERS = {
    "codex-ide-task",
    "codex-cli",
    "claude-code-cli",
    "kimi-code-cli",
}
AGENT_ROLES = {"solver", "tester"}
ACTION_STATES = {
    "queued",
    "claimed",
    "running",
    "uncertain",
    "retry-pending",
    "completed",
    "failed",
    "cancelled",
}
AGENT_OUTPUT_KINDS = {
    "pending-evidence-repair",
    "solver-blocker",
    "solver-candidate-proposal",
    "solver-diagnostic-request",
}
class AgentContractError(ValueError):
    pass


def validate_agent_registration(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_REGISTRATION_SCHEMA)
    for field in (
        "agent_id",
        "driver",
        "executable",
        "executable_digest",
        "observed_version",
        "registration_generation",
        "observed_at",
    ):
        _text(raw.get(field), field)
    if raw["driver"] not in AGENT_DRIVERS:
        raise AgentContractError(f"unsupported agent driver: {raw['driver']}")
    _sha256(raw["executable_digest"], "executable_digest")
    capabilities = _object(raw.get("capabilities"), "capabilities")
    for field in ("stream_json", "resume", "structured_output"):
        if not isinstance(capabilities.get(field), bool):
            raise AgentContractError(f"capabilities.{field} must be boolean")
    return dict(raw)


def validate_agent_pool(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_POOL_SCHEMA)
    for field in ("pool_id", "registration_generation"):
        _token(raw.get(field), field)
    if not isinstance(raw.get("enabled"), bool):
        raise AgentContractError("enabled must be boolean")
    roles = _string_list(raw.get("roles"), "roles")
    if not roles or any(role not in AGENT_ROLES for role in roles):
        raise AgentContractError("roles must contain supported Agent roles")
    drivers = _string_list(raw.get("drivers"), "drivers")
    if not drivers or any(driver not in AGENT_DRIVERS for driver in drivers):
        raise AgentContractError("drivers must contain supported Agent drivers")
    required = _object(raw.get("required_capabilities"), "required_capabilities")
    if any(not isinstance(key, str) or not key.strip() for key in required):
        raise AgentContractError("required_capabilities keys must be non-empty text")
    if any(not isinstance(value, bool) for value in required.values()):
        raise AgentContractError("required_capabilities values must be boolean")
    priority = raw.get("priority")
    if not isinstance(priority, int) or isinstance(priority, bool):
        raise AgentContractError("priority must be an integer")
    return dict(raw)


def validate_agent_action(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_ACTION_SCHEMA)
    for field in (
        "action_id",
        "idempotency_key",
        "iteration_id",
        "campaign",
        "operator_id",
        "agent_pool_id",
        "role",
        "workflow_epoch",
        "producer_generation",
        "board_revision",
        "board_digest",
        "runbook_path",
        "runbook_digest",
        "origin_workspace",
        "candidate_version",
        "created_at",
    ):
        _text(raw.get(field), field)
    if raw["role"] not in AGENT_ROLES:
        raise AgentContractError(f"unsupported agent role: {raw['role']}")
    _token(raw["agent_pool_id"], "agent_pool_id")
    for field in ("board_digest", "runbook_digest"):
        _sha256(raw[field], field)
    _relative_path(raw["runbook_path"], "runbook_path")
    _relative_path(raw["origin_workspace"], "origin_workspace")
    _string_list(raw.get("write_scope"), "write_scope", relative_paths=True)
    output_contracts = raw.get("output_contracts")
    if not isinstance(output_contracts, list):
        raise AgentContractError("output_contracts must be a list")
    validated_outputs = [validate_agent_output_contract(item) for item in output_contracts]
    output_ids = [item["output_id"] for item in validated_outputs]
    if len(output_ids) != len(set(output_ids)):
        raise AgentContractError("output_contracts output_id values must be unique")
    isolated_paths = [item["isolated_path"] for item in validated_outputs]
    canonical_paths = [item["canonical_path"] for item in validated_outputs]
    if len(isolated_paths) != len(set(isolated_paths)):
        raise AgentContractError("output_contracts isolated paths must be unique")
    if len(canonical_paths) != len(set(canonical_paths)):
        raise AgentContractError("output_contracts canonical paths must be unique")
    _object(raw.get("candidate_identity"), "candidate_identity")
    budget = _object(raw.get("tool_budget"), "tool_budget")
    max_turn_seconds = budget.get("max_turn_seconds")
    if not isinstance(max_turn_seconds, int) or isinstance(max_turn_seconds, bool):
        raise AgentContractError("tool_budget.max_turn_seconds must be an integer")
    if not 30 <= max_turn_seconds <= 3600:
        raise AgentContractError("tool_budget.max_turn_seconds must be in [30, 3600]")
    preferred_agent_id = raw.get("preferred_agent_id", "")
    if preferred_agent_id:
        _text(preferred_agent_id, "preferred_agent_id")
    return dict(raw)


def validate_agent_output_contract(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_OUTPUT_CONTRACT_SCHEMA)
    _token(raw.get("output_id"), "output_id")
    kind = _text(raw.get("output_kind"), "output_kind")
    if kind not in AGENT_OUTPUT_KINDS:
        raise AgentContractError(f"unsupported Agent output kind: {kind}")
    isolated = _relative_path(raw.get("isolated_path"), "isolated_path")
    if not isolated.startswith(".ascendop-output/"):
        raise AgentContractError(
            "isolated_path must be inside .ascendop-output"
        )
    _relative_path(raw.get("canonical_path"), "canonical_path")
    for field in ("required", "must_change"):
        if not isinstance(raw.get(field), bool):
            raise AgentContractError(f"{field} must be boolean")
    max_bytes = raw.get("max_bytes")
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool):
        raise AgentContractError("max_bytes must be an integer")
    if not 1 <= max_bytes <= 4 * 1024 * 1024:
        raise AgentContractError("max_bytes must be in [1, 4194304]")
    target_before = _object(raw.get("target_before"), "target_before")
    state = _text(target_before.get("state"), "target_before.state")
    if state not in {"present", "absent"}:
        raise AgentContractError("target_before.state must be present or absent")
    before_digest = target_before.get("sha256", "")
    if state == "present":
        _sha256(before_digest, "target_before.sha256")
    elif before_digest not in {"", None}:
        raise AgentContractError("absent target_before must not have a digest")
    identity = _object(raw.get("identity"), "identity")
    if any(not isinstance(key, str) or not key.strip() for key in identity):
        raise AgentContractError("output identity keys must be non-empty text")
    return dict(raw)


def validate_solver_candidate_proposal(raw: Mapping[str, Any]) -> dict[str, Any]:
    expected_fields = {
        "schema",
        "campaign",
        "operator",
        "candidate_version",
        "case_version",
        "base_version",
        "source_before_digest",
        "intent",
        "observed_signal",
        "primary_hypothesis",
        "counter_hypothesis",
        "router_gap",
        "consulted_evidence",
        "optimization_method_decision",
        "skill_feedback",
        "shared_knowledge_decision",
        "changed_source",
        "risks",
        "hardware",
        "created_at",
    }
    missing = sorted(expected_fields - set(raw))
    unknown = sorted(set(raw) - expected_fields)
    if missing or unknown:
        raise AgentContractError(
            "Solver candidate proposal fields do not match schema: "
            f"missing={missing}, unknown={unknown}"
        )
    _schema(raw, SOLVER_CANDIDATE_PROPOSAL_SCHEMA)
    for field in (
        "campaign",
        "operator",
        "candidate_version",
        "case_version",
        "base_version",
        "intent",
        "observed_signal",
        "primary_hypothesis",
        "counter_hypothesis",
        "router_gap",
        "optimization_method_decision",
        "skill_feedback",
        "shared_knowledge_decision",
        "created_at",
    ):
        _text(raw.get(field), field)
    _sha256(raw.get("source_before_digest"), "source_before_digest")
    consulted = _string_list(raw.get("consulted_evidence"), "consulted_evidence")
    if not consulted:
        raise AgentContractError("consulted_evidence must not be empty")
    changed = _string_list(raw.get("changed_source"), "changed_source")
    if not changed:
        raise AgentContractError("changed_source must not be empty")
    risks = _object(raw.get("risks"), "risks")
    for field in ("correctness", "performance", "infrastructure"):
        _text(risks.get(field), f"risks.{field}")
    hardware = _text(raw.get("hardware"), "hardware")
    if hardware not in {"910B2", "910B4", "unknown"}:
        raise AgentContractError("hardware must be 910B2, 910B4, or unknown")
    return dict(raw)


def validate_agent_action_receipt(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_ACTION_RECEIPT_SCHEMA)
    for field in (
        "action_id",
        "iteration_id",
        "agent_id",
        "lease_id",
        "status",
        "started_at",
        "completed_at",
    ):
        _text(raw.get(field), field)
    if raw["status"] not in {"completed", "failed", "cancelled", "uncertain"}:
        raise AgentContractError(f"unsupported agent receipt status: {raw['status']}")
    _object(raw.get("completion"), "completion")
    _string_list(raw.get("artifacts", []), "artifacts", relative_paths=True)
    return dict(raw)


def validate_agent_work_lease(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_WORK_LEASE_SCHEMA)
    for field in (
        "lease_id",
        "lease_token",
        "action_id",
        "iteration_id",
        "operator_id",
        "role",
        "agent_id",
        "state",
        "acquired_at",
        "heartbeat_at",
        "expires_at",
    ):
        _text(raw.get(field), field)
    if raw["role"] not in AGENT_ROLES:
        raise AgentContractError(f"unsupported agent role: {raw['role']}")
    if raw["state"] not in {"active", "released", "expired", "cancelled"}:
        raise AgentContractError(f"unsupported agent lease state: {raw['state']}")
    return dict(raw)


def validate_agent_iteration(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_ITERATION_SCHEMA)
    for field in (
        "iteration_id",
        "operator_id",
        "role",
        "candidate_version",
        "state",
        "created_at",
        "updated_at",
    ):
        _text(raw.get(field), field)
    if raw["role"] not in AGENT_ROLES:
        raise AgentContractError(f"unsupported agent role: {raw['role']}")
    for field in ("source_before_digest", "source_after_digest"):
        value = raw.get(field, "")
        if value:
            _sha256(value, field)
    _string_list(raw.get("artifacts", []), "artifacts", relative_paths=True)
    return dict(raw)


def validate_agent_context_snapshot(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_CONTEXT_SNAPSHOT_SCHEMA)
    for field in (
        "snapshot_id",
        "iteration_id",
        "operator_id",
        "role",
        "board_revision",
        "board_digest",
        "created_at",
    ):
        _text(raw.get(field), field)
    if raw["role"] not in AGENT_ROLES:
        raise AgentContractError(f"unsupported agent role: {raw['role']}")
    _sha256(raw["board_digest"], "board_digest")
    for field in (
        "candidate_identity",
        "gate",
        "recent_results",
        "official_evidence",
        "open_hypotheses",
    ):
        value = raw.get(field)
        if not isinstance(value, (Mapping, list)):
            raise AgentContractError(f"{field} must be an object or list")
    _string_list(raw.get("permitted_operations"), "permitted_operations")
    return dict(raw)


def validate_agent_turn_delivery(raw: Mapping[str, Any]) -> dict[str, Any]:
    schema = str(raw.get("schema") or "")
    if schema not in {AGENT_TURN_DELIVERY_SCHEMA, AGENT_TURN_DELIVERY_V2_SCHEMA}:
        raise AgentContractError(
            "unsupported Agent turn delivery; expected v1 or v2"
        )
    for field in (
        "delivery_id",
        "delivery_key",
        "adapter_id",
        "adapter_generation",
        "target_kind",
        "target_id",
        "workspace",
        "prompt_digest",
        "created_at",
    ):
        _text(raw.get(field), field)
    _token(raw["adapter_id"], "adapter_id")
    _token(raw["adapter_generation"], "adapter_generation")
    _token(raw["target_kind"], "target_kind")
    _relative_path(raw["workspace"], "workspace")
    _sha256(raw["prompt_digest"], "prompt_digest")
    action = validate_agent_action(_object(raw.get("action"), "action"))
    validate_agent_context_snapshot(_object(raw.get("context"), "context"))
    lease = validate_agent_work_lease(_object(raw.get("lease"), "lease"))
    agent = _object(raw.get("agent"), "agent")
    _text(agent.get("agent_id"), "agent.agent_id")
    _text(agent.get("driver"), "agent.driver")
    if agent["driver"] not in AGENT_DRIVERS:
        raise AgentContractError(f"unsupported agent driver: {agent['driver']}")
    _text(raw.get("prompt"), "prompt")
    prompt = str(raw["prompt"])
    if len(prompt.encode("utf-8")) > 4 * 1024 * 1024:
        raise AgentContractError("prompt exceeds the Agent delivery limit")
    if hashlib.sha256(prompt.encode("utf-8")).hexdigest() != raw["prompt_digest"]:
        raise AgentContractError("Agent delivery prompt digest mismatch")
    if schema == AGENT_TURN_DELIVERY_V2_SCHEMA:
        from ascendop_protocol.actor import validate_agent_action_context_v3

        action_context = validate_agent_action_context_v3(
            _object(raw.get("action_context"), "action_context")
        )
        attempt = _object(action_context.get("attempt"), "action_context.attempt")
        expected_key = f"{action['action_id']}:{attempt['attempt_id']}"
        if raw["delivery_key"] != expected_key:
            raise AgentContractError("Agent delivery key does not match its attempt")
        if action_context["action_id"] != action["action_id"]:
            raise AgentContractError("Agent action context identity mismatch")
        if lease["action_id"] != action["action_id"]:
            raise AgentContractError("Agent delivery lease action mismatch")
    return dict(raw)


def agent_turn_delivery_identity(raw: Mapping[str, Any]) -> dict[str, str]:
    """Return the attempt-scoped identity used by external turn carriers."""

    action = _object(raw.get("action"), "action")
    action_id = _token(action.get("action_id"), "action.action_id")
    delivery_key = _text(raw.get("delivery_key"), "delivery_key")
    prefix = f"{action_id}:"
    if not delivery_key.startswith(prefix):
        raise AgentContractError("Agent delivery key does not match its action")
    attempt_id = _token(delivery_key[len(prefix) :], "delivery_key.attempt_id")
    return {
        "action_id": action_id,
        "attempt_id": attempt_id,
        "delivery_key": delivery_key,
        "delivery_marker": (
            f"ASCENDOP_AGENT_ACTION={action_id} ATTEMPT={attempt_id}"
        ),
    }


def validate_agent_turn_completion(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the attempt-scoped terminal envelope produced by a carrier."""

    _schema(raw, AGENT_TURN_COMPLETION_SCHEMA)
    for field in (
        "action_id",
        "attempt_id",
        "delivery_key",
        "delivery_marker",
        "target_id",
        "turn_id",
        "terminal_status",
        "observed_at",
    ):
        _text(raw.get(field), field)
    action_id = _token(raw["action_id"], "action_id")
    attempt_id = _token(raw["attempt_id"], "attempt_id")
    expected_key = f"{action_id}:{attempt_id}"
    if raw["delivery_key"] != expected_key:
        raise AgentContractError("Agent completion delivery key changed")
    expected_marker = f"ASCENDOP_AGENT_ACTION={action_id} ATTEMPT={attempt_id}"
    if raw["delivery_marker"] != expected_marker:
        raise AgentContractError("Agent completion delivery marker changed")
    if raw["terminal_status"] not in {
        "completed",
        "failed",
        "interrupted",
        "cancelled",
        "uncertain",
    }:
        raise AgentContractError(
            f"unsupported Agent terminal status: {raw['terminal_status']}"
        )
    structured = _object(raw.get("structured_result"), "structured_result")
    if structured.get("schema") == "ascendop.agent-action-outcome.v1":
        from ascendop_protocol.actor import validate_agent_action_outcome

        outcome = validate_agent_action_outcome(structured)
        if outcome["action_id"] != action_id:
            raise AgentContractError("Agent completion outcome action identity changed")
        expected_status = (
            "failed" if raw["terminal_status"] == "interrupted" else raw["terminal_status"]
        )
        if outcome["execution_status"] != expected_status:
            raise AgentContractError("Agent completion outcome status changed")
    output_error = raw.get("output_error", "")
    if output_error:
        _text(output_error, "output_error")
        if structured.get("schema") == "ascendop.agent-action-outcome.v1":
            raise AgentContractError(
                "Agent completion cannot contain both a typed outcome and output_error"
            )
    elif raw["terminal_status"] == "completed" and not structured:
        raise AgentContractError(
            "completed Agent turn requires structured_result or output_error"
        )
    return dict(raw)


def _schema(raw: Mapping[str, Any], expected: str) -> None:
    if not isinstance(raw, Mapping) or raw.get("schema") != expected:
        raise AgentContractError(f"unsupported contract; expected {expected}")


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AgentContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AgentContractError(f"{field} must be non-empty text")
    return value.strip()


def _token(value: Any, field: str) -> str:
    text = _text(value, field)
    if not re.fullmatch(r"[A-Za-z0-9._-]+", text):
        raise AgentContractError(f"{field} must be a safe token")
    return text


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise AgentContractError(f"{field} must be SHA-256")
    return text


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    if text.startswith("/") or any(part in {"", ".."} for part in text.split("/")):
        raise AgentContractError(f"{field} must be a bounded relative path")
    return text


def _string_list(
    value: Any,
    field: str,
    *,
    relative_paths: bool = False,
) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise AgentContractError(f"{field} must be a non-empty text list")
    if relative_paths:
        for item in value:
            _relative_path(item, field)
    return list(value)
