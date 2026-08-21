from __future__ import annotations

from typing import Any, Mapping

from ascendop_protocol.actor import validate_actor_action_envelope


CONTROL_COMMAND_SCHEMA = "ascendop.control-command.v1"
CONTROL_COMMAND_RECEIPT_SCHEMA = "ascendop.control-command-receipt.v1"
PUBLIC_RESOURCE_SCHEMA = "ascendop.public-resource.v1"
CONTROL_EVENT_SCHEMA = "ascendop.control-event.v1"
OPERATOR_WORKFLOW_PROJECTION_SCHEMA = (
    "ascendop.operator-workflow-projection.v1"
)
WORKFLOW_TRACE_PROJECTION_SCHEMA = "ascendop.workflow-trace-projection.v1"
MANAGER_NOTIFICATION_SCHEMA = "ascendop.manager-notification.v1"

COMMAND_KINDS = {
    "agent.register",
    "agent.bind",
    "agent.cancel-action",
    "agent.debug-pin",
    "endpoint.drain",
    "endpoint.resume",
    "manager.flow-start",
    "manager.flow-pause",
    "manager.flow-resume",
    "manager.flow-stop",
    "manager.request-user-decision",
}
COMMAND_CAPABILITIES = {
    "agent.register": "admin",
    "agent.bind": "admin",
    "agent.cancel-action": "operator",
    "agent.debug-pin": "operator",
    "endpoint.drain": "operator",
    "endpoint.resume": "operator",
    "manager.flow-start": "operator",
    "manager.flow-pause": "operator",
    "manager.flow-resume": "operator",
    "manager.flow-stop": "operator",
    "manager.request-user-decision": "operator",
}


class ManagementContractError(ValueError):
    pass


def validate_control_command(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, CONTROL_COMMAND_SCHEMA)
    for field in (
        "command_id",
        "idempotency_key",
        "command_kind",
        "actor_id",
        "required_capability",
        "created_at",
    ):
        _text(raw.get(field), field)
    if raw["command_kind"] not in COMMAND_KINDS:
        raise ManagementContractError(
            f"unsupported control command: {raw['command_kind']}"
        )
    expected_capability = COMMAND_CAPABILITIES[raw["command_kind"]]
    if raw["required_capability"] != expected_capability:
        raise ManagementContractError(
            f"{raw['command_kind']} requires {expected_capability} capability"
        )
    parameters = _object(raw.get("parameters"), "parameters")
    if str(raw["command_kind"]).startswith("manager."):
        actor_action = validate_actor_action_envelope(
            _object(raw.get("actor_action"), "actor_action")
        )
        if actor_action["action_kind"] != raw["command_kind"]:
            raise ManagementContractError(
                "manager command kind must match actor action kind"
            )
        if actor_action["effective_role"] != "manager":
            raise ManagementContractError("manager command requires manager role")
        if actor_action["principal_id"] != raw["actor_id"]:
            raise ManagementContractError(
                "manager command actor must own actor action"
            )
        if actor_action["payload"] != parameters:
            raise ManagementContractError(
                "manager command parameters must equal immutable actor payload"
            )
    return dict(raw)


def validate_control_command_receipt(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, CONTROL_COMMAND_RECEIPT_SCHEMA)
    for field in ("command_id", "status", "completed_at"):
        _text(raw.get(field), field)
    if raw["status"] not in {"completed", "failed", "rejected"}:
        raise ManagementContractError(
            f"unsupported control command status: {raw['status']}"
        )
    _object(raw.get("result"), "result")
    return dict(raw)


def validate_public_resource(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, PUBLIC_RESOURCE_SCHEMA)
    for field in ("resource_type", "resource_id", "revision", "observed_at"):
        _text(raw.get(field), field)
    _object(raw.get("attributes"), "attributes")
    return dict(raw)


def validate_control_event(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, CONTROL_EVENT_SCHEMA)
    sequence = raw.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ManagementContractError("sequence must be a positive integer")
    for field in ("event_at", "event_type", "entity_type", "entity_id"):
        _text(raw.get(field), field)
    _object(raw.get("payload"), "payload")
    return dict(raw)


def validate_operator_workflow_projection(
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    _schema(raw, OPERATOR_WORKFLOW_PROJECTION_SCHEMA)
    for field in (
        "operator_id",
        "agent_phase",
        "candidate_phase",
        "test_phase",
        "endpoint_phase",
        "recovery_phase",
        "headline_phase",
        "next_owner",
        "observed_at",
    ):
        _text(raw.get(field), field)
    _object(raw.get("gate"), "gate")
    blocker = raw.get("primary_blocker")
    if blocker is not None:
        value = _object(blocker, "primary_blocker")
        for field in ("kind", "code", "details", "source_ref"):
            _text(value.get(field), f"primary_blocker.{field}")
    _string_list(raw.get("allowed_commands"), "allowed_commands")
    _object(raw.get("causative_evidence"), "causative_evidence")
    sequence = raw.get("last_event_sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise ManagementContractError(
            "last_event_sequence must be a non-negative integer"
        )
    _validate_freshness(raw.get("freshness"))
    return dict(raw)


def validate_workflow_trace_projection(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, WORKFLOW_TRACE_PROJECTION_SCHEMA)
    for field in ("trace_id", "operator_id", "observed_at"):
        _text(raw.get(field), field)
    nodes = raw.get("nodes")
    if not isinstance(nodes, list):
        raise ManagementContractError("nodes must be a list")
    for index, node in enumerate(nodes):
        value = _object(node, f"nodes[{index}]")
        for field in ("kind", "id", "state", "observed_at"):
            _text(value.get(field), f"nodes[{index}].{field}")
    links = raw.get("links")
    if not isinstance(links, list):
        raise ManagementContractError("links must be a list")
    for index, link in enumerate(links):
        value = _object(link, f"links[{index}]")
        for field in ("from", "to", "relation"):
            _text(value.get(field), f"links[{index}].{field}")
    _string_list(raw.get("gaps"), "gaps")
    sequence = raw.get("last_event_sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise ManagementContractError(
            "last_event_sequence must be a non-negative integer"
        )
    _validate_freshness(raw.get("freshness"))
    return dict(raw)


def validate_manager_notification(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, MANAGER_NOTIFICATION_SCHEMA)
    for field in (
        "notification_id",
        "notification_kind",
        "flow_id",
        "operator_id",
        "summary",
        "state",
        "created_at",
    ):
        _text(raw.get(field), field)
    if raw["notification_kind"] not in {
        "user_decision",
        "external_hold",
        "capability_gap",
    }:
        raise ManagementContractError("unsupported Manager notification kind")
    if not isinstance(raw.get("requires_response"), bool):
        raise ManagementContractError("requires_response must be boolean")
    _string_list(raw.get("evidence_refs"), "evidence_refs")
    _string_list(raw.get("allowed_commands"), "allowed_commands")
    return dict(raw)


def _validate_freshness(value: Any) -> None:
    freshness = _object(value, "freshness")
    if freshness.get("state") not in {"fresh", "stale", "unknown"}:
        raise ManagementContractError("freshness.state is invalid")
    age = freshness.get("age_seconds")
    if not isinstance(age, int) or isinstance(age, bool) or age < 0:
        raise ManagementContractError("freshness.age_seconds must be non-negative")
    _text(freshness.get("source_updated_at"), "freshness.source_updated_at")


def _string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ManagementContractError(f"{field} must be a list of non-empty text")
    return list(value)


def _schema(raw: Mapping[str, Any], expected: str) -> None:
    if not isinstance(raw, Mapping) or raw.get("schema") != expected:
        raise ManagementContractError(f"unsupported contract; expected {expected}")


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ManagementContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManagementContractError(f"{field} must be non-empty text")
    return value.strip()
