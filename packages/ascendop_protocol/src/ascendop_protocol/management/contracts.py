from __future__ import annotations

from typing import Any, Mapping


CONTROL_COMMAND_SCHEMA = "ascendop.control-command.v1"
CONTROL_COMMAND_RECEIPT_SCHEMA = "ascendop.control-command-receipt.v1"
PUBLIC_RESOURCE_SCHEMA = "ascendop.public-resource.v1"
CONTROL_EVENT_SCHEMA = "ascendop.control-event.v1"

COMMAND_KINDS = {
    "agent.register",
    "agent.bind",
    "agent.cancel-action",
    "agent.debug-pin",
    "endpoint.drain",
    "endpoint.resume",
}
COMMAND_CAPABILITIES = {
    "agent.register": "admin",
    "agent.bind": "admin",
    "agent.cancel-action": "operator",
    "agent.debug-pin": "operator",
    "endpoint.drain": "operator",
    "endpoint.resume": "operator",
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
    _object(raw.get("parameters"), "parameters")
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
