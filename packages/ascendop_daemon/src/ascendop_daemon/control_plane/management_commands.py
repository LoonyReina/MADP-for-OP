from __future__ import annotations

from typing import Any, Mapping

from ascendop_control.application import ControlCommandRejected
from ascendop_control.storage.errors import ControlRepositoryError

from ascendop_daemon.storage.control_types import ControlDatabaseError


class DaemonControlCommandHandler:
    """Translate typed management commands into daemon-owned use cases."""

    def __init__(self, database: Any) -> None:
        self.database = database

    def handle(self, command: Mapping[str, Any]) -> Mapping[str, Any]:
        kind = str(command["command_kind"])
        parameters = _mapping(command.get("parameters"), "parameters")
        try:
            if kind == "agent.register":
                return self._register_agent(parameters)
            if kind == "agent.bind":
                return self._bind_agent(parameters)
            if kind == "agent.cancel-action":
                return self.database.cancel_agent_action(
                    action_id=_text(parameters, "action_id"),
                    actor_id=str(command["actor_id"]),
                    reason=_text(parameters, "reason"),
                )
            if kind == "agent.debug-pin":
                return self.database.pin_agent_action(
                    action_id=_text(parameters, "action_id"),
                    agent_id=_text(parameters, "agent_id"),
                    expires_at=_text(parameters, "expires_at"),
                )
            if kind in {"endpoint.drain", "endpoint.resume"}:
                return self.database.set_endpoint_runtime_drain(
                    endpoint_id=_text(parameters, "endpoint_id"),
                    draining=kind == "endpoint.drain",
                    command_id=str(command["command_id"]),
                    actor_id=str(command["actor_id"]),
                    reason=_text(parameters, "reason"),
                )
        except (ControlDatabaseError, ControlRepositoryError, ValueError) as exc:
            raise ControlCommandRejected(str(exc)) from exc
        raise ControlCommandRejected(f"unsupported command kind: {kind}")

    def _register_agent(self, parameters: Mapping[str, Any]) -> Mapping[str, Any]:
        registration = _mapping(parameters.get("registration"), "registration")
        return self.database.register_agent(
            registration,
            health_state=str(parameters.get("health_state") or "ready"),
            boot_id=str(parameters.get("boot_id") or ""),
            lease_seconds=_integer(parameters, "lease_seconds", default=30),
        )

    def _bind_agent(self, parameters: Mapping[str, Any]) -> Mapping[str, Any]:
        operator_id = _text(parameters, "operator_id")
        known = {
            str(row["operator_id"])
            for row in self.database.operator_registrations()
        }
        if operator_id not in known:
            raise ControlCommandRejected(
                f"operator is not registered: {operator_id}"
            )
        return self.database.bind_agent(
            operator_id=operator_id,
            role=_text(parameters, "role"),
            agent_id=_text(parameters, "agent_id"),
            enabled=_boolean(parameters, "enabled", default=True),
            priority=_integer(parameters, "priority", default=100),
        )


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ControlCommandRejected(f"{field} must be an object")
    return value


def _text(value: Mapping[str, Any], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result.strip():
        raise ControlCommandRejected(f"{field} must be non-empty text")
    return result.strip()


def _integer(value: Mapping[str, Any], field: str, *, default: int) -> int:
    result = value.get(field, default)
    if isinstance(result, bool) or not isinstance(result, int):
        raise ControlCommandRejected(f"{field} must be an integer")
    return result


def _boolean(value: Mapping[str, Any], field: str, *, default: bool) -> bool:
    result = value.get(field, default)
    if not isinstance(result, bool):
        raise ControlCommandRejected(f"{field} must be boolean")
    return result
