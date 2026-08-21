from __future__ import annotations

from typing import Any

from ascendop_protocol.actor import validate_role_binding
from ascendop_protocol.agent import validate_agent_registration

from ascendop_daemon.registry.models import SystemRegistryError
from ascendop_daemon.registry.topology_parser import list_value, object_row


def parse_actor_registrations(raw: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    registrations: list[dict[str, Any]] = []
    for value in list_value(raw, "actor_registrations"):
        try:
            registrations.append(
                validate_agent_registration(object_row(value, "actor registration"))
            )
        except ValueError as exc:
            raise SystemRegistryError(f"invalid actor registration: {exc}") from exc
    _unique(registrations, "agent_id", "actor registration")
    return tuple(registrations)


def parse_role_bindings(
    raw: dict[str, Any],
    *,
    actor_registrations: tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    registrations = {str(value["agent_id"]) for value in actor_registrations}
    bindings: list[dict[str, Any]] = []
    for value in list_value(raw, "role_bindings"):
        try:
            binding = validate_role_binding(object_row(value, "role binding"))
        except ValueError as exc:
            raise SystemRegistryError(f"invalid role binding: {exc}") from exc
        if binding["agent_registration_id"] not in registrations:
            raise SystemRegistryError(
                "role binding references an actor registration outside the registry: "
                f"{binding['agent_registration_id']}"
            )
        bindings.append(binding)
    _unique(bindings, "role_binding_id", "role binding")
    return tuple(bindings)


def _unique(values: list[dict[str, Any]], field: str, label: str) -> None:
    identities = [str(value[field]) for value in values]
    if len(identities) != len(set(identities)):
        raise SystemRegistryError(f"duplicate {label} {field}")


__all__ = ["parse_actor_registrations", "parse_role_bindings"]
