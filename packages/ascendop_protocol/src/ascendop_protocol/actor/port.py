from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

from .contracts import validate_native_turn_outcome


class AgentPortError(RuntimeError):
    """The native runtime violated the AgentExecutionPort contract."""


class AgentTurnPending(AgentPortError):
    """The native turn exists but has not reached a terminal state."""


@dataclass(frozen=True)
class NativeSessionObservation:
    port_id: str
    native_session_id: str
    state: str
    native_turn_id: str = ""
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.port_id.strip() or not self.native_session_id.strip():
            raise AgentPortError("session observation requires port and session identity")
        if self.state not in {
            "missing",
            "idle",
            "running",
            "terminal",
            "unavailable",
        }:
            raise AgentPortError(f"unsupported native session state: {self.state}")


@dataclass(frozen=True)
class NativeTurnBinding:
    port_id: str
    action_id: str
    idempotency_key: str
    native_session_id: str
    native_turn_id: str
    delivery_state: str
    observed_at: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        required = (
            self.port_id,
            self.action_id,
            self.idempotency_key,
            self.native_session_id,
            self.native_turn_id,
            self.observed_at,
        )
        if any(not value.strip() for value in required):
            raise AgentPortError("native turn binding contains an empty identity")
        if self.delivery_state not in {"delivered", "reconciled"}:
            raise AgentPortError(
                f"unsupported native delivery state: {self.delivery_state}"
            )


@runtime_checkable
class AgentExecutionPort(Protocol):
    """Adapter-neutral native turn lifecycle.

    Implementations may transport prompts and observe native sessions, but they
    must never acquire workflow leases or commit terminal workflow state.
    """

    port_id: str

    def observe_session(
        self, *, native_session_id: str
    ) -> NativeSessionObservation: ...

    def deliver_action(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
        prompt: str,
        workspace: Path,
        output_schema: Mapping[str, Any],
    ) -> NativeTurnBinding: ...

    def reconcile_delivery(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
    ) -> NativeTurnBinding | None: ...

    def cancel_turn(
        self,
        *,
        native_session_id: str,
        native_turn_id: str,
    ) -> NativeSessionObservation: ...

    def collect_outcome(
        self,
        *,
        action_id: str,
        native_session_id: str,
        native_turn_id: str,
    ) -> dict[str, Any]: ...


def validate_port(port: AgentExecutionPort) -> AgentExecutionPort:
    if not isinstance(port, AgentExecutionPort):
        raise AgentPortError("runtime does not implement AgentExecutionPort")
    if not str(port.port_id).strip():
        raise AgentPortError("AgentExecutionPort requires a stable port_id")
    return port


def native_turn_outcome(
    *,
    action_id: str,
    native_session_id: str,
    native_turn_id: str,
    terminal_status: str,
    structured_result: Mapping[str, Any],
    artifact_refs: list[str] | tuple[str, ...] = (),
    usage_telemetry: Mapping[str, Any] | None = None,
    skill_telemetry: Mapping[str, Any] | None = None,
    observed_at: str,
) -> dict[str, Any]:
    return validate_native_turn_outcome(
        {
            "schema": "ascendop.native-turn-outcome.v1",
            "action_id": action_id,
            "native_session_id": native_session_id,
            "native_turn_id": native_turn_id,
            "terminal_status": terminal_status,
            "structured_result": dict(structured_result),
            "artifact_refs": list(artifact_refs),
            "telemetry": {
                "usage": dict(usage_telemetry or {}),
                "skills": dict(skill_telemetry or {}),
            },
            "observed_at": observed_at,
        }
    )


__all__ = [
    "AgentExecutionPort",
    "AgentPortError",
    "AgentTurnPending",
    "NativeSessionObservation",
    "NativeTurnBinding",
    "native_turn_outcome",
    "validate_port",
]
