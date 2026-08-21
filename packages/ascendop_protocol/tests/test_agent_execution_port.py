from pathlib import Path

import pytest

from ascendop_protocol.actor import (
    AgentExecutionPort,
    AgentPortError,
    NativeSessionObservation,
    NativeTurnBinding,
    native_turn_outcome,
    validate_port,
)


class _Port:
    port_id = "test-port"

    def observe_session(self, *, native_session_id: str):
        return NativeSessionObservation(self.port_id, native_session_id, "idle")

    def deliver_action(self, **kwargs):
        return NativeTurnBinding(
            self.port_id,
            kwargs["action_id"],
            kwargs["idempotency_key"],
            kwargs["native_session_id"],
            "turn-1",
            "delivered",
            "2026-08-17T00:00:00+00:00",
        )

    def reconcile_delivery(self, **kwargs):
        return None

    def cancel_turn(self, *, native_session_id: str, native_turn_id: str):
        return NativeSessionObservation(
            self.port_id, native_session_id, "running", native_turn_id
        )

    def collect_outcome(self, **kwargs):
        return native_turn_outcome(
            action_id=kwargs["action_id"],
            native_session_id=kwargs["native_session_id"],
            native_turn_id=kwargs["native_turn_id"],
            terminal_status="completed",
            structured_result={"summary": "done"},
            observed_at="2026-08-17T00:00:00+00:00",
        )


def test_agent_execution_port_is_structural_and_has_no_workflow_commit() -> None:
    port = validate_port(_Port())

    assert isinstance(port, AgentExecutionPort)
    assert set(AgentExecutionPort.__dict__) >= {
        "observe_session",
        "deliver_action",
        "reconcile_delivery",
        "cancel_turn",
        "collect_outcome",
    }
    assert not any(
        name in AgentExecutionPort.__dict__
        for name in ("complete_action", "acquire_lease", "promote")
    )


def test_native_turn_binding_rejects_ambiguous_delivery() -> None:
    with pytest.raises(AgentPortError, match="delivery state"):
        NativeTurnBinding(
            "test",
            "action",
            "key",
            "session",
            "turn",
            "uncertain",
            "2026-08-17T00:00:00+00:00",
        )


def test_native_turn_outcome_helper_uses_public_validator() -> None:
    outcome = native_turn_outcome(
        action_id="action-1",
        native_session_id="session-1",
        native_turn_id="turn-1",
        terminal_status="completed",
        structured_result={"summary": "done"},
        artifact_refs=["runs/action-1/output.json"],
        observed_at="2026-08-17T00:00:00+00:00",
    )

    assert outcome["schema"] == "ascendop.native-turn-outcome.v1"
    assert outcome["telemetry"] == {"usage": {}, "skills": {}}
