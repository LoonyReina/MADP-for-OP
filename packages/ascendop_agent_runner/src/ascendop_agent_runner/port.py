from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from ascendop_protocol.actor import (
    AgentPortError,
    NativeSessionObservation,
    NativeTurnBinding,
    native_turn_outcome,
)

from .drivers import AgentDriver, DriverResult


class CliAgentExecutionPort:
    """AgentExecutionPort over one bounded CLI driver invocation."""

    def __init__(
        self,
        *,
        root: Path,
        driver: AgentDriver,
        run_root: Path,
        timeout_seconds: int,
        heartbeat: Callable[[], None],
        resume_session_id: str = "",
    ) -> None:
        self.root = root.resolve()
        self.driver = driver
        self.port_id = f"cli-driver:{driver.driver_id}"
        self.run_root = run_root
        self.timeout_seconds = int(timeout_seconds)
        self.heartbeat_callback = heartbeat
        self.resume_session_id = str(resume_session_id)
        self._binding: NativeTurnBinding | None = None
        self._result: DriverResult | None = None

    def observe_session(
        self, *, native_session_id: str
    ) -> NativeSessionObservation:
        details = dict(self.driver.heartbeat(native_session_id))
        state = str(details.get("status") or details.get("state") or "running")
        normalized = "terminal" if state in {
            "completed",
            "failed",
            "cancelled",
        } else "running"
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=native_session_id,
            state=normalized,
            details=details,
        )

    def deliver_action(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
        prompt: str,
        workspace: Path,
        output_schema: Mapping[str, Any],
    ) -> NativeTurnBinding:
        del output_schema
        existing = self.reconcile_delivery(
            action_id=action_id,
            idempotency_key=idempotency_key,
            native_session_id=native_session_id,
        )
        if existing is not None and not self.resume_session_id:
            return existing
        if self.resume_session_id:
            result = self.driver.resume(
                session_id=self.resume_session_id,
                prompt=prompt,
                workspace=workspace,
                run_root=self.run_root,
                timeout_seconds=self.timeout_seconds,
                heartbeat=self.heartbeat_callback,
            )
            if result.session_id and result.session_id != self.resume_session_id:
                raise AgentPortError(
                    "CLI resume returned a different native session identity"
                )
        else:
            result = self.driver.start(
                prompt=prompt,
                workspace=workspace,
                run_root=self.run_root,
                timeout_seconds=self.timeout_seconds,
                heartbeat=self.heartbeat_callback,
            )
        turn_id = str(result.session_id or native_session_id)
        if not turn_id:
            raise AgentPortError("CLI driver returned no native session identity")
        self._result = result
        self._binding = NativeTurnBinding(
            port_id=self.port_id,
            action_id=action_id,
            idempotency_key=idempotency_key,
            native_session_id=turn_id,
            native_turn_id=turn_id,
            delivery_state="delivered",
            observed_at=_utc_now(),
            details={"driver_id": self.driver.driver_id},
        )
        return self._binding

    def reconcile_delivery(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
    ) -> NativeTurnBinding | None:
        del native_session_id
        if (
            self._binding is not None
            and self._binding.action_id == action_id
            and self._binding.idempotency_key == idempotency_key
        ):
            return NativeTurnBinding(
                port_id=self._binding.port_id,
                action_id=self._binding.action_id,
                idempotency_key=self._binding.idempotency_key,
                native_session_id=self._binding.native_session_id,
                native_turn_id=self._binding.native_turn_id,
                delivery_state="reconciled",
                observed_at=_utc_now(),
                details=self._binding.details,
            )
        return None

    def cancel_turn(
        self,
        *,
        native_session_id: str,
        native_turn_id: str,
    ) -> NativeSessionObservation:
        details = dict(self.driver.cancel(native_session_id))
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            state="running",
            details={**details, "cancellation_requested": True},
        )

    def collect_outcome(
        self,
        *,
        action_id: str,
        native_session_id: str,
        native_turn_id: str,
    ) -> dict[str, Any]:
        if self._result is None or self._binding is None:
            raise AgentPortError("CLI turn result is unavailable")
        if native_turn_id != self._binding.native_turn_id:
            raise AgentPortError("CLI native turn identity changed")
        completion = dict(self.driver.collect(self._result))
        artifacts = [
            self._relative(self._result.raw_output_path),
            self._relative(self._result.stderr_path),
        ]
        return native_turn_outcome(
            action_id=action_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            terminal_status=self._result.status,
            structured_result=completion,
            artifact_refs=artifacts,
            observed_at=_utc_now(),
        )

    def _relative(self, path: Path) -> str:
        resolved = path.resolve()
        if self.root not in resolved.parents:
            raise AgentPortError("CLI driver artifact is outside the MADP root")
        return resolved.relative_to(self.root).as_posix()


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


__all__ = ["CliAgentExecutionPort"]
