from __future__ import annotations

import time
from typing import Any

from ascendop_daemon.control_plane.control_database import (
    ACTIVE_ATTEMPT_STATES,
    OUTBOX_ACTIVE_STATES,
    SCHEMA_VERSION,
)
from ascendop_daemon.runtime.application_support import (
    assert_live_generation_compatible,
    materialize_task_profiles,
    record_service_heartbeats,
)
from ascendop_daemon.runtime.control import read_stop_request


class ApplicationLifecycle:
    def initialize(self) -> dict[str, Any]:
        reconciliation = self.database.reconcile(self.config, self.registry)
        profiles = self._materialize_task_profiles()
        self._assert_live_generation_compatible()
        self._heartbeat(
            state="stopped" if read_stop_request(self.paths.root) else "ready"
        )
        return {
            "schema": "ascendop.daemon-initialize.v4",
            "generation": self.generation,
            "database_schema": SCHEMA_VERSION,
            "variable_registry": {
                "path": str(self.policy.registry.path),
                "version": self.policy.registry.registry_version,
                "digest": self.policy.registry.digest,
                "semantic_digest": self.policy.registry.digest,
                "artifact_sha256": self.variable_registry_artifact_sha256,
            },
            "task_profiles": profiles,
            "reconciliation": reconciliation,
        }

    def run(
        self,
        *,
        interval_seconds: float = 1.0,
        max_cycles: int = 0,
    ) -> dict[str, Any]:
        self.initialize()
        cycles = 0
        last: dict[str, Any] = {}
        try:
            while True:
                last = self.run_once(nonblocking_dispatch=True)
                cycles += 1
                if max_cycles and cycles >= max_cycles:
                    break
                if (
                    last["stop_fenced"]
                    and not last["drain_work"]
                    and not bool(last["dispatch"].get("active"))
                ):
                    break
                time.sleep(max(0.05, float(interval_seconds)))
        finally:
            self._endpoint_dispatch.shutdown()
            self._endpoint_reconciliation.shutdown()
            self._heartbeat(state="stopped")
        return {**last, "cycles": cycles, "outcome": "stopped"}

    def has_drain_work(self) -> bool:
        status = self.database.status(event_limit=0)
        return bool(
            any(
                int(status["attempts"].get(state, 0))
                for state in ACTIVE_ATTEMPT_STATES
            )
            or any(
                int(status["outbox"].get(state, 0))
                for state in OUTBOX_ACTIVE_STATES
            )
            or int(status["returns"].get("return-ready", 0))
        )

    def status(self) -> dict[str, Any]:
        value = self.database.status()
        value.update(
            {
                "schema": "ascendop.daemon-status.v4",
                "generation": self.generation,
                "stop_request": read_stop_request(self.paths.root) or {},
            }
        )
        return value

    def _assert_live_generation_compatible(self) -> None:
        assert_live_generation_compatible(self.database, self.generation)

    def _heartbeat(
        self,
        *,
        state: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        record_service_heartbeats(
            self.database,
            generation=self.generation,
            variable_registry_digest=self.policy.registry.digest,
            variable_registry_artifact_sha256=(
                self.variable_registry_artifact_sha256
            ),
            state=state,
            details=details,
        )

    def _materialize_task_profiles(self) -> list[dict[str, str]]:
        return materialize_task_profiles(self.paths.root, self.database)
