from __future__ import annotations
import hashlib
import json
from pathlib import Path
import time
from typing import Any
from ascendop_daemon.control_plane.control_database import (
    ACTIVE_ATTEMPT_STATES,
    OUTBOX_ACTIVE_STATES,
    SCHEMA_VERSION,
    ControlDatabase,
)
from ascendop_daemon.control_plane.endpoint_dispatcher import build_dispatcher_pool
from ascendop_daemon.control_plane.retry_controller import RetryController
from ascendop_daemon.automation.assistant_coordinator import AssistantCoordinator
from ascendop_daemon.automation.official_progress import OfficialProgressPublisher
from ascendop_daemon.automation.session_gate import SessionGateCoordinator
from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.runtime.config_loader import load_config
from ascendop_daemon.runtime.control import read_stop_request
from ascendop_daemon.runtime.application_support import (
    ApplicationPaths,
    LOCAL_SERVICES,
    assert_live_generation_compatible,
    materialize_task_profiles,
    record_service_heartbeats,
)
from ascendop_daemon.runtime.policy_registry import RuntimePolicy
from ascendop_daemon.runtime.periodic_worker import (
    EndpointDispatchWorkerPool,
    EndpointReconciliationPool,
)
from ascendop_daemon.runtime.endpoint_reconciliation import (
    EndpointReconciliationService,
)
from ascendop_daemon.runtime.release_identity import source_generation
from ascendop_daemon.runtime.submit_intake import SubmitIntake

class V3Application:
    def __init__(self, paths: ApplicationPaths) -> None:
        self.paths = paths
        self.config = load_config(paths.config)
        self.registry = SystemRegistry.load(paths.registry)
        self.database = ControlDatabase(paths.database)
        self.policy = RuntimePolicy.load(
            paths.root,
            self.config.policy,
            database_schema=SCHEMA_VERSION,
        )
        self.variable_registry_artifact_sha256 = hashlib.sha256(
            self.policy.registry.path.read_bytes()
        ).hexdigest()
        self.generation = source_generation(
            paths.root,
            policy_digest=self.policy.registry.digest,
        )
        self.retry_controller = RetryController(
            self.database,
            code_generation=self.generation,
            max_transport_retries=int(
                self.policy.get("retry.max_transport_retries")
            ),
        )
        self.dispatchers = build_dispatcher_pool(
            paths.root,
            self.database,
            self.registry,
            capacity=int(self.policy.get("host.cache_hit_concurrency")),
            command_timeout_seconds=int(
                self.policy.get("transport.git_operation_timeout_seconds")
            ),
            max_delivery_attempts=int(
                self.policy.get("retry.max_transport_retries")
            ),
        )
        self.assistant = AssistantCoordinator(paths.root, self.database)
        self.official_progress = OfficialProgressPublisher(
            root=paths.root,
            database=self.database,
            output_path=Path(
                str(self.policy.get("automation.official_progress_path"))
            ),
            profile_glob=str(
                self.policy.get("automation.official_progress_profile_glob")
            ),
        )
        self.session_gate = SessionGateCoordinator(
            root=paths.root,
            database=self.database,
            config=self.config,
            assistant_target_id=str(
                self.policy.get("automation.workflow_relay_target_id")
            ),
        )
        self.endpoint_reconciliation = EndpointReconciliationService(
            root=paths.root,
            database=self.database,
            registry=self.registry,
            config=self.config,
            policy=self.policy,
            generation=self.generation,
        )
        self._endpoint_dispatch = EndpointDispatchWorkerPool(
            dispatchers=tuple(self.dispatchers.dispatchers),
        )
        self._endpoint_reconciliation = EndpointReconciliationPool(
            endpoint_ids=tuple(
                endpoint.endpoint_id
                for endpoint in self.registry.endpoints
                if endpoint.enabled
            ),
            callback=self._reconcile_endpoints_resident,
            enabled=bool(
                self.config.policy.get(
                    "control_plane_node_reconciler_enabled",
                    False,
                )
            ),
            interval_seconds=float(
                self.config.policy.get(
                    "control_plane_remote_node_refresh_interval_seconds",
                    60,
                )
                or 60
            ),
        )
        self.submit_intake = SubmitIntake(
            root=paths.root,
            config=self.config,
            database=self.database,
            registry=self.registry,
            code_generation=self.generation,
        )

    def initialize(self) -> dict[str, Any]:
        reconciliation = self.database.reconcile(self.config, self.registry)
        profiles = self._materialize_task_profiles()
        self._assert_live_generation_compatible()
        self._heartbeat(state="stopped" if read_stop_request(self.paths.root) else "ready")
        return {
            "schema": "ascendop.daemon-initialize.v3",
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

    def run_once(
        self,
        *,
        nonblocking_dispatch: bool = False,
    ) -> dict[str, Any]:
        stop_request = read_stop_request(self.paths.root)
        self._assert_live_generation_compatible()
        drain_before = self.has_drain_work()
        endpoint_reconciliation = (
            self._endpoint_reconciliation.poll(schedule=not bool(stop_request))
            if nonblocking_dispatch
            else {"state": "not-scheduled", "active": False}
        )
        workflow_result_recovery = self.dispatchers.reconcile_results(limit=1)
        intake = (
            {"state": "stopped", "generated_count": 0}
            if stop_request
            else self.submit_intake.run_once()
        )
        dispatch = (
            self._endpoint_dispatch.poll(
                allow_claims=not bool(stop_request),
                schedule=not bool(stop_request) or drain_before,
            )
            if nonblocking_dispatch
            else self.dispatchers.run_once(
                allow_claims=not bool(stop_request)
            )
        )
        retry = self.retry_controller.run_once()
        session_gate = (
            {"board_rows": 0, "eligible_count": 0, "actions": [], "errors": [], "stopped": True}
            if stop_request
            else self.session_gate.run_once()
        )
        official_progress = self._publish_official_progress()
        automation = (
            {"source_count": 0, "actions": [], "errors": [], "stopped": True}
            if stop_request
            else self.assistant.run_once()
        )
        self._heartbeat(
            state="draining" if stop_request else "ready",
            details={
                "assistant_trigger": automation,
                "official_progress": official_progress,
                "submit_intake": intake,
                "endpoint_dispatch": dispatch,
                "endpoint_reconciliation": endpoint_reconciliation,
                "workflow_result_recovery": workflow_result_recovery,
                "retry_controller": retry,
                "session_gate": session_gate,
            },
        )
        return {
            "schema": "ascendop.daemon-tick.v3",
            "generation": self.generation,
            "stop_fenced": bool(stop_request),
            "stop_request": stop_request or {},
            "dispatch": dispatch,
            "intake": intake,
            "retry": retry,
            "session_gate": session_gate,
            "endpoint_reconciliation": endpoint_reconciliation,
            "workflow_result_recovery": workflow_result_recovery,
            "automation": automation,
            "official_progress": official_progress,
            "drain_work": self.has_drain_work(),
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
            any(int(status["attempts"].get(state, 0)) for state in ACTIVE_ATTEMPT_STATES)
            or any(int(status["outbox"].get(state, 0)) for state in OUTBOX_ACTIVE_STATES)
            or int(status["returns"].get("return-ready", 0))
        )

    def status(self) -> dict[str, Any]:
        value = self.database.status()
        value.update(
            {
                "schema": "ascendop.daemon-status.v3",
                "generation": self.generation,
                "stop_request": read_stop_request(self.paths.root) or {},
            }
        )
        return value

    def reconcile_endpoints(
        self,
        *,
        endpoint_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        self.database.reconcile(self.config, self.registry)
        return self._reconcile_endpoints(
            endpoint_ids=endpoint_ids,
            allow_trusted_lease_probe=True,
        )

    def _reconcile_endpoints_resident(
        self,
        endpoint_ids: set[str],
    ) -> dict[str, Any]:
        return self._reconcile_endpoints(
            endpoint_ids=endpoint_ids,
            allow_trusted_lease_probe=False,
        )

    def _reconcile_endpoints(
        self,
        *,
        endpoint_ids: set[str] | None,
        allow_trusted_lease_probe: bool,
    ) -> dict[str, Any]:
        return self.endpoint_reconciliation.run_once(
            endpoint_ids=endpoint_ids,
            allow_trusted_lease_probe=allow_trusted_lease_probe,
        )

    def accept_endpoint(self, endpoint_id: str) -> dict[str, Any]:
        return self.endpoint_reconciliation.accept(endpoint_id)

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

    def _publish_official_progress(self) -> dict[str, Any]:
        try:
            return self.official_progress.run_once()
        except Exception as exc:
            return {
                "path": str(self.official_progress.output_path),
                "operator_count": 0,
                "candidate_count": 0,
                "held_count": 0,
                "errors": [{"operator_id": "*", "error": str(exc)}],
            }
