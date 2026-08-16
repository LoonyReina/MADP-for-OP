from __future__ import annotations
import hashlib
from pathlib import Path
from typing import Any
from ascendop_daemon.control_plane.control_database import (
    SCHEMA_VERSION,
    ControlDatabase,
)
from ascendop_daemon.control_plane.retry_controller import RetryController
from ascendop_daemon.automation.assistant_coordinator import AssistantCoordinator
from ascendop_daemon.automation.official_progress import OfficialProgressPublisher
from ascendop_daemon.automation.agent_gate import AgentGateCoordinator
from ascendop_daemon.automation.codex_ide_adapter import CodexIdeTaskAdapter
from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.runtime.application_lifecycle import ApplicationLifecycle
from ascendop_daemon.runtime.config_loader import load_config
from ascendop_daemon.runtime.application_support import ApplicationPaths, LOCAL_SERVICES
from ascendop_daemon.runtime.application_tick import run_application_tick
from ascendop_daemon.runtime.policy_registry import RuntimePolicy
from ascendop_daemon.runtime.periodic_worker import (
    EndpointDispatchWorkerPool,
    EndpointReconciliationPool,
)
from ascendop_daemon.runtime.endpoint_reconciliation import EndpointReconciliationService
from ascendop_daemon.runtime.release_identity import source_generation
from ascendop_daemon.runtime.dispatcher_factory import build_application_dispatchers
from ascendop_daemon.runtime.management_composition import build_control_command_worker
from ascendop_daemon.runtime.submit_intake import SubmitIntake
from ascendop_daemon.runtime.diagnostic_intake import DiagnosticIntake
from ascendop_daemon.workflow.action_coordinator import WorkflowActionCoordinator
from ascendop_daemon.workflow.typed_executor import TypedActionExecutor
class V4Application(ApplicationLifecycle):
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
            max_agent_preflight_attempts=int(
                self.policy.get("retry.max_agent_preflight_attempts")
            ),
        )
        self.control_commands = build_control_command_worker(self.database, self.policy)
        self.dispatchers = build_application_dispatchers(
            root=paths.root,
            database=self.database,
            registry=self.registry,
            policy=self.policy,
            code_generation=self.generation,
        )
        self.assistant = AssistantCoordinator(
            paths.root,
            self.database,
            steward_target_id=str(
                self.config.policy.get("flow_v3_steward_assistant_target_id") or ""
            ),
            steward_runbook_path=Path(
                str(
                    self.config.policy.get("flow_v3_steward_runbook_path")
                    or "docs/next/steward_escalation_runbook.md"
                )
            ),
        )
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
        self.session_gate = AgentGateCoordinator(
            root=paths.root,
            database=self.database,
            config=self.config,
            producer_generation=self.generation,
            max_turn_seconds=int(self.policy.get("agent.max_turn_seconds")),
        )
        self.codex_ide_adapter = CodexIdeTaskAdapter(
            paths.root,
            self.database,
            self.config,
            code_generation=self.generation,
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
        self.diagnostic_intake = DiagnosticIntake(
            root=paths.root,
            config=self.config,
            database=self.database,
            registry=self.registry,
            code_generation=self.generation,
        )
        self.workflow_actions = WorkflowActionCoordinator(
            paths.root,
            self.database,
            self.config,
            producer_generation=self.generation,
        )
        self.workflow_executor = TypedActionExecutor(
            paths.root,
            self.database,
            worker_id="ascendop-v4-workflow-executor",
            producer_generation=self.generation,
            claim_seconds=int(self.policy.get("workflow.action_claim_seconds")),
            local_timeout_seconds=int(
                self.policy.get("workflow.local_action_timeout_seconds")
            ),
            device_timeout_seconds=int(
                self.policy.get("workflow.device_action_timeout_seconds")
            ),
        )
    def initialize(self) -> dict[str, Any]:
        result = super().initialize()
        result["agent_execution"] = self.codex_ide_adapter.reconcile(
            health_state="offline"
        )
        result["agent_source_promotions"] = (
            self.codex_ide_adapter.reconcile_promotions()
        )
        return result

    def run_once(
        self,
        *,
        nonblocking_dispatch: bool = False,
    ) -> dict[str, Any]:
        return run_application_tick(
            self,
            nonblocking_dispatch=nonblocking_dispatch,
        )
    def _coordinate_workflow_actions(self) -> dict[str, Any]:
        try:
            return self.workflow_actions.run_once()
        except Exception as exc:
            return {
                "state": "failed",
                "action": None,
                "errors": [str(exc)],
            }

    def _launch_workflow_action(self) -> dict[str, Any]:
        try:
            return self.workflow_executor.launch_once()
        except Exception as exc:
            return {"state": "failed", "launched": False, "error": str(exc)}
    def reconcile_endpoints(
        self,
        *,
        endpoint_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        self.database.reconcile(self.config, self.registry)
        return self._reconcile_endpoints(
            endpoint_ids=endpoint_ids,
            allow_trusted_lease_probe=False,
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
