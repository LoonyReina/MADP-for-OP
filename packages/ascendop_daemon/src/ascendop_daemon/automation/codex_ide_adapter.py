from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.automation.agent_promotions import AgentPromotionQueue
from ascendop_daemon.automation.agent_workspace import AgentWorkspace
from ascendop_daemon.automation.agent_completion import AgentCompletionService
from ascendop_daemon.automation.agent_turn_completion import (
    AgentTurnCompletionError,
    prepare_agent_turn_completion,
)
from ascendop_daemon.automation.candidate_proposals import CandidateProposalPublisher
from ascendop_daemon.automation.case_proposals import CaseProposalPublisher
from ascendop_daemon.automation.agent_outputs import (
    AgentOutputBroker,
)
from ascendop_daemon.automation.agent_workspace import (
    AgentSourceIdentityChanged,
)
from ascendop_daemon.automation.codex_ide_preflight import (
    superseded_adapter_state,
    superseded_receipt,
)
from ascendop_daemon.automation.codex_ide_adapter_support import (
    CodexIdeAdapterSupportMixin,
    _promotion_action_id,
    _utc_now,
)
from ascendop_daemon.automation.codex_ide_settings import (
    ADAPTER_ID,
    CodexIdeAdapterError,
    CodexIdeAdapterSettings,
    agent_id,
    adapter_component_digest,
    boot_id,
    identity_digest,
)
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.core.models import (
    DaemonConfig,
    observed_operators,
    operator_session,
)


class CodexIdeTaskAdapter(CodexIdeAdapterSupportMixin):
    """DB-backed adapter boundary for long-lived Codex IDE tasks.

    The app-side consumer performs the actual task-tool call. This class owns
    registration, claim, immutable delivery, lease, receipt, and restart state.
    """

    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        config: DaemonConfig,
        *,
        code_generation: str,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.config = config
        self.code_generation = str(code_generation).strip()
        self.settings = CodexIdeAdapterSettings.from_config(config)
        self.workspace = AgentWorkspace(self.root)
        self.outputs = AgentOutputBroker(self.root, self.workspace.runs_root)
        self.promotions = AgentPromotionQueue(
            self.root,
            database,
            self.workspace,
            code_generation=self.code_generation,
        )
        self.completions = AgentCompletionService(
            self.root,
            database,
            code_generation=self.code_generation,
            workspace=self.workspace,
            outputs=self.outputs,
            promotions=self.promotions,
        )
        self.candidate_proposals = CandidateProposalPublisher(
            self.root,
            database,
            self.workspace,
        )
        self.case_proposals = CaseProposalPublisher(self.root)
        self.boot_id = boot_id()
        self.source_digest = adapter_component_digest()
        self.execution_contract_digest = identity_digest(
            {
                "schema": "ascendop.agent-execution-contract.v1",
                "adapter_id": ADAPTER_ID,
                "adapter_generation": self.settings.adapter_generation,
                "source_digest": self.source_digest,
                "pool_id": self.settings.pool_id,
            }
        )
        if not self.code_generation:
            raise CodexIdeAdapterError("Codex IDE adapter code generation is required")

    def reconcile(self, *, health_state: str = "offline") -> dict[str, Any]:
        if not self.settings.enabled:
            return {"enabled": False, "registrations": [], "bindings": []}
        try:
            pool = self.database.agent_pool(self.settings.pool_id)
        except Exception as exc:
            raise CodexIdeAdapterError(
                f"production Agent pool is unavailable: {self.settings.pool_id}"
            ) from exc
        if not bool(pool.get("enabled")):
            raise CodexIdeAdapterError("production Codex IDE Agent pool is disabled")
        if "codex-ide-task" not in set(pool.get("drivers", [])):
            raise CodexIdeAdapterError(
                "production Codex IDE Agent pool does not allow codex-ide-task"
            )
        registrations: list[dict[str, Any]] = []
        bindings: list[dict[str, Any]] = []
        for target in self.targets():
            registration = self._registration(target)
            registrations.append(
                self.database.register_agent(
                    registration,
                    health_state=health_state,
                    boot_id=self.boot_id if health_state != "offline" else "offline",
                    manager_runner_id=self.settings.manager_runner_id,
                    lease_seconds=self.settings.lease_seconds,
                )
            )
            bindings.append(
                {
                    "operator_id": target["operator_id"],
                    "role": target["role"],
                    "bindings": self.database.replace_agent_role_bindings(
                        operator_id=target["operator_id"],
                        role=target["role"],
                        bindings=[
                            {
                                "agent_id": target["agent_id"],
                                "enabled": True,
                                "priority": 100,
                            }
                        ],
                    ),
                }
            )
        return {
            "enabled": True,
            "pool_id": self.settings.pool_id,
            "adapter_generation": self.settings.adapter_generation,
            "registrations": registrations,
            "bindings": bindings,
        }

    def targets(self) -> list[dict[str, str]]:
        targets: list[dict[str, str]] = []
        for display_name in observed_operators(self.config):
            session = operator_session(self.config, display_name)
            if session is None or not session.enabled:
                continue
            registration = self.database.operator_for_display_name(display_name)
            operator_id = str(registration["operator_id"])
            roles = (
                ("solver", session.solver_thread_id),
                ("tester", session.tester_thread_id),
            )
            for role, task_id in roles:
                if role == "tester" and not bool(session.roles.get("casegen", False)):
                    continue
                if not task_id:
                    raise CodexIdeAdapterError(
                        f"{display_name} {role} has no Codex IDE task binding"
                    )
                targets.append(
                    {
                        "display_name": display_name,
                        "operator_id": operator_id,
                        "role": role,
                        "task_id": task_id,
                        "agent_id": agent_id(operator_id, role),
                    }
                )
        return targets

    def peek(self, *, consumer_id: str) -> dict[str, Any]:
        """Return compact delivery state without claiming work or renewing leases."""
        self._require_consumer(consumer_id)
        targets = {target["agent_id"]: target for target in self.targets()}
        targets_by_role = {
            (target["operator_id"], target["role"]): target
            for target in targets.values()
        }
        rows: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        for record in self.database.agent_actions_v4():
            action = dict(record.get("action") or {})
            assigned_agent_id = str(record.get("assigned_agent_id") or "")
            target = targets.get(assigned_agent_id) or targets_by_role.get(
                (
                    str(record.get("operator_id") or ""),
                    str(record.get("role") or ""),
                )
            )
            if target is None:
                continue
            if str(action.get("agent_pool_id") or "") != self.settings.pool_id:
                continue
            state = str(record.get("state") or "")
            counts[state] = counts.get(state, 0) + 1
            rows.append(
                {
                    "action_id": str(record.get("action_id") or ""),
                    "iteration_id": str(record.get("iteration_id") or ""),
                    "operator_id": str(record.get("operator_id") or ""),
                    "display_name": str(target.get("display_name") or ""),
                    "role": str(record.get("role") or ""),
                    "state": state,
                    "candidate_version": str(action.get("candidate_version") or ""),
                    "target_id": str(target.get("task_id") or ""),
                    "current_attempt_id": str(
                        record.get("current_attempt_id") or ""
                    ),
                    "current_lease_id": str(record.get("current_lease_id") or ""),
                    "created_at": str(record.get("created_at") or ""),
                    "updated_at": str(record.get("updated_at") or ""),
                }
            )
        rows.sort(key=lambda row: (row["created_at"], row["action_id"]), reverse=True)
        current = next(
            (
                row
                for row in rows
                if row["state"]
                in {"queued", "retry-pending", "claimed", "running", "uncertain"}
            ),
            rows[0] if rows else None,
        )
        return {
            "schema": "ascendop.codex-ide-adapter-peek.v1",
            "state": str(current.get("state") or "idle") if current else "idle",
            "consumer_id": consumer_id,
            "delivery_capacity": len(targets),
            "claim_side_effects": False,
            "current": current,
            "state_counts": dict(sorted(counts.items())),
        }

    def register_service(self, *, consumer_id: str) -> dict[str, Any]:
        """Publish adapter readiness without touching delivery or work leases."""
        self._require_consumer(consumer_id)
        reconciliation = self.reconcile(health_state="ready")
        self._service_heartbeat(consumer_id)
        return {
            "schema": "ascendop.codex-ide-adapter-registration.v1",
            "state": "ready",
            "consumer_id": consumer_id,
            "claim_side_effects": False,
            "execution_contract_digest": self.execution_contract_digest,
            "code_generation": self.code_generation,
            "reconciliation": reconciliation,
        }

    def poll(self, *, consumer_id: str) -> dict[str, Any]:
        self._require_consumer(consumer_id)
        reconciliation = self.reconcile(health_state="ready")
        self._service_heartbeat(consumer_id)
        managed = self._managed_agent_ids_for_consumer(consumer_id)
        adopted = self.database.adopt_uncertain_agent_action(
            runner_id=self.settings.manager_runner_id,
            boot_id=self.boot_id,
            managed_agent_ids=managed,
            lease_seconds=self.settings.lease_seconds,
        )
        if adopted is not None:
            try:
                delivery = self._prepare_delivery(adopted, consumer_id=consumer_id)
            except AgentSourceIdentityChanged as exc:
                terminal = self._cancel_superseded_action(adopted, exc)
                return {
                    "state": "idle",
                    "delivery": None,
                    "reason": "stale candidate action cancelled before delivery",
                    "terminal": terminal,
                    "reconciliation": reconciliation,
                }
            self._write_adapter_state(
                adopted,
                delivery,
                consumer_id=consumer_id,
                phase="reconcile-required",
            )
            return {
                "state": "reconcile-required",
                "reason": "uncertain Agent turn adopted; inspect the same target task before sending",
                "delivery": delivery,
                "reconciliation": reconciliation,
            }
        active = self._active_state(consumer_id)
        if active is not None:
            action = self.database.agent_action(str(active["action_id"]))
            if action is not None and action["state"] in {"claimed", "running"}:
                heartbeat = self.database.heartbeat_agent_action(
                    action_id=str(active["action_id"]),
                    lease_token=str(active["lease_token"]),
                    lease_seconds=self.settings.lease_seconds,
                )
                return {
                    "state": (
                        "observe"
                        if action["state"] == "running"
                        else "reconcile-required"
                    ),
                    "reason": (
                        "observe the existing IDE turn"
                        if action["state"] == "running"
                        else "delivery acknowledgement is unknown; inspect before sending"
                    ),
                    "delivery": self._read_delivery(str(active["action_id"])),
                    "turn_id": str(active.get("turn_id") or ""),
                    "heartbeat": heartbeat,
                    "reconciliation": reconciliation,
                }
        claimed = self.database.claim_agent_action(
            runner_id=self.settings.manager_runner_id,
            boot_id=self.boot_id,
            lease_seconds=self.settings.lease_seconds,
            managed_agent_ids=managed,
        )
        if claimed is None:
            return {"state": "idle", "delivery": None, "reconciliation": reconciliation}
        try:
            delivery = self._prepare_delivery(claimed, consumer_id=consumer_id)
        except AgentSourceIdentityChanged as exc:
            terminal = self._cancel_superseded_action(claimed, exc)
            return {
                "state": "idle",
                "delivery": None,
                "reason": "stale candidate action cancelled before delivery",
                "terminal": terminal,
                "reconciliation": reconciliation,
            }
        except Exception as exc:
            self.database.defer_agent_action_retry(
                action_id=str(claimed["action"]["action_id"]),
                lease_token=str(claimed["lease"]["lease_token"]),
                failure={
                    "failure_class": "agent-preflight",
                    "error": str(exc),
                    "runner_generation": self.settings.adapter_generation,
                    "agent_execution_contract_digest": self.execution_contract_digest,
                },
                lease_seconds=self.settings.lease_seconds,
            )
            raise
        self._write_adapter_state(
            claimed,
            delivery,
            consumer_id=consumer_id,
            phase="claimed",
        )
        return {
            "state": "delivery-ready",
            "delivery": delivery,
            "reconciliation": reconciliation,
        }

    def _cancel_superseded_action(
        self,
        claimed: Mapping[str, Any],
        error: AgentSourceIdentityChanged,
    ) -> dict[str, Any]:
        action = dict(claimed["action"])
        lease = dict(claimed["lease"])
        completed_at = _utc_now()
        receipt = superseded_receipt(
            claimed,
            error,
            adapter_generation=self.settings.adapter_generation,
            execution_contract_digest=self.execution_contract_digest,
            completed_at=completed_at,
        )
        terminal = self.database.complete_agent_action(
            receipt,
            lease_token=str(lease["lease_token"]),
        )
        self._write_state(
            str(action["action_id"]),
            superseded_adapter_state(claimed, completed_at=completed_at),
        )
        return terminal

    def mark_delivered(
        self,
        *,
        action_id: str,
        consumer_id: str,
        turn_id: str,
    ) -> dict[str, Any]:
        state = self._require_state(action_id, consumer_id)
        if not turn_id.strip():
            raise CodexIdeAdapterError("Codex IDE turn_id is required")
        result = self.database.start_agent_action(
            action_id=action_id,
            lease_token=str(state["lease_token"]),
            session_id=turn_id.strip(),
            lease_seconds=self.settings.lease_seconds,
        )
        state.update(
            {
                "phase": "running",
                "turn_id": turn_id.strip(),
                "delivered_at": _utc_now(),
                "updated_at": _utc_now(),
            }
        )
        self._write_state(action_id, state)
        return result

    def mark_not_delivered(
        self,
        *,
        action_id: str,
        consumer_id: str,
        verification: str,
    ) -> dict[str, Any]:
        """Submit a proven pre-publication failure to central retry arbitration."""

        state = self._require_state(action_id, consumer_id)
        if str(state.get("turn_id") or "").strip():
            raise CodexIdeAdapterError(
                "a delivery with a recorded turn_id cannot be marked not delivered"
            )
        if verification != "exact-delivery-marker-absent":
            raise CodexIdeAdapterError(
                "not-delivered requires exact-delivery-marker-absent verification"
            )
        result = self.database.defer_agent_action_retry(
            action_id=action_id,
            lease_token=str(state["lease_token"]),
            failure={
                "failure_class": "agent-adapter",
                "error": "target task has no exact delivery marker after reconciliation",
                "delivery_publish_state": "not-published",
                "verification": verification,
                "runner_generation": self.settings.adapter_generation,
                "agent_execution_contract_digest": self.execution_contract_digest,
            },
            lease_seconds=self.settings.lease_seconds,
        )
        state.update(
            {
                "phase": "retry-pending",
                "verification": verification,
                "updated_at": _utc_now(),
            }
        )
        self._write_state(action_id, state)
        return result

    def heartbeat(self, *, action_id: str, consumer_id: str) -> dict[str, Any]:
        state = self._require_state(action_id, consumer_id)
        self.reconcile(health_state="ready")
        self._service_heartbeat(consumer_id)
        return self.database.heartbeat_agent_action(
            action_id=action_id,
            lease_token=str(state["lease_token"]),
            lease_seconds=self.settings.lease_seconds,
        )

    def complete(
        self,
        *,
        action_id: str,
        consumer_id: str,
        status: str = "",
        summary: str = "",
        artifacts: list[str] | None = None,
        failure_class: str = "",
        completion_envelope: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        state = self._require_state(action_id, consumer_id)
        try:
            prepared = prepare_agent_turn_completion(
                database=self.database,
                action_id=action_id,
                state=state,
                delivery=(
                    self._read_delivery(action_id)
                    if completion_envelope is not None
                    else None
                ),
                completion_envelope=completion_envelope,
                status=status,
                summary=summary,
                failure_class=failure_class,
                runner_id=self.settings.manager_runner_id,
                lease_seconds=self.settings.lease_seconds,
                observed_at=_utc_now(),
            )
        except AgentTurnCompletionError as exc:
            raise CodexIdeAdapterError(str(exc)) from exc
        if prepared.state_updates is not None:
            state.update(prepared.state_updates)
            self._write_state(action_id, state)
        status = prepared.terminal_status
        structured_result = prepared.structured_result
        recovery = prepared.reconciliation
        native_session_id = str(state.get("turn_id") or state["target_id"])
        result = self.completions.complete(
            action_id=action_id,
            lease_token=str(state["lease_token"]),
            lease_id=str(state["lease_id"]),
            agent_id=str(state["agent_id"]),
            started_at=str(state.get("delivered_at") or state["claimed_at"]),
            native_outcome={
                "schema": "ascendop.native-turn-outcome.v1",
                "action_id": action_id,
                "native_session_id": native_session_id,
                "native_turn_id": native_session_id,
                "terminal_status": status,
                "structured_result": structured_result,
                "artifact_refs": list(artifacts or []),
                "telemetry": {"usage": {}, "skills": {}},
                "observed_at": _utc_now(),
            },
            completion_metadata={
                "target_id": str(state["target_id"]),
                "adapter_id": ADAPTER_ID,
                "runner_generation": self.settings.adapter_generation,
                "agent_execution_contract_digest": self.execution_contract_digest,
                **(
                    {"receipt_reconciliation": recovery}
                    if recovery is not None
                    else {}
                ),
            },
        )
        terminal_state = str(result["terminal"]["state"])
        state.update({"phase": terminal_state, "updated_at": _utc_now()})
        self._write_state(action_id, state)
        return result

    def reconcile_promotions(self) -> list[dict[str, Any]]:
        if not self.settings.enabled:
            return []
        queued: list[dict[str, Any]] = []
        for action_record in self.database.agent_actions_v4(state="completed"):
            action = dict(action_record["action"])
            is_current = self.database.workflow_agent_action_is_current(
                str(action["action_id"])
            )
            is_tester = str(action.get("role") or "") == "tester"
            if not is_current and not self._recoverable_tester_case(action):
                continue
            seal_path = (
                self.workspace.run_root(str(action["action_id"])) / "source-seal.json"
            )
            if not seal_path.is_file():
                continue
            seal = json.loads(seal_path.read_text(encoding="utf-8"))
            changed_source = (
                seal
                if isinstance(seal, dict) and seal.get("changed_paths")
                else None
            )
            output_seal_path = (
                self.workspace.run_root(str(action["action_id"])) / "output-seal.json"
            )
            if not output_seal_path.is_file():
                if changed_source is not None:
                    queued.append(
                        self.promotions.enqueue_case(action, changed_source)
                        if is_tester
                        else self.promotions.enqueue_source(action, changed_source)
                    )
                continue
            output_seal = json.loads(output_seal_path.read_text(encoding="utf-8"))
            if isinstance(output_seal, dict) and output_seal.get("outputs"):
                queued.append(
                    self.promotions.enqueue_output(
                        action,
                        output_seal,
                        source_seal=changed_source,
                    )
                )
            elif changed_source is not None:
                queued.append(
                    self.promotions.enqueue_case(action, changed_source)
                    if is_tester
                    else self.promotions.enqueue_source(action, changed_source)
                )
        return queued

    def _recoverable_tester_case(self, action: Mapping[str, Any]) -> bool:
        if str(action.get("role") or "") != "tester":
            return False
        if self.case_proposals.replay_source_receipt(action) is not None:
            return True
        source_root = (self.root / str(action.get("origin_workspace") or "")).resolve()
        contract_path = source_root / "case_contract.json"
        try:
            contract = json.loads(contract_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        if not isinstance(contract, dict):
            return False
        return (
            contract.get("action_id") == action.get("action_id")
            and contract.get("board_digest") == action.get("board_digest")
            and contract.get("case_version") == action.get("candidate_version")
        )

    def promote(self, seal_path: Path) -> dict[str, Any]:
        action_id = _promotion_action_id(seal_path)
        with self.database.agent_action_promotion_guard(action_id):
            return self.workspace.promote(seal_path)

    def promote_case(self, seal_path: Path) -> dict[str, Any]:
        action_id = _promotion_action_id(seal_path)
        action_record = self.database.agent_action(action_id)
        if action_record is None:
            raise CodexIdeAdapterError(f"Agent action does not exist: {action_id}")
        action = dict(action_record["action"])
        replay_source_receipt = self.case_proposals.replay_source_receipt(action)
        historical_contract_matches = (
            replay_source_receipt is not None
            or self._recoverable_tester_case(action)
        )
        with self.database.agent_action_promotion_guard(
            action_id,
            allow_obsolete_completed_tester_case=historical_contract_matches,
        ):
            source_receipt = (
                replay_source_receipt
                if replay_source_receipt is not None
                else self.workspace.promote(seal_path)
            )
            return self.case_proposals.publish(
                action,
                source_receipt,
            )

    def promote_output(self, seal_path: Path) -> dict[str, Any]:
        action_id = _promotion_action_id(seal_path)
        with self.database.agent_action_promotion_guard(action_id):
            source_seal = seal_path.parent / "source-seal.json"
            if source_seal.is_file():
                source = json.loads(source_seal.read_text(encoding="utf-8"))
                if isinstance(source, dict) and source.get("changed_paths"):
                    self.workspace.promote(source_seal)
            receipt = self.outputs.promote(seal_path)
            self.candidate_proposals.publish_from_output_receipt(receipt)
            return receipt

    def recover_obsolete_output(
        self,
        stale_seal_path: Path,
        prior_seal_path: Path,
    ) -> dict[str, Any]:
        action_id = _promotion_action_id(stale_seal_path)
        with self.database.obsolete_agent_output_recovery_guard(action_id):
            return self.outputs.recover_obsolete(stale_seal_path, prior_seal_path)

    def recover_cancelled_delivery(
        self,
        *,
        action_id: str,
        consumer_id: str,
        turn_id: str,
        verification: str,
    ) -> dict[str, Any]:
        """Resume one exact published IDE turn misclassified by old restaging."""

        self._require_consumer(consumer_id)
        action_record = self.database.agent_action(action_id)
        if action_record is None:
            raise CodexIdeAdapterError(f"Agent action does not exist: {action_id}")
        if action_record["state"] != "cancelled":
            raise CodexIdeAdapterError(
                "cancelled delivery recovery requires a cancelled action"
            )
        action = dict(action_record["action"])
        attempt_id = str(action_record["current_attempt_id"])
        delivery = self._read_delivery(action_id)
        if str(delivery["delivery_id"]) != f"atd-{attempt_id}" or str(
            delivery["delivery_key"]
        ) != f"{action_id}:{attempt_id}":
            raise CodexIdeAdapterError(
                "cancelled delivery recovery attempt identity changed"
            )
        if dict(delivery["action"]) != action:
            raise CodexIdeAdapterError(
                "cancelled delivery recovery action payload changed"
            )
        if str(delivery["lease"]["lease_id"]) != str(
            action_record["current_lease_id"]
        ):
            raise CodexIdeAdapterError(
                "cancelled delivery recovery lease identity changed"
            )
        target = self._target_for_agent(str(action_record["assigned_agent_id"]))
        if str(delivery["target_id"]) != target["task_id"]:
            raise CodexIdeAdapterError(
                "cancelled delivery recovery target identity changed"
            )
        run_root = self.workspace.run_root(action_id)
        if (run_root / "source-seal.json").exists() or (
            run_root / "output-seal.json"
        ).exists():
            raise CodexIdeAdapterError(
                "cancelled delivery recovery rejects an already sealed action"
            )
        try:
            stage = json.loads((run_root / "stage.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CodexIdeAdapterError(
                "cancelled delivery recovery stage is unavailable"
            ) from exc
        if not isinstance(stage, dict) or stage.get("action_id") != action_id:
            raise CodexIdeAdapterError(
                "cancelled delivery recovery stage identity changed"
            )
        expected = str(
            action.get("candidate_identity", {}).get("execution_source_digest") or ""
        )
        if (
            not expected
            or str(stage.get("source_before_digest") or "") != expected
            or str(stage.get("workspace") or "") != str(delivery["workspace"])
        ):
            raise CodexIdeAdapterError(
                "cancelled delivery recovery source stage changed"
            )
        origin = (self.root / str(action["origin_workspace"])).resolve()
        if self.root not in origin.parents or not origin.is_dir():
            raise CodexIdeAdapterError(
                "cancelled delivery recovery origin is unavailable"
            )
        actual = self.workspace.digest(origin)
        if actual != expected:
            raise CodexIdeAdapterError(
                "cancelled delivery recovery canonical source changed: "
                f"expected={expected} actual={actual}"
            )
        recovered = self.database.recover_cancelled_published_agent_action(
            action_id=action_id,
            attempt_id=attempt_id,
            turn_id=turn_id,
            verification=verification,
            runner_id=self.settings.manager_runner_id,
            lease_seconds=self.settings.lease_seconds,
        )
        existing = self._existing_delivery(recovered)
        if existing is None:
            raise CodexIdeAdapterError(
                "cancelled delivery recovery lost its immutable delivery"
            )
        self._write_adapter_state(
            recovered,
            existing,
            consumer_id=consumer_id,
            phase="running",
        )
        return {
            "state": "running",
            "action_id": action_id,
            "attempt_id": attempt_id,
            "turn_id": turn_id,
            "delivery_id": str(existing["delivery_id"]),
        }
