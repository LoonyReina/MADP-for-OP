from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.agent import (
    AGENT_ACTION_RECEIPT_SCHEMA,
    AGENT_REGISTRATION_SCHEMA,
    AGENT_TURN_DELIVERY_SCHEMA,
    render_agent_output_authoring_contract,
    validate_agent_turn_delivery,
)
from ascendop_daemon.automation.agent_promotions import AgentPromotionQueue
from ascendop_daemon.automation.agent_workspace import AgentWorkspace
from ascendop_daemon.automation.candidate_proposals import CandidateProposalPublisher
from ascendop_daemon.automation.case_proposals import CaseProposalPublisher
from ascendop_daemon.automation.agent_outputs import (
    AgentOutputBroker,
    AgentOutputError,
)
from ascendop_daemon.automation.agent_workspace import (
    AgentSourceIdentityChanged,
    AgentWorkspaceError,
)
from ascendop_daemon.automation.codex_ide_preflight import (
    superseded_adapter_state,
    superseded_receipt,
)
from ascendop_daemon.automation.codex_ide_settings import (
    ADAPTER_ID,
    TOKEN_CHARS,
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


class CodexIdeTaskAdapter:
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
        if verification != "exact-action-marker-absent":
            raise CodexIdeAdapterError(
                "not-delivered requires exact-action-marker-absent verification"
            )
        result = self.database.defer_agent_action_retry(
            action_id=action_id,
            lease_token=str(state["lease_token"]),
            failure={
                "failure_class": "agent-adapter",
                "error": "target task has no exact action marker after reconciliation",
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
        status: str,
        summary: str,
        artifacts: list[str] | None = None,
        failure_class: str = "",
    ) -> dict[str, Any]:
        if status not in {
            "completed",
            "failed",
            "interrupted",
            "cancelled",
            "uncertain",
        }:
            raise CodexIdeAdapterError(f"unsupported Agent completion status: {status}")
        reported_status = status
        if status == "interrupted":
            status = "failed"
            failure_class = failure_class or "adapter-execution"
        state = self._require_state(action_id, consumer_id)
        action_record = self.database.agent_action(action_id)
        if action_record is None:
            raise CodexIdeAdapterError(f"Agent action does not exist: {action_id}")
        action = dict(action_record["action"])
        seal: dict[str, Any] | None = None
        output_seal: dict[str, Any] | None = None
        artifact_paths = list(artifacts or [])
        completion: dict[str, Any] = {
            "summary": summary,
            "session_id": str(state.get("turn_id") or ""),
            "target_id": str(state["target_id"]),
            "adapter_id": ADAPTER_ID,
            "runner_generation": self.settings.adapter_generation,
            "agent_execution_contract_digest": self.execution_contract_digest,
        }
        if reported_status != status:
            completion["reported_status"] = reported_status
        if failure_class:
            completion["failure_class"] = failure_class
        if status == "completed" and not self.database.workflow_agent_action_is_current(
            action_id
        ):
            status = "cancelled"
            completion.update(
                {
                    "failure_class": "agent-gate-obsolete",
                    "cancellation_reason": "workflow-gate-no-longer-current",
                    "reported_status": "completed",
                }
            )
        if status == "completed":
            try:
                seal = self.workspace.seal(action)
                seal_path = self.workspace.run_root(action_id) / "source-seal.json"
                artifact_paths.append(seal_path.relative_to(self.root).as_posix())
                workspace = self.workspace.run_root(action_id) / "workspace"
                output_seal = self.outputs.seal(action, workspace)
                output_seal_path = (
                    self.workspace.run_root(action_id) / "output-seal.json"
                )
                artifact_paths.append(
                    output_seal_path.relative_to(self.root).as_posix()
                )
                changed_code = any(
                    str(path).startswith(("op_host/", "op_kernel/"))
                    for path in seal["changed_paths"]
                )
                proposed_candidate = any(
                    str(item.get("output_kind") or "") == "solver-candidate-proposal"
                    for item in output_seal["outputs"]
                )
                candidate_contract = any(
                    str(item.get("output_kind") or "") == "solver-candidate-proposal"
                    for item in action.get("output_contracts", [])
                )
                if (
                    action.get("role") == "solver"
                    and changed_code
                    and candidate_contract
                    and not proposed_candidate
                ):
                    raise CodexIdeAdapterError(
                        "Solver changed op_host/op_kernel without a candidate proposal"
                    )
                if not seal["changed_paths"] and not output_seal["outputs"]:
                    raise CodexIdeAdapterError(
                        "completed Agent action produced no durable source or workflow output"
                    )
                completion.update(
                    {
                        "source_before_digest": seal["source_before_digest"],
                        "source_after_digest": seal["source_after_digest"],
                        "changed_paths": seal["changed_paths"],
                        "workflow_outputs": output_seal["outputs"],
                        "out_of_scope_paths": [],
                    }
                )
            except AgentWorkspaceError as exc:
                status = "failed"
                completion.update(
                    {
                        "failure_class": "protocol",
                        "validation_error": str(exc),
                    }
                )
                seal = None
                output_seal = None
            except (AgentOutputError, CodexIdeAdapterError) as exc:
                status = "failed"
                completion.update(
                    {
                        "failure_class": "agent-output-validation",
                        "validation_error": str(exc),
                    }
                )
                seal = None
                output_seal = None
        receipt = {
            "schema": AGENT_ACTION_RECEIPT_SCHEMA,
            "action_id": action_id,
            "iteration_id": str(action["iteration_id"]),
            "agent_id": str(state["agent_id"]),
            "lease_id": str(state["lease_id"]),
            "status": status,
            "started_at": str(state.get("delivered_at") or state["claimed_at"]),
            "completed_at": _utc_now(),
            "completion": completion,
            "artifacts": sorted(set(artifact_paths)),
        }
        terminal = self.database.complete_agent_action(
            receipt,
            lease_token=str(state["lease_token"]),
        )
        terminal_state = str(terminal["state"])
        if terminal_state != str(receipt["status"]):
            committed_receipt = self.database.agent_action_receipt(action_id)
            if committed_receipt is None:
                raise CodexIdeAdapterError(
                    "terminal Agent action has no committed receipt"
                )
            receipt = committed_receipt
        state.update({"phase": terminal_state, "updated_at": _utc_now()})
        self._write_state(action_id, state)
        promotable = (
            status == "completed"
            and terminal_state == "completed"
            and self.database.workflow_agent_action_is_current(action_id)
        )
        output_promotion = (
            self.promotions.enqueue_output(
                action,
                output_seal,
                source_seal=(
                    seal if seal and seal.get("changed_paths") else None
                ),
            )
            if promotable and output_seal and output_seal["outputs"]
            else None
        )
        promotion = (
            output_promotion
            if output_promotion is not None and seal and seal.get("changed_paths")
            else (
                (
                    self.promotions.enqueue_case(action, seal)
                    if str(action.get("role") or "") == "tester"
                    else self.promotions.enqueue_source(action, seal)
                )
                if promotable and seal and seal["changed_paths"]
                else None
            )
        )
        return {
            "terminal": terminal,
            "receipt": receipt,
            "promotion": promotion,
            "output_promotion": output_promotion,
        }

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

    def recover_legacy_evidence_validation(
        self,
        *,
        action_id: str,
        verification: str,
    ) -> dict[str, Any]:
        """Reclassify one proven legacy zero-byte evidence false failure."""

        if verification != "zero-byte-evidence-blobs-match":
            raise CodexIdeAdapterError(
                "legacy evidence recovery requires zero-byte-evidence-blobs-match"
            )
        action = self.database.agent_action(action_id)
        if action is None:
            raise CodexIdeAdapterError(f"Agent action does not exist: {action_id}")
        proof = self.workspace.audit_legacy_zero_byte_evidence(action_id)
        recovered = self.database.reclassify_legacy_agent_evidence_validation(
            action_id=action_id,
            attempt_id=str(action["current_attempt_id"]),
            proof=proof,
        )
        return {
            "schema": "ascendop.codex-ide-evidence-recovery.v1",
            "state": "reclassified",
            "action_id": action_id,
            "attempt_id": str(action["current_attempt_id"]),
            "failure_class": "agent-output-validation",
            "proof": proof,
            "action_state": str(recovered["state"]),
        }

    def _registration(self, target: Mapping[str, str]) -> dict[str, Any]:
        return {
            "schema": AGENT_REGISTRATION_SCHEMA,
            "agent_id": target["agent_id"],
            "driver": "codex-ide-task",
            "executable": f"codex-ide-task://{target['task_id']}",
            "executable_digest": self.source_digest,
            "observed_version": self.settings.adapter_generation,
            "registration_generation": identity_digest(
                {
                    "adapter_generation": self.settings.adapter_generation,
                    "operator_id": target["operator_id"],
                    "role": target["role"],
                    "task_id": target["task_id"],
                }
            ),
            "capabilities": {
                "stream_json": False,
                "resume": True,
                "structured_output": True,
                "task_visibility": True,
                "isolated_workspace": True,
            },
            "target_kind": "codex-ide-task",
            "target_id": target["task_id"],
            "operator_id": target["operator_id"],
            "role": target["role"],
            "observed_at": _utc_now(),
        }

    def _prepare_delivery(
        self,
        claimed: Mapping[str, Any],
        *,
        consumer_id: str,
    ) -> dict[str, Any]:
        action = dict(claimed["action"])
        context = dict(claimed["context"])
        agent = dict(claimed["agent"])
        existing = self._existing_delivery(claimed)
        if existing is not None:
            return existing
        workflow_evidence = context.get("workflow_evidence", [])
        if not isinstance(workflow_evidence, list) or not all(
            isinstance(item, Mapping) for item in workflow_evidence
        ):
            raise CodexIdeAdapterError(
                "Agent workflow evidence must be a list of objects"
            )
        reference_evidence = context.get("reference_evidence", [])
        if not isinstance(reference_evidence, list) or not all(
            isinstance(item, Mapping) for item in reference_evidence
        ):
            raise CodexIdeAdapterError(
                "Agent reference evidence must be a list of objects"
            )
        _run_root, workspace, _before = self.workspace.stage(
            action,
            [*workflow_evidence, *reference_evidence],
        )
        self.outputs.stage(action, workspace)
        target = self._target_for_agent(str(agent["agent_id"]))
        prompt = self._prompt(action, context, workspace, str(claimed["attempt_id"]))
        attempt_id = str(claimed["attempt_id"])
        delivery = validate_agent_turn_delivery(
            {
                "schema": AGENT_TURN_DELIVERY_SCHEMA,
                "delivery_id": f"atd-{attempt_id}",
                "delivery_key": f"{action['action_id']}:{attempt_id}",
                "adapter_id": ADAPTER_ID,
                "adapter_generation": self.settings.adapter_generation,
                "target_kind": "codex-ide-task",
                "target_id": target["task_id"],
                "workspace": workspace.relative_to(self.root).as_posix(),
                "prompt": prompt,
                "prompt_digest": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
                "action": action,
                "context": context,
                "lease": dict(claimed["lease"]),
                "agent": {
                    "agent_id": agent["agent_id"],
                    "driver": agent["driver"],
                },
                "created_at": _utc_now(),
            }
        )
        self._write_json(
            self.workspace.run_root(str(action["action_id"])) / "delivery.json",
            delivery,
        )
        return delivery

    def _existing_delivery(
        self,
        claimed: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        action = dict(claimed["action"])
        action_id = str(action["action_id"])
        attempt_id = str(claimed["attempt_id"])
        path = self.workspace.run_root(action_id) / "delivery.json"
        if not path.is_file():
            return None
        delivery = self._read_delivery(action_id)
        if str(delivery["delivery_id"]) != f"atd-{attempt_id}":
            return None
        if str(delivery["delivery_key"]) != f"{action_id}:{attempt_id}":
            raise CodexIdeAdapterError("Agent delivery identity is inconsistent")
        if dict(delivery["action"]) != action:
            raise CodexIdeAdapterError("Agent delivery action changed after publication")
        if dict(delivery["context"]) != dict(claimed["context"]):
            raise CodexIdeAdapterError("Agent delivery context changed after publication")
        if str(delivery["agent"]["agent_id"]) != str(
            claimed["agent"]["agent_id"]
        ):
            raise CodexIdeAdapterError("Agent delivery executor identity changed")
        target = self._target_for_agent(str(claimed["agent"]["agent_id"]))
        if str(delivery["target_id"]) != target["task_id"]:
            raise CodexIdeAdapterError("Agent delivery target changed after publication")
        lease = dict(delivery["lease"])
        current_lease = dict(claimed["lease"])
        for field in (
            "lease_id",
            "lease_token",
            "action_id",
            "iteration_id",
            "operator_id",
            "role",
            "agent_id",
        ):
            if str(lease.get(field) or "") != str(current_lease.get(field) or ""):
                raise CodexIdeAdapterError(
                    f"Agent delivery lease identity changed: {field}"
                )
        expected_workspace = (
            self.workspace.run_root(action_id) / "workspace"
        ).relative_to(self.root).as_posix()
        if str(delivery["workspace"]) != expected_workspace:
            raise CodexIdeAdapterError("Agent delivery workspace changed after publication")
        return delivery

    def _prompt(
        self,
        action: Mapping[str, Any],
        context: Mapping[str, Any],
        workspace: Path,
        attempt_id: str,
    ) -> str:
        runbook = (self.root / str(action["runbook_path"])).resolve()
        if self.root not in runbook.parents or not runbook.is_file():
            raise CodexIdeAdapterError("Agent action runbook is missing or unbounded")
        if hashlib.sha256(runbook.read_bytes()).hexdigest() != action["runbook_digest"]:
            raise CodexIdeAdapterError("Agent action runbook digest mismatch")
        marker = f"ASCENDOP_AGENT_ACTION={action['action_id']} ATTEMPT={attempt_id}"
        output_authoring = render_agent_output_authoring_contract(
            action.get("output_contracts", [])
        )
        return (
            marker
            + "\n\n"
            + runbook.read_text(encoding="utf-8")
            + "\n\nFLOW V4 ACTION (immutable):\n"
            + json.dumps(action, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n\nHANDOFF CONTEXT (immutable):\n"
            + json.dumps(context, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n\n"
            + output_authoring
            + "\n\nWork only inside this isolated workspace: "
            + str(workspace)
            + "\nImmutable workflow and reference artifacts, when supplied, are indexed "
            "by .ascendop-evidence/MANIFEST.json. Inspect or skip them according "
            "to the registered skills and current evidence."
            + "\nDo not mutate the canonical workspace, queue/result archives, control DB, "
            "endpoint state, or official website. Finish with a concise result summary; "
            "the app-side adapter records the typed receipt. Any daemon-authorized "
            "non-source output must be written only to the exact slot declared in "
            ".ascendop-output/CONTRACT.json.\n"
        )

    def _target_for_agent(self, agent_id: str) -> dict[str, str]:
        for target in self.targets():
            if target["agent_id"] == agent_id:
                return target
        raise CodexIdeAdapterError(f"Codex IDE target is not configured: {agent_id}")

    def _managed_agent_ids_for_consumer(self, consumer_id: str) -> set[str]:
        targets = self.targets()
        if not targets:
            return set()
        if len(targets) == 1:
            return {targets[0]["agent_id"]}
        base = self.settings.manager_runner_id
        if consumer_id == base:
            index = 0
        elif consumer_id.startswith(base + "-"):
            suffix = consumer_id[len(base) + 1 :]
            if not suffix.isdigit():
                raise CodexIdeAdapterError(
                    "Codex IDE consumer does not identify a configured target slot"
                )
            index = int(suffix)
        else:
            raise CodexIdeAdapterError(
                "Codex IDE consumer does not identify a configured target slot"
            )
        if index >= len(targets):
            raise CodexIdeAdapterError(
                "Codex IDE consumer target slot is outside delivery capacity"
            )
        return {targets[index]["agent_id"]}

    def _service_heartbeat(self, consumer_id: str) -> None:
        self.database.record_runtime_service_heartbeat(
            service_id="ascendop-codex-ide-task-adapter",
            role="agent-execution",
            code_generation=self.code_generation,
            capabilities=[
                "agent-pool-routing",
                "agent-work-lease",
                "isolated-workspace",
                "session-resume",
                "app-side-task-delivery",
            ],
            state="ready",
            boot_id=self.boot_id,
            lease_seconds=self.settings.lease_seconds,
            details={
                "adapter_id": ADAPTER_ID,
                "consumer_id": consumer_id,
                "runner_id": self.settings.manager_runner_id,
                "runner_generation": self.settings.adapter_generation,
                "execution_contract_digest": self.execution_contract_digest,
            },
        )

    def _write_adapter_state(
        self,
        claimed: Mapping[str, Any],
        delivery: Mapping[str, Any],
        *,
        consumer_id: str,
        phase: str,
    ) -> None:
        action = claimed["action"]
        lease = claimed["lease"]
        state = {
            "schema": "ascendop.codex-ide-adapter-state.v1",
            "action_id": str(action["action_id"]),
            "attempt_id": str(claimed["attempt_id"]),
            "agent_id": str(claimed["agent"]["agent_id"]),
            "lease_id": str(lease["lease_id"]),
            "lease_token": str(lease["lease_token"]),
            "target_id": str(delivery["target_id"]),
            "consumer_id": consumer_id,
            "phase": phase,
            "turn_id": str(claimed.get("session_id") or ""),
            "claimed_at": str(lease["acquired_at"]),
            "updated_at": _utc_now(),
        }
        self._write_state(str(action["action_id"]), state)

    def _active_state(self, consumer_id: str) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        if not self.workspace.runs_root.is_dir():
            return None
        for path in self.workspace.runs_root.glob("*/adapter-state.json"):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(state, dict) or state.get("consumer_id") != consumer_id:
                continue
            action = self.database.agent_action(str(state.get("action_id") or ""))
            if (
                action is not None
                and action["state"] in {"claimed", "running"}
                and str(state.get("attempt_id") or "")
                == str(action.get("current_attempt_id") or "")
                and str(state.get("lease_id") or "")
                == str(action.get("current_lease_id") or "")
            ):
                candidates.append(state)
        if len(candidates) > 1:
            raise CodexIdeAdapterError("consumer owns multiple active Agent deliveries")
        return candidates[0] if candidates else None

    def _require_state(self, action_id: str, consumer_id: str) -> dict[str, Any]:
        self._require_consumer(consumer_id)
        path = self.workspace.run_root(action_id) / "adapter-state.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CodexIdeAdapterError(
                f"Codex IDE adapter state is unavailable: {action_id}"
            ) from exc
        if not isinstance(value, dict) or value.get("consumer_id") != consumer_id:
            raise CodexIdeAdapterError("Codex IDE adapter consumer identity mismatch")
        action = self.database.agent_action(action_id)
        if action is None:
            raise CodexIdeAdapterError(f"Agent action does not exist: {action_id}")
        if (
            str(value.get("attempt_id") or "")
            != str(action.get("current_attempt_id") or "")
            or str(value.get("lease_id") or "")
            != str(action.get("current_lease_id") or "")
        ):
            raise CodexIdeAdapterError(
                "Codex IDE adapter attempt or lease identity is stale"
            )
        return value

    def _read_delivery(self, action_id: str) -> dict[str, Any]:
        path = self.workspace.run_root(action_id) / "delivery.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CodexIdeAdapterError(
                f"Agent delivery is unavailable: {action_id}"
            ) from exc
        if not isinstance(value, dict):
            raise CodexIdeAdapterError("Agent delivery must be a JSON object")
        return validate_agent_turn_delivery(value)

    def _write_state(self, action_id: str, state: Mapping[str, Any]) -> None:
        path = self.workspace.run_root(action_id) / "adapter-state.json"
        self._write_json(path, state)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _write_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as handle:
            handle.write(payload)
            temporary = Path(handle.name)
        os.replace(temporary, path)

    @staticmethod
    def _require_consumer(consumer_id: str) -> None:
        if not consumer_id or any(ch not in TOKEN_CHARS for ch in consumer_id):
            raise CodexIdeAdapterError("consumer_id must be a safe token")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _promotion_action_id(seal_path: Path) -> str:
    try:
        value = json.loads(seal_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CodexIdeAdapterError(f"invalid Agent promotion seal: {seal_path}") from exc
    action_id = str(value.get("action_id") or "") if isinstance(value, dict) else ""
    if not action_id or any(char not in TOKEN_CHARS for char in action_id):
        raise CodexIdeAdapterError("Agent promotion seal has an invalid action_id")
    return action_id
