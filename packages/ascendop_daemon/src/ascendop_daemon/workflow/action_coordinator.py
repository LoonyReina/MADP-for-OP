from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import (
    ControlDatabase,
    ControlDatabaseError,
)
from ascendop_daemon.control_plane.scheduler import Scheduler
from ascendop_daemon.core.models import ActionKind, DaemonConfig, GateDecision
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.workflow.action_factory import workflow_action_from_decision
from ascendop_daemon.workflow.candidate_sealer import CandidateSealer
from ascendop_daemon.workflow.gate_engine import GateEngine
from ascendop_daemon.workflow.policy_pipeline import WorkflowPolicyPipeline
from ascendop_daemon.workflow.task_execution_profile import (
    task_execution_profile_path,
)


NON_EXECUTABLE = {
    ActionKind.HOLD,
    ActionKind.REVIEW_MANUAL,
    ActionKind.NOTIFY_SOLVER,
    ActionKind.NOTIFY_TESTER_CASEGEN,
}


class WorkflowActionCoordinator:
    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        config: DaemonConfig,
        *,
        producer_generation: str,
        reader: StateReader | None = None,
        policy: WorkflowPolicyPipeline | None = None,
        sealer: CandidateSealer | None = None,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.config = config
        self.producer_generation = producer_generation
        self.reader = reader or StateReader(self.root, config)
        self.gates = GateEngine(config)
        self.policy = policy or WorkflowPolicyPipeline(
            self.root,
            config,
            database=database,
        )
        self.sealer = sealer or CandidateSealer(self.root)
        self.scheduler = Scheduler(config)

    def run_once(self) -> dict[str, Any]:
        snapshot = self.reader.read()
        raw_decisions = self.policy.apply(
            self.gates.evaluate(snapshot.rows, snapshot.transport)
        )
        decisions: list[GateDecision] = []
        prepared: dict[str, dict[str, Any]] = {}
        current_action_ids: set[str] = set()
        errors: list[dict[str, str]] = []
        for decision in raw_decisions:
            if decision.action in NON_EXECUTABLE:
                decisions.append(decision)
                continue
            try:
                action = workflow_action_from_decision(
                    self.root,
                    decision,
                    producer_generation=self.producer_generation,
                    task_profile_path=(
                        self._task_profile_path(decision.row.op)
                        if decision.action == ActionKind.PREPARE_SUBMIT
                        else None
                    ),
                )
            except (ControlDatabaseError, OSError, ValueError) as exc:
                errors.append(
                    {
                        "operator": decision.row.op,
                        "action": decision.action.value,
                        "error": str(exc),
                    }
                )
                decisions.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.HOLD,
                        reason=(
                            "V4 typed action contract rejected this board row: "
                            f"{exc}"
                        ),
                        command="",
                        priority=0,
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            current_action_ids.add(str(action["action_id"]))
            state = self.database.workflow_action_state(action["idempotency_key"])
            if not state:
                decisions.append(decision)
                prepared[decision.action_id] = action
                continue
            decisions.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "typed workflow action already exists for this board revision: "
                        f"{action['action_id']} state={state}"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
        cancelled_obsolete = self.database.cancel_obsolete_workflow_actions(
            producer_generation=self.producer_generation,
            active_operators=set(self.config.operators),
            current_action_ids=current_action_ids,
            cancel_board_drift=bool(snapshot.rows) and not errors,
        )
        previous = self.database.read_scheduler_state("workflow-v4")
        executable_decisions = tuple(
            decision
            for decision in decisions
            if decision.action not in NON_EXECUTABLE
        )
        plan = self.scheduler.plan(executable_decisions, previous)
        selected = plan.selected
        result: dict[str, Any] = {
            "schema": "ascendop.workflow-action-cycle.v1",
            "captured_at": snapshot.captured_at,
            "board_rows": len(snapshot.rows),
            "decision_count": len(decisions),
            "executable_decision_count": len(executable_decisions),
            "selected_action": selected.action.value if selected else "",
            "selected_operator": selected.row.op if selected else "",
            "action": None,
            "errors": errors,
            "cancelled_obsolete": cancelled_obsolete,
        }
        if selected is None or selected.action in NON_EXECUTABLE:
            return result
        try:
            action = prepared.get(selected.action_id) or workflow_action_from_decision(
                self.root,
                selected,
                producer_generation=self.producer_generation,
                task_profile_path=(
                    self._task_profile_path(selected.row.op)
                    if selected.action == ActionKind.PREPARE_SUBMIT
                    else None
                ),
            )
            action = self.sealer.seal(action)
            stored = self.database.create_workflow_action(
                action,
                scheduler_id="workflow-v4",
                scheduler_state=plan.scheduler_state,
            )
            result["action"] = {
                "action_id": stored["action_id"],
                "action_kind": stored["action_kind"],
                "operator": stored["operator"],
                "board_revision": stored["board_revision"],
                "candidate_seal": str(
                    (stored.get("artifacts") or [{}])[0].get("sha256") or ""
                ),
            }
        except (ControlDatabaseError, OSError, ValueError) as exc:
            result["errors"].append(str(exc))
        return result

    def _task_profile_path(self, operator: str) -> Path:
        try:
            registration = self.database.operator_for_display_name(operator)
        except ControlDatabaseError:
            fallback = (
                self.root
                / "operators_workspace"
                / operator
                / "TASK_EXECUTION_PROFILE.json"
            )
            if fallback.is_file():
                return fallback
            raise
        return task_execution_profile_path(self.root, registration)
