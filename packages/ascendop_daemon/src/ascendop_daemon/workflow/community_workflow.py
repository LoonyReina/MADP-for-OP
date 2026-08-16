from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.workflow_profiles import (
    WorkflowInstance,
    WorkflowGate,
    WorkflowProfile,
    WorkflowProfileError,
    WorkflowOperationContract,
    WorkflowSnapshot,
    relative_path,
)


STATE_SCHEMA = "ascendop.community-workflow-state.v1"

CHECKPOINT_TRANSITIONS: dict[str, dict[str, frozenset[str]]] = {
    "design": {
        "not_submitted": frozenset({"draft"}),
        "draft": frozenset({"pr_open"}),
        "pr_open": frozenset({"review_changes", "merged"}),
        "review_changes": frozenset({"pr_open", "merged"}),
        "merged": frozenset(),
    },
    "acceptance": {
        "not_submitted": frozenset({"ready"}),
        "ready": frozenset({"submitted"}),
        "submitted": frozenset({"feedback", "first_pass", "rejected"}),
        "feedback": frozenset({"ready", "first_pass", "rejected"}),
        "rejected": frozenset({"ready"}),
        "first_pass": frozenset(),
    },
    "code": {
        "not_started": frozenset({"requirement_issue_open"}),
        "requirement_issue_open": frozenset({"pr_open"}),
        "pr_open": frozenset({"review_changes", "merged"}),
        "review_changes": frozenset({"pr_open", "merged"}),
        "merged": frozenset(),
    },
}

EXTERNAL_ACTION_STATES = frozenset(
    {
        "draft",
        "prepared",
        "needs_authorization",
        "authorized",
        "posted",
        "cancelled",
    }
)
FEEDBACK_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CannCommunityTaskProfile(WorkflowProfile):
    profile_id = "cann-community-task.v1"
    profile_revision = "1.0"

    def discover(self) -> tuple[WorkflowInstance, ...]:
        index_path = self._index_path()
        index = read_object(index_path)
        tasks = index.get("tasks")
        if not isinstance(tasks, list):
            raise WorkflowProfileError(
                f"community task index requires a tasks list: {index_path}"
            )
        instances: list[WorkflowInstance] = []
        for row in tasks:
            if not isinstance(row, dict):
                raise WorkflowProfileError(
                    f"community task index rows must be objects: {index_path}"
                )
            task_key = required_string(row, "task_key", source=index_path)
            task_center_id = required_string(
                row,
                "task_center_id",
                source=index_path,
            )
            task_json = safe_relative_file(
                index_path.parent,
                required_string(row, "task_path", source=index_path),
            )
            task_dir = task_json.parent
            state_path = task_dir / "WORKFLOW_STATE.json"
            instances.append(
                WorkflowInstance(
                    profile_id=self.profile_id,
                    profile_revision=self.profile_revision,
                    instance_id=(
                        f"cann-community:{task_center_id}:{task_key}"
                    ),
                    domain="competition",
                    season_id="cann-community-2026",
                    subject_kind="community-task",
                    subject_id=task_key,
                    state_path=relative_path(self.root, state_path),
                    metadata={
                        "task_center_id": task_center_id,
                        "task_path": relative_path(self.root, task_json),
                        "spec_name": row.get("spec_name"),
                        "account_stage": row.get("account_stage"),
                    },
                )
            )
        return tuple(instances)

    def read_snapshot(self, instance: WorkflowInstance) -> WorkflowSnapshot:
        if instance.profile_id != self.profile_id:
            raise WorkflowProfileError(
                f"instance belongs to {instance.profile_id}, not {self.profile_id}"
            )
        state_path = safe_root_file(self.root, instance.state_path)
        state = read_object(state_path)
        validate_community_state(state, instance=instance, source=state_path)
        task_path = safe_root_file(
            self.root,
            required_metadata_string(instance, "task_path"),
        )
        task = read_object(task_path)
        progress_path = task_path.parent / "PROGRESS_STATE.json"
        question_board_path = task_path.parent / "community_questions" / "BOARD.json"
        progress = read_object(progress_path) if progress_path.is_file() else {}
        board = read_object(question_board_path) if question_board_path.is_file() else {}
        warnings = community_warnings(state, progress)
        return WorkflowSnapshot(
            instance=instance,
            generation=state["generation"],
            state=state["lifecycle"]["derived_stage"],
            checkpoints=state["checkpoints"],
            context={
                "task": {
                    "title": task.get("task", {}).get("title"),
                    "deadline": task.get("task", {}).get(
                        "internal_fail_closed_deadline"
                    ),
                    "execution_profile": task.get("execution_profile", {}),
                },
                "progress": state["progress"],
                "local_test": state["local_test"],
                "external_actions": state["external_actions"],
                "official_feedback": state["official_feedback"],
                "pending_verified_feedback": pending_verified_feedback(state),
                "question_board": {
                    "path": relative_path(self.root, question_board_path),
                    "status": board.get("status"),
                    "question_count": len(board.get("questions", []))
                    if isinstance(board.get("questions"), list)
                    else 0,
                },
                "ownership": {
                    "local_technical_execution": "daemon-engine",
                    "reminders": "assistant",
                    "external_publication": "assistant-with-exact-user-authorization",
                },
            },
            warnings=tuple(warnings),
        )

    def derive_gates(
        self,
        snapshot: WorkflowSnapshot,
    ) -> tuple[WorkflowGate, ...]:
        local_test = snapshot.context.get("local_test")
        if not isinstance(local_test, dict):
            return ()
        next_gate = local_test.get("next_gate")
        if next_gate is None:
            return ()
        if not isinstance(next_gate, dict):
            raise WorkflowProfileError(
                f"community next_gate must be an object: "
                f"{snapshot.instance.instance_id}"
            )
        operation_type = gate_string(next_gate, "operation_type")
        operation_version = gate_string(next_gate, "operation_version")
        contract = self.operation_contract(operation_type, operation_version)
        if contract.external_side_effect:
            raise WorkflowProfileError(
                "community daemon gates cannot perform external side effects"
            )
        state = gate_string(next_gate, "state")
        if state not in {"ready", "blocked", "running", "completed"}:
            raise WorkflowProfileError(
                f"invalid community next_gate state: {state}"
            )
        context_paths_value = next_gate.get("context_paths", [])
        if not isinstance(context_paths_value, list) or any(
            not isinstance(item, str) or not item for item in context_paths_value
        ):
            raise WorkflowProfileError(
                "community next_gate.context_paths must be a string list"
            )
        context_paths = list(context_paths_value)
        if snapshot.context.get("pending_verified_feedback"):
            context_paths.append(snapshot.instance.state_path)
        return (
            WorkflowGate(
                instance_id=snapshot.instance.instance_id,
                gate_id=gate_string(next_gate, "gate_id"),
                checkpoint=gate_string(next_gate, "checkpoint"),
                owner=contract.owner,
                operation_type=operation_type,
                state_generation=snapshot.generation,
                actionable=state == "ready",
                reason=gate_string(next_gate, "reason"),
                context_paths=tuple(dict.fromkeys(context_paths)),
            ),
        )

    def operation_contracts(self) -> tuple[WorkflowOperationContract, ...]:
        return (
            WorkflowOperationContract(
                operation_type="community.local-validation",
                operation_version="v1",
                owner="daemon",
                required_capabilities=(
                    "flow-v3",
                    "engine-archive",
                    "correctness-first",
                ),
                ingest_adapter="community.local-result.v1",
            ),
        )

    def _index_path(self) -> Path:
        configured = self.config.get("index_path", "operators/cann-community/index.json")
        if not isinstance(configured, str) or not configured:
            raise WorkflowProfileError(
                "community workflow profile config.index_path must be non-empty"
            )
        return safe_root_file(self.root, configured)


def validate_community_state(
    state: dict[str, Any],
    *,
    instance: WorkflowInstance,
    source: Path,
) -> None:
    if state.get("schema") != STATE_SCHEMA:
        raise WorkflowProfileError(
            f"unsupported community workflow state schema in {source}: "
            f"{state.get('schema')}"
        )
    if state.get("profile_id") != instance.profile_id:
        raise WorkflowProfileError(f"community profile identity mismatch: {source}")
    if state.get("task_key") != instance.subject_id:
        raise WorkflowProfileError(f"community task identity mismatch: {source}")
    if state.get("task_center_id") != instance.metadata.get("task_center_id"):
        raise WorkflowProfileError(f"community task center identity mismatch: {source}")
    generation = state.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise WorkflowProfileError(
            f"community state generation must be a positive integer: {source}"
        )
    lifecycle = require_object(state, "lifecycle", source=source)
    for key in ("observed_stage", "derived_stage"):
        required_string(lifecycle, key, source=source)
    checkpoints = require_object(state, "checkpoints", source=source)
    for checkpoint, transitions in CHECKPOINT_TRANSITIONS.items():
        row = require_object(checkpoints, checkpoint, source=source)
        checkpoint_state = required_string(row, "state", source=source)
        if checkpoint_state not in transitions:
            raise WorkflowProfileError(
                f"invalid {checkpoint} checkpoint state {checkpoint_state!r}: {source}"
            )
    validate_checkpoint_prerequisites(checkpoints, source=source)
    progress = require_object(state, "progress", source=source)
    required_string(progress, "status", source=source)
    local_test = require_object(state, "local_test", source=source)
    result_root = required_string(local_test, "result_root", source=source)
    normalized_result_root = result_root.replace("\\", "/")
    if normalized_result_root.startswith("TestUtils/") or normalized_result_root.startswith(
        "operators_testresult/"
    ):
        raise WorkflowProfileError(
            f"community result_root must not use CANNJudge state roots: {source}"
        )
    next_gate = local_test.get("next_gate")
    if next_gate is not None:
        if not isinstance(next_gate, dict):
            raise WorkflowProfileError(
                f"community local_test.next_gate must be an object: {source}"
            )
        for key in (
            "gate_id",
            "checkpoint",
            "operation_type",
            "operation_version",
            "state",
            "reason",
        ):
            required_string(next_gate, key, source=source)
        if next_gate["operation_type"] != "community.local-validation":
            raise WorkflowProfileError(
                f"community next_gate operation is not daemon-local: {source}"
            )
    external_actions = state.get("external_actions")
    if not isinstance(external_actions, list):
        raise WorkflowProfileError(
            f"community external_actions must be a list: {source}"
        )
    for position, action in enumerate(external_actions):
        if not isinstance(action, dict):
            raise WorkflowProfileError(
                f"community external action {position} must be an object: {source}"
            )
        required_string(action, "action_id", source=source)
        action_state = required_string(action, "state", source=source)
        if action_state not in EXTERNAL_ACTION_STATES:
            raise WorkflowProfileError(
                f"invalid external action state {action_state!r}: {source}"
            )
        if action_state in {"authorized", "posted"}:
            required_string(action, "payload_sha256", source=source)
            required_string(action, "authorization_id", source=source)
    official_feedback = state.get("official_feedback")
    if not isinstance(official_feedback, list):
        raise WorkflowProfileError(
            f"community official_feedback must be a list: {source}"
        )
    seen_feedback: set[str] = set()
    for position, feedback in enumerate(official_feedback):
        if not isinstance(feedback, dict):
            raise WorkflowProfileError(
                f"community official feedback {position} must be an object: {source}"
            )
        feedback_id = required_string(feedback, "feedback_id", source=source)
        if feedback_id in seen_feedback:
            raise WorkflowProfileError(
                f"duplicate community feedback id {feedback_id}: {source}"
            )
        seen_feedback.add(feedback_id)
        status = required_string(feedback, "status", source=source)
        if status not in {"verified", "consumed"}:
            raise WorkflowProfileError(
                f"invalid community feedback status {status!r}: {source}"
            )
        required_string(feedback, "source_url", source=source)
        digest = required_string(feedback, "content_sha256", source=source)
        if not FEEDBACK_SHA256.fullmatch(digest):
            raise WorkflowProfileError(
                f"invalid community feedback SHA-256: {source}"
            )
        if status == "consumed":
            consumed_generation = feedback.get("consumed_at_generation")
            if (
                isinstance(consumed_generation, bool)
                or not isinstance(consumed_generation, int)
                or consumed_generation < 1
            ):
                raise WorkflowProfileError(
                    f"consumed feedback requires a positive generation: {source}"
                )
    extensions = state.get("extensions")
    if not isinstance(extensions, dict):
        raise WorkflowProfileError(
            f"community extensions must be an object: {source}"
        )


def validate_checkpoint_transition(
    checkpoint: str,
    previous: str,
    current: str,
) -> None:
    transitions = CHECKPOINT_TRANSITIONS.get(checkpoint)
    if transitions is None or previous not in transitions or current not in transitions:
        raise WorkflowProfileError(
            f"unknown checkpoint transition {checkpoint}:{previous}->{current}"
        )
    if previous != current and current not in transitions[previous]:
        raise WorkflowProfileError(
            f"illegal checkpoint transition {checkpoint}:{previous}->{current}"
        )


def validate_checkpoint_prerequisites(
    checkpoints: dict[str, Any],
    *,
    source: Path,
) -> None:
    design = checkpoints["design"]["state"]
    acceptance = checkpoints["acceptance"]["state"]
    code = checkpoints["code"]["state"]
    if acceptance not in {"not_submitted", "ready"} and design != "merged":
        raise WorkflowProfileError(
            f"acceptance submission requires a merged design checkpoint: {source}"
        )
    if code != "not_started" and acceptance != "first_pass":
        raise WorkflowProfileError(
            f"code contribution requires first-pass acceptance: {source}"
        )


def community_warnings(
    state: dict[str, Any],
    progress: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if state["progress"]["status"] == "at_risk_or_overdue":
        warnings.append(
            "progress evidence is at risk or overdue; Assistant owns follow-up"
        )
    external_pending = [
        row
        for row in state["external_actions"]
        if row.get("state") in {"draft", "prepared", "needs_authorization", "authorized"}
    ]
    if external_pending:
        warnings.append(
            f"{len(external_pending)} Assistant-owned external action(s) remain pending"
        )
    observed = progress.get("deadline_assessment", {}).get("status")
    if observed and observed != state["progress"]["status"]:
        warnings.append(
            "durable progress status differs from the latest observation sidecar"
        )
    return warnings


def pending_verified_feedback(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in state["official_feedback"]
        if item.get("status") == "verified"
    ]


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkflowProfileError(f"cannot read JSON object {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise WorkflowProfileError(f"JSON value must be an object: {path}")
    return value


def require_object(
    value: dict[str, Any],
    key: str,
    *,
    source: Path,
) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise WorkflowProfileError(f"{source} requires object {key}")
    return item


def required_string(value: dict[str, Any], key: str, *, source: Path) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise WorkflowProfileError(f"{source} requires non-empty {key}")
    return item


def required_metadata_string(instance: WorkflowInstance, key: str) -> str:
    value = instance.metadata.get(key)
    if not isinstance(value, str) or not value:
        raise WorkflowProfileError(
            f"workflow instance {instance.instance_id} requires metadata.{key}"
        )
    return value


def gate_string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise WorkflowProfileError(f"community gate requires non-empty {key}")
    return item


def safe_relative_file(parent: Path, value: str) -> Path:
    candidate = (parent / value).resolve()
    try:
        candidate.relative_to(parent.resolve())
    except ValueError as exc:
        raise WorkflowProfileError(f"path escapes community index root: {value}") from exc
    return candidate


def safe_root_file(root: Path, value: str) -> Path:
    candidate = (root / value).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkflowProfileError(f"path escapes repository root: {value}") from exc
    return candidate
