from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ascendop_protocol.competition import (
    canonical_tree_digest,
    lineage_tree_digest,
)
from ascendop_protocol.workflow import (
    BOARD_ACTION_SCHEMA,
    WORKFLOW_ACTION_SCHEMA,
    validate_board_action,
    validate_workflow_action,
)

from ascendop_daemon.core.models import (
    ActionKind,
    GateDecision,
    extract_row_test_version,
    utc_now_iso,
)
from ascendop_daemon.storage.control_types import SCHEMA_VERSION
from ascendop_daemon.workflow.case_tree import case_package_paths


ACTION_OPERATIONS: dict[ActionKind, set[str]] = {
    ActionKind.GENERATE_WORKSPACE: {"gitpartner-run-msopgen"},
    ActionKind.PREPARE_SUBMIT: {"prepare-submit"},
    ActionKind.DISPATCH_SUBMIT: {"gitpartner-run-submit"},
    ActionKind.RESTORE_SUBMIT: {"restore-submit"},
    ActionKind.REQUEUE_SUBMIT: {"requeue-submit"},
    ActionKind.REPAIR_QUEUE: {"set-queue-status"},
    ActionKind.RECONCILE_TERMINAL_RESULT: {"reconcile-terminal-result"},
    ActionKind.ADVANCE_RELEASE: {
        "promote-release",
        "create-v1-regression-sentinel",
        "record-v1-regression-sentinel",
        "create-case-regression-sentinel",
        "record-case-regression-sentinel",
    },
    ActionKind.RECOVER_BLOCKED: {"gitpartner-recover-blocked"},
    ActionKind.RECOVER_GITPARTNER_WORKTREE: {"gitpartner-recover-worktree"},
    ActionKind.HEARTBEAT_ACTIVE_REQUEST: {"gitpartner-heartbeat"},
    ActionKind.CANCEL_STALLED_REQUEST: {"gitpartner-cancel-stalled"},
    ActionKind.COLLECT_PROFILER_EVIDENCE: {"collect-profiler-evidence"},
    ActionKind.REGISTER_SOLVER_DIAGNOSTIC: {"register-solver-diagnostic"},
    ActionKind.PUBLISH_TEST_REQUEST: {"publish-test-request"},
}


def workflow_action_from_decision(
    root: Path,
    decision: GateDecision,
    *,
    producer_generation: str,
    task_profile_path: Path | None = None,
) -> dict[str, Any]:
    arguments = parse_typed_harness_arguments(decision)
    test_version = extract_row_test_version(decision.row) or _test_version(arguments)
    board_identity = {
        "campaign": decision.row.season,
        "operator": decision.row.op,
        "gate_stage": decision.row.gate_stage,
        "next_owner": decision.row.next_owner,
        "action_kind": decision.action.value,
        "test_version": test_version,
        "arguments": arguments,
        "producer_generation": producer_generation,
    }
    board_revision = _digest(board_identity)
    idempotency_key = ":".join(
        (
            "workflow-v4",
            decision.row.season,
            decision.row.op,
            decision.action.value,
            board_revision,
        )
    )
    action_id = f"wfa-{hashlib.sha256(idempotency_key.encode('utf-8')).hexdigest()[:32]}"
    origin_workspace = _origin_workspace(root, decision.row.op, test_version)
    candidate_identity, artifacts = _candidate_evidence(
        root,
        decision.row.season,
        decision.row.op,
        test_version,
        arguments,
        task_profile_path=task_profile_path,
    )
    return validate_workflow_action(
        {
            "schema": WORKFLOW_ACTION_SCHEMA,
            "action_id": action_id,
            "idempotency_key": idempotency_key,
            "action_kind": decision.action.value,
            "campaign": decision.row.season,
            "operator": decision.row.op,
            "test_version": test_version,
            "board_revision": board_revision,
            "producer_generation": producer_generation,
            "control_schema": SCHEMA_VERSION,
            "origin_workspace": origin_workspace,
            "priority": int(decision.priority),
            "resource_class": _resource_class(decision.action),
            "arguments": arguments,
            "candidate_identity": candidate_identity,
            "artifacts": artifacts,
            "parent_trace_id": "",
            "deadline_at": "",
            "retry_policy_ref": "workflow-v4-central-retry",
            "created_at": utc_now_iso(),
        }
    )


def parse_typed_harness_arguments(decision: GateDecision) -> dict[str, Any]:
    allowed = ACTION_OPERATIONS.get(decision.action)
    if not allowed:
        raise ValueError(f"action has no typed executor contract: {decision.action.value}")
    descriptor = validate_board_action(
        decision.action_descriptor or decision.row.action_descriptor
    )
    if descriptor.get("schema") != BOARD_ACTION_SCHEMA:
        raise ValueError("board action descriptor is missing or unsupported")
    operation = str(descriptor.get("operation") or "")
    if operation not in allowed:
        raise ValueError(
            f"{decision.action.value} cannot publish operation {operation}"
        )
    raw_positional = descriptor.get("positional", [])
    raw_options = descriptor.get("options", {})
    if not isinstance(raw_positional, list) or not all(
        isinstance(value, str) for value in raw_positional
    ):
        raise ValueError("board action positional arguments are invalid")
    if not isinstance(raw_options, dict) or not all(
        isinstance(key, str)
        and isinstance(value, (str, int, float, bool, type(None)))
        for key, value in raw_options.items()
    ):
        raise ValueError("board action options are invalid")
    positional = list(raw_positional)
    options = dict(raw_options)
    return {"operation": operation, "positional": positional, "options": options}


def _test_version(arguments: dict[str, Any]) -> str:
    positional = arguments.get("positional", [])
    if len(positional) > 1:
        return str(positional[1])
    options = arguments.get("options", {})
    return str(options.get("test_version") or "")


def _origin_workspace(root: Path, operator: str, test_version: str) -> str:
    for relative in (
        Path("TestUtils") / "pending" / operator / test_version,
        Path("TestUtils") / "submit" / operator / test_version,
        Path("operators_testresult") / operator / test_version,
        Path("operators_workspace") / operator,
    ):
        if (root / relative).exists():
            return relative.as_posix()
    return (Path("operators_workspace") / operator).as_posix()


def _candidate_evidence(
    root: Path,
    campaign: str,
    operator: str,
    test_version: str,
    arguments: dict[str, Any],
    *,
    task_profile_path: Path | None = None,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    lineage_path = (
        root / "TestUtils" / "pending" / operator / test_version / "SOURCE_LINEAGE.json"
    )
    if not lineage_path.is_file():
        lineage_path = (
            root / "TestUtils" / "submit" / operator / test_version
            / "pending_snapshot" / "SOURCE_LINEAGE.json"
        )
    lineage: dict[str, Any] = {}
    if lineage_path.is_file():
        try:
            raw = json.loads(lineage_path.read_text(encoding="utf-8"))
            lineage = raw if isinstance(raw, dict) else {}
        except (OSError, json.JSONDecodeError):
            lineage = {}
    declared_lineage_digest = str(
        (lineage.get("candidate") or {}).get("sha256", "")
        if isinstance(lineage.get("candidate"), dict)
        else ""
    )
    source_root = lineage_path.parent / "source_snapshot"
    execution_source_digest = (
        canonical_tree_digest(source_root) if source_root.is_dir() else ""
    )
    actual_lineage_digest = (
        lineage_tree_digest(source_root) if source_root.is_dir() else ""
    )
    lineage_digest = (
        declared_lineage_digest
        if declared_lineage_digest == actual_lineage_digest
        else ""
    )
    profile_path = (
        task_profile_path.resolve()
        if task_profile_path is not None
        else (root / "operators_workspace" / operator / "TASK_EXECUTION_PROFILE.json").resolve()
    )
    try:
        profile_path.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError("task execution profile escapes workspace") from exc
    task_profile_digest = _file_digest(profile_path)
    options = arguments.get("options", {})
    case_version = str(options.get("case_version", ""))
    case_root = root / "TestUtils" / "casegen" / operator / "case" / case_version
    case_digest = (
        _tree_digest(case_root, paths=case_package_paths(case_root))
        if case_version and case_root.is_dir()
        else ""
    )
    artifacts: list[dict[str, Any]] = []
    for logical_name, path in (
        ("source_lineage", lineage_path),
        ("task_execution_profile", profile_path),
    ):
        if not path.is_file():
            continue
        artifacts.append(
            {
                "logical_name": logical_name,
                "sha256": _file_digest(path),
                "size": path.stat().st_size,
                "media_type": "application/json",
                "uri": path.relative_to(root).as_posix(),
            }
        )
    if arguments.get("operation") == "reconcile-terminal-result":
        source_result = str(options.get("source_result") or "")
        source_path = (root / source_result).resolve()
        try:
            source_path.relative_to(root.resolve())
        except ValueError as exc:
            raise ValueError("terminal result source escapes workspace") from exc
        for logical_name, path in (
            ("archived_terminal_result", source_path),
            ("archived_engine_return", source_path.parent / "ENGINE_RETURN.json"),
        ):
            if not path.is_file():
                raise ValueError(f"terminal reconciliation artifact is missing: {path}")
            artifacts.append(
                {
                    "logical_name": logical_name,
                    "sha256": _file_digest(path),
                    "size": path.stat().st_size,
                    "media_type": (
                        "text/markdown"
                        if path.suffix.lower() == ".md"
                        else "application/json"
                    ),
                    "uri": path.relative_to(root).as_posix(),
                }
            )
    return (
        {
            "execution_source_digest": execution_source_digest,
            "lineage_tree_digest": lineage_digest,
            "official_project_digest": "",
            "case_package_digest": case_digest,
            "task_profile_digest": task_profile_digest,
            "environment_generation": "",
            "endpoint_affinity": "",
        },
        artifacts,
    )


def _resource_class(action: ActionKind) -> str:
    return (
        "device"
        if action
        in {
            ActionKind.GENERATE_WORKSPACE,
            ActionKind.DISPATCH_SUBMIT,
            ActionKind.RECOVER_BLOCKED,
            ActionKind.RECOVER_GITPARTNER_WORKTREE,
            ActionKind.HEARTBEAT_ACTIVE_REQUEST,
            ActionKind.CANCEL_STALLED_REQUEST,
            ActionKind.COLLECT_PROFILER_EVIDENCE,
        }
        else "local"
    )


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def _tree_digest(root: Path, *, paths: tuple[Path, ...] | None = None) -> str:
    digest = hashlib.sha256()
    candidates = paths if paths is not None else tuple(root.rglob("*"))
    for path in sorted((item for item in candidates if item.is_file())):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        content_digest = hashlib.sha256(path.read_bytes()).digest()
        digest.update(content_digest)
    return digest.hexdigest()


def _digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
