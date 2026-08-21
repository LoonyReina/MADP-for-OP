from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

from ascendop_protocol.actor import flow_v5_catalog_digest
from ascendop_protocol.agent import AGENT_OUTPUT_AUTHORING_REVISION

from ascendop_daemon.control_plane.control_database import (
    ControlDatabase,
    ControlDatabaseError,
)
from ascendop_daemon.automation.agent_outputs import (
    agent_output_contracts_digest,
    build_agent_output_contracts,
)
from ascendop_daemon.automation.agent_context import (
    SOURCE_IDENTITY_SCHEMA,
    build_agent_context_evidence,
)
from ascendop_daemon.core.models import (
    ActionKind,
    BoardSnapshot,
    DaemonConfig,
    GateDecision,
)
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.workflow.gate_engine import GateEngine
from ascendop_daemon.workflow.policy_pipeline import WorkflowPolicyPipeline


ROLE_GATE_VALIDATION_SCHEMA = "ascendop.effective-role-gate-validation.v1"

_ROLE_ACTION_CONTRACTS = {
    "solver": {
        "write_scope": [
            "op_host",
            "op_kernel",
            "SOLVER_BLOCKER.md",
            "SKILL_APPLICATION_REVIEW.md",
        ],
        "permitted_operations": [
            "inspect-evidence",
            "edit-operator-source",
            "write-blocker",
        ],
    },
    "tester": {
        "write_scope": [
            "case_specs.py",
            "test_op.py",
            "README.md",
            "case_contract.json",
        ],
        "permitted_operations": [
            "inspect-evidence",
            "edit-casegen-source",
            "write-coverage-contract",
        ],
    },
}


class SnapshotReader(Protocol):
    def read(self) -> BoardSnapshot: ...


def latest_recovery_evidence(
    root: Path,
    operator: str,
) -> tuple[Path, str] | None:
    recovery_root = (
        root
        / ".ascendop-work"
        / "quarantine"
        / "incomplete-pending"
        / operator
    )
    candidates = list(recovery_root.glob("*/INCOMPLETE_PENDING_RECOVERY.json"))
    if not candidates:
        return None
    latest = max(
        candidates,
        key=lambda path: (path.stat().st_mtime_ns, path.as_posix()),
    )
    return latest, hashlib.sha256(latest.read_bytes()).hexdigest()


def role_workspace(root: Path, operator: str, role: str) -> str:
    candidate = (
        Path("operators_workspace") / operator
        if role == "solver"
        else Path("TestUtils") / "casegen" / operator
    )
    if (root / candidate).is_dir():
        return candidate.as_posix()
    raise ValueError(f"{operator} {role} workspace is missing")


def normalize_relative_path(value: str) -> str:
    path = value.strip().replace("\\", "/")
    if path.startswith("/") or any(part == ".." for part in path.split("/")):
        return ""
    return path


def role_gate_identity(
    decision: GateDecision,
    *,
    role: str,
    runbook_path: str,
    runbook_digest: str,
    operator_id: str,
) -> dict[str, str]:
    row = decision.row
    identity = {
        "gate_validation_schema": ROLE_GATE_VALIDATION_SCHEMA,
        "season": row.season,
        "operator": row.op,
        "operator_id": operator_id,
        "role": role,
        "gate_stage": row.gate_stage,
        "next_owner": row.next_owner,
        "wakeups": row.wakeups,
        "next_command": row.next_command,
        "runbook_path": runbook_path,
        "runbook_digest": runbook_digest,
        "agent_output_authoring_revision": AGENT_OUTPUT_AUTHORING_REVISION,
        "flow_v5_catalog_digest": flow_v5_catalog_digest(),
    }
    identity["role_contract_digest"] = role_action_contract_digest(role)
    return identity


def role_action_contract(role: str) -> dict[str, list[str]]:
    normalized = role.strip().lower()
    try:
        contract = _ROLE_ACTION_CONTRACTS[normalized]
    except KeyError as exc:
        raise ValueError("role must be solver or tester") from exc
    return {key: list(values) for key, values in contract.items()}


def role_action_contract_digest(role: str) -> str:
    return hashlib.sha256(
        json.dumps(
            role_action_contract(role),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def role_gate_digest(identity: dict[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(
            identity,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def role_gate_validation_command(
    operator: str,
    role: str,
    board_digest: str,
) -> str:
    return (
        "python tools/tester_daemon/daemon.py workflow-role-gate-validate "
        f"--operator {operator} --role {role} --board-digest {board_digest}"
    )


def current_effective_role_gate(
    root: Path,
    config: DaemonConfig,
    *,
    operator: str,
    role: str,
    reader: SnapshotReader | None = None,
    policy: WorkflowPolicyPipeline | None = None,
    database: ControlDatabase | None = None,
) -> dict[str, Any] | None:
    normalized_role = role.strip().lower()
    if normalized_role not in {"solver", "tester"}:
        raise ValueError("role must be solver or tester")
    if database is None:
        raise ValueError("control database is required for Agent gate validation")
    try:
        operator_id = str(
            database.operator_for_display_name(operator)["operator_id"]
        )
    except (ControlDatabaseError, ValueError):
        return None
    snapshot = (reader or StateReader(root, config)).read()
    pipeline = policy or WorkflowPolicyPipeline(
        root,
        config,
        database=database,
    )
    decisions = pipeline.apply(
        GateEngine(config).evaluate(snapshot.rows, snapshot.transport)
    )
    expected_action = (
        ActionKind.NOTIFY_SOLVER
        if normalized_role == "solver"
        else ActionKind.NOTIFY_TESTER_CASEGEN
    )
    for decision in decisions:
        if decision.row.op != operator or decision.action != expected_action:
            continue
        runbook = normalize_relative_path(
            decision.row.solver_goal
            if normalized_role == "solver"
            else decision.row.tester_goal
        )
        runbook_file = root / runbook
        if not runbook or not runbook_file.is_file():
            return None
        runbook_digest = hashlib.sha256(runbook_file.read_bytes()).hexdigest()
        identity = role_gate_identity(
            decision,
            role=normalized_role,
            runbook_path=runbook,
            runbook_digest=runbook_digest,
            operator_id=operator_id,
        )
        workspace_path = role_workspace(root, operator, normalized_role)
        evidence = build_agent_context_evidence(
            root,
            operator=operator,
            workspace=root / workspace_path,
        )
        identity["source_identity_schema"] = SOURCE_IDENTITY_SCHEMA
        identity["execution_source_digest"] = evidence["source_before_digest"]
        identity["workflow_evidence_digest"] = evidence[
            "workflow_evidence_digest"
        ]
        identity["reference_evidence_digest"] = evidence[
            "reference_evidence_digest"
        ]
        recovery = latest_recovery_evidence(root, operator)
        if recovery is not None:
            identity["recovery_epoch"] = recovery[1]
        proposal_key = role_gate_digest(identity)
        output_contracts = build_agent_output_contracts(
            root,
            campaign=decision.row.season,
            operator=operator,
            role=normalized_role,
            gate_stage=decision.row.gate_stage,
            next_command=decision.row.next_command,
            action_descriptor=decision.row.action_descriptor,
            source_digest=evidence["source_before_digest"],
            proposal_key=proposal_key,
        )
        identity["output_contracts_digest"] = agent_output_contracts_digest(
            output_contracts
        )
        return {
            "identity": identity,
            "board_digest": role_gate_digest(identity),
            "captured_at": snapshot.captured_at,
        }
    return None


def validate_effective_role_gate(
    root: Path,
    config: DaemonConfig,
    *,
    operator: str,
    role: str,
    board_digest: str,
    reader: SnapshotReader | None = None,
    policy: WorkflowPolicyPipeline | None = None,
    database: ControlDatabase | None = None,
) -> dict[str, Any]:
    current = current_effective_role_gate(
        root,
        config,
        operator=operator,
        role=role,
        reader=reader,
        policy=policy,
        database=database,
    )
    actual_digest = str((current or {}).get("board_digest") or "")
    return {
        "schema": ROLE_GATE_VALIDATION_SCHEMA,
        "valid": bool(actual_digest and actual_digest == board_digest),
        "operator": operator,
        "role": role,
        "expected_board_digest": board_digest,
        "actual_board_digest": actual_digest,
        "effective_gate": current,
        "raw_board_authoritative": False,
    }
