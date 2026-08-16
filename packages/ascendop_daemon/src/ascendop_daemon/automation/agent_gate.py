from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from ascendop_protocol.agent import (
    AGENT_ACTION_SCHEMA,
    AGENT_CONTEXT_SNAPSHOT_SCHEMA,
)
from ascendop_protocol.workflow import SOLVER_STEWARD_ESCALATION_STATE
from ascendop_control.storage.errors import ControlRepositoryError

from ascendop_daemon.automation.agent_context import (
    SOURCE_IDENTITY_SCHEMA,
    build_agent_context_evidence,
)
from ascendop_daemon.automation.agent_outputs import (
    agent_output_contracts_digest,
    build_agent_output_contracts,
    solver_pending_reservation,
)
from ascendop_daemon.automation.gate_validation import (
    latest_recovery_evidence,
    normalize_relative_path,
    role_gate_digest,
    role_gate_identity,
    role_gate_validation_command,
    role_action_contract,
    role_workspace,
)
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.core.models import ActionKind, BoardSnapshot, DaemonConfig, GateDecision
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.workflow.gate_engine import GateEngine
from ascendop_daemon.workflow.policy_pipeline import WorkflowPolicyPipeline


class SnapshotReader(Protocol):
    def read(self) -> BoardSnapshot: ...


class AgentGateCoordinator:
    """Materialize policy-effective Solver/Tester gates as Agent actions."""

    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        config: DaemonConfig,
        *,
        producer_generation: str,
        max_turn_seconds: int,
        reader: SnapshotReader | None = None,
        policy: WorkflowPolicyPipeline | None = None,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.config = config
        self.producer_generation = str(producer_generation).strip()
        self.max_turn_seconds = int(max_turn_seconds)
        if not self.producer_generation:
            raise ValueError("Agent gate producer generation is required")
        if not 30 <= self.max_turn_seconds <= 3600:
            raise ValueError("Agent max turn seconds must be in [30, 3600]")
        self.reader = reader or StateReader(self.root, config)
        self.gate_engine = GateEngine(config)
        self.policy = policy or WorkflowPolicyPipeline(
            self.root,
            config,
            database=database,
        )

    def run_once(self) -> dict[str, Any]:
        try:
            snapshot = self.reader.read()
            decisions = self.policy.apply(
                self.gate_engine.evaluate(snapshot.rows, snapshot.transport)
            )
        except Exception as exc:
            return {
                "board_rows": 0,
                "eligible_count": 0,
                "actions": [],
                "steward_escalations": [],
                "errors": [str(exc)],
            }
        actions: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        eligible = 0
        for decision in decisions:
            if decision.action not in {
                ActionKind.NOTIFY_SOLVER,
                ActionKind.NOTIFY_TESTER_CASEGEN,
            }:
                continue
            eligible += 1
            try:
                actions.append(self._publish(decision, snapshot.captured_at))
            except (OSError, ValueError, ControlDatabaseError, ControlRepositoryError) as exc:
                errors.append({"operator": decision.row.op, "error": str(exc)})
        cancelled = self.database.synchronize_workflow_agent_gate_heads(
            actions,
            observed_at=snapshot.captured_at,
        )
        steward_escalations = [
            {
                "gate_stage": row.gate_stage,
                "next_owner": row.next_owner,
                "wakeups": row.wakeups,
                "next_command": row.next_command,
                "action_descriptor": dict(row.action_descriptor),
            }
            for row in snapshot.rows
            if row.gate_stage == SOLVER_STEWARD_ESCALATION_STATE
        ]
        return {
            "captured_at": snapshot.captured_at,
            "board_rows": len(snapshot.rows),
            "eligible_count": eligible,
            "actions": actions,
            "steward_escalations": steward_escalations,
            "cancelled_obsolete": cancelled,
            "errors": errors,
        }

    def _publish(self, decision: GateDecision, captured_at: str) -> dict[str, Any]:
        row = decision.row
        role = "solver" if decision.action == ActionKind.NOTIFY_SOLVER else "tester"
        runbook = normalize_relative_path(
            row.solver_goal if role == "solver" else row.tester_goal
        )
        if not runbook or not (self.root / runbook).is_file():
            raise ValueError(f"{row.op} {role} runbook is missing: {runbook}")
        workspace_path = role_workspace(self.root, row.op, role)
        workspace = self.root / workspace_path
        evidence = build_agent_context_evidence(
            self.root,
            operator=row.op,
            workspace=workspace,
        )
        registration = self.database.operator_for_display_name(row.op)
        operator_id = str(registration["operator_id"])
        agent_pool_id = _agent_pool_id(registration, role=role)
        identity = role_gate_identity(
            decision,
            role=role,
            operator_id=operator_id,
            runbook_path=runbook,
        )
        identity["source_identity_schema"] = SOURCE_IDENTITY_SCHEMA
        identity["execution_source_digest"] = evidence["source_before_digest"]
        identity["workflow_evidence_digest"] = evidence[
            "workflow_evidence_digest"
        ]
        identity["reference_evidence_digest"] = evidence[
            "reference_evidence_digest"
        ]
        recovery = latest_recovery_evidence(self.root, row.op)
        if recovery is not None:
            identity["recovery_epoch"] = recovery[1]
        proposal_key = role_gate_digest(identity)
        role_contract = role_action_contract(role)
        output_contracts = build_agent_output_contracts(
            self.root,
            campaign=row.season,
            operator=row.op,
            role=role,
            gate_stage=row.gate_stage,
            next_command=row.next_command,
            action_descriptor=row.action_descriptor,
            source_digest=evidence["source_before_digest"],
            proposal_key=proposal_key,
        )
        pending_repair_version = _pending_repair_version(output_contracts)
        pending_reservation = solver_pending_reservation(
            operator=row.op,
            gate_stage=row.gate_stage,
            action_descriptor=row.action_descriptor,
        )
        if row.gate_stage == "needs-pending-candidate" and pending_reservation is None:
            raise ValueError(
                f"{row.op} pending gate has no typed create-pending reservation"
            )
        if pending_repair_version:
            role_contract = {
                "write_scope": [],
                "permitted_operations": ["repair-pending-evidence"],
            }
        elif str(dict(row.action_descriptor or {}).get("operation") or "") in {
            "author-solver-blocker-contract",
            "author-solver-diagnostic-request",
        }:
            role_contract = {
                "write_scope": [],
                "permitted_operations": ["author-workflow-evidence"],
            }
        workflow_evidence_version = _workflow_evidence_version(output_contracts)
        if workflow_evidence_version:
            identity["workflow_evidence_version"] = workflow_evidence_version
        identity["output_contracts_digest"] = agent_output_contracts_digest(
            output_contracts
        )
        board_digest = role_gate_digest(identity)
        action_id = f"aga-{board_digest}"
        existing = self.database.agent_action(action_id)
        if existing is not None:
            return existing
        candidate_version = (
            pending_reservation[0]
            if pending_reservation is not None
            else pending_repair_version
            or workflow_evidence_version
            or _board_case_version(row.next_command, role=role)
            or _next_candidate_version(
                self.root,
                self.database,
                operator=row.op,
                operator_id=operator_id,
                role=role,
            )
        )
        iteration_id = f"agi-{board_digest}"
        runbook_digest = hashlib.sha256((self.root / runbook).read_bytes()).hexdigest()
        created_at = captured_at or _utc_now()
        action = {
            "schema": AGENT_ACTION_SCHEMA,
            "action_id": action_id,
            "idempotency_key": f"workflow-gate:{board_digest}",
            "iteration_id": iteration_id,
            "campaign": row.season,
            "operator_id": operator_id,
            "agent_pool_id": agent_pool_id,
            "role": role,
            "workflow_epoch": str(registration["registration_generation"]),
            "producer_generation": self.producer_generation,
            "board_revision": board_digest,
            "board_digest": board_digest,
            "runbook_path": runbook,
            "runbook_digest": runbook_digest,
            "origin_workspace": workspace_path,
            "candidate_version": candidate_version,
            "candidate_identity": {
                "origin": "workflow-gate",
                "display_name": row.op,
                "gate_stage": row.gate_stage,
                "next_command": row.next_command,
                "source_identity_schema": SOURCE_IDENTITY_SCHEMA,
                "execution_source_digest": evidence["source_before_digest"],
            },
            "write_scope": role_contract["write_scope"],
            "output_contracts": output_contracts,
            "tool_budget": {"max_turn_seconds": self.max_turn_seconds},
            "created_at": created_at,
        }
        context = {
            "schema": AGENT_CONTEXT_SNAPSHOT_SCHEMA,
            "snapshot_id": f"ags-{board_digest}",
            "iteration_id": iteration_id,
            "operator_id": operator_id,
            "role": role,
            "board_revision": board_digest,
            "board_digest": board_digest,
            "candidate_identity": action["candidate_identity"],
            "gate": {
                **identity,
                "validation_command": role_gate_validation_command(
                    row.op,
                    role,
                    board_digest,
                ),
            },
            "recent_results": evidence["recent_results"],
            "official_evidence": evidence["official_evidence"],
            "open_hypotheses": evidence["open_hypotheses"],
            "workflow_evidence": evidence["workflow_evidence"],
            "reference_projection": evidence["reference_projection"],
            "reference_evidence": evidence["reference_evidence"],
            "permitted_operations": role_contract["permitted_operations"],
            "created_at": created_at,
        }
        if role == "tester":
            self.database.reconcile_workflow_agent_candidate(
                operator_id=operator_id,
                role=role,
                candidate_version=candidate_version,
                keep_action_id=action_id,
            )
        return self.database.create_agent_action(action, context)


def _next_candidate_version(
    root: Path,
    database: ControlDatabase,
    *,
    operator: str,
    operator_id: str,
    role: str,
) -> str:
    if role == "tester":
        candidates = [
            path.name
            for path in (
                root / "TestUtils" / "casegen" / operator / "case"
            ).glob("case_v*")
            if path.is_dir()
        ]
        numbers = [
            int(match.group(1))
            for value in candidates
            if (match := re.fullmatch(r"case_v(\d+)", value, re.IGNORECASE))
        ]
        return f"case_v{(max(numbers, default=0) + 1):03d}"
    existing = [
        str(item["action"].get("candidate_version") or "")
        for item in database.agent_actions_v4()
        if item["operator_id"] == operator_id and item["role"] == role
    ]
    candidates = [path.name for path in (root / "operators_workspace" / operator).glob(f"{operator}_V*")]
    versions: list[tuple[int, int]] = []
    pattern = re.compile(rf"{re.escape(operator)}_V(\d+)(?:_(\d+))?", re.IGNORECASE)
    for value in (*candidates, *existing):
        match = pattern.fullmatch(value)
        if match:
            versions.append((int(match.group(1)), int(match.group(2) or 0)))
    if not versions:
        return f"{operator}_V1_1"
    major, minor = max(versions)
    return f"{operator}_V{major}_{minor + 1}"


def _board_case_version(next_command: str, *, role: str) -> str:
    if role != "tester":
        return ""
    versions = {
        match.lower()
        for match in re.findall(
            r"\bcase_v\d+\b",
            str(next_command),
            re.IGNORECASE,
        )
    }
    if len(versions) > 1:
        raise ValueError(
            "Tester gate references multiple case versions: "
            + ", ".join(sorted(versions))
        )
    return next(iter(versions), "")


def _pending_repair_version(contracts: list[dict[str, Any]]) -> str:
    for contract in contracts:
        if contract.get("output_kind") == "pending-evidence-repair":
            return str(contract.get("identity", {}).get("pending_version") or "")
    return ""


def _workflow_evidence_version(contracts: list[dict[str, Any]]) -> str:
    for contract in contracts:
        if contract.get("output_kind") not in {
            "solver-blocker",
            "solver-diagnostic-request",
        }:
            continue
        identity = contract.get("identity", {})
        if not isinstance(identity, dict):
            continue
        result_version = str(identity.get("result_version") or "")
        if result_version:
            return result_version
    return ""


def _agent_pool_id(registration: dict[str, Any], *, role: str) -> str:
    definition = registration.get("definition", {})
    routing = definition.get("agent_routing", {}) if isinstance(definition, dict) else {}
    pool_id = str(routing.get(role) or "") if isinstance(routing, dict) else ""
    if not pool_id:
        raise ValueError(
            f"operator {registration.get('display_name', '')} has no {role} Agent pool"
        )
    return pool_id


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
