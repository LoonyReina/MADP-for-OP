from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol

from ascendop_protocol.actor import AGENT_ACTION_OUTCOME_SCHEMA
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
from ascendop_daemon.automation.evidence_operations import (
    EvidenceOperationCoordinator,
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
from ascendop_daemon.workflow.engine_candidates import extract_submit_command
from ascendop_daemon.workflow.operator_job_builder import parse_submit_command
from ascendop_daemon.workflow.policy_pipeline import WorkflowPolicyPipeline


class SnapshotReader(Protocol):
    def read(self) -> BoardSnapshot: ...


class EvidenceOperationPending(RuntimeError):
    def __init__(self, operation: Mapping[str, Any]) -> None:
        super().__init__("registered evidence operation is pending")
        self.operation = dict(operation)


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
        evidence_pending: list[dict[str, Any]] = []
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
            except EvidenceOperationPending as exc:
                evidence_pending.append(exc.operation)
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
            "evidence_operations_pending": evidence_pending,
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
        runbook_digest = hashlib.sha256((self.root / runbook).read_bytes()).hexdigest()
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
            runbook_digest=runbook_digest,
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
        context_completeness = self._ensure_context_completeness(
            role=role,
            operator_id=operator_id,
            operator=row.op,
            evidence=evidence,
        )
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
            "causation": _next_action_causation(
                context_completeness["evidence_operations"]
            ),
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
            "active_case_version": context_completeness["active_case_version"],
            "evidence_operations": context_completeness["evidence_operations"],
            "context_completeness": context_completeness,
            "causation": action["causation"],
            "permitted_operations": role_contract["permitted_operations"],
            "created_at": created_at,
        }
        self.database.reconcile_workflow_agent_candidate(
            operator_id=operator_id,
            role=role,
            candidate_version=candidate_version,
            keep_action_id=action_id,
        )
        return self.database.create_agent_action(action, context)

    def _ensure_context_completeness(
        self,
        *,
        role: str,
        operator_id: str,
        operator: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        recent_operations = self.database.recent_evidence_operations(
            operator_id=operator_id,
            expected_consumer=role,
        )
        base = {
            "schema": "ascendop.agent-context-completeness.v1",
            "state": "complete",
            "required_fields": [
                "candidate",
                "active_case_version",
                "latest_comparable_result",
                "baseline",
                "evidence_index",
                "environment",
                "budget",
            ],
            "missing_fields": [],
            "active_case_version": "",
            "evidence_operations": recent_operations,
        }
        if role != "solver" or list(evidence.get("recent_results") or []):
            return base
        previous = next(
            (
                item
                for item in reversed(self.database.agent_actions_v4())
                if item.get("operator_id") == operator_id
                and item.get("role") == "solver"
                and item.get("state") == "completed"
            ),
            None,
        )
        if previous is None:
            return base
        previous_action = dict(previous.get("action") or {})
        test_version = str(previous_action.get("candidate_version") or "")
        submit_root = _candidate_submit_root(self.root, operator, test_version)
        if submit_root is None:
            return base
        parsed = parse_submit_command(
            extract_submit_command(submit_root / "SUBMIT.md")
        )
        case_version = str(parsed["case_version"])
        base["active_case_version"] = case_version
        existing = [
            item
            for item in self.database.evidence_operations_for_origin(
                action_id=str(previous_action["action_id"])
            )
            if item["operation_code"] == "test.correctness"
            and item["request"]["parameters"]["test_version"] == test_version
            and item["request"]["parameters"]["case_version"] == case_version
        ]
        if existing:
            operation = existing[-1]
            if operation["state"] in {"completed", "failed", "cancelled"}:
                base["evidence_operations"] = self.database.recent_evidence_operations(
                    operator_id=operator_id,
                    expected_consumer=role,
                )
                return base
            raise EvidenceOperationPending(operation)
        outcome = {
            "schema": AGENT_ACTION_OUTCOME_SCHEMA,
            "action_id": str(previous_action["action_id"]),
            "execution_status": "completed",
            "disposition": "request_evidence",
            "failure_class": None,
            "summary": (
                "daemon context completeness scheduled the missing comparable result"
            ),
            "outputs": [],
            "evidence_refs": [],
            "requested_operation": {
                "operation_code": "test.correctness",
                "parameters": {
                    "candidate_id": test_version,
                    "test_version": test_version,
                    "case_version": case_version,
                },
                "expected_consumer": "solver",
                "resume_condition": (
                    "a comparable correctness result is indexed for the exact "
                    "candidate and case version"
                ),
            },
            "blocker": None,
            "completed_at": _utc_now(),
        }
        operation = EvidenceOperationCoordinator(
            self.root,
            self.database,
        ).register(previous_action, outcome)
        if operation is None:
            raise ValueError("context completeness did not create evidence operation")
        raise EvidenceOperationPending(operation)


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
    candidate_roots = (
        root / "operators_workspace" / operator,
        root / "TestUtils" / "pending" / operator,
        root / "operators_testresult" / operator,
        root / "operators_finish" / operator,
    )
    candidates = [
        path.name
        for candidate_root in candidate_roots
        for path in candidate_root.glob(f"{operator}_V*")
        if path.is_dir()
    ]
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


def _next_action_causation(
    evidence_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    completed = [
        item
        for item in evidence_operations
        if str(item.get("state") or "") in {"completed", "failed", "cancelled"}
        and isinstance(item.get("result"), dict)
    ]
    if not completed:
        return {}
    latest = completed[0]
    request = dict(latest.get("request") or {})
    origin = dict(request.get("origin") or {})
    route = dict(latest.get("route") or {})
    result = dict(latest.get("result") or {})
    origin_action_id = str(origin.get("action_id") or "")
    return {
        "trace_id": origin_action_id
        or str(latest.get("operation_request_id") or ""),
        "parent_action_id": origin_action_id,
        "operation_request_id": str(
            latest.get("operation_request_id") or ""
        ),
        "operation_result_id": str(
            result.get("operation_result_id") or ""
        ),
        "request_id": str(route.get("test_request_id") or ""),
        "attempt_id": str(route.get("wire_attempt_id") or ""),
    }


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
    if not contracts or any(not bool(contract.get("required")) for contract in contracts):
        return ""
    if any(
        contract.get("output_kind")
        not in {"solver-blocker", "solver-diagnostic-request"}
        for contract in contracts
    ):
        return ""
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


def _candidate_submit_root(
    root: Path,
    operator: str,
    test_version: str,
) -> Path | None:
    if not test_version:
        return None
    candidates = (
        root
        / "operators_testresult"
        / operator
        / test_version
        / "submit_snapshot",
        root / "TestUtils" / "submit" / operator / test_version,
    )
    return next(
        (path.resolve() for path in candidates if (path / "SUBMIT.md").is_file()),
        None,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
