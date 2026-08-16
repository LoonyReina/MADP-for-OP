from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.agent import (
    SOLVER_CANDIDATE_PROMOTION_RECEIPT_SCHEMA,
    validate_solver_candidate_proposal,
)

from ascendop_daemon.automation.agent_workspace import AgentWorkspace
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.runtime.workflow_adapter import load_workflow_adapter_module


class CandidateProposalError(RuntimeError):
    pass


class CandidateProposalPublisher:
    """Turn one sealed Solver proposal into one daemon-owned pending package."""

    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        workspace: AgentWorkspace,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.workspace = workspace

    def publish_from_output_receipt(
        self,
        output_receipt: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        candidates = [
            dict(item)
            for item in output_receipt.get("outputs", [])
            if str(item.get("output_kind") or "") == "solver-candidate-proposal"
        ]
        if not candidates:
            return None
        if len(candidates) != 1:
            raise CandidateProposalError("one Agent action may publish one candidate proposal")
        action_id = str(output_receipt.get("action_id") or "")
        record = self.database.agent_action(action_id)
        if record is None:
            raise CandidateProposalError(f"candidate Agent action is unavailable: {action_id}")
        action = dict(record["action"])
        if str(action.get("role") or "") != "solver":
            raise CandidateProposalError("only Solver actions may publish candidates")

        item = candidates[0]
        proposal_path = self._bounded_file(str(item.get("canonical_path") or ""))
        proposal = self._read_object(proposal_path)
        try:
            proposal = validate_solver_candidate_proposal(proposal)
        except ValueError as exc:
            raise CandidateProposalError(f"invalid Solver candidate proposal: {exc}") from exc
        display_name = str(
            action.get("candidate_identity", {}).get("display_name")
            or action.get("operator_id")
            or ""
        )
        expected = {
            "campaign": str(action.get("campaign") or ""),
            "operator": display_name,
            "candidate_version": str(action.get("candidate_version") or ""),
            "source_before_digest": str(
                action.get("candidate_identity", {}).get("execution_source_digest") or ""
            ),
        }
        for field, value in expected.items():
            if str(proposal.get(field) or "") != value:
                raise CandidateProposalError(
                    f"Solver candidate proposal conflicts with Agent action: {field}"
                )

        run_root = self.workspace.run_root(action_id)
        source_seal = self._read_object(run_root / "source-seal.json")
        if (
            str(source_seal.get("action_id") or "") != action_id
            or str(source_seal.get("candidate_version") or "")
            != expected["candidate_version"]
            or str(source_seal.get("source_before_digest") or "")
            != expected["source_before_digest"]
        ):
            raise CandidateProposalError("candidate source seal conflicts with Agent action")

        # Promotion is idempotent. Doing it here gives candidate publication a
        # hard dependency on canonical source without relying on scheduler order.
        source_receipt = self.workspace.promote(run_root / "source-seal.json")
        source_after = str(source_receipt.get("source_after_digest") or "")
        origin = self._bounded_directory(str(action.get("origin_workspace") or ""))
        if self.workspace.digest(origin) != source_after:
            raise CandidateProposalError("canonical candidate source digest is not sealed")
        code_changes = {
            str(path)
            for path in source_seal.get("changed_paths", [])
            if str(path).startswith(("op_host/", "op_kernel/"))
        }
        if code_changes and not code_changes.issubset(
            {str(path) for path in proposal["changed_source"]}
        ):
            raise CandidateProposalError(
                "candidate proposal does not enumerate every changed kernel/host source"
            )

        receipt_path = run_root / "candidate-promotion-receipt.json"
        pending = (
            self.root
            / "TestUtils"
            / "pending"
            / display_name
            / expected["candidate_version"]
        )
        if receipt_path.is_file():
            receipt = self._read_object(receipt_path)
            self._validate_receipt(receipt, action_id, pending, source_after)
            return receipt
        if pending.exists():
            self._validate_pending(pending, display_name, expected["candidate_version"], source_after)
            command_output = "reconciled already-published pending package"
        else:
            command_output = self._create_pending(action, proposal, origin)
            self._validate_pending(pending, display_name, expected["candidate_version"], source_after)

        receipt = {
            "schema": SOLVER_CANDIDATE_PROMOTION_RECEIPT_SCHEMA,
            "action_id": action_id,
            "iteration_id": str(action.get("iteration_id") or ""),
            "operator": display_name,
            "candidate_version": expected["candidate_version"],
            "case_version": str(proposal["case_version"]),
            "source_after_digest": source_after,
            "pending_path": pending.relative_to(self.root).as_posix(),
            "workflow_output": command_output.strip(),
            "promoted_at": _utc_now(),
        }
        self._write_json(receipt_path, receipt)
        return receipt

    def _create_pending(
        self,
        action: Mapping[str, Any],
        proposal: Mapping[str, Any],
        origin: Path,
    ) -> str:
        adapter = load_workflow_adapter_module(self.root)
        if Path(str(adapter.ROOT)).resolve() != self.root:
            raise CandidateProposalError("workflow adapter is bound to another workspace")
        args = argparse.Namespace(
            op=str(proposal["operator"]),
            test_version=str(proposal["candidate_version"]),
            season=str(proposal["campaign"]),
            source=origin,
            base_version=str(proposal["base_version"]),
            scaffold_source="Flow V4 isolated Agent workspace",
            case_version=str(proposal["case_version"]),
            regression_sentinel_for=None,
            intent=str(proposal["intent"]),
            observed_signal=str(proposal["observed_signal"]),
            primary_hypothesis=str(proposal["primary_hypothesis"]),
            counter_hypothesis=str(proposal["counter_hypothesis"]),
            router_gap=str(proposal["router_gap"]),
            consulted_evidence=[str(item) for item in proposal["consulted_evidence"]],
            optimization_method_decision=str(
                proposal["optimization_method_decision"]
            ),
            skill_feedback=str(proposal["skill_feedback"]),
            shared_knowledge_decision=str(proposal["shared_knowledge_decision"]),
            changed_source=[str(item) for item in proposal["changed_source"]],
            correctness_risk=str(proposal["risks"]["correctness"]),
            perf_risk=str(proposal["risks"]["performance"]),
            infra_risk=str(proposal["risks"]["infrastructure"]),
            correctness_range="official + active attack cases",
            perf_cases="all registered performance cases",
            hardware=str(proposal["hardware"]),
            allow_legacy_version_name=False,
            # The DB-backed Agent action is the reservation authority. Gaps are
            # legal when an earlier reserved Agent turn produced no candidate.
            allow_out_of_order_test_version=True,
            allow_placeholder_evidence=False,
            dry_run=False,
        )
        output = io.StringIO()
        try:
            with contextlib.redirect_stdout(output):
                result = adapter.create_pending(args)
        except SystemExit as exc:
            raise CandidateProposalError(f"daemon create-pending rejected proposal: {exc}") from exc
        if result != 0:
            raise CandidateProposalError(f"daemon create-pending returned {result}")
        return output.getvalue()

    def _validate_pending(
        self,
        pending: Path,
        operator: str,
        candidate_version: str,
        source_after: str,
    ) -> None:
        if not pending.is_dir() or pending.is_symlink():
            raise CandidateProposalError("candidate pending package is unavailable")
        snapshot = pending / "source_snapshot"
        if not snapshot.is_dir() or snapshot.is_symlink():
            raise CandidateProposalError("candidate pending source snapshot is unavailable")
        if self.workspace.digest(snapshot) != source_after:
            raise CandidateProposalError(
                "candidate pending source snapshot conflicts with sealed source"
            )
        adapter = load_workflow_adapter_module(self.root)
        if Path(str(adapter.ROOT)).resolve() != self.root:
            raise CandidateProposalError("workflow adapter is bound to another workspace")
        lineage_digest = str(adapter.source_tree_digest(snapshot))
        lineage = self._read_object(pending / "SOURCE_LINEAGE.json")
        if (
            str(lineage.get("op") or "") != operator
            or str(lineage.get("test_version") or "") != candidate_version
            or str(lineage.get("candidate", {}).get("sha256") or "")
            != lineage_digest
        ):
            raise CandidateProposalError("candidate pending lineage is inconsistent")
        if not (pending / "VERSION.md").is_file():
            raise CandidateProposalError("candidate pending VERSION.md is missing")

    def _validate_receipt(
        self,
        receipt: Mapping[str, Any],
        action_id: str,
        pending: Path,
        source_after: str,
    ) -> None:
        if (
            receipt.get("schema") != SOLVER_CANDIDATE_PROMOTION_RECEIPT_SCHEMA
            or str(receipt.get("action_id") or "") != action_id
            or str(receipt.get("source_after_digest") or "") != source_after
            or str(receipt.get("pending_path") or "")
            != pending.relative_to(self.root).as_posix()
        ):
            raise CandidateProposalError("candidate promotion receipt conflicts")
        self._validate_pending(
            pending,
            str(receipt.get("operator") or ""),
            str(receipt.get("candidate_version") or ""),
            source_after,
        )

    def _bounded_file(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if self.root not in path.parents or not path.is_file() or path.is_symlink():
            raise CandidateProposalError("candidate proposal path is unavailable or unbounded")
        return path

    def _bounded_directory(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if self.root not in path.parents or not path.is_dir() or path.is_symlink():
            raise CandidateProposalError("candidate source directory is unavailable or unbounded")
        return path

    @staticmethod
    def _read_object(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CandidateProposalError(f"invalid candidate artifact: {path}") from exc
        if not isinstance(value, dict):
            raise CandidateProposalError("candidate artifact must be an object")
        return value

    @staticmethod
    def _write_json(path: Path, value: Mapping[str, Any]) -> None:
        payload = (
            json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            handle.write(payload)
            temporary = Path(handle.name)
        try:
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
