from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.actor import (
    AGENT_ACTION_OUTCOME_SCHEMA,
    validate_agent_action_outcome,
    validate_native_turn_outcome,
)
from ascendop_protocol.agent import AGENT_ACTION_RECEIPT_SCHEMA

from ascendop_daemon.automation.agent_outputs import (
    AgentOutputBroker,
    AgentOutputError,
)
from ascendop_daemon.automation.agent_promotions import AgentPromotionQueue
from ascendop_daemon.automation.evidence_operations import (
    EvidenceOperationCoordinator,
    EvidenceOperationError,
)
from ascendop_daemon.automation.agent_workspace import (
    AgentWorkspace,
    AgentWorkspaceError,
    AgentWriteScopeViolation,
)


class AgentCompletionError(RuntimeError):
    pass


class OutcomeNormalizer:
    """Validate native terminal data and remove runtime-specific status drift."""

    def normalize(
        self,
        native_outcome: Mapping[str, Any],
        *,
        completion_metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        native = validate_native_turn_outcome(native_outcome)
        reported_status = str(native["terminal_status"])
        status = "failed" if reported_status == "interrupted" else reported_status
        structured = dict(native["structured_result"])
        completion = {
            **dict(completion_metadata or {}),
            "summary": str(structured.get("summary") or "Agent turn completed"),
            "session_id": str(native["native_session_id"]),
            "native_turn_id": str(native["native_turn_id"]),
            "native_observed_at": str(native["observed_at"]),
            "telemetry": dict(native["telemetry"]),
        }
        if reported_status == "interrupted":
            completion["reported_status"] = reported_status
            completion.setdefault("failure_class", "adapter-execution")
        raw_failure = str(
            structured.get("failure_class")
            or completion.get("failure_class")
            or ""
        )
        if raw_failure:
            completion["failure_class"] = raw_failure
        return {
            "native": native,
            "status": status,
            "reported_status": reported_status,
            "structured_result": structured,
            "artifact_refs": [str(item) for item in native["artifact_refs"]],
            "completion": completion,
        }


class AgentCompletionService:
    """Daemon-owned terminal normalization, sealing, and promotion scheduling."""

    def __init__(
        self,
        root: Path,
        database: Any,
        *,
        code_generation: str,
        workspace: AgentWorkspace | None = None,
        outputs: AgentOutputBroker | None = None,
        promotions: AgentPromotionQueue | None = None,
        normalizer: OutcomeNormalizer | None = None,
        evidence_operations: EvidenceOperationCoordinator | None = None,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.workspace = workspace or AgentWorkspace(self.root)
        self.outputs = outputs or AgentOutputBroker(
            self.root, self.workspace.runs_root
        )
        self.promotions = promotions or AgentPromotionQueue(
            self.root,
            database,
            self.workspace,
            code_generation=code_generation,
        )
        self.normalizer = normalizer or OutcomeNormalizer()
        self.evidence_operations = evidence_operations or EvidenceOperationCoordinator(
            self.root,
            database,
        )

    def complete(
        self,
        *,
        action_id: str,
        lease_token: str,
        lease_id: str,
        agent_id: str,
        started_at: str,
        native_outcome: Mapping[str, Any],
        completion_metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized = self.normalizer.normalize(
            native_outcome,
            completion_metadata=completion_metadata,
        )
        native = normalized["native"]
        if str(native["action_id"]) != action_id:
            raise AgentCompletionError("native outcome action identity changed")
        action_record = self.database.agent_action(action_id)
        if action_record is None:
            raise AgentCompletionError(f"Agent action does not exist: {action_id}")
        action = dict(action_record["action"])
        status = str(normalized["status"])
        structured = dict(normalized["structured_result"])
        artifacts = list(normalized["artifact_refs"])
        completion = dict(normalized["completion"])

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

        source_seal: dict[str, Any] | None = None
        output_seal: dict[str, Any] | None = None
        agent_outcome: dict[str, Any]
        if status == "completed":
            try:
                source_seal = self.workspace.seal(action)
                source_path = self.workspace.run_root(action_id) / "source-seal.json"
                artifacts.append(source_path.relative_to(self.root).as_posix())
                workspace = self.workspace.run_root(action_id) / "workspace"
                output_seal = self.outputs.seal(action, workspace)
                output_path = self.workspace.run_root(action_id) / "output-seal.json"
                artifacts.append(output_path.relative_to(self.root).as_posix())
                self._validate_candidate_contract(action, source_seal, output_seal)
                durable_outputs = self._outcome_outputs(
                    source_path=source_path,
                    source_seal=source_seal,
                    output_seal=output_seal,
                )
                legacy_outcome = self.evidence_operations.legacy_diagnostic_outcome(
                    action,
                    output_seal,
                    workspace=workspace,
                    durable_outputs=durable_outputs,
                    summary=completion["summary"],
                    completed_at=_utc_now(),
                )
                if legacy_outcome is not None:
                    agent_outcome = legacy_outcome
                elif durable_outputs:
                    agent_outcome = validate_agent_action_outcome(
                        {
                            "schema": AGENT_ACTION_OUTCOME_SCHEMA,
                            "action_id": action_id,
                            "execution_status": "completed",
                            "disposition": "proposed_change",
                            "failure_class": None,
                            "summary": completion["summary"],
                            "outputs": durable_outputs,
                            "evidence_refs": artifacts,
                            "requested_operation": None,
                            "blocker": None,
                            "completed_at": _utc_now(),
                        }
                    )
                else:
                    agent_outcome = self._explicit_no_change_outcome(
                        action_id, structured
                    )
                completion.update(
                    {
                        "source_before_digest": source_seal["source_before_digest"],
                        "source_after_digest": source_seal["source_after_digest"],
                        "changed_paths": source_seal["changed_paths"],
                        "workflow_outputs": output_seal["outputs"],
                        "out_of_scope_paths": [],
                    }
                )
                self.evidence_operations.build_request(action, agent_outcome)
            except AgentWriteScopeViolation as exc:
                status = "failed"
                completion.update(
                    {
                        "failure_class": "write-scope-violation",
                        "validation_error": str(exc),
                        "out_of_scope_paths": exc.paths,
                    }
                )
                source_seal = None
                output_seal = None
                agent_outcome = self._failed_outcome(
                    action_id,
                    completion["summary"],
                    completion["failure_class"],
                )
            except AgentWorkspaceError as exc:
                status = "failed"
                completion.update(
                    {
                        "failure_class": "protocol",
                        "validation_error": str(exc),
                    }
                )
                source_seal = None
                output_seal = None
                agent_outcome = self._failed_outcome(
                    action_id,
                    completion["summary"],
                    completion["failure_class"],
                )
            except (
                AgentOutputError,
                AgentCompletionError,
                EvidenceOperationError,
            ) as exc:
                status = "failed"
                completion.update(
                    {
                        "failure_class": "agent-output-validation",
                        "validation_error": str(exc),
                    }
                )
                source_seal = None
                output_seal = None
                agent_outcome = self._failed_outcome(
                    action_id,
                    completion["summary"],
                    completion["failure_class"],
                )
        else:
            agent_outcome = self._failed_outcome(
                action_id,
                completion["summary"],
                completion.get("failure_class", ""),
                status=status,
            )

        completion["agent_action_outcome"] = agent_outcome
        receipt = {
            "schema": AGENT_ACTION_RECEIPT_SCHEMA,
            "action_id": action_id,
            "iteration_id": str(action["iteration_id"]),
            "agent_id": agent_id,
            "lease_id": lease_id,
            "status": status,
            "started_at": started_at,
            "completed_at": _utc_now(),
            "completion": completion,
            "artifacts": sorted(set(artifacts)),
        }
        terminal = self.database.complete_agent_action(
            receipt,
            lease_token=lease_token,
        )
        terminal_state = str(terminal["state"])
        committed = self.database.agent_action_receipt(action_id)
        if committed is not None:
            receipt = committed
            committed_outcome = receipt.get("completion", {}).get(
                "agent_action_outcome"
            )
            if isinstance(committed_outcome, Mapping):
                agent_outcome = validate_agent_action_outcome(committed_outcome)
        elif terminal_state != str(receipt["status"]):
            if committed is None:
                raise AgentCompletionError(
                    "terminal Agent action has no committed receipt"
                )

        promotable = (
            status == "completed"
            and terminal_state == "completed"
            and self.database.workflow_agent_action_is_current(action_id)
        )
        evidence_operation = (
            self.evidence_operations.register(action, agent_outcome)
            if status == "completed"
            and terminal_state == "completed"
            and agent_outcome.get("disposition") == "request_evidence"
            else None
        )
        output_promotion = (
            self.promotions.enqueue_output(
                action,
                output_seal,
                source_seal=(
                    source_seal
                    if source_seal and source_seal.get("changed_paths")
                    else None
                ),
            )
            if promotable and output_seal and output_seal["outputs"]
            else None
        )
        promotion = (
            output_promotion
            if output_promotion is not None
            and source_seal
            and source_seal.get("changed_paths")
            else (
                (
                    self.promotions.enqueue_case(action, source_seal)
                    if str(action.get("role") or "") == "tester"
                    else self.promotions.enqueue_source(action, source_seal)
                )
                if promotable and source_seal and source_seal["changed_paths"]
                else None
            )
        )
        return {
            "terminal": terminal,
            "receipt": receipt,
            "outcome": agent_outcome,
            "promotion": promotion,
            "output_promotion": output_promotion,
            "evidence_operation": evidence_operation,
        }

    @staticmethod
    def _validate_candidate_contract(
        action: Mapping[str, Any],
        source_seal: Mapping[str, Any],
        output_seal: Mapping[str, Any],
    ) -> None:
        changed_code = any(
            str(path).startswith(("op_host/", "op_kernel/"))
            for path in source_seal["changed_paths"]
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
            raise AgentCompletionError(
                "Solver changed op_host/op_kernel without a candidate proposal"
            )

    def _outcome_outputs(
        self,
        *,
        source_path: Path,
        source_seal: Mapping[str, Any],
        output_seal: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []
        if source_seal["changed_paths"]:
            outputs.append(
                {
                    "output_id": "source-change",
                    "output_kind": "source-change",
                    "artifact_ref": source_path.relative_to(self.root).as_posix(),
                    "sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
                }
            )
        workspace = self.workspace.run_root(str(source_seal["action_id"])) / "workspace"
        for item in output_seal["outputs"]:
            artifact = workspace / str(item["isolated_path"])
            outputs.append(
                {
                    "output_id": str(item["output_id"]),
                    "output_kind": str(item["output_kind"]),
                    "artifact_ref": artifact.relative_to(self.root).as_posix(),
                    "sha256": str(item["sha256"]),
                }
            )
        return outputs

    @staticmethod
    def _explicit_no_change_outcome(
        action_id: str,
        structured: Mapping[str, Any],
    ) -> dict[str, Any]:
        native_error = str(structured.get("native_output_validation_error") or "")
        if native_error:
            raise AgentCompletionError(native_error)
        if structured.get("schema") != AGENT_ACTION_OUTCOME_SCHEMA:
            raise AgentCompletionError(
                "completed Agent action produced no durable output or typed outcome"
            )
        outcome = validate_agent_action_outcome(structured)
        if str(outcome["action_id"]) != action_id:
            raise AgentCompletionError("typed Agent outcome action identity changed")
        if outcome["disposition"] == "proposed_change":
            raise AgentCompletionError("proposed_change has no daemon-sealed output")
        return outcome

    @staticmethod
    def _failed_outcome(
        action_id: str,
        summary: str,
        failure_class: str,
        *,
        status: str = "failed",
    ) -> dict[str, Any]:
        normalized_status = status if status in {"failed", "cancelled", "uncertain"} else "failed"
        if normalized_status == "cancelled":
            normalized_failure = "cancelled"
        elif normalized_status == "uncertain":
            normalized_failure = "uncertain_delivery"
        elif "auth" in str(failure_class).lower():
            normalized_failure = "authentication"
        elif any(
            token in str(failure_class).lower()
            for token in ("adapter", "execution", "interrupt")
        ):
            normalized_failure = "adapter_execution"
        else:
            normalized_failure = "protocol_violation"
        return validate_agent_action_outcome(
            {
                "schema": AGENT_ACTION_OUTCOME_SCHEMA,
                "action_id": action_id,
                "execution_status": normalized_status,
                "disposition": None,
                "failure_class": normalized_failure,
                "summary": summary or "Agent turn did not complete",
                "outputs": [],
                "evidence_refs": [],
                "requested_operation": None,
                "blocker": None,
                "completed_at": _utc_now(),
            }
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "AgentCompletionError",
    "AgentCompletionService",
    "OutcomeNormalizer",
]
