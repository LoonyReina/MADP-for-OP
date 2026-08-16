from __future__ import annotations

from typing import Any

from ascendop_daemon.control_plane.flow_v3_policy import (
    FailureRecord,
    decide_retry,
)
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.runtime.process_identity import process_identity_matches


class RetryController:
    """Sole production owner of retry and reconciliation decisions."""

    def __init__(
        self,
        database: ControlDatabase,
        *,
        code_generation: str,
        max_transport_retries: int,
        max_agent_preflight_attempts: int,
    ) -> None:
        self.database = database
        self.code_generation = code_generation
        self.max_transport_retries = max(0, int(max_transport_retries))
        self.max_agent_preflight_attempts = max(
            1, int(max_agent_preflight_attempts)
        )

    def run_once(
        self,
        *,
        limit: int = 100,
        current_agent_action_ids: set[str] | None = None,
        current_agent_execution_contract_digests: set[str] | None = None,
        current_agent_runner_generations: set[str] | None = None,
    ) -> dict[str, Any]:
        report: dict[str, Any] = {
            "candidates": 0,
            "decisions": [],
            "workflow_candidates": 0,
            "workflow_decisions": [],
            "errors": [],
            "postprocess_candidates": 0,
            "postprocess_decisions": [],
            "postprocess_reconciliations": [],
            "preparation_candidates": 0,
            "preparation_decisions": [],
            "agent_candidates": 0,
            "agent_decisions": [],
        }
        self._decide_agent_retries(
            report,
            limit=limit,
            current_action_ids=current_agent_action_ids or set(),
            current_execution_contract_digests=(
                current_agent_execution_contract_digests or set()
            ),
            current_runner_generations=current_agent_runner_generations or set(),
        )
        self._reconcile_workflow_actions(report, limit=limit)
        reconcile_postprocess = getattr(
            self.database,
            "reconcile_uncertain_postprocess_recoveries",
            None,
        )
        if callable(reconcile_postprocess):
            try:
                report["postprocess_reconciliations"] = reconcile_postprocess(
                    code_generation=self.code_generation,
                    limit=limit,
                )
            except Exception as exc:
                report["errors"].append(
                    {"postprocess_reconciliation": True, "error": str(exc)}
                )
        self._decide_postprocess_recoveries(report, limit=limit)
        self._decide_preparation_retries(report, limit=limit)
        candidates = self.database.retry_decision_candidates(limit=limit)
        report["candidates"] = len(candidates)
        for candidate in candidates:
            try:
                failure = FailureRecord(**dict(candidate["failure"]))
                decision = decide_retry(
                    failure,
                    execution_attempt=int(candidate["execution_ordinal"]),
                    max_execution_attempts=int(candidate["execution_ordinal"]),
                    transport_retry=int(candidate["transport_retry_count"]),
                    max_transport_retries=self.max_transport_retries,
                    stage_retry=0,
                    max_idempotent_stage_retries=0,
                    stage_idempotent=False,
                )
                applied = self.database.apply_retry_decision(
                    str(candidate["outbox_id"]),
                    failure_event_sequence=int(
                        candidate["failure_event_sequence"]
                    ),
                    decision=decision.to_dict(),
                    failure=failure.to_dict(),
                    code_generation=self.code_generation,
                )
                report["decisions"].append(applied)
            except Exception as exc:
                report["errors"].append(
                    {
                        "outbox_id": str(candidate.get("outbox_id") or ""),
                        "error": str(exc),
                    }
                )
        return report

    def _decide_agent_retries(
        self,
        report: dict[str, Any],
        *,
        limit: int,
        current_action_ids: set[str],
        current_execution_contract_digests: set[str],
        current_runner_generations: set[str],
    ) -> None:
        candidates = self.database.agent_retry_candidates(limit=limit)
        candidates = [
            candidate
            for candidate in candidates
            if str(candidate.get("action_id") or "") in current_action_ids
        ]
        report["agent_candidates"] = len(candidates)
        for candidate in candidates:
            action_id = str(candidate.get("action_id") or "")
            attempt_id = str(candidate.get("current_attempt_id") or "")
            try:
                ordinal = int(candidate.get("ordinal", 0) or 0)
                failure_class = str(
                    dict(candidate.get("failure") or {}).get("failure_class") or ""
                )
                failure = dict(candidate.get("failure") or {})
                base_failure_class = _base_agent_failure_class(failure_class)
                exhausted = failure_class.endswith("-retry-exhausted")
                contract_changed = _agent_execution_contract_changed(
                    failure,
                    current_execution_contract_digests=(
                        current_execution_contract_digests
                    ),
                    current_runner_generations=current_runner_generations,
                )
                if ordinal < self.max_agent_preflight_attempts:
                    decision = "retry"
                    if base_failure_class == "agent-auth":
                        reason = (
                            "agent authentication failed before a source-changing "
                            "turn; quarantine that registration and rebind the same "
                            "immutable action and lease to another healthy compatible "
                            "Agent"
                        )
                    elif base_failure_class == "adapter-execution":
                        reason = (
                            "the exact IDE turn terminated before a durable source or "
                            "workflow output was sealed; retry the same immutable action "
                            "and lease through central bounded arbitration"
                        )
                    elif base_failure_class == "agent-output-validation":
                        reason = (
                            "the exact IDE turn completed but its isolated output failed "
                            "the typed authoring contract; retry the same immutable action, "
                            "candidate version, and lease for a bounded correction turn"
                        )
                    else:
                        reason = (
                            "agent preflight or adapter contract failed without a valid "
                            "source-changing turn; retry the same immutable action and "
                            "lease under central bounded arbitration"
                        )
                elif exhausted and contract_changed:
                    decision = "retry"
                    reason = (
                        "the previous zero-change Agent adapter budget was exhausted "
                        "under an older execution contract; authorize exactly one "
                        "attempt under the current runner/provider contract"
                    )
                else:
                    decision = "exhausted"
                    reason = (
                        f"{failure_class or 'agent pre-turn'} attempt budget exhausted "
                        "before a valid durable outcome completed"
                    )
                applied = self.database.apply_agent_retry_decision(
                    action_id=action_id,
                    attempt_id=attempt_id,
                    decision=decision,
                    reason=reason,
                    code_generation=self.code_generation,
                )
                report["agent_decisions"].append(applied)
            except Exception as exc:
                report["errors"].append(
                    {"agent_action_id": action_id, "error": str(exc)}
                )
    def _decide_preparation_retries(
        self,
        report: dict[str, Any],
        *,
        limit: int,
    ) -> None:
        candidates = self.database.preparation_retry_candidates(
            code_generation=self.code_generation,
            limit=limit,
        )
        report["preparation_candidates"] = len(candidates)
        for candidate in candidates:
            preparation_id = str(candidate.get("preparation_id") or "")
            try:
                applied = self.database.apply_preparation_retry_decision(
                    preparation_id,
                    failure_event_sequence=int(
                        candidate["failure_event_sequence"]
                    ),
                    code_generation=self.code_generation,
                    reason=(
                        "immutable request package build failed under an older "
                        "daemon generation; rebuild before publication without "
                        "consuming an execution attempt"
                    ),
                )
                report["preparation_decisions"].append(applied)
            except Exception as exc:
                report["errors"].append(
                    {
                        "preparation_id": preparation_id,
                        "error": str(exc),
                    }
                )

    def _decide_postprocess_recoveries(
        self,
        report: dict[str, Any],
        *,
        limit: int,
    ) -> None:
        candidates = self.database.postprocess_recovery_candidates(limit=limit)
        report["postprocess_candidates"] = len(candidates)
        for candidate in candidates:
            recovery_id = str(candidate.get("recovery_id") or "")
            try:
                projection = candidate.get("projection", {})
                projection = projection if isinstance(projection, dict) else {}
                evidence = projection.get("stage_evidence", {})
                evidence = evidence if isinstance(evidence, dict) else {}
                result = candidate.get("result", {})
                result = result if isinstance(result, dict) else {}
                stages = candidate.get("stages", [])
                accepted_stage_attempts = candidate.get(
                    "accepted_stage_attempts", {}
                )
                accepted_stage_attempts = (
                    accepted_stage_attempts
                    if isinstance(accepted_stage_attempts, dict)
                    else {}
                )
                exhausted_stages = sorted(
                    str(stage)
                    for stage in stages
                    if accepted_stage_attempts.get(str(stage))
                ) if isinstance(stages, list) else []
                facts_complete = (
                    evidence.get("correctness", {}).get("status") == "passed"
                    and evidence.get("performance_capture", {}).get("status")
                    == "passed"
                    and evidence.get("device_reexecution_allowed") is False
                )
                recovery_safe = (
                    facts_complete
                    and result.get("failure_domain") == "export"
                    and bool(result.get("retryable"))
                    and isinstance(stages, list)
                    and 0 < len(stages) <= 8
                    and int(candidate.get("dispatch_attempts", 0) or 0) == 0
                    and not exhausted_stages
                )
                if recovery_safe:
                    action = "retry-idempotent-stage"
                    reason = (
                        "correctness and profiler capture are durable; recover only "
                        "the immutable idempotent host/export stage closure"
                    )
                elif exhausted_stages:
                    action = "unrecoverable"
                    reason = (
                        "postprocess recovery stage-attempt budget is exhausted: "
                        + ",".join(exhausted_stages)
                    )
                else:
                    action = "unrecoverable"
                    reason = (
                        "postprocess recovery contract is incomplete or does not "
                        "prove durable device facts and retryable export failure"
                    )
                decision = self.database.apply_postprocess_recovery_decision(
                    recovery_id,
                    action=action,
                    reason=reason,
                    code_generation=self.code_generation,
                )
                report["postprocess_decisions"].append(decision)
            except Exception as exc:
                report["errors"].append(
                    {"postprocess_recovery_id": recovery_id, "error": str(exc)}
                )

    def _reconcile_workflow_actions(
        self,
        report: dict[str, Any],
        *,
        limit: int,
    ) -> None:
        candidates = self.database.workflow_action_recovery_candidates(limit=limit)
        report["workflow_candidates"] = len(candidates)
        for candidate in candidates:
            action = dict(candidate["action"])
            try:
                if candidate["attempt_state"] == "claimed":
                    decision = "retry-prepublish"
                    reason = "claim expired before the action worker started"
                elif self._recorded_process_is_alive(candidate):
                    decision = "wait"
                    reason = "recorded workflow worker or child process is still alive"
                elif (
                    action.get("resource_class") == "local"
                    and action.get("retry_policy_ref")
                    == "workflow-v4-central-retry"
                ):
                    decision = "retry-idempotent"
                    reason = (
                        "local idempotent action lost its process identity before receipt"
                    )
                else:
                    decision = "reconcile-required"
                    reason = (
                        "device or transport action lost process identity with uncertain "
                        "side effects"
                    )
                applied = self.database.apply_workflow_action_recovery_decision(
                    str(action["action_id"]),
                    str(candidate["claim_token"]),
                    decision=decision,
                    reason=reason,
                    lease_seconds=30,
                )
                report["workflow_decisions"].append(applied)
            except Exception as exc:
                report["errors"].append(
                    {
                        "workflow_action_id": str(action.get("action_id") or ""),
                        "error": str(exc),
                    }
                )

    @staticmethod
    def _recorded_process_is_alive(candidate: dict[str, Any]) -> bool:
        return any(
            process_identity_matches(
                int(candidate.get(pid_field, 0) or 0),
                str(candidate.get(token_field, "") or ""),
            )
            for pid_field, token_field in (
                ("worker_pid", "worker_start_token"),
                ("child_pid", "child_start_token"),
            )
        )


def _base_agent_failure_class(failure_class: str) -> str:
    suffix = "-retry-exhausted"
    return (
        failure_class[: -len(suffix)]
        if failure_class.endswith(suffix)
        else failure_class
    )


def _agent_execution_contract_changed(
    failure: dict[str, Any],
    *,
    current_execution_contract_digests: set[str],
    current_runner_generations: set[str],
) -> bool:
    previous_contract = str(
        failure.get("agent_execution_contract_digest") or ""
    ).strip()
    if previous_contract:
        return bool(
            current_execution_contract_digests
            and previous_contract not in current_execution_contract_digests
        )
    previous_runner = str(failure.get("runner_generation") or "").strip()
    return bool(
        current_execution_contract_digests
        and current_runner_generations
        and previous_runner
        and previous_runner not in current_runner_generations
    )
