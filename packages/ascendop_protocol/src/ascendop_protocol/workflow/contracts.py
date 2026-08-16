from __future__ import annotations

import re
from typing import Any, Mapping


WORKFLOW_ACTION_SCHEMA = "ascendop.workflow-action.v1"
WORKFLOW_ACTION_RECEIPT_SCHEMA = "ascendop.workflow-action-receipt.v1"
BOARD_ACTION_SCHEMA = "ascendop.board-action.v1"
SOLVER_DIAGNOSTIC_REQUEST_SCHEMA = "ascendop.solver-diagnostic-request.v1"
SOLVER_BLOCKER_CONTRACT_REVISION = "solver-diagnostic-v3"
SOLVER_DIAGNOSTIC_CAPABILITY_GENERATION = "solver-diagnostic-capabilities-v9"
SOLVER_DIAGNOSTIC_CAPABILITIES = (
    "all-case-correctness-replay",
    "staged-diagnostic-evidence",
    "consumed-version-runtime-boundary-replay",
    "runtime-boundary-trace",
    "native-workspace-query-attribution",
    "kernel-fault-attribution",
    "same-endpoint-profile-comparison",
    "case-lifetime-rollover",
    "pending-reservation-regeneration",
    "pending-reservation-regeneration-execution",
)
SOLVER_DIAGNOSTIC_HOLD_OWNERS = {
    "daemon-harness",
    "external",
}
SOLVER_DIAGNOSTIC_HOLD_RESUME_TRIGGERS = {
    "daemon-evidence-transition",
    "external-evidence-transition",
}
SOLVER_DIAGNOSTIC_DISPOSITIONS = {
    "durable-hold",
    "evidence-exhausted",
}
SOLVER_DIAGNOSTIC_EXHAUSTION_SCOPE = (
    "current-result-and-registered-diagnostics"
)
SOLVER_STEWARD_ESCALATION_SCHEMA = "ascendop.board-steward-escalation.v1"
SOLVER_STEWARD_ESCALATION_OPERATION = "resolve-workflow-capability-gap"
SOLVER_STEWARD_ESCALATION_STATE = "needs-steward-escalation"

SOLVER_DIAGNOSTIC_OPERATIONS = {
    "diagnostic-profile",
    "diagnostic-correctness-replay",
}
SOLVER_DIAGNOSTIC_PROFILER_MODES = {
    "primary-all-cases",
    "primary-roofline-all-cases",
}
SOLVER_DIAGNOSTIC_CORRECTNESS_ARTIFACTS = (
    "engine-identity",
    "runtime-readiness",
    "runtime-compatibility",
    "operator-install-precheck",
    "phase-timeline",
    "correctness-batch",
    "first-failure-traceback",
    "case-logs",
    "runtime-boundary-trace",
    "native-workspace-query-attribution",
    "kernel-fault-attribution",
)


class WorkflowContractError(ValueError):
    pass


def validate_board_action(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or raw.get("schema") != BOARD_ACTION_SCHEMA:
        raise WorkflowContractError("unsupported board action descriptor")
    _text(raw.get("operation"), "operation")
    positional = raw.get("positional", [])
    options = raw.get("options", {})
    if not isinstance(positional, list) or not all(
        isinstance(value, str) for value in positional
    ):
        raise WorkflowContractError("positional must be a text list")
    if not isinstance(options, Mapping) or not all(
        isinstance(key, str) and isinstance(value, (str, int, float, bool, type(None)))
        for key, value in options.items()
    ):
        raise WorkflowContractError("options must contain scalar values")
    return dict(raw)


def validate_solver_steward_escalation(raw: Mapping[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(raw, Mapping)
        or raw.get("schema") != SOLVER_STEWARD_ESCALATION_SCHEMA
        or raw.get("operation") != SOLVER_STEWARD_ESCALATION_OPERATION
    ):
        raise WorkflowContractError("unsupported Solver steward escalation")
    canonical_path = _relative_path(raw.get("canonical_path"), "canonical_path")
    identity = _object(raw.get("identity"), "identity")
    for field in (
        "campaign",
        "operator",
        "case_version",
        "result_version",
        "blocker_generation",
        "diagnostic_contract_revision",
        "diagnostic_capability_generation",
    ):
        _text(identity.get(field), f"identity.{field}")
    evidence = raw.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise WorkflowContractError("evidence must be a non-empty path list")
    normalized = [
        _relative_path(value, f"evidence[{index}]")
        for index, value in enumerate(evidence)
    ]
    if len(set(normalized)) != len(normalized):
        raise WorkflowContractError("steward escalation evidence must be unique")
    if canonical_path not in normalized:
        raise WorkflowContractError("canonical_path must be included in evidence")
    value = dict(raw)
    value["identity"] = dict(identity)
    value["evidence"] = normalized
    return value


def validate_workflow_action(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or raw.get("schema") != WORKFLOW_ACTION_SCHEMA:
        raise WorkflowContractError("unsupported workflow action")
    for field in (
        "action_id",
        "idempotency_key",
        "action_kind",
        "campaign",
        "operator",
        "board_revision",
        "producer_generation",
        "origin_workspace",
        "created_at",
    ):
        _text(raw.get(field), field)
    control_schema = raw.get("control_schema")
    if not isinstance(control_schema, int) or control_schema < 1:
        raise WorkflowContractError("control_schema must be a positive integer")
    priority = raw.get("priority")
    if not isinstance(priority, int):
        raise WorkflowContractError("priority must be an integer")
    arguments = _object(raw.get("arguments"), "arguments")
    _text(arguments.get("operation"), "arguments.operation")
    positional = arguments.get("positional", [])
    options = arguments.get("options", {})
    if not isinstance(positional, list) or not all(
        isinstance(value, str) for value in positional
    ):
        raise WorkflowContractError("arguments.positional must be a text list")
    if not isinstance(options, Mapping) or not all(
        isinstance(key, str) and isinstance(value, (str, int, float, bool, type(None)))
        for key, value in options.items()
    ):
        raise WorkflowContractError("arguments.options must contain scalar values")
    candidate = raw.get("candidate_identity", {})
    if not isinstance(candidate, Mapping):
        raise WorkflowContractError("candidate_identity must be an object")
    artifacts = raw.get("artifacts", [])
    if not isinstance(artifacts, list) or not all(
        isinstance(value, Mapping) for value in artifacts
    ):
        raise WorkflowContractError("artifacts must be an object list")
    seen_artifacts: set[str] = set()
    for index, artifact in enumerate(artifacts):
        locator_fields = [field for field in ("path", "uri") if artifact.get(field)]
        if len(locator_fields) != 1:
            raise WorkflowContractError(
                f"artifacts[{index}] must have exactly one path or uri"
            )
        locator = _relative_path(
            artifact[locator_fields[0]], f"artifacts[{index}].{locator_fields[0]}"
        )
        if locator in seen_artifacts:
            raise WorkflowContractError("workflow artifacts must have unique paths")
        seen_artifacts.add(locator)
        _sha256(artifact.get("sha256"), f"artifacts[{index}].sha256")
        size = artifact.get("size")
        if size is not None and (
            not isinstance(size, int) or isinstance(size, bool) or size < 0
        ):
            raise WorkflowContractError(f"artifacts[{index}].size must be non-negative")
    return dict(raw)


def validate_workflow_action_receipt(raw: Mapping[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(raw, Mapping)
        or raw.get("schema") != WORKFLOW_ACTION_RECEIPT_SCHEMA
    ):
        raise WorkflowContractError("unsupported workflow action receipt")
    for field in (
        "action_id",
        "status",
        "worker_id",
        "producer_generation",
        "started_at",
        "completed_at",
    ):
        _text(raw.get(field), field)
    if raw["status"] not in {"succeeded", "failed", "cancelled"}:
        raise WorkflowContractError(
            f"unsupported workflow action receipt status: {raw['status']}"
        )
    return_code = raw.get("return_code")
    if return_code is not None and not isinstance(return_code, int):
        raise WorkflowContractError("return_code must be an integer or null")
    details = raw.get("details", {})
    if not isinstance(details, Mapping):
        raise WorkflowContractError("details must be an object")
    return dict(raw)


def validate_solver_diagnostic_request(raw: Mapping[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(raw, Mapping)
        or raw.get("schema") != SOLVER_DIAGNOSTIC_REQUEST_SCHEMA
    ):
        raise WorkflowContractError("unsupported Solver diagnostic request")
    for field in (
        "campaign",
        "operator",
        "case_version",
        "result_version",
        "blocker_generation",
        "operation_kind",
        "created_at",
        "rationale",
    ):
        _text(raw.get(field), field)
    if raw.get("producer") != "solver":
        raise WorkflowContractError("producer must be solver")
    operation = str(raw["operation_kind"])
    if operation not in SOLVER_DIAGNOSTIC_OPERATIONS:
        raise WorkflowContractError(f"unsupported diagnostic operation: {operation}")
    consulted_evidence = raw.get("consulted_evidence")
    if (
        not isinstance(consulted_evidence, list)
        or not consulted_evidence
        or not all(
            isinstance(value, str) and value.strip()
            for value in consulted_evidence
        )
    ):
        raise WorkflowContractError(
            "consulted_evidence must be a non-empty text list"
        )

    target = _diagnostic_target(raw.get("target"), "target")
    comparison_targets = raw.get("comparison_targets", [])
    if not isinstance(comparison_targets, list) or len(comparison_targets) > 3:
        raise WorkflowContractError(
            "comparison_targets must be a list of at most 3 targets"
        )
    normalized_comparisons = [
        _diagnostic_target(value, f"comparison_targets[{index}]")
        for index, value in enumerate(comparison_targets)
    ]
    target_versions = [
        str(target["test_version"]),
        *(str(value["test_version"]) for value in normalized_comparisons),
    ]
    if len(target_versions) != len(set(target_versions)):
        raise WorkflowContractError(
            "diagnostic target test_version values must be unique"
        )

    scope = _object(raw.get("scope"), "scope")
    if scope.get("case_selection") != "all":
        raise WorkflowContractError("scope.case_selection must be all")
    fresh_processes = scope.get("fresh_processes")
    if fresh_processes != 1:
        raise WorkflowContractError("scope.fresh_processes must be 1")
    requested_artifacts = scope.get("requested_artifacts", [])
    if not isinstance(requested_artifacts, list) or not requested_artifacts:
        raise WorkflowContractError(
            "scope.requested_artifacts must be a non-empty text list"
        )
    if not all(
        isinstance(value, str) and value.strip() for value in requested_artifacts
    ):
        raise WorkflowContractError(
            "scope.requested_artifacts must be a non-empty text list"
        )
    profiler_mode = str(scope.get("profiler_mode") or "none")
    measurement_repetitions = scope.get("measurement_repetitions", 1)
    if (
        not isinstance(measurement_repetitions, int)
        or isinstance(measurement_repetitions, bool)
        or not 1 <= measurement_repetitions <= 5
    ):
        raise WorkflowContractError(
            "scope.measurement_repetitions must be an integer in [1, 5]"
        )
    comparison_affinity = str(scope.get("comparison_affinity") or "")
    expected_block_dims = scope.get("expected_block_dims", {})
    if not isinstance(expected_block_dims, Mapping):
        raise WorkflowContractError("scope.expected_block_dims must be an object")
    for case_id, block_dim in expected_block_dims.items():
        if (
            not isinstance(case_id, str)
            or not re.fullmatch(r"[1-9][0-9]*", case_id)
            or not isinstance(block_dim, int)
            or isinstance(block_dim, bool)
            or block_dim <= 0
        ):
            raise WorkflowContractError(
                "scope.expected_block_dims must map decimal case ids to positive integers"
            )
    if operation == "diagnostic-profile":
        if profiler_mode not in SOLVER_DIAGNOSTIC_PROFILER_MODES:
            raise WorkflowContractError(
                f"unsupported diagnostic profiler mode: {profiler_mode}"
            )
        if normalized_comparisons and comparison_affinity != (
            "same-endpoint-environment-device"
        ):
            raise WorkflowContractError(
                "comparison targets require same-endpoint-environment-device affinity"
            )
        if not normalized_comparisons and comparison_affinity:
            raise WorkflowContractError(
                "comparison_affinity requires at least one comparison target"
            )
    else:
        if normalized_comparisons:
            raise WorkflowContractError(
                "diagnostic-correctness-replay cannot contain comparison targets"
            )
        if measurement_repetitions != 1:
            raise WorkflowContractError(
                "diagnostic-correctness-replay measurement_repetitions must be 1"
            )
        if comparison_affinity or expected_block_dims:
            raise WorkflowContractError(
                "diagnostic-correctness-replay cannot request profiler comparison controls"
            )
        if profiler_mode != "none":
            raise WorkflowContractError(
                "diagnostic-correctness-replay profiler_mode must be none"
            )
        unsupported = sorted(
            set(requested_artifacts) - set(SOLVER_DIAGNOSTIC_CORRECTNESS_ARTIFACTS)
        )
        if unsupported:
            allowed = ", ".join(SOLVER_DIAGNOSTIC_CORRECTNESS_ARTIFACTS)
            raise WorkflowContractError(
                "unsupported diagnostic-correctness-replay artifacts: "
                f"{', '.join(unsupported)}; allowed: {allowed}"
            )

    requested_seconds = raw.get("requested_device_session_seconds")
    if (
        not isinstance(requested_seconds, int)
        or isinstance(requested_seconds, bool)
        or not 30 <= requested_seconds <= 240
    ):
        raise WorkflowContractError(
            "requested_device_session_seconds must be an integer in [30, 240]"
        )
    retry = _object(raw.get("retry_policy"), "retry_policy")
    if retry.get("max_execution_attempts") != 1:
        raise WorkflowContractError("diagnostic max_execution_attempts must be 1")
    if retry.get("max_device_stage_retries") != 0:
        raise WorkflowContractError("diagnostic max_device_stage_retries must be 0")
    if retry.get("transport_reuses_request_id") is not True:
        raise WorkflowContractError(
            "diagnostic transport retries must reuse request_id"
        )
    return dict(raw)


def _diagnostic_target(value: Any, field: str) -> Mapping[str, Any]:
    target = _object(value, field)
    _text(target.get("test_version"), f"{field}.test_version")
    _sha256(target.get("source_sha256"), f"{field}.source_sha256")
    return target


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkflowContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkflowContractError(f"{field} must be non-empty text")
    return value.strip()


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    if text.startswith("/") or any(part in {"", ".", ".."} for part in text.split("/")):
        raise WorkflowContractError(f"{field} must be a bounded relative path")
    return text


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise WorkflowContractError(f"{field} must be SHA-256")
    return text
