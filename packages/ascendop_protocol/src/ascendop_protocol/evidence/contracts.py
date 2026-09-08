from __future__ import annotations

import re
from typing import Any, Mapping

from .registry import (
    evidence_operation_definition,
    evidence_operation_registry_for_identity,
)


EVIDENCE_OPERATION_REQUEST_SCHEMA = "ascendop.evidence-operation-request.v1"
EVIDENCE_OPERATION_RESULT_SCHEMA = "ascendop.evidence-operation-result.v1"
EVIDENCE_REQUEST_STATES = frozenset(
    {"queued", "claimed", "routed", "running", "completed", "failed", "cancelled"}
)
EVIDENCE_RESULT_STATUSES = frozenset({"completed", "failed", "cancelled"})
FLOW_V5_ROLES = frozenset({"assistant", "developer", "manager", "solver", "tester"})
EVIDENCE_OPERATION_REQUIRED_PARAMETERS = {
    "test.correctness": ("candidate_id", "test_version", "case_version"),
    "test.performance": (
        "candidate_id",
        "test_version",
        "case_version",
    ),
    "profile.collect": (
        "candidate_id",
        "test_version",
        "case_version",
        "profiler_mode",
    ),
    "profile.compare": (
        "baseline_evidence_ref",
        "candidate_evidence_ref",
        "comparison_rule",
    ),
    "correctness.replay": ("candidate_id", "test_version", "case_version"),
    "environment.conformance": (
        "endpoint_id",
        "execution_environment_id",
        "requirements_generation",
    ),
    "artifact.recover": (
        "artifact_ref",
        "expected_sha256",
        "recovery_source",
    ),
}


class EvidenceOperationContractError(ValueError):
    pass


def validate_evidence_operation_request(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, EVIDENCE_OPERATION_REQUEST_SCHEMA)
    for field in (
        "operation_request_id",
        "idempotency_key",
        "registry_generation",
        "registry_digest",
        "operation_code",
        "expected_consumer",
        "resume_condition",
        "state",
        "created_at",
    ):
        _text(raw.get(field), field)
    _token(raw["operation_request_id"], "operation_request_id")
    registry = _registry_for_contract(raw)
    code = str(raw["operation_code"])
    codes = {row["operation_code"] for row in registry["operations"]}
    if code not in codes:
        raise EvidenceOperationContractError(f"unsupported evidence operation: {code}")
    definition = evidence_operation_definition(code, registry=registry)
    consumer = str(raw["expected_consumer"])
    if consumer not in definition["expected_consumers"]:
        raise EvidenceOperationContractError(
            f"operation {code} does not produce evidence for {consumer}"
        )
    if raw["state"] not in EVIDENCE_REQUEST_STATES:
        raise EvidenceOperationContractError(
            f"unsupported evidence request state: {raw['state']}"
        )
    _validate_origin(_object(raw.get("origin"), "origin"))
    validate_evidence_operation_parameters(
        code, _object(raw.get("parameters"), "parameters")
    )
    return dict(raw)


def validate_evidence_operation_result(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, EVIDENCE_OPERATION_RESULT_SCHEMA)
    for field in (
        "operation_result_id",
        "operation_request_id",
        "registry_generation",
        "registry_digest",
        "operation_code",
        "expected_consumer",
        "status",
        "summary",
        "completed_at",
    ):
        _text(raw.get(field), field)
    _token(raw["operation_result_id"], "operation_result_id")
    _token(raw["operation_request_id"], "operation_request_id")
    registry = _registry_for_contract(raw)
    code = str(raw["operation_code"])
    codes = {row["operation_code"] for row in registry["operations"]}
    if code not in codes:
        raise EvidenceOperationContractError(f"unsupported evidence operation: {code}")
    definition = evidence_operation_definition(code, registry=registry)
    consumer = str(raw["expected_consumer"])
    if consumer not in definition["expected_consumers"]:
        raise EvidenceOperationContractError(
            f"operation {code} does not produce evidence for {consumer}"
        )
    status = str(raw["status"])
    if status not in EVIDENCE_RESULT_STATUSES:
        raise EvidenceOperationContractError(
            f"unsupported evidence result status: {status}"
        )
    _validate_origin(_object(raw.get("origin"), "origin"))
    execution = _object(raw.get("execution"), "execution")
    for field in (
        "test_request_id",
        "wire_attempt_id",
        "endpoint_id",
        "execution_environment_id",
    ):
        value = execution.get(field)
        if value not in {None, ""}:
            _text(value, f"execution.{field}")
    evidence = _object(raw.get("evidence"), "evidence")
    expected_type = str(definition["produced_evidence_types"][0])
    if evidence.get("evidence_type") != expected_type:
        raise EvidenceOperationContractError(
            f"operation {code} requires evidence type {expected_type}"
        )
    _relative_path_list(evidence.get("artifact_refs"), "evidence.artifact_refs")
    _object(evidence.get("payload"), "evidence.payload")
    failure_class = raw.get("failure_class")
    if status == "completed":
        if failure_class is not None:
            raise EvidenceOperationContractError(
                "completed evidence result must not have failure_class"
            )
    else:
        _text(failure_class, "failure_class")
    return dict(raw)


def _registry_for_contract(raw: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return evidence_operation_registry_for_identity(
            str(raw["registry_generation"]), str(raw["registry_digest"])
        )
    except KeyError as exc:
        raise EvidenceOperationContractError(
            "evidence registry identity is not current or pinned history"
        ) from exc


def _validate_origin(origin: Mapping[str, Any]) -> None:
    for field in ("action_id", "iteration_id", "operator_id", "role"):
        _text(origin.get(field), f"origin.{field}")
    if origin["role"] not in FLOW_V5_ROLES:
        raise EvidenceOperationContractError(f"unsupported origin role: {origin['role']}")


def evidence_operation_required_parameters(operation_code: str) -> tuple[str, ...]:
    try:
        return EVIDENCE_OPERATION_REQUIRED_PARAMETERS[operation_code]
    except KeyError as exc:
        raise EvidenceOperationContractError(
            f"unsupported evidence operation: {operation_code}"
        ) from exc


def validate_evidence_operation_parameters(
    code: str, parameters: Mapping[str, Any]
) -> dict[str, Any]:
    required = evidence_operation_required_parameters(code)
    for field in required:
        _text(parameters.get(field), f"parameters.{field}")
    if code == "correctness.replay":
        cases = parameters.get("failing_cases")
        if not isinstance(cases, list) or not cases or any(
            not isinstance(item, (str, int)) or str(item).strip() == ""
            for item in cases
        ):
            raise EvidenceOperationContractError(
                "parameters.failing_cases must be a non-empty scalar list"
            )
    if code == "profile.collect":
        kernel_name = parameters.get("profiler_kernel_name")
        if kernel_name is not None:
            _token(kernel_name, "parameters.profiler_kernel_name")
        kernel_selection = parameters.get("profiler_kernel_selection")
        if kernel_selection is not None and kernel_selection not in {
            "exact",
            "prefix-postfilter",
        }:
            raise EvidenceOperationContractError(
                "parameters.profiler_kernel_selection must be exact or "
                "prefix-postfilter"
            )
    if code == "test.performance":
        baseline_result_id = parameters.get("baseline_result_id")
        baseline_bootstrap = parameters.get("baseline_bootstrap")
        if baseline_bootstrap is True:
            if baseline_result_id not in {None, ""}:
                raise EvidenceOperationContractError(
                    "parameters.baseline_bootstrap and baseline_result_id are "
                    "mutually exclusive"
                )
        elif baseline_bootstrap in {None, False}:
            _text(baseline_result_id, "parameters.baseline_result_id")
        else:
            raise EvidenceOperationContractError(
                "parameters.baseline_bootstrap must be a boolean"
            )
    if code in {"profile.compare", "artifact.recover"}:
        for field in (
            ("baseline_evidence_ref", "candidate_evidence_ref")
            if code == "profile.compare"
            else ("artifact_ref",)
        ):
            _relative_path(parameters[field], f"parameters.{field}")
    if code == "artifact.recover":
        _sha256(parameters["expected_sha256"], "parameters.expected_sha256")
    return dict(parameters)


def _schema(raw: Mapping[str, Any], expected: str) -> None:
    if not isinstance(raw, Mapping) or raw.get("schema") != expected:
        raise EvidenceOperationContractError(
            f"unsupported contract; expected {expected}"
        )


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceOperationContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceOperationContractError(f"{field} must be non-empty text")
    return value.strip()


def _token(value: Any, field: str) -> str:
    text = _text(value, field)
    if not re.fullmatch(r"[A-Za-z0-9._-]+", text):
        raise EvidenceOperationContractError(f"{field} must be a safe token")
    return text


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise EvidenceOperationContractError(f"{field} must be SHA-256")
    return text


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    if text.startswith("/") or any(part in {"", ".."} for part in text.split("/")):
        raise EvidenceOperationContractError(f"{field} must be a bounded relative path")
    return text


def _relative_path_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise EvidenceOperationContractError(f"{field} must be a text list")
    if len(value) != len(set(value)):
        raise EvidenceOperationContractError(f"{field} must not contain duplicates")
    for item in value:
        _relative_path(item, field)
    return list(value)


__all__ = [
    "EVIDENCE_OPERATION_REQUEST_SCHEMA",
    "EVIDENCE_OPERATION_RESULT_SCHEMA",
    "EVIDENCE_OPERATION_REQUIRED_PARAMETERS",
    "EVIDENCE_REQUEST_STATES",
    "EVIDENCE_RESULT_STATUSES",
    "EvidenceOperationContractError",
    "evidence_operation_required_parameters",
    "validate_evidence_operation_request",
    "validate_evidence_operation_parameters",
    "validate_evidence_operation_result",
]
