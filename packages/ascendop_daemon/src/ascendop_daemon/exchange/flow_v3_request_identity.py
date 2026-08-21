from __future__ import annotations

from typing import Any, Mapping

from ascendop_daemon.exchange.flow_v3_request_validation import (
    FLOW_V3_RESULT_ADAPTER,
)


BUSINESS_FAILURE_REQUIRED_ARTIFACTS = {
    "result/SUMMARY.txt",
    "result/PHASE_TIMELINE.jsonl",
    "result/ENGINE_IDENTITY.json",
    "result/CORRECTNESS.json",
    "result/CORRECTNESS_SUMMARY.txt",
    "result/CORRECTNESS_BATCH.json",
    "result/RUNTIME_BOUNDARY_TRACE.json",
    "result/runtime_boundary",
    "result/NATIVE_WORKSPACE_QUERY_ATTRIBUTION.json",
    "result/HOST_CALLBACK_ATTRIBUTION.json",
    "result/KERNEL_FAULT_ATTRIBUTION.json",
    "result/kernel_fault",
}


def request_identity_material(
    *,
    operator: str,
    test_version: str,
    case_version: str,
    payload_digest: str,
    execution_spec_digest: str,
    code_generation: str,
    endpoint_id: str,
    endpoint_generation: str,
    registration_generation: str,
    operation_kind: str,
    profiler_mode: str,
    extensions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    material: dict[str, Any] = {
        "operator": operator,
        "test_version": test_version,
        "case_version": case_version,
        "payload_digest": payload_digest,
        "execution_spec_digest": execution_spec_digest,
        "code_generation": code_generation,
        "endpoint_id": endpoint_id,
        "endpoint_generation": endpoint_generation,
        "registration_generation": registration_generation,
        "operation_kind": operation_kind,
        "profiler_mode": profiler_mode,
    }
    material.update(dict(extensions or {}))
    return material


def execution_identity_projection(
    internal_spec: Mapping[str, Any],
) -> dict[str, Any]:
    """Remove attempt-local labels while preserving executable semantics."""

    volatile = {
        str(internal_spec.get("request_id") or ""),
        str(internal_spec.get("engine_job_id") or ""),
        str(internal_spec.get("attempt_id") or ""),
    }
    tokens = sorted((item for item in volatile if item), key=len, reverse=True)

    def project(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): project(item)
                for key, item in value.items()
                if str(key) not in {"request_id", "engine_job_id", "attempt_id"}
            }
        if isinstance(value, list):
            return [project(item) for item in value]
        if isinstance(value, str):
            projected = value
            for token in tokens:
                projected = projected.replace(token, "<attempt-identity>")
            return projected
        return value

    return project(internal_spec)


def result_contract_for_operator_test(
    internal_spec: Mapping[str, Any],
) -> dict[str, Any]:
    required = [
        str(item) for item in internal_spec.get("required_artifacts", []) if str(item)
    ]
    return {
        "ingest_adapter": FLOW_V3_RESULT_ADAPTER,
        "terminal_states": [
            "terminal-success",
            "terminal-business-failure",
            "terminal-infrastructure-failure",
            "terminal-cancelled",
        ],
        "required_artifacts": required,
        "required_artifacts_by_terminal_state": {
            "terminal-success": required,
            "terminal-business-failure": [
                item for item in required if item in BUSINESS_FAILURE_REQUIRED_ARTIFACTS
            ],
        },
        "optional_artifacts": [
            str(item)
            for item in internal_spec.get("optional_artifacts", [])
            if str(item)
        ],
    }
