from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.core.filesystem import filesystem_path
from ascendop_daemon.workflow.operator_job_builder import (
    FUSED_SCALABLE_PROFILE,
    PROFILER_EVIDENCE_PROFILE,
    build_compatibility_job,
    canonical_object_sha256,
    parse_submit_command,
    safe_token,
)
from ascendop_daemon.exchange.flow_v3_payload import package_payload
from ascendop_daemon.control_plane.flow_v3_policy import RETRY_POLICY_VERSION, grant_device_budget
from ascendop_protocol.wire_v3 import build_envelope, validate_envelope


FLOW_V3_EXECUTION_PROFILE = "correctness-first-all-cases-v3"
FLOW_V3_RESULT_ADAPTER = "cannjudge-result-v3"
BUSINESS_FAILURE_REQUIRED_ARTIFACTS = {
    "result/SUMMARY.txt",
    "result/PHASE_TIMELINE.jsonl",
    "result/ENGINE_IDENTITY.json",
    "result/CORRECTNESS.json",
    "result/CORRECTNESS_SUMMARY.txt",
    "result/CORRECTNESS_BATCH.json",
}
STAGE_NAME_MAP = {
    "prepare-materialize-environment": "host-prepare",
    "prepare-build-install-wheel": "host-prepare",
    "operator-build-install": "operator-build",
    "runtime-wheel-install": "runtime-install",
    "case-cache": "case-cache",
    "correctness": "correctness",
    "performance-capture": "performance-primary",
    "profile-export": "profile-export",
    "profile-parse": "profile-parse",
    "assemble-result": "result-assemble",
    "postprocess-result": "result-assemble",
}


class FlowV3RequestBuildError(RuntimeError):
    pass


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
        str(item)
        for item in internal_spec.get("required_artifacts", [])
        if str(item)
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
                item
                for item in required
                if item in BUSINESS_FAILURE_REQUIRED_ARTIFACTS
            ],
        },
        "optional_artifacts": [
            str(item)
            for item in internal_spec.get("optional_artifacts", [])
            if str(item)
        ],
    }


def build_diagnostic_request(
    root: Path,
    candidate: Mapping[str, Any],
    *,
    profiler_plan: Mapping[str, Any],
    profiler_mode: str,
    endpoint_id: str,
    endpoint_generation: str,
    registration_generation: str,
    code_generation: str,
    remote_root: str,
    package_root: Path,
    request_id_override: str = "",
    attempt_id_override: str = "",
) -> tuple[dict[str, Any], Path]:
    if profiler_mode not in {
        "primary-all-cases",
        "primary-roofline-all-cases",
    }:
        raise FlowV3RequestBuildError(
            f"unsupported diagnostic profiler mode: {profiler_mode}"
        )
    root = root.resolve()
    command = str(candidate.get("command") or "")
    parsed = parse_submit_command(command)
    test_version = str(candidate.get("test_version") or parsed["test_version"])
    if parsed["test_version"] != test_version:
        raise FlowV3RequestBuildError(
            "diagnostic candidate test version does not match command"
        )
    attempt_index = int(candidate.get("attempt_index", 1) or 1)
    internal_spec_path, payload_root = build_compatibility_job(
        root,
        command,
        remote_root=remote_root,
        submit_root_override=(
            Path(str(candidate["submit_root_override"])).resolve()
            if candidate.get("submit_root_override")
            else None
        ),
        execution_profile=PROFILER_EVIDENCE_PROFILE,
        job_id_suffix=str(candidate.get("job_id_suffix") or "diagnostic"),
        attempt_index=attempt_index,
        workflow_ingest=False,
        profiler_plan=dict(profiler_plan),
    )
    internal_spec = read_object(internal_spec_path)
    normalized_plan = read_object(payload_root / "profiler_plan.json")
    performance_cases = [
        int(value)
        for value in internal_spec.get("test_contract", {}).get(
            "performance_cases", []
        )
    ]
    if list(normalized_plan.get("cases", [])) != performance_cases:
        raise FlowV3RequestBuildError(
            "diagnostic profiler must cover every performance case exactly once"
        )
    expected_engine_mode = (
        "fast-single"
        if profiler_mode == "primary-all-cases"
        else "deep-dual"
    )
    if str(normalized_plan.get("profiler_mode") or "") != expected_engine_mode:
        raise FlowV3RequestBuildError(
            "diagnostic profiler plan mode does not match Wire V3 mode"
        )
    if int(normalized_plan.get("profile_timeout_seconds", 0) or 0) != 90:
        raise FlowV3RequestBuildError(
            "diagnostic profiler process timeout must be exactly 90 seconds"
        )
    if (
        profiler_mode == "primary-roofline-all-cases"
        and list(normalized_plan.get("roofline_cases", [])) != performance_cases
    ):
        raise FlowV3RequestBuildError(
            "diagnostic roofline pass must cover every performance case"
        )
    material = request_identity_material(
        operator=parsed["op"],
        test_version=test_version,
        case_version=parsed["case_version"],
        payload_digest=str(internal_spec["bundle_hash"]),
        execution_spec_digest=canonical_object_sha256(internal_spec),
        code_generation=code_generation,
        endpoint_id=endpoint_id,
        endpoint_generation=endpoint_generation,
        registration_generation=registration_generation,
        operation_kind="diagnostic-profile",
        profiler_mode=profiler_mode,
        extensions={
            "profiler_plan_digest": canonical_object_sha256(normalized_plan),
        },
    )
    request_digest = canonical_object_sha256(material)
    request_id = (
        safe_token(request_id_override)
        if request_id_override
        else safe_token(f"diag-{test_version}-{request_digest[:12]}")
    )
    attempt_id = (
        safe_token(attempt_id_override)
        if attempt_id_override
        else f"attempt-{attempt_index:03d}"
    )
    payload = package_payload(
        payload_root,
        package_root,
        request_id=request_id,
    )
    grant = grant_device_budget(
        {
            "budget_class": "diagnostic",
            "performance_mode": profiler_mode,
        }
    )
    stages = translate_diagnostic_stage_plan(
        internal_spec,
        profiler_mode=profiler_mode,
        granted_device_session_seconds=grant.granted_device_session_seconds,
    )
    envelope = build_envelope(
        meta={
            "request_id": request_id,
            "attempt_id": attempt_id,
            "trace_id": f"trace-{request_id}",
            "producer": "daemon",
            "code_generation": code_generation,
        },
        identity={
            "endpoint_id": endpoint_id,
            "endpoint_generation": endpoint_generation,
            "registration_generation": registration_generation,
            "idempotency_key": request_id,
        },
        workflow={
            "domain": "cannjudge",
            "season": parsed["season"],
            "operator": parsed["op"],
            "test_version": test_version,
            "case_version": parsed["case_version"],
            "gate": "diagnostic-only",
            "operation_kind": "diagnostic-profile",
            "source_command": command,
            "mode": parsed["mode"],
            "vendor": parsed["vendor"],
            "hardware": parsed["hardware"],
            "job_kind": "profiler-evidence",
            "profiler": normalized_plan,
        },
        payload={
            **payload,
            "package_root": str(package_root.resolve()),
            "source_bundle_digest": str(internal_spec["bundle_hash"]),
        },
        execution={
            "profile": "diagnostic-all-cases-v3",
            **grant.to_dict(),
            "correctness_required": False,
            "publish_eligible": False,
            "performance_mode": profiler_mode,
            "required_capabilities": [
                "flow-v3",
                "engine-archive",
                "diagnostic-profile",
                "device-session-wall-budget",
            ],
            "resources": {
                "host_cpu_weight": 4,
                "host_memory_mb": 8192,
                "host_io_weight": 4,
                "device_count": 1,
            },
            "stages": stages,
            "test_contract": internal_spec.get("test_contract", {}),
            "profiler_plan_digest": canonical_object_sha256(normalized_plan),
        },
        retry_policy={
            "policy_id": RETRY_POLICY_VERSION,
            "max_execution_attempts": 1,
            "max_transport_retries": 3,
            "max_idempotent_stage_retries": 1,
        },
        result_contract={
            "ingest_adapter": FLOW_V3_RESULT_ADAPTER,
            "terminal_states": [
                "terminal-success",
                "terminal-infrastructure-failure",
                "terminal-cancelled",
            ],
            "required_artifacts": [
                str(item)
                for item in internal_spec.get("required_artifacts", [])
                if str(item)
            ],
            "optional_artifacts": [
                str(item)
                for item in internal_spec.get("optional_artifacts", [])
                if str(item)
            ],
        },
    )
    request_dir = package_root.resolve() / request_id
    envelope_path = request_dir / "REQUEST_ENVELOPE.json"
    write_immutable_json(envelope_path, envelope)
    return envelope, envelope_path


def translate_diagnostic_stage_plan(
    internal_spec: Mapping[str, Any],
    *,
    profiler_mode: str,
    granted_device_session_seconds: int,
) -> list[dict[str, Any]]:
    name_map = {
        "prepare-materialize-environment": "host-prepare",
        "operator-build-install": "operator-build",
        "runtime-wheel-install": "runtime-install",
        "profiler-evidence": "performance-primary",
        "assemble-profiler-evidence": "result-assemble",
    }
    raw_stages = internal_spec.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise FlowV3RequestBuildError("diagnostic stage plan is missing")
    stages: list[dict[str, Any]] = []
    for raw in raw_stages:
        original = str(raw.get("name") or "")
        name = name_map.get(original)
        if not name:
            raise FlowV3RequestBuildError(
                f"unsupported diagnostic stage: {original}"
            )
        resource = str(raw.get("resource") or "host")
        resource_class = stage_resource_class(original, resource)
        dependencies = [
            name_map[str(item)]
            for item in raw.get("depends_on", [])
            if str(item) in name_map
        ]
        timeout_seconds = int(raw.get("timeout_seconds", 0) or 0)
        if name == "performance-primary":
            timeout_seconds = granted_device_session_seconds
        stages.append(
            {
                "name": name,
                "resource_class": resource_class,
                "depends_on": dependencies,
                "idempotent": resource != "device",
                "timeout_seconds": timeout_seconds,
                "command": list(raw.get("command", [])),
                "failure_policy": "skip-dependents",
                "max_stage_retries": 1 if resource != "device" else 0,
            }
        )
    enforce_host_handoff_dependencies(stages)
    names = {str(stage["name"]) for stage in stages}
    if "correctness" in names or "performance-primary" not in names:
        raise FlowV3RequestBuildError(
            "diagnostic profile must contain only profiler device work"
        )
    if profiler_mode not in {
        "primary-all-cases",
        "primary-roofline-all-cases",
    }:
        raise FlowV3RequestBuildError(
            f"unsupported diagnostic profiler mode: {profiler_mode}"
        )
    return validate_envelope_stages(
        stages,
        operation_kind="diagnostic-profile",
        profiler_mode=profiler_mode,
    )


def build_candidate_request(
    root: Path,
    candidate: Mapping[str, Any],
    *,
    endpoint_id: str,
    endpoint_generation: str,
    registration_generation: str,
    code_generation: str,
    remote_root: str,
    package_root: Path,
    submit_root_override: Path | None = None,
    profiler_mode: str = "primary-all-cases",
    requested_device_session_seconds: int = 0,
    budget_class: str = "standard",
    gate_evidence: str = "",
    operation_kind: str = "operator-test",
    request_id_override: str = "",
    attempt_id_override: str = "",
    publish_eligible: bool = True,
) -> tuple[dict[str, Any], Path]:
    root = root.resolve()
    command = str(candidate.get("command") or "")
    parsed = parse_submit_command(command)
    test_version = str(candidate.get("test_version") or parsed["test_version"])
    if parsed["test_version"] != test_version:
        raise FlowV3RequestBuildError("candidate test version does not match command")
    if operation_kind != "operator-test":
        raise FlowV3RequestBuildError(
            "candidate request builder only owns operator-test; use the diagnostic builder"
        )
    # Workflow retry counters are legacy presentation state. A new V3 logical
    # request always starts at attempt-001; only the Retry Controller may mint
    # a later execution attempt for that request.
    attempt_index = 1
    internal_spec_path, payload_root = build_compatibility_job(
        root,
        command,
        remote_root=remote_root,
        submit_root_override=submit_root_override,
        execution_profile=FUSED_SCALABLE_PROFILE,
        job_id_suffix=str(candidate.get("job_id_suffix") or ""),
        attempt_index=attempt_index,
        workflow_ingest=True,
    )
    internal_spec = read_object(internal_spec_path)
    material = request_identity_material(
        operator=parsed["op"],
        test_version=test_version,
        case_version=parsed["case_version"],
        payload_digest=str(internal_spec["bundle_hash"]),
        execution_spec_digest=canonical_object_sha256(
            execution_identity_projection(internal_spec)
        ),
        code_generation=code_generation,
        endpoint_id=endpoint_id,
        endpoint_generation=endpoint_generation,
        registration_generation=registration_generation,
        operation_kind=operation_kind,
        profiler_mode=profiler_mode,
    )
    request_digest = canonical_object_sha256(material)
    request_id = (
        safe_token(request_id_override)
        if request_id_override
        else safe_token(f"flow-{test_version}-{request_digest[:12]}")
    )
    attempt_id = (
        safe_token(attempt_id_override)
        if attempt_id_override
        else f"attempt-{attempt_index:03d}"
    )
    payload = package_payload(
        payload_root,
        package_root,
        request_id=request_id,
    )
    grant = grant_device_budget(
        {
            "budget_class": budget_class,
            "requested_device_session_seconds": requested_device_session_seconds,
            "performance_mode": profiler_mode,
        },
        gate_evidence=gate_evidence,
    )
    gate_evidence_value: dict[str, Any] | None = None
    if gate_evidence:
        try:
            parsed_gate_evidence = json.loads(gate_evidence)
        except json.JSONDecodeError as exc:
            raise FlowV3RequestBuildError(
                "heavy gate evidence must be canonical JSON"
            ) from exc
        if not isinstance(parsed_gate_evidence, dict):
            raise FlowV3RequestBuildError(
                "heavy gate evidence must be a JSON object"
            )
        gate_evidence_value = parsed_gate_evidence
    stages = translate_stage_plan(
        internal_spec,
        profiler_mode=profiler_mode,
        granted_device_session_seconds=grant.granted_device_session_seconds,
    )
    envelope = build_envelope(
        meta={
            "request_id": request_id,
            "attempt_id": attempt_id,
            "trace_id": f"trace-{request_id}",
            "producer": "daemon",
            "code_generation": code_generation,
        },
        identity={
            "endpoint_id": endpoint_id,
            "endpoint_generation": endpoint_generation,
            "registration_generation": registration_generation,
            "idempotency_key": request_id,
        },
        workflow={
            "domain": "cannjudge",
            "season": parsed["season"],
            "operator": parsed["op"],
            "test_version": test_version,
            "case_version": parsed["case_version"],
            "gate": "submit-ready" if publish_eligible else "acceptance-only",
            "operation_kind": operation_kind,
            "source_command": command,
            "mode": parsed["mode"],
            "vendor": parsed["vendor"],
            "hardware": parsed["hardware"],
        },
        payload={
            **payload,
            "package_root": str(package_root.resolve()),
            "source_bundle_digest": str(internal_spec["bundle_hash"]),
        },
        execution={
            "profile": FLOW_V3_EXECUTION_PROFILE,
            **grant.to_dict(),
            **(
                {
                    "budget_gate_evidence": gate_evidence_value,
                    "budget_gate_evidence_digest": hashlib.sha256(
                        gate_evidence.encode("utf-8")
                    ).hexdigest(),
                }
                if gate_evidence_value is not None
                else {}
            ),
            "correctness_required": True,
            "publish_eligible": bool(publish_eligible),
            "performance_mode": profiler_mode,
            "required_capabilities": [
                "flow-v3",
                "engine-archive",
                "correctness-first",
                "device-session-wall-budget",
            ],
            "resources": {
                "host_cpu_weight": 4,
                "host_memory_mb": 8192,
                "host_io_weight": 4,
                "device_count": int(
                    internal_spec.get("execution_requirements", {}).get(
                        "device_count", 1
                    )
                    or 1
                ),
            },
            "stages": stages,
            "test_contract": internal_spec.get("test_contract", {}),
        },
        retry_policy={
            "policy_id": RETRY_POLICY_VERSION,
            "max_execution_attempts": 2 if publish_eligible else 1,
            "max_transport_retries": 3,
            "max_idempotent_stage_retries": 1,
        },
        result_contract=result_contract_for_operator_test(internal_spec),
    )
    request_dir = package_root.resolve() / request_id
    envelope_path = request_dir / "REQUEST_ENVELOPE.json"
    write_immutable_json(envelope_path, envelope)
    return envelope, envelope_path


def translate_stage_plan(
    internal_spec: Mapping[str, Any],
    *,
    profiler_mode: str,
    granted_device_session_seconds: int,
) -> list[dict[str, Any]]:
    raw_stages = internal_spec.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise FlowV3RequestBuildError("internal stage plan is missing")
    translated_names: dict[str, str] = {}
    for raw in raw_stages:
        if not isinstance(raw, Mapping):
            raise FlowV3RequestBuildError("internal stage plan contains a non-object")
        original = str(raw.get("name") or "")
        translated = STAGE_NAME_MAP.get(original, safe_token(original))
        if translated in translated_names.values():
            raise FlowV3RequestBuildError(
                f"stage translation creates a duplicate name: {translated}"
            )
        translated_names[original] = translated
    stages: list[dict[str, Any]] = []
    for raw in raw_stages:
        original = str(raw["name"])
        name = translated_names[original]
        resource = str(raw.get("resource") or "host")
        resource_class = stage_resource_class(original, resource)
        dependencies = [
            translated_names[str(item)]
            for item in raw.get("depends_on", [])
            if str(item) in translated_names
        ]
        timeout_seconds = int(raw.get("timeout_seconds", 0) or 0)
        if name == "correctness":
            timeout_seconds = min(
                max(1, timeout_seconds or granted_device_session_seconds),
                granted_device_session_seconds,
            )
        elif name == "performance-primary":
            timeout_seconds = 90
        stages.append(
            {
                "name": name,
                "resource_class": resource_class,
                "depends_on": dependencies,
                "idempotent": resource_class != "device",
                "timeout_seconds": timeout_seconds,
                "command": list(raw.get("command", [])),
                "failure_policy": (
                    "business-terminal-continue-cases"
                    if name == "correctness"
                    else "skip-dependents"
                ),
                "max_stage_retries": (
                    1 if resource_class in {"host-light", "host-build-heavy", "export"} else 0
                ),
            }
        )
    enforce_host_handoff_dependencies(stages)
    enforce_correctness_first(stages)
    if profiler_mode == "none":
        stages = [
            stage
            for stage in stages
            if stage["name"]
            not in {"performance-primary", "profile-export", "profile-parse"}
        ]
        for stage in stages:
            if stage["name"] == "result-assemble":
                stage["depends_on"] = ["correctness"]
    elif profiler_mode not in {
        "primary-all-cases",
        "primary-roofline-all-cases",
    }:
        raise FlowV3RequestBuildError(f"unsupported profiler mode: {profiler_mode}")
    if profiler_mode == "primary-roofline-all-cases":
        raise FlowV3RequestBuildError(
            "operator-test uses primary-all-cases; roofline is a diagnostic-profile operation"
        )
    return validate_envelope_stages(stages)


def enforce_correctness_first(stages: list[dict[str, Any]]) -> None:
    names = {str(stage["name"]) for stage in stages}
    if "correctness" not in names:
        raise FlowV3RequestBuildError("operator-test plan has no correctness stage")
    for stage in stages:
        if stage["name"] == "performance-primary":
            stage["depends_on"] = ["correctness"]
        if stage["name"] == "correctness" and "performance-primary" in stage["depends_on"]:
            raise FlowV3RequestBuildError("correctness cannot depend on performance")


def enforce_host_handoff_dependencies(stages: list[dict[str, Any]]) -> None:
    by_name = {str(stage["name"]): stage for stage in stages}
    runtime_install = by_name.get("runtime-install")
    if runtime_install is None or "operator-build" not in by_name:
        return
    dependencies = list(runtime_install.get("depends_on", []))
    if "operator-build" not in dependencies:
        dependencies.append("operator-build")
    runtime_install["depends_on"] = dependencies


def validate_envelope_stages(
    stages: list[dict[str, Any]],
    *,
    operation_kind: str = "operator-test",
    profiler_mode: str = "",
) -> list[dict[str, Any]]:
    sample = {
        "meta": {
            "schema": "ascendop.flow.request.v3",
            "version": 3,
            "request_id": "stage-validation",
            "attempt_id": "attempt-001",
            "trace_id": "trace-stage-validation",
            "producer": "builder",
            "code_generation": "validation",
            "created_at": "2026-01-01T00:00:00+00:00",
        },
        "identity": {
            "endpoint_id": "validation",
            "endpoint_generation": "validation",
            "registration_generation": "validation",
            "idempotency_key": "validation",
        },
        "workflow": {
            "domain": "validation",
            "season": "validation",
            "operator": "ValidationOp",
            "test_version": "ValidationOp_V1_1",
            "case_version": "case_v001",
            "gate": "validation",
            "operation_kind": operation_kind,
        },
        "payload": {
            "digest": hashlib.sha256(b"validation").hexdigest(),
            "parts": [],
        },
        "execution": {
            "profile": FLOW_V3_EXECUTION_PROFILE,
            "budget_class": "diagnostic"
            if operation_kind == "diagnostic-profile"
            else "standard",
            "budget_policy_version": "ascendop.device-budget.v3",
            "requested_device_session_seconds": 210,
            "granted_device_session_seconds": 210,
            "performance_mode": (
                profiler_mode
                or (
                    "primary-all-cases"
                    if any(
                        stage["name"] == "performance-primary"
                        for stage in stages
                    )
                    else "none"
                )
            ),
            "correctness_required": operation_kind == "operator-test",
            "publish_eligible": operation_kind == "operator-test",
            "resources": {
                "host_cpu_weight": 1,
                "host_memory_mb": 1,
                "host_io_weight": 1,
                "device_count": 1,
            },
            "stages": stages,
        },
        "retry_policy": {
            "policy_id": RETRY_POLICY_VERSION,
            "max_execution_attempts": 2,
            "max_transport_retries": 3,
            "max_idempotent_stage_retries": 1,
        },
        "result_contract": {
            "ingest_adapter": FLOW_V3_RESULT_ADAPTER,
            "terminal_states": ["terminal-success"],
            "required_artifacts": [],
        },
    }
    validate_envelope(sample)
    return stages


def stage_resource_class(name: str, resource: str) -> str:
    if resource == "device":
        return "device"
    if resource == "export":
        return "export"
    if name == "operator-build-install":
        return "host-build-heavy"
    return "host-light"


def read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowV3RequestBuildError(f"internal stage plan is unreadable: {path}") from exc
    if not isinstance(raw, dict):
        raise FlowV3RequestBuildError(f"internal stage plan is not an object: {path}")
    return raw


def write_immutable_json(path: Path, value: Mapping[str, Any]) -> None:
    payload = json.dumps(
        dict(value),
        ensure_ascii=True,
        indent=2,
        sort_keys=True,
    ) + "\n"
    io_path = filesystem_path(path)
    if io_path.is_file():
        existing = io_path.read_text(encoding="utf-8-sig")
        if existing != payload:
            raise FlowV3RequestBuildError(f"immutable envelope collision: {path}")
        return
    io_path.parent.mkdir(parents=True, exist_ok=True)
    io_path.write_text(payload, encoding="utf-8")
