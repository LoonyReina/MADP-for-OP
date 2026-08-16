from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.control_plane.flow_v3_policy import RETRY_POLICY_VERSION
from ascendop_daemon.core.filesystem import filesystem_path
from ascendop_protocol.wire_v3 import validate_envelope


FLOW_V3_EXECUTION_PROFILE = "correctness-first-all-cases-v3"
FLOW_V3_RESULT_ADAPTER = "cannjudge-result-v3"


class FlowV3RequestBuildError(RuntimeError):
    pass


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
    granted_device_session_seconds: int,
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
            if operation_kind.startswith("diagnostic-")
            else "standard",
            "budget_policy_version": "ascendop.device-budget.v3",
            "requested_device_session_seconds": granted_device_session_seconds,
            "granted_device_session_seconds": granted_device_session_seconds,
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
            "correctness_required": operation_kind
            in {"operator-test", "diagnostic-correctness-replay"},
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
            "max_execution_attempts": (
                1 if operation_kind.startswith("diagnostic-") else 2
            ),
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
