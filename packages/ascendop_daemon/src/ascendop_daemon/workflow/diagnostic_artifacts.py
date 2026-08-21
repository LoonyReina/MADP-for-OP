from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def diagnostic_collection_complete(
    state: Mapping[str, Any], materialized_root: Path
) -> tuple[bool, list[str]]:
    """Judge evidence completeness separately from the target verdict."""

    artifact_roots = (materialized_root, materialized_root / "result_bundle")
    requested = [
        str(value)
        for value in state.get("scope", {}).get("requested_artifacts", [])
        if str(value)
    ]
    missing = [
        artifact
        for artifact in requested
        if not any(
            requested_artifact_present(root, artifact) for root in artifact_roots
        )
    ]
    return not missing, missing


def requested_artifact_present(root: Path, artifact: str) -> bool:
    paths = {
        "engine-identity": ("result/ENGINE_IDENTITY.json",),
        "runtime-readiness": ("result/RUNTIME_READINESS.json",),
        "runtime-compatibility": ("result/RUNTIME_COMPATIBILITY.json",),
        "operator-install-precheck": ("result/OPERATOR_INSTALL_PRECHECK.txt",),
        "phase-timeline": ("result/PHASE_TIMELINE.jsonl",),
        "correctness-batch": ("result/CORRECTNESS_BATCH.json",),
        "case-logs": ("result/case_logs", "logs"),
    }
    if artifact in paths:
        return any(_path_has_evidence(root / path) for path in paths[artifact])
    if artifact == "first-failure-traceback":
        return _path_has_evidence(
            root / "result/FAILURE_CASE_LOG.txt"
        ) or _runtime_trace_has_stack(root / "result/RUNTIME_BOUNDARY_TRACE.json")
    if artifact == "runtime-boundary-trace":
        return _runtime_trace_has_boundary(
            root / "result/RUNTIME_BOUNDARY_TRACE.json"
        ) and _path_has_evidence(root / "result/runtime_boundary")
    if artifact == "native-workspace-query-attribution":
        return _native_workspace_attribution_has_attempt(
            root / "result/NATIVE_WORKSPACE_QUERY_ATTRIBUTION.json"
        ) or _runtime_boundary_has_native_workspace_attempt(
            root / "result/runtime_boundary"
        )
    if artifact == "host-callback-attribution":
        return _host_callback_attribution_is_complete(
            root / "result/HOST_CALLBACK_ATTRIBUTION.json"
        )
    if artifact == "kernel-fault-attribution":
        return _kernel_fault_attribution_is_captured(
            root / "result/KERNEL_FAULT_ATTRIBUTION.json"
        ) and _path_has_evidence(root / "result/kernel_fault")
    return False


def _path_has_evidence(path: Path) -> bool:
    if path.is_file():
        return path.stat().st_size > 0
    if path.is_dir():
        return any(
            item.is_file() and item.stat().st_size > 0 for item in path.rglob("*")
        )
    return False


def _runtime_trace_has_stack(path: Path) -> bool:
    trace = _read_object(path)
    return any(
        str(sample.get("stack") or "").strip()
        for execution in trace.get("executions", [])
        if isinstance(execution, Mapping)
        for sample in execution.get("stack_samples", [])
        if isinstance(sample, Mapping)
    )


def _runtime_trace_has_boundary(path: Path) -> bool:
    trace = _read_object(path)
    return any(
        execution.get("boundaries") or execution.get("stack_samples")
        for execution in trace.get("executions", [])
        if isinstance(execution, Mapping)
    )


def _native_workspace_attribution_has_attempt(path: Path) -> bool:
    attribution = _read_object(path)
    return any(
        execution.get("attempts")
        for execution in attribution.get("executions", [])
        if isinstance(execution, Mapping)
    )


def _runtime_boundary_has_native_workspace_attempt(path: Path) -> bool:
    if not path.is_dir():
        return False
    for events_path in path.rglob("*.jsonl"):
        try:
            lines = events_path.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()
        except OSError:
            continue
        for raw_line in lines:
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, Mapping):
                continue
            if str(event.get("schema") or "") != (
                "ascendop.native-workspace-query-event.v1"
            ):
                continue
            if str(event.get("phase") or "").startswith("workspace-query-native-"):
                return True
    return False


def _host_callback_attribution_is_complete(path: Path) -> bool:
    attribution = _read_object(path)
    attestation = attribution.get("overlay_attestation", {})
    return (
        str(attribution.get("capture_status") or "")
        in {"captured", "callback-not-observed"}
        and isinstance(attestation, Mapping)
        and str(attestation.get("status") or "") == "implemented"
        and bool(attestation.get("patched"))
    )


def _kernel_fault_attribution_is_captured(path: Path) -> bool:
    attribution = _read_object(path)
    return (
        str(attribution.get("status") or "") in {"captured", "no-fault"}
        and str(attribution.get("tool", {}).get("status") or "") == "available"
    )


def _read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


__all__ = ["diagnostic_collection_complete", "requested_artifact_present"]
