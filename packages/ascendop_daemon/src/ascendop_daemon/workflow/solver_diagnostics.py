from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.workflow import validate_solver_diagnostic_request

from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.core.models import DaemonConfig, operator_season
from ascendop_daemon.workflow.operator_job_builder import tree_digest
from ascendop_daemon.workflow.profiler_request_state import (
    ProfilerRequestStateError,
    create_typed_profiler_request,
    profiler_evidence_status,
    retry_typed_profiler_request,
)


REQUEST_FILE = "SOLVER_DIAGNOSTIC_REQUEST.json"
INDEX_FILE = "SOLVER_DIAGNOSTIC_INDEX.json"
STATE_PROTOCOL = "ascendop.solver-diagnostic-state.v1"
INDEX_PROTOCOL = "ascendop.solver-diagnostic-index.v1"


class SolverDiagnosticError(RuntimeError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def canonical_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def generation_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def case_directory(root: Path, operator: str, case_version: str) -> Path:
    return root / "TestUtils" / "casegen" / operator / "case" / case_version


def request_path(root: Path, operator: str, case_version: str) -> Path:
    return case_directory(root, operator, case_version) / REQUEST_FILE


def index_path(root: Path, operator: str, case_version: str) -> Path:
    return case_directory(root, operator, case_version) / INDEX_FILE


def state_path(
    root: Path,
    operator: str,
    case_version: str,
    blocker_generation: str,
) -> Path:
    return (
        root
        / "TestUtils"
        / "tester_daemon"
        / "solver_diagnostic_requests"
        / operator
        / case_version
        / generation_digest(blocker_generation)
        / "request.json"
    )


def revision_path(
    root: Path,
    operator: str,
    case_version: str,
    blocker_generation: str,
    request_digest: str,
) -> Path:
    return (
        state_path(root, operator, case_version, blocker_generation).parent
        / "revisions"
        / f"r-{request_digest[:16]}.json"
    )


def archived_submit_snapshot(root: Path, operator: str, test_version: str) -> Path:
    snapshot = (
        root
        / "operators_testresult"
        / operator
        / test_version
        / "submit_snapshot"
    )
    if not snapshot.is_dir():
        raise SolverDiagnosticError(
            f"immutable submit snapshot is missing: {operator}/{test_version}"
        )
    return snapshot


def load_request(
    root: Path,
    *,
    operator: str,
    case_version: str,
    result_version: str,
    blocker_generation: str,
) -> dict[str, Any]:
    path = request_path(root, operator, case_version)
    if not path.is_file():
        raise SolverDiagnosticError(f"Solver diagnostic request is missing: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        request = validate_solver_diagnostic_request(raw)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise SolverDiagnosticError(
            f"invalid Solver diagnostic request {path}: {exc}"
        ) from exc
    expected = (operator, case_version, result_version, blocker_generation)
    actual = (
        str(request.get("operator") or ""),
        str(request.get("case_version") or ""),
        str(request.get("result_version") or ""),
        str(request.get("blocker_generation") or ""),
    )
    if actual != expected:
        raise SolverDiagnosticError(
            f"Solver diagnostic scope mismatch: {actual} != {expected}"
        )
    target = request["target"]
    if str(target["test_version"]) != result_version:
        raise SolverDiagnosticError(
            "Solver diagnostic target must be the blocker result version"
        )
    snapshot = archived_submit_snapshot(root, operator, result_version)
    source = snapshot / "pending_snapshot" / "source_snapshot"
    observed_source = tree_digest(source)
    expected_source = str(target["source_sha256"]).lower()
    if observed_source.lower() != expected_source:
        raise SolverDiagnosticError(
            "Solver diagnostic source digest mismatch: "
            f"{observed_source} != {expected_source}"
        )
    return request


def observe_request(
    root: Path,
    *,
    operator: str,
    case_version: str,
    result_version: str,
    blocker_generation: str,
) -> dict[str, Any]:
    path = request_path(root, operator, case_version)
    if not path.is_file():
        return {"status": "missing", "request_path": relative_path(root, path)}
    try:
        request = load_request(
            root,
            operator=operator,
            case_version=case_version,
            result_version=result_version,
            blocker_generation=blocker_generation,
        )
    except SolverDiagnosticError as exc:
        return {
            "status": "invalid",
            "request_path": relative_path(root, path),
            "error": str(exc),
        }
    digest = canonical_digest(request)
    requested_artifacts = list(
        request.get("scope", {}).get("requested_artifacts", [])
    )
    state = read_object(
        state_path(root, operator, case_version, blocker_generation)
    )
    if state:
        if str(state.get("request_digest") or "") != digest:
            if _can_supersede_failed_correctness(state, request):
                return {
                    "status": "unregistered",
                    "request_path": relative_path(root, path),
                    "request_digest": digest,
                    "operation_kind": str(request["operation_kind"]),
                    "requested_artifacts": requested_artifacts,
                    "supersedes_request_digest": str(state["request_digest"]),
                    "revision": int(state.get("revision") or 1) + 1,
                }
            return {
                "status": "collision",
                "request_path": relative_path(root, path),
                "request_digest": digest,
                "error": "registered diagnostic state has a different request digest",
            }
        effective_state = state
        if str(request["operation_kind"]) == "diagnostic-profile":
            profiler_state = profiler_evidence_status(
                root,
                operator,
                case_version,
                blocker_generation,
            )
            if profiler_state:
                effective_state = profiler_state
        return {
            "status": str(effective_state.get("status") or "registered"),
            "request_path": relative_path(root, path),
            "request_digest": digest,
            "operation_kind": str(request["operation_kind"]),
            "requested_artifacts": requested_artifacts,
            "state_path": relative_path(
                root,
                state_path(root, operator, case_version, blocker_generation),
            ),
            "evidence_path": str(effective_state.get("evidence_path") or ""),
            "request_id": str(effective_state.get("request_id") or ""),
            "attempt_id": str(effective_state.get("attempt_id") or ""),
            "error": str(effective_state.get("last_error") or ""),
        }
    return {
        "status": "unregistered",
        "request_path": relative_path(root, path),
        "request_digest": digest,
        "operation_kind": str(request["operation_kind"]),
        "requested_artifacts": requested_artifacts,
    }


def register_request(
    root: Path,
    *,
    operator: str,
    case_version: str,
    result_version: str,
    blocker_generation: str,
) -> dict[str, Any]:
    request = load_request(
        root,
        operator=operator,
        case_version=case_version,
        result_version=result_version,
        blocker_generation=blocker_generation,
    )
    digest = canonical_digest(request)
    path = state_path(root, operator, case_version, blocker_generation)
    existing = read_object(path)
    revision = 1
    supersedes_request_digest = ""
    if existing:
        if str(existing.get("request_digest") or "") != digest:
            if not _can_supersede_failed_correctness(existing, request):
                raise SolverDiagnosticError(
                    f"Solver diagnostic request identity collision: {path}"
                )
            supersedes_request_digest = str(existing["request_digest"])
            revision = int(existing.get("revision") or 1) + 1
            _archive_superseded_state(
                root,
                operator=operator,
                case_version=case_version,
                blocker_generation=blocker_generation,
                state=existing,
            )
        else:
            sync_index(root, existing)
            return existing
    now = utc_now_iso()
    snapshot = archived_submit_snapshot(root, operator, result_version)
    state = {
        "protocol_version": STATE_PROTOCOL,
        "operator": operator,
        "case_version": case_version,
        "result_version": result_version,
        "blocker_generation": blocker_generation,
        "generation_digest": generation_digest(blocker_generation),
        "operation_kind": str(request["operation_kind"]),
        "request_digest": digest,
        "request_snapshot": dict(request),
        "revision": revision,
        "supersedes_request_digest": supersedes_request_digest,
        "request_path": relative_path(
            root, request_path(root, operator, case_version)
        ),
        "request_state_path": relative_path(root, path),
        "submit_snapshot": relative_path(root, snapshot),
        "target_source_sha256": str(request["target"]["source_sha256"]),
        "scope": dict(request["scope"]),
        "requested_device_session_seconds": int(
            request["requested_device_session_seconds"]
        ),
        "retry_policy": dict(request["retry_policy"]),
        "status": "ready",
        "request_id": "",
        "attempt_id": "",
        "evidence_path": "",
        "last_error": "",
        "created_at": now,
        "updated_at": now,
    }
    if state["operation_kind"] == "diagnostic-profile":
        # Compile the profiler-lane state first.  If this fails, no diagnostic
        # registration is published and a later idempotent registration can
        # retry without being trapped behind a half-written parent state.
        try:
            create_typed_profiler_request(
                root,
                diagnostic_request=request,
                diagnostic_state=state,
            )
        except ProfilerRequestStateError as exc:
            raise SolverDiagnosticError(str(exc)) from exc
    write_json_atomic(path, state, ensure_ascii=True, sort_keys=True)
    sync_index(root, state)
    return state


def _can_supersede_failed_correctness(
    state: Mapping[str, Any],
    request: Mapping[str, Any],
) -> bool:
    return (
        str(state.get("status") or "") == "failed"
        and str(state.get("operation_kind") or "")
        == "diagnostic-correctness-replay"
        and str(request.get("operation_kind") or "")
        == "diagnostic-correctness-replay"
    )


def _archive_superseded_state(
    root: Path,
    *,
    operator: str,
    case_version: str,
    blocker_generation: str,
    state: Mapping[str, Any],
) -> None:
    digest = str(state.get("request_digest") or "")
    if not digest:
        raise SolverDiagnosticError("superseded diagnostic state has no digest")
    path = revision_path(
        root,
        operator,
        case_version,
        blocker_generation,
        digest,
    )
    archived = dict(state)
    archived["archived_at"] = utc_now_iso()
    existing = read_object(path)
    if existing:
        comparable = dict(existing)
        comparable.pop("archived_at", None)
        original = dict(archived)
        original.pop("archived_at", None)
        if comparable != original:
            raise SolverDiagnosticError(
                f"diagnostic revision archive collision: {path}"
            )
        return
    write_json_atomic(path, archived, ensure_ascii=True, sort_keys=True)


def retry_profiler_request(
    root: Path,
    *,
    operator: str,
    case_version: str,
    result_version: str,
    blocker_generation: str,
    expected_request_attempt: int,
    retry_generation: str,
) -> dict[str, Any]:
    request = load_request(
        root,
        operator=operator,
        case_version=case_version,
        result_version=result_version,
        blocker_generation=blocker_generation,
    )
    if str(request.get("operation_kind") or "") != "diagnostic-profile":
        raise SolverDiagnosticError(
            "typed profiler retry requires a diagnostic-profile operation"
        )
    diagnostic_state = register_request(
        root,
        operator=operator,
        case_version=case_version,
        result_version=result_version,
        blocker_generation=blocker_generation,
    )
    try:
        return retry_typed_profiler_request(
            root,
            diagnostic_request=request,
            diagnostic_state=diagnostic_state,
            expected_request_attempt=expected_request_attempt,
            retry_generation=retry_generation,
        )
    except ProfilerRequestStateError as exc:
        raise SolverDiagnosticError(str(exc)) from exc


def discover_ready_correctness_request(
    root: Path,
    config: DaemonConfig,
) -> dict[str, Any] | None:
    base = root / "TestUtils" / "tester_daemon" / "solver_diagnostic_requests"
    for path in sorted(base.glob("*/*/*/request.json")):
        state = read_object(path)
        if (
            str(state.get("protocol_version") or "") != STATE_PROTOCOL
            or str(state.get("operation_kind") or "")
            != "diagnostic-correctness-replay"
            or str(state.get("status") or "") != "ready"
        ):
            continue
        operator = str(state.get("operator") or "")
        if operator not in config.operators:
            continue
        result_version = str(state["result_version"])
        snapshot = root / str(state["submit_snapshot"])
        hardware = str(config.policy.get("test_engine_hardware") or "910B4")
        remote_root = str(
            config.policy.get("test_engine_remote_root")
            or config.remote_root
        )
        command = (
            f"python scripts\\next_workflow.py gitpartner-run-submit "
            f"{operator} {result_version} --season {operator_season(config, operator)} "
            f"--mode correct --vendor {result_version.lower()}_diag "
            f"--hardware {hardware} --case-version {state['case_version']} "
            f"--remote-root {remote_root}"
        )
        return {
            "root": root.resolve(),
            "state_path": path,
            "state": state,
            "candidate": {
                "op": operator,
                "test_version": result_version,
                "command": command,
                "attempt_index": 1,
                "job_id_suffix": (
                    f"diag-{state['generation_digest']}-a01"
                ),
                "submit_root_override": str(snapshot),
            },
            "diagnostic": {
                "protocol_version": "ascendop.solver-diagnostic-plan.v1",
                "request_digest": str(state["request_digest"]),
                "request_state_path": str(state["request_state_path"]),
                "blocker_generation": str(state["blocker_generation"]),
                "case_version": str(state["case_version"]),
                "result_version": result_version,
                "requested_artifacts": list(
                    state.get("scope", {}).get("requested_artifacts", [])
                ),
            },
        }
    return None


def mark_enqueued(
    request: dict[str, Any],
    *,
    request_id: str,
    attempt_id: str,
) -> dict[str, Any]:
    state = request["state"]
    state.update(
        {
            "status": "collecting",
            "request_id": request_id,
            "attempt_id": attempt_id,
            "updated_at": utc_now_iso(),
        }
    )
    path = Path(str(request["state_path"]))
    write_json_atomic(path, state, ensure_ascii=True, sort_keys=True)
    sync_index(Path(str(request["root"])), state)
    return {
        "operator": str(state["operator"]),
        "result_version": str(state["result_version"]),
        "request_id": request_id,
        "attempt_id": attempt_id,
        "blocker_generation": str(state["blocker_generation"]),
    }


def record_result(
    root: Path,
    *,
    request_state_relative: str,
    request_id: str,
    attempt_id: str,
    materialized_root: Path,
    terminal_state: str,
) -> dict[str, Any]:
    state_file = (root / request_state_relative).resolve()
    allowed = (
        root / "TestUtils" / "tester_daemon" / "solver_diagnostic_requests"
    ).resolve()
    if state_file != allowed and allowed not in state_file.parents:
        raise SolverDiagnosticError(
            f"diagnostic state escapes daemon root: {state_file}"
        )
    state = read_object(state_file)
    if not state:
        raise SolverDiagnosticError(f"diagnostic state is missing: {state_file}")
    expected = (
        str(state.get("request_id") or ""),
        str(state.get("attempt_id") or ""),
    )
    if expected != (request_id, attempt_id):
        raise SolverDiagnosticError(
            f"diagnostic result identity mismatch: {(request_id, attempt_id)} != {expected}"
        )
    evidence_key = canonical_digest(
        {
            "operator": str(state["operator"]),
            "case_version": str(state["case_version"]),
            "generation_digest": str(state["generation_digest"]),
            "request_id": request_id,
            "attempt_id": attempt_id,
        }
    )
    # Request and attempt IDs are deliberately descriptive. Repeating both in
    # the directory hierarchy exceeds the legacy Windows MAX_PATH limit for
    # long operator names, so the durable index stores their readable identity
    # while the filesystem uses a collision-checked content key.
    evidence = (
        root
        / "TestUtils"
        / "tester_daemon"
        / "diagnostic_evidence"
        / "by-id"
        / evidence_key[:2]
        / evidence_key[:32]
    )
    materialized_digest = tree_digest(materialized_root)
    artifacts_complete, missing_requested_artifacts = (
        _diagnostic_collection_complete(state, materialized_root)
    )
    target_terminal = terminal_state in {
        "terminal-success",
        "terminal-business-failure",
    }
    collection_complete = target_terminal or artifacts_complete
    bundle = _materialize_evidence_blob(
        root,
        materialized_root,
        materialized_digest,
    )
    summary_path = evidence / "DIAGNOSTIC_EVIDENCE.json"
    existing_summary = read_object(summary_path)
    if existing_summary:
        expected_summary = (
            request_id,
            attempt_id,
            terminal_state,
            materialized_digest,
        )
        observed_summary = (
            str(existing_summary.get("request_id") or ""),
            str(existing_summary.get("attempt_id") or ""),
            str(existing_summary.get("terminal_state") or ""),
            str(existing_summary.get("materialized_bundle_sha256") or ""),
        )
        if observed_summary != expected_summary:
            raise SolverDiagnosticError(
                "diagnostic evidence identity collision: "
                f"{observed_summary} != {expected_summary}"
            )
        summary = {
            **existing_summary,
            "collection_status": (
                "complete" if collection_complete else "failed"
            ),
            "missing_requested_artifacts": missing_requested_artifacts,
            "requested_artifacts": list(
                state.get("scope", {}).get("requested_artifacts", [])
            ),
        }
        if summary != existing_summary:
            write_json_atomic(
                summary_path,
                summary,
                ensure_ascii=True,
                sort_keys=True,
            )
    else:
        evidence.mkdir(parents=True, exist_ok=True)
        summary = {
            "schema": "ascendop.solver-diagnostic-evidence.v1",
            "operator": str(state["operator"]),
            "case_version": str(state["case_version"]),
            "result_version": str(state["result_version"]),
            "blocker_generation": str(state["blocker_generation"]),
            "request_digest": str(state["request_digest"]),
            "request_id": request_id,
            "attempt_id": attempt_id,
            "terminal_state": terminal_state,
            "collection_status": (
                "complete" if collection_complete else "failed"
            ),
            "missing_requested_artifacts": missing_requested_artifacts,
            "requested_artifacts": list(
                state.get("scope", {}).get("requested_artifacts", [])
            ),
            "evidence_key": evidence_key,
            "evidence_root": relative_path(root, evidence),
            "materialized_bundle": relative_path(root, bundle),
            "materialized_bundle_sha256": materialized_digest,
            "recorded_at": utc_now_iso(),
        }
        write_json_atomic(
            summary_path,
            summary,
            ensure_ascii=True,
            sort_keys=True,
        )
    state.update(
        {
            "status": (
                "complete"
                if collection_complete
                else "failed"
            ),
            "collection_status": (
                "complete" if collection_complete else "failed"
            ),
            "missing_requested_artifacts": missing_requested_artifacts,
            "target_terminal_state": terminal_state,
            "evidence_path": relative_path(
                root, evidence / "DIAGNOSTIC_EVIDENCE.json"
            ),
            "last_error": (
                ""
                if collection_complete
                else (
                    f"diagnostic terminal state: {terminal_state}; "
                    "missing requested artifacts: "
                    + ", ".join(missing_requested_artifacts)
                )
            ),
            "updated_at": utc_now_iso(),
        }
    )
    write_json_atomic(state_file, state, ensure_ascii=True, sort_keys=True)
    sync_index(root, state)
    return summary


def _diagnostic_collection_complete(
    state: Mapping[str, Any], materialized_root: Path
) -> tuple[bool, list[str]]:
    """Judge the diagnostic collection separately from the target verdict."""

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
            _requested_artifact_present(root, artifact)
            for root in artifact_roots
        )
    ]
    return not missing, missing


def _requested_artifact_present(root: Path, artifact: str) -> bool:
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
        return (
            _path_has_evidence(root / "result/FAILURE_CASE_LOG.txt")
            or _runtime_trace_has_stack(root / "result/RUNTIME_BOUNDARY_TRACE.json")
        )
    if artifact == "runtime-boundary-trace":
        return (
            _runtime_trace_has_boundary(root / "result/RUNTIME_BOUNDARY_TRACE.json")
            and _path_has_evidence(root / "result/runtime_boundary")
        )
    if artifact == "native-workspace-query-attribution":
        return _native_workspace_attribution_has_attempt(
            root / "result/NATIVE_WORKSPACE_QUERY_ATTRIBUTION.json"
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
        return any(item.is_file() and item.stat().st_size > 0 for item in path.rglob("*"))
    return False


def _runtime_trace_has_stack(path: Path) -> bool:
    trace = read_object(path)
    return any(
        str(sample.get("stack") or "").strip()
        for execution in trace.get("executions", [])
        if isinstance(execution, Mapping)
        for sample in execution.get("stack_samples", [])
        if isinstance(sample, Mapping)
    )


def _runtime_trace_has_boundary(path: Path) -> bool:
    trace = read_object(path)
    return any(
        execution.get("boundaries") or execution.get("stack_samples")
        for execution in trace.get("executions", [])
        if isinstance(execution, Mapping)
    )


def _native_workspace_attribution_has_attempt(path: Path) -> bool:
    attribution = read_object(path)
    return any(
        execution.get("attempts")
        for execution in attribution.get("executions", [])
        if isinstance(execution, Mapping)
    )


def _kernel_fault_attribution_is_captured(path: Path) -> bool:
    attribution = read_object(path)
    return (
        str(attribution.get("status") or "") in {"captured", "no-fault"}
        and str(attribution.get("tool", {}).get("status") or "") == "available"
    )


def _materialize_evidence_blob(
    root: Path,
    source: Path,
    digest: str,
) -> Path:
    blob_root = root / ".ascendop-work" / "blobs" / "sd"
    legacy_blob = blob_root / digest / "b"
    blob = blob_root / digest[:2] / digest[:20] / "b"
    for existing in (legacy_blob, blob):
        if not existing.exists():
            continue
        if tree_digest(existing) != digest:
            raise SolverDiagnosticError(
                f"diagnostic evidence blob collision: {existing}"
            )
        return existing

    blob.parent.mkdir(parents=True, exist_ok=True)
    blob_root.mkdir(parents=True, exist_ok=True)
    staging = blob_root / f".s-{uuid.uuid4().hex[:8]}"
    try:
        shutil.copytree(source, staging)
        if tree_digest(staging) != digest:
            raise SolverDiagnosticError(
                f"diagnostic evidence blob digest mismatch: {staging}"
            )
        os.replace(staging, blob)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return blob


def sync_index(root: Path, state: Mapping[str, Any]) -> None:
    path = index_path(root, str(state["operator"]), str(state["case_version"]))
    index = {
        "protocol_version": INDEX_PROTOCOL,
        "operator": str(state["operator"]),
        "case_version": str(state["case_version"]),
        "result_version": str(state["result_version"]),
        "blocker_generation": str(state["blocker_generation"]),
        "operation_kind": str(state["operation_kind"]),
        "request_digest": str(state["request_digest"]),
        "revision": int(state.get("revision") or 1),
        "supersedes_request_digest": str(
            state.get("supersedes_request_digest") or ""
        ),
        "status": str(state["status"]),
        "request_id": str(state.get("request_id") or ""),
        "attempt_id": str(state.get("attempt_id") or ""),
        "evidence_path": str(state.get("evidence_path") or ""),
        "last_error": str(state.get("last_error") or ""),
        "updated_at": str(state["updated_at"]),
    }
    write_json_atomic(path, index, ensure_ascii=True, sort_keys=True)


def read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def relative_path(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


__all__ = [
    "INDEX_FILE",
    "REQUEST_FILE",
    "SolverDiagnosticError",
    "discover_ready_correctness_request",
    "mark_enqueued",
    "observe_request",
    "record_result",
    "register_request",
]
