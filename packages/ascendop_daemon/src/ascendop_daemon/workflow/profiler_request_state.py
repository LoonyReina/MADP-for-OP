from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.core.filesystem import filesystem_path
from ascendop_daemon.workflow.operator_job_builder import tree_digest
from ascendop_daemon.workflow.profiler_failure_evidence import (
    terminal_failure_evidence,
)
from ascendop_daemon.workflow.profiler_request_index import (
    PROFILER_INDEX_FILE,
    PROFILER_INDEX_PROTOCOL,
    ProfilerRequestStateError,
    archived_submit_snapshot,
    available_case_ids,
    case_directory,
    observe_profiler_request,
    profiler_state_path,
    sync_profiler_index,
)


PROFILER_REQUEST_PROTOCOL = "ascendop-profiler-request-v1"
PROFILER_EVIDENCE_FILE = "PROFILER_EVIDENCE.json"
PROFILER_EXECUTION_CONTRACT_REVISION = "comparable-measurement-v9"
PROFILER_LAUNCH_COUNT_CAP = 5000
PROFILER_EFFECTIVE_PROFILE_ROUNDS = 1

PROFILER_MEASUREMENT_COMPONENTS = {
    "daemon.diagnostic_request_materializer": (
        "daemon",
        "ascendop_daemon/control_plane/test_requests.py",
    ),
    "daemon.diagnostic_intake": (
        "daemon",
        "ascendop_daemon/runtime/diagnostic_intake.py",
    ),
    "daemon.operator_job_builder": (
        "daemon",
        "ascendop_daemon/workflow/operator_job_builder.py",
    ),
    "daemon.profiler_evidence_runner": (
        "daemon",
        "ascendop_daemon/workflow/profiler_evidence_runner.py",
    ),
    "daemon.profiler_row_attribution": (
        "daemon",
        "ascendop_daemon/workflow/profiler_row_attribution.py",
    ),
    "engine.batch_case_runner": (
        "engine",
        "limited_remote_partner/engine/batch_case_runner.py",
    ),
    "engine.perf_pipeline": (
        "engine",
        "limited_remote_partner/engine/stages/perf_pipeline.py",
    ),
    "engine.profile_call_plan": (
        "engine",
        "limited_remote_partner/engine/stages/profile_call_plan.py",
    ),
    "engine.profile_session_runner": (
        "engine",
        "limited_remote_partner/engine/stages/profile_session_runner.py",
    ),
    "engine.test_engine_worker": (
        "engine",
        "limited_remote_partner/engine/test_engine_worker.py",
    ),
}


def profiler_execution_contract(
    root: Path,
    *,
    operator: str,
    profiler_mode: str,
    measurement_repetitions: int = 1,
    requested_device_session_seconds: int = 0,
    comparison_affinity: str = "",
) -> dict[str, Any]:
    active = _read_object(root / ".ascendop-work" / "runtime" / "active-release.json")
    configured = str(active.get("system_registry_path") or "").strip()
    registry_path = Path(configured) if configured else root / "Develop" / "registry" / "system_registry.json"
    if not registry_path.is_absolute():
        registry_path = root / registry_path
    registry = _read_object(registry_path)
    defaults = registry.get("operator_defaults", {})
    overrides = registry.get("operator_overrides", {})
    default_definition = defaults if isinstance(defaults, dict) else {}
    operator_definition = (
        overrides.get(operator, {}) if isinstance(overrides, dict) else {}
    )
    if not isinstance(operator_definition, dict):
        operator_definition = {}
    kernel_name = str(
        operator_definition.get("profiler_kernel_name")
        or operator_definition.get("runtime_operator_name")
        or default_definition.get("profiler_kernel_name")
        or default_definition.get("runtime_operator_name")
        or operator
    ).strip()
    _validate_token(kernel_name, "profiler kernel name")
    kernel_selection = str(
        operator_definition.get("profiler_kernel_selection")
        or default_definition.get("profiler_kernel_selection")
        or "exact"
    ).strip()
    if kernel_selection not in {"exact", "prefix-postfilter"}:
        raise ProfilerRequestStateError(
            f"unsupported profiler kernel selection: {kernel_selection}"
        )
    measurement_components = _measurement_component_manifest(root, active)
    material = {
        "revision": PROFILER_EXECUTION_CONTRACT_REVISION,
        "operator": operator,
        "profiler_mode": profiler_mode,
        "profiler_kernel_name": kernel_name,
        "profiler_kernel_selection": kernel_selection,
        "launch_count_cap": PROFILER_LAUNCH_COUNT_CAP,
        "effective_profile_rounds": PROFILER_EFFECTIVE_PROFILE_ROUNDS,
        "measurement_repetitions": measurement_repetitions,
        "requested_device_session_seconds": int(requested_device_session_seconds),
        "comparison_affinity": comparison_affinity,
        "profile_call_plan_protocol": "engine-profile-call-plan-v2",
        "measurement_components": measurement_components,
    }
    encoded = json.dumps(
        material,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        **material,
        "digest": hashlib.sha256(encoded).hexdigest(),
    }


def _measurement_component_manifest(
    root: Path,
    active_release: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_roots = {
        "daemon": _component_source_root(
            root,
            active_release,
            active_field="daemon_source",
            repository_path=Path("tools/tester_daemon/src"),
            package_marker=Path("ascendop_daemon/workflow/profiler_request_state.py"),
        ),
        "engine": _component_source_root(
            root,
            active_release,
            active_field="transport_source",
            repository_path=Path("GitPartner/src"),
            package_marker=Path("limited_remote_partner/engine/test_engine_worker.py"),
        ),
    }
    manifest: dict[str, dict[str, Any]] = {}
    for name, (source_kind, relative_path) in sorted(
        PROFILER_MEASUREMENT_COMPONENTS.items()
    ):
        path = source_roots[source_kind] / Path(relative_path)
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise ProfilerRequestStateError(
                f"profiler measurement component is unavailable: {name}: {path}"
            ) from exc
        manifest[name] = {
            "path": relative_path,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
        }
    return manifest


def _component_source_root(
    root: Path,
    active_release: dict[str, Any],
    *,
    active_field: str,
    repository_path: Path,
    package_marker: Path,
) -> Path:
    configured = str(active_release.get(active_field) or "").strip()
    if configured:
        configured_path = Path(configured)
        if not configured_path.is_absolute():
            configured_path = root / configured_path
        if (configured_path / package_marker).is_file():
            return configured_path.resolve()
        raise ProfilerRequestStateError(
            f"active release {active_field} is incomplete: {configured_path}"
        )

    candidates = [root.resolve()]
    candidates.extend(Path(__file__).resolve().parents)
    for candidate in candidates:
        source_root = candidate / repository_path
        if (source_root / package_marker).is_file():
            return source_root.resolve()
    raise ProfilerRequestStateError(
        f"cannot resolve profiler component source root for {active_field}"
    )


def profiler_evidence_status(
    root: Path,
    operator: str,
    case_version: str,
    blocker_generation: str,
) -> dict[str, Any]:
    path = case_directory(root, operator, case_version) / PROFILER_INDEX_FILE
    index = _read_object(path)
    if (
        str(index.get("protocol_version") or "") != PROFILER_INDEX_PROTOCOL
        or str(index.get("operator") or "") != operator
        or str(index.get("case_version") or "") != case_version
        or str(index.get("blocker_generation") or "") != blocker_generation
    ):
        return {}
    return index


def create_typed_profiler_request(
    root: Path,
    *,
    diagnostic_request: dict[str, Any],
    diagnostic_state: dict[str, Any],
) -> dict[str, Any]:
    """Compile a validated Solver diagnostic into the V4 profiler lane."""

    operator = str(diagnostic_state["operator"])
    case_version = str(diagnostic_state["case_version"])
    result_version = str(diagnostic_state["result_version"])
    blocker_generation = str(diagnostic_state["blocker_generation"])
    for value, field in (
        (operator, "operator"),
        (case_version, "case version"),
        (result_version, "result version"),
    ):
        _validate_token(value, field)
    profiler_mode = str(diagnostic_request["scope"]["profiler_mode"])
    if profiler_mode not in {
        "primary-all-cases",
        "primary-roofline-all-cases",
    }:
        raise ProfilerRequestStateError(
            f"unsupported typed profiler mode: {profiler_mode}"
        )
    state_file = profiler_state_path(
        root,
        operator,
        case_version,
        blocker_generation,
    )
    request_sha256 = str(diagnostic_state["request_digest"])
    existing = _read_object(state_file)
    if existing:
        if (
            str(existing.get("blocker_generation") or "")
            != blocker_generation
            or str(existing.get("request_sha256") or "") != request_sha256
        ):
            raise ProfilerRequestStateError(
                f"typed profiler request identity collision: {state_file}"
            )
        sync_profiler_index(root, existing)
        return existing

    target_specs = [
        dict(diagnostic_request["target"]),
        *[
            dict(value)
            for value in diagnostic_request.get("comparison_targets", [])
            if isinstance(value, dict)
        ],
    ]
    if str(target_specs[0].get("test_version") or "") != result_version:
        raise ProfilerRequestStateError(
            "typed profiler primary target must match blocker result version"
        )
    targets: list[dict[str, Any]] = []
    cases: list[int] = []
    primary_snapshot: Path | None = None
    for target_spec in target_specs:
        target_version = str(target_spec.get("test_version") or "")
        _validate_token(target_version, "profiler target version")
        snapshot = archived_submit_snapshot(root, operator, target_version)
        source_root = snapshot / "pending_snapshot" / "source_snapshot"
        source_sha256 = tree_digest(source_root)
        if source_sha256 != str(target_spec.get("source_sha256") or ""):
            raise ProfilerRequestStateError(
                "typed profiler target source changed before registration: "
                f"{target_version}"
            )
        target_cases = available_case_ids(
            root, operator, case_version, target_version
        )
        if not cases:
            cases = target_cases
            primary_snapshot = snapshot
        elif target_cases != cases:
            raise ProfilerRequestStateError(
                "typed profiler comparison targets must use the identical case set"
            )
        targets.append(
            {
                "test_version": target_version,
                "submit_snapshot": _relative_path(snapshot, root),
                "source_sha256": source_sha256,
                "status": "planned",
                "attempt": 0,
                "engine_job_id": "",
                "evidence_path": "",
                "last_error": "",
            }
        )
    if not cases:
        raise ProfilerRequestStateError(
            "typed profiler request has no executable cases: "
            f"{operator}/{case_version}"
        )
    scope = dict(diagnostic_request["scope"])
    measurement_repetitions = int(scope.get("measurement_repetitions", 1) or 1)
    comparison_affinity = str(scope.get("comparison_affinity") or "")
    expected_block_dims = {
        str(case_id): int(block_dim)
        for case_id, block_dim in dict(scope.get("expected_block_dims", {})).items()
    }
    now = _utc_now_iso()
    execution_contract = profiler_execution_contract(
        root,
        operator=operator,
        profiler_mode=profiler_mode,
        measurement_repetitions=measurement_repetitions,
        requested_device_session_seconds=int(
            diagnostic_request["requested_device_session_seconds"]
        ),
        comparison_affinity=comparison_affinity,
    )
    state = {
        "protocol_version": PROFILER_REQUEST_PROTOCOL,
        "operator": operator,
        "case_version": case_version,
        "blocker_result_version": result_version,
        "blocker_generation": blocker_generation,
        "generation_digest": _generation_digest(blocker_generation),
        "blocker_path": _relative_path(
            case_directory(root, operator, case_version) / "SOLVER_BLOCKER.md",
            root,
        ),
        "evidence_request_path": str(diagnostic_state["request_path"]),
        "request_sha256": request_sha256,
        "request_attempt": 1,
        "status": "ready",
        "season": str(diagnostic_request["campaign"]),
        "hardware": "",
        "remote_root": "",
        "cases": cases,
        "max_cases": len(cases),
        "primary_metrics": "PipeUtilization,Occupancy,KernelScale,BasicInfo",
        "roofline_case_count": (
            len(cases)
            if profiler_mode == "primary-roofline-all-cases"
            else 0
        ),
        "requested_profiler_mode": profiler_mode,
        "measurement_repetitions": measurement_repetitions,
        "requested_device_session_seconds": int(
            diagnostic_request["requested_device_session_seconds"]
        ),
        "comparison_affinity": comparison_affinity,
        "comparison_route": {},
        "expected_block_dims": expected_block_dims,
        "case_shapes": _case_shapes(primary_snapshot, cases),
        "case_specs_sha256": _case_specs_sha256(primary_snapshot),
        "profiler_execution_contract": execution_contract,
        "profiler_execution_contract_digest": execution_contract["digest"],
        "typed_solver_diagnostic": True,
        "targets": targets,
        "created_at": now,
        "updated_at": now,
        "request_state_path": _relative_path(state_file, root),
    }
    write_json_atomic(state_file, state, ensure_ascii=True, sort_keys=True)
    sync_profiler_index(root, state)
    return state


def retry_typed_profiler_request(
    root: Path,
    *,
    diagnostic_request: dict[str, Any],
    diagnostic_state: dict[str, Any],
    expected_request_attempt: int,
    retry_generation: str,
) -> dict[str, Any]:
    """Reset one failed typed profiler request with compare-and-swap semantics."""

    if str(diagnostic_request.get("operation_kind") or "") != "diagnostic-profile":
        raise ProfilerRequestStateError(
            "typed profiler retry requires a diagnostic-profile request"
        )
    operator = str(diagnostic_state.get("operator") or "")
    case_version = str(diagnostic_state.get("case_version") or "")
    blocker_generation = str(diagnostic_state.get("blocker_generation") or "")
    request_sha256 = str(diagnostic_state.get("request_digest") or "")
    state_file = profiler_state_path(
        root,
        operator,
        case_version,
        blocker_generation,
    )
    state = _read_object(state_file)
    if not state:
        raise ProfilerRequestStateError(
            f"typed profiler request is missing: {state_file}"
        )
    if (
        str(state.get("protocol_version") or "") != PROFILER_REQUEST_PROTOCOL
        or str(state.get("blocker_generation") or "") != blocker_generation
        or str(state.get("request_sha256") or "") != request_sha256
    ):
        raise ProfilerRequestStateError(
            f"typed profiler retry identity collision: {state_file}"
        )
    current_attempt = int(state.get("request_attempt", 1) or 1)
    requested_attempt = int(expected_request_attempt)
    current_status = str(state.get("status") or "").lower()
    retry_generation = str(retry_generation or "").strip()
    _validate_token(retry_generation, "retry release generation")
    requested_device_session_seconds = int(
        diagnostic_request["requested_device_session_seconds"]
    )
    current_contract = profiler_execution_contract(
        root,
        operator=operator,
        profiler_mode=str(state.get("requested_profiler_mode") or "primary-all-cases"),
        measurement_repetitions=int(
            state.get("measurement_repetitions", 1) or 1
        ),
        requested_device_session_seconds=requested_device_session_seconds,
        comparison_affinity=str(state.get("comparison_affinity") or ""),
    )
    prior_contract_digest = str(
        state.get("profiler_execution_contract_digest") or ""
    )

    # A repeated action for the attempt that already won the CAS is idempotent
    # while that attempt remains active or has completed successfully.
    if requested_attempt == current_attempt and current_status in {
        "ready",
        "collecting",
        "complete",
    }:
        sync_profiler_index(root, state)
        return state
    next_attempt = current_attempt + 1
    if requested_attempt != next_attempt:
        raise ProfilerRequestStateError(
            "typed profiler retry attempt changed before reset: "
            f"expected={next_attempt} requested={requested_attempt}"
        )
    if current_status not in {"failed", "partial"}:
        raise ProfilerRequestStateError(
            "typed profiler retry requires failed or partial state: "
            f"status={current_status or 'missing'}"
        )
    if str(state.get("retry_release_generation") or "") == retry_generation:
        raise ProfilerRequestStateError(
            "typed profiler retry generation already consumed: "
            f"{retry_generation}"
        )
    if prior_contract_digest == str(current_contract["digest"]):
        raise ProfilerRequestStateError(
            "typed profiler retry execution contract is unchanged: "
            f"{prior_contract_digest}"
        )

    targets = [
        dict(target)
        for target in state.get("targets", [])
        if isinstance(target, dict)
    ]
    reset_count = 0
    for target in targets:
        if str(target.get("status") or "").lower() not in {"failed", "partial"}:
            continue
        target["status"] = "planned"
        for field in (
            "completed_at",
            "engine_job_id",
            "engine_state",
            "evidence_path",
            "evidence_status",
            "flow_v3_attempt_id",
            "flow_v3_request_id",
            "last_error",
            "reported_evidence_status",
        ):
            target.pop(field, None)
        reset_count += 1
    if reset_count == 0:
        raise ProfilerRequestStateError(
            "typed profiler retry has no failed or partial target to reset"
        )

    history = [
        dict(item)
        for item in state.get("attempt_history", [])
        if isinstance(item, dict)
    ]
    history.append(
        {
            "request_attempt": current_attempt,
            "status": current_status,
            "targets": deepcopy(state.get("targets", [])),
            "terminal_updated_at": str(state.get("updated_at") or ""),
            "archived_at": _utc_now_iso(),
        }
    )
    state.update(
        {
            "request_attempt": requested_attempt,
            "status": "ready",
            "targets": targets,
            "attempt_history": history,
            "last_error": "",
            "retry_release_generation": retry_generation,
            "requested_device_session_seconds": requested_device_session_seconds,
            "profiler_execution_contract": current_contract,
            "profiler_execution_contract_digest": current_contract["digest"],
            "retry_authorized_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }
    )
    write_json_atomic(state_file, state, ensure_ascii=True, sort_keys=True)
    sync_profiler_index(root, state)
    return state


def reconcile_unpublished_profiler_budget(
    root: Path,
    state_path: Path,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Backfill a verified budget only while the active attempt is unpublished."""

    requested = int(state.get("requested_device_session_seconds", 0) or 0)
    if requested > 0:
        return state

    targets = [
        dict(target)
        for target in state.get("targets", [])
        if isinstance(target, dict)
    ]
    if (
        str(state.get("status") or "") != "ready"
        or not targets
        or any(str(target.get("status") or "") != "planned" for target in targets)
        or any(
            str(target.get("flow_v3_request_id") or "")
            or str(target.get("flow_v3_attempt_id") or "")
            for target in targets
        )
    ):
        raise ProfilerRequestStateError(
            "profiler budget migration is allowed only before publication"
        )

    evidence_relative = str(state.get("evidence_request_path") or "")
    evidence_path = (root / evidence_relative).resolve()
    try:
        evidence_path.relative_to(root.resolve())
    except ValueError as exc:
        raise ProfilerRequestStateError(
            f"profiler diagnostic request escapes workspace: {evidence_relative}"
        ) from exc
    diagnostic_request = _read_object(evidence_path)
    observed_digest = _canonical_digest(diagnostic_request)
    if observed_digest != str(state.get("request_sha256") or ""):
        raise ProfilerRequestStateError(
            "profiler diagnostic request changed before budget migration"
        )
    expected_identity = (
        str(state.get("operator") or ""),
        str(state.get("case_version") or ""),
        str(state.get("blocker_result_version") or ""),
        str(state.get("blocker_generation") or ""),
    )
    observed_identity = (
        str(diagnostic_request.get("operator") or ""),
        str(diagnostic_request.get("case_version") or ""),
        str(diagnostic_request.get("result_version") or ""),
        str(diagnostic_request.get("blocker_generation") or ""),
    )
    if observed_identity != expected_identity:
        raise ProfilerRequestStateError(
            "profiler diagnostic identity changed before budget migration"
        )
    requested = int(
        diagnostic_request.get("requested_device_session_seconds", 0) or 0
    )
    if requested <= 0:
        raise ProfilerRequestStateError(
            "profiler diagnostic request has no positive device-session budget"
        )
    repaired_contract = profiler_execution_contract(
        root,
        operator=expected_identity[0],
        profiler_mode=str(
            state.get("requested_profiler_mode") or "primary-all-cases"
        ),
        measurement_repetitions=int(
            state.get("measurement_repetitions", 1) or 1
        ),
        requested_device_session_seconds=requested,
        comparison_affinity=str(state.get("comparison_affinity") or ""),
    )
    repaired = dict(state)
    repaired.update(
        {
            "requested_device_session_seconds": requested,
            "profiler_execution_contract": repaired_contract,
            "profiler_execution_contract_digest": repaired_contract["digest"],
            "updated_at": _utc_now_iso(),
        }
    )
    write_json_atomic(state_path, repaired, ensure_ascii=True, sort_keys=True)
    sync_profiler_index(root, repaired)
    return repaired


def record_typed_profiler_result(
    root: Path,
    *,
    request_state_relative: str,
    operator: str,
    case_version: str,
    blocker_generation: str,
    target_version: str,
    engine_job_id: str,
    evidence_root: Path,
    terminal: dict[str, Any] | None = None,
    failure_artifact_root: Path | None = None,
) -> dict[str, Any]:
    """Validate and archive one V4 typed profiler return."""

    state_path = (root / request_state_relative).resolve()
    allowed = (
        root / "TestUtils" / "tester_daemon" / "profiler_requests"
    ).resolve()
    if state_path != allowed and allowed not in state_path.parents:
        raise ProfilerRequestStateError(
            f"profiler request state escapes daemon root: {state_path}"
        )
    state = _read_object(state_path)
    if not state:
        raise ProfilerRequestStateError(
            f"typed profiler request state is missing: {state_path}"
        )
    if (
        str(state.get("protocol_version") or "") != PROFILER_REQUEST_PROTOCOL
        or state.get("typed_solver_diagnostic") is not True
    ):
        raise ProfilerRequestStateError(
            "profiler result ingest accepts only V4 typed Solver diagnostics"
        )
    expected_state_identity = (
        operator,
        case_version,
        blocker_generation,
    )
    actual_state_identity = (
        str(state.get("operator") or ""),
        str(state.get("case_version") or ""),
        str(state.get("blocker_generation") or ""),
    )
    if actual_state_identity != expected_state_identity:
        raise ProfilerRequestStateError(
            "typed profiler state identity mismatch: "
            f"{actual_state_identity} != {expected_state_identity}"
        )

    target = next(
        (
            item
            for item in _existing_targets(state)
            if str(item.get("test_version") or "") == target_version
        ),
        None,
    )
    if target is None:
        raise ProfilerRequestStateError(
            f"typed profiler target is not registered: {target_version}"
        )
    if str(target.get("engine_job_id") or "") != engine_job_id:
        raise ProfilerRequestStateError(
            "typed profiler engine job id mismatch: "
            f"{target.get('engine_job_id')} != {engine_job_id}"
        )

    evidence = _read_object(evidence_root / PROFILER_EVIDENCE_FILE)
    failure_evidence = False
    if not evidence:
        terminal = terminal if isinstance(terminal, dict) else {}
        if str(terminal.get("state") or "").lower() != "failed":
            raise ProfilerRequestStateError(
                f"returned profiler evidence is missing: {evidence_root}"
            )
        evidence = terminal_failure_evidence(
            terminal,
            operator=operator,
            case_version=case_version,
            blocker_generation=blocker_generation,
            target_version=target_version,
            engine_job_id=engine_job_id,
        )
        failure_evidence = True
    if str(evidence.get("protocol_version") or "") != (
        "ascendop-profiler-evidence-v1"
    ):
        raise ProfilerRequestStateError(
            "returned profiler evidence has an unsupported protocol"
        )
    expected_evidence_identity = (
        operator,
        case_version,
        blocker_generation,
        target_version,
    )
    actual_evidence_identity = (
        str(evidence.get("operator") or ""),
        str(evidence.get("case_version") or ""),
        str(evidence.get("blocker_generation") or ""),
        str(evidence.get("target_version") or ""),
    )
    if actual_evidence_identity != expected_evidence_identity:
        raise ProfilerRequestStateError(
            "typed profiler evidence identity mismatch: "
            f"{actual_evidence_identity} != {expected_evidence_identity}"
        )

    reported_status = str(evidence.get("status") or "failed").lower()
    validation_error = _complete_evidence_validation_error(evidence)
    status = (
        "failed"
        if reported_status == "complete" and validation_error
        else reported_status
    )
    mapped = status if status in {
        "complete",
        "unsupported",
        "failed",
        "partial",
    } else "failed"
    destination = (
        case_directory(root, operator, case_version)
        / "profiler_evidence"
        / str(state["generation_digest"])
        / target_version
        / f"attempt-{int(target.get('attempt', 1) or 1):03d}"
    )
    destination_io = filesystem_path(destination)
    existing_relative = str(target.get("evidence_path") or "")
    if (
        str(target.get("status") or "") == mapped
        and existing_relative == _relative_path(destination, root)
        and destination_io.is_dir()
    ):
        archived = _read_object(destination / PROFILER_EVIDENCE_FILE)
        if (
            str(archived.get("operator") or ""),
            str(archived.get("case_version") or ""),
            str(archived.get("blocker_generation") or ""),
            str(archived.get("target_version") or ""),
        ) != expected_evidence_identity:
            raise ProfilerRequestStateError(
                "typed profiler archived evidence identity collision"
            )
        return _profiler_result_summary(root, state_path, state, target)

    staging = destination.with_name(
        destination.name
        + ".staging-"
        + hashlib.sha256(engine_job_id.encode("utf-8")).hexdigest()[:12]
    )
    staging_io = filesystem_path(staging)
    if staging_io.exists():
        shutil.rmtree(staging_io)
    filesystem_path(destination.parent).mkdir(parents=True, exist_ok=True)
    if failure_evidence:
        staging_io.mkdir(parents=True)
        artifact_root = (
            filesystem_path(failure_artifact_root)
            if failure_artifact_root is not None
            else filesystem_path(evidence_root)
        )
        if artifact_root.is_dir():
            shutil.copytree(artifact_root, staging_io / "engine_return")
        write_json_atomic(
            staging_io / PROFILER_EVIDENCE_FILE,
            evidence,
            ensure_ascii=True,
            sort_keys=True,
        )
    else:
        shutil.copytree(filesystem_path(evidence_root), staging_io)
    if destination_io.exists():
        shutil.rmtree(destination_io)
    os.replace(staging_io, destination_io)

    target.update(
        {
            "status": mapped,
            "evidence_path": _relative_path(destination, root),
            "evidence_status": status,
            "reported_evidence_status": reported_status,
            "completed_at": _utc_now_iso(),
            "last_error": validation_error or str(evidence.get("error") or ""),
        }
    )
    _recompute_request_status(state)
    state["updated_at"] = _utc_now_iso()
    write_json_atomic(state_path, state, ensure_ascii=True, sort_keys=True)
    sync_profiler_index(root, state)
    return _profiler_result_summary(root, state_path, state, target)


def _existing_targets(state: dict[str, Any]) -> list[dict[str, Any]]:
    raw = state.get("targets", [])
    return (
        [item for item in raw if isinstance(item, dict)]
        if isinstance(raw, list)
        else []
    )


def _recompute_request_status(state: dict[str, Any]) -> str:
    statuses = [
        str(item.get("status") or "") for item in _existing_targets(state)
    ]
    if statuses and all(status == "complete" for status in statuses):
        status = "complete"
    elif any(status == "enqueued" for status in statuses):
        status = "collecting"
    elif statuses and all(status == "unsupported" for status in statuses):
        status = "unsupported"
    elif any(
        status in {"failed", "partial", "unsupported"}
        for status in statuses
    ):
        status = "failed"
    elif any(status == "planned" for status in statuses):
        status = "collecting"
    else:
        status = "ready"
    state["status"] = status
    return status


def _complete_evidence_validation_error(evidence: dict[str, Any]) -> str:
    if str(evidence.get("status") or "").lower() != "complete":
        return ""
    runs = evidence.get("runs")
    if not isinstance(runs, list) or not runs:
        return "profiler evidence reported complete without any profiler runs"
    repetitions = int(evidence.get("measurement_repetitions", 1) or 1)
    primary_runs = [
        value
        for value in runs
        if isinstance(value, dict)
        and str(value.get("metric_label") or "") == "primary"
    ]
    roofline_runs = [
        value
        for value in runs
        if isinstance(value, dict)
        and str(value.get("metric_label") or "") == "roofline"
    ]
    profiler_mode = str(evidence.get("profiler_mode") or "")
    if len(primary_runs) != repetitions:
        return "profiler evidence primary repetition count is incomplete"
    if (
        profiler_mode == "deep-dual"
        and len(roofline_runs) != repetitions
    ):
        return "profiler evidence roofline repetition count is incomplete"
    for index, raw_run in enumerate(runs, start=1):
        if not isinstance(raw_run, dict):
            return f"profiler run {index} is not a structured record"
        metric = str(raw_run.get("metric_label") or "unknown")
        matched = raw_run.get("matched_operator_rows")
        if matched is None:
            csv_rows = raw_run.get("csv_evidence")
            matched = (
                sum(
                    int(row.get("matched_operator_rows", 0) or 0)
                    for row in csv_rows
                    if isinstance(row, dict)
                )
                if isinstance(csv_rows, list)
                else 0
            )
        if not bool(raw_run.get("success")) or int(matched or 0) <= 0:
            case_id = raw_run.get("case_id", index)
            return (
                "profiler evidence reported complete but "
                f"case {case_id} {metric} did not capture a target operator row"
            )
        zero_only_pipe_rows = raw_run.get("zero_only_pipe_rows")
        if zero_only_pipe_rows is None:
            zero_only_pipe_rows = bool(
                raw_run.get("zero_only_primary_pipe_rows")
                or raw_run.get("zero_only_roofline_pipe_rows")
            )
        if bool(zero_only_pipe_rows):
            if metric == "primary" and profiler_mode == "deep-dual":
                continue
            return f"profiler evidence {metric} pipe rows are zero-only"
    return ""


def _profiler_result_summary(
    root: Path,
    state_path: Path,
    state: dict[str, Any],
    target: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": str(state.get("status") or ""),
        "target_status": str(target.get("status") or ""),
        "evidence_path": str(target.get("evidence_path") or ""),
        "request_state_path": _relative_path(state_path, root),
    }


def _case_shapes(snapshot: Path | None, cases: list[int]) -> dict[str, Any]:
    if snapshot is None:
        return {}
    path = snapshot / "task_case" / "case_specs.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, list):
        return {}
    shapes: dict[str, Any] = {}
    allowed = set(cases)
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            continue
        match = re.fullmatch(r"case([1-9][0-9]*)", str(item.get("generated_case") or ""))
        case_id = int(match.group(1)) if match else index
        if case_id not in allowed:
            continue
        inputs = item.get("inputs")
        first_input = (
            inputs[0]
            if isinstance(inputs, list) and inputs and isinstance(inputs[0], dict)
            else {}
        )
        shape = first_input.get("shape", item.get("shape", []))
        if not isinstance(shape, list) or not all(isinstance(value, int) for value in shape):
            continue
        shapes[str(case_id)] = list(shape)
    return shapes


def _case_specs_sha256(snapshot: Path | None) -> str:
    if snapshot is None:
        return ""
    path = snapshot / "task_case" / "case_specs.json"
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(
            filesystem_path(path).read_text(encoding="utf-8-sig")
        )
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _canonical_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _generation_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _relative_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _validate_token(value: str, field: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ProfilerRequestStateError(f"unsafe {field}: {value!r}")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


__all__ = [
    "ProfilerRequestStateError",
    "create_typed_profiler_request",
    "observe_profiler_request",
    "profiler_evidence_status",
    "reconcile_unpublished_profiler_budget",
    "record_typed_profiler_result",
    "retry_typed_profiler_request",
]
