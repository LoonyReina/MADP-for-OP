from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.legacy.engine_job_builder import (
    PROFILER_EVIDENCE_PROFILE,
    EngineJobBuildError,
    build_compatibility_job,
    tree_digest,
)
from ascendop_daemon.legacy.engine_pump import EnginePump
from ascendop_daemon.core.models import DaemonConfig, operator_season


PROFILER_REQUEST_PROTOCOL = "ascendop-profiler-request-v1"
PROFILER_INDEX_PROTOCOL = "ascendop-profiler-evidence-index-v1"
PROFILER_EVIDENCE_FILE = "PROFILER_EVIDENCE.json"
PROFILER_SUMMARY_FILE = "PROFILER_SUMMARY.md"
PROFILER_CAPABILITY_FILE = "PROFILER_CAPABILITY.json"
PROFILER_INDEX_FILE = "PROFILER_EVIDENCE_INDEX.json"
PROFILER_REQUEST_FILE = "request.json"
PROFILER_JOB_KIND = "profiler-evidence"
MISSING_ENGINE_ENTRY_GRACE_SECONDS = 30

ENGINE_TERMINAL_STATES = {
    "workflow-archived",
    "superseded-by-workflow-result",
    "canary-complete",
    "standby-cancelled",
    "cancelled",
    "return-lost",
    "superseded-by-logical-attempt",
}
ENGINE_ACTIVE_STATES = {
    "pending",
    "admitting",
    "staging-standby",
    "standby",
    "accepted",
    "running",
    "return-ready",
    "returned",
    "returned-awaiting-ingest",
}


class ProfilerEvidenceError(RuntimeError):
    pass


def generation_digest(generation: str) -> str:
    return hashlib.sha256(generation.encode("utf-8")).hexdigest()[:16]


def case_dir(root: Path, op: str, case_version: str) -> Path:
    return root / "TestUtils" / "casegen" / op / "case" / case_version


def request_state_path(
    root: Path,
    op: str,
    case_version: str,
    blocker_generation: str,
) -> Path:
    return (
        root
        / "TestUtils"
        / "tester_daemon"
        / "profiler_requests"
        / op
        / case_version
        / generation_digest(blocker_generation)
        / PROFILER_REQUEST_FILE
    )


def evidence_index_path(root: Path, op: str, case_version: str) -> Path:
    return case_dir(root, op, case_version) / PROFILER_INDEX_FILE


def profiler_evidence_status(
    root: Path,
    op: str,
    case_version: str,
    blocker_generation: str,
) -> dict[str, Any]:
    index = read_object(evidence_index_path(root, op, case_version))
    if (
        not index
        or str(index.get("protocol_version") or "") != PROFILER_INDEX_PROTOCOL
        or str(index.get("operator") or "") != op
        or str(index.get("case_version") or "") != case_version
        or str(index.get("blocker_generation") or "") != blocker_generation
    ):
        return {}
    return index


def create_profiler_request(
    root: Path,
    *,
    op: str,
    case_version: str,
    result_version: str,
    blocker_generation: str,
    season: str,
    hardware: str,
    remote_root: str,
    max_cases: int = 4,
    primary_metrics: str = "PipeUtilization,Occupancy,KernelScale,BasicInfo",
    roofline_case_count: int = 1,
    retry: bool = False,
    expected_request_attempt: int | None = None,
) -> dict[str, Any]:
    validate_token(op, "operator")
    validate_token(case_version, "case version")
    validate_token(result_version, "result version")
    max_cases = max(1, min(int(max_cases), 16))
    roofline_case_count = max(0, min(int(roofline_case_count), max_cases))
    active_case_dir = case_dir(root, op, case_version)
    blocker_path = active_case_dir / "SOLVER_BLOCKER.md"
    request_path = active_case_dir / "PROFILER_EVIDENCE_REQUEST.md"
    inactive_request_paths = [
        active_case_dir / "PROFILER_EVIDENCE_REQUEST_BLOCKED.md",
        active_case_dir / "PROFILER_EVIDENCE_REQUEST_UNAVAILABLE.md",
    ]
    inactive_request_path = next(
        (path for path in inactive_request_paths if path.is_file()),
        inactive_request_paths[0],
    )
    reactivating_blocked_request = (
        retry
        and not request_path.is_file()
        and inactive_request_path.is_file()
    )
    request_source_path = (
        inactive_request_path if reactivating_blocked_request else request_path
    )
    if not blocker_path.is_file() or not request_source_path.is_file():
        raise ProfilerEvidenceError(
            f"profiler request requires SOLVER_BLOCKER.md and "
            f"PROFILER_EVIDENCE_REQUEST.md: {op}/{case_version}"
        )
    expected_generation = (
        f"{case_version}|{result_version}|{blocker_path.stat().st_mtime_ns}"
    )
    if blocker_generation != expected_generation:
        raise ProfilerEvidenceError(
            "profiler blocker generation changed before request creation: "
            f"expected={expected_generation} requested={blocker_generation}"
        )
    request_text = request_source_path.read_text(
        encoding="utf-8-sig", errors="replace"
    )
    request_sha256 = hashlib.sha256(request_text.encode("utf-8")).hexdigest()
    targets = infer_profiler_target_versions(
        root,
        op=op,
        request_text=request_text,
        fallback_result=result_version,
    )
    cases = infer_profiler_cases(
        root,
        op=op,
        case_version=case_version,
        request_text=request_text,
        target_version=targets[0],
        max_cases=max_cases,
    )
    state_path = request_state_path(
        root, op, case_version, blocker_generation
    )
    existing = read_object(state_path)
    if existing:
        if (
            str(existing.get("blocker_generation") or "") != blocker_generation
            or str(existing.get("request_sha256") or "") != request_sha256
        ):
            raise ProfilerEvidenceError(
                f"profiler request identity collision: {state_path}"
            )
        retryable_statuses = {"failed", "unsupported"}
        if reactivating_blocked_request:
            retryable_statuses.update({"complete", "partial", "collecting", "ready"})
        if not retry or str(existing.get("status") or "") not in retryable_statuses:
            sync_profiler_index(root, existing)
            return existing
        next_attempt = int(existing.get("request_attempt", 1) or 1) + 1
        if (
            expected_request_attempt is not None
            and int(expected_request_attempt) != next_attempt
        ):
            raise ProfilerEvidenceError(
                "profiler retry attempt changed before request reset: "
                f"expected={next_attempt} requested={expected_request_attempt}"
            )
        existing["request_attempt"] = next_attempt
        existing.update(
            {
                "cases": cases,
                "max_cases": max_cases,
                "primary_metrics": primary_metrics,
                "roofline_case_count": roofline_case_count,
            }
        )
        for target in existing_targets(existing):
            if reactivating_blocked_request or str(target.get("status") or "") in {
                "failed",
                "unsupported",
            }:
                target.update(
                    {
                        "status": "planned",
                        "engine_job_id": "",
                        "last_error": "",
                        "evidence_path": "",
                    }
                )
        if reactivating_blocked_request:
            inactive_request_path.replace(request_path)
            existing["evidence_request_path"] = relative_path(request_path, root)
            existing["recovered_blocked_request_at"] = utc_now_iso()
        existing["status"] = "ready"
        existing["updated_at"] = utc_now_iso()
        write_json_atomic(state_path, existing, ensure_ascii=True, sort_keys=True)
        sync_profiler_index(root, existing)
        return existing

    target_rows: list[dict[str, Any]] = []
    for target_version in targets:
        snapshot = archived_submit_snapshot(root, op, target_version)
        source_root = snapshot / "pending_snapshot" / "source_snapshot"
        target_rows.append(
            {
                "test_version": target_version,
                "submit_snapshot": relative_path(snapshot, root),
                "source_sha256": tree_digest(source_root),
                "status": "planned",
                "attempt": 0,
                "engine_job_id": "",
                "evidence_path": "",
                "last_error": "",
            }
        )
    now = utc_now_iso()
    if reactivating_blocked_request:
        inactive_request_path.replace(request_path)
    state = {
        "protocol_version": PROFILER_REQUEST_PROTOCOL,
        "operator": op,
        "case_version": case_version,
        "blocker_result_version": result_version,
        "blocker_generation": blocker_generation,
        "generation_digest": generation_digest(blocker_generation),
        "blocker_path": relative_path(blocker_path, root),
        "evidence_request_path": relative_path(request_path, root),
        "request_sha256": request_sha256,
        "request_attempt": 1,
        "status": "ready",
        "season": season,
        "hardware": hardware,
        "remote_root": remote_root,
        "cases": cases,
        "max_cases": max_cases,
        "primary_metrics": primary_metrics,
        "roofline_case_count": roofline_case_count,
        "targets": target_rows,
        "created_at": now,
        "updated_at": now,
        "request_state_path": relative_path(state_path, root),
    }
    if reactivating_blocked_request:
        state["recovered_blocked_request_at"] = now
    if expected_request_attempt not in {None, 1}:
        raise ProfilerEvidenceError(
            "initial profiler request must use expected request attempt 1"
        )
    write_json_atomic(state_path, state, ensure_ascii=True, sort_keys=True)
    sync_profiler_index(root, state)
    return state


def enqueue_next_profiler_job(
    root: Path,
    config: DaemonConfig,
    pump: EnginePump,
    pump_state: dict[str, Any],
) -> dict[str, Any]:
    requests = discover_request_states(root)
    entries = pump_state.get("entries", {})
    if not isinstance(entries, dict):
        entries = {}
    for state_path, state in requests:
        changed = refresh_request_from_pump(state, entries)
        changed = revalidate_completed_targets(root, state) or changed
        if changed:
            write_json_atomic(
                state_path,
                state,
                ensure_ascii=True,
                sort_keys=True,
            )
            sync_profiler_index(root, state)
    active = [
        record
        for record in entries.values()
        if isinstance(record, dict)
        and str(record.get("job_kind") or "") == PROFILER_JOB_KIND
        and str(record.get("state") or "") in ENGINE_ACTIVE_STATES
    ]
    if active:
        return {
            "outcome": "profiler-job-active",
            "engine_job_id": str(active[0].get("engine_job_id") or ""),
        }
    ordinary_active = [
        record
        for record in entries.values()
        if isinstance(record, dict)
        and str(record.get("job_kind") or "") != PROFILER_JOB_KIND
        and str(record.get("state") or "") in ENGINE_ACTIVE_STATES
    ]
    if ordinary_active:
        return {
            "outcome": "ordinary-engine-work-active",
            "engine_job_id": str(
                ordinary_active[0].get("engine_job_id") or ""
            ),
            "operator": str(ordinary_active[0].get("operator") or ""),
        }

    for state_path, state in requests:
        if str(state.get("status") or "") in {"complete", "unsupported"}:
            write_json_atomic(state_path, state, ensure_ascii=True, sort_keys=True)
            sync_profiler_index(root, state)
            continue
        target = next_planned_target(state)
        if target is None:
            recompute_request_status(state)
            write_json_atomic(state_path, state, ensure_ascii=True, sort_keys=True)
            sync_profiler_index(root, state)
            continue
        op = str(state["operator"])
        if op not in config.operators:
            continue
        target_version = str(target["test_version"])
        snapshot = root / str(target["submit_snapshot"])
        attempt = int(target.get("attempt", 0) or 0) + 1
        cases = [int(value) for value in state.get("cases", [])]
        policy_max_cases = max(
            1,
            int(config.policy.get("test_engine_profiler_max_cases", 4) or 4),
        )
        normalized_cases = evenly_sample(cases, policy_max_cases)
        policy_roofline_count = max(
            0,
            int(
                config.policy.get(
                    "test_engine_profiler_roofline_case_count", 1
                )
                or 0
            ),
        )
        normalized_roofline_count = min(
            int(state.get("roofline_case_count", 0) or 0),
            policy_roofline_count,
            len(normalized_cases),
        )
        requested_mode = str(
            config.policy.get(
                "test_engine_profiler_collection_mode",
                "fast-single",
            )
            or "fast-single"
        )
        profiler_mode = {
            "fast-single": "fast-single",
            "deep-dual": "deep-dual",
            "batched-primary-only": "fast-single",
            "batched-primary-roofline": "deep-dual",
        }.get(requested_mode)
        if profiler_mode is None:
            raise ProfilerEvidenceError(
                f"unsupported profiler mode: {requested_mode}"
            )
        normalized_roofline_count = (
            len(normalized_cases)
            if profiler_mode == "deep-dual"
            else 0
        )
        if (
            normalized_cases != cases
            or int(state.get("max_cases", len(cases)) or len(cases))
            != policy_max_cases
            or int(state.get("roofline_case_count", 0) or 0)
            != normalized_roofline_count
        ):
            state.update(
                {
                    "cases": normalized_cases,
                    "max_cases": policy_max_cases,
                    "roofline_case_count": normalized_roofline_count,
                    "policy_normalized_at": utc_now_iso(),
                }
            )
            write_json_atomic(
                state_path,
                state,
                ensure_ascii=True,
                sort_keys=True,
            )
            sync_profiler_index(root, state)
        cases = normalized_cases
        roofline_cases = cases[
            : normalized_roofline_count
        ]
        profiler_plan = {
            "protocol_version": "ascendop-profiler-plan-v2",
            "request_attempt": int(state.get("request_attempt", 1) or 1),
            "operator": op,
            "case_version": str(state["case_version"]),
            "blocker_result_version": str(state["blocker_result_version"]),
            "blocker_generation": str(state["blocker_generation"]),
            "request_sha256": str(state["request_sha256"]),
            "request_state_path": str(state["request_state_path"]),
            "target_version": target_version,
            "target_source_sha256": str(target["source_sha256"]),
            "cases": cases,
            "profiler_mode": profiler_mode,
            "collection_mode": (
                requested_mode
                if requested_mode.startswith("batched-primary-")
                else profiler_mode
            ),
            "primary_metrics": str(state["primary_metrics"]),
            "roofline_cases": (
                cases if profiler_mode == "deep-dual" else []
            ),
            "warmup_runs": 1,
            "profile_timeout_seconds": int(
                config.policy.get("test_engine_profiler_timeout_seconds", 90)
                or 90
            ),
            "stage_priority": int(
                config.policy.get("test_engine_profiler_stage_priority", 200) or 200
            ),
        }
        remote_root = str(
            config.policy.get("test_engine_remote_root")
            or state.get("remote_root")
            or config.remote_root
        )
        hardware = str(
            config.policy.get("test_engine_hardware")
            or state.get("hardware")
            or "910B4"
        )
        season = operator_season(config, op)
        vendor = re.sub(r"[^a-z0-9_]+", "_", target_version.lower()).strip("_")
        command = (
            f"python scripts\\next_workflow.py gitpartner-run-submit "
            f"{op} {target_version} --season {season} --mode both "
            f"--vendor {vendor}_profiler --hardware {hardware} "
            f"--case-version {state['case_version']} --remote-root {remote_root}"
        )
        suffix = (
            f"prof-{state['generation_digest']}-"
            f"a{int(state.get('request_attempt', 1) or 1):02d}-t{attempt:02d}"
        )
        spec_path, payload_root = build_compatibility_job(
            root,
            command,
            remote_root=remote_root,
            submit_root_override=snapshot,
            execution_profile=PROFILER_EVIDENCE_PROFILE,
            job_id_suffix=suffix,
            attempt_index=attempt,
            workflow_ingest=True,
            profiler_plan=profiler_plan,
        )
        entry = pump.enqueue(spec_path, payload_root)
        target.update(
            {
                "status": "enqueued",
                "attempt": attempt,
                "engine_job_id": str(entry.get("engine_job_id") or ""),
                "enqueued_at": utc_now_iso(),
                "last_error": "",
            }
        )
        state["status"] = "collecting"
        state["updated_at"] = utc_now_iso()
        write_json_atomic(state_path, state, ensure_ascii=True, sort_keys=True)
        sync_profiler_index(root, state)
        return {
            "outcome": "enqueued",
            "operator": op,
            "case_version": state["case_version"],
            "target_version": target_version,
            "engine_job_id": target["engine_job_id"],
        }
    return {"outcome": "no-profiler-request"}


def next_flow_v3_profiler_request(
    root: Path,
    config: DaemonConfig,
) -> dict[str, Any] | None:
    """Compile one legacy request record into the fixed all-case V3 contract."""
    requested_mode = str(
        config.policy.get(
            "test_engine_profiler_collection_mode",
            "primary-all-cases",
        )
        or "primary-all-cases"
    )
    profiler_mode = {
        "fast-single": "primary-all-cases",
        "batched-primary-only": "primary-all-cases",
        "primary-all-cases": "primary-all-cases",
        "deep-dual": "primary-roofline-all-cases",
        "batched-primary-roofline": "primary-roofline-all-cases",
        "primary-roofline-all-cases": "primary-roofline-all-cases",
    }.get(requested_mode)
    if profiler_mode is None:
        raise ProfilerEvidenceError(
            f"unsupported Flow V3 profiler mode: {requested_mode}"
        )
    for state_path, state in discover_request_states(root):
        if revalidate_completed_targets(root, state):
            write_json_atomic(
                state_path,
                state,
                ensure_ascii=True,
                sort_keys=True,
            )
            sync_profiler_index(root, state)
        if str(state.get("status") or "") in {"complete", "unsupported"}:
            continue
        target = next_planned_target(state)
        if target is None:
            continue
        op = str(state["operator"])
        if op not in config.operators:
            continue
        target_version = str(target["test_version"])
        snapshot = root / str(target["submit_snapshot"])
        attempt = int(target.get("attempt", 0) or 0) + 1
        cases = available_case_ids(
            root,
            op,
            str(state["case_version"]),
            target_version,
        )
        if not cases:
            raise ProfilerEvidenceError(
                f"Flow V3 profiler has no cases: {op}/{target_version}"
            )
        profiler_plan = {
            "protocol_version": "ascendop-profiler-plan-v3",
            "request_attempt": int(state.get("request_attempt", 1) or 1),
            "operator": op,
            "case_version": str(state["case_version"]),
            "blocker_result_version": str(state["blocker_result_version"]),
            "blocker_generation": str(state["blocker_generation"]),
            "request_sha256": str(state["request_sha256"]),
            "request_state_path": str(state["request_state_path"]),
            "target_version": target_version,
            "target_source_sha256": str(target["source_sha256"]),
            "cases": cases,
            "profiler_mode": (
                "fast-single"
                if profiler_mode == "primary-all-cases"
                else "deep-dual"
            ),
            "collection_mode": profiler_mode,
            "primary_metrics": str(state["primary_metrics"]),
            "roofline_cases": (
                cases
                if profiler_mode == "primary-roofline-all-cases"
                else []
            ),
            "warmup_runs": 1,
            "profile_timeout_seconds": 90,
            "stage_priority": int(
                config.policy.get("test_engine_profiler_stage_priority", 200)
                or 200
            ),
        }
        remote_root = str(
            config.policy.get("test_engine_remote_root")
            or state.get("remote_root")
            or config.remote_root
        )
        hardware = str(
            config.policy.get("test_engine_hardware")
            or state.get("hardware")
            or "910B4"
        )
        season = operator_season(config, op)
        vendor = re.sub(
            r"[^a-z0-9_]+", "_", target_version.lower()
        ).strip("_")
        command = (
            f"python scripts\\next_workflow.py gitpartner-run-submit "
            f"{op} {target_version} --season {season} --mode both "
            f"--vendor {vendor}_profiler --hardware {hardware} "
            f"--case-version {state['case_version']} "
            f"--remote-root {remote_root}"
        )
        suffix = (
            f"prof-{state['generation_digest']}-"
            f"a{int(state.get('request_attempt', 1) or 1):02d}-"
            f"t{attempt:02d}"
        )
        return {
            "root": root.resolve(),
            "state_path": state_path,
            "state": state,
            "target": target,
            "profiler_mode": profiler_mode,
            "profiler_plan": profiler_plan,
            "candidate": {
                "op": op,
                "test_version": target_version,
                "command": command,
                "attempt_index": attempt,
                "job_id_suffix": suffix,
                "submit_root_override": str(snapshot),
            },
        }
    return None


def mark_flow_v3_profiler_enqueued(
    request: dict[str, Any],
    *,
    request_id: str,
    attempt_id: str,
) -> dict[str, Any]:
    state_path = Path(str(request["state_path"]))
    state = request["state"]
    target = request["target"]
    engine_job_id = f"{request_id}-{attempt_id}"
    target.update(
        {
            "status": "enqueued",
            "attempt": int(
                request["candidate"].get("attempt_index", 1) or 1
            ),
            "engine_job_id": engine_job_id,
            "flow_v3_request_id": request_id,
            "flow_v3_attempt_id": attempt_id,
            "enqueued_at": utc_now_iso(),
            "last_error": "",
        }
    )
    state.update(
        {
            "status": "collecting",
            "cases": list(request["profiler_plan"]["cases"]),
            "max_cases": len(request["profiler_plan"]["cases"]),
            "roofline_case_count": len(
                request["profiler_plan"]["roofline_cases"]
            ),
            "collection_mode": request["profiler_mode"],
            "policy_normalized_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
    )
    write_json_atomic(
        state_path,
        state,
        ensure_ascii=True,
        sort_keys=True,
    )
    sync_profiler_index(Path(str(request["root"])), state)
    return {
        "request_id": request_id,
        "attempt_id": attempt_id,
        "engine_job_id": engine_job_id,
        "operator": str(state["operator"]),
        "target_version": str(target["test_version"]),
        "case_count": len(request["profiler_plan"]["cases"]),
        "profiler_mode": request["profiler_mode"],
    }


def record_profiler_result(
    root: Path,
    *,
    request_state_relative: str,
    target_version: str,
    engine_job_id: str,
    evidence_root: Path,
) -> dict[str, Any]:
    state_path = (root / request_state_relative).resolve()
    allowed = (root / "TestUtils" / "tester_daemon" / "profiler_requests").resolve()
    if state_path != allowed and allowed not in state_path.parents:
        raise ProfilerEvidenceError(
            f"profiler request state escapes daemon root: {state_path}"
        )
    state = read_object(state_path)
    if not state:
        raise ProfilerEvidenceError(f"profiler request state missing: {state_path}")
    evidence = read_object(evidence_root / PROFILER_EVIDENCE_FILE)
    if not evidence:
        raise ProfilerEvidenceError(
            f"returned profiler evidence is missing: {evidence_root}"
        )
    expected = (
        str(state.get("operator") or ""),
        str(state.get("case_version") or ""),
        str(state.get("blocker_generation") or ""),
        target_version,
    )
    actual = (
        str(evidence.get("operator") or ""),
        str(evidence.get("case_version") or ""),
        str(evidence.get("blocker_generation") or ""),
        str(evidence.get("target_version") or ""),
    )
    if actual != expected:
        raise ProfilerEvidenceError(
            f"profiler evidence identity mismatch: {actual} != {expected}"
        )
    target = next(
        (
            item
            for item in existing_targets(state)
            if str(item.get("test_version") or "") == target_version
        ),
        None,
    )
    if target is None:
        raise ProfilerEvidenceError(
            f"profiler target is not registered: {target_version}"
        )
    if str(target.get("engine_job_id") or "") != engine_job_id:
        raise ProfilerEvidenceError(
            "profiler engine job id mismatch: "
            f"{target.get('engine_job_id')} != {engine_job_id}"
        )
    reported_status = str(evidence.get("status") or "failed").lower()
    validation_error = complete_evidence_validation_error(evidence)
    status = "failed" if reported_status == "complete" and validation_error else reported_status
    mapped = "complete" if status == "complete" else status
    if mapped not in {"complete", "unsupported", "failed", "partial"}:
        mapped = "failed"
    destination = (
        case_dir(root, str(state["operator"]), str(state["case_version"]))
        / "profiler_evidence"
        / str(state["generation_digest"])
        / target_version
        / f"attempt-{int(target.get('attempt', 1) or 1):03d}"
    )
    destination_io = filesystem_path(destination)
    if destination_io.exists():
        shutil.rmtree(destination_io)
    filesystem_path(destination.parent).mkdir(parents=True, exist_ok=True)
    shutil.copytree(filesystem_path(evidence_root), destination_io)
    target.update(
        {
            "status": mapped,
            "evidence_path": relative_path(destination, root),
            "evidence_status": status,
            "reported_evidence_status": reported_status,
            "completed_at": utc_now_iso(),
            "last_error": validation_error or str(evidence.get("error") or ""),
        }
    )
    recompute_request_status(state)
    state["updated_at"] = utc_now_iso()
    write_json_atomic(state_path, state, ensure_ascii=True, sort_keys=True)
    sync_profiler_index(root, state)
    return {
        "status": state["status"],
        "target_status": mapped,
        "evidence_path": target["evidence_path"],
        "request_state_path": relative_path(state_path, root),
    }


def infer_profiler_target_versions(
    root: Path,
    *,
    op: str,
    request_text: str,
    fallback_result: str,
) -> list[str]:
    candidates: list[str] = []
    for raw_line in request_text.splitlines():
        line = raw_line.strip()
        lower = line.lower()
        if not line or "do not profile" in lower:
            continue
        if not any(
            marker in lower
            for marker in (
                "profile exact",
                "profile the exact",
                "source profile",
                "source result",
                "baseline identity",
                "from the immutable",
            )
        ):
            continue
        line_versions = extract_versions_from_line(op, line)
        candidates.extend(line_versions)
    candidates = unique(candidates)
    existing = [
        version
        for version in candidates
        if archived_submit_snapshot_or_none(root, op, version) is not None
    ]
    if not existing:
        if archived_submit_snapshot_or_none(root, op, fallback_result) is None:
            raise ProfilerEvidenceError(
                f"no immutable submit snapshot for profiler target: "
                f"{op}/{fallback_result}"
            )
        existing = [fallback_result]
    return existing


def extract_versions_from_line(op: str, line: str) -> list[str]:
    versions: list[str] = []
    full_pattern = re.compile(
        rf"\b{re.escape(op)}_V(\d+)[._](\d+)\b", flags=re.IGNORECASE
    )
    occupied: list[tuple[int, int]] = []
    for match in full_pattern.finditer(line):
        versions.append(f"{op}_V{int(match.group(1))}_{int(match.group(2))}")
        occupied.append(match.span())
    short: list[tuple[int, int, str]] = []
    for match in re.finditer(r"\bV(\d+)(?:[._](\d+))?\b", line):
        if any(start <= match.start() < finish for start, finish in occupied):
            continue
        major = int(match.group(1))
        minor = match.group(2)
        if minor is not None:
            short.append((major, int(minor), f"{op}_V{major}_{int(minor)}"))
    versions.extend(item[2] for item in short)
    return unique(versions)


def infer_profiler_cases(
    root: Path,
    *,
    op: str,
    case_version: str,
    request_text: str,
    target_version: str,
    max_cases: int,
) -> list[int]:
    available = available_case_ids(root, op, case_version, target_version)
    requested: list[int] = []
    for raw_line in request_text.splitlines():
        lower = raw_line.lower()
        if not any(
            marker in lower
            for marker in (
                "required target",
                "weighted case",
                "matched case",
                "selected case",
                "control",
                "include",
                "cases",
                "case-level",
                "low-work",
                "high-work",
            )
        ):
            continue
        requested.extend(extract_case_ids(raw_line))
    requested = [value for value in unique(requested) if value in available]
    pool = requested or available
    selected = evenly_sample(pool, max_cases)
    if len(selected) < max_cases:
        selected = unique([*selected, *evenly_sample(available, max_cases)])
        selected = selected[:max_cases]
    if not selected:
        raise ProfilerEvidenceError(
            f"profiler request has no available cases: {op}/{case_version}"
        )
    return selected


def extract_case_ids(line: str) -> list[int]:
    values: list[int] = []
    for match in re.finditer(r"\bcases?\s*(\d+)\s*(?:\.\.|-|–)\s*(\d+)", line, re.I):
        start, finish = int(match.group(1)), int(match.group(2))
        if 0 < start <= finish <= 512:
            values.extend(range(start, finish + 1))
    for match in re.finditer(r"\bcase(?:s)?\s*(\d+(?:\s*/\s*\d+)+)", line, re.I):
        values.extend(int(value) for value in re.findall(r"\d+", match.group(1)))
    for match in re.finditer(r"\bcase\s*(\d+)\b", line, re.I):
        values.append(int(match.group(1)))
    return unique(value for value in values if 0 < value <= 512)


def available_case_ids(
    root: Path,
    op: str,
    case_version: str,
    target_version: str,
) -> list[int]:
    metadata_paths = [
        case_dir(root, op, case_version) / "meta.json",
        archived_submit_snapshot(root, op, target_version) / "attack_case" / "meta.json",
    ]
    for path in metadata_paths:
        raw = read_object(path)
        buckets = raw.get("buckets") if raw else None
        if isinstance(buckets, list) and buckets:
            return list(range(1, len(buckets) + 1))
        for field in ("default_perf_case_range", "default_correctness_range"):
            if raw and raw.get(field):
                parsed = parse_case_range(str(raw[field]))
                if parsed:
                    return parsed
    test_op = (
        archived_submit_snapshot(root, op, target_version)
        / "task_case"
        / "test_op.py"
    )
    if test_op.is_file():
        text = test_op.read_text(encoding="utf-8-sig", errors="replace")
        ids = sorted({int(value) for value in re.findall(r"['\"]case(\d+)['\"]", text)})
        if ids:
            return ids
    return list(range(1, 17))


def parse_case_range(value: str) -> list[int]:
    try:
        if ".." in value:
            left, right = value.split("..", 1)
            return list(range(int(left), int(right) + 1))
        return [int(item) for item in re.findall(r"\d+", value)]
    except ValueError:
        return []


def evenly_sample(values: list[int], limit: int) -> list[int]:
    ordered = unique(values)
    if len(ordered) <= limit:
        return ordered
    if limit <= 1:
        return [ordered[0]]
    indices = [
        round(index * (len(ordered) - 1) / (limit - 1))
        for index in range(limit)
    ]
    return unique(ordered[index] for index in indices)


def archived_submit_snapshot(root: Path, op: str, test_version: str) -> Path:
    snapshot = archived_submit_snapshot_or_none(root, op, test_version)
    if snapshot is None:
        raise ProfilerEvidenceError(
            f"archived submit snapshot is incomplete: {op}/{test_version}"
        )
    return snapshot


def archived_submit_snapshot_or_none(
    root: Path, op: str, test_version: str
) -> Path | None:
    snapshot = root / "operators_testresult" / op / test_version / "submit_snapshot"
    required = (
        snapshot / "pending_snapshot" / "source_snapshot",
        snapshot / "task_case",
    )
    return snapshot if all(path.is_dir() for path in required) else None


def discover_request_states(root: Path) -> list[tuple[Path, dict[str, Any]]]:
    request_root = root / "TestUtils" / "tester_daemon" / "profiler_requests"
    if not request_root.is_dir():
        return []
    rows: list[tuple[Path, dict[str, Any]]] = []
    for path in request_root.rglob(PROFILER_REQUEST_FILE):
        raw = read_object(path)
        if str(raw.get("protocol_version") or "") == PROFILER_REQUEST_PROTOCOL:
            rows.append((path, raw))
    return sorted(
        rows,
        key=lambda item: (
            str(item[1].get("created_at") or ""),
            str(item[1].get("operator") or ""),
        ),
    )


def profiler_request_refresh_needed(root: Path) -> bool:
    return any(
        str(state.get("status") or "") not in {"complete", "unsupported"}
        for _path, state in discover_request_states(root)
    )


def refresh_request_from_pump(
    state: dict[str, Any],
    entries: dict[str, Any],
    *,
    now: datetime | None = None,
    missing_grace_seconds: int = MISSING_ENGINE_ENTRY_GRACE_SECONDS,
) -> bool:
    now = now or datetime.now(timezone.utc)
    changed = False
    for target in existing_targets(state):
        job_id = str(target.get("engine_job_id") or "")
        if not job_id:
            continue
        entry = entries.get(job_id)
        if not isinstance(entry, dict):
            if str(target.get("status") or "") != "enqueued":
                continue
            enqueued_at = parse_utc_datetime(
                str(target.get("enqueued_at") or "")
            )
            missing_expired = (
                enqueued_at is None
                or (now - enqueued_at).total_seconds()
                >= max(0, int(missing_grace_seconds))
            )
            if not missing_expired:
                continue
            target.update(
                {
                    "status": "failed",
                    "engine_state": "missing",
                    "last_error": (
                        "engine outbox record missing after durable "
                        "enqueue grace"
                    ),
                }
            )
            changed = True
            continue
        engine_state = str(entry.get("state") or "")
        if str(target.get("engine_state") or "") != engine_state:
            target["engine_state"] = engine_state
            changed = True
        if (
            str(target.get("status") or "") == "enqueued"
            and engine_state in ENGINE_TERMINAL_STATES
            and engine_state != "workflow-archived"
        ):
            target["status"] = "failed"
            target["last_error"] = str(
                entry.get("last_error") or f"engine terminal state {engine_state}"
            )
            changed = True
    if changed:
        recompute_request_status(state)
        state["updated_at"] = utc_now_iso()
    return changed


def parse_utc_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def next_planned_target(state: dict[str, Any]) -> dict[str, Any] | None:
    for target in existing_targets(state):
        status = str(target.get("status") or "")
        if status == "complete":
            continue
        return target if status == "planned" else None
    return None


def existing_targets(state: dict[str, Any]) -> list[dict[str, Any]]:
    raw = state.get("targets", [])
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def recompute_request_status(state: dict[str, Any]) -> str:
    statuses = [str(item.get("status") or "") for item in existing_targets(state)]
    if statuses and all(status == "complete" for status in statuses):
        status = "complete"
    elif any(status == "enqueued" for status in statuses):
        status = "collecting"
    elif statuses and all(status == "unsupported" for status in statuses):
        status = "unsupported"
    elif any(status in {"failed", "partial", "unsupported"} for status in statuses):
        status = "failed"
    elif any(status == "planned" for status in statuses):
        status = "collecting"
    else:
        status = "ready"
    state["status"] = status
    return status


def complete_evidence_validation_error(evidence: dict[str, Any]) -> str:
    if str(evidence.get("status") or "").lower() != "complete":
        return ""
    runs = evidence.get("runs")
    if not isinstance(runs, list) or not runs:
        return "profiler evidence reported complete without any profiler runs"
    for index, raw_run in enumerate(runs, start=1):
        if not isinstance(raw_run, dict):
            return f"profiler run {index} is not a structured record"
        matched = raw_run.get("matched_operator_rows")
        if matched is None:
            csv_rows = raw_run.get("csv_evidence")
            matched = sum(
                int(row.get("matched_operator_rows", 0) or 0)
                for row in csv_rows
                if isinstance(row, dict)
            ) if isinstance(csv_rows, list) else 0
        if not bool(raw_run.get("success")) or int(matched or 0) <= 0:
            case_id = raw_run.get("case_id", index)
            metric = str(raw_run.get("metric_label") or "unknown")
            return (
                "profiler evidence reported complete but "
                f"case {case_id} {metric} did not capture a target operator row"
            )
    return ""


def revalidate_completed_targets(root: Path, state: dict[str, Any]) -> bool:
    changed = False
    for target in existing_targets(state):
        if str(target.get("status") or "") != "complete":
            continue
        evidence_relative = str(target.get("evidence_path") or "")
        evidence = (
            read_object(root / evidence_relative / PROFILER_EVIDENCE_FILE)
            if evidence_relative
            else {}
        )
        error = (
            complete_evidence_validation_error(evidence)
            if evidence
            else "completed profiler target has no archived evidence"
        )
        if not error:
            continue
        target.update(
            {
                "status": "failed",
                "evidence_status": "failed",
                "reported_evidence_status": str(
                    evidence.get("status") or target.get("evidence_status") or ""
                ),
                "last_error": error,
                "invalidated_at": utc_now_iso(),
            }
        )
        changed = True
    if changed:
        recompute_request_status(state)
        state["updated_at"] = utc_now_iso()
    return changed


def sync_profiler_index(root: Path, state: dict[str, Any]) -> None:
    targets = existing_targets(state)
    index = {
        "protocol_version": PROFILER_INDEX_PROTOCOL,
        "operator": str(state.get("operator") or ""),
        "case_version": str(state.get("case_version") or ""),
        "blocker_result_version": str(state.get("blocker_result_version") or ""),
        "blocker_generation": str(state.get("blocker_generation") or ""),
        "generation_digest": str(state.get("generation_digest") or ""),
        "request_sha256": str(state.get("request_sha256") or ""),
        "request_attempt": int(state.get("request_attempt", 1) or 1),
        "request_state_path": str(state.get("request_state_path") or ""),
        "status": str(state.get("status") or ""),
        "cases": list(state.get("cases", [])),
        "targets": [
            {
                key: target.get(key, "")
                for key in (
                    "test_version",
                    "source_sha256",
                    "status",
                    "engine_job_id",
                    "engine_state",
                    "evidence_path",
                    "evidence_status",
                    "last_error",
                )
            }
            for target in targets
        ],
        "created_at": str(state.get("created_at") or ""),
        "updated_at": str(state.get("updated_at") or utc_now_iso()),
    }
    path = evidence_index_path(
        root, str(state["operator"]), str(state["case_version"])
    )
    write_json_atomic(path, index, ensure_ascii=True, sort_keys=True)


def read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def unique(values: Any) -> list[Any]:
    result: list[Any] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve())).replace("\\", "/")
    except ValueError:
        return str(path.resolve()).replace("\\", "/")


def filesystem_path(path: Path) -> Path:
    resolved = str(path.resolve())
    if os.name != "nt" or resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved[2:])
    return Path("\\\\?\\" + resolved)


def validate_token(value: str, field: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ProfilerEvidenceError(f"unsafe {field}: {value!r}")


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


__all__ = [
    "PROFILER_CAPABILITY_FILE",
    "PROFILER_EVIDENCE_FILE",
    "PROFILER_EVIDENCE_PROFILE",
    "PROFILER_INDEX_FILE",
    "PROFILER_JOB_KIND",
    "PROFILER_SUMMARY_FILE",
    "ProfilerEvidenceError",
    "create_profiler_request",
    "enqueue_next_profiler_job",
    "infer_profiler_cases",
    "infer_profiler_target_versions",
    "profiler_evidence_status",
    "profiler_request_refresh_needed",
    "record_profiler_result",
]
