from __future__ import annotations

import ast
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.observability.engine_timing import (
    critical_path_service_seconds,
    resource_service_seconds,
    summarize_values,
)


IDENTITY_FIELDS = (
    "test_version",
    "source_sha256",
    "case_bundle_sha256",
    "golden_bundle_sha256",
    "test_contract_sha256",
    "correctness_case_count",
    "performance_case_count",
    "correctness_repetitions",
    "performance_samples_per_case",
    "environment_sha256",
)
INPUT_IDENTITY_FIELDS = tuple(
    field for field in IDENTITY_FIELDS if field != "environment_sha256"
)

ENGINE_DAEMON_FUNCTIONS = {
    "tick",
    "run_tick",
    "build_tick_plan",
    "enqueue_engine_compatibility_candidates",
    "enqueue_casegen_cache_prewarm_candidates",
    "write_casegen_cache_prewarm_status",
    "maybe_start_engine_pump_worker",
    "prepare_engine_stop_drain",
    "reconcile_engine_executor_config",
    "reconcile_engine_operator_membership",
    "run_engine_pump_loop",
}
ENGINE_CODE_GENERATION_SCHEMA = "engine-code-generation-v3-source-slices"
REMOTE_ENGINE_CODE_FILES = (
    "batch_case_runner.py",
    "case_cache.py",
    "correctness_pipeline.py",
    "engine_identity.py",
    "flow_v3_endpoint.py",
    "flow_v3_protocol.py",
    "operator_cache.py",
    "payload_archive.py",
    "runtime_readiness.py",
    "perf_pipeline.py",
    "profile_session_runner.py",
    "shared_resource_lease.py",
    "test_engine.py",
    "test_engine_cli.py",
    "test_engine_worker.py",
    "wheel_cache.py",
)


def build_identity_evidence(
    *,
    root: Path,
    baseline_identity: dict[str, Any],
    candidate_identity: dict[str, Any],
    baseline_timeline: list[dict[str, Any]],
    candidate_terminal: dict[str, Any],
    baseline_terminal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    baseline_history = history_value(
        object_value(baseline_terminal).get("history")
    )
    baseline_device = device_intervals(baseline_history, job_id="baseline")
    baseline_capture = capture_intervals(baseline_history, job_id="baseline")
    baseline_interval = legacy_device_interval(baseline_timeline)
    candidate_history = history_value(candidate_terminal.get("history"))
    candidate_device = device_intervals(candidate_history, job_id="candidate")
    candidate_capture = capture_intervals(candidate_history, job_id="candidate")
    baseline_intervals = (
        baseline_device
        if baseline_device
        else ([baseline_interval] if baseline_interval else [])
    )
    device_overlap = interval_overlap_count(baseline_intervals + candidate_device)
    capture_overlap = interval_overlap_count(
        (baseline_capture if baseline_device else baseline_intervals)
        + candidate_capture
    )
    legacy_lease = baseline_interval is not None
    baseline_engine_lease = bool(baseline_device) and all(
        "npu" in item.get("locks", []) for item in baseline_device
    )
    baseline_lease = baseline_engine_lease or legacy_lease
    engine_lease = bool(candidate_device) and all(
        "npu" in item.get("locks", []) for item in candidate_device
    )
    terminal_input_identity = identity_fields(
        object_value(candidate_terminal.get("input_identity")),
        fields=INPUT_IDENTITY_FIELDS,
    )
    candidate_runtime_input = identity_fields(
        candidate_identity,
        fields=INPUT_IDENTITY_FIELDS,
    )
    spec_matches_runtime = all(
        terminal_input_identity.get(field)
        and terminal_input_identity.get(field) == candidate_runtime_input.get(field)
        for field in INPUT_IDENTITY_FIELDS
    )
    return {
        "protocol_version": "engine-identity-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine_code_generation": engine_code_generation(root),
        "clock_domains": {
            "accepted_at": "b-engine-wall-clock",
            "terminal_at": "b-engine-wall-clock",
            "stage_history": "b-engine-wall-clock",
            "returned_at": "local-daemon-wall-clock",
            "ingested_at": "local-daemon-wall-clock",
            "transport_elapsed_seconds": "local-monotonic-clock",
        },
        "baseline": identity_fields(baseline_identity),
        "candidate": identity_fields(candidate_identity),
        "candidate_spec_input": terminal_input_identity,
        "candidate_spec_matches_runtime": spec_matches_runtime,
        "device_exclusivity": {
            "exclusive_lease": baseline_lease and engine_lease and device_overlap == 0,
            "device_overlap_count": device_overlap,
            "capture_overlap_count": capture_overlap,
            "lease_conflict_count": device_overlap,
            "baseline_evidence_mode": (
                "engine-terminal"
                if baseline_engine_lease
                else ("legacy-timeline" if legacy_lease else "missing")
            ),
            "legacy_lease_markers_complete": legacy_lease,
            "baseline_engine_device_stages_lease_npu": baseline_engine_lease,
            "engine_device_stages_lease_npu": engine_lease,
        },
    }


def build_throughput_evidence(
    *,
    root: Path,
    pump_state: dict[str, Any],
    pump_events: list[dict[str, Any]],
    admission_state: dict[str, Any] | None = None,
    job_ids: list[str] | None = None,
    baseline_terminal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected = {str(item) for item in (job_ids or []) if str(item)}
    entries_raw = object_value(pump_state.get("entries"))
    entries = {
        str(job_id): dict(record)
        for job_id, record in entries_raw.items()
        if isinstance(record, dict) and (not selected or str(job_id) in selected)
    }
    histories: dict[str, list[dict[str, Any]]] = {}
    for event in pump_events:
        if event.get("kind") != "engine_job_terminal_returned":
            continue
        job_id = str(event.get("engine_job_id") or "")
        if job_id in entries:
            histories[job_id] = history_value(event.get("stage_history"))
    admission_jobs = object_value((admission_state or {}).get("jobs"))
    for job_id in entries:
        if histories.get(job_id):
            continue
        admission = admission_jobs.get(job_id)
        if isinstance(admission, dict):
            terminal = object_value(admission.get("terminal_manifest"))
            histories[job_id] = history_value(terminal.get("history"))

    accepted_intervals: list[dict[str, Any]] = []
    activated_intervals: list[dict[str, Any]] = []
    all_device: list[dict[str, Any]] = []
    all_host: list[dict[str, Any]] = []
    all_export: list[dict[str, Any]] = []
    all_preactivation: list[dict[str, Any]] = []
    all_measurement: list[dict[str, Any]] = []
    jobs: list[dict[str, Any]] = []
    performance_case_total = 0
    performance_task_row_total = 0
    correctness_execution_total = 0
    contract_complete_job_count = 0
    target_identities: list[dict[str, Any]] = []
    scheduler_policy_counts = {"enabled": 0, "disabled": 0, "missing": 0}
    device_continuation_policy_counts = {
        "enabled": 0,
        "disabled": 0,
        "missing": 0,
    }
    remote_engine_generations: set[str] = set()
    for job_id, record in entries.items():
        accepted = parse_time(str(record.get("accepted_at") or ""))
        terminal_time = parse_time(str(record.get("terminal_at") or ""))
        if (
            accepted is not None
            and terminal_time is not None
            and terminal_time >= accepted
        ):
            accepted_intervals.append(
                {"job_id": job_id, "start": accepted, "finish": terminal_time}
            )
        admission = admission_jobs.get(job_id)
        activated = parse_time(
            str(admission.get("activated_at") or "")
            if isinstance(admission, dict)
            else ""
        )
        if (
            activated is not None
            and terminal_time is not None
            and terminal_time >= activated
        ):
            activated_intervals.append(
                {"job_id": job_id, "start": activated, "finish": terminal_time}
            )
        history = histories.get(job_id, [])
        intervals = device_intervals(history, job_id=job_id)
        all_device.extend(intervals)
        stages = stage_intervals(history, job_id=job_id)
        all_host.extend(
            item for item in stages if item.get("resource") == "host"
        )
        all_export.extend(
            item for item in stages if item.get("resource") == "export"
        )
        all_preactivation.extend(
            item for item in stages if item.get("pre_activation")
        )
        all_measurement.extend(
            item
            for item in stages
            if "performance-measurement" in item.get("locks", [])
        )
        terminal = (
            object_value(admission.get("terminal_manifest"))
            if isinstance(admission, dict)
            else {}
        )
        raw_scheduler_policy = record.get("scheduler_policy")
        if not isinstance(raw_scheduler_policy, dict):
            raw_scheduler_policy = terminal.get("scheduler_policy")
        queue_preactivation = (
            str(raw_scheduler_policy.get("queue_preactivation") or "")
            if isinstance(raw_scheduler_policy, dict)
            else ""
        )
        if queue_preactivation not in {"enabled", "disabled"}:
            queue_preactivation = "missing"
        scheduler_policy_counts[queue_preactivation] += 1
        device_continuation = (
            str(raw_scheduler_policy.get("device_continuation") or "")
            if isinstance(raw_scheduler_policy, dict)
            else ""
        )
        if device_continuation not in {"enabled", "disabled"}:
            device_continuation = "missing"
        device_continuation_policy_counts[device_continuation] += 1
        remote_generation = str(
            terminal.get("engine_code_generation")
            or (
                admission.get("engine_code_generation")
                if isinstance(admission, dict)
                else ""
            )
            or ""
        )
        if remote_generation:
            remote_engine_generations.add(remote_generation)
        identity = throughput_job_identity(record, terminal)
        target_identities.append(identity)
        correctness_case_count = integer(identity.get("correctness_case_count"))
        performance_case_count = integer(identity.get("performance_case_count"))
        correctness_repetitions = integer(identity.get("correctness_repetitions"))
        performance_samples_per_case = integer(
            identity.get("performance_samples_per_case")
        )
        contract_complete = all(
            value > 0
            for value in (
                correctness_case_count,
                performance_case_count,
                correctness_repetitions,
                performance_samples_per_case,
            )
        )
        if contract_complete:
            contract_complete_job_count += 1
            performance_case_total += performance_case_count
            performance_task_row_total += (
                performance_case_count * performance_samples_per_case
            )
            correctness_execution_total += (
                correctness_case_count * correctness_repetitions
            )
        jobs.append(
            {
                "engine_job_id": job_id,
                "state": str(record.get("state") or ""),
                "accepted_at": str(record.get("accepted_at") or ""),
                "terminal_at": str(record.get("terminal_at") or ""),
                "returned_at": str(record.get("returned_at") or ""),
                "ingested_at": str(record.get("ingested_at") or ""),
                "device_stage_count": len(intervals),
                "contract_complete": contract_complete,
                "correctness_case_count": correctness_case_count,
                "performance_case_count": performance_case_count,
                "correctness_repetitions": correctness_repetitions,
                "performance_samples_per_case": performance_samples_per_case,
                "correctness_execution_count": (
                    correctness_case_count * correctness_repetitions
                    if contract_complete
                    else 0
                ),
                "expected_performance_task_rows": (
                    performance_case_count * performance_samples_per_case
                    if contract_complete
                    else 0
                ),
                "queue_preactivation": queue_preactivation,
                "device_continuation": device_continuation,
                "remote_engine_code_generation": remote_generation,
                "own_critical_path_service_seconds": round(
                    critical_path_service_seconds(history), 6
                ),
                "own_resource_service_seconds": resource_service_seconds(history),
            }
        )
    all_device.sort(key=lambda item: item["start"])
    preactivation_measurement_overlaps = cross_interval_overlaps(
        all_preactivation,
        all_measurement,
    )
    device_groups: dict[str, list[dict[str, Any]]] = {}
    for interval in all_device:
        device_groups.setdefault(
            str(interval.get("device_id") or "legacy"), []
        ).append(interval)
    handoffs: list[float] = []
    handoffs_by_device: dict[str, list[float]] = {}
    same_device_overlap_count = 0
    for device_id, intervals in sorted(device_groups.items()):
        intervals.sort(key=lambda item: item["start"])
        device_handoffs: list[float] = []
        same_device_overlap_count += interval_overlap_count(intervals)
        for previous, current in zip(intervals, intervals[1:]):
            if previous.get("job_id") == current.get("job_id"):
                continue
            gap = (current["start"] - previous["finish"]).total_seconds()
            if gap >= 0:
                device_handoffs.append(round(gap, 6))
        handoffs.extend(device_handoffs)
        handoffs_by_device[device_id] = device_handoffs
    job_count = len(entries)
    device_busy_seconds = sum(
        max(0.0, (item["finish"] - item["start"]).total_seconds())
        for item in all_device
    )
    device_window_seconds = (
        max(
            0.0,
            (
                max(item["finish"] for item in all_device)
                - min(item["start"] for item in all_device)
            ).total_seconds(),
        )
        if all_device
        else 0.0
    )
    physical_device_count = max(1, len(device_groups))
    device_hours = device_window_seconds * physical_device_count / 3600.0
    baseline_manifest = object_value(baseline_terminal)
    baseline_history = history_value(baseline_manifest.get("history"))
    baseline_device = device_intervals(baseline_history, job_id="baseline")
    baseline_device_busy_seconds = sum(
        max(0.0, (item["finish"] - item["start"]).total_seconds())
        for item in baseline_device
    )
    baseline_input = identity_fields(
        object_value(baseline_manifest.get("input_identity")),
        fields=INPUT_IDENTITY_FIELDS,
    )
    baseline_contract_matches = bool(baseline_device and target_identities) and all(
        all(
            baseline_input.get(field)
            and baseline_input.get(field) == identity.get(field)
            for field in INPUT_IDENTITY_FIELDS
        )
        for identity in target_identities
    )
    equivalent_baseline_device_seconds = baseline_device_busy_seconds * job_count
    speedup_vs_baseline = (
        equivalent_baseline_device_seconds / device_window_seconds
        if baseline_contract_matches and device_window_seconds > 0
        else 0.0
    )
    last_engine_snapshot = object_value(
        (admission_state or {}).get("last_engine_snapshot")
    )
    snapshot_remote_generation = str(
        last_engine_snapshot.get("engine_code_generation") or ""
    )
    remote_generation = (
        next(iter(remote_engine_generations))
        if len(remote_engine_generations) == 1
        else ""
    )
    expected_remote_generation = expected_remote_engine_code_generation(root)
    return {
        "protocol_version": "engine-throughput-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine_code_generation": engine_code_generation(root),
        "job_count": job_count,
        "max_inflight_observed": max_interval_concurrency(accepted_intervals),
        "max_active_jobs_observed": max_interval_concurrency(activated_intervals),
        "max_host_stage_concurrency": max_interval_concurrency(all_host),
        "max_device_stage_concurrency": max_interval_concurrency(all_device),
        "max_export_stage_concurrency": max_interval_concurrency(all_export),
        "preactivation_interval_count": len(all_preactivation),
        "performance_measurement_interval_count": len(all_measurement),
        "preactivation_measurement_overlap_count": len(
            preactivation_measurement_overlaps
        ),
        "preactivation_measurement_overlaps": preactivation_measurement_overlaps,
        "scheduler_policy_counts": scheduler_policy_counts,
        "device_continuation_policy_counts": device_continuation_policy_counts,
        "remote_engine_code_generation": remote_generation,
        "remote_engine_code_generations": sorted(remote_engine_generations),
        "snapshot_remote_engine_code_generation": snapshot_remote_generation,
        "remote_engine_generation_consistent": bool(remote_generation)
        and remote_generation == snapshot_remote_generation,
        "expected_remote_engine_code_generation": expected_remote_generation,
        "remote_engine_generation_current": bool(remote_generation)
        and bool(expected_remote_generation)
        and remote_generation == expected_remote_generation,
        "terminal_count": sum(bool(item.get("terminal_at")) for item in entries.values()),
        "returned_count": sum(bool(item.get("returned_at")) for item in entries.values()),
        "workflow_ingested_count": sum(
            bool(item.get("ingested_at"))
            and str(item.get("ingest_outcome") or "") != "canary-no-archive"
            for item in entries.values()
        ),
        "finalized_count": sum(bool(item.get("ingested_at")) for item in entries.values()),
        "device_overlap_count": same_device_overlap_count,
        "cross_device_overlap_count": max(
            0, interval_overlap_count(all_device) - same_device_overlap_count
        ),
        "physical_device_count": len(device_groups),
        "device_busy_seconds": round(device_busy_seconds, 6),
        "device_window_seconds": round(device_window_seconds, 6),
        "baseline_contract_matches": baseline_contract_matches,
        "baseline_device_busy_seconds": round(baseline_device_busy_seconds, 6),
        "equivalent_baseline_device_seconds": round(
            equivalent_baseline_device_seconds, 6
        ),
        "speedup_vs_baseline": round(speedup_vs_baseline, 6),
        "device_utilization_ratio": (
            round(
                min(
                    1.0,
                    device_busy_seconds
                    / (device_window_seconds * physical_device_count),
                ),
                6,
            )
            if device_window_seconds > 0
            else 0.0
        ),
        "contract_complete_job_count": contract_complete_job_count,
        "performance_case_total": performance_case_total,
        "performance_task_row_total": performance_task_row_total,
        "correctness_execution_total": correctness_execution_total,
        "jobs_per_device_hour": (
            round(job_count / device_hours, 6) if device_hours > 0 else 0.0
        ),
        "performance_cases_per_device_hour": (
            round(performance_case_total / device_hours, 6)
            if device_hours > 0
            else 0.0
        ),
        "performance_task_rows_per_device_hour": (
            round(performance_task_row_total / device_hours, 6)
            if device_hours > 0
            else 0.0
        ),
        "correctness_executions_per_device_hour": (
            round(correctness_execution_total / device_hours, 6)
            if device_hours > 0
            else 0.0
        ),
        "device_handoff_gap_seconds": handoffs,
        "device_handoff_gap_seconds_by_device": handoffs_by_device,
        "device_handoff_gap_summary": summarize_values(handoffs),
        "completion_to_next_device_start_seconds": handoffs,
        "jobs": sorted(jobs, key=lambda item: item["engine_job_id"]),
    }


def throughput_job_identity(
    record: dict[str, Any], terminal: dict[str, Any]
) -> dict[str, Any]:
    evidence = object_value(record.get("identity_evidence"))
    identity = object_value(evidence.get("identity"))
    if identity:
        return identity
    return object_value(terminal.get("input_identity"))


def identity_fields(
    value: dict[str, Any], *, fields: tuple[str, ...] = IDENTITY_FIELDS
) -> dict[str, Any]:
    return {field: value.get(field) for field in fields}


def history_value(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def legacy_device_interval(timeline: list[dict[str, Any]]) -> dict[str, Any] | None:
    phases: dict[str, datetime] = {}
    for item in timeline:
        phase = str(item.get("phase") or "")
        timestamp = parse_time(str(item.get("timestamp") or item.get("time") or ""))
        if phase and timestamp is not None:
            phases[phase] = timestamp
    start = phases.get("shared_device_lease_wait_start")
    finish = phases.get("shared_device_lease_released")
    if start is None or finish is None or finish < start:
        return None
    return {"job_id": "legacy", "start": start, "finish": finish, "locks": ["npu", "performance-measurement"]}


def device_intervals(
    history: list[dict[str, Any]], *, job_id: str = "engine"
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in history:
        locks = [str(lock) for lock in item.get("stage_locks", []) if str(lock)]
        runtime_locks = [
            str(lock)
            for lock in item.get("runtime_stage_locks", locks)
            if str(lock)
        ]
        if (
            str(item.get("stage_resource") or "") != "device"
            and "npu" not in locks
            and not any(lock.startswith("npu:") for lock in runtime_locks)
        ):
            continue
        start = parse_time(str(item.get("started_at") or ""))
        finish = parse_time(str(item.get("finished_at") or ""))
        if start is None or finish is None or finish < start:
            continue
        result.append(
            {
                "job_id": job_id,
                "device_id": str(item.get("device_id") or "legacy"),
                "stage_name": str(item.get("stage_name") or ""),
                "start": start,
                "finish": finish,
                "locks": locks,
                "runtime_locks": runtime_locks,
            }
        )
    return result


def stage_intervals(
    history: list[dict[str, Any]], *, job_id: str = "engine"
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in history:
        start = parse_time(str(item.get("started_at") or ""))
        finish = parse_time(str(item.get("finished_at") or ""))
        if start is None or finish is None or finish < start:
            continue
        result.append(
            {
                "job_id": job_id,
                "stage_name": str(item.get("stage_name") or ""),
                "resource": str(item.get("stage_resource") or ""),
                "pre_activation": bool(item.get("pre_activation", False)),
                "start": start,
                "finish": finish,
                "locks": [
                    str(lock) for lock in item.get("stage_locks", []) if str(lock)
                ],
            }
        )
    return result


def capture_intervals(
    history: list[dict[str, Any]], *, job_id: str = "engine"
) -> list[dict[str, Any]]:
    return [
        item
        for item in device_intervals(history, job_id=job_id)
        if "performance-measurement" in item.get("locks", [])
    ]


def interval_overlap_count(intervals: list[dict[str, Any]]) -> int:
    count = 0
    for index, left in enumerate(intervals):
        for right in intervals[index + 1 :]:
            if left.get("job_id") == right.get("job_id"):
                continue
            if left["start"] < right["finish"] and right["start"] < left["finish"]:
                count += 1
    return count


def cross_interval_overlaps(
    left_intervals: list[dict[str, Any]],
    right_intervals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    overlaps: list[dict[str, Any]] = []
    for left in left_intervals:
        for right in right_intervals:
            start = max(left["start"], right["start"])
            finish = min(left["finish"], right["finish"])
            if start >= finish:
                continue
            overlaps.append(
                {
                    "preactivation_job_id": str(left.get("job_id") or ""),
                    "preactivation_stage_name": str(left.get("stage_name") or ""),
                    "measurement_job_id": str(right.get("job_id") or ""),
                    "measurement_stage_name": str(right.get("stage_name") or ""),
                    "overlap_seconds": round((finish - start).total_seconds(), 6),
                }
            )
    return overlaps


def max_interval_concurrency(intervals: list[dict[str, Any]]) -> int:
    points: list[tuple[datetime, int]] = []
    for item in intervals:
        points.append((item["start"], 1))
        points.append((item["finish"], -1))
    current = 0
    maximum = 0
    for _, delta in sorted(points, key=lambda item: (item[0], item[1])):
        current += delta
        maximum = max(maximum, current)
    return maximum


def parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_json_lines(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    result: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            for line in handle:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    result.append(item)
    except OSError:
        return []
    return result


def engine_code_generation(root: Path) -> str:
    root = root.resolve()
    daemon_path = root / "tools" / "tester_daemon" / "daemon.py"
    candidates = [
        root
        / "tools"
        / "tester_daemon"
        / "ascendop_daemon"
        / "casegen_prewarm.py",
        *sorted(
            item
            for item in (root / "tools" / "tester_daemon" / "ascendop_daemon").glob(
                "engine_*.py"
            )
            if item.name != "engine_promotion.py"
        ),
    ]
    gp = root / "GitPartner" / "src" / "limited_remote_partner"
    for name in (
        "batch_case_runner.py",
        "case_cache.py",
        "client.py",
        "correctness_pipeline.py",
        "engine_ab_compare.py",
        "engine_identity.py",
        "git_lock.py",
        "perf_pipeline.py",
        "payload_archive.py",
        "profile_session_runner.py",
        "shared_resource_lease.py",
        "submit_job.py",
        "test_engine.py",
        "test_engine_cli.py",
        "test_engine_worker.py",
        "wheel_cache.py",
    ):
        candidates.append(gp / name)
    paths = sorted({item.resolve() for item in candidates if item.is_file()})
    last_error: OSError | None = None
    for attempt in range(3):
        digest = hashlib.sha256()
        try:
            digest.update(ENGINE_CODE_GENERATION_SCHEMA.encode("ascii") + b"\0")
            if daemon_path.is_file():
                digest.update(daemon_path.relative_to(root).as_posix().encode("utf-8"))
                digest.update(b"\0")
                digest.update(engine_daemon_semantic_source(daemon_path))
                digest.update(b"\0")
            for path in paths:
                digest.update(path.relative_to(root).as_posix().encode("utf-8"))
                digest.update(b"\0")
                digest.update(path.read_bytes())
                digest.update(b"\0")
            return digest.hexdigest()
        except OSError as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.02)
    raise OSError("engine code generation changed during read") from last_error


def expected_remote_engine_code_generation(
    root: Path,
    *,
    gitpartner_repo: Path | None = None,
) -> str:
    canonical_package_root = (
        root.resolve() / "GitPartner" / "src" / "limited_remote_partner"
    )
    package_roots: list[Path] = []
    if gitpartner_repo is not None:
        registered_package_root = (
            gitpartner_repo.resolve() / "src" / "limited_remote_partner"
        )
        package_roots.append(registered_package_root)
    if canonical_package_root not in package_roots:
        package_roots.append(canonical_package_root)
    package_root = next(
        (
            candidate
            for candidate in package_roots
            if all(
                (candidate / name).is_file()
                for name in REMOTE_ENGINE_CODE_FILES
            )
        ),
        None,
    )
    if package_root is None:
        return ""
    paths = [package_root / name for name in REMOTE_ENGINE_CODE_FILES]
    last_error: OSError | None = None
    for attempt in range(3):
        digest = hashlib.sha256()
        try:
            for path in paths:
                digest.update(path.name.encode("utf-8"))
                # Git checks these Python sources out with LF on the Linux
                # engine host.  Canonicalize the Windows worktree bytes so a
                # code generation comparison reflects content, not checkout
                # line-ending policy.
                digest.update(path.read_bytes().replace(b"\r\n", b"\n"))
            return digest.hexdigest()[:16]
        except OSError as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.02)
    raise OSError("remote engine code generation changed during read") from last_error


def engine_daemon_semantic_source(path: Path) -> bytes:
    """Hash engine orchestration without coupling it to the Python AST schema."""
    try:
        source = path.read_text(encoding="utf-8-sig")
        tree = ast.parse(source, filename=str(path))
    except (OSError, SyntaxError) as exc:
        raise OSError(f"cannot read engine orchestration from {path}") from exc

    selected: list[ast.AST] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            name = node.name
            if (
                name in ENGINE_DAEMON_FUNCTIONS
                or name.startswith("engine_")
                or name.startswith("reconcile_engine_")
            ):
                selected.append(node)
            continue
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(
                isinstance(target, ast.Name) and target.id.startswith("ENGINE_")
                for target in targets
            ):
                selected.append(node)

    lines = source.splitlines(keepends=True)
    chunks: list[str] = []
    for node in selected:
        start_line = int(getattr(node, "lineno", 1) or 1)
        decorators = getattr(node, "decorator_list", [])
        if decorators:
            start_line = min(
                start_line,
                *(int(getattr(item, "lineno", start_line) or start_line) for item in decorators),
            )
        end_line = int(getattr(node, "end_lineno", start_line) or start_line)
        chunks.append("".join(lines[start_line - 1 : end_line]))

    # ast.dump() is not stable across Python minor versions because new AST
    # fields are added over time.  Top-level source slices preserve the same
    # semantic scope while making the promotion receipt interpreter-neutral.
    payload = "\n\0engine-node\0\n".join(chunks)
    return payload.encode("utf-8")


def evaluate_engine_promotion(
    *,
    root: Path,
    ab_report: dict[str, Any],
    identity_evidence: dict[str, Any],
    throughput_evidence: dict[str, Any],
    max_device_handoff_seconds: float = 10.0,
    minimum_jobs: int = 2,
    minimum_speedup: float = 4.0,
) -> dict[str, Any]:
    blockers: list[str] = []
    generation = engine_code_generation(root)
    ab_protocol = str(ab_report.get("protocol_version") or "")
    if ab_protocol not in {
        "engine-ab-series-v1",
        "engine-ab-series-v2",
        "engine-ab-series-v3",
    } or ab_report.get("verdict") != "PASS":
        blockers.append("alternating same-version A/B series gate is missing or not PASS")
    if integer(ab_report.get("pair_count")) < 3:
        blockers.append("alternating same-version A/B series has fewer than 3 pairs")
    if ab_report.get("order_complete") is not True and set(
        str(item) for item in ab_report.get("execution_orders", [])
    ) != {"legacy-engine", "engine-legacy"}:
        blockers.append("alternating same-version A/B series lacks both execution orders")
    if ab_protocol == "engine-ab-series-v2":
        if ab_report.get("equivalence_mode") != "causal-performance-first-v1":
            blockers.append("causal A/B series equivalence mode is invalid")
        aggregate = object_value(
            ab_report.get("aggregate_weighted_shift_percent")
        )
        if aggregate.get("ok") is not True:
            blockers.append("causal A/B aggregate weighted shift is not within limit")
    if ab_protocol == "engine-ab-series-v3":
        if ab_report.get("equivalence_mode") != "scheduler-policy-v1":
            blockers.append("scheduler-policy A/B series equivalence mode is invalid")
        aggregate = object_value(
            ab_report.get("aggregate_weighted_shift_percent")
        )
        if aggregate.get("ok") is not True:
            blockers.append(
                "scheduler-policy A/B aggregate weighted shift is not within limit"
            )
        if not str(ab_report.get("remote_engine_code_generation") or ""):
            blockers.append(
                "scheduler-policy A/B remote engine code generation is missing"
            )

    baseline = object_value(identity_evidence.get("baseline"))
    candidate = object_value(identity_evidence.get("candidate"))
    if identity_evidence.get("protocol_version") != "engine-identity-v1":
        blockers.append("identity evidence protocol is not engine-identity-v1")
    for field in IDENTITY_FIELDS:
        left = str(baseline.get(field) or "")
        right = str(candidate.get(field) or "")
        if not left or not right:
            blockers.append(f"identity evidence missing {field}")
        elif left != right:
            blockers.append(f"baseline/candidate {field} differs")
    identity_generation = str(identity_evidence.get("engine_code_generation") or "")
    if identity_generation != generation:
        blockers.append("identity evidence engine code generation is stale or missing")
    if identity_evidence.get("candidate_spec_matches_runtime") is not True:
        blockers.append("candidate spec input identity does not match B runtime identity")

    exclusivity = object_value(identity_evidence.get("device_exclusivity"))
    if exclusivity.get("exclusive_lease") is not True:
        blockers.append("exclusive device lease is not proven")
    for field in ("device_overlap_count", "capture_overlap_count", "lease_conflict_count"):
        if integer(exclusivity.get(field), default=-1) != 0:
            blockers.append(f"device exclusivity {field} is not zero")

    if throughput_evidence.get("protocol_version") != "engine-throughput-v1":
        blockers.append("throughput evidence protocol is not engine-throughput-v1")
    throughput_generation = str(throughput_evidence.get("engine_code_generation") or "")
    if throughput_generation != generation:
        blockers.append("throughput evidence engine code generation is stale or missing")
    job_count = integer(throughput_evidence.get("job_count"))
    required_jobs = max(
        4 if ab_protocol == "engine-ab-series-v3" else 2,
        int(minimum_jobs),
    )
    if job_count < required_jobs:
        blockers.append(f"throughput evidence has fewer than {required_jobs} jobs")
    required_inflight = 4 if ab_protocol == "engine-ab-series-v3" else 2
    if integer(throughput_evidence.get("max_inflight_observed")) < required_inflight:
        blockers.append(
            f"at least {required_inflight} inflight jobs were not observed"
        )
    continuation_counts = object_value(
        throughput_evidence.get("device_continuation_policy_counts")
    )
    continuation_mode = (
        ab_protocol == "engine-ab-series-v3"
        and integer(continuation_counts.get("enabled"), default=-1) == job_count
        and integer(continuation_counts.get("disabled")) == 0
        and integer(continuation_counts.get("missing")) == 0
    )
    required_active_jobs = 1 if continuation_mode else 2
    if integer(throughput_evidence.get("max_active_jobs_observed")) < required_active_jobs:
        blockers.append(
            f"at least {required_active_jobs} active engine jobs were not observed"
        )
    if integer(throughput_evidence.get("max_host_stage_concurrency")) < 2:
        blockers.append("concurrent host stages were not observed")
    if integer(
        throughput_evidence.get("max_device_stage_concurrency"), default=-1
    ) != 1:
        blockers.append("device stage concurrency is not exactly one")
    export_concurrency = integer(
        throughput_evidence.get("max_export_stage_concurrency"), default=-1
    )
    if export_concurrency < 1 or export_concurrency > 1:
        blockers.append("export stage concurrency is not exactly one")
    if integer(
        throughput_evidence.get("preactivation_measurement_overlap_count"),
        default=-1,
    ) != 0:
        blockers.append("queue preactivation overlapped performance measurement")
    for field in ("terminal_count", "returned_count", "finalized_count"):
        if integer(throughput_evidence.get(field), default=-1) != job_count:
            blockers.append(f"throughput {field} does not equal job_count")
    if integer(
        throughput_evidence.get("contract_complete_job_count"), default=-1
    ) != job_count:
        blockers.append("throughput contract cardinality is incomplete")
    if integer(throughput_evidence.get("performance_case_total")) <= 0:
        blockers.append("throughput performance case total is missing")
    if integer(throughput_evidence.get("performance_task_row_total")) <= 0:
        blockers.append("throughput performance task row total is missing")
    if integer(throughput_evidence.get("correctness_execution_total")) <= 0:
        blockers.append("throughput correctness execution total is missing")
    if integer(throughput_evidence.get("device_overlap_count"), default=-1) != 0:
        blockers.append("throughput device_overlap_count is not zero")
    if throughput_evidence.get("baseline_contract_matches") is not True:
        blockers.append("throughput baseline contract does not match target jobs")
    if throughput_evidence.get("remote_engine_generation_consistent") is not True:
        blockers.append(
            "throughput jobs do not match the observed remote engine generation"
        )
    if throughput_evidence.get("remote_engine_generation_current") is not True:
        blockers.append(
            "throughput remote engine generation is stale or unverified"
        )
    if ab_protocol == "engine-ab-series-v3":
        if integer(throughput_evidence.get("preactivation_interval_count")) <= 0:
            blockers.append("scheduler-policy throughput did not execute preactivation")
        policy_counts = object_value(
            throughput_evidence.get("scheduler_policy_counts")
        )
        if integer(policy_counts.get("enabled"), default=-1) != job_count:
            blockers.append(
                "scheduler-policy throughput jobs are not all preactivation-enabled"
            )
        if integer(policy_counts.get("disabled")) != 0 or integer(
            policy_counts.get("missing")
        ) != 0:
            blockers.append("scheduler-policy throughput contains wrong/missing policy")
        ab_remote_generation = str(
            ab_report.get("remote_engine_code_generation") or ""
        )
        throughput_remote_generation = str(
            throughput_evidence.get("remote_engine_code_generation") or ""
        )
        if (
            not ab_remote_generation
            or ab_remote_generation != throughput_remote_generation
        ):
            blockers.append(
                "A/B and throughput remote engine code generations differ"
            )
    speedup_vs_baseline = numeric(throughput_evidence.get("speedup_vs_baseline"))
    if speedup_vs_baseline < float(minimum_speedup):
        blockers.append(
            "throughput speedup is below "
            f"{float(minimum_speedup):g}x: {speedup_vs_baseline:g}x"
        )

    handoffs = numeric_list(
        throughput_evidence.get("device_handoff_gap_seconds")
        or throughput_evidence.get("completion_to_next_device_start_seconds")
    )
    if len(handoffs) < max(1, job_count - 1):
        blockers.append("completion-to-next-device-start samples are incomplete")
    elif max(handoffs) >= float(max_device_handoff_seconds):
        blockers.append(
            "completion-to-next-device-start exceeds "
            f"{float(max_device_handoff_seconds):g}s"
        )

    return {
        "protocol_version": "engine-promotion-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "verdict": "PASS" if not blockers else "FAIL",
        "blockers": blockers,
        "engine_code_generation": generation,
        "thresholds": {
            "minimum_jobs": required_jobs,
            "max_device_handoff_seconds": float(max_device_handoff_seconds),
            "minimum_speedup": float(minimum_speedup),
        },
        "ab_comparison_id": str(ab_report.get("comparison_id") or ""),
        "identity": {
            "baseline": {field: baseline.get(field) for field in IDENTITY_FIELDS},
            "candidate": {field: candidate.get(field) for field in IDENTITY_FIELDS},
            "device_exclusivity": exclusivity,
        },
        "throughput": {
            "job_count": job_count,
            "max_inflight_observed": integer(throughput_evidence.get("max_inflight_observed")),
            "max_active_jobs_observed": integer(
                throughput_evidence.get("max_active_jobs_observed")
            ),
            "max_host_stage_concurrency": integer(
                throughput_evidence.get("max_host_stage_concurrency")
            ),
            "max_device_stage_concurrency": integer(
                throughput_evidence.get("max_device_stage_concurrency"), default=-1
            ),
            "max_export_stage_concurrency": integer(
                throughput_evidence.get("max_export_stage_concurrency"), default=-1
            ),
            "preactivation_interval_count": integer(
                throughput_evidence.get("preactivation_interval_count")
            ),
            "preactivation_measurement_overlap_count": integer(
                throughput_evidence.get("preactivation_measurement_overlap_count"),
                default=-1,
            ),
            "scheduler_policy_counts": object_value(
                throughput_evidence.get("scheduler_policy_counts")
            ),
            "remote_engine_code_generation": str(
                throughput_evidence.get("remote_engine_code_generation") or ""
            ),
            "remote_engine_generation_consistent": throughput_evidence.get(
                "remote_engine_generation_consistent"
            )
            is True,
            "expected_remote_engine_code_generation": str(
                throughput_evidence.get("expected_remote_engine_code_generation")
                or ""
            ),
            "remote_engine_generation_current": throughput_evidence.get(
                "remote_engine_generation_current"
            )
            is True,
            "terminal_count": integer(throughput_evidence.get("terminal_count")),
            "returned_count": integer(throughput_evidence.get("returned_count")),
            "workflow_ingested_count": integer(
                throughput_evidence.get("workflow_ingested_count")
            ),
            "finalized_count": integer(throughput_evidence.get("finalized_count")),
            "device_overlap_count": integer(
                throughput_evidence.get("device_overlap_count"), default=-1
            ),
            "device_busy_seconds": numeric(
                throughput_evidence.get("device_busy_seconds")
            ),
            "device_window_seconds": numeric(
                throughput_evidence.get("device_window_seconds")
            ),
            "device_utilization_ratio": numeric(
                throughput_evidence.get("device_utilization_ratio")
            ),
            "baseline_contract_matches": throughput_evidence.get(
                "baseline_contract_matches"
            ) is True,
            "baseline_device_busy_seconds": numeric(
                throughput_evidence.get("baseline_device_busy_seconds")
            ),
            "equivalent_baseline_device_seconds": numeric(
                throughput_evidence.get("equivalent_baseline_device_seconds")
            ),
            "speedup_vs_baseline": speedup_vs_baseline,
            "contract_complete_job_count": integer(
                throughput_evidence.get("contract_complete_job_count"), default=-1
            ),
            "performance_case_total": integer(
                throughput_evidence.get("performance_case_total")
            ),
            "performance_task_row_total": integer(
                throughput_evidence.get("performance_task_row_total")
            ),
            "correctness_execution_total": integer(
                throughput_evidence.get("correctness_execution_total")
            ),
            "jobs_per_device_hour": numeric(
                throughput_evidence.get("jobs_per_device_hour")
            ),
            "performance_cases_per_device_hour": numeric(
                throughput_evidence.get("performance_cases_per_device_hour")
            ),
            "performance_task_rows_per_device_hour": numeric(
                throughput_evidence.get("performance_task_rows_per_device_hour")
            ),
            "correctness_executions_per_device_hour": numeric(
                throughput_evidence.get("correctness_executions_per_device_hour")
            ),
            "device_handoff_gap_seconds": handoffs,
            "device_handoff_gap_summary": object_value(
                throughput_evidence.get("device_handoff_gap_summary")
            ),
            "completion_to_next_device_start_seconds": handoffs,
        },
    }


def promotion_gate_status(root: Path, policy: dict[str, Any]) -> dict[str, Any]:
    mode = str(policy.get("test_executor", "legacy") or "legacy")
    required = mode == "engine-v1"
    path = root / "TestUtils" / "tester_daemon" / "engine_promotion_latest.json"
    report = read_object(path)
    blockers: list[str] = []
    if required:
        if report.get("protocol_version") != "engine-promotion-v1":
            blockers.append("engine promotion receipt is missing")
        elif report.get("verdict") != "PASS":
            blockers.extend(str(item) for item in report.get("blockers", []) if str(item))
            if not blockers:
                blockers.append("engine promotion receipt is not PASS")
        if str(report.get("engine_code_generation") or "") != engine_code_generation(root):
            blockers.append("engine promotion receipt does not match current code generation")
    return {
        "requested_mode": mode,
        "required": required,
        "allowed": not required or not blockers,
        "blockers": unique(blockers),
        "path": path.relative_to(root).as_posix(),
        "report": report,
    }


def write_promotion_report(
    report: dict[str, Any], output_dir: Path
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "ENGINE_PROMOTION_REPORT.json"
    markdown_path = output_dir / "ENGINE_PROMOTION_REPORT.md"
    write_json(json_path, report)
    lines = [
        "# Engine Production Promotion Gate",
        "",
        f"- Verdict: **{report.get('verdict', 'FAIL')}**",
        f"- Generated: `{report.get('generated_at', '')}`",
        f"- Engine generation: `{report.get('engine_code_generation', '')}`",
        f"- A/B comparison: `{report.get('ab_comparison_id', '')}`",
        (
            "- Measured throughput speedup: "
            f"`{numeric(object_value(report.get('throughput')).get('speedup_vs_baseline')):.6f}x`"
        ),
        (
            "- Required throughput speedup: "
            f"`{numeric(object_value(report.get('thresholds')).get('minimum_speedup')):.6f}x`"
        ),
        "",
        "## Blockers",
        "",
    ]
    blockers = [str(item) for item in report.get("blockers", []) if str(item)]
    lines.extend(f"- {item}" for item in blockers)
    if not blockers:
        lines.append("- none")
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, markdown_path


def read_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def object_value(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def integer(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def numeric(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def numeric_list(value: object) -> list[float]:
    if not isinstance(value, list):
        return []
    result: list[float] = []
    for item in value:
        try:
            result.append(float(item))
        except (TypeError, ValueError):
            continue
    return result


def unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))
