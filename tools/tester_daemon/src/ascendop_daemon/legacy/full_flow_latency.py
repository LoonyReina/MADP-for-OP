from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ascendop_daemon.observability.engine_timing import (
    critical_path_service_seconds,
    device_flow_summary,
    resource_service_seconds,
    serialized_device_intervals,
)
from ascendop_daemon.core.models import BoardSnapshot, DaemonConfig, extract_test_version, observed_operators, utc_now_iso


FULL_FLOW_JSON = "full_flow_latency.json"
FULL_FLOW_MD = "FULL_FLOW_LATENCY.md"
SCHEMA_VERSION = "full-flow-latency-v5"
QUALIFIED_RESULT_RE = re.compile(r"^Verdict:\s*(\S+)", re.IGNORECASE | re.MULTILINE)
NON_QUALIFIED_RESULT_PREFIXES = ("INFRA", "BUILD", "INSTALL", "PRECHECK", "TRANSPORT")
_JSONL_CACHE: dict[Path, tuple[int, int, list[dict[str, Any]]]] = {}

DEFAULT_THRESHOLDS_SECONDS = {
    "solver_candidate_ready_to_submit_prepared": 10.0,
    "submit_prepared_to_local_dispatch": 10.0,
    "local_dispatch_to_engine_outbox": 2.0,
    "engine_outbox_to_accept_start": 2.0,
    "engine_accept_transport": 15.0,
    "gp_ingress_publish": 5.0,
    "gp_ingress_wait_fetch_merge": 10.0,
    "gp_ingress_wait_sleep": 5.0,
    "b_request_to_accept": 2.0,
    "b_accept_to_first_stage": 2.0,
    "b_terminal_to_snapshot": 2.0,
    "b_terminal_to_return_export_start": 2.0,
    "b_return_export_duration": 5.0,
    "snapshot_transport": 15.0,
    "gp_return_publish": 5.0,
    "gp_return_wait_fetch_merge": 10.0,
    "gp_return_wait_sleep": 5.0,
    "return_to_ingest": 2.0,
    "ingest_to_result_archive": 2.0,
    "result_to_relay_ready": 2.0,
    "relay_ready_to_claim": 5.0,
    "relay_claim_to_solver_turn": 5.0,
    "result_to_solver_turn": 10.0,
}

STRICT_REQUIRED_TIMESTAMPS = (
    "solver_candidate_ready_at",
    "submit_prepared_at",
    "local_dispatch_started_at",
    "engine_outbox_enqueued_at",
    "engine_accept_started_at",
    "engine_admitted_observed_at",
    "b_request_observed_at",
    "b_accepted_at",
    "b_first_stage_started_at",
    "b_terminal_at",
    "b_return_ready_at",
    "b_snapshot_observed_at",
    "b_return_export_started_at",
    "b_return_export_finished_at",
    "snapshot_started_at",
    "snapshot_completed_at",
    "engine_return_observed_at",
    "engine_ingest_started_at",
    "engine_ingested_at",
    "result_archived_at",
    "solver_relay_ready_at",
    "solver_relay_claimed_at",
    "solver_turn_started_at",
)


def build_full_flow_latency(
    root: Path,
    config: DaemonConfig,
    snapshot: BoardSnapshot,
    timeline_events: list[dict[str, Any]],
) -> dict[str, Any]:
    allowed = set(observed_operators(config))
    pump_events = [
        item
        for item in read_jsonl(root / "TestUtils" / "tester_daemon" / "engine_pump_events.jsonl")
        if str(item.get("operator") or item.get("op") or "") in allowed
    ]
    relay_events = read_jsonl(
        root / "TestUtils" / "tester_daemon" / "native_relay_claim_events.jsonl"
    )
    relay_events.extend(current_relay_outbox_events(root))

    pump_by_task: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in pump_events:
        op = str(event.get("operator") or event.get("op") or "")
        version = str(event.get("test_version") or "")
        if op and version:
            pump_by_task[(op, version)].append(event)

    timeline_by_task: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in timeline_events:
        op = str(event.get("op") or "")
        version = str(event.get("test_version") or "")
        if op in allowed and version:
            timeline_by_task[(op, version)].append(event)

    relay_by_task: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in relay_events:
        if str(event.get("type") or "") != "solver":
            continue
        op = str(event.get("op") or "")
        if op not in allowed:
            continue
        version = extract_test_version(str(event.get("key") or event.get("prompt_preview") or ""))
        if version:
            relay_by_task[(op, version)].append(event)

    strict_since = parse_time(str(config.policy.get("full_flow_strict_since", "") or ""))
    thresholds = configured_thresholds(config)
    samples: list[dict[str, Any]] = []
    for task, task_pump_events in pump_by_task.items():
        if not any(
            str(item.get("kind") or "")
            in {"engine_job_terminal_returned", "engine_result_ingested"}
            for item in task_pump_events
        ):
            continue
        sample = build_task_sample(
            root,
            task,
            task_pump_events,
            timeline_by_task.get(task, []),
            relay_by_task.get(task, []),
            strict_since=strict_since,
            thresholds=thresholds,
        )
        if sample:
            samples.append(sample)

    samples.sort(key=sample_sort_key)
    sample_limit = max(10, int(config.policy.get("full_flow_sample_limit", 80) or 80))
    samples = samples[-sample_limit:]
    cohort = current_runtime_cohort(root, config)
    for sample in samples:
        sample["current_cohort"] = sample_matches_runtime_cohort(sample, cohort)
    current_samples = [item for item in samples if item.get("current_cohort")]
    aggregate = aggregate_samples(current_samples, thresholds)
    device_flow = device_flow_summary(current_samples)
    historical_aggregate = aggregate_samples(samples, thresholds)
    min_strict_samples = max(
        1, int(config.policy.get("full_flow_min_strict_samples", 3) or 3)
    )
    eligible = [item for item in current_samples if item.get("strict_eligible")]
    strict_complete = [item for item in eligible if item.get("strict_complete")]
    strict_violations = [
        item
        for item in eligible
        if not item.get("strict_complete") or item.get("threshold_violations")
    ]
    if strict_since is None:
        gate_status = "observing"
    elif len(eligible) < min_strict_samples:
        gate_status = "warming-up"
    elif strict_violations:
        gate_status = "failed"
    else:
        gate_status = "passed"

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": snapshot.captured_at or utc_now_iso(),
        "clock_policy": {
            "rule": "never subtract timestamps from different hosts",
            "a_side": "local daemon/GP monotonic or wall clock",
            "b_side": "engine accepted/stage/terminal/snapshot timestamps",
            "transport": "local monotonic elapsed reported by EngineTransportAdapter",
        },
        "strict_since": strict_since.isoformat() if strict_since else "",
        "strict_required_timestamps": list(STRICT_REQUIRED_TIMESTAMPS),
        "thresholds_seconds": thresholds,
        "current_runtime_cohort": {
            **cohort,
            "sample_count": len(current_samples),
            "historical_sample_count": len(samples),
            "cache_state_counts": dict(
                Counter(
                    str(dict(item.get("runtime_identity") or {}).get("cache_state") or "unknown")
                    for item in current_samples
                )
            ),
        },
        "gate": {
            "status": gate_status,
            "min_strict_samples": min_strict_samples,
            "eligible_sample_count": len(eligible),
            "strict_complete_count": len(strict_complete),
            "violation_count": len(strict_violations),
            "violations": [compact_violation(item) for item in strict_violations[-10:]],
        },
        "aggregate": aggregate,
        "device_flow": device_flow,
        "historical_aggregate": historical_aggregate,
        "cache_state_aggregates": {
            state: aggregate_samples(
                [
                    item
                    for item in current_samples
                    if str(dict(item.get("runtime_identity") or {}).get("cache_state") or "unknown")
                    == state
                ],
                thresholds,
            )
            for state in sorted(
                {
                    str(dict(item.get("runtime_identity") or {}).get("cache_state") or "unknown")
                    for item in current_samples
                }
            )
        },
        "samples": samples,
    }


def build_task_sample(
    root: Path,
    task: tuple[str, str],
    pump_events: list[dict[str, Any]],
    timeline_events: list[dict[str, Any]],
    relay_events: list[dict[str, Any]],
    *,
    strict_since: datetime | None,
    thresholds: dict[str, float],
) -> dict[str, Any] | None:
    op, version = task
    admitted = latest_kind(pump_events, "engine_job_admitted")
    returned = latest_kind(pump_events, "engine_job_terminal_returned")
    ingested = latest_kind(pump_events, "engine_result_ingested")
    if returned is None and ingested is None:
        return None
    outbox = latest_before(
        events_of_kind(pump_events, "engine_outbox_enqueued"),
        event_datetime(admitted) if admitted else None,
    )
    accept_started = matching_accept_started(pump_events, admitted)
    ingest_started = latest_before(
        events_of_kind(pump_events, "engine_result_ingest_started"),
        event_datetime(ingested) if ingested else None,
    )

    return_dt = event_datetime(returned)
    result_events = [
        item
        for item in timeline_events
        if str(item.get("event") or "") == "result_archived"
        and (return_dt is None or event_datetime(item) is None or event_datetime(item) >= return_dt)
    ]
    result_archived = first_event(result_events)
    result_dt = event_datetime(result_archived)
    dispatch_candidates = [
        item
        for item in timeline_events
        if str(item.get("event") or "")
        in {
            "engine_dispatch_enqueued",
            "execute_worker_command_started",
            "execute_worker_started",
            "resource_lease_acquired",
        }
    ]
    local_dispatch = latest_before(
        dispatch_candidates,
        event_datetime(outbox) or event_datetime(admitted) or return_dt,
    )
    submit_prepared = latest_before(
        [item for item in timeline_events if str(item.get("event") or "") == "submit_prepared"],
        event_datetime(local_dispatch) or event_datetime(outbox) or event_datetime(admitted),
    )
    solver_candidate_ready = latest_before(
        [
            item
            for item in timeline_events
            if str(item.get("event") or "") == "solver_candidate_ready"
        ],
        event_datetime(submit_prepared)
        or event_datetime(local_dispatch)
        or event_datetime(outbox)
        or event_datetime(admitted),
    )
    selected = latest_before(
        [item for item in timeline_events if str(item.get("event") or "") == "selected_action"],
        event_datetime(local_dispatch),
    )

    relay_claimed, relay_completed = result_solver_relay(relay_events, result_dt)
    history = list((returned or {}).get("stage_history") or [])
    history = [item for item in history if isinstance(item, dict)]
    history.sort(key=lambda item: parse_time(str(item.get("started_at") or "")) or min_time())
    first_stage = history[0] if history else {}

    timestamps = {
        "solver_candidate_ready_at": event_time_text(solver_candidate_ready),
        "submit_prepared_at": event_time_text(submit_prepared),
        "selected_at": event_time_text(selected),
        "local_dispatch_started_at": event_time_text(local_dispatch),
        "engine_outbox_enqueued_at": event_time_text(outbox),
        "engine_accept_started_at": str((accept_started or {}).get("started_at") or event_time_text(accept_started)),
        "engine_admitted_observed_at": str((admitted or {}).get("accept_completed_at") or event_time_text(admitted)),
        "b_request_observed_at": str((admitted or {}).get("b_request_observed_at") or ""),
        "b_accepted_at": str((admitted or {}).get("accepted_at") or ""),
        "b_first_stage_started_at": str(first_stage.get("started_at") or ""),
        "b_terminal_at": str((returned or {}).get("terminal_at") or ""),
        "b_return_ready_at": str(
            (returned or {}).get("return_ready_at")
            or (returned or {}).get("terminal_at")
            or ""
        ),
        "b_snapshot_observed_at": str((returned or {}).get("engine_observed_at") or ""),
        "b_return_export_started_at": str(
            (returned or {}).get("b_return_export_started_at") or ""
        ),
        "b_return_export_finished_at": str(
            (returned or {}).get("b_return_export_finished_at") or ""
        ),
        "snapshot_started_at": str((returned or {}).get("snapshot_started_at") or ""),
        "snapshot_completed_at": str((returned or {}).get("snapshot_completed_at") or ""),
        "engine_return_observed_at": event_time_text(returned),
        "engine_ingest_started_at": str((ingest_started or {}).get("started_at") or event_time_text(ingest_started)),
        "engine_ingested_at": str((ingested or {}).get("ingested_at") or event_time_text(ingested)),
        "result_archived_at": event_time_text(result_archived),
        "solver_relay_ready_at": str((relay_claimed or {}).get("plan_updated_at") or ""),
        "solver_relay_claimed_at": str((relay_claimed or {}).get("claimed_at") or event_time_text(relay_claimed)),
        "solver_turn_started_at": str((relay_completed or {}).get("delivery_started_at") or (relay_completed or {}).get("completed_at") or event_time_text(relay_completed)),
    }
    durations = build_durations(timestamps, admitted, returned, history)
    local_order = (
        "solver_candidate_ready_at",
        "submit_prepared_at",
        "local_dispatch_started_at",
        "engine_outbox_enqueued_at",
        "engine_accept_started_at",
        "engine_admitted_observed_at",
        "snapshot_started_at",
        "snapshot_completed_at",
        "engine_return_observed_at",
        "engine_ingest_started_at",
        "engine_ingested_at",
        "result_archived_at",
        "solver_relay_ready_at",
        "solver_relay_claimed_at",
        "solver_turn_started_at",
    )
    b_order = (
        "b_request_observed_at",
        "b_accepted_at",
        "b_first_stage_started_at",
        "b_terminal_at",
        "b_return_ready_at",
        "b_snapshot_observed_at",
        "b_return_export_started_at",
        "b_return_export_finished_at",
    )
    order_violations = validate_timestamp_order(timestamps, local_order, "a-side")
    order_violations.extend(validate_timestamp_order(timestamps, b_order, "b-side"))

    qualified = result_is_qualified(root, op, version)
    terminal_state = str((returned or {}).get("terminal_state") or "")
    anchor = parse_time(timestamps["engine_outbox_enqueued_at"])
    strict_eligible = bool(
        strict_since is not None
        and anchor is not None
        and anchor >= strict_since
        and terminal_state == "completed"
        and qualified
    )
    missing = [name for name in STRICT_REQUIRED_TIMESTAMPS if not timestamps.get(name)]
    missing_observations = []
    if not has_local_transport_timeline(admitted):
        missing_observations.append("admission_gitpartner_local_timeline")
    if not has_local_transport_timeline(returned):
        missing_observations.append("return_gitpartner_local_timeline")
    threshold_violations = [
        {
            "metric": name,
            "value_seconds": value,
            "threshold_seconds": thresholds[name],
        }
        for name, value in durations.items()
        if name in thresholds and value is not None and value >= thresholds[name]
    ]
    components = blocker_components(durations, history)
    controllable = [item for item in components if item[0] != "b_device"]
    bottleneck = max(controllable, key=lambda item: item[1], default=("", 0.0))
    correlation_record = returned or admitted or outbox or {}
    runtime_identity = result_runtime_identity(root, op, version)
    own_service_seconds = critical_path_service_seconds(history)
    resource_seconds = resource_service_seconds(history)
    return {
        "op": op,
        "test_version": version,
        "request_id": str(correlation_record.get("request_id") or ""),
        "engine_job_id": str(correlation_record.get("engine_job_id") or version),
        "attempt_id": str(correlation_record.get("attempt_id") or ""),
        "terminal_state": terminal_state,
        "qualified_result": qualified,
        "correlation_quality": "exact-engine-job-and-test-version",
        "runtime_identity": runtime_identity,
        "timestamps": timestamps,
        "durations_seconds": durations,
        "stage_durations_seconds": stage_duration_summary(history),
        "stage_queue_wait_seconds": round(stage_queue_wait_seconds(history), 6),
        "own_critical_path_service_seconds": round(own_service_seconds, 6),
        "own_resource_service_seconds": resource_seconds,
        "device_intervals": serialized_device_intervals(
            history,
            job_id=str(correlation_record.get("engine_job_id") or version),
        ),
        "parallelism": stage_parallelism_summary(history),
        "missing_required_timestamps": missing,
        "missing_required_observations": missing_observations,
        "timestamp_order_violations": order_violations,
        "strict_eligible": strict_eligible,
        "strict_complete": (
            strict_eligible
            and not missing
            and not missing_observations
            and not order_violations
        ),
        "threshold_violations": threshold_violations,
        "controllable_bottleneck": {
            "component": bottleneck[0],
            "seconds": round(bottleneck[1], 6),
        },
        "component_seconds": {name: round(value, 6) for name, value in components},
        "solver_turn_id": str((relay_completed or {}).get("turn_id") or ""),
        "relay_entry_id": str((relay_claimed or {}).get("entry_id") or ""),
    }


def build_durations(
    timestamps: dict[str, str],
    admitted: dict[str, Any] | None,
    returned: dict[str, Any] | None,
    history: list[dict[str, Any]],
) -> dict[str, float | None]:
    durations: dict[str, float | None] = {
        "solver_candidate_ready_to_submit_prepared": duration(
            timestamps, "solver_candidate_ready_at", "submit_prepared_at"
        ),
        "solver_candidate_ready_to_local_dispatch": duration(
            timestamps, "solver_candidate_ready_at", "local_dispatch_started_at"
        ),
        "submit_prepared_to_local_dispatch": duration(
            timestamps, "submit_prepared_at", "local_dispatch_started_at"
        ),
        "selected_to_local_dispatch": duration(timestamps, "selected_at", "local_dispatch_started_at"),
        "local_dispatch_to_engine_outbox": duration(
            timestamps, "local_dispatch_started_at", "engine_outbox_enqueued_at"
        ),
        "engine_outbox_to_accept_start": duration(
            timestamps, "engine_outbox_enqueued_at", "engine_accept_started_at"
        ),
        "engine_accept_transport": numeric((admitted or {}).get("transport_elapsed_seconds")),
        "b_request_to_accept": duration(
            timestamps, "b_request_observed_at", "b_accepted_at"
        ),
        "b_accept_to_first_stage": duration(
            timestamps, "b_accepted_at", "b_first_stage_started_at"
        ),
        "b_accept_to_terminal": duration(
            timestamps, "b_accepted_at", "b_terminal_at"
        ),
        "b_first_stage_to_terminal": duration(
            timestamps, "b_first_stage_started_at", "b_terminal_at"
        ),
        "b_terminal_to_snapshot": duration(
            timestamps, "b_terminal_at", "b_snapshot_observed_at"
        ),
        "b_terminal_to_return_export_start": duration(
            timestamps, "b_terminal_at", "b_return_export_started_at"
        ),
        "b_return_export_duration": duration(
            timestamps,
            "b_return_export_started_at",
            "b_return_export_finished_at",
        ),
        "snapshot_transport": numeric((returned or {}).get("transport_elapsed_seconds")),
        "return_to_ingest": duration(
            timestamps, "engine_return_observed_at", "engine_ingest_started_at"
        ),
        "ingest_duration": duration(
            timestamps, "engine_ingest_started_at", "engine_ingested_at"
        ),
        "ingest_to_result_archive": duration(
            timestamps, "engine_ingested_at", "result_archived_at"
        ),
        "result_to_relay_ready": duration(
            timestamps, "result_archived_at", "solver_relay_ready_at"
        ),
        "relay_ready_to_claim": duration(
            timestamps, "solver_relay_ready_at", "solver_relay_claimed_at"
        ),
        "relay_claim_to_solver_turn": duration(
            timestamps, "solver_relay_claimed_at", "solver_turn_started_at"
        ),
        "result_to_solver_turn": duration(
            timestamps, "result_archived_at", "solver_turn_started_at"
        ),
        "local_dispatch_to_result_archive": duration(
            timestamps, "local_dispatch_started_at", "result_archived_at"
        ),
        "local_dispatch_to_solver_turn": duration(
            timestamps, "local_dispatch_started_at", "solver_turn_started_at"
        ),
        "submit_prepared_to_result_archive": duration(
            timestamps, "submit_prepared_at", "result_archived_at"
        ),
        "submit_prepared_to_solver_turn": duration(
            timestamps, "submit_prepared_at", "solver_turn_started_at"
        ),
        "solver_candidate_ready_to_result_archive": duration(
            timestamps, "solver_candidate_ready_at", "result_archived_at"
        ),
        "solver_candidate_ready_to_solver_turn": duration(
            timestamps, "solver_candidate_ready_at", "solver_turn_started_at"
        ),
    }
    durations.update(
        {f"stage:{name}": value for name, value in stage_duration_summary(history).items()}
    )
    durations.update(local_transport_breakdown(admitted, "gp_ingress"))
    durations.update(local_transport_breakdown(returned, "gp_return"))
    return durations


def blocker_components(
    durations: dict[str, float | None], history: list[dict[str, Any]]
) -> list[tuple[str, float]]:
    resources: dict[str, float] = defaultdict(float)
    for item in history:
        resource = str(item.get("stage_resource") or "host")
        started = parse_time(str(item.get("started_at") or ""))
        finished = parse_time(str(item.get("finished_at") or ""))
        if started is not None and finished is not None:
            resources[resource] += max(
                0.0, (finished - started).total_seconds()
            )
    components = [
        ("candidate_wait", value(durations, "submit_prepared_to_local_dispatch")),
        (
            "local_scheduler",
            value(durations, "local_dispatch_to_engine_outbox")
            + value(durations, "engine_outbox_to_accept_start"),
        ),
        (
            "b_queue",
            value(durations, "b_accept_to_first_stage") + stage_queue_wait_seconds(history),
        ),
        ("b_host", resources.get("host", 0.0)),
        ("b_device", resources.get("device", 0.0)),
        ("b_export", resources.get("export", 0.0)),
        ("return_detection", value(durations, "b_terminal_to_snapshot")),
        (
            "return_packaging",
            value(durations, "b_terminal_to_return_export_start")
            + value(durations, "b_return_export_duration"),
        ),
        (
            "local_ingest",
            value(durations, "return_to_ingest")
            + value(durations, "ingest_duration")
            + value(durations, "ingest_to_result_archive"),
        ),
        ("trigger_generation", value(durations, "result_to_relay_ready")),
        (
            "ide_relay",
            value(durations, "relay_ready_to_claim")
            + value(durations, "relay_claim_to_solver_turn"),
        ),
    ]
    components.extend(
        transport_blocker_components(
            durations, "gp_ingress", "engine_accept_transport"
        )
    )
    components.extend(
        transport_blocker_components(durations, "gp_return", "snapshot_transport")
    )
    return components


def has_local_transport_timeline(record: dict[str, Any] | None) -> bool:
    timeline = (record or {}).get("local_transport_timeline")
    return bool(
        isinstance(timeline, dict)
        and timeline.get("protocol_version") == "gitpartner-local-timeline-v1"
        and isinstance(timeline.get("steps"), list)
    )


def local_transport_breakdown(
    record: dict[str, Any] | None, prefix: str
) -> dict[str, float | None]:
    if not has_local_transport_timeline(record):
        return {}
    timeline = dict((record or {}).get("local_transport_timeline") or {})
    by_name: dict[str, float] = defaultdict(float)
    wait_fetch_merge = 0.0
    wait_sleep = 0.0
    wait_duration = 0.0
    for step in timeline.get("steps", []):
        if not isinstance(step, dict):
            continue
        name = str(step.get("name") or "")
        step_duration = numeric(step.get("duration_seconds"))
        if not name or step_duration is None:
            continue
        by_name[name] += max(0.0, step_duration)
        if name == "wait_for_result":
            wait_duration += max(0.0, step_duration)
            wait_fetch_merge += max(0.0, numeric(step.get("fetch_merge_seconds")) or 0.0)
            wait_sleep += max(0.0, numeric(step.get("sleep_seconds")) or 0.0)
    prepare_names = ("prepare_payload", "build_job", "write_job")
    publish_detail_names = (
        "publish_fetch",
        "publish_fast_forward",
        "publish_rebase",
    )
    publish_tail_names = ("publish_add", "publish_commit", "publish_push")
    prepare = sum(by_name.get(name, 0.0) for name in prepare_names)
    detailed_sync = sum(by_name.get(name, 0.0) for name in publish_detail_names)
    publish_sync = detailed_sync or by_name.get("publish_sync", 0.0)
    publish = publish_sync + sum(by_name.get(name, 0.0) for name in publish_tail_names)
    wait_other = max(0.0, wait_duration - wait_fetch_merge - wait_sleep)
    total = max(0.0, numeric(timeline.get("total_seconds")) or 0.0)
    measured_steps = sum(by_name.values())
    local_unattributed = max(0.0, total - measured_steps)
    adapter_total = max(0.0, numeric((record or {}).get("transport_elapsed_seconds")) or 0.0)
    adapter_overhead = max(0.0, adapter_total - total)
    result: dict[str, float | None] = {
        f"{prefix}_local_total": total,
        f"{prefix}_prepare": prepare,
        f"{prefix}_publish": publish,
        f"{prefix}_wait_fetch_merge": wait_fetch_merge,
        f"{prefix}_wait_sleep": wait_sleep,
        f"{prefix}_wait_other": wait_other,
        f"{prefix}_local_unattributed": local_unattributed,
        f"{prefix}_adapter_overhead": adapter_overhead,
        f"{prefix}_publish_sync": publish_sync,
    }
    for name in (*prepare_names, *publish_detail_names, *publish_tail_names):
        result[f"{prefix}_{name}"] = by_name.get(name, 0.0)
    return result


def transport_blocker_components(
    durations: dict[str, float | None], prefix: str, fallback_metric: str
) -> list[tuple[str, float]]:
    if durations.get(f"{prefix}_local_total") is None:
        return [(prefix, value(durations, fallback_metric))]
    return [
        (f"{prefix}_prepare", value(durations, f"{prefix}_prepare")),
        (f"{prefix}_publish", value(durations, f"{prefix}_publish")),
        (
            f"{prefix}_wait_fetch_merge",
            value(durations, f"{prefix}_wait_fetch_merge"),
        ),
        (f"{prefix}_wait_sleep", value(durations, f"{prefix}_wait_sleep")),
        (f"{prefix}_wait_other", value(durations, f"{prefix}_wait_other")),
        (
            f"{prefix}_unattributed",
            value(durations, f"{prefix}_local_unattributed")
            + value(durations, f"{prefix}_adapter_overhead"),
        ),
    ]


def aggregate_samples(
    samples: list[dict[str, Any]], thresholds: dict[str, float]
) -> dict[str, Any]:
    metrics: dict[str, list[float]] = defaultdict(list)
    bottlenecks: Counter[str] = Counter()
    missing: Counter[str] = Counter()
    missing_observations: Counter[str] = Counter()
    for sample in samples:
        for name, raw in dict(sample.get("durations_seconds") or {}).items():
            number = numeric(raw)
            if number is not None:
                metrics[name].append(number)
        bottleneck = str(
            dict(sample.get("controllable_bottleneck") or {}).get("component") or ""
        )
        if bottleneck:
            bottlenecks[bottleneck] += 1
        missing.update(str(item) for item in sample.get("missing_required_timestamps", []))
        missing_observations.update(
            str(item) for item in sample.get("missing_required_observations", [])
        )
    return {
        "sample_count": len(samples),
        "strict_eligible_count": sum(bool(item.get("strict_eligible")) for item in samples),
        "strict_complete_count": sum(bool(item.get("strict_complete")) for item in samples),
        "metrics_seconds": {
            name: summarize(values, thresholds.get(name)) for name, values in sorted(metrics.items())
        },
        "controllable_bottleneck_counts": dict(bottlenecks.most_common()),
        "missing_timestamp_counts": dict(missing.most_common()),
        "missing_observation_counts": dict(missing_observations.most_common()),
        "recent_10": [
            {
                "op": item.get("op"),
                "test_version": item.get("test_version"),
                "strict_complete": item.get("strict_complete"),
                "result_to_solver_turn_seconds": dict(item.get("durations_seconds") or {}).get(
                    "result_to_solver_turn"
                ),
                "candidate_ready_to_solver_turn_seconds": dict(
                    item.get("durations_seconds") or {}
                ).get("solver_candidate_ready_to_solver_turn"),
                "own_critical_path_service_seconds": item.get(
                    "own_critical_path_service_seconds"
                ),
                "external_stage_queue_wait_seconds": item.get(
                    "stage_queue_wait_seconds"
                ),
                "bottleneck": dict(item.get("controllable_bottleneck") or {}).get("component"),
            }
            for item in samples[-10:]
        ],
    }


def current_runtime_cohort(root: Path, config: DaemonConfig) -> dict[str, Any]:
    admission = read_json(
        root / "TestUtils" / "tester_daemon" / "engine_admission_state.json"
    )
    live_snapshot = admission.get("last_engine_snapshot", {})
    live_snapshot = live_snapshot if isinstance(live_snapshot, dict) else {}
    promotion = read_json(
        root / "TestUtils" / "tester_daemon" / "engine_promotion_latest.json"
    )
    throughput = promotion.get("throughput", {})
    throughput = throughput if isinstance(throughput, dict) else {}
    remote_generation = str(
        live_snapshot.get("engine_code_generation")
        or throughput.get("remote_engine_code_generation")
        or promotion.get("remote_engine_code_generation")
        or ""
    )
    return {
        "engine_code_generation": remote_generation,
        "local_engine_code_generation": str(promotion.get("engine_code_generation") or ""),
        "execution_profile": str(
            config.policy.get("test_engine_execution_profile") or ""
        ),
        "generation_source": (
            "engine_admission_state"
            if live_snapshot.get("engine_code_generation")
            else "engine_promotion_latest"
            if remote_generation
            else "unavailable"
        ),
        "filter_active": bool(remote_generation),
    }


def sample_matches_runtime_cohort(
    sample: dict[str, Any], cohort: dict[str, Any]
) -> bool:
    if not cohort.get("filter_active"):
        return True
    identity = dict(sample.get("runtime_identity") or {})
    generation = str(cohort.get("engine_code_generation") or "")
    profile = str(cohort.get("execution_profile") or "")
    if generation and str(identity.get("engine_code_generation") or "") != generation:
        return False
    if profile and str(identity.get("execution_profile") or "") != profile:
        return False
    return True


def result_runtime_identity(root: Path, op: str, version: str) -> dict[str, Any]:
    output = root / "operators_testresult" / op / version / "gitpartner_output"
    state = read_json(output / "state.json")
    result_root = output / "result_bundle" / "result"
    case_cache = read_json(result_root / "CASE_CACHE.json")
    operator_cache = read_json(result_root / "OPERATOR_CACHE.json")
    case_hit = optional_bool(case_cache, "cache_hit")
    operator_hit = optional_bool(operator_cache, "cache_hit")
    if case_hit is True and operator_hit is True:
        cache_state = "hot"
    elif case_hit is True and operator_hit is False:
        cache_state = "operator-cold"
    elif case_hit is False and operator_hit is True:
        cache_state = "case-cold"
    elif case_hit is False and operator_hit is False:
        cache_state = "cold"
    else:
        cache_state = "unknown"
    case_timing = case_cache.get("timing_seconds", {})
    case_timing = case_timing if isinstance(case_timing, dict) else {}
    operator_timing = operator_cache.get("timing_seconds", {})
    operator_timing = operator_timing if isinstance(operator_timing, dict) else {}
    return {
        "engine_code_generation": str(state.get("engine_code_generation") or ""),
        "execution_profile": str(state.get("execution_profile") or ""),
        "case_cache_hit": case_hit,
        "operator_cache_hit": operator_hit,
        "cache_state": cache_state,
        "case_cache_payload_bytes": int(case_cache.get("payload_bytes", 0) or 0),
        "case_cache_total_seconds": numeric(case_timing.get("total")),
        "case_cache_population_seconds": numeric(case_timing.get("population")),
        "operator_cache_total_seconds": numeric(operator_timing.get("total")),
        "operator_cache_population_seconds": numeric(operator_timing.get("population")),
    }


def optional_bool(payload: dict[str, Any], key: str) -> bool | None:
    if key not in payload:
        return None
    return bool(payload.get(key))


def summarize(values: list[float], threshold: float | None) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50": round(percentile(ordered, 50), 6),
        "p95": round(percentile(ordered, 95), 6),
        "max": round(max(ordered), 6),
        "threshold": threshold,
        "violation_count": (
            sum(value >= threshold for value in ordered) if threshold is not None else 0
        ),
    }


def render_full_flow_latency(report: dict[str, Any]) -> str:
    gate = dict(report.get("gate") or {})
    aggregate = dict(report.get("aggregate") or {})
    cohort = dict(report.get("current_runtime_cohort") or {})
    device_flow = dict(report.get("device_flow") or {})
    handoff_summary = dict(device_flow.get("handoff_gap_seconds") or {})
    lines = [
        "# Full Flow Latency",
        "",
        f"- generated_at: `{report.get('generated_at', '-')}`",
        f"- schema: `{report.get('schema_version', '-')}`",
        f"- gate: `{gate.get('status', '-')}`",
        f"- strict samples: `{gate.get('strict_complete_count', 0)}/{gate.get('eligible_sample_count', 0)}`",
        f"- violations: `{gate.get('violation_count', 0)}`",
        f"- current engine generation: `{cohort.get('engine_code_generation', '-') or '-'}`",
        f"- current execution profile: `{cohort.get('execution_profile', '-') or '-'}`",
        f"- current/historical samples: `{cohort.get('sample_count', 0)}/{cohort.get('historical_sample_count', 0)}`",
        f"- current cache states: `{json.dumps(cohort.get('cache_state_counts', {}), ensure_ascii=True, sort_keys=True)}`",
        "- clock rule: never subtract A-side and B-side wall-clock timestamps",
        "",
        "## Engine Service And NPU Flow",
        "",
        "- single-task primary metric: own DAG critical-path service time; excludes waits for other jobs",
        f"- device intervals/handoffs: `{device_flow.get('device_interval_count', 0)}/{device_flow.get('handoff_count', 0)}`",
        f"- device busy/window seconds: `{display(device_flow.get('device_busy_seconds'))}/{display(device_flow.get('device_window_seconds'))}`",
        f"- device utilization: `{display(device_flow.get('device_utilization_ratio'))}`",
        f"- NPU handoff gap p50/p95/max seconds: `{display(handoff_summary.get('p50'))}/{display(handoff_summary.get('p95'))}/{display(handoff_summary.get('max'))}`",
        "",
        "## Bottlenecks",
        "",
    ]
    bottlenecks = dict(aggregate.get("controllable_bottleneck_counts") or {})
    if bottlenecks:
        lines.extend(f"- `{name}`: {count}" for name, count in bottlenecks.items())
    else:
        lines.append("- none")
    lines.extend(["", "## Metrics", "", "| metric | n | p50 s | p95 s | max s | limit s | violations |", "|---|---:|---:|---:|---:|---:|---:|"])
    for name, item in dict(aggregate.get("metrics_seconds") or {}).items():
        if not isinstance(item, dict):
            continue
        lines.append(
            f"| {name} | {item.get('count', 0)} | {display(item.get('p50'))} | "
            f"{display(item.get('p95'))} | {display(item.get('max'))} | "
            f"{display(item.get('threshold'))} | {item.get('violation_count', 0)} |"
        )
    lines.extend(["", "## Recent Current-Cohort Tasks", "", "| op | version | cache | strict | own service s | external stage wait s | NPU busy s | accepted -> terminal s (diagnostic) | candidate -> Solver s | result -> Solver s | bottleneck | missing |", "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|"])
    current_samples = [
        item
        for item in list(report.get("samples") or [])
        if isinstance(item, dict) and item.get("current_cohort")
    ]
    for sample in current_samples[-10:]:
        if not isinstance(sample, dict):
            continue
        durations = dict(sample.get("durations_seconds") or {})
        runtime = dict(sample.get("runtime_identity") or {})
        resource_service = dict(sample.get("own_resource_service_seconds") or {})
        missing_items = list(sample.get("missing_required_timestamps", []))
        missing_items.extend(sample.get("missing_required_observations", []))
        missing = ", ".join(str(item) for item in missing_items) or "-"
        lines.append(
            f"| {sample.get('op', '-')} | {sample.get('test_version', '-')} | "
            f"{runtime.get('cache_state', 'unknown')} | {sample.get('strict_complete', False)} | "
            f"{display(sample.get('own_critical_path_service_seconds'))} | "
            f"{display(sample.get('stage_queue_wait_seconds'))} | "
            f"{display(resource_service.get('device'))} | "
            f"{display(durations.get('b_accept_to_terminal'))} | "
            f"{display(durations.get('solver_candidate_ready_to_solver_turn'))} | "
            f"{display(durations.get('result_to_solver_turn'))} | "
            f"{dict(sample.get('controllable_bottleneck') or {}).get('component', '-')} | {missing} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_full_flow_latency(root: Path, report: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / FULL_FLOW_JSON).write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (state_dir / FULL_FLOW_MD).write_text(
        render_full_flow_latency(report), encoding="utf-8"
    )


def configured_thresholds(config: DaemonConfig) -> dict[str, float]:
    result = dict(DEFAULT_THRESHOLDS_SECONDS)
    raw = config.policy.get("full_flow_thresholds_seconds", {})
    if isinstance(raw, dict):
        for name, candidate in raw.items():
            number = numeric(candidate)
            if name in result and number is not None and number >= 0:
                result[str(name)] = number
    return result


def result_solver_relay(
    events: list[dict[str, Any]], result_at: datetime | None
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    result_gate = [
        item
        for item in events
        if str(item.get("gate_stage") or "") == "result-exists"
        and (result_at is None or event_datetime(item) is None or event_datetime(item) >= result_at)
    ]
    claimed = first_event(
        [item for item in result_gate if str(item.get("event") or "") == "native_relay_claimed"]
    )
    if claimed is None:
        return None, None
    key = str(claimed.get("key") or "")
    completed = first_event(
        [
            item
            for item in result_gate
            if str(item.get("event") or "") == "native_relay_completed"
            and str(item.get("key") or "") == key
            and (
                event_datetime(claimed) is None
                or event_datetime(item) is None
                or event_datetime(item) >= event_datetime(claimed)
            )
        ]
    )
    return claimed, completed


def current_relay_outbox_events(root: Path) -> list[dict[str, Any]]:
    payload = read_json(root / "TestUtils" / "tester_daemon" / "native_relay_outbox.json")
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    result = []
    for item in entries if isinstance(entries, list) else []:
        if not isinstance(item, dict):
            continue
        record = dict(item)
        record.setdefault("event", "native_relay_ready")
        record.setdefault("time", record.get("updated_at") or record.get("created_at") or "")
        result.append(record)
    return result


def matching_accept_started(
    events: list[dict[str, Any]], admitted: dict[str, Any] | None
) -> dict[str, Any] | None:
    candidates = events_of_kind(events, "engine_accept_started")
    request_id = str((admitted or {}).get("transport_request_id") or "")
    if request_id:
        matching = [
            item for item in candidates if str(item.get("transport_request_id") or "") == request_id
        ]
        if matching:
            return latest_event(matching)
    return latest_before(candidates, event_datetime(admitted) if admitted else None)


def validate_timestamp_order(
    timestamps: dict[str, str], names: Iterable[str], clock: str
) -> list[str]:
    violations: list[str] = []
    previous_name = ""
    previous_time: datetime | None = None
    for name in names:
        current = parse_time(str(timestamps.get(name) or ""))
        if current is None:
            continue
        if previous_time is not None and current < previous_time:
            violations.append(f"{clock}:{previous_name}>{name}")
        previous_name = name
        previous_time = current
    return violations


def stage_duration_summary(history: list[dict[str, Any]]) -> dict[str, float]:
    result: dict[str, float] = {}
    for item in history:
        name = str(item.get("stage_name") or "")
        started = parse_time(str(item.get("started_at") or ""))
        finished = parse_time(str(item.get("finished_at") or ""))
        if name and started is not None and finished is not None:
            result[name] = round(
                result.get(name, 0.0)
                + max(0.0, (finished - started).total_seconds()),
                6,
            )
    return result


def stage_queue_wait_seconds(history: list[dict[str, Any]]) -> float:
    if history and all("depends_on" in item for item in history):
        successful_finishes: dict[str, datetime] = {}
        for item in history:
            if int(item.get("exit_code", 0) or 0) != 0:
                continue
            name = str(item.get("stage_name") or "")
            finished = parse_time(str(item.get("finished_at") or ""))
            if not name or finished is None:
                continue
            previous = successful_finishes.get(name)
            if previous is None or finished > previous:
                successful_finishes[name] = finished

        total = 0.0
        for item in history:
            if int(item.get("exit_code", 0) or 0) != 0:
                continue
            dependencies = [
                str(name)
                for name in item.get("depends_on", [])
                if str(name)
            ]
            if not dependencies:
                continue
            dependency_finishes = [
                successful_finishes[name]
                for name in dependencies
                if name in successful_finishes
            ]
            if len(dependency_finishes) != len(dependencies):
                continue
            started = parse_time(str(item.get("started_at") or ""))
            ready_at = max(dependency_finishes)
            if started is not None and started > ready_at:
                total += (started - ready_at).total_seconds()
        return max(0.0, total)

    # Old returned bundles predate explicit DAG metadata and are sequential.
    total = 0.0
    previous_finished: datetime | None = None
    for item in history:
        started = parse_time(str(item.get("started_at") or ""))
        finished = parse_time(str(item.get("finished_at") or ""))
        if started is not None and previous_finished is not None and started > previous_finished:
            total += (started - previous_finished).total_seconds()
        if finished is not None:
            previous_finished = finished
    return max(0.0, total)


def stage_parallelism_summary(history: list[dict[str, Any]]) -> dict[str, Any]:
    resources = Counter(str(item.get("stage_resource") or "host") for item in history)
    locks = Counter(
        str(lock)
        for item in history
        for lock in (item.get("stage_locks") if isinstance(item.get("stage_locks"), list) else [])
    )
    return {"stage_count_by_resource": dict(resources), "lock_count": dict(locks)}


def compact_violation(sample: dict[str, Any]) -> dict[str, Any]:
    return {
        "op": sample.get("op"),
        "test_version": sample.get("test_version"),
        "missing_required_timestamps": sample.get("missing_required_timestamps", []),
        "missing_required_observations": sample.get(
            "missing_required_observations", []
        ),
        "timestamp_order_violations": sample.get("timestamp_order_violations", []),
        "threshold_violations": sample.get("threshold_violations", []),
        "controllable_bottleneck": sample.get("controllable_bottleneck", {}),
    }


def result_is_qualified(root: Path, op: str, version: str) -> bool:
    path = root / "operators_testresult" / op / version / "RESULT.md"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    match = QUALIFIED_RESULT_RE.search(text)
    if not match:
        return False
    verdict = match.group(1).strip().upper()
    return not verdict.startswith(NON_QUALIFIED_RESULT_PREFIXES)


def events_of_kind(events: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [item for item in events if str(item.get("kind") or "") == kind]


def latest_kind(events: list[dict[str, Any]], kind: str) -> dict[str, Any] | None:
    return latest_event(events_of_kind(events, kind))


def latest_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid = [item for item in events if event_datetime(item) is not None]
    return max(valid, key=lambda item: event_datetime(item) or min_time(), default=None)


def first_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid = [item for item in events if event_datetime(item) is not None]
    return min(valid, key=lambda item: event_datetime(item) or min_time(), default=None)


def latest_before(
    events: list[dict[str, Any]], cutoff: datetime | None
) -> dict[str, Any] | None:
    if cutoff is None:
        return latest_event(events)
    return latest_event(
        [item for item in events if event_datetime(item) is not None and event_datetime(item) <= cutoff]
    )


def event_datetime(event: dict[str, Any] | None) -> datetime | None:
    if not isinstance(event, dict):
        return None
    return parse_time(
        str(
            event.get("time")
            or event.get("completed_at")
            or event.get("claimed_at")
            or event.get("updated_at")
            or ""
        )
    )


def event_time_text(event: dict[str, Any] | None) -> str:
    if not isinstance(event, dict):
        return ""
    return str(
        event.get("time")
        or event.get("completed_at")
        or event.get("claimed_at")
        or event.get("updated_at")
        or ""
    )


def duration(timestamps: dict[str, str], start: str, end: str) -> float | None:
    left = parse_time(str(timestamps.get(start) or ""))
    right = parse_time(str(timestamps.get(end) or ""))
    if left is None or right is None or right < left:
        return None
    return round((right - left).total_seconds(), 6)


def value(durations: dict[str, float | None], name: str) -> float:
    raw = durations.get(name)
    return float(raw) if raw is not None else 0.0


def numeric(raw: object) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def percentile(ordered: list[float], percent: float) -> float:
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percent / 100.0
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def sample_sort_key(sample: dict[str, Any]) -> tuple[datetime, str, str]:
    timestamps = dict(sample.get("timestamps") or {})
    anchor = (
        parse_time(str(timestamps.get("result_archived_at") or ""))
        or parse_time(str(timestamps.get("engine_return_observed_at") or ""))
        or min_time()
    )
    return anchor, str(sample.get("op") or ""), str(sample.get("test_version") or "")


def parse_time(text: str) -> datetime | None:
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def min_time() -> datetime:
    return datetime.min.replace(tzinfo=timezone.utc)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        stat = path.stat()
    except OSError:
        return []
    cached = _JSONL_CACHE.get(path)
    if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return [dict(item) for item in cached[2]]
    result: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    for line in lines:
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            result.append(item)
    _JSONL_CACHE[path] = (stat.st_mtime_ns, stat.st_size, result)
    return [dict(item) for item in result]


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def display(raw: object) -> str:
    if raw is None or raw == "":
        return "-"
    if isinstance(raw, float):
        return f"{raw:.3f}"
    return str(raw)
