from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


def parse_engine_time(value: object) -> datetime | None:
    text = str(value or "")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def stage_duration_seconds(stage: dict[str, Any]) -> float:
    started = parse_engine_time(stage.get("started_at"))
    finished = parse_engine_time(stage.get("finished_at"))
    if started is None or finished is None:
        return 0.0
    return max(0.0, (finished - started).total_seconds())


def critical_path_service_seconds(history: list[dict[str, Any]]) -> float:
    """Return own DAG service time without external scheduler/resource waits."""
    valid = [
        item
        for item in history
        if str(item.get("stage_name") or "") and stage_duration_seconds(item) >= 0
    ]
    if not valid:
        return 0.0
    if not all("depends_on" in item for item in valid):
        return sum(stage_duration_seconds(item) for item in valid)

    durations: dict[str, float] = defaultdict(float)
    dependencies: dict[str, list[str]] = {}
    order: list[str] = []
    for item in valid:
        name = str(item.get("stage_name") or "")
        if name not in durations:
            order.append(name)
        durations[name] += stage_duration_seconds(item)
        dependencies[name] = [
            str(value) for value in item.get("depends_on", []) if str(value)
        ]

    memo: dict[str, float] = {}
    visiting: set[str] = set()

    def finish_time(name: str) -> float:
        if name in memo:
            return memo[name]
        if name in visiting:
            return durations.get(name, 0.0)
        visiting.add(name)
        parent_time = max(
            (finish_time(parent) for parent in dependencies.get(name, [])),
            default=0.0,
        )
        visiting.discard(name)
        memo[name] = parent_time + durations.get(name, 0.0)
        return memo[name]

    return max((finish_time(name) for name in order), default=0.0)


def resource_service_seconds(history: list[dict[str, Any]]) -> dict[str, float]:
    result: dict[str, float] = defaultdict(float)
    for item in history:
        result[str(item.get("stage_resource") or "host")] += stage_duration_seconds(
            item
        )
    return {name: round(value, 6) for name, value in sorted(result.items())}


def task_timing_sample(
    history: list[dict[str, Any]],
    *,
    job_id: str,
    request_id: str = "",
) -> dict[str, Any]:
    starts = [
        parsed
        for item in history
        if (parsed := parse_engine_time(item.get("started_at"))) is not None
    ]
    finishes = [
        parsed
        for item in history
        if (parsed := parse_engine_time(item.get("finished_at"))) is not None
    ]
    service = critical_path_service_seconds(history)
    wall = (
        max(0.0, (max(finishes) - min(starts)).total_seconds())
        if starts and finishes
        else 0.0
    )
    return {
        "request_id": request_id,
        "engine_job_id": job_id,
        "own_critical_path_service_seconds": round(service, 6),
        "resource_queue_wait_seconds": round(max(0.0, wall - service), 6),
        "engine_residency_seconds": round(wall, 6),
        # Compatibility aliases for reports created before the metric split.
        "own_service_seconds": round(service, 6),
        "wall_seconds": round(wall, 6),
        "external_wait_seconds": round(max(0.0, wall - service), 6),
        "resource_service_seconds": resource_service_seconds(history),
        "resource_intervals": serialized_resource_intervals(
            history,
            job_id=job_id,
        ),
        "device_intervals": serialized_device_intervals(
            history,
            job_id=job_id,
        ),
    }


def experiment_engine_timing_summary(
    samples: list[dict[str, Any]],
    *,
    handoff_threshold_seconds: float = 10.0,
    own_service_min_seconds: float = 20.0,
    own_service_max_seconds: float = 40.0,
) -> dict[str, Any]:
    flow = device_flow_summary(samples)
    resource_parallelism = resource_parallelism_summary(samples)
    services = [
        float(item.get("own_service_seconds", 0.0) or 0.0)
        for item in samples
    ]
    walls = [
        float(item.get("wall_seconds", 0.0) or 0.0)
        for item in samples
    ]
    waits = [
        float(item.get("external_wait_seconds", 0.0) or 0.0)
        for item in samples
    ]
    handoff_max = flow["handoff_gap_seconds"]["max"]
    handoff_violations = [
        item
        for item in flow.get("handoffs", [])
        if float(item.get("gap_seconds", 0.0) or 0.0)
        >= float(handoff_threshold_seconds)
    ]
    own_service_violations = [
        item
        for item in samples
        if not (
            float(own_service_min_seconds)
            <= float(item.get("own_service_seconds", 0.0) or 0.0)
            <= float(own_service_max_seconds)
        )
    ]
    return {
        "metric_contract": {
            "single_task_performance_metric": (
                "own_critical_path_service_seconds"
            ),
            "npu_utilization_metric": (
                "device_flow.per_device.<device_id>.handoff_gap_seconds"
            ),
            "queue_wait_metric": "resource_queue_wait_seconds",
            "diagnostic_only_metric": "engine_residency_seconds",
            "cross_task_device_queue_time_in_single_task_performance": False,
        },
        "sample_count": len(samples),
        "own_critical_path_service_seconds": summarize_values(services),
        "resource_queue_wait_seconds": summarize_values(waits),
        "engine_residency_seconds": summarize_values(walls),
        # Compatibility aliases for existing report consumers.
        "own_service_seconds": summarize_values(services),
        "wall_seconds": summarize_values(walls),
        "external_wait_seconds": summarize_values(waits),
        "resource_parallelism": resource_parallelism,
        "device_flow": flow,
        "acceptance": {
            "handoff_threshold_seconds": float(handoff_threshold_seconds),
            "handoff_observed": bool(flow["handoff_count"]),
            "handoff_violation_count": len(handoff_violations),
            "handoff_all_below_threshold": bool(
                flow["handoff_count"]
                and handoff_max is not None
                and float(handoff_max) < float(handoff_threshold_seconds)
            ),
            "own_service_range_seconds": [
                float(own_service_min_seconds),
                float(own_service_max_seconds),
            ],
            "own_service_violation_count": len(own_service_violations),
            "own_service_all_in_range": bool(
                services
                and all(
                    float(own_service_min_seconds)
                    <= value
                    <= float(own_service_max_seconds)
                    for value in services
                )
            ),
            "host_parallelism_observed": bool(
                resource_parallelism.get("host", {}).get(
                    "max_concurrency", 0
                )
                > 1
            ),
        },
        "recent_samples": samples[-10:],
    }


def serialized_resource_intervals(
    history: list[dict[str, Any]], *, job_id: str
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in history:
        started = parse_engine_time(item.get("started_at"))
        finished = parse_engine_time(item.get("finished_at"))
        if started is None or finished is None or finished < started:
            continue
        result.append(
            {
                "engine_job_id": job_id,
                "resource": str(item.get("stage_resource") or "host"),
                "stage_name": str(item.get("stage_name") or ""),
                "started_at": started.isoformat(),
                "finished_at": finished.isoformat(),
                "duration_seconds": round((finished - started).total_seconds(), 6),
            }
        )
    return result


def resource_parallelism_summary(
    samples: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        for interval in sample.get("resource_intervals", []):
            if isinstance(interval, dict):
                grouped[str(interval.get("resource") or "host")].append(interval)

    result: dict[str, dict[str, Any]] = {}
    for resource, intervals in sorted(grouped.items()):
        events: list[tuple[datetime, int]] = []
        service_seconds = 0.0
        for interval in intervals:
            started = parse_engine_time(interval.get("started_at"))
            finished = parse_engine_time(interval.get("finished_at"))
            if started is None or finished is None or finished < started:
                continue
            service_seconds += (finished - started).total_seconds()
            events.append((started, 1))
            events.append((finished, -1))
        events.sort(key=lambda item: (item[0], item[1]))
        concurrency = 0
        maximum = 0
        union_seconds = 0.0
        overlap_seconds = 0.0
        previous: datetime | None = None
        for timestamp, delta in events:
            if previous is not None and timestamp > previous:
                span = (timestamp - previous).total_seconds()
                if concurrency > 0:
                    union_seconds += span
                if concurrency > 1:
                    overlap_seconds += span
            concurrency += delta
            maximum = max(maximum, concurrency)
            previous = timestamp
        result[resource] = {
            "interval_count": len(intervals),
            "max_concurrency": maximum,
            "service_seconds": round(service_seconds, 6),
            "active_window_seconds": round(union_seconds, 6),
            "parallel_overlap_seconds": round(overlap_seconds, 6),
            "effective_parallelism": (
                round(service_seconds / union_seconds, 6)
                if union_seconds > 0
                else 0.0
            ),
        }
    return result


def serialized_device_intervals(
    history: list[dict[str, Any]], *, job_id: str
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
        started = parse_engine_time(item.get("started_at"))
        finished = parse_engine_time(item.get("finished_at"))
        if started is None or finished is None or finished < started:
            continue
        result.append(
            {
                "engine_job_id": job_id,
                "device_id": str(item.get("device_id") or "legacy"),
                "stage_name": str(item.get("stage_name") or ""),
                "started_at": started.isoformat(),
                "finished_at": finished.isoformat(),
                "duration_seconds": round((finished - started).total_seconds(), 6),
            }
        )
    return result


def device_flow_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    intervals = [
        dict(interval)
        for sample in samples
        for interval in sample.get("device_intervals", [])
        if isinstance(interval, dict)
    ]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for interval in intervals:
        grouped[str(interval.get("device_id") or "legacy")].append(interval)
    handoffs: list[dict[str, Any]] = []
    per_device: dict[str, dict[str, Any]] = {}
    total_busy = 0.0
    total_window = 0.0
    for device_id, device_intervals in sorted(grouped.items()):
        device_intervals.sort(
            key=lambda item: parse_engine_time(item.get("started_at"))
            or datetime.min.replace(tzinfo=timezone.utc)
        )
        device_handoffs: list[dict[str, Any]] = []
        for previous, current in zip(device_intervals, device_intervals[1:]):
            previous_finish = parse_engine_time(previous.get("finished_at"))
            current_start = parse_engine_time(current.get("started_at"))
            if previous_finish is None or current_start is None:
                continue
            gap = (current_start - previous_finish).total_seconds()
            if gap < 0:
                continue
            device_handoffs.append(
                {
                    "device_id": device_id,
                    "previous_engine_job_id": str(
                        previous.get("engine_job_id") or ""
                    ),
                    "next_engine_job_id": str(current.get("engine_job_id") or ""),
                    "previous_stage_name": str(previous.get("stage_name") or ""),
                    "next_stage_name": str(current.get("stage_name") or ""),
                    "gap_seconds": round(gap, 6),
                }
            )
        handoffs.extend(device_handoffs)
        busy = sum(
            float(item.get("duration_seconds", 0.0) or 0.0)
            for item in device_intervals
        )
        first = parse_engine_time(device_intervals[0].get("started_at"))
        last = parse_engine_time(device_intervals[-1].get("finished_at"))
        window = (
            max(0.0, (last - first).total_seconds())
            if first is not None and last is not None
            else 0.0
        )
        total_busy += busy
        total_window += window
        per_device[device_id] = {
            "device_interval_count": len(device_intervals),
            "handoff_count": len(device_handoffs),
            "device_busy_seconds": round(busy, 6),
            "device_window_seconds": round(window, 6),
            "device_utilization_ratio": (
                round(min(1.0, busy / window), 6) if window > 0 else 0.0
            ),
            "handoff_gap_seconds": summarize_values(
                [float(item["gap_seconds"]) for item in device_handoffs]
            ),
            "handoffs": device_handoffs,
            "recent_handoffs": device_handoffs[-10:],
        }
    gaps = [float(item["gap_seconds"]) for item in handoffs]
    return {
        "device_interval_count": len(intervals),
        "handoff_count": len(handoffs),
        "physical_device_count": len(per_device),
        "device_busy_seconds": round(total_busy, 6),
        "device_window_seconds": round(total_window, 6),
        "device_utilization_ratio": (
            round(min(1.0, total_busy / total_window), 6)
            if total_window > 0
            else 0.0
        ),
        "handoff_gap_seconds": summarize_values(gaps),
        "handoffs": handoffs,
        "recent_handoffs": handoffs[-10:],
        "per_device": per_device,
    }


def summarize_values(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50": round(percentile(ordered, 50), 6),
        "p95": round(percentile(ordered, 95), 6),
        "max": round(max(ordered), 6),
    }


def percentile(values: list[float], percent: float) -> float:
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * percent / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(values) - 1)
    fraction = rank - lower
    return values[lower] * (1.0 - fraction) + values[upper] * fraction
