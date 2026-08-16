from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import (
    BOOTSTRAP_CONTROL_PROBE_POLICY,
    ControlDatabase,
)
from ascendop_daemon.control_plane.endpoint_dispatcher import EndpointDispatcherPool
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.observability.engine_timing import (
    experiment_engine_timing_summary,
    task_timing_sample,
)
from ascendop_daemon.registry.system_registry import BackendEndpoint, SystemRegistry


EXPERIMENT_SCHEMA = "ascendop.distributed-experiment.v1"
TERMINAL_REQUEST_STATES = {"completed", "failed"}
SUSTAINED_AUTO_TRANSPORT_WINDOW = 32


def resolve_sustained_transport_capacity(
    requested_capacity: int,
    task_count: int,
) -> tuple[int, str]:
    requested = int(requested_capacity)
    count = int(task_count)
    if count < 1:
        raise ValueError("sustained acceptance requires at least one task")
    if requested < 0:
        raise ValueError("sustained acceptance capacity must be zero or positive")
    if requested == 0:
        return min(count, SUSTAINED_AUTO_TRANSPORT_WINDOW), "bounded-auto"
    return requested, "explicit"


@dataclass(frozen=True)
class ExperimentTask:
    endpoint_id: str
    task_class: str
    synthetic_duration_ms: int = 0
    host_duration_ms: int = 0
    device_duration_ms: int = 0
    export_duration_ms: int = 0
    payload_size: int = 0
    failure_mode: str = "none"


class DistributedExperimentRunner:
    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        registry: SystemRegistry,
        *,
        report_root: Path | None = None,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.database.initialize()
        self.registry = registry
        self.report_root = (
            report_root.resolve()
            if report_root is not None
            else self.root
            / "TestUtils"
            / "tester_daemon"
            / "distributed_experiments"
        )

    def enqueue(
        self,
        experiment_id: str,
        tasks: list[ExperimentTask],
    ) -> dict[str, Any]:
        identity = safe_token(experiment_id, "experiment_id")
        endpoints = {item.endpoint_id: item for item in self.registry.endpoints}
        experiment_root = self.report_root / identity
        request_root = experiment_root / "requests"
        payload_root = experiment_root / "payloads"
        request_root.mkdir(parents=True, exist_ok=True)
        payload_root.mkdir(parents=True, exist_ok=True)
        rows: list[dict[str, Any]] = []
        for sequence, task in enumerate(tasks, start=1):
            endpoint = endpoints.get(task.endpoint_id)
            if endpoint is None:
                raise ValueError(f"unknown experiment endpoint: {task.endpoint_id}")
            validate_task(task, endpoint)
            request_id = f"exp-{identity}-{sequence:04d}"
            payload_file = self._payload_file(payload_root, task.payload_size)
            manifest = {
                "schema": EXPERIMENT_SCHEMA,
                "experiment_id": identity,
                "sequence": sequence,
                "request_id": request_id,
                "test_version": identity,
                "task_class": task.task_class,
                "synthetic_duration_ms": int(task.synthetic_duration_ms),
                "host_duration_ms": int(task.host_duration_ms),
                "device_duration_ms": int(task.device_duration_ms),
                "export_duration_ms": int(task.export_duration_ms),
                "payload_size": int(task.payload_size),
                "payload_file": str(payload_file) if payload_file else "",
                "failure_mode": task.failure_mode,
                "workflow_ingest": False,
                "execution_requirements": execution_requirements(
                    task,
                    endpoint,
                ),
            }
            manifest["request_digest"] = canonical_digest(manifest)
            path = request_root / f"{request_id}.json"
            write_json_atomic(path, manifest, ensure_ascii=True, sort_keys=True)
            created = self.database.create_experiment_request(manifest, path)
            routed = self.database.route_test_request(request_id, self.registry)
            rows.append(
                {
                    "request": created,
                    "route": routed,
                    "manifest_path": str(path),
                }
            )
        metadata = {
            "schema": EXPERIMENT_SCHEMA,
            "experiment_id": identity,
            "task_count": len(tasks),
            "created_at": utc_now(),
            "workflow_ingest": False,
            "tasks": [
                {
                    "endpoint_id": task.endpoint_id,
                    "task_class": task.task_class,
                    "synthetic_duration_ms": task.synthetic_duration_ms,
                    "host_duration_ms": task.host_duration_ms,
                    "device_duration_ms": task.device_duration_ms,
                    "export_duration_ms": task.export_duration_ms,
                    "payload_size": task.payload_size,
                    "failure_mode": task.failure_mode,
                }
                for task in tasks
            ],
        }
        write_json_atomic(
            experiment_root / "experiment.json",
            metadata,
            ensure_ascii=True,
            sort_keys=True,
        )
        return {**metadata, "requests": rows}

    def run_until_terminal(
        self,
        experiment_id: str,
        pool: EndpointDispatcherPool,
        *,
        timeout_seconds: float = 300.0,
        poll_seconds: float = 0.25,
    ) -> dict[str, Any]:
        initial_report = self.report(experiment_id)
        if initial_report["task_count"] == 0:
            initial_report.update(
                state="failed",
                error=f"experiment has no queued tasks: {experiment_id}",
                dispatcher_ticks=0,
                dispatcher_elapsed_seconds=0.0,
            )
            self._write_report(experiment_id, initial_report)
            return initial_report
        blocked = blocked_experiment_report(initial_report)
        if blocked is not None:
            self._write_report(experiment_id, blocked)
            return blocked
        deadline = time.monotonic() + max(0.1, float(timeout_seconds))
        ticks: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            ticks.append(pool.run_once())
            report = self.report(experiment_id)
            if report["terminal_count"] == report["task_count"]:
                report["dispatcher_ticks"] = len(ticks)
                report["dispatcher_elapsed_seconds"] = round(
                    sum(float(item["elapsed_seconds"]) for item in ticks), 6
                )
                report["transport_timing"] = summarize_transport_timing(ticks)
                self._write_report(experiment_id, report)
                return report
            blocked = blocked_experiment_report(report)
            if blocked is not None:
                blocked["dispatcher_ticks"] = len(ticks)
                blocked["dispatcher_elapsed_seconds"] = round(
                    sum(float(item["elapsed_seconds"]) for item in ticks), 6
                )
                blocked["transport_timing"] = summarize_transport_timing(ticks)
                self._write_report(experiment_id, blocked)
                return blocked
            time.sleep(max(0.01, float(poll_seconds)))
        report = self.report(experiment_id)
        report["timed_out"] = True
        report["dispatcher_ticks"] = len(ticks)
        report["transport_timing"] = summarize_transport_timing(ticks)
        self._write_report(experiment_id, report)
        return report

    def run_sustained_acceptance(
        self,
        experiment_id: str,
        tasks: list[ExperimentTask],
        pool: EndpointDispatcherPool,
        *,
        timeout_seconds: float = 3000.0,
        poll_seconds: float = 0.1,
        minimum_window_seconds: float = 1800.0,
        minimum_accepted_tasks: int = 7,
        minimum_physical_devices: int = 2,
        handoff_threshold_seconds: float = 10.0,
        own_service_min_seconds: float = 20.0,
        own_service_max_seconds: float = 40.0,
        transport_capacity: int = 0,
        transport_capacity_mode: str = "",
    ) -> dict[str, Any]:
        with self.database.connection() as conn:
            existing = int(
                conn.execute("SELECT COUNT(*) FROM test_requests").fetchone()[0]
            )
        if existing:
            raise ValueError(
                "sustained acceptance requires a fresh control database; "
                f"found {existing} existing test requests"
            )
        self.enqueue(experiment_id, tasks)
        report = self.run_until_terminal(
            experiment_id,
            pool,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        report["sustained_acceptance"] = evaluate_sustained_acceptance(
            report,
            minimum_window_seconds=minimum_window_seconds,
            minimum_accepted_tasks=minimum_accepted_tasks,
            minimum_physical_devices=minimum_physical_devices,
            handoff_threshold_seconds=handoff_threshold_seconds,
            own_service_min_seconds=own_service_min_seconds,
            own_service_max_seconds=own_service_max_seconds,
        )
        report["sustained_acceptance"]["transport_capacity"] = int(
            transport_capacity
        )
        report["sustained_acceptance"]["transport_capacity_mode"] = str(
            transport_capacity_mode
        )
        self._write_report(experiment_id, report)
        return report

    def report(self, experiment_id: str) -> dict[str, Any]:
        identity = safe_token(experiment_id, "experiment_id")
        requests: list[dict[str, Any]] = []
        attempts: dict[str, dict[str, Any]] = {}
        outboxes: dict[str, dict[str, Any]] = {}
        returns: dict[str, dict[str, Any]] = {}
        events: list[dict[str, Any]] = []
        with self.database.connection() as conn:
            for row in conn.execute(
                "SELECT * FROM test_requests ORDER BY created_at, request_id"
            ).fetchall():
                manifest = json.loads(str(row["manifest_json"]))
                if str(manifest.get("experiment_id") or "") != identity:
                    continue
                value = dict(row)
                value["manifest"] = manifest
                requests.append(value)
            request_ids = {str(row["request_id"]) for row in requests}
            if request_ids:
                for row in conn.execute(
                    "SELECT * FROM execution_attempts ORDER BY created_at, attempt_id"
                ).fetchall():
                    if str(row["request_id"]) in request_ids:
                        attempts[str(row["attempt_id"])] = dict(row)
                attempt_ids = set(attempts)
                for row in conn.execute(
                    "SELECT * FROM transport_outbox ORDER BY created_at, outbox_id"
                ).fetchall():
                    if str(row["attempt_id"]) in attempt_ids:
                        outboxes[str(row["outbox_id"])] = dict(row)
                outbox_ids = set(outboxes)
                for row in conn.execute(
                    "SELECT * FROM transport_returns ORDER BY received_at, return_id"
                ).fetchall():
                    if str(row["outbox_id"]) in outbox_ids:
                        returns[str(row["return_id"])] = dict(row)
                entity_ids = request_ids | attempt_ids | outbox_ids | set(returns)
                for row in conn.execute(
                    "SELECT * FROM control_events ORDER BY sequence"
                ).fetchall():
                    if str(row["entity_id"]) in entity_ids:
                        events.append(dict(row))

        rows: list[dict[str, Any]] = []
        completion_order: list[tuple[str, str]] = []
        for request in requests:
            request_id = str(request["request_id"])
            attempt = next(
                (
                    row
                    for row in attempts.values()
                    if str(row["request_id"]) == request_id
                ),
                {},
            )
            outbox = next(
                (
                    row
                    for row in outboxes.values()
                    if str(row["attempt_id"]) == str(attempt.get("attempt_id") or "")
                ),
                {},
            )
            returned = next(
                (
                    row
                    for row in returns.values()
                    if str(row["outbox_id"]) == str(outbox.get("outbox_id") or "")
                ),
                {},
            )
            outbox_payload = decode_json_object(
                outbox.get("payload_json"),
            )
            result_payload = decode_json_object(
                returned.get("payload_json"),
            )
            engine_identity = (
                result_payload.get("engine", {})
                if isinstance(result_payload.get("engine"), dict)
                else {}
            )
            history = engine_identity.get("stage_history", [])
            engine_timing = (
                task_timing_sample(
                    [item for item in history if isinstance(item, dict)],
                    job_id=str(engine_identity.get("engine_job_id") or ""),
                    request_id=request_id,
                )
                if isinstance(history, list) and history
                else {}
            )
            identity_mismatches = result_identity_mismatches(
                outbox_payload,
                result_payload,
            )
            finished_at = str(
                returned.get("acknowledged_at")
                or returned.get("received_at")
                or ""
            )
            if finished_at:
                completion_order.append((finished_at, request_id))
            event_types = {
                str(event["event_type"])
                for event in events
                if str(event["entity_id"])
                in {
                    request_id,
                    str(attempt.get("attempt_id") or ""),
                    str(outbox.get("outbox_id") or ""),
                    str(returned.get("return_id") or ""),
                }
            }
            successful = str(request["state"]) == "completed"
            expected_trace = {
                "test-request-created",
                "test-request-routed",
                "transport-outbox-claimed",
                "transport-send-started",
                "transport-return-received",
                "transport-return-acknowledged",
            }
            missing_trace_events = set(expected_trace - event_types)
            if successful and not {
                "transport-delivery-accepted",
                "transport-acceptance-reconciled",
            } & event_types:
                missing_trace_events.add("transport-acceptance-visible")
            rows.append(
                {
                    "request_id": request_id,
                    "sequence": int(request["manifest"].get("sequence", 0)),
                    "endpoint_id": str(attempt.get("endpoint_id") or ""),
                    "task_class": str(request["manifest"].get("task_class") or ""),
                    "request_state": str(request["state"]),
                    "request_blocker": str(request.get("blocker") or ""),
                    "attempt_state": str(attempt.get("state") or ""),
                    "outbox_state": str(outbox.get("state") or ""),
                    "return_state": str(returned.get("state") or ""),
                    "created_at": str(request["created_at"]),
                    "accepted_at": str(outbox.get("accepted_at") or ""),
                    "returned_at": str(returned.get("received_at") or ""),
                    "acknowledged_at": str(returned.get("acknowledged_at") or ""),
                    "latency_seconds": duration_seconds(
                        str(request["created_at"]), finished_at
                    ),
                    "event_types": sorted(event_types),
                    "trace_complete": not missing_trace_events,
                    "missing_trace_events": sorted(missing_trace_events),
                    "result_identity_complete": (
                        bool(result_payload)
                        and not identity_mismatches
                    ),
                    "result_identity_mismatches": identity_mismatches,
                    "engine_identity": engine_identity,
                    "engine_timing": engine_timing,
                }
            )
        terminal = [
            row
            for row in rows
            if row["request_state"] in TERMINAL_REQUEST_STATES
        ]
        latencies = sorted(
            float(row["latency_seconds"])
            for row in terminal
            if row["latency_seconds"] is not None
        )
        started = min(
            (str(row["created_at"]) for row in rows),
            default="",
        )
        finished = max(
            (
                str(row["acknowledged_at"] or row["returned_at"])
                for row in terminal
            ),
            default="",
        )
        elapsed = duration_seconds(started, finished)
        by_endpoint: dict[str, int] = {}
        for row in terminal:
            endpoint_id = str(row["endpoint_id"])
            by_endpoint[endpoint_id] = by_endpoint.get(endpoint_id, 0) + 1
        report = {
            "schema": "ascendop.distributed-experiment-report.v1",
            "experiment_id": identity,
            "generated_at": utc_now(),
            "workflow_ingest": False,
            "task_count": len(rows),
            "terminal_count": len(terminal),
            "success_count": sum(
                row["request_state"] == "completed" for row in rows
            ),
            "failure_count": sum(row["request_state"] == "failed" for row in rows),
            "trace_complete_count": sum(row["trace_complete"] for row in rows),
            "result_identity_complete_count": sum(
                row["result_identity_complete"] for row in rows
            ),
            "route_distribution": dict(sorted(by_endpoint.items())),
            "submission_order": [
                row["request_id"] for row in sorted(rows, key=lambda item: item["sequence"])
            ],
            "completion_order": [
                request_id for _, request_id in sorted(completion_order)
            ],
            "elapsed_seconds": elapsed,
            "throughput_per_second": (
                round(len(terminal) / elapsed, 6)
                if elapsed is not None and elapsed > 0
                else 0.0
            ),
            "latency_seconds": latency_summary(latencies),
            "engine_timing": experiment_engine_timing_summary(
                [
                    row["engine_timing"]
                    for row in rows
                    if row.get("engine_timing")
                ]
            ),
            "requests": rows,
        }
        return report

    def _payload_file(self, root: Path, size: int) -> Path | None:
        value = int(size)
        if value < 0 or value > 1024 * 1024:
            raise ValueError("experiment payload_size must be 0..1048576")
        if value == 0:
            return None
        path = root / f"payload-{value}.bin"
        if not path.is_file() or path.stat().st_size != value:
            block = hashlib.sha256(f"ascendop-canary:{value}".encode()).digest()
            path.write_bytes((block * ((value + len(block) - 1) // len(block)))[:value])
        return path

    def _write_report(self, experiment_id: str, report: dict[str, Any]) -> None:
        path = self.report_root / safe_token(
            experiment_id, "experiment_id"
        ) / "report.json"
        write_json_atomic(path, report, ensure_ascii=True, sort_keys=True)


def blocked_experiment_report(
    report: dict[str, Any],
) -> dict[str, Any] | None:
    requests = [
        item
        for item in report.get("requests", [])
        if isinstance(item, dict)
    ]
    nonterminal = [
        item
        for item in requests
        if str(item.get("request_state") or "") not in TERMINAL_REQUEST_STATES
    ]
    if not nonterminal or any(
        str(item.get("request_state") or "") != "blocked"
        for item in nonterminal
    ):
        return None
    blockers = sorted(
        {
            str(item.get("request_blocker") or "unspecified-blocker")
            for item in nonterminal
        }
    )
    blocked = dict(report)
    blocked.update(
        state="blocked",
        blocked_count=len(nonterminal),
        blockers=blockers,
        dispatcher_ticks=0,
        dispatcher_elapsed_seconds=0.0,
        transport_timing=summarize_transport_timing([]),
    )
    return blocked


def evaluate_sustained_acceptance(
    report: dict[str, Any],
    *,
    minimum_window_seconds: float = 1800.0,
    minimum_accepted_tasks: int = 7,
    minimum_physical_devices: int = 2,
    handoff_threshold_seconds: float = 10.0,
    own_service_min_seconds: float = 20.0,
    own_service_max_seconds: float = 40.0,
) -> dict[str, Any]:
    requests = [
        item for item in report.get("requests", []) if isinstance(item, dict)
    ]
    accepted_count = sum(bool(item.get("accepted_at")) for item in requests)
    timing = (
        report.get("engine_timing")
        if isinstance(report.get("engine_timing"), dict)
        else {}
    )
    device_flow = (
        timing.get("device_flow")
        if isinstance(timing.get("device_flow"), dict)
        else {}
    )
    per_device = (
        device_flow.get("per_device")
        if isinstance(device_flow.get("per_device"), dict)
        else {}
    )
    device_windows = {
        str(device_id): float(
            value.get("device_window_seconds", 0.0) or 0.0
        )
        for device_id, value in per_device.items()
        if isinstance(value, dict)
    }
    handoff_gaps = [
        float(item.get("gap_seconds", 0.0) or 0.0)
        for item in device_flow.get("handoffs", [])
        if isinstance(item, dict)
    ]
    own_services = [
        float(
            item.get("engine_timing", {}).get(
                "own_critical_path_service_seconds",
                item.get("engine_timing", {}).get("own_service_seconds", 0.0),
            )
            or 0.0
        )
        for item in requests
        if isinstance(item.get("engine_timing"), dict)
        and item.get("engine_timing")
    ]
    engine_job_ids = [
        str(item.get("engine_identity", {}).get("engine_job_id") or "")
        for item in requests
        if isinstance(item.get("engine_identity"), dict)
        and str(item.get("engine_identity", {}).get("engine_job_id") or "")
    ]
    task_count = int(report.get("task_count", 0) or 0)
    checks = {
        "fresh_database": True,
        "workflow_ingest_disabled": report.get("workflow_ingest") is False,
        "all_tasks_terminal": (
            task_count > 0
            and int(report.get("terminal_count", 0) or 0) == task_count
        ),
        "all_tasks_successful": (
            int(report.get("failure_count", 0) or 0) == 0
            and int(report.get("success_count", 0) or 0) == task_count
        ),
        "accepted_task_count": accepted_count
        >= int(minimum_accepted_tasks),
        "physical_device_count": len(per_device)
        >= int(minimum_physical_devices),
        "each_device_window": (
            len(device_windows) >= int(minimum_physical_devices)
            and all(
                value >= float(minimum_window_seconds)
                for value in device_windows.values()
            )
        ),
        "handoff_observed": bool(handoff_gaps),
        "all_handoffs_below_threshold": (
            bool(handoff_gaps)
            and all(
                value < float(handoff_threshold_seconds)
                for value in handoff_gaps
            )
        ),
        "all_own_service_in_range": (
            len(own_services) == task_count
            and all(
                float(own_service_min_seconds)
                <= value
                <= float(own_service_max_seconds)
                for value in own_services
            )
        ),
        "host_parallelism_observed": bool(
            timing.get("resource_parallelism", {})
            .get("host", {})
            .get("max_concurrency", 0)
            > 1
        ),
        "trace_complete": int(report.get("trace_complete_count", 0) or 0)
        == task_count,
        "result_identity_complete": int(
            report.get("result_identity_complete_count", 0) or 0
        )
        == task_count,
        "no_duplicate_engine_job_ids": (
            len(engine_job_ids) == task_count
            and len(set(engine_job_ids)) == len(engine_job_ids)
        ),
    }
    return {
        "schema": "ascendop.sustained-multicard-acceptance.v1",
        "status": "passed" if all(checks.values()) else "failed",
        "clean_window_contract": (
            "fresh database and workflow_ingest=false; no manual probes, "
            "maintenance, source sync, or endpoint restart may overlap the run"
        ),
        "thresholds": {
            "minimum_window_seconds_per_device": float(
                minimum_window_seconds
            ),
            "minimum_accepted_tasks": int(minimum_accepted_tasks),
            "minimum_physical_devices": int(minimum_physical_devices),
            "handoff_threshold_seconds": float(handoff_threshold_seconds),
            "own_service_range_seconds": [
                float(own_service_min_seconds),
                float(own_service_max_seconds),
            ],
        },
        "observed": {
            "accepted_task_count": accepted_count,
            "physical_device_count": len(per_device),
            "device_window_seconds": device_windows,
            "handoff_count": len(handoff_gaps),
            "handoff_max_seconds": max(handoff_gaps) if handoff_gaps else None,
            "own_service_count": len(own_services),
            "own_service_min_seconds": (
                min(own_services) if own_services else None
            ),
            "own_service_max_seconds": (
                max(own_services) if own_services else None
            ),
            "duplicate_engine_job_id_count": (
                len(engine_job_ids) - len(set(engine_job_ids))
            ),
        },
        "checks": checks,
        "failed_checks": [
            name for name, passed in checks.items() if not passed
        ],
    }


def validate_task(task: ExperimentTask, endpoint: BackendEndpoint) -> None:
    if task.task_class not in {
        "control-probe",
        "host-only-canary",
        "engine-host-canary",
        "engine-device-canary",
    }:
        raise ValueError(f"unsupported experiment task class: {task.task_class}")
    if task.failure_mode not in {
        "none",
        "fail-before-result",
        "fail-after-result",
    }:
        raise ValueError(f"unsupported experiment failure mode: {task.failure_mode}")
    for name, value in (
        ("synthetic", task.synthetic_duration_ms),
        ("host", task.host_duration_ms),
        ("device", task.device_duration_ms),
        ("export", task.export_duration_ms),
    ):
        if int(value) < 0:
            raise ValueError(f"{name} duration cannot be negative")
    if task.task_class not in set(endpoint.capabilities.get("features", [])):
        raise ValueError(
            f"endpoint {endpoint.endpoint_id} lacks {task.task_class}"
        )


def execution_requirements(
    task: ExperimentTask,
    endpoint: BackendEndpoint,
) -> dict[str, Any]:
    engine_task = task.task_class.startswith("engine-")
    device_task = task.task_class == "engine-device-canary"
    features = [task.task_class]
    if engine_task:
        features.append("engine-v3-staged-fused")
    return {
        "allowed_endpoints": [endpoint.endpoint_id],
        "backend_pool": endpoint.backend_pool,
        "transport": endpoint.transport,
        "features": features,
        "runtime_node_policy": (
            BOOTSTRAP_CONTROL_PROBE_POLICY
            if task.task_class == "control-probe"
            else "require-accepted-node"
        ),
        "device_count": 1 if device_task else 0,
        "host_slots": 1 if engine_task else 0,
        "device_slots": 1 if device_task else 0,
        "export_slots": 1 if engine_task else 0,
        "return_capacity": 1 if engine_task else 0,
        "require_fresh_credit": engine_task,
        "require_engine_resident": engine_task,
    }


def canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def decode_json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value:
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def result_identity_mismatches(
    expected: dict[str, Any],
    observed: dict[str, Any],
) -> list[str]:
    if not expected or not observed:
        return ["result-payload-missing"]
    return [
        key
        for key in (
            "attempt_id",
            "request_id",
            "target_endpoint_id",
            "target_node_id",
            "target_environment_id",
            "target_gateway_id",
            "target_transport_mode",
            "target_generation",
        )
        if str(expected.get(key) or "") != str(observed.get(key) or "")
    ]


def summarize_transport_timing(
    ticks: list[dict[str, Any]],
) -> dict[str, Any]:
    per_endpoint: dict[str, dict[str, Any]] = {}
    for tick in ticks:
        for endpoint in tick.get("endpoints", []):
            if not isinstance(endpoint, dict):
                continue
            endpoint_id = str(endpoint.get("endpoint_id") or "unknown")
            summary = per_endpoint.setdefault(
                endpoint_id,
                {
                    "result_query_count": 0,
                    "result_query_seconds": {},
                },
            )
            for polled in endpoint.get("polled", []):
                if not isinstance(polled, dict):
                    continue
                diagnostics = polled.get("diagnostics")
                if not isinstance(diagnostics, dict):
                    continue
                timing = diagnostics.get("result_query_timing")
                if not isinstance(timing, dict):
                    continue
                summary["result_query_count"] += 1
                totals = summary["result_query_seconds"]
                for name, value in timing.items():
                    if not isinstance(value, (int, float)):
                        continue
                    totals[name] = round(
                        float(totals.get(name, 0.0)) + float(value),
                        6,
                    )
    return {
        "endpoints": per_endpoint,
        "result_query_count": sum(
            int(item["result_query_count"])
            for item in per_endpoint.values()
        ),
    }


def safe_token(value: str, label: str) -> str:
    if not value or not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise ValueError(f"{label} must be a non-empty safe token")
    return value


def utc_now() -> str:
    return datetime.now().astimezone().isoformat()


def duration_seconds(started_at: str, finished_at: str) -> float | None:
    if not started_at or not finished_at:
        return None
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round(max(0.0, (finished - started).total_seconds()), 6)


def latency_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "max": None}
    return {
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "max": round(max(values), 6),
    }


def percentile(values: list[float], fraction: float) -> float:
    index = min(len(values) - 1, max(0, int((len(values) - 1) * fraction)))
    return round(values[index], 6)
