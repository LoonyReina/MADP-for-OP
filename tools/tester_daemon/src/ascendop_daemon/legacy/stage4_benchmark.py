from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.legacy.distributed_experiment import (
    DistributedExperimentRunner,
    ExperimentTask,
    latency_summary,
)
from ascendop_daemon.control_plane.endpoint_dispatcher import (
    DeliveryObservation,
    EndpointDispatcher,
    EndpointDispatcherPool,
    QueryObservation,
    deterministic_receipt_id,
    transport_identity,
)
from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.registry.system_registry import SystemRegistry


@dataclass(frozen=True)
class BenchmarkProfile:
    name: str
    features: tuple[str, ...]
    poll_max_seconds: float


PROFILES = (
    BenchmarkProfile("legacy", ("control-probe",), 0.01),
    BenchmarkProfile(
        "append-batch",
        ("control-probe", "gp-append-request-v1"),
        0.01,
    ),
    BenchmarkProfile(
        "batch-result-query",
        (
            "control-probe",
            "gp-append-request-v1",
            "gp-batch-result-query-v1",
        ),
        0.01,
    ),
    BenchmarkProfile(
        "adaptive-poll",
        (
            "control-probe",
            "gp-append-request-v1",
            "gp-batch-result-query-v1",
            "gp-adaptive-poll-v1",
        ),
        0.08,
    ),
)


class BenchmarkTransport:
    def __init__(
        self,
        *,
        publish_seconds: float,
        query_seconds: float,
    ) -> None:
        self.publish_seconds = float(publish_seconds)
        self.query_seconds = float(query_seconds)
        self.ready_at: dict[str, float] = {}
        self.events: list[dict[str, Any]] = []
        self.publish_operations = 0
        self.query_operations = 0
        self.lock = threading.Lock()

    def publish(self, payload: dict[str, Any]) -> DeliveryObservation:
        self._delay("publish", self.publish_seconds, 1)
        self._accept(payload)
        return DeliveryObservation(
            status="accepted",
            receipt=self._acceptance(payload),
        )

    def publish_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[DeliveryObservation]:
        self._delay("publish", self.publish_seconds, len(payloads))
        for payload in payloads:
            self._accept(payload)
        return [
            DeliveryObservation(
                status="accepted",
                receipt=self._acceptance(payload),
            )
            for payload in payloads
        ]

    def query(self, payload: dict[str, Any]) -> QueryObservation:
        self._delay("query", self.query_seconds, 1)
        return self._query_observation(payload)

    def query_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[QueryObservation]:
        self._delay("query", self.query_seconds, len(payloads))
        return [
            self._query_observation(payload)
            for payload in payloads
        ]

    def acknowledge(
        self,
        payload: dict[str, Any],
        returned: dict[str, Any],
    ) -> bool:
        return str(returned.get("receipt_id") or "") == (
            deterministic_receipt_id(payload)
        )

    def overlap_seconds(self) -> float:
        publishes = [
            event for event in self.events if event["lane"] == "publish"
        ]
        queries = [
            event for event in self.events if event["lane"] == "query"
        ]
        return round(
            sum(
                max(
                    0.0,
                    min(pub["finished"], query["finished"])
                    - max(pub["started"], query["started"]),
                )
                for pub in publishes
                for query in queries
            ),
            6,
        )

    def _accept(self, payload: dict[str, Any]) -> None:
        manifest_path = Path(str(payload["manifest_path"]))
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8-sig")
        )
        duration = (
            int(manifest.get("synthetic_duration_ms", 0)) / 1000.0
        )
        self.ready_at[str(payload["request_id"])] = (
            time.monotonic() + duration
        )

    @staticmethod
    def _acceptance(payload: dict[str, Any]) -> dict[str, Any]:
        return {
            **transport_identity(payload),
            "acceptance_id": f"benchmark-{payload['request_id']}",
        }

    def _query_observation(
        self,
        payload: dict[str, Any],
    ) -> QueryObservation:
        request_id = str(payload["request_id"])
        acceptance = self._acceptance(payload)
        if time.monotonic() < self.ready_at.get(request_id, float("inf")):
            return QueryObservation(acceptance=acceptance)
        return QueryObservation(
            acceptance=acceptance,
            result={
                **transport_identity(payload),
                "schema": "gitpartner.distributed-canary-result.v2",
                "receipt_id": deterministic_receipt_id(payload),
                "outcome": "success",
                "workflow_ingest": False,
            },
        )

    def _delay(self, lane: str, seconds: float, item_count: int) -> None:
        started = time.monotonic()
        time.sleep(seconds)
        finished = time.monotonic()
        with self.lock:
            if lane == "publish":
                self.publish_operations += 1
            else:
                self.query_operations += 1
            self.events.append(
                {
                    "lane": lane,
                    "started": started,
                    "finished": finished,
                    "item_count": item_count,
                }
            )


def run_stage4_benchmark(
    output: Path,
    *,
    repeats: int = 3,
    task_count: int = 12,
    capacity: int = 4,
    publish_seconds: float = 0.02,
    query_seconds: float = 0.01,
) -> dict[str, Any]:
    repeat_count = max(1, int(repeats))
    count = max(4, int(task_count))
    profile_reports: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory(prefix="ascendop-stage4-") as temp:
        temp_root = Path(temp)
        for profile in PROFILES:
            samples = [
                run_profile(
                    temp_root / f"{profile.name}-{index}",
                    profile,
                    task_count=count,
                    capacity=capacity,
                    publish_seconds=publish_seconds,
                    query_seconds=query_seconds,
                )
                for index in range(repeat_count)
            ]
            profile_reports[profile.name] = summarize_profile(
                profile,
                samples,
            )
        duplex = {
            mode: run_duplex_probe(
                temp_root / f"duplex-{mode}",
                duplex=(mode == "duplex"),
                lane_seconds=max(
                    0.08,
                    float(publish_seconds),
                    float(query_seconds),
                ),
            )
            for mode in ("serial", "duplex")
        }
        adaptive_poll = {
            mode: run_adaptive_poll_probe(
                temp_root / f"adaptive-poll-{mode}",
                adaptive=(mode == "adaptive"),
                poll_max_seconds=(0.08 if mode == "adaptive" else 0.01),
                pending_seconds=0.4,
                publish_seconds=publish_seconds,
                query_seconds=query_seconds,
            )
            for mode in ("fixed", "adaptive")
        }
    report = {
        "schema": "ascendop.stage4-transport-benchmark.v1",
        "generated_at": utc_now(),
        "workflow_ingest": False,
        "matched_workload": {
            "repeats": repeat_count,
            "task_count": count,
            "capacity": capacity,
            "publish_seconds": publish_seconds,
            "query_seconds": query_seconds,
            "synthetic_duration_pattern_ms": duration_pattern(count),
            "cache_state": "not-applicable-control-probe",
        },
        "profiles": profile_reports,
        "duplex_probe": duplex,
        "adaptive_poll_probe": adaptive_poll,
        "comparisons": {
            "append_batch_vs_legacy": compare_profiles(
                profile_reports["legacy"],
                profile_reports["append-batch"],
            ),
            "batch_query_vs_append_batch": compare_profiles(
                profile_reports["append-batch"],
                profile_reports["batch-result-query"],
            ),
            "adaptive_vs_fixed_batch_query": compare_profiles(
                profile_reports["batch-result-query"],
                profile_reports["adaptive-poll"],
            ),
            "duplex_vs_serial": {
                "elapsed_speedup": ratio(
                    duplex["serial"]["elapsed_seconds"],
                    duplex["duplex"]["elapsed_seconds"],
                ),
                "elapsed_reduction_percent": reduction_percent(
                    duplex["serial"]["elapsed_seconds"],
                    duplex["duplex"]["elapsed_seconds"],
                ),
                "duplex_overlap_seconds": duplex["duplex"][
                    "lane_overlap_seconds"
                ],
            },
            "adaptive_polling": {
                "query_operation_reduction_percent": reduction_percent(
                    float(adaptive_poll["fixed"]["query_operations"]),
                    float(adaptive_poll["adaptive"]["query_operations"]),
                ),
                "fixed_query_operations": adaptive_poll["fixed"][
                    "query_operations"
                ],
                "adaptive_query_operations": adaptive_poll["adaptive"][
                    "query_operations"
                ],
                "trace_complete_preserved": all(
                    int(sample["success_count"]) == 1
                    and int(sample["failure_count"]) == 0
                    and int(sample["trace_complete_count"]) == 1
                    and int(sample["result_identity_complete_count"]) == 1
                    for sample in adaptive_poll.values()
                ),
            },
        },
    }
    comparisons = report["comparisons"]
    append_comparison = comparisons["append_batch_vs_legacy"]
    batch_comparison = comparisons["batch_query_vs_append_batch"]
    adaptive_comparison = comparisons["adaptive_vs_fixed_batch_query"]
    duplex_comparison = comparisons["duplex_vs_serial"]
    adaptive_probe_comparison = comparisons["adaptive_polling"]
    report["optimization_decisions"] = {
        "gp-append-request-v1": {
            "accepted": (
                append_comparison["elapsed_reduction_percent"] > 0
                and append_comparison["trace_complete_preserved"]
            ),
            "measured_effect": append_comparison,
        },
        "gp-duplex-lanes-v1": {
            "accepted": (
                duplex_comparison["elapsed_reduction_percent"] > 0
                and duplex_comparison["duplex_overlap_seconds"] > 0
            ),
            "measured_effect": duplex_comparison,
        },
        "gp-batch-result-query-v1": {
            "accepted": (
                batch_comparison["elapsed_reduction_percent"] > 0
                and batch_comparison[
                    "query_operation_reduction_percent"
                ]
                > 0
                and batch_comparison["trace_complete_preserved"]
            ),
            "measured_effect": batch_comparison,
        },
        "gp-adaptive-poll-v1": {
            "accepted": (
                adaptive_comparison["elapsed_reduction_percent"] > 0
                and adaptive_probe_comparison[
                    "query_operation_reduction_percent"
                ]
                > 0
                and adaptive_comparison["trace_complete_preserved"]
                and adaptive_probe_comparison[
                    "trace_complete_preserved"
                ]
            ),
            "measured_effect": {
                "matched_profile": adaptive_comparison,
                "pending_request_probe": adaptive_probe_comparison,
            },
        },
    }
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    return report


def run_profile(
    root: Path,
    profile: BenchmarkProfile,
    *,
    task_count: int,
    capacity: int,
    publish_seconds: float,
    query_seconds: float,
) -> dict[str, Any]:
    registry = write_registry(root, profile.features)
    database = ControlDatabase(root / "state" / "control.sqlite3")
    database.reconcile(benchmark_config(), registry)
    accept_runtime_node(database, registry)
    runner = DistributedExperimentRunner(
        root,
        database,
        registry,
        report_root=root / "reports",
    )
    tasks = [
        ExperimentTask(
            "benchmark-endpoint",
            "control-probe",
            synthetic_duration_ms=duration,
        )
        for duration in duration_pattern(task_count)
    ]
    runner.enqueue("stage4", tasks)
    endpoint = registry.endpoints[0]
    transport = BenchmarkTransport(
        publish_seconds=publish_seconds,
        query_seconds=query_seconds,
    )
    dispatcher = EndpointDispatcher(
        database,
        endpoint,
        transport,
        capacity=capacity,
        poll_min_interval_seconds=0.01,
        poll_max_interval_seconds=profile.poll_max_seconds,
    )
    report = runner.run_until_terminal(
        "stage4",
        EndpointDispatcherPool([dispatcher]),
        timeout_seconds=30,
        poll_seconds=0.002,
    )
    return {
        "elapsed_seconds": float(report["elapsed_seconds"] or 0.0),
        "throughput_per_second": float(
            report["throughput_per_second"]
        ),
        "latency_seconds": report["latency_seconds"],
        "request_latencies": [
            float(row["latency_seconds"])
            for row in report["requests"]
            if row.get("latency_seconds") is not None
        ],
        "success_count": int(report["success_count"]),
        "failure_count": int(report["failure_count"]),
        "trace_complete_count": int(report["trace_complete_count"]),
        "result_identity_complete_count": int(
            report["result_identity_complete_count"]
        ),
        "publish_operations": transport.publish_operations,
        "query_operations": transport.query_operations,
        "lane_overlap_seconds": transport.overlap_seconds(),
    }


def run_duplex_probe(
    root: Path,
    *,
    duplex: bool,
    lane_seconds: float,
) -> dict[str, Any]:
    features = ["control-probe", "gp-append-request-v1"]
    if duplex:
        features.append("gp-duplex-lanes-v1")
    registry = write_registry(root, tuple(features))
    database = ControlDatabase(root / "state" / "control.sqlite3")
    database.reconcile(benchmark_config(), registry)
    accept_runtime_node(database, registry)
    runner = DistributedExperimentRunner(
        root,
        database,
        registry,
        report_root=root / "reports",
    )
    runner.enqueue(
        "duplex-probe",
        [
            ExperimentTask(
                "benchmark-endpoint",
                "control-probe",
                synthetic_duration_ms=1000,
            ),
            ExperimentTask(
                "benchmark-endpoint",
                "control-probe",
                synthetic_duration_ms=1000,
            ),
        ],
    )
    seed = database.claim_transport_outbox(
        "benchmark-seed",
        max_items=1,
        endpoint_id="benchmark-endpoint",
    )[0]
    database.mark_transport_sending(
        seed["outbox_id"],
        consumer="benchmark-seed",
        claim_token=seed["claim_token"],
    )
    database.record_transport_delivery(
        seed["outbox_id"],
        consumer="benchmark-seed",
        claim_token=seed["claim_token"],
        status="accepted",
        receipt={
            **transport_identity(seed["payload"]),
            "acceptance_id": "benchmark-seed",
        },
    )
    transport = BenchmarkTransport(
        publish_seconds=lane_seconds,
        query_seconds=lane_seconds,
    )
    transport._accept(seed["payload"])
    dispatcher = EndpointDispatcher(
        database,
        registry.endpoints[0],
        transport,
        capacity=2,
        poll_min_interval_seconds=0.01,
        poll_max_interval_seconds=0.01,
    )
    started = time.monotonic()
    tick = dispatcher.run_once()
    elapsed = time.monotonic() - started
    return {
        "lane_mode": tick["lane_mode"],
        "lane_seconds": lane_seconds,
        "elapsed_seconds": round(elapsed, 6),
        "lane_overlap_seconds": transport.overlap_seconds(),
        "publish_operations": transport.publish_operations,
        "query_operations": transport.query_operations,
        "claimed_count": len(tick["claimed"]),
        "error_count": len(tick["errors"]),
    }


def run_adaptive_poll_probe(
    root: Path,
    *,
    adaptive: bool,
    poll_max_seconds: float,
    pending_seconds: float,
    publish_seconds: float,
    query_seconds: float,
) -> dict[str, Any]:
    features = [
        "control-probe",
        "gp-append-request-v1",
        "gp-batch-result-query-v1",
    ]
    if adaptive:
        features.append("gp-adaptive-poll-v1")
    registry = write_registry(root, tuple(features))
    database = ControlDatabase(root / "state" / "control.sqlite3")
    database.reconcile(benchmark_config(), registry)
    accept_runtime_node(database, registry)
    runner = DistributedExperimentRunner(
        root,
        database,
        registry,
        report_root=root / "reports",
    )
    runner.enqueue(
        "adaptive-poll-probe",
        [
            ExperimentTask(
                "benchmark-endpoint",
                "control-probe",
                synthetic_duration_ms=int(pending_seconds * 1000),
            )
        ],
    )
    transport = BenchmarkTransport(
        publish_seconds=publish_seconds,
        query_seconds=query_seconds,
    )
    dispatcher = EndpointDispatcher(
        database,
        registry.endpoints[0],
        transport,
        capacity=1,
        poll_min_interval_seconds=0.01,
        poll_max_interval_seconds=poll_max_seconds,
    )
    report = runner.run_until_terminal(
        "adaptive-poll-probe",
        EndpointDispatcherPool([dispatcher]),
        timeout_seconds=10,
        poll_seconds=0.002,
    )
    return {
        "poll_mode": "adaptive" if adaptive else "fixed",
        "poll_max_seconds": poll_max_seconds,
        "pending_seconds": pending_seconds,
        "elapsed_seconds": float(report["elapsed_seconds"] or 0.0),
        "query_operations": transport.query_operations,
        "success_count": int(report["success_count"]),
        "failure_count": int(report["failure_count"]),
        "trace_complete_count": int(report["trace_complete_count"]),
        "result_identity_complete_count": int(
            report["result_identity_complete_count"]
        ),
    }


def write_registry(
    root: Path,
    features: tuple[str, ...],
) -> SystemRegistry:
    registry_path = (
        root / "Develop" / "registry" / "system_registry.json"
    )
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    (root / "GitPartner").mkdir(parents=True, exist_ok=True)
    duplex = "gp-duplex-lanes-v1" in features
    registry_path.write_text(
        json.dumps(
            {
                "schema": "ascendop.system-registry.v2",
                "registry_generation": "stage4-benchmark",
                "operator_defaults": {},
                "operator_overrides": {},
                "transport_gateways": [],
                "execution_nodes": [
                    {
                        "node_id": "benchmark-node",
                        "display_name": "benchmark-node",
                    }
                ],
                "execution_environments": [
                    {
                        "execution_environment_id": "benchmark-env",
                        "node_id": "benchmark-node",
                        "backend_pool": "benchmark",
                        "remote_root": "/benchmark",
                        "engine_root": "control-only",
                        "capabilities": {
                            "features": list(features),
                            "device_count": 0,
                        },
                    }
                ],
                "route_endpoints": [
                    {
                        "endpoint_id": "benchmark-endpoint",
                        "node_id": "benchmark-node",
                        "execution_environment_id": "benchmark-env",
                        "transport_binding": {"mode": "direct-git"},
                        "backend_pool": "benchmark",
                        "gitpartner_repo": "GitPartner",
                        "result_worktree": (
                            "GitPartner-result" if duplex else ""
                        ),
                        "control_channel": "gp/control/benchmark",
                        "result_channel": "gp/results/benchmark",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return SystemRegistry.load(registry_path)


def accept_runtime_node(
    database: ControlDatabase,
    registry: SystemRegistry,
) -> None:
    endpoint = registry.endpoints[0]
    now = datetime.now(timezone.utc)
    report = {
        "schema": "git-partner.node-report.v1",
        "node_id": endpoint.node_id,
        "endpoint_id": endpoint.endpoint_id,
        "execution_environment_id": endpoint.execution_environment_id,
        "gateway_id": endpoint.gateway_id,
        "transport_mode": endpoint.transport_mode,
        "generation": endpoint.generation,
        "session_id": "stage4-benchmark-session",
        "boot_id": "stage4-benchmark-boot",
        "sequence": 1,
        "state": "ready",
        "reason": "benchmark",
        "started_at": format_timestamp(now),
        "heartbeat_at": format_timestamp(now),
        "lease_expires_at": format_timestamp(
            now + timedelta(minutes=5)
        ),
        "capability_generation": "stage4-benchmark-capabilities",
        "capabilities": {
            "observed_at": format_timestamp(now),
            "ready": True,
            "execution_environment_id": endpoint.execution_environment_id,
            "endpoint_generation": endpoint.generation,
            "features": list(
                endpoint.capabilities.get("features", [])
            ),
            "device_count": 0,
            "cache_adapters": [],
        },
    }
    database.ingest_node_report(report, source="stage4-benchmark")
    database.accept_node(endpoint.node_id, registry)


def benchmark_config() -> DaemonConfig:
    return DaemonConfig(
        season="stage4-benchmark",
        transport="gitpartner",
        remote_root="/benchmark",
        operators=(),
        operator_sessions={},
    )


def summarize_profile(
    profile: BenchmarkProfile,
    samples: list[dict[str, Any]],
) -> dict[str, Any]:
    elapsed = [float(sample["elapsed_seconds"]) for sample in samples]
    throughput = [
        float(sample["throughput_per_second"]) for sample in samples
    ]
    request_latencies = [
        float(value)
        for sample in samples
        for value in sample.get("request_latencies", [])
    ]
    request_latencies.sort()
    return {
        "features": list(profile.features),
        "poll_max_seconds": profile.poll_max_seconds,
        "elapsed_seconds": summary(elapsed),
        "throughput_per_second": summary(throughput),
        "request_latency_seconds": latency_summary(request_latencies),
        "publish_operations": summary(
            [float(sample["publish_operations"]) for sample in samples]
        ),
        "query_operations": summary(
            [float(sample["query_operations"]) for sample in samples]
        ),
        "lane_overlap_seconds": summary(
            [float(sample["lane_overlap_seconds"]) for sample in samples]
        ),
        "success_count": sum(
            int(sample["success_count"]) for sample in samples
        ),
        "failure_count": sum(
            int(sample["failure_count"]) for sample in samples
        ),
        "trace_complete_count": sum(
            int(sample["trace_complete_count"]) for sample in samples
        ),
        "result_identity_complete_count": sum(
            int(sample["result_identity_complete_count"])
            for sample in samples
        ),
        "samples": samples,
    }


def compare_profiles(
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, Any]:
    before_elapsed = float(before["elapsed_seconds"]["p50"])
    after_elapsed = float(after["elapsed_seconds"]["p50"])
    before_publish = float(before["publish_operations"]["p50"])
    after_publish = float(after["publish_operations"]["p50"])
    before_query = float(before["query_operations"]["p50"])
    after_query = float(after["query_operations"]["p50"])
    return {
        "elapsed_speedup": ratio(before_elapsed, after_elapsed),
        "elapsed_reduction_percent": reduction_percent(
            before_elapsed,
            after_elapsed,
        ),
        "publish_operation_reduction_percent": reduction_percent(
            before_publish,
            after_publish,
        ),
        "query_operation_reduction_percent": reduction_percent(
            before_query,
            after_query,
        ),
        "trace_complete_preserved": (
            after["failure_count"] == 0
            and before["trace_complete_count"] == before["success_count"]
            and after["trace_complete_count"] == after["success_count"]
            and before["result_identity_complete_count"]
            == before["success_count"]
            and after["result_identity_complete_count"]
            == after["success_count"]
        ),
    }


def summary(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50": round(percentile(ordered, 0.50), 6),
        "p95": round(percentile(ordered, 0.95), 6),
        "max": round(max(ordered), 6) if ordered else 0.0,
        "mean": round(statistics.fmean(ordered), 6) if ordered else 0.0,
    }


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(len(values) - 1, lower + 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def duration_pattern(count: int) -> list[int]:
    pattern = (20, 80, 35, 120, 15, 65)
    return [pattern[index % len(pattern)] for index in range(count)]


def ratio(before: float, after: float) -> float:
    return round(before / after, 4) if after > 0 else 0.0


def reduction_percent(before: float, after: float) -> float:
    if before <= 0:
        return 0.0
    return round((before - after) * 100.0 / before, 2)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def format_timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run isolated Stage 4 transport optimization benchmarks"
    )
    parser.add_argument(
        "--output",
        default=(
            "TestUtils/tester_daemon/distributed_experiments/"
            "stage4_transport_benchmark.json"
        ),
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--tasks", type=int, default=12)
    parser.add_argument("--capacity", type=int, default=4)
    args = parser.parse_args(argv)
    report = run_stage4_benchmark(
        Path(args.output),
        repeats=args.repeats,
        task_count=args.tasks,
        capacity=args.capacity,
    )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
