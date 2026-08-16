from __future__ import annotations

import hashlib
import json
import os
import socket
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.storage.control_types import SCHEMA_VERSION
from ascendop_daemon.legacy.engine_promotion import expected_remote_engine_code_generation
from ascendop_daemon.registry.engine_route import EngineTransportRoute, resolve_registered_engine_route
from ascendop_daemon.legacy.flow_v3_dispatcher import (
    CannJudgeFlowV3WorkflowAdapter,
    FlowV3Dispatcher,
    FlowV3ResultIngestor,
    GitPartnerFlowV3Transport,
    GitPartnerTransportConfig,
    gitpartner_transport_generation,
)
from ascendop_daemon.legacy.flow_v3_store import FLOW_DB_SCHEMA, FlowV3Store
from ascendop_daemon.observability.flow_v3_timing import read_boot_id


LOCAL_COMPONENTS = {
    "flow-v3-daemon": {
        "role": "daemon",
        "capabilities": ["flow-v3", "workflow-gate"],
    },
    "flow-v3-scheduler": {
        "role": "scheduler",
        "capabilities": ["flow-v3", "correctness-first"],
    },
    "flow-v3-retry-controller": {
        "role": "retry-controller",
        "capabilities": ["flow-v3", "central-retry"],
    },
}
WORKER_COMPONENTS = {
    "flow-v3-dispatcher": {
        "role": "dispatcher",
        "capabilities": ["flow-v3", "engine-archive"],
    },
    "flow-v3-result-ingestor": {
        "role": "result-ingestor",
        "capabilities": ["flow-v3", "durable-return-ack"],
    },
}
OBSERVED_COMPONENTS = {
    "flow-v3-native-relay": {
        "role": "native-relay",
        "capabilities": ["flow-v3", "observed-service"],
    },
    "flow-v3-watchdog": {
        "role": "watchdog",
        "capabilities": ["flow-v3", "observed-service"],
    },
    "flow-v3-supervisor": {
        "role": "supervisor",
        "capabilities": ["flow-v3", "observed-service"],
    },
}
REMOTE_COMPONENT_ID = "flow-v3-endpoint"
REMOTE_ENDPOINT_CAPABILITIES = [
    "flow-v3",
    "engine-archive",
    "device-session-wall-budget",
    "correctness-first",
    "diagnostic-profile",
    "profiler-primary-all-cases",
    "profiler-primary-roofline-all-cases",
    "weighted-host-scheduler",
    "endpoint-journal",
]
REMOTE_NODE_FEATURES = {
    "engine-v3-staged-fused",
    "operator-test",
    "profiler",
}


@dataclass(frozen=True)
class FlowV3Release:
    release_generation: str
    local_generation: str
    local_source_generation: str
    policy_generation: str
    transport_generation: str
    endpoint_code_generation: str
    endpoint_id: str
    endpoint_generation: str
    registration_generation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "ascendop.flow.release.v3",
            "release_generation": self.release_generation,
            "local_generation": self.local_generation,
            "local_source_generation": self.local_source_generation,
            "policy_generation": self.policy_generation,
            "transport_generation": self.transport_generation,
            "endpoint_code_generation": self.endpoint_code_generation,
            "endpoint_id": self.endpoint_id,
            "endpoint_generation": self.endpoint_generation,
            "registration_generation": self.registration_generation,
            "wire_version": 3,
            "control_database_schema": SCHEMA_VERSION,
            "endpoint_database_schema": 1,
        }


@dataclass(frozen=True)
class FlowV3RuntimeConfig:
    root: Path
    database_path: Path
    package_root: Path
    result_root: Path
    route: EngineTransportRoute
    release: FlowV3Release
    wait_timeout_seconds: int
    node_report_cache_seconds: int
    node_liveness_query_timeout_seconds: int
    git_operation_timeout_seconds: int
    transport_package_source: Path
    endpoint_capacity: dict[str, int]


class FlowV3Runtime:
    def __init__(self, config: FlowV3RuntimeConfig) -> None:
        self.config = config
        self.store = FlowV3Store(config.database_path)
        self.transport = GitPartnerFlowV3Transport(
            GitPartnerTransportConfig(
                repo=resolve_path(config.root, config.route.gitpartner_repo),
                result_worktree=resolve_path(
                    config.root,
                    config.route.result_worktree
                    or config.route.gitpartner_repo,
                ),
                endpoint_id=config.route.endpoint_id,
                endpoint_generation=config.release.endpoint_generation,
                registration_generation=config.release.registration_generation,
                remote_root=config.route.remote_root,
                engine_root=config.route.engine_root,
                target_node=config.route.node_id,
                target_environment_id=config.route.execution_environment_id,
                transport=config.route.transport,
                transport_mode=config.route.transport_mode,
                control_channel=config.route.control_channel,
                node_report_cache_seconds=config.node_report_cache_seconds,
                node_liveness_query_timeout_seconds=(
                    config.node_liveness_query_timeout_seconds
                ),
                git_operation_timeout_seconds=config.git_operation_timeout_seconds,
                wait_timeout_seconds=config.wait_timeout_seconds,
                package_source=config.transport_package_source,
                package_generation=config.release.transport_generation,
                control_database=config.database_path,
            )
        )
        self.dispatcher = FlowV3Dispatcher(self.store, self.transport)
        self.ingestor = FlowV3ResultIngestor(
            self.store,
            self.transport,
            result_root=config.result_root,
            workflow_adapter=CannJudgeFlowV3WorkflowAdapter(
                config.root,
                gitpartner_repo=resolve_path(
                    config.root,
                    config.route.gitpartner_repo,
                ),
            ),
        )

    def register_local(self, *, lease_seconds: int = 60) -> None:
        for component_id, requirement in LOCAL_COMPONENTS.items():
            self._register_local_component(
                component_id,
                requirement,
                lease_seconds=lease_seconds,
            )

    def register_worker(self, *, lease_seconds: int = 60) -> None:
        for component_id, requirement in WORKER_COMPONENTS.items():
            self._register_local_component(
                component_id,
                requirement,
                lease_seconds=lease_seconds,
            )

    def register_observed_services(
        self,
        states: Mapping[str, bool],
        *,
        lease_seconds: int = 60,
    ) -> None:
        for component_id, requirement in OBSERVED_COMPONENTS.items():
            self.store.register_component(
                component_id=component_id,
                role=str(requirement["role"]),
                code_generation=self.config.release.local_generation,
                wire_min=3,
                wire_max=3,
                capabilities=list(requirement["capabilities"]),
                state="ready" if bool(states.get(component_id)) else "hold",
                boot_id=read_boot_id(),
                database_schema=FLOW_DB_SCHEMA,
                lease_seconds=lease_seconds,
            )

    def probe_endpoint(self) -> dict[str, Any]:
        report = self.transport.endpoint_report(force_refresh=True)
        node_capabilities = report.get("capabilities", {})
        if not isinstance(node_capabilities, Mapping):
            node_capabilities = {}
        flow_v3 = node_capabilities.get("flow_v3", {})
        if not isinstance(flow_v3, Mapping):
            flow_v3 = {}
        engine = node_capabilities.get("engine", {})
        if not isinstance(engine, Mapping):
            engine = {}
        node_features = {
            str(value)
            for value in node_capabilities.get("features", [])
            if str(value)
        }
        derived_flow_v3 = bool(
            str(engine.get("protocol_version") or "") == "engine-v3"
            and bool(engine.get("present"))
            and bool(engine.get("snapshot_available"))
            and REMOTE_NODE_FEATURES.issubset(node_features)
        )
        wire_versions = list(flow_v3.get("wire_versions", []))
        endpoint_schema = int(flow_v3.get("endpoint_db_schema", 0) or 0)
        endpoint_capabilities = list(flow_v3.get("capabilities", []))
        if derived_flow_v3 and not wire_versions:
            wire_versions = [3]
            endpoint_schema = 1
            endpoint_capabilities = list(REMOTE_ENDPOINT_CAPABILITIES)
        local_liveness = ControlDatabase(
            self.config.database_path
        ).observed_node_liveness(self.config.route.node_id)
        observation = {
            "schema": "ascendop.flow.endpoint-observation.v3",
            "endpoint_id": str(report.get("endpoint_id") or ""),
            "endpoint_generation": str(report.get("generation") or ""),
            "wire_versions": wire_versions,
            "endpoint_db_schema": endpoint_schema,
            "engine_protocol": str(
                flow_v3.get("engine_protocol")
                or engine.get("protocol_version")
                or ""
            ),
            "code_generation": str(
                flow_v3.get("code_generation")
                or engine.get("code_generation")
                or ""
            ),
            "capabilities": endpoint_capabilities,
            "engine": dict(engine),
            "node_report": {
                "boot_id": str(report.get("boot_id") or ""),
                "session_id": str(report.get("session_id") or ""),
                "sequence": int(report.get("sequence", 0) or 0),
                "source_heartbeat_at": str(report.get("heartbeat_at") or ""),
                "local_liveness": local_liveness,
            },
        }
        wire_versions = [
            int(value)
            for value in observation.get("wire_versions", [])
        ]
        endpoint_schema = int(
            observation.get("endpoint_db_schema", 0) or 0
        )
        capabilities = [
            str(value)
            for value in observation.get("capabilities", [])
            if str(value)
        ]
        capacity = engine.get("capacity", {})
        if not isinstance(capacity, Mapping):
            capacity = {}
        capacity_unreported = sorted(
            key
            for key in self.config.endpoint_capacity
            if key not in capacity or capacity.get(key) is None
        )
        capacity_mismatch = {
            key: {
                "expected": expected,
                "actual": int(capacity.get(key) or 0),
            }
            for key, expected in self.config.endpoint_capacity.items()
            if key in capacity
            and capacity.get(key) is not None
            and int(capacity.get(key) or 0) < expected
        }
        state = "ready"
        resident = engine.get("resident", {})
        if not isinstance(resident, Mapping):
            resident = {}
        if (
            str(observation.get("endpoint_id") or "")
            != self.config.release.endpoint_id
            or str(observation.get("endpoint_generation") or "")
            != self.config.release.endpoint_generation
            or str(observation.get("code_generation") or "")
            != self.config.release.endpoint_code_generation
            or 3 not in wire_versions
            or endpoint_schema != 1
            or capacity_mismatch
            or bool(capacity.get("draining"))
            or str(observation.get("engine_protocol") or "") != "engine-v3"
            or not bool(resident.get("resident_ok"))
            or not bool(resident.get("code_generation_current"))
            or not bool(local_liveness.get("live"))
            or str(local_liveness.get("admission_state") or "") != "accepted"
        ):
            state = "hold"
        self.store.register_component(
            component_id=REMOTE_COMPONENT_ID,
            role="endpoint",
            code_generation=str(observation.get("code_generation") or ""),
            wire_min=min(wire_versions or [0]),
            wire_max=max(wire_versions or [0]),
            capabilities=capabilities,
            state=state,
            boot_id=str(
                observation.get("node_report", {}).get("boot_id")
                or "remote-unreported"
            ),
            database_schema=endpoint_schema,
            lease_seconds=max(90, self.config.wait_timeout_seconds + 60),
        )
        return {
            **observation,
            "capacity_mismatch": capacity_mismatch,
            "capacity_unreported": capacity_unreported,
        }

    def readiness(self) -> dict[str, Any]:
        required: dict[str, Any] = {}
        for group in (
            LOCAL_COMPONENTS,
            WORKER_COMPONENTS,
            OBSERVED_COMPONENTS,
        ):
            for component_id, requirement in group.items():
                required[component_id] = {
                    **requirement,
                    "code_generation": self.config.release.local_generation,
                    "database_schema": FLOW_DB_SCHEMA,
                }
        required[REMOTE_COMPONENT_ID] = {
            "role": "endpoint",
            "code_generation": (
                self.config.release.endpoint_code_generation
            ),
            "database_schema": 1,
            "capabilities": [
                "flow-v3",
                "engine-archive",
                "correctness-first",
                "device-session-wall-budget",
                "weighted-host-scheduler",
                "diagnostic-profile",
                "profiler-primary-all-cases",
                "profiler-primary-roofline-all-cases",
            ],
        }
        readiness = self.store.readiness(
            required,
            code_generation=self.config.release.local_generation,
        )
        return {
            **readiness,
            "release": self.config.release.to_dict(),
        }

    def tick(self, *, allow_dispatch: bool = True) -> dict[str, Any]:
        self.register_worker()
        recovery = self.store.recover_generation_mismatch_terminals(
            actor="flow-v3-recovery",
        )
        readiness = self.readiness()
        accept_new = bool(allow_dispatch and readiness["ready"])
        reconcile_transport = self.has_transport_reconcile_work()
        dispatch = (
            self.dispatcher.dispatch_once(accept_new=accept_new)
            if accept_new or reconcile_transport
            else None
        )
        ingest = (
            self.ingestor.poll_once()
            if allow_dispatch or self.has_drain_work()
            else None
        )
        outcome = "progress" if recovery or dispatch or ingest else "idle"
        if outcome == "idle" and allow_dispatch and not readiness["ready"]:
            outcome = "held"
        return {
            "outcome": outcome,
            "recovery": recovery,
            "dispatch": dispatch,
            "ingest": ingest,
            "readiness": self.readiness(),
        }

    def tick_with_heartbeat(
        self,
        *,
        allow_dispatch: bool,
        heartbeat_interval_seconds: float,
        heartbeat: Any,
    ) -> dict[str, Any]:
        """Run one potentially blocking tick while its owner renews liveness."""

        interval = max(0.25, float(heartbeat_interval_seconds))
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                self.tick,
                allow_dispatch=allow_dispatch,
            )
            while True:
                done, _ = wait(
                    {future},
                    timeout=interval,
                    return_when=FIRST_COMPLETED,
                )
                if done:
                    return future.result()
                heartbeat()

    def has_drain_work(self) -> bool:
        return bool(
            self.store.attempts_in_states(
                {
                    "dispatched",
                    "accepted",
                    "running",
                    "return-ready",
                    "ingested",
                    "acknowledged",
                },
                limit=1,
            )
        )

    def has_transport_reconcile_work(self) -> bool:
        return bool(
            self.store.attempts_in_states(
                {"dispatched"},
                limit=1,
            )
        )

    def run(
        self,
        *,
        interval_seconds: float = 1.0,
        stop_requested: Any = None,
    ) -> dict[str, Any]:
        cycles = 0
        last: dict[str, Any] = {}
        interval = max(0.25, float(interval_seconds))
        while True:
            stopping = bool(
                callable(stop_requested) and stop_requested()
            )
            last = self.tick(allow_dispatch=not stopping)
            cycles += 1
            if stopping and not self.has_drain_work():
                break
            time.sleep(interval)
        return {
            **last,
            "outcome": "stopped",
            "cycles": cycles,
        }

    def _register_local_component(
        self,
        component_id: str,
        requirement: Mapping[str, Any],
        *,
        lease_seconds: int,
    ) -> None:
        self.store.register_component(
            component_id=component_id,
            role=str(requirement["role"]),
            code_generation=self.config.release.local_generation,
            wire_min=3,
            wire_max=3,
            capabilities=list(requirement["capabilities"]),
            state="ready",
            boot_id=read_boot_id(),
            database_schema=FLOW_DB_SCHEMA,
            lease_seconds=lease_seconds,
        )


def runtime_config(
    root: Path,
    policy: Mapping[str, Any],
) -> FlowV3RuntimeConfig:
    root = root.resolve()
    endpoint_id = str(policy.get("test_engine_endpoint_id") or "").strip()
    if not endpoint_id:
        raise ValueError("Flow V3 requires test_engine_endpoint_id")
    registry_path = str(
        policy.get("control_plane_registry")
        or "Develop/registry/system_registry.json"
    )
    database_value = str(
        policy.get("control_plane_database")
        or ".ascendop-work/runtime/control.sqlite3"
    )
    route = resolve_registered_engine_route(
        root,
        registry_path=registry_path,
        endpoint_id=endpoint_id,
        control_database_path=database_value,
    )
    gp_repo = resolve_path(root, route.gitpartner_repo)
    transport_package_source = gp_repo / "src"
    local_source_generation = flow_v3_local_generation(root)
    policy_generation = object_digest(flow_v3_policy_projection(policy))
    transport_generation = gitpartner_transport_generation(
        transport_package_source
    )
    local_generation = object_digest(
        {
            "local_source_generation": local_source_generation,
            "policy_generation": policy_generation,
            "transport_generation": transport_generation,
        }
    )
    endpoint_code_generation = expected_remote_engine_code_generation(
        root,
        gitpartner_repo=gp_repo,
    )
    if not endpoint_code_generation:
        raise ValueError("Flow V3 endpoint code generation is unavailable")
    release_material = {
        "local_generation": local_generation,
        "local_source_generation": local_source_generation,
        "policy_generation": policy_generation,
        "transport_generation": transport_generation,
        "endpoint_code_generation": endpoint_code_generation,
        "endpoint_id": route.endpoint_id,
        "endpoint_generation": route.registration_generation,
        "registration_generation": route.registration_generation,
        "wire_version": 3,
        "control_database_schema": SCHEMA_VERSION,
        "endpoint_database_schema": 1,
    }
    release_generation = object_digest(release_material)
    release = FlowV3Release(
        release_generation=release_generation,
        local_generation=local_generation,
        local_source_generation=local_source_generation,
        policy_generation=policy_generation,
        transport_generation=transport_generation,
        endpoint_code_generation=endpoint_code_generation,
        endpoint_id=route.endpoint_id,
        endpoint_generation=route.registration_generation,
        registration_generation=route.registration_generation,
    )
    return FlowV3RuntimeConfig(
        root=root,
        database_path=resolve_path(root, database_value),
        package_root=resolve_path(
            root,
            str(
                policy.get("flow_v3_package_root")
                or "TestUtils/tester_daemon/flow_v3/packages"
            ),
        ),
        result_root=resolve_path(
            root,
            str(
                policy.get("flow_v3_result_root")
                or "TestUtils/tester_daemon/flow_v3/results"
            ),
        ),
        route=route,
        release=release,
        wait_timeout_seconds=max(
            30,
            int(policy.get("test_engine_wait_timeout_seconds", 180) or 180),
        ),
        node_report_cache_seconds=max(
            0,
            int(policy.get("flow_v3_node_report_cache_seconds", 5) or 0),
        ),
        node_liveness_query_timeout_seconds=max(
            5,
            int(
                policy.get(
                    "flow_v3_node_liveness_query_timeout_seconds",
                    15,
                )
                or 15
            ),
        ),
        git_operation_timeout_seconds=max(
            15,
            min(
                120,
                int(
                    policy.get(
                        "flow_v3_git_operation_timeout_seconds",
                        60,
                    )
                    or 60
                ),
            ),
        ),
        transport_package_source=transport_package_source,
        endpoint_capacity={
            "max_inflight": max(
                1,
                int(policy.get("test_engine_target_inflight", 4) or 4),
            ),
            "active_job_slots": max(
                1,
                int(policy.get("test_engine_active_job_slots", 4) or 4),
            ),
            "host_slots": max(
                1,
                int(policy.get("flow_v3_host_slots", 4) or 4),
            ),
            "host_cpu_weight_capacity": max(
                1,
                int(
                    policy.get(
                        "flow_v3_host_cpu_weight_capacity",
                        4,
                    )
                    or 4
                ),
            ),
            "host_memory_mb_capacity": max(
                1024,
                int(
                    policy.get(
                        "flow_v3_host_memory_mb_capacity",
                        16384,
                    )
                    or 16384
                ),
            ),
            "host_io_weight_capacity": max(
                1,
                int(
                    policy.get(
                        "flow_v3_host_io_weight_capacity",
                        4,
                    )
                    or 4
                ),
            ),
            "cold_build_slots": 1,
            "cache_hit_slots": max(
                1,
                int(policy.get("flow_v3_cache_hit_slots", 4) or 4),
            ),
            "device_slots": 1,
            "export_slots": max(
                1,
                int(policy.get("flow_v3_export_slots", 1) or 1),
            ),
        },
    )


def flow_v3_local_generation(root: Path) -> str:
    daemon_root = (
        root / "tools" / "tester_daemon" / "src" / "ascendop_daemon"
    )
    protocol_root = (
        root / "packages" / "ascendop_protocol" / "src" / "ascendop_protocol"
    )
    paths = [
        root / "tools" / "tester_daemon" / "daemon.py",
        root / "tools" / "tester_daemon" / "launch_s5_910b.py",
        *sorted(daemon_root.rglob("*.py"), key=lambda item: item.as_posix()),
        *sorted(protocol_root.rglob("*.py"), key=lambda item: item.as_posix()),
        *sorted(protocol_root.rglob("*.json"), key=lambda item: item.as_posix()),
        root
        / "docs"
        / "engine_exchange_protocol"
        / "v3"
        / "variables.json",
    ]
    digest = hashlib.sha256()
    for path in paths:
        if not path.is_file():
            raise ValueError(f"Flow V3 release file is missing: {path}")
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        data = path.read_bytes()
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def flow_v3_policy_projection(
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    exact_keys = {
        "control_plane_database",
        "control_plane_registry",
        "test_executor",
    }
    prefixes = ("flow_v3_", "test_engine_")
    return {
        str(key): policy[key]
        for key in sorted(policy)
        if str(key) in exact_keys
        or any(str(key).startswith(prefix) for prefix in prefixes)
    }


def write_release_manifest(config: FlowV3RuntimeConfig) -> Path:
    path = (
        config.root
        / "TestUtils"
        / "tester_daemon"
        / "flow_v3"
        / "RELEASE.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(
            config.release.to_dict(),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return path


def endpoint_status_control_ref(release: FlowV3Release) -> str:
    material = (
        f"{release.endpoint_id}:{release.endpoint_generation}:"
        f"{release.release_generation}:status"
    ).encode("utf-8")
    return f"flowv3-status-{hashlib.sha256(material).hexdigest()[:24]}"


def object_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resolve_path(root: Path, value: str) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def process_identity() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{read_boot_id()}"
