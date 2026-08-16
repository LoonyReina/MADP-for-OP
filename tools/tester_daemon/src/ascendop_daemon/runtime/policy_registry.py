from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.schemas import VariableRegistry, VariableRegistryError


VARIABLE_REGISTRY_PATH = Path("docs/engine_exchange_protocol/v3/variables.json")

# Legacy configuration keys are accepted only at this release boundary. Runtime
# components consume the registered IDs below, never these aliases directly.
CONFIG_BINDINGS = {
    "automation.official_progress_path": "flow_v3_official_progress_path",
    "automation.official_progress_profile_glob": "flow_v3_official_progress_profile_glob",
    "automation.workflow_relay_target_id": "flow_v3_workflow_relay_target_id",
    "runtime.worker_interval_seconds": "flow_v3_worker_interval_seconds",
    "runtime.endpoint_probe_interval_seconds": "flow_v3_endpoint_probe_interval_seconds",
    "runtime.stop_command_timeout_seconds": "flow_v3_stop_command_timeout_seconds",
    "transport.node_report_cache_seconds": "flow_v3_node_report_cache_seconds",
    "transport.node_report_max_concurrency": "flow_v3_node_report_max_concurrency",
    "transport.node_liveness_query_timeout_seconds": "flow_v3_node_liveness_query_timeout_seconds",
    "transport.wait_timeout_seconds": "test_engine_wait_timeout_seconds",
    "transport.git_operation_timeout_seconds": "flow_v3_git_operation_timeout_seconds",
    "profiler.process_timeout_seconds": "test_engine_profiler_timeout_seconds",
    "host.cold_build_concurrency": "flow_v3_cold_build_slots",
    "host.cache_hit_concurrency": "flow_v3_cache_hit_slots",
    "host.cpu_weight_capacity": "flow_v3_host_cpu_weight_capacity",
    "host.memory_capacity_mb": "flow_v3_host_memory_mb_capacity",
    "host.io_weight_capacity": "flow_v3_host_io_weight_capacity",
}


@dataclass(frozen=True)
class RuntimePolicy:
    registry: VariableRegistry
    values: Mapping[str, Any]

    @classmethod
    def load(
        cls,
        root: Path,
        config_policy: Mapping[str, Any],
        *,
        database_schema: int,
    ) -> "RuntimePolicy":
        configured_path = os.environ.get("ASCENDOP_VARIABLE_REGISTRY_PATH", "")
        registry_path = (
            Path(configured_path).resolve()
            if configured_path
            else root / VARIABLE_REGISTRY_PATH
        )
        registry = VariableRegistry.load(registry_path)
        overrides = {
            variable_id: config_policy[config_key]
            for variable_id, config_key in CONFIG_BINDINGS.items()
            if config_key in config_policy
        }
        values = registry.resolve(overrides)
        declared_schema = values["database.control_schema"]
        if declared_schema != database_schema:
            raise VariableRegistryError(
                "control database schema does not match the variable registry: "
                f"runtime={database_schema}, registry={declared_schema}"
            )
        if values["wire.version"] != 3:
            raise VariableRegistryError("Wire V3 runtime requires wire.version=3")
        return cls(registry=registry, values=values)

    def get(self, variable_id: str) -> Any:
        if variable_id not in self.values:
            raise VariableRegistryError(
                f"runtime requested an unregistered variable: {variable_id}"
            )
        return self.values[variable_id]
