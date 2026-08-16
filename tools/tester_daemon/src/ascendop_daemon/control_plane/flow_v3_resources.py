from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


class FlowV3ResourceError(ValueError):
    pass


@dataclass(frozen=True)
class HostCapacity:
    cpu_weight: int = 4
    memory_mb: int = 16384
    io_weight: int = 4
    cold_build_concurrency: int = 1
    cache_hit_concurrency: int = 4


@dataclass(frozen=True)
class HostDemand:
    resource_key: str
    cpu_weight: int
    memory_mb: int
    io_weight: int
    count_limit: int
    singleflight_key: str


def host_demand(
    stage: Mapping[str, Any],
    *,
    endpoint_id: str,
    cache_hit: bool,
    cache_key: str = "",
) -> HostDemand:
    resource_class = str(stage.get("resource_class") or "")
    if resource_class not in {"host-light", "host-build-heavy"}:
        raise FlowV3ResourceError(
            f"stage is not a host resource: {resource_class}"
        )
    if resource_class == "host-build-heavy" and not cache_hit:
        return HostDemand(
            resource_key=f"host:{endpoint_id}:cold-build",
            cpu_weight=4,
            memory_mb=8192,
            io_weight=4,
            count_limit=1,
            singleflight_key=(
                f"singleflight:{endpoint_id}:{cache_key}" if cache_key else ""
            ),
        )
    return HostDemand(
        resource_key=f"host:{endpoint_id}:cache-hit",
        cpu_weight=1,
        memory_mb=1024,
        io_weight=1,
        count_limit=4,
        singleflight_key=(
            f"singleflight:{endpoint_id}:{cache_key}" if cache_key else ""
        ),
    )


def demand_fits(
    demand: HostDemand,
    usage: Mapping[str, int],
    capacity: HostCapacity,
) -> bool:
    count_limit = (
        capacity.cold_build_concurrency
        if demand.resource_key.endswith(":cold-build")
        else capacity.cache_hit_concurrency
    )
    return bool(
        int(usage.get("count", 0)) + 1 <= min(count_limit, demand.count_limit)
        and int(usage.get("cpu_weight", 0)) + demand.cpu_weight
        <= capacity.cpu_weight
        and int(usage.get("memory_mb", 0)) + demand.memory_mb
        <= capacity.memory_mb
        and int(usage.get("io_weight", 0)) + demand.io_weight
        <= capacity.io_weight
    )
