from __future__ import annotations

from typing import Any

from ascendop_daemon.registry.models import BackendEndpoint
from ascendop_daemon.registry.topology_parser import string_list

def route_rejection_reasons(
    endpoint: BackendEndpoint, requirements: dict[str, Any]
) -> list[str]:
    reasons: list[str] = []
    if not endpoint.enabled:
        reasons.append("endpoint-disabled")
    if endpoint.draining:
        reasons.append("endpoint-draining")
    if not endpoint.gateway_enabled:
        reasons.append("gateway-disabled")
    if endpoint.gateway_draining:
        reasons.append("gateway-draining")
    if not endpoint.node_enabled:
        reasons.append("node-disabled")
    if endpoint.node_draining:
        reasons.append("node-draining")
    if not endpoint.environment_enabled:
        reasons.append("environment-disabled")
    if endpoint.environment_draining:
        reasons.append("environment-draining")
    allowed = set(string_list(requirements.get("allowed_endpoints")))
    if allowed and endpoint.endpoint_id not in allowed:
        reasons.append("endpoint-not-allowed")
    pool = str(requirements.get("backend_pool") or "")
    if pool and endpoint.backend_pool != pool:
        reasons.append(f"backend-pool-mismatch:{endpoint.backend_pool or 'none'}")
    transport = str(requirements.get("transport") or "")
    if transport and endpoint.transport not in {transport, "auto"}:
        reasons.append(f"transport-mismatch:{endpoint.transport}")
    for key in ("soc", "cann"):
        required = set(string_list(requirements.get(key)))
        available = set(string_list(endpoint.capabilities.get(key)))
        if required and "*" not in required and "*" not in available:
            if not required.intersection(available):
                reasons.append(f"{key}-mismatch")
    required_os = set(string_list(requirements.get("operating_systems")))
    available_os = set(
        string_list(endpoint.capabilities.get("operating_systems"))
    )
    if required_os and "*" not in required_os and "*" not in available_os:
        if not required_os.intersection(available_os):
            reasons.append("operating-systems-mismatch")
    for key in ("features", "cache_adapters"):
        missing = sorted(
            set(string_list(requirements.get(key)))
            - set(string_list(endpoint.capabilities.get(key)))
        )
        if missing:
            reasons.append(f"missing-{key}:" + ",".join(missing))
    required_raw = requirements.get("device_count", 0)
    available_raw = endpoint.capabilities.get("device_count", 0)
    required_devices = int(0 if required_raw is None else required_raw)
    available_devices = int(0 if available_raw is None else available_raw)
    if required_devices < 0 or available_devices < 0:
        reasons.append("invalid-device-count")
        return reasons
    if required_devices > available_devices:
        reasons.append(
            f"device-count-mismatch:required={required_devices};available={available_devices}"
        )
    return reasons
