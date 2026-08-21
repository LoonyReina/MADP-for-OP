from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


RUNTIME_BOUNDARY_TRACE = "runtime-boundary-trace"
NATIVE_WORKSPACE_QUERY_ATTRIBUTION = "native-workspace-query-attribution"
HOST_CALLBACK_ATTRIBUTION = "host-callback-attribution"
KERNEL_FAULT_ATTRIBUTION = "kernel-fault-attribution"


@dataclass(frozen=True)
class DiagnosticRequestCapabilities:
    runtime_compatibility: tuple[str, ...]
    required_capabilities: tuple[str, ...]


def resolve_diagnostic_request_capabilities(
    requested_artifacts: Iterable[object],
    runtime_compatibility: tuple[str, ...],
) -> DiagnosticRequestCapabilities:
    requested = {str(item) for item in requested_artifacts if str(item)}
    native_requested = NATIVE_WORKSPACE_QUERY_ATTRIBUTION in requested
    host_requested = HOST_CALLBACK_ATTRIBUTION in requested
    kernel_requested = KERNEL_FAULT_ATTRIBUTION in requested
    trace_requested = (
        RUNTIME_BOUNDARY_TRACE in requested
        or native_requested
        or host_requested
    )
    diagnostic_flags = (
        *((RUNTIME_BOUNDARY_TRACE,) if trace_requested else ()),
        *((NATIVE_WORKSPACE_QUERY_ATTRIBUTION,) if native_requested else ()),
        *((HOST_CALLBACK_ATTRIBUTION,) if host_requested else ()),
        *((KERNEL_FAULT_ATTRIBUTION,) if kernel_requested else ()),
    )
    return DiagnosticRequestCapabilities(
        runtime_compatibility=tuple(
            dict.fromkeys((*runtime_compatibility, *diagnostic_flags))
        ),
        required_capabilities=diagnostic_flags,
    )


__all__ = [
    "DiagnosticRequestCapabilities",
    "resolve_diagnostic_request_capabilities",
]
