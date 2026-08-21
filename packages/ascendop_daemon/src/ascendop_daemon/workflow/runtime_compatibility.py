from __future__ import annotations

from ascendop_daemon.workflow.operator_job_errors import EngineJobBuildError


RUNTIME_COMPATIBILITY_FLAGS = {
    "correctness-single-round-custom-op",
    "ge-fixed-output-dtype-fallback",
    "runtime-boundary-trace",
    "native-workspace-query-attribution",
    "host-callback-attribution",
    "kernel-fault-attribution",
}


def validate_runtime_compatibility(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(str(item).strip() for item in values if str(item).strip())
    if len(normalized) != len(set(normalized)):
        raise EngineJobBuildError("runtime compatibility flags must be unique")
    unsupported = sorted(set(normalized) - RUNTIME_COMPATIBILITY_FLAGS)
    if unsupported:
        raise EngineJobBuildError(
            "unsupported runtime compatibility: " + ", ".join(unsupported)
        )
    return normalized


def runtime_compatibility_fragments(values: tuple[str, ...]) -> list[str]:
    fragments = ['export ASCENDOP_RUNTIME_COMPATIBILITY="' + ",".join(values) + '"']
    if "ge-fixed-output-dtype-fallback" in values:
        fragments.append("export IGNORE_INFER_ERROR=1")
    if {
        "runtime-boundary-trace",
        "native-workspace-query-attribution",
        "host-callback-attribution",
    }.intersection(values):
        fragments.extend(
            [
                "export ASCEND_LAUNCH_BLOCKING=1",
                "export ASCENDOP_RUNTIME_BOUNDARY_TRACE=1",
            ]
        )
    if "native-workspace-query-attribution" in values:
        fragments.append("export ASCENDOP_NATIVE_WORKSPACE_QUERY_ATTRIBUTION=1")
    if "host-callback-attribution" in values:
        fragments.append("export ASCENDOP_HOST_CALLBACK_ATTRIBUTION=1")
    if "kernel-fault-attribution" in values:
        if not {
            "runtime-boundary-trace",
            "native-workspace-query-attribution",
        }.intersection(values):
            fragments.append("export ASCEND_LAUNCH_BLOCKING=1")
        fragments.append("export ASCENDOP_KERNEL_FAULT_ATTRIBUTION=1")
    return fragments
