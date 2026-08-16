from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Mapping


TASK_PROFILE_SCHEMA = "ascendop.task-execution-profile.v1"
ROUTE_MODES = {"dynamic", "pinned"}
BUDGET_CLASSES = {"standard", "heavy", "diagnostic", "maintenance"}
RUNTIME_COMPATIBILITY_FLAGS = {
    "correctness-single-round-custom-op",
    "ge-fixed-output-dtype-fallback",
}


class TaskProfileError(ValueError):
    pass


@dataclass(frozen=True)
class TaskExecutionProfile:
    task_id: str
    backend_pool: str
    soc: tuple[str, ...]
    cann: tuple[str, ...]
    operating_systems: tuple[str, ...]
    capabilities: tuple[str, ...]
    route_mode: str
    endpoint_id: str | None
    budget_class: str
    requested_device_session_seconds: int | None
    origin_workspace: str
    runtime_compatibility: tuple[str, ...]
    runtime_operator_name: str

    def requirements(self) -> dict[str, Any]:
        value = {
            "backend_pool": self.backend_pool,
            "soc": list(self.soc),
            "cann": list(self.cann),
            "operating_systems": list(self.operating_systems),
            "features": list(self.capabilities),
        }
        if self.route_mode == "pinned" and self.endpoint_id:
            value["allowed_endpoints"] = [self.endpoint_id]
        return value


def parse_task_execution_profile(raw: Mapping[str, Any]) -> TaskExecutionProfile:
    if not isinstance(raw, Mapping):
        raise TaskProfileError("task execution profile must be an object")
    if raw.get("schema") != TASK_PROFILE_SCHEMA:
        raise TaskProfileError(f"unsupported task profile schema: {raw.get('schema')}")
    task_id = _text(raw.get("task_id"), "task_id")
    backend_pool = _text(raw.get("backend_pool"), "backend_pool")
    environment = _object(raw.get("environment"), "environment")
    routing = _object(raw.get("routing"), "routing")
    budget = _object(raw.get("budget"), "budget")
    route_mode = _text(routing.get("mode"), "routing.mode")
    if route_mode not in ROUTE_MODES:
        raise TaskProfileError(f"unsupported routing mode: {route_mode}")
    endpoint_id = _optional_text(routing.get("endpoint_id"), "routing.endpoint_id")
    if route_mode == "pinned" and not endpoint_id:
        raise TaskProfileError("pinned routing requires routing.endpoint_id")
    if route_mode == "dynamic" and endpoint_id:
        raise TaskProfileError("dynamic routing must not declare routing.endpoint_id")
    budget_class = _text(budget.get("class"), "budget.class")
    if budget_class not in BUDGET_CLASSES:
        raise TaskProfileError(f"unsupported budget class: {budget_class}")
    requested = budget.get("requested_device_session_seconds")
    if requested is not None and (
        isinstance(requested, bool) or not isinstance(requested, int) or requested < 1
    ):
        raise TaskProfileError(
            "budget.requested_device_session_seconds must be a positive integer"
        )
    origin_workspace = _relative_path(raw.get("origin_workspace"), "origin_workspace")
    extensions = raw.get("extensions", {})
    if not isinstance(extensions, Mapping):
        raise TaskProfileError("extensions must be an object")
    runtime_operator_name = _optional_text(
        extensions.get("ascendop.profiler/operator_name"),
        "extensions.ascendop.profiler/operator_name",
    ) or task_id
    return TaskExecutionProfile(
        task_id=task_id,
        backend_pool=backend_pool,
        soc=_text_tuple(environment.get("soc"), "environment.soc"),
        cann=_text_tuple(environment.get("cann"), "environment.cann"),
        operating_systems=_text_tuple(
            environment.get("operating_systems"), "environment.operating_systems"
        ),
        capabilities=_text_tuple(raw.get("capabilities"), "capabilities"),
        route_mode=route_mode,
        endpoint_id=endpoint_id,
        budget_class=budget_class,
        requested_device_session_seconds=requested,
        origin_workspace=origin_workspace,
        runtime_compatibility=_runtime_compatibility(
            raw.get("runtime_compatibility")
        ),
        runtime_operator_name=runtime_operator_name,
    )


def _runtime_compatibility(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if value == []:
        return ()
    flags = _text_tuple(value, "runtime_compatibility")
    duplicates = sorted({item for item in flags if flags.count(item) > 1})
    if duplicates:
        raise TaskProfileError(
            "runtime_compatibility contains duplicates: " + ", ".join(duplicates)
        )
    unsupported = sorted(set(flags) - RUNTIME_COMPATIBILITY_FLAGS)
    if unsupported:
        raise TaskProfileError(
            "unsupported runtime compatibility: " + ", ".join(unsupported)
        )
    return flags


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TaskProfileError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TaskProfileError(f"{field} must be non-empty text")
    return value.strip()


def _optional_text(value: Any, field: str) -> str | None:
    if value is None or value == "":
        return None
    return _text(value, field)


def _text_tuple(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise TaskProfileError(f"{field} must be a non-empty list")
    return tuple(_text(item, field) for item in value)


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts:
        raise TaskProfileError(f"{field} must be a bounded relative path")
    return str(path)
