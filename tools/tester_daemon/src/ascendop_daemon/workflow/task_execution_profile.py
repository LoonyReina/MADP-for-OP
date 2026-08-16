from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.task_profile import (
    TASK_PROFILE_SCHEMA,
    TaskExecutionProfile,
    TaskProfileError,
    parse_task_execution_profile,
)


TASK_EXECUTION_PROFILE_NAME = "TASK_EXECUTION_PROFILE.json"


class TaskExecutionProfileError(RuntimeError):
    pass


def ensure_task_execution_profile(
    root: Path,
    registration: Mapping[str, Any],
) -> tuple[Path, dict[str, Any], TaskExecutionProfile]:
    root = _resolved_path(root)
    workspace = _object(registration.get("workspace"), "workspace")
    source = _relative_path(workspace.get("source"), "workspace.source")
    workspace_root = _resolved_path(root / source)
    _ensure_bounded(root, workspace_root)
    path = workspace_root / TASK_EXECUTION_PROFILE_NAME
    if path.is_file():
        raw = _read_profile(path)
        profile = _parse_profile(path, raw)
        _validate_registration_identity(profile, registration, source)
        return path, raw, profile

    raw = build_task_execution_profile(registration)
    profile = _parse_profile(path, raw)
    workspace_root.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(raw, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    temporary = path.with_name(f".profile-{uuid.uuid4().hex[:8]}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    except FileExistsError:
        existing = _read_profile(path)
        parsed = _parse_profile(path, existing)
        _validate_registration_identity(parsed, registration, source)
        return path, existing, parsed
    finally:
        temporary.unlink(missing_ok=True)
    return path, raw, profile


def build_task_execution_profile(
    registration: Mapping[str, Any],
) -> dict[str, Any]:
    requirements = _object(registration.get("requirements"), "requirements")
    routing = _object(registration.get("routing_policy"), "routing_policy")
    workspace = _object(registration.get("workspace"), "workspace")
    endpoint_id = str(routing.get("endpoint_id") or "").strip()
    route_mode = "pinned" if endpoint_id else "dynamic"
    budget = _object_or_empty(routing.get("budget"))
    budget_class = str(budget.get("class") or "standard")
    requested = budget.get("requested_device_session_seconds")
    return {
        "schema": TASK_PROFILE_SCHEMA,
        "task_id": str(registration.get("operator_id") or ""),
        "backend_pool": str(
            requirements.get("backend_pool")
            or routing.get("backend_pool")
            or ""
        ),
        "environment": {
            "soc": _string_list(requirements.get("soc"), default=["*"]),
            "cann": _string_list(requirements.get("cann"), default=["*"]),
            "operating_systems": _string_list(
                requirements.get("operating_systems"),
                default=["*"],
            ),
        },
        "capabilities": _string_list(
            requirements.get("features"),
            default=["operator-test"],
        ),
        "routing": {
            "mode": route_mode,
            **({"endpoint_id": endpoint_id} if endpoint_id else {}),
        },
        "budget": {
            "class": budget_class,
            "requested_device_session_seconds": requested,
            "policy": str(budget.get("policy") or "daemon-approved-v1"),
        },
        "origin_workspace": _relative_path(
            workspace.get("source"),
            "workspace.source",
        ),
        "extensions": {},
    }


def _read_profile(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TaskExecutionProfileError(f"cannot read task profile {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise TaskExecutionProfileError(f"task profile must be an object: {path}")
    return value


def _parse_profile(path: Path, raw: Mapping[str, Any]) -> TaskExecutionProfile:
    try:
        return parse_task_execution_profile(raw)
    except TaskProfileError as exc:
        raise TaskExecutionProfileError(f"invalid task profile {path}: {exc}") from exc


def _validate_registration_identity(
    profile: TaskExecutionProfile,
    registration: Mapping[str, Any],
    source: str,
) -> None:
    if profile.task_id != str(registration.get("operator_id") or ""):
        raise TaskExecutionProfileError("task profile operator identity is stale")
    if profile.origin_workspace != source:
        raise TaskExecutionProfileError("task profile origin workspace is stale")


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TaskExecutionProfileError(f"{field} must be an object")
    return value


def _object_or_empty(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _string_list(value: Any, *, default: list[str]) -> list[str]:
    if not isinstance(value, list):
        return list(default)
    normalized = [str(item).strip() for item in value if str(item).strip()]
    return normalized or list(default)


def _relative_path(value: Any, field: str) -> str:
    text = str(value or "").replace("\\", "/").strip("/")
    if not text or ".." in Path(text).parts:
        raise TaskExecutionProfileError(f"{field} must be a bounded relative path")
    return text


def _ensure_bounded(root: Path, path: Path) -> None:
    if path != root and root not in path.parents:
        raise TaskExecutionProfileError(f"task workspace escapes repository: {path}")


def _resolved_path(path: Path) -> Path:
    text = os.fspath(path.resolve())
    if text.startswith("\\\\?\\UNC\\"):
        text = "\\\\" + text[8:]
    elif text.startswith("\\\\?\\"):
        text = text[4:]
    return Path(text)
