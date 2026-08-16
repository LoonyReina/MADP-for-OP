from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.core.atomic_io import write_json_atomic


ATTEMPT_SCHEMA = "ascendop.windows-bootstrap-attempt.v1"
EVENT_SCHEMA = "ascendop.windows-bootstrap-event.v1"
ATTEMPT_PATH_ENV = "ASCENDOP_WINDOWS_BOOTSTRAP_ATTEMPT_PATH"
ATTEMPT_ID_ENV = "ASCENDOP_WINDOWS_BOOTSTRAP_ATTEMPT_ID"
BOUNDARY_ORDER = (
    "demand_accepted",
    "task_instance_visible",
    "action_started",
    "python_entered",
    "generation_verified",
    "child_adopted_or_started",
    "resident_probe_ready",
    "bootstrap_terminal",
)


class WindowsBootstrapError(RuntimeError):
    pass


def active_attempt_path() -> Path | None:
    raw = str(os.environ.get(ATTEMPT_PATH_ENV) or "").strip()
    return Path(raw).resolve() if raw else None


def record_boundary(
    boundary: str,
    *,
    details: Mapping[str, Any] | None = None,
    boundary_state: str = "observed",
    terminal_state: str | None = None,
    failure: Mapping[str, str] | None = None,
) -> dict[str, Any] | None:
    path = active_attempt_path()
    if path is None:
        return None
    return record_boundary_at(
        path,
        boundary,
        details=details,
        boundary_state=boundary_state,
        terminal_state=terminal_state,
        failure=failure,
    )


def record_boundary_at(
    path: Path,
    boundary: str,
    *,
    details: Mapping[str, Any] | None = None,
    boundary_state: str = "observed",
    terminal_state: str | None = None,
    failure: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if boundary not in BOUNDARY_ORDER:
        raise WindowsBootstrapError(f"unknown bootstrap boundary: {boundary}")
    if boundary_state not in {"pending", "observed", "missing", "failed"}:
        raise WindowsBootstrapError(
            f"invalid bootstrap boundary state: {boundary_state}"
        )
    if terminal_state not in {None, "running", "succeeded", "paused", "failed", "quarantined"}:
        raise WindowsBootstrapError(
            f"invalid bootstrap terminal state: {terminal_state}"
        )
    projection = _read_projection(path)
    now_utc = _utc_now()
    now_monotonic = time.monotonic_ns()
    rows = projection.get("boundaries")
    if not isinstance(rows, list) or len(rows) != len(BOUNDARY_ORDER):
        raise WindowsBootstrapError("bootstrap projection has invalid boundaries")
    expected_index = BOUNDARY_ORDER.index(boundary)
    row = rows[expected_index]
    if not isinstance(row, dict) or row.get("boundary") != boundary:
        raise WindowsBootstrapError("bootstrap boundary order is corrupted")
    row.update(
        {
            "state": boundary_state,
            "observed_at_utc": now_utc,
            "monotonic_ns": now_monotonic,
            "details": dict(details or {}),
        }
    )
    projection["last_event_at_utc"] = now_utc
    if terminal_state is not None:
        projection["state"] = terminal_state
    if failure is not None:
        projection["failure"] = {
            "failure_class": str(failure.get("failure_class") or "bootstrap"),
            "message": str(failure.get("message") or "bootstrap failed"),
            "boundary": str(failure.get("boundary") or boundary),
        }
    event = {
        "schema": EVENT_SCHEMA,
        "attempt_id": projection["attempt_id"],
        "sequence": expected_index + 1,
        "boundary": boundary,
        "state": boundary_state,
        "observed_at_utc": now_utc,
        "monotonic_ns": now_monotonic,
        "pid": os.getpid(),
        "details": dict(details or {}),
    }
    _append_event(Path(str(projection["journal_path"])), event)
    write_json_atomic(path, projection, ensure_ascii=True, sort_keys=True)
    latest = path.parent.parent / "latest-windows-bootstrap-attempt.json"
    write_json_atomic(latest, projection, ensure_ascii=True, sort_keys=True)
    return projection


def record_ensure_result(result: Mapping[str, Any]) -> dict[str, Any] | None:
    path = active_attempt_path()
    if path is None:
        return None
    outcome = str(result.get("outcome") or "")
    record_boundary_at(
        path,
        "child_adopted_or_started",
        details={
            "outcome": outcome,
            "resident_pid": int(result.get("pid") or 0),
            "resident_start_token": str(result.get("start_token") or ""),
            "release_generation": str(result.get("generation") or ""),
        },
    )
    if bool(result.get("healthy")):
        record_boundary_at(
            path,
            "resident_probe_ready",
            details={
                "resident_pid": int(result.get("pid") or 0),
                "release_generation": str(result.get("generation") or ""),
                "outcome": outcome,
            },
        )
        return record_boundary_at(
            path,
            "bootstrap_terminal",
            details={"outcome": outcome},
            terminal_state="succeeded",
        )
    return record_boundary_at(
        path,
        "bootstrap_terminal",
        details={"outcome": outcome},
        boundary_state="failed",
        terminal_state="failed",
        failure={
            "failure_class": "resident-not-ready",
            "message": f"ensure-resident ended without a healthy resident: {outcome}",
            "boundary": "resident_probe_ready",
        },
    )


def record_ensure_failure(exc: BaseException) -> dict[str, Any] | None:
    path = active_attempt_path()
    if path is None:
        return None
    return record_boundary_at(
        path,
        "bootstrap_terminal",
        details={"exception_type": type(exc).__name__},
        boundary_state="failed",
        terminal_state="failed",
        failure={
            "failure_class": "resident-bootstrap",
            "message": str(exc) or type(exc).__name__,
            "boundary": _first_pending_boundary(_read_projection(path)),
        },
    )


def _read_projection(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WindowsBootstrapError(
            f"cannot read bootstrap projection {path}: {exc}"
        ) from exc
    if not isinstance(value, dict) or value.get("schema") != ATTEMPT_SCHEMA:
        raise WindowsBootstrapError("unsupported bootstrap projection")
    return value


def _append_event(path: Path, event: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(event, ensure_ascii=True, sort_keys=True) + "\n").encode(
        "ascii"
    )
    with path.open("ab", buffering=0) as handle:
        handle.write(payload)
        os.fsync(handle.fileno())


def _first_pending_boundary(projection: Mapping[str, Any]) -> str:
    for row in projection.get("boundaries") or []:
        if isinstance(row, dict) and row.get("state") == "pending":
            return str(row.get("boundary") or "bootstrap_terminal")
    return "bootstrap_terminal"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
