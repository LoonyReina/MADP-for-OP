from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from ascendop_daemon.runtime.locking import process_alive


@dataclass(frozen=True)
class WorkerProcessStatus:
    active: bool
    effective_pid: int
    worker_pid: int
    child_pid: int
    heartbeat_pid: int
    heartbeat_active: bool
    heartbeat_age_seconds: int | None
    adopted_child: bool
    reason: str


def worker_process_status(
    root: Path,
    action_id: str,
    worker: dict[str, Any] | None = None,
) -> WorkerProcessStatus:
    worker_pid = int((worker or {}).get("pid", 0) or 0)
    if worker_pid > 0 and process_alive(worker_pid):
        return WorkerProcessStatus(
            active=True,
            effective_pid=worker_pid,
            worker_pid=worker_pid,
            child_pid=0,
            heartbeat_pid=0,
            heartbeat_active=False,
            heartbeat_age_seconds=None,
            adopted_child=False,
            reason="worker_pid_alive",
        )

    heartbeat = read_worker_heartbeat(root, action_id)
    heartbeat_pid = int(heartbeat.get("pid", 0) or 0)
    child_pid = int(heartbeat.get("child_pid", 0) or 0)
    heartbeat_active = bool(heartbeat.get("active"))
    heartbeat_age_seconds = heartbeat_age(heartbeat)

    if heartbeat_active and heartbeat_pid > 0 and process_alive(heartbeat_pid):
        return WorkerProcessStatus(
            active=True,
            effective_pid=heartbeat_pid,
            worker_pid=worker_pid,
            child_pid=child_pid,
            heartbeat_pid=heartbeat_pid,
            heartbeat_active=True,
            heartbeat_age_seconds=heartbeat_age_seconds,
            adopted_child=worker_pid > 0 and worker_pid != heartbeat_pid,
            reason="heartbeat_pid_alive",
        )
    if heartbeat_active and child_pid > 0 and process_alive(child_pid):
        return WorkerProcessStatus(
            active=True,
            effective_pid=child_pid,
            worker_pid=worker_pid,
            child_pid=child_pid,
            heartbeat_pid=heartbeat_pid,
            heartbeat_active=True,
            heartbeat_age_seconds=heartbeat_age_seconds,
            adopted_child=True,
            reason="child_pid_alive",
        )

    reason = "no_live_worker_process"
    if worker_pid <= 0 and not heartbeat:
        reason = "missing_worker_pid_and_heartbeat"
    elif heartbeat and not heartbeat_active:
        reason = "heartbeat_inactive"
    return WorkerProcessStatus(
        active=False,
        effective_pid=0,
        worker_pid=worker_pid,
        child_pid=child_pid,
        heartbeat_pid=heartbeat_pid,
        heartbeat_active=heartbeat_active,
        heartbeat_age_seconds=heartbeat_age_seconds,
        adopted_child=False,
        reason=reason,
    )


def read_worker_heartbeat(root: Path, action_id: str) -> dict[str, Any]:
    path = (
        root
        / "TestUtils"
        / "tester_daemon"
        / "execute_worker_heartbeats"
        / f"{action_digest(action_id)}.json"
    )
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def heartbeat_age(heartbeat: dict[str, Any]) -> int | None:
    timestamp = parse_timestamp(str(heartbeat.get("time", "") or ""))
    if timestamp is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - timestamp).total_seconds()))


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def action_digest(action_id: str) -> str:
    return sha1(action_id.encode("utf-8")).hexdigest()[:12]
