from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.storage.control_types import SCHEMA_VERSION
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.runtime.application_support import LOCAL_SERVICES
from ascendop_daemon.runtime.control import (
    clear_stop_request,
    read_stop_request,
    write_stop_request,
)
from ascendop_daemon.runtime.locking import (
    DaemonLock,
    NamedProcessLock,
    lock_owner_alive,
    read_lock_metadata,
    read_lock_pid,
)
from ascendop_daemon.runtime.process_identity import (
    process_identity_matches,
    process_start_token,
)
from ascendop_daemon.runtime.release_identity import source_generation
from ascendop_daemon.runtime.resident_topology import resident_child_names
from ascendop_daemon.runtime.resident_watchdog import end_active_resident_task


SERVICE_SCHEMA = "ascendop.v4-resident-service.v1"
LIFECYCLE_LOCK_NAME = "v4_resident_lifecycle"
LIFECYCLE_LOCK_WAIT_SECONDS = 60.0
RESIDENT_CHILD_ENV = "ASCENDOP_RESIDENT_CHILD"


class ResidentServiceError(RuntimeError):
    pass


def service_paths(root: Path) -> dict[str, Path]:
    runtime = root.resolve() / ".ascendop-work" / "runtime"
    return {
        "runtime": runtime,
        "metadata": runtime / "v4-resident-service.json",
        "children": runtime / "v4-resident-children.json",
        "legacy_v4_metadata": runtime / "v4-daemon-service.json",
        "legacy_metadata": runtime / "v3-daemon-service.json",
        "stdout": runtime / "logs" / "v4-resident-supervisor.out.log",
        "stderr": runtime / "logs" / "v4-resident-supervisor.err.log",
    }


def service_status(
    root: Path,
    *,
    database: Path,
) -> dict[str, Any]:
    root = root.resolve()
    paths = service_paths(root)
    metadata = read_json(paths["metadata"])
    pid = int(metadata.get("pid") or 0)
    token = str(metadata.get("start_token") or "")
    running = process_identity_matches(pid, token)
    expected_generation = active_release_generation(root)
    child_document = read_json(paths["children"])
    children = child_document.get("children")
    children = children if isinstance(children, dict) else {}
    expected_children = _expected_resident_children(root, metadata)
    child_pids = {
        name: int(value.get("pid") or 0)
        for name, value in children.items()
        if isinstance(value, dict)
    }
    child_identities_live = set(child_pids) == set(expected_children) and all(
        process_identity_matches(
            int(value.get("pid") or 0), str(value.get("start_token") or "")
        )
        for value in children.values()
        if isinstance(value, dict)
    )
    try:
        services = ControlDatabase(_resolve(root, database)).service_health()
    except (OSError, sqlite3.Error):
        services = []
    compatible = {
        row["service_id"]: row
        for row in services
        if row["live"]
        and row["code_generation"] == expected_generation
        and row["wire_version"] == 3
        and int(row.get("database_schema") or SCHEMA_VERSION) == SCHEMA_VERSION
    }
    daemon_pid = child_pids.get("daemon", 0)
    api_pid = child_pids.get("control-api", 0)
    runner_pid = child_pids.get("agent-runner", 0)
    official_eval_pid = child_pids.get("official-eval", 0)
    local_services_healthy = all(
        service_id in compatible
        and int(compatible[service_id]["details"].get("pid") or 0) == daemon_pid
        for service_id in LOCAL_SERVICES
    )
    api_healthy = (
        "ascendop-control-api" in compatible
        and int(compatible["ascendop-control-api"]["details"].get("pid") or 0)
        == api_pid
    )
    runner_rows = [
        row
        for service_id, row in compatible.items()
        if service_id.startswith("ascendop-agent-runner:")
        and int(row["details"].get("pid") or 0) == runner_pid
    ]
    official_eval_healthy = (
        "ascendop-official-eval" in compatible
        and int(compatible["ascendop-official-eval"]["details"].get("pid") or 0)
        == official_eval_pid
    )
    runner_healthy = (
        len(runner_rows) == 1 if "agent-runner" in expected_children else True
    )
    daemon_lock = daemon_lock_status(root)
    return {
        "schema": SERVICE_SCHEMA,
        "running": running,
        "healthy": bool(
            running
            and str(metadata.get("generation") or "") == expected_generation
            and str(child_document.get("generation") or "") == expected_generation
            and child_document.get("state") == "running"
            and child_identities_live
            and local_services_healthy
            and api_healthy
            and runner_healthy
            and official_eval_healthy
        ),
        "pid": pid if running else 0,
        "start_token": token if running else "",
        "generation": expected_generation,
        "recorded_generation": str(metadata.get("generation") or ""),
        "stop_fenced": bool(read_stop_request(root)),
        "services": services,
        "expected_children": list(expected_children),
        "children": children,
        "children_metadata_path": str(paths["children"]),
        "metadata_path": str(paths["metadata"]),
        "legacy_metadata_path": str(paths["legacy_metadata"]),
        "legacy_metadata_present": paths["legacy_metadata"].is_file(),
        "legacy_v4_metadata_path": str(paths["legacy_v4_metadata"]),
        "legacy_v4_metadata_present": paths["legacy_v4_metadata"].is_file(),
        "daemon_lock": daemon_lock,
        "stdout": str(paths["stdout"]),
        "stderr": str(paths["stderr"]),
    }


def start_service(
    root: Path,
    *,
    config: Path,
    registry: Path,
    database: Path,
    interval_seconds: float,
    resume: bool,
    startup_timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    with _lifecycle_lock(root):
        return _start_service_locked(
            root,
            config=config,
            registry=registry,
            database=database,
            interval_seconds=interval_seconds,
            resume=resume,
            startup_timeout_seconds=startup_timeout_seconds,
        )


def _start_service_locked(
    root: Path,
    *,
    config: Path,
    registry: Path,
    database: Path,
    interval_seconds: float,
    resume: bool,
    startup_timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    root = root.resolve()
    current = service_status(root, database=database)
    if current["running"]:
        if current["recorded_generation"] != current["generation"]:
            raise ResidentServiceError("live resident generation mismatch")
        deadline = time.monotonic() + max(0.1, startup_timeout_seconds)
        while current["running"] and time.monotonic() < deadline:
            if current["healthy"]:
                return {**current, "outcome": "already-running"}
            time.sleep(0.1)
            current = service_status(root, database=database)
        if current["running"]:
            raise ResidentServiceError(
                "live resident did not recover a compatible healthy state"
            )
    daemon_lock = current["daemon_lock"]
    if daemon_lock["live"]:
        return {
            **current,
            "outcome": "daemon-running-unmanaged",
            "unmanaged_pid": daemon_lock["pid"],
        }
    if daemon_lock["present"]:
        if not daemon_lock["stale"]:
            raise ResidentServiceError(
                "daemon lock identity is uncertain; refusing resident startup"
            )
        Path(str(daemon_lock["path"])).unlink(missing_ok=True)
    if resume:
        clear_stop_request(root)
    elif read_stop_request(root):
        return {**current, "outcome": "paused-by-stop-fence"}
    paths = service_paths(root)
    paths["stdout"].parent.mkdir(parents=True, exist_ok=True)
    command = daemon_command(
        root,
        config=config,
        registry=registry,
        database=database,
        interval_seconds=interval_seconds,
    )
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment[RESIDENT_CHILD_ENV] = "1"
    with paths["stdout"].open("a", encoding="utf-8") as stdout, paths[
        "stderr"
    ].open("a", encoding="utf-8") as stderr:
        process = _spawn(command, root, environment, stdout, stderr)
    start_token = _wait_for_process_start_token(process.pid)
    metadata = {
        "schema": SERVICE_SCHEMA,
        "pid": process.pid,
        "start_token": start_token,
        "generation": active_release_generation(root),
        "started_at": utc_now(),
        "command": command,
        "config": str(config),
        "registry": str(registry),
        "database": str(database),
    }
    if not start_token:
        _terminate_unverified_child(process)
        raise ResidentServiceError("resident process identity is unavailable")
    write_json_atomic(paths["metadata"], metadata, ensure_ascii=True, sort_keys=True)
    deadline = time.monotonic() + max(0.1, startup_timeout_seconds)
    while time.monotonic() < deadline:
        current = service_status(root, database=database)
        if current["healthy"]:
            return {**current, "outcome": "started"}
        if not current["running"]:
            raise ResidentServiceError(
                f"resident exited during startup; inspect {paths['stderr']}"
            )
        time.sleep(0.1)
    _stop_service_locked(
        root,
        database=database,
        reason="startup health timeout",
        force=True,
    )
    raise ResidentServiceError("resident did not publish a compatible heartbeat")


def stop_service(
    root: Path,
    *,
    database: Path,
    reason: str,
    wait_seconds: float = 30.0,
    force: bool = False,
) -> dict[str, Any]:
    watchdog = end_active_resident_task()
    with _lifecycle_lock(root):
        result = _stop_service_locked(
            root,
            database=database,
            reason=reason,
            wait_seconds=wait_seconds,
            force=force,
        )
    return {**result, "resident_watchdog": watchdog}


def _stop_service_locked(
    root: Path,
    *,
    database: Path,
    reason: str,
    wait_seconds: float = 30.0,
    force: bool = False,
) -> dict[str, Any]:
    root = root.resolve()
    write_stop_request(root, reason)
    paths = service_paths(root)
    metadata = read_json(paths["metadata"])
    metadata_source = paths["metadata"]
    if not int(metadata.get("pid") or 0):
        for candidate in (paths["legacy_v4_metadata"], paths["legacy_metadata"]):
            legacy = read_json(candidate)
            if process_identity_matches(
                int(legacy.get("pid") or 0),
                str(legacy.get("start_token") or ""),
            ):
                metadata = legacy
                metadata_source = candidate
                break
    pid = int(metadata.get("pid") or 0)
    token = str(metadata.get("start_token") or "")
    deadline = time.monotonic() + max(0.0, wait_seconds)
    while process_identity_matches(pid, token) and time.monotonic() < deadline:
        time.sleep(0.1)
    forced = False
    if process_identity_matches(pid, token) and force:
        forced = _terminate_exact(pid, token)
    status = service_status(root, database=database)
    return {
        **status,
        "outcome": "stopped" if not status["running"] else "draining",
        "forced": forced,
        "stopped_metadata_path": str(metadata_source),
    }


def restart_service(
    root: Path,
    *,
    config: Path,
    registry: Path,
    database: Path,
    interval_seconds: float,
    reason: str,
    wait_seconds: float = 30.0,
    force: bool = False,
    startup_timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    watchdog = end_active_resident_task()
    with _lifecycle_lock(root):
        stopped = _stop_service_locked(
            root,
            database=database,
            reason=reason,
            wait_seconds=wait_seconds,
            force=force,
        )
        if stopped["running"]:
            return {**stopped, "resident_watchdog": watchdog}
        started = _start_service_locked(
            root,
            config=config,
            registry=registry,
            database=database,
            interval_seconds=interval_seconds,
            resume=True,
            startup_timeout_seconds=startup_timeout_seconds,
        )
    return {**started, "resident_watchdog": watchdog}


def daemon_command(
    root: Path,
    *,
    config: Path,
    registry: Path,
    database: Path,
    interval_seconds: float,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "ascendop_daemon.runtime.resident_supervisor",
        "--root",
        str(root),
        "--config",
        str(config),
        "--registry",
        str(registry),
        "--database",
        str(database),
        "--interval-seconds",
        str(max(0.05, interval_seconds)),
        "--generation",
        active_release_generation(root),
        "--control-api-port",
        str(_release_variable_default(root, "runtime.control_api_port", 0)),
        "--restart-limit",
        str(_release_variable_default(root, "runtime.resident_restart_limit", 5)),
        "--restart-window-seconds",
        str(
            _release_variable_default(
                root,
                "runtime.resident_restart_window_seconds",
                60,
            )
        ),
    ]


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def active_release_generation(root: Path) -> str:
    active = read_json(
        root.resolve() / ".ascendop-work" / "runtime" / "active-release.json"
    )
    if str(active.get("schema") or "") == "ascendop.active-release.v4":
        generation = str(active.get("release_generation") or "").strip()
        if generation:
            return generation
    return source_generation(root)


def _expected_resident_children(
    root: Path,
    metadata: dict[str, Any],
) -> tuple[str, ...]:
    config_text = str(metadata.get("config") or "").strip()
    if not config_text:
        # Old metadata predates topology recording and used the resident CLI runner.
        return ("daemon", "control-api", "agent-runner", "official-eval")
    return resident_child_names(_resolve(root, Path(config_text)))


def _release_variable_default(root: Path, variable_id: str, fallback: int) -> int:
    active = read_json(
        root.resolve() / ".ascendop-work" / "runtime" / "active-release.json"
    )
    registry_path = Path(str(active.get("variable_registry_path") or ""))
    registry = read_json(registry_path) if registry_path.is_file() else {}
    variables = registry.get("variables")
    if not isinstance(variables, list):
        return fallback
    matches = [
        item
        for item in variables
        if isinstance(item, dict) and item.get("id") == variable_id
    ]
    if len(matches) != 1:
        return fallback
    try:
        return int(matches[0]["default"])
    except (KeyError, TypeError, ValueError):
        return fallback


def _spawn(command, root, environment, stdout, stderr):
    kwargs: dict[str, Any] = {}
    if os.name == "nt":
        kwargs["creationflags"] = (
            getattr(subprocess, "CREATE_NO_WINDOW", 0)
            | getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = 0
        kwargs["startupinfo"] = startup
    return subprocess.Popen(
        command,
        cwd=root,
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        env=environment,
        **kwargs,
    )


def _lifecycle_lock(root: Path) -> NamedProcessLock:
    return NamedProcessLock(
        root.resolve(),
        LIFECYCLE_LOCK_NAME,
        stale_after_seconds=120,
        wait_timeout_seconds=LIFECYCLE_LOCK_WAIT_SECONDS,
        poll_interval_seconds=0.05,
    )


@contextmanager
def daemon_run_lock(
    root: Path,
    *,
    stale_after_seconds: int = 0,
) -> Iterator[DaemonLock]:
    """Serialize manual starts with resident spawning, then own daemon execution."""

    daemon_lock = DaemonLock(
        root.resolve(),
        stale_after_seconds=max(0, stale_after_seconds),
    )
    if os.environ.get(RESIDENT_CHILD_ENV) == "1":
        with daemon_lock:
            yield daemon_lock
        return
    with _lifecycle_lock(root):
        daemon_lock.__enter__()
    try:
        yield daemon_lock
    finally:
        daemon_lock.__exit__(None, None, None)


def daemon_lock_status(root: Path) -> dict[str, Any]:
    path = root.resolve() / "TestUtils" / "tester_daemon" / "daemon.lock"
    if not path.exists():
        return {
            "path": str(path),
            "present": False,
            "live": False,
            "stale": False,
            "pid": 0,
            "metadata": {},
        }
    metadata = read_lock_metadata(path)
    live = lock_owner_alive(path)
    stale = DaemonLock(root.resolve(), stale_after_seconds=120).is_stale()
    return {
        "path": str(path),
        "present": True,
        "live": live,
        "stale": stale,
        "pid": read_lock_pid(path),
        "metadata": metadata,
    }


def _wait_for_process_start_token(pid: int, timeout_seconds: float = 2.0) -> str:
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        token = process_start_token(pid)
        if token:
            return token
        if time.monotonic() >= deadline:
            return ""
        time.sleep(0.02)


def _terminate_unverified_child(process: subprocess.Popen[Any]) -> None:
    try:
        process.terminate()
        process.wait(timeout=2)
    except (OSError, subprocess.SubprocessError):
        try:
            process.kill()
        except OSError:
            pass


def _terminate_exact(pid: int, token: str) -> bool:
    if not process_identity_matches(pid, token):
        return False
    if os.name == "nt":
        _force_terminate(pid)
        return not process_identity_matches(pid, token)
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 5.0
    while process_identity_matches(pid, token) and time.monotonic() < deadline:
        time.sleep(0.05)
    if process_identity_matches(pid, token):
        _force_terminate(pid)
    return not process_identity_matches(pid, token)


def _force_terminate(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=10,
            creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
        )
        return
    os.kill(pid, signal.SIGKILL)


def _resolve(root: Path, value: Path) -> Path:
    return value.resolve() if value.is_absolute() else (root / value).resolve()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
