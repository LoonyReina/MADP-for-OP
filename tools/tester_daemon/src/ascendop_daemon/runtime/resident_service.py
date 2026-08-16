from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.runtime.application import LOCAL_SERVICES, source_generation
from ascendop_daemon.runtime.control import (
    clear_stop_request,
    read_stop_request,
    write_stop_request,
)
from ascendop_daemon.runtime.process_identity import (
    process_identity_matches,
    process_start_token,
)


SERVICE_SCHEMA = "ascendop.v3-resident-service.v1"


class ResidentServiceError(RuntimeError):
    pass


def service_paths(root: Path) -> dict[str, Path]:
    runtime = root.resolve() / ".ascendop-work" / "runtime"
    return {
        "runtime": runtime,
        "metadata": runtime / "v3-daemon-service.json",
        "stdout": runtime / "logs" / "v3-daemon.out.log",
        "stderr": runtime / "logs" / "v3-daemon.err.log",
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
    expected_generation = source_generation(root)
    services = ControlDatabase(_resolve(root, database)).service_health()
    compatible = [
        row
        for row in services
        if row["live"]
        and row["code_generation"] == expected_generation
        and row["wire_version"] == 3
        and row["details"].get("pid") == pid
    ]
    return {
        "schema": SERVICE_SCHEMA,
        "running": running,
        "healthy": bool(
            running
            and {row["service_id"] for row in compatible} == set(LOCAL_SERVICES)
        ),
        "pid": pid if running else 0,
        "start_token": token if running else "",
        "generation": expected_generation,
        "recorded_generation": str(metadata.get("generation") or ""),
        "stop_fenced": bool(read_stop_request(root)),
        "services": services,
        "metadata_path": str(paths["metadata"]),
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
    root = root.resolve()
    current = service_status(root, database=database)
    if current["running"]:
        if current["recorded_generation"] != current["generation"]:
            raise ResidentServiceError("live resident generation mismatch")
        return {**current, "outcome": "already-running"}
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
    with paths["stdout"].open("a", encoding="utf-8") as stdout, paths[
        "stderr"
    ].open("a", encoding="utf-8") as stderr:
        process = _spawn(command, root, environment, stdout, stderr)
    metadata = {
        "schema": SERVICE_SCHEMA,
        "pid": process.pid,
        "start_token": process_start_token(process.pid),
        "generation": source_generation(root),
        "started_at": utc_now(),
        "command": command,
        "config": str(config),
        "registry": str(registry),
        "database": str(database),
    }
    if not metadata["start_token"]:
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
    stop_service(root, database=database, reason="startup health timeout", force=True)
    raise ResidentServiceError("resident did not publish a compatible heartbeat")


def stop_service(
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
    }


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
        str(root / "tools" / "tester_daemon" / "daemon.py"),
        "run",
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
    ]


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


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


def _terminate_exact(pid: int, token: str) -> bool:
    if not process_identity_matches(pid, token):
        return False
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 5.0
    while process_identity_matches(pid, token) and time.monotonic() < deadline:
        time.sleep(0.05)
    if process_identity_matches(pid, token):
        os.kill(pid, signal.SIGKILL)
    return not process_identity_matches(pid, token)


def _resolve(root: Path, value: Path) -> Path:
    return value.resolve() if value.is_absolute() else (root / value).resolve()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
