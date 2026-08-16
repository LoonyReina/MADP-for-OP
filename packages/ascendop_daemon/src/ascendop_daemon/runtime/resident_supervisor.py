from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.runtime.control import read_stop_request
from ascendop_daemon.runtime.control_api_credentials import (
    ensure_control_api_credentials,
)
from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
)
from ascendop_daemon.runtime.process_identity import process_start_token
from ascendop_daemon.runtime.resident_topology import cli_runner_enabled
from ascendop_daemon.storage.control_types import SCHEMA_VERSION


SUPERVISOR_SCHEMA = "ascendop.v4-resident-children.v1"
DEFAULT_CONTROL_API_PORT = 0
DEFAULT_RESTART_LIMIT = 5
DEFAULT_RESTART_WINDOW_SECONDS = 60


@dataclass(frozen=True)
class ChildSpec:
    name: str
    command: tuple[str, ...]


@dataclass
class ChildState:
    spec: ChildSpec
    process: subprocess.Popen[Any]
    start_token: str
    started_at: str
    restart_times: list[float] = field(default_factory=list)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Supervise Flow V4 local services")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--control-api-port", type=int, default=DEFAULT_CONTROL_API_PORT)
    parser.add_argument("--restart-limit", type=int, default=DEFAULT_RESTART_LIMIT)
    parser.add_argument(
        "--restart-window-seconds",
        type=int,
        default=DEFAULT_RESTART_WINDOW_SECONDS,
    )
    args = parser.parse_args(argv)
    return run_supervisor(
        root=args.root,
        config=args.config,
        registry=args.registry,
        database=args.database,
        generation=args.generation,
        interval_seconds=args.interval_seconds,
        control_api_port=args.control_api_port,
        restart_limit=args.restart_limit,
        restart_window_seconds=args.restart_window_seconds,
    )


def run_supervisor(
    *,
    root: Path,
    config: Path,
    registry: Path,
    database: Path,
    generation: str,
    interval_seconds: float,
    control_api_port: int,
    restart_limit: int,
    restart_window_seconds: int,
) -> int:
    root = root.resolve()
    database = _resolve(root, database)
    credentials = ensure_control_api_credentials(root)
    specs = child_specs(
        root=root,
        config=_resolve(root, config),
        registry=_resolve(root, registry),
        database=database,
        generation=generation,
        interval_seconds=interval_seconds,
        control_api_port=control_api_port,
        token_file=Path(credentials["token_path"]),
    )
    stopping = False

    def request_shutdown(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, request_shutdown)

    children: dict[str, ChildState] = {}
    restart_history: dict[str, list[float]] = {spec.name: [] for spec in specs}
    try:
        children["daemon"] = _start_child(root, specs[0])
        _wait_for_control_database(database, children["daemon"].process)
        for spec in specs[1:]:
            children[spec.name] = _start_child(root, spec)
        _write_children_metadata(root, generation, children, credentials)
        while not stopping:
            if _active_generation(root) != generation:
                _write_failure(root, generation, "active-release-generation-changed")
                return 4
            fenced = bool(read_stop_request(root))
            daemon = children.get("daemon")
            if fenced and (daemon is None or daemon.process.poll() is not None):
                return 0
            now = time.monotonic()
            changed = False
            for spec in specs:
                child = children.get(spec.name)
                if child is not None and child.process.poll() is None:
                    continue
                if fenced:
                    continue
                history = [
                    value
                    for value in restart_history[spec.name]
                    if now - value <= max(1, restart_window_seconds)
                ]
                if len(history) >= max(0, restart_limit):
                    _write_failure(root, generation, f"restart-budget-exhausted:{spec.name}")
                    return 3
                history.append(now)
                restart_history[spec.name] = history
                replacement = _start_child(root, spec)
                replacement.restart_times = list(history)
                children[spec.name] = replacement
                changed = True
            if changed:
                _write_children_metadata(root, generation, children, credentials)
            time.sleep(max(0.1, min(float(interval_seconds), 5.0)))
        return 0
    finally:
        _terminate_children(children)
        _retire_child_heartbeats(database, children)
        _write_children_metadata(
            root,
            generation,
            children,
            credentials,
            state="stopped",
        )


def child_specs(
    *,
    root: Path,
    config: Path,
    registry: Path,
    database: Path,
    generation: str,
    interval_seconds: float,
    control_api_port: int,
    token_file: Path,
) -> tuple[ChildSpec, ...]:
    python = sys.executable
    daemon_entrypoint = _daemon_entrypoint(root, generation)
    cli_runner_enabled = _cli_runner_enabled(config)
    daemon = (
        python,
        str(daemon_entrypoint),
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
    )
    api = (
        python,
        "-m",
        "ascendop_control.api.main",
        "--database",
        str(database),
        "--token-file",
        str(token_file),
        "--port",
        str(control_api_port),
        "--endpoint-file",
        str(root / ".ascendop-work" / "runtime" / "control-api" / "endpoint.json"),
        "--generation",
        generation,
    )
    _official_eval_source, official_eval_config = _official_eval_paths(
        root, generation
    )
    official_eval = (
        python,
        "-m",
        "official_eval.cli",
        "--config",
        str(official_eval_config),
        "run-supervised",
        "--generation",
        generation,
        "--control-database",
        str(database),
    )
    specs = [
        ChildSpec("daemon", daemon),
        ChildSpec("control-api", api),
    ]
    if cli_runner_enabled:
        specs.append(
            ChildSpec(
                "agent-runner",
                (
                    python,
                    "-m",
                    "ascendop_agent_runner.cli",
                    "run-resident",
                    "--root",
                    str(root),
                    "--database",
                    str(database),
                    "--config",
                    str(config),
                    "--interval-seconds",
                    str(max(0.25, interval_seconds)),
                    "--generation",
                    generation,
                    "--runner-generation",
                    _agent_runner_generation(root, generation),
                ),
            )
        )
    specs.append(ChildSpec("official-eval", official_eval))
    return tuple(specs)


def _start_child(root: Path, spec: ChildSpec) -> ChildState:
    logs = root / ".ascendop-work" / "runtime" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["ASCENDOP_RESIDENT_CHILD"] = "1"
    with (logs / f"v4-{spec.name}.out.log").open(
        "a", encoding="utf-8"
    ) as stdout, (logs / f"v4-{spec.name}.err.log").open(
        "a", encoding="utf-8"
    ) as stderr:
        process = subprocess.Popen(
            list(spec.command),
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            env=environment,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
    token = _wait_for_token(process.pid)
    if not token:
        process.terminate()
        raise RuntimeError(f"child process identity is unavailable: {spec.name}")
    return ChildState(
        spec=spec,
        process=process,
        start_token=token,
        started_at=_utc_now(),
    )


def _terminate_children(children: dict[str, ChildState]) -> None:
    for name in ("official-eval", "agent-runner", "control-api", "daemon"):
        child = children.get(name)
        if child is not None and child.process.poll() is None:
            child.process.terminate()
    deadline = time.monotonic() + 5.0
    for child in children.values():
        if child.process.poll() is None:
            try:
                child.process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.process.kill()


def _retire_child_heartbeats(
    database: Path,
    children: dict[str, ChildState],
) -> None:
    pids = {child.process.pid for child in children.values()}
    try:
        store = ControlDatabase(database)
        rows = store.runtime_service_health()
        for row in rows:
            if int(row.get("details", {}).get("pid") or 0) not in pids:
                continue
            store.retire_runtime_service(
                service_id=str(row["service_id"]),
                expected_boot_id=str(row["boot_id"]),
                reason="resident-supervisor-child-stopped",
            )
    except (OSError, RuntimeError, ValueError):
        return


def _write_children_metadata(
    root: Path,
    generation: str,
    children: dict[str, ChildState],
    credentials: dict[str, str],
    *,
    state: str = "running",
) -> None:
    path = root / ".ascendop-work" / "runtime" / "v4-resident-children.json"
    payload = {
        "schema": SUPERVISOR_SCHEMA,
        "state": state,
        "generation": generation,
        "host": socket.gethostname(),
        "updated_at": _utc_now(),
        "control_api_credentials_path": credentials["credentials_path"],
        "children": {
            name: {
                "pid": child.process.pid,
                "start_token": child.start_token,
                "started_at": child.started_at,
                "restart_count": len(child.restart_times),
                "command": list(child.spec.command),
                "returncode": child.process.poll(),
            }
            for name, child in sorted(children.items())
        },
    }
    write_json_atomic(path, payload, ensure_ascii=True, sort_keys=True)


def _write_failure(root: Path, generation: str, reason: str) -> None:
    path = root / ".ascendop-work" / "runtime" / "v4-resident-failure.json"
    write_json_atomic(
        path,
        {
            "schema": "ascendop.v4-resident-failure.v1",
            "generation": generation,
            "reason": reason,
            "failed_at": _utc_now(),
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def _active_generation(root: Path) -> str:
    try:
        value = json.loads(
            (root / ".ascendop-work" / "runtime" / "active-release.json").read_text(
                encoding="utf-8"
            )
        )
    except (OSError, json.JSONDecodeError):
        return os.environ.get("ASCENDOP_RELEASE_GENERATION", "")
    return str(value.get("release_generation") or "")


def _daemon_entrypoint(root: Path, generation: str) -> Path:
    active_path = root / ".ascendop-work" / "runtime" / "active-release.json"
    try:
        active = json.loads(active_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        active = {}
    active_generation = str(active.get("release_generation") or "")
    if active_generation:
        if active_generation != generation:
            raise RuntimeError(
                "resident generation differs from the active release"
            )
        raw = str(active.get("daemon_entrypoint_path") or "")
        entrypoint = Path(raw) if raw else Path()
        if not raw or not entrypoint.is_file():
            raise RuntimeError(
                "active release does not expose its immutable daemon entrypoint"
            )
        return entrypoint.resolve()
    entrypoint = root / "tools" / "tester_daemon" / "daemon.py"
    if not entrypoint.is_file():
        raise RuntimeError(f"development daemon entrypoint is missing: {entrypoint}")
    return entrypoint.resolve()


def _agent_runner_generation(root: Path, generation: str) -> str:
    active_path = root / ".ascendop-work" / "runtime" / "active-release.json"
    try:
        active = json.loads(active_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        active = {}
    active_generation = str(active.get("release_generation") or "")
    if active_generation:
        if active_generation != generation:
            raise RuntimeError("resident generation differs from the active release")
        runner_generation = str(active.get("agent_runner_generation") or "")
        if not runner_generation:
            raise RuntimeError(
                "active release does not expose its Agent runner generation"
            )
        return runner_generation
    return generation


def _cli_runner_enabled(config: Path) -> bool:
    return cli_runner_enabled(config)


def _official_eval_paths(root: Path, generation: str) -> tuple[Path, Path]:
    active_path = root / ".ascendop-work" / "runtime" / "active-release.json"
    try:
        active = json.loads(active_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        active = {}
    active_generation = str(active.get("release_generation") or "")
    if active_generation:
        if active_generation != generation:
            raise RuntimeError("resident generation differs from the active release")
        source_text = str(active.get("official_eval_source") or "")
        config_text = str(active.get("official_eval_config_path") or "")
        source = Path(source_text) if source_text else Path()
        config = Path(config_text) if config_text else Path()
        if not source_text or not (source / "official_eval").is_dir():
            raise RuntimeError(
                "active release does not expose its immutable official-eval source"
            )
        if not config_text or not config.is_file():
            raise RuntimeError(
                "active release does not expose its immutable official-eval config"
            )
        return source.resolve(), config.resolve()
    source = root / "tools" / "official_eval_daemon"
    config = source / "config" / "august.json"
    if not (source / "official_eval").is_dir() or not config.is_file():
        raise RuntimeError("development official-eval product is incomplete")
    return source.resolve(), config.resolve()


def _wait_for_token(pid: int) -> str:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        token = process_start_token(pid)
        if token:
            return token
        time.sleep(0.02)
    return ""


def _wait_for_control_database(
    database: Path,
    daemon: subprocess.Popen[Any],
    timeout_seconds: float = 30.0,
) -> None:
    deadline = time.monotonic() + max(0.1, timeout_seconds)
    while time.monotonic() < deadline:
        if daemon.poll() is not None:
            raise RuntimeError("daemon exited before control database initialization")
        try:
            with sqlite3.connect(str(database), timeout=1.0) as connection:
                row = connection.execute(
                    "SELECT value FROM metadata WHERE key='schema_version'"
                ).fetchone()
        except (sqlite3.Error, OSError):
            row = None
        if row is not None and int(row[0]) == SCHEMA_VERSION:
            return
        time.sleep(0.1)
    raise RuntimeError(
        f"control database did not reach schema {SCHEMA_VERSION}"
    )


def _resolve(root: Path, value: Path) -> Path:
    return value.resolve() if value.is_absolute() else (root / value).resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
