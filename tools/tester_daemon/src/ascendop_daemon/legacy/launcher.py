#!/usr/bin/env python3
"""No-window launcher for the S5 910B tester daemon watchdog.

PowerShell Start-Process can still surface transient windows in the Codex IDE
on some hosts.  This launcher keeps the public entrypoint simple while using
Python's Windows process flags for the resident supervise loop.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[5]
for package_root in (
    ROOT / "tools" / "tester_daemon" / "src",
    ROOT / "packages" / "ascendop_protocol" / "src",
    ROOT,
):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from ascendop_daemon.runtime.process_inspection import (
    windows_process_command_line,
)
from ascendop_daemon.runtime.process_identity import process_start_token
from ascendop_daemon.runtime.locking import (
    process_alive as kernel_process_alive,
)
from ascendop_daemon.runtime.control import read_stop_request
from ascendop_daemon.legacy.supervisor import (
    active_execute_worker_pids,
    taskkill_tree,
)

DEFAULT_CONFIG = "tools/tester_daemon/config/cann_ladder_910b_cann90.json"
STATE_DIR = ROOT / "TestUtils" / "tester_daemon"
LOG_DIR = STATE_DIR / "logs"
LAUNCHER_STATE = STATE_DIR / "watchdog_launcher_state.json"
RESIDENT_TASK_STATE = STATE_DIR / "resident_watchdog_task.json"
RESIDENT_TASK_CHECK_STATE = STATE_DIR / "resident_watchdog_check.json"
RESIDENT_TASK_XML = STATE_DIR / "resident_watchdog_task.xml"
RESIDENT_TASK_NAME = "AscendOP-S6-Resident-Watchdog"
WORKFLOW_SERVICE_STATE = STATE_DIR / "workflow_service_state.json"

ENGINE_REMOTE_ACTIVE_STATES = {
    "admitting",
    "staging-standby",
    "standby",
    "standby-cancel-requested",
    "accepted",
    "running",
    "return-ready",
    "returned-awaiting-ingest",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def ensure_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def normalized_config_path(value: object, *, require_exists: bool = True) -> str:
    text = str(value or DEFAULT_CONFIG).strip() or DEFAULT_CONFIG
    candidate = Path(text)
    resolved = (
        candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()
    )
    if require_exists and not resolved.is_file():
        raise ValueError(f"daemon config does not exist: {resolved}")
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError:
        return str(resolved)


def selected_config(args: argparse.Namespace | None = None) -> str:
    if args is not None and not hasattr(args, "config"):
        return normalized_config_path(DEFAULT_CONFIG)
    value = getattr(args, "config", "") if args is not None else ""
    if not str(value or "").strip():
        value = os.environ.get("ASCENDOP_DAEMON_CONFIG", "")
    if not str(value or "").strip():
        heartbeat = read_json(STATE_DIR / "supervisor_loop_heartbeat.json")
        observed = str(heartbeat.get("config") or "").strip()
        if observed:
            observed_path = Path(observed)
            resolved = (
                observed_path.resolve()
                if observed_path.is_absolute()
                else (ROOT / observed_path).resolve()
            )
            if resolved.is_file():
                value = observed
    if not str(value or "").strip():
        value = DEFAULT_CONFIG
    return normalized_config_path(value)


def config_identity(value: object) -> str:
    normalized = normalized_config_path(value, require_exists=False)
    candidate = Path(normalized)
    resolved = candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()
    return os.path.normcase(os.path.normpath(str(resolved)))


def process_creation_flags(
    *, detached: bool = False, include_breakaway: bool = False
) -> int:
    if os.name != "nt":
        return 0
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if detached:
        flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    if detached and include_breakaway:
        flags |= getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0x01000000)
    return flags


def process_startupinfo() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return startupinfo


def run_command(
    argv: list[str], *, timeout: float | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        creationflags=process_creation_flags(),
        startupinfo=process_startupinfo(),
        timeout=timeout,
    )


def print_completed(completed: subprocess.CompletedProcess[str]) -> int:
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    return completed.returncode


def python_candidate_paths() -> list[Path]:
    candidates: list[Path] = []
    env_python = os.environ.get("ASCENDOP_DAEMON_PYTHON")
    if env_python:
        candidates.append(Path(env_python))
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        candidates.append(
            Path(conda_prefix) / ("python.exe" if os.name == "nt" else "bin/python")
        )
    home = Path.home()
    if os.name == "nt":
        candidates.extend(
            [
                home / "anaconda3" / "python.exe",
                home / "miniconda3" / "python.exe",
                Path(sys.executable),
            ]
        )
    else:
        candidates.extend(
            [
                home / "anaconda3" / "bin" / "python",
                home / "miniconda3" / "bin" / "python",
                Path(sys.executable),
            ]
        )
    which_python = shutil.which("python")
    if which_python:
        candidates.append(Path(which_python))
    seen: set[str] = set()
    unique: list[Path] = []
    for candidate in candidates:
        key = str(candidate).lower() if os.name == "nt" else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def python_supports_bridge(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        completed = subprocess.run(
            [str(path), "-c", "import psutil, websocket"],
            cwd=ROOT,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


def daemon_python_executable(*, require_bridge: bool = False) -> str:
    fallback = sys.executable
    for candidate in python_candidate_paths():
        if not candidate.exists():
            continue
        if require_bridge and not python_supports_bridge(candidate):
            continue
        return str(candidate)
    if require_bridge:
        raise RuntimeError(
            "no Python interpreter with websocket-client found; set ASCENDOP_DAEMON_PYTHON"
        )
    return fallback


def daemon_command(*parts: str, require_bridge: bool = False) -> list[str]:
    return [
        daemon_python_executable(require_bridge=require_bridge),
        "tools/tester_daemon/daemon.py",
        *parts,
    ]


def background_python_executable(executable: str) -> str:
    exe = Path(executable)
    if os.name == "nt" and exe.name.lower() == "python.exe":
        pythonw = exe.with_name("pythonw.exe")
        if pythonw.exists():
            return str(pythonw)
    return sys.executable


def supervise_loop_command(args: argparse.Namespace) -> list[str]:
    no_bridge = bool(args.no_bridge or os.name == "nt")
    config = selected_config(args)
    command = daemon_command(
        "supervise-loop",
        "--config",
        config,
        "--mode",
        "execute",
        "--write-state",
        "--allow-live-execute",
        "--clear-stop",
        "--max-heartbeat-age-seconds",
        "120",
        "--bridge-max-heartbeat-age-seconds",
        "120",
        "--replace-stale-lock-after-seconds",
        "120",
        "--interval-seconds",
        str(args.interval_seconds),
        "--max-iterations",
        str(args.max_iterations),
        require_bridge=not no_bridge,
    )
    command[0] = background_python_executable(command[0])
    if no_bridge:
        command.append("--no-bridge")
    return command


def supervise_once_command(args: argparse.Namespace) -> list[str]:
    no_bridge = bool(args.no_bridge or os.name == "nt")
    config = selected_config(args)
    command = daemon_command(
        "supervise",
        "--config",
        config,
        "--mode",
        "execute",
        "--write-state",
        "--allow-live-execute",
        "--clear-stop",
        "--max-heartbeat-age-seconds",
        "120",
        "--bridge-max-heartbeat-age-seconds",
        "120",
        "--replace-stale-lock-after-seconds",
        "120",
        require_bridge=not no_bridge,
    )
    if no_bridge:
        command.append("--no-bridge")
    return command


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def normalize_command_line(value: str) -> str:
    return value.replace("\\", "/").lower()


def command_line_matches(command_line: str, markers: list[str]) -> bool:
    normalized = normalize_command_line(command_line)
    return all(marker.replace("\\", "/").lower() in normalized for marker in markers)


def query_windows_command_line(pid: int) -> str:
    if os.name != "nt":
        return ""
    return windows_process_command_line(pid)


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return bool(query_windows_command_line(pid)) or kernel_process_alive(pid)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def parse_utc_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def resident_supervisor_status(
    max_heartbeat_age_seconds: int = 120,
    *,
    expected_config: str = "",
) -> dict[str, Any]:
    heartbeat = read_json(STATE_DIR / "supervisor_loop_heartbeat.json")
    pid = int(heartbeat.get("pid") or 0)
    observed_config = str(heartbeat.get("config") or "")
    config_matches = bool(
        not expected_config
        or (
            observed_config
            and config_identity(observed_config) == config_identity(expected_config)
        )
    )
    heartbeat_time = parse_utc_timestamp(heartbeat.get("time"))
    age_seconds = None
    if heartbeat_time is not None:
        age_seconds = max(
            0, int((datetime.now(timezone.utc) - heartbeat_time).total_seconds())
        )
    alive = pid_alive(pid)
    fresh = age_seconds is not None and age_seconds <= max(1, max_heartbeat_age_seconds)
    return {
        "pid": pid,
        "alive": alive,
        "heartbeat_time": heartbeat.get("time", ""),
        "heartbeat_age_seconds": age_seconds,
        "heartbeat_fresh": fresh,
        "config": observed_config,
        "expected_config": expected_config,
        "config_matches": config_matches,
        "resident_ok": alive and fresh and config_matches,
    }


def runtime_process_specs() -> list[tuple[Path, list[str]]]:
    return [
        (LAUNCHER_STATE, ["tools/tester_daemon/daemon.py", "supervise-loop"]),
        (
            STATE_DIR / "supervisor_loop_process.json",
            ["tools/tester_daemon/daemon.py", "supervise-loop"],
        ),
        (STATE_DIR / "daemon_process.json", ["tools/tester_daemon/daemon.py", "run"]),
        (
            STATE_DIR / "solver_trigger_bridge_process.json",
            ["tools/tester_daemon/src/ascendop_daemon/legacy/bridge.py"],
        ),
        (
            STATE_DIR / "engine_pump_worker.json",
            ["tools/tester_daemon/daemon.py", "engine-pump"],
        ),
        (
            STATE_DIR / "flow_v3_worker.json",
            ["tools/tester_daemon/daemon.py", "flow-v3", "run"],
        ),
    ]


def workflow_control_status(config: str = DEFAULT_CONFIG) -> dict[str, Any]:
    stop_request = read_stop_request(ROOT)
    service = read_json(WORKFLOW_SERVICE_STATE)
    service_action = str(service.get("action") or "")
    intentional_stop = bool(stop_request) or service_action in {
        "stopped",
        "stop_requested",
        "force_stopped",
    }
    if not intentional_stop:
        return {
            "state": "running-or-unhealthy",
            "intentional_stop": False,
            "service_action": service_action,
        }

    runtime_processes: list[dict[str, Any]] = []
    alive_runtime_pids: set[int] = set()
    for path, _markers in runtime_process_specs():
        pid = int(read_json(path).get("pid") or 0)
        alive = bool(pid and pid_alive(pid))
        if alive:
            alive_runtime_pids.add(pid)
        runtime_processes.append(
            {"record": path.name, "pid": pid, "alive": alive}
        )

    active_worker_pids = sorted(active_execute_worker_pids(ROOT, STATE_DIR))
    pump_state = read_json(STATE_DIR / "engine_pump_state.json")
    pump_entries = pump_state.get("entries", {})
    remote_active_jobs = sorted(
        str(job_id)
        for job_id, raw in (
            pump_entries.items() if isinstance(pump_entries, dict) else []
        )
        if isinstance(raw, dict)
        and str(raw.get("state") or "") in ENGINE_REMOTE_ACTIVE_STATES
        and not str(raw.get("credit_released_at") or "")
    )
    relay_outbox = read_json(STATE_DIR / "native_relay_outbox.json")
    relay_paused = bool(stop_request) and bool(
        service.get("relay_claims_paused_by_stop_request", True)
    )
    stopped_clean = (
        service_action in {"stopped", "force_stopped"}
        and relay_paused
        and not alive_runtime_pids
        and not active_worker_pids
        and not remote_active_jobs
    )
    return {
        "state": "stopped-clean" if stopped_clean else "stopping-drain",
        "intentional_stop": True,
        "service_action": service_action,
        "stop_requested_at": str(stop_request.get("requested_at") or ""),
        "stop_reason": str(
            stop_request.get("reason") or service.get("reason") or ""
        ),
        "relay_claims_paused": relay_paused,
        "relay_available_entries": int(
            relay_outbox.get("available_entry_count", 0) or 0
        ),
        "relay_claimed_entries": int(
            relay_outbox.get("claimed_entry_count", 0) or 0
        ),
        "alive_runtime_pids": sorted(alive_runtime_pids),
        "active_execute_worker_pids": active_worker_pids,
        "remote_active_engine_jobs": remote_active_jobs,
        "runtime_processes": runtime_processes,
        "resume_command": (
            "python tools\\tester_daemon\\launch_s5_910b.py start "
            f'--config "{normalized_config_path(config, require_exists=False)}" '
            "--interval-seconds 1"
        ),
    }


def record_workflow_started(
    *,
    pid: int,
    started_at: str,
    reset_metrics: bool,
    reason: str,
) -> dict[str, Any]:
    previous = read_json(WORKFLOW_SERVICE_STATE)
    previous_epoch = str(previous.get("metrics_epoch_at", "") or "")
    metrics_epoch = (
        started_at if reset_metrics or not previous_epoch else previous_epoch
    )
    record = {
        "target": "workflow",
        "action": "started",
        "reason": reason,
        "started_at": started_at,
        "pid": int(pid or 0),
        "metrics_epoch_at": metrics_epoch,
        "metrics_epoch_reason": (
            "explicit-resume-after-stop"
            if reset_metrics
            else "preserve-across-resident-recovery"
        ),
        "previous_action": str(previous.get("action", "") or ""),
        "relay_claims_paused_by_stop_request": False,
    }
    write_json(WORKFLOW_SERVICE_STATE, record)
    return record


def terminate_process(pid: int, *, tree: bool = True) -> tuple[bool, str]:
    if pid <= 0:
        return False, "invalid pid"
    if os.name == "nt":
        argv = ["taskkill.exe", "/PID", str(pid), "/F"]
        if tree:
            argv.insert(3, "/T")
        completed = run_command(argv, timeout=20)
        output = (completed.stdout + completed.stderr).strip()
        return completed.returncode == 0, output
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        return False, str(exc)
    return True, "terminated"


def terminate_process_tree(pid: int) -> tuple[bool, str]:
    return terminate_process(pid, tree=True)


def stop_recorded_process(
    path: Path, markers: list[str], *, quiet: bool = False, tree: bool = True
) -> dict[str, Any]:
    record = read_json(path)
    pid = int(record.get("pid") or 0)
    expected_start_token = str(record.get("start_token") or "")
    result: dict[str, Any] = {
        "path": str(path),
        "pid": pid,
        "expected_start_token": expected_start_token,
        "status": "missing",
    }
    if not pid:
        return result
    command_line = (
        query_windows_command_line(pid)
        if os.name == "nt"
        else " ".join(record.get("command", []))
    )
    current_start_token = process_start_token(pid)
    result["current_start_token"] = current_start_token
    if not expected_start_token:
        result["status"] = "missing_start_token"
        if not quiet:
            print(f"skip pid={pid}: recorded process has no start token")
        return result
    if current_start_token != expected_start_token:
        result.update(
            {"status": "pid_reused_or_identity_changed", "command_line": command_line}
        )
        if not quiet:
            print(
                f"skip pid={pid}: start token changed "
                f"expected={expected_start_token} current={current_start_token}"
            )
        return result
    if not command_line:
        if not kernel_process_alive(pid):
            result["status"] = "not_running"
            return result
        result["identity_verification"] = "pid-start-token-kernel-alive"
    elif markers and not command_line_matches(command_line, markers):
        result.update(
            {"status": "pid_reused_or_unexpected_command", "command_line": command_line}
        )
        if not quiet:
            print(f"skip pid={pid}: command line no longer matches {markers}")
        return result
    ok, output = terminate_process(pid, tree=tree)
    result.update(
        {
            "status": "stopped" if ok else "stop_failed",
            "output": output,
            "tree": tree,
            "signal": "taskkill-force" if os.name == "nt" else "SIGTERM",
        }
    )
    if not quiet:
        print(f"{result['status']} pid={pid} file={path.name}")
        if output:
            print(
                output.encode("utf-8", errors="replace").decode(
                    "utf-8", errors="replace"
                )
            )
    return result


def stop_command_timeout_seconds(config: str) -> float:
    config_path = Path(normalized_config_path(config))
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    raw = read_json(config_path)
    policy = raw.get("policy", {}) if isinstance(raw, dict) else {}
    value = (
        policy.get("flow_v3_stop_command_timeout_seconds", 60)
        if isinstance(policy, dict)
        else 60
    )
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        seconds = 60.0
    return min(120.0, max(20.0, seconds))


def stop_watchdog(
    reason: str,
    *,
    config: str = DEFAULT_CONFIG,
    quiet: bool = False,
    force: bool = False,
    preserve_workers: bool = True,
    wait_seconds: float = 30.0,
) -> int:
    worker_pids = set()
    if force and not preserve_workers:
        worker_pids = active_execute_worker_pids(ROOT, STATE_DIR)
    stop_args = [
        "stop",
        "--reason",
        reason,
        "--config",
        normalized_config_path(config),
    ]
    # Force replacement already owns verified coordinator termination below.
    # Avoid synchronously stopping a stale bridge here: that path can block
    # before process replacement and clear-stop, leaving the service half-stopped.
    if force:
        stop_args.append("--daemon-only")
    stop = run_command(
        daemon_command(*stop_args),
        timeout=stop_command_timeout_seconds(config),
    )
    if not quiet:
        print_completed(stop)
    process_specs = runtime_process_specs()
    preserved_specs = (
        {
            STATE_DIR / "engine_pump_worker.json",
            STATE_DIR / "flow_v3_worker.json",
        }
        if preserve_workers
        else set()
    )
    if force:
        for path, markers in process_specs:
            if path in preserved_specs:
                continue
            stop_recorded_process(path, markers, quiet=quiet, tree=not preserve_workers)
    else:
        deadline = time.monotonic() + max(0.0, float(wait_seconds))
        while time.monotonic() < deadline:
            alive = [
                int(read_json(path).get("pid") or 0) for path, _markers in process_specs
            ]
            if not any(pid_alive(pid) for pid in alive if pid):
                break
            time.sleep(0.25)
    if force and not preserve_workers:
        # A breakaway worker can become observable only after its coordinator
        # exits, so rescan before terminating the final process trees.
        worker_pids.update(active_execute_worker_pids(ROOT, STATE_DIR))
    worker_results = [taskkill_tree(pid) for pid in sorted(worker_pids)]
    if worker_results:
        record = {
            "target": "execute_workers",
            "action": (
                "stopped"
                if all(not item.get("remaining") for item in worker_results)
                else "stop_attempted"
            ),
            "reason": reason,
            "stopped_at": utc_now_iso(),
            "worker_pids": sorted(worker_pids),
            "results": worker_results,
        }
        write_json(STATE_DIR / "watchdog_stop_workers.json", record)
        if not quiet:
            print(json.dumps(record, ensure_ascii=False, indent=2))
    remaining = [
        int(read_json(path).get("pid") or 0)
        for path, _markers in process_specs
        if path not in preserved_specs
        if pid_alive(int(read_json(path).get("pid") or 0))
    ]
    preserved_runtime_pids = [
        int(read_json(path).get("pid") or 0)
        for path in preserved_specs
        if pid_alive(int(read_json(path).get("pid") or 0))
    ]
    previous_service = read_json(WORKFLOW_SERVICE_STATE)
    record = {
        "target": "workflow",
        "action": (
            "force_stopped"
            if force
            else ("stopped" if not remaining else "stop_requested")
        ),
        "reason": reason,
        "stopped_at": utc_now_iso(),
        "remaining_runtime_pids": remaining,
        "preserved_runtime_pids": preserved_runtime_pids,
        "active_execute_worker_pids": sorted(
            active_execute_worker_pids(ROOT, STATE_DIR)
        ),
        "relay_claims_paused_by_stop_request": True,
    }
    previous_epoch = str(previous_service.get("metrics_epoch_at", "") or "")
    if previous_epoch:
        record["metrics_epoch_at"] = previous_epoch
    write_json(WORKFLOW_SERVICE_STATE, record)
    if not quiet:
        print(json.dumps(record, ensure_ascii=False, indent=2))
    return 0 if not remaining else 2


def wait_for_runtime_quiescence(
    *,
    timeout_seconds: float,
    preserve_engine_pump: bool,
) -> list[dict[str, object]]:
    preserved = (
        {
            STATE_DIR / "engine_pump_worker.json",
            STATE_DIR / "flow_v3_worker.json",
        }
        if preserve_engine_pump
        else set()
    )
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    while True:
        remaining: list[dict[str, object]] = []
        observed_pids: set[int] = set()
        for path, markers in runtime_process_specs():
            if path in preserved:
                continue
            record = read_json(path)
            pid = int(record.get("pid") or 0)
            if not pid or pid in observed_pids or not pid_alive(pid):
                continue
            observed_pids.add(pid)
            remaining.append(
                {
                    "record": path.name,
                    "pid": pid,
                    "start_token": str(record.get("start_token") or ""),
                    "markers": markers,
                }
            )
        if not remaining or time.monotonic() >= deadline:
            return remaining
        time.sleep(0.25)


def start_watchdog(args: argparse.Namespace) -> int:
    ensure_dirs()
    config = selected_config(args)
    action = str(getattr(args, "action", "start") or "start")
    explicit_start = action in {"start", "restart", "recover-sessions"}
    current = resident_supervisor_status(expected_config=config)
    if current.get("resident_ok"):
        service = read_json(WORKFLOW_SERVICE_STATE)
        if str(service.get("action", "") or "") != "started":
            launcher = read_json(LAUNCHER_STATE)
            started_at = str(launcher.get("started_at", "") or utc_now_iso())
            record_workflow_started(
                pid=int(current.get("pid", 0) or 0),
                started_at=started_at,
                reset_metrics=bool(
                    explicit_start
                    and str(service.get("action", "") or "")
                    in {"stopped", "stop_requested", "force_stopped"}
                ),
                reason=(
                    "explicit start reconciled an already-resident watchdog"
                    if explicit_start
                    else "resident self-check reconciled service state without resetting metrics"
                ),
            )
        print(f"watchdog already running pid={current.get('pid')}")
        return 0
    stop_request_before_start = read_stop_request(ROOT)
    service_before_start = read_json(WORKFLOW_SERVICE_STATE)
    reset_metrics = bool(
        explicit_start
        and (
            stop_request_before_start
            or str(service_before_start.get("action", "") or "")
            in {"stopped", "stop_requested", "force_stopped"}
            or not service_before_start
        )
    )
    stop_watchdog(
        "launch_s5_910b.py start replacing stale local watchdog",
        quiet=True,
        force=True,
        preserve_workers=True,
    )
    clear = run_command(daemon_command("clear-stop"), timeout=20)
    if clear.returncode != 0:
        return print_completed(clear)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stdout_path = LOG_DIR / f"watchdog_supervise_loop_{stamp}.out.log"
    stderr_path = LOG_DIR / f"watchdog_supervise_loop_{stamp}.err.log"
    command = supervise_loop_command(args)
    with stdout_path.open("a", encoding="utf-8") as stdout, stderr_path.open(
        "a", encoding="utf-8"
    ) as stderr:
        try:
            proc = subprocess.Popen(
                command,
                cwd=ROOT,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                creationflags=process_creation_flags(
                    detached=True, include_breakaway=True
                ),
                startupinfo=process_startupinfo(),
            )
            breakaway = True
        except OSError:
            proc = subprocess.Popen(
                command,
                cwd=ROOT,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                creationflags=process_creation_flags(
                    detached=True, include_breakaway=False
                ),
                startupinfo=process_startupinfo(),
            )
            breakaway = False
    record = {
        "target": "supervisor_loop",
        "started_at": utc_now_iso(),
        "pid": proc.pid,
        "start_token": process_start_token(proc.pid),
        "action": "started",
        "interval_seconds": args.interval_seconds,
        "max_iterations": args.max_iterations,
        "no_bridge": bool(args.no_bridge or os.name == "nt"),
        "relay_topology": (
            "app-side-native-relay-only" if os.name == "nt" else "daemon-bridge"
        ),
        "cwd": str(ROOT),
        "command": command,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "config": config,
        "create_breakaway_from_job": breakaway,
        "launcher": "python-subprocess-no-window",
    }
    write_json(LAUNCHER_STATE, record)
    write_json(STATE_DIR / "supervisor_loop_process.json", record)
    record_workflow_started(
        pid=proc.pid,
        started_at=str(record["started_at"]),
        reset_metrics=reset_metrics,
        reason=(
            "explicit workflow resume"
            if reset_metrics
            else "resident watchdog recovery preserved the existing metric window"
        ),
    )
    print(f"watchdog started pid={proc.pid}")
    print(f"stdout={stdout_path}")
    print(f"stderr={stderr_path}")
    return 0


def ensure_resident(args: argparse.Namespace) -> int:
    """Restore only the resident supervisor, without overriding an explicit pause."""
    stop_request = read_stop_request(ROOT)
    if stop_request:
        write_json(
            RESIDENT_TASK_CHECK_STATE,
            {
                "task_name": RESIDENT_TASK_NAME,
                "checked_at": utc_now_iso(),
                "action": "paused_by_stop_fence",
                "stop_request": stop_request,
            },
        )
        return 0
    config = selected_config(args)
    current = resident_supervisor_status(expected_config=config)
    if current.get("resident_ok"):
        write_json(
            RESIDENT_TASK_CHECK_STATE,
            {
                "task_name": RESIDENT_TASK_NAME,
                "checked_at": utc_now_iso(),
                "action": "healthy_noop",
                "resident_supervisor": current,
            },
        )
        return 0
    rc = start_watchdog(args)
    write_json(
        RESIDENT_TASK_CHECK_STATE,
        {
            "task_name": RESIDENT_TASK_NAME,
            "checked_at": utc_now_iso(),
            "action": "resident_restarted" if rc == 0 else "resident_restart_failed",
            "returncode": rc,
            "previous_supervisor": current,
            "resident_supervisor": resident_supervisor_status(expected_config=config),
        },
    )
    return rc


def resident_task_action(config: str = DEFAULT_CONFIG) -> str:
    python = background_python_executable(daemon_python_executable())
    launcher = Path(__file__).resolve()
    return subprocess.list2cmdline(
        [
            python,
            str(launcher),
            "ensure-resident",
            "--config",
            normalized_config_path(config, require_exists=False),
            "--interval-seconds",
            "1",
            "--no-bridge",
        ]
    )


def resident_task_xml(config: str = DEFAULT_CONFIG) -> str:
    import xml.etree.ElementTree as ET

    namespace = "http://schemas.microsoft.com/windows/2004/02/mit/task"
    ET.register_namespace("", namespace)
    q = lambda name: f"{{{namespace}}}{name}"
    task = ET.Element(q("Task"), {"version": "1.4"})
    registration = ET.SubElement(task, q("RegistrationInfo"))
    ET.SubElement(registration, q("Description")).text = (
        "AscendOP resident supervisor watchdog only; no workflow gate selection or session delivery."
    )
    triggers = ET.SubElement(task, q("Triggers"))
    trigger = ET.SubElement(triggers, q("TimeTrigger"))
    repetition = ET.SubElement(trigger, q("Repetition"))
    ET.SubElement(repetition, q("Interval")).text = "PT1M"
    ET.SubElement(repetition, q("StopAtDurationEnd")).text = "false"
    ET.SubElement(trigger, q("StartBoundary")).text = (
        datetime.now().astimezone().replace(microsecond=0).isoformat()
    )
    ET.SubElement(trigger, q("Enabled")).text = "true"
    principals = ET.SubElement(task, q("Principals"))
    principal = ET.SubElement(principals, q("Principal"), {"id": "Author"})
    domain = str(os.environ.get("USERDOMAIN", "") or "").strip()
    username = str(os.environ.get("USERNAME", "") or Path.home().name).strip()
    ET.SubElement(principal, q("UserId")).text = (
        f"{domain}\\{username}" if domain else username
    )
    ET.SubElement(principal, q("LogonType")).text = "InteractiveToken"
    ET.SubElement(principal, q("RunLevel")).text = "LeastPrivilege"
    settings = ET.SubElement(task, q("Settings"))
    for name, value in (
        ("MultipleInstancesPolicy", "IgnoreNew"),
        ("DisallowStartIfOnBatteries", "false"),
        ("StopIfGoingOnBatteries", "false"),
        ("AllowHardTerminate", "true"),
        ("StartWhenAvailable", "true"),
        ("RunOnlyIfNetworkAvailable", "false"),
        ("AllowStartOnDemand", "true"),
        ("Enabled", "true"),
        ("Hidden", "true"),
        ("RunOnlyIfIdle", "false"),
        ("WakeToRun", "false"),
        # The scheduled-task action owns the resident child job on Windows when
        # CREATE_BREAKAWAY_FROM_JOB is unavailable. A finite task limit would
        # therefore terminate the healthy supervisor together with its launcher.
        ("ExecutionTimeLimit", "PT0S"),
        ("Priority", "7"),
    ):
        ET.SubElement(settings, q(name)).text = value
    actions = ET.SubElement(task, q("Actions"), {"Context": "Author"})
    execute = ET.SubElement(actions, q("Exec"))
    python = background_python_executable(daemon_python_executable())
    ET.SubElement(execute, q("Command")).text = python
    ET.SubElement(execute, q("Arguments")).text = subprocess.list2cmdline(
        [
            str(Path(__file__).resolve()),
            "ensure-resident",
            "--config",
            normalized_config_path(config, require_exists=False),
            "--interval-seconds",
            "1",
            "--no-bridge",
        ]
    )
    ET.SubElement(execute, q("WorkingDirectory")).text = str(ROOT)
    return ET.tostring(task, encoding="unicode")


def install_resident_task(config: str = DEFAULT_CONFIG) -> int:
    if os.name != "nt":
        print(
            "resident task installation is only supported on Windows", file=sys.stderr
        )
        return 2
    ensure_dirs()
    config = normalized_config_path(config)
    action = resident_task_action(config)
    RESIDENT_TASK_XML.write_text(resident_task_xml(config), encoding="utf-16")
    completed = run_command(
        [
            "schtasks.exe",
            "/Create",
            "/TN",
            RESIDENT_TASK_NAME,
            "/XML",
            str(RESIDENT_TASK_XML),
            "/F",
        ],
        timeout=30,
    )
    record = {
        "task_name": RESIDENT_TASK_NAME,
        "installed_at": utc_now_iso(),
        "action": action,
        "schedule": "every_1_minute",
        "task_xml": str(RESIDENT_TASK_XML),
        "allow_start_on_batteries": True,
        "stop_if_going_on_batteries": False,
        "purpose": "resident-supervisor-only; no gate selection or native session delivery",
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }
    write_json(RESIDENT_TASK_STATE, record)
    if completed.returncode != 0:
        return print_completed(completed)
    run_now = run_command(
        ["schtasks.exe", "/Run", "/TN", RESIDENT_TASK_NAME],
        timeout=30,
    )
    record.update(
        {
            "run_requested_at": utc_now_iso(),
            "run_returncode": run_now.returncode,
            "run_stdout": run_now.stdout.strip(),
            "run_stderr": run_now.stderr.strip(),
        }
    )
    write_json(RESIDENT_TASK_STATE, record)
    print_completed(completed)
    return print_completed(run_now)


def resident_task_status() -> int:
    if os.name != "nt":
        print("resident task status is only supported on Windows", file=sys.stderr)
        return 2
    completed = run_command(
        ["schtasks.exe", "/Query", "/TN", RESIDENT_TASK_NAME, "/FO", "LIST", "/V"],
        timeout=30,
    )
    return print_completed(completed)


def launcher_status(config: str = DEFAULT_CONFIG) -> int:
    config = normalized_config_path(config)
    record = read_json(LAUNCHER_STATE)
    if not record:
        print("no watchdog launcher state")
    else:
        pid = int(record.get("pid") or 0)
        command_line = query_windows_command_line(pid) if os.name == "nt" else ""
        alive = pid_alive(pid)
        status = {**record, "alive": alive}
        if command_line:
            status["observed_command_line"] = command_line
        print(json.dumps(status, ensure_ascii=False, indent=2))
    resident = resident_supervisor_status(expected_config=config)
    print(json.dumps({"resident_supervisor": resident}, ensure_ascii=False, indent=2))
    control = workflow_control_status(config)
    print(json.dumps({"workflow_control": control}, ensure_ascii=False, indent=2))
    if control.get("intentional_stop"):
        if control.get("state") == "stopped-clean":
            print("WORKFLOW_STOPPED_CLEAN")
            return 0
        print("WORKFLOW_STOPPING_DRAIN")
        return 2
    health = run_command(
        daemon_command("health", "--max-heartbeat-age-seconds", "120"), timeout=60
    )
    return print_completed(health)


def run_foreground_action(args: argparse.Namespace) -> int:
    config = selected_config(args)
    if args.action == "health":
        return print_completed(
            run_command(
                daemon_command("health", "--max-heartbeat-age-seconds", "120"),
                timeout=60,
            )
        )
    if args.action == "query":
        return print_completed(
            run_command(
                daemon_command(
                    "status-query",
                    "--config",
                    config,
                    "--max-heartbeat-age-seconds",
                    "120",
                    "--write-state",
                ),
                timeout=180,
            )
        )
    if args.action == "supervise-once":
        return print_completed(run_command(supervise_once_command(args), timeout=300))
    if args.action == "recover-sessions":
        start_rc = start_watchdog(args)
        if start_rc != 0:
            return start_rc
        command = daemon_command(
            "recover-native-sessions",
            "--config",
            config,
            "--reason",
            args.reason
            or "explicit one-click recovery after Codex Desktop/session restart",
            "--json",
        )
        if args.dry_run:
            command.append("--dry-run")
        return print_completed(run_command(command, timeout=60))
    raise AssertionError(args.action)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="No-window launcher for S5 910B tester daemon"
    )
    parser.add_argument(
        "action",
        choices=[
            "start",
            "stop",
            "restart",
            "status",
            "health",
            "query",
            "supervise-once",
            "recover-sessions",
            "ensure-resident",
            "install-resident-task",
            "resident-task-status",
        ],
        nargs="?",
        default="status",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0.0,
        help="0 means use config policy.run_interval_seconds",
    )
    parser.add_argument(
        "--config",
        default="",
        help=(
            "Daemon config path, relative to the repository or absolute. "
            "When omitted, reuse the live resident config before falling back "
            "to the current CANN-Ladder config."
        ),
    )
    parser.add_argument("--max-iterations", type=int, default=0)
    parser.add_argument("--no-bridge", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force-stop runtime process trees; graceful drain is the default",
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=30.0,
        help="Graceful stop wait before reporting still-stopping",
    )
    parser.add_argument(
        "--reason", default="", help="audit reason for recover-sessions"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report recover-sessions effects without changing state",
    )
    parser.add_argument(
        "--preserve-workers",
        action="store_true",
        help="When stopping/restarting, do not taskkill the supervise-loop process tree; existing execute workers are adopted by the next daemon.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_stdio()
    args = parse_args(argv or sys.argv[1:])
    try:
        args.config = selected_config(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.action == "start":
        return start_watchdog(args)
    if args.action == "ensure-resident":
        return ensure_resident(args)
    if args.action == "install-resident-task":
        return install_resident_task(args.config)
    if args.action == "resident-task-status":
        return resident_task_status()
    if args.action == "stop":
        return stop_watchdog(
            "launch_s5_910b.py stop",
            config=args.config,
            force=bool(args.force),
            preserve_workers=bool(args.preserve_workers) or not bool(args.force),
            wait_seconds=args.wait_seconds,
        )
    if args.action == "restart":
        preserve_engine_pump = bool(args.preserve_workers) or not bool(args.force)
        stop_watchdog(
            "launch_s5_910b.py restart",
            config=args.config,
            quiet=False,
            force=True,
            preserve_workers=preserve_engine_pump,
            wait_seconds=args.wait_seconds,
        )
        remaining = wait_for_runtime_quiescence(
            timeout_seconds=args.wait_seconds,
            preserve_engine_pump=preserve_engine_pump,
        )
        if remaining:
            print(
                json.dumps(
                    {
                        "action": "restart_blocked_by_live_prior_generation",
                        "remaining_runtime": remaining,
                        "recovery": (
                            "inspect the recorded PID/start-token before retrying; "
                            "the launcher will not overlap coordinator generations"
                        ),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                file=sys.stderr,
            )
            return 2
        return start_watchdog(args)
    if args.action == "status":
        return launcher_status(args.config)
    return run_foreground_action(args)


if __name__ == "__main__":
    raise SystemExit(main())
