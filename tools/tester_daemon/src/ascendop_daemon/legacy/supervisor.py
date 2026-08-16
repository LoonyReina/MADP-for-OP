from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from ascendop_daemon.runtime.control import clear_stop_request, read_stop_request
from ascendop_daemon.legacy.health import parse_timestamp, read_json
from ascendop_daemon.runtime.locking import process_alive, read_lock_pid
from ascendop_daemon.runtime.process_inspection import windows_process_table as native_windows_process_table
from ascendop_daemon.runtime.process_identity import process_start_token


@dataclass(frozen=True)
class SupervisorOptions:
    config_path: str
    mode: str = "execute"
    write_state: bool = True
    allow_live_execute: bool = False
    clear_stop: bool = False
    max_heartbeat_age_seconds: int = 120
    bridge_max_heartbeat_age_seconds: int = 120
    replace_stale_lock_after_seconds: int = 120
    ensure_bridge: bool = True
    bridge_max_workers: int = 0
    bridge_wait_seconds: int = 1800
    dry_run: bool = False


def supervise_runtime(root: Path, options: SupervisorOptions) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    actions: list[dict[str, Any]] = []

    stop_request_would_be_cleared = False
    if options.clear_stop:
        if options.dry_run:
            if read_stop_request(root):
                actions.append({"target": "daemon", "action": "would_clear_stop"})
                stop_request_would_be_cleared = True
        else:
            removed = clear_stop_request(root)
            if removed:
                actions.append({"target": "daemon", "action": "clear_stop"})

    stop_request = {} if stop_request_would_be_cleared else read_stop_request(root)
    if not options.ensure_bridge:
        disabled_bridge_action = reconcile_disabled_bridge_state(
            root,
            dry_run=options.dry_run,
        )
        if disabled_bridge_action:
            actions.append(disabled_bridge_action)
    if stop_request:
        actions.append(
            {
                "target": "daemon",
                "action": "skip_start",
                "reason": "stop_requested",
                "requested_at": stop_request.get("requested_at", ""),
            }
        )
        if options.ensure_bridge and (
            runtime_process_alive(state_dir, "solver_trigger_bridge")
            or runtime_lock_present(state_dir, "solver_trigger_bridge")
        ):
            actions.append(
                stop_runtime_process(
                    root,
                    "solver_trigger_bridge",
                    dry_run=options.dry_run,
                    reason="daemon stop requested",
                )
            )
    elif not daemon_runtime_ok(state_dir, options.max_heartbeat_age_seconds):
        stop_action: dict[str, Any] | None = None
        if runtime_code_stale(root, state_dir, "daemon_process.json"):
            active_worker_pids = active_execute_worker_pids(root, state_dir)
            if active_worker_pids:
                stop_action = stop_daemon_coordinator_preserving_workers(
                    root,
                    dry_run=options.dry_run,
                    active_worker_pids=active_worker_pids,
                    reason="daemon code changed while execute workers own live harness actions",
                )
            else:
                stop_action = stop_runtime_process(root, "daemon", dry_run=options.dry_run)
            actions.append(stop_action)
        elif runtime_process_alive(state_dir, "daemon"):
            active_worker_pids = active_execute_worker_pids(root, state_dir)
            if active_worker_pids:
                stop_action = stop_daemon_coordinator_preserving_workers(
                    root,
                    dry_run=options.dry_run,
                    active_worker_pids=active_worker_pids,
                    reason="daemon heartbeat stale while execute workers own live harness actions",
                )
            else:
                stop_action = stop_runtime_process(
                    root,
                    "daemon",
                    dry_run=options.dry_run,
                    reason="daemon heartbeat stale",
                )
            actions.append(stop_action)
        elif runtime_lock_present(state_dir, "daemon"):
            stop_action = stop_runtime_process(root, "daemon", dry_run=options.dry_run, reason="daemon lock pid not alive")
            actions.append(stop_action)
        if stop_action and not options.dry_run and not stop_action.get("stop_confirmed", False):
            actions.append(
                {
                    "target": "daemon",
                    "action": "restart_deferred",
                    "reason": "previous daemon PID still owns runtime state",
                    "pid": stop_action.get("pid", 0),
                }
            )
        else:
            daemon_action = start_daemon(root, options)
            actions.append(daemon_action)
            if daemon_action.get("action") == "start_failed":
                actions.append(run_daemon_once(root, options))

    if (
        options.ensure_bridge
        and not stop_request
        and not bridge_runtime_ok(state_dir, options.bridge_max_heartbeat_age_seconds)
    ):
        bridge_stop_action: dict[str, Any] | None = None
        active_worker_pids = active_bridge_worker_pids(root, state_dir)
        if runtime_code_stale(root, state_dir, "solver_trigger_bridge_process.json"):
            if active_worker_pids:
                bridge_stop_action = stop_bridge_coordinator_preserving_workers(
                        root,
                        dry_run=options.dry_run,
                        active_worker_pids=active_worker_pids,
                    )
                actions.append(bridge_stop_action)
            else:
                bridge_stop_action = stop_runtime_process(root, "solver_trigger_bridge", dry_run=options.dry_run)
                actions.append(bridge_stop_action)
        elif runtime_process_alive(state_dir, "solver_trigger_bridge"):
            if active_worker_pids:
                bridge_stop_action = stop_bridge_coordinator_preserving_workers(
                    root,
                    dry_run=options.dry_run,
                    active_worker_pids=active_worker_pids,
                    reason="bridge heartbeat stale while delivery workers own live turns",
                )
            else:
                bridge_stop_action = stop_runtime_process(
                    root,
                    "solver_trigger_bridge",
                    dry_run=options.dry_run,
                    reason="solver trigger bridge heartbeat stale",
                )
            actions.append(bridge_stop_action)
        elif runtime_lock_present(state_dir, "solver_trigger_bridge"):
            if active_worker_pids:
                bridge_stop_action = stop_bridge_coordinator_preserving_workers(
                    root,
                    dry_run=options.dry_run,
                    active_worker_pids=active_worker_pids,
                    reason="bridge lock owner lost while delivery workers own live turns",
                )
            else:
                bridge_stop_action = stop_runtime_process(
                    root,
                    "solver_trigger_bridge",
                    dry_run=options.dry_run,
                    reason="solver trigger bridge lock pid not alive",
                )
            actions.append(bridge_stop_action)
        if bridge_stop_action and not options.dry_run and not bridge_stop_action.get("stop_confirmed", False):
            actions.append(
                {
                    "target": "solver_trigger_bridge",
                    "action": "restart_deferred",
                    "reason": "previous bridge PID still owns runtime state",
                    "pid": bridge_stop_action.get("pid", 0),
                }
            )
        else:
            actions.append(start_bridge(root, options))

    if options.ensure_bridge and not stop_request:
        cleanup = cleanup_stale_bridge_app_servers(root, dry_run=options.dry_run)
        if cleanup:
            actions.append(cleanup)

    result = {
        "time": utc_now_iso(),
        "dry_run": options.dry_run,
        "actions": actions,
    }
    append_supervisor_event(root, "supervise", result)
    (state_dir / "supervisor_state.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return result


def daemon_runtime_ok(state_dir: Path, max_heartbeat_age_seconds: int) -> bool:
    if _daemon_runtime_ok_once(state_dir, max_heartbeat_age_seconds):
        return True
    # The coordinator publishes heartbeat state concurrently with this
    # one-second supervisor poll. Recheck once before converting a transient
    # Windows file/process observation into a destructive restart.
    time.sleep(0.05)
    return _daemon_runtime_ok_once(
        state_dir,
        max_heartbeat_age_seconds,
    ) or daemon_activity_fresh(
        state_dir,
        max_heartbeat_age_seconds,
    )


def _daemon_runtime_ok_once(
    state_dir: Path,
    max_heartbeat_age_seconds: int,
) -> bool:
    lock_pid = read_lock_pid(state_dir / "daemon.lock")
    if lock_pid <= 0 or not process_alive(lock_pid):
        return False
    root = state_dir.parent.parent
    if runtime_code_stale(root, state_dir, "daemon_process.json"):
        return False
    heartbeat = read_json(state_dir / "daemon_heartbeat.json")
    heartbeat_pid = int(heartbeat.get("pid", 0) or 0) if heartbeat else 0
    if heartbeat_pid > 0 and heartbeat_pid != lock_pid:
        return daemon_startup_grace_ok(
            state_dir,
            lock_pid,
            max_heartbeat_age_seconds,
        )
    if not heartbeat:
        return heartbeat_file_fresh(
            state_dir / "daemon_heartbeat.json",
            max_heartbeat_age_seconds,
        ) or daemon_startup_grace_ok(
            state_dir,
            lock_pid,
            max_heartbeat_age_seconds,
        )
    if max_heartbeat_age_seconds <= 0:
        return bool(heartbeat)
    return heartbeat_age_ok(
        heartbeat,
        max_heartbeat_age_seconds,
    ) or daemon_startup_grace_ok(
        state_dir,
        lock_pid,
        max_heartbeat_age_seconds,
    )


def daemon_startup_grace_ok(
    state_dir: Path,
    lock_pid: int,
    max_heartbeat_age_seconds: int,
) -> bool:
    process_record = read_json(state_dir / "daemon_process.json")
    process_pid = int(process_record.get("pid", 0) or 0)
    if process_pid <= 0 or process_pid != lock_pid:
        return False
    started_at = parse_timestamp(str(process_record.get("started_at", "") or ""))
    if started_at is None:
        return False
    grace_seconds = max(10, min(max(1, max_heartbeat_age_seconds), 30))
    age = max(
        0.0,
        (datetime.now(timezone.utc) - started_at).total_seconds(),
    )
    return age <= float(grace_seconds)


def daemon_activity_fresh(
    state_dir: Path,
    max_heartbeat_age_seconds: int,
) -> bool:
    """Confirm the coordinator through its independent critical-loop signal."""

    process_record = read_json(state_dir / "daemon_process.json")
    process_pid = int(process_record.get("pid", 0) or 0)
    if process_pid <= 0 or not process_alive(process_pid):
        return False
    expected_start_token = str(process_record.get("start_token", "") or "")
    if expected_start_token:
        observed_start_token = process_start_token(process_pid)
        if not observed_start_token or observed_start_token != expected_start_token:
            return False
    observer = read_json(state_dir / "critical_thread_observer.json")
    if (
        str(observer.get("status", "") or "") != "ok"
        or int(observer.get("pid", 0) or 0) != process_pid
    ):
        return False
    updated_at = parse_timestamp(str(observer.get("updated_at", "") or ""))
    if updated_at is None:
        return False
    freshness_seconds = max(
        5,
        min(max(1, max_heartbeat_age_seconds), 15),
    )
    age = max(
        0.0,
        (datetime.now(timezone.utc) - updated_at).total_seconds(),
    )
    return age <= float(freshness_seconds)


def bridge_runtime_ok(state_dir: Path, max_heartbeat_age_seconds: int) -> bool:
    lock_pid = read_lock_pid(state_dir / "solver_trigger_bridge.lock")
    heartbeat = read_json(state_dir / "solver_trigger_bridge_heartbeat.json")
    heartbeat_pid = int(heartbeat.get("pid", 0) or 0) if heartbeat else 0
    process_ok = (lock_pid > 0 and process_alive(lock_pid)) or (heartbeat_pid > 0 and process_alive(heartbeat_pid))
    if not process_ok:
        return False
    root = state_dir.parent.parent
    if runtime_code_stale(root, state_dir, "solver_trigger_bridge_process.json"):
        return False
    if max_heartbeat_age_seconds <= 0:
        return bool(heartbeat)
    return heartbeat_age_ok(heartbeat, max_heartbeat_age_seconds)


def heartbeat_age_ok(heartbeat: dict[str, Any], max_age_seconds: int) -> bool:
    if not heartbeat:
        return False
    timestamp = parse_timestamp(str(heartbeat.get("time", "")))
    if timestamp is None:
        return False
    age = max(0, int((datetime.now(timezone.utc) - timestamp).total_seconds()))
    return age <= max_age_seconds


def heartbeat_file_fresh(path: Path, max_age_seconds: int) -> bool:
    """Use file freshness when a concurrent Windows publish is unreadable."""

    if max_age_seconds <= 0:
        return path.exists()
    try:
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return False
    age = max(0.0, (datetime.now(timezone.utc) - modified_at).total_seconds())
    return age <= float(max_age_seconds)


def runtime_code_stale(root: Path, state_dir: Path, process_file: str) -> bool:
    record = read_json(state_dir / process_file)
    started_at = parse_timestamp(str(record.get("started_at", "") or ""))
    if started_at is None:
        return False
    latest_code = latest_runtime_code_timestamp(root, process_record=record)
    if latest_code is None:
        return False
    return latest_code.timestamp() > started_at.timestamp() + 1.0


def latest_runtime_code_timestamp(
    root: Path,
    *,
    process_record: dict[str, Any] | None = None,
) -> datetime | None:
    base = root / "tools" / "tester_daemon"
    paths: list[Path] = []
    if base.exists():
        paths.extend(
            path
            for path in base.rglob("*.py")
            if "tests" not in path.parts
        )
    # The daemon executes the workflow harness in-process, so changing this
    # module must rotate the coordinator just like changing daemon code.
    paths.append(root / "scripts" / "next_workflow.py")
    command = (
        process_record.get("command", [])
        if isinstance(process_record, dict)
        else []
    )
    if isinstance(command, list):
        for index, value in enumerate(command[:-1]):
            if str(value) != "--config":
                continue
            config_path = Path(str(command[index + 1]))
            paths.append(config_path if config_path.is_absolute() else root / config_path)
            break
    latest: float | None = None
    for path in paths:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        latest = mtime if latest is None else max(latest, mtime)
    if latest is None:
        return None
    return datetime.fromtimestamp(latest, tz=timezone.utc)


def runtime_process_alive(state_dir: Path, target: str) -> bool:
    lock_name, _, _ = runtime_file_names(target)
    lock_pid = read_lock_pid(state_dir / lock_name)
    process_pid = runtime_process_file_pid(state_dir, target)
    return (lock_pid > 0 and process_alive(lock_pid)) or (process_pid > 0 and process_alive(process_pid))


def runtime_lock_present(state_dir: Path, target: str) -> bool:
    lock_name, _, _ = runtime_file_names(target)
    lock_path = state_dir / lock_name
    return lock_path.exists()


def runtime_process_file_pid(state_dir: Path, target: str) -> int:
    _, _, process_name = runtime_file_names(target)
    record = read_json(state_dir / process_name)
    return int(record.get("pid", 0) or 0) if record else 0


def runtime_file_names(target: str) -> tuple[str, str, str]:
    if target == "solver_trigger_bridge":
        return (
            "solver_trigger_bridge.lock",
            "solver_trigger_bridge_heartbeat.json",
            "solver_trigger_bridge_process.json",
        )
    if target == "supervisor_loop":
        return (
            "supervisor_loop.lock",
            "supervisor_loop_heartbeat.json",
            "supervisor_loop_process.json",
        )
    return ("daemon.lock", "daemon_heartbeat.json", "daemon_process.json")


def reconcile_disabled_bridge_state(root: Path, *, dry_run: bool) -> dict[str, Any] | None:
    """Discard stale bridge ownership without signaling a PID reused by Windows."""
    state_dir = root / "TestUtils" / "tester_daemon"
    lock_path = state_dir / "solver_trigger_bridge.lock"
    heartbeat_path = state_dir / "solver_trigger_bridge_heartbeat.json"
    heartbeat = read_json(heartbeat_path)
    process_record = read_json(state_dir / "solver_trigger_bridge_process.json")
    if not lock_path.exists() and str(heartbeat.get("mode", "")).startswith("disabled"):
        return None
    if not lock_path.exists() and not heartbeat and not process_record:
        return None

    candidate_pids = {
        read_lock_pid(lock_path),
        int(heartbeat.get("pid", 0) or 0),
        int(process_record.get("pid", 0) or 0),
    }
    candidate_pids.discard(0)
    matching_pids: list[int] = []
    if os.name == "nt" and candidate_pids:
        for process in windows_process_table():
            pid = int(process.get("ProcessId", 0) or 0)
            if pid not in candidate_pids:
                continue
            command = str(process.get("CommandLine", "") or "").lower().replace("/", "\\")
            if "tools\\tester_daemon\\bridge.py" in command and " run" in command:
                matching_pids.append(pid)
    elif os.name != "nt":
        matching_pids = [pid for pid in candidate_pids if process_alive(pid)]

    action = "would_reconcile_disabled_bridge" if dry_run else "reconciled_disabled_bridge"
    record: dict[str, Any] = {
        "target": "solver_trigger_bridge",
        "action": action,
        "recorded_pids": sorted(candidate_pids),
        "matching_bridge_pids": sorted(matching_pids),
    }
    if matching_pids:
        record["action"] = "disabled_bridge_still_running"
        record["reason"] = "refusing to clear live bridge ownership without the explicit stop path"
        return record
    if dry_run:
        return record

    lock_path.unlink(missing_ok=True)
    heartbeat_path.write_text(
        json.dumps(
            {
                "time": utc_now_iso(),
                "pid": 0,
                "mode": "disabled-reconciled",
                "previous_pids": sorted(candidate_pids),
                "reason": "daemon-side bridge disabled; stale ownership record cleared identity-safely",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    clear_bridge_workers_file(root, state_dir, record)
    append_supervisor_event(root, "disabled_bridge_state_reconciled", record)
    return record


def bridge_worker_pids_from_state(state_dir: Path) -> set[int]:
    pids: set[int] = set()
    workers = read_json(state_dir / "solver_trigger_bridge_workers.json").get("workers", {})
    if isinstance(workers, dict):
        for worker in workers.values():
            if not isinstance(worker, dict):
                continue
            pid = int(worker.get("pid", 0) or 0)
            if pid > 0:
                pids.add(pid)
    return pids


def active_bridge_worker_pids(root: Path, state_dir: Path) -> set[int]:
    pids = bridge_worker_pids_from_state(state_dir)
    pids.update(recent_bridge_worker_pids(state_dir))
    if os.name == "nt":
        pids.update(bridge_worker_pids_from_process_table(root, windows_process_table()))
    return {pid for pid in pids if process_alive(pid)}


def execute_worker_pids_from_state(state_dir: Path) -> set[int]:
    workers = read_json(state_dir / "execute_workers.json").get("workers", {})
    if not isinstance(workers, dict):
        return set()
    return {
        int(worker.get("pid", 0) or 0)
        for worker in workers.values()
        if isinstance(worker, dict) and int(worker.get("pid", 0) or 0) > 0
    }


def execute_worker_pids_from_process_table(root: Path, processes: list[dict[str, Any]]) -> set[int]:
    root_text = str(root).lower().replace("/", "\\")
    pids: set[int] = set()
    for proc in processes:
        command = str(proc.get("CommandLine", "") or "").lower().replace("/", "\\")
        if "tools\\tester_daemon\\daemon.py execute-worker" not in command:
            continue
        if root_text and root_text not in command:
            continue
        pid = int(proc.get("ProcessId", 0) or 0)
        if pid > 0:
            pids.add(pid)
    return pids


def active_execute_worker_pids(root: Path, state_dir: Path) -> set[int]:
    if os.name == "nt":
        # A stale worker record can point at a PID reused by an unrelated
        # Windows service. Only a matching live command line is kill-safe.
        return execute_worker_pids_from_process_table(root, windows_process_table())
    pids = execute_worker_pids_from_state(state_dir)
    return {pid for pid in pids if process_alive(pid)}


def recent_bridge_worker_pids(state_dir: Path, *, max_lines: int = 200, max_age_seconds: int = 900) -> set[int]:
    path = state_dir / "solver_trigger_bridge_events.jsonl"
    if not path.exists():
        return set()
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-max_lines:]
    except OSError:
        return set()
    pids: set[int] = set()
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        timestamp = parse_timestamp(str(record.get("time", "") or ""))
        if timestamp is not None and (datetime.now(timezone.utc) - timestamp).total_seconds() > max_age_seconds:
            continue
        event = str(record.get("event", "") or "")
        if event not in {"bridge_worker_started", "bridge_watch_worker_started"}:
            continue
        pid = int(record.get("pid", 0) or 0)
        if pid > 0:
            pids.add(pid)
    return pids


def recent_runtime_pids_from_supervisor_events(
    state_dir: Path,
    target: str,
    *,
    max_lines: int = 200,
    max_age_seconds: int = 900,
) -> set[int]:
    path = state_dir / "supervisor_events.jsonl"
    if not path.exists():
        return set()
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-max_lines:]
    except OSError:
        return set()
    pids: set[int] = set()
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        timestamp = parse_timestamp(str(record.get("time", "") or ""))
        if timestamp is not None and (datetime.now(timezone.utc) - timestamp).total_seconds() > max_age_seconds:
            continue
        if str(record.get("target", "") or "") == target:
            pid = int(record.get("pid", 0) or 0)
            if pid > 0:
                pids.add(pid)
        actions = record.get("actions", [])
        if not isinstance(actions, list):
            continue
        for action in actions:
            if not isinstance(action, dict) or str(action.get("target", "") or "") != target:
                continue
            pid = int(action.get("pid", 0) or 0)
            if pid > 0:
                pids.add(pid)
    return pids


def windows_process_table() -> list[dict[str, Any]]:
    return native_windows_process_table()


def bridge_worker_pids_from_process_table(root: Path, processes: list[dict[str, Any]]) -> set[int]:
    root_text = str(root).lower().replace("/", "\\")
    pids: set[int] = set()
    for proc in processes:
        command = str(proc.get("CommandLine", "") or "").lower().replace("/", "\\")
        if not command:
            continue
        if "tools\\tester_daemon\\bridge.py" not in command:
            continue
        if " deliver " not in command and " watch " not in command:
            continue
        if root_text and root_text not in command:
            continue
        pid = int(proc.get("ProcessId", 0) or 0)
        if pid > 0:
            pids.add(pid)
    return pids


def descendant_process_ids(processes: list[dict[str, Any]], root_pids: set[int]) -> set[int]:
    children: dict[int, list[int]] = {}
    for proc in processes:
        pid = int(proc.get("ProcessId", 0) or 0)
        parent = int(proc.get("ParentProcessId", 0) or 0)
        if pid <= 0:
            continue
        children.setdefault(parent, []).append(pid)
    descendants: set[int] = set()
    stack = list(root_pids)
    while stack:
        parent = stack.pop()
        for child in children.get(parent, []):
            if child in descendants:
                continue
            descendants.add(child)
            stack.append(child)
    return descendants


def related_runtime_process_ids(root: Path, state_dir: Path, target: str, primary_pid: int) -> list[int]:
    pids = {pid for pid in (primary_pid, runtime_process_file_pid(state_dir, target)) if pid > 0}
    if target == "solver_trigger_bridge":
        pids.update(bridge_worker_pids_from_state(state_dir))
    if os.name == "nt" and target == "solver_trigger_bridge":
        processes = windows_process_table()
        pids.update(bridge_worker_pids_from_process_table(root, processes))
    return sorted(pid for pid in pids if pid > 0)


def taskkill_tree(pid: int) -> dict[str, Any]:
    command = ["taskkill", "/PID", str(pid), "/T", "/F"]
    try:
        completed = subprocess.run(
            command,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
            timeout=5,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "pid": pid,
            "returncode": 124,
            "stdout": str(exc.stdout or "")[-500:],
            "stderr": f"taskkill timed out after {exc.timeout}s"[-500:],
        }
    return {
        "pid": pid,
        "returncode": completed.returncode,
        "stdout": completed.stdout[-500:],
        "stderr": completed.stderr[-500:],
    }


def stop_process_force(pid: int) -> dict[str, Any]:
    error = ""
    try:
        os.kill(pid, signal.SIGTERM)
        returncode = 0
    except OSError as exc:
        returncode = 1
        error = str(exc)
    return {
        "pid": pid,
        "returncode": returncode,
        "stdout": "",
        "stderr": error[-500:],
    }


def clear_bridge_workers_file(root: Path, state_dir: Path, record: dict[str, Any]) -> None:
    path = state_dir / "solver_trigger_bridge_workers.json"
    if not path.exists():
        return
    try:
        path.write_text(
            json.dumps({"updated_at": utc_now_iso(), "workers": {}}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        record["workers_cleared"] = str(path.relative_to(root))
    except OSError as exc:
        record["workers_clear_error"] = str(exc)


def cleanup_stale_bridge_app_servers(root: Path, *, dry_run: bool) -> dict[str, Any] | None:
    if os.name != "nt":
        return None
    state_dir = root / "TestUtils" / "tester_daemon"
    current_bridge_pids = {
        read_lock_pid(state_dir / "solver_trigger_bridge.lock"),
        int(read_json(state_dir / "solver_trigger_bridge_heartbeat.json").get("pid", 0) or 0),
        runtime_process_file_pid(state_dir, "solver_trigger_bridge"),
    }
    current_bridge_pids = {pid for pid in current_bridge_pids if pid > 0 and process_alive(pid)}
    active_worker_pids = {
        pid for pid in bridge_worker_pids_from_state(state_dir) if pid > 0 and process_alive(pid)
    }
    recent_roots = recent_runtime_pids_from_supervisor_events(state_dir, "solver_trigger_bridge")
    recent_roots.update(recent_bridge_worker_pids(state_dir))
    stale_roots = {pid for pid in recent_roots if pid > 0 and pid not in current_bridge_pids and pid not in active_worker_pids}
    if not stale_roots:
        return None
    processes = windows_process_table()
    descendants = descendant_process_ids(processes, stale_roots)
    # A bridge coordinator reload intentionally leaves delivery workers alive.
    # Those workers are still descendants of the old coordinator, so exclude
    # their complete subtrees before cleaning genuinely orphaned app-servers.
    protected_worker_tree = set(active_worker_pids)
    protected_worker_tree.update(descendant_process_ids(processes, active_worker_pids))
    descendants.difference_update(protected_worker_tree)
    app_server_pids = sorted(
        {
            int(proc.get("ProcessId", 0) or 0)
            for proc in processes
            if int(proc.get("ProcessId", 0) or 0) in descendants
            and "app-server" in str(proc.get("CommandLine", "") or "").lower()
        }
    )
    if not app_server_pids:
        return None
    record: dict[str, Any] = {
        "target": "solver_trigger_bridge",
        "action": "would_cleanup_stale_app_servers" if dry_run else "cleanup_stale_app_servers",
        "stale_roots": sorted(stale_roots),
        "pids": app_server_pids,
    }
    if dry_run:
        return record
    taskkill_results: list[dict[str, Any]] = []
    for pid in app_server_pids:
        if process_alive(pid):
            taskkill_results.append(taskkill_tree(pid))
            time.sleep(0.1)
    if taskkill_results:
        record["taskkill_results"] = taskkill_results
    append_supervisor_event(root, "cleanup_stale_app_servers", record)
    return record


def stop_runtime_process(
    root: Path,
    target: str,
    *,
    dry_run: bool,
    reason: str = "tester daemon code changed after runtime start",
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    lock_name, heartbeat_name, _ = runtime_file_names(target)
    lock_path = state_dir / lock_name
    lock_pid = read_lock_pid(lock_path)
    heartbeat = read_json(state_dir / heartbeat_name)
    heartbeat_pid = int(heartbeat.get("pid", 0) or 0) if heartbeat else 0
    pid = lock_pid if lock_pid > 0 else heartbeat_pid
    related_pids = related_runtime_process_ids(root, state_dir, target, pid)
    record: dict[str, Any] = {
        "target": target,
        "action": "would_stop_for_code_update" if dry_run else "stopped_for_code_update",
        "pid": pid,
        "related_process_pids": related_pids,
        "reason": reason,
    }
    if dry_run:
        return record
    pid_was_alive = pid > 0 and process_alive(pid)
    if pid_was_alive:
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except OSError as exc:
            record["error"] = str(exc)
    if os.name == "nt":
        taskkill_results: list[dict[str, Any]] = []
        if pid_was_alive and target not in {"solver_trigger_bridge", "supervisor_loop"}:
            # os.kill(SIGTERM) can make os.kill(pid, 0) briefly report dead
            # before Windows has released file handles. Always ask Windows to
            # terminate the primary tree and verify its disappearance below.
            primary_result = taskkill_tree(pid)
            taskkill_results.append(primary_result)
            record["taskkill_returncode"] = primary_result["returncode"]
            record["taskkill_stdout"] = primary_result["stdout"]
            record["taskkill_stderr"] = primary_result["stderr"]
            time.sleep(0.2)
        elif pid > 0:
            # A tree kill of either coordinator would also kill independently
            # owned runtime work. Stop only the coordinator; explicit daemon or
            # bridge shutdown paths own their respective descendants.
            primary_result = stop_process_force(pid)
            record["stop_process_returncode"] = primary_result["returncode"]
            record["stop_process_stdout"] = primary_result["stdout"]
            record["stop_process_stderr"] = primary_result["stderr"]
            time.sleep(0.2)
        for related_pid in related_pids:
            if related_pid == pid:
                continue
            if related_pid > 0 and process_alive(related_pid):
                result = taskkill_tree(related_pid)
                taskkill_results.append(result)
                if related_pid == pid:
                    record["taskkill_returncode"] = result["returncode"]
                    record["taskkill_stdout"] = result["stdout"]
                    record["taskkill_stderr"] = result["stderr"]
                time.sleep(0.1)
        if taskkill_results:
            record["taskkill_results"] = taskkill_results
        stop_process_results: list[dict[str, Any]] = []
        for related_pid in related_pids:
            if related_pid > 0 and process_alive(related_pid):
                result = stop_process_force(related_pid)
                stop_process_results.append(result)
                if related_pid == pid:
                    record["stop_process_returncode"] = result["returncode"]
                    record["stop_process_stdout"] = result["stdout"]
                    record["stop_process_stderr"] = result["stderr"]
                time.sleep(0.1)
        if stop_process_results:
            record["stop_process_results"] = stop_process_results
    remaining_pids = wait_for_runtime_processes_exit(related_pids or [pid], timeout_seconds=5.0)
    record["remaining_process_pids"] = remaining_pids
    record["stop_confirmed"] = pid <= 0 or pid not in remaining_pids
    if target == "solver_trigger_bridge":
        clear_bridge_workers_file(root, state_dir, record)
    if target == "supervisor_loop":
        try:
            unregister_windows_supervisor_task(root)
            record["scheduled_task_removed"] = windows_supervisor_task_name(root)
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            record["scheduled_task_remove_error"] = str(exc)
    if record["stop_confirmed"]:
        try:
            lock_path.unlink(missing_ok=True)
            record["lock_removed"] = str(lock_path.relative_to(root))
        except OSError as exc:
            record["lock_remove_error"] = str(exc)
    append_supervisor_event(root, "process_stopped_for_code_update", record)
    return record


def wait_for_runtime_processes_exit(pids: list[int], *, timeout_seconds: float) -> list[int]:
    tracked = {int(pid) for pid in pids if int(pid) > 0}
    if not tracked:
        return []
    deadline = time.monotonic() + max(0.1, timeout_seconds)
    remaining = set(tracked)
    while remaining and time.monotonic() < deadline:
        if os.name == "nt":
            table = windows_process_table()
            if table:
                present = {int(item.get("ProcessId", 0) or 0) for item in table}
                remaining.intersection_update(present)
            else:
                remaining = {pid for pid in remaining if process_alive(pid)}
        else:
            remaining = {pid for pid in remaining if process_alive(pid)}
        if remaining:
            time.sleep(0.2)
    return sorted(remaining)


def stop_bridge_coordinator_preserving_workers(
    root: Path,
    *,
    dry_run: bool,
    active_worker_pids: set[int],
    reason: str = "bridge code changed while delivery workers own live turns",
) -> dict[str, Any]:
    """Reload bridge control-plane code without terminating live turn owners."""
    state_dir = root / "TestUtils" / "tester_daemon"
    lock_path = state_dir / "solver_trigger_bridge.lock"
    lock_pid = read_lock_pid(lock_path)
    heartbeat = read_json(state_dir / "solver_trigger_bridge_heartbeat.json")
    heartbeat_pid = int(heartbeat.get("pid", 0) or 0) if heartbeat else 0
    pid = lock_pid if lock_pid > 0 else heartbeat_pid
    record: dict[str, Any] = {
        "target": "solver_trigger_bridge",
        "action": (
            "would_stop_coordinator_preserving_workers"
            if dry_run
            else "stopped_coordinator_preserving_workers"
        ),
        "pid": pid,
        "reason": reason,
        "preserved_worker_pids": sorted(active_worker_pids),
    }
    if dry_run:
        return record
    if pid > 0 and process_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except OSError as exc:
            record["error"] = str(exc)
    if pid > 0 and process_alive(pid):
        result = stop_process_force(pid)
        record["stop_process_returncode"] = result["returncode"]
        record["stop_process_stdout"] = result["stdout"]
        record["stop_process_stderr"] = result["stderr"]
    remaining_pids = wait_for_runtime_processes_exit([pid], timeout_seconds=5.0)
    record["remaining_process_pids"] = remaining_pids
    record["stop_confirmed"] = pid <= 0 or pid not in remaining_pids
    if record["stop_confirmed"]:
        try:
            lock_path.unlink(missing_ok=True)
            record["lock_removed"] = str(lock_path.relative_to(root))
        except OSError as exc:
            record["lock_remove_error"] = str(exc)
    append_supervisor_event(root, "bridge_coordinator_reloaded_preserving_workers", record)
    return record


def stop_daemon_coordinator_preserving_workers(
    root: Path,
    *,
    dry_run: bool,
    active_worker_pids: set[int],
    reason: str,
) -> dict[str, Any]:
    """Reload daemon policy code without terminating live harness action owners."""
    state_dir = root / "TestUtils" / "tester_daemon"
    lock_path = state_dir / "daemon.lock"
    lock_pid = read_lock_pid(lock_path)
    heartbeat = read_json(state_dir / "daemon_heartbeat.json")
    heartbeat_pid = int(heartbeat.get("pid", 0) or 0) if heartbeat else 0
    pid = lock_pid if lock_pid > 0 else heartbeat_pid
    record: dict[str, Any] = {
        "target": "daemon",
        "action": (
            "would_stop_coordinator_preserving_workers"
            if dry_run
            else "stopped_coordinator_preserving_workers"
        ),
        "pid": pid,
        "reason": reason,
        "preserved_worker_pids": sorted(active_worker_pids),
    }
    if dry_run:
        return record
    if pid > 0 and process_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except OSError as exc:
            record["error"] = str(exc)
    if pid > 0 and process_alive(pid):
        result = stop_process_force(pid)
        record["stop_process_returncode"] = result["returncode"]
        record["stop_process_stdout"] = result["stdout"]
        record["stop_process_stderr"] = result["stderr"]
    remaining_pids = wait_for_runtime_processes_exit([pid], timeout_seconds=5.0)
    record["remaining_process_pids"] = remaining_pids
    record["stop_confirmed"] = pid <= 0 or pid not in remaining_pids
    if record["stop_confirmed"]:
        try:
            lock_path.unlink(missing_ok=True)
            record["lock_removed"] = str(lock_path.relative_to(root))
        except OSError as exc:
            record["lock_remove_error"] = str(exc)
    append_supervisor_event(root, "daemon_coordinator_reloaded_preserving_workers", record)
    return record


def start_daemon(root: Path, options: SupervisorOptions) -> dict[str, Any]:
    command = [
        background_python_executable(),
        "tools/tester_daemon/daemon.py",
        "run",
        "--config",
        options.config_path,
        "--mode",
        options.mode,
        "--replace-stale-lock-after-seconds",
        str(options.replace_stale_lock_after_seconds),
    ]
    if options.write_state:
        command.append("--write-state")
    if options.allow_live_execute:
        command.append("--allow-live-execute")
    if options.clear_stop:
        command.append("--clear-stop")
    return start_process(
        root,
        "daemon",
        command,
        process_file="daemon_process.json",
        dry_run=options.dry_run,
        verify_seconds=6.0,
    )


def start_bridge(root: Path, options: SupervisorOptions) -> dict[str, Any]:
    command = [
        background_python_executable(),
        "tools/tester_daemon/src/ascendop_daemon/legacy/bridge.py",
        "run",
        "--config",
        options.config_path,
        "--max-workers",
        str(options.bridge_max_workers),
        "--wait-seconds",
        str(options.bridge_wait_seconds),
        "--replace-stale-lock-after-seconds",
        str(options.replace_stale_lock_after_seconds),
    ]
    env_overrides = bridge_env_overrides(root, options.config_path)
    return start_process(
        root,
        "solver_trigger_bridge",
        command,
        process_file="solver_trigger_bridge_process.json",
        dry_run=options.dry_run,
        env_overrides=env_overrides,
    )


def start_supervisor_loop(root: Path, options: SupervisorOptions, *, interval_seconds: float = 0.0) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    code_stale = runtime_code_stale(
        root,
        state_dir,
        "supervisor_loop_process.json",
    )
    if (
        supervisor_loop_runtime_ok(state_dir, options.max_heartbeat_age_seconds)
        and not code_stale
    ):
        heartbeat = read_json(state_dir / "supervisor_loop_heartbeat.json")
        pid = (
            runtime_process_file_pid(state_dir, "supervisor_loop")
            or read_lock_pid(state_dir / "supervisor_loop.lock")
            or int(heartbeat.get("pid", 0) or 0)
        )
        return {
            "target": "supervisor_loop",
            "action": "already_running",
            "pid": pid,
        }
    restart_actions: list[dict[str, Any]] = []
    restart_reason = (
        "supervisor loop code changed"
        if code_stale
        else "supervisor loop heartbeat stale"
    )
    if runtime_process_alive(state_dir, "supervisor_loop"):
        restart_actions.append(
            stop_runtime_process(
                root,
                "supervisor_loop",
                dry_run=options.dry_run,
                reason=restart_reason,
            )
        )
    command = [
        background_python_executable(),
        "tools/tester_daemon/daemon.py",
        "supervise-loop",
        "--config",
        options.config_path,
        "--mode",
        options.mode,
        "--max-heartbeat-age-seconds",
        str(options.max_heartbeat_age_seconds),
        "--bridge-max-heartbeat-age-seconds",
        str(options.bridge_max_heartbeat_age_seconds),
        "--replace-stale-lock-after-seconds",
        str(options.replace_stale_lock_after_seconds),
    ]
    if options.write_state:
        command.append("--write-state")
    if options.allow_live_execute:
        command.append("--allow-live-execute")
    if options.clear_stop:
        command.append("--clear-stop")
    if not options.ensure_bridge:
        command.append("--no-bridge")
    if options.bridge_max_workers > 0:
        command.extend(["--bridge-max-workers", str(options.bridge_max_workers)])
    command.extend(["--bridge-wait-seconds", str(options.bridge_wait_seconds)])
    if interval_seconds > 0:
        command.extend(["--interval-seconds", str(interval_seconds)])
    command.append("--json")
    record = start_process(
        root,
        "supervisor_loop",
        command,
        process_file="supervisor_loop_process.json",
        dry_run=options.dry_run,
        verify_seconds=2.0,
    )
    if restart_actions:
        return {
            "target": "supervisor_loop",
            "action": "would_restart" if options.dry_run else "restarted",
            "reason": restart_reason,
            "actions": [*restart_actions, record],
        }
    return record


def supervisor_loop_runtime_ok(state_dir: Path, max_heartbeat_age_seconds: int) -> bool:
    if not runtime_process_alive(state_dir, "supervisor_loop"):
        return False
    heartbeat = read_json(state_dir / "supervisor_loop_heartbeat.json")
    timestamp = parse_timestamp(str(heartbeat.get("time", "") or ""))
    if timestamp is not None:
        return heartbeat_age_ok(heartbeat, max_heartbeat_age_seconds)
    process_record = read_json(state_dir / "supervisor_loop_process.json")
    started_at = parse_timestamp(str(process_record.get("started_at", "") or ""))
    if started_at is None:
        return False
    startup_grace_seconds = max(10, max_heartbeat_age_seconds)
    age = max(0, int((datetime.now(timezone.utc) - started_at).total_seconds()))
    return age <= startup_grace_seconds


def bridge_env_overrides(root: Path, config_path: str) -> dict[str, str]:
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root / config_file
        if not config_file.exists():
            config_file = Path(config_path)
    data = read_json(config_file)
    policy = data.get("policy", {}) if isinstance(data.get("policy"), dict) else {}
    mode = str(policy.get("solver_bridge_app_server_mode", "") or "").strip().lower()
    require_visible = bool(policy.get("solver_bridge_require_ide_visible_delivery", False))
    allow_storage_visible = bool(policy.get("solver_bridge_allow_storage_visible_delivery", False))
    cli_resume_fallback = bool(policy.get("solver_bridge_cli_resume_fallback", False))
    if require_visible and not allow_storage_visible and mode not in {"proxy", "ws"}:
        mode = "proxy"
    env: dict[str, str] = {}
    if mode in {"proxy", "stdio", "ws"}:
        env["ASCENDOP_CODEX_APP_SERVER_MODE"] = mode
    if require_visible:
        env["ASCENDOP_REQUIRE_IDE_VISIBLE_DELIVERY"] = "1"
    env["ASCENDOP_ALLOW_STORAGE_VISIBLE_DELIVERY"] = "1" if allow_storage_visible else "0"
    env["ASCENDOP_CODEX_CLI_RESUME_FALLBACK"] = "1" if cli_resume_fallback else "0"
    env["ASCENDOP_LOCAL_TURN_TIMEOUT_SECONDS"] = str(
        int(policy.get("solver_bridge_local_turn_timeout_seconds", 7200) or 7200)
    )
    return env


def start_process(
    root: Path,
    target: str,
    command: list[str],
    *,
    process_file: str,
    dry_run: bool,
    env_overrides: dict[str, str] | None = None,
    verify_seconds: float = 0.5,
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    logs = state_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    stamp = utc_now_iso().replace(":", "").replace("+", "Z")
    stdout_path = logs / f"supervisor_{target}_{stamp}.out.log"
    stderr_path = logs / f"supervisor_{target}_{stamp}.err.log"
    if dry_run:
        record = {
            "target": target,
            "action": "would_start",
            "command": command,
            "stdout": str(stdout_path.relative_to(root)),
            "stderr": str(stderr_path.relative_to(root)),
        }
        if env_overrides:
            record["env_overrides"] = env_overrides
        return record
    creationflags = process_creation_flags(include_breakaway=True)
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    breakaway_fallback_error = ""
    independent_launch_error = ""
    independent_launch_method = ""
    proc: subprocess.Popen[str] | None = None
    independent_pid = 0
    if os.name == "nt" and target == "supervisor_loop":
        try:
            independent_pid = start_windows_scheduled_task_process(
                root,
                command,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                env_overrides=env_overrides,
            )
            independent_launch_method = "windows-scheduled-task"
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            scheduled_task_error = str(exc)
            try:
                independent_pid = start_windows_independent_process(
                    root,
                    command,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    env_overrides=env_overrides,
                )
                independent_launch_method = "windows-cim-process"
            except (OSError, RuntimeError, subprocess.SubprocessError) as cim_exc:
                independent_launch_error = (
                    f"scheduled-task: {scheduled_task_error}; CIM: {cim_exc}"
                )
    if independent_pid <= 0:
        with stdout_path.open("w", encoding="utf-8") as out, stderr_path.open("w", encoding="utf-8") as err:
            try:
                proc = subprocess.Popen(
                    command,
                    cwd=str(root),
                    stdin=subprocess.DEVNULL,
                    stdout=out,
                    stderr=err,
                    creationflags=creationflags,
                    startupinfo=process_startupinfo(),
                    env=env,
                )
            except PermissionError as exc:
                if os.name != "nt" or not creationflags & windows_breakaway_flag():
                    raise
                breakaway_fallback_error = str(exc)
                proc = subprocess.Popen(
                    command,
                    cwd=str(root),
                    stdin=subprocess.DEVNULL,
                    stdout=out,
                    stderr=err,
                    creationflags=process_creation_flags(include_breakaway=False),
                    startupinfo=process_startupinfo(),
                    env=env,
                )
    time.sleep(max(0.0, verify_seconds))
    pid = independent_pid if independent_pid > 0 else int(proc.pid if proc is not None else 0)
    returncode = None if independent_pid > 0 and process_alive(independent_pid) else (proc.poll() if proc else 1)
    runtime_alive = runtime_process_alive(state_dir, target)
    if returncode is not None and not runtime_alive:
        record = {
            "target": target,
            "action": "start_failed",
            "pid": pid,
            "returncode": returncode,
            "command": command,
            "started_at": utc_now_iso(),
            "stdout": str(stdout_path.relative_to(root)),
            "stderr": str(stderr_path.relative_to(root)),
            "stdout_tail": read_text_tail(stdout_path),
            "stderr_tail": read_text_tail(stderr_path),
        }
        if env_overrides:
            record["env_overrides"] = env_overrides
        if breakaway_fallback_error:
            record["breakaway_fallback_error"] = breakaway_fallback_error
        if independent_launch_error:
            record["independent_launch_error"] = independent_launch_error
        if independent_launch_method:
            record["launcher"] = independent_launch_method
        (state_dir / process_file).write_text(
            json.dumps(record, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        append_supervisor_event(root, "process_start_failed", record)
        return record
    record = {
        "target": target,
        "action": "started",
        "pid": pid,
        "start_token": process_start_token(pid),
        "command": command,
        "started_at": utc_now_iso(),
        "stdout": str(stdout_path.relative_to(root)),
        "stderr": str(stderr_path.relative_to(root)),
    }
    if env_overrides:
        record["env_overrides"] = env_overrides
    if breakaway_fallback_error:
        record["breakaway_fallback_error"] = breakaway_fallback_error
    if independent_launch_error:
        record["independent_launch_error"] = independent_launch_error
    if independent_launch_method:
        record["launcher"] = independent_launch_method
    (state_dir / process_file).write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    append_supervisor_event(root, "process_started", record)
    return record


def start_windows_independent_process(
    root: Path,
    command: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    env_overrides: dict[str, str] | None = None,
) -> int:
    """Create a hidden breakaway process without a shell wrapper."""

    if os.name != "nt":
        raise OSError("Windows independent launch is only available on Windows")
    launcher_command = windows_resident_launcher_command(
        root,
        command,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        env_overrides=env_overrides,
    )
    process = subprocess.Popen(
        launcher_command,
        cwd=str(root),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=process_creation_flags(include_breakaway=True),
        startupinfo=process_startupinfo(),
    )
    return int(process.pid)


def windows_supervisor_task_name(root: Path) -> str:
    digest = sha1(str(root.resolve()).lower().encode("utf-8")).hexdigest()[:10]
    return f"AscendOP_tester_daemon_supervisor_{digest}"


def start_windows_scheduled_task_process(
    root: Path,
    command: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    env_overrides: dict[str, str] | None = None,
) -> int:
    """Launch the resident supervisor through the Windows Task Scheduler service.

    Codex command runners use kill-on-close job objects. WMI-created processes
    can still inherit that job on some Windows builds; Task Scheduler is the
    stable system-owned launch boundary. The task is one-shot, never periodic,
    and is replaced on each explicit supervisor launch.
    """

    if os.name != "nt":
        raise OSError("Windows scheduled-task launch is only available on Windows")
    task_name = windows_supervisor_task_name(root)
    launcher_command = windows_resident_launcher_command(
        root,
        command,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        env_overrides=env_overrides,
    )
    payload_dir = root / "TestUtils" / "tester_daemon" / "resident_launch_payloads"
    payload_dir.mkdir(parents=True, exist_ok=True)
    xml_path = payload_dir / f"{task_name}.xml"
    write_windows_task_xml(
        xml_path,
        execute=launcher_command[0],
        arguments=subprocess.list2cmdline(launcher_command[1:]),
        working_directory=str(root),
    )
    unregister_windows_supervisor_task(root)
    completed = subprocess.run(
        ["schtasks.exe", "/Create", "/TN", task_name, "/XML", str(xml_path), "/F"],
        cwd=str(root),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        creationflags=diagnostic_creation_flags(),
        startupinfo=process_startupinfo(),
        timeout=30,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"scheduled task registration failed rc={completed.returncode}: "
            f"{(completed.stderr or completed.stdout).strip()[-1000:]}"
        )
    started = subprocess.run(
        ["schtasks.exe", "/Run", "/TN", task_name],
        cwd=str(root),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        creationflags=diagnostic_creation_flags(),
        startupinfo=process_startupinfo(),
        timeout=30,
    )
    if started.returncode != 0:
        raise RuntimeError(
            f"scheduled task start failed rc={started.returncode}: "
            f"{(started.stderr or started.stdout).strip()[-1000:]}"
        )

    state_dir = root / "TestUtils" / "tester_daemon"
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        pid = read_lock_pid(state_dir / "supervisor_loop.lock")
        if pid > 0 and process_alive(pid):
            return pid
        heartbeat = read_json(state_dir / "supervisor_loop_heartbeat.json")
        pid = int(heartbeat.get("pid", 0) or 0)
        if pid > 0 and process_alive(pid):
            return pid
        time.sleep(0.25)
    raise RuntimeError(
        f"scheduled task {task_name} started but no live supervisor heartbeat appeared"
    )


def windows_resident_launcher_command(
    root: Path,
    command: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    env_overrides: dict[str, str] | None = None,
) -> list[str]:
    state_dir = root / "TestUtils" / "tester_daemon" / "resident_launch_payloads"
    state_dir.mkdir(parents=True, exist_ok=True)
    digest = sha1("\0".join(command).encode("utf-8")).hexdigest()[:16]
    payload_path = state_dir / f"resident_{digest}.json"
    payload = {
        "command": command,
        "cwd": str(root),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "state": str(payload_path.with_suffix(".state.json")),
        "environment": env_overrides or {},
    }
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    executable = Path(command[0])
    if executable.name.lower() not in {"python.exe", "pythonw.exe"}:
        executable = Path(sys.executable)
    if executable.name.lower() == "pythonw.exe":
        executable = executable.with_name("python.exe")
    resident_command = [
        str(executable),
        str((root / "tools" / "tester_daemon" / "resident_process.py").resolve()),
        "--payload",
        str(payload_path.resolve()),
    ]
    if os.name != "nt":
        return resident_command
    system_root = Path(os.environ.get("SystemRoot", r"C:\Windows"))
    wscript = system_root / "System32" / "wscript.exe"
    if not wscript.exists():
        pythonw = executable.with_name("pythonw.exe")
        return [
            str(pythonw if pythonw.exists() else executable),
            *resident_command[1:],
        ]
    launcher_path = payload_path.with_suffix(".launcher.vbs")
    command_line = subprocess.list2cmdline(resident_command).replace('"', '""')
    working_directory = str(root.resolve()).replace('"', '""')
    launcher_path.write_text(
        "\n".join(
            (
                'Set shell = CreateObject("WScript.Shell")',
                f'shell.CurrentDirectory = "{working_directory}"',
                f'exitCode = shell.Run("{command_line}", 0, True)',
                "WScript.Quit exitCode",
                "",
            )
        ),
        encoding="ascii",
    )
    return [
        str(wscript),
        "//B",
        "//Nologo",
        str(launcher_path.resolve()),
    ]


def write_windows_task_xml(
    path: Path,
    *,
    execute: str,
    arguments: str,
    working_directory: str,
    description: str = "AscendOP tester daemon resident supervisor",
    on_demand_only: bool = False,
) -> None:
    from xml.etree import ElementTree

    namespace = "http://schemas.microsoft.com/windows/2004/02/mit/task"
    ElementTree.register_namespace("", namespace)
    task = ElementTree.Element(f"{{{namespace}}}Task", {"version": "1.4"})
    registration = ElementTree.SubElement(task, f"{{{namespace}}}RegistrationInfo")
    ElementTree.SubElement(
        registration,
        f"{{{namespace}}}Description",
    ).text = description
    triggers = ElementTree.SubElement(task, f"{{{namespace}}}Triggers")
    if not on_demand_only:
        trigger = ElementTree.SubElement(triggers, f"{{{namespace}}}TimeTrigger")
        ElementTree.SubElement(trigger, f"{{{namespace}}}StartBoundary").text = (
            datetime.now().astimezone().replace(microsecond=0) + timedelta(hours=1)
        ).isoformat()
        ElementTree.SubElement(trigger, f"{{{namespace}}}Enabled").text = "true"
    principals = ElementTree.SubElement(task, f"{{{namespace}}}Principals")
    principal = ElementTree.SubElement(principals, f"{{{namespace}}}Principal", {"id": "Author"})
    username = os.environ.get("USERNAME", "")
    domain = os.environ.get("USERDOMAIN", "")
    user_id = f"{domain}\\{username}" if domain and username else username
    if user_id:
        ElementTree.SubElement(principal, f"{{{namespace}}}UserId").text = user_id
    ElementTree.SubElement(principal, f"{{{namespace}}}LogonType").text = "InteractiveToken"
    ElementTree.SubElement(principal, f"{{{namespace}}}RunLevel").text = "LeastPrivilege"
    settings = ElementTree.SubElement(task, f"{{{namespace}}}Settings")
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
        ("ExecutionTimeLimit", "PT0S"),
        ("Priority", "7"),
    ):
        ElementTree.SubElement(settings, f"{{{namespace}}}{name}").text = value
    actions = ElementTree.SubElement(task, f"{{{namespace}}}Actions", {"Context": "Author"})
    action = ElementTree.SubElement(actions, f"{{{namespace}}}Exec")
    ElementTree.SubElement(action, f"{{{namespace}}}Command").text = execute
    ElementTree.SubElement(action, f"{{{namespace}}}Arguments").text = arguments
    ElementTree.SubElement(action, f"{{{namespace}}}WorkingDirectory").text = working_directory
    path.parent.mkdir(parents=True, exist_ok=True)
    ElementTree.ElementTree(task).write(path, encoding="utf-16", xml_declaration=True)


def start_windows_on_demand_task(
    root: Path,
    command: list[str],
    *,
    task_name: str,
    stdout_path: Path,
    stderr_path: Path,
    description: str,
    env_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    if os.name != "nt":
        raise OSError("Windows on-demand task launch is only available on Windows")
    if not task_name.startswith("AscendOP_"):
        raise ValueError("on-demand task name must use the AscendOP_ prefix")
    launcher_command = windows_resident_launcher_command(
        root,
        command,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        env_overrides=env_overrides,
    )
    payload_dir = root / "TestUtils" / "tester_daemon" / "resident_launch_payloads"
    xml_path = payload_dir / f"{task_name}.xml"
    write_windows_task_xml(
        xml_path,
        execute=launcher_command[0],
        arguments=subprocess.list2cmdline(launcher_command[1:]),
        working_directory=str(root),
        description=description,
        on_demand_only=True,
    )
    unregister_windows_task(root, task_name)
    created = subprocess.run(
        ["schtasks.exe", "/Create", "/TN", task_name, "/XML", str(xml_path), "/F"],
        cwd=str(root),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        creationflags=diagnostic_creation_flags(),
        startupinfo=process_startupinfo(),
        timeout=30,
    )
    if created.returncode != 0:
        raise RuntimeError(
            f"scheduled task registration failed rc={created.returncode}: "
            f"{(created.stderr or created.stdout).strip()[-1000:]}"
        )
    digest = sha1("\0".join(command).encode("utf-8")).hexdigest()[:16]
    state_path = payload_dir / f"resident_{digest}.state.json"
    previous_mtime_ns = (
        state_path.stat().st_mtime_ns if state_path.exists() else 0
    )
    started = subprocess.run(
        ["schtasks.exe", "/Run", "/TN", task_name],
        cwd=str(root),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        creationflags=diagnostic_creation_flags(),
        startupinfo=process_startupinfo(),
        timeout=30,
    )
    if started.returncode != 0:
        raise RuntimeError(
            f"scheduled task start failed rc={started.returncode}: "
            f"{(started.stderr or started.stdout).strip()[-1000:]}"
        )
    deadline = time.monotonic() + 20.0
    runtime_state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        if state_path.exists() and state_path.stat().st_mtime_ns > previous_mtime_ns:
            runtime_state = read_json(state_path)
            if str(runtime_state.get("state") or "") in {
                "running",
                "completed",
                "failed",
            }:
                break
        time.sleep(0.1)
    if not runtime_state:
        unregister_windows_task(root, task_name)
        raise RuntimeError(
            f"scheduled task {task_name} started but resident state did not advance"
        )
    if str(runtime_state.get("state") or "") == "failed":
        raise RuntimeError(
            f"scheduled task {task_name} failed during startup: "
            f"{str(runtime_state.get('error') or '')[-1000:]}"
        )
    return {
        "task_name": task_name,
        "state": str(runtime_state.get("state") or "start-requested"),
        "pid": int(runtime_state.get("pid", 0) or 0),
        "command": command,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "resident_state_path": str(state_path),
        "task_xml": str(xml_path),
    }


def unregister_windows_task(root: Path, task_name: str) -> None:
    if os.name != "nt":
        return
    for command in (
        ["schtasks.exe", "/End", "/TN", task_name],
        ["schtasks.exe", "/Delete", "/TN", task_name, "/F"],
    ):
        subprocess.run(
            command,
            cwd=str(root),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            creationflags=diagnostic_creation_flags(),
            startupinfo=process_startupinfo(),
            timeout=20,
            check=False,
        )


def unregister_windows_supervisor_task(root: Path) -> None:
    if os.name != "nt":
        return
    task_name = windows_supervisor_task_name(root)
    unregister_windows_task(root, task_name)


def run_daemon_once(root: Path, options: SupervisorOptions) -> dict[str, Any]:
    stop_request = read_stop_request(root)
    if stop_request:
        return {
            "target": "daemon",
            "action": "sync_tick_skipped_stop_requested",
            "requested_at": str(stop_request.get("requested_at") or ""),
            "reason": str(stop_request.get("reason") or ""),
        }
    command = [
        background_python_executable(),
        "tools/tester_daemon/daemon.py",
        "run",
        "--config",
        options.config_path,
        "--mode",
        options.mode,
        "--replace-stale-lock-after-seconds",
        "1",
        "--max-ticks",
        "1",
    ]
    if options.write_state:
        command.append("--write-state")
    if options.allow_live_execute:
        command.append("--allow-live-execute")
    if options.dry_run:
        return {"target": "daemon", "action": "would_run_sync_tick", "command": command}
    started_at = utc_now_iso()
    timeout_seconds = 120
    try:
        completed = subprocess.run(
            command,
            cwd=str(root),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        record = {
            "target": "daemon",
            "action": "sync_tick_timeout",
            "command": command,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
            "timeout_seconds": timeout_seconds,
            "stdout_tail": subprocess_timeout_tail(exc.stdout),
            "stderr_tail": subprocess_timeout_tail(exc.stderr),
            "recovery": "child terminated by subprocess timeout; resident supervisor continues",
        }
        append_supervisor_event(root, "sync_tick_timeout", record)
        return record
    record = {
        "target": "daemon",
        "action": "sync_tick_after_failed_start",
        "returncode": completed.returncode,
        "command": command,
        "started_at": started_at,
        "completed_at": utc_now_iso(),
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
    }
    append_supervisor_event(root, "sync_tick_after_failed_start", record)
    return record


def subprocess_timeout_tail(value: str | bytes | None, *, limit: int = 2000) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return str(value)[-max(1, int(limit)) :]


def process_creation_flags(*, include_breakaway: bool = False) -> int:
    if os.name != "nt":
        return 0
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
    if include_breakaway:
        # Codex/IDE shells can run commands inside a Windows job object.  When
        # the job permits breakaway, this keeps watchdog children alive after
        # the one-shot supervise command exits.  Some Windows job policies deny
        # it; start_process catches that and falls back to ordinary hidden mode.
        flags |= windows_breakaway_flag()
    return flags


def diagnostic_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return getattr(subprocess, "CREATE_NO_WINDOW", 0)


def windows_breakaway_flag() -> int:
    return getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0x01000000)


def process_startupinfo() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return startupinfo


def background_python_executable() -> str:
    executable = Path(sys.executable)
    if os.name == "nt" and executable.name.lower() == "python.exe":
        pythonw = executable.with_name("pythonw.exe")
        if pythonw.exists():
            return str(pythonw)
    return sys.executable


def read_text_tail(path: Path, max_chars: int = 2000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max_chars:]


def append_supervisor_event(root: Path, event: str, payload: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    record = {"time": utc_now_iso(), "event": event, **payload}
    with (state_dir / "supervisor_events.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
