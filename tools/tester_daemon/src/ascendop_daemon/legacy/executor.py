from __future__ import annotations

import contextlib
import importlib
import json
import math
import os
import shlex
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import ActionKind, DaemonPlan, GateDecision
from ascendop_daemon.core.models import utc_now_iso
from ascendop_daemon.control_plane.resource_manager import ResourceManager
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.runtime.locking import process_alive
from ascendop_daemon.runtime.locking import read_lock_pid
from ascendop_daemon.automation.worker_liveness import worker_process_status


ALLOWED_ACTIONS = {
    ActionKind.GENERATE_WORKSPACE,
    ActionKind.PREPARE_SUBMIT,
    ActionKind.DISPATCH_SUBMIT,
    ActionKind.RESTORE_SUBMIT,
    ActionKind.REQUEUE_SUBMIT,
    ActionKind.REPAIR_QUEUE,
    ActionKind.ADVANCE_RELEASE,
    ActionKind.RECOVER_BLOCKED,
    ActionKind.RECOVER_GITPARTNER_WORKTREE,
    ActionKind.HEARTBEAT_ACTIVE_REQUEST,
    ActionKind.CANCEL_STALLED_REQUEST,
    ActionKind.COLLECT_PROFILER_EVIDENCE,
}

RESOURCE_ACTIONS = {
    ActionKind.GENERATE_WORKSPACE,
    ActionKind.DISPATCH_SUBMIT,
    ActionKind.RECOVER_BLOCKED,
    ActionKind.RECOVER_GITPARTNER_WORKTREE,
    ActionKind.HEARTBEAT_ACTIVE_REQUEST,
    ActionKind.CANCEL_STALLED_REQUEST,
}

EXECUTE_WORKERS_FILE = "execute_workers.json"
EXECUTE_WORKER_EVENTS_FILE = "execute_worker_events.jsonl"
EXECUTE_FAILURES_FILE = "execute_failures.jsonl"
LAST_EXECUTE_RESULT_FILE = "last_execute_result.json"
RECOVERY_ACTION_SUCCESS_DEDUP_SECONDS = 10


class Executor:
    def __init__(
        self,
        root: Path,
        dry_run: bool = False,
        expected_action_id: str = "",
        allow_live_execute: bool = False,
        resource_manager: ResourceManager | None = None,
        async_execute: bool = False,
        config_path: str = "",
        failed_retry_seconds: int = 0,
    ) -> None:
        self.root = root
        self.dry_run = dry_run
        self.expected_action_id = expected_action_id
        self.allow_live_execute = allow_live_execute
        self.resource_manager = resource_manager
        self.async_execute = async_execute
        self.config_path = config_path
        self.failed_retry_seconds = failed_retry_seconds

    def run(self, plan: DaemonPlan) -> int:
        if self.async_execute and self.resource_manager is not None:
            prune_execute_workers(self.root, self.resource_manager)
        selected = plan.selected
        if selected is None:
            print("execute: no selected action")
            write_last_execute_result(self.root, {"outcome": "no_selected", "ran": False})
            return 0
        if selected.action not in ALLOWED_ACTIONS:
            if selected.action in {ActionKind.NOTIFY_SOLVER, ActionKind.NOTIFY_TESTER_CASEGEN}:
                print(f"execute: action {selected.action.value} is relay-owned; bridge will deliver it")
                write_last_execute_result(
                    self.root,
                    {
                        "outcome": "relay_owned",
                        "ran": False,
                        "op": selected.row.op,
                        "action": selected.action.value,
                        "action_id": selected.action_id,
                    },
                )
                return 0
            print(f"execute: action {selected.action.value} is not daemon-executable; no-op")
            write_last_execute_result(
                self.root,
                {
                    "outcome": "not_executable",
                    "ran": False,
                    "op": selected.row.op,
                    "action": selected.action.value,
                    "action_id": selected.action_id,
                },
            )
            return 0
        if was_successfully_executed(self.root, selected.action_id):
            print("execute: selected action_id was already executed successfully; waiting for board to advance")
            print(f"selected: {selected.action_id}")
            write_last_execute_result(
                self.root,
                {
                    "outcome": "already_successful",
                    "ran": False,
                    "op": selected.row.op,
                    "action": selected.action.value,
                    "action_id": selected.action_id,
                },
            )
            return 0
        backoff = failed_execute_backoff(self.root, selected.action_id, self.failed_retry_seconds)
        if backoff and failed_execute_backoff_is_delegated_to_specialized_policy(selected, backoff):
            backoff = None
        if backoff:
            print("execute postponed: selected action_id is in failed-execute backoff")
            print(f"selected: {selected.action_id}")
            print(f"remaining_seconds: {backoff.get('remaining_seconds')}")
            write_last_execute_result(
                self.root,
                {
                    "outcome": "backoff",
                    "ran": False,
                    "op": selected.row.op,
                    "action": selected.action.value,
                    "action_id": selected.action_id,
                    "remaining_seconds": backoff.get("remaining_seconds"),
                },
            )
            return 0
        if not self.dry_run:
            if self.expected_action_id:
                if selected.action_id != self.expected_action_id:
                    print("execute refused: selected action_id does not match expected action_id")
                    print(f"selected: {selected.action_id}")
                    print(f"expected: {self.expected_action_id}")
                    write_last_execute_result(
                        self.root,
                        {
                            "outcome": "expected_action_mismatch",
                            "ran": False,
                            "op": selected.row.op,
                            "action": selected.action.value,
                            "action_id": selected.action_id,
                        },
                    )
                    return 3
            elif not self.allow_live_execute:
                print("execute refused: true execute requires --expected-action-id or --allow-live-execute")
                print(f"selected: {selected.action_id}")
                write_last_execute_result(
                    self.root,
                    {
                        "outcome": "live_execute_refused",
                        "ran": False,
                        "op": selected.row.op,
                        "action": selected.action.value,
                        "action_id": selected.action_id,
                    },
                )
                return 3
        if not selected.command.startswith("python scripts\\next_workflow.py "):
            print(f"execute refused: command is not trusted harness command: {selected.command}")
            write_last_execute_result(
                self.root,
                {
                    "outcome": "untrusted_command",
                    "ran": False,
                    "op": selected.row.op,
                    "action": selected.action.value,
                    "action_id": selected.action_id,
                },
            )
            return 2
        argv = shlex.split(selected.command.replace("\\", "/"), posix=False)
        argv = [arg.replace("scripts/next_workflow.py", "scripts\\next_workflow.py") for arg in argv]
        print(f"execute command: {' '.join(argv)}")
        if self.dry_run:
            write_last_execute_result(
                self.root,
                {
                    "outcome": "dry_run",
                    "ran": False,
                    "op": selected.row.op,
                    "action": selected.action.value,
                    "action_id": selected.action_id,
                },
            )
            return 0
        if self.async_execute and action_requires_resource(selected.action):
            return self.start_async_worker(selected, argv)
        lease = None
        try:
            if self.resource_manager is not None and action_requires_resource(selected.action):
                try:
                    lease = self.resource_manager.acquire(selected)
                except RuntimeError as exc:
                    print(f"execute postponed: {exc}")
                    return 4
                if lease:
                    print(f"resource lease acquired: {lease.get('resource_id')}")
            if action_requires_resource(selected.action):
                returncode = run_with_execute_heartbeat(self.root, argv, selected.action_id, lease)
                execution_mode = "sync"
            else:
                returncode = run_harness_inline(self.root, argv, selected.action_id)
                execution_mode = "inline"
            if returncode == 0:
                record_successful_execute(self.root, selected.row.op, selected.action.value, selected.action_id)
            else:
                record_failed_execute(
                    self.root,
                    selected.row.op,
                    selected.action.value,
                    selected.action_id,
                    returncode,
                    execute_log_details(self.root, selected.action_id),
                )
            write_last_execute_result(
                self.root,
                {
                    "outcome": "completed",
                    "ran": True,
                    "execution_mode": execution_mode,
                    "returncode": returncode,
                    "resource_bound": bool(lease),
                    "op": selected.row.op,
                    "action": selected.action.value,
                    "action_id": selected.action_id,
                    "finished_at": utc_now_iso(),
                },
            )
            return returncode
        finally:
            if self.resource_manager is not None:
                self.resource_manager.release(lease)

    def start_async_worker(self, selected, argv: list[str]) -> int:
        workers = read_execute_workers(self.root)
        active = active_worker_for_action(self.root, workers, selected.action_id)
        if active:
            print(f"execute: action worker already active pid={active.get('pid')}")
            write_last_execute_result(
                self.root,
                {
                    "outcome": "worker_already_active",
                    "ran": False,
                    "op": selected.row.op,
                    "action": selected.action.value,
                    "action_id": selected.action_id,
                    "pid": active.get("pid"),
                },
            )
            return 0

        lease = None
        if self.resource_manager is not None and action_requires_resource(selected.action):
            try:
                lease = self.resource_manager.acquire(selected)
            except RuntimeError as exc:
                print(f"execute postponed: {exc}")
                return 0
            if lease:
                print(f"resource lease acquired: {lease.get('resource_id')}")

        payload_path = write_worker_payload(self.root, selected, argv, lease, self.config_path)
        cmd = [
            background_python_executable(),
            str(self.root / "tools" / "tester_daemon" / "daemon.py"),
            "execute-worker",
            "--payload",
            str(payload_path),
        ]
        creationflags = process_creation_flags()
        logs = self.root / "TestUtils" / "tester_daemon" / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        digest = action_digest(selected.action_id)
        stdout_path = logs / f"execute_worker_{selected.row.op}_{digest}.out.log"
        stderr_path = logs / f"execute_worker_{selected.row.op}_{digest}.err.log"
        with stdout_path.open("a", encoding="utf-8") as out, stderr_path.open("a", encoding="utf-8") as err:
            proc = subprocess.Popen(
                cmd,
                cwd=self.root,
                stdout=out,
                stderr=err,
                creationflags=creationflags,
                startupinfo=process_startupinfo(),
            )

        if lease and self.resource_manager is not None:
            lease["pid"] = proc.pid
            replace_lease_pid(self.resource_manager, lease, proc.pid)

        workers[selected.action_id] = {
            "pid": proc.pid,
            "op": selected.row.op,
            "action": selected.action.value,
            "action_id": selected.action_id,
            "command": " ".join(argv),
            "lease": lease or {},
            "payload_path": relpath(payload_path, self.root),
            "stdout": relpath(stdout_path, self.root),
            "stderr": relpath(stderr_path, self.root),
            "started_at": utc_now_iso(),
            "status": "started",
        }
        write_execute_workers(self.root, workers)
        append_execute_worker_event(self.root, "execute_worker_started", workers[selected.action_id])
        write_last_execute_result(
            self.root,
            {
                "outcome": "started_async_worker",
                "ran": True,
                "execution_mode": "async_worker",
                "returncode": None,
                "resource_bound": bool(lease),
                "op": selected.row.op,
                "action": selected.action.value,
                "action_id": selected.action_id,
                "pid": proc.pid,
                "started_at": workers[selected.action_id]["started_at"],
            },
        )
        print(f"execute worker started: pid={proc.pid}")
        return 0


def record_successful_execute(root: Path, op: str, action: str, action_id: str) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    path = state_dir / "scheduler_history.json"
    previous = read_scheduler_history(path)
    previous_op = str(previous.get("selected_op", ""))
    previous_count = int(previous.get("consecutive_count", 0) or 0)
    consecutive_count = previous_count + 1 if previous_op == op else 1
    payload = {
        "selected_op": op,
        "selected_action": action,
        "consecutive_count": consecutive_count,
        "last_successful_action_id": action_id,
    }
    state_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with (state_dir / "execute_history.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "time": utc_now_iso(),
                    "op": op,
                    "action": action,
                    "action_id": action_id,
                },
                ensure_ascii=False,
            )
            + "\n"
        )


def record_failed_execute(
    root: Path,
    op: str,
    action: str,
    action_id: str,
    returncode: int,
    details: dict[str, Any] | None = None,
) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "time": utc_now_iso(),
        "op": op,
        "action": action,
        "action_id": action_id,
        "returncode": returncode,
    }
    if details:
        payload.update(details)
    with (state_dir / EXECUTE_FAILURES_FILE).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def action_requires_resource(action: ActionKind) -> bool:
    return action in RESOURCE_ACTIONS


def read_execute_workers(root: Path) -> dict[str, Any]:
    path = root / "TestUtils" / "tester_daemon" / EXECUTE_WORKERS_FILE
    data = read_scheduler_history(path)
    workers = data.get("workers", {})
    return workers if isinstance(workers, dict) else {}


def write_execute_workers(root: Path, workers: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / EXECUTE_WORKERS_FILE).write_text(
        json.dumps({"updated_at": utc_now_iso(), "workers": workers}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_last_execute_result(root: Path, payload: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    data = {"updated_at": utc_now_iso(), **payload}
    (state_dir / LAST_EXECUTE_RESULT_FILE).write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def active_worker_for_action(root: Path, workers: dict[str, Any], action_id: str) -> dict[str, Any] | None:
    worker = workers.get(action_id)
    if not isinstance(worker, dict):
        return None
    return worker if worker_process_status(root, action_id, worker).active else None


def prune_execute_workers(root: Path, resource_manager: ResourceManager) -> dict[str, Any]:
    workers = read_execute_workers(root)
    kept: dict[str, Any] = {}
    changed = False
    for action_id, worker in workers.items():
        if not isinstance(worker, dict):
            changed = True
            continue
        pid = int(worker.get("pid", 0) or 0)
        status = worker_process_status(root, action_id, worker)
        if status.active:
            kept_worker = dict(worker)
            if status.effective_pid > 0 and status.effective_pid != pid:
                kept_worker["pid"] = status.effective_pid
                lease = kept_worker.get("lease") if isinstance(kept_worker.get("lease"), dict) else None
                if lease:
                    lease["pid"] = status.effective_pid
                    replace_lease_pid(resource_manager, lease, status.effective_pid)
                append_execute_worker_event(
                    root,
                    "execute_worker_adopted_process",
                    {
                        "action_id": action_id,
                        "old_pid": pid,
                        "effective_pid": status.effective_pid,
                        "child_pid": status.child_pid,
                        "reason": status.reason,
                    },
                )
            kept[action_id] = kept_worker
            continue
        changed = True
        lease = worker.get("lease") if isinstance(worker.get("lease"), dict) else None
        resource_manager.release(lease)
        if not has_terminal_execute_record_after(root, action_id, str(worker.get("started_at", "") or "")):
            record_failed_execute(
                root,
                str(worker.get("op", "") or ""),
                str(worker.get("action", "") or ""),
                action_id,
                -999,
                {
                    "failure_kind": "execute_worker_exited_without_terminal_record",
                    "pid": pid,
                    "started_at": worker.get("started_at", ""),
                    **execute_log_details(root, action_id),
                },
            )
        append_execute_worker_event(
            root,
            "execute_worker_pruned",
            {
                "action_id": action_id,
                "pid": pid,
                "op": worker.get("op", ""),
                "action": worker.get("action", ""),
            },
        )
    if changed:
        write_execute_workers(root, kept)
    return kept


def write_worker_payload(
    root: Path,
    selected: GateDecision,
    argv: list[str],
    lease: dict[str, Any] | None,
    config_path: str,
) -> Path:
    payload_dir = root / "TestUtils" / "tester_daemon" / "execute_worker_payloads"
    payload_dir.mkdir(parents=True, exist_ok=True)
    path = payload_dir / f"{selected.row.op}_{action_digest(selected.action_id)}.json"
    payload = {
        "root": str(root),
        "config_path": config_path,
        "argv": argv,
        "action_id": selected.action_id,
        "op": selected.row.op,
        "action": selected.action.value,
        "lease": lease or {},
        "created_at": utc_now_iso(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def run_execute_worker(payload_path: Path) -> int:
    payload = read_scheduler_history(payload_path)
    root = Path(str(payload.get("root", "")))
    argv = [str(arg) for arg in payload.get("argv", [])] if isinstance(payload.get("argv"), list) else []
    action_id = str(payload.get("action_id", "") or "")
    op = str(payload.get("op", "") or "")
    action = str(payload.get("action", "") or "")
    lease = payload.get("lease") if isinstance(payload.get("lease"), dict) else None
    config_path = Path(str(payload.get("config_path", "") or "tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json"))
    if not root or not argv or not action_id:
        raise RuntimeError(f"invalid execute worker payload: {payload_path}")
    returncode = -998
    try:
        obsolete_reason = obsolete_worker_action_reason(root, argv, action_id, op, action)
        if obsolete_reason:
            append_execute_worker_event(
                root,
                "execute_worker_action_obsolete",
                {
                    "action_id": action_id,
                    "op": op,
                    "action": action,
                    "reason": obsolete_reason,
                },
            )
            returncode = 0
            record_successful_execute(root, op, action, action_id)
            return returncode
        append_execute_worker_event(root, "execute_worker_command_started", {"action_id": action_id, "op": op, "action": action})
        returncode = run_with_worker_heartbeat(root, argv, action_id, lease)
        if returncode == 0:
            record_successful_execute(root, op, action, action_id)
        else:
            record_failed_execute(root, op, action, action_id, returncode, execute_log_details(root, action_id))
        return returncode
    except Exception as exc:
        details = {
            "failure_kind": "execute_worker_exception",
            "error": repr(exc),
            "traceback": traceback.format_exc()[-4000:],
            **execute_log_details(root, action_id),
        }
        append_execute_worker_event(
            root,
            "execute_worker_exception",
            {"action_id": action_id, "op": op, "action": action, "error": repr(exc)},
        )
        record_failed_execute(root, op, action, action_id, returncode, details)
        return returncode
    finally:
        if lease:
            from ascendop_daemon.runtime.config_loader import load_config

            config_full_path = root / config_path if not config_path.is_absolute() else config_path
            ResourceManager(
                root,
                load_config(config_full_path, apply_completion_markers=True),
            ).release(lease)
        append_execute_worker_event(
            root,
            "execute_worker_command_finished",
            {"action_id": action_id, "op": op, "action": action, "returncode": returncode},
        )


def obsolete_worker_action_reason(
    root: Path,
    argv: list[str],
    action_id: str,
    op: str,
    action: str,
) -> str:
    if action != ActionKind.DISPATCH_SUBMIT.value or "--attach-existing" not in argv:
        return ""
    parts = action_id.split("|", 3)
    test_version = parts[1].strip() if len(parts) > 1 else ""
    if not op or not test_version:
        return ""
    result_path = root / "operators_testresult" / op / test_version / "RESULT.md"
    if result_path.is_file():
        return f"result already archived before attach-existing worker start: {result_path.relative_to(root)}"
    return ""


def run_with_worker_heartbeat(
    root: Path,
    argv: list[str],
    action_id: str,
    lease: dict[str, Any] | None,
    interval_seconds: float = 1.0,
) -> int:
    logs = root / "TestUtils" / "tester_daemon" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    digest = action_digest(action_id)
    stdout_path = logs / f"execute_child_{digest}.out.log"
    stderr_path = logs / f"execute_child_{digest}.err.log"
    child_argv = no_window_harness_argv(argv)
    write_action_worker_heartbeat(root, action_id, lease, active=True, child_pid=None)
    with NamedProcessLock(
        root,
        "gitpartner_client_transport",
        stale_after_seconds=120,
        wait_timeout_seconds=300,
        on_wait=lambda _elapsed: write_action_worker_heartbeat(
            root,
            action_id,
            lease,
            active=True,
            child_pid=None,
        ),
    ):
        with stdout_path.open("a", encoding="utf-8") as out, stderr_path.open("a", encoding="utf-8") as err:
            proc = subprocess.Popen(
                child_argv,
                cwd=root,
                stdout=out,
                stderr=err,
                creationflags=process_creation_flags(),
                startupinfo=process_startupinfo(),
            )
            append_execute_worker_event(
                root,
                "execute_worker_child_started",
                {"action_id": action_id, "child_pid": proc.pid, "stdout": relpath(stdout_path, root), "stderr": relpath(stderr_path, root)},
            )
            while True:
                returncode = proc.poll()
                if returncode is not None:
                    write_action_worker_heartbeat(root, action_id, lease, active=False, child_pid=proc.pid)
                    return returncode
                write_action_worker_heartbeat(root, action_id, lease, active=True, child_pid=proc.pid)
                time.sleep(interval_seconds)


def write_action_worker_heartbeat(
    root: Path,
    action_id: str,
    lease: dict[str, Any] | None,
    active: bool,
    child_pid: int | None,
) -> None:
    state_dir = root / "TestUtils" / "tester_daemon" / "execute_worker_heartbeats"
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "time": utc_now_iso(),
        "pid": os.getpid(),
        "child_pid": child_pid,
        "active": active,
        "action_id": action_id,
        "resource_lease_count": 1 if lease else 0,
    }
    (state_dir / f"{action_digest(action_id)}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def replace_lease_pid(resource_manager: ResourceManager, lease: dict[str, Any], pid: int) -> None:
    resource_id = lease.get("resource_id")
    action_id = lease.get("action_id")
    leases = resource_manager.read_leases()
    for current in leases:
        if current.get("resource_id") == resource_id and current.get("action_id") == action_id:
            current["pid"] = pid
    resource_manager.write_leases(leases)


def append_execute_worker_event(root: Path, event: str, payload: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    with (state_dir / EXECUTE_WORKER_EVENTS_FILE).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"time": utc_now_iso(), "event": event, **payload}, ensure_ascii=False) + "\n")


def action_digest(action_id: str) -> str:
    return sha1(action_id.encode("utf-8")).hexdigest()[:12]


def relpath(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def read_scheduler_history(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def was_successfully_executed(root: Path, action_id: str) -> bool:
    state_dir = root / "TestUtils" / "tester_daemon"
    previous = read_scheduler_history(state_dir / "scheduler_history.json")
    if previous.get("last_successful_action_id") == action_id:
        if successful_action_effect_invalidated(root, action_id):
            return False
        return True
    history_path = state_dir / "execute_history.jsonl"
    if not history_path.exists():
        return False
    try:
        lines = history_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    op, version, _action = split_action_id(action_id)
    restored_after_success = False
    for line in reversed(lines[-200:]):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("action_id") == action_id:
            if successful_action_effect_invalidated(root, action_id):
                return False
            return not restored_after_success
        if op and version and record.get("op") == op and record.get("action") == ActionKind.RESTORE_SUBMIT.value:
            record_action_id = str(record.get("action_id", "") or "")
            record_op, record_version, _ = split_action_id(record_action_id)
            if record_op == op and record_version == version:
                restored_after_success = True
    return False


def successful_action_effect_invalidated(root: Path, action_id: str) -> bool:
    return (
        restore_submit_effect_missing(root, action_id)
        or repair_queue_effect_missing(root, action_id)
        or dispatch_submit_has_infra_fail_result(root, action_id)
        or dispatch_submit_transport_failed(root, action_id)
        or recovery_action_success_dedup_expired(root, action_id)
    )


def recovery_action_success_dedup_expired(root: Path, action_id: str) -> bool:
    _op, _version, action = split_action_id(action_id)
    if action not in {
        ActionKind.RECOVER_BLOCKED.value,
        ActionKind.RECOVER_GITPARTNER_WORKTREE.value,
        ActionKind.HEARTBEAT_ACTIVE_REQUEST.value,
        ActionKind.CANCEL_STALLED_REQUEST.value,
    }:
        return False
    record = latest_successful_execute(root, action_id)
    if not record:
        return False
    record_time = parse_utc_timestamp(str(record.get("time", "") or ""))
    if record_time is None:
        return False
    age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - record_time.timestamp())
    return age_seconds >= RECOVERY_ACTION_SUCCESS_DEDUP_SECONDS


def latest_successful_execute(root: Path, action_id: str) -> dict[str, Any] | None:
    history_path = root / "TestUtils" / "tester_daemon" / "execute_history.jsonl"
    if not history_path.exists():
        return None
    try:
        lines = history_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in reversed(lines[-200:]):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("action_id") == action_id:
            return record
    return None


def restore_submit_effect_missing(root: Path, action_id: str) -> bool:
    op, version, action = split_action_id(action_id)
    if action != ActionKind.RESTORE_SUBMIT.value or not op or not version:
        return False
    return not (root / "TestUtils" / "submit" / op / version).exists()


def repair_queue_effect_missing(root: Path, action_id: str) -> bool:
    op, version, action = split_action_id(action_id)
    if action != ActionKind.REPAIR_QUEUE.value or not op or not version:
        return False
    submit_path = root / "TestUtils" / "submit" / op / version
    result_path = root / "operators_testresult" / op / version / "RESULT.md"
    return submit_path.exists() and result_path.exists()


def dispatch_submit_has_infra_fail_result(root: Path, action_id: str) -> bool:
    op, version, action = split_action_id(action_id)
    if action != ActionKind.DISPATCH_SUBMIT.value or not op or not version:
        return False
    result_path = root / "operators_testresult" / op / version / "RESULT.md"
    if not result_path.exists():
        return False
    try:
        text = result_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    for line in text.splitlines():
        if line.lower().startswith("verdict:"):
            return line.split(":", 1)[1].strip().split()[0].upper() == "INFRA_FAIL"
    return False


def dispatch_submit_transport_failed(root: Path, action_id: str) -> bool:
    op, version, action = split_action_id(action_id)
    if action != ActionKind.DISPATCH_SUBMIT.value or not op or not version:
        return False
    for status_path in dispatch_transport_status_paths(root, version):
        status = read_scheduler_history(status_path)
        state = str(status.get("state", "") or "").strip().lower()
        if state in {"failed", "failure", "error", "cancelled", "canceled", "timeout", "timed_out"}:
            return True
        exit_code = status.get("exit_code")
        if exit_code not in (None, "", 0, "0"):
            return True
    return False


def dispatch_transport_status_paths(root: Path, version: str) -> list[Path]:
    output_root = root / "GitPartner" / "output"
    candidates = [output_root / f"{version}_gitpartner_b_local_both" / "status.json"]
    if output_root.exists():
        candidates.extend(path for path in output_root.glob(f"{version}*/status.json") if path.is_file())
    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.exists():
            unique.append(path)
    return unique


def split_action_id(action_id: str) -> tuple[str, str, str]:
    parts = action_id.split("|", 3)
    if len(parts) < 3:
        return "", "", ""
    return parts[0], parts[1], parts[2]


def has_terminal_execute_record_after(root: Path, action_id: str, started_at: str) -> bool:
    started = parse_utc_timestamp(started_at)
    state_dir = root / "TestUtils" / "tester_daemon"
    for path_name in ("execute_history.jsonl", EXECUTE_FAILURES_FILE):
        path = state_dir / path_name
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in reversed(lines[-200:]):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict) or record.get("action_id") != action_id:
                continue
            if started is None:
                return True
            record_time = parse_utc_timestamp(str(record.get("time", "") or ""))
            if record_time is None or record_time >= started:
                return True
    return False


def latest_failed_execute(root: Path, action_id: str) -> dict[str, Any] | None:
    history_path = root / "TestUtils" / "tester_daemon" / EXECUTE_FAILURES_FILE
    if not history_path.exists():
        return None
    try:
        lines = history_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in reversed(lines[-200:]):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("action_id") == action_id:
            return enrich_failed_execute_record(root, action_id, record)
    return None


def consecutive_failed_execute_count(root: Path, action_id: str) -> int:
    history_path = root / "TestUtils" / "tester_daemon" / EXECUTE_FAILURES_FILE
    if not history_path.exists():
        return 0
    latest_success = latest_successful_execute(root, action_id)
    latest_success_at = (
        parse_utc_timestamp(str(latest_success.get("time", "") or ""))
        if latest_success
        else None
    )
    try:
        lines = history_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return 0
    count = 0
    for line in reversed(lines[-200:]):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict) or record.get("action_id") != action_id:
            continue
        failed_at = parse_utc_timestamp(str(record.get("time", "") or ""))
        if (
            latest_success_at is not None
            and failed_at is not None
            and failed_at <= latest_success_at
        ):
            break
        count += 1
    return count


def failed_execute_backoff(
    root: Path,
    action_id: str,
    retry_seconds: int,
    now: datetime | None = None,
    max_retry_seconds: int | None = None,
) -> dict[str, Any] | None:
    if retry_seconds <= 0:
        return None
    record = latest_failed_execute(root, action_id)
    if not record:
        return None
    failure_count = max(1, consecutive_failed_execute_count(root, action_id))
    effective_retry_seconds = int(retry_seconds)
    if failure_count > 1:
        effective_retry_seconds = retry_seconds * (2 ** min(failure_count - 1, 6))
    if max_retry_seconds and max_retry_seconds > 0:
        effective_retry_seconds = min(effective_retry_seconds, max_retry_seconds)
    failed_at = parse_utc_timestamp(str(record.get("time", "") or ""))
    if failed_at is None:
        return None
    now = now or datetime.now(timezone.utc)
    elapsed_seconds = max(0.0, (now - failed_at).total_seconds())
    if failed_at.microsecond == 0 and elapsed_seconds > 0:
        # Failure records are persisted with second precision. Without this
        # grace, a fresh 1s backoff can expire immediately at a second boundary.
        elapsed_seconds = max(0.0, elapsed_seconds - 0.999999)
    if elapsed_seconds >= effective_retry_seconds:
        return None
    age_seconds = int(elapsed_seconds)
    payload = dict(record)
    payload["age_seconds"] = age_seconds
    payload["failure_count"] = failure_count
    payload["retry_seconds"] = effective_retry_seconds
    payload["remaining_seconds"] = max(1, int(math.ceil(effective_retry_seconds - elapsed_seconds)))
    return payload


def failed_execute_backoff_is_delegated_to_specialized_policy(
    selected: GateDecision,
    backoff_record: dict[str, Any],
) -> bool:
    if selected.action not in {ActionKind.RECOVER_BLOCKED, ActionKind.HEARTBEAT_ACTIVE_REQUEST}:
        return False
    return (
        failed_execute_mentions_relay_stall(backoff_record)
        or failed_execute_mentions_gitpartner_origin_visibility_pending(backoff_record)
    )


def failed_execute_mentions_relay_stall(record: dict[str, Any]) -> bool:
    text = "\n".join(
        str(record.get(field, "") or "")
        for field in ("stdout_tail", "stderr_tail", "error", "reason", "failure_kind")
    )
    return "TEST_GITPARTNER_RELAY_STALLED" in text or "GITPARTNER_RELAY_STALLED" in text


def failed_execute_mentions_gitpartner_origin_visibility_pending(record: dict[str, Any]) -> bool:
    text = "\n".join(
        str(record.get(field, "") or "")
        for field in ("stdout_tail", "stderr_tail", "error", "reason", "failure_kind")
    ).lower()
    if "refusing --allow-unmatched-commit" in text or "current target worktree commit failed" in text:
        return False
    return "push succeeded but origin/main did not expose input/job.json" in text


def execute_log_details(root: Path, action_id: str) -> dict[str, Any]:
    digest = action_digest(action_id)
    logs = root / "TestUtils" / "tester_daemon" / "logs"
    stdout_path = logs / f"execute_child_{digest}.out.log"
    stderr_path = logs / f"execute_child_{digest}.err.log"
    details: dict[str, Any] = {
        "stdout": relpath(stdout_path, root),
        "stderr": relpath(stderr_path, root),
    }
    stdout_tail = tail_text(stdout_path)
    stderr_tail = tail_text(stderr_path)
    if stdout_tail:
        details["stdout_tail"] = stdout_tail
    if stderr_tail:
        details["stderr_tail"] = stderr_tail
    return details


def enrich_failed_execute_record(root: Path, action_id: str, record: dict[str, Any]) -> dict[str, Any]:
    if record.get("stdout_tail") or record.get("stderr_tail"):
        return record
    details = execute_log_details(root, action_id)
    if not details:
        return record
    enriched = dict(record)
    for key, value in details.items():
        if value and not enriched.get(key):
            enriched[key] = value
    return enriched


def tail_text(path: Path, limit: int = 2000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-limit:]


def parse_utc_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def process_creation_flags() -> int:
    if os.name != "nt":
        return 0
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
    return flags


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


def run_with_execute_heartbeat(
    root: Path,
    argv: list[str],
    action_id: str,
    lease: dict[str, Any] | None,
    interval_seconds: float = 1.0,
) -> int:
    logs = root / "TestUtils" / "tester_daemon" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    digest = action_digest(action_id)
    stdout_path = logs / f"execute_child_{digest}.out.log"
    stderr_path = logs / f"execute_child_{digest}.err.log"
    child_argv = no_window_harness_argv(argv)
    write_execute_heartbeat(root, action_id, lease, active=True, child_pid=None)
    with NamedProcessLock(
        root,
        "gitpartner_client_transport",
        stale_after_seconds=120,
        wait_timeout_seconds=300,
        on_wait=lambda _elapsed: write_execute_heartbeat(
            root,
            action_id,
            lease,
            active=True,
            child_pid=None,
        ),
    ):
        with stdout_path.open("a", encoding="utf-8") as out, stderr_path.open("a", encoding="utf-8") as err:
            proc = subprocess.Popen(
                child_argv,
                cwd=root,
                stdout=out,
                stderr=err,
                creationflags=process_creation_flags(),
                startupinfo=process_startupinfo(),
            )
            append_execute_worker_event(
                root,
                "execute_child_started",
                {
                    "action_id": action_id,
                    "child_pid": proc.pid,
                    "stdout": relpath(stdout_path, root),
                    "stderr": relpath(stderr_path, root),
                },
            )
            while True:
                returncode = proc.poll()
                if returncode is not None:
                    write_execute_heartbeat(root, action_id, lease, active=False, child_pid=proc.pid)
                    return returncode
                write_execute_heartbeat(root, action_id, lease, active=True, child_pid=proc.pid)
                time.sleep(interval_seconds)


def run_harness_inline(root: Path, argv: list[str], action_id: str) -> int:
    logs = root / "TestUtils" / "tester_daemon" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    digest = action_digest(action_id)
    stdout_path = logs / f"execute_child_{digest}.out.log"
    stderr_path = logs / f"execute_child_{digest}.err.log"
    append_execute_worker_event(
        root,
        "execute_inline_started",
        {
            "action_id": action_id,
            "argv": argv,
            "stdout": relpath(stdout_path, root),
            "stderr": relpath(stderr_path, root),
        },
    )
    old_argv = sys.argv[:]
    old_cwd = Path.cwd()
    root_text = str(root)
    inserted_path = False
    if root_text not in sys.path:
        sys.path.insert(0, root_text)
        inserted_path = True
    write_execute_heartbeat(root, action_id, lease=None, active=True, child_pid=None)
    returncode = 1
    try:
        with stdout_path.open("a", encoding="utf-8") as out, stderr_path.open("a", encoding="utf-8") as err:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                os.chdir(root)
                sys.argv = [str(root / "scripts" / "next_workflow.py"), *argv[2:]]
                module = importlib.import_module("scripts.next_workflow")
                try:
                    returncode = int(module.main())
                except SystemExit as exc:
                    if exc.code is None:
                        returncode = 0
                    elif isinstance(exc.code, int):
                        returncode = exc.code
                    else:
                        print(exc.code, file=sys.stderr)
                        returncode = 1
    finally:
        sys.argv = old_argv
        os.chdir(old_cwd)
        if inserted_path:
            try:
                sys.path.remove(root_text)
            except ValueError:
                pass
        write_execute_heartbeat(root, action_id, lease=None, active=False, child_pid=None)
    append_execute_worker_event(
        root,
        "execute_inline_finished",
        {
            "action_id": action_id,
            "returncode": returncode,
            "stdout": relpath(stdout_path, root),
            "stderr": relpath(stderr_path, root),
        },
    )
    return returncode


def no_window_harness_argv(argv: list[str]) -> list[str]:
    if not argv:
        return argv
    executable = Path(argv[0])
    name = executable.name.lower()
    if os.name == "nt" and name in {"python", "python.exe"}:
        return [background_python_executable(), *argv[1:]]
    return argv


def write_execute_heartbeat(
    root: Path,
    action_id: str,
    lease: dict[str, Any] | None,
    active: bool,
    child_pid: int | None = None,
) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    heartbeat_path = state_dir / "daemon_heartbeat.json"
    try:
        existing = json.loads(heartbeat_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        existing = {}
    payload = dict(existing) if isinstance(existing, dict) else {}
    coordinator_pid = read_lock_pid(state_dir / "daemon.lock")
    if coordinator_pid <= 0:
        coordinator_pid = int(payload.get("pid", 0) or 0)
    writer_pid = os.getpid()
    payload.update({
        "time": utc_now_iso(),
        "pid": coordinator_pid if coordinator_pid > 0 else writer_pid,
        "writer_pid": writer_pid,
        "child_pid": child_pid,
        "mode": "execute",
        "active_execute": active,
        "selected_action_id": action_id,
        "resource_lease_count": 1 if lease else 0,
        "action_liveness": {
            "selected_action_id": action_id,
            "repeat_count": 1,
            "age_seconds": 0,
            "stagnant": False,
        },
    })
    write_json_atomic(heartbeat_path, payload)
from ascendop_daemon.core.atomic_io import write_json_atomic
