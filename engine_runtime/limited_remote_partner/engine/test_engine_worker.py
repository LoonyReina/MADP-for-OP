from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from limited_remote_partner.resources.shared_resource_lease import (
    SharedResourceLeaseSet,
    is_shared_resource,
    shared_lease_root_for_engine,
)

class EngineWorkerError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def read_boot_id() -> str:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="ascii"
        ).strip()
    except OSError:
        value = ""
    return value or f"{socket.gethostname()}-unknown"


def read_json(path: Path) -> dict[str, Any]:
    for attempt in range(50):
        try:
            text = path.read_text(encoding="utf-8-sig")
            break
        except PermissionError:
            if attempt >= 49:
                raise
            time.sleep(0.01)
    raw = json.loads(text)
    if not isinstance(raw, dict):
        raise EngineWorkerError(f"JSON document must be an object: {path}")
    return raw


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for attempt in range(50):
        try:
            os.replace(temp, path)
            return
        except PermissionError:
            if attempt >= 49:
                temp.unlink(missing_ok=True)
                raise
            time.sleep(0.01)


def hidden_process_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))


def hidden_process_startup_info() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    info = subprocess.STARTUPINFO()
    info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    info.wShowWindow = 0
    return info


def terminate_command_process(
    process: subprocess.Popen[Any],
    *,
    grace_seconds: float = 5.0,
) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.terminate()
        else:
            os.killpg(process.pid, signal.SIGTERM)
    except (OSError, ProcessLookupError):
        pass
    try:
        process.wait(timeout=max(0.1, grace_seconds))
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        if os.name == "nt":
            process.kill()
        else:
            os.killpg(process.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        pass
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        pass


def run_stage(job_dir: Path, stage_index: int) -> int:
    job_dir = job_dir.resolve()
    spec = read_json(job_dir / "spec.json")
    stages = spec.get("stages", [])
    if stage_index < 0 or stage_index >= len(stages):
        raise EngineWorkerError(f"stage index out of range: {stage_index}")
    stage = stages[stage_index]
    working_dir = safe_join(job_dir, str(stage.get("working_dir", ".")))
    working_dir.mkdir(parents=True, exist_ok=True)
    command = list(stage.get("command", []))
    started_at = utc_now()
    started_monotonic_ns = time.monotonic_ns()
    exit_code = 0
    error = ""
    timed_out = False
    stage_timeout_seconds = int(stage.get("timeout_seconds", 0) or 0)
    effective_timeout_seconds = float(stage_timeout_seconds)
    deadline_limited = False
    deadline_at_epoch_seconds = float(
        os.environ.get("ASCENDOP_ENGINE_JOB_DEADLINE_AT_EPOCH_SECONDS", "0") or 0
    )
    shared_lease_wait_seconds = 0.0
    if command:
        env = os.environ.copy()
        env.update({str(key): str(value) for key, value in stage.get("env", {}).items()})
        env.update(
            {
                "ASCENDOP_ENGINE_REQUEST_ID": str(spec["request_id"]),
                "ASCENDOP_ENGINE_JOB_ID": str(spec["engine_job_id"]),
                "ASCENDOP_ENGINE_ATTEMPT_ID": str(spec["attempt_id"]),
                "ASCENDOP_ENGINE_STAGE": str(stage["name"]),
                "ASCENDOP_ENGINE_EXECUTION_PROFILE": str(
                    spec.get("execution_profile") or ""
                ),
                "ASCENDOP_ENGINE_JOB_ROOT": str(job_dir),
                "ASCENDOP_ENGINE_ROOT": str(job_dir.parent.parent),
                "ASCENDOP_ENGINE_CACHE_ROOT": (
                    env.get("ASCENDOP_ENGINE_CACHE_ROOT", "").strip()
                    or str(job_dir.parent.parent / "cache")
                ),
                "ASCENDOP_ENGINE_PAYLOAD_ROOT": str(job_dir / "payload"),
                "ASCENDOP_ENGINE_RESULT_ROOT": str(job_dir / "result_bundle"),
            }
        )
        try:
            runtime_locks_raw = env.get("ASCENDOP_ENGINE_STAGE_LOCKS_JSON", "")
            runtime_locks = (
                json.loads(runtime_locks_raw)
                if runtime_locks_raw
                else list(stage.get("locks", []))
            )
            if not isinstance(runtime_locks, list):
                raise EngineWorkerError("runtime stage locks must be a JSON list")
            shared_resources = [
                str(item) for item in runtime_locks if is_shared_resource(str(item))
            ]
            timeout_seconds = float(
                env.get("ASCENDOP_SHARED_LEASE_TIMEOUT_SECONDS", "1800")
            )
            if deadline_at_epoch_seconds > 0:
                timeout_seconds = min(
                    timeout_seconds,
                    max(0.001, deadline_at_epoch_seconds - time.time()),
                )
            lease = SharedResourceLeaseSet(
                shared_lease_root_for_engine(job_dir.parent.parent),
                shared_resources,
                holder_kind="engine-stage",
                request_id=str(spec["request_id"]),
                engine_job_id=str(spec["engine_job_id"]),
                attempt_id=str(spec["attempt_id"]),
                stage=str(stage["name"]),
                timeout_seconds=timeout_seconds,
            )
            with lease:
                shared_lease_wait_seconds = lease.wait_seconds
                remaining_job_seconds = (
                    deadline_at_epoch_seconds - time.time()
                    if deadline_at_epoch_seconds > 0
                    else 0.0
                )
                if deadline_at_epoch_seconds > 0 and remaining_job_seconds <= 0:
                    timed_out = True
                    deadline_limited = True
                    exit_code = 124
                    error = "job execution deadline exhausted before stage command"
                    raise EngineWorkerError(error)
                if deadline_at_epoch_seconds > 0 and (
                    stage_timeout_seconds <= 0
                    or remaining_job_seconds < stage_timeout_seconds
                ):
                    effective_timeout_seconds = max(0.001, remaining_job_seconds)
                    deadline_limited = True
                log_root = job_dir / "logs"
                log_root.mkdir(parents=True, exist_ok=True)
                stdout_path = log_root / f"{stage_index:03d}_{stage['name']}.out.log"
                stderr_path = log_root / f"{stage_index:03d}_{stage['name']}.err.log"
                # Close and flush command-owned log handles before publishing the
                # stage result, so the resident engine never bundles empty logs.
                with stdout_path.open("a", encoding="utf-8") as stdout, stderr_path.open(
                    "a", encoding="utf-8"
                ) as stderr:
                    process = subprocess.Popen(
                        command,
                        cwd=str(working_dir),
                        env=env,
                        stdout=stdout,
                        stderr=stderr,
                        creationflags=hidden_process_creation_flags(),
                        startupinfo=hidden_process_startup_info(),
                        start_new_session=os.name != "nt",
                    )
                    try:
                        exit_code = int(
                            process.wait(
                                timeout=(
                                    effective_timeout_seconds
                                    if effective_timeout_seconds > 0
                                    else None
                                )
                            )
                        )
                    except subprocess.TimeoutExpired:
                        timed_out = True
                        exit_code = 124
                        error = (
                            (
                                "job execution deadline exhausted during stage after "
                                if deadline_limited
                                else "stage command timed out after "
                            )
                            + f"{effective_timeout_seconds:g} seconds"
                        )
                        terminate_command_process(process)
        except Exception as exc:
            if timed_out or (
                deadline_at_epoch_seconds > 0
                and time.time() >= deadline_at_epoch_seconds
            ):
                timed_out = True
                deadline_limited = True
                exit_code = 124
                error = error or "job execution deadline exhausted during stage"
            else:
                exit_code = 1
                error = str(exc)
    finished_monotonic_ns = time.monotonic_ns()
    finished_at = utc_now()
    result = {
        "request_id": spec["request_id"],
        "engine_job_id": spec["engine_job_id"],
        "attempt_id": spec["attempt_id"],
        "stage_index": stage_index,
        "stage_name": stage["name"],
        "stage_resource": stage["resource"],
        "device_id": os.environ.get("ASCENDOP_ENGINE_DEVICE_ID", ""),
        "runtime_stage_locks": json.loads(
            os.environ.get("ASCENDOP_ENGINE_STAGE_LOCKS_JSON", "[]")
        ),
        "started_at": started_at,
        "finished_at": finished_at,
        "host": socket.gethostname(),
        "boot_id": read_boot_id(),
        "pid": os.getpid(),
        "started_monotonic_ns": started_monotonic_ns,
        "finished_monotonic_ns": finished_monotonic_ns,
        "duration_ns": max(0, finished_monotonic_ns - started_monotonic_ns),
        "exit_code": exit_code,
        "timeout_seconds": stage_timeout_seconds,
        "effective_timeout_seconds": round(effective_timeout_seconds, 6),
        "timed_out": timed_out,
        "deadline_limited": deadline_limited,
        "shared_lease_wait_seconds": round(shared_lease_wait_seconds, 6),
        "command": [str(item) for item in command],
    }
    if error:
        result["error"] = error
    atomic_write_json(job_dir / "stage_results" / f"{stage_index:03d}.json", result)
    return exit_code


def safe_join(root: Path, relative: str) -> Path:
    target = (root / relative).resolve()
    if target != root and root not in target.parents:
        raise EngineWorkerError(f"working directory escapes job root: {relative}")
    return target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one durable test-engine stage")
    parser.add_argument("--job-dir", type=Path, required=True)
    parser.add_argument("--stage-index", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run_stage(args.job_dir, args.stage_index)
    except Exception as exc:
        print(f"test-engine worker failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
