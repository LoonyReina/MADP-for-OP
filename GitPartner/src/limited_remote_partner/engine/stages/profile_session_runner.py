from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

from limited_remote_partner.engine.batch_case_runner import MAX_BATCH_EXECUTIONS_PER_CASE, parse_cases
from limited_remote_partner.engine.test_engine import (
    atomic_write_json,
    hidden_process_creation_flags,
    hidden_process_startup_info,
    utc_now,
)


class ProfileSessionRunnerError(RuntimeError):
    pass


PopenFactory = Callable[..., subprocess.Popen[str]]


def run_profile_session(
    *,
    task_case: Path,
    case_range: str,
    output_path: Path,
    log_root: Path,
    timeout_per_case_seconds: int,
    expected_task_rows_per_case: int,
    popen_factory: PopenFactory = subprocess.Popen,
) -> dict[str, Any]:
    task_case = task_case.resolve()
    output_path = output_path.resolve()
    log_root = log_root.resolve()
    test_op = task_case / "test_op.py"
    if not test_op.is_file():
        raise ProfileSessionRunnerError(f"test_op.py is missing: {test_op}")
    if not 1 <= timeout_per_case_seconds <= 86400:
        raise ProfileSessionRunnerError("timeout_per_case_seconds must be within 1..86400")
    if not 1 <= expected_task_rows_per_case <= MAX_BATCH_EXECUTIONS_PER_CASE:
        raise ProfileSessionRunnerError(
            "expected_task_rows_per_case must be within "
            f"1..{MAX_BATCH_EXECUTIONS_PER_CASE}"
        )

    case_ids = parse_cases(case_range)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {
        "protocol_version": "engine-profile-session-v1",
        "execution_mode": "isolated-child-process",
        "case_range": case_range,
        "case_ids": case_ids,
        "case_count": len(case_ids),
        "expected_execution_count": len(case_ids),
        "expected_task_rows_per_case": expected_task_rows_per_case,
        "timeout_per_case_seconds": timeout_per_case_seconds,
        "parent_pid": os.getpid(),
        "started_at": utc_now(),
        "executions": records,
    }
    atomic_write_json(output_path, manifest)

    for sequence_index, case in enumerate(case_ids):
        log_path = log_root / f"case{case}.log"
        command = [sys.executable, str(test_op), str(case)]
        started = time.monotonic()
        started_at = utc_now()
        timed_out = False
        error = ""
        output = ""
        process: subprocess.Popen[str] | None = None
        try:
            process = popen_factory(
                command,
                cwd=str(task_case),
                env=os.environ.copy(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                creationflags=hidden_process_creation_flags(),
                startupinfo=hidden_process_startup_info(),
            )
            try:
                output, _unused = process.communicate(timeout=timeout_per_case_seconds)
            except subprocess.TimeoutExpired as exc:
                timed_out = True
                output = _timeout_output(exc)
                process.terminate()
                try:
                    tail, _unused = process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    tail, _unused = process.communicate(timeout=5)
                output += tail or ""
        except OSError as exc:
            error = f"{type(exc).__name__}: {exc}"
        returncode = int(process.returncode) if process and process.returncode is not None else 127
        log_path.write_text(output + (("\n" + error) if error else ""), encoding="utf-8")
        passed = (
            not timed_out
            and not error
            and returncode == 0
            and "verify result pass" in output
            and "verify result failed" not in output
            and "[ERROR]" not in output
        )
        record = {
            "case": case,
            "sequence_index": sequence_index,
            "process_instance": sequence_index + 1,
            "pid": int(process.pid) if process is not None else 0,
            "parent_pid": os.getpid(),
            "command": command,
            "started_at": started_at,
            "finished_at": utc_now(),
            "duration_seconds": round(max(0.0, time.monotonic() - started), 6),
            "returncode": returncode,
            "timed_out": timed_out,
            "verdict": "PASS" if passed else "FAIL",
            "log": str(log_path),
            "error": error or ("case timeout" if timed_out else ""),
        }
        records.append(record)
        atomic_write_json(output_path, {**manifest, "updated_at": utc_now()})

    failures = [item for item in records if item["verdict"] != "PASS"]
    manifest.update(
        {
            "finished_at": utc_now(),
            "state": "passed" if not failures else "failed",
            "execution_count": len(records),
            "pass_count": len(records) - len(failures),
            "fail_count": len(failures),
            "child_pids": [int(item["pid"]) for item in records],
        }
    )
    atomic_write_json(output_path, manifest)
    return manifest


def _timeout_output(exc: subprocess.TimeoutExpired) -> str:
    value = exc.output
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run performance cases as isolated children in one profiler session"
    )
    parser.add_argument("--task-case", type=Path, required=True)
    parser.add_argument("--case-range", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--log-root", type=Path, required=True)
    parser.add_argument("--timeout-per-case-seconds", type=int, default=330)
    parser.add_argument("--expected-task-rows-per-case", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_profile_session(
            task_case=args.task_case,
            case_range=args.case_range,
            output_path=args.output,
            log_root=args.log_root,
            timeout_per_case_seconds=args.timeout_per_case_seconds,
            expected_task_rows_per_case=args.expected_task_rows_per_case,
        )
    except (ProfileSessionRunnerError, OSError, ValueError) as exc:
        print(f"PROFILE_SESSION_RUNNER_ERROR: {exc}", file=sys.stderr)
        return 2
    return 0 if result.get("state") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
