from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from limited_remote_partner.engine.batch_case_runner import BatchCaseRunnerError
from limited_remote_partner.engine.stages.perf_pipeline import PerfPipelineError, parse_case_range
from limited_remote_partner.engine.test_engine import (
    atomic_write_json,
    hidden_process_creation_flags,
    hidden_process_startup_info,
    utc_now,
)


class CorrectnessPipelineError(RuntimeError):
    pass


Runner = Callable[..., subprocess.CompletedProcess[Any]]
MAX_CORRECTNESS_TIMEOUT_PER_EXECUTION_SECONDS = 86400
SHARED_CORRECTNESS_TIMEOUT_GRACE_SECONDS = 30


def run_correctness(
    *,
    task_case: Path,
    run_dir: Path,
    case_range: str,
    python_bin: str,
    repetitions: int = 1,
    timeout_seconds: int = 240,
    batch_process: bool = False,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    task_case = task_case.resolve()
    run_dir = run_dir.resolve()
    if not (task_case / "test_op.py").is_file():
        raise CorrectnessPipelineError(f"test_op.py is missing: {task_case}")
    if repetitions < 1:
        raise CorrectnessPipelineError("correctness repetitions must be positive")
    if not 1 <= int(timeout_seconds) <= MAX_CORRECTNESS_TIMEOUT_PER_EXECUTION_SECONDS:
        raise CorrectnessPipelineError(
            "timeout_seconds must be within 1.."
            f"{MAX_CORRECTNESS_TIMEOUT_PER_EXECUTION_SECONDS}"
        )
    case_ids = parse_case_range(case_range)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_root = run_dir / "correctness"
    ensure_under(log_root, run_dir)
    log_root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    manifest_path = run_dir / "CORRECTNESS.json"
    manifest: dict[str, Any] = {
        "protocol_version": "engine-correctness-v1",
        "stage": "correctness",
        "execution_mode": "batched-process" if batch_process else "isolated-process",
        "case_range": case_range,
        "case_ids": case_ids,
        "case_count": len(case_ids),
        "repetitions": repetitions,
        "expected_execution_count": len(case_ids) * repetitions,
        "started_at": utc_now(),
        "executions": records,
    }

    if batch_process:
        return run_batched_correctness(
            task_case=task_case,
            run_dir=run_dir,
            case_range=case_range,
            repetitions=repetitions,
            manifest=manifest,
            manifest_path=manifest_path,
            python_bin=python_bin,
            timeout_seconds=timeout_seconds,
            runner=runner,
        )

    for case in case_ids:
        case_root = log_root / f"case{case}"
        case_root.mkdir(parents=True, exist_ok=True)
        for repetition in range(1, repetitions + 1):
            command = [python_bin, "test_op.py", str(case)]
            log_path = case_root / f"repeat{repetition}.log"
            started_at = utc_now()
            started = time.monotonic()
            timed_out = False
            try:
                completed = runner(
                    command,
                    cwd=task_case,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=max(1, int(timeout_seconds)),
                    check=False,
                    creationflags=hidden_process_creation_flags(),
                    startupinfo=hidden_process_startup_info(),
                )
                returncode = int(completed.returncode)
                output = str(completed.stdout or "")
            except subprocess.TimeoutExpired as exc:
                timed_out = True
                returncode = 124
                output = timeout_output(exc)
            duration = max(0.0, time.monotonic() - started)
            log_path.write_text(output, encoding="utf-8")
            passed = returncode == 0 and "verify result pass" in output
            record = {
                "case": case,
                "repetition": repetition,
                "started_at": started_at,
                "finished_at": utc_now(),
                "duration_seconds": round(duration, 6),
                "returncode": returncode,
                "timed_out": timed_out,
                "verdict": "PASS" if passed else "FAIL",
                "log": str(log_path),
            }
            records.append(record)
            atomic_write_json(manifest_path, {**manifest, "updated_at": utc_now()})

    failures = [item for item in records if item["verdict"] != "PASS"]
    case_count = len(case_ids)
    summary = [
        (
            f"AscendOP correctness cases {case_range}: "
            f"executions={len(records)} PASS={len(records) - len(failures)} "
            f"FAIL={len(failures)} repetitions={repetitions}"
        )
    ]
    for case in case_ids:
        case_records = [item for item in records if item["case"] == case]
        case_failures = [item for item in case_records if item["verdict"] != "PASS"]
        summary.append(
            f"  case{case}: PASS={len(case_records) - len(case_failures)} "
            f"FAIL={len(case_failures)}"
        )
    summary_path = run_dir / "CORRECTNESS_SUMMARY.txt"
    summary_path.write_text("\n".join(summary) + "\n", encoding="utf-8")
    manifest.update(
        {
            "finished_at": utc_now(),
            "state": "passed" if not failures else "failed",
            "case_count": case_count,
            "expected_execution_count": case_count * repetitions,
            "execution_count": len(records),
            "pass_count": len(records) - len(failures),
            "fail_count": len(failures),
            "summary": str(summary_path),
        }
    )
    atomic_write_json(manifest_path, manifest)
    if failures:
        first = failures[0]
        raise CorrectnessPipelineError(
            "correctness failed: "
            f"case={first['case']} repetition={first['repetition']} "
            f"rc={first['returncode']}"
        )
    return manifest


def run_batched_correctness(
    *,
    task_case: Path,
    run_dir: Path,
    case_range: str,
    repetitions: int,
    manifest: dict[str, Any],
    manifest_path: Path,
    python_bin: str,
    timeout_seconds: int,
    runner: Runner,
) -> dict[str, Any]:
    batch_path = run_dir / "CORRECTNESS_BATCH.json"
    case_ids = parse_case_range(case_range)
    expected_execution_count = len(case_ids) * repetitions
    batch_timeout_seconds = shared_correctness_timeout_seconds(
        timeout_seconds, expected_execution_count
    )
    runner_log_path = run_dir / "correctness_batch_runner.log"
    command = [
        python_bin,
        "-m",
        "limited_remote_partner.engine.batch_case_runner",
        "--task-case",
        str(task_case),
        "--case-range",
        case_range,
        "--repetitions",
        str(repetitions),
        "--output",
        str(batch_path),
        "--log-root",
        str(run_dir / "correctness"),
        "--mode",
        "correctness",
    ]
    runner_timed_out = False
    runner_returncode = 127
    runner_output = ""
    runner_env = os.environ.copy()
    package_root = str(Path(__file__).resolve().parents[1])
    existing_pythonpath = runner_env.get("PYTHONPATH", "")
    runner_env["PYTHONPATH"] = os.pathsep.join(
        item for item in (package_root, existing_pythonpath) if item
    )
    try:
        completed = runner(
            command,
            cwd=task_case,
            env=runner_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=batch_timeout_seconds,
            check=False,
            creationflags=hidden_process_creation_flags(),
            startupinfo=hidden_process_startup_info(),
        )
        runner_returncode = int(completed.returncode)
        runner_output = str(completed.stdout or "")
    except subprocess.TimeoutExpired as exc:
        runner_timed_out = True
        runner_returncode = 124
        runner_output = timeout_output(exc)
    runner_log_path.write_text(runner_output, encoding="utf-8")
    batch = read_batch_manifest(batch_path)
    records = [dict(item) for item in batch.get("executions", []) if isinstance(item, dict)]
    record_failures = [item for item in records if item.get("verdict") != "PASS"]
    validation_failures: list[str] = []
    case_count = len(case_ids)
    if runner_timed_out:
        validation_failures.append(
            f"batch runner timed out after {batch_timeout_seconds}s"
        )
    if batch.get("protocol_version") != "engine-batch-case-v1":
        validation_failures.append("batch manifest protocol is missing or invalid")
    if str(batch.get("mode") or "") != "correctness":
        validation_failures.append("batch manifest mode is not correctness")
    if list(batch.get("case_ids") or []) != case_ids:
        validation_failures.append("batch manifest case ids do not match the contract")
    if int(batch.get("repetitions", 0) or 0) != repetitions:
        validation_failures.append("batch manifest repetitions do not match the contract")
    if len(records) != expected_execution_count:
        validation_failures.append(
            "execution count mismatch: "
            f"expected={expected_execution_count} observed={len(records)}"
        )
    expected_grid = [
        (case, repetition)
        for case in case_ids
        for repetition in range(1, repetitions + 1)
    ]
    observed_grid = [
        (int(item.get("case", 0) or 0), int(item.get("repetition", 0) or 0))
        for item in records
    ]
    if observed_grid != expected_grid:
        validation_failures.append("batch execution grid is incomplete or out of order")
    if runner_returncode not in {0, 1}:
        validation_failures.append(f"batch runner returned {runner_returncode}")
    elif runner_returncode == 1 and not record_failures:
        validation_failures.append(
            "batch runner returned 1 without a failed case record"
        )
    elif runner_returncode == 0 and record_failures:
        validation_failures.append(
            "batch runner returned 0 despite failed case records"
        )
    failures = len(record_failures) + len(validation_failures)
    pass_count = sum(item.get("verdict") == "PASS" for item in records)
    summary = [
        (
            f"AscendOP correctness cases {case_range}: "
            f"executions={len(records)} PASS={pass_count} "
            f"FAIL={failures} repetitions={repetitions} mode=batched-process"
        )
    ]
    for case in case_ids:
        case_records = [item for item in records if int(item.get("case", 0) or 0) == case]
        case_failures = [item for item in case_records if item.get("verdict") != "PASS"]
        summary.append(
            f"  case{case}: PASS={len(case_records) - len(case_failures)} "
            f"FAIL={len(case_failures)}"
        )
    summary_path = run_dir / "CORRECTNESS_SUMMARY.txt"
    summary_path.write_text("\n".join(summary) + "\n", encoding="utf-8")
    manifest.update(
        {
            "finished_at": str(batch.get("finished_at") or utc_now()),
            "state": "passed" if failures == 0 else "failed",
            "case_count": case_count,
            "expected_execution_count": expected_execution_count,
            "execution_count": len(records),
            "pass_count": pass_count,
            "fail_count": failures,
            "executions": records,
            "summary": str(summary_path),
            "batch_manifest": str(batch_path),
            "batch_runner_command": command,
            "batch_runner_returncode": runner_returncode,
            "batch_runner_timed_out": runner_timed_out,
            "batch_runner_log": str(runner_log_path),
            "timeout_per_execution_seconds": int(timeout_seconds),
            "batch_timeout_seconds": batch_timeout_seconds,
            "validation_failures": validation_failures,
        }
    )
    atomic_write_json(manifest_path, manifest)
    if failures:
        first = record_failures[0] if record_failures else {}
        raise CorrectnessPipelineError(
            "batched correctness failed: "
            f"case={first.get('case', 0)} repetition={first.get('repetition', 0)} "
            f"reason={validation_failures[0] if validation_failures else 'case failure'}"
        )
    return manifest


def derive_correctness_from_performance(
    *,
    run_dir: Path,
    case_range: str,
    repetitions: int = 1,
) -> dict[str, Any]:
    """Promote the profiled batch's real verification into correctness evidence.

    The batched performance application executes the same TestCustomOP method as
    the standalone correctness runner.  This path removes a second Python/NPU
    startup while retaining one real output-vs-golden verification per case.
    """

    run_dir = run_dir.resolve()
    if repetitions != 1:
        raise CorrectnessPipelineError(
            "profiled correctness evidence supports exactly one outer repetition"
        )
    case_ids = parse_case_range(case_range)
    performance_path = run_dir / "PERF_BATCH.json"
    performance = read_batch_manifest(performance_path)
    raw_records = [
        dict(item)
        for item in performance.get("executions", [])
        if isinstance(item, dict)
    ]
    validation_failures: list[str] = []
    if performance.get("protocol_version") != "engine-batch-case-v1":
        validation_failures.append("performance batch protocol is missing or invalid")
    if str(performance.get("mode") or "") != "performance":
        validation_failures.append("source batch mode is not performance")
    if str(performance.get("state") or "") != "passed":
        validation_failures.append("source performance batch did not pass")
    if list(performance.get("case_ids") or []) != case_ids:
        validation_failures.append("source performance case ids do not match contract")
    if int(performance.get("repetitions", 0) or 0) != repetitions:
        validation_failures.append("source performance repetitions do not match contract")
    expected_grid = [(case, 1) for case in case_ids]
    observed_grid = [
        (int(item.get("case", 0) or 0), int(item.get("repetition", 0) or 0))
        for item in raw_records
    ]
    if observed_grid != expected_grid:
        validation_failures.append("source performance execution grid is incomplete")

    records: list[dict[str, Any]] = []
    for raw in raw_records:
        record = dict(raw)
        log_path = Path(str(record.get("log") or "")).resolve()
        try:
            ensure_under(log_path, run_dir)
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
        except (OSError, CorrectnessPipelineError):
            log_text = ""
            validation_failures.append(
                f"profiled correctness log is missing or unsafe: case={record.get('case')}"
            )
        if (
            record.get("verdict") != "PASS"
            or "verify result pass" not in log_text
            or "verify result failed" in log_text
            or "[ERROR]" in log_text
        ):
            validation_failures.append(
                f"profiled correctness verification failed: case={record.get('case')}"
            )
            record["verdict"] = "FAIL"
        record["evidence_source"] = "profiled-performance-batch"
        records.append(record)

    fail_count = sum(item.get("verdict") != "PASS" for item in records)
    fail_count += len(validation_failures)
    pass_count = sum(item.get("verdict") == "PASS" for item in records)
    finished_at = str(performance.get("finished_at") or utc_now())
    batch = {
        **performance,
        "mode": "correctness",
        "state": "passed" if fail_count == 0 else "failed",
        "source_mode": "performance",
        "evidence_source": "profiled-performance-batch",
        "source_batch_manifest": str(performance_path),
        "executions": records,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "validation_failures": validation_failures,
    }
    batch_path = run_dir / "CORRECTNESS_BATCH.json"
    atomic_write_json(batch_path, batch)

    summary_lines = [
        (
            f"AscendOP correctness cases {case_range}: executions={len(records)} "
            f"PASS={pass_count} FAIL={fail_count} repetitions=1 "
            "mode=profiled-performance-batch"
        )
    ]
    for case in case_ids:
        item = next(
            (record for record in records if int(record.get("case", 0) or 0) == case),
            {},
        )
        summary_lines.append(
            f"  case{case}: PASS={1 if item.get('verdict') == 'PASS' else 0} "
            f"FAIL={0 if item.get('verdict') == 'PASS' else 1}"
        )
    summary_path = run_dir / "CORRECTNESS_SUMMARY.txt"
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    manifest = {
        "protocol_version": "engine-correctness-v1",
        "stage": "correctness-evidence",
        "execution_mode": "profiled-performance-batch",
        "evidence_source": "profiled-performance-batch",
        "source_batch_manifest": str(performance_path),
        "batch_manifest": str(batch_path),
        "case_range": case_range,
        "case_ids": case_ids,
        "case_count": len(case_ids),
        "repetitions": 1,
        "expected_execution_count": len(case_ids),
        "execution_count": len(records),
        "pass_count": pass_count,
        "fail_count": fail_count,
        "executions": records,
        "validation_failures": validation_failures,
        "started_at": str(performance.get("started_at") or ""),
        "finished_at": finished_at,
        "summary": str(summary_path),
        "state": "passed" if fail_count == 0 else "failed",
    }
    atomic_write_json(run_dir / "CORRECTNESS.json", manifest)
    if fail_count:
        raise CorrectnessPipelineError(
            "profiled correctness evidence failed: "
            + (validation_failures[0] if validation_failures else "case failure")
        )
    return manifest


def shared_correctness_timeout_seconds(
    timeout_per_execution_seconds: int, execution_count: int
) -> int:
    if timeout_per_execution_seconds < 1 or execution_count < 1:
        raise CorrectnessPipelineError(
            "shared correctness timeout requires positive inputs"
        )
    return (
        int(timeout_per_execution_seconds) * int(execution_count)
        + SHARED_CORRECTNESS_TIMEOUT_GRACE_SECONDS
    )


def read_batch_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def timeout_output(exc: subprocess.TimeoutExpired) -> str:
    output = exc.stdout or ""
    if isinstance(output, bytes):
        output = output.decode("utf-8", errors="replace")
    return f"{output}\n[engine] correctness execution timed out\n"


def ensure_under(path: Path, root: Path) -> None:
    path = path.resolve()
    root = root.resolve()
    if path != root and root not in path.parents:
        raise CorrectnessPipelineError(f"path escapes run directory: {path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AscendOP split correctness runner")
    parser.add_argument("--task-case", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--case-range", required=True)
    parser.add_argument("--python-bin", required=True)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    parser.add_argument("--batch-process", action="store_true")
    parser.add_argument("--from-performance-batch", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.from_performance_batch:
            if args.batch_process:
                raise CorrectnessPipelineError(
                    "--batch-process and --from-performance-batch are mutually exclusive"
                )
            derive_correctness_from_performance(
                run_dir=Path(args.run_dir),
                case_range=args.case_range,
                repetitions=args.repetitions,
            )
        else:
            run_correctness(
                task_case=Path(args.task_case),
                run_dir=Path(args.run_dir),
                case_range=args.case_range,
                python_bin=args.python_bin,
                repetitions=args.repetitions,
                timeout_seconds=args.timeout_seconds,
                batch_process=args.batch_process,
            )
    except (
        BatchCaseRunnerError,
        CorrectnessPipelineError,
        PerfPipelineError,
        OSError,
        ValueError,
    ) as exc:
        print(f"CORRECTNESS_PIPELINE_ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
