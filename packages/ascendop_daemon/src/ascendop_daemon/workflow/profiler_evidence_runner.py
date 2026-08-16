from __future__ import annotations

import argparse
import csv
import importlib
import itertools
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

attribute_case_rows = importlib.import_module(f"{__package__ + '.' if __package__ else ''}profiler_row_attribution").attribute_case_rows

MAX_MSPROF_LAUNCH_COUNT = 5000
MAX_CSV_ROWS = 5000
MAX_CSV_FILES = 512
MAX_PROFILE_FILES = 200
MAX_VISUALIZE_FILES = 64
PROFILE_SCAN_DEADLINE_SECONDS = 20
EFFECTIVE_PROFILE_ROUNDS = 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def read_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"JSON document must be an object: {path}")
    return raw


def write_object(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def safe_token(value: str, field: str) -> str:
    if not value or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for char in value):
        raise RuntimeError(f"unsafe {field}: {value!r}")
    return value


def subprocess_launch_kwargs() -> dict[str, Any]:
    if os.name != "nt":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return {
        "creationflags": int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
        "startupinfo": startupinfo,
    }


def command_help(msprof: str) -> tuple[str, int]:
    completed = subprocess.run(
        [msprof, "op", "--help"],
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=30,
        **subprocess_launch_kwargs(),
    )
    return completed.stdout or "", int(completed.returncode)


def supported_option(help_text: str, option: str) -> bool:
    return option in help_text


TEST_OP_BOOTSTRAP_SOURCE = (
    "import os\n"
    "import runpy\n"
    "import sys\n"
    "\n"
    "os.environ['TORCH_DEVICE_BACKEND_AUTOLOAD'] = '0'\n"
    "os.environ['ROUND'] = '1'\n"
    "import torch\n"
    "import torch_npu\n"
    "target = sys.argv[1]\n"
    "if target == '--batch-runner':\n"
    "    sys.argv = [sys.argv[0], *sys.argv[2:]]\n"
    "    runpy.run_module('limited_remote_partner.engine.batch_case_runner', run_name='__main__')\n"
    "else:\n"
    "    sys.argv = sys.argv[1:]\n"
    "    runpy.run_path(target, run_name='__main__')\n"
)


def build_test_op_command(
    python_bin: str,
    bootstrap_path: Path,
    test_op: Path,
    case_id: int,
) -> list[str]:
    return [
        "env",
        "TORCH_DEVICE_BACKEND_AUTOLOAD=0",
        "ROUND=1",
        python_bin,
        str(bootstrap_path),
        str(test_op),
        str(case_id),
    ]


def _descendant_pids(root_pid: int) -> list[int]:
    if os.name == "nt":
        return []
    children: dict[int, list[int]] = {}
    for status_path in Path("/proc").glob("[0-9]*/status"):
        try:
            pid = int(status_path.parent.name)
            parent_pid = next(
                int(line.split(":", 1)[1].strip())
                for line in status_path.read_text(
                    encoding="utf-8", errors="replace"
                ).splitlines()
                if line.startswith("PPid:")
            )
        except (OSError, StopIteration, ValueError):
            continue
        children.setdefault(parent_pid, []).append(pid)
    descendants: list[int] = []
    pending = list(children.get(root_pid, []))
    while pending:
        pid = pending.pop()
        descendants.append(pid)
        pending.extend(children.get(pid, []))
    return descendants


def _signal_process_tree(process: subprocess.Popen[Any], sig: int) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            **subprocess_launch_kwargs(),
        )
        return
    pids = [*reversed(_descendant_pids(process.pid)), process.pid]
    for pid in pids:
        try:
            os.kill(pid, sig)
        except (OSError, ProcessLookupError):
            continue


def run_process(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    started = time.monotonic()
    started_at = utc_now_iso()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timed_out = False
    with log_path.open("w", encoding="utf-8", errors="replace") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=log_file,
            stderr=subprocess.STDOUT,
            **subprocess_launch_kwargs(),
        )
        try:
            proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            _signal_process_tree(proc, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                _signal_process_tree(proc, signal.SIGKILL)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # The caller must still return before the enclosing device
                    # session deadline even if an OS process cannot be reaped.
                    pass
    returncode = proc.returncode
    if returncode is None:
        returncode = -9
    elif timed_out and returncode == 0:
        returncode = 124
    return {
        "command": shlex.join(command),
        "returncode": int(returncode),
        "timed_out": timed_out,
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "duration_seconds": round(time.monotonic() - started, 6),
        "log_path": str(log_path),
    }


def find_profile_root(output_root: Path) -> Path | None:
    values = sorted(
        itertools.islice(
            (path for path in output_root.rglob("OPPROF_*") if path.is_dir()),
            MAX_CSV_FILES,
        ),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    return values[0] if values else None


def csv_evidence(
    profile_root: Path,
    op: str,
    case_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    expected_cases = list(case_ids or [])
    started = time.monotonic()
    paths = list(itertools.islice(profile_root.rglob("*.csv"), MAX_CSV_FILES + 1))
    scan_truncated = len(paths) > MAX_CSV_FILES
    for path in sorted(paths[:MAX_CSV_FILES]):
        if time.monotonic() - started > PROFILE_SCAN_DEADLINE_SECONDS:
            evidence.append(
                {
                    "path": "",
                    "scan_deadline_exceeded": True,
                    "matched_operator_rows": 0,
                    "rows": [],
                }
            )
            break
        try:
            with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
                reader = csv.DictReader(handle)
                observed = [
                    dict(row)
                    for row in itertools.islice(reader, MAX_CSV_ROWS + 1)
                ]
        except (OSError, csv.Error):
            continue
        truncated = len(observed) > MAX_CSV_ROWS
        rows = observed[:MAX_CSV_ROWS]
        matched = [
            row
            for row in rows
            if op.lower() in " ".join(str(value) for value in row.values()).lower()
        ]
        selected = matched or rows[:20]
        compact_rows = []
        for row in selected:
            compact = {
                key: value
                for key, value in row.items()
                if value not in (None, "")
                and any(
                    marker in key.lower()
                    for marker in (
                        "name",
                        "time",
                        "duration",
                        "ratio",
                        "block",
                        "core",
                        "mte",
                        "vec",
                        "scalar",
                        "stall",
                        "sync",
                        "bandwidth",
                        "occup",
                    )
                )
            }
            if compact:
                compact_rows.append(compact)
        record = {
            "path": str(path.relative_to(profile_root.parent)).replace("\\", "/"),
            "row_count_observed": len(rows),
            "row_limit": MAX_CSV_ROWS,
            "rows_truncated": truncated,
            "matched_operator_rows": len(matched),
            "rows": compact_rows,
        }
        if expected_cases and len(matched) == len(expected_cases):
            record["case_rows"] = [
                {
                    "case_id": case_id,
                    "sequence_index": index,
                    "row": compact_rows[index] if index < len(compact_rows) else {},
                }
                for index, case_id in enumerate(expected_cases)
            ]
        evidence.append(record)
    if scan_truncated:
        evidence.append(
            {
                "path": "",
                "scan_truncated": True,
                "csv_file_limit": MAX_CSV_FILES,
                "matched_operator_rows": 0,
                "rows": [],
            }
        )
    return evidence


def build_batch_command(
    python_bin: str,
    bootstrap_path: Path,
    task_case: Path,
    case_ids: list[int],
    manifest_path: Path,
    log_root: Path,
) -> list[str]:
    return [
        "env",
        "TORCH_DEVICE_BACKEND_AUTOLOAD=0",
        "ROUND=1",
        python_bin,
        str(bootstrap_path),
        "--batch-runner",
        "--task-case",
        str(task_case),
        "--case-range",
        ",".join(str(case_id) for case_id in case_ids),
        "--repetitions",
        "1",
        "--output",
        str(manifest_path),
        "--log-root",
        str(log_root),
        "--mode",
        "performance",
        "--expected-task-rows-per-case",
        "1",
        "--effective-profile-rounds",
        str(EFFECTIVE_PROFILE_ROUNDS),
    ]


_SKIPPED_KERNEL_RE = re.compile(r"Kernel ([A-Za-z0-9_]+) skipped:")


def selector_diagnostics(log_path: Path, requested: str) -> dict[str, Any]:
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        text = ""
    observed: dict[str, int] = {}
    for match in _SKIPPED_KERNEL_RE.finditer(text):
        name = match.group(1)
        observed[name] = observed.get(name, 0) + 1
    ranked = sorted(observed.items(), key=lambda item: (-item[1], item[0]))[:20]
    return {
        "requested_kernel_name": requested,
        "no_profiling_data_dumped": "No profiling data dumped" in text,
        "observed_skipped_kernel_count": sum(observed.values()),
        "observed_skipped_kernel_names": [
            {"name": name, "count": count} for name, count in ranked
        ],
    }


def _zero_only_pipe_rows(parsed_csv: list[dict[str, Any]]) -> bool:
    rows: list[dict[str, Any]] = []
    for record in parsed_csv:
        if "pipeutilization" not in str(record.get("path") or "").lower():
            continue
        rows.extend(
            value
            for value in record.get("rows", [])
            if isinstance(value, dict)
        )
    if not rows:
        return False
    ignored = {"block_id", "sub_block_id", "name", "op name"}
    for row in rows:
        for key, value in row.items():
            if str(key).strip().lower() in ignored:
                continue
            try:
                if float(str(value)) != 0.0:
                    return False
            except (TypeError, ValueError):
                continue
    return True


def _pipe_quality_failure(
    metric_label: str,
    zero_only_pipe_rows: bool,
    *,
    allow_zero_only_pipe_rows: bool,
) -> str:
    if not zero_only_pipe_rows or allow_zero_only_pipe_rows:
        return ""
    return f"profiler-{metric_label}-pipe-zero-only"


def profile_batch(
    *,
    msprof: str,
    help_text: str,
    task_case: Path,
    run_dir: Path,
    python_bin: str,
    kernel_name: str,
    kernel_selection: str,
    target_version: str,
    case_ids: list[int],
    metric_label: str,
    metrics: str,
    timeout_seconds: int,
    env: dict[str, str],
    bootstrap_path: Path,
    repetition: int = 1,
    case_shapes: dict[str, Any] | None = None,
    expected_block_dims: dict[str, Any] | None = None,
    allow_zero_only_pipe_rows: bool = False,
) -> dict[str, Any]:
    output_root = (
        run_dir
        / "profiler_raw"
        / target_version
        / f"{metric_label}-r{repetition:03d}"
    )
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)
    batch_manifest_path = output_root / "CASE_BATCH.json"
    batch_log_root = output_root / "case_logs"
    application = shlex.join(
        build_batch_command(
            python_bin,
            bootstrap_path,
            task_case,
            case_ids,
            batch_manifest_path,
            batch_log_root,
        )
    )
    command = [msprof, "op", f"--output={output_root}"]
    if (
        kernel_selection == "exact"
        and supported_option(help_text, "--kernel-name")
    ):
        command.append(f"--kernel-name={kernel_name}")
    if supported_option(help_text, "--launch-count"):
        command.append(f"--launch-count={MAX_MSPROF_LAUNCH_COUNT}")
    if supported_option(help_text, "--kill"):
        command.append("--kill=off")
    if supported_option(help_text, "--warm-up"):
        command.append("--warm-up=0")
    if supported_option(help_text, "--aic-metrics"):
        command.append(f"--aic-metrics={metrics}")
    command.append(f"--application={application}")
    result = run_process(
        command,
        cwd=task_case,
        env=env,
        log_path=output_root / "msprof.log",
        timeout_seconds=timeout_seconds,
    )
    log_path = output_root / "msprof.log"
    selector = selector_diagnostics(log_path, kernel_name)
    profile_root = find_profile_root(output_root)
    parsed_csv = (
        csv_evidence(profile_root, kernel_name, case_ids)
        if profile_root is not None
        else []
    )
    csv_scan_incomplete = any(
        bool(row.get("scan_truncated") or row.get("scan_deadline_exceeded"))
        for row in parsed_csv
    )
    batch_manifest = (
        read_object(batch_manifest_path) if batch_manifest_path.is_file() else {}
    )
    matched_operator_rows = max(
        (
            int(row.get("matched_operator_rows", 0) or 0)
            for row in parsed_csv
        ),
        default=0,
    )
    total_matched_operator_rows = sum(
        int(row.get("matched_operator_rows", 0) or 0)
        for row in parsed_csv
    )
    case_rows, attribution_method = attribute_case_rows(
        parsed_csv,
        case_ids,
        batch_manifest,
        log_path,
        case_shapes,
        expected_block_dims,
    )
    zero_only_pipe_rows = _zero_only_pipe_rows(parsed_csv)
    zero_only_primary_pipe_rows = (
        metric_label == "primary" and zero_only_pipe_rows
    )
    zero_only_roofline_pipe_rows = (
        metric_label == "roofline" and zero_only_pipe_rows
    )
    pipe_quality_failure = _pipe_quality_failure(
        metric_label,
        zero_only_pipe_rows,
        allow_zero_only_pipe_rows=allow_zero_only_pipe_rows,
    )
    observed_cases = [
        int(item.get("case", 0) or 0)
        for item in batch_manifest.get("executions", [])
        if isinstance(item, dict)
    ]
    batch_valid = (
        str(batch_manifest.get("state") or "") == "passed"
        and observed_cases == case_ids
        and int(batch_manifest.get("execution_count", 0) or 0) == len(case_ids)
    )
    visualize_data = (
        [
            str(path.relative_to(run_dir)).replace("\\", "/")
            for path in itertools.islice(
                profile_root.rglob("visualize_data.bin"),
                MAX_VISUALIZE_FILES,
            )
        ]
        if profile_root is not None
        else []
    )
    profile_files = (
        [
            str(path.relative_to(output_root)).replace("\\", "/")
            for path in itertools.islice(profile_root.rglob("*"), MAX_PROFILE_FILES)
            if path.is_file()
        ]
        if profile_root is not None
        else []
    )
    failure_reason = (
        "profiler-process-timeout" if result["timed_out"] else
        "profiler-process-nonzero" if result["returncode"] != 0 else
        "profiler-batch-invalid" if not batch_valid else
        "profiler-kernel-selector-miss" if (
            kernel_selection == "exact"
            and
            selector["no_profiling_data_dumped"]
            and selector["observed_skipped_kernel_count"] > 0
        ) else
        "profiler-profile-root-missing" if profile_root is None else
        "profiler-csv-missing" if not parsed_csv else
        "profiler-csv-scan-incomplete" if csv_scan_incomplete else
        "profiler-kernel-unmatched" if matched_operator_rows == 0 else
        pipe_quality_failure if pipe_quality_failure else
        attribution_method if len(case_rows) != len(case_ids) else ""
    )
    result.update(
        {
            "requested_kernel_name": kernel_name,
            "kernel_selection": kernel_selection,
            "case_ids": case_ids,
            "case_count": len(case_ids),
            "metric_label": metric_label,
            "repetition": repetition,
            "metrics": metrics,
            "batch_manifest": str(
                batch_manifest_path.relative_to(run_dir)
            ).replace("\\", "/"),
            "batch_state": str(batch_manifest.get("state") or "missing"),
            "observed_cases": observed_cases,
            "output_root": str(output_root.relative_to(run_dir)).replace("\\", "/"),
            "profile_root": (
                str(profile_root.relative_to(run_dir)).replace("\\", "/")
                if profile_root is not None
                else ""
            ),
            "csv_evidence": parsed_csv,
            "csv_scan_incomplete": csv_scan_incomplete,
            "matched_operator_rows": matched_operator_rows,
            "total_matched_operator_rows": total_matched_operator_rows,
            "selector_diagnostics": selector,
            "launch_count_cap": MAX_MSPROF_LAUNCH_COUNT,
            "effective_profile_rounds": EFFECTIVE_PROFILE_ROUNDS,
            "attributable_case_csv_count": 1 if case_rows else 0,
            "case_attribution_method": attribution_method,
            "case_rows": case_rows,
            "zero_only_pipe_rows": zero_only_pipe_rows,
            "zero_only_primary_pipe_rows": zero_only_primary_pipe_rows,
            "zero_only_roofline_pipe_rows": zero_only_roofline_pipe_rows,
            "zero_only_pipe_rows_allowed": (
                zero_only_pipe_rows and allow_zero_only_pipe_rows
            ),
            "visualize_data": visualize_data,
            "profile_files": profile_files,
            "failure_reason": failure_reason,
        }
    )
    result["success"] = (
        result["returncode"] == 0
        and not result["timed_out"]
        and profile_root is not None
        and batch_valid
        and len(case_rows) == len(case_ids)
        and not failure_reason
    )
    return result


def render_summary(evidence: dict[str, Any]) -> str:
    lines = [
        f"# Profiler Evidence: {evidence['operator']} {evidence['target_version']}",
        "",
        f"- Status: `{evidence['status']}`",
        f"- Case version: `{evidence['case_version']}`",
        f"- Blocker generation: `{evidence['blocker_generation']}`",
        f"- Source SHA256: `{evidence['target_source_sha256']}`",
        f"- Primary metrics: `{evidence['primary_metrics']}`",
        f"- Primary success: `{evidence['primary_success_count']}/{evidence['primary_run_count']}`",
        f"- Roofline success: `{evidence['roofline_success_count']}/{evidence['roofline_run_count']}`",
        "",
        "## Runs",
        "",
        "| Cases | Metrics | Success | Duration (s) | Raw profile |",
        "|---|---|---:|---:|---|",
    ]
    for row in evidence.get("runs", []):
        case_label = ",".join(str(value) for value in row.get("case_ids", []))
        lines.append(
            f"| {case_label or '-'} | {row['metric_label']} r{row.get('repetition', 1)} | "
            f"{'yes' if row['success'] else 'no'} | "
            f"{row['duration_seconds']} | `{row.get('profile_root') or '-'}` |"
        )
    if evidence.get("error"):
        lines.extend(["", "## Error", "", str(evidence["error"])])
    return "\n".join(lines) + "\n"


def persist_progress(
    run_dir: Path,
    base: dict[str, Any],
    runs: list[dict[str, Any]],
    *,
    phase: str,
    metric_label: str = "",
    repetition: int = 0,
) -> None:
    primary = [row for row in runs if row.get("metric_label") == "primary"]
    roofline = [row for row in runs if row.get("metric_label") == "roofline"]
    progress = {
        "protocol_version": "ascendop-profiler-progress-v1",
        "operator": str(base.get("operator") or ""),
        "target_version": str(base.get("target_version") or ""),
        "blocker_generation": str(base.get("blocker_generation") or ""),
        "phase": phase,
        "metric_label": metric_label,
        "repetition": int(repetition),
        "measurement_repetitions": int(
            base.get("measurement_repetitions", 1) or 1
        ),
        "completed_primary_processes": len(primary),
        "completed_roofline_processes": len(roofline),
        "updated_at": utc_now_iso(),
    }
    snapshot = {
        **base,
        "status": "collecting",
        "runs": list(runs),
        "primary_run_count": len(primary),
        "primary_success_count": sum(bool(row.get("success")) for row in primary),
        "roofline_run_count": len(roofline),
        "roofline_success_count": sum(
            bool(row.get("success")) for row in roofline
        ),
        "progress": progress,
        "updated_at": progress["updated_at"],
    }
    write_object(run_dir / PROFILER_EVIDENCE_FILE, snapshot)
    write_object(run_dir / PROFILER_PROGRESS_FILE, progress)


def run(plan_path: Path, run_dir: Path, python_bin: str) -> int:
    plan = read_object(plan_path)
    op = safe_token(str(plan["operator"]), "operator")
    profiler_kernel_name = safe_token(
        str(plan["profiler_kernel_name"]), "profiler kernel name"
    )
    profiler_kernel_selection = str(
        plan.get("profiler_kernel_selection") or "exact"
    ).strip()
    if profiler_kernel_selection not in {"exact", "prefix-postfilter"}:
        raise RuntimeError(
            "unsupported profiler kernel selection: "
            f"{profiler_kernel_selection}"
        )
    target_version = safe_token(str(plan["target_version"]), "target version")
    cases = [int(value) for value in plan.get("cases", [])]
    roofline_cases = {int(value) for value in plan.get("roofline_cases", [])}
    requested_mode = str(
        plan.get("profiler_mode")
        or plan.get("collection_mode")
        or "fast-single"
    )
    profiler_mode = {
        "fast-single": "fast-single",
        "deep-dual": "deep-dual",
        "batched-primary-only": "fast-single",
        "batched-primary-roofline": "deep-dual",
    }.get(requested_mode)
    if profiler_mode is None:
        raise RuntimeError(f"unsupported profiler mode: {requested_mode}")
    primary_metrics = str(
        plan.get("primary_metrics")
        or "PipeUtilization,Occupancy,KernelScale,BasicInfo"
    )
    timeout_seconds = max(30, int(plan.get("profile_timeout_seconds", 90) or 90))
    timeout_seconds = min(timeout_seconds, 90)
    measurement_repetitions = int(plan.get("measurement_repetitions", 1) or 1)
    if not 1 <= measurement_repetitions <= 5:
        raise RuntimeError("measurement_repetitions must be in [1, 5]")
    case_shapes = dict(plan.get("case_shapes") or {})
    expected_block_dims = dict(plan.get("expected_block_dims") or {})
    task_case = run_dir / "task_case"
    test_op = task_case / "test_op.py"
    if not test_op.is_file():
        raise RuntimeError(f"profiler task case is missing test_op.py: {test_op}")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "profiler_raw").mkdir(parents=True, exist_ok=True)
    bootstrap_path = run_dir / "profiler_test_op_bootstrap.py"
    bootstrap_path.write_text(TEST_OP_BOOTSTRAP_SOURCE, encoding="utf-8")
    msprof = shutil.which("msprof")
    capability: dict[str, Any] = {
        "protocol_version": "ascendop-profiler-capability-v1",
        "observed_at": utc_now_iso(),
        "msprof_path": msprof or "",
        "msprof_op_available": False,
        "primary_metrics": primary_metrics,
        "roofline_requested": bool(roofline_cases),
    }
    base: dict[str, Any] = {
        "protocol_version": "ascendop-profiler-evidence-v1",
        "operator": op,
        "profiler_kernel_name": profiler_kernel_name,
        "profiler_kernel_selection": profiler_kernel_selection,
        "case_version": str(plan["case_version"]),
        "blocker_result_version": str(plan["blocker_result_version"]),
        "blocker_generation": str(plan["blocker_generation"]),
        "request_sha256": str(plan["request_sha256"]),
        "request_state_path": str(plan["request_state_path"]),
        "profiler_execution_contract_digest": str(
            plan["profiler_execution_contract_digest"]
        ),
        "target_version": target_version,
        "target_source_sha256": str(plan["target_source_sha256"]),
        "cases": cases,
        "profiler_mode": profiler_mode,
        "collection_mode": requested_mode,
        "primary_metrics": primary_metrics,
        "roofline_cases": sorted(roofline_cases),
        "measurement_repetitions": measurement_repetitions,
        "comparison_affinity": str(plan.get("comparison_affinity") or ""),
        "case_shapes": case_shapes,
        "case_specs_sha256": str(plan.get("case_specs_sha256") or ""),
        "expected_block_dims": expected_block_dims,
        "started_at": utc_now_iso(),
        "runs": [],
        "warmup": {},
        "error": "",
    }
    if not msprof:
        base.update(
            {
                "status": "unsupported",
                "error": "msprof executable is not available in the Engine runtime",
                "primary_run_count": 0,
                "primary_success_count": 0,
                "roofline_run_count": 0,
                "roofline_success_count": 0,
                "finished_at": utc_now_iso(),
            }
        )
        write_object(run_dir / PROFILER_CAPABILITY_FILE, capability)
        write_object(run_dir / PROFILER_EVIDENCE_FILE, base)
        (run_dir / PROFILER_SUMMARY_FILE).write_text(
            render_summary(base), encoding="utf-8"
        )
        return 0
    help_text, help_rc = command_help(msprof)
    capability.update(
        {
            "msprof_op_available": help_rc == 0,
            "help_returncode": help_rc,
            "supports_kernel_name": supported_option(help_text, "--kernel-name"),
            "supports_launch_count": supported_option(help_text, "--launch-count"),
            "supports_aic_metrics": supported_option(help_text, "--aic-metrics"),
            "supports_application": supported_option(help_text, "--application"),
        }
    )
    (run_dir / "msprof_op_help.txt").write_text(help_text, encoding="utf-8")
    if help_rc != 0 or not capability["supports_application"]:
        base.update(
            {
                "status": "unsupported",
                "error": "msprof op does not expose the required application interface",
                "primary_run_count": 0,
                "primary_success_count": 0,
                "roofline_run_count": 0,
                "roofline_success_count": 0,
                "finished_at": utc_now_iso(),
            }
        )
        write_object(run_dir / PROFILER_CAPABILITY_FILE, capability)
        write_object(run_dir / PROFILER_EVIDENCE_FILE, base)
        (run_dir / PROFILER_SUMMARY_FILE).write_text(
            render_summary(base), encoding="utf-8"
        )
        return 0

    env = {
        **os.environ,
        # Do not let PyTorch discover torch_npu twice through both the explicit
        # import and the device-backend entry point.
        "TORCH_DEVICE_BACKEND_AUTOLOAD": "0",
    }
    runs: list[dict[str, Any]] = []
    write_object(run_dir / PROFILER_CAPABILITY_FILE, capability)
    persist_progress(run_dir, base, runs, phase="preparing")
    warmup_runs = max(0, int(plan.get("warmup_runs", 1) or 0))
    if warmup_runs and cases:
        warmup_command = build_test_op_command(
            python_bin,
            bootstrap_path,
            test_op,
            cases[0],
        )
        persist_progress(run_dir, base, runs, phase="warmup-running")
        base["warmup"] = run_process(
            warmup_command,
            cwd=task_case,
            env={**env, "ROUND": "1"},
            log_path=run_dir / "profiler_warmup.log",
            timeout_seconds=min(60, timeout_seconds),
        )
        persist_progress(run_dir, base, runs, phase="warmup-complete")
        if (
            bool(base["warmup"].get("timed_out"))
            or int(base["warmup"].get("returncode", 1) or 0) != 0
        ):
            base.update(
                {
                    "status": "failed",
                    "error": "profiler warmup failed; profiling runs skipped",
                    "runs": [],
                    "primary_run_count": 0,
                    "primary_success_count": 0,
                    "roofline_run_count": 0,
                    "roofline_success_count": 0,
                    "finished_at": utc_now_iso(),
                    "raw_root": "profiler_raw",
                }
            )
            write_object(run_dir / PROFILER_CAPABILITY_FILE, capability)
            write_object(run_dir / PROFILER_EVIDENCE_FILE, base)
            (run_dir / PROFILER_SUMMARY_FILE).write_text(
                render_summary(base), encoding="utf-8"
            )
            return 0
    for repetition in range(1, measurement_repetitions + 1):
        persist_progress(
            run_dir,
            base,
            runs,
            phase="profiler-process-running",
            metric_label="primary",
            repetition=repetition,
        )
        primary_run = profile_batch(
            msprof=msprof,
            help_text=help_text,
            task_case=task_case,
            run_dir=run_dir,
            python_bin=python_bin,
            kernel_name=profiler_kernel_name,
            kernel_selection=profiler_kernel_selection,
            target_version=target_version,
            case_ids=cases,
            metric_label="primary",
            metrics=primary_metrics,
            timeout_seconds=timeout_seconds,
            env=env,
            bootstrap_path=bootstrap_path,
            repetition=repetition,
            case_shapes=case_shapes,
            expected_block_dims=expected_block_dims,
            allow_zero_only_pipe_rows=profiler_mode == "deep-dual",
        )
        runs.append(primary_run)
        persist_progress(
            run_dir,
            base,
            runs,
            phase="profiler-process-complete",
            metric_label="primary",
            repetition=repetition,
        )
        if not primary_run["success"]:
            break
    primary = [row for row in runs if row["metric_label"] == "primary"]
    primary_success = sum(bool(row["success"]) for row in primary)
    if (
        profiler_mode == "deep-dual"
        and len(primary) == measurement_repetitions
        and primary_success == measurement_repetitions
    ):
        for repetition in range(1, measurement_repetitions + 1):
            persist_progress(
                run_dir,
                base,
                runs,
                phase="profiler-process-running",
                metric_label="roofline",
                repetition=repetition,
            )
            runs.append(
                profile_batch(
                    msprof=msprof,
                    help_text=help_text,
                    task_case=task_case,
                    run_dir=run_dir,
                    python_bin=python_bin,
                    kernel_name=profiler_kernel_name,
                    kernel_selection=profiler_kernel_selection,
                    target_version=target_version,
                    case_ids=cases,
                    metric_label="roofline",
                    metrics="Roofline",
                    timeout_seconds=timeout_seconds,
                    env=env,
                    bootstrap_path=bootstrap_path,
                    repetition=repetition,
                    case_shapes=case_shapes,
                    expected_block_dims=expected_block_dims,
                    allow_zero_only_pipe_rows=False,
                )
            )
            persist_progress(
                run_dir,
                base,
                runs,
                phase="profiler-process-complete",
                metric_label="roofline",
                repetition=repetition,
            )
            if not runs[-1]["success"]:
                break
    roofline = [row for row in runs if row["metric_label"] == "roofline"]
    roofline_success = sum(bool(row["success"]) for row in roofline)
    roofline_required = profiler_mode == "deep-dual"
    status = (
        "complete"
        if (
            len(primary) == measurement_repetitions
            and primary_success == measurement_repetitions
            and (
                not roofline_required
                or (
                    len(roofline) == measurement_repetitions
                    and roofline_success == measurement_repetitions
                )
            )
        )
        else "partial"
        if primary_success or roofline_success
        else "failed"
    )
    base.update(
        {
            "status": status,
            "runs": runs,
            "primary_run_count": len(primary),
            "primary_success_count": primary_success,
            "roofline_run_count": len(roofline),
            "roofline_success_count": roofline_success,
            "finished_at": utc_now_iso(),
            "raw_root": "profiler_raw",
            "error": "; ".join(
                str(row.get("failure_reason") or "")
                for row in runs
                if row.get("failure_reason")
            ),
        }
    )
    write_object(run_dir / PROFILER_CAPABILITY_FILE, capability)
    write_object(run_dir / PROFILER_EVIDENCE_FILE, base)
    (run_dir / PROFILER_SUMMARY_FILE).write_text(
        render_summary(base), encoding="utf-8"
    )
    return 0


PROFILER_EVIDENCE_FILE = "PROFILER_EVIDENCE.json"
PROFILER_SUMMARY_FILE = "PROFILER_SUMMARY.md"
PROFILER_CAPABILITY_FILE = "PROFILER_CAPABILITY.json"
PROFILER_PROGRESS_FILE = "PROFILER_PROGRESS.json"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--python-bin", required=True)
    args = parser.parse_args()
    try:
        return run(args.plan.resolve(), args.run_dir.resolve(), args.python_bin)
    except Exception as exc:
        run_dir = args.run_dir.resolve()
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "profiler_raw").mkdir(parents=True, exist_ok=True)
        try:
            plan = read_object(args.plan.resolve())
        except Exception:
            plan = {}
        fallback = {
            "protocol_version": "ascendop-profiler-evidence-v1",
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "operator": str(plan.get("operator") or ""),
            "profiler_kernel_name": str(
                plan.get("profiler_kernel_name") or ""
            ),
            "profiler_kernel_selection": str(
                plan.get("profiler_kernel_selection") or "exact"
            ),
            "case_version": str(plan.get("case_version") or ""),
            "blocker_result_version": str(
                plan.get("blocker_result_version") or ""
            ),
            "blocker_generation": str(plan.get("blocker_generation") or ""),
            "request_sha256": str(plan.get("request_sha256") or ""),
            "request_state_path": str(plan.get("request_state_path") or ""),
            "target_version": str(plan.get("target_version") or ""),
            "target_source_sha256": str(
                plan.get("target_source_sha256") or ""
            ),
            "primary_run_count": 0,
            "primary_success_count": 0,
            "roofline_run_count": 0,
            "roofline_success_count": 0,
            "runs": [],
            "finished_at": utc_now_iso(),
        }
        write_object(run_dir / PROFILER_EVIDENCE_FILE, fallback)
        (run_dir / PROFILER_SUMMARY_FILE).write_text(
            render_summary(fallback), encoding="utf-8"
        )
        write_object(
            run_dir / PROFILER_CAPABILITY_FILE,
            {
                "protocol_version": "ascendop-profiler-capability-v1",
                "status": "runner-failed",
                "error": fallback["error"],
            },
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
