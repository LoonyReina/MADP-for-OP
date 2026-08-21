from __future__ import annotations

import csv
import itertools
import json
import os
import re
import shlex
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
    if not value or any(
        char
        not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
        for char in value
    ):
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
            with path.open(
                "r", encoding="utf-8-sig", errors="replace", newline=""
            ) as handle:
                reader = csv.DictReader(handle)
                observed = [
                    dict(row) for row in itertools.islice(reader, MAX_CSV_ROWS + 1)
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


def zero_only_pipe_rows(parsed_csv: list[dict[str, Any]]) -> bool:
    rows: list[dict[str, Any]] = []
    for record in parsed_csv:
        if "pipeutilization" not in str(record.get("path") or "").lower():
            continue
        rows.extend(
            value for value in record.get("rows", []) if isinstance(value, dict)
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


def pipe_quality_failure(
    metric_label: str,
    zero_only_pipe_rows: bool,
    *,
    allow_zero_only_pipe_rows: bool,
) -> str:
    if not zero_only_pipe_rows or allow_zero_only_pipe_rows:
        return ""
    return f"profiler-{metric_label}-pipe-zero-only"
