from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
    proc = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        **subprocess_launch_kwargs(),
    )
    timed_out = False
    try:
        stdout, _ = proc.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            proc.terminate()
        try:
            stdout, _ = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                proc.kill()
            stdout, _ = proc.communicate()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(stdout or "", encoding="utf-8")
    return {
        "command": shlex.join(command),
        "returncode": int(proc.returncode or 0),
        "timed_out": timed_out,
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "duration_seconds": round(time.monotonic() - started, 6),
        "log_path": str(log_path),
    }


def find_profile_root(output_root: Path) -> Path | None:
    values = sorted(
        (path for path in output_root.rglob("OPPROF_*") if path.is_dir()),
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
    for path in sorted(profile_root.rglob("*.csv")):
        try:
            with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = [dict(row) for _, row in zip(range(500), reader)]
        except (OSError, csv.Error):
            continue
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
            "matched_operator_rows": len(matched),
            "rows": compact_rows[:50],
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
    return evidence


_CSV_TIMESTAMP_RE = re.compile(r"_(\d{17})\.csv$", re.IGNORECASE)


def attribute_case_rows(
    parsed_csv: list[dict[str, Any]],
    case_ids: list[int],
) -> tuple[list[dict[str, Any]], str]:
    direct = [
        row
        for row in parsed_csv
        if len(row.get("case_rows", [])) == len(case_ids)
    ]
    if direct:
        return list(direct[0]["case_rows"]), "single-csv-row-order"

    launches: list[tuple[str, str, int, dict[str, Any]]] = []
    for record in parsed_csv:
        matched_count = int(record.get("matched_operator_rows", 0) or 0)
        if matched_count <= 0:
            continue
        path = str(record.get("path") or "")
        match = _CSV_TIMESTAMP_RE.search(path)
        if match is None:
            return [], "unattributable-missing-csv-timestamp"
        rows = list(record.get("rows") or [])
        if len(rows) < matched_count:
            return [], "unattributable-missing-compact-rows"
        for row_index in range(matched_count):
            launches.append((match.group(1), path, row_index, rows[row_index]))

    if len(launches) != len(case_ids):
        return [], "unattributable-launch-count-mismatch"
    sequence_keys = [(timestamp, path, row_index) for timestamp, path, row_index, _ in launches]
    if len(set(sequence_keys)) != len(sequence_keys):
        return [], "unattributable-duplicate-sequence-key"

    launches.sort(key=lambda item: (item[0], item[1], item[2]))
    return (
        [
            {
                "case_id": case_id,
                "sequence_index": index,
                "source_path": path,
                "source_row_index": row_index,
                "source_timestamp": timestamp,
                "row": row,
            }
            for index, (case_id, (timestamp, path, row_index, row)) in enumerate(
                zip(case_ids, launches)
            )
        ],
        "msprof-csv-timestamp-order",
    )


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
    ]


def profile_batch(
    *,
    msprof: str,
    help_text: str,
    task_case: Path,
    run_dir: Path,
    python_bin: str,
    op: str,
    target_version: str,
    case_ids: list[int],
    metric_label: str,
    metrics: str,
    timeout_seconds: int,
    env: dict[str, str],
    bootstrap_path: Path,
) -> dict[str, Any]:
    output_root = (
        run_dir
        / "profiler_raw"
        / target_version
        / metric_label
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
    if supported_option(help_text, "--kernel-name"):
        command.append(f"--kernel-name={op}")
    if supported_option(help_text, "--launch-count"):
        command.append(f"--launch-count={len(case_ids)}")
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
    profile_root = find_profile_root(output_root)
    parsed_csv = (
        csv_evidence(profile_root, op, case_ids)
        if profile_root is not None
        else []
    )
    matched_operator_rows = sum(
        int(row.get("matched_operator_rows", 0) or 0)
        for row in parsed_csv
    )
    case_rows, attribution_method = attribute_case_rows(parsed_csv, case_ids)
    batch_manifest = (
        read_object(batch_manifest_path) if batch_manifest_path.is_file() else {}
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
            for path in profile_root.rglob("visualize_data.bin")
        ]
        if profile_root is not None
        else []
    )
    result.update(
        {
            "case_ids": case_ids,
            "case_count": len(case_ids),
            "metric_label": metric_label,
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
            "matched_operator_rows": matched_operator_rows,
            "attributable_case_csv_count": 1 if case_rows else 0,
            "case_attribution_method": attribution_method,
            "case_rows": case_rows,
            "visualize_data": visualize_data,
        }
    )
    result["success"] = (
        result["returncode"] == 0
        and not result["timed_out"]
        and profile_root is not None
        and batch_valid
        and len(case_rows) == len(case_ids)
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
            f"| {case_label or '-'} | {row['metric_label']} | "
            f"{'yes' if row['success'] else 'no'} | "
            f"{row['duration_seconds']} | `{row.get('profile_root') or '-'}` |"
        )
    if evidence.get("error"):
        lines.extend(["", "## Error", "", str(evidence["error"])])
    return "\n".join(lines) + "\n"


def run(plan_path: Path, run_dir: Path, python_bin: str) -> int:
    plan = read_object(plan_path)
    op = safe_token(str(plan["operator"]), "operator")
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
        "case_version": str(plan["case_version"]),
        "blocker_result_version": str(plan["blocker_result_version"]),
        "blocker_generation": str(plan["blocker_generation"]),
        "request_sha256": str(plan["request_sha256"]),
        "request_state_path": str(plan["request_state_path"]),
        "target_version": target_version,
        "target_source_sha256": str(plan["target_source_sha256"]),
        "cases": cases,
        "profiler_mode": profiler_mode,
        "collection_mode": requested_mode,
        "primary_metrics": primary_metrics,
        "roofline_cases": sorted(roofline_cases),
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
    warmup_runs = max(0, int(plan.get("warmup_runs", 1) or 0))
    if warmup_runs and cases:
        warmup_command = build_test_op_command(
            python_bin,
            bootstrap_path,
            test_op,
            cases[0],
        )
        base["warmup"] = run_process(
            warmup_command,
            cwd=task_case,
            env={**env, "ROUND": "1"},
            log_path=run_dir / "profiler_warmup.log",
            timeout_seconds=min(60, timeout_seconds),
        )
        if int(base["warmup"].get("returncode", 1) or 0) != 0:
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
    runs: list[dict[str, Any]] = []
    primary_run = profile_batch(
        msprof=msprof,
        help_text=help_text,
        task_case=task_case,
        run_dir=run_dir,
        python_bin=python_bin,
        op=op,
        target_version=target_version,
        case_ids=cases,
        metric_label="primary",
        metrics=primary_metrics,
        timeout_seconds=timeout_seconds,
        env=env,
        bootstrap_path=bootstrap_path,
    )
    runs.append(primary_run)
    if (
        primary_run["success"]
        and profiler_mode == "deep-dual"
    ):
        runs.append(
            profile_batch(
                msprof=msprof,
                help_text=help_text,
                task_case=task_case,
                run_dir=run_dir,
                python_bin=python_bin,
                op=op,
                target_version=target_version,
                case_ids=cases,
                metric_label="roofline",
                metrics="Roofline",
                timeout_seconds=timeout_seconds,
                env=env,
                bootstrap_path=bootstrap_path,
            )
        )
    primary = [row for row in runs if row["metric_label"] == "primary"]
    roofline = [row for row in runs if row["metric_label"] == "roofline"]
    primary_success = sum(bool(row["success"]) for row in primary)
    roofline_success = sum(bool(row["success"]) for row in roofline)
    roofline_required = profiler_mode == "deep-dual"
    status = (
        "complete"
        if (
            primary
            and primary_success == len(primary)
            and (not roofline_required or roofline_success == 1)
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
