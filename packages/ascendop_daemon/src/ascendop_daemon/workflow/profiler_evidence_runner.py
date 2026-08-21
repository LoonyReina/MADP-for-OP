from __future__ import annotations

import argparse
import importlib
import itertools
import json
import os
import shlex
import shutil
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.profiler_evidence_support import (
    EFFECTIVE_PROFILE_ROUNDS,
    MAX_MSPROF_LAUNCH_COUNT,
    MAX_PROFILE_FILES,
    MAX_VISUALIZE_FILES,
    TEST_OP_BOOTSTRAP_SOURCE,
    build_batch_command,
    build_test_op_command,
    command_help,
    csv_evidence,
    find_profile_root,
    pipe_quality_failure as _pipe_quality_failure,
    read_object,
    run_process,
    safe_token,
    selector_diagnostics,
    supported_option,
    utc_now_iso,
    write_object,
    zero_only_pipe_rows as _zero_only_pipe_rows,
)

attribute_case_rows = importlib.import_module(f"{__package__ + '.' if __package__ else ''}profiler_row_attribution").attribute_case_rows


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
