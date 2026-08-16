from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import signal
import shlex
import shutil
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

from limited_remote_partner.engine.test_engine import (
    atomic_write_json,
    hidden_process_creation_flags,
    hidden_process_startup_info,
    utc_now,
)


class PerfPipelineError(RuntimeError):
    pass


Runner = Callable[..., subprocess.CompletedProcess[Any]]
DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE = 50
MAX_PERFORMANCE_CASE_COUNT = 512
MAX_TASK_ROWS_PER_CASE = 10000
BATCH_PROFILE_STORAGE_MIN_MB = 256
BATCH_PROFILE_STORAGE_MB_PER_TASK_ROW = 12
BATCH_PROFILE_STORAGE_DEFAULT_MAX_MB = 262144
SHARED_SESSION_CAPTURE_MODES = {"batched-process", "session-isolated-process"}
PERFORMANCE_FIRST_PROFILES = {
    "engine-v1-staged-performance-first-split",
    "engine-v1-staged-performance-first-correctness-batched",
    "engine-v1-staged-performance-session-correctness-batched",
    "engine-v2-staged-scalable",
    "engine-v3-staged-fused",
}


def parse_case_range(value: str) -> list[int]:
    raw = str(value or "").strip()
    if not raw:
        raise PerfPipelineError("performance case range is empty")
    if ".." in raw:
        left, right = raw.split("..", 1)
        start = int(left)
        finish = int(right)
        if start <= 0 or finish < start:
            raise PerfPipelineError(f"invalid performance case range: {value}")
        values = list(range(start, finish + 1))
    else:
        values = [int(item) for item in raw.replace(",", " ").split()]
    if not values or any(item <= 0 for item in values):
        raise PerfPipelineError(f"invalid performance case range: {value}")
    if len(values) != len(set(values)):
        raise PerfPipelineError(f"duplicate performance case id: {value}")
    if len(values) > MAX_PERFORMANCE_CASE_COUNT:
        raise PerfPipelineError(
            f"performance case count exceeds {MAX_PERFORMANCE_CASE_COUNT}: {len(values)}"
        )
    return values


def capture_profiles(
    *,
    task_case: Path,
    run_dir: Path,
    label: str,
    case_range: str,
    python_bin: str,
    storage_limit: str,
    timeout_seconds: int = 330,
    batch_process: bool = False,
    session_isolated_process: bool = False,
    expected_task_rows_per_case: int = DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    task_case = task_case.resolve()
    run_dir = run_dir.resolve()
    if not (task_case / "test_op.py").is_file():
        raise PerfPipelineError(f"test_op.py is missing: {task_case}")
    if batch_process and session_isolated_process:
        raise PerfPipelineError(
            "batch_process and session_isolated_process are mutually exclusive"
        )
    expected_task_rows_per_case = validate_task_rows_per_case(
        expected_task_rows_per_case
    )
    case_ids = parse_case_range(case_range)
    run_dir.mkdir(parents=True, exist_ok=True)
    profile_root = run_dir / "profiles_raw" / safe_token(label)
    ensure_under(profile_root, run_dir)
    shutil.rmtree(profile_root, ignore_errors=True)
    profile_root.mkdir(parents=True)
    cases: list[dict[str, Any]] = []
    capture_mode = (
        "batched-process"
        if batch_process
        else "session-isolated-process"
        if session_isolated_process
        else "isolated-process"
    )
    measurement_contract = build_measurement_contract(
        capture_mode=capture_mode,
        case_ids=case_ids,
        expected_task_rows_per_case=expected_task_rows_per_case,
        storage_limit=storage_limit,
    )
    manifest = {
        "protocol_version": "engine-perf-v1",
        "stage": "capture",
        "capture_mode": capture_mode,
        "execution_profile": measurement_contract["execution_profile"],
        "stage_sequence": measurement_contract["stage_sequence"],
        "capture_before_correctness": measurement_contract[
            "capture_before_correctness"
        ],
        "export_before_correctness": measurement_contract[
            "export_before_correctness"
        ],
        "measurement_contract": measurement_contract["measurement_contract"],
        "measurement_contract_sha256": measurement_contract[
            "measurement_contract_sha256"
        ],
        "measurement_pipeline_sha256": measurement_contract[
            "measurement_pipeline_sha256"
        ],
        "label": label,
        "case_range": case_range,
        "case_ids": case_ids,
        "case_count": len(case_ids),
        "expected_op_name": str(
            measurement_contract["measurement_contract"].get("expected_op_name")
            or ""
        ),
        "expected_task_rows_per_case": expected_task_rows_per_case,
        "expected_task_rows": len(case_ids) * expected_task_rows_per_case,
        "started_at": utc_now(),
        "profile_root": str(profile_root),
        "cases": cases,
    }
    manifest_path = run_dir / "PERF_CAPTURE.json"
    if batch_process:
        return capture_batched_profile(
            task_case=task_case,
            run_dir=run_dir,
            profile_root=profile_root,
            manifest=manifest,
            manifest_path=manifest_path,
            case_range=case_range,
            python_bin=python_bin,
            storage_limit=storage_limit,
            timeout_seconds=timeout_seconds,
            expected_task_rows_per_case=expected_task_rows_per_case,
            runner=runner,
        )
    if session_isolated_process:
        return capture_session_isolated_profile(
            task_case=task_case,
            run_dir=run_dir,
            profile_root=profile_root,
            manifest=manifest,
            manifest_path=manifest_path,
            case_range=case_range,
            python_bin=python_bin,
            storage_limit=storage_limit,
            timeout_seconds=timeout_seconds,
            expected_task_rows_per_case=expected_task_rows_per_case,
            runner=runner,
        )
    for case in case_ids:
        case_root = profile_root / f"case{case}"
        case_root.mkdir(parents=True)
        log_path = run_dir / f"perf_case{case}_capture.log"
        application = [python_bin, "test_op.py", str(case)]
        started_at = utc_now()
        returncode = run_msprof_capture(
            runner,
            application=application,
            output_root=case_root,
            storage_limit=storage_limit,
            cwd=task_case,
            log_path=log_path,
            timeout_seconds=max(1, int(timeout_seconds)),
        )
        item = {
            "case": case,
            "profile_root": str(case_root),
            "capture_log": str(log_path),
            "started_at": started_at,
            "finished_at": utc_now(),
            "returncode": returncode,
        }
        cases.append(item)
        atomic_write_json(manifest_path, {**manifest, "updated_at": utc_now()})
        if returncode != 0 or not any(case_root.glob("PROF*")):
            raise PerfPipelineError(
                f"profile capture failed for case {case}: rc={returncode}"
            )
    manifest["finished_at"] = utc_now()
    manifest["state"] = "captured"
    atomic_write_json(manifest_path, manifest)
    return manifest


def capture_batched_profile(
    *,
    task_case: Path,
    run_dir: Path,
    profile_root: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    case_range: str,
    python_bin: str,
    storage_limit: str,
    timeout_seconds: int,
    expected_task_rows_per_case: int,
    runner: Runner,
) -> dict[str, Any]:
    batch_root = profile_root / "batch"
    batch_root.mkdir(parents=True)
    batch_manifest = run_dir / "PERF_BATCH.json"
    batch_log_root = run_dir / "perf_batch_execution"
    log_path = run_dir / "perf_batch_capture.log"
    application = [
        python_bin,
        "-m",
        "limited_remote_partner.engine.batch_case_runner",
        "--task-case",
        str(task_case),
        "--case-range",
        case_range,
        "--repetitions",
        "1",
        "--output",
        str(batch_manifest),
        "--log-root",
        str(batch_log_root),
        "--mode",
        "performance",
        "--expected-task-rows-per-case",
        str(expected_task_rows_per_case),
    ]
    started_at = utc_now()
    capture_timeout_seconds = shared_capture_timeout_seconds(
        timeout_seconds, len(parse_case_range(case_range))
    )
    effective_storage_limit, storage_budget = scaled_batched_storage_limit(
        storage_limit,
        case_count=len(parse_case_range(case_range)),
        expected_task_rows_per_case=expected_task_rows_per_case,
    )
    returncode = run_msprof_capture(
        runner,
        application=application,
        output_root=batch_root,
        storage_limit=effective_storage_limit,
        cwd=task_case,
        log_path=log_path,
        timeout_seconds=capture_timeout_seconds,
    )
    cases = [
        {
            "case": case,
            "sequence_index": index,
            "profile_root": str(batch_root),
            "capture_log": str(log_path),
            "started_at": started_at,
            "finished_at": utc_now(),
            "returncode": returncode,
        }
        for index, case in enumerate(parse_case_range(case_range))
    ]
    failure = ""
    if returncode != 0 or not any(batch_root.glob("PROF*")):
        failure = f"batched profile capture failed: rc={returncode}"
        log_tail = read_log_tail(log_path)
        if log_tail:
            failure += f"; log_tail={log_tail}"
    elif not batch_manifest.is_file():
        failure = "batched profile application did not write PERF_BATCH.json"
    batch = read_object(batch_manifest) if batch_manifest.is_file() else {}
    expected = parse_case_range(case_range)
    observed = [
        int(item.get("case", 0) or 0)
        for item in batch.get("executions", [])
        if isinstance(item, dict)
    ]
    batch_state = str(batch.get("state") or "")
    failed_executions = [
        item
        for item in batch.get("executions", [])
        if isinstance(item, dict) and str(item.get("verdict") or "") != "PASS"
    ]
    if not failure and batch_state != "passed":
        failed_cases = sorted(
            {
                int(item.get("case", 0) or 0)
                for item in failed_executions
                if int(item.get("case", 0) or 0) > 0
            }
        )
        details = "; ".join(
            (
                f"case={int(item.get('case', 0) or 0)} "
                f"repeat={int(item.get('repetition', 0) or 0)} "
                f"detail={str(item.get('error') or item.get('log') or '-')}"
            )
            for item in failed_executions[:8]
        )
        failure = (
            "batched profile application failed: "
            f"state={batch_state or 'missing'} "
            f"fail_count={int(batch.get('fail_count', len(failed_executions)) or 0)} "
            f"failed_cases={failed_cases}"
            + (f"; {details}" if details else "")
        )
    elif not failure and observed != expected:
        failure = (
            "batched profile execution order mismatch: "
            f"expected={expected} observed={observed}"
        )
    observed_rows_per_case = int(batch.get("expected_task_rows_per_case", 0) or 0)
    if not failure and observed_rows_per_case != expected_task_rows_per_case:
        failure = (
            "batched profile sample contract mismatch: "
            f"expected={expected_task_rows_per_case} "
            f"observed={observed_rows_per_case}"
        )
    manifest.update(
        {
            "cases": cases,
            "batch_profile_root": str(batch_root),
            "shared_profile_root": str(batch_root),
            "batch_manifest": str(batch_manifest),
            "timeout_per_case_seconds": max(1, int(timeout_seconds)),
            "capture_timeout_seconds": capture_timeout_seconds,
            "expected_task_rows_per_case": expected_task_rows_per_case,
            "profile_storage_budget": storage_budget,
            "finished_at": utc_now(),
            "state": "failed" if failure else "captured",
            "failure": failure,
        }
    )
    atomic_write_json(manifest_path, manifest)
    if failure:
        raise PerfPipelineError(failure)
    return manifest


def capture_session_isolated_profile(
    *,
    task_case: Path,
    run_dir: Path,
    profile_root: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    case_range: str,
    python_bin: str,
    storage_limit: str,
    timeout_seconds: int,
    expected_task_rows_per_case: int,
    runner: Runner,
) -> dict[str, Any]:
    session_root = profile_root / "session"
    session_root.mkdir(parents=True)
    session_manifest = run_dir / "PERF_SESSION.json"
    session_log_root = run_dir / "perf_session_execution"
    log_path = run_dir / "perf_session_capture.log"
    application = [
        python_bin,
        "-m",
        "limited_remote_partner.engine.stages.profile_session_runner",
        "--task-case",
        str(task_case),
        "--case-range",
        case_range,
        "--output",
        str(session_manifest),
        "--log-root",
        str(session_log_root),
        "--timeout-per-case-seconds",
        str(max(1, int(timeout_seconds))),
        "--expected-task-rows-per-case",
        str(expected_task_rows_per_case),
    ]
    case_ids = parse_case_range(case_range)
    capture_timeout_seconds = shared_capture_timeout_seconds(
        timeout_seconds, len(case_ids)
    )
    started_at = utc_now()
    returncode = run_msprof_capture(
        runner,
        application=application,
        output_root=session_root,
        storage_limit=storage_limit,
        cwd=task_case,
        log_path=log_path,
        timeout_seconds=capture_timeout_seconds,
    )
    finished_at = utc_now()
    cases = [
        {
            "case": case,
            "sequence_index": index,
            "profile_root": str(session_root),
            "capture_log": str(log_path),
            "started_at": started_at,
            "finished_at": finished_at,
            "returncode": returncode,
        }
        for index, case in enumerate(case_ids)
    ]
    failure = ""
    if returncode != 0 or not any(session_root.glob("PROF*")):
        failure = f"session-isolated profile capture failed: rc={returncode}"
    elif not session_manifest.is_file():
        failure = "profile application did not write PERF_SESSION.json"
    session = read_object(session_manifest) if session_manifest.is_file() else {}
    observed = [
        int(item.get("case", 0) or 0)
        for item in session.get("executions", [])
        if isinstance(item, dict)
    ]
    executions = [
        item for item in session.get("executions", []) if isinstance(item, dict)
    ]
    parent_pid = int(session.get("parent_pid", 0) or 0)
    child_pids = [int(item.get("pid", 0) or 0) for item in executions]
    process_instances = [
        int(item.get("process_instance", 0) or 0) for item in executions
    ]
    isolation_evidence_valid = (
        session.get("protocol_version") == "engine-profile-session-v1"
        and parent_pid > 0
        and len(executions) == len(case_ids)
        and all(str(item.get("verdict") or "") == "PASS" for item in executions)
        and all(pid > 0 and pid != parent_pid for pid in child_pids)
        and len(set(child_pids)) == len(case_ids)
        and process_instances == list(range(1, len(case_ids) + 1))
        and [int(item.get("sequence_index", -1)) for item in executions]
        == list(range(len(case_ids)))
    )
    if not failure and (
        session.get("state") != "passed"
        or session.get("execution_mode") != "isolated-child-process"
        or observed != case_ids
        or not isolation_evidence_valid
    ):
        failure = (
            "session-isolated profile execution mismatch: "
            f"expected={case_ids} observed={observed} "
            f"state={session.get('state')} mode={session.get('execution_mode')} "
            f"parent_pid={parent_pid} child_pids={child_pids} "
            f"process_instances={process_instances}"
        )
    observed_rows_per_case = int(session.get("expected_task_rows_per_case", 0) or 0)
    if not failure and observed_rows_per_case != expected_task_rows_per_case:
        failure = (
            "session-isolated profile sample contract mismatch: "
            f"expected={expected_task_rows_per_case} observed={observed_rows_per_case}"
        )
    manifest.update(
        {
            "cases": cases,
            "shared_profile_root": str(session_root),
            "session_profile_root": str(session_root),
            "session_manifest": str(session_manifest),
            "timeout_per_case_seconds": max(1, int(timeout_seconds)),
            "capture_timeout_seconds": capture_timeout_seconds,
            "finished_at": utc_now(),
            "state": "failed" if failure else "captured",
            "failure": failure,
        }
    )
    atomic_write_json(manifest_path, manifest)
    if failure:
        raise PerfPipelineError(failure)
    return manifest


def export_profiles(
    *,
    run_dir: Path,
    timeout_seconds: int = 330,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    capture = read_object(run_dir / "PERF_CAPTURE.json")
    if capture.get("state") != "captured":
        raise PerfPipelineError("capture manifest is not complete")
    exported: list[dict[str, Any]] = []
    manifest = {
        "protocol_version": "engine-perf-v1",
        "stage": "export",
        "capture_mode": str(capture.get("capture_mode") or "isolated-process"),
        "execution_profile": str(capture.get("execution_profile") or ""),
        "stage_sequence": list(capture.get("stage_sequence") or []),
        "capture_before_correctness": capture.get("capture_before_correctness")
        is True,
        "export_before_correctness": capture.get("export_before_correctness")
        is True,
        "measurement_contract": dict(capture.get("measurement_contract") or {}),
        "measurement_contract_sha256": str(
            capture.get("measurement_contract_sha256") or ""
        ),
        "measurement_pipeline_sha256": str(
            capture.get("measurement_pipeline_sha256") or ""
        ),
        "label": str(capture.get("label") or ""),
        "case_range": str(capture.get("case_range") or ""),
        "case_ids": list(capture.get("case_ids") or []),
        "case_count": int(capture.get("case_count", 0) or 0),
        "expected_op_name": str(capture.get("expected_op_name") or ""),
        "expected_task_rows_per_case": int(
            capture.get("expected_task_rows_per_case", 0) or 0
        ),
        "expected_task_rows": int(capture.get("expected_task_rows", 0) or 0),
        "started_at": utc_now(),
        "cases": exported,
    }
    manifest_path = run_dir / "PERF_EXPORT.json"
    if capture.get("capture_mode") in SHARED_SESSION_CAPTURE_MODES:
        return export_shared_profile(
            run_dir=run_dir,
            capture=capture,
            manifest=manifest,
            manifest_path=manifest_path,
            timeout_seconds=timeout_seconds,
            runner=runner,
        )
    for raw in capture.get("cases", []):
        if not isinstance(raw, dict):
            raise PerfPipelineError("invalid capture case record")
        case = int(raw.get("case", 0) or 0)
        case_root = Path(str(raw.get("profile_root") or "")).resolve()
        ensure_under(case_root, run_dir)
        log_path = run_dir / f"perf_case{case}_export.log"
        cached = bool(exported_csv_paths(case_root))
        returncode = 0
        started_at = utc_now()
        if not cached:
            returncode = run_logged(
                runner,
                ["msprof", "--export=on", f"--output={case_root}"],
                cwd=run_dir,
                log_path=log_path,
                timeout_seconds=max(1, int(timeout_seconds)),
                append=True,
            )
        csv_paths = exported_csv_paths(case_root)
        item = {
            "case": case,
            "profile_root": str(case_root),
            "export_log": str(log_path),
            "started_at": started_at,
            "finished_at": utc_now(),
            "returncode": returncode,
            "cache_hit": cached,
            "op_summary_csv": [str(path) for path in csv_paths],
        }
        exported.append(item)
        atomic_write_json(manifest_path, {**manifest, "updated_at": utc_now()})
        if returncode != 0 or not csv_paths:
            raise PerfPipelineError(
                f"profile export failed for case {case}: rc={returncode}"
            )
    manifest.update(measurement_input_manifest(manifest, run_dir))
    manifest["finished_at"] = utc_now()
    manifest["state"] = "exported"
    atomic_write_json(manifest_path, manifest)
    return manifest


def export_shared_profile(
    *,
    run_dir: Path,
    capture: dict[str, Any],
    manifest: dict[str, Any],
    manifest_path: Path,
    timeout_seconds: int,
    runner: Runner,
) -> dict[str, Any]:
    capture_mode = str(capture.get("capture_mode") or "")
    shared_root = Path(
        str(capture.get("shared_profile_root") or capture.get("batch_profile_root") or "")
    ).resolve()
    ensure_under(shared_root, run_dir)
    log_stem = "perf_session" if capture_mode == "session-isolated-process" else "perf_batch"
    log_path = run_dir / f"{log_stem}_export.log"
    cached = bool(exported_csv_paths(shared_root))
    returncode = 0
    started_at = utc_now()
    export_timeout_seconds = shared_export_timeout_seconds()
    if not cached:
        returncode = run_logged(
            runner,
            ["msprof", "--export=on", f"--output={shared_root}"],
            cwd=run_dir,
            log_path=log_path,
            timeout_seconds=export_timeout_seconds,
            append=True,
        )
    csv_paths = exported_csv_paths(shared_root)
    if returncode != 0 or not csv_paths:
        raise PerfPipelineError(f"shared profile export failed: rc={returncode}")
    exported = [
        {
            "case": int(item.get("case", 0) or 0),
            "sequence_index": int(item.get("sequence_index", index) or index),
            "profile_root": str(shared_root),
            "export_log": str(log_path),
            "started_at": started_at,
            "finished_at": utc_now(),
            "returncode": returncode,
            "cache_hit": cached,
            "op_summary_csv": [str(path) for path in csv_paths],
        }
        for index, item in enumerate(capture.get("cases", []))
        if isinstance(item, dict)
    ]
    manifest.update(
        {
            "cases": exported,
            "shared_profile_root": str(shared_root),
            "batch_profile_root": str(shared_root) if capture_mode == "batched-process" else "",
            "session_profile_root": (
                str(shared_root) if capture_mode == "session-isolated-process" else ""
            ),
            "batch_manifest": str(capture.get("batch_manifest") or ""),
            "session_manifest": str(capture.get("session_manifest") or ""),
            "expected_task_rows_per_case": int(
                capture.get("expected_task_rows_per_case", 0) or 0
            ),
            "timeout_per_case_seconds": max(1, int(timeout_seconds)),
            "export_timeout_seconds": export_timeout_seconds,
            "export_timeout_applied": not cached,
            "finished_at": utc_now(),
            "state": "exported",
        }
    )
    manifest.update(measurement_input_manifest(manifest, run_dir))
    atomic_write_json(manifest_path, manifest)
    return manifest


def parse_profiles(
    *,
    run_dir: Path,
    attack_meta: Path | None,
    baseline: str,
    weighted_target: str,
) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    exported = read_object(run_dir / "PERF_EXPORT.json")
    if exported.get("state") != "exported":
        raise PerfPipelineError("export manifest is not complete")
    input_integrity = verify_measurement_inputs(exported, run_dir)
    times: list[dict[str, Any]] = []
    failures: list[str] = []
    if exported.get("capture_mode") in SHARED_SESSION_CAPTURE_MODES:
        times, failures = parse_batched_cases(exported, run_dir)
    else:
        expected_rows_per_case = int(
            exported.get("expected_task_rows_per_case", 0) or 0
        )
        for raw in exported.get("cases", []):
            if not isinstance(raw, dict):
                failures.append("invalid export case record")
                continue
            case = int(raw.get("case", 0) or 0)
            values: list[float] = []
            for value in raw.get("op_summary_csv", []):
                path = Path(str(value)).resolve()
                ensure_under(path, run_dir)
                values.extend(read_task_durations(path))
            if expected_rows_per_case > 0 and len(values) != expected_rows_per_case:
                failures.append(
                    f"case{case}: Task Duration row count mismatch: "
                    f"expected={expected_rows_per_case} observed={len(values)}"
                )
                continue
            append_case_timing(times, failures, case, values)

    meta = read_optional_object(attack_meta)
    formula, weights = weighting(meta, [int(item["case"]) for item in times])
    weighted = 0.0
    details: list[str] = []
    for index, item in enumerate(times):
        case = int(item["case"])
        value = float(item["time_use_us"])
        weight = float(weights[index]) if index < len(weights) else 1.0
        weighted += value * weight
        details.append(f"case{index + 1}(remote_case{case})={value:.6g}*{weight:g}")
    score_groups = grouped_scores(meta, times)
    target_text = str(weighted_target or "").strip()
    verdict = "BASELINE_ONLY"
    if target_text:
        try:
            verdict = "PASS" if weighted <= float(target_text) else "FAIL"
        except ValueError:
            verdict = "BASELINE_ONLY"
    parse_failed = bool(failures)

    label = str(exported.get("label") or "")
    case_range = str(exported.get("case_range") or "")
    summary_lines = [
        f"AscendOP perf label={label} cases {case_range} baseline={baseline}",
    ]
    for item in times:
        case = int(item["case"])
        value = float(item["time_use_us"])
        count = int(item["sample_count"])
        summary_lines.append(
            f"  case{case} perf PASS: time_use={value:.6f} baseline={baseline} samples={count}"
        )
    summary_lines.extend(f"  perf FAIL: {item}" for item in failures)
    summary_lines.extend(
        [
            f"AscendOP perf cases {case_range}: PASS={len(times)} FAIL={len(failures)}",
            f"weighted_formula={formula}",
            "weighted_time_unit=us",
            f"weighted_detail={'; '.join(details)}",
            f"weighted_time={weighted:.6f}",
        ]
    )
    for name, group in score_groups.items():
        summary_lines.extend(
            [
                f"score_group_{name}_formula={group['formula']}",
                f"score_group_{name}_time_us={group['time_us']:.6f}",
            ]
        )
    if target_text:
        summary_lines.append(f"weighted_target={target_text}")
    summary_lines.append(f"weighted_verdict={verdict}")
    if times and not failures:
        summary_lines.append("Operator performance and accuracy have passed")
    summary = "\n".join(summary_lines) + "\n"
    (run_dir / "PERF_SUMMARY.txt").write_text(summary, encoding="utf-8")
    (run_dir / "times.tsv").write_text(
        "".join(
            f"{int(item['case'])}\t{float(item['time_use_us']):.6f}\n"
            for item in times
        ),
        encoding="utf-8",
    )
    result = {
        "protocol_version": "engine-perf-v1",
        "stage": "parse",
        "state": "failed" if parse_failed else "parsed",
        "label": label,
        "case_range": case_range,
        "case_ids": [int(item["case"]) for item in times],
        "case_count": len(times),
        "expected_task_rows_per_case": int(
            exported.get("expected_task_rows_per_case", 0) or 0
        ),
        "execution_profile": str(exported.get("execution_profile") or ""),
        "stage_sequence": list(exported.get("stage_sequence") or []),
        "capture_before_correctness": exported.get("capture_before_correctness")
        is True,
        "export_before_correctness": exported.get("export_before_correctness")
        is True,
        "measurement_contract_sha256": str(
            exported.get("measurement_contract_sha256") or ""
        ),
        "measurement_pipeline_sha256": str(
            exported.get("measurement_pipeline_sha256") or ""
        ),
        "measurement_input_sha256": input_integrity["measurement_input_sha256"],
        "measurement_input_verified": input_integrity["verified"],
        "weighted_formula": formula,
        "weighted_weights": weights,
        "weighted_time_unit": "us",
        "weighted_time": round(weighted, 6),
        "weighted_target": target_text,
        "weighted_verdict": verdict,
        "score_groups": score_groups,
        "failures": failures,
        "cases": [rounded_case_record(item) for item in times],
        "finished_at": utc_now(),
    }
    atomic_write_json(run_dir / "PERF_PARSE.json", result)
    print(summary, end="")
    if parse_failed:
        raise PerfPipelineError("performance parse failed")
    return result


def parse_batched_cases(
    exported: dict[str, Any], run_dir: Path
) -> tuple[list[dict[str, Any]], list[str]]:
    raw_cases = [item for item in exported.get("cases", []) if isinstance(item, dict)]
    expected_cases = [int(item.get("case", 0) or 0) for item in raw_cases]
    csv_paths: list[Path] = []
    for item in raw_cases:
        for value in item.get("op_summary_csv", []):
            path = Path(str(value)).resolve()
            ensure_under(path, run_dir)
            if path not in csv_paths:
                csv_paths.append(path)
    records: list[dict[str, Any]] = []
    for path in csv_paths:
        records.extend(read_task_records(path))
    expected_op_name = str(exported.get("expected_op_name") or "").strip()
    if expected_op_name:
        records = [
            item
            for item in records
            if profiler_op_matches(
                expected_op_name,
                str(item.get("op_name") or ""),
            )
        ]
    rows_per_case = int(exported.get("expected_task_rows_per_case", 0) or 0)
    groups: list[list[dict[str, Any]]]
    auxiliary_counts: list[int]
    batch_manifest_path = str(exported.get("batch_manifest") or "").strip()
    batch_manifest = None
    if batch_manifest_path:
        path = Path(batch_manifest_path).resolve()
        ensure_under(path, run_dir)
        batch_manifest = read_object(path)
    if batch_manifest and has_supported_profile_call_plans(
        batch_manifest,
        expected_cases,
        rows_per_case,
    ):
        groups, auxiliary_counts = partition_task_call_plans(
            records,
            executions=list(batch_manifest.get("executions") or []),
            expected_cases=expected_cases,
            rows_per_case=rows_per_case,
        )
    else:
        groups = partition_task_invocations(
            records,
            invocation_count=len(expected_cases),
            rows_per_invocation=rows_per_case,
        )
        auxiliary_counts = [0] * len(groups)
    times: list[dict[str, Any]] = []
    failures: list[str] = []
    for case, group, auxiliary_count in zip(
        expected_cases, groups, auxiliary_counts
    ):
        append_case_timing(
            times,
            failures,
            case,
            [float(item["duration_us"]) for item in group],
            block_dim=int(group[0]["block_dim"]),
        )
        if times and int(times[-1].get("case", 0) or 0) == case:
            times[-1]["auxiliary_task_rows_excluded"] = auxiliary_count
    return times, failures


def append_case_timing(
    times: list[dict[str, Any]],
    failures: list[str],
    case: int,
    values: list[float],
    *,
    block_dim: int | None = None,
) -> None:
    if not values:
        failures.append(f"case{case}: no Task Duration(us) values")
        return
    window = values[20:40] if len(values) >= 40 else values
    times.append(
        {
            "case": case,
            "time_use_us": sum(window) / len(window),
            "sample_count": len(values),
            "samples_us": values,
            "window_samples_us": window,
            "window_start": 20 if len(values) >= 40 else 0,
            "window_end": 40 if len(values) >= 40 else len(values),
            "median_us": statistics.median(window),
            "p95_us": percentile(window, 0.95),
            "stddev_us": statistics.pstdev(window) if len(window) > 1 else 0.0,
            "batch_block_dim": block_dim,
        }
    )


def weighting(meta: dict[str, Any], case_ids: list[int]) -> tuple[str, list[float]]:
    formula = str(meta.get("perf_weighted_time_formula") or "")
    raw_weights = meta.get("perf_weighted_time_weights")
    if isinstance(raw_weights, list) and raw_weights:
        try:
            weights = [float(item) for item in raw_weights]
        except (TypeError, ValueError):
            raise PerfPipelineError("invalid configured performance weight list")
        if len(weights) != len(case_ids):
            raise PerfPipelineError(
                "configured performance weight count mismatch: "
                f"cases={len(case_ids)} weights={len(weights)}"
            )
        validate_weights(weights)
        return formula or "configured weights", weights
    if isinstance(raw_weights, dict) and raw_weights:
        try:
            weights = [
                float(raw_weights[f"case{case}"]) for case in case_ids
            ]
        except (KeyError, TypeError, ValueError) as exc:
            raise PerfPipelineError(
                f"configured performance weights do not cover cases {case_ids}"
            ) from exc
        validate_weights(weights)
        return formula or "configured weights", weights
    if len(case_ids) > 5:
        raise PerfPipelineError(
            "performance contracts with more than five cases require explicit "
            "perf_weighted_time_weights"
        )
    if "*20" in formula and "*2" in formula:
        defaults = [20, 2, 1, 1, 1]
        return formula or "case1*20 + case2*2 + case3 + case4 + case5", (
            defaults[: len(case_ids)]
            + [1.0] * max(0, len(case_ids) - len(defaults))
        )
    defaults = [100, 10, 1, 0.02, 0.002]
    return (
        formula or "case1*100 + case2*10 + case3 + case4/50 + case5/500",
        defaults[: len(case_ids)]
        + [1.0] * max(0, len(case_ids) - len(defaults)),
    )


def grouped_scores(
    meta: dict[str, Any], times: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    raw_groups = meta.get("perf_score_groups")
    if raw_groups in (None, {}):
        return {}
    if not isinstance(raw_groups, dict):
        raise PerfPipelineError("perf_score_groups must be an object")
    timing = {int(item["case"]): float(item["time_use_us"]) for item in times}
    groups: dict[str, dict[str, Any]] = {}
    covered: set[int] = set()
    for raw_name, raw_group in raw_groups.items():
        name = str(raw_name or "").strip()
        if not name or not isinstance(raw_group, dict):
            raise PerfPipelineError("invalid perf_score_groups entry")
        raw_ids = raw_group.get("case_ids")
        raw_weights = raw_group.get("weights")
        if not isinstance(raw_ids, list) or not raw_ids:
            raise PerfPipelineError(f"score group {name} has no case_ids")
        try:
            case_ids = [int(item) for item in raw_ids]
        except (TypeError, ValueError) as exc:
            raise PerfPipelineError(f"score group {name} has invalid case_ids") from exc
        if len(case_ids) != len(set(case_ids)) or any(item <= 0 for item in case_ids):
            raise PerfPipelineError(f"score group {name} has duplicate/invalid case_ids")
        if not isinstance(raw_weights, dict):
            raise PerfPipelineError(f"score group {name} weights must be an object")
        try:
            weights = [float(raw_weights[f"case{case_id}"]) for case_id in case_ids]
        except (KeyError, TypeError, ValueError) as exc:
            raise PerfPipelineError(
                f"score group {name} weights do not cover cases {case_ids}"
            ) from exc
        validate_weights(weights)
        missing = [case_id for case_id in case_ids if case_id not in timing]
        if missing:
            raise PerfPipelineError(
                f"score group {name} is missing measured cases {missing}"
            )
        overlap = covered.intersection(case_ids)
        if overlap:
            raise PerfPipelineError(
                f"score group {name} overlaps prior groups at cases {sorted(overlap)}"
            )
        covered.update(case_ids)
        group_time = sum(
            timing[case_id] * weight for case_id, weight in zip(case_ids, weights)
        )
        groups[name] = {
            "formula": str(raw_group.get("formula") or "configured group weights"),
            "case_ids": case_ids,
            "weights": {
                f"case{case_id}": weight
                for case_id, weight in zip(case_ids, weights)
            },
            "time_unit": "us",
            "time_us": round(group_time, 6),
        }
    if covered != set(timing):
        raise PerfPipelineError(
            "perf_score_groups must cover every measured case exactly once"
        )
    return groups


def validate_weights(weights: list[float]) -> None:
    if any(not math.isfinite(item) or item < 0 for item in weights):
        raise PerfPipelineError(
            "configured performance weights must be finite and non-negative"
        )
    if not any(item > 0 for item in weights):
        raise PerfPipelineError(
            "configured performance weights must contain a positive value"
        )


def validate_task_rows_per_case(value: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise PerfPipelineError(f"invalid expected_task_rows_per_case: {value!r}") from exc
    if result <= 0 or result > MAX_TASK_ROWS_PER_CASE:
        raise PerfPipelineError(
            f"expected_task_rows_per_case must be within 1..{MAX_TASK_ROWS_PER_CASE}: {result}"
        )
    return result


def read_task_durations(path: Path) -> list[float]:
    values: list[float] = []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            try:
                values.append(float(row["Task Duration(us)"]))
            except (KeyError, TypeError, ValueError):
                continue
    return values


def read_task_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            try:
                duration = float(row["Task Duration(us)"])
                block_dim_text = str(
                    row.get("Block Dim")
                    if row.get("Block Dim") not in (None, "")
                    else row.get("Block Num", "")
                ).strip()
                block_dim = int(block_dim_text)
            except (KeyError, TypeError, ValueError):
                continue
            records.append(
                {
                    "duration_us": duration,
                    "block_dim": block_dim,
                    "op_name": str(row.get("Op Name") or ""),
                }
            )
    return records


def normalized_op_token(value: str) -> str:
    return "".join(char.lower() for char in str(value) if char.isalnum())


def profiler_op_matches(expected: str, observed: str) -> bool:
    """Match an op type inside CANN profiler composite kernel names."""

    expected_token = normalized_op_token(expected)
    observed_token = normalized_op_token(observed)
    return bool(expected_token) and expected_token in observed_token


def partition_task_invocations(
    records: list[dict[str, Any]],
    *,
    invocation_count: int,
    rows_per_invocation: int,
) -> list[list[dict[str, Any]]]:
    if invocation_count <= 0:
        raise PerfPipelineError("batched profile has no declared invocations")
    if rows_per_invocation <= 0:
        raise PerfPipelineError(
            "batched profile is missing expected_task_rows_per_case"
        )
    expected_rows = invocation_count * rows_per_invocation
    if len(records) != expected_rows:
        raise PerfPipelineError(
            "batched Task Duration row count mismatch: "
            f"expected={expected_rows} observed={len(records)} "
            f"invocations={invocation_count} rows_per_invocation={rows_per_invocation}"
        )
    groups: list[list[dict[str, Any]]] = []
    for invocation_index in range(invocation_count):
        index = invocation_index * rows_per_invocation
        group = records[index : index + rows_per_invocation]
        validate_task_identity_slice(
            group, label=f"batched invocation {invocation_index}"
        )
        groups.append(group)
    return groups


def has_supported_profile_call_plans(
    batch_manifest: dict[str, Any],
    expected_cases: list[int],
    rows_per_case: int,
) -> bool:
    executions = [
        item for item in batch_manifest.get("executions", []) if isinstance(item, dict)
    ]
    if len(executions) != len(expected_cases):
        return False
    for case, execution in zip(expected_cases, executions):
        plan = execution.get("profile_call_plan")
        if int(execution.get("case", 0) or 0) != case or not isinstance(plan, dict):
            return False
        if plan.get("protocol_version") != "engine-profile-call-plan-v1":
            return False
        if plan.get("supported") is not True:
            return False
        if int(plan.get("expected_primary_task_rows", 0) or 0) != rows_per_case:
            return False
    return True


def partition_task_call_plans(
    records: list[dict[str, Any]],
    *,
    executions: list[dict[str, Any]],
    expected_cases: list[int],
    rows_per_case: int,
) -> tuple[list[list[dict[str, Any]]], list[int]]:
    cursor = 0
    groups: list[list[dict[str, Any]]] = []
    auxiliary_counts: list[int] = []
    for case, execution in zip(expected_cases, executions):
        plan = execution.get("profile_call_plan")
        if not isinstance(plan, dict):
            raise PerfPipelineError(f"case{case}: missing profile call plan")
        calls = [item for item in plan.get("calls", []) if isinstance(item, dict)]
        primary_groups: list[list[dict[str, Any]]] = []
        auxiliary_count = 0
        for call in calls:
            count = int(call.get("declared_task_rows", 0) or 0)
            if count <= 0:
                raise PerfPipelineError(
                    f"case{case}: invalid declared profile task rows: {count}"
                )
            end = cursor + count
            if end > len(records):
                raise PerfPipelineError(
                    f"case{case}: profile call plan exceeds observed rows: "
                    f"need={end} observed={len(records)}"
                )
            group = records[cursor:end]
            cursor = end
            validate_task_identity_slice(group, label=f"case{case} call")
            if str(call.get("role") or "") == "primary":
                if count != rows_per_case:
                    raise PerfPipelineError(
                        f"case{case}: primary profile rows mismatch: "
                        f"expected={rows_per_case} observed={count}"
                    )
                primary_groups.append(group)
            else:
                auxiliary_count += count
        if len(primary_groups) != 1:
            raise PerfPipelineError(
                f"case{case}: expected one primary profile call, "
                f"observed={len(primary_groups)}"
            )
        groups.append(primary_groups[0])
        auxiliary_counts.append(auxiliary_count)
    if cursor != len(records):
        raise PerfPipelineError(
            "profile call plan row count mismatch: "
            f"consumed={cursor} observed={len(records)}"
        )
    return groups, auxiliary_counts


def validate_task_identity_slice(
    group: list[dict[str, Any]], *, label: str
) -> None:
    op_names = {str(item.get("op_name") or "") for item in group}
    block_dims = {int(item.get("block_dim", 0) or 0) for item in group}
    if "" in op_names or len(op_names) != 1:
        raise PerfPipelineError(
            f"ambiguous {label} Op Name slice: ops={sorted(op_names)}"
        )
    if len(block_dims) != 1 or next(iter(block_dims), 0) <= 0:
        raise PerfPipelineError(
            f"ambiguous {label} Block Dim slice: dims={sorted(block_dims)}"
        )


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise PerfPipelineError("cannot compute percentile of empty samples")
    ordered = sorted(float(value) for value in values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


def rounded_case_record(item: dict[str, Any]) -> dict[str, Any]:
    record = {
        "case": int(item["case"]),
        "time_use_us": round(float(item["time_use_us"]), 6),
        "sample_count": int(item["sample_count"]),
        "samples_us": [round(float(value), 6) for value in item["samples_us"]],
        "window_samples_us": [
            round(float(value), 6) for value in item["window_samples_us"]
        ],
        "window_start": int(item["window_start"]),
        "window_end": int(item["window_end"]),
        "median_us": round(float(item["median_us"]), 6),
        "p95_us": round(float(item["p95_us"]), 6),
        "stddev_us": round(float(item["stddev_us"]), 6),
    }
    if item.get("batch_block_dim") is not None:
        record["batch_block_dim"] = int(item["batch_block_dim"])
    if item.get("auxiliary_task_rows_excluded") is not None:
        record["auxiliary_task_rows_excluded"] = int(
            item["auxiliary_task_rows_excluded"]
        )
    return record


def exported_csv_paths(case_root: Path) -> list[Path]:
    return sorted(case_root.glob("PROF*/mindstudio_profiler_output/op_summary*.csv"))


def build_measurement_contract(
    *,
    capture_mode: str,
    case_ids: list[int],
    expected_task_rows_per_case: int,
    storage_limit: str,
) -> dict[str, Any]:
    execution_profile = os.environ.get(
        "ASCENDOP_ENGINE_EXECUTION_PROFILE", ""
    ).strip()
    stage_sequence: list[str] = []
    expected_op_name = ""
    job_root_raw = os.environ.get("ASCENDOP_ENGINE_JOB_ROOT", "").strip()
    if job_root_raw:
        spec_path = Path(job_root_raw) / "spec.json"
        if spec_path.is_file():
            spec = read_object(spec_path)
            stage_sequence = [
                str(item.get("name") or "")
                for item in spec.get("stages", [])
                if isinstance(item, dict)
            ]
            spec_profile = str(spec.get("execution_profile") or "")
            expected_op_name = str(spec.get("operator") or "").strip()
            if execution_profile and spec_profile != execution_profile:
                raise PerfPipelineError(
                    "engine execution profile environment/spec mismatch"
                )
            execution_profile = spec_profile
    capture_index = stage_index(stage_sequence, "performance-capture")
    export_index = stage_index(stage_sequence, "profile-export")
    correctness_index = stage_index(stage_sequence, "correctness")
    contract = {
        "protocol_version": "engine-performance-measurement-contract-v1",
        "capture_mode": capture_mode,
        "case_ids": list(case_ids),
        "expected_op_name": expected_op_name,
        "expected_task_rows_per_case": int(expected_task_rows_per_case),
        "expected_task_rows": len(case_ids) * int(expected_task_rows_per_case),
        "storage_limit": str(storage_limit),
        "application_shape": "one-fresh-python-msprof-process-per-case"
        if capture_mode == "isolated-process"
        else capture_mode,
        "parser_window": "middle-20-of-50-or-full-declared-window",
    }
    return {
        "execution_profile": execution_profile,
        "stage_sequence": stage_sequence,
        "capture_before_correctness": (
            capture_index >= 0
            and correctness_index >= 0
            and capture_index < correctness_index
        ),
        "export_before_correctness": (
            export_index >= 0
            and correctness_index >= 0
            and export_index < correctness_index
        ),
        "measurement_contract": contract,
        "measurement_contract_sha256": canonical_object_sha256(contract),
        "measurement_pipeline_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
    }


def stage_index(stage_sequence: list[str], name: str) -> int:
    try:
        return stage_sequence.index(name)
    except ValueError:
        return -1


def measurement_input_manifest(
    exported: dict[str, Any], run_dir: Path
) -> dict[str, Any]:
    paths: list[Path] = []
    for item in exported.get("cases", []):
        if not isinstance(item, dict):
            continue
        for value in item.get("op_summary_csv", []):
            path = Path(str(value)).resolve()
            ensure_under(path, run_dir)
            if path not in paths:
                paths.append(path)
    entries: list[dict[str, Any]] = []
    for path in sorted(paths, key=lambda item: item.as_posix()):
        if not path.is_file():
            raise PerfPipelineError(f"exported measurement input is missing: {path}")
        entries.append(
            {
                "path": path.relative_to(run_dir).as_posix(),
                "size": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    if not entries:
        raise PerfPipelineError("exported measurement input manifest is empty")
    return {
        "measurement_inputs": entries,
        "measurement_input_sha256": canonical_object_sha256(entries),
    }


def verify_measurement_inputs(
    exported: dict[str, Any], run_dir: Path
) -> dict[str, Any]:
    expected = str(exported.get("measurement_input_sha256") or "")
    execution_profile = str(exported.get("execution_profile") or "")
    if not expected:
        if execution_profile in PERFORMANCE_FIRST_PROFILES:
            raise PerfPipelineError(
                "performance-first export is missing measurement input integrity"
            )
        return {"measurement_input_sha256": "", "verified": False}
    observed = measurement_input_manifest(exported, run_dir)
    if observed["measurement_input_sha256"] != expected:
        raise PerfPipelineError(
            "exported measurement inputs changed before profile parse"
        )
    if list(exported.get("measurement_inputs") or []) != observed["measurement_inputs"]:
        raise PerfPipelineError(
            "exported measurement input manifest changed before profile parse"
        )
    return {"measurement_input_sha256": expected, "verified": True}


def canonical_object_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_log_tail(path: Path, *, max_lines: int = 40, max_chars: int = 6000) -> str:
    if not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()[-max(1, max_lines) :]
    return " | ".join(lines)[-max(1, max_chars) :]


def run_logged(
    runner: Runner,
    command: list[str],
    *,
    cwd: Path,
    log_path: Path,
    timeout_seconds: int,
    append: bool = False,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with log_path.open(mode, encoding="utf-8") as log:
        log.write("COMMAND=" + shlex.join(command) + "\n")
        log.flush()
        if runner is subprocess.run:
            process = subprocess.Popen(
                command,
                cwd=str(cwd),
                env=os.environ.copy(),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                creationflags=hidden_process_creation_flags(),
                startupinfo=hidden_process_startup_info(),
                start_new_session=os.name != "nt",
            )
            try:
                return int(process.wait(timeout=timeout_seconds))
            except subprocess.TimeoutExpired:
                log.write(f"TIMEOUT seconds={timeout_seconds}\n")
                log.flush()
                terminate_logged_process(process)
                return 124
        try:
            completed = runner(
                command,
                cwd=str(cwd),
                env=os.environ.copy(),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_seconds,
                check=False,
                creationflags=hidden_process_creation_flags(),
                startupinfo=hidden_process_startup_info(),
            )
        except subprocess.TimeoutExpired:
            log.write(f"TIMEOUT seconds={timeout_seconds}\n")
            return 124
    return int(completed.returncode)


def terminate_logged_process(
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


def run_msprof_capture(
    runner: Runner,
    *,
    application: list[str],
    output_root: Path,
    storage_limit: str,
    cwd: Path,
    log_path: Path,
    timeout_seconds: int,
) -> int:
    common = [
        "msprof",
        f"--storage-limit={storage_limit}",
        f"--output={output_root}",
    ]
    direct_command = [*common, *application]
    returncode = run_logged(
        runner,
        direct_command,
        cwd=cwd,
        log_path=log_path,
        timeout_seconds=timeout_seconds,
    )
    if returncode == 0 or not msprof_rejected_application_syntax(log_path):
        return returncode

    legacy_command = [
        "msprof",
        f"--storage-limit={storage_limit}",
        f"--application={shlex.join(application)}",
        f"--output={output_root}",
    ]
    with log_path.open("a", encoding="utf-8") as log:
        log.write("MSPROF_APPLICATION_SYNTAX_FALLBACK=legacy-application-option\n")
    return run_logged(
        runner,
        legacy_command,
        cwd=cwd,
        log_path=log_path,
        timeout_seconds=timeout_seconds,
        append=True,
    )


def msprof_rejected_application_syntax(log_path: Path) -> bool:
    if not log_path.is_file():
        return False
    text = log_path.read_text(encoding="utf-8", errors="replace").lower()
    markers = (
        "script params are invalid",
        "unrecognized arguments",
        "unrecognized option",
        "unknown option",
    )
    return any(marker in text for marker in markers)


def shared_capture_timeout_seconds(timeout_per_case_seconds: int, case_count: int) -> int:
    if timeout_per_case_seconds < 1 or case_count < 1:
        raise PerfPipelineError("shared capture timeout requires positive inputs")
    try:
        process_timeout_seconds = int(
            os.environ.get("ASCENDOP_PROFILE_PROCESS_TIMEOUT_SECONDS", "210")
        )
    except ValueError as exc:
        raise PerfPipelineError(
            "ASCENDOP_PROFILE_PROCESS_TIMEOUT_SECONDS must be an integer"
        ) from exc
    if not 30 <= process_timeout_seconds <= 300:
        raise PerfPipelineError(
            "ASCENDOP_PROFILE_PROCESS_TIMEOUT_SECONDS must be within 30..300"
        )
    aggregate_timeout = int(timeout_per_case_seconds) * int(case_count) + 30
    return min(aggregate_timeout, process_timeout_seconds)


def shared_export_timeout_seconds() -> int:
    try:
        export_timeout_seconds = int(
            os.environ.get("ASCENDOP_PROFILE_EXPORT_TIMEOUT_SECONDS", "60")
        )
    except ValueError as exc:
        raise PerfPipelineError(
            "ASCENDOP_PROFILE_EXPORT_TIMEOUT_SECONDS must be an integer"
        ) from exc
    if not 30 <= export_timeout_seconds <= 90:
        raise PerfPipelineError(
            "ASCENDOP_PROFILE_EXPORT_TIMEOUT_SECONDS must be within 30..90"
        )
    return export_timeout_seconds
def scaled_batched_storage_limit(
    requested: str,
    *,
    case_count: int,
    expected_task_rows_per_case: int,
) -> tuple[str, dict[str, Any]]:
    if case_count < 1 or expected_task_rows_per_case < 1:
        raise PerfPipelineError("batched profile storage budget requires positive inputs")
    expected_task_rows = int(case_count) * int(expected_task_rows_per_case)
    estimated_minimum_mb = max(
        BATCH_PROFILE_STORAGE_MIN_MB,
        expected_task_rows * BATCH_PROFILE_STORAGE_MB_PER_TASK_ROW,
    )
    try:
        maximum_mb = int(
            os.environ.get(
                "ASCENDOP_MAX_PROFILE_STORAGE_MB",
                str(BATCH_PROFILE_STORAGE_DEFAULT_MAX_MB),
            )
        )
    except ValueError as exc:
        raise PerfPipelineError(
            "ASCENDOP_MAX_PROFILE_STORAGE_MB must be an integer"
        ) from exc
    if maximum_mb < BATCH_PROFILE_STORAGE_MIN_MB:
        raise PerfPipelineError(
            "ASCENDOP_MAX_PROFILE_STORAGE_MB is below the minimum profile budget"
        )
    if estimated_minimum_mb > maximum_mb:
        raise PerfPipelineError(
            "batched profile contract exceeds node storage budget: "
            f"required_mb={estimated_minimum_mb} maximum_mb={maximum_mb}"
        )

    requested_text = str(requested or "").strip()
    match = re.fullmatch(r"([1-9][0-9]*)\s*(MB|GB)", requested_text, re.IGNORECASE)
    if match is None:
        raise PerfPipelineError(
            f"unsupported profile storage limit: {requested_text or '<empty>'}"
        )
    requested_mb = int(match.group(1))
    if match.group(2).upper() == "GB":
        requested_mb *= 1024
    effective_mb = max(requested_mb, estimated_minimum_mb)
    if effective_mb > maximum_mb:
        raise PerfPipelineError(
            "requested profile storage exceeds node budget: "
            f"requested_mb={effective_mb} maximum_mb={maximum_mb}"
        )
    return (
        f"{effective_mb}MB",
        {
            "policy": "task-row-scaled-v1",
            "requested": requested_text,
            "requested_mb": requested_mb,
            "effective": f"{effective_mb}MB",
            "effective_mb": effective_mb,
            "estimated_minimum_mb": estimated_minimum_mb,
            "maximum_mb": maximum_mb,
            "case_count": int(case_count),
            "expected_task_rows_per_case": int(expected_task_rows_per_case),
            "expected_task_rows": expected_task_rows,
            "mb_per_task_row": BATCH_PROFILE_STORAGE_MB_PER_TASK_ROW,
        },
    )


def read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise PerfPipelineError(f"cannot read manifest: {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise PerfPipelineError(f"manifest must be an object: {path}")
    return raw


def read_optional_object(path: Path | None) -> dict[str, Any]:
    if path is None or not str(path) or not path.is_file():
        return {}
    return read_object(path)


def ensure_under(path: Path, root: Path) -> None:
    resolved = path.resolve()
    base = root.resolve()
    if resolved != base and base not in resolved.parents:
        raise PerfPipelineError(f"path escapes run directory: {path}")


def safe_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char in "._-" else "_" for char in value)
    token = token.strip("._-")
    if not token:
        raise PerfPipelineError("invalid performance label")
    return token


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Durable split msprof pipeline")
    sub = parser.add_subparsers(dest="action", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--task-case", type=Path, required=True)
    capture.add_argument("--run-dir", type=Path, required=True)
    capture.add_argument("--label", required=True)
    capture.add_argument("--case-range", required=True)
    capture.add_argument("--python-bin", required=True)
    capture.add_argument("--storage-limit", default="200MB")
    capture.add_argument("--timeout-seconds", type=int, default=330)
    capture.add_argument("--batch-process", action="store_true")
    capture.add_argument("--session-isolated-process", action="store_true")
    capture.add_argument(
        "--expected-task-rows-per-case",
        type=int,
        default=DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE,
    )
    export = sub.add_parser("export")
    export.add_argument("--run-dir", type=Path, required=True)
    export.add_argument("--timeout-seconds", type=int, default=330)
    parse = sub.add_parser("parse")
    parse.add_argument("--run-dir", type=Path, required=True)
    parse.add_argument("--attack-meta", type=Path)
    parse.add_argument("--baseline", default="9999999999999")
    parse.add_argument("--weighted-target", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.action == "capture":
            capture_profiles(
                task_case=args.task_case,
                run_dir=args.run_dir,
                label=args.label,
                case_range=args.case_range,
                python_bin=args.python_bin,
                storage_limit=args.storage_limit,
                timeout_seconds=args.timeout_seconds,
                batch_process=args.batch_process,
                session_isolated_process=args.session_isolated_process,
                expected_task_rows_per_case=args.expected_task_rows_per_case,
            )
        elif args.action == "export":
            export_profiles(run_dir=args.run_dir, timeout_seconds=args.timeout_seconds)
        else:
            parse_profiles(
                run_dir=args.run_dir,
                attack_meta=args.attack_meta,
                baseline=args.baseline,
                weighted_target=args.weighted_target,
            )
    except PerfPipelineError as exc:
        print(f"PERF_PIPELINE_FAILED: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
