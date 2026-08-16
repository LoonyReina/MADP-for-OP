from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

from limited_remote_partner.engine.test_engine import atomic_write_json, utc_now


class BatchCaseRunnerError(RuntimeError):
    pass


MAX_BATCH_CASE_COUNT = 512
MAX_BATCH_EXECUTIONS_PER_CASE = 10000
PROFILE_CALL_PLAN_PROTOCOL = "engine-profile-call-plan-v1"


def declared_profile_rows(args: tuple[Any, ...], kwargs: dict[str, Any]) -> int:
    for name in ("profile_rounds", "rounds", "repeats"):
        value = kwargs.get(name)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return int(value)
    if args:
        value = args[-1]
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return int(value)
    return 0


@contextlib.contextmanager
def observe_profile_calls(module: Any, expected_rows: int):
    calls: list[dict[str, Any]] = []
    custom_ops_lib = getattr(module, "custom_ops_lib", None)
    original = getattr(custom_ops_lib, "custom_op", None)
    enabled = expected_rows > 1 and callable(original)
    if not enabled:
        yield calls
        return

    def observed_custom_op(*args: Any, **kwargs: Any) -> Any:
        rows = declared_profile_rows(args, kwargs)
        record = {
            "sequence_index": len(calls),
            "declared_task_rows": rows,
            "started_at": utc_now(),
        }
        calls.append(record)
        try:
            return original(*args, **kwargs)
        finally:
            record["finished_at"] = utc_now()

    setattr(custom_ops_lib, "custom_op", observed_custom_op)
    try:
        yield calls
    finally:
        setattr(custom_ops_lib, "custom_op", original)


def profile_call_plan(calls: list[dict[str, Any]], expected_rows: int) -> dict[str, Any]:
    primary_indexes = [
        index
        for index, item in enumerate(calls)
        if int(item.get("declared_task_rows", 0) or 0) == expected_rows
    ]
    supported = expected_rows > 1 and len(primary_indexes) == 1 and all(
        1 <= int(item.get("declared_task_rows", 0) or 0)
        <= MAX_BATCH_EXECUTIONS_PER_CASE
        for item in calls
    )
    primary_index = primary_indexes[0] if supported else -1
    normalized_calls = []
    for index, item in enumerate(calls):
        normalized_calls.append(
            {
                **item,
                "role": "primary" if index == primary_index else "auxiliary",
            }
        )
    return {
        "protocol_version": PROFILE_CALL_PLAN_PROTOCOL,
        "supported": supported,
        "expected_primary_task_rows": expected_rows,
        "primary_call_count": len(primary_indexes),
        "declared_total_task_rows": sum(
            int(item.get("declared_task_rows", 0) or 0) for item in calls
        ),
        "calls": normalized_calls,
    }


def parse_cases(value: str) -> list[int]:
    raw = str(value or "").strip()
    if ".." in raw:
        left, right = raw.split("..", 1)
        start = int(left)
        finish = int(right)
        if start <= 0 or finish < start:
            raise BatchCaseRunnerError(f"invalid case range: {value}")
        values = list(range(start, finish + 1))
    else:
        values = [int(item) for item in raw.replace(",", " ").split()]
    if not values or any(item <= 0 for item in values):
        raise BatchCaseRunnerError(f"invalid case range: {value}")
    if len(values) != len(set(values)):
        raise BatchCaseRunnerError(f"duplicate case id: {value}")
    if len(values) > MAX_BATCH_CASE_COUNT:
        raise BatchCaseRunnerError(
            f"case count exceeds {MAX_BATCH_CASE_COUNT}: {len(values)}"
        )
    return values


def run_batch_cases(
    *,
    task_case: Path,
    case_range: str,
    repetitions: int,
    output_path: Path,
    log_root: Path,
    mode: str,
    expected_task_rows_per_case: int = 0,
) -> dict[str, Any]:
    task_case = task_case.resolve()
    output_path = output_path.resolve()
    log_root = log_root.resolve()
    test_op = task_case / "test_op.py"
    if not test_op.is_file():
        raise BatchCaseRunnerError(f"test_op.py is missing: {test_op}")
    if repetitions < 1 or repetitions > MAX_BATCH_EXECUTIONS_PER_CASE:
        raise BatchCaseRunnerError(
            f"repetitions must be within 1..{MAX_BATCH_EXECUTIONS_PER_CASE}"
        )
    if mode not in {"correctness", "performance"}:
        raise BatchCaseRunnerError(f"invalid batch mode: {mode}")
    if mode == "performance" and not (
        1 <= expected_task_rows_per_case <= MAX_BATCH_EXECUTIONS_PER_CASE
    ):
        raise BatchCaseRunnerError(
            "performance mode requires expected_task_rows_per_case within "
            f"1..{MAX_BATCH_EXECUTIONS_PER_CASE}"
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)

    case_ids = parse_cases(case_range)
    previous_cwd = Path.cwd()
    previous_path = list(sys.path)
    try:
        os.chdir(task_case)
        sys.path.insert(0, str(task_case))
        cache_entry = os.environ.get("ASCENDOP_CASE_CACHE_ENTRY", "").strip()
        if cache_entry:
            from limited_remote_partner.resources.case_cache import load_cached_test_module

            module = load_cached_test_module(test_op)
        else:
            module = load_test_module(test_op)
        case_class = getattr(module, "TestCustomOP", None)
        if case_class is None:
            raise BatchCaseRunnerError("test_op.py does not define TestCustomOP")
        records: list[dict[str, Any]] = []
        manifest: dict[str, Any] = {
            "protocol_version": "engine-batch-case-v1",
            "mode": mode,
            "case_range": case_range,
            "case_ids": case_ids,
            "case_count": len(case_ids),
            "repetitions": repetitions,
            "expected_execution_count": len(case_ids) * repetitions,
            "started_at": utc_now(),
            "executions": records,
        }
        if mode == "performance":
            manifest["expected_task_rows_per_case"] = expected_task_rows_per_case
        if cache_entry:
            from limited_remote_partner.resources.case_cache import load_cache_manifest

            cache_manifest = load_cache_manifest(Path(cache_entry))
            manifest["case_cache"] = {
                "protocol_version": cache_manifest.get("protocol_version"),
                "cache_key": cache_manifest.get("cache_key"),
                "operator": cache_manifest.get("operator"),
            }
        for case in case_ids:
            for repetition in range(1, repetitions + 1):
                log_path = log_root / f"case{case}" / f"repeat{repetition}.log"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                output = io.StringIO()
                started = time.monotonic()
                started_at = utc_now()
                error = ""
                observed_calls: list[dict[str, Any]] = []
                with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                    try:
                        if cache_entry:
                            from limited_remote_partner.resources.case_cache import set_active_case

                            set_active_case(module, case)
                        instance = case_class()
                        method = getattr(instance, "test_custom_op_case")
                        with observe_profile_calls(
                            module,
                            expected_task_rows_per_case if mode == "performance" else 0,
                        ) as observed_calls:
                            method(str(case))
                    except (Exception, SystemExit):
                        # Some official test_op.py entrypoints use SystemExit to
                        # report a deterministic case failure. Keep the shared
                        # process alive so the manifest records the exact case
                        # verdict instead of misclassifying it as engine infra.
                        error = traceback.format_exc()
                        print(error, end="")
                text = output.getvalue()
                log_path.write_text(text, encoding="utf-8")
                passed = (
                    not error
                    and "verify result pass" in text
                    and "verify result failed" not in text
                    and "[ERROR]" not in text
                )
                record = {
                    "case": case,
                    "repetition": repetition,
                    "sequence_index": len(records),
                    "started_at": started_at,
                    "finished_at": utc_now(),
                    "duration_seconds": round(max(0.0, time.monotonic() - started), 6),
                    "verdict": "PASS" if passed else "FAIL",
                    "log": str(log_path),
                    "error": error.splitlines()[-1] if error else "",
                }
                if mode == "performance":
                    record["profile_call_plan"] = profile_call_plan(
                        observed_calls,
                        expected_task_rows_per_case,
                    )
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
            }
        )
        atomic_write_json(output_path, manifest)
        return manifest
    finally:
        os.chdir(previous_cwd)
        sys.path[:] = previous_path


def load_test_module(path: Path) -> Any:
    module_name = f"ascendop_engine_test_op_{os.getpid()}_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise BatchCaseRunnerError(f"cannot load test module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run AscendOP cases in one Python/NPU process")
    parser.add_argument("--task-case", type=Path, required=True)
    parser.add_argument("--case-range", required=True)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--log-root", type=Path, required=True)
    parser.add_argument("--mode", choices=["correctness", "performance"], required=True)
    parser.add_argument("--expected-task-rows-per-case", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_batch_cases(
            task_case=args.task_case,
            case_range=args.case_range,
            repetitions=args.repetitions,
            output_path=args.output,
            log_root=args.log_root,
            mode=args.mode,
            expected_task_rows_per_case=args.expected_task_rows_per_case,
        )
    except (BatchCaseRunnerError, OSError, ValueError) as exc:
        print(f"BATCH_CASE_RUNNER_ERROR: {exc}", file=sys.stderr)
        return 2
    return 0 if result.get("state") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
