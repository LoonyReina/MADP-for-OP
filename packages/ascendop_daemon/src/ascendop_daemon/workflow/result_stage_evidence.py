from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any, Mapping


STAGE_EVIDENCE_SCHEMA = "ascendop.result-stage-evidence.v1"


def summarize_result_stage_evidence(output_root: Path) -> dict[str, Any]:
    root = output_root.resolve()
    correctness_path, correctness = _first_object(root, "CORRECTNESS_BATCH.json")
    performance_path, performance = _first_object(root, "PERF_BATCH.json")
    capture_path, capture = _first_object(root, "PERF_CAPTURE.json")
    parse_path, parsed = _first_object(root, "PERF_PARSE.json")
    terminal_path, terminal = _first_object(root, "terminal.json")
    _spec_path, spec = _first_object(root, "spec.json")

    correctness_summary = _batch_summary(correctness, expected_mode="correctness")
    performance_batch_summary = _batch_summary(
        performance,
        expected_mode="performance",
    )
    capture_summary = _capture_summary(capture)
    parse_summary = {
        "status": "passed" if parsed else "missing",
        "path": _relative(root, parse_path),
    }
    primary_failure = terminal.get("primary_failure", {})
    if not isinstance(primary_failure, Mapping):
        primary_failure = {}
    if not primary_failure:
        history = terminal.get("history", [])
        failed = [
            item
            for item in history
            if isinstance(item, Mapping) and int(item.get("exit_code", 0) or 0) != 0
        ] if isinstance(history, list) else []
        primary_failure = dict(failed[0]) if failed else {}
    failed_stage = str(primary_failure.get("stage_name") or "")
    if failed_stage in {"profile-parse", "result-assemble"} and not parsed:
        parse_summary["status"] = "failed"
    parse_summary["failed_stage"] = failed_stage

    correctness_summary["path"] = _relative(root, correctness_path)
    performance_batch_summary["path"] = _relative(root, performance_path)
    capture_summary["path"] = _relative(root, capture_path)
    device_facts_complete = correctness_summary["status"] == "passed"
    capture_complete = (
        performance_batch_summary["status"] == "passed"
        and capture_summary["status"] == "passed"
    )
    postprocess_recovery_required = (
        device_facts_complete
        and capture_complete
        and parse_summary["status"] != "passed"
    )
    recovery_contract = _recovery_contract(
        spec,
        terminal,
        failed_stage=failed_stage,
        enabled=postprocess_recovery_required,
    )
    return {
        "schema": STAGE_EVIDENCE_SCHEMA,
        "correctness": correctness_summary,
        "performance_batch": performance_batch_summary,
        "performance_capture": capture_summary,
        "performance_parse": parse_summary,
        "terminal_path": _relative(root, terminal_path),
        "primary_failure_stage": failed_stage,
        "postprocess_recovery_required": postprocess_recovery_required,
        "device_reexecution_allowed": False if device_facts_complete else None,
        "postprocess_recovery_contract": recovery_contract,
    }


def _recovery_contract(
    spec: Mapping[str, Any],
    terminal: Mapping[str, Any],
    *,
    failed_stage: str,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled or not spec or not terminal or not failed_stage:
        return {}
    stages = spec.get("stages", [])
    if not isinstance(stages, list):
        return {}
    by_name = {
        str(stage.get("name") or ""): (index, stage)
        for index, stage in enumerate(stages)
        if isinstance(stage, Mapping) and str(stage.get("name") or "")
    }
    if failed_stage not in by_name:
        return {}
    selected = {failed_stage}
    changed = True
    while changed:
        changed = False
        for name, (_index, stage) in by_name.items():
            dependencies = {
                str(item) for item in stage.get("depends_on", [])
            }
            if name not in selected and dependencies & selected:
                selected.add(name)
                changed = True
    ordered = [
        name
        for name, (index, _stage) in sorted(
            by_name.items(), key=lambda item: item[1][0]
        )
        if name in selected
    ]
    if not ordered or len(ordered) > 8:
        return {}
    for name in ordered:
        _index, stage = by_name[name]
        if str(stage.get("resource") or "") == "device" or not bool(
            stage.get("idempotent")
        ):
            return {}
    terminal_bytes = json.dumps(
        terminal,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "schema": "ascendop.result-postprocess-recovery-contract.v1",
        "engine_job_id": str(
            terminal.get("engine_job_id") or spec.get("engine_job_id") or ""
        ),
        "terminal_digest": hashlib.sha256(terminal_bytes).hexdigest(),
        "terminal_revision": int(terminal.get("terminal_revision", 0) or 0),
        "stages": ordered,
        "max_stage_attempts": 1,
    }


def _batch_summary(raw: Mapping[str, Any], *, expected_mode: str) -> dict[str, Any]:
    if not raw:
        return _empty_summary("missing")
    try:
        mode = str(raw.get("mode") or "")
        case_ids = [int(item) for item in raw.get("case_ids", [])]
        repetitions = int(raw.get("repetitions", 0) or 0)
        executions = raw.get("executions", [])
        if not isinstance(executions, list):
            return _empty_summary("invalid")
        expected = int(
            raw.get("expected_execution_count", len(case_ids) * repetitions) or 0
        )
        executed = int(raw.get("execution_count", len(executions)) or 0)
        verdicts = [
            str(item.get("verdict") or "")
            for item in executions
            if isinstance(item, Mapping)
        ]
    except (TypeError, ValueError):
        return _empty_summary("invalid")
    passed = sum(value == "PASS" for value in verdicts)
    failed = sum(value == "FAIL" for value in verdicts)
    complete = (
        mode == expected_mode
        and expected > 0
        and executed == expected
        and len(executions) == expected
        and len(verdicts) == expected
        and passed + failed == expected
    )
    status = "passed" if complete and failed == 0 else "failed" if complete else "incomplete"
    return {
        "status": status,
        "expected": expected,
        "executed": executed,
        "passed": passed,
        "failed": failed,
        "case_count": len(case_ids),
        "repetitions": repetitions,
    }


def _capture_summary(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not raw:
        return _empty_summary("missing")
    cases = raw.get("cases", [])
    if not isinstance(cases, list):
        return _empty_summary("invalid")
    expected = int(raw.get("case_count", 0) or 0)
    returncodes = [
        int(item.get("returncode", 1) or 0)
        for item in cases
        if isinstance(item, Mapping)
    ]
    complete = expected > 0 and len(cases) == expected and len(returncodes) == expected
    passed = sum(value == 0 for value in returncodes)
    failed = len(returncodes) - passed
    return {
        "status": "passed" if complete and not failed else "failed" if complete else "incomplete",
        "expected": expected,
        "executed": len(cases),
        "passed": passed,
        "failed": failed,
    }


def _empty_summary(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "expected": 0,
        "executed": 0,
        "passed": 0,
        "failed": 0,
    }


def _first_object(root: Path, filename: str) -> tuple[Path | None, dict[str, Any]]:
    for path in sorted(root.rglob(filename)):
        if not path.is_file():
            continue
        try:
            value = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            return path, value
    return None, {}


def _relative(root: Path, path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)
