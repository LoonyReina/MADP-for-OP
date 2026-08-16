from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.core.filesystem import filesystem_path


PROFILER_INDEX_PROTOCOL = "ascendop-profiler-evidence-index-v1"
PROFILER_INDEX_FILE = "PROFILER_EVIDENCE_INDEX.json"


class ProfilerRequestStateError(RuntimeError):
    pass


def sync_profiler_index(root: Path, state: dict[str, Any]) -> None:
    targets = [item for item in state.get("targets", []) if isinstance(item, dict)]
    index = {
        "protocol_version": PROFILER_INDEX_PROTOCOL,
        "operator": str(state.get("operator") or ""),
        "case_version": str(state.get("case_version") or ""),
        "blocker_result_version": str(state.get("blocker_result_version") or ""),
        "blocker_generation": str(state.get("blocker_generation") or ""),
        "generation_digest": str(state.get("generation_digest") or ""),
        "request_sha256": str(state.get("request_sha256") or ""),
        "request_attempt": int(state.get("request_attempt", 1) or 1),
        "retry_release_generation": str(state.get("retry_release_generation") or ""),
        "retry_authorized_at": str(state.get("retry_authorized_at") or ""),
        "profiler_execution_contract_digest": str(
            state.get("profiler_execution_contract_digest") or ""
        ),
        "request_state_path": str(state.get("request_state_path") or ""),
        "status": str(state.get("status") or ""),
        "cases": list(state.get("cases", [])),
        "requested_profiler_mode": str(state.get("requested_profiler_mode") or ""),
        "typed_solver_diagnostic": bool(state.get("typed_solver_diagnostic", False)),
        "targets": [
            {
                key: target.get(key, "")
                for key in (
                    "test_version",
                    "source_sha256",
                    "status",
                    "engine_job_id",
                    "engine_state",
                    "evidence_path",
                    "evidence_status",
                    "last_error",
                )
            }
            for target in targets
        ],
        "created_at": str(state.get("created_at") or ""),
        "updated_at": str(state.get("updated_at") or _utc_now_iso()),
    }
    path = (
        case_directory(root, str(state["operator"]), str(state["case_version"]))
        / PROFILER_INDEX_FILE
    )
    write_json_atomic(path, index, ensure_ascii=True, sort_keys=True)


def profiler_state_path(
    root: Path,
    operator: str,
    case_version: str,
    blocker_generation: str,
) -> Path:
    return (
        root
        / "TestUtils"
        / "tester_daemon"
        / "profiler_requests"
        / operator
        / case_version
        / _generation_digest(blocker_generation)
        / "request.json"
    )


def case_directory(root: Path, operator: str, case_version: str) -> Path:
    return root / "TestUtils" / "casegen" / operator / "case" / case_version


def archived_submit_snapshot(
    root: Path,
    operator: str,
    test_version: str,
) -> Path:
    snapshot = root / "operators_testresult" / operator / test_version / "submit_snapshot"
    required = (
        snapshot / "pending_snapshot" / "source_snapshot",
        snapshot / "task_case",
    )
    if not all(path.is_dir() for path in required):
        raise ProfilerRequestStateError(
            f"archived submit snapshot is incomplete: {operator}/{test_version}"
        )
    return snapshot


def available_case_ids(
    root: Path,
    operator: str,
    case_version: str,
    target_version: str,
) -> list[int]:
    paths = (
        case_directory(root, operator, case_version) / "meta.json",
        archived_submit_snapshot(root, operator, target_version)
        / "attack_case"
        / "meta.json",
    )
    for path in paths:
        raw = _read_object(path)
        buckets = raw.get("buckets") if raw else None
        if isinstance(buckets, list) and buckets:
            return list(range(1, len(buckets) + 1))
        for field in ("default_perf_case_range", "default_correctness_range"):
            if raw.get(field):
                parsed = _parse_case_range(str(raw[field]))
                if parsed:
                    return parsed
    test_op = (
        archived_submit_snapshot(root, operator, target_version)
        / "task_case"
        / "test_op.py"
    )
    if test_op.is_file():
        text = test_op.read_text(encoding="utf-8-sig", errors="replace")
        ids = sorted({int(value) for value in re.findall(r"['\"]case(\d+)['\"]", text)})
        if ids:
            return ids
    return list(range(1, 17))


def observe_profiler_request(
    root: Path,
    *,
    operator: str,
    case_version: str,
    result_version: str,
    blocker_generation: str,
) -> dict[str, Any]:
    """Observe the exact durable profiler registration without importing legacy."""

    base = (
        root.resolve()
        / "TestUtils"
        / "tester_daemon"
        / "profiler_requests"
        / operator
        / case_version
    )
    matches: list[tuple[str, Path, dict[str, Any]]] = []
    for path in sorted(base.glob("*/request.json")):
        value = _read_object(path)
        if not value:
            continue
        targets = value.get("targets")
        target_versions = (
            {
                str(target.get("test_version") or "")
                for target in targets
                if isinstance(target, dict)
            }
            if isinstance(targets, list)
            else set()
        )
        if (
            str(value.get("operator") or "") != operator
            or str(value.get("case_version") or "") != case_version
            or str(value.get("blocker_generation") or "") != blocker_generation
            or (
                str(value.get("blocker_result_version") or "") != result_version
                and result_version not in target_versions
            )
        ):
            continue
        matches.append((str(value.get("created_at") or ""), path, value))
    if not matches:
        return {
            "status": "missing",
            "operator": operator,
            "case_version": case_version,
            "result_version": result_version,
            "blocker_generation": blocker_generation,
            "error": "exact profiler request state is missing",
        }
    if len(matches) != 1:
        return {
            "status": "collision",
            "operator": operator,
            "case_version": case_version,
            "result_version": result_version,
            "blocker_generation": blocker_generation,
            "error": "multiple profiler states match one blocker identity",
            "paths": [path.relative_to(root.resolve()).as_posix() for _, path, _ in matches],
        }
    _created_at, path, value = matches[0]
    return {
        "status": str(value.get("status") or "unknown"),
        "operator": operator,
        "case_version": case_version,
        "result_version": result_version,
        "blocker_generation": blocker_generation,
        "request_state_path": path.relative_to(root.resolve()).as_posix(),
        "request_sha256": str(value.get("request_sha256") or ""),
        "request_id": _active_request_id(value),
        "error": str(value.get("last_error") or ""),
    }


def _active_request_id(value: dict[str, Any]) -> str:
    targets = value.get("targets")
    if not isinstance(targets, list):
        return ""
    active = [
        str(target.get("flow_v3_request_id") or "")
        for target in targets
        if isinstance(target, dict)
        and str(target.get("status") or "")
        in {"enqueued", "running", "returned-awaiting-ingest", "complete"}
    ]
    return next((request_id for request_id in active if request_id), "")


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(filesystem_path(path).read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _generation_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _parse_case_range(value: str) -> list[int]:
    try:
        if ".." in value:
            left, right = value.split("..", 1)
            return list(range(int(left), int(right) + 1))
        return [int(item) for item in re.findall(r"\d+", value)]
    except ValueError:
        return []


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


__all__ = [
    "PROFILER_INDEX_FILE",
    "PROFILER_INDEX_PROTOCOL",
    "ProfilerRequestStateError",
    "archived_submit_snapshot",
    "available_case_ids",
    "case_directory",
    "observe_profiler_request",
    "profiler_state_path",
    "sync_profiler_index",
]
