from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.filesystem import filesystem_path
from ascendop_daemon.workflow.profiler_request_index import ProfilerRequestStateError


PROFILER_MEASUREMENT_COMPONENTS = {
    "daemon.diagnostic_request_materializer": (
        "daemon",
        "ascendop_daemon/control_plane/test_requests.py",
    ),
    "daemon.diagnostic_intake": (
        "daemon",
        "ascendop_daemon/runtime/diagnostic_intake.py",
    ),
    "daemon.operator_job_builder": (
        "daemon",
        "ascendop_daemon/workflow/operator_job_builder.py",
    ),
    "daemon.profiler_evidence_runner": (
        "daemon",
        "ascendop_daemon/workflow/profiler_evidence_runner.py",
    ),
    "daemon.profiler_row_attribution": (
        "daemon",
        "ascendop_daemon/workflow/profiler_row_attribution.py",
    ),
    "engine.batch_case_runner": (
        "engine",
        "limited_remote_partner/engine/batch_case_runner.py",
    ),
    "engine.perf_pipeline": (
        "engine",
        "limited_remote_partner/engine/stages/perf_pipeline.py",
    ),
    "engine.profile_call_plan": (
        "engine",
        "limited_remote_partner/engine/stages/profile_call_plan.py",
    ),
    "engine.profile_session_runner": (
        "engine",
        "limited_remote_partner/engine/stages/profile_session_runner.py",
    ),
    "engine.test_engine_worker": (
        "engine",
        "limited_remote_partner/engine/test_engine_worker.py",
    ),
}


def measurement_component_manifest(
    root: Path,
    active_release: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_roots = {
        "daemon": _component_source_root(
            root,
            active_release,
            active_field="daemon_source",
            repository_path=Path("tools/tester_daemon/src"),
            package_marker=Path("ascendop_daemon/workflow/profiler_request_state.py"),
        ),
        "engine": _component_source_root(
            root,
            active_release,
            active_field="transport_source",
            repository_path=Path("GitPartner/src"),
            package_marker=Path("limited_remote_partner/engine/test_engine_worker.py"),
        ),
    }
    manifest: dict[str, dict[str, Any]] = {}
    for name, (source_kind, relative_path) in sorted(
        PROFILER_MEASUREMENT_COMPONENTS.items()
    ):
        path = source_roots[source_kind] / Path(relative_path)
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise ProfilerRequestStateError(
                f"profiler measurement component is unavailable: {name}: {path}"
            ) from exc
        manifest[name] = {
            "path": relative_path,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
        }
    return manifest


def _component_source_root(
    root: Path,
    active_release: dict[str, Any],
    *,
    active_field: str,
    repository_path: Path,
    package_marker: Path,
) -> Path:
    configured = str(active_release.get(active_field) or "").strip()
    if configured:
        configured_path = Path(configured)
        if not configured_path.is_absolute():
            configured_path = root / configured_path
        if (configured_path / package_marker).is_file():
            return configured_path.resolve()
        raise ProfilerRequestStateError(
            f"active release {active_field} is incomplete: {configured_path}"
        )

    candidates = [root.resolve()]
    candidates.extend(Path(__file__).resolve().parents)
    for candidate in candidates:
        source_root = candidate / repository_path
        if (source_root / package_marker).is_file():
            return source_root.resolve()
    raise ProfilerRequestStateError(
        f"cannot resolve profiler component source root for {active_field}"
    )


def existing_targets(state: dict[str, Any]) -> list[dict[str, Any]]:
    raw = state.get("targets", [])
    return (
        [item for item in raw if isinstance(item, dict)]
        if isinstance(raw, list)
        else []
    )


def recompute_request_status(state: dict[str, Any]) -> str:
    statuses = [str(item.get("status") or "") for item in existing_targets(state)]
    if statuses and all(status == "complete" for status in statuses):
        status = "complete"
    elif any(status == "enqueued" for status in statuses):
        status = "collecting"
    elif statuses and all(status == "unsupported" for status in statuses):
        status = "unsupported"
    elif any(status in {"failed", "partial", "unsupported"} for status in statuses):
        status = "failed"
    elif any(status == "planned" for status in statuses):
        status = "collecting"
    else:
        status = "ready"
    state["status"] = status
    return status


def complete_evidence_validation_error(evidence: dict[str, Any]) -> str:
    if str(evidence.get("status") or "").lower() != "complete":
        return ""
    runs = evidence.get("runs")
    if not isinstance(runs, list) or not runs:
        return "profiler evidence reported complete without any profiler runs"
    repetitions = int(evidence.get("measurement_repetitions", 1) or 1)
    primary_runs = [
        value
        for value in runs
        if isinstance(value, dict) and str(value.get("metric_label") or "") == "primary"
    ]
    roofline_runs = [
        value
        for value in runs
        if isinstance(value, dict) and str(value.get("metric_label") or "") == "roofline"
    ]
    profiler_mode = str(evidence.get("profiler_mode") or "")
    if len(primary_runs) != repetitions:
        return "profiler evidence primary repetition count is incomplete"
    if profiler_mode == "deep-dual" and len(roofline_runs) != repetitions:
        return "profiler evidence roofline repetition count is incomplete"
    for index, raw_run in enumerate(runs, start=1):
        if not isinstance(raw_run, dict):
            return f"profiler run {index} is not a structured record"
        metric = str(raw_run.get("metric_label") or "unknown")
        matched = raw_run.get("matched_operator_rows")
        if matched is None:
            csv_rows = raw_run.get("csv_evidence")
            matched = (
                sum(
                    int(row.get("matched_operator_rows", 0) or 0)
                    for row in csv_rows
                    if isinstance(row, dict)
                )
                if isinstance(csv_rows, list)
                else 0
            )
        if not bool(raw_run.get("success")) or int(matched or 0) <= 0:
            case_id = raw_run.get("case_id", index)
            return (
                "profiler evidence reported complete but "
                f"case {case_id} {metric} did not capture a target operator row"
            )
        zero_only_pipe_rows = raw_run.get("zero_only_pipe_rows")
        if zero_only_pipe_rows is None:
            zero_only_pipe_rows = bool(
                raw_run.get("zero_only_primary_pipe_rows")
                or raw_run.get("zero_only_roofline_pipe_rows")
            )
        if bool(zero_only_pipe_rows):
            if metric == "primary" and profiler_mode == "deep-dual":
                continue
            return f"profiler evidence {metric} pipe rows are zero-only"
    return ""


def profiler_result_summary(
    root: Path,
    state_path: Path,
    state: dict[str, Any],
    target: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": str(state.get("status") or ""),
        "target_status": str(target.get("status") or ""),
        "evidence_path": str(target.get("evidence_path") or ""),
        "request_state_path": relative_path(state_path, root),
    }


def case_shapes(snapshot: Path | None, cases: list[int]) -> dict[str, Any]:
    if snapshot is None:
        return {}
    path = snapshot / "task_case" / "case_specs.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, list):
        return {}
    shapes: dict[str, Any] = {}
    allowed = set(cases)
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            continue
        match = re.fullmatch(r"case([1-9][0-9]*)", str(item.get("generated_case") or ""))
        case_id = int(match.group(1)) if match else index
        if case_id not in allowed:
            continue
        inputs = item.get("inputs")
        first_input = (
            inputs[0]
            if isinstance(inputs, list) and inputs and isinstance(inputs[0], dict)
            else {}
        )
        shape = first_input.get("shape", item.get("shape", []))
        if not isinstance(shape, list) or not all(isinstance(value, int) for value in shape):
            continue
        shapes[str(case_id)] = list(shape)
    return shapes


def case_specs_sha256(snapshot: Path | None) -> str:
    if snapshot is None:
        return ""
    path = snapshot / "task_case" / "case_specs.json"
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(filesystem_path(path).read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def canonical_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def generation_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def relative_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def validate_token(value: str, field: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ProfilerRequestStateError(f"unsafe {field}: {value!r}")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
