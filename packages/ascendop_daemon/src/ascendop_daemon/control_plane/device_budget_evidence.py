from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


STANDARD_DEFAULT_SECONDS = 210
STANDARD_MAX_SECONDS = 240
HEAVY_MAX_SECONDS = 900
HEAVY_EVIDENCE_SCHEMA = "ascendop.device-budget-gate-evidence.v2"
PROFILER_HEAVY_EVIDENCE_SCHEMA = (
    "ascendop.profiler-device-budget-gate-evidence.v1"
)
PROFILER_PROCESS_TIMEOUT_SECONDS = 90
PROFILER_STARTUP_GRACE_SECONDS = 30
PROFILER_HISTORY_SAFETY_FACTOR = 1.35
PROFILER_MAX_JUSTIFIED_FACTOR = 1.5
PROFILER_BUDGET_QUANTUM_SECONDS = 30


class DeviceBudgetEvidenceError(ValueError):
    pass


@dataclass(frozen=True)
class DeviceBudgetDecision:
    requested_class: str
    requested_seconds: int
    effective_class: str
    effective_seconds: int
    reason: str
    evidence: dict[str, Any]

    @property
    def approved_gate_evidence(self) -> dict[str, Any]:
        return self.evidence if self.effective_class == "heavy" else {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "ascendop.device-budget-decision.v1",
            "requested_class": self.requested_class,
            "requested_device_session_seconds": self.requested_seconds,
            "effective_class": self.effective_class,
            "effective_device_session_seconds": self.effective_seconds,
            "reason": self.reason,
            **({"evidence": self.evidence} if self.evidence else {}),
        }


def decide_device_budget(
    root: Path,
    *,
    operator: str,
    case_version: str,
    budget_class: str,
    requested_seconds: int | None,
    task_profile_sha256: str,
    submit_md: Path,
    task_case: Path,
    attack_case: Path,
) -> DeviceBudgetDecision:
    requested = int(requested_seconds or 0)
    if budget_class != "heavy":
        effective = requested or STANDARD_DEFAULT_SECONDS
        return DeviceBudgetDecision(
            requested_class=budget_class,
            requested_seconds=effective,
            effective_class=budget_class,
            effective_seconds=effective,
            reason="task-profile-request",
            evidence={},
        )
    if not submit_md.is_file() or not task_case.is_dir():
        raise DeviceBudgetEvidenceError(
            "heavy device budget requires a submit-ready immutable workload"
        )
    evidence = build_heavy_gate_evidence(
        root,
        operator=operator,
        case_version=case_version,
        requested_seconds=requested,
        task_profile_sha256=task_profile_sha256,
        submit_md=submit_md,
        task_case=task_case,
        attack_case=attack_case,
    )
    try:
        validate_heavy_gate_evidence(evidence, requested_seconds=requested)
    except DeviceBudgetEvidenceError as exc:
        return DeviceBudgetDecision(
            requested_class="heavy",
            requested_seconds=requested,
            effective_class="standard",
            effective_seconds=STANDARD_DEFAULT_SECONDS,
            reason=f"heavy-request-downgraded:{exc}",
            evidence=evidence,
        )
    return DeviceBudgetDecision(
        requested_class="heavy",
        requested_seconds=requested,
        effective_class="heavy",
        effective_seconds=requested,
        reason="heavy-workload-history-approved",
        evidence=evidence,
    )


def decide_profiler_device_budget(
    root: Path,
    *,
    operator: str,
    case_version: str,
    profiler_plan: Mapping[str, Any],
    requested_seconds: int | None,
    task_profile_sha256: str,
    submit_md: Path,
    task_case: Path,
) -> DeviceBudgetDecision:
    """Approve one diagnostic profiler lease from its immutable process plan."""

    requested = int(requested_seconds or 0)
    if requested < 0 or requested > STANDARD_MAX_SECONDS:
        raise DeviceBudgetEvidenceError(
            "Solver diagnostic budget must remain within the standard 240-second "
            "request range; daemon policy owns any evidence-backed heavy grant"
        )
    mode = _canonical_profiler_mode(
        str(
            profiler_plan.get("collection_mode")
            or profiler_plan.get("profiler_mode")
            or ""
        )
    )
    repetitions = int(profiler_plan.get("measurement_repetitions", 1) or 1)
    if not 1 <= repetitions <= 5:
        raise DeviceBudgetEvidenceError(
            "profiler measurement_repetitions must be within 1..5"
        )
    processes_per_repetition = 2 if mode == "primary-roofline-all-cases" else 1
    process_count = processes_per_repetition * repetitions
    guard_bound = (
        process_count * PROFILER_PROCESS_TIMEOUT_SECONDS
        + PROFILER_STARTUP_GRACE_SECONDS
    )
    if guard_bound <= STANDARD_MAX_SECONDS:
        effective = max(requested, guard_bound)
        return DeviceBudgetDecision(
            requested_class="diagnostic",
            requested_seconds=requested or effective,
            effective_class="diagnostic",
            effective_seconds=effective,
            reason="diagnostic-profiler-process-plan",
            evidence={},
        )

    cases = [int(value) for value in profiler_plan.get("cases", [])]
    if not cases:
        raise DeviceBudgetEvidenceError(
            "profiler heavy budget requires an immutable all-case process plan"
        )
    samples = collect_completed_profiler_sessions(
        root,
        operator=operator,
        case_version=case_version,
        profiler_mode=mode,
        cases=cases,
    )
    if not samples:
        raise DeviceBudgetEvidenceError(
            "profiler heavy budget requires completed same-operator, same-case, "
            "same-mode process history"
        )
    run_durations = sorted(
        float(duration)
        for sample in samples
        for duration in sample["run_durations_seconds"]
    )
    process_p95 = _percentile95(run_durations)
    process_maximum = max(run_durations)
    recommended = _round_up_seconds(
        process_p95 * process_count * PROFILER_HISTORY_SAFETY_FACTOR
        + PROFILER_STARTUP_GRACE_SECONDS
    )
    approved = max(STANDARD_MAX_SECONDS + 1, requested, recommended)
    justified = min(
        HEAVY_MAX_SECONDS,
        _round_up_seconds(
            process_maximum * process_count * PROFILER_MAX_JUSTIFIED_FACTOR
            + PROFILER_STARTUP_GRACE_SECONDS
        ),
    )
    if approved > HEAVY_MAX_SECONDS or approved > justified:
        raise DeviceBudgetEvidenceError(
            "profiler process plan exceeds the evidence-backed 900-second heavy "
            f"budget: approved={approved} justified={justified}"
        )
    evidence = {
        "schema": PROFILER_HEAVY_EVIDENCE_SCHEMA,
        "gate": "diagnostic-profile-ready",
        "clock": "same-host-process-monotonic",
        "workload": {
            "operator": operator,
            "case_version": case_version,
            "target_version": str(profiler_plan.get("target_version") or ""),
            "target_source_sha256": str(
                profiler_plan.get("target_source_sha256") or ""
            ),
            "blocker_generation": str(
                profiler_plan.get("blocker_generation") or ""
            ),
            "request_sha256": str(profiler_plan.get("request_sha256") or ""),
            "task_profile_sha256": task_profile_sha256,
            "profiler_plan_sha256": _canonical_sha256(profiler_plan),
            "profiler_mode": mode,
            "measurement_repetitions": repetitions,
            "process_count": process_count,
            "process_timeout_seconds": PROFILER_PROCESS_TIMEOUT_SECONDS,
            "solver_requested_device_session_seconds": requested,
            "requested_device_session_seconds": approved,
            "submit_md": _artifact_ref(root, submit_md),
            "task_case": _tree_ref(root, task_case),
            "cases": cases,
        },
        "history": {
            "sample_count": len(samples),
            "completed_process_count": len(run_durations),
            "completed_process_p95_seconds": round(process_p95, 6),
            "completed_process_max_seconds": round(process_maximum, 6),
            "safety_factor": PROFILER_HISTORY_SAFETY_FACTOR,
            "maximum_justified_factor": PROFILER_MAX_JUSTIFIED_FACTOR,
            "startup_grace_seconds": PROFILER_STARTUP_GRACE_SECONDS,
            "budget_quantum_seconds": PROFILER_BUDGET_QUANTUM_SECONDS,
            "recommended_seconds": recommended,
            "maximum_justified_seconds": justified,
            "samples": samples[-20:],
        },
    }
    validate_profiler_heavy_gate_evidence(
        evidence,
        requested_seconds=approved,
    )
    return DeviceBudgetDecision(
        requested_class="diagnostic",
        requested_seconds=requested or approved,
        effective_class="heavy",
        effective_seconds=approved,
        reason="diagnostic-profiler-history-approved",
        evidence=evidence,
    )


def build_heavy_gate_evidence(
    root: Path,
    *,
    operator: str,
    case_version: str,
    requested_seconds: int,
    task_profile_sha256: str,
    submit_md: Path,
    task_case: Path,
    attack_case: Path,
) -> dict[str, Any]:
    samples = collect_completed_device_sessions(
        root,
        operator=operator,
        case_version=case_version,
    )
    durations = sorted(float(item["duration_seconds"]) for item in samples)
    p95 = _percentile95(durations)
    maximum = max(durations, default=0.0)
    justified = min(
        HEAVY_MAX_SECONDS,
        math.ceil(max(p95 * 1.5, maximum * 1.25)),
    )
    workload = {
        "operator": operator,
        "case_version": case_version,
        "requested_device_session_seconds": int(requested_seconds),
        "task_profile_sha256": task_profile_sha256,
        "submit_md": _artifact_ref(root, submit_md),
        "task_case": _tree_ref(root, task_case),
    }
    if attack_case.is_dir():
        workload["attack_case"] = _tree_ref(root, attack_case)
    return {
        "schema": HEAVY_EVIDENCE_SCHEMA,
        "gate": "submit-ready",
        "clock": "same-host-boot-monotonic",
        "workload": workload,
        "history": {
            "sample_count": len(samples),
            "completed_session_p95_seconds": round(p95, 6),
            "completed_session_max_seconds": round(maximum, 6),
            "maximum_justified_seconds": int(justified),
            "samples": samples[-20:],
        },
    }


def validate_heavy_gate_evidence(
    raw: Mapping[str, Any] | str,
    *,
    requested_seconds: int,
) -> dict[str, Any]:
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DeviceBudgetEvidenceError(
                "heavy gate evidence must be canonical JSON"
            ) from exc
    else:
        value = dict(raw)
    if value.get("schema") != HEAVY_EVIDENCE_SCHEMA:
        raise DeviceBudgetEvidenceError("heavy gate evidence schema is not v2")
    workload = value.get("workload")
    history = value.get("history")
    if not isinstance(workload, Mapping) or not isinstance(history, Mapping):
        raise DeviceBudgetEvidenceError(
            "heavy gate evidence requires workload and history"
        )
    if int(workload.get("requested_device_session_seconds", 0) or 0) != int(
        requested_seconds
    ):
        raise DeviceBudgetEvidenceError("heavy evidence request does not match budget")
    samples = history.get("samples")
    if not isinstance(samples, list):
        raise DeviceBudgetEvidenceError("heavy history samples must be a list")
    durations: list[float] = []
    for sample in samples:
        if not isinstance(sample, Mapping):
            raise DeviceBudgetEvidenceError("heavy history sample is not an object")
        duration = float(sample.get("duration_seconds", 0) or 0)
        if duration <= 0:
            raise DeviceBudgetEvidenceError("heavy history duration must be positive")
        if not all(
            _is_sha256(str(sample.get(field) or ""))
            for field in ("state_sha256", "spec_sha256", "correctness_sha256")
        ):
            raise DeviceBudgetEvidenceError("heavy history artifact digest is invalid")
        durations.append(duration)
    if int(history.get("sample_count", -1)) != len(samples) or not samples:
        raise DeviceBudgetEvidenceError(
            "heavy budget requires completed same-case history"
        )
    p95 = _percentile95(sorted(durations))
    maximum = max(durations)
    justified = min(
        HEAVY_MAX_SECONDS,
        math.ceil(max(p95 * 1.5, maximum * 1.25)),
    )
    if abs(float(history.get("completed_session_p95_seconds", -1)) - p95) > 1e-6:
        raise DeviceBudgetEvidenceError("heavy history p95 does not match samples")
    if abs(float(history.get("completed_session_max_seconds", -1)) - maximum) > 1e-6:
        raise DeviceBudgetEvidenceError("heavy history max does not match samples")
    if int(history.get("maximum_justified_seconds", -1)) != justified:
        raise DeviceBudgetEvidenceError(
            "heavy history justified bound does not match samples"
        )
    if p95 <= STANDARD_MAX_SECONDS:
        raise DeviceBudgetEvidenceError(
            "same-case completed history does not exceed the standard budget"
        )
    if requested_seconds > justified:
        raise DeviceBudgetEvidenceError(
            f"requested heavy budget exceeds evidence bound {justified} seconds"
        )
    return value


def validate_device_budget_gate_evidence(
    raw: Mapping[str, Any] | str,
    *,
    requested_seconds: int,
) -> dict[str, Any]:
    value = _evidence_object(raw)
    if value.get("schema") == PROFILER_HEAVY_EVIDENCE_SCHEMA:
        return validate_profiler_heavy_gate_evidence(
            value,
            requested_seconds=requested_seconds,
        )
    return validate_heavy_gate_evidence(
        value,
        requested_seconds=requested_seconds,
    )


def validate_profiler_heavy_gate_evidence(
    raw: Mapping[str, Any] | str,
    *,
    requested_seconds: int,
) -> dict[str, Any]:
    value = _evidence_object(raw)
    if value.get("schema") != PROFILER_HEAVY_EVIDENCE_SCHEMA:
        raise DeviceBudgetEvidenceError(
            "profiler heavy gate evidence schema is not v1"
        )
    workload = value.get("workload")
    history = value.get("history")
    if not isinstance(workload, Mapping) or not isinstance(history, Mapping):
        raise DeviceBudgetEvidenceError(
            "profiler heavy evidence requires workload and history"
        )
    if int(workload.get("requested_device_session_seconds", 0) or 0) != int(
        requested_seconds
    ):
        raise DeviceBudgetEvidenceError(
            "profiler heavy evidence request does not match approved budget"
        )
    mode = _canonical_profiler_mode(str(workload.get("profiler_mode") or ""))
    repetitions = int(workload.get("measurement_repetitions", 0) or 0)
    expected_process_count = repetitions * (
        2 if mode == "primary-roofline-all-cases" else 1
    )
    if repetitions not in range(1, 6) or int(
        workload.get("process_count", 0) or 0
    ) != expected_process_count:
        raise DeviceBudgetEvidenceError(
            "profiler heavy evidence process plan is inconsistent"
        )
    if int(workload.get("process_timeout_seconds", 0) or 0) != (
        PROFILER_PROCESS_TIMEOUT_SECONDS
    ):
        raise DeviceBudgetEvidenceError(
            "profiler heavy evidence process timeout is not the registered 90 seconds"
        )
    if not _is_sha256(str(workload.get("profiler_plan_sha256") or "")):
        raise DeviceBudgetEvidenceError(
            "profiler heavy evidence plan digest is invalid"
        )
    samples = history.get("samples")
    if not isinstance(samples, list) or not samples:
        raise DeviceBudgetEvidenceError(
            "profiler heavy budget requires completed process history"
        )
    durations: list[float] = []
    for sample in samples:
        if not isinstance(sample, Mapping):
            raise DeviceBudgetEvidenceError(
                "profiler heavy history sample is not an object"
            )
        if not _is_sha256(str(sample.get("evidence_sha256") or "")):
            raise DeviceBudgetEvidenceError(
                "profiler heavy history evidence digest is invalid"
            )
        if _canonical_profiler_mode(
            str(sample.get("profiler_mode") or "")
        ) != mode:
            raise DeviceBudgetEvidenceError(
                "profiler heavy history mode does not match the workload"
            )
        sample_durations = sample.get("run_durations_seconds")
        if not isinstance(sample_durations, list) or not sample_durations:
            raise DeviceBudgetEvidenceError(
                "profiler heavy history sample has no completed process durations"
            )
        for duration_value in sample_durations:
            duration = float(duration_value or 0)
            if not 0 < duration <= PROFILER_PROCESS_TIMEOUT_SECONDS:
                raise DeviceBudgetEvidenceError(
                    "profiler heavy history duration is outside the process guard"
                )
            durations.append(duration)
    if int(history.get("sample_count", -1)) != len(samples):
        raise DeviceBudgetEvidenceError(
            "profiler heavy history sample count does not match"
        )
    if int(history.get("completed_process_count", -1)) != len(durations):
        raise DeviceBudgetEvidenceError(
            "profiler heavy history process count does not match"
        )
    process_p95 = _percentile95(sorted(durations))
    process_maximum = max(durations)
    recommended = _round_up_seconds(
        process_p95 * expected_process_count * PROFILER_HISTORY_SAFETY_FACTOR
        + PROFILER_STARTUP_GRACE_SECONDS
    )
    justified = min(
        HEAVY_MAX_SECONDS,
        _round_up_seconds(
            process_maximum
            * expected_process_count
            * PROFILER_MAX_JUSTIFIED_FACTOR
            + PROFILER_STARTUP_GRACE_SECONDS
        ),
    )
    checks = (
        ("completed_process_p95_seconds", process_p95),
        ("completed_process_max_seconds", process_maximum),
    )
    for field, expected in checks:
        if abs(float(history.get(field, -1)) - expected) > 1e-6:
            raise DeviceBudgetEvidenceError(
                f"profiler heavy history {field} does not match samples"
            )
    if int(history.get("recommended_seconds", -1)) != recommended:
        raise DeviceBudgetEvidenceError(
            "profiler heavy history recommended budget does not match"
        )
    if int(history.get("maximum_justified_seconds", -1)) != justified:
        raise DeviceBudgetEvidenceError(
            "profiler heavy history justified budget does not match"
        )
    if requested_seconds <= STANDARD_MAX_SECONDS or requested_seconds > justified:
        raise DeviceBudgetEvidenceError(
            "profiler heavy grant is outside the evidence-backed bound"
        )
    return value


def collect_completed_profiler_sessions(
    root: Path,
    *,
    operator: str,
    case_version: str,
    profiler_mode: str,
    cases: list[int],
) -> list[dict[str, Any]]:
    evidence_root = (
        root.resolve()
        / "TestUtils"
        / "casegen"
        / operator
        / "case"
        / case_version
        / "profiler_evidence"
    )
    if not evidence_root.is_dir():
        return []
    candidates = sorted(
        evidence_root.rglob("PROFILER_EVIDENCE.json"),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )[:100]
    samples: list[dict[str, Any]] = []
    for path in candidates:
        evidence = _read_object(path)
        if (
            evidence.get("protocol_version") != "ascendop-profiler-evidence-v1"
            or str(evidence.get("status") or "") != "complete"
            or str(evidence.get("operator") or "") != operator
            or str(evidence.get("case_version") or "") != case_version
            or _canonical_profiler_mode(
                str(
                    evidence.get("collection_mode")
                    or evidence.get("profiler_mode")
                    or ""
                )
            )
            != profiler_mode
            or [int(value) for value in evidence.get("cases", [])] != cases
        ):
            continue
        runs = [row for row in evidence.get("runs", []) if isinstance(row, Mapping)]
        durations = [
            round(float(row.get("duration_seconds", 0) or 0), 6)
            for row in runs
            if row.get("success") is True
        ]
        if not runs or len(durations) != len(runs) or not all(
            0 < duration <= PROFILER_PROCESS_TIMEOUT_SECONDS
            for duration in durations
        ):
            continue
        samples.append(
            {
                "evidence_path": path.resolve().relative_to(root.resolve()).as_posix(),
                "evidence_sha256": _file_sha256(path),
                "target_version": str(evidence.get("target_version") or ""),
                "target_source_sha256": str(
                    evidence.get("target_source_sha256") or ""
                ),
                "profiler_mode": profiler_mode,
                "measurement_repetitions": int(
                    evidence.get("measurement_repetitions", 1) or 1
                ),
                "run_durations_seconds": durations,
                "total_duration_seconds": round(sum(durations), 6),
            }
        )
    return sorted(samples, key=lambda item: item["evidence_path"])[-20:]


def collect_completed_device_sessions(
    root: Path,
    *,
    operator: str,
    case_version: str,
) -> list[dict[str, Any]]:
    result_root = root.resolve() / "operators_testresult" / operator
    if not result_root.is_dir():
        return []
    samples: dict[tuple[str, str], dict[str, Any]] = {}
    for version_root in sorted(result_root.iterdir(), key=lambda item: item.name):
        if not version_root.is_dir():
            continue
        evidence_root = version_root / "gitpartner_output"
        state_path = evidence_root / "state.json"
        spec_path = evidence_root / "spec.json"
        correctness_path = (
            evidence_root
            / "result_bundle"
            / "result"
            / "CORRECTNESS_BATCH.json"
        )
        state = _read_object(state_path)
        spec = _read_object(spec_path)
        correctness = _read_object(correctness_path)
        if not state or not spec or not correctness:
            continue
        workflow = spec.get("workflow")
        if not isinstance(workflow, Mapping) or str(
            workflow.get("case_version") or ""
        ) != str(case_version):
            continue
        expected = int(correctness.get("expected_execution_count", 0) or 0)
        completed = int(correctness.get("execution_count", 0) or 0)
        if expected <= 0 or completed != expected:
            continue
        records = [
            item
            for item in state.get("history", [])
            if isinstance(item, Mapping)
            and str(item.get("stage_resource") or "") == "device"
        ]
        if not records or any(bool(item.get("timed_out")) for item in records):
            continue
        hosts = {str(item.get("host") or "") for item in records}
        boots = {str(item.get("boot_id") or "") for item in records}
        if len(hosts) != 1 or len(boots) != 1 or "" in hosts or "" in boots:
            continue
        started = [int(item.get("started_monotonic_ns", 0) or 0) for item in records]
        finished = [int(item.get("finished_monotonic_ns", 0) or 0) for item in records]
        if not started or min(started) <= 0 or max(finished) <= min(started):
            continue
        duration = (max(finished) - min(started)) / 1_000_000_000
        request_id = str(state.get("request_id") or "")
        attempt_id = str(state.get("attempt_id") or "")
        key = (request_id, attempt_id)
        samples[key] = {
            "test_version": str(state.get("test_version") or version_root.name),
            "request_id": request_id,
            "attempt_id": attempt_id,
            "host": next(iter(hosts)),
            "boot_id": next(iter(boots)),
            "duration_seconds": round(duration, 6),
            "state_sha256": _file_sha256(state_path),
            "spec_sha256": _file_sha256(spec_path),
            "correctness_sha256": _file_sha256(correctness_path),
        }
    return sorted(samples.values(), key=lambda item: (item["test_version"], item["attempt_id"]))


def _evidence_object(raw: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DeviceBudgetEvidenceError(
                "heavy gate evidence must be canonical JSON"
            ) from exc
    else:
        value = dict(raw)
    if not isinstance(value, dict):
        raise DeviceBudgetEvidenceError("heavy gate evidence must be an object")
    return value


def _canonical_profiler_mode(value: str) -> str:
    normalized = {
        "fast-single": "primary-all-cases",
        "batched-primary-only": "primary-all-cases",
        "primary-all-cases": "primary-all-cases",
        "deep-dual": "primary-roofline-all-cases",
        "batched-primary-roofline": "primary-roofline-all-cases",
        "primary-roofline-all-cases": "primary-roofline-all-cases",
    }.get(str(value or "").strip())
    if normalized is None:
        raise DeviceBudgetEvidenceError(
            f"unsupported profiler budget mode: {value!r}"
        )
    return normalized


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _round_up_seconds(value: float) -> int:
    return int(
        math.ceil(float(value) / PROFILER_BUDGET_QUANTUM_SECONDS)
        * PROFILER_BUDGET_QUANTUM_SECONDS
    )


def _percentile95(values: list[float]) -> float:
    if not values:
        return 0.0
    return values[min(len(values) - 1, max(0, math.ceil(len(values) * 0.95) - 1))]


def _artifact_ref(root: Path, path: Path) -> dict[str, Any]:
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "sha256": _file_sha256(path),
    }


def _tree_ref(root: Path, path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    for child in sorted(path.rglob("*"), key=lambda item: item.as_posix()):
        relative = child.relative_to(path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if child.is_file():
            _update_canonical_file_digest(digest, child)
            digest.update(b"\0")
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "sha256": digest.hexdigest(),
    }


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _update_canonical_file_digest(digest: Any, path: Path) -> None:
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            data = carry + chunk
            carry = b"\r" if data.endswith(b"\r") else b""
            if carry:
                data = data[:-1]
            digest.update(data.replace(b"\r\n", b"\n"))
    if carry:
        digest.update(carry)


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )
