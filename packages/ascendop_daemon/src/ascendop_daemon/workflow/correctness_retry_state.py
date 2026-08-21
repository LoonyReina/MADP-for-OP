from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.workflow.solver_diagnostic_paths import (
    SolverDiagnosticError,
    utc_now_iso,
)


def active_engine_code_generation(root: Path) -> str:
    active = _read_object(root / ".ascendop-work" / "runtime" / "active-release.json")
    return str(active.get("engine_code_generation") or "").strip()


def materialized_engine_code_generation(materialized_root: Path) -> str:
    for relative in (
        "state.json",
        "terminal.json",
        "result_bundle/state.json",
        "result_bundle/terminal.json",
    ):
        value = _read_object(materialized_root / relative)
        generation = str(value.get("engine_code_generation") or "").strip()
        if generation:
            return generation
    return ""


def state_execution_engine_generation(
    root: Path,
    state: Mapping[str, Any],
) -> str:
    generation = str(state.get("execution_engine_generation") or "").strip()
    if generation:
        return generation
    evidence_path = str(state.get("evidence_path") or "")
    evidence = _read_object(root / evidence_path) if evidence_path else {}
    generation = str(evidence.get("execution_engine_generation") or "").strip()
    if generation:
        return generation
    materialized_bundle = str(evidence.get("materialized_bundle") or "")
    if materialized_bundle:
        return materialized_engine_code_generation(root / materialized_bundle)
    return ""


def authorize_correctness_retry(
    root: Path,
    state: Mapping[str, Any],
    *,
    expected_request_attempt: int,
    expected_failed_engine_generation: str,
) -> dict[str, Any]:
    """Return one CAS-scoped same-intent correctness replay state."""

    updated = dict(state)
    current_attempt = int(updated.get("request_attempt", 1) or 1)
    requested_attempt = int(expected_request_attempt)
    current_status = str(updated.get("status") or "").lower()
    active_generation = active_engine_code_generation(root)
    failed_generation = state_execution_engine_generation(root, updated) or str(
        updated.get("retry_from_engine_generation") or ""
    )
    expected_failed_generation = str(expected_failed_engine_generation or "").strip()
    retry_generation = str(updated.get("retry_engine_generation") or "")
    if not active_generation:
        raise SolverDiagnosticError(
            "correctness retry requires an active Engine code generation"
        )
    if (
        not expected_failed_generation
        or failed_generation != expected_failed_generation
    ):
        raise SolverDiagnosticError(
            "correctness retry failed Engine generation changed before reset: "
            f"observed={failed_generation or 'missing'} "
            f"expected={expected_failed_generation or 'missing'}"
        )
    if (
        requested_attempt == current_attempt
        and retry_generation == active_generation
        and current_status in {"ready", "collecting", "complete"}
    ):
        return updated

    next_attempt = current_attempt + 1
    if requested_attempt != next_attempt:
        raise SolverDiagnosticError(
            "correctness retry attempt changed before reset: "
            f"expected={next_attempt} requested={requested_attempt}"
        )
    if current_status != "failed":
        raise SolverDiagnosticError(
            "correctness retry requires failed state: "
            f"status={current_status or 'missing'}"
        )
    if str(updated.get("target_terminal_state") or "") != (
        "terminal-infrastructure-failure"
    ):
        raise SolverDiagnosticError(
            "correctness retry requires an infrastructure terminal"
        )
    missing_requested_artifacts = [
        str(value)
        for value in updated.get("missing_requested_artifacts", [])
        if str(value)
    ]
    if not missing_requested_artifacts:
        raise SolverDiagnosticError(
            "correctness retry requires missing requested diagnostic artifacts"
        )
    if failed_generation == active_generation:
        raise SolverDiagnosticError(
            "correctness retry Engine capability generation is unchanged: "
            f"{active_generation}"
        )
    if retry_generation == active_generation:
        raise SolverDiagnosticError(
            f"correctness retry Engine generation already consumed: {active_generation}"
        )

    history = [
        dict(item)
        for item in updated.get("attempt_history", [])
        if isinstance(item, dict)
    ]
    history.append(
        {
            "request_attempt": current_attempt,
            "status": current_status,
            "request_id": str(updated.get("request_id") or ""),
            "attempt_id": str(updated.get("attempt_id") or ""),
            "evidence_path": str(updated.get("evidence_path") or ""),
            "target_terminal_state": str(updated.get("target_terminal_state") or ""),
            "missing_requested_artifacts": missing_requested_artifacts,
            "execution_engine_generation": failed_generation,
            "terminal_updated_at": str(updated.get("updated_at") or ""),
            "archived_at": utc_now_iso(),
        }
    )
    updated.update(
        {
            "request_attempt": requested_attempt,
            "status": "ready",
            "request_id": "",
            "attempt_id": "",
            "evidence_path": "",
            "collection_status": "",
            "missing_requested_artifacts": [],
            "target_terminal_state": "",
            "execution_engine_generation": "",
            "retry_engine_generation": active_generation,
            "retry_from_engine_generation": failed_generation,
            "attempt_history": history,
            "last_error": "",
            "retry_authorized_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
    )
    return updated


def reconcile_reused_correctness_attempt_identity(
    state: Mapping[str, Any],
) -> dict[str, Any]:
    """Recover a retry that was accidentally bound to a historical transport."""

    updated = dict(state)
    if (
        str(updated.get("status") or "") != "collecting"
        or int(updated.get("request_attempt", 1) or 1) <= 1
    ):
        return updated
    request_id = str(updated.get("request_id") or "")
    attempt_id = str(updated.get("attempt_id") or "")
    if not request_id or not attempt_id:
        return updated
    reused = any(
        str(item.get("request_id") or "") == request_id
        and str(item.get("attempt_id") or "") == attempt_id
        for item in updated.get("attempt_history", [])
        if isinstance(item, Mapping)
    )
    if not reused:
        return updated

    reconciliations = [
        dict(item)
        for item in updated.get("transport_identity_reconciliations", [])
        if isinstance(item, Mapping)
    ]
    reconciliations.append(
        {
            "request_attempt": int(updated.get("request_attempt", 1) or 1),
            "reused_request_id": request_id,
            "reused_attempt_id": attempt_id,
            "reason": "historical-transport-identity-reused",
            "reconciled_at": utc_now_iso(),
        }
    )
    updated.update(
        {
            "status": "ready",
            "request_id": "",
            "attempt_id": "",
            "last_error": "",
            "transport_identity_reconciliations": reconciliations,
            "updated_at": utc_now_iso(),
        }
    )
    return updated


def _read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


__all__ = [
    "active_engine_code_generation",
    "authorize_correctness_retry",
    "materialized_engine_code_generation",
    "reconcile_reused_correctness_attempt_identity",
    "state_execution_engine_generation",
]
