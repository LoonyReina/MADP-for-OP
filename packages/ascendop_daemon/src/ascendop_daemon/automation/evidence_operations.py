from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.actor import (
    AGENT_ACTION_OUTCOME_SCHEMA,
    validate_agent_action_outcome,
)
from ascendop_protocol.evidence import (
    EVIDENCE_OPERATION_REQUEST_SCHEMA,
    evidence_operation_registry,
    evidence_operation_registry_digest,
    validate_evidence_operation_request,
)
from ascendop_protocol.workflow import validate_solver_diagnostic_request


class EvidenceOperationError(RuntimeError):
    pass


class EvidenceOperationCoordinator:
    """Translate Agent evidence selections into authoritative queued operations."""

    def __init__(self, root: Path, database: Any) -> None:
        self.root = root.resolve()
        self.database = database

    def build_request(
        self,
        action: Mapping[str, Any],
        outcome: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        value = validate_agent_action_outcome(outcome)
        if value.get("disposition") != "request_evidence":
            return None
        selected = value.get("requested_operation")
        if not isinstance(selected, Mapping):
            raise EvidenceOperationError(
                "request_evidence outcome has no requested operation"
            )
        registry = evidence_operation_registry()
        operation_code = str(selected["operation_code"])
        parameters = dict(selected["parameters"])
        identity = {
            "origin_action_id": str(action["action_id"]),
            "origin_iteration_id": str(action["iteration_id"]),
            "operator_id": str(action["operator_id"]),
            "origin_role": str(action["role"]),
            "operation_code": operation_code,
            "expected_consumer": str(selected["expected_consumer"]),
            "parameters": parameters,
        }
        digest = _digest(identity)
        request = {
            "schema": EVIDENCE_OPERATION_REQUEST_SCHEMA,
            "operation_request_id": f"eor-{digest[:24]}",
            "idempotency_key": f"evidence-operation:{digest}",
            "registry_generation": registry["generation"],
            "registry_digest": evidence_operation_registry_digest(),
            "operation_code": operation_code,
            "origin": {
                "action_id": str(action["action_id"]),
                "iteration_id": str(action["iteration_id"]),
                "operator_id": str(action["operator_id"]),
                "role": str(action["role"]),
            },
            "expected_consumer": str(selected["expected_consumer"]),
            "parameters": parameters,
            "resume_condition": str(selected["resume_condition"]),
            "state": "queued",
            "created_at": str(value["completed_at"]),
        }
        try:
            return validate_evidence_operation_request(request)
        except ValueError as exc:
            raise EvidenceOperationError(str(exc)) from exc

    def register(
        self,
        action: Mapping[str, Any],
        outcome: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        request = self.build_request(action, outcome)
        if request is None:
            return None
        return self.database.create_evidence_operation_request(request)

    def legacy_diagnostic_outcome(
        self,
        action: Mapping[str, Any],
        output_seal: Mapping[str, Any],
        *,
        workspace: Path,
        durable_outputs: list[dict[str, Any]],
        summary: str,
        completed_at: str,
    ) -> dict[str, Any] | None:
        legacy = [
            item
            for item in output_seal.get("outputs", [])
            if str(item.get("output_kind") or "") == "solver-diagnostic-request"
        ]
        if not legacy:
            return None
        if len(legacy) != 1:
            raise EvidenceOperationError(
                "legacy diagnostic compatibility accepts exactly one request"
            )
        path = (workspace / str(legacy[0]["isolated_path"])).resolve()
        try:
            path.relative_to(workspace.resolve())
        except ValueError as exc:
            raise EvidenceOperationError(
                "legacy diagnostic request escapes the Agent workspace"
            ) from exc
        try:
            request = validate_solver_diagnostic_request(
                json.loads(path.read_text(encoding="utf-8-sig"))
            )
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise EvidenceOperationError(
                f"legacy diagnostic request is invalid: {exc}"
            ) from exc
        target = dict(request["target"])
        scope = dict(request["scope"])
        operation_kind = str(request["operation_kind"])
        common = {
            "candidate_id": str(target["test_version"]),
            "test_version": str(target["test_version"]),
            "case_version": str(request["case_version"]),
        }
        if operation_kind == "diagnostic-profile":
            operation_code = "profile.collect"
            parameters = {
                **common,
                "profiler_mode": str(scope["profiler_mode"]),
                "legacy_blocker_generation": str(request["blocker_generation"]),
            }
        elif operation_kind == "diagnostic-correctness-replay":
            operation_code = "correctness.replay"
            parameters = {
                **common,
                "failing_cases": ["all-failing-cases"],
                "requested_artifacts": list(scope["requested_artifacts"]),
                "legacy_blocker_generation": str(request["blocker_generation"]),
            }
        else:
            raise EvidenceOperationError(
                f"legacy diagnostic operation is not mapped: {operation_kind}"
            )
        return validate_agent_action_outcome(
            {
                "schema": AGENT_ACTION_OUTCOME_SCHEMA,
                "action_id": str(action["action_id"]),
                "execution_status": "completed",
                "disposition": "request_evidence",
                "failure_class": None,
                "summary": summary,
                "outputs": durable_outputs,
                "evidence_refs": [str(item) for item in request["consulted_evidence"]],
                "requested_operation": {
                    "operation_code": operation_code,
                    "parameters": parameters,
                    "expected_consumer": "solver",
                    "resume_condition": (
                        "registered evidence is indexed for the exact source, "
                        "case version, and origin iteration"
                    ),
                },
                "blocker": None,
                "completed_at": completed_at,
            }
        )


def _digest(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "EvidenceOperationCoordinator",
    "EvidenceOperationError",
]
