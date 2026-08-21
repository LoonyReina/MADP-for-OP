from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Protocol

from ascendop_protocol.evidence import (
    EVIDENCE_OPERATION_RESULT_SCHEMA,
    evidence_operation_definition,
    validate_evidence_operation_result,
)


class ResultIngestor(Protocol):
    def ingest(
        self,
        route: Mapping[str, Any],
        returned: Mapping[str, Any],
        *,
        projection_dir: Path,
    ) -> dict[str, Any]: ...


class EvidenceOperationResultRouter:
    """Complete a routed V5 evidence operation after Wire result validation."""

    def __init__(
        self,
        root: Path,
        database: Any,
        *,
        delegate: ResultIngestor,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.delegate = delegate

    def ingest(
        self,
        route: Mapping[str, Any],
        returned: Mapping[str, Any],
        *,
        projection_dir: Path,
    ) -> dict[str, Any]:
        workflow_ingest = self.delegate.ingest(
            route,
            returned,
            projection_dir=projection_dir,
        )
        request_id = str(route.get("request_id") or "")
        operation = self.database.evidence_operation_for_test_request(request_id)
        if operation is None:
            return workflow_ingest

        result = self._build_result(
            operation,
            returned,
            projection_dir=projection_dir,
            workflow_ingest=workflow_ingest,
        )
        completed = self.database.complete_evidence_operation(result)
        return {
            **workflow_ingest,
            "evidence_operation": completed,
        }

    def _build_result(
        self,
        operation: Mapping[str, Any],
        returned: Mapping[str, Any],
        *,
        projection_dir: Path,
        workflow_ingest: Mapping[str, Any],
    ) -> dict[str, Any]:
        request = dict(operation["request"])
        route = dict(operation["route"])
        returned_payload = returned.get("payload")
        if not isinstance(returned_payload, Mapping):
            returned_payload = {}
        terminal_state = str(returned_payload.get("terminal_state") or "")
        status = _result_status(terminal_state)
        definition = evidence_operation_definition(str(operation["operation_code"]))
        projection = projection_dir.resolve()
        _bounded(self.root, projection)
        artifact_refs = [
            path.relative_to(self.root).as_posix()
            for path in sorted(projection.rglob("*"), key=lambda item: item.as_posix())
            if path.is_file()
        ]
        identity = {
            "operation_request_id": operation["operation_request_id"],
            "terminal_state": terminal_state,
            "return_id": str(returned.get("return_id") or ""),
            "receipt_id": str(returned.get("receipt_id") or ""),
            "artifact_refs": artifact_refs,
        }
        digest = _digest(identity)
        return validate_evidence_operation_result(
            {
                "schema": EVIDENCE_OPERATION_RESULT_SCHEMA,
                "operation_result_id": f"eores-{digest[:24]}",
                "operation_request_id": str(operation["operation_request_id"]),
                "registry_generation": str(request["registry_generation"]),
                "registry_digest": str(request["registry_digest"]),
                "operation_code": str(operation["operation_code"]),
                "origin": dict(request["origin"]),
                "expected_consumer": str(operation["expected_consumer"]),
                "status": status,
                "summary": (
                    f"{operation['operation_code']} returned {terminal_state or 'unknown'}"
                ),
                "execution": {
                    "test_request_id": str(route["test_request_id"]),
                    "wire_attempt_id": str(route["wire_attempt_id"]),
                    "endpoint_id": str(route["endpoint_id"]),
                    "execution_environment_id": str(
                        route["execution_environment_id"]
                    ),
                },
                "evidence": {
                    "evidence_type": str(definition["produced_evidence_types"][0]),
                    "artifact_refs": artifact_refs,
                    "payload": {
                        "terminal_state": terminal_state,
                        "return_id": str(returned.get("return_id") or ""),
                        "receipt_id": str(returned.get("receipt_id") or ""),
                        "workflow_ingest": dict(workflow_ingest),
                    },
                },
                "failure_class": (
                    None if status == "completed" else f"wire-{terminal_state or 'failed'}"
                ),
                "completed_at": _completed_at(returned_payload),
            }
        )


def _result_status(terminal_state: str) -> str:
    if terminal_state in {"terminal-success", "completed", "pass", "passed"}:
        return "completed"
    if terminal_state in {"cancelled", "canceled", "terminal-cancelled"}:
        return "cancelled"
    return "failed"


def _completed_at(payload: Mapping[str, Any]) -> str:
    engine = payload.get("engine")
    if isinstance(engine, Mapping):
        value = str(engine.get("terminal_at") or "")
        if value:
            return value
    for field in ("completed_at", "returned_at", "observed_at"):
        value = str(payload.get(field) or "")
        if value:
            return value
    return "1970-01-01T00:00:00+00:00"


def _bounded(root: Path, path: Path) -> None:
    if path != root and root not in path.parents:
        raise ValueError(f"evidence projection escapes workspace: {path}")


def _digest(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


__all__ = ["EvidenceOperationResultRouter"]
