from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ascendop_protocol.management import (
    OPERATOR_WORKFLOW_PROJECTION_SCHEMA,
    WORKFLOW_TRACE_PROJECTION_SCHEMA,
    validate_operator_workflow_projection,
    validate_workflow_trace_projection,
)

from ascendop_control.storage.database import ControlStore


class WorkflowProjectionService:
    """Build read-only V5 projections from one authoritative DB snapshot."""

    def __init__(self, store: ControlStore) -> None:
        self.store = store

    def operator_workflows(self) -> list[dict[str, Any]]:
        snapshot = self._snapshot()
        return [
            self._operator_projection(registration, snapshot)
            for registration in snapshot["operators"]
        ]

    def workflow_traces(self) -> list[dict[str, Any]]:
        snapshot = self._snapshot()
        return [self._trace(action, snapshot) for action in snapshot["actions"]]

    def _snapshot(self) -> dict[str, Any]:
        tables = {
            "operators": "operator_registrations",
            "actions": "agent_actions_v4",
            "receipts": "agent_action_receipts_v4",
            "workflow_actions": "workflow_actions",
            "workflow_receipts": "workflow_action_receipts",
            "requests": "test_requests",
            "attempts": "execution_attempts",
            "returns": "transport_returns",
            "recoveries": "postprocess_recoveries",
            "endpoints": "backend_endpoints",
            "operation_requests": "evidence_operation_requests_v5",
            "operation_results": "evidence_operation_results_v5",
        }
        with self.store.connection() as conn:
            snapshot = {
                name: _table_rows(conn, table)
                for name, table in tables.items()
            }
            event = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS sequence FROM control_events"
            ).fetchone()
        snapshot["last_event_sequence"] = int(event["sequence"] if event else 0)
        snapshot["observed_at"] = _utc_now()
        return snapshot

    def _operator_projection(
        self,
        registration: Mapping[str, Any],
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        operator_id = str(registration["operator_id"])
        aliases = {operator_id, str(registration.get("display_name") or "")}
        actions = _matching(snapshot["actions"], aliases, "operator_id")
        requests = _matching(snapshot["requests"], aliases, "operator_id")
        operations = _matching(
            snapshot["operation_requests"], aliases, "operator_id"
        )
        action = _latest(actions)
        request = _latest(requests)
        operation = _latest(operations)
        receipt = _by_key(snapshot["receipts"], "action_id").get(
            str(action.get("action_id") or "")
        )
        outcome = _agent_outcome(receipt)
        attempt = _latest(
            row
            for row in snapshot["attempts"]
            if row.get("request_id") == request.get("request_id")
        )
        returned = _latest(
            row
            for row in snapshot["returns"]
            if row.get("attempt_id") == attempt.get("attempt_id")
        )
        recovery = _latest(
            row
            for row in snapshot["recoveries"]
            if row.get("attempt_id") == attempt.get("attempt_id")
        )
        endpoint = _by_key(snapshot["endpoints"], "endpoint_id").get(
            str(attempt.get("endpoint_id") or ""),
            {},
        )
        blocker = _primary_blocker(outcome, request, returned, recovery)
        owner, commands, gate_stage = _next_step(
            action=action,
            outcome=outcome,
            request=request,
            operation=operation,
            blocker=blocker,
        )
        latest_time = _latest_timestamp(
            registration,
            action,
            receipt or {},
            request,
            attempt,
            returned,
            recovery,
            operation,
        )
        value = {
            "schema": OPERATOR_WORKFLOW_PROJECTION_SCHEMA,
            "operator_id": operator_id,
            "agent_phase": str(action.get("state") or "idle"),
            "candidate_phase": _candidate_phase(action, outcome, request),
            "test_phase": str(request.get("state") or "not-requested"),
            "endpoint_phase": _endpoint_phase(endpoint, attempt),
            "recovery_phase": str(recovery.get("state") or "not-required"),
            "gate": {
                "stage": gate_stage,
                "next_owner": owner,
                "reason": blocker["details"] if blocker else "ready",
            },
            "primary_blocker": blocker,
            "headline_phase": _headline(action, request, blocker),
            "next_owner": owner,
            "allowed_commands": commands,
            "causative_evidence": {
                "agent_action_id": str(action.get("action_id") or ""),
                "agent_outcome_disposition": str(
                    outcome.get("disposition") or ""
                ),
                "evidence_operation_request_id": str(
                    operation.get("operation_request_id") or ""
                ),
                "test_request_id": str(request.get("request_id") or ""),
                "wire_attempt_id": str(attempt.get("attempt_id") or ""),
                "result_return_id": str(returned.get("return_id") or ""),
                "recovery_id": str(recovery.get("recovery_id") or ""),
            },
            "observed_at": snapshot["observed_at"],
            "last_event_sequence": int(snapshot["last_event_sequence"]),
            "freshness": _freshness(latest_time, snapshot["observed_at"]),
        }
        return validate_operator_workflow_projection(value)

    def _trace(
        self,
        action: Mapping[str, Any],
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        action_value = dict(action.get("action") or {})
        action_id = str(action["action_id"])
        operator_id = str(action["operator_id"])
        candidate_id = str(action_value.get("candidate_version") or "")
        causation = dict(action_value.get("causation") or {})
        trace_id = str(causation.get("trace_id") or action_id)
        nodes: list[dict[str, Any]] = []
        links: list[dict[str, str]] = []
        self._node(nodes, "agent-action", action_id, action)
        receipt = _by_key(snapshot["receipts"], "action_id").get(action_id)
        outcome = _agent_outcome(receipt)
        if receipt:
            outcome_id = f"outcome:{action_id}"
            self._node(nodes, "agent-action-outcome", outcome_id, receipt)
            _link(links, action_id, outcome_id, "completed-as")
        promotions = [
            row
            for row in snapshot["workflow_actions"]
            if str(dict(row.get("action") or {}).get("parent_trace_id") or "")
            == action_id
        ]
        workflow_receipts = _by_key(snapshot["workflow_receipts"], "action_id")
        latest_promotion_parent = ""
        for promotion in promotions:
            promotion_id = str(promotion["action_id"])
            self._node(nodes, "promotion-action", promotion_id, promotion)
            _link(
                links,
                f"outcome:{action_id}" if receipt else action_id,
                promotion_id,
                "promoted-by",
            )
            promotion_receipt = workflow_receipts.get(promotion_id)
            if promotion_receipt:
                receipt_id = f"promotion-receipt:{promotion_id}"
                self._node(
                    nodes,
                    "promotion-receipt",
                    receipt_id,
                    promotion_receipt,
                )
                _link(links, promotion_id, receipt_id, "completed-as")
                latest_promotion_parent = receipt_id
        candidate_node = f"candidate:{candidate_id}" if candidate_id else ""
        if candidate_node:
            self._node(
                nodes,
                "candidate",
                candidate_node,
                {
                    "state": _candidate_phase(action, outcome, {}),
                    "updated_at": action.get("updated_at"),
                    "candidate_id": candidate_id,
                },
            )
            parent = latest_promotion_parent or action_id
            _link(links, parent, candidate_node, "materialized")
        requests = [
            row
            for row in snapshot["requests"]
            if _request_belongs_to_action(row, action_id, operator_id, candidate_id)
        ]
        for request in requests:
            request_id = str(request["request_id"])
            self._node(nodes, "test-request", request_id, request)
            if candidate_node:
                _link(links, candidate_node, request_id, "tested-by")
            for attempt in snapshot["attempts"]:
                if attempt.get("request_id") != request_id:
                    continue
                attempt_id = str(attempt["attempt_id"])
                self._node(nodes, "wire-attempt", attempt_id, attempt)
                _link(links, request_id, attempt_id, "routed-as")
                for returned in snapshot["returns"]:
                    if returned.get("attempt_id") != attempt_id:
                        continue
                    result_id = str(returned["return_id"])
                    self._node(nodes, "result", result_id, returned)
                    _link(links, attempt_id, result_id, "returned-as")
        operations = [
            row
            for row in snapshot["operation_requests"]
            if row.get("origin_action_id") == action_id
        ]
        operation_results = _by_key(
            snapshot["operation_results"], "operation_request_id"
        )
        for operation in operations:
            operation_id = str(operation["operation_request_id"])
            self._node(nodes, "evidence-operation", operation_id, operation)
            _link(links, action_id, operation_id, "requested-evidence")
            result = operation_results.get(operation_id)
            if result:
                result_id = str(result["operation_result_id"])
                self._node(nodes, "evidence", result_id, result)
                _link(links, operation_id, result_id, "produced")
                for next_action in snapshot["actions"]:
                    next_value = dict(next_action.get("action") or {})
                    next_causation = dict(next_value.get("causation") or {})
                    if next_causation.get("operation_request_id") == operation_id:
                        next_id = str(next_action["action_id"])
                        self._node(nodes, "next-agent-action", next_id, next_action)
                        _link(links, result_id, next_id, "resumed-by")
        gaps = _trace_gaps(nodes, candidate_node)
        latest_time = _latest_timestamp(*nodes)
        value = {
            "schema": WORKFLOW_TRACE_PROJECTION_SCHEMA,
            "trace_id": trace_id,
            "operator_id": operator_id,
            "nodes": nodes,
            "links": links,
            "gaps": gaps,
            "observed_at": snapshot["observed_at"],
            "last_event_sequence": int(snapshot["last_event_sequence"]),
            "freshness": _freshness(latest_time, snapshot["observed_at"]),
        }
        return validate_workflow_trace_projection(value)

    @staticmethod
    def _node(
        nodes: list[dict[str, Any]],
        kind: str,
        identity: str,
        source: Mapping[str, Any],
    ) -> None:
        if not identity or any(item["id"] == identity for item in nodes):
            return
        nodes.append(
            {
                "kind": kind,
                "id": identity,
                "state": str(
                    source.get("state")
                    or source.get("status")
                    or source.get("disposition")
                    or "observed"
                ),
                "observed_at": _latest_timestamp(source),
            }
        )


def _table_rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    try:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    except sqlite3.OperationalError:
        return []
    return [_decode(row) for row in rows]


def _decode(row: sqlite3.Row) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key in row.keys():
        item = row[key]
        if key.endswith("_json") and isinstance(item, str):
            try:
                value[key[:-5]] = json.loads(item)
                continue
            except json.JSONDecodeError:
                pass
        value[key] = item
    return value


def _matching(
    rows: Iterable[Mapping[str, Any]], aliases: set[str], field: str
) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get(field) or "") in aliases]


def _latest(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    values = [dict(row) for row in rows]
    return (
        max(values, key=lambda row: (_latest_timestamp(row), _row_identity(row)))
        if values
        else {}
    )


def _row_identity(row: Mapping[str, Any]) -> str:
    for field in (
        "action_id",
        "operation_request_id",
        "request_id",
        "attempt_id",
        "return_id",
        "recovery_id",
        "endpoint_id",
    ):
        value = str(row.get(field) or "")
        if value:
            return value
    return ""


def _by_key(
    rows: Iterable[Mapping[str, Any]], field: str
) -> dict[str, dict[str, Any]]:
    return {str(row.get(field) or ""): dict(row) for row in rows}


def _agent_outcome(receipt: Mapping[str, Any] | None) -> dict[str, Any]:
    if not receipt:
        return {}
    value = receipt.get("receipt") or receipt
    completion = dict(value.get("completion") or {})
    return dict(completion.get("agent_action_outcome") or {})


def _primary_blocker(
    outcome: Mapping[str, Any],
    request: Mapping[str, Any],
    returned: Mapping[str, Any],
    recovery: Mapping[str, Any],
) -> dict[str, str] | None:
    blocker = outcome.get("blocker")
    if isinstance(blocker, Mapping):
        return {
            "kind": str(blocker.get("kind") or "agent"),
            "code": str(blocker.get("code") or "agent.blocked"),
            "details": str(blocker.get("details") or "Agent reported a blocker"),
            "source_ref": f"agent-action:{outcome.get('action_id')}",
        }
    if str(request.get("blocker") or ""):
        return {
            "kind": "request",
            "code": "test-request.blocked",
            "details": str(request["blocker"]),
            "source_ref": f"test-request:{request.get('request_id')}",
        }
    if str(returned.get("hold_reason") or ""):
        return {
            "kind": "external_dependency",
            "code": "transport-result.held",
            "details": str(returned["hold_reason"]),
            "source_ref": f"result:{returned.get('return_id')}",
        }
    if recovery and str(recovery.get("state") or "") == "failed":
        return {
            "kind": "recovery",
            "code": "postprocess-recovery.failed",
            "details": str(recovery.get("error") or "postprocess recovery failed"),
            "source_ref": f"recovery:{recovery.get('recovery_id')}",
        }
    return None


def _next_step(
    *,
    action: Mapping[str, Any],
    outcome: Mapping[str, Any],
    request: Mapping[str, Any],
    operation: Mapping[str, Any],
    blocker: Mapping[str, Any] | None,
) -> tuple[str, list[str], str]:
    disposition = str(outcome.get("disposition") or "")
    if disposition == "protocol_gap":
        return "developer", ["developer.repair-capability"], "capability-gap"
    if disposition == "blocked_external":
        kind = str(dict(outcome.get("blocker") or {}).get("kind") or "")
        command = (
            "manager.request-user-decision"
            if kind == "user_policy_decision"
            else "manager.review-notification"
        )
        return "manager", [command], "blocked"
    action_state = str(action.get("state") or "")
    if action_state in {"queued", "claimed", "running", "retry-pending"}:
        return str(action.get("role") or "solver"), ["agent.complete-action"], "agent"
    if blocker:
        return "manager", ["manager.review-notification"], "blocked"
    if disposition == "request_evidence":
        code = str(
            dict(outcome.get("requested_operation") or {}).get("operation_code")
            or operation.get("operation_code")
            or "unknown"
        )
        return "daemon-harness", [f"evidence-operation:{code}"], "evidence"
    if disposition == "proposed_change":
        return "daemon-harness", ["daemon.promote-candidate"], "promotion"
    request_state = str(request.get("state") or "")
    if request_state in {"queued", "preparing", "routed", "running"}:
        return "daemon-harness", ["daemon.wait-for-result"], "testing"
    if request_state in {"completed", "terminal"}:
        return "solver", ["solver.iterate"], "iteration"
    return "solver", ["solver.iterate"], "iteration"


def _candidate_phase(
    action: Mapping[str, Any],
    outcome: Mapping[str, Any],
    request: Mapping[str, Any],
) -> str:
    if request:
        return "under-test"
    if outcome.get("disposition") == "proposed_change":
        return "promotion-ready"
    if action:
        return "agent-edit" if action.get("state") != "completed" else "unchanged"
    return "not-created"


def _endpoint_phase(
    endpoint: Mapping[str, Any], attempt: Mapping[str, Any]
) -> str:
    if not attempt:
        return "unassigned"
    if not endpoint:
        return "unknown"
    if bool(endpoint.get("draining")):
        return "draining"
    return "ready" if bool(endpoint.get("enabled", True)) else "disabled"


def _headline(
    action: Mapping[str, Any],
    request: Mapping[str, Any],
    blocker: Mapping[str, Any] | None,
) -> str:
    if blocker:
        return "blocked"
    if request:
        return f"test:{request.get('state') or 'observed'}"
    if action:
        return f"agent:{action.get('state') or 'observed'}"
    return "ready-for-iteration"


def _request_belongs_to_action(
    request: Mapping[str, Any],
    action_id: str,
    operator_id: str,
    candidate_id: str,
) -> bool:
    manifest = dict(request.get("manifest") or {})
    lineage = dict(dict(manifest.get("workflow") or {}).get("lineage") or {})
    if lineage.get("origin_action_id") == action_id:
        return True
    return bool(
        candidate_id
        and request.get("operator_id") == operator_id
        and request.get("test_version") == candidate_id
    )


def _trace_gaps(nodes: list[dict[str, Any]], candidate_node: str) -> list[str]:
    kinds = {str(node["kind"]) for node in nodes}
    gaps: list[str] = []
    if "agent-action-outcome" not in kinds:
        gaps.append("agent-action-outcome")
    if candidate_node and "promotion-receipt" not in kinds:
        gaps.append("promotion-receipt")
    if "test-request" in kinds and "wire-attempt" not in kinds:
        gaps.append("wire-attempt")
    if "wire-attempt" in kinds and "result" not in kinds:
        gaps.append("result")
    return gaps


def _link(
    links: list[dict[str, str]], source: str, target: str, relation: str
) -> None:
    value = {"from": source, "to": target, "relation": relation}
    if source and target and value not in links:
        links.append(value)


def _latest_timestamp(*values: Mapping[str, Any]) -> str:
    candidates: list[str] = []
    for value in values:
        for field in (
            "updated_at",
            "completed_at",
            "received_at",
            "observed_at",
            "created_at",
        ):
            item = value.get(field)
            if isinstance(item, str) and item:
                candidates.append(item)
    return max(candidates, key=_instant) if candidates else _utc_now()


def _freshness(source: str, observed: str) -> dict[str, Any]:
    age = max(0, int((_instant(observed) - _instant(source)).total_seconds()))
    return {
        "state": "fresh" if age <= 120 else "stale",
        "age_seconds": age,
        "source_updated_at": source,
    }


def _instant(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = ["WorkflowProjectionService"]
