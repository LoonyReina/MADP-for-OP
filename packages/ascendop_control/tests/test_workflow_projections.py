from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from ascendop_control.application import WorkflowProjectionService
from ascendop_control.storage import ControlStore


def test_projection_exposes_full_lineage_and_legal_next_step(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    action = {
        "action_id": "agent-1",
        "iteration_id": "iteration-1",
        "operator_id": "demo.op",
        "role": "solver",
        "candidate_version": "Demo_V1_2",
    }
    outcome = {
        "action_id": "agent-1",
        "execution_status": "completed",
        "disposition": "request_evidence",
        "requested_operation": {"operation_code": "test.correctness"},
    }
    next_action = {
        **action,
        "action_id": "agent-2",
        "iteration_id": "iteration-2",
        "causation": {
            "trace_id": "agent-1",
            "parent_action_id": "agent-1",
            "operation_request_id": "evidence-1",
        },
    }
    with store.connection() as conn:
        conn.execute(
            "INSERT INTO operator_registrations VALUES(?,?,?)",
            ("demo.op", "Demo", now),
        )
        conn.execute(
            "INSERT INTO agent_actions_v4 VALUES(?,?,?,?,?,?,?)",
            ("agent-1", "demo.op", "solver", "completed", json.dumps(action), now, now),
        )
        conn.execute(
            "INSERT INTO agent_actions_v4 VALUES(?,?,?,?,?,?,?)",
            ("agent-2", "demo.op", "solver", "queued", json.dumps(next_action), now, now),
        )
        conn.execute(
            "INSERT INTO agent_action_receipts_v4 VALUES(?,?,?,?)",
            (
                "agent-1",
                "completed",
                json.dumps({"completion": {"agent_action_outcome": outcome}}),
                now,
            ),
        )
        promotion = {"parent_trace_id": "agent-1"}
        conn.execute(
            "INSERT INTO workflow_actions VALUES(?,?,?,?,?,?,?)",
            ("promotion-1", "Demo", "Demo_V1_2", "completed", json.dumps(promotion), now, now),
        )
        conn.execute(
            "INSERT INTO workflow_action_receipts VALUES(?,?,?,?)",
            ("promotion-1", "completed", "{}", now),
        )
        manifest = {
            "workflow": {"lineage": {"origin_action_id": "agent-1"}}
        }
        conn.execute(
            "INSERT INTO test_requests VALUES(?,?,?,?,?,?,?,?,?)",
            ("request-1", "demo.op", "Demo_V1_2", "completed", "", json.dumps(manifest), "request.json", now, now),
        )
        conn.execute(
            "INSERT INTO execution_attempts VALUES(?,?,?,?,?,?,?)",
            ("attempt-1", "request-1", "completed", "endpoint-1", "", now, now),
        )
        conn.execute(
            "INSERT INTO backend_endpoints VALUES(?,?,?,?,?)",
            ("endpoint-1", 1, 0, now, "{}"),
        )
        conn.execute(
            "INSERT INTO transport_returns VALUES(?,?,?,?,?,?,?)",
            ("return-1", "attempt-1", "terminal-success", "", "{}", now, now),
        )
        operation_request = {
            "origin": {"action_id": "agent-1", "iteration_id": "iteration-1"}
        }
        conn.execute(
            "INSERT INTO evidence_operation_requests_v5 VALUES(?,?,?,?,?,?,?,?,?,?)",
            ("evidence-1", "demo.op", "agent-1", "completed", "test.correctness", json.dumps(operation_request), "request-1", "attempt-1", now, now),
        )
        conn.execute(
            "INSERT INTO evidence_operation_results_v5 VALUES(?,?,?,?,?)",
            ("evidence-result-1", "evidence-1", "completed", "{}", now),
        )
        conn.execute(
            "INSERT INTO control_events(event_at,event_type,entity_type,entity_id,payload_json) VALUES(?,?,?,?,?)",
            (now, "result-projected", "operator", "demo.op", "{}"),
        )

    service = WorkflowProjectionService(store)
    projection = service.operator_workflows()[0]
    trace = next(item for item in service.workflow_traces() if item["trace_id"] == "agent-1")

    assert projection["freshness"]["state"] == "fresh"
    assert projection["last_event_sequence"] == 1
    assert projection["next_owner"] == "solver"
    assert projection["allowed_commands"] == ["agent.complete-action"]
    assert projection["causative_evidence"]["test_request_id"] == "request-1"
    assert {
        "agent-action",
        "agent-action-outcome",
        "promotion-action",
        "promotion-receipt",
        "candidate",
        "test-request",
        "wire-attempt",
        "result",
        "evidence-operation",
        "evidence",
        "next-agent-action",
    }.issubset({node["kind"] for node in trace["nodes"]})
    assert trace["gaps"] == []
    assert {
        "from": "promotion-receipt:promotion-1",
        "to": "candidate:Demo_V1_2",
        "relation": "materialized",
    } in trace["links"]

    with store.connection() as conn:
        conn.execute(
            "DELETE FROM workflow_action_receipts WHERE action_id=?",
            ("promotion-1",),
        )
    broken_trace = next(
        item
        for item in service.workflow_traces()
        if item["trace_id"] == "agent-1"
    )
    assert "promotion-receipt" in broken_trace["gaps"]


def test_projection_routes_protocol_gap_to_developer(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    action = {
        "action_id": "agent-gap",
        "iteration_id": "iteration-gap",
        "operator_id": "demo.op",
        "role": "solver",
        "candidate_version": "Demo_V1_1",
    }
    blocker = {
        "kind": "capability_gap",
        "code": "evidence.missing",
        "details": "registered evidence operation is missing",
        "resume_condition": "operation is installed",
    }
    outcome = {
        "action_id": "agent-gap",
        "execution_status": "completed",
        "disposition": "protocol_gap",
        "blocker": blocker,
    }
    with store.connection() as conn:
        conn.execute("INSERT INTO operator_registrations VALUES(?,?,?)", ("demo.op", "Demo", now))
        conn.execute(
            "INSERT INTO agent_actions_v4 VALUES(?,?,?,?,?,?,?)",
            ("agent-gap", "demo.op", "solver", "completed", json.dumps(action), now, now),
        )
        conn.execute(
            "INSERT INTO agent_action_receipts_v4 VALUES(?,?,?,?)",
            ("agent-gap", "completed", json.dumps({"completion": {"agent_action_outcome": outcome}}), now),
        )

    projection = WorkflowProjectionService(store).operator_workflows()[0]
    assert projection["headline_phase"] == "blocked"
    assert projection["next_owner"] == "developer"
    assert projection["allowed_commands"] == ["developer.repair-capability"]
    assert projection["primary_blocker"]["code"] == "evidence.missing"


def _store(tmp_path: Path) -> ControlStore:
    path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE operator_registrations(operator_id TEXT, display_name TEXT, updated_at TEXT);
        CREATE TABLE agent_actions_v4(action_id TEXT, operator_id TEXT, role TEXT, state TEXT, action_json TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE agent_action_receipts_v4(action_id TEXT, status TEXT, receipt_json TEXT, completed_at TEXT);
        CREATE TABLE workflow_actions(action_id TEXT, operator_id TEXT, test_version TEXT, state TEXT, action_json TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE workflow_action_receipts(action_id TEXT, status TEXT, receipt_json TEXT, completed_at TEXT);
        CREATE TABLE test_requests(request_id TEXT, operator_id TEXT, test_version TEXT, state TEXT, blocker TEXT, manifest_json TEXT, manifest_path TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE execution_attempts(attempt_id TEXT, request_id TEXT, state TEXT, endpoint_id TEXT, error TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE transport_returns(return_id TEXT, attempt_id TEXT, state TEXT, hold_reason TEXT, payload_json TEXT, received_at TEXT, acknowledged_at TEXT);
        CREATE TABLE postprocess_recoveries(recovery_id TEXT, attempt_id TEXT, state TEXT, error TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE backend_endpoints(endpoint_id TEXT, enabled INTEGER, draining INTEGER, updated_at TEXT, config_json TEXT);
        CREATE TABLE evidence_operation_requests_v5(operation_request_id TEXT, operator_id TEXT, origin_action_id TEXT, state TEXT, operation_code TEXT, request_json TEXT, test_request_id TEXT, wire_attempt_id TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE evidence_operation_results_v5(operation_result_id TEXT, operation_request_id TEXT, status TEXT, result_json TEXT, completed_at TEXT);
        CREATE TABLE control_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_at TEXT, event_type TEXT, entity_type TEXT, entity_id TEXT, payload_json TEXT);
        """
    )
    conn.commit()
    conn.close()
    return ControlStore(path)
