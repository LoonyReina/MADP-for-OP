from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from ascendop_control.storage.outbox_repository import enqueue_control_intent
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.automation.workspace_snapshot import read_workspace_snapshot
from ascendop_daemon.automation.workspace_projection_port import project_workspace


def fixture(tmp_path):
    database = ControlDatabase(tmp_path / "control.sqlite3")
    database.initialize()
    owner = {"schema": "ascendop.workspace-owner.v1", "workspace": "workspaces/demo",
        "campaign_id": "synthetic", "operator_id": "demo", "action_id": "action-1",
        "attempt_id": "attempt-1", "lease_id": "lease-1", "principal_id": "solver",
        "native_session_id": "session-1", "revision": 1}
    record = {"context": {"operator_id": "demo", "request": {"logical_request_id": "request-1"},
        "test": {"mode": "correctness", "case_version": "cases-1"}, "case_path": "cases/demo",
        "input_source_sha256": "a" * 64}}
    database.admit_workspace_action(binding=owner, expected_action_id="", expected_revision=0,
        plan={"record": record})
    return database, owner, record


def test_ack_bookkeeping_does_not_change_workspace_revision(tmp_path):
    database, owner, _ = fixture(tmp_path)
    before = read_workspace_snapshot(database, owner["workspace"])["view"]
    with database.transaction() as connection:
        enqueue_control_intent(connection, origin_id="accepted-1", attempt_id="attempt-1", topic="test.ack",
            payload={"retry": True}, created_at=datetime.now(timezone.utc).isoformat())
    claim = database.claim_control_outbox(topic="test.ack", owner="ack")
    database.defer_control_outbox(outbox_id=claim["outbox_id"], claim_token=claim["claim_token"], error="network waiting")
    after = read_workspace_snapshot(database, owner["workspace"])["view"]
    assert after == before
    assert after["server_feedback"]["state"] == "not_run"


def test_projection_barrier_and_owner_fence(tmp_path):
    database, owner, _ = fixture(tmp_path)
    view = read_workspace_snapshot(database, owner["workspace"])["view"]
    with pytest.raises(ValueError, match="not accepted"):
        read_workspace_snapshot(database, owner["workspace"], required_revision=view["revision"] + 100)
    with pytest.raises(ValueError, match="fenced"):
        read_workspace_snapshot(database, owner["workspace"], expected_action_id="another-action")


def test_projection_repairs_view_from_database_without_reacceptance(tmp_path):
    database, owner, _ = fixture(tmp_path)
    def publish(directory, snapshot):
        target = directory / ".ascendop/ITERATION.json"
        target.write_text(json.dumps(snapshot["view"]), encoding="utf-8")
    before = project_workspace(tmp_path, database, owner["workspace"], publish=publish)
    target = tmp_path / owner["workspace"] / ".ascendop/ITERATION.json"
    target.write_text('{"next":"fake pass"}', encoding="utf-8")
    after = project_workspace(tmp_path, database, owner["workspace"], publish=publish)
    assert before == after == json.loads(target.read_text())


def test_historical_terminal_does_not_replace_new_candidate(tmp_path):
    database, owner, record = fixture(tmp_path)
    second = {**owner, "action_id": "action-2", "attempt_id": "attempt-2", "revision": 2,
              "lease_id": "lease-2"}
    database.admit_workspace_action(binding=second, expected_action_id="action-1", expected_revision=1,
        plan={"record": {"context": {**record["context"], "request": {"logical_request_id": "request-2"}}}})
    with database.transaction() as connection:
        enqueue_control_intent(connection, origin_id="late-result", attempt_id="attempt-1", topic="test.terminal",
            payload={"event": {"event_id": "late-result", "request_id": "request-1", "outcome": "success", "failure_domain": ""},
                "continuation": {"workspace": owner["workspace"], "source_action_id": "action-1", "kind": "official.wait",
                    "proof_ref": "results/old.json", "business_summary": {"full_correctness_pass": True}}},
            created_at=datetime.now(timezone.utc).isoformat())
    view = read_workspace_snapshot(database, owner["workspace"])["view"]
    assert view["owner"] == second and view["candidate"]["candidate_id"] == "request-2"
    assert view["server_feedback"]["state"] == "not_run"
    assert view["next"]["action"] == "work"
    assert view["recent_results"][0]["full_correctness_pass"] is True
