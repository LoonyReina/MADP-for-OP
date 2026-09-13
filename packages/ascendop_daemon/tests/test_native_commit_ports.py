"""Admission and receipt writes reuse owner CAS and the original control outbox."""
from datetime import datetime, timezone

import pytest

from ascendop_control.storage.outbox_repository import enqueue_control_intent
from ascendop_control.storage.errors import ControlRepositoryError
from ascendop_daemon.automation import native_commit
from test_workspace_snapshot import fixture


def start(database, owner):
    with database.transaction() as connection:
        enqueue_control_intent(connection, topic="agent.native-start", origin_id=owner["action_id"],
            attempt_id=owner["attempt_id"], payload={"binding": owner},
            created_at=datetime.now(timezone.utc).isoformat())
    return database.claim_control_outbox(topic="agent.native-start", owner="fixture")


def test_native_admission_commits_before_process_activation_and_rejects_lost_token(tmp_path):
    database, owner, _ = fixture(tmp_path)
    claim = start(database, owner)
    result = {"turn_id": "turn-1", "pid": 123, "start_token": "original-birth"}
    native_commit.accept_native_start(database, claim, result)
    saved = database.control_outbox(topic="agent.native-start")[0]
    assert saved["state"] == "delivered" and saved["result"] == result
    with pytest.raises(ControlRepositoryError):
        native_commit.accept_native_start(database, claim, result)
    assert database.control_outbox(topic="agent.native-start")[0] == saved


def test_new_owner_fences_old_suspended_process_release(tmp_path):
    database, owner, _ = fixture(tmp_path)
    claim = start(database, owner)
    successor = {**owner, "action_id": "action-2", "attempt_id": "attempt-2", "lease_id": "lease-2", "revision": 2}
    database.admit_workspace_action(binding=successor, expected_action_id=owner["action_id"],
        expected_revision=1, plan={"record": {}})
    with pytest.raises(ValueError, match="newer workspace owner"):
        native_commit.accept_native_start(database, claim, {"turn_id": "turn-1"})
    assert database.control_outbox(topic="agent.native-start")[0]["state"] == "claimed"


def test_native_terminal_and_projection_rollback_together_then_replay(tmp_path, monkeypatch):
    database, owner, _ = fixture(tmp_path)
    payload = {"binding": owner, "delivery": {"native_turn_id": "turn-1"}, "status": "completed"}
    original = native_commit.enqueue_workspace_projection
    def cut(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("before commit")
    monkeypatch.setattr(native_commit, "enqueue_workspace_projection", cut)
    with pytest.raises(RuntimeError, match="before commit"):
        native_commit.record_native_terminal(database, payload)
    assert database.control_outbox(topic="agent.native-terminal") == []
    monkeypatch.setattr(native_commit, "enqueue_workspace_projection", original)
    first = native_commit.record_native_terminal(database, payload)
    assert native_commit.record_native_terminal(database, payload)["outbox_id"] == first["outbox_id"]
    assert len(database.control_outbox(topic="agent.native-terminal")) == 1
