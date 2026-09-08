from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ascendop_control.delivery import deliver_claim, dispatch_topic
from ascendop_control.storage import outbox_repository
from test_agent_repository import _store


def enqueue(store, origin, topic="test.terminal"):
    with store.transaction() as connection:
        return outbox_repository.enqueue_control_intent(
            connection, origin_id=origin, attempt_id="attempt", topic=topic,
            payload={"event": origin}, created_at=datetime.now(timezone.utc).isoformat(),
        )


def test_pending_ack_does_not_block_another_topic(tmp_path):
    store = _store(tmp_path)
    enqueue(store, "accepted", "test.ack")
    terminal = enqueue(store, "accepted")
    ack = dispatch_topic(store, topic="test.ack", owner="ack",
                         handler=lambda _: {"disposition": "pending"})
    result = dispatch_topic(store, topic="test.terminal", owner="continuation",
                            handler=lambda _: {"disposition": "delivered"})
    assert ack == {"delivered": [], "errors": []}
    assert result == {"delivered": [terminal], "errors": []}
    assert store.control_outbox(topic="test.ack")[0]["state"] == "pending"


def test_bad_handler_does_not_block_healthy_claim(tmp_path):
    store = _store(tmp_path)
    enqueue(store, "bad")
    healthy = enqueue(store, "good")
    def handler(row):
        if row["origin_id"] == "bad":
            raise ValueError("invalid integration record")
        return {"disposition": "delivered"}
    result = dispatch_topic(store, topic="test.terminal", owner="worker", handler=handler)
    assert result["delivered"] == [healthy]
    assert len(result["errors"]) == 1


def test_pause_retains_accepted_intent(tmp_path):
    store = _store(tmp_path)
    enqueue(store, "accepted", "workspace.action")
    result = dispatch_topic(store, topic="workspace.action", owner="worker",
                            handler=lambda _: {"disposition": "paused"})
    assert result == {"delivered": [], "errors": []}
    assert store.control_outbox(topic="workspace.action")[0]["state"] == "pending"


def test_process_cut_preserves_claim_and_replays_same_identity(tmp_path, monkeypatch):
    store = _store(tmp_path)
    identifier = enqueue(store, "accepted")
    effects = set()
    class ProcessCut(BaseException):
        pass
    def interrupted(row):
        effects.add(row["outbox_id"])
        raise ProcessCut()
    with pytest.raises(ProcessCut):
        dispatch_topic(store, topic="test.terminal", owner="old", handler=interrupted)
    assert store.control_outbox(topic="test.terminal")[0]["state"] == "claimed"
    monkeypatch.setattr(outbox_repository, "_now",
        lambda: (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat())
    def recovered(row):
        effects.add(row["outbox_id"])
        return {"disposition": "delivered"}
    result = dispatch_topic(store, topic="test.terminal", owner="new", handler=recovered)
    assert result["delivered"] == [identifier]
    assert effects == {identifier}
    assert store.control_outbox(topic="test.terminal")[0]["attempts"] == 2


def test_lost_claim_does_not_overwrite_next_owner(tmp_path, monkeypatch):
    store = _store(tmp_path)
    enqueue(store, "accepted")
    old = store.claim_control_outbox(topic="test.terminal", owner="old")
    monkeypatch.setattr(outbox_repository, "_now",
        lambda: (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat())
    new = store.claim_control_outbox(topic="test.terminal", owner="new")
    result = deliver_claim(store, old, lambda _: {"disposition": "delivered"})
    assert result["errors"][0]["defer_error"]
    saved = store.control_outbox(topic="test.terminal")[0]
    assert saved["claim_token"] == new["claim_token"]
    assert saved["state"] == "claimed"
