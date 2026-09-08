"""Synthetic records exercise the real terminal/owner/outbox repositories."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from ascendop_control.delivery import dispatch_topic
from ascendop_control.storage import outbox_repository
from ascendop_daemon.control_plane.control_database import ControlDatabase, ControlDatabaseError


def event():
    identity = {"request_id": "request-1", "attempt_id": "attempt-1", "receipt_id": "receipt-1",
                "terminal_revision": 1, "result_payload_sha256": "a" * 64, "envelope_digest": "b" * 64}
    identifier = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
    return {"schema": "ascendop.gp-terminal-ingest-event.v1", "event_id": identifier, **identity,
            "outcome": "success", "failure_domain": "", "artifact_root": "result/payload",
            "ack": {k: identity[k] for k in ("request_id", "attempt_id", "receipt_id")}}


def test_accepted_terminal_restarts_and_hands_off_while_ack_waits(tmp_path):
    database = ControlDatabase(tmp_path / "control.sqlite3")
    original = event()
    continuation = {"kind": "solver.next", "action_id": "next-exact"}
    first = database.record_gp_terminal_ingest_event(original, continuation=continuation,
        ack_request={"proof_ref": "results/accepted.json"})
    assert first["disposition"] == "recorded"
    reopened = ControlDatabase(database.path)
    def followup(row):
        reopened.bind_gp_terminal_ingest_successor(row["origin_id"], "next-exact")
        return {"disposition": "delivered"}
    result = dispatch_topic(reopened, topic="test.terminal", owner="worker", handler=followup)
    assert len(result["delivered"]) == 1 and not result["errors"]
    assert reopened.control_outbox(topic="test.ack")[0]["state"] == "pending"
    replay = reopened.record_gp_terminal_ingest_event(original, continuation=continuation,
        ack_request={"proof_ref": "results/accepted.json"})
    assert replay["disposition"] == "already-recorded"
    assert reopened.gp_terminal_ingest_event(original["event_id"])["successor_action_id"] == "next-exact"
    assert dispatch_topic(reopened, topic="test.terminal", owner="worker", handler=followup)["delivered"] == []


def test_conflicting_result_does_not_replace_accepted_event(tmp_path):
    database = ControlDatabase(tmp_path / "control.sqlite3")
    original = event()
    database.record_gp_terminal_ingest_event(original)
    with pytest.raises(ControlDatabaseError, match="identity collision"):
        database.record_gp_terminal_ingest_event({**original, "outcome": "failure"})
    assert database.gp_terminal_ingest_event(original["event_id"])["outcome"] == "success"


def test_terminal_and_ack_intents_rollback_together(tmp_path, monkeypatch):
    database = ControlDatabase(tmp_path / "control.sqlite3")
    from ascendop_daemon.storage.repositories import gp_terminal_ingest
    original_enqueue = gp_terminal_ingest.enqueue_control_intent
    def cut(connection, **kwargs):
        original_enqueue(connection, **kwargs)
        if kwargs["topic"] == "test.ack":
            raise RuntimeError("transaction cut")
    monkeypatch.setattr(gp_terminal_ingest, "enqueue_control_intent", cut)
    with pytest.raises(RuntimeError, match="transaction cut"):
        database.record_gp_terminal_ingest_event(event(), continuation={"kind": "solver.next"}, ack_request={})
    assert database.gp_terminal_ingest_event(event()["event_id"]) == {}
    assert database.control_outbox(topic="test.terminal") == []
    assert database.control_outbox(topic="test.ack") == []
