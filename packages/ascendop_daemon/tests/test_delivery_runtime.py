from datetime import datetime, timezone
from threading import Event, Thread
import time

from ascendop_daemon.automation.delivery_runtime import reconcile_delivery, _keep_request_claim
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_control.storage.outbox_repository import enqueue_control_intent


def store(tmp_path):
    database = ControlDatabase(tmp_path / "control.db")
    database.initialize()
    return database


def put(database, topic, identity):
    with database.transaction() as connection:
        return enqueue_control_intent(connection, origin_id=identity, attempt_id=identity,
            topic=topic, payload={"identity": identity}, created_at=datetime.now(timezone.utc).isoformat())


def test_terminal_arriving_during_slow_submit_precedes_next_request(tmp_path):
    database = store(tmp_path)
    for identity in ("one", "two"):
        put(database, "test.request", identity)
    calls = []
    def deliver(row):
        calls.append(row["topic"])
        if calls == ["test.request"]:
            put(database, "test.terminal", "first-result")
        return {"disposition": "accepted"}
    def terminals(allowed):
        return reconcile_delivery(database, deliver, stop_requested=lambda: False,
            ingest_terminals=terminals, owner="terminal", allow_test_continuations=allowed,
            _local_terminal_only=True)
    result = reconcile_delivery(database, deliver, stop_requested=lambda: False,
        ingest_terminals=terminals, owner="fixture", limit=2)
    assert not result["errors"]
    assert calls == ["test.request", "test.terminal", "test.request"]


def test_pause_drains_accepted_publication_but_preserves_new_work_until_resume(tmp_path):
    database = store(tmp_path)
    for topic in ("test.request", "test.publication", "official.terminal", "agent.followup"):
        put(database, topic, topic)
    seen = []
    def deliver(row):
        seen.append(row["topic"])
        return {"disposition": "accepted"}
    def tick(stopped):
        return reconcile_delivery(database, deliver, stop_requested=lambda: stopped,
            ingest_terminals=lambda _: {"delivered": [], "errors": []}, owner="fixture", limit=1)
    assert not tick(True)["errors"]
    assert seen == ["test.publication"]
    assert database.control_outbox(topic="test.request")[0]["attempts"] == 0
    assert not tick(False)["errors"]
    assert set(seen) == {"test.request", "test.publication", "official.terminal", "agent.followup"}


def test_slow_publication_renews_original_lease_and_joins_worker(tmp_path):
    database = store(tmp_path)
    put(database, "test.request", "one")
    row = database.claim_control_outbox(topic="test.request", owner="fixture")
    observed, workers = Event(), []
    original = database.renew_control_outbox
    calls = []
    def renew(**kwargs):
        result = original(**kwargs)
        calls.append(result)
        if len(calls) > 1:
            observed.set()
        return result
    database.renew_control_outbox = renew
    def thread(**kwargs):
        value = Thread(**kwargs)
        workers.append(value)
        return value
    with _keep_request_claim(database, row, interval_seconds=.005, thread_factory=thread):
        assert observed.wait(3)
        assert ControlDatabase(database.path).claim_control_outbox(topic="test.request", owner="other") is None
    assert workers and all(not worker.is_alive() for worker in workers)
    assert {r["claim_token"] for r in calls} == {row["claim_token"]}


def test_delivery_limit_leaves_other_requests_unclaimed(tmp_path):
    database = store(tmp_path)
    for identity in ("one", "two", "three"):
        put(database, "test.request", identity)
    result = reconcile_delivery(database, lambda _: {"disposition": "accepted"},
        stop_requested=lambda: False, ingest_terminals=lambda _: {"delivered": [], "errors": []},
        owner="fixture", limit=4, test_request_limit=1)
    assert len(result["delivered"]) == 1
    assert sum(row["state"] == "pending" for row in database.control_outbox(topic="test.request")) == 2
