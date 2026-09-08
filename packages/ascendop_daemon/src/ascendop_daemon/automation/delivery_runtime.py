"""Existing outbox delivery lifecycle; no replacement queue or completion authority."""
from __future__ import annotations
from contextlib import contextmanager
from threading import Event, Thread
from typing import Any, Mapping
from ascendop_control.delivery import deliver_claim
REQUEST_LEASE_SECONDS = 60
REQUEST_RENEW_INTERVAL_SECONDS = 20

@contextmanager
def _keep_request_claim(database: Any, row: Mapping[str, Any], *,
                        lease_seconds=REQUEST_LEASE_SECONDS,
                        interval_seconds=REQUEST_RENEW_INTERVAL_SECONDS, thread_factory=Thread):
    """A blocking Gateway publication retains its original outbox delivery lease."""
    stopped = Event()
    failures = []

    def renew():
        database.renew_control_outbox(
            outbox_id=row["outbox_id"], claim_token=row["claim_token"],
            lease_seconds=lease_seconds,
        )

    def heartbeat():
        while not stopped.wait(interval_seconds):
            try:
                renew()
            except Exception as exc:
                failures.append(exc)
                return  # A lost token must never be reacquired by this delivery.

    renew()  # Fence a stale claim before entering the transport side effect.
    worker = thread_factory(target=heartbeat, name="test-request-claim-renewal", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stopped.set()
        worker.join()
    if failures:
        raise failures[0]


def reconcile_delivery(database, deliver, *, stop_requested, ingest_terminals, owner,
    limit=4, origin_id=None, allow_test_continuations=True, test_request_limit=None,
    _local_terminal_only=False):
    delivered, errors = [], []
    if origin_id is None and not _local_terminal_only:
        priority = ingest_terminals(allow_test_continuations)
        delivered.extend(priority["delivered"])
        errors.extend(priority["errors"])
    topics = (("test.terminal", "workspace.project") if _local_terminal_only else
        ("agent.native-terminal", "agent.completion", "agent.outcome", "workspace.project", "agent.followup", "test.request", "test.publication", "test.terminal", "official.terminal", "workspace.action", "test.ack"))
    work = [
        topic
        for topic in topics
        if topic not in {"agent.followup", "test.request", "test.terminal", "official.terminal", "workspace.action"} or (allow_test_continuations and not stop_requested())
    ]
    for topic in work:
        topic_limit = min(limit, test_request_limit) if topic == "test.request" and test_request_limit is not None else limit
        for _ in range(max(0, min(int(topic_limit), 100))):
            if topic == "test.request" and origin_id is None:
                # A terminal may land during the previous blocking submit.
                # Do not hold a fresh request claim while accepting it.
                priority = ingest_terminals(allow_test_continuations)
                delivered.extend(priority["delivered"])
                errors.extend(priority["errors"])
            claimed = database.claim_control_outbox(
                topic=topic,
                owner=owner,
                origin_id=origin_id,
            )
            if claimed is None:
                break
            result = deliver_claim(database, claimed, deliver)
            delivered.extend(result["delivered"])
            errors.extend(result["errors"])
        if topic == "test.request" and origin_id is None:
            # Includes a terminal that landed during the last permitted
            # submit; the normal watcher can expose its successor now.
            priority = ingest_terminals(allow_test_continuations)
            delivered.extend(priority["delivered"])
            errors.extend(priority["errors"])
    return {"delivered": delivered, "errors": errors}
