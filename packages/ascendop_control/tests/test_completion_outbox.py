from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ascendop_control.storage import ControlStore
from ascendop_control.storage.errors import ControlRepositoryError
from ascendop_control.storage import outbox_repository
from test_agent_repository import _store, _registration, _action, _snapshot, _receipt


def _running(root: Path):
    store = _store(root)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    store.create_agent_action(_action("1"), _snapshot("1"))
    claim = store.claim_agent_action(runner_id="runner", boot_id="boot", lease_seconds=60)
    assert claim is not None
    store.start_agent_action(action_id="action-1", lease_token=claim["lease"]["lease_token"], session_id="session")
    return store, claim, _receipt("1", claim)


@pytest.mark.parametrize("created", [
    "2026-09-05T12:00:00.123456Z",
    "2026-09-05T12:00:00.123456+00:00",
    "2026-09-05T20:00:00.123456+08:00",
    "2026-09-05T12:00:00Z",
])
def test_due_time_uses_one_utc_representation_even_at_identical_clock_tick(tmp_path, monkeypatch, created):
    store = _store(tmp_path)
    monkeypatch.setattr(outbox_repository, "_now", lambda: "2026-09-05T12:00:00.123456+00:00")
    with store.transaction() as connection:
        outbox_repository.enqueue_control_intent(
            connection, origin_id="event-1", attempt_id="attempt-1", topic="test.ack",
            payload={"accepted": True}, created_at=created,
        )
    claimed = store.claim_control_outbox(topic="test.ack", owner="ack-worker")
    assert claimed is not None
    assert claimed["attempts"] == 1


@pytest.mark.parametrize("cut", ["before-commit", "after-commit"])
def test_process_death_never_leaves_receipt_without_intent(tmp_path: Path, cut: str) -> None:
    store, claim, receipt = _running(tmp_path)
    code = """
import json, os, sys
from pathlib import Path
from ascendop_control.storage import ControlStore
from ascendop_control.storage import repository
data = json.loads(sys.stdin.read())
store = ControlStore(Path(data['database']))
original = repository.enqueue_control_intent
def cut_after_insert(*args, **kwargs):
    original(*args, **kwargs)
    os._exit(93)
if data['cut'] == 'before-commit':
    repository.enqueue_control_intent = cut_after_insert
store.complete_agent_action(data['receipt'], lease_token=data['token'], continuation={'workflow_actions': []})
os._exit(93)
"""
    child = subprocess.run(
        [sys.executable, "-B", "-c", code], input=json.dumps({
            "database": str(store.path), "cut": cut, "receipt": receipt,
            "token": claim["lease"]["lease_token"],
        }), text=True, capture_output=True, timeout=15,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    assert child.returncode == 93, child.stderr
    reopened = ControlStore(store.path)
    rows = reopened.control_outbox(topic="agent.completion")
    if cut == "before-commit":
        assert reopened.agent_action_receipt("action-1") is None
        assert rows == []
        assert reopened.agent_action("action-1")["state"] == "running"
    else:
        assert reopened.agent_action_receipt("action-1") == receipt
        assert len(rows) == 1 and rows[0]["state"] == "pending"
        assert rows[0]["attempt_id"] == claim["attempt_id"]


def test_same_completion_replays_but_conflicts_are_rejected(tmp_path: Path) -> None:
    store, claim, receipt = _running(tmp_path)
    token = claim["lease"]["lease_token"]
    store.complete_agent_action(receipt, lease_token=token, continuation={"workflow_actions": []})
    store.complete_agent_action(receipt, lease_token=token)
    assert len(store.control_outbox(topic="agent.completion")) == 1
    with pytest.raises(ControlRepositoryError, match="continuation replay"):
        store.complete_agent_action(receipt, lease_token=token, continuation={"different": True})
    for changed, used_token in (({**receipt, "completion": {}}, token), (receipt, "wrong-token")):
        with pytest.raises(ControlRepositoryError, match="conflicting"):
            store.complete_agent_action(changed, lease_token=used_token)


def test_claim_is_exclusive_and_expired_owner_is_fenced(tmp_path: Path, monkeypatch) -> None:
    store, claim, receipt = _running(tmp_path)
    store.complete_agent_action(receipt, lease_token=claim["lease"]["lease_token"], continuation={})
    def claim_once(owner):
        return ControlStore(store.path).claim_control_outbox(topic="agent.completion", owner=owner, lease_seconds=1)
    with ThreadPoolExecutor(max_workers=2) as pool:
        attempts = list(pool.map(claim_once, ["one", "two"]))
    won = [row for row in attempts if row is not None]
    assert len(won) == 1
    first = won[0]
    later = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat()
    monkeypatch.setattr(outbox_repository, "_now", lambda: later)
    second = claim_once("restarted")
    assert second is not None and second["outbox_id"] == first["outbox_id"]
    with pytest.raises(ControlRepositoryError, match="fenced"):
        store.finish_control_outbox(outbox_id=first["outbox_id"], claim_token=first["claim_token"], result={})
    completed = store.finish_control_outbox(outbox_id=second["outbox_id"], claim_token=second["claim_token"], result={"accepted": True})
    assert completed["state"] == "delivered" and completed["attempts"] == 2


@pytest.mark.parametrize("state", ["failed", "uncertain", "cancelled"])
def test_non_success_does_not_queue_source_promotion(tmp_path: Path, state: str) -> None:
    store, claim, receipt = _running(tmp_path)
    receipt["status"] = state
    store.complete_agent_action(receipt, lease_token=claim["lease"]["lease_token"], continuation={})
    assert store.control_outbox(topic="agent.completion") == []


def test_gate_change_at_transaction_cancels_receipt_and_intent(tmp_path: Path, monkeypatch) -> None:
    store, claim, receipt = _running(tmp_path)
    monkeypatch.setattr(store, "_workflow_agent_gate_is_current", lambda *args: False)
    result = store.complete_agent_action(
        receipt, lease_token=claim["lease"]["lease_token"], continuation={"workflow_actions": []},
    )
    assert result["state"] == "cancelled"
    committed = store.agent_action_receipt("action-1")
    assert committed["completion"]["agent_action_outcome"]["execution_status"] == "cancelled"
    assert store.control_outbox(topic="agent.completion") == []
