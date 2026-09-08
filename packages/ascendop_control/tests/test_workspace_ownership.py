from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

from ascendop_control.storage.errors import ControlRepositoryError
from ascendop_control.storage.workspace_repository import workspace_key
from test_agent_repository import _store, _action, _snapshot


def _binding(action="a", revision=1, workspace="workspaces/September/Op"):
    return {"schema": "ascendop.workspace-owner.v1", "workspace": workspace,
            "campaign_id": "September", "operator_id": "Op", "action_id": action,
            "attempt_id": f"attempt-{action}", "lease_id": f"lease-{action}",
            "principal_id": "solver", "native_session_id": "session", "revision": revision}


def _admit(store, binding, previous="", plan=None):
    return store.admit_workspace_action(binding=binding, expected_action_id=previous,
        expected_revision=binding["revision"] - 1, plan=plan or {"action_id": binding["action_id"]})


def test_two_plans_on_one_revision_have_exactly_one_writer(tmp_path):
    store = _store(tmp_path)
    _admit(store, _binding())
    def compete(action):
        try:
            _admit(store, _binding(action, 2), "a")
            return action
        except ControlRepositoryError as exc:
            assert "CAS fenced" in str(exc)
            return None
    with ThreadPoolExecutor(2) as pool:
        winners = [value for value in pool.map(compete, ("b", "c")) if value]
    assert len(winners) == 1
    assert store.workspace_owner("workspaces/September/Op") == _binding(winners[0], 2)
    assert len(store.control_outbox(topic="workspace.action")) == 2


def test_replay_is_idempotent_and_cannot_restore_old_owner(tmp_path):
    store = _store(tmp_path)
    _admit(store, _binding())
    _admit(store, _binding("b", 2), "a")
    assert _admit(store, _binding()) == _binding()
    assert store.workspace_owner("workspaces/September/Op") == _binding("b", 2)
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM control_events WHERE event_type='workspace-owner-admitted'").fetchone()[0] == 2
    with pytest.raises(ControlRepositoryError, match="replay changed"):
        _admit(store, _binding(), plan={"action_id": "changed"})


@pytest.mark.parametrize("change", [{"campaign_id": "August"}, {"operator_id": "OtherOp"}])
def test_same_physical_workspace_cannot_be_rebound_to_other_task(tmp_path, change):
    store = _store(tmp_path)
    _admit(store, _binding())
    with pytest.raises(ControlRepositoryError, match="another task/operator"):
        _admit(store, {**_binding("b", 2), **change}, "a")
    assert len(store.control_outbox(topic="workspace.action")) == 1


@pytest.mark.parametrize("path", ["../elsewhere", "C:/other", "a/../b", "a//b", "/root", ".", "a\\b"])
def test_workspace_scope_rejects_ambiguous_paths(path):
    with pytest.raises(ControlRepositoryError):
        workspace_key(path)


def test_different_workspaces_progress_independently(tmp_path):
    store = _store(tmp_path)
    _admit(store, _binding())
    other = _binding("other", workspace="workspaces/September/Other")
    _admit(store, other)
    assert store.workspace_owner(other["workspace"]) == other
    assert store.workspace_owner("WORKSPACES/SEPTEMBER/OP") == _binding()


def test_formal_and_standalone_writers_cannot_be_active_together(tmp_path):
    store = _store(tmp_path)
    action = _action("old")
    store.create_agent_action(action, _snapshot("old"))
    owner = _binding(workspace=action["origin_workspace"])
    with pytest.raises(ControlRepositoryError, match="formal workspace writer must be drained"):
        _admit(store, owner)
    assert store.workspace_owner(owner["workspace"]) is None


def test_managed_workspace_rejects_formal_action_creator(tmp_path):
    store = _store(tmp_path)
    action = _action("old")
    _admit(store, _binding(workspace=action["origin_workspace"]))
    with pytest.raises(ControlRepositoryError, match="legacy formal action creator"):
        store.create_agent_action(action, _snapshot("old"))
    assert store.agent_action(action["action_id"]) is None


def test_formal_creation_racing_owner_admission_has_one_winner(tmp_path):
    store = _store(tmp_path)
    action = _action("old")
    owner = _binding(workspace=action["origin_workspace"])
    def compete(kind):
        try:
            if kind == "formal":
                store.create_agent_action(action, _snapshot("old"))
            else:
                _admit(store, owner)
            return kind
        except ControlRepositoryError:
            return None
    with ThreadPoolExecutor(2) as pool:
        winners = [value for value in pool.map(compete, ("formal", "managed")) if value]
    assert len(winners) == 1
    assert bool(store.workspace_owner(owner["workspace"])) != bool(store.agent_action(action["action_id"]))


def test_bad_unrelated_formal_record_does_not_block_healthy_workspace(tmp_path):
    store = _store(tmp_path)
    store.create_agent_action(_action("bad"), _snapshot("bad"))
    with store.transaction() as connection:
        connection.execute("UPDATE agent_actions_v4 SET action_json='{' WHERE action_id='action-bad'")
    assert _admit(store, _binding()) == _binding()


@pytest.mark.parametrize("cut", ["before-commit", "after-commit"])
def test_real_process_death_keeps_owner_and_publication_in_one_transaction(tmp_path, cut):
    store = _store(tmp_path)
    binding = _binding()
    code = """
import json, os, sys
from pathlib import Path
from ascendop_control.storage import ControlStore
from ascendop_control.storage import workspace_repository as repository
store = ControlStore(Path(sys.argv[1]))
if sys.argv[3] == 'before-commit':
    original = repository.enqueue_control_intent
    def crash(*args, **kwargs):
        original(*args, **kwargs)
        os._exit(91)
    repository.enqueue_control_intent = crash
store.admit_workspace_action(binding=json.loads(sys.argv[2]), expected_action_id='',
    expected_revision=0, plan={'action_id': 'a'})
os._exit(92)
"""
    process = subprocess.run([sys.executable, "-B", "-c", code, str(store.path), json.dumps(binding), cut],
                             env=os.environ.copy(), capture_output=True, text=True, timeout=30)
    assert process.returncode == (91 if cut == "before-commit" else 92), process.stderr
    assert bool(store.workspace_owner(binding["workspace"])) == (cut == "after-commit")
    assert bool(store.control_outbox(topic="workspace.action")) == (cut == "after-commit")
    _admit(store, binding)
    assert store.workspace_owner(binding["workspace"]) == binding
    assert len(store.control_outbox(topic="workspace.action")) == 1
