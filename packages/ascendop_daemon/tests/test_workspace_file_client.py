from __future__ import annotations

import json

import pytest

from ascendop_daemon.automation.workspace_file_client import proposal_path, submit_workspace_proposal


def client(tmp_path):
    binding = {"action_id": "action-1", "attempt_id": "attempt-1", "lease_id": "lease-1"}
    context = {"schema": "ascendop.workspace-client.v1", "available": True,
        "binding": binding, "native_start_id": "start-1", "native_turn_id": "turn-1",
        "execution_phase": "candidate-test", "proposal_path": proposal_path(binding, "turn-1"),
        "case_revision": {"base_case_version": "cases-1"}}
    state = tmp_path / ".ascendop"
    state.mkdir()
    (state / "CLIENT.json").write_text(json.dumps(context), encoding="utf-8")
    return context


def test_one_file_proposal_is_requested_not_accepted(tmp_path):
    context = client(tmp_path)
    original = (tmp_path / ".ascendop/CLIENT.json").read_bytes()
    first = submit_workspace_proposal(tmp_path, summary="Try a legal boundary input")
    assert first["state"] == "requested"
    assert submit_workspace_proposal(tmp_path, summary="Try a legal boundary input") == first
    proposal = tmp_path / context["proposal_path"]
    value = json.loads(proposal.read_text())
    assert value["kind"] == "test" and value["summary"] == "Try a legal boundary input"
    assert (tmp_path / ".ascendop/CLIENT.json").read_bytes() == original
    assert not list(tmp_path.rglob("*.sqlite3"))
    with pytest.raises(ValueError, match="different semantic proposal"):
        submit_workspace_proposal(tmp_path, summary="Silently replace it")


def test_same_solver_can_request_case_revision(tmp_path):
    context = client(tmp_path)
    result = submit_workspace_proposal(tmp_path, summary="Broaden coverage after external failure", request_revision=True)
    assert result["state"] == "requested"
    value = json.loads((tmp_path / context["proposal_path"]).read_text())
    assert value["kind"] == "case-revision"
    assert value["binding"] == context["binding"]


@pytest.mark.parametrize("mutation", ["unavailable", "stale-action", "outside-path", "duplicate-key"])
def test_bad_client_cannot_publish(tmp_path, mutation):
    context = client(tmp_path)
    path = tmp_path / ".ascendop/CLIENT.json"
    kwargs = {}
    if mutation == "unavailable":
        context["available"] = False
    elif mutation == "stale-action":
        kwargs["action_id"] = "old-action"
    elif mutation == "outside-path":
        context["proposal_path"] = "../outside.json"
    path.write_text(json.dumps(context), encoding="utf-8")
    if mutation == "duplicate-key":
        path.write_text('{"schema":"a","schema":"b"}', encoding="utf-8")
    with pytest.raises(ValueError):
        submit_workspace_proposal(tmp_path, summary="request", **kwargs)
    assert not (tmp_path / ".ascendop/proposals").exists()
