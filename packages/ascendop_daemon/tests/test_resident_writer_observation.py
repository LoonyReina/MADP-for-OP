from __future__ import annotations

import os
from pathlib import Path

import pytest

from ascendop_daemon.runtime import resident_writer_observation as observation


@pytest.fixture
def sample(tmp_path, monkeypatch):
    executable = tmp_path / "runtime/codex.exe"
    root = {"pid": 1, "start_token": "win-0000000000000001", "parent_pid": 90, "image": str(executable)}
    host = {"pid": 2, "start_token": "win-0000000000000002", "parent_pid": 1,
        "image": str(executable.with_name("codex-code-mode-host.exe"))}
    members = [root, host]
    monkeypatch.setenv("SystemRoot", str(tmp_path / "windows"))
    monkeypatch.setattr(observation, "job_processes", lambda _handle: members)
    def observe(**kwargs):
        return observation.observe_resident_writers(123,
            runtime={"pid": 1, "start_token": root["start_token"]}, executable=executable,
            turn_terminal=kwargs.get("terminal", True))
    return members, observe


def test_only_selected_direct_runtime_helper_can_remain_idle(sample):
    members, observe = sample
    assert observe()["state"] == "quiescent"
    members.append({"pid": 3, "start_token": "win-0000000000000003", "parent_pid": 1,
        "image": str(Path(os.environ["SystemRoot"]) / "System32/conhost.exe")})
    assert observe()["state"] == "quiescent"


@pytest.mark.parametrize("variation", ["orphan", "lookalike", "wrong-parent", "duplicate-host", "old-birth"])
def test_orphan_or_unqualified_process_blocks_capture(sample, variation):
    members, observe = sample
    child = {**members[1], "pid": 3, "start_token": "win-0000000000000003"}
    if variation == "orphan":
        child.update(parent_pid=999, image="C:/Python/python.exe")
    elif variation == "lookalike":
        child["image"] = "C:/draft/codex-code-mode-host.exe"
    elif variation == "wrong-parent":
        child["parent_pid"] = 2
    elif variation == "old-birth":
        child["start_token"] = "win-0000000000000000"
    members.append(child)
    result = observe()
    assert result["state"] == "busy"
    assert result["writers"] == [child]


def test_nonterminal_turn_never_queries_or_accepts_a_quiet_tree(sample, monkeypatch):
    _, observe = sample
    monkeypatch.setattr(observation, "job_processes", lambda _handle: pytest.fail("no terminal"))
    assert observe(terminal=False)["state"] == "busy"


def test_failed_inventory_is_unknown_not_empty(sample, monkeypatch):
    _, observe = sample
    def fail(_handle):
        raise OSError("query unavailable")
    monkeypatch.setattr(observation, "job_processes", fail)
    assert observe()["state"] == "unknown"


def test_missing_original_resident_is_not_quiescence(sample):
    members, observe = sample
    members.pop(0)
    assert observe()["state"] == "unknown"
