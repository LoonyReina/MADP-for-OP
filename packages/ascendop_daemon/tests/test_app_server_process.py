from __future__ import annotations

import os
import sys
import time

import pytest

from ascendop_daemon.runtime.app_server_process import AppServerProcess
from ascendop_daemon.runtime.process_identity import observe_process_identity

pytestmark = pytest.mark.skipif(os.name != "nt", reason="selected Windows resident primitive")


def test_real_owned_job_is_quiescent_only_after_terminal_turn(tmp_path):
    from ascendop_daemon.runtime.resident_writer_observation import observe_resident_writers
    executable = getattr(sys, "_base_executable", sys.executable)
    runtime = AppServerProcess.prepare(command=[executable, "-B", "-c", "import sys; sys.stdin.read()"],
        cwd=tmp_path, environment=os.environ.copy(), stderr_path=tmp_path / "err")
    try:
        runtime.activate()
        process = runtime.process
        identity = {"pid": process.pid, "start_token": process.start_token}
        def observe(terminal):
            return observe_resident_writers(process._job, runtime=identity,
                executable=executable, turn_terminal=terminal)
        assert observe(False)["state"] == "busy"
        _wait(lambda: observe(True)["state"] == "quiescent")
        assert process.poll() is None
    finally:
        runtime.request_shutdown()
        runtime.process.close()


def _prepare(root):
    return AppServerProcess.prepare(command=[sys.executable, "-B", "-c",
        "from pathlib import Path; import sys; Path('activated').write_text('once'); sys.stdin.read()"],
        cwd=root, environment=os.environ.copy(), stderr_path=root / "err")


def _wait(predicate):
    deadline = time.monotonic() + 10
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(.02)


def test_preparation_never_runs_and_unadmitted_abort_closes_owned_tree(tmp_path):
    runtime = _prepare(tmp_path)
    process = runtime.process
    assert process.poll() is None and not (tmp_path / "activated").exists()
    runtime.abort_prepared()
    _wait(lambda: observe_process_identity(process.pid, process.start_token) == "exited")
    assert not (tmp_path / "activated").exists()
    with pytest.raises(ValueError, match="already activated"):
        runtime.activate()


def test_active_process_requires_real_exit_before_disposal(tmp_path):
    runtime = _prepare(tmp_path)
    try:
        runtime.activate()
        _wait(lambda: (tmp_path / "activated").exists())
        with pytest.raises(ValueError, match="fully exited"):
            runtime.dispose_exited()
        with pytest.raises(ValueError, match="drain/exit"):
            runtime.abort_prepared()
        assert runtime.process.poll() is None
        runtime.request_shutdown()
        def disposed():
            try:
                runtime.dispose_exited()
                return True
            except ValueError:
                return False
        _wait(disposed)
        runtime.dispose_exited()
        assert runtime.incoming.closed and runtime.outgoing.closed
    finally:
        runtime.process.close()  # Isolated fixture fallback only.
