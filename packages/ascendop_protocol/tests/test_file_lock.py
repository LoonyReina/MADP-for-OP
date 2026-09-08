from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from ascendop_protocol.file_lock import exclusive_file_lock


def _holder(path: Path, *, abrupt: bool) -> subprocess.Popen:
    code = (
        "import os,sys\n"
        "from pathlib import Path\n"
        "from ascendop_protocol.file_lock import exclusive_file_lock\n"
        "with exclusive_file_lock(Path(sys.argv[1])):\n"
        " print('locked', flush=True)\n"
        + (" os._exit(0)\n" if abrupt else " sys.stdin.read(1)\n")
    )
    return subprocess.Popen(
        [sys.executable, "-B", "-c", code, str(path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )


def test_process_death_releases_lock_with_file_retained(tmp_path: Path) -> None:
    path = tmp_path / "journal.lock"
    child = _holder(path, abrupt=True)
    stdout, stderr = child.communicate(timeout=10)
    assert child.returncode == 0, stderr
    assert stdout.strip() == "locked"
    assert path.is_file()
    with exclusive_file_lock(path, timeout_seconds=0.2):
        pass
    assert path.is_file()


def test_live_owner_is_not_stolen_and_waiter_can_retry(tmp_path: Path) -> None:
    path = tmp_path / "journal.lock"
    child = _holder(path, abrupt=False)
    try:
        assert child.stdout.readline().strip() == "locked"
        with pytest.raises(TimeoutError):
            with exclusive_file_lock(path, timeout_seconds=0.05):
                pytest.fail("live lock stolen")
    finally:
        _stdout, stderr = child.communicate("x", timeout=10)
    assert child.returncode == 0, stderr
    with exclusive_file_lock(path, timeout_seconds=0):
        pass


def test_exception_releases_lock_and_nested_handle_cannot_steal(tmp_path: Path) -> None:
    path = tmp_path / "nested.lock"
    with pytest.raises(RuntimeError):
        with exclusive_file_lock(path):
            with pytest.raises(TimeoutError):
                with exclusive_file_lock(path, 0):
                    pytest.fail("nested independent handle stole the lock")
            raise RuntimeError("interrupted work")
    with exclusive_file_lock(path, 0):
        pass


@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf")])
def test_invalid_timeout_is_rejected(tmp_path: Path, timeout: float) -> None:
    with pytest.raises(ValueError):
        with exclusive_file_lock(tmp_path / "invalid.lock", timeout):
            pass
