from __future__ import annotations

import os
import stat
from pathlib import Path

from ascendop_test_gateway import journal as journal_module
from ascendop_test_gateway.journal import RunJournal


def test_journal_retries_transient_windows_replace_and_retains_lock_inode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    replace_failures = 0
    real_replace = os.replace

    def flaky_replace(source: Path, destination: Path) -> None:
        nonlocal replace_failures
        if Path(destination).name == "JOURNAL.json" and replace_failures < 2:
            replace_failures += 1
            raise PermissionError(5, "transient sharing violation")
        real_replace(source, destination)

    monkeypatch.setattr(journal_module.os, "replace", flaky_replace)
    monkeypatch.setattr(journal_module.time, "sleep", lambda _: None)

    journal = RunJournal(tmp_path)
    created = journal.initialize("request-1")

    assert created["request_id"] == "request-1"
    assert replace_failures == 2
    assert journal.lock_path.is_file()
    assert journal.initialize("request-1") == created


def test_journal_syncs_before_replacing(tmp_path: Path, monkeypatch) -> None:
    events = []
    real_sync, real_replace = os.fsync, os.replace
    real_directory_sync = journal_module.sync_directory

    def sync(descriptor):
        kind = "directory-fsync" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file-fsync"
        events.append(kind)
        real_sync(descriptor)

    def replace(source, destination):
        events.append("replace")
        real_replace(source, destination)

    def directory_sync(path):
        assert Path(path) == tmp_path
        events.append("sync-directory")
        real_directory_sync(path)

    monkeypatch.setattr(journal_module, "sync_directory", directory_sync)
    monkeypatch.setattr(journal_module.os, "fsync", sync)
    monkeypatch.setattr(journal_module.os, "replace", replace)
    RunJournal(tmp_path).initialize("request-1")
    expected = ["file-fsync", "replace", "sync-directory"]
    if os.name == "posix":
        expected.append("directory-fsync")
    assert events == expected
