"""File projection adapter for a trusted host, not a Solver control API."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ascendop_control.storage.workspace_repository import workspace_key
from ascendop_protocol.file_lock import exclusive_file_lock
from ascendop_daemon.core.atomic_io import write_json_atomic
from .workspace_snapshot import read_workspace_snapshot


def project_workspace(root: Path, database: Any, workspace: str, *,
                      publish: Callable[[Path, dict[str, Any]], None],
                      required_revision: int = 0, expected_action_id: str | None = None) -> dict[str, Any]:
    """Serialize publication with the same owner lock and revision barrier.

The integration publisher receives accepted facts and writes BRIEF/CLIENT and
evidence mirrors. It must not schedule actions or infer truth from old views.
The callback is explicit so missing domain integration cannot silently succeed.
"""
    root = root.resolve()
    workspace_key(workspace)
    directory = (root / workspace).resolve()
    if root not in directory.parents or workspace_key(directory.relative_to(root).as_posix()) != workspace_key(workspace):
        raise ValueError("workspace must use its canonical in-repository path")
    with exclusive_file_lock(directory / ".ascendop" / ".workspace-publish.lock", 5):
        snapshot = read_workspace_snapshot(database, workspace, required_revision=required_revision,
                                            expected_action_id=expected_action_id)
        publish(directory, snapshot)
        return snapshot["view"]
