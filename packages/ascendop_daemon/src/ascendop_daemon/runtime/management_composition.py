from __future__ import annotations

from typing import Any

from ascendop_control.application import ControlCommandWorker

from ascendop_daemon.control_plane.management_commands import (
    DaemonControlCommandHandler,
)


def build_control_command_worker(database: Any, policy: Any) -> ControlCommandWorker:
    return ControlCommandWorker(
        database,
        DaemonControlCommandHandler(database),
        worker_id="ascendop-v4-control-command-worker",
        claim_seconds=int(policy.get("management.command_claim_seconds")),
    )
