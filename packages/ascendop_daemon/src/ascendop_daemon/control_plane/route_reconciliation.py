from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.control_plane.test_requests import (
    route_and_prepare_test_request,
)
from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.workflow.engine_candidates import (
    discover_control_plane_submit_candidates,
)


def reconcile_waiting_test_requests(
    root: Path,
    config: DaemonConfig,
    database: ControlDatabase,
    registry: SystemRegistry,
    *,
    code_generation: str = "",
    limit: int = 64,
) -> dict[str, Any]:
    """Re-evaluate only pre-publication requests blocked on route availability."""

    waiting = database.waiting_route_requests(limit=1024)
    active_candidates = {
        (str(row.get("op") or ""), str(row.get("test_version") or ""))
        for row in discover_control_plane_submit_candidates(root.resolve(), config)
    }
    eligible_all = [
        row
        for row in waiting
        if (row["operator"], row["test_version"]) in active_candidates
    ]
    eligible = eligible_all[: max(1, min(int(limit), 1024))]
    stale = [
        {
            "request_id": row["request_id"],
            "operator": row["operator"],
            "test_version": row["test_version"],
            "reason": "not-current-submit-candidate",
        }
        for row in waiting
        if (row["operator"], row["test_version"]) not in active_candidates
    ]
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for waiting_request in eligible:
        request_id = waiting_request["request_id"]
        try:
            routed = route_and_prepare_test_request(
                root,
                database,
                registry,
                request_id,
                code_generation=code_generation,
            )
            rows.append(
                {
                    "request_id": request_id,
                    "state": (
                        "routed"
                        if routed.get("attempt")
                        else "preparing"
                        if routed.get("preparation")
                        else "blocked"
                    ),
                    "route": routed,
                }
            )
        except Exception as exc:
            errors.append({"request_id": request_id, "error": str(exc)})
    return {
        "schema": "ascendop.route-reconciliation.v1",
        "waiting_count": len(waiting),
        "eligible_count": len(eligible),
        "deferred_count": len(eligible_all) - len(eligible),
        "stale_count": len(stale),
        "stale": stale,
        "reconciled_count": len(rows),
        "requests": rows,
        "errors": errors,
    }
