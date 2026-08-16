from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import (
    SCHEMA_VERSION,
    ControlDatabaseError,
)
from ascendop_daemon.core.clock import boot_id, host_id
from ascendop_daemon.runtime.release_identity import under_root
from ascendop_daemon.workflow.task_execution_profile import (
    ensure_task_execution_profile,
)


LOCAL_SERVICES = {
    "daemon": ("control-plane", ["wire-v3", "stop-fence", "service-lease"]),
    "scheduler": ("scheduler", ["capability-routing", "immutable-route"]),
    "dispatcher": ("transport", ["transactional-outbox", "drain-only-stop"]),
    "retry-controller": ("retry", ["central-retry-decision"]),
    "result-ingestor": ("result", ["workspace-result-projection"]),
    "assistant-trigger": ("automation", ["typed-trigger", "exactly-once-action"]),
}


@dataclass(frozen=True)
class ApplicationPaths:
    root: Path
    config: Path
    registry: Path
    database: Path

    @classmethod
    def resolve(
        cls,
        *,
        root: Path,
        config: Path,
        registry: Path,
        database: Path,
    ) -> "ApplicationPaths":
        resolved_root = root.resolve()
        return cls(
            root=resolved_root,
            config=under_root(resolved_root, config),
            registry=under_root(resolved_root, registry),
            database=under_root(resolved_root, database),
        )


def assert_live_generation_compatible(database, generation: str) -> None:
    mismatches = [
        row
        for row in database.service_health()
        if row["live"]
        and (
            row["code_generation"] != generation
            or row["wire_version"] != 3
            or row["database_schema"] != SCHEMA_VERSION
        )
    ]
    if mismatches:
        identities = ", ".join(row["service_id"] for row in mismatches)
        raise ControlDatabaseError(
            "live service generation mismatch; hard-cut startup refused: "
            + identities
        )


def record_service_heartbeats(
    database,
    *,
    generation: str,
    variable_registry_digest: str,
    state: str,
    variable_registry_artifact_sha256: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    current_boot_id = boot_id()
    for service_id, (role, capabilities) in LOCAL_SERVICES.items():
        database.record_service_heartbeat(
            service_id=service_id,
            role=role,
            code_generation=generation,
            wire_version=3,
            capabilities=capabilities,
            state=state,
            boot_id=current_boot_id,
            lease_seconds=30,
            details={
                "host_id": host_id(),
                "pid": os.getpid(),
                "variable_registry_digest": variable_registry_digest,
                "variable_registry_artifact_sha256": (
                    variable_registry_artifact_sha256
                ),
                **(details or {}),
            },
        )


def materialize_task_profiles(root: Path, database) -> list[dict[str, str]]:
    profiles: list[dict[str, str]] = []
    for registration in database.operator_registrations(desired_state="enabled"):
        path, _document, profile = ensure_task_execution_profile(root, registration)
        profiles.append(
            {
                "operator_id": profile.task_id,
                "path": path.relative_to(root).as_posix(),
                "route_mode": profile.route_mode,
                "backend_pool": profile.backend_pool,
            }
        )
    return profiles
