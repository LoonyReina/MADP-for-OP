from __future__ import annotations

from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import load_config
from limited_remote_partner.endpoint.node_enrollment import (
    NODE_IDENTITY_SCHEMA,
    NodeEnrollmentError,
    _bootstrap_channels,
    _portable_path,
    _read_json,
    _utc_now,
    _write_json_atomic,
    node_registration_status,
)


def enroll_materialized_node(
    config_path: Path,
    *,
    repo_dir: Path | None = None,
    bootstrap_channels: bool = True,
) -> dict[str, Any]:
    config_path = config_path.resolve()
    config_root = (repo_dir or _config_repo_root(config_path)).resolve()
    config = load_config(config_path, base_dir=config_root)
    if not config.node_lifecycle.enabled:
        raise NodeEnrollmentError("materialized node lifecycle is disabled")
    if config.node_lifecycle.registration_state not in {"enrolled", "accepted"}:
        raise NodeEnrollmentError(
            "materialized node registration_state must be enrolled or accepted"
        )
    if not config.node.node_id or not config.endpoint.endpoint_id:
        raise NodeEnrollmentError("materialized node and endpoint IDs are required")
    if not config.endpoint.generation:
        raise NodeEnrollmentError("materialized endpoint generation is required")

    identity_path = config.repo_dir / config.io.state_dir / "node_identity.json"
    existing = _read_json(identity_path)
    if existing and not _matches_config(existing, config):
        raise NodeEnrollmentError(
            f"node identity already exists with another identity: {identity_path}"
        )
    identity = existing or {
        "schema": NODE_IDENTITY_SCHEMA,
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "created_at": _utc_now(),
    }
    identity.update(
        {
            "config_path": _portable_path(config_path, config.repo_dir),
            "source_branch": config.repo.source_branch,
            "control_branch": config.repo.branch,
            "result_branch": config.repo.result_branch,
            "report_branch": config.node_lifecycle.report_branch,
            "registration_state": config.node_lifecycle.registration_state,
        }
    )
    bootstrap = identity.get("channel_bootstrap")
    if not isinstance(bootstrap, dict):
        identity["channel_bootstrap"] = {
            "state": "pending" if bootstrap_channels else "deferred",
            "method": "materialized-config",
            "updated_at": _utc_now(),
        }
    _write_json_atomic(identity_path, identity)

    channels: dict[str, Any] | None = None
    if bootstrap_channels:
        current = identity.get("channel_bootstrap")
        if not isinstance(current, dict) or current.get("state") != "ready":
            channels = _bootstrap_channels(config, identity_path=identity_path)
    status = node_registration_status(
        config_path=config_path,
        repo_dir=config_root,
    )
    if not status.get("runnable"):
        raise NodeEnrollmentError(
            f"materialized enrollment is not runnable: {status.get('code')}"
        )
    return {
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "config_path": _portable_path(config_path, config_root),
        "identity_path": _portable_path(identity_path, config_root),
        "registration_state": config.node_lifecycle.registration_state,
        "central_status": status.get("central_status", "unconfirmed"),
        "channel_bootstrap_state": status.get("channel_bootstrap_state", ""),
        "channels": channels,
        "runnable": True,
    }


def _matches_config(identity: dict[str, Any], config: Any) -> bool:
    return bool(
        identity.get("schema") == NODE_IDENTITY_SCHEMA
        and identity.get("node_id") == config.node.node_id
        and identity.get("endpoint_id") == config.endpoint.endpoint_id
        and identity.get("generation") == config.endpoint.generation
    )


def _config_repo_root(path: Path) -> Path:
    parent = path.resolve().parent
    if parent.name in {"nodes", "endpoints"} and parent.parent.name == "configs":
        return parent.parent.parent
    if parent.name == "configs":
        return parent.parent
    return parent
