from __future__ import annotations

import hashlib
import json
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import load_config
from limited_remote_partner.gateway.git_client import GitClient, GitError


NODE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
NODE_IDENTITY_SCHEMA = "git-partner.node-identity.v1"
CHANNEL_PROVISIONING_RECEIPT_SCHEMA = (
    "git-partner.channel-provisioning-receipt.v1"
)
DEFAULT_REPORT_BRANCH = "gp/nodes"


class NodeEnrollmentError(ValueError):
    pass


def normalize_node_id(value: str) -> str:
    node_id = value.strip().lower()
    if not NODE_ID_PATTERN.fullmatch(node_id):
        raise NodeEnrollmentError(
            "node_id must be 1-63 lowercase letters, digits, '.', '_' or '-', "
            "and must start with a letter or digit"
        )
    return node_id


def initialize_node(
    base_config_path: Path,
    *,
    node_id: str,
    output_path: Path,
    role: str = "client",
    source_branch: str = "main",
    control_branch: str | None = None,
    result_branch: str | None = None,
    report_branch: str = DEFAULT_REPORT_BRANCH,
    repo_dir: Path | None = None,
    remote_root: str = "",
    engine_root: str = "test_engine_demo",
    publish_mode: str = "auto",
    import_login_network_environment: bool = False,
    transport_mode: str = "direct",
    gateway_id: str = "",
    server_ssh: str = "",
    bootstrap_channels: bool = True,
) -> dict[str, Any]:
    node_id = normalize_node_id(node_id)
    if role not in {"client", "server", "local"}:
        raise NodeEnrollmentError("role must be client, server, or local")
    transport_mode = transport_mode.strip().lower()
    gateway_id = gateway_id.strip()
    server_ssh = server_ssh.strip()
    if transport_mode not in {"direct", "relay"}:
        raise NodeEnrollmentError("transport_mode must be direct or relay")
    if role == "client" and transport_mode == "relay":
        if not gateway_id or not server_ssh:
            raise NodeEnrollmentError(
                "relay client enrollment requires gateway_id and server_ssh"
            )
    elif gateway_id or server_ssh:
        raise NodeEnrollmentError(
            "gateway_id and server_ssh are only valid for relay client enrollment"
        )
    control_branch = control_branch or f"gp/control/{node_id}"
    result_branch = result_branch or f"gp/results/{node_id}"
    if len({control_branch, result_branch, report_branch}) != 3:
        raise NodeEnrollmentError(
            "control, result, and node-report branches must be distinct"
        )

    base_config_path = base_config_path.resolve()
    config_root = _config_repo_root(base_config_path)
    raw = json.loads(base_config_path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise NodeEnrollmentError("base GP config must be a JSON object")
    base_config = load_config(base_config_path, base_dir=config_root)
    effective_repo_dir = (repo_dir or base_config.repo_dir).resolve()
    stored_repo_dir = _portable_path(effective_repo_dir, config_root)
    generation = _generation(
        node_id,
        source_branch,
        control_branch,
        result_branch,
        report_branch,
        stored_repo_dir,
        transport_mode,
        gateway_id,
        server_ssh,
    )

    result = _materialize_role_config(raw, role)
    result["repo_dir"] = stored_repo_dir
    repo = _section(result, "repo")
    repo["source_branch"] = source_branch
    repo["branch"] = control_branch
    repo["result_branch"] = result_branch
    repo["token_file"] = "api.txt"
    relay = _section(result, "relay")
    relay["role"] = role
    if role == "client":
        relay["transport_mode"] = transport_mode
        relay["client_ssh"] = ""
        relay["client_password_file"] = ""
        relay["server_ssh"] = server_ssh
        relay["server_password_file"] = ""
        relay["client_inbox_dir"] = "work/relay/inbox"
        relay["client_work_dir"] = remote_root or "."
    elif role == "local":
        relay["transport_mode"] = "direct"
        relay["client_ssh"] = ""
        relay["server_ssh"] = ""
    exchange_watch = _section(result, "exchange_watch")
    exchange_watch["enabled"] = False
    io = _section(result, "io")
    io["state_dir"] = f".partner_state/endpoints/{node_id}"
    result["node"] = {
        "node_id": node_id,
        "display_name": node_id,
        "roles": [role, "npu-backend"],
        "capabilities": ["git-sync", "execute", "node-discovery"],
        "tags": [],
    }
    result["routing"] = {
        "enabled": True,
        "source_node": "",
        "target_node": node_id,
        "require_explicit_target": True,
        "served_nodes": [node_id],
        "served_tags": [],
        "served_roles": ["npu-backend"],
    }
    result["endpoint"] = {
        "endpoint_id": node_id,
        "execution_environment_id": f"{node_id}-discovering",
        "generation": generation,
        "gateway_id": gateway_id,
        "transport_mode": (
            "lan-relay" if transport_mode == "relay" else "direct-git"
        ),
        "backend_pool": "unassigned",
        "remote_root": remote_root,
        "engine_root": engine_root,
    }
    result["node_lifecycle"] = {
        "enabled": True,
        "registration_state": "enrolled",
        "report_branch": report_branch,
        "publish_mode": publish_mode,
        "heartbeat_seconds": 30,
        "lease_seconds": 90,
        "probe_on_start": True,
    }
    result["network_environment"] = {
        "login_shell_import": bool(import_login_network_environment),
    }

    output_path = output_path.resolve()
    identity_path = (
        effective_repo_dir
        / str(result["io"]["state_dir"])
        / "node_identity.json"
    )
    existing = _read_json(identity_path)
    if existing and (
        existing.get("node_id") != node_id
        or existing.get("generation") != generation
    ):
        raise NodeEnrollmentError(
            f"node identity already exists with another identity: {identity_path}"
        )
    _write_json_atomic(output_path, result)
    effective = load_config(output_path, base_dir=config_root)
    identity = existing or {
        "schema": NODE_IDENTITY_SCHEMA,
        "node_id": node_id,
        "endpoint_id": node_id,
        "generation": generation,
        "created_at": _utc_now(),
    }
    identity.update(
        {
            "config_path": _portable_path(output_path, effective_repo_dir),
            "source_branch": source_branch,
            "control_branch": control_branch,
            "result_branch": result_branch,
            "report_branch": report_branch,
            "registration_state": "enrolled",
            "channel_bootstrap": {
                "state": "pending" if bootstrap_channels else "deferred",
                "updated_at": _utc_now(),
            },
        }
    )
    _write_json_atomic(identity_path, identity)

    channels: dict[str, Any] | None = None
    if bootstrap_channels:
        channels = _bootstrap_channels(
            effective,
            identity_path=identity_path,
        )

    return {
        "node_id": node_id,
        "endpoint_id": node_id,
        "generation": generation,
        "config_path": _portable_path(output_path, config_root),
        "identity_path": _portable_path(identity_path, config_root),
        "registration_state": "enrolled",
        "central_status": "unconfirmed",
        "channels": channels,
    }


def resume_node_enrollment(
    config_path: Path,
    *,
    repo_dir: Path | None = None,
) -> dict[str, Any]:
    config_path = config_path.resolve()
    config_root = (repo_dir or _config_repo_root(config_path)).resolve()
    migrated = _migrate_resumable_config(config_path, config_root)
    generation_adopted = adopt_published_generation(
        config_path,
        repo_dir=config_root,
    )
    config = load_config(config_path, base_dir=config_root)
    status = node_registration_status(
        config_path=config_path,
        repo_dir=config_root,
    )
    if not status.get("registered"):
        raise NodeEnrollmentError(
            f"cannot resume invalid enrollment: {status.get('code')} "
            f"{status.get('message')}"
        )
    identity_path = config.repo_dir / config.io.state_dir / "node_identity.json"
    identity = _read_json(identity_path)
    identity["config_path"] = _portable_path(config_path, config.repo_dir)
    _write_json_atomic(identity_path, identity)
    if (
        status.get("runnable")
        and status.get("channel_bootstrap_state") == "ready"
    ):
        bootstrap = (
            identity.get("channel_bootstrap")
            if isinstance(identity.get("channel_bootstrap"), dict)
            else {}
        )
        return {
            "node_id": config.node.node_id,
            "endpoint_id": config.endpoint.endpoint_id,
            "generation": config.endpoint.generation,
            "config_path": _portable_path(config_path, config_root),
            "identity_path": _portable_path(identity_path, config_root),
            "registration_state": config.node_lifecycle.registration_state,
            "central_status": status.get("central_status", "unconfirmed"),
            "channel_bootstrap_state": str(bootstrap.get("state") or "ready"),
            "resumed": True,
            "config_migrated": migrated,
            "generation_adopted": generation_adopted,
            "channels": bootstrap.get("channels", {}),
        }
    channels = _bootstrap_channels(config, identity_path=identity_path)
    return {
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "config_path": _portable_path(config_path, config_root),
        "identity_path": _portable_path(identity_path, config_root),
        "registration_state": config.node_lifecycle.registration_state,
        "central_status": status.get("central_status", "unconfirmed"),
        "channel_bootstrap_state": "ready",
        "resumed": True,
        "config_migrated": migrated,
        "generation_adopted": generation_adopted,
        "channels": channels,
    }


def adopt_published_generation(
    config_path: Path,
    *,
    repo_dir: Path | None = None,
) -> bool:
    config_path = config_path.resolve()
    config_root = (repo_dir or _config_repo_root(config_path)).resolve()
    config = load_config(config_path, base_dir=config_root)
    identity_path = config.repo_dir / config.io.state_dir / "node_identity.json"
    identity = _read_json(identity_path)
    if not identity:
        return False
    expected_identity = (
        identity.get("schema") == NODE_IDENTITY_SCHEMA
        and identity.get("node_id") == config.node.node_id
        and identity.get("endpoint_id") == config.endpoint.endpoint_id
    )
    if not expected_identity:
        return False
    if identity.get("generation") == config.endpoint.generation:
        return False
    identity["generation"] = config.endpoint.generation
    identity["registration_state"] = "enrolled"
    identity["generation_adopted_at"] = _utc_now()
    identity["config_path"] = _portable_path(config_path, config.repo_dir)
    _write_json_atomic(identity_path, identity)
    ack_path = config.repo_dir / config.io.state_dir / "central_ack.json"
    if ack_path.exists():
        ack_path.unlink()
    return True


def build_channel_provisioning_receipt(
    *,
    node_id: str,
    endpoint_id: str,
    generation: str,
    source_branch: str,
    channels: dict[str, Any],
) -> dict[str, Any]:
    normalized_channels = _validated_receipt_channels(channels)
    return {
        "schema": CHANNEL_PROVISIONING_RECEIPT_SCHEMA,
        "node_id": normalize_node_id(node_id),
        "endpoint_id": normalize_node_id(endpoint_id),
        "generation": generation,
        "source_branch": source_branch,
        "source_commit": _validated_commit(channels.get("source_commit")),
        "channels": normalized_channels,
        "provisioned_at": _utc_now(),
        "provisioning_transport": "trusted-control-plane",
    }


def confirm_node_channel_provisioning(
    config_path: Path,
    receipt_path: Path,
    *,
    repo_dir: Path | None = None,
) -> dict[str, Any]:
    config_path = config_path.resolve()
    config_root = (repo_dir or _config_repo_root(config_path)).resolve()
    _migrate_resumable_config(config_path, config_root)
    config = load_config(config_path, base_dir=config_root)
    status = node_registration_status(
        config_path=config_path,
        repo_dir=config_root,
    )
    if not status.get("registered"):
        raise NodeEnrollmentError(
            f"cannot confirm channels for invalid enrollment: "
            f"{status.get('code')} {status.get('message')}"
        )

    receipt_path = receipt_path.resolve()
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NodeEnrollmentError(
            f"channel provisioning receipt is unreadable: {receipt_path}: {exc}"
        ) from exc
    if not isinstance(receipt, dict):
        raise NodeEnrollmentError("channel provisioning receipt must be an object")
    if receipt.get("schema") != CHANNEL_PROVISIONING_RECEIPT_SCHEMA:
        raise NodeEnrollmentError("channel provisioning receipt schema mismatch")

    expected_identity = {
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "source_branch": config.repo.source_branch,
    }
    for field, expected in expected_identity.items():
        if receipt.get(field) != expected:
            raise NodeEnrollmentError(
                f"channel provisioning receipt {field} mismatch"
            )
    expected_branches = {
        "control": config.repo.branch,
        "result": config.repo.result_branch,
        "node-report": config.node_lifecycle.report_branch,
    }
    receipt_channels = _validated_receipt_channels(receipt)
    for channel_type, expected_branch in expected_branches.items():
        if receipt_channels[channel_type]["branch"] != expected_branch:
            raise NodeEnrollmentError(
                f"channel provisioning receipt {channel_type} branch mismatch"
            )

    state_dir = config.repo_dir / config.io.state_dir
    stored_receipt_path = state_dir / "channel_provisioning_receipt.json"
    _write_json_atomic(stored_receipt_path, receipt)
    digest = hashlib.sha256(
        json.dumps(
            receipt,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    identity_path = state_dir / "node_identity.json"
    identity = _read_json(identity_path)
    identity["channel_bootstrap"] = {
        "state": "ready",
        "method": "central-provisioning-receipt",
        "receipt_digest": digest,
        "receipt_path": _portable_path(
            stored_receipt_path,
            config.repo_dir,
        ),
        "updated_at": _utc_now(),
        "channels": {
            "source_branch": receipt["source_branch"],
            "source_commit": receipt["source_commit"],
            "channels": receipt_channels,
        },
    }
    _write_json_atomic(identity_path, identity)
    return {
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "channel_bootstrap_state": "ready",
        "method": "central-provisioning-receipt",
        "receipt_digest": digest,
        "receipt_path": _portable_path(stored_receipt_path, config.repo_dir),
        "runnable": node_registration_status(
            config_path=config_path,
            repo_dir=config_root,
        ).get("runnable", False),
    }


def node_registration_status(
    *,
    config_path: Path | None = None,
    repo_dir: Path | None = None,
    node_id: str | None = None,
) -> dict[str, Any]:
    root = (repo_dir or Path.cwd()).resolve()
    if config_path is None:
        config_path, selection_error, candidates = select_node_config(root, node_id)
        if selection_error:
            return {
                "registered": False,
                "runnable": False,
                "code": selection_error,
                "message": (
                    "multiple enrolled node configs exist; select one with "
                    "--node-id <id> or --config <path>"
                    if selection_error == "NODE_SELECTION_REQUIRED"
                    else "requested node config was not found; enroll it first"
                    if node_id
                    else "no enrolled node config was found; enroll this node first"
                ),
                "candidates": [_portable_path(path, root) for path in candidates],
                "reminder": (
                    "bash scripts/start_gitpartner_service.sh --enroll"
                ),
            }
    if config_path is None or not config_path.is_file():
        return {
            "registered": False,
            "runnable": False,
            "code": "NODE_NOT_ENROLLED",
            "message": (
                "no enrolled node config found; run "
                "bash scripts/start_gitpartner_service.sh --enroll"
            ),
            "reminder": "bash scripts/start_gitpartner_service.sh --enroll",
        }
    try:
        config_root = (
            root if repo_dir is not None else _config_repo_root(config_path.resolve())
        )
        config = load_config(config_path.resolve(), base_dir=config_root)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "registered": False,
            "runnable": False,
            "code": "NODE_CONFIG_INVALID",
            "message": str(exc),
            "config_path": _portable_path(config_path.resolve(), config_root),
        }
    identity_path = config.repo_dir / config.io.state_dir / "node_identity.json"
    identity = _read_json(identity_path)
    expected = config.node.node_id
    valid_identity = bool(
        expected
        and identity.get("schema") == NODE_IDENTITY_SCHEMA
        and identity.get("node_id") == expected and identity.get("endpoint_id") == config.endpoint.endpoint_id
        and identity.get("generation") == config.endpoint.generation
    )
    channel_bootstrap = identity.get("channel_bootstrap")
    if not isinstance(channel_bootstrap, dict):
        channel_bootstrap = {}
    channel_bootstrap_state = str(channel_bootstrap.get("state") or "unknown")
    channel_bootstrap_blocked = channel_bootstrap_state in {
        "pending",
        "in-progress",
        "blocked",
    }
    ack_path = config.repo_dir / config.io.state_dir / "central_ack.json"
    ack = _read_json(ack_path)
    central_status = str(
        ack.get("state")
        or (
            "accepted"
            if config.node_lifecycle.registration_state == "accepted"
            else "unconfirmed"
        )
    )
    runnable = bool(
        valid_identity
        and config.node_lifecycle.enabled
        and config.node_lifecycle.registration_state in {"enrolled", "accepted"}
        and not channel_bootstrap_blocked
    )
    code = (
        "NODE_CHANNEL_BOOTSTRAP_BLOCKED"
        if valid_identity and channel_bootstrap_blocked
        else "NODE_ENROLLED"
        if runnable
        else "NODE_IDENTITY_INVALID"
    )
    return {
        "registered": valid_identity,
        "runnable": runnable,
        "code": code,
        "message": (
            "node identity is saved but channel bootstrap must be resumed"
            if valid_identity and channel_bootstrap_blocked
            else
            "node is locally enrolled; central acceptance is pending"
            if runnable and central_status == "unconfirmed"
            else "node enrollment is valid"
            if runnable
            else "node config exists but identity is missing or mismatched"
        ),
        "node_id": expected,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "registration_state": config.node_lifecycle.registration_state,
        "central_status": central_status,
        "config_path": _portable_path(config_path.resolve(), config_root),
        "identity_path": _portable_path(identity_path, config_root),
        "ack_path": _portable_path(ack_path, config_root),
        "control_branch": config.repo.branch,
        "result_branch": config.repo.result_branch,
        "report_branch": config.node_lifecycle.report_branch,
        "channel_bootstrap_state": channel_bootstrap_state,
        "channel_bootstrap_error_code": str(
            channel_bootstrap.get("error_code") or ""
        ),
        "channel_bootstrap_error": str(channel_bootstrap.get("error") or ""),
        "next_action": (
            "provide repo-local api.txt, then rerun "
            "bash scripts/start_gitpartner_service.sh --enroll"
            if channel_bootstrap_blocked
            else ""
        ),
    }


def select_node_config(
    root: Path,
    node_id: str | None,
) -> tuple[Path | None, str, tuple[Path, ...]]:
    config_dir = root / "configs" / "nodes"
    endpoint_dir = root / "configs" / "endpoints"
    if node_id:
        normalized = normalize_node_id(node_id)
        candidate = config_dir / f"{normalized}.json"
        if candidate.is_file():
            return candidate, "", (candidate,)
        endpoint_candidates = tuple(
            path
            for path in sorted(endpoint_dir.glob("*.json"))
            if _config_matches_node_or_endpoint(path, normalized)
        )
        if len(endpoint_candidates) == 1:
            return endpoint_candidates[0], "", endpoint_candidates
        if len(endpoint_candidates) > 1:
            return None, "NODE_SELECTION_REQUIRED", endpoint_candidates
        return None, "NODE_NOT_ENROLLED", ()
    node_candidates = tuple(
        sorted(config_dir.glob("*.json")) if config_dir.is_dir() else []
    )
    if len(node_candidates) == 1:
        return node_candidates[0], "", node_candidates
    if len(node_candidates) > 1:
        return None, "NODE_SELECTION_REQUIRED", node_candidates
    endpoint_candidates = tuple(
        sorted(endpoint_dir.glob("*.json")) if endpoint_dir.is_dir() else []
    )
    if len(endpoint_candidates) == 1:
        return endpoint_candidates[0], "", endpoint_candidates
    if len(endpoint_candidates) > 1:
        return None, "NODE_SELECTION_REQUIRED", endpoint_candidates
    return None, "NODE_NOT_ENROLLED", ()


def _config_matches_node_or_endpoint(path: Path, value: str) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    node = payload.get("node")
    endpoint = payload.get("endpoint")
    node_id = str(node.get("node_id") or "") if isinstance(node, dict) else ""
    endpoint_id = (
        str(endpoint.get("endpoint_id") or "")
        if isinstance(endpoint, dict)
        else ""
    )
    return value in {
        normalize_node_id(node_id) if node_id else "",
        normalize_node_id(endpoint_id) if endpoint_id else "",
    }


def _generation(*values: str) -> str:
    payload = json.dumps(values, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _config_repo_root(path: Path) -> Path:
    parent = path.resolve().parent
    if parent.name == "nodes" and parent.parent.name == "configs":
        return parent.parent.parent
    if parent.name == "configs":
        return parent.parent
    return parent


def _portable_path(path: Path, root: Path) -> str:
    try:
        relative = os.path.relpath(path.resolve(), root.resolve())
    except ValueError as exc:
        raise NodeEnrollmentError(
            f"path cannot be represented relative to GP root: {path}"
        ) from exc
    return "." if relative == "." else relative.replace("\\", "/")


def _bootstrap_channels(
    config: Any,
    *,
    identity_path: Path,
) -> dict[str, Any]:
    identity = _read_json(identity_path)
    identity["channel_bootstrap"] = {
        "state": "in-progress",
        "updated_at": _utc_now(),
    }
    _write_json_atomic(identity_path, identity)
    try:
        git = GitClient(config.repo, config.repo_dir)
        git.ensure_worktree()
        channels = git.ensure_channel_branches(
            extra_channels={"node-report": config.node_lifecycle.report_branch}
        )
    except GitError as exc:
        detail = str(exc).strip()
        error_code = _bootstrap_error_code(detail)
        identity = _read_json(identity_path)
        identity["channel_bootstrap"] = {
            "state": "blocked",
            "error_code": error_code,
            "error": detail[-2000:],
            "updated_at": _utc_now(),
        }
        _write_json_atomic(identity_path, identity)
        hint = (
            "put the Git token in repo-local api.txt (chmod 600)"
            if error_code == "AUTH_REQUIRED"
            else "fix the reported Git transport error"
        )
        raise NodeEnrollmentError(
            f"GITPARTNER_CHANNEL_BOOTSTRAP_{error_code} enrollment checkpoint "
            f"saved; {hint}, then rerun "
            "bash scripts/start_gitpartner_service.sh --enroll"
        ) from exc
    identity = _read_json(identity_path)
    identity["channel_bootstrap"] = {
        "state": "ready",
        "updated_at": _utc_now(),
        "channels": channels,
    }
    _write_json_atomic(identity_path, identity)
    return channels


def _bootstrap_error_code(detail: str) -> str:
    lowered = detail.lower()
    auth_markers = (
        "could not read password",
        "authentication failed",
        "terminal prompts disabled",
        "invalid username or password",
        "http basic: access denied",
        "permission denied",
    )
    return "AUTH_REQUIRED" if any(marker in lowered for marker in auth_markers) else "FAILED"


def _validated_receipt_channels(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    raw_channels = payload.get("channels")
    if not isinstance(raw_channels, dict):
        raise NodeEnrollmentError(
            "channel provisioning receipt channels must be an object"
        )
    result: dict[str, dict[str, str]] = {}
    for channel_type in ("control", "result", "node-report"):
        raw = raw_channels.get(channel_type)
        if not isinstance(raw, dict):
            raise NodeEnrollmentError(
                f"channel provisioning receipt is missing {channel_type}"
            )
        branch = str(raw.get("branch") or "")
        if not branch:
            raise NodeEnrollmentError(
                f"channel provisioning receipt {channel_type} branch is missing"
            )
        result[channel_type] = {
            "branch": branch,
            "commit": _validated_commit(raw.get("commit")),
        }
    return result


def _validated_commit(value: Any) -> str:
    commit = str(value or "").lower()
    if not re.fullmatch(r"[0-9a-f]{40,64}", commit):
        raise NodeEnrollmentError(
            "channel provisioning receipt commit must be a full object id"
        )
    return commit


def _materialize_role_config(
    raw: dict[str, Any],
    role: str,
) -> dict[str, Any]:
    result = deepcopy(raw)
    roles = result.pop("roles", {})
    if not isinstance(roles, dict):
        raise NodeEnrollmentError("base GP config roles must be an object")
    overlay = roles.get(role)
    if overlay is None:
        return result
    if not isinstance(overlay, dict):
        raise NodeEnrollmentError(f"base GP config roles.{role} must be an object")
    return _deep_merge(result, overlay)


def _migrate_resumable_config(config_path: Path, config_root: Path) -> bool:
    raw = json.loads(config_path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise NodeEnrollmentError("node config must be a JSON object")
    relay = raw.get("relay")
    role = str(relay.get("role") or "client") if isinstance(relay, dict) else "client"
    if role not in {"client", "server", "local"}:
        raise NodeEnrollmentError(f"node config has invalid relay role: {role}")
    migrated = _materialize_role_config(raw, role)
    migrated["repo_dir"] = "."
    repo = _section(migrated, "repo")
    repo["token_file"] = "api.txt"
    if role == "client":
        relay = _section(migrated, "relay")
        endpoint = _section(migrated, "endpoint")
        endpoint_mode = str(endpoint.get("transport_mode") or "").lower()
        transport_mode = (
            "relay"
            if endpoint_mode == "lan-relay"
            else "direct"
            if endpoint_mode == "direct-git"
            else str(relay.get("transport_mode") or "direct").lower()
        )
        if transport_mode not in {"direct", "relay"}:
            transport_mode = "direct"
        relay["transport_mode"] = transport_mode
        relay["client_ssh"] = ""
        relay["client_password_file"] = ""
        relay["server_password_file"] = ""
        relay["client_inbox_dir"] = "work/relay/inbox"
        relay["client_work_dir"] = str(endpoint.get("remote_root") or ".")
        if transport_mode == "direct":
            relay["server_ssh"] = ""
            endpoint["gateway_id"] = ""
            endpoint["transport_mode"] = "direct-git"
        else:
            if not str(endpoint.get("gateway_id") or ""):
                raise NodeEnrollmentError(
                    "relay node config requires endpoint.gateway_id"
                )
            if not str(relay.get("server_ssh") or ""):
                raise NodeEnrollmentError(
                    "relay node config requires relay.server_ssh"
                )
    if migrated == raw:
        return False
    _write_json_atomic(config_path, migrated)
    return True


def _deep_merge(
    base: dict[str, Any],
    overlay: dict[str, Any],
) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in overlay.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw.setdefault(name, {})
    if not isinstance(value, dict):
        raise NodeEnrollmentError(f"config section {name!r} must be an object")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
