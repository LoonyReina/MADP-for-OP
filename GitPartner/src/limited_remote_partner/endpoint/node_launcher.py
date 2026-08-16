from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import load_config
from limited_remote_partner.endpoint.endpoint_runtime import (
    EndpointRuntimeError,
    activate_runtime_identity,
    provision_runtime_worktree,
)
from limited_remote_partner.core.login_environment import import_login_network_environment


LAUNCHER_SCHEMA = "git-partner.node-launchers.v1"
SAFE_NODE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")


class NodeLauncherError(RuntimeError):
    pass


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Launch a registered GP node from the canonical manifest"
    )
    parser.add_argument("--manifest", default="configs/node_launchers.json")
    parser.add_argument("--node", "--node-id", dest="node_id", required=True)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--foreground", action="store_true")
    actions.add_argument("--status", action="store_true")
    actions.add_argument("--stop", action="store_true")
    parser.add_argument("--force-restart", action="store_true")
    args = parser.parse_args(argv)
    try:
        launch_registered_node(
            manifest_path=Path(args.manifest),
            node_id=args.node_id,
            action=(
                "foreground"
                if args.foreground
                else "status"
                if args.status
                else "stop"
                if args.stop
                else "start"
            ),
            force_restart=args.force_restart,
        )
    except (
        NodeLauncherError,
        EndpointRuntimeError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise SystemExit(f"GITPARTNER_NODE_LAUNCH_ERROR {exc}") from exc


def launch_registered_node(
    *,
    manifest_path: Path,
    node_id: str,
    action: str,
    force_restart: bool,
) -> None:
    manifest_path = manifest_path.resolve()
    source = manifest_path.parent.parent.resolve()
    launcher = _load_launcher(manifest_path, node_id)
    target = _resolve_sibling(source, str(launcher["worktree"]))
    config_relative = _safe_relative_path(str(launcher["config"]))
    config_path = target / config_relative
    if action in {"start", "foreground"}:
        environment, report = import_login_network_environment(
            os.environ,
            enabled=bool(launcher.get("import_login_network_env", False)),
        )
        git_tls_verify = bool(launcher.get("git_tls_verify", True))
        if git_tls_verify:
            environment.pop("GIT_SSL_NO_VERIFY", None)
        else:
            environment["GIT_SSL_NO_VERIFY"] = "1"
        report["git_tls_verify"] = git_tls_verify
        report["git_tls_policy"] = (
            "verify"
            if git_tls_verify
            else "node-explicit-insecure-proxy"
        )
        os.environ.update(environment)
        print(
            "GITPARTNER_NODE_NETWORK "
            + json.dumps(report, ensure_ascii=True, sort_keys=True),
            flush=True,
        )
        provision_runtime_worktree(
            source,
            target,
            control_branch=str(launcher["control_branch"]),
            remote=str(launcher.get("remote") or "origin"),
        )
        if not config_path.is_file():
            raise NodeLauncherError(
                f"registered runtime config is missing: {config_path}"
            )
        config = load_config(config_path, base_dir=target)
        _validate_effective_config(config, launcher, node_id)
        activate_runtime_identity(config_path, config)
    elif not config_path.is_file():
        raise NodeLauncherError(
            f"node runtime is not provisioned: {target}"
        )

    command = [
        sys.executable,
        "-m",
        "limited_remote_partner.endpoint.node_service",
        action,
        "--repo-dir",
        str(target),
        "--config",
        str(config_path),
        "--role",
        str(launcher["role"]),
    ]
    if force_restart:
        command.append("--force-restart")
        for endpoint_id in launcher.get("retire_endpoint_ids", []):
            command.extend(["--retire-endpoint-id", str(endpoint_id)])
    environment = os.environ.copy()
    source_path = str(target / "src")
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        source_path if not existing else source_path + os.pathsep + existing
    )
    os.chdir(target)
    os.execve(sys.executable, command, environment)


def has_registered_launcher(manifest_path: Path, node_id: str) -> bool:
    try:
        _load_launcher(manifest_path.resolve(), node_id)
    except (NodeLauncherError, OSError, ValueError, json.JSONDecodeError):
        return False
    return True


def _load_launcher(path: Path, node_id: str) -> dict[str, Any]:
    normalized = node_id.strip().lower()
    if not SAFE_NODE.fullmatch(normalized):
        raise NodeLauncherError("node id is invalid")
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict) or raw.get("schema") != LAUNCHER_SCHEMA:
        raise NodeLauncherError("node launcher manifest schema mismatch")
    launchers = raw.get("nodes")
    if not isinstance(launchers, dict):
        raise NodeLauncherError("node launcher manifest nodes must be an object")
    value = launchers.get(normalized)
    if not isinstance(value, dict) or not value.get("enabled", True):
        raise NodeLauncherError(f"node launcher is not registered: {normalized}")
    required = ("control_branch", "worktree", "config", "role", "generation")
    missing = [key for key in required if not str(value.get(key) or "")]
    if missing:
        raise NodeLauncherError(
            f"node launcher {normalized} is missing {', '.join(missing)}"
        )
    if value["role"] not in {"client", "server", "local"}:
        raise NodeLauncherError(f"node launcher {normalized} has invalid role")
    if "git_tls_verify" in value and not isinstance(
        value["git_tls_verify"], bool
    ):
        raise NodeLauncherError(
            f"node launcher {normalized} has invalid git_tls_verify"
        )
    retired = value.get("retire_endpoint_ids", [])
    if not isinstance(retired, list) or any(
        not isinstance(endpoint_id, str)
        or not SAFE_NODE.fullmatch(endpoint_id)
        for endpoint_id in retired
    ):
        raise NodeLauncherError(
            f"node launcher {normalized} has invalid retire_endpoint_ids"
        )
    return value


def _resolve_sibling(source: Path, value: str) -> Path:
    target = (
        Path(value).resolve()
        if Path(value).is_absolute()
        else (source / value).resolve()
    )
    if target == source or target.parent != source.parent:
        raise NodeLauncherError(
            "node launcher worktree must be a sibling of the source repo"
        )
    return target


def _safe_relative_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise NodeLauncherError("node launcher config must be worktree-relative")
    return path


def _validate_effective_config(
    config: Any,
    launcher: dict[str, Any],
    node_id: str,
) -> None:
    expected = {
        "node_id": node_id.strip().lower(),
        "generation": str(launcher["generation"]),
        "role": str(launcher["role"]),
        "control_branch": str(launcher["control_branch"]),
    }
    observed = {
        "node_id": config.node.node_id,
        "generation": config.endpoint.generation,
        "role": config.relay.role,
        "control_branch": config.repo.branch,
    }
    mismatches = [
        key
        for key, value in expected.items()
        if observed[key] != value
    ]
    if mismatches:
        raise NodeLauncherError(
            "node launcher/config mismatch: " + ", ".join(mismatches)
        )


if __name__ == "__main__":
    main()
