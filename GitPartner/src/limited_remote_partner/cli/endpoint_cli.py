from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

from limited_remote_partner.core.config import load_config
from limited_remote_partner.endpoint.endpoint_registry import EndpointRegistry, EndpointRegistryError
from limited_remote_partner.gateway.git_client import GitClient, GitError
from limited_remote_partner.endpoint.node_enrollment import (
    NodeEnrollmentError,
    build_channel_provisioning_receipt,
    confirm_node_channel_provisioning,
    initialize_node,
    node_registration_status,
    normalize_node_id,
    resume_node_enrollment,
)
from limited_remote_partner.gateway.git_lock import GitOperationLock
from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs


SOURCE_SYNC_PATHS = (
    "src",
    "scripts",
    "configs",
    "services",
    "docs",
    "README.md",
    "pyproject.toml",
)
ENROLLED_NODE_CONFIG_DIR = Path("configs/nodes")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Validate, inspect, and materialize AscendOP GP endpoints"
    )
    parser.add_argument(
        "--registry",
        default="../Develop/registry/system_registry.json",
        help="central AscendOP system registry JSON",
    )
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("validate")
    sub.add_parser("list")
    route = sub.add_parser("route")
    route_input = route.add_mutually_exclusive_group(required=True)
    route_input.add_argument("--requirements-json")
    route_input.add_argument("--requirements-file")
    materialize = sub.add_parser("materialize-config")
    materialize.add_argument("--endpoint", required=True)
    materialize.add_argument("--base-config", required=True)
    materialize.add_argument("--output", required=True)
    materialize.add_argument("--portable-worktree", action="store_true")
    launcher_manifest = sub.add_parser("materialize-launchers")
    launcher_manifest.add_argument("--source-repo", default=".")
    launcher_manifest.add_argument(
        "--output",
        default="configs/node_launchers.json",
    )
    bootstrap = sub.add_parser("bootstrap-channels")
    bootstrap.add_argument("--endpoint", required=True)
    bootstrap.add_argument(
        "--base-config",
        help="base GP config; defaults to the endpoint registry entry",
    )
    provision_worktree = sub.add_parser("provision-worktree")
    provision_worktree.add_argument("--endpoint", required=True)
    provision_worktree.add_argument("--source-repo", default=".")
    provision_worktree.add_argument(
        "--base-config",
        default="configs/partner.json",
    )
    provision_worktree.add_argument("--output-config")
    sync_source = sub.add_parser("sync-source")
    sync_source.add_argument("--endpoint", required=True)
    sync_source.add_argument("--source-repo", default=".")
    sync_source.add_argument("--expected-generation", required=True)
    sync_source.add_argument("--source-branch", default="main")
    publish_source = sub.add_parser("publish-working-source")
    publish_source.add_argument("--endpoint", required=True)
    publish_source.add_argument("--source-repo", default=".")
    publish_source.add_argument("--expected-generation", required=True)
    provision_node = sub.add_parser("provision-node-channels")
    provision_node.add_argument("--node", required=True)
    provision_node.add_argument("--generation", required=True)
    provision_node.add_argument("--base-config", default="configs/partner.json")
    provision_node.add_argument("--repo-dir")
    provision_node.add_argument("--source-branch", default="main")
    provision_node.add_argument("--control-branch")
    provision_node.add_argument("--result-branch")
    provision_node.add_argument("--report-branch", default="gp/nodes")
    provision_node.add_argument("--receipt-output", required=True)
    init_node = sub.add_parser("init-node")
    init_node.add_argument("--node")
    init_node.add_argument("--base-config", default="configs/partner.json")
    init_node.add_argument("--output")
    init_node.add_argument("--repo-dir")
    init_node.add_argument("--role", choices=("client", "server", "local"))
    init_node.add_argument("--source-branch")
    init_node.add_argument("--control-branch")
    init_node.add_argument("--result-branch")
    init_node.add_argument("--report-branch")
    init_node.add_argument("--remote-root", default="")
    init_node.add_argument("--engine-root", default="test_engine_demo")
    init_node.add_argument(
        "--publish-mode", choices=("auto", "git", "relay", "file"), default="auto"
    )
    init_node.add_argument(
        "--import-login-network-env",
        action="store_true",
        help="import proxy/CA variables from the node login shell",
    )
    init_node.add_argument(
        "--transport-mode",
        choices=("direct", "relay"),
        default="direct",
    )
    init_node.add_argument("--gateway-id", default="")
    init_node.add_argument("--server-ssh", default="")
    init_node.add_argument("--interactive", action="store_true")
    init_node.add_argument("--no-bootstrap-channels", action="store_true")
    resume_node = sub.add_parser("resume-node")
    resume_node.add_argument("--config", required=True)
    resume_node.add_argument("--repo-dir")
    confirm_channels = sub.add_parser("confirm-node-channels")
    confirm_channels.add_argument("--config", required=True)
    confirm_channels.add_argument("--receipt", required=True)
    confirm_channels.add_argument("--repo-dir")
    node_status = sub.add_parser("node-status")
    node_status.add_argument("--node")
    node_status.add_argument("--config")
    node_status.add_argument("--repo-dir")
    args = parser.parse_args(argv)

    try:
        if args.action == "init-node":
            payload = _initialize_node_from_args(args)
        elif args.action == "provision-node-channels":
            payload = _provision_node_channels_from_args(args)
        elif args.action == "resume-node":
            payload = resume_node_enrollment(
                Path(args.config),
                repo_dir=Path(args.repo_dir) if args.repo_dir else None,
            )
        elif args.action == "confirm-node-channels":
            payload = confirm_node_channel_provisioning(
                Path(args.config),
                Path(args.receipt),
                repo_dir=Path(args.repo_dir) if args.repo_dir else None,
            )
        elif args.action == "node-status":
            payload = node_registration_status(
                config_path=Path(args.config).resolve() if args.config else None,
                repo_dir=Path(args.repo_dir).resolve() if args.repo_dir else None,
                node_id=args.node,
            )
        else:
            registry = EndpointRegistry.load(Path(args.registry))
            payload = _registry_action(registry, args)
    except (
        EndpointRegistryError,
        NodeEnrollmentError,
        GitError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise SystemExit(f"GITPARTNER_ENDPOINT_ERROR {exc}") from exc
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))


def _registry_action(
    registry: EndpointRegistry,
    args: argparse.Namespace,
) -> dict[str, object]:
    if args.action == "validate":
        return {
            "valid": True,
            "registry": str(registry.path),
            "endpoint_count": len(registry.endpoints),
            "enabled_endpoint_count": sum(
                1 for endpoint in registry.endpoints if endpoint.enabled
            ),
        }
    if args.action == "list":
        return {
            "registry": str(registry.path),
            "endpoints": [endpoint.to_dict() for endpoint in registry.endpoints],
        }
    if args.action == "route":
        requirements = _requirements(args)
        return {
            "registry": str(registry.path),
            "requirements": requirements,
            **registry.route(requirements).to_dict(),
        }
    if args.action == "materialize-config":
        result = registry.materialize_config(
            args.endpoint,
            Path(args.base_config),
            Path(args.output),
            portable_worktree=bool(args.portable_worktree),
        )
        return {
            "registry": str(registry.path),
            "endpoint": args.endpoint,
            "output": str(Path(args.output).resolve()),
            "repo_dir": result["repo_dir"],
            "control_channel": result["repo"]["branch"],
            "result_channel": result["repo"]["result_branch"],
        }
    if args.action == "materialize-launchers":
        source_repo = Path(args.source_repo).resolve()
        output = Path(args.output)
        if not output.is_absolute():
            output = source_repo / output
        result = registry.materialize_launcher_manifest(
            output,
            source_repo=source_repo,
        )
        return {
            "registry": str(registry.path),
            "output": str(output.resolve()),
            "launcher_count": len(result["nodes"]),
            "nodes": sorted(result["nodes"]),
        }
    if args.action == "provision-worktree":
        return _provision_endpoint_worktree(registry, args)
    if args.action == "sync-source":
        endpoint = registry.get(args.endpoint)
        if args.expected_generation != endpoint.generation:
            raise EndpointRegistryError(
                "endpoint generation mismatch: "
                f"expected={args.expected_generation} "
                f"current={endpoint.generation}"
            )
        source_repo = Path(args.source_repo).resolve()
        target = registry.resolve_path(endpoint.gitpartner_repo)
        return {
            "endpoint_id": endpoint.endpoint_id,
            "generation": endpoint.generation,
            **_sync_endpoint_worktree_source(
                source_repo,
                target,
                endpoint.control_channel,
                args.source_branch,
                remote="origin",
            ),
        }
    if args.action == "publish-working-source":
        endpoint = registry.get(args.endpoint)
        if args.expected_generation != endpoint.generation:
            raise EndpointRegistryError(
                "endpoint generation mismatch: "
                f"expected={args.expected_generation} "
                f"current={endpoint.generation}"
            )
        return _publish_endpoint_working_source(
            registry,
            endpoint_id=endpoint.endpoint_id,
            source_repo=Path(args.source_repo).resolve(),
        )
    endpoint = registry.get(args.endpoint)
    base_config = (
        Path(args.base_config).resolve()
        if args.base_config
        else registry.resolve_path(endpoint.gitpartner_config)
    )
    config = load_config(base_config)
    repo = replace(
        config.repo,
        branch=endpoint.control_channel,
        result_branch=endpoint.result_channel,
    )
    repo_dir = registry.resolve_path(endpoint.gitpartner_repo)
    client = GitClient(repo, repo_dir)
    client.ensure_worktree()
    channel_state = client.ensure_channel_branches()
    return {
        "registry": str(registry.path),
        "endpoint": endpoint.endpoint_id,
        "node_id": endpoint.node_id,
        "repo_dir": str(repo_dir),
        **channel_state,
    }


def _provision_endpoint_worktree(
    registry: EndpointRegistry,
    args: argparse.Namespace,
) -> dict[str, object]:
    endpoint = registry.get(args.endpoint)
    source_repo = Path(args.source_repo).resolve()
    if not (source_repo / ".git").exists():
        raise EndpointRegistryError(
            f"source repo is not a Git worktree: {source_repo}"
        )
    base_config = Path(args.base_config)
    if not base_config.is_absolute():
        base_config = source_repo / base_config
    base_config = base_config.resolve()
    config = load_config(base_config, base_dir=source_repo)
    channels_client = GitClient(
        replace(
            config.repo,
            branch=endpoint.control_channel,
            result_branch=endpoint.result_channel,
        ),
        source_repo,
    )
    channels_client.ensure_worktree()
    channels = channels_client.ensure_channel_branches()
    target = registry.resolve_path(endpoint.gitpartner_repo)
    channel_rows = dict(channels["channels"])
    created = _ensure_endpoint_worktree(
        source_repo,
        target,
        str(channel_rows["control"]["commit"]),
        label=f"{endpoint.endpoint_id} ingress",
    )
    result_target = (
        registry.resolve_path(endpoint.result_worktree)
        if endpoint.result_worktree
        else None
    )
    result_created = False
    if result_target is not None:
        result_created = _ensure_endpoint_worktree(
            source_repo,
            result_target,
            str(channel_rows["result"]["commit"]),
            label=f"{endpoint.endpoint_id} result",
            checkout=False,
        )
    source_sync = _sync_endpoint_worktree_source(
        source_repo,
        target,
        endpoint.control_channel,
        config.repo.source_branch,
        remote=config.repo.remote,
    )
    source_token = source_repo / "api.txt"
    credential_copied = _copy_credential(source_token, target / "api.txt")
    result_credential_copied = bool(
        result_target is not None
        and _copy_credential(source_token, result_target / "api.txt")
    )
    output_config = (
        Path(args.output_config)
        if args.output_config
        else (
            target
            / ".partner_state"
            / "generated"
            / f"{endpoint.endpoint_id}.json"
        )
    )
    if not output_config.is_absolute():
        output_config = target / output_config
    materialized = registry.materialize_config(
        endpoint.endpoint_id,
        base_config,
        output_config,
        portable_worktree=True,
    )
    return {
        "endpoint_id": endpoint.endpoint_id,
        "node_id": endpoint.node_id,
        "worktree": str(target),
        "created": created,
        "result_worktree": str(result_target) if result_target else "",
        "result_worktree_created": result_created,
        "credential_copied": credential_copied,
        "result_credential_copied": result_credential_copied,
        "config": str(output_config.resolve()),
        "control_channel": endpoint.control_channel,
        "result_channel": endpoint.result_channel,
        "generation": endpoint.generation,
        "materialized_repo_dir": materialized["repo_dir"],
        "channels": channels,
        "source_sync": source_sync,
    }


def _ensure_endpoint_worktree(
    source_repo: Path,
    target: Path,
    commit: str,
    *,
    label: str,
    checkout: bool = True,
) -> bool:
    if (target / ".git").exists():
        _recover_initializing_worktree_lock(
            source_repo,
            target,
            label=label,
        )
        return False
    if target.exists() and any(target.iterdir()):
        raise EndpointRegistryError(
            f"{label} worktree path is non-empty: {target}"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "git",
        "-c",
        f"safe.directory={source_repo.as_posix()}",
        "worktree",
        "add",
        "--detach",
    ]
    if not checkout:
        command.append("--no-checkout")
    command.extend([str(target), commit])
    with GitOperationLock(source_repo, f"{label} worktree provision"):
        completed = subprocess.run(
            command,
            cwd=source_repo,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            **hidden_subprocess_kwargs(),
        )
    if completed.returncode != 0:
        raise EndpointRegistryError(
            f"unable to provision {label} worktree: "
            + (completed.stderr or completed.stdout).strip()
        )
    return True


def _recover_initializing_worktree_lock(
    source_repo: Path,
    target: Path,
    *,
    label: str,
) -> bool:
    pointer = target / ".git"
    try:
        line = pointer.read_text(encoding="utf-8").strip()
    except OSError:
        return False
    prefix = "gitdir:"
    if not line.lower().startswith(prefix):
        return False
    metadata = Path(line[len(prefix) :].strip()).resolve()
    worktree_root = (source_repo / ".git" / "worktrees").resolve()
    if metadata.parent != worktree_root:
        raise EndpointRegistryError(
            f"{label} worktree metadata escapes source repository: {metadata}"
        )
    lock = metadata / "locked"
    try:
        reason = lock.read_text(encoding="utf-8").strip()
    except OSError:
        return False
    if reason != "initializing":
        return False
    with GitOperationLock(source_repo, f"{label} initializing-lock recovery"):
        completed = subprocess.run(
            ["git", "worktree", "unlock", str(target)],
            cwd=source_repo,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=30,
            **hidden_subprocess_kwargs(),
        )
    if completed.returncode != 0:
        raise EndpointRegistryError(
            f"unable to recover {label} initializing lock: "
            + _command_error(completed)
        )
    return True


def _copy_credential(source: Path, target: Path) -> bool:
    if not source.is_file() or target.exists():
        return False
    shutil.copy2(source, target)
    try:
        target.chmod(0o600)
    except OSError:
        pass
    return True


def _sync_endpoint_worktree_source(
    source_repo: Path,
    target: Path,
    control_branch: str,
    source_branch: str,
    *,
    remote: str,
    attempts: int = 3,
) -> dict[str, object]:
    if not (target / ".git").exists():
        raise EndpointRegistryError(
            f"endpoint worktree is missing: {target}"
        )
    with GitOperationLock(source_repo, "endpoint source synchronization"):
        before = _git_output(target, "rev-parse", "HEAD")
        last_error = ""
        network_timeout = _endpoint_sync_timeout_seconds()
        for attempt in range(1, max(1, attempts) + 1):
            fetch = _run_git(
                target,
                "fetch",
                remote,
                (
                    f"+refs/heads/{source_branch}:"
                    f"refs/remotes/{remote}/{source_branch}"
                ),
                (
                    f"+refs/heads/{control_branch}:"
                    f"refs/remotes/{remote}/{control_branch}"
                ),
                timeout_seconds=network_timeout,
            )
            if fetch.returncode != 0:
                last_error = _command_error(fetch)
                continue
            checkout = _run_git(
                target,
                "checkout",
                "-B",
                control_branch,
                f"{remote}/{control_branch}",
            )
            if checkout.returncode != 0:
                raise EndpointRegistryError(
                    "unable to select endpoint control branch: "
                    + _command_error(checkout)
                )
            source_ref = f"{remote}/{source_branch}"
            for path in SOURCE_SYNC_PATHS:
                exists = _run_git(
                    target,
                    "cat-file",
                    "-e",
                    f"{source_ref}:{path}",
                ).returncode == 0
                if exists:
                    restore = _run_git(
                        target,
                        "checkout",
                        source_ref,
                        "--",
                        path,
                    )
                    if restore.returncode != 0:
                        raise EndpointRegistryError(
                            "endpoint source restore failed: "
                            + _command_error(restore)
                        )
                else:
                    _run_git(
                        target,
                        "rm",
                        "-r",
                        "--ignore-unmatch",
                        "--",
                        path,
                    )
            _run_git(target, "add", "-A", "--", *SOURCE_SYNC_PATHS)
            changed = (
                _run_git(target, "diff", "--cached", "--quiet").returncode
                != 0
            )
            if changed:
                source_oid = _git_output(
                    target,
                    "rev-parse",
                    f"{remote}/{source_branch}",
                )
                commit = _run_git(
                    target,
                    "commit",
                    "-m",
                    f"gitpartner: sync source {source_oid[:12]}",
                )
                if commit.returncode != 0:
                    raise EndpointRegistryError(
                        "endpoint source commit failed: "
                        + _command_error(commit)
                    )
            push = _run_git(
                target,
                "push",
                remote,
                f"HEAD:refs/heads/{control_branch}",
                timeout_seconds=network_timeout,
            )
            if push.returncode == 0:
                after = _git_output(target, "rev-parse", "HEAD")
                return {
                    "source_branch": source_branch,
                    "control_branch": control_branch,
                    "before": before,
                    "after": after,
                    "changed": before != after,
                    "attempts": attempt,
                }
            last_error = _command_error(push)
        raise EndpointRegistryError(
            "unable to publish endpoint source synchronization: "
            + last_error
        )


def _publish_endpoint_working_source(
    registry: EndpointRegistry,
    *,
    endpoint_id: str,
    source_repo: Path,
    attempts: int = 3,
) -> dict[str, object]:
    endpoint = registry.get(endpoint_id)
    target = registry.resolve_path(endpoint.gitpartner_repo)
    if not (source_repo / ".git").exists():
        raise EndpointRegistryError(
            f"source repo is not a Git worktree: {source_repo}"
        )
    if not (target / ".git").exists():
        raise EndpointRegistryError(
            f"endpoint worktree is missing: {target}"
        )
    config_relative = _working_source_config_path(
        endpoint.node_gitpartner_config
    )
    config_path = source_repo / config_relative
    try:
        config_payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EndpointRegistryError(
            f"working source endpoint config is unreadable: {config_path}"
        ) from exc
    endpoint_config = config_payload.get("endpoint")
    observed_generation = str(
        endpoint_config.get("generation") or ""
        if isinstance(endpoint_config, dict)
        else ""
    )
    if observed_generation != endpoint.generation:
        raise EndpointRegistryError(
            "working source endpoint config generation mismatch: "
            f"endpoint={endpoint.endpoint_id} "
            f"expected={endpoint.generation} "
            f"observed={observed_generation or 'missing'}"
        )
    source_digest = _working_source_digest(source_repo)
    before = _git_output(target, "rev-parse", "HEAD")
    last_error = ""
    network_timeout = _endpoint_sync_timeout_seconds()
    recovered_interrupted_publication = False
    with GitOperationLock(target, "endpoint working source publication"):
        for attempt in range(1, max(1, attempts) + 1):
            dirty = _run_git(
                target,
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
                *SOURCE_SYNC_PATHS,
            )
            if dirty.returncode != 0:
                raise EndpointRegistryError(
                    "unable to inspect endpoint source paths: "
                    + _command_error(dirty)
                )
            dirty_source = dirty.stdout.strip()
            if dirty_source:
                branch = _git_output(target, "branch", "--show-current")
                if branch != endpoint.control_channel:
                    raise EndpointRegistryError(
                        "endpoint source paths contain uncommitted changes "
                        f"outside the active control branch {endpoint.control_channel}: "
                        + dirty_source.replace("\n", "; ")
                    )
                recovered_interrupted_publication = True
            fetch = _run_git(
                target,
                "fetch",
                "origin",
                (
                    f"+refs/heads/{endpoint.control_channel}:"
                    f"refs/remotes/origin/{endpoint.control_channel}"
                ),
                timeout_seconds=network_timeout,
            )
            if fetch.returncode != 0:
                last_error = _command_error(fetch)
                continue
            if dirty_source:
                remote_head = _git_output(
                    target,
                    "rev-parse",
                    f"origin/{endpoint.control_channel}",
                )
                local_head = _git_output(target, "rev-parse", "HEAD")
                if local_head != remote_head:
                    raise EndpointRegistryError(
                        "interrupted endpoint publication cannot recover after "
                        "the remote control branch advanced"
                    )
            else:
                checkout = _run_git(
                    target,
                    "checkout",
                    "-B",
                    endpoint.control_channel,
                    f"origin/{endpoint.control_channel}",
                )
                if checkout.returncode != 0:
                    raise EndpointRegistryError(
                        "unable to select endpoint control branch: "
                        + _command_error(checkout)
                    )
            sync_paths = _copy_working_source_snapshot(
                source_repo,
                target,
            )
            (
                node_config_seeded,
                node_config_normalized,
                node_config_path,
            ) = _ensure_node_launcher_config(
                target,
                config_relative=config_relative,
                node_id=endpoint.node_id,
                generation=endpoint.generation,
            )
            add = _run_git(target, "add", "-A", "--", *sync_paths)
            if add.returncode != 0:
                raise EndpointRegistryError(
                    "unable to stage endpoint working source: "
                    + _command_error(add)
                )
            changed = (
                _run_git(target, "diff", "--cached", "--quiet").returncode
                != 0
            )
            if changed:
                commit = _run_git(
                    target,
                    "commit",
                    "-m",
                    (
                        "gitpartner: publish working source "
                        f"{source_digest[:12]}"
                    ),
                )
                if commit.returncode != 0:
                    raise EndpointRegistryError(
                        "endpoint working source commit failed: "
                        + _command_error(commit)
                    )
            push = _run_git(
                target,
                "push",
                "origin",
                f"HEAD:refs/heads/{endpoint.control_channel}",
                timeout_seconds=network_timeout,
            )
            if push.returncode == 0:
                after = _git_output(target, "rev-parse", "HEAD")
                return {
                    "endpoint_id": endpoint.endpoint_id,
                    "generation": endpoint.generation,
                    "control_channel": endpoint.control_channel,
                    "source_digest": source_digest,
                    "source_paths": list(sync_paths),
                    "node_config_path": node_config_path,
                    "node_config_seeded": node_config_seeded,
                    "node_config_normalized": node_config_normalized,
                    "before": before,
                    "after": after,
                    "changed": before != after,
                    "attempts": attempt,
                    "recovered_interrupted_publication": (
                        recovered_interrupted_publication
                    ),
                }
            last_error = _command_error(push)
        raise EndpointRegistryError(
            "unable to publish endpoint working source: " + last_error
        )


def _working_source_config_path(config_path: str) -> Path:
    parts = Path(config_path.replace("\\", "/")).parts
    try:
        index = parts.index("configs")
    except ValueError as exc:
        raise EndpointRegistryError(
            "endpoint config must be rooted below configs/: "
            f"{config_path}"
        ) from exc
    relative = Path(*parts[index:])
    if ".." in relative.parts:
        raise EndpointRegistryError(
            f"endpoint config escapes working source: {config_path}"
        )
    return relative


def _copy_working_source_snapshot(
    source_repo: Path,
    target: Path,
) -> tuple[str, ...]:
    copied: list[str] = []
    for relative_text in SOURCE_SYNC_PATHS:
        source = source_repo / relative_text
        destination = target / relative_text
        if not source.exists() and not destination.exists():
            continue
        if source.is_symlink():
            raise EndpointRegistryError(
                f"working source cannot contain symlinks: {source}"
            )
        if source.is_dir():
            for child in source.rglob("*"):
                if child.is_symlink():
                    raise EndpointRegistryError(
                        f"working source cannot contain symlinks: {child}"
                    )
            if relative_text == "configs":
                _copy_configs_preserving_enrollment(source, destination)
            else:
                _remove_source_path(destination)
                shutil.copytree(source, destination)
        elif source.is_file():
            _remove_source_path(destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        else:
            _remove_source_path(destination)
        copied.append(relative_text)
    if not copied:
        raise EndpointRegistryError("working source snapshot is empty")
    return tuple(copied)


def _remove_source_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path)


def _copy_configs_preserving_enrollment(
    source: Path,
    destination: Path,
) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for child in destination.iterdir():
        if child.name == ENROLLED_NODE_CONFIG_DIR.name:
            continue
        _remove_source_path(child)
    for child in source.iterdir():
        if child.name == ENROLLED_NODE_CONFIG_DIR.name:
            continue
        target = destination / child.name
        if child.is_dir():
            shutil.copytree(child, target)
        elif child.is_file():
            shutil.copy2(child, target)


def _ensure_node_launcher_config(
    target: Path,
    *,
    config_relative: Path,
    node_id: str,
    generation: str,
) -> tuple[bool, bool, str]:
    endpoint_config_path = target / config_relative
    try:
        payload = json.loads(
            endpoint_config_path.read_text(encoding="utf-8-sig")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise EndpointRegistryError(
            "published endpoint config is unreadable: "
            f"{endpoint_config_path}"
        ) from exc
    lifecycle = payload.get("node_lifecycle")
    if not isinstance(lifecycle, dict) or not lifecycle.get("enabled"):
        return False, False, ""
    safe_node = normalize_node_id(node_id)
    node_config = target / ENROLLED_NODE_CONFIG_DIR / f"{safe_node}.json"
    relative = node_config.relative_to(target).as_posix()
    if node_config.is_file():
        try:
            existing = json.loads(
                node_config.read_text(encoding="utf-8-sig")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise EndpointRegistryError(
                f"enrolled node config is unreadable: {node_config}"
            ) from exc
        existing_node = existing.get("node")
        existing_endpoint = existing.get("endpoint")
        if (
            not isinstance(existing_node, dict)
            or str(existing_node.get("node_id") or "") != safe_node
            or not isinstance(existing_endpoint, dict)
            or str(existing_endpoint.get("endpoint_id") or "")
            != str(payload.get("endpoint", {}).get("endpoint_id") or "")
        ):
            raise EndpointRegistryError(
                "enrolled node config identity mismatch: "
                f"path={relative} expected_node={safe_node} "
                f"expected_generation={generation}"
            )
        payload["repo_dir"] = "."
        normalized = existing != payload
        if normalized:
            _write_json_atomic(node_config, payload)
        return False, normalized, relative
    node_config.parent.mkdir(parents=True, exist_ok=True)
    payload["repo_dir"] = "."
    _write_json_atomic(node_config, payload)
    return True, True, relative


def _write_json_atomic(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _working_source_digest(source_repo: Path) -> str:
    digest = hashlib.sha256()
    for relative_text in SOURCE_SYNC_PATHS:
        root = source_repo / relative_text
        digest.update(relative_text.encode("utf-8"))
        digest.update(b"\0")
        if not root.exists():
            digest.update(b"missing\0")
            continue
        paths = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in paths:
            if (
                relative_text == "configs"
                and path.is_relative_to(
                    source_repo / ENROLLED_NODE_CONFIG_DIR
                )
            ):
                continue
            if path.is_symlink():
                raise EndpointRegistryError(
                    f"working source cannot contain symlinks: {path}"
                )
            relative = path.relative_to(source_repo).as_posix()
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            if path.is_file():
                digest.update(b"file\0")
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
            elif path.is_dir():
                digest.update(b"directory\0")
    return digest.hexdigest()


def _run_git(
    cwd: Path,
    *args: str,
    timeout_seconds: int | None = None,
) -> subprocess.CompletedProcess[str]:
    command = ["git", *args]
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
            **hidden_subprocess_kwargs(),
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(
            command,
            124,
            stdout=_timeout_text(exc.stdout),
            stderr=(
                _timeout_text(exc.stderr)
                or f"git command timed out after {timeout_seconds} seconds"
            ),
        )


def _git_output(cwd: Path, *args: str) -> str:
    completed = _run_git(cwd, *args)
    if completed.returncode != 0:
        raise EndpointRegistryError(_command_error(completed))
    return completed.stdout.strip()


def _command_error(completed: subprocess.CompletedProcess[str]) -> str:
    return (completed.stderr or completed.stdout or "").strip()


def _endpoint_sync_timeout_seconds() -> int:
    try:
        return max(
            5,
            int(os.environ.get("GITPARTNER_ENDPOINT_SYNC_TIMEOUT_SECONDS", "60")),
        )
    except ValueError:
        return 60


def _timeout_text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value or "")


def _initialize_node_from_args(args: argparse.Namespace) -> dict[str, object]:
    interactive = bool(args.interactive or not args.node)
    if interactive and not sys.stdin.isatty():
        raise NodeEnrollmentError(
            "interactive enrollment requires a terminal; pass --node for non-interactive use"
        )
    node_id = normalize_node_id(
        _prompt("Node ID", args.node or "") if interactive else str(args.node)
    )
    role = args.role or "client"
    source_branch = args.source_branch or "main"
    control_branch = args.control_branch or f"gp/control/{node_id}"
    result_branch = args.result_branch or f"gp/results/{node_id}"
    report_branch = args.report_branch or "gp/nodes"
    if interactive:
        role = _prompt("Role", role)
        source_branch = _prompt("Source branch", source_branch)
        control_branch = _prompt("Control branch", control_branch)
        result_branch = _prompt("Result branch", result_branch)
        report_branch = _prompt("Node report branch", report_branch)
    output = Path(args.output or f"configs/nodes/{node_id}.json")
    return initialize_node(
        Path(args.base_config),
        node_id=node_id,
        output_path=output,
        role=role,
        source_branch=source_branch,
        control_branch=control_branch,
        result_branch=result_branch,
        report_branch=report_branch,
        repo_dir=Path(args.repo_dir) if args.repo_dir else None,
        remote_root=args.remote_root,
        engine_root=args.engine_root,
        publish_mode=args.publish_mode,
        import_login_network_environment=args.import_login_network_env,
        transport_mode=args.transport_mode,
        gateway_id=args.gateway_id,
        server_ssh=args.server_ssh,
        bootstrap_channels=not args.no_bootstrap_channels,
    )


def _provision_node_channels_from_args(
    args: argparse.Namespace,
) -> dict[str, object]:
    node_id = normalize_node_id(args.node)
    base_config = Path(args.base_config).resolve()
    repo_dir = (
        Path(args.repo_dir).resolve()
        if args.repo_dir
        else base_config.parent.parent.resolve()
    )
    config = load_config(base_config, base_dir=repo_dir)
    control_branch = args.control_branch or f"gp/control/{node_id}"
    result_branch = args.result_branch or f"gp/results/{node_id}"
    repo = replace(
        config.repo,
        source_branch=args.source_branch,
        branch=control_branch,
        result_branch=result_branch,
    )
    client = GitClient(repo, repo_dir)
    client.ensure_worktree()
    channels = client.ensure_channel_branches(
        extra_channels={"node-report": args.report_branch}
    )
    receipt = build_channel_provisioning_receipt(
        node_id=node_id,
        endpoint_id=node_id,
        generation=args.generation,
        source_branch=args.source_branch,
        channels=channels,
    )
    output = Path(args.receipt_output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(receipt, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    return {
        "node_id": node_id,
        "generation": args.generation,
        "repo_dir": str(repo_dir),
        "receipt_path": str(output),
        **channels,
    }


def _prompt(label: str, default: str) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    return value or default


def _requirements(args: argparse.Namespace) -> dict[str, object]:
    if args.requirements_json:
        value = json.loads(args.requirements_json)
    else:
        value = json.loads(Path(args.requirements_file).read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise EndpointRegistryError("route requirements must be a JSON object")
    return value


if __name__ == "__main__":
    main()
