from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.gateway.git_lock import GitOperationLock
from limited_remote_partner.core.login_environment import import_login_network_environment
from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs


RUNTIME_SCHEMA = "git-partner.endpoint-runtime.v1"
IDENTITY_SCHEMA = "git-partner.node-identity.v1"


class EndpointRuntimeError(RuntimeError):
    pass


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Provision and control one isolated GP endpoint runtime"
    )
    parser.add_argument(
        "action",
        choices=("provision", "start", "status", "stop"),
    )
    parser.add_argument("--source-repo", default=".")
    parser.add_argument("--worktree", required=True)
    parser.add_argument("--control-branch", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--role", choices=("client", "server", "local"))
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--import-login-network-env", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = endpoint_runtime_action(
            action=args.action,
            source_repo=Path(args.source_repo),
            worktree=Path(args.worktree),
            control_branch=args.control_branch,
            config_path=Path(args.config),
            role=args.role,
            remote=args.remote,
            import_login_network_env=args.import_login_network_env,
        )
    except (EndpointRuntimeError, OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"GITPARTNER_ENDPOINT_RUNTIME_ERROR {exc}") from exc
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))


def endpoint_runtime_action(
    *,
    action: str,
    source_repo: Path,
    worktree: Path,
    control_branch: str,
    config_path: Path,
    role: str | None,
    remote: str,
    import_login_network_env: bool = False,
) -> dict[str, Any]:
    source = source_repo.resolve()
    target = worktree.resolve()
    _validate_roots(source, target)
    network_environment, network_report = import_login_network_environment(
        os.environ,
        enabled=import_login_network_env,
    )
    os.environ.update(network_environment)
    if action in {"provision", "start"}:
        provision = provision_runtime_worktree(
            source,
            target,
            control_branch=control_branch,
            remote=remote,
        )
    else:
        provision = {}
    config = _resolve_runtime_config(target, config_path)
    loaded = load_config(config, base_dir=target)
    if loaded.repo_dir.resolve() != target:
        raise EndpointRuntimeError(
            f"runtime config repo_dir must resolve to worktree: {loaded.repo_dir}"
        )
    if loaded.repo.branch != control_branch:
        raise EndpointRuntimeError(
            "runtime config control branch mismatch: "
            f"{loaded.repo.branch} != {control_branch}"
        )
    if not loaded.endpoint.endpoint_id or not loaded.endpoint.generation:
        raise EndpointRuntimeError(
            "runtime config must declare endpoint id and generation"
        )
    if action in {"provision", "start"}:
        identity = activate_runtime_identity(config, loaded)
    else:
        identity = {}
    service_action = "status" if action == "provision" else action
    service = run_node_service(
        target,
        config,
        action=service_action,
        role=role or loaded.relay.role,
    )
    return {
        "schema": RUNTIME_SCHEMA,
        "action": action,
        "endpoint_id": loaded.endpoint.endpoint_id,
        "generation": loaded.endpoint.generation,
        "worktree": str(target),
        "config": _portable_path(config, target),
        "provision": provision,
        "identity": identity,
        "service": service,
        "network_environment": network_report,
    }


def provision_runtime_worktree(
    source: Path,
    target: Path,
    *,
    control_branch: str,
    remote: str,
) -> dict[str, Any]:
    if not (source / ".git").exists():
        raise EndpointRuntimeError(f"source is not a Git worktree: {source}")
    with GitOperationLock(source, "endpoint runtime provision"):
        fetch = _fetch_control_branch(
            source,
            remote=remote,
            control_branch=control_branch,
        )
        _require_git(fetch, "fetch endpoint control branch")
        created = False
        if not (target / ".git").exists():
            if target.exists() and any(target.iterdir()):
                raise EndpointRuntimeError(
                    f"endpoint runtime target is non-empty: {target}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            add = _git(
                source,
                "worktree",
                "add",
                "--detach",
                str(target),
                f"{remote}/{control_branch}",
            )
            _require_git(add, "create endpoint runtime worktree")
            created = True
        dirty = _git(target, "status", "--porcelain", "--untracked-files=no")
        _require_git(dirty, "inspect endpoint runtime worktree")
        if dirty.stdout.strip():
            raise EndpointRuntimeError(
                "endpoint runtime has tracked local changes; refusing reset"
            )
        checkout = _git(
            target,
            "checkout",
            "-B",
            control_branch,
            f"{remote}/{control_branch}",
        )
        _require_git(checkout, "select endpoint control branch")
        reset = _git(
            target,
            "reset",
            "--hard",
            f"{remote}/{control_branch}",
        )
        _require_git(reset, "refresh endpoint control branch")
    copied = False
    source_token = source / "api.txt"
    target_token = target / "api.txt"
    if source_token.is_file() and not target_token.exists():
        shutil.copy2(source_token, target_token)
        try:
            target_token.chmod(0o600)
        except OSError:
            pass
        copied = True
    return {
        "created": created,
        "credential_copied": copied,
        "head": _git_output(target, "rev-parse", "HEAD"),
        "control_branch": control_branch,
    }


def activate_runtime_identity(
    config_path: Path,
    config: AppConfig,
) -> dict[str, Any]:
    state_dir = config.repo_dir / config.io.state_dir
    identity_path = state_dir / "node_identity.json"
    previous_generation = ""
    try:
        previous = json.loads(identity_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        previous = {}
    if isinstance(previous, dict):
        previous_generation = str(previous.get("generation") or "")
    identity = {
        "schema": IDENTITY_SCHEMA,
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "gateway_id": config.endpoint.gateway_id,
        "generation": config.endpoint.generation,
        "config_path": _portable_path(config_path, config.repo_dir),
        "source_branch": config.repo.source_branch,
        "control_branch": config.repo.branch,
        "result_branch": config.repo.result_branch or "",
        "report_branch": config.node_lifecycle.report_branch,
        "registration_state": config.node_lifecycle.registration_state,
        "transport_mode": config.endpoint.transport_mode,
        "channel_bootstrap": {
            "state": "ready",
            "method": "central-endpoint-provisioning",
        },
    }
    _write_json_atomic(identity_path, identity)
    ack_path = state_dir / "central_ack.json"
    if (
        previous_generation
        and previous_generation != config.endpoint.generation
        and ack_path.exists()
    ):
        ack_path.unlink()
    return {
        "identity_path": _portable_path(identity_path, config.repo_dir),
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
    }


def run_node_service(
    root: Path,
    config_path: Path,
    *,
    action: str,
    role: str,
) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "limited_remote_partner.endpoint.node_service",
        action,
        "--repo-dir",
        str(root),
        "--config",
        str(config_path),
        "--role",
        role,
    ]
    environment = os.environ.copy()
    source = str(root / "src")
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        source if not existing else source + os.pathsep + existing
    )
    completed = subprocess.run(
        command,
        cwd=root,
        env=environment,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        raise EndpointRuntimeError(
            "endpoint node service failed: "
            + (completed.stderr or completed.stdout).strip()
        )
    value = _last_json_object(completed.stdout)
    return value


def _validate_roots(source: Path, target: Path) -> None:
    if target == source or target.parent != source.parent:
        raise EndpointRuntimeError(
            "endpoint runtime worktree must be a sibling of source repo"
        )


def _resolve_runtime_config(root: Path, value: Path) -> Path:
    path = value if value.is_absolute() else root / value
    path = path.resolve()
    if path != root and root not in path.parents:
        raise EndpointRuntimeError(f"runtime config escapes worktree: {path}")
    if not path.is_file():
        raise EndpointRuntimeError(f"runtime config is missing: {path}")
    return path


def _fetch_control_branch(
    source: Path,
    *,
    remote: str,
    control_branch: str,
) -> subprocess.CompletedProcess[str]:
    remote_url = _git_output(source, "remote", "get-url", remote)
    auth_header = None
    if remote_url.lower().startswith(("http://", "https://")):
        token_path = source / "api.txt"
        if not token_path.is_file():
            raise EndpointRuntimeError(
                f"repository token is missing: {token_path}"
            )
        token = (
            token_path.read_text(encoding="utf-8-sig")
            .strip()
            .strip('"')
            .strip("'")
        )
        username = os.environ.get(
            "GITPARTNER_AUTH_USERNAME",
            "git-user",
        ).strip()
        if not token or not username:
            raise EndpointRuntimeError(
                f"repository token or auth username is empty: {token_path}"
            )
        encoded = base64.b64encode(
            f"{username}:{token}".encode("utf-8")
        ).decode("ascii")
        auth_header = f"Authorization: Basic {encoded}"
    return _git(
        source,
        "fetch",
        remote,
        (
            f"+refs/heads/{control_branch}:"
            f"refs/remotes/{remote}/{control_branch}"
        ),
        auth_header=auth_header,
    )


def _git(
    cwd: Path,
    *args: str,
    auth_header: str | None = None,
) -> subprocess.CompletedProcess[str]:
    command = ["git", "-c", f"safe.directory={cwd.as_posix()}"]
    if auth_header:
        command.extend(["-c", f"http.extraHeader={auth_header}"])
    command.extend(args)
    environment = os.environ.copy()
    environment.setdefault("GIT_TERMINAL_PROMPT", "0")
    environment.setdefault("GCM_INTERACTIVE", "Never")
    return subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        stdin=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        **hidden_subprocess_kwargs(),
    )


def _require_git(
    completed: subprocess.CompletedProcess[str],
    action: str,
) -> None:
    if completed.returncode != 0:
        raise EndpointRuntimeError(
            f"unable to {action}: "
            + (completed.stderr or completed.stdout).strip()
        )


def _git_output(cwd: Path, *args: str) -> str:
    completed = _git(cwd, *args)
    _require_git(completed, "read Git state")
    return completed.stdout.strip()


def _last_json_object(value: str) -> dict[str, Any]:
    lines = value.splitlines()
    for index, line in enumerate(lines):
        if not line.lstrip().startswith("{"):
            continue
        candidate = "\n".join(lines[index:])
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise EndpointRuntimeError("node service returned no JSON status")


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _portable_path(path: Path, root: Path) -> str:
    return os.path.relpath(path.resolve(), root.resolve()).replace("\\", "/")


if __name__ == "__main__":
    main()
