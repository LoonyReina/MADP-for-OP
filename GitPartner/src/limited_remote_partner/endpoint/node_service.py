from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.gateway.git_lock import force_cleanup_repo_git
from limited_remote_partner.core.login_environment import (
    import_login_network_environment,
    network_environment_report,
)
from limited_remote_partner.endpoint.node_enrollment import (
    adopt_published_generation,
    node_registration_status,
    select_node_config,
)
from limited_remote_partner.core.process_utils import (
    PartnerRoleProcess,
    hidden_subprocess_kwargs,
    matching_partner_role_processes,
    matching_partner_role_processes_for_repo_identity,
    process_group_kwargs,
    process_start_token,
    stop_exact_process_identity,
)


SERVICE_SCHEMA = "git-partner.node-service.v1"
NOT_ENROLLED_EXIT = 4


@dataclass(frozen=True)
class ServicePaths:
    state_dir: Path
    metadata: Path
    log: Path
    lock: Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run one enrolled GitPartner node as a fenced resident service"
    )
    parser.add_argument(
        "action",
        choices=("start", "foreground", "supervise", "status", "stop", "follow"),
    )
    parser.add_argument("--config")
    parser.add_argument("--node")
    parser.add_argument("--repo-dir", default=".")
    parser.add_argument("--role", choices=("client", "server", "local"))
    parser.add_argument("--stop-timeout-seconds", type=float, default=12.0)
    parser.add_argument("--tail-lines", type=int, default=40)
    parser.add_argument("--follow-interval-seconds", type=float, default=0.5)
    parser.add_argument("--force-restart", action="store_true")
    parser.add_argument("--retire-endpoint-id", action="append", default=[])
    args = parser.parse_args(argv)

    root = Path(args.repo_dir).resolve()
    config_path = _resolve_config(root, args.config, args.node)
    if config_path is None:
        status = node_registration_status(repo_dir=root, node_id=args.node)
        _print_status(status)
        raise SystemExit(NOT_ENROLLED_EXIT)
    adopt_published_generation(config_path, repo_dir=root)
    config = load_config(config_path, base_dir=root)
    status = _registration_status(config_path, config)
    if not status.get("runnable"):
        _print_status(status)
        raise SystemExit(NOT_ENROLLED_EXIT)

    role = args.role or config.relay.role
    paths = _service_paths(config)
    if args.action == "status":
        _print_status(service_status(config_path, config, paths))
        return
    if args.action == "follow":
        raise SystemExit(
            follow_service_log(
                config_path,
                config,
                paths,
                tail_lines=max(0, args.tail_lines),
                interval_seconds=max(0.1, args.follow_interval_seconds),
            )
        )
    if args.action == "stop":
        result = stop_service(paths, timeout_seconds=args.stop_timeout_seconds)
        result["orphan_processes"] = _stop_endpoint_processes(
            config,
            role,
            timeout_seconds=args.stop_timeout_seconds,
        )
        _print_status(result)
        return
    if args.action == "foreground":
        stop_service(paths, timeout_seconds=args.stop_timeout_seconds)
        replaced = _stop_endpoint_processes(
            config,
            role,
            timeout_seconds=args.stop_timeout_seconds,
            retire_endpoint_ids=(
                set(args.retire_endpoint_id)
                if args.force_restart
                else set()
            ),
        )
        force_recovery = (
            force_cleanup_repo_git(config.repo_dir)
            if args.force_restart
            else {}
        )
        print(
            "GITPARTNER_NODE_FOREGROUND_START "
            f"node_id={config.node.node_id} "
            f"endpoint_id={config.endpoint.endpoint_id} "
            f"generation={config.endpoint.generation} "
            f"replaced_processes={len(replaced)} "
            f"force_recovery={json.dumps(force_recovery, sort_keys=True)}",
            flush=True,
        )
        raise SystemExit(_supervise(config_path, config, paths, role))
    if args.action == "supervise":
        raise SystemExit(_supervise(config_path, config, paths, role))

    stop_service(paths, timeout_seconds=args.stop_timeout_seconds)
    orphan_processes = _stop_endpoint_processes(
        config,
        role,
        timeout_seconds=args.stop_timeout_seconds,
        retire_endpoint_ids=(
            set(args.retire_endpoint_id)
            if args.force_restart
            else set()
        ),
    )
    force_recovery = (
        force_cleanup_repo_git(config.repo_dir)
        if args.force_restart
        else {}
    )
    payload = _start_background(config_path, config, paths, role)
    payload["replaced_processes"] = orphan_processes
    payload["force_recovery"] = force_recovery
    _print_status(payload)
    if not payload.get("running"):
        raise SystemExit(3)


def service_status(
    config_path: Path,
    config: AppConfig,
    paths: ServicePaths,
) -> dict[str, Any]:
    registration = _registration_status(config_path, config)
    metadata = _read_json(paths.metadata)
    pid = int(metadata.get("supervisor_pid") or 0)
    token = str(metadata.get("supervisor_start_token") or "")
    running = bool(pid and token and process_start_token(pid) == token)
    child_pid = int(metadata.get("child_pid") or 0)
    child_token = str(metadata.get("child_start_token") or "")
    child_running = bool(
        child_pid
        and child_token
        and process_start_token(child_pid) == child_token
    )
    service_state = str(metadata.get("state") or "stopped")
    presence = _read_json(paths.state_dir / "presence.json")
    capabilities = presence.get("capabilities")
    if not isinstance(capabilities, dict):
        capabilities = {}
    blockers = capabilities.get("blockers")
    if not isinstance(blockers, list):
        blockers = []
    return {
        "schema": SERVICE_SCHEMA,
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "config_path": _portable_path(config_path, config.repo_dir),
        "registered": True,
        "registration_state": registration.get("registration_state", ""),
        "central_status": registration.get("central_status", "unconfirmed"),
        "running": running,
        "healthy": bool(running and child_running and service_state == "running"),
        "service_state": service_state,
        "supervisor_pid": pid if running else 0,
        "child_pid": child_pid if running else 0,
        "child_running": child_running if running else False,
        "presence_state": str(presence.get("state") or ""),
        "presence_reason": str(presence.get("reason") or ""),
        "presence_sequence": int(presence.get("sequence") or 0),
        "heartbeat_at": str(presence.get("heartbeat_at") or ""),
        "lease_expires_at": str(presence.get("lease_expires_at") or ""),
        "last_publish_error": str(presence.get("last_publish_error") or ""),
        "capability_ready": bool(capabilities.get("ready")),
        "capability_blockers": [str(value) for value in blockers],
        "log_path": _portable_path(paths.log, config.repo_dir),
        "metadata_path": _portable_path(paths.metadata, config.repo_dir),
    }


def follow_service_log(
    config_path: Path,
    config: AppConfig,
    paths: ServicePaths,
    *,
    tail_lines: int,
    interval_seconds: float,
) -> int:
    status = service_status(config_path, config, paths)
    _print_status(status)
    position = 0
    if paths.log.is_file():
        data = paths.log.read_bytes()
        if tail_lines:
            selected = data.splitlines(keepends=True)[-tail_lines:]
            sys.stdout.write(
                b"".join(selected).decode("utf-8", errors="replace")
            )
            sys.stdout.flush()
        position = len(data)
    elif not status.get("running"):
        print(
            "GITPARTNER_NODE_LOG_UNAVAILABLE service is not running "
            f"path={_portable_path(paths.log, config.repo_dir)}",
            file=sys.stderr,
        )
        return 3

    stopped_checks = 0
    try:
        while True:
            if paths.log.is_file():
                size = paths.log.stat().st_size
                if size < position:
                    position = 0
                if size > position:
                    with paths.log.open("rb") as handle:
                        handle.seek(position)
                        chunk = handle.read()
                        position = handle.tell()
                    sys.stdout.write(chunk.decode("utf-8", errors="replace"))
                    sys.stdout.flush()
            current = service_status(config_path, config, paths)
            if current.get("running"):
                stopped_checks = 0
            else:
                stopped_checks += 1
                if stopped_checks >= 2:
                    print(
                        "GITPARTNER_NODE_LOG_STOPPED "
                        f"service_state={current.get('service_state', '')}",
                        file=sys.stderr,
                    )
                    return 3
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        return 0


def _registration_status(
    config_path: Path,
    config: AppConfig,
) -> dict[str, Any]:
    if config.node_lifecycle.enabled:
        return node_registration_status(
            config_path=config_path,
            repo_dir=config.repo_dir,
        )
    return {
        "registered": True,
        "runnable": True,
        "code": "ENDPOINT_LIFECYCLE_DELEGATED",
        "registration_state": "delegated",
        "central_status": "accepted",
    }


def stop_service(paths: ServicePaths, *, timeout_seconds: float) -> dict[str, Any]:
    metadata = _read_json(paths.metadata)
    pid = int(metadata.get("supervisor_pid") or 0)
    token = str(metadata.get("supervisor_start_token") or "")
    child_pid = int(metadata.get("child_pid") or 0)
    child_token = str(metadata.get("child_start_token") or "")
    if not pid or not token or process_start_token(pid) != token:
        child = stop_exact_process_identity(
            child_pid,
            child_token,
            timeout_seconds=timeout_seconds,
        )
        return {
            "schema": SERVICE_SCHEMA,
            "running": False,
            "stopped": bool(child["stopped"]),
            "reason": (
                "orphan-child-stopped"
                if child["stopped"]
                else "no-matching-supervisor"
            ),
            "child": child,
            "metadata_path": str(paths.metadata),
        }
    supervisor = stop_exact_process_identity(
        pid,
        token,
        timeout_seconds=timeout_seconds,
    )
    child = stop_exact_process_identity(
        child_pid,
        child_token,
        timeout_seconds=timeout_seconds,
    )
    running = (
        process_start_token(pid) == token
        or process_start_token(child_pid) == child_token
    )
    return {
        "schema": SERVICE_SCHEMA,
        "running": running,
        "stopped": bool(supervisor["stopped"] or child["stopped"]),
        "forced": bool(supervisor["forced"] or child["forced"]),
        "detail": str(child.get("detail") or supervisor.get("detail") or ""),
        "reason": "service-stopped" if not running else "service-still-running",
        "supervisor": supervisor,
        "child": child,
        "metadata_path": str(paths.metadata),
    }


def _stop_endpoint_processes(
    config: AppConfig,
    role: str,
    *,
    timeout_seconds: float,
    retire_endpoint_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for process in _matching_endpoint_processes(
        config,
        role,
        retire_endpoint_ids=retire_endpoint_ids,
    ):
        if process.pid == os.getpid():
            continue
        token = process_start_token(process.pid)
        supervisor = _stop_endpoint_supervisor(
            process,
            timeout_seconds=timeout_seconds,
        )
        child = (
            {
                "stopped": True,
                "forced": False,
                "detail": "stopped-with-supervisor",
            }
            if process_start_token(process.pid) != token
            else stop_exact_process_identity(
                process.pid,
                token,
                timeout_seconds=timeout_seconds,
            )
        )
        rows.append(
            {
                "pid": process.pid,
                "start_token": token,
                "args": list(process.args),
                "stopped": bool(
                    supervisor.get("stopped") or child.get("stopped")
                ),
                "forced": bool(
                    supervisor.get("forced") or child.get("forced")
                ),
                "detail": str(child.get("detail") or ""),
                "supervisor": supervisor,
            }
        )
    return rows


def _stop_endpoint_supervisor(
    process: PartnerRoleProcess,
    *,
    timeout_seconds: float,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    args = list(process.args)
    try:
        config_index = args.index("--config")
    except ValueError:
        return {"stopped": False, "detail": "config-argument-missing"}
    if config_index + 1 >= len(args):
        return {"stopped": False, "detail": "config-path-missing"}
    try:
        cwd = (proc_root / str(process.pid) / "cwd").resolve(strict=True)
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return {"stopped": False, "detail": "process-cwd-unavailable"}
    config_path = Path(args[config_index + 1])
    if not config_path.is_absolute():
        config_path = cwd / config_path
    try:
        old_config = load_config(config_path, base_dir=cwd)
    except (OSError, ValueError, json.JSONDecodeError):
        return {"stopped": False, "detail": "old-config-unreadable"}
    paths = _service_paths(old_config)
    metadata = _read_json(paths.metadata)
    child_pid = int(metadata.get("child_pid") or 0)
    child_token = str(metadata.get("child_start_token") or "")
    observed_token = process_start_token(process.pid)
    if (
        child_pid != process.pid
        or not child_token
        or child_token != observed_token
    ):
        return {
            "stopped": False,
            "detail": "supervisor-child-identity-mismatch",
        }
    result = stop_service(paths, timeout_seconds=timeout_seconds)
    return {
        "stopped": bool(result.get("stopped")),
        "forced": bool(result.get("forced")),
        "detail": str(result.get("reason") or result.get("detail") or ""),
        "supervisor_pid": int(metadata.get("supervisor_pid") or 0),
    }


def _matching_endpoint_processes(
    config: AppConfig,
    role: str,
    *,
    retire_endpoint_ids: set[str] | None = None,
) -> list[PartnerRoleProcess]:
    matches = {
        process.pid: process
        for process in matching_partner_role_processes(
            role,
            repo_dir=config.repo_dir,
        )
    }
    for process in matching_partner_role_processes_for_repo_identity(
        role,
        config.repo_dir,
    ):
        if process.pid in matches:
            continue
        if _process_endpoint_identity_matches(
            process,
            config,
            retire_endpoint_ids=retire_endpoint_ids,
        ):
            matches[process.pid] = process
    return [matches[pid] for pid in sorted(matches)]


def _process_endpoint_identity_matches(
    process: PartnerRoleProcess,
    config: AppConfig,
    *,
    retire_endpoint_ids: set[str] | None = None,
    proc_root: Path = Path("/proc"),
) -> bool:
    args = list(process.args)
    try:
        config_index = args.index("--config")
    except ValueError:
        return False
    if config_index + 1 >= len(args):
        return False
    path = Path(args[config_index + 1])
    if not path.is_absolute():
        try:
            cwd = (proc_root / str(process.pid) / "cwd").resolve(strict=True)
        except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
            return False
        path = cwd / path
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, PermissionError, OSError, json.JSONDecodeError):
        return False
    node = payload.get("node")
    endpoint = payload.get("endpoint")
    if not isinstance(node, dict) or not isinstance(endpoint, dict):
        return False
    if str(node.get("node_id") or "") != config.node.node_id:
        return False
    endpoint_id = str(endpoint.get("endpoint_id") or "")
    if endpoint_id in (retire_endpoint_ids or set()):
        return True
    return bool(
        endpoint_id == config.endpoint.endpoint_id
        and str(endpoint.get("generation") or "") == config.endpoint.generation
    )


def _start_background(
    config_path: Path,
    config: AppConfig,
    paths: ServicePaths,
    role: str,
) -> dict[str, Any]:
    paths.log.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "limited_remote_partner.endpoint.node_service",
        "supervise",
        "--config",
        str(config_path),
        "--repo-dir",
        str(config.repo_dir),
        "--role",
        role,
    ]
    environment = _runtime_environment(config)
    with paths.log.open("a", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=config.repo_dir,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            close_fds=True,
            **process_group_kwargs(),
        )
    deadline = time.monotonic() + 8
    healthy_since: float | None = None
    while time.monotonic() < deadline:
        metadata = _read_json(paths.metadata)
        if (
            int(metadata.get("supervisor_pid") or 0) == process.pid
            and metadata.get("state") in {"starting", "running"}
        ):
            status = service_status(config_path, config, paths)
            if status.get("healthy"):
                if healthy_since is None:
                    healthy_since = time.monotonic()
                elif time.monotonic() - healthy_since >= 1.0:
                    return status
            else:
                healthy_since = None
        if process.poll() is not None:
            break
        time.sleep(0.1)
    return {
        **service_status(config_path, config, paths),
        "start_error": (
            f"supervisor exited rc={process.returncode}"
            if process.poll() is not None
            else "supervisor did not publish service metadata within 8 seconds"
        ),
    }


def _supervise(
    config_path: Path,
    config: AppConfig,
    paths: ServicePaths,
    role: str,
) -> int:
    paths.state_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = paths.lock.open("a+", encoding="utf-8")
    if not _acquire_lock(lock_handle):
        return 5
    stopping = False
    child: subprocess.Popen[bytes] | None = None

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        if child is not None and child.poll() is None:
            child.terminate()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    supervisor_pid = os.getpid()
    supervisor_token = process_start_token(supervisor_pid)
    base = {
        "schema": SERVICE_SCHEMA,
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
        "config_path": _portable_path(config_path, config.repo_dir),
        "role": role,
        "supervisor_pid": supervisor_pid,
        "supervisor_start_token": supervisor_token,
        "network_environment": network_environment_report(os.environ),
    }
    _write_json_atomic(paths.metadata, {**base, "state": "starting", "child_pid": 0})
    fast_failures = 0
    try:
        while not stopping:
            started = time.monotonic()
            child = subprocess.Popen(
                _partner_command(config_path, config, role),
                cwd=config.repo_dir,
                env=_runtime_environment(config),
                stdin=subprocess.DEVNULL,
                **hidden_subprocess_kwargs(),
            )
            _write_json_atomic(
                paths.metadata,
                {
                    **base,
                    "state": "running",
                    "child_pid": child.pid,
                    "child_start_token": process_start_token(child.pid),
                    "started_at": _utc_now(),
                    "restart_count": fast_failures,
                },
            )
            returncode = child.wait()
            runtime = time.monotonic() - started
            if stopping:
                break
            fast_failures = 0 if runtime >= 30 else fast_failures + 1
            _write_json_atomic(
                paths.metadata,
                {
                    **base,
                    "state": "backoff",
                    "child_pid": 0,
                    "last_exit_code": returncode,
                    "last_runtime_seconds": round(runtime, 3),
                    "restart_count": fast_failures,
                    "updated_at": _utc_now(),
                },
            )
            if fast_failures >= 5:
                _write_json_atomic(
                    paths.metadata,
                    {
                        **base,
                        "state": "failed",
                        "child_pid": 0,
                        "last_exit_code": returncode,
                        "reason": "five-fast-failures",
                        "updated_at": _utc_now(),
                    },
                )
                return returncode or 6
            time.sleep(min(30.0, float(2 ** max(0, fast_failures - 1))))
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        _write_json_atomic(
            paths.metadata,
            {
                **base,
                "state": "stopped",
                "child_pid": 0,
                "stopped_at": _utc_now(),
            },
        )
        lock_handle.close()
    return 0


def _run_partner(config_path: Path, config: AppConfig, role: str) -> int:
    process = subprocess.run(
        _partner_command(config_path, config, role),
        cwd=config.repo_dir,
        env=_runtime_environment(config),
        stdin=sys.stdin,
        check=False,
        **hidden_subprocess_kwargs(),
    )
    return process.returncode


def _partner_command(
    config_path: Path,
    config: AppConfig,
    role: str,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "limited_remote_partner.cli.partner",
        "--config",
        str(config_path),
        "--role",
        role,
        "--transport",
        config.relay.transport_mode,
    ]


def _resolve_config(root: Path, value: str | None, node: str | None) -> Path | None:
    if value:
        path = Path(value)
        return path.resolve() if path.is_absolute() else (root / path).resolve()
    path, _error, _candidates = select_node_config(root, node)
    return path


def _service_paths(config: AppConfig) -> ServicePaths:
    state_dir = config.repo_dir / config.io.state_dir
    safe_node = config.node.node_id or "unnamed-node"
    return ServicePaths(
        state_dir=state_dir,
        metadata=state_dir / "node_service.json",
        log=config.repo_dir / "work" / "logs" / f"git-partner-node-{safe_node}.log",
        lock=state_dir / "node_service.lock",
    )


def _runtime_environment(config: AppConfig) -> dict[str, str]:
    environment, _report = import_login_network_environment(
        os.environ,
        enabled=config.network_environment.login_shell_import,
    )
    repo_source = (config.repo_dir / "src").resolve()
    configured_source = str(
        environment.get("GITPARTNER_RUNTIME_SOURCE") or ""
    ).strip()
    if configured_source:
        requested = Path(configured_source)
        if not requested.is_absolute():
            raise RuntimeError("GITPARTNER_RUNTIME_SOURCE must be absolute")
        source_path = requested.resolve()
        package_path = source_path / "limited_remote_partner"
        if (
            requested.is_symlink()
            or source_path.is_symlink()
            or not (package_path / "__init__.py").is_file()
            or not (package_path / "core" / "__init__.py").is_file()
            or not (package_path / "cli" / "partner.py").is_file() or not (package_path / "endpoint" / "node_service.py").is_file()
        ):
            raise RuntimeError(
                "GITPARTNER_RUNTIME_SOURCE is not an immutable "
                "GitPartner source tree"
            )
    else:
        source_path = repo_source
    source = str(source_path)
    existing_parts = [
        item
        for item in str(environment.get("PYTHONPATH") or "").split(
            os.pathsep
        )
        if item and item not in {source, str(repo_source)}
    ]
    environment["PYTHONPATH"] = os.pathsep.join([source, *existing_parts])
    if configured_source:
        environment["GITPARTNER_RUNTIME_SOURCE"] = source
    environment["PYTHONUNBUFFERED"] = "1"
    environment["GIT_TERMINAL_PROMPT"] = "0"
    environment.pop("GIT_ASKPASS", None)
    environment.pop("SSH_ASKPASS", None)
    return environment


def _portable_path(path: Path, root: Path) -> str:
    try:
        value = os.path.relpath(path.resolve(), root.resolve())
    except ValueError:
        return path.name
    return "." if value == "." else value.replace("\\", "/")


def _acquire_lock(handle: Any) -> bool:
    try:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except ImportError:
        return True
    except BlockingIOError:
        return False


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for attempt in range(50):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt >= 49:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(0.02)


def _print_status(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True))
    if value.get("code") == "NODE_CHANNEL_BOOTSTRAP_BLOCKED":
        print(
            "GITPARTNER_NODE_CHANNEL_BOOTSTRAP_BLOCKED "
            f"next_action={value.get('next_action', '')}",
            file=sys.stderr,
        )
    elif not value.get("registered", True):
        reminder = str(
            value.get("reminder")
            or "bash scripts/start_gitpartner_service.sh --enroll"
        )
        print(f"GITPARTNER_NODE_NOT_ENROLLED reminder={reminder}", file=sys.stderr)
    elif value.get("central_status") == "unconfirmed":
        marker = (
            "GITPARTNER_NODE_SERVICE_RUNNING_AWAITING_ACCEPTANCE"
            if value.get("running")
            else "GITPARTNER_NODE_AWAITING_ACCEPTANCE"
        )
        print(
            f"{marker} this is not a startup failure; "
            "the service may report capabilities but cannot receive scheduled "
            "work until central admission",
            file=sys.stderr,
        )


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    main()
