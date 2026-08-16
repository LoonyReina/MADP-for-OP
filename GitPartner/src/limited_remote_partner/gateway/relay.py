from __future__ import annotations

import json
import os
import posixpath
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import AppConfig
from limited_remote_partner.core.request import ExecutionRequest
from limited_remote_partner.gateway.input_parser import _optional_client_action


RELAY_PROTOCOL_VERSION = "relay-v2"
SUPPORTED_RELAY_PROTOCOL_VERSIONS = {"relay-v1", RELAY_PROTOCOL_VERSION}
REMOTE_SHELL_TIMEOUT_SECONDS = 120
REMOTE_TRANSFER_TIMEOUT_SECONDS = 180
from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs


class RelayError(RuntimeError):
    pass


def effective_transport(config: AppConfig, request: ExecutionRequest) -> str:
    transport = (request.transport or config.relay.transport_mode or "relay").lower()
    if transport not in {"relay", "direct", "auto"}:
        raise RelayError(f"unsupported transport mode: {transport}")
    return transport


class ScpTransport:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._multiplexing_disabled_hosts: set[str] = set()

    def push_dir(
        self,
        src: Path,
        host: str | None,
        dst: str,
        *,
        timeout_seconds: float = REMOTE_TRANSFER_TIMEOUT_SECONDS,
    ) -> None:
        src = src.resolve()
        if host:
            self._ssh(
                host,
                ["mkdir", "-p", dst],
                timeout_seconds=REMOTE_SHELL_TIMEOUT_SECONDS,
            )
            self._scp(
                host,
                [str(src / "."), f"{host}:{dst.rstrip('/')}/"],
                timeout_seconds=timeout_seconds,
            )
            return

        dst_path = _local_path(self.config.repo_dir, dst)
        replace_tree(src, dst_path)

    def push_file(self, src: Path, host: str | None, dst: str) -> None:
        src = src.resolve()
        if host:
            parent = posixpath.dirname(dst.rstrip("/")) or "."
            self._ssh(
                host,
                ["mkdir", "-p", parent],
                timeout_seconds=REMOTE_SHELL_TIMEOUT_SECONDS,
            )
            self._scp(
                host,
                [str(src), f"{host}:{dst}"],
                timeout_seconds=REMOTE_TRANSFER_TIMEOUT_SECONDS,
            )
            return

        dst_path = _local_path(self.config.repo_dir, dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst_path)

    def pull_dir(self, host: str | None, src: str, dst: Path) -> None:
        dst = dst.resolve()
        if host:
            remove_tree_or_file(dst)
            dst.parent.mkdir(parents=True, exist_ok=True)
            self._scp(
                host,
                [f"{host}:{src.rstrip('/')}", str(dst)],
                timeout_seconds=REMOTE_TRANSFER_TIMEOUT_SECONDS,
            )
            return

        src_path = _local_path(self.config.repo_dir, src)
        replace_tree(src_path, dst)

    def run_ssh(
        self,
        host: str | None,
        remote_args: list[str],
    ) -> subprocess.CompletedProcess[str]:
        if host:
            return self._ssh(host, remote_args)
        return _run_checked(remote_args)

    def run_ssh_shell(
        self,
        host: str | None,
        script: str,
    ) -> subprocess.CompletedProcess[str]:
        if host:
            return self._ssh(
                host,
                ["bash", "-lc", shlex.quote(script)],
                timeout_seconds=REMOTE_SHELL_TIMEOUT_SECONDS,
            )
        return _run_checked(
            ["bash", "-lc", script],
            timeout_seconds=REMOTE_SHELL_TIMEOUT_SECONDS,
        )

    def stream_dir_to_ssh_shell(
        self,
        src: Path,
        host: str,
        script: str,
    ) -> subprocess.CompletedProcess[str]:
        """Stream a locally-created tar archive to one trusted SSH transaction."""
        src = src.resolve()
        archive_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(prefix="gitpartner-relay-", suffix=".tar", delete=False) as archive:
                archive_path = Path(archive.name)
            with tarfile.open(archive_path, mode="w") as bundle:
                for child in sorted(src.iterdir(), key=lambda item: item.name):
                    bundle.add(child, arcname=child.name, recursive=True)

            password_file = self._password_file(host)
            command = [
                *_sshpass_prefix(password_file),
                "ssh",
                *_interactive_options(self.config.relay.ssh_options, password_file),
                host,
                "bash",
                "-lc",
                shlex.quote(script),
            ]
            with archive_path.open("rb") as archive_stream:
                raw = subprocess.run(
                    command,
                    env=_noninteractive_env(),
                    stdin=archive_stream,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=False,
                    check=False,
                    timeout=REMOTE_TRANSFER_TIMEOUT_SECONDS,
                    **hidden_subprocess_kwargs(),
                )
            stdout = raw.stdout.decode("utf-8", errors="replace")
            stderr = raw.stderr.decode("utf-8", errors="replace")
            result = subprocess.CompletedProcess(command, raw.returncode, stdout, stderr)
            if result.returncode != 0:
                raise RelayError(
                    f"{command[0]} failed with exit {result.returncode}\n"
                    f"stdout: {stdout.strip()}\nstderr: {stderr.strip()}"
                )
            return result
        finally:
            if archive_path is not None:
                archive_path.unlink(missing_ok=True)

    def stream_dirs_to_ssh_shell(
        self,
        sources: dict[str, Path],
        host: str,
        script: str,
    ) -> subprocess.CompletedProcess[str]:
        """Stream multiple named directories through one trusted SSH transaction."""
        if not sources:
            raise RelayError("multi-directory relay stream requires sources")
        archive_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                prefix="gitpartner-relay-batch-",
                suffix=".tar",
                delete=False,
            ) as archive:
                archive_path = Path(archive.name)
            with tarfile.open(archive_path, mode="w") as bundle:
                for name, source in sorted(sources.items()):
                    src = source.resolve()
                    for child in sorted(src.iterdir(), key=lambda item: item.name):
                        bundle.add(
                            child,
                            arcname=f"{name}/{child.name}",
                            recursive=True,
                        )

            password_file = self._password_file(host)
            command = [
                *_sshpass_prefix(password_file),
                "ssh",
                *_interactive_options(self.config.relay.ssh_options, password_file),
                host,
                "bash",
                "-lc",
                shlex.quote(script),
            ]
            with archive_path.open("rb") as archive_stream:
                raw = subprocess.run(
                    command,
                    env=_noninteractive_env(),
                    stdin=archive_stream,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=False,
                    check=False,
                    timeout=REMOTE_TRANSFER_TIMEOUT_SECONDS,
                    **hidden_subprocess_kwargs(),
                )
            stdout = raw.stdout.decode("utf-8", errors="replace")
            stderr = raw.stderr.decode("utf-8", errors="replace")
            result = subprocess.CompletedProcess(
                command,
                raw.returncode,
                stdout,
                stderr,
            )
            if result.returncode != 0:
                raise RelayError(
                    f"{command[0]} failed with exit {result.returncode}\n"
                    f"stdout: {stdout.strip()}\nstderr: {stderr.strip()}"
                )
            return result
        finally:
            if archive_path is not None:
                archive_path.unlink(missing_ok=True)

    def _ssh(
        self,
        host: str,
        remote_args: list[str],
        *,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        password_file = self._password_file(host)
        options = self._remote_options(
            host,
            self.config.relay.ssh_options,
            password_file,
        )
        command = [
            *_sshpass_prefix(password_file),
            "ssh",
            *options,
            host,
            *remote_args,
        ]
        return self._run_remote_checked(
            host,
            command,
            password_file=password_file,
            program="ssh",
            configured_options=self.config.relay.ssh_options,
            trailing_args=[host, *remote_args],
            timeout_seconds=timeout_seconds,
        )

    def _scp(
        self,
        host: str,
        args: list[str],
        *,
        timeout_seconds: float | None = REMOTE_TRANSFER_TIMEOUT_SECONDS,
    ) -> None:
        password_file = self._password_file(host)
        options = self._remote_options(
            host,
            self.config.relay.scp_options,
            password_file,
        )
        command = [
            *_sshpass_prefix(password_file),
            "scp",
            "-r",
            *options,
            *args,
        ]
        self._run_remote_checked(
            host,
            command,
            password_file=password_file,
            program="scp",
            configured_options=self.config.relay.scp_options,
            trailing_args=["-r", *args],
            timeout_seconds=timeout_seconds,
        )

    def _remote_options(
        self,
        host: str,
        configured_options: tuple[str, ...],
        password_file: str | None,
    ) -> list[str]:
        options = _interactive_options(configured_options, password_file)
        if os.name == "nt" or host in self._multiplexing_disabled_hosts:
            return _without_ssh_multiplexing(options)
        return options

    def _run_remote_checked(
        self,
        host: str,
        command: list[str],
        *,
        password_file: str | None,
        program: str,
        configured_options: tuple[str, ...],
        trailing_args: list[str],
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        try:
            if timeout_seconds is None:
                return _run_checked(command)
            return _run_checked(
                command,
                timeout_seconds=timeout_seconds,
            )
        except RelayError as exc:
            if not _is_ssh_multiplexing_failure(str(exc)):
                raise
            options = _without_ssh_multiplexing(
                _interactive_options(configured_options, password_file)
            )
            fallback = [
                *_sshpass_prefix(password_file),
                program,
                *options,
                *trailing_args,
            ]
            if timeout_seconds is None:
                result = _run_checked(fallback)
            else:
                result = _run_checked(
                    fallback,
                    timeout_seconds=timeout_seconds,
                )
            self._multiplexing_disabled_hosts.add(host)
            return result

    def _password_file(self, host: str) -> str | None:
        if host == self.config.relay.client_ssh:
            return self.config.relay.client_password_file
        if host == self.config.relay.server_ssh:
            return self.config.relay.server_password_file
        return None


def request_to_dict(request: ExecutionRequest) -> dict[str, Any]:
    data = asdict(request)
    for key, value in list(data.items()):
        if isinstance(value, tuple):
            data[key] = list(value)
    data["relay_protocol_version"] = request.relay_protocol_version
    return data


def request_from_dict(raw: dict[str, Any]) -> ExecutionRequest:
    protocol_version = str(raw.get("relay_protocol_version") or "relay-v1")
    if protocol_version not in SUPPORTED_RELAY_PROTOCOL_VERSIONS:
        raise RelayError(f"unsupported relay protocol: {protocol_version}")
    return ExecutionRequest(
        request_id=str(raw.get("request_id") or raw.get("id") or "request"),
        command=_string_tuple(raw.get("command", []), "command"),
        working_dir=str(raw.get("working_dir") or "."),
        output_subdir=str(raw.get("output_subdir") or raw.get("request_id") or "latest"),
        env={
            str(key): str(value)
            for key, value in _dict(raw.get("env", {}), "env").items()
        },
        timeout_seconds=int(raw.get("timeout_seconds", 0)),
        sync_interval_seconds=max(1, int(raw.get("sync_interval_seconds", 30))),
        log_name=str(raw.get("log_name") or "job.log"),
        payload_paths=_string_tuple(raw.get("payload_paths", []), "payload_paths"),
        client_command=_string_tuple(raw.get("client_command", []), "client_command"),
        return_paths=_string_tuple(raw.get("return_paths", []), "return_paths"),
        payload_root=_optional_string(raw.get("payload_root")),
        client_work_dir=_optional_string(raw.get("client_work_dir")),
        sandbox_profile=_optional_string(raw.get("sandbox_profile")),
        transport=_optional_transport(raw.get("transport")),
        server_action=_optional_server_action(raw.get("server_action")),
        server_action_args={
            str(key): str(value)
            for key, value in _dict(raw.get("server_action_args", {}), "server_action_args").items()
        },
        client_action=_optional_client_action(raw.get("client_action")),
        client_action_args={
            str(key): str(value)
            for key, value in _dict(raw.get("client_action_args", {}), "client_action_args").items()
        },
        request_kind=str(raw.get("request_kind") or "command"),
        completion_mode=str(raw.get("completion_mode") or "terminal"),
        engine_job_id=_optional_string(raw.get("engine_job_id")),
        target_nodes=_string_tuple(raw.get("target_nodes", []), "target_nodes"),
        target_tags=_string_tuple(raw.get("target_tags", []), "target_tags"),
        target_roles=_string_tuple(raw.get("target_roles", []), "target_roles"),
        target_endpoint_id=_optional_string(raw.get("target_endpoint_id")),
        target_environment_id=_optional_string(raw.get("target_environment_id")),
        target_gateway_id=_optional_string(raw.get("target_gateway_id")),
        target_transport_mode=_optional_string(raw.get("target_transport_mode")),
        registration_generation=_optional_string(raw.get("registration_generation")),
        experiment_id=_optional_string(raw.get("experiment_id")),
        attempt_id=_optional_string(raw.get("attempt_id")),
        workflow_ingest=bool(raw.get("workflow_ingest", True)),
        fanout=bool(raw.get("fanout", False)),
        relay_protocol_version=protocol_version,
    )


def write_request(path: Path, request: ExecutionRequest, max_file_bytes: int) -> None:
    write_json(path, request_to_dict(request), max_file_bytes)


def read_request(path: Path) -> ExecutionRequest:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RelayError(f"request file must be an object: {path}")
    return request_from_dict(raw)


def write_json(path: Path, payload: dict[str, Any], max_file_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    if len(data) > max_file_bytes:
        raise RelayError(f"{path.name} exceeds configured max_file_bytes")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    temporary.write_bytes(data)
    for attempt in range(50):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt >= 49:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(0.01)


def read_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RelayError(f"json file must be an object: {path}")
    return raw


def copy_requested_paths(
    source_root: Path,
    dest_root: Path,
    paths: tuple[str, ...],
    max_file_bytes: int,
    *,
    missing_ok: bool = False,
) -> tuple[list[str], list[str]]:
    copied: list[str] = []
    missing: list[str] = []
    source_root = source_root.resolve()
    dest_root.mkdir(parents=True, exist_ok=True)

    for repo_path in paths:
        if repo_path in ("", "."):
            raise RelayError("refusing to relay the whole source root")
        src = safe_join(source_root, repo_path)
        if not src.exists():
            if missing_ok:
                missing.append(repo_path)
                continue
            raise RelayError(f"payload path does not exist: {repo_path}")
        dst = safe_join(dest_root, repo_path)
        _check_path_sizes(src, max_file_bytes)
        if src.is_dir():
            replace_tree(src, dst)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        copied.append(repo_path)
    return copied, missing


def replace_tree(src: Path, dst: Path) -> None:
    remove_tree_or_file(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)


def remove_tree_or_file(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def safe_join(root: Path, repo_path: str) -> Path:
    normalized = repo_path.replace("\\", "/").lstrip("/")
    if ".." in Path(normalized).parts:
        raise RelayError(f"path cannot contain '..': {repo_path}")
    target = (root / normalized).resolve()
    root = root.resolve()
    if target != root and root not in target.parents:
        raise RelayError(f"path escapes root: {repo_path}")
    return target


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _check_path_sizes(path: Path, max_file_bytes: int) -> None:
    if path.is_file():
        if path.stat().st_size > max_file_bytes:
            raise RelayError(f"refusing file above size limit: {path}")
        return
    for item in path.rglob("*"):
        if item.is_file() and item.stat().st_size > max_file_bytes:
            raise RelayError(f"refusing file above size limit: {item}")


def _local_path(base: Path, path: str) -> Path:
    candidate = Path(os.path.expanduser(path))
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def _run_checked(
    command: list[str],
    *,
    timeout_seconds: float | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        env=_noninteractive_env(),
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_seconds,
        **hidden_subprocess_kwargs(),
    )
    if result.returncode != 0:
        raise RelayError(
            f"{command[0]} failed with exit {result.returncode}\n"
            f"stdout: {result.stdout.strip()}\nstderr: {result.stderr.strip()}"
        )
    return result


def _sshpass_prefix(password_file: str | None) -> list[str]:
    if not password_file:
        return []
    path = Path(password_file)
    if not path.exists():
        raise RelayError(f"ssh password file does not exist: {password_file}")
    return ["sshpass", "-f", password_file]


def _interactive_options(options: tuple[str, ...], password_file: str | None) -> list[str]:
    if not password_file:
        return _force_no_password_prompts(options)
    filtered: list[str] = []
    items = list(options)
    index = 0
    while index < len(items):
        item = items[index]
        next_item = items[index + 1] if index + 1 < len(items) else ""
        if item == "-o" and next_item.startswith("BatchMode="):
            index += 2
            continue
        if item.startswith("BatchMode="):
            index += 1
            continue
        filtered.append(item)
        index += 1
    filtered.extend(["-o", "BatchMode=no", "-o", "PasswordAuthentication=yes"])
    return filtered


def _force_no_password_prompts(options: tuple[str, ...]) -> list[str]:
    filtered: list[str] = []
    items = list(options)
    index = 0
    has_strict_host_key = False
    while index < len(items):
        item = items[index]
        next_item = items[index + 1] if index + 1 < len(items) else ""
        if item == "-o" and (
            next_item.startswith("BatchMode=")
            or next_item.startswith("NumberOfPasswordPrompts=")
        ):
            index += 2
            continue
        if item == "-o":
            if next_item.startswith("StrictHostKeyChecking="):
                has_strict_host_key = True
            filtered.extend([item, next_item])
            index += 2
            continue
        if item.startswith("BatchMode=") or item.startswith("NumberOfPasswordPrompts="):
            index += 1
            continue
        if "StrictHostKeyChecking=" in item:
            has_strict_host_key = True
        filtered.append(item)
        index += 1
    filtered.extend(["-o", "BatchMode=yes", "-o", "NumberOfPasswordPrompts=0"])
    if not has_strict_host_key:
        filtered.extend(["-o", "StrictHostKeyChecking=accept-new"])
    return filtered


def _without_ssh_multiplexing(options: list[str]) -> list[str]:
    filtered: list[str] = []
    items = list(options)
    index = 0
    multiplexing_keys = ("ControlMaster=", "ControlPersist=", "ControlPath=")
    while index < len(items):
        item = items[index]
        next_item = items[index + 1] if index + 1 < len(items) else ""
        if item == "-o" and next_item.startswith(multiplexing_keys):
            index += 2
            continue
        if item.startswith(multiplexing_keys):
            index += 1
            continue
        filtered.append(item)
        index += 1
    return filtered


def _is_ssh_multiplexing_failure(message: str) -> bool:
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "getsockname failed",
            "control socket",
            "mux_client",
            "not a socket",
        )
    )


def _noninteractive_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    env.setdefault("GIT_ASKPASS", "")
    env.setdefault("SSH_ASKPASS", "")
    env.setdefault("GCM_INTERACTIVE", "Never")
    return env


def _dict(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RelayError(f"{name} must be an object")
    return value


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RelayError(f"{name} must be a string list")
    return tuple(value)


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_transport(value: Any) -> str | None:
    if value in (None, ""):
        return None
    transport = str(value).lower()
    if transport not in {"relay", "direct", "auto"}:
        raise RelayError("request transport must be one of: relay, direct, auto")
    return transport


def _optional_server_action(value: Any) -> str | None:
    if value in (None, ""):
        return None
    action = str(value).lower()
    if action not in {
        "lan-bootstrap",
        "lan-sync-code",
        "lan-restart-service",
        "lan-cancel-request",
        "lan-node-ack",
        "lan-diagnose",
        "lan-sync-artifact",
        "server-tmux-command",
    }:
        raise RelayError(
            "request server_action must be one of: "
            "lan-bootstrap, lan-sync-code, lan-restart-service, lan-cancel-request, "
            "lan-node-ack, lan-diagnose, lan-sync-artifact, "
            "server-tmux-command"
        )
    return action
