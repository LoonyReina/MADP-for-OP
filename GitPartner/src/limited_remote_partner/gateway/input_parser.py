from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import AppConfig
from limited_remote_partner.core.request import ExecutionRequest


def input_changed(config: AppConfig, changed_paths: list[str]) -> bool:
    input_dir = config.io.input_dir.rstrip("/")
    request_file = config.executor.request_file.replace("\\", "/").strip("/")
    payload_prefix = f"{input_dir}/payloads"
    run_script = f"{input_dir}/run.sh"
    watched_paths = {request_file, run_script}
    for path in changed_paths:
        normalized = _normalized_repo_path(path)
        if (
            normalized in watched_paths
            or is_append_request_path(config, normalized)
            or _path_matches(normalized, payload_prefix)
        ):
            return True
    return False


def changed_request_paths(
    config: AppConfig,
    changed_paths: list[str],
) -> list[str]:
    legacy = config.executor.request_file.replace("\\", "/").strip("/")
    paths = {
        normalized
        for path in changed_paths
        if (
            (normalized := _normalized_repo_path(path)) == legacy
            or is_append_request_path(config, normalized)
        )
    }
    return sorted(paths, key=lambda path: (path == legacy, path))


def is_append_request_path(config: AppConfig, path: str) -> bool:
    input_dir = config.io.input_dir.rstrip("/")
    pattern = (
        rf"^{re.escape(input_dir)}/requests/"
        r"[A-Za-z0-9._-]+/job\.json$"
    )
    return re.fullmatch(pattern, path.replace("\\", "/").strip("/")) is not None


def parse_request(
    config: AppConfig,
    trigger_ref: str,
    request_file: str | Path | None = None,
) -> ExecutionRequest:
    request_path = resolve_request_path(config, request_file)
    raw = _load_request_file(request_path)

    request_id = _normalize_name(
        str(raw.get("id") or raw.get("request_id") or trigger_ref[:12])
    )
    output_subdir = _normalize_repo_path(
        str(raw.get("output_subdir") or config.executor.default_output_subdir)
    ).rstrip("/")

    command = _command_from(raw, config)
    working_dir = _normalize_repo_path(str(raw.get("working_dir") or config.executor.working_dir))
    env = _env_from(raw, config)
    timeout_seconds = int(raw.get("timeout_seconds", config.executor.timeout_seconds))
    sync_interval_seconds = max(
        1, int(raw.get("sync_interval_seconds", config.executor.sync_interval_seconds))
    )
    payload_paths = _path_list_from(raw, "payload_paths")
    client_command = _optional_command_from(raw, "client_command")
    return_paths = _path_list_from(raw, "return_paths")
    transport = _optional_transport(raw.get("transport"))
    server_action = _optional_server_action(raw.get("server_action"))
    server_action_args = _string_dict_from(raw, "server_action_args")
    client_action = _optional_client_action(raw.get("client_action"))
    client_action_args = _string_dict_from(raw, "client_action_args")
    request_kind = _request_kind(raw.get("request_kind"))
    completion_mode = _completion_mode(raw.get("completion_mode"))
    target_nodes = _string_list_from(raw, "target_nodes")
    target_tags = _string_list_from(raw, "target_tags")
    target_roles = _string_list_from(raw, "target_roles")

    return ExecutionRequest(
        request_id=request_id,
        command=tuple(command),
        working_dir=working_dir,
        output_subdir=output_subdir,
        env=env,
        timeout_seconds=timeout_seconds,
        sync_interval_seconds=sync_interval_seconds,
        log_name=str(raw.get("log_name") or config.executor.log_name),
        payload_paths=tuple(payload_paths),
        client_command=tuple(client_command),
        return_paths=tuple(return_paths),
        payload_root=_optional_string(raw.get("payload_root")),
        client_work_dir=_optional_string(raw.get("client_work_dir")),
        sandbox_profile=_optional_string(raw.get("sandbox_profile")),
        transport=transport,
        server_action=server_action,
        server_action_args=server_action_args,
        client_action=client_action,
        client_action_args=client_action_args,
        request_kind=request_kind,
        completion_mode=completion_mode,
        engine_job_id=_optional_string(raw.get("engine_job_id")),
        target_nodes=tuple(target_nodes),
        target_tags=tuple(target_tags),
        target_roles=tuple(target_roles),
        target_endpoint_id=_optional_string(raw.get("target_endpoint_id")),
        target_environment_id=_optional_string(raw.get("target_environment_id")),
        target_gateway_id=_optional_string(raw.get("target_gateway_id")),
        target_transport_mode=_optional_string(raw.get("target_transport_mode")),
        registration_generation=_optional_string(raw.get("registration_generation")),
        experiment_id=_optional_string(raw.get("experiment_id")),
        attempt_id=_optional_string(raw.get("attempt_id")),
        workflow_ingest=bool(raw.get("workflow_ingest", True)),
        fanout=bool(raw.get("fanout", False)),
        dispatch_class=_optional_string(raw.get("dispatch_class")),
        supersession_key=_optional_string(raw.get("supersession_key")),
        request_sequence=max(0, int(raw.get("request_sequence", 0) or 0)),
        not_after=_optional_string(raw.get("not_after")),
    )


def request_status_metadata(request: ExecutionRequest) -> dict[str, Any]:
    return {
        "relay_protocol_version": request.relay_protocol_version,
        "output_subdir": request.output_subdir,
        "request_kind": request.request_kind,
        "completion_mode": request.completion_mode,
        "server_action": request.server_action or "",
        "server_action_args": dict(request.server_action_args),
        "engine_job_id": request.engine_job_id or "",
        "experiment_id": request.experiment_id or "",
        "attempt_id": request.attempt_id or "",
        "workflow_ingest": request.workflow_ingest,
        "target_nodes": list(request.target_nodes),
        "target_endpoint_id": request.target_endpoint_id or "",
        "target_environment_id": request.target_environment_id or "",
        "target_gateway_id": request.target_gateway_id or "",
        "target_transport_mode": request.target_transport_mode or "",
        "registration_generation": request.registration_generation or "",
        "dispatch_class": request.dispatch_class or "",
        "supersession_key": request.supersession_key or "",
        "request_sequence": request.request_sequence,
        "not_after": request.not_after or "",
    }


def request_dispatch_priority(request: ExecutionRequest) -> tuple[int, int, str]:
    dispatch_class = str(request.dispatch_class or "").strip().lower()
    if not dispatch_class:
        request_id = request.request_id.lower()
        if (
            request_id.startswith("engine-runtime-sync-")
            or "restart" in request_id
            or request.client_action in {"engine-runtime-sync", "restart-role"}
            or request.server_action in {"lan-restart-service", "lan-bootstrap"}
        ):
            dispatch_class = "maintenance"
        elif request.request_kind in {"engine-return-ack", "engine-collect"}:
            dispatch_class = "return"
        elif request.request_kind in {
            "engine-admission",
            "engine-exchange",
            "engine-capacity",
            "flow-v3-exchange",
        }:
            dispatch_class = "exchange"
        elif request.request_kind == "engine-snapshot":
            dispatch_class = "watch"
        else:
            dispatch_class = "work"
    rank = {
        "maintenance": 0,
        "return": 10,
        "exchange": 20,
        "work": 30,
        "watch": 40,
    }.get(dispatch_class, 30)
    return rank, request.request_sequence, request.request_id


def request_identity(
    config: AppConfig,
    trigger_ref: str,
    request_file: str | Path | None = None,
) -> tuple[str, str]:
    fallback_id = _normalize_name(f"rejected-{trigger_ref[:12]}")
    try:
        raw = _load_request_file(resolve_request_path(config, request_file))
    except Exception:
        return fallback_id, f"rejected/{fallback_id}"
    request_id = _normalize_name(
        str(raw.get("id") or raw.get("request_id") or fallback_id)
    )
    try:
        output_subdir = _normalize_repo_path(
            str(raw.get("output_subdir") or config.executor.default_output_subdir)
        ).rstrip("/")
    except Exception:
        output_subdir = f"rejected/{request_id}"
    return request_id, output_subdir


def resolve_request_path(
    config: AppConfig,
    request_file: str | Path | None,
) -> Path:
    if request_file is None:
        return config.repo_dir / config.executor.request_file
    raw = Path(request_file)
    path = raw if raw.is_absolute() else config.repo_dir / raw
    resolved = path.resolve()
    repo = config.repo_dir.resolve()
    try:
        relative = resolved.relative_to(repo).as_posix()
    except ValueError as exc:
        raise ValueError(f"request file escapes repository: {resolved}") from exc
    legacy = config.executor.request_file.replace("\\", "/").strip("/")
    if relative != legacy and not is_append_request_path(config, relative):
        raise ValueError(f"unsupported request file path: {relative}")
    return resolved


def _load_request_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ValueError(f"request file must be an object: {path}")
    return raw


def _command_from(raw: dict[str, Any], config: AppConfig) -> list[str]:
    command = raw.get("command")
    if command is not None and config.executor.allow_request_command:
        if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
            raise ValueError("input request command must be a string list")
        return list(command)

    if not config.executor.default_command:
        raise ValueError(
            "no command found; set executor.default_command or provide input/job.json command"
        )
    return list(config.executor.default_command)


def _optional_command_from(raw: dict[str, Any], key: str) -> list[str]:
    command = raw.get(key)
    if command is None:
        return []
    if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
        raise ValueError(f"input request {key} must be a string list")
    return list(command)


def _path_list_from(raw: dict[str, Any], key: str) -> list[str]:
    values = raw.get(key, [])
    if values is None:
        return []
    if not isinstance(values, list) or not all(isinstance(item, str) for item in values):
        raise ValueError(f"input request {key} must be a string list")
    return [_normalize_repo_path(item).rstrip("/") for item in values]


def _env_from(raw: dict[str, Any], config: AppConfig) -> dict[str, str]:
    env = dict(config.executor.env)
    request_env = raw.get("env", {})
    if not isinstance(request_env, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in request_env.items()
    ):
        raise ValueError("input request env must be an object of string values")
    env.update({key: str(value) for key, value in request_env.items()})
    return env


def _string_dict_from(raw: dict[str, Any], key: str) -> dict[str, str]:
    value = raw.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict) or not all(
        isinstance(item_key, str) and isinstance(item_value, str)
        for item_key, item_value in value.items()
    ):
        raise ValueError(f"input request {key} must be an object of string values")
    return {str(item_key): str(item_value) for item_key, item_value in value.items()}


def _string_list_from(raw: dict[str, Any], key: str) -> list[str]:
    value = raw.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"input request {key} must be a string list")
    return [item for item in value if item]


def _path_matches(path: str, prefix: str) -> bool:
    normalized = path.replace("\\", "/").strip("/")
    prefix = prefix.strip("/")
    return normalized == prefix or normalized.startswith(prefix + "/")


def _normalized_repo_path(path: str) -> str:
    return path.replace("\\", "/").strip("/")


def _normalize_repo_path(path: str) -> str:
    normalized = path.replace("\\", "/").lstrip("/")
    if normalized in ("", "."):
        return "."
    if ".." in Path(normalized).parts:
        raise ValueError(f"path cannot contain '..': {path}")
    return normalized


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_transport(value: Any) -> str | None:
    if value in (None, ""):
        return None
    transport = str(value).lower()
    if transport not in {"relay", "direct", "auto"}:
        raise ValueError("input request transport must be one of: relay, direct, auto")
    return transport


def _optional_server_action(value: Any) -> str | None:
    if value in (None, ""):
        return None
    action = str(value).lower()
    if action not in {
        "lan-bootstrap",
        "lan-sync-code",
        "lan-restart-service",
        "lan-reconcile-service",
        "lan-reconcile-request",
        "lan-cancel-request",
        "lan-node-ack",
        "lan-diagnose",
        "lan-inspect-artifact",
        "lan-sync-artifact",
        "endpoint-runtime",
        "server-tmux-command",
    }:
        raise ValueError(
            "input request server_action must be one of: "
            "lan-bootstrap, lan-sync-code, lan-restart-service, lan-reconcile-service, "
            "lan-reconcile-request, "
            "lan-cancel-request, lan-node-ack, "
            "lan-diagnose, lan-inspect-artifact, lan-sync-artifact, endpoint-runtime, "
            "server-tmux-command"
        )
    return action


def _optional_client_action(value: Any) -> str | None:
    if value in (None, ""):
        return None
    action = str(value).lower()
    allowed = {"engine-exchange", "flow-v3-exchange"}
    if action not in allowed:
        raise ValueError(
            "input request client_action must be one of: "
            + ", ".join(sorted(allowed))
        )
    return action


def _request_kind(value: Any) -> str:
    kind = str(value or "command").lower()
    if kind not in {
        "command",
        "control-probe",
        "host-only-canary",
        "engine-host-canary",
        "engine-device-canary",
        "engine-admission",
        "engine-exchange",
        "flow-v3-exchange",
        "engine-snapshot",
        "engine-collect",
        "engine-capacity",
    }:
        raise ValueError(
            "input request request_kind must be one of: command, control-probe, "
            "host-only-canary, engine-host-canary, engine-device-canary, "
            "engine-admission, engine-exchange, flow-v3-exchange, "
            "engine-snapshot, engine-collect, "
            "engine-capacity"
        )
    return kind


def _completion_mode(value: Any) -> str:
    mode = str(value or "terminal").lower()
    if mode not in {"terminal", "accepted", "snapshot"}:
        raise ValueError("input request completion_mode must be one of: terminal, accepted, snapshot")
    return mode


def _normalize_name(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "._-" else "_" for char in value)
    return cleaned.strip("._-") or "request"
