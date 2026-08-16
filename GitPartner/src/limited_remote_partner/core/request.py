from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExecutionRequest:
    request_id: str
    command: tuple[str, ...]
    working_dir: str
    output_subdir: str
    env: dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 0
    sync_interval_seconds: int = 30
    log_name: str = "job.log"
    payload_paths: tuple[str, ...] = field(default_factory=tuple)
    client_command: tuple[str, ...] = field(default_factory=tuple)
    return_paths: tuple[str, ...] = field(default_factory=tuple)
    payload_root: str | None = None
    client_work_dir: str | None = None
    sandbox_profile: str | None = None
    transport: str | None = None
    server_action: str | None = None
    server_action_args: dict[str, str] = field(default_factory=dict)
    client_action: str | None = None
    client_action_args: dict[str, str] = field(default_factory=dict)
    request_kind: str = "command"
    completion_mode: str = "terminal"
    engine_job_id: str | None = None
    target_nodes: tuple[str, ...] = field(default_factory=tuple)
    target_tags: tuple[str, ...] = field(default_factory=tuple)
    target_roles: tuple[str, ...] = field(default_factory=tuple)
    target_endpoint_id: str | None = None
    target_environment_id: str | None = None
    target_gateway_id: str | None = None
    target_transport_mode: str | None = None
    registration_generation: str | None = None
    experiment_id: str | None = None
    attempt_id: str | None = None
    workflow_ingest: bool = True
    fanout: bool = False
    dispatch_class: str | None = None
    supersession_key: str | None = None
    request_sequence: int = 0
    not_after: str | None = None
    relay_protocol_version: str = "relay-v2"
