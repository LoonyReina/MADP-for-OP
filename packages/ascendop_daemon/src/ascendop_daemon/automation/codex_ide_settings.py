from __future__ import annotations

import hashlib
import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path

from ascendop_daemon.core.models import DaemonConfig


ADAPTER_ID = "codex-ide-task-adapter"
DEFAULT_ADAPTER_GENERATION = "codex-ide-task-adapter-v1"
DEFAULT_MANAGER_RUNNER_ID = "codex-ide-app-relay"
TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)


class CodexIdeAdapterError(RuntimeError):
    pass


@dataclass(frozen=True)
class CodexIdeAdapterSettings:
    enabled: bool
    pool_id: str
    adapter_generation: str
    manager_runner_id: str
    lease_seconds: int

    @classmethod
    def from_config(cls, config: DaemonConfig) -> "CodexIdeAdapterSettings":
        execution = config.agent_execution
        production = (
            execution.get("production", {}) if isinstance(execution, dict) else {}
        )
        if not isinstance(production, dict):
            raise CodexIdeAdapterError("agent_execution.production must be an object")
        production_enabled = bool(production.get("enabled", False))
        adapter = str(production.get("adapter") or "")
        if production_enabled and not adapter:
            raise CodexIdeAdapterError("production Agent adapter is required")
        enabled = production_enabled and adapter == "codex-ide-task"
        pool_id = str(production.get("pool_id") or "").strip()
        if enabled and not pool_id:
            raise CodexIdeAdapterError("production Agent pool_id is required")
        generation = str(
            production.get("adapter_generation") or DEFAULT_ADAPTER_GENERATION
        ).strip()
        manager = str(
            production.get("manager_runner_id") or DEFAULT_MANAGER_RUNNER_ID
        ).strip()
        lease_seconds = int(production.get("lease_seconds", 120))
        if enabled and not 30 <= lease_seconds <= 3600:
            raise CodexIdeAdapterError("Codex IDE lease_seconds must be in [30, 3600]")
        for value, field in (
            (generation, "adapter_generation"),
            (manager, "manager_runner_id"),
        ):
            if enabled and (not value or any(ch not in TOKEN_CHARS for ch in value)):
                raise CodexIdeAdapterError(f"{field} must be a safe token")
        return cls(enabled, pool_id, generation, manager, lease_seconds)


def adapter_component_digest() -> str:
    digest = hashlib.sha256()
    directory = Path(__file__).resolve().parent
    for name in (
        "agent_context.py",
        "agent_outputs.py",
        "agent_workspace.py",
        "candidate_proposals.py",
        "codex_ide_adapter.py",
        "codex_ide_preflight.py",
        "codex_ide_settings.py",
    ):
        path = directory / name
        relative = name.encode("ascii")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def agent_id(operator_id: str, role: str) -> str:
    digest = hashlib.sha256(f"{operator_id}:{role}".encode("utf-8")).hexdigest()[:20]
    return f"codex-ide.{role}.{digest}"


def identity_digest(value: dict[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def boot_id() -> str:
    value = f"{socket.gethostname()}:{os.getpid()}:{Path(__file__).stat().st_ctime_ns}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
