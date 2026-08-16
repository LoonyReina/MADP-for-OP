from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ascendop_daemon.registry.system_registry import BackendEndpoint
from ascendop_daemon.runtime.process_adapter import process_creation_flags, process_startupinfo
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.exchange.runtime_source import (
    apply_transport_runtime_environment,
    load_active_transport_runtime,
)


class NodeControlTransportError(RuntimeError):
    pass


class NodeControlTransport:
    """Narrow Wire V3 transport for node admission and trusted snapshots.

    It deliberately has no Engine job/admission API. Workflow execution goes
    through the transactional outbox; this adapter only maintains endpoint
    identity and liveness during registration.
    """

    def __init__(
        self,
        root: Path,
        endpoint: BackendEndpoint,
        *,
        git_operation_timeout_seconds: int = 60,
        git_operation_lock_timeout_seconds: int = 60,
    ) -> None:
        self.root = root.resolve()
        self.endpoint = endpoint
        self.transport_runtime = load_active_transport_runtime(self.root)
        repo = Path(endpoint.gitpartner_repo)
        self.repo = repo.resolve() if repo.is_absolute() else (self.root / repo).resolve()
        self.git_operation_timeout_seconds = max(
            15,
            min(120, int(git_operation_timeout_seconds)),
        )
        self.git_operation_lock_timeout_seconds = max(
            5,
            int(git_operation_lock_timeout_seconds),
        )

    def snapshot(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        command = self._base_command(request_id, wait_timeout_seconds)
        command.extend(
            [
                "ascendop-engine-snapshot",
                "--transport",
                self.endpoint.transport,
                "--request-id",
                request_id,
                "--client-work-dir",
                self.endpoint.remote_root,
                "--engine-root",
                self.endpoint.engine_root,
                *self._target_args(),
            ]
        )
        elapsed = self._run(command, wait_timeout_seconds)
        output = self.repo / "output" / "engine-demo" / request_id
        status = _read_object(output / "status.json")
        snapshot = _read_object(output / "engine_status.json")
        if status.get("state") != "success" or not snapshot:
            raise NodeControlTransportError(
                f"trusted node snapshot did not return success: {request_id}"
            )
        return {
            "request_id": request_id,
            "state": "success",
            "engine_snapshot": snapshot,
            "transport_elapsed_seconds": elapsed,
        }

    def acknowledge_node(
        self,
        *,
        request_id: str,
        ack_path: Path,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        resolved_ack = ack_path.resolve()
        if not resolved_ack.is_file():
            raise NodeControlTransportError(
                f"node acknowledgement does not exist: {resolved_ack}"
            )
        command = self._base_command(request_id, wait_timeout_seconds)
        command.extend(
            [
                "lan-bootstrap",
                "--request-id",
                request_id,
                "--action",
                "lan-node-ack",
                "--target-role",
                "client",
                "--remote-config",
                f"configs/runtime/{self.endpoint.node_id}.json",
                "--node-ack-file",
                str(resolved_ack),
                *self._target_args(),
            ]
        )
        elapsed = self._run(command, wait_timeout_seconds)
        status = _read_object(self.repo / "output" / request_id / "status.json")
        sandbox = status.get("sandbox")
        trusted = status.get("server_action") == "lan-node-ack" or (
            isinstance(sandbox, dict)
            and sandbox.get("profile") == "local-node-ack"
        )
        if status.get("state") != "success" or not trusted:
            raise NodeControlTransportError(
                f"node acknowledgement did not return trusted success: {request_id}"
            )
        return {
            "request_id": request_id,
            "state": "success",
            "server_action": "lan-node-ack",
            "status": status,
            "transport_elapsed_seconds": elapsed,
        }

    def _base_command(self, request_id: str, wait_seconds: int) -> list[str]:
        return [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(max(1, int(wait_seconds))),
        ]

    def _target_args(self) -> list[str]:
        values = [
            "--target-node",
            self.endpoint.node_id,
            "--target-endpoint-id",
            self.endpoint.endpoint_id,
            "--target-environment-id",
            self.endpoint.execution_environment_id,
            "--target-transport-mode",
            self.endpoint.transport_mode,
            "--registration-generation",
            self.endpoint.generation,
        ]
        if self.endpoint.gateway_id:
            values.extend(["--target-gateway-id", self.endpoint.gateway_id])
        return values

    def _run(self, command: list[str], wait_seconds: int) -> float:
        if not (self.repo / ".git").exists():
            raise NodeControlTransportError(
                f"endpoint GP worktree is missing: {self.repo}"
            )
        env = os.environ.copy()
        env["GITPARTNER_BRANCH"] = self.endpoint.control_channel
        env["GITPARTNER_RESULT_BRANCH"] = self.endpoint.result_channel
        env["GITPARTNER_ENDPOINT_ID"] = self.endpoint.endpoint_id
        env["GITPARTNER_GIT_TIMEOUT_SECONDS"] = str(
            self.git_operation_timeout_seconds
        )
        env["GITPARTNER_GIT_OPERATION_LOCK_TIMEOUT_SECONDS"] = str(
            self.git_operation_lock_timeout_seconds
        )
        apply_transport_runtime_environment(env, self.transport_runtime)
        started = time.monotonic()
        with NamedProcessLock(
            self.root,
            f"gitpartner_node_control_{self.endpoint.endpoint_id}",
            stale_after_seconds=max(120, int(wait_seconds) + 30),
            wait_timeout_seconds=30,
        ):
            completed = subprocess.run(
                command,
                cwd=self.repo,
                env=env,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=max(30, int(wait_seconds) + 2 * self.git_operation_timeout_seconds),
                creationflags=process_creation_flags(),
                startupinfo=process_startupinfo(),
            )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()[-4000:]
            raise NodeControlTransportError(
                f"node control transport failed ({completed.returncode}): {detail}"
            )
        return round(time.monotonic() - started, 3)


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}
