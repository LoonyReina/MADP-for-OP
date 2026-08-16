from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from limited_remote_partner.core.config import AppConfig
from limited_remote_partner.gateway.git_client import GitClient
from limited_remote_partner.core.request import ExecutionRequest
from limited_remote_partner.gateway.input_parser import request_status_metadata
from limited_remote_partner.observability.log_chunker import ChunkedLogWriter
from limited_remote_partner.core.sandbox import build_sandbox_command, sandbox_status


class CommandRunner:
    def __init__(
        self,
        git: GitClient,
        config: AppConfig,
        *,
        status_extra: dict[str, object] | None = None,
    ) -> None:
        self.git = git
        self.config = config
        self.repo_dir = config.repo_dir
        self.status_extra = status_extra or {}

    def run_request(self, request: ExecutionRequest, trigger_ref: str) -> int:
        result_dir = self.repo_dir / self.config.io.output_dir / request.output_subdir
        writer = ChunkedLogWriter(result_dir, request.log_name, self.config.io.max_file_bytes)
        started_at = _utc_now()
        request_metadata = request_status_metadata(request)
        working_dir = _safe_join(self.repo_dir, request.working_dir)
        runtime_env = _build_env(request)
        sandbox = build_sandbox_command(
            self.config,
            request,
            request.sandbox_profile,
            working_dir,
            runtime_env,
        )
        self._write_status(
            result_dir,
            {
                **request_metadata,
                "state": "running",
                "request_id": request.request_id,
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "command": list(request.command),
                "sandbox": sandbox_status(sandbox),
            },
        )
        self._sync(
            result_dir,
            f"limited_remote_partner: {request.request_id} started",
        )
        for warning in sandbox.warnings:
            writer.write_line(f"[limited_remote_partner] sandbox warning: {warning}\n")
        writer.write_line(
            "[limited_remote_partner] sandbox "
            f"profile={sandbox.profile_name} requested={sandbox.requested_backend} "
            f"active={sandbox.active_backend}\n"
        )

        output_queue: queue.Queue[str | None] = queue.Queue()
        process = subprocess.Popen(
            list(sandbox.command),
            cwd=str(working_dir),
            env=runtime_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            **_process_group_kwargs(),
        )
        reader = threading.Thread(
            target=_read_stdout,
            args=(process, output_queue),
            daemon=True,
        )
        reader.start()

        last_sync = time.monotonic()
        deadline = time.monotonic() + request.timeout_seconds if request.timeout_seconds > 0 else None
        exit_code: int | None = None

        try:
            while True:
                try:
                    line = output_queue.get(timeout=0.5)
                except queue.Empty:
                    line = None

                if line is not None:
                    writer.write_line(line)

                now = time.monotonic()
                if deadline and now > deadline and process.poll() is None:
                    _terminate_process_group(process)
                    exit_code = -1
                    writer.write_line("\n[limited_remote_partner] timeout reached\n")

                if now - last_sync >= request.sync_interval_seconds:
                    self._write_status(
                        result_dir,
                        {
                            **request_metadata,
                            "state": "running",
                            "request_id": request.request_id,
                            "trigger_ref": trigger_ref,
                            "started_at": started_at,
                            "heartbeat_at": _utc_now(),
                            "log_parts": [path.name for path in writer.written_paths()],
                            "sandbox": sandbox_status(sandbox),
                        },
                    )
                    self._sync(
                        result_dir,
                        f"limited_remote_partner: {request.request_id} heartbeat",
                    )
                    last_sync = now

                if process.poll() is not None and output_queue.empty():
                    break

            if exit_code is None:
                exit_code = process.wait(timeout=5)
        finally:
            if process.poll() is None:
                _kill_process_group(process)
            reader.join(timeout=1)

        finished_at = _utc_now()
        self._write_status(
            result_dir,
            {
                **request_metadata,
                "state": "success" if exit_code == 0 else "failed",
                "request_id": request.request_id,
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "finished_at": finished_at,
                "exit_code": exit_code,
                "log_parts": [path.name for path in writer.written_paths()],
                "sandbox": sandbox_status(sandbox),
            },
        )
        self._sync(
            result_dir,
            f"limited_remote_partner: {request.request_id} finished",
        )
        return exit_code

    def _sync(self, result_dir: Path, message: str) -> None:
        self.git.commit_and_push(
            [result_dir.resolve().relative_to(self.repo_dir.resolve()).as_posix()],
            message,
            self.config.io.max_file_bytes,
        )

    def _write_status(self, result_dir: Path, status: dict[str, object]) -> None:
        result_dir.mkdir(parents=True, exist_ok=True)
        status_path = result_dir / "status.json"
        payload_data = {**self.status_extra, **status}
        payload = json.dumps(payload_data, ensure_ascii=False, indent=2).encode("utf-8")
        if len(payload) > self.config.io.max_file_bytes:
            raise ValueError("status.json exceeds configured max_file_bytes")
        status_path.write_bytes(payload)


def _read_stdout(
    process: subprocess.Popen[str],
    output_queue: queue.Queue[str | None],
) -> None:
    assert process.stdout is not None
    try:
        for line in process.stdout:
            output_queue.put(line)
    finally:
        process.stdout.close()
        output_queue.put(None)


def _build_env(request: ExecutionRequest) -> dict[str, str]:
    env = os.environ.copy()
    env.update(request.env)
    return env


def _process_group_kwargs() -> dict[str, object]:
    if os.name == "nt":
        return {}
    return {"start_new_session": True}


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    if os.name == "nt":
        process.terminate()
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return


def _kill_process_group(process: subprocess.Popen[str]) -> None:
    if os.name == "nt":
        process.kill()
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def _safe_join(root: Path, repo_path: str) -> Path:
    target = (root / repo_path).resolve()
    root = root.resolve()
    if target != root and root not in target.parents:
        raise ValueError(f"path escapes repo root: {repo_path}")
    return target


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
