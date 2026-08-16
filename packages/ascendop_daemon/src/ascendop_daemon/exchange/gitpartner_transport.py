from __future__ import annotations

import atexit
import hashlib
import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from ascendop_daemon.control_plane.control_database import ControlDatabase, ControlDatabaseError
from ascendop_daemon.runtime.process_adapter import process_creation_flags, process_startupinfo
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.registry.system_registry import BackendEndpoint, SystemRegistry
from ascendop_daemon.workflow.workspace_result_collector import WorkspaceResultCollector
from ascendop_daemon.exchange.runtime_source import (
    apply_transport_runtime_environment,
    load_active_transport_runtime,
)


TERMINAL_GP_STATES = {"success", "failed", "claim_failed", "return_failed"}


from ascendop_daemon.exchange.transport_contracts import (
    DeliveryObservation,
    QueryObservation,
    deterministic_receipt_id,
    endpoint_supports,
    parse_last_json_object,
    process_error,
    status_transport_identity,
    target_args,
    transport_identity,
    transport_identity_mismatch,
)

class GitPartnerCanaryTransport:
    def __init__(
        self,
        root: Path,
        endpoint: BackendEndpoint,
        *,
        remote: str = "origin",
        command_timeout_seconds: int = 120,
        git_operation_lock_timeout_seconds: int = 60,
    ) -> None:
        self.root = root.resolve()
        self.endpoint = endpoint
        self.transport_runtime = load_active_transport_runtime(self.root)
        repo = Path(endpoint.gitpartner_repo)
        self.repo = (
            repo.resolve()
            if repo.is_absolute()
            else (self.root / repo).resolve()
        )
        result_repo = Path(endpoint.result_worktree)
        self.result_repo = (
            (
                result_repo.resolve()
                if result_repo.is_absolute()
                else (self.root / result_repo).resolve()
            )
            if endpoint.result_worktree
            else self.repo
        )
        self.remote = remote
        self.command_timeout_seconds = max(5, int(command_timeout_seconds))
        self.git_operation_lock_timeout_seconds = max(
            5,
            int(git_operation_lock_timeout_seconds),
        )
        self._result_query_process: subprocess.Popen[str] | None = None
        self._result_query_responses: queue.Queue[str | None] | None = None
        self._result_query_reader: threading.Thread | None = None
        self._result_query_lock = threading.Lock()
        atexit.register(self.close)

    def publish(self, payload: dict[str, Any]) -> DeliveryObservation:
        if not (self.repo / ".git").exists():
            return DeliveryObservation(
                status="retry",
                error=f"endpoint GP worktree is missing: {self.repo}",
                retry_after_seconds=5,
            )
        command = self._publish_command(
            payload,
            commit_push=True,
            append_request=endpoint_supports(
                self.endpoint,
                "gp-append-request-v1",
            ),
        )
        try:
            completed = self._run(command, lane="ingress", repo=self.repo)
        except subprocess.TimeoutExpired as exc:
            return DeliveryObservation(
                status="uncertain",
                error=f"GP publish timed out after {exc.timeout}s",
            )
        return self._delivery_observation(completed)

    def publish_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[DeliveryObservation]:
        if not payloads:
            return []
        if not endpoint_supports(self.endpoint, "gp-append-request-v1"):
            return [self.publish(payload) for payload in payloads]
        if not (self.repo / ".git").exists():
            return [
                DeliveryObservation(
                    status="retry",
                    error=f"endpoint GP worktree is missing: {self.repo}",
                    retry_after_seconds=5,
                )
                for _payload in payloads
            ]
        request_ids = [str(payload["request_id"]) for payload in payloads]
        commands = [
            self._publish_command(
                payload,
                commit_push=index == len(payloads) - 1,
                append_request=True,
                also_commit_request_ids=(
                    request_ids[:-1] if index == len(payloads) - 1 else []
                ),
            )
            for index, payload in enumerate(payloads)
        ]
        batch_root = (
            self.root
            / "TestUtils"
            / "tester_daemon"
            / "transport_batches"
        )
        batch_root.mkdir(parents=True, exist_ok=True)
        batch_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                prefix="submit_",
                dir=batch_root,
                delete=False,
            ) as handle:
                batch_path = Path(handle.name)
                json.dump(
                    {
                        "schema": "gitpartner.submit-batch.v1",
                        "commands": [command[3:] for command in commands],
                    },
                    handle,
                    ensure_ascii=True,
                )
            batch_module = (
                self.transport_runtime.source
                / "limited_remote_partner"
                / "gateway"
                / "submit_job_batch.py"
            )
            if batch_module.is_file():
                completed_items = [
                    self._run(
                        [
                            sys.executable,
                            "-m",
                            "limited_remote_partner.gateway.submit_job_batch",
                            "--commands-file",
                            str(batch_path),
                        ],
                        lane="ingress",
                        repo=self.repo,
                    )
                ]
            else:
                completed_items = self._run_batch(
                    commands,
                    lane="ingress",
                    repo=self.repo,
                )
        except subprocess.TimeoutExpired as exc:
            return [
                DeliveryObservation(
                    status="uncertain",
                    error=f"GP batch publish timed out after {exc.timeout}s",
                )
                for _payload in payloads
            ]
        finally:
            if batch_path is not None:
                batch_path.unlink(missing_ok=True)
        failed = next(
            (
                completed
                for completed in completed_items
                if completed.returncode != 0
            ),
            None,
        )
        if failed is not None:
            observation = self._delivery_observation(failed)
            return [observation for _payload in payloads]
        return [
            DeliveryObservation(
                status="uncertain",
                error=(
                    "batch published; awaiting endpoint-visible acceptance"
                ),
            )
            for _payload in payloads
        ]

    def _publish_command(
        self,
        payload: dict[str, Any],
        *,
        commit_push: bool,
        append_request: bool,
        also_commit_request_ids: list[str] | None = None,
    ) -> list[str]:
        manifest = self._manifest(payload)
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
        ]
        if commit_push:
            command.append("--commit-push")
        if append_request:
            command.append("--append-request")
        for request_id in also_commit_request_ids or []:
            command.extend(["--also-commit-request-id", request_id])
        command.extend(
            [
            "ascendop-distributed-canary",
            "--transport",
            self.endpoint.transport,
            "--request-id",
            str(payload["request_id"]),
            "--client-work-dir",
            self.endpoint.remote_root,
            "--task-class",
            str(manifest["task_class"]),
            "--experiment-id",
            str(manifest["experiment_id"]),
            "--attempt-id",
            str(payload["attempt_id"]),
            "--synthetic-duration-ms",
            str(int(manifest.get("synthetic_duration_ms", 0))),
            "--host-duration-ms",
            str(int(manifest.get("host_duration_ms", 0))),
            "--device-duration-ms",
            str(int(manifest.get("device_duration_ms", 0))),
            "--export-duration-ms",
            str(int(manifest.get("export_duration_ms", 0))),
            "--failure-mode",
            str(manifest.get("failure_mode") or "none"),
            *target_args(payload),
            ]
        )
        payload_file = str(manifest.get("payload_file") or "")
        if payload_file:
            command.extend(["--payload-file", payload_file])
        if str(manifest["task_class"]).startswith("engine-"):
            command.extend(["--engine-root", self.endpoint.engine_root])
        return command

    @staticmethod
    def _delivery_observation(
        completed: subprocess.CompletedProcess[str],
    ) -> DeliveryObservation:
        if completed.returncode == 0:
            return DeliveryObservation(
                status="uncertain",
                error="published; awaiting endpoint-visible acceptance",
            )
        error = process_error(completed)
        if any(
            token in error.lower()
            for token in (
                "push failed",
                "connection reset",
                "timed out",
                "timeout",
                "remote end hung up",
            )
        ):
            return DeliveryObservation(status="uncertain", error=error)
        return DeliveryObservation(
            status="retry",
            error=error,
            retry_after_seconds=5,
        )

    def query(self, payload: dict[str, Any]) -> QueryObservation:
        return self.query_batch([payload])[0]

    def query_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[QueryObservation]:
        if not payloads:
            return []
        if not (self.result_repo / ".git").exists():
            error = (
                "endpoint GP result worktree is missing: "
                f"{self.result_repo}"
            )
            return [QueryObservation(error=error) for _payload in payloads]
        output_subdirs = [
            f"distributed-canary/{payload['request_id']}"
            for payload in payloads
        ]
        result_templates = [
            (
                "client_output/client_output/canary_results/"
                "{request_id}/result.json"
            ),
            (
                "client_output/canary_results/"
                "{request_id}/result.json"
            ),
            "canary_results/{request_id}/result.json",
        ]
        query_started = time.monotonic()
        try:
            observed = self._run_result_query(
                output_subdirs,
                result_templates,
            )
        except subprocess.TimeoutExpired as exc:
            error = f"GP batch result query timed out after {exc.timeout}s"
            return [QueryObservation(error=error) for _payload in payloads]
        timing = observed.get("timing")
        if isinstance(timing, dict):
            timing = {
                **timing,
                "subprocess_wall_seconds": round(
                    time.monotonic() - query_started,
                    6,
                ),
            }
        diagnostics = (
            {
                "result_query_timing": dict(timing),
                "result_commit_created_at": str(
                    observed.get("commit_created_at") or ""
                ),
            }
            if isinstance(timing, dict)
            else {}
        )
        raw_items = observed.get("items")
        if not isinstance(raw_items, list):
            error = "GP batch result query returned no items"
            return [QueryObservation(error=error) for _payload in payloads]
        by_request = {
            str(item.get("request_id") or ""): item
            for item in raw_items
            if isinstance(item, dict)
        }
        observations: list[QueryObservation] = []
        for index, payload in enumerate(payloads):
            request_id = str(payload["request_id"])
            item = by_request.get(request_id)
            if item is None:
                observations.append(
                    QueryObservation(
                        error=(
                            "GP batch result query omitted request "
                            f"{request_id}"
                        )
                    )
                )
                continue
            status = item.get("status")
            result = item.get("result")
            observations.append(
                self._query_observation(
                    payload,
                    status if isinstance(status, dict) else None,
                    result if isinstance(result, dict) else None,
                    diagnostics=diagnostics if index == 0 else {},
                )
            )
        return observations

    def _run_result_query(
        self,
        output_subdirs: list[str],
        result_templates: list[str],
    ) -> dict[str, Any]:
        request = {
            "output_subdirs": output_subdirs,
            "result_templates": result_templates,
            "wait_seconds": 0.0,
            "poll_seconds": 0.05,
        }
        with NamedProcessLock(
            self.root,
            self._lane_lock_name("result"),
            stale_after_seconds=120,
            wait_timeout_seconds=300,
        ):
            with self._result_query_lock:
                last_error = ""
                for attempt in range(2):
                    process, responses = self._ensure_result_query_process()
                    try:
                        assert process.stdin is not None
                        process.stdin.write(
                            json.dumps(
                                request,
                                ensure_ascii=True,
                                sort_keys=True,
                            )
                            + "\n"
                        )
                        process.stdin.flush()
                    except (BrokenPipeError, OSError, ValueError) as exc:
                        last_error = f"query service write failed: {exc}"
                        self._stop_result_query_process(process)
                        continue

                    deadline = (
                        time.monotonic() + self.command_timeout_seconds
                    )
                    while time.monotonic() < deadline:
                        try:
                            line = responses.get(
                                timeout=max(
                                    0.01,
                                    deadline - time.monotonic(),
                                )
                            )
                        except queue.Empty:
                            last_error = "query service response timed out"
                            break
                        if line is None:
                            last_error = "query service exited before response"
                            break
                        try:
                            response = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(response, dict) or "ok" not in response:
                            continue
                        if not bool(response.get("ok")):
                            raise RuntimeError(
                                str(response.get("error") or "query failed")
                            )
                        observed = response.get("observed")
                        if not isinstance(observed, dict):
                            raise RuntimeError(
                                "query service returned no observation"
                            )
                        return observed
                    self._stop_result_query_process(process)
                    if attempt == 0:
                        continue
                raise subprocess.TimeoutExpired(
                    "persistent batch result query",
                    self.command_timeout_seconds,
                    stderr=last_error,
                )

    def _ensure_result_query_process(
        self,
    ) -> tuple[subprocess.Popen[str], queue.Queue[str | None]]:
        process = self._result_query_process
        responses = self._result_query_responses
        if (
            process is not None
            and process.poll() is None
            and responses is not None
        ):
            return process, responses
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.batch_result_query",
            "--serve-jsonl",
            "--repo",
            str(self.result_repo),
            "--result-branch",
            self.endpoint.result_channel,
            "--control-branch",
            self.endpoint.control_channel,
            "--remote",
            self.remote,
        ]
        responses = queue.Queue()
        process = subprocess.Popen(
            command,
            cwd=self.result_repo,
            env=self._command_env(self.result_repo),
            text=True,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=1,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )

        def read_responses() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                responses.put(line)
            responses.put(None)

        reader = threading.Thread(
            target=read_responses,
            name=f"gp-result-query-{self.endpoint.endpoint_id}",
            daemon=True,
        )
        reader.start()
        self._result_query_process = process
        self._result_query_responses = responses
        self._result_query_reader = reader
        return process, responses

    def _stop_result_query_process(
        self,
        process: subprocess.Popen[str] | None = None,
    ) -> None:
        current = self._result_query_process
        if process is not None and current is not process:
            return
        self._result_query_process = None
        self._result_query_responses = None
        self._result_query_reader = None
        if current is None or current.poll() is not None:
            return
        try:
            if current.stdin is not None:
                current.stdin.close()
            current.wait(timeout=1)
        except (OSError, subprocess.TimeoutExpired):
            current.terminate()
            try:
                current.wait(timeout=1)
            except subprocess.TimeoutExpired:
                current.kill()
                current.wait(timeout=1)

    def close(self) -> None:
        with self._result_query_lock:
            self._stop_result_query_process()

    @staticmethod
    def _query_observation(
        payload: dict[str, Any],
        status: dict[str, Any] | None,
        result: dict[str, Any] | None,
        *,
        diagnostics: dict[str, Any] | None = None,
    ) -> QueryObservation:
        diagnostics = diagnostics or {}
        if status is None:
            return QueryObservation(diagnostics=diagnostics)
        request_id = str(payload["request_id"])
        identity = transport_identity(payload)
        acceptance = None
        state = str(status.get("state") or "")
        if state:
            observed_identity = status_transport_identity(status)
            mismatch = transport_identity_mismatch(identity, observed_identity)
            if mismatch:
                return QueryObservation(
                    error=(
                        "endpoint-visible acceptance identity mismatch for "
                        + mismatch
                    ),
                    diagnostics=diagnostics,
                )
            acceptance = {
                **observed_identity,
                "acceptance_id": str(
                    status.get("trigger_ref")
                    or status.get("dispatched_at")
                    or request_id
                ),
                "remote_state": state,
            }
        if result is not None:
            result = {**identity, **result}
            if state in TERMINAL_GP_STATES and state != "success":
                result["outcome"] = "failed"
                result["error"] = str(
                    status.get("error")
                    or f"GitPartner terminal state {state}"
                )
        elif state in TERMINAL_GP_STATES:
            result = {
                **identity,
                "schema": "gitpartner.distributed-canary-result.v1",
                "receipt_id": deterministic_receipt_id(payload),
                "outcome": "failed" if state != "success" else "success",
                "error": str(
                    status.get("error")
                    or (
                        "terminal result artifact missing"
                        if state == "success"
                        else f"GitPartner terminal state {state}"
                    )
                ),
                "workflow_ingest": False,
            }
        return QueryObservation(
            acceptance=acceptance,
            result=result,
            diagnostics=diagnostics,
        )

    def acknowledge(
        self, payload: dict[str, Any], returned: dict[str, Any]
    ) -> bool:
        return str(returned.get("receipt_id") or "") == deterministic_receipt_id(
            payload
        )

    def _manifest(self, payload: dict[str, Any]) -> dict[str, Any]:
        path = Path(str(payload.get("manifest_path") or "")).resolve()
        if path != self.root and self.root not in path.parents:
            raise ValueError(f"experiment manifest escapes workspace: {path}")
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise ValueError("experiment manifest must be an object")
        if bool(raw.get("workflow_ingest", True)):
            raise ValueError("distributed canary cannot ingest workflow")
        if str(raw.get("task_class") or "") not in {
            "control-probe",
            "host-only-canary",
            "engine-host-canary",
            "engine-device-canary",
        }:
            raise ValueError("unsupported distributed canary task_class")
        return raw

    def _read_result(self, request_id: str) -> dict[str, Any] | None:
        root = (
            self.result_repo
            / "output"
            / "distributed-canary"
            / request_id
        )
        candidates = (
            root
            / "client_output"
            / "client_output"
            / "canary_results"
            / request_id
            / "result.json",
            root
            / "client_output"
            / "canary_results"
            / request_id
            / "result.json",
            root / "canary_results" / request_id / "result.json",
        )
        for path in candidates:
            if path.is_file():
                raw = json.loads(path.read_text(encoding="utf-8-sig"))
                if isinstance(raw, dict):
                    return raw
        return None

    def _run(
        self,
        command: list[str],
        *,
        lane: str,
        repo: Path,
    ) -> subprocess.CompletedProcess[str]:
        with NamedProcessLock(
            self.root,
            self._lane_lock_name(lane),
            stale_after_seconds=120,
            wait_timeout_seconds=300,
        ):
            return self._run_unlocked(command, repo=repo)

    def _run_batch(
        self,
        commands: list[list[str]],
        *,
        lane: str,
        repo: Path,
    ) -> list[subprocess.CompletedProcess[str]]:
        with NamedProcessLock(
            self.root,
            self._lane_lock_name(lane),
            stale_after_seconds=120,
            wait_timeout_seconds=300,
        ):
            return [
                self._run_unlocked(command, repo=repo)
                for command in commands
            ]

    def _run_unlocked(
        self,
        command: list[str],
        *,
        repo: Path,
    ) -> subprocess.CompletedProcess[str]:
        env = self._command_env(repo)
        return subprocess.run(
            command,
            cwd=repo,
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=self.command_timeout_seconds,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )

    def _command_env(self, repo: Path) -> dict[str, str]:
        env = os.environ.copy()
        env["GITPARTNER_BRANCH"] = self.endpoint.control_channel
        env["GITPARTNER_RESULT_BRANCH"] = self.endpoint.result_channel
        env["GITPARTNER_REMOTE"] = self.remote
        env["GITPARTNER_ENDPOINT_ID"] = self.endpoint.endpoint_id
        env["GITPARTNER_GIT_TIMEOUT_SECONDS"] = str(
            self.command_timeout_seconds
        )
        env["GITPARTNER_GIT_OPERATION_LOCK_TIMEOUT_SECONDS"] = str(
            self.git_operation_lock_timeout_seconds
        )
        token_file = self.repo / "api.txt"
        if token_file.is_file():
            env["GITPARTNER_TOKEN_FILE"] = str(token_file)
        apply_transport_runtime_environment(env, self.transport_runtime)
        return env

    def _lane_lock_name(self, lane: str) -> str:
        suffix = (
            f"_{lane}"
            if endpoint_supports(
                self.endpoint,
                "gp-duplex-lanes-v1",
            )
            else ""
        )
        return (
            f"gitpartner_endpoint_{self.endpoint.endpoint_id}{suffix}"
        )
