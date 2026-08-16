from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import shlex
import signal
import socket
import subprocess
import threading
import time
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from limited_remote_partner.maintenance.auto_update import (
    LOCAL_WATCH_TRIGGER_REF,
    PendingReexec,
    consume_pending_reexec,
    reexec_partner_role,
    should_reexec_for_update,
    watch_signature,
    watch_signature_changed,
)
from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.core.request import ExecutionRequest
from limited_remote_partner.gateway.compact_receipt import write_compact_receipt
from limited_remote_partner.gateway.exchange_sync import push_exchange_if_changed
from limited_remote_partner.gateway.git_client import GitClient, GitError, configure_direct_git_fast_fail
from limited_remote_partner.gateway.input_parser import (
    changed_request_paths,
    parse_request,
    request_dispatch_priority,
    request_status_metadata,
)
from limited_remote_partner.gateway.local_sync import sync_once
from limited_remote_partner.maintenance.lan_ops import apply_local_node_ack, schedule_local_restart_service
from limited_remote_partner.observability.log_chunker import ChunkedLogWriter
from limited_remote_partner.core.loop_backoff import LoopErrorBackoff
from limited_remote_partner.gateway.relay import (
    RELAY_PROTOCOL_VERSION,
    RelayError,
    ScpTransport,
    copy_requested_paths,
    effective_transport,
    read_json,
    read_request,
    safe_join,
    utc_now,
    write_json,
)
from limited_remote_partner.core.resident_lock import ResidentRoleLock
from limited_remote_partner.core.process_utils import process_start_token
from limited_remote_partner.core.sandbox import build_sandbox_command, sandbox_status
from limited_remote_partner.engine.scheduler import should_execute
from limited_remote_partner.core.targeting import route_request_for_current_runtime


LEGACY_INBOX_SCAN_BUDGET = 64


class RelayClient:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.config = load_config(config_path)
        _configure_auto_direct_fast_fail(self.config)
        self.transport = ScpTransport(self.config)
        self._parallel_executor = ThreadPoolExecutor(
            max_workers=self.config.relay.max_parallel_requests,
            thread_name_prefix="gp-engine-relay",
        )
        self.direct = (
            DirectClient(
                config_path,
                self.config,
                executor=self._parallel_executor,
            )
            if self.config.relay.transport_mode in {"direct", "auto"}
            else None
        )
        self.exchange_git = None if self.direct else GitClient(self.config.repo, self.config.repo_dir)
        self._exchange_initialized = False
        self._legacy_scan_iter: Iterator[os.DirEntry[str]] | None = None
        self._parallel_lock = threading.Lock()
        self._parallel_futures: dict[str, Future[int]] = {}
        self._return_batch_condition = threading.Condition()
        self._return_batches: dict[str, dict[str, object]] = {}

    def run_once(self, request_id: str) -> int:
        task_dir = self._inbox_dir() / request_id
        if not task_dir.exists():
            raise RelayError(f"missing relay task: {task_dir}")
        bundle_dir = self._bundle_dir(task_dir, request_id)
        try:
            request = read_request(bundle_dir / "request.json")
        except Exception as exc:
            return self._reject_relay_task(task_dir, request_id, exc)
        work_root = self._work_root(request.client_work_dir)
        result_dir = task_dir / "result"
        result_dir.mkdir(parents=True, exist_ok=True)

        started_at = utc_now()
        self._write_status(
            result_dir,
            request,
            {
                "state": "received",
                "started_at": started_at,
                "work_root": str(work_root),
                "command": list(request.client_command or request.command),
            },
        )

        copied_payload: list[str] = []
        copied_return: list[str] = []
        missing_return: list[str] = []
        exit_code = 1
        error: str | None = None
        sandbox_payload: dict[str, object] = {}
        cancel_payload: dict[str, str] = {}
        try:
            self._write_status(
                result_dir,
                request,
                {
                    "state": "copying-payload",
                    "started_at": started_at,
                    "work_root": str(work_root),
                },
            )
            copied_payload, _missing_payload = copy_requested_paths(
                bundle_dir / "payload",
                work_root,
                request.payload_paths,
                self.config.io.max_file_bytes,
            )
            self._write_status(
                result_dir,
                request,
                {
                    "state": "running",
                    "started_at": started_at,
                    "work_root": str(work_root),
                    "payload_paths": copied_payload,
                },
            )
            exit_code, sandbox_payload = self._execute(
                request,
                work_root,
                result_dir,
                task_dir,
                started_at,
            )
            cancel_payload = _existing_cancel_reason(result_dir)
            self._write_status(
                result_dir,
                request,
                {
                    "state": "collecting-return",
                    "started_at": started_at,
                    "work_root": str(work_root),
                    "exit_code": exit_code,
                    "payload_paths": copied_payload,
                    "sandbox": sandbox_payload,
                    **cancel_payload,
                },
            )
            copied_return, missing_return = copy_requested_paths(
                work_root,
                result_dir / "client_output",
                request.return_paths,
                self.config.io.max_file_bytes,
                missing_ok=True,
            )
        except Exception as exc:
            error = str(exc)
            exit_code = 1

        finished_at = utc_now()
        final_status = {
            "state": _final_state(exit_code, error),
            "started_at": started_at,
            "finished_at": finished_at,
            "exit_code": exit_code,
            "error": error,
            "work_root": str(work_root),
            "payload_paths": copied_payload,
            "return_paths": copied_return,
            "missing_return_paths": missing_return,
            "return_state": "pending",
            "log_parts": sorted(path.name for path in result_dir.glob("client.log.part*.txt")),
            "sandbox": sandbox_payload,
            **cancel_payload,
        }
        self._write_status(result_dir, request, final_status)
        return_error: str | None = None
        try:
            return_mode = self._return_result(request, result_dir, bundle_dir)
        except Exception as exc:
            return_error = str(exc)
            failed_status = {
                **final_status,
                "state": "return_failed",
                "return_state": "failed",
                "return_error": return_error,
                "return_failed_at": utc_now(),
            }
            self._write_status(result_dir, request, failed_status)
        else:
            return_payload = (
                {
                    "return_state": "pullback_ready",
                    "pullback_ready_at": utc_now(),
                }
                if return_mode == "pullback"
                else {
                    "return_state": "pushed",
                    "returned_at": utc_now(),
                }
            )
            self._write_status(
                result_dir,
                request,
                {
                    **final_status,
                    **return_payload,
                },
            )
        (task_dir / "DONE").write_text(finished_at + "\n", encoding="utf-8")
        self._clear_ready_marker(request.request_id)
        return 1 if return_error else exit_code

    def _reject_relay_task(
        self,
        task_dir: Path,
        request_id: str,
        error: Exception,
    ) -> int:
        result_dir = task_dir / "result"
        result_dir.mkdir(parents=True, exist_ok=True)
        finished_at = utc_now()
        status = {
            "request_id": request_id,
            "updated_at": finished_at,
            "state": "failed",
            "phase": "parse-request",
            "finished_at": finished_at,
            "exit_code": 1,
            "error": str(error),
            "return_state": "pending",
        }
        write_json(result_dir / "status.json", status, self.config.io.max_file_bytes)
        status.update(
            {
                "return_state": "pullback_ready",
                "pullback_ready_at": utc_now(),
            }
        )
        write_json(result_dir / "status.json", status, self.config.io.max_file_bytes)
        (task_dir / "DONE").write_text(finished_at + "\n", encoding="utf-8")
        self._clear_ready_marker(request_id)
        return 1

    def run_daemon(self) -> None:
        stop = StopFlag()
        signal.signal(signal.SIGINT, stop.handle)
        signal.signal(signal.SIGTERM, stop.handle)
        pending_reexec = consume_pending_reexec()
        print(
            f"git-partner-client watching {self._inbox_dir()} "
            f"working in {self._work_root(None)}",
            flush=True,
        )
        watched_signature = watch_signature(self.config)
        error_backoff = LoopErrorBackoff(self.config.error_backoff)
        if pending_reexec and self.direct:
            self._resume_direct_after_reexec(pending_reexec)
        while not stop.requested:
            try:
                current_signature = watch_signature(self.config)
                changed = watch_signature_changed(watched_signature, current_signature)
                if (
                    changed
                    and self.config.auto_update.enabled
                    and self.config.auto_update.mode == "reexec"
                ):
                    print(
                        "git-partner-client reexec for local update: "
                        + ", ".join(changed[:8]),
                        flush=True,
                    )
                    reexec_partner_role(
                        self.config,
                        self.config_path,
                        "client",
                        LOCAL_WATCH_TRIGGER_REF,
                        "",
                        changed,
                    )
                watched_signature = current_signature

                if self._run_pending_relay_once():
                    error_backoff.reset()
                    continue

                if self.direct:
                    try:
                        self.direct.tick()
                    except Exception as exc:
                        print(f"git-partner-client direct mode error: {exc}", flush=True)
                elif self._sync_exchange_once():
                    print("git-partner-client synced exchange", flush=True)

                request_id = self._next_request_id()
                if request_id:
                    print(f"running relay request {request_id}", flush=True)
                    self.run_once(request_id)
                else:
                    time.sleep(self.config.relay.poll_interval_seconds)
                error_backoff.reset()
            except Exception as exc:
                sleep_seconds = error_backoff.record_failure()
                print(
                    f"git-partner-client error: {exc}; "
                    f"sleeping {sleep_seconds:.1f}s before retry",
                    flush=True,
                )
                time.sleep(sleep_seconds)
        self._parallel_executor.shutdown(wait=True)

    def _resume_direct_after_reexec(
        self,
        pending_reexec: PendingReexec,
    ) -> None:
        assert self.direct is not None
        try:
            self.direct._ensure_initialized()
            if pending_reexec.trigger_ref == LOCAL_WATCH_TRIGGER_REF:
                print(
                    "git-partner-client resumed after local source update",
                    flush=True,
                )
                return
            changed_paths = list(pending_reexec.changed_paths)
            if should_execute(self.config, changed_paths):
                request_files = changed_request_paths(
                    self.config,
                    changed_paths,
                )
                if not request_files:
                    request_files = [self.config.executor.request_file]
                for request_file in request_files:
                    request = parse_request(
                        self.config,
                        pending_reexec.trigger_ref,
                        request_file,
                    )
                    request = route_request_for_current_runtime(
                        self.config,
                        request,
                    )
                    if request is None:
                        continue
                    if effective_transport(self.config, request) in {
                        "direct",
                        "auto",
                    }:
                        if self.direct.schedule_background_watch(
                            request,
                            pending_reexec.trigger_ref,
                        ):
                            continue
                        print(
                            f"direct mode handling pending reexec request "
                            f"{request.request_id} for "
                            f"{pending_reexec.trigger_ref}",
                            flush=True,
                        )
                        self.direct.run_request(
                            request,
                            pending_reexec.trigger_ref,
                        )
            if self.direct._last_delivery_succeeded:
                self.direct._advance_cursor(pending_reexec.trigger_ref)
        except Exception as exc:
            print(f"git-partner-client pending direct error: {exc}", flush=True)

    def _execute(
        self,
        request,
        work_root: Path,
        result_dir: Path,
        task_dir: Path,
        started_at: str,
    ) -> tuple[int, dict[str, object]]:
        if request.client_action:
            return self._execute_client_action(request, work_root)
        command = list(request.client_command or request.command)
        if not command:
            raise RelayError("relay request has no client_command or command")

        work_root.mkdir(parents=True, exist_ok=True)
        writer = ChunkedLogWriter(result_dir, "client.log", self.config.io.max_file_bytes)
        output_queue: queue.Queue[str | None] = queue.Queue()
        env = os.environ.copy()
        env.update(request.env)
        env.setdefault("ASCENDOP_REMOTE_ROOT", str(work_root))
        exec_request = replace(request, command=tuple(command), working_dir=".")
        sandbox = build_sandbox_command(
            self.config,
            exec_request,
            request.sandbox_profile,
            work_root,
            env,
        )
        sandbox_payload = sandbox_status(sandbox)
        for warning in sandbox.warnings:
            writer.write_line(f"[git-partner-client] sandbox warning: {warning}\n")
        writer.write_line(
            "[git-partner-client] sandbox "
            f"profile={sandbox.profile_name} requested={sandbox.requested_backend} "
            f"active={sandbox.active_backend}\n"
        )
        process = subprocess.Popen(
            list(sandbox.command),
            cwd=str(work_root),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            **_process_group_kwargs(),
        )
        process_payload = _process_payload(process)
        self._write_status(
            result_dir,
            request,
            {
                "state": "running",
                "started_at": started_at,
                "heartbeat_at": utc_now(),
                "work_root": str(work_root),
                "process": process_payload,
                "sandbox": sandbox_payload,
            },
        )
        reader = threading.Thread(
            target=_read_stdout,
            args=(process, output_queue),
            daemon=True,
        )
        reader.start()
        deadline = time.monotonic() + request.timeout_seconds if request.timeout_seconds > 0 else None
        next_heartbeat = time.monotonic() + request.sync_interval_seconds
        exit_code: int | None = None
        stop_started: float | None = None
        cancel_reason: str | None = None
        kill_escalated = False
        try:
            while True:
                try:
                    line = output_queue.get(timeout=0.5)
                except queue.Empty:
                    line = None
                if line is not None:
                    writer.write_line(line)

                now = time.monotonic()
                if deadline and now > deadline and process.poll() is None and stop_started is None:
                    _terminate_process_group(process)
                    exit_code = -1
                    stop_started = now
                    writer.write_line("\n[git-partner-client] timeout reached\n")

                if now >= next_heartbeat:
                    if process.poll() is None and stop_started is None:
                        cancel_reason = _relay_cancel_reason(task_dir, request)
                        if cancel_reason:
                            _terminate_process_group(process)
                            exit_code = -2
                            stop_started = now
                            writer.write_line(
                                "\n[git-partner-client] cancellation requested: "
                                f"{cancel_reason}\n"
                            )
                    if (
                        stop_started is not None
                        and process.poll() is None
                        and now - stop_started > 10
                        and not kill_escalated
                    ):
                        _kill_process_group(process)
                        kill_escalated = True
                        writer.write_line(
                            "\n[git-partner-client] stop escalated to SIGKILL\n"
                        )
                    write_json(
                        result_dir / "heartbeat.json",
                        {
                            "state": "cancelling" if cancel_reason else "running",
                            "request_id": request.request_id,
                            "updated_at": utc_now(),
                            "log_parts": sorted(
                                path.name for path in result_dir.glob("client.log.part*.txt")
                            ),
                            **(
                                {"cancel_reason": cancel_reason}
                                if cancel_reason
                                else {}
                            ),
                            "process": process_payload,
                            "sandbox": sandbox_payload,
                        },
                        self.config.io.max_file_bytes,
                    )
                    self._write_status(
                        result_dir,
                        request,
                        {
                            "state": "cancelling" if cancel_reason else "running",
                            "started_at": started_at,
                            "heartbeat_at": utc_now(),
                            "work_root": str(work_root),
                            "log_parts": sorted(
                                path.name for path in result_dir.glob("client.log.part*.txt")
                            ),
                            **(
                                {"cancel_reason": cancel_reason}
                                if cancel_reason
                                else {}
                            ),
                            "process": process_payload,
                            "sandbox": sandbox_payload,
                        },
                    )
                    next_heartbeat = now + request.sync_interval_seconds

                if process.poll() is not None and output_queue.empty():
                    break
            if exit_code is None:
                exit_code = process.wait(timeout=5)
        finally:
            if process.poll() is None:
                _kill_process_group(process)
            reader.join(timeout=1)
        return int(exit_code), sandbox_payload

    def _execute_client_action(
        self,
        request: ExecutionRequest,
        work_root: Path,
    ) -> tuple[int, dict[str, object]]:
        if (
            request.client_action != "engine-exchange"
            or request.request_kind != "engine-exchange"
            or request.completion_mode != "snapshot"
            or request.relay_protocol_version != RELAY_PROTOCOL_VERSION
        ):
            raise RelayError("trusted client action does not match engine exchange contract")
        if set(request.client_action_args) != {"argv"}:
            raise RelayError("engine-exchange client action requires only argv")
        try:
            raw_argv = json.loads(request.client_action_args["argv"])
        except json.JSONDecodeError as exc:
            raise RelayError(f"invalid engine-exchange client argv: {exc}") from exc
        if (
            not isinstance(raw_argv, list)
            or not raw_argv
            or len(raw_argv) > 128
            or not all(isinstance(item, str) and len(item) <= 4096 for item in raw_argv)
        ):
            raise RelayError("engine-exchange client argv must be a bounded string list")

        from limited_remote_partner.cli import test_engine_cli
        from limited_remote_partner.engine.test_engine import TestEngine

        try:
            args = test_engine_cli.build_parser().parse_args(raw_argv)
        except SystemExit as exc:
            raise RelayError("invalid engine-exchange client argv") from exc
        if args.command != "exchange":
            raise RelayError("trusted client action may only invoke engine exchange")

        root_text = str(args.root)
        manifest_text = str(args.manifest)
        transport_text = str(args.transport_dir)
        if any(Path(value).is_absolute() for value in (root_text, manifest_text, transport_text)):
            raise RelayError("trusted engine-exchange paths must be relative")
        args.root = safe_join(work_root, root_text)
        args.manifest = safe_join(work_root, manifest_text)
        args.transport_dir = safe_join(work_root, transport_text)
        args.path_root = work_root.resolve()

        payload_roots = [safe_join(work_root, item) for item in request.payload_paths]
        if not any(
            args.manifest == payload_root or payload_root in args.manifest.parents
            for payload_root in payload_roots
        ):
            raise RelayError("engine-exchange manifest is outside the relayed payload")

        action_started = time.monotonic()
        engine = TestEngine(args.root)
        test_engine_cli.dispatch(engine, args)
        return 0, {
            "profile": request.sandbox_profile or "process",
            "requested_backend": "trusted-in-process",
            "active_backend": "trusted-in-process",
            "warnings": [],
            "client_action": request.client_action,
            "elapsed_seconds": round(time.monotonic() - action_started, 6),
        }

    def _return_result(
        self,
        request: ExecutionRequest,
        result_dir: Path,
        bundle_dir: Path | None = None,
    ) -> str:
        if request.relay_protocol_version == RELAY_PROTOCOL_VERSION:
            if self.config.relay.relay_v2_return_mode != "push-atomic":
                return "pullback"
            batch = self._relay_return_batch(bundle_dir, request.request_id)
            if batch is not None:
                self._push_coalesced_atomic_relay_result(
                    request,
                    result_dir,
                    batch_id=str(batch["batch_id"]),
                    batch_size=int(batch["batch_size"]),
                )
                return "pushed-atomic-batch"
            self._push_atomic_relay_result(request, result_dir)
            return "pushed-atomic"
        dst = (
            f"{self.config.relay.server_return_dir.rstrip('/')}"
            f"/{request.request_id}"
        )
        self.transport.push_dir(result_dir, self.config.relay.server_ssh, dst)
        return "pushed"

    def _push_atomic_relay_result(
        self,
        request: ExecutionRequest,
        result_dir: Path,
    ) -> None:
        host = self.config.relay.server_ssh
        if not host:
            raise RelayError("push-atomic relay return requires relay.server_ssh")
        stream_push = getattr(self.transport, "stream_dir_to_ssh_shell", None)
        if not callable(stream_push):
            raise RelayError("configured transport cannot stream an atomic relay return")

        return_root = self.config.relay.server_return_dir.rstrip("/")
        final_dir = f"{return_root}/{request.request_id}"
        staging_dir = (
            f"{return_root}/.staging/{request.request_id}."
            f"{os.getpid()}.{time.time_ns()}.incoming"
        )
        ready_marker = f"{return_root}/.ready/{request.request_id}.ready"
        publishing_marker = (
            f"{return_root}/.publishing/{request.request_id}.publishing"
        )
        script = "\n".join(
            [
                "set -euo pipefail",
                f"FINAL={shlex.quote(final_dir)}",
                f"STAGING={shlex.quote(staging_dir)}",
                f"REQUEST_ID={shlex.quote(request.request_id)}",
                f"READY_MARKER={shlex.quote(ready_marker)}",
                f"PUBLISHING_MARKER={shlex.quote(publishing_marker)}",
                'mkdir -p "$(dirname "$STAGING")" "$(dirname "$READY_MARKER")" "$(dirname "$PUBLISHING_MARKER")"',
                'printf \'%s\\n\' "$REQUEST_ID" > "$PUBLISHING_MARKER"',
                'cleanup() { rm -f "$PUBLISHING_MARKER"; }',
                "trap cleanup EXIT",
                (
                    'if [ -f "$READY_MARKER" ] && '
                    '[ "$(cat "$READY_MARKER" 2>/dev/null || true)" = "$REQUEST_ID" ] && '
                    '[ -f "$FINAL/status.json" ]; then'
                ),
                "  cat >/dev/null",
                "  echo RELAY_RETURN_ALREADY_READY",
                "  exit 0",
                "fi",
                'rm -rf "$STAGING"',
                'mkdir -p "$STAGING"',
                'tar -xf - -C "$STAGING"',
                'if [ ! -f "$STAGING/status.json" ]; then',
                "  echo RELAY_RETURN_VERIFY_FAILED",
                "  exit 24",
                "fi",
                'rm -f "$READY_MARKER" "$READY_MARKER".__tmp.*',
                'rm -rf "$FINAL"',
                'mv "$STAGING" "$FINAL"',
                'marker_tmp="${READY_MARKER}.__tmp.$$"',
                'printf \'%s\\n\' "$REQUEST_ID" > "$marker_tmp"',
                'mv "$marker_tmp" "$READY_MARKER"',
                "echo RELAY_RETURN_VERIFY_OK",
            ]
        )
        stream_push(result_dir, host, script)

    def _relay_return_batch(
        self,
        bundle_dir: Path | None,
        request_id: str,
    ) -> dict[str, object] | None:
        if bundle_dir is None:
            return None
        metadata_path = bundle_dir / "relay_batch.json"
        if not metadata_path.is_file():
            return None
        try:
            metadata = read_json(metadata_path)
        except Exception:
            return None
        if metadata.get("schema") != "git-partner.relay-batch.v1":
            return None
        batch_id = str(metadata.get("batch_id") or "")
        request_ids = metadata.get("request_ids")
        if not batch_id or not isinstance(request_ids, list) or request_id not in request_ids:
            return None
        try:
            batch_size = int(metadata.get("batch_size") or 0)
        except (TypeError, ValueError):
            return None
        if batch_size <= 1 or batch_size != len(request_ids):
            return None
        return {"batch_id": batch_id, "batch_size": batch_size}

    def _push_coalesced_atomic_relay_result(
        self,
        request: ExecutionRequest,
        result_dir: Path,
        *,
        batch_id: str,
        batch_size: int,
    ) -> None:
        deadline = time.monotonic() + max(
            0.1,
            float(self.config.relay.relay_v2_push_grace_seconds),
        )
        registered = False
        while True:
            selected: dict[str, Path] | None = None
            with self._return_batch_condition:
                state = self._return_batches.setdefault(
                    batch_id,
                    {
                        "expected": batch_size,
                        "pending": {},
                        "completed": set(),
                        "departed": set(),
                        "sending": False,
                        "error": "",
                        "deadline": deadline,
                    },
                )
                pending = state["pending"]
                completed = state["completed"]
                departed = state["departed"]
                if not isinstance(pending, dict) or not isinstance(completed, set):
                    raise RelayError("invalid relay return batch state")
                if not isinstance(departed, set):
                    raise RelayError("invalid relay return batch departure state")
                if not registered:
                    pending[request.request_id] = result_dir
                    state["deadline"] = min(float(state["deadline"]), deadline)
                    registered = True
                    self._return_batch_condition.notify_all()
                error = str(state.get("error") or "")
                if error:
                    departed.add(request.request_id)
                    self._cleanup_return_batch(batch_id, state)
                    raise RelayError(error)
                if request.request_id in completed:
                    departed.add(request.request_id)
                    self._cleanup_return_batch(batch_id, state)
                    return
                expected = int(state["expected"])
                observed = len(pending) + len(completed)
                expired = time.monotonic() >= float(state["deadline"])
                if (
                    not bool(state["sending"])
                    and pending
                    and (observed >= expected or expired)
                ):
                    selected = {
                        str(item): Path(path)
                        for item, path in pending.items()
                    }
                    pending.clear()
                    state["sending"] = True
                else:
                    remaining = max(
                        0.05,
                        float(state["deadline"]) - time.monotonic(),
                    )
                    self._return_batch_condition.wait(timeout=remaining)
                    continue
            try:
                self._push_atomic_relay_results_batch(selected)
            except Exception as exc:
                with self._return_batch_condition:
                    state = self._return_batches[batch_id]
                    state["sending"] = False
                    state["error"] = str(exc)
                    self._return_batch_condition.notify_all()
                continue
            with self._return_batch_condition:
                state = self._return_batches[batch_id]
                completed = state["completed"]
                if not isinstance(completed, set):
                    raise RelayError("invalid relay return batch completion state")
                completed.update(selected)
                state["sending"] = False
                self._return_batch_condition.notify_all()

    def _cleanup_return_batch(
        self,
        batch_id: str,
        state: dict[str, object],
    ) -> None:
        departed = state.get("departed")
        expected = int(state.get("expected") or 0)
        if isinstance(departed, set) and len(departed) >= expected:
            self._return_batches.pop(batch_id, None)

    def _push_atomic_relay_results_batch(
        self,
        results: dict[str, Path],
    ) -> None:
        if not results:
            raise RelayError("atomic relay return batch is empty")
        host = self.config.relay.server_ssh
        if not host:
            raise RelayError("push-atomic relay return requires relay.server_ssh")
        stream_batch = getattr(self.transport, "stream_dirs_to_ssh_shell", None)
        if not callable(stream_batch):
            for request_id, result_dir in results.items():
                request = ExecutionRequest(
                    request_id=request_id,
                    command=(),
                    working_dir=".",
                    output_subdir=request_id,
                )
                self._push_atomic_relay_result(request, result_dir)
            return

        return_root = self.config.relay.server_return_dir.rstrip("/")
        stamp = f"{os.getpid()}.{time.time_ns()}"
        batch_dir = f"{return_root}/.staging/return-batch.{stamp}.incoming"
        request_ids = sorted(results)
        request_words = " ".join(shlex.quote(item) for item in request_ids)
        script = "\n".join(
            [
                "set -euo pipefail",
                f"RETURN_ROOT={shlex.quote(return_root)}",
                f"BATCH={shlex.quote(batch_dir)}",
                'rm -rf "$BATCH"',
                'mkdir -p "$BATCH" "$RETURN_ROOT/.ready" "$RETURN_ROOT/.publishing"',
                'tar -xf - -C "$BATCH"',
                f"for REQUEST_ID in {request_words}; do",
                '  SOURCE="$BATCH/$REQUEST_ID"',
                '  FINAL="$RETURN_ROOT/$REQUEST_ID"',
                '  READY_MARKER="$RETURN_ROOT/.ready/$REQUEST_ID.ready"',
                '  PUBLISHING_MARKER="$RETURN_ROOT/.publishing/$REQUEST_ID.publishing"',
                '  if [ ! -f "$SOURCE/status.json" ]; then',
                '    echo "RELAY_RETURN_BATCH_VERIFY_FAILED:$REQUEST_ID"',
                "    exit 24",
                "  fi",
                '  printf \'%s\\n\' "$REQUEST_ID" > "$PUBLISHING_MARKER"',
                '  rm -f "$READY_MARKER" "$READY_MARKER".__tmp.*',
                '  rm -rf "$FINAL"',
                '  mv "$SOURCE" "$FINAL"',
                "done",
                f"for REQUEST_ID in {request_words}; do",
                '  READY_MARKER="$RETURN_ROOT/.ready/$REQUEST_ID.ready"',
                '  PUBLISHING_MARKER="$RETURN_ROOT/.publishing/$REQUEST_ID.publishing"',
                '  marker_tmp="${READY_MARKER}.__tmp.$$"',
                '  printf \'%s\\n\' "$REQUEST_ID" > "$marker_tmp"',
                '  mv "$marker_tmp" "$READY_MARKER"',
                '  rm -f "$PUBLISHING_MARKER"',
                "done",
                'rm -rf "$BATCH"',
                "echo RELAY_RETURN_BATCH_VERIFY_OK",
            ]
        )
        stream_batch(results, host, script)

    def _write_status(self, result_dir: Path, request, payload: dict[str, object]) -> None:
        existing: dict[str, object] = {}
        status_path = result_dir / "status.json"
        if status_path.exists():
            try:
                raw = read_json(status_path)
                if isinstance(raw, dict):
                    existing = raw
            except Exception:
                existing = {}
        history_raw = existing.get("relay_state_history", [])
        history = [dict(item) for item in history_raw if isinstance(item, dict)]
        updated_at = utc_now()
        state = str(payload.get("state", ""))
        if not history or str(history[-1].get("state", "")) != state:
            history.append({"observed_at": updated_at, "state": state})
        data = {
            **request_status_metadata(request),
            "request_id": request.request_id,
            "updated_at": updated_at,
            "relay_state_history": history[-64:],
            **payload,
        }
        write_json(result_dir / "status.json", data, self.config.io.max_file_bytes)

    def _next_request_id(self) -> str | None:
        if self.config.relay.transport_mode == "direct":
            return None
        ready_request = self._next_ready_request_id()
        if ready_request:
            return ready_request
        return self._next_legacy_request_id()

    def _ready_request_ids(self, limit: int) -> list[str]:
        if self.config.relay.transport_mode == "direct" or limit <= 0:
            return []
        ready_dir = self._ready_dir()
        if not ready_dir.exists():
            return []
        markers = sorted(
            (item for item in ready_dir.glob("*.ready") if item.is_file()),
            key=lambda item: (item.stat().st_mtime, item.name),
        )
        request_ids: list[str] = []
        with self._parallel_lock:
            active_request_ids = set(self._parallel_futures)
        for marker in markers:
            request_id = marker.name.removesuffix(".ready")
            if not request_id or Path(request_id).name != request_id:
                marker.unlink(missing_ok=True)
                continue
            task_dir = self._inbox_dir() / request_id
            if not task_dir.is_dir() or not self._has_request(task_dir):
                marker.unlink(missing_ok=True)
                continue
            if (task_dir / "DONE").exists() or self._mark_terminal_relay_task_done(task_dir):
                marker.unlink(missing_ok=True)
                continue
            if request_id in active_request_ids:
                continue
            request_ids.append(request_id)
            if len(request_ids) >= limit:
                break
        return request_ids

    def _next_ready_request_id(self) -> str | None:
        request_ids = self._ready_request_ids(1)
        return request_ids[0] if request_ids else None

    def _next_legacy_request_id(self) -> str | None:
        inbox_dir = self._inbox_dir()
        if not inbox_dir.exists():
            return None
        if self._legacy_scan_iter is None:
            self._legacy_scan_iter = os.scandir(inbox_dir)
        scanned = 0
        while scanned < LEGACY_INBOX_SCAN_BUDGET:
            try:
                entry = next(self._legacy_scan_iter)
            except StopIteration:
                self._legacy_scan_iter.close()
                self._legacy_scan_iter = None
                return None
            scanned += 1
            if not entry.is_dir(follow_symlinks=False) or entry.name == ".ready":
                continue
            item = Path(entry.path)
            if (item / "DONE").exists() or not self._has_request(item):
                continue
            if self._mark_terminal_relay_task_done(item):
                continue
            if (item / "result" / "status.json").exists():
                continue
            return item.name
        return None

    def _mark_terminal_relay_task_done(self, task_dir: Path) -> bool:
        status_path = task_dir / "result" / "status.json"
        if not status_path.exists():
            return False
        try:
            raw = json.loads(status_path.read_text(encoding="utf-8"))
        except Exception:
            return False
        state = str(raw.get("state", ""))
        if state not in {"success", "failed", "return_failed", "stalled", "abandoned", "cancelled"}:
            return False
        (task_dir / "DONE").write_text(utc_now() + "\n", encoding="utf-8")
        self._clear_ready_marker(task_dir.name)
        return True

    def _ready_dir(self) -> Path:
        return self._inbox_dir() / ".ready"

    def _clear_ready_marker(self, request_id: str) -> None:
        (self._ready_dir() / f"{request_id}.ready").unlink(missing_ok=True)

    def _run_pending_relay_once(self) -> bool:
        reaped = self._reap_parallel_requests()
        ready_ids = self._ready_request_ids(
            self.config.relay.max_parallel_requests
        )
        if not ready_ids:
            if self._parallel_request_count():
                time.sleep(min(0.05, self.config.relay.poll_interval_seconds))
                return True
            request_id = self._next_legacy_request_id()
            if not request_id:
                return reaped
            print(f"running relay request {request_id}", flush=True)
            self.run_once(request_id)
            return True

        first = ready_ids[0]
        if not self._parallel_engine_request(first):
            if self._parallel_request_count():
                time.sleep(min(0.05, self.config.relay.poll_interval_seconds))
                return True
            print(f"running relay request {first}", flush=True)
            self.run_once(first)
            return True

        batch = [
            request_id
            for request_id in ready_ids
            if self._parallel_engine_request(request_id)
        ]
        print(
            "starting parallel engine relay requests " + ", ".join(batch),
            flush=True,
        )
        with self._parallel_lock:
            for request_id in batch:
                if request_id in self._parallel_futures:
                    continue
                self._parallel_futures[request_id] = self._parallel_executor.submit(
                    self.run_once,
                    request_id,
                )
        return True

    def _reap_parallel_requests(self) -> bool:
        with self._parallel_lock:
            completed = [
                (request_id, future)
                for request_id, future in self._parallel_futures.items()
                if future.done()
            ]
            for request_id, _future in completed:
                self._parallel_futures.pop(request_id, None)
        for request_id, future in completed:
            try:
                future.result()
            except Exception as exc:
                print(
                    f"parallel relay request {request_id} failed: {exc}",
                    flush=True,
                )
        return bool(completed)

    def _parallel_request_count(self) -> int:
        with self._parallel_lock:
            return len(self._parallel_futures)

    def _parallel_engine_request(self, request_id: str) -> bool:
        task_dir = self._inbox_dir() / request_id
        try:
            request = read_request(
                self._bundle_dir(task_dir, request_id) / "request.json"
            )
        except Exception:
            return False
        if request.relay_protocol_version != RELAY_PROTOCOL_VERSION:
            return False
        if (
            request.request_kind == "engine-exchange"
            and request.completion_mode == "snapshot"
            and request.client_action == "engine-exchange"
        ):
            return True
        return bool(
            request.request_kind
            in {
                "host-only-canary",
                "engine-host-canary",
                "engine-device-canary",
            }
            and request.completion_mode == "terminal"
            and not request.workflow_ingest
            and request.experiment_id
            and request.attempt_id
        )

    def _inbox_dir(self) -> Path:
        return _local_path(self.config.repo_dir, self.config.relay.client_inbox_dir)

    @staticmethod
    def _bundle_dir(task_dir: Path, request_id: str) -> Path:
        if (task_dir / "request.json").exists():
            return task_dir
        nested = task_dir / request_id
        if (nested / "request.json").exists():
            return nested
        raise RelayError(f"missing relay request: {task_dir / 'request.json'}")

    @classmethod
    def _has_request(cls, task_dir: Path) -> bool:
        try:
            cls._bundle_dir(task_dir, task_dir.name)
        except RelayError:
            return False
        return True

    def _work_root(self, request_work_dir: str | None) -> Path:
        return _local_path(
            self.config.repo_dir,
            request_work_dir or self.config.relay.client_work_dir,
        )

    def _sync_exchange_once(self) -> bool:
        if not self.config.exchange_watch.enabled or self.exchange_git is None:
            return False
        if not self._exchange_initialized:
            self.exchange_git.ensure_worktree()
            self.exchange_git.configure_identity()
            self._exchange_initialized = True
        return sync_once(self.exchange_git, self.config)


class DirectClient:
    def __init__(
        self,
        config_path: Path,
        config: AppConfig,
        *,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self.config_path = config_path
        self.config = config
        self.git = GitClient(config.repo, config.repo_dir)
        self.transport = ScpTransport(config)
        self._executor = executor
        self._background_lock = threading.Lock()
        self._background_watches: dict[
            str,
            Future[tuple[int, bool]],
        ] = {}
        self._initialized = False
        self._last_ref: str | None = None
        self._pending_ref: str | None = None
        self._completed_request_paths: set[str] = set()
        self._last_delivery_succeeded = True
        self._cursor_path = (
            config.repo_dir
            / config.io.state_dir
            / "direct_control_cursor.json"
        )

    def tick(self) -> None:
        self._ensure_initialized()
        self._reap_background_watches()
        push_exchange_if_changed(self.git, self.config)
        remote_ref = self.git.fetch()
        if self._last_ref is None:
            self._last_ref = self.git.rev_parse("HEAD")
        target_ref = remote_ref
        if self._pending_ref and self._pending_ref != remote_ref:
            self._pending_ref = remote_ref
            self._write_cursor(str(self._last_ref or remote_ref))
        if target_ref == self._last_ref:
            return

        changed = self.git.changed_paths(self._last_ref, target_ref)
        self.git.checkout_remote_head()
        self._begin_cursor_batch(target_ref)
        if should_reexec_for_update(self.config, changed):
            reexec_partner_role(
                self.config,
                self.config_path,
                "client",
                remote_ref,
                self._last_ref,
                changed,
            )
        if should_execute(self.config, changed):
            delivery_failed = False
            request_files = changed_request_paths(
                self.config,
                changed,
            ) or [self.config.executor.request_file]
            queued_requests: list[tuple[str, ExecutionRequest]] = []
            for request_file in request_files:
                if request_file in self._completed_request_paths:
                    continue
                try:
                    request = parse_request(
                        self.config,
                        target_ref,
                        request_file,
                    )
                except Exception as exc:
                    print(
                        f"direct request rejected for {target_ref} "
                        f"path={request_file}: {exc}",
                        flush=True,
                    )
                    self._mark_request_completed(request_file)
                    continue
                request = route_request_for_current_runtime(
                    self.config,
                    request,
                )
                if request is None:
                    self._mark_request_completed(request_file)
                    continue
                queued_requests.append((request_file, request))
            for request_file, request in sorted(
                queued_requests,
                key=lambda item: request_dispatch_priority(item[1]),
            ):
                transport = effective_transport(self.config, request)
                if transport in {"direct", "auto"}:
                    if self.schedule_background_watch(request, remote_ref):
                        continue
                    print(
                        f"direct mode handling request {request.request_id} "
                        f"for {target_ref}",
                        flush=True,
                    )
                    self.run_request(request, target_ref)
                    if not self._last_delivery_succeeded:
                        delivery_failed = True
                        break
                self._mark_request_completed(request_file)
                if self._adopt_newer_control_ref(target_ref):
                    return
            if delivery_failed:
                return
            # Only advance through the ref whose changed paths were dispatched.
            # A fresh fetch here can include requests published while one slow
            # request was executing and would skip them permanently.
            self._advance_cursor(remote_ref)
            return
        self._advance_cursor(target_ref)

    def schedule_background_watch(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
    ) -> bool:
        if self._executor is None or not self._is_background_watch(request):
            return False
        with self._background_lock:
            existing = self._background_watches.get(request.request_id)
            if existing is not None and not existing.done():
                return True
            self._background_watches[request.request_id] = self._executor.submit(
                self._run_request_isolated,
                request,
                trigger_ref,
            )
        print(
            f"direct mode started background Engine watch "
            f"{request.request_id} for {trigger_ref}",
            flush=True,
        )
        return True

    def _reap_background_watches(self) -> None:
        with self._background_lock:
            completed = [
                (request_id, future)
                for request_id, future in self._background_watches.items()
                if future.done()
            ]
            for request_id, _future in completed:
                self._background_watches.pop(request_id, None)
        for request_id, future in completed:
            try:
                _exit_code, delivered = future.result()
            except Exception as exc:
                print(
                    f"background Engine watch {request_id} failed: {exc}",
                    flush=True,
                )
                continue
            if not delivered:
                print(
                    f"background Engine watch {request_id} return was not published",
                    flush=True,
                )

    def _is_background_watch(self, request: ExecutionRequest) -> bool:
        if (
            request.request_kind != "engine-exchange"
            or request.completion_mode != "snapshot"
            or request.client_action != "engine-exchange"
            or set(request.client_action_args) != {"argv"}
        ):
            return False
        try:
            raw_argv = json.loads(request.client_action_args["argv"])
        except (json.JSONDecodeError, TypeError):
            return False
        if (
            not isinstance(raw_argv, list)
            or not raw_argv
            or not all(isinstance(item, str) for item in raw_argv)
        ):
            return False
        if "--ack-return" in raw_argv or "--ack-required" in raw_argv:
            return False
        try:
            manifest_index = raw_argv.index("--manifest") + 1
            manifest_text = raw_argv[manifest_index]
            wait_index = raw_argv.index("--wait-ready-seconds") + 1
            wait_ready_seconds = float(raw_argv[wait_index])
            manifest_path = safe_join(self.config.repo_dir, manifest_text)
            payload_roots = [
                safe_join(self.config.repo_dir, item)
                for item in request.payload_paths
            ]
        except (ValueError, IndexError, RelayError):
            return False
        if wait_ready_seconds <= 0 or not any(
            manifest_path == payload_root or payload_root in manifest_path.parents
            for payload_root in payload_roots
        ):
            return False
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return False
        if not isinstance(manifest, dict):
            return False
        return not manifest.get("jobs") and not manifest.get(
            "standby_cancellations"
        )

    def schedule_background_watch(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
    ) -> bool:
        if self._executor is None or not self._is_background_watch(request):
            return False
        with self._background_lock:
            existing = self._background_watches.get(request.request_id)
            if existing is not None and not existing.done():
                return True
            self._background_watches[request.request_id] = self._executor.submit(
                self._run_request_isolated,
                request,
                trigger_ref,
            )
        print(
            f"direct mode started background Engine watch "
            f"{request.request_id} for {trigger_ref}",
            flush=True,
        )
        return True

    def _reap_background_watches(self) -> None:
        with self._background_lock:
            completed = [
                (request_id, future)
                for request_id, future in self._background_watches.items()
                if future.done()
            ]
            for request_id, _future in completed:
                self._background_watches.pop(request_id, None)
        for request_id, future in completed:
            try:
                _exit_code, delivered = future.result()
            except Exception as exc:
                print(
                    f"background Engine watch {request_id} failed: {exc}",
                    flush=True,
                )
                continue
            if not delivered:
                print(
                    f"background Engine watch {request_id} return was not published",
                    flush=True,
                )

    def _is_background_watch(self, request: ExecutionRequest) -> bool:
        if (
            request.request_kind != "engine-exchange"
            or request.completion_mode != "snapshot"
            or request.client_action != "engine-exchange"
            or set(request.client_action_args) != {"argv"}
        ):
            return False
        try:
            raw_argv = json.loads(request.client_action_args["argv"])
        except (json.JSONDecodeError, TypeError):
            return False
        if (
            not isinstance(raw_argv, list)
            or not raw_argv
            or not all(isinstance(item, str) for item in raw_argv)
        ):
            return False
        if "--ack-return" in raw_argv or "--ack-required" in raw_argv:
            return False
        try:
            manifest_index = raw_argv.index("--manifest") + 1
            manifest_text = raw_argv[manifest_index]
            wait_index = raw_argv.index("--wait-ready-seconds") + 1
            wait_ready_seconds = float(raw_argv[wait_index])
            manifest_path = safe_join(self.config.repo_dir, manifest_text)
            payload_roots = [
                safe_join(self.config.repo_dir, item)
                for item in request.payload_paths
            ]
        except (ValueError, IndexError, RelayError):
            return False
        if wait_ready_seconds <= 0 or not any(
            manifest_path == payload_root or payload_root in manifest_path.parents
            for payload_root in payload_roots
        ):
            return False
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return False
        if not isinstance(manifest, dict):
            return False
        return not manifest.get("jobs") and not manifest.get(
            "standby_cancellations"
        )

    def run_request(self, request: ExecutionRequest, trigger_ref: str) -> int:
        self._last_delivery_succeeded = False
        exit_code, delivered = self._run_request_isolated(request, trigger_ref)
        self._last_delivery_succeeded = delivered
        return exit_code

    def _run_request_isolated(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
    ) -> tuple[int, bool]:
        recovered = self._republish_terminal_result(request, trigger_ref)
        if recovered is not None:
            return recovered, True
        if not self._claim(request, trigger_ref):
            print(
                f"direct claim failed for {request.request_id}; "
                "leaving request for relay fallback",
                flush=True,
            )
            return (
                1 if effective_transport(self.config, request) == "direct" else 0,
                False,
            )

        proxy = DirectGitProxy(self.git)
        exit_code = self._run_claimed_request(request, trigger_ref, proxy)
        if proxy.failures:
            self._return_direct_result(request, proxy.failures)
        return exit_code, not proxy.failures

    def _republish_terminal_result(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
    ) -> int | None:
        result_dir = (
            self.config.repo_dir
            / self.config.io.output_dir
            / request.output_subdir
        )
        status_path = result_dir / "status.json"
        if not status_path.is_file():
            return None
        status = read_json(status_path)
        if (
            str(status.get("request_id") or "") != request.request_id
            or str(status.get("state") or "")
            not in {"success", "failed", "cancelled"}
        ):
            return None
        same_trigger = str(status.get("trigger_ref") or "") == trigger_ref
        if not same_trigger and not self._terminal_status_matches_attempt(
            request,
            status,
        ):
            return None
        try:
            self.git.commit_and_push(
                [self._result_repo_path(result_dir)],
                f"git_partner_client: {request.request_id} direct result recovery",
                self.config.io.max_file_bytes,
            )
        except Exception as exc:
            print(
                f"direct result recovery failed for {request.request_id}: {exc}",
                flush=True,
            )
            return int(status.get("exit_code", 1))
        return int(status.get("exit_code", 0))

    def _run_claimed_request(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
        git: "DirectGitProxy",
    ) -> int:
        result_dir = self.config.repo_dir / self.config.io.output_dir / request.output_subdir
        work_root = _local_path(
            self.config.repo_dir,
            request.client_work_dir or self.config.relay.client_work_dir,
        )
        started_at = utc_now()
        copied_payload: list[str] = []
        copied_return: list[str] = []
        missing_return: list[str] = []
        sandbox_payload: dict[str, object] = {}
        exit_code = 1
        error: str | None = None

        self._write_direct_status(
            result_dir,
            request,
            {
                "state": "copying-payload",
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "work_root": str(work_root),
                "command": list(request.client_command or request.command),
            },
        )
        if not self._coalesce_direct_control_probe_status(request):
            git.commit_and_push(
                [self._result_repo_path(result_dir)],
                f"git_partner_client: {request.request_id} direct copying payload",
                self.config.io.max_file_bytes,
            )

        try:
            if request.payload_paths:
                copied_payload, _missing_payload = copy_requested_paths(
                    self._payload_root(request),
                    work_root,
                    request.payload_paths,
                    self.config.io.max_file_bytes,
                )
            self._write_direct_status(
                result_dir,
                request,
                {
                    "state": "running",
                    "trigger_ref": trigger_ref,
                    "started_at": started_at,
                    "work_root": str(work_root),
                    "payload_paths": copied_payload,
                    "command": list(request.client_command or request.command),
                },
            )
            if not self._coalesce_direct_control_probe_status(request):
                git.commit_and_push(
                    [self._result_repo_path(result_dir)],
                    f"git_partner_client: {request.request_id} direct running",
                    self.config.io.max_file_bytes,
                )
            exit_code, sandbox_payload = self._execute_direct(
                request,
                work_root,
                result_dir,
                trigger_ref,
                started_at,
                git,
            )
            copied_return, missing_return = copy_requested_paths(
                work_root,
                result_dir / "client_output",
                request.return_paths,
                self.config.io.max_file_bytes,
                missing_ok=True,
            )
        except Exception as exc:
            error = str(exc)
            exit_code = 1

        finished_at = utc_now()
        self._write_direct_status(
            result_dir,
            request,
            {
                "state": _final_state(exit_code, error),
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "finished_at": finished_at,
                "exit_code": exit_code,
                "error": error,
                "work_root": str(work_root),
                "payload_paths": copied_payload,
                "return_paths": copied_return,
                "missing_return_paths": missing_return,
                "log_parts": sorted(
                    path.name for path in result_dir.glob(f"{request.log_name}.part*.txt")
                ),
                "sandbox": sandbox_payload,
                **_existing_cancel_reason(result_dir),
            },
        )
        git.commit_and_push(
            [self._result_repo_path(result_dir)],
            f"git_partner_client: {request.request_id} direct finished",
            self.config.io.max_file_bytes,
        )
        return exit_code

    def _execute_direct(
        self,
        request: ExecutionRequest,
        work_root: Path,
        result_dir: Path,
        trigger_ref: str,
        started_at: str,
        git: "DirectGitProxy",
    ) -> tuple[int, dict[str, object]]:
        command = list(request.client_command or request.command)
        if not command:
            raise RelayError("direct request has no client_command or command")

        work_root.mkdir(parents=True, exist_ok=True)
        writer = ChunkedLogWriter(result_dir, request.log_name, self.config.io.max_file_bytes)
        if request.server_action == "lan-node-ack":
            ack_result = apply_local_node_ack(
                self.config_path,
                request.server_action_args.get("node_ack_json", ""),
            )
            writer.write_line(
                "[git-partner-client] local node acknowledgement applied "
                f"session={ack_result['session_id']}\n"
            )
            return 0, {
                "profile": "local-node-ack",
                "requested_backend": "local-control",
                "active_backend": "local-control",
                "warnings": [],
            }
        if request.server_action == "lan-restart-service":
            restart_result = self._schedule_direct_local_restart(request)
            writer.write_line(
                "[git-partner-client] local client restart scheduled "
                f"target={restart_result['target_dir']} "
                f"delay_seconds={restart_result['delay_seconds']}\n"
            )
            return 0, {
                "profile": "local-role-restart",
                "requested_backend": "local-control",
                "active_backend": "local-control",
                "warnings": [],
                "restart": restart_result,
            }
        if request.server_action == "lan-reconcile-request":
            reconcile_result = self._reconcile_direct_request(
                request,
                trigger_ref,
            )
            writer.write_line(
                "[git-partner-client] direct request reconciliation "
                f"target={reconcile_result['target_request_id']} "
                f"state={reconcile_result['state']}\n"
            )
            return 0, {
                "profile": "local-request-reconcile",
                "requested_backend": "local-control",
                "active_backend": "local-control",
                "warnings": [],
                "reconcile": reconcile_result,
            }
        output_queue: queue.Queue[str | None] = queue.Queue()
        env = os.environ.copy()
        env.update(request.env)
        env.setdefault("ASCENDOP_REMOTE_ROOT", str(work_root))
        exec_request = replace(request, command=tuple(command), working_dir=".")
        sandbox = build_sandbox_command(
            self.config,
            exec_request,
            request.sandbox_profile,
            work_root,
            env,
        )
        sandbox_payload = sandbox_status(sandbox)
        for warning in sandbox.warnings:
            writer.write_line(f"[git-partner-client] sandbox warning: {warning}\n")
        writer.write_line(
            "[git-partner-client] sandbox "
            f"profile={sandbox.profile_name} requested={sandbox.requested_backend} "
            f"active={sandbox.active_backend}\n"
        )
        process = subprocess.Popen(
            list(sandbox.command),
            cwd=str(work_root),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            **_process_group_kwargs(),
        )
        process_payload = _process_payload(process)
        self._write_direct_status(
            result_dir,
            request,
            {
                "state": "running",
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "heartbeat_at": utc_now(),
                "work_root": str(work_root),
                "process": process_payload,
                "sandbox": sandbox_payload,
            },
        )
        reader = threading.Thread(
            target=_read_stdout,
            args=(process, output_queue),
            daemon=True,
        )
        reader.start()
        deadline = time.monotonic() + request.timeout_seconds if request.timeout_seconds > 0 else None
        next_heartbeat = time.monotonic() + request.sync_interval_seconds
        exit_code: int | None = None
        stop_started: float | None = None
        cancel_reason: str | None = None
        kill_escalated = False
        try:
            while True:
                try:
                    line = output_queue.get(timeout=0.5)
                except queue.Empty:
                    line = None
                if line is not None:
                    writer.write_line(line)

                now = time.monotonic()
                if deadline and now > deadline and process.poll() is None and stop_started is None:
                    _terminate_process_group(process)
                    exit_code = -1
                    stop_started = now
                    writer.write_line("\n[git-partner-client] timeout reached\n")

                if now >= next_heartbeat:
                    if (
                        process.poll() is None
                        and stop_started is None
                        and not self._coalesce_direct_control_probe_status(request)
                    ):
                        cancel_reason = self._remote_cancel_reason(request, trigger_ref, writer)
                        if cancel_reason:
                            _terminate_process_group(process)
                            exit_code = -2
                            stop_started = now
                            writer.write_line(
                                "\n[git-partner-client] cancellation requested: "
                                f"{cancel_reason}\n"
                            )
                    if (
                        stop_started is not None
                        and process.poll() is None
                        and now - stop_started > 10
                        and not kill_escalated
                    ):
                        _kill_process_group(process)
                        kill_escalated = True
                        writer.write_line(
                            "\n[git-partner-client] stop escalated to SIGKILL\n"
                        )
                    self._write_direct_status(
                        result_dir,
                        request,
                        {
                            "state": "cancelling" if cancel_reason else "running",
                            "trigger_ref": trigger_ref,
                            "started_at": started_at,
                            "heartbeat_at": utc_now(),
                            "log_parts": [path.name for path in writer.written_paths()],
                            **(
                                {"cancel_reason": cancel_reason}
                                if cancel_reason
                                else {}
                            ),
                            "process": process_payload,
                            "sandbox": sandbox_payload,
                        },
                    )
                    if not self._coalesce_direct_control_probe_status(request):
                        git.commit_and_push(
                            [self._result_repo_path(result_dir)],
                            f"git_partner_client: {request.request_id} direct heartbeat",
                            self.config.io.max_file_bytes,
                        )
                    next_heartbeat = now + request.sync_interval_seconds

                if process.poll() is not None and output_queue.empty():
                    break
            if exit_code is None:
                exit_code = process.wait(timeout=5)
        finally:
            if process.poll() is None:
                _kill_process_group(process)
            reader.join(timeout=1)
        return int(exit_code), sandbox_payload

    def _schedule_direct_local_restart(
        self,
        request: ExecutionRequest,
    ) -> dict[str, object]:
        target_role = request.server_action_args.get("target_role", "client")
        current_role = str(self.config.relay.role or "").strip().lower()
        if target_role != "client" or current_role != "client":
            raise RelayError(
                "direct local restart only supports the resident client role"
            )

        target_dir = self.config.repo_dir.resolve()
        requested_target = request.server_action_args.get("target_dir", "")
        if requested_target:
            requested_path = Path(requested_target).expanduser().resolve()
            if requested_path != target_dir:
                raise RelayError(
                    "direct local restart target_dir must match the active repo_dir"
                )

        config_path = self.config_path.resolve()
        delay_seconds = 15
        args = argparse.Namespace(
            service_name=request.server_action_args.get("service_name", ""),
            remote_config=str(config_path),
            no_process_fallback=request.server_action_args.get(
                "no_process_fallback", "0"
            ).lower()
            in {"1", "true", "yes", "on"},
            cleanup_request_id=request.server_action_args.get(
                "cleanup_request_id", ""
            ),
        )
        report = schedule_local_restart_service(
            self.transport,
            str(target_dir),
            target_role,
            args,
            request.request_id,
            delay_seconds=delay_seconds,
        )
        return {
            "action": "restart-service-scheduled",
            "target_role": target_role,
            "target_dir": str(target_dir),
            "config_path": str(config_path),
            "delay_seconds": delay_seconds,
            "report": report,
        }

    def _reconcile_direct_request(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
    ) -> dict[str, object]:
        target_request_id = request.server_action_args.get(
            "reconcile_request_id", ""
        ).strip()
        expected_sha256 = request.server_action_args.get(
            "reconcile_job_sha256", ""
        ).strip().lower()
        if (
            not target_request_id
            or any(
                char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
                for char in target_request_id
            )
        ):
            raise RelayError(
                "lan-reconcile-request requires a safe reconcile_request_id"
            )
        if (
            len(expected_sha256) != 64
            or any(char not in "0123456789abcdef" for char in expected_sha256)
        ):
            raise RelayError(
                "lan-reconcile-request requires a lowercase SHA-256 job digest"
            )

        request_path = (
            self.config.repo_dir
            / self.config.io.input_dir
            / "requests"
            / target_request_id
            / "job.json"
        )
        if not request_path.is_file():
            raise RelayError(
                "reconciled direct request does not exist: "
                f"{target_request_id}"
            )
        actual_sha256 = hashlib.sha256(request_path.read_bytes()).hexdigest()
        if actual_sha256 != expected_sha256:
            raise RelayError(
                "reconciled direct request digest mismatch: "
                f"expected={expected_sha256} actual={actual_sha256}"
            )
        request_file = request_path.resolve().relative_to(
            self.config.repo_dir.resolve()
        )
        target = parse_request(
            self.config,
            trigger_ref,
            request_file,
        )
        target = route_request_for_current_runtime(self.config, target)
        if target is None or target.request_id != target_request_id:
            raise RelayError(
                "reconciled direct request does not target this runtime"
            )
        if effective_transport(self.config, target) not in {"direct", "auto"}:
            raise RelayError(
                "reconciled request is not a direct transport request"
            )
        if not self._is_background_watch(target):
            raise RelayError(
                "lan-reconcile-request only accepts snapshot Engine watches"
            )

        status_path = (
            self.config.repo_dir
            / self.config.io.output_dir
            / target.output_subdir
            / "status.json"
        )
        if status_path.is_file():
            status = read_json(status_path)
            state = str(status.get("state") or "")
            if state in {"success", "failed", "cancelled"}:
                return {
                    "target_request_id": target_request_id,
                    "target_job_sha256": actual_sha256,
                    "state": "already-terminal",
                }
            return {
                "target_request_id": target_request_id,
                "target_job_sha256": actual_sha256,
                "state": "already-active",
            }
        if not self.schedule_background_watch(target, trigger_ref):
            raise RelayError(
                "reconciled Engine watch could not enter the background executor"
            )
        return {
            "target_request_id": target_request_id,
            "target_job_sha256": actual_sha256,
            "state": "scheduled",
        }

    def _remote_cancel_reason(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
        writer: ChunkedLogWriter,
    ) -> str | None:
        try:
            remote_ref = self.git.fetch()
            cancel_text = self.git.show_text(
                f"{remote_ref}:{self.config.io.input_dir.rstrip('/')}/cancel.json",
                check=False,
            )
            cancel_reason = _cancel_reason_from_text(cancel_text, request)
            if cancel_reason:
                return cancel_reason

            request_text = self.git.show_text(
                f"{remote_ref}:{self.config.executor.request_file}",
                check=False,
            )
            remote_request_id = _request_id_from_text(request_text)
            if remote_ref != trigger_ref and remote_request_id and remote_request_id != request.request_id:
                return (
                    "remote request replaced current job: "
                    f"{request.request_id} -> {remote_request_id}"
                )
        except (GitError, json.JSONDecodeError) as exc:
            writer.write_line(
                "\n[git-partner-client] cancellation check skipped: "
                f"{exc}\n"
            )
        return None

    def _claim(self, request: ExecutionRequest, trigger_ref: str) -> bool:
        result_dir = self.config.repo_dir / self.config.io.output_dir / request.output_subdir
        self._write_direct_status(
            result_dir,
            request,
            {
                "state": "claimed",
                "trigger_ref": trigger_ref,
                "claimed_at": utc_now(),
                "command": list(request.client_command or request.command),
            },
        )
        if self._coalesce_direct_control_probe_status(request):
            return True
        try:
            self.git.commit_and_push(
                [self._result_repo_path(result_dir)],
                f"git_partner_client: {request.request_id} direct claimed",
                self.config.io.max_file_bytes,
            )
        except Exception as exc:
            self._write_direct_status(
                result_dir,
                request,
                {
                    "state": "claim_failed",
                    "trigger_ref": trigger_ref,
                    "claim_error": str(exc),
                    "updated_at": utc_now(),
                },
            )
            return False
        return True

    def _return_direct_result(
        self,
        request: ExecutionRequest,
        git_failures: list[str],
    ) -> None:
        result_dir = self.config.repo_dir / self.config.io.output_dir / request.output_subdir
        write_json(
            result_dir / "direct_return.json",
            {
                "transport": "direct",
                "return_channel": "scp-to-server",
                "request_id": request.request_id,
                "output_subdir": request.output_subdir,
                "returned_by": _actor_name(),
                "git_push_failures": git_failures[-5:],
                "updated_at": utc_now(),
            },
            self.config.io.max_file_bytes,
        )
        dst = f"{self.config.relay.server_return_dir.rstrip('/')}/{request.request_id}"
        try:
            self.transport.push_dir(result_dir, self.config.relay.server_ssh, dst)
        except Exception as exc:
            write_json(
                result_dir / "direct_return_error.json",
                {
                    "request_id": request.request_id,
                    "return_error": str(exc),
                    "updated_at": utc_now(),
                },
                self.config.io.max_file_bytes,
            )

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        self.git.ensure_worktree()
        self.git.configure_identity()
        head = self.git.rev_parse("HEAD")
        cursor = read_json(self._cursor_path) if self._cursor_path.is_file() else {}
        candidate = str(cursor.get("control_ref") or "")
        pending = str(cursor.get("pending_ref") or "")
        completed = cursor.get("completed_request_paths", [])
        if (
            candidate
            and str(cursor.get("control_branch") or "") == self.config.repo.branch
            and str(cursor.get("generation") or "") == self.config.endpoint.generation
        ):
            try:
                self.git.rev_parse(candidate)
            except GitError:
                candidate = ""
            else:
                if not self.git.has_common_ancestor(candidate, head):
                    candidate = ""
        self._last_ref = candidate or head
        self._pending_ref = pending or None
        self._completed_request_paths = (
            {str(path) for path in completed if str(path)}
            if pending and isinstance(completed, list)
            else set()
        )
        if not candidate:
            self._write_cursor(head)
        self._initialized = True

    def _begin_cursor_batch(self, ref: str) -> None:
        if self._pending_ref == ref:
            return
        self._pending_ref = ref
        self._completed_request_paths = set()
        self._write_cursor(str(self._last_ref or ref))

    def _mark_request_completed(self, request_path: str) -> None:
        if not self._pending_ref:
            return
        self._completed_request_paths.add(str(request_path))
        self._write_cursor(str(self._last_ref or self._pending_ref))

    def _adopt_newer_control_ref(self, dispatched_ref: str) -> bool:
        latest_ref = self.git.fetch()
        if latest_ref == dispatched_ref:
            return False
        self._pending_ref = latest_ref
        self._write_cursor(str(self._last_ref or latest_ref))
        return True

    def _advance_cursor(self, ref: str) -> None:
        self._last_ref = ref
        self._pending_ref = None
        self._completed_request_paths = set()
        self._write_cursor(ref)

    def _write_cursor(self, ref: str) -> None:
        write_json(
            self._cursor_path,
            {
                "schema": "gitpartner.direct-control-cursor.v2",
                "control_branch": self.config.repo.branch,
                "control_ref": ref,
                "pending_ref": self._pending_ref or "",
                "completed_request_paths": sorted(
                    self._completed_request_paths
                ),
                "generation": self.config.endpoint.generation,
                "updated_at": utc_now(),
            },
            self.config.io.max_file_bytes,
        )

    def _write_direct_status(
        self,
        result_dir: Path,
        request: ExecutionRequest,
        payload: dict[str, object],
    ) -> None:
        status = {
            **request_status_metadata(request),
            "transport": "direct",
            "request_id": request.request_id,
            "claimed_by": _actor_name(),
            "updated_at": utc_now(),
            **payload,
        }
        write_json(
            result_dir / "status.json",
            status,
            self.config.io.max_file_bytes,
        )
        write_compact_receipt(
            result_dir,
            status,
            self.config.io.max_file_bytes,
        )

    def _result_repo_path(self, result_dir: Path) -> str:
        return result_dir.resolve().relative_to(
            self.config.repo_dir.resolve()
        ).as_posix()

    def _coalesce_direct_control_probe_status(
        self,
        request: ExecutionRequest,
    ) -> bool:
        return (
            effective_transport(self.config, request) == "direct"
            and (
                request.request_kind == "control-probe"
                or self._is_background_watch(request)
            )
        )

    def _terminal_status_matches_attempt(
        self,
        request: ExecutionRequest,
        status: dict[str, object],
    ) -> bool:
        if not request.attempt_id:
            return False
        expected = request_status_metadata(request)
        return all(key in status and status[key] == value for key, value in expected.items())

    def _payload_root(self, request: ExecutionRequest) -> Path:
        if request.payload_root:
            path = Path(request.payload_root)
            if not path.is_absolute():
                path = self.config.repo_dir / path
            return path.resolve()
        return self.config.repo_dir


class DirectGitProxy:
    def __init__(self, git: GitClient) -> None:
        self.git = git
        self.failures: list[str] = []

    def commit_and_push(self, paths: list[str], message: str, max_file_bytes: int) -> bool:
        try:
            return self.git.commit_and_push(paths, message, max_file_bytes)
        except Exception as exc:
            self.failures.append(str(exc))
            return False


class StopFlag:
    def __init__(self) -> None:
        self.requested = False

    def handle(self, _signum: int, _frame: object) -> None:
        self.requested = True


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


def _process_group_kwargs() -> dict[str, object]:
    if os.name == "nt":
        return {}
    return {"start_new_session": True}


def _process_payload(process: subprocess.Popen[str]) -> dict[str, object]:
    payload: dict[str, object] = {
        "pid": process.pid,
        "start_token": process_start_token(process.pid),
    }
    if os.name != "nt":
        try:
            payload["pgid"] = os.getpgid(process.pid)
        except OSError:
            pass
    return payload


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


def _final_state(exit_code: int, error: str | None) -> str:
    if exit_code == -2:
        return "cancelled"
    if exit_code == 0 and error is None:
        return "success"
    return "failed"


def _existing_cancel_reason(result_dir: Path) -> dict[str, str]:
    status_path = result_dir / "status.json"
    if not status_path.exists():
        return {}
    try:
        raw = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    reason = raw.get("cancel_reason")
    if not reason:
        return {}
    return {"cancel_reason": str(reason)}


def _relay_cancel_reason(task_dir: Path, request: ExecutionRequest) -> str | None:
    for name in ("cancel.json", "CANCEL.json"):
        path = task_dir / name
        if not path.exists():
            continue
        try:
            return _cancel_reason_from_text(path.read_text(encoding="utf-8"), request)
        except (OSError, json.JSONDecodeError):
            return None
    return None


def _cancel_reason_from_text(text: str | None, request: ExecutionRequest) -> str | None:
    if not text:
        return None
    raw = json.loads(text)
    if not isinstance(raw, dict) or raw.get("cancel") is not True:
        return None
    targets = _cancel_targets(raw)
    target = str(raw.get("request_id") or raw.get("id") or "*")
    if target != "*":
        targets.add(target)
    if "*" not in targets and request.request_id not in targets:
        return None
    return str(raw.get("reason") or "remote cancellation requested")[:1000]


def _cancel_targets(raw: dict[str, object]) -> set[str]:
    targets: set[str] = set()
    value = raw.get("request_ids")
    if isinstance(value, list):
        targets.update(str(item) for item in value)
    return targets


def _request_id_from_text(text: str | None) -> str | None:
    if not text:
        return None
    raw = json.loads(text)
    if not isinstance(raw, dict):
        return None
    value = raw.get("id") or raw.get("request_id")
    if not value:
        return None
    return _normalize_request_id(str(value))


def _normalize_request_id(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "._-" else "_" for char in value)
    return cleaned.strip("._-") or "request"


def _local_path(base: Path, path: str) -> Path:
    candidate = Path(os.path.expanduser(path))
    if candidate.is_absolute():
        return candidate
    return safe_join(base, str(candidate))


def _actor_name() -> str:
    user = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
    return f"{user}@{socket.gethostname()}"


def _configure_auto_direct_fast_fail(config: AppConfig) -> None:
    configure_direct_git_fast_fail(config.relay.transport_mode)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="path to config JSON")
    parser.add_argument("--once", help="run one request id from relay.client_inbox_dir")
    args = parser.parse_args()

    client = RelayClient(Path(args.config).resolve())
    if args.once:
        raise SystemExit(client.run_once(args.once))
    resident_lock = ResidentRoleLock(
        client.config.repo_dir / client.config.io.state_dir,
        "client",
    )
    resident_lock.acquire()
    try:
        client.run_daemon()
    finally:
        resident_lock.release()


if __name__ == "__main__":
    main()
