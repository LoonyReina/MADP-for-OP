from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import signal
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from limited_remote_partner.maintenance.auto_update import (
    consume_pending_reexec,
    reexec_partner_role,
    should_reexec_for_update,
)
from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.core.request import ExecutionRequest
from limited_remote_partner.gateway.compact_receipt import write_compact_receipt
from limited_remote_partner.gateway.exchange_sync import push_exchange_if_changed
from limited_remote_partner.gateway.git_client import GitClient
from limited_remote_partner.gateway.input_parser import (
    changed_request_paths,
    parse_request,
    request_identity,
    request_status_metadata,
)
from limited_remote_partner.core.loop_backoff import LoopErrorBackoff
from limited_remote_partner.endpoint.node_lifecycle import collect_relay_node_reports
from limited_remote_partner.core.process_utils import matching_partner_role_pids, process_start_token
from limited_remote_partner.core.resident_lock import ResidentRoleLock
from limited_remote_partner.gateway.relay import (
    RELAY_PROTOCOL_VERSION,
    RelayError,
    ScpTransport,
    copy_requested_paths,
    effective_transport,
    read_json,
    replace_tree,
    utc_now,
    write_json,
    write_request,
)
from limited_remote_partner.engine.scheduler import should_execute
from limited_remote_partner.core.targeting import route_request_for_current_runtime


@dataclass(frozen=True)
class RelayReturn:
    path: Path
    source: str


TERMINAL_CLIENT_STATES = {
    "success",
    "failed",
    "return_failed",
    "cancelled",
    "stalled",
    "abandoned",
}
RELAY_STATE_HISTORY_LIMIT = 64
DIRECT_ACTIVE_STATES = {"claimed", "running", "success", "failed"}
WAITING_RELAY_RECOVERY_INTERVAL_SECONDS = 300
WAITING_RELAY_INITIAL_RECOVERY_GRACE_SECONDS = 30
WAITING_RELAY_ACTIVE_CLIENT_REFRESH_SECONDS = 120
WAITING_RELAY_RECOVERY_RETRY_SECONDS = 30
RELAY_BUNDLE_PUBLISH_ATTEMPTS = 2
ENGINE_RELAY_REQUEST_KINDS = {
    "engine-admission",
    "engine-exchange",
    "engine-snapshot",
    "engine-collect",
    "engine-return-ack",
    "engine-capacity",
}
PARALLEL_CANARY_REQUEST_KINDS = {
    "host-only-canary",
    "engine-host-canary",
    "engine-device-canary",
}


def _engine_relay_atomic_completion(request: ExecutionRequest) -> bool:
    return bool(
        request.relay_protocol_version == RELAY_PROTOCOL_VERSION
        and (
            (
                request.request_kind in ENGINE_RELAY_REQUEST_KINDS
                and request.completion_mode in {"accepted", "snapshot"}
            )
            or (
                request.request_kind == "control-probe"
                and request.completion_mode == "terminal"
            )
        )
    )


def _server_code_generation() -> str:
    digest = hashlib.sha256()
    package_root = Path(__file__).resolve().parents[1]
    for relative in (
        "gateway/server.py",
        "maintenance/lan_ops.py",
        "gateway/git_lock.py",
        "gateway/relay.py",
        "core/process_utils.py",
        "core/resident_lock.py",
    ):
        path = package_root / relative
        digest.update(relative.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


SERVER_CODE_GENERATION = _server_code_generation()


def _server_process_status(config: AppConfig | None = None) -> dict[str, object]:
    current_generation = _server_code_generation()
    resident_pids = matching_partner_role_pids(
        "server",
        repo_dir=config.repo_dir if config is not None else None,
    )
    lock_owner: dict[str, object] = {}
    if config is not None:
        lock_owner = _read_status_if_present(
            config.repo_dir
            / config.io.state_dir
            / "resident-server.lock.d"
            / "owner.json"
        )
    active_pid = int(lock_owner.get("pid") or 0)
    if active_pid not in resident_pids:
        active_pid = resident_pids[0] if len(resident_pids) == 1 else 0
    return {
        "pid": active_pid or None,
        "action_pid": os.getpid(),
        "action_start_token": process_start_token(os.getpid()),
        "resident_pids": resident_pids,
        "standby_pids": [pid for pid in resident_pids if pid != active_pid],
        "resident_lock_owner": lock_owner,
        "loaded_code_generation": SERVER_CODE_GENERATION,
        "current_code_generation": current_generation,
        "code_generation_current": current_generation == SERVER_CODE_GENERATION,
    }


class RelayServer:
    def __init__(self, git: GitClient, config: AppConfig) -> None:
        self.git = git
        self.config = config
        self.repo_dir = config.repo_dir
        self.transport = ScpTransport(config)
        self._relay_pull_not_before: dict[str, float] = {}
        self._relay_return_sources: dict[str, str] = {}
        self._server_action_guard = threading.Lock()
        self._server_action_threads: dict[str, threading.Thread] = {}
        self._waiting_index_path = (
            self.repo_dir
            / self.config.io.state_dir
            / "relay_waiting_index.json"
        )
        self._waiting_outputs = self._rebuild_waiting_index()

    def run_request(self, request: ExecutionRequest, trigger_ref: str, *, wait: bool = True) -> int:
        result_dir = self.repo_dir / self.config.io.output_dir / request.output_subdir
        existing = _read_status_if_present(result_dir / "status.json")
        if _same_waiting_dispatch(existing, request, trigger_ref) and not wait:
            self._mark_waiting(request.request_id, request.output_subdir)
            return 0
        if _same_terminal_dispatch(existing, request, trigger_ref):
            return int(existing.get("exit_code", 0))
        server_action_recovery: dict[str, object] = {}
        if request.server_action and _same_running_server_action(
            existing,
            request,
            trigger_ref,
        ):
            if _server_action_owner_alive(existing):
                return 0
            server_action_recovery = {
                "recovered_at": utc_now(),
                "recovered_from_action_pid": _server_action_pid(existing),
                "recovered_from_action_start_token": _server_action_start_token(
                    existing
                ),
            }
        started_at = utc_now()
        if request.server_action:
            if not wait:
                return self._schedule_server_action(
                    request,
                    trigger_ref,
                    result_dir,
                    started_at,
                    recovery=server_action_recovery,
                )
            return self._run_server_action(
                request,
                trigger_ref,
                result_dir,
                started_at,
                recovery=server_action_recovery,
            )

        transport = effective_transport(self.config, request)
        if transport in {"direct", "auto"}:
            direct_status = self._wait_for_direct_claim(request)
            if direct_status:
                state = str(direct_status.get("state", ""))
                return int(direct_status.get("exit_code", 0 if state != "failed" else 1))
            if transport == "direct":
                self._write_status(
                    result_dir,
                    {
                        "state": "failed",
                        "phase": "direct-claim",
                        "transport": "direct",
                        "request_id": request.request_id,
                        "trigger_ref": trigger_ref,
                        "started_at": started_at,
                        "finished_at": utc_now(),
                        "exit_code": 1,
                        "error": "no direct client claim observed before grace timeout",
                    },
                )
                self._sync(
                    f"git_partner_server: {request.request_id} direct claim missing",
                    result_dir,
                )
                return 1

        self._write_status(
            result_dir,
            {
                "state": "dispatching",
                "transport": "relay",
                **_relay_request_metadata(request),
                "request_id": request.request_id,
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "payload_paths": list(request.payload_paths),
                "return_paths": list(request.return_paths),
            },
        )
        defer_dispatch_status_sync = _engine_relay_atomic_completion(request)
        dispatch_status_sync_seconds = 0.0
        if not defer_dispatch_status_sync:
            dispatch_status_sync_started = time.monotonic()
            self._sync(
                f"git_partner_server: {request.request_id} dispatching",
                result_dir,
            )
            dispatch_status_sync_seconds = time.monotonic() - dispatch_status_sync_started

        try:
            bundle_prepare_started = time.monotonic()
            bundle_dir = self._bundle_dir(request.request_id)
            if bundle_dir.exists():
                shutil.rmtree(bundle_dir)
            bundle_dir.mkdir(parents=True, exist_ok=True)
            write_request(bundle_dir / "request.json", request, self.config.io.max_file_bytes)
            copied_payload, _missing = copy_requested_paths(
                self._payload_root(request),
                bundle_dir / "payload",
                request.payload_paths,
                self.config.io.max_file_bytes,
            )
            bundle_prepare_seconds = time.monotonic() - bundle_prepare_started

            dispatch_info = self._push_relay_bundle(request, bundle_dir)
            if (
                request.relay_protocol_version == RELAY_PROTOCOL_VERSION
                and self.config.relay.relay_v2_return_mode == "push-atomic"
            ):
                self._relay_pull_not_before[request.request_id] = (
                    time.monotonic()
                    + self.config.relay.relay_v2_push_grace_seconds
                )
            self._write_status(
                result_dir,
                {
                    "state": "waiting-client",
                    "transport": "relay",
                    **_relay_request_metadata(request),
                    "request_id": request.request_id,
                    "trigger_ref": trigger_ref,
                    "started_at": started_at,
                    "dispatched_at": utc_now(),
                    "payload_paths": copied_payload,
                    "return_paths": list(request.return_paths),
                    "client_ssh": self.config.relay.client_ssh or "local",
                    "client_inbox_dir": self.config.relay.client_inbox_dir,
                    "relay_return_mode": self.config.relay.relay_v2_return_mode,
                    "dispatch_status_sync_seconds": f"{dispatch_status_sync_seconds:.6f}",
                    "dispatch_status_sync_deferred": defer_dispatch_status_sync,
                    "waiting_status_sync_deferred": defer_dispatch_status_sync,
                    "bundle_prepare_seconds": f"{bundle_prepare_seconds:.6f}",
                    **dispatch_info,
                },
            )
            self._mark_waiting(request.request_id, request.output_subdir)
            if not defer_dispatch_status_sync:
                self._sync(
                    f"git_partner_server: {request.request_id} waiting client",
                    result_dir,
                )
            if not wait:
                return 0

            returned = self._wait_for_return(request)
            returned_dir = returned.path
            client_status = read_json(returned_dir / "status.json")
            replace_tree(returned_dir, result_dir / "client_output")
            (returned_dir / "PUBLISHED").write_text(utc_now() + "\n", encoding="utf-8")
            exit_code = int(client_status.get("exit_code", 1))
            collected_at = utc_now()
            self._write_status(
                result_dir,
                {
                    **_read_status_if_present(result_dir / "status.json"),
                    "state": "success" if exit_code == 0 else "failed",
                    "transport": "relay",
                    **_relay_request_metadata(request),
                    "request_id": request.request_id,
                    "trigger_ref": trigger_ref,
                    "started_at": started_at,
                    "finished_at": collected_at,
                    "exit_code": exit_code,
                    "client_state": str(client_status.get("state", "")),
                    "client_started_at": str(client_status.get("started_at", "")),
                    "client_finished_at": str(client_status.get("finished_at", "")),
                    "client_returned_at": str(client_status.get("returned_at", "")),
                    "return_collected_at": collected_at,
                    "client_return_source": returned.source,
                    "client_status": "client_output/status.json",
                    "client_output": "client_output",
                },
            )
            self._sync(
                f"git_partner_server: {request.request_id} finished",
                result_dir,
            )
            self._relay_pull_not_before.pop(request.request_id, None)
            self._return_ready_marker(request.request_id).unlink(missing_ok=True)
            self._return_publishing_marker(request.request_id).unlink(missing_ok=True)
            return exit_code
        except Exception as exc:
            relay_diagnostic = self._relay_failure_diagnostic(request)
            self._write_status(
                result_dir,
                {
                    **_read_status_if_present(result_dir / "status.json"),
                    "state": "failed",
                    "phase": "dispatch" if not (result_dir / "client_output").exists() else "return",
                    "transport": "relay",
                    "request_id": request.request_id,
                    "trigger_ref": trigger_ref,
                    "started_at": started_at,
                    "finished_at": utc_now(),
                    "exit_code": 1,
                    "error": str(exc),
                    "relay_diagnostic": relay_diagnostic,
                },
            )
            self._sync(
                f"git_partner_server: {request.request_id} failed",
                result_dir,
            )
            return 1

    def _schedule_server_action(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
        result_dir: Path,
        started_at: str,
        *,
        recovery: dict[str, object] | None = None,
    ) -> int:
        with self._server_action_guard:
            current = self._server_action_threads.get(request.request_id)
            if current is not None and current.is_alive():
                return 0
            worker = threading.Thread(
                target=self._run_scheduled_server_action,
                args=(request, trigger_ref, result_dir, started_at),
                kwargs={"recovery": recovery},
                name=f"gp-server-action-{request.request_id[:48]}",
                daemon=True,
            )
            self._server_action_threads[request.request_id] = worker
            worker.start()
        return 0

    def _run_scheduled_server_action(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
        result_dir: Path,
        started_at: str,
        *,
        recovery: dict[str, object] | None = None,
    ) -> None:
        try:
            self._run_server_action(
                request,
                trigger_ref,
                result_dir,
                started_at,
                recovery=recovery,
            )
        finally:
            with self._server_action_guard:
                current = self._server_action_threads.get(request.request_id)
                if current is threading.current_thread():
                    self._server_action_threads.pop(request.request_id, None)

    def run_requests_batch(
        self,
        requests: list[ExecutionRequest],
        trigger_ref: str,
        *,
        wait: bool = False,
    ) -> int:
        if wait or len(requests) < 2:
            return max(
                (
                    self.run_request(request, trigger_ref, wait=wait)
                    for request in requests
                ),
                default=0,
            )
        if not all(_parallel_dispatch_safe(self.config, request) for request in requests):
            raise RelayError("unsafe request entered relay batch dispatch")

        prepared: list[dict[str, object]] = []
        exit_code = 0
        for request in requests:
            result_dir = (
                self.repo_dir
                / self.config.io.output_dir
                / request.output_subdir
            )
            existing = _read_status_if_present(result_dir / "status.json")
            if _same_waiting_dispatch(existing, request, trigger_ref):
                continue
            if _same_terminal_dispatch(existing, request, trigger_ref):
                exit_code = max(exit_code, int(existing.get("exit_code", 0)))
                continue
            started_at = utc_now()
            self._write_status(
                result_dir,
                {
                    "state": "dispatching",
                    "transport": "relay",
                    **_relay_request_metadata(request),
                    "request_id": request.request_id,
                    "trigger_ref": trigger_ref,
                    "started_at": started_at,
                    "payload_paths": list(request.payload_paths),
                    "return_paths": list(request.return_paths),
                },
            )
            bundle_started = time.monotonic()
            bundle_dir = self._bundle_dir(request.request_id)
            if bundle_dir.exists():
                shutil.rmtree(bundle_dir)
            bundle_dir.mkdir(parents=True, exist_ok=True)
            write_request(
                bundle_dir / "request.json",
                request,
                self.config.io.max_file_bytes,
            )
            copied_payload, _missing = copy_requested_paths(
                self._payload_root(request),
                bundle_dir / "payload",
                request.payload_paths,
                self.config.io.max_file_bytes,
            )
            prepared.append(
                {
                    "request": request,
                    "result_dir": result_dir,
                    "bundle_dir": bundle_dir,
                    "started_at": started_at,
                    "copied_payload": copied_payload,
                    "bundle_prepare_seconds": time.monotonic() - bundle_started,
                }
            )
        if not prepared:
            return exit_code

        try:
            dispatch_rows = self._push_relay_bundle_batch(
                [
                    (row["request"], row["bundle_dir"])
                    for row in prepared
                ]
            )
            dispatched_at = utc_now()
            for row in prepared:
                request = row["request"]
                result_dir = row["result_dir"]
                assert isinstance(request, ExecutionRequest)
                assert isinstance(result_dir, Path)
                if (
                    request.relay_protocol_version == RELAY_PROTOCOL_VERSION
                    and self.config.relay.relay_v2_return_mode == "push-atomic"
                ):
                    self._relay_pull_not_before[request.request_id] = (
                        time.monotonic()
                        + self.config.relay.relay_v2_push_grace_seconds
                    )
                self._write_status(
                    result_dir,
                    {
                        "state": "waiting-client",
                        "transport": "relay",
                        **_relay_request_metadata(request),
                        "request_id": request.request_id,
                        "trigger_ref": trigger_ref,
                        "started_at": row["started_at"],
                        "dispatched_at": dispatched_at,
                        "payload_paths": row["copied_payload"],
                        "return_paths": list(request.return_paths),
                        "client_ssh": self.config.relay.client_ssh or "local",
                        "client_inbox_dir": self.config.relay.client_inbox_dir,
                        "relay_return_mode": self.config.relay.relay_v2_return_mode,
                        "dispatch_status_sync_seconds": "0.000000",
                        "dispatch_status_sync_deferred": True,
                        "waiting_status_sync_deferred": True,
                        "bundle_prepare_seconds": (
                            f"{float(row['bundle_prepare_seconds']):.6f}"
                        ),
                        **dispatch_rows[request.request_id],
                    },
                )
                self._mark_waiting(request.request_id, request.output_subdir)
            return exit_code
        except Exception as exc:
            for row in prepared:
                request = row["request"]
                result_dir = row["result_dir"]
                assert isinstance(request, ExecutionRequest)
                assert isinstance(result_dir, Path)
                self._write_status(
                    result_dir,
                    {
                        **_read_status_if_present(result_dir / "status.json"),
                        "state": "failed",
                        "phase": "batch-dispatch",
                        "transport": "relay",
                        "request_id": request.request_id,
                        "trigger_ref": trigger_ref,
                        "started_at": row["started_at"],
                        "finished_at": utc_now(),
                        "exit_code": 1,
                        "error": str(exc),
                    },
                )
                self._sync(
                    f"git_partner_server: {request.request_id} batch dispatch failed",
                    result_dir,
                )
            return 1

    def reject_request(
        self,
        trigger_ref: str,
        error: Exception,
        request_file: str | Path | None = None,
    ) -> None:
        request_id, output_subdir = request_identity(
            self.config,
            trigger_ref,
            request_file,
        )
        result_dir = self.repo_dir / self.config.io.output_dir / output_subdir
        self._write_status(
            result_dir,
            {
                "state": "failed",
                "phase": "parse-request",
                "transport": "server-local",
                "request_id": request_id,
                "trigger_ref": trigger_ref,
                "finished_at": utc_now(),
                "exit_code": 1,
                "error": str(error),
            },
        )
        self._sync(
            f"git_partner_server: {request_id} request rejected",
            result_dir,
        )

    def _run_server_action(
        self,
        request: ExecutionRequest,
        trigger_ref: str,
        result_dir: Path,
        started_at: str,
        *,
        recovery: dict[str, object] | None = None,
    ) -> int:
        action = str(request.server_action)
        args = _server_action_namespace(request.server_action_args)
        target_role = args.target_role
        args.remote_config = _registered_remote_config(
            self.config,
            target_role,
            args.remote_config,
        )
        target_host = args.target_host or _default_action_target_host(self.config, target_role)
        target_dir = args.target_dir or _default_action_target_dir(self.config, target_role)
        visible_args = {
            "target_role": target_role,
            "target_host": target_host,
            "target_dir": target_dir,
            "remote_config": args.remote_config,
            "service_name": args.service_name,
            "remote_staging_dir": args.remote_staging_dir,
            "sync_path": list(args.sync_path),
            "no_process_fallback": str(args.no_process_fallback).lower(),
            "cleanup_request_id": args.cleanup_request_id,
            "diagnose_request_id": args.diagnose_request_id,
            "cancel_request_id": args.cancel_request_id,
            "cancel_reason": args.cancel_reason,
            "node_ack_json": args.node_ack_json,
            "tmux_session": args.tmux_session,
            "artifact_profile": args.artifact_profile,
            "endpoint_action": args.endpoint_action,
            "source_repo": args.source_repo,
            "worktree": args.worktree,
            "control_branch": args.control_branch,
            "endpoint_config": args.endpoint_config,
            "endpoint_role": args.endpoint_role,
            "endpoint_remote": args.endpoint_remote,
            "import_login_network_env": args.import_login_network_env,
        }
        self._write_status(
            result_dir,
            {
                "state": "running",
                "transport": "server-local",
                "request_id": request.request_id,
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "server_action": action,
                "server_action_args": visible_args,
                "server_process": _server_process_status(self.config),
                **({"server_action_recovery": recovery} if recovery else {}),
            },
        )
        self._sync(
            f"git_partner_server: {request.request_id} {action} running",
            result_dir,
        )
        action_log: list[dict[str, object]] = []
        try:
            from limited_remote_partner.maintenance.lan_ops import (
                cancel_request,
                diagnose_peer,
                endpoint_runtime_command,
                inspect_artifact_profile,
                restart_service,
                schedule_duplicate_service_reconciliation,
                schedule_local_restart_service,
                start_tmux_command,
                sync_artifact_profile,
                sync_code,
                write_node_ack,
            )

            if action in {"lan-bootstrap", "lan-sync-code"}:
                _append_action_log(
                    action_log,
                    sync_code(self.config, self.transport, target_host, target_dir, args),
                )
            if action in {"lan-bootstrap", "lan-restart-service"}:
                if target_role == "server" and not target_host:
                    _append_action_log(
                        action_log,
                        schedule_local_restart_service(
                            self.transport,
                            target_dir,
                            target_role,
                            args,
                            request.request_id,
                        ),
                    )
                else:
                    _append_action_log(
                        action_log,
                        restart_service(
                            self.transport,
                            target_host,
                            target_dir,
                            target_role,
                            args,
                        ),
                    )
            if action == "lan-cancel-request":
                _append_action_log(
                    action_log,
                    cancel_request(
                        self.transport,
                        target_host,
                        target_dir,
                        target_role,
                        args,
                    ),
                )
            if action == "lan-reconcile-service":
                if target_host:
                    raise RelayError(
                        "lan-reconcile-service only supports the local server role"
                    )
                _append_action_log(
                    action_log,
                    schedule_duplicate_service_reconciliation(
                        self.transport,
                        target_dir,
                        target_role,
                        request.request_id,
                    ),
                )
            if action == "lan-diagnose":
                diagnostic = diagnose_peer(
                    self.transport,
                    target_host,
                    target_dir,
                    target_role,
                    request_id=args.diagnose_request_id,
                )
                _append_action_log(
                    action_log,
                    diagnostic,
                )
                if not diagnostic.get("reachable"):
                    raise RelayError(str(diagnostic.get("error") or "LAN peer is unreachable"))
            if action == "server-tmux-command":
                _append_action_log(
                    action_log,
                    start_tmux_command(
                        self.transport,
                        target_host,
                        target_dir,
                        target_role,
                        args,
                        request.request_id,
                    ),
                )
            if action == "lan-inspect-artifact":
                _append_action_log(
                    action_log,
                    inspect_artifact_profile(
                        target_dir,
                        args,
                    ),
                )
            if action == "lan-sync-artifact":
                _append_action_log(
                    action_log,
                    sync_artifact_profile(
                        self.transport,
                        target_host,
                        target_dir,
                        target_role,
                        args,
                        request.request_id,
                    ),
                )
            if action == "lan-node-ack":
                _append_action_log(
                    action_log,
                    write_node_ack(
                        self.transport,
                        target_host,
                        target_dir,
                        target_role,
                        args,
                    ),
                )
            if action == "endpoint-runtime":
                _append_action_log(
                    action_log,
                    endpoint_runtime_command(
                        self.transport,
                        target_host,
                        target_dir,
                        target_role,
                        args,
                    ),
                )
        except Exception as exc:
            self._write_status(
                result_dir,
                {
                    "state": "failed",
                    "transport": "server-local",
                    "request_id": request.request_id,
                    "trigger_ref": trigger_ref,
                    "started_at": started_at,
                    "finished_at": utc_now(),
                    "exit_code": 1,
                    "server_action": action,
                    "server_action_args": visible_args,
                    "action_log": action_log,
                    "server_process": _server_process_status(self.config),
                    "error": str(exc),
                },
            )
            self._sync(
                f"git_partner_server: {request.request_id} {action} failed",
                result_dir,
            )
            return 1
        self._write_status(
            result_dir,
            {
                "state": "success",
                "transport": "server-local",
                "request_id": request.request_id,
                "trigger_ref": trigger_ref,
                "started_at": started_at,
                "finished_at": utc_now(),
                "exit_code": 0,
                "server_action": action,
                "server_action_args": visible_args,
                "action_log": action_log,
                "server_process": _server_process_status(self.config),
            },
        )
        self._sync(
            f"git_partner_server: {request.request_id} {action} finished",
            result_dir,
        )
        return 0

    def _wait_for_direct_claim(self, request: ExecutionRequest) -> dict[str, object] | None:
        deadline = time.monotonic() + self.config.relay.direct_claim_grace_seconds
        while True:
            status = self._direct_claim_status(request)
            if status:
                return status
            if time.monotonic() >= deadline:
                return None
            try:
                remote_ref = self.git.fetch()
                if self.git.rev_parse("HEAD") != remote_ref:
                    self.git.checkout_remote_head()
            except Exception:
                return None
            time.sleep(self.config.relay.direct_claim_poll_seconds)

    def _direct_claim_status(self, request: ExecutionRequest) -> dict[str, object] | None:
        status_path = (
            self.repo_dir
            / self.config.io.output_dir
            / request.output_subdir
            / "status.json"
        )
        if not status_path.exists():
            return None
        try:
            status = read_json(status_path)
        except Exception:
            return None
        if status.get("request_id") != request.request_id:
            return None
        if status.get("transport") != "direct":
            return None
        if str(status.get("state", "")) not in DIRECT_ACTIVE_STATES:
            return None
        return status

    def publish_direct_returns(self) -> int:
        return_base = self._return_base_dir()
        if not return_base.exists():
            return 0
        published = 0
        for item in sorted(return_base.iterdir()):
            if not item.is_dir() or (item / "PUBLISHED").exists():
                continue
            marker = item / "direct_return.json"
            status = item / "status.json"
            if not marker.exists() and not status.exists():
                continue
            try:
                if marker.exists():
                    payload = read_json(marker)
                    output_subdir = str(payload.get("output_subdir") or item.name)
                    message = f"git_partner_server: {item.name} direct return published"
                else:
                    client_status = read_json(status)
                    if str(client_status.get("state", "")) not in TERMINAL_CLIENT_STATES:
                        continue
                    if self._has_waiting_relay_request(item.name):
                        continue
                    output_subdir = item.name
                    message = f"git_partner_server: {item.name} relay return published"
                result_dir = self.repo_dir / self.config.io.output_dir / output_subdir
                replace_tree(item, result_dir)
                (item / "PUBLISHED").write_text(utc_now() + "\n", encoding="utf-8")
                self._sync(message, result_dir)
                published += 1
            except Exception as exc:
                print(f"git-partner-server return publish error for {item}: {exc}", flush=True)
        return published

    def _has_waiting_relay_request(self, request_id: str) -> bool:
        return request_id in self._waiting_outputs.values()

    def poll_waiting_relay_returns(self) -> int:
        output_base = self.repo_dir / self.config.io.output_dir
        if not output_base.exists():
            return 0

        terminal_rows: list[
            tuple[
                ExecutionRequest,
                Path,
                dict[str, object],
                Path,
                str,
            ]
        ] = []
        waiting_by_batch: dict[str, int] = {}
        for status_path in self._waiting_status_paths():
            result_dir = status_path.parent
            output_subdir = result_dir.relative_to(output_base).as_posix()
            try:
                status = read_json(status_path)
            except Exception:
                continue
            if not _status_needs_relay_collection(status, output_subdir):
                continue

            request = _request_from_waiting_status(status, output_subdir)
            batch_id = str(status.get("relay_batch_id") or "")
            if batch_id:
                waiting_by_batch[batch_id] = waiting_by_batch.get(batch_id, 0) + 1
            returned_dir = self._return_dir(request.request_id)
            if self._try_pull_client_result(request, returned_dir):
                client_state = self._client_state(returned_dir)
                if client_state in TERMINAL_CLIENT_STATES:
                    source = self._relay_return_sources.pop(
                        request.request_id,
                        "server-pullback-daemon",
                    )
                    terminal_rows.append(
                        (
                            request,
                            result_dir,
                            status,
                            returned_dir,
                            source,
                        )
                    )
                elif client_state:
                    self._publish_waiting_client_progress(
                        request,
                        result_dir,
                        status,
                        returned_dir,
                        client_state,
                    )
                continue

            if _waiting_recovery_due(status):
                self._recover_waiting_client(request)

        ready_by_batch: dict[str, list[tuple[ExecutionRequest, Path, dict[str, object], Path, str]]] = {}
        immediate: list[
            tuple[ExecutionRequest, Path, dict[str, object], Path, str]
        ] = []
        for row in terminal_rows:
            batch_id = str(row[2].get("relay_batch_id") or "")
            if batch_id:
                ready_by_batch.setdefault(batch_id, []).append(row)
            else:
                immediate.append(row)
        for batch_id, rows in ready_by_batch.items():
            expected = max(
                int(row[2].get("relay_batch_size") or 1)
                for row in rows
            )
            waiting = waiting_by_batch.get(batch_id, len(rows))
            if len(rows) < min(expected, waiting) and not _relay_batch_grace_elapsed(
                rows,
                self.config.relay.relay_v2_push_grace_seconds,
            ):
                continue
            immediate.extend(rows)
        if not immediate:
            return 0

        completed_rows: list[tuple[str, Path, Path]] = []
        for request, result_dir, status, returned_dir, source in immediate:
            self._finish_waiting_relay_result(
                result_dir,
                status,
                returned_dir,
                source,
            )
            completed_rows.append((request.request_id, result_dir, returned_dir))

        publish_batch_size = len(completed_rows)
        for _request_id, result_dir, _returned_dir in completed_rows:
            status = _read_status_if_present(result_dir / "status.json")
            self._write_status(
                result_dir,
                {
                    **status,
                    "result_publish_batch_size": publish_batch_size,
                    "result_publish_batched_at": utc_now(),
                },
            )
        self._sync_many(
            "git_partner_server: relay result collected batch "
            + ",".join(request_id for request_id, _result, _returned in completed_rows),
            [result_dir for _request, result_dir, _returned in completed_rows],
        )
        for request_id, _result_dir, returned_dir in completed_rows:
            (returned_dir / "PUBLISHED").write_text(
                utc_now() + "\n",
                encoding="utf-8",
            )
            self._relay_pull_not_before.pop(request_id, None)
            self._return_ready_marker(request_id).unlink(missing_ok=True)
            self._return_publishing_marker(request_id).unlink(missing_ok=True)
            self._unmark_waiting(request_id)
        return publish_batch_size

    def _rebuild_waiting_index(self) -> dict[str, str]:
        output_base = self.repo_dir / self.config.io.output_dir
        waiting: dict[str, str] = {}
        if output_base.exists():
            for status_path in _iter_output_status_paths(output_base):
                try:
                    status = read_json(status_path)
                except Exception:
                    continue
                output_subdir = status_path.parent.relative_to(output_base).as_posix()
                if not _status_needs_relay_collection(status, output_subdir):
                    continue
                request_id = str(status.get("request_id") or "")
                if request_id:
                    waiting[output_subdir] = request_id
        self._write_waiting_index(waiting)
        return waiting

    def _waiting_status_paths(self) -> list[Path]:
        output_base = self.repo_dir / self.config.io.output_dir
        paths: list[Path] = []
        stale: list[str] = []
        for output_subdir in sorted(self._waiting_outputs):
            status_path = output_base / output_subdir / "status.json"
            try:
                status = read_json(status_path)
            except Exception:
                stale.append(output_subdir)
                continue
            if not _status_needs_relay_collection(status, output_subdir):
                stale.append(output_subdir)
                continue
            paths.append(status_path)
        if stale:
            for output_subdir in stale:
                self._waiting_outputs.pop(output_subdir, None)
            self._write_waiting_index(self._waiting_outputs)
        return paths

    def _mark_waiting(self, request_id: str, output_subdir: str) -> None:
        if self._waiting_outputs.get(output_subdir) == request_id:
            return
        self._waiting_outputs[output_subdir] = request_id
        self._write_waiting_index(self._waiting_outputs)

    def _unmark_waiting(self, request_id: str) -> None:
        retained = {
            output_subdir: indexed_request_id
            for output_subdir, indexed_request_id in self._waiting_outputs.items()
            if indexed_request_id != request_id
        }
        if len(retained) == len(self._waiting_outputs):
            return
        self._waiting_outputs = retained
        self._write_waiting_index(self._waiting_outputs)

    def _write_waiting_index(self, waiting: dict[str, str]) -> None:
        write_json(
            self._waiting_index_path,
            {
                "protocol_version": "relay-waiting-index-v1",
                "updated_at": utc_now(),
                "entries": waiting,
            },
            self.config.io.max_file_bytes,
        )

    def _finish_waiting_relay_result(
        self,
        result_dir: Path,
        waiting_status: dict[str, object],
        returned_dir: Path,
        source: str,
    ) -> None:
        client_status = read_json(returned_dir / "status.json")
        replace_tree(returned_dir, result_dir / "client_output")
        exit_code = int(client_status.get("exit_code", 1))
        collected_at = utc_now()
        self._write_status(
            result_dir,
            {
                **waiting_status,
                "state": "success" if exit_code == 0 else "failed",
                "finished_at": collected_at,
                "exit_code": exit_code,
                "client_state": str(client_status.get("state", "")),
                "client_started_at": str(client_status.get("started_at", "")),
                "client_finished_at": str(client_status.get("finished_at", "")),
                "client_returned_at": str(client_status.get("returned_at", "")),
                "return_collected_at": collected_at,
                "client_return_source": source,
                "client_status": "client_output/status.json",
                "client_output": "client_output",
            },
        )

    def _publish_waiting_client_progress(
        self,
        request: ExecutionRequest,
        result_dir: Path,
        waiting_status: dict[str, object],
        returned_dir: Path,
        client_state: str,
    ) -> bool:
        client_status = read_json(returned_dir / "status.json")
        client_updated_at = str(client_status.get("updated_at", ""))
        if (
            str(waiting_status.get("client_state", "")) == client_state
            and str(waiting_status.get("client_updated_at", "")) == client_updated_at
        ):
            return False

        self._write_status(
            result_dir,
            {
                **waiting_status,
                "state": "waiting-client",
                "client_state": client_state,
                "client_updated_at": client_updated_at,
                "client_started_at": str(client_status.get("started_at", "")),
                "client_progress_observed_at": utc_now(),
                "client_progress_source": "server-pullback",
            },
        )
        if not _engine_relay_atomic_completion(request):
            self._sync(
                f"git_partner_server: {request.request_id} client progress {client_state}",
                result_dir,
            )
        return True

    def _wait_for_return(self, request: ExecutionRequest) -> RelayReturn:
        returned_dir = self._return_dir(request.request_id)
        deadline = (
            time.monotonic() + request.timeout_seconds
            if request.timeout_seconds > 0
            else None
        )
        next_pull = time.monotonic() + self.config.relay.reverse_log_initial_interval_seconds
        active_log_seen = False
        saw_server_pull = False
        last_signature: tuple[tuple[str, int], ...] | None = None
        unchanged_checks = 0
        defer_initial_recovery = _engine_relay_atomic_completion(request)
        recovery_not_before = (
            time.monotonic() + WAITING_RELAY_INITIAL_RECOVERY_GRACE_SECONDS
            if defer_initial_recovery
            else 0.0
        )
        while True:
            if (returned_dir / "status.json").exists():
                if not saw_server_pull:
                    return RelayReturn(returned_dir, "client-push")
                if self._client_state(returned_dir) in TERMINAL_CLIENT_STATES:
                    return RelayReturn(returned_dir, "server-pullback-terminal")
            now = time.monotonic()
            if now >= next_pull:
                pulled = self._try_pull_client_result(request, returned_dir)
                if pulled:
                    saw_server_pull = True
                    client_state = self._client_state(returned_dir)
                    signature = self._log_signature(returned_dir)
                    if client_state in TERMINAL_CLIENT_STATES:
                        return RelayReturn(returned_dir, "server-pullback-terminal")
                    if client_state:
                        result_dir = (
                            self.repo_dir
                            / self.config.io.output_dir
                            / request.output_subdir
                        )
                        status_path = result_dir / "status.json"
                        if status_path.exists():
                            try:
                                waiting_status = read_json(status_path)
                                self._publish_waiting_client_progress(
                                    request,
                                    result_dir,
                                    waiting_status,
                                    returned_dir,
                                    client_state,
                                )
                            except Exception as exc:
                                print(
                                    "git-partner-server progress publish error for "
                                    f"{request.request_id}: {exc}",
                                    flush=True,
                                )
                    if signature:
                        if active_log_seen and signature == last_signature:
                            unchanged_checks += 1
                        else:
                            unchanged_checks = 0
                        active_log_seen = True
                        last_signature = signature
                        if (
                            request.timeout_seconds <= 0
                            and unchanged_checks >= self.config.relay.reverse_log_stall_checks
                        ):
                            self._write_stalled_status(returned_dir, request, signature)
                            return RelayReturn(returned_dir, "server-pullback-stalled")
                    next_pull = now + (
                        self.config.relay.reverse_log_active_interval_seconds
                        if active_log_seen
                        else self.config.relay.reverse_log_initial_interval_seconds
                    )
                else:
                    if time.monotonic() >= recovery_not_before:
                        self._recover_waiting_client(request)
                        recovery_status = _read_status_if_present(
                            self.repo_dir
                            / self.config.io.output_dir
                            / request.output_subdir
                            / "status.json"
                        )
                        recovery_not_before = time.monotonic() + _waiting_recovery_delay_seconds(
                            recovery_status
                        )
                    next_pull = now + self.config.relay.reverse_log_initial_interval_seconds
            if deadline and time.monotonic() > deadline:
                return self._pull_client_result_after_timeout(request, returned_dir)
            time.sleep(self.config.relay.poll_interval_seconds)

    def _try_pull_client_result(
        self,
        request: ExecutionRequest,
        returned_dir: Path,
    ) -> bool:
        if (returned_dir / "status.json").exists():
            self._relay_return_sources[request.request_id] = "client-push-atomic"
            return True

        if (
            request.relay_protocol_version == RELAY_PROTOCOL_VERSION
            and self.config.relay.relay_v2_return_mode == "push-atomic"
        ):
            publishing_marker = self._return_publishing_marker(request.request_id)
            if publishing_marker.exists():
                try:
                    marker_age = max(0.0, time.time() - publishing_marker.stat().st_mtime)
                except OSError:
                    marker_age = 0.0
                stale_after = max(
                    60.0,
                    float(self.config.relay.relay_v2_push_grace_seconds) * 10.0,
                )
                if marker_age <= stale_after:
                    return False
                publishing_marker.unlink(missing_ok=True)

            not_before = self._relay_pull_not_before.setdefault(
                request.request_id,
                time.monotonic()
                + self.config.relay.relay_v2_push_grace_seconds,
            )
            if time.monotonic() < not_before:
                return False

        remote_result = self._remote_client_result_dir(request)
        try:
            self.transport.pull_dir(
                self.config.relay.client_ssh,
                remote_result,
                returned_dir,
            )
        except Exception:
            return False
        pulled = (returned_dir / "status.json").exists() or any(
            returned_dir.glob("client.log.part*.txt")
        )
        if pulled:
            self._relay_return_sources[request.request_id] = "server-pullback-daemon"
        return pulled

    def _pull_client_result_after_timeout(
        self,
        request: ExecutionRequest,
        returned_dir: Path,
    ) -> RelayReturn:
        remote_result = self._remote_client_result_dir(request)
        try:
            self.transport.pull_dir(
                self.config.relay.client_ssh,
                remote_result,
                returned_dir,
            )
        except Exception as exc:
            raise RelayError(
                f"timed out waiting for relay return: {returned_dir}; "
                f"server pullback from client inbox failed: {exc}"
            ) from exc
        if not (returned_dir / "status.json").exists():
            raise RelayError(
                f"timed out waiting for relay return: {returned_dir}; "
                f"server pullback copied {remote_result} but status.json is missing"
            )
        return RelayReturn(returned_dir, "server-pullback")

    def _recover_waiting_client(self, request: ExecutionRequest) -> None:
        target_host = self.config.relay.client_ssh
        target_dir = _default_action_target_dir(self.config, "client")
        result_dir = self.repo_dir / self.config.io.output_dir / request.output_subdir
        waiting_status = _read_status_if_present(result_dir / "status.json")
        dispatched_at = str(waiting_status.get("dispatched_at", ""))
        waiting_age_seconds = _timestamp_age_seconds(dispatched_at) if dispatched_at else 0.0
        recovery: dict[str, object] = {
            "attempted_at": utc_now(),
            "action": "diagnose-only",
            "waiting_age_seconds": round(waiting_age_seconds, 3),
        }
        if not target_host:
            recovery.update(
                {
                    "skipped": True,
                    "reason": "relay.client_ssh is empty",
                }
            )
            self._write_recovery_status(result_dir, recovery)
            return
        if not target_dir:
            recovery.update(
                {
                    "skipped": True,
                    "reason": "relay.client_inbox_dir does not end with /work/relay/inbox",
                    "client_inbox_dir": self.config.relay.client_inbox_dir,
                }
            )
            self._write_recovery_status(result_dir, recovery)
            return

        try:
            from limited_remote_partner.maintenance.lan_ops import diagnose_peer, restart_service, sync_code

            diagnostic = diagnose_peer(
                self.transport,
                target_host,
                target_dir,
                "client",
                request_id=request.request_id,
            )
            recovery["diagnostic"] = diagnostic
            if _diagnostic_wants_relay_republish(diagnostic):
                bundle_dir = self._bundle_dir(request.request_id)
                if bundle_dir.exists():
                    recovery["action"] = "relay-bundle-republish"
                    recovery["dispatch_info"] = self._push_relay_bundle(request, bundle_dir)
                    recovery["post_diagnostic"] = diagnose_peer(
                        self.transport,
                        target_host,
                        target_dir,
                        "client",
                        request_id=request.request_id,
                    )
                    if _diagnostic_wants_client_refresh_for_ready_request(
                        recovery["post_diagnostic"],
                        waiting_age_seconds=waiting_age_seconds,
                    ):
                        args = _server_action_namespace(
                            {
                                "target_role": "client",
                            }
                        )
                        recovery["action"] = "relay-bundle-republish-and-refresh-client"
                        recovery["sync_code"] = sync_code(
                            self.config,
                            self.transport,
                            target_host,
                            target_dir,
                            args,
                        )
                        recovery["restart_service"] = restart_service(
                            self.transport,
                            target_host,
                            target_dir,
                            "client",
                            args,
                        )
                else:
                    recovery["action"] = "relay-bundle-republish-missing-local-bundle"
                    recovery["local_bundle_dir"] = str(bundle_dir)
            elif _diagnostic_wants_client_refresh_for_ready_request(
                diagnostic,
                waiting_age_seconds=waiting_age_seconds,
            ):
                args = _server_action_namespace(
                    {
                        "target_role": "client",
                    }
                )
                recovery["action"] = "lan-refresh-client-ready-request"
                recovery["sync_code"] = sync_code(
                    self.config,
                    self.transport,
                    target_host,
                    target_dir,
                    args,
                )
                recovery["restart_service"] = restart_service(
                    self.transport,
                    target_host,
                    target_dir,
                    "client",
                    args,
                )
            elif _diagnostic_ready_request_without_result(diagnostic) and _diagnostic_process_lines(
                str(diagnostic.get("stdout", ""))
            ):
                recovery["action"] = "observe-active-client"
            elif _diagnostic_wants_client_bootstrap(diagnostic):
                args = _server_action_namespace(
                    {
                        "target_role": "client",
                    }
                )
                recovery["action"] = "lan-bootstrap-client"
                recovery["sync_code"] = sync_code(
                    self.config,
                    self.transport,
                    target_host,
                    target_dir,
                    args,
                )
                recovery["restart_service"] = restart_service(
                    self.transport,
                    target_host,
                    target_dir,
                    "client",
                    args,
                )
        except Exception as exc:
            recovery.update(
                {
                    "action": "recovery-failed",
                    "error": str(exc),
                }
            )
        self._write_recovery_status(result_dir, recovery)

    def _write_recovery_status(
        self,
        result_dir: Path,
        recovery: dict[str, object],
    ) -> None:
        status: dict[str, object] = {}
        status_path = result_dir / "status.json"
        if status_path.exists():
            try:
                status = read_json(status_path)
            except Exception:
                status = {}
        status["state"] = status.get("state", "waiting-client")
        status["relay_recovery"] = recovery
        self._write_status(result_dir, status)
        self._sync(
            f"git_partner_server: {status.get('request_id', 'request')} relay recovery",
            result_dir,
        )

    def _remote_client_result_dir(self, request: ExecutionRequest) -> str:
        return (
            f"{self.config.relay.client_inbox_dir.rstrip('/')}/"
            f"{request.request_id}/result"
        )

    def _push_relay_bundle(
        self,
        request: ExecutionRequest,
        bundle_dir: Path,
    ) -> dict[str, str]:
        inbox_dir = self.config.relay.client_inbox_dir.rstrip("/")
        final_dir = f"{inbox_dir}/{request.request_id}"
        host = self.config.relay.client_ssh
        if not host:
            self.transport.push_dir(bundle_dir, None, final_dir)
            self._verify_local_relay_bundle(final_dir, request.request_id)
            ready_marker = self._publish_local_ready_marker(final_dir, request.request_id)
            return {
                "relay_dispatch_mode": "local-replace",
                "remote_bundle_dir": final_dir,
                "relay_ready_marker": ready_marker,
            }

        staging_root = _relay_staging_root(inbox_dir)
        ready_root = f"{inbox_dir}/.ready"
        ready_marker = f"{ready_root}/{request.request_id}.ready"
        last_error = ""
        stream_push = getattr(self.transport, "stream_dir_to_ssh_shell", None)
        if callable(stream_push):
            for attempt in range(1, RELAY_BUNDLE_PUBLISH_ATTEMPTS + 1):
                stamp = f"{int(time.time() * 1000)}.{attempt}"
                staging_dir = f"{staging_root}/{request.request_id}.{stamp}.incoming"
                stream_script = "\n".join(
                    [
                        "set -euo pipefail",
                        f"FINAL={shlex.quote(final_dir)}",
                        f"STAGING={shlex.quote(staging_dir)}",
                        f"REQUEST_ID={shlex.quote(request.request_id)}",
                        f"READY_ROOT={shlex.quote(ready_root)}",
                        f"READY_MARKER={shlex.quote(ready_marker)}",
                        'mkdir -p "$READY_ROOT"',
                        (
                            'if [ -f "$READY_MARKER" ] && '
                            '[ "$(cat "$READY_MARKER" 2>/dev/null || true)" = "$REQUEST_ID" ] && '
                            '[ -f "$FINAL/request.json" ] && [ -d "$FINAL/payload" ]; then'
                        ),
                        "  cat >/dev/null",
                        "  echo RELAY_BUNDLE_ALREADY_READY",
                        "  exit 0",
                        "fi",
                        'rm -rf "$STAGING"',
                        'mkdir -p "$STAGING"',
                        'tar -xf - -C "$STAGING"',
                        'rm -f "$READY_MARKER" "$READY_MARKER".__tmp.*',
                        'rm -rf "$FINAL"',
                        'mv "$STAGING" "$FINAL"',
                        (
                            'if [ ! -f "$FINAL/request.json" ] && '
                            '[ -f "$FINAL/$REQUEST_ID/request.json" ]; then'
                        ),
                        '  tmp="${FINAL}.__flatten.$$"',
                        '  rm -rf "$tmp"',
                        '  mv "$FINAL/$REQUEST_ID" "$tmp"',
                        '  rm -rf "$FINAL"',
                        '  mv "$tmp" "$FINAL"',
                        "fi",
                        (
                            'if [ ! -f "$FINAL/request.json" ] || '
                            '[ ! -d "$FINAL/payload" ]; then'
                        ),
                        "  echo RELAY_BUNDLE_VERIFY_FAILED",
                        (
                            '  find "$FINAL" -maxdepth 3 -mindepth 1 '
                            '-printf "%P\\n" 2>/dev/null | sort | head -80 || true'
                        ),
                        "  exit 23",
                        "fi",
                        'marker_tmp="${READY_MARKER}.__tmp.$$"',
                        'printf \'%s\\n\' "$REQUEST_ID" > "$marker_tmp"',
                        'mv "$marker_tmp" "$READY_MARKER"',
                        "echo RELAY_BUNDLE_VERIFY_OK",
                    ]
                )
                try:
                    stream_started = time.monotonic()
                    streamed = stream_push(bundle_dir, host, stream_script)
                    stream_seconds = time.monotonic() - stream_started
                except RelayError as exc:
                    last_error = f"stream attempt {attempt}: {exc}"
                    continue
                verify = streamed.stdout.strip() or "RELAY_BUNDLE_VERIFY_OK"
                return {
                    "relay_dispatch_mode": "remote-stream-atomic-rename",
                    "remote_staging_dir": staging_dir,
                    "remote_bundle_dir": final_dir,
                    "relay_ready_marker": ready_marker,
                    "relay_publish_attempt": str(attempt),
                    "relay_publish_verify": verify,
                    "relay_stream_seconds": f"{stream_seconds:.6f}",
                    "relay_push_seconds": f"{stream_seconds:.6f}",
                    "relay_finalize_seconds": "0.000000",
                    "relay_transport_processes": "1",
                }

        stream_fallback_error = last_error
        for attempt in range(1, RELAY_BUNDLE_PUBLISH_ATTEMPTS + 1):
            stamp = f"{int(time.time() * 1000)}.{attempt}"
            staging_dir = f"{staging_root}/{request.request_id}.{stamp}.incoming"
            try:
                push_started = time.monotonic()
                self.transport.push_dir(bundle_dir, host, staging_dir)
                push_seconds = time.monotonic() - push_started
                finalize_started = time.monotonic()
                finalized = self.transport.run_ssh_shell(
                    host,
                    "\n".join(
                        [
                            "set -euo pipefail",
                            f"FINAL={shlex.quote(final_dir)}",
                            f"STAGING={shlex.quote(staging_dir)}",
                            f"REQUEST_ID={shlex.quote(request.request_id)}",
                            f"READY_ROOT={shlex.quote(ready_root)}",
                            f"READY_MARKER={shlex.quote(ready_marker)}",
                            'mkdir -p "$READY_ROOT"',
                            'rm -f "$READY_MARKER" "$READY_MARKER".__tmp.*',
                            'rm -rf "$FINAL"',
                            'mv "$STAGING" "$FINAL"',
                            (
                                'if [ ! -f "$FINAL/request.json" ] && '
                                '[ -f "$FINAL/$REQUEST_ID/request.json" ]; then'
                            ),
                            '  tmp="${FINAL}.__flatten.$$"',
                            '  rm -rf "$tmp"',
                            '  mv "$FINAL/$REQUEST_ID" "$tmp"',
                            '  rm -rf "$FINAL"',
                            '  mv "$tmp" "$FINAL"',
                            "fi",
                            (
                                'if [ ! -f "$FINAL/request.json" ] || '
                                '[ ! -d "$FINAL/payload" ]; then'
                            ),
                            "  echo RELAY_BUNDLE_VERIFY_FAILED",
                            (
                                '  find "$FINAL" -maxdepth 3 -mindepth 1 '
                                '-printf "%P\\n" 2>/dev/null | sort | head -80 || true'
                            ),
                            "  exit 23",
                            "fi",
                            'marker_tmp="${READY_MARKER}.__tmp.$$"',
                            'printf \'%s\\n\' "$REQUEST_ID" > "$marker_tmp"',
                            'mv "$marker_tmp" "$READY_MARKER"',
                            "echo RELAY_BUNDLE_VERIFY_OK",
                        ]
                    ),
                )
                finalize_seconds = time.monotonic() - finalize_started
            except RelayError as exc:
                last_error = f"attempt {attempt}: {exc}"
                continue
            verify = finalized.stdout.strip() or "RELAY_BUNDLE_VERIFY_OK"
            dispatch = {
                "relay_dispatch_mode": "remote-atomic-rename",
                "remote_staging_dir": staging_dir,
                "remote_bundle_dir": final_dir,
                "relay_ready_marker": ready_marker,
                "relay_publish_attempt": str(attempt),
                "relay_publish_verify": verify,
                "relay_push_seconds": f"{push_seconds:.6f}",
                "relay_finalize_seconds": f"{finalize_seconds:.6f}",
            }
            if stream_fallback_error:
                dispatch["relay_stream_fallback_error"] = stream_fallback_error
            return dispatch
        raise RelayError(
            "relay bundle publish verification failed after "
            f"{RELAY_BUNDLE_PUBLISH_ATTEMPTS} attempts: {last_error}"
        )

    def _push_relay_bundle_batch(
        self,
        rows: list[tuple[ExecutionRequest, Path]],
    ) -> dict[str, dict[str, str]]:
        if len(rows) < 2 or not self.config.relay.client_ssh:
            return {
                request.request_id: self._push_relay_bundle(request, bundle_dir)
                for request, bundle_dir in rows
            }
        stream_batch = getattr(
            self.transport,
            "stream_dirs_to_ssh_shell",
            None,
        )
        if not callable(stream_batch):
            return {
                request.request_id: self._push_relay_bundle(request, bundle_dir)
                for request, bundle_dir in rows
            }

        inbox_dir = self.config.relay.client_inbox_dir.rstrip("/")
        staging_root = _relay_staging_root(inbox_dir)
        ready_root = f"{inbox_dir}/.ready"
        request_ids = [request.request_id for request, _bundle_dir in rows]
        stamp = f"{int(time.time() * 1000)}"
        batch_id = f"relay-batch-{stamp}"
        batch_metadata = {
            "schema": "git-partner.relay-batch.v1",
            "batch_id": batch_id,
            "batch_size": len(rows),
            "request_ids": request_ids,
        }
        for _request, bundle_dir in rows:
            write_json(
                bundle_dir / "relay_batch.json",
                batch_metadata,
                self.config.io.max_file_bytes,
            )
        batch_dir = f"{staging_root}/batch.{stamp}.incoming"
        request_words = " ".join(shlex.quote(item) for item in request_ids)
        script = "\n".join(
            [
                "set -euo pipefail",
                f"INBOX={shlex.quote(inbox_dir)}",
                f"BATCH={shlex.quote(batch_dir)}",
                f"READY_ROOT={shlex.quote(ready_root)}",
                'rm -rf "$BATCH"',
                'mkdir -p "$BATCH" "$READY_ROOT"',
                'tar -xf - -C "$BATCH"',
                f"for REQUEST_ID in {request_words}; do",
                '  SOURCE="$BATCH/$REQUEST_ID"',
                '  FINAL="$INBOX/$REQUEST_ID"',
                '  READY_MARKER="$READY_ROOT/$REQUEST_ID.ready"',
                (
                    '  if [ ! -f "$SOURCE/request.json" ] || '
                    '[ ! -d "$SOURCE/payload" ]; then'
                ),
                '    echo "RELAY_BATCH_VERIFY_FAILED:$REQUEST_ID"',
                "    exit 23",
                "  fi",
                '  rm -f "$READY_MARKER" "$READY_MARKER".__tmp.*',
                '  rm -rf "$FINAL"',
                '  mv "$SOURCE" "$FINAL"',
                '  marker_tmp="${READY_MARKER}.__tmp.$$"',
                '  printf \'%s\\n\' "$REQUEST_ID" > "$marker_tmp"',
                '  mv "$marker_tmp" "$READY_MARKER"',
                "done",
                'rm -rf "$BATCH"',
                "echo RELAY_BATCH_VERIFY_OK",
            ]
        )
        started = time.monotonic()
        streamed = stream_batch(
            {request.request_id: bundle_dir for request, bundle_dir in rows},
            self.config.relay.client_ssh,
            script,
        )
        stream_seconds = time.monotonic() - started
        verify = streamed.stdout.strip() or "RELAY_BATCH_VERIFY_OK"
        return {
            request_id: {
                "relay_dispatch_mode": "remote-stream-batch-atomic-rename",
                "remote_staging_dir": f"{batch_dir}/{request_id}",
                "remote_bundle_dir": f"{inbox_dir}/{request_id}",
                "relay_ready_marker": f"{ready_root}/{request_id}.ready",
                "relay_publish_attempt": "1",
                "relay_publish_verify": verify,
                "relay_stream_seconds": f"{stream_seconds:.6f}",
                "relay_push_seconds": f"{stream_seconds:.6f}",
                "relay_finalize_seconds": "0.000000",
                "relay_transport_processes": "1",
                "relay_batch_size": str(len(rows)),
                "relay_batch_id": batch_id,
            }
            for request_id in request_ids
        }

    def _verify_local_relay_bundle(self, final_dir: str, request_id: str) -> None:
        final_path = Path(final_dir)
        if not final_path.is_absolute():
            final_path = self.repo_dir / final_path
        if _bundle_has_request(final_path, request_id):
            return
        raise RelayError(f"local relay bundle is incomplete: {final_path}")

    def _publish_local_ready_marker(self, final_dir: str, request_id: str) -> str:
        final_path = Path(final_dir)
        if not final_path.is_absolute():
            final_path = self.repo_dir / final_path
        ready_dir = final_path.parent / ".ready"
        ready_dir.mkdir(parents=True, exist_ok=True)
        marker = ready_dir / f"{request_id}.ready"
        temporary = ready_dir / f".{request_id}.ready.tmp"
        temporary.write_text(request_id + "\n", encoding="utf-8")
        temporary.replace(marker)
        return str(marker)

    def _verify_remote_relay_bundle(
        self,
        host: str,
        final_dir: str,
        request_id: str,
    ) -> str:
        final_q = shlex.quote(final_dir)
        request_q = shlex.quote(request_id)
        script = "\n".join(
            [
                "set -euo pipefail",
                f"FINAL={final_q}",
                f"REQUEST_ID={request_q}",
                (
                    'if [ -f "$FINAL/request.json" ] && '
                    '[ -d "$FINAL/payload" ]; then '
                    "echo RELAY_BUNDLE_VERIFY_OK; exit 0; fi"
                ),
                (
                    'if [ -f "$FINAL/$REQUEST_ID/request.json" ] && '
                    '[ -d "$FINAL/$REQUEST_ID/payload" ]; then '
                    "echo RELAY_BUNDLE_VERIFY_NESTED_ONLY; exit 24; fi"
                ),
                "echo RELAY_BUNDLE_VERIFY_FAILED",
                'find "$FINAL" -maxdepth 3 -mindepth 1 -printf "%P\\n" '
                "2>/dev/null | sort | head -80 || true",
                "exit 23",
            ]
        )
        result = self.transport.run_ssh_shell(host, script)
        return result.stdout.strip() or "RELAY_BUNDLE_VERIFY_OK"

    def _client_state(self, returned_dir: Path) -> str:
        status_path = returned_dir / "status.json"
        if not status_path.exists():
            return ""
        try:
            status = read_json(status_path)
        except Exception:
            return ""
        return str(status.get("state", ""))

    def _log_signature(self, returned_dir: Path) -> tuple[tuple[str, int], ...]:
        return tuple(
            (path.name, path.stat().st_size)
            for path in sorted(returned_dir.glob("client.log.part*.txt"))
        )

    def _write_stalled_status(
        self,
        returned_dir: Path,
        request: ExecutionRequest,
        signature: tuple[tuple[str, int], ...],
    ) -> None:
        base_status = {}
        status_path = returned_dir / "status.json"
        if status_path.exists():
            try:
                base_status = read_json(status_path)
            except Exception:
                base_status = {}
        write_json(
            status_path,
            {
                **base_status,
                "state": "stalled",
                "request_id": request.request_id,
                "exit_code": 1,
                "stalled_at": utc_now(),
                "stall_reason": (
                    "client log signature was unchanged across "
                    f"{self.config.relay.reverse_log_stall_checks} active pullback checks"
                ),
                "log_signature": [
                    {"name": name, "bytes": size}
                    for name, size in signature
                ],
            },
            self.config.io.max_file_bytes,
        )

    def _relay_failure_diagnostic(self, request: ExecutionRequest) -> dict[str, object]:
        target_host = self.config.relay.client_ssh
        target_dir = _default_action_target_dir(self.config, "client")
        if not target_host:
            return {
                "step": "diagnose-peer",
                "reachable": False,
                "skipped": True,
                "reason": "relay.client_ssh is empty",
            }
        if not target_dir:
            return {
                "step": "diagnose-peer",
                "reachable": False,
                "skipped": True,
                "reason": "relay.client_inbox_dir does not end with /work/relay/inbox",
                "client_inbox_dir": self.config.relay.client_inbox_dir,
            }
        try:
            from limited_remote_partner.maintenance.lan_ops import diagnose_peer

            return diagnose_peer(
                self.transport,
                target_host,
                target_dir,
                "client",
                request_id=request.request_id,
            )
        except Exception as exc:
            return {
                "step": "diagnose-peer",
                "reachable": False,
                "error": str(exc),
            }

    def _payload_root(self, request: ExecutionRequest) -> Path:
        if request.payload_root:
            path = Path(request.payload_root)
            if not path.is_absolute():
                path = self.repo_dir / path
            return path.resolve()
        return self.repo_dir

    def _bundle_dir(self, request_id: str) -> Path:
        return (
            self.repo_dir
            / self.config.io.state_dir
            / "relay_out"
            / request_id
        )

    def _return_dir(self, request_id: str) -> Path:
        return (self._return_base_dir() / request_id).resolve()

    def _return_base_dir(self) -> Path:
        base = Path(self.config.relay.server_return_dir)
        if not base.is_absolute():
            base = self.repo_dir / base
        return base.resolve()

    def _return_ready_marker(self, request_id: str) -> Path:
        return self._return_base_dir() / ".ready" / f"{request_id}.ready"

    def _return_publishing_marker(self, request_id: str) -> Path:
        return (
            self._return_base_dir()
            / ".publishing"
            / f"{request_id}.publishing"
        )

    def _sync(self, message: str, result_dir: Path) -> None:
        self._sync_many(message, [result_dir])

    def _sync_many(self, message: str, result_dirs: list[Path]) -> None:
        result_paths = [
            result_dir.resolve().relative_to(self.repo_dir.resolve()).as_posix()
            for result_dir in result_dirs
        ]
        self.git.commit_and_push(
            result_paths,
            message,
            self.config.io.max_file_bytes,
        )

    def _write_status(self, result_dir: Path, status: dict[str, object]) -> None:
        if str(status.get("transport", "")) == "relay":
            status = _with_relay_state_history(
                _read_status_if_present(result_dir / "status.json"),
                status,
            )
        write_json(result_dir / "status.json", status, self.config.io.max_file_bytes)
        write_compact_receipt(
            result_dir,
            status,
            self.config.io.max_file_bytes,
        )


def _server_action_namespace(raw: dict[str, str]) -> SimpleNamespace:
    target_role = raw.get("target_role", "client")
    if target_role not in {"client", "server"}:
        raise RelayError("server action target_role must be client or server")
    sync_paths = raw.get("sync_path", "")
    if sync_paths:
        sync_path = [item.strip() for item in sync_paths.split(",") if item.strip()]
    else:
        from limited_remote_partner.maintenance.lan_ops import DEFAULT_SYNC_PATHS

        sync_path = list(DEFAULT_SYNC_PATHS)
    return SimpleNamespace(
        target_role=target_role,
        target_host=raw.get("target_host", ""),
        target_dir=raw.get("target_dir", ""),
        remote_config=raw.get("remote_config", "configs/partner.json"),
        remote_staging_dir=raw.get("remote_staging_dir", ""),
        service_name=raw.get("service_name", ""),
        cleanup_request_id=raw.get("cleanup_request_id", ""),
        diagnose_request_id=raw.get("diagnose_request_id", ""),
        cancel_request_id=raw.get("cancel_request_id", ""),
        cancel_reason=raw.get("cancel_reason", "LAN operator requested cancellation"),
        node_ack_json=raw.get("node_ack_json", ""),
        tmux_session=raw.get("tmux_session", ""),
        artifact_profile=raw.get("artifact_profile", ""),
        script_b64=raw.get("script_b64", ""),
        endpoint_action=raw.get("endpoint_action", "status"),
        source_repo=raw.get("source_repo", ""),
        worktree=raw.get("worktree", ""),
        control_branch=raw.get("control_branch", ""),
        endpoint_config=raw.get("endpoint_config", ""),
        endpoint_role=raw.get("endpoint_role", "local"),
        endpoint_remote=raw.get("endpoint_remote", "origin"),
        import_login_network_env=raw.get(
            "import_login_network_env", "0"
        ).lower()
        in {"1", "true", "yes", "on"},
        no_process_fallback=raw.get("no_process_fallback", "0").lower()
        in {"1", "true", "yes", "on"},
        sync_path=sync_path,
    )


def _append_action_log(
    action_log: list[dict[str, object]],
    entry: object,
) -> None:
    if isinstance(entry, dict):
        action_log.append(entry)


def _diagnostic_wants_client_bootstrap(diagnostic: dict[str, object]) -> bool:
    if not diagnostic.get("reachable"):
        return False
    stdout = str(diagnostic.get("stdout", ""))
    if "TARGET_DIR_MISSING" in stdout:
        return False
    if "REQUEST_DIR_MISSING" in stdout:
        return False
    if "PROCESS_SCAN_START" not in stdout or "PROCESS_SCAN_DONE" not in stdout:
        return False
    return not _diagnostic_process_lines(stdout)


def _diagnostic_wants_relay_republish(diagnostic: dict[str, object]) -> bool:
    if not diagnostic.get("reachable"):
        return False
    stdout = str(diagnostic.get("stdout", ""))
    if "REQUEST_DIR_OK" not in stdout:
        return False
    has_direct = "REQUEST_JSON_OK" in stdout and "PAYLOAD_DIR_OK" in stdout
    if has_direct:
        return False
    return "REQUEST_JSON_MISSING" in stdout or "PAYLOAD_DIR_MISSING" in stdout


def _diagnostic_wants_client_refresh_for_ready_request(
    diagnostic: object,
    *,
    waiting_age_seconds: float = float("inf"),
) -> bool:
    if not _diagnostic_ready_request_without_result(diagnostic):
        return False
    assert isinstance(diagnostic, dict)
    process_lines = _diagnostic_process_lines(str(diagnostic.get("stdout", "")))
    return not process_lines or waiting_age_seconds >= WAITING_RELAY_ACTIVE_CLIENT_REFRESH_SECONDS


def _diagnostic_ready_request_without_result(diagnostic: object) -> bool:
    if not isinstance(diagnostic, dict) or not diagnostic.get("reachable"):
        return False
    stdout = str(diagnostic.get("stdout", ""))
    ready = "REQUEST_JSON_OK" in stdout and "PAYLOAD_DIR_OK" in stdout
    if not ready:
        return False
    if "REQUEST_DONE_PRESENT" in stdout:
        return False
    if "REQUEST_RESULT_STATUS_START" in stdout:
        return False
    return "RESULT_DIR_MISSING" in stdout


def _diagnostic_process_lines(stdout: str) -> list[str]:
    lines = stdout.splitlines()
    try:
        start = lines.index("PROCESS_SCAN_START") + 1
        end = lines.index("PROCESS_SCAN_DONE")
    except ValueError:
        return []
    return [line for line in lines[start:end] if line.strip()]


def _bundle_has_request(path: Path, request_id: str) -> bool:
    if (path / "request.json").is_file() and (path / "payload").is_dir():
        return True
    nested = path / request_id
    return (nested / "request.json").is_file() and (nested / "payload").is_dir()


def _request_from_waiting_status(
    status: dict[str, object],
    output_subdir: str,
) -> ExecutionRequest:
    request_id = str(status.get("request_id") or output_subdir)
    return ExecutionRequest(
        request_id=request_id,
        command=(),
        working_dir=".",
        output_subdir=output_subdir,
        payload_paths=tuple(_string_items(status.get("payload_paths"))),
        return_paths=tuple(_string_items(status.get("return_paths"))),
        transport="relay",
        request_kind=str(status.get("request_kind") or "command"),
        completion_mode=str(status.get("completion_mode") or "terminal"),
        engine_job_id=str(status.get("engine_job_id") or "") or None,
        target_nodes=tuple(_string_items(status.get("target_nodes"))),
        target_endpoint_id=str(status.get("target_endpoint_id") or "") or None,
        target_environment_id=(
            str(status.get("target_environment_id") or "") or None
        ),
        target_gateway_id=str(status.get("target_gateway_id") or "") or None,
        target_transport_mode=(
            str(status.get("target_transport_mode") or "") or None
        ),
        registration_generation=(
            str(status.get("registration_generation") or "") or None
        ),
        experiment_id=str(status.get("experiment_id") or "") or None,
        attempt_id=str(status.get("attempt_id") or "") or None,
        workflow_ingest=bool(status.get("workflow_ingest", True)),
    )


def _relay_request_metadata(request: ExecutionRequest) -> dict[str, object]:
    return request_status_metadata(request)


def _with_relay_state_history(
    existing: dict[str, object],
    status: dict[str, object],
) -> dict[str, object]:
    updated_at = utc_now()
    history_raw = existing.get("relay_state_history", [])
    history = [dict(item) for item in history_raw if isinstance(item, dict)]
    recovery = status.get("relay_recovery")
    recovery_action = (
        str(recovery.get("action", "")) if isinstance(recovery, dict) else ""
    )
    event = {
        "observed_at": updated_at,
        "state": str(status.get("state", "")),
        "phase": str(status.get("phase", "")),
        "client_state": str(status.get("client_state", "")),
        "recovery_action": recovery_action,
    }
    signature = tuple(event[key] for key in ("state", "phase", "client_state", "recovery_action"))
    previous_signature: tuple[object, ...] | None = None
    if history:
        previous = history[-1]
        previous_signature = tuple(
            previous.get(key, "")
            for key in ("state", "phase", "client_state", "recovery_action")
        )
    if signature != previous_signature:
        history.append(event)
    merged = dict(status)
    merged["relay_protocol_version"] = str(
        status.get("relay_protocol_version") or RELAY_PROTOCOL_VERSION
    )
    merged["relay_status_updated_at"] = updated_at
    merged["relay_state_history"] = history[-RELAY_STATE_HISTORY_LIMIT:]
    return merged


def _read_status_if_present(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        raw = read_json(path)
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _same_dispatch(
    status: dict[str, object],
    request: ExecutionRequest,
    trigger_ref: str,
) -> bool:
    return (
        str(status.get("request_id", "")) == request.request_id
        and str(status.get("trigger_ref", "")) == trigger_ref
    )


def _same_waiting_dispatch(
    status: dict[str, object],
    request: ExecutionRequest,
    trigger_ref: str,
) -> bool:
    return _same_dispatch(status, request, trigger_ref) and str(
        status.get("state", "")
    ) == "waiting-client"


def _same_terminal_dispatch(
    status: dict[str, object],
    request: ExecutionRequest,
    trigger_ref: str,
) -> bool:
    return _same_dispatch(status, request, trigger_ref) and str(
        status.get("state", "")
    ) in TERMINAL_CLIENT_STATES


def _iter_output_status_paths(output_base: Path) -> list[Path]:
    paths: list[Path] = []
    for status_path in output_base.rglob("status.json"):
        relative = status_path.relative_to(output_base)
        if "client_output" in relative.parts[:-1]:
            continue
        paths.append(status_path)
    return sorted(paths)


def _status_needs_relay_collection(
    status: dict[str, object],
    output_subdir: str,
) -> bool:
    state = str(status.get("state", ""))
    transport = str(status.get("transport", ""))
    if transport == "relay":
        return state == "waiting-client"
    if transport in {"server-local", "direct"}:
        return False
    if state in TERMINAL_CLIENT_STATES:
        return False
    request_id = str(status.get("request_id") or "")
    if request_id != output_subdir:
        return False
    return state in {
        "received",
        "copying-payload",
        "running",
        "cancelling",
        "collecting-return",
    }


def _string_items(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str)]


def _waiting_recovery_due(status: dict[str, object]) -> bool:
    recovery = status.get("relay_recovery")
    if not isinstance(recovery, dict):
        dispatched_at = status.get("dispatched_at")
        if not dispatched_at:
            return True
        return (
            _timestamp_age_seconds(str(dispatched_at))
            >= WAITING_RELAY_INITIAL_RECOVERY_GRACE_SECONDS
        )
    attempted_at = recovery.get("attempted_at")
    if not attempted_at:
        return True
    attempted_age = _timestamp_age_seconds(str(attempted_at))
    if str(recovery.get("action") or "") == "observe-active-client":
        dispatched_at = status.get("dispatched_at")
        waiting_age = (
            _timestamp_age_seconds(str(dispatched_at))
            if dispatched_at
            else float(recovery.get("waiting_age_seconds") or 0.0)
        )
        return (
            waiting_age >= WAITING_RELAY_ACTIVE_CLIENT_REFRESH_SECONDS
            and attempted_age >= WAITING_RELAY_RECOVERY_RETRY_SECONDS
        )
    return attempted_age >= WAITING_RELAY_RECOVERY_INTERVAL_SECONDS


def _waiting_recovery_delay_seconds(status: dict[str, object]) -> float:
    recovery = status.get("relay_recovery")
    if not isinstance(recovery, dict):
        return float(WAITING_RELAY_INITIAL_RECOVERY_GRACE_SECONDS)
    if str(recovery.get("action") or "") == "observe-active-client":
        waiting_age = float(recovery.get("waiting_age_seconds") or 0.0)
        return max(
            float(WAITING_RELAY_RECOVERY_RETRY_SECONDS),
            float(WAITING_RELAY_ACTIVE_CLIENT_REFRESH_SECONDS) - waiting_age,
        )
    return float(WAITING_RELAY_RECOVERY_INTERVAL_SECONDS)


def _relay_batch_grace_elapsed(
    rows: list[
        tuple[
            ExecutionRequest,
            Path,
            dict[str, object],
            Path,
            str,
        ]
    ],
    grace_seconds: float,
) -> bool:
    ready_times: list[float] = []
    for _request, _result_dir, _status, returned_dir, _source in rows:
        try:
            ready_times.append((returned_dir / "status.json").stat().st_mtime)
        except OSError:
            continue
    if not ready_times:
        return True
    return time.time() - min(ready_times) >= max(0.0, float(grace_seconds))


def _same_running_server_action(
    status: dict[str, object],
    request: ExecutionRequest,
    trigger_ref: str,
) -> bool:
    return (
        str(status.get("state", "")) == "running"
        and str(status.get("transport", "")) == "server-local"
        and str(status.get("request_id", "")) == request.request_id
        and str(status.get("trigger_ref", "")) == trigger_ref
        and str(status.get("server_action", "")) == str(request.server_action or "")
    )


def _server_action_pid(status: dict[str, object]) -> int:
    process = status.get("server_process")
    if not isinstance(process, dict):
        return 0
    return int(process.get("action_pid") or process.get("pid") or 0)


def _server_action_start_token(status: dict[str, object]) -> str:
    process = status.get("server_process")
    if not isinstance(process, dict):
        return ""
    return str(process.get("action_start_token") or "")


def _server_action_owner_alive(status: dict[str, object]) -> bool:
    pid = _server_action_pid(status)
    if pid <= 0:
        return False
    current_token = process_start_token(pid)
    if not current_token:
        return False
    expected_token = _server_action_start_token(status)
    return not expected_token or current_token == expected_token


def _timestamp_age_seconds(value: str) -> float:
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return float("inf")
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - stamp).total_seconds()


def _default_action_target_host(config: AppConfig, target_role: str) -> str:
    if target_role == "client":
        return config.relay.client_ssh
    if target_role == "server":
        return config.relay.server_ssh
    raise RelayError("server action target_role must be client or server")


def _default_action_target_dir(config: AppConfig, target_role: str) -> str:
    if target_role == "client":
        return _strip_suffix(config.relay.client_inbox_dir, "/work/relay/inbox")
    if target_role == "server":
        return _strip_suffix(config.relay.server_return_dir, "/work/relay/return")
    raise RelayError("server action target_role must be client or server")


def _registered_remote_config(
    config: AppConfig,
    target_role: str,
    requested: str,
) -> str:
    if target_role != "client" or requested not in {"", "configs/partner.json"}:
        return requested
    target_node = config.routing.target_node or config.endpoint.endpoint_id
    if not target_node:
        return requested
    manifest_path = config.repo_dir / "configs" / "node_launchers.json"
    if not manifest_path.is_file():
        return requested
    try:
        manifest = read_json(manifest_path)
    except (OSError, ValueError, RelayError):
        return requested
    nodes = manifest.get("nodes")
    if not isinstance(nodes, dict):
        return requested
    entry = nodes.get(target_node)
    if not isinstance(entry, dict) or str(entry.get("role") or "") != "client":
        return requested
    candidate = str(entry.get("config") or "").strip().replace("\\", "/")
    candidate_path = Path(candidate)
    if (
        not candidate
        or candidate_path.is_absolute()
        or ".." in candidate_path.parts
        or not (config.repo_dir / candidate_path).is_file()
    ):
        return requested
    return candidate


def _strip_suffix(path: str, suffix: str) -> str:
    normalized = path.rstrip("/")
    if normalized.endswith(suffix):
        return normalized[: -len(suffix)]
    return ""


def _relay_staging_root(inbox_dir: str) -> str:
    target_dir = _strip_suffix(inbox_dir, "/work/relay/inbox")
    if target_dir:
        return f"{target_dir.rstrip('/')}/work/relay/staging"
    return f"{inbox_dir.rstrip('/')}/.staging"


class StopFlag:
    def __init__(self) -> None:
        self.requested = False

    def handle(self, _signum: int, _frame: object) -> None:
        self.requested = True


def main() -> None:
    args = _parse_args()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    resident_lock = None
    if not args.once:
        resident_lock = ResidentRoleLock(
            config.repo_dir / config.io.state_dir,
            "server",
        )
        resident_lock.acquire()
    git = GitClient(config.repo, config.repo_dir)
    git.ensure_worktree()
    git.configure_identity()
    server = RelayServer(git, config)

    if args.once:
        trigger_ref = git.rev_parse("HEAD")
        try:
            request = parse_request(config, trigger_ref)
        except Exception as exc:
            server.reject_request(trigger_ref, exc)
            raise SystemExit(1) from exc
        request = route_request_for_current_runtime(config, request)
        if request is None:
            print(f"request for {trigger_ref} is not routed to this server", flush=True)
            raise SystemExit(0)
        raise SystemExit(server.run_request(request, trigger_ref, wait=not args.no_wait))

    try:
        _run_daemon(git, config, server, config_path, consume_pending_reexec())
    finally:
        if resident_lock is not None:
            resident_lock.release()


def _run_daemon(
    git: GitClient,
    config: AppConfig,
    server: RelayServer,
    config_path: Path,
    pending_reexec,
) -> None:
    stop = StopFlag()
    signal.signal(signal.SIGINT, stop.handle)
    signal.signal(signal.SIGTERM, stop.handle)
    last_ref = git.fetch()
    local_ref = git.rev_parse("HEAD")
    if pending_reexec and local_ref == pending_reexec.trigger_ref:
        changed = list(pending_reexec.changed_paths)
        if should_execute(config, changed):
            _dispatch_changed_requests(
                server,
                config,
                pending_reexec.trigger_ref,
                changed,
                after_reexec=True,
            )
            # Advance only to the ref that was actually dispatched. A newer
            # request may arrive while dispatch is running and must remain
            # visible to the next daemon poll.
            last_ref = pending_reexec.trigger_ref
        else:
            last_ref = pending_reexec.trigger_ref
    elif local_ref != last_ref:
        changed = git.changed_paths(local_ref, last_ref)
        git.checkout_remote_head()
        if should_reexec_for_update(config, changed):
            reexec_partner_role(
                config,
                config_path,
                "server",
                last_ref,
                local_ref,
                changed,
            )
        if should_execute(config, changed):
            _dispatch_changed_requests(server, config, last_ref, changed)

    _recover_current_server_action(server, config, last_ref)

    print(
        f"git-partner-server watching {config.io.input_dir}/ "
        f"dispatching to {config.relay.client_ssh or 'local'}:{config.relay.client_inbox_dir}",
        flush=True,
    )
    error_backoff = LoopErrorBackoff(config.error_backoff)
    while not stop.requested:
        try:
            remote_ref = git.fetch()
            if remote_ref != last_ref:
                changed = git.changed_paths(last_ref, remote_ref)
                git.checkout_remote_head()
                if should_reexec_for_update(config, changed):
                    reexec_partner_role(
                        config,
                        config_path,
                        "server",
                        remote_ref,
                        last_ref,
                        changed,
                    )
                if should_execute(config, changed):
                    _dispatch_changed_requests(
                        server,
                        config,
                        remote_ref,
                        changed,
                    )
                # The processed cursor is the fetched ref, never a later ref
                # observed after dispatch. This prevents a control or data
                # request committed during dispatch from being skipped.
                last_ref = remote_ref
            error_backoff.reset()
        except Exception as exc:
            sleep_seconds = error_backoff.record_failure()
            print(
                f"git-partner-server control error: {exc}; "
                f"sleeping {sleep_seconds:.1f}s before retry",
                flush=True,
            )
            time.sleep(sleep_seconds)
            continue

        for phase, action in (
            ("node-report", lambda: collect_relay_node_reports(config)),
            ("exchange", lambda: push_exchange_if_changed(git, config)),
            ("direct-return", server.publish_direct_returns),
            ("relay-return", server.poll_waiting_relay_returns),
        ):
            try:
                action()
            except Exception as exc:
                print(
                    f"git-partner-server {phase} housekeeping error: {exc}",
                    flush=True,
                )
        time.sleep(config.poll_interval_seconds)


def _dispatch_or_reject(
    server: RelayServer,
    config: AppConfig,
    trigger_ref: str,
    *,
    request_file: str | Path | None = None,
    after_reexec: bool = False,
) -> int:
    try:
        request = parse_request(config, trigger_ref, request_file)
    except Exception as exc:
        server.reject_request(trigger_ref, exc, request_file)
        print(f"rejected request for {trigger_ref}: {exc}", flush=True)
        return 1
    request = route_request_for_current_runtime(config, request)
    if request is None:
        print(f"skipping request for another GP endpoint: {trigger_ref}", flush=True)
        return 0
    suffix = " after reexec" if after_reexec else ""
    print(
        f"dispatching request {request.request_id}{suffix} for {trigger_ref}",
        flush=True,
    )
    # The resident server loop must remain available to collect older returns
    # and execute maintenance actions while an engine request is in flight.
    # Foreground submitters still wait for the terminal Git publication.
    return server.run_request(
        request,
        trigger_ref,
        wait=False,
    )


def _trusted_parallel_canary(request: ExecutionRequest) -> bool:
    return bool(
        request.relay_protocol_version == RELAY_PROTOCOL_VERSION
        and request.request_kind in PARALLEL_CANARY_REQUEST_KINDS
        and request.completion_mode == "terminal"
        and not request.workflow_ingest
        and request.experiment_id
        and request.attempt_id
    )


def _parallel_dispatch_safe(config: AppConfig, request: ExecutionRequest) -> bool:
    return bool(
        not request.server_action
        and effective_transport(config, request) == "relay"
        and (
            _engine_relay_atomic_completion(request)
            or _trusted_parallel_canary(request)
        )
    )


def _dispatch_request(
    server: RelayServer,
    request: ExecutionRequest,
    trigger_ref: str,
    *,
    after_reexec: bool = False,
) -> int:
    suffix = " after reexec" if after_reexec else ""
    print(
        f"dispatching request {request.request_id}{suffix} for {trigger_ref}",
        flush=True,
    )
    return server.run_request(request, trigger_ref, wait=False)


def _parse_dispatch_request(
    server: RelayServer,
    config: AppConfig,
    trigger_ref: str,
    request_file: str | Path,
) -> ExecutionRequest | None:
    try:
        request = parse_request(config, trigger_ref, request_file)
    except Exception as exc:
        server.reject_request(trigger_ref, exc, request_file)
        print(f"rejected request for {trigger_ref}: {exc}", flush=True)
        return None
    return route_request_for_current_runtime(config, request)


def _dispatch_changed_requests(
    server: RelayServer,
    config: AppConfig,
    trigger_ref: str,
    changed_paths: list[str],
    *,
    after_reexec: bool = False,
) -> int:
    request_files = changed_request_paths(config, changed_paths)
    if not request_files:
        if not after_reexec:
            return _dispatch_or_reject(
                server,
                config,
                trigger_ref,
            )
        return _dispatch_or_reject(
            server,
            config,
            trigger_ref,
            after_reexec=after_reexec,
        )
    parsed: list[ExecutionRequest] = []
    for request_file in request_files:
        request = _parse_dispatch_request(
            server,
            config,
            trigger_ref,
            request_file,
        )
        if request is not None:
            parsed.append(request)

    exit_code = 0
    index = 0
    while index < len(parsed):
        request = parsed[index]
        if not _parallel_dispatch_safe(config, request):
            exit_code = max(
                exit_code,
                _dispatch_request(
                    server,
                    request,
                    trigger_ref,
                    after_reexec=after_reexec,
                ),
            )
            index += 1
            continue

        batch: list[ExecutionRequest] = []
        while (
            index < len(parsed)
            and len(batch) < config.relay.max_parallel_requests
            and _parallel_dispatch_safe(config, parsed[index])
        ):
            batch.append(parsed[index])
            index += 1
        if len(batch) == 1:
            exit_code = max(
                exit_code,
                _dispatch_request(
                    server,
                    batch[0],
                    trigger_ref,
                    after_reexec=after_reexec,
                ),
            )
            continue
        batch_dispatch = getattr(server, "run_requests_batch", None)
        if callable(batch_dispatch):
            exit_code = max(
                exit_code,
                int(
                    batch_dispatch(
                        batch,
                        trigger_ref,
                        wait=False,
                    )
                ),
            )
            continue
        with ThreadPoolExecutor(
            max_workers=len(batch),
            thread_name_prefix="gp-relay-dispatch",
        ) as pool:
            futures = [
                pool.submit(
                    _dispatch_request,
                    server,
                    item,
                    trigger_ref,
                    after_reexec=after_reexec,
                )
                for item in batch
            ]
            for future in futures:
                exit_code = max(exit_code, int(future.result()))
    return exit_code


def _recover_current_server_action(
    server: RelayServer,
    config: AppConfig,
    trigger_ref: str,
) -> bool:
    try:
        request = parse_request(config, trigger_ref)
    except Exception:
        return False
    request = route_request_for_current_runtime(config, request)
    if request is None:
        return False
    if not request.server_action:
        return False
    result_dir = config.repo_dir / config.io.output_dir / request.output_subdir
    status = _read_status_if_present(result_dir / "status.json")
    if not _same_running_server_action(status, request, trigger_ref):
        return False
    if _server_action_owner_alive(status):
        return False
    print(
        "recovering orphaned server-local action "
        f"request={request.request_id} owner_pid={_server_action_pid(status)}",
        flush=True,
    )
    server.run_request(request, trigger_ref, wait=False)
    return True


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="path to config JSON")
    parser.add_argument("--once", action="store_true", help="dispatch current input/job.json once")
    parser.add_argument("--no-wait", action="store_true", help="dispatch payload and return before waiting for B")
    return parser.parse_args()


if __name__ == "__main__":
    main()
