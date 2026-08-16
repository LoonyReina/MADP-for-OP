from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ascendop_daemon.control_plane.endpoint_dispatcher import (
    GitPartnerCanaryTransport,
    parse_last_json_object,
)
from ascendop_daemon.legacy.executor import process_creation_flags, process_startupinfo
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.exchange.runtime_source import (
    apply_transport_runtime_environment,
    load_active_transport_runtime,
)
from ascendop_daemon.registry.system_registry import (
    BackendEndpoint,
    ManagementLink,
    SystemRegistry,
    validate_git_ref,
)


DIRECT_DESCRIPTOR_SCHEMA = "ascendop.gp-diagnostic-target.v1"
DIAGNOSTIC_EVENT_SCHEMA = "ascendop.gp-diagnostic-event.v1"
TERMINAL_GP_STATES = {"success", "failed", "claim_failed", "return_failed"}


class GpDiagnosticError(ValueError):
    pass


@dataclass(frozen=True)
class DirectDiagnosticTarget:
    diagnostic_target_id: str
    gitpartner_repo: str
    result_worktree: str
    control_channel: str
    result_channel: str
    transport: str
    remote_root: str
    target_node_id: str
    target_endpoint_id: str
    target_environment_id: str
    target_gateway_id: str
    expected_generation: str
    append_requests: bool
    duplex_lanes: bool
    cache_root: str = ""

    @classmethod
    def from_endpoint(cls, endpoint: BackendEndpoint) -> "DirectDiagnosticTarget":
        features = {
            str(item)
            for item in endpoint.capabilities.get("features", [])
            if str(item)
        }
        return cls(
            diagnostic_target_id=endpoint.endpoint_id,
            gitpartner_repo=endpoint.gitpartner_repo,
            result_worktree=endpoint.result_worktree,
            control_channel=endpoint.control_channel,
            result_channel=endpoint.result_channel,
            transport=endpoint.transport,
            remote_root=endpoint.remote_root,
            target_node_id=endpoint.node_id,
            target_endpoint_id=endpoint.endpoint_id,
            target_environment_id=endpoint.execution_environment_id,
            target_gateway_id=endpoint.gateway_id,
            expected_generation=endpoint.generation,
            append_requests="gp-append-request-v1" in features,
            duplex_lanes="gp-duplex-lanes-v1" in features,
            cache_root=endpoint.cache_root,
        )

    def as_endpoint(self) -> BackendEndpoint:
        features = []
        if self.append_requests:
            features.append("gp-append-request-v1")
        if self.duplex_lanes:
            features.append("gp-duplex-lanes-v1")
        return BackendEndpoint(
            endpoint_id=f"diagnostic-{self.diagnostic_target_id}",
            node_id=self.target_node_id,
            execution_environment_id=self.target_environment_id,
            gateway_id=self.target_gateway_id,
            transport_mode=(
                "direct-git" if self.transport == "direct" else "lan-relay"
            ),
            enabled=True,
            draining=False,
            gateway_enabled=True,
            gateway_draining=False,
            node_enabled=True,
            node_draining=False,
            environment_enabled=True,
            environment_draining=False,
            priority=0,
            backend_pool="diagnostic-only",
            transport=self.transport,
            gitpartner_repo=self.gitpartner_repo,
            result_worktree=self.result_worktree,
            gitpartner_config="",
            control_channel=self.control_channel,
            result_channel=self.result_channel,
            channel_mode="legacy-shared",
            remote_root=self.remote_root,
            engine_root="control-only",
            cache_root=self.cache_root,
            capabilities={"features": features, "device_count": 0},
            tags=("diagnostic-only",),
            generation=self.expected_generation or "diagnostic-unfenced",
        )


class GpDiagnosticRunner:
    def __init__(
        self,
        root: Path,
        *,
        command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.state_dir = self.root / "TestUtils" / "tester_daemon"
        self.command_runner = command_runner or subprocess.run

    def direct_probe(
        self,
        target: DirectDiagnosticTarget,
        *,
        request_id: str = "",
        wait_seconds: float = 60.0,
        poll_seconds: float = 1.0,
    ) -> dict[str, Any]:
        request_id = request_id or diagnostic_request_id(
            target.diagnostic_target_id
        )
        ensure_safe_token(request_id, "request_id")
        endpoint = target.as_endpoint()
        manifest_path = (
            self.state_dir
            / "gp_diagnostics"
            / "manifests"
            / f"{request_id}.json"
        )
        manifest = {
            "schema": "ascendop.gp-diagnostic-manifest.v1",
            "experiment_id": request_id,
            "task_class": "control-probe",
            "workflow_ingest": False,
            "scheduler_eligible": False,
        }
        write_json_atomic(manifest_path, manifest)
        payload = {
            "manifest_path": str(manifest_path),
            "request_id": request_id,
            "attempt_id": "diagnostic-1",
            "target_endpoint_id": target.target_endpoint_id,
            "target_node_id": target.target_node_id,
            "target_environment_id": target.target_environment_id,
            "target_gateway_id": target.target_gateway_id,
            "target_transport_mode": (
                "direct-git" if target.transport == "direct" else "lan-relay"
            ),
            "target_generation": target.expected_generation,
        }
        transport = GitPartnerCanaryTransport(
            self.root,
            endpoint,
            command_timeout_seconds=max(15, int(wait_seconds) or 15),
        )
        started_at = utc_now()
        started_monotonic = time.monotonic()
        publish_seconds = 0.0
        query_seconds = 0.0
        query_count = 0
        sleep_seconds = 0.0
        result: dict[str, Any] = {
            "schema": "ascendop.gp-diagnostic-result.v1",
            "request_id": request_id,
            "mode": "direct-gp",
            "diagnostic_target_id": target.diagnostic_target_id,
            "target_node_id": target.target_node_id,
            "target_endpoint_id": target.target_endpoint_id,
            "registered_endpoint_required": False,
            "scheduler_eligible": False,
            "workflow_ingest": False,
            "generation_fenced": bool(target.expected_generation),
            "started_at": started_at,
        }

        def record() -> dict[str, Any]:
            result.setdefault("finished_at", utc_now())
            result["timing"] = direct_probe_timing(
                result,
                local_elapsed_seconds=time.monotonic() - started_monotonic,
                publish_seconds=publish_seconds,
                query_seconds=query_seconds,
                query_count=query_count,
                sleep_seconds=sleep_seconds,
            )
            return self._record(result)

        try:
            publish_started = time.monotonic()
            delivery = transport.publish(payload)
            publish_seconds = time.monotonic() - publish_started
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            publish_seconds = time.monotonic() - publish_started
            result.update(
                state="failed",
                error=bounded(f"{type(exc).__name__}: {exc}"),
                finished_at=utc_now(),
            )
            return record()
        result["state"] = delivery.status
        result["delivery"] = {
            "status": delivery.status,
            "error": bounded(delivery.error),
        }
        if delivery.status == "retry":
            result["state"] = "failed"
            result["finished_at"] = utc_now()
            return record()
        if wait_seconds <= 0:
            result["state"] = "published"
            result["finished_at"] = utc_now()
            return record()

        deadline = time.monotonic() + max(0.0, float(wait_seconds))
        last_error = ""
        last_acceptance: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            try:
                query_started = time.monotonic()
                observation = transport.query(payload)
                query_seconds += time.monotonic() - query_started
                query_count += 1
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                query_seconds += time.monotonic() - query_started
                query_count += 1
                last_error = f"{type(exc).__name__}: {exc}"
                sleep_for = max(0.05, float(poll_seconds))
                time.sleep(sleep_for)
                sleep_seconds += sleep_for
                continue
            if observation.acceptance is not None:
                last_acceptance = observation.acceptance
            if observation.error:
                last_error = observation.error
            if observation.result is not None:
                result["state"] = (
                    "completed"
                    if observation.result.get("outcome") == "success"
                    else "failed"
                )
                result["acceptance"] = last_acceptance
                result["remote_result"] = observation.result
                result["finished_at"] = utc_now()
                return record()
            sleep_for = max(0.05, float(poll_seconds))
            time.sleep(sleep_for)
            sleep_seconds += sleep_for
        result["state"] = "uncertain"
        result["acceptance"] = last_acceptance
        result["error"] = bounded(
            last_error or "diagnostic result was not visible before timeout"
        )
        result["finished_at"] = utc_now()
        return record()

    def link_probe(
        self,
        link: ManagementLink,
        *,
        request_id: str = "",
        wait_seconds: float = 60.0,
        poll_seconds: float = 1.0,
    ) -> dict[str, Any]:
        if not link.enabled:
            raise GpDiagnosticError(f"management link is disabled: {link.link_id}")
        if "lan-diagnose" not in link.allowed_actions:
            raise GpDiagnosticError(
                f"management link does not allow lan-diagnose: {link.link_id}"
            )
        request_id = request_id or diagnostic_request_id(link.link_id)
        ensure_safe_token(request_id, "request_id")
        repo = resolve_workspace_path(self.root, link.gitpartner_repo)
        result_repo = resolve_workspace_path(self.root, link.result_worktree)
        if not (repo / ".git").exists():
            raise GpDiagnosticError(f"management GP worktree is missing: {repo}")
        command = build_link_probe_command(link, request_id=request_id)
        started_at = utc_now()
        result: dict[str, Any] = {
            "schema": "ascendop.gp-diagnostic-result.v1",
            "request_id": request_id,
            "mode": "gateway-management-link",
            "management_link_id": link.link_id,
            "source_gateway_id": link.source_gateway_id,
            "target_node_id": link.target_node_id,
            "registered_target_required": False,
            "scheduler_eligible": False,
            "workflow_ingest": False,
            "action": "lan-diagnose",
            "started_at": started_at,
        }
        try:
            completed = self._run_gp(
                command,
                repo=repo,
                control_channel=link.control_channel,
                result_channel=link.result_channel,
                endpoint_id=f"management-{link.link_id}",
                lock_name=f"gp_management_{link.link_id}_ingress",
                timeout_seconds=max(15, int(wait_seconds) or 15),
            )
        except subprocess.TimeoutExpired as exc:
            result.update(
                state="uncertain",
                error=f"GP management publish timed out after {exc.timeout}s",
                finished_at=utc_now(),
            )
            return self._record(result)
        if completed.returncode != 0:
            result.update(
                state="failed",
                error=bounded(completed.stderr or completed.stdout),
                finished_at=utc_now(),
            )
            return self._record(result)
        result["state"] = "published"
        if wait_seconds <= 0:
            result["finished_at"] = utc_now()
            return self._record(result)

        deadline = time.monotonic() + max(0.0, float(wait_seconds))
        last_error = ""
        while time.monotonic() < deadline:
            query_command = [
                sys.executable,
                "-m",
                "limited_remote_partner.result_query",
                "--repo",
                str(result_repo),
                "--output-subdir",
                request_id,
                "--result-branch",
                link.result_channel,
                "--control-branch",
                link.control_channel,
                "--remote",
                "origin",
            ]
            try:
                queried = self._run_gp(
                    query_command,
                    repo=result_repo,
                    package_repo=repo,
                    control_channel=link.control_channel,
                    result_channel=link.result_channel,
                    endpoint_id=f"management-{link.link_id}",
                    lock_name=f"gp_management_{link.link_id}_result",
                    timeout_seconds=max(15, int(wait_seconds)),
                )
            except subprocess.TimeoutExpired as exc:
                last_error = f"GP management query timed out after {exc.timeout}s"
                time.sleep(max(0.05, float(poll_seconds)))
                continue
            if queried.returncode != 0:
                last_error = bounded(queried.stderr or queried.stdout)
            else:
                try:
                    observed = parse_last_json_object(queried.stdout)
                except ValueError as exc:
                    last_error = str(exc)
                else:
                    status = observed.get("status")
                    if isinstance(status, dict):
                        state = str(status.get("state") or "")
                        result["remote_status"] = bounded_mapping(status)
                        if state in TERMINAL_GP_STATES:
                            result["state"] = (
                                "completed" if state == "success" else "failed"
                            )
                            if state != "success":
                                result["error"] = bounded(
                                    str(status.get("error") or state)
                                )
                            result["finished_at"] = utc_now()
                            return self._record(result)
            time.sleep(max(0.05, float(poll_seconds)))
        result.update(
            state="uncertain",
            error=last_error or "diagnostic result was not visible before timeout",
            finished_at=utc_now(),
        )
        return self._record(result)

    def _run_gp(
        self,
        command: list[str],
        *,
        repo: Path,
        control_channel: str,
        result_channel: str,
        endpoint_id: str,
        lock_name: str,
        timeout_seconds: int,
        package_repo: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        assert_no_local_remote_shell(command)
        package_repo = package_repo or repo
        runtime = load_active_transport_runtime(self.root)
        env = os.environ.copy()
        env["GITPARTNER_BRANCH"] = control_channel
        env["GITPARTNER_RESULT_BRANCH"] = result_channel
        env["GITPARTNER_REMOTE"] = "origin"
        env["GITPARTNER_ENDPOINT_ID"] = endpoint_id
        token_file = package_repo / "api.txt"
        if token_file.is_file():
            env["GITPARTNER_TOKEN_FILE"] = str(token_file)
        apply_transport_runtime_environment(env, runtime)
        with NamedProcessLock(
            self.root,
            lock_name,
            stale_after_seconds=120,
            wait_timeout_seconds=300,
        ):
            return self.command_runner(
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
                timeout=max(5, int(timeout_seconds)),
                creationflags=process_creation_flags(),
                startupinfo=process_startupinfo(),
            )

    def _record(self, result: dict[str, Any]) -> dict[str, Any]:
        request_id = str(result["request_id"])
        result["recorded_at"] = utc_now()
        request_path = (
            self.state_dir
            / "gp_diagnostics"
            / "requests"
            / f"{request_id}.json"
        )
        write_json_atomic(request_path, result)
        event = {
            "schema": DIAGNOSTIC_EVENT_SCHEMA,
            "event_id": hashlib.sha256(
                (
                    f"{request_id}:{result.get('state')}:{result['recorded_at']}"
                ).encode("utf-8")
            ).hexdigest(),
            "event_type": "gp.diagnostic.observed",
            "event_version": 1,
            "occurred_at": str(result.get("finished_at") or result["recorded_at"]),
            "observed_at": result["recorded_at"],
            "recorded_at": result["recorded_at"],
            "level": (
                "INFO"
                if result.get("state") in {"published", "completed"}
                else "WARN"
            ),
            "component": "tester-daemon.gp-diagnostic",
            "instance_id": "local-gp-diagnostic",
            "trace_id": request_id,
            "request_id": request_id,
            "outcome": str(result.get("state") or "unknown"),
            "reason_code": (
                "gp_diagnostic_ok"
                if result.get("state") in {"published", "completed"}
                else "gp_diagnostic_incomplete"
            ),
            "message": (
                "GP diagnostic completed"
                if result.get("state") == "completed"
                else f"GP diagnostic state {result.get('state')}"
            ),
            "details": {
                key: value
                for key, value in result.items()
                if key
                in {
                    "mode",
                    "management_link_id",
                    "diagnostic_target_id",
                    "source_gateway_id",
                    "target_node_id",
                    "target_endpoint_id",
                    "action",
                    "scheduler_eligible",
                    "workflow_ingest",
                    "generation_fenced",
                    "timing",
                    "error",
                }
            },
        }
        events_path = self.state_dir / "gp_diagnostic_events.jsonl"
        events_path.parent.mkdir(parents=True, exist_ok=True)
        with events_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, ensure_ascii=True, sort_keys=True) + "\n")
        return {**result, "record_path": str(request_path)}


def direct_target_from_descriptor(
    root: Path,
    descriptor_path: Path,
) -> DirectDiagnosticTarget:
    raw = json.loads(descriptor_path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise GpDiagnosticError("GP diagnostic descriptor must be an object")
    if str(raw.get("schema") or "") != DIRECT_DESCRIPTOR_SCHEMA:
        raise GpDiagnosticError(
            f"GP diagnostic descriptor schema must be {DIRECT_DESCRIPTOR_SCHEMA}"
        )
    target_id = safe_token(raw, "diagnostic_target_id")
    transport = str(raw.get("transport") or "direct")
    if transport not in {"direct", "relay", "auto"}:
        raise GpDiagnosticError(f"invalid diagnostic transport: {transport}")
    control_channel = str(raw.get("control_channel") or "")
    result_channel = str(raw.get("result_channel") or control_channel)
    validate_git_ref(control_channel)
    validate_git_ref(result_channel)
    repo = workspace_relative_path(root, str(raw.get("gitpartner_repo") or ""))
    result_worktree = workspace_relative_path(
        root,
        str(raw.get("result_worktree") or repo),
    )
    return DirectDiagnosticTarget(
        diagnostic_target_id=target_id,
        gitpartner_repo=repo,
        result_worktree=result_worktree,
        control_channel=control_channel,
        result_channel=result_channel,
        transport=transport,
        remote_root=str(raw.get("remote_root") or "."),
        target_node_id=safe_token(raw, "target_node_id"),
        target_endpoint_id=safe_token(raw, "target_endpoint_id"),
        target_environment_id=optional_safe_token(
            raw,
            "target_environment_id",
        ),
        target_gateway_id=optional_safe_token(raw, "target_gateway_id"),
        expected_generation=str(raw.get("expected_generation") or ""),
        append_requests=bool(raw.get("append_requests", False)),
        duplex_lanes=bool(raw.get("duplex_lanes", False)),
        cache_root=str(raw.get("cache_root") or ""),
    )


def endpoint_target(registry: SystemRegistry, endpoint_id: str) -> DirectDiagnosticTarget:
    for endpoint in registry.endpoints:
        if endpoint.endpoint_id == endpoint_id:
            return DirectDiagnosticTarget.from_endpoint(endpoint)
    raise GpDiagnosticError(f"unknown route endpoint: {endpoint_id}")


def build_link_probe_command(
    link: ManagementLink,
    *,
    request_id: str,
) -> list[str]:
    ensure_safe_token(request_id, "request_id")
    command = [
        sys.executable,
        "-m",
        "limited_remote_partner.gateway.submit_job",
        "--commit-push",
    ]
    if link.append_requests:
        command.append("--append-request")
    command.extend(
        [
            "lan-bootstrap",
            "--request-id",
            request_id,
            "--output-subdir",
            request_id,
            "--action",
            "lan-diagnose",
            "--target-role",
            link.target_role,
            "--target-host",
            link.target_host,
            "--target-dir",
            link.target_dir,
            "--diagnose-request-id",
            request_id,
        ]
    )
    assert_no_local_remote_shell(command)
    return command


def assert_no_local_remote_shell(command: list[str]) -> None:
    forbidden = {"ssh", "scp", "sftp", "plink", "pscp"}
    executables = {
        Path(item).name.lower()
        for item in command
        if item and not str(item).startswith("-")
    }
    if executables.intersection(forbidden):
        raise GpDiagnosticError("local remote-shell fallback is forbidden")


def diagnostic_request_id(label: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", label).strip(".-") or "target"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"gpdiag-{safe}-{stamp}-{secrets.token_hex(3)}"


def resolve_workspace_path(root: Path, value: str) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    if resolved != root and root not in resolved.parents:
        raise GpDiagnosticError(f"path escapes workspace: {value}")
    return resolved


def workspace_relative_path(root: Path, value: str) -> str:
    if not value:
        raise GpDiagnosticError("GP diagnostic worktree path is required")
    return resolve_workspace_path(root.resolve(), value).relative_to(
        root.resolve()
    ).as_posix()


def safe_token(raw: dict[str, Any], key: str) -> str:
    value = str(raw.get(key) or "")
    ensure_safe_token(value, key)
    return value


def optional_safe_token(raw: dict[str, Any], key: str) -> str:
    value = str(raw.get(key) or "")
    if value:
        ensure_safe_token(value, key)
    return value


def ensure_safe_token(value: str, label: str) -> None:
    if not value or not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise GpDiagnosticError(f"{label} must be a non-empty safe token")


def assert_target_is_not_scheduler_registered(
    registry: SystemRegistry,
    target_node_id: str,
) -> bool:
    return all(
        endpoint.node_id != target_node_id for endpoint in registry.endpoints
    )


def bounded(value: object, limit: int = 4000) -> str:
    return str(value or "").strip()[-limit:]


def bounded_mapping(value: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, str):
            result[str(key)] = bounded(item)
        elif isinstance(item, (bool, int, float)) or item is None:
            result[str(key)] = item
        elif isinstance(item, list):
            result[str(key)] = item[:50]
    return result


def direct_probe_timing(
    result: dict[str, Any],
    *,
    local_elapsed_seconds: float,
    publish_seconds: float,
    query_seconds: float,
    query_count: int,
    sleep_seconds: float,
) -> dict[str, Any]:
    timing: dict[str, Any] = {
        "local_elapsed_seconds": round(max(0.0, local_elapsed_seconds), 6),
        "publish_seconds": round(max(0.0, publish_seconds), 6),
        "query_seconds": round(max(0.0, query_seconds), 6),
        "query_count": max(0, int(query_count)),
        "poll_sleep_seconds": round(max(0.0, sleep_seconds), 6),
    }
    accounted = publish_seconds + query_seconds + sleep_seconds
    timing["local_unaccounted_seconds"] = round(
        max(0.0, local_elapsed_seconds - accounted),
        6,
    )
    remote = result.get("remote_result")
    if not isinstance(remote, dict):
        return timing
    remote_started = parse_utc_timestamp(remote.get("started_at"))
    remote_finished = parse_utc_timestamp(remote.get("finished_at"))
    local_started = parse_utc_timestamp(result.get("started_at"))
    local_finished = parse_utc_timestamp(result.get("finished_at"))
    if remote_started is None or remote_finished is None:
        return timing
    remote_elapsed = max(0.0, (remote_finished - remote_started).total_seconds())
    timing["remote_execution_seconds"] = round(remote_elapsed, 6)
    if local_started is None or local_finished is None:
        return timing
    local_midpoint = local_started + (local_finished - local_started) / 2
    remote_midpoint = remote_started + (remote_finished - remote_started) / 2
    offset = (remote_midpoint - local_midpoint).total_seconds()
    uncertainty = max(0.0, (local_elapsed_seconds - remote_elapsed) / 2)
    timing["estimated_remote_clock_offset_seconds"] = round(offset, 6)
    timing["clock_offset_uncertainty_seconds"] = round(uncertainty, 6)
    timing["cross_node_raw_timestamp_comparison_safe"] = (
        abs(offset) <= uncertainty + 1.0
    )
    return timing


def parse_utc_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
