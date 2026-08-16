from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import AppConfig
from limited_remote_partner.gateway.git_client import GitClient
from limited_remote_partner.endpoint.node_enrollment import node_registration_status, normalize_node_id
from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs, process_start_token
from limited_remote_partner.gateway.relay import ScpTransport, write_json


NODE_REPORT_SCHEMA = "git-partner.node-report.v1"
ACTIVE_NODE_STATES = {
    "starting",
    "discovering",
    "awaiting-acceptance",
    "ready",
    "degraded",
}


class NodeLifecycleError(RuntimeError):
    pass


class NodeLifecycleReporter:
    def __init__(self, config: AppConfig, config_path: Path, role: str) -> None:
        self.config = config
        self.config_path = config_path.resolve()
        self.role = role
        self.enabled = config.node_lifecycle.enabled
        self.node_id = config.node.node_id
        self.session_id = uuid.uuid4().hex
        self.boot_id = _boot_id()
        self.started_at = _utc_now()
        self.state_dir = config.repo_dir / config.io.state_dir
        self.local_report_path = self.state_dir / "presence.json"
        self.public_report_path = (
            config.repo_dir
            / config.io.output_dir
            / "_control"
            / "nodes"
            / self.node_id
            / "report.json"
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._capabilities: dict[str, Any] = {}
        self._sequence = 0
        self._last_publish_error = ""
        self._last_ack_signature = self._central_ack_signature()

    def start(self) -> None:
        if not self.enabled:
            return
        status = node_registration_status(
            config_path=self.config_path,
            repo_dir=self.config.repo_dir,
        )
        if not status.get("runnable"):
            raise NodeLifecycleError(
                f"node enrollment is not runnable: {status.get('code')} "
                f"{status.get('message')}"
            )
        self._publish("starting", "process-started")
        self._capabilities = (
            probe_node_capabilities(self.config)
            if self.config.node_lifecycle.probe_on_start
            else {"ready": False, "probe_skipped": True}
        )
        self._publish(self._operational_state(), "initial-probe-complete")
        self._last_ack_signature = self._central_ack_signature()
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"gitpartner-node-heartbeat-{self.node_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, reason: str = "process-exit") -> None:
        if not self.enabled:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._publish("offline", reason, lease=False)

    def __enter__(self) -> NodeLifecycleReporter:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, _tb: object) -> None:
        reason = "process-error" if exc_type is not None else "process-exit"
        if exc is not None:
            reason = f"{reason}:{type(exc).__name__}"
        self.stop(reason)

    def _heartbeat_loop(self) -> None:
        heartbeat_seconds = max(
            1.0,
            float(self.config.node_lifecycle.heartbeat_seconds),
        )
        next_heartbeat = time.monotonic() + heartbeat_seconds
        while not self._stop.wait(
            min(1.0, max(0.05, next_heartbeat - time.monotonic()))
        ):
            try:
                ack_signature = self._central_ack_signature()
                if ack_signature != self._last_ack_signature:
                    self._last_ack_signature = ack_signature
                    self._publish(
                        self._operational_state(),
                        "central-ack-updated",
                    )
                if time.monotonic() >= next_heartbeat:
                    self._refresh_dynamic_capabilities()
                    self._publish(self._operational_state(), "heartbeat")
                    next_heartbeat = time.monotonic() + heartbeat_seconds
            except Exception as exc:
                self._last_publish_error = (
                    f"heartbeat-loop:{type(exc).__name__}: {exc}"
                )
                print(
                    "GITPARTNER_NODE_HEARTBEAT_ERROR "
                    + json.dumps(
                        {
                            "node_id": self.node_id,
                            "session_id": self.session_id,
                            "error": self._last_publish_error,
                        },
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                    flush=True,
                )
                try:
                    self._publish("degraded", "heartbeat-loop-error")
                except Exception as publish_exc:
                    self._last_publish_error = (
                        f"heartbeat-publish:{type(publish_exc).__name__}: "
                        f"{publish_exc}"
                    )
                next_heartbeat = time.monotonic() + heartbeat_seconds

    def _central_ack_signature(self) -> str:
        ack = _read_json(self.state_dir / "central_ack.json")
        return _stable_digest(
            {
                key: ack.get(key)
                for key in (
                    "state",
                    "node_id",
                    "endpoint_id",
                    "generation",
                    "session_id",
                    "accepted_at",
                )
            }
        )

    def _refresh_dynamic_capabilities(self) -> None:
        if not self._capabilities:
            return
        self._capabilities = refresh_engine_capability(
            self.config,
            self._capabilities,
        )

    def _operational_state(self) -> str:
        ack = _read_json(self.state_dir / "central_ack.json")
        accepted = (
            self.config.node_lifecycle.registration_state == "accepted"
            or (
                ack.get("state") == "accepted"
                and ack.get("node_id") == self.node_id
                and ack.get("generation") == self.config.endpoint.generation
                and ack.get("session_id") == self.session_id
            )
        )
        if not self._capabilities.get("ready"):
            return "degraded"
        if not accepted:
            return "awaiting-acceptance"
        return "ready"

    def _publish(self, state: str, reason: str, *, lease: bool = True) -> None:
        self._sequence += 1
        now = datetime.now(timezone.utc)
        report = {
            "schema": NODE_REPORT_SCHEMA,
            "node_id": self.node_id,
            "endpoint_id": self.config.endpoint.endpoint_id or self.node_id,
            "execution_environment_id": self.config.endpoint.execution_environment_id,
            "gateway_id": self.config.endpoint.gateway_id,
            "transport_mode": self.config.endpoint.transport_mode,
            "generation": self.config.endpoint.generation,
            "session_id": self.session_id,
            "boot_id": self.boot_id,
            "sequence": self._sequence,
            "state": state,
            "reason": reason,
            "role": self.role,
            "pid": os.getpid(),
            "process_start_token": process_start_token(os.getpid()),
            "started_at": self.started_at,
            "heartbeat_at": _format_time(now),
            "lease_expires_at": (
                _format_time(
                    now + timedelta(seconds=self.config.node_lifecycle.lease_seconds)
                )
                if lease
                else _format_time(now)
            ),
            "registration_state": self.config.node_lifecycle.registration_state,
            "config_path": _portable_path(
                self.config_path,
                self.config.repo_dir,
            ),
            "channels": {
                "control": self.config.repo.branch,
                "result": self.config.repo.result_branch or "",
                "node_report": self.config.node_lifecycle.report_branch,
            },
            "capability_generation": _capability_generation(self._capabilities),
            "capabilities": self._capabilities,
            "last_publish_error": self._last_publish_error,
        }
        write_json(
            self.local_report_path,
            report,
            self.config.io.max_file_bytes,
        )
        write_json(
            self.public_report_path,
            report,
            self.config.io.max_file_bytes,
        )
        try:
            self._publish_remote(report)
            self._last_publish_error = ""
            report["last_publish_error"] = ""
            write_json(
                self.local_report_path,
                report,
                self.config.io.max_file_bytes,
            )
        except Exception as exc:
            self._last_publish_error = f"{type(exc).__name__}: {exc}"
            report["last_publish_error"] = self._last_publish_error
            write_json(
                self.local_report_path,
                report,
                self.config.io.max_file_bytes,
            )
        _print_lifecycle_event(report)

    def _publish_remote(self, report: dict[str, Any]) -> None:
        mode = self.config.node_lifecycle.publish_mode
        if mode == "file":
            return
        relay_candidate = (
            self.role == "client"
            and self.config.relay.transport_mode in {"relay", "auto"}
        )
        if mode == "relay" or (mode == "auto" and relay_candidate):
            try:
                self._publish_relay()
                return
            except Exception:
                if (
                    mode == "relay"
                    or self.config.relay.transport_mode == "relay"
                ):
                    raise
        self._publish_git(report)

    def _publish_git(self, report: dict[str, Any]) -> None:
        report_branch = self.config.node_lifecycle.report_branch
        repo = replace(self.config.repo, result_branch=report_branch)
        git = GitClient(repo, self.config.repo_dir)
        git.ensure_worktree()
        relative = self.public_report_path.relative_to(
            self.config.repo_dir
        ).as_posix()
        git.commit_and_push(
            [relative],
            f"git_partner: node {self.node_id} {report['state']}",
            self.config.io.max_file_bytes,
        )

    def _publish_relay(self) -> None:
        transport = ScpTransport(self.config)
        root = self.config.relay.server_return_dir.rstrip("/")
        destination = f"{root}/_nodes/{self.node_id}/report.json"
        temporary = f"{destination}.tmp.{self.session_id}"
        transport.push_file(
            self.public_report_path,
            self.config.relay.server_ssh,
            temporary,
        )
        transport.run_ssh(
            self.config.relay.server_ssh,
            ["mv", "-f", temporary, destination],
        )


def _print_lifecycle_event(report: dict[str, Any]) -> None:
    capabilities = report.get("capabilities")
    if not isinstance(capabilities, dict):
        capabilities = {}
    blockers = capabilities.get("blockers")
    if not isinstance(blockers, list):
        blockers = []
    event = {
        "node_id": str(report.get("node_id") or ""),
        "endpoint_id": str(report.get("endpoint_id") or ""),
        "generation": str(report.get("generation") or ""),
        "sequence": int(report.get("sequence") or 0),
        "state": str(report.get("state") or ""),
        "reason": str(report.get("reason") or ""),
        "heartbeat_at": str(report.get("heartbeat_at") or ""),
        "lease_expires_at": str(report.get("lease_expires_at") or ""),
        "capability_ready": bool(capabilities.get("ready")),
        "capability_blockers": [str(value) for value in blockers],
        "publish_error": str(report.get("last_publish_error") or ""),
    }
    print(
        "GITPARTNER_NODE_LIFECYCLE "
        + json.dumps(event, ensure_ascii=True, sort_keys=True),
        flush=True,
    )


def probe_node_capabilities(config: AppConfig) -> dict[str, Any]:
    npu_smi = shutil.which("npu-smi")
    npu_output = ""
    npu_error = ""
    if npu_smi:
        try:
            result = subprocess.run(
                [npu_smi, "info"],
                stdin=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=8,
                check=False,
                **hidden_subprocess_kwargs(),
            )
            npu_output = result.stdout
            if result.returncode != 0:
                npu_error = result.stderr.strip() or f"exit={result.returncode}"
        except (OSError, subprocess.TimeoutExpired) as exc:
            npu_error = f"{type(exc).__name__}: {exc}"
    physical_device_ids = _detect_physical_device_ids(npu_output)
    detected_soc_models = {
        match.group(0).replace(" ", "")
        for match in re.finditer(
            r"(?:Ascend\s*)?(?:910B\d*|910B|310P\d*|310B\d*)",
            npu_output,
            flags=re.IGNORECASE,
        )
    }
    soc_models = sorted({*detected_soc_models, *config.endpoint.soc})
    device_present = bool(physical_device_ids or detected_soc_models)
    cann = _detect_cann()
    cann["versions"] = sorted(
        {
            *(str(value) for value in cann.get("versions", [])),
            *config.endpoint.cann,
        }
    )
    engine_path = _engine_path(config)
    features = sorted(set(config.node.capabilities))
    device_required = bool(
        {"operator-test", "profiler"}.intersection(features)
    )
    engine_required = any(feature.startswith("engine-") for feature in features)
    engine = _probe_engine(config, engine_path)
    cache_adapters = _supported_cache_adapters(config, engine_path)
    profiler = _probe_profiler(cann)
    device_inventory = _runtime_device_inventory(
        physical_device_ids,
        soc_models=soc_models,
    )
    device_ids = [str(item["device_id"]) for item in device_inventory]
    device_ready = device_present and bool(cann["homes"])
    blockers = [
        blocker
        for condition, blocker in (
            (not device_required or device_present, "npu-not-detected"),
            (not device_required or bool(cann["homes"]), "cann-not-detected"),
            (not engine_required or bool(engine.get("present")), "engine-not-present"),
            (
                not engine_required or bool(engine.get("snapshot_available")),
                "engine-snapshot-unavailable",
            ),
            (
                "profiler" not in features or bool(profiler.get("available")),
                "profiler-not-detected",
            ),
        )
        if not condition
    ]
    ready = not blockers and (device_ready if device_required else True)
    return {
        "observed_at": _utc_now(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "execution_environment_id": config.endpoint.execution_environment_id,
        "endpoint_generation": config.endpoint.generation,
        "features": features,
        "npu_smi": {
            "path": npu_smi or "",
            "available": bool(npu_smi),
            "error": npu_error,
        },
        "soc": soc_models,
        "physical_device_ids": physical_device_ids,
        "device_ids": device_ids,
        "device_count": len(device_ids),
        "device_inventory": device_inventory,
        "cann": cann,
        "engine": engine,
        "cache_adapters": cache_adapters,
        "profiler": profiler,
        "runtime_scopes": _runtime_scopes(config, engine_path),
        "ready": ready,
        "blockers": blockers,
    }


def _detect_physical_device_ids(npu_output: str) -> list[str]:
    physical_device_ids = sorted(
        path.name.removeprefix("davinci")
        for path in Path("/dev").glob("davinci[0-9]*")
        if path.name.removeprefix("davinci").isdigit()
    )
    if not physical_device_ids:
        physical_device_ids = sorted(
            set(re.findall(r"(?m)^\|\s*(\d+)\s+", npu_output)),
            key=_device_id_sort_key,
        )
    return physical_device_ids


def _device_id_sort_key(device_id: str) -> tuple[int, int | str]:
    if device_id.isdigit():
        return (0, int(device_id))
    return (1, device_id)


def _runtime_device_inventory(
    physical_device_ids: list[str],
    *,
    soc_models: list[str],
) -> list[dict[str, Any]]:
    physical_ids = sorted(
        {str(device_id) for device_id in physical_device_ids},
        key=_device_id_sort_key,
    )
    soc = soc_models[0] if len(soc_models) == 1 else ""
    return [
        {
            "device_id": str(logical_id),
            "physical_device_id": physical_device_id,
            "soc": soc,
            "lease_resource": f"npu:{logical_id}",
            "exclusive_measurement": True,
        }
        for logical_id, physical_device_id in enumerate(physical_ids)
    ]


def refresh_engine_capability(
    config: AppConfig,
    capabilities: dict[str, Any],
) -> dict[str, Any]:
    refreshed = dict(capabilities)
    engine = _probe_engine(config, _engine_path(config))
    refreshed["engine"] = engine
    refreshed["observed_at"] = _utc_now()
    features = {
        str(value)
        for value in refreshed.get("features", config.node.capabilities)
    }
    blockers = [
        str(value)
        for value in refreshed.get("blockers", [])
        if str(value)
        not in {"engine-not-present", "engine-snapshot-unavailable"}
    ]
    if any(feature.startswith("engine-") for feature in features):
        if not engine.get("present"):
            blockers.append("engine-not-present")
        elif not engine.get("snapshot_available"):
            blockers.append("engine-snapshot-unavailable")
    refreshed["blockers"] = sorted(set(blockers))
    refreshed["ready"] = not refreshed["blockers"]
    return refreshed


def _probe_engine(
    config: AppConfig,
    engine_path: Path | None,
) -> dict[str, Any]:
    identity = {
        "endpoint_id": config.endpoint.endpoint_id,
        "execution_environment_id": config.endpoint.execution_environment_id,
        "endpoint_generation": config.endpoint.generation,
        "path": str(engine_path) if engine_path else "",
    }
    value: dict[str, Any] = {
        **identity,
        "engine_id": _stable_digest(identity)[:24],
        "present": bool(engine_path and engine_path.exists()),
        "snapshot_available": False,
        "scheduler_policy": {
            "host_preparation": "parallel",
            "device_execution": "exclusive-per-physical-device",
            "postprocess": "parallel-by-resource",
            "terminal_return": "independent",
        },
    }
    if not value["present"] or engine_path is None:
        return value
    try:
        runtime_source = _engine_runtime_source(engine_path)
        if runtime_source is not None:
            snapshot = _engine_cli_observation(
                engine_path,
                runtime_source,
                "status",
            )
            resident = _engine_cli_observation(
                engine_path,
                runtime_source,
                "service-status",
            )
        else:
            from limited_remote_partner.engine.test_engine import TestEngine

            instance = TestEngine(engine_path)
            snapshot = instance.snapshot()
            resident = instance.resident_status()
    except Exception as exc:
        value["snapshot_error"] = f"{type(exc).__name__}: {exc}"
        return value
    capacity = snapshot.get("capacity", {})
    if not isinstance(capacity, dict):
        capacity = {}
    value.update(
        {
            "snapshot_available": True,
            "protocol_version": str(snapshot.get("protocol_version") or ""),
            "generation": str(snapshot.get("engine_generation") or ""),
            "code_generation": str(
                snapshot.get("engine_code_generation") or ""
            ),
            "capacity": {
                key: capacity.get(key)
                for key in (
                    "max_inflight",
                    "active_job_slots",
                    "host_slots",
                    "device_slots",
                    "export_slots",
                    "standby_slots",
                    "draining",
                )
            },
            "available": {
                "admission_credit": int(snapshot.get("admission_credit", 0)),
                "standby_credit": int(snapshot.get("standby_credit", 0)),
                "active_job_slots": int(
                    snapshot.get("active_job_slots_free", 0)
                ),
                "host_slots": int(snapshot.get("host_slots_free", 0)),
                "device_slots": int(snapshot.get("device_slots_free", 0)),
                "export_slots": int(snapshot.get("export_slots_free", 0)),
            },
            "backlog": {
                "accepted_nonterminal": int(
                    snapshot.get("accepted_nonterminal", 0)
                ),
                "queued_nonterminal": int(
                    snapshot.get("queued_nonterminal", 0)
                ),
                "return_ready_count": int(
                    snapshot.get("return_ready_count", 0)
                ),
                "return_backlog_bytes": int(
                    snapshot.get("return_backlog_bytes", 0)
                ),
                "return_backpressure_active": bool(
                    snapshot.get("return_backpressure_active", False)
                ),
            },
            "resident": {
                key: resident.get(key)
                for key in (
                    "pid",
                    "alive",
                    "heartbeat_age_seconds",
                    "heartbeat_stale_seconds",
                    "heartbeat_fresh",
                    "code_generation",
                    "current_code_generation",
                    "code_generation_current",
                    "resident_ok",
                    "stop_requested",
                )
            },
        }
    )
    return value


def _engine_runtime_source(engine_path: Path) -> Path | None:
    pointer_path = engine_path / "runtime" / "current"
    try:
        generation = pointer_path.read_text(encoding="ascii").strip().lower()
    except OSError:
        return None
    if (
        len(generation) != 16
        or any(char not in "0123456789abcdef" for char in generation)
    ):
        raise NodeLifecycleError(
            f"invalid Engine runtime pointer: {pointer_path}"
        )
    runtime_source = engine_path / "runtime" / "generations" / generation / "src"
    if not (runtime_source / "limited_remote_partner").is_dir():
        raise NodeLifecycleError(
            f"Engine runtime source is missing: {runtime_source}"
        )
    return runtime_source


def _engine_cli_observation(
    engine_path: Path,
    runtime_source: Path,
    command: str,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (
            str(runtime_source),
            env.get("PYTHONPATH", ""),
        )
        if item
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "limited_remote_partner.cli.test_engine_cli",
            "--root",
            str(engine_path),
            command,
        ],
        cwd=str(engine_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise NodeLifecycleError(
            f"Engine runtime {command} failed with exit {completed.returncode}: "
            f"{detail[:512]}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise NodeLifecycleError(
            f"Engine runtime {command} returned invalid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise NodeLifecycleError(
            f"Engine runtime {command} returned a non-object payload"
        )
    return payload


def _supported_cache_adapters(
    config: AppConfig,
    engine_path: Path | None,
) -> list[dict[str, Any]]:
    package_root = Path(__file__).resolve().parents[1]
    candidates = (
        ("case-cache-v1", "resources/case_cache.py", "case"),
        ("wheel-cas-v1", "resources/wheel_cache.py", "wheel"),
        (
            "operator-build-cas-v1",
            "resources/operator_cache.py",
            "operator-build",
        ),
        (
            "runtime-readiness-v1",
            "observability/runtime_readiness.py",
            "runtime-readiness",
        ),
    )
    configured_cache_root = os.environ.get("ASCENDOP_ENGINE_CACHE_ROOT", "").strip()
    cache_root = (
        Path(configured_cache_root).expanduser()
        if configured_cache_root
        else engine_path / "cache"
        if engine_path
        else None
    )
    return [
        {
            "adapter_id": adapter_id,
            "kind": kind,
            "supported": True,
            "scope": (
                str(cache_root / kind)
                if cache_root is not None
                else ""
            ),
            "writable": bool(cache_root),
            "execution_environment_id": (
                config.endpoint.execution_environment_id
            ),
        }
        for adapter_id, module_name, kind in candidates
        if (package_root / module_name).is_file()
    ]


def _probe_profiler(cann: dict[str, Any]) -> dict[str, Any]:
    path = shutil.which("msprof") or ""
    if not path:
        for home in cann.get("homes", []):
            candidate = Path(str(home)) / "tools" / "profiler" / "bin" / "msprof"
            if candidate.is_file():
                path = str(candidate)
                break
    return {
        "available": bool(path),
        "path": path,
        "capture_requires_device": True,
        "export_resource": "export",
    }


def _runtime_scopes(
    config: AppConfig,
    engine_path: Path | None,
) -> dict[str, str]:
    base = engine_path or (
        Path(config.endpoint.remote_root)
        / ".ascendop"
        / config.endpoint.execution_environment_id
    )
    configured_cache_root = os.environ.get("ASCENDOP_ENGINE_CACHE_ROOT", "").strip()
    cache_root = (
        Path(configured_cache_root).expanduser()
        if configured_cache_root
        else base / "cache"
    )
    return {
        "execution_environment_id": config.endpoint.execution_environment_id,
        "writable_root": str(base),
        "jobs": str(base / "jobs"),
        "cache": str(cache_root),
        "returns": str(base / "return_ready"),
    }


def _stable_digest(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def collect_relay_node_reports(config: AppConfig) -> int:
    """Mirror client lifecycle reports into the shared node-report branch."""
    root = Path(config.relay.server_return_dir).expanduser() / "_nodes"
    if not root.is_dir():
        return 0
    published = 0
    for source in sorted(root.glob("*/report.json")):
        report = _read_json(source)
        if report.get("schema") != NODE_REPORT_SCHEMA:
            continue
        try:
            node_id = normalize_node_id(str(report.get("node_id") or ""))
        except ValueError:
            continue
        if source.parent.name != node_id:
            continue
        destination = (
            config.repo_dir
            / config.io.output_dir
            / "_control"
            / "nodes"
            / node_id
            / "report.json"
        )
        write_json(destination, report, config.io.max_file_bytes)
        git = GitClient(
            replace(
                config.repo,
                result_branch=config.node_lifecycle.report_branch,
            ),
            config.repo_dir,
        )
        relative = destination.relative_to(config.repo_dir).as_posix()
        if git.commit_and_push(
            [relative],
            f"git_partner: mirror node {node_id} {report.get('state', 'unknown')}",
            config.io.max_file_bytes,
        ):
            published += 1
    return published


def _detect_cann() -> dict[str, Any]:
    candidates: list[Path] = []
    for name in (
        "ASCEND_HOME_PATH",
        "ASCEND_TOOLKIT_HOME",
        "ASCEND_TOOLKIT_HOME_PATH",
    ):
        value = os.environ.get(name, "").strip()
        if value:
            candidates.append(Path(value))
    candidates.extend(
        (
            Path("/usr/local/Ascend/ascend-toolkit/latest"),
            Path("/usr/local/Ascend/latest"),
            Path.home() / "Ascend/ascend-toolkit/latest",
        )
    )
    candidates.extend(sorted(Path("/usr/local/Ascend").glob("cann-*")))
    candidates.extend(sorted(Path("/usr/local/Ascend/ascend-toolkit").glob("*")))
    candidates.extend(sorted((Path.home() / "Ascend/ascend-toolkit").glob("*")))
    homes: list[str] = []
    versions: list[str] = []
    version_info: list[dict[str, str]] = []
    set_env: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            continue
        key = str(resolved)
        if key in seen or not resolved.exists():
            continue
        seen.add(key)
        homes.append(key)
        for env_path in (resolved / "set_env.sh", resolved.parent / "set_env.sh"):
            if env_path.is_file():
                set_env.append(str(env_path))
        for version_path in (
            resolved / "version.info",
            resolved / "compiler/version.info",
            resolved.parent / "version.info",
        ):
            if not version_path.is_file():
                continue
            try:
                text = version_path.read_text(
                    encoding="utf-8-sig", errors="replace"
                ).strip()
            except OSError:
                continue
            if text:
                versions.extend(_parse_cann_versions(text))
                version_info.append(
                    {
                        "path": str(version_path),
                        "sha256": hashlib.sha256(
                            text.encode("utf-8")
                        ).hexdigest(),
                    }
                )
    return {
        "homes": sorted(set(homes)),
        "versions": sorted(set(versions)),
        "version_info": sorted(version_info, key=lambda item: item["path"]),
        "set_env": sorted(set(set_env)),
    }


def _parse_cann_versions(text: str) -> list[str]:
    values = {
        match.group(1).strip()
        for match in re.finditer(
            r"(?im)^\s*(?:version|version_number|version_name)\s*[:=]\s*"
            r"([A-Za-z0-9][A-Za-z0-9._+-]*)\s*$",
            text,
        )
    }
    return sorted(value for value in values if value)


def _engine_path(config: AppConfig) -> Path | None:
    value = config.endpoint.engine_root.strip()
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    remote_root = config.endpoint.remote_root.strip()
    if remote_root:
        return Path(remote_root) / path
    return config.repo_dir / path


def _capability_generation(value: dict[str, Any]) -> str:
    stable = dict(value)
    stable.pop("observed_at", None)
    payload = json.dumps(stable, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _portable_path(path: Path, root: Path) -> str:
    try:
        value = os.path.relpath(path.resolve(), root.resolve())
    except ValueError:
        return path.name
    return "." if value == "." else value.replace("\\", "/")


def _boot_id() -> str:
    path = Path("/proc/sys/kernel/random/boot_id")
    try:
        value = path.read_text(encoding="ascii").strip()
    except OSError:
        value = ""
    return value or f"{socket.gethostname()}-{uuid.uuid4().hex}"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _utc_now() -> str:
    return _format_time(datetime.now(timezone.utc))


def _format_time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
