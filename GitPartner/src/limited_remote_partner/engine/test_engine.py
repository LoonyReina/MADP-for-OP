from __future__ import annotations

import hashlib
import json
import math
import os
import signal
import shutil
import subprocess
import sys
import tarfile
import time
import ctypes
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from limited_remote_partner.resources.payload_archive import PayloadArchiveError, materialize_payload_tree
from limited_remote_partner.resources.shared_resource_lease import shared_lease_root_for_engine, shared_lease_snapshot
from limited_remote_partner.core.atomic_file import (
    atomic_write_json as atomic_write_json_file,
    read_json_object,
    unlink_file,
)
from limited_remote_partner.core.process_utils import process_start_token
from limited_remote_partner.engine.runtime_manifest import runtime_generation


PROTOCOL_VERSION = "engine-v3"
EXECUTION_DEADLINE_POLICIES = {
    "first-stage-start-hard-cap-v1",
    "activation-start-hard-cap-v2",
    "device-lease-wall-v3",
}
DEFAULT_EXECUTION_DEADLINE_POLICY = "first-stage-start-hard-cap-v1"
STAGE_RESULT_VISIBILITY_GRACE_SECONDS = 2.0
WORKER_EXIT_HANDLE_GRACE_SECONDS = 0.1
MANAGER_STAGE_TIMEOUT_GRACE_SECONDS = 30.0
DEFAULT_RESIDENT_HEARTBEAT_STALE_SECONDS = 15.0
READY_ARCHIVE_CHUNK_SIZE_BYTES = 512 * 1024
TRANSPORT_RETURNED_JOB_LIMIT = 16
TERMINAL_STATES = {"completed", "failed"}
STAGE_RESOURCES = {"host", "device", "export"}
DEFAULT_CAPACITY = {
    "max_inflight": 4,
    "standby_slots": 0,
    "host_slots": 4,
    "host_cpu_weight_capacity": 4,
    "host_memory_mb_capacity": 16384,
    "host_io_weight_capacity": 4,
    "cold_build_slots": 1,
    "cache_hit_slots": 4,
    "device_slots": 1,
    "device_inventory": [
        {
            "device_id": "0",
            "enabled": True,
            "draining": False,
            "lease_resource": "npu:0",
            "measurement_resource": "performance-measurement:0",
        }
    ],
    "export_slots": 1,
    "return_backlog_soft_limit_bytes": 512 * 1024 * 1024,
    "return_backlog_hard_limit_bytes": 1024 * 1024 * 1024,
    "return_backlog_soft_limit_jobs": 0,
    "return_backlog_hard_limit_jobs": 0,
    "draining": False,
}
RETURN_ACK_COMPACT_DIRS = (
    "payload",
    "work",
    "vendor",
    "result",
    "result_bundle",
    "logs",
    "stage_results",
)


class EngineError(RuntimeError):
    pass


class EngineCapacityError(EngineError):
    pass


@dataclass(frozen=True)
class CorrelationKey:
    request_id: str
    engine_job_id: str
    attempt_id: str


class EngineProcessLock:
    def __init__(
        self,
        root: Path,
        *,
        name: str = "engine.lock",
        wait_timeout_seconds: float = 0.0,
    ) -> None:
        self.path = root.resolve() / name
        self.wait_timeout_seconds = max(0.0, float(wait_timeout_seconds))
        self.held = False

    def __enter__(self) -> EngineProcessLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": os.getpid(),
            "start_token": process_start_token(os.getpid()),
            "acquired_at": utc_now(),
        }
        deadline = time.monotonic() + self.wait_timeout_seconds
        while True:
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                try:
                    current = read_json(self.path)
                except (OSError, ValueError, json.JSONDecodeError):
                    try:
                        lock_age = time.time() - self.path.stat().st_mtime
                    except OSError:
                        continue
                    if time.monotonic() < deadline or lock_age < 1.0:
                        time.sleep(0.05)
                        continue
                    self.path.unlink(missing_ok=True)
                    continue
                pid = int(current.get("pid", 0) or 0)
                start_token = str(current.get("start_token") or "")
                if process_identity_alive(pid, start_token):
                    if time.monotonic() < deadline:
                        time.sleep(0.05)
                        continue
                    raise EngineError(
                        f"test-engine lock is busy: path={self.path} pid={pid}"
                    )
                self.path.unlink(missing_ok=True)
                continue
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=True, indent=2) + "\n")
            self.held = True
            return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.held:
            try:
                release_lock_file(self.path, wait_seconds=1.0)
            finally:
                self.held = False


def release_lock_file(path: Path, *, wait_seconds: float) -> None:
    deadline = time.monotonic() + max(0.0, float(wait_seconds))
    while True:
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)


class TestEngine:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.jobs_dir = self.root / "jobs"
        self.accepted_dir = self.root / "accepted"
        self.return_ready_dir = self.root / "return_ready"
        self.required_returned_dir = self.root / "required_returned"
        self.returned_dir = self.root / "returned"
        self.cancelled_standby_dir = self.root / "cancelled_standby"
        self.config_path = self.root / "engine_config.json"
        self.status_path = self.root / "engine_status.json"
        self.events_path = self.root / "events.jsonl"
        self.stop_path = self.root / "engine_stop.json"
        self.resident_path = self.root / "resident.json"

    def initialize(self, capacity: dict[str, Any] | None = None) -> dict[str, Any]:
        created = not self.config_path.exists()
        for path in (
            self.root,
            self.jobs_dir,
            self.accepted_dir,
            self.return_ready_dir,
            self.required_returned_dir,
            self.returned_dir,
            self.cancelled_standby_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        if not self.config_path.exists():
            payload = dict(DEFAULT_CAPACITY)
            discovered_device_inventory = discover_runtime_device_inventory()
            if (
                discovered_device_inventory
                and (
                    not capacity
                    or (
                        "device_inventory" not in capacity
                        and "device_slots" not in capacity
                    )
                )
            ):
                payload["device_inventory"] = discovered_device_inventory
            if capacity:
                payload.update(capacity)
                if (
                    "device_slots" in capacity
                    and "device_inventory" not in capacity
                ):
                    payload["device_inventory"] = [
                        {
                            "device_id": str(index),
                            "enabled": True,
                            "draining": False,
                        }
                        for index in range(max(0, int(capacity["device_slots"])))
                    ]
            self._write_capacity(payload)
        discovery = self._reconcile_discovered_device_inventory()
        status = self.snapshot()
        if created:
            self._event("engine_initialized", capacity=status["capacity"])
        if discovery["changed"]:
            self._event(
                "device_inventory_reconciled",
                added_device_ids=discovery["added"],
                disabled_device_ids=discovery["disabled"],
                draining_device_ids=discovery["draining"],
                capacity=status["capacity"],
            )
        return status

    def capacity(self) -> dict[str, Any]:
        if not self.config_path.exists():
            self.initialize()
        raw = read_json(self.config_path)
        return validate_capacity(raw)

    def set_capacity(
        self,
        *,
        max_inflight: int | None = None,
        standby_slots: int | None = None,
        active_job_slots: int | None = None,
        host_slots: int | None = None,
        host_cpu_weight_capacity: int | None = None,
        host_memory_mb_capacity: int | None = None,
        host_io_weight_capacity: int | None = None,
        cold_build_slots: int | None = None,
        cache_hit_slots: int | None = None,
        device_slots: int | None = None,
        device_inventory: list[dict[str, Any]] | None = None,
        export_slots: int | None = None,
        return_backlog_soft_limit_bytes: int | None = None,
        return_backlog_hard_limit_bytes: int | None = None,
        return_backlog_soft_limit_jobs: int | None = None,
        return_backlog_hard_limit_jobs: int | None = None,
        draining: bool | None = None,
    ) -> dict[str, Any]:
        current = self.capacity()
        payload = dict(current)
        if device_slots is not None and device_inventory is None:
            current_by_id = {
                str(item["device_id"]): dict(item)
                for item in current.get("device_inventory", [])
                if isinstance(item, dict) and item.get("device_id") is not None
            }
            selected_ids = {str(index) for index in range(max(0, int(device_slots)))}
            payload["device_inventory"] = [
                current_by_id.get(
                    str(index),
                    {
                        "device_id": str(index),
                        "enabled": True,
                        "draining": False,
                    },
                )
                for index in range(max(0, int(device_slots)))
            ]
            payload["device_inventory"].extend(
                {
                    **item,
                    "enabled": False,
                    "draining": False,
                }
                for device_id, item in current_by_id.items()
                if device_id not in selected_ids
            )
        elif device_inventory is not None:
            requested_ids = {
                str(item.get("device_id") or "")
                for item in device_inventory
                if isinstance(item, dict)
            }
            retained_disabled = [
                {
                    **item,
                    "enabled": False,
                    "draining": False,
                }
                for item in current.get("device_inventory", [])
                if str(item.get("device_id") or "") not in requested_ids
            ]
            device_inventory = [*device_inventory, *retained_disabled]
        updates = {
            "max_inflight": max_inflight,
            "standby_slots": standby_slots,
            "active_job_slots": active_job_slots,
            "host_slots": host_slots,
            "host_cpu_weight_capacity": host_cpu_weight_capacity,
            "host_memory_mb_capacity": host_memory_mb_capacity,
            "host_io_weight_capacity": host_io_weight_capacity,
            "cold_build_slots": cold_build_slots,
            "cache_hit_slots": cache_hit_slots,
            "device_slots": device_slots,
            "device_inventory": device_inventory,
            "export_slots": export_slots,
            "return_backlog_soft_limit_bytes": return_backlog_soft_limit_bytes,
            "return_backlog_hard_limit_bytes": return_backlog_hard_limit_bytes,
            "return_backlog_soft_limit_jobs": return_backlog_soft_limit_jobs,
            "return_backlog_hard_limit_jobs": return_backlog_hard_limit_jobs,
            "draining": draining,
        }
        for key, value in updates.items():
            if value is not None:
                payload[key] = value
        normalized = validate_capacity(payload)
        self._assert_device_capacity_transition_safe(current, normalized)
        self._write_capacity(normalized)
        self._event("capacity_updated", capacity=normalized)
        return self.snapshot()

    def _reconcile_discovered_device_inventory(self) -> dict[str, Any]:
        discovered_inventory = discover_runtime_device_inventory()
        discovered = [
            str(item["device_id"])
            for item in discovered_inventory
        ]
        if not discovered or not self.config_path.exists():
            return {
                "changed": False,
                "added": [],
                "disabled": [],
                "draining": [],
            }
        current = self.capacity()
        known = {
            str(item["device_id"]): dict(item)
            for item in current.get("device_inventory", [])
            if isinstance(item, dict) and item.get("device_id") is not None
        }
        discovered_set = set(discovered)
        active_device_ids = {
            str(state.get("device_id") or "")
            for state in self._states()
            if state.get("state") not in TERMINAL_STATES
            and str(state.get("device_id") or "")
        }
        added = [device_id for device_id in discovered if device_id not in known]
        disabled: list[str] = []
        draining: list[str] = []
        inventory: list[dict[str, Any]] = []
        for device_id, device in known.items():
            updated = dict(device)
            if bool(updated.get("enabled", True)) and device_id not in discovered_set:
                if device_id in active_device_ids:
                    if not bool(updated.get("draining", False)):
                        updated["draining"] = True
                        draining.append(device_id)
                else:
                    updated["enabled"] = False
                    updated["draining"] = False
                    disabled.append(device_id)
            inventory.append(updated)
        discovered_by_id = {
            str(item["device_id"]): item
            for item in discovered_inventory
        }
        inventory.extend(discovered_by_id[device_id] for device_id in added)
        if not added and not disabled and not draining:
            return {
                "changed": False,
                "added": [],
                "disabled": [],
                "draining": [],
            }
        current["device_inventory"] = inventory
        self._write_capacity(current)
        return {
            "changed": True,
            "added": added,
            "disabled": disabled,
            "draining": draining,
        }

    def _assert_device_capacity_transition_safe(
        self,
        current: dict[str, Any],
        proposed: dict[str, Any],
    ) -> None:
        proposed_by_id = {
            str(item["device_id"]): item
            for item in proposed.get("device_inventory", [])
        }
        blocked: dict[str, list[str]] = {}
        for state in self._states():
            if state.get("state") in TERMINAL_STATES:
                continue
            device_id = str(state.get("device_id") or "")
            if not device_id:
                continue
            proposed_device = proposed_by_id.get(device_id)
            if proposed_device is None or not bool(proposed_device.get("enabled", True)):
                blocked.setdefault(device_id, []).append(
                    str(state.get("engine_job_id") or "unknown")
                )
        if blocked:
            details = ", ".join(
                f"{device_id}=[{','.join(sorted(job_ids))}]"
                for device_id, job_ids in sorted(blocked.items())
            )
            raise EngineCapacityError(
                "cannot disable or remove devices with nonterminal assigned jobs; "
                f"mark them draining and wait for completion first: {details}"
            )

    def submit(
        self,
        spec: dict[str, Any],
        *,
        payload_root: Path | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        normalized = validate_spec(spec)
        job_id = str(normalized["engine_job_id"])
        if (self.cancelled_standby_dir / f"{job_id}.json").is_file():
            raise EngineError(f"engine job was cancelled and cannot be reused: {job_id}")
        job_dir = self._job_dir(job_id)
        spec_path = job_dir / "spec.json"
        digest = document_digest(normalized)

        if spec_path.exists():
            existing = read_json(spec_path)
            if document_digest(existing) != digest:
                raise EngineError(f"engine job id already exists with different spec: {job_id}")
            accepted_path = job_dir / "accepted.json"
            if accepted_path.is_file():
                return read_json(accepted_path)
            state_path = job_dir / "state.json"
            if state_path.is_file() and read_json(state_path).get("state") == "standby":
                if int(self.snapshot()["admission_credit"]) <= 0:
                    raise EngineCapacityError("engine has no fresh admission credit")
                return self._accept_standby(job_dir)
            raise EngineError(f"engine job is incomplete: {job_id}")

        snapshot = self.snapshot()
        if int(snapshot["admission_credit"]) <= 0:
            raise EngineCapacityError("engine has no fresh admission credit")

        job_dir.mkdir(parents=True, exist_ok=False)
        try:
            (job_dir / "stage_results").mkdir()
            (job_dir / "logs").mkdir()
            (job_dir / "result_bundle").mkdir()
            payload_provided = payload_root is not None
            if payload_provided:
                copy_payload_tree(payload_root.resolve(), job_dir / "payload")
            else:
                (job_dir / "payload").mkdir()
            payload_digest = tree_digest(job_dir / "payload")
            if payload_provided and payload_digest != str(normalized["bundle_hash"]):
                manifest = canonical_file_manifest(job_dir / "payload")
                raise EngineError(
                    "engine payload digest does not match immutable bundle_hash: "
                    f"expected={normalized['bundle_hash']} actual={payload_digest} "
                    f"actual_manifest={json.dumps(manifest, sort_keys=True, separators=(',', ':'))}"
                )
            atomic_write_json(spec_path, normalized)
        except Exception:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise
        accepted_at = utc_now()
        code_generation = engine_code_generation()
        receipt = {
            **correlation_fields(normalized),
            "protocol_version": PROTOCOL_VERSION,
            "bundle_hash": normalized["bundle_hash"],
            "execution_profile": str(normalized.get("execution_profile") or ""),
            "scheduler_policy": dict(normalized["scheduler_policy"]),
            "engine_code_generation": code_generation,
            "payload_digest": payload_digest,
            "spec_hash": digest,
            "accepted_at": accepted_at,
            "state": "accepted",
        }
        state = {
            **correlation_fields(normalized),
            "protocol_version": PROTOCOL_VERSION,
            "execution_profile": str(normalized.get("execution_profile") or ""),
            "scheduler_policy": dict(normalized["scheduler_policy"]),
            "engine_code_generation": code_generation,
            "state": "accepted",
            "stage_index": 0,
            "stage_state": "pending",
            "completed_stage_indices": [],
            "running_stages": {},
            "stage_attempts": {},
            "accepted_at": accepted_at,
            "updated_at": accepted_at,
            "history": [],
        }
        atomic_write_json(job_dir / "accepted.json", receipt)
        atomic_write_json(job_dir / "state.json", state)
        atomic_write_json(self.accepted_dir / f"{job_id}.json", receipt)
        self._event("job_accepted", **receipt)
        self._write_snapshot()
        return receipt

    def stage_standby(
        self,
        spec: dict[str, Any],
        *,
        payload_root: Path | None = None,
    ) -> dict[str, Any]:
        """Persist immutable work without consuming accepted queue credit."""
        self.initialize()
        normalized = validate_spec(spec)
        job_id = str(normalized["engine_job_id"])
        if (self.cancelled_standby_dir / f"{job_id}.json").is_file():
            raise EngineError(f"engine job was cancelled and cannot be reused: {job_id}")
        job_dir = self._job_dir(job_id)
        spec_path = job_dir / "spec.json"
        digest = document_digest(normalized)

        if spec_path.exists():
            existing = read_json(spec_path)
            if document_digest(existing) != digest:
                raise EngineError(f"engine job id already exists with different spec: {job_id}")
            accepted_path = job_dir / "accepted.json"
            if accepted_path.is_file():
                accepted = read_json(accepted_path)
                standby_path = job_dir / "standby.json"
                if not standby_path.is_file():
                    atomic_write_json(standby_path, accepted)
                return accepted
            standby_path = job_dir / "standby.json"
            if standby_path.is_file():
                return read_json(standby_path)
            raise EngineError(f"engine standby job is incomplete: {job_id}")

        snapshot = self.snapshot()
        if int(snapshot["standby_credit"]) <= 0:
            raise EngineCapacityError("engine has no fresh standby credit")

        job_dir.mkdir(parents=True, exist_ok=False)
        try:
            (job_dir / "stage_results").mkdir()
            (job_dir / "logs").mkdir()
            (job_dir / "result_bundle").mkdir()
            payload_provided = payload_root is not None
            if payload_provided:
                copy_payload_tree(payload_root.resolve(), job_dir / "payload")
            else:
                (job_dir / "payload").mkdir()
            payload_digest = tree_digest(job_dir / "payload")
            if payload_provided and payload_digest != str(normalized["bundle_hash"]):
                manifest = canonical_file_manifest(job_dir / "payload")
                raise EngineError(
                    "engine payload digest does not match immutable bundle_hash: "
                    f"expected={normalized['bundle_hash']} actual={payload_digest} "
                    f"actual_manifest={json.dumps(manifest, sort_keys=True, separators=(',', ':'))}"
                )
            atomic_write_json(spec_path, normalized)
        except Exception:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise

        staged_at = utc_now()
        code_generation = engine_code_generation()
        receipt = {
            **correlation_fields(normalized),
            "protocol_version": PROTOCOL_VERSION,
            "bundle_hash": normalized["bundle_hash"],
            "execution_profile": str(normalized.get("execution_profile") or ""),
            "scheduler_policy": dict(normalized["scheduler_policy"]),
            "engine_code_generation": code_generation,
            "payload_digest": payload_digest,
            "spec_hash": digest,
            "staged_at": staged_at,
            "state": "standby",
        }
        state = {
            **correlation_fields(normalized),
            "protocol_version": PROTOCOL_VERSION,
            "execution_profile": str(normalized.get("execution_profile") or ""),
            "scheduler_policy": dict(normalized["scheduler_policy"]),
            "engine_code_generation": code_generation,
            "state": "standby",
            "stage_index": 0,
            "stage_state": "standby",
            "completed_stage_indices": [],
            "running_stages": {},
            "stage_attempts": {},
            "staged_at": staged_at,
            "updated_at": staged_at,
            "history": [],
        }
        atomic_write_json(job_dir / "standby.json", receipt)
        atomic_write_json(job_dir / "state.json", state)
        self._event("job_staged_standby", **receipt)
        self._write_snapshot()
        return receipt

    def cancel_standby(self, engine_job_id: str, *, reason: str) -> dict[str, Any]:
        job_id = normalize_token(engine_job_id, "engine_job_id")
        cancelled_path = self.cancelled_standby_dir / f"{job_id}.json"
        if cancelled_path.is_file():
            return read_json(cancelled_path)
        job_dir = self._job_dir(job_id)
        state_path = job_dir / "state.json"
        if not state_path.is_file():
            raise EngineError(f"standby job does not exist: {job_id}")
        state = read_json(state_path)
        if state.get("state") != "standby":
            raise EngineError(f"standby job is already accepted: {job_id}")
        receipt = {
            **correlation_fields(state),
            "protocol_version": PROTOCOL_VERSION,
            "state": "standby-cancelled",
            "staged_at": str(state.get("staged_at") or ""),
            "cancelled_at": utc_now(),
            "reason": str(reason).strip() or "standby cancelled by controller",
        }
        atomic_write_json(cancelled_path, receipt)
        shutil.rmtree(job_dir)
        self._event("job_standby_cancelled", **receipt)
        self._write_snapshot()
        return receipt

    def tick(self) -> dict[str, Any]:
        self.initialize()
        self._reconcile_running()
        self._finish_empty_jobs()
        self._promote_standby()
        self._schedule_ready_stages()
        return self._write_snapshot()

    def run(self, *, interval_seconds: float = 0.5, max_ticks: int = 0) -> None:
        ticks = 0
        while (max_ticks <= 0 or ticks < max_ticks) and not self.stop_path.exists():
            self.tick()
            ticks += 1
            time.sleep(max(0.05, interval_seconds))

    def ensure_resident(
        self,
        *,
        interval_seconds: float = 0.25,
        heartbeat_stale_seconds: float = DEFAULT_RESIDENT_HEARTBEAT_STALE_SECONDS,
    ) -> dict[str, Any]:
        self.initialize()
        with EngineProcessLock(
            self.root,
            name="engine.start.lock",
            wait_timeout_seconds=15.0,
        ):
            return self._ensure_resident_locked(
                interval_seconds=interval_seconds,
                heartbeat_stale_seconds=heartbeat_stale_seconds,
            )

    def _ensure_resident_locked(
        self,
        *,
        interval_seconds: float,
        heartbeat_stale_seconds: float,
    ) -> dict[str, Any]:
        heartbeat_stale_seconds = max(2.0, float(heartbeat_stale_seconds))
        current = self.resident_status()
        if current["resident_ok"]:
            resident = read_json(self.resident_path)
            if float(
                resident.get(
                    "heartbeat_stale_seconds",
                    DEFAULT_RESIDENT_HEARTBEAT_STALE_SECONDS,
                )
            ) != heartbeat_stale_seconds:
                resident["heartbeat_stale_seconds"] = heartbeat_stale_seconds
                atomic_write_json(self.resident_path, resident)
                return self.resident_status()
            return current
        if current["alive"]:
            stopped = self.request_stop(wait_seconds=5)
            if stopped["alive"]:
                raise EngineError(
                    "stale test-engine resident did not stop: "
                    f"pid={stopped.get('pid', 0)}"
                )
        self.stop_path.unlink(missing_ok=True)
        logs_dir = self.root / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.cli.test_engine_cli",
            "--root",
            str(self.root),
            "run",
            "--interval-seconds",
            str(max(0.05, interval_seconds)),
        ]
        worker_env = os.environ.copy()
        package_root = str(Path(__file__).resolve().parents[1])
        existing_pythonpath = worker_env.get("PYTHONPATH", "")
        worker_env["PYTHONPATH"] = os.pathsep.join(
            item for item in (package_root, existing_pythonpath) if item
        )
        with (logs_dir / "resident.out.log").open("a", encoding="utf-8") as stdout, (
            logs_dir / "resident.err.log"
        ).open("a", encoding="utf-8") as stderr:
            process = subprocess.Popen(
                command,
                cwd=str(self.root),
                env=worker_env,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                creationflags=hidden_process_creation_flags(),
                startupinfo=hidden_process_startup_info(),
                start_new_session=os.name != "nt",
            )
        resident_start_token = ""
        token_deadline = time.monotonic() + 1.0
        while (
            not resident_start_token
            and process.poll() is None
            and time.monotonic() < token_deadline
        ):
            resident_start_token = process_start_token(process.pid)
            if not resident_start_token:
                time.sleep(0.01)
        atomic_write_json(
            self.resident_path,
            {
                "pid": process.pid,
                "start_token": resident_start_token,
                "started_at": utc_now(),
                "command": command,
                "interval_seconds": max(0.05, interval_seconds),
                "heartbeat_stale_seconds": heartbeat_stale_seconds,
                "code_generation": engine_code_generation(),
            },
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = self.resident_status()
            if status["resident_ok"]:
                self._event("resident_started", pid=process.pid)
                return status
            if process.poll() is not None:
                break
            time.sleep(0.05)
        raise EngineError(f"test-engine resident failed to start: pid={process.pid}")

    def request_stop(self, *, wait_seconds: float = 5.0) -> dict[str, Any]:
        self.initialize()
        atomic_write_json(
            self.stop_path,
            {"requested_at": utc_now(), "reason": "test-engine CLI stop"},
        )
        deadline = time.monotonic() + max(0.0, wait_seconds)
        while time.monotonic() < deadline:
            status = self.resident_status()
            if not status["alive"]:
                # Windows may publish the process exit code a moment before its
                # inherited log handles are released. Keep stop bounded, but do
                # not report completion until that teardown edge has settled.
                if os.name == "nt":
                    time.sleep(0.1)
                self._event("resident_stopped", pid=status.get("pid", 0))
                return status
            time.sleep(0.05)
        status = self.resident_status()
        try:
            resident = (
                read_json(self.resident_path)
                if self.resident_path.exists()
                else {}
            )
        except (OSError, ValueError, json.JSONDecodeError):
            resident = {}
        lock_path = self.root / "engine.lock"
        try:
            lock = read_json(lock_path) if lock_path.exists() else {}
        except (OSError, ValueError, json.JSONDecodeError):
            lock = {}
        live_identities = {
            (
                int(row.get("pid", 0) or 0),
                str(row.get("start_token") or ""),
            )
            for row in (resident, lock)
            if process_identity_alive(
                int(row.get("pid", 0) or 0),
                str(row.get("start_token") or ""),
            )
        }
        if len(live_identities) == 1:
            pid, start_token = next(iter(live_identities))
            if start_token and terminate_process_identity(
                pid,
                start_token,
                wait_seconds=2.0,
            ):
                if os.name == "nt":
                    time.sleep(0.1)
                self._event(
                    "resident_stop_escalated",
                    pid=pid,
                    start_token=start_token,
                )
                stopped = self.resident_status()
                return {**stopped, "stop_escalated": True}
        return {
            **status,
            "stop_escalated": False,
            "stop_escalation_blocker": (
                "resident identity is missing or ambiguous"
            ),
        }

    def resident_status(self) -> dict[str, Any]:
        resident = read_json(self.resident_path) if self.resident_path.exists() else {}
        pid = int(resident.get("pid", 0) or 0)
        lock = {}
        lock_path = self.root / "engine.lock"
        if lock_path.exists():
            try:
                lock = read_json(lock_path)
            except (OSError, ValueError, json.JSONDecodeError):
                lock = {}
        lock_pid = int(lock.get("pid", 0) or 0)
        resident_start_token = str(resident.get("start_token") or "")
        lock_start_token = str(lock.get("start_token") or "")
        resident_alive = process_identity_alive(pid, resident_start_token)
        manager_alive = process_identity_alive(
            lock_pid,
            lock_start_token,
        )
        alive = resident_alive or manager_alive
        heartbeat_age = timestamp_age_seconds(
            str(read_json(self.status_path).get("observed_at", ""))
        ) if self.status_path.exists() else None
        interval = float(resident.get("interval_seconds", 0.25) or 0.25)
        heartbeat_stale_seconds = max(
            2.0,
            float(
                resident.get(
                    "heartbeat_stale_seconds",
                    DEFAULT_RESIDENT_HEARTBEAT_STALE_SECONDS,
                )
                or DEFAULT_RESIDENT_HEARTBEAT_STALE_SECONDS
            ),
            interval * 4,
        )
        current_generation = engine_code_generation()
        resident_generation = str(resident.get("code_generation") or "")
        generation_current = resident_generation == current_generation
        heartbeat_fresh = (
            heartbeat_age is not None
            and heartbeat_age <= heartbeat_stale_seconds
        )
        return {
            **resident,
            "pid": pid if resident_alive else lock_pid,
            "manager_lock_pid": lock_pid,
            "manager_lock_start_token": lock_start_token,
            "alive": alive,
            "heartbeat_age_seconds": heartbeat_age,
            "heartbeat_stale_seconds": heartbeat_stale_seconds,
            "heartbeat_fresh": heartbeat_fresh,
            "code_generation": resident_generation,
            "current_code_generation": current_generation,
            "code_generation_current": generation_current,
            "resident_ok": bool(
                lock_pid > 0
                and manager_alive
                and heartbeat_fresh
                and generation_current
                and not self.stop_path.exists()
            ),
            "stop_requested": self.stop_path.exists(),
        }

    def snapshot(self) -> dict[str, Any]:
        capacity = self.capacity() if self.config_path.exists() else validate_capacity(DEFAULT_CAPACITY)
        states = [self._observable_state(item) for item in self._states()]
        standby = [item for item in states if item.get("state") == "standby"]
        nonterminal = [
            item
            for item in states
            if item.get("state") not in TERMINAL_STATES
            and item.get("state") != "standby"
        ]
        active_nonterminal = [item for item in nonterminal if item.get("activated_at")]
        queued_nonterminal = [item for item in nonterminal if not item.get("activated_at")]
        running_by_resource = {resource: 0 for resource in STAGE_RESOURCES}
        running_locks: set[str] = set()
        host_runtime = {
            "cpu_weight": 0,
            "memory_mb": 0,
            "io_weight": 0,
            "cold_build": 0,
            "cache_hit": 0,
            "general": 0,
            "singleflight_keys": [],
        }
        running_by_device: dict[str, list[dict[str, Any]]] = {
            str(device["device_id"]): []
            for device in capacity.get("device_inventory", [])
        }
        assigned_by_device: dict[str, list[str]] = {
            str(device["device_id"]): []
            for device in capacity.get("device_inventory", [])
        }
        for state in nonterminal:
            assigned_device_id = str(state.get("device_id") or "")
            if assigned_device_id:
                assigned_by_device.setdefault(assigned_device_id, []).append(
                    str(state.get("engine_job_id") or "")
                )
            for running in self._running_stage_records(state).values():
                resource = str(running.get("stage_resource", ""))
                if resource in running_by_resource:
                    running_by_resource[resource] += 1
                if resource == "host":
                    host_runtime["cpu_weight"] += int(
                        running.get("host_cpu_weight", 1) or 1
                    )
                    host_runtime["memory_mb"] += int(
                        running.get("host_memory_mb", 1024) or 1024
                    )
                    host_runtime["io_weight"] += int(
                        running.get("host_io_weight", 1) or 1
                    )
                    host_class = str(
                        running.get("host_concurrency_class") or "general"
                    ).replace("-", "_")
                    if host_class in host_runtime:
                        host_runtime[host_class] += 1
                    singleflight_key = str(
                        running.get("singleflight_key") or ""
                    )
                    if singleflight_key:
                        host_runtime["singleflight_keys"].append(
                            singleflight_key
                        )
                running_locks.update(
                    str(item)
                    for item in running.get(
                        "runtime_stage_locks", running.get("stage_locks", [])
                    )
                )
                runtime_device_id = str(running.get("device_id") or "")
                if runtime_device_id:
                    running_by_device.setdefault(runtime_device_id, []).append(
                        {
                            "engine_job_id": str(state.get("engine_job_id") or ""),
                            "stage_index": int(running.get("stage_index", 0) or 0),
                            "stage_name": str(running.get("stage_name") or ""),
                            "stage_resource": resource,
                        }
                    )
        device_runtime = []
        for device in capacity.get("device_inventory", []):
            device_id = str(device["device_id"])
            running = running_by_device.get(device_id, [])
            device_runtime.append(
                {
                    **device,
                    "busy": bool(running),
                    "running": running,
                    "assigned_job_ids": sorted(assigned_by_device.get(device_id, [])),
                }
            )
        return_ready = self.return_ready()
        backlog = return_backlog_stats(return_ready)
        limits = effective_return_backlog_limits(capacity)
        pressure = return_backlog_pressure(backlog, limits)
        credit_before_backpressure = 0
        credit = 0
        standby_credit = 0
        if not bool(capacity["draining"]):
            credit_before_backpressure = max(
                0, int(capacity["max_inflight"]) - len(nonterminal)
            )
            allowed_nonterminal = int(
                math.ceil(int(capacity["max_inflight"]) * (1.0 - pressure["ratio"]))
            )
            credit = min(
                credit_before_backpressure,
                max(0, allowed_nonterminal - len(nonterminal)),
            )
            standby_credit = max(
                0, int(capacity["standby_slots"]) - len(standby)
            )
        terminal = [item for item in states if item.get("state") in TERMINAL_STATES]
        return {
            "protocol_version": PROTOCOL_VERSION,
            "return_export_protocol": "snapshot-ready-archive-v4",
            "engine_generation": engine_generation(self.root),
            "engine_code_generation": engine_code_generation(),
            "observed_at": utc_now(),
            "capacity": capacity,
            "accepted_nonterminal": len(nonterminal),
            "standby_count": len(standby),
            "standby_credit": standby_credit,
            "active_nonterminal": len(active_nonterminal),
            "queued_nonterminal": len(queued_nonterminal),
            "active_job_slots_free": max(
                0, int(capacity["active_job_slots"]) - len(active_nonterminal)
            ),
            "admission_credit": credit,
            "admission_credit_before_backpressure": credit_before_backpressure,
            "return_backlog_bytes": backlog["total_bytes"],
            "required_return_backlog_bytes": backlog["required_bytes"],
            "return_backlog_pressure_ratio": pressure["ratio"],
            "return_backpressure_active": pressure["ratio"] > 0.0,
            "return_backpressure_reason": pressure["reason"],
            "return_backlog_limits": limits,
            "running_by_resource": running_by_resource,
            "host_runtime": {
                **host_runtime,
                "singleflight_keys": sorted(
                    set(host_runtime["singleflight_keys"])
                ),
            },
            "running_locks": sorted(running_locks),
            "device_runtime": device_runtime,
            "shared_resource_leases": shared_lease_snapshot(
                shared_lease_root_for_engine(self.root)
            ),
            "host_slots_free": max(0, int(capacity["host_slots"]) - running_by_resource["host"]),
            "device_slots_free": max(
                0, int(capacity["device_slots"]) - running_by_resource["device"]
            ),
            "export_slots_free": max(
                0, int(capacity["export_slots"]) - running_by_resource["export"]
            ),
            "return_ready_count": backlog["jobs"],
            "returned_count": len(list(self.returned_dir.glob("*.json")))
            if self.returned_dir.exists()
            else 0,
            "terminal_count": len(terminal),
            "jobs": sorted(states, key=lambda item: str(item.get("accepted_at", ""))),
        }

    def transport_snapshot(self) -> dict[str, Any]:
        """Return control-plane state without replaying historical stage payloads."""
        snapshot = self.snapshot()
        raw_jobs = [
            item for item in snapshot.get("jobs", []) if isinstance(item, dict)
        ]
        recent_returned_ids = {
            str(item.get("engine_job_id") or "")
            for item in sorted(
                (item for item in raw_jobs if item.get("returned_at")),
                key=lambda item: (
                    str(item.get("returned_at") or ""),
                    str(item.get("updated_at") or ""),
                    str(item.get("accepted_at") or ""),
                    str(item.get("engine_job_id") or ""),
                ),
                reverse=True,
            )[:TRANSPORT_RETURNED_JOB_LIMIT]
        }
        compact_jobs: list[dict[str, Any]] = []
        retained = {
            "request_id",
            "engine_job_id",
            "attempt_id",
            "operator",
            "test_version",
            "state",
            "stage_state",
            "accepted_at",
            "staged_at",
            "promoted_at",
            "promotion_source",
            "activated_at",
            "terminal_at",
            "return_ready_at",
            "returned_at",
            "return_receipt_id",
            "required_returned_at",
            "required_return_receipt_id",
            "updated_at",
            "error",
            "failure_pending",
            "completed_stage_indices",
            "running_stage_details",
            "stage_index",
            "stage_name",
            "stage_resource",
            "stage_locks",
            "runtime_stage_locks",
            "device_id",
            "stage_started_at",
            "stage_attempt",
            "worker_pid",
        }
        for raw in raw_jobs:
            if raw.get("returned_at") and str(raw.get("engine_job_id") or "") not in recent_returned_ids:
                continue
            item = {key: raw[key] for key in retained if key in raw}
            item["history_count"] = len(raw.get("history", []))
            item["transport_compacted"] = True
            compact_jobs.append(item)
        snapshot["jobs"] = compact_jobs
        snapshot["snapshot_scope"] = "transport-compact-v2"
        snapshot["full_job_count"] = len(raw_jobs)
        snapshot["transport_job_count"] = len(compact_jobs)
        snapshot["omitted_returned_job_count"] = len(raw_jobs) - len(compact_jobs)
        snapshot["returned_receipt_limit"] = TRANSPORT_RETURNED_JOB_LIMIT
        snapshot["transport_history_omitted"] = True
        return snapshot

    def _observable_state(self, state: dict[str, Any]) -> dict[str, Any]:
        item = dict(state)
        running_stages = self._running_stage_records(item)
        if not running_stages:
            return item
        job_dir = self._job_dir(str(item.get("engine_job_id", "")))
        details: list[dict[str, Any]] = []
        for stage_index, running in sorted(running_stages.items()):
            stage_name = str(running.get("stage_name", ""))
            pid = int(running.get("worker_pid", 0) or 0)
            stage_result_path = job_dir / "stage_results" / f"{stage_index:03d}.json"
            logs: dict[str, dict[str, Any]] = {}
            latest_mtime = 0.0
            total_bytes = 0
            for stream, suffix in (("stdout", "out"), ("stderr", "err")):
                path = job_dir / "logs" / f"{stage_index:03d}_{stage_name}.{suffix}.log"
                if not path.exists():
                    logs[stream] = {"exists": False}
                    continue
                stat = path.stat()
                latest_mtime = max(latest_mtime, stat.st_mtime)
                total_bytes += stat.st_size
                logs[stream] = {
                    "exists": True,
                    "bytes": stat.st_size,
                    "updated_at": datetime.fromtimestamp(
                        stat.st_mtime, timezone.utc
                    ).isoformat(),
                    "path": str(path.relative_to(self.root)).replace("\\", "/"),
                }
            details.append(
                {
                    **running,
                    "stage_index": stage_index,
                    "worker_alive": bool(pid > 0 and process_alive(pid)),
                    "worker_process": proc_process_summary(pid),
                    "stage_age_seconds": timestamp_age_seconds(
                        str(running.get("stage_started_at", ""))
                    ),
                    "stage_result_exists": stage_result_path.exists(),
                    "stage_logs": logs,
                    "stage_output_bytes": total_bytes,
                    "stage_output_age_seconds": (
                        round(max(0.0, time.time() - latest_mtime), 3)
                        if latest_mtime
                        else None
                    ),
                }
            )
        item["running_stage_details"] = details
        if len(details) == 1:
            detail = details[0]
            for key in (
                "worker_alive",
                "worker_process",
                "stage_age_seconds",
                "stage_result_exists",
                "stage_logs",
                "stage_output_bytes",
                "stage_output_age_seconds",
            ):
                item[key] = detail[key]
            item["timeout_seconds"] = int(detail.get("timeout_seconds", 0) or 0)
        activity: list[dict[str, Any]] = []
        for relative_dir in ("work", "result_bundle"):
            activity_root = job_dir / relative_dir
            if not activity_root.is_dir():
                continue
            for path in activity_root.iterdir():
                if not path.is_file():
                    continue
                stat = path.stat()
                activity.append(
                    {
                        "bytes": stat.st_size,
                        "updated_at": datetime.fromtimestamp(
                            stat.st_mtime, timezone.utc
                        ).isoformat(),
                        "path": str(path.relative_to(job_dir)).replace("\\", "/"),
                        "mtime": stat.st_mtime,
                    }
                )
        activity.sort(key=lambda entry: float(entry["mtime"]), reverse=True)
        activity_latest_mtime = float(activity[0]["mtime"]) if activity else 0.0
        for entry in activity:
            entry.pop("mtime", None)
        item["stage_activity_files"] = activity[:8]
        item["stage_activity_age_seconds"] = (
            round(max(0.0, time.time() - activity_latest_mtime), 3)
            if activity
            else None
        )
        return item

    def _completed_stage_indices(self, state: dict[str, Any]) -> set[int]:
        raw = state.get("completed_stage_indices")
        if isinstance(raw, list):
            return {int(item) for item in raw}
        # Pre-V3 states created before DAG support used stage_index as a
        # durable sequential completion cursor.
        return set(range(max(0, int(state.get("stage_index", 0) or 0))))

    def _running_stage_records(self, state: dict[str, Any]) -> dict[int, dict[str, Any]]:
        raw = state.get("running_stages")
        records: dict[int, dict[str, Any]] = {}
        if isinstance(raw, dict):
            for key, value in raw.items():
                if isinstance(value, dict):
                    records[int(key)] = dict(value)
        if records or state.get("stage_state") != "running":
            return records
        # Adopt a pre-DAG running state without relaunching its worker.
        stage_index = int(state.get("stage_index", 0) or 0)
        records[stage_index] = {
            "stage_index": stage_index,
            "stage_name": str(state.get("stage_name", "")),
            "stage_resource": str(state.get("stage_resource", "")),
            "stage_locks": list(state.get("stage_locks", [])),
            "runtime_stage_locks": list(
                state.get("runtime_stage_locks", state.get("stage_locks", []))
            ),
            "device_id": str(state.get("device_id", "")),
            "stage_started_at": str(state.get("stage_started_at", "")),
            "stage_attempt": int(state.get("stage_attempt", 1) or 1),
            "timeout_seconds": int(state.get("timeout_seconds", 0) or 0),
            "worker_pid": int(state.get("worker_pid", 0) or 0),
            "worker_start_token": str(state.get("worker_start_token") or ""),
        }
        return records

    def _sync_state_projection(
        self,
        state: dict[str, Any],
        spec: dict[str, Any],
        running: dict[int, dict[str, Any]] | None = None,
    ) -> None:
        records = running if running is not None else self._running_stage_records(state)
        completed = self._completed_stage_indices(state)
        state["completed_stage_indices"] = sorted(completed)
        state["running_stages"] = {
            str(index): dict(record) for index, record in sorted(records.items())
        }
        attempts = state.get("stage_attempts")
        state["stage_attempts"] = dict(attempts) if isinstance(attempts, dict) else {}
        incomplete = [
            index
            for index in range(len(spec.get("stages", [])))
            if index not in completed
        ]
        state["stage_index"] = min(incomplete) if incomplete else len(spec.get("stages", []))
        if state.get("state") in TERMINAL_STATES:
            state["stage_state"] = "terminal"
        elif records:
            state["state"] = "running"
            state["stage_state"] = "running"
        else:
            state["state"] = "accepted"
            state["stage_state"] = "pending"
        for key in (
            "stage_name",
            "stage_resource",
            "stage_locks",
            "runtime_stage_locks",
            "stage_attempt",
            "stage_started_at",
            "timeout_seconds",
            "worker_pid",
            "worker_start_token",
        ):
            state.pop(key, None)
        if len(records) == 1:
            record = next(iter(records.values()))
            for key in (
                "stage_name",
                "stage_resource",
                "stage_locks",
                "runtime_stage_locks",
                "stage_attempt",
                "stage_started_at",
                "timeout_seconds",
                "worker_pid",
                "worker_start_token",
            ):
                state[key] = record.get(key)

    def _stage_dependencies(self, spec: dict[str, Any], stage_index: int) -> set[int]:
        stages = list(spec.get("stages", []))
        stage = stages[stage_index]
        if "depends_on" not in stage:
            return {stage_index - 1} if stage_index > 0 else set()
        name_to_index = {str(item["name"]): index for index, item in enumerate(stages)}
        return {name_to_index[str(name)] for name in stage.get("depends_on", [])}

    def return_ready(self) -> list[dict[str, Any]]:
        if not self.return_ready_dir.exists():
            return []
        manifests: list[dict[str, Any]] = []
        for path in self.return_ready_dir.glob("*.json"):
            manifest = read_json(path)
            job_id = str(manifest.get("engine_job_id") or "")
            required_receipt_path = self.required_returned_dir / f"{job_id}.json"
            if required_receipt_path.is_file():
                required_receipt = read_json(required_receipt_path)
                manifest.update(
                    {
                        "required_returned_at": str(
                            required_receipt.get("required_returned_at") or ""
                        ),
                        "required_return_receipt_id": str(
                            required_receipt.get("required_return_receipt_id") or ""
                        ),
                    }
                )
            manifests.append(manifest)
        return sorted(manifests, key=lambda item: str(item.get("terminal_at", "")))

    def export_ready(
        self,
        destination: Path,
        *,
        archive: Path | None = None,
        optional_export_limit: int = 4,
    ) -> dict[str, Any]:
        """Atomically export required evidence first, then bounded optional payloads.

        Four optional bundles match the default engine queue width and prevent a
        completed batch from requiring one full transport round-trip per job.
        """
        target = destination.resolve()
        transport_root = (self.root / "transport").resolve()
        if target == transport_root or transport_root not in target.parents:
            raise EngineError(f"ready export must stay under engine transport root: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = target.parent / f".{target.name}.tmp-{os.getpid()}-{time.time_ns()}"
        staging.mkdir(parents=True)
        exported: list[dict[str, Any]] = []
        try:
            prepared: list[
                tuple[dict[str, Any], Path, dict[str, Any], list[dict[str, Any]]]
            ] = []
            for manifest in self.return_ready():
                job_id = normalize_token(
                    str(manifest.get("engine_job_id") or ""), "engine_job_id"
                )
                job_dir = self._job_dir(job_id)
                self._repair_failed_return_bundle(job_dir)
                terminal_path = job_dir / "terminal.json"
                state_path = job_dir / "state.json"
                artifact_manifest_path = job_dir / "artifact_manifest.json"
                result_bundle = job_dir / "result_bundle"
                for required in (terminal_path, state_path, artifact_manifest_path):
                    if not required.is_file():
                        raise EngineError(
                            f"ready export is missing {required.name}: {job_id}"
                        )
                if not result_bundle.is_dir():
                    raise EngineError(f"ready export is missing result_bundle: {job_id}")
                terminal = read_json(terminal_path)
                artifact_manifest = read_json(artifact_manifest_path)
                artifacts = artifact_manifest.get("artifacts")
                if not isinstance(artifacts, list) or artifacts != terminal.get(
                    "artifacts"
                ):
                    raise EngineError(
                        f"ready export artifact manifest mismatch: {job_id}"
                    )
                prepared.append((manifest, job_dir, terminal, artifacts))

            required = [
                item for item in prepared if not item[0].get("required_returned_at")
            ]
            optional = [
                item for item in prepared if item[0].get("required_returned_at")
            ]
            selected = required or optional[: max(1, int(optional_export_limit))]
            for manifest, job_dir, terminal, artifacts in selected:
                job_id = normalize_token(
                    str(manifest.get("engine_job_id") or ""), "engine_job_id"
                )
                required_artifacts = [
                    dict(item)
                    for item in artifacts
                    if isinstance(item, dict) and bool(item.get("required"))
                ]
                optional_artifacts = [
                    dict(item)
                    for item in artifacts
                    if isinstance(item, dict) and not bool(item.get("required"))
                ]
                if manifest.get("required_returned_at"):
                    return_phase = "optional"
                    phase_artifacts = optional_artifacts
                    deferred_artifacts: list[dict[str, Any]] = []
                elif optional_artifacts:
                    return_phase = "required"
                    phase_artifacts = required_artifacts
                    deferred_artifacts = optional_artifacts
                else:
                    return_phase = "complete"
                    phase_artifacts = required_artifacts
                    deferred_artifacts = []
                destination_job = staging / job_id
                destination_job.mkdir()
                shutil.copy2(job_dir / "terminal.json", destination_job / "terminal.json")
                shutil.copy2(job_dir / "state.json", destination_job / "state.json")
                atomic_write_json(
                    destination_job / "artifact_manifest.json",
                    {
                        "protocol_version": "engine-return-artifacts-v2",
                        "return_phase": return_phase,
                        "artifacts": phase_artifacts,
                        "deferred_artifacts": deferred_artifacts,
                    },
                )
                copy_return_artifacts(
                    job_dir / "result_bundle",
                    destination_job / "result_bundle",
                    phase_artifacts,
                )
                exported.append(
                    {
                        "engine_job_id": job_id,
                        "terminal_at": str(manifest.get("terminal_at") or ""),
                        "state": str(manifest.get("state") or ""),
                        "relative_path": job_id,
                        "return_phase": return_phase,
                        "artifact_count": len(phase_artifacts),
                        "deferred_artifact_count": len(deferred_artifacts),
                    }
                )
            index = {
                "protocol_version": PROTOCOL_VERSION,
                "exported_at": utc_now(),
                "job_count": len(exported),
                "jobs": exported,
            }
            atomic_write_json(staging / "ready_index.json", index)
            if target.exists():
                shutil.rmtree(target)
            os.replace(staging, target)
            if archive is not None:
                archive_path = archive.resolve()
                if (
                    archive_path == transport_root
                    or transport_root not in archive_path.parents
                ):
                    raise EngineError(
                        f"ready archive must stay under engine transport root: {archive_path}"
                    )
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                archive_staging = archive_path.with_name(
                    f".{archive_path.name}.tmp-{os.getpid()}-{time.time_ns()}"
                )
                chunk_staging = archive_path.with_name(
                    f".{archive_path.name}.parts-{os.getpid()}-{time.time_ns()}"
                )
                try:
                    with tarfile.open(archive_staging, "w") as handle:
                        handle.add(target, arcname="ready_jobs", recursive=True)
                    archive_size = archive_staging.stat().st_size
                    archive_sha256 = file_digest(archive_staging)
                    if archive_path.exists():
                        if archive_path.is_dir():
                            shutil.rmtree(archive_path)
                        else:
                            archive_path.unlink()
                    if archive_size <= READY_ARCHIVE_CHUNK_SIZE_BYTES:
                        os.replace(archive_staging, archive_path)
                        archive_layout = "single-file"
                        archive_parts: list[dict[str, Any]] = []
                    else:
                        chunk_staging.mkdir(parents=True)
                        archive_parts = []
                        with archive_staging.open("rb") as source:
                            part_index = 0
                            while True:
                                payload = source.read(READY_ARCHIVE_CHUNK_SIZE_BYTES)
                                if not payload:
                                    break
                                part_name = f"part-{part_index:05d}"
                                part_path = chunk_staging / part_name
                                part_path.write_bytes(payload)
                                archive_parts.append(
                                    {
                                        "name": part_name,
                                        "size_bytes": len(payload),
                                        "sha256": hashlib.sha256(payload).hexdigest(),
                                    }
                                )
                                part_index += 1
                        os.replace(chunk_staging, archive_path)
                        archive_layout = "chunked-directory"
                        archive_staging.unlink()
                finally:
                    archive_staging.unlink(missing_ok=True)
                    shutil.rmtree(chunk_staging, ignore_errors=True)
                index["archive"] = str(archive_path.relative_to(self.root)).replace(
                    "\\", "/"
                )
                index["archive_layout"] = archive_layout
                index["archive_size_bytes"] = archive_size
                index["archive_sha256"] = archive_sha256
                index["archive_part_count"] = len(archive_parts)
                if archive_parts:
                    index["archive_parts"] = archive_parts
            return index
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def _repair_failed_return_bundle(self, job_dir: Path) -> None:
        terminal_path = job_dir / "terminal.json"
        if not terminal_path.is_file():
            return
        terminal = read_json(terminal_path)
        if terminal.get("state") != "failed":
            return
        spec = read_json(job_dir / "spec.json")
        state = read_json(job_dir / "state.json")
        ensure_failure_protocol_artifacts(
            job_dir,
            spec,
            state,
            error=str(terminal.get("error") or state.get("error") or "engine job failed"),
        )
        failure = classify_terminal_failure(job_dir, state)
        state.update(failure)
        diagnostic_artifacts = failure_diagnostic_artifacts()
        required_artifacts = failure_required_artifacts(job_dir, spec, state)
        optional_artifacts = list(spec.get("optional_artifacts", []))
        optional_artifacts.extend(
            item for item in diagnostic_artifacts if item not in optional_artifacts
        )
        artifacts = collect_result_artifacts(
            job_dir,
            required_artifacts,
            optional_artifacts,
        )
        terminal.update(
            {
                "return_ready_at": str(
                    terminal.get("return_ready_at")
                    or terminal.get("terminal_at")
                    or utc_now()
                ),
                "required_artifacts": required_artifacts,
                "optional_artifacts": optional_artifacts,
                "diagnostic_artifacts": diagnostic_artifacts,
                "artifacts": artifacts,
                **failure,
            }
        )
        atomic_write_json(terminal_path, terminal)
        atomic_write_json(job_dir / "artifact_manifest.json", {"artifacts": artifacts})
        atomic_write_json(
            self.return_ready_dir / f"{spec['engine_job_id']}.json",
            terminal,
        )

    def acknowledge_required(
        self, engine_job_id: str, receipt_id: str
    ) -> dict[str, Any]:
        job_id = normalize_token(engine_job_id, "engine_job_id")
        normalized_receipt = normalize_token(receipt_id, "receipt_id")
        receipt_path = self.required_returned_dir / f"{job_id}.json"
        if receipt_path.is_file():
            existing = read_json(receipt_path)
            if str(existing.get("required_return_receipt_id") or "") != normalized_receipt:
                raise EngineError(f"required return receipt conflict: {job_id}")
            return existing
        returned_path = self.returned_dir / f"{job_id}.json"
        if returned_path.is_file():
            returned = read_json(returned_path)
            return {
                **returned,
                "required_return_receipt_id": normalized_receipt,
                "required_returned_at": str(returned.get("returned_at") or utc_now()),
                "state": "returned",
            }
        job_dir = self._job_dir(job_id)
        state = read_json(job_dir / "state.json")
        if state.get("state") not in TERMINAL_STATES:
            raise EngineError(f"job is not terminal: {job_id}")
        manifest_path = self.return_ready_dir / f"{job_id}.json"
        if not manifest_path.is_file():
            raise EngineError(f"return-ready manifest missing: {job_id}")
        manifest = read_json(manifest_path)
        required_returned_at = utc_now()
        receipt = {
            **manifest,
            "state": "required-returned",
            "required_return_receipt_id": normalized_receipt,
            "required_returned_at": required_returned_at,
        }
        atomic_write_json(receipt_path, receipt)
        state.update(
            {
                "required_return_receipt_id": normalized_receipt,
                "required_returned_at": required_returned_at,
                "updated_at": required_returned_at,
            }
        )
        atomic_write_json(job_dir / "state.json", state)
        self._event("required_return_acknowledged", **correlation_fields(manifest))
        return receipt

    def acknowledge_return(self, engine_job_id: str, receipt_id: str) -> dict[str, Any]:
        job_dir = self._job_dir(engine_job_id)
        state = read_json(job_dir / "state.json")
        if state.get("state") not in TERMINAL_STATES:
            raise EngineError(f"job is not terminal: {engine_job_id}")
        manifest_path = self.return_ready_dir / f"{engine_job_id}.json"
        if not manifest_path.exists():
            returned = self.returned_dir / f"{engine_job_id}.json"
            if returned.exists():
                receipt = read_json(returned)
                if not isinstance(receipt.get("storage_compaction"), dict):
                    receipt["storage_compaction"] = compact_returned_job(job_dir)
                    atomic_write_json(returned, receipt)
                return receipt
            raise EngineError(f"return-ready manifest missing: {engine_job_id}")
        manifest = read_json(manifest_path)
        receipt = {
            **manifest,
            "return_receipt_id": normalize_token(receipt_id, "receipt_id"),
            "returned_at": utc_now(),
        }
        atomic_write_json(self.returned_dir / f"{engine_job_id}.json", receipt)
        unlink_file(manifest_path, missing_ok=False)
        state["returned_at"] = receipt["returned_at"]
        state["return_receipt_id"] = receipt["return_receipt_id"]
        state["updated_at"] = utc_now()
        atomic_write_json(job_dir / "state.json", state)
        compaction = compact_returned_job(job_dir)
        receipt["storage_compaction"] = compaction
        state["storage_compaction"] = compaction
        state["updated_at"] = utc_now()
        atomic_write_json(self.returned_dir / f"{engine_job_id}.json", receipt)
        atomic_write_json(job_dir / "state.json", state)
        self._event("job_return_acknowledged", **receipt)
        self._event(
            "job_storage_compacted",
            engine_job_id=engine_job_id,
            **compaction,
        )
        self._write_snapshot()
        return receipt

    def _write_capacity(self, payload: dict[str, Any]) -> None:
        normalized = validate_capacity(payload)
        self.root.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.config_path, normalized)

    def _states(self) -> Iterable[dict[str, Any]]:
        if not self.jobs_dir.exists():
            return []
        states: list[dict[str, Any]] = []
        for path in self.jobs_dir.glob("*/state.json"):
            try:
                states.append(read_json(path))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        return states

    def _reconcile_running(self) -> None:
        for state in self._states():
            running = self._running_stage_records(state)
            if not running:
                continue
            job_id = str(state["engine_job_id"])
            job_dir = self._job_dir(job_id)
            spec = read_json(job_dir / "spec.json")
            for stage_index, record in sorted(list(running.items())):
                result_path = job_dir / "stage_results" / f"{stage_index:03d}.json"
                pid = int(record.get("worker_pid", 0) or 0)
                if result_path.exists():
                    # A result is durable before worker exit. Retain every stage
                    # lock until its wrapper and inherited handles are gone.
                    if pid > 0 and process_alive(pid):
                        continue
                    if os.name == "nt":
                        try:
                            result_age = time.time() - result_path.stat().st_mtime
                        except OSError:
                            continue
                        if result_age < WORKER_EXIT_HANDLE_GRACE_SECONDS:
                            continue
                    if not self._apply_stage_result(
                        job_dir,
                        state,
                        spec,
                        stage_index,
                        record,
                        read_json(result_path),
                    ):
                        break
                    running = self._running_stage_records(state)
                    continue
                self._enforce_running_stage_timeout(
                    job_dir,
                    state,
                    stage_index,
                    record,
                    result_path,
                )
                if result_path.exists():
                    if pid > 0 and process_alive(pid):
                        continue
                    if not self._apply_stage_result(
                        job_dir,
                        state,
                        spec,
                        stage_index,
                        record,
                        read_json(result_path),
                    ):
                        break
                    running = self._running_stage_records(state)
                    continue
                if pid > 0 and process_alive(pid):
                    continue
                stage_age = timestamp_age_seconds(
                    str(record.get("stage_started_at", ""))
                )
                if (
                    stage_age is not None
                    and stage_age < STAGE_RESULT_VISIBILITY_GRACE_SECONDS
                ):
                    continue
                if not self._handle_disappeared_stage(
                    job_dir, state, spec, stage_index, record
                ):
                    break
                running = self._running_stage_records(state)

    def _enforce_running_stage_timeout(
        self,
        job_dir: Path,
        state: dict[str, Any],
        stage_index: int,
        record: dict[str, Any],
        result_path: Path,
    ) -> bool:
        timeout_seconds = int(record.get("timeout_seconds", 0) or 0)
        stage_age = timestamp_age_seconds(
            str(record.get("stage_started_at", ""))
        )
        if (
            timeout_seconds <= 0
            or stage_age is None
            or stage_age
            <= timeout_seconds + MANAGER_STAGE_TIMEOUT_GRACE_SECONDS
        ):
            return False
        pid = int(record.get("worker_pid", 0) or 0)
        start_token = str(record.get("worker_start_token") or "")
        if pid <= 0 or not start_token:
            self._event(
                "stage_timeout_recovery_blocked",
                **correlation_fields(state),
                stage_index=stage_index,
                stage_name=record.get("stage_name", ""),
                worker_pid=pid,
                reason="worker identity is incomplete",
            )
            return False
        if not terminate_process_tree_identity(
            pid,
            start_token,
            wait_seconds=5.0,
        ):
            self._event(
                "stage_timeout_recovery_blocked",
                **correlation_fields(state),
                stage_index=stage_index,
                stage_name=record.get("stage_name", ""),
                worker_pid=pid,
                reason="worker process tree did not terminate",
            )
            return False
        if not result_path.exists():
            atomic_write_json(
                result_path,
                {
                    **correlation_fields(state),
                    "stage_index": stage_index,
                    "stage_name": record.get("stage_name", ""),
                    "stage_resource": record.get("stage_resource", ""),
                    "device_id": str(record.get("device_id") or ""),
                    "runtime_stage_locks": list(
                        record.get(
                            "runtime_stage_locks",
                            record.get("stage_locks", []),
                        )
                    ),
                    "started_at": record.get("stage_started_at", ""),
                    "finished_at": utc_now(),
                    "exit_code": 124,
                    "timeout_seconds": timeout_seconds,
                    "timed_out": True,
                    "shared_lease_wait_seconds": 0.0,
                    "command": [],
                    "error": (
                        "stage worker exceeded its timeout and manager grace: "
                        f"timeout_seconds={timeout_seconds} "
                        f"grace_seconds={MANAGER_STAGE_TIMEOUT_GRACE_SECONDS:g}"
                    ),
                },
            )
        self._event(
            "stage_timeout_recovered",
            **correlation_fields(state),
            stage_index=stage_index,
            stage_name=record.get("stage_name", ""),
            worker_pid=pid,
            timeout_seconds=timeout_seconds,
            stage_age_seconds=stage_age,
        )
        return True

    def _finish_empty_jobs(self) -> None:
        for state in self._states():
            if state.get("state") in TERMINAL_STATES or state.get("state") == "standby":
                continue
            job_dir = self._job_dir(str(state["engine_job_id"]))
            spec = read_json(job_dir / "spec.json")
            running = self._running_stage_records(state)
            if running:
                continue
            if state.get("failure_pending"):
                self._terminal_failure(
                    job_dir, state, error=str(state["failure_pending"])
                )
                continue
            if len(self._completed_stage_indices(state)) >= len(spec["stages"]):
                self._terminal_success(job_dir, state)

    def _promote_standby(self) -> list[dict[str, Any]]:
        promoted: list[dict[str, Any]] = []
        standby_states = sorted(
            (
                state
                for state in self._states()
                if state.get("state") == "standby"
            ),
            key=lambda item: (
                str(item.get("staged_at") or ""),
                str(item.get("engine_job_id") or ""),
            ),
        )
        for state in standby_states:
            if int(self.snapshot().get("admission_credit", 0) or 0) <= 0:
                break
            job_dir = self._job_dir(str(state["engine_job_id"]))
            promoted.append(self._accept_standby(job_dir))
        return promoted

    def _accept_standby(self, job_dir: Path) -> dict[str, Any]:
        spec = read_json(job_dir / "spec.json")
        state_path = job_dir / "state.json"
        state = read_json(state_path)
        accepted_path = job_dir / "accepted.json"
        if state.get("state") != "standby":
            if accepted_path.is_file():
                return read_json(accepted_path)
            raise EngineError(
                f"standby promotion found invalid state: {spec['engine_job_id']}"
            )
        if int(self.snapshot().get("admission_credit", 0) or 0) <= 0:
            raise EngineCapacityError("engine has no fresh admission credit")

        standby_receipt = read_json(job_dir / "standby.json")
        code_generation = engine_code_generation()
        if accepted_path.is_file():
            receipt = read_json(accepted_path)
            accepted_at = str(receipt.get("accepted_at") or utc_now())
        else:
            accepted_at = utc_now()
            receipt = {
                **standby_receipt,
                "state": "accepted",
                "accepted_at": accepted_at,
                "promoted_at": accepted_at,
                "promotion_source": "b-side-standby",
                "engine_code_generation": code_generation,
            }
        receipt["engine_code_generation"] = code_generation
        atomic_write_json(accepted_path, receipt)
        state.update(
            {
                "state": "accepted",
                "stage_state": "pending",
                "accepted_at": accepted_at,
                "promoted_at": str(receipt.get("promoted_at") or accepted_at),
                "promotion_source": "b-side-standby",
                "engine_code_generation": code_generation,
                "updated_at": accepted_at,
            }
        )
        atomic_write_json(state_path, state)
        atomic_write_json(
            self.accepted_dir / f"{spec['engine_job_id']}.json", receipt
        )
        self._event("job_standby_promoted", **receipt)
        return receipt

    def _schedule_ready_stages(self) -> None:
        capacity = self.capacity()
        snapshot = self.snapshot()
        used = dict(snapshot["running_by_resource"])
        used_locks = set(snapshot.get("running_locks", []))
        host_cpu_used = 0
        host_memory_used = 0
        host_io_used = 0
        host_class_used = {
            "cold-build": 0,
            "cache-hit": 0,
            "general": 0,
        }
        active_singleflight: set[str] = set()
        for running_state in self._states():
            for running_record in self._running_stage_records(
                running_state
            ).values():
                if str(running_record.get("stage_resource") or "") != "host":
                    continue
                host_cpu_used += int(
                    running_record.get("host_cpu_weight", 1) or 1
                )
                host_memory_used += int(
                    running_record.get("host_memory_mb", 1024) or 1024
                )
                host_io_used += int(
                    running_record.get("host_io_weight", 1) or 1
                )
                host_class = str(
                    running_record.get("host_concurrency_class") or "general"
                )
                host_class_used[host_class] = (
                    host_class_used.get(host_class, 0) + 1
                )
                singleflight_key = str(
                    running_record.get("singleflight_key") or ""
                )
                if singleflight_key:
                    active_singleflight.add(singleflight_key)
        active_job_limit = int(capacity["active_job_slots"])
        limits = {
            "host": int(capacity["host_slots"]),
            "device": int(capacity["device_slots"]),
            "export": int(capacity["export_slots"]),
        }
        ready: list[
            tuple[
                int,
                int,
                str,
                str,
                int,
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
            ]
        ] = []
        contexts: list[
            tuple[dict[str, Any], dict[str, Any], set[int], dict[int, dict[str, Any]]]
        ] = []
        states = sorted(
            self._states(),
            key=lambda state: (
                str(state.get("accepted_at", "")),
                str(state.get("engine_job_id", "")),
            ),
        )
        for state in states:
            if (
                state.get("state") in TERMINAL_STATES
                or state.get("state") == "standby"
                or state.get("failure_pending")
            ):
                continue
            job_dir = self._job_dir(str(state["engine_job_id"]))
            spec = read_json(job_dir / "spec.json")
            completed = self._completed_stage_indices(state)
            running = self._running_stage_records(state)
            if len(completed) >= len(spec["stages"]) and not running:
                self._terminal_success(job_dir, state)
                continue
            contexts.append((state, spec, completed, running))
            for stage_index, stage in enumerate(spec["stages"]):
                if stage_index in completed or stage_index in running:
                    continue
                if not self._stage_dependencies(spec, stage_index).issubset(completed):
                    continue
                ready.append(
                    (
                        (
                            0
                            if self._device_continuation_ready(
                                spec, completed, stage_index
                            )
                            else 1
                        ),
                        int(stage.get("priority", 100)),
                        str(state.get("accepted_at", "")),
                        str(state.get("engine_job_id", "")),
                        stage_index,
                        state,
                        spec,
                        stage,
                    )
                )

        active_job_ids = {
            str(state.get("engine_job_id", ""))
            for state, _spec, _completed, _running in contexts
            if state.get("activated_at")
        }
        cohort_job_ids = set(active_job_ids)
        if len(cohort_job_ids) < active_job_limit:
            for state, _spec, _completed, _running in contexts:
                job_id = str(state.get("engine_job_id", ""))
                if job_id in cohort_job_ids:
                    continue
                cohort_job_ids.add(job_id)
                if len(cohort_job_ids) >= active_job_limit:
                    break

        cohort_measurements_complete = all(
            self._stage_group_complete(
                completed,
                self._stage_indices_with_lock(spec, "performance-measurement"),
            )
            for state, spec, completed, _running in contexts
            if str(state.get("engine_job_id", "")) in cohort_job_ids
        )
        context_specs = {
            str(state.get("engine_job_id", "")): spec
            for state, spec, _completed, _running in contexts
        }
        cohort_allows_measurement_overlap = all(
            self._measurement_preactivation_overlap_enabled(spec)
            for state, spec, _completed, _running in contexts
            if str(state.get("engine_job_id", "")) in cohort_job_ids
        )
        pre_activation_incomplete = {
            str(state.get("engine_job_id", ""))
            for state, spec, completed, running in contexts
            if self._pre_activation_started(state, spec, completed, running)
            and not self._stage_group_complete(
                completed, self._pre_activation_stage_indices(spec)
            )
        }
        for (
            _continuation_rank,
            _priority,
            _accepted_at,
            _job_id,
            stage_index,
            state,
            spec,
            stage,
        ) in sorted(
            ready, key=lambda item: item[:5]
        ):
            job_dir = self._job_dir(str(state["engine_job_id"]))
            job_id = str(state["engine_job_id"])
            pre_activation = bool(stage.get("pre_activation"))
            if (
                pre_activation
                and job_id not in active_job_ids
                and job_id not in cohort_job_ids
                and not cohort_measurements_complete
                and not (
                    cohort_allows_measurement_overlap
                    and self._measurement_preactivation_overlap_enabled(spec)
                )
            ):
                continue
            if (
                job_id not in active_job_ids
                and len(active_job_ids) >= active_job_limit
                and not pre_activation
            ):
                continue
            resource = str(stage["resource"])
            if used[resource] >= limits[resource]:
                continue
            if resource == "host":
                cpu_demand = int(stage.get("host_cpu_weight", 1) or 1)
                memory_demand = int(
                    stage.get("host_memory_mb", 1024) or 1024
                )
                io_demand = int(stage.get("host_io_weight", 1) or 1)
                host_class = str(
                    stage.get("host_concurrency_class") or "general"
                )
                class_limit = {
                    "cold-build": int(capacity["cold_build_slots"]),
                    "cache-hit": int(capacity["cache_hit_slots"]),
                    "general": int(capacity["host_slots"]),
                }.get(host_class, int(capacity["host_slots"]))
                if host_class_used.get(host_class, 0) >= class_limit:
                    continue
                if (
                    host_cpu_used + cpu_demand
                    > int(capacity["host_cpu_weight_capacity"])
                    or host_memory_used + memory_demand
                    > int(capacity["host_memory_mb_capacity"])
                    or host_io_used + io_demand
                    > int(capacity["host_io_weight_capacity"])
                ):
                    continue
                singleflight_key = str(
                    stage.get("singleflight_key") or ""
                )
                if (
                    singleflight_key
                    and singleflight_key in active_singleflight
                ):
                    continue
            stage_locks = set(str(item) for item in stage.get("locks", []))
            runtime_assignment = self._stage_runtime_assignment(
                capacity,
                state,
                stage,
                used_locks,
            )
            if runtime_assignment is None:
                continue
            device_id, runtime_stage_locks = runtime_assignment
            if (
                resource == "device"
                and "performance-measurement" in stage_locks
                and pre_activation_incomplete
            ):
                if not self._measurement_preactivation_overlap_enabled(spec):
                    continue
                if any(
                    not self._measurement_preactivation_overlap_enabled(
                        context_specs[incomplete_job_id]
                    )
                    for incomplete_job_id in pre_activation_incomplete
                    if incomplete_job_id in context_specs
                ):
                    continue
            activate_job = job_id in active_job_ids or not pre_activation
            self._launch_stage(
                job_dir,
                spec,
                state,
                stage_index,
                stage,
                activate_job=activate_job,
                device_id=device_id,
                runtime_stage_locks=runtime_stage_locks,
            )
            if activate_job:
                active_job_ids.add(job_id)
            elif pre_activation:
                pre_activation_incomplete.add(job_id)
            used[resource] += 1
            used_locks.update(runtime_stage_locks)
            if resource == "host":
                host_cpu_used += cpu_demand
                host_memory_used += memory_demand
                host_io_used += io_demand
                host_class_used[host_class] = (
                    host_class_used.get(host_class, 0) + 1
                )
                if singleflight_key:
                    active_singleflight.add(singleflight_key)

    def _stage_runtime_assignment(
        self,
        capacity: dict[str, Any],
        state: dict[str, Any],
        stage: dict[str, Any],
        used_locks: set[str],
    ) -> tuple[str, list[str]] | None:
        logical_locks = {str(item) for item in stage.get("locks", [])}
        device_affine = (
            str(stage.get("resource")) == "device"
            or "npu" in logical_locks
            or "performance-measurement" in logical_locks
        )
        if not device_affine:
            runtime_locks = sorted(logical_locks)
            if used_locks.intersection(runtime_locks):
                return None
            return "", runtime_locks

        inventory = {
            str(device["device_id"]): device
            for device in capacity.get("device_inventory", [])
            if isinstance(device, dict)
        }
        assigned_device_id = str(state.get("device_id") or "")
        if assigned_device_id:
            device = inventory.get(assigned_device_id)
            candidates = (
                [device]
                if device is not None and bool(device.get("enabled", True))
                else []
            )
        else:
            assignment_counts: dict[str, int] = {
                device_id: 0 for device_id in inventory
            }
            for candidate_state in self._states():
                candidate_device_id = str(candidate_state.get("device_id") or "")
                if (
                    candidate_device_id in assignment_counts
                    and candidate_state.get("state") not in TERMINAL_STATES
                ):
                    assignment_counts[candidate_device_id] += 1
            candidates = sorted(
                (
                    device
                    for device in inventory.values()
                    if bool(device.get("enabled", True))
                    and not bool(device.get("draining", False))
                ),
                key=lambda device: (
                    assignment_counts.get(str(device["device_id"]), 0),
                    str(device["device_id"]),
                ),
            )
        for device in candidates:
            if device is None:
                continue
            runtime_locks = self._materialize_stage_locks(stage, device)
            if not used_locks.intersection(runtime_locks):
                return str(device["device_id"]), runtime_locks
        return None

    @staticmethod
    def _materialize_stage_locks(
        stage: dict[str, Any],
        device: dict[str, Any],
    ) -> list[str]:
        logical_locks = {str(item) for item in stage.get("locks", [])}
        if str(stage.get("resource")) == "device":
            logical_locks.add("npu")
        runtime_locks: set[str] = set()
        for lock in logical_locks:
            if lock == "npu":
                runtime_locks.add(str(device["lease_resource"]))
            elif lock == "performance-measurement":
                runtime_locks.add(str(device["measurement_resource"]))
            else:
                runtime_locks.add(lock)
        return sorted(runtime_locks)

    def _device_continuation_ready(
        self,
        spec: dict[str, Any],
        completed: set[int],
        stage_index: int,
    ) -> bool:
        policy = spec.get("scheduler_policy", {})
        if (
            not isinstance(policy, dict)
            or policy.get("device_continuation") != "enabled"
        ):
            return False
        stages = list(spec.get("stages", []))
        return any(
            completed_index < stage_index
            and str(stages[completed_index].get("resource")) == "device"
            for completed_index in completed
        )

    @staticmethod
    def _stage_indices_with_lock(spec: dict[str, Any], lock: str) -> set[int]:
        return {
            index
            for index, stage in enumerate(spec.get("stages", []))
            if lock in {str(item) for item in stage.get("locks", [])}
        }

    @staticmethod
    def _measurement_preactivation_overlap_enabled(spec: dict[str, Any]) -> bool:
        policy = spec.get("scheduler_policy", {})
        return (
            isinstance(policy, dict)
            and policy.get("measurement_preactivation_overlap") == "enabled"
        )

    @staticmethod
    def _pre_activation_stage_indices(spec: dict[str, Any]) -> set[int]:
        return {
            index
            for index, stage in enumerate(spec.get("stages", []))
            if bool(stage.get("pre_activation"))
        }

    @staticmethod
    def _stage_group_complete(completed: set[int], stage_indices: set[int]) -> bool:
        return stage_indices.issubset(completed)

    def _pre_activation_started(
        self,
        state: dict[str, Any],
        spec: dict[str, Any],
        completed: set[int],
        running: dict[int, dict[str, Any]],
    ) -> bool:
        if state.get("pre_activation_started_at"):
            return True
        indices = self._pre_activation_stage_indices(spec)
        return bool(indices.intersection(completed) or indices.intersection(running))

    def _launch_stage(
        self,
        job_dir: Path,
        spec: dict[str, Any],
        state: dict[str, Any],
        stage_index: int,
        stage: dict[str, Any],
        *,
        activate_job: bool = True,
        device_id: str = "",
        runtime_stage_locks: list[str] | None = None,
    ) -> None:
        # Keep the worker wrapper streams separate from the stage command logs.
        # Windows does not reliably allow the child command to reopen a file
        # already inherited by its wrapper process.
        stdout_path = job_dir / "logs" / f"{stage_index:03d}_{stage['name']}.worker.out.log"
        stderr_path = job_dir / "logs" / f"{stage_index:03d}_{stage['name']}.worker.err.log"
        started_at = utc_now()
        activated_at = str(state.get("activated_at") or started_at)
        execution_deadline_policy = str(
            spec.get("execution_deadline_policy")
            or DEFAULT_EXECUTION_DEADLINE_POLICY
        )
        device_session_policy = (
            execution_deadline_policy == "device-lease-wall-v3"
        )
        deadline_applies = (
            str(stage.get("resource") or "") == "device"
            if device_session_policy
            else (
                execution_deadline_policy == "first-stage-start-hard-cap-v1"
                or activate_job
            )
        )
        started_at_field = (
            "device_session_started_at"
            if device_session_policy
            else "execution_started_at"
        )
        execution_started_at = (
            str(
                state.get(started_at_field)
                or (activated_at if activate_job and not device_session_policy else started_at)
            )
            if deadline_applies
            else ""
        )
        execution_deadline_seconds = int(
            spec.get("execution_deadline_seconds", 0) or 0
        )
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.engine.test_engine_worker",
            "--job-dir",
            str(job_dir),
            "--stage-index",
            str(stage_index),
        ]
        worker_env = os.environ.copy()
        package_root = str(Path(__file__).resolve().parents[1])
        existing_pythonpath = worker_env.get("PYTHONPATH", "")
        worker_env["PYTHONPATH"] = os.pathsep.join(
            item for item in (package_root, existing_pythonpath) if item
        )
        runtime_locks = list(
            runtime_stage_locks
            if runtime_stage_locks is not None
            else stage.get("locks", [])
        )
        worker_env["ASCENDOP_ENGINE_STAGE_LOCKS_JSON"] = json.dumps(runtime_locks)
        if execution_deadline_seconds > 0 and deadline_applies:
            worker_env["ASCENDOP_ENGINE_EXECUTION_DEADLINE_SECONDS"] = str(
                execution_deadline_seconds
            )
            worker_env["ASCENDOP_ENGINE_JOB_DEADLINE_AT_EPOCH_SECONDS"] = str(
                timestamp_epoch_seconds(execution_started_at)
                + execution_deadline_seconds
            )
        if device_id:
            worker_env["ASCENDOP_ENGINE_DEVICE_ID"] = device_id
            worker_env["ASCEND_RT_VISIBLE_DEVICES"] = device_id
        with stdout_path.open("a", encoding="utf-8") as stdout, stderr_path.open(
            "a", encoding="utf-8"
        ) as stderr:
            process = subprocess.Popen(
                command,
                cwd=str(job_dir),
                env=worker_env,
                stdout=stdout,
                stderr=stderr,
                creationflags=hidden_process_creation_flags(),
                startupinfo=hidden_process_startup_info(),
                start_new_session=os.name != "nt",
            )
        worker_start_token = ""
        token_deadline = time.monotonic() + 1.0
        while (
            not worker_start_token
            and process.poll() is None
            and time.monotonic() < token_deadline
        ):
            worker_start_token = process_start_token(process.pid)
            if not worker_start_token:
                time.sleep(0.01)
        started_at = utc_now()
        if bool(stage.get("pre_activation")) and not state.get(
            "pre_activation_started_at"
        ):
            state["pre_activation_started_at"] = started_at
        attempts = state.get("stage_attempts")
        attempts = dict(attempts) if isinstance(attempts, dict) else {}
        attempt = int(attempts.get(str(stage_index), 0) or 0) + 1
        attempts[str(stage_index)] = attempt
        state["stage_attempts"] = attempts
        if deadline_applies:
            state[started_at_field] = execution_started_at
        if execution_deadline_seconds > 0 and deadline_applies:
            state["execution_deadline_seconds"] = execution_deadline_seconds
            state["execution_deadline_policy"] = execution_deadline_policy
        if device_id:
            state["device_id"] = device_id
        running = self._running_stage_records(state)
        running[stage_index] = {
            "stage_index": stage_index,
            "stage_name": stage["name"],
            "stage_resource": stage["resource"],
            "stage_locks": list(stage.get("locks", [])),
            "runtime_stage_locks": runtime_locks,
            "host_concurrency_class": str(
                stage.get("host_concurrency_class") or "none"
            ),
            "host_cpu_weight": int(stage.get("host_cpu_weight", 0) or 0),
            "host_memory_mb": int(stage.get("host_memory_mb", 0) or 0),
            "host_io_weight": int(stage.get("host_io_weight", 0) or 0),
            "singleflight_key": str(stage.get("singleflight_key") or ""),
            "device_id": device_id,
            "depends_on": list(stage.get("depends_on", [])),
            "pre_activation": bool(stage.get("pre_activation")),
            "stage_started_at": started_at,
            "stage_attempt": attempt,
            "timeout_seconds": int(stage.get("timeout_seconds", 0) or 0),
            "worker_pid": process.pid,
            "worker_start_token": worker_start_token,
        }
        if activate_job:
            state["activated_at"] = activated_at
        state["updated_at"] = started_at
        self._sync_state_projection(state, spec, running)
        atomic_write_json(job_dir / "state.json", state)
        self._event(
            "stage_started",
            **correlation_fields(spec),
            stage_index=stage_index,
            stage_name=stage["name"],
            stage_resource=stage["resource"],
            stage_locks=list(stage.get("locks", [])),
            runtime_stage_locks=runtime_locks,
            host_concurrency_class=str(
                stage.get("host_concurrency_class") or "none"
            ),
            host_cpu_weight=int(stage.get("host_cpu_weight", 0) or 0),
            host_memory_mb=int(stage.get("host_memory_mb", 0) or 0),
            host_io_weight=int(stage.get("host_io_weight", 0) or 0),
            singleflight_key=str(stage.get("singleflight_key") or ""),
            device_id=device_id,
            stage_attempt=attempt,
            timeout_seconds=int(stage.get("timeout_seconds", 0) or 0),
            depends_on=list(stage.get("depends_on", [])),
            pre_activation=bool(stage.get("pre_activation")),
            activates_job=activate_job,
            worker_pid=process.pid,
            worker_start_token=worker_start_token,
        )

    def _apply_stage_result(
        self,
        job_dir: Path,
        state: dict[str, Any],
        spec: dict[str, Any],
        stage_index: int,
        record: dict[str, Any],
        result: dict[str, Any],
    ) -> bool:
        if int(result.get("stage_index", -1)) != stage_index:
            return self._mark_stage_failure(
                job_dir,
                state,
                spec,
                stage_index,
                record,
                error="stage result index mismatch",
            )
        exit_code = int(result.get("exit_code", 1))
        history = list(state.get("history", []))
        history.append(
            {
                "stage_index": stage_index,
                "stage_name": record.get("stage_name", ""),
                "stage_resource": record.get("stage_resource", ""),
                "stage_locks": list(record.get("stage_locks", [])),
                "runtime_stage_locks": list(
                    record.get("runtime_stage_locks", record.get("stage_locks", []))
                ),
                "device_id": str(record.get("device_id") or ""),
                "depends_on": list(record.get("depends_on", [])),
                "pre_activation": bool(record.get("pre_activation")),
                "stage_attempt": int(record.get("stage_attempt", 1) or 1),
                "started_at": result.get(
                    "started_at", record.get("stage_started_at", "")
                ),
                "finished_at": result.get("finished_at", utc_now()),
                "host": str(result.get("host") or ""),
                "boot_id": str(result.get("boot_id") or ""),
                "pid": int(result.get("pid", record.get("worker_pid", 0)) or 0),
                "started_monotonic_ns": int(
                    result.get("started_monotonic_ns", 0) or 0
                ),
                "finished_monotonic_ns": int(
                    result.get("finished_monotonic_ns", 0) or 0
                ),
                "duration_ns": int(result.get("duration_ns", 0) or 0),
                "exit_code": exit_code,
                "error": str(result.get("error") or ""),
                "timeout_seconds": int(result.get("timeout_seconds", 0) or 0),
                "timed_out": bool(result.get("timed_out")),
                "stdout_path": f"logs/{stage_index:03d}_{record.get('stage_name', '')}.out.log",
                "stderr_path": f"logs/{stage_index:03d}_{record.get('stage_name', '')}.err.log",
                "result_path": f"stage_results/{stage_index:03d}.json",
                "shared_lease_wait_seconds": float(
                    result.get("shared_lease_wait_seconds", 0.0) or 0.0
                ),
            }
        )
        state["history"] = history
        running = self._running_stage_records(state)
        running.pop(stage_index, None)
        if exit_code != 0:
            return self._mark_stage_failure(
                job_dir,
                state,
                spec,
                stage_index,
                record,
                error=str(result.get("error") or f"stage exit code {exit_code}"),
                running=running,
                history_already_recorded=True,
            )
        completed = self._completed_stage_indices(state)
        completed.add(stage_index)
        state["completed_stage_indices"] = sorted(completed)
        if (
            str(spec.get("execution_deadline_policy") or "")
            == "device-lease-wall-v3"
            and str(record.get("stage_resource") or "") == "device"
        ):
            remaining_device_stages = {
                index
                for index, stage in enumerate(spec.get("stages", []))
                if str(stage.get("resource") or "") == "device"
                and index not in completed
            }
            if not remaining_device_stages:
                state["device_session_finished_at"] = str(
                    result.get("finished_at") or utc_now()
                )
        state["updated_at"] = utc_now()
        self._sync_state_projection(state, spec, running)
        atomic_write_json(job_dir / "state.json", state)
        self._event(
            "stage_completed",
            **correlation_fields(state),
            stage_index=stage_index,
            stage_name=record.get("stage_name", ""),
            finished_at=result.get("finished_at", ""),
        )
        if state.get("failure_pending") and not running:
            self._terminal_failure(
                job_dir, state, error=str(state["failure_pending"])
            )
            return False
        return True

    def _handle_disappeared_stage(
        self,
        job_dir: Path,
        state: dict[str, Any],
        spec: dict[str, Any],
        stage_index: int,
        record: dict[str, Any],
    ) -> bool:
        stage = spec["stages"][stage_index]
        pid = int(record.get("worker_pid", 0) or 0)
        attempt = int(record.get("stage_attempt", 1) or 1)
        max_attempts = int(stage.get("max_attempts", 1) or 1)
        error = f"stage worker disappeared: pid={pid}"
        history = list(state.get("history", []))
        history.append(
            {
                "stage_index": stage_index,
                "stage_name": record.get("stage_name", ""),
                "stage_resource": record.get("stage_resource", ""),
                "stage_locks": list(record.get("stage_locks", [])),
                "runtime_stage_locks": list(
                    record.get("runtime_stage_locks", record.get("stage_locks", []))
                ),
                "device_id": str(record.get("device_id") or ""),
                "depends_on": list(record.get("depends_on", [])),
                "stage_attempt": attempt,
                "started_at": record.get("stage_started_at", ""),
                "finished_at": utc_now(),
                "exit_code": -1,
                "error": error,
            }
        )
        state["history"] = history
        running = self._running_stage_records(state)
        running.pop(stage_index, None)
        if str(stage.get("resource")) != "device" and attempt < max_attempts:
            state["updated_at"] = utc_now()
            self._sync_state_projection(state, spec, running)
            atomic_write_json(job_dir / "state.json", state)
            self._event(
                "stage_retry_scheduled",
                **correlation_fields(state),
                stage_index=stage_index,
                stage_name=record.get("stage_name", ""),
                stage_attempt=attempt,
                max_attempts=max_attempts,
                reason=error,
            )
            return True
        return self._mark_stage_failure(
            job_dir,
            state,
            spec,
            stage_index,
            record,
            error=f"stage worker disappeared without result: pid={pid}",
            running=running,
            history_already_recorded=True,
        )

    def _mark_stage_failure(
        self,
        job_dir: Path,
        state: dict[str, Any],
        spec: dict[str, Any],
        stage_index: int,
        record: dict[str, Any],
        *,
        error: str,
        running: dict[int, dict[str, Any]] | None = None,
        history_already_recorded: bool = False,
    ) -> bool:
        records = running if running is not None else self._running_stage_records(state)
        records.pop(stage_index, None)
        if not history_already_recorded:
            history = list(state.get("history", []))
            history.append(
                {
                    "stage_index": stage_index,
                    "stage_name": record.get("stage_name", ""),
                    "stage_resource": record.get("stage_resource", ""),
                    "stage_locks": list(record.get("stage_locks", [])),
                    "runtime_stage_locks": list(
                        record.get(
                            "runtime_stage_locks", record.get("stage_locks", [])
                        )
                    ),
                    "device_id": str(record.get("device_id") or ""),
                    "depends_on": list(record.get("depends_on", [])),
                    "stage_attempt": int(record.get("stage_attempt", 1) or 1),
                    "started_at": record.get("stage_started_at", ""),
                    "finished_at": utc_now(),
                    "exit_code": -1,
                    "error": error,
                }
            )
            state["history"] = history
        state["failure_pending"] = error
        if (
            str(spec.get("execution_deadline_policy") or "")
            == "device-lease-wall-v3"
            and str(record.get("stage_resource") or "") == "device"
        ):
            state["device_session_finished_at"] = utc_now()
        state["updated_at"] = utc_now()
        self._sync_state_projection(state, spec, records)
        atomic_write_json(job_dir / "state.json", state)
        self._event(
            "stage_failed",
            **correlation_fields(state),
            stage_index=stage_index,
            stage_name=record.get("stage_name", ""),
            error=error,
            waiting_for_siblings=bool(records),
        )
        if records:
            return True
        self._terminal_failure(job_dir, state, error=error)
        return False

    def _terminal_success(self, job_dir: Path, state: dict[str, Any]) -> None:
        spec = read_json(job_dir / "spec.json")
        missing = missing_required_artifacts(job_dir, spec.get("required_artifacts", []))
        if missing:
            self._terminal_failure(
                job_dir,
                state,
                error="required artifacts missing: " + ", ".join(missing),
            )
            return
        self._write_terminal(job_dir, state, "completed", "")

    def _terminal_failure(self, job_dir: Path, state: dict[str, Any], *, error: str) -> None:
        self._write_terminal(job_dir, state, "failed", error)

    def _write_terminal(
        self, job_dir: Path, state: dict[str, Any], terminal_state: str, error: str
    ) -> None:
        if state.get("state") in TERMINAL_STATES:
            return
        spec = read_json(job_dir / "spec.json")
        if terminal_state == "failed":
            ensure_failure_protocol_artifacts(job_dir, spec, state, error=error)
            failure = classify_terminal_failure(job_dir, state)
            state.update(failure)
        required_artifacts = (
            failure_required_artifacts(job_dir, spec, state)
            if terminal_state == "failed"
            else list(spec.get("required_artifacts", []))
        )
        diagnostic_artifacts = (
            failure_diagnostic_artifacts() if terminal_state == "failed" else []
        )
        optional_artifacts = list(spec.get("optional_artifacts", []))
        optional_artifacts.extend(
            item for item in diagnostic_artifacts if item not in optional_artifacts
        )
        artifacts = collect_result_artifacts(
            job_dir,
            required_artifacts,
            optional_artifacts,
        )
        terminal_at = utc_now()
        state.update(
            {
                "state": terminal_state,
                "stage_state": "terminal",
                "running_stages": {},
                "terminal_at": terminal_at,
                "return_ready_at": terminal_at,
                "updated_at": terminal_at,
            }
        )
        if error:
            state["error"] = error
        for key in (
            "stage_name",
            "stage_resource",
            "stage_locks",
            "stage_attempt",
            "stage_started_at",
            "worker_pid",
            "failure_pending",
        ):
            state.pop(key, None)
        atomic_write_json(job_dir / "state.json", state)
        manifest = {
            **correlation_fields(spec),
            "protocol_version": PROTOCOL_VERSION,
            "bundle_hash": spec["bundle_hash"],
            "execution_profile": str(spec.get("execution_profile") or ""),
            "scheduler_policy": dict(spec["scheduler_policy"]),
            "engine_code_generation": str(
                state.get("engine_code_generation") or engine_code_generation()
            ),
            "input_identity": dict(spec.get("input_identity", {})),
            "state": terminal_state,
            "accepted_at": state["accepted_at"],
            "terminal_at": terminal_at,
            "return_ready_at": terminal_at,
            "history": state.get("history", []),
            "failure_domain": str(state.get("failure_domain") or ""),
            "failure_code": str(state.get("failure_code") or ""),
            "required_artifacts": required_artifacts,
            "optional_artifacts": optional_artifacts,
            "diagnostic_artifacts": diagnostic_artifacts,
            "artifacts": artifacts,
        }
        if error:
            manifest["error"] = error
        atomic_write_json(job_dir / "terminal.json", manifest)
        atomic_write_json(job_dir / "artifact_manifest.json", {"artifacts": artifacts})
        atomic_write_json(self.return_ready_dir / f"{spec['engine_job_id']}.json", manifest)
        self._event("job_terminal", **manifest)

    def _write_snapshot(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        atomic_write_json(self.status_path, snapshot)
        return snapshot

    def _job_dir(self, engine_job_id: str) -> Path:
        token = normalize_token(engine_job_id, "engine_job_id")
        return self.jobs_dir / token

    def _event(self, kind: str, **fields: Any) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        payload = {"time": utc_now(), "kind": kind, **fields}
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def failure_primary_artifacts() -> list[str]:
    return [
        "result/SUMMARY.txt",
        "result/PHASE_TIMELINE.jsonl",
        "result/ENGINE_IDENTITY.json",
        "result/PERF_CAPTURE.json",
        "result/PERF_BATCH.json",
        "result/CORRECTNESS.json",
        "result/CORRECTNESS_SUMMARY.txt",
        "result/CORRECTNESS_BATCH.json",
        "result/FAILURE_CASE_LOG.txt",
        "result/WHEEL_CACHE.json",
        "result/OPERATOR_CACHE.json",
        "result/RUNTIME_READINESS.json",
    ]


def failure_diagnostic_artifacts() -> list[str]:
    return [
        *failure_primary_artifacts(),
        "logs",
        "stage_results",
    ]


def classify_terminal_failure(
    job_dir: Path,
    state: dict[str, Any],
) -> dict[str, str]:
    history = state.get("history", [])
    latest = history[-1] if isinstance(history, list) and history else {}
    stage_name = str(latest.get("stage_name") or "")
    correctness_path = job_dir / "result" / "CORRECTNESS.json"
    if stage_name == "correctness" and correctness_path.is_file():
        correctness = read_json(correctness_path)
        expected = int(correctness.get("expected_execution_count", 0) or 0)
        executed = int(correctness.get("execution_count", 0) or 0)
        executions = correctness.get("executions", [])
        complete = (
            expected > 0
            and executed == expected
            and isinstance(executions, list)
            and len(executions) == expected
        )
        mismatch = complete and any(
            isinstance(item, dict) and str(item.get("verdict") or "") == "FAIL"
            for item in executions
        )
        if mismatch:
            return {
                "failure_domain": "business",
                "failure_code": "correctness-mismatch",
            }
    return {
        "failure_domain": "infrastructure",
        "failure_code": f"stage-failed:{stage_name or 'unknown'}",
    }


def failure_required_artifacts(
    job_dir: Path,
    spec: dict[str, Any],
    state: dict[str, Any],
) -> list[str]:
    conditional = spec.get("required_artifacts_by_terminal_state", {})
    if (
        str(state.get("failure_domain") or "") == "business"
        and isinstance(conditional, dict)
    ):
        selected = conditional.get("terminal-business-failure")
    else:
        selected = None
    values = [
        str(item)
        for item in (
            selected
            if isinstance(selected, list)
            else spec.get("required_artifacts", [])
        )
    ]
    for relative in failure_primary_artifacts():
        if relative in values or not (job_dir / relative).exists():
            continue
        values.append(relative)
    return values


def ensure_failure_protocol_artifacts(
    job_dir: Path,
    spec: dict[str, Any],
    state: dict[str, Any],
    *,
    error: str,
) -> None:
    result_dir = job_dir / "result"
    result_dir.mkdir(parents=True, exist_ok=True)
    work_dir = job_dir / "work"
    terminal_at = str(state.get("terminal_at") or utc_now())
    history = state.get("history", [])
    latest_stage = history[-1] if isinstance(history, list) and history else {}

    summary_path = result_dir / "SUMMARY.txt"
    if not summary_path.exists():
        lines = [
            "ENGINE_JOB_STATE: failed",
            f"ENGINE_JOB_ID: {spec.get('engine_job_id', '')}",
            f"REQUEST_ID: {spec.get('request_id', '')}",
            f"OPERATOR: {spec.get('operator', '')}",
            f"TEST_VERSION: {spec.get('test_version', '')}",
            f"FAILED_STAGE: {latest_stage.get('stage_name', state.get('stage_name', ''))}",
            f"FAILED_STAGE_INDEX: {latest_stage.get('stage_index', state.get('stage_index', ''))}",
            f"EXIT_CODE: {latest_stage.get('exit_code', '')}",
            f"ERROR: {error}",
            f"TERMINAL_AT: {terminal_at}",
        ]
        summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    timeline_path = result_dir / "PHASE_TIMELINE.jsonl"
    if not timeline_path.exists():
        source_timeline = job_dir / "work" / "PHASE_TIMELINE.jsonl"
        if source_timeline.is_file():
            shutil.copy2(source_timeline, timeline_path)
        event = {
            "phase": "engine-terminal-failed",
            "timestamp": terminal_at,
            "engine_job_id": str(spec.get("engine_job_id") or ""),
            "stage_index": latest_stage.get("stage_index", state.get("stage_index", "")),
            "stage_name": latest_stage.get("stage_name", state.get("stage_name", "")),
            "exit_code": latest_stage.get("exit_code", ""),
            "error": error,
        }
        with timeline_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True, sort_keys=True) + "\n")

    identity_path = result_dir / "ENGINE_IDENTITY.json"
    if not identity_path.exists():
        input_identity = spec.get("input_identity", {})
        if not isinstance(input_identity, dict):
            input_identity = {}
        identity = {
            "protocol_version": "engine-runtime-identity-v1-failure",
            "generated_at": terminal_at,
            "engine_job_id": str(spec.get("engine_job_id") or ""),
            "request_id": str(spec.get("request_id") or ""),
            "attempt_id": str(spec.get("attempt_id") or ""),
            "operator": str(spec.get("operator") or ""),
            "test_version": str(
                input_identity.get("test_version") or spec.get("test_version") or ""
            ),
            "source_sha256": str(input_identity.get("source_sha256") or ""),
            "case_bundle_sha256": str(input_identity.get("case_bundle_sha256") or ""),
            "golden_bundle_sha256": str(
                input_identity.get("golden_bundle_sha256") or ""
            ),
            "test_contract_sha256": str(
                input_identity.get("test_contract_sha256") or ""
            ),
            "correctness_case_count": input_identity.get("correctness_case_count", 0),
            "performance_case_count": input_identity.get("performance_case_count", 0),
            "correctness_repetitions": input_identity.get("correctness_repetitions", 0),
            "performance_samples_per_case": input_identity.get(
                "performance_samples_per_case", 0
            ),
            "environment_sha256": "",
            "environment": {
                "status": "unavailable-before-runtime-setup",
                "terminal_state": "failed",
                "error": error,
            },
        }
        atomic_write_json(identity_path, identity)

    for name in (
        "PERF_CAPTURE.json",
        "PERF_BATCH.json",
        "CORRECTNESS.json",
        "CORRECTNESS_SUMMARY.txt",
        "CORRECTNESS_BATCH.json",
        "WHEEL_CACHE.json",
        "OPERATOR_CACHE.json",
        "RUNTIME_READINESS.json",
    ):
        source = work_dir / name
        destination = result_dir / name
        if source.is_file() and not destination.exists():
            shutil.copy2(source, destination)

    failure_log_path = result_dir / "FAILURE_CASE_LOG.txt"
    if not failure_log_path.exists():
        excerpts = failure_case_log_excerpts(work_dir)
        if excerpts:
            failure_log_path.write_text(excerpts, encoding="utf-8")


def failure_case_log_excerpts(work_dir: Path, *, limit_bytes: int = 131_072) -> str:
    batch_path = work_dir / "PERF_BATCH.json"
    if not batch_path.is_file():
        return ""
    try:
        batch = read_json(batch_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return ""
    executions = batch.get("executions")
    if not isinstance(executions, list):
        return ""
    resolved_work = work_dir.resolve()
    parts: list[str] = []
    size = 0
    for raw in executions:
        if not isinstance(raw, dict) or str(raw.get("verdict") or "") == "PASS":
            continue
        value = str(raw.get("log") or "").strip()
        if not value:
            continue
        source = Path(value)
        if not source.is_absolute():
            source = work_dir / source
        try:
            source = source.resolve()
        except OSError:
            continue
        if source != resolved_work and resolved_work not in source.parents:
            continue
        try:
            text = source.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        header = (
            f"===== case={int(raw.get('case', 0) or 0)} "
            f"repetition={int(raw.get('repetition', 0) or 0)} "
            f"verdict={str(raw.get('verdict') or '')} =====\n"
        )
        remaining = max(0, limit_bytes - size - len(header.encode("utf-8")))
        if remaining <= 0:
            break
        encoded = text.encode("utf-8", errors="replace")[-remaining:]
        excerpt = encoded.decode("utf-8", errors="replace")
        part = header + excerpt.rstrip() + "\n"
        parts.append(part)
        size += len(part.encode("utf-8"))
        if size >= limit_bytes:
            break
    return "\n".join(parts)


def copy_payload_tree(source: Path, destination: Path) -> None:
    try:
        materialize_payload_tree(source, destination)
    except PayloadArchiveError as exc:
        raise EngineError(str(exc)) from exc


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    if not root.exists():
        return digest.hexdigest()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if path.is_file():
            update_canonical_file_digest(digest, path)
            digest.update(b"\0")
    return digest.hexdigest()


def update_canonical_file_digest(digest: Any, path: Path) -> None:
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            data = carry + chunk
            carry = b"\r" if data.endswith(b"\r") else b""
            if carry:
                data = data[:-1]
            digest.update(data.replace(b"\r\n", b"\n"))
    if carry:
        digest.update(carry)


def canonical_file_manifest(root: Path) -> dict[str, dict[str, object]]:
    manifest: dict[str, dict[str, object]] = {}
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file():
            continue
        digest = hashlib.sha256()
        update_canonical_file_digest(digest, path)
        manifest[path.relative_to(root).as_posix()] = {
            "sha256": digest.hexdigest(),
            "size_bytes": path.stat().st_size,
        }
    return manifest


def safe_artifact_path(job_dir: Path, value: object) -> tuple[str, Path]:
    relative = str(value or "").replace("\\", "/").strip("/")
    if not relative or ".." in Path(relative).parts:
        raise EngineError(f"unsafe engine artifact path: {value}")
    target = (job_dir / relative).resolve()
    resolved_job = job_dir.resolve()
    if target != resolved_job and resolved_job not in target.parents:
        raise EngineError(f"engine artifact escapes job root: {value}")
    return relative, target


def compact_returned_job(job_dir: Path) -> dict[str, Any]:
    resolved_job = job_dir.resolve()
    reclaimed_bytes = 0
    removed: list[str] = []
    errors: list[str] = []
    for name in RETURN_ACK_COMPACT_DIRS:
        path = (resolved_job / name).resolve()
        if resolved_job not in path.parents or not path.exists():
            continue
        reclaimed_bytes += path_storage_bytes(path)
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            removed.append(name)
        except OSError as exc:
            errors.append(f"{name}: {type(exc).__name__}: {exc}")
    return {
        "protocol_version": "engine-return-compaction-v1",
        "compacted_at": utc_now(),
        "state": "compacted" if not errors else "partial",
        "removed": removed,
        "reclaimed_bytes": reclaimed_bytes,
        "errors": errors,
        "retained": [
            "accepted.json",
            "artifact_manifest.json",
            "spec.json",
            "state.json",
            "terminal.json",
        ],
    }


def path_storage_bytes(path: Path) -> int:
    try:
        if path.is_file():
            return int(path.stat().st_size)
    except OSError:
        return 0
    total = 0
    try:
        entries = path.rglob("*") if path.is_dir() else []
        for item in entries:
            try:
                if item.is_file():
                    total += int(item.stat().st_size)
            except OSError:
                continue
    except OSError:
        return total
    return total


def missing_required_artifacts(job_dir: Path, values: object) -> list[str]:
    if not isinstance(values, list):
        return ["<invalid required_artifacts>"]
    missing: list[str] = []
    for value in values:
        relative, target = safe_artifact_path(job_dir, value)
        if not target.exists():
            missing.append(relative)
    return missing


def collect_result_artifacts(
    job_dir: Path,
    required: object,
    optional: object,
) -> list[dict[str, Any]]:
    bundle_root = job_dir / "result_bundle"
    bundle_root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    requested: list[tuple[str, bool]] = []
    for values, required_flag in ((required, True), (optional, False)):
        if isinstance(values, list):
            requested.extend((str(value), required_flag) for value in values)
    seen: set[str] = set()
    for value, required_flag in requested:
        relative, source = safe_artifact_path(job_dir, value)
        if relative in seen or not source.exists():
            continue
        seen.add(relative)
        destination = bundle_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, destination, dirs_exist_ok=True)
            digest = tree_digest(source)
            kind = "directory"
            size_bytes = sum(path.stat().st_size for path in source.rglob("*") if path.is_file())
        else:
            shutil.copy2(source, destination)
            digest = file_digest(source)
            kind = "file"
            size_bytes = source.stat().st_size
        records.append(
            {
                "path": relative,
                "required": required_flag,
                "kind": kind,
                "size_bytes": size_bytes,
                "sha256": digest,
            }
        )
    return records


def copy_return_artifacts(
    source_root: Path,
    destination_root: Path,
    artifacts: list[dict[str, Any]],
) -> None:
    destination_root.mkdir(parents=True, exist_ok=True)
    resolved_source = source_root.resolve()
    for artifact in artifacts:
        relative = str(artifact.get("path") or "").replace("\\", "/").strip("/")
        if not relative or ".." in Path(relative).parts:
            raise EngineError(f"unsafe return artifact path: {relative}")
        source = (resolved_source / relative).resolve()
        if source != resolved_source and resolved_source not in source.parents:
            raise EngineError(f"return artifact escapes result bundle: {relative}")
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        kind = str(artifact.get("kind") or "")
        if kind == "file" and source.is_file():
            shutil.copy2(source, destination)
        elif kind == "directory" and source.is_dir():
            shutil.copytree(source, destination, dirs_exist_ok=True)
        else:
            raise EngineError(f"return artifact is missing or wrong kind: {relative}")


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def return_backlog_stats(manifests: Iterable[dict[str, Any]]) -> dict[str, int]:
    jobs = 0
    total_bytes = 0
    required_bytes = 0
    for manifest in manifests:
        if not isinstance(manifest, dict):
            continue
        jobs += 1
        artifacts = manifest.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            size = max(0, int(artifact.get("size_bytes", 0) or 0))
            total_bytes += size
            if artifact.get("required") and not manifest.get("required_returned_at"):
                required_bytes += size
    return {
        "jobs": jobs,
        "total_bytes": total_bytes,
        "required_bytes": required_bytes,
    }


def effective_return_backlog_limits(capacity: dict[str, Any]) -> dict[str, int]:
    max_inflight = max(1, int(capacity["max_inflight"]))
    soft_jobs = int(capacity.get("return_backlog_soft_limit_jobs", 0) or 0)
    hard_jobs = int(capacity.get("return_backlog_hard_limit_jobs", 0) or 0)
    if soft_jobs == 0:
        soft_jobs = max(2, max_inflight * 2)
    if hard_jobs == 0:
        hard_jobs = max(soft_jobs + 1, max_inflight * 4)
    return {
        "soft_bytes": int(capacity["return_backlog_soft_limit_bytes"]),
        "hard_bytes": int(capacity["return_backlog_hard_limit_bytes"]),
        "soft_jobs": soft_jobs,
        "hard_jobs": hard_jobs,
    }


def return_backlog_pressure(
    backlog: dict[str, int], limits: dict[str, int]
) -> dict[str, Any]:
    byte_ratio = threshold_ratio(
        int(backlog.get("total_bytes", 0) or 0),
        int(limits["soft_bytes"]),
        int(limits["hard_bytes"]),
    )
    job_ratio = threshold_ratio(
        int(backlog.get("jobs", 0) or 0),
        int(limits["soft_jobs"]),
        int(limits["hard_jobs"]),
    )
    ratio = max(byte_ratio, job_ratio)
    reasons: list[str] = []
    if byte_ratio > 0:
        reasons.append("bytes")
    if job_ratio > 0:
        reasons.append("jobs")
    return {
        "ratio": round(min(1.0, max(0.0, ratio)), 6),
        "reason": "+".join(reasons),
        "byte_ratio": round(byte_ratio, 6),
        "job_ratio": round(job_ratio, 6),
    }


def threshold_ratio(value: int, soft: int, hard: int) -> float:
    if value <= soft:
        return 0.0
    if value >= hard:
        return 1.0
    return (value - soft) / (hard - soft)


def discover_runtime_device_inventory() -> list[dict[str, Any]]:
    configured = os.environ.get("ASCENDOP_ENGINE_DEVICE_IDS")
    if configured is not None:
        runtime_ids = sorted(
            {
                item.strip()
                for item in configured.split(",")
                if item.strip()
                and all(
                    character.isalnum() or character in "._-"
                    for character in item.strip()
                )
            },
            key=device_id_sort_key,
        )
        return device_inventory_for_ids(
            runtime_ids,
            physical_device_ids=_configured_device_ids(
                "ASCENDOP_ENGINE_PHYSICAL_DEVICE_IDS"
            ),
        )
    device_root = Path("/dev")
    if not device_root.is_dir():
        return []
    detected: set[str] = set()
    for path in device_root.glob("davinci*"):
        suffix = path.name.removeprefix("davinci")
        if suffix.isdigit():
            detected.add(suffix)
    physical_ids = sorted(detected, key=device_id_sort_key)
    return device_inventory_for_ids(
        [str(index) for index in range(len(physical_ids))],
        physical_device_ids=physical_ids,
    )


def _configured_device_ids(name: str) -> list[str]:
    configured = os.environ.get(name)
    if configured is None:
        return []
    return sorted(
        {
            item.strip()
            for item in configured.split(",")
            if item.strip()
            and all(
                character.isalnum() or character in "._-"
                for character in item.strip()
            )
        },
        key=device_id_sort_key,
    )


def device_id_sort_key(device_id: str) -> tuple[int, int | str]:
    if device_id.isdigit():
        return (0, int(device_id))
    return (1, device_id)


def device_inventory_for_ids(
    device_ids: Iterable[str],
    *,
    physical_device_ids: Iterable[str] = (),
) -> list[dict[str, Any]]:
    physical_ids = list(physical_device_ids)
    inventory: list[dict[str, Any]] = []
    for index, device_id in enumerate(device_ids):
        item = {
            "device_id": str(device_id),
            "enabled": True,
            "draining": False,
        }
        if index < len(physical_ids):
            item["physical_device_id"] = str(physical_ids[index])
        inventory.append(item)
    return inventory


def validate_capacity(raw: dict[str, Any]) -> dict[str, Any]:
    payload = dict(DEFAULT_CAPACITY)
    payload.update(raw)
    if "active_job_slots" not in raw:
        payload["active_job_slots"] = payload["max_inflight"]
    raw_inventory = raw.get("device_inventory")
    if raw_inventory is None:
        legacy_slots = int(raw.get("device_slots", payload["device_slots"]))
        raw_inventory = [
            {
                "device_id": str(index),
                "enabled": True,
                "draining": False,
            }
            for index in range(max(0, legacy_slots))
        ]
    if not isinstance(raw_inventory, list):
        raise EngineError("device_inventory must be a list")
    inventory: list[dict[str, Any]] = []
    seen_device_ids: set[str] = set()
    for raw_device in raw_inventory:
        if not isinstance(raw_device, dict):
            raise EngineError("device_inventory entries must be objects")
        device_id = str(raw_device.get("device_id") or "").strip()
        if not device_id or not all(
            character.isalnum() or character in "._-" for character in device_id
        ):
            raise EngineError(f"invalid engine device_id: {device_id!r}")
        if device_id in seen_device_ids:
            raise EngineError(f"duplicate engine device_id: {device_id}")
        seen_device_ids.add(device_id)
        enabled = bool(raw_device.get("enabled", True))
        draining = bool(raw_device.get("draining", False))
        device = {
            "device_id": device_id,
            "enabled": enabled,
            "draining": draining,
            "lease_resource": f"npu:{device_id}",
            "measurement_resource": f"performance-measurement:{device_id}",
        }
        for key in (
            "physical_device_id",
            "soc",
            "exclusive_measurement",
        ):
            if key in raw_device:
                device[key] = raw_device[key]
        inventory.append(device)
    if not inventory:
        raise EngineError("device_inventory must contain at least one runtime device")
    payload["device_inventory"] = inventory
    payload["device_slots"] = sum(
        1
        for device in inventory
        if bool(device["enabled"]) and not bool(device["draining"])
    )
    for key in (
        "max_inflight",
        "active_job_slots",
        "host_slots",
        "host_cpu_weight_capacity",
        "host_memory_mb_capacity",
        "host_io_weight_capacity",
        "cold_build_slots",
        "cache_hit_slots",
        "export_slots",
    ):
        value = int(payload[key])
        if value < 1:
            raise EngineError(f"capacity must be positive: {key}={value}")
        payload[key] = value
    payload["device_slots"] = int(payload["device_slots"])
    standby_slots = int(payload.get("standby_slots", 0) or 0)
    if standby_slots < 0:
        raise EngineError(
            f"standby capacity cannot be negative: standby_slots={standby_slots}"
        )
    payload["standby_slots"] = standby_slots
    if int(payload["active_job_slots"]) > int(payload["max_inflight"]):
        raise EngineError(
            "active_job_slots cannot exceed accepted queue max_inflight: "
            f"active_job_slots={payload['active_job_slots']} "
            f"max_inflight={payload['max_inflight']}"
        )
    for prefix in ("bytes", "jobs"):
        soft_key = f"return_backlog_soft_limit_{prefix}"
        hard_key = f"return_backlog_hard_limit_{prefix}"
        soft = int(payload[soft_key])
        hard = int(payload[hard_key])
        if soft < 0 or hard < 0:
            raise EngineError(f"return backlog limits cannot be negative: {prefix}")
        if prefix == "bytes" and (soft == 0 or hard <= soft):
            raise EngineError("return backlog byte limits require 0 < soft < hard")
        if prefix == "jobs" and ((soft == 0) != (hard == 0)):
            raise EngineError("return backlog job limits must both be zero(auto) or explicit")
        if prefix == "jobs" and soft > 0 and hard <= soft:
            raise EngineError("return backlog job limits require soft < hard")
        payload[soft_key] = soft
        payload[hard_key] = hard
    payload["draining"] = bool(payload["draining"])
    return payload


def validate_spec(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise EngineError("engine job spec must be an object")
    payload = json.loads(json.dumps(raw))
    version = str(payload.get("protocol_version") or PROTOCOL_VERSION)
    if version != PROTOCOL_VERSION:
        raise EngineError(f"unsupported engine protocol: {version}")
    payload["protocol_version"] = version
    execution_deadline_seconds = int(
        payload.get("execution_deadline_seconds", 0) or 0
    )
    if execution_deadline_seconds < 0 or execution_deadline_seconds > 604800:
        raise EngineError("execution_deadline_seconds must be within 0..604800")
    payload["execution_deadline_seconds"] = execution_deadline_seconds
    execution_deadline_policy = str(
        payload.get("execution_deadline_policy")
        or DEFAULT_EXECUTION_DEADLINE_POLICY
    )
    if execution_deadline_policy not in EXECUTION_DEADLINE_POLICIES:
        raise EngineError(
            "execution_deadline_policy must be one of "
            + ", ".join(sorted(EXECUTION_DEADLINE_POLICIES))
        )
    payload["execution_deadline_policy"] = execution_deadline_policy
    for key in (
        "request_id",
        "engine_job_id",
        "attempt_id",
        "operator",
        "test_version",
        "bundle_hash",
    ):
        payload[key] = normalize_token(str(payload.get(key, "")), key)
    stages = payload.get("stages", [])
    if not isinstance(stages, list):
        raise EngineError("engine job stages must be a list")
    normalized_stages: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    explicit_dependencies: list[list[str] | None] = []
    for index, stage in enumerate(stages):
        if not isinstance(stage, dict):
            raise EngineError(f"stage {index} must be an object")
        name = normalize_token(str(stage.get("name", "")), f"stage[{index}].name")
        if name in seen_names:
            raise EngineError(f"duplicate stage name: {name}")
        seen_names.add(name)
        resource = str(stage.get("resource", "host"))
        if resource not in STAGE_RESOURCES:
            raise EngineError(f"unsupported stage resource: {resource}")
        locks = stage.get("locks", [])
        if not isinstance(locks, list) or any(
            not isinstance(item, str) or not item.strip() for item in locks
        ):
            raise EngineError("stage locks must be a list of non-empty strings")
        normalized_locks = sorted(set(item.strip() for item in locks))
        command = stage.get("command", [])
        if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
            raise EngineError(f"stage command must be a string list: {name}")
        env = stage.get("env", {})
        if not isinstance(env, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in env.items()
        ):
            raise EngineError(f"stage env must contain string values: {name}")
        working_dir = normalize_relative_path(str(stage.get("working_dir", ".")))
        max_attempts = int(stage.get("max_attempts", 1) or 1)
        if max_attempts < 1 or max_attempts > 3:
            raise EngineError(f"stage max_attempts must be 1..3: {name}")
        timeout_seconds = int(stage.get("timeout_seconds", 0) or 0)
        if timeout_seconds < 0 or timeout_seconds > 604800:
            raise EngineError(
                f"stage timeout_seconds must be within 0..604800: {name}"
            )
        priority = int(stage.get("priority", 100))
        if priority < 1 or priority > 1000:
            raise EngineError(f"stage priority must be 1..1000: {name}")
        timeout_seconds = int(stage.get("timeout_seconds", 0) or 0)
        if timeout_seconds < 0 or timeout_seconds > 86400:
            raise EngineError(f"stage timeout_seconds must be 0..86400: {name}")
        pre_activation = stage.get("pre_activation", False)
        if not isinstance(pre_activation, bool):
            raise EngineError(f"stage pre_activation must be boolean: {name}")
        if pre_activation and resource != "host":
            raise EngineError(
                f"pre-activation stage must use host resource: {name}"
            )
        if pre_activation and normalized_locks:
            raise EngineError(
                f"pre-activation stage cannot acquire shared locks: {name}"
            )
        host_class = str(
            stage.get(
                "host_concurrency_class",
                "general" if resource == "host" else "none",
            )
        )
        if host_class not in {"none", "general", "cold-build", "cache-hit"}:
            raise EngineError(
                f"unsupported host_concurrency_class for {name}: {host_class}"
            )
        host_cpu_weight = int(
            stage.get("host_cpu_weight", 1 if resource == "host" else 0)
            or 0
        )
        host_memory_mb = int(
            stage.get("host_memory_mb", 1024 if resource == "host" else 0)
            or 0
        )
        host_io_weight = int(
            stage.get("host_io_weight", 1 if resource == "host" else 0)
            or 0
        )
        if min(host_cpu_weight, host_memory_mb, host_io_weight) < 0:
            raise EngineError(f"host resource weights cannot be negative: {name}")
        if resource == "host" and min(
            host_cpu_weight,
            host_memory_mb,
            host_io_weight,
        ) < 1:
            raise EngineError(
                f"host stages require positive resource weights: {name}"
            )
        if resource != "host" and any(
            (host_cpu_weight, host_memory_mb, host_io_weight)
        ):
            raise EngineError(
                f"non-host stage cannot request host resource weights: {name}"
            )
        singleflight_key = str(stage.get("singleflight_key") or "")
        if singleflight_key:
            singleflight_key = normalize_token(
                singleflight_key,
                f"stage[{index}].singleflight_key",
            )
        depends_on_raw = stage.get("depends_on") if "depends_on" in stage else None
        if depends_on_raw is not None and (
            not isinstance(depends_on_raw, list)
            or any(
                not isinstance(item, str) or not item.strip()
                for item in depends_on_raw
            )
        ):
            raise EngineError(f"stage depends_on must be a string list: {name}")
        explicit_dependencies.append(
            [str(item).strip() for item in depends_on_raw]
            if isinstance(depends_on_raw, list)
            else None
        )
        normalized_stages.append(
            {
                "name": name,
                "resource": resource,
                "locks": normalized_locks,
                "max_attempts": max_attempts,
                "timeout_seconds": timeout_seconds,
                "priority": priority,
                "timeout_seconds": timeout_seconds,
                "pre_activation": pre_activation,
                "host_concurrency_class": host_class,
                "host_cpu_weight": host_cpu_weight,
                "host_memory_mb": host_memory_mb,
                "host_io_weight": host_io_weight,
                "singleflight_key": singleflight_key,
                "command": command,
                "env": env,
                "working_dir": working_dir,
            }
        )
    stage_names = [str(stage["name"]) for stage in normalized_stages]
    known_names = set(stage_names)
    for index, stage in enumerate(normalized_stages):
        dependencies = explicit_dependencies[index]
        if dependencies is None:
            dependencies = [stage_names[index - 1]] if index > 0 else []
        dependencies = list(dict.fromkeys(dependencies))
        unknown = [name for name in dependencies if name not in known_names]
        if unknown:
            raise EngineError(
                f"stage depends_on references unknown stage: {stage['name']} -> {unknown[0]}"
            )
        if str(stage["name"]) in dependencies:
            raise EngineError(f"stage cannot depend on itself: {stage['name']}")
        stage["depends_on"] = dependencies
    stages_by_name = {
        str(stage["name"]): stage for stage in normalized_stages
    }
    for stage in normalized_stages:
        if not bool(stage.get("pre_activation")):
            continue
        invalid_dependencies = [
            name
            for name in stage["depends_on"]
            if not bool(stages_by_name[name].get("pre_activation"))
        ]
        if invalid_dependencies:
            raise EngineError(
                "pre-activation stage cannot depend on an activation-gated stage: "
                f"{stage['name']} -> {invalid_dependencies[0]}"
            )
    dependency_map = {
        str(stage["name"]): set(str(item) for item in stage["depends_on"])
        for stage in normalized_stages
    }
    pending = {name: set(dependencies) for name, dependencies in dependency_map.items()}
    resolved: set[str] = set()
    while pending:
        ready_names = sorted(
            name for name, dependencies in pending.items() if dependencies <= resolved
        )
        if not ready_names:
            cycle = ", ".join(sorted(pending))
            raise EngineError(f"stage dependency graph contains a cycle: {cycle}")
        for name in ready_names:
            resolved.add(name)
            pending.pop(name)
    payload["stages"] = normalized_stages
    scheduler_policy = payload.get("scheduler_policy")
    has_preactivation = any(
        bool(stage.get("pre_activation")) for stage in normalized_stages
    )
    if scheduler_policy is None:
        scheduler_policy = {
            "queue_preactivation": "enabled" if has_preactivation else "disabled"
        }
    if not isinstance(scheduler_policy, dict):
        raise EngineError("scheduler_policy must be an object")
    queue_preactivation = str(
        scheduler_policy.get("queue_preactivation") or ""
    )
    if queue_preactivation not in {"enabled", "disabled"}:
        raise EngineError(
            "scheduler_policy.queue_preactivation must be enabled or disabled"
        )
    if has_preactivation != (queue_preactivation == "enabled"):
        raise EngineError(
            "scheduler_policy.queue_preactivation disagrees with stage pre_activation"
        )
    payload["scheduler_policy"] = {
        **scheduler_policy,
        "queue_preactivation": queue_preactivation,
        "measurement_preactivation_overlap": str(
            scheduler_policy.get("measurement_preactivation_overlap") or "disabled"
        ),
        "profile_export_capture_overlap": str(
            scheduler_policy.get("profile_export_capture_overlap") or "disabled"
        ),
        "device_continuation": str(
            scheduler_policy.get("device_continuation") or "disabled"
        ),
    }
    if payload["scheduler_policy"]["measurement_preactivation_overlap"] not in {
        "enabled",
        "disabled",
    }:
        raise EngineError(
            "scheduler_policy.measurement_preactivation_overlap must be enabled or disabled"
        )
    if payload["scheduler_policy"]["profile_export_capture_overlap"] not in {
        "enabled",
        "disabled",
    }:
        raise EngineError(
            "scheduler_policy.profile_export_capture_overlap must be enabled or disabled"
        )
    if payload["scheduler_policy"]["device_continuation"] not in {
        "enabled",
        "disabled",
    }:
        raise EngineError(
            "scheduler_policy.device_continuation must be enabled or disabled"
        )
    for key in ("required_artifacts", "optional_artifacts"):
        values = payload.get(key, [])
        if not isinstance(values, list) or not all(isinstance(item, str) for item in values):
            raise EngineError(f"{key} must be a string list")
        payload[key] = [normalize_relative_path(item) for item in values]
    return payload


def correlation_fields(raw: dict[str, Any]) -> dict[str, str]:
    return {
        "request_id": str(raw["request_id"]),
        "engine_job_id": str(raw["engine_job_id"]),
        "attempt_id": str(raw["attempt_id"]),
        "operator": str(raw["operator"]),
        "test_version": str(raw["test_version"]),
    }


def normalize_token(value: str, label: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "._-" else "_" for char in value)
    cleaned = cleaned.strip("._-")
    if not cleaned:
        raise EngineError(f"missing or invalid {label}")
    return cleaned


def normalize_relative_path(value: str) -> str:
    normalized = value.replace("\\", "/").strip("/") or "."
    if ".." in Path(normalized).parts:
        raise EngineError(f"path cannot contain '..': {value}")
    return normalized


def read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object(path)
    except json.JSONDecodeError:
        raise
    except ValueError as exc:
        raise EngineError(f"JSON document must be an object: {path}") from exc


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_json_file(path, payload)


def document_digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def engine_generation(root: Path) -> str:
    return hashlib.sha256(str(root.resolve()).encode("utf-8")).hexdigest()[:16]


def engine_code_generation() -> str:
    return runtime_generation()


def _canonical_engine_source(path: Path) -> bytes:
    """Keep Engine generations stable across Windows and Linux checkouts."""
    return path.read_bytes().replace(b"\r\n", b"\n")


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        process_query_limited_information = 0x1000
        still_active = 259
        handle = ctypes.windll.kernel32.OpenProcess(
            process_query_limited_information,
            False,
            pid,
        )
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return int(exit_code.value) == still_active
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    if proc_process_state(pid) == "Z":
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def process_identity_alive(pid: int, start_token: str) -> bool:
    if not process_alive(pid):
        return False
    if not start_token:
        # Legacy metadata can still stop gracefully, but it cannot authorize
        # forced termination because PID reuse is ambiguous.
        return True
    return process_start_token(pid) == start_token


def terminate_process_identity(
    pid: int,
    start_token: str,
    *,
    wait_seconds: float,
) -> bool:
    if (
        pid <= 0
        or pid == os.getpid()
        or not start_token
        or not process_identity_alive(pid, start_token)
    ):
        return False
    if os.name == "nt":
        process_terminate = 0x0001
        handle = ctypes.windll.kernel32.OpenProcess(
            process_terminate,
            False,
            pid,
        )
        if not handle:
            return False
        try:
            if not ctypes.windll.kernel32.TerminateProcess(handle, 1):
                return False
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    else:
        try:
            os.kill(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            return not process_identity_alive(pid, start_token)
    deadline = time.monotonic() + max(0.0, wait_seconds)
    while time.monotonic() < deadline:
        if not process_identity_alive(pid, start_token):
            return True
        time.sleep(0.05)
    if os.name != "nt" and process_identity_alive(pid, start_token):
        try:
            os.kill(pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if not process_identity_alive(pid, start_token):
                return True
            time.sleep(0.05)
    return not process_identity_alive(pid, start_token)


def proc_descendant_pids(
    pid: int,
    proc_root: Path = Path("/proc"),
) -> list[int]:
    descendants: list[int] = []
    pending = [pid]
    seen = {pid}
    while pending:
        parent = pending.pop()
        children_path = proc_root / str(parent) / "task" / str(parent) / "children"
        try:
            child_tokens = children_path.read_text(
                encoding="utf-8",
                errors="replace",
            ).split()
        except OSError:
            continue
        for token in child_tokens:
            try:
                child = int(token)
            except ValueError:
                continue
            if child <= 0 or child in seen:
                continue
            seen.add(child)
            descendants.append(child)
            pending.append(child)
    return descendants


def terminate_process_tree_identity(
    pid: int,
    start_token: str,
    *,
    wait_seconds: float,
) -> bool:
    if (
        pid <= 0
        or pid == os.getpid()
        or not start_token
        or not process_identity_alive(pid, start_token)
    ):
        return False
    if os.name == "nt":
        return terminate_process_identity(
            pid,
            start_token,
            wait_seconds=wait_seconds,
        )
    descendants = proc_descendant_pids(pid)
    targets = [*reversed(descendants), pid]
    for target in targets:
        try:
            os.kill(target, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass
    deadline = time.monotonic() + max(0.0, wait_seconds)
    while time.monotonic() < deadline:
        if not process_identity_alive(pid, start_token):
            return True
        time.sleep(0.05)
    for target in targets:
        try:
            os.kill(target, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if not process_identity_alive(pid, start_token):
            return True
        time.sleep(0.05)
    return not process_identity_alive(pid, start_token)


def proc_process_state(pid: int, proc_root: Path = Path("/proc")) -> str:
    try:
        text = (proc_root / str(pid) / "stat").read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return ""
    closing = text.rfind(")")
    if closing < 0:
        return ""
    fields = text[closing + 1 :].strip().split()
    return fields[0] if fields else ""


def proc_process_summary(pid: int, proc_root: Path = Path("/proc")) -> dict[str, Any]:
    if pid <= 0:
        return {"pid": pid, "state": ""}
    process_root = proc_root / str(pid)
    summary: dict[str, Any] = {"pid": pid, "state": proc_process_state(pid, proc_root)}
    try:
        raw = (process_root / "cmdline").read_bytes().replace(b"\0", b" ").strip()
        summary["command"] = raw.decode("utf-8", errors="replace")[:500]
    except OSError:
        summary["command"] = ""
    child_ids: list[int] = []
    try:
        children = process_root / "task" / str(pid) / "children"
        child_ids = [int(value) for value in children.read_text(encoding="ascii").split()[:8]]
    except (OSError, UnicodeError, ValueError):
        child_ids = []
    summary["children"] = [
        {
            "pid": child_pid,
            "state": proc_process_state(child_pid, proc_root),
            "command": proc_command(child_pid, proc_root),
        }
        for child_pid in child_ids
    ]
    return summary


def proc_command(pid: int, proc_root: Path = Path("/proc")) -> str:
    try:
        raw = (proc_root / str(pid) / "cmdline").read_bytes().replace(b"\0", b" ").strip()
    except OSError:
        return ""
    return raw.decode("utf-8", errors="replace")[:500]


def hidden_process_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))


def hidden_process_startup_info() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    info = subprocess.STARTUPINFO()
    info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    info.wShowWindow = 0
    return info


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def timestamp_epoch_seconds(value: str) -> float:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise EngineError(f"invalid engine timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def timestamp_age_seconds(value: str) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())
