from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.core.models import utc_now_iso


ENGINE_PROTOCOL = "engine-v1"
ACTIVE_STATES = {"admitting", "accepted", "running"}
TERMINAL_STATES = {"completed", "failed", "returned", "fallback"}


class EngineAdmissionError(RuntimeError):
    pass


class EngineAdmissionControlled(EngineAdmissionError):
    pass


class EngineAdmissionStore:
    def __init__(self, root: Path, *, endpoint_id: str = "") -> None:
        self.root = root
        self.endpoint_id = str(endpoint_id or "").strip()
        self.state_dir = root / "TestUtils" / "tester_daemon"
        self.path = self.state_dir / "engine_admission_state.json"
        self.events_path = self.state_dir / "engine_admission_events.jsonl"

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "protocol_version": ENGINE_PROTOCOL,
                "endpoint_id": self.endpoint_id,
                "enabled": False,
                "draining": False,
                "target_inflight": 1,
                "jobs": {},
                "updated_at": "",
            }
        raw = json.loads(self.path.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise EngineAdmissionError("engine admission state must be an object")
        raw.setdefault("protocol_version", ENGINE_PROTOCOL)
        observed_endpoint = str(raw.get("endpoint_id") or "")
        if (
            self.endpoint_id
            and observed_endpoint
            and observed_endpoint != self.endpoint_id
        ):
            raise EngineAdmissionError(
                "engine admission endpoint mismatch: "
                f"requested={self.endpoint_id} observed={observed_endpoint}"
            )
        raw.setdefault("endpoint_id", self.endpoint_id)
        raw.setdefault("enabled", False)
        raw.setdefault("draining", False)
        raw.setdefault("target_inflight", 1)
        raw.setdefault("jobs", {})
        raw.setdefault("controller_lease", {})
        if not isinstance(raw["jobs"], dict):
            raise EngineAdmissionError("engine admission jobs must be an object")
        return raw

    def configure(
        self,
        *,
        enabled: bool,
        target_inflight: int,
        draining: bool = False,
        controller_owner: str = "",
        controller_token: str = "",
        lease_seconds: int = 0,
    ) -> dict[str, Any]:
        with self._mutation_lock():
            return self._configure_unlocked(
                enabled=enabled,
                target_inflight=target_inflight,
                draining=draining,
                controller_owner=controller_owner,
                controller_token=controller_token,
                lease_seconds=lease_seconds,
            )

    def _configure_unlocked(
        self,
        *,
        enabled: bool,
        target_inflight: int,
        draining: bool = False,
        controller_owner: str = "",
        controller_token: str = "",
        lease_seconds: int = 0,
    ) -> dict[str, Any]:
        if target_inflight < 1:
            raise EngineAdmissionError("target_inflight must be positive")
        state = self.read()
        if lease_seconds:
            if lease_seconds < 1:
                raise EngineAdmissionError("lease_seconds must be positive")
            owner = token(controller_owner, "controller_owner")
            lease_token = token(controller_token, "controller_token")
            active = self.active_controller_lease(state)
            if active and (
                active.get("owner") != owner or active.get("token") != lease_token
            ):
                raise EngineAdmissionError(
                    f"engine admission is controlled by {active.get('owner', 'unknown')}"
                )
            now = datetime.now(timezone.utc)
            state["controller_lease"] = {
                "owner": owner,
                "token": lease_token,
                "acquired_at": now.isoformat(timespec="seconds"),
                "expires_at": (now + timedelta(seconds=lease_seconds)).isoformat(
                    timespec="seconds"
                ),
            }
        state["enabled"] = bool(enabled)
        state["draining"] = bool(draining and enabled)
        state["target_inflight"] = int(target_inflight)
        state["updated_at"] = utc_now_iso()
        self._write(state)
        self._event(
            "admission_configured",
            enabled=bool(enabled),
            draining=bool(draining and enabled),
            target_inflight=int(target_inflight),
            controller_lease=state.get("controller_lease", {}),
        )
        return self.snapshot()

    def active_controller_lease(
        self,
        state: dict[str, Any] | None = None,
        *,
        now: datetime | None = None,
    ) -> dict[str, str]:
        state = state or self.read()
        raw = state.get("controller_lease", {})
        if not isinstance(raw, dict):
            return {}
        expires_at = parse_time(str(raw.get("expires_at") or ""))
        current = now or datetime.now(timezone.utc)
        if expires_at is None or expires_at <= current:
            return {}
        owner = str(raw.get("owner") or "")
        lease_token = str(raw.get("token") or "")
        if not owner or not lease_token:
            return {}
        return {str(key): str(value) for key, value in raw.items()}

    def release_controller_lease(self, owner: str, lease_token: str) -> dict[str, Any]:
        with self._mutation_lock():
            return self._release_controller_lease_unlocked(owner, lease_token)

    def _release_controller_lease_unlocked(
        self, owner: str, lease_token: str
    ) -> dict[str, Any]:
        state = self.read()
        active = self.active_controller_lease(state)
        normalized_owner = token(owner, "controller_owner")
        normalized_token = token(lease_token, "controller_token")
        if active and (
            active.get("owner") != normalized_owner
            or active.get("token") != normalized_token
        ):
            raise EngineAdmissionError(
                f"engine admission is controlled by {active.get('owner', 'unknown')}"
            )
        released = dict(state.get("controller_lease", {}))
        state["controller_lease"] = {}
        state["updated_at"] = utc_now_iso()
        self._write(state)
        self._event(
            "admission_controller_released",
            owner=normalized_owner,
            controller_token=normalized_token,
            released_lease=released,
        )
        return self.snapshot()

    def begin_admission(self, candidate: dict[str, Any]) -> dict[str, Any]:
        with self._mutation_lock():
            return self._begin_admission_unlocked(candidate)

    def begin_standby(self, candidate: dict[str, Any]) -> dict[str, Any]:
        with self._mutation_lock():
            state = self.read()
            normalized = correlation(candidate)
            job_id = normalized["engine_job_id"]
            existing = state["jobs"].get(job_id)
            if existing:
                if correlation(existing) != normalized:
                    raise EngineAdmissionError(f"engine job id collision: {job_id}")
                if existing.get("state") != "admission-failed":
                    return existing
            if not state.get("enabled"):
                raise EngineAdmissionError("engine admission is disabled")
            if state.get("draining"):
                raise EngineAdmissionError("engine admission is draining")
            controller = self.active_controller_lease(state)
            if controller and bool(candidate.get("workflow_ingest", True)):
                raise EngineAdmissionControlled(
                    f"engine admission is controlled by {controller.get('owner', 'unknown')}"
                )
            if existing:
                return existing
            now = utc_now_iso()
            record = {
                **normalized,
                "protocol_version": ENGINE_PROTOCOL,
                "state": "staging-standby",
                "standby_started_at": now,
                "updated_at": now,
            }
            state["jobs"][job_id] = record
            state["updated_at"] = now
            self._write(state)
            self._event("standby_staging_started", **record)
            return record

    def _begin_admission_unlocked(self, candidate: dict[str, Any]) -> dict[str, Any]:
        state = self.read()
        job_id = token(candidate.get("engine_job_id", ""), "engine_job_id")
        normalized = correlation(candidate)
        existing = state["jobs"].get(job_id)
        if existing:
            if correlation(existing) != normalized:
                raise EngineAdmissionError(f"engine job id collision: {job_id}")
            if existing.get("state") != "admission-failed":
                return existing
        if not state.get("enabled"):
            raise EngineAdmissionError("engine admission is disabled")
        if state.get("draining"):
            raise EngineAdmissionError("engine admission is draining")
        controller = self.active_controller_lease(state)
        if controller and bool(candidate.get("workflow_ingest", True)):
            raise EngineAdmissionControlled(
                f"engine admission is controlled by {controller.get('owner', 'unknown')}"
            )
        if self.local_credit(state) <= 0:
            raise EngineAdmissionError("local engine in-flight target is full")
        if existing:
            existing.update(
                {
                    "state": "admitting",
                    "admission_started_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                    "last_error": "",
                }
            )
            state["updated_at"] = utc_now_iso()
            self._write(state)
            self._event("admission_retried", **normalized)
            return existing
        now = utc_now_iso()
        record = {
            **normalized,
            "protocol_version": ENGINE_PROTOCOL,
            "state": "admitting",
            "admission_started_at": now,
            "updated_at": now,
        }
        state["jobs"][job_id] = record
        state["updated_at"] = now
        self._write(state)
        self._event("admission_started", **record)
        return record

    def record_acceptance(self, receipt: dict[str, Any]) -> dict[str, Any]:
        with self._mutation_lock():
            return self._record_acceptance_unlocked(receipt)

    def _record_acceptance_unlocked(self, receipt: dict[str, Any]) -> dict[str, Any]:
        state = self.read()
        normalized = correlation(receipt)
        job_id = normalized["engine_job_id"]
        record = state["jobs"].get(job_id)
        if record is None:
            if not state.get("enabled") or self.local_credit(state) <= 0:
                raise EngineAdmissionError("acceptance has no local admission credit")
            record = {
                **normalized,
                "admission_started_at": receipt.get("accepted_at", utc_now_iso()),
            }
        elif correlation(record) != normalized:
            raise EngineAdmissionError(f"acceptance correlation mismatch: {job_id}")
        record.update(
            {
                "protocol_version": str(
                    receipt.get("protocol_version") or ENGINE_PROTOCOL
                ),
                "state": "accepted",
                "accepted_at": str(receipt.get("accepted_at") or utc_now_iso()),
                "bundle_hash": str(receipt.get("bundle_hash") or ""),
                "spec_hash": str(receipt.get("spec_hash") or ""),
                "updated_at": utc_now_iso(),
            }
        )
        state["jobs"][job_id] = record
        state["updated_at"] = utc_now_iso()
        self._write(state)
        self._event("admission_accepted", **record)
        return record

    def record_standby(self, receipt: dict[str, Any]) -> dict[str, Any]:
        with self._mutation_lock():
            state = self.read()
            normalized = correlation(receipt)
            job_id = normalized["engine_job_id"]
            record = state["jobs"].get(job_id)
            if record is None:
                record = {**normalized, "standby_started_at": utc_now_iso()}
            elif correlation(record) != normalized:
                raise EngineAdmissionError(f"standby correlation mismatch: {job_id}")
            if record.get("state") in {
                "accepted",
                "running",
                "return-ready",
                "returned",
            }:
                return record
            record.update(
                {
                    "protocol_version": str(
                        receipt.get("protocol_version") or ENGINE_PROTOCOL
                    ),
                    "state": "standby",
                    "staged_at": str(receipt.get("staged_at") or utc_now_iso()),
                    "bundle_hash": str(receipt.get("bundle_hash") or ""),
                    "spec_hash": str(receipt.get("spec_hash") or ""),
                    "updated_at": utc_now_iso(),
                }
            )
            state["jobs"][job_id] = record
            state["updated_at"] = utc_now_iso()
            self._write(state)
            self._event("standby_staged", **record)
            return record

    def record_standby_cancellation(self, result: dict[str, Any]) -> dict[str, Any]:
        with self._mutation_lock():
            state = self.read()
            job_id = token(result.get("engine_job_id", ""), "engine_job_id")
            record = state["jobs"].get(job_id)
            if not isinstance(record, dict):
                raise EngineAdmissionError(
                    f"standby cancellation has no local record: {job_id}"
                )
            outcome = str(result.get("outcome") or "")
            if outcome not in {"cancelled", "conflict"}:
                raise EngineAdmissionError(
                    f"invalid standby cancellation outcome for {job_id}: {outcome}"
                )
            now = utc_now_iso()
            record.update(
                {
                    "standby_cancellation_outcome": outcome,
                    "standby_cancellation_observed_at": now,
                    "standby_cancellation_error": str(result.get("error") or ""),
                    "updated_at": now,
                }
            )
            if outcome == "cancelled":
                record.update(
                    {
                        "state": "standby-cancelled",
                        "cancelled_at": str(result.get("cancelled_at") or now),
                        "cancel_reason": str(result.get("reason") or ""),
                    }
                )
            state["jobs"][job_id] = record
            state["updated_at"] = now
            self._write(state)
            self._event("standby_cancellation_recorded", **record)
            return record

    def record_admission_failure(
        self, engine_job_id: str, error: str
    ) -> dict[str, Any]:
        with self._mutation_lock():
            return self._record_admission_failure_unlocked(engine_job_id, error)

    def _record_admission_failure_unlocked(
        self, engine_job_id: str, error: str
    ) -> dict[str, Any]:
        state = self.read()
        job_id = token(engine_job_id, "engine_job_id")
        record = state["jobs"].get(job_id)
        if record is None:
            raise EngineAdmissionError(f"unknown engine admission: {job_id}")
        if record.get("state") in {"accepted", "running", "return-ready", "returned"}:
            return record
        record.update(
            {
                "state": "admission-failed",
                "last_error": str(error),
                "failed_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        state["updated_at"] = utc_now_iso()
        self._write(state)
        self._event("admission_failed", **correlation(record), error=str(error))
        return record

    def reconcile_engine_snapshot(
        self, engine_snapshot: dict[str, Any]
    ) -> dict[str, Any]:
        with self._mutation_lock():
            return self._reconcile_engine_snapshot_unlocked(engine_snapshot)

    def _reconcile_engine_snapshot_unlocked(
        self, engine_snapshot: dict[str, Any]
    ) -> dict[str, Any]:
        state = self.read()
        for remote in engine_snapshot.get("jobs", []):
            if not isinstance(remote, dict):
                continue
            try:
                normalized = correlation(remote)
            except EngineAdmissionError:
                continue
            job_id = normalized["engine_job_id"]
            local = state["jobs"].get(job_id)
            if local is None or correlation(local) != normalized:
                continue
            remote_state = str(remote.get("state") or "")
            stage_state = str(remote.get("stage_state") or "")
            local_state = str(local.get("state") or "")
            if remote_state in {"completed", "failed"}:
                if local_state not in {"returned", "fallback"}:
                    local["state"] = "return-ready"
                    local["engine_terminal_state"] = remote_state
                    local["terminal_at"] = str(remote.get("terminal_at") or "")
                    local.setdefault("terminal_observed_at", utc_now_iso())
            elif remote_state == "running" or stage_state == "running":
                if local_state in {
                    "admitting",
                    "staging-standby",
                    "standby",
                    "accepted",
                    "running",
                }:
                    local["state"] = "running"
            elif remote_state == "accepted":
                if local_state in {"admitting", "accepted"}:
                    local["state"] = "accepted"
            elif remote_state == "standby":
                if local_state in {"staging-standby", "standby"}:
                    local["state"] = "standby"
            local["stage_name"] = str(remote.get("stage_name") or "")
            local["stage_resource"] = str(remote.get("stage_resource") or "")
            local["stage_locks"] = [
                str(item) for item in remote.get("stage_locks", []) if str(item)
            ]
            local["stage_index"] = int(remote.get("stage_index", 0) or 0)
            local["stage_started_at"] = str(remote.get("stage_started_at") or "")
            local["running_stage_details"] = [
                dict(item)
                for item in remote.get("running_stage_details", [])
                if isinstance(item, dict)
            ]
            local["running_stage_count"] = len(local["running_stage_details"])
            local["completed_stage_indices"] = [
                int(item)
                for item in remote.get("completed_stage_indices", [])
                if isinstance(item, int) or str(item).isdigit()
            ]
            local["failure_pending"] = str(remote.get("failure_pending") or "")
            for field in (
                "staged_at",
                "accepted_at",
                "promoted_at",
                "promotion_source",
                "activated_at",
            ):
                value = remote.get(field)
                if value:
                    local[field] = value
            local.update(execution_projection(local))
            local["updated_at"] = utc_now_iso()
        incoming_view = capacity_view(engine_snapshot)
        current_view = state.get("last_engine_snapshot")
        current_view = current_view if isinstance(current_view, dict) else {}
        incoming_observed = parse_time(str(incoming_view.get("observed_at") or ""))
        current_observed = parse_time(str(current_view.get("observed_at") or ""))
        snapshot_regressed = bool(
            current_observed is not None
            and (
                incoming_observed is None
                or incoming_observed < current_observed
            )
        )
        if not snapshot_regressed:
            state["last_engine_snapshot"] = incoming_view
        state["updated_at"] = utc_now_iso()
        self._write(state)
        if snapshot_regressed:
            self._event(
                "engine_snapshot_regression_ignored",
                incoming_observed_at=str(incoming_view.get("observed_at") or ""),
                incoming_transport_received_at=str(
                    incoming_view.get("transport_received_at") or ""
                ),
                current_observed_at=str(current_view.get("observed_at") or ""),
                current_transport_received_at=str(
                    current_view.get("transport_received_at") or ""
                ),
            )
        return self.snapshot(engine_snapshot=engine_snapshot)

    def record_terminal_manifest(self, manifest: dict[str, Any]) -> dict[str, Any]:
        with self._mutation_lock():
            return self._record_terminal_manifest_unlocked(manifest)

    def _record_terminal_manifest_unlocked(
        self, manifest: dict[str, Any]
    ) -> dict[str, Any]:
        state = self.read()
        normalized = correlation(manifest)
        job_id = normalized["engine_job_id"]
        record = state["jobs"].get(job_id)
        if record is None or correlation(record) != normalized:
            raise EngineAdmissionError(
                f"terminal manifest has no matching admission: {job_id}"
            )
        if record.get("state") in {"returned", "fallback"}:
            return record
        terminal_state = str(manifest.get("state") or "")
        if terminal_state not in {"completed", "failed"}:
            raise EngineAdmissionError(
                f"invalid engine terminal state: {terminal_state}"
            )
        record.update(
            {
                "state": "return-ready",
                "engine_terminal_state": terminal_state,
                "terminal_at": str(manifest.get("terminal_at") or utc_now_iso()),
                "terminal_observed_at": str(
                    record.get("terminal_observed_at") or utc_now_iso()
                ),
                "terminal_manifest": dict(manifest),
                "updated_at": utc_now_iso(),
            }
        )
        state["updated_at"] = utc_now_iso()
        self._write(state)
        self._event(
            "engine_terminal_observed", **normalized, terminal_state=terminal_state
        )
        return record

    def record_return(self, engine_job_id: str, receipt_id: str) -> dict[str, Any]:
        with self._mutation_lock():
            return self._record_return_unlocked(engine_job_id, receipt_id)

    def _record_return_unlocked(
        self, engine_job_id: str, receipt_id: str
    ) -> dict[str, Any]:
        state = self.read()
        job_id = token(engine_job_id, "engine_job_id")
        record = state["jobs"].get(job_id)
        if record is None:
            raise EngineAdmissionError(f"unknown engine job: {job_id}")
        if record.get("state") == "returned":
            return record
        if record.get("state") != "return-ready":
            raise EngineAdmissionError(f"engine job is not return-ready: {job_id}")
        record.update(
            {
                "state": "returned",
                "return_receipt_id": token(receipt_id, "receipt_id"),
                "returned_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        state["updated_at"] = utc_now_iso()
        self._write(state)
        self._event(
            "engine_return_recorded", **correlation(record), receipt_id=receipt_id
        )
        return record

    def effective_credit(
        self,
        engine_snapshot: dict[str, Any] | None,
        *,
        max_snapshot_age_seconds: int = 10,
        now: datetime | None = None,
    ) -> int:
        state = self.read()
        if not state.get("enabled") or state.get("draining") or engine_snapshot is None:
            return 0
        if snapshot_stale(engine_snapshot, max_snapshot_age_seconds, now=now):
            return 0
        if bool(engine_snapshot.get("capacity", {}).get("draining")):
            return 0
        remote_credit = max(0, int(engine_snapshot.get("admission_credit", 0) or 0))
        return min(self.local_credit(state), remote_credit)

    def local_credit(self, state: dict[str, Any] | None = None) -> int:
        state = state or self.read()
        active = sum(
            1
            for record in state["jobs"].values()
            if isinstance(record, dict) and record.get("state") in ACTIVE_STATES
        )
        return max(0, int(state.get("target_inflight", 1) or 1) - active)

    def snapshot(
        self, *, engine_snapshot: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        state = self.read()
        counts: dict[str, int] = {}
        execution_counts: dict[str, int] = {}
        running_stage_counts_by_resource: dict[str, int] = {}
        active_slot_job_count = 0
        preactivation_only_job_count = 0
        running_stage_count = 0
        for record in state["jobs"].values():
            if not isinstance(record, dict):
                continue
            name = str(record.get("state") or "unknown")
            counts[name] = counts.get(name, 0) + 1
            projection = execution_projection(record)
            execution_name = str(projection["execution_state"])
            execution_counts[execution_name] = (
                execution_counts.get(execution_name, 0) + 1
            )
            active_slot_job_count += int(bool(projection["active_slot_occupied"]))
            preactivation_only_job_count += int(bool(projection["preactivation_only"]))
            details = [
                item
                for item in record.get("running_stage_details", [])
                if isinstance(item, dict)
            ]
            running_stage_count += len(details)
            for detail in details:
                resource = str(detail.get("stage_resource") or "unknown")
                running_stage_counts_by_resource[resource] = (
                    running_stage_counts_by_resource.get(resource, 0) + 1
                )
        return {
            **state,
            "controller_lease_active": bool(self.active_controller_lease(state)),
            "local_credit": self.local_credit(state),
            "effective_credit": (
                self.effective_credit(engine_snapshot)
                if engine_snapshot is not None
                else 0
            ),
            "state_counts": counts,
            "execution_state_counts": execution_counts,
            "active_slot_job_count": active_slot_job_count,
            "preactivation_only_job_count": preactivation_only_job_count,
            "running_stage_count": running_stage_count,
            "running_stage_counts_by_resource": running_stage_counts_by_resource,
        }

    def _write(self, payload: dict[str, Any]) -> None:
        observed_endpoint = str(payload.get("endpoint_id") or "")
        if (
            self.endpoint_id
            and observed_endpoint
            and observed_endpoint != self.endpoint_id
        ):
            raise EngineAdmissionError(
                "engine admission endpoint mismatch before write: "
                f"requested={self.endpoint_id} observed={observed_endpoint}"
            )
        if self.endpoint_id:
            payload["endpoint_id"] = self.endpoint_id
        self.state_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.path, payload)

    def _mutation_lock(self) -> NamedProcessLock:
        return NamedProcessLock(
            self.root,
            (
                f"engine_admission_mutation_{self.endpoint_id}"
                if self.endpoint_id
                else "engine_admission_mutation"
            ),
            stale_after_seconds=30,
            wait_timeout_seconds=5,
        )

    def _event(self, kind: str, **fields: Any) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "time": utc_now_iso(),
            "kind": kind,
            "endpoint_id": self.endpoint_id,
            **fields,
        }
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def correlation(raw: dict[str, Any]) -> dict[str, str]:
    return {
        "request_id": token(str(raw.get("request_id") or ""), "request_id"),
        "engine_job_id": token(str(raw.get("engine_job_id") or ""), "engine_job_id"),
        "attempt_id": token(str(raw.get("attempt_id") or ""), "attempt_id"),
        "operator": token(str(raw.get("operator") or raw.get("op") or ""), "operator"),
        "test_version": token(str(raw.get("test_version") or ""), "test_version"),
    }


def execution_projection(record: dict[str, Any]) -> dict[str, Any]:
    state = str(record.get("state") or "unknown")
    details = [
        item
        for item in record.get("running_stage_details", [])
        if isinstance(item, dict)
    ]
    active_slot_occupied = bool(record.get("activated_at")) and state in ACTIVE_STATES
    preactivation_running_stage_count = sum(
        bool(item.get("pre_activation")) for item in details
    )
    preactivation_only = (
        state == "running"
        and not active_slot_occupied
        and bool(details)
        and preactivation_running_stage_count == len(details)
    )
    if preactivation_only:
        execution_state = "preactivating"
    elif state == "running" and active_slot_occupied:
        execution_state = "active-running"
    elif state == "running":
        execution_state = "running-unresolved"
    elif state == "accepted" and active_slot_occupied:
        execution_state = "active-idle"
    elif state == "accepted":
        execution_state = "queued"
    else:
        execution_state = state
    return {
        "execution_state": execution_state,
        "active_slot_occupied": active_slot_occupied,
        "preactivation_only": preactivation_only,
        "preactivation_running_stage_count": preactivation_running_stage_count,
    }


def token(value: str, label: str) -> str:
    cleaned = "".join(
        char if char.isalnum() or char in "._-" else "_" for char in value
    )
    cleaned = cleaned.strip("._-")
    if not cleaned:
        raise EngineAdmissionError(f"missing or invalid {label}")
    return cleaned


def capacity_view(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "observed_at": str(snapshot.get("observed_at") or ""),
        "transport_received_at": str(snapshot.get("transport_received_at") or ""),
        "return_export_protocol": str(snapshot.get("return_export_protocol") or ""),
        "engine_generation": str(snapshot.get("engine_generation") or ""),
        "engine_code_generation": str(snapshot.get("engine_code_generation") or ""),
        "admission_credit": int(snapshot.get("admission_credit", 0) or 0),
        "admission_credit_before_backpressure": int(
            snapshot.get("admission_credit_before_backpressure", 0) or 0
        ),
        "accepted_nonterminal": int(snapshot.get("accepted_nonterminal", 0) or 0),
        "standby_count": int(snapshot.get("standby_count", 0) or 0),
        "standby_credit": int(snapshot.get("standby_credit", 0) or 0),
        "active_nonterminal": int(snapshot.get("active_nonterminal", 0) or 0),
        "queued_nonterminal": int(snapshot.get("queued_nonterminal", 0) or 0),
        "active_job_slots_free": int(snapshot.get("active_job_slots_free", 0) or 0),
        "return_ready_count": int(snapshot.get("return_ready_count", 0) or 0),
        "return_backlog_bytes": int(snapshot.get("return_backlog_bytes", 0) or 0),
        "required_return_backlog_bytes": int(
            snapshot.get("required_return_backlog_bytes", 0) or 0
        ),
        "return_backlog_pressure_ratio": float(
            snapshot.get("return_backlog_pressure_ratio", 0.0) or 0.0
        ),
        "return_backpressure_active": bool(
            snapshot.get("return_backpressure_active", False)
        ),
        "return_backpressure_reason": str(
            snapshot.get("return_backpressure_reason") or ""
        ),
        "return_backlog_limits": dict(snapshot.get("return_backlog_limits", {})),
        "running_by_resource": dict(snapshot.get("running_by_resource", {})),
        "running_locks": list(snapshot.get("running_locks", [])),
        "shared_resource_leases": list(snapshot.get("shared_resource_leases", [])),
        "host_slots_free": int(snapshot.get("host_slots_free", 0) or 0),
        "device_slots_free": int(snapshot.get("device_slots_free", 0) or 0),
        "export_slots_free": int(snapshot.get("export_slots_free", 0) or 0),
        "capacity": dict(snapshot.get("capacity", {})),
    }


def snapshot_stale(
    snapshot: dict[str, Any],
    max_age_seconds: int,
    *,
    now: datetime | None = None,
) -> bool:
    observed = parse_time(
        str(snapshot.get("transport_received_at") or snapshot.get("observed_at") or "")
    )
    if observed is None:
        return True
    current = now or datetime.now(timezone.utc)
    return (current - observed).total_seconds() > max(0, max_age_seconds)


def parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def atomic_write_json(
    path: Path,
    payload: dict[str, Any],
    *,
    replace_attempts: int = 40,
    retry_seconds: float = 0.025,
) -> None:
    temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    try:
        for attempt in range(max(1, replace_attempts)):
            try:
                os.replace(temp, path)
                return
            except PermissionError:
                if attempt + 1 >= max(1, replace_attempts):
                    raise
                time.sleep(max(0.0, retry_seconds))
    finally:
        temp.unlink(missing_ok=True)
