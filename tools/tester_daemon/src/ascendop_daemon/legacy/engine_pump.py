from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from ascendop_daemon.legacy.engine_admission import (
    EngineAdmissionControlled,
    EngineAdmissionStore,
    correlation,
    snapshot_stale,
)
from ascendop_daemon.legacy.engine_transport import EngineReturnAlreadyCompactedError
from ascendop_daemon.runtime.locking import process_alive
from ascendop_daemon.core.models import utc_now_iso


RECOVERY_SNAPSHOT_MIN_TIMEOUT_SECONDS = 45
REMOTE_CREDIT_SNAPSHOT_MAX_AGE_SECONDS = 10
RUNTIME_SNAPSHOT_MAX_AGE_SECONDS = 60
RUNTIME_SYNC_RETRY_BASE_SECONDS = 15
RUNTIME_SYNC_RETRY_MAX_SECONDS = 300
RUNTIME_CONTROL_WAIT_SECONDS = 10
RUNTIME_SYNC_SCHEMA = "ascendop.engine-runtime-sync.v3"
RETRYABLE_REMOTE_ADMISSION_MARKERS = (
    "no fresh admission credit",
    "no fresh standby credit",
)
RUNTIME_GENERATION_ADMISSION_MARKERS = (
    '".gitpartner_payload_archive.json"',
    ".gitpartner_payload_chunks/",
)
OPTIONAL_MATERIALIZATION_TERMINAL_STATES = {
    "materialized",
    "no-materializable-paths",
    "retained-in-engine-cache",
}
LOGICAL_JOB_ACTIVE_STATES = {
    "pending",
    "admitting",
    "staging-standby",
    "standby",
    "standby-cancel-requested",
    "accepted",
    "running",
    "return-ready",
    "returned",
    "returned-awaiting-ingest",
}
LOGICAL_JOB_SUPERSEDED_STATE = "superseded-by-logical-attempt"
ADMISSION_STATE_ORDER = {
    "pending": 0,
    "admitting": 1,
    "staging-standby": 1,
    "standby": 2,
    "standby-cancel-requested": 2,
    "accepted": 3,
    "running": 4,
    "return-ready": 5,
    "returned": 6,
    "returned-awaiting-ingest": 7,
    "workflow-archived": 8,
    "superseded-by-workflow-result": 8,
    "canary-complete": 8,
    LOGICAL_JOB_SUPERSEDED_STATE: 8,
}


class EnginePumpError(RuntimeError):
    pass


def admission_receipt_can_advance(current_state: str, target_state: str) -> bool:
    current_rank = ADMISSION_STATE_ORDER.get(str(current_state or ""))
    target_rank = ADMISSION_STATE_ORDER.get(str(target_state or ""))
    if current_rank is None or target_rank is None:
        return False
    return target_rank >= current_rank


def capacity_value_matches(observed: Any, desired: Any) -> bool:
    if isinstance(desired, list):
        def normalize_inventory(value: Any) -> dict[str, dict[str, Any]]:
            if not isinstance(value, list):
                return {}
            return {
                str(item.get("device_id") or ""): {
                    "device_id": str(item.get("device_id") or ""),
                    "enabled": bool(item.get("enabled", True)),
                    "draining": bool(item.get("draining", False)),
                }
                for item in value
                if isinstance(item, dict) and str(item.get("device_id") or "")
            }

        observed_inventory = normalize_inventory(observed)
        desired_inventory = normalize_inventory(desired)
        if not desired_inventory:
            return not any(
                item["enabled"] for item in observed_inventory.values()
            )
        if any(
            observed_inventory.get(device_id) != expected
            for device_id, expected in desired_inventory.items()
        ):
            return False
        # A node may report all physical devices while the route registers only
        # the enabled subset.  Extra disabled devices are inventory, not usable
        # capacity; an extra enabled device still requires convergence.
        return not any(
            item["enabled"]
            for device_id, item in observed_inventory.items()
            if device_id not in desired_inventory
        )
    if isinstance(desired, dict):
        return json.dumps(observed, sort_keys=True) == json.dumps(
            desired, sort_keys=True
        )
    try:
        return int(observed) == int(desired)
    except (TypeError, ValueError):
        return observed == desired


def retryable_remote_admission_rejection(reason: str) -> bool:
    normalized = str(reason or "").strip().lower()
    return any(marker in normalized for marker in RETRYABLE_REMOTE_ADMISSION_MARKERS)


def runtime_generation_admission_rejection(reason: str) -> bool:
    normalized = str(reason or "").strip().lower()
    return any(marker in normalized for marker in RUNTIME_GENERATION_ADMISSION_MARKERS)


def runtime_sync_blocking_exchange(exchange: dict[str, Any]) -> bool:
    if str(exchange.get("state") or "") not in {
        "publishing",
        "published",
        "uncertain",
    }:
        return False
    jobs = exchange.get("jobs")
    return str(exchange.get("exchange_kind") or "") == "admission" or (
        isinstance(jobs, list) and bool(jobs)
    )


def definitely_local_exchange_publish_failure(error: str) -> bool:
    normalized = str(error or "").strip().lower()
    return any(
        marker in normalized
        for marker in (
            "[winerror 2]",
            "[winerror 3]",
            "local payload copy failed",
            "source payload does not exist",
        )
    )


def logical_job_key_from_spec(spec: dict[str, Any]) -> str:
    workflow = spec.get("workflow")
    workflow = workflow if isinstance(workflow, dict) else {}
    identity = spec.get("input_identity")
    identity = identity if isinstance(identity, dict) else {}
    job_kind = str(
        workflow.get("job_kind")
        or identity.get("job_kind")
        or "operator-test"
    )
    operator = str(spec.get("operator") or "")
    if not operator:
        return ""
    if job_kind == "case-cache-prewarm":
        requirement = str(
            identity.get("case_cache_requirement_sha256") or ""
        )
        return (
            f"{job_kind}|{operator}|{requirement}"
            if requirement
            else ""
        )
    if job_kind == "profiler-evidence":
        request = str(identity.get("profiler_request_sha256") or "")
        source = str(
            identity.get("profiler_target_source_sha256")
            or identity.get("target_source_sha256")
            or ""
        )
        return (
            f"{job_kind}|{operator}|{request}|{source}"
            if request and source
            else ""
        )
    test_version = str(spec.get("test_version") or "")
    return (
        f"operator-test|{operator}|{test_version}"
        if test_version
        else ""
    )


class EngineTransport(Protocol):
    def exchange(
        self,
        jobs: list[dict[str, Any]],
        *,
        request_id: str,
        acknowledgements: list[dict[str, str]] | None = None,
        required_acknowledgements: list[dict[str, str]] | None = None,
        standby_cancellations: list[dict[str, str]] | None = None,
        max_inflight: int,
        draining: bool,
        standby_slots: int | None = None,
        active_job_slots: int | None = None,
        host_slots: int | None = None,
        export_slots: int | None = None,
        wait_timeout_seconds: int = 180,
        **capacity: Any,
    ) -> dict[str, Any]: ...

    def snapshot(
        self,
        *,
        request_id: str,
        acknowledgements: list[dict[str, str]] | None = None,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]: ...

    def accept(
        self,
        spec_path: Path,
        *,
        request_id: str,
        engine_job_id: str,
        payload_root: Path | None = None,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]: ...

    def collect(
        self,
        *,
        request_id: str,
        engine_job_id: str,
        receipt_id: str,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]: ...

    def acknowledge(
        self,
        *,
        request_id: str,
        engine_job_id: str,
        receipt_id: str,
        wait: bool = False,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]: ...

    def sync_code(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 900,
    ) -> dict[str, Any]: ...


class EngineResultIngestor(Protocol):
    def ingest(self, record: dict[str, Any]) -> dict[str, Any]: ...


class EnginePump:
    def __init__(
        self,
        root: Path,
        transport: EngineTransport | None = None,
        result_ingestor: EngineResultIngestor | None = None,
        capacity_overrides: dict[str, Any] | None = None,
        operator_scope: set[str] | None = None,
        expected_remote_generation: str = "",
        endpoint_id: str = "",
    ) -> None:
        self.root = root.resolve()
        self.endpoint_id = str(endpoint_id or "").strip()
        self.transport = transport
        self.state_dir = self.root / "TestUtils" / "tester_daemon"
        self.bundle_dir = self.state_dir / "engine_bundles"
        self.state_path = self.state_dir / "engine_pump_state.json"
        self.events_path = self.state_dir / "engine_pump_events.jsonl"
        self.lock_path = self.state_dir / "engine_pump.lock"
        self.enqueue_dir = self.state_dir / "engine_enqueue_requests"
        self.enqueue_lock_path = self.state_dir / "engine_enqueue.lock"
        self.admission = EngineAdmissionStore(
            self.root,
            endpoint_id=self.endpoint_id,
        )
        self.result_ingestor = result_ingestor
        self.capacity_overrides = {
            str(key): json.loads(json.dumps(value))
            for key, value in (capacity_overrides or {}).items()
            if value is not None
        }
        self.operator_scope = (
            {str(op) for op in operator_scope if str(op)}
            if operator_scope is not None
            else None
        )
        self.expected_remote_generation = str(expected_remote_generation or "")

    def _record_in_scope(self, record: dict[str, Any]) -> bool:
        if not bool(record.get("workflow_ingest", True)):
            return True
        if self.operator_scope is None:
            return True
        return str(record.get("operator") or "") in self.operator_scope

    def _runtime_generation_view(
        self,
        admission: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        admission = admission if admission is not None else self.admission.read()
        snapshot = admission.get("last_engine_snapshot")
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        expected = self.expected_remote_generation
        observed = str(snapshot.get("engine_code_generation") or "")
        enabled = bool(expected)
        return {
            "enabled": enabled,
            "state": (
                "ready"
                if not enabled or observed == expected
                else "hold"
            ),
            "reason": (
                ""
                if not enabled or observed == expected
                else "remote-engine-runtime-generation-unobserved"
                if not observed
                else "remote-engine-runtime-generation-mismatch"
            ),
            "expected_remote_generation": expected,
            "observed_remote_generation": observed,
            "snapshot_observed_at": str(snapshot.get("observed_at") or ""),
            "snapshot_fresh": bool(
                snapshot
                and not snapshot_stale(
                    snapshot,
                    RUNTIME_SNAPSHOT_MAX_AGE_SECONDS,
                )
            ),
            "remote_quiescent": self._runtime_snapshot_quiescent(snapshot),
        }

    @staticmethod
    def _runtime_snapshot_quiescent(snapshot: dict[str, Any]) -> bool:
        if not snapshot or not str(snapshot.get("observed_at") or ""):
            return False
        return all(
            int(snapshot.get(field, 0) or 0) == 0
            for field in (
                "accepted_nonterminal",
                "queued_nonterminal",
                "active_nonterminal",
                "standby_count",
                "return_ready_count",
            )
        )

    def _patch_runtime_sync(self, **fields: Any) -> dict[str, Any]:
        state = self.read()
        current = state.get("runtime_sync")
        current = dict(current) if isinstance(current, dict) else {}
        if (
            str(current.get("schema") or "") != RUNTIME_SYNC_SCHEMA
            or
            str(current.get("expected_remote_generation") or "")
            != self.expected_remote_generation
        ):
            current = {
                "schema": RUNTIME_SYNC_SCHEMA,
                "state": "unobserved",
                "attempts": 0,
                "snapshot_attempts": 0,
                "verify_attempts": 0,
                "expected_remote_generation": self.expected_remote_generation,
                "observed_remote_generation": "",
                "last_error": "",
                "next_retry_at": "",
            }
        current.update(fields)
        current["schema"] = RUNTIME_SYNC_SCHEMA
        current["expected_remote_generation"] = self.expected_remote_generation
        current["updated_at"] = utc_now_iso()
        state["runtime_sync"] = current
        state["updated_at"] = current["updated_at"]
        self._write(state)
        return current

    def _runtime_sync_retry_deferred(self) -> bool:
        state = self.read().get("runtime_sync")
        if not isinstance(state, dict):
            return False
        if (
            str(state.get("expected_remote_generation") or "")
            != self.expected_remote_generation
        ):
            return False
        retry_at = parse_datetime(str(state.get("next_retry_at") or ""))
        return bool(
            retry_at is not None
            and datetime.now().astimezone() < retry_at
        )

    def _record_runtime_sync_failure(
        self,
        *,
        operation: str,
        error: Exception | str,
        attempt: int,
        request_id: str,
        observed_generation: str,
    ) -> dict[str, Any]:
        current = self.read().get("runtime_sync")
        current = current if isinstance(current, dict) else {}
        observation_operation = operation in {"snapshot", "verify"}
        counter_field = (
            "snapshot_attempts"
            if operation == "snapshot"
            else "verify_attempts"
            if operation == "verify"
            else "attempts"
        )
        failure_attempt = (
            int(current.get(counter_field, 0) or 0) + 1
            if observation_operation
            else attempt
        )
        delay = min(
            (
                min(60, RUNTIME_SYNC_RETRY_MAX_SECONDS)
                if observation_operation
                else RUNTIME_SYNC_RETRY_MAX_SECONDS
            ),
            RUNTIME_SYNC_RETRY_BASE_SECONDS
            * (2 ** max(0, failure_attempt - 1)),
        )
        next_retry_at = (
            datetime.now().astimezone() + timedelta(seconds=delay)
        ).isoformat()
        request_field = {
            (
                "snapshot_request_id"
                if operation == "snapshot"
                else "verify_snapshot_request_id"
                if operation == "verify"
                else "request_id"
            ): request_id,
            counter_field: failure_attempt,
        }
        state = self._patch_runtime_sync(
            state="failed",
            reason=f"remote-engine-runtime-{operation}-failed",
            observed_remote_generation=observed_generation,
            last_error=str(error),
            failed_at=utc_now_iso(),
            next_retry_at=next_retry_at,
            retry_delay_seconds=delay,
            **request_field,
        )
        self._event(
            "engine_runtime_sync_failed",
            operation=operation,
            request_id=request_id,
            expected_remote_generation=self.expected_remote_generation,
            observed_remote_generation=observed_generation,
            attempt=failure_attempt,
            error=str(error),
            next_retry_at=next_retry_at,
        )
        return state

    def _refresh_runtime_snapshot(
        self,
        *,
        wait_timeout_seconds: int,
        purpose: str,
        request_id: str = "",
    ) -> dict[str, Any]:
        request_id = request_id or (
            f"engine-runtime-{purpose}-"
            f"{self.expected_remote_generation[:16]}-{cycle_token()}"
        )
        started_at = utc_now_iso()
        remote = self.transport.snapshot(
            request_id=request_id,
            wait_timeout_seconds=min(
                RUNTIME_CONTROL_WAIT_SECONDS,
                max(1, wait_timeout_seconds),
            ),
        )
        snapshot = remote.get("engine_snapshot", {})
        if not isinstance(snapshot, dict) or not snapshot:
            raise EnginePumpError(
                "Engine runtime generation snapshot returned no engine_snapshot"
            )
        self.admission.reconcile_engine_snapshot(snapshot)
        self._event(
            "engine_runtime_snapshot_observed",
            purpose=purpose,
            request_id=request_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            expected_remote_generation=self.expected_remote_generation,
            observed_remote_generation=str(
                snapshot.get("engine_code_generation") or ""
            ),
            remote_quiescent=self._runtime_snapshot_quiescent(snapshot),
        )
        return self.admission.read()

    def _ensure_remote_runtime_generation(
        self,
        *,
        admission: dict[str, Any],
        active_exchanges: list[dict[str, Any]],
        wait_timeout_seconds: int,
        allow_sync: bool = True,
    ) -> dict[str, Any]:
        view = self._runtime_generation_view(admission)
        if not view["enabled"]:
            return view
        runtime_sync = self.read().get("runtime_sync")
        runtime_sync = runtime_sync if isinstance(runtime_sync, dict) else {}
        if (
            str(runtime_sync.get("schema") or "") != RUNTIME_SYNC_SCHEMA
            or str(runtime_sync.get("expected_remote_generation") or "")
            != self.expected_remote_generation
        ):
            # Migrate before honoring cooldowns or deriving request ids. Older
            # state mixed snapshot visibility failures into sync attempts.
            runtime_sync = self._patch_runtime_sync()
        if view["state"] == "ready":
            sync = runtime_sync
            if (
                not isinstance(sync, dict)
                or str(sync.get("state") or "") != "confirmed"
                or str(sync.get("expected_remote_generation") or "")
                != self.expected_remote_generation
            ):
                self._patch_runtime_sync(
                    state="confirmed",
                    reason="",
                    observed_remote_generation=view[
                        "observed_remote_generation"
                    ],
                    confirmed_at=utc_now_iso(),
                    last_error="",
                    next_retry_at="",
                )
            return view

        if active_exchanges:
            view["reason"] = "remote-engine-runtime-sync-waits-for-exchange"
            self._patch_runtime_sync(
                state="hold",
                reason=view["reason"],
                observed_remote_generation=view[
                    "observed_remote_generation"
                ],
                active_exchange_ids=[
                    str(item.get("request_id") or "")
                    for item in active_exchanges
                ],
            )
            return view
        if not allow_sync:
            view["reason"] = "remote-engine-runtime-sync-disabled-by-stop"
            self._patch_runtime_sync(
                state="hold",
                reason=view["reason"],
                observed_remote_generation=view[
                    "observed_remote_generation"
                ],
            )
            return view
        if self._runtime_sync_retry_deferred():
            view["reason"] = "remote-engine-runtime-sync-retry-cooldown"
            return view

        sync_state = self.read().get("runtime_sync")
        sync_state = sync_state if isinstance(sync_state, dict) else {}
        verify_request_id = str(
            sync_state.get("verify_snapshot_request_id") or ""
        )
        verify_pending = bool(
            verify_request_id
            and (
                str(sync_state.get("state") or "") == "verifying"
                or str(sync_state.get("reason") or "")
                == "remote-engine-runtime-verify-failed"
            )
        )
        if verify_pending:
            try:
                admission = self._refresh_runtime_snapshot(
                    wait_timeout_seconds=wait_timeout_seconds,
                    purpose="verify",
                    request_id=verify_request_id,
                )
            except Exception as exc:
                self._record_runtime_sync_failure(
                    operation="verify",
                    error=exc,
                    attempt=int(sync_state.get("attempts", 0) or 0),
                    request_id=verify_request_id,
                    observed_generation=view[
                        "observed_remote_generation"
                    ],
                )
                view["reason"] = "remote-engine-runtime-verify-failed"
                view["error"] = str(exc)
                return view
            view = self._runtime_generation_view(admission)
            if view["state"] == "ready":
                self._patch_runtime_sync(
                    state="confirmed",
                    reason="",
                    observed_remote_generation=view[
                        "observed_remote_generation"
                    ],
                    confirmed_at=utc_now_iso(),
                    last_error="",
                    next_retry_at="",
                )
                return view
            attempt = max(1, int(sync_state.get("attempts", 0) or 0))
            error = EnginePumpError(
                "Engine runtime sync did not converge after verification: "
                f"expected={self.expected_remote_generation} "
                f"observed={view['observed_remote_generation']}"
            )
            self._patch_runtime_sync(
                verify_snapshot_request_id="",
                verify_attempts=0,
            )
            self._record_runtime_sync_failure(
                operation="sync",
                error=error,
                attempt=attempt,
                request_id=str(sync_state.get("request_id") or ""),
                observed_generation=view["observed_remote_generation"],
            )
            view["reason"] = "remote-engine-runtime-sync-failed"
            view["error"] = str(error)
            return view

        attempt = (
            int(sync_state.get("attempts", 0) or 0) + 1
            if str(sync_state.get("expected_remote_generation") or "")
            == self.expected_remote_generation
            else 1
        )
        if not view["snapshot_fresh"]:
            snapshot_request_id = str(
                sync_state.get("snapshot_request_id") or ""
            )
            if not snapshot_request_id:
                snapshot_request_id = (
                    "engine-runtime-preflight-"
                    f"{self.expected_remote_generation[:16]}-v3-"
                    f"{cycle_token()}"
                )
                self._patch_runtime_sync(
                    state="observing",
                    reason="remote-engine-runtime-snapshot-required",
                    attempts=int(sync_state.get("attempts", 0) or 0),
                    snapshot_request_id=snapshot_request_id,
                    observed_remote_generation=view[
                        "observed_remote_generation"
                    ],
                )
            try:
                admission = self._refresh_runtime_snapshot(
                    wait_timeout_seconds=wait_timeout_seconds,
                    purpose="preflight",
                    request_id=snapshot_request_id,
                )
            except Exception as exc:
                self._record_runtime_sync_failure(
                    operation="snapshot",
                    error=exc,
                    attempt=attempt,
                    request_id=snapshot_request_id,
                    observed_generation=view[
                        "observed_remote_generation"
                    ],
                )
                view["reason"] = "remote-engine-runtime-snapshot-failed"
                view["error"] = str(exc)
                return view
            self._patch_runtime_sync(
                snapshot_request_id="",
                snapshot_attempts=0,
                last_error="",
                next_retry_at="",
            )
            view = self._runtime_generation_view(admission)
            if view["state"] == "ready":
                self._patch_runtime_sync(
                    state="confirmed",
                    reason="",
                    attempts=int(sync_state.get("attempts", 0) or 0),
                    observed_remote_generation=view[
                        "observed_remote_generation"
                    ],
                    confirmed_at=utc_now_iso(),
                    last_error="",
                    next_retry_at="",
                )
                return view

        if not view["remote_quiescent"]:
            view["reason"] = "remote-engine-runtime-sync-waits-for-quiescence"
            self._patch_runtime_sync(
                state="hold",
                reason=view["reason"],
                observed_remote_generation=view[
                    "observed_remote_generation"
                ],
                last_error="",
                next_retry_at="",
            )
            return view

        sync_code = getattr(self.transport, "sync_code", None)
        if not callable(sync_code):
            exc = EnginePumpError(
                "Engine transport does not support runtime code synchronization"
            )
            self._record_runtime_sync_failure(
                operation="sync",
                error=exc,
                attempt=attempt,
                request_id="",
                observed_generation=view["observed_remote_generation"],
            )
            view["reason"] = "remote-engine-runtime-sync-unsupported"
            view["error"] = str(exc)
            return view

        request_id = (
            f"engine-runtime-sync-{self.expected_remote_generation[:16]}"
            f"-v3-a{attempt:03d}"
        )
        started_at = utc_now_iso()
        self._patch_runtime_sync(
            state="syncing",
            reason="",
            attempts=attempt,
            request_id=request_id,
            observed_remote_generation=view[
                "observed_remote_generation"
            ],
            started_at=started_at,
            last_error="",
            next_retry_at="",
            verify_snapshot_request_id="",
            verify_attempts=0,
        )
        self._event(
            "engine_runtime_sync_started",
            request_id=request_id,
            expected_remote_generation=self.expected_remote_generation,
            observed_remote_generation=view["observed_remote_generation"],
            attempt=attempt,
            started_at=started_at,
        )
        try:
            result = sync_code(
                request_id=request_id,
                wait_timeout_seconds=min(
                    RUNTIME_CONTROL_WAIT_SECONDS,
                    max(1, wait_timeout_seconds),
                ),
            )
        except Exception as exc:
            self._record_runtime_sync_failure(
                operation="sync",
                error=exc,
                attempt=attempt,
                request_id=request_id,
                observed_generation=self._runtime_generation_view()[
                    "observed_remote_generation"
                ],
            )
            view["reason"] = "remote-engine-runtime-sync-failed"
            view["error"] = str(exc)
            return view

        verify_request_id = (
            f"engine-runtime-verify-{self.expected_remote_generation[:16]}"
            f"-v3-a{attempt:03d}"
        )
        self._patch_runtime_sync(
            state="verifying",
            reason="",
            attempts=attempt,
            request_id=request_id,
            verify_snapshot_request_id=verify_request_id,
            verify_attempts=0,
            transport_result=result,
            last_error="",
            next_retry_at="",
        )
        try:
            admission = self._refresh_runtime_snapshot(
                wait_timeout_seconds=wait_timeout_seconds,
                purpose="verify",
                request_id=verify_request_id,
            )
        except Exception as exc:
            self._record_runtime_sync_failure(
                operation="verify",
                error=exc,
                attempt=attempt,
                request_id=verify_request_id,
                observed_generation=self._runtime_generation_view()[
                    "observed_remote_generation"
                ],
            )
            view["reason"] = "remote-engine-runtime-verify-failed"
            view["error"] = str(exc)
            return view
        verified = self._runtime_generation_view(admission)
        if verified["state"] != "ready":
            exc = EnginePumpError(
                "Engine runtime sync did not converge: "
                f"expected={self.expected_remote_generation} "
                f"observed={verified['observed_remote_generation']}"
            )
            self._patch_runtime_sync(
                verify_snapshot_request_id="",
                verify_attempts=0,
            )
            self._record_runtime_sync_failure(
                operation="sync",
                error=exc,
                attempt=attempt,
                request_id=request_id,
                observed_generation=verified[
                    "observed_remote_generation"
                ],
            )
            view["reason"] = "remote-engine-runtime-sync-failed"
            view["error"] = str(exc)
            return view

        completed_at = utc_now_iso()
        self._patch_runtime_sync(
            state="confirmed",
            reason="",
            attempts=attempt,
            request_id=request_id,
            observed_remote_generation=self.expected_remote_generation,
            completed_at=completed_at,
            confirmed_at=completed_at,
            last_error="",
            next_retry_at="",
            transport_result=result,
        )
        self._event(
            "engine_runtime_sync_completed",
            request_id=request_id,
            expected_remote_generation=self.expected_remote_generation,
            observed_remote_generation=self.expected_remote_generation,
            attempt=attempt,
            started_at=started_at,
            completed_at=completed_at,
            transport_elapsed_seconds=result.get(
                "transport_elapsed_seconds"
            ),
        )
        return self._runtime_generation_view()

    def enqueue(
        self, spec_path: Path, payload_root: Path | None = None
    ) -> dict[str, Any]:
        record, staged = self._stage_enqueue(spec_path, payload_root)
        if not staged:
            return record
        try:
            with PumpLock(self.lock_path):
                self._merge_enqueue_requests_unlocked()
        except EnginePumpError:
            return record
        merged = self.read()["entries"].get(
            str(record.get("engine_job_id") or "")
        )
        return merged if isinstance(merged, dict) else record

    def _stage_enqueue(
        self, spec_path: Path, payload_root: Path | None = None
    ) -> tuple[dict[str, Any], bool]:
        with PumpLock(
            self.enqueue_lock_path,
            wait_timeout_seconds=10.0,
        ):
            spec = read_object(spec_path)
            key = correlation(spec)
            job_id = key["engine_job_id"]
            spec_digest = json_digest(spec)
            logical_job_key = logical_job_key_from_spec(spec)
            state = self.read()
            existing = state["entries"].get(job_id)
            if isinstance(existing, dict):
                if str(existing.get("spec_digest") or "") != spec_digest:
                    raise EnginePumpError(
                        f"engine outbox job id collision: {job_id}"
                    )
                return existing, False
            staged = self._staged_enqueue_records_unlocked()
            staged_exact = next(
                (
                    item
                    for _path, item in staged
                    if str(item.get("engine_job_id") or "") == job_id
                ),
                None,
            )
            if staged_exact is not None:
                if str(staged_exact.get("spec_digest") or "") != spec_digest:
                    raise EnginePumpError(
                        f"engine staged job id collision: {job_id}"
                    )
                return staged_exact, False
            logical_existing = self._active_logical_record(
                logical_job_key,
                [
                    *(
                        item
                        for item in state["entries"].values()
                        if isinstance(item, dict)
                    ),
                    *(item for _path, item in staged),
                ],
            )
            if logical_existing is not None:
                self._event(
                    "engine_logical_enqueue_deduplicated",
                    logical_job_key=logical_job_key,
                    requested_engine_job_id=job_id,
                    engine_job_id=str(
                        logical_existing.get("engine_job_id") or ""
                    ),
                    existing_state=str(logical_existing.get("state") or ""),
                )
                return logical_existing, False

            target = self.bundle_dir / job_id
            if target.exists():
                shutil.rmtree(target)
            target.mkdir(parents=True)
            write_json(target / "spec.json", spec)
            if payload_root is not None:
                copy_bundle(payload_root.resolve(), target / "payload")
            now = utc_now_iso()
            workflow = spec.get("workflow")
            workflow = workflow if isinstance(workflow, dict) else {}
            input_identity = spec.get("input_identity")
            input_identity = (
                input_identity if isinstance(input_identity, dict) else {}
            )
            job_kind = str(
                workflow.get("job_kind")
                or input_identity.get("job_kind")
                or "operator-test"
            )
            record = {
                **key,
                "state": "pending",
                "spec_path": relative_path(target / "spec.json", self.root),
                "payload_root": (
                    relative_path(target / "payload", self.root)
                    if payload_root is not None
                    else ""
                ),
                "spec_digest": spec_digest,
                "logical_job_key": logical_job_key,
                "execution_profile": str(
                    spec.get("execution_profile") or ""
                ),
                "job_kind": job_kind,
                "case_cache_requirement_sha256": str(
                    input_identity.get("case_cache_requirement_sha256") or ""
                ),
                "case_cache_access": str(
                    input_identity.get("case_cache_access") or ""
                ),
                "profiler_request_sha256": str(
                    input_identity.get("profiler_request_sha256") or ""
                ),
                "profiler_target_source_sha256": str(
                    input_identity.get("profiler_target_source_sha256")
                    or input_identity.get("target_source_sha256")
                    or ""
                ),
                "scheduler_policy": (
                    dict(spec.get("scheduler_policy", {}))
                    if isinstance(spec.get("scheduler_policy"), dict)
                    else {}
                ),
                "workflow_ingest": bool(spec.get("workflow_ingest", True)),
                "enqueued_at": now,
                "sequence": 0,
                "updated_at": now,
                "attempts": 0,
                "admission_attempts": 0,
                "last_error": "",
            }
            write_json(
                self._enqueue_request_path(job_id),
                {
                    "protocol_version": "engine-enqueue-v1",
                    "record": record,
                    "staged_at": now,
                },
            )
            self._event("engine_outbox_staged", **key)
            return record, True

    def _enqueue_request_path(self, job_id: str) -> Path:
        digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()[:24]
        return self.enqueue_dir / f"{digest}.json"

    def _staged_enqueue_records_unlocked(
        self,
    ) -> list[tuple[Path, dict[str, Any]]]:
        if not self.enqueue_dir.is_dir():
            return []
        records: list[tuple[Path, dict[str, Any]]] = []
        for path in sorted(self.enqueue_dir.glob("*.json")):
            request = read_object(path)
            if str(request.get("protocol_version") or "") != (
                "engine-enqueue-v1"
            ):
                raise EnginePumpError(
                    f"engine enqueue request has unsupported protocol: {path}"
                )
            record = request.get("record")
            if not isinstance(record, dict):
                raise EnginePumpError(
                    f"engine enqueue request record is invalid: {path}"
                )
            records.append((path, dict(record)))
        return records

    def _staged_enqueue_records(
        self,
    ) -> list[tuple[Path, dict[str, Any]]]:
        try:
            return self._staged_enqueue_records_unlocked()
        except (
            OSError,
            ValueError,
            json.JSONDecodeError,
            EnginePumpError,
        ):
            return []

    def _active_logical_record(
        self,
        logical_job_key: str,
        records: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not logical_job_key:
            return None
        return next(
            (
                record
                for record in records
                if self._logical_job_key_for_record(record)
                == logical_job_key
                and str(record.get("state") or "")
                in LOGICAL_JOB_ACTIVE_STATES
            ),
            None,
        )

    def _merge_enqueue_requests_unlocked(self) -> list[str]:
        try:
            lock = PumpLock(self.enqueue_lock_path)
            lock.__enter__()
        except EnginePumpError:
            return []
        try:
            staged = self._staged_enqueue_records_unlocked()
            if not staged:
                return []
            state = self.read()
            sequence = max(
                (
                    int(item.get("sequence", 0) or 0)
                    for item in state["entries"].values()
                    if isinstance(item, dict)
                ),
                default=0,
            )
            merged: list[dict[str, Any]] = []
            deduplicated: list[tuple[dict[str, Any], dict[str, Any]]] = []
            completed_paths: list[Path] = []
            for request_path, staged_record in staged:
                job_id = str(staged_record.get("engine_job_id") or "")
                spec_digest = str(staged_record.get("spec_digest") or "")
                if not job_id or not spec_digest:
                    raise EnginePumpError(
                        "engine staged enqueue identity is incomplete: "
                        f"{request_path}"
                    )
                existing = state["entries"].get(job_id)
                if isinstance(existing, dict):
                    if str(existing.get("spec_digest") or "") != spec_digest:
                        raise EnginePumpError(
                            f"engine outbox job id collision: {job_id}"
                        )
                    completed_paths.append(request_path)
                    continue
                logical_key = self._logical_job_key_for_record(staged_record)
                logical_existing = self._active_logical_record(
                    logical_key,
                    [
                        item
                        for item in state["entries"].values()
                        if isinstance(item, dict)
                    ],
                )
                if logical_existing is not None:
                    deduplicated.append(
                        (staged_record, logical_existing)
                    )
                    completed_paths.append(request_path)
                    continue
                sequence += 1
                staged_record.update(
                    {
                        "sequence": sequence,
                        "logical_job_key": logical_key,
                        "updated_at": utc_now_iso(),
                    }
                )
                state["entries"][job_id] = staged_record
                merged.append(dict(staged_record))
                completed_paths.append(request_path)
            if merged:
                state["updated_at"] = utc_now_iso()
                self._write(state)
            for request_path in completed_paths:
                request_path.unlink(missing_ok=True)
            for record in merged:
                self._event(
                    "engine_outbox_enqueued",
                    **correlation(record),
                )
            for requested, existing in deduplicated:
                self._event(
                    "engine_logical_enqueue_deduplicated",
                    logical_job_key=self._logical_job_key_for_record(
                        requested
                    ),
                    requested_engine_job_id=str(
                        requested.get("engine_job_id") or ""
                    ),
                    engine_job_id=str(
                        existing.get("engine_job_id") or ""
                    ),
                    existing_state=str(existing.get("state") or ""),
                    source="durable-enqueue-merge",
                )
            return [
                str(record.get("engine_job_id") or "")
                for record in merged
            ]
        finally:
            lock.__exit__(None, None, None)

    def _logical_job_key_for_record(
        self, record: dict[str, Any]
    ) -> str:
        existing = str(record.get("logical_job_key") or "")
        if existing:
            return existing
        identity = {
            "job_kind": str(record.get("job_kind") or "operator-test"),
            "case_cache_requirement_sha256": str(
                record.get("case_cache_requirement_sha256") or ""
            ),
            "profiler_request_sha256": str(
                record.get("profiler_request_sha256") or ""
            ),
            "profiler_target_source_sha256": str(
                record.get("profiler_target_source_sha256") or ""
            ),
        }
        payload = {
            "operator": str(record.get("operator") or ""),
            "test_version": str(record.get("test_version") or ""),
            "workflow": {"job_kind": identity["job_kind"]},
            "input_identity": identity,
        }
        logical_key = logical_job_key_from_spec(payload)
        if logical_key:
            return logical_key
        spec_path = str(record.get("spec_path") or "")
        if not spec_path:
            return ""
        return logical_job_key_from_spec(read_object(self.root / spec_path))

    def tick(self, *, wait_timeout_seconds: int = 180) -> dict[str, Any]:
        if (
            bool(getattr(self.transport, "supports_duplex_exchange", False))
            and callable(getattr(self.transport, "publish_exchange", None))
            and callable(getattr(self.transport, "poll_exchanges", None))
        ):
            return self._tick_duplex(wait_timeout_seconds=wait_timeout_seconds)
        with PumpLock(self.lock_path):
            self._merge_enqueue_requests_unlocked()
            if self.transport is None:
                raise EnginePumpError("engine pump transport is not configured")
            admission = self.admission.read()
            if not admission.get("enabled"):
                return self.status(outcome="disabled")
            cached_ready_recovered = self._recover_cached_ready_bundles()
            optional_materialized = self._recover_optional_materializations()
            cached_reconciled = self._sync_from_admission()
            if cached_reconciled:
                self._event(
                    "engine_uncertain_admission_reconciled",
                    engine_job_ids=cached_reconciled,
                    source="trusted-admission-cache",
                )
            recovered_capacity_retries = (
                self._recover_retryable_admission_failures()
            )
            if recovered_capacity_retries:
                self._event(
                    "engine_remote_capacity_retries_recovered",
                    engine_job_ids=recovered_capacity_retries,
                    source="pump-state-upgrade",
                )
            # Reconciliation and upgrade recovery can change active local jobs.
            # Do not make this cycle's credit decision from the pre-recovery read.
            admission = self.admission.read()
            superseded = self._suppress_pending_with_workflow_result()
            stopped = stop_requested(self.root)
            if stopped or admission.get("draining"):
                self._request_all_standby_cancellations_unlocked(
                    reason=(
                        "workflow stop fence cancelled remote standby"
                        if stopped
                        else "engine drain cancelled remote standby"
                    )
                )

            active_exchanges = [
                dict(item)
                for item in self.read().get("exchanges", {}).values()
                if isinstance(item, dict)
                and str(item.get("state") or "")
                in {"publishing", "published", "uncertain"}
            ]
            runtime_blocking_exchanges = [
                item
                for item in active_exchanges
                if runtime_sync_blocking_exchange(item)
            ]
            runtime_gate = self._ensure_remote_runtime_generation(
                admission=admission,
                active_exchanges=runtime_blocking_exchanges,
                wait_timeout_seconds=wait_timeout_seconds,
                allow_sync=not stopped,
            )
            runtime_admission_ready = runtime_gate["state"] == "ready"
            runtime_recovered = (
                self._recover_runtime_generation_admission_failures()
                if runtime_admission_ready
                else []
            )
            if runtime_recovered:
                admission = self.admission.read()

            cycle = cycle_token()
            pending_acknowledgements = self._pending_remote_acknowledgements()
            pending_required_acknowledgements = (
                self._pending_required_acknowledgements()
            )
            pending_standby_cancellations = self._pending_standby_cancellations()
            desired_inflight = max(1, int(admission.get("target_inflight", 1) or 1))
            desired_draining = bool(admission.get("draining"))
            exchange = getattr(self.transport, "exchange", None)
            exchange_used = callable(exchange)
            exchange_records: list[dict[str, Any]] = []
            exchange_attempts: dict[str, tuple[int, str, str, str]] = {}
            if exchange_used:
                local_credit = self.admission.local_credit(admission)
                last_engine_snapshot = admission.get("last_engine_snapshot")
                has_fresh_remote_snapshot = (
                    isinstance(last_engine_snapshot, dict)
                    and bool(last_engine_snapshot)
                    and not snapshot_stale(
                        last_engine_snapshot,
                        REMOTE_CREDIT_SNAPSHOT_MAX_AGE_SECONDS,
                    )
                )
                credit = (
                    self.admission.effective_credit(
                        last_engine_snapshot,
                        max_snapshot_age_seconds=(
                            REMOTE_CREDIT_SNAPSHOT_MAX_AGE_SECONDS
                        ),
                    )
                    if has_fresh_remote_snapshot
                    else local_credit
                )
                manual_controller = bool(self.admission.active_controller_lease())
                pending_records = (
                    [
                        record
                        for record in self._entries_in_state("pending")
                        if self._payload_is_transportable(record)
                    ]
                    if runtime_admission_ready
                    else []
                )
                selected_ids: set[str] = set()
                for record in pending_records:
                    if credit <= 0:
                        break
                    workflow_ingest = bool(record.get("workflow_ingest", True))
                    if manual_controller and workflow_ingest:
                        continue
                    if stopped and (not manual_controller or workflow_ingest):
                        continue
                    job_id = str(record["engine_job_id"])
                    admission_attempt = (
                        int(record.get("admission_attempts", 0) or 0) + 1
                    )
                    transport_request_id = f"engine-exchange-{cycle}"
                    accept_started_at = utc_now_iso()
                    self._patch_entry(
                        job_id,
                        state="admitting",
                        admission_attempts=admission_attempt,
                        last_admission_request_id=transport_request_id,
                    )
                    self._event(
                        "engine_accept_started",
                        **correlation(record),
                        admission_attempt=admission_attempt,
                        transport_request_id=transport_request_id,
                        started_at=accept_started_at,
                        transport_mode="batch-exchange",
                    )
                    exchange_records.append(
                        {
                            "engine_job_id": job_id,
                            "admission_mode": "accept",
                            "spec_path": str(self.root / str(record["spec_path"])),
                            "payload_root": (
                                str(self.root / str(record["payload_root"]))
                                if record.get("payload_root")
                                else ""
                            ),
                        }
                    )
                    exchange_attempts[job_id] = (
                        admission_attempt,
                        transport_request_id,
                        accept_started_at,
                        "accept",
                    )
                    selected_ids.add(job_id)
                    credit -= 1

                standby_target = max(
                    0, int(self.capacity_overrides.get("standby_slots", 0) or 0)
                )
                standby_active = sum(
                    1
                    for item in admission.get("jobs", {}).values()
                    if isinstance(item, dict)
                    and item.get("state") in {"staging-standby", "standby"}
                )
                standby_credit = max(0, standby_target - standby_active)
                if has_fresh_remote_snapshot:
                    standby_credit = min(
                        standby_credit,
                        max(
                            0,
                            int(
                                last_engine_snapshot.get("standby_credit", 0)
                                or 0
                            ),
                        ),
                    )
                for record in pending_records:
                    if standby_credit <= 0:
                        break
                    job_id = str(record["engine_job_id"])
                    if job_id in selected_ids:
                        continue
                    workflow_ingest = bool(record.get("workflow_ingest", True))
                    if manual_controller and workflow_ingest:
                        continue
                    if stopped and (not manual_controller or workflow_ingest):
                        continue
                    standby_attempt = int(record.get("admission_attempts", 0) or 0) + 1
                    transport_request_id = f"engine-exchange-{cycle}"
                    standby_started_at = utc_now_iso()
                    self._patch_entry(
                        job_id,
                        state="staging-standby",
                        admission_attempts=standby_attempt,
                        last_admission_request_id=transport_request_id,
                    )
                    self._event(
                        "engine_standby_started",
                        **correlation(record),
                        admission_attempt=standby_attempt,
                        transport_request_id=transport_request_id,
                        started_at=standby_started_at,
                        transport_mode="batch-exchange",
                    )
                    exchange_records.append(
                        {
                            "engine_job_id": job_id,
                            "admission_mode": "standby",
                            "spec_path": str(self.root / str(record["spec_path"])),
                            "payload_root": (
                                str(self.root / str(record["payload_root"]))
                                if record.get("payload_root")
                                else ""
                            ),
                        }
                    )
                    exchange_attempts[job_id] = (
                        standby_attempt,
                        transport_request_id,
                        standby_started_at,
                        "standby",
                    )
                    selected_ids.add(job_id)
                    standby_credit -= 1

            snapshot_started_at = utc_now_iso()
            snapshot_request_id = (
                f"engine-exchange-{cycle}"
                if exchange_used
                else f"engine-snapshot-{cycle}"
            )
            self._event(
                (
                    "engine_exchange_started"
                    if exchange_used
                    else "engine_snapshot_started"
                ),
                snapshot_request_id=snapshot_request_id,
                started_at=snapshot_started_at,
                acknowledgement_count=len(pending_acknowledgements),
                required_acknowledgement_count=len(pending_required_acknowledgements),
                standby_cancellation_count=len(pending_standby_cancellations),
                admission_count=len(exchange_records),
            )
            exchange_recovered = False
            exchange_original_error = ""
            exchange_recovery_request_id = ""
            try:
                if exchange_used:
                    remote = exchange(
                        exchange_records,
                        request_id=snapshot_request_id,
                        acknowledgements=pending_acknowledgements,
                        required_acknowledgements=pending_required_acknowledgements,
                        standby_cancellations=pending_standby_cancellations,
                        max_inflight=desired_inflight,
                        draining=desired_draining,
                        **self.capacity_overrides,
                        wait_timeout_seconds=wait_timeout_seconds,
                    )
                else:
                    remote = self.transport.snapshot(
                        request_id=snapshot_request_id,
                        acknowledgements=pending_acknowledgements,
                        wait_timeout_seconds=wait_timeout_seconds,
                    )
            except Exception as exc:
                exchange_original_error = str(exc)
                recovery_snapshot = getattr(self.transport, "snapshot", None)
                try:
                    if not exchange_used or not callable(recovery_snapshot):
                        raise
                    exchange_recovery_request_id = f"engine-reconcile-{cycle}"
                    remote = recovery_snapshot(
                        request_id=exchange_recovery_request_id,
                        acknowledgements=pending_acknowledgements,
                        wait_timeout_seconds=max(
                            wait_timeout_seconds,
                            RECOVERY_SNAPSHOT_MIN_TIMEOUT_SECONDS,
                        ),
                    )
                    recovered_receipts = self._recover_exchange_receipts(
                        remote.get("engine_snapshot", {}),
                        exchange_attempts,
                    )
                    remote = dict(remote)
                    remote["accepted_receipts"] = recovered_receipts["accepted"]
                    remote["standby_receipts"] = recovered_receipts["standby"]
                    remote.setdefault("required_acknowledgement_receipts", [])
                    remote.setdefault("standby_cancellation_receipts", [])
                    exchange_recovered = True
                    self._event(
                        "engine_exchange_recovered_from_snapshot",
                        snapshot_request_id=snapshot_request_id,
                        recovery_request_id=exchange_recovery_request_id,
                        started_at=snapshot_started_at,
                        admission_count=len(exchange_records),
                        recovered_acceptance_count=len(recovered_receipts["accepted"]),
                        recovered_standby_count=len(recovered_receipts["standby"]),
                        recovered_job_ids=[
                            str(item.get("engine_job_id") or "")
                            for item in (
                                recovered_receipts["accepted"]
                                + recovered_receipts["standby"]
                            )
                        ],
                        error=exchange_original_error,
                    )
                except Exception as recovery_exc:
                    for record in exchange_records:
                        self._record_error(
                            str(record["engine_job_id"]), "exchange", exc
                        )
                    self._event(
                        (
                            "engine_exchange_failed"
                            if exchange_used
                            else "engine_snapshot_failed"
                        ),
                        snapshot_request_id=snapshot_request_id,
                        started_at=snapshot_started_at,
                        admission_count=len(exchange_records),
                        required_acknowledgement_count=len(
                            pending_required_acknowledgements
                        ),
                        standby_cancellation_count=len(pending_standby_cancellations),
                        error=str(exc),
                        recovery_error=(
                            str(recovery_exc) if recovery_exc is not exc else ""
                        ),
                    )
                    return self.status(
                        outcome=(
                            "exchange-failed" if exchange_used else "snapshot-failed"
                        ),
                        error=str(exc),
                        recovery_error=(
                            str(recovery_exc) if recovery_exc is not exc else ""
                        ),
                    )

            snapshot_completed_at = utc_now_iso()
            capacity_error = ""
            snapshot = remote.get("engine_snapshot", {})
            exchange_timeline = (
                remote.get("exchange_timeline", {}) if exchange_used else {}
            )
            exchange_timeline = (
                exchange_timeline if isinstance(exchange_timeline, dict) else {}
            )
            local_transport_timeline = (
                remote.get("local_transport_timeline", {}) if exchange_used else {}
            )
            local_transport_timeline = (
                local_transport_timeline
                if isinstance(local_transport_timeline, dict)
                else {}
            )
            exchange_job_timings = {
                str(item.get("engine_job_id") or ""): item
                for item in exchange_timeline.get("jobs", [])
                if isinstance(item, dict) and item.get("engine_job_id")
            }
            snapshot_transport_elapsed = remote.get("transport_elapsed_seconds")
            snapshot_ready_bundles = remote.get("ready_bundles", [])
            snapshot_acknowledgements = remote.get("acknowledgements_sent", [])
            snapshot_request_id = str(
                remote.get("snapshot_request_id") or f"engine-snapshot-{cycle}"
            )
            self._event(
                (
                    "engine_exchange_completed"
                    if exchange_used
                    else "engine_snapshot_completed"
                ),
                snapshot_request_id=snapshot_request_id,
                started_at=snapshot_started_at,
                completed_at=snapshot_completed_at,
                transport_elapsed_seconds=snapshot_transport_elapsed,
                engine_observed_at=str(snapshot.get("observed_at") or ""),
                ready_job_ids=[
                    str(item.get("engine_job_id") or "")
                    for item in snapshot_ready_bundles
                    if isinstance(item, dict) and item.get("engine_job_id")
                ],
                admitted_job_ids=[
                    str(item.get("engine_job_id") or "")
                    for item in remote.get("accepted_receipts", [])
                    if isinstance(item, dict) and item.get("engine_job_id")
                ],
                b_request_observed_at=str(
                    exchange_timeline.get("b_request_observed_at") or ""
                ),
                b_return_export_started_at=str(
                    exchange_timeline.get("return_export_started_at") or ""
                ),
                b_return_export_finished_at=str(
                    exchange_timeline.get("return_export_finished_at") or ""
                ),
                b_exchange_finished_at=str(
                    exchange_timeline.get("b_exchange_finished_at") or ""
                ),
                local_transport_timeline=local_transport_timeline,
            )
            remote_capacity = snapshot.get("capacity", {})
            capacity_mismatch = (
                int(remote_capacity.get("max_inflight", 0) or 0) != desired_inflight
                or bool(remote_capacity.get("draining")) != desired_draining
                or any(
                    not capacity_value_matches(remote_capacity.get(key), value)
                    for key, value in self.capacity_overrides.items()
                )
            )
            configure = getattr(self.transport, "configure", None)
            if capacity_mismatch and exchange_used and not exchange_recovered:
                capacity_error = "batch exchange returned mismatched capacity"
                self._event(
                    "engine_capacity_converge_failed",
                    error=capacity_error,
                    remote_capacity=remote_capacity,
                    desired_inflight=desired_inflight,
                    desired_draining=desired_draining,
                )
            elif capacity_mismatch and callable(configure):
                try:
                    remote = configure(
                        request_id=f"engine-configure-{cycle}",
                        max_inflight=desired_inflight,
                        draining=desired_draining,
                        **self.capacity_overrides,
                        wait_timeout_seconds=wait_timeout_seconds,
                    )
                    snapshot = remote.get("engine_snapshot", {})
                    self._event(
                        "engine_capacity_converged",
                        max_inflight=desired_inflight,
                        draining=desired_draining,
                        **self.capacity_overrides,
                    )
                except Exception as exc:
                    capacity_error = str(exc)
                    self._event("engine_capacity_converge_failed", error=capacity_error)

            exchange_admitted: list[str] = []
            if exchange_used:
                batch_elapsed = remote.get("transport_elapsed_seconds")
                receipts = remote.get("accepted_receipts", [])
                for receipt in receipts if isinstance(receipts, list) else []:
                    if not isinstance(receipt, dict):
                        continue
                    job_id = str(receipt.get("engine_job_id") or "")
                    state = self.read()
                    record = state.get("entries", {}).get(job_id)
                    if not isinstance(record, dict) or job_id not in exchange_attempts:
                        continue
                    (
                        admission_attempt,
                        transport_request_id,
                        accept_started_at,
                        admission_mode,
                    ) = exchange_attempts[job_id]
                    if admission_mode != "accept":
                        continue
                    accepted_at = str(receipt.get("accepted_at") or utc_now_iso())
                    accept_completed_at = snapshot_completed_at
                    self._update_entry(job_id, "accepted", accepted_at=accepted_at)
                    self._event(
                        "engine_job_admitted",
                        **correlation(record),
                        accepted_at=accepted_at,
                        admission_attempt=admission_attempt,
                        transport_request_id=transport_request_id,
                        transport_elapsed_seconds=batch_elapsed,
                        amortized_transport_elapsed_seconds=(
                            round(float(batch_elapsed) / max(1, len(receipts)), 6)
                            if batch_elapsed is not None
                            else None
                        ),
                        accept_started_at=accept_started_at,
                        accept_completed_at=accept_completed_at,
                        transport_mode="batch-exchange",
                        batch_size=len(receipts),
                        b_request_observed_at=str(
                            exchange_timeline.get("b_request_observed_at") or ""
                        ),
                        b_admission_started_at=str(
                            dict(exchange_job_timings.get(job_id, {})).get("started_at")
                            or ""
                        ),
                        b_admission_finished_at=str(
                            dict(exchange_job_timings.get(job_id, {})).get(
                                "finished_at"
                            )
                            or ""
                        ),
                        local_transport_timeline=local_transport_timeline,
                    )
                    exchange_admitted.append(job_id)
                standby_receipts = remote.get("standby_receipts", [])
                for receipt in (
                    standby_receipts if isinstance(standby_receipts, list) else []
                ):
                    if not isinstance(receipt, dict):
                        continue
                    job_id = str(receipt.get("engine_job_id") or "")
                    state = self.read()
                    record = state.get("entries", {}).get(job_id)
                    if not isinstance(record, dict) or job_id not in exchange_attempts:
                        continue
                    (
                        standby_attempt,
                        transport_request_id,
                        standby_started_at,
                        admission_mode,
                    ) = exchange_attempts[job_id]
                    if admission_mode != "standby":
                        continue
                    staged_at = str(receipt.get("staged_at") or snapshot_completed_at)
                    self._update_entry(job_id, "standby", staged_at=staged_at)
                    self._event(
                        "engine_job_staged_standby",
                        **correlation(record),
                        staged_at=staged_at,
                        admission_attempt=standby_attempt,
                        transport_request_id=transport_request_id,
                        standby_started_at=standby_started_at,
                        transport_mode="batch-exchange",
                    )
                rejected_jobs = remote.get("rejected_jobs", [])
                rejection_errors = remote.get("rejection_errors", {})
                for job_id in rejected_jobs if isinstance(rejected_jobs, list) else []:
                    if str(job_id) in exchange_attempts:
                        admission_mode = exchange_attempts[str(job_id)][3]
                        reason = (
                            str(rejection_errors.get(str(job_id)) or "")
                            if isinstance(rejection_errors, dict)
                            else ""
                        )
                        message = f"remote engine rejected batch {admission_mode}"
                        if reason:
                            message += f": {reason}"
                        self._fail_remote_admission(
                            str(job_id),
                            f"exchange-{admission_mode}",
                            message,
                        )

            standby_cancelled = self._record_standby_cancellation_receipts(
                remote.get("standby_cancellation_receipts", []),
                request_id=snapshot_request_id,
            )
            required_ack_piggybacked = self._record_required_ack_piggybacks(
                remote.get("required_acknowledgement_receipts", []),
                snapshot_request_id=snapshot_request_id,
            )

            admission_retries_scheduled = self._release_unobserved_admissions(
                snapshot,
                exchange_attempts=exchange_attempts,
                exchange_request_id=snapshot_request_id,
                recovery_request_id=exchange_recovery_request_id,
                original_error=exchange_original_error,
            )
            self._sync_from_admission()
            exchange_admitted.extend(
                self._observe_standby_promotions(snapshot_completed_at)
            )
            ack_piggybacked = self._record_ack_piggybacks(
                snapshot_acknowledgements,
                snapshot_request_id=snapshot_request_id,
            )
            self._confirm_remote_acks(snapshot)
            released_records = self._unreplenished_credit_records()
            for job_id in exchange_admitted:
                if not released_records:
                    break
                state = self.read()
                replacement = state.get("entries", {}).get(job_id)
                if not isinstance(replacement, dict):
                    continue
                released = released_records.pop(0)
                self._record_replenishment(
                    released,
                    replacement_engine_job_id=job_id,
                    replenished_at=str(
                        replacement.get("accepted_at") or snapshot_completed_at
                    ),
                    replenished_observed_at=snapshot_completed_at,
                )
            collected = list(cached_ready_recovered)
            for job_id in self._consume_ready_bundles(
                snapshot_ready_bundles,
                snapshot_request_id=snapshot_request_id,
                transport_elapsed_seconds=snapshot_transport_elapsed,
                snapshot_started_at=snapshot_started_at,
                snapshot_completed_at=snapshot_completed_at,
                engine_observed_at=str(snapshot.get("observed_at") or ""),
                exchange_timeline=exchange_timeline,
                local_transport_timeline=local_transport_timeline,
            ):
                if job_id not in collected:
                    collected.append(job_id)
            for record in self._entries_in_state("return-ready"):
                job_id = str(record["engine_job_id"])
                receipt_id = return_receipt_id(record)
                if not record.get("return_receipt_id"):
                    self._update_entry(
                        job_id,
                        "return-ready",
                        return_receipt_id=receipt_id,
                    )
                try:
                    current = self.read()["entries"].get(job_id, {})
                    collect_attempt = (
                        int(
                            current.get("collect_attempts", 0)
                            if isinstance(current, dict)
                            else 0
                        )
                        + 1
                    )
                    collect_request_id = (
                        f"engine-collect-{job_id}-attempt{collect_attempt:03d}"
                    )
                    self._patch_entry(
                        job_id,
                        collect_attempts=collect_attempt,
                        last_collect_request_id=collect_request_id,
                    )
                    returned = self.transport.collect(
                        request_id=collect_request_id,
                        engine_job_id=job_id,
                        receipt_id=receipt_id,
                        wait_timeout_seconds=wait_timeout_seconds,
                    )
                    terminal = returned.get("terminal", {})
                    failed_stage, failed_exit_code = terminal_failure_summary(terminal)
                    admission_record = returned.get("admission_record", {})
                    identity_evidence = returned.get("identity_evidence", {})
                    returned_at = (
                        str(admission_record.get("returned_at") or "")
                        if isinstance(admission_record, dict)
                        else ""
                    )
                    self._update_entry(
                        job_id,
                        "returned-awaiting-ingest",
                        return_receipt_id=receipt_id,
                        collect_request_id=collect_request_id,
                        engine_terminal_state=str(terminal.get("state") or ""),
                        engine_failed_stage=failed_stage,
                        engine_failed_exit_code=failed_exit_code,
                        engine_code_generation=str(
                            terminal.get("engine_code_generation") or ""
                        ),
                        terminal_at=str(terminal.get("terminal_at") or ""),
                        returned_at=returned_at or utc_now_iso(),
                        identity_evidence=(
                            identity_evidence
                            if isinstance(identity_evidence, dict)
                            else {}
                        ),
                        remote_ack_state="confirmed",
                        remote_ack_confirmed_at=returned_at or utc_now_iso(),
                        return_transport_mode="legacy-collect",
                        collect_transport_elapsed_seconds=returned.get(
                            "transport_elapsed_seconds"
                        ),
                    )
                    self._event(
                        "engine_job_terminal_returned",
                        **correlation(record),
                        collect_request_id=collect_request_id,
                        terminal_at=str(terminal.get("terminal_at") or ""),
                        return_ready_at=str(
                            terminal.get("return_ready_at")
                            or terminal.get("terminal_at")
                            or ""
                        ),
                        terminal_state=str(terminal.get("state") or ""),
                        stage_history=list(terminal.get("history", [])),
                        stage_durations_seconds=stage_duration_summary(
                            terminal.get("history", [])
                        ),
                        identity_evidence=(
                            identity_evidence
                            if isinstance(identity_evidence, dict)
                            else {}
                        ),
                        transport_elapsed_seconds=returned.get(
                            "transport_elapsed_seconds"
                        ),
                        return_transport_mode="legacy-collect",
                        returned_at=returned_at or utc_now_iso(),
                    )
                    collected.append(job_id)
                except EngineReturnAlreadyCompactedError as exc:
                    self._record_return_loss(job_id, exc)
                except Exception as exc:
                    self._record_error(job_id, "collect", exc)

            credit = (
                0
                if capacity_error or exchange_used or not runtime_admission_ready
                else self.admission.effective_credit(snapshot)
            )
            admitted: list[str] = list(exchange_admitted)
            for record in self._entries_in_state("pending"):
                if credit <= 0:
                    break
                manual_controller = bool(self.admission.active_controller_lease())
                workflow_ingest = bool(record.get("workflow_ingest", True))
                if manual_controller and workflow_ingest:
                    continue
                if stopped and (not manual_controller or workflow_ingest):
                    continue
                job_id = str(record["engine_job_id"])
                try:
                    admission_attempt = (
                        int(record.get("admission_attempts", 0) or 0) + 1
                    )
                    transport_request_id = (
                        f"engine-admit-{job_id}-transport-{admission_attempt:03d}"
                    )
                    accept_started_at = utc_now_iso()
                    self._patch_entry(
                        job_id,
                        admission_attempts=admission_attempt,
                        last_admission_request_id=transport_request_id,
                    )
                    self._event(
                        "engine_accept_started",
                        **correlation(record),
                        admission_attempt=admission_attempt,
                        transport_request_id=transport_request_id,
                        started_at=accept_started_at,
                    )
                    receipt = self.transport.accept(
                        self.root / str(record["spec_path"]),
                        request_id=transport_request_id,
                        engine_job_id=job_id,
                        payload_root=(
                            (self.root / str(record["payload_root"]))
                            if record.get("payload_root")
                            else None
                        ),
                        wait_timeout_seconds=wait_timeout_seconds,
                    )
                    accepted_at = str(receipt.get("accepted_at") or utc_now_iso())
                    accept_completed_at = utc_now_iso()
                    self._update_entry(job_id, "accepted", accepted_at=accepted_at)
                    self._event(
                        "engine_job_admitted",
                        **correlation(record),
                        accepted_at=accepted_at,
                        admission_attempt=admission_attempt,
                        transport_request_id=transport_request_id,
                        transport_elapsed_seconds=receipt.get(
                            "transport_elapsed_seconds"
                        ),
                        accept_started_at=accept_started_at,
                        accept_completed_at=accept_completed_at,
                    )
                    admitted.append(job_id)
                    if released_records:
                        released = released_records.pop(0)
                        self._record_replenishment(
                            released,
                            replacement_engine_job_id=job_id,
                            replenished_at=accepted_at,
                            replenished_observed_at=accept_completed_at,
                        )
                    credit -= 1
                except EngineAdmissionControlled as exc:
                    self._event(
                        "engine_job_admission_held_by_controller",
                        **correlation(record),
                        controller_lease=self.admission.active_controller_lease(),
                        reason=str(exc),
                    )
                    break
                except Exception as exc:
                    self._record_error(job_id, "accept", exc)
                    # One malformed or temporarily unavailable candidate must not
                    # strand unrelated engine credit. Keep the failed row pending
                    # for a later retry and scan the remaining runnable rows.
                    continue

            self._sync_from_admission()
            ingested: list[str] = []
            finalized: list[str] = []
            for record in self._entries_in_state("returned-awaiting-ingest"):
                if bool(record.get("workflow_ingest", True)):
                    continue
                job_id = str(record["engine_job_id"])
                finalized_at = utc_now_iso()
                self._update_entry(
                    job_id,
                    "canary-complete",
                    ingest_outcome="canary-no-archive",
                    ingested_at=finalized_at,
                )
                self._event(
                    "engine_canary_finalized",
                    **correlation(record),
                    collect_request_id=str(record.get("collect_request_id") or ""),
                    finalized_at=finalized_at,
                )
                finalized.append(job_id)
            if self.result_ingestor is not None:
                for record in self._entries_in_state("returned-awaiting-ingest"):
                    job_id = str(record["engine_job_id"])
                    try:
                        ingest_started_at = utc_now_iso()
                        self._event(
                            "engine_result_ingest_started",
                            **correlation(record),
                            collect_request_id=str(
                                record.get("collect_request_id") or ""
                            ),
                            started_at=ingest_started_at,
                        )
                        result = self.result_ingestor.ingest(record)
                        outcome = str(result.get("outcome") or "workflow-archived")
                        state_name = (
                            outcome
                            if outcome
                            in {
                                "superseded-by-workflow-result",
                                "superseded-by-logical-attempt",
                            }
                            else "workflow-archived"
                        )
                        ingested_at = utc_now_iso()
                        self._update_entry(
                            job_id,
                            state_name,
                            ingest_outcome=outcome,
                            ingested_at=ingested_at,
                            existing_result_evidence=result.get(
                                "existing_result_evidence", {}
                            ),
                        )
                        self._event(
                            "engine_result_ingested",
                            **correlation(record),
                            collect_request_id=str(
                                record.get("collect_request_id") or ""
                            ),
                            ingest_started_at=ingest_started_at,
                            ingested_at=ingested_at,
                            ingest_outcome=outcome,
                        )
                        ingested.append(job_id)
                    except Exception as exc:
                        self._record_error(job_id, "ingest", exc)
            self._event(
                "engine_pump_cycle",
                admitted=admitted,
                collected=collected,
                ingested=ingested,
                finalized=finalized,
                optional_materialized=optional_materialized,
                ack_piggybacked=ack_piggybacked,
                required_ack_piggybacked=required_ack_piggybacked,
                standby_cancelled=standby_cancelled,
                superseded=superseded,
                remaining_credit=credit,
                snapshot_transport_elapsed_seconds=snapshot_transport_elapsed,
            )
            return self.status(
                outcome="ok",
                admitted=admitted,
                collected=collected,
                ingested=ingested,
                finalized=finalized,
                optional_materialized=optional_materialized,
                ack_piggybacked=ack_piggybacked,
                required_ack_piggybacked=required_ack_piggybacked,
                standby_cancelled=standby_cancelled,
                superseded=superseded,
                engine_snapshot=snapshot,
                error=capacity_error,
                exchange_recovered=exchange_recovered,
                exchange_original_error=exchange_original_error,
                exchange_recovery_request_id=exchange_recovery_request_id,
                admission_retries_scheduled=admission_retries_scheduled,
                runtime_generation_gate=runtime_gate,
                runtime_generation_recovered=runtime_recovered,
            )

    def _tick_duplex(self, *, wait_timeout_seconds: int) -> dict[str, Any]:
        cycle_started = time.monotonic()
        with PumpLock(self.lock_path):
            self._merge_enqueue_requests_unlocked()
            phase_started = time.monotonic()
            phase_timing = {
                "lock_wait_seconds": round(phase_started - cycle_started, 3),
            }
            if self.transport is None:
                raise EnginePumpError("engine pump transport is not configured")
            admission = self.admission.read()
            if not admission.get("enabled"):
                return self.status(outcome="disabled")
            phase_timing["initial_admission_read_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )
            phase_started = time.monotonic()
            cached_ready_recovered = self._recover_cached_ready_bundles()
            phase_timing["cached_ready_recovery_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )
            phase_started = time.monotonic()
            optional_materialized = self._recover_optional_materializations()
            phase_timing["optional_materialization_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )
            phase_started = time.monotonic()
            cached_reconciled = self._sync_from_admission()
            phase_timing["initial_reconciliation_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )
            if cached_reconciled:
                self._event(
                    "engine_uncertain_admission_reconciled",
                    engine_job_ids=cached_reconciled,
                    source="trusted-admission-cache",
                )
            phase_started = time.monotonic()
            recovered_capacity_retries = (
                self._recover_retryable_admission_failures()
            )
            phase_timing["capacity_recovery_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )
            phase_started = time.monotonic()
            if recovered_capacity_retries:
                self._event(
                    "engine_remote_capacity_retries_recovered",
                    engine_job_ids=recovered_capacity_retries,
                    source="pump-state-upgrade",
                )
            admission = self.admission.read()
            superseded = self._suppress_pending_with_workflow_result()
            retired_exchanges = self._retire_superseded_uncertain_exchanges()
            stopped = stop_requested(self.root)
            if stopped or admission.get("draining"):
                self._request_all_standby_cancellations_unlocked(
                    reason=(
                        "workflow stop fence cancelled remote standby"
                        if stopped
                        else "engine drain cancelled remote standby"
                    )
                )
            phase_timing["planning_preflight_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )

            phase_started = time.monotonic()
            cycle = cycle_token()
            state = self.read()
            active_exchanges = [
                dict(item)
                for item in state.get("exchanges", {}).values()
                if isinstance(item, dict)
                and str(item.get("state") or "")
                in {"publishing", "published", "uncertain"}
            ]
            runtime_blocking_exchanges = [
                item
                for item in active_exchanges
                if runtime_sync_blocking_exchange(item)
            ]
            runtime_gate = self._ensure_remote_runtime_generation(
                admission=admission,
                active_exchanges=runtime_blocking_exchanges,
                wait_timeout_seconds=wait_timeout_seconds,
                allow_sync=not stopped,
            )
            runtime_admission_ready = runtime_gate["state"] == "ready"
            runtime_recovered = (
                self._recover_runtime_generation_admission_failures()
                if runtime_admission_ready
                else []
            )
            if runtime_recovered:
                admission = self.admission.read()
            publish_journal = self._duplex_retry_exchange(
                active_exchanges,
                allow_admission=runtime_admission_ready,
            )
            if publish_journal is None:
                publish_journal = self._duplex_prepare_exchange(
                    cycle=cycle,
                    admission=admission,
                    active_exchanges=active_exchanges,
                    stopped=stopped,
                    allow_admission=runtime_admission_ready,
                    allow_watch=(
                        runtime_admission_ready
                        or not bool(runtime_gate.get("remote_quiescent"))
                    ),
                )

            poll_payloads = [
                self._duplex_poll_payload(item) for item in active_exchanges
            ]
            polled: dict[str, dict[str, Any] | None] = {}
            poll_error = ""
            publish_result: dict[str, Any] | None = None
            publish_error = ""
            phase_timing["exchange_planning_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )
            phase_started = time.monotonic()
            if poll_payloads or publish_journal is not None:
                with ThreadPoolExecutor(
                    max_workers=2,
                    thread_name_prefix="engine-duplex",
                ) as executor:
                    poll_future = (
                        executor.submit(
                            self.transport.poll_exchanges,
                            poll_payloads,
                            wait_timeout_seconds=wait_timeout_seconds,
                        )
                        if poll_payloads
                        else None
                    )
                    publish_future = (
                        executor.submit(
                            self._duplex_publish_exchange,
                            publish_journal,
                            wait_timeout_seconds=wait_timeout_seconds,
                        )
                        if publish_journal is not None
                        else None
                    )
                    if poll_future is not None:
                        try:
                            polled = poll_future.result()
                        except Exception as exc:
                            poll_error = str(exc)
                    if publish_future is not None:
                        try:
                            publish_result = publish_future.result()
                        except Exception as exc:
                            publish_error = str(exc)
            phase_timing["transport_concurrent_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )

            phase_started = time.monotonic()
            if poll_error:
                self._event(
                    "engine_duplex_result_poll_failed",
                    request_ids=[
                        str(item.get("request_id") or "")
                        for item in active_exchanges
                    ],
                    error=poll_error,
                )
                for journal in active_exchanges:
                    self._patch_exchange(
                        str(journal.get("request_id") or ""),
                        last_poll_at=utc_now_iso(),
                        last_poll_error=poll_error,
                    )
            else:
                for journal in active_exchanges:
                    request_id = str(journal.get("request_id") or "")
                    remote = polled.get(request_id)
                    if remote is None:
                        self._patch_exchange(
                            request_id,
                            last_poll_at=utc_now_iso(),
                            last_poll_error="",
                        )

            if publish_journal is not None:
                request_id = str(publish_journal.get("request_id") or "")
                attempts = int(publish_journal.get("publish_attempts", 0) or 0) + 1
                if publish_error:
                    self._patch_exchange(
                        request_id,
                        state="uncertain",
                        publish_attempts=attempts,
                        last_publish_at=utc_now_iso(),
                        last_publish_error=publish_error,
                    )
                    self._event(
                        "engine_duplex_exchange_publish_uncertain",
                        request_id=request_id,
                        publish_attempt=attempts,
                        error=publish_error,
                    )
                elif isinstance(publish_result, dict):
                    self._patch_exchange(
                        request_id,
                        state="published",
                        publish_attempts=attempts,
                        published_at=str(
                            publish_result.get("published_at") or utc_now_iso()
                        ),
                        last_publish_at=utc_now_iso(),
                        last_publish_error="",
                        transport_elapsed_seconds=publish_result.get(
                            "transport_elapsed_seconds"
                        ),
                        local_transport_timeline=publish_result.get(
                            "local_transport_timeline", {}
                        ),
                    )
                    self._event(
                        "engine_duplex_exchange_published",
                        request_id=request_id,
                        exchange_kind=str(
                            publish_journal.get("exchange_kind") or ""
                        ),
                        job_ids=[
                            str(item.get("engine_job_id") or "")
                            for item in publish_journal.get("jobs", [])
                            if isinstance(item, dict)
                        ],
                        publish_attempt=attempts,
                        transport_elapsed_seconds=publish_result.get(
                            "transport_elapsed_seconds"
                        ),
                        local_transport_timeline=publish_result.get(
                            "local_transport_timeline", {}
                        ),
                    )
            phase_timing["transport_state_update_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )

            phase_started = time.monotonic()
            admitted: list[str] = []
            collected = list(cached_ready_recovered)
            ack_piggybacked: list[str] = []
            required_ack_piggybacked: list[str] = []
            standby_cancelled: list[str] = []
            exchange_failures: list[dict[str, Any]] = []
            latest_snapshot: dict[str, Any] = {}
            for journal in active_exchanges:
                request_id = str(journal.get("request_id") or "")
                remote = polled.get(request_id)
                if not isinstance(remote, dict):
                    continue
                remote_error = str(remote.get("duplex_exchange_error") or "")
                if remote_error:
                    terminal = bool(remote.get("duplex_exchange_terminal"))
                    failure = {
                        "request_id": request_id,
                        "terminal": terminal,
                        "error": remote_error,
                        "remote_status": remote.get("remote_status", {}),
                    }
                    exchange_failures.append(failure)
                    self._patch_exchange(
                        request_id,
                        state="failed" if terminal else str(
                            journal.get("state") or "published"
                        ),
                        completed_at=utc_now_iso() if terminal else "",
                        last_poll_at=utc_now_iso(),
                        last_poll_error=remote_error,
                        remote_status=remote.get("remote_status", {}),
                    )
                    self._event(
                        (
                            "engine_duplex_exchange_failed"
                            if terminal
                            else "engine_duplex_exchange_materialization_deferred"
                        ),
                        **failure,
                    )
                    continue
                applied = self._duplex_apply_exchange(
                    journal,
                    remote,
                )
                admitted.extend(applied["admitted"])
                collected.extend(
                    item for item in applied["collected"] if item not in collected
                )
                ack_piggybacked.extend(applied["ack_piggybacked"])
                required_ack_piggybacked.extend(
                    applied["required_ack_piggybacked"]
                )
                standby_cancelled.extend(applied["standby_cancelled"])
                latest_snapshot = applied["snapshot"]
                self._patch_exchange(
                    request_id,
                    state="completed",
                    completed_at=utc_now_iso(),
                    last_poll_at=utc_now_iso(),
                    last_poll_error="",
                )
            phase_timing["exchange_apply_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )

            phase_started = time.monotonic()
            self._sync_from_admission()
            finalized, ingested = self._duplex_finalize_results()
            self._prune_exchange_journal()
            phase_timing["finalization_seconds"] = round(
                time.monotonic() - phase_started,
                3,
            )
            phase_timing["cycle_elapsed_seconds"] = round(
                time.monotonic() - cycle_started,
                3,
            )
            self._event(
                "engine_pump_cycle",
                transport_mode="duplex-exchange",
                admitted=admitted,
                collected=collected,
                ingested=ingested,
                finalized=finalized,
                optional_materialized=optional_materialized,
                ack_piggybacked=ack_piggybacked,
                required_ack_piggybacked=required_ack_piggybacked,
                standby_cancelled=standby_cancelled,
                superseded=superseded,
                retired_exchanges=retired_exchanges,
                publish_request_id=(
                    str(publish_journal.get("request_id") or "")
                    if publish_journal is not None
                    else ""
                ),
                exchange_failures=exchange_failures,
                poll_error=poll_error,
                publish_error=publish_error,
                phase_timing=phase_timing,
                runtime_generation_gate=runtime_gate,
                runtime_generation_recovered=runtime_recovered,
            )
            return self.status(
                outcome=(
                    "duplex-poll-deferred"
                    if poll_error
                    else "duplex-exchange-failed"
                    if any(item["terminal"] for item in exchange_failures)
                    else "duplex-materialization-deferred"
                    if exchange_failures
                    else "ok"
                ),
                transport_mode="duplex-exchange",
                admitted=admitted,
                collected=collected,
                ingested=ingested,
                finalized=finalized,
                optional_materialized=optional_materialized,
                ack_piggybacked=ack_piggybacked,
                required_ack_piggybacked=required_ack_piggybacked,
                standby_cancelled=standby_cancelled,
                superseded=superseded,
                retired_exchanges=retired_exchanges,
                engine_snapshot=latest_snapshot,
                exchange_failures=exchange_failures,
                publish_error=publish_error,
                poll_error=poll_error,
                runtime_generation_gate=runtime_gate,
                runtime_generation_recovered=runtime_recovered,
            )

    def _duplex_prepare_exchange(
        self,
        *,
        cycle: str,
        admission: dict[str, Any],
        active_exchanges: list[dict[str, Any]],
        stopped: bool,
        allow_admission: bool = True,
        allow_watch: bool = True,
    ) -> dict[str, Any] | None:
        inflight_acknowledgements = {
            (
                str(item.get("engine_job_id") or ""),
                str(item.get("receipt_id") or ""),
            )
            for exchange in active_exchanges
            for item in exchange.get("acknowledgements", [])
            if isinstance(item, dict)
        }
        inflight_required_acknowledgements = {
            (
                str(item.get("engine_job_id") or ""),
                str(item.get("receipt_id") or ""),
            )
            for exchange in active_exchanges
            for item in exchange.get("required_acknowledgements", [])
            if isinstance(item, dict)
        }
        inflight_cancellations = {
            str(item.get("engine_job_id") or "")
            for exchange in active_exchanges
            for item in exchange.get("standby_cancellations", [])
            if isinstance(item, dict)
        }
        acknowledgements = [
            item
            for item in self._pending_remote_acknowledgements()
            if (item["engine_job_id"], item["receipt_id"])
            not in inflight_acknowledgements
        ]
        required_acknowledgements = [
            item
            for item in self._pending_required_acknowledgements()
            if (item["engine_job_id"], item["receipt_id"])
            not in inflight_required_acknowledgements
        ]
        standby_cancellations = [
            item
            for item in self._pending_standby_cancellations()
            if item["engine_job_id"] not in inflight_cancellations
        ]
        desired_inflight = max(1, int(admission.get("target_inflight", 1) or 1))
        last_snapshot = admission.get("last_engine_snapshot")
        has_fresh_snapshot = (
            isinstance(last_snapshot, dict)
            and bool(last_snapshot)
            and not snapshot_stale(
                last_snapshot,
                REMOTE_CREDIT_SNAPSHOT_MAX_AGE_SECONDS,
            )
        )
        local_credit = self.admission.local_credit(admission)
        credit = (
            self.admission.effective_credit(
                last_snapshot,
                max_snapshot_age_seconds=REMOTE_CREDIT_SNAPSHOT_MAX_AGE_SECONDS,
            )
            if has_fresh_snapshot
            else local_credit
        )
        manual_controller = bool(self.admission.active_controller_lease())
        jobs: list[dict[str, Any]] = []
        attempts: dict[str, dict[str, Any]] = {}
        selected_ids: set[str] = set()
        pending_records = (
            [
                item
                for item in self._entries_in_state("pending")
                if self._payload_is_transportable(item)
            ]
            if allow_admission
            else []
        )
        for record in pending_records:
            if credit <= 0:
                break
            workflow_ingest = bool(record.get("workflow_ingest", True))
            if manual_controller and workflow_ingest:
                continue
            if stopped and (not manual_controller or workflow_ingest):
                continue
            job_id = str(record["engine_job_id"])
            attempt = int(record.get("admission_attempts", 0) or 0) + 1
            started_at = utc_now_iso()
            request_id = f"engine-exchange-{cycle}"
            self._patch_entry(
                job_id,
                state="admitting",
                admission_attempts=attempt,
                last_admission_request_id=request_id,
            )
            self._event(
                "engine_accept_started",
                **correlation(record),
                admission_attempt=attempt,
                transport_request_id=request_id,
                started_at=started_at,
                transport_mode="duplex-exchange",
            )
            jobs.append(self._duplex_job_record(record, admission_mode="accept"))
            attempts[job_id] = {
                "attempt": attempt,
                "request_id": request_id,
                "started_at": started_at,
                "admission_mode": "accept",
            }
            selected_ids.add(job_id)
            credit -= 1

        standby_target = max(
            0, int(self.capacity_overrides.get("standby_slots", 0) or 0)
        )
        standby_active = sum(
            1
            for item in admission.get("jobs", {}).values()
            if isinstance(item, dict)
            and item.get("state") in {"staging-standby", "standby"}
        )
        standby_credit = max(0, standby_target - standby_active)
        if has_fresh_snapshot:
            standby_credit = min(
                standby_credit,
                max(0, int(last_snapshot.get("standby_credit", 0) or 0)),
            )
        for record in pending_records:
            if standby_credit <= 0:
                break
            job_id = str(record["engine_job_id"])
            if job_id in selected_ids:
                continue
            workflow_ingest = bool(record.get("workflow_ingest", True))
            if manual_controller and workflow_ingest:
                continue
            if stopped and (not manual_controller or workflow_ingest):
                continue
            attempt = int(record.get("admission_attempts", 0) or 0) + 1
            started_at = utc_now_iso()
            request_id = f"engine-exchange-{cycle}"
            self._patch_entry(
                job_id,
                state="staging-standby",
                admission_attempts=attempt,
                last_admission_request_id=request_id,
            )
            self._event(
                "engine_standby_started",
                **correlation(record),
                admission_attempt=attempt,
                transport_request_id=request_id,
                started_at=started_at,
                transport_mode="duplex-exchange",
            )
            jobs.append(self._duplex_job_record(record, admission_mode="standby"))
            attempts[job_id] = {
                "attempt": attempt,
                "request_id": request_id,
                "started_at": started_at,
                "admission_mode": "standby",
            }
            standby_credit -= 1

        has_controls = bool(
            acknowledgements
            or required_acknowledgements
            or standby_cancellations
        )
        active_watch = any(
            str(item.get("exchange_kind") or "") == "watch"
            for item in active_exchanges
        )
        admission_snapshot = self.admission.snapshot()
        has_remote_work = any(
            int(admission_snapshot.get("state_counts", {}).get(name, 0) or 0) > 0
            for name in ("admitting", "accepted", "running", "return-ready")
        )
        if isinstance(last_snapshot, dict):
            has_remote_work = has_remote_work or any(
                int(last_snapshot.get(name, 0) or 0) > 0
                for name in (
                    "accepted_nonterminal",
                    "queued_nonterminal",
                    "active_nonterminal",
                    "standby_count",
                    "return_ready_count",
                )
            )
        if (
            not jobs
            and not has_controls
            and (
                not allow_watch
                or active_watch
                or not has_remote_work
            )
        ):
            return None
        request_id = f"engine-exchange-{cycle}"
        exchange_kind = (
            "admission"
            if jobs
            else "control"
            if has_controls
            else "watch"
        )
        now = utc_now_iso()
        journal = {
            "request_id": request_id,
            "state": "publishing",
            "exchange_kind": exchange_kind,
            "jobs": jobs,
            "attempts": attempts,
            "acknowledgements": acknowledgements,
            "required_acknowledgements": required_acknowledgements,
            "standby_cancellations": standby_cancellations,
            "created_at": now,
            "updated_at": now,
            "publish_attempts": 0,
            "wait_ready_seconds": (
                None if exchange_kind == "watch" else 0.0
            ),
        }
        state = self.read()
        state.setdefault("exchanges", {})[request_id] = journal
        state["updated_at"] = now
        self._write(state)
        self._event(
            "engine_duplex_exchange_created",
            request_id=request_id,
            exchange_kind=exchange_kind,
            job_ids=[
                str(item.get("engine_job_id") or "") for item in jobs
            ],
            acknowledgement_count=len(acknowledgements),
            required_acknowledgement_count=len(required_acknowledgements),
            standby_cancellation_count=len(standby_cancellations),
        )
        return journal

    @staticmethod
    def _duplex_job_record(
        record: dict[str, Any],
        *,
        admission_mode: str,
    ) -> dict[str, Any]:
        return {
            "engine_job_id": str(record.get("engine_job_id") or ""),
            "admission_mode": admission_mode,
            "spec_path": str(record.get("spec_path") or ""),
            "payload_root": str(record.get("payload_root") or ""),
        }

    def _duplex_transport_jobs(
        self,
        journal: dict[str, Any],
    ) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        for item in journal.get("jobs", []):
            if not isinstance(item, dict):
                continue
            jobs.append(
                {
                    **item,
                    "spec_path": str(
                        self.root / str(item.get("spec_path") or "")
                    ),
                    "payload_root": (
                        str(self.root / str(item.get("payload_root") or ""))
                        if item.get("payload_root")
                        else ""
                    ),
                }
            )
        return jobs

    def _duplex_publish_exchange(
        self,
        journal: dict[str, Any],
        *,
        wait_timeout_seconds: int,
    ) -> dict[str, Any]:
        admission = self.admission.read()
        return self.transport.publish_exchange(
            self._duplex_transport_jobs(journal),
            request_id=str(journal.get("request_id") or ""),
            acknowledgements=list(journal.get("acknowledgements", [])),
            required_acknowledgements=list(
                journal.get("required_acknowledgements", [])
            ),
            standby_cancellations=list(
                journal.get("standby_cancellations", [])
            ),
            max_inflight=max(
                1, int(admission.get("target_inflight", 1) or 1)
            ),
            draining=bool(admission.get("draining")),
            **self.capacity_overrides,
            wait_timeout_seconds=wait_timeout_seconds,
            wait_ready_seconds=journal.get("wait_ready_seconds"),
        )

    def _duplex_poll_payload(
        self,
        journal: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            **journal,
            "jobs": self._duplex_transport_jobs(journal),
        }

    @staticmethod
    def _duplex_retry_exchange(
        active_exchanges: list[dict[str, Any]],
        *,
        allow_admission: bool = True,
    ) -> dict[str, Any] | None:
        now = datetime.now().astimezone()
        for journal in sorted(
            active_exchanges,
            key=lambda item: str(item.get("created_at") or ""),
        ):
            if (
                not allow_admission
                and str(journal.get("exchange_kind") or "") == "admission"
            ):
                continue
            attempts = int(journal.get("publish_attempts", 0) or 0)
            if attempts >= 3:
                continue
            last = parse_datetime(
                str(
                    journal.get("last_publish_at")
                    or journal.get("created_at")
                    or ""
                )
            )
            if last is None:
                return journal
            age = max(0.0, (now - last).total_seconds())
            state = str(journal.get("state") or "")
            if state not in {"publishing", "uncertain"}:
                continue
            if age >= 20.0:
                return journal
        return None

    def _duplex_apply_exchange(
        self,
        journal: dict[str, Any],
        remote: dict[str, Any],
    ) -> dict[str, Any]:
        request_id = str(journal.get("request_id") or "")
        completed_at = utc_now_iso()
        snapshot = (
            dict(remote.get("engine_snapshot", {}))
            if isinstance(remote.get("engine_snapshot"), dict)
            else {}
        )
        exchange_timeline = (
            dict(remote.get("exchange_timeline", {}))
            if isinstance(remote.get("exchange_timeline"), dict)
            else {}
        )
        local_transport_timeline = (
            dict(remote.get("local_transport_timeline", {}))
            if isinstance(remote.get("local_transport_timeline"), dict)
            else {}
        )
        transport_elapsed = remote.get("transport_elapsed_seconds")
        self._event(
            "engine_exchange_completed",
            snapshot_request_id=request_id,
            completed_at=completed_at,
            transport_mode="duplex-exchange",
            transport_elapsed_seconds=transport_elapsed,
            engine_observed_at=str(snapshot.get("observed_at") or ""),
            ready_job_ids=[
                str(item.get("engine_job_id") or "")
                for item in remote.get("ready_bundles", [])
                if isinstance(item, dict)
            ],
            b_request_observed_at=str(
                exchange_timeline.get("b_request_observed_at") or ""
            ),
            b_return_export_started_at=str(
                exchange_timeline.get("return_export_started_at") or ""
            ),
            b_return_export_finished_at=str(
                exchange_timeline.get("return_export_finished_at") or ""
            ),
            b_exchange_finished_at=str(
                exchange_timeline.get("b_exchange_finished_at") or ""
            ),
            local_transport_timeline=local_transport_timeline,
            result_query=remote.get("result_query", {}),
        )
        attempts = {
            str(key): value
            for key, value in journal.get("attempts", {}).items()
            if isinstance(value, dict)
        }
        admitted: list[str] = []
        released_records = self._unreplenished_credit_records()
        for receipt in remote.get("accepted_receipts", []):
            if not isinstance(receipt, dict):
                continue
            job_id = str(receipt.get("engine_job_id") or "")
            attempt = attempts.get(job_id)
            record = self.read().get("entries", {}).get(job_id)
            if not isinstance(attempt, dict) or not isinstance(record, dict):
                continue
            accepted_at = str(receipt.get("accepted_at") or completed_at)
            if admission_receipt_can_advance(
                str(record.get("state") or ""), "accepted"
            ):
                self._update_entry(job_id, "accepted", accepted_at=accepted_at)
            else:
                self._event(
                    "engine_late_admission_receipt_ignored",
                    **correlation(record),
                    receipt_kind="accepted",
                    current_state=str(record.get("state") or ""),
                    transport_request_id=request_id,
                )
            self._event(
                "engine_job_admitted",
                **correlation(record),
                accepted_at=accepted_at,
                admission_attempt=int(attempt.get("attempt", 0) or 0),
                transport_request_id=request_id,
                transport_elapsed_seconds=transport_elapsed,
                accept_started_at=str(attempt.get("started_at") or ""),
                accept_completed_at=completed_at,
                transport_mode="duplex-exchange",
            )
            admitted.append(job_id)
            if released_records:
                self._record_replenishment(
                    released_records.pop(0),
                    replacement_engine_job_id=job_id,
                    replenished_at=accepted_at,
                    replenished_observed_at=completed_at,
                )
        for receipt in remote.get("standby_receipts", []):
            if not isinstance(receipt, dict):
                continue
            job_id = str(receipt.get("engine_job_id") or "")
            attempt = attempts.get(job_id)
            record = self.read().get("entries", {}).get(job_id)
            if not isinstance(attempt, dict) or not isinstance(record, dict):
                continue
            if not admission_receipt_can_advance(
                str(record.get("state") or ""), "standby"
            ):
                self._event(
                    "engine_late_admission_receipt_ignored",
                    **correlation(record),
                    receipt_kind="standby",
                    current_state=str(record.get("state") or ""),
                    transport_request_id=request_id,
                )
                continue
            staged_at = str(receipt.get("staged_at") or completed_at)
            self._update_entry(job_id, "standby", staged_at=staged_at)
            self._event(
                "engine_job_staged_standby",
                **correlation(record),
                staged_at=staged_at,
                admission_attempt=int(attempt.get("attempt", 0) or 0),
                transport_request_id=request_id,
                standby_started_at=str(attempt.get("started_at") or ""),
                transport_mode="duplex-exchange",
            )
        rejection_errors = remote.get("rejection_errors", {})
        for job_id in remote.get("rejected_jobs", []):
            job_id = str(job_id)
            attempt = attempts.get(job_id, {})
            reason = (
                str(rejection_errors.get(job_id) or "")
                if isinstance(rejection_errors, dict)
                else ""
            )
            mode = str(attempt.get("admission_mode") or "accept")
            self._fail_remote_admission(
                job_id,
                f"exchange-{mode}",
                f"remote engine rejected duplex {mode}"
                + (f": {reason}" if reason else ""),
            )
        standby_cancelled = self._record_standby_cancellation_receipts(
            remote.get("standby_cancellation_receipts", []),
            request_id=request_id,
        )
        required_ack_piggybacked = self._record_required_ack_piggybacks(
            remote.get("required_acknowledgement_receipts", []),
            snapshot_request_id=request_id,
        )
        exchange_attempts = {
            job_id: (
                int(item.get("attempt", 0) or 0),
                request_id,
                str(item.get("started_at") or ""),
                str(item.get("admission_mode") or "accept"),
            )
            for job_id, item in attempts.items()
        }
        self._release_unobserved_admissions(
            snapshot,
            exchange_attempts=exchange_attempts,
            exchange_request_id=request_id,
            recovery_request_id="",
            original_error="",
        )
        self._sync_from_admission()
        admitted.extend(self._observe_standby_promotions(completed_at))
        ack_piggybacked = self._record_ack_piggybacks(
            remote.get("acknowledgements_sent", []),
            snapshot_request_id=request_id,
        )
        self._confirm_remote_acks(snapshot)
        collected = self._consume_ready_bundles(
            remote.get("ready_bundles", []),
            snapshot_request_id=request_id,
            transport_elapsed_seconds=transport_elapsed,
            snapshot_started_at=str(journal.get("created_at") or ""),
            snapshot_completed_at=completed_at,
            engine_observed_at=str(snapshot.get("observed_at") or ""),
            exchange_timeline=exchange_timeline,
            local_transport_timeline=local_transport_timeline,
        )
        return {
            "admitted": admitted,
            "collected": collected,
            "ack_piggybacked": ack_piggybacked,
            "required_ack_piggybacked": required_ack_piggybacked,
            "standby_cancelled": standby_cancelled,
            "snapshot": snapshot,
        }

    def _duplex_finalize_results(self) -> tuple[list[str], list[str]]:
        finalized: list[str] = []
        ingested: list[str] = []
        for record in self._entries_in_state("returned"):
            if (
                bool(record.get("workflow_ingest", True))
                or str(record.get("ingest_outcome") or "") != "canary-no-archive"
                or str(record.get("engine_terminal_state") or "")
                not in {"completed", "failed"}
            ):
                continue
            job_id = str(record["engine_job_id"])
            recovered_at = utc_now_iso()
            self._update_entry(
                job_id,
                "canary-complete",
                ingested_at=str(record.get("ingested_at") or recovered_at),
            )
            self._event(
                "engine_canary_state_recovered",
                **correlation(record),
                previous_state="returned",
                recovered_at=recovered_at,
            )
            finalized.append(job_id)
        for record in self._entries_in_state("returned-awaiting-ingest"):
            if bool(record.get("workflow_ingest", True)):
                continue
            job_id = str(record["engine_job_id"])
            finalized_at = utc_now_iso()
            self._update_entry(
                job_id,
                "canary-complete",
                ingest_outcome="canary-no-archive",
                ingested_at=finalized_at,
            )
            self._event(
                "engine_canary_finalized",
                **correlation(record),
                collect_request_id=str(record.get("collect_request_id") or ""),
                finalized_at=finalized_at,
            )
            finalized.append(job_id)
        if self.result_ingestor is None:
            return finalized, ingested
        for record in self._entries_in_state("returned-awaiting-ingest"):
            job_id = str(record["engine_job_id"])
            try:
                ingest_started_at = utc_now_iso()
                self._event(
                    "engine_result_ingest_started",
                    **correlation(record),
                    collect_request_id=str(record.get("collect_request_id") or ""),
                    started_at=ingest_started_at,
                )
                result = self.result_ingestor.ingest(record)
                outcome = str(result.get("outcome") or "workflow-archived")
                state_name = (
                    outcome
                    if outcome
                    in {
                        "superseded-by-workflow-result",
                        "superseded-by-logical-attempt",
                    }
                    else "workflow-archived"
                )
                ingested_at = utc_now_iso()
                self._update_entry(
                    job_id,
                    state_name,
                    ingest_outcome=outcome,
                    ingested_at=ingested_at,
                    existing_result_evidence=result.get(
                        "existing_result_evidence", {}
                    ),
                )
                self._event(
                    "engine_result_ingested",
                    **correlation(record),
                    collect_request_id=str(record.get("collect_request_id") or ""),
                    ingest_started_at=ingest_started_at,
                    ingested_at=ingested_at,
                    ingest_outcome=outcome,
                )
                ingested.append(job_id)
            except Exception as exc:
                self._record_error(job_id, "ingest", exc)
        return finalized, ingested

    def _patch_exchange(self, request_id: str, **fields: Any) -> None:
        if not request_id:
            return
        state = self.read()
        exchange = state.setdefault("exchanges", {}).get(request_id)
        if not isinstance(exchange, dict):
            return
        exchange.update(fields)
        exchange["updated_at"] = utc_now_iso()
        state["updated_at"] = exchange["updated_at"]
        self._write(state)

    def _retire_superseded_uncertain_exchanges(self) -> list[str]:
        state = self.read()
        entries = state.get("entries", {})
        exchanges = state.get("exchanges", {})
        if not isinstance(entries, dict) or not isinstance(exchanges, dict):
            return []

        retired: list[tuple[str, list[str], str]] = []
        for request_id, exchange in exchanges.items():
            if (
                not isinstance(exchange, dict)
                or str(exchange.get("state") or "")
                not in {"uncertain", "published"}
            ):
                continue
            publish_attempts = (
                int(exchange.get("publish_attempts", 0) or 0)
            )
            publish_error = (
                str(exchange.get("last_publish_error") or "")
            )
            immutable_identity_collision = (
                "immutable append request identity collision" in publish_error.lower()
            )
            local_publish_failure = definitely_local_exchange_publish_failure(
                publish_error
            )
            jobs = [
                item
                for item in exchange.get("jobs", [])
                if isinstance(item, dict) and item.get("engine_job_id")
            ]
            job_ids = [str(item["engine_job_id"]) for item in jobs]
            recovered_without_remote_admission = bool(job_ids) and all(
                isinstance(entries.get(job_id), dict)
                and str(entries[job_id].get("state") or "") == "pending"
                and str(
                    entries[job_id].get(
                        "last_admission_recovery_request_id"
                    )
                    or ""
                )
                == str(request_id)
                and str(
                    entries[job_id].get(
                        "last_admission_recovery_outcome"
                    )
                    or ""
                )
                == "remote-not-observed-retry"
                for job_id in job_ids
            )
            failed_before_remote_publish = bool(job_ids) and (
                definitely_local_exchange_publish_failure(publish_error)
                and all(
                    isinstance(entries.get(job_id), dict)
                    and str(entries[job_id].get("state") or "") == "pending"
                    and str(
                        entries[job_id].get("last_admission_request_id")
                        or ""
                    )
                    == str(request_id)
                    for job_id in job_ids
                )
            )
            if (
                not recovered_without_remote_admission
                and not failed_before_remote_publish
                and not local_publish_failure
                and (
                    publish_attempts < 3
                    and not immutable_identity_collision
                )
            ):
                continue
            exchange_kind = str(exchange.get("exchange_kind") or "")
            if exchange_kind == "watch":
                has_controls = any(
                    exchange.get(field)
                    for field in (
                        "jobs",
                        "acknowledgements",
                        "required_acknowledgements",
                        "standby_cancellations",
                    )
                )
                exchange_created_at = self._exchange_timestamp(
                    exchange.get("created_at")
                )
                newer_completed_exchange = any(
                    other_request_id != request_id
                    and isinstance(other_exchange, dict)
                    and str(other_exchange.get("state") or "") == "completed"
                    and self._exchange_timestamp(
                        other_exchange.get("created_at")
                    )
                    > exchange_created_at
                    for other_request_id, other_exchange in exchanges.items()
                )
                if (
                    not has_controls
                    and (
                        local_publish_failure
                        or publish_attempts >= 3
                        or (
                            immutable_identity_collision
                            and newer_completed_exchange
                        )
                    )
                ):
                    reason = (
                        "control-free watch identity collided after a newer "
                        "exchange completed; the advanced endpoint cursor "
                        "proves a fresh watch may safely replace it"
                        if immutable_identity_collision
                        else (
                            "control-free watch exhausted its publish retries; "
                            "it carries no jobs or acknowledgements, so a fresh "
                            "observation may safely replace it"
                            if publish_attempts >= 3
                            else (
                                "control-free watch failed before remote "
                                "publication; a fresh watch may safely replace it"
                            )
                        )
                    )
                    retired.append(
                        (
                            str(request_id),
                            [],
                            reason,
                        )
                    )
                continue
            if exchange_kind != "admission":
                continue
            if exchange.get("standby_cancellations"):
                continue
            if not self._uncertain_exchange_acks_are_replayable(
                exchange,
                entries,
            ):
                continue
            if not jobs:
                continue
            resolved = True
            for job_id in job_ids:
                record = entries.get(job_id)
                if not isinstance(record, dict):
                    continue
                current_request_id = str(
                    record.get("last_admission_request_id") or ""
                )
                current_state = str(record.get("state") or "")
                superseded_by_newer_request = bool(
                    current_request_id and current_request_id != request_id
                )
                progressed_beyond_admission = current_state not in {
                    "pending",
                    "admitting",
                    "staging-standby",
                }
                recovered_for_retry = bool(
                    current_state == "pending"
                    and str(
                        record.get("last_admission_recovery_request_id")
                        or ""
                    )
                    == str(request_id)
                    and str(
                        record.get("last_admission_recovery_outcome")
                        or ""
                    )
                    == "remote-not-observed-retry"
                )
                local_publish_failed = bool(
                    current_state == "pending"
                    and current_request_id == str(request_id)
                    and definitely_local_exchange_publish_failure(
                        publish_error
                    )
                )
                if (
                    not superseded_by_newer_request
                    and not progressed_beyond_admission
                    and not recovered_for_retry
                    and not local_publish_failed
                ):
                    resolved = False
                    break
            if not resolved:
                continue
            if failed_before_remote_publish:
                reason = (
                    "local payload publication failed before remote delivery; "
                    "all jobs remain pending for an idempotent retry"
                )
            elif recovered_without_remote_admission:
                reason = (
                    "trusted remote snapshot proved the admission was not "
                    "published; all jobs were released for an idempotent retry"
                )
            else:
                reason = (
                    "all admission jobs were taken over by a newer request "
                    "or progressed beyond admission; any unconfirmed "
                    "acknowledgements remain queued for idempotent replay"
                )
            retired.append(
                (
                    str(request_id),
                    job_ids,
                    reason,
                )
            )

        if not retired:
            return []
        retired_at = utc_now_iso()
        for request_id, job_ids, reason in retired:
            exchange = exchanges[request_id]
            exchange.update(
                {
                    "state": "superseded",
                    "completed_at": retired_at,
                    "updated_at": retired_at,
                    "superseded_at": retired_at,
                    "superseded_job_ids": job_ids,
                    "superseded_reason": reason,
                }
            )
        state["updated_at"] = retired_at
        self._write(state)
        for request_id, job_ids, _reason in retired:
            self._event(
                "engine_duplex_exchange_superseded",
                request_id=request_id,
                engine_job_ids=job_ids,
                reason=exchanges[request_id]["superseded_reason"],
                superseded_at=retired_at,
                acknowledgement_count=len(
                    exchanges[request_id].get("acknowledgements", [])
                ),
                required_acknowledgement_count=len(
                    exchanges[request_id].get("required_acknowledgements", [])
                ),
            )
        return [item[0] for item in retired]

    @staticmethod
    def _exchange_timestamp(value: Any) -> float:
        try:
            return datetime.fromisoformat(
                str(value or "").replace("Z", "+00:00")
            ).timestamp()
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _uncertain_exchange_acks_are_replayable(
        exchange: dict[str, Any],
        entries: dict[str, Any],
    ) -> bool:
        contracts = (
            (
                "acknowledgements",
                "return_receipt_id",
                "remote_ack_state",
                {"pending", "dispatched", "piggybacked", "confirmed"},
            ),
            (
                "required_acknowledgements",
                "required_return_receipt_id",
                "remote_required_ack_state",
                {"pending", "piggybacked", "confirmed"},
            ),
        )
        for field, receipt_field, state_field, replayable_states in contracts:
            for acknowledgement in exchange.get(field, []):
                if not isinstance(acknowledgement, dict):
                    return False
                job_id = str(acknowledgement.get("engine_job_id") or "")
                receipt_id = str(acknowledgement.get("receipt_id") or "")
                record = entries.get(job_id)
                if (
                    not job_id
                    or not receipt_id
                    or not isinstance(record, dict)
                    or str(record.get(receipt_field) or "") != receipt_id
                    or str(record.get(state_field) or "") not in replayable_states
                ):
                    return False
        return True

    def _prune_exchange_journal(self, *, keep_completed: int = 32) -> None:
        state = self.read()
        exchanges = state.get("exchanges", {})
        terminal = sorted(
            (
                (request_id, item)
                for request_id, item in exchanges.items()
                if isinstance(item, dict)
                and str(item.get("state") or "")
                in {"completed", "superseded", "failed"}
            ),
            key=lambda pair: str(
                pair[1].get("completed_at")
                or pair[1].get("updated_at")
                or ""
            ),
            reverse=True,
        )
        removed = [
            request_id for request_id, _item in terminal[keep_completed:]
        ]
        if not removed:
            return
        for request_id in removed:
            exchanges.pop(request_id, None)
        state["updated_at"] = utc_now_iso()
        self._write(state)

    def _recover_exchange_receipts(
        self,
        snapshot: object,
        exchange_attempts: dict[str, tuple[int, str, str, str]],
    ) -> dict[str, list[dict[str, Any]]]:
        if not isinstance(snapshot, dict):
            return {"accepted": [], "standby": []}
        jobs = snapshot.get("jobs", [])
        if not isinstance(jobs, list):
            return {"accepted": [], "standby": []}
        remote = {
            str(item.get("engine_job_id") or ""): item
            for item in jobs
            if isinstance(item, dict) and item.get("engine_job_id")
        }
        entries = self.read().get("entries", {})
        accepted: list[dict[str, Any]] = []
        standby: list[dict[str, Any]] = []
        for job_id, attempt in exchange_attempts.items():
            observed = remote.get(job_id)
            local = entries.get(job_id) if isinstance(entries, dict) else None
            if not isinstance(observed, dict) or not isinstance(local, dict):
                continue
            correlation_matches = all(
                str(observed.get(key) or "") == str(local.get(key) or "")
                for key in (
                    "request_id",
                    "engine_job_id",
                    "attempt_id",
                    "operator",
                    "test_version",
                )
            )
            if not correlation_matches:
                continue
            admission_mode = attempt[3]
            remote_state = str(observed.get("state") or "")
            stage_state = str(observed.get("stage_state") or "")
            if admission_mode == "accept":
                if remote_state in {"accepted", "running", "completed", "failed"} or (
                    stage_state == "running" and remote_state != "standby"
                ):
                    accepted.append(dict(observed))
                continue
            if admission_mode == "standby" and (
                remote_state == "standby" or observed.get("staged_at")
            ):
                standby.append(dict(observed))
        return {"accepted": accepted, "standby": standby}

    def _release_unobserved_admissions(
        self,
        snapshot: object,
        *,
        exchange_attempts: dict[str, tuple[int, str, str, str]],
        exchange_request_id: str,
        recovery_request_id: str,
        original_error: str,
    ) -> list[str]:
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("jobs"), list):
            return []
        remote_jobs = {
            str(item.get("engine_job_id") or ""): item
            for item in snapshot["jobs"]
            if isinstance(item, dict) and item.get("engine_job_id")
        }
        state = self.read()
        released: list[tuple[str, dict[str, Any], str, str]] = []
        for job_id, record in state["entries"].items():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or job_id not in exchange_attempts
                or record.get("state") not in {
                "admitting",
                "staging-standby",
                }
            ):
                continue
            observed = remote_jobs.get(job_id)
            if isinstance(observed, dict):
                if correlation(observed) == correlation(record):
                    continue
                self._event(
                    "engine_uncertain_admission_correlation_conflict",
                    **correlation(record),
                    exchange_request_id=exchange_request_id,
                    recovery_request_id=recovery_request_id,
                    observed_correlation=correlation(observed),
                )
                continue

            attempt = exchange_attempts.get(job_id)
            admission_mode = (
                attempt[3]
                if attempt is not None
                else (
                    "standby" if record.get("state") == "staging-standby" else "accept"
                )
            )
            reason = (
                "trusted engine snapshot did not contain the uncertain "
                f"{admission_mode} admission; retry scheduled"
            )
            if original_error:
                reason += f" after transport error: {original_error}"
            admission_error = ""
            try:
                admission_record = self.admission.record_admission_failure(
                    job_id, reason
                )
                if str(admission_record.get("state") or "") in {
                    "accepted",
                    "running",
                    "return-ready",
                    "returned",
                    "standby",
                }:
                    continue
            except Exception as exc:
                admission_error = str(exc)

            released_at = utc_now_iso()
            record.update(
                {
                    "state": "pending",
                    "last_error": reason,
                    "last_admission_recovery_at": released_at,
                    "last_admission_recovery_request_id": recovery_request_id
                    or exchange_request_id,
                    "last_admission_recovery_outcome": "remote-not-observed-retry",
                    "updated_at": released_at,
                }
            )
            released.append((job_id, dict(record), admission_mode, admission_error))

        if not released:
            return []
        state["updated_at"] = utc_now_iso()
        self._write(state)
        for job_id, record, admission_mode, admission_error in released:
            attempt = exchange_attempts.get(job_id)
            self._event(
                "engine_uncertain_admission_released_for_retry",
                **correlation(record),
                admission_mode=admission_mode,
                admission_attempt=(
                    attempt[0]
                    if attempt is not None
                    else int(record.get("admission_attempts", 0) or 0)
                ),
                exchange_request_id=exchange_request_id,
                recovery_request_id=recovery_request_id,
                original_error=original_error,
                admission_error=admission_error,
                released_at=str(record.get("last_admission_recovery_at") or ""),
            )
        return [item[0] for item in released]

    def _consume_ready_bundles(
        self,
        raw_bundles: object,
        *,
        snapshot_request_id: str,
        transport_elapsed_seconds: object,
        snapshot_started_at: str = "",
        snapshot_completed_at: str = "",
        engine_observed_at: str = "",
        exchange_timeline: dict[str, Any] | None = None,
        local_transport_timeline: dict[str, Any] | None = None,
    ) -> list[str]:
        if not isinstance(raw_bundles, list):
            return []
        collected: list[str] = []
        for bundle in raw_bundles:
            if not isinstance(bundle, dict):
                continue
            terminal = bundle.get("terminal")
            if not isinstance(terminal, dict):
                continue
            job_id = str(
                bundle.get("engine_job_id") or terminal.get("engine_job_id") or ""
            )
            state = self.read()
            record = state["entries"].get(job_id)
            artifact_evidence = bundle.get("artifact_evidence")
            return_phase = (
                str(artifact_evidence.get("return_phase") or "complete")
                if isinstance(artifact_evidence, dict)
                else ""
            )
            if return_phase == "optional":
                if isinstance(record, dict) and self._consume_optional_bundle(
                    record,
                    bundle,
                    terminal,
                    snapshot_request_id=snapshot_request_id,
                    transport_elapsed_seconds=transport_elapsed_seconds,
                ):
                    collected.append(job_id)
                continue
            if not isinstance(record, dict) or record.get("state") != "return-ready":
                continue
            try:
                if correlation(record) != correlation(terminal):
                    raise EnginePumpError(
                        f"snapshot bundle correlation mismatch: {job_id}"
                    )
                identity_evidence = bundle.get("identity_evidence")
                artifact_status = (
                    str(artifact_evidence.get("status") or "")
                    if isinstance(artifact_evidence, dict)
                    else ""
                )
                if artifact_status not in {"verified", "required-verified"}:
                    raise EnginePumpError(
                        f"snapshot bundle artifacts are not verified: {job_id}"
                    )
                identity_status = (
                    str(identity_evidence.get("status") or "")
                    if isinstance(identity_evidence, dict)
                    else ""
                )
                failed_diagnostic_mismatch = (
                    str(terminal.get("state") or "") == "failed"
                    and identity_status == "mismatch-terminal-failed"
                )
                if identity_status not in {"verified", "not-required"} and not (
                    failed_diagnostic_mismatch
                ):
                    raise EnginePumpError(
                        f"snapshot bundle identity is not verified: {job_id}"
                    )
                receipt_id = str(
                    record.get("return_receipt_id") or return_receipt_id(record)
                )
                failed_stage, failed_exit_code = terminal_failure_summary(terminal)
                required_receipt_id = required_return_receipt_id(record)
                required_first = artifact_status == "required-verified"
                self.admission.record_terminal_manifest(terminal)
                returned = self.admission.record_return(job_id, receipt_id)
                returned_at = str(returned.get("returned_at") or utc_now_iso())
                self._update_entry(
                    job_id,
                    "returned-awaiting-ingest",
                    return_receipt_id=receipt_id,
                    collect_request_id=snapshot_request_id,
                    engine_terminal_state=str(terminal.get("state") or ""),
                    engine_failed_stage=failed_stage,
                    engine_failed_exit_code=failed_exit_code,
                    engine_code_generation=str(
                        terminal.get("engine_code_generation") or ""
                    ),
                    terminal_at=str(terminal.get("terminal_at") or ""),
                    returned_at=returned_at,
                    identity_evidence=identity_evidence,
                    artifact_evidence=artifact_evidence,
                    return_transport_mode=(
                        "snapshot-required-bundle"
                        if required_first
                        else "snapshot-ready-bundle"
                    ),
                    remote_ack_state="" if required_first else "pending",
                    required_return_receipt_id=(
                        required_receipt_id if required_first else ""
                    ),
                    remote_required_ack_state=(
                        "pending" if required_first else "not-required"
                    ),
                    collect_transport_elapsed_seconds=transport_elapsed_seconds,
                    snapshot_bundle_root=str(bundle.get("bundle_root") or ""),
                )
                self._event(
                    "engine_job_terminal_returned",
                    **correlation(record),
                    collect_request_id=snapshot_request_id,
                    terminal_at=str(terminal.get("terminal_at") or ""),
                    return_ready_at=str(
                        terminal.get("return_ready_at")
                        or terminal.get("terminal_at")
                        or ""
                    ),
                    terminal_state=str(terminal.get("state") or ""),
                    stage_history=list(terminal.get("history", [])),
                    stage_durations_seconds=stage_duration_summary(
                        terminal.get("history", [])
                    ),
                    identity_evidence=identity_evidence,
                    artifact_evidence=artifact_evidence,
                    transport_elapsed_seconds=transport_elapsed_seconds,
                    return_transport_mode=(
                        "snapshot-required-bundle"
                        if required_first
                        else "snapshot-ready-bundle"
                    ),
                    returned_at=returned_at,
                    snapshot_started_at=snapshot_started_at,
                    snapshot_completed_at=snapshot_completed_at,
                    engine_observed_at=engine_observed_at,
                    b_request_observed_at=str(
                        dict(exchange_timeline or {}).get("b_request_observed_at") or ""
                    ),
                    b_return_export_started_at=str(
                        dict(exchange_timeline or {}).get("return_export_started_at")
                        or ""
                    ),
                    b_return_export_finished_at=str(
                        dict(exchange_timeline or {}).get("return_export_finished_at")
                        or ""
                    ),
                    b_exchange_finished_at=str(
                        dict(exchange_timeline or {}).get("b_exchange_finished_at")
                        or ""
                    ),
                    local_transport_timeline=dict(local_transport_timeline or {}),
                )
                collected.append(job_id)
            except Exception as exc:
                self._record_error(job_id, "snapshot-ready-bundle", exc)
        return collected

    def _recover_cached_ready_bundles(self) -> list[str]:
        """Adopt verified bundles left durable before a local state-write failure."""
        from ascendop_daemon.legacy.engine_transport import (
            verify_returned_artifacts,
            verify_returned_identity,
        )

        cache_root = self.state_dir / "engine_ready_cache"
        if not cache_root.is_dir():
            return []
        recovered: list[str] = []
        records = [
            record
            for state_name in ("return-ready", "returned")
            for record in self._entries_in_state(state_name)
            if not record.get("snapshot_bundle_root")
        ]
        for record in records:
            job_id = str(record.get("engine_job_id") or "")
            if not job_id:
                continue
            candidates = sorted(
                cache_root.glob(f"*/ready_jobs/{job_id}"),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
            for job_root in candidates:
                try:
                    terminal = read_object(job_root / "terminal.json")
                    if correlation(record) != correlation(terminal):
                        raise EnginePumpError(
                            f"cached ready bundle correlation mismatch: {job_id}"
                        )
                    artifact_evidence = verify_returned_artifacts(job_root, terminal)
                    if str(artifact_evidence.get("return_phase") or "") == "optional":
                        continue
                    identity_evidence = verify_returned_identity(
                        job_root,
                        engine_job_id=job_id,
                        expected=terminal.get("input_identity"),
                        terminal_state=str(terminal.get("state") or ""),
                    )
                    self._update_entry(job_id, "return-ready")
                    consumed = self._consume_ready_bundles(
                        [
                            {
                                "engine_job_id": job_id,
                                "terminal": terminal,
                                "artifact_evidence": artifact_evidence,
                                "identity_evidence": identity_evidence,
                                "bundle_root": relative_path(job_root, self.root),
                            }
                        ],
                        snapshot_request_id=f"engine-ready-cache-recovery-{job_id}",
                        transport_elapsed_seconds=0.0,
                    )
                    if job_id in consumed:
                        recovered.append(job_id)
                        self._event(
                            "engine_cached_ready_bundle_recovered",
                            **correlation(record),
                            bundle_root=relative_path(job_root, self.root),
                        )
                        break
                except Exception as exc:
                    self._record_error(job_id, "cached-ready-recovery", exc)
        return recovered

    def _consume_optional_bundle(
        self,
        record: dict[str, Any],
        bundle: dict[str, Any],
        terminal: dict[str, Any],
        *,
        snapshot_request_id: str,
        transport_elapsed_seconds: object,
    ) -> bool:
        job_id = str(record.get("engine_job_id") or "")
        if record.get("state") not in {
            "returned-awaiting-ingest",
            "workflow-archived",
            "superseded-by-workflow-result",
            "canary-complete",
        }:
            return False
        try:
            if correlation(record) != correlation(terminal):
                raise EnginePumpError(
                    f"optional snapshot bundle correlation mismatch: {job_id}"
                )
            evidence = bundle.get("artifact_evidence")
            if not isinstance(evidence, dict) or evidence.get("status") != (
                "optional-verified"
            ):
                raise EnginePumpError(
                    f"optional snapshot bundle artifacts are not verified: {job_id}"
                )
            bundle_root = str(bundle.get("bundle_root") or "")
            materialized = materialize_optional_result_artifacts(
                self.root,
                record,
                bundle_root=bundle_root,
                evidence=evidence,
            )
            self._patch_entry(
                job_id,
                optional_artifact_evidence=evidence,
                optional_bundle_root=bundle_root,
                optional_returned_at=utc_now_iso(),
                optional_collect_request_id=snapshot_request_id,
                optional_collect_transport_elapsed_seconds=transport_elapsed_seconds,
                optional_materialization=materialized,
                remote_required_ack_state="confirmed",
                remote_ack_state="pending",
                last_error="",
            )
            self._event(
                "engine_optional_artifacts_returned",
                **correlation(record),
                collect_request_id=snapshot_request_id,
                artifact_evidence=evidence,
                materialization=materialized,
                transport_elapsed_seconds=transport_elapsed_seconds,
            )
            return True
        except Exception as exc:
            self._record_error(job_id, "snapshot-optional-bundle", exc)
            return False

    def _recover_optional_materializations(self) -> list[str]:
        """Materialize verified optional evidence after workflow archival.

        Required artifacts are intentionally returned first so a completed test
        can release engine credit immediately. Optional diagnostics may arrive
        before or after the workflow archive exists. Keep their verified cache
        durable, then idempotently merge it once the canonical RESULT path is
        available.
        """

        recovered: list[str] = []
        state = self.read()
        changed = False
        events: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        errors: list[tuple[str, str]] = []
        for record in state["entries"].values():
            if not isinstance(record, dict) or not self._record_in_scope(record):
                continue
            evidence = record.get("optional_artifact_evidence")
            bundle_root = str(record.get("optional_bundle_root") or "")
            materialization = record.get("optional_materialization")
            materialization_status = (
                str(materialization.get("status") or "")
                if isinstance(materialization, dict)
                else ""
            )
            if (
                not isinstance(evidence, dict)
                or evidence.get("status") != "optional-verified"
                or not bundle_root
                or materialization_status
                in OPTIONAL_MATERIALIZATION_TERMINAL_STATES
            ):
                continue
            if bool(record.get("workflow_ingest", True)):
                result_dir = (
                    self.root
                    / "operators_testresult"
                    / str(record.get("operator") or "")
                    / str(record.get("test_version") or "")
                )
                if not result_dir.is_dir():
                    # A workflow archive is the retry signal. Polling the same
                    # absent path every second only rewrites unchanged state.
                    continue
            try:
                outcome = materialize_optional_result_artifacts(
                    self.root,
                    record,
                    bundle_root=bundle_root,
                    evidence=evidence,
                )
                if outcome.get("status") == "awaiting-workflow-archive":
                    continue
                record["optional_materialization"] = outcome
                record["last_error"] = ""
                record["updated_at"] = utc_now_iso()
                changed = True
                events.append(
                    (
                        str(outcome.get("status") or ""),
                        dict(record),
                        dict(outcome),
                    )
                )
                if outcome.get("status") == "materialized":
                    job_id = str(record.get("engine_job_id") or "")
                    recovered.append(job_id)
            except Exception as exc:
                job_id = str(record.get("engine_job_id") or "")
                record["attempts"] = int(record.get("attempts", 0) or 0) + 1
                record["last_error"] = f"optional-materialization-recovery: {exc}"
                record["updated_at"] = utc_now_iso()
                changed = True
                errors.append((job_id, str(exc)))
        if changed:
            state["updated_at"] = utc_now_iso()
            self._write(state)
        for status, record, outcome in events:
            event_kind = {
                "materialized": "engine_optional_artifacts_materialized",
                "retained-in-engine-cache": "engine_optional_artifacts_retained",
                "no-materializable-paths": (
                    "engine_optional_artifacts_no_materializable_paths"
                ),
            }.get(status, "engine_optional_artifacts_terminalized")
            self._event(
                event_kind,
                **correlation(record),
                materialization=outcome,
            )
        for job_id, error in errors:
            self._event(
                "engine_pump_operation_failed",
                engine_job_id=job_id,
                operation="optional-materialization-recovery",
                error=error,
            )
        return recovered

    def _confirm_remote_acks(self, snapshot: dict[str, Any]) -> list[str]:
        jobs = snapshot.get("jobs", [])
        if not isinstance(jobs, list):
            return []
        remote = {
            str(item.get("engine_job_id") or ""): item
            for item in jobs
            if isinstance(item, dict) and item.get("engine_job_id")
        }
        confirmed: list[str] = []
        for job_id, record in self.read()["entries"].items():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or record.get("remote_ack_state") not in {
                "pending",
                "dispatched",
                "piggybacked",
                }
            ):
                continue
            observed = remote.get(job_id)
            if not isinstance(observed, dict):
                continue
            expected = str(record.get("return_receipt_id") or "")
            actual = str(observed.get("return_receipt_id") or "")
            if expected and actual and actual != expected:
                self._patch_entry(
                    job_id,
                    remote_ack_state="conflict",
                    remote_ack_last_error=(
                        f"remote receipt mismatch: expected={expected} actual={actual}"
                    ),
                )
                self._event(
                    "engine_remote_ack_conflict",
                    **correlation(record),
                    expected_receipt_id=expected,
                    actual_receipt_id=actual,
                )
                continue
            if expected and actual == expected and observed.get("returned_at"):
                self._patch_entry(
                    job_id,
                    remote_ack_state="confirmed",
                    remote_ack_confirmed_at=utc_now_iso(),
                    remote_returned_at=str(observed.get("returned_at") or ""),
                )
                self._event(
                    "engine_remote_ack_confirmed",
                    **correlation(record),
                    receipt_id=expected,
                )
                confirmed.append(job_id)
        return confirmed

    def _pending_remote_acknowledgements(self) -> list[dict[str, str]]:
        acknowledgements: list[dict[str, str]] = []
        for record in self.read()["entries"].values():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or record.get("remote_ack_state") not in {
                "pending",
                "dispatched",
                "piggybacked",
                }
            ):
                continue
            state = str(record.get("state") or "")
            workflow_ingest = bool(record.get("workflow_ingest", True))
            if workflow_ingest and state not in {
                "workflow-archived",
                "superseded-by-workflow-result",
                LOGICAL_JOB_SUPERSEDED_STATE,
            }:
                continue
            if not workflow_ingest and state != "canary-complete":
                continue
            job_id = str(record.get("engine_job_id") or "")
            receipt_id = str(record.get("return_receipt_id") or "")
            if job_id and receipt_id:
                acknowledgements.append(
                    {"engine_job_id": job_id, "receipt_id": receipt_id}
                )
        return acknowledgements

    def _pending_required_acknowledgements(self) -> list[dict[str, str]]:
        acknowledgements: list[dict[str, str]] = []
        for record in self.read()["entries"].values():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or record.get("remote_required_ack_state")
                not in {"pending", "piggybacked"}
            ):
                continue
            job_id = str(record.get("engine_job_id") or "")
            receipt_id = str(record.get("required_return_receipt_id") or "")
            if job_id and receipt_id:
                acknowledgements.append(
                    {"engine_job_id": job_id, "receipt_id": receipt_id}
                )
        return acknowledgements

    def _pending_standby_cancellations(self) -> list[dict[str, str]]:
        cancellations: list[dict[str, str]] = []
        for record in self._entries_in_state("standby-cancel-requested"):
            job_id = str(record.get("engine_job_id") or "")
            if not job_id:
                continue
            cancellations.append(
                {
                    "engine_job_id": job_id,
                    "reason": str(record.get("cancel_reason") or "")
                    or "standby cancelled by controller",
                }
            )
        return cancellations

    def _request_all_standby_cancellations_unlocked(self, *, reason: str) -> list[str]:
        requested: list[str] = []
        for record in list(self._entries_in_state("standby")):
            job_id = str(record.get("engine_job_id") or "")
            if not job_id:
                continue
            result = self._cancel_pending_unlocked(job_id, reason=reason)
            if result.get("state") == "standby-cancel-requested":
                requested.append(job_id)
        return requested

    def _record_standby_cancellation_receipts(
        self,
        raw: object,
        *,
        request_id: str,
    ) -> list[str]:
        if not isinstance(raw, list):
            return []
        cancelled: list[str] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("engine_job_id") or "")
            record = self.read()["entries"].get(job_id)
            if not isinstance(record, dict):
                continue
            outcome = str(
                item.get("standby_cancellation_outcome") or item.get("outcome") or ""
            )
            if outcome == "cancelled":
                cancelled_at = str(item.get("cancelled_at") or utc_now_iso())
                self._update_entry(
                    job_id,
                    "standby-cancelled",
                    cancelled_at=cancelled_at,
                    standby_cancellation_outcome="cancelled",
                    standby_cancellation_request_id=request_id,
                    standby_cancellation_error="",
                )
                self._event(
                    "engine_standby_cancellation_confirmed",
                    **correlation(record),
                    cancelled_at=cancelled_at,
                    request_id_sent=request_id,
                )
                cancelled.append(job_id)
                continue
            if outcome == "conflict":
                self._patch_entry(
                    job_id,
                    standby_cancellation_outcome="conflict",
                    standby_cancellation_request_id=request_id,
                    standby_cancellation_error=str(
                        item.get("standby_cancellation_error")
                        or item.get("error")
                        or ""
                    ),
                )
                self._event(
                    "engine_standby_cancellation_conflict",
                    **correlation(record),
                    request_id_sent=request_id,
                    error=str(
                        item.get("standby_cancellation_error")
                        or item.get("error")
                        or ""
                    ),
                )
        return cancelled

    def _record_ack_piggybacks(
        self,
        raw: object,
        *,
        snapshot_request_id: str,
    ) -> list[str]:
        if not isinstance(raw, list):
            return []
        sent: list[str] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("engine_job_id") or "")
            receipt_id = str(item.get("receipt_id") or "")
            record = self.read()["entries"].get(job_id)
            if (
                not isinstance(record, dict)
                or str(record.get("return_receipt_id") or "") != receipt_id
                or record.get("remote_ack_state") == "confirmed"
            ):
                continue
            attempt = int(record.get("remote_ack_attempts", 0) or 0) + 1
            self._patch_entry(
                job_id,
                remote_ack_state="piggybacked",
                remote_ack_attempts=attempt,
                remote_ack_request_id=snapshot_request_id,
                remote_ack_dispatched_at=utc_now_iso(),
            )
            self._event(
                "engine_remote_ack_piggybacked",
                **correlation(record),
                receipt_id=receipt_id,
                snapshot_request_id=snapshot_request_id,
            )
            sent.append(job_id)
        return sent

    def _record_required_ack_piggybacks(
        self,
        raw: object,
        *,
        snapshot_request_id: str,
    ) -> list[str]:
        if not isinstance(raw, list):
            return []
        sent: list[str] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("engine_job_id") or "")
            receipt_id = str(item.get("required_return_receipt_id") or "")
            record = self.read()["entries"].get(job_id)
            if (
                not isinstance(record, dict)
                or str(record.get("required_return_receipt_id") or "") != receipt_id
                or record.get("remote_required_ack_state") == "confirmed"
            ):
                continue
            attempt = int(record.get("remote_required_ack_attempts", 0) or 0) + 1
            self._patch_entry(
                job_id,
                remote_required_ack_state="confirmed",
                remote_required_ack_attempts=attempt,
                remote_required_ack_request_id=snapshot_request_id,
                remote_required_ack_confirmed_at=utc_now_iso(),
                remote_required_returned_at=str(item.get("required_returned_at") or ""),
            )
            self._event(
                "engine_required_ack_piggybacked",
                **correlation(record),
                receipt_id=receipt_id,
                snapshot_request_id=snapshot_request_id,
            )
            sent.append(job_id)
        return sent

    def read(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {
                "protocol_version": "engine-v1",
                "endpoint_id": self.endpoint_id,
                "entries": {},
                "exchanges": {},
                "runtime_sync": {},
                "updated_at": "",
            }
        raw = read_object(self.state_path)
        raw.setdefault("protocol_version", "engine-v1")
        observed_endpoint = str(raw.get("endpoint_id") or "")
        if (
            self.endpoint_id
            and observed_endpoint
            and observed_endpoint != self.endpoint_id
        ):
            raise EnginePumpError(
                "engine pump endpoint mismatch: "
                f"requested={self.endpoint_id} observed={observed_endpoint}"
            )
        raw.setdefault("endpoint_id", self.endpoint_id)
        raw.setdefault("entries", {})
        raw.setdefault("exchanges", {})
        raw.setdefault("runtime_sync", {})
        if not isinstance(raw["entries"], dict):
            raise EnginePumpError("engine pump entries must be an object")
        if not isinstance(raw["exchanges"], dict):
            raise EnginePumpError("engine pump exchanges must be an object")
        if not isinstance(raw["runtime_sync"], dict):
            raise EnginePumpError("engine pump runtime_sync must be an object")
        return raw

    def scheduling_state(self) -> dict[str, Any]:
        state = self.read()
        for _path, record in self._staged_enqueue_records():
            job_id = str(record.get("engine_job_id") or "")
            if job_id and job_id not in state["entries"]:
                state["entries"][job_id] = record
        return state

    def status(self, *, outcome: str = "status", **extra: Any) -> dict[str, Any]:
        state = self.read()
        staged_enqueue_count = sum(
            1
            for _path, record in self._staged_enqueue_records()
            if self._record_in_scope(record)
        )
        counts: dict[str, int] = {}
        profiles: dict[str, int] = {}
        pending_remote_ack_count = 0
        pending_required_ack_count = 0
        for record in state["entries"].values():
            if isinstance(record, dict) and self._record_in_scope(record):
                name = str(record.get("state") or "unknown")
                counts[name] = counts.get(name, 0) + 1
                profile = str(record.get("execution_profile") or "unspecified")
                profiles[profile] = profiles.get(profile, 0) + 1
                if record.get("remote_ack_state") in {
                    "pending",
                    "dispatched",
                    "piggybacked",
                }:
                    pending_remote_ack_count += 1
                if record.get("remote_required_ack_state") in {
                    "pending",
                    "piggybacked",
                }:
                    pending_required_ack_count += 1
        return {
            **state,
            "outcome": outcome,
            "state_counts": counts,
            "execution_profiles": profiles,
            "pending_remote_ack_count": pending_remote_ack_count,
            "pending_required_ack_count": pending_required_ack_count,
            "staged_enqueue_count": staged_enqueue_count,
            "outstanding_exchange_count": sum(
                1
                for item in state.get("exchanges", {}).values()
                if isinstance(item, dict)
                and str(item.get("state") or "")
                in {"publishing", "published", "uncertain"}
            ),
            "admission": self.admission.snapshot(),
            "recent_replenishments": self._recent_replenishments(),
            **extra,
        }

    def cancel_pending(self, job_id: str, *, reason: str) -> dict[str, Any]:
        with PumpLock(self.lock_path):
            return self._cancel_pending_unlocked(job_id, reason=reason)

    def _cancel_pending_unlocked(self, job_id: str, *, reason: str) -> dict[str, Any]:
        state = self.read()
        record = state["entries"].get(job_id)
        if not isinstance(record, dict):
            raise EnginePumpError(f"unknown engine outbox job: {job_id}")
        record_state = str(record.get("state") or "")
        if record_state == "standby-cancel-requested":
            return dict(record)
        if record_state == "standby" and not record.get("accepted_at"):
            cancelled_at = utc_now_iso()
            record.update(
                {
                    "state": "standby-cancel-requested",
                    "cancel_requested_at": cancelled_at,
                    "cancel_reason": str(reason).strip()
                    or "operator cancelled standby work",
                    "updated_at": cancelled_at,
                    "last_error": "",
                }
            )
            state["updated_at"] = cancelled_at
            self._write(state)
            self._event(
                "engine_standby_cancellation_requested",
                **correlation(record),
                cancel_requested_at=cancelled_at,
                reason=record["cancel_reason"],
            )
            return dict(record)
        if record_state != "pending" or record.get("accepted_at"):
            raise EnginePumpError(
                f"engine outbox job is not unaccepted pending work: {job_id}"
            )
        cancelled_at = utc_now_iso()
        record.update(
            {
                "state": "cancelled-before-admission",
                "cancelled_at": cancelled_at,
                "cancel_reason": str(reason).strip()
                or "operator cancelled pending work",
                "updated_at": cancelled_at,
                "last_error": "",
            }
        )
        state["updated_at"] = cancelled_at
        self._write(state)
        self._event(
            "engine_pending_cancelled",
            **correlation(record),
            cancelled_at=cancelled_at,
            reason=record["cancel_reason"],
        )
        return dict(record)

    def reconcile_allowed_operators(
        self, allowed_operators: set[str]
    ) -> dict[str, Any]:
        allowed = {str(op) for op in allowed_operators if str(op)}
        cancelled: list[str] = []
        cancellation_requested: list[str] = []
        blockers: list[dict[str, str]] = []
        with PumpLock(self.lock_path):
            for record in list(self.read()["entries"].values()):
                if not isinstance(record, dict):
                    continue
                if not bool(record.get("workflow_ingest", True)):
                    continue
                op = str(record.get("operator") or "")
                if not op or op in allowed:
                    continue
                state = str(record.get("state") or "")
                job_id = str(record.get("engine_job_id") or "")
                if state == "pending":
                    self._cancel_pending_unlocked(
                        job_id, reason=f"operator plugin is no longer active: {op}"
                    )
                    cancelled.append(job_id)
                elif state in {"standby", "standby-cancel-requested"}:
                    result = self._cancel_pending_unlocked(
                        job_id, reason=f"operator plugin is no longer active: {op}"
                    )
                    if result.get("state") == "standby-cancel-requested":
                        cancellation_requested.append(job_id)
                elif state in {
                    "admitting",
                    "staging-standby",
                    "accepted",
                    "running",
                    "return-ready",
                    "returned-awaiting-ingest",
                }:
                    blockers.append(
                        {"engine_job_id": job_id, "operator": op, "state": state}
                    )
        return {
            "allowed_operators": sorted(allowed),
            "cancelled": cancelled,
            "standby_cancellation_requested": cancellation_requested,
            "blockers": blockers,
        }

    def _recent_replenishments(self, limit: int = 10) -> list[dict[str, Any]]:
        if not self.events_path.is_file():
            return []
        recent: deque[dict[str, Any]] = deque(maxlen=max(1, int(limit)))
        try:
            with self.events_path.open("r", encoding="utf-8-sig") as handle:
                for line in handle:
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (
                        isinstance(item, dict)
                        and item.get("kind") == "engine_credit_replenished"
                    ):
                        recent.append(item)
        except OSError:
            return []
        return list(recent)

    def _entries_in_state(self, name: str) -> list[dict[str, Any]]:
        records = [
            record
            for record in self.read()["entries"].values()
            if isinstance(record, dict)
            and self._record_in_scope(record)
            and record.get("state") == name
        ]
        return sorted(
            records,
            key=lambda item: (
                int(item.get("sequence", 0) or 0),
                str(item.get("enqueued_at") or ""),
            ),
        )

    def _sync_from_admission(self) -> list[str]:
        admission_jobs = self.admission.read().get("jobs", {})
        state = self.read()
        changed = False
        reconciled: list[str] = []
        for job_id, record in state["entries"].items():
            if not isinstance(record, dict) or not self._record_in_scope(record):
                continue
            if record.get("state") in {
                "return-lost",
                "returned-awaiting-ingest",
                "workflow-archived",
                "superseded-by-workflow-result",
                "canary-complete",
                LOGICAL_JOB_SUPERSEDED_STATE,
            }:
                continue
            remote = admission_jobs.get(job_id)
            if not isinstance(remote, dict):
                continue
            remote_state = str(remote.get("state") or "")
            mapped = {
                "admitting": "admitting",
                "staging-standby": "staging-standby",
                "standby": "standby",
                "standby-cancelled": "standby-cancelled",
                "accepted": "accepted",
                "running": "running",
                "return-ready": "return-ready",
                "returned": "returned",
            }.get(remote_state)
            cancellation_pending = record.get(
                "state"
            ) == "standby-cancel-requested" and remote_state in {
                "staging-standby",
                "standby",
            }
            previous_state = str(record.get("state") or "")
            if mapped and not cancellation_pending and previous_state != mapped:
                record["state"] = mapped
                record["updated_at"] = utc_now_iso()
                if previous_state == "pending":
                    record["last_error"] = ""
                    reconciled.append(job_id)
                changed = True
            terminal_credit_released = remote_state == "return-ready" or str(
                remote.get("engine_terminal_state") or ""
            ) in {"completed", "failed"}
            if terminal_credit_released and not record.get("credit_released_at"):
                record["credit_released_at"] = str(
                    remote.get("terminal_at")
                    or remote.get("terminal_observed_at")
                    or utc_now_iso()
                )
                changed = True
            for field in (
                "staged_at",
                "accepted_at",
                "promoted_at",
                "promotion_source",
                "terminal_at",
                "terminal_observed_at",
                "returned_at",
                "return_receipt_id",
            ):
                value = remote.get(field)
                if value and record.get(field) != value:
                    record[field] = value
                    changed = True
        if changed:
            state["updated_at"] = utc_now_iso()
            self._write(state)
        return reconciled

    def _observe_standby_promotions(self, observed_at: str) -> list[str]:
        admission_jobs = self.admission.read().get("jobs", {})
        state = self.read()
        promoted: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        for job_id, record in state["entries"].items():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or record.get("standby_promotion_observed_at")
            ):
                continue
            remote = admission_jobs.get(job_id)
            if not isinstance(remote, dict):
                continue
            if str(remote.get("promotion_source") or "") != "b-side-standby":
                continue
            accepted_at = str(remote.get("accepted_at") or "")
            if not accepted_at or remote.get("state") not in {
                "accepted",
                "running",
                "return-ready",
                "returned",
            }:
                continue
            record.update(
                {
                    "accepted_at": accepted_at,
                    "promoted_at": str(remote.get("promoted_at") or accepted_at),
                    "promotion_source": "b-side-standby",
                    "standby_promotion_observed_at": observed_at,
                    "updated_at": observed_at,
                }
            )
            promoted.append((job_id, dict(record), dict(remote)))
        if not promoted:
            return []
        state["updated_at"] = observed_at
        self._write(state)
        for _job_id, record, remote in promoted:
            self._event(
                "engine_job_admitted",
                **correlation(record),
                accepted_at=str(remote.get("accepted_at") or ""),
                promoted_at=str(remote.get("promoted_at") or ""),
                staged_at=str(remote.get("staged_at") or ""),
                promotion_source="b-side-standby",
                transport_mode="b-side-standby-promotion",
                observed_at=observed_at,
            )
        return [job_id for job_id, _record, _remote in promoted]

    def _unreplenished_credit_records(self) -> list[dict[str, Any]]:
        records = [
            dict(record)
            for record in self.read()["entries"].values()
            if isinstance(record, dict)
            and self._record_in_scope(record)
            and record.get("credit_released_at")
            and not record.get("replacement_engine_job_id")
            and record.get("state")
            in {
                "return-ready",
                "returned-awaiting-ingest",
                "workflow-archived",
                "canary-complete",
            }
        ]
        return sorted(
            records,
            key=lambda item: (
                str(item.get("credit_released_at") or ""),
                int(item.get("sequence", 0) or 0),
            ),
        )

    def _suppress_pending_with_workflow_result(self) -> list[str]:
        state = self.read()
        superseded: list[str] = []
        for job_id, record in state["entries"].items():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or record.get("state") != "pending"
            ):
                continue
            if not bool(record.get("workflow_ingest", True)):
                continue
            if str(record.get("job_kind") or "operator-test") != "operator-test":
                continue
            op = str(record.get("operator") or "")
            version = str(record.get("test_version") or "")
            result = self.root / "operators_testresult" / op / version / "RESULT.md"
            if not result.is_file():
                continue
            record.update(
                {
                    "state": "superseded-by-workflow-result",
                    "superseded_at": utc_now_iso(),
                    "superseded_result": relative_path(result, self.root),
                    "updated_at": utc_now_iso(),
                    "last_error": "",
                }
            )
            superseded.append(job_id)
            self._event(
                "engine_pending_superseded",
                **correlation(record),
                result=relative_path(result, self.root),
            )
        if superseded:
            state["updated_at"] = utc_now_iso()
            self._write(state)
        return superseded

    def _update_entry(self, job_id: str, state_name: str, **fields: Any) -> None:
        state = self.read()
        record = state["entries"].get(job_id)
        if not isinstance(record, dict):
            raise EnginePumpError(f"unknown engine outbox job: {job_id}")
        record.update(fields)
        record["state"] = state_name
        record["updated_at"] = utc_now_iso()
        record["last_error"] = ""
        state["updated_at"] = utc_now_iso()
        self._write(state)

    def _patch_entry(self, job_id: str, **fields: Any) -> dict[str, Any]:
        state = self.read()
        record = state["entries"].get(job_id)
        if not isinstance(record, dict):
            raise EnginePumpError(f"unknown engine outbox job: {job_id}")
        record.update(fields)
        record["updated_at"] = utc_now_iso()
        state["updated_at"] = utc_now_iso()
        self._write(state)
        return dict(record)

    def _record_replenishment(
        self,
        released: dict[str, Any],
        *,
        replacement_engine_job_id: str,
        replenished_at: str,
        replenished_observed_at: str = "",
    ) -> None:
        released_job_id = str(released.get("engine_job_id") or "")
        if not released_job_id:
            return
        current = self.read()["entries"].get(released_job_id)
        if not isinstance(current, dict) or current.get("replacement_engine_job_id"):
            return
        terminal_at = str(
            current.get("terminal_at")
            or released.get("terminal_at")
            or current.get("credit_released_at")
            or current.get("updated_at")
            or ""
        )
        returned_at = str(current.get("returned_at") or "")
        replacement = self.read()["entries"].get(replacement_engine_job_id)
        replacement = replacement if isinstance(replacement, dict) else {}
        self._patch_entry(
            released_job_id,
            replacement_engine_job_id=replacement_engine_job_id,
            replenished_at=replenished_at,
            replenished_observed_at=replenished_observed_at,
        )
        self._patch_entry(
            replacement_engine_job_id,
            replenishes_engine_job_id=released_job_id,
        )
        self._event(
            "engine_credit_replenished",
            operator=str(replacement.get("operator") or ""),
            test_version=str(replacement.get("test_version") or ""),
            request_id=str(replacement.get("request_id") or ""),
            attempt_id=str(replacement.get("attempt_id") or ""),
            engine_job_id=replacement_engine_job_id,
            released_engine_job_id=released_job_id,
            released_operator=str(current.get("operator") or ""),
            released_test_version=str(current.get("test_version") or ""),
            replacement_engine_job_id=replacement_engine_job_id,
            terminal_at=terminal_at,
            returned_at=returned_at,
            replenished_at=replenished_at,
            replenished_observed_at=replenished_observed_at,
            terminal_to_replenish_seconds=duration_between(terminal_at, replenished_at),
            return_to_replenish_seconds=duration_between(
                returned_at,
                replenished_observed_at or replenished_at,
            ),
        )

    def _record_error(self, job_id: str, operation: str, exc: Exception) -> None:
        state = self.read()
        record = state["entries"].get(job_id)
        if isinstance(record, dict):
            record["attempts"] = int(record.get("attempts", 0) or 0) + 1
            record["last_error"] = f"{operation}: {exc}"
            record["updated_at"] = utc_now_iso()
            state["updated_at"] = utc_now_iso()
            self._write(state)
        self._event(
            "engine_pump_operation_failed",
            engine_job_id=job_id,
            operation=operation,
            error=str(exc),
        )

    def _record_return_loss(
        self, job_id: str, exc: EngineReturnAlreadyCompactedError
    ) -> None:
        state = self.read()
        record = state["entries"].get(job_id)
        if not isinstance(record, dict):
            return
        failed_at = utc_now_iso()
        record.update(
            {
                "state": "return-lost",
                "retryable_transport_failure": True,
                "return_lost_at": failed_at,
                "attempts": int(record.get("attempts", 0) or 0) + 1,
                "last_error": f"collect: {exc}",
                "updated_at": failed_at,
            }
        )
        state["updated_at"] = failed_at
        self._write(state)
        self._event(
            "engine_return_lost_before_ingest",
            **correlation(record),
            error=str(exc),
            retryable=True,
        )

    def _payload_is_transportable(self, record: dict[str, Any]) -> bool:
        spec_relative = str(record.get("spec_path") or "")
        if spec_relative:
            try:
                spec = read_object(self.root / spec_relative)
            except (OSError, ValueError) as exc:
                self._fail_local_admission(
                    record,
                    f"cannot read frozen engine spec: {exc}",
                )
                return False
            contract = spec.get("test_contract")
            if isinstance(contract, dict):
                declared = contract.get("declared_performance_samples_per_case")
                effective = contract.get("performance_samples_per_case")
                if declared is not None and effective is not None:
                    try:
                        declared_count = int(declared)
                        effective_count = int(effective)
                    except (TypeError, ValueError):
                        self._fail_local_admission(
                            record,
                            "test contract performance sample counts must be integers",
                        )
                        return False
                    if declared_count != effective_count:
                        explicit_effective = contract.get(
                            "effective_performance_samples_per_case"
                        )
                        try:
                            explicit_effective_count = int(explicit_effective)
                        except (TypeError, ValueError):
                            explicit_effective_count = -1
                        fast_single_override = (
                            contract.get("protocol_version")
                            == "engine-test-contract-v2"
                            and contract.get("performance_measurement_mode")
                            == "fast-single"
                            and contract.get("performance_sample_policy")
                            == "fast-single-effective-one-declared-preserved"
                            and contract.get("profile_round_override_protocol")
                            == "ascendop-profile-rounds-override-v1"
                            and bool(contract.get("profile_round_override_required"))
                            and contract.get("performance_capture_mode")
                            == "single-python-multi-case"
                            and declared_count >= effective_count == 1
                            and explicit_effective_count == effective_count
                        )
                        if not fast_single_override:
                            self._fail_local_admission(
                                record,
                                "test contract mutates declared performance samples: "
                                f"declared={declared_count} effective={effective_count}",
                            )
                            return False
        relative = str(record.get("payload_root") or "")
        if not relative:
            return True
        payload_root = self.root / relative
        if not payload_root.is_dir():
            self._fail_local_admission(
                record,
                f"payload root is missing: {relative}",
            )
            return False
        empty_directories = sorted(
            path.relative_to(payload_root).as_posix()
            for path in payload_root.rglob("*")
            if path.is_dir() and not any(path.iterdir())
        )
        if empty_directories:
            self._fail_local_admission(
                record,
                "payload contains Git-invisible empty directories: "
                + ", ".join(empty_directories[:8]),
            )
            return False
        return True

    def _fail_local_admission(
        self, record: dict[str, Any], reason: str
    ) -> None:
        job_id = str(record.get("engine_job_id") or "")
        state = self.read()
        current = state["entries"].get(job_id)
        if not isinstance(current, dict):
            return
        failed_at = utc_now_iso()
        current.update(
            {
                "state": "admission-failed",
                "admission_failed_at": failed_at,
                "last_error": f"transport-payload-validation: {reason}",
                "attempts": int(current.get("attempts", 0) or 0) + 1,
                "updated_at": failed_at,
            }
        )
        state["updated_at"] = failed_at
        self._write(state)
        self._event(
            "engine_payload_not_transportable",
            **correlation(current),
            error=str(current["last_error"]),
        )

    def _fail_remote_admission(
        self, job_id: str, operation: str, reason: str
    ) -> None:
        state = self.read()
        current = state["entries"].get(job_id)
        if not isinstance(current, dict):
            return
        failed_at = utc_now_iso()
        retryable = retryable_remote_admission_rejection(reason)
        runtime_view = self._runtime_generation_view()
        retry_after_runtime_sync = bool(
            self.expected_remote_generation
            and (
                runtime_generation_admission_rejection(reason)
                or (
                    runtime_view["observed_remote_generation"]
                    and runtime_view["state"] != "ready"
                )
            )
        )
        current.update(
            {
                "state": "pending" if retryable else "admission-failed",
                "admission_failed_at": failed_at,
                "last_error": f"{operation}: {reason}",
                "attempts": int(current.get("attempts", 0) or 0) + 1,
                "retryable_admission_failure": retryable,
                "admission_retry_scheduled_at": failed_at if retryable else "",
                "retry_after_runtime_sync": retry_after_runtime_sync,
                "failure_remote_engine_generation": runtime_view[
                    "observed_remote_generation"
                ],
                "failure_expected_remote_engine_generation": (
                    self.expected_remote_generation
                ),
                "updated_at": failed_at,
            }
        )
        state["updated_at"] = failed_at
        self._write(state)
        self._event(
            (
                "engine_remote_admission_deferred"
                if retryable
                else "engine_remote_admission_held_for_runtime_sync"
                if retry_after_runtime_sync
                else "engine_remote_admission_rejected"
            ),
            **correlation(current),
            operation=operation,
            error=str(current["last_error"]),
            retryable=retryable,
            retry_after_runtime_sync=retry_after_runtime_sync,
            failure_remote_engine_generation=runtime_view[
                "observed_remote_generation"
            ],
            failure_expected_remote_engine_generation=(
                self.expected_remote_generation
            ),
        )

    def _recover_runtime_generation_admission_failures(self) -> list[str]:
        if not self.expected_remote_generation:
            return []
        runtime_view = self._runtime_generation_view()
        if runtime_view["state"] != "ready":
            return []
        state = self.read()
        recovered: list[tuple[str, dict[str, Any]]] = []
        superseded: list[tuple[str, dict[str, Any]]] = []
        recovered_at = utc_now_iso()
        for job_id, record in state.get("entries", {}).items():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or record.get("state") != "admission-failed"
            ):
                continue
            error = str(record.get("last_error") or "")
            if not (
                bool(record.get("retry_after_runtime_sync"))
                or runtime_generation_admission_rejection(error)
            ):
                continue
            logical_key = self._logical_job_key_for_record(record)
            sequence = int(record.get("sequence", 0) or 0)
            later = next(
                (
                    (other_job_id, other)
                    for other_job_id, other in state.get("entries", {}).items()
                    if isinstance(other, dict)
                    and str(other_job_id) != str(job_id)
                    and int(other.get("sequence", 0) or 0) > sequence
                    and logical_key
                    and self._logical_job_key_for_record(other) == logical_key
                ),
                None,
            )
            if later is not None:
                later_job_id, later_record = later
                record.update(
                    {
                        "state": LOGICAL_JOB_SUPERSEDED_STATE,
                        "logical_job_key": logical_key,
                        "logical_superseded_by": str(later_job_id),
                        "logical_superseded_at": recovered_at,
                        "logical_supersede_reason": (
                            "later logical attempt exists after runtime "
                            "generation recovery"
                        ),
                        "retry_after_runtime_sync": False,
                        "updated_at": recovered_at,
                    }
                )
                superseded.append((str(job_id), dict(record)))
                continue
            record.update(
                {
                    "state": "pending",
                    "logical_job_key": logical_key,
                    "retry_after_runtime_sync": False,
                    "runtime_generation_retry_recovered_at": recovered_at,
                    "runtime_generation_retry_target": (
                        self.expected_remote_generation
                    ),
                    "admission_retry_scheduled_at": recovered_at,
                    "updated_at": recovered_at,
                }
            )
            recovered.append((str(job_id), dict(record)))
        if not recovered and not superseded:
            return []
        state["updated_at"] = recovered_at
        self._write(state)
        for _, record in superseded:
            self._event(
                "engine_runtime_generation_admission_superseded",
                **correlation(record),
                logical_job_key=str(record.get("logical_job_key") or ""),
                logical_superseded_by=str(
                    record.get("logical_superseded_by") or ""
                ),
                previous_error=str(record.get("last_error") or ""),
                recovered_at=recovered_at,
            )
        for _, record in recovered:
            self._event(
                "engine_runtime_generation_admission_recovered",
                **correlation(record),
                expected_remote_generation=self.expected_remote_generation,
                previous_error=str(record.get("last_error") or ""),
                recovered_at=recovered_at,
            )
        return [item[0] for item in recovered]

    def _recover_retryable_admission_failures(self) -> list[str]:
        state = self.read()
        recovered: list[tuple[str, dict[str, Any]]] = []
        recovered_at = utc_now_iso()
        for job_id, record in state.get("entries", {}).items():
            if (
                not isinstance(record, dict)
                or not self._record_in_scope(record)
                or record.get("state") != "admission-failed"
                or not retryable_remote_admission_rejection(
                    str(record.get("last_error") or "")
                )
            ):
                continue
            record.update(
                {
                    "state": "pending",
                    "retryable_admission_failure": True,
                    "admission_retry_scheduled_at": recovered_at,
                    "updated_at": recovered_at,
                }
            )
            recovered.append((str(job_id), dict(record)))
        if not recovered:
            return []
        state["updated_at"] = recovered_at
        self._write(state)
        for _, record in recovered:
            self._event(
                "engine_remote_admission_deferred",
                **correlation(record),
                operation="state-upgrade",
                error=str(record.get("last_error") or ""),
                retryable=True,
            )
        return [item[0] for item in recovered]

    def _write(self, payload: dict[str, Any]) -> None:
        observed_endpoint = str(payload.get("endpoint_id") or "")
        if (
            self.endpoint_id
            and observed_endpoint
            and observed_endpoint != self.endpoint_id
        ):
            raise EnginePumpError(
                "engine pump endpoint mismatch before write: "
                f"requested={self.endpoint_id} observed={observed_endpoint}"
            )
        if self.endpoint_id:
            payload["endpoint_id"] = self.endpoint_id
        self.state_dir.mkdir(parents=True, exist_ok=True)
        write_json(self.state_path, payload)

    def _event(self, kind: str, **fields: Any) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "time": utc_now_iso(),
                        "kind": kind,
                        "endpoint_id": self.endpoint_id,
                        **fields,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                )
                + "\n"
            )


def materialize_optional_result_artifacts(
    root: Path,
    record: dict[str, Any],
    *,
    bundle_root: str,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    """Merge a verified optional return into the canonical workflow archive."""

    job_id = str(record.get("engine_job_id") or "")
    op = str(record.get("operator") or "")
    test_version = str(record.get("test_version") or "")
    if not job_id or not op or not test_version:
        raise EnginePumpError("optional artifact record has incomplete correlation")
    raw_paths = evidence.get("paths")
    if (
        isinstance(raw_paths, list)
        and not raw_paths
        or raw_paths is None
        and int(evidence.get("artifact_count", 0) or 0) == 0
    ):
        return {
            "status": "no-materializable-paths",
            "engine_job_id": job_id,
            "paths": [],
        }
    if not isinstance(raw_paths, list):
        raise EnginePumpError(f"optional artifact evidence has no paths: {job_id}")
    if not bool(record.get("workflow_ingest", True)):
        return {
            "status": "retained-in-engine-cache",
            "engine_job_id": job_id,
            "bundle_root": bundle_root,
            "paths": [str(path) for path in raw_paths],
            "retained_at": utc_now_iso(),
        }
    raw_bundle = Path(bundle_root)
    if raw_bundle.is_absolute():
        job_root = raw_bundle.resolve()
    else:
        job_root = (root / raw_bundle).resolve()
    cache_root = (root / "TestUtils" / "tester_daemon" / "engine_ready_cache").resolve()
    if job_root != cache_root and cache_root not in job_root.parents:
        raise EnginePumpError(
            f"optional artifact bundle is outside engine cache: {job_root}"
        )
    source_root = job_root / "result_bundle"
    if not source_root.is_dir():
        raise EnginePumpError(f"optional result bundle is missing: {source_root}")

    result_dir = root / "operators_testresult" / op / test_version
    if not result_dir.is_dir():
        return {
            "status": "awaiting-workflow-archive",
            "engine_job_id": job_id,
            "result_dir": relative_path(result_dir, root),
        }

    paths: list[str] = []
    for raw in raw_paths:
        relative = str(raw or "").replace("\\", "/").strip("/")
        if (
            not relative
            or ".." in Path(relative).parts
            or relative in paths
        ):
            raise EnginePumpError(f"unsafe optional artifact path: {relative!r}")
        source = (source_root / relative).resolve()
        if source != source_root.resolve() and source_root.resolve() not in source.parents:
            raise EnginePumpError(f"optional artifact escapes result bundle: {relative}")
        if source.is_symlink() or not source.exists():
            raise EnginePumpError(f"optional artifact is missing or symlinked: {relative}")
        paths.append(relative)

    archive_roots = [
        result_dir / "gitpartner_output" / "result_bundle",
        result_dir / "submit_snapshot" / "gitpartner_output" / "result_bundle",
    ]
    materialized_roots: list[str] = []
    for archive_root in archive_roots:
        archive_root.mkdir(parents=True, exist_ok=True)
        resolved_archive_root = archive_root.resolve()
        for relative in paths:
            source = source_root / relative
            destination = (archive_root / relative).resolve()
            if (
                destination != resolved_archive_root
                and resolved_archive_root not in destination.parents
            ):
                raise EnginePumpError(
                    f"optional artifact escapes workflow archive: {relative}"
                )
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.is_dir():
                shutil.copytree(source, destination, dirs_exist_ok=True)
            else:
                temporary = destination.with_name(
                    f".{destination.name}.{os.getpid()}.{time.time_ns()}.tmp"
                )
                try:
                    shutil.copy2(source, temporary)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
        materialized_roots.append(relative_path(archive_root, root))

    diagnostic = write_result_diagnostic_supplement(
        root,
        result_dir=result_dir,
        job_root=job_root,
        record=record,
    )
    return {
        "status": "materialized",
        "engine_job_id": job_id,
        "paths": paths,
        "archive_roots": materialized_roots,
        "diagnostic_supplement": diagnostic,
        "materialized_at": utc_now_iso(),
    }


def write_result_diagnostic_supplement(
    root: Path,
    *,
    result_dir: Path,
    job_root: Path,
    record: dict[str, Any],
) -> str:
    terminal_path = job_root / "terminal.json"
    if not terminal_path.is_file():
        return ""
    terminal = read_object(terminal_path)
    if str(terminal.get("state") or "") != "failed":
        return ""
    history = terminal.get("history")
    failed_stage: dict[str, Any] | None = None
    if isinstance(history, list):
        for item in reversed(history):
            if not isinstance(item, dict):
                continue
            try:
                failed = int(item.get("exit_code", 0) or 0) != 0
            except (TypeError, ValueError):
                failed = False
            if failed:
                failed_stage = item
                break
    if failed_stage is None:
        return ""

    source_root = job_root / "result_bundle"
    result_relative = str(failed_stage.get("result_path") or "").replace("\\", "/")
    stage_result: dict[str, Any] = {}
    if result_relative and ".." not in Path(result_relative).parts:
        stage_result_path = source_root / result_relative
        if stage_result_path.is_file():
            stage_result = read_object(stage_result_path)
    stdout_relative = str(
        failed_stage.get("stdout_path") or stage_result.get("stdout_path") or ""
    ).replace("\\", "/")
    stderr_relative = str(
        failed_stage.get("stderr_path") or stage_result.get("stderr_path") or ""
    ).replace("\\", "/")
    log_texts: list[str] = []
    for relative in (stderr_relative, stdout_relative):
        if not relative or ".." in Path(relative).parts:
            continue
        path = source_root / relative
        if path.is_file():
            log_texts.append(path.read_text(encoding="utf-8", errors="replace"))
    first_failure = first_source_failure_line("\n".join(log_texts))
    command = failed_stage.get("command") or stage_result.get("command")
    command_json = (
        json.dumps(command, ensure_ascii=False, indent=2)
        if isinstance(command, list)
        else json.dumps(str(command or ""), ensure_ascii=False)
    )
    supplement = result_dir / "RESULT_DIAGNOSTIC.md"
    op = str(record.get("operator") or "")
    test_version = str(record.get("test_version") or "")
    stage_name = str(
        failed_stage.get("stage_name") or stage_result.get("stage_name") or ""
    )
    exit_code = failed_stage.get("exit_code")
    if exit_code in {None, ""}:
        exit_code = stage_result.get("exit_code", "")
    archive_log = (
        result_dir
        / "gitpartner_output"
        / "result_bundle"
        / (stdout_relative or stderr_relative)
    )
    body = (
        f"# Result Diagnostic {op} {test_version}\n\n"
        f"Engine job: `{record.get('engine_job_id', '')}`\n"
        f"Stage: `{stage_name}`\n"
        f"Exit code: `{exit_code}`\n"
        f"Generated: `{utc_now_iso()}`\n\n"
        "## Exact Command\n\n"
        "```json\n"
        f"{command_json}\n"
        "```\n\n"
        "## First Failing Line\n\n"
        f"`{first_failure}`\n\n"
        "## Evidence\n\n"
        f"- stage result: `{relative_path(result_dir / 'gitpartner_output' / 'result_bundle' / result_relative, root)}`\n"
        f"- stdout/stderr: `{relative_path(archive_log, root)}`\n"
        f"- optional return cache: `{relative_path(job_root, root)}`\n"
    )
    write_text_atomic(supplement, body)
    return relative_path(supplement, root)


def first_source_failure_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for markers in (
        ("error:",),
        ("CMake Error",),
        ("Kernel Compilation Error", "Opc tool compile failed"),
        ("BUILD_FAILED", "OPERATOR_CACHE_FAILED", "INSTALL_FAILED"),
    ):
        for line in lines:
            if any(marker in line for marker in markers):
                return line[:1000]
    return lines[0][:1000] if lines else "diagnostic log contained no non-empty line"


def stage_duration_summary(history: object) -> dict[str, float]:
    if not isinstance(history, list):
        return {}
    result: dict[str, float] = {}
    for item in history:
        if not isinstance(item, dict):
            continue
        name = str(item.get("stage_name") or "")
        try:
            started = datetime.fromisoformat(
                str(item.get("started_at") or "").replace("Z", "+00:00")
            )
            finished = datetime.fromisoformat(
                str(item.get("finished_at") or "").replace("Z", "+00:00")
            )
        except ValueError:
            continue
        if name:
            result[name] = round(max(0.0, (finished - started).total_seconds()), 6)
    return result


def terminal_failure_summary(terminal: object) -> tuple[str, int]:
    if not isinstance(terminal, dict):
        return "", 0
    history = terminal.get("history")
    if not isinstance(history, list):
        return "", 0
    for item in reversed(history):
        if not isinstance(item, dict):
            continue
        try:
            exit_code = int(item.get("exit_code", 0) or 0)
        except (TypeError, ValueError):
            continue
        if exit_code:
            return str(item.get("stage_name") or ""), exit_code
    return "", 0


def duration_between(start_text: str, finish_text: str) -> float | None:
    try:
        started = datetime.fromisoformat(str(start_text or "").replace("Z", "+00:00"))
        finished = datetime.fromisoformat(str(finish_text or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if started.tzinfo is None or finished.tzinfo is None:
        return None
    return round(max(0.0, (finished - started).total_seconds()), 6)


def parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


class PumpLock:
    def __init__(
        self,
        path: Path,
        *,
        wait_timeout_seconds: float = 0.0,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        self.path = path
        self.held = False
        self.wait_timeout_seconds = max(0.0, float(wait_timeout_seconds))
        self.poll_interval_seconds = max(
            0.01, float(poll_interval_seconds)
        )

    def __enter__(self) -> "PumpLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.wait_timeout_seconds
        while True:
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except FileExistsError as exc:
                try:
                    owner = read_object(self.path)
                except (OSError, ValueError, json.JSONDecodeError, EnginePumpError):
                    if time.monotonic() < deadline:
                        time.sleep(self.poll_interval_seconds)
                        continue
                    raise EnginePumpError(
                        "engine pump cycle is active or lock metadata is not yet readable"
                    ) from exc
                if process_alive(int(owner.get("pid", 0) or 0)):
                    if time.monotonic() < deadline:
                        time.sleep(self.poll_interval_seconds)
                        continue
                    raise EnginePumpError(
                        "engine pump cycle is already active"
                    ) from exc
                try:
                    self.path.unlink(missing_ok=True)
                except OSError as unlink_exc:
                    if time.monotonic() < deadline:
                        time.sleep(self.poll_interval_seconds)
                        continue
                    raise EnginePumpError(
                        "engine pump cycle is active or stale lock removal is blocked"
                    ) from unlink_exc
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps({"pid": os.getpid(), "started_at": utc_now_iso()}) + "\n"
            )
        self.held = True
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.held:
            self.path.unlink(missing_ok=True)


def copy_bundle(source: Path, destination: Path) -> None:
    if not source.is_dir():
        raise EnginePumpError(f"payload root is not a directory: {source}")
    for path in source.rglob("*"):
        if path.is_symlink():
            raise EnginePumpError(f"payload bundle cannot contain symlinks: {path}")
    shutil.copytree(source, destination)


def read_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise EnginePumpError(f"JSON document must be an object: {path}")
    return raw


def write_json(
    path: Path,
    payload: object,
    *,
    replace_timeout_seconds: float = 2.0,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        deadline = time.monotonic() + max(0.0, replace_timeout_seconds)
        delay = 0.02
        while True:
            try:
                os.replace(temporary, path)
                return
            except OSError as exc:
                retryable = isinstance(exc, PermissionError) or getattr(
                    exc, "winerror", None
                ) in {5, 32, 33}
                if not retryable or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 0.2)
    finally:
        temporary.unlink(missing_ok=True)


def write_text_atomic(
    path: Path,
    text: str,
    *,
    replace_timeout_seconds: float = 2.0,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(text, encoding="utf-8")
        deadline = time.monotonic() + max(0.0, replace_timeout_seconds)
        delay = 0.02
        while True:
            try:
                os.replace(temporary, path)
                return
            except OSError as exc:
                retryable = isinstance(exc, PermissionError) or getattr(
                    exc, "winerror", None
                ) in {5, 32, 33}
                if not retryable or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 0.2)
    finally:
        temporary.unlink(missing_ok=True)


def json_digest(payload: object) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def relative_path(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def cycle_token() -> str:
    wall = utc_now_iso().replace("+00:00", "Z").replace(":", "").replace("-", "")
    return f"{wall}-{time.time_ns() % 1_000_000_000:09d}"


def return_receipt_id(record: dict[str, Any]) -> str:
    existing = str(record.get("return_receipt_id") or "")
    if existing:
        return existing
    seed = "|".join(
        [
            str(record.get("engine_job_id") or ""),
            str(record.get("attempt_id") or ""),
            str(record.get("updated_at") or ""),
        ]
    )
    return "engine-return-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def required_return_receipt_id(record: dict[str, Any]) -> str:
    existing = str(record.get("required_return_receipt_id") or "")
    if existing:
        return existing
    seed = "|".join(
        [
            str(record.get("engine_job_id") or ""),
            str(record.get("attempt_id") or ""),
            "required",
        ]
    )
    return "engine-required-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def stop_requested(root: Path) -> bool:
    state_dir = root / "TestUtils" / "tester_daemon"
    return any(
        path.exists()
        for path in (
            state_dir / "daemon_stop.json",
            state_dir / "STOP_REQUEST.json",
        )
    )
