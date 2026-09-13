from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ascendop_protocol.file_lock import exclusive_file_lock

from .contracts import (
    PreparedBundle,
    StandaloneTestRequest,
    TERMINAL_STATES,
    TestState,
    TransportReceipt,
    TransportStatus,
    terminal_ingest_event,
)
from .journal import RunJournal
from .ports import BundleStorePort, GPTransportPort, TransportAdmissionError, TransportRequestError
from .terminal_evidence import retain_terminal_evidence


class GatewayRuntime:
    def __init__(self, runs_root: Path | str, transport: GPTransportPort, *, bundle_store: BundleStorePort) -> None:
        self.runs_root = Path(runs_root).resolve()
        self.transport = transport
        self.bundle_store = bundle_store

    def prepare(self, request: StandaloneTestRequest) -> PreparedBundle:
        bundle = self.bundle_store.prepare(request, self.runs_root)
        RunJournal(bundle.run_dir).initialize(bundle.request_id)
        return bundle

    def bundle(self, request_id: str) -> PreparedBundle:
        return self.bundle_store.load(self.runs_root / request_id)

    def resume_prepublication(self, bundle: PreparedBundle) -> dict[str, Any]:
        """Resume a sealed request only when no publication or receipt existed."""

        return RunJournal(bundle.run_dir).resume_prepublication_failure()

    def submit(
        self,
        bundle: PreparedBundle,
        *,
        retry_uncertain: bool = False,
    ) -> dict[str, Any]:
        journal = RunJournal(bundle.run_dir)
        state = TestState(str(journal.read()["state"]))
        receipt = journal.receipt()
        if receipt is not None:
            return self.status(bundle.request_id)
        try:
            self.transport.preflight(bundle)
        except TransportRequestError as exc:
            return journal.transition(
                TestState.FAILED,
                reason="invalid-request",
                remote_status={
                    "schema": "ascendop.standalone-transport-status.v1",
                    "request_id": bundle.request_id,
                    "state": TestState.FAILED.value,
                    "classification": "invalid-request",
                    "result": {"error": str(exc)},
                    "metrics": {},
                },
            )
        if state in {TestState.PUBLISHING, TestState.UNCERTAIN}:
            reconcile_rejection = getattr(self.transport, "reconcile_rejection", None)
            rejection = (
                reconcile_rejection(bundle) if callable(reconcile_rejection) else None
            )
            if rejection is not None:
                return journal.transition(
                    TestState.FAILED,
                    reason=rejection.classification or "endpoint-admission-rejected",
                    remote_status=rejection.to_dict(),
                )
            receipt = self.transport.reconcile(bundle)
            if receipt is not None:
                journal.attach_receipt(receipt)
                journal.transition(TestState.ACCEPTED, reason="transport-reconciled")
                return self.status(bundle.request_id)
            if state == TestState.PUBLISHING:
                journal.transition(
                    TestState.UNCERTAIN,
                    reason="publication-without-durable-receipt",
                )
            if not retry_uncertain:
                return journal.read()
            state = TestState.UNCERTAIN
        if state not in {TestState.PREPARED, TestState.UNCERTAIN}:
            raise ValueError(f"cannot publish request from {state.value}")
        journal.transition(TestState.PUBLISHING, reason="transport-submit-started")
        try:
            receipt = self.transport.submit(bundle)
        except TransportAdmissionError as exc:
            details = dict(exc.details)
            code = str(details.get("code") or "endpoint-nack")
            return journal.transition(
                TestState.FAILED,
                reason="endpoint-admission-rejected",
                remote_status={
                    "schema": "ascendop.standalone-transport-status.v1",
                    "request_id": bundle.request_id,
                    "state": TestState.FAILED.value,
                    "classification": f"endpoint-admission-rejected:{code}",
                    "result": {
                        "error": str(exc),
                        "endpoint_nack": details,
                    },
                    "metrics": {
                        "publication_started": True,
                        "return_path_ready": True,
                        "retryable": bool(details.get("retryable", False)),
                    },
                },
            )
        except TransportRequestError as exc:
            return journal.transition(
                TestState.FAILED,
                reason="invalid-request",
                remote_status={
                    "schema": "ascendop.standalone-transport-status.v1",
                    "request_id": bundle.request_id,
                    "state": TestState.FAILED.value,
                    "classification": "invalid-request",
                    "result": {"error": str(exc)},
                    "metrics": {"publication_started": False},
                },
            )
        except Exception:
            journal.transition(TestState.UNCERTAIN, reason="transport-submit-uncertain")
            raise
        if receipt.request_id != bundle.request_id:
            journal.transition(TestState.UNCERTAIN, reason="transport-receipt-mismatch")
            raise ValueError("transport receipt request identity mismatch")
        journal.attach_receipt(receipt)
        journal.transition(TestState.ACCEPTED, reason="transport-accepted")
        return self.status(bundle.request_id)

    def status(self, request_id: str) -> dict[str, Any]:
        bundle = self.bundle(request_id)
        journal = RunJournal(bundle.run_dir)
        data = journal.read()
        local_state = TestState(str(data["state"]))
        receipt = journal.receipt()
        if local_state in TERMINAL_STATES:
            # Import a pre-V5 local terminal without querying or ACKing remotely.
            # Already retained results are immutable; status is not an ACK worker.
            if receipt is not None and data.get("terminal_retention") is None:
                raw = data.get("remote_status")
                if isinstance(raw, dict):
                    return self._accept_observation(
                        journal, receipt, _status_from_dict(raw)
                    )
            return data
        if receipt is None:
            if local_state in {TestState.PUBLISHING, TestState.UNCERTAIN}:
                receipt = self.transport.reconcile(bundle)
                if receipt is not None:
                    journal.attach_receipt(receipt)
                    journal.transition(
                        TestState.ACCEPTED,
                        reason="transport-reconciled",
                    )
                    local_state = TestState.ACCEPTED
                elif local_state == TestState.PUBLISHING:
                    return journal.transition(
                        TestState.PUBLISHING,
                        reason="transport-reconciliation-pending",
                        remote_status={
                            "schema": "ascendop.standalone-transport-status.v1",
                            "request_id": request_id,
                            "state": TestState.PUBLISHING.value,
                            "classification": "transport-reconciliation-pending",
                            "result": {},
                            "metrics": {
                                "return_path_ready": False,
                                "watchdog": "reconciliation-pending",
                            },
                        },
                    )
                else:
                    return data
            else:
                return data
        cancellation = journal.read().get("cancellation")
        remote = (
            self.transport.cancel(receipt)
            if isinstance(cancellation, dict)
            else self.transport.status(receipt)
        )
        if remote.request_id != request_id:
            journal.transition(TestState.UNCERTAIN, reason="remote-status-mismatch")
            raise ValueError("remote status request identity mismatch")
        event = _wire_terminal_ingest_event(receipt, remote)
        if event is not None:
            return self._accept_observation(journal, receipt, remote)
        target_state = remote.state
        progress_rank = {
            TestState.ACCEPTED: 1,
            TestState.RUNNING: 2,
            TestState.COLLECTING: 3,
        }
        if (
            local_state in progress_rank
            and target_state in progress_rank
            and progress_rank[target_state] < progress_rank[local_state]
        ):
            target_state = local_state
        data = journal.transition(
            target_state,
            reason=remote.classification or "remote-status",
            remote_status=remote.to_dict(),
        )
        return data

    def _accept_observation(
        self,
        journal: RunJournal,
        receipt: TransportReceipt,
        remote: TransportStatus,
    ) -> dict[str, Any]:
        if remote.request_id != receipt.request_id:
            raise ValueError("remote status request identity mismatch")
        event = _wire_terminal_ingest_event(receipt, remote)
        if event is not None:
            retained = retain_terminal_evidence(journal.run_dir, event)
            return journal.accept_terminal(receipt, remote, retained=retained)
        return journal.transition(
            remote.state, reason=remote.classification, remote_status=remote.to_dict()
        )

    def acknowledge(self, request_id: str, *, event_id: str) -> dict[str, Any]:
        """Retry one exact accepted return independently of result observation.

        Standalone callers own their consumption boundary. Managed callers invoke
        this only after their control transaction commits continuation intent.
        """
        journal = RunJournal(self.bundle(request_id).run_dir)
        with exclusive_file_lock(journal.run_dir / ".gateway-terminal-ack.lock", 30):
            local = journal.read()
            if local.get("terminal_retention") is None:
                receipt = journal.receipt()
                if receipt is None or TestState(local["state"]) not in TERMINAL_STATES:
                    raise ValueError("ACK requires a locally accepted terminal")
                remote = _status_from_dict(local["remote_status"])
                event = _wire_terminal_ingest_event(receipt, remote)
                if event is None or event["event_id"] != event_id:
                    raise ValueError("ACK terminal event identity mismatch")
                self._accept_observation(journal, receipt, remote)
            data = journal.require_terminal_acceptance(event_id)
            if data["terminal_ack"]["state"] == "delivered":
                return data
            journal.record_terminal_ack(event_id, begin=True)
            receipt = TransportReceipt.from_dict(data["transport_receipt"])
            try:
                acknowledged = self.transport.acknowledge(receipt, event_id=event_id)
                if not isinstance(acknowledged, bool):
                    raise ValueError("transport ACK must return an explicit boolean")
            except Exception as exc:
                return journal.record_terminal_ack(event_id, error=str(exc))
            return journal.record_terminal_ack(
                event_id,
                acknowledged=acknowledged,
                error="" if acknowledged else "remote acknowledgement pending",
            )

    def wait(
        self,
        request_id: str,
        *,
        timeout_seconds: float = 1800,
        poll_seconds: float = 2.0,
        max_poll_seconds: float = 30.0,
        no_progress_seconds: float = 90.0,
        terminal_ack_grace_seconds: float = 0.0,
    ) -> dict[str, Any]:
        journal = RunJournal(self.bundle(request_id).run_dir)
        deadline = time.monotonic() + timeout_seconds
        delay = max(0.01, poll_seconds)
        max_delay = max(delay, max_poll_seconds)
        progress_signature = ""
        progress_at = time.monotonic()
        while True:
            data = self.status(request_id)
            state = TestState(str(data["state"]))
            if state in TERMINAL_STATES:
                # Legacy grace argument is accepted but no longer delays results.
                return data
            if state == TestState.UNCERTAIN:
                return data
            signature = _progress_signature(data)
            if signature != progress_signature:
                progress_signature = signature
                progress_at = time.monotonic()
            elif (
                no_progress_seconds > 0
                and _watchdog_applicable(data)
                and time.monotonic() - progress_at >= no_progress_seconds
            ):
                remote = dict(data.get("remote_status") or {})
                metrics = dict(remote.get("metrics") or {})
                metrics.update(
                    {
                        "watchdog": "no-progress",
                        "no_progress_seconds": round(time.monotonic() - progress_at, 3),
                    }
                )
                remote.update(
                    {
                        "state": state.value,
                        "classification": "no-progress-watchdog",
                        "metrics": metrics,
                    }
                )
                return journal.transition(
                    state,
                    reason="no-progress-watchdog",
                    remote_status=remote,
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out waiting for standalone test {request_id}"
                )
            time.sleep(delay)
            delay = min(max_delay, delay * 1.5)

    def cancel(self, request_id: str) -> dict[str, Any]:
        bundle = self.bundle(request_id)
        journal = RunJournal(bundle.run_dir)
        data = journal.read()
        state = TestState(str(data["state"]))
        if state in TERMINAL_STATES:
            return data
        data = journal.request_cancellation(
            reason="standalone gateway cancellation requested"
        )
        receipt = journal.receipt()
        if receipt is None and state == TestState.PREPARED:
            return journal.transition(
                TestState.CANCELLED, reason="cancelled-before-submit"
            )
        if receipt is None:
            receipt = self.transport.reconcile(bundle)
            if receipt is None:
                if state == TestState.PUBLISHING:
                    return journal.transition(
                        TestState.UNCERTAIN,
                        reason="cancellation-publication-uncertain",
                    )
                return journal.read()
            journal.attach_receipt(receipt)
            if state in {TestState.PUBLISHING, TestState.UNCERTAIN}:
                journal.transition(
                    TestState.ACCEPTED, reason="transport-reconciled-for-cancel"
                )
        remote = self.transport.cancel(receipt)
        return self._accept_observation(journal, receipt, remote)


def _status_from_dict(raw: dict[str, Any]) -> TransportStatus:
    return TransportStatus(
        request_id=raw["request_id"],
        state=TestState(raw["state"]),
        classification=str(raw.get("classification") or ""),
        result=dict(raw.get("result") or {}),
        metrics=dict(raw.get("metrics") or {}),
    )


def _wire_terminal_ingest_event(
    receipt: TransportReceipt,
    status: TransportStatus,
) -> dict[str, Any] | None:
    if status.state not in TERMINAL_STATES:
        return None
    if str(receipt.details.get("schema") or "") != (
        "ascendop.standalone-wire-v3-receipt.v1"
    ):
        return None
    return terminal_ingest_event(receipt, status)


def _progress_signature(data: dict[str, Any]) -> str:
    remote = data.get("remote_status")
    remote = remote if isinstance(remote, dict) else {}
    metrics = remote.get("metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    heartbeat = metrics.get("progress_heartbeat")
    heartbeat = heartbeat if isinstance(heartbeat, dict) else {}
    stable = {
        "state": data.get("state"),
        "classification": remote.get("classification"),
        "engine_state": metrics.get("engine_state"),
        "engine_updated_at": metrics.get("engine_updated_at"),
        "stage_name": metrics.get("stage_name"),
        "heartbeat_sequence": heartbeat.get("sequence"),
        "heartbeat_phase": heartbeat.get("phase"),
        "return_path_ready": metrics.get("return_path_ready"),
    }
    return json.dumps(stable, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _watchdog_applicable(data: dict[str, Any]) -> bool:
    """Only diagnose stalled execution, never ordinary admission/device queues."""

    remote = data.get("remote_status")
    remote = remote if isinstance(remote, dict) else {}
    metrics = remote.get("metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    engine_state = str(metrics.get("engine_state") or "").lower()
    if engine_state not in {"running", "activated", "executing"}:
        return False
    return not bool(metrics.get("progress_heartbeat_fresh"))
