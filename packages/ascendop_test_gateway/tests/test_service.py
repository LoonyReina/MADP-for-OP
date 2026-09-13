from __future__ import annotations

from pathlib import Path

import pytest

from ascendop_test_gateway.adapters.memory import MemoryTransport
from ascendop_test_gateway.contracts import (
    StandaloneTestRequest,
    TestState as RunState,
    TransportReceipt,
    TransportStatus,
)
from ascendop_test_gateway.journal import RunJournal
from ascendop_test_gateway.ports import (
    TransportAdmissionError,
    TransportRequestError,
)
from gateway_fixture import StandaloneTestGateway


def _tree(root: Path, name: str) -> Path:
    path = root / name
    path.mkdir()
    (path / "file.txt").write_text(name, encoding="utf-8")
    return path


def _gateway(tmp_path: Path) -> tuple[StandaloneTestGateway, MemoryTransport]:
    transport = MemoryTransport()
    return StandaloneTestGateway(tmp_path / "runs", transport), transport


def _prepare(gateway: StandaloneTestGateway, tmp_path: Path):
    return gateway.prepare(
        StandaloneTestRequest(
            workspace=_tree(tmp_path, "source"),
            task_case=_tree(tmp_path, "cases"),
            op="Demo",
            release="Demo_V1",
            test_version="Demo_V1_1",
            hardware="910B3",
        )
    )


def test_submit_is_idempotent_and_restart_safe(tmp_path: Path) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)

    first = gateway.submit(bundle)
    restarted = StandaloneTestGateway(gateway.runs_root, transport)
    second = restarted.submit(restarted.bundle(bundle.request_id))

    assert first["state"] == "accepted"
    assert second["state"] == "accepted"
    assert transport.submit_count == 1


def test_local_preflight_failure_is_terminal_without_publication(
    tmp_path: Path,
) -> None:
    class InvalidRequestTransport(MemoryTransport):
        def preflight(self, bundle):
            raise TransportRequestError("invalid case range")

    transport = InvalidRequestTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)

    failed = gateway.submit(bundle)

    assert failed["state"] == "failed"
    assert failed["remote_status"]["classification"] == "invalid-request"
    assert failed["remote_status"]["result"]["error"] == "invalid case range"
    assert transport.submit_count == 0


def test_submit_time_validation_failure_is_terminal_invalid_request(
    tmp_path: Path,
) -> None:
    class LateInvalidRequestTransport(MemoryTransport):
        def submit(self, bundle):
            raise TransportRequestError("compiled payload is invalid")

    transport = LateInvalidRequestTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)

    failed = gateway.submit(bundle)

    assert failed["state"] == "failed"
    assert failed["remote_status"]["classification"] == "invalid-request"
    assert failed["remote_status"]["metrics"]["publication_started"] is False


def test_endpoint_nack_is_terminal_after_publication(tmp_path: Path) -> None:
    class RejectedTransport(MemoryTransport):
        def submit(self, bundle):
            raise TransportAdmissionError(
                "engine has no fresh standby credit",
                details={
                    "schema": "ascendop.flow.endpoint-nack.v3",
                    "code": "engine-validation-error",
                    "retryable": False,
                },
            )

    transport = RejectedTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)

    failed = gateway.submit(bundle)

    assert failed["state"] == "failed"
    assert failed["events"][-1]["reason"] == "endpoint-admission-rejected"
    assert failed["remote_status"]["classification"] == (
        "endpoint-admission-rejected:engine-validation-error"
    )
    assert failed["remote_status"]["metrics"]["publication_started"] is True
    assert failed["remote_status"]["metrics"]["return_path_ready"] is True


def test_retryable_capacity_nack_preserves_typed_retry_evidence(tmp_path: Path) -> None:
    class DrainingTransport(MemoryTransport):
        def submit(self, bundle):
            raise TransportAdmissionError(
                "engine is draining",
                details={
                    "schema": "ascendop.flow.endpoint-nack.v3",
                    "code": "engine-draining",
                    "retryable": True,
                    "failure_domain": "engine-capacity",
                    "failure_phase": "admission",
                },
            )

    gateway = StandaloneTestGateway(
        tmp_path / "runs", DrainingTransport()
    )
    failed = gateway.submit(_prepare(gateway, tmp_path))

    assert failed["state"] == "failed"
    assert failed["transport_receipt"] is None
    assert failed["remote_status"]["classification"] == (
        "endpoint-admission-rejected:engine-draining"
    )
    assert failed["remote_status"]["metrics"]["retryable"] is True
    assert failed["remote_status"]["result"]["endpoint_nack"][
        "failure_domain"
    ] == "engine-capacity"


def test_uncertain_endpoint_nack_recovers_without_resubmission(tmp_path: Path) -> None:
    class RejectedTransport(MemoryTransport):
        def reconcile_rejection(self, bundle):
            return TransportStatus(
                request_id=bundle.request_id,
                state=RunState.FAILED,
                classification=(
                    "endpoint-admission-rejected:engine-validation-error"
                ),
                result={"error": "engine draining"},
                metrics={"recovered_without_resubmission": True},
            )

    transport = RejectedTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)
    journal.transition(RunState.UNCERTAIN)

    failed = gateway.submit(bundle)

    assert failed["state"] == "failed"
    assert failed["remote_status"]["metrics"][
        "recovered_without_resubmission"
    ] is True
    assert transport.submit_count == 0


def test_uncertain_local_preflight_failure_converges_to_failed(tmp_path: Path) -> None:
    class InvalidRequestTransport(MemoryTransport):
        def preflight(self, bundle):
            raise TransportRequestError("invalid case range")

    transport = InvalidRequestTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)
    journal.transition(RunState.UNCERTAIN)

    failed = gateway.submit(bundle, retry_uncertain=True)

    assert failed["state"] == "failed"
    assert failed["events"][-1]["reason"] == "invalid-request"
    assert transport.submit_count == 0


def test_remote_completion_is_recorded_once(tmp_path: Path) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    gateway.submit(bundle)
    transport.advance(bundle.request_id, RunState.COMPLETED)

    completed = gateway.wait(bundle.request_id, timeout_seconds=1, poll_seconds=0.01)
    repeated = gateway.status(bundle.request_id)

    assert completed["state"] == "completed"
    assert repeated == completed
    assert completed["events"][-1]["to"] == "completed"


def test_wire_terminal_event_is_attached_to_journal(tmp_path: Path) -> None:
    class WireTerminalTransport(MemoryTransport):
        def submit(self, bundle):
            self.submit_count += 1
            self.payload = bundle.run_dir / "results" / "attempt-003" / "payload"
            (self.payload / "result_bundle").mkdir(parents=True)
            for name in ("terminal.json", "state.json", "artifact_manifest.json"):
                (self.payload / name).write_text("{}", encoding="utf-8")
            (self.payload.parent / ".payload.sha256").write_text("b" * 64, encoding="ascii")
            return TransportReceipt(
                request_id=bundle.request_id,
                remote_attempt_id="attempt-003",
                output_subdir="flow-v3/exact",
                details={
                    "schema": "ascendop.standalone-wire-v3-receipt.v1",
                    "envelope_digest": "a" * 64,
                },
            )

        def status(self, receipt):
            return TransportStatus(
                request_id=receipt.request_id,
                state=RunState.COMPLETED,
                classification="wire-v3:terminal:success:ack-pending",
                result={
                    "schema": "ascendop.standalone-wire-v3-result.v1",
                    "request_id": receipt.request_id,
                    "attempt_id": receipt.remote_attempt_id,
                    "receipt_id": "receipt-exact-1",
                    "terminal_revision": 1,
                    "result_payload_sha256": "b" * 64,
                    "outcome": "success",
                    "failure_domain": "",
                    "artifact_root": str(self.payload),
                },
                metrics={"return_path_ready": False},
            )

    transport = WireTerminalTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)

    terminal = gateway.submit(bundle)

    event = terminal["terminal_ingest_event"]
    assert event["schema"] == "ascendop.gp-terminal-ingest-event.v1"
    assert event["request_id"] == bundle.request_id
    assert event["ack"] == {
        "request_id": bundle.request_id,
        "attempt_id": "attempt-003",
        "receipt_id": "receipt-exact-1",
    }


def test_terminal_status_does_not_reconcile_ack_or_resubmit(
    tmp_path: Path,
) -> None:
    class DeferredAckTransport(MemoryTransport):
        status_count = 0

        def status(self, receipt):
            self.status_count += 1
            return TransportStatus(
                request_id=receipt.request_id,
                state=RunState.COMPLETED,
                classification=(
                    "wire-v3:terminal:success:ack-pending"
                    if self.status_count == 1
                    else "wire-v3:terminal:success:acknowledged"
                ),
                metrics={"return_path_ready": self.status_count > 1},
            )

    transport = DeferredAckTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)

    first = gateway.submit(bundle)
    reconciled = gateway.status(bundle.request_id)

    assert first["state"] == "completed"
    assert first["remote_status"]["metrics"]["return_path_ready"] is False
    assert reconciled["remote_status"]["metrics"]["return_path_ready"] is False
    assert transport.status_count == 1
    assert transport.submit_count == 1


def test_wait_returns_terminal_without_owning_ack_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DeferredAckTransport(MemoryTransport):
        status_count = 0

        def status(self, receipt):
            self.status_count += 1
            return TransportStatus(
                request_id=receipt.request_id,
                state=RunState.COMPLETED,
                classification=(
                    "wire-v3:terminal:success:ack-pending"
                    if self.status_count < 3
                    else "wire-v3:terminal:success:acknowledged"
                ),
                metrics={"return_path_ready": self.status_count >= 3},
            )

    transport = DeferredAckTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    sleeps: list[float] = []
    monkeypatch.setattr("ascendop_test_gateway.runtime.time.sleep", sleeps.append)

    submitted = gateway.submit(bundle)
    completed = gateway.wait(
        bundle.request_id,
        timeout_seconds=60,
        poll_seconds=0.5,
        terminal_ack_grace_seconds=10,
    )

    assert submitted["remote_status"]["classification"].endswith(":ack-pending")
    assert completed["state"] == "completed"
    assert completed["remote_status"]["classification"].endswith(":ack-pending")
    assert completed["remote_status"]["metrics"]["return_path_ready"] is False
    assert transport.status_count == 1
    assert transport.submit_count == 1
    assert sleeps == []


def test_same_state_remote_progress_is_persisted_without_state_event(
    tmp_path: Path,
) -> None:
    gateway, _ = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)
    journal.transition(RunState.ACCEPTED)
    event_count = len(journal.read()["events"])

    observed = journal.transition(
        RunState.ACCEPTED,
        reason="heartbeat",
        remote_status={"state": "accepted", "metrics": {"sequence": 2}},
    )

    assert observed["remote_status"]["metrics"]["sequence"] == 2
    assert len(observed["events"]) == event_count


def test_cancel_before_and_after_submit(tmp_path: Path) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    assert gateway.cancel(bundle.request_id)["state"] == "cancelled"

    other_root = tmp_path / "other"
    other_root.mkdir()
    gateway2, transport2 = _gateway(other_root)
    bundle2 = _prepare(gateway2, other_root)
    gateway2.submit(bundle2)
    assert gateway2.cancel(bundle2.request_id)["state"] == "cancelled"
    assert transport2.states[bundle2.request_id] == RunState.CANCELLED


def test_async_cancellation_survives_gateway_restart(tmp_path: Path) -> None:
    class DeferredCancellationTransport(MemoryTransport):
        cancel_count = 0

        def cancel(self, receipt):
            self.cancel_count += 1
            state = (
                RunState.CANCELLING if self.cancel_count == 1 else RunState.CANCELLED
            )
            self.states[receipt.request_id] = state
            return TransportStatus(
                request_id=receipt.request_id,
                state=state,
                classification="deferred-cancellation",
            )

    transport = DeferredCancellationTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    gateway.submit(bundle)

    first = gateway.cancel(bundle.request_id)
    restarted = StandaloneTestGateway(gateway.runs_root, transport)
    second = restarted.status(bundle.request_id)

    assert first["state"] == "cancelling"
    assert second["state"] == "cancelled"
    assert second["cancellation"]["reason"]
    assert transport.cancel_count == 2


def test_cancellation_reconciles_uncertain_publication_without_resubmit(
    tmp_path: Path,
) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)

    uncertain = gateway.cancel(bundle.request_id)
    assert uncertain["state"] == "uncertain"
    assert uncertain["cancellation"] is not None
    assert transport.submit_count == 0

    transport.states[bundle.request_id] = RunState.ACCEPTED
    restarted = StandaloneTestGateway(gateway.runs_root, transport)
    terminal = restarted.status(bundle.request_id)

    assert terminal["state"] == "cancelled"
    assert transport.submit_count == 0


def test_remote_terminal_wins_cancellation_race(tmp_path: Path) -> None:
    class CompletionWinsTransport(MemoryTransport):
        def cancel(self, receipt):
            self.states[receipt.request_id] = RunState.COMPLETED
            return TransportStatus(
                request_id=receipt.request_id,
                state=RunState.COMPLETED,
                classification="terminal-conflict:completed",
            )

    transport = CompletionWinsTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    gateway.submit(bundle)

    terminal = gateway.cancel(bundle.request_id)

    assert terminal["state"] == "completed"
    assert terminal["events"][-1]["reason"] == "terminal-conflict:completed"


def test_terminal_journal_cannot_be_rewritten(tmp_path: Path) -> None:
    gateway, _ = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.CANCELLED)

    with pytest.raises(ValueError, match="invalid test transition"):
        journal.transition(RunState.PUBLISHING)


def test_fake_execution_does_not_touch_madp_state(tmp_path: Path) -> None:
    protected = [
        tmp_path / ".ascendop-work",
        tmp_path / "TestUtils" / "pending",
        tmp_path / "submit",
        tmp_path / "operators_testresult",
    ]
    for path in protected:
        path.mkdir(parents=True)
        (path / "sentinel.txt").write_text("unchanged", encoding="utf-8")
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    gateway.submit(bundle)
    transport.advance(bundle.request_id, RunState.COMPLETED)
    gateway.wait(bundle.request_id, timeout_seconds=1, poll_seconds=0.01)

    assert all(
        (path / "sentinel.txt").read_text(encoding="utf-8") == "unchanged"
        for path in protected
    )
    assert all(list(path.iterdir()) == [path / "sentinel.txt"] for path in protected)


def test_uncertain_submission_requires_explicit_retry(tmp_path: Path) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)

    first = gateway.submit(bundle)
    second = gateway.submit(bundle, retry_uncertain=True)

    assert first["state"] == "uncertain"
    assert second["state"] == "accepted"
    assert transport.submit_count == 1


def test_status_keeps_interrupted_publication_recoverable(tmp_path: Path) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)
    event_count = len(journal.read()["events"])

    pending = gateway.status(bundle.request_id)

    assert pending["state"] == "publishing"
    assert len(pending["events"]) == event_count
    assert pending["remote_status"]["classification"] == (
        "transport-reconciliation-pending"
    )
    assert pending["remote_status"]["metrics"]["watchdog"] == ("reconciliation-pending")
    assert transport.submit_count == 0

    transport.states[bundle.request_id] = RunState.ACCEPTED
    recovered = gateway.status(bundle.request_id)

    assert recovered["state"] == "accepted"
    assert recovered["events"][-1]["reason"] == "transport-reconciled"
    assert transport.submit_count == 0


def test_wait_returns_uncertain_after_one_reconciliation_attempt(
    tmp_path: Path,
) -> None:
    class CountingReconcileTransport(MemoryTransport):
        reconcile_count = 0

        def reconcile(self, bundle):
            self.reconcile_count += 1
            return super().reconcile(bundle)

    transport = CountingReconcileTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)
    journal.transition(RunState.UNCERTAIN)

    outcome = gateway.wait(bundle.request_id, timeout_seconds=3600)

    assert outcome["state"] == "uncertain"
    assert transport.reconcile_count == 1


def test_wait_backs_off_for_accepted_remote_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    gateway.submit(bundle)
    sleeps: list[float] = []

    def advance_after_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 3:
            transport.advance(bundle.request_id, RunState.COMPLETED)

    monkeypatch.setattr("ascendop_test_gateway.runtime.time.sleep", advance_after_sleep)

    outcome = gateway.wait(
        bundle.request_id,
        timeout_seconds=3600,
        poll_seconds=2.0,
        max_poll_seconds=3.0,
    )

    assert outcome["state"] == "completed"
    assert sleeps == [2.0, 3.0, 3.0]


def test_late_result_after_status_outage_reuses_same_attempt_on_restart(
    tmp_path: Path,
) -> None:
    class LateResultTransport(MemoryTransport):
        status_count = 0

        def status(self, receipt):
            self.status_count += 1
            state = RunState.ACCEPTED if self.status_count == 1 else RunState.COMPLETED
            self.states[receipt.request_id] = state
            return TransportStatus(
                request_id=receipt.request_id,
                state=state,
                classification=(
                    "status-timeout" if state == RunState.ACCEPTED else "late-result"
                ),
                metrics={"return_path_ready": state == RunState.COMPLETED},
            )

    transport = LateResultTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    accepted = gateway.submit(bundle)
    receipt = accepted["transport_receipt"]

    restarted = StandaloneTestGateway(gateway.runs_root, transport)
    completed = restarted.status(bundle.request_id)

    assert completed["state"] == "completed"
    assert completed["transport_receipt"] == receipt
    assert transport.submit_count == 1


def test_no_progress_watchdog_does_not_reject_accepted_device_queue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, transport = _gateway(tmp_path)
    bundle = _prepare(gateway, tmp_path)
    gateway.submit(bundle)
    clock = [0.0]
    monkeypatch.setattr(
        "ascendop_test_gateway.runtime.time.monotonic", lambda: clock[0]
    )
    sleeps = 0

    def advance_after_sleep(seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        clock[0] += seconds
        if sleeps == 3:
            transport.advance(bundle.request_id, RunState.COMPLETED)

    monkeypatch.setattr("ascendop_test_gateway.runtime.time.sleep", advance_after_sleep)

    outcome = gateway.wait(
        bundle.request_id,
        timeout_seconds=100,
        poll_seconds=2,
        max_poll_seconds=2,
        no_progress_seconds=3,
    )

    assert outcome["state"] == "completed"
    assert transport.submit_count == 1


def test_no_progress_watchdog_returns_for_stale_running_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StaleRunningTransport(MemoryTransport):
        def status(self, receipt):
            self.states[receipt.request_id] = RunState.RUNNING
            return TransportStatus(
                request_id=receipt.request_id,
                state=RunState.RUNNING,
                classification="engine-running",
                metrics={
                    "engine_state": "running",
                    "progress_heartbeat_fresh": False,
                    "watchdog": "no-progress-evidence",
                    "return_path_ready": True,
                },
            )

    transport = StaleRunningTransport()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = _prepare(gateway, tmp_path)
    gateway.submit(bundle)
    clock = [0.0]
    monkeypatch.setattr(
        "ascendop_test_gateway.runtime.time.monotonic", lambda: clock[0]
    )
    monkeypatch.setattr(
        "ascendop_test_gateway.runtime.time.sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    outcome = gateway.wait(
        bundle.request_id,
        timeout_seconds=100,
        poll_seconds=2,
        max_poll_seconds=2,
        no_progress_seconds=3,
    )

    assert outcome["state"] == "running"
    assert outcome["remote_status"]["classification"] == "no-progress-watchdog"
    assert transport.submit_count == 1
