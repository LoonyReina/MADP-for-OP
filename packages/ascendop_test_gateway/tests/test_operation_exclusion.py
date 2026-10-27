"""Gateway lifecycle races using synthetic transports, never external execution."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import multiprocessing
from pathlib import Path
from threading import Event

import pytest

from ascendop_protocol.file_lock import exclusive_file_lock
from ascendop_test_gateway import runtime as runtime_module
from ascendop_test_gateway.adapters.memory import MemoryTransport
from ascendop_test_gateway.contracts import (
    StandaloneTestRequest, TestState as RunState, TransportReceipt,
)
from ascendop_test_gateway.journal import RunJournal
from gateway_fixture import StandaloneTestGateway


def prepare(root, transport):
    gateway = StandaloneTestGateway(root / "runs", transport)
    bundle = gateway.prepare(StandaloneTestRequest(
        workspace=root, task_case=root, op="Demo", release="v1",
        test_version="v1", hardware="CPU", request_id="request-1",
    ))
    return gateway, bundle


class ProcessTransport(MemoryTransport):
    def __init__(self, count, entered, release):
        super().__init__()
        self.count, self.entered, self.release = count, entered, release

    def preflight(self, bundle):
        self.entered.set()
        assert self.release.wait(10), "publisher was not released"

    def submit(self, bundle):
        with self.count.get_lock():
            self.count.value += 1
        return TransportReceipt(bundle.request_id, "shared-attempt", "shared-output")

    def status(self, receipt):
        self.states[receipt.request_id] = RunState.ACCEPTED
        return super().status(receipt)


def process_submit(root, count, entered, release, called, results):
    transport = ProcessTransport(count, entered, release)
    gateway = StandaloneTestGateway(root / "runs", transport)
    called.set()
    try:
        results.put(gateway.submit(gateway.bundle("request-1"))["state"])
    except Exception as exc:
        results.put(type(exc).__name__ + ": " + str(exc))


def test_two_processes_share_publication_exclusion(tmp_path):
    prepare(tmp_path, MemoryTransport())
    context = multiprocessing.get_context("spawn")
    count, results = context.Value("i", 0), context.Queue()
    entered1, entered2, release = context.Event(), context.Event(), context.Event()
    called1, called2 = context.Event(), context.Event()
    first = context.Process(target=process_submit, args=(tmp_path, count, entered1, release, called1, results))
    second = context.Process(target=process_submit, args=(tmp_path, count, entered2, release, called2, results))
    first.start()
    try:
        assert entered1.wait(10)
        second.start()
        assert called2.wait(10)
        # The contender cannot reach preflight while the first owns the request.
        assert not entered2.wait(0.5)
        release.set()
        assert [results.get(timeout=10), results.get(timeout=10)] == ["accepted", "accepted"]
        assert count.value == 1
    finally:
        release.set()
        for child in (first, second):
            if child.pid is not None:
                child.join(10)
                if child.is_alive():
                    child.terminate(); child.join(5)
        results.close()
    assert first.exitcode == second.exitcode == 0


@pytest.mark.parametrize("operation", ["submit", "status", "cancel"])
def test_contender_cannot_reconcile_an_active_publisher(tmp_path, operation, monkeypatch):
    entered, release, attempted = Event(), Event(), Event()

    class BlockingTransport(MemoryTransport):
        def submit(self, bundle):
            entered.set()
            assert release.wait(5)
            return super().submit(bundle)

        def reconcile(self, bundle):
            pytest.fail("active publisher must not be reconciled by a contender")

    transport = BlockingTransport()
    first, bundle = prepare(tmp_path, transport)
    second = StandaloneTestGateway(first.runs_root, transport)
    actual_lock = runtime_module.exclusive_file_lock

    @contextmanager
    def observed_lock(path, *args, **kwargs):
        if entered.is_set() and Path(path).name == ".gateway-operation.lock":
            attempted.set()
        with actual_lock(path, *args, **kwargs):
            yield

    monkeypatch.setattr(runtime_module, "exclusive_file_lock", observed_lock)
    with ThreadPoolExecutor(max_workers=2) as pool:
        publishing = pool.submit(first.submit, bundle)
        try:
            assert entered.wait(5)
            argument = second.bundle(bundle.request_id) if operation == "submit" else bundle.request_id
            competing = pool.submit(getattr(second, operation), argument)
            assert attempted.wait(5)
            assert not competing.done()
        finally:
            release.set()
        assert publishing.result(timeout=5)["state"] == "accepted"
        assert competing.result(timeout=5)["state"] == ("cancelled" if operation == "cancel" else "accepted")
    assert transport.submit_count == 1


def test_restart_honors_durable_prepared_cancellation(tmp_path):
    transport = MemoryTransport()
    gateway, bundle = prepare(tmp_path, transport)
    journal = RunJournal(bundle.run_dir)
    # Exact durable crash cut between cancellation intent and local terminal.
    journal.request_cancellation(reason="stop before publication")
    restarted = StandaloneTestGateway(gateway.runs_root, transport)
    assert restarted.submit(restarted.bundle(bundle.request_id))["state"] == "cancelled"
    assert restarted.submit(restarted.bundle(bundle.request_id))["state"] == "cancelled"
    assert transport.submit_count == 0


@pytest.mark.parametrize("state", [RunState.PUBLISHING, RunState.UNCERTAIN])
@pytest.mark.parametrize("remote_exists", [False, True])
def test_uncertain_cancellation_reconciles_without_retry(tmp_path, state, remote_exists):
    transport = MemoryTransport()
    gateway, bundle = prepare(tmp_path, transport)
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)
    if state == RunState.UNCERTAIN:
        journal.transition(state)
    journal.request_cancellation(reason="durable cancellation")
    if remote_exists:
        transport.states[bundle.request_id] = RunState.ACCEPTED
    result = gateway.submit(bundle, retry_uncertain=True)
    assert result["state"] == ("cancelled" if remote_exists else "uncertain")
    assert transport.submit_count == 0


def exit_while_publishing(root, entered):
    gateway = StandaloneTestGateway(root / "runs", MemoryTransport())
    bundle = gateway.bundle("request-1")
    with exclusive_file_lock(bundle.run_dir / ".gateway-operation.lock"):
        RunJournal(bundle.run_dir).transition(RunState.PUBLISHING)
        entered.set()
        # A terminated owner must not leave a stale ownership file to clean up.
        import time
        time.sleep(60)


def test_process_exit_releases_lock_and_restart_reconciles(tmp_path):
    gateway, bundle = prepare(tmp_path, MemoryTransport())
    context = multiprocessing.get_context("spawn")
    entered = context.Event()
    child = context.Process(target=exit_while_publishing, args=(tmp_path, entered))
    child.start()
    try:
        assert entered.wait(10)
    finally:
        child.terminate(); child.join(10)
        if child.is_alive():
            child.kill(); child.join(5)
    # Model an already accepted remote submission whose local receipt was lost.
    transport = MemoryTransport(states={bundle.request_id: RunState.ACCEPTED})
    recovered = StandaloneTestGateway(gateway.runs_root, transport).submit(bundle)
    assert recovered["state"] == "accepted"
    assert transport.submit_count == 0
    assert (bundle.run_dir / ".gateway-operation.lock").is_file()


def test_lock_timeout_causes_no_publication(tmp_path, monkeypatch):
    transport = MemoryTransport()
    gateway, bundle = prepare(tmp_path, transport)
    actual_lock = runtime_module.exclusive_file_lock

    @contextmanager
    def immediate_lock(path):
        with actual_lock(path, timeout_seconds=0):
            yield

    monkeypatch.setattr(runtime_module, "exclusive_file_lock", immediate_lock)
    with actual_lock(bundle.run_dir / ".gateway-operation.lock"):
        with pytest.raises(TimeoutError):
            gateway.submit(bundle)
    assert transport.submit_count == 0
    assert RunJournal(bundle.run_dir).read()["state"] == "prepared"


def test_cancel_timeout_does_not_record_intent_and_can_be_retried(tmp_path, monkeypatch):
    entered, release = Event(), Event()

    class BlockingTransport(MemoryTransport):
        def submit(self, bundle):
            entered.set()
            assert release.wait(10)
            return super().submit(bundle)

    transport = BlockingTransport()
    gateway, bundle = prepare(tmp_path, transport)
    actual_lock = runtime_module.exclusive_file_lock

    @contextmanager
    def immediate_lock(path):
        with actual_lock(path, timeout_seconds=0):
            yield

    monkeypatch.setattr(runtime_module, "exclusive_file_lock", immediate_lock)
    with ThreadPoolExecutor(max_workers=1) as pool:
        publishing = pool.submit(gateway.submit, bundle)
        try:
            assert entered.wait(5)
            journal = RunJournal(bundle.run_dir)
            before = journal.path.read_bytes()
            with pytest.raises(TimeoutError):
                gateway.cancel(bundle.request_id)
            assert journal.path.read_bytes() == before
            assert journal.read().get("cancellation") is None
        finally:
            release.set()
        assert publishing.result(timeout=5)["state"] == "accepted"
    assert gateway.cancel(bundle.request_id)["state"] == "cancelled"
    assert transport.submit_count == 1
