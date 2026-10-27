from pathlib import Path

import pytest

from ascendop_test_gateway.adapters.memory import MemoryTransport
from ascendop_test_gateway.contracts import (
    StandaloneTestRequest,
    TestState as RunState,
    TransportReceipt,
)
from ascendop_test_gateway.journal import RunJournal
from gateway_fixture import StandaloneTestGateway


def test_attach_receipt_rejects_wrong_identity_without_mutating_journal(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize("request-1")
    before = journal.path.read_bytes()
    with pytest.raises(ValueError, match="request identity mismatch"):
        journal.attach_receipt(TransportReceipt("other", "attempt", "output"))
    assert journal.path.read_bytes() == before

    correct = TransportReceipt("request-1", "attempt", "output")
    journal.attach_receipt(correct)
    journal.attach_receipt(correct)
    assert journal.receipt() == correct
    with pytest.raises(ValueError, match="immutable"):
        journal.attach_receipt(TransportReceipt("request-1", "changed", "output"))
    assert journal.receipt() == correct


@pytest.mark.parametrize("entrypoint", ["submit", "status", "cancel"])
def test_reconcile_rejects_wrong_identity_before_persistence_or_cancel(
    tmp_path: Path, entrypoint: str,
):
    class Reconciler(MemoryTransport):
        wrong = True

        def reconcile(self, bundle):
            if self.wrong:
                return TransportReceipt("other", "other-attempt", "other-output")
            return super().reconcile(bundle)

    transport = Reconciler()
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    bundle = gateway.prepare(StandaloneTestRequest(
        workspace=tmp_path, task_case=tmp_path, op="Demo", release="v1",
        test_version="v1", hardware="CPU", request_id="request-1",
    ))
    journal = RunJournal(bundle.run_dir)
    journal.transition(RunState.PUBLISHING)
    journal.transition(RunState.UNCERTAIN)
    # Both remote requests exist, so an erroneous cancel would be observable.
    transport.states.update({"request-1": RunState.ACCEPTED, "other": RunState.RUNNING})
    argument = bundle if entrypoint == "submit" else bundle.request_id
    with pytest.raises(ValueError, match="request identity mismatch"):
        getattr(gateway, entrypoint)(argument)
    assert journal.receipt() is None
    assert journal.read()["state"] == "uncertain"
    assert transport.states["other"] == RunState.RUNNING

    transport.wrong = False
    recovered = gateway.status(bundle.request_id)
    assert journal.receipt().request_id == bundle.request_id
    assert recovered["state"] == ("cancelled" if entrypoint == "cancel" else "accepted")
    assert transport.states["other"] == RunState.RUNNING
    assert transport.submit_count == 0
