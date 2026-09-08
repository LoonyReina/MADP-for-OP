from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ascendop_test_gateway.adapters.memory import MemoryTransport
from ascendop_test_gateway.contracts import (
    StandaloneTestRequest,
    TransportReceipt,
    TransportStatus,
    TestState as State,
)
from ascendop_test_gateway.journal import RunJournal
from gateway_fixture import StandaloneTestGateway


class WireTransport(MemoryTransport):
    ready = False
    ack_count = 0
    ack_result = True

    def submit(self, bundle):
        receipt = super().submit(bundle)
        self.run_dir = bundle.run_dir
        self.payload = (
            bundle.run_dir / "results" / receipt.remote_attempt_id / "payload"
        )
        (self.payload / "result_bundle").mkdir(parents=True)
        for name in ("terminal.json", "state.json", "artifact_manifest.json"):
            (self.payload / name).write_text("{}", encoding="utf-8")
        (self.payload / "result_bundle" / "SUMMARY.txt").write_text(
            "FAIL: discriminating case", encoding="utf-8"
        )
        (self.payload.parent / ".payload.sha256").write_text("b" * 64, encoding="ascii")
        return TransportReceipt(
            request_id=receipt.request_id,
            remote_attempt_id=receipt.remote_attempt_id,
            output_subdir=receipt.output_subdir,
            details={
                "schema": "ascendop.standalone-wire-v3-receipt.v1",
                "envelope_digest": "a" * 64,
            },
        )

    def status(self, receipt):
        if not self.ready:
            return super().status(receipt)
        return TransportStatus(
            request_id=receipt.request_id,
            state=State.FAILED,
            classification="wire-v3:terminal:business:ack-pending",
            result={
                "schema": "ascendop.standalone-wire-v3-result.v1",
                "request_id": receipt.request_id,
                "attempt_id": receipt.remote_attempt_id,
                "receipt_id": "receipt-1",
                "terminal_revision": 1,
                "result_payload_sha256": "b" * 64,
                "outcome": "failed",
                "failure_domain": "business",
                "artifact_root": str(self.payload),
            },
            metrics={"return_path_ready": False},
        )

    def cancel(self, receipt):
        return self.status(receipt)  # A completed failure wins a cancellation race.

    def acknowledge(self, receipt, *, event_id):
        data = RunJournal(self.run_dir).require_terminal_acceptance(event_id)
        assert data["state"] == "failed"
        self.ack_count += 1
        return self.ack_result


def _gateway(root):
    root.mkdir(parents=True, exist_ok=True)
    source, cases = root / "source", root / "cases"
    source.mkdir()
    cases.mkdir()
    (source / "x.cpp").write_text("kernel", encoding="utf-8")
    (cases / "test.py").write_text("case", encoding="utf-8")
    transport = WireTransport()
    gateway = StandaloneTestGateway(root / "runs", transport)
    bundle = gateway.prepare(
        StandaloneTestRequest(
            workspace=source,
            task_case=cases,
            op="Demo",
            release="Demo_V1",
            test_version="Demo_V1_1",
            hardware="910B3",
            request_id="request-1",
        )
    )
    gateway.submit(bundle)
    transport.ready = True
    return gateway, transport, bundle


def test_terminal_and_cancel_accept_without_ack_and_wait_does_not_sleep(
    tmp_path, monkeypatch
):
    gateway, transport, bundle = _gateway(tmp_path)
    monkeypatch.setattr(
        "ascendop_test_gateway.runtime.time.sleep",
        lambda _: pytest.fail("terminal wait slept"),
    )
    failed = gateway.cancel(bundle.request_id)
    assert failed["terminal_ack"]["state"] == "pending"
    assert failed["terminal_retention"]["files"]
    assert transport.ack_count == 0
    assert gateway.wait(bundle.request_id, terminal_ack_grace_seconds=120) == failed
    assert gateway.status(bundle.request_id) == failed
    event_id = failed["terminal_ingest_event"]["event_id"]
    done = gateway.acknowledge(bundle.request_id, event_id=event_id)
    assert done["terminal_ack"]["state"] == "delivered"
    assert gateway.acknowledge(bundle.request_id, event_id=event_id) == done
    assert transport.ack_count == 1
    assert done["events"] == failed["events"]
    assert done["state"] == "failed"  # ACK cannot turn an algorithm failure into PASS.


@pytest.mark.parametrize("damage", ["edit", "delete", "add", "marker"])
def test_ack_rejects_missing_or_changed_retained_evidence(tmp_path, damage):
    gateway, transport, bundle = _gateway(tmp_path)
    data = gateway.status(bundle.request_id)
    path = transport.payload / "result_bundle" / "SUMMARY.txt"
    if damage == "edit":
        path.write_text("PASS", encoding="utf-8")
    elif damage == "delete":
        path.unlink()
    elif damage == "add":
        (transport.payload / "extra.txt").write_text("extra", encoding="utf-8")
    else:
        (transport.payload.parent / ".payload.sha256").write_text("c" * 64)
    with pytest.raises(ValueError, match="evidence"):
        gateway.acknowledge(
            bundle.request_id, event_id=data["terminal_ingest_event"]["event_id"]
        )
    assert transport.ack_count == 0


def test_wrong_identity_and_preterminal_ack_do_not_query_or_confirm(tmp_path):
    gateway, transport, bundle = _gateway(tmp_path)
    with pytest.raises(ValueError, match="locally accepted"):
        gateway.acknowledge(bundle.request_id, event_id="c" * 64)
    data = gateway.status(bundle.request_id)
    with pytest.raises(ValueError, match="exact durable"):
        gateway.acknowledge(bundle.request_id, event_id="c" * 64)
    assert transport.ack_count == 0
    assert data["terminal_ack"]["attempts"] == 0


def test_ack_delay_is_observable_and_does_not_hide_result(tmp_path):
    gateway, transport, bundle = _gateway(tmp_path)
    terminal = gateway.status(bundle.request_id)
    event_id = terminal["terminal_ingest_event"]["event_id"]
    transport.ack_result = False
    pending = gateway.acknowledge(bundle.request_id, event_id=event_id)
    assert pending["terminal_ack"]["last_error"]
    assert gateway.wait(bundle.request_id)["state"] == "failed"
    transport.ack_result = True
    restarted = StandaloneTestGateway(gateway.runs_root, transport)
    done = restarted.acknowledge(bundle.request_id, event_id=event_id)
    assert done["terminal_ack"]["attempts"] == 2
    assert done["terminal_ack"]["state"] == "delivered"
    assert transport.submit_count == 1


def test_old_terminal_is_retained_without_requery_or_reissuing_observed_ack(
    tmp_path, monkeypatch
):
    gateway, transport, bundle = _gateway(tmp_path)
    journal = RunJournal(bundle.run_dir)
    remote = transport.status(journal.receipt()).to_dict()
    remote["classification"] = "wire-v3:terminal:business:acknowledged"
    remote["metrics"]["return_path_ready"] = True
    journal.transition(State.FAILED, remote_status=remote)
    monkeypatch.setattr(
        transport, "status", lambda _: pytest.fail("old terminal queried remotely")
    )
    imported = gateway.status(bundle.request_id)
    assert imported["terminal_ack"]["state"] == "delivered"
    assert imported["terminal_ack"]["provenance"] == "prior-transport-observation"
    assert imported["terminal_ack"]["attempts"] == 0
    assert (
        gateway.acknowledge(
            bundle.request_id, event_id=imported["terminal_ingest_event"]["event_id"]
        )
        == imported
    )
    assert transport.ack_count == 0
    with pytest.raises(ValueError, match="exact ACK"):
        journal.transition(
            State.FAILED, remote_status={**remote, "result": {"outcome": "success"}}
        )


@pytest.mark.parametrize("action", ["outside", "missing", "sync-error"])
def test_failed_retention_never_commits_acceptance_or_sends_ack(
    tmp_path, monkeypatch, action
):
    gateway, transport, bundle = _gateway(tmp_path)
    if action == "outside":
        transport.payload = tmp_path / "outside"
    elif action == "missing":
        (transport.payload / "terminal.json").unlink()
    else:

        def no_sync(_):
            raise OSError("injected sync failure")

        monkeypatch.setattr("ascendop_test_gateway.terminal_evidence.os.fsync", no_sync)
    with pytest.raises((ValueError, OSError)):
        gateway.status(bundle.request_id)
    data = RunJournal(bundle.run_dir).read()
    assert data["state"] == "accepted"
    assert "terminal_ingest_event" not in data
    assert "terminal_ack" not in data
    assert transport.ack_count == 0


def _child_cut(root, cut):
    import ascendop_test_gateway.journal as journal_module

    gateway, transport, bundle = _gateway(root)
    original = journal_module._replace_with_retry

    def replace(source, destination):
        value = json.loads(source.read_text())
        if "terminal_retention" in value and cut == "before-commit":
            os._exit(71)
        original(source, destination)
        if "terminal_retention" in value and cut == "after-commit":
            os._exit(72)

    journal_module._replace_with_retry = replace
    data = gateway.status(bundle.request_id)

    def ack(receipt, *, event_id):
        # Model remote success followed by loss of the local reply/ACK update.
        assert RunJournal(bundle.run_dir).require_terminal_acceptance(event_id)
        os._exit(73)

    transport.acknowledge = ack
    gateway.acknowledge(
        bundle.request_id, event_id=data["terminal_ingest_event"]["event_id"]
    )


def test_ack_resyncs_journal_directory_before_attempt_or_transport(tmp_path, monkeypatch):
    gateway, transport, bundle = _gateway(tmp_path)
    data = gateway.status(bundle.request_id)

    def failed_directory_sync(_):
        raise OSError("injected ACK-gate directory sync failure")

    monkeypatch.setattr("ascendop_test_gateway.journal.sync_directory", failed_directory_sync)
    with pytest.raises(OSError, match="ACK-gate"):
        gateway.acknowledge(bundle.request_id, event_id=data["terminal_ingest_event"]["event_id"])
    assert transport.ack_count == 0
    assert RunJournal(bundle.run_dir).read()["terminal_ack"]["attempts"] == 0


@pytest.mark.parametrize(
    "cut,exit_code",
    [("before-commit", 71), ("after-commit", 72), ("after-remote-ack", 73)],
)
def test_process_death_preserves_acceptance_and_same_ack_identity(
    tmp_path, cut, exit_code
):
    code = "import runpy,sys; from pathlib import Path; sys.path.insert(0,str(Path(sys.argv[1]).parent)); m=runpy.run_path(sys.argv[1]); m['_child_cut'](Path(sys.argv[2]),sys.argv[3])"
    child = subprocess.run(
        [sys.executable, "-B", "-c", code, __file__, str(tmp_path), cut],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == exit_code, child.stderr
    journal = RunJournal(tmp_path / "runs" / "request-1")
    value = journal.read()
    if cut == "before-commit":
        assert value["state"] == "accepted"
        assert "terminal_ingest_event" not in value
        assert "terminal_retention" not in value
        assert "terminal_ack" not in value
    else:
        assert value["state"] == "failed"
        assert value["terminal_ack"]["state"] == "pending"
        assert journal.require_terminal_acceptance(
            value["terminal_ingest_event"]["event_id"]
        )
    transport = WireTransport()
    transport.ready = True
    transport.run_dir = journal.run_dir
    receipt = journal.receipt()
    transport.payload = (
        journal.run_dir / "results" / receipt.remote_attempt_id / "payload"
    )
    gateway = StandaloneTestGateway(tmp_path / "runs", transport)
    recovered = gateway.status("request-1")
    done = gateway.acknowledge(
        "request-1", event_id=recovered["terminal_ingest_event"]["event_id"]
    )
    assert done["terminal_ack"]["state"] == "delivered"
    assert done["terminal_ack"]["attempts"] == (2 if cut == "after-remote-ack" else 1)
    assert transport.submit_count == 0
