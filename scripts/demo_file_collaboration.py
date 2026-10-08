"""Offline file handoff: two fixture writers, one real Gateway journal.

No live LLM, GP server, accelerator, daemon or external evaluator is used.
Toy candidates are JSON coefficients; the trusted demo executor tests y=2*x.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

# The public demo is intentionally executable as ``python -I scripts/...``.
# In isolated mode Python does not honor PYTHONPATH, so make the checked-out
# source packages discoverable without requiring an editable install. Installed
# users still resolve the normal package imports unchanged.
if __package__ in (None, ""):
    _repo_root = Path(__file__).resolve().parents[1]
    for _package in ("ascendop_protocol", "ascendop_control", "ascendop_test_gateway"):
        sys.path.insert(0, str(_repo_root / "packages" / _package / "src"))

from ascendop_test_gateway.contracts import PreparedBundle, StandaloneTestRequest
from ascendop_test_gateway.contracts import TransportReceipt, TransportStatus, TestState
from ascendop_test_gateway.journal import RunJournal
from ascendop_test_gateway.runtime import GatewayRuntime
from ascendop_test_gateway.terminal_evidence import read_retained_file


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


class DemoStore:
    """A trusted, fixed-domain example port, not a general untrusted bundler."""

    def prepare(self, request, runs_root):
        directory = runs_root / request.request_id
        directory.mkdir(parents=True, exist_ok=False)
        candidate = json.loads((request.workspace / 'candidate.json').read_text())
        cases = json.loads((request.task_case / 'inputs.json').read_text())
        frozen = dict(request_id=request.request_id, candidate=candidate, inputs=cases)
        write_json(directory / 'BUNDLE.json', frozen)
        return PreparedBundle(request.request_id, directory, frozen)

    def load(self, directory):
        frozen = json.loads((directory / 'BUNDLE.json').read_text())
        return PreparedBundle(frozen['request_id'], directory, frozen)


class DemoExecutor:
    """Synthetic wire adapter with actual CPU arithmetic and retained files."""

    def __init__(self, runs):
        self.runs = runs
        self.submissions = 0
        self.queries = 0

    def preflight(self, bundle):
        if set(bundle.request['candidate']) != {'scale'}:
            raise ValueError('toy candidate must contain only scale')

    def submit(self, bundle):
        self.submissions += 1
        payload = bundle.run_dir / 'results/demo-attempt/payload'
        values = bundle.request['inputs']
        scale = bundle.request['candidate']['scale']
        rows = [dict(input=x, expected=2*x, actual=scale*x, passed=scale*x == 2*x) for x in values]
        report = dict(scope='synthetic-CPU-only', rows=rows, passed=all(r['passed'] for r in rows))
        write_json(payload / 'result_bundle/RESULT.json', report)
        for name in ('terminal.json', 'state.json', 'artifact_manifest.json'):
            write_json(payload / name, dict(scope='synthetic-demo', request_id=bundle.request_id))
        digest = hashlib.sha256(json.dumps(report, sort_keys=True).encode()).hexdigest()
        (payload.parent / '.payload.sha256').write_text(digest, encoding='ascii')
        receipt = TransportReceipt(request_id=bundle.request_id, remote_attempt_id='demo-attempt',
            output_subdir='synthetic', details=dict(schema='ascendop.standalone-wire-v3-receipt.v1',
                envelope_digest=hashlib.sha256(json.dumps(bundle.request, sort_keys=True).encode()).hexdigest()))
        write_json(bundle.run_dir / 'DEMO_RECEIPT.json', receipt.to_dict())
        return receipt

    def reconcile(self, bundle):
        path = bundle.run_dir / 'DEMO_RECEIPT.json'
        return TransportReceipt.from_dict(json.loads(path.read_text())) if path.exists() else None

    def status(self, receipt):
        self.queries += 1
        payload = self.runs / receipt.request_id / 'results/demo-attempt/payload'
        report = json.loads((payload / 'result_bundle/RESULT.json').read_text())
        return TransportStatus(request_id=receipt.request_id,
            state=TestState.COMPLETED if report['passed'] else TestState.FAILED,
            classification='wire-v3:synthetic-CPU', result=dict(
                schema='ascendop.standalone-wire-v3-result.v1', request_id=receipt.request_id,
                attempt_id=receipt.remote_attempt_id, receipt_id='demo-receipt-' + receipt.request_id,
                terminal_revision=1, result_payload_sha256=(payload.parent / '.payload.sha256').read_text(),
                outcome='success' if report['passed'] else 'failed',
                failure_domain='' if report['passed'] else 'business', artifact_root=str(payload)))

    def acknowledge(self, receipt, *, event_id):
        RunJournal(self.runs / receipt.request_id).require_terminal_acceptance(event_id)
        return True

    def cancel(self, receipt):
        return self.status(receipt)


def fixture_writer(workspace, revision):
    """Simulates two independent harness processes; no SDK or model invocation."""
    if revision == 2:
        handoff = json.loads((workspace / 'HANDOVER.json').read_text())
        result = json.loads((workspace / handoff['result_ref']).read_text())
        assert not result['passed'] and handoff['next'] == 'repair'
    write_json(workspace / f'candidates/v{revision}/candidate.json', {'scale': revision})
    write_json(workspace / 'EXPERIMENT.json', dict(candidate=f'candidates/v{revision}',
        participant=f'fixture-harness-{revision}', submit_decision='hold',
        reason='local diagnosis only; external submission is not requested'))


def run_demo(root):
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    workspace = root / 'workspaces/DemoScale'
    write_json(workspace / 'cases/inputs.json', [-3, 0, 7])
    rows = []
    for revision in (1, 2):
        # The coordinator waits for writer exit; it does not share chat context.
        subprocess.run([sys.executable, '-I', str(Path(__file__).resolve()),
            '--fixture-writer', str(workspace), '--revision', str(revision)], check=True, timeout=30)
        intent = json.loads((workspace / 'EXPERIMENT.json').read_text())
        assert intent['submit_decision'] == 'hold'
        transport = DemoExecutor(workspace / 'runs')
        gateway = GatewayRuntime(workspace / 'runs', transport, bundle_store=DemoStore())
        request = StandaloneTestRequest(workspace=workspace / intent['candidate'], task_case=workspace / 'cases',
            op='DemoScale', release='demo', test_version='demo-1', hardware='cpu',
            request_id=f'demo-round-{revision}', mode='correctness', operation_code='test.correctness')
        state = gateway.submit(gateway.prepare(request))
        assert state['terminal_ack']['state'] == 'pending'
        # A new host object recovers the accepted result from the existing journal.
        restarted_transport = DemoExecutor(workspace / 'runs')
        restarted = GatewayRuntime(workspace / 'runs', restarted_transport, bundle_store=DemoStore())
        recovered = restarted.status(request.request_id)
        assert restarted_transport.submissions == restarted_transport.queries == 0
        result_path = workspace / 'runs' / request.request_id / 'results/demo-attempt/payload/result_bundle/RESULT.json'
        result = json.loads(read_retained_file(workspace / 'runs' / request.request_id,
            recovered['terminal_retention'], result_path))
        write_json(workspace / 'HANDOVER.json', dict(result_ref=result_path.relative_to(workspace).as_posix(),
            request_id=request.request_id, next='done' if result['passed'] else 'repair'))
        (workspace / 'HANDOVER.md').write_text(
            f"# DemoScale handoff\nRead HANDOVER.json for the original result.\n"
            f"Participant: {intent['participant']}\nNext: {'done' if result['passed'] else 'repair'}\n"
            "External decision: hold. Local success is not external success.\n", encoding='utf-8')
        ack = restarted.acknowledge(request.request_id, event_id=recovered['terminal_ingest_event']['event_id'])
        assert ack['terminal_ack']['state'] == 'delivered'
        rows.append(dict(participant=intent['participant'], state=recovered['state'],
            pass_count=sum(r['passed'] for r in result['rows']), total=len(result['rows']),
            ack='delivered', recovered_without_resubmit=True))
    report = dict(schema='madp.file-collaboration-demo.v1', scope='synthetic-CPU-only',
        model_calls=0, hardware_calls=0, external_submissions=0, rounds=rows)
    write_json(root / 'SUMMARY.json', report)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path)
    parser.add_argument('--fixture-writer', type=Path)
    parser.add_argument('--revision', type=int, choices=(1, 2))
    args = parser.parse_args()
    if args.fixture_writer is not None and args.revision is not None:
        fixture_writer(args.fixture_writer, args.revision)
    elif args.root is not None:
        print(json.dumps(run_demo(args.root), indent=2))
    else:
        parser.error('provide --root with a new output directory')
