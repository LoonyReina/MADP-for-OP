"""Synthetic file-Solver -> completion -> local test -> next revision example.

Uses the exported control repositories and shared completion/snapshot code.
The Solver and executor are deterministic Python fixtures, not live models or
NPU evaluation. Native carrier security and external evaluation belong to B.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import runpy
from datetime import datetime, timezone

from ascendop_control.delivery import dispatch_topic
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.automation.managed_completion_commit import record_workspace_completion
from ascendop_daemon.automation.workspace_file_client import proposal_path, submit_workspace_proposal
from ascendop_daemon.automation.workspace_projection_port import project_workspace
from ascendop_daemon.workflow.workspace_messages import workspace_entry_prompt
from ascendop_protocol.actor import validate_actor_action_receipt


def _now():
    return datetime.now(timezone.utc).isoformat()


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _plan(operator, revision):
    action = f"{operator}-round-{revision}"
    owner = {"schema": "ascendop.workspace-owner.v1", "workspace": f"workspaces/{operator}",
        "campaign_id": "synthetic-demo", "operator_id": operator, "action_id": action,
        "attempt_id": f"attempt-{action}", "lease_id": f"lease-{action}", "principal_id": "solver",
        "native_session_id": f"session-{operator}", "revision": revision}
    record = {"context": {"operator_id": operator, "request": {"logical_request_id": f"request-{action}"},
        "test": {"mode": "correctness", "case_version": "cases-1"}, "case_path": "cases.json",
        "input_source_sha256": "a" * 64}}
    return owner, {"record": record}


def _publish(directory, snapshot):
    view = snapshot["view"]
    _write(directory / ".ascendop/ITERATION.json", view)
    brief = f"# Synthetic iteration\nProjection revision: {view['revision']}\n"
    brief += f"Action: {view['owner']['action_id']}\nLocal result: {view['server_feedback']['state']}\n"
    brief += f"Next: {view['next']['action']}\nRead CLIENT.json for the current file request.\n"
    (directory / ".ascendop/BRIEF.md").write_text(brief, encoding="utf-8")


def _terminal(request, attempt, source_digest, successful):
    identity = {"request_id": request, "attempt_id": attempt, "receipt_id": f"receipt-{request}",
        "terminal_revision": 1, "result_payload_sha256": source_digest, "envelope_digest": source_digest}
    identifier = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
    return {"schema": "ascendop.gp-terminal-ingest-event.v1", "event_id": identifier, **identity,
        "outcome": "success" if successful else "failed", "failure_domain": "" if successful else "correctness",
        "artifact_root": "result/payload", "ack": {k: identity[k] for k in ("request_id", "attempt_id", "receipt_id")}}


def run_demo(root: Path, *, operators: int = 3):
    root = root.resolve()
    if root.exists() or not 1 <= operators <= 6:
        raise ValueError("choose a new demo output directory and 1..6 synthetic operators")
    root.mkdir(parents=True)
    database = ControlDatabase(root / "control.sqlite3")
    database.initialize()
    results = []
    for index in range(operators):
        operator = f"demo-{index + 1}"
        owner, plan = _plan(operator, 1)
        database.admit_workspace_action(binding=owner, expected_action_id="", expected_revision=0, plan=plan)
        for revision in (1, 2):
            owner = database.workspace_owner(owner["workspace"])
            snapshot = project_workspace(root, database, owner["workspace"], publish=_publish,
                                         expected_action_id=owner["action_id"])
            workspace = root / owner["workspace"]
            request = f"request-{owner['action_id']}"
            turn = f"turn-{revision}"
            context = {"schema": "ascendop.workspace-client.v1", "available": True, "binding": owner,
                "native_start_id": f"fixture-start-{revision}", "native_turn_id": turn,
                "execution_phase": "candidate-test", "proposal_path": proposal_path(owner, turn)}
            _write(workspace / ".ascendop/CLIENT.json", context)
            # Deterministic in-process Solver fixture. It returns before host intake;
            # this is NOT evidence for resident native-writer qualification.
            source = workspace / "candidate.py"
            source.write_text("def solve(x):\n    return " + ("x + 1" if revision == 1 else "2 * x") + "\n", encoding="utf-8")
            requested = submit_workspace_proposal(workspace, summary="Try candidate" if revision == 1 else "Fix the observed mismatch")
            assert requested["state"] == "requested"
            proposal = json.loads((workspace / context["proposal_path"]).read_text())
            assert proposal["binding"] == database.workspace_owner(owner["workspace"])
            source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
            receipt = validate_actor_action_receipt({"schema": "ascendop.actor-action-receipt.v1",
                "action_id": owner["action_id"], "action_kind": "solver.iterate", "effective_role": "solver",
                "role_binding_id": f"role-{operator}", "lease_id": owner["lease_id"], "status": "completed",
                "result": {"native_turn_id": turn}, "failure_class": None, "completed_at": _now()})
            continuation = {"workspace": owner["workspace"], "source_action_id": owner["action_id"],
                "source_sha256": source_digest, "accepted_request": {"request_id": request},
                "receipt_ref": context["proposal_path"]}
            payload = {"carrier": "synthetic-host", "binding": owner, "receipt": receipt, "continuation": continuation}
            record_workspace_completion(database, action_id=owner["action_id"], owner=owner, receipt=receipt, payload=payload)
            replay = record_workspace_completion(database, action_id=owner["action_id"], owner=owner, receipt=receipt, payload=payload)
            assert replay["payload"] == payload
            solve = runpy.run_path(str(source))["solve"]
            successful = all(solve(value) == 2 * value for value in (2, 4, 7))
            event = _terminal(request, owner["attempt_id"], source_digest, successful)
            next_owner, next_plan = _plan(operator, revision + 1)
            intent = {**continuation, "kind": "official.wait" if successful else "solver.next",
                "proof_ref": "synthetic-result.json", "workspace_owner": owner,
                "business_summary": {"full_correctness_pass": successful, "summary": "3/3" if successful else "0/3"},
                "next_binding": next_owner, "action_plan": next_plan}
            database.record_gp_terminal_ingest_event(event, source_action_id=owner["action_id"],
                continuation=intent, ack_request={"purpose": "deliberately pending in this demo"})
            feedback = project_workspace(root, database, owner["workspace"], publish=_publish)
            assert feedback["server_feedback"]["state"] == ("passed" if successful else "failed")
            def handoff(row):
                accepted = row["payload"]["continuation"]
                if accepted["kind"] == "solver.next":
                    database.admit_workspace_action(binding=accepted["next_binding"],
                        expected_action_id=accepted["source_action_id"], expected_revision=revision,
                        plan=accepted["action_plan"])
                    database.bind_gp_terminal_ingest_successor(row["origin_id"], accepted["next_binding"]["action_id"])
                return {"disposition": "delivered"}
            # Restart the facade; durable original intents, not memory, drive delivery.
            database = ControlDatabase(database.path)
            delivered = dispatch_topic(database, topic="test.terminal", owner="fixture-host", handler=handoff)
            assert len(delivered["delivered"]) == 1 and not delivered["errors"]
            assert not dispatch_topic(database, topic="test.terminal", owner="fixture-host", handler=handoff)["delivered"]
            notification = workspace_entry_prompt(operator=operator, workspace=owner["workspace"],
                phase="candidate iteration", action_id=owner["action_id"])
            assert len(notification) <= 800
        results.append({"operator": operator, "solver_rounds": 2, "local_pass": successful,
                        "official": "not_run", "owner_revision": owner["revision"]})
    pending_ack = sum(row["state"] == "pending" for row in database.control_outbox(topic="test.ack"))
    assert pending_ack == 2 * operators
    report = {"state": "passed", "evidence": "synthetic-host-and-executor", "operators": results,
              "pending_ack": pending_ack, "model_calls": 0, "hardware_calls": 0, "external_submissions": 0}
    _write(root / "REPORT.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--operators", default=3, type=int)
    args = parser.parse_args()
    print(json.dumps(run_demo(args.root, operators=args.operators), indent=2))
