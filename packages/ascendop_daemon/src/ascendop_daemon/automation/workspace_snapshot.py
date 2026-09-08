"""Read a derived workspace snapshot from the existing control event journal.

No filesystem state is read back as truth, and ACK retries do not advance its
business revision. Publication belongs to a trusted integration port.
"""
from __future__ import annotations

import json
from typing import Any, Mapping

from ascendop_control.storage.workspace_repository import workspace_key
from ascendop_protocol.agent import validate_workspace_iteration

PLAN_SCHEMA = "ascendop.historical-action-adoption.v1"

SOURCE_TOPICS = ("workspace.action", "agent.recovery", "agent.native-start", "agent.native-terminal", "agent.completion", "agent.outcome", "test.request", "test.publication", "test.terminal", "official.terminal")
_SOURCE_QUERY = (
    "SELECT o.outbox_id,o.topic,o.payload_json,o.result_json,e.sequence,e.event_at FROM control_outbox_v5 o "
    "JOIN control_events e ON e.entity_type='control-outbox' AND e.entity_id=o.outbox_id "
    "AND e.event_type='control-outbox-enqueued' WHERE o.topic IN ('workspace.action','agent.recovery','agent.native-start','agent.native-terminal','agent.completion','agent.outcome','test.request','test.publication','test.terminal','official.terminal') "
    "AND (o.topic!='agent.native-start' OR o.state='delivered') "
    "AND lower(COALESCE(json_extract(o.payload_json,'$.binding.workspace'),"
    "json_extract(o.payload_json,'$.continuation.workspace')))=? "
)


def _decode(row) -> dict[str, Any]:
    return {**dict(row), "payload": json.loads(row["payload_json"]), "result": json.loads(row["result_json"]) if row["result_json"] else None}


def _feedback(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {"state": "not_run", "summary": "No accepted local terminal for this action", "action_id": None,
                "request_id": None, "event_id": None, "full_correctness_pass": False}
    payload = row["payload"]
    if row["topic"] == "agent.native-start":
        owner, started = payload["binding"], row["result"]
        return {"action_id": owner["action_id"], "request_id": None, "event_id": None,
            "control_outbox_id": row["outbox_id"], "state": "native-start-admitted",
            "summary": f"Native attempt {payload['ordinal']} admitted; no completion or test PASS inferred",
            "result_ref": f"{owner['workspace']}/.ascendop/native/{started['turn_id']}/CONTROL_START.json"}
    if row["topic"] == "agent.recovery":
        owner = payload["binding"]
        return {"action_id": owner["action_id"], "request_id": payload["record"]["context"]["request"]["logical_request_id"],
            "event_id": None, "control_outbox_id": row["outbox_id"], "state": "recovery-admitted",
            "summary": f"Continue original native budget at {payload['next_ordinal']}/{payload['max_ordinal']}; no completion or PASS inferred",
            "result_ref": f"{owner['workspace']}/.ascendop/native/{owner['action_id']}/CONTROL_RECOVERY.json"}
    if row["topic"] == "agent.native-terminal":
        owner, delivery = payload["binding"], payload["delivery"]
        return {"action_id": owner["action_id"], "request_id": None, "event_id": None,
            "control_outbox_id": row["outbox_id"], "state": payload["status"],
            "summary": f"Native turn {delivery['native_turn_id']} exited ({payload['status']}); this is not a Solver outcome or test PASS",
            "result_ref": f"{owner['workspace']}/.ascendop/native/{delivery['native_turn_id']}/CONTROL_EXIT.json"}
    if row["topic"] in {"test.request", "test.publication"}:
        intent = payload["continuation"]
        state = "queued" if row["topic"] == "test.request" else payload["disposition"]
        return {"action_id": intent["source_action_id"], "request_id": intent["request_id"],
            "control_outbox_id": row["outbox_id"], "event_id": None, "state": state,
            "full_correctness_pass": False, "summary": f"Managed request {state}; keep the original request ID",
            "result_ref": intent["request_ref"]}
    if row["topic"] == "agent.outcome":
        intent, outcome = payload["continuation"], payload["outcome"]
        return {"action_id": outcome["action_id"], "request_id": (intent["accepted_request"] or {}).get("request_id"),
            "control_outbox_id": row["outbox_id"], "event_id": None, "state": outcome["execution_status"],
            "summary": outcome["summary"], "result_ref": intent["publication_ref"]}
    if row["topic"] == "agent.completion":
        receipt, intent = payload["receipt"], payload["continuation"]
        outcome = receipt["result"].get("outcome") or {}
        return {"action_id": receipt["action_id"], "request_id": (intent["accepted_request"] or {}).get("request_id"),
            "control_outbox_id": row["outbox_id"], "event_id": None, "state": receipt["status"],
            "summary": outcome.get("summary") or f"Solver delivery {receipt['status']}",
            "result_ref": intent["receipt_ref"]}
    intent, event = payload["continuation"], payload["event"]
    base = {"action_id": intent["source_action_id"], "request_id": event["request_id"],
            "control_outbox_id": row["outbox_id"], "event_id": event["event_id"],
            "result_ref": intent["proof_ref"]}
    if row["topic"] == "test.terminal":
        summary = intent.get("business_summary") or {}
        return {**base, "state": "passed" if summary.get("full_correctness_pass") is True else event["outcome"],
                "result_ref": f"{intent['workspace']}/.ascendop/results/{event['request_id']}/CONTROL_RESULT.json",
                "full_correctness_pass": summary.get("full_correctness_pass") is True,
                "summary": summary.get("summary") or f"Local terminal: {event['outcome']}",
                "correctness": summary.get("correctness"), "failure_domain": event["failure_domain"]}
    official = payload["official_event"]
    receipt = official["payload"]
    if receipt.get("purpose") == "official_submission_decision":
        return {**base, "event_id": official["event_id"], "local_event_id": event["event_id"],
            "state": "not_submitted", "summary": receipt["decision"]["summary"],
            "checkpoint_id": receipt["checkpoint_id"],
            "result_ref": f"{intent['workspace']}/.ascendop/official/{receipt['checkpoint_id']}/CONTROL_DECISION.json"}
    verdict = str(receipt["feedback"].get("result") or receipt["feedback"].get("verdict") or "Unknown")
    return {**base, "event_id": official["event_id"], "local_event_id": event["event_id"],
            "state": "passed" if verdict.casefold() in {"pass", "passed", "accepted"} else "failed",
            "summary": verdict, "receipt_id": receipt["official_receipt_id"],
            "checkpoint_id": receipt["checkpoint_id"], "official_attempt_id": receipt["official_attempt_id"],
            "result_ref": f"{intent['workspace']}/.ascendop/official/{receipt['official_attempt_id']}/CONTROL_FEEDBACK.json"}


def read_workspace_snapshot(database: Any, workspace: str, *, required_revision: int = 0,
                            expected_action_id: str | None = None) -> dict[str, Any]:
    key = workspace_key(workspace)
    with database.connection() as connection:
        connection.execute("BEGIN")  # One read snapshot, not a write transaction held over filesystem I/O.
        head = connection.execute("SELECT binding_json FROM workspace_owners_v5 WHERE workspace_key=?", (key,)).fetchone()
        if head is None:
            raise ValueError("workspace projection requires explicit owner adoption")
        owner = json.loads(head["binding_json"])
        if expected_action_id is not None and owner["action_id"] != expected_action_id:
            raise ValueError("workspace projection delivery owner was fenced")
        rows = list(connection.execute(_SOURCE_QUERY + "ORDER BY e.sequence DESC LIMIT 5", (key,)).fetchall())
        # Read current facts explicitly as well: many late historical
        # receipts cannot push the current owner/local event out of the view.
        for topic in SOURCE_TOPICS:
            condition = "o.origin_id=?" if topic in {"workspace.action", "agent.native-start"} else "json_extract(o.payload_json,'$.continuation.source_action_id')=?"
            rows.extend(connection.execute(_SOURCE_QUERY + f"AND o.topic=? AND {condition} ORDER BY e.sequence DESC LIMIT 1",
                (key, topic, owner["action_id"])).fetchall())
        # Successor lifecycle traffic must not hide the last real test.
        # Historical terminals are evidence only, never current PASS/state.
        for topic in ("test.terminal", "official.terminal"):
            rows.extend(connection.execute(_SOURCE_QUERY + "AND o.topic=? ORDER BY e.sequence DESC LIMIT 1",
                (key, topic)).fetchall())
    facts = sorted({_decode(row)["outbox_id"]: _decode(row) for row in rows}.values(),
                   key=lambda row: row["sequence"], reverse=True)
    if not facts or facts[0]["sequence"] < required_revision:
        raise ValueError("workspace projection source revision is not accepted")
    publications = [row for row in facts if row["topic"] == "workspace.action"
                    and row["payload"]["binding"] == owner]
    if len(publications) != 1:
        raise ValueError("workspace owner has no exact accepted action publication")
    record = publications[0]["payload"]["plan"]["record"]
    context = record["context"]
    current = [row for row in facts if row["topic"] != "workspace.action"
               and (row["payload"]["binding"]["action_id"] if row["topic"] == "agent.native-start"
                    else row["payload"]["continuation"]["source_action_id"]) == owner["action_id"]]
    local = next((row for row in current if row["topic"] == "test.terminal"), None)
    official = next((row for row in current if row["topic"] == "official.terminal"), None)
    completion = next((row for row in current if row["topic"] == "agent.completion"), None)
    late = next((row for row in current if row["topic"] == "agent.outcome"), None)
    submitted = next((row for row in current if row["topic"] == "test.request"), None)
    publication = next((row for row in current if row["topic"] == "test.publication"), None)
    native = next((row for row in current if row["topic"] == "agent.native-terminal"), None)
    native_start = next((row for row in current if row["topic"] == "agent.native-start"), None)
    recovery_admission = next((row for row in current if row["topic"] == "agent.recovery"), None)
    if recovery_admission is not None:
        record = recovery_admission["payload"]["record"]
        context = record["context"]
    effective = late or completion
    solver_status = late["payload"]["outcome"]["execution_status"] if late else completion["payload"]["receipt"]["status"] if completion else None
    accepted_output = solver_status == "completed" and bool(late or completion["payload"]["receipt"]["result"].get("outcome"))
    server = _feedback(local)
    official_view = _feedback(official) if official else {
        "state": "not_received", "summary": "No accepted official terminal for this action", "action_id": None,
        "request_id": None, "event_id": None, "local_event_id": None}
    next_action = {"owner": "solver", "action": "work", "reason": "Read this action's inputs and use its managed outcome/test entry"}
    if publications[0]["payload"]["plan"].get("schema") == PLAN_SCHEMA and not effective:
        next_action = {"owner": "harness", "action": "await_continuation",
            "reason": "Historical owner adopted; original completion/request/official receipt reconciliation is pending. Do not restart this native action."}
    if recovery_admission is not None and not effective:
        next_action = {"owner": "solver", "action": "work",
            "reason": "Explicit recovery admitted for the original unfinished candidate; continue the recorded remaining native budget."}
    if native_start and not effective and (native is None or native_start["sequence"] > native["sequence"]):
        next_action = {"owner": "solver", "action": "work",
            "reason": "The current native attempt is admitted; continue this original action using its workspace instructions."}
    elif native and not effective:
        next_action = {"owner": "harness", "action": "await_continuation",
            "reason": "Native exit observation is durable; outcome validation/completion is pending. Do not infer writer release from an outcome file."}
    if effective and not local:
        accepted_request = effective["payload"]["continuation"]["accepted_request"]
        if accepted_request:
            server = {**server, "state": "accepted", "action_id": owner["action_id"],
                "request_id": accepted_request["request_id"],
                "summary": "Request accepted; awaiting trusted terminal intake (do not resubmit)"}
            next_action = {"owner": "harness", "action": "await_local",
                "reason": "The Solver turn is complete; the harness tracks the original accepted request"}
        else:
            next_action = {"owner": "harness", "action": "await_continuation",
                "reason": "Solver completion accepted; the harness owns case/outcome continuation"}
        if not late and completion["payload"]["continuation"].get("native_recovery"):
            recovery = completion["payload"]["continuation"]["native_recovery"]
            observation = recovery["request_observation"]
            next_action = {"owner": "harness", "action": "await_local" if accepted_request else "adapter_recovery_required",
                "reason": "Final native adapter failure; automatic delivery retries have ended. "
                    + ("Track the original request; do not start another business action."
                       if accepted_request or observation else "No Solver outcome accepted; preserve draft work for adapter recovery.")}
            if observation and observation.get("evidence_error"):
                next_action["reason"] += " Request evidence needs reconciliation: " + observation["evidence_error"]
    if submitted and not local:
        server = _feedback(publication or submitted)
        rejected = publication and publication["payload"]["disposition"] != "accepted"
        next_action = {"owner": "harness", "action": "qualification_gap" if rejected else "await_local",
            "reason": "Original request was not accepted; inspect publication failure" if rejected else
                "Frozen input/request intent is durable; the harness tracks this original request without requiring a Solver outcome"}
    outcome = late["payload"]["outcome"] if late else completion["payload"]["receipt"]["result"].get("outcome") if completion else None
    if outcome and outcome.get("disposition") == "protocol_gap" and not submitted and not local:
        blocker = outcome["blocker"]
        next_action = {"owner": "harness", "action": "qualification_gap",
            "reason": f"{blocker['code']}: {blocker['details']} Resume when: {blocker['resume_condition']}"}
    if local:
        kind = local["payload"]["continuation"]["kind"]
        next_action = {"owner": "harness", "action": "await_official" if kind == "official.wait" else "await_continuation",
                       "reason": "Local result accepted; the control continuation owns the next action"}
        if kind == "framework.wait":
            next_action = {"owner": "harness", "action": "qualification_gap",
                "reason": "Infrastructure failure retained; repair the framework before an authorized test recovery. Solver must not resubmit unchanged code."}
    if official:
        kind = official["payload"]["continuation"]["kind"]
        if kind == "official.complete" and server["full_correctness_pass"]:
            next_action = {"owner": "none", "action": "correctness_complete",
                           "reason": "This exact candidate has qualified full local correctness and official Pass"}
        elif kind == "official.complete":
            next_action = {"owner": "harness", "action": "qualification_gap",
                           "reason": "Official Pass exists but qualified full local evidence is missing"}
        else:
            next_action = {"owner": "harness", "action": "await_continuation",
                           "reason": "Official feedback accepted; the control continuation owns the next action"}
    request = context["request"]["logical_request_id"]
    view = validate_workspace_iteration({
        "schema": "ascendop.workspace-iteration.v2", "operator": owner["operator_id"],
        "campaign_id": owner["campaign_id"], "workspace": workspace, "owner": owner,
        "revision": facts[0]["sequence"], "updated_at": facts[0]["event_at"],
        "mode": (context.get("test") or {}).get("mode") or "case-authoring",
        "candidate": {"candidate_id": request, "action_id": owner["action_id"],
            "state": "accepted-local-terminal" if local else "test-submitted" if submitted else "solver-" + solver_status if effective else "awaiting-proposal",
            "source_sha256": (local or submitted or effective)["payload"]["continuation"]["source_sha256"] if local or submitted or accepted_output else context["input_source_sha256"],
            "case_version": (context.get("test") or {}).get("case_version"),
            "case_ref": context["case_path"], "source_kind": "frozen-test-input" if submitted else "accepted-output" if local or accepted_output else "action-input-baseline"},
        "server_feedback": server, "official_feedback": official_view, "next": next_action,
        "recent_results": [_feedback(row) for row in sorted(
            (row for row in facts if row["topic"] != "workspace.action"),
            key=lambda row: row["topic"] not in {"test.terminal", "official.terminal"})][:4],
    })
    return {"view": view, "facts": facts, "record": record, "native_start": native_start,
            "effective": effective, "submitted": submitted, "native": native,
            "recovery_admission": recovery_admission}
