"""Trusted carrier commit ports: original outbox, owner fence, no extra ledger.
Admission policy and carrier observation MUST precede these host-only calls.
"""
from __future__ import annotations
import json
from ascendop_control.storage.outbox_repository import enqueue_control_intent, _json, _now, _require_claim
from ascendop_control.storage.workspace_repository import workspace_key, enqueue_workspace_projection

def accept_native_start(database, claim, result):
    # Owner CAS and claim fence share the release transaction. No content hash.
    now = _now()
    binding = claim["payload"]["binding"]
    with database.transaction() as conn:
        _require_claim(conn, claim["outbox_id"], claim["claim_token"], now)
        owner = conn.execute("SELECT binding_json FROM workspace_owners_v5 WHERE workspace_key=?",
            (workspace_key(binding["workspace"]),)).fetchone()
        if owner is None or json.loads(owner["binding_json"]) != binding:
            raise ValueError("native release fenced by a newer workspace owner")
        conn.execute("UPDATE control_outbox_v5 SET state='delivered',result_json=?,claim_token='',"
            "lease_expires_at='',last_error='',updated_at=? WHERE outbox_id=?",
            (_json(result), now, claim["outbox_id"]))
        conn.execute("INSERT INTO control_events(event_at,event_type,entity_type,entity_id,payload_json) "
            "VALUES(?,'native-start-admitted','control-outbox',?,?)",
            (now, claim["outbox_id"], _json({"turn_id": result["turn_id"]})))
        enqueue_workspace_projection(conn, workspace=binding["workspace"], source_outbox_id=claim["outbox_id"], created_at=now)


def record_native_terminal(database, payload, *, now=_now):
    """Shared durable receipt; carrier-specific observation precedes this call."""
    owner, turn_id = payload["binding"], payload["delivery"]["native_turn_id"]
    with database.transaction() as connection:
        outbox_id = enqueue_control_intent(connection, origin_id=turn_id, attempt_id=owner["attempt_id"],
            topic="agent.native-terminal", payload=payload, created_at=now())
        enqueue_workspace_projection(connection, workspace=owner["workspace"], source_outbox_id=outbox_id,
            created_at=now())
    return database.control_outbox(topic="agent.native-terminal", origin_id=turn_id)[0]
