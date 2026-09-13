"""Validate original admitted runtime evidence; unknown is never writer exit."""
from __future__ import annotations
CARRIER = "codex-app-server"
STATUSES = {"completed", "failed", "interrupted", "cancelled"}

def require_boundary(database, payload):
    """Check the stored carrier evidence, not current mutable files/processes."""
    proof = payload.get("writer_quiescence") or {}
    rows = database.control_outbox(topic="agent.native-start", origin_id=payload["binding"]["action_id"])
    admitted = next((row for row in rows if row["outbox_id"] == proof.get("native_start_id")), None)
    if (admitted is None or admitted["state"] != "delivered"
            or admitted["payload"].get("carrier") != CARRIER
            or admitted["payload"]["binding"] != payload["binding"]):
        raise ValueError("resident proposal requires its original admission")
    result, process = admitted["result"], payload["process"]
    if (proof.get("observation") != "quiescent" or proof.get("job_name") != result["job_name"]
            or proof.get("session_id") != admitted["payload"]["target_id"]
            or not proof.get("provider_turn_id") or proof.get("status") != payload["status"]
            or payload["status"] not in STATUSES
            or payload["delivery"]["native_turn_id"] != result["turn_id"]
            or process.get("observation") != "resident-turn-terminal"
            or any(process.get(key) != result[key] for key in ("pid", "start_token"))):
        raise ValueError("resident proposal boundary differs from original turn/runtime")
