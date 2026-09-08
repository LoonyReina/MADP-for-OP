"""Typed-action delivery through the shared control outbox, without IDE recovery."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from ascendop_control.delivery import dispatch_topic
from ascendop_daemon.storage.control_types import SCHEMA_VERSION


class FormalContinuations:
    def __init__(self, root: Path, database: Any, *, code_generation: str):
        self.root, self.database, self.code_generation = root.resolve(), database, code_generation

    def reconcile(self, *, limit: int = 4, origin_id: str | None = None):
        return dispatch_topic(self.database, topic="agent.completion", owner="formal-completion",
            limit=limit, origin_id=origin_id,
            handler=lambda row: deliver_formal_continuation(self.root, self.database, self.code_generation, row))

    def result(self, origin_id: str, lease_id: str) -> dict[str, Any]:
        for row in self.database.control_outbox(topic="agent.completion", origin_id=origin_id):
            if row["payload"]["receipt"]["lease_id"] == lease_id:
                return {**row["result"], "continuation_state": row["state"]}
        return {"continuation_state": "none"}


def deliver_formal_continuation(root: Path, database: Any, code_generation: str,
                                row: Mapping[str, Any]) -> dict[str, Any]:
    origin_id = str(row["origin_id"])
    action = database.agent_action(origin_id)
    receipt = row["payload"]["receipt"]
    if (
        action is None
        or action["state"] != "completed"
        or str(action.get("current_attempt_id")) != row["attempt_id"]
        or database.agent_action_receipt(origin_id) != receipt
        or not database.workflow_agent_action_is_current(origin_id)
    ):
        return {
            "disposition": "obsolete",
            "promotion": None,
            "output_promotion": None,
            "evidence_operation": None,
        }
    intent = row["payload"]["continuation"]
    result = {
        "promotion": None,
        "output_promotion": None,
        "evidence_operation": None,
    }
    for planned in intent["workflow_actions"]:
        for artifact in planned["action"]["artifacts"]:
            path = (root / artifact["path"]).resolve()
            if root not in path.parents or not path.is_file():
                raise ValueError("completion artifact is missing or outside root")
            if hashlib.sha256(path.read_bytes()).hexdigest() != artifact["sha256"]:
                raise ValueError("completion artifact seal changed")
    # Saved plans have immutable artifact and idempotency identities. The
    # current compatible installer supplies executor generation, never a
    # newly inferred candidate, case, or source identity.
    for planned in intent["workflow_actions"]:
        result[planned["slot"]] = database.create_workflow_action(
            {
                **planned["action"],
                "producer_generation": code_generation,
                "control_schema": SCHEMA_VERSION,
            }
        )
    if intent.get("promotion_is_output"):
        result["promotion"] = result["output_promotion"]
    if intent.get("evidence_request") is not None:
        result["evidence_operation"] = (
            database.create_evidence_operation_request(
                intent["evidence_request"]
            )
        )
    return {"disposition": "delivered", **result}
