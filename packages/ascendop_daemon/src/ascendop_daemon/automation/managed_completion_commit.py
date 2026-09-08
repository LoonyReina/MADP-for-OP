"""Shared managed completion transaction after trusted host validation.

The caller is the trusted intake, never a Solver. It must validate the native
writer boundary and freeze inputs before calling. No carrier, domain planner,
filesystem mutation or second receipt ledger is introduced here.
"""
from __future__ import annotations

import json
from typing import Any, Mapping

from ascendop_control.storage.outbox_repository import enqueue_control_intent
from ascendop_control.storage.workspace_repository import enqueue_workspace_projection
from .completion_facts import AgentCompletionError


def record_workspace_completion(database: Any, *, action_id: str, owner: Mapping[str, Any],
                                receipt: Mapping[str, Any], payload: Mapping[str, Any],
                                followup: Mapping[str, Any] | None = None) -> dict[str, Any]:
    with database.transaction() as connection:
        concurrent = connection.execute("SELECT outbox_id,payload_json FROM control_outbox_v5 WHERE topic='agent.completion' AND origin_id=?",
            (action_id,)).fetchone()
        if concurrent is not None:
            saved = json.loads(concurrent["payload_json"])
            if saved.get("receipt") != receipt or saved.get("binding") != owner:
                raise AgentCompletionError("conflicting standalone completion replay")
            return {"outbox_id": concurrent["outbox_id"], "payload": saved}
        outbox_id = enqueue_control_intent(connection, origin_id=action_id,
            attempt_id=owner["attempt_id"], topic="agent.completion", payload=payload,
            created_at=receipt["completed_at"])
        enqueue_workspace_projection(connection, workspace=owner["workspace"],
            source_outbox_id=outbox_id, created_at=receipt["completed_at"])
        if followup is not None:
            enqueue_control_intent(connection, origin_id=action_id, attempt_id=owner["attempt_id"],
                topic="agent.followup", payload={"completion_outbox_id": outbox_id, "continuation": followup},
                created_at=receipt["completed_at"])
    return {"outbox_id": outbox_id, "payload": payload}
