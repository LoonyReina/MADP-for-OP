"""External results share the original local candidate and existing control outbox.
Trusted hosts supply domain receipt validation and successor planning.
"""
from __future__ import annotations
from copy import deepcopy
from contextlib import nullcontext
from datetime import datetime, timezone
import json
from typing import Any
from ascendop_control.storage.outbox_repository import enqueue_control_intent
from ascendop_control.storage.workspace_repository import enqueue_workspace_projection
TOPIC = "official.terminal"


def official_origin(local_event_id: str) -> str:
    return f"official:{local_event_id}"


class ExternalCompletionCore:
    def __init__(self, database, *, validate_local, plan_continuation, validate_policy):
        self.database = database
        self.validate_local = validate_local
        self.plan_continuation = plan_continuation
        self.validate_policy = validate_policy

    def _accepted_event(self, instance_id: str, event_id: str, *, connection: Any = None) -> dict[str, Any] | None:
        # The original external event may belong to only one local terminal.
        # This check is repeated inside the write transaction, across origins.
        with (nullcontext(connection) if connection is not None else self.database.connection()) as conn:
            rows = conn.execute(
                "SELECT outbox_id,payload_json FROM control_outbox_v5 WHERE topic=? AND attempt_id=?",
                (TOPIC, f"{instance_id}:{event_id}"),
            ).fetchall()
        if len(rows) > 1:
            raise ValueError("official event is already associated with multiple local terminals")
        return {"outbox_id": rows[0]["outbox_id"], "payload": json.loads(rows[0]["payload_json"])} if rows else None

    def ingest(self, event: dict[str, Any]) -> str:
        payload = event["payload"]
        binding = payload["control_binding"]
        if (event.get("schema") != "ascendop.official-control-event.v1"
                or binding.get("schema") != "ascendop.official-control-binding.v1"):
            raise ValueError("unsupported official control event")
        origin = official_origin(binding["local_event_id"])
        attempt = f"{event['instance_id']}:{event['event_id']}"
        existing = self._accepted_event(event["instance_id"], event["event_id"])
        if existing is not None:
            if existing["payload"]["official_event"] != event:
                raise ValueError("official event replay changed")
            return existing["outbox_id"]
        accepted = self.database.control_outbox(topic="test.terminal", origin_id=binding["local_event_id"])
        if len(accepted) != 1:
            raise ValueError("official event has no exact accepted local terminal")
        local = accepted[0]["payload"]
        intent = deepcopy(local["continuation"])
        if (local["event"]["request_id"] != binding["request_id"]
                or local["event"]["outcome"] != "success"
                or intent["source_action_id"] != binding["source_action_id"]
                or intent["source_sha256"] != payload["source_sha256"]
                or intent["operator"] != payload["operator_id"]
                or intent["kind"] != "official.wait"):
            raise ValueError("official feedback does not belong to this local candidate")
        self.validate_local(binding, payload, intent, local)
        if payload.get("purpose") == "official_submission_decision":
            self.validate_policy(payload)
            passed = False  # A policy decision can never establish official PASS.
        else:
            feedback = payload["feedback"]
            verdict = str(feedback.get("result") or feedback.get("verdict") or "").lower()
            if feedback.get("terminal") is not True or verdict in {"", "unknown", "pending", "running"}:
                raise ValueError("official terminal feedback is incomplete")
            passed = verdict in {"pass", "passed", "accepted"}
        intent = self.plan_continuation(binding, intent, passed)
        with self.database.transaction() as connection:
            existing = self._accepted_event(event["instance_id"], event["event_id"], connection=connection)
            if existing is not None:
                if existing["payload"]["official_event"] != event:
                    raise ValueError("official event replay changed")
                return existing["outbox_id"]
            now = datetime.now(timezone.utc).isoformat()
            outbox_id = enqueue_control_intent(
                connection, origin_id=origin, attempt_id=attempt, topic=TOPIC,
                payload={"official_event": event, "event": local["event"], "continuation": intent},
                created_at=now,
            )
            if "workspace_owner" in intent:
                enqueue_workspace_projection(connection, workspace=intent["workspace"],
                    source_outbox_id=outbox_id, created_at=now)
            return outbox_id
