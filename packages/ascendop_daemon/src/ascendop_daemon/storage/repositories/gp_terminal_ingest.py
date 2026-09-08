from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping
from ascendop_control.storage.outbox_repository import enqueue_control_intent
from ascendop_control.storage.workspace_repository import enqueue_workspace_projection

from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.storage.control_validation import canonical_json, utc_now


SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SAFE_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
TERMINAL_INGEST_SCHEMA = "ascendop.gp-terminal-ingest-event.v1"


class GpTerminalIngestRepository:
    def record_gp_terminal_ingest_event(
        self,
        raw: Mapping[str, Any],
        *,
        source_action_id: str = "",
        workspace_projection_ref: str = "",
        continuation: Mapping[str, Any] | None = None,
        ack_request: Mapping[str, Any] | None = None,
        import_existing: bool = False,
    ) -> dict[str, Any]:
        event = _validated_event(raw)
        self.initialize()
        now = utc_now()
        encoded = canonical_json(event)
        if (continuation is None) != (ack_request is None):
            raise ControlDatabaseError(
                "terminal continuation and ACK intent must be committed together"
            )
        intents = (
            {
                "test.terminal": {"event": event, "continuation": dict(continuation)},
                "test.ack": {"event": event, "ack_request": dict(ack_request)},
            }
            if continuation is not None
            else {}
        )
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT event_json, source_action_id, workspace_projection_ref "
                "FROM gp_terminal_ingest_events_v5 WHERE event_id=?",
                (event["event_id"],),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["event_json"]) != encoded
                    or str(existing["source_action_id"]) != source_action_id
                    or str(existing["workspace_projection_ref"])
                    != workspace_projection_ref
                ):
                    raise ControlDatabaseError(
                        "GP terminal ingest event identity collision"
                    )
                if intents and not import_existing:
                    topics = {
                        row["topic"]
                        for row in conn.execute(
                            "SELECT topic FROM control_outbox_v5 WHERE origin_id=? AND attempt_id=?",
                            (event["event_id"], event["attempt_id"]),
                        )
                    }
                    if not set(intents).issubset(topics):
                        raise ControlDatabaseError(
                            "historical terminal requires an explicit evidence-checked import"
                        )
            else:
                conn.execute(
                    """
                INSERT INTO gp_terminal_ingest_events_v5(
                    event_id, request_id, attempt_id, receipt_id,
                    terminal_revision, result_payload_sha256, envelope_digest,
                    event_json, source_action_id, workspace_projection_ref,
                    successor_action_id, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)
                """,
                    (
                        event["event_id"],
                        event["request_id"],
                        event["attempt_id"],
                        event["receipt_id"],
                        event["terminal_revision"],
                        event["result_payload_sha256"],
                        event["envelope_digest"],
                        encoded,
                        source_action_id,
                        workspace_projection_ref,
                        now,
                        now,
                    ),
                )
                self._event(
                    conn,
                    "gp-terminal-ingest-recorded",
                    "gp-terminal-ingest-event",
                    event["event_id"],
                    {
                        "request_id": event["request_id"],
                        "attempt_id": event["attempt_id"],
                        "receipt_id": event["receipt_id"],
                        "source_action_id": source_action_id,
                    },
                )
            for topic, payload in intents.items():
                outbox_id = enqueue_control_intent(
                    conn,
                    origin_id=event["event_id"],
                    attempt_id=event["attempt_id"],
                    topic=topic,
                    payload=payload,
                    created_at=now,
                )
                if topic == "test.terminal" and "workspace_owner" in continuation:
                    enqueue_workspace_projection(conn, workspace=continuation["workspace"],
                        source_outbox_id=outbox_id, created_at=now)
        return {
            **event,
            "disposition": "already-recorded" if existing is not None else "recorded",
        }

    def bind_gp_terminal_ingest_successor(
        self,
        event_id: str,
        successor_action_id: str,
    ) -> dict[str, Any]:
        _safe_token(event_id, "event_id", sha=True)
        _safe_token(successor_action_id, "successor_action_id")
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT successor_action_id FROM gp_terminal_ingest_events_v5 "
                "WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError("unknown GP terminal ingest event")
            existing = str(row["successor_action_id"])
            if existing and existing != successor_action_id:
                raise ControlDatabaseError(
                    "GP terminal ingest successor identity collision"
                )
            if not existing:
                conn.execute(
                    "UPDATE gp_terminal_ingest_events_v5 "
                    "SET successor_action_id=?, updated_at=? WHERE event_id=?",
                    (successor_action_id, now, event_id),
                )
                self._event(
                    conn,
                    "gp-terminal-ingest-successor-bound",
                    "gp-terminal-ingest-event",
                    event_id,
                    {"successor_action_id": successor_action_id},
                )
        return self.gp_terminal_ingest_event(event_id)

    def gp_terminal_ingest_event(self, event_id: str) -> dict[str, Any]:
        _safe_token(event_id, "event_id", sha=True)
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM gp_terminal_ingest_events_v5 WHERE event_id=?",
                (event_id,),
            ).fetchone()
        if row is None:
            return {}
        event = json.loads(str(row["event_json"]))
        return {
            **event,
            "source_action_id": str(row["source_action_id"]),
            "workspace_projection_ref": str(row["workspace_projection_ref"]),
            "successor_action_id": str(row["successor_action_id"]),
        }


def _validated_event(raw: Mapping[str, Any]) -> dict[str, Any]:
    event = dict(raw)
    if event.get("schema") != TERMINAL_INGEST_SCHEMA:
        raise ControlDatabaseError("unsupported GP terminal ingest event")
    identity = {
        "request_id": _safe_token(event.get("request_id"), "request_id"),
        "attempt_id": _safe_token(event.get("attempt_id"), "attempt_id"),
        "receipt_id": _safe_token(event.get("receipt_id"), "receipt_id"),
        "terminal_revision": _terminal_revision(event.get("terminal_revision")),
        "result_payload_sha256": _safe_token(
            event.get("result_payload_sha256"),
            "result_payload_sha256",
            sha=True,
        ),
        "envelope_digest": _safe_token(
            event.get("envelope_digest"), "envelope_digest", sha=True
        ),
    }
    expected_event_id = hashlib.sha256(
        json.dumps(
            identity,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
    if event.get("event_id") != expected_event_id:
        raise ControlDatabaseError("GP terminal ingest event digest mismatch")
    ack = event.get("ack")
    if not isinstance(ack, Mapping) or dict(ack) != {
        "request_id": identity["request_id"],
        "attempt_id": identity["attempt_id"],
        "receipt_id": identity["receipt_id"],
    }:
        raise ControlDatabaseError("GP terminal ingest ACK identity mismatch")
    return {
        "schema": TERMINAL_INGEST_SCHEMA,
        "event_id": expected_event_id,
        **identity,
        "outcome": str(event.get("outcome") or ""),
        "failure_domain": str(event.get("failure_domain") or ""),
        "artifact_root": str(event.get("artifact_root") or ""),
        "ack": dict(ack),
    }


def _safe_token(value: Any, field: str, *, sha: bool = False) -> str:
    normalized = str(value or "").strip().lower() if sha else str(value or "").strip()
    pattern = SHA256 if sha else SAFE_TOKEN
    if not pattern.fullmatch(normalized):
        raise ControlDatabaseError(f"invalid GP terminal ingest {field}")
    return normalized


def _terminal_revision(value: Any) -> int:
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise ControlDatabaseError("invalid GP terminal revision") from exc
    if revision < 0:
        raise ControlDatabaseError("invalid GP terminal revision")
    return revision
