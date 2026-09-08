"""Domain-neutral transactional delivery intents; handlers must be idempotent.

This is delivery state, not another business reducer. The authoritative business
transaction enqueues an immutable intent using its existing connection.
"""
from __future__ import annotations

import hashlib
import json
import math
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from .errors import ControlRepositoryError


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _after(now: str, seconds: float) -> str:
    if not math.isfinite(float(seconds)) or seconds < 0:
        raise ControlRepositoryError("outbox delay must be finite and nonnegative")
    return (datetime.fromisoformat(now) + timedelta(seconds=seconds)).isoformat(timespec="microseconds")


def enqueue_control_intent(
    conn: sqlite3.Connection, *, origin_id: str, attempt_id: str,
    topic: str, payload: Mapping[str, Any], created_at: str,
) -> str:
    if not all(isinstance(value, str) and value.strip() for value in (origin_id, attempt_id, topic)):
        raise ControlRepositoryError("outbox origin, attempt and topic are required")
    instant = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ControlRepositoryError("outbox timestamp requires a timezone")
    created_at = instant.astimezone(timezone.utc).isoformat(timespec="microseconds")
    key = _json([origin_id, attempt_id, topic])
    outbox_id = "cob-" + hashlib.sha256(key.encode()).hexdigest()
    body = _json(dict(payload))
    row = conn.execute("SELECT payload_json FROM control_outbox_v5 WHERE outbox_id=?", (outbox_id,)).fetchone()
    if row is not None:
        if row["payload_json"] != body:
            raise ControlRepositoryError(f"outbox intent conflict: {outbox_id}")
        return outbox_id
    conn.execute(
        "INSERT INTO control_outbox_v5(outbox_id,origin_id,attempt_id,topic,payload_json,"
        "state,available_at,created_at,updated_at) VALUES(?,?,?,?,?,'pending',?,?,?)",
        (outbox_id, origin_id, attempt_id, topic, body, created_at, created_at, created_at),
    )
    conn.execute(
        "INSERT INTO control_events(event_at,event_type,entity_type,entity_id,payload_json) "
        "VALUES(?,'control-outbox-enqueued','control-outbox',?,?)",
        (created_at, outbox_id, _json({"topic": topic})),
    )
    return outbox_id


class ControlOutboxRepository:
    def control_outbox(
        self, *, topic: str, origin_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        conditions, values = ["topic=?"], [topic]
        if origin_id is not None:
            conditions.append("origin_id=?")
            values.append(origin_id)
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM control_outbox_v5 WHERE " + " AND ".join(conditions)
                + " ORDER BY created_at,outbox_id LIMIT ?",
                (*values, max(1, min(1000, int(limit)))),
            ).fetchall()
        return [_decode(row) for row in rows]

    def claim_control_outbox(
        self, *, topic: str, owner: str, lease_seconds: float = 60,
        origin_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not owner or lease_seconds <= 0:
            raise ControlRepositoryError("outbox claim requires owner and positive lease")
        now = _now()
        expires = _after(now, lease_seconds)
        condition = " AND origin_id=?" if origin_id is not None else ""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM control_outbox_v5 WHERE topic=? AND "
                "((state='pending' AND available_at<=?) OR (state='claimed' AND lease_expires_at<=?))"
                + condition + " ORDER BY available_at,created_at,outbox_id LIMIT 1",
                (topic, now, now, *((origin_id,) if origin_id is not None else ())),
            ).fetchone()
            if row is None:
                return None
            token = secrets.token_hex(24)
            conn.execute(
                "UPDATE control_outbox_v5 SET state='claimed',claim_token=?,owner=?,"
                "lease_expires_at=?,attempts=attempts+1,updated_at=? WHERE outbox_id=?",
                (token, owner, expires, now, row["outbox_id"]),
            )
            return _decode(conn.execute("SELECT * FROM control_outbox_v5 WHERE outbox_id=?", (row["outbox_id"],)).fetchone())

    def renew_control_outbox(
        self, *, outbox_id: str, claim_token: str, lease_seconds: float = 60,
    ) -> dict[str, Any]:
        """Keep one live delivery claim; never revive an expired/replaced token."""
        if lease_seconds <= 0:
            raise ControlRepositoryError("outbox renewal requires a positive lease")
        with self.transaction() as conn:
            # Read time after acquiring the write lock, not before a busy wait.
            now = _now()
            expires = _after(now, lease_seconds)
            _require_claim(conn, outbox_id, claim_token, now)
            conn.execute(
                "UPDATE control_outbox_v5 SET lease_expires_at=?,updated_at=? WHERE outbox_id=?",
                (expires, now, outbox_id),
            )
            return _decode(conn.execute("SELECT * FROM control_outbox_v5 WHERE outbox_id=?", (outbox_id,)).fetchone())

    def finish_control_outbox(
        self, *, outbox_id: str, claim_token: str, result: Mapping[str, Any]
    ) -> dict[str, Any]:
        body = _json(dict(result))
        now = _now()
        with self.transaction() as conn:
            _require_claim(conn, outbox_id, claim_token, now)
            conn.execute(
                "UPDATE control_outbox_v5 SET state='delivered',result_json=?,claim_token='',"
                "lease_expires_at='',last_error='',updated_at=? WHERE outbox_id=?",
                (body, now, outbox_id),
            )
            return _decode(conn.execute("SELECT * FROM control_outbox_v5 WHERE outbox_id=?", (outbox_id,)).fetchone())

    def defer_control_outbox(
        self, *, outbox_id: str, claim_token: str, error: str, delay_seconds: float = 5,
    ) -> dict[str, Any]:
        now = _now()
        available = _after(now, delay_seconds)
        with self.transaction() as conn:
            _require_claim(conn, outbox_id, claim_token, now)
            conn.execute(
                "UPDATE control_outbox_v5 SET state='pending',available_at=?,claim_token='',"
                "lease_expires_at='',last_error=?,updated_at=? WHERE outbox_id=?",
                (available, str(error)[:2000], now, outbox_id),
            )
            return _decode(conn.execute("SELECT * FROM control_outbox_v5 WHERE outbox_id=?", (outbox_id,)).fetchone())


def _require_claim(conn: sqlite3.Connection, outbox_id: str, token: str, now: str) -> None:
    row = conn.execute("SELECT * FROM control_outbox_v5 WHERE outbox_id=?", (outbox_id,)).fetchone()
    if (row is None or row["state"] != "claimed" or not token
            or row["claim_token"] != token or row["lease_expires_at"] <= now):
        raise ControlRepositoryError("outbox claim is missing, expired or fenced")


def _decode(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["payload"] = json.loads(value.pop("payload_json"))
    value["result"] = json.loads(value.pop("result_json"))
    return value
