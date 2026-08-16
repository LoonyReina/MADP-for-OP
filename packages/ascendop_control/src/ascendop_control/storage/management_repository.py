from __future__ import annotations

import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from ascendop_protocol.management import (
    CONTROL_EVENT_SCHEMA,
    PUBLIC_RESOURCE_SCHEMA,
    validate_control_command,
    validate_control_command_receipt,
)

from .errors import ControlRepositoryError


class ManagementRepository:
    def submit_control_command(self, command: Mapping[str, Any]) -> dict[str, Any]:
        value = validate_control_command(command)
        payload = _canonical_json(value)
        now = _utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT command_id, command_json, state FROM control_commands_v4 "
                "WHERE idempotency_key=?",
                (value["idempotency_key"],),
            ).fetchone()
            if existing is not None:
                if str(existing["command_json"]) != payload:
                    raise ControlRepositoryError(
                        "control command idempotency collision with different payload"
                    )
                return {
                    "command_id": str(existing["command_id"]),
                    "state": str(existing["state"]),
                    "deduplicated": True,
                }
            conn.execute(
                "INSERT INTO control_commands_v4(command_id, idempotency_key, "
                "command_kind, actor_id, required_capability, state, command_json, "
                "created_at, updated_at) VALUES(?, ?, ?, ?, ?, 'queued', ?, ?, ?)",
                (
                    value["command_id"],
                    value["idempotency_key"],
                    value["command_kind"],
                    value["actor_id"],
                    value["required_capability"],
                    payload,
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "control-command-created",
                "control-command",
                str(value["command_id"]),
                {"command_kind": value["command_kind"], "actor_id": value["actor_id"]},
            )
        return {
            "command_id": value["command_id"],
            "state": "queued",
            "deduplicated": False,
        }

    def claim_control_command(
        self,
        *,
        worker_id: str,
        lease_seconds: int = 30,
    ) -> dict[str, Any] | None:
        normalized_worker = str(worker_id).strip()
        if not normalized_worker:
            raise ControlRepositoryError("control command worker id is required")
        if not 1 <= int(lease_seconds) <= 3600:
            raise ControlRepositoryError("claim lease seconds must be in [1, 3600]")
        now = _utc_now()
        expires_at = (
            datetime.fromisoformat(now) + timedelta(seconds=int(lease_seconds))
        ).isoformat()
        with self.transaction() as conn:
            expired = conn.execute(
                "SELECT command_id FROM control_commands_v4 WHERE state='claimed' "
                "AND claim_expires_at<>'' AND claim_expires_at<=? ORDER BY created_at",
                (now,),
            ).fetchall()
            for row in expired:
                command_id = str(row["command_id"])
                conn.execute(
                    "UPDATE control_commands_v4 SET state='queued', claimed_by='', "
                    "claim_token='', claim_expires_at='', updated_at=? "
                    "WHERE command_id=? AND state='claimed'",
                    (now, command_id),
                )
                self._event(
                    conn,
                    "control-command-claim-expired",
                    "control-command",
                    command_id,
                    {},
                )
            row = conn.execute(
                "SELECT command_id, command_json FROM control_commands_v4 "
                "WHERE state='queued' ORDER BY created_at, command_id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            claim_token = secrets.token_urlsafe(32)
            command_id = str(row["command_id"])
            changed = conn.execute(
                "UPDATE control_commands_v4 SET state='claimed', claimed_by=?, "
                "claim_token=?, claim_expires_at=?, updated_at=? "
                "WHERE command_id=? AND state='queued'",
                (
                    normalized_worker,
                    claim_token,
                    expires_at,
                    now,
                    command_id,
                ),
            ).rowcount
            if changed != 1:
                return None
            self._event(
                conn,
                "control-command-claimed",
                "control-command",
                command_id,
                {"worker_id": normalized_worker, "claim_expires_at": expires_at},
            )
            return {
                "command": json.loads(str(row["command_json"])),
                "claim_token": claim_token,
                "claim_expires_at": expires_at,
            }

    def complete_control_command(
        self,
        receipt: Mapping[str, Any],
        *,
        claim_token: str,
    ) -> dict[str, Any]:
        value = validate_control_command_receipt(receipt)
        payload = _canonical_json(value)
        now = _utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT receipt_json FROM control_command_receipts_v4 "
                "WHERE command_id=?",
                (value["command_id"],),
            ).fetchone()
            if existing is not None:
                if str(existing["receipt_json"]) != payload:
                    raise ControlRepositoryError(
                        "control command receipt collision with different payload"
                    )
                return dict(value)
            row = conn.execute(
                "SELECT state, claim_token, claim_expires_at "
                "FROM control_commands_v4 WHERE command_id=?",
                (value["command_id"],),
            ).fetchone()
            if row is None:
                raise ControlRepositoryError("control command does not exist")
            if str(row["state"]) != "claimed":
                raise ControlRepositoryError("control command is not claimed")
            if not claim_token or str(row["claim_token"]) != claim_token:
                raise ControlRepositoryError("control command claim token mismatch")
            if str(row["claim_expires_at"]) <= now:
                raise ControlRepositoryError("control command claim has expired")
            conn.execute(
                "UPDATE control_commands_v4 SET state=?, claimed_by='', "
                "claim_token='', claim_expires_at='', updated_at=? WHERE command_id=?",
                (value["status"], now, value["command_id"]),
            )
            conn.execute(
                "INSERT INTO control_command_receipts_v4(command_id, status, "
                "receipt_json, completed_at) VALUES(?, ?, ?, ?)",
                (
                    value["command_id"],
                    value["status"],
                    payload,
                    value["completed_at"],
                ),
            )
            self._event(
                conn,
                "control-command-terminal",
                "control-command",
                str(value["command_id"]),
                {"status": value["status"]},
            )
        return dict(value)

    def record_control_command_receipt(
        self,
        receipt: Mapping[str, Any],
        *,
        claim_token: str,
    ) -> dict[str, Any]:
        return self.complete_control_command(receipt, claim_token=claim_token)

    def upsert_public_resource(
        self,
        *,
        resource_type: str,
        resource_id: str,
        revision: str,
        attributes: Mapping[str, Any],
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        value = {
            "schema": PUBLIC_RESOURCE_SCHEMA,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "revision": revision,
            "observed_at": observed_at or _utc_now(),
            "attributes": dict(attributes),
        }
        now = _utc_now()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO public_resource_projections_v4(resource_type, resource_id, "
                "revision, resource_json, observed_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(resource_type, resource_id) DO UPDATE SET "
                "revision=excluded.revision, resource_json=excluded.resource_json, "
                "observed_at=excluded.observed_at",
                (
                    resource_type,
                    resource_id,
                    revision,
                    _canonical_json(value),
                    value["observed_at"],
                ),
            )
            self._event(
                conn,
                "public-resource-projected",
                resource_type,
                resource_id,
                {"revision": revision, "projected_at": now},
            )
        return value

    def public_resources(
        self, resource_type: str, *, resource_id: str | None = None
    ) -> list[dict[str, Any]]:
        query = (
            "SELECT resource_json FROM public_resource_projections_v4 "
            "WHERE resource_type=?"
        )
        parameters: tuple[Any, ...] = (resource_type,)
        if resource_id is not None:
            query += " AND resource_id=?"
            parameters += (resource_id,)
        query += " ORDER BY resource_id"
        with self.connection() as conn:
            rows = conn.execute(query, parameters).fetchall()
        return [json.loads(str(row["resource_json"])) for row in rows]

    def control_events_after(
        self, sequence: int, *, limit: int = 200
    ) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM control_events WHERE sequence>? ORDER BY sequence LIMIT ?",
                (max(0, int(sequence)), max(1, min(int(limit), 1000))),
            ).fetchall()
        return [
            {
                "schema": CONTROL_EVENT_SCHEMA,
                "sequence": int(row["sequence"]),
                "event_at": str(row["event_at"]),
                "event_type": str(row["event_type"]),
                "entity_type": str(row["entity_type"]),
                "entity_id": str(row["entity_id"]),
                "payload": json.loads(str(row["payload_json"])),
            }
            for row in rows
        ]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
