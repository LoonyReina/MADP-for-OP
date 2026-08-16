from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ascendop_daemon.workflow.workflow_wire import (
    WireProtocolError,
    validate_delivery_transition,
    validate_packet,
)


@dataclass(frozen=True)
class InboxDecision:
    outcome: str
    packet_id: str
    idempotency_key: str
    state: str
    sequence: int
    packet_digest: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class WorkflowInboxStore:
    """Persistent monotonic receipt state for independent Wire V2 operations."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def accept(
        self,
        packet: dict[str, Any],
        *,
        capabilities: Iterable[str] = (),
        supported_extensions: Iterable[str] = (),
        current_generation: int | None = None,
    ) -> InboxDecision:
        validated = validate_packet(
            packet,
            capabilities=capabilities,
            supported_extensions=supported_extensions,
        )
        value = validated.packet
        packet_id = value["header"]["packet_id"]
        operation_key = value["delivery"]["idempotency_key"]
        payload_digest = value["payload"]["digest"]
        state = value["delivery"]["state"]
        sequence = value["delivery"]["sequence"]
        generation = value["workflow"]["state_generation"]
        if current_generation is not None and generation != current_generation:
            self._record_incident(
                packet_id=packet_id,
                idempotency_key=operation_key,
                code="stale-generation",
                detail=(
                    f"packet generation {generation} does not match "
                    f"current generation {current_generation}"
                ),
                packet_digest=validated.digest,
            )
            raise WireProtocolError(
                "stale-generation",
                f"packet generation {generation} does not match "
                f"current generation {current_generation}",
                layer="workflow",
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            packet_owner = connection.execute(
                "SELECT idempotency_key, packet_digest, state, sequence "
                "FROM workflow_packets "
                "WHERE packet_id=?",
                (packet_id,),
            ).fetchone()
            if packet_owner is not None and (
                packet_owner[0] != operation_key
                or packet_owner[1] != validated.digest
            ):
                self._insert_incident(
                    connection,
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    code="packet-id-conflict",
                    detail="packet id is already owned by another operation",
                    packet_digest=validated.digest,
                )
                connection.commit()
                raise WireProtocolError(
                    "packet-id-conflict",
                    "packet id is already owned by different packet content",
                    layer="header",
                )
            if packet_owner is not None:
                return InboxDecision(
                    outcome="duplicate",
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    state=str(packet_owner[2]),
                    sequence=int(packet_owner[3]),
                    packet_digest=validated.digest,
                )
            existing = connection.execute(
                "SELECT packet_id, payload_digest, state, sequence, "
                "state_generation, packet_digest "
                "FROM workflow_operations WHERE idempotency_key=?",
                (operation_key,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO workflow_operations("
                    "idempotency_key, packet_id, profile_id, instance_id, "
                    "payload_digest, state, sequence, state_generation, "
                    "packet_digest, updated_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        operation_key,
                        packet_id,
                        value["workflow"]["profile_id"],
                        value["workflow"]["instance_id"],
                        payload_digest,
                        state,
                        sequence,
                        generation,
                        validated.digest,
                        utc_now_iso(),
                    ),
                )
                self._insert_packet(
                    connection,
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    packet_digest=validated.digest,
                    state=state,
                    sequence=sequence,
                )
                return InboxDecision(
                    outcome="accepted",
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    state=state,
                    sequence=sequence,
                    packet_digest=validated.digest,
                )
            (
                existing_packet_id,
                existing_payload_digest,
                previous_state,
                previous_sequence,
                previous_generation,
                previous_packet_digest,
            ) = existing
            if (
                existing_payload_digest != payload_digest
                or previous_generation != generation
            ):
                self._insert_incident(
                    connection,
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    code="idempotency-conflict",
                    detail=(
                        "an existing operation has a different payload "
                        "or workflow generation"
                    ),
                    packet_digest=validated.digest,
                )
                connection.commit()
                raise WireProtocolError(
                    "idempotency-conflict",
                    "an existing operation has a different payload "
                    "or workflow generation",
                    layer="delivery",
                )
            if sequence < previous_sequence:
                self._insert_incident(
                    connection,
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    code="stale-sequence",
                    detail=f"sequence regressed from {previous_sequence} to {sequence}",
                    packet_digest=validated.digest,
                )
                connection.commit()
                raise WireProtocolError(
                    "stale-sequence",
                    f"sequence regressed from {previous_sequence} to {sequence}",
                    layer="delivery",
                )
            if sequence == previous_sequence:
                if (
                    state == previous_state
                    and validated.digest == previous_packet_digest
                ):
                    return InboxDecision(
                        outcome="duplicate",
                        packet_id=str(existing_packet_id),
                        idempotency_key=operation_key,
                        state=state,
                        sequence=sequence,
                        packet_digest=validated.digest,
                    )
                self._insert_incident(
                    connection,
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    code="sequence-conflict",
                    detail="the same sequence carries different packet content",
                    packet_digest=validated.digest,
                )
                connection.commit()
                raise WireProtocolError(
                    "sequence-conflict",
                    "the same sequence carries different packet content",
                    layer="delivery",
                )
            try:
                validate_delivery_transition(previous_state, state)
            except WireProtocolError as exc:
                self._insert_incident(
                    connection,
                    packet_id=packet_id,
                    idempotency_key=operation_key,
                    code=exc.code,
                    detail=exc.detail,
                    packet_digest=validated.digest,
                )
                connection.commit()
                raise
            connection.execute(
                "UPDATE workflow_operations SET packet_id=?, state=?, "
                "sequence=?, packet_digest=?, updated_at=? "
                "WHERE idempotency_key=?",
                (
                    packet_id,
                    state,
                    sequence,
                    validated.digest,
                    utc_now_iso(),
                    operation_key,
                ),
            )
            self._insert_packet(
                connection,
                packet_id=packet_id,
                idempotency_key=operation_key,
                packet_digest=validated.digest,
                state=state,
                sequence=sequence,
            )
            return InboxDecision(
                outcome="advanced",
                packet_id=packet_id,
                idempotency_key=operation_key,
                state=state,
                sequence=sequence,
                packet_digest=validated.digest,
            )

    def status(self) -> dict[str, Any]:
        with self._connect() as connection:
            operations = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM workflow_operations ORDER BY updated_at, "
                    "idempotency_key"
                ).fetchall()
            ]
            incidents = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM workflow_incidents ORDER BY incident_id"
                ).fetchall()
            ]
            packet_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM workflow_packets"
                ).fetchone()[0]
            )
        return {
            "schema": "ascendop.workflow-inbox-status.v1",
            "operation_count": len(operations),
            "incident_count": len(incidents),
            "packet_count": packet_count,
            "operations": operations,
            "incidents": incidents,
        }

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflow_operations (
                    idempotency_key TEXT PRIMARY KEY,
                    packet_id TEXT NOT NULL UNIQUE,
                    profile_id TEXT NOT NULL,
                    instance_id TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    state TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    state_generation INTEGER NOT NULL,
                    packet_digest TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS workflow_incidents (
                    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    packet_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    code TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    packet_digest TEXT NOT NULL,
                    quarantined_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS workflow_packets (
                    packet_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    packet_digest TEXT NOT NULL,
                    state TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    received_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS workflow_packets_operation_idx
                    ON workflow_packets(idempotency_key, sequence);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _record_incident(
        self,
        *,
        packet_id: str,
        idempotency_key: str,
        code: str,
        detail: str,
        packet_digest: str,
    ) -> None:
        with self._connect() as connection:
            self._insert_incident(
                connection,
                packet_id=packet_id,
                idempotency_key=idempotency_key,
                code=code,
                detail=detail,
                packet_digest=packet_digest,
            )

    @staticmethod
    def _insert_packet(
        connection: sqlite3.Connection,
        *,
        packet_id: str,
        idempotency_key: str,
        packet_digest: str,
        state: str,
        sequence: int,
    ) -> None:
        connection.execute(
            "INSERT INTO workflow_packets("
            "packet_id, idempotency_key, packet_digest, state, sequence, "
            "received_at"
            ") VALUES(?,?,?,?,?,?)",
            (
                packet_id,
                idempotency_key,
                packet_digest,
                state,
                sequence,
                utc_now_iso(),
            ),
        )

    @staticmethod
    def _insert_incident(
        connection: sqlite3.Connection,
        *,
        packet_id: str,
        idempotency_key: str,
        code: str,
        detail: str,
        packet_digest: str,
    ) -> None:
        connection.execute(
            "INSERT INTO workflow_incidents("
            "packet_id, idempotency_key, code, detail, packet_digest, "
            "quarantined_at"
            ") VALUES(?,?,?,?,?,?)",
            (
                packet_id,
                idempotency_key,
                code,
                detail,
                packet_digest,
                utc_now_iso(),
            ),
        )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
