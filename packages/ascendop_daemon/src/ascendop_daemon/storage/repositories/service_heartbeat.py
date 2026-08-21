from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from ascendop_daemon.storage.control_types import SCHEMA_VERSION, ControlDatabaseError
from ascendop_daemon.storage.control_validation import (
    _format_timestamp,
    _parse_timestamp,
    canonical_json,
)


class ServiceHeartbeatRepository:
    def record_service_heartbeat(
        self,
        *,
        service_id: str,
        role: str,
        code_generation: str,
        wire_version: int,
        capabilities: list[str],
        state: str,
        boot_id: str,
        lease_seconds: int = 30,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        if not all(
            isinstance(value, str) and value.strip()
            for value in (service_id, role, code_generation, state, boot_id)
        ):
            raise ControlDatabaseError("service heartbeat identity is incomplete")
        if isinstance(wire_version, bool) or int(wire_version) != 3:
            raise ControlDatabaseError("service heartbeat must advertise Wire V3")
        normalized_capabilities = sorted(
            {
                value.strip()
                for value in capabilities
                if isinstance(value, str) and value.strip()
            }
        )
        now_value = datetime.now(timezone.utc)
        now = _format_timestamp(now_value)
        lease_expires_at = _format_timestamp(
            now_value + timedelta(seconds=max(1, int(lease_seconds)))
        )
        capabilities_json = canonical_json(normalized_capabilities)
        details_json = canonical_json(details or {})
        with self.transaction() as conn:
            previous = conn.execute(
                "SELECT role, code_generation, wire_version, database_schema, "
                "capabilities_json, state, boot_id FROM service_heartbeats "
                "WHERE service_id=?",
                (service_id,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO service_heartbeats(
                    service_id, role, code_generation, wire_version,
                    database_schema, capabilities_json, state, boot_id,
                    heartbeat_at, lease_expires_at, details_json, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(service_id) DO UPDATE SET
                    role=excluded.role,
                    code_generation=excluded.code_generation,
                    wire_version=excluded.wire_version,
                    database_schema=excluded.database_schema,
                    capabilities_json=excluded.capabilities_json,
                    state=excluded.state,
                    boot_id=excluded.boot_id,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at,
                    details_json=excluded.details_json,
                    updated_at=excluded.updated_at
                """,
                (
                    service_id,
                    role,
                    code_generation,
                    3,
                    SCHEMA_VERSION,
                    capabilities_json,
                    state,
                    boot_id,
                    now,
                    lease_expires_at,
                    details_json,
                    now,
                ),
            )
            identity = (
                role,
                code_generation,
                3,
                SCHEMA_VERSION,
                capabilities_json,
                state,
                boot_id,
            )
            if previous is None or tuple(previous) != identity:
                self._event(
                    conn,
                    "service-registration-changed",
                    "service",
                    service_id,
                    {
                        "role": role,
                        "code_generation": code_generation,
                        "wire_version": 3,
                        "database_schema": SCHEMA_VERSION,
                        "capabilities": normalized_capabilities,
                        "state": state,
                        "boot_id": boot_id,
                    },
                )
        return {
            "service_id": service_id,
            "role": role,
            "code_generation": code_generation,
            "wire_version": 3,
            "database_schema": SCHEMA_VERSION,
            "capabilities": normalized_capabilities,
            "state": state,
            "boot_id": boot_id,
            "heartbeat_at": now,
            "lease_expires_at": lease_expires_at,
            "details": details or {},
            "live": state == "ready",
        }

    def service_health(self) -> list[dict[str, Any]]:
        self.initialize()
        now = datetime.now(timezone.utc)
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM service_heartbeats ORDER BY service_id"
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            lease_expires_at = _parse_timestamp(
                str(row["lease_expires_at"]),
                "lease_expires_at",
            )
            result.append(
                {
                    "service_id": str(row["service_id"]),
                    "role": str(row["role"]),
                    "code_generation": str(row["code_generation"]),
                    "wire_version": int(row["wire_version"]),
                    "database_schema": int(row["database_schema"]),
                    "capabilities": json.loads(row["capabilities_json"]),
                    "state": str(row["state"]),
                    "boot_id": str(row["boot_id"]),
                    "heartbeat_at": str(row["heartbeat_at"]),
                    "lease_expires_at": str(row["lease_expires_at"]),
                    "details": json.loads(row["details_json"]),
                    "live": (
                        str(row["state"]) == "ready"
                        and lease_expires_at >= now
                    ),
                }
            )
        return result
